//! How many worker threads `--threads 0` (auto) starts, and why.
//!
//! Auto is one worker per CPU the process may actually run on: the CPUs
//! online, cut down to the affinity mask (`taskset`, a cpuset, Docker's
//! `--cpuset-cpus`, Kubernetes' static CPU manager) and to the cgroup CPU
//! quota (Docker's `--cpus`, a Kubernetes CPU limit), at least one.
//!
//! Up to 1.2 auto was the CPUs minus two (all of them below four): two
//! workers on a 4-CPU machine. With the server on 4 vCPUs of 4 cores that
//! served 27% less than four workers on io_uring and 44% less on tokio,
//! whose p99 it multiplied by six (#873). No other thread of the server
//! needs a CPU of its own: the AOF writer, the replication hub, expiry and a
//! background save run beside the workers, and a worker fewer than the CPUs
//! was not consistently better with any of them running.
//!
//! `worker-cpu-affinity auto` (off by default) also binds each io_uring
//! worker to a CPU of its own, when the workers are exactly as many as the
//! CPUs the process may use ([`pinning`]). An io_uring worker serves its
//! connections alone: when the scheduler puts two workers on one CPU, which
//! it does at times once a third busy thread (the replication hub feeding a
//! kept backlog) shares their CPUs, every connection of one waits out the
//! other's time slice: a rate limiter's p99 went to 4-5 ms on two CPUs
//! that it otherwise kept at 0.2 ms (#894).

/// `worker-cpu-affinity`: whether each io_uring worker is bound to a CPU of
/// its own. See [`pinning`].
#[derive(clap::ValueEnum, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorkerCpuAffinity {
    /// The workers run wherever the scheduler puts them.
    #[default]
    Off,
    /// Worker N on the N-th CPU the process may use, when they are as many.
    Auto,
}

impl WorkerCpuAffinity {
    pub fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Auto => "auto",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "off" | "no" => Some(Self::Off),
            "auto" => Some(Self::Auto),
            _ => None,
        }
    }
}

/// What [`pinning`] decided.
#[derive(Debug, Clone, PartialEq)]
pub enum Pinning {
    /// Worker N on CPU `cpus[N]`.
    Pin(Vec<usize>),
    /// Not pinned, and why (for the startup line).
    Not(String),
}

impl Pinning {
    /// The startup line's value: `worker 0 on CPU 0, worker 1 on CPU 2 (...)`.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn describe(&self) -> String {
        match self {
            Pinning::Pin(cpus) => format!(
                "each worker on a CPU of its own: {} (worker-cpu-affinity auto)",
                cpus.iter()
                    .enumerate()
                    .map(|(worker, cpu)| format!("worker {worker} on CPU {cpu}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Pinning::Not(why) => format!("not pinned: {why}"),
        }
    }
}

/// What the workers' attempts to bind themselves came to: each worker reports
/// once, and the last one gets the startup line, which says what happened,
/// not what was planned (a bind refused with EPERM or EINVAL is named).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct PinReport {
    cpus: Vec<usize>,
    left: std::sync::atomic::AtomicUsize,
    failed: std::sync::Mutex<Vec<String>>,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl PinReport {
    pub fn new(cpus: Vec<usize>) -> Self {
        let left = std::sync::atomic::AtomicUsize::new(cpus.len());
        Self {
            cpus,
            left,
            failed: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// The CPU worker N is to be bound to.
    pub fn cpu(&self, worker: usize) -> usize {
        self.cpus[worker]
    }

    /// Every CPU the workers are bound to: the process's, as the rule bound
    /// them only when they are as many.
    pub fn cpus(&self) -> &[usize] {
        &self.cpus
    }

    /// Worker `worker` has tried: the startup line, once every worker has.
    pub fn tried(&self, worker: usize, result: &std::io::Result<()>) -> Option<String> {
        if let Err(e) = result {
            let mut failed = self.failed.lock().unwrap_or_else(|p| p.into_inner());
            failed.push(format!("worker {worker} on CPU {}: {e}", self.cpus[worker]));
        }
        if self.left.fetch_sub(1, std::sync::atomic::Ordering::AcqRel) != 1 {
            return None;
        }
        let failed = self.failed.lock().unwrap_or_else(|p| p.into_inner());
        let n = self.cpus.len();
        let bound = n - failed.len();
        let mut line = if failed.is_empty() {
            format!(
                "pinned {n} of {n}: {} (worker-cpu-affinity auto)",
                self.cpus
                    .iter()
                    .enumerate()
                    .map(|(worker, cpu)| format!("worker {worker} on CPU {cpu}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else if bound == 0 {
            "not pinned: the system refused every bind".to_string()
        } else {
            format!("pinned {bound} of {n} (worker-cpu-affinity auto)")
        };
        if !failed.is_empty() {
            let mut failed = failed.clone();
            failed.sort();
            line.push_str(&format!("; refused: {}", failed.join(", ")));
        }
        Some(line)
    }
}

/// Whether to bind the `workers` io_uring workers to CPUs, given the CPUs
/// the process may use (`allowed`, its affinity mask, which on Linux is
/// already cut down to its cgroup's cpuset) and the cgroup CPU quota.
///
/// Only when the workers are exactly as many as the allowed CPUs: each then
/// has one, and no CPU is left over that a pinned worker could not use. Not
/// when a CPU quota is what limits the process (Docker's `--cpus`, a
/// Kubernetes CPU limit): the quota is time on any CPU, not CPUs of its own,
/// and a bound worker could not move off a CPU another container is busy on.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn pinning(
    mode: WorkerCpuAffinity,
    workers: usize,
    allowed: Option<&[usize]>,
    quota: Option<f64>,
) -> Pinning {
    if mode == WorkerCpuAffinity::Off {
        return Pinning::Not("worker-cpu-affinity off".to_string());
    }
    let Some(allowed) = allowed.filter(|cpus| !cpus.is_empty()) else {
        return Pinning::Not("the CPUs this process may use could not be read".to_string());
    };
    if let Some(quota) = quota.filter(|q| q.is_finite() && *q > 0.0)
        && quota < allowed.len() as f64
    {
        return Pinning::Not(format!(
            "a cgroup CPU quota of {} CPUs limits this process, not CPUs of its own",
            trim(quota)
        ));
    }
    if workers != allowed.len() {
        return Pinning::Not(format!(
            "{workers} workers on {} CPUs: pinned only when they are as many",
            allowed.len()
        ));
    }
    Pinning::Pin(allowed.to_vec())
}

/// What `worker-cpu-affinity auto` does on an I/O model or system that
/// cannot pin: the startup line.
pub fn not_pinned_here(mode: WorkerCpuAffinity, tokio: bool) -> Option<String> {
    if mode == WorkerCpuAffinity::Off {
        return None;
    }
    Some(if cfg!(not(target_os = "linux")) {
        // macOS has no call that binds a thread to a CPU (only affinity
        // tags, a hint); FreeBSD's cpuset_setaffinity is not used.
        format!(
            "not pinned: worker-cpu-affinity works on Linux only, not on {}",
            std::env::consts::OS
        )
    } else if tokio {
        "not pinned: the tokio I/O model's workers take each other's connections, \
         so none waits on one held up; worker-cpu-affinity is for io_uring"
            .to_string()
    } else {
        return None;
    })
}

/// The ids of the CPUs this process may run on, lowest first.
#[cfg(target_os = "linux")]
pub fn allowed_cpu_ids() -> Option<Vec<usize>> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `set` is a valid cpu_set_t of the size passed.
    let ok =
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if ok != 0 {
        return None;
    }
    let ids: Vec<usize> = (0..libc::CPU_SETSIZE as usize)
        // SAFETY: `cpu` is below CPU_SETSIZE.
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .collect();
    (!ids.is_empty()).then_some(ids)
}

/// The CPUs the process could use before any worker was pinned, once one
/// has been: what a thread a pinned worker starts goes back to.
#[cfg(target_os = "linux")]
static ALLOWED: std::sync::OnceLock<Vec<usize>> = std::sync::OnceLock::new();

/// `std::thread::available_parallelism` as the first worker saw it before
/// it was pinned: the CPU quota is in it, which the mask alone is not.
#[cfg(target_os = "linux")]
static UNPINNED: std::sync::OnceLock<Option<std::num::NonZeroUsize>> = std::sync::OnceLock::new();

/// Set once a worker has been pinned: [`release_this_thread`] has work.
static PINNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

thread_local! {
    /// This thread has run on the process's CPUs since it started, or has
    /// been given them back: nothing for [`release_this_thread`] to do.
    static FREE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(target_os = "linux")]
fn set_affinity(cpus: &[usize]) -> std::io::Result<()> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for &cpu in cpus {
        // SAFETY: every id came from `allowed_cpu_ids`, below CPU_SETSIZE.
        unsafe { libc::CPU_SET(cpu, &mut set) };
    }
    // SAFETY: `set` is a valid cpu_set_t of the size passed; 0 is this thread.
    let ok = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if ok == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Bind the calling thread (a worker) to `cpu` of `allowed`.
///
/// A thread starts on the CPUs of the thread that starts it, so whatever a
/// pinned worker starts — the blocking pool of its runtime (a full sync's
/// dump, a script, `BGSAVE`), a helper started on first use (snapshot
/// compression, lazy free, pub/sub, vector postings) — would be held to the
/// worker's one CPU. Each such thread calls [`release_this_thread`] first.
#[cfg(target_os = "linux")]
#[allow(clippy::disallowed_methods)]
pub fn pin_this_thread(cpu: usize, allowed: &[usize]) -> std::io::Result<()> {
    // What std counts while this thread still has the process's CPUs: the
    // mask and a CPU quota (cgroup `cpu.max`, a container's `--cpus`).
    let _ = UNPINNED.get_or_init(|| std::thread::available_parallelism().ok());
    let _ = ALLOWED.set(allowed.to_vec());
    PINNED.store(true, std::sync::atomic::Ordering::Relaxed);
    FREE.with(|free| free.set(false));
    set_affinity(&[cpu])
}

/// Give the calling thread back every CPU the process may use, if a worker
/// that started it was pinned. Run first by every thread the server starts
/// (the spawn functions below); one relaxed load when nothing is pinned.
#[inline]
pub fn release_this_thread() {
    if !PINNED.load(std::sync::atomic::Ordering::Relaxed) || FREE.with(|free| free.get()) {
        return;
    }
    #[cfg(target_os = "linux")]
    if let Some(allowed) = ALLOWED.get() {
        let _ = set_affinity(allowed);
    }
    FREE.with(|free| free.set(true));
}

/// `std::thread::available_parallelism`, for the process. std reads the
/// calling thread's mask, and on a bound worker that is one CPU: a `SAVE`
/// run there froze the dataset with one thread instead of one per CPU, and
/// held writes twice as long (42 against 23 ms on two CPUs). Once a worker
/// is bound this answers with the CPUs the process may use, no more than a
/// CPU quota allows (as std counts it).
#[allow(clippy::disallowed_methods)]
pub fn available_parallelism() -> std::io::Result<std::num::NonZeroUsize> {
    #[cfg(target_os = "linux")]
    if PINNED.load(std::sync::atomic::Ordering::Relaxed)
        && let Some(n) = ALLOWED
            .get()
            .and_then(|cpus| std::num::NonZeroUsize::new(cpus.len()))
    {
        return Ok(match UNPINNED.get().copied().flatten() {
            Some(quota) => n.min(quota),
            None => n,
        });
    }
    std::thread::available_parallelism()
}

// ── Starting threads ──────────────────────────────────────────────────────
//
// Every thread the server starts goes through one of these, so that a thread
// a pinned worker starts — a `SAVE`'s freeze threads, a cluster fan-out, a
// full sync's dump, a script — is not held to that worker's one CPU:
// clippy.toml's `disallowed-methods` forbids the std and tokio calls
// anywhere else. Each runs `release_this_thread` first, then the closure.

/// `std::thread::spawn`, released.
#[allow(clippy::disallowed_methods)]
pub fn spawn<F, T>(f: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        release_this_thread();
        f()
    })
}

/// `std::thread::Builder::spawn`, released: `Builder::new().name(..).spawn_released(f)`.
pub trait BuilderExt {
    fn spawn_released<F, T>(self, f: F) -> std::io::Result<std::thread::JoinHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;
}

impl BuilderExt for std::thread::Builder {
    #[allow(clippy::disallowed_methods)]
    fn spawn_released<F, T>(self, f: F) -> std::io::Result<std::thread::JoinHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.spawn(move || {
            release_this_thread();
            f()
        })
    }
}

/// `std::thread::Scope::spawn`, released: `scope.spawn_released(f)`.
pub trait ScopeExt<'scope> {
    fn spawn_released<F, T>(self, f: F) -> std::thread::ScopedJoinHandle<'scope, T>
    where
        F: FnOnce() -> T + Send + 'scope,
        T: Send + 'scope;
}

impl<'scope, 'env> ScopeExt<'scope> for &'scope std::thread::Scope<'scope, 'env> {
    #[allow(clippy::disallowed_methods)]
    fn spawn_released<F, T>(self, f: F) -> std::thread::ScopedJoinHandle<'scope, T>
    where
        F: FnOnce() -> T + Send + 'scope,
        T: Send + 'scope,
    {
        self.spawn(move || {
            release_this_thread();
            f()
        })
    }
}

/// `tokio::task::spawn_blocking`, released.
#[allow(clippy::disallowed_methods)]
pub fn spawn_blocking<F, R>(f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        release_this_thread();
        f()
    })
}

/// `tokio::runtime::Handle::spawn_blocking`, released (scripts use it).
#[allow(clippy::disallowed_methods)]
#[cfg_attr(not(feature = "lua"), allow(dead_code))]
pub fn spawn_blocking_on<F, R>(handle: &tokio::runtime::Handle, f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    handle.spawn_blocking(move || {
        release_this_thread();
        f()
    })
}

/// What the operating system says the process may run on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cpus {
    /// CPUs online on the machine.
    pub online: usize,
    /// CPUs in the process's affinity mask, where it could be read.
    pub affinity: Option<usize>,
    /// The cgroup CPU quota in CPUs (`cpu.max` quota / period), where one is
    /// set; `None` for no limit.
    pub quota: Option<f64>,
}

/// The worker count auto picked, and a sentence saying why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auto {
    pub threads: usize,
    pub reason: String,
}

/// The CPUs the process may use: the fewest of the CPUs online, the
/// affinity mask and the quota. A fractional quota is rounded down, as std
/// rounds it: workers that together want more CPU time than the quota are
/// throttled for the rest of each period. Measured in a container with
/// `--cpus 1.5`, two workers served 45% more than one but had a p99 of
/// 27 ms at 500 connections (one: 4.7 ms); with `--cpus 2.5`, three
/// workers 3% more than two, at a p99 of 18 ms (two: 2.1 ms).
pub fn available(cpus: &Cpus) -> Auto {
    let online = cpus.online.max(1);
    let mut threads = online;
    let mut reason = format!("{online} CPUs online");
    if let Some(mask) = cpus.affinity.filter(|&n| n > 0 && n < threads) {
        threads = mask;
        reason = "its affinity mask: a cpuset or taskset".to_string();
    }
    if let Some(quota) = cpus.quota.filter(|q| q.is_finite() && *q > 0.0) {
        let whole = (quota.floor() as usize).max(1);
        if whole < threads {
            threads = whole;
            reason = format!("a cgroup CPU quota of {} CPUs", trim(quota));
        }
    }
    Auto { threads, reason }
}

/// `2.5` as `2.5`, `2.0` as `2`.
fn trim(quota: f64) -> String {
    let text = format!("{quota:.2}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// Reads [`Cpus`] for this process.
pub fn detect() -> Cpus {
    // What std counts: the affinity mask and the cgroup quota (v1 and v2,
    // the lowest up the hierarchy), the quota rounded down.
    let std_count = std::thread::available_parallelism().map(|n| n.get()).ok();
    #[cfg(target_os = "linux")]
    {
        let online = online_cpus().or(std_count).unwrap_or(1);
        let affinity = affinity_cpus();
        let mut quota = cgroup_v2_quota();
        // A limit std found that the cgroup v2 files did not show (cgroup
        // v1): take std's count as the quota.
        if quota.is_none()
            && let Some(n) = std_count
            && n < affinity.unwrap_or(online).min(online)
        {
            quota = Some(n as f64);
        }
        Cpus {
            online,
            affinity,
            quota,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Cpus {
            online: std_count.unwrap_or(1),
            affinity: None,
            quota: None,
        }
    }
}

#[cfg(target_os = "linux")]
fn online_cpus() -> Option<usize> {
    let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    usize::try_from(n).ok().filter(|&n| n > 0)
}

#[cfg(target_os = "linux")]
fn affinity_cpus() -> Option<usize> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let ok =
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if ok != 0 {
        return None;
    }
    usize::try_from(unsafe { libc::CPU_COUNT(&set) })
        .ok()
        .filter(|&n| n > 0)
}

/// The lowest `cpu.max` from this process's cgroup up to the root of the
/// cgroup v2 hierarchy at `/sys/fs/cgroup`, in CPUs.
#[cfg(target_os = "linux")]
fn cgroup_v2_quota() -> Option<f64> {
    let own = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = own.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
    let mut dir = std::path::PathBuf::from("/sys/fs/cgroup");
    dir.push(path.trim_start_matches('/'));
    let root = std::path::Path::new("/sys/fs/cgroup");
    let mut lowest: Option<f64> = None;
    loop {
        if let Ok(text) = std::fs::read_to_string(dir.join("cpu.max"))
            && let Some(cpus) = parse_cpu_max(&text)
        {
            lowest = Some(lowest.map_or(cpus, |l| l.min(cpus)));
        }
        if dir == root || !dir.pop() || !dir.starts_with(root) {
            break;
        }
    }
    lowest
}

/// `cpu.max` is `<quota> <period>` in microseconds, or `max <period>`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cpu_max(text: &str) -> Option<f64> {
    let mut parts = text.split_whitespace();
    let quota = parts.next()?.parse::<f64>().ok()?;
    let period = parts.next()?.parse::<f64>().ok()?;
    (quota > 0.0 && period > 0.0).then(|| quota / period)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auto(online: usize, affinity: Option<usize>, quota: Option<f64>) -> usize {
        available(&Cpus {
            online,
            affinity,
            quota,
        })
        .threads
    }

    #[test]
    fn every_cpu_online_without_limits() {
        assert_eq!(auto(1, None, None), 1);
        assert_eq!(auto(2, None, None), 2);
        assert_eq!(auto(4, None, None), 4);
        assert_eq!(auto(64, None, None), 64);
    }

    #[test]
    fn the_affinity_mask_caps_it() {
        assert_eq!(auto(64, Some(2), None), 2);
        assert_eq!(auto(16, Some(4), None), 4);
        // A mask as wide as the machine changes nothing.
        assert_eq!(auto(8, Some(8), None), 8);
    }

    #[test]
    fn the_quota_caps_it_rounded_down() {
        // docker run --cpus=2 on a 64-CPU host
        assert_eq!(auto(64, Some(64), Some(2.0)), 2);
        assert_eq!(auto(16, None, Some(1.5)), 1);
        assert_eq!(auto(16, None, Some(2.5)), 2);
        assert_eq!(auto(16, None, Some(0.5)), 1);
        assert_eq!(auto(16, None, Some(0.01)), 1);
        // A quota above the CPUs online does not add workers.
        assert_eq!(auto(4, None, Some(8.0)), 4);
    }

    #[test]
    fn the_lowest_of_mask_and_quota_wins() {
        assert_eq!(auto(64, Some(4), Some(2.0)), 2);
        assert_eq!(auto(64, Some(2), Some(6.0)), 2);
        assert_eq!(auto(64, Some(4), Some(3.9)), 3);
    }

    #[test]
    fn nonsense_reads_as_no_limit_and_never_zero() {
        assert_eq!(auto(0, None, None), 1);
        assert_eq!(auto(8, Some(0), None), 8);
        assert_eq!(auto(8, None, Some(0.0)), 8);
        assert_eq!(auto(8, None, Some(f64::NAN)), 8);
        assert_eq!(auto(8, None, Some(f64::INFINITY)), 8);
    }

    #[test]
    fn the_reason_names_the_limit() {
        let cpus = |online, affinity, quota| Cpus {
            online,
            affinity,
            quota,
        };
        assert_eq!(available(&cpus(8, None, None)).reason, "8 CPUs online");
        assert_eq!(
            available(&cpus(64, Some(4), None)).reason,
            "its affinity mask: a cpuset or taskset"
        );
        assert_eq!(
            available(&cpus(64, Some(64), Some(2.0))).reason,
            "a cgroup CPU quota of 2 CPUs"
        );
        assert_eq!(
            available(&cpus(64, None, Some(1.5))).reason,
            "a cgroup CPU quota of 1.5 CPUs"
        );
    }

    #[test]
    fn cpu_max_parses() {
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2.0));
        assert_eq!(parse_cpu_max("150000 100000"), Some(1.5));
        assert_eq!(parse_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cpu_max(""), None);
    }

    use WorkerCpuAffinity::{Auto, Off};

    fn pin(workers: usize, allowed: &[usize], quota: Option<f64>) -> Pinning {
        pinning(Auto, workers, Some(allowed), quota)
    }

    #[test]
    fn worker_cpu_affinity_parses() {
        assert_eq!(WorkerCpuAffinity::parse("auto"), Some(Auto));
        assert_eq!(WorkerCpuAffinity::parse("AUTO"), Some(Auto));
        assert_eq!(WorkerCpuAffinity::parse("off"), Some(Off));
        assert_eq!(WorkerCpuAffinity::parse("no"), Some(Off));
        assert_eq!(WorkerCpuAffinity::parse("yes"), None);
        assert_eq!(WorkerCpuAffinity::parse("0-3"), None);
        assert_eq!(WorkerCpuAffinity::default(), Off);
        assert_eq!(Auto.name(), "auto");
        assert_eq!(Off.name(), "off");
    }

    #[test]
    fn off_never_pins() {
        assert!(matches!(
            pinning(Off, 2, Some(&[0, 2]), None),
            Pinning::Not(_)
        ));
    }

    #[test]
    fn auto_pins_worker_n_to_the_nth_allowed_cpu_when_as_many() {
        // taskset -c 0,2 --threads 2; a cpuset 4-7 with auto threads.
        assert_eq!(pin(2, &[0, 2], None), Pinning::Pin(vec![0, 2]));
        assert_eq!(pin(4, &[4, 5, 6, 7], None), Pinning::Pin(vec![4, 5, 6, 7]));
        assert_eq!(pin(1, &[3], None), Pinning::Pin(vec![3]));
        // A quota of at least the CPUs allowed limits nothing.
        assert_eq!(pin(2, &[0, 1], Some(2.0)), Pinning::Pin(vec![0, 1]));
        assert_eq!(pin(2, &[0, 1], Some(8.0)), Pinning::Pin(vec![0, 1]));
    }

    #[test]
    fn auto_does_not_pin_more_or_fewer_workers_than_cpus() {
        let Pinning::Not(why) = pin(2, &[0, 1, 2, 3], None) else {
            panic!("pinned 2 workers on 4 CPUs")
        };
        assert_eq!(
            why,
            "2 workers on 4 CPUs: pinned only when they are as many"
        );
        assert!(matches!(pin(8, &[0, 1, 2, 3], None), Pinning::Not(_)));
    }

    #[test]
    fn auto_does_not_pin_under_a_cpu_quota() {
        // docker run --cpus 2 on an 8-CPU host: auto threads gives 2 workers,
        // and the 8 CPUs are shared time, not CPUs of their own.
        let Pinning::Not(why) = pin(2, &[0, 1, 2, 3, 4, 5, 6, 7], Some(2.0)) else {
            panic!("pinned under a quota")
        };
        assert_eq!(
            why,
            "a cgroup CPU quota of 2 CPUs limits this process, not CPUs of its own"
        );
        assert!(matches!(pin(4, &[0, 1, 2, 3], Some(3.5)), Pinning::Not(_)));
    }

    #[test]
    fn auto_without_a_readable_mask_does_not_pin() {
        assert!(matches!(pinning(Auto, 2, None, None), Pinning::Not(_)));
        assert!(matches!(pinning(Auto, 2, Some(&[]), None), Pinning::Not(_)));
    }

    #[test]
    fn the_startup_line_says_what_was_pinned() {
        assert_eq!(
            Pinning::Pin(vec![0, 2]).describe(),
            "each worker on a CPU of its own: worker 0 on CPU 0, worker 1 on CPU 2 \
             (worker-cpu-affinity auto)"
        );
        assert_eq!(
            pinning(Off, 2, Some(&[0, 2]), None).describe(),
            "not pinned: worker-cpu-affinity off"
        );
        assert_eq!(not_pinned_here(Off, true), None);
        #[cfg(target_os = "linux")]
        {
            assert!(not_pinned_here(Auto, true).unwrap().contains("tokio"));
            assert_eq!(not_pinned_here(Auto, false), None);
        }
    }

    #[test]
    fn the_pin_report_says_what_happened() {
        let ok = || Ok(());
        let refused = || Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        let all = PinReport::new(vec![0, 2]);
        assert_eq!(all.tried(1, &ok()), None);
        assert_eq!(
            all.tried(0, &ok()).unwrap(),
            "pinned 2 of 2: worker 0 on CPU 0, worker 1 on CPU 2 (worker-cpu-affinity auto)"
        );
        let one = PinReport::new(vec![0, 2]);
        assert_eq!(one.tried(0, &ok()), None);
        let line = one.tried(1, &refused()).unwrap();
        assert!(
            line.starts_with(
                "pinned 1 of 2 (worker-cpu-affinity auto); refused: worker 1 on CPU 2: "
            ),
            "{line}"
        );
        let none = PinReport::new(vec![4]);
        let line = none.tried(0, &refused()).unwrap();
        assert!(
            line.starts_with(
                "not pinned: the system refused every bind; refused: worker 0 on CPU 4"
            ),
            "{line}"
        );
    }

    /// A helper thread a pinned worker starts gets every allowed CPU back.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_thread_a_pinned_thread_starts_is_released() {
        let allowed = allowed_cpu_ids().expect("the mask");
        std::thread::spawn(move || {
            pin_this_thread(allowed[0], &allowed).unwrap();
            assert_eq!(allowed_cpu_ids().unwrap(), vec![allowed[0]]);
            let inner = allowed.clone();
            std::thread::spawn(move || {
                // Inherited: held to the worker's CPU until released.
                assert_eq!(allowed_cpu_ids().unwrap(), vec![inner[0]]);
                release_this_thread();
                assert_eq!(allowed_cpu_ids().unwrap(), inner);
            })
            .join()
            .unwrap();
            // Through the wrappers, released without asking: a thread, a
            // named one, a scoped one (a SAVE's freeze threads).
            let want = allowed.clone();
            let seen = spawn(allowed_cpu_ids).join().unwrap().unwrap();
            assert_eq!(seen, want);
            let seen = std::thread::Builder::new()
                .name("test-released".into())
                .spawn_released(allowed_cpu_ids)
                .unwrap()
                .join()
                .unwrap()
                .unwrap();
            assert_eq!(seen, want);
            let seen = std::thread::scope(|scope| {
                scope
                    .spawn_released(allowed_cpu_ids)
                    .join()
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(seen, want);
            // The pinned thread itself stays where it is.
            assert_eq!(allowed_cpu_ids().unwrap(), vec![allowed[0]]);
        })
        .join()
        .unwrap();
    }
}
