//! How many worker threads `--threads 0` (auto) starts, and why.
//!
//! Auto is one worker per CPU the process may actually run on: the CPUs
//! online, cut down to the affinity mask (`taskset`, a cpuset, Docker's
//! `--cpuset-cpus`, Kubernetes' static CPU manager) and to the cgroup CPU
//! quota (Docker's `--cpus`, a Kubernetes CPU limit), at least one.
//!
//! Up to 1.2 auto was the CPUs minus two (all of them below four): two
//! workers on a 4-CPU machine. With the server on 4 vCPUs of 4 cores that
//! served 30% less than four workers on io_uring and 44% less on tokio,
//! whose p99 it multiplied by six (#873). No other thread of the server
//! needs a CPU of its own: the AOF writer, the replication hub, expiry and a
//! background save run beside the workers, and a worker fewer than the CPUs
//! was not consistently better with any of them running.

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
        reason = format!("{mask} of the {online} CPUs online in its affinity mask (cpuset)");
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
    let ok = unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
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
            "4 of the 64 CPUs online in its affinity mask (cpuset)"
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
}
