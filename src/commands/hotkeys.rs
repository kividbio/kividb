//! `HOTKEYS` (Redis 8.6): find the keys that cost the most.
//!
//! ```text
//! HOTKEYS START METRICS count [CPU] [NET] [COUNT k] [DURATION seconds]
//!               [SAMPLE ratio] [SLOTS count slot [slot ...]]
//! HOTKEYS STOP | GET | RESET | HELP
//! ```
//!
//! While a session is running, every command's execution time and network
//! bytes are charged to the keys it names, and `HOTKEYS GET` reports the top
//! `k` by each metric asked for.
//!
//! When no session is running — the normal state — the cost to a command is
//! one relaxed atomic load.

use crate::commands::ctx::Ctx;
use crate::resp::write::*;
use ahash::AHashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Whether a session is collecting. Checked on every command.
static ACTIVE: AtomicBool = AtomicBool::new(false);

/// The current session, or the last one until `HOTKEYS RESET`.
static SESSION: Mutex<Option<Session>> = Mutex::new(None);

const DEFAULT_TOP_K: usize = 10;
const MAX_TOP_K: usize = 1000;
const SLOT_COUNT: u16 = 16384;

#[derive(Default, Clone, Copy)]
struct Cost {
    cpu_us: u64,
    net_bytes: u64,
}

struct Session {
    track_cpu: bool,
    track_net: bool,
    top_k: usize,
    sample_ratio: u64,
    /// `SLOTS`: the only slots whose keys are tracked. `None` = all of them.
    slots: Option<Vec<u16>>,
    /// `DURATION`: when collection stops by itself.
    deadline: Option<Instant>,
    started: Instant,
    started_unix_ms: u64,
    /// Set when collection stopped; fixes the reported duration.
    stopped_after: Option<Duration>,
    cpu_at_start: (u64, u64),
    cpu_at_stop: Option<(u64, u64)>,

    keys: AHashMap<Box<[u8]>, Cost>,
    /// The key table is pruned back to this many entries when it doubles.
    capacity: usize,

    all: Cost,
    selected_slots: Cost,
    sampled_selected_slots: Cost,
}

impl Session {
    fn stop(&mut self) {
        if self.stopped_after.is_none() {
            let ran = self.started.elapsed();
            // A session that ran out its DURATION stopped then, not whenever
            // somebody next looked.
            let limit = self
                .deadline
                .map(|d| d.saturating_duration_since(self.started));
            self.stopped_after = Some(limit.map_or(ran, |limit| ran.min(limit)));
            self.cpu_at_stop = Some(process_cpu_ms());
        }
    }

    fn expired(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() >= d)
    }

    fn tracks_slot(&self, slot: u16) -> bool {
        self.slots
            .as_ref()
            .is_none_or(|slots| slots.binary_search(&slot).is_ok())
    }

    fn charge(&mut self, key: &[u8], cost: Cost) {
        match self.keys.get_mut(key) {
            Some(total) => {
                total.cpu_us += cost.cpu_us;
                total.net_bytes += cost.net_bytes;
            }
            None => {
                self.keys.insert(key.into(), cost);
                if self.keys.len() > self.capacity * 2 {
                    self.prune();
                }
            }
        }
    }

    /// Keep the heaviest `capacity` keys by either metric. A key that falls
    /// out loses what it had accumulated; with a table many times larger than
    /// `k`, a key that belongs in the top `k` does not fall out.
    fn prune(&mut self) {
        let mut entries: Vec<(Box<[u8]>, Cost)> = self.keys.drain().collect();
        entries.sort_unstable_by_key(|(_, c)| std::cmp::Reverse(c.cpu_us.max(c.net_bytes)));
        entries.truncate(self.capacity);
        self.keys.extend(entries);
    }

    fn top(&self, by: impl Fn(&Cost) -> u64) -> Vec<(&[u8], u64)> {
        let mut ranked: Vec<(&[u8], u64)> = self
            .keys
            .iter()
            .map(|(key, cost)| (key.as_ref(), by(cost)))
            .filter(|(_, n)| *n > 0)
            .collect();
        // Heaviest first; ties by name so the order is stable.
        ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        ranked.truncate(self.top_k);
        ranked
    }
}

/// User and system CPU time this process has used, in milliseconds.
fn process_cpu_ms() -> (u64, u64) {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage fills the struct it is given and reports failure
        // through its return value.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
            let usage = unsafe { usage.assume_init() };
            let ms = |t: libc::timeval| t.tv_sec as u64 * 1000 + t.tv_usec as u64 / 1000;
            return (ms(usage.ru_utime), ms(usage.ru_stime));
        }
    }
    (0, 0)
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A cheap per-thread random number, for `SAMPLE`.
fn next_random() -> u64 {
    thread_local! {
        static STATE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            // Seeded from the address of the thread-local and the clock.
            x = (state as *const _ as u64) ^ unix_ms() ^ 0x9e37_79b9_7f4a_7c15;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    })
}

/// Whether a session is collecting: the check the command path makes.
#[inline]
pub(crate) fn active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Charge the command that just ran to the keys it named.
pub(crate) fn record(ctx: &Ctx, name: &str, reply_len: usize, elapsed: Duration) {
    // The tracker's own commands are not workload.
    if name == "HOTKEYS" {
        return;
    }
    let cost = Cost {
        cpu_us: elapsed.as_micros() as u64,
        // The request as it arrived on the wire, plus the reply.
        net_bytes: (ctx.cmd.iter().map(|arg| arg.len() + 16).sum::<usize>() + reply_len) as u64,
    };

    let mut guard = SESSION.lock();
    let Some(session) = guard.as_mut() else {
        return;
    };
    if session.stopped_after.is_some() {
        return;
    }
    if session.expired() {
        session.stop();
        ACTIVE.store(false, Ordering::Relaxed);
        return;
    }

    session.all.cpu_us += cost.cpu_us;
    session.all.net_bytes += cost.net_bytes;

    let keys = crate::keyspec::key_positions(name, &ctx.cmd);
    let mut in_selected = false;
    let mut sampled = false;
    for pos in keys.iter() {
        let key = ctx.cmd[pos];
        if session.slots.is_some() && !session.tracks_slot(crate::cluster::key_hash_slot(key)) {
            continue;
        }
        in_selected = true;
        if session.sample_ratio > 1 && !next_random().is_multiple_of(session.sample_ratio) {
            continue;
        }
        sampled = true;
        session.charge(key, cost);
    }
    if session.slots.is_some() && in_selected {
        session.selected_slots.cpu_us += cost.cpu_us;
        session.selected_slots.net_bytes += cost.net_bytes;
        if sampled {
            session.sampled_selected_slots.cpu_us += cost.cpu_us;
            session.sampled_selected_slots.net_bytes += cost.net_bytes;
        }
    }
}

/// `slots` (sorted, distinct) as inclusive `[start, end]` ranges.
fn slot_ranges(slots: &[u16]) -> Vec<(u16, u16)> {
    let mut ranges: Vec<(u16, u16)> = Vec::new();
    for &slot in slots {
        match ranges.last_mut() {
            Some((_, end)) if *end + 1 == slot => *end = slot,
            _ => ranges.push((slot, slot)),
        }
    }
    ranges
}

struct StartArgs {
    track_cpu: bool,
    track_net: bool,
    top_k: usize,
    duration: Option<Duration>,
    sample_ratio: u64,
    slots: Option<Vec<u16>>,
}

/// Parse everything after `HOTKEYS START`.
fn parse_start(args: &[&[u8]], cluster_enabled: bool) -> Result<StartArgs, String> {
    let number = |arg: Option<&&[u8]>| -> Option<u64> {
        arg.and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|s| s.parse().ok())
    };
    let is = |arg: &[u8], word: &str| arg.eq_ignore_ascii_case(word.as_bytes());

    let mut parsed = StartArgs {
        track_cpu: false,
        track_net: false,
        top_k: DEFAULT_TOP_K,
        duration: None,
        sample_ratio: 1,
        slots: None,
    };
    let mut metrics_given = false;
    let mut i = 0;
    while i < args.len() {
        if is(args[i], "METRICS") {
            let count = number(args.get(i + 1))
                .filter(|n| (1..=2).contains(n))
                .ok_or("ERR METRICS count must be 1 or 2")? as usize;
            for metric in args.get(i + 2..i + 2 + count).ok_or("ERR syntax error")? {
                if is(metric, "CPU") && !parsed.track_cpu {
                    parsed.track_cpu = true;
                } else if is(metric, "NET") && !parsed.track_net {
                    parsed.track_net = true;
                } else {
                    return Err("ERR METRICS must be CPU and/or NET, each at most once".into());
                }
            }
            metrics_given = true;
            i += 2 + count;
        } else if is(args[i], "COUNT") {
            parsed.top_k = number(args.get(i + 1))
                .filter(|n| (1..=MAX_TOP_K as u64).contains(n))
                .ok_or_else(|| format!("ERR COUNT must be between 1 and {MAX_TOP_K}"))?
                as usize;
            i += 2;
        } else if is(args[i], "DURATION") {
            let seconds = number(args.get(i + 1))
                .filter(|n| *n > 0)
                .ok_or("ERR DURATION must be a positive number of seconds")?;
            parsed.duration = Some(Duration::from_secs(seconds));
            i += 2;
        } else if is(args[i], "SAMPLE") {
            parsed.sample_ratio = number(args.get(i + 1))
                .filter(|n| *n > 0)
                .ok_or("ERR SAMPLE ratio must be a positive integer")?;
            i += 2;
        } else if is(args[i], "SLOTS") {
            if !cluster_enabled {
                return Err("ERR SLOTS can only be used in cluster mode".into());
            }
            let count = number(args.get(i + 1))
                .filter(|n| *n > 0)
                .ok_or("ERR SLOTS count must be a positive integer")?
                as usize;
            let mut slots = Vec::with_capacity(count.min(SLOT_COUNT as usize));
            for slot in args.get(i + 2..i + 2 + count).ok_or("ERR syntax error")? {
                let slot = number(Some(slot))
                    .filter(|n| *n < SLOT_COUNT as u64)
                    .ok_or("ERR Invalid or out of range slot")?;
                slots.push(slot as u16);
            }
            slots.sort_unstable();
            slots.dedup();
            parsed.slots = Some(slots);
            i += 2 + count;
        } else {
            return Err("ERR syntax error".into());
        }
    }
    if !metrics_given {
        return Err("ERR METRICS is required".into());
    }
    Ok(parsed)
}

fn start(ctx: &Ctx) -> Vec<u8> {
    let args = match parse_start(&ctx.cmd[2..], ctx.cluster.enabled) {
        Ok(args) => args,
        Err(e) => return resp_error(&e),
    };
    let mut guard = SESSION.lock();
    if guard
        .as_ref()
        .is_some_and(|s| s.stopped_after.is_none() && !s.expired())
    {
        return resp_error("ERR hotkeys tracking is already in progress");
    }
    let started = Instant::now();
    // A new session replaces whatever the last one left behind.
    *guard = Some(Session {
        track_cpu: args.track_cpu,
        track_net: args.track_net,
        top_k: args.top_k,
        sample_ratio: args.sample_ratio,
        slots: args.slots,
        deadline: args.duration.map(|d| started + d),
        started,
        started_unix_ms: unix_ms(),
        stopped_after: None,
        cpu_at_start: process_cpu_ms(),
        cpu_at_stop: None,
        keys: AHashMap::new(),
        capacity: (args.top_k * 32).max(1024),
        all: Cost::default(),
        selected_slots: Cost::default(),
        sampled_selected_slots: Cost::default(),
    });
    ACTIVE.store(true, Ordering::Relaxed);
    resp_simple("OK")
}

fn stop() -> Vec<u8> {
    let mut guard = SESSION.lock();
    match guard.as_mut() {
        Some(session) if session.stopped_after.is_none() => {
            session.stop();
            ACTIVE.store(false, Ordering::Relaxed);
            resp_simple("OK")
        }
        _ => resp_error("ERR hotkeys tracking is not active"),
    }
}

fn reset() -> Vec<u8> {
    let mut guard = SESSION.lock();
    if let Some(session) = guard.as_mut()
        && session.stopped_after.is_none()
    {
        if !session.expired() {
            return resp_error(
                "ERR hotkeys tracking is active; stop it with HOTKEYS STOP before resetting",
            );
        }
        session.stop();
    }
    *guard = None;
    ACTIVE.store(false, Ordering::Relaxed);
    resp_simple("OK")
}

fn get(ctx: &Ctx) -> Vec<u8> {
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let mut guard = SESSION.lock();
    let Some(session) = guard.as_mut() else {
        return resp_null_for(resp);
    };
    // A session whose DURATION has passed is over, whether or not a command
    // has come along since to notice.
    if session.stopped_after.is_none() && session.expired() {
        session.stop();
        ACTIVE.store(false, Ordering::Relaxed);
    }

    let int = |n: u64| resp_integer(n.min(i64::MAX as u64) as i64);
    let mut fields: Vec<Vec<u8>> = Vec::new();
    let mut field = |name: &str, value: Vec<u8>| {
        fields.push(resp_bulk_str(name));
        fields.push(value);
    };
    let sampling = session.sample_ratio > 1;
    let slots = session.slots.as_deref();

    field(
        "tracking-active",
        int(session.stopped_after.is_none() as u64),
    );
    field("sample-ratio", int(session.sample_ratio));
    let ranges = match slots {
        Some(slots) => slot_ranges(slots),
        None => vec![(0, SLOT_COUNT - 1)],
    };
    field(
        "selected-slots",
        resp_array(
            ranges
                .iter()
                .map(|(a, b)| resp_array(vec![int(*a as u64), int(*b as u64)]))
                .collect(),
        ),
    );
    if slots.is_some() {
        if sampling {
            field(
                "sampled-commands-selected-slots-us",
                int(session.sampled_selected_slots.cpu_us),
            );
        }
        field(
            "all-commands-selected-slots-us",
            int(session.selected_slots.cpu_us),
        );
    }
    field("all-commands-all-slots-us", int(session.all.cpu_us));
    if slots.is_some() {
        if sampling {
            field(
                "net-bytes-sampled-commands-selected-slots",
                int(session.sampled_selected_slots.net_bytes),
            );
        }
        field(
            "net-bytes-all-commands-selected-slots",
            int(session.selected_slots.net_bytes),
        );
    }
    field(
        "net-bytes-all-commands-all-slots",
        int(session.all.net_bytes),
    );
    field(
        "collection-start-time-unix-ms",
        int(session.started_unix_ms),
    );
    let ran = session
        .stopped_after
        .unwrap_or_else(|| session.started.elapsed());
    field("collection-duration-ms", int(ran.as_millis() as u64));

    if session.track_cpu {
        let now = session.cpu_at_stop.unwrap_or_else(process_cpu_ms);
        field(
            "total-cpu-time-user-ms",
            int(now.0.saturating_sub(session.cpu_at_start.0)),
        );
        field(
            "total-cpu-time-sys-ms",
            int(now.1.saturating_sub(session.cpu_at_start.1)),
        );
    }
    if session.track_net {
        field("total-net-bytes", int(session.all.net_bytes));
    }
    let ranking = |ranked: Vec<(&[u8], u64)>| {
        resp_array(
            ranked
                .into_iter()
                .flat_map(|(key, n)| [resp_bulk(key), int(n)])
                .collect(),
        )
    };
    if session.track_cpu {
        field("by-cpu-time-us", ranking(session.top(|c| c.cpu_us)));
    }
    if session.track_net {
        field("by-net-bytes", ranking(session.top(|c| c.net_bytes)));
    }

    // One element: the fields as a flat list (RESP2) or a map (RESP3).
    resp_array(vec![resp_map_for(resp, fields)])
}

pub(super) fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if name != "HOTKEYS" {
        return None;
    }
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hotkeys' command",
        ));
    }
    let sub = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();
    let exactly_two = |reply: Vec<u8>| {
        if cmd.len() == 2 {
            reply
        } else {
            resp_error(&format!(
                "ERR wrong number of arguments for 'hotkeys|{}' command",
                sub.to_ascii_lowercase()
            ))
        }
    };
    Some(match sub.as_str() {
        "START" => start(ctx),
        "STOP" if cmd.len() == 2 => stop(),
        "GET" if cmd.len() == 2 => get(ctx),
        "RESET" if cmd.len() == 2 => reset(),
        "STOP" | "GET" | "RESET" => exactly_two(Vec::new()),
        "HELP" => resp_array(
            [
                "HOTKEYS <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "START METRICS <count> [CPU] [NET] [COUNT <k>] [DURATION <seconds>]",
                "      [SAMPLE <ratio>] [SLOTS <count> <slot> [<slot> ...]]",
                "    Start tracking the keys that use the most CPU time and/or network bytes.",
                "STOP",
                "    Stop tracking, keeping the results.",
                "GET",
                "    Return the results of the current or most recent session.",
                "RESET",
                "    Discard the results of a stopped session.",
                "HELP",
                "    Print this help.",
            ]
            .iter()
            .map(|line| resp_simple(line))
            .collect(),
        ),
        other => resp_error(&format!(
            "ERR unknown subcommand '{other}'. Try HOTKEYS HELP."
        )),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(tokens: &[&str]) -> Result<StartArgs, String> {
        let tokens: Vec<&[u8]> = tokens.iter().map(|t| t.as_bytes()).collect();
        parse_start(&tokens, false)
    }

    #[test]
    fn start_arguments() {
        let a = args(&[
            "METRICS", "2", "CPU", "NET", "COUNT", "5", "DURATION", "30", "SAMPLE", "4",
        ])
        .unwrap();
        assert!(a.track_cpu && a.track_net);
        assert_eq!(a.top_k, 5);
        assert_eq!(a.duration, Some(Duration::from_secs(30)));
        assert_eq!(a.sample_ratio, 4);

        let a = args(&["metrics", "1", "net"]).unwrap();
        assert!(!a.track_cpu && a.track_net);
        assert_eq!(a.top_k, DEFAULT_TOP_K);
        assert_eq!(a.sample_ratio, 1);

        assert!(args(&[]).is_err(), "METRICS is required");
        assert!(args(&["COUNT", "5"]).is_err());
        assert!(args(&["METRICS", "0"]).is_err());
        assert!(args(&["METRICS", "2", "CPU"]).is_err());
        assert!(args(&["METRICS", "2", "CPU", "CPU"]).is_err());
        assert!(args(&["METRICS", "1", "DISK"]).is_err());
        assert!(args(&["METRICS", "1", "CPU", "COUNT", "0"]).is_err());
        assert!(args(&["METRICS", "1", "CPU", "SAMPLE", "0"]).is_err());
        assert!(args(&["METRICS", "1", "CPU", "BOGUS"]).is_err());
    }

    #[test]
    fn slots_need_cluster_mode_and_are_validated() {
        let tokens =
            |t: &[&str]| -> Vec<Vec<u8>> { t.iter().map(|s| s.as_bytes().to_vec()).collect() };
        let parse = |t: &[&str], cluster| {
            let owned = tokens(t);
            let refs: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
            parse_start(&refs, cluster)
        };
        let base = ["METRICS", "1", "CPU", "SLOTS", "3", "5", "4", "9"];
        assert!(parse(&base, false).is_err());
        let a = parse(&base, true).unwrap();
        assert_eq!(a.slots, Some(vec![4, 5, 9]));
        assert!(parse(&["METRICS", "1", "CPU", "SLOTS", "1", "16384"], true).is_err());
        assert!(parse(&["METRICS", "1", "CPU", "SLOTS", "2", "1"], true).is_err());
    }

    #[test]
    fn slots_are_reported_as_ranges() {
        assert_eq!(slot_ranges(&[4, 5, 9]), [(4, 5), (9, 9)]);
        assert_eq!(slot_ranges(&[0, 1, 2, 3]), [(0, 3)]);
        assert!(slot_ranges(&[]).is_empty());
    }

    fn session(top_k: usize, capacity: usize) -> Session {
        Session {
            track_cpu: true,
            track_net: true,
            top_k,
            sample_ratio: 1,
            slots: None,
            deadline: None,
            started: Instant::now(),
            started_unix_ms: 0,
            stopped_after: None,
            cpu_at_start: (0, 0),
            cpu_at_stop: None,
            keys: AHashMap::new(),
            capacity,
            all: Cost::default(),
            selected_slots: Cost::default(),
            sampled_selected_slots: Cost::default(),
        }
    }

    #[test]
    fn the_heaviest_keys_come_first_by_each_metric() {
        let mut s = session(2, 100);
        s.charge(
            b"a",
            Cost {
                cpu_us: 10,
                net_bytes: 1,
            },
        );
        s.charge(
            b"b",
            Cost {
                cpu_us: 5,
                net_bytes: 50,
            },
        );
        s.charge(
            b"c",
            Cost {
                cpu_us: 1,
                net_bytes: 9,
            },
        );
        s.charge(
            b"a",
            Cost {
                cpu_us: 10,
                net_bytes: 1,
            },
        );

        let cpu = s.top(|c| c.cpu_us);
        assert_eq!(cpu, [(&b"a"[..], 20), (&b"b"[..], 5)]);
        let net = s.top(|c| c.net_bytes);
        assert_eq!(net, [(&b"b"[..], 50), (&b"c"[..], 9)]);
    }

    #[test]
    fn the_key_table_is_bounded_and_keeps_the_heavy_keys() {
        let mut s = session(2, 8);
        s.charge(
            b"heavy",
            Cost {
                cpu_us: 1_000_000,
                net_bytes: 1,
            },
        );
        for i in 0..1000u32 {
            s.charge(
                format!("light:{i}").as_bytes(),
                Cost {
                    cpu_us: 1,
                    net_bytes: 1,
                },
            );
        }
        assert!(s.keys.len() <= 16, "{} keys", s.keys.len());
        assert_eq!(s.top(|c| c.cpu_us)[0], (&b"heavy"[..], 1_000_000));
    }

    #[test]
    fn a_session_that_ran_out_its_duration_stopped_then() {
        let mut s = session(1, 8);
        s.started = Instant::now() - Duration::from_secs(10);
        s.deadline = Some(s.started + Duration::from_secs(2));
        assert!(s.expired());
        s.stop();
        assert_eq!(s.stopped_after, Some(Duration::from_secs(2)));
    }
}
