// src/cmdstats.rs
//
// `INFO commandstats`: for each command, how many times it was called, how
// long those calls took in all, and how many of them failed.
//
// Every command is counted, so counting has to cost next to nothing. Each
// thread keeps its own counters — a lookup in a small table that only it
// touches, and additions to memory only it writes — and the counters of all
// threads are added up when somebody asks.

use ahash::AHashMap;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One command's counters, on one thread.
#[derive(Default)]
struct Counters {
    calls: AtomicU64,
    usec: AtomicU64,
    /// Refused before running: wrong arity, no permission, out of memory.
    rejected: AtomicU64,
    /// Ran, and answered with an error.
    failed: AtomicU64,
    /// Calls by how long they took: `buckets[i]` counts those of less than
    /// `2^i` microseconds (and, for `i > 0`, at least `2^(i-1)`).
    buckets: [AtomicU64; BUCKETS],
}

/// Up to 2^31 µs — thirty-five minutes — and anything longer in the last.
pub const BUCKETS: usize = 32;

/// The totals of one command across all threads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub calls: u64,
    pub usec: u64,
    pub rejected: u64,
    pub failed: u64,
    /// See [`Counters::buckets`].
    pub buckets: [u64; BUCKETS],
}

/// Every thread's counters, by command name (lower case, `config|get` for a
/// subcommand). A thread adds its entry for a command the first time it runs
/// it; the entries are never removed, so a thread that has gone still counts.
static ALL: Mutex<Vec<(String, Arc<Counters>)>> = Mutex::new(Vec::new());

thread_local! {
    /// This thread's counters, by command name as it arrives (upper case).
    static MINE: RefCell<AHashMap<String, Arc<Counters>>> = RefCell::new(AHashMap::new());
    /// The counters of the commands this thread ran last, found by the name
    /// packed into a word: no string is hashed for a command that was just
    /// run, which is nearly every command. An entry in `MINE` is never
    /// replaced or removed, so what is remembered here stays right.
    static RECENT: RefCell<[(u64, Option<Arc<Counters>>); RECENT_SLOTS]> =
        const { RefCell::new([const { (0, None) }; RECENT_SLOTS]) };
}

const RECENT_SLOTS: usize = 16;

/// A name of eight bytes or fewer as a word, and where in `RECENT` it goes.
/// Names have no zero byte in them, so two names never pack the same.
#[inline]
fn packed(name: &str) -> Option<(u64, usize)> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 8 {
        return None;
    }
    let mut word = [0u8; 8];
    word[..bytes.len()].copy_from_slice(bytes);
    let word = u64::from_le_bytes(word);
    let slot = (word.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 60) as usize;
    Some((word, slot))
}

/// An error that means the command was not run at all.
fn was_rejected(reply: &[u8]) -> bool {
    const REFUSALS: &[&[u8]] = &[
        b"-ERR wrong number of arguments",
        b"-ERR unknown command",
        b"-ERR unknown subcommand",
        b"-NOPERM",
        b"-NOAUTH",
        b"-OOM",
        b"-MOVED",
        b"-ASK",
        b"-CLUSTERDOWN",
        b"-READONLY",
        b"-BUSY",
        b"-LOADING",
        b"-NOREPLICAS",
    ];
    REFUSALS.iter().any(|prefix| reply.starts_with(prefix))
}

thread_local! {
    /// How long the last command run on this thread took, in microseconds.
    static LAST_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// What the command just run on this thread took, as `execute_command`
/// timed it. The connection loops ask this for the slow log instead of
/// reading the clock twice more around every command: they were a third of
/// the clock readings a `GET` made, and those 7% of a profile of it.
///
/// It is good only straight after the call, before anything else is run on
/// the thread.
#[inline]
pub fn last_command_us() -> u64 {
    LAST_US.with(|last| last.get())
}

#[inline]
pub fn note_last_command(elapsed: std::time::Duration) {
    LAST_US.with(|last| last.set(elapsed.as_micros() as u64));
}

/// Count one call. `name` is the command's name in upper case, `cmd` the
/// whole command, `reply` what it answered.
#[inline]
pub fn record(name: &str, cmd: &[&[u8]], reply: &[u8], elapsed: std::time::Duration) {
    let failed = reply.first() == Some(&b'-');
    let rejected = failed && was_rejected(reply);
    let count = |counters: &Counters| {
        // Read and written back, not added to in one step: this thread is
        // the only one that counts here, and the one step is what costs.
        // (A reset from another thread that lands between the two is undone
        // for this one count.)
        let usec = elapsed.as_micros() as u64;
        counters.calls.store(
            counters.calls.load(Ordering::Relaxed) + 1,
            Ordering::Relaxed,
        );
        counters.usec.store(
            counters.usec.load(Ordering::Relaxed) + usec,
            Ordering::Relaxed,
        );
        let bucket = (64 - usec.leading_zeros() as usize).min(BUCKETS - 1);
        let bucket = &counters.buckets[bucket];
        bucket.store(bucket.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
        if rejected {
            counters.rejected.fetch_add(1, Ordering::Relaxed);
        } else if failed {
            counters.failed.fetch_add(1, Ordering::Relaxed);
        }
    };

    // A command with subcommands is counted per subcommand, as Redis counts
    // it. That takes building the name, which the rest do not pay for.
    let has_subcommands = crate::acl::has_subcommands(name);
    let recent = if has_subcommands { None } else { packed(name) };
    if let Some((word, slot)) = recent {
        let found = RECENT.with(|recent| match &recent.borrow()[slot] {
            (remembered, Some(counters)) if *remembered == word => {
                count(counters);
                true
            }
            _ => false,
        });
        if found {
            return;
        }
    }
    let composite;
    let key: &str = match cmd.get(1) {
        Some(sub) if has_subcommands => {
            composite = format!(
                "{name}|{}",
                String::from_utf8_lossy(sub).to_ascii_uppercase()
            );
            &composite
        }
        _ => name,
    };

    let remember = |counters: &Arc<Counters>| {
        if let Some((word, slot)) = recent {
            RECENT.with(|recent| recent.borrow_mut()[slot] = (word, Some(Arc::clone(counters))));
        }
    };
    MINE.with(|mine| {
        if let Some(counters) = mine.borrow().get(key) {
            count(counters);
            remember(counters);
            return;
        }
        // First call of this command on this thread. Only real commands get
        // an entry: a client sending nonsense must not grow the table.
        if !crate::commands::server::is_registered_command(name) {
            return;
        }
        let counters = Arc::new(Counters::default());
        count(&counters);
        remember(&counters);
        ALL.lock()
            .push((key.to_ascii_lowercase(), Arc::clone(&counters)));
        mine.borrow_mut().insert(key.to_string(), counters);
    });
}

/// Every command that has been called, with its totals, sorted by name.
pub fn totals() -> Vec<(String, Totals)> {
    let mut sums: AHashMap<String, Totals> = AHashMap::new();
    for (name, counters) in ALL.lock().iter() {
        let entry = sums.entry(name.clone()).or_default();
        entry.calls += counters.calls.load(Ordering::Relaxed);
        entry.usec += counters.usec.load(Ordering::Relaxed);
        entry.rejected += counters.rejected.load(Ordering::Relaxed);
        entry.failed += counters.failed.load(Ordering::Relaxed);
        for (sum, bucket) in entry.buckets.iter_mut().zip(&counters.buckets) {
            *sum += bucket.load(Ordering::Relaxed);
        }
    }
    let mut out: Vec<(String, Totals)> = sums
        .into_iter()
        .filter(|(_, totals)| totals.calls > 0)
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The time under which `percent` per cent of the calls counted in `buckets`
/// took, in microseconds: `INFO latencystats` (#158). The buckets are powers
/// of two, so it is the top of the bucket that call falls in.
pub fn percentile_usec(buckets: &[u64; BUCKETS], percent: f64) -> f64 {
    let calls: u64 = buckets.iter().sum();
    if calls == 0 {
        return 0.0;
    }
    let wanted = ((percent / 100.0) * calls as f64).ceil().max(1.0) as u64;
    let mut so_far = 0;
    for (i, count) in buckets.iter().enumerate() {
        so_far += count;
        if so_far >= wanted {
            return (1u64 << i) as f64;
        }
    }
    (1u64 << (BUCKETS - 1)) as f64
}

/// `CONFIG RESETSTAT`.
pub fn reset() {
    for (_, counters) in ALL.lock().iter() {
        counters.calls.store(0, Ordering::Relaxed);
        counters.usec.store(0, Ordering::Relaxed);
        counters.rejected.store(0, Ordering::Relaxed);
        counters.failed.store(0, Ordering::Relaxed);
        for bucket in &counters.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn of(name: &str) -> Totals {
        totals()
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t)
            .unwrap_or_default()
    }

    #[test]
    fn calls_are_counted_per_command_across_threads() {
        // Commands no other test in this process runs, so the counts are
        // this test's alone.
        let before = of("pfdebug");
        let threads: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..250 {
                        record(
                            "PFDEBUG",
                            &[b"PFDEBUG", b"k"],
                            b"+OK\r\n",
                            Duration::from_micros(3),
                        );
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let after = of("pfdebug");
        assert_eq!(after.calls - before.calls, 1000);
        assert_eq!(after.usec - before.usec, 3000);
        assert_eq!(after.failed, before.failed);
    }

    #[test]
    fn errors_are_told_apart_and_subcommands_counted_separately() {
        let before = (of("lolwut"), of("latency|doctor"), of("latency|latest"));
        record(
            "LOLWUT",
            &[b"LOLWUT"],
            b"-WRONGTYPE Operation\r\n",
            Duration::ZERO,
        );
        record(
            "LOLWUT",
            &[b"LOLWUT"],
            b"-ERR wrong number of arguments for 'lolwut' command\r\n",
            Duration::ZERO,
        );
        record("LOLWUT", &[b"LOLWUT"], b"-NOPERM no\r\n", Duration::ZERO);
        record(
            "LATENCY",
            &[b"LATENCY", b"doctor"],
            b"+OK\r\n",
            Duration::ZERO,
        );
        record(
            "LATENCY",
            &[b"LATENCY", b"LATEST"],
            b"*0\r\n",
            Duration::ZERO,
        );
        // Not a command: not counted, and no entry made for it.
        record(
            "NOSUCHCOMMAND",
            &[b"NOSUCHCOMMAND"],
            b"-ERR unknown command\r\n",
            Duration::ZERO,
        );

        let lolwut = of("lolwut");
        assert_eq!(lolwut.calls - before.0.calls, 3);
        assert_eq!(lolwut.failed - before.0.failed, 1);
        assert_eq!(lolwut.rejected - before.0.rejected, 2);
        assert_eq!(of("latency|doctor").calls - before.1.calls, 1);
        assert_eq!(of("latency|latest").calls - before.2.calls, 1);
        assert_eq!(of("nosuchcommand"), Totals::default());
    }
}
