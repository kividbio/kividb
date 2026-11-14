//! Giving freed memory back to the system once a lot of it was freed and
//! the server is idle (#753).
//!
//! Freeing a value only returns its memory to the allocator. The value slabs
//! (`value_slab`) give an empty slab's pages back at once, but everything
//! else, the fields of a hash, the members of a set, the tables they sit in,
//! is the system allocator's, and glibc keeps the free pages in its arenas
//! until it is asked for them. After a hash of three million fields was
//! deleted, `used_memory` was 1.4 MB and RSS 188 MiB; a `MEMORY PURGE` gave
//! it back.
//!
//! So a thread watches what the process has allocated, and when that has
//! fallen far below what it was ([`worth_trimming`]) and the server is idle
//! ([`idle`]), it asks the allocator to give back its free pages
//! (`MEMORY PURGE`'s
//! [`purge_allocator`](crate::commands::memory::purge_allocator)). Whatever
//! freed the memory, a `DEL` on a client's thread, the lazyfree thread, an
//! `HDEL` a hundred fields at a time, expiry or eviction, the fall shows the
//! same, and nothing on the path of a command changes.
//!
//! **Only when idle.** A trim walks the free lists of every arena, each
//! under that arena's lock: on a heap of a gigabyte with its free chunks
//! scattered, 80 to 130 ms for an arena. A worker thread that allocates in
//! the arena being walked waits that long, and other workers wait for it if
//! it holds a shard's lock. So the thread trims only once the server has
//! served fewer than [`IDLE_OPS`] commands over the last second and nothing
//! is waiting to be freed in the background; under load it waits, and
//! counts the trims it put off (`allocator_trims_skipped_busy`). A server
//! that is never idle keeps its free pages until a `MEMORY PURGE`.
//!
//! `allocator-give-back no` turns it off. Only a Linux build on glibc has the
//! thread: with the `mimalloc` feature, Rust's allocations do not go
//! through glibc, and a trim would give back nothing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(all(target_os = "linux", not(feature = "mimalloc")))]
use std::time::{Duration, Instant};

/// How often the thread looks at what is allocated.
#[cfg(all(target_os = "linux", not(feature = "mimalloc")))]
const TICK: Duration = Duration::from_millis(250);
/// Looks that make the last second: [`idle`] is judged over them.
#[cfg(all(target_os = "linux", not(feature = "mimalloc")))]
const TICKS_A_SECOND: usize = 4;
/// The least time between two trims.
#[cfg(all(target_os = "linux", not(feature = "mimalloc")))]
const MIN_GAP: Duration = Duration::from_secs(1);
/// The least fall in what is allocated that is worth a trim.
const MIN_FALL: usize = 32 << 20;
/// Commands in the last second under which the server counts as idle.
const IDLE_OPS: u64 = 100;

/// `allocator-give-back`.
static ENABLED: AtomicBool = AtomicBool::new(true);
/// Trims done since start: `allocator_trims` in `INFO memory`.
static TRIMS: AtomicU64 = AtomicU64::new(0);
/// Times a trim that was due was put off because the server was busy:
/// `allocator_trims_skipped_busy`. Counted once each time it starts waiting.
static SKIPPED_BUSY: AtomicU64 = AtomicU64::new(0);

/// Whether what is allocated, `now`, has fallen far enough below `high`, the
/// most it was since the last trim, for a trim to give back something worth
/// the walk: by [`MIN_FALL`] bytes, and by an eighth of `high`. A server of
/// 100 GB that deletes 1 GB keeps the free pages for what it writes next.
#[cfg_attr(
    not(all(target_os = "linux", not(feature = "mimalloc"))),
    allow(dead_code)
)]
pub fn worth_trimming(high: usize, now: usize) -> bool {
    let fall = high.saturating_sub(now);
    fall >= MIN_FALL && fall >= high / 8
}

/// Whether the server is idle enough for a trim: fewer than [`IDLE_OPS`]
/// commands served in the last second, and nothing waiting to be freed in
/// the background (more of the fall is still to come).
#[cfg_attr(
    not(all(target_os = "linux", not(feature = "mimalloc"))),
    allow(dead_code)
)]
pub fn idle(ops_last_second: u64, lazyfree_pending: u64) -> bool {
    ops_last_second < IDLE_OPS && lazyfree_pending == 0
}

/// `CONFIG SET allocator-give-back`, and the setting at startup.
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// `CONFIG GET allocator-give-back`.
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Start the thread that trims, once per process. Not on a build where a
/// trim gives nothing back (see the module's notes).
pub fn start() {
    #[cfg(all(target_os = "linux", not(feature = "mimalloc")))]
    {
        static STARTED: std::sync::Once = std::sync::Once::new();
        STARTED.call_once(|| {
            let started = std::thread::Builder::new()
                .name("kividb-give-back".into())
                .spawn(run);
            if let Err(e) = started {
                crate::log_warn!("could not start the thread that gives memory back: {e}");
            }
        });
    }
}

#[cfg(all(target_os = "linux", not(feature = "mimalloc")))]
fn run() {
    let commands = || crate::stats::TOTAL_COMMANDS_PROCESSED.total();
    // Commands served at each of the last looks, the oldest first.
    let mut seen = std::collections::VecDeque::from(vec![commands(); TICKS_A_SECOND]);
    let mut high = crate::memory_use::allocated();
    let mut last: Option<Instant> = None;
    let mut waiting = false;
    loop {
        std::thread::sleep(TICK);
        let served = commands();
        let ops = served.saturating_sub(seen.pop_front().unwrap_or(served));
        seen.push_back(served);
        let now = crate::memory_use::allocated();
        high = high.max(now);
        if !enabled() {
            // Off: what falls while it is off is not owed a trim later.
            high = now;
            waiting = false;
            continue;
        }
        if !worth_trimming(high, now) || last.is_some_and(|at| at.elapsed() < MIN_GAP) {
            continue;
        }
        if !idle(ops, crate::lazyfree::pending_objects()) {
            if !waiting {
                SKIPPED_BUSY.fetch_add(1, Ordering::Relaxed);
                waiting = true;
            }
            continue;
        }
        waiting = false;
        let began = Instant::now();
        crate::commands::memory::purge_allocator();
        TRIMS.fetch_add(1, Ordering::Relaxed);
        crate::log_verbose!(
            "Allocated fell from {} to {} bytes: gave free memory back in {:?}",
            high,
            now,
            began.elapsed()
        );
        last = Some(Instant::now());
        high = crate::memory_use::allocated();
    }
}

/// Trims done since start.
pub fn trims() -> u64 {
    TRIMS.load(Ordering::Relaxed)
}

/// Trims put off because the server was busy, since start.
pub fn skipped_busy() -> u64 {
    SKIPPED_BUSY.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::{IDLE_OPS, MIN_FALL, idle, worth_trimming};

    #[test]
    fn trims_after_a_large_fall_only() {
        let mib = 1 << 20;
        // The hash of #753: 200 MiB allocated, then 1.4 MB.
        assert!(worth_trimming(200 * mib, 1_400_000));
        // A small server whose few MiB come and go: not worth it.
        assert!(!worth_trimming(20 * mib, mib));
        assert!(!worth_trimming(MIN_FALL + mib, 2 * mib));
        // Steady load: what is freed is allocated again.
        assert!(!worth_trimming(4096 * mib, 4090 * mib));
        // A large server that drops a small share of its data keeps the pages.
        assert!(!worth_trimming(100 * 1024 * mib, 99 * 1024 * mib));
        assert!(worth_trimming(100 * 1024 * mib, 80 * 1024 * mib));
        // Allocated grew since: nothing fell.
        assert!(!worth_trimming(mib, 2 * mib));
    }

    #[test]
    fn trims_only_when_idle() {
        assert!(idle(0, 0));
        // A monitoring poll or two is idle.
        assert!(idle(IDLE_OPS - 1, 0));
        assert!(!idle(IDLE_OPS, 0));
        assert!(!idle(500_000, 0));
        // The lazyfree thread is still freeing: wait for the rest of the fall.
        assert!(!idle(0, 1));
    }
}
