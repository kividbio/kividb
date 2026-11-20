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
//! **Or when RSS has drifted far above what is allocated** (#245), without
//! anything falling: memory that was allocated and freed between two looks,
//! never seen as a fall, or a fall too small for a trim. A replica that
//! has applied a heavy stream is one: everything it allocates on the way to
//! applying a write is allocated on its one applier thread, so in one
//! arena, which after 2.5M writes of 256 B was 128 MiB with 118 MiB of it
//! free; a master after the same load kept 20 to 37 MiB that a `MEMORY
//! PURGE` gave back. So once a second, while idle, the thread compares RSS
//! with what is allocated ([`gap_worth_trimming`]). The gap a trim leaves
//! (slab blocks left empty, what the allocator cannot return) is the floor
//! the next one is measured from, so a server whose gap a trim cannot close
//! is not trimmed again for it.
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

#[cfg(all(target_os = "linux", not(feature = "mimalloc")))]
use crate::cpus::BuilderExt as _;
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
/// The least growth of RSS over what is allocated, past what the last trim
/// left, that is worth a trim.
const MIN_DRIFT: usize = 16 << 20;
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

/// Whether RSS has drifted far enough above what is allocated for a trim to
/// give back something worth the walk, nothing having fallen: the gap
/// between them past `floor`, the gap the last trim left, by [`MIN_DRIFT`]
/// bytes and by a sixteenth of what is allocated. Lower than for a fall:
/// a trim made for a drift is made once, and the floor keeps it from being
/// made again until RSS has drifted as far once more.
#[cfg_attr(
    not(all(target_os = "linux", not(feature = "mimalloc"))),
    allow(dead_code)
)]
pub fn gap_worth_trimming(rss: usize, allocated: usize, floor: usize) -> bool {
    let drift = rss.saturating_sub(allocated).saturating_sub(floor);
    drift >= MIN_DRIFT && drift >= allocated / 16
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
                .spawn_released(run);
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
    let rss = || crate::commands::memory::read_proc_status().0;
    // The gap between RSS and what is allocated that the last trim left.
    let mut floor = 0usize;
    let mut ticks = 0usize;
    loop {
        std::thread::sleep(TICK);
        ticks = ticks.wrapping_add(1);
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
        if last.is_some_and(|at| at.elapsed() < MIN_GAP) {
            continue;
        }
        let fell = worth_trimming(high, now);
        // RSS is read once a second, and only while idle: a drift is only
        // ever acted on then.
        let mut drifted = None;
        if !fell
            && ticks.is_multiple_of(TICKS_A_SECOND)
            && idle(ops, crate::lazyfree::pending_objects())
        {
            let rss = rss();
            floor = floor.min(rss.saturating_sub(now));
            if gap_worth_trimming(rss, now, floor) {
                drifted = Some(rss);
            }
        }
        if !fell && drifted.is_none() {
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
        match drifted {
            Some(before) => crate::log_verbose!(
                "RSS {} bytes, {} allocated: gave free memory back in {:?}",
                before,
                now,
                began.elapsed()
            ),
            None => crate::log_verbose!(
                "Allocated fell from {} to {} bytes: gave free memory back in {:?}",
                high,
                now,
                began.elapsed()
            ),
        }
        last = Some(Instant::now());
        high = crate::memory_use::allocated();
        floor = rss().saturating_sub(high);
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
    use super::{IDLE_OPS, MIN_DRIFT, MIN_FALL, gap_worth_trimming, idle, worth_trimming};

    #[test]
    fn trims_a_large_drift_of_rss_past_the_last_trims_gap_only() {
        let mib = 1 << 20;
        // The replica of #245: 316 MiB allocated, RSS 1.35 times that.
        assert!(gap_worth_trimming(427 * mib, 316 * mib, 0));
        // Once a trim left 100 MiB it cannot give back, the same RSS is no
        // longer worth one; 20 MiB more past it is.
        assert!(!gap_worth_trimming(427 * mib, 316 * mib, 100 * mib));
        assert!(gap_worth_trimming(
            (316 + 100 + 20) * mib,
            316 * mib,
            100 * mib
        ));
        // A small server: 16 MiB, not a sixteenth.
        assert!(!gap_worth_trimming(20 * mib + MIN_DRIFT - 1, 20 * mib, 0));
        assert!(gap_worth_trimming(20 * mib + MIN_DRIFT, 20 * mib, 0));
        // Less than 16 MiB over: not worth it.
        assert!(!gap_worth_trimming(331 * mib, 316 * mib, 0));
        // Less than a sixteenth of what is allocated: not worth it.
        assert!(!gap_worth_trimming(1084 * mib, 1024 * mib, 0));
        assert!(gap_worth_trimming(1088 * mib, 1024 * mib, 0));
        // A large server: a sixteenth of what is allocated, not 16 MiB.
        assert!(!gap_worth_trimming(
            100 * 1024 * mib + 512 * mib,
            100 * 1024 * mib,
            0
        ));
        assert!(gap_worth_trimming(130 * 1024 * mib, 100 * 1024 * mib, 0));
        // RSS below what is allocated (pages not touched yet): nothing to do.
        assert!(!gap_worth_trimming(mib, 2 * mib, 0));
    }

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
