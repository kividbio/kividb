//! Giving freed memory back to the system once a lot of it was freed (#753).
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
//! fallen far below what it was ([`worth_trimming`]), it asks the allocator
//! to give back its free pages (`MEMORY PURGE`'s
//! [`purge_allocator`](crate::commands::memory::purge_allocator)). It looks
//! a few times a second and trims at most once in [`MIN_GAP`]. Whatever
//! frees the memory, a `DEL` on a client's thread, the lazyfree thread, an
//! `HDEL` a hundred fields at a time, expiry or eviction, the fall shows the
//! same, and nothing on the path of a command changes.
//!
//! A server under steady load does not trim: what it frees it allocates
//! again, and what it has allocated does not fall. A trim walks the free
//! lists of every arena, each under that arena's lock, so a thread that
//! allocates meanwhile in the arena being walked waits for it; it is paid
//! only after a fall of at least [`MIN_FALL`] bytes and an eighth of the
//! most allocated since the last trim, and not more than once in
//! [`MIN_GAP`].

use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// How often the thread looks at what is allocated.
const TICK: Duration = Duration::from_millis(250);
/// The least time between two trims.
const MIN_GAP: Duration = Duration::from_secs(1);
/// The least fall in what is allocated that is worth a trim.
const MIN_FALL: usize = 32 << 20;

/// Trims done since start: `allocator_trims` in `INFO memory`.
static TRIMS: AtomicU64 = AtomicU64::new(0);

/// Whether what is allocated, `now`, has fallen far enough below `high`, the
/// most it was since the last trim, for a trim to give back something worth
/// the walk: by [`MIN_FALL`] bytes, and by an eighth of `high`. A server of
/// 100 GB that deletes 1 GB keeps the free pages for what it writes next.
pub fn worth_trimming(high: usize, now: usize) -> bool {
    let fall = high.saturating_sub(now);
    fall >= MIN_FALL && fall >= high / 8
}

/// Start the thread that trims, once per process.
pub fn start() {
    static STARTED: Once = Once::new();
    STARTED.call_once(|| {
        let started = std::thread::Builder::new()
            .name("kividb-give-back".into())
            .spawn(run);
        if let Err(e) = started {
            crate::log_warn!("could not start the thread that gives memory back: {e}");
        }
    });
}

fn run() {
    let mut high = crate::memory_use::allocated();
    let mut last: Option<Instant> = None;
    loop {
        std::thread::sleep(TICK);
        let now = crate::memory_use::allocated();
        high = high.max(now);
        if !worth_trimming(high, now) || last.is_some_and(|at| at.elapsed() < MIN_GAP) {
            continue;
        }
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

#[cfg(test)]
mod tests {
    use super::{MIN_FALL, worth_trimming};

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
}
