//! Active defrag (#113): moving string values off the slabs that deletes
//! have left sparse, so that those slabs empty and their memory goes back to
//! the kernel (see `value_slab`).
//!
//! Nothing is collected and nothing waits for this: a value is freed when
//! its last holder lets go of it, as before. Compaction only moves values
//! that are still wanted, from a thread of its own, a batch of keys at a
//! time:
//! - under a shard's read lock it looks through [`BATCH`] entries for values
//!   in the slabs being emptied ([`value_slab::plan_evacuation`]);
//! - under the write lock it moves those it found, [`MOVES_PER_HOLD`] at a
//!   time, each a copy of a key's whole entry (up to 4 KiB, the next one
//!   asked for while it copies), and frees the old blocks once the lock is
//!   let go;
//! - and once it has worked for a [`QUANTUM`], it sleeps, long enough to
//!   keep to `active-defrag-cycle-max` per cent of one core ([`Pacer`]).
//!
//! A value that something holds besides its key (a snapshot being written, a
//! command running) is left where it is, and its slab with it.
//!
//! It starts while the slabs take more than `active-defrag-threshold-lower`
//! per cent above what is in them, and more than `active-defrag-ignore-bytes`,
//! and stops when the waste is under half of both.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::store::{Relocated, Shard, Store};
use crate::value_slab;

/// Buckets looked at under one hold of a shard's read lock (about half as
/// many entries). Each entry is read to tell where its value is, somewhere
/// else in memory: with 512 a hold took about 50 us, and a write waiting for
/// it held up the reads behind it; with 128, and the entries asked for
/// ahead, about 11.
const BATCH: usize = 128;

/// Entries moved under one hold of a shard's write lock. A command waiting
/// for the shard waits for no more than these copies.
const MOVES_PER_HOLD: usize = 32;

/// Work done between two sleeps. Slept after every batch, a pass over a
/// million keys of 1 KB slept 18,304 times, 1,920 of them after the empty
/// shards of the databases not in use; each sleep overshot by about a
/// millisecond on a VM, and each batch after one found the caches cold, so
/// the pass took 65 s, of which 5 s working (#240). Sleeping once a
/// millisecond of work is owed its rest, the thread still works no longer
/// than this (plus one batch) at a time, and rests as long in all.
const QUANTUM: Duration = Duration::from_millis(1);

static ENABLED: AtomicBool = AtomicBool::new(true);
static IGNORE_BYTES: AtomicU64 = AtomicU64::new(64 << 20);
static THRESHOLD_LOWER: AtomicU32 = AtomicU32::new(20);
static CYCLE_MAX: AtomicU32 = AtomicU32::new(10);

static RUNNING: AtomicBool = AtomicBool::new(false);
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// One compaction at a time in the process: the slabs being emptied are the
/// process's, whatever store their values are in.
static ONE_AT_A_TIME: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// `activedefrag`.
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// `active-defrag-ignore-bytes`.
pub fn ignore_bytes() -> u64 {
    IGNORE_BYTES.load(Ordering::Relaxed)
}

pub fn set_ignore_bytes(bytes: u64) {
    IGNORE_BYTES.store(bytes, Ordering::Relaxed);
}

/// `active-defrag-threshold-lower`, a percentage.
pub fn threshold_lower() -> u32 {
    THRESHOLD_LOWER.load(Ordering::Relaxed)
}

pub fn set_threshold_lower(percent: u32) {
    THRESHOLD_LOWER.store(percent, Ordering::Relaxed);
}

/// `active-defrag-cycle-max`, a percentage of one core.
pub fn cycle_max() -> u32 {
    CYCLE_MAX.load(Ordering::Relaxed)
}

pub fn set_cycle_max(percent: u32) {
    CYCLE_MAX.store(percent.clamp(1, 100), Ordering::Relaxed);
}

/// INFO's `active_defrag_running`.
pub fn running() -> bool {
    RUNNING.load(Ordering::Relaxed)
}

/// Values moved (INFO `active_defrag_hits`).
pub fn hits() -> u64 {
    HITS.load(Ordering::Relaxed)
}

/// Values found in a slab being emptied but left there, because something
/// besides their key held them (INFO `active_defrag_misses`).
pub fn misses() -> u64 {
    MISSES.load(Ordering::Relaxed)
}

/// The settings a server starts with.
pub fn configure(config: &crate::config::Config) {
    set_enabled(config.activedefrag);
    set_ignore_bytes(config.active_defrag_ignore_bytes);
    set_threshold_lower(config.active_defrag_threshold_lower);
    set_cycle_max(config.active_defrag_cycle_max);
}

/// Whether the slabs waste enough to compact: more than `ignore` bytes, and
/// more than `percent` per cent of what is in them.
fn wasting(ignore: u64, percent: u32) -> bool {
    let fill = value_slab::fill();
    let waste = fill.slab_bytes.saturating_sub(fill.live_bytes) as u64;
    waste > ignore && waste * 100 > fill.live_bytes as u64 * u64::from(percent)
}

/// Start the thread that compacts `store`'s values.
pub fn spawn(store: Store) {
    std::thread::Builder::new()
        .name("kividb-defrag".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                if enabled() && wasting(ignore_bytes(), threshold_lower()) {
                    compact(&store);
                }
            }
        })
        .expect("start the active defrag thread");
}

/// Empty the sparsest slabs, a set at a time, until the waste is under half
/// the thresholds, or a set yields nothing.
fn compact(store: &Store) {
    let _one = ONE_AT_A_TIME.lock();
    RUNNING.store(true, Ordering::Relaxed);
    let mut pacer = Pacer::default();
    while enabled() && wasting(ignore_bytes() / 2, threshold_lower() / 2) {
        // A set whose values together are an eighth of the slabs: what is
        // moved takes new room before the slabs it left are given back.
        let budget = (value_slab::slab_bytes() / 8).max(16 << 20);
        if value_slab::plan_evacuation(budget) == 0 {
            break;
        }
        let regions = value_slab::regions_snapshot();
        let before = hits();
        'all: for db in store.iter() {
            for shard in db.iter() {
                compact_shard(shard, &regions, &mut pacer);
                if !enabled() {
                    break 'all;
                }
            }
        }
        value_slab::finish_evacuation();
        if hits() == before {
            break;
        }
    }
    RUNNING.store(false, Ordering::Relaxed);
}

/// Move the values of `shard` that are in slabs being emptied, [`BATCH`]
/// buckets at a time. Returns early if active defrag is switched off.
fn compact_shard(shard: &Shard, regions: &[usize], pacer: &mut Pacer) {
    // A bucket index: entries keep their buckets while others come and go;
    // only a resize moves them, and then some are looked at twice or missed
    // until the next set, which is harmless. Where walking past the first
    // `at` entries again on every batch cost the whole shard each time.
    let mut at = 0usize;
    loop {
        let started = Instant::now();
        let looked;
        let buckets;
        // The buckets whose entry, or whose entry's string, is in a slab
        // being emptied. Looked at again under the write lock: the bucket
        // may hold another entry by then, which is checked the same way.
        let moving: Vec<usize> = {
            let map = shard.read();
            buckets = map.buckets();
            let end = (at + BATCH).min(buckets);
            looked = end.saturating_sub(at);
            map.evacuating_in(at, end, regions)
        };
        at += looked;
        for chunk in moving.chunks(MOVES_PER_HOLD) {
            let mut old = Vec::with_capacity(chunk.len());
            {
                let mut map = shard.write();
                for (i, &index) in chunk.iter().enumerate() {
                    if let Some(&next) = chunk.get(i + 1) {
                        map.prefetch_bucket(next);
                    }
                    match map.relocate_at(index, regions) {
                        Relocated::Moved(was) => old.push(was),
                        Relocated::Held => {
                            MISSES.fetch_add(1, Ordering::Relaxed);
                        }
                        Relocated::Stays => {}
                    }
                }
            }
            HITS.fetch_add(old.len() as u64, Ordering::Relaxed);
            // Freed with the shard unlocked: a slab that empties is given back
            // to the kernel, a system call.
            drop(old);
        }
        if let Some(rest) = pacer.worked(started.elapsed(), cycle_max()) {
            std::thread::sleep(rest);
            // Switched off while asleep: the rest of the shard can wait.
            if !enabled() {
                return;
            }
        }
        if at >= buckets {
            return;
        }
    }
}

/// How long the thread has worked since it last slept, and the rest it owes
/// for that work: `active-defrag-cycle-max` per cent of one core means
/// `100 - cap` units of rest for every `cap` of work. Each batch's rest is
/// worked out at the cap of its time, as when the thread slept after each.
#[derive(Default)]
struct Pacer {
    worked: Duration,
    owed: Duration,
}

impl Pacer {
    /// Count `busy` more work at `cap` per cent. Returns the rest to take
    /// now, once [`QUANTUM`] of work is done; it is then owed no more. A
    /// sleep that overshoots is not made up for: the cap is the most the
    /// thread uses, and overshooting only leaves it under.
    fn worked(&mut self, busy: Duration, cap: u32) -> Option<Duration> {
        let cap = cap.clamp(1, 100);
        self.worked = self.worked.saturating_add(busy);
        self.owed = self
            .owed
            .saturating_add(busy.saturating_mul(100 - cap) / cap);
        if self.worked < QUANTUM {
            return None;
        }
        self.worked = Duration::ZERO;
        Some(std::mem::take(&mut self.owed)).filter(|rest| !rest.is_zero())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const US: Duration = Duration::from_micros(1);

    /// The rest the thread slept after each batch before #240.
    fn per_batch(busy: Duration, cap: u32) -> Duration {
        let cap = cap.clamp(1, 100);
        busy * (100 - cap) / cap
    }

    #[test]
    fn it_rests_as_long_in_all_as_after_every_batch() {
        // Batches of every length from 0 to 400 us, at caps from 1 to 100.
        for cap in [1, 2, 10, 25, 50, 99, 100] {
            let mut pacer = Pacer::default();
            let (mut slept, mut owed_before) = (Duration::ZERO, Duration::ZERO);
            for i in 0..10_000u64 {
                let busy = US * (i * 7919 % 401) as u32;
                owed_before += per_batch(busy, cap);
                if let Some(rest) = pacer.worked(busy, cap) {
                    slept += rest;
                }
            }
            // What is still owed is under one quantum's worth.
            let left = owed_before - slept;
            assert_eq!(left, pacer.owed, "cap {cap}");
            assert!(
                left <= per_batch(QUANTUM + 400 * US, cap),
                "cap {cap}: {left:?} left"
            );
        }
    }

    #[test]
    fn it_works_no_longer_than_a_quantum_and_a_batch_between_sleeps() {
        let mut pacer = Pacer::default();
        let (mut run, mut longest) = (Duration::ZERO, Duration::ZERO);
        for i in 0..10_000u64 {
            let busy = US * (i * 104_729 % 300) as u32;
            run += busy;
            longest = longest.max(run);
            if pacer.worked(busy, 10).is_some() {
                run = Duration::ZERO;
            }
        }
        assert!(longest < QUANTUM + 300 * US, "{longest:?} without a sleep");
    }

    #[test]
    fn a_quantum_of_work_owes_its_rest_at_the_cap() {
        let mut pacer = Pacer::default();
        // 10%: nine times the work; 1%: 99 times; the most a cap of 1 makes
        // the thread wait between two quanta is under 100 ms.
        assert_eq!(pacer.worked(QUANTUM, 10), Some(9 * QUANTUM));
        assert_eq!(pacer.worked(QUANTUM, 1), Some(99 * QUANTUM));
        assert_eq!(pacer.worked(QUANTUM, 50), Some(QUANTUM));
        // At 100 (and above, clamped) nothing is owed, and nothing slept.
        assert_eq!(pacer.worked(QUANTUM, 100), None);
        assert_eq!(pacer.worked(QUANTUM, 1000), None);
        // A cap of 0 is taken as 1, not a division by zero.
        assert_eq!(pacer.worked(QUANTUM, 0), Some(99 * QUANTUM));
    }

    #[test]
    fn short_batches_sleep_once_a_quantum_not_once_each() {
        // An empty shard's batch: a lock taken and let go, well under a
        // microsecond. A thousand of them sleep at most once.
        let mut pacer = Pacer::default();
        let sleeps = (0..1000)
            .filter(|_| pacer.worked(Duration::from_nanos(500), 10).is_some())
            .count();
        assert_eq!(sleeps, 0);
        // Work of no length owes no rest, and never wakes the thread.
        let mut pacer = Pacer::default();
        assert!((0..100_000).all(|_| pacer.worked(Duration::ZERO, 10).is_none()));
        // 128-bucket batches of 25 us (1 KB values, caches warm): one sleep
        // per 40 batches where there was one per batch.
        let mut pacer = Pacer::default();
        let sleeps = (0..18_304)
            .filter(|_| pacer.worked(25 * US, 10).is_some())
            .count();
        assert_eq!(sleeps, 18_304 / 40);
    }

    #[test]
    fn each_batch_rests_at_the_cap_of_its_time() {
        // The cap changed (CONFIG SET) halfway through a quantum.
        let mut pacer = Pacer::default();
        assert_eq!(pacer.worked(QUANTUM / 2, 10), None);
        assert_eq!(
            pacer.worked(QUANTUM / 2, 50),
            Some(per_batch(QUANTUM / 2, 10) + per_batch(QUANTUM / 2, 50))
        );
    }

    #[test]
    fn nothing_owed_carries_over_a_sleep_and_nothing_overflows() {
        let mut pacer = Pacer::default();
        assert!(pacer.worked(5 * QUANTUM, 10).is_some());
        assert_eq!((pacer.worked, pacer.owed), (Duration::ZERO, Duration::ZERO));
        // A batch that took absurdly long (a stalled thread) saturates
        // instead of panicking on overflow.
        let mut pacer = Pacer::default();
        assert_eq!(pacer.worked(Duration::MAX, 1), Some(Duration::MAX));
        assert!(pacer.worked(Duration::MAX, 10).is_some());
    }
}
