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
//! - under the write lock it moves those it found, each a copy of at most
//!   512 bytes, and frees the old blocks once the lock is let go;
//! - then it sleeps, long enough to keep to `active-defrag-cycle-max` per
//!   cent of one core.
//!
//! A value that something holds besides its key (a snapshot being written, a
//! command running) is left where it is, and its slab with it.
//!
//! It starts while the slabs take more than `active-defrag-threshold-lower`
//! per cent above what is in them, and more than `active-defrag-ignore-bytes`,
//! and stops when the waste is under half of both.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::store::{Key, Shard, Store};
use crate::types::KiviDBValue;
use crate::value_slab;

/// Entries looked at under one hold of a shard's read lock, and the most
/// values moved under one hold of its write lock.
const BATCH: usize = 256;

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
                compact_shard(shard, &regions);
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
/// entries at a time.
fn compact_shard(shard: &Shard, regions: &[usize]) {
    let mut at = 0usize;
    loop {
        let started = Instant::now();
        let mut looked = 0usize;
        let moving: Vec<Key> = {
            let map = shard.read();
            // Entries keep their places in the table while others come and
            // go; only a resize moves them, and then some are looked at twice
            // or missed until the next set, which is harmless.
            map.iter()
                .skip(at)
                .take(BATCH)
                .inspect(|_| looked += 1)
                .filter(|(_, value)| {
                    matches!(value, KiviDBValue::String(bytes)
                        if value_slab::is_evacuating(bytes.block(), regions))
                })
                .map(|(key, _)| key.clone())
                .collect()
        };
        at += looked;
        if !moving.is_empty() {
            let mut old = Vec::with_capacity(moving.len());
            {
                let mut map = shard.write();
                for key in &moving {
                    if let Some(KiviDBValue::String(bytes)) = map.get_mut(key.as_str()) {
                        match bytes.relocate(regions) {
                            Some(was) => old.push(was),
                            None => {
                                MISSES.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            HITS.fetch_add(old.len() as u64, Ordering::Relaxed);
            // Freed with the shard unlocked: a slab that empties is given back
            // to the kernel, a system call.
            drop(old);
        }
        rest(started.elapsed());
        if looked < BATCH {
            return;
        }
    }
}

/// Sleep after `busy`, long enough to keep to `active-defrag-cycle-max`.
fn rest(busy: Duration) {
    let cap = cycle_max().clamp(1, 100);
    let rest = busy * (100 - cap) / cap;
    if !rest.is_zero() {
        std::thread::sleep(rest);
    }
}
