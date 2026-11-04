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
//! - then it sleeps, long enough to keep to `active-defrag-cycle-max` per
//!   cent of one core.
//!
//! A value that something holds besides its key (a snapshot being written, a
//! command running) is left where it is, and its slab with it.
//!
//! Only slab blocks move: an entry, and a string's bytes. A collection's
//! entry holds just its `Arc`; what that points to, and everything inside it
//! (a set's table, or its array of integers, #297), is the system
//! allocator's and stays where it is, on purpose. Copying the entry copies
//! the pointer, so a command holding the collection, or about to, finds it
//! unchanged.
//!
//! It starts while the slabs take more than `active-defrag-threshold-lower`
//! per cent above what is in them, and more than `active-defrag-ignore-bytes`,
//! and stops when the waste is under half of both.
//!
//! **A pass that panics** (#358) does not end the thread. Release builds
//! unwind, and a panic there used to end this thread alone: the server went
//! on, `active_defrag_running` stayed 1, the slabs being emptied were never
//! listed again, and nothing was compacted until a restart. Now each pass
//! runs in `catch_unwind`. Nothing it leaves is half done: the locks it held
//! are `parking_lot`'s, which a panic does not poison, and are let go of as
//! it unwinds; a key whose entry was being copied still points to its old
//! block, the copy freed (`Moving`, #356); an entry or string already moved
//! is where its key points, and the block it left is freed. What a panic
//! does leave is the slabs the pass was emptying: they are put back as a
//! pass that ends puts them back ([`value_slab::finish_evacuation`]), still
//! under [`ONE_AT_A_TIME`]. The panic is logged, and the next pass comes
//! after a wait that doubles with each panic in a row. After
//! [`PANICS_TO_TURN_OFF`] in a row, with no pass ending between them, active
//! defrag turns itself off (`activedefrag no`, INFO
//! `active_defrag_disabled_by_panics:1`) until it is turned on again.

use std::panic::{AssertUnwindSafe, catch_unwind};
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

/// How often the thread looks at the slabs.
const TICK: Duration = Duration::from_secs(1);

/// The wait after a pass that panicked, doubled for each panic in a row
/// before it, up to [`BACKOFF_MAX`]: 1, 2, 4 and 8 seconds.
const BACKOFF: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Passes in a row that panic, with none ending between them, after which
/// active defrag turns itself off: a panic that comes back every time is a
/// bug that retrying will not get past.
const PANICS_TO_TURN_OFF: u32 = 5;

static ENABLED: AtomicBool = AtomicBool::new(true);
static IGNORE_BYTES: AtomicU64 = AtomicU64::new(64 << 20);
static THRESHOLD_LOWER: AtomicU32 = AtomicU32::new(20);
static CYCLE_MAX: AtomicU32 = AtomicU32::new(10);

static RUNNING: AtomicBool = AtomicBool::new(false);
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// Passes that panicked (INFO `active_defrag_panics`).
static PANICS: AtomicU64 = AtomicU64::new(0);
/// Passes that panicked since the last that ended, or since active defrag
/// was last turned on.
static PANICS_IN_A_ROW: AtomicU32 = AtomicU32::new(0);
/// Whether active defrag turned itself off after panics, and has not been
/// turned on since (INFO `active_defrag_disabled_by_panics`).
static OFF_BY_PANICS: AtomicBool = AtomicBool::new(false);
/// Taken to turn active defrag on or off, so that a `CONFIG SET` and its
/// turning itself off do not cross: never by `enabled()`.
static SWITCH: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// One compaction at a time in the process: the slabs being emptied are the
/// process's, whatever store their values are in.
static ONE_AT_A_TIME: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// `activedefrag`.
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// `activedefrag`. Turned on, it starts afresh: whatever panics there were
/// before, the next pass is the first.
pub fn set_enabled(on: bool) {
    let _switch = SWITCH.lock();
    ENABLED.store(on, Ordering::Relaxed);
    if on {
        PANICS_IN_A_ROW.store(0, Ordering::Relaxed);
        OFF_BY_PANICS.store(false, Ordering::Relaxed);
    }
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

/// Passes that panicked (INFO `active_defrag_panics`).
pub fn panics() -> u64 {
    PANICS.load(Ordering::Relaxed)
}

/// Whether active defrag turned itself off after passes that kept panicking,
/// and has not been turned on since (INFO `active_defrag_disabled_by_panics`).
pub fn off_by_panics() -> bool {
    OFF_BY_PANICS.load(Ordering::Relaxed)
}

/// Active defrag's lines of INFO memory.
pub fn info() -> String {
    format!(
        "active_defrag_running:{}\r\n\
         active_defrag_hits:{}\r\n\
         active_defrag_misses:{}\r\n\
         active_defrag_panics:{}\r\n\
         active_defrag_disabled_by_panics:{}\r\n",
        u8::from(running()),
        hits(),
        misses(),
        panics(),
        u8::from(off_by_panics()),
    )
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
            let mut wait = TICK;
            loop {
                std::thread::sleep(wait);
                wait = step(&store);
            }
        })
        .expect("start the active defrag thread");
}

/// What the thread does each time it wakes: a pass, if one is due. Returns
/// how long to sleep before the next.
fn step(store: &Store) -> Duration {
    match pass(store) {
        Ok(false) => TICK,
        Ok(true) => {
            PANICS_IN_A_ROW.store(0, Ordering::Relaxed);
            TICK
        }
        Err(message) => panicked(&message),
    }
}

/// A pass of compaction, if active defrag is on and the slabs waste enough:
/// whether one ran, or what it panicked with. A pass that panics leaves no
/// slab being emptied, and `active_defrag_running` at 0.
fn pass(store: &Store) -> Result<bool, String> {
    // Held while a panic's slabs are put back too: another store's thread
    // would otherwise start a pass, and have its own put back with them.
    let _one = ONE_AT_A_TIME.lock();
    let ran = catch_unwind(AssertUnwindSafe(|| {
        let due = enabled() && wasting(ignore_bytes(), threshold_lower());
        if due {
            compact(store);
        }
        due
    }));
    ran.map_err(|panic| {
        // The pass let go of every lock it held as it unwound; what it left
        // is the slabs it chose, listed nowhere.
        if let Err(again) = catch_unwind(value_slab::finish_evacuation) {
            // Those not put back are still on the list, for the next pass.
            crate::log_error!(
                "active defrag: putting back the slabs being emptied panicked too: {}",
                panic_message(again.as_ref())
            );
        }
        RUNNING.store(false, Ordering::Relaxed);
        panic_message(panic.as_ref())
    })
}

/// Count and log a pass that panicked with `message`, and turn active defrag
/// off after [`PANICS_TO_TURN_OFF`] in a row. Returns how long to wait
/// before the next pass.
fn panicked(message: &str) -> Duration {
    PANICS.fetch_add(1, Ordering::Relaxed);
    let (in_a_row, turned_off) = {
        let _switch = SWITCH.lock();
        let in_a_row = PANICS_IN_A_ROW.fetch_add(1, Ordering::Relaxed) + 1;
        // Not if it was turned off while the pass ran: then panics did not.
        let turn_off = in_a_row >= PANICS_TO_TURN_OFF && enabled();
        if turn_off {
            ENABLED.store(false, Ordering::Relaxed);
            OFF_BY_PANICS.store(true, Ordering::Relaxed);
        }
        (in_a_row, turn_off)
    };
    if turned_off {
        crate::log_error!(
            "active defrag turned itself off: {in_a_row} passes in a row panicked, the last \
             with: {message}. Every value is intact and the slabs it was emptying are back in \
             use. Turn it back on with CONFIG SET activedefrag yes"
        );
        return TICK;
    }
    let wait = backoff(in_a_row);
    crate::log_error!(
        "active defrag: a pass panicked: {message}. Every value is intact and the slabs it \
         was emptying are back in use; the next pass is in {}s ({in_a_row} in a row; active \
         defrag turns itself off at {PANICS_TO_TURN_OFF})",
        wait.as_secs()
    );
    wait
}

/// The wait after the `in_a_row`th pass in a row that panicked.
fn backoff(in_a_row: u32) -> Duration {
    let doublings = in_a_row.saturating_sub(1).min(16);
    BACKOFF.saturating_mul(1 << doublings).min(BACKOFF_MAX)
}

/// What a panic said: its message, if it was a string.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "a panic with no message".to_string()
    }
}

/// Empty the sparsest slabs, a set at a time, until the waste is under half
/// the thresholds, or a set yields nothing. The caller holds
/// [`ONE_AT_A_TIME`].
fn compact(store: &Store) {
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
/// buckets at a time.
fn compact_shard(shard: &Shard, regions: &[usize]) {
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
            // Its room is taken here: a push below cannot panic. Should a
            // panic come anyway, what it holds is dropped with the lock let
            // go, as below: each block is one its key no longer points to.
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
                // For tests: a pass that panics here, the shard held for
                // writing and the blocks just left not yet freed.
                fault_point();
            }
            HITS.fetch_add(old.len() as u64, Ordering::Relaxed);
            // Freed with the shard unlocked: a slab that empties is given back
            // to the kernel, a system call.
            drop(old);
        }
        rest(started.elapsed());
        if at >= buckets {
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

/// For tests: how many of the next passes panic (in [`compact_shard`], with
/// a shard held for writing, once they have moved something).
#[cfg(test)]
static FAULTS: AtomicU32 = AtomicU32::new(0);

#[cfg(test)]
fn fault_point() {
    if FAULTS.load(Ordering::Relaxed) > 0 {
        FAULTS.fetch_sub(1, Ordering::Relaxed);
        panic!("injected fault in an active defrag pass");
    }
}

#[cfg(not(test))]
#[inline(always)]
fn fault_point() {}

#[cfg(test)]
mod tests {
    //! A pass that panics (#358), made to with [`FAULTS`]. The thread's
    //! [`step`] is called here, on the test's thread, as the thread would.
    //! These hold `value_slab::EVACUATION_TESTS`: the slabs are the
    //! process's, and so are the settings.

    use super::*;
    use crate::store::{ValueRef, new_store, shard_index};
    use std::sync::Arc;

    /// What a test here holds, with the settings that make any waste worth
    /// a pass, and no rest between batches; dropped, the defaults are back.
    struct Setup {
        _one: parking_lot::MutexGuard<'static, ()>,
    }

    impl Setup {
        fn new() -> Self {
            let one = value_slab::EVACUATION_TESTS.lock();
            FAULTS.store(0, Ordering::Relaxed);
            set_ignore_bytes(0);
            set_threshold_lower(0);
            set_cycle_max(100);
            set_enabled(true);
            Setup { _one: one }
        }
    }

    impl Drop for Setup {
        fn drop(&mut self) {
            FAULTS.store(0, Ordering::Relaxed);
            configure(&crate::config::Config::default());
        }
    }

    const KEYS: usize = 40_000;

    fn key(i: usize) -> String {
        format!("k:{i}")
    }

    /// Key `i`'s value: 100 bytes, its number in them.
    fn value(i: usize) -> Vec<u8> {
        format!("{i:0>100}").into_bytes()
    }

    /// Whether key `i` is one of the eighth kept: a fixed pattern that looks
    /// random, so that every slab keeps about an eighth.
    fn kept(i: usize) -> bool {
        (i.wrapping_mul(2654435761) >> 7).is_multiple_of(8)
    }

    /// A store of [`KEYS`] values, made on a thread that has ended, so that
    /// nobody owns their slabs, with all but an eighth deleted: the slabs
    /// they are in are an eighth full, sparser than nearly any other.
    fn sparse_store() -> Store {
        let store = new_store(1);
        let filling = Arc::clone(&store);
        std::thread::spawn(move || {
            let db = &filling[0];
            for i in 0..KEYS {
                let key = key(i);
                let shard = &db[shard_index(db.len(), &key)];
                let was = shard.write().insert_str_expiring(&key, value(i), None);
                assert!(was.is_none());
            }
        })
        .join()
        .unwrap();
        let db = &store[0];
        for i in (0..KEYS).filter(|&i| !kept(i)) {
            let key = key(i);
            let taken = db[shard_index(db.len(), &key)].write().remove(&key);
            assert!(taken.is_some());
        }
        store
    }

    /// Every kept key reads back its value, byte for byte, and no other key
    /// is there.
    fn check(store: &Store) {
        let db = &store[0];
        for i in 0..KEYS {
            let key = key(i);
            let map = db[shard_index(db.len(), &key)].read();
            match map.get(&key) {
                Some(ValueRef::String(s)) if kept(i) => {
                    assert_eq!(s.as_bytes(), value(i).as_slice(), "{key}");
                }
                None if !kept(i) => {}
                Some(_) => panic!("{key} is there, or not a string (kept: {})", kept(i)),
                None => panic!("{key} is gone"),
            }
        }
        let keys: usize = db.iter().map(|shard| shard.read().len()).sum();
        assert_eq!(keys, (0..KEYS).filter(|&i| kept(i)).count());
    }

    /// How many slabs the kept values are in.
    fn slabs(store: &Store) -> usize {
        let db = &store[0];
        let mut slabs: Vec<usize> = (0..KEYS)
            .filter(|&i| kept(i))
            .map(|i| {
                let key = key(i);
                let map = db[shard_index(db.len(), &key)].read();
                let Some(ValueRef::String(s)) = map.get(&key) else {
                    panic!("{key} is gone");
                };
                s.as_bytes().as_ptr() as usize / value_slab::SLAB
            })
            .collect();
        slabs.sort_unstable();
        slabs.dedup();
        slabs.len()
    }

    /// Whether a value of `store` is in a slab still being emptied.
    fn any_evacuating(store: &Store) -> bool {
        let regions = value_slab::regions_snapshot();
        store[0].iter().any(|shard| {
            let map = shard.read();
            (0..map.buckets()).any(|i| map.evacuating_at(i, &regions))
        })
    }

    /// [`step`], once; a panic out of it would have ended the thread. After
    /// it, no pass runs and no slab is being emptied, whatever the pass did.
    fn stepped(store: &Store) -> Duration {
        let stepped = catch_unwind(AssertUnwindSafe(|| step(store)));
        assert!(
            !running(),
            "active_defrag_running stays 1 after the pass, with {} slabs left being emptied",
            value_slab::evacuating_slabs()
        );
        assert_eq!(
            value_slab::evacuating_slabs(),
            0,
            "slabs left being emptied after the pass"
        );
        stepped.unwrap_or_else(|_| {
            panic!("the panic got out of the pass: it would have ended the active defrag thread")
        })
    }

    #[test]
    fn a_pass_that_panics_puts_its_slabs_back_and_the_next_one_compacts() {
        let _setup = Setup::new();
        let store = sparse_store();
        let before = slabs(&store);
        let panics_before = panics();

        FAULTS.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&store), BACKOFF);
        assert_eq!(
            FAULTS.load(Ordering::Relaxed),
            0,
            "no pass reached the fault"
        );
        assert_eq!(panics() - panics_before, 1);
        assert_eq!(PANICS_IN_A_ROW.load(Ordering::Relaxed), 1);
        assert!(enabled() && !off_by_panics());
        assert!(!any_evacuating(&store));
        check(&store);

        // The slabs are listed again: the next pass empties them.
        let hits_before = hits();
        assert_eq!(stepped(&store), TICK);
        assert_eq!(PANICS_IN_A_ROW.load(Ordering::Relaxed), 0);
        assert!(hits() > hits_before, "nothing moved after the panic");
        let after = slabs(&store);
        assert!(
            after * 2 < before,
            "the kept values are in {after} slabs, after {before}"
        );
        check(&store);
        assert_eq!(panics() - panics_before, 1);
    }

    #[test]
    fn passes_that_keep_panicking_turn_active_defrag_off_until_it_is_turned_on() {
        let _setup = Setup::new();
        let store = sparse_store();
        let panics_before = panics();

        FAULTS.store(PANICS_TO_TURN_OFF, Ordering::Relaxed);
        let waits: Vec<Duration> = (0..PANICS_TO_TURN_OFF).map(|_| stepped(&store)).collect();
        assert_eq!(
            FAULTS.load(Ordering::Relaxed),
            0,
            "a pass did not reach the fault"
        );
        // 1, 2, 4 and 8 seconds, then off.
        assert_eq!(
            waits,
            [BACKOFF, BACKOFF * 2, BACKOFF * 4, BACKOFF * 8, TICK]
        );
        assert!(!enabled() && off_by_panics());
        let fields = info();
        assert!(fields.contains("active_defrag_running:0\r\n"), "{fields}");
        assert!(
            fields.contains(&format!(
                "active_defrag_panics:{}\r\n",
                panics_before + u64::from(PANICS_TO_TURN_OFF)
            )),
            "{fields}"
        );
        assert!(
            fields.contains("active_defrag_disabled_by_panics:1\r\n"),
            "{fields}"
        );
        assert!(!any_evacuating(&store));
        check(&store);

        // Off: no pass runs.
        FAULTS.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&store), TICK);
        assert_eq!(FAULTS.load(Ordering::Relaxed), 1, "a pass ran while off");
        FAULTS.store(0, Ordering::Relaxed);

        // What `CONFIG SET activedefrag yes` does.
        set_enabled(true);
        assert!(info().contains("active_defrag_disabled_by_panics:0\r\n"));
        let before = slabs(&store);
        let hits_before = hits();
        assert_eq!(stepped(&store), TICK);
        assert!(hits() > hits_before, "nothing moved once turned on again");
        let after = slabs(&store);
        assert!(
            after * 2 < before,
            "the kept values are in {after} slabs, after {before}"
        );
        check(&store);
        assert_eq!(PANICS_IN_A_ROW.load(Ordering::Relaxed), 0);
        assert_eq!(panics() - panics_before, u64::from(PANICS_TO_TURN_OFF));
        assert!(enabled() && !off_by_panics());
    }

    #[test]
    fn a_panic_message_is_read_from_a_str_or_a_string() {
        let of = |f: fn()| panic_message(catch_unwind(f).unwrap_err().as_ref());
        assert_eq!(of(|| panic!("plain")), "plain");
        assert_eq!(of(|| panic!("made {}", 1)), "made 1");
        assert_eq!(of(|| std::panic::panic_any(7u8)), "a panic with no message");
    }
}
