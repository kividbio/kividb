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
//! is where its key points, and the block it left is freed (and counted in
//! `active_defrag_hits`). What a panic does leave is the slabs the pass was
//! emptying: they are put back as a pass that ends puts them back
//! ([`value_slab::finish_evacuation`]), still under [`ONE_AT_A_TIME`]; any
//! that putting back did not reach, because it panicked too, are put back
//! at the next tick, which does that first whether or not active defrag is
//! on. The panic is logged with where it happened, and the next pass comes
//! after a wait that doubles with each panic in a row. After
//! [`PANICS_TO_TURN_OFF`] in a row on one thread, with no pass ending
//! between them, active defrag turns itself off (`activedefrag no`, INFO
//! `active_defrag_disabled_by_panics:1`) until it is turned on again.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::store::{Leftover, Relocated, Shard, Store};
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
/// before it, up to [`BACKOFF_MAX`]: 1, 2, 4 and 8 seconds. The fifth in a
/// row turns active defrag off, so 8 seconds is the longest wait there is.
const BACKOFF: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(8);

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
/// How many times active defrag has been turned from off to on: a thread
/// that sees it change starts its count of panics in a row afresh.
static TURNED_ON: AtomicU64 = AtomicU64::new(0);
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

/// `activedefrag`. Turned from off to on, it starts afresh: whatever panics
/// there were before, the next pass is the first. Turned on while it is on,
/// nothing changes: a configuration loop that sends `CONFIG SET activedefrag
/// yes` again and again would otherwise keep a pass that always panics from
/// ever turning it off (#370).
pub fn set_enabled(on: bool) {
    let _switch = SWITCH.lock();
    let was = ENABLED.swap(on, Ordering::Relaxed);
    if on && !was {
        TURNED_ON.fetch_add(1, Ordering::Relaxed);
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
/// Not kept across a restart.
pub fn off_by_panics() -> bool {
    OFF_BY_PANICS.load(Ordering::Relaxed)
}

/// `CONFIG RESETSTAT`: the counts in INFO back to 0, as Redis resets its
/// `active_defrag_hits` and `_misses`. Whether active defrag turned itself
/// off is not a count, and stays.
pub fn reset_stats() {
    HITS.store(0, Ordering::Relaxed);
    MISSES.store(0, Ordering::Relaxed);
    PANICS.store(0, Ordering::Relaxed);
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
            let mut streak = Streak::new();
            let mut wait = TICK;
            loop {
                std::thread::sleep(wait);
                wait = step(&store, &mut streak);
            }
        })
        .expect("start the active defrag thread");
}

/// One defrag thread's passes in a row that panicked. Each thread has its
/// own: in a process with several stores (the in-process servers of tests),
/// another store's thread whose passes end does not start this one's count
/// afresh (#370). Turning active defrag on, which is the process's, does.
struct Streak {
    in_a_row: u32,
    /// [`TURNED_ON`] when `in_a_row` was last started afresh.
    turned_on: u64,
}

impl Streak {
    fn new() -> Self {
        Streak {
            in_a_row: 0,
            turned_on: TURNED_ON.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn in_a_row(&self) -> u32 {
        self.in_a_row
    }

    /// Count a pass that panicked, log it, and turn active defrag off after
    /// [`PANICS_TO_TURN_OFF`] in a row. Returns how long to wait before the
    /// next pass.
    fn panicked(&mut self, panic: &Panicked) -> Duration {
        PANICS.fetch_add(1, Ordering::Relaxed);
        let outcome = {
            let _switch = SWITCH.lock();
            let turned_on = TURNED_ON.load(Ordering::Relaxed);
            if turned_on != self.turned_on {
                self.turned_on = turned_on;
                self.in_a_row = 0;
            }
            self.in_a_row += 1;
            if !enabled() {
                // Turned off while the pass ran: then panics did not.
                Outcome::WasOff
            } else if self.in_a_row >= PANICS_TO_TURN_OFF {
                ENABLED.store(false, Ordering::Relaxed);
                OFF_BY_PANICS.store(true, Ordering::Relaxed);
                Outcome::TurnedOff
            } else {
                Outcome::Retry(backoff(self.in_a_row))
            }
        };
        log_error(report(panic, self.in_a_row, outcome));
        match outcome {
            Outcome::Retry(wait) => wait,
            Outcome::TurnedOff | Outcome::WasOff => TICK,
        }
    }
}

/// What came of a pass that panicked.
#[derive(Clone, Copy)]
enum Outcome {
    /// The next pass is after this wait.
    Retry(Duration),
    /// That was the last in a row: active defrag turned itself off.
    TurnedOff,
    /// Active defrag was turned off while the pass ran.
    WasOff,
}

/// A pass that panicked: where and with what, and whether the slabs it was
/// emptying were put back, or what putting them back panicked with.
struct Panicked {
    message: String,
    put_back: Result<(), String>,
}

/// How a tick panicked.
enum Failed {
    /// Putting back, at its start, the slabs an earlier panic left out: no
    /// pass ran, and none is counted.
    PuttingBack(String),
    /// A pass.
    Pass(Panicked),
}

/// The ERROR line of a pass that panicked, the `in_a_row`th in a row.
fn report(panic: &Panicked, in_a_row: u32, outcome: Outcome) -> String {
    let slabs = match &panic.put_back {
        Ok(()) => "Every value is intact and the slabs it was emptying are back in use".to_string(),
        Err(again) => format!(
            "Putting back the slabs it was emptying panicked too, {again}; those it did not \
             reach are put back at the next tick"
        ),
    };
    // "at src/...: message", or "with: message".
    let what = &panic.message;
    match outcome {
        Outcome::Retry(wait) => format!(
            "active defrag: a pass panicked {what}. {slabs}; the next pass is in {}s \
             ({in_a_row} in a row; active defrag turns itself off at {PANICS_TO_TURN_OFF})",
            wait.as_secs()
        ),
        Outcome::TurnedOff => format!(
            "active defrag turned itself off: {in_a_row} passes in a row panicked, the last \
             {what}. {slabs}. Turn it back on with CONFIG SET activedefrag yes"
        ),
        Outcome::WasOff => format!(
            "active defrag: a pass panicked {what}. {slabs}. Active defrag was turned off while \
             the pass ran: no pass runs until it is turned on again"
        ),
    }
}

/// What the thread does each time it wakes: a pass, if one is due. Returns
/// how long to sleep before the next.
fn step(store: &Store, streak: &mut Streak) -> Duration {
    match pass(store) {
        Ok(false) => TICK,
        Ok(true) => {
            streak.in_a_row = 0;
            TICK
        }
        Err(Failed::PuttingBack(message)) => {
            log_error(format!(
                "active defrag: putting back the slabs a panic left out panicked {message}; no \
                 pass ran, and those it did not reach are put back at the next tick"
            ));
            TICK
        }
        Err(Failed::Pass(panic)) => streak.panicked(&panic),
    }
}

/// A pass of compaction, if active defrag is on and the slabs waste enough:
/// whether one ran, or how it panicked. A pass that panics leaves no slab
/// being emptied, unless putting them back panicked too, and
/// `active_defrag_running` at 0.
fn pass(store: &Store) -> Result<bool, Failed> {
    // Held while a panic's slabs are put back too: another store's thread
    // would otherwise start a pass, and have its own put back with them.
    let _one = ONE_AT_A_TIME.lock();
    // Slabs a panic before this one could not put back (#370): first, and on
    // or off, so that none stays out of use while active defrag is off.
    // Nothing to do, and one lock taken, otherwise. The hook is put in here
    // too, where a panic is caught.
    let put_back = in_pass(|| {
        catch_unwind(|| {
            note_panic_locations();
            value_slab::finish_evacuation();
        })
    });
    if let Err(panic) = put_back {
        return Err(Failed::PuttingBack(described(panic.as_ref())));
    }
    let ran = in_pass(|| {
        catch_unwind(AssertUnwindSafe(|| {
            let due = enabled() && wasting(ignore_bytes(), threshold_lower());
            if due {
                compact(store);
            }
            due
        }))
    });
    ran.map_err(|panic| {
        let message = described(panic.as_ref());
        // The pass let go of every lock it held as it unwound; what it left
        // is the slabs it chose, listed nowhere. Those this does not reach,
        // if it panics too, stay on the list for the next pass.
        let put_back = in_pass(|| catch_unwind(value_slab::finish_evacuation))
            .map_err(|again| described(again.as_ref()));
        RUNNING.store(false, Ordering::Relaxed);
        Failed::Pass(Panicked { message, put_back })
    })
}

thread_local! {
    /// Whether this thread is in a pass: a panic's place is noted only then.
    static IN_PASS: Cell<bool> = const { Cell::new(false) };
    /// Where the last panic in a pass on this thread happened.
    static PANIC_AT: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// `f`, with this thread marked as in a pass until it returns or unwinds.
fn in_pass<T>(f: impl FnOnce() -> T) -> T {
    struct Out;
    impl Drop for Out {
        fn drop(&mut self) {
            let _ = IN_PASS.try_with(|p| p.set(false));
        }
    }
    IN_PASS.with(|p| p.set(true));
    let _out = Out;
    f()
}

/// From the first pass on, panics note where they happened, for the ERROR
/// line (#370): the payload `catch_unwind` gives has only the message, and
/// where the panic was went only to stderr, never to `logfile`. The hook
/// before it still runs after, for every panic. A panic outside a pass
/// notes nothing.
fn note_panic_locations() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let next = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // `try_with`: a panic on a thread on its way out must not panic
            // again here.
            if IN_PASS.try_with(Cell::get).unwrap_or(false) {
                let at = info.location().map(|at| at.to_string());
                let _ = PANIC_AT.try_with(|p| *p.borrow_mut() = at);
            }
            next(info);
        }));
    });
}

/// A caught panic as the ERROR line gives it: where, when its place was
/// noted, and its message.
fn described(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic_message(panic);
    match PANIC_AT.with(|p| p.borrow_mut().take()) {
        Some(at) => format!("at {at}: {message}"),
        None => format!("with: {message}"),
    }
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

/// The wait after the `in_a_row`th pass in a row that panicked.
fn backoff(in_a_row: u32) -> Duration {
    let doublings = in_a_row.saturating_sub(1).min(16);
    BACKOFF.saturating_mul(1 << doublings).min(BACKOFF_MAX)
}

/// Log `line` at ERROR (and, for tests, keep it).
fn log_error(line: String) {
    crate::log_error!("{line}");
    #[cfg(test)]
    LOGGED.lock().push(line);
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
        // For tests: a pass that panics with its slabs chosen and nothing
        // moved yet.
        fault_point(Fault::AfterPlan);
        let regions = value_slab::regions_snapshot();
        // Counted here, not read from `active_defrag_hits`, which a
        // `CONFIG RESETSTAT` can set to 0 while the set runs (#370).
        let mut moved = 0u64;
        'all: for db in store.iter() {
            for shard in db.iter() {
                moved += compact_shard(shard, &regions);
                if !enabled() {
                    break 'all;
                }
            }
        }
        value_slab::finish_evacuation();
        if moved == 0 {
            break;
        }
    }
    RUNNING.store(false, Ordering::Relaxed);
}

/// The blocks a chunk's moves left behind, freed when this is dropped: with
/// the shard let go of, whether the pass goes on or unwinds. Each is a value
/// moved, counted then in `active_defrag_hits`, so that the moves of a chunk
/// a panic cut short are counted too (#370).
struct Leftovers(Vec<Leftover>);

impl Drop for Leftovers {
    fn drop(&mut self) {
        HITS.fetch_add(self.0.len() as u64, Ordering::Relaxed);
    }
}

/// Move the values of `shard` that are in slabs being emptied, [`BATCH`]
/// buckets at a time. Returns how many moved.
fn compact_shard(shard: &Shard, regions: &[usize]) -> u64 {
    let mut moved = 0u64;
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
            let mut old = Leftovers(Vec::with_capacity(chunk.len()));
            {
                let mut map = shard.write();
                for (i, &index) in chunk.iter().enumerate() {
                    if let Some(&next) = chunk.get(i + 1) {
                        map.prefetch_bucket(next);
                    }
                    match map.relocate_at(index, regions) {
                        Relocated::Moved(was) => old.0.push(was),
                        Relocated::Held => {
                            MISSES.fetch_add(1, Ordering::Relaxed);
                        }
                        Relocated::Stays => {}
                    }
                }
                // For tests: a pass that panics here, the shard held for
                // writing and the blocks just left not yet freed.
                fault_point(Fault::AfterChunk);
            }
            // Counted and freed with the shard unlocked: a slab that empties
            // is given back to the kernel, a system call.
            moved += old.0.len() as u64;
            drop(old);
        }
        rest(started.elapsed());
        if at >= buckets {
            return moved;
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

/// Where a test can make a pass panic ([`fault_point`]).
#[derive(Clone, Copy)]
enum Fault {
    /// In [`compact`], once a set of slabs is chosen.
    AfterPlan,
    /// In [`compact_shard`], with a shard held for writing, once a chunk is
    /// moved.
    AfterChunk,
}

/// For tests: how many of the next passes panic at each [`Fault`].
#[cfg(test)]
static FAULTS_AFTER_PLAN: AtomicU32 = AtomicU32::new(0);
#[cfg(test)]
static FAULTS_AFTER_CHUNK: AtomicU32 = AtomicU32::new(0);
/// For tests: a fault turns active defrag off before it panics, as a
/// `CONFIG SET activedefrag no` while the pass runs would.
#[cfg(test)]
static FAULT_TURNS_OFF: AtomicBool = AtomicBool::new(false);
/// For tests: the ERROR lines logged.
#[cfg(test)]
static LOGGED: parking_lot::Mutex<Vec<String>> = parking_lot::Mutex::new(Vec::new());

#[cfg(test)]
fn fault_point(at: Fault) {
    let faults = match at {
        Fault::AfterPlan => &FAULTS_AFTER_PLAN,
        Fault::AfterChunk => &FAULTS_AFTER_CHUNK,
    };
    if faults.load(Ordering::Relaxed) > 0 {
        faults.fetch_sub(1, Ordering::Relaxed);
        if FAULT_TURNS_OFF.load(Ordering::Relaxed) {
            set_enabled(false);
        }
        panic!("injected fault in an active defrag pass");
    }
}

#[cfg(not(test))]
#[inline(always)]
fn fault_point(_: Fault) {}

#[cfg(test)]
mod tests {
    //! A pass that panics (#358, #370), made to at a [`Fault`]. The thread's
    //! [`step`] is called here, on the test's thread, as the thread would.
    //! These hold `value_slab::EVACUATION_TESTS`: the slabs are the
    //! process's, and so are the settings.

    use super::*;
    use crate::store::{ValueRef, new_store, shard_index};
    use std::sync::Arc;

    /// What a test here holds, with the settings that make any waste worth
    /// a pass, no rest between batches, and active defrag just turned on;
    /// dropped, the defaults are back.
    struct Setup {
        _one: parking_lot::MutexGuard<'static, ()>,
    }

    impl Setup {
        fn new() -> Self {
            let one = value_slab::EVACUATION_TESTS.lock();
            clear_faults();
            set_ignore_bytes(0);
            set_threshold_lower(0);
            set_cycle_max(100);
            set_enabled(false);
            set_enabled(true);
            LOGGED.lock().clear();
            Setup { _one: one }
        }
    }

    impl Drop for Setup {
        fn drop(&mut self) {
            clear_faults();
            configure(&crate::config::Config::default());
        }
    }

    fn clear_faults() {
        FAULTS_AFTER_PLAN.store(0, Ordering::Relaxed);
        FAULTS_AFTER_CHUNK.store(0, Ordering::Relaxed);
        FAULT_TURNS_OFF.store(false, Ordering::Relaxed);
        value_slab::FAULTS_IN_FINISH.store(0, Ordering::Relaxed);
    }

    /// The ERROR lines logged since the last call.
    fn logged() -> Vec<String> {
        std::mem::take(&mut *LOGGED.lock())
    }

    /// The keys and values a test's store holds.
    struct Data {
        keys: usize,
        key: fn(usize) -> String,
        value: fn(usize) -> Vec<u8>,
    }

    /// 100-byte strings, kept in their keys' entries: the entries move.
    const INLINE: Data = Data {
        keys: 40_000,
        key: |i| format!("k:{i}"),
        value: |i| format!("{i:0>100}").into_bytes(),
    };

    /// 1,000-byte strings of their own (`SharedBytes`) under keys too long
    /// for their entries to be in a slab: only the strings move, and leave
    /// `Leftover::Bytes`.
    const OWN_BYTES: Data = Data {
        keys: 4_000,
        key: |i| format!("{i:0>4200}"),
        value: |i| format!("{i:0>1000}").into_bytes(),
    };

    /// Whether key `i` is one of the eighth kept: a fixed pattern that looks
    /// random, so that every slab keeps about an eighth.
    fn kept(i: usize) -> bool {
        (i.wrapping_mul(2654435761) >> 7).is_multiple_of(8)
    }

    /// A store of `data`, made on a thread that has ended, so that nobody
    /// owns their slabs, with all but an eighth deleted: the slabs they are
    /// in are an eighth full, sparser than nearly any other.
    fn sparse_store(data: &'static Data) -> Store {
        let store = new_store(1);
        let filling = Arc::clone(&store);
        std::thread::spawn(move || {
            let db = &filling[0];
            for i in 0..data.keys {
                let key = (data.key)(i);
                let shard = &db[shard_index(db.len(), &key)];
                let was = shard
                    .write()
                    .insert_str_expiring(&key, (data.value)(i), None);
                assert!(was.is_none());
            }
        })
        .join()
        .unwrap();
        let db = &store[0];
        for i in (0..data.keys).filter(|&i| !kept(i)) {
            let key = (data.key)(i);
            let taken = db[shard_index(db.len(), &key)].write().remove(&key);
            assert!(taken.is_some());
        }
        store
    }

    /// Every kept key reads back its value, byte for byte, and no other key
    /// is there.
    fn check(store: &Store, data: &Data) {
        let db = &store[0];
        for i in 0..data.keys {
            let key = (data.key)(i);
            let map = db[shard_index(db.len(), &key)].read();
            match map.get(&key) {
                Some(ValueRef::String(s)) if kept(i) => {
                    assert_eq!(s.as_bytes(), (data.value)(i).as_slice(), "key {i}");
                }
                None if !kept(i) => {}
                Some(_) => panic!("key {i} is there, or not a string (kept: {})", kept(i)),
                None => panic!("key {i} is gone"),
            }
        }
        let keys: usize = db.iter().map(|shard| shard.read().len()).sum();
        assert_eq!(keys, (0..data.keys).filter(|&i| kept(i)).count());
    }

    /// Where each kept value's bytes are.
    fn places(store: &Store, data: &Data) -> Vec<usize> {
        let db = &store[0];
        (0..data.keys)
            .filter(|&i| kept(i))
            .map(|i| {
                let key = (data.key)(i);
                let map = db[shard_index(db.len(), &key)].read();
                let Some(ValueRef::String(s)) = map.get(&key) else {
                    panic!("key {i} is gone");
                };
                s.as_bytes().as_ptr() as usize
            })
            .collect()
    }

    /// How many slabs the kept values are in.
    fn slabs(store: &Store, data: &Data) -> usize {
        let mut slabs: Vec<usize> = places(store, data)
            .into_iter()
            .map(|at| at / value_slab::SLAB)
            .collect();
        slabs.sort_unstable();
        slabs.dedup();
        slabs.len()
    }

    /// How many of the values at `before` are somewhere else now.
    fn moved(before: &[usize], store: &Store, data: &Data) -> u64 {
        let now = places(store, data);
        before.iter().zip(&now).filter(|(a, b)| a != b).count() as u64
    }

    /// Whether a value of `store` is in a slab still being emptied.
    fn any_evacuating(store: &Store) -> bool {
        let regions = value_slab::regions_snapshot();
        store[0].iter().any(|shard| {
            let map = shard.read();
            (0..map.buckets()).any(|i| map.evacuating_at(i, &regions))
        })
    }

    /// [`step`], once, whose panic, had one got out of it, would have ended
    /// the thread.
    fn step_once(store: &Store, streak: &mut Streak) -> Duration {
        catch_unwind(AssertUnwindSafe(|| step(store, streak))).unwrap_or_else(|_| {
            panic!("the panic got out of the pass: it would have ended the active defrag thread")
        })
    }

    /// [`step_once`], after which no pass runs and no slab is being
    /// emptied, whatever the pass did.
    fn stepped(store: &Store, streak: &mut Streak) -> Duration {
        let wait = step_once(store, streak);
        assert!(!running(), "active_defrag_running stays 1 after the pass");
        assert_eq!(
            value_slab::evacuating_slabs(),
            0,
            "slabs left being emptied after the pass"
        );
        wait
    }

    /// A pass that panics after a chunk of moves, on `data`: the slabs come
    /// back, every value is intact, the moves are counted, the line says
    /// where it panicked, and the next pass compacts.
    fn a_pass_that_panics_after_a_chunk(data: &'static Data) {
        let _setup = Setup::new();
        let store = sparse_store(data);
        let mut streak = Streak::new();
        let before = slabs(&store, data);
        let places_before = places(&store, data);
        let panics_before = panics();
        let hits_before = hits();

        FAULTS_AFTER_CHUNK.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&store, &mut streak), BACKOFF);
        assert_eq!(
            FAULTS_AFTER_CHUNK.load(Ordering::Relaxed),
            0,
            "no pass reached the fault"
        );
        assert_eq!(panics() - panics_before, 1);
        assert_eq!(streak.in_a_row(), 1);
        assert!(enabled() && !off_by_panics());
        assert!(!any_evacuating(&store));
        check(&store, data);
        // The chunk that panicked moved values, and they are counted.
        let moved = moved(&places_before, &store, data);
        assert!(moved > 0, "the chunk that panicked moved nothing");
        assert_eq!(
            hits() - hits_before,
            moved,
            "moves of the chunk that panicked not counted"
        );
        let lines = logged();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("injected fault"), "{}", lines[0]);
        assert!(
            lines[0].contains("src/compaction.rs:"),
            "no place in: {}",
            lines[0]
        );
        assert!(lines[0].contains("are back in use"), "{}", lines[0]);

        // The slabs are listed again: the next pass empties them.
        let hits_before = hits();
        assert_eq!(stepped(&store, &mut streak), TICK);
        assert_eq!(streak.in_a_row(), 0);
        assert!(hits() > hits_before, "nothing moved after the panic");
        let after = slabs(&store, data);
        assert!(
            after * 2 < before,
            "the kept values are in {after} slabs, after {before}"
        );
        check(&store, data);
        assert_eq!(panics() - panics_before, 1);
    }

    #[test]
    fn a_pass_that_panics_puts_its_slabs_back_and_the_next_one_compacts() {
        a_pass_that_panics_after_a_chunk(&INLINE);
    }

    #[test]
    fn a_pass_that_panics_moving_strings_of_their_own_frees_only_what_they_left() {
        a_pass_that_panics_after_a_chunk(&OWN_BYTES);
    }

    #[test]
    fn a_pass_that_panics_once_its_slabs_are_chosen_puts_them_all_back() {
        let _setup = Setup::new();
        let store = sparse_store(&INLINE);
        let mut streak = Streak::new();
        let before = slabs(&store, &INLINE);
        let hits_before = hits();

        FAULTS_AFTER_PLAN.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&store, &mut streak), BACKOFF);
        assert_eq!(
            FAULTS_AFTER_PLAN.load(Ordering::Relaxed),
            0,
            "no pass reached the fault"
        );
        assert_eq!(hits(), hits_before, "something moved before the fault");
        assert!(!any_evacuating(&store));
        check(&store, &INLINE);

        assert_eq!(stepped(&store, &mut streak), TICK);
        assert!(slabs(&store, &INLINE) * 2 < before);
        check(&store, &INLINE);
    }

    #[test]
    fn a_put_back_that_panics_too_says_so_and_the_next_tick_puts_the_rest_back() {
        let _setup = Setup::new();
        let store = sparse_store(&INLINE);
        let mut streak = Streak::new();

        FAULTS_AFTER_CHUNK.store(1, Ordering::Relaxed);
        value_slab::FAULTS_IN_FINISH.store(1, Ordering::Relaxed);
        assert_eq!(step_once(&store, &mut streak), BACKOFF);
        assert_eq!(value_slab::FAULTS_IN_FINISH.load(Ordering::Relaxed), 0);
        assert!(!running());
        let left = value_slab::evacuating_slabs();
        assert!(
            left > 0,
            "putting back stopped at its first slab, and none is left out"
        );
        let lines = logged();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("panicked too"), "{}", lines[0]);
        assert!(!lines[0].contains("are back in use"), "{}", lines[0]);
        assert!(
            lines[0].contains("src/value_slab.rs:"),
            "no place in: {}",
            lines[0]
        );
        check(&store, &INLINE);

        // Off: no pass runs, but the next tick puts the rest back.
        set_enabled(false);
        assert_eq!(stepped(&store, &mut streak), TICK);
        assert!(!any_evacuating(&store));
        check(&store, &INLINE);
    }

    #[test]
    fn a_put_back_at_the_start_of_a_tick_that_panics_is_no_pass_and_says_so() {
        let _setup = Setup::new();
        let store = sparse_store(&INLINE);
        let mut streak = Streak::new();

        // Slabs left out: a pass panics, and so does putting back.
        FAULTS_AFTER_CHUNK.store(1, Ordering::Relaxed);
        value_slab::FAULTS_IN_FINISH.store(1, Ordering::Relaxed);
        assert_eq!(step_once(&store, &mut streak), BACKOFF);
        let left = value_slab::evacuating_slabs();
        assert!(left >= 2, "{left} slabs left out");
        logged();

        // Off; the next tick's putting back panics after one slab.
        set_enabled(false);
        let panics_before = panics();
        value_slab::FAULTS_IN_FINISH.store(1, Ordering::Relaxed);
        assert_eq!(step_once(&store, &mut streak), TICK);
        assert_eq!(value_slab::FAULTS_IN_FINISH.load(Ordering::Relaxed), 0);
        assert_eq!(panics(), panics_before, "counted as a pass that panicked");
        assert_eq!(streak.in_a_row(), 1);
        assert_eq!(value_slab::evacuating_slabs(), left - 1);
        let lines = logged();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("no pass ran"), "{}", lines[0]);
        assert!(!lines[0].contains("a pass panicked"), "{}", lines[0]);
        assert!(!lines[0].contains("turned off while"), "{}", lines[0]);
        assert!(
            lines[0].contains("src/value_slab.rs:"),
            "no place in: {}",
            lines[0]
        );

        // The tick after puts back the rest.
        assert_eq!(stepped(&store, &mut streak), TICK);
        assert!(!any_evacuating(&store));
        check(&store, &INLINE);
    }

    #[test]
    fn a_thread_is_out_of_its_pass_once_the_pass_unwinds() {
        let unwound = catch_unwind(|| in_pass(|| panic!("in a pass")));
        assert!(unwound.is_err());
        assert!(!IN_PASS.with(Cell::get), "still marked as in a pass");
        PANIC_AT.with(|p| p.borrow_mut().take());
    }

    #[test]
    fn passes_that_keep_panicking_turn_active_defrag_off_until_it_is_turned_on() {
        let _setup = Setup::new();
        let store = sparse_store(&INLINE);
        let mut streak = Streak::new();
        let panics_before = panics();

        FAULTS_AFTER_CHUNK.store(PANICS_TO_TURN_OFF, Ordering::Relaxed);
        let waits: Vec<Duration> = (0..PANICS_TO_TURN_OFF)
            .map(|_| stepped(&store, &mut streak))
            .collect();
        assert_eq!(
            FAULTS_AFTER_CHUNK.load(Ordering::Relaxed),
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
        let lines = logged();
        assert!(
            lines
                .last()
                .is_some_and(|l| l.contains("turned itself off")
                    && l.contains("CONFIG SET activedefrag yes")),
            "{lines:?}"
        );
        assert!(!any_evacuating(&store));
        check(&store, &INLINE);

        // Off: no pass runs.
        FAULTS_AFTER_CHUNK.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&store, &mut streak), TICK);
        assert_eq!(
            FAULTS_AFTER_CHUNK.load(Ordering::Relaxed),
            1,
            "a pass ran while off"
        );
        FAULTS_AFTER_CHUNK.store(0, Ordering::Relaxed);

        // What `CONFIG SET activedefrag yes` does.
        set_enabled(true);
        assert!(info().contains("active_defrag_disabled_by_panics:0\r\n"));
        let before = slabs(&store, &INLINE);
        let hits_before = hits();
        assert_eq!(stepped(&store, &mut streak), TICK);
        assert!(hits() > hits_before, "nothing moved once turned on again");
        assert!(slabs(&store, &INLINE) * 2 < before);
        check(&store, &INLINE);
        assert_eq!(streak.in_a_row(), 0);
        assert_eq!(panics() - panics_before, u64::from(PANICS_TO_TURN_OFF));
        assert!(enabled() && !off_by_panics());
    }

    #[test]
    fn turning_it_on_while_it_is_on_does_not_start_the_count_afresh() {
        let _setup = Setup::new();
        let store = sparse_store(&INLINE);
        let mut streak = Streak::new();

        FAULTS_AFTER_CHUNK.store(3, Ordering::Relaxed);
        for _ in 0..3 {
            stepped(&store, &mut streak);
        }
        assert_eq!(streak.in_a_row(), 3);
        // A configuration loop's `CONFIG SET activedefrag yes`, while on.
        set_enabled(true);
        FAULTS_AFTER_CHUNK.store(2, Ordering::Relaxed);
        assert_eq!(stepped(&store, &mut streak), BACKOFF * 8);
        assert_eq!(stepped(&store, &mut streak), TICK);
        assert_eq!(streak.in_a_row(), PANICS_TO_TURN_OFF);
        assert!(
            !enabled() && off_by_panics(),
            "five panics in a row did not turn it off"
        );
        check(&store, &INLINE);
    }

    #[test]
    fn a_thread_whose_passes_end_does_not_start_anothers_count_afresh() {
        let _setup = Setup::new();
        let panicking = sparse_store(&INLINE);
        let ending = sparse_store(&INLINE);
        let (mut a, mut b) = (Streak::new(), Streak::new());

        FAULTS_AFTER_CHUNK.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&panicking, &mut a), BACKOFF);
        // The other store's thread: a pass that ends.
        assert_eq!(stepped(&ending, &mut b), TICK);
        assert_eq!(b.in_a_row(), 0);
        FAULTS_AFTER_CHUNK.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&panicking, &mut a), BACKOFF * 2);
        assert_eq!(a.in_a_row(), 2);
        check(&panicking, &INLINE);
        check(&ending, &INLINE);
    }

    #[test]
    fn a_panic_after_it_was_turned_off_mid_pass_says_so_and_turns_nothing_off() {
        let _setup = Setup::new();
        let store = sparse_store(&INLINE);
        let mut streak = Streak::new();

        FAULT_TURNS_OFF.store(true, Ordering::Relaxed);
        FAULTS_AFTER_CHUNK.store(1, Ordering::Relaxed);
        assert_eq!(stepped(&store, &mut streak), TICK);
        assert!(!enabled() && !off_by_panics());
        let lines = logged();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("turned off while the pass ran"),
            "{}",
            lines[0]
        );
        assert!(!lines[0].contains("next pass is in"), "{}", lines[0]);
        check(&store, &INLINE);
    }

    #[test]
    fn the_wait_after_a_panic_doubles_up_to_its_cap() {
        let waits: Vec<u64> = (1..=7).map(|n| backoff(n).as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 8, 8, 8]);
        assert_eq!(backoff(PANICS_TO_TURN_OFF - 1), BACKOFF_MAX);
    }

    #[test]
    fn resetstat_resets_the_counts_but_not_why_it_is_off() {
        let _setup = Setup::new();
        let store = sparse_store(&INLINE);
        let mut streak = Streak::new();
        FAULTS_AFTER_CHUNK.store(PANICS_TO_TURN_OFF, Ordering::Relaxed);
        for _ in 0..PANICS_TO_TURN_OFF {
            stepped(&store, &mut streak);
        }
        assert!(panics() > 0 && hits() > 0 && off_by_panics());
        reset_stats();
        let fields = info();
        for zero in [
            "active_defrag_hits:0\r\n",
            "active_defrag_misses:0\r\n",
            "active_defrag_panics:0\r\n",
            "active_defrag_disabled_by_panics:1\r\n",
        ] {
            assert!(fields.contains(zero), "{zero} not in {fields}");
        }
    }

    #[test]
    fn a_panic_message_is_read_from_a_str_or_a_string() {
        let of = |f: fn()| panic_message(catch_unwind(f).unwrap_err().as_ref());
        assert_eq!(of(|| panic!("plain")), "plain");
        assert_eq!(of(|| panic!("made {}", 1)), "made 1");
        assert_eq!(of(|| std::panic::panic_any(7u8)), "a panic with no message");
    }
}
