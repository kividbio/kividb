// src/command_gate.rs
//
// A transaction, or a script, is one step as far as every other client is
// concerned.
//
// Redis gets that for free: it runs one command at a time, so nothing can
// happen between the commands of an `EXEC` or the `redis.call`s of a script.
// KiviDB runs commands on many threads. Without something to stop them, a
// client's `MGET a b` could land between the `DECRBY a` and the `INCRBY b` of
// somebody else's transfer and read a sum that never existed; a plain write
// could land between an `EXEC`'s check of its watched keys and its first
// command.
//
// This is that something. Every command passes through the gate, shared, for
// as long as it runs. A transaction or a script takes it alone: it waits for
// the commands in flight to finish, and holds new ones back, until it is
// done.
//
// The shared side is on the path of every command, so it has to cost next to
// nothing. As in `write_gate`, there is one lock per slot and each thread is
// pinned to a slot: entering is an uncontended operation on a cache line only
// that thread touches. Taking the gate alone pays for it by taking every
// slot a thread has — not every slot there is: a server with four threads
// has a handful, and a script that took all 128 spent more on the gate than
// a short script spends on its work.
//
// A thread that gets its first slot while the gate is being taken alone may
// have a slot the taker did not count. Such a thread also goes through
// `LATE`, which every taker takes after its slots, until no taker that
// could have missed it is left. Which of the two happened is settled by the
// order of two steps on each side: the taker says it is taking the gate and
// then counts the slots; the thread takes its slot and then looks for
// takers. One of them sees the other.
//
// A late thread goes through `LATE` without queueing behind a taker that is
// waiting for it, so a few of them running command after command, their
// turns overlapping, could keep it from a taker for ever — and every taker
// after that one queues behind it. So a taker that cannot have `LATE` within
// `RECOUNT` counts the slots again and takes the ones it missed: the late
// threads then wait at their own slots, `LATE` empties, and the taker gets
// it. Slots only ever get added, at most `SLOTS` of them, so the counting
// ends.
//
// It is a different gate from `write_gate`, which exists so that a snapshot
// can be cut between two whole writes. A cut can last as long as the dataset
// takes to write out, and must not stop reads; this gate is held alone only
// for as long as a transaction or a script runs, and has to.

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

/// More slots than worker threads on any machine this runs on; threads beyond
/// that share slots, which is correct, just slightly less cache-friendly.
const SLOTS: usize = 128;

/// How long a taker waits for `LATE` before it counts the slots again. See
/// the top.
const RECOUNT: std::time::Duration = std::time::Duration::from_millis(2);

/// Padded so neighbouring slots never share a cache line.
#[repr(align(128))]
struct Slot(RwLock<()>);

static GATE: [Slot; SLOTS] = [const { Slot(parking_lot::const_rwlock(())) }; SLOTS];

/// Slots handed out so far, counting each once: `0..IN_USE` are the ones to
/// take to hold the gate alone. Only goes up.
static IN_USE: AtomicUsize = AtomicUsize::new(0);

/// How many are taking the gate alone, or hold it.
static TAKERS: AtomicUsize = AtomicUsize::new(0);

/// The slot of the threads a taker may not have counted. See the top.
static LATE: Slot = Slot(parking_lot::const_rwlock(()));

static OVERFLOW: AtomicUsize = AtomicUsize::new(0);

const UNASSIGNED: usize = usize::MAX;

/// A thread's slot.
///
/// A slot is not given to another thread when its thread ends. A new thread
/// always gets the next one, higher than any a taker that is waiting has
/// already taken — so a thread's first command is never held up behind a
/// taker that is itself waiting for somebody else. Threads that come and go
/// (the blocking pool's) use the slots up in time, and the gate is then what
/// it was before slots were counted: all of them, shared.
struct MySlot {
    slot: Cell<usize>,
    /// A taker may not have counted this thread's slot: it goes through
    /// `LATE` as well.
    late: Cell<bool>,
}

/// Give the calling thread a slot.
#[cold]
fn assign() -> usize {
    let next = IN_USE.fetch_add(1, Ordering::SeqCst);
    let slot = if next < SLOTS {
        next
    } else {
        // More threads than slots: they share, which is correct, just
        // slightly less cache-friendly. Every slot there is is taken by a
        // taker from here on.
        IN_USE.store(SLOTS, Ordering::SeqCst);
        OVERFLOW.fetch_add(1, Ordering::Relaxed) % SLOTS
    };
    MY_SLOT.with(|mine| {
        mine.slot.set(slot);
        // After the slot is taken, not before: see the top.
        mine.late.set(TAKERS.load(Ordering::SeqCst) > 0);
    });
    slot
}

/// The calling thread's slot, and whether it must go through `LATE` too.
#[inline]
fn my_slot() -> (usize, bool) {
    let (slot, late) = MY_SLOT.with(|mine| (mine.slot.get(), mine.late.get()));
    if slot == UNASSIGNED {
        let slot = assign();
        return (slot, MY_SLOT.with(|mine| mine.late.get()));
    }
    if late {
        return (slot, still_late());
    }
    (slot, false)
}

/// Whether a thread that was late still is: not once no taker is left, for
/// every taker from then on counts its slot.
#[cold]
fn still_late() -> bool {
    if TAKERS.load(Ordering::SeqCst) == 0 {
        MY_SLOT.with(|mine| mine.late.set(false));
        false
    } else {
        true
    }
}

/// Counts a taker in for as long as it is taking the gate or holds it.
struct Taking;

impl Taking {
    fn begin() -> Taking {
        TAKERS.fetch_add(1, Ordering::SeqCst);
        Taking
    }
}

impl Drop for Taking {
    fn drop(&mut self) {
        TAKERS.fetch_sub(1, Ordering::SeqCst);
    }
}

thread_local! {
    static MY_SLOT: MySlot = const {
        MySlot {
            slot: Cell::new(UNASSIGNED),
            late: Cell::new(false),
        }
    };
    /// How deep this thread is in commands: a script's `redis.call`, or a
    /// command run by `EXEC`, is a command inside a command.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    /// This thread holds the gate alone.
    static ALONE: Cell<bool> = const { Cell::new(false) };
}

/// Held by a command for as long as it runs.
pub struct Guard {
    /// `None` for a command inside a command: the outermost one on this
    /// thread already holds the slot.
    _slot: Option<RwLockReadGuard<'static, ()>>,
    /// Held as well by a thread a taker may not have counted.
    _late: Option<RwLockReadGuard<'static, ()>>,
    /// Tied to the thread that took it (the depth counter is thread-local).
    _not_send: PhantomData<*const ()>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Enter the gate as an ordinary command.
///
/// Safe to nest on one thread: an inner entry takes nothing, because taking
/// the slot a second time would queue behind anything waiting to take the
/// gate alone — which is itself waiting for the outer guard.
#[inline]
pub fn enter() -> Guard {
    let nested = DEPTH.with(|d| {
        let depth = d.get();
        d.set(depth + 1);
        depth > 0
    });
    let (slot, late) = if nested {
        (None, None)
    } else {
        let (slot, late) = my_slot();
        let slot = GATE[slot].0.read();
        // Not behind a taker that is waiting for `LATE`: that taker may be
        // waiting for a command that is waiting for this one.
        (Some(slot), late.then(|| LATE.0.read_recursive()))
    };
    Guard {
        _slot: slot,
        _late: late,
        _not_send: PhantomData,
    }
}

/// [`enter`], for a caller that will not wait for ever: each time `slice`
/// has passed and the gate is still held alone, `give_up` is asked whether to
/// stop waiting. `None` if it said yes.
///
/// A script holds the gate alone for as long as it runs, and a script may
/// never end. The commands that arrive once it is known to be running are
/// turned away before they get here (`BUSY`); one that arrives as it
/// starts is not, and used to wait here until the script ended — and if it
/// came from the client that was going to kill the script, that was never.
#[inline]
pub fn enter_unless(
    slice: std::time::Duration,
    mut give_up: impl FnMut() -> bool,
) -> Option<Guard> {
    let nested = DEPTH.with(|d| d.get() > 0);
    let (slot, late) = if nested {
        (None, None)
    } else {
        let (slot, late) = my_slot();
        let mut wait_for = |gate: &'static RwLock<()>, late: bool| {
            // Open, nearly always: taken at once, as `enter` takes it.
            let at_once = if late {
                gate.try_read_recursive()
            } else {
                gate.try_read()
            };
            if let Some(held) = at_once {
                return Some(held);
            }
            loop {
                let in_time = if late {
                    gate.try_read_recursive_for(slice)
                } else {
                    gate.try_read_for(slice)
                };
                if let Some(held) = in_time {
                    return Some(held);
                }
                if give_up() {
                    return None;
                }
            }
        };
        let slot = wait_for(&GATE[slot].0, false)?;
        let late = if late {
            Some(wait_for(&LATE.0, true)?)
        } else {
            None
        };
        (Some(slot), late)
    };
    DEPTH.with(|d| d.set(d.get() + 1));
    Some(Guard {
        _slot: slot,
        _late: late,
        _not_send: PhantomData,
    })
}

/// Held by a transaction or a script: no other command is running, and none
/// starts, until this is dropped.
pub struct AloneGuard {
    // Let go of in this order: the slots, and then being counted a taker.
    _slots: Vec<RwLockWriteGuard<'static, ()>>,
    _taking: Option<Taking>,
    /// Whether this guard is the one that closed the gate.
    outermost: bool,
    _not_send: PhantomData<*const ()>,
}

impl Drop for AloneGuard {
    fn drop(&mut self) {
        if self.outermost {
            ALONE.with(|a| a.set(false));
        }
        DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Enter the gate alone: wait for every command in flight to finish, and hold
/// back new ones, until the guard is dropped.
///
/// Slots are taken in index order, so two transactions queue behind one
/// another instead of deadlocking.
///
/// From inside a command on this thread — a script queued in a transaction,
/// a script calling a script — it takes nothing: the thread cannot wait for
/// itself, and whatever the outer command holds is what orders it.
pub fn enter_alone() -> AloneGuard {
    let nested = DEPTH.with(|d| {
        let depth = d.get();
        d.set(depth + 1);
        depth > 0
    });
    if nested {
        return AloneGuard {
            _slots: Vec::new(),
            _taking: None,
            outermost: false,
            _not_send: PhantomData,
        };
    }
    // Said first, and the slots counted after: see the top.
    let taking = Taking::begin();
    let slots = close(None).expect("a taker without a way to give up gets the gate");
    ALONE.with(|a| a.set(true));
    AloneGuard {
        _slots: slots,
        _taking: Some(taking),
        outermost: true,
        _not_send: PhantomData,
    }
}

/// [`enter_alone`], for a caller that will not wait for ever — a second
/// script, arriving as the first starts. See [`enter_unless`].
pub fn enter_alone_unless(
    slice: std::time::Duration,
    mut give_up: impl FnMut() -> bool,
) -> Option<AloneGuard> {
    if DEPTH.with(|d| d.get() > 0) {
        return Some(enter_alone());
    }
    let taking = Taking::begin();
    // On giving up, the slots taken so far are let go of inside.
    let slots = close(Some((slice, &mut give_up)))?;
    DEPTH.with(|d| d.set(d.get() + 1));
    ALONE.with(|a| a.set(true));
    Some(AloneGuard {
        _slots: slots,
        _taking: Some(taking),
        outermost: true,
        _not_send: PhantomData,
    })
}

/// Take every slot in use, in index order, and then `LATE`; count again and
/// take the slots added meanwhile whenever `LATE` cannot be had within
/// `RECOUNT` (see the top). With `patience`, a slot that is not free is
/// waited for a `slice` at a time, and `give_up` is asked after each slice,
/// and after each wait for `LATE`: `None` if it said yes, with everything
/// taken let go of again.
fn close(
    mut patience: Option<(std::time::Duration, &mut dyn FnMut() -> bool)>,
) -> Option<Vec<RwLockWriteGuard<'static, ()>>> {
    let mut slots = Vec::with_capacity(IN_USE.load(Ordering::SeqCst).min(SLOTS) + 1);
    loop {
        // Only goes up, so never below what is already held.
        let in_use = IN_USE.load(Ordering::SeqCst).min(SLOTS);
        for slot in &GATE[slots.len()..in_use] {
            let held = match &mut patience {
                None => slot.0.write(),
                Some((slice, give_up)) => match slot.0.try_write() {
                    Some(held) => held,
                    None => loop {
                        if let Some(held) = slot.0.try_write_for(*slice) {
                            break held;
                        }
                        if give_up() {
                            return None;
                        }
                    },
                },
            };
            slots.push(held);
        }
        if let Some(late) = LATE.0.try_write_for(RECOUNT) {
            slots.push(late);
            return Some(slots);
        }
        if let Some((_, give_up)) = &mut patience
            && give_up()
        {
            return None;
        }
    }
}

/// Whether the calling thread holds the gate alone — whether what it is
/// running is part of a transaction or a script.
#[inline]
pub fn alone_on_this_thread() -> bool {
    ALONE.with(|a| a.get())
}

/// Commands that do not go through the gate.
///
/// * The ones a client must be able to send *while a script is running*:
///   that is what they are for.
/// * The ones that wait — for replicas, for the network — for as long as
///   their timeout. Holding the gate while they do would hold up every
///   transaction on the server for that long, and they touch no data while
///   they wait.
#[inline(always)]
pub fn skips(name: &str, cmd: &[&[u8]]) -> bool {
    let sub = |word: &[u8]| cmd.get(1).is_some_and(|s| s.eq_ignore_ascii_case(word));
    match name {
        "WAIT" | "WAITAOF" | "MIGRATE" | "SHUTDOWN" | "DEBUG" => true,
        "SCRIPT" => sub(b"KILL"),
        "FUNCTION" => sub(b"KILL") || sub(b"STATS"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::Duration;

    /// While the gate is held alone, no other command is between its start
    /// and its end.
    #[test]
    fn nothing_else_runs_while_the_gate_is_held_alone() {
        let in_flight = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let workers: Vec<_> = (0..6)
            .map(|_| {
                let in_flight = Arc::clone(&in_flight);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let _guard = enter();
                        in_flight.fetch_add(1, Ordering::SeqCst);
                        std::thread::yield_now();
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for _ in 0..200 {
            let _alone = enter_alone();
            assert!(alone_on_this_thread());
            for _ in 0..20 {
                assert_eq!(in_flight.load(Ordering::SeqCst), 0);
            }
        }
        assert!(!alone_on_this_thread());
        stop.store(true, Ordering::Relaxed);
        for w in workers {
            w.join().unwrap();
        }
    }

    /// A command inside a command takes nothing, and a transaction started
    /// from inside one does not wait for itself.
    #[test]
    fn nesting_does_not_deadlock() {
        let outer = enter_alone();
        let inner = enter();
        let script_in_transaction = enter_alone();
        assert!(alone_on_this_thread());
        drop(script_in_transaction);
        // Still the outer transaction's.
        assert!(alone_on_this_thread());
        drop(inner);
        drop(outer);
        assert!(!alone_on_this_thread());

        let command = enter();
        let nested = enter_alone();
        assert!(!alone_on_this_thread(), "it could not close the gate");
        drop(nested);
        drop(command);
        // And the gate is free again.
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        let t = std::thread::spawn(move || {
            let _alone = enter_alone();
            flag.store(true, Ordering::SeqCst);
        });
        t.join().unwrap();
        assert!(done.load(Ordering::SeqCst));
    }

    #[test]
    fn commands_do_not_wait_for_each_other() {
        let _mine = enter();
        let entered = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&entered);
        std::thread::spawn(move || {
            let _theirs = enter();
            flag.store(true, Ordering::SeqCst);
        })
        .join()
        .unwrap();
        assert!(entered.load(Ordering::SeqCst));
        std::thread::sleep(Duration::from_millis(1));
    }

    #[test]
    fn the_commands_that_must_get_through() {
        assert!(skips("SCRIPT", &[b"SCRIPT", b"kill"]));
        assert!(!skips("SCRIPT", &[b"SCRIPT", b"LOAD", b"return 1"]));
        assert!(skips("FUNCTION", &[b"FUNCTION", b"STATS"]));
        assert!(skips("WAIT", &[b"WAIT", b"1", b"0"]));
        assert!(skips("MIGRATE", &[b"MIGRATE"]));
        assert!(!skips("GET", &[b"GET", b"k"]));
        assert!(!skips("EXEC", &[b"EXEC"]));
    }

    /// A command that will not wait for ever is let through when the gate
    /// opens, and told so when it is asked to give up first.
    #[test]
    fn a_wait_for_the_gate_can_be_given_up() {
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _alone = enter_alone();
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        held_rx.recv().unwrap();

        let slice = Duration::from_millis(5);
        let mut asked = 0;
        assert!(
            enter_unless(slice, || {
                asked += 1;
                asked == 3
            })
            .is_none()
        );
        assert_eq!(asked, 3);
        assert!(enter_alone_unless(slice, || true).is_none());
        // Having given up, it holds nothing: the next one is not nested.
        assert!(!alone_on_this_thread());

        release_tx.send(()).unwrap();
        holder.join().unwrap();
        // The gate is the process's: another test running beside this one
        // may hold it for a moment, so these wait rather than give up.
        assert!(enter_unless(slice, || false).is_some());
        let alone = enter_alone_unless(slice, || false).expect("the gate is open");
        assert!(alone_on_this_thread());
        // Inside it, a command of the same thread is let in at once.
        assert!(enter_unless(slice, || panic!("it does not wait")).is_some());
        drop(alone);
        assert!(!alone_on_this_thread());
    }
}

#[cfg(test)]
mod late_threads {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::Duration;

    /// Threads that arrive while a taker is on its way, and then run command
    /// after command, do not keep `LATE` from it for ever. They used to: the
    /// taker had counted the slots before they had any, their turns through
    /// `LATE` overlapped, and every taker after it queued behind it.
    #[test]
    fn late_threads_running_commands_back_to_back_do_not_starve_a_taker() {
        // A command under way: the taker below counts its slot, and waits for
        // it. While the taker waits, threads that arrive are late.
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _command = enter();
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        held_rx.recv().unwrap();

        let (got_tx, got_rx) = std::sync::mpsc::channel();
        let taker = std::thread::spawn(move || {
            let _alone = enter_alone();
            got_tx.send(()).unwrap();
        });
        // Let it count the slots and queue behind the holder.
        std::thread::sleep(Duration::from_millis(50));

        let stop = Arc::new(AtomicBool::new(false));
        let crowd: Vec<_> = (0..6)
            .map(|_| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let _command = enter();
                        std::thread::yield_now();
                    }
                })
            })
            .collect();
        // Running command after command through `LATE` by now.
        std::thread::sleep(Duration::from_millis(50));
        release_tx.send(()).unwrap();
        holder.join().unwrap();

        let got_in = got_rx.recv_timeout(Duration::from_secs(20)).is_ok();
        stop.store(true, Ordering::Relaxed);
        for thread in crowd {
            thread.join().unwrap();
        }
        assert!(got_in, "the taker never got the gate");
        taker.join().unwrap();
    }

    /// Threads that run their first command while the gate is being taken
    /// alone are kept out as well as the ones that were there before: the
    /// taker did not count their slots, and they go through `LATE`.
    #[test]
    fn a_thread_that_starts_while_the_gate_is_taken_is_kept_out() {
        let in_flight = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        // Short-lived threads, one after another, each new to the gate.
        let spawners: Vec<_> = (0..4)
            .map(|_| {
                let in_flight = Arc::clone(&in_flight);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let in_flight = Arc::clone(&in_flight);
                        std::thread::spawn(move || {
                            for _ in 0..3 {
                                let _command = enter();
                                in_flight.fetch_add(1, Ordering::SeqCst);
                                std::hint::spin_loop();
                                in_flight.fetch_sub(1, Ordering::SeqCst);
                            }
                        })
                        .join()
                        .unwrap();
                    }
                })
            })
            .collect();
        for _ in 0..2000 {
            let _alone = enter_alone();
            assert_eq!(
                in_flight.load(Ordering::SeqCst),
                0,
                "a command ran beside it"
            );
            std::hint::spin_loop();
            assert_eq!(
                in_flight.load(Ordering::SeqCst),
                0,
                "a command started beside it"
            );
        }
        stop.store(true, Ordering::Relaxed);
        for spawner in spawners {
            spawner.join().unwrap();
        }
    }
}
