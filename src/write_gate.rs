// src/write_gate.rs
//
// A gate that lets a full resync or a checkpoint take an exact cut through the
// write stream.
//
// A write has two halves that are not atomic with each other: it mutates the
// store under a shard lock, and then — after that lock is released — it hands
// its replication payload to the hub and its record to the AOF. Anything that
// snapshots the dataset *and* starts a stream at "now" lands between those two
// halves for some writes: their effect is already in the snapshot, and their
// payload arrives in the stream afterwards. The receiver applies them twice.
// For `SET` that is harmless. For `INCR`, `RPUSH` or `APPEND` it is silent
// divergence — a replica that attached under load came up with counters ahead
// of its master and duplicated list elements, and an AOF checkpoint taken
// under load replayed the same way.
//
// Writers hold this gate, shared, across both halves. A cutter takes it
// exclusively, which waits out every write in flight and holds new ones back,
// so at the cut every write is either wholly before it (in the snapshot, not in
// the stream) or wholly after (in the stream, not in the snapshot).
//
// The shared side has to be nearly free: it is on the path of every write
// command. A single `RwLock` would have every worker thread bouncing one cache
// line. Instead there is one lock per slot and each thread is pinned to a slot,
// so the shared acquisition is an uncontended operation on a line only that
// thread touches. The exclusive side pays for it by taking every slot, which
// is fine — it runs once per replica attach or checkpoint.

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

/// More slots than worker threads on any machine this runs on; threads beyond
/// that share slots, which is correct, just slightly less cache-friendly.
const SLOTS: usize = 128;

/// Padded so neighbouring slots never share a cache line.
#[repr(align(128))]
struct Slot(RwLock<()>);

static GATE: [Slot; SLOTS] = [const { Slot(parking_lot::const_rwlock(())) }; SLOTS];

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static MY_SLOT: usize = NEXT_SLOT.fetch_add(1, Ordering::Relaxed) % SLOTS;
    /// How many `WriteGuard`s this thread currently holds.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Held by a write command from before it touches the store until its
/// replication / AOF side effects have been queued.
pub struct WriteGuard {
    /// `None` for a nested entry: the outermost guard on this thread already
    /// holds the slot.
    _slot: Option<RwLockReadGuard<'static, ()>>,
    /// Guards are tied to the thread that took them (the depth counter is
    /// thread-local), so they must not be sent to another.
    _not_send: PhantomData<*const ()>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Enter the gate as a writer.
///
/// Safe to nest on one thread: an inner entry is a no-op, because taking the
/// slot's lock a second time would queue behind any cutter waiting on it —
/// which is itself waiting for the outer guard to be released.
#[inline]
pub fn enter() -> WriteGuard {
    let nested = DEPTH.with(|d| {
        let depth = d.get();
        d.set(depth + 1);
        depth > 0
    });
    let slot = if nested {
        None
    } else {
        Some(GATE[MY_SLOT.with(|s| *s)].0.read())
    };
    WriteGuard {
        _slot: slot,
        _not_send: PhantomData,
    }
}

/// Whether the calling thread is inside a write (holds a [`WriteGuard`]).
pub fn held_by_this_thread() -> bool {
    DEPTH.with(|d| d.get() > 0)
}

/// Held while a cut is taken. No write command is in flight, and none can
/// start, until this is dropped.
pub struct CutGuard(#[allow(dead_code)] Vec<RwLockWriteGuard<'static, ()>>);

/// Close the gate: wait for every in-flight write to finish, and hold back new
/// ones until the returned guard is dropped.
///
/// Slots are always taken in index order, so two concurrent cutters queue
/// behind one another instead of deadlocking.
///
/// Returns `None` when called from inside a write on this thread — a script
/// calling `SAVE`, say. Waiting there would be waiting for ourselves; the
/// caller proceeds without a cut, which is what it did before the gate existed.
pub fn cut() -> Option<CutGuard> {
    if held_by_this_thread() {
        return None;
    }
    Some(CutGuard(GATE.iter().map(|slot| slot.0.write()).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::Duration;

    /// The property everything else rests on: while a cut is held, no writer
    /// is between "mutated" and "propagated".
    #[test]
    fn a_cut_never_observes_a_half_finished_write() {
        let applied = Arc::new(AtomicU64::new(0));
        let propagated = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let writers: Vec<_> = (0..4)
            .map(|_| {
                let applied = Arc::clone(&applied);
                let propagated = Arc::clone(&propagated);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let _gate = enter();
                        applied.fetch_add(1, Ordering::SeqCst);
                        // The window a cut must never land in.
                        std::thread::yield_now();
                        propagated.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for _ in 0..200 {
            let _cut = cut();
            let a = applied.load(Ordering::SeqCst);
            let p = propagated.load(Ordering::SeqCst);
            assert_eq!(
                a, p,
                "a write was applied but not yet propagated at the cut"
            );
        }

        stop.store(true, Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
    }

    /// A write nested inside another on the same thread must not take the slot
    /// again, and a cut requested from inside a write must not wait for itself.
    #[test]
    fn nesting_on_one_thread_cannot_self_deadlock() {
        let outer = enter();
        assert!(held_by_this_thread());
        {
            let _inner = enter();
            assert!(held_by_this_thread());
        }
        assert!(held_by_this_thread(), "the outer guard is still held");
        assert!(
            cut().is_none(),
            "a cut from inside a write must be declined"
        );
        drop(outer);
        assert!(!held_by_this_thread());
        assert!(cut().is_some());
    }

    /// Writers must not be blocked by each other, only by a cut.
    #[test]
    fn writers_do_not_exclude_one_another() {
        let _a = enter();
        let done = Arc::new(AtomicBool::new(false));
        let done2 = Arc::clone(&done);
        let t = std::thread::spawn(move || {
            let _b = enter();
            done2.store(true, Ordering::SeqCst);
        });
        for _ in 0..200 {
            if done.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(done.load(Ordering::SeqCst), "a second writer was blocked");
        t.join().unwrap();
    }
}
