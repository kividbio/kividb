// src/write_order.rs
//
// Writes to one key reach replicas and the AOF in the order the store took
// them.
//
// A write has two halves: it changes the store under a shard lock, and then,
// with that lock released, hands its payload to the replication hub and the
// AOF. Two connections writing the *same key* at the same moment could do
// those halves crossed over — A changes the store, B changes the store, B
// hands over, A hands over. The master then holds A-then-B and every replica,
// and the AOF, B-then-A. For `SET` the last value differs; for `RPUSH` the
// order of the list; for `APPEND` the string. Nothing ever corrects it.
//
// Measured before this existed: six clients appending to one list, one
// replica, no faults of any kind — about 500 of 18 000 elements out of place
// on the replica, every run.
//
// The write gate (`write_gate`) does not prevent this: it is shared between
// writers, and exists to give a cutter a point with no write in flight.
//
// So a write that somebody is listening to holds a lock on each key it names,
// from before it touches the store until its payload is queued. The locks are
// stripes — a fixed table indexed by a hash of the key — so two writes to the
// same key always meet on the same lock, and writes to different keys almost
// never do. A write that names no key but changes many (`FLUSHALL`), and a
// script, which may touch keys it did not declare, take the write gate
// exclusively instead ([`write_gate::enter_exclusive`]).
//
// It is taken when nobody is listening too — no replica, no backlog, no AOF,
// no backup. There is then no second order to disagree with, but a write
// must still run whole before or after every other write to its keys, and
// not every command does all of its reading and writing under one hold of
// its shard's lock: `PFADD` read the registers under one and wrote them
// back under another, and two of them at once lost elements (#271). Holding
// the order of a key says nothing about propagating the write: whether
// anything is listening decides that, separately (`write_is_observed`).
//
// A few commands need less: they do the whole of their reading and writing
// of their one key under one hold of its shard's lock, so two of them on one
// key already run one before the other (`SADD`, `HSET`, `LPUSH`, `INCR`,
// `PFADD`, ...; see `handler::atomic_under_its_shard`). While nothing is
// listening, such a write takes its key's stripe *shared*
// ([`lock_key_shared`]): beside the others like it, which the shard orders,
// and never beside a write that holds the stripe whole — every other write,
// an expiry, an eviction, the field-expiry sweep. Holding one lock whole for
// the whole of every command on a hot key halved what one key could take
// (`SADD` 645k a second to 339k, 12 threads): the commands queued for it,
// one at a time, where the shard had let them overlap but for the moment
// each changes the value.

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cell::Cell;
use std::marker::PhantomData;

/// A power of two. Large enough that unrelated keys rarely share a stripe.
const STRIPES: usize = 4096;

static STRIPE: [RwLock<()>; STRIPES] = [const { parking_lot::const_rwlock(()) }; STRIPES];

/// [`HELD`]: no guard on this thread.
const FREE: u8 = 0;
/// [`HELD`]: a guard that holds no stripe — of a write that names no key
/// (`FT.ALTER`, `FT.CREATE`), or [`held_exclusively`].
const NO_STRIPE: u8 = 1;
/// [`HELD`]: a guard that holds stripes.
const STRIPES_HELD: u8 = 2;

thread_local! {
    /// Set while this thread holds an [`OrderGuard`], so that a write made
    /// from inside another — a script's `redis.call`, the commands of a
    /// transaction applied as one — does not try to take a stripe its own
    /// thread already holds. One of [`FREE`], [`NO_STRIPE`] and
    /// [`STRIPES_HELD`].
    static HELD: Cell<u8> = const { Cell::new(FREE) };
}

/// Held by a write from before it touches the store until its replication /
/// AOF payload has been queued.
pub struct OrderGuard {
    /// The stripe of a single-key write — nearly all of them — held without
    /// allocating.
    _one: Option<RwLockWriteGuard<'static, ()>>,
    /// Or held shared, by a write that only has to keep the others out
    /// ([`lock_key_shared`]).
    _shared: Option<RwLockReadGuard<'static, ()>>,
    _stripes: Vec<RwLockWriteGuard<'static, ()>>,
    /// Whether this guard is the one that set [`HELD`].
    outermost: bool,
    /// Tied to the thread that took it.
    _not_send: PhantomData<*const ()>,
}

impl OrderGuard {
    /// A guard that holds no stripe.
    fn holding_none(outermost: bool) -> Self {
        OrderGuard {
            _one: None,
            _shared: None,
            _stripes: Vec::new(),
            outermost,
            _not_send: PhantomData,
        }
    }
}

impl Drop for OrderGuard {
    fn drop(&mut self) {
        if self.outermost {
            HELD.with(|h| h.set(FREE));
        }
    }
}

fn stripe_of(db: usize, key: &[u8]) -> usize {
    (xxhash_rust::xxh3::xxh3_64_with_seed(key, db as u64) as usize) & (STRIPES - 1)
}

/// Take the order lock of every key in `keys`, in database `db`.
///
/// Stripes are taken in index order, so two writes that share several keys
/// cannot deadlock. Nested on one thread, it takes nothing: the outer write's
/// guard already orders everything this thread does until it is dropped.
pub fn lock_keys<'a>(db: usize, keys: impl Iterator<Item = &'a [u8]>) -> OrderGuard {
    if HELD.with(|h| h.get()) != FREE {
        return nested();
    }
    let mut keys = keys;
    let first = keys.next().map(|key| stripe_of(db, key));
    let held = if first.is_some() {
        STRIPES_HELD
    } else {
        NO_STRIPE
    };
    HELD.with(|h| h.set(held));
    let mut guard = OrderGuard::holding_none(true);
    match (first, keys.next()) {
        (None, _) => {}
        (Some(only), None) => guard._one = Some(STRIPE[only].write()),
        (Some(first), Some(second)) => {
            let mut stripes: Vec<usize> = vec![first, stripe_of(db, second)];
            stripes.extend(keys.map(|key| stripe_of(db, key)));
            stripes.sort_unstable();
            stripes.dedup();
            guard._stripes = stripes.into_iter().map(|i| STRIPE[i].write()).collect();
        }
    }
    guard
}

/// The order lock of `key`, in database `db`, held shared: for a write that
/// nothing listens to, by a command that reads and writes its one key under
/// one hold of the key's shard lock, so that two such writes of the key are
/// ordered by the shard already (see the module). It waits for, and keeps
/// out, every write that holds the key's order whole ([`lock_keys`]): one
/// that reads the key under one hold of the shard and writes it under
/// another (`SUNIONSTORE`, `BITFIELD`), one that may empty it (`SREM`), an
/// expiry, an eviction. Nested, it takes nothing, as [`lock_keys`] does.
pub fn lock_key_shared(db: usize, key: &[u8]) -> OrderGuard {
    if HELD.with(|h| h.get()) != FREE {
        return nested();
    }
    HELD.with(|h| h.set(STRIPES_HELD));
    let mut guard = OrderGuard::holding_none(true);
    guard._shared = Some(STRIPE[stripe_of(db, key)].read());
    guard
}

/// [`lock_keys`] for keys of several databases, each `(db, key)`: for a
/// write that puts a key in another database than the one it names it in
/// (`MOVE`, `COPY ... DB`). Its record is then ordered against the writes to
/// the key in either database, the expiry of the key where it went among
/// them: one that took the key there before the `MOVE` was queued sent its
/// `DEL` ahead of the `MOVE`, and the replica, and a replay of the AOF, kept
/// the key there for good.
pub fn lock_keys_in<'a>(keys: impl Iterator<Item = (usize, &'a [u8])>) -> OrderGuard {
    if HELD.with(|h| h.get()) != FREE {
        return nested();
    }
    let mut stripes: Vec<usize> = keys.map(|(db, key)| stripe_of(db, key)).collect();
    stripes.sort_unstable();
    stripes.dedup();
    let held = if stripes.is_empty() {
        NO_STRIPE
    } else {
        STRIPES_HELD
    };
    HELD.with(|h| h.set(held));
    let mut guard = OrderGuard::holding_none(true);
    guard._stripes = stripes.into_iter().map(|i| STRIPE[i].write()).collect();
    guard
}

/// Mark this thread as ordered without taking any stripe — for a write that
/// holds the write gate exclusively, and so has no other write to be ordered
/// against. Writes nested inside it then take nothing either.
pub fn held_exclusively() -> OrderGuard {
    let outermost = HELD.with(|h| {
        let free = h.get() == FREE;
        if free {
            h.set(NO_STRIPE);
        }
        free
    });
    OrderGuard::holding_none(outermost)
}

/// The order of one key, for a walk over many keys (the backfill of a
/// search index) that a command makes one key at a time: each key's write
/// order is held while the walk reads its value and indexes it, so that a
/// write to the key falls before the read, or after the indexing, and then
/// indexes the key itself.
///
/// Unlike [`lock_keys`], it takes the stripe from inside a guard that holds
/// none: the command's own, which names no key (`FT.ALTER`, `FT.CREATE`), or
/// [`held_exclusively`]'s. One stripe taken while none is held cannot
/// deadlock. Inside a guard that holds stripes (a transaction that also
/// writes keys) it takes nothing, as [`lock_keys`] would: the key's stripe
/// may be one of them.
pub fn lock_key_for_walk(db: usize, key: &[u8]) -> OrderGuard {
    match HELD.with(|h| h.get()) {
        FREE => lock_keys(db, std::iter::once(key)),
        NO_STRIPE => {
            let mut guard = OrderGuard::holding_none(false);
            guard._one = Some(STRIPE[stripe_of(db, key)].write());
            guard
        }
        _ => nested(),
    }
}

/// The guard of a write made from inside another: it takes nothing.
fn nested() -> OrderGuard {
    OrderGuard::holding_none(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[test]
    fn two_writes_to_one_key_take_turns() {
        let guard = lock_keys(0, [b"k".as_slice()].into_iter());
        let entered = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&entered);
        let other = std::thread::spawn(move || {
            let _guard = lock_keys(0, [b"other".as_slice(), b"k".as_slice()].into_iter());
            flag.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(!entered.load(Ordering::SeqCst), "the second write got in");
        drop(guard);
        other.join().unwrap();
        assert!(entered.load(Ordering::SeqCst));
    }

    #[test]
    fn shared_holds_of_one_key_overlap() {
        let guard = lock_key_shared(0, b"s");
        std::thread::spawn(|| {
            let _guard = lock_key_shared(0, b"s");
        })
        .join()
        .unwrap();
        drop(guard);
    }

    #[test]
    fn a_shared_hold_and_a_whole_one_take_turns() {
        // The whole hold waits for the shared one.
        let shared = lock_key_shared(0, b"t");
        let entered = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&entered);
        let whole = std::thread::spawn(move || {
            let _guard = lock_keys(0, [b"t".as_slice()].into_iter());
            flag.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(!entered.load(Ordering::SeqCst), "the whole hold got in");
        drop(shared);
        whole.join().unwrap();
        assert!(entered.load(Ordering::SeqCst));

        // And the shared one for the whole one.
        let whole = lock_keys(0, [b"u".as_slice(), b"t".as_slice()].into_iter());
        let entered = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&entered);
        let shared = std::thread::spawn(move || {
            let _guard = lock_key_shared(0, b"t");
            flag.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(!entered.load(Ordering::SeqCst), "the shared hold got in");
        drop(whole);
        shared.join().unwrap();
        assert!(entered.load(Ordering::SeqCst));
    }

    #[test]
    fn a_shared_hold_nested_in_a_whole_one_takes_nothing() {
        let outer = lock_keys(0, [b"v".as_slice()].into_iter());
        // On this thread: it would wait for itself if it took the stripe.
        let inner = lock_key_shared(0, b"v");
        drop(inner);
        assert_eq!(HELD.with(|h| h.get()), STRIPES_HELD);
        drop(outer);
        assert_eq!(HELD.with(|h| h.get()), FREE);
        let shared = lock_key_shared(0, b"v");
        assert_eq!(HELD.with(|h| h.get()), STRIPES_HELD);
        let inner = lock_keys(0, [b"v".as_slice()].into_iter());
        drop(inner);
        // Nor does a walk's hold: the key's stripe may be the one held.
        drop(lock_key_for_walk(0, b"v"));
        drop(shared);
        assert_eq!(HELD.with(|h| h.get()), FREE);
    }

    #[test]
    fn a_write_to_a_key_in_two_databases_orders_both() {
        let guard = lock_keys_in([(0, b"m".as_slice()), (3, b"m".as_slice())].into_iter());
        let entered = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&entered);
        // An expiry of the key in database 3.
        let other = std::thread::spawn(move || {
            let _guard = lock_keys(3, [b"m".as_slice()].into_iter());
            flag.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(!entered.load(Ordering::SeqCst), "the expiry got in");
        drop(guard);
        other.join().unwrap();
        assert!(entered.load(Ordering::SeqCst));
    }

    #[test]
    fn writes_to_different_keys_do_not_wait() {
        // Different stripes, by construction of the names.
        let (a, b) = (b"order:a".as_slice(), b"order:b".as_slice());
        assert_ne!(stripe_of(0, a), stripe_of(0, b));
        let _guard = lock_keys(0, [a].into_iter());
        std::thread::spawn(move || {
            let _guard = lock_keys(0, [b].into_iter());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn nested_and_repeated_keys_do_not_deadlock() {
        let outer = lock_keys(
            0,
            [b"n".as_slice(), b"n".as_slice(), b"m".as_slice()].into_iter(),
        );
        // The same key again, from inside: nothing is taken.
        let inner = lock_keys(0, [b"n".as_slice()].into_iter());
        drop(inner);
        // The outer guard is still the one in charge.
        assert_eq!(HELD.with(|h| h.get()), STRIPES_HELD);
        drop(outer);
        assert_eq!(HELD.with(|h| h.get()), FREE);

        let exclusive = held_exclusively();
        let inner = lock_keys(0, [b"n".as_slice()].into_iter());
        drop(inner);
        assert_eq!(HELD.with(|h| h.get()), NO_STRIPE);
        drop(exclusive);
        assert_eq!(HELD.with(|h| h.get()), FREE);
    }

    #[test]
    fn a_walk_inside_a_command_that_names_no_key_orders_each_key() {
        // The guard of `FT.ALTER`: no key, no stripe.
        let command = lock_keys(0, std::iter::empty());
        let walking = lock_key_for_walk(0, b"doc:1");
        let entered = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&entered);
        // An `HSET` of the key the walk is at.
        let write = std::thread::spawn(move || {
            let _guard = lock_keys(0, [b"doc:1".as_slice()].into_iter());
            flag.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(!entered.load(Ordering::SeqCst), "the write got in");
        drop(walking);
        write.join().unwrap();
        assert!(entered.load(Ordering::SeqCst));
        // The command's guard is still the one in charge.
        assert_eq!(HELD.with(|h| h.get()), NO_STRIPE);
        drop(command);
        assert_eq!(HELD.with(|h| h.get()), FREE);

        // Inside a guard with stripes it takes nothing: no deadlock on a
        // stripe this thread holds.
        let outer = lock_keys(0, [b"doc:1".as_slice()].into_iter());
        drop(lock_key_for_walk(0, b"doc:1"));
        drop(outer);
        // With nothing held, it is an ordinary guard.
        drop(lock_key_for_walk(0, b"doc:1"));
        assert_eq!(HELD.with(|h| h.get()), FREE);
    }

    #[test]
    fn opposite_key_orders_do_not_deadlock() {
        let threads: Vec<_> = (0..8)
            .map(|t| {
                std::thread::spawn(move || {
                    for i in 0..2000 {
                        let keys: [&[u8]; 3] = if (t + i) % 2 == 0 {
                            [b"x", b"y", b"z"]
                        } else {
                            [b"z", b"y", b"x"]
                        };
                        let _guard = lock_keys(0, keys.into_iter());
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
    }
}
