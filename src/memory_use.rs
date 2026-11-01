//! How much memory the process has allocated: what `maxmemory` is measured
//! against, and `used_memory` in `INFO`.
//!
//! The resident set size, which is what both used to be, cannot serve: it is
//! what the operating system has given the process, not what the process is
//! using, and it does not go down when keys are deleted — the allocator keeps
//! the pages. A server that evicts keys to get under its limit has to see
//! that it got there.
//!
//! So the allocator counts. It is the system allocator with a counter in
//! front: each thread adds to a counter of its own (a cache line apiece, so
//! that threads do not contend), and the total is their sum. A thread's
//! counter goes negative when it frees what another allocated; only the sum
//! means anything.
//!
//! What is counted is what was asked for (a slab block at its class's size).
//! What the process holds besides is free space: in the slabs, between
//! blocks given back, and in the system allocator's heap. Near the limit the
//! slabs keep theirs small (see [`near_limit`]), so that a process at its
//! `maxmemory` holds about that much (#202).

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

/// What the counter is in front of: the system allocator, or mimalloc on
/// Linux when built with the `mimalloc` feature.
#[cfg(all(target_os = "linux", feature = "mimalloc"))]
static INNER: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(not(all(target_os = "linux", feature = "mimalloc")))]
static INNER: std::alloc::System = std::alloc::System;

/// Counters that are each one thread's own: only that thread writes it, so
/// it adds with a load and a store. An atomic add is a locked instruction
/// (on arm64 a call to one), paid on every allocation and every free: about
/// a twentieth of the time of a server running scripts, when measured.
///
/// A thread keeps its counter when it ends (what it counted is still
/// allocated, or was freed by somebody else, who counted that), so threads
/// that come and go use them up. The ones after that, and a thread whose
/// thread-locals are already gone, share the last counter and add to it
/// atomically.
const OWNED: usize = 512;
const SHARED: usize = OWNED;

#[repr(align(128))]
struct Stripe(AtomicIsize);

static COUNTS: [Stripe; OWNED + 1] = [const { Stripe(AtomicIsize::new(0)) }; OWNED + 1];
static THREADS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    // No destructor and a constant initialiser: reading it allocates nothing,
    // which matters to code that runs inside the allocator.
    static MINE: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[inline]
fn count(delta: isize) {
    let stripe = MINE
        .try_with(|mine| {
            let mut stripe = mine.get();
            if stripe == usize::MAX {
                stripe = THREADS.fetch_add(1, Ordering::Relaxed).min(SHARED);
                mine.set(stripe);
            }
            stripe
        })
        // A thread on its way out, its thread-locals gone.
        .unwrap_or(SHARED);
    let counter = &COUNTS[stripe].0;
    if stripe < OWNED {
        // This thread's own: nobody else writes it.
        counter.store(
            counter.load(Ordering::Relaxed).wrapping_add(delta),
            Ordering::Relaxed,
        );
    } else {
        counter.fetch_add(delta, Ordering::Relaxed);
    }
}

/// The allocator, counting. Installed as the global allocator by the
/// crate root.
pub struct Counting;

// SAFETY: every call is passed straight to `INNER`, which upholds the
// contract; the counting touches only atomics and a thread-local that needs
// no allocation.
unsafe impl GlobalAlloc for Counting {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { INNER.alloc(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { INNER.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { INNER.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { INNER.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            count(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

/// Count `delta` bytes as allocated (freed, if negative) by what is not the
/// global allocator: the slabs of string values (`value_slab`), which count
/// each block they hand out.
#[inline]
pub fn note(delta: isize) {
    count(delta);
}

/// Bytes allocated and not yet freed, by the whole process.
pub fn allocated() -> usize {
    // The counters that have been given out, and the shared one.
    let owned = THREADS.load(Ordering::Relaxed).min(OWNED);
    let total: isize = COUNTS[..owned]
        .iter()
        .chain(std::iter::once(&COUNTS[SHARED]))
        .map(|stripe| stripe.0.load(Ordering::Relaxed))
        .fold(0isize, |sum, count| sum.wrapping_add(count));
    total.max(0) as usize
}

/// What the calling thread has allocated less what it has freed, while it
/// has a counter of its own (`None` once it shares the last one): unlike
/// [`allocated`], not moved by what other threads allocate meanwhile. For
/// tests that measure what an operation allocates in a process whose other
/// threads (a test harness's) allocate at the same time.
pub fn allocated_by_this_thread() -> Option<isize> {
    let stripe = MINE.try_with(Cell::get).ok()?;
    if stripe == usize::MAX {
        // Nothing counted yet: its counter, when it is given one, starts at
        // nought.
        return Some(0);
    }
    (stripe < OWNED).then(|| COUNTS[stripe].0.load(Ordering::Relaxed))
}

/// Bytes allocated that deleting keys would not give back: Redis's
/// `mem_not_counted_for_evict`. `maxmemory` is measured against what is
/// allocated less these ([`counted`]).
///
/// What a snapshot keeps while it is written, which in Redis is the forked
/// child's, and outside the server's `used_memory`:
///
/// - entries the table has let go of (a key written over, deleted or
///   evicted) that a frozen snapshot still holds (`store::entry`, which
///   adds and takes away here as the table lets go of a held entry and its
///   last hold goes);
/// - what a snapshot lays out besides: the values it encodes at the cut,
///   and a full-sync or backup image built in memory ([`NotCounted`]).
///
/// And, kept apart, what is queued for replicas ([`set_replica_queued`]):
/// Redis does not count its replicas' output buffers either.
///
/// Without this, a write during a snapshot found memory over the limit and
/// evicted a key, which gave nothing back while the snapshot held it, and
/// the next, until nearly every key was gone (#284).
///
/// One counter for the process: it changes only while something holds an
/// entry the table has let go of, or lays a snapshot out, never on a plain
/// write.
#[repr(align(128))]
struct Shared(AtomicIsize);
static NOT_COUNTED: Shared = Shared(AtomicIsize::new(0));

/// Count `bytes` as allocated but not to be evicted for (see
/// [`NOT_COUNTED`]); [`counted_again`] takes them back.
#[inline]
pub fn not_counted_add(bytes: usize) {
    NOT_COUNTED.0.fetch_add(bytes as isize, Ordering::Relaxed);
}

/// Take back `bytes` [`not_counted_add`] counted.
#[inline]
pub fn counted_again(bytes: usize) {
    NOT_COUNTED.0.fetch_sub(bytes as isize, Ordering::Relaxed);
}

/// What is queued for the replicas: the replication hub says, after each
/// thing it does (`replication::start_hub`).
static REPLICA_QUEUED: AtomicUsize = AtomicUsize::new(0);

/// The replication hub's word on what is queued for its replicas.
#[inline]
pub fn set_replica_queued(bytes: usize) {
    if REPLICA_QUEUED.load(Ordering::Relaxed) != bytes {
        REPLICA_QUEUED.store(bytes, Ordering::Relaxed);
    }
}

/// Bytes allocated that deleting keys would not give back (see
/// [`NOT_COUNTED`]), and what is queued for replicas:
/// `mem_not_counted_for_evict` in `INFO`.
pub fn not_counted() -> usize {
    (NOT_COUNTED.0.load(Ordering::Relaxed).max(0) as usize)
        .saturating_add(REPLICA_QUEUED.load(Ordering::Relaxed))
}

/// What `maxmemory` is measured against: what is allocated, less what
/// deleting keys would not give back.
pub fn counted() -> usize {
    allocated().saturating_sub(not_counted())
}

/// Bytes a snapshot laid out, not counted against `maxmemory` while this
/// lives (see [`NOT_COUNTED`]).
#[derive(Default)]
pub struct NotCounted(usize);

impl NotCounted {
    pub fn new(bytes: usize) -> Self {
        not_counted_add(bytes);
        NotCounted(bytes)
    }

    /// `bytes` more.
    pub fn add(&mut self, bytes: usize) {
        not_counted_add(bytes);
        self.0 += bytes;
    }
}

impl Drop for NotCounted {
    fn drop(&mut self) {
        counted_again(self.0);
    }
}

static RECENT: AtomicUsize = AtomicUsize::new(0);

/// Whether memory was near `maxmemory` the last time a command looked
/// ([`over`]).
static NEAR: AtomicBool = AtomicBool::new(false);

/// Whether memory is near `maxmemory`: within an eighth of it, or 4 MB.
///
/// The slabs of values read it to decide how much room a slab must have
/// before it is used again (`value_slab::listable`). A server at its limit
/// frees a block for every block it takes (it evicts to make room), and
/// what a slab has free is memory the process holds and cannot use until
/// the slab is taken again.
#[inline]
pub fn near_limit() -> bool {
    NEAR.load(Ordering::Relaxed)
}

/// `maxmemory` was changed: memory is not near it until a command finds
/// that it is.
pub fn limit_changed() {
    NEAR.store(false, Ordering::Relaxed);
}

thread_local! {
    static CALLS: Cell<u32> = const { Cell::new(0) };
}

/// Whether more than `limit` bytes are allocated, less what deleting keys
/// would not give back ([`counted`]): the check every command makes against
/// `maxmemory`, which has to be cheap.
///
/// The sum over the threads' counters is taken on every 32nd call from a
/// thread and remembered in between — unless what was remembered is near
/// the limit, and then it is taken every time: far below the limit a figure
/// a few commands old is as good as a new one, and near it the command
/// after the one that went over is the one that has to see that it did.
///
/// Whether it is near is kept for the slabs: see [`near_limit`].
pub fn over(limit: usize) -> bool {
    let calls = CALLS.with(|calls| {
        let n = calls.get();
        calls.set(n.wrapping_add(1));
        n
    });
    let recent = RECENT.load(Ordering::Relaxed);
    let near = recent.saturating_add((limit / 8).max(4 << 20)) >= limit;
    if near != NEAR.load(Ordering::Relaxed) {
        NEAR.store(near, Ordering::Relaxed);
    }
    let now = if near || calls.is_multiple_of(32) {
        let now = counted();
        RECENT.store(now, Ordering::Relaxed);
        now
    } else {
        recent
    };
    now > limit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_is_allocated_is_counted_and_what_is_freed_is_not() {
        // Other tests allocate at the same time: look for a change far
        // larger than anything they do.
        const SIZE: usize = 256 << 20;
        let before = allocated();
        let block = vec![0u8; SIZE];
        let during = allocated();
        assert!(
            during >= before + SIZE / 2,
            "{before} before, {during} with 256 MB more"
        );
        // Freed on another thread: the count is the process's, not a thread's.
        std::thread::spawn(move || drop(block)).join().unwrap();
        let after = allocated();
        assert!(
            after + SIZE / 2 <= during,
            "{during} with the block, {after} without"
        );
    }

    /// Many threads allocating and freeing each other's blocks at once come
    /// out even: each adds to a counter of its own, and the sum is what
    /// counts.
    #[test]
    fn threads_that_free_each_others_blocks_come_out_even() {
        const BLOCK: usize = 1 << 20;
        let before = allocated();
        let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let makers: Vec<_> = (0..8)
            .map(|_| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        tx.send(vec![1u8; BLOCK]).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        // Freed here, by a thread that allocated none of them.
        let freed = rx.iter().map(drop).count();
        for maker in makers {
            maker.join().unwrap();
        }
        assert_eq!(freed, 1600);
        let after = allocated();
        // Other tests run at the same time; 1.6 GB went through.
        assert!(
            after.abs_diff(before) < 256 << 20,
            "{before} before, {after} after"
        );
    }
}
