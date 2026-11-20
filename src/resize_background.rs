//! The background's part of a large set's or hash's resize (#764).
//!
//! A [`CollectionTable`](crate::collection_table::CollectionTable) grows
//! and shrinks a few elements a write (`WRITE_STEP`), so a set or a hash
//! whose writes stop part-way through a resize would hold both arrays until
//! its next write, maybe for ever. A write that leaves its collection
//! mid-resize, or with a finished resize's old array kept back
//! (`defer_free`: freeing a big one took about a millisecond), hands the
//! collection to the background ([`hand`], through `member_set::after_write`
//! and `hash_fields::after_write`); the active-expire thread finishes the
//! resize in bounded steps, each under the collection's own write lock
//! ([`step_background`]), and gives the old arrays to `lazyfree`.
//!
//! The background holds the collections weakly: a key deleted, overwritten,
//! expired, evicted, flushed or moved meanwhile is not kept alive by it, and
//! it never waits for a collection's lock (`try_write`), so a reader that
//! holds one for long delays only that one's resize, and nothing waits on
//! the background. A collection it finds itself the last holder of (its key
//! gone while it held it) goes to `lazyfree` whole. The registry's lock is
//! never held while a collection's lock is taken.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::collection_table::Retired;

/// A key's value whose table the background can work on: a set's or a
/// hash's, under the value's own lock, never waited for.
pub trait Resizable: Send + Sync + 'static {
    /// One step of about `budget` units of a resize in progress, under the
    /// write lock taken without waiting: `None` if the lock is held. Else
    /// whether nothing is left for the background, and the array a finished
    /// resize left, to be freed elsewhere.
    fn try_step(&self, budget: usize) -> Option<(bool, Option<Retired>)>;
    /// Finish a resize in progress now and free what it leaves, under the
    /// write lock taken without waiting: `None` if it is held, else whether
    /// any memory was given back.
    fn try_settle(&self) -> Option<bool>;
    /// Let go of the value: if this was its last holder (`Arc::into_inner`,
    /// which tells that atomically), it goes to `lazyfree` whole, never
    /// freed on the calling thread, and the result is true.
    fn let_go(self: Arc<Self>) -> bool;
}

/// Collections handed to the background, not yet done with.
static HANDED: Mutex<Vec<Weak<dyn Resizable>>> = Mutex::new(Vec::new());
/// How many collections [`HANDED`] holds, read without its lock.
static HANDED_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Units of a resize's moves done under one hold of a collection's lock (a
/// unit is a group visited or an element moved). Finishing the growth of a
/// 3.2M-member set, 4,096 of them held the lock for up to 0.49 ms, 1,024 for
/// up to 0.27 ms (a move into pages of the new array touched for the first
/// time costs a page fault), so a writer waits no longer than that.
pub const BACKGROUND_STEP: usize = 1024;

/// Hand a collection to the background. Called with the collection's write
/// lock held: the registry's lock is taken after it, never the other way.
#[cold]
pub fn hand(value: Weak<dyn Resizable>) {
    let mut handed = HANDED.lock();
    handed.push(value);
    HANDED_COUNT.store(handed.len(), Ordering::Release);
}

/// Whether the arrays a resize holds (`transient` bytes) could take memory
/// past `maxmemory`: a limit is set and memory is within that many bytes of
/// it. Read only when a table holds such arrays.
pub fn tight(transient: usize) -> bool {
    let limit = crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed);
    limit != 0 && crate::memory_use::counted().saturating_add(transient) >= limit
}

/// The process is over `maxmemory` (a command's check): finish every resize
/// handed to the background and free what they leave, now, so that only
/// what the collections settle at is counted when the check is made again.
/// A collection whose lock is held is left (it is never waited for).
/// Returns whether anything was given back.
pub fn release_transients() -> bool {
    let values = std::mem::take(&mut *HANDED.lock());
    let mut kept = Vec::with_capacity(values.len());
    let mut released = false;
    for weak in values {
        let Some(value) = weak.upgrade() else {
            continue;
        };
        match value.try_settle() {
            Some(gave_back) => released |= gave_back,
            None => kept.push(weak),
        }
        let _ = value.let_go();
    }
    let mut handed = HANDED.lock();
    kept.append(&mut handed);
    *handed = kept;
    HANDED_COUNT.store(handed.len(), Ordering::Release);
    released
}

/// Whether any collection waits for the background.
#[inline]
pub fn background_pending() -> bool {
    HANDED_COUNT.load(Ordering::Acquire) > 0
}

/// What one call of [`step_background`] did.
#[derive(Debug, Default, Clone, Copy)]
pub struct BackgroundWork {
    /// Steps taken, each under one hold of a collection's lock.
    pub steps: usize,
    /// The longest a collection's lock was held for one of them.
    pub longest_hold: std::time::Duration,
    /// Collections finished with.
    pub finished: usize,
}

/// Work on the collections handed to the background for about `budget` of
/// time, a [`BACKGROUND_STEP`] at a time, each under the collection's write
/// lock taken without waiting. Those left over are kept for the next call.
pub fn step_background(budget: std::time::Duration) -> BackgroundWork {
    let started = std::time::Instant::now();
    let mut work = BackgroundWork::default();
    let values = std::mem::take(&mut *HANDED.lock());
    let mut kept = Vec::with_capacity(values.len());
    let mut values = values.into_iter();
    for weak in values.by_ref() {
        if started.elapsed() >= budget {
            kept.push(weak);
            break;
        }
        // Gone (deleted, overwritten, flushed...): nothing to do.
        let Some(value) = weak.upgrade() else {
            continue;
        };
        let mut done = false;
        while started.elapsed() < budget {
            let hold = std::time::Instant::now();
            // A reader holds it (a long `SMEMBERS`, say): next time.
            let Some((finished, retired)) = value.try_step(BACKGROUND_STEP) else {
                break;
            };
            work.longest_hold = work.longest_hold.max(hold.elapsed());
            work.steps += 1;
            if let Some(retired) = retired {
                // Near the limit, given back now: `lazyfree` frees it a
                // moment later, and the next command's check would count it.
                if tight(retired.bytes()) {
                    drop(retired);
                } else {
                    crate::lazyfree::free_later(retired, 0);
                }
            }
            if finished {
                done = true;
                break;
            }
        }
        // Let go of it without ever freeing it here. If its key went while
        // this held it (deleted, flushed, `UNLINK`ed...), this is the last
        // holder, and it goes to `lazyfree` whole: `Arc::into_inner` tells
        // that atomically, where a count read first could see another
        // holder that lets go just after, leaving the free to this thread.
        if value.let_go() {
            continue;
        }
        if done {
            work.finished += 1;
        } else {
            kept.push(weak);
        }
    }
    kept.extend(values);
    let mut handed = HANDED.lock();
    // Collections handed over while these were worked on come after them.
    kept.append(&mut handed);
    *handed = kept;
    HANDED_COUNT.store(handed.len(), Ordering::Release);
    work
}

/// Whether the value at `at` (its `Arc`'s pointer) is handed to the
/// background (tests).
#[cfg(test)]
pub(crate) fn is_handed(at: *const ()) -> bool {
    HANDED
        .lock()
        .iter()
        .any(|w| std::ptr::addr_eq(w.as_ptr(), at))
}
