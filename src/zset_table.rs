//! The large form of a sorted set (#875, the storage half; #825): its
//! members and scores in a [`CollectionTable`] of [`Scores`], one pool block
//! a member (`[score][len][member]`), and their order in an index behind
//! [`ScoreOrder`], which holds each member as an [`EntryRef`]: the address
//! of its block.
//!
//! A member is looked up by name in the table (one hash, one probe, as a
//! large set's), and its score is read from and written to its block in
//! place (eight bytes, no reallocation). The index keeps `(score, entry)`
//! in the set's order, by score and then by the member's bytes (byte by
//! byte, then the shorter first, as Redis compares them), read through the
//! entry. The index is [`CountedOrder`], a B+tree counted for rank
//! (`counted_btree`): a rank, and a range by rank, are one walk down.
//! [`BTreeOrder`], the `BTreeMap` the large form had, is the reference the
//! tests compare it with.
//!
//! # The one unsafe rule
//!
//! An [`EntryRef`] is a raw address, and the index reads a member's bytes
//! through it whenever it compares two members of equal score. That is
//! sound because of two facts and one rule.
//!
//! - **Entries do not move.** A resize of the table (a growth, a shrink, a
//!   resize with a new seed) moves the 8-byte slot words from one group
//!   array to another, never the blocks they point to
//!   (`collection_table::place`, `CollectionTable::step_resize`); an
//!   overflow group holds slot words too. Active defrag never moves a pool
//!   entry: `value_slab::plan_evacuation` only picks slabs of the keyspace's
//!   classes, and an entry is a block of a pool class or, past
//!   `value_slab::MAX_BLOCK`, of the system allocator. So an entry's address
//!   is fixed from the insert that makes it to the removal that frees it.
//! - **Only this module changes the table or the index.** [`Large`] owns
//!   both, privately; every write goes through its methods, and an
//!   [`EntryRef`] is made only here.
//! - **The rule.** The table frees an entry only once the index has handed
//!   back that very reference (a removal, a pop, a refused insert), and no
//!   reference is held across a call that can free or rewrite its entry (a
//!   score is rewritten in place only under `&mut self`, once the index has
//!   taken it, and the index keeps each member's score itself, so it never
//!   reads a score through a reference). An index that does not find a
//!   member, or refuses a change, leaves its entry as it is. Dropping the set
//!   drops the index first: it is the first field of [`Large`].
//!
//! Debug builds check after every write that the index and the table hold
//! as many members; the model test (`tests` below) checks everything against
//! a `BTreeMap` and a `HashMap` through random operations, and runs under
//! Miri (`cargo +nightly miri test --lib zset_table`), entry pool included.

// `BTreeOrder` is the tests' reference, and some of the index's interface
// is for the checks: the binary does not call all of it.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::ops::Bound;
use std::ptr::NonNull;

use ordered_float::OrderedFloat;

use crate::collection_table::{
    CollectionTable, EntryPool, Memory, Retired, Scores, TooLong, detached_scored_entry,
    scored_member, scored_score,
};
use crate::counted_btree::{self, CountedTree};

/// A member's entry in the table, as the index holds it: the address of its
/// block. Not `Clone`: the index holds one for each member, and only this
/// module makes others (to say which member a call is about).
#[repr(transparent)]
pub struct EntryRef(NonNull<u8>);

// SAFETY: an address into a table its `Large` owns, read through only under
// a borrow of that `Large` (`&self` to read, `&mut self` to write), as the
// table's own slots are; the table is `Send` and `Sync`.
unsafe impl Send for EntryRef {}
// SAFETY: as above.
unsafe impl Sync for EntryRef {}

impl EntryRef {
    #[inline]
    fn new(entry: NonNull<u8>) -> Self {
        EntryRef(entry)
    }

    /// The member's bytes.
    #[inline]
    pub fn member(&self) -> &[u8] {
        // SAFETY: the module's rule: a reference exists only while its entry
        // is in the table, and nothing frees or rewrites the entry while
        // this borrow of the reference lasts.
        unsafe { scored_member(self.0.as_ptr()) }
    }

    /// The entry's address: the same for the same member as long as it is
    /// in the set.
    #[inline]
    pub fn addr(&self) -> usize {
        self.0.as_ptr().addr()
    }

    /// The score in the entry. The index never needs it (it keeps its own
    /// copy); for the checks, and a member the index hands back wrongly.
    #[inline]
    fn score_in_entry(&self) -> f64 {
        // SAFETY: as in `member`.
        unsafe { scored_score(self.0.as_ptr()) }
    }

    /// Another reference to the same entry, to say which member a call is
    /// about: dropped before the call returns.
    #[inline]
    fn alias(&self) -> EntryRef {
        EntryRef(self.0)
    }
}

impl PartialEq for EntryRef {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for EntryRef {}

impl std::fmt::Debug for EntryRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EntryRef({:#x})", self.addr())
    }
}

/// A `(score, member)` to compare with: a bound of a range, or the key of a
/// member being looked for.
#[derive(Clone, Copy, Debug)]
pub struct Probe<'a> {
    pub score: f64,
    pub member: &'a [u8],
}

/// The set's order: by score (as `OrderedFloat` orders them: -0 is 0, and a
/// set holds neither -0 nor NaN), then by the member's bytes, byte by byte
/// and then the shorter first, as Redis compares them.
#[inline]
pub fn order(a: f64, a_member: &[u8], b: f64, b_member: &[u8]) -> Ordering {
    OrderedFloat(a)
        .cmp(&OrderedFloat(b))
        .then_with(|| a_member.cmp(b_member))
}

/// The order of a large sorted set's members, by `(score, member)`, each
/// member held as the [`EntryRef`] of its entry. What `Large` asks of it:
///
/// | | |
/// |---|---|
/// | `insert`, `remove`, `rescore` | add a member at its score, take one out (by its score and entry), move one to a new score |
/// | `rank`, `nth` | the rank of a member it holds; the member at a rank |
/// | `range_by_rank` | ranks `start..end`, both ways |
/// | `range` | the members between two `(score, member)` bounds: a range of scores, or of members within one score (`ZRANGEBYLEX` of a set whose scores are equal) |
/// | `first`, `last`, `pop_first`, `pop_last` | the ends |
/// | `iter`, `len` | every member, both ways; how many |
/// | `from_sorted` | the whole index at once from members in order (a load, a block made large) |
/// | `heap_bytes` | what it allocates, for `MEMORY USAGE` |
///
/// The order compares `(score, member)` with [`order`], the member's bytes
/// read through its entry ([`EntryRef::member`]). A rank is 0-based from the
/// lowest. Scores are never NaN, and never -0 (it is kept as 0).
///
/// # Safety
/// `Large` frees an entry only once the index has handed back its very
/// reference (by a removal, a pop or a refused insert), and relies on this:
///
/// - An implementation holds each [`EntryRef`] from the `insert` that takes
///   it, or the `from_sorted` that gives it, to the `remove`, `pop_first`,
///   `pop_last` or refused `insert` that hands it back, or to its own drop,
///   and no longer.
/// - What it hands back it no longer holds: the reference given, or one it
///   held. It keeps no reference elsewhere; one made for a lookup is gone
///   when the call returns.
/// - It reads a member's bytes only through references it holds, or that a
///   call gives it for that call.
/// - It keeps each member's score itself, and never reads one through a
///   reference: an entry takes a new score after the index has it.
///
/// A removal that finds nothing, and a refused insert or rescore, are
/// sound: `Large` then frees nothing and writes no score. Finding what it is
/// asked for is the implementation's correctness, not `Large`'s safety.
pub unsafe trait ScoreOrder: Default + Send + Sync {
    /// Members and their scores in order, both ways, over a range of ranks
    /// (the whole index, too); borrowed from the index.
    type Iter<'a>: DoubleEndedIterator<Item = (f64, &'a EntryRef)>
    where
        Self: 'a;

    /// The same over a range of `(score, member)` keys.
    type Range<'a>: DoubleEndedIterator<Item = (f64, &'a EntryRef)>
    where
        Self: 'a;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add `entry` at `score`. If an equal key is held: unchanged, `Err`
    /// hands it back.
    fn insert(&mut self, score: f64, entry: EntryRef) -> Result<(), EntryRef>;

    /// Take the member `entry` (at `score`) out: its reference, `None` if it
    /// is not held.
    fn remove(&mut self, score: f64, entry: &EntryRef) -> Option<EntryRef>;

    /// Move `entry` from `old` to `new`. False, nothing changed, if not at
    /// `old` or `new` is taken.
    fn rescore(&mut self, old: f64, new: f64, entry: &EntryRef) -> bool;

    /// The rank of the member `entry`, at `score`, which it holds.
    fn rank(&self, score: f64, entry: &EntryRef) -> usize;

    /// The member at `rank`.
    fn nth(&self, rank: usize) -> Option<(f64, &EntryRef)>;

    fn first(&self) -> Option<(f64, &EntryRef)>;
    fn last(&self) -> Option<(f64, &EntryRef)>;
    fn pop_first(&mut self) -> Option<(f64, EntryRef)>;
    fn pop_last(&mut self) -> Option<(f64, EntryRef)>;

    /// Every member, in order.
    fn iter(&self) -> Self::Iter<'_>;

    /// The members of ranks `start..end` (`start <= end <= len`).
    fn range_by_rank(&self, start: usize, end: usize) -> Self::Iter<'_>;

    /// The members from `from` to `to`, each a `(score, member)` bound: a
    /// range of scores (from `(lo, "")` included to `(hi', "")` excluded,
    /// `hi'` the least score past it), or of members within one score.
    fn range(&self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> Self::Range<'_>;

    /// The index of `sorted`: each a different member, in strictly
    /// increasing order.
    fn from_sorted(sorted: Vec<(f64, EntryRef)>) -> Self;

    /// The bytes it holds on the heap.
    fn heap_bytes(&self) -> usize;
}

// ---------------------------------------------------------------------------
// The reference index: the B-tree the large form had
// ---------------------------------------------------------------------------

/// A key of [`BTreeOrder`]: the member's score, and its entry (16 bytes).
struct Key {
    score: f64,
    entry: EntryRef,
}

impl Ord for Key {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score)
            .cmp(&OrderedFloat(other.score))
            .then_with(|| {
                if self.entry == other.entry {
                    // One member: its bytes are its bytes.
                    Ordering::Equal
                } else {
                    self.entry.member().cmp(other.entry.member())
                }
            })
    }
}

impl PartialOrd for Key {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Key {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Key {}

/// [`ScoreOrder`] as a `BTreeMap` of `(score, entry)`: what the large form
/// has had (a tree of `(score, member)`), with an 8-byte reference in place
/// of the member's 16-byte `Arc`. Rank is a walk to the member (as before);
/// a range by rank walks to its start from the nearer end. The tests check
/// [`CountedOrder`] against it.
#[derive(Default)]
pub struct BTreeOrder {
    map: BTreeMap<Key, ()>,
}

impl BTreeOrder {
    /// A key to find `entry` at `score` by: a second reference, dropped
    /// when the call that uses it returns.
    #[inline]
    fn key(score: f64, entry: &EntryRef) -> Key {
        Key {
            score,
            entry: entry.alias(),
        }
    }
}

/// An entry of the empty member outside any table: what the bounds of a
/// range of scores, `(score, "")`, point to. Only its member is ever read
/// (a key keeps its own score): its length byte, 0, at offset 8.
static EMPTY_MEMBER: [u64; 2] = [0, 0];

/// A key that stands for a bound: the probe's score, and an entry of its
/// own holding the probe's member as the table lays one out
/// (`collection_table::detached_scored_entry`, kept in `held`; the empty
/// member is [`EMPTY_MEMBER`]), so that the tree compares a bound with its
/// keys as it compares two of them, with no call through a trait object.
/// Its address is no table entry's, so it is never taken for one of them.
#[inline]
fn probe_key(probe: &Probe<'_>, held: &mut Option<Box<[u64]>>) -> Key {
    let entry = if probe.member.is_empty() {
        NonNull::from(&EMPTY_MEMBER).cast::<u8>()
    } else {
        let buf = held.insert(detached_scored_entry(probe.member));
        // SAFETY: a boxed slice's pointer is never null.
        unsafe { NonNull::new_unchecked(buf.as_mut_ptr().cast::<u8>()) }
    };
    Key {
        score: probe.score,
        entry: EntryRef(entry),
    }
}

// The two iterators are as lean as walking the tree directly was before
// the move to the table (a range of `ZRANGE`, the cells of `GEOSEARCH`): one
// enum of both, or a step that was not inlined into the command, cost up to
// 76% more instructions on a walk from a random rank, and 10% on GEOSEARCH.

/// [`BTreeOrder`]'s iterator of ranks: the tree walked lazily from either
/// end, as `keys()` walks it, the members outside the range of ranks stepped
/// past on the first step from each end.
pub struct BTreeIter<'a> {
    keys: btree_map::Keys<'a, Key, ()>,
    skip_front: usize,
    skip_back: usize,
    left: usize,
}

impl<'a> Iterator for BTreeIter<'a> {
    type Item = (f64, &'a EntryRef);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        if self.skip_front > 0 {
            for _ in 0..std::mem::take(&mut self.skip_front) {
                self.keys.next()?;
            }
        }
        let key = self.keys.next()?;
        Some((key.score, &key.entry))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl DoubleEndedIterator for BTreeIter<'_> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        if self.skip_back > 0 {
            for _ in 0..std::mem::take(&mut self.skip_back) {
                self.keys.next_back()?;
            }
        }
        let key = self.keys.next_back()?;
        Some((key.score, &key.entry))
    }
}

/// [`BTreeOrder`]'s iterator of a range of keys.
pub struct BTreeRange<'a> {
    range: btree_map::Range<'a, Key, ()>,
}

impl<'a> Iterator for BTreeRange<'a> {
    type Item = (f64, &'a EntryRef);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|(key, ())| (key.score, &key.entry))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl DoubleEndedIterator for BTreeRange<'_> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .next_back()
            .map(|(key, ())| (key.score, &key.entry))
    }
}

impl BTreeOrder {
    #[inline]
    fn range_between(&self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> BTreeRange<'_> {
        let (mut held_from, mut held_to) = (None, None);
        let lo = from.as_ref().map(|p| probe_key(p, &mut held_from));
        let hi = to.as_ref().map(|p| probe_key(p, &mut held_to));
        // The range keeps no bound: the probes' entries can go when this
        // returns.
        BTreeRange {
            range: self.map.range::<Key, _>((lo.as_ref(), hi.as_ref())),
        }
    }
}

// SAFETY: the map holds each reference from `insert`/`from_sorted` to the
// `remove`/`pop_*` that hands it back (`remove_entry`, `pop_first`,
// `pop_last` move the key out), and drops the rest with itself. An insert of
// an equal key keeps the map's key and drops the one given, so the given
// reference, not held, goes back by its address. The references `key` makes
// for a lookup, and the probe keys of a range (which point to entries of
// their own, never a table's), are dropped when the call returns. Members'
// bytes are read only through keys in the map or the call's own. Scores are
// the keys' own.
unsafe impl ScoreOrder for BTreeOrder {
    type Iter<'a> = BTreeIter<'a>;
    type Range<'a> = BTreeRange<'a>;

    #[inline]
    fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    fn insert(&mut self, score: f64, entry: EntryRef) -> Result<(), EntryRef> {
        let at = entry.0;
        match self.map.insert(Key { score, entry }, ()) {
            None => Ok(()),
            // The map kept its own key and dropped this one.
            Some(()) => Err(EntryRef(at)),
        }
    }

    #[inline]
    fn remove(&mut self, score: f64, entry: &EntryRef) -> Option<EntryRef> {
        self.map
            .remove_entry(&Self::key(score, entry))
            .map(|(key, ())| key.entry)
    }

    #[inline]
    fn rescore(&mut self, old: f64, new: f64, entry: &EntryRef) -> bool {
        let Some(held) = self.remove(old, entry) else {
            return false;
        };
        let Err(held) = self.insert(new, held) else {
            return true;
        };
        // An equal key at `new`: back to its own, just freed.
        if self.insert(old, held).is_err() {
            disagree("a member's own key was taken: the index lost it");
        }
        false
    }

    fn rank(&self, score: f64, entry: &EntryRef) -> usize {
        self.map.range(..Self::key(score, entry)).count()
    }

    fn nth(&self, rank: usize) -> Option<(f64, &EntryRef)> {
        self.map.keys().nth(rank).map(|key| (key.score, &key.entry))
    }

    #[inline]
    fn first(&self) -> Option<(f64, &EntryRef)> {
        self.map
            .first_key_value()
            .map(|(key, ())| (key.score, &key.entry))
    }

    #[inline]
    fn last(&self) -> Option<(f64, &EntryRef)> {
        self.map
            .last_key_value()
            .map(|(key, ())| (key.score, &key.entry))
    }

    #[inline]
    fn pop_first(&mut self) -> Option<(f64, EntryRef)> {
        self.map.pop_first().map(|(key, ())| (key.score, key.entry))
    }

    #[inline]
    fn pop_last(&mut self) -> Option<(f64, EntryRef)> {
        self.map.pop_last().map(|(key, ())| (key.score, key.entry))
    }

    #[inline]
    fn iter(&self) -> BTreeIter<'_> {
        self.range_by_rank(0, self.map.len())
    }

    #[inline]
    fn range_by_rank(&self, start: usize, end: usize) -> BTreeIter<'_> {
        let len = self.map.len();
        let end = end.min(len);
        let start = start.min(end);
        BTreeIter {
            keys: self.map.keys(),
            skip_front: start,
            skip_back: len - end,
            left: end - start,
        }
    }

    #[inline]
    fn range(&self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> BTreeRange<'_> {
        self.range_between(from, to)
    }

    fn from_sorted(sorted: Vec<(f64, EntryRef)>) -> Self {
        debug_assert!(sorted.windows(2).all(|w| {
            order(w[0].0, w[0].1.member(), w[1].0, w[1].1.member()) == Ordering::Less
        }));
        // In order: the tree is built whole, its nodes full, with no
        // comparison but the check that they are in order.
        let map = sorted
            .into_iter()
            .map(|(score, entry)| (Key { score, entry }, ()))
            .collect();
        BTreeOrder { map }
    }

    #[inline]
    fn heap_bytes(&self) -> usize {
        crate::commands::memory_layout::score_index::<Key>(self.map.len())
    }
}

// ---------------------------------------------------------------------------
// The index: a counted tree
// ---------------------------------------------------------------------------

impl counted_btree::Member for EntryRef {
    #[inline]
    fn member(&self) -> &[u8] {
        EntryRef::member(self)
    }
}

/// [`ScoreOrder`] as a counted B+tree of `(score, entry)`: a rank, and a
/// range by rank, are one walk down.
#[derive(Default)]
pub struct CountedOrder(CountedTree<EntryRef>);

// SAFETY: what `ScoreOrder` asks, and how the tree keeps to it.
//
// - Held from the `insert` that takes it, or `from_sorted`, to the removal,
//   pop or refused insert that hands it back, or to the tree's drop. The
//   tree is safe Rust, generic over a reference it can neither clone nor
//   make: `EntryRef` is not `Clone`, and only this module makes one. So it
//   moves, hands back or drops each reference, and never copies one.
// - What it hands back it no longer holds: a refused insert returns the
//   reference untouched; a removal or pop takes it out of its leaf, and a
//   removal's is the very reference asked for (compared by address).
// - A build refuses only a member given twice, then dropped: never held.
// - Members' bytes are read only through references in its leaves and the
//   one a call names; a bound's are bytes the call lends.
// - Scores are its own: `counted_btree::Member` reads no score.
// - `Send`, `Sync`: no interior mutability; read under `&self`, written under
//   `&mut self`, as `Large` is.
unsafe impl ScoreOrder for CountedOrder {
    type Iter<'a> = counted_btree::Iter<'a, EntryRef>;
    type Range<'a> = counted_btree::Iter<'a, EntryRef>;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn insert(&mut self, score: f64, entry: EntryRef) -> Result<(), EntryRef> {
        self.0.insert(score, entry)
    }

    fn remove(&mut self, score: f64, entry: &EntryRef) -> Option<EntryRef> {
        self.0.remove(score, entry)
    }

    fn rescore(&mut self, old: f64, new: f64, entry: &EntryRef) -> bool {
        self.0.rescore(old, new, entry).is_some()
    }

    fn rank(&self, score: f64, entry: &EntryRef) -> usize {
        // Not held: where it would go, as `BTreeOrder` counts it.
        let (Ok(rank) | Err(rank)) = self.0.rank(score, entry.member());
        rank
    }

    fn nth(&self, rank: usize) -> Option<(f64, &EntryRef)> {
        self.0.nth(rank)
    }

    fn first(&self) -> Option<(f64, &EntryRef)> {
        self.0.nth(0)
    }

    fn last(&self) -> Option<(f64, &EntryRef)> {
        self.0.nth(self.0.len().checked_sub(1)?)
    }

    fn pop_first(&mut self) -> Option<(f64, EntryRef)> {
        self.0.remove_at(0)
    }

    fn pop_last(&mut self) -> Option<(f64, EntryRef)> {
        self.0.remove_at(self.0.len().checked_sub(1)?)
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.0.iter()
    }

    fn range_by_rank(&self, start: usize, end: usize) -> Self::Iter<'_> {
        self.0.range_by_rank(start, end)
    }

    fn range(&self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> Self::Range<'_> {
        let from = from.map(|probe| (probe.score, probe.member));
        let to = to.map(|probe| (probe.score, probe.member));
        let (start, end) = self.0.rank_range(from, to);
        self.0.range_by_rank(start, end)
    }

    fn from_sorted(sorted: Vec<(f64, EntryRef)>) -> Self {
        match CountedTree::from_sorted(sorted) {
            Ok(tree) => CountedOrder(tree),
            // Out of order: one at a time, which takes any order.
            Err(sorted) => {
                let mut index = CountedOrder::default();
                for (score, entry) in sorted {
                    if index.insert(score, entry).is_err() {
                        disagree("a build was given a member twice: one is out of the index");
                    }
                }
                index
            }
        }
    }

    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

// ---------------------------------------------------------------------------
// The large form
// ---------------------------------------------------------------------------

/// The index broke its contract, a bug: called once the set is left safe.
/// Debug builds stop; release builds log it and go on.
#[cold]
fn disagree(what: &str) {
    #[cfg(test)]
    DISAGREED.with(|n| n.set(n.get() + 1));
    debug_assert!(false, "sorted set index: {what}");
    crate::log_throttled!(10, crate::loglevel::ERROR, "Sorted set index: {what}");
}

#[cfg(test)]
thread_local! {
    /// How often this thread's sets found their index broken.
    static DISAGREED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A large sorted set: the table of members and scores, and the index of
/// their order. See the module's notes for the rule that keeps the index's
/// references sound.
pub struct Large<O: ScoreOrder = CountedOrder, A: Memory = EntryPool> {
    /// First, so dropped first: it points into the table's entries.
    index: O,
    table: CollectionTable<Scores, A>,
    /// Handed to the background (`sorted_set::after_write`).
    pub(crate) queued: bool,
}

impl<O: ScoreOrder, A: Memory> Large<O, A> {
    /// Room for `members` before the table grows.
    pub fn with_capacity(members: usize) -> Self {
        Large {
            index: O::default(),
            table: Self::table_for(members),
            queued: false,
        }
    }

    fn table_for(members: usize) -> CollectionTable<Scores, A> {
        let mut table = CollectionTable::with_capacity(members);
        // A finished resize's old array is freed by the background, not by
        // the write that finished it.
        table.defer_free(true);
        table
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Members the table takes before it grows.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.table.capacity()
    }

    /// The table, for `ZSCAN`'s cursor and the measurements: read only.
    #[inline]
    pub fn table(&self) -> &CollectionTable<Scores, A> {
        &self.table
    }

    /// Debug builds: the index holds a reference for every entry, and no
    /// more.
    #[inline]
    fn debug_check(&self) {
        debug_assert_eq!(self.index.len(), self.table.len(), "index and table");
    }

    /// The member's score.
    #[inline]
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        let entry = self.table.entry(member)?;
        // SAFETY: a live entry of the table, read under `&self`.
        Some(unsafe { scored_score(entry.as_ptr()) })
    }

    /// The member's 0-based rank, and its score.
    pub fn rank(&self, member: &[u8]) -> Option<(usize, f64)> {
        let entry = EntryRef::new(self.table.entry(member)?);
        let score = entry.score_in_entry();
        Some((self.index.rank(score, &entry), score))
    }

    /// Insert or update a member; true if it is new.
    pub fn insert(&mut self, member: &[u8], score: f64) -> bool {
        self.upsert(member, |old| (Some(score), old.is_none()))
    }

    /// Look `member` up and give it a score, in one lookup: `decide` is
    /// given its score if it is a member, and says what its score is to be
    /// (`None`: as it is, or not added) and what to return. A score of -0
    /// is kept as 0.
    #[inline]
    pub fn upsert<R>(
        &mut self,
        member: &[u8],
        decide: impl FnOnce(Option<f64>) -> (Option<f64>, R),
    ) -> R {
        match self.table.lookup(member) {
            Ok(entry) => {
                // SAFETY: a live entry of the table.
                let old = unsafe { scored_score(entry.as_ptr()) };
                let (score, out) = decide(Some(old));
                if let Some(score) = score {
                    let score = score + 0.0;
                    // The index first: if it refuses, neither changes.
                    if score.to_bits() != old.to_bits() {
                        if self.index.rescore(old, score, &EntryRef::new(entry)) {
                            // SAFETY: a live entry of the table; nothing
                            // borrows it (`&mut self`).
                            unsafe { self.table.set_score(entry, score) };
                        } else {
                            disagree("the index refused a score: the member keeps its own");
                        }
                    }
                }
                out
            }
            Err(at) => {
                let (score, out) = decide(None);
                if let Some(score) = score {
                    let score = score + 0.0;
                    // A member longer than the table holds (4 GiB) is never
                    // given by a command (512 MB at most) and is not added,
                    // as a set's is not; a load refuses it (`Builder::push`).
                    if let Ok(entry) = self.table.insert_vacant(at, member, score)
                        && self.index.insert(score, EntryRef::new(entry)).is_err()
                    {
                        // SAFETY: a live entry of the table, refused, so
                        // held by no index.
                        unsafe { self.table.remove_entry(entry) };
                        disagree("the index refused a new member: it is not added");
                    }
                }
                self.debug_check();
                out
            }
        }
    }

    /// Remove a member; true if it was one.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        let Some(entry) = self.table.entry(member) else {
            return false;
        };
        // SAFETY: a live entry of the table.
        let score = unsafe { scored_score(entry.as_ptr()) };
        let removed = self.remove_held(score, entry);
        self.debug_check();
        removed
    }

    /// Out of the index, then freed, only if the index hands this entry back.
    fn remove_held(&mut self, score: f64, entry: NonNull<u8>) -> bool {
        match self.index.remove(score, &EntryRef::new(entry)) {
            Some(EntryRef(held)) if held == entry => {
                // SAFETY: a live entry of the table; the index handed its
                // reference back, and no other is left.
                unsafe { self.table.remove_entry(entry) };
                true
            }
            Some(other) => {
                // Another member's, still in the table: back at its score.
                let lost = self.index.insert(other.score_in_entry(), other).is_err();
                disagree(if lost {
                    "the index handed back another member, then refused it: it is out"
                } else {
                    "the index handed back another member: the one removed is kept"
                });
                false
            }
            None => {
                disagree("the index does not hold a member removed: it is kept");
                false
            }
        }
    }

    /// Take out the member with the lowest score (`max`: the highest),
    /// handing it to `take` before it goes.
    pub fn pop_with<R>(&mut self, max: bool, take: impl FnOnce(&[u8], f64) -> R) -> Option<R> {
        let (out, at) = {
            let (score, entry) = if max {
                self.index.pop_last()
            } else {
                self.index.pop_first()
            }?;
            (take(entry.member(), score), entry.0)
        };
        // SAFETY: a live entry of the table; the index no longer holds it,
        // and the last reference to it is gone (with the block above).
        unsafe { self.table.remove_entry(at) };
        self.debug_check();
        Some(out)
    }

    /// Take out the members `doomed` lists, by score and entry: out of the
    /// index first, then out of the table; how many went.
    fn remove_listed(&mut self, doomed: Vec<(f64, NonNull<u8>)>) -> usize {
        // Each entry is freed only once the index hands it back.
        let mut removed = 0;
        for (score, at) in doomed {
            removed += usize::from(self.remove_held(score, at));
        }
        self.debug_check();
        removed
    }

    /// Take out the members of ranks `start..end`; how many went.
    pub fn remove_ranks(&mut self, start: usize, end: usize) -> usize {
        let doomed = self
            .index
            .range_by_rank(start, end)
            .map(|(score, entry)| (score, entry.0))
            .collect();
        self.remove_listed(doomed)
    }

    /// Take out the members from `from` to `to`; how many went.
    pub fn remove_range(&mut self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> usize {
        let doomed = self
            .index
            .range(from, to)
            .map(|(score, entry)| (score, entry.0))
            .collect();
        self.remove_listed(doomed)
    }

    /// Take out the members `gone` says to; how many went.
    pub fn remove_where(&mut self, mut gone: impl FnMut(&[u8], f64) -> bool) -> usize {
        let doomed = self
            .index
            .iter()
            .filter(|(score, entry)| gone(entry.member(), *score))
            .map(|(score, entry)| (score, entry.0))
            .collect();
        self.remove_listed(doomed)
    }

    /// Members and scores in order, both ways.
    #[inline]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&[u8], f64)> {
        self.index
            .iter()
            .map(|(score, entry)| (entry.member(), score))
    }

    /// Members of ranks `start..end`, both ways.
    #[inline]
    pub fn range_by_rank(
        &self,
        start: usize,
        end: usize,
    ) -> impl DoubleEndedIterator<Item = (&[u8], f64)> {
        self.index
            .range_by_rank(start, end)
            .map(|(score, entry)| (entry.member(), score))
    }

    /// Members from `from` to `to`, both ways: borrowed from the set, not
    /// from the bounds.
    #[inline]
    pub fn range<'s>(
        &'s self,
        from: Bound<Probe<'_>>,
        to: Bound<Probe<'_>>,
    ) -> impl DoubleEndedIterator<Item = (&'s [u8], f64)> + use<'s, O, A> {
        self.index
            .range(from, to)
            .map(|(score, entry)| (entry.member(), score))
    }

    /// Members and scores in no particular order: one walk of the table.
    #[inline]
    pub fn iter_unordered(&self) -> impl ExactSizeIterator<Item = (&[u8], f64)> {
        self.table.entries().map(|entry| {
            // SAFETY: a live entry of the table, read under `&self`.
            unsafe { (scored_member(entry.as_ptr()), scored_score(entry.as_ptr())) }
        })
    }

    /// A member picked uniformly at random, its score, and its entry's
    /// address (which tells two picks of one member apart).
    #[inline]
    pub fn random(&self, rng: &mut impl rand::Rng) -> Option<(&[u8], f64, usize)> {
        let entry = self.table.random_entry(rng)?;
        // SAFETY: a live entry of the table, read under `&self`.
        Some(unsafe {
            (
                scored_member(entry.as_ptr()),
                scored_score(entry.as_ptr()),
                entry.as_ptr().addr(),
            )
        })
    }

    /// A table reserved for more than it holds starts its shrink.
    #[inline]
    pub fn shrink_if_sparse(&mut self) {
        self.table.shrink_if_sparse();
    }

    /// Everything the set holds but its own box: the table
    /// ([`CollectionTable::allocation_size`]) and the index's nodes, both
    /// exactly ([`ScoreOrder::heap_bytes`]; `BTreeOrder` estimates its own).
    #[inline]
    pub fn allocation_size(&self) -> usize {
        self.table.allocation_size() + self.index.heap_bytes()
    }

    /// The table's bytes, exactly: what freeing the set gives back at
    /// least.
    #[inline]
    pub fn table_bytes(&self) -> usize {
        self.table.allocation_size()
    }

    // The background's part (`sorted_set::after_write`): the table's resize
    // alone, which moves slot words and no entry.

    #[inline]
    pub fn resizing(&self) -> bool {
        self.table.resizing()
    }

    #[inline]
    pub fn holds_retired(&self) -> bool {
        self.table.holds_retired()
    }

    #[inline]
    pub fn transient_bytes(&self) -> usize {
        self.table.transient_bytes()
    }

    #[inline]
    pub fn step_resize(&mut self, budget: usize) -> bool {
        self.table.step_resize(budget)
    }

    #[inline]
    pub fn take_retired(&mut self) -> Option<Retired<A>> {
        self.table.take_retired()
    }

    /// Every invariant, the slow way (tests): the index in strictly
    /// increasing order, each of its members the table's at the same
    /// address and with the same score, as many as the table holds.
    #[cfg(test)]
    pub(crate) fn check(&self) {
        assert_eq!(self.index.len(), self.table.len());
        let mut last: Option<(f64, &[u8])> = None;
        let mut n = 0;
        for (score, entry) in self.index.iter() {
            let member = entry.member();
            if let Some((s, m)) = last {
                assert_eq!(order(s, m, score, member), Ordering::Less, "out of order");
            }
            let found = self.table.entry(member).expect("a member of the table");
            assert_eq!(found.as_ptr().addr(), entry.addr(), "another entry");
            assert_eq!(entry.score_in_entry().to_bits(), score.to_bits());
            last = Some((score, member));
            n += 1;
        }
        assert_eq!(n, self.table.len());
        let back = self.index.iter().rev().count();
        assert_eq!(back, n);
    }

    /// The addresses of the entries the index holds, in its order (tests).
    #[cfg(test)]
    pub(crate) fn index_addrs(&self) -> Vec<usize> {
        self.index.iter().map(|(_, entry)| entry.addr()).collect()
    }
}

impl<O: ScoreOrder, A: Memory> Clone for Large<O, A> {
    /// A set of its own (`COPY`): every entry copied into a table of its
    /// own, and the index built anew over the copies, in one go.
    fn clone(&self) -> Self {
        let mut table = self.table.clone();
        table.defer_free(true);
        let sorted = self
            .index
            .iter()
            .map(|(score, entry)| {
                let copy = table.entry(entry.member()).expect("a copied member");
                (score, EntryRef::new(copy))
            })
            .collect();
        Large {
            index: O::from_sorted(sorted),
            table,
            queued: false,
        }
    }
}

/// A large set built whole from members given one at a time (a load,
/// `RESTORE`, a command's result, a block made large): the table filled as
/// they come, the index built at the end in one go. Members given in order
/// (a snapshot gives them so) are laid down as they are; otherwise, or if a
/// member is given twice (its last score kept), they are sorted at the end.
pub struct Builder<O: ScoreOrder = CountedOrder, A: Memory = EntryPool> {
    table: CollectionTable<Scores, A>,
    /// Each member's entry, as given, and its score then: the index's
    /// references until `finish` gives them to it.
    sorted: Vec<(f64, EntryRef)>,
    in_order: bool,
    _index: std::marker::PhantomData<O>,
}

impl<O: ScoreOrder, A: Memory> Builder<O, A> {
    /// Room for `members` before the table grows.
    pub fn with_capacity(members: usize) -> Self {
        Builder {
            table: Large::<O, A>::table_for(members),
            sorted: Vec::with_capacity(members),
            in_order: true,
            _index: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Add `member` at `score` (-0 kept as 0); one given before takes this
    /// score. A member the table cannot hold (over 4 GiB) is refused, with
    /// nothing changed: a load fails rather than drop it.
    pub fn push(&mut self, member: &[u8], score: f64) -> Result<(), TooLong> {
        let score = score + 0.0;
        match self.table.lookup(member) {
            Ok(entry) => {
                // SAFETY: a live entry of the table.
                unsafe { self.table.set_score(entry, score) };
                self.in_order = false;
            }
            Err(at) => {
                let entry = self.table.insert_vacant(at, member, score)?;
                if let Some((last, before)) = self.sorted.last()
                    && order(*last, before.member(), score, member) != Ordering::Less
                {
                    self.in_order = false;
                }
                self.sorted.push((score, EntryRef::new(entry)));
            }
        }
        Ok(())
    }

    /// The set.
    pub fn finish(self) -> Large<O, A> {
        let Builder {
            table,
            mut sorted,
            in_order,
            ..
        } = self;
        if !in_order {
            // A member given twice has its last score in its entry.
            for (score, entry) in &mut sorted {
                *score = entry.score_in_entry();
            }
            sorted.sort_unstable_by(|a, b| order(a.0, a.1.member(), b.0, b.1.member()));
        }
        let large = Large {
            index: O::from_sorted(sorted),
            table,
            queued: false,
        };
        large.debug_check();
        large
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::collections::HashMap;

    type Model = (
        BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>,
        HashMap<Vec<u8>, f64>,
    );

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    impl rand::RngCore for Rng {
        fn next_u32(&mut self) -> u32 {
            self.next() as u32
        }
        fn next_u64(&mut self) -> u64 {
            self.next()
        }
        fn fill_bytes(&mut self, dest: &mut [u8]) {
            for b in dest {
                *b = self.next() as u8;
            }
        }
        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
            self.fill_bytes(dest);
            Ok(())
        }
    }

    fn model_insert(model: &mut Model, member: &[u8], score: f64) -> bool {
        let score = score + 0.0;
        let old = model.1.insert(member.to_vec(), score);
        if let Some(old) = old {
            model.0.remove(&(OrderedFloat(old), member.to_vec()));
        }
        model.0.insert((OrderedFloat(score), member.to_vec()), ());
        old.is_none()
    }

    fn model_remove(model: &mut Model, member: &[u8]) -> bool {
        match model.1.remove(member) {
            Some(old) => {
                model.0.remove(&(OrderedFloat(old), member.to_vec()));
                true
            }
            None => false,
        }
    }

    fn ordered(model: &Model) -> Vec<(Vec<u8>, f64)> {
        model.0.keys().map(|(s, m)| (m.clone(), s.0)).collect()
    }

    fn held<O: ScoreOrder, A: Memory>(large: &Large<O, A>) -> Vec<(Vec<u8>, f64)> {
        large.iter().map(|(m, s)| (m.to_vec(), s)).collect()
    }

    fn agree<O: ScoreOrder, A: Memory>(large: &Large<O, A>, model: &Model, step: usize) {
        large.check();
        let want = ordered(model);
        assert_eq!(held(large), want, "step {step}");
        let back: Vec<(Vec<u8>, f64)> = large.iter().rev().map(|(m, s)| (m.to_vec(), s)).collect();
        assert!(back.iter().eq(want.iter().rev()), "step {step}");
        // A few members' ranks and scores, and one range of ranks.
        for (rank, (member, score)) in want.iter().enumerate().step_by(want.len() / 8 + 1) {
            assert_eq!(large.rank(member), Some((rank, *score)), "step {step}");
            assert_eq!(large.score(member), Some(*score), "step {step}");
        }
        if !want.is_empty() {
            let a = step % want.len();
            let b = (a + step % 7).min(want.len());
            let range: Vec<(Vec<u8>, f64)> = large
                .range_by_rank(a, b)
                .map(|(m, s)| (m.to_vec(), s))
                .collect();
            assert_eq!(range, want[a..b].to_vec(), "step {step}");
        }
        assert_eq!(large.iter_unordered().count(), want.len());
    }

    /// Random operations on a large set against a `BTreeMap` of `(score,
    /// member)` and a `HashMap` of scores: inserts, score changes (to equal
    /// scores too, which compare members through their entries), removals
    /// by name, from both ends, by rank, by score and by a test, clones,
    /// and sets built whole in order and not. Growth and shrinks of the
    /// table happen between them, which move slot words and never entries:
    /// a moved entry would be read freed here (and by Miri).
    fn run_model<O: ScoreOrder, A: Memory>(rounds: usize, steps: usize, seed: u64) {
        let mut rng = Rng(seed);
        for round in 0..rounds {
            let space = [30u64, 300, 3000][round % 3];
            let mut large = Large::<O, A>::with_capacity(0);
            let mut model: Model = Default::default();
            for step in 0..steps {
                let id = rng.below(space);
                let mut member = format!("m{id}").into_bytes();
                if rng.below(10) == 0 {
                    // Long ones too (a LONG entry header).
                    member.resize(140, b'~');
                }
                if id == 0 {
                    member.clear();
                }
                let score = match rng.below(6) {
                    0 => [f64::INFINITY, f64::NEG_INFINITY, -0.0, 1e300][rng.below(4) as usize],
                    1 => -(rng.below(1000) as f64),
                    _ => rng.below(4) as f64,
                };
                match rng.below(12) {
                    0..=4 => {
                        assert_eq!(
                            large.insert(&member, score),
                            model_insert(&mut model, &member, score)
                        );
                    }
                    5 => {
                        let new = large.upsert(&member, |old| {
                            let new = old.unwrap_or(0.0) + score;
                            (Some(new).filter(|n| !n.is_nan()), new)
                        });
                        if !new.is_nan() {
                            model_insert(&mut model, &member, new);
                        }
                    }
                    6 | 7 => assert_eq!(large.remove(&member), model_remove(&mut model, &member)),
                    8 => {
                        let max = rng.below(2) == 0;
                        let got = large.pop_with(max, |m, s| (m.to_vec(), s));
                        let want = ordered(&model);
                        let want = if max {
                            want.last().cloned()
                        } else {
                            want.first().cloned()
                        };
                        assert_eq!(got, want);
                        if let Some((m, _)) = want {
                            model_remove(&mut model, &m);
                        }
                    }
                    9 if !model.1.is_empty() => {
                        let n = model.1.len();
                        let a = rng.below(n as u64) as usize;
                        let b = (a + rng.below(20) as usize).min(n);
                        let want = ordered(&model);
                        assert_eq!(large.remove_ranks(a, b), b - a);
                        for (m, _) in &want[a..b] {
                            model_remove(&mut model, m);
                        }
                    }
                    10 => {
                        let lo = rng.below(4) as f64 - 1.0;
                        let hi = lo + rng.below(3) as f64;
                        let from = Bound::Included(Probe {
                            score: lo,
                            member: b"",
                        });
                        let to = Bound::Excluded(Probe {
                            score: hi,
                            member: b"",
                        });
                        let want: Vec<(Vec<u8>, f64)> = ordered(&model)
                            .into_iter()
                            .filter(|(_, s)| *s >= lo && *s < hi)
                            .collect();
                        let got: Vec<(Vec<u8>, f64)> = large
                            .range(from, to)
                            .map(|(m, s)| (m.to_vec(), s))
                            .collect();
                        assert_eq!(got, want, "step {step}");
                        // Within one score, by member.
                        let from = Bound::Excluded(Probe {
                            score: 1.0,
                            member: b"m1",
                        });
                        let to = Bound::Included(Probe {
                            score: 1.0,
                            member: b"m5",
                        });
                        let lex: Vec<Vec<u8>> = ordered(&model)
                            .into_iter()
                            .filter(|(m, s)| *s == 1.0 && m[..] > b"m1"[..] && m[..] <= b"m5"[..])
                            .map(|(m, _)| m)
                            .collect();
                        let got: Vec<Vec<u8>> =
                            large.range(from, to).map(|(m, _)| m.to_vec()).collect();
                        assert_eq!(got, lex, "step {step}");
                        if rng.below(2) == 0 {
                            let from = Bound::Included(Probe {
                                score: lo,
                                member: b"",
                            });
                            let to = Bound::Excluded(Probe {
                                score: hi,
                                member: b"",
                            });
                            assert_eq!(large.remove_range(from, to), want.len());
                            for (m, _) in &want {
                                model_remove(&mut model, m);
                            }
                        }
                    }
                    11 => {
                        if rng.below(4) == 0 {
                            let odd = |m: &[u8]| m.last().is_some_and(|b| b % 2 == 1);
                            let want: Vec<Vec<u8>> =
                                model.1.keys().filter(|m| odd(m)).cloned().collect();
                            assert_eq!(large.remove_where(|m, _| odd(m)), want.len());
                            for m in &want {
                                model_remove(&mut model, m);
                            }
                        } else if rng.below(3) == 0 {
                            // Built whole: in order, or as given.
                            let mut given = ordered(&model);
                            if rng.below(2) == 0 {
                                given.reverse();
                                if let Some(first) = given.first().cloned() {
                                    given.push(first);
                                }
                            }
                            let mut builder = Builder::<O, A>::with_capacity(given.len());
                            for (m, s) in &given {
                                builder.push(m, *s).unwrap();
                            }
                            large = builder.finish();
                        } else {
                            large = large.clone();
                        }
                    }
                    _ => {}
                }
                agree(&large, &model, step);
            }
        }
    }

    #[test]
    fn the_large_form_agrees_with_a_model() {
        if cfg!(miri) {
            run_model::<BTreeOrder, EntryPool>(3, 150, 0x9E37_79B9_7F4A_7C15);
        } else {
            run_model::<BTreeOrder, EntryPool>(12, 4_000, 0x9E37_79B9_7F4A_7C15);
            run_model::<BTreeOrder, crate::collection_table::SystemMemory>(3, 2_000, 7);
        }
    }

    /// Entries stay where they are through growth, shrinks and score
    /// changes: the address of each member's entry is the same after the
    /// table has doubled many times and shrunk again, and after its score
    /// has been rewritten in place.
    #[test]
    fn entries_do_not_move() {
        entries_stay_put::<BTreeOrder>();
    }

    fn entries_stay_put<O: ScoreOrder>() {
        let n = if cfg!(miri) { 300 } else { 50_000 };
        let mut large = Large::<O>::with_capacity(0);
        let addr = |large: &Large<O>, i: usize| {
            large
                .table()
                .entry(format!("m{i}").as_bytes())
                .unwrap()
                .as_ptr()
                .addr()
        };
        large.insert(b"m0", 0.0);
        let first = addr(&large, 0);
        for i in 1..n {
            large.insert(format!("m{i}").as_bytes(), i as f64);
        }
        let before: Vec<usize> = (0..n).step_by(7).map(|i| addr(&large, i)).collect();
        assert_eq!(addr(&large, 0), first);
        // Rescored: in place.
        large.insert(b"m0", -1.0);
        assert_eq!(addr(&large, 0), first);
        // Down to a few, through shrinks; the background's steps too.
        for i in (0..n).filter(|i| i % 7 != 0) {
            assert!(large.remove(format!("m{i}").as_bytes()));
        }
        while !large.step_resize(usize::MAX) {}
        let after: Vec<usize> = (0..n).step_by(7).map(|i| addr(&large, i)).collect();
        assert_eq!(before, after);
        large.check();
    }

    /// A member the table cannot hold is refused by a build, with nothing
    /// added; a command's insert leaves it out.
    #[test]
    fn a_build_refuses_what_the_table_cannot_hold() {
        build_refuses_what_the_table_cannot_hold::<BTreeOrder>();
    }

    fn build_refuses_what_the_table_cannot_hold<O: ScoreOrder>() {
        crate::collection_table::set_test_max_len(Some(8));
        let mut builder = Builder::<O>::with_capacity(2);
        builder.push(b"short", 1.0).unwrap();
        assert_eq!(builder.push(b"longer than eight", 2.0), Err(TooLong));
        assert_eq!(builder.len(), 1);
        let mut large = builder.finish();
        large.insert(b"longer than eight", 3.0);
        assert_eq!(large.len(), 1);
        assert_eq!(large.score(b"longer than eight"), None);
        large.check();
        crate::collection_table::set_test_max_len(None);
    }

    // -----------------------------------------------------------------------
    // The counted tree as the index
    // -----------------------------------------------------------------------

    #[test]
    fn the_counted_order_agrees_with_a_model() {
        if cfg!(miri) {
            run_model::<CountedOrder, EntryPool>(3, 150, 0x9E37_79B9_7F4A_7C15);
        } else {
            run_model::<CountedOrder, EntryPool>(12, 4_000, 0x9E37_79B9_7F4A_7C15);
            run_model::<CountedOrder, crate::collection_table::SystemMemory>(3, 2_000, 7);
        }
    }

    #[test]
    fn entries_do_not_move_under_the_counted_order() {
        entries_stay_put::<CountedOrder>();
    }

    #[test]
    fn a_build_under_the_counted_order_refuses_what_the_table_cannot_hold() {
        build_refuses_what_the_table_cannot_hold::<CountedOrder>();
    }

    fn shuffle<T>(items: &mut [T], rng: &mut Rng) {
        for i in (1..items.len()).rev() {
            items.swap(i, rng.below(i as u64 + 1) as usize);
        }
    }

    /// Entries outside any table, laid out as a table's: each entry's own
    /// score is 0, so an index reading scores through them would misorder.
    fn detached(n: usize) -> Vec<(Vec<u8>, Box<[u64]>)> {
        (0..n)
            .map(|i| {
                let member = format!("m{i:05}").into_bytes();
                let entry = detached_scored_entry(&member);
                (member, entry)
            })
            .collect()
    }

    fn entry_of(entry: &[u64]) -> EntryRef {
        EntryRef::new(NonNull::from(entry).cast::<u8>())
    }

    /// Every reference given comes back once, the very one, by removal or
    /// pop; one of equal bytes at another address is not taken for it.
    #[test]
    fn the_counted_order_hands_back_each_reference_it_holds() {
        let n = if cfg!(miri) { 200 } else { 3_000 };
        let entries = detached(n);
        // Seven scores: runs of equal scores across leaves, ordered by member.
        let score = |i: usize| (i % 7) as f64;
        let mut index = CountedOrder::default();
        for (i, (_, entry)) in entries.iter().enumerate() {
            index.insert(score(i), entry_of(entry)).unwrap();
        }
        let mut model: BTreeMap<(OrderedFloat<f64>, &[u8]), usize> = entries
            .iter()
            .enumerate()
            .map(|(i, (member, _))| ((OrderedFloat(score(i)), &member[..]), i))
            .collect();
        let addr = |i: usize| entries[i].1.as_ptr().addr();
        let want: Vec<(f64, usize)> = model.iter().map(|(k, &i)| (k.0.0, addr(i))).collect();
        let held: Vec<(f64, usize)> = index.iter().map(|(s, e)| (s, e.addr())).collect();
        assert_eq!(held, want, "in order by the index's own scores");
        let twin = detached_scored_entry(&entries[5].0);
        assert!(index.remove(score(5), &entry_of(&twin)).is_none());
        assert_eq!(index.len(), n);
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let mut back = vec![false; n];
        while !model.is_empty() {
            // The ends and a rank, which `Large` does not ask for.
            let at = |entry: Option<(f64, &EntryRef)>| entry.map(|(s, e)| (s, e.addr()));
            let end = |(key, &i): (&(OrderedFloat<f64>, &[u8]), &usize)| (key.0.0, addr(i));
            assert_eq!(at(index.first()), model.iter().next().map(end));
            assert_eq!(at(index.last()), model.iter().next_back().map(end));
            let rank = rng.below(model.len() as u64) as usize;
            assert_eq!(at(index.nth(rank)), model.iter().nth(rank).map(end));
            let way = rng.below(3);
            let (key, i) = match way {
                0 => model.pop_first(),
                1 => model.pop_last(),
                _ => {
                    let at = rng.below(model.len() as u64) as usize;
                    let key = *model.keys().nth(at).unwrap();
                    model.remove_entry(&key)
                }
            }
            .unwrap();
            let (score, held) = match way {
                0 => index.pop_first().unwrap(),
                1 => index.pop_last().unwrap(),
                _ => (
                    key.0.0,
                    index.remove(key.0.0, &entry_of(&entries[i].1)).unwrap(),
                ),
            };
            assert_eq!((score, held.addr()), (key.0.0, addr(i)));
            assert!(!std::mem::replace(&mut back[i], true), "handed back twice");
            assert_eq!(index.len(), model.len());
            if model.len().is_multiple_of(97) {
                index.0.check();
            }
        }
        assert!(back.iter().all(|&back| back));
        assert_eq!((index.len(), index.heap_bytes()), (0, 0));
    }

    /// A removal takes the very reference asked for, at its own score only.
    #[test]
    fn a_counted_removal_takes_only_the_reference_asked_for() {
        let [x, y, twin] = [b"x", b"y", b"x"].map(|m| detached_scored_entry(m));
        let mut index = CountedOrder::default();
        index.insert(1.0, entry_of(&x)).unwrap();
        index.insert(5.0, entry_of(&y)).unwrap();
        assert!(
            index.remove(2.0, &entry_of(&x)).is_none(),
            "at another score"
        );
        assert!(
            index.remove(1.0, &entry_of(&twin)).is_none(),
            "equal bytes, not held"
        );
        assert_eq!(index.len(), 2);
        let back = index.remove(1.0, &entry_of(&x)).map(|e| e.addr());
        assert_eq!(back, Some(x.as_ptr().addr()));
        assert_eq!(index.len(), 1);
        index.0.check();
    }

    /// A refused insert hands back the reference given; a refused rescore
    /// moves nothing.
    fn refusals_change_nothing<O: ScoreOrder>() {
        let [x, y, twin] = [b"x", b"y", b"x"].map(|m| detached_scored_entry(m));
        let addr = |e: &[u64]| e.as_ptr().addr();
        let holds =
            |index: &O| -> Vec<(f64, usize)> { index.iter().map(|(s, e)| (s, e.addr())).collect() };
        let mut index = O::default();
        index.insert(1.0, entry_of(&x)).unwrap();
        index.insert(5.0, entry_of(&y)).unwrap();
        let two = [(1.0, addr(&x)), (5.0, addr(&y))];
        let back = index.insert(1.0, entry_of(&twin)).map_err(|e| e.addr());
        assert_eq!(
            back,
            Err(addr(&twin)),
            "an equal key: its own reference back"
        );
        assert_eq!(holds(&index), two);
        assert!(
            index.remove(4.0, &entry_of(&y)).is_none(),
            "at another score"
        );
        assert!(!index.rescore(3.0, 7.0, &entry_of(&x)), "not at 3");
        assert_eq!(holds(&index), two);
        // `new` held by an equal member: it stays where it was.
        index.insert(9.0, entry_of(&twin)).unwrap();
        assert!(!index.rescore(1.0, 9.0, &entry_of(&x)));
        let three = [(1.0, addr(&x)), (5.0, addr(&y)), (9.0, addr(&twin))];
        assert_eq!(holds(&index), three);
        assert!(index.rescore(1.0, 7.0, &entry_of(&x)));
        let moved = [(5.0, addr(&y)), (7.0, addr(&x)), (9.0, addr(&twin))];
        assert_eq!(holds(&index), moved);
        assert_eq!(index.rank(7.0, &entry_of(&x)), 1);
    }

    #[test]
    fn refusals_change_nothing_in_either_order() {
        refusals_change_nothing::<BTreeOrder>();
        refusals_change_nothing::<CountedOrder>();
    }

    /// Runs `f`, which breaks the index's contract once: reported once. A
    /// debug build stops there; a release build goes on.
    fn reported_once<R>(f: impl FnOnce() -> R) -> Option<R> {
        let before = DISAGREED.with(Cell::get);
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        assert_eq!(DISAGREED.with(Cell::get), before + 1, "reported once");
        assert_eq!(out.is_err(), cfg!(debug_assertions), "debug builds stop");
        out.ok()
    }

    /// A build given one member twice holds it once, and says so.
    #[test]
    fn a_counted_build_given_a_member_twice_reports_it() {
        let [x, twin] = [b"x"; 2].map(|m| detached_scored_entry(m));
        let given = vec![(1.0, entry_of(&x)), (1.0, entry_of(&twin))];
        if let Some(built) = reported_once(|| CountedOrder::from_sorted(given)) {
            let holds: Vec<usize> = built.iter().map(|(_, e)| e.addr()).collect();
            assert_eq!(holds, [x.as_ptr().addr()]);
            built.0.check();
        }
    }

    /// What the next call of its kind does wrong.
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Fault {
        RefuseInsert,
        RefuseRescore,
        /// Finds nothing, the member still held.
        MissRemoval,
        /// Hands back another member, the one asked for still held.
        HandBackAnother,
    }

    /// An index that breaks its contract once, as told, never its safety.
    #[derive(Default)]
    struct Faulty<O> {
        index: O,
        next: Option<Fault>,
    }

    impl<O> Faulty<O> {
        fn meets(&mut self, fault: Fault) -> bool {
            self.next.take_if(|next| *next == fault).is_some()
        }
    }

    // SAFETY: `O`'s, which each fault keeps: a refused insert hands back the
    // reference given; a refused rescore and a missed removal change
    // nothing; a member handed back in place of another is popped first, so
    // no longer held.
    unsafe impl<O: ScoreOrder> ScoreOrder for Faulty<O> {
        type Iter<'a>
            = O::Iter<'a>
        where
            Self: 'a;
        type Range<'a>
            = O::Range<'a>
        where
            Self: 'a;

        fn len(&self) -> usize {
            self.index.len()
        }

        fn insert(&mut self, score: f64, entry: EntryRef) -> Result<(), EntryRef> {
            if self.meets(Fault::RefuseInsert) {
                return Err(entry);
            }
            self.index.insert(score, entry)
        }

        fn remove(&mut self, score: f64, entry: &EntryRef) -> Option<EntryRef> {
            if self.meets(Fault::MissRemoval) {
                return None;
            }
            if self.meets(Fault::HandBackAnother) {
                let first = self.index.first().is_some_and(|(_, e)| e == entry);
                let other = if first {
                    self.index.pop_last()
                } else {
                    self.index.pop_first()
                };
                return other.map(|(_, e)| e);
            }
            self.index.remove(score, entry)
        }

        fn rescore(&mut self, old: f64, new: f64, entry: &EntryRef) -> bool {
            !self.meets(Fault::RefuseRescore) && self.index.rescore(old, new, entry)
        }

        fn rank(&self, score: f64, entry: &EntryRef) -> usize {
            self.index.rank(score, entry)
        }

        fn nth(&self, rank: usize) -> Option<(f64, &EntryRef)> {
            self.index.nth(rank)
        }

        fn first(&self) -> Option<(f64, &EntryRef)> {
            self.index.first()
        }

        fn last(&self) -> Option<(f64, &EntryRef)> {
            self.index.last()
        }

        fn pop_first(&mut self) -> Option<(f64, EntryRef)> {
            self.index.pop_first()
        }

        fn pop_last(&mut self) -> Option<(f64, EntryRef)> {
            self.index.pop_last()
        }

        fn iter(&self) -> Self::Iter<'_> {
            self.index.iter()
        }

        fn range_by_rank(&self, start: usize, end: usize) -> Self::Iter<'_> {
            self.index.range_by_rank(start, end)
        }

        fn range(&self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> Self::Range<'_> {
            self.index.range(from, to)
        }

        fn from_sorted(sorted: Vec<(f64, EntryRef)>) -> Self {
            Faulty {
                index: O::from_sorted(sorted),
                next: None,
            }
        }

        fn heap_bytes(&self) -> usize {
            self.index.heap_bytes()
        }
    }

    /// Each fault, again and again: reported, the set whole, entries unmoved.
    /// Once the index is sound, the same writes are done.
    fn faults_leave_the_set_whole<O: ScoreOrder>() {
        let n = if cfg!(miri) { 60 } else { 600 };
        let member = |i: usize| format!("m{i}").into_bytes();
        let mut large = Large::<Faulty<O>>::with_capacity(0);
        for i in 0..n {
            assert!(large.insert(&member(i), i as f64));
        }
        let addr = |large: &Large<Faulty<O>>, i: usize| {
            large.table().entry(&member(i)).map(|e| e.as_ptr().addr())
        };
        let before: Vec<_> = (0..n).map(|i| addr(&large, i)).collect();
        let release = !cfg!(debug_assertions);
        for round in 0..8 {
            let i = round * 7;
            let want = held(&large);
            large.index.next = Some(Fault::RefuseRescore);
            reported_once(|| large.insert(&member(i), -1.0 - i as f64));
            assert_eq!(large.score(&member(i)), Some(i as f64), "the old score");
            large.index.next = Some(Fault::RefuseInsert);
            reported_once(|| large.insert(b"new", 0.5));
            assert_eq!(large.score(b"new"), None, "not added");
            for fault in [Fault::MissRemoval, Fault::HandBackAnother] {
                large.index.next = Some(fault);
                let out = reported_once(|| large.remove(&member(i + 1)));
                assert_eq!(out, release.then_some(false));
                large.check();
            }
            assert_eq!(held(&large), want, "nothing changed");
            assert_eq!(addr(&large, i), before[i], "the entry where it was");
            // Sound again: each write done.
            assert!(!large.insert(&member(i), -1.0 - i as f64));
            assert_eq!(large.rank(&member(i)), Some((0, -1.0 - i as f64)));
            assert!(large.insert(b"new", 0.5));
            assert!(large.remove(b"new"));
            assert!(large.remove(&member(i + 1)));
            assert_eq!(addr(&large, i), before[i], "rescored in place");
            large.check();
        }
        // A listed removal the index misses: the rest go, that one stays.
        for fault in [Fault::MissRemoval, Fault::HandBackAnother] {
            let len = large.len();
            let missed = large.range_by_rank(20, 21).next().map(|(m, _)| m.to_vec());
            large.index.next = Some(fault);
            let out = reported_once(|| large.remove_ranks(20, 30));
            assert_eq!(out, release.then_some(9));
            assert_eq!(large.len(), len - if release { 9 } else { 0 });
            assert!(
                large.score(&missed.unwrap()).is_some(),
                "the one missed stays"
            );
            large.check();
        }
    }

    #[test]
    fn faults_leave_a_btree_set_whole() {
        faults_leave_the_set_whole::<BTreeOrder>();
    }

    #[test]
    fn faults_leave_a_counted_set_whole() {
        faults_leave_the_set_whole::<CountedOrder>();
    }

    /// One write, maybe meeting `fault`: its answer, `None` if a debug build stops.
    fn write<O: ScoreOrder>(
        large: &mut Large<Faulty<O>>,
        member: &[u8],
        score: Option<f64>,
        fault: Option<Fault>,
    ) -> Option<bool> {
        large.index.next = fault;
        let mut write = || match score {
            Some(score) => large.insert(member, score),
            None => large.remove(member),
        };
        let out = match fault {
            Some(_) => reported_once(write),
            None => Some(write()),
        };
        assert_eq!(large.index.next, None, "the fault met");
        out
    }

    /// Both orders in step with a model, faults and all, through a deep tree.
    /// A write that meets a fault changes nothing.
    #[test]
    fn faults_leave_both_orders_as_one() {
        let (fill, steps, every) = if cfg!(miri) {
            (300, 300, 100)
        } else {
            (50_000, 20_000, 2_000)
        };
        let space = fill as u64 + fill as u64 / 5;
        let mut rng = Rng(0x6C07_8965_D5F0_4B31);
        let mut a = Large::<Faulty<BTreeOrder>>::with_capacity(0);
        let mut b = Large::<Faulty<CountedOrder>>::with_capacity(0);
        let mut model: HashMap<Vec<u8>, f64> = HashMap::new();
        let mut ids: Vec<u64> = (0..fill as u64).collect();
        shuffle(&mut ids, &mut rng);
        for id in ids {
            let score = (id % 512) as f64;
            assert!(a.insert(&name(id), score) && b.insert(&name(id), score));
            model.insert(name(id), score);
        }
        if fill >= 40_000 {
            assert!(
                b.index.index.0.height() >= 4,
                "three levels of internal nodes"
            );
        }
        let whole = |a: &Large<Faulty<BTreeOrder>>, b: &Large<Faulty<CountedOrder>>| {
            a.check();
            b.check();
            b.index.index.0.check();
            assert!(a.iter().eq(b.iter()));
        };
        let mut faults = 0;
        for step in 0..steps {
            let member = name(rng.below(space));
            let had = model.get(&member).copied();
            if rng.below(32) == 0 {
                let max = rng.below(2) == 0;
                let take = |m: &[u8], s: f64| (m.to_vec(), s);
                let popped = a.pop_with(max, take);
                assert_eq!(popped, b.pop_with(max, take));
                model.remove(&popped.unwrap().0);
                continue;
            }
            let score = (rng.below(1_024) as f64) / 2.0;
            let score = (rng.below(3) != 0).then_some(score);
            // One write in eight meets the fault its call can meet.
            let fault = match (rng.below(8) == 0, score, had) {
                (false, ..) => None,
                (true, None, Some(_)) => {
                    Some([Fault::MissRemoval, Fault::HandBackAnother][rng.below(2) as usize])
                }
                (true, Some(_), None) => Some(Fault::RefuseInsert),
                (true, Some(new), Some(old)) if new.to_bits() != old.to_bits() => {
                    Some(Fault::RefuseRescore)
                }
                _ => None,
            };
            let out = write(&mut a, &member, score, fault);
            assert_eq!(out, write(&mut b, &member, score, fault));
            match (fault, score) {
                (Some(_), _) => faults += 1,
                (None, Some(score)) => drop(model.insert(member.clone(), score)),
                (None, None) => drop(model.remove(&member)),
            }
            let want = model.get(&member).copied();
            assert_eq!((a.score(&member), b.score(&member)), (want, want));
            assert_eq!(a.rank(&member), b.rank(&member));
            assert_eq!((a.len(), b.len()), (model.len(), model.len()));
            if step % every == 0 {
                whole(&a, &b);
            }
        }
        whole(&a, &b);
        let mut want: Vec<(Vec<u8>, f64)> = model.into_iter().collect();
        want.sort_by(|x, y| order(x.1, &x.0, y.1, &y.0));
        assert_eq!(held(&a), want);
        assert!(faults > steps / 20, "{faults} faults met");
    }

    /// In order, reversed and shuffled: the same index, its scores its own.
    #[test]
    fn the_counted_order_is_built_from_members_in_any_order() {
        let n = if cfg!(miri) { 300 } else { 5_000 };
        let entries = detached(n);
        let score = |i: usize| ((i * 7_919) % 13) as f64;
        let mut sorted: Vec<usize> = (0..n).collect();
        sorted.sort_by(|&a, &b| order(score(a), &entries[a].0, score(b), &entries[b].0));
        let want: Vec<(f64, usize)> = sorted
            .iter()
            .map(|&i| (score(i), entries[i].1.as_ptr().addr()))
            .collect();
        let reversed: Vec<usize> = sorted.iter().rev().copied().collect();
        let mut shuffled = sorted.clone();
        shuffle(&mut shuffled, &mut Rng(7));
        for given in [sorted, reversed, shuffled] {
            let given = given.iter().map(|&i| (score(i), entry_of(&entries[i].1)));
            let index = CountedOrder::from_sorted(given.collect());
            index.0.check();
            let held: Vec<(f64, usize)> = index.iter().map(|(s, e)| (s, e.addr())).collect();
            assert_eq!(held, want);
        }
    }

    /// Member `id`'s name: one in ten long (a LONG entry header), 0 empty.
    fn name(id: u64) -> Vec<u8> {
        let mut member = format!("m{id}").into_bytes();
        if id % 10 == 9 {
            member.resize(140, b'~');
        }
        if id == 0 {
            member.clear();
        }
        member
    }

    fn build<O: ScoreOrder>(given: &[(Vec<u8>, f64)]) -> Large<O> {
        let mut builder = Builder::<O>::with_capacity(given.len());
        for (member, score) in given {
            builder.push(member, *score).unwrap();
        }
        builder.finish()
    }

    /// Both sets, and the counted tree itself, whole; every member in order
    /// both ways, and ranks spread over the set.
    fn same(a: &Large<BTreeOrder>, b: &Large<CountedOrder>) {
        a.check();
        b.check();
        b.index.0.check();
        assert!(a.iter().eq(b.iter()), "in order");
        assert!(a.iter().rev().eq(b.iter().rev()), "in reverse");
        let n = b.len();
        for rank in (0..n).step_by(n / 40 + 1) {
            let (member, score) = b.range_by_rank(rank, rank + 1).next().unwrap();
            assert_eq!(b.rank(member), Some((rank, score)));
            assert_eq!(a.rank(member), Some((rank, score)));
        }
    }

    /// The same writes to a set under each index, every answer compared:
    /// ranks, scores, ranges by rank, score and member, pops, removals,
    /// builds and clones. First `fill` members, in no order: even ones alone
    /// at a score, odd ones in runs of equal scores.
    fn differential(seed: u64, fill: usize, space: u64, steps: usize, every: usize) {
        let mut rng = Rng(seed);
        let mut a = Large::<BTreeOrder>::with_capacity(0);
        let mut b = Large::<CountedOrder>::with_capacity(0);
        let mut ids: Vec<u64> = (0..fill as u64).collect();
        shuffle(&mut ids, &mut rng);
        for id in ids {
            let score = if id % 2 == 0 {
                (id * 4) as f64
            } else {
                (id % 64) as f64
            };
            assert_eq!(a.insert(&name(id), score), b.insert(&name(id), score));
        }
        same(&a, &b);
        if fill >= 40_000 {
            assert!(b.index.0.height() >= 4, "three levels of internal nodes");
        }
        for step in 0..steps {
            let member = name(rng.below(space));
            let score = match rng.below(5) {
                0 => [f64::INFINITY, f64::NEG_INFINITY, -0.0, 1e300][rng.below(4) as usize],
                1 => rng.below(64) as f64,
                _ => (rng.below(space) * 4) as f64,
            };
            match rng.below(16) {
                0..=3 => assert_eq!(a.insert(&member, score), b.insert(&member, score)),
                4 | 5 => {
                    // A nudge: mostly keeps the rank, so is made in place.
                    let by = [-1.0, 1.0, 0.5][rng.below(3) as usize];
                    let nudge = move |old: Option<f64>| (old.map(|old| old + by), old);
                    assert_eq!(a.upsert(&member, nudge), b.upsert(&member, nudge));
                }
                6 | 7 => assert_eq!(a.remove(&member), b.remove(&member)),
                8 => {
                    let max = rng.below(2) == 0;
                    let take = |m: &[u8], s: f64| (m.to_vec(), s);
                    assert_eq!(a.pop_with(max, take), b.pop_with(max, take));
                }
                9 => {
                    let start = rng.below(a.len() as u64 + 1) as usize;
                    let end = start + rng.below(20) as usize;
                    assert!(a.range_by_rank(start, end).eq(b.range_by_rank(start, end)));
                    assert!(
                        a.range_by_rank(start, end)
                            .rev()
                            .eq(b.range_by_rank(start, end).rev())
                    );
                    if rng.below(4) == 0 {
                        assert_eq!(a.remove_ranks(start, end), b.remove_ranks(start, end));
                    }
                }
                10 | 11 => {
                    let mut pair = [name(rng.below(space)), name(rng.below(space))];
                    pair.sort();
                    let lo = if rng.below(2) == 0 {
                        rng.below(72) as f64 - 8.0
                    } else {
                        (rng.below(space) * 4) as f64
                    };
                    fn probe(score: f64, member: &[u8]) -> Probe<'_> {
                        Probe { score, member }
                    }
                    let (from, to) = if rng.below(2) == 0 {
                        // Scores from `lo` to before `hi`.
                        let hi = lo + [0.0, 1.0, 4.0, 50.0, 400.0][rng.below(5) as usize];
                        (
                            Bound::Included(probe(lo, b"")),
                            Bound::Excluded(probe(hi, b"")),
                        )
                    } else {
                        // Members within one score, the first bound either way.
                        let from = probe(lo, &pair[0]);
                        let from = if rng.below(2) == 0 {
                            Bound::Included(from)
                        } else {
                            Bound::Excluded(from)
                        };
                        (from, Bound::Included(probe(lo, &pair[1])))
                    };
                    assert!(a.range(from, to).eq(b.range(from, to)));
                    assert!(a.range(from, to).rev().eq(b.range(from, to).rev()));
                    if rng.below(8) == 0 {
                        assert_eq!(a.remove_range(from, to), b.remove_range(from, to));
                    }
                }
                12 => {
                    assert_eq!(a.rank(&member), b.rank(&member));
                    assert_eq!(a.score(&member), b.score(&member));
                }
                13 if rng.below(8) == 0 => {
                    let gone = |m: &[u8], _: f64| m.last().is_some_and(|byte| byte % 5 == 0);
                    assert_eq!(a.remove_where(gone), b.remove_where(gone));
                }
                14 if rng.below(64) == 0 => {
                    // Clones; the originals go first, so the copies stand alone.
                    let (copy_a, copy_b) = (a.clone(), b.clone());
                    (a, b) = (copy_a, copy_b);
                    same(&a, &b);
                }
                15 if rng.below(64) == 0 => {
                    // Built whole: in order, or shuffled with one given twice.
                    let mut given: Vec<(Vec<u8>, f64)> =
                        a.iter().map(|(m, s)| (m.to_vec(), s)).collect();
                    if rng.below(2) == 0 {
                        shuffle(&mut given, &mut rng);
                        if let Some((m, _)) = given.first().cloned() {
                            given.push((m, score + 0.0));
                        }
                    }
                    (a, b) = (build(&given), build(&given));
                    same(&a, &b);
                }
                _ => {}
            }
            if step % every == 0 {
                same(&a, &b);
            }
        }
        same(&a, &b);
    }

    #[test]
    fn the_counted_order_answers_as_the_btree_does() {
        let (fill, steps) = if cfg!(miri) {
            (300, 300)
        } else {
            (3_000, 30_000)
        };
        differential(0x5851_F42D_4C95_7F2D, fill, 4_000, steps, 1_000);
    }

    /// Past 40,000 members, three levels of internal nodes: neighbours and
    /// separators found two levels up and more.
    #[test]
    fn a_deep_counted_order_answers_as_the_btree_does() {
        let (fill, steps) = if cfg!(miri) {
            (1_200, 400)
        } else {
            (50_000, 20_000)
        };
        let space = fill as u64 + fill as u64 / 5;
        differential(0x1405_7B7E_F767_814F, fill, space, steps, 5_000);
    }

    /// Every member at one score: their bytes, read through the entries, are
    /// the whole order, as `ZRANGEBYLEX` reads it.
    #[test]
    fn members_of_one_score_order_by_their_bytes() {
        let n = if cfg!(miri) { 300 } else { 10_000 };
        let mut rng = Rng(11);
        let mut a = Large::<BTreeOrder>::with_capacity(0);
        let mut b = Large::<CountedOrder>::with_capacity(0);
        for _ in 0..n {
            // Lengths differ: a shorter member first where one begins another.
            let member = format!("{:x}", rng.next() >> rng.below(60)).into_bytes();
            assert_eq!(a.insert(&member, 0.0), b.insert(&member, 0.0));
        }
        same(&a, &b);
        let names: Vec<Vec<u8>> = b.iter().map(|(m, _)| m.to_vec()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        for _ in 0..200 {
            let mut at = [rng.below(names.len() as u64), rng.below(names.len() as u64)];
            at.sort();
            let [i, j] = at.map(|at| at as usize);
            let from = Bound::Excluded(Probe {
                score: 0.0,
                member: &names[i],
            });
            let to = Bound::Included(Probe {
                score: 0.0,
                member: &names[j],
            });
            let got: Vec<&[u8]> = b.range(from, to).map(|(m, _)| m).collect();
            let want: Vec<&[u8]> = names[i + 1..=j].iter().map(|m| &m[..]).collect();
            assert_eq!(got, want);
            assert!(a.range(from, to).eq(b.range(from, to)));
        }
        let from = Bound::Included(Probe {
            score: 0.0,
            member: &names[n / 4],
        });
        let to = Bound::Included(Probe {
            score: 0.0,
            member: &names[n / 2],
        });
        assert_eq!(a.remove_range(from, to), b.remove_range(from, to));
        same(&a, &b);
    }

    /// Rescores in place and past neighbours: each entry stays where it is,
    /// and its score in the table and in the index stay one.
    #[test]
    fn rescores_keep_the_table_and_the_index_as_one() {
        let n = if cfg!(miri) { 300 } else { 20_000 };
        let names: Vec<Vec<u8>> = (0..n).map(|i| format!("m{i}").into_bytes()).collect();
        let mut scores: Vec<f64> = (0..n).map(|i| (i * 4) as f64).collect();
        let mut large = Large::<CountedOrder>::with_capacity(0);
        for (member, &score) in names.iter().zip(&scores) {
            assert!(large.insert(member, score));
        }
        let addrs = |large: &Large<CountedOrder>| -> Vec<usize> {
            let entry = |member: &Vec<u8>| large.table().entry(member).unwrap().as_ptr().addr();
            names.iter().map(entry).collect()
        };
        let before = addrs(&large);
        let mut rng = Rng(0xDA94_2042_E4DD_58B5);
        for step in 0..3 * n {
            let i = rng.below(n as u64) as usize;
            let new = match rng.below(4) {
                // Beside its neighbours, 4 apart: mostly in place.
                0 | 1 => scores[i] + [-1.0, 1.0][rng.below(2) as usize],
                2 => (rng.below(n as u64) * 4) as f64 + 2.0,
                _ => scores[i],
            };
            assert!(!large.insert(&names[i], new));
            scores[i] = new;
            let (rank, score) = large.rank(&names[i]).unwrap();
            assert_eq!(score.to_bits(), new.to_bits(), "the entry's score");
            let by_index = large.range_by_rank(rank, rank + 1).next();
            assert_eq!(by_index, Some((&names[i][..], new)), "the index's score");
            if step % 5_000 == 0 {
                large.check();
                large.index.0.check();
            }
        }
        large.check();
        large.index.0.check();
        assert_eq!(addrs(&large), before, "a rescore moves no entry");
        let mut want: Vec<(Vec<u8>, f64)> = names.iter().cloned().zip(scores).collect();
        want.sort_by(|x, y| order(x.1, &x.0, y.1, &y.0));
        assert_eq!(held(&large), want);
    }

    /// Filled and emptied, each time another way: every node given back,
    /// the index and the table empty together.
    #[test]
    fn filled_and_emptied_again_and_again() {
        let n = if cfg!(miri) { 200 } else { 10_000 };
        let mut rng = Rng(0x9FB2_1C65_1E98_DF25);
        let mut large = Large::<CountedOrder>::with_capacity(0);
        for cycle in 0..8 {
            let mut ids: Vec<usize> = (0..n).collect();
            shuffle(&mut ids, &mut rng);
            let member = |i: usize| format!("c{cycle}-{i}").into_bytes();
            for &i in &ids {
                assert!(large.insert(&member(i), (i % 50) as f64));
            }
            large.check();
            large.index.0.check();
            let nodes = large.index.0.heap_bytes();
            assert!(nodes > 0);
            assert_eq!(large.allocation_size(), large.table_bytes() + nodes);
            match cycle % 4 {
                0 => ids.iter().for_each(|&i| assert!(large.remove(&member(i)))),
                1 => while large.pop_with(false, |_, _| ()).is_some() {},
                2 => while large.pop_with(true, |_, _| ()).is_some() {},
                _ => assert_eq!(large.remove_ranks(0, n), n),
            }
            assert!(large.is_empty());
            assert_eq!(large.index.heap_bytes(), 0, "every node given back");
            large.check();
        }
    }

    /// A clone's references point into its own table: the original gone
    /// first, the copy reads and changes on its own.
    #[test]
    fn a_clone_of_a_counted_order_stands_alone() {
        let n = if cfg!(miri) { 300 } else { 20_000 };
        let member = |i: usize| format!("m{i}").into_bytes();
        let mut original = Large::<CountedOrder>::with_capacity(0);
        for i in 0..n {
            original.insert(&member(i), (i % 97) as f64);
        }
        let mut copy = original.clone();
        copy.check();
        copy.index.0.check();
        let addrs = |large: &Large<CountedOrder>| -> std::collections::HashSet<usize> {
            large.index.iter().map(|(_, entry)| entry.addr()).collect()
        };
        assert!(
            addrs(&original).is_disjoint(&addrs(&copy)),
            "no entry shared"
        );
        let kept = held(&original);
        assert_eq!(held(&copy), kept);
        for i in (0..n).step_by(3) {
            assert!(copy.remove(&member(i)));
        }
        for i in (1..n).step_by(3) {
            copy.insert(&member(i), -(i as f64));
        }
        assert_eq!(held(&original), kept);
        original.check();
        drop(original);
        copy.check();
        copy.index.0.check();
        assert_eq!(copy.len(), n - n.div_ceil(3));
    }
}
