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
//! entry. In this module the index is [`BTreeOrder`], a `BTreeMap` as the
//! large form has had (rank and ranges cost what they did); a counted tree
//! (#875) replaces it behind the same trait.
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
//!   `value_slab::POOL_MAX_BLOCK`, of the system allocator. So an entry's address
//!   is fixed from the insert that makes it to the removal that frees it.
//! - **Only this module changes the table or the index.** [`Large`] owns
//!   both, privately; every write goes through its methods, and an
//!   [`EntryRef`] is made only here.
//! - **The rule.** The index drops its reference to an entry before the
//!   table frees the entry (a removal takes the member out of the index
//!   first, then out of the table), and no reference is held across a call
//!   that can free or rewrite its entry (a score is rewritten in place only
//!   under `&mut self`, and the index keeps each member's score itself, so it
//!   never reads a score through a reference). Dropping the set drops the
//!   index first: it is the first field of [`Large`].
//!
//! Debug builds check after every write that the index and the table hold
//! as many members; the model test (`tests` below) checks everything against
//! a `BTreeMap` and a `HashMap` through random operations, and runs under
//! Miri (`cargo +nightly miri test --lib zset_table`), entry pool included.

// Some of the index's interface (`nth`, `first`, `last`) is for the
// counted tree (#875) and the checks; the binary does not call all of it.
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
    /// copy); for the checks.
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
/// An implementation holds each [`EntryRef`] from the `insert` or
/// `from_sorted` that gave it to the `remove`, `pop_first` or `pop_last` that
/// hands it back, or to its own drop, and no longer: it never keeps a
/// reference elsewhere or makes one, and it reads a member's bytes only
/// through references it holds, or that a call gives it for that call. It
/// keeps each member's score itself (it never reads a score through a
/// reference: one being rescored is already the new one there). `Large`
/// relies on this to free an entry once the index has handed its reference
/// back.
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

    /// Add a member at `score`; it is not one.
    fn insert(&mut self, score: f64, entry: EntryRef);

    /// Take the member `entry` (at `score`) out: its reference, `None` if it
    /// is not held.
    fn remove(&mut self, score: f64, entry: &EntryRef) -> Option<EntryRef>;

    /// Move the member `entry` from `old` to `new`.
    fn rescore(&mut self, old: f64, new: f64, entry: &EntryRef);

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
// The index the large form has had: a B-tree
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
/// a range by rank walks to its start from the nearer end.
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
// `pop_last` move the key out), and drops the rest with itself; the
// references `key` makes for a lookup, and the probe keys of a range (which
// point to entries of their own, never a table's), are dropped when the call
// returns. Members' bytes are read only through keys in the map or the
// call's own. Scores are the keys' own.
unsafe impl ScoreOrder for BTreeOrder {
    type Iter<'a> = BTreeIter<'a>;
    type Range<'a> = BTreeRange<'a>;

    #[inline]
    fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    fn insert(&mut self, score: f64, entry: EntryRef) {
        let fresh = self.map.insert(Key { score, entry }, ()).is_none();
        debug_assert!(fresh, "a member inserted twice");
    }

    #[inline]
    fn remove(&mut self, score: f64, entry: &EntryRef) -> Option<EntryRef> {
        self.map
            .remove_entry(&Self::key(score, entry))
            .map(|(key, ())| key.entry)
    }

    #[inline]
    fn rescore(&mut self, old: f64, new: f64, entry: &EntryRef) {
        let held = self.remove(old, entry).expect("a member of the index");
        self.insert(new, held);
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
// The large form
// ---------------------------------------------------------------------------

/// A large sorted set: the table of members and scores, and the index of
/// their order. See the module's notes for the rule that keeps the index's
/// references sound.
pub struct Large<O: ScoreOrder = BTreeOrder, A: Memory = EntryPool> {
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
                    if score.to_bits() != old.to_bits() {
                        // SAFETY: a live entry of the table; nothing borrows
                        // it (`&mut self`). The index keeps the old score
                        // itself, so it finds the member by it.
                        unsafe { self.table.set_score(entry, score) };
                        self.index.rescore(old, score, &EntryRef::new(entry));
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
                    if let Ok(entry) = self.table.insert_vacant(at, member, score) {
                        self.index.insert(score, EntryRef::new(entry));
                    }
                }
                self.debug_check();
                out
            }
        }
    }

    /// Remove a member; true if it was one.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        let index = &mut self.index;
        let removed = self
            .table
            .remove_scored(member, |entry| {
                // SAFETY: the entry is out of the table but not yet freed.
                let score = unsafe { scored_score(entry.as_ptr()) };
                // The index's reference goes before the entry does.
                let held = index.remove(score, &EntryRef::new(entry));
                debug_assert!(held.is_some(), "a member missing from the index");
            })
            .is_some();
        self.debug_check();
        removed
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
        let n = doomed.len();
        for (score, at) in doomed {
            // The others still listed are live: an entry is freed only
            // once its member is out of the index.
            {
                let held = self.index.remove(score, &EntryRef::new(at));
                debug_assert!(held.is_some(), "a member missing from the index");
            }
            // SAFETY: a live entry of the table, out of the index.
            unsafe { self.table.remove_entry(at) };
        }
        self.debug_check();
        n
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

    /// Everything the set holds but its own box: the table exactly
    /// ([`CollectionTable::allocation_size`]) and the index's estimate.
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
pub struct Builder<O: ScoreOrder = BTreeOrder, A: Memory = EntryPool> {
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
        let n = if cfg!(miri) { 300 } else { 50_000 };
        let mut large = Large::<BTreeOrder>::with_capacity(0);
        let addr = |large: &Large, i: usize| {
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
        crate::collection_table::set_test_max_len(Some(8));
        let mut builder = Builder::<BTreeOrder>::with_capacity(2);
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
}
