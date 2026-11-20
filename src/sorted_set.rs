//! A sorted set's members and scores, in one of two forms, as Redis keeps a
//! sorted set as a listpack or as a skiplist:
//!
//! - **Packed.** A set of no more than `zset-max-listpack-entries` members
//!   (128), none longer than `zset-max-listpack-value` bytes (64), is one
//!   block ([`PackedZset`]): its entries in order, by score and then by
//!   member, a fingerprint a member to find one by name. A new set is an
//!   empty block, which allocates nothing.
//! - **Large.** Past either limit, a [`CollectionTable`] of members, one
//!   pool block a member holding its score and its bytes, and an index of
//!   their order that holds each member as its block's address
//!   ([`zset_table`](crate::zset_table)): rank, ranges and lookups by name
//!   for sets of any size. A score is rewritten in place. The table grows
//!   and shrinks a few members a write (and the rest in the background, as
//!   a large set's or hash's: [`after_write`]), gives `ZSCAN` its cursor,
//!   and a member at random in O(1) (`ZRANDMEMBER`).
//!
//! A set moves from the block to the large form, its entries in order in
//! one pass, when a write would take it past a limit, and never back (as
//! in Redis: a set that has been large is kept large, whatever is taken out
//! of it). The limits are read at each write that adds a member, so a
//! `CONFIG SET` takes effect at a set's next new member. A set built whole
//! (a load, `RESTORE`, a `*STORE` command's result) is built in the form its
//! size and members call for, the large form's index in one go.
//!
//! Every reader goes through the methods here, so nothing outside knows the
//! form: what is written to disk and to replicas (members and scores, in
//! order) is the same either way.

use crate::collection_table::{CollectionTable, Scores};
use crate::member_set::Member;
use crate::packed_zset::{self, PackedZset, Score};
use crate::zset_table::{Builder, Large, Probe};
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

/// A member name stored as a cheaply-cloneable Arc'd byte slice: what
/// callers hand in and get back for a member they keep.
pub type ZMember = Arc<[u8]>;

/// `zset-max-listpack-entries` until `CONFIG SET` or the configuration file
/// gives another: Redis's, and the most a block holds
/// ([`packed_zset::MAX_ENTRIES`]).
pub const DEFAULT_PACKED_ENTRIES: usize = packed_zset::MAX_ENTRIES;
/// `zset-max-listpack-value` until set otherwise: Redis's, and the longest
/// member a block holds ([`packed_zset::MAX_LEN`]).
pub const DEFAULT_PACKED_VALUE: usize = packed_zset::MAX_LEN;

/// The most members a sorted set keeps in one block, as
/// `zset-max-listpack-entries` says. 0: no blocks.
static PACKED_ENTRIES: AtomicUsize = AtomicUsize::new(DEFAULT_PACKED_ENTRIES);
/// The longest member a sorted set keeps in one block, as
/// `zset-max-listpack-value` says.
static PACKED_VALUE: AtomicUsize = AtomicUsize::new(DEFAULT_PACKED_VALUE);

#[cfg(test)]
thread_local! {
    /// Limits of a test's own, on its thread only: tests run side by side,
    /// and the settings are the process's.
    static OWN_LIMITS: std::cell::Cell<Option<[usize; 2]>> = const { std::cell::Cell::new(None) };
}

/// The limits on this thread alone, for a test: `None` puts the settings
/// back.
#[cfg(test)]
pub(crate) fn own_limits(limits: Option<[usize; 2]>) {
    OWN_LIMITS.with(|own| own.set(limits));
}

#[cfg(test)]
fn own_limit(which: usize) -> Option<usize> {
    OWN_LIMITS.with(|own| own.get()).map(|limits| limits[which])
}

#[inline]
pub fn packed_entries() -> usize {
    #[cfg(test)]
    if let Some(limit) = own_limit(0) {
        return limit;
    }
    PACKED_ENTRIES.load(Ordering::Relaxed)
}

/// `zset-max-listpack-entries`: no more than a block holds. Returns what
/// was kept, for `CONFIG GET` to report. Takes effect at a set's next new
/// member.
pub fn set_packed_entries(limit: usize) -> usize {
    let limit = limit.min(packed_zset::MAX_ENTRIES);
    PACKED_ENTRIES.store(limit, Ordering::Relaxed);
    limit
}

#[inline]
pub fn packed_value() -> usize {
    #[cfg(test)]
    if let Some(limit) = own_limit(1) {
        return limit;
    }
    PACKED_VALUE.load(Ordering::Relaxed)
}

/// `zset-max-listpack-value`: no longer than a block holds. Returns what
/// was kept, for `CONFIG GET` to report.
pub fn set_packed_value(limit: usize) -> usize {
    let limit = limit.min(packed_zset::MAX_LEN);
    PACKED_VALUE.store(limit, Ordering::Relaxed);
    limit
}

/// Whether `n` members, the longest `longest` bytes, may be one block.
#[inline]
fn fits(n: usize, longest: usize) -> bool {
    n <= packed_entries() && longest <= packed_value()
}

/// What a sorted set is kept as, by Redis's names for its encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    Listpack,
    Skiplist,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Encoding::Listpack => "listpack",
            Encoding::Skiplist => "skiplist",
        }
    }
}

/// The inner data for a Sorted Set key: see the module's notes.
#[derive(Clone)]
pub struct SortedSetInner {
    repr: Repr,
}

#[derive(Clone)]
enum Repr {
    Packed(PackedZset),
    /// Behind a box: a set's handle (`Arc<RwLock<SortedSetInner>>`) is in
    /// every key that holds one, and a block is a fraction of the size of
    /// the table's and the index's headers.
    Large(Box<Large>),
}

/// Kept inline in every sorted set's shared block: a block's handle, no
/// more.
const _: () = assert!(std::mem::size_of::<SortedSetInner>() <= 32);

/// A bound of a range of `(score, member)` keys, as the large form's index
/// takes it.
#[inline]
fn probe(bound: Bound<&(OrderedFloat<f64>, ZMember)>) -> Bound<Probe<'_>> {
    bound.map(|(score, member)| Probe {
        score: score.0,
        member,
    })
}

impl SortedSetInner {
    pub fn new() -> Self {
        Self {
            repr: Repr::Packed(PackedZset::new()),
        }
    }

    /// The form the set is kept in.
    #[inline]
    pub fn encoding(&self) -> Encoding {
        match self.repr {
            Repr::Packed(_) => Encoding::Listpack,
            Repr::Large(_) => Encoding::Skiplist,
        }
    }

    /// Whether the set is one block.
    #[inline]
    pub fn is_packed(&self) -> bool {
        matches!(self.repr, Repr::Packed(_))
    }

    /// The block's bytes, if the set is one block: its one allocation,
    /// room to spare included.
    #[inline]
    pub fn packed_bytes(&self) -> Option<usize> {
        match &self.repr {
            Repr::Packed(block) => Some(block.allocation_size()),
            Repr::Large(_) => None,
        }
    }

    /// The bytes of the large form's own allocation (the box holding the
    /// table and the index, without their contents).
    pub const LARGE_HEADER: usize = std::mem::size_of::<Large>();

    /// Everything the set holds but its own handle, in O(1): a block's one
    /// allocation, room to spare included; for the large form its box, the
    /// table exactly as `used_memory` counts it (group arrays, overflow
    /// groups, the members' entries at their pool class's size) and the
    /// index's estimate (`memory_layout::score_index`). For `MEMORY USAGE`.
    pub fn allocation_size(&self) -> usize {
        match &self.repr {
            Repr::Packed(block) => block.allocation_size(),
            Repr::Large(large) => Self::LARGE_HEADER + large.allocation_size(),
        }
    }

    /// How many allocations freeing the set frees, as Redis counts them for
    /// lazy freeing: a block is one.
    pub fn free_effort(&self) -> usize {
        match &self.repr {
            Repr::Packed(_) => 1,
            Repr::Large(large) => large.len(),
        }
    }

    /// Bytes that freeing the set gives back, at least (for eviction): a
    /// block's, exactly; the large form's box and table exactly (the index,
    /// which is estimated, not counted: `per_member` is kept for the callers
    /// that pass one for every type).
    pub fn freeing_at_least(&self, _per_member: usize) -> usize {
        match &self.repr {
            Repr::Packed(block) => block.allocation_size(),
            Repr::Large(large) => Self::LARGE_HEADER + large.table_bytes(),
        }
    }

    /// The large form, from the block, in one pass in order, the table
    /// sized for the block's members (a command that adds more grows it as
    /// they come: a table is never sized for a command's arguments, which
    /// may name a member many times).
    #[cold]
    #[inline(never)]
    fn make_large(&mut self) {
        let Repr::Packed(block) = &self.repr else {
            return;
        };
        let mut built = Builder::with_capacity(block.len());
        for (member, score) in block.iter() {
            // A block's members are short, different and in order.
            let _ = built.push(member, score);
        }
        self.repr = Repr::Large(Box::new(built.finish()));
    }

    #[inline]
    fn large_mut(&mut self) -> &mut Large {
        match &mut self.repr {
            Repr::Large(large) => large,
            Repr::Packed(_) => unreachable!("the set is one block"),
        }
    }

    /// Before `elements` pairs of a command are written: a block moves to
    /// the large form now if they are more than it may hold, as Redis's
    /// `ZADD` judges by the command before it adds anything (members already
    /// there, or given twice, included). A long member moves it only if it
    /// is added (`ZADD XX` of one that is not there leaves Redis's listpack
    /// as it is).
    pub fn expect(&mut self, elements: usize) {
        if self.is_packed() && elements > packed_entries() {
            self.make_large();
        }
    }

    /// Insert or update a member. Returns true if the member is new.
    ///
    /// A score of -0 is kept as 0, as Redis keeps it in the listpack that
    /// holds a sorted set of up to 128 members: `ZADD z -0 m` then
    /// `ZSCORE z m` is `0` (#422).
    pub fn insert(&mut self, member: ZMember, score: f64) -> bool {
        self.insert_bytes(&member, score)
    }

    /// [`insert`](Self::insert) of a member's bytes, which are copied in.
    pub fn insert_bytes(&mut self, member: &[u8], score: f64) -> bool {
        self.upsert(member, |old| (Some(score), old.is_none()))
    }

    /// Look `member` up and give it a score, in one lookup: `decide` is
    /// given the member's score, if it is a member, and says what its score
    /// is to be (`None`: as it is, or not added), and what to return. A new
    /// member that does not fit the block moves the set to the large form.
    #[inline]
    pub fn upsert<R>(
        &mut self,
        member: &[u8],
        decide: impl FnOnce(Option<f64>) -> (Option<f64>, R),
    ) -> R {
        match &mut self.repr {
            Repr::Packed(block) => match block.locate(member) {
                Ok(i) => {
                    let old = block.score(i);
                    let (score, out) = decide(Some(old));
                    if let Some(score) = score {
                        let score = score + 0.0;
                        if score.to_bits() != old.to_bits() {
                            block.set_score(i, score);
                        }
                    }
                    out
                }
                Err(f) => {
                    let (score, out) = decide(None);
                    if let Some(score) = score {
                        let score = score + 0.0;
                        if fits(block.len() + 1, member.len()) {
                            let at = block.lower_bound(score, member, false);
                            block.insert_new(at, member, &Score::new(score), f);
                        } else {
                            self.make_large();
                            self.large_mut().insert(member, score);
                        }
                    }
                    out
                }
            },
            Repr::Large(large) => large.upsert(member, decide),
        }
    }

    /// Remove a member. Returns true if it existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match &mut self.repr {
            Repr::Packed(block) => match block.find(member) {
                Some(i) => {
                    block.remove_at(i);
                    true
                }
                None => false,
            },
            Repr::Large(large) => large.remove(member),
        }
    }

    /// Removes and returns the member with the lowest score.
    pub fn pop_min(&mut self) -> Option<(ZMember, f64)> {
        self.pop_with(false, |member, score| (Arc::from(member), score))
    }

    /// Removes and returns the member with the highest score.
    pub fn pop_max(&mut self) -> Option<(ZMember, f64)> {
        self.pop_with(true, |member, score| (Arc::from(member), score))
    }

    /// Removes the member with the lowest score (`max`: the highest), and
    /// hands it to `take` before it goes: it is not copied.
    #[inline]
    pub fn pop_with<R>(&mut self, max: bool, take: impl FnOnce(&[u8], f64) -> R) -> Option<R> {
        match &mut self.repr {
            Repr::Packed(block) => {
                if block.is_empty() {
                    return None;
                }
                let at = if max { block.len() - 1 } else { 0 };
                let (member, score) = block.get(at);
                let out = take(member, score);
                block.remove_at(at);
                Some(out)
            }
            Repr::Large(large) => large.pop_with(max, take),
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match &self.repr {
            Repr::Packed(block) => block.find(member).map(|i| block.score(i)),
            Repr::Large(large) => large.score(member),
        }
    }

    /// The member's 0-based rank by ascending score, and its score.
    pub fn rank(&self, member: &[u8]) -> Option<(usize, f64)> {
        match &self.repr {
            Repr::Packed(block) => block.find(member).map(|i| (i, block.score(i))),
            Repr::Large(large) => large.rank(member),
        }
    }

    /// Members and scores, ordered by score, then by member.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (Member<'_>, f64)> {
        match &self.repr {
            Repr::Packed(block) => Either::A(Packed(block.iter())),
            Repr::Large(large) => Either::B(
                large
                    .iter()
                    .map(|(member, score)| (Member::Bytes(member), score)),
            ),
        }
    }

    /// Like `iter`, limited to ranks `start` to `stop`, inclusive.
    pub fn range_by_rank(
        &self,
        start: usize,
        stop: usize,
    ) -> impl DoubleEndedIterator<Item = (Member<'_>, f64)> {
        let len = self.len();
        let end = stop.saturating_add(1).min(len);
        match &self.repr {
            Repr::Packed(block) => Either::A(Packed(block.range(start.min(end), end))),
            Repr::Large(large) => Either::B(
                large
                    .range_by_rank(start.min(end), end)
                    .map(|(member, score)| (Member::Bytes(member), score)),
            ),
        }
    }

    /// Members and scores in no particular order.
    pub fn iter_unordered(&self) -> impl Iterator<Item = (Member<'_>, f64)> {
        match &self.repr {
            Repr::Packed(block) => Either::A(Packed(block.iter())),
            Repr::Large(large) => Either::B(
                large
                    .iter_unordered()
                    .map(|(member, score)| (Member::Bytes(member), score)),
            ),
        }
    }

    /// The table of the large form, for `ZSCAN`'s cursor (`table_scan`);
    /// `None` for a block, which `ZSCAN` returns whole.
    #[inline]
    pub fn table(&self) -> Option<&CollectionTable<Scores>> {
        match &self.repr {
            Repr::Large(large) => Some(large.table()),
            Repr::Packed(_) => None,
        }
    }

    /// Like `iter`, limited to the given range of (score, member) keys.
    pub fn range_by_score<R>(&self, range: R) -> impl DoubleEndedIterator<Item = (Member<'_>, f64)>
    where
        R: RangeBounds<(OrderedFloat<f64>, ZMember)>,
    {
        match &self.repr {
            Repr::Packed(block) => {
                let (from, to) = Self::packed_bounds(block, &range);
                Either::A(Packed(block.range(from, to.max(from))))
            }
            Repr::Large(large) => Either::B(
                large
                    .range(probe(range.start_bound()), probe(range.end_bound()))
                    .map(|(member, score)| (Member::Bytes(member), score)),
            ),
        }
    }

    /// The indexes of a block that a range of keys covers: `from..to`.
    fn packed_bounds<R>(block: &PackedZset, range: &R) -> (usize, usize)
    where
        R: RangeBounds<(OrderedFloat<f64>, ZMember)>,
    {
        let from = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included((score, member)) => block.lower_bound(score.0, member, false),
            Bound::Excluded((score, member)) => block.lower_bound(score.0, member, true),
        };
        let to = match range.end_bound() {
            Bound::Unbounded => block.len(),
            Bound::Included((score, member)) => block.lower_bound(score.0, member, true),
            Bound::Excluded((score, member)) => block.lower_bound(score.0, member, false),
        };
        (from, to)
    }

    /// A member and its score, any one as likely as another (`ZRANDMEMBER`):
    /// a block's by its index, the large form's by a random slot of its
    /// table. Neither depends on how many members there are.
    pub fn random(&self, rng: &mut impl rand::Rng) -> Option<(Member<'_>, f64)> {
        match &self.repr {
            Repr::Packed(block) if block.is_empty() => None,
            Repr::Packed(block) => {
                let (member, score) = block.get(rng.gen_range(0..block.len()));
                Some((Member::Bytes(member), score))
            }
            Repr::Large(large) => large
                .random(rng)
                .map(|(member, score, _)| (Member::Bytes(member), score)),
        }
    }

    /// `count` different members and their scores, picked at random, in a
    /// random order (`ZRANDMEMBER` with a positive count); all of them if
    /// `count` is the size of the set or more, but then in no order that
    /// was picked.
    ///
    /// A few of many (no more than a third) are picked a member at a time
    /// from the table, a member that comes up twice (told apart by its
    /// entry's address) picked again: the expected picks are at most 1.5 a
    /// member. More are found by one walk of the table to the last of the
    /// positions chosen, which is then no more work than the answer is
    /// long. A block is asked for the chosen places only. Every way gives
    /// each set of `count` members, in each order, alike.
    pub fn random_distinct(
        &self,
        count: usize,
        rng: &mut impl rand::Rng,
    ) -> Vec<(Member<'_>, f64)> {
        let len = self.len();
        let count = count.min(len);
        if let Repr::Large(large) = &self.repr
            && count * 3 <= len
        {
            let mut seen: ahash::AHashSet<usize> = ahash::AHashSet::with_capacity(count);
            let mut picked = Vec::with_capacity(count);
            while picked.len() < count {
                let Some((member, score, at)) = large.random(rng) else {
                    break;
                };
                if seen.insert(at) {
                    picked.push((Member::Bytes(member), score));
                }
            }
            return picked;
        }
        self.members_at(&rand::seq::index::sample(rng, len, count).into_vec())
    }

    /// `count` members picked at random, each on its own (so they may
    /// repeat), handed to `f` in the order picked (`ZRANDMEMBER` with a
    /// negative count): a block's by index, the large form's by a random
    /// slot of its table each. A call costs the count, whatever the size.
    pub fn random_each(
        &self,
        count: usize,
        rng: &mut impl rand::Rng,
        mut f: impl FnMut(Member<'_>, f64),
    ) {
        let len = self.len();
        if len == 0 {
            return;
        }
        match &self.repr {
            Repr::Packed(block) => {
                for _ in 0..count {
                    let (member, score) = block.get(rng.gen_range(0..len));
                    f(Member::Bytes(member), score);
                }
            }
            Repr::Large(large) => {
                for _ in 0..count {
                    let Some((member, score, _)) = large.random(rng) else {
                        return;
                    };
                    f(Member::Bytes(member), score);
                }
            }
        }
    }

    /// After a command has written `written` members: a table left under
    /// an eighth full (by a reservation for more than came) starts to shrink,
    /// a few members a write as any resize. The table is never reserved for
    /// a command's arguments, so after `ZADD` or `GEOADD` this is a check.
    pub fn settle(&mut self, _written: usize) {
        if let Repr::Large(large) = &mut self.repr {
            large.shrink_if_sparse();
        }
    }

    /// Members at positions of `iter_unordered`, in the order given; may repeat.
    fn members_at(&self, picks: &[usize]) -> Vec<(Member<'_>, f64)> {
        let large = match &self.repr {
            Repr::Packed(block) => {
                return picks
                    .iter()
                    .filter(|&&at| at < block.len())
                    .map(|&at| {
                        let (member, score) = block.get(at);
                        (Member::Bytes(member), score)
                    })
                    .collect();
            }
            Repr::Large(large) => large,
        };
        let mut slots: Vec<usize> = (0..picks.len()).collect();
        slots.sort_unstable_by_key(|&slot| picks[slot]);
        let mut out: Vec<Option<(&[u8], f64)>> = vec![None; picks.len()];
        // One walk, up to the highest position picked.
        let mut walk = large.iter_unordered();
        let mut at: Option<usize> = None;
        let mut last: Option<(&[u8], f64)> = None;
        for slot in slots {
            let want = picks[slot];
            if at != Some(want) {
                let skip = match at {
                    Some(at) => want - at - 1,
                    None => want,
                };
                last = walk.nth(skip);
                at = Some(want);
            }
            out[slot] = last;
        }
        out.into_iter()
            .flatten()
            .map(|(member, score)| (Member::Bytes(member), score))
            .collect()
    }

    /// Take out the members of ranks `start` to `stop`, inclusive (both
    /// within the set); how many went.
    pub fn remove_ranks(&mut self, start: usize, stop: usize) -> usize {
        debug_assert!(start <= stop && stop < self.len());
        match &mut self.repr {
            Repr::Packed(block) => {
                block.remove_range(start, stop + 1);
                stop + 1 - start
            }
            Repr::Large(large) => large.remove_ranks(start, stop + 1),
        }
    }

    /// Take out the members whose keys are in `range`; how many went.
    pub fn remove_range_by_score<R>(&mut self, range: R) -> usize
    where
        R: RangeBounds<(OrderedFloat<f64>, ZMember)>,
    {
        match &mut self.repr {
            Repr::Packed(block) => {
                let (from, to) = Self::packed_bounds(block, &range);
                let to = to.max(from);
                block.remove_range(from, to);
                to - from
            }
            Repr::Large(large) => {
                large.remove_range(probe(range.start_bound()), probe(range.end_bound()))
            }
        }
    }

    /// Take out the members `gone` says to; how many went.
    pub fn remove_where(&mut self, gone: impl FnMut(&[u8], f64) -> bool) -> usize {
        match &mut self.repr {
            Repr::Packed(block) => {
                let mut gone = gone;
                block.retain(|member, score| !gone(member, score))
            }
            Repr::Large(large) => large.remove_where(gone),
        }
    }

    /// Give back a block's room to spare (a set just made whole).
    pub fn shrink_to_fit(&mut self) {
        if let Repr::Packed(block) = &mut self.repr {
            block.shrink_to_fit();
        }
    }

    /// Members the member table holds before it grows; a block's members.
    pub fn capacity(&self) -> usize {
        match &self.repr {
            Repr::Packed(block) => block.len(),
            Repr::Large(large) => large.capacity(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Packed(block) => block.len(),
            Repr::Large(large) => large.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A builder of a set of about `count` members, given one at a time
    /// (a load, `RESTORE`, a command's result): a block if they fit one, the
    /// large form if not, its table sized for them (up to 2^16 at first: a
    /// count read from a file is not trusted further, and the table grows
    /// as they come) and its index built in one go at the end.
    pub fn loader(count: usize) -> Loader {
        if count > packed_entries() {
            Loader(Building::Large(Box::new(Builder::with_capacity(
                count.min(LOADER_RESERVE),
            ))))
        } else {
            Loader(Building::Packed {
                bytes: Vec::new(),
                entries: Vec::with_capacity(count),
            })
        }
    }

    /// A set of `pairs`, in any order; a member given twice has the score
    /// it was given last.
    pub fn from_pairs<M: AsRef<[u8]>>(pairs: impl IntoIterator<Item = (M, f64)>) -> Self {
        let pairs = pairs.into_iter();
        let mut loader = Self::loader(pairs.size_hint().0);
        for (member, score) in pairs {
            loader.push(member.as_ref(), score);
        }
        loader.finish()
    }
}

/// The most members [`SortedSetInner::loader`] reserves room for before
/// any is given.
const LOADER_RESERVE: usize = 1 << 16;

/// See [`SortedSetInner::loader`].
pub struct Loader(Building);

enum Building {
    /// The members so far, end to end in `bytes`; for each, where it ends
    /// and its score.
    Packed {
        bytes: Vec<u8>,
        entries: Vec<(u32, f64)>,
    },
    /// The table filled as the members come, the index built at the end.
    Large(Box<Builder>),
}

impl Loader {
    /// Add a member a command gives (at most 512 MB, which the table
    /// always holds).
    pub fn push(&mut self, member: &[u8], score: f64) {
        let _ = self.push_loaded(member, score);
    }

    /// Add a member read from a snapshot, the AOF's base, a replication
    /// stream or a `RESTORE` payload. A member too long for the table
    /// ([`crate::collection_table::MAX_LEN`]) is an error, never dropped:
    /// the load fails.
    pub fn push_loaded(&mut self, member: &[u8], score: f64) -> std::io::Result<()> {
        // -0 + 0 is 0; any other score is itself.
        let score = score + 0.0;
        match &mut self.0 {
            Building::Packed { bytes, entries } => {
                // A member given twice counts once, so the count is known
                // only at the end: past twice the limit, it is past it.
                if member.len() > packed_value() || entries.len() >= 2 * packed_entries() {
                    // Past what a block holds: the large form, with those
                    // given so far, then this one.
                    let mut built = Self::large_of(bytes, entries, entries.len() + 1);
                    built.push(member, score).map_err(too_long)?;
                    self.0 = Building::Large(built);
                } else {
                    bytes.extend_from_slice(member);
                    entries.push((bytes.len() as u32, score));
                }
            }
            Building::Large(built) => built.push(member, score).map_err(too_long)?,
        }
        Ok(())
    }

    /// The large form's builder, with the members given so far, in the
    /// order given (each short).
    fn large_of(bytes: &[u8], entries: &[(u32, f64)], room: usize) -> Box<Builder> {
        let mut built = Box::new(Builder::with_capacity(room.min(LOADER_RESERVE)));
        let mut from = 0;
        for &(end, score) in entries {
            let end = end as usize;
            let _ = built.push(&bytes[from..end], score);
            from = end;
        }
        built
    }

    pub fn finish(self) -> SortedSetInner {
        match self.0 {
            Building::Large(built) => SortedSetInner {
                repr: Repr::Large(Box::new(built.finish())),
            },
            Building::Packed { bytes, entries } => {
                let mut pairs: Vec<(&[u8], f64)> = Vec::with_capacity(entries.len());
                let mut from = 0;
                for &(end, score) in &entries {
                    let end = end as usize;
                    pairs.push((&bytes[from..end], score));
                    from = end;
                }
                let order = |a: &(&[u8], f64), b: &(&[u8], f64)| {
                    OrderedFloat(a.1)
                        .cmp(&OrderedFloat(b.1))
                        .then_with(|| a.0.cmp(b.0))
                };
                // In order, as a snapshot gives them: laid down as they
                // come, and the block asked whether a member came twice (in
                // order by score, two of one member need not be together).
                let in_order = pairs
                    .windows(2)
                    .all(|w| order(&w[0], &w[1]) == std::cmp::Ordering::Less);
                if in_order && pairs.len() <= packed_entries() {
                    let block = PackedZset::from_sorted(&pairs);
                    if !block.has_twice() {
                        return SortedSetInner {
                            repr: Repr::Packed(block),
                        };
                    }
                }
                {
                    // By member, the last given of each kept (a stable sort
                    // keeps those given twice in the order given), then in
                    // the set's order.
                    pairs.sort_by(|a, b| a.0.cmp(b.0));
                    let mut kept: Vec<(&[u8], f64)> = Vec::with_capacity(pairs.len());
                    for pair in pairs {
                        match kept.last_mut() {
                            Some(last) if last.0 == pair.0 => *last = pair,
                            _ => kept.push(pair),
                        }
                    }
                    kept.sort_by(order);
                    pairs = kept;
                }
                if pairs.len() > packed_entries() {
                    // Sized for the members that stay, laid down in order.
                    let mut built = Builder::with_capacity(pairs.len());
                    for (member, score) in pairs {
                        let _ = built.push(member, score);
                    }
                    return SortedSetInner {
                        repr: Repr::Large(Box::new(built.finish())),
                    };
                }
                SortedSetInner {
                    repr: Repr::Packed(PackedZset::from_sorted(&pairs)),
                }
            }
        }
    }
}

#[cold]
fn too_long(_: crate::collection_table::TooLong) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "a sorted set member longer than the {} bytes a sorted set holds",
            crate::collection_table::MAX_LEN
        ),
    )
}

impl Default for SortedSetInner {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SortedSetInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.iter()
                    .map(|(member, score)| (String::from_utf8_lossy(&member).into_owned(), score)),
            )
            .finish()
    }
}

// ── the background's part ─────────────────────────────────────────────────────
//
// See `resize_background`: a write that leaves its set's table mid-resize, or
// with a finished resize's old array kept back, hands the set over, and the
// active-expire thread finishes it a step at a time under the set's lock. A
// resize moves the table's slot words, never an entry, so the index's
// references stay good throughout.

type Shared = Arc<RwLock<SortedSetInner>>;

impl SortedSetInner {
    /// Whether the background has work on this set (a resize to finish, or
    /// a finished one's array to free) that it has not been handed yet; if
    /// so, it is now marked handed. Near `maxmemory` the work is done here
    /// instead ([`Self::settle_if_tight`]) and nothing is handed over.
    #[inline]
    fn hand_over(&mut self) -> bool {
        let Repr::Large(large) = &mut self.repr else {
            return false;
        };
        if !(large.resizing() || large.holds_retired()) {
            return false;
        }
        if self.settle_if_tight() {
            return false;
        }
        let Repr::Large(large) = &mut self.repr else {
            return false;
        };
        let due = !large.queued;
        large.queued = true;
        due
    }

    /// Near `maxmemory`, finish a resize in progress and free the old
    /// arrays now, on this thread: the arrays a resize holds are there only
    /// until it ends, and must never be what makes the server refuse a
    /// write or evict a key. Returns whether it did. See
    /// `resize_background::tight`.
    #[cold]
    fn settle_if_tight(&mut self) -> bool {
        let Repr::Large(large) = &mut self.repr else {
            return false;
        };
        if !crate::resize_background::tight(large.transient_bytes()) {
            return false;
        }
        while !large.step_resize(usize::MAX) {}
        drop(large.take_retired());
        true
    }

    /// One step of the background's work on the set: about `budget` units
    /// of a resize in progress. Returns whether the set has no more for it,
    /// and the array a finished resize left, to be freed elsewhere.
    fn background_step(
        &mut self,
        budget: usize,
    ) -> (bool, Option<crate::collection_table::Retired>) {
        let Repr::Large(large) = &mut self.repr else {
            return (true, None);
        };
        let finished = large.step_resize(budget);
        let retired = large.take_retired();
        if finished {
            large.queued = false;
        }
        (finished, retired)
    }

    /// Whether the set is handed to the background (tests).
    #[cfg(test)]
    pub(crate) fn is_queued(&self) -> bool {
        matches!(&self.repr, Repr::Large(large) if large.queued)
    }

    /// Finish whatever resize is in progress now, and free what it leaves
    /// (tests).
    #[cfg(test)]
    pub(crate) fn finish_resize(&mut self) {
        while !self.background_step(usize::MAX).0 {}
    }
}

impl crate::resize_background::Resizable for RwLock<SortedSetInner> {
    fn try_step(&self, budget: usize) -> Option<(bool, Option<crate::collection_table::Retired>)> {
        Some(self.try_write()?.background_step(budget))
    }

    fn try_settle(&self) -> Option<bool> {
        let mut inner = self.try_write()?;
        let Repr::Large(large) = &mut inner.repr else {
            return Some(false);
        };
        let released = large.transient_bytes() > 0;
        while !large.step_resize(usize::MAX) {}
        drop(large.take_retired());
        large.queued = false;
        Some(released)
    }

    fn let_go(self: Arc<Self>) -> bool {
        match Arc::into_inner(self) {
            Some(last) => {
                crate::lazyfree::free_later(last, 0);
                true
            }
            None => false,
        }
    }
}

/// After a write to `set` (whose write lock `inner` is): hands it to the
/// background if its table has work there. A few instructions when not.
#[inline]
pub fn after_write(set: &Shared, inner: &mut SortedSetInner) {
    if inner.hand_over() {
        hand(set);
    }
}

#[cold]
fn hand(set: &Shared) {
    let weak: Weak<RwLock<SortedSetInner>> = Arc::downgrade(set);
    crate::resize_background::hand(weak);
}

/// `set` made a key's value: shared, and handed to the background if a
/// resize it went through while it was built is still in progress. What
/// every sorted set made whole goes through (a new key of `ZADD`, `ZINCRBY`
/// or `GEOADD`, a load, `RESTORE`, `COPY`, a `*STORE` command's result).
///
/// A new `Arc` is never in the background's registry, so a mark of having
/// been handed over (from a set moved out of another value) is cleared
/// first.
pub fn shared(mut set: SortedSetInner) -> Shared {
    if let Repr::Large(large) = &mut set.repr {
        large.queued = false;
    }
    let due = set.hand_over();
    let shared = Arc::new(RwLock::new(set));
    if due {
        hand(&shared);
    }
    shared
}

/// A block's entries as a sorted set hands them out.
struct Packed<'a>(packed_zset::Range<'a>);

impl<'a> Iterator for Packed<'a> {
    type Item = (Member<'a>, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(member, score)| (Member::Bytes(member), score))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.0
            .nth(n)
            .map(|(member, score)| (Member::Bytes(member), score))
    }

    #[inline]
    fn count(self) -> usize {
        self.0.count()
    }
}

impl DoubleEndedIterator for Packed<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|(member, score)| (Member::Bytes(member), score))
    }

    #[inline]
    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.0
            .nth_back(n)
            .map(|(member, score)| (Member::Bytes(member), score))
    }
}

impl ExactSizeIterator for Packed<'_> {}

/// One of two iterators of the same items: a block's, or the large form's.
enum Either<A, B> {
    A(A),
    B(B),
}

impl<T, A: Iterator<Item = T>, B: Iterator<Item = T>> Iterator for Either<A, B> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self {
            Either::A(a) => a.next(),
            Either::B(b) => b.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Either::A(a) => a.size_hint(),
            Either::B(b) => b.size_hint(),
        }
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        match self {
            Either::A(a) => a.nth(n),
            Either::B(b) => b.nth(n),
        }
    }

    #[inline]
    fn count(self) -> usize {
        match self {
            Either::A(a) => a.count(),
            Either::B(b) => b.count(),
        }
    }
}

impl<T, A: DoubleEndedIterator<Item = T>, B: DoubleEndedIterator<Item = T>> DoubleEndedIterator
    for Either<A, B>
{
    #[inline]
    fn next_back(&mut self) -> Option<T> {
        match self {
            Either::A(a) => a.next_back(),
            Either::B(b) => b.next_back(),
        }
    }

    #[inline]
    fn nth_back(&mut self, n: usize) -> Option<T> {
        match self {
            Either::A(a) => a.nth_back(n),
            Either::B(b) => b.nth_back(n),
        }
    }
}

impl<T, A: ExactSizeIterator<Item = T>, B: ExactSizeIterator<Item = T>> ExactSizeIterator
    for Either<A, B>
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resize_background::step_background;
    use std::collections::{BTreeSet, HashMap};

    fn m(name: &[u8]) -> ZMember {
        Arc::from(name)
    }

    fn large(zs: &SortedSetInner) -> &Large {
        match &zs.repr {
            Repr::Large(large) => large,
            Repr::Packed(_) => panic!("packed"),
        }
    }

    fn made_large() -> SortedSetInner {
        let mut zs = SortedSetInner::new();
        zs.make_large();
        zs
    }

    /// The table and the index hold exactly the same members, the index's
    /// references the table's entries (`Large::check`).
    fn assert_in_sync(zs: &SortedSetInner) {
        large(zs).check();
    }

    fn order(zs: &SortedSetInner) -> Vec<(Vec<u8>, f64)> {
        zs.iter().map(|(m, s)| (m.to_vec(), s)).collect()
    }

    fn pairs(list: &[(&[u8], f64)]) -> Vec<(Vec<u8>, f64)> {
        list.iter().map(|(m, s)| (m.to_vec(), *s)).collect()
    }

    #[test]
    fn remove_keeps_both_indexes_in_sync() {
        let mut zs = made_large();
        assert!(!zs.remove(b"a"), "empty set");
        assert!(zs.insert(m(b"a"), 1.0));
        assert!(zs.insert(m(b"b"), 2.0));
        assert!(zs.insert(m(b"c"), 2.0)); // ties b: ordered by name
        assert!(zs.insert(m(b"\xff\x00z"), 0.5)); // not UTF-8
        assert_in_sync(&zs);

        assert!(zs.remove(b"b"));
        assert!(!zs.remove(b"b"), "already removed");
        assert!(!zs.remove(b"missing"));
        assert_in_sync(&zs);
        assert_eq!(
            order(&zs),
            pairs(&[(b"\xff\x00z", 0.5), (b"a", 1.0), (b"c", 2.0)])
        );

        assert!(zs.remove(b"\xff\x00z"));
        assert_in_sync(&zs);
        assert_eq!(order(&zs), pairs(&[(b"a", 1.0), (b"c", 2.0)]));
    }

    #[test]
    fn update_remove_and_reinsert_keep_both_indexes_in_sync() {
        let mut zs = made_large();
        zs.insert(m(b"a"), 1.0);
        zs.insert(m(b"b"), 2.0);
        assert!(!zs.insert(m(b"a"), 3.0), "score update is not new");
        assert_in_sync(&zs);
        assert_eq!(order(&zs), pairs(&[(b"b", 2.0), (b"a", 3.0)]));

        assert!(zs.remove(b"a"));
        assert!(zs.insert(m(b"a"), -1.0), "reinsertion is new");
        assert_in_sync(&zs);
        assert_eq!(order(&zs), pairs(&[(b"a", -1.0), (b"b", 2.0)]));

        assert!(zs.remove(b"a"));
        assert!(zs.remove(b"b"));
        assert!(zs.is_empty());
        assert!(large(&zs).is_empty());
    }

    /// A score changed through `upsert` is written into the member's entry
    /// in place: the entry is where it was, and the index finds it at its
    /// new score.
    #[test]
    fn a_score_change_rewrites_the_entry_in_place() {
        let mut zs = made_large();
        zs.insert_bytes(b"a", 1.0);
        zs.insert_bytes(b"b", 1.5);
        let at = |zs: &SortedSetInner| zs.table().unwrap().entry(b"a").unwrap();
        let before = at(&zs);
        zs.upsert(b"a", |_| (Some(2.0), ()));
        assert_eq!(at(&zs), before);
        assert_eq!(zs.score(b"a"), Some(2.0));
        assert_eq!(zs.rank(b"a"), Some((1, 2.0)));
        assert_in_sync(&zs);
    }

    /// The model of a sorted set: (score, member) in order, and each
    /// member's score.
    #[derive(Default)]
    struct Model {
        order: BTreeSet<(OrderedFloat<f64>, Vec<u8>)>,
        scores: HashMap<Vec<u8>, f64>,
    }

    impl Model {
        fn insert(&mut self, member: &[u8], score: f64) -> bool {
            let score = score + 0.0;
            let old = self.scores.insert(member.to_vec(), score);
            if let Some(old) = old {
                self.order.remove(&(OrderedFloat(old), member.to_vec()));
            }
            self.order.insert((OrderedFloat(score), member.to_vec()));
            old.is_none()
        }
        fn remove(&mut self, member: &[u8]) -> bool {
            match self.scores.remove(member) {
                Some(old) => {
                    self.order.remove(&(OrderedFloat(old), member.to_vec()));
                    true
                }
                None => false,
            }
        }
        fn ordered(&self) -> Vec<(Vec<u8>, f64)> {
            self.order.iter().map(|(s, m)| (m.clone(), s.0)).collect()
        }
    }

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

    fn check(zs: &SortedSetInner, model: &Model, step: usize) {
        assert_eq!(zs.len(), model.scores.len(), "step {step}");
        let held = order(zs);
        let want = model.ordered();
        assert_eq!(held, want, "step {step}");
        // Backwards too, ranks, scores, and ranges by rank.
        let back: Vec<(Vec<u8>, f64)> = zs.iter().rev().map(|(m, s)| (m.to_vec(), s)).collect();
        assert!(back.iter().eq(want.iter().rev()), "step {step}");
        for (rank, (member, score)) in want.iter().enumerate() {
            assert_eq!(zs.rank(member), Some((rank, *score)), "step {step}");
            assert_eq!(zs.score(member), Some(*score), "step {step}");
        }
        let n = want.len();
        if n > 0 {
            let (a, b) = (step % n, (step * 7) % n);
            let (start, stop) = (a.min(b), a.max(b));
            let range: Vec<(Vec<u8>, f64)> = zs
                .range_by_rank(start, stop)
                .map(|(m, s)| (m.to_vec(), s))
                .collect();
            assert_eq!(range, want[start..=stop].to_vec(), "step {step}");
            let range: Vec<(Vec<u8>, f64)> = zs
                .range_by_rank(start, stop)
                .rev()
                .map(|(m, s)| (m.to_vec(), s))
                .collect();
            assert!(
                range.iter().eq(want[start..=stop].iter().rev()),
                "step {step}"
            );
        }
        let unordered: BTreeSet<Vec<u8>> = zs.iter_unordered().map(|(m, _)| m.to_vec()).collect();
        assert_eq!(unordered.len(), n);
        // The form: a block as long as nothing has taken it past a limit.
        if let Repr::Packed(block) = &zs.repr {
            block.check();
        } else {
            assert_in_sync(zs);
        }
    }

    /// Every operation, at random, against the model, crossing every
    /// boundary: a block taken past the count and past the member length
    /// (each at random, with the limits changed mid-run), sets emptied and
    /// made again. Contents, order, ranks and scores checked at every step.
    #[test]
    fn it_agrees_with_a_model_through_every_operation() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut became_large = [0usize; 2];
        for round in 0..24 {
            let limits = match round % 4 {
                0 => [128, 64],
                1 => [16, 64],
                2 => [128, 8],
                _ => [5, 3],
            };
            own_limits(Some(limits));
            let mut zs = SortedSetInner::new();
            let mut model = Model::default();
            let space = [20u64, 60, 200, 400][round % 4];
            for step in 0..3_000 {
                let id = rng.below(space);
                let len = match rng.below(20) {
                    0 => 65 + rng.below(20) as usize,
                    1 => 9 + rng.below(8) as usize,
                    _ => 1 + rng.below(8) as usize,
                };
                let mut member = format!("{id}").into_bytes();
                member.resize(len.max(member.len()), b'~');
                if id == 0 {
                    member.clear();
                }
                let score = match rng.below(8) {
                    0 => {
                        [f64::INFINITY, f64::NEG_INFINITY, -0.0, 0.0, 1e300][rng.below(5) as usize]
                    }
                    1 => -(rng.below(100_000) as f64),
                    2 => rng.below(1000) as f64 / 16.0,
                    _ => rng.below(5) as f64,
                };
                let was_packed = zs.is_packed();
                let mut made_again = false;
                match rng.below(14) {
                    0..=4 => {
                        assert_eq!(
                            zs.insert_bytes(&member, score),
                            model.insert(&member, score)
                        );
                    }
                    5 => {
                        assert_eq!(
                            zs.insert(Arc::from(&member[..]), score),
                            model.insert(&member, score)
                        );
                    }
                    6 => {
                        // As ZINCRBY does: inf + -inf is refused.
                        let new = zs.upsert(&member, |old| {
                            let new = old.unwrap_or(0.0) + score;
                            (Some(new).filter(|new| !new.is_nan()), new)
                        });
                        let old = model.scores.get(&member).copied().unwrap_or(0.0);
                        assert_eq!(new.to_bits(), (old + score).to_bits());
                        if !new.is_nan() {
                            model.insert(&member, new);
                        }
                    }
                    7 | 8 => assert_eq!(zs.remove(&member), model.remove(&member)),
                    9 => {
                        let max = rng.below(2) == 0;
                        let got = zs.pop_with(max, |m, s| (m.to_vec(), s));
                        let want = if max {
                            model.ordered().last().cloned()
                        } else {
                            model.ordered().first().cloned()
                        };
                        assert_eq!(got, want);
                        if let Some((m, _)) = want {
                            model.remove(&m);
                        }
                    }
                    10 if !model.scores.is_empty() => {
                        let n = model.scores.len();
                        let a = rng.below(n as u64) as usize;
                        let b = (a + rng.below(4) as usize).min(n - 1);
                        let want = model.ordered();
                        assert_eq!(zs.remove_ranks(a, b), b + 1 - a);
                        for (m, _) in &want[a..=b] {
                            model.remove(m);
                        }
                    }
                    11 => {
                        let lo = rng.below(5) as f64 - 1.0;
                        let hi = lo + rng.below(3) as f64;
                        let least: ZMember = Arc::from(&b""[..]);
                        let range =
                            (OrderedFloat(lo), Arc::clone(&least))..(OrderedFloat(hi), least);
                        let want: Vec<(Vec<u8>, f64)> = model
                            .ordered()
                            .into_iter()
                            .filter(|(_, s)| *s >= lo && *s < hi)
                            .collect();
                        let got: Vec<(Vec<u8>, f64)> = zs
                            .range_by_score(range.clone())
                            .map(|(m, s)| (m.to_vec(), s))
                            .collect();
                        assert_eq!(got, want, "step {step}");
                        assert_eq!(zs.remove_range_by_score(range), want.len());
                        for (m, _) in &want {
                            model.remove(m);
                        }
                    }
                    12 => {
                        let odd = |m: &[u8]| m.last().is_some_and(|b| b % 2 == 1);
                        let want: Vec<Vec<u8>> =
                            model.scores.keys().filter(|m| odd(m)).cloned().collect();
                        assert_eq!(zs.remove_where(|m, _| odd(m)), want.len());
                        for m in &want {
                            model.remove(m);
                        }
                    }
                    _ => {
                        // Emptied, and made again, or a clone carried on.
                        if rng.below(30) == 0 {
                            while zs.pop_with(false, |_, _| ()).is_some() {}
                            model = Model::default();
                            if rng.below(2) == 0 {
                                zs = SortedSetInner::new();
                                made_again = true;
                            }
                        } else {
                            zs = zs.clone();
                        }
                    }
                }
                if was_packed && !zs.is_packed() {
                    // Why it moved: one past the count, or a member past
                    // the length.
                    let too_long = model.scores.keys().any(|m| m.len() > limits[1]);
                    became_large[usize::from(too_long)] += 1;
                    assert!(model.scores.len() > limits[0] || too_long, "step {step}");
                }
                if !was_packed && !made_again {
                    assert!(!zs.is_packed(), "never back");
                }
                // A block holds no more than the limits allow.
                if zs.is_packed() {
                    assert!(zs.len() <= limits[0]);
                    assert!(model.scores.keys().all(|m| m.len() <= limits[1]));
                }
                check(&zs, &model, step);
            }
        }
        own_limits(None);
        assert!(
            became_large[0] > 0 && became_large[1] > 0,
            "{became_large:?}"
        );
    }

    #[test]
    fn the_loader_builds_what_inserts_build() {
        own_limits(Some([128, 64]));
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for round in 0..300 {
            let n = rng.below(300) as usize;
            let mut given: Vec<(Vec<u8>, f64)> = (0..n)
                .map(|_| {
                    let len = if rng.below(50) == 0 {
                        70
                    } else {
                        1 + rng.below(12) as usize
                    };
                    let mut m = format!("{}", rng.below(400)).into_bytes();
                    m.resize(len.max(m.len()), b'.');
                    (m, (rng.below(9) as f64) - 4.0)
                })
                .collect();
            if round % 3 == 0 {
                // As a snapshot gives them: each member once, in order.
                let mut last: HashMap<Vec<u8>, f64> = HashMap::new();
                for (m, s) in &given {
                    last.insert(m.clone(), *s);
                }
                given = last.into_iter().collect();
                given.sort_by(|a, b| {
                    OrderedFloat(a.1)
                        .cmp(&OrderedFloat(b.1))
                        .then(a.0.cmp(&b.0))
                });
            }
            // A count is given with members that are each given once, as
            // a load gives them.
            let hint = if round % 3 == 0 { given.len() } else { 0 };
            let mut loader = SortedSetInner::loader(hint);
            let mut inserted = SortedSetInner::new();
            for (m, s) in &given {
                loader.push(m, *s);
                inserted.insert_bytes(m, *s);
            }
            let loaded = loader.finish();
            assert_eq!(order(&loaded), order(&inserted), "round {round}");
            assert_eq!(loaded.is_packed(), inserted.is_packed(), "round {round}");
            if let Repr::Packed(block) = &loaded.repr {
                block.check();
                assert_eq!(block.allocation_size(), loaded.packed_bytes().unwrap());
            }
        }
        // In order by score, a member given twice: once, with its last score.
        let mut loader = SortedSetInner::loader(3);
        loader.push(b"a", 1.0);
        loader.push(b"b", 2.0);
        loader.push(b"a", 3.0);
        let loaded = loader.finish();
        assert!(loaded.is_packed());
        assert_eq!(order(&loaded), pairs(&[(b"b", 2.0), (b"a", 3.0)]));
        // A count over the limit starts large; -0 is kept as 0.
        let mut loader = SortedSetInner::loader(129);
        loader.push(b"a", -0.0);
        let loaded = loader.finish();
        assert!(!loaded.is_packed());
        assert_eq!(loaded.score(b"a").unwrap().to_bits(), 0.0f64.to_bits());
        let mut loader = SortedSetInner::loader(1);
        loader.push(b"a", -0.0);
        assert_eq!(
            loader.finish().score(b"a").unwrap().to_bits(),
            0.0f64.to_bits()
        );
        own_limits(None);
    }

    #[test]
    fn expect_moves_a_block_as_redis_zadd_does() {
        own_limits(Some([128, 64]));
        let mut zs = SortedSetInner::new();
        zs.insert_bytes(b"a", 1.0);
        zs.expect(128);
        assert!(zs.is_packed());
        zs.expect(129);
        assert!(!zs.is_packed());
        assert_eq!(order(&zs), pairs(&[(b"a", 1.0)]));
        own_limits(None);
    }

    #[test]
    fn a_handle_is_no_bigger_than_a_block() {
        assert!(std::mem::size_of::<SortedSetInner>() <= 32);
        assert_eq!(SortedSetInner::new().packed_bytes(), Some(0));
    }

    /// A set of `n` members `m0..`, `mI` with score `I`, in either form.
    fn numbered(n: usize, packed: bool) -> SortedSetInner {
        let mut zs = if packed {
            SortedSetInner::new()
        } else {
            made_large()
        };
        for i in 0..n {
            zs.insert_bytes(format!("m{i}").as_bytes(), i as f64);
        }
        assert_eq!(zs.is_packed(), packed);
        zs
    }

    fn index_of(member: &[u8]) -> usize {
        std::str::from_utf8(&member[1..]).unwrap().parse().unwrap()
    }

    /// Pearson's statistic of `counts` against equal expectation, and a
    /// bound seven standard deviations over its mean: a fair draw is over
    /// it about once in a billion.
    fn chi_square_fails(counts: &[u64]) -> Option<(f64, f64)> {
        let total: u64 = counts.iter().sum();
        let expected = total as f64 / counts.len() as f64;
        let x: f64 = counts
            .iter()
            .map(|&o| (o as f64 - expected).powi(2) / expected)
            .sum();
        let df = (counts.len() - 1) as f64;
        let bound = df + 7.0 * (2.0 * df).sqrt();
        (x >= bound).then_some((x, bound))
    }

    /// Every member is as likely as any other, one at a time and in a
    /// distinct sample (counted where it falls in the reply too, so the
    /// order is random), in both forms; the score is the member's own.
    #[test]
    fn random_picks_are_uniform_in_both_forms() {
        let mut rng = rand::thread_rng();
        for (n, packed) in [(100, true), (100, false), (1000, false)] {
            let zs = numbered(n, packed);
            let mut one = vec![0u64; n];
            for _ in 0..20 * n {
                let (member, score) = zs.random(&mut rng).unwrap();
                assert_eq!(index_of(&member) as f64, score);
                one[index_of(&member)] += 1;
            }
            assert_eq!(chi_square_fails(&one), None, "one, n {n} packed {packed}");
            // A third of the set (picked a bucket at a time) and most of it
            // (one walk), and one.
            for count in [1, n / 10, n / 3, n / 3 + 1, n - 1] {
                let mut all = vec![0u64; n];
                let mut first = vec![0u64; n];
                let mut last = vec![0u64; n];
                // At least five first and last picks a member expected.
                for _ in 0..(40 * n / count).max(5 * n) {
                    let sample = zs.random_distinct(count, &mut rng);
                    assert_eq!(sample.len(), count);
                    let distinct: BTreeSet<usize> =
                        sample.iter().map(|(m, _)| index_of(m)).collect();
                    assert_eq!(distinct.len(), count, "repeats in {count} of {n}");
                    for (member, score) in &sample {
                        assert_eq!(index_of(member) as f64, *score);
                        all[index_of(member)] += 1;
                    }
                    first[index_of(&sample[0].0)] += 1;
                    last[index_of(&sample[count - 1].0)] += 1;
                }
                assert_eq!(
                    chi_square_fails(&all),
                    None,
                    "{count} of {n}, packed {packed}"
                );
                assert_eq!(chi_square_fails(&first), None, "first, {count} of {n}");
                assert_eq!(chi_square_fails(&last), None, "last, {count} of {n}");
            }
        }
    }

    /// A count of the size of the set or more is every member once; none
    /// is none; an empty set has nothing to pick.
    #[test]
    fn random_distinct_edges() {
        let mut rng = rand::thread_rng();
        for packed in [true, false] {
            let zs = numbered(100, packed);
            for count in [100, 101, usize::MAX] {
                let mut got: Vec<usize> = zs
                    .random_distinct(count, &mut rng)
                    .iter()
                    .map(|(m, _)| index_of(m))
                    .collect();
                got.sort_unstable();
                assert_eq!(got, (0..100).collect::<Vec<_>>());
            }
            assert!(zs.random_distinct(0, &mut rng).is_empty());
        }
        let mut zs = made_large();
        assert!(zs.random(&mut rng).is_none());
        assert!(zs.random_distinct(3, &mut rng).is_empty());
        zs.insert_bytes(b"m0", 0.0);
        assert_eq!(&*zs.random(&mut rng).unwrap().0, b"m0");
        assert!(SortedSetInner::new().random(&mut rng).is_none());
    }

    /// A big set emptied of most of its members one at a time shrinks its
    /// table as it goes (a few members a removal, the rest by the
    /// background), and the picks stay uniform throughout, mid-shrink too.
    #[test]
    fn random_picks_stay_uniform_as_a_table_shrinks() {
        let mut rng = rand::thread_rng();
        let mut zs = numbered(100_000, false);
        let big = zs.allocation_size();
        // Every member but m0..m49 by every remover: one at a time, the
        // lowest, the highest, by rank and by score.
        assert_eq!(zs.remove_ranks(90_000, 99_999), 10_000);
        assert_eq!(
            zs.remove_range_by_score((
                Bound::Included((OrderedFloat(50_000.0), m(b""))),
                Bound::Unbounded
            )),
            40_000
        );
        for i in 50..40_000 {
            assert!(zs.remove(format!("m{i}").as_bytes()));
        }
        while zs.len() > 50 {
            zs.pop_max();
        }
        large(&zs).check();
        let mut one = vec![0u64; 50];
        for _ in 0..50 * 400 {
            one[index_of(&zs.random(&mut rng).unwrap().0)] += 1;
        }
        assert_eq!(chi_square_fails(&one), None);
        zs.finish_resize();
        assert!(
            zs.allocation_size() * 20 < big,
            "{} bytes left of {big}",
            zs.allocation_size()
        );
        let sample = zs.random_distinct(16, &mut rng);
        let distinct: BTreeSet<usize> = sample.iter().map(|(m, _)| index_of(m)).collect();
        assert_eq!(distinct.len(), 16);
    }

    /// `ZRANDMEMBER` costs the count asked for: a hundred thousand picks of
    /// a set that held a hundred thousand members and keeps one, or a
    /// thousand, take no walk a pick, and are every member alike.
    #[test]
    fn picks_cost_the_count_whatever_the_table_held() {
        let mut rng = rand::thread_rng();
        for n in [1usize, 1000] {
            let mut zs = numbered(100_000, false);
            for i in n..100_000 {
                assert!(zs.remove(format!("m{i}").as_bytes()));
            }
            let start = std::time::Instant::now();
            let mut counts = vec![0u64; n];
            let mut picks = 0;
            zs.random_each(100_000, &mut rng, |member, score| {
                assert_eq!(index_of(&member) as f64, score);
                counts[index_of(&member)] += 1;
                picks += 1;
            });
            assert_eq!(picks, 100_000);
            let sample = zs.random_distinct(n / 3, &mut rng);
            let distinct: BTreeSet<usize> = sample.iter().map(|(m, _)| index_of(m)).collect();
            assert_eq!(distinct.len(), n / 3);
            let took = start.elapsed();
            assert!(
                took < std::time::Duration::from_secs(3),
                "{n} members: {took:?}"
            );
            if n > 1 {
                assert_eq!(chi_square_fails(&counts), None, "picks of {n}");
                let mut all = vec![0u64; n];
                let mut first = vec![0u64; n];
                for _ in 0..(5 * n) {
                    let sample = zs.random_distinct(50, &mut rng);
                    for (member, _) in &sample {
                        all[index_of(member)] += 1;
                    }
                    first[index_of(&sample[0].0)] += 1;
                }
                assert_eq!(chi_square_fails(&all), None, "distinct of {n}");
                assert_eq!(chi_square_fails(&first), None, "first of distinct of {n}");
            }
        }
    }

    /// A table is sized for the members it holds, not for a command's
    /// arguments: a `ZADD` of a hundred thousand pairs naming two hundred
    /// members leaves the table a `ZADD` of the two hundred once leaves, and
    /// `settle` after it has nothing to give back.
    #[test]
    fn a_table_is_sized_for_its_members_not_a_commands_arguments() {
        own_limits(Some([128, 64]));
        let mut repeated = SortedSetInner::new();
        repeated.expect(100_000);
        for i in 0..100_000 {
            repeated.insert_bytes(format!("m{}", i % 200).as_bytes(), (i % 200) as f64);
        }
        repeated.settle(100_000);
        let mut once = SortedSetInner::new();
        once.expect(200);
        for i in 0..200 {
            once.insert_bytes(format!("m{i}").as_bytes(), i as f64);
        }
        once.settle(200);
        assert!(!repeated.is_packed() && !once.is_packed());
        assert_eq!(order(&repeated), order(&once));
        assert_eq!(repeated.capacity(), once.capacity());
        // Each table has its own random seed, so their overflow groups
        // differ a little.
        let (a, b) = (repeated.allocation_size(), once.allocation_size());
        assert!(a.abs_diff(b) * 20 <= b, "{a} against {b}");
        own_limits(None);
    }

    /// A bulk removal of most of the set shrinks the table as it goes and
    /// ends at the size of what is left; so does a set popped down one at a
    /// time. (A table shrinks once under an eighth full: it is never more
    /// than about seven times the size its members need.)
    #[test]
    fn a_removal_leaves_a_table_sized_for_what_is_left() {
        let mut zs = numbered(100_000, false);
        let full = zs.capacity();
        assert_eq!(zs.remove_ranks(0, 89_999), 90_000);
        large(&zs).check();
        zs.finish_resize();
        assert!(zs.capacity() < full);
        assert!(
            zs.capacity() <= 4 * zs.len(),
            "{} for {}",
            zs.capacity(),
            zs.len()
        );
        while zs.len() > 1_000 {
            zs.pop_min();
        }
        zs.finish_resize();
        assert!(zs.capacity() <= 8 * zs.len());
        large(&zs).check();
    }

    /// A load gives up on a member the table cannot hold, rather than drop
    /// it: the error says what, and nothing is half built.
    #[test]
    fn a_load_refuses_what_a_table_cannot_hold() {
        crate::collection_table::set_test_max_len(Some(100));
        for count in [10, 1_000] {
            let mut loader = SortedSetInner::loader(count);
            for i in 0..count {
                loader
                    .push_loaded(format!("m{i}").as_bytes(), i as f64)
                    .unwrap();
            }
            let err = loader
                .push_loaded(&[b'x'; 101], 1.0)
                .expect_err("a member over the limit");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        }
        crate::collection_table::set_test_max_len(None);
    }

    fn table_state(set: &Shared) -> (bool, bool, usize) {
        let inner = set.read();
        let t = inner.table().expect("a table");
        (t.resizing(), t.holds_retired(), inner.allocation_size())
    }

    /// Steps the background until `set` has nothing left there, as the
    /// expiry thread would; returns the longest hold of its lock.
    fn finish_in_background(set: &Shared) -> std::time::Duration {
        let mut longest = std::time::Duration::ZERO;
        for _ in 0..1_000_000 {
            let (resizing, retired, _) = table_state(set);
            if !resizing && !retired && !set.read().is_queued() {
                return longest;
            }
            let work = step_background(std::time::Duration::from_millis(1));
            longest = longest.max(work.longest_hold);
        }
        panic!("the background never finished the set");
    }

    /// A set whose writes stop just after a growth started is finished by
    /// the background with no further write, a step at a time, and its old
    /// array freed; after most of it is removed, the shrinks the removes
    /// leave are finished too, and the memory comes back. The index's
    /// references are good throughout: the moves are slot words.
    #[test]
    fn the_background_finishes_a_resize_the_writes_left() {
        own_limits(Some([128, 64]));
        let n: usize = if cfg!(debug_assertions) {
            200_000
        } else {
            3_000_000
        };
        let set = shared(SortedSetInner::new());
        let mut i = 0usize;
        {
            let mut inner = set.write();
            while i < n || !inner.table().is_some_and(|t| t.resizing()) {
                inner.insert_bytes(format!("member:{i:010}").as_bytes(), i as f64);
                i += 1;
            }
            after_write(&set, &mut inner);
            assert!(inner.is_queued());
        }
        let (resizing, _, mid) = table_state(&set);
        assert!(resizing);
        let longest = finish_in_background(&set);
        let (_, _, done) = table_state(&set);
        assert!(done < mid, "{done} bytes after, {mid} during");
        assert_eq!(set.read().len(), i);
        large(&set.read()).check();
        eprintln!("growth to {i} members finished; longest hold {longest:?}");
        {
            let mut inner = set.write();
            for j in 0..i {
                if j % 20 != 0 {
                    assert!(inner.remove(format!("member:{j:010}").as_bytes()));
                }
            }
            after_write(&set, &mut inner);
        }
        finish_in_background(&set);
        let (_, _, shrunk) = table_state(&set);
        assert_eq!(set.read().len(), i.div_ceil(20));
        assert!(
            shrunk * 8 < done,
            "{shrunk} bytes after the shrink, {done} before"
        );
        large(&set.read()).check();
        own_limits(None);
    }

    /// A set whose key went while it waited for the background is not kept
    /// alive by it; one held by a reader is left for later, never waited
    /// for; and a set moved out of its value mid-resize and made a key's
    /// value again (as a value made whole from another's is) is handed over
    /// again, not left with the old value's mark (the bug #976 found in
    /// hashes).
    #[test]
    fn the_background_holds_sets_weakly_and_a_rewrapped_set_is_handed_over_again() {
        own_limits(Some([128, 64]));
        let grown = || {
            let set = shared(SortedSetInner::new());
            let mut inner = set.write();
            let mut i = 0;
            while i < 1000 || !inner.table().is_some_and(|t| t.resizing()) {
                inner.insert_bytes(format!("m{i}").as_bytes(), i as f64);
                i += 1;
            }
            after_write(&set, &mut inner);
            drop(inner);
            set
        };
        let gone = grown();
        let weak = Arc::downgrade(&gone);
        drop(gone);
        assert!(weak.upgrade().is_none());
        step_background(std::time::Duration::from_millis(50));
        assert!(
            !crate::resize_background::is_handed(weak.as_ptr().cast()),
            "a deleted set stayed handed over"
        );
        let held = grown();
        let reader = held.read();
        step_background(std::time::Duration::from_millis(5));
        assert!(reader.table().unwrap().resizing());
        assert!(crate::resize_background::is_handed(
            Arc::as_ptr(&held).cast()
        ));
        drop(reader);
        finish_in_background(&held);
        assert!(!crate::resize_background::is_handed(
            Arc::as_ptr(&held).cast()
        ));
        let old = grown();
        let inner = std::mem::take(&mut *old.write());
        assert!(inner.is_queued());
        drop(old);
        let again = shared(inner);
        assert!(again.read().is_queued());
        assert!(crate::resize_background::is_handed(
            Arc::as_ptr(&again).cast()
        ));
        finish_in_background(&again);
        large(&again.read()).check();
        // A set built whole mid-growth (a load, `RESTORE`, a `*STORE`
        // command) is handed over as it is made a key's value.
        let mut loader = SortedSetInner::loader(200_705);
        for i in 0..200_705 {
            loader.push(format!("x{i:08}").as_bytes(), i as f64);
        }
        let built = loader.finish();
        assert!(built.table().unwrap().resizing());
        let built = shared(built);
        assert!(built.read().is_queued());
        finish_in_background(&built);
        large(&built.read()).check();
        own_limits(None);
    }
}
