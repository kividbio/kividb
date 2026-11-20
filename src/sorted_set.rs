//! A sorted set's members and scores, in one of two forms, as Redis keeps a
//! sorted set as a listpack or as a skiplist:
//!
//! - **Packed.** A set of no more than `zset-max-listpack-entries` members
//!   (128), none longer than `zset-max-listpack-value` bytes (64), is one
//!   block ([`PackedZset`]): its entries in order, by score and then by
//!   member, a fingerprint a member to find one by name. A new set is an
//!   empty block, which allocates nothing.
//! - **Large.** Past either limit, a hash table of members and an ordered
//!   tree of `(score, member)`, each holding the member's one `Arc`: rank,
//!   ranges and lookups by name for sets of any size. The table is held
//!   directly, so that a bucket can be asked for by its number: a random
//!   member is a random bucket, tried again while it is empty, as Redis
//!   picks one (`ZRANDMEMBER`). A table that falls below an eighth full is
//!   made smaller when that costs the command little (see
//!   `Large::shrink_if_sparse`), so that is a handful of tries; one too big
//!   to shrink on the command path is sampled by one walk a command.
//!
//! A set moves from the block to the table and the tree, its entries in
//! order in one pass, when a write would take it past a limit, and never
//! back (as in Redis: a set that has been large is kept large, whatever is
//! taken out of it). The limits are read at each write that adds a member,
//! so a `CONFIG SET` takes effect at a set's next new member. A set built
//! whole (a load, `RESTORE`, a `*STORE` command's result) is built in the
//! form its size and members call for.
//!
//! Every reader goes through the methods here, so nothing outside knows the
//! form: what is written to disk and to replicas (members and scores, in
//! order) is the same either way.

use crate::member_set::Member;
use crate::packed_zset::{self, PackedZset, Score};
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A member name stored as a cheaply-cloneable Arc'd byte slice.
/// Using Arc<[u8]> means cloning the key between the BTreeMap and the table
/// costs only an atomic refcount increment, not a heap allocation.
pub type ZMember = Arc<[u8]>;

type ScoreKey = (OrderedFloat<f64>, ZMember);

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
    /// every key that holds one, and a block is a third of the size of the
    /// table and the tree.
    Large(Box<Large>),
}

/// Kept inline in every sorted set's shared block: a block's handle, no
/// more.
const _: () = assert!(std::mem::size_of::<SortedSetInner>() <= 32);

/// The large form: dual representation for O(log N) score-ordered
/// iteration AND O(1) member lookup:
///
///   members:  HashTable<(ZMember, f64)>
///             → "what is this member's score?" — O(1) average; and
///               "a member at random" — a random bucket, O(1) average
///
///   by_score: BTreeMap<(OrderedFloat<f64>, ZMember), ()>
///             → "give me all members with scores in [lo, hi]" — O(log N) seek + O(M) scan
///
/// The composite key (score, member) in by_score ensures that members with
/// equal scores are ordered lexicographically — matching Redis semantics exactly.
///
/// INVARIANT: members and by_score are always in sync. Every write path must
/// update both atomically (they live inside a single RwLock, so this is safe).
#[derive(Clone, Default)]
struct Large {
    /// Each member and its score. A `HashTable` rather than a map, for its
    /// buckets by number (a random member) and nothing else: the same
    /// table, the same bytes a member, the same hash.
    members: HashTable<(ZMember, f64)>,
    hasher: ahash::RandomState,
    by_score: BTreeMap<(OrderedFloat<f64>, ZMember), ()>,
}

impl Large {
    fn with_capacity(members: usize) -> Self {
        Self {
            members: HashTable::with_capacity(members),
            ..Self::default()
        }
    }

    #[inline]
    fn hash(&self, member: &[u8]) -> u64 {
        self.hasher.hash_one(member)
    }

    /// The member's own `Arc` and its score.
    #[inline]
    fn get(&self, member: &[u8]) -> Option<&(ZMember, f64)> {
        self.members
            .find(self.hash(member), |(have, _)| &**have == member)
    }

    #[inline]
    fn len(&self) -> usize {
        self.members.len()
    }

    /// Insert or update a member. Returns true if the member is new.
    /// This is the ONLY place both maps should be written — keeps the invariant.
    fn insert(&mut self, member: ZMember, score: f64) -> bool {
        let hash = self.hash(&member);
        let hasher = &self.hasher;
        match self.members.entry(
            hash,
            |(have, _)| **have == *member,
            |(have, _)| hasher.hash_one(&**have),
        ) {
            Entry::Occupied(mut found) => {
                // Member exists — remove old position in the score index
                // first. The table keeps its own key, as a map would.
                let old_score = std::mem::replace(&mut found.get_mut().1, score);
                self.by_score
                    .remove(&(OrderedFloat(old_score), member.clone()));
                self.by_score.insert((OrderedFloat(score), member), ());
                false // not new
            }
            Entry::Vacant(place) => {
                place.insert((member.clone(), score));
                self.by_score.insert((OrderedFloat(score), member), ());
                true // new member
            }
        }
    }

    /// Take the member out of the table alone: its `Arc` and score. The
    /// table is not made smaller here: see [`Self::shrink_if_sparse`].
    fn take(&mut self, member: &[u8]) -> Option<(ZMember, f64)> {
        let hash = self.hash(member);
        let found = self
            .members
            .find_entry(hash, |(have, _)| &**have == member)
            .ok()?;
        let (taken, _) = found.remove();
        Some(taken)
    }

    /// Remove a member from both indexes, the table left its size. Returns
    /// true if it existed.
    fn remove_only(&mut self, member: &[u8]) -> bool {
        match self.take(member) {
            Some((arc_member, score)) => {
                let indexed = self.by_score.remove(&(OrderedFloat(score), arc_member));
                debug_assert!(indexed.is_some(), "member missing from by_score");
                true
            }
            None => false,
        }
    }

    /// Remove a member. Returns true if it existed.
    fn remove(&mut self, member: &[u8]) -> bool {
        let removed = self.remove_only(member);
        if removed {
            self.shrink_if_sparse(0);
        }
        removed
    }

    fn pop_min(&mut self) -> Option<(ZMember, f64)> {
        let ((score, member), _) = self.by_score.pop_first()?;
        self.take(&member);
        self.shrink_if_sparse(0);
        Some((member, score.0))
    }

    fn pop_max(&mut self) -> Option<(ZMember, f64)> {
        let ((score, member), _) = self.by_score.pop_last()?;
        self.take(&member);
        self.shrink_if_sparse(0);
        Some((member, score.0))
    }

    /// Whether the table is less than an eighth full (and more than a few
    /// buckets): a random bucket of it is more often empty than not.
    #[inline]
    fn is_sparse(&self) -> bool {
        let buckets = self.members.num_buckets();
        buckets > 64 && self.members.len() * 8 < buckets
    }

    /// A table that has lost most of its members is made smaller, when that
    /// costs the command little next to what it did: it would otherwise
    /// keep the memory of its largest size, and a random bucket of it would
    /// nearly always be an empty one.
    ///
    /// `work` is the members the command wrote or removed (0 for a single
    /// removal, `ZREM` or `ZPOP*`). A table of up to
    /// [`SHRINK_INLINE_BUCKETS`] buckets, or eight times `work`, is shrunk:
    /// a single removal then moves fewer than 8,192 members (0.23 ms) and
    /// frees 1.5 MB at most. A bigger one is kept as it is (as it was before
    /// tables shrank), because giving its memory back stalls the command:
    /// freeing the 2^21 buckets of a million-member table took 20 ms on a
    /// `Standard_D4s_v5`, and even on the lazy-free thread a `ZPOPMIN` that
    /// handed it over took 14 ms (the unmapping holds the process's memory
    /// map). Sampling a sparse table walks it once a command (see
    /// [`Self::try_bucket`]).
    fn shrink_if_sparse(&mut self, work: usize) {
        if !self.is_sparse()
            || self.members.num_buckets() > SHRINK_INLINE_BUCKETS.max(work.saturating_mul(8))
        {
            return;
        }
        let hasher = &self.hasher;
        let mut smaller = HashTable::with_capacity(self.members.len() * 2);
        for entry in self.members.drain() {
            smaller.insert_unique(hasher.hash_one(&*entry.0), entry, |(have, _)| {
                hasher.hash_one(&**have)
            });
        }
        let emptied = std::mem::replace(&mut self.members, smaller);
        if emptied.num_buckets() > SHRINK_INLINE_BUCKETS {
            // A command that did as much work as the table is big (a bulk
            // removal, a `ZADD` of members given many times): its memory
            // goes back off the command path. Not an object of the
            // keyspace's: `INFO`'s lazy-free counts are left as they are.
            crate::lazyfree::free_later(emptied, 0);
        }
    }

    /// The bucket of a member picked at random, any one as likely as
    /// another, in at most sixty-four tries: `None` if the set is empty or
    /// every try found an empty bucket.
    ///
    /// A bucket picked at random is taken if it holds a member and picked
    /// again if not: each try is every bucket alike, so the one taken is
    /// every member alike. A table at least an eighth full misses sixty-four
    /// times in a row about once in five thousand picks; one left sparse
    /// (reserved for more than came, or too big to shrink on the command
    /// path) misses more often, and a caller then picks by a walk instead,
    /// which is uniform too. Whether the tries miss depends on which
    /// buckets are full, not on which member a hit would give, so switching
    /// to a walk keeps every pick uniform.
    #[inline]
    fn try_bucket(&self, rng: &mut impl rand::Rng) -> Option<usize> {
        if self.members.is_empty() {
            return None;
        }
        let buckets = self.members.num_buckets();
        for _ in 0..64 {
            let at = rng.gen_range(0..buckets);
            if self.members.get_bucket(at).is_some() {
                return Some(at);
            }
        }
        None
    }

    /// A member and its score, picked at random: a random bucket, or after
    /// sixty-four misses a walk to a random member.
    fn random(&self, rng: &mut impl rand::Rng) -> Option<&(ZMember, f64)> {
        if let Some(at) = self.try_bucket(rng) {
            return self.members.get_bucket(at);
        }
        use rand::seq::IteratorRandom;
        self.members.iter().choose(rng)
    }

    /// `count` different members, picked at random, in a random order, for
    /// a count of no more than a third of the set: one bucket at a time, a
    /// bucket that comes up twice picked again. Each pick is every member
    /// not yet picked alike, so the sample is as `rand::seq::index::sample`
    /// would give; the expected tries are at most twelve a member (a table
    /// an eighth full, the third of it already picked). `None` if a pick
    /// missed sixty-four times (a sparse table): the caller then samples by
    /// one walk, and what was picked so far is dropped, which is fair, as
    /// the misses do not depend on which members were picked.
    fn random_distinct(
        &self,
        count: usize,
        rng: &mut impl rand::Rng,
    ) -> Option<Vec<&(ZMember, f64)>> {
        debug_assert!(count <= self.len());
        let mut seen: ahash::AHashSet<usize> = ahash::AHashSet::with_capacity(count);
        let mut picked = Vec::with_capacity(count);
        while picked.len() < count {
            let at = self.try_bucket(rng)?;
            if seen.insert(at)
                && let Some(entry) = self.members.get_bucket(at)
            {
                picked.push(entry);
            }
        }
        Some(picked)
    }
}

/// The biggest table (1.5 MB of buckets) a single removal (`ZREM`,
/// `ZPOP*`) makes smaller: it moves fewer than 8,192 members, about 0.23 ms
/// on a `Standard_D4s_v5` (4,096 take 0.08 ms, 16,384 0.64 ms; see the
/// `shrink_cost` test).
const SHRINK_INLINE_BUCKETS: usize = 1 << 16;

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
    /// table and the tree, without their contents).
    pub const LARGE_HEADER: usize = std::mem::size_of::<Large>();

    /// How many allocations freeing the set frees, as Redis counts them for
    /// lazy freeing: a block is one.
    pub fn free_effort(&self) -> usize {
        match &self.repr {
            Repr::Packed(_) => 1,
            Repr::Large(large) => large.members.len(),
        }
    }

    /// Bytes that freeing the set gives back, at least (for eviction): a
    /// block's, exactly; the large form's, `per_member` a member.
    pub fn freeing_at_least(&self, per_member: usize) -> usize {
        match &self.repr {
            Repr::Packed(block) => block.allocation_size(),
            Repr::Large(large) => large.members.len() * per_member,
        }
    }

    /// The table and the tree, from the block, in one pass in order; with
    /// room in the table for `more` members besides.
    #[cold]
    #[inline(never)]
    fn make_large(&mut self, more: usize) {
        let Repr::Packed(block) = &self.repr else {
            return;
        };
        let mut large = Large::with_capacity(block.len() + more);
        let mut keys = Vec::with_capacity(block.len());
        for (member, score) in block.iter() {
            let member: ZMember = Arc::from(member);
            // A block's members are distinct.
            let hasher = &large.hasher;
            large.members.insert_unique(
                hasher.hash_one(&*member),
                (Arc::clone(&member), score),
                |(have, _)| hasher.hash_one(&**have),
            );
            keys.push(((OrderedFloat(score), member), ()));
        }
        large.by_score = keys.into_iter().collect();
        self.repr = Repr::Large(Box::new(large));
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
            self.make_large(elements);
        }
    }

    /// Insert or update a member. Returns true if the member is new.
    ///
    /// A score of -0 is kept as 0, as Redis keeps it in the listpack that
    /// holds a sorted set of up to 128 members: `ZADD z -0 m` then
    /// `ZSCORE z m` is `0` (#422).
    pub fn insert(&mut self, member: ZMember, score: f64) -> bool {
        // -0 + 0 is 0; any other score is itself.
        let score = score + 0.0;
        match &mut self.repr {
            Repr::Large(large) => large.insert(member, score),
            Repr::Packed(_) => self.insert_bytes(&member, score),
        }
    }

    /// [`insert`](Self::insert) of a member's bytes: a block copies them,
    /// and the large form makes its `Arc` of them only if the member is new.
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
                            self.make_large(1);
                            self.large_mut().insert(Arc::from(member), score);
                        }
                    }
                    out
                }
            },
            Repr::Large(large) => match large.get(member) {
                Some(&(ref key, old)) => {
                    let (score, out) = decide(Some(old));
                    if let Some(score) = score {
                        let score = score + 0.0;
                        if score.to_bits() != old.to_bits() {
                            // The member's own `Arc`, for both indexes: no
                            // second copy of its bytes.
                            let key = Arc::clone(key);
                            large.insert(key, score);
                        }
                    }
                    out
                }
                None => {
                    let (score, out) = decide(None);
                    if let Some(score) = score {
                        large.insert(Arc::from(member), score + 0.0);
                    }
                    out
                }
            },
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
        match &mut self.repr {
            Repr::Large(large) => large.pop_min(),
            Repr::Packed(_) => self.pop_with(false, |member, score| (Arc::from(member), score)),
        }
    }

    /// Removes and returns the member with the highest score.
    pub fn pop_max(&mut self) -> Option<(ZMember, f64)> {
        match &mut self.repr {
            Repr::Large(large) => large.pop_max(),
            Repr::Packed(_) => self.pop_with(true, |member, score| (Arc::from(member), score)),
        }
    }

    /// Removes the member with the lowest score (`max`: the highest), and
    /// hands it to `take` before it goes: a block's member is not copied.
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
            Repr::Large(large) => {
                let (member, score) = if max {
                    large.pop_max()
                } else {
                    large.pop_min()
                }?;
                Some(take(&member, score))
            }
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match &self.repr {
            Repr::Packed(block) => block.find(member).map(|i| block.score(i)),
            Repr::Large(large) => large.get(member).map(|&(_, score)| score),
        }
    }

    /// The member's 0-based rank by ascending score, and its score.
    pub fn rank(&self, member: &[u8]) -> Option<(usize, f64)> {
        match &self.repr {
            Repr::Packed(block) => block.find(member).map(|i| (i, block.score(i))),
            Repr::Large(large) => {
                let (stored, score) = large.get(member)?;
                let score = *score;
                let rank = large
                    .by_score
                    .range(..(OrderedFloat(score), Arc::clone(stored)))
                    .count();
                Some((rank, score))
            }
        }
    }

    /// Members and scores, ordered by score, then by member.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (Member<'_>, f64)> {
        match &self.repr {
            Repr::Packed(block) => Either::A(Packed(block.iter())),
            Repr::Large(large) => Either::B(
                large
                    .by_score
                    .keys()
                    .map(|(score, member)| (Member::Shared(member), score.0)),
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
            Repr::Large(large) => Either::B(RankRange {
                keys: large.by_score.keys(),
                skip_front: start,
                skip_back: len - end,
                left: end.saturating_sub(start),
            }),
        }
    }

    /// Members and scores in no particular order.
    pub fn iter_unordered(&self) -> impl Iterator<Item = (Member<'_>, f64)> {
        match &self.repr {
            Repr::Packed(block) => Either::A(Packed(block.iter())),
            Repr::Large(large) => Either::B(
                large
                    .members
                    .iter()
                    .map(|(member, score)| (Member::Shared(member), *score)),
            ),
        }
    }

    /// A page of a scan from `cursor`: see `helpers::scan_page`. A block is
    /// one page, whole, whatever the cursor: as Redis scans a listpack.
    pub fn scan_page(
        &self,
        cursor: u64,
        count: usize,
        pages: usize,
    ) -> (impl ExactSizeIterator<Item = (Member<'_>, f64)>, u64) {
        use crate::commands::helpers::{prefetched, scan_page, scan_position};
        match &self.repr {
            Repr::Packed(block) => (Either::A(Packed(block.iter())), 0),
            Repr::Large(large) => {
                let (page, next_cursor) = scan_page(
                    prefetched(large.members.iter(), |(member, _)| member.as_ptr())
                        .map(|(member, score)| (scan_position(member), (member, *score))),
                    cursor,
                    count,
                    large.members.len(),
                    pages,
                );
                let page = page
                    .into_iter()
                    .map(|(member, score)| (Member::Shared(member), score));
                (Either::B(page), next_cursor)
            }
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
                    .by_score
                    .range(range)
                    .map(|((score, member), _)| (Member::Shared(member), score.0)),
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
    /// a block's by its index, the large form's by a random bucket of its
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
                .map(|(member, score)| (Member::Shared(member), *score)),
        }
    }

    /// `count` different members and their scores, picked at random, in a
    /// random order (`ZRANDMEMBER` with a positive count); all of them if
    /// `count` is the size of the set or more, but then in no order that
    /// was picked.
    ///
    /// A few of many (no more than a third) are picked a bucket at a time,
    /// a bucket that comes up twice picked again. More are found by one walk
    /// of the table to the last of the positions chosen, which is then no
    /// more work than the answer is long. A block is asked for the chosen
    /// places only. Every way gives each set of `count` members, in each
    /// order, alike. A table too sparse to pick from by bucket (see
    /// `Large::try_bucket`) is walked once, whatever the count: no call
    /// costs more than one walk of the table and the count.
    pub fn random_distinct(
        &self,
        count: usize,
        rng: &mut impl rand::Rng,
    ) -> Vec<(Member<'_>, f64)> {
        let len = self.len();
        let count = count.min(len);
        if let Repr::Large(large) = &self.repr
            && count * 3 <= len
            && let Some(picked) = large.random_distinct(count, rng)
        {
            return picked
                .into_iter()
                .map(|(member, score)| (Member::Shared(member), *score))
                .collect();
        }
        self.members_at(&rand::seq::index::sample(rng, len, count).into_vec())
    }

    /// `count` members picked at random, each on its own (so they may
    /// repeat), handed to `f` in the order picked (`ZRANDMEMBER` with a
    /// negative count). A block's by index; the large form's by random
    /// bucket while the buckets tried hold members, and if a pick misses
    /// sixty-four times (a sparse table), every pick left by one walk of the
    /// table: a call costs at most one walk and the count, as the walk alone
    /// did before. Whether a pick misses does not depend on which member a
    /// hit would give, so every pick is every member alike either way.
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
        let large = match &self.repr {
            Repr::Packed(block) => {
                for _ in 0..count {
                    let (member, score) = block.get(rng.gen_range(0..len));
                    f(Member::Bytes(member), score);
                }
                return;
            }
            Repr::Large(large) => large,
        };
        let mut done = 0;
        while done < count {
            let Some((member, score)) = large
                .try_bucket(rng)
                .and_then(|at| large.members.get_bucket(at))
            else {
                break;
            };
            f(Member::Shared(member), *score);
            done += 1;
        }
        if done < count {
            let picks: Vec<usize> = (done..count).map(|_| rng.gen_range(0..len)).collect();
            for (member, score) in self.members_at(&picks) {
                f(member, score);
            }
        }
    }

    /// After a command has written `written` members: a table it reserved
    /// for more than came (`ZADD` of members given twice) is made smaller,
    /// which moves no more members than the command wrote.
    pub fn settle(&mut self, written: usize) {
        if let Repr::Large(large) = &mut self.repr {
            large.shrink_if_sparse(written);
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
        let mut out: Vec<Option<(&ZMember, f64)>> = vec![None; picks.len()];
        // One walk, up to the highest position picked.
        let mut walk = large.members.iter();
        let mut at: Option<usize> = None;
        let mut last: Option<(&ZMember, f64)> = None;
        for slot in slots {
            let want = picks[slot];
            if at != Some(want) {
                let skip = match at {
                    Some(at) => want - at - 1,
                    None => want,
                };
                last = walk.nth(skip).map(|(member, score)| (member, *score));
                at = Some(want);
            }
            out[slot] = last;
        }
        out.into_iter()
            .flatten()
            .map(|(member, score)| (Member::Shared(member), score))
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
            Repr::Large(_) => {
                let gone: Vec<ZMember> = self
                    .range_by_rank(start, stop)
                    .map(|(member, _)| member.to_shared())
                    .collect();
                let large = self.large_mut();
                for member in &gone {
                    large.remove_only(member);
                }
                // Once, at the end: the command has paid for as many.
                large.shrink_if_sparse(gone.len());
                gone.len()
            }
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
            Repr::Large(_) => {
                let gone: Vec<ZMember> = self
                    .range_by_score(range)
                    .map(|(member, _)| member.to_shared())
                    .collect();
                let large = self.large_mut();
                for member in &gone {
                    large.remove_only(member);
                }
                // Once, at the end: the command has paid for as many.
                large.shrink_if_sparse(gone.len());
                gone.len()
            }
        }
    }

    /// Take out the members `gone` says to; how many went.
    pub fn remove_where(&mut self, mut gone: impl FnMut(&[u8], f64) -> bool) -> usize {
        match &mut self.repr {
            Repr::Packed(block) => block.retain(|member, score| !gone(member, score)),
            Repr::Large(_) => {
                let members: Vec<ZMember> = self
                    .iter()
                    .filter(|(member, score)| gone(member, *score))
                    .map(|(member, _)| member.to_shared())
                    .collect();
                let large = self.large_mut();
                for member in &members {
                    large.remove_only(member);
                }
                // Once, at the end: the command has paid for as many.
                large.shrink_if_sparse(members.len());
                members.len()
            }
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
            Repr::Large(large) => large.members.capacity(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Packed(block) => block.len(),
            Repr::Large(large) => large.members.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A builder of a set of about `count` members, given one at a time
    /// (a load, `RESTORE`, a command's result): a block if they fit one, the
    /// table and the tree if not, built in one go either way.
    pub fn loader(count: usize) -> Loader {
        if count > packed_entries() {
            Loader(Building::Large(Box::default()))
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

/// See [`SortedSetInner::loader`].
pub struct Loader(Building);

enum Building {
    /// The members so far, end to end in `bytes`; for each, where it ends
    /// and its score.
    Packed {
        bytes: Vec<u8>,
        entries: Vec<(u32, f64)>,
    },
    /// The table and the tree, filled as the members come (a large load is
    /// as fast as it was before blocks).
    Large(Box<Large>),
}

impl Loader {
    pub fn push(&mut self, member: &[u8], score: f64) {
        // -0 + 0 is 0; any other score is itself.
        let score = score + 0.0;
        match &mut self.0 {
            Building::Packed { bytes, entries } => {
                // A member given twice counts once, so the count is known
                // only at the end: past twice the limit, it is past it.
                if member.len() > packed_value() || entries.len() >= 2 * packed_entries() {
                    // Past what a block holds: the large form, with those
                    // given so far, then this one.
                    let mut large = Self::large_of(bytes, entries);
                    large.insert(Arc::from(member), score);
                    self.0 = Building::Large(large);
                } else {
                    bytes.extend_from_slice(member);
                    entries.push((bytes.len() as u32, score));
                }
            }
            Building::Large(large) => {
                large.insert(Arc::from(member), score);
            }
        }
    }

    /// The large form of the members given so far, in the order given.
    fn large_of(bytes: &[u8], entries: &[(u32, f64)]) -> Box<Large> {
        let mut large = Box::<Large>::default();
        let mut from = 0;
        for &(end, score) in entries {
            let end = end as usize;
            large.insert(Arc::from(&bytes[from..end]), score);
            from = end;
        }
        large
    }

    pub fn finish(self) -> SortedSetInner {
        match self.0 {
            Building::Large(large) => SortedSetInner {
                repr: Repr::Large(large),
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
                    return SortedSetInner {
                        repr: Repr::Large(Self::large_of(&bytes, &entries)),
                    };
                }
                SortedSetInner {
                    repr: Repr::Packed(PackedZset::from_sorted(&pairs)),
                }
            }
        }
    }
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

struct RankRange<'a> {
    keys: btree_map::Keys<'a, ScoreKey, ()>,
    // Members outside the range, walked past on the first step from each end.
    skip_front: usize,
    skip_back: usize,
    left: usize,
}

impl<'a> Iterator for RankRange<'a> {
    type Item = (Member<'a>, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        if self.skip_front > 0 {
            self.keys.nth(std::mem::take(&mut self.skip_front) - 1);
        }
        let (score, member) = self.keys.next()?;
        Some((Member::Shared(member), score.0))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl DoubleEndedIterator for RankRange<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        if self.skip_back > 0 {
            self.keys.nth_back(std::mem::take(&mut self.skip_back) - 1);
        }
        let (score, member) = self.keys.next_back()?;
        Some((Member::Shared(member), score.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        zs.make_large(0);
        zs
    }

    /// `members` and `by_score` hold exactly the same (member, score) pairs.
    /// (By value: after a score update the two keys can be different Arcs of
    /// the same bytes, as `insert` keeps the old key in `members`.)
    fn assert_in_sync(zs: &SortedSetInner) {
        let zs = large(zs);
        assert_eq!(zs.members.len(), zs.by_score.len());
        for (score, member) in zs.by_score.keys() {
            assert_eq!(zs.get(member.as_ref()).map(|e| e.1), Some(score.0));
        }
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
        assert!(large(&zs).by_score.is_empty());
    }

    /// Removal drops the member's last reference: neither index keeps it.
    #[test]
    fn remove_releases_the_member() {
        let mut zs = made_large();
        let a = m(b"a");
        zs.insert(Arc::clone(&a), 1.0);
        assert_eq!(Arc::strong_count(&a), 3); // ours, members, by_score
        assert!(zs.remove(b"a"));
        assert_eq!(Arc::strong_count(&a), 1);
    }

    /// A score changed through `upsert` keeps the member's one `Arc`.
    #[test]
    fn a_large_set_keeps_one_copy_of_a_member_whose_score_changes() {
        let mut zs = made_large();
        zs.insert_bytes(b"a", 1.0);
        zs.upsert(b"a", |_| (Some(2.0), ()));
        let large = large(&zs);
        let (key, _) = large.get(b"a").unwrap();
        assert_eq!(Arc::strong_count(key), 2);
        assert_eq!(zs.score(b"a"), Some(2.0));
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

    /// A big table emptied of most of its members one at a time is kept at
    /// its size (shrinking it would stall a command), and the picks stay
    /// uniform, by the walk; so they do from one reserved far beyond its
    /// members.
    #[test]
    fn random_picks_stay_uniform_in_a_sparse_table() {
        let mut rng = rand::thread_rng();
        let mut zs = numbered(100_000, false);
        let big = large(&zs).members.num_buckets();
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
        assert_in_sync(&zs);
        assert_eq!(large(&zs).members.num_buckets(), big);
        assert!(large(&zs).is_sparse());
        let mut one = vec![0u64; 50];
        for _ in 0..50 * 400 {
            one[index_of(&zs.random(&mut rng).unwrap().0)] += 1;
        }
        assert_eq!(chi_square_fails(&one), None);

        // Reserved for a hundred thousand, holding two hundred.
        let mut zs = SortedSetInner::new();
        zs.expect(100_000);
        for i in 0..200 {
            zs.insert_bytes(format!("m{i}").as_bytes(), i as f64);
        }
        assert!(large(&zs).members.num_buckets() > 200 * 256);
        let mut one = vec![0u64; 200];
        for _ in 0..200 * 100 {
            one[index_of(&zs.random(&mut rng).unwrap().0)] += 1;
        }
        assert_eq!(chi_square_fails(&one), None);
        let sample = zs.random_distinct(50, &mut rng);
        let distinct: BTreeSet<usize> = sample.iter().map(|(m, _)| index_of(m)).collect();
        assert_eq!(distinct.len(), 50);
    }
    /// A table reserved for a hundred thousand and holding one member, or a
    /// thousand: every pick misses often, and a call falls back to one walk
    /// for all its picks, so a hundred thousand picks cost about one walk,
    /// not a hundred thousand; and they are still every member alike.
    #[test]
    fn a_sparse_table_is_sampled_in_one_walk_a_call() {
        let mut rng = rand::thread_rng();
        for n in [1usize, 1000] {
            let mut zs = SortedSetInner::new();
            zs.expect(100_000);
            for i in 0..n {
                zs.insert_bytes(format!("m{i}").as_bytes(), i as f64);
            }
            assert!(large(&zs).is_sparse());
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
            // A walk a pick would be 100,000 walks of 131,072 buckets.
            assert!(
                took < std::time::Duration::from_secs(3),
                "{n} members: {took:?}"
            );
            if n > 1 {
                assert_eq!(chi_square_fails(&counts), None, "picks of {n}");
                // Distinct samples on the walk path: every member alike.
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
            // What a ZADD does after its writes: as small as what came.
            zs.settle(100_000);
            assert!(!large(&zs).is_sparse());
            assert_eq!(zs.len(), n);
        }
    }

    /// A single removal makes a table smaller only if it has no more than
    /// `SHRINK_INLINE_BUCKETS` buckets; a bulk removal, one of up to eight
    /// buckets a member it removed; a bigger sparse table is kept.
    #[test]
    fn a_shrink_on_the_command_path_is_bounded() {
        // 30,000 members: 65,536 buckets. Shrunk by a pop once sparse.
        let mut zs = numbered(30_000, false);
        let buckets = large(&zs).members.num_buckets();
        assert_eq!(buckets, SHRINK_INLINE_BUCKETS);
        while zs.len() > 8_192 {
            zs.pop_min();
        }
        assert_eq!(large(&zs).members.num_buckets(), buckets);
        // The first pop that leaves it less than an eighth full.
        zs.pop_min();
        assert!(large(&zs).members.num_buckets() < buckets);
        assert!(!large(&zs).is_sparse());
        assert_in_sync(&zs);

        // 100,000 members: 131,072 buckets. Popped to 100: kept.
        let mut zs = numbered(100_000, false);
        let buckets = large(&zs).members.num_buckets();
        assert!(buckets > SHRINK_INLINE_BUCKETS);
        while zs.len() > 100 {
            zs.pop_min();
        }
        assert!(large(&zs).is_sparse());
        assert_eq!(large(&zs).members.num_buckets(), buckets);
        assert_in_sync(&zs);
        // A bulk removal of the rest but one shrinks it only if it removed
        // an eighth as many members as there are buckets: 99 do not.
        assert_eq!(zs.remove_ranks(0, 98), 99);
        assert_eq!(large(&zs).members.num_buckets(), buckets);

        // A bulk removal of most of the set shrinks once, at the end.
        let mut zs = numbered(100_000, false);
        assert_eq!(zs.remove_ranks(0, 89_999), 90_000);
        assert!(!large(&zs).is_sparse());
        assert!(large(&zs).members.num_buckets() < buckets);
        assert_in_sync(&zs);
    }

    /// What moving `n` members into a smaller table costs, and freeing a
    /// big one: printed, for the choice of `SHRINK_INLINE_BUCKETS`. Run with `--release -- --ignored
    /// --nocapture shrink_cost`.
    #[test]
    #[ignore]
    fn shrink_cost() {
        for n in [4_096usize, 8_192, 16_384, 65_536, 262_144, 2_097_152] {
            let mut best = std::time::Duration::MAX;
            for _ in 0..5 {
                let mut large = Large::with_capacity(n * 8);
                for i in 0..n {
                    large.insert(Arc::from(format!("membr:{i}").as_bytes()), i as f64);
                }
                let start = std::time::Instant::now();
                large.shrink_if_sparse(usize::MAX);
                best = best.min(start.elapsed());
                assert!(!large.is_sparse());
            }
            println!("shrink of {n} members: {best:?}");
        }
        // A million members taken down to 8,192, one at a time: what
        // shrinking that table would cost (it is not shrunk on the command
        // path), and its parts.
        let mut large = Large::default();
        for i in 0..1_000_000usize {
            large.insert(Arc::from(format!("membr:{i}").as_bytes()), i as f64);
        }
        for i in 0..1_000_000 - 8_192 {
            large.take(format!("membr:{i}").as_bytes());
        }
        let mut copy = large.clone();
        let start = std::time::Instant::now();
        large.shrink_if_sparse(usize::MAX);
        let whole = start.elapsed();
        assert!(!large.is_sparse());
        let start = std::time::Instant::now();
        let entries: Vec<_> = copy.members.drain().collect();
        let drained = start.elapsed();
        let start = std::time::Instant::now();
        drop(copy);
        let freed = start.elapsed();
        println!(
            "shrink from 2^21 buckets to {}: {whole:?} (drain {drained:?}, free in place {freed:?})",
            entries.len()
        );
    }
}
