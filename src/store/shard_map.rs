//! One shard of a database's keyspace, behind an interface that does not
//! say how it is kept.
//!
//! Commands ask for a key's value and get a [`ValueRef`], a view of it that
//! lives as long as the shard's lock. They never see a `&KiviDBValue` from
//! the table. A value going in, coming out or moving between keys is an
//! owned [`KiviDBValue`] (or, coming out, a [`Taken`] entry that can give
//! one).
//!
//! The table is hashbrown's, used directly: its buckets can be reached by
//! index, which is what a cursor that survives the lock being let go
//! (compaction), and a key picked at random (`RANDOMKEY`, eviction), need.
//! Each bucket is an eight-byte [`Slot`]: the address of one block holding
//! the key and its value together (`entry`), and its time to live if it has
//! one.
//!
//! **Times to live.** A write that gives a key a value keeps the time to
//! live the key had ([`ShardMap::insert`], [`ShardMap::insert_str`]), as
//! writing the value of a key does in Redis when the command does not say
//! otherwise; the `_expiring` forms set it, or take it away, in the same
//! step. A key past its time is absent to reads and writes here, though it
//! stays in the table until expiry takes it out. Beside the table, the
//! shard keeps a list of the entries that have one, each entry knowing its
//! place in it: what active expiry goes through and what the `volatile-*`
//! policies pick from, at random in one step.
//!
//! **A key's use.** The lookups a command makes to read or write a key
//! write its use to the key's entry as they find it, when the command counts
//! as a use under the eviction policy (`access`): [`ShardMap::get`],
//! `get_expiring`, `lookup`, `contains_key`, `use_key`, every write of a
//! value, `set_expires` and `persist`. [`ShardMap::peek`], `expires`,
//! `access` and the walks over the table never do, and nothing does to a
//! key past its time: that key is gone, whatever its entry says.
//!
//! **Writes in place.** A string written over a string kept inline goes
//! into the same block when the table alone holds the entry, the key is not
//! past its time, and the new length keeps the block in its slab class
//! ([`ShardMap::set_str`], `append_in_place`, `set_range_in_place`): nothing
//! is allocated or freed, and the entry stays where it is. Anything else,
//! and every entry somebody else holds (a snapshot being written), gets a
//! new entry, as before.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hashbrown::HashTable;
use parking_lot::RwLock;

use super::access;
use super::entry::{Entry, Left, Relocation, Slot};
pub use super::entry::{EntryRef, Taken};
use super::ttl_list::TtlList;
use crate::shared_bytes::SharedBytes;
use crate::types::{
    ArrayInner, HashFields, HashInner, KiviDBValue, SortedSetInner, StreamInner, TsInner,
    VectorSetInner,
};
use crate::vector::IndexOn;

/// A key's string value, borrowed from its shard: its bytes, kept in the
/// key's entry, or a `SharedBytes` of their own when they are too long.
#[derive(Clone, Copy)]
pub struct StrRef<'a>(Bytes<'a>);

#[derive(Clone, Copy)]
enum Bytes<'a> {
    Inline(&'a [u8]),
    Shared(&'a SharedBytes),
}

impl<'a> StrRef<'a> {
    #[inline]
    pub(super) fn bytes(bytes: &'a [u8]) -> Self {
        StrRef(Bytes::Inline(bytes))
    }

    #[inline]
    pub fn as_bytes(self) -> &'a [u8] {
        match self.0 {
            Bytes::Inline(bytes) => bytes,
            Bytes::Shared(shared) => shared.as_slice(),
        }
    }

    /// The bytes, to keep once the shard is let go of: a second hold on
    /// the same block when they have one of their own, a copy when they are
    /// in the key's entry.
    #[inline]
    pub fn to_shared(self) -> SharedBytes {
        match self.0 {
            Bytes::Inline(bytes) => SharedBytes::from(bytes),
            Bytes::Shared(shared) => shared.clone(),
        }
    }

    /// The `SharedBytes` the bytes are in, when they are not in the entry.
    #[inline]
    pub(super) fn shared(self) -> Option<&'a SharedBytes> {
        match self.0 {
            Bytes::Inline(_) => None,
            Bytes::Shared(shared) => Some(shared),
        }
    }

    /// Whether `other` is this very value, not merely equal bytes. Bytes
    /// in an entry are no `SharedBytes`: never.
    #[inline]
    pub fn is(self, other: &SharedBytes) -> bool {
        self.shared()
            .is_some_and(|shared| SharedBytes::ptr_eq(shared, other))
    }
}

impl std::ops::Deref for StrRef<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for StrRef<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// A key's value, borrowed from its shard: what [`ShardMap::get`] gives.
/// The variants are [`KiviDBValue`]'s. A collection is its shared handle,
/// which a command locks, or clones to keep.
#[derive(Clone, Copy)]
pub enum ValueRef<'a> {
    String(StrRef<'a>),
    List(&'a Arc<RwLock<VecDeque<Arc<[u8]>>>>),
    Hash(&'a Arc<RwLock<HashFields>>),
    Json(&'a Arc<RwLock<serde_json::Value>>),
    Set(&'a Arc<RwLock<crate::member_set::MemberSet>>),
    VectorSet(&'a Arc<RwLock<VectorSetInner>>),
    SortedSet(&'a Arc<RwLock<SortedSetInner>>),
    Stream(&'a Arc<RwLock<StreamInner>>),
    TimeSeries(&'a Arc<RwLock<TsInner>>),
    HashEx(&'a Arc<RwLock<HashInner>>),
    Array(&'a Arc<RwLock<ArrayInner>>),
    Sketch(&'a Arc<RwLock<crate::sketch::Sketch>>),
}

impl<'a> From<&'a KiviDBValue> for ValueRef<'a> {
    #[inline]
    fn from(value: &'a KiviDBValue) -> Self {
        match value {
            KiviDBValue::String(v) => ValueRef::String(StrRef(Bytes::Shared(v))),
            KiviDBValue::List(v) => ValueRef::List(v),
            KiviDBValue::Hash(v) => ValueRef::Hash(v),
            KiviDBValue::Json(v) => ValueRef::Json(v),
            KiviDBValue::Set(v) => ValueRef::Set(v),
            KiviDBValue::VectorSet(v) => ValueRef::VectorSet(v),
            KiviDBValue::SortedSet(v) => ValueRef::SortedSet(v),
            KiviDBValue::Stream(v) => ValueRef::Stream(v),
            KiviDBValue::TimeSeries(v) => ValueRef::TimeSeries(v),
            KiviDBValue::HashEx(v) => ValueRef::HashEx(v),
            KiviDBValue::Array(v) => ValueRef::Array(v),
            KiviDBValue::Sketch(v) => ValueRef::Sketch(v),
        }
    }
}

impl KiviDBValue {
    /// A view of an owned value, as [`ShardMap::get`] gives one of a stored
    /// value: for code that takes either.
    #[inline]
    pub fn view(&self) -> ValueRef<'_> {
        ValueRef::from(self)
    }
}

impl ValueRef<'_> {
    /// The value, to keep once the shard is let go of: another handle on
    /// the same string or collection (see [`KiviDBValue::deep_clone`] for a
    /// copy that shares nothing).
    pub fn to_value(self) -> KiviDBValue {
        match self {
            ValueRef::String(v) => KiviDBValue::String(v.to_shared()),
            ValueRef::List(v) => KiviDBValue::List(Arc::clone(v)),
            ValueRef::Hash(v) => KiviDBValue::Hash(Arc::clone(v)),
            ValueRef::Json(v) => KiviDBValue::Json(Arc::clone(v)),
            ValueRef::Set(v) => KiviDBValue::Set(Arc::clone(v)),
            ValueRef::VectorSet(v) => KiviDBValue::VectorSet(Arc::clone(v)),
            ValueRef::SortedSet(v) => KiviDBValue::SortedSet(Arc::clone(v)),
            ValueRef::Stream(v) => KiviDBValue::Stream(Arc::clone(v)),
            ValueRef::TimeSeries(v) => KiviDBValue::TimeSeries(Arc::clone(v)),
            ValueRef::HashEx(v) => KiviDBValue::HashEx(Arc::clone(v)),
            ValueRef::Array(v) => KiviDBValue::Array(Arc::clone(v)),
            ValueRef::Sketch(v) => KiviDBValue::Sketch(Arc::clone(v)),
        }
    }

    /// [`KiviDBValue::deep_clone`] of the value.
    pub fn deep_clone(self) -> KiviDBValue {
        self.to_value().deep_clone()
    }

    /// [`KiviDBValue::is_empty_collection`].
    pub fn is_empty_collection(self) -> bool {
        match self {
            ValueRef::List(list) => list.read().is_empty(),
            ValueRef::Set(set) => set.read().is_empty(),
            ValueRef::SortedSet(zset) => zset.read().is_empty(),
            ValueRef::Hash(hash) => hash.read().is_empty(),
            ValueRef::HashEx(hash) => hash.read().is_empty(),
            ValueRef::VectorSet(vset) => vset.read().entries.is_empty(),
            ValueRef::String(_)
            | ValueRef::Json(_)
            | ValueRef::Stream(_)
            | ValueRef::TimeSeries(_)
            | ValueRef::Array(_)
            | ValueRef::Sketch(_) => false,
        }
    }

    /// [`KiviDBValue::type_name`].
    pub fn type_name(self) -> &'static str {
        match self {
            ValueRef::String(_) => "string",
            ValueRef::List(_) => "list",
            ValueRef::Hash(_) => "hash",
            ValueRef::Json(_) => "ReJSON-RL",
            ValueRef::Set(_) => "set",
            ValueRef::VectorSet(_) => "vectorset",
            ValueRef::SortedSet(_) => "zset",
            ValueRef::Stream(_) => "stream",
            ValueRef::TimeSeries(_) => "TSDB-TYPE",
            ValueRef::HashEx(_) => "hash",
            ValueRef::Array(_) => "array",
            ValueRef::Sketch(sketch) => sketch.read().type_name(),
        }
    }
}

/// What [`ShardMap::relocate_at`] did.
pub enum Relocated {
    /// Moved: what was left behind, to be let go of once the shard is
    /// unlocked (freeing it can give a slab back to the kernel).
    Moved(Leftover),
    /// In a slab being emptied, but held by something besides its key.
    Held,
    /// Nothing there, or nothing in a slab being emptied.
    Stays,
}

/// A block compaction moved a key's entry, or its string, out of.
pub enum Leftover {
    Entry(Left),
    Bytes(SharedBytes),
}

/// What a write of a key's value did ([`ShardMap::set_str`]).
pub enum Written {
    /// The key was not there.
    New,
    /// The value was written over the old one, in the same entry.
    InPlace,
    /// The key has a new entry; this is the old one, best dropped once the
    /// shard is unlocked.
    Replaced(Taken),
}

/// What [`ShardMap::remove_if`] did with a key.
pub enum Removal {
    /// It was taken out: its entry, best dropped once the shard is unlocked.
    Taken(Taken),
    /// It is there, and was left as it was.
    Kept,
    /// There is no such key.
    Absent,
}

impl Written {
    /// The old entry, if the key had one that is not its entry now.
    #[inline]
    pub fn replaced(self) -> Option<Taken> {
        match self {
            Written::Replaced(old) => Some(old),
            Written::New | Written::InPlace => None,
        }
    }
}

/// `entry` was found or made by a lookup of a key hashed `hash` in the table
/// `map` (its address): the key's use is written, if the command this
/// thread runs counts as one (see `access`). One thread-local load when it
/// does not.
#[inline(always)]
fn used(entry: Entry, hash: u64, map: u64) {
    let stamp = access::stamp();
    if stamp != 0 {
        access::used(entry, stamp, map ^ hash);
    }
}

/// How many positions a `SCAN` goes through a shard's keys in
/// ([`ShardMap::scan`]): one for each value of the 12-bit fragment of a
/// key's hash a slot keeps. A key keeps its position for as long as it is
/// in the table, whatever the table does to make room or give it back.
pub const SCAN_POSITIONS: u64 = 4096;

/// One shard's keys and values: a table of [`Slot`]s, each the address of
/// an entry holding a key and its value (`entry`).
pub struct ShardMap {
    table: HashTable<Slot>,
    /// While the table grows, the one it grows from: its keys are moved
    /// into `table` a few at a time, by every write and by the expiry
    /// thread (`ShardMap::step_growth`), and every look that misses in
    /// `table` looks here too. Empty, without an allocation, the rest of
    /// the time. See [`ShardMap::make_room`].
    old: HashTable<Slot>,
    /// The bucket of `old` the next step of moving starts at.
    moving_at: usize,
    /// Changes whenever entries change buckets (a growth begun, a step of
    /// one, a shrink): what a walk over the buckets that lets go of the
    /// lock between parts compares ([`ShardMap::layout`]).
    layout: u64,
    hash: ahash::RandomState,
    /// Every entry whose key has a time to live, each knowing where it is
    /// here (`Entry::pos`), in two parts: those that may come due soon, and
    /// those that cannot (see `ttl_list`).
    ttl: TtlList,
    /// The time a write lock holds the shard at, so that a command sees one
    /// time throughout ([`now_at`]). [`UNPINNED`] while no write lock holds
    /// the shard: every look reads the clock. [`PENDING`] once one does,
    /// until the first entry with a time to live it looks at reads the
    /// clock and keeps the time here. So a lock that meets no such entry
    /// never reads the clock: reading it on every write lock of a shard
    /// with a key with a time to live cost SET, INCR and DEL on such keys
    /// 3-4% more instructions.
    ///
    /// Atomic so that a look through `&self` can keep the time; it is only
    /// written under the write lock, so a reader, which only ever finds
    /// [`UNPINNED`] here, never writes it.
    now: AtomicU64,
    /// Keys a write under this lock has something to say of when it lets
    /// go, and what ([`ToSay`]). One list, so that a write that leaves none
    /// reads one word to find that out.
    left: Vec<(ToSay, crate::store::Key)>,
}

/// What a write lock has to say of a key once it lets go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ToSay {
    /// It was past its time, and a write put a new value over it: to be
    /// expired (`expiry::written_over`).
    PastItsTime,
    /// Its document (a hash, a JSON document) was taken out, or a value of
    /// another kind put over it: to leave its indexes
    /// (`vector::documents_left`).
    Document,
}

/// What [`ShardMap::lookup`] finds under a key.
pub enum Lookup<'a> {
    /// The key's value: the key is live.
    Live(ValueRef<'a>),
    /// A key past its time, still in the table.
    Past,
    /// No such key.
    Absent,
}

/// What a write does with the time to live the key has.
#[derive(Clone, Copy)]
enum Ttl {
    /// Keeps it (none, if the key is new).
    Keep,
    /// Gives the key this one, or takes it away.
    Set(Option<u64>),
}

/// No write lock holds the shard: see `ShardMap::now`.
const UNPINNED: u64 = 0;
/// A write lock holds the shard and has not read the clock yet.
const PENDING: u64 = u64::MAX;

/// Whether `entry` is past its time, by the shard's time `now` (see
/// `ShardMap::now`). Keys without a time to live never read the clock.
#[inline(always)]
fn expired(entry: Entry, now: &AtomicU64) -> bool {
    entry.has_ttl() && entry.expires().is_some_and(|at| past(at, now))
}

/// Whether the time to live that ends at `at` has, by the shard's time.
/// Never while the thread is loading a dataset or applying its master's
/// stream (`expiry::held`): what expired there is said by a `DEL`.
#[inline]
fn past(at: u64, now: &AtomicU64) -> bool {
    now_at(now) > at && !crate::expiry::held()
}

/// The shard's time: the one its write lock keeps, read from the clock the
/// first time it is asked for; the clock's own without a write lock.
#[inline]
fn now_at(now: &AtomicU64) -> u64 {
    match now.load(Ordering::Relaxed) {
        UNPINNED => crate::commands::helpers::now_ms(),
        PENDING => {
            let t = crate::commands::helpers::now_ms();
            now.store(t, Ordering::Relaxed);
            t
        }
        t => t,
    }
}

/// A key a write found past its time: expired lazily, as Redis counts it.
/// Kept for the write lock to expire once it lets go (`ShardWrite`): its
/// `expired` event, the clients watching or caching it, its documents, and
/// the `DEL` propagated before the write's own record.
#[cold]
#[inline(never)]
fn written_over_expired(left: &mut Vec<(ToSay, crate::store::Key)>, key: &[u8]) {
    crate::stats::record_expired();
    left.push((ToSay::PastItsTime, crate::store::Key::new(key)));
}

/// A live key's document was taken out, or written over by a value of
/// another kind: whatever command did it, the document is no longer there
/// to find. Kept for the write lock to take out of the search indexes once
/// it lets go (`ShardWrite`). Not every command that can do this to a
/// document told the indexes, and a search went on finding the key (#619).
#[cold]
#[inline(never)]
fn document_left(left: &mut Vec<(ToSay, crate::store::Key)>, key: &[u8]) {
    if !crate::vector::none_anywhere() {
        left.push((ToSay::Document, crate::store::Key::new(key)));
    }
}

/// The kind of document a value is to the search indexes, if it is one.
#[inline(always)]
fn document_on(value: ValueRef<'_>) -> Option<IndexOn> {
    match value {
        ValueRef::Hash(_) | ValueRef::HashEx(_) => Some(IndexOn::Hash),
        ValueRef::Json(_) => Some(IndexOn::Json),
        _ => None,
    }
}

impl Default for ShardMap {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ShardMap {
    fn drop(&mut self) {
        self.clear();
    }
}

impl ShardMap {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ShardMap {
            table: HashTable::with_capacity(capacity),
            old: HashTable::new(),
            moving_at: 0,
            layout: 0,
            hash: ahash::RandomState::new(),
            ttl: TtlList::new(),
            now: AtomicU64::new(UNPINNED),
            left: Vec::new(),
        }
    }

    #[inline(always)]
    fn hash_of(&self, key: &[u8]) -> u64 {
        self.hash.hash_one(key)
    }

    /// The slot of `key`, hashed `hash`.
    #[inline(always)]
    fn find(&self, hash: u64, key: &[u8]) -> Option<Slot> {
        match self
            .table
            .find(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        {
            Some(s) => Some(*s),
            None if self.is_growing() => self.find_old(hash, key),
            None => None,
        }
    }

    /// The slot of `key` in the table being grown from, if it is still
    /// there.
    #[cold]
    #[inline(never)]
    fn find_old(&self, hash: u64, key: &[u8]) -> Option<Slot> {
        self.old
            .find(hash, |s| s.may_be(hash) && s.entry().has_key(key))
            .copied()
    }

    /// Whether the table is growing: keys are still to be moved from the
    /// table it grows from.
    #[inline(always)]
    pub fn is_growing(&self) -> bool {
        !self.old.is_empty()
    }

    /// What changes whenever entries change buckets: a walk over
    /// [`ShardMap::bucket`]s in parts, the lock let go of in between, that
    /// finds it changed may have looked at some keys twice and missed
    /// others.
    #[inline]
    pub fn layout(&self) -> u64 {
        self.layout
    }

    /// Before a write that may add `key`, hashed `hash`: room in the table
    /// for one more key, and the key in `table` if it was still in `old`.
    #[inline(always)]
    fn make_room_for(&mut self, hash: u64, key: &[u8]) {
        if self.table.len() == self.table.capacity() || self.is_growing() {
            self.make_room(hash, key);
        }
    }

    /// Before a write that changes or takes out `key`, hashed `hash`: the
    /// key in `table` if it was still in `old`.
    #[inline(always)]
    fn settle(&mut self, hash: u64, key: &[u8]) {
        if self.is_growing() {
            self.make_room(hash, key);
        }
    }

    /// A full table grows, without stopping the shard for as long as it
    /// takes to move all its keys.
    ///
    /// hashbrown grows a table in one step: a table twice the size, every
    /// key hashed again and put in it. A key's hash is not kept (the slot
    /// keeps 12 bits of it), so that is a visit to every entry, wherever in
    /// memory it is: about 75 ns a key, 18 ms for a shard of 230,000 keys
    /// and 34 ms for 460,000, with the shard locked for writing. Keys
    /// spread evenly over the shards, so the shards fill, and grow, at
    /// about the same time: while 8 million keys were written, a pipeline
    /// of 2,000 `SET`s met up to eight growths, and a `PING` from another
    /// client waited up to 150 ms (#752).
    ///
    /// Here a large table grows as Redis's do: into a new table twice the
    /// size, which takes every new key, while the keys of the old one move
    /// over a few at a time ([`GROWTH_STEP`] for each write to the shard,
    /// and the rest from the expiry thread, `ShardLock::step_growth`). A
    /// look that misses in the new table looks in the old one, and a write
    /// to a key still in the old one moves that key first, so every key is
    /// in exactly one of them. A slot moves as it is, the fragment of its
    /// key's hash with it. Both tables are there until the old one is
    /// empty, as both were while hashbrown moved the keys: the new one is
    /// twice the old one, so at most half as much again.
    #[cold]
    #[inline(never)]
    fn make_room(&mut self, hash: u64, key: &[u8]) {
        if self.is_growing() {
            if let Ok(at) = self
                .old
                .find_entry(hash, |s| s.may_be(hash) && s.entry().has_key(key))
            {
                let slot = at.remove().0;
                self.insert_moved(hash, slot);
            }
            self.step_growth(GROWTH_STEP);
        }
        if self.table.len() == self.table.capacity() {
            // Each write moves GROWTH_STEP keys and adds one at most, so
            // the old table is empty long before the new one is full.
            // Should it not be, its keys move now.
            while !self.step_growth(usize::MAX) {}
            self.grow();
        }
    }

    /// Grow the full table: in one step if it is small, else by moving to
    /// a new table twice its size (see [`ShardMap::make_room`]).
    fn grow(&mut self) {
        debug_assert!(!self.is_growing());
        self.layout = self.layout.wrapping_add(1);
        let keys = self.table.len();
        if keys < GROW_IN_STEPS_FROM {
            let hasher = &self.hash;
            self.table.reserve(1, |s| hasher.hash_one(s.entry().key()));
            return;
        }
        // Twice the buckets of a table full to hashbrown's 7/8; as many,
        // sweeping away what removals left, if it is half full or less.
        let new = HashTable::with_capacity(keys * 2);
        self.old = std::mem::replace(&mut self.table, new);
        self.moving_at = 0;
        self.step_growth(GROWTH_STEP);
    }

    /// Put `slot`, moved from `old`, its key hashed `hash`, in `table`.
    #[inline(always)]
    fn insert_moved(&mut self, hash: u64, slot: Slot) {
        let hasher = &self.hash;
        // Never full while the old table has keys (`make_room`); were it
        // full, it would grow here in one step, which is correct too.
        self.table
            .insert_unique(hash, slot, |s| hasher.hash_one(s.entry().key()));
    }

    /// Move up to `keys` keys from the table being grown from into the new
    /// one, looking at no more than ten buckets for each. Once the old
    /// table is empty its memory is given back. Returns whether the table
    /// has stopped growing.
    pub fn step_growth(&mut self, keys: usize) -> bool {
        const BATCH: usize = 16;
        if !self.is_growing() {
            // Its last key may have been moved by a write of it.
            self.old = HashTable::new();
            self.moving_at = 0;
            return true;
        }
        self.layout = self.layout.wrapping_add(1);
        let buckets = self.old.num_buckets();
        let mut left = keys;
        let mut visits = keys.saturating_mul(10);
        while left > 0 && visits > 0 && self.moving_at < buckets && self.is_growing() {
            // A batch of slots, their entries' first lines asked for
            // together: hashing a key reads it from its entry, a miss each.
            let mut batch = [(0usize, None::<Slot>); BATCH];
            let mut n = 0;
            while n < BATCH.min(left) && visits > 0 && self.moving_at < buckets {
                let at = self.moving_at;
                self.moving_at += 1;
                visits -= 1;
                if let Some(&slot) = self.old.get_bucket(at) {
                    slot.prefetch_header();
                    batch[n] = (at, Some(slot));
                    n += 1;
                }
            }
            for &(at, slot) in &batch[..n] {
                let Some(slot) = slot else { continue };
                let hash = self.hash.hash_one(slot.entry().key());
                if let Ok(entry) = self.old.get_bucket_entry(at) {
                    entry.remove();
                }
                self.insert_moved(hash, slot);
            }
            left -= n;
        }
        if self.is_growing() {
            if self.moving_at >= buckets {
                // Every bucket was passed and a key is still there, which
                // nothing puts in the old table: start over rather than
                // let go of it.
                debug_assert!(self.old.is_empty(), "a key left in the old table");
                self.moving_at = 0;
            }
            return false;
        }
        self.old = HashTable::new();
        self.moving_at = 0;
        true
    }

    /// The slot of `key`, unless the key is past its time.
    #[inline(always)]
    fn find_live(&self, hash: u64, key: &[u8]) -> Option<Slot> {
        self.find(hash, key)
            .filter(|s| !expired(s.entry(), &self.now))
    }

    /// A write lock holds the shard from now: its time is the clock's when
    /// it is first needed, and then the same throughout (see `now`).
    #[inline(always)]
    pub(super) fn pin_now(&mut self) {
        *self.now.get_mut() = PENDING;
    }

    /// The write lock lets go: the clock's time again.
    #[inline(always)]
    pub(super) fn unpin_now(&mut self) {
        *self.now.get_mut() = UNPINNED;
    }

    /// Whether a write has anything to say once the lock lets go: a key
    /// past its time that it put a value over ([`written_over_expired`]),
    /// or a document that left ([`document_left`]).
    #[inline(always)]
    pub(super) fn has_to_announce(&self) -> bool {
        !self.left.is_empty()
    }

    /// The kind of document `key` holds, if it holds one. Not a use of the
    /// key.
    pub(super) fn document_at(&self, key: &[u8]) -> Option<IndexOn> {
        self.find_live(self.hash_of(key), key)
            .and_then(|slot| document_on(slot.entry().value()))
    }

    /// The keys [`written_over_expired`] and [`document_left`] kept, taken.
    pub(super) fn take_left(&mut self) -> Vec<(ToSay, crate::store::Key)> {
        std::mem::take(&mut self.left)
    }

    /// This table, as a number: which table a key is in, for `access`.
    #[inline(always)]
    fn id(&self) -> u64 {
        self as *const Self as usize as u64
    }

    /// `key`'s value: a use of the key (see the module).
    #[inline(always)]
    pub fn get(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> Option<ValueRef<'_>> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        self.find_live(hash, key).map(|s| {
            let e = s.entry();
            used(e, hash, self.id());
            e.value()
        })
    }

    /// `key`'s value, without its use being written: for what only looks
    /// (`OBJECT`, `SORT`'s `BY` and `GET` patterns, ...). A key past its
    /// time is absent here too.
    #[inline]
    pub fn peek(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> Option<ValueRef<'_>> {
        let key = key.as_ref();
        self.find_live(self.hash_of(key), key)
            .map(|s| s.entry().value())
    }

    /// `key`'s value, and when its time to live ends (milliseconds since
    /// the Unix epoch) if it has one: both from the one entry. A use of the
    /// key.
    #[inline(always)]
    pub fn get_expiring(
        &self,
        key: &(impl AsRef<[u8]> + ?Sized),
    ) -> Option<(ValueRef<'_>, Option<u64>)> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        self.find_live(hash, key).map(|s| {
            let e = s.entry();
            used(e, hash, self.id());
            (e.value(), e.expires())
        })
    }

    /// [`ShardMap::get`], telling a key past its time from one not there:
    /// for a reader that expires the one it finds past its time, as `GET`
    /// does. The time is looked at once, here. A use of the key if it is
    /// live, and only then: the access word of a key past its time was
    /// brought up to date by a `GET` that went on to find it gone, by an
    /// `MGET`, and by a `GET` on a replica, which leaves it for its master.
    #[inline(always)]
    pub fn lookup(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> Lookup<'_> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        match self.find(hash, key) {
            Some(s) => {
                let e = s.entry();
                if expired(e, &self.now) {
                    return Lookup::Past;
                }
                used(e, hash, self.id());
                Lookup::Live(e.value())
            }
            None => Lookup::Absent,
        }
    }

    /// `key`'s value and when its time to live ends, past its time or not,
    /// its use not written: what a key past its time still holds.
    pub fn peek_expiring(
        &self,
        key: &(impl AsRef<[u8]> + ?Sized),
    ) -> Option<(ValueRef<'_>, Option<u64>)> {
        let key = key.as_ref();
        self.find(self.hash_of(key), key)
            .map(|s| (s.entry().value(), s.entry().expires()))
    }

    /// When `key`'s time to live ends, in milliseconds since the Unix epoch:
    /// `None` if it has none, or there is no such key. Past or not: this is
    /// what lazy and active expiry go by.
    #[inline(always)]
    pub fn expires(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> Option<u64> {
        let key = key.as_ref();
        self.find(self.hash_of(key), key)
            .and_then(|s| s.entry().expires())
    }

    /// Whether there is such a key: a use of it, if there is.
    #[inline(always)]
    pub fn contains_key(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> bool {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        match self.find_live(hash, key) {
            Some(s) => {
                used(s.entry(), hash, self.id());
                true
            }
            None => false,
        }
    }

    /// `key`'s access word: when it was last used, or how often
    /// (`eviction::Access`). `None` if there is no such key.
    #[inline]
    pub fn access(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> Option<u32> {
        let key = key.as_ref();
        self.find(self.hash_of(key), key)
            .map(|s| s.entry().access())
    }

    /// Make `key`'s access word over with `next`, written only if that
    /// changes it. A read lock is enough: the word is atomic, and no part
    /// of the key or its value (see `entry`'s note on it). Nothing happens
    /// if there is no such key, or it is past its time. The key is taken as
    /// the command gave it, in bytes: a key is found by its bytes, and one
    /// that is not UTF-8 is not there to find.
    #[inline]
    pub fn touch(&self, key: &[u8], next: impl FnOnce(u32) -> u32) {
        if let Some(s) = self.find_live(self.hash_of(key), key) {
            s.entry().touch(next);
        }
    }

    /// `key` is used, if it is there, not past its time, and this thread's
    /// command counts as a use (see the module): a lookup for nothing else.
    /// The key is taken in bytes, as `touch` takes it.
    #[inline]
    pub fn use_key(&self, key: &[u8]) {
        let hash = self.hash_of(key);
        if let Some(s) = self.find_live(hash, key) {
            used(s.entry(), hash, self.id());
        }
    }

    /// A hold on `key`'s entry: its key and value, readable once the shard
    /// is let go of, and unchanged for as long as it is held.
    pub fn get_ref(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> Option<EntryRef> {
        let key = key.as_ref();
        // SAFETY: the entry is in this table, which the caller has locked.
        self.find_live(self.hash_of(key), key)
            .map(|s| unsafe { EntryRef::new(s.entry()) })
    }

    /// Whether `key`'s entry is the one `held` holds: whether the key's
    /// value is the very one it had when `held` was taken.
    pub fn holds(&self, key: &(impl AsRef<[u8]> + ?Sized), held: &EntryRef) -> bool {
        let key = key.as_ref();
        self.find_live(self.hash_of(key), key)
            .is_some_and(|s| held.is(s.entry()))
    }

    /// Put an entry under `key`, and say what became of what the key had,
    /// past its time or not. One probe, whether the key is there or not.
    ///
    /// `make` makes the entry, given the time to live it is to have: the one
    /// `ttl` says, or the key's own if it keeps it. Made once the key is
    /// found, so that keeping a time to live costs nothing more. A key past
    /// its time is put as a new one, without it.
    ///
    /// With `in_place`, the string the entry is to hold: written over the
    /// key's string in its own entry, if that can be done (see the module),
    /// and nothing is made. Never over a key past its time.
    #[inline(always)]
    fn put(
        &mut self,
        key: &[u8],
        ttl: Ttl,
        in_place: Option<&[u8]>,
        make: impl FnOnce(&[u8], Option<u64>) -> Entry,
    ) -> Written {
        let hash = self.hash_of(key);
        self.make_room_for(hash, key);
        let now = &self.now;
        let map = self.id();
        let hasher = &self.hash;
        let list = &mut self.ttl;
        let left = &mut self.left;
        match self.table.entry(
            hash,
            |s| s.may_be(hash) && s.entry().has_key(key),
            |s| hasher.hash_one(s.entry().key()),
        ) {
            hashbrown::hash_table::Entry::Occupied(mut at) => {
                let old = at.get().entry();
                let stale = expired(old, now);
                if stale {
                    written_over_expired(left, key);
                }
                if !stale && let Some(bytes) = in_place {
                    // The time to live is in the block before the key: a
                    // write that gives the key one, or takes it away, needs
                    // a new block.
                    let room_stays = match ttl {
                        Ttl::Keep => true,
                        Ttl::Set(expires) => expires.is_some() == old.has_ttl(),
                    };
                    if room_stays && old.fits_in_place(bytes.len()) {
                        // The time it had, for the list's sum of them.
                        let was = match ttl {
                            Ttl::Set(Some(_)) => old.expires(),
                            _ => None,
                        };
                        // SAFETY: the shard is locked for writing (`&mut
                        // self`), the table alone holds the entry, and the
                        // bytes keep it in its class; a time to live it
                        // has is there to change.
                        unsafe {
                            old.write_in_place(bytes);
                            if let Ttl::Set(Some(ms)) = ttl {
                                old.set_expires(ms);
                            }
                        }
                        // A time that now ends before the horizon.
                        if let Some(was) = was {
                            list.retimed(old, was);
                        }
                        used(old, hash, map);
                        return Written::InPlace;
                    }
                }
                let entry = make(
                    key,
                    match ttl {
                        Ttl::Keep if stale => None,
                        Ttl::Keep => old.expires(),
                        Ttl::Set(expires) => expires,
                    },
                );
                // The key's use goes on: a value written over keeps the
                // count of an LFU policy, as in Redis. Not one past its
                // time: that key is gone, and this is a new one, which
                // starts where a new key does.
                if !stale {
                    entry.set_access(old.access());
                    let was = document_on(old.value());
                    if was.is_some() && was != document_on(entry.value()) {
                        document_left(left, key);
                    }
                }
                used(entry, hash, map);
                *at.get_mut() = Slot::new(entry, hash);
                list.replaced(old, entry);
                // SAFETY: the table's hold on the old entry goes to the caller.
                Written::Replaced(unsafe { Taken::new(old) })
            }
            hashbrown::hash_table::Entry::Vacant(place) => {
                let entry = make(
                    key,
                    match ttl {
                        Ttl::Keep => None,
                        Ttl::Set(expires) => expires,
                    },
                );
                used(entry, hash, map);
                place.insert(Slot::new(entry, hash));
                if entry.has_ttl() {
                    list.push(entry);
                }
                Written::New
            }
        }
    }

    /// Set `key` to `value`, and give back what it had: dropping that frees
    /// it, which is best done once the shard is unlocked. The key keeps its
    /// time to live.
    #[inline]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: KiviDBValue) -> Option<Taken> {
        self.put(key.as_ref(), Ttl::Keep, None, |key, expires| {
            Entry::new(key, value, expires)
        })
        .replaced()
    }

    /// [`ShardMap::insert`], the key's time to live set to `expires`
    /// (milliseconds since the Unix epoch), or taken away.
    #[inline]
    pub fn insert_expiring(
        &mut self,
        key: impl AsRef<[u8]>,
        value: KiviDBValue,
        expires: Option<u64>,
    ) -> Option<Taken> {
        self.put(key.as_ref(), Ttl::Set(expires), None, |key, expires| {
            Entry::new(key, value, expires)
        })
        .replaced()
    }

    /// Set `key` to the string `bytes`: [`ShardMap::insert`] of a string,
    /// with its bytes copied once, into the entry.
    #[inline]
    pub fn insert_str(&mut self, key: impl AsRef<[u8]>, bytes: impl AsRef<[u8]>) -> Option<Taken> {
        let bytes = bytes.as_ref();
        self.put(key.as_ref(), Ttl::Keep, None, |key, expires| {
            Entry::string(key, bytes, expires)
        })
        .replaced()
    }

    /// Set `key` to the string `bytes`, keeping its time to live: in place,
    /// in the key's own entry, when that can be done (see the module); else
    /// as [`ShardMap::insert_str`].
    #[inline]
    pub fn set_str(&mut self, key: impl AsRef<[u8]>, bytes: impl AsRef<[u8]>) -> Written {
        let bytes = bytes.as_ref();
        self.put(key.as_ref(), Ttl::Keep, Some(bytes), |key, expires| {
            Entry::string(key, bytes, expires)
        })
    }

    /// [`ShardMap::set_str`], the key's time to live set to `expires`, or
    /// taken away.
    #[inline]
    pub fn set_str_expiring(
        &mut self,
        key: impl AsRef<[u8]>,
        bytes: impl AsRef<[u8]>,
        expires: Option<u64>,
    ) -> Written {
        let bytes = bytes.as_ref();
        self.put(
            key.as_ref(),
            Ttl::Set(expires),
            Some(bytes),
            |key, expires| Entry::string(key, bytes, expires),
        )
    }

    /// `extra` added to the end of `key`'s string, in its own entry, if that
    /// can be done (see the module). Returns the string's new length if it
    /// was; `None` leaves everything as it was, for the caller to write a
    /// new value.
    pub fn append_in_place(
        &mut self,
        key: &(impl AsRef<[u8]> + ?Sized),
        extra: &[u8],
    ) -> Option<usize> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        let map = self.id();
        let entry = self.find_live(hash, key)?.entry();
        let len = entry.inline_len()? + extra.len();
        if !entry.fits_in_place(len) {
            return None;
        }
        // SAFETY: locked for writing, held by the table alone, and in class.
        unsafe { entry.write_at_in_place(len - extra.len(), extra) };
        used(entry, hash, map);
        Some(len)
    }

    /// `patch` written over `key`'s string from `offset`, the string grown
    /// with zero bytes to reach it, in its own entry if that can be done
    /// (see the module). Returns the string's new length if it was; `None`
    /// leaves everything as it was.
    pub fn set_range_in_place(
        &mut self,
        key: &(impl AsRef<[u8]> + ?Sized),
        offset: usize,
        patch: &[u8],
    ) -> Option<usize> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        let map = self.id();
        let entry = self.find_live(hash, key)?.entry();
        let len = entry.inline_len()?.max(offset.checked_add(patch.len())?);
        if !entry.fits_in_place(len) {
            return None;
        }
        // SAFETY: as in `append_in_place`.
        unsafe { entry.write_at_in_place(offset, patch) };
        used(entry, hash, map);
        Some(len)
    }

    /// [`ShardMap::insert_str`], the key's time to live set to `expires`, or
    /// taken away.
    #[inline]
    pub fn insert_str_expiring(
        &mut self,
        key: impl AsRef<[u8]>,
        bytes: impl AsRef<[u8]>,
        expires: Option<u64>,
    ) -> Option<Taken> {
        let bytes = bytes.as_ref();
        self.put(key.as_ref(), Ttl::Set(expires), None, |key, expires| {
            Entry::string(key, bytes, expires)
        })
        .replaced()
    }

    /// Give `key` the time to live that ends at `expires` (milliseconds
    /// since the Unix epoch). Returns whether there is such a key: one past
    /// its time is left for expiry to take out.
    pub fn set_expires(&mut self, key: &(impl AsRef<[u8]> + ?Sized), expires: u64) -> bool {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        self.settle(hash, key);
        let map = self.id();
        let Some(slot) = self
            .table
            .find_mut(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        else {
            return false;
        };
        let old = slot.entry();
        if expired(old, &self.now) {
            return false;
        }
        if old.has_ttl() && old.holders() == 1 {
            let was = old.expires().unwrap_or(0);
            // SAFETY: the shard is locked for writing, and nothing but the
            // table holds the entry.
            unsafe { old.set_expires(expires) };
            self.ttl.retimed(old, was);
            used(old, hash, map);
            return true;
        }
        let new = old.remade(Some(expires));
        used(new, hash, map);
        *slot = Slot::new(new, slot.hash_part());
        self.ttl.replaced(old, new);
        // SAFETY: the table's hold, given up: whoever else holds the old
        // entry still reads it as it was.
        unsafe { old.let_go() };
        true
    }

    /// Take `key`'s time to live away. Returns whether it had one.
    pub fn persist(&mut self, key: &(impl AsRef<[u8]> + ?Sized)) -> bool {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        self.settle(hash, key);
        let map = self.id();
        let Some(slot) = self
            .table
            .find_mut(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        else {
            return false;
        };
        let old = slot.entry();
        if expired(old, &self.now) {
            return false;
        }
        used(old, hash, map);
        if !old.has_ttl() {
            return false;
        }
        let new = old.remade(None);
        *slot = Slot::new(new, slot.hash_part());
        self.ttl.replaced(old, new);
        // SAFETY: as in `set_expires`.
        unsafe { old.let_go() };
        true
    }

    /// How many keys have a time to live.
    #[inline]
    pub fn ttl_len(&self) -> usize {
        self.ttl.len()
    }

    /// The sum of when the times to live of the [`ShardMap::ttl_len`] keys
    /// that have one end (milliseconds since the Unix epoch), kept as they
    /// change: `INFO keyspace`'s `avg_ttl` without a pass over them (#622).
    /// Keys past their time that expiry has not taken out yet are in it.
    #[inline]
    pub fn ttl_deadline_sum(&self) -> u128 {
        self.ttl.deadline_sum()
    }

    /// The entry at place `index` of the list of keys with a time to live:
    /// what a pick at random from them is (`index` modulo how many there
    /// are).
    pub fn ttl_entry(&self, index: usize) -> Option<EntryView<'_>> {
        Some(EntryView {
            entry: self.ttl.pick(index)?,
            _map: std::marker::PhantomData,
        })
    }

    /// Every key with a time to live, and when it ends.
    pub fn expiring(&self) -> impl Iterator<Item = (&[u8], u64)> {
        self.ttl.iter().map(|e| (e.key(), e.expires().unwrap_or(0)))
    }

    /// The keys whose time to live ended before `now` (milliseconds since
    /// the Unix epoch), at most `limit` of them: one pass over those that
    /// may have (`ttl_list`), stopping at the last one it takes. Returns
    /// them, and how many keys it looked at.
    pub fn due(&self, now: u64, limit: usize) -> (Vec<crate::store::Key>, usize) {
        self.ttl.due(now, limit)
    }

    /// Whether the keys with a time to live should be sorted again at `now`
    /// by when they may come due ([`ShardMap::sweep_ttl`]).
    #[inline]
    pub fn ttl_sweep_due(&self, now: u64) -> bool {
        self.ttl.sweep_due(now)
    }

    /// Sort the keys with a time to live by whether they may come due before
    /// the next horizon, looking at `steps` keys at most: what lets a pass
    /// for due keys look only at those (`ttl_list`). Returns whether it is
    /// done; if not, the next call goes on from where this one stopped.
    pub fn sweep_ttl(&mut self, now: u64, steps: usize) -> bool {
        self.ttl.sweep(now, steps)
    }

    /// Take `key` out if its time to live ended before `now`.
    pub fn remove_if_due(&mut self, key: &(impl AsRef<[u8]> + ?Sized), now: u64) -> Option<Taken> {
        if self.expires(key).is_some_and(|at| at < now) {
            self.remove(key)
        } else {
            None
        }
    }

    #[inline]
    pub fn remove(&mut self, key: &(impl AsRef<[u8]> + ?Sized)) -> Option<Taken> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        self.settle(hash, key);
        match self
            .table
            .find_entry(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        {
            Ok(at) => {
                let entry = at.remove().0.entry();
                if entry.has_ttl() {
                    self.ttl.take(entry);
                }
                if document_on(entry.value()).is_some() {
                    document_left(&mut self.left, key);
                }
                // SAFETY: the table's hold goes to the caller.
                Some(unsafe { Taken::new(entry) })
            }
            Err(_) => None,
        }
    }

    /// Take `key` out if `take` says so of its value. If it says no, the
    /// key is left as it was, its time to live and its access word with it,
    /// and the lookup is a use of the key: what a `GETDEL` of a key of
    /// another type does. One probe either way. Past its time or not, as
    /// [`ShardMap::remove`].
    ///
    /// Taking the key out and putting it back, as `GETDEL` did, made it a
    /// new entry: an LFU count of 5, whatever it had been (#221).
    #[inline]
    pub fn remove_if(
        &mut self,
        key: &(impl AsRef<[u8]> + ?Sized),
        take: impl FnOnce(ValueRef<'_>) -> bool,
    ) -> Removal {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        self.settle(hash, key);
        let map = self.id();
        match self
            .table
            .find_entry(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        {
            Ok(at) => {
                let entry = at.get().entry();
                if !take(entry.value()) {
                    used(entry, hash, map);
                    return Removal::Kept;
                }
                let entry = at.remove().0.entry();
                if entry.has_ttl() {
                    self.ttl.take(entry);
                }
                if document_on(entry.value()).is_some() {
                    document_left(&mut self.left, key);
                }
                // SAFETY: the table's hold goes to the caller.
                Removal::Taken(unsafe { Taken::new(entry) })
            }
            Err(_) => Removal::Absent,
        }
    }

    /// Put an entry [`ShardMap::remove`] took, from this table or another
    /// database's, under its key here: the entry itself, so that the key's
    /// value, time to live and access word are what they were, as Redis
    /// moves a key's object (`MOVE`). A key here past its time is written
    /// over, and expired as a write expires it; that entry is given back,
    /// best dropped once the shard is unlocked. A live key is left alone,
    /// and `taken` is given back instead.
    pub fn put_back(&mut self, taken: Taken) -> Result<Option<Taken>, Taken> {
        let hash = self.hash_of(taken.key());
        self.make_room_for(hash, taken.key());
        let now = &self.now;
        let hasher = &self.hash;
        let list = &mut self.ttl;
        let left = &mut self.left;
        let key = taken.key();
        match self.table.entry(
            hash,
            |s| s.may_be(hash) && s.entry().has_key(key),
            |s| hasher.hash_one(s.entry().key()),
        ) {
            hashbrown::hash_table::Entry::Occupied(mut at) => {
                let old = at.get().entry();
                if !expired(old, now) {
                    return Err(taken);
                }
                written_over_expired(left, key);
                let entry = taken.into_entry();
                *at.get_mut() = Slot::new(entry, hash);
                list.replaced(old, entry);
                // SAFETY: the table's hold on the old entry goes to the caller.
                Ok(Some(unsafe { Taken::new(old) }))
            }
            hashbrown::hash_table::Entry::Vacant(place) => {
                let entry = taken.into_entry();
                place.insert(Slot::new(entry, hash));
                if entry.has_ttl() {
                    list.push(entry);
                }
                Ok(None)
            }
        }
    }

    /// The value of `key`, made with `make` if there is none.
    pub fn get_or_insert_with(
        &mut self,
        key: &(impl AsRef<[u8]> + ?Sized),
        make: impl FnOnce() -> KiviDBValue,
    ) -> ValueRef<'_> {
        if !self.contains_key(key) {
            self.insert(key, make());
        }
        self.get(key).expect("a key just put in")
    }

    /// Every slot: the table's, then those of the table it grows from.
    #[inline]
    fn slots(&self) -> impl Iterator<Item = &Slot> {
        self.table.iter().chain(self.old.iter())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], ValueRef<'_>)> {
        self.slots().map(|s| {
            let e = s.entry();
            (e.key(), e.value())
        })
    }

    /// One page of a `SCAN` of this shard: each entry at a position from
    /// `from` on (see [`SCAN_POSITIONS`]), up to the position that brings
    /// the page to `want` entries, given to `visit`. A position is never
    /// split between two pages. Returns the position to go on from: zero
    /// when no entry is at a position past the page.
    ///
    /// A key's position is the fragment of its hash its slot keeps, so the
    /// pages are found from the slots alone: two passes over the table
    /// that touch only the entries of the page. It used to hash every key
    /// of the shard, visiting each entry, for every page (#751).
    ///
    /// While the table grows, both tables are counted and walked: a key
    /// still in the one it grows from is at its position as much as one
    /// already moved, and a move between pages keeps the slot, fragment
    /// and all, so a key moved mid-scan keeps its position too.
    pub fn scan(&self, from: u64, want: usize, mut visit: impl FnMut(EntryView<'_>)) -> u64 {
        const POSITIONS: usize = SCAN_POSITIONS as usize;
        let Ok(from) = usize::try_from(from) else {
            return 0;
        };
        if from >= POSITIONS || self.is_empty() {
            return 0;
        }
        let mut at = [0u32; POSITIONS];
        for slot in self.slots() {
            at[slot.scan_position() & (POSITIONS - 1)] += 1;
        }
        let mut taken = 0usize;
        let mut end = from;
        while end < POSITIONS && taken < want.max(1) {
            taken += at[end] as usize;
            end += 1;
        }
        let next = (end..POSITIONS).find(|&p| at[p] != 0).unwrap_or(0);
        if taken > 0 {
            for slot in self.slots() {
                if (from..end).contains(&slot.scan_position()) {
                    visit(EntryView {
                        entry: slot.entry(),
                        _map: std::marker::PhantomData,
                    });
                }
            }
        }
        next as u64
    }

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.slots().map(|s| s.entry().key())
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'_>> {
        self.slots().map(|s| s.entry().value())
    }

    /// Every entry, each as a key, a value and the means to hold it.
    pub fn entries(&self) -> impl Iterator<Item = EntryView<'_>> {
        self.slots().map(|s| EntryView {
            entry: s.entry(),
            _map: std::marker::PhantomData,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len() + self.old.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty() && self.old.is_empty()
    }

    /// What `key` takes in this shard besides its value (`MEMORY USAGE`):
    /// its entry's block, its share of the table's buckets, and, with a
    /// time to live, its share of the list of keys that have one. None if
    /// the key is absent or past its time. A look, not a use.
    pub fn entry_footprint(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> Option<usize> {
        let key = key.as_ref();
        let entry = self.find_live(self.hash_of(key), key)?.entry();
        // The table (and the list) is shared by the shard's keys, so a key
        // is given a share of it, at most a half-full table's share. A table
        // that has only grown is at least half full (it doubles when full),
        // so this is the whole share until keys leave. After a mass deletion
        // the room they left stays until the shard is shrunk (a sweep a
        // second, and never below a minimum size); spread over the keys
        // left, it made one key's estimate several times what it was. Under
        // the cap it stays put, and that room is no key's: in the deletions
        // tested, the keys' estimates then added up to typically 3-4% less
        // than `used_memory`, and from 1.1% to 5.7% less in all of them (the
        // most before the sweep had shrunk the shards): measured, not a
        // bound. An estimate policy, not an exact accounting of every live
        // allocation. (The key is in the table, so it has at least one key,
        // and a time to live puts it on the list.)
        let half_room = |len: usize, room: usize| len.max(room / 2).max(1);
        let table = (self.table.allocation_size() + self.old.allocation_size())
            / half_room(self.len(), self.table.num_buckets() / 8 * 7);
        let ttl = if entry.has_ttl() {
            self.ttl.capacity() * std::mem::size_of::<Entry>()
                / half_room(self.ttl.len(), self.ttl.capacity())
        } else {
            0
        };
        Some(entry.block_bytes() + table + ttl)
    }

    /// Keys the table has room for before it grows (while it grows, the
    /// new table's room).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.table.capacity()
    }

    /// Take every key out, keeping the table's room.
    pub fn clear(&mut self) {
        self.ttl.clear();
        for slot in self.table.drain().chain(self.old.drain()) {
            // SAFETY: the table's hold, given up.
            unsafe { slot.entry().let_go() };
        }
        self.old = HashTable::new();
        self.moving_at = 0;
        self.layout = self.layout.wrapping_add(1);
    }

    /// Shrink the table to room for `keys`, and the list of keys with a
    /// time to live, if it is sparse, to its keys and an eighth more. A
    /// table that is growing is left as it is until it is done.
    pub fn shrink_to(&mut self, keys: usize) {
        if !self.is_growing() {
            let hasher = &self.hash;
            let buckets = self.table.num_buckets();
            self.table
                .shrink_to(keys, |s| hasher.hash_one(s.entry().key()));
            if self.table.num_buckets() != buckets {
                self.layout = self.layout.wrapping_add(1);
            }
        }
        self.ttl.shrink_if(sparse);
    }

    /// Whether the table, or the list of keys with a time to live, has room
    /// for many more keys than it holds (see `store::shrink_if_sparse`).
    pub fn is_sparse(&self) -> bool {
        (!self.is_growing() && sparse(self.len(), self.capacity())) || self.ttl.is_sparse(sparse)
    }

    /// How many buckets there are: the positions [`ShardMap::bucket`]
    /// takes, the table's and then, while it grows, those of the table it
    /// grows from. An entry stays where it is while keys come and go, until
    /// the table grows or shrinks, or a step of a growth moves it: a walk
    /// that lets go of the lock between parts compares
    /// [`ShardMap::layout`].
    #[inline]
    pub fn buckets(&self) -> usize {
        self.table.num_buckets() + self.old_buckets()
    }

    /// The buckets of the table being grown from: none when the table is
    /// not growing (an empty hashbrown table says it has one).
    #[inline]
    fn old_buckets(&self) -> usize {
        if self.is_growing() {
            self.old.num_buckets()
        } else {
            0
        }
    }

    /// The slot in bucket `index` of [`ShardMap::buckets`].
    #[inline]
    fn slot_at(&self, index: usize) -> Option<&Slot> {
        let new = self.table.num_buckets();
        if index < new {
            self.table.get_bucket(index)
        } else if index - new < self.old_buckets() {
            self.old.get_bucket(index - new)
        } else {
            None
        }
    }

    /// The key and value in bucket `index`, if it holds one.
    #[inline]
    pub fn bucket(&self, index: usize) -> Option<(&[u8], ValueRef<'_>)> {
        self.slot_at(index).map(|s| {
            let e = s.entry();
            (e.key(), e.value())
        })
    }

    /// The entry in bucket `index`, if it holds one: with a bucket picked
    /// at random, and another until one is full, a key picked at random
    /// with every key as likely as any other (see `eviction`).
    #[inline]
    pub fn entry_at(&self, index: usize) -> Option<EntryView<'_>> {
        self.slot_at(index).map(|s| EntryView {
            entry: s.entry(),
            _map: std::marker::PhantomData,
        })
    }

    /// Up to `count` entries from the buckets at and after `start` (taken
    /// modulo the bucket count), wrapping round once. Costs about `count`
    /// divided by how full the table is. Not a sample of keys each as
    /// likely as the next: a key after a run of empty buckets comes first
    /// far more often than one after a full one.
    pub fn sample_from(&self, start: usize, count: usize) -> impl Iterator<Item = EntryView<'_>> {
        let buckets = self.buckets();
        let start = if buckets == 0 { 0 } else { start % buckets };
        (0..buckets)
            .filter_map(move |step| {
                self.slot_at((start + step) % buckets).map(|s| EntryView {
                    entry: s.entry(),
                    _map: std::marker::PhantomData,
                })
            })
            .take(if self.is_empty() { 0 } else { count })
    }

    /// One key picked at random, `pick()` giving the random numbers. A
    /// random bucket is tried until one is full, as Redis's
    /// `dictGetFairRandomKey` does; a table that is very sparse is walked
    /// from a random bucket.
    pub fn random_entry(&self, mut pick: impl FnMut() -> usize) -> Option<(&[u8], ValueRef<'_>)> {
        if self.is_empty() {
            return None;
        }
        let buckets = self.buckets();
        for _ in 0..64 {
            if let Some(found) = self.bucket(pick() % buckets) {
                return Some(found);
            }
        }
        self.sample_from(pick(), 1)
            .next()
            .map(|e| (e.key(), e.value()))
    }

    /// Whether the entry in bucket `index`, or its string's bytes, is in a
    /// slab compaction is emptying (`regions` is
    /// `value_slab::regions_snapshot`).
    pub fn evacuating_at(&self, index: usize, regions: &[usize]) -> bool {
        let Some(slot) = self.slot_at(index) else {
            return false;
        };
        let entry = slot.entry();
        (crate::value_slab::class_of(entry.size()).is_some()
            && crate::value_slab::is_evacuating(entry.base(), regions))
            || matches!(entry.value(), ValueRef::String(s)
                if s.shared().is_some_and(|b| crate::value_slab::is_evacuating(b.block(), regions)))
    }

    /// The buckets from `from` to `to` (not included) whose entry, or its
    /// string's bytes, is in a slab compaction is emptying: [`evacuating_at`]
    /// of each, with the entries' first lines asked for a few buckets ahead.
    /// Each entry is somewhere else in memory, and the shard is locked for
    /// reading while this runs: one after the other, their misses held the
    /// lock about 50 us for 512 buckets.
    ///
    /// [`evacuating_at`]: ShardMap::evacuating_at
    pub fn evacuating_in(&self, from: usize, to: usize, regions: &[usize]) -> Vec<usize> {
        const AHEAD: usize = 8;
        let to = to.min(self.buckets());
        for index in from..(from + AHEAD).min(to) {
            if let Some(slot) = self.slot_at(index) {
                slot.prefetch_header();
            }
        }
        let mut found = Vec::new();
        for index in from..to {
            if index + AHEAD < to
                && let Some(slot) = self.slot_at(index + AHEAD)
            {
                slot.prefetch_header();
            }
            if self.evacuating_at(index, regions) {
                found.push(index);
            }
        }
        found
    }

    /// Ask for the first lines of the entry in bucket `index`, if there is
    /// one: compaction's next copy.
    #[inline]
    pub fn prefetch_bucket(&self, index: usize) {
        if let Some(slot) = self.slot_at(index) {
            slot.prefetch();
        }
    }

    /// Move the entry in bucket `index`, or its string's bytes, out of a
    /// slab compaction is emptying, if the table's is its only hold.
    pub fn relocate_at(&mut self, index: usize, regions: &[usize]) -> Relocated {
        let new = self.table.num_buckets();
        let found = if index < new {
            self.table.get_bucket_mut(index)
        } else if index - new < self.old_buckets() {
            self.old.get_bucket_mut(index - new)
        } else {
            None
        };
        let Some(slot) = found else {
            return Relocated::Stays;
        };
        let entry = slot.entry();
        // SAFETY: the table is locked for writing (`&mut self`), and holds
        // the entry.
        match unsafe { entry.relocate(regions) } {
            Relocation::Stays => Relocated::Stays,
            Relocation::Held => Relocated::Held,
            Relocation::Moved(moving) => {
                let to = moving.to();
                // What can panic runs before the store changes: unwinding
                // then frees only the copy (`Moving`).
                fail_point(1);
                // The hash is not stored; the fragment is in the slot.
                let moved = slot.moved_to(to);
                // The position moved with the block.
                let listed = if to.has_ttl() {
                    fail_point(2);
                    let listed = self.ttl.at_mut(to.pos());
                    debug_assert!(*listed == entry);
                    Some(listed)
                } else {
                    None
                };
                *slot = moved;
                if let Some(listed) = listed {
                    *listed = to;
                }
                Relocated::Moved(Leftover::Entry(moving.done()))
            }
            Relocation::BytesMoved(old) => Relocated::Moved(Leftover::Bytes(old)),
        }
    }
}

// For tests: the point in `ShardMap::relocate_at` at which to panic.
#[cfg(test)]
thread_local! {
    static FAIL_AT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn fail_point(at: u8) {
    if FAIL_AT.with(|f| f.get()) == at {
        panic!("injected at {at}");
    }
}

#[cfg(not(test))]
#[inline(always)]
fn fail_point(_: u8) {}

/// Keys moved from the table being grown from by each write to the shard
/// while it grows (see [`ShardMap::make_room`]). Any number from two up
/// empties the old table before the new one, twice its size, is full.
const GROWTH_STEP: usize = 16;

/// A table with fewer keys than this grows in one step, as hashbrown grows
/// it: under 0.2 ms.
const GROW_IN_STEPS_FROM: usize = 2048;

/// Tables and lists with room for fewer keys than this are left as they
/// are: what shrinking one would give back is not worth it.
const SHRINK_MIN_CAPACITY: usize = 256;

/// Whether room for `capacity` is sparse for `len` keys: no more than 4/9
/// of it used (see `store::shrink_if_sparse`).
#[inline]
pub(super) fn sparse(len: usize, capacity: usize) -> bool {
    capacity >= SHRINK_MIN_CAPACITY && len * 9 <= capacity * 4
}

/// One entry of a locked shard, as [`ShardMap::entries`] gives them.
pub struct EntryView<'a> {
    entry: Entry,
    _map: std::marker::PhantomData<&'a ShardMap>,
}

impl<'a> EntryView<'a> {
    pub fn key(&self) -> &'a [u8] {
        self.entry.key()
    }

    pub fn value(&self) -> ValueRef<'a> {
        self.entry.value()
    }

    /// When the key's time to live ends, if it has one.
    pub fn expires(&self) -> Option<u64> {
        self.entry.expires()
    }

    /// The key's access word (`eviction::Access`).
    pub fn access(&self) -> u32 {
        self.entry.access()
    }

    /// Which entry this is, as long as it lives: a number no other entry
    /// has meanwhile. A place found holding the same number later holds
    /// the same entry, or one made since in the same memory; either is
    /// that place's key now.
    #[inline]
    pub fn id(&self) -> usize {
        self.entry.base() as usize
    }

    /// A hold on the entry, to read it once the shard is let go of.
    pub fn hold(&self) -> EntryRef {
        // SAFETY: the shard is locked for as long as `'a`.
        unsafe { EntryRef::new(self.entry) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> KiviDBValue {
        KiviDBValue::String(SharedBytes::from(v))
    }

    fn text(v: Option<ValueRef<'_>>) -> Option<Vec<u8>> {
        match v {
            Some(ValueRef::String(b)) => Some(b.to_vec()),
            _ => None,
        }
    }

    /// What a relocation test does with the key once it has moved.
    enum Then {
        /// Drops the map, as a shard's going does.
        DropMap,
        /// Takes the key out.
        Remove,
        /// Lets its time to live end.
        Expire,
    }

    /// A panic in `relocate_at` at `point` (1: where the slot's new address
    /// is made, 2: where the entry's place in the TTL list is found) leaves
    /// the store as it was, frees only the copy, and the entry moves later;
    /// the moved key is then let go of as `then` says.
    fn relocation_unwinds_at(point: u8, ttl: bool, then: Then) {
        use crate::value_slab;
        // These share their slab class, and a pass of compaction's tests
        // would empty their slab: one at a time.
        let _one = value_slab::EVACUATION_TESTS.lock();
        let key = |i: usize| format!("{i:0>3000}");
        let expires = ttl.then_some(u64::MAX / 2);
        // Made on a thread that ends, so that its slabs are nobody's.
        let mut map = std::thread::spawn(move || {
            let mut map = ShardMap::new();
            for i in 0..64 {
                map.insert_str_expiring(key(i), b"v", expires);
            }
            map
        })
        .join()
        .unwrap();
        // One key left: its slab holds nothing else, and empties when its
        // block is freed.
        for i in 1..64 {
            map.remove(&key(i));
        }
        let listed = |map: &ShardMap, e: Entry| {
            if ttl {
                map.ttl.len() == 1 && map.ttl.at(e.pos()) == e
            } else {
                map.ttl.is_empty()
            }
        };
        let index = (0..map.buckets())
            .find(|&i| map.table.get_bucket(i).is_some())
            .unwrap();
        let old = map.table.get_bucket(index).unwrap().entry();
        assert_eq!(old.has_ttl(), ttl);
        let generation = value_slab::generation_of(old.base());
        // Only this key's slab, not every test's.
        let evacuating = value_slab::evacuate_only(old.base()).expect("a slab nobody owns");
        // A freed old block shows below as its slab emptying, which needs
        // the slab to hold only this key, not another test's block too.
        assert_eq!(evacuating.live(), 1, "the key's slab is shared");
        let regions = value_slab::regions_snapshot();
        assert!(map.evacuating_at(index, &regions));

        FAIL_AT.with(|f| f.set(point));
        let relocated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            map.relocate_at(index, &regions)
        }));
        FAIL_AT.with(|f| f.set(0));
        assert!(relocated.is_err());
        if value_slab::generation_of(old.base()) != generation {
            // The store points to freed memory: never drop it.
            std::mem::forget(map);
            drop(evacuating);
            panic!("the old block was freed while the store still points to it");
        }
        assert!(map.table.get_bucket(index).unwrap().entry() == old);
        assert!(listed(&map, old));
        assert_eq!(text(map.get(&key(0))), Some(b"v".to_vec()));
        assert_eq!(map.expires(&key(0)), expires);

        // Nothing was left half done: the move goes through now.
        match map.relocate_at(index, &regions) {
            Relocated::Moved(left) => drop(left),
            _ => panic!("not moved"),
        }
        let new = map.table.get_bucket(index).unwrap().entry();
        assert!(new != old && listed(&map, new));
        // Its slab emptied when the old block was freed.
        assert_ne!(value_slab::generation_of(old.base()), generation);
        drop(evacuating);
        assert_eq!(text(map.get(&key(0))), Some(b"v".to_vec()));
        assert_eq!(map.expires(&key(0)), expires);
        // Nothing else moves: the same bucket has nothing left to move.
        assert!(matches!(map.relocate_at(index, &regions), Relocated::Stays));

        // The moved key goes the usual ways, its value dropped once, from
        // the copy.
        match then {
            Then::DropMap => drop(map),
            Then::Remove => {
                let taken = map.remove(&key(0)).expect("the key");
                drop(taken);
                assert!(map.is_empty() && map.ttl.is_empty());
                assert!(map.get(&key(0)).is_none());
                drop(map);
            }
            Then::Expire => {
                let (due, _) = map.due(u64::MAX, 8);
                assert_eq!(due.len(), 1);
                assert!(map.remove_if_due(&key(0), u64::MAX).is_some());
                assert!(map.is_empty() && map.ttl.is_empty());
                drop(map);
            }
        }
    }

    #[test]
    fn a_panic_making_the_moved_slot_frees_only_the_copy() {
        relocation_unwinds_at(1, true, Then::DropMap);
    }

    #[test]
    fn a_panic_finding_the_ttl_place_frees_only_the_copy() {
        relocation_unwinds_at(2, true, Then::Expire);
    }

    #[test]
    fn a_panic_moving_a_key_with_no_ttl_frees_only_the_copy() {
        relocation_unwinds_at(1, false, Then::Remove);
    }

    /// A whole scan of `map` in pages of `want`, `change` run on it after
    /// each page: every key seen, as often as it was.
    fn scan_all(
        map: &mut ShardMap,
        want: usize,
        mut change: impl FnMut(&mut ShardMap, usize),
    ) -> std::collections::HashMap<Vec<u8>, usize> {
        let mut seen = std::collections::HashMap::new();
        let mut from = 0;
        let mut pages = 0;
        loop {
            let next = map.scan(from, want, |entry| {
                *seen.entry(entry.key().to_vec()).or_insert(0) += 1;
            });
            pages += 1;
            assert!(pages <= SCAN_POSITIONS as usize + 1, "a scan ends");
            if next == 0 {
                return seen;
            }
            assert!(next > from, "a scan goes forward");
            from = next;
            change(map, pages);
        }
    }

    #[test]
    fn a_scan_sees_every_key_once_when_nothing_changes() {
        let mut map = ShardMap::new();
        for i in 0..20_000 {
            map.insert_str(format!("k:{i}").as_bytes(), "v");
        }
        let seen = scan_all(&mut map, 100, |_, _| {});
        assert_eq!(seen.len(), 20_000);
        assert!(seen.values().all(|&n| n == 1));
        // Pages of about the size asked for.
        let mut pages = 0;
        let mut from = 0;
        loop {
            let mut n = 0;
            from = map.scan(from, 1000, |_| n += 1);
            assert!((1000..1100).contains(&n) || from == 0, "a page of {n}");
            pages += 1;
            if from == 0 {
                break;
            }
        }
        assert!((18..=21).contains(&pages), "{pages} pages");
        assert_eq!(ShardMap::new().scan(0, 10, |_| panic!("empty")), 0);
        assert_eq!(map.scan(SCAN_POSITIONS, 10, |_| panic!("past the end")), 0);
    }

    /// Keys there all along are seen while the table grows many times over,
    /// loses keys, and is shrunk.
    #[test]
    fn a_scan_sees_every_lasting_key_while_the_table_grows_and_shrinks() {
        let mut map = ShardMap::new();
        for i in 0..500 {
            map.insert_str(format!("lasting:{i}").as_bytes(), "v");
            map.insert_str(format!("doomed:{i}").as_bytes(), "v");
        }
        let capacity = map.capacity();
        let mut most = capacity;
        let mut shrunk = false;
        let mut added = 0usize;
        let seen = scan_all(&mut map, 10, |map, page| {
            most = most.max(map.capacity());
            for _ in 0..200 {
                map.insert_str(format!("new:{added}").as_bytes(), "v");
                added += 1;
            }
            if page <= 50 {
                for i in (page - 1) * 10..page * 10 {
                    assert!(map.remove(format!("doomed:{i}").as_str()).is_some());
                }
            }
            if page % 40 == 0 {
                // Take most of the new keys out, and give the room back.
                for i in added.saturating_sub(5000)..added {
                    map.remove(format!("new:{i}").as_str());
                }
                let before = map.capacity();
                map.shrink_to(map.len());
                shrunk |= map.capacity() < before;
            }
        });
        assert!(most > 4 * capacity, "the table grew");
        assert!(shrunk, "the table shrank");
        for i in 0..500 {
            assert!(
                seen.contains_key(format!("lasting:{i}").as_bytes()),
                "lasting:{i} was missed"
            );
        }
    }

    #[test]
    fn insert_get_remove() {
        let mut map = ShardMap::new();
        assert!(map.insert("a", s("1")).is_none());
        assert_eq!(text(map.get("a")), Some(b"1".to_vec()));
        assert!(map.contains_key("a"));
        let old = map.insert(crate::store::Key::from("a"), s("2"));
        assert!(
            matches!(old.map(Taken::into_value), Some(KiviDBValue::String(b)) if &b[..] == b"1")
        );
        assert_eq!(map.len(), 1);
        assert!(map.remove("a").is_some());
        assert!(map.remove("a").is_none());
        assert!(map.is_empty());
    }

    /// The table takes and gives back keys as bytes, any bytes: what the
    /// commands will hand it once they take keys that are not text (#437).
    #[test]
    fn keys_are_bytes_of_any_kind() {
        let mut map = ShardMap::new();
        let keys: Vec<Vec<u8>> = vec![
            b"\xff\x00k".to_vec(),
            b"\xfe".to_vec(),
            vec![0xe7; 24],
            (0u8..=255).collect(),
            b"plain".to_vec(),
        ];
        for (i, key) in keys.iter().enumerate() {
            assert!(map.insert_str(key.as_slice(), i.to_string()).is_none());
        }
        assert_eq!(map.len(), keys.len());
        for (i, key) in keys.iter().enumerate() {
            match map.get(key.as_slice()) {
                Some(ValueRef::String(v)) => assert_eq!(&v[..], i.to_string().as_bytes()),
                _ => panic!("{key:?} not found"),
            }
        }
        // `\xfe` and `\xff\x00k` are two keys, not one U+FFFD.
        assert!(map.get(&b"\xef\xbf\xbd"[..]).is_none());
        let mut seen: Vec<Vec<u8>> = map.keys().map(<[u8]>::to_vec).collect();
        seen.sort();
        let mut want = keys.clone();
        want.sort();
        assert_eq!(seen, want);
        assert!(map.set_expires(&b"\xfe"[..], u64::MAX - 1));
        assert_eq!(
            map.expiring().map(|(k, _)| k.to_vec()).collect::<Vec<_>>(),
            vec![b"\xfe".to_vec()]
        );
        let taken = map.remove(&keys[0][..]).expect("there");
        assert_eq!(taken.key(), &keys[0][..]);
        assert!(map.get(&keys[0][..]).is_none());
    }

    #[test]
    fn keys_survive_growth_and_shrinking() {
        let mut map = ShardMap::new();
        for i in 0..10_000 {
            map.insert(format!("key-{i}"), s(&i.to_string()));
        }
        for i in (0..10_000).filter(|i| i % 10 != 0) {
            map.remove(&format!("key-{i}"));
        }
        map.shrink_to(1_000);
        assert_eq!(map.len(), 1_000);
        assert_eq!(text(map.get("key-990")), Some(b"990".to_vec()));
        assert!(map.get("key-991").is_none());
    }

    #[test]
    fn buckets_reach_every_key_once() {
        let mut map = ShardMap::new();
        for i in 0..1_000 {
            map.insert(format!("k{i}"), s("v"));
        }
        let mut seen: Vec<Vec<u8>> = (0..map.buckets())
            .filter_map(|i| map.bucket(i).map(|(k, _)| k.to_vec()))
            .collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 1_000);
        assert!(map.bucket(map.buckets()).is_none());
    }

    #[test]
    fn samples_and_random_keys() {
        let mut map = ShardMap::new();
        assert!(map.random_entry(|| 7).is_none());
        assert_eq!(map.sample_from(3, 5).count(), 0);
        for i in 0..100 {
            map.insert(format!("k{i}"), s("v"));
        }
        assert_eq!(map.sample_from(12345, 5).count(), 5);
        assert_eq!(map.sample_from(12345, 500).count(), 100);
        let mut n = 0usize;
        let (key, _) = map
            .random_entry(|| {
                n += 7919;
                n
            })
            .unwrap();
        assert!(map.contains_key(key));
        // A table nearly empty after a big delete still gives its key.
        for i in 1..100 {
            map.remove(&format!("k{i}"));
        }
        assert_eq!(map.random_entry(|| 0).map(|(k, _)| k), Some(&b"k0"[..]));
    }

    #[test]
    fn a_key_past_its_time_is_absent_until_taken_out() {
        let mut map = ShardMap::new();
        map.insert_expiring("k", s("100"), Some(1));
        assert!(map.get("k").is_none());
        assert!(map.get_expiring("k").is_none());
        assert!(!map.contains_key("k"));
        assert!(map.get_ref("k").is_none());
        assert!(!map.set_expires("k", u64::MAX));
        assert!(!map.persist("k"));
        // Expiry still sees it, by its own time.
        assert_eq!(map.expires("k"), Some(1));
        assert_eq!(map.ttl_len(), 1);

        // A write puts a new key: the old time to live goes with the old value.
        assert!(map.insert("k", s("1")).is_some());
        assert_eq!(text(map.get("k")), Some(b"1".to_vec()));
        assert_eq!(map.expires("k"), None);
        assert_eq!(map.ttl_len(), 0);
        assert!(map.remove_if_due("k", u64::MAX).is_none());

        // A live key keeps its time to live through a write.
        map.insert_expiring("live", s("a"), Some(u64::MAX - 1));
        map.insert("live", s("b"));
        assert_eq!(map.expires("live"), Some(u64::MAX - 1));
    }

    /// Nothing writes the access word of a key past its time: not the
    /// lookup `GET` and `MGET` make, not `use_key` (a command refused, a key
    /// named twice), not `touch` (a command whose use is written after it).
    #[test]
    fn a_key_past_its_time_is_not_used() {
        let mut map = ShardMap::new();
        map.insert_expiring("dead", s("1"), Some(1));
        map.insert_expiring("live", s("1"), Some(u64::MAX - 1));
        let dead = map.access("dead").unwrap();
        // A use writes this word (an LRU clock).
        let stamp = 12345;
        assert_ne!(dead, stamp);
        let _scope = super::access::AccessScope::open(stamp);

        assert!(matches!(map.lookup("dead"), Lookup::Past));
        map.use_key(b"dead");
        map.touch(b"dead", |_| stamp);
        assert_eq!(map.access("dead"), Some(dead));

        assert!(matches!(
            map.lookup("live"),
            Lookup::Live(ValueRef::String(_))
        ));
        assert_eq!(map.access("live"), Some(stamp));
        map.touch(b"live", |_| 777);
        assert_eq!(map.access("live"), Some(777));
        map.use_key(b"live");
        assert_eq!(map.access("live"), Some(stamp));
        assert!(matches!(map.lookup("none"), Lookup::Absent));
    }

    #[test]
    fn a_key_past_its_time_is_not_written_in_place() {
        let mut map = ShardMap::new();
        // SET k v KEEPTTL, INCR, SETRANGE's fallback: a new key, no time to live.
        map.insert_str_expiring("set", "5", Some(1));
        assert!(matches!(map.set_str("set", "6"), Written::Replaced(_)));
        assert_eq!(text(map.get("set")), Some(b"6".to_vec()));
        assert_eq!(map.expires("set"), None);
        // APPEND and SETRANGE leave it to the caller's write of a new value.
        map.insert_str_expiring("app", "5", Some(1));
        assert_eq!(map.append_in_place("app", b"6"), None);
        map.insert_str_expiring("rng", "5", Some(1));
        assert_eq!(map.set_range_in_place("rng", 0, b"6"), None);
        assert_eq!(
            map.take_left(),
            vec![(ToSay::PastItsTime, crate::store::Key::from("set"))]
        );
        // A live key is still written in place.
        map.insert_str_expiring("live", "5", Some(u64::MAX - 1));
        assert!(matches!(map.set_str("live", "6"), Written::InPlace));
        assert_eq!(map.append_in_place("live", b"7"), Some(2));
        assert_eq!(map.expires("live"), Some(u64::MAX - 1));
    }

    #[test]
    fn a_write_lock_sees_one_time() {
        let shard = crate::store::ShardLock::default();
        let soon = crate::commands::helpers::now_ms() + 200;
        shard.write().insert_expiring("k", s("100"), Some(soon));
        let mut map = shard.write();
        // Live when the lock first looked at the time: the write that
        // follows keeps it, however long after.
        assert!(map.get("k").is_some());
        std::thread::sleep(std::time::Duration::from_millis(250));
        map.insert("k", s("101"));
        assert_eq!(map.expires("k"), Some(soon));
        drop(map);
        assert!(shard.read().get("k").is_none());
    }

    #[test]
    fn a_write_lock_reads_the_clock_only_for_a_key_with_a_time_to_live() {
        let shard = crate::store::ShardLock::default();
        shard.write().insert("plain", s("1"));
        shard
            .write()
            .insert_expiring("ttl", s("1"), Some(u64::MAX - 1));
        let mut map = shard.write();
        assert!(map.get("plain").is_some());
        map.insert("plain", s("2"));
        assert_eq!(map.now.load(Ordering::Relaxed), PENDING, "no clock read");
        assert!(map.get("ttl").is_some());
        let t = map.now.load(Ordering::Relaxed);
        assert!(t != PENDING && t != UNPINNED, "read once, and kept");
        drop(map);
        assert_eq!(shard.read().now.load(Ordering::Relaxed), UNPINNED);
    }

    #[test]
    fn get_or_insert_with_makes_once() {
        let mut map = ShardMap::new();
        let mut made = 0;
        for _ in 0..3 {
            map.get_or_insert_with("a", || {
                made += 1;
                s("x")
            });
        }
        assert_eq!(made, 1);
        assert_eq!(map.len(), 1);
    }

    /// With every key's time ending well after the horizon, a pass for due
    /// keys looks at none of them once the shard has been swept (#618):
    /// before, every pass looked at every one.
    #[test]
    fn a_pass_looks_at_no_key_that_cannot_be_due() {
        let mut map = ShardMap::new();
        let now = crate::commands::helpers::now_ms();
        for i in 0..1000 {
            map.insert_expiring(format!("k{i}"), s("v"), Some(now + 3_600_000));
        }
        // Not swept yet: everything is looked at.
        assert_eq!(map.due(now, usize::MAX), (vec![], 1000));
        assert!(map.ttl_sweep_due(now));
        while !map.sweep_ttl(now, 64) {}
        assert!(!map.ttl_sweep_due(now));
        assert_eq!(map.due(now, usize::MAX), (vec![], 0));
        // A key given a short time goes where a pass sees it, whichever way
        // it is given.
        map.insert_expiring("new", s("v"), Some(now + 5));
        map.set_expires("k1", now + 5);
        map.set_str_expiring("k2", "w", Some(now + 5));
        map.insert_expiring("k3", s("w"), Some(now + 5));
        map.ttl.check();
        let (mut due, looked) = map.due(now + 10, usize::MAX);
        due.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let due: Vec<&[u8]> = due.iter().map(|k| k.as_bytes()).collect();
        assert_eq!(due, [&b"k1"[..], b"k2", b"k3", b"new"]);
        assert_eq!(looked, 4);
        // Past the horizon, with no sweep since, a pass looks at them all.
        let later = now + super::super::ttl_list::HORIZON_MS + 1;
        assert_eq!(map.due(later, usize::MAX).1, 1001);
        assert!(map.ttl_sweep_due(later));
    }

    /// Whatever writes do between the steps of a sweep, a pass finds every
    /// key past its time and no other, and every entry knows its place.
    #[test]
    fn a_pass_finds_exactly_the_due_keys_whatever_writes_do_meanwhile() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut rand = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        // Far past the clock, so that no write takes a key for expired.
        let mut now = crate::commands::helpers::now_ms() + 1_000_000_000;
        let mut map = ShardMap::new();
        // What the table says, not the list under test: every key with a
        // time to live and when it ends.
        let timed = |map: &ShardMap| -> Vec<(Vec<u8>, u64)> {
            let mut timed: Vec<(Vec<u8>, u64)> = map
                .entries()
                .filter_map(|e| e.expires().map(|at| (e.key().to_vec(), at)))
                .collect();
            timed.sort();
            timed
        };
        for step in 0..80_000 {
            let key = format!("k{}", rand(300));
            let at = now + rand(60_000);
            match rand(16) {
                0 => {
                    map.set_str_expiring(&key, "v", Some(at));
                }
                1 => {
                    map.insert_expiring(&key, s("v"), Some(at));
                }
                2 => {
                    map.set_str(&key, "w");
                }
                3 => {
                    map.insert(&key, s("w"));
                }
                4 => {
                    map.set_str_expiring(&key, "v", None);
                }
                5 => {
                    map.remove(&key);
                }
                6 => {
                    map.set_expires(&key, at);
                }
                7 => {
                    map.persist(&key);
                }
                8 | 9 => {
                    map.sweep_ttl(now, 1 + rand(40) as usize);
                }
                10 => now += rand(4_000),
                // The clock stepped back, now and then by much more than a
                // horizon.
                11 => {
                    let most = if rand(20) == 0 { 600_000 } else { 4_000 };
                    now -= rand(most);
                }
                12 => {
                    // A key taken out and put back, as `MOVE` does.
                    if let Some(taken) = map.remove(&key) {
                        assert!(map.put_back(taken).is_ok_and(|old| old.is_none()));
                    }
                }
                13 => {
                    // A new time given to an entry someone else holds: a
                    // new entry takes its place.
                    let held = map.get_ref(&key);
                    map.set_expires(&key, at);
                    drop(held);
                }
                14 => {
                    // The sweep under way finished, then one from now if
                    // that is due: none is due after it, wherever the clock
                    // has gone meanwhile.
                    while !map.sweep_ttl(now, 64) {}
                    if map.ttl_sweep_due(now) {
                        while !map.sweep_ttl(now, 64) {}
                    }
                    assert!(!map.ttl_sweep_due(now), "step {step}");
                }
                _ => {
                    // A key past its time, taken out as expiry takes one.
                    let past = timed(&map)
                        .into_iter()
                        .find(|&(_, at)| at < now)
                        .map(|(k, _)| k);
                    if let Some(k) = past {
                        assert!(map.remove_if_due(&k, now).is_some());
                    }
                }
            }
            map.ttl.check();
            if step % 50 == 0 {
                let probe = now + rand(30_000);
                let table = timed(&map);
                let want: Vec<Vec<u8>> = table
                    .iter()
                    .filter(|&&(_, at)| at < probe)
                    .map(|(k, _)| k.clone())
                    .collect();
                let mut got: Vec<Vec<u8>> = map
                    .due(probe, usize::MAX)
                    .0
                    .iter()
                    .map(|k| k.as_bytes().to_vec())
                    .collect();
                got.sort();
                assert_eq!(got, want, "step {step}");
                // The list holds every key with a time to live, and nothing
                // else.
                let mut listed: Vec<(Vec<u8>, u64)> =
                    map.expiring().map(|(k, at)| (k.to_vec(), at)).collect();
                listed.sort();
                assert_eq!(listed, table, "step {step}");
                // And the sum of their times, kept as they changed, is the
                // table's (#622).
                let sum: u128 = table.iter().map(|&(_, at)| u128::from(at)).sum();
                assert_eq!(map.ttl_deadline_sum(), sum, "step {step}");
                assert_eq!(map.ttl_len(), table.len(), "step {step}");
            }
            if step % 20_000 == 19_999 {
                // Everything taken out at once, as `FLUSHDB` does.
                map.clear();
                assert_eq!((map.ttl_len(), map.ttl_deadline_sum()), (0, 0));
            }
        }
        // The near part is no bigger than what can come due before the
        // horizon, once a sweep is done: the one under way, then one from now.
        while !map.sweep_ttl(now, 7) {}
        while !map.sweep_ttl(now, 7) {}
        map.ttl.check();
        let soon = timed(&map)
            .iter()
            .filter(|&&(_, at)| at < now + super::super::ttl_list::HORIZON_MS)
            .count();
        assert_eq!(map.ttl.near_len(), soon);
    }

    /// The sum of the times to live is kept through every way a key gets,
    /// changes and loses one: what `avg_ttl` is made of (#622).
    #[test]
    fn the_sum_of_the_times_follows_every_change() {
        let mut map = ShardMap::new();
        let now = crate::commands::helpers::now_ms() + 1_000_000_000;
        let sum = |map: &ShardMap| {
            map.ttl.check();
            map.ttl_deadline_sum()
        };
        assert_eq!(sum(&map), 0);
        map.insert_expiring("a", s("v"), Some(now + 100));
        map.set_str_expiring("b", "v", Some(now + 200));
        map.set_str("c", "v");
        assert_eq!(sum(&map), u128::from(2 * now + 300));
        // Written over in place with a new time, and with none given (the
        // time stays).
        map.set_str_expiring("b", "w", Some(now + 250));
        assert_eq!(sum(&map), u128::from(2 * now + 350));
        map.set_str("b", "y");
        assert_eq!(sum(&map), u128::from(2 * now + 350));
        // A new time, in place and on an entry held elsewhere.
        assert!(map.set_expires("a", now + 50_000));
        let held = map.get_ref("a");
        assert!(map.set_expires("a", now + 60_000));
        drop(held);
        assert_eq!(sum(&map), u128::from(2 * now + 60_250));
        // A key without one given one, and one losing it.
        assert!(map.set_expires("c", now + 1));
        assert!(map.persist("a"));
        assert_eq!(sum(&map), u128::from(2 * now + 251));
        // Written over with its time taken away.
        map.set_str_expiring("b", "x", None);
        assert_eq!(sum(&map), u128::from(now + 1));
        // Taken out and put back, as `MOVE` does.
        let taken = map.remove("c").unwrap();
        assert_eq!(sum(&map), 0);
        assert!(map.put_back(taken).is_ok());
        assert_eq!(sum(&map), u128::from(now + 1));
        // A sweep moves entries between the lists and leaves the sum.
        for i in 0..100u64 {
            map.insert_expiring(format!("k{i}"), s("v"), Some(now + i * 1_000));
        }
        let before = sum(&map);
        while !map.sweep_ttl(now, 7) {}
        assert_eq!(sum(&map), before);
        // Times up to the largest there is do not overflow it.
        map.insert_expiring("max", s("v"), Some(u64::MAX - 1));
        assert_eq!(sum(&map), before + u128::from(u64::MAX - 1));
        map.clear();
        assert_eq!(sum(&map), 0);
    }

    /// A clock stepped forward, a sweep, then the clock stepped back: the
    /// next sweep brings the horizon back, and the keys that cannot be due
    /// soon go back to the far list, rather than every pass looking at them
    /// until the clock has caught up.
    #[test]
    fn a_clock_stepped_back_does_not_leave_every_key_near() {
        use super::super::ttl_list::HORIZON_MS;
        let mut map = ShardMap::new();
        let now = crate::commands::helpers::now_ms() + 1_000_000_000;
        for i in 0..1000 {
            let at = if i % 10 == 0 {
                now + 5_000
            } else {
                now + 3_600_000
            };
            map.insert_expiring(format!("k{i}"), s("v"), Some(at));
        }
        // An hour ahead: every key ends before that horizon.
        let ahead = now + 3_600_000;
        while !map.sweep_ttl(ahead, 64) {}
        assert_eq!(map.ttl.near_len(), 1000);
        // Back: a sweep is due at once, though the horizon is an hour off.
        assert!(map.ttl_sweep_due(now));
        while !map.sweep_ttl(now, 64) {}
        map.ttl.check();
        assert!(!map.ttl_sweep_due(now));
        assert_eq!(map.ttl.near_len(), 100);
        // A pass looks at those alone, and finds exactly the due ones.
        let (due, looked) = map.due(now + 6_000, usize::MAX);
        assert_eq!((due.len(), looked), (100, 100));
        assert!(
            due.iter()
                .all(|k| map.expires(k.as_bytes()) == Some(now + 5_000))
        );
        assert_eq!(map.due(now + 4_000, usize::MAX), (vec![], 100));
        // A key given a time now goes where the new horizon says.
        map.insert_expiring("late", s("v"), Some(now + HORIZON_MS + 1));
        map.insert_expiring("soon", s("v"), Some(now + 1));
        map.ttl.check();
        assert_eq!(map.ttl.near_len(), 101);
        // Past the horizon, a pass looks at everything again.
        let past = now + HORIZON_MS + 2;
        assert_eq!(map.due(past, usize::MAX).0.len(), 102);
    }

    /// Compaction moving an entry while a sweep is under way: its place in
    /// the list goes with it, and the sweep carries on.
    #[test]
    fn an_entry_moved_by_compaction_mid_sweep_keeps_its_place() {
        use crate::value_slab;
        let _one = value_slab::EVACUATION_TESTS.lock();
        let long = |i: usize| format!("{i:0>3000}");
        let now = crate::commands::helpers::now_ms() + 1_000_000_000;
        let late = now + 3_600_000;
        // Made on a thread that ends, so that its slabs are nobody's.
        let mut map = std::thread::spawn(move || {
            let mut map = ShardMap::new();
            for i in 0..64 {
                map.insert_str_expiring(long(i), b"v", Some(late));
            }
            map
        })
        .join()
        .unwrap();
        // One long key left: its slab holds nothing else.
        for i in 1..64 {
            map.remove(&long(i));
        }
        for i in 0..200 {
            let at = if i % 2 == 0 { now + 5_000 } else { late };
            map.insert_expiring(format!("k{i}"), s("v"), Some(at));
        }
        // Part of the way: the long key, first in the far list, is in the
        // part looked at.
        assert!(!map.sweep_ttl(now, 50));
        map.ttl.check();
        let index = (0..map.buckets())
            .find(|&i| {
                map.table
                    .get_bucket(i)
                    .is_some_and(|s| s.entry().has_key(long(0).as_bytes()))
            })
            .unwrap();
        let old = map.table.get_bucket(index).unwrap().entry();
        let evacuating = value_slab::evacuate_only(old.base()).expect("a slab nobody owns");
        assert_eq!(evacuating.live(), 1, "the key's slab is shared");
        let regions = value_slab::regions_snapshot();
        match map.relocate_at(index, &regions) {
            Relocated::Moved(left) => drop(left),
            _ => panic!("not moved"),
        }
        drop(evacuating);
        let new = map.table.get_bucket(index).unwrap().entry();
        assert!(new != old && map.ttl.at(new.pos()) == new);
        map.ttl.check();
        while !map.sweep_ttl(now, 50) {}
        map.ttl.check();
        assert_eq!(map.ttl.len(), 201);
        assert_eq!(map.ttl.near_len(), 100);
        assert_eq!(map.due(now + 6_000, usize::MAX).0.len(), 100);
        assert_eq!(map.expires(&long(0)), Some(late));
        assert_eq!(map.due(late + 1, usize::MAX).0.len(), 201);
    }

    /// A map in the middle of a growth in steps: keys `g0`, `g1`, ... with
    /// their numbers as values, written until one began, and how many.
    fn growing() -> (ShardMap, usize) {
        let mut map = ShardMap::new();
        let mut n = 0;
        while !map.is_growing() {
            map.insert(format!("g{n}"), s(&n.to_string()));
            n += 1;
        }
        assert!(n > GROW_IN_STEPS_FROM);
        (map, n)
    }

    /// The keys still in the table being grown from.
    fn in_old(map: &ShardMap) -> Vec<String> {
        map.old
            .iter()
            .map(|s| String::from_utf8(s.entry().key().to_vec()).unwrap())
            .collect()
    }

    /// Every key is in exactly one of the two tables.
    fn each_key_once(map: &ShardMap) {
        for slot in map.slots() {
            let key = slot.entry().key();
            let hash = map.hash_of(key);
            let new = map.table.find(hash, |s| s.entry().has_key(key)).is_some();
            let old = map.old.find(hash, |s| s.entry().has_key(key)).is_some();
            assert!(new != old, "{key:?} in both tables or neither");
        }
    }

    #[test]
    fn small_tables_grow_in_one_step() {
        let mut map = ShardMap::new();
        for i in 0..GROW_IN_STEPS_FROM {
            map.insert(format!("k{i}"), s("v"));
            assert!(!map.is_growing());
        }
    }

    #[test]
    fn a_large_table_grows_a_few_keys_at_a_time() {
        let (mut map, n) = growing();
        // Only the first step was taken: most keys are still to move.
        let left = map.old.len();
        assert!(left > n / 2, "{left} of {n} left");
        assert_eq!(map.table.len() + left, n);
        assert_eq!(map.len(), n);
        // Twice the buckets.
        assert_eq!(map.table.num_buckets(), 2 * map.old.num_buckets());
        each_key_once(&map);
        for i in 0..n {
            assert_eq!(
                text(map.get(&format!("g{i}"))),
                Some(i.to_string().into_bytes())
            );
        }
        // Each write moves GROWTH_STEP keys on.
        let mut writes = 0;
        while map.is_growing() {
            let left = map.old.len();
            map.insert(format!("more{writes}"), s("m"));
            writes += 1;
            assert!(map.old.len() + GROWTH_STEP <= left || !map.is_growing());
        }
        assert!(writes <= n / GROWTH_STEP + 1, "{writes} writes");
        // The old table's memory is given back.
        assert_eq!(map.old.allocation_size(), 0);
        assert_eq!(map.len(), n + writes);
        for i in 0..n {
            assert_eq!(
                text(map.get(&format!("g{i}"))),
                Some(i.to_string().into_bytes())
            );
        }
    }

    #[test]
    fn a_growth_keeps_each_slot_as_it_was() {
        let slot_of = |map: &ShardMap, key: &str| {
            let hash = map.hash_of(key.as_bytes());
            map.find(hash, key.as_bytes()).unwrap().raw()
        };
        let mut map = ShardMap::new();
        let mut kept = std::collections::HashMap::new();
        // Across several growths, each in steps, and the steps between.
        for n in 0..6 * GROW_IN_STEPS_FROM {
            let key = format!("k{n}");
            map.insert(&key, s("v"));
            kept.insert(key.clone(), slot_of(&map, &key));
        }
        while !map.step_growth(1) {}
        for (key, slot) in &kept {
            // The fragment of its hash (what `SCAN` goes by), its size hint
            // and its address: the slot is the same.
            assert_eq!(slot_of(&map, key), *slot, "{key}");
        }
    }

    #[test]
    fn writes_during_a_growth_find_keys_in_either_table() {
        let (mut map, n) = growing();
        let old = in_old(&map);
        assert!(old.len() > 100);
        let mut len = n;
        // Written over: in place if it can be, and with a new entry.
        map.set_str(&old[0], "a");
        let long = "a value too long to be written in place over the one it had";
        map.insert(&old[1], s(long));
        assert_eq!(text(map.get(&old[0])), Some(b"a".to_vec()));
        assert_eq!(text(map.get(&old[1])), Some(long.as_bytes().to_vec()));
        // Taken out, two ways, and one put back.
        let taken = map.remove(&old[2]).unwrap();
        len -= 1;
        assert!(map.get(&old[2]).is_none());
        assert!(matches!(map.remove_if(&old[3], |_| false), Removal::Kept));
        assert!(matches!(
            map.remove_if(&old[3], |_| true),
            Removal::Taken(_)
        ));
        len -= 1;
        assert!(matches!(map.put_back(taken), Ok(None)));
        len += 1;
        assert!(map.contains_key(&old[2]));
        // A time to live given, and taken away.
        assert!(map.set_expires(&old[4], u64::MAX / 2));
        assert_eq!(map.expires(&old[4]), Some(u64::MAX / 2));
        assert_eq!(map.ttl_len(), 1);
        assert!(map.persist(&old[4]));
        assert_eq!(map.expires(&old[4]), None);
        assert_eq!(map.ttl_len(), 0);
        // A new key with one.
        map.insert_expiring("new", s("n"), Some(u64::MAX / 2));
        len += 1;
        assert_eq!(map.ttl_len(), 1);
        assert_eq!(map.len(), len);
        each_key_once(&map);
        // A key that is in neither table.
        assert!(map.get("nowhere").is_none());
        assert!(map.remove("nowhere").is_none());
        while !map.step_growth(GROWTH_STEP) {}
        assert_eq!(map.len(), len);
        assert_eq!(map.keys().count(), len);
        assert_eq!(text(map.get(&old[0])), Some(b"a".to_vec()));
        assert!(map.get(&old[3]).is_none());
        assert_eq!(map.expires("new"), Some(u64::MAX / 2));
    }

    #[test]
    fn walks_during_a_growth_see_every_key_once() {
        let (map, n) = growing();
        let mut seen: Vec<Vec<u8>> = map.keys().map(<[u8]>::to_vec).collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), n);
        assert_eq!(map.entries().count(), n);
        let mut seen: Vec<Vec<u8>> = (0..map.buckets())
            .filter_map(|i| map.bucket(i).map(|(k, _)| k.to_vec()))
            .collect();
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), n);
        assert!(map.bucket(map.buckets()).is_none());
        assert_eq!(map.sample_from(17, n + 10).count(), n);
        // A random key, from either table.
        let mut x = 1usize;
        let mut picked = std::collections::HashSet::new();
        for _ in 0..2_000 {
            let (key, _) = map
                .random_entry(|| {
                    x = x
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    x >> 16
                })
                .unwrap();
            picked.insert(key.to_vec());
        }
        let olds: std::collections::HashSet<Vec<u8>> =
            in_old(&map).into_iter().map(String::into_bytes).collect();
        assert!(picked.iter().any(|k| olds.contains(k)));
        assert!(picked.iter().any(|k| !olds.contains(k)));
    }

    /// A `SCAN` during a growth sees every key, those still in the table
    /// grown from as well as those moved, once: a growth that stays where
    /// it is, and one that moves on between pages until it is done.
    #[test]
    fn a_scan_during_a_growth_sees_every_key_once() {
        let (mut map, n) = growing();
        let old: Vec<Vec<u8>> = in_old(&map).into_iter().map(String::into_bytes).collect();
        assert!(old.len() > n / 2);
        let seen = scan_all(&mut map, 10, |map, _| assert!(map.is_growing()));
        assert!(map.is_growing(), "the growth stayed partly done");
        assert_eq!(seen.len(), n);
        assert!(seen.values().all(|&times| times == 1));
        assert!(old.iter().all(|key| seen.contains_key(key)));
        // A page that ends at a position with keys in both tables takes all
        // of them: the page sizes add up to the keys.
        let mut from = 0;
        let mut total = 0;
        loop {
            from = map.scan(from, 1, |_| total += 1);
            if from == 0 {
                break;
            }
        }
        assert_eq!(total, n);

        // Writes and steps of the growth between pages move keys from the
        // old table to the new one, each keeping its position: none is
        // missed or seen twice, through to the end of the growth.
        let (mut map, n) = growing();
        let mut added = 0usize;
        let mut grew_through = 0;
        let seen = scan_all(&mut map, 10, |map, _| {
            if map.is_growing() {
                grew_through += 1;
            }
            map.insert(format!("new:{added}"), s("v"));
            added += 1;
            map.step_growth(GROWTH_STEP);
            each_key_once(map);
        });
        assert!(!map.is_growing(), "the growth finished mid-scan");
        assert!(grew_through > 20, "{grew_through} pages while growing");
        for i in 0..n {
            assert_eq!(seen.get(format!("g{i}").as_bytes()), Some(&1), "g{i}");
        }
        assert!(seen.values().all(|&times| times == 1));
    }

    #[test]
    fn the_layout_changes_when_keys_move() {
        let (mut map, _) = growing();
        let before = map.layout();
        map.insert("one more", s("v"));
        assert_ne!(map.layout(), before);
        while !map.step_growth(GROWTH_STEP) {}
        let done = map.layout();
        map.insert("and another", s("v"));
        assert!(map.remove("one more").is_some());
        assert_eq!(map.layout(), done);
    }

    #[test]
    fn a_growing_table_is_not_shrunk_and_can_be_cleared() {
        let (mut map, n) = growing();
        let buckets = map.buckets();
        for i in 0..n - 10 {
            assert!(map.remove(&format!("g{i}")).is_some());
            if map.is_growing() {
                assert!(!map.is_sparse());
                map.shrink_to(20);
                assert_eq!(map.buckets(), buckets);
            }
        }
        while !map.step_growth(GROWTH_STEP) {}
        assert!(map.is_sparse());
        map.shrink_to(20);
        assert!(map.buckets() < buckets);
        assert_eq!(map.len(), 10);

        let (mut map, _) = growing();
        map.clear();
        assert!(map.is_empty());
        assert!(!map.is_growing());
        assert!(map.get("g0").is_none());
        map.insert("g0", s("again"));
        assert_eq!(text(map.get("g0")), Some(b"again".to_vec()));
        // Dropped in the middle of a growth: every entry let go of once.
        let (map, _) = growing();
        assert!(map.is_growing());
        drop(map);
    }
}
