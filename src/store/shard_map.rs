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
//! ([`ShardMap::set_str`], `append_str`, `set_range_in_place`,
//! `edit_str_in_place`): nothing is allocated or freed, and the entry
//! stays where it is. A string of its own is written in its block the
//! same way when the entry is also its bytes' only holder. Anything else,
//! and every entry somebody else holds (a snapshot being written), gets a
//! new entry, as before.

use std::sync::Arc;

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

    /// The room the bytes' block keeps past their length for appends
    /// (`SharedBytes::spare`): 0 for bytes in the entry.
    #[inline]
    pub fn spare(self) -> usize {
        self.shared().map_or(0, SharedBytes::spare)
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
    List(&'a Arc<RwLock<crate::list_values::ListValues>>),
    Hash(&'a Arc<RwLock<HashFields>>),
    Json(&'a Arc<RwLock<crate::json_tree::Value>>),
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

impl Leftover {
    /// The bytes the move copied.
    pub fn bytes(&self) -> usize {
        match self {
            Leftover::Entry(left) => left.size(),
            Leftover::Bytes(bytes) => bytes.heap_size(),
        }
    }
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

/// Why [`ShardMap::append_str`] wrote nothing.
#[derive(Debug, PartialEq, Eq)]
pub enum AppendRefused {
    /// The key holds another type.
    WrongType,
    /// The string would pass the longest allowed.
    TooLong,
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

/// How many entries a `SCAN` of a shard asks for before it visits them
/// ([`ShardMap::scan`]): enough for the first to have arrived when the
/// last is asked for, few enough that what is asked for is not pushed out
/// again before it is read.
const SCAN_BATCH: usize = 16;

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

/// Whether `entry` is past its time. Keys without a time to live never
/// read the clock.
///
/// By the time of the command running on this thread
/// (`script_clock::command`, #632): every lookup and write lock of one
/// command goes by the same time, the one the command read first. A write
/// lock used to keep a time of its own for the same purpose, set when it
/// was taken and cleared when it was let go of: two stores on every write
/// lock, which the command's time made redundant. Outside a command (the
/// background expiry, eviction), each look reads the clock.
#[inline(always)]
fn expired(entry: Entry) -> bool {
    entry.has_ttl()
        && entry
            .expires()
            .is_some_and(crate::commands::helpers::is_past)
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

    /// The keys still to be moved from the table being grown from: none
    /// when the table is not growing.
    #[inline]
    pub fn growth_left(&self) -> usize {
        self.old.len()
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
    ///
    /// The growth, if the table is full, comes first, and the written key
    /// is moved after it: a growth begun here puts every key in the old
    /// table, the written one too, and the write that follows looks in the
    /// new table only. Moved the other way round, a key written over by the
    /// write that began a growth was left in the old table and the write
    /// added it again to the new one: two entries for one key.
    #[cold]
    #[inline(never)]
    fn make_room(&mut self, hash: u64, key: &[u8]) {
        if self.table.len() == self.table.capacity() {
            // Each write moves GROWTH_STEP keys and adds one at most, so
            // the old table is empty long before the new one is full.
            // Should it not be, its keys move now.
            while !self.step_growth(usize::MAX) {}
            self.grow();
        }
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
        debug_assert!(
            self.find_old(hash, key).is_none(),
            "a key about to be written is still in the old table"
        );
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
                // Every bucket was passed and a key is still there. Nothing
                // puts a key in the old table once it is there, so this
                // cannot happen; should it, start over rather than let go
                // of the key.
                if cfg!(debug_assertions) {
                    unreachable!(
                        "{} keys left in the old table after every bucket was passed",
                        self.old.len()
                    );
                }
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
        self.find(hash, key).filter(|s| !expired(s.entry()))
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
                if expired(e) {
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
                let stale = expired(old);
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

    /// Redis's `APPEND`: `extra` added to the end of `key`'s string, the key
    /// made if it is not there (or is past its time). Returns the string's
    /// new length, and what was written: [`Written::Replaced`]'s entry is
    /// best dropped once the shard is unlocked. Refused, with nothing
    /// written, if the key holds another type, or its string would pass
    /// `max_len`.
    ///
    /// Written in place when the table alone holds the entry and, for a
    /// string with a block of its own, the entry alone its bytes (both
    /// counted under this write lock): an inline string that stays in its
    /// slab class, or bytes of their own, in their block if it has room,
    /// else in the block `SharedBytes::append` grows or makes for them,
    /// with room to spare (`shared_bytes::room_for_append`); the entry
    /// stays. Anything else is copied once into a new entry, with the same
    /// room, and the key's time to live kept. An empty `extra` writes
    /// nothing.
    pub fn append_str(
        &mut self,
        key: &(impl AsRef<[u8]> + ?Sized),
        extra: &[u8],
        max_len: usize,
    ) -> Result<(usize, Written), AppendRefused> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        let found = self.find_live(hash, key).map(|slot| slot.entry());
        let old: &[u8] = match found.map(Entry::value) {
            Some(ValueRef::String(bytes)) => bytes.as_bytes(),
            Some(_) => return Err(AppendRefused::WrongType),
            None => &[],
        };
        let len = old.len() + extra.len();
        if let Some(entry) = found {
            if len > max_len.min(crate::shared_bytes::MAX_LEN) {
                return Err(AppendRefused::TooLong);
            }
            let in_place = if extra.is_empty() {
                true
            } else if entry.inline_len().is_some() {
                let fits = entry.fits_in_place(len);
                if fits {
                    // SAFETY: locked for writing, held by the table alone,
                    // and in class.
                    unsafe { entry.write_at_in_place(old.len(), extra) };
                }
                fits
            } else {
                // SAFETY: locked for writing (`&mut self`), the table holds
                // the entry, and the length is in bounds. `old` is not used
                // again: the bytes may move.
                unsafe { entry.append_own(extra) }.is_some()
            };
            if in_place {
                used(entry, hash, self.id());
                return Ok((len, Written::InPlace));
            }
        }
        // `old` is in the entry `put` replaces, which lives until the
        // caller drops what it gives back.
        let written = self.put(key, Ttl::Keep, None, |key, expires| {
            Entry::string_appended(key, old, extra, expires)
        });
        Ok((len, written))
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
        // SAFETY: locked for writing, held by the table alone, and in class.
        unsafe { entry.write_at_in_place(offset, patch) };
        used(entry, hash, map);
        Some(len)
    }

    /// `patch` written over `key`'s string from `offset`, the string grown
    /// with zero bytes to reach it. In place only when the table alone holds
    /// both the entry and, for a string with a block of its own, its bytes,
    /// and the new length keeps their block (`Entry::fits_in_place`). Else
    /// a new string, the old bytes copied once, with the key's time to live
    /// kept. A key not there, or past its time, is an empty string. `None`
    /// leaves everything as it was: the key holds another type, or the
    /// string would pass `MAX_LEN`.
    ///
    /// Nothing but byte copies runs between the first byte written in
    /// place and the last: no allocation, and nothing that can fail.
    pub fn edit_str_in_place(
        &mut self,
        key: &(impl AsRef<[u8]> + ?Sized),
        offset: usize,
        patch: &[u8],
    ) -> Option<Written> {
        let key = key.as_ref();
        let end = offset
            .checked_add(patch.len())
            .filter(|&end| end <= crate::shared_bytes::MAX_LEN)?;
        let hash = self.hash_of(key);
        let found = self.find_live(hash, key).map(|slot| slot.entry());
        let old: &[u8] = match found.map(Entry::value) {
            Some(ValueRef::String(bytes)) => bytes.as_bytes(),
            Some(_) => return None,
            None => &[],
        };
        // Both holds are counted under this write lock, and a new one
        // needs the lock or an existing hold: none can start before the
        // write ends.
        if let Some(entry) = found
            && entry.fits_in_place(old.len().max(end))
        {
            // SAFETY: locked for writing, held by the table alone, any bytes
            // of its own by the entry alone, and in class.
            unsafe { entry.write_at_in_place(offset, patch) };
            used(entry, hash, self.id());
            return Some(Written::InPlace);
        }
        // `old` is in the entry `put` replaces, which lives until the
        // caller drops what it gives back.
        Some(self.put(key, Ttl::Keep, None, |key, expires| {
            Entry::string_written_at(key, old, offset, patch, expires)
        }))
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
        if expired(old) {
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
        if expired(old) {
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
                if !expired(old) {
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
    /// split between two pages, so a page holds `want` entries and those
    /// that share the last one's position (or all that are left, if fewer).
    /// Returns the position to go on from: zero when no entry is at a
    /// position past the page.
    ///
    /// A key's position is the fragment of its hash its slot keeps, so the
    /// pages are found from the slots alone, touching only the entries of
    /// the page. It used to hash every key of the shard, visiting each
    /// entry, for every page (#751).
    ///
    /// While the table grows, both tables are walked: a key still in the
    /// one it grows from is at its position as much as one already moved,
    /// and a move between pages keeps the slot, fragment and all, so a key
    /// moved mid-scan keeps its position too.
    ///
    /// One pass over the slots (#884), where there were two: a count of
    /// each of the 4,096 positions, cleared and summed for every page, and
    /// then the walk. A page that may take every entry left (`want` at
    /// least the shard's length) is visited as the walk finds it. A
    /// smaller page is gathered first (`ScanPage`).
    pub fn scan(&self, from: u64, want: usize, mut visit: impl FnMut(EntryView<'_>)) -> u64 {
        const POSITIONS: usize = SCAN_POSITIONS as usize;
        let Ok(from) = usize::try_from(from) else {
            return 0;
        };
        let len = self.len();
        if from >= POSITIONS || len == 0 {
            return 0;
        }
        if want >= len {
            self.visit_positions(from, &mut visit);
            return 0;
        }
        let want = want.max(1);
        // Keys spread evenly over the positions (fragments of their
        // hashes): the span that holds `want` of them and a margin (a
        // sixteenth, and four times the spread of a count of `want`), so
        // that the guess falls short about never. Only a shard emptied
        // unevenly (the keys before a cursor deleted, say) holds fewer
        // there; then the rest of the shard is gathered too, a second pass.
        let margin = want / 16 + 4 * want.isqrt() + 8;
        let span = want
            .saturating_add(margin)
            .saturating_mul(POSITIONS)
            .div_ceil(len);
        let guess = from.saturating_add(span).min(POSITIONS);
        let mut page = ScanPage::new(from, want, want + margin, guess);
        page.gather(self, from);
        if page.held < want && page.later && guess < POSITIONS {
            page.widen(POSITIONS);
            page.gather(self, guess);
        }
        page.cut();
        // Each entry asked for a few before it is visited: asked for as the
        // walk found them, most would be pushed out again by the time a
        // page of thousands is visited, and asked for a batch at once, the
        // asks wait for each other.
        Self::visit_batch(page.slots(), true, &mut visit);
        if page.later { page.end as u64 } else { 0 }
    }

    /// Every entry at a position from `from` on, given to `visit` as the
    /// walk finds it: the page of a `want` as large as the shard.
    ///
    /// Each entry is somewhere else in memory, and the slot alone says
    /// whether it is in the page: visited one after the other as the walk
    /// found them, the misses on the entries' first lines were most of a
    /// page's time, with the shard locked for reading throughout (#884).
    /// So the walk asks for the lines an entry's header and key are in as
    /// it finds the entry, and puts it in a batch, and the entries of a
    /// full batch are visited together, by then on their way. The walk is
    /// a plain loop over each table: `next` on the two tables chained cost
    /// more a slot.
    #[inline(always)]
    fn visit_positions(&self, from: usize, visit: &mut impl FnMut(EntryView<'_>)) {
        let mut batch = [Slot::NONE; SCAN_BATCH];
        let mut held = 0usize;
        for table in [&self.table, &self.old] {
            for &slot in table.iter() {
                if slot.scan_position() < from {
                    continue;
                }
                slot.prefetch_key();
                batch[held % SCAN_BATCH] = slot;
                held += 1;
                if held == SCAN_BATCH {
                    Self::visit_batch(&batch, false, visit);
                    held = 0;
                }
            }
        }
        Self::visit_batch(&batch[..held], false, visit);
    }

    /// The entries of `batch`, to `visit`, with `ask` the lines of the
    /// entry a few ahead asked for meanwhile: out of line, and the one place
    /// `visit` is called from, so that it is inlined here.
    #[inline(never)]
    fn visit_batch(batch: &[Slot], ask: bool, visit: &mut impl FnMut(EntryView<'_>)) {
        // Enough entries in flight to cover a miss to memory (about 100 ns)
        // with the visits of the ones before: with 8, the page waited for
        // memory at every entry (#884).
        const AHEAD: usize = 12;
        if ask {
            batch
                .iter()
                .take(AHEAD)
                .for_each(|slot| slot.prefetch_key());
        }
        for (i, slot) in batch.iter().enumerate() {
            if ask && let Some(later) = batch.get(i + AHEAD) {
                later.prefetch_key();
            }
            visit(EntryView {
                entry: slot.entry(),
                _map: std::marker::PhantomData,
            });
        }
    }

    /// Every entry in buckets `from..to` of [`ShardMap::buckets`], given to
    /// `visit` in bucket order: a walk over a shard in parts, the lock let
    /// go of between parts if a writer waits (`KEYS`, #1018).
    ///
    /// Each entry is somewhere else in memory: visited one after the other
    /// as the walk finds them, a `KEYS` over 500,000 keys of 2 KB waited
    /// for memory at nearly every entry. As [`ShardMap::scan`] does for a
    /// page (#884), the lines of an entry's header and key are asked for as
    /// the walk finds it, and the entries of a full batch are visited
    /// together, by then on their way.
    #[inline(always)]
    pub fn visit_buckets(&self, from: usize, to: usize, visit: &mut impl FnMut(EntryView<'_>)) {
        let mut batch = [Slot::NONE; SCAN_BATCH];
        let mut held = 0usize;
        let new = self.table.num_buckets();
        let to = to.min(self.buckets());
        let parts = [
            (&self.table, from.min(new), to.min(new)),
            (&self.old, from.max(new) - new, to.max(new) - new),
        ];
        for (table, start, end) in parts {
            for index in start..end {
                let Some(&slot) = table.get_bucket(index) else {
                    continue;
                };
                slot.prefetch_key();
                batch[held] = slot;
                held += 1;
                if held == SCAN_BATCH {
                    Self::visit_batch(&batch, false, visit);
                    held = 0;
                }
            }
        }
        Self::visit_batch(&batch[..held], false, visit);
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

/// A smaller page of [`ShardMap::scan`], gathered in one pass: the slots
/// at positions from `from` up to `end` (the end it may still have), then
/// counted by position. When the slots gathered fill their room
/// (`want` and a margin, at most twice `want`), `end` comes down to the
/// position that brings the page to `want` and the slots past it are
/// dropped, so a page is never more than `want` and the keys sharing the
/// last one's position, whatever the keys left in the shard are, and never
/// less while keys are left ahead: a page ends where `want` is reached, as
/// one counted beforehand did. It takes 8 bytes for each slot it has room
/// for and 4 for each position, while the page is made.
struct ScanPage {
    from: usize,
    want: usize,
    end: usize,
    /// Slots gathered so far, the first `held`, all at positions
    /// `from..end`; the rest is room.
    slots: Vec<Slot>,
    held: usize,
    /// How many of the slots held are at each position from `from`,
    /// counted when the page is cut.
    at: Vec<u32>,
    /// Whether a slot was at a position from `end` on.
    later: bool,
}

impl ScanPage {
    /// A page from `from` of `want` slots, about `expect` of them before
    /// `end`.
    fn new(from: usize, want: usize, expect: usize, end: usize) -> ScanPage {
        // Room for a count that comes out above its mean; past twice
        // `want`, `end` comes down.
        let room = expect
            .saturating_add(expect / 8 + 16)
            .min(want.saturating_mul(2).max(64));
        ScanPage {
            from,
            want,
            end,
            slots: vec![Slot::NONE; room + 1],
            held: 0,
            at: Vec::new(),
            later: false,
        }
    }

    /// The slots gathered.
    fn slots(&self) -> &[Slot] {
        &self.slots[..self.held]
    }

    /// Let the page reach `end` (past the guess it was gathered to).
    fn widen(&mut self, end: usize) {
        self.end = end;
        self.later = false;
    }

    /// Gather the map's slots at positions from `lo` up to `end`.
    ///
    /// Without a branch on the position: whether a slot is in the page is
    /// as likely as not, and the mispredicted branches were most of the
    /// walk's time. Every slot is written to the next place, which only a
    /// slot in the page takes (there is always one place to spare); the
    /// slots are counted by position once gathered (`cut`). What the loop
    /// changes is kept in locals: kept in the page, each slot's store had
    /// to be read back by the next.
    #[inline(always)]
    fn gather(&mut self, map: &ShardMap, lo: usize) {
        let mut end = self.end;
        let mut held = self.held;
        let mut later = self.later;
        let mut slots: &mut [Slot] = &mut self.slots;
        for table in [&map.table, &map.old] {
            for &slot in table.iter() {
                let position = slot.scan_position();
                let inside = position.wrapping_sub(lo) < end.wrapping_sub(lo);
                later |= position >= end;
                slots[held] = slot;
                held += usize::from(inside);
                if held + 1 == slots.len() {
                    self.held = held;
                    self.end = end;
                    self.make_room();
                    later |= self.later;
                    (held, end) = (self.held, self.end);
                    slots = &mut self.slots;
                }
            }
        }
        self.held = held;
        self.end = end;
        self.later = later;
    }

    /// The position after the one that brings the page to `want`, and
    /// how many slots are before it: `end` and all of them if fewer.
    fn reach(&self) -> (usize, usize) {
        let mut taken = 0usize;
        for (i, &n) in self.at.iter().enumerate() {
            taken += n as usize;
            if taken >= self.want {
                return (self.from + i + 1, taken);
            }
        }
        (self.end, taken)
    }

    /// End the page where `want` is reached, and drop the slots past it;
    /// `later` if there were any.
    fn cut(&mut self) {
        self.at.clear();
        self.at.resize(self.end - self.from, 0);
        for slot in &self.slots[..self.held] {
            self.at[slot.scan_position() - self.from] += 1;
        }
        let (end, kept) = self.reach();
        self.end = end;
        if kept < self.held {
            self.later = true;
            let mut held = 0;
            for i in 0..self.held {
                let slot = self.slots[i];
                if slot.scan_position() < end {
                    self.slots[held] = slot;
                    held += 1;
                }
            }
            self.held = held;
        }
    }

    /// The slots fill the room: bring `end` down to where `want` is
    /// reached and drop the slots past it, and if that leaves no room (many
    /// keys at one position: a page holds them all), make more.
    #[cold]
    #[inline(never)]
    fn make_room(&mut self) {
        self.cut();
        if self.held * 4 > self.slots.len() * 3 {
            let more = self.slots.len().saturating_mul(2);
            self.slots.resize(more, Slot::NONE);
        }
    }
}

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
        let mut map = crate::cpus::spawn(move || {
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

    /// Defrag moving a set's entry (#297) moves only the entry: the set and
    /// its array of integers stay where they are, held once, as they were.
    #[test]
    fn moving_a_sets_entry_leaves_its_array_in_place() {
        use crate::member_set::MemberSet;
        use crate::value_slab;
        // Slab evacuation state is shared across the whole process.
        // Serialize tests that plan or finish evacuation so they
        // cannot take each other's slabs or affect global counts.
        let _one = value_slab::EVACUATION_TESTS.lock();
        // A length no other test's keys have: a slab class of its own.
        let key = |i: usize| format!("{i:0>2500}");
        let members: Vec<Vec<u8>> = (-30i64..30).map(|n| n.to_string().into_bytes()).collect();
        // Made on a thread that ends, so that its slabs are nobody's.
        let setup = members.clone();
        let mut map = crate::cpus::spawn(move || {
            let mut map = ShardMap::new();
            for i in 0..64 {
                let mut set = MemberSet::new();
                for m in &setup {
                    set.insert_bytes(m);
                }
                map.insert(key(i), KiviDBValue::Set(Arc::new(RwLock::new(set))));
            }
            map
        })
        .join()
        .unwrap();
        for i in 1..64 {
            map.remove(&key(i));
        }
        let index = (0..map.buckets())
            .find(|&i| map.table.get_bucket(i).is_some())
            .unwrap();
        let old = map.table.get_bucket(index).unwrap().entry();
        let set_at = |map: &ShardMap| match map.get(&key(0)) {
            Some(ValueRef::Set(set)) => Arc::as_ptr(set),
            _ => panic!("not a set"),
        };
        let before = set_at(&map);
        let (array, bytes) = {
            let Some(ValueRef::Set(set)) = map.get(&key(0)) else {
                unreachable!()
            };
            let set = set.read();
            (set.array_ptr(), set.compact_bytes())
        };
        assert!(array.is_some(), "the test needs the array, not the table");

        let evacuating = value_slab::evacuate_only(old.base()).expect("a slab nobody owns");
        assert_eq!(evacuating.live(), 1, "the key's slab is shared");
        let regions = value_slab::regions_snapshot();
        assert!(map.evacuating_at(index, &regions));
        match map.relocate_at(index, &regions) {
            Relocated::Moved(left) => drop(left),
            _ => panic!("not moved"),
        }
        drop(evacuating);
        assert!(map.table.get_bucket(index).unwrap().entry() != old);

        // The same set, at the same address, held only by its key: the copy
        // of the entry did not drop it or take another hold.
        assert_eq!(set_at(&map), before);
        let Some(ValueRef::Set(set)) = map.get(&key(0)) else {
            panic!("not a set")
        };
        assert_eq!(Arc::strong_count(set), 1);
        let set = set.read();
        assert_eq!(set.array_ptr(), array, "the array moved");
        assert_eq!(set.compact_bytes(), bytes);
        let mut got: Vec<Vec<u8>> = set.iter().map(|m| m.to_vec()).collect();
        got.sort();
        let mut want = members;
        want.sort();
        assert_eq!(got, want);
        drop(set);
        // Dropped once, from the copy.
        drop(map.remove(&key(0)).expect("the key"));
        assert!(map.is_empty());
    }

    /// As `moving_a_sets_entry_leaves_its_array_in_place`, for a large
    /// sorted set (#875): its key's entry, which holds the set's `Arc`,
    /// moves; the set, its members' entries in the entry pool and the
    /// index's references to them stay where they are.
    #[test]
    fn moving_a_sorted_sets_entry_leaves_its_members_in_place() {
        use crate::sorted_set::SortedSetInner;
        use crate::value_slab;
        // Slab evacuation state is shared across the whole process.
        // Serialize tests that plan or finish evacuation so they
        // cannot take each other's slabs or affect global counts.
        let _one = value_slab::EVACUATION_TESTS.lock();
        // A length no other test's keys have: a slab class of its own.
        let key = |i: usize| format!("{i:0>2800}");
        // Members too long for a block, so the large form; runs of equal
        // scores, ordered by the members' bytes.
        let members: Vec<(Vec<u8>, f64)> = (0..200)
            .map(|i| (format!("{i:0>80}").into_bytes(), (i % 7) as f64))
            .collect();
        // Made on a thread that ends, so that its slabs are nobody's.
        let setup = members.clone();
        let mut map = crate::cpus::spawn(move || {
            let mut map = ShardMap::new();
            for i in 0..64 {
                let mut set = SortedSetInner::new();
                for (member, score) in &setup {
                    assert!(set.insert_bytes(member, *score));
                }
                map.insert(key(i), KiviDBValue::SortedSet(Arc::new(RwLock::new(set))));
            }
            map
        })
        .join()
        .unwrap();
        for i in 1..64 {
            map.remove(&key(i));
        }
        let index = (0..map.buckets())
            .find(|&i| map.table.get_bucket(i).is_some())
            .unwrap();
        let old = map.table.get_bucket(index).unwrap().entry();
        let generation = value_slab::generation_of(old.base());
        let zset_at = |map: &ShardMap| match map.get(&key(0)) {
            Some(ValueRef::SortedSet(zset)) => Arc::as_ptr(zset),
            _ => panic!("not a sorted set"),
        };
        // Each member's entry, and the index's references in its order.
        let places = |map: &ShardMap| {
            let Some(ValueRef::SortedSet(zset)) = map.get(&key(0)) else {
                panic!("not a sorted set")
            };
            let set = zset.read();
            let large = set.large().expect("the large form");
            large.check();
            let entries: Vec<usize> = members
                .iter()
                .map(|(member, _)| large.table().entry(member).expect("a member"))
                .map(|entry| entry.as_ptr().addr())
                .collect();
            (entries, large.index_addrs())
        };
        let before = zset_at(&map);
        let placed = places(&map);

        let evacuating = value_slab::evacuate_only(old.base()).expect("a slab nobody owns");
        assert_eq!(evacuating.live(), 1, "the key's slab is shared");
        let regions = value_slab::regions_snapshot();
        assert!(map.evacuating_at(index, &regions));
        match map.relocate_at(index, &regions) {
            Relocated::Moved(left) => drop(left),
            _ => panic!("not moved"),
        }
        drop(evacuating);
        assert!(map.table.get_bucket(index).unwrap().entry() != old);
        // The old block freed: its slab emptied, and the set did not need it.
        assert_ne!(value_slab::generation_of(old.base()), generation);

        // The same set, at the same address, held only by its key, its
        // members where they were.
        assert_eq!(zset_at(&map), before);
        assert_eq!(places(&map), placed, "a member's entry moved");
        let Some(ValueRef::SortedSet(zset)) = map.get(&key(0)) else {
            panic!("not a sorted set")
        };
        assert_eq!(Arc::strong_count(zset), 1);
        let set = zset.read();
        let mut want = members;
        want.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let got: Vec<(Vec<u8>, f64)> = set
            .iter()
            .map(|(member, score)| (member.borrowed().to_vec(), score))
            .collect();
        assert_eq!(got, want);
        for (rank, (member, score)) in want.iter().enumerate() {
            assert_eq!(set.rank(member), Some((rank, *score)));
        }
        drop(set);
        // Dropped once, from the copy.
        drop(map.remove(&key(0)).expect("the key"));
        assert!(map.is_empty());
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

    /// A SET over a string of its own writes its block when nothing but the
    /// entry holds it and the new length keeps the block's class; a holder
    /// of the bytes (a GET's reply on its way out) keeps them as they were.
    #[test]
    fn a_string_of_its_own_is_written_in_place_while_nothing_else_holds_it() {
        let mut map = ShardMap::new();
        let block = |map: &ShardMap| match map.get("k") {
            Some(ValueRef::String(s)) => s.as_bytes().as_ptr() as usize,
            _ => panic!("no string"),
        };
        // Two lengths of one slab class's blocks (8 bytes of header each).
        let top = crate::value_slab::block_size(crate::value_slab::class_of(5_100).unwrap()) - 8;
        let (a, b) = (vec![1u8; top - 40], vec![2u8; top]);
        assert_eq!(
            crate::value_slab::class_of(top - 32),
            crate::value_slab::class_of(top + 8)
        );
        assert!(matches!(map.set_str("k", &a), Written::New));
        let at = block(&map);
        assert!(matches!(map.set_str("k", &b), Written::InPlace));
        assert_eq!(text(map.get("k")), Some(b.clone()));
        assert_eq!(block(&map), at);
        // Held: a new block, and the holder still reads what it held.
        let held = match map.get("k") {
            Some(ValueRef::String(s)) => s.to_shared(),
            _ => panic!("no string"),
        };
        assert!(matches!(map.set_str("k", &a), Written::Replaced(_)));
        assert_eq!(held.as_slice(), &b[..]);
        assert_eq!(text(map.get("k")), Some(a.clone()));
        drop(held);
        // Another class: a new block.
        let at = block(&map);
        assert!(matches!(
            map.set_str("k", vec![3u8; 9_000]),
            Written::Replaced(_)
        ));
        assert_ne!(block(&map), at);
        // The system allocator's block: in place at the same length only.
        assert!(matches!(
            map.set_str("k", vec![4u8; 40_000]),
            Written::Replaced(_)
        ));
        let at = block(&map);
        assert!(matches!(
            map.set_str("k", vec![5u8; 40_000]),
            Written::InPlace
        ));
        assert_eq!(block(&map), at);
        assert_eq!(text(map.get("k")), Some(vec![5u8; 40_000]));
        assert!(matches!(
            map.set_str("k", vec![6u8; 40_001]),
            Written::Replaced(_)
        ));
        assert_eq!(text(map.get("k")), Some(vec![6u8; 40_001]));
    }

    /// A SET at a string's length while compaction empties the slab of its
    /// block (#240 review): written in place, in the slab being emptied,
    /// then moved out of it whole, the new bytes. An edit does the same.
    #[test]
    fn a_string_written_in_place_while_its_slab_is_emptied_moves_whole() {
        // A class no other test allocates (21 KB: nothing else here is), on
        // a thread that ends, so that nobody owns its slab.
        let len = 21_000;
        let mut map = std::thread::spawn(move || {
            let mut map = ShardMap::new();
            assert!(matches!(map.set_str("k", vec![1u8; len]), Written::New));
            map
        })
        .join()
        .unwrap();
        let block = match map.get("k") {
            Some(ValueRef::String(s)) => s.as_bytes().as_ptr(),
            _ => panic!("no string"),
        };
        let evacuating = crate::value_slab::evacuate_only(block).expect("a slab nobody owns");
        assert!(matches!(map.set_str("k", vec![2u8; len]), Written::InPlace));
        let mut want = vec![2u8; len];
        want[len - 1] = 7;
        assert!(matches!(
            map.edit_str_in_place("k", len - 1, &[7]),
            Some(Written::InPlace)
        ));
        let regions = crate::value_slab::regions_snapshot();
        let index = (0..map.buckets())
            .find(|&i| map.table.get_bucket(i).is_some())
            .unwrap();
        assert!(map.evacuating_at(index, &regions));
        match map.relocate_at(index, &regions) {
            Relocated::Moved(left) => drop(left),
            _ => panic!("not moved"),
        }
        drop(evacuating);
        let moved = match map.get("k") {
            Some(ValueRef::String(s)) => s.as_bytes().as_ptr(),
            _ => panic!("no string"),
        };
        assert_ne!(moved, block, "still in the slab being emptied");
        assert_eq!(text(map.get("k")), Some(want));
        // In place again where it is now.
        assert!(matches!(map.set_str("k", vec![3u8; len]), Written::InPlace));
        assert_eq!(text(map.get("k")), Some(vec![3u8; len]));
    }

    /// A snapshot's hold of an entry ([`EntryView::hold`]) and a reply's
    /// ([`ShardMap::get_ref`]) taken before a SET at the string's length:
    /// the SET makes a new entry and block, and both still read the old
    /// bytes, an inline string and a string of its own alike.
    #[test]
    fn a_held_entry_keeps_its_bytes_across_a_set_at_their_length() {
        for len in [100, 5_000] {
            let mut map = ShardMap::new();
            map.set_str("k", vec![1u8; len]);
            let snapshot = map.entries().next().expect("one entry").hold();
            let reply = map.get_ref("k").expect("the key");
            assert!(matches!(
                map.set_str("k", vec![2u8; len]),
                Written::Replaced(_)
            ));
            for held in [&snapshot, &reply] {
                match held.value() {
                    ValueRef::String(s) => assert_eq!(s.as_bytes(), &vec![1u8; len][..], "{len}"),
                    _ => panic!("not a string"),
                }
            }
            assert_eq!(text(map.get("k")), Some(vec![2u8; len]));
            drop((snapshot, reply));
            // Nothing holds it now: in place.
            assert!(matches!(map.set_str("k", vec![3u8; len]), Written::InPlace));
            assert_eq!(text(map.get("k")), Some(vec![3u8; len]));
        }
    }

    #[test]
    fn a_key_past_its_time_is_not_written_in_place() {
        let mut map = ShardMap::new();
        // SET k v KEEPTTL, INCR, SETRANGE's fallback: a new key, no time to live.
        map.insert_str_expiring("set", "5", Some(1));
        assert!(matches!(map.set_str("set", "6"), Written::Replaced(_)));
        assert_eq!(text(map.get("set")), Some(b"6".to_vec()));
        assert_eq!(map.expires("set"), None);
        // APPEND writes a new key over it, which is announced; SETRANGE
        // leaves it to the caller's write of a new value.
        map.insert_str_expiring("app", "5", Some(1));
        assert!(matches!(
            map.append_str("app", b"6", usize::MAX),
            Ok((1, Written::Replaced(_)))
        ));
        assert_eq!(text(map.get("app")), Some(b"6".to_vec()));
        assert_eq!(map.expires("app"), None);
        map.insert_str_expiring("rng", "5", Some(1));
        assert_eq!(map.set_range_in_place("rng", 0, b"6"), None);
        assert_eq!(
            map.take_left(),
            vec![
                (ToSay::PastItsTime, crate::store::Key::from("set")),
                (ToSay::PastItsTime, crate::store::Key::from("app")),
            ]
        );
        // A live key is still written in place.
        map.insert_str_expiring("live", "5", Some(u64::MAX - 1));
        assert!(matches!(map.set_str("live", "6"), Written::InPlace));
        assert!(matches!(
            map.append_str("live", b"7", usize::MAX),
            Ok((2, Written::InPlace))
        ));
        assert_eq!(map.expires("live"), Some(u64::MAX - 1));
    }

    /// Where `key`'s string's bytes are.
    fn bytes_at(map: &ShardMap, key: &str) -> usize {
        match map.get(key) {
            Some(ValueRef::String(s)) => s.as_bytes().as_ptr() as usize,
            _ => panic!("no string"),
        }
    }

    /// `len` bytes that differ from their neighbours.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// An edit of a string nothing else holds is written where it is, in
    /// the entry, in a slab block or in the system allocator's block, its
    /// time to live kept; only the byte written changes.
    #[test]
    fn an_edit_nothing_else_holds_is_written_where_it_is() {
        for len in [100, 10_000, 64 << 10, 1 << 20] {
            let mut map = ShardMap::new();
            let mut want = pattern(len);
            map.insert_str_expiring("k", &want, Some(u64::MAX - 1));
            let at = bytes_at(&map, "k");
            for offset in [0, len / 2, len - 1] {
                let written = map.edit_str_in_place("k", offset, &[0xFF]);
                assert!(matches!(written, Some(Written::InPlace)), "{len} {offset}");
                want[offset] = 0xFF;
            }
            assert_eq!(text(map.get("k")), Some(want), "{len}");
            assert_eq!(bytes_at(&map, "k"), at, "{len}");
            assert_eq!(map.expires("k"), Some(u64::MAX - 1), "{len}");
        }
    }

    /// Growth stays in place while the block keeps its class, zero-filled;
    /// past it, or for the system allocator's block, it takes a new one.
    #[test]
    fn an_edit_grows_in_place_only_within_the_blocks_class() {
        // In the entry: some of a byte at a time stay, one moves.
        let mut map = ShardMap::new();
        let mut want = pattern(100);
        map.set_str("k", &want);
        let (mut stayed, mut moved) = (0, 0);
        for len in 101..=200 {
            let at = bytes_at(&map, "k");
            let in_place = match map.edit_str_in_place("k", len - 1, &[7]) {
                Some(Written::InPlace) => true,
                Some(Written::Replaced(_)) => false,
                _ => panic!("not written"),
            };
            assert_eq!(bytes_at(&map, "k") == at, in_place, "{len}");
            if in_place {
                stayed += 1;
            } else {
                moved += 1;
            }
            want.push(7);
        }
        assert!(stayed > 0 && moved > 0, "{stayed} in place, {moved} moved");
        assert_eq!(text(map.get("k")), Some(want));
        // A slab block grown to its class's top, zeros between, then past it.
        let top = crate::value_slab::block_size(crate::value_slab::class_of(10_008).unwrap()) - 8;
        assert!(top > 10_001, "no room in the class of 10,000 bytes");
        let mut want = pattern(10_000);
        map.set_str("s", &want);
        let at = bytes_at(&map, "s");
        let written = map.edit_str_in_place("s", top - 1, &[7]);
        assert!(matches!(written, Some(Written::InPlace)), "{top}");
        want.resize(top - 1, 0);
        want.push(7);
        assert_eq!(
            (text(map.get("s")), bytes_at(&map, "s")),
            (Some(want.clone()), at)
        );
        let written = map.edit_str_in_place("s", top, &[8]);
        assert!(matches!(written, Some(Written::Replaced(_))));
        assert_ne!(bytes_at(&map, "s"), at);
        want.push(8);
        assert_eq!(text(map.get("s")), Some(want));
        // The system allocator's block keeps its length to stay.
        let mut want = pattern(64 << 10);
        map.set_str("b", &want);
        let at = bytes_at(&map, "b");
        let written = map.edit_str_in_place("b", want.len() + 2, &[9]);
        assert!(matches!(written, Some(Written::Replaced(_))));
        assert_ne!(bytes_at(&map, "b"), at);
        want.extend([0, 0, 9]);
        assert_eq!(text(map.get("b")), Some(want));
    }

    /// A hold on the entry (a snapshot's) or on its bytes (a reply's) makes
    /// an edit copy the string once; the holder keeps reading what it held,
    /// and once it lets go, edits are in place again.
    #[test]
    fn an_edit_of_a_held_string_copies_it_and_the_holder_keeps_its_bytes() {
        for len in [100, 10_000, 64 << 10] {
            let mut map = ShardMap::new();
            let old = pattern(len);
            map.set_str("k", &old);
            let mut want = old.clone();
            want[len / 2] ^= 1;
            // A snapshot holds the entry.
            let snapshot = map.entries().next().expect("one entry").hold();
            let at = bytes_at(&map, "k");
            let written = map.edit_str_in_place("k", len / 2, &want[len / 2..len / 2 + 1]);
            assert!(matches!(written, Some(Written::Replaced(_))), "{len}");
            drop(written);
            match snapshot.value() {
                ValueRef::String(s) => assert_eq!(s.as_bytes(), &old[..], "{len}"),
                _ => panic!("not a string"),
            }
            assert_eq!(text(map.get("k")), Some(want.clone()), "{len}");
            assert_ne!(bytes_at(&map, "k"), at, "{len}");
            drop(snapshot);
            // A reply holds the bytes: a copy of them, when in the entry.
            let reply = match map.get("k") {
                Some(ValueRef::String(s)) => s.to_shared(),
                _ => panic!("no string"),
            };
            let in_entry = reply.as_ptr() as usize != bytes_at(&map, "k");
            let written = map.edit_str_in_place("k", 0, &[!want[0]]);
            assert_eq!(matches!(written, Some(Written::InPlace)), in_entry, "{len}");
            assert_eq!(&reply[..], &want[..], "{len}");
            want[0] = !want[0];
            assert_eq!(text(map.get("k")), Some(want.clone()), "{len}");
            drop((written, reply));
            // Nothing holds it now.
            assert!(matches!(
                map.edit_str_in_place("k", 1, &[0]),
                Some(Written::InPlace)
            ));
        }
    }

    /// Entries each held by the table alone can share their bytes (`COPY`,
    /// or a time to live given while a snapshot holds the entry): an edit
    /// of either copies them, and the other keeps them as they were.
    #[test]
    fn an_edit_of_bytes_two_entries_share_copies_them() {
        let mut map = ShardMap::new();
        let old = pattern(10_000);
        map.set_str("a", &old);
        let value = map.get("a").expect("a").to_value();
        map.insert("b", value);
        let written = map.edit_str_in_place("a", 0, &[0xFF]);
        assert!(matches!(written, Some(Written::Replaced(_))));
        drop(written);
        assert_eq!(text(map.get("b")), Some(old.clone()));
        // Each has bytes of its own now.
        for key in ["a", "b"] {
            let written = map.edit_str_in_place(key, 1, &[0xFF]);
            assert!(matches!(written, Some(Written::InPlace)), "{key}");
        }
        // A time to live given while a snapshot holds the entry remakes it,
        // the bytes shared with the held one.
        let snapshot = map.get_ref("b").expect("b");
        assert!(map.set_expires("b", u64::MAX - 1));
        assert!(!map.holds("b", &snapshot));
        let written = map.edit_str_in_place("b", 2, &[0xFF]);
        assert!(matches!(written, Some(Written::Replaced(_))));
        match snapshot.value() {
            ValueRef::String(s) => assert_eq!(s[2], old[2]),
            _ => panic!("not a string"),
        }
        assert_eq!(map.expires("b"), Some(u64::MAX - 1));
    }

    /// A key not there, or past its time, is an empty string; another type,
    /// or a length past `MAX_LEN`, leaves everything as it was.
    #[test]
    fn an_edit_of_a_missing_expired_or_other_key() {
        let mut map = ShardMap::new();
        assert!(matches!(
            map.edit_str_in_place("new", 2, b"x"),
            Some(Written::New)
        ));
        assert_eq!(text(map.get("new")), Some(b"\0\0x".to_vec()));
        map.insert_str_expiring("past", "abc", Some(1));
        let written = map.edit_str_in_place("past", 1, b"z");
        assert!(matches!(written, Some(Written::Replaced(_))));
        assert_eq!(text(map.get("past")), Some(b"\0z".to_vec()));
        assert_eq!(map.expires("past"), None);
        assert_eq!(
            map.take_left(),
            vec![(ToSay::PastItsTime, crate::store::Key::from("past"))]
        );
        let set = crate::member_set::MemberSet::new();
        map.insert("set", KiviDBValue::Set(Arc::new(RwLock::new(set))));
        assert!(map.edit_str_in_place("set", 0, b"x").is_none());
        assert!(matches!(map.get("set"), Some(ValueRef::Set(_))));
        map.set_str("k", "abc");
        let max = crate::shared_bytes::MAX_LEN;
        for (offset, patch) in [(max, &b"x"[..]), (usize::MAX, b"x"), (max - 1, b"xy")] {
            assert!(map.edit_str_in_place("k", offset, patch).is_none());
        }
        assert_eq!(text(map.get("k")), Some(b"abc".to_vec()));
    }

    /// Seeded random edits, each made on two maps: on one nothing else holds
    /// the string, on the other a holder may make the edit copy it. Both
    /// end as a model says; in place exactly when the block has room;
    /// each holder and each neighbouring string keeps its bytes.
    #[test]
    fn edits_in_place_and_copied_agree_with_a_model() {
        use rand::{Rng, SeedableRng};
        const SEED: u64 = 0x5e7_b175;
        let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
        // The slab class of bytes of their own, after their 8-byte header.
        let class = |len: usize| crate::value_slab::class_of(len + 8);
        let top = |len: usize| class(len).map(|c| crate::value_slab::block_size(c) - 8);
        let inline = crate::store::entry::INLINE_MAX;
        let slab = crate::value_slab::MAX_BLOCK - 8;
        // The largest class is a `value_slab` test's alone: kept out of it.
        let below = crate::value_slab::block_size(class(slab).unwrap() - 1) - 8;
        let largest = |len: usize| len > below && len <= slab;
        assert!(class(below) != class(slab) && class(below + 1) == class(slab));
        // Bytes of their own: a second hold shares them, not a copy.
        let own = |map: &ShardMap| match map.get("k") {
            Some(ValueRef::String(s)) => s.to_shared().as_ptr() == s.as_bytes().as_ptr(),
            _ => panic!("no string"),
        };
        let random = |rng: &mut rand::rngs::StdRng, len: usize| {
            let mut bytes = vec![0u8; len];
            rng.fill(&mut bytes[..]);
            bytes
        };
        // Each form of string the cases wrote in place, and copied.
        let mut reached = std::collections::BTreeSet::new();
        for case in 0..400 {
            let len = match rng.gen_range(0..6) {
                0 => rng.gen_range(0..=64),
                1 => rng.gen_range(inline - 64..=inline + 64),
                2 => rng.gen_range(inline + 1..=below),
                3 => top(rng.gen_range(inline + 1..=below)).unwrap(),
                4 => [below - rng.gen_range(0..=32), slab + rng.gen_range(1..=32)]
                    [rng.gen_range(0..2)],
                _ => rng.gen_range(slab + 1..=96 << 10),
            };
            let mut model = random(&mut rng, len);
            let ttl = rng.gen_bool(0.5).then_some(u64::MAX - 1);
            let neighbours: Vec<Vec<u8>> = (0..3).map(|_| random(&mut rng, len)).collect();
            let mut maps = [ShardMap::new(), ShardMap::new()];
            for map in &mut maps {
                // Strings of the same length, in blocks beside the edited one.
                map.set_str("n0", &neighbours[0]);
                map.set_str("n1", &neighbours[1]);
                map.insert_str_expiring("k", &model, ttl);
                map.set_str("n2", &neighbours[2]);
            }
            for step in 0..8 {
                let len = model.len();
                let patch_len = match rng.gen_range(0..8) {
                    0 => 0,
                    1..=5 => rng.gen_range(1..=8),
                    6 => rng.gen_range(9..=300),
                    _ => rng.gen_range(301..=5000),
                };
                let patch = random(&mut rng, patch_len);
                // Where the patch ends: in the string, past it, or at a limit
                // of the block, the entry or the slabs, and one byte past.
                let end = match rng.gen_range(0..5) {
                    0 if len > 0 => rng.gen_range(1..=len),
                    0 | 1 => len + rng.gen_range(0..=64),
                    2 => top(len).filter(|_| own(&maps[0])).unwrap_or(len),
                    3 => [inline, below][rng.gen_range(0..2)],
                    _ => len + 1,
                } + rng.gen_range(0..=1);
                // Past the largest class instead, to the system allocator.
                let end = if largest(len.max(end)) { slab + 1 } else { end };
                let offset = end.saturating_sub(patch.len());
                let end = offset + patch.len();
                let grown = len.max(end);
                assert!(!largest(grown), "{grown}");
                let at = maps.each_ref().map(|map| bytes_at(map, "k"));
                let owns = maps.each_ref().map(own);
                // 0: nothing; 1: the entry (a snapshot); 2: its bytes (a
                // reply); 3: a second entry sharing them; 4: let go before.
                let holder = rng.gen_range(0..5);
                let entry = (holder == 1).then(|| maps[1].get_ref("k").expect("k"));
                let bytes = (holder == 2).then(|| match maps[1].get("k") {
                    Some(ValueRef::String(s)) => s.to_shared(),
                    _ => panic!("no string"),
                });
                if holder == 3 {
                    let value = maps[1].get("k").expect("k").to_value();
                    drop(maps[1].insert("twin", value));
                }
                if holder == 4 {
                    drop(maps[1].get_ref("k"));
                }
                // Whether the bytes are shared, not copied for the holder.
                let shared = match holder {
                    2 => bytes.as_ref().is_some_and(|b| b.as_ptr() as usize == at[1]),
                    3 => bytes_at(&maps[1], "twin") == at[1],
                    _ => false,
                };
                let written = [
                    maps[0].edit_str_in_place("k", offset, &patch),
                    maps[1].edit_str_in_place("k", offset, &patch),
                ];
                let old = model.clone();
                model.resize(grown, 0);
                model[offset..end].copy_from_slice(&patch);
                let what = format!(
                    "seed {SEED:#x}, case {case}, step {step}: {len} bytes, own {owns:?}, \
                     {} written at {offset}, holder {holder}",
                    patch.len()
                );
                if holder == 2 {
                    // A reply shares bytes of their own, and copies an entry's.
                    assert_eq!(shared, owns[1], "{what}");
                }
                for (i, map) in maps.iter().enumerate() {
                    let in_place = match &written[i] {
                        Some(Written::InPlace) => true,
                        Some(Written::Replaced(_)) => false,
                        _ => panic!("{what}: map {i} not written"),
                    };
                    assert_eq!(bytes_at(map, "k") == at[i], in_place, "{what}: map {i}");
                    if i == 0 {
                        reached.insert((owns[0], class(len).is_some(), in_place));
                    }
                    assert!(text(map.get("k")) == Some(model.clone()), "{what}: map {i}");
                    assert_eq!(map.expires("k"), ttl, "{what}: map {i}");
                    // Only bytes of their own can tell: an entry's room
                    // depends on its key and header too.
                    if i == 1 && (holder == 1 || shared) {
                        assert!(!in_place, "{what}: map {i} held");
                    } else if owns[i] {
                        let room = match class(len) {
                            Some(c) => class(grown) == Some(c),
                            None => grown == len,
                        };
                        assert_eq!(in_place, room, "{what}: map {i}");
                    }
                }
                if let Some(entry) = entry {
                    match entry.value() {
                        ValueRef::String(s) => assert!(s.as_bytes() == old, "{what}: entry"),
                        _ => panic!("{what}: not a string"),
                    }
                }
                if let Some(bytes) = bytes {
                    assert!(bytes[..] == old[..], "{what}: bytes");
                }
                if holder == 3 {
                    assert!(text(maps[1].get("twin")) == Some(old), "{what}: twin");
                    drop(maps[1].remove("twin"));
                }
            }
            for map in &maps {
                for (n, want) in neighbours.iter().enumerate() {
                    let key = format!("n{n}");
                    let got = text(map.get(&key));
                    assert!(
                        got.as_ref() == Some(want),
                        "seed {SEED:#x}, case {case}: {key}"
                    );
                }
            }
        }
        // In the entry, a slab block and a block of its own: each both ways.
        assert_eq!(reached.len(), 6, "{reached:?}");
    }

    /// How `append_str` wrote, as the appends model test tells the cases
    /// apart.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Appended {
        /// In the entry's own block.
        Inline,
        /// In the block the bytes were in, which had room.
        InRoom,
        /// The entry stayed; its bytes were grown or moved to a new block.
        Grown,
        /// A new entry: a copy.
        Copied,
    }

    /// Seeded random appends, of nothing to a few MiB, made on two maps: on
    /// one nothing else holds the string, on the other a holder may make the
    /// append copy it (a snapshot's hold on the entry, a reply's or a
    /// `GETEX`'s on the bytes, a second key sharing them since a `COPY`, a
    /// hold let go before). Both end as a model says, with the time to live
    /// kept; in the bytes' block, with nothing allocated, exactly when it has
    /// room; a block grown or made by an append has the room
    /// `room_for_append` says; each holder and each neighbouring string keeps
    /// its bytes.
    ///
    /// Strings step through the largest slab class too, which a
    /// `value_slab` test owns: the run holds that test's lock, on a thread
    /// that ends before the lock goes, so no slab of the class is left
    /// owned by it.
    #[test]
    fn appends_in_place_and_copied_agree_with_a_model() {
        let _one = crate::value_slab::EVACUATION_TESTS.lock();
        std::thread::spawn(appends_model_run).join().unwrap();
    }

    fn appends_model_run() {
        use crate::shared_bytes::room_for_append;
        use rand::{Rng, SeedableRng};
        const SEED: u64 = 0x0a99_e4d0;
        let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
        let class = |len: usize| crate::value_slab::class_of(len + 8);
        let inline = crate::store::entry::INLINE_MAX;
        let slab = crate::value_slab::MAX_BLOCK - 8;
        // The largest slab class: lengths past `below`, up to `slab`.
        let below = crate::value_slab::block_size(class(slab).unwrap() - 1) - 8;
        let largest = |len: usize| len > below && len <= slab;
        let shared = |map: &ShardMap| match map.get("k") {
            Some(ValueRef::String(s)) => (s.shared().is_some(), s.spare()),
            _ => panic!("no string"),
        };
        let random = |rng: &mut rand::rngs::StdRng, len: usize| {
            let mut bytes = vec![0u8; len];
            rng.fill(&mut bytes[..]);
            bytes
        };
        let mut reached = std::collections::BTreeSet::new();
        // How appends took bytes in the largest class to the system allocator.
        let mut top_to_system = std::collections::BTreeSet::new();
        for case in 0..300 {
            let len = match rng.gen_range(0..7) {
                0 => rng.gen_range(0..=64),
                1 => rng.gen_range(inline - 64..=inline + 64),
                2 => rng.gen_range(inline + 1..=below),
                3 => rng.gen_range(slab + 1..=96 << 10),
                4 => rng.gen_range(1 << 20..=3 << 20),
                5 => rng.gen_range(below - 2000..=below),
                _ => rng.gen_range(below + 1..=slab),
            };
            let mut model = random(&mut rng, len);
            let ttl = rng.gen_bool(0.5).then_some(u64::MAX - 1);
            let neighbours: Vec<Vec<u8>> = (0..3)
                .map(|_| random(&mut rng, len.min(64 << 10)))
                .collect();
            let mut maps = [ShardMap::new(), ShardMap::new()];
            for map in &mut maps {
                map.set_str("n0", &neighbours[0]);
                map.set_str("n1", &neighbours[1]);
                map.insert_str_expiring("k", &model, ttl);
                map.set_str("n2", &neighbours[2]);
            }
            for step in 0..10 {
                let len = model.len();
                let mut extra_len = match rng.gen_range(0..10) {
                    0 => 0,
                    1..=4 => rng.gen_range(1..=8),
                    5 | 6 => rng.gen_range(9..=300),
                    7 | 8 => rng.gen_range(301..=5000),
                    _ if len < 6 << 20 => rng.gen_range(5001..=2 << 20),
                    _ => 100,
                };
                if largest(len + extra_len) && rng.gen_bool(0.5) {
                    // Past the largest class instead, to the system allocator.
                    extra_len = slab + 1 - len + rng.gen_range(0..64);
                }
                let extra = random(&mut rng, extra_len);
                let grown = len + extra_len;
                let at = maps.each_ref().map(|map| bytes_at(map, "k"));
                let before = maps.each_ref().map(shared);
                let holder = rng.gen_range(0..5);
                let entry = (holder == 1).then(|| maps[1].get_ref("k").expect("k"));
                let bytes = (holder == 2).then(|| match maps[1].get("k") {
                    Some(ValueRef::String(s)) => s.to_shared(),
                    _ => panic!("no string"),
                });
                if holder == 3 {
                    let value = maps[1].get("k").expect("k").to_value();
                    drop(maps[1].insert("twin", value));
                }
                if holder == 4 {
                    drop(maps[1].get_ref("k"));
                }
                // Whether the bytes are shared, not copied, by the holder.
                let held = match holder {
                    1 => true,
                    2 => bytes.as_ref().is_some_and(|b| b.as_ptr() as usize == at[1]),
                    3 => bytes_at(&maps[1], "twin") == at[1],
                    _ => false,
                };
                let mut counted = [None, None];
                let mut written = Vec::new();
                for (i, map) in maps.iter_mut().enumerate() {
                    let start = crate::memory_use::allocated_by_this_thread();
                    let done = map.append_str("k", &extra, usize::MAX);
                    counted[i] = start.zip(crate::memory_use::allocated_by_this_thread());
                    written.push(done);
                }
                let old = model.clone();
                model.extend_from_slice(&extra);
                let what = format!(
                    "seed {SEED:#x}, case {case}, step {step}: {len} bytes, {before:?}, \
                     {extra_len} appended, holder {holder}"
                );
                for (i, map) in maps.iter().enumerate() {
                    let after = shared(map);
                    let room = before[i].0
                        && match class(len) {
                            Some(c) => class(grown) == Some(c),
                            None => grown <= len + before[i].1,
                        };
                    let how = match &written[i] {
                        Ok((n, _)) if *n != grown => panic!("{what}: map {i} said {n}"),
                        Ok((_, Written::Replaced(_))) => Appended::Copied,
                        Ok((_, Written::InPlace)) if !before[i].0 => Appended::Inline,
                        Ok((_, Written::InPlace)) if room || extra_len == 0 => Appended::InRoom,
                        Ok((_, Written::InPlace)) => Appended::Grown,
                        other => panic!("{what}: map {i}: {:?}", other.as_ref().err()),
                    };
                    reached.insert((before[i].0, class(len).is_some(), how));
                    if before[i].0 && largest(len) && class(grown).is_none() {
                        top_to_system.insert(how);
                    }
                    assert!(text(map.get("k")) == Some(model.clone()), "{what}: map {i}");
                    assert_eq!(map.expires("k"), ttl, "{what}: map {i}");
                    if i == 1 && held && extra_len > 0 {
                        assert_eq!(how, Appended::Copied, "{what}: map {i} held");
                    } else if before[i].0 {
                        // Bytes of their own, nothing else holding them.
                        assert_ne!(how, Appended::Copied, "{what}: map {i}");
                    }
                    if how == Appended::InRoom {
                        assert_eq!(bytes_at(map, "k"), at[i], "{what}: map {i}");
                        if let Some((a, b)) = counted[i] {
                            assert_eq!(a, b, "{what}: map {i} allocated in place");
                        }
                    }
                    // The room: what was left, or what an append leaves.
                    let spare = match how {
                        _ if !after.0 || class(grown).is_some() => 0,
                        Appended::InRoom => before[i].1 - extra_len,
                        _ => room_for_append(grown) - grown,
                    };
                    assert_eq!(after.1, spare, "{what}: map {i} room");
                }
                drop(written);
                if let Some(entry) = entry {
                    match entry.value() {
                        ValueRef::String(s) => assert!(s.as_bytes() == old, "{what}: entry"),
                        _ => panic!("{what}: not a string"),
                    }
                }
                if let Some(bytes) = bytes {
                    assert!(bytes[..] == old[..], "{what}: bytes");
                }
                if holder == 3 {
                    assert!(text(maps[1].get("twin")) == Some(old), "{what}: twin");
                    drop(maps[1].remove("twin"));
                }
            }
            for map in &maps {
                for (n, want) in neighbours.iter().enumerate() {
                    let key = format!("n{n}");
                    let got = text(map.get(&key));
                    assert!(
                        got.as_ref() == Some(want),
                        "seed {SEED:#x}, case {case}: {key}"
                    );
                }
            }
        }
        // Inline, in place and copied; a slab's bytes and a large block's,
        // each in their room, grown and copied.
        for want in [
            (false, true, Appended::Inline),
            (false, true, Appended::Copied),
            (true, true, Appended::InRoom),
            (true, true, Appended::Grown),
            (true, true, Appended::Copied),
            (true, false, Appended::InRoom),
            (true, false, Appended::Grown),
            (true, false, Appended::Copied),
        ] {
            assert!(
                reached.contains(&want),
                "{want:?} never reached: {reached:?}"
            );
        }
        // From the largest slab class to a block of the system allocator's:
        // the entry kept (and the room set), and copied when held.
        for want in [Appended::Grown, Appended::Copied] {
            assert!(
                top_to_system.contains(&want),
                "the largest class to a system block {want:?}: {top_to_system:?}"
            );
        }
    }

    /// A `SET` onto a string an `APPEND` grew: the same length is written in
    /// place and keeps the room; any other length, large or across 26,208
    /// bytes either way, is a new block of exactly its length.
    #[test]
    fn a_set_onto_a_block_an_append_grew() {
        use crate::shared_bytes::room_for_append;
        let spare = |map: &ShardMap| match map.get("k") {
            Some(ValueRef::String(s)) => s.spare(),
            _ => panic!("no string"),
        };
        let slab = crate::value_slab::MAX_BLOCK - 8;
        let mut map = ShardMap::new();
        let grow = |map: &mut ShardMap, extra: usize| {
            let (n, written) = map.append_str("k", &vec![5u8; extra], usize::MAX).unwrap();
            assert!(matches!(written, Written::InPlace), "{n}");
            assert_eq!(spare(map), room_for_append(n) - n, "{n}");
            n
        };
        map.set_str("k", pattern(100_000));
        let grown = grow(&mut map, 4);
        let at = bytes_at(&map, "k");
        assert!(matches!(map.set_str("k", vec![7u8; grown]), Written::InPlace));
        assert_eq!(bytes_at(&map, "k"), at);
        assert_eq!(spare(&map), room_for_append(grown) - grown);
        assert_eq!(text(map.get("k")), Some(vec![7u8; grown]));
        // Shorter, longer, and just past the slabs: large, no room.
        for (n, fill) in [(grown - 1, 1u8), (grown + 1, 2), (slab + 1, 3)] {
            assert!(matches!(map.set_str("k", vec![fill; n]), Written::Replaced(_)), "{n}");
            assert_eq!(spare(&map), 0, "{n}");
            assert_eq!(text(map.get("k")), Some(vec![fill; n]), "{n}");
        }
        // Down across 26,208 bytes to a slab's block (not the largest
        // class, a `value_slab` test's), and up again.
        grow(&mut map, 1000);
        for (n, fill) in [(20_000, 4u8), (50_000, 6), (10_000, 8)] {
            assert!(matches!(map.set_str("k", vec![fill; n]), Written::Replaced(_)), "{n}");
            assert_eq!(spare(&map), 0, "{n}");
            assert_eq!(text(map.get("k")), Some(vec![fill; n]), "{n}");
            if n == 50_000 {
                grow(&mut map, 10);
            }
        }
    }

    /// An append onto a string a `COPY` shares copies it once, with room;
    /// the next ones are in that room. The other key keeps its bytes, and
    /// appends onto it are then in place too.
    #[test]
    fn an_append_after_a_copy_copies_once_then_writes_in_place() {
        let mut map = ShardMap::new();
        let len = 1 << 20;
        map.set_str("a", pattern(len));
        let value = map.get("a").expect("a").to_value();
        drop(map.insert("b", value));
        assert_eq!(bytes_at(&map, "a"), bytes_at(&map, "b"));
        let (n, written) = map.append_str("a", b"xyz", usize::MAX).unwrap();
        assert_eq!(n, len + 3);
        assert!(matches!(written, Written::Replaced(_)));
        drop(written);
        let at = bytes_at(&map, "a");
        for i in 1..=100 {
            let (n, written) = map.append_str("a", b"xyz", usize::MAX).unwrap();
            assert_eq!(n, len + 3 * (i + 1));
            assert!(matches!(written, Written::InPlace));
            assert_eq!(bytes_at(&map, "a"), at, "append {i}");
        }
        assert_eq!(text(map.get("b")), Some(pattern(len)));
        let (_, written) = map.append_str("b", b"!", usize::MAX).unwrap();
        assert!(matches!(written, Written::InPlace));
        let mut want = pattern(len);
        want.push(b'!');
        assert_eq!(text(map.get("b")), Some(want));
        // A COPY of a string with room shares the block, room and all; a
        // write other than an APPEND to either copies it at its length.
        let spare = |map: &ShardMap, key: &str| match map.get(key) {
            Some(ValueRef::String(s)) => s.spare(),
            _ => panic!("no string"),
        };
        let room = spare(&map, "a");
        assert!(room > 0);
        let value = map.get("a").expect("a").to_value();
        drop(map.insert("c", value));
        assert_eq!(bytes_at(&map, "c"), bytes_at(&map, "a"));
        assert_eq!(spare(&map, "c"), room);
        assert!(matches!(
            map.edit_str_in_place("c", 0, b"Z"),
            Some(Written::Replaced(_))
        ));
        assert_eq!(spare(&map, "c"), 0);
        assert_eq!(spare(&map, "a"), room);
        assert_eq!(text(map.get("a")).unwrap()[0], pattern(1)[0]);
        assert_eq!(text(map.get("c")).unwrap()[0], b'Z');
    }

    /// `APPEND`'s refusals write nothing; a key not there is made, an empty
    /// append onto one that is changes nothing.
    #[test]
    fn append_str_refuses_and_makes_as_redis() {
        let mut map = ShardMap::new();
        map.insert("list", KiviDBValue::List(Default::default()));
        assert_eq!(
            map.append_str("list", b"x", usize::MAX).err(),
            Some(AppendRefused::WrongType)
        );
        map.set_str("k", b"abc");
        assert_eq!(
            map.append_str("k", b"de", 4).err(),
            Some(AppendRefused::TooLong)
        );
        assert_eq!(text(map.get("k")), Some(b"abc".to_vec()));
        // Inline, its entry a block of 16 bytes: a fourth byte takes the next.
        assert!(matches!(
            map.append_str("k", b"d", 4),
            Ok((4, Written::Replaced(_)))
        ));
        assert!(matches!(
            map.append_str("k", b"", 4),
            Ok((4, Written::InPlace))
        ));
        assert!(matches!(
            map.append_str("new", b"", 4),
            Ok((0, Written::New))
        ));
        assert_eq!(text(map.get("new")), Some(Vec::new()));
        // A key past its time is not there.
        map.insert_str_expiring("old", b"gone", Some(1));
        assert!(matches!(
            map.append_str("old", b"new", usize::MAX),
            Ok((3, Written::Replaced(_)))
        ));
        assert_eq!(text(map.get("old")), Some(b"new".to_vec()));
        assert_eq!(map.expires("old"), None);
    }

    #[test]
    fn a_command_sees_one_time_across_its_write_locks() {
        let shard = crate::store::ShardLock::default();
        let soon = crate::commands::helpers::now_ms() + 200;
        shard.write().insert_expiring("k", s("100"), Some(soon));
        let command = crate::script_clock::command();
        // Live when the command first looked at the time: a write that
        // follows keeps it, however long after, under this lock or another.
        assert!(shard.write().get("k").is_some());
        std::thread::sleep(std::time::Duration::from_millis(250));
        assert!(shard.read().get("k").is_some());
        let mut map = shard.write();
        map.insert("k", s("101"));
        assert_eq!(map.expires("k"), Some(soon));
        drop(map);
        drop(command);
        assert!(shard.read().get("k").is_none());
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
        let mut map = crate::cpus::spawn(move || {
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

    /// The most keys at one position of `map`: what a page may hold past
    /// `want`, since it never splits a position.
    fn most_at_one_position(map: &ShardMap) -> usize {
        let mut at = vec![0usize; SCAN_POSITIONS as usize];
        for slot in map.slots() {
            at[slot.scan_position()] += 1;
        }
        at.into_iter().max().unwrap_or(0)
    }

    /// Each page deleted as soon as it is given (`SCAN` piped into `DEL`):
    /// the keys left are then all past the cursor, far denser there than
    /// the shard's length says. A page still holds `want` keys and those
    /// that share the last one's position, never more, and the scan ends
    /// with the shard empty (#884).
    #[test]
    fn a_scan_that_deletes_each_page_keeps_its_pages_at_want() {
        for (want, n) in [(10, 10_000), (100, 30_000), (1000, 30_000)] {
            let mut map = ShardMap::new();
            for i in 0..n {
                map.insert_str(format!("k:{i}").as_bytes(), "v");
            }
            let ties = most_at_one_position(&map);
            let mut from = 0;
            let mut pages = 0;
            loop {
                let mut keys = Vec::new();
                let next = map.scan(from, want, |e| keys.push(e.key().to_vec()));
                pages += 1;
                assert!(
                    keys.len() < want + ties,
                    "a page of {} for {want}",
                    keys.len()
                );
                assert!(
                    keys.len() >= want || next == 0,
                    "a short page of {}",
                    keys.len()
                );
                for key in &keys {
                    assert!(map.remove(key.as_slice()).is_some());
                }
                if next == 0 {
                    break;
                }
                assert!(next > from);
                from = next;
            }
            assert!(
                map.is_empty(),
                "{} keys left after {pages} pages",
                map.len()
            );
        }
    }

    /// A shard whose keys before the middle position are gone holds none
    /// where its length says a page's worth would be: the page reaches
    /// further, to `want` keys, rather than coming back empty with a
    /// position to go on from (#884).
    #[test]
    fn a_page_reaches_past_positions_left_empty() {
        let mut map = ShardMap::new();
        for i in 0..30_000 {
            map.insert_str(format!("k:{i}").as_bytes(), "v");
        }
        let early: Vec<Vec<u8>> = map
            .slots()
            .filter(|slot| slot.scan_position() < 2048)
            .map(|slot| slot.entry().key().to_vec())
            .collect();
        for key in &early {
            assert!(map.remove(key.as_slice()).is_some());
        }
        let ties = most_at_one_position(&map);
        for want in [1, 100, 1000] {
            let mut n = 0;
            let next = map.scan(0, want, |_| n += 1);
            assert!(n >= want && n < want + ties, "a page of {n} for {want}");
            assert!(next > 2048, "goes on from {next}");
        }
    }

    /// A page that may take the rest of the shard (`want` at least its
    /// length) takes it (#884): every key at a position from `from` on,
    /// exactly the ones smaller pages from there give, in both tables of a
    /// growth, and the scan of the shard ends.
    #[test]
    fn a_page_as_large_as_the_shard_takes_the_rest_of_it() {
        let (mut map, n) = growing();
        map.insert(String::from("new:0"), s("v"));
        assert!(map.is_growing());
        let all = map.len();
        for from in [0u64, 1, 700, 2048, SCAN_POSITIONS - 1] {
            let mut whole = Vec::new();
            assert_eq!(map.scan(from, all, |e| whole.push(e.key().to_vec())), 0);
            let mut paged = Vec::new();
            let mut at = from;
            loop {
                at = map.scan(at, 7, |e| paged.push(e.key().to_vec()));
                if at == 0 {
                    break;
                }
            }
            whole.sort();
            paged.sort();
            assert_eq!(whole, paged, "from {from}");
            if from == 0 {
                assert_eq!(whole.len(), all);
            }
        }
        // A first page of a few, then the rest at once: every key once.
        let mut seen = std::collections::HashMap::new();
        let mut note = |e: EntryView<'_>| *seen.entry(e.key().to_vec()).or_insert(0) += 1;
        let next = map.scan(0, 10, &mut note);
        assert_ne!(next, 0);
        map.step_growth(GROWTH_STEP);
        map.insert(String::from("new:1"), s("v"));
        assert_eq!(map.scan(next, usize::MAX, &mut note), 0);
        for i in 0..n {
            assert_eq!(seen.get(format!("g{i}").as_bytes()), Some(&1), "g{i}");
        }
    }

    /// A full table of keys `g0`, `g1`, ... with their numbers as values,
    /// large enough to grow in steps: the next write begins a growth.
    fn full() -> (ShardMap, usize) {
        let mut map = ShardMap::new();
        let mut n = 0;
        while map.len() < GROW_IN_STEPS_FROM || map.len() != map.capacity() {
            map.insert(format!("g{n}"), s(&n.to_string()));
            n += 1;
        }
        assert!(!map.is_growing());
        (map, n)
    }

    /// After a write that may have begun a growth, of a key that was there:
    /// one entry for each of the `n` keys, `key` holding `value`, through
    /// the end of the growth, and nothing left once it is taken out.
    fn written_over_once(mut map: ShardMap, n: usize, key: &str, value: &str) {
        assert_eq!(map.len(), n);
        each_key_once(&map);
        assert_eq!(text(map.get(key)), Some(value.as_bytes().to_vec()));
        while !map.step_growth(GROWTH_STEP) {}
        assert_eq!(map.len(), n);
        assert_eq!(map.keys().count(), n);
        assert_eq!(text(map.get(key)), Some(value.as_bytes().to_vec()));
        assert!(map.remove(key).is_some());
        assert!(map.get(key).is_none());
        assert!(!map.keys().any(|k| k == key.as_bytes()));
        assert_eq!(map.len(), n - 1);
        assert_eq!(map.ttl_len(), 0);
        assert_eq!(map.ttl.iter().count(), 0);
    }

    /// The write that begins a growth writes over a key that is there: the
    /// growth moves every key to the old table, and the write must find it
    /// there, not add it again to the new one (#780 review).
    #[test]
    fn a_write_over_a_key_that_begins_a_growth_keeps_one_entry() {
        // A new value, in a new entry.
        let (mut map, n) = full();
        let long = "a value too long to be written in place over the one it had";
        assert!(map.insert("g5", s(long)).is_some());
        assert!(map.is_growing());
        written_over_once(map, n, "g5", long);

        // Written in place.
        let (mut map, n) = full();
        assert!(matches!(map.set_str("g6", "7"), Written::InPlace));
        assert!(map.is_growing());
        written_over_once(map, n, "g6", "7");

        // A key with a time to live given a new one: one place on the list.
        let (mut map, n) = full();
        assert!(map.set_expires("g7", u64::MAX / 4));
        assert!(!map.is_growing());
        assert!(
            map.insert_expiring("g7", s("x"), Some(u64::MAX / 2))
                .is_some()
        );
        assert!(map.is_growing());
        assert_eq!(map.ttl_len(), 1);
        assert_eq!(map.expires("g7"), Some(u64::MAX / 2));
        written_over_once(map, n, "g7", "x");

        // An entry put back under a live key: refused, the key as it was.
        let (mut map, n) = full();
        let mut other = ShardMap::new();
        other.insert("g8", s("elsewhere"));
        let taken = other.remove("g8").unwrap();
        assert!(map.put_back(taken).is_err());
        written_over_once(map, n, "g8", "8");

        // Put back over a key past its time: written over, once.
        let (mut map, n) = full();
        assert!(map.set_expires("g9", 1));
        let mut other = ShardMap::new();
        other.insert("g9", s("back"));
        let taken = other.remove("g9").unwrap();
        assert!(matches!(map.put_back(taken), Ok(Some(_))));
        assert_eq!(map.ttl_len(), 0);
        written_over_once(map, n, "g9", "back");

        // `INCR`'s way: read, then written over.
        let (mut map, n) = full();
        let was = text(map.get("g10")).unwrap();
        assert_eq!(was, b"10");
        assert!(!matches!(map.set_str("g10", "11"), Written::New));
        written_over_once(map, n, "g10", "11");
    }

    /// A table that is growing, and full (or one key short of it), with
    /// keys `f0`... in the new table and `g0`... still in the old one.
    fn growing_and_full(spare: usize) -> (ShardMap, Vec<String>, usize) {
        let (mut from, _) = growing();
        let mut map = ShardMap::new();
        map.hash = from.hash.clone();
        let mut n = 0;
        while map.len() < GROW_IN_STEPS_FROM || map.len() + spare != map.capacity() {
            map.insert(format!("f{n}"), s("f"));
            n += 1;
        }
        map.old = std::mem::take(&mut from.old);
        map.moving_at = 0;
        let old = in_old(&map);
        assert!(map.is_growing());
        let len = map.len();
        (map, old, len)
    }

    /// A table that fills while it grows finishes that growth and begins
    /// another: a key written over then is still found, once.
    #[test]
    fn a_write_over_a_key_while_a_full_table_grows_keeps_one_entry() {
        for spare in [0, 1] {
            // A key still in the old table, and one in the new.
            for key in [None, Some("f3")] {
                let (mut map, old, n) = growing_and_full(spare);
                let key = key.map_or(old[0].clone(), str::to_string);
                assert!(map.insert(&key, s("again")).is_some());
                written_over_once(map, n, &key, "again");
            }
            // A new key: one more.
            let (mut map, _, n) = growing_and_full(spare);
            assert!(map.insert("brand new", s("n")).is_none());
            written_over_once(map, n + 1, "brand new", "n");
        }
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
