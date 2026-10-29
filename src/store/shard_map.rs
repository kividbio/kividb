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
//! step. Beside the table, the shard keeps a list of the entries that have
//! one, each entry knowing its place in it: what active expiry goes through
//! and what the `volatile-*` policies pick from, at random in one step.

use std::collections::VecDeque;
use std::sync::Arc;

use hashbrown::HashTable;
use parking_lot::RwLock;

use super::entry::{Entry, Left, Relocation, Slot, prefetch_expiry};
pub use super::entry::{EntryRef, Taken};
use crate::shared_bytes::SharedBytes;
use crate::types::{
    ArrayInner, HashFields, HashInner, KiviDBValue, SortedSetInner, StreamInner, TsInner,
    VectorSetInner,
};

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
            | ValueRef::Array(_) => false,
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

/// One shard's keys and values: a table of [`Slot`]s, each the address of
/// an entry holding a key and its value (`entry`).
pub struct ShardMap {
    table: HashTable<Slot>,
    hash: ahash::RandomState,
    /// Every entry whose key has a time to live, each knowing where it is
    /// here (`Entry::pos`). The order means nothing: a key that loses its
    /// time to live, or goes, has the last one moved into its place.
    ttl: Vec<Entry>,
}

/// What a write does with the time to live the key has.
#[derive(Clone, Copy)]
enum Ttl {
    /// Keeps it (none, if the key is new).
    Keep,
    /// Gives the key this one, or takes it away.
    Set(Option<u64>),
}

/// `entry`, which has a time to live, put at the end of the list.
#[inline]
fn ttl_push(ttl: &mut Vec<Entry>, entry: Entry) {
    assert!(ttl.len() < u32::MAX as usize, "keys with a time to live");
    entry.set_pos(ttl.len());
    ttl.push(entry);
}

/// `entry` taken out of the list, the last one moved into its place.
#[inline]
fn ttl_take(ttl: &mut Vec<Entry>, entry: Entry) {
    let at = entry.pos();
    debug_assert!(ttl[at] == entry);
    ttl.swap_remove(at);
    if let Some(moved) = ttl.get(at) {
        moved.set_pos(at);
    }
}

/// The list made right for `new` taking `old`'s place in the table: either
/// of them may have a time to live.
#[inline(always)]
fn ttl_replaced(ttl: &mut Vec<Entry>, old: Entry, new: Entry) {
    match (old.has_ttl(), new.has_ttl()) {
        (false, false) => {}
        (true, true) => {
            let at = old.pos();
            new.set_pos(at);
            ttl[at] = new;
        }
        (true, false) => ttl_take(ttl, old),
        (false, true) => ttl_push(ttl, new),
    }
}

/// How far ahead a pass over the list asks for entries' lines.
const TTL_AHEAD: usize = 8;

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
        ShardMap {
            table: HashTable::new(),
            hash: ahash::RandomState::new(),
            ttl: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ShardMap {
            table: HashTable::with_capacity(capacity),
            hash: ahash::RandomState::new(),
            ttl: Vec::new(),
        }
    }

    #[inline(always)]
    fn hash_of(&self, key: &[u8]) -> u64 {
        self.hash.hash_one(key)
    }

    /// The slot of `key`, hashed `hash`.
    #[inline(always)]
    fn find(&self, hash: u64, key: &[u8]) -> Option<Slot> {
        self.table
            .find(hash, |s| s.may_be(hash) && s.entry().has_key(key))
            .copied()
    }

    #[inline(always)]
    pub fn get(&self, key: &str) -> Option<ValueRef<'_>> {
        let key = key.as_bytes();
        self.find(self.hash_of(key), key).map(|s| s.entry().value())
    }

    /// `key`'s value, and when its time to live ends (milliseconds since
    /// the Unix epoch) if it has one: both from the one entry.
    #[inline(always)]
    pub fn get_expiring(&self, key: &str) -> Option<(ValueRef<'_>, Option<u64>)> {
        let key = key.as_bytes();
        self.find(self.hash_of(key), key).map(|s| {
            let e = s.entry();
            (e.value(), e.expires())
        })
    }

    /// When `key`'s time to live ends, in milliseconds since the Unix epoch:
    /// `None` if it has none, or there is no such key.
    #[inline(always)]
    pub fn expires(&self, key: &str) -> Option<u64> {
        let key = key.as_bytes();
        self.find(self.hash_of(key), key)
            .and_then(|s| s.entry().expires())
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &str) -> bool {
        let key = key.as_bytes();
        self.find(self.hash_of(key), key).is_some()
    }

    /// A hold on `key`'s entry: its key and value, readable once the shard
    /// is let go of, and unchanged for as long as it is held.
    pub fn get_ref(&self, key: &str) -> Option<EntryRef> {
        let key = key.as_bytes();
        // SAFETY: the entry is in this table, which the caller has locked.
        self.find(self.hash_of(key), key)
            .map(|s| unsafe { EntryRef::new(s.entry()) })
    }

    /// Whether `key`'s entry is the one `held` holds: whether the key's
    /// value is the very one it had when `held` was taken.
    pub fn holds(&self, key: &str, held: &EntryRef) -> bool {
        let key = key.as_bytes();
        self.find(self.hash_of(key), key)
            .is_some_and(|s| held.is(s.entry()))
    }

    /// Put an entry under `key`, and give back what the key had. One
    /// probe, whether the key is there or not.
    ///
    /// `make` makes the entry, given the time to live it is to have: the one
    /// `ttl` says, or the key's own if it keeps it. Made once the key is
    /// found, so that keeping a time to live costs nothing more.
    #[inline(always)]
    fn put(
        &mut self,
        key: &[u8],
        ttl: Ttl,
        make: impl FnOnce(&[u8], Option<u64>) -> Entry,
    ) -> Option<Taken> {
        let hash = self.hash_of(key);
        let hasher = &self.hash;
        let list = &mut self.ttl;
        match self.table.entry(
            hash,
            |s| s.may_be(hash) && s.entry().has_key(key),
            |s| hasher.hash_one(s.entry().key()),
        ) {
            hashbrown::hash_table::Entry::Occupied(mut at) => {
                let old = at.get().entry();
                let entry = make(
                    key,
                    match ttl {
                        Ttl::Keep => old.expires(),
                        Ttl::Set(expires) => expires,
                    },
                );
                *at.get_mut() = Slot::new(entry, hash);
                ttl_replaced(list, old, entry);
                // SAFETY: the table's hold on the old entry goes to the caller.
                Some(unsafe { Taken::new(old) })
            }
            hashbrown::hash_table::Entry::Vacant(place) => {
                let entry = make(
                    key,
                    match ttl {
                        Ttl::Keep => None,
                        Ttl::Set(expires) => expires,
                    },
                );
                place.insert(Slot::new(entry, hash));
                if entry.has_ttl() {
                    ttl_push(list, entry);
                }
                None
            }
        }
    }

    /// Set `key` to `value`, and give back what it had: dropping that frees
    /// it, which is best done once the shard is unlocked. The key keeps its
    /// time to live.
    #[inline]
    pub fn insert(&mut self, key: impl AsRef<str>, value: KiviDBValue) -> Option<Taken> {
        self.put(key.as_ref().as_bytes(), Ttl::Keep, |key, expires| {
            Entry::new(key, value, expires)
        })
    }

    /// [`ShardMap::insert`], the key's time to live set to `expires`
    /// (milliseconds since the Unix epoch), or taken away.
    #[inline]
    pub fn insert_expiring(
        &mut self,
        key: impl AsRef<str>,
        value: KiviDBValue,
        expires: Option<u64>,
    ) -> Option<Taken> {
        self.put(
            key.as_ref().as_bytes(),
            Ttl::Set(expires),
            |key, expires| Entry::new(key, value, expires),
        )
    }

    /// Set `key` to the string `bytes`: [`ShardMap::insert`] of a string,
    /// with its bytes copied once, into the entry.
    #[inline]
    pub fn insert_str(&mut self, key: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> Option<Taken> {
        let bytes = bytes.as_ref();
        self.put(key.as_ref().as_bytes(), Ttl::Keep, |key, expires| {
            Entry::string(key, bytes, expires)
        })
    }

    /// [`ShardMap::insert_str`], the key's time to live set to `expires`, or
    /// taken away.
    #[inline]
    pub fn insert_str_expiring(
        &mut self,
        key: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        expires: Option<u64>,
    ) -> Option<Taken> {
        let bytes = bytes.as_ref();
        self.put(
            key.as_ref().as_bytes(),
            Ttl::Set(expires),
            |key, expires| Entry::string(key, bytes, expires),
        )
    }

    /// Put an entry made before the shard was locked ([`NewEntry`]), with
    /// the time to live it was made with, or, if it was made to keep the
    /// key's, that one.
    #[inline]
    pub fn insert_entry(&mut self, entry: NewEntry) -> Option<Taken> {
        let (made, keep) = (entry.entry, entry.keep);
        std::mem::forget(entry);
        let ttl = if keep {
            Ttl::Keep
        } else {
            Ttl::Set(made.expires())
        };
        self.put(made.key(), ttl, |_, expires| {
            if expires == made.expires() {
                made
            } else {
                // The key had a time to live to keep: made again with it.
                let again = made.remade(expires);
                // SAFETY: the only hold, never put in a table.
                unsafe { made.release() };
                again
            }
        })
    }

    /// Give `key` the time to live that ends at `expires` (milliseconds
    /// since the Unix epoch). Returns whether there is such a key.
    pub fn set_expires(&mut self, key: &str, expires: u64) -> bool {
        let key = key.as_bytes();
        let hash = self.hash_of(key);
        let Some(slot) = self
            .table
            .find_mut(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        else {
            return false;
        };
        let old = slot.entry();
        if old.has_ttl() && old.holders() == 1 {
            // SAFETY: the shard is locked for writing, and nothing but the
            // table holds the entry.
            unsafe { old.set_expires(expires) };
            return true;
        }
        let new = old.remade(Some(expires));
        *slot = Slot::new(new, slot.hash_part());
        ttl_replaced(&mut self.ttl, old, new);
        // SAFETY: the table's hold, given up: whoever else holds the old
        // entry still reads it as it was.
        unsafe { old.release() };
        true
    }

    /// Take `key`'s time to live away. Returns whether it had one.
    pub fn persist(&mut self, key: &str) -> bool {
        let key = key.as_bytes();
        let hash = self.hash_of(key);
        let Some(slot) = self
            .table
            .find_mut(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        else {
            return false;
        };
        let old = slot.entry();
        if !old.has_ttl() {
            return false;
        }
        let new = old.remade(None);
        *slot = Slot::new(new, slot.hash_part());
        ttl_replaced(&mut self.ttl, old, new);
        // SAFETY: as in `set_expires`.
        unsafe { old.release() };
        true
    }

    /// How many keys have a time to live.
    #[inline]
    pub fn ttl_len(&self) -> usize {
        self.ttl.len()
    }

    /// The key at place `index` of the list of keys with a time to live,
    /// its value and when its time ends: what a pick at random from them is
    /// (`index` modulo how many there are).
    pub fn ttl_entry(&self, index: usize) -> Option<(&str, ValueRef<'_>, u64)> {
        if self.ttl.is_empty() {
            return None;
        }
        let e = self.ttl[index % self.ttl.len()];
        Some((e.key_str(), e.value(), e.expires().unwrap_or(0)))
    }

    /// Every key with a time to live, and when it ends.
    pub fn expiring(&self) -> impl Iterator<Item = (&str, u64)> {
        self.ttl
            .iter()
            .map(|e| (e.key_str(), e.expires().unwrap_or(0)))
    }

    /// The keys whose time to live ended before `now` (milliseconds since
    /// the Unix epoch), at most `limit` of them: one pass over the list,
    /// stopping at the last one it takes. Returns them, and how many keys
    /// it looked at.
    pub fn due(&self, now: u64, limit: usize) -> (Vec<crate::store::Key>, usize) {
        let list = &self.ttl;
        let mut due = Vec::new();
        for (i, e) in list.iter().enumerate() {
            // Each entry is somewhere else in memory: a few are asked for
            // ahead, so that their lines come in together.
            if let Some(ahead) = list.get(i + TTL_AHEAD) {
                prefetch_expiry(*ahead);
            }
            if e.expires().is_some_and(|at| at < now) {
                due.push(crate::store::Key::from(e.key_str()));
                if due.len() == limit {
                    return (due, i + 1);
                }
            }
        }
        (due, list.len())
    }

    /// Take `key` out if its time to live ended before `now`.
    pub fn remove_if_due(&mut self, key: &str, now: u64) -> Option<Taken> {
        if self.expires(key).is_some_and(|at| at < now) {
            self.remove(key)
        } else {
            None
        }
    }

    #[inline]
    pub fn remove(&mut self, key: &str) -> Option<Taken> {
        let key = key.as_bytes();
        let hash = self.hash_of(key);
        match self
            .table
            .find_entry(hash, |s| s.may_be(hash) && s.entry().has_key(key))
        {
            Ok(at) => {
                let entry = at.remove().0.entry();
                if entry.has_ttl() {
                    ttl_take(&mut self.ttl, entry);
                }
                // SAFETY: the table's hold goes to the caller.
                Some(unsafe { Taken::new(entry) })
            }
            Err(_) => None,
        }
    }

    /// The value of `key`, made with `make` if there is none.
    pub fn get_or_insert_with(
        &mut self,
        key: &str,
        make: impl FnOnce() -> KiviDBValue,
    ) -> ValueRef<'_> {
        if !self.contains_key(key) {
            self.insert(key, make());
        }
        self.get(key).expect("a key just put in")
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, ValueRef<'_>)> {
        self.table.iter().map(|s| {
            let e = s.entry();
            (e.key_str(), e.value())
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.table.iter().map(|s| s.entry().key_str())
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'_>> {
        self.table.iter().map(|s| s.entry().value())
    }

    /// Every entry, each as a key, a value and the means to hold it.
    pub fn entries(&self) -> impl Iterator<Item = EntryView<'_>> {
        self.table.iter().map(|s| EntryView {
            entry: s.entry(),
            _map: std::marker::PhantomData,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Keys the table has room for before it grows.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.table.capacity()
    }

    /// Take every key out, keeping the table's room.
    pub fn clear(&mut self) {
        self.ttl.clear();
        for slot in self.table.drain() {
            // SAFETY: the table's hold, given up.
            unsafe { slot.entry().release() };
        }
    }

    /// Shrink the table to room for `keys`, and the list of keys with a
    /// time to live, if it is sparse, to its keys and an eighth more.
    pub fn shrink_to(&mut self, keys: usize) {
        let hasher = &self.hash;
        self.table
            .shrink_to(keys, |s| hasher.hash_one(s.entry().key()));
        if sparse(self.ttl.len(), self.ttl.capacity()) {
            self.ttl.shrink_to(self.ttl.len() + self.ttl.len() / 8);
        }
    }

    /// Whether the table, or the list of keys with a time to live, has room
    /// for many more keys than it holds (see `store::shrink_if_sparse`).
    pub fn is_sparse(&self) -> bool {
        sparse(self.len(), self.capacity()) || sparse(self.ttl.len(), self.ttl.capacity())
    }

    /// How many buckets the table has: the positions [`ShardMap::bucket`]
    /// takes. They stay where they are while keys come and go, until the
    /// table grows or shrinks.
    #[inline]
    pub fn buckets(&self) -> usize {
        self.table.num_buckets()
    }

    /// The key and value in bucket `index`, if it holds one.
    #[inline]
    pub fn bucket(&self, index: usize) -> Option<(&str, ValueRef<'_>)> {
        self.table.get_bucket(index).map(|s| {
            let e = s.entry();
            (e.key_str(), e.value())
        })
    }

    /// Up to `count` keys and values from the buckets at and after `start`
    /// (taken modulo the bucket count), wrapping round once: what a sample
    /// from a random place in the table is. Costs about `count` divided by
    /// how full the table is, where walking to the n-th key cost n.
    pub fn sample_from(
        &self,
        start: usize,
        count: usize,
    ) -> impl Iterator<Item = (&str, ValueRef<'_>)> {
        let buckets = self.buckets();
        let start = if buckets == 0 { 0 } else { start % buckets };
        (0..buckets)
            .filter_map(move |step| self.bucket((start + step) % buckets))
            .take(if self.is_empty() { 0 } else { count })
    }

    /// One key picked at random, `pick()` giving the random numbers. A
    /// random bucket is tried until one is full, as Redis's
    /// `dictGetFairRandomKey` does; a table that is very sparse is walked
    /// from a random bucket.
    pub fn random_entry(&self, mut pick: impl FnMut() -> usize) -> Option<(&str, ValueRef<'_>)> {
        if self.is_empty() {
            return None;
        }
        let buckets = self.buckets();
        for _ in 0..64 {
            if let Some(found) = self.bucket(pick() % buckets) {
                return Some(found);
            }
        }
        self.sample_from(pick(), 1).next()
    }

    /// Whether the entry in bucket `index`, or its string's bytes, is in a
    /// slab compaction is emptying (`regions` is
    /// `value_slab::regions_snapshot`).
    pub fn evacuating_at(&self, index: usize, regions: &[usize]) -> bool {
        let Some(slot) = self.table.get_bucket(index) else {
            return false;
        };
        let entry = slot.entry();
        (crate::value_slab::class_of(entry.size()).is_some()
            && crate::value_slab::is_evacuating(entry.base(), regions))
            || matches!(entry.value(), ValueRef::String(s)
                if s.shared().is_some_and(|b| crate::value_slab::is_evacuating(b.block(), regions)))
    }

    /// Move the entry in bucket `index`, or its string's bytes, out of a
    /// slab compaction is emptying, if the table's is its only hold.
    pub fn relocate_at(&mut self, index: usize, regions: &[usize]) -> Relocated {
        let Some(slot) = self.table.get_bucket_mut(index) else {
            return Relocated::Stays;
        };
        let entry = slot.entry();
        // SAFETY: the table is locked for writing (`&mut self`), and holds
        // the entry.
        match unsafe { entry.relocate(regions) } {
            Relocation::Stays => Relocated::Stays,
            Relocation::Held => Relocated::Held,
            Relocation::Moved { to, left } => {
                // The hash is not stored; the fragment is in the slot.
                *slot = slot.moved_to(to);
                // The position moved with the block.
                if to.has_ttl() {
                    self.ttl[to.pos()] = to;
                }
                Relocated::Moved(Leftover::Entry(left))
            }
            Relocation::BytesMoved(old) => Relocated::Moved(Leftover::Bytes(old)),
        }
    }
}

/// A key's entry, made before its shard is locked: what the shard then
/// waits for is only the table. Freed if dropped unused.
pub struct NewEntry {
    entry: Entry,
    /// Whether the key keeps the time to live it has.
    keep: bool,
}

impl NewEntry {
    /// The key keeps its time to live, as [`ShardMap::insert`].
    pub fn new(key: &str, value: KiviDBValue) -> Self {
        NewEntry {
            entry: Entry::new(key.as_bytes(), value, None),
            keep: true,
        }
    }

    /// The string `bytes`, copied once, into the entry. The key keeps its
    /// time to live.
    pub fn string(key: &str, bytes: &[u8]) -> Self {
        NewEntry {
            entry: Entry::string(key.as_bytes(), bytes, None),
            keep: true,
        }
    }

    /// The string `bytes`, the key's time to live to be `expires`, or none.
    pub fn string_expiring(key: &str, bytes: &[u8], expires: Option<u64>) -> Self {
        NewEntry {
            entry: Entry::string(key.as_bytes(), bytes, expires),
            keep: false,
        }
    }
}

impl Drop for NewEntry {
    fn drop(&mut self) {
        // SAFETY: the only hold, never put in a table.
        unsafe { self.entry.release() };
    }
}

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
    pub fn key(&self) -> &'a str {
        self.entry.key_str()
    }

    pub fn value(&self) -> ValueRef<'a> {
        self.entry.value()
    }

    /// When the key's time to live ends, if it has one.
    pub fn expires(&self) -> Option<u64> {
        self.entry.expires()
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
        let mut seen: Vec<String> = (0..map.buckets())
            .filter_map(|i| map.bucket(i).map(|(k, _)| k.to_string()))
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
        assert_eq!(map.random_entry(|| 0).map(|(k, _)| k), Some("k0"));
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
}
