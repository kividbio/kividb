//! One shard of a database's keyspace, behind an interface that does not
//! say how it is kept.
//!
//! Commands ask for a key's value and get a [`ValueRef`], a view of it that
//! lives as long as the shard's lock. They never see a `&KiviDBValue` from
//! the table: that is what lets the table change how it keeps a key and its
//! value (one allocation for both)
//! without every command changing again. A value going in, coming out or
//! moving between keys is still an owned [`KiviDBValue`].
//!
//! The table is hashbrown's, as the standard library's `HashMap` is, used
//! directly: its buckets can be reached by index, which is what a cursor
//! that survives the lock being let go (compaction), and a key picked at
//! random (`RANDOMKEY`, eviction), need. Through a `HashMap` the only way
//! to the n-th key was to walk past the n before it.

use std::collections::VecDeque;
use std::sync::Arc;

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use parking_lot::RwLock;

use crate::shared_bytes::SharedBytes;
use crate::store::Key;
use crate::types::{
    ArrayInner, HashFields, HashInner, KiviDBValue, SortedSetInner, StreamInner, TsInner,
    VectorSetInner,
};

/// A key's string value, borrowed from its shard.
#[derive(Clone, Copy)]
pub struct StrRef<'a>(&'a SharedBytes);

impl<'a> StrRef<'a> {
    #[inline]
    pub fn as_bytes(self) -> &'a [u8] {
        self.0.as_slice()
    }

    /// The bytes, to keep once the shard is let go of: a second hold on the
    /// same block, not a copy.
    #[inline]
    pub fn to_shared(self) -> SharedBytes {
        self.0.clone()
    }

    /// Whether `other` is this very value, not merely equal bytes.
    #[inline]
    pub fn is(self, other: &SharedBytes) -> bool {
        SharedBytes::ptr_eq(self.0, other)
    }

    /// Where the bytes are, for compaction (`value_slab::is_evacuating`).
    #[inline]
    pub fn block(self) -> *const u8 {
        self.0.block()
    }
}

impl std::ops::Deref for StrRef<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl AsRef<[u8]> for StrRef<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
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
            KiviDBValue::String(v) => ValueRef::String(StrRef(v)),
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

    /// [`KiviDBValue::same_value`].
    pub fn same_value(self, other: &KiviDBValue) -> bool {
        match (self, other) {
            (ValueRef::String(a), KiviDBValue::String(b)) => a.is(b),
            (ValueRef::List(a), KiviDBValue::List(b)) => Arc::ptr_eq(a, b),
            (ValueRef::Hash(a), KiviDBValue::Hash(b)) => Arc::ptr_eq(a, b),
            (ValueRef::Json(a), KiviDBValue::Json(b)) => Arc::ptr_eq(a, b),
            (ValueRef::Set(a), KiviDBValue::Set(b)) => Arc::ptr_eq(a, b),
            (ValueRef::VectorSet(a), KiviDBValue::VectorSet(b)) => Arc::ptr_eq(a, b),
            (ValueRef::SortedSet(a), KiviDBValue::SortedSet(b)) => Arc::ptr_eq(a, b),
            (ValueRef::Stream(a), KiviDBValue::Stream(b)) => Arc::ptr_eq(a, b),
            (ValueRef::TimeSeries(a), KiviDBValue::TimeSeries(b)) => Arc::ptr_eq(a, b),
            (ValueRef::HashEx(a), KiviDBValue::HashEx(b)) => Arc::ptr_eq(a, b),
            (ValueRef::Array(a), KiviDBValue::Array(b)) => Arc::ptr_eq(a, b),
            _ => false,
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

/// What [`ShardMap::relocate_string`] did.
pub enum Relocated {
    /// Moved: the old block, to be let go of once the shard is unlocked.
    Moved(SharedBytes),
    /// In a slab being emptied, but held by something besides its key.
    Held,
    /// Not a string, not there, or not in a slab being emptied.
    Stays,
}

/// One shard's keys and values.
pub struct ShardMap {
    table: HashTable<(Key, KiviDBValue)>,
    hash: ahash::RandomState,
}

impl Default for ShardMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardMap {
    pub fn new() -> Self {
        ShardMap {
            table: HashTable::new(),
            hash: ahash::RandomState::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ShardMap {
            table: HashTable::with_capacity(capacity),
            hash: ahash::RandomState::new(),
        }
    }

    #[inline]
    fn hash_of(&self, key: &str) -> u64 {
        self.hash.hash_one(key)
    }

    #[inline]
    pub fn get(&self, key: &str) -> Option<ValueRef<'_>> {
        self.table
            .find(self.hash_of(key), |(k, _)| k.as_str() == key)
            .map(|(_, v)| ValueRef::from(v))
    }

    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        self.table
            .find(self.hash_of(key), |(k, _)| k.as_str() == key)
            .is_some()
    }

    /// Set `key` to `value`, and give back the value it had. A key that was
    /// there keeps its stored name: no new one is made for it.
    #[inline]
    pub fn insert(&mut self, key: impl AsRef<str>, value: KiviDBValue) -> Option<KiviDBValue> {
        let key = key.as_ref();
        let hash = self.hash_of(key);
        let hasher = &self.hash;
        match self.table.entry(
            hash,
            |(k, _)| k.as_str() == key,
            |(k, _)| hasher.hash_one(k.as_str()),
        ) {
            Entry::Occupied(mut slot) => Some(std::mem::replace(&mut slot.get_mut().1, value)),
            Entry::Vacant(place) => {
                place.insert((Key::from(key), value));
                None
            }
        }
    }

    #[inline]
    pub fn remove(&mut self, key: &str) -> Option<KiviDBValue> {
        let hash = self.hash_of(key);
        match self.table.find_entry(hash, |(k, _)| k.as_str() == key) {
            Ok(slot) => Some(slot.remove().0.1),
            Err(_) => None,
        }
    }

    /// The value of `key`, made with `make` if there is none.
    pub fn get_or_insert_with(
        &mut self,
        key: &str,
        make: impl FnOnce() -> KiviDBValue,
    ) -> ValueRef<'_> {
        let hash = self.hash_of(key);
        let hasher = &self.hash;
        let slot = self
            .table
            .entry(
                hash,
                |(k, _)| k.as_str() == key,
                |(k, _)| hasher.hash_one(k.as_str()),
            )
            .or_insert_with(|| (Key::from(key), make()));
        ValueRef::from(&slot.into_mut().1)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, ValueRef<'_>)> {
        self.table
            .iter()
            .map(|(k, v)| (k.as_str(), ValueRef::from(v)))
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.table.iter().map(|(k, _)| k.as_str())
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'_>> {
        self.table.iter().map(|(_, v)| ValueRef::from(v))
    }

    /// Every key and value, taken out: the table is left empty, with the
    /// room it had.
    pub fn drain(&mut self) -> impl Iterator<Item = (Key, KiviDBValue)> + '_ {
        self.table.drain()
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

    pub fn clear(&mut self) {
        self.table.clear();
    }

    pub fn shrink_to(&mut self, keys: usize) {
        let hasher = &self.hash;
        self.table
            .shrink_to(keys, |(k, _)| hasher.hash_one(k.as_str()));
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
        self.table
            .get_bucket(index)
            .map(|(k, v)| (k.as_str(), ValueRef::from(v)))
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
    /// random bucket is tried until one is full, as Redis's `dictGetFairRandomKey`
    /// does; a table that is very sparse is walked from a random bucket.
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

    /// Move `key`'s string to a new block if its block is in a slab
    /// compaction is emptying and nothing else holds it
    /// ([`SharedBytes::relocate`]).
    pub fn relocate_string(&mut self, key: &str, regions: &[usize]) -> Relocated {
        let hash = self.hash_of(key);
        match self.table.find_mut(hash, |(k, _)| k.as_str() == key) {
            Some((_, KiviDBValue::String(bytes))) => {
                if !crate::value_slab::is_evacuating(bytes.block(), regions) {
                    return Relocated::Stays;
                }
                match bytes.relocate(regions) {
                    Some(old) => Relocated::Moved(old),
                    None => Relocated::Held,
                }
            }
            _ => Relocated::Stays,
        }
    }
}

impl FromIterator<(Key, KiviDBValue)> for ShardMap {
    fn from_iter<I: IntoIterator<Item = (Key, KiviDBValue)>>(items: I) -> Self {
        let mut map = ShardMap::new();
        for (key, value) in items {
            map.insert(key, value);
        }
        map
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
        let old = map.insert(Key::from("a"), s("2"));
        assert!(matches!(old, Some(KiviDBValue::String(b)) if &b[..] == b"1"));
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
