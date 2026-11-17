//! The fields of a hash and their values.
//!
//! Fields keep the order they were first set in, which is the order
//! `HGETALL`, `HKEYS`, `HVALS` and `HSCAN` give them in. Redis keeps that
//! order for a hash small enough for a listpack, and clients have come to
//! rely on it (BullMQ's tests among them).
//!
//! Callers see fields as `&str` and values as [`Value`]s: bytes, whatever
//! the hash keeps them as. Snapshots, the AOF, replication and `DUMP` see
//! nothing else, so none of them depends on the form.

use std::ops::Deref;
use std::sync::Arc;

type Table = indexmap::IndexMap<String, Arc<[u8]>, ahash::RandomState>;

/// Up to this many fields, removing one keeps the others in order, as in a
/// Redis listpack (`hash-max-listpack-entries`). A larger hash is a hash
/// table in Redis too, with no order to keep: there the last field takes the
/// removed one's place, which costs nothing.
pub const HASH_ORDERED_FIELDS: usize = 512;

/// A value, as a hash hands it out: the table's shared bytes, or bytes held
/// in place. Either is its bytes (`Deref`).
#[derive(Clone, Copy)]
pub enum Value<'a> {
    Shared(&'a Arc<[u8]>),
    Bytes(&'a [u8]),
}

impl<'a> Value<'a> {
    /// The value as shared bytes: the table's own, or a new copy.
    #[inline]
    pub fn to_shared(self) -> Arc<[u8]> {
        match self {
            Value::Shared(bytes) => Arc::clone(bytes),
            Value::Bytes(bytes) => Arc::from(bytes),
        }
    }

    /// The bytes, for as long as the hash is borrowed.
    #[inline]
    pub fn bytes(self) -> &'a [u8] {
        match self {
            Value::Shared(bytes) => bytes,
            Value::Bytes(bytes) => bytes,
        }
    }
}

impl Deref for Value<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Value::Shared(bytes) => bytes,
            Value::Bytes(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for Value<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for Value<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self))
    }
}

/// A hash's fields and values.
#[derive(Clone, Default)]
pub struct HashFields {
    table: Table,
}

impl HashFields {
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty hash that is to hold about `fields`.
    pub fn with_capacity(fields: usize) -> Self {
        Self {
            table: Table::with_capacity_and_hasher(fields, Default::default()),
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

    /// The value of `field`, if the hash has it.
    #[inline]
    pub fn get(&self, field: &str) -> Option<Value<'_>> {
        self.table.get(field).map(Value::Shared)
    }

    /// The bytes of `field`'s value, if the hash has it.
    #[inline]
    pub fn get_bytes(&self, field: &str) -> Option<&[u8]> {
        self.table.get(field).map(|v| &**v)
    }

    #[inline]
    pub fn contains_key(&self, field: &str) -> bool {
        self.table.contains_key(field)
    }

    /// Field `i` and its value, in the order of [`Self::iter`].
    #[inline]
    pub fn get_index(&self, i: usize) -> Option<(&str, Value<'_>)> {
        self.table
            .get_index(i)
            .map(|(f, v)| (f.as_str(), Value::Shared(v)))
    }

    /// Give `field` the value `value`. Whether the field is new.
    #[inline]
    pub fn insert(&mut self, field: &str, value: &Arc<[u8]>) -> bool {
        self.table
            .insert(field.to_owned(), Arc::clone(value))
            .is_none()
    }

    /// [`Self::insert`] of bytes.
    #[inline]
    pub fn insert_bytes(&mut self, field: &str, value: &[u8]) -> bool {
        self.table
            .insert(field.to_owned(), Arc::from(value))
            .is_none()
    }

    /// [`Self::insert`] of a field and a value that are the caller's to
    /// give away (a load).
    #[inline]
    pub fn insert_owned(&mut self, field: String, value: Arc<[u8]>) -> bool {
        self.table.insert(field, value).is_none()
    }

    /// Remove `field`. Whether the hash had it. Up to
    /// [`HASH_ORDERED_FIELDS`] fields the others keep their order.
    #[inline]
    pub fn remove(&mut self, field: &str) -> bool {
        self.take(field).is_some()
    }

    /// [`Self::remove`], handing back the value.
    pub fn take(&mut self, field: &str) -> Option<Arc<[u8]>> {
        if self.table.len() <= HASH_ORDERED_FIELDS {
            self.table.shift_remove(field)
        } else {
            self.table.swap_remove(field)
        }
    }

    /// Keep the fields `keep` says to, in their order.
    pub fn retain(&mut self, mut keep: impl FnMut(&str, &[u8]) -> bool) {
        self.table.retain(|f, v| keep(f, v));
    }

    pub fn clear(&mut self) {
        self.table.clear();
    }

    /// Fields there is room for without growing.
    pub fn capacity(&self) -> usize {
        self.table.capacity()
    }

    /// The fields and their values, in order.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            inner: self.table.iter(),
        }
    }

    #[inline]
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &str> + '_ {
        self.iter().map(|(f, _)| f)
    }

    #[inline]
    pub fn values(&self) -> impl ExactSizeIterator<Item = Value<'_>> + '_ {
        self.iter().map(|(_, v)| v)
    }

    /// What the hash's memory is, its own header left out, for
    /// `MEMORY USAGE`: `samples` of the fields looked at, and the rest
    /// counted as they are.
    ///
    /// The table (`IndexMap`): its table of indexes, its vector of entries
    /// (hash, name, value), both with room for as many as it reports, and
    /// each name's and each value's block, sampled. What it reports is the
    /// lesser room of the two. That is the vector's for a hash made with
    /// room for its fields (as a snapshot loads one) and both's for one that
    /// grew; but deleting fields can leave the table fewer free buckets
    /// (hashbrown keeps a deleted bucket marked until it is rebuilt), and
    /// then part of the vector's room is not seen and not counted.
    pub fn layout_bytes(&self, samples: usize) -> usize {
        use crate::commands::memory_layout::{arc_slice, hash_table, sampled};
        let capacity = self.table.capacity();
        let indexes = hash_table(
            capacity,
            std::mem::size_of::<usize>(),
            std::mem::align_of::<usize>(),
        );
        let entries = capacity.saturating_mul(std::mem::size_of::<(usize, String, Arc<[u8]>)>());
        let len = self.table.len();
        // An `IndexMap` gets to its n-th entry at once, so the sample is
        // spread over the hash rather than taken from its first fields only.
        let bytes = |(name, value): (&String, &Arc<[u8]>)| name.capacity() + arc_slice(value.len());
        let each = if samples >= len {
            sampled(len, samples, || self.table.iter().map(bytes))
        } else {
            let step = (len / samples.max(1)).max(1);
            sampled(len, samples, || self.table.iter().step_by(step).map(bytes))
        };
        indexes + entries + each
    }
}

pub struct Iter<'a> {
    inner: indexmap::map::Iter<'a, String, Arc<[u8]>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a str, Value<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(f, v)| (f.as_str(), Value::Shared(v)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.inner
            .nth(n)
            .map(|(f, v)| (f.as_str(), Value::Shared(v)))
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl<'a> IntoIterator for &'a HashFields {
    type Item = (&'a str, Value<'a>);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl IntoIterator for HashFields {
    type Item = (String, Arc<[u8]>);
    type IntoIter = indexmap::map::IntoIter<String, Arc<[u8]>>;

    fn into_iter(self) -> Self::IntoIter {
        self.table.into_iter()
    }
}

impl FromIterator<(String, Arc<[u8]>)> for HashFields {
    fn from_iter<I: IntoIterator<Item = (String, Arc<[u8]>)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut out = Self::with_capacity(iter.size_hint().0);
        for (field, value) in iter {
            out.insert_owned(field, value);
        }
        out
    }
}

impl Extend<(String, Arc<[u8]>)> for HashFields {
    fn extend<I: IntoIterator<Item = (String, Arc<[u8]>)>>(&mut self, iter: I) {
        for (field, value) in iter {
            self.insert_owned(field, value);
        }
    }
}

impl std::ops::Index<&str> for HashFields {
    type Output = [u8];

    /// The value of `field`, which the hash must have.
    fn index(&self, field: &str) -> &[u8] {
        self.get_bytes(field).expect("no such field")
    }
}

impl PartialEq for HashFields {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(f, v)| other.get_bytes(f) == Some(v.bytes()))
    }
}

impl std::fmt::Debug for HashFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(
                self.iter()
                    .map(|(field, value)| (field, String::from_utf8_lossy(&value).into_owned())),
            )
            .finish()
    }
}
