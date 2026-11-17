//! The fields of a hash and their values, kept in one of two forms, as
//! Redis keeps a hash as a listpack or a hash table.
//!
//! - **Packed.** A hash of no more than `hash-max-listpack-entries` fields,
//!   no field and no value longer than `hash-max-listpack-value` bytes, is
//!   one block holding the fields and values end to end ([`PackedHash`]): a
//!   lookup scans a byte a field.
//! - **Table.** Any other hash is an `IndexMap` of fields to shared values
//!   (`Arc`).
//!
//! A new hash is packed; it moves to the table when a write does not fit
//! the block, and never back, as in Redis. Either form changes only through
//! `&mut self`, so under the hash's own lock, as before.
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
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::packed_hash::{self, PackedHash};

type Table = indexmap::IndexMap<String, Arc<[u8]>, ahash::RandomState>;

/// Up to this many fields, removing one keeps the others in order, as in a
/// Redis listpack (`hash-max-listpack-entries`). A larger hash is a hash
/// table in Redis too, with no order to keep: there the last field takes the
/// removed one's place, which costs nothing. A packed hash always keeps
/// its order.
pub const HASH_ORDERED_FIELDS: usize = 512;

/// `hash-max-listpack-entries` until `CONFIG SET` or the configuration file
/// gives another: Redis's, and the most a block holds
/// ([`packed_hash::MAX_ENTRIES`]).
pub const DEFAULT_PACKED_ENTRIES: usize = packed_hash::MAX_ENTRIES;
/// `hash-max-listpack-value` until set otherwise: Redis's, and the longest
/// field or value a block holds ([`packed_hash::MAX_LEN`]).
pub const DEFAULT_PACKED_VALUE: usize = packed_hash::MAX_LEN;

/// The most fields a hash keeps in one block, as `hash-max-listpack-entries`
/// says. 0: no blocks.
static PACKED_ENTRIES: AtomicUsize = AtomicUsize::new(DEFAULT_PACKED_ENTRIES);
/// The longest field or value a hash keeps in one block, as
/// `hash-max-listpack-value` says.
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

/// `hash-max-listpack-entries`: no more than a block holds. Returns what was
/// kept, for `CONFIG GET` to report. Takes effect at a hash's next write
/// that crosses it.
pub fn set_packed_entries(limit: usize) -> usize {
    let limit = limit.min(packed_hash::MAX_ENTRIES);
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

/// `hash-max-listpack-value`: no longer than a block holds. Returns what was
/// kept, for `CONFIG GET` to report.
pub fn set_packed_value(limit: usize) -> usize {
    let limit = limit.min(packed_hash::MAX_LEN);
    PACKED_VALUE.store(limit, Ordering::Relaxed);
    limit
}

/// A value, as a hash hands it out: the table's shared bytes, or a block's
/// bytes. Either is its bytes (`Deref`).
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

/// What a hash is kept as, by Redis's names for its encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    Listpack,
    Hashtable,
}

/// A hash's fields and values.
#[derive(Clone)]
pub struct HashFields {
    repr: Repr,
}

// Kept inline in every hash's shared block: no bigger than the table alone
// was, so that hashes kept as the table do not grow.
const _: () = assert!(std::mem::size_of::<HashFields>() <= std::mem::size_of::<Table>());

#[derive(Clone)]
enum Repr {
    Packed(PackedHash),
    Table(Table),
}

impl Default for HashFields {
    fn default() -> Self {
        Self::new()
    }
}

impl HashFields {
    /// An empty hash. It is an empty block, which allocates nothing.
    pub fn new() -> Self {
        Self {
            repr: Repr::Packed(PackedHash::new()),
        }
    }

    /// An empty table with room for `fields`, whatever the limits: for a
    /// hash that would only be moved to one (a JSON document's projection
    /// for its index holds vectors, longer than a block takes).
    pub fn table_with_capacity(fields: usize) -> Self {
        Self {
            repr: Repr::Table(Table::with_capacity_and_hasher(fields, Default::default())),
        }
    }

    /// An empty hash that is to hold about `fields`: the table at once if
    /// that is more than a block would hold.
    pub fn with_capacity(fields: usize) -> Self {
        if fields > packed_entries() {
            return Self {
                repr: Repr::Table(Table::with_capacity_and_hasher(fields, Default::default())),
            };
        }
        Self::new()
    }

    /// What the hash is kept as (`OBJECT ENCODING`).
    #[inline]
    pub fn encoding(&self) -> Encoding {
        match &self.repr {
            Repr::Packed(_) => Encoding::Listpack,
            Repr::Table(_) => Encoding::Hashtable,
        }
    }

    /// Whether the fields are a table. A hash in one block is read whole by
    /// `HSCAN`, as Redis reads a listpack.
    #[inline]
    pub fn is_table(&self) -> bool {
        matches!(self.repr, Repr::Table(_))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => t.len(),
            Repr::Packed(p) => p.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How much work freeing the hash is, as Redis's `lazyfreeGetFreeEffort`
    /// counts it: a block is one allocation, a table one per field.
    pub fn free_effort(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => t.len(),
            Repr::Packed(_) => 1,
        }
    }

    /// Bytes that freeing the hash gives back, at least (for eviction): a
    /// block's, exactly; a table's, `per_field` a field.
    pub fn freeing_at_least(&self, per_field: usize) -> usize {
        match &self.repr {
            Repr::Table(t) => t.len() * per_field,
            Repr::Packed(p) => p.allocation_size(),
        }
    }

    /// The value of `field`, if the hash has it.
    #[inline(always)]
    pub fn get(&self, field: &str) -> Option<Value<'_>> {
        match &self.repr {
            Repr::Table(t) => t.get(field).map(Value::Shared),
            Repr::Packed(p) => p.get(field).map(Value::Bytes),
        }
    }

    /// The bytes of `field`'s value, if the hash has it.
    #[inline(always)]
    pub fn get_bytes(&self, field: &str) -> Option<&[u8]> {
        match &self.repr {
            Repr::Table(t) => t.get(field).map(|v| &**v),
            Repr::Packed(p) => p.get(field),
        }
    }

    #[inline(always)]
    pub fn contains_key(&self, field: &str) -> bool {
        match &self.repr {
            Repr::Table(t) => t.contains_key(field),
            Repr::Packed(p) => p.contains(field),
        }
    }

    /// Field `i` and its value, in the order of [`Self::iter`]: a random
    /// field is a random index, in either form.
    #[inline]
    pub fn get_index(&self, i: usize) -> Option<(&str, Value<'_>)> {
        match &self.repr {
            Repr::Table(t) => t.get_index(i).map(|(f, v)| (f.as_str(), Value::Shared(v))),
            Repr::Packed(p) => (i < p.len()).then(|| {
                let (field, value) = p.entry(i);
                (field, Value::Bytes(value))
            }),
        }
    }

    /// The write of `field` with `value` into the block, if there is one
    /// and they fit: `Some(whether the field is new)`. `None` if the hash
    /// is a table, now or because they did not fit: one pass moves the
    /// fields to it, in order, and the caller writes to it.
    #[inline(always)]
    fn set_packed(&mut self, field: &str, value: &[u8]) -> Option<bool> {
        match self.repr {
            Repr::Table(_) => None,
            Repr::Packed(_) => self.set_in_block(field, value),
        }
    }

    /// [`Self::set_packed`] of a hash in a block: a call of its own, so
    /// that a table's writes stay small enough to inline.
    #[inline(never)]
    fn set_in_block(&mut self, field: &str, value: &[u8]) -> Option<bool> {
        let Repr::Packed(p) = &mut self.repr else {
            return None;
        };
        let longest = packed_value();
        if field.len() <= longest && value.len() <= longest {
            // A write that leaves the hash over the limit moves it, as in
            // Redis: a value set again too when the limit was lowered.
            let entries = packed_entries();
            match p.locate(field) {
                Ok(i) => {
                    if p.len() <= entries && p.set_value_at(i, value) {
                        return Some(false);
                    }
                }
                Err(f) => {
                    if p.len() < entries && p.push_fingerprinted(field, value, f) {
                        return Some(true);
                    }
                }
            }
        }
        self.make_table(1);
        None
    }

    /// The block's fields moved to a table with room for `more` besides.
    #[cold]
    #[inline(never)]
    fn make_table(&mut self, more: usize) {
        let Repr::Packed(p) = &self.repr else {
            return;
        };
        let mut table = Table::with_capacity_and_hasher(p.len() + more, Default::default());
        for (field, value) in p.iter() {
            table.insert(field.to_owned(), Arc::from(value));
        }
        self.repr = Repr::Table(table);
    }

    #[inline]
    fn table_mut(&mut self) -> &mut Table {
        match &mut self.repr {
            Repr::Table(t) => t,
            Repr::Packed(_) => unreachable!("set_packed leaves a table"),
        }
    }

    /// Give `field` the value `value`. Whether the field is new.
    #[inline]
    pub fn insert(&mut self, field: &str, value: &Arc<[u8]>) -> bool {
        if let Some(added) = self.set_packed(field, value) {
            return added;
        }
        self.table_mut()
            .insert(field.to_owned(), Arc::clone(value))
            .is_none()
    }

    /// [`Self::insert`] of bytes.
    #[inline]
    pub fn insert_bytes(&mut self, field: &str, value: &[u8]) -> bool {
        if let Some(added) = self.set_packed(field, value) {
            return added;
        }
        self.table_mut()
            .insert(field.to_owned(), Arc::from(value))
            .is_none()
    }

    /// [`Self::insert`] of a field and a value that are the caller's to
    /// give away (a load): a table takes them as they are.
    #[inline]
    pub fn insert_owned(&mut self, field: String, value: Arc<[u8]>) -> bool {
        if let Some(added) = self.set_packed(&field, &value) {
            return added;
        }
        self.table_mut().insert(field, value).is_none()
    }

    /// [`Self::insert`] of a field read from a file or a payload (a load):
    /// a table takes the field as it is, a block copies both.
    #[inline]
    pub fn insert_loaded(&mut self, field: String, value: &[u8]) -> bool {
        if let Some(added) = self.set_packed(&field, value) {
            return added;
        }
        self.table_mut().insert(field, Arc::from(value)).is_none()
    }

    /// Remove `field`. Whether the hash had it. A block, and a table of up
    /// to [`HASH_ORDERED_FIELDS`] fields, keep the others in order.
    #[inline]
    pub fn remove(&mut self, field: &str) -> bool {
        match &mut self.repr {
            Repr::Packed(p) => p.remove(field),
            Repr::Table(_) => self.take(field).is_some(),
        }
    }

    /// [`Self::remove`], handing the value to `f` on its way out.
    pub fn take_with<R>(&mut self, field: &str, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        match &mut self.repr {
            Repr::Packed(p) => {
                let i = p.find(field)?;
                let out = f(p.value(i));
                p.remove_at(i);
                Some(out)
            }
            Repr::Table(_) => self.take(field).map(|v| f(&v)),
        }
    }

    /// [`Self::remove`], handing back the value.
    pub fn take(&mut self, field: &str) -> Option<Arc<[u8]>> {
        match &mut self.repr {
            Repr::Packed(_) => self.take_with(field, |v: &[u8]| Arc::<[u8]>::from(v)),
            Repr::Table(t) => {
                if t.len() <= HASH_ORDERED_FIELDS {
                    t.shift_remove(field)
                } else {
                    t.swap_remove(field)
                }
            }
        }
    }

    /// Keep the fields `keep` says to, in their order.
    pub fn retain(&mut self, mut keep: impl FnMut(&str, &[u8]) -> bool) {
        match &mut self.repr {
            Repr::Table(t) => t.retain(|f, v| keep(f, v)),
            Repr::Packed(p) => p.retain(keep),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.repr {
            Repr::Table(t) => t.clear(),
            Repr::Packed(p) => p.clear(),
        }
    }

    /// The fields and their values, in order.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        match &self.repr {
            Repr::Table(t) => Iter::Table(t.iter()),
            Repr::Packed(p) => Iter::Packed(p.iter()),
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
    /// A block is its one allocation, exactly. The table (`IndexMap`): its
    /// table of indexes, its vector of entries (hash, name, value), both
    /// with room for as many as it reports, and each name's and each
    /// value's block, sampled. What it reports is the lesser room of the
    /// two. That is the vector's for a hash made with room for its fields
    /// (as a snapshot loads one) and both's for one that grew; but deleting
    /// fields can leave the table fewer free buckets (hashbrown keeps a
    /// deleted bucket marked until it is rebuilt), and then part of the
    /// vector's room is not seen and not counted.
    pub fn layout_bytes(&self, samples: usize) -> usize {
        use crate::commands::memory_layout::{arc_slice, hash_table, sampled};
        let table = match &self.repr {
            Repr::Packed(p) => return p.allocation_size(),
            Repr::Table(t) => t,
        };
        let capacity = table.capacity();
        let indexes = hash_table(
            capacity,
            std::mem::size_of::<usize>(),
            std::mem::align_of::<usize>(),
        );
        let entries = capacity.saturating_mul(std::mem::size_of::<(usize, String, Arc<[u8]>)>());
        let len = table.len();
        // An `IndexMap` gets to its n-th entry at once, so the sample is
        // spread over the hash rather than taken from its first fields only.
        let bytes = |(name, value): (&String, &Arc<[u8]>)| name.capacity() + arc_slice(value.len());
        let each = if samples >= len {
            sampled(len, samples, || table.iter().map(bytes))
        } else {
            let step = (len / samples.max(1)).max(1);
            sampled(len, samples, || table.iter().step_by(step).map(bytes))
        };
        indexes + entries + each
    }
}

pub enum Iter<'a> {
    Packed(packed_hash::Iter<'a>),
    Table(indexmap::map::Iter<'a, String, Arc<[u8]>>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a str, Value<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Packed(it) => it.next().map(|(f, v)| (f, Value::Bytes(v))),
            Iter::Table(it) => it.next().map(|(f, v)| (f.as_str(), Value::Shared(v))),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::Packed(it) => it.size_hint(),
            Iter::Table(it) => it.size_hint(),
        }
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Iter::Packed(it) => it.nth(n).map(|(f, v)| (f, Value::Bytes(v))),
            Iter::Table(it) => it.nth(n).map(|(f, v)| (f.as_str(), Value::Shared(v))),
        }
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

/// The fields and values of a hash given away: a table's as they are, a
/// block's copied out.
pub enum IntoIter {
    Packed(PackedHash, usize),
    Table(indexmap::map::IntoIter<String, Arc<[u8]>>),
}

impl Iterator for IntoIter {
    type Item = (String, Arc<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IntoIter::Packed(p, next) => {
                let i = *next;
                (i < p.len()).then(|| {
                    *next += 1;
                    {
                        let (field, value) = p.entry(i);
                        (field.to_owned(), Arc::from(value))
                    }
                })
            }
            IntoIter::Table(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            IntoIter::Packed(p, next) => (p.len() - next, Some(p.len() - next)),
            IntoIter::Table(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for IntoIter {}

impl IntoIterator for HashFields {
    type Item = (String, Arc<[u8]>);
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        match self.repr {
            Repr::Packed(p) => IntoIter::Packed(p, 0),
            Repr::Table(t) => IntoIter::Table(t.into_iter()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// What the hash should hold: the fields in the order first set.
    #[derive(Default)]
    struct Model(Vec<(String, Vec<u8>)>);

    impl Model {
        fn set(&mut self, field: &str, value: &[u8]) -> bool {
            match self.0.iter_mut().find(|(f, _)| f == field) {
                Some((_, v)) => {
                    *v = value.to_vec();
                    false
                }
                None => {
                    self.0.push((field.to_string(), value.to_vec()));
                    true
                }
            }
        }
        fn remove(&mut self, field: &str, ordered: bool) -> bool {
            match self.0.iter().position(|(f, _)| f == field) {
                Some(i) => {
                    if ordered {
                        self.0.remove(i);
                    } else {
                        self.0.swap_remove(i);
                    }
                    true
                }
                None => false,
            }
        }
    }

    /// Contents and `len` always; the order while the hash is packed, and
    /// while a table keeps it (up to `HASH_ORDERED_FIELDS`, which the model
    /// follows by removing as the table does).
    fn assert_same(hash: &HashFields, model: &Model, step: usize) {
        assert_eq!(hash.len(), model.0.len(), "step {step}");
        let held: Vec<(String, Vec<u8>)> = hash
            .iter()
            .map(|(f, v)| (f.to_string(), v.to_vec()))
            .collect();
        assert_eq!(held, model.0, "step {step}");
        for (i, (f, v)) in model.0.iter().enumerate() {
            assert_eq!(hash.get_bytes(f), Some(v.as_slice()), "step {step} {f}");
            let (field, value) = hash.get_index(i).unwrap();
            assert_eq!((field, value.bytes()), (f.as_str(), v.as_slice()));
        }
        assert!(hash.get_index(model.0.len()).is_none());
    }

    struct Limits;
    impl Drop for Limits {
        fn drop(&mut self) {
            own_limits(None);
        }
    }

    #[test]
    fn a_new_hash_is_an_empty_block() {
        let hash = HashFields::new();
        assert_eq!(hash.encoding(), Encoding::Listpack);
        assert_eq!(hash.layout_bytes(usize::MAX), 0);
        assert!(HashFields::with_capacity(packed_entries()).encoding() == Encoding::Listpack);
        assert!(HashFields::with_capacity(packed_entries() + 1).is_table());
    }

    #[test]
    fn it_moves_to_a_table_by_count_by_a_long_field_and_by_a_long_value() {
        let _limits = Limits;
        own_limits(Some([4, 8]));
        // By count: the fifth field.
        let mut hash = HashFields::new();
        for i in 0..4 {
            assert!(hash.insert_bytes(&format!("f{i}"), b"v"));
        }
        assert_eq!(hash.encoding(), Encoding::Listpack);
        assert!(
            !hash.insert_bytes("f0", b"w"),
            "a value set again is no new field"
        );
        assert_eq!(hash.encoding(), Encoding::Listpack);
        assert!(hash.insert_bytes("f4", b"v"));
        assert!(hash.is_table());
        let fields: Vec<&str> = hash.keys().collect();
        assert_eq!(fields, ["f0", "f1", "f2", "f3", "f4"]);
        assert_eq!(hash.get_bytes("f0"), Some(&b"w"[..]));
        // By a long field.
        let mut hash = HashFields::new();
        hash.insert_bytes("a", b"1");
        assert!(hash.insert_bytes("123456789", b"2"));
        assert!(hash.is_table());
        assert_eq!(hash.len(), 2);
        // By a long value, new or set again; and it never moves back.
        let mut hash = HashFields::new();
        hash.insert_bytes("a", b"1");
        assert!(!hash.insert_bytes("a", b"123456789"));
        assert!(hash.is_table());
        hash.insert_bytes("a", b"1");
        assert!(hash.remove("a"));
        assert!(hash.is_table());
    }

    #[test]
    fn it_moves_to_a_table_at_the_blocks_own_byte_limit() {
        // 511 fields of 64 bytes with 64-byte values are 65,408 bytes: the
        // 512th does not fit a block, whatever the settings say.
        let mut hash = HashFields::new();
        let value = [b'v'; 64];
        for i in 0..511 {
            hash.insert_bytes(&format!("{i:064}"), &value);
        }
        assert_eq!(hash.encoding(), Encoding::Listpack);
        assert!(hash.insert_bytes(&format!("{:064}", 511), &value));
        assert!(hash.is_table());
        assert_eq!(hash.len(), 512);
        assert_eq!(hash.keys().next(), Some(&*format!("{:064}", 0)));
    }

    #[test]
    fn take_and_take_with_hand_back_the_value() {
        for table in [false, true] {
            let mut hash = HashFields::with_capacity(if table { 10_000 } else { 0 });
            hash.insert_bytes("a", b"1");
            hash.insert_bytes("b", b"2");
            assert_eq!(hash.is_table(), table);
            assert_eq!(hash.take("a").as_deref(), Some(&b"1"[..]));
            assert_eq!(hash.take("a"), None);
            assert_eq!(hash.take_with("b", |v| v.to_vec()), Some(b"2".to_vec()));
            assert!(hash.is_empty());
        }
    }

    #[test]
    fn it_matches_a_model_over_a_long_random_run() {
        let _limits = Limits;
        // A fixed sequence (an xorshift), so a failure repeats.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for run in 0..24 {
            // Settings that move during the run, as `CONFIG SET` may.
            let mut limits = [512usize, 64];
            own_limits(Some(limits));
            let mut hash = HashFields::new();
            let mut model = Model::default();
            let names = [16usize, 600, 700][run % 3];
            let long_every = [0u64, 50, 400][(run / 3) % 3];
            for step in 0..6_000 {
                let r = next();
                let name = if long_every != 0 && r % long_every == 1 {
                    format!("a-long-field-name-past-sixty-four-bytes-{:032}", r % 7)
                } else {
                    format!("field:{:08}", (r >> 8) % names as u64)
                };
                match (r >> 20) % 16 {
                    0..=8 => {
                        let len = if long_every != 0 && (r >> 30) % long_every == 2 {
                            65 + (r >> 40) as usize % 30
                        } else {
                            ((r >> 40) % 64) as usize
                        };
                        let value = vec![b'a' + (r >> 50) as u8 % 26; len];
                        let added = model.set(&name, &value);
                        let did = match (r >> 56) % 3 {
                            0 => hash.insert_bytes(&name, &value),
                            1 => hash.insert(&name, &Arc::from(value.as_slice())),
                            _ => hash.insert_owned(name.clone(), Arc::from(value.as_slice())),
                        };
                        assert_eq!(did, added, "step {step}");
                    }
                    9..=12 => {
                        // A table past the ordered limit swaps the last in.
                        let ordered = !hash.is_table() || hash.len() <= HASH_ORDERED_FIELDS;
                        let had = model.remove(&name, ordered);
                        let did = if r % 2 == 0 {
                            hash.remove(&name)
                        } else {
                            hash.take(&name).is_some()
                        };
                        assert_eq!(did, had, "step {step}");
                    }
                    13 => {
                        assert_eq!(
                            hash.get_bytes(&name),
                            model
                                .0
                                .iter()
                                .find(|(f, _)| *f == name)
                                .map(|(_, v)| v.as_slice())
                        );
                        assert_eq!(
                            hash.contains_key(&name),
                            model.0.iter().any(|(f, _)| *f == name)
                        );
                        if !model.0.is_empty() {
                            let i = (r >> 30) as usize % model.0.len();
                            let (f, v) = hash.get_index(i).unwrap();
                            assert_eq!(
                                (f, v.bytes()),
                                (model.0[i].0.as_str(), model.0[i].1.as_slice())
                            );
                        }
                    }
                    14 => {
                        let cut = (r >> 40) % 5;
                        let keep = |f: &str| f.as_bytes()[f.len() - 1] as u64 % 5 != cut;
                        hash.retain(|f, _| keep(f));
                        model.0.retain(|(f, _)| keep(f));
                    }
                    _ => {
                        // The settings change: hashes follow from their next
                        // write that crosses them.
                        limits = [
                            [0, 3, 8, 100, 512, 600][(r >> 30) as usize % 6],
                            [0, 5, 20, 64, 100][(r >> 40) as usize % 5],
                        ];
                        own_limits(Some(limits));
                    }
                }
                let packed = !hash.is_table();
                if packed {
                    assert!(hash.len() <= packed_hash::MAX_ENTRIES);
                }
                assert_same(&hash, &model, step);
            }
        }
    }

    /// A field loaded twice (a file or a payload that names it twice) keeps
    /// its first place and its last value, in a block and in a table, as
    /// the `IndexMap` always did.
    #[test]
    fn a_field_loaded_twice_keeps_its_first_place_and_last_value() {
        for (n, table) in [(3usize, false), (600, true)] {
            let mut pairs: Vec<(String, Arc<[u8]>)> = (0..n)
                .map(|i| (format!("f{i}"), Arc::from(format!("v{i}").as_bytes())))
                .collect();
            pairs.push(("f1".to_string(), Arc::from(&b"again"[..])));
            let collected: HashFields = pairs.iter().cloned().collect();
            let mut loaded = HashFields::with_capacity(pairs.len());
            for (field, value) in &pairs {
                loaded.insert_loaded(field.clone(), value);
            }
            for hash in [&collected, &loaded] {
                assert_eq!(hash.is_table(), table, "{n}");
                assert_eq!(hash.len(), n, "{n}");
                assert_eq!(
                    hash.get_index(1).map(|(f, v)| (f, v.to_vec())),
                    Some(("f1", b"again".to_vec()))
                );
                assert_eq!(hash.get_bytes("f1"), Some(&b"again"[..]));
                let fields: Vec<&str> = hash.keys().take(3).collect();
                assert_eq!(fields, ["f0", "f1", "f2"][..n.min(3)]);
            }
        }
        assert!(HashFields::table_with_capacity(2).is_table());
    }

    #[test]
    fn a_hash_given_away_hands_over_its_fields_in_order() {
        for n in [3usize, 600] {
            let mut hash = HashFields::new();
            for i in 0..n {
                hash.insert_bytes(&format!("f{i}"), format!("v{i}").as_bytes());
            }
            let pairs: Vec<(String, Arc<[u8]>)> = hash.clone().into_iter().collect();
            assert_eq!(pairs.len(), n);
            assert_eq!(pairs[2], ("f2".to_string(), Arc::from(&b"v2"[..])));
            let back: HashFields = pairs.into_iter().collect();
            assert_eq!(back, hash);
            assert_eq!(back.is_table(), hash.is_table());
        }
    }
}
