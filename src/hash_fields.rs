//! The fields of a hash and their values, kept in one of two forms, as
//! Redis keeps a hash as a listpack or a hash table.
//!
//! - **Packed.** A hash of no more than `hash-max-listpack-entries` fields,
//!   no field and no value longer than `hash-max-listpack-value` bytes, is
//!   one block holding the fields and values end to end ([`PackedHash`]): a
//!   lookup scans a byte a field.
//! - **Table.** Any other hash is a [`CollectionTable`] (#764): each field
//!   and its value side by side in one block of the entry pool (a value
//!   past the pool's largest class in a block of the system allocator). It
//!   gives `HSCAN` its cursor (`table_scan`), a uniformly random field in
//!   O(1), and grows and shrinks a few fields at a time, so no write pays
//!   for moving them all. A resize that the writes leave unfinished is
//!   finished by the background ([`after_write`]), and the array a
//!   finished one leaves is freed there too.
//!
//! A new hash is packed; it moves to the table when a write does not fit
//! the block, and never back, as in Redis. Either form changes only through
//! `&mut self`, so under the hash's own lock, as before.
//!
//! A block keeps its fields in the order they were first set in, which is
//! the order `HGETALL`, `HKEYS`, `HVALS` and `HSCAN` give them in: Redis
//! keeps that order for a hash small enough for a listpack, and clients
//! have come to rely on it (BullMQ's tests among them). A table has no
//! order, as a Redis hashtable has none.
//!
//! Field names are bytes, any bytes, as Redis's are: a field is kept as the
//! bytes it was sent as, and every lookup compares bytes. Callers see
//! fields as `&[u8]` and values as [`Value`]s: bytes, whatever the hash
//! keeps them as. Snapshots, the AOF, replication and `DUMP` see nothing
//! else, so none of them depends on the form.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::collection_table::{CollectionTable, FieldIter, FieldRef, Fields};
use crate::packed_hash::{self, PackedHash};

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

/// The bytes `field` is written as where KiviDB writes a hash for itself:
/// its own snapshots (KDB), `DUMP` payloads (and so `MIGRATE` and slot
/// migration), and the digest (`DEBUG DIGEST`, replication's resync check).
///
/// These keep the spelling 1.1 and 1.2 kept a field in, when a hash held
/// its fields as text: a name that is UTF-8, and has none of the characters
/// U+F780 to U+F7FF, is itself; any other is [`crate::key_bytes::text_of`]
/// its bytes. So a file or a payload written here reads the same on a 1.2
/// server, and one written there reads the same here ([`loaded_name`]),
/// both ways, byte for byte.
///
/// A name of ASCII bytes only, as nearly every name is, is itself without
/// more ado: it is UTF-8, and every byte of the block's characters is 0x80
/// or above. A save asks this of every field of every hash it writes.
#[inline]
pub fn saved_name(field: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    if field.is_ascii() {
        return std::borrow::Cow::Borrowed(field);
    }
    saved_text_name(field)
}

/// [`saved_name`] of a name with a byte of 0x80 or above.
#[inline(never)]
fn saved_text_name(field: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    match crate::key_bytes::text_of(field) {
        std::borrow::Cow::Borrowed(text) => std::borrow::Cow::Borrowed(text.as_bytes()),
        std::borrow::Cow::Owned(text) => std::borrow::Cow::Owned(text.into_bytes()),
    }
}

/// The field a name read from a KDB file or a `DUMP` payload stands for:
/// the inverse of [`saved_name`]. `None` for bytes that are not UTF-8, which
/// no writer of that form writes.
#[inline]
pub fn loaded_name(saved: &[u8]) -> Option<std::borrow::Cow<'_, [u8]>> {
    std::str::from_utf8(saved)
        .ok()
        .map(crate::key_bytes::bytes_of)
}

/// Whether `saved` is the spelling [`saved_name`] gives `field`, which
/// [`loaded_name`] read from it. Only a name with a character of the
/// escape's block in it can be another: 1.2.0 kept such a name, given to
/// `HIMPORT`, as it came. Two spellings of one field are a payload no writer
/// makes (see [`Collisions`]).
#[inline]
fn is_canonical(saved: &[u8], field: &[u8]) -> bool {
    saved == field || *saved_name(field) == *saved
}

/// What a load of a hash's names needs to tell a field named twice in two
/// spellings (refused: one field where the count says two, a file or a
/// payload made by hand) from one named twice alike, which is set twice, as
/// a load always has done.
#[derive(Default)]
pub struct Collisions {
    other_spellings: bool,
}

impl Collisions {
    /// `saved`, read as `field`, was added to the hash (`added`) or set a
    /// field it had. `false`: two spellings of one field.
    #[inline]
    pub fn allows(&mut self, saved: &[u8], field: &[u8], added: bool) -> bool {
        if !is_canonical(saved, field) {
            self.other_spellings = true;
        }
        added || !self.other_spellings
    }
}

/// A value, as a hash hands it out: bytes of a block or of a table's entry.
/// It is its bytes (`Deref`). The same as a list's elements.
pub use crate::element::Value;

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

// Kept inline in every hash's shared block: a block's header, or the box of
// a table (whose own header is 80 bytes).
const _: () = assert!(std::mem::size_of::<HashFields>() <= 32);

#[derive(Clone)]
enum Repr {
    Packed(PackedHash),
    Table(Box<Table>),
}

/// The table (#764), boxed, and whether it has been handed to the
/// background ([`after_write`]).
struct Table {
    fields: CollectionTable<Fields>,
    queued: bool,
}

impl Table {
    fn with_capacity(fields: usize) -> Box<Self> {
        let mut table = CollectionTable::with_capacity(fields);
        // A finished resize's old array is freed by the background, not by
        // the write that finished it.
        table.defer_free(true);
        Box::new(Table {
            fields: table,
            queued: false,
        })
    }

    /// Set `field` to `value`; true if the field is new. A field or value
    /// is at most `collection_table::MAX_LEN` (4 GiB) long here: a
    /// command's argument is at most 512 MB, and a load checks first
    /// ([`HashFields::insert_loaded`]).
    #[inline]
    fn set(&mut self, field: &[u8], value: &[u8]) -> bool {
        debug_assert!(field.len() <= crate::collection_table::MAX_LEN);
        debug_assert!(value.len() <= crate::collection_table::MAX_LEN);
        self.fields.insert(field, value, None).unwrap_or(false)
    }
}

impl Clone for Table {
    /// A table of its own (`COPY`): every field copied, nothing shared.
    fn clone(&self) -> Self {
        let mut fields = self.fields.clone();
        fields.defer_free(true);
        Table {
            fields,
            queued: false,
        }
    }
}

#[inline(always)]
fn pair(entry: FieldRef<'_>) -> (&[u8], Value<'_>) {
    (entry.field, Value::Bytes(entry.value))
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
            repr: Repr::Table(Table::with_capacity(fields)),
        }
    }

    /// An empty hash that is to hold about `fields`: the table at once if
    /// that is more than a block would hold.
    pub fn with_capacity(fields: usize) -> Self {
        if fields > packed_entries() {
            return Self::table_with_capacity(fields);
        }
        Self::new()
    }

    /// The hash in the form its fields would have been loaded in, if a
    /// table made for more holds far fewer (a load that dropped fields
    /// already past their time): a block if they fit one, else a table
    /// with room for them, not one at least twice their size.
    pub fn into_fitting(self) -> Self {
        let Repr::Table(t) = &self.repr else {
            return self;
        };
        let n = t.fields.len();
        let longest = packed_value();
        let fits_block = n <= packed_entries()
            && t.fields
                .iter()
                .all(|e| e.field.len() <= longest && e.value.len() <= longest);
        if !fits_block && t.fields.capacity() < 2 * n.max(1) {
            return self;
        }
        let mut out = Self::with_capacity(n);
        for (field, value) in self.iter() {
            out.insert_bytes(field, &value);
        }
        out
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

    /// The table, for `HSCAN`'s cursor (`table_scan`); `None` for a block,
    /// which `HSCAN` returns whole.
    #[inline]
    pub fn table(&self) -> Option<&CollectionTable<Fields>> {
        match &self.repr {
            Repr::Table(t) => Some(&t.fields),
            Repr::Packed(_) => None,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => t.fields.len(),
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
            Repr::Table(t) => t.fields.len(),
            Repr::Packed(_) => 1,
        }
    }

    /// Bytes that freeing the hash gives back (for eviction): exact in
    /// either form ([`Self::allocation_size`]).
    pub fn freeing_at_least(&self, _per_field: usize) -> usize {
        self.allocation_size()
    }

    /// Everything the hash holds but its own header, exactly as
    /// `used_memory` counts it, in O(1): a block's one allocation, room to
    /// spare included; for the table its box, group arrays, overflow groups
    /// and entries at their pool class's size
    /// ([`CollectionTable::allocation_size`]), mid-resize too.
    pub fn allocation_size(&self) -> usize {
        match &self.repr {
            Repr::Table(t) => std::mem::size_of::<Table>() + t.fields.allocation_size(),
            Repr::Packed(p) => p.allocation_size(),
        }
    }

    /// The value of `field`, if the hash has it.
    #[inline(always)]
    pub fn get(&self, field: &(impl AsRef<[u8]> + ?Sized)) -> Option<Value<'_>> {
        self.get_bytes(field).map(Value::Bytes)
    }

    /// The bytes of `field`'s value, if the hash has it.
    #[inline(always)]
    pub fn get_bytes(&self, field: &(impl AsRef<[u8]> + ?Sized)) -> Option<&[u8]> {
        let field = field.as_ref();
        match &self.repr {
            Repr::Table(t) => t.fields.get(field).map(|entry| entry.value),
            Repr::Packed(p) => p.get(field),
        }
    }

    #[inline(always)]
    pub fn contains_key(&self, field: &(impl AsRef<[u8]> + ?Sized)) -> bool {
        let field = field.as_ref();
        match &self.repr {
            Repr::Table(t) => t.fields.contains_key(field),
            Repr::Packed(p) => p.contains(field),
        }
    }

    /// A field and its value, any one as likely as another; `None` if the
    /// hash is empty.
    pub fn random(&self, rng: &mut impl rand::Rng) -> Option<(&[u8], Value<'_>)> {
        match &self.repr {
            Repr::Table(t) => t.fields.random(rng).map(pair),
            Repr::Packed(p) if p.is_empty() => None,
            Repr::Packed(p) => {
                let (field, value) = p.entry(rng.gen_range(0..p.len()));
                Some((field, Value::Bytes(value)))
            }
        }
    }

    /// Up to `count` different fields with their values, chosen at random
    /// (`HRANDFIELD` with a positive count), in random order.
    ///
    /// A few of many (no more than a third) are picked one at a time,
    /// picking again when one comes up twice: O(count). More of them are
    /// chosen in one walk of the table (`choose_multiple`), then shuffled:
    /// O(len), which is then no more than three times the answer. A block is
    /// asked for the chosen places only.
    pub fn random_distinct(
        &self,
        count: usize,
        rng: &mut impl rand::Rng,
    ) -> Vec<(&[u8], Value<'_>)> {
        use rand::seq::{IteratorRandom, SliceRandom};
        let len = self.len();
        let count = count.min(len);
        let t = match &self.repr {
            Repr::Packed(p) => {
                return rand::seq::index::sample(rng, len, count)
                    .into_iter()
                    .map(|i| {
                        let (field, value) = p.entry(i);
                        (field, Value::Bytes(value))
                    })
                    .collect();
            }
            Repr::Table(t) => t,
        };
        if count.saturating_mul(3) > len {
            let mut picked = self.iter().choose_multiple(rng, count);
            picked.shuffle(rng);
            return picked;
        }
        // Each field is told apart by where its entry is: it has one.
        let mut seen: ahash::AHashSet<usize> = ahash::AHashSet::with_capacity(count);
        let mut picked = Vec::with_capacity(count);
        while picked.len() < count {
            let Some(entry) = t.fields.random(rng) else {
                break;
            };
            if seen.insert(entry.field.as_ptr() as usize) {
                picked.push(pair(entry));
            }
        }
        picked
    }

    /// The write of `field` with `value` into the block, if there is one
    /// and they fit: `Some(whether the field is new)`. `None` if the hash
    /// is a table, now or because they did not fit: one pass moves the
    /// fields to it, and the caller writes to it.
    #[inline(always)]
    fn set_packed(&mut self, field: &[u8], value: &[u8]) -> Option<bool> {
        match self.repr {
            Repr::Table(_) => None,
            Repr::Packed(_) => self.set_in_block(field, value),
        }
    }

    /// [`Self::set_packed`] of a hash in a block: a call of its own, so
    /// that a table's writes stay small enough to inline.
    #[inline(never)]
    fn set_in_block(&mut self, field: &[u8], value: &[u8]) -> Option<bool> {
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
        let mut table = Table::with_capacity(p.len() + more);
        for (field, value) in p.iter() {
            table.set(field, value);
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
    pub fn insert(&mut self, field: &(impl AsRef<[u8]> + ?Sized), value: &Arc<[u8]>) -> bool {
        self.insert_bytes(field, value)
    }

    /// [`Self::insert`] of bytes.
    #[inline]
    pub fn insert_bytes(&mut self, field: &(impl AsRef<[u8]> + ?Sized), value: &[u8]) -> bool {
        let field = field.as_ref();
        if let Some(added) = self.set_packed(field, value) {
            return added;
        }
        self.table_mut().set(field, value)
    }

    /// [`Self::insert`] of a field and a value that are the caller's to
    /// give away: copied in either form.
    #[inline]
    pub fn insert_owned(&mut self, field: impl AsRef<[u8]>, value: Arc<[u8]>) -> bool {
        self.insert_bytes(field.as_ref(), &value)
    }

    /// [`Self::insert`] of a field read from a file or a payload (a load):
    /// an error, and nothing changed, for a field or a value longer than a
    /// table holds (`collection_table::MAX_LEN`, 4 GiB), never dropped.
    #[inline]
    pub fn insert_loaded(
        &mut self,
        field: impl AsRef<[u8]>,
        value: &[u8],
    ) -> std::io::Result<bool> {
        let field = field.as_ref();
        if let Some(added) = self.set_packed(field, value) {
            return Ok(added);
        }
        self.table_mut()
            .fields
            .insert(field, value, None)
            .map_err(|too_long| std::io::Error::new(std::io::ErrorKind::InvalidData, too_long))
    }

    /// Remove `field`. Whether the hash had it. A block keeps the others in
    /// order.
    #[inline]
    pub fn remove(&mut self, field: &(impl AsRef<[u8]> + ?Sized)) -> bool {
        let field = field.as_ref();
        match &mut self.repr {
            Repr::Packed(p) => p.remove(field),
            Repr::Table(t) => t.fields.remove(field),
        }
    }

    /// [`Self::remove`], handing the value to `f` on its way out.
    pub fn take_with<R>(
        &mut self,
        field: &(impl AsRef<[u8]> + ?Sized),
        f: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        let field = field.as_ref();
        match &mut self.repr {
            Repr::Packed(p) => {
                let i = p.find(field)?;
                let out = f(p.value(i));
                p.remove_at(i);
                Some(out)
            }
            Repr::Table(t) => t.fields.remove_with(field, |entry| f(entry.value)),
        }
    }

    /// [`Self::remove`], handing back the value.
    pub fn take(&mut self, field: &(impl AsRef<[u8]> + ?Sized)) -> Option<Arc<[u8]>> {
        self.take_with(field, |v: &[u8]| Arc::<[u8]>::from(v))
    }

    /// Keep the fields `keep` says to (a block keeps them in order).
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8], &[u8]) -> bool) {
        match &mut self.repr {
            Repr::Table(t) => t.fields.retain(|entry| keep(entry.field, entry.value)),
            Repr::Packed(p) => p.retain(keep),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.repr {
            Repr::Table(t) => t.fields.clear(),
            Repr::Packed(p) => p.clear(),
        }
    }

    /// The fields and their values: a block's in order, a table's in none.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        match &self.repr {
            Repr::Table(t) => Iter::Table(t.fields.iter()),
            Repr::Packed(p) => Iter::Packed(p.iter()),
        }
    }

    /// Every field and its value, in [`Self::iter`]'s order, given to `f`:
    /// one loop for each form, the form chosen once (`HGETALL`, `HKEYS`,
    /// `HVALS`); a table's walk reads no TTL
    /// (`CollectionTable::for_each_field`).
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(&[u8], Value<'_>)) {
        match &self.repr {
            Repr::Packed(p) => {
                for (field, value) in p.iter() {
                    f(field, Value::Bytes(value));
                }
            }
            Repr::Table(t) => t
                .fields
                .for_each_field(|field, value| f(field, Value::Bytes(value))),
        }
    }

    #[inline]
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &[u8]> + '_ {
        self.iter().map(|(f, _)| f)
    }

    #[inline]
    pub fn values(&self) -> impl ExactSizeIterator<Item = Value<'_>> + '_ {
        self.iter().map(|(_, v)| v)
    }

    /// What the hash's memory is, its own header left out, for
    /// `MEMORY USAGE`: exact in either form and in O(1), whatever `samples`
    /// says ([`Self::allocation_size`]).
    pub fn layout_bytes(&self, _samples: usize) -> usize {
        self.allocation_size()
    }

    /// Whether the background has work on this hash (a resize to finish,
    /// or a finished one's array to free) that it has not been handed yet;
    /// if so, it is now marked handed. Near `maxmemory` the work is done
    /// here instead ([`Self::settle_if_tight`]) and nothing is handed over.
    #[inline]
    fn hand_over(&mut self) -> bool {
        let Repr::Table(t) = &mut self.repr else {
            return false;
        };
        if !(t.fields.resizing() || t.fields.holds_retired()) {
            return false;
        }
        self.hand_over_table()
    }

    /// [`Self::hand_over`] of a table with work for the background: out of
    /// line, so that a write that leaves none pays only the check above.
    #[cold]
    #[inline(never)]
    fn hand_over_table(&mut self) -> bool {
        if self.settle_if_tight() {
            return false;
        }
        let Repr::Table(t) = &mut self.repr else {
            return false;
        };
        let due = !t.queued;
        t.queued = true;
        due
    }

    /// Near `maxmemory`, finish a resize in progress and free the old
    /// arrays now, on this thread, as a table that grows all at once does
    /// (and as hashes did before #764): the arrays a resize holds are there
    /// only until it ends, and must never be what makes the server refuse a
    /// write or evict a key. Returns whether it did
    /// (`resize_background::tight`).
    fn settle_if_tight(&mut self) -> bool {
        let Repr::Table(t) = &mut self.repr else {
            return false;
        };
        if !crate::resize_background::tight(t.fields.transient_bytes()) {
            return false;
        }
        while !t.fields.step_resize(usize::MAX) {}
        drop(t.fields.take_retired());
        true
    }

    /// Finish a resize in progress and free what it leaves, now. Whether
    /// any memory was given back.
    fn settle(&mut self) -> bool {
        let Repr::Table(t) = &mut self.repr else {
            return false;
        };
        let released = t.fields.transient_bytes() > 0;
        while !t.fields.step_resize(usize::MAX) {}
        drop(t.fields.take_retired());
        t.queued = false;
        released
    }

    /// One step of the background's work on the hash: about `budget` units
    /// of a resize in progress. Returns whether the hash has no more for
    /// it, and the array a finished resize left, to be freed elsewhere.
    fn background_step(
        &mut self,
        budget: usize,
    ) -> (bool, Option<crate::collection_table::Retired>) {
        let Repr::Table(t) = &mut self.repr else {
            return (true, None);
        };
        let finished = t.fields.step_resize(budget);
        let retired = t.fields.take_retired();
        if finished {
            t.queued = false;
        }
        (finished, retired)
    }

    /// Forget that the table was handed to the background: for a value
    /// about to get a new `Arc` ([`shared`]), which no registry entry can
    /// point at.
    fn unqueue(&mut self) {
        if let Repr::Table(t) = &mut self.repr {
            t.queued = false;
        }
    }

    /// Whether the hash is handed to the background (tests).
    #[cfg(test)]
    pub(crate) fn is_queued(&self) -> bool {
        matches!(&self.repr, Repr::Table(t) if t.queued)
    }

    /// Finish whatever resize is in progress now, and free what it leaves
    /// (tests).
    #[cfg(test)]
    pub(crate) fn finish_resize(&mut self) {
        while !self.background_step(usize::MAX).0 {}
    }
}

// ── the background's part (#764 step 3) ───────────────────────────────────────
//
// As for sets (`member_set`, `resize_background`): a write that leaves its
// hash mid-resize, or with a finished resize's old array kept back, hands
// the hash over, and the active-expire thread finishes it a step at a time
// under the hash's own lock. A hash with field TTLs (`HashInner`) is handed
// over the same way.

type Shared = Arc<RwLock<HashFields>>;
type SharedEx = Arc<RwLock<crate::types::HashInner>>;

impl crate::resize_background::Resizable for RwLock<HashFields> {
    fn try_step(&self, budget: usize) -> Option<(bool, Option<crate::collection_table::Retired>)> {
        Some(self.try_write()?.background_step(budget))
    }

    fn try_settle(&self) -> Option<bool> {
        Some(self.try_write()?.settle())
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

impl crate::resize_background::Resizable for RwLock<crate::types::HashInner> {
    fn try_step(&self, budget: usize) -> Option<(bool, Option<crate::collection_table::Retired>)> {
        Some(self.try_write()?.fields.background_step(budget))
    }

    fn try_settle(&self) -> Option<bool> {
        Some(self.try_write()?.fields.settle())
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

/// After a write to `hash` (whose write lock `inner` is): hands it to the
/// background if it has work there. A few instructions when not.
#[inline]
pub fn after_write(hash: &Shared, inner: &mut HashFields) {
    if inner.hand_over() {
        hand(hash);
    }
}

/// [`after_write`] of a hash with field TTLs.
#[inline]
pub fn after_write_ex(hash: &SharedEx, inner: &mut crate::types::HashInner) {
    if inner.fields.hand_over() {
        hand_ex(hash);
    }
}

#[cold]
fn hand(hash: &Shared) {
    let weak: Weak<RwLock<HashFields>> = Arc::downgrade(hash);
    crate::resize_background::hand(weak);
}

#[cold]
fn hand_ex(hash: &SharedEx) {
    let weak: Weak<RwLock<crate::types::HashInner>> = Arc::downgrade(hash);
    crate::resize_background::hand(weak);
}

/// `hash` made a key's value: shared, and handed to the background if a
/// resize it went through while it was built is still in progress. What
/// every hash made whole goes through (a load, `RESTORE`, `COPY`, a new key
/// of `HSET`).
///
/// A table moved here out of another key's value (a hash that gets its
/// first field TTL: `mem::take` then `shared_ex`) may still say it was
/// handed over: but that was the old value's handle, which nothing holds
/// any more, and a new `Arc` is never in the background's registry. The
/// mark is cleared first, so the table is handed over again under its new
/// value if it has work there.
pub fn shared(mut hash: HashFields) -> Shared {
    hash.unqueue();
    let due = hash.hand_over();
    let shared = Arc::new(RwLock::new(hash));
    if due {
        hand(&shared);
    }
    shared
}

/// [`shared`] of a hash with field TTLs (one made whole, or a hash that
/// gets its first field TTL and moves its fields to a `HashInner`).
pub fn shared_ex(mut hash: crate::types::HashInner) -> SharedEx {
    hash.fields.unqueue();
    let due = hash.fields.hand_over();
    let shared = Arc::new(RwLock::new(hash));
    if due {
        hand_ex(&shared);
    }
    shared
}

pub enum Iter<'a> {
    Packed(packed_hash::Iter<'a>),
    Table(FieldIter<'a>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], Value<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Packed(it) => it.next().map(|(f, v)| (f, Value::Bytes(v))),
            Iter::Table(it) => it.next().map(pair),
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
            Iter::Table(it) => it.nth(n).map(pair),
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl<'a> IntoIterator for &'a HashFields {
    type Item = (&'a [u8], Value<'a>);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// The fields and values of a hash given away, copied out.
pub enum IntoIter {
    Packed(PackedHash, usize),
    Table(std::vec::IntoIter<(Vec<u8>, Arc<[u8]>)>),
}

impl Iterator for IntoIter {
    type Item = (Vec<u8>, Arc<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IntoIter::Packed(p, next) => {
                let i = *next;
                (i < p.len()).then(|| {
                    *next += 1;
                    {
                        let (field, value) = p.entry(i);
                        (field.to_vec(), Arc::from(value))
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
    type Item = (Vec<u8>, Arc<[u8]>);
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        match self.repr {
            Repr::Packed(p) => IntoIter::Packed(p, 0),
            Repr::Table(t) => IntoIter::Table(
                t.fields
                    .iter()
                    .map(|entry| (entry.field.to_vec(), Arc::from(entry.value)))
                    .collect::<Vec<_>>()
                    .into_iter(),
            ),
        }
    }
}

impl<F: AsRef<[u8]>> FromIterator<(F, Arc<[u8]>)> for HashFields {
    fn from_iter<I: IntoIterator<Item = (F, Arc<[u8]>)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut out = Self::with_capacity(iter.size_hint().0);
        for (field, value) in iter {
            out.insert_owned(field, value);
        }
        out
    }
}

impl<F: AsRef<[u8]>> Extend<(F, Arc<[u8]>)> for HashFields {
    fn extend<I: IntoIterator<Item = (F, Arc<[u8]>)>>(&mut self, iter: I) {
        for (field, value) in iter {
            self.insert_owned(field, value);
        }
    }
}

impl std::ops::Index<&[u8]> for HashFields {
    type Output = [u8];

    /// The value of `field`, which the hash must have.
    fn index(&self, field: &[u8]) -> &[u8] {
        self.get_bytes(field).expect("no such field")
    }
}

impl std::ops::Index<&str> for HashFields {
    type Output = [u8];

    /// The value of the field named `field`, which the hash must have.
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
            .entries(self.iter().map(|(field, value)| {
                (
                    String::from_utf8_lossy(field).into_owned(),
                    String::from_utf8_lossy(&value).into_owned(),
                )
            }))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`saved_name`] as it was written before its ASCII path: every name
    /// through [`crate::key_bytes::text_of`].
    fn saved_name_reference(field: &[u8]) -> Vec<u8> {
        crate::key_bytes::text_of(field).into_owned().into_bytes()
    }

    #[test]
    fn saved_name_is_the_reference_for_every_kind_of_name() {
        let mut names: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"f000000000000001".to_vec(),
            vec![0x7f, 0x00, b'a'],
            "caf\u{e9}".as_bytes().to_vec(),
            "\u{1F600} \u{4E2D}".as_bytes().to_vec(),
            "\u{F780}".as_bytes().to_vec(),
            "a\u{F7FF}b\u{F77F}c\u{F800}".as_bytes().to_vec(),
            vec![0xEF, 0x9E],
            vec![0xff],
            b"abc\xc3".to_vec(),
            b"\xe2\x82 tail".to_vec(),
            (0u8..=255).collect(),
        ];
        // Pseudo-random names of each kind: ASCII, UTF-8, UTF-8 with the
        // block's characters, and bytes that are not UTF-8.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let pool = [
            'a',
            '~',
            '\u{e9}',
            '\u{F780}',
            '\u{F7C0}',
            '\u{F7FF}',
            '\u{10FFFF}',
        ];
        for i in 0..20_000usize {
            let len = (next() % 40) as usize;
            let name: Vec<u8> = match i % 4 {
                0 => (0..len).map(|_| (next() % 128) as u8).collect(),
                1 => (0..len)
                    .map(|_| char::from_u32((next() % 0xD000) as u32).unwrap_or('x'))
                    .collect::<String>()
                    .into_bytes(),
                2 => (0..len)
                    .map(|_| pool[(next() % pool.len() as u64) as usize])
                    .collect::<String>()
                    .into_bytes(),
                _ => (0..len).map(|_| next() as u8).collect(),
            };
            names.push(name);
        }
        let mut kinds = [0usize; 4];
        for name in &names {
            let saved = saved_name(name);
            assert_eq!(*saved, *saved_name_reference(name), "{name:?}");
            assert_eq!(
                matches!(saved, std::borrow::Cow::Borrowed(_)),
                *saved == **name,
                "{name:?}"
            );
            assert_eq!(loaded_name(&saved).as_deref(), Some(&name[..]), "{name:?}");
            let kind = match std::str::from_utf8(name) {
                Ok(_) if name.is_ascii() => 0,
                Ok(text) if text.chars().any(|c| ('\u{F780}'..='\u{F7FF}').contains(&c)) => 2,
                Ok(_) => 1,
                Err(_) => 3,
            };
            kinds[kind] += 1;
        }
        assert!(kinds.iter().all(|&n| n > 1000), "{kinds:?}");
    }

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
        fn remove(&mut self, field: &str) -> bool {
            match self.0.iter().position(|(f, _)| f == field) {
                Some(i) => {
                    self.0.remove(i);
                    true
                }
                None => false,
            }
        }
    }

    /// Contents and `len` always; the order while the hash is packed (a
    /// table has none).
    fn assert_same(hash: &HashFields, model: &Model, step: usize) {
        assert_eq!(hash.len(), model.0.len(), "step {step}");
        let mut held: Vec<(String, Vec<u8>)> = hash
            .iter()
            .map(|(f, v)| (String::from_utf8(f.to_vec()).unwrap(), v.to_vec()))
            .collect();
        if hash.is_table() {
            let mut want = model.0.clone();
            held.sort();
            want.sort();
            assert_eq!(held, want, "step {step}");
        } else {
            assert_eq!(held, model.0, "step {step}");
        }
        for (f, v) in &model.0 {
            assert_eq!(hash.get_bytes(f), Some(v.as_slice()), "step {step} {f}");
        }
    }

    struct Limits;
    impl Drop for Limits {
        fn drop(&mut self) {
            own_limits(None);
        }
    }

    fn rng() -> rand::rngs::StdRng {
        use rand::SeedableRng;
        rand::rngs::StdRng::seed_from_u64(764)
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
        let mut fields: Vec<&[u8]> = hash.keys().collect();
        fields.sort_unstable();
        assert_eq!(fields, [b"f0", b"f1", b"f2", b"f3", b"f4"]);
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
        assert_eq!(hash.get_bytes(&format!("{:064}", 0)), Some(&value[..]));
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
        let mut rng = rng();
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
                            65 + (r >> 40) as usize % 300
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
                        let had = model.remove(&name);
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
                        match hash.random(&mut rng) {
                            Some((f, v)) => assert!(
                                model
                                    .0
                                    .iter()
                                    .any(|(mf, mv)| mf.as_bytes() == f && mv == v.bytes()),
                                "step {step}"
                            ),
                            None => assert!(model.0.is_empty()),
                        }
                        let n = (r >> 30) as usize % (model.0.len() + 2);
                        let picked = hash.random_distinct(n, &mut rng);
                        assert_eq!(picked.len(), n.min(model.0.len()));
                        let distinct: std::collections::HashSet<&[u8]> =
                            picked.iter().map(|(f, _)| *f).collect();
                        assert_eq!(distinct.len(), picked.len());
                        for (f, v) in &picked {
                            assert_eq!(hash.get_bytes(f), Some(v.bytes()));
                        }
                    }
                    14 => {
                        let cut = (r >> 40) % 5;
                        let keep = |f: &[u8]| f[f.len() - 1] as u64 % 5 != cut;
                        hash.retain(|f, _| keep(f));
                        model.0.retain(|(f, _)| keep(f.as_bytes()));
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
                if !hash.is_table() {
                    assert!(hash.len() <= packed_hash::MAX_ENTRIES);
                }
                assert_same(&hash, &model, step);
            }
        }
    }

    /// A field loaded twice (a file or a payload that names it twice) keeps
    /// its last value, in a block (and its first place there) and in a
    /// table.
    #[test]
    fn a_field_loaded_twice_keeps_its_last_value() {
        for (n, table) in [(3usize, false), (600, true)] {
            let mut pairs: Vec<(String, Arc<[u8]>)> = (0..n)
                .map(|i| (format!("f{i}"), Arc::from(format!("v{i}").as_bytes())))
                .collect();
            pairs.push(("f1".to_string(), Arc::from(&b"again"[..])));
            let collected: HashFields = pairs.iter().cloned().collect();
            let mut loaded = HashFields::with_capacity(pairs.len());
            for (field, value) in &pairs {
                loaded.insert_loaded(field.clone(), value).unwrap();
            }
            for hash in [&collected, &loaded] {
                assert_eq!(hash.is_table(), table, "{n}");
                assert_eq!(hash.len(), n, "{n}");
                assert_eq!(hash.get_bytes("f1"), Some(&b"again"[..]));
                if !table {
                    let fields: Vec<&[u8]> = hash.keys().collect();
                    assert_eq!(fields, [b"f0", b"f1", b"f2"]);
                }
            }
        }
        assert!(HashFields::table_with_capacity(2).is_table());
    }

    #[test]
    fn a_hash_given_away_hands_over_its_fields() {
        for n in [3usize, 600] {
            let mut hash = HashFields::new();
            for i in 0..n {
                hash.insert_bytes(&format!("f{i}"), format!("v{i}").as_bytes());
            }
            let pairs: Vec<(Vec<u8>, Arc<[u8]>)> = hash.clone().into_iter().collect();
            assert_eq!(pairs.len(), n);
            assert!(pairs.contains(&(b"f2".to_vec(), Arc::from(&b"v2"[..]))));
            let back: HashFields = pairs.into_iter().collect();
            assert_eq!(back, hash);
            assert_eq!(back.is_table(), hash.is_table());
        }
    }

    /// A load refuses a field or a value longer than a table holds, with an
    /// error and nothing changed (#965's review: never dropped). Tried with
    /// the table's limit lowered: 4 GiB of bytes is not something a test
    /// makes.
    #[test]
    fn a_load_refuses_what_a_table_cannot_hold() {
        struct Back;
        impl Drop for Back {
            fn drop(&mut self) {
                crate::collection_table::set_test_max_len(None);
            }
        }
        let _back = Back;
        crate::collection_table::set_test_max_len(Some(100));
        let mut hash = HashFields::with_capacity(1_000);
        assert!(hash.insert_loaded("f", b"v").unwrap());
        let long = vec![b'x'; 101];
        let err = hash.insert_loaded("g", &long).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let err = hash
            .insert_loaded(String::from_utf8(long.clone()).unwrap(), b"v")
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(hash.len(), 1);
        assert_eq!(hash.get_bytes("f"), Some(&b"v"[..]));
        // A block moves to the table for a long value, and is refused the
        // same way.
        let mut small = HashFields::new();
        small.insert_loaded("a", b"1").unwrap();
        assert!(small.insert_loaded("b", &long).is_err());
        assert!(small.get_bytes("b").is_none());
    }

    /// Random picks of a table are uniform: each of 1,000 fields comes up
    /// about as often as another (a chi-square bound, generous).
    #[test]
    fn random_fields_of_a_table_are_uniform() {
        let n = 1_000;
        let mut hash = HashFields::new();
        for i in 0..n {
            hash.insert_bytes(&format!("field:{i}"), b"v");
        }
        assert!(hash.is_table());
        let mut counts: ahash::AHashMap<String, usize> = ahash::AHashMap::new();
        let per = 200;
        let mut rng = rng();
        for _ in 0..n * per {
            let (f, _) = hash.random(&mut rng).unwrap();
            *counts
                .entry(String::from_utf8(f.to_vec()).unwrap())
                .or_default() += 1;
        }
        assert_eq!(counts.len(), n);
        let chi: f64 = counts
            .values()
            .map(|&c| {
                let d = c as f64 - per as f64;
                d * d / per as f64
            })
            .sum();
        // 999 degrees of freedom: mean 999, sd about 45.
        assert!(chi < 1_300.0, "chi-square {chi}");
    }

    /// A table that a load made for many but that holds few (fields past
    /// their time dropped) is put at its size: a block if they fit one,
    /// else a table with room for them.
    #[test]
    fn a_table_that_kept_few_fits_them() {
        let _limits = Limits;
        own_limits(Some([DEFAULT_PACKED_ENTRIES, DEFAULT_PACKED_VALUE]));
        let mut small = HashFields::with_capacity(100_000);
        let mut large = HashFields::with_capacity(100_000);
        for i in 0..100_000 {
            small.insert_bytes(&format!("f{i}"), b"v");
            large.insert_bytes(&format!("f{i}"), b"v");
        }
        small.retain(|f, _| f.len() <= 3);
        large.retain(|f, _| f.len() <= 5);
        let (small, large) = (small.into_fitting(), large.into_fitting());
        assert_eq!(small.encoding(), Encoding::Listpack);
        assert_eq!(small.len(), 100);
        assert!(large.is_table());
        assert_eq!(large.len(), 10_000);
        let fresh = {
            let mut h = HashFields::with_capacity(10_000);
            for (f, v) in large.iter() {
                h.insert_bytes(f, &v);
            }
            h
        };
        assert!(large.allocation_size() <= fresh.allocation_size() * 11 / 10);
        // A table already near its size is left as it is.
        let mut kept = HashFields::with_capacity(1_000);
        for i in 0..1_000 {
            kept.insert_bytes(&format!("f{i}"), b"v");
        }
        let before = kept.allocation_size();
        assert_eq!(kept.into_fitting().allocation_size(), before);
    }

    /// A plain hash handed to the background mid-resize that gets its
    /// first field TTL (its fields taken out of the old value and wrapped
    /// anew, as HEXPIRE, HSETEX and HGETEX do) is handed over again under
    /// the new value: the old value's mark does not come with it.
    #[test]
    fn a_table_moved_to_a_new_value_is_handed_over_again() {
        let _limits = Limits;
        own_limits(Some([DEFAULT_PACKED_ENTRIES, DEFAULT_PACKED_VALUE]));
        let plain = shared(HashFields::new());
        {
            let mut inner = plain.write();
            let mut i = 0usize;
            while i < 5_000 || !inner.table().is_some_and(|t| t.resizing()) {
                inner.insert_bytes(&format!("field:{i:08}"), b"value");
                i += 1;
            }
            after_write(&plain, &mut inner);
            assert!(inner.is_queued());
        }
        // The upgrade, as the commands make it.
        let fields = std::mem::take(&mut *plain.write());
        let ex = shared_ex(crate::types::HashInner::from_fields(fields));
        assert!(
            crate::resize_background::is_handed(Arc::as_ptr(&ex).cast()),
            "the new value was not handed over"
        );
        let mid = ex.read().fields.allocation_size();
        for _ in 0..1_000_000 {
            if !ex.read().fields.is_queued() {
                break;
            }
            crate::resize_background::step_background(std::time::Duration::from_millis(1));
        }
        let inner = ex.read();
        assert!(!inner.fields.table().unwrap().resizing());
        assert!(inner.fields.allocation_size() < mid);
    }

    /// The background finishes a resize the writes left, a step at a time,
    /// and frees the old array; a hash with field TTLs too.
    #[test]
    fn the_background_finishes_a_hashs_resize() {
        let _limits = Limits;
        own_limits(Some([DEFAULT_PACKED_ENTRIES, DEFAULT_PACKED_VALUE]));
        let hash = shared(HashFields::new());
        let mut i = 0usize;
        {
            let mut inner = hash.write();
            while i < 5_000 || !inner.table().is_some_and(|t| t.resizing()) {
                inner.insert_bytes(&format!("field:{i:08}"), b"value");
                i += 1;
            }
            after_write(&hash, &mut inner);
            assert!(inner.is_queued());
        }
        assert!(crate::resize_background::is_handed(
            Arc::as_ptr(&hash).cast()
        ));
        let mid = hash.read().allocation_size();
        for _ in 0..1_000_000 {
            {
                let inner = hash.read();
                let t = inner.table().unwrap();
                if !t.resizing() && !t.holds_retired() && !inner.is_queued() {
                    break;
                }
            }
            crate::resize_background::step_background(std::time::Duration::from_millis(1));
        }
        let inner = hash.read();
        assert!(!inner.table().unwrap().resizing());
        assert!(!inner.is_queued());
        assert!(inner.allocation_size() < mid);
        assert_eq!(inner.len(), i);
        assert_eq!(inner.get_bytes("field:00000007"), Some(&b"value"[..]));
        drop(inner);
        // With field TTLs: the same, through `HashInner`.
        let mut fields = HashFields::table_with_capacity(0);
        let mut j = 0usize;
        while j < 5_000 || !fields.table().unwrap().resizing() {
            fields.insert_bytes(&format!("f{j}"), b"v");
            j += 1;
        }
        let ex = shared_ex(crate::types::HashInner::from_fields(fields));
        assert!(ex.read().fields.is_queued());
        for _ in 0..1_000_000 {
            if !ex.read().fields.is_queued() {
                break;
            }
            crate::resize_background::step_background(std::time::Duration::from_millis(1));
        }
        let inner = ex.read();
        assert!(!inner.fields.table().unwrap().resizing());
        assert_eq!(inner.len(), j);
        let mut copy = inner.fields.clone();
        copy.finish_resize();
        assert_eq!(copy, inner.fields);
    }

    /// `MEMORY USAGE`'s figure is exact in both forms: what was allocated
    /// for the hash, mid-resize included, by the table's own count.
    #[test]
    fn allocation_size_counts_the_box_and_the_table() {
        let mut hash = HashFields::new();
        for i in 0..2_000 {
            hash.insert_bytes(&format!("field:{i}"), b"value");
        }
        let Repr::Table(t) = &hash.repr else {
            panic!("a table");
        };
        assert_eq!(
            hash.allocation_size(),
            std::mem::size_of::<Table>() + t.fields.allocation_size()
        );
        assert_eq!(hash.freeing_at_least(0), hash.allocation_size());
        assert_eq!(hash.layout_bytes(5), hash.allocation_size());
    }
}
