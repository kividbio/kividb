//! A small hash, fields and values, in one block of memory.
//!
//! The hash's counterpart of [`crate::packed_set`]: a hash of a few short
//! fields does not need a table that pays a header and an `Arc<[u8]>`
//! (two allocations, 40 bytes of headers) and a slot for every field. Here
//! each field lies next to its value, all of them end to end in one buffer,
//! and a field is found by scanning one byte a field: its *fingerprint*
//! (the one a packed set uses), kept together at the front, sixteen
//! compared at a time. Only a field whose fingerprint matches is compared
//! in full.
//!
//! The buffer, for a capacity of `cap` fields (8, 16, ... 512):
//!
//! ```text
//! [ fingerprints: cap bytes ][ offsets: (2 * cap + 1) x u16 ][ field 0 | value 0 | field 1 | value 1 ... ]
//! ```
//!
//! `offsets[2 * i]` is where field `i` begins in the bytes,
//! `offsets[2 * i + 1]` where it ends and its value begins, and
//! `offsets[2 * i + 2]` where the value ends: no length is stored, and
//! field `i` and its value are reached in one step, so a random field is a
//! random index. Five bytes a field besides its own and its value's.
//!
//! Fields keep the order they were first set in, which is the order
//! `HGETALL` gives them in; removing one closes the gap. A value set again
//! keeps its place, and the bytes after it move when its length changes.
//!
//! The block refuses a field or a value longer than [`MAX_LEN`], a field
//! past [`MAX_ENTRIES`], and bytes past what a `u16` offset reaches:
//! whoever holds it then moves to a table.

use crate::packed_set::{fingerprint, same, scan_fingerprints};

/// The most fields a packed hash holds.
pub const MAX_ENTRIES: usize = 512;
/// The longest field, and the longest value, a packed hash holds.
pub const MAX_LEN: usize = 64;
/// The most bytes of fields and values together: an offset is a `u16`.
const MAX_DATA: usize = u16::MAX as usize;

/// What [`PackedHash::set`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Set {
    /// The field is new and was added, with its value.
    Added,
    /// The field was there and has the new value.
    Replaced,
    /// The field or the value cannot be held: one is too long, or the hash
    /// is at a limit. The hash is unchanged.
    NoRoom,
}

#[derive(Clone, Default)]
pub struct PackedHash {
    buf: Vec<u8>,
    /// Fields held.
    len: u16,
    /// Fields there is room for in the fingerprints and offsets.
    cap: u16,
}

impl PackedHash {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The bytes of the hash's one allocation.
    #[inline]
    pub fn allocation_size(&self) -> usize {
        self.buf.capacity()
    }

    /// The bytes of the fields and values held, without the block's own.
    #[inline]
    pub fn data_len(&self) -> usize {
        if self.cap == 0 {
            0
        } else {
            self.offset(2 * self.len())
        }
    }

    /// The bytes before the fields' and values' for a capacity of `cap`.
    #[inline]
    fn head(cap: usize) -> usize {
        cap + 2 * (2 * cap + 1)
    }

    /// Where the fields' and values' bytes begin.
    #[inline]
    fn data_start(&self) -> usize {
        Self::head(self.cap as usize)
    }

    /// The offsets, each as its two bytes: `offsets()[j]` is offset `j`.
    #[inline(always)]
    fn offsets(&self) -> &[[u8; 2]] {
        let cap = self.cap as usize;
        self.buf[cap..Self::head(cap)].as_chunks::<2>().0
    }

    #[inline(always)]
    fn offsets_mut(&mut self) -> &mut [[u8; 2]] {
        let cap = self.cap as usize;
        self.buf[cap..Self::head(cap)].as_chunks_mut::<2>().0
    }

    #[inline(always)]
    fn offset(&self, j: usize) -> usize {
        u16::from_le_bytes(self.offsets()[j]) as usize
    }

    /// Field `i` and its value. `i < len()`.
    #[inline]
    pub fn entry(&self, i: usize) -> (&[u8], &[u8]) {
        debug_assert!(i < self.len());
        let start = self.data_start();
        let o = &self.offsets()[2 * i..2 * i + 3];
        let (from, mid, to) = (
            u16::from_le_bytes(o[0]) as usize,
            u16::from_le_bytes(o[1]) as usize,
            u16::from_le_bytes(o[2]) as usize,
        );
        self.buf[start + from..start + to].split_at(mid - from)
    }

    /// Field `i`, in the order fields were first set. `i < len()`.
    #[inline]
    pub fn field(&self, i: usize) -> &[u8] {
        self.entry(i).0
    }

    /// The value of field `i`. `i < len()`.
    #[inline]
    pub fn value(&self, i: usize) -> &[u8] {
        debug_assert!(i < self.len());
        let start = self.data_start();
        let o = &self.offsets()[2 * i + 1..2 * i + 3];
        let (from, to) = (
            u16::from_le_bytes(o[0]) as usize,
            u16::from_le_bytes(o[1]) as usize,
        );
        &self.buf[start + from..start + to]
    }

    /// Where `field` is, if the hash has it.
    #[inline]
    pub fn find(&self, field: &(impl AsRef<[u8]> + ?Sized)) -> Option<usize> {
        let field = field.as_ref();
        if self.len == 0 || field.len() > MAX_LEN {
            return None;
        }
        self.find_by(field, fingerprint(field))
    }

    /// Where `field` is, or, if the hash does not have it, its fingerprint,
    /// for [`push_fingerprinted`](Self::push_fingerprinted): a write looks
    /// for a field and adds it with one fingerprint computed.
    #[inline]
    pub fn locate(&self, field: &(impl AsRef<[u8]> + ?Sized)) -> Result<usize, u8> {
        let field = field.as_ref();
        if field.len() > MAX_LEN {
            return Err(0);
        }
        let f = fingerprint(field);
        if self.len == 0 {
            return Err(f);
        }
        self.find_by(field, f).ok_or(f)
    }

    /// The value of `field`, if the hash has it.
    #[inline(never)]
    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        self.find(field).map(|i| self.value(i))
    }

    #[inline(never)]
    pub fn contains(&self, field: &[u8]) -> bool {
        self.find(field).is_some()
    }

    /// The scan, as a packed set's ([`scan_fingerprints`]): the
    /// fingerprints take `cap` bytes, a multiple of eight, and the offsets
    /// follow them, at least `5 * cap + 2` bytes in all, so a vector from 0
    /// is always there, and 64 bytes from any `i < n` once there are more
    /// than 16 fields (a capacity of 32 or more).
    #[inline(always)]
    fn find_by(&self, field: &[u8], f: u8) -> Option<usize> {
        let n = self.len();
        let cap = self.cap as usize;
        debug_assert!(n <= cap && cap.is_multiple_of(8));
        let start = Self::head(cap);
        scan_fingerprints(&self.buf, n, f, |at| self.is_at(at, start, field))
    }

    /// Whether field `at` is `field`: the length first, from the field's
    /// two offsets read as one word, then the bytes.
    #[inline(always)]
    fn is_at(&self, at: usize, start: usize, field: &[u8]) -> bool {
        let o = self.cap as usize + 4 * at;
        let both = u32::from_le_bytes(self.buf[o..o + 4].try_into().unwrap());
        let (from, to) = ((both & 0xffff) as usize, (both >> 16) as usize);
        to.wrapping_sub(from) == field.len() && same(&self.buf[start + from..start + to], field)
    }

    /// Give `field` the value `value`, adding the field if it is new.
    pub fn set(&mut self, field: &(impl AsRef<[u8]> + ?Sized), value: &[u8]) -> Set {
        let field = field.as_ref();
        match self.locate(field) {
            Ok(i) => {
                if self.set_value_at(i, value) {
                    Set::Replaced
                } else {
                    Set::NoRoom
                }
            }
            Err(f) => {
                if self.push_fingerprinted(field, value, f) {
                    Set::Added
                } else {
                    Set::NoRoom
                }
            }
        }
    }

    /// Add a field known not to be there, if it and its value fit.
    /// `false`, and nothing changed, if they do not.
    pub fn push_new(&mut self, field: &(impl AsRef<[u8]> + ?Sized), value: &[u8]) -> bool {
        let field = field.as_ref();
        debug_assert!(self.find(field).is_none());
        self.push_fingerprinted(field, value, fingerprint(field))
    }

    /// [`push_new`](Self::push_new) of a field whose fingerprint is known
    /// ([`locate`](Self::locate) gave it).
    pub fn push_fingerprinted(&mut self, field: &[u8], value: &[u8], f: u8) -> bool {
        debug_assert!(self.find(field).is_none());
        let n = self.len();
        if field.len() > MAX_LEN || value.len() > MAX_LEN || n >= MAX_ENTRIES {
            return false;
        }
        let end = self.data_len();
        let more = field.len() + value.len();
        if end + more > MAX_DATA {
            return false;
        }
        if n == self.cap as usize {
            self.grow();
        }
        self.reserve(more);
        self.buf.extend_from_slice(field);
        self.buf.extend_from_slice(value);
        self.buf[n] = f;
        let o = &mut self.offsets_mut()[2 * n + 1..2 * n + 3];
        o[0] = ((end + field.len()) as u16).to_le_bytes();
        o[1] = ((end + more) as u16).to_le_bytes();
        self.len += 1;
        true
    }

    /// Room for `more` bytes after what is there: a quarter over at most,
    /// not the doubling a `Vec` does by itself, since the block is the
    /// whole of a small hash's memory.
    #[inline]
    fn reserve(&mut self, more: usize) {
        if self.buf.capacity() - self.buf.len() < more {
            self.buf.reserve_exact(more.max(self.buf.len() / 4).max(16));
        }
    }

    /// Give field `i` the value `value`, in its place. `false`, and nothing
    /// changed, if the value is too long or the bytes would pass the limit.
    pub fn set_value_at(&mut self, i: usize, value: &[u8]) -> bool {
        let n = self.len();
        debug_assert!(i < n);
        let start = self.data_start();
        let (from, to) = (self.offset(2 * i + 1), self.offset(2 * i + 2));
        let old = to - from;
        if value.len() == old {
            self.buf[start + from..start + to].copy_from_slice(value);
            return true;
        }
        if value.len() > MAX_LEN || self.data_len() - old + value.len() > MAX_DATA {
            return false;
        }
        if value.len() > old {
            let more = value.len() - old;
            self.reserve(more);
            // Open the gap: the bytes after the old value move up, and the
            // new value is written over the old one and the gap.
            let end = self.buf.len();
            self.buf.resize(end + more, 0);
            self.buf.copy_within(start + to..end, start + to + more);
            // The offsets after it move up with them: a loop the compiler
            // does many at a time.
            for o in &mut self.offsets_mut()[2 * i + 2..=2 * n] {
                *o = u16::from_le_bytes(*o)
                    .wrapping_add(more as u16)
                    .to_le_bytes();
            }
        } else {
            let less = old - value.len();
            let end = self.buf.len();
            self.buf.copy_within(start + to..end, start + to - less);
            self.buf.truncate(end - less);
            for o in &mut self.offsets_mut()[2 * i + 2..=2 * n] {
                *o = u16::from_le_bytes(*o)
                    .wrapping_sub(less as u16)
                    .to_le_bytes();
            }
        }
        self.buf[start + from..start + from + value.len()].copy_from_slice(value);
        true
    }

    /// Room for twice the fields (eight to begin with): the fingerprints
    /// and the offsets are laid out again, the fields and values after them.
    fn grow(&mut self) {
        let n = self.len();
        let old_cap = self.cap as usize;
        let cap = if old_cap == 0 { 8 } else { old_cap * 2 };
        assert!(cap <= MAX_ENTRIES);
        let data = self.data_len();
        let head = Self::head(cap);
        let mut buf = Vec::with_capacity(head + data + (data / 4).max(32));
        buf.resize(head, 0);
        if old_cap != 0 {
            buf[..n].copy_from_slice(&self.buf[..n]);
            let offsets = 2 * (2 * n + 1);
            buf[cap..cap + offsets].copy_from_slice(&self.buf[old_cap..old_cap + offsets]);
            let start = self.data_start();
            buf.extend_from_slice(&self.buf[start..start + data]);
        }
        self.buf = buf;
        self.cap = cap as u16;
    }

    /// Take field `i` and its value out; the fields after it keep their
    /// order.
    pub fn remove_at(&mut self, i: usize) {
        let n = self.len();
        debug_assert!(i < n);
        let start = self.data_start();
        let (from, to) = (self.offset(2 * i), self.offset(2 * i + 2));
        let gone = to - from;
        let end = self.buf.len();
        self.buf.copy_within(start + to..end, start + from);
        self.buf.truncate(end - gone);
        self.buf.copy_within(i + 1..n, i);
        // Offset `j` becomes what offset `j + 2` was, less the bytes gone:
        // moved down whole, then a loop the compiler does many at a time.
        let offsets = self.offsets_mut();
        offsets.copy_within(2 * i + 2..=2 * n, 2 * i);
        for o in &mut offsets[2 * i..=2 * (n - 1)] {
            *o = u16::from_le_bytes(*o)
                .wrapping_sub(gone as u16)
                .to_le_bytes();
        }
        self.len -= 1;
    }

    /// Remove `field` if the hash has it.
    pub fn remove(&mut self, field: &(impl AsRef<[u8]> + ?Sized)) -> bool {
        match self.find(field) {
            Some(i) => {
                self.remove_at(i);
                true
            }
            None => false,
        }
    }

    /// Keep the fields `keep` says to.
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8], &[u8]) -> bool) {
        let mut i = 0;
        while i < self.len() {
            let (field, value) = self.entry(i);
            if keep(field, value) {
                i += 1;
            } else {
                self.remove_at(i);
            }
        }
    }

    pub fn clear(&mut self) {
        self.buf = Vec::new();
        self.len = 0;
        self.cap = 0;
    }

    /// The fields and their values, in the order the fields were first set.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        if self.cap == 0 {
            return Iter {
                data: &[],
                offsets: &[],
                at: 0,
                next: 0,
                len: 0,
            };
        }
        Iter {
            data: &self.buf[self.data_start()..],
            offsets: self.offsets(),
            at: 0,
            next: 0,
            len: self.len(),
        }
    }
}

/// The fields and values in order: each begins where the one before ended,
/// so a step reads the two offsets that end its field and its value.
pub struct Iter<'a> {
    data: &'a [u8],
    offsets: &'a [[u8; 2]],
    /// Where the next field begins.
    at: usize,
    next: usize,
    len: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.len {
            return None;
        }
        let o = &self.offsets[2 * self.next + 1..2 * self.next + 3];
        let (mid, to) = (
            u16::from_le_bytes(o[0]) as usize,
            u16::from_le_bytes(o[1]) as usize,
        );
        let (field, value) = self.data[self.at..to].split_at(mid - self.at);
        self.at = to;
        self.next += 1;
        Some((field, value))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.len - self.next;
        (left, Some(left))
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl std::fmt::Debug for PackedHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.iter().map(|(field, value)| {
                (
                    String::from_utf8_lossy(field),
                    String::from_utf8_lossy(value),
                )
            }))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(i: usize) -> String {
        format!("field:{i:08}")
    }

    /// What the block should hold: the fields in the order first set.
    #[derive(Default)]
    struct Model(Vec<(String, Vec<u8>)>);

    impl Model {
        fn set(&mut self, field: &str, value: &[u8]) {
            match self.0.iter_mut().find(|(f, _)| f == field) {
                Some((_, v)) => *v = value.to_vec(),
                None => self.0.push((field.to_string(), value.to_vec())),
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

    fn assert_same(hash: &PackedHash, model: &Model) {
        assert_eq!(hash.len(), model.0.len());
        let held: Vec<(String, Vec<u8>)> = hash
            .iter()
            .map(|(f, v)| (String::from_utf8(f.to_vec()).unwrap(), v.to_vec()))
            .collect();
        assert_eq!(held, model.0);
        for (i, (f, v)) in model.0.iter().enumerate() {
            assert_eq!(hash.find(f), Some(i), "{f}");
            assert_eq!(hash.get(f.as_bytes()), Some(v.as_slice()), "{f}");
            assert_eq!(hash.entry(i), (f.as_bytes(), v.as_slice()));
        }
        let bytes: usize = model.0.iter().map(|(f, v)| f.len() + v.len()).sum();
        assert_eq!(hash.data_len(), bytes);
    }

    #[test]
    fn it_is_a_hash() {
        let mut hash = PackedHash::new();
        assert!(hash.is_empty());
        assert_eq!(hash.get(b"a"), None);
        assert_eq!(hash.allocation_size(), 0);
        assert_eq!(hash.data_len(), 0);
        assert_eq!(hash.set("a", b"1"), Set::Added);
        assert_eq!(hash.set("a", b"2"), Set::Replaced);
        assert_eq!(hash.set("", b""), Set::Added);
        assert_eq!(hash.set("", b"empty"), Set::Replaced);
        assert_eq!(hash.len(), 2);
        assert_eq!(hash.get(b"a"), Some(&b"2"[..]));
        assert_eq!(hash.get(b""), Some(&b"empty"[..]));
        assert_eq!(hash.get(b"b"), None);
        assert!(hash.remove("a"));
        assert!(!hash.remove("a"));
        assert_eq!(hash.len(), 1);
        assert_eq!(hash.entry(0), (&b""[..], &b"empty"[..]));
        hash.clear();
        assert!(hash.is_empty() && !hash.contains(b""));
        assert_eq!(hash.allocation_size(), 0);
    }

    #[test]
    fn fields_keep_the_order_they_were_first_set_in() {
        let mut hash = PackedHash::new();
        let mut model = Model::default();
        for i in 0..40 {
            assert_eq!(hash.set(&field(i), b"v"), Set::Added);
            model.set(&field(i), b"v");
        }
        // A value set again keeps its place, whatever its length.
        for (i, value) in [
            (3, &b""[..]),
            (17, b"a longer value than before"),
            (39, b"x"),
        ] {
            assert_eq!(hash.set(&field(i), value), Set::Replaced);
            model.set(&field(i), value);
            assert_same(&hash, &model);
        }
        for i in [0, 39, 20, 1] {
            assert!(hash.remove(&field(i)));
            assert!(model.remove(&field(i)));
            assert_same(&hash, &model);
        }
        // One removed and set again goes to the end.
        assert_eq!(hash.set(&field(0), b"back"), Set::Added);
        model.set(&field(0), b"back");
        assert_same(&hash, &model);
        assert_eq!(hash.field(hash.len() - 1), field(0).as_bytes());
    }

    #[test]
    fn it_grows_through_every_capacity_to_the_limit() {
        let mut hash = PackedHash::new();
        let mut model = Model::default();
        for i in 0..MAX_ENTRIES {
            let value = format!("value:{i}");
            assert_eq!(hash.set(&field(i), value.as_bytes()), Set::Added, "{i}");
            model.set(&field(i), value.as_bytes());
            if i.is_power_of_two() || (i + 1).is_power_of_two() {
                assert_same(&hash, &model);
            }
        }
        assert_same(&hash, &model);
        // Full: a new field is refused and nothing changes; one that is
        // there can still be set and removed.
        assert_eq!(hash.set("one more", b"v"), Set::NoRoom);
        assert!(!hash.push_new("one more", b"v"));
        assert_same(&hash, &model);
        assert_eq!(hash.set(&field(7), b"changed"), Set::Replaced);
        model.set(&field(7), b"changed");
        assert!(hash.remove(&field(300)));
        model.remove(&field(300));
        assert_eq!(hash.set("one more", b"v"), Set::Added);
        model.set("one more", b"v");
        assert_same(&hash, &model);
    }

    #[test]
    fn what_is_too_long_is_refused_and_changes_nothing() {
        let long = "f".repeat(MAX_LEN + 1);
        let longest = "f".repeat(MAX_LEN);
        let mut hash = PackedHash::new();
        let mut model = Model::default();
        assert_eq!(hash.set(&long, b"v"), Set::NoRoom);
        assert_eq!(hash.set("f", long.as_bytes()), Set::NoRoom);
        assert!(hash.is_empty());
        assert_eq!(hash.get(long.as_bytes()), None);
        assert_eq!(hash.set(&longest, longest.as_bytes()), Set::Added);
        model.set(&longest, longest.as_bytes());
        assert_eq!(hash.set("f", b"v"), Set::Added);
        model.set("f", b"v");
        // A value there cannot be made too long either.
        assert_eq!(hash.set("f", long.as_bytes()), Set::NoRoom);
        assert!(!hash.set_value_at(1, long.as_bytes()));
        assert_same(&hash, &model);
    }

    #[test]
    fn the_bytes_stop_at_what_an_offset_reaches() {
        // 511 fields of 64 bytes with 64-byte values are 65,408 bytes: the
        // 512th such pair would end past 65,535.
        let mut hash = PackedHash::new();
        let mut model = Model::default();
        let value = [b'v'; MAX_LEN];
        let name = |i: usize| format!("{i:064}");
        for i in 0..MAX_ENTRIES - 1 {
            assert_eq!(hash.set(&name(i), &value), Set::Added, "{i}");
            model.set(&name(i), &value);
        }
        assert_eq!(hash.data_len(), 511 * 128);
        assert_eq!(hash.set(&name(511), &value), Set::NoRoom);
        assert!(!hash.push_new(&name(511), &value));
        assert_same(&hash, &model);
        // What does fit is taken: 127 bytes are left.
        assert_eq!(hash.set(&name(511), &value[..63]), Set::Added);
        model.set(&name(511), &value[..63]);
        assert_eq!(hash.data_len(), MAX_DATA);
        // Full to the byte: a value cannot grow, but can shrink and grow back.
        assert_eq!(hash.set("", b""), Set::NoRoom);
        let short = name(3);
        assert_eq!(hash.set(&short, &value[..10]), Set::Replaced);
        model.set(&short, &value[..10]);
        assert_same(&hash, &model);
        assert_eq!(hash.set(&short, &value), Set::Replaced);
        model.set(&short, &value);
        assert_same(&hash, &model);
    }

    #[test]
    fn it_matches_a_model_over_a_long_random_run() {
        // A fixed sequence (an xorshift), so a failure repeats.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut hash = PackedHash::new();
        let mut model = Model::default();
        for step in 0..20_000 {
            let r = next();
            let name = field((r % 600) as usize);
            match (r >> 16) % 8 {
                0..=4 => {
                    let value = vec![b'a' + (r >> 24) as u8 % 26; ((r >> 32) % 70) as usize];
                    let there = model.0.iter().any(|(f, _)| *f == name);
                    let fits = value.len() <= MAX_LEN && (there || model.0.len() < MAX_ENTRIES);
                    let did = hash.set(&name, &value);
                    if fits {
                        assert_eq!(did, if there { Set::Replaced } else { Set::Added });
                        model.set(&name, &value);
                    } else {
                        assert_eq!(did, Set::NoRoom, "step {step}");
                    }
                }
                5..=6 => assert_eq!(hash.remove(&name), model.remove(&name), "step {step}"),
                _ => {
                    let cut = (r >> 40) % 4;
                    hash.retain(|f, _| f[f.len() - 1] as u64 % 4 != cut);
                    model
                        .0
                        .retain(|(f, _)| f.as_bytes()[f.len() - 1] as u64 % 4 != cut);
                }
            }
            if step % 64 == 0 {
                assert_same(&hash, &model);
            }
        }
        assert_same(&hash, &model);
    }

    #[test]
    fn fields_with_one_fingerprint_are_told_apart() {
        // More fields than fingerprints: some must share one.
        let mut hash = PackedHash::new();
        for i in 0..400 {
            assert_eq!(
                hash.set(&format!("{i}"), format!("{i}").as_bytes()),
                Set::Added
            );
        }
        for i in 0..400 {
            assert_eq!(
                hash.get(format!("{i}").as_bytes()),
                Some(format!("{i}").as_bytes())
            );
        }
        for i in 400..2_000 {
            assert_eq!(hash.get(format!("{i}").as_bytes()), None);
        }
    }

    #[test]
    fn the_block_is_a_few_bytes_a_field_over_the_fields_themselves() {
        let mut hash = PackedHash::new();
        for i in 0..100 {
            hash.set(&field(i), b"valuevaluevaluevaluevalue0000000");
        }
        let own = hash.data_len();
        assert_eq!(own, 100 * (14 + 32));
        // Capacity 128: 128 fingerprints, 257 offsets, and at most a
        // quarter of slack.
        assert!(hash.allocation_size() <= (own + 128 + 2 * 257) * 5 / 4 + 32);
    }
}
