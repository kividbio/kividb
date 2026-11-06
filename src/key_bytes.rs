//! A key as bytes, in twenty-four bytes where it is held: what a key that is
//! kept outside the keyspace is (`store::Key`), as the list of keys due to
//! expire, the keys a snapshot gives a time to live, eviction's choice.
//!
//! Redis keys are any bytes. A `CompactString` (what a kept key was) holds
//! only UTF-8, and it cannot be talked into holding anything else: it keeps
//! whether its bytes are inline, and how many, in the last byte of the
//! inline ones, in values no UTF-8 string ends in (0xC0 and up). Handed a
//! 24-byte key that ends in 0xD8, it would take the key's own bytes for a
//! pointer to free.
//!
//! [`KeyBytes`] does the same with values that any bytes can be told apart
//! from. The last of the twenty-four bytes is
//!
//! * below [`HEAP`] (0xE7): the twenty-fourth byte of a key of exactly
//!   twenty-four bytes, all of them inline;
//! * [`HEAP`]: the key is on the heap, a block of its own, and the first
//!   two words say where and how long;
//! * [`LEN0`] + n (0xE8 to 0xFF): n bytes inline, n from 0 to 23.
//!
//! So a key of up to twenty-three bytes is always inline, and one of
//! twenty-four is too unless its last byte is 0xE7 or more. No UTF-8 string
//! ends in a byte above 0xBF: every key a `CompactString` held inline, this
//! holds inline, in the same twenty-four bytes.

use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// How many bytes are held where the key is.
const SIZE: usize = 24;
/// The last byte of a key on the heap.
const HEAP: u8 = 0xE7;
/// The last byte of an inline key of no bytes; `LEN0 + n` of one of `n`.
const LEN0: u8 = 0xE8;
/// The longest key always held inline: a twenty-fourth byte is the key's own
/// only when it is below [`HEAP`].
const INLINE: usize = SIZE - 1;
const _: () = assert!(LEN0 as usize + INLINE == u8::MAX as usize);

/// Where a key on the heap is, laid over the inline bytes: the pointer and
/// the length in the first two words, the tag in the last byte.
#[derive(Clone, Copy)]
#[repr(C)]
struct Heap {
    ptr: *mut u8,
    len: usize,
    _pad: [u8; SIZE - 2 * std::mem::size_of::<usize>() - 1],
    tag: u8,
}

#[repr(C)]
union Repr {
    inline: [u8; SIZE],
    heap: Heap,
}

const _: () = assert!(std::mem::size_of::<Repr>() == SIZE);

pub struct KeyBytes(Repr);

// The heap block is the key's own, never shared and never written once made.
unsafe impl Send for KeyBytes {}
unsafe impl Sync for KeyBytes {}

impl KeyBytes {
    /// A copy of `bytes`.
    #[inline]
    pub fn new(bytes: &[u8]) -> Self {
        let len = bytes.len();
        if len <= INLINE {
            let mut inline = [0u8; SIZE];
            inline[..len].copy_from_slice(bytes);
            inline[SIZE - 1] = LEN0 + len as u8;
            KeyBytes(Repr { inline })
        } else if len == SIZE && bytes[SIZE - 1] < HEAP {
            let mut inline = [0u8; SIZE];
            inline.copy_from_slice(bytes);
            KeyBytes(Repr { inline })
        } else {
            Self::on_heap(bytes)
        }
    }

    #[cold]
    fn on_heap(bytes: &[u8]) -> Self {
        let boxed: Box<[u8]> = bytes.into();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed).cast::<u8>();
        KeyBytes(Repr {
            heap: Heap {
                ptr,
                len,
                _pad: [0; SIZE - 2 * std::mem::size_of::<usize>() - 1],
                tag: HEAP,
            },
        })
    }

    #[inline(always)]
    fn tag(&self) -> u8 {
        // SAFETY: the last byte is written whichever way the key is held:
        // as the inline bytes' last, or as `Heap::tag`.
        unsafe { self.0.inline[SIZE - 1] }
    }

    /// The key's bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let tag = self.tag();
        if tag < HEAP {
            // SAFETY: a last byte below `HEAP` is only ever written inline.
            unsafe { &self.0.inline }
        } else if tag != HEAP {
            // SAFETY: as above; `tag - LEN0` is at most `INLINE`.
            unsafe { &self.0.inline[..(tag - LEN0) as usize] }
        } else {
            // SAFETY: `HEAP` is only written with `heap`, whose block is
            // this key's own until it is dropped.
            unsafe {
                let heap = self.0.heap;
                std::slice::from_raw_parts(heap.ptr, heap.len)
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Whether the key is held in its own twenty-four bytes, not on the heap.
    #[inline]
    pub fn is_inline(&self) -> bool {
        self.tag() != HEAP
    }

    /// The key as text, if it is UTF-8.
    #[inline]
    pub fn to_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_bytes())
    }
}

impl Drop for KeyBytes {
    #[inline]
    fn drop(&mut self) {
        if self.tag() == HEAP {
            // SAFETY: made by `on_heap` from a `Box<[u8]>` of `len` bytes,
            // and dropped once.
            unsafe {
                let heap = self.0.heap;
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    heap.ptr, heap.len,
                )));
            }
        }
    }
}

impl Clone for KeyBytes {
    #[inline]
    fn clone(&self) -> Self {
        if self.tag() == HEAP {
            Self::on_heap(self.as_bytes())
        } else {
            // SAFETY: inline, every byte written.
            KeyBytes(Repr {
                inline: unsafe { self.0.inline },
            })
        }
    }
}

impl Default for KeyBytes {
    #[inline]
    fn default() -> Self {
        Self::new(b"")
    }
}

impl Deref for KeyBytes {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for KeyBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for KeyBytes {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// The bytes' hash, as `[u8]`'s: a table of keys can be asked for a key by
/// its bytes (`Borrow<[u8]>`).
impl Hash for KeyBytes {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state)
    }
}

impl PartialEq for KeyBytes {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for KeyBytes {}

impl PartialEq<[u8]> for KeyBytes {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

impl PartialEq<&[u8]> for KeyBytes {
    #[inline]
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_bytes() == *other
    }
}

impl PartialEq<str> for KeyBytes {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<&str> for KeyBytes {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

/// Byte order, as Redis compares keys (`memcmp`).
impl Ord for KeyBytes {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl PartialOrd for KeyBytes {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<&[u8]> for KeyBytes {
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes)
    }
}

impl<const N: usize> From<&[u8; N]> for KeyBytes {
    #[inline]
    fn from(bytes: &[u8; N]) -> Self {
        Self::new(bytes)
    }
}

impl From<&str> for KeyBytes {
    #[inline]
    fn from(text: &str) -> Self {
        Self::new(text.as_bytes())
    }
}

impl From<&String> for KeyBytes {
    #[inline]
    fn from(text: &String) -> Self {
        Self::new(text.as_bytes())
    }
}

impl From<String> for KeyBytes {
    #[inline]
    fn from(text: String) -> Self {
        Self::new(text.as_bytes())
    }
}

impl From<&KeyBytes> for KeyBytes {
    #[inline]
    fn from(key: &KeyBytes) -> Self {
        key.clone()
    }
}

impl From<Vec<u8>> for KeyBytes {
    #[inline]
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(&bytes)
    }
}

/// As `redis-cli` quotes a reply: printable ASCII as it is, everything
/// else escaped, so that a key that is not text is shown as what it is
/// rather than with U+FFFD in it. There is no `Display`: a key is not
/// turned into text to be used as a key again.
impl fmt::Debug for KeyBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&quoted(self.as_bytes()))
    }
}

/// `bytes` quoted as `redis-cli` quotes a bulk reply: `"key:1"`,
/// `"\xff\x00k"`, `"a\"b\n"`. For messages and logs that name a key.
pub fn quoted(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() + 2);
    out.push('"');
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x07 => out.push_str("\\a"),
            0x08 => out.push_str("\\b"),
            0x20..=0x7e => out.push(b as char),
            _ => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.push_str("\\x");
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 15) as usize] as char);
            }
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Every length that matters, each ending in every last byte that does.
    fn cases() -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for len in [
            0usize, 1, 2, 15, 16, 22, 23, 24, 25, 31, 32, 64, 255, 256, 1000,
        ] {
            for last in [
                0x00u8, 0x41, 0x7f, 0x80, 0xbf, 0xc0, 0xd8, 0xe6, 0xe7, 0xe8, 0xfe, 0xff,
            ] {
                let mut key: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
                if let Some(end) = key.last_mut() {
                    *end = last;
                }
                out.push(key);
            }
        }
        out.push((0u8..=255).collect());
        out
    }

    #[test]
    fn holds_any_bytes() {
        for key in cases() {
            let held = KeyBytes::new(&key);
            assert_eq!(held.as_bytes(), &key[..], "{key:?}");
            assert_eq!(held.len(), key.len());
            assert_eq!(held.is_empty(), key.is_empty());
            let copy = held.clone();
            assert_eq!(copy.as_bytes(), &key[..]);
            assert_eq!(copy.is_inline(), held.is_inline());
            drop(held);
            assert_eq!(copy.as_bytes(), &key[..], "a clone outlives its original");
        }
    }

    #[test]
    fn inline_up_to_23_and_24_unless_the_last_byte_says_otherwise() {
        for len in 0..=23 {
            for last in [0x00u8, 0xe7, 0xe8, 0xff] {
                let mut key = vec![b'k'; len];
                if let Some(end) = key.last_mut() {
                    *end = last;
                }
                assert!(
                    KeyBytes::new(&key).is_inline(),
                    "{len} bytes, last {last:#x}"
                );
            }
        }
        for last in 0..=255u8 {
            let mut key = [b'k'; 24];
            key[23] = last;
            let held = KeyBytes::new(&key);
            assert_eq!(held.is_inline(), last < 0xe7, "24 bytes, last {last:#x}");
            assert_eq!(held.as_bytes(), &key[..]);
        }
        for len in [25, 26, 100, 4096] {
            assert!(!KeyBytes::new(&vec![b'k'; len]).is_inline(), "{len} bytes");
        }
    }

    /// Every key a `CompactString` held inline is inline here too: no UTF-8
    /// string ends in a byte above 0xBF.
    #[test]
    fn every_utf8_key_of_24_bytes_is_inline() {
        for text in [
            "abcdefghijklmnopqrstuvwx",
            "user:1234567890123456789",
            "caf\u{e9}caf\u{e9}caf\u{e9}caf\u{e9}abcd",
            "\u{1F600}\u{1F600}\u{1F600}\u{1F600}\u{1F600}\u{1F600}",
            "\u{F7FF}\u{F7FF}\u{F7FF}\u{F7FF}\u{F7FF}\u{F7FF}\u{F7FF}\u{F7FF}",
        ] {
            assert_eq!(text.len(), 24, "{text}");
            let held = KeyBytes::from(text);
            assert!(held.is_inline(), "{text}");
            assert_eq!(held.to_str(), Ok(text));
        }
    }

    #[test]
    fn utf8_both_ways() {
        let held = KeyBytes::from("key:1");
        assert_eq!(held.to_str(), Ok("key:1"));
        assert_eq!(held, "key:1");
        assert_eq!(held, *"key:1");
        assert_eq!(KeyBytes::from(String::from("key:1")), held);
        assert_eq!(KeyBytes::from(&String::from("key:1")), held);
        assert!(KeyBytes::from(&b"\xff\x00k"[..]).to_str().is_err());
        let long = "k".repeat(40);
        assert_eq!(KeyBytes::from(long.as_str()).to_str(), Ok(long.as_str()));
    }

    #[test]
    fn eq_ord_and_hash_are_the_bytes() {
        let keys = cases();
        let state = ahash::RandomState::new();
        for a in &keys {
            let ka = KeyBytes::new(a);
            assert_eq!(state.hash_one(&ka), state.hash_one(&a[..]), "{a:?}");
            assert_eq!(ka, a[..]);
            assert_eq!(ka, &a[..]);
            for b in keys.iter().take(40) {
                let kb = KeyBytes::new(b);
                assert_eq!(ka == kb, a == b);
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{a:?} {b:?}");
                assert_eq!(ka.partial_cmp(&kb), Some(a.cmp(b)));
            }
        }
    }

    #[test]
    fn found_in_a_table_by_its_bytes() {
        let mut keys = cases();
        keys.sort();
        keys.dedup();
        let mut table: HashMap<KeyBytes, usize, ahash::RandomState> = HashMap::default();
        for (i, key) in keys.iter().enumerate() {
            assert!(table.insert(KeyBytes::new(key), i).is_none());
        }
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(table.get(&key[..]), Some(&i), "{key:?}");
        }
        assert_eq!(table.get(&b"not there"[..]), None);
    }

    #[test]
    fn default_is_empty_and_inline() {
        let empty = KeyBytes::default();
        assert!(empty.is_empty() && empty.is_inline());
        assert_eq!(empty, KeyBytes::from(""));
    }

    #[test]
    fn quoted_as_redis_cli_quotes() {
        assert_eq!(quoted(b"key:1"), "\"key:1\"");
        assert_eq!(quoted(b"\xff\x00k"), "\"\\xff\\x00k\"");
        assert_eq!(quoted(b"a\"b\\c\n\r\t"), "\"a\\\"b\\\\c\\n\\r\\t\"");
        assert_eq!(format!("{:?}", KeyBytes::from(&b"\x7f"[..])), "\"\\x7f\"");
    }

    #[test]
    fn is_the_size_of_a_compact_string() {
        assert_eq!(std::mem::size_of::<KeyBytes>(), 24);
    }
}
