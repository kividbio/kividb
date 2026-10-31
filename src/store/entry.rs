//! A key and its value in one block: what a bucket of a [`super::ShardMap`]
//! points to.
//!
//! As a `(Key, KiviDBValue)` in the table, every bucket was forty bytes, and
//! a table sits between half and seven eighths full: a key of a 100-byte
//! string cost the table about 86 bytes before its value's own block. Here a
//! bucket is the eight bytes of a [`Slot`], and the key, a header and (for a
//! string that fits) the value's bytes share one block, from the slabs when
//! it is small enough (`value_slab`, up to 4 KiB).
//!
//! **Layout.**
//!
//! ```text
//! 0   u32  holders (low 24 bits) | form (top 8)
//! 4   u32  access word: when last used, or how often (`eviction::Access`)
//! 8   u8   key length, or LONG_KEY: the length is a u32 after the header
//! 9   u8   flags: HAS_TTL
//! 10  u16  length of an inline string
//! 12  [u32 position, u64 expiry time]   only with a time to live
//!     [u32 key length]   only for a long key
//!     key bytes
//!     an inline string's bytes, or, 8-aligned, a KiviDBValue kept whole
//! ```
//!
//! A string short enough is kept inline: one block for key and value, and
//! nothing else to visit. Anything else (a collection, or a string too long
//! to be inline) is its `KiviDBValue`, kept whole in the block: its handle
//! is borrowed in place, so a collection is reached as before.
//!
//! **Time to live.** A key that has one keeps it in its entry: the time it
//! expires at, in milliseconds since the Unix epoch, and where the entry is
//! in its shard's list of the keys that have one (which is how expiry and
//! the `volatile-*` policies find them). A key without one pays nothing for
//! it. A read finds the time in the block it reads anyway.
//!
//! **Holders.** The table holds each entry once. A frozen snapshot, and
//! whatever else must read a key and its value once the shard is unlocked,
//! takes another hold ([`EntryRef`]); the block goes when the last hold
//! does. What a holder can read of a block (the key, the value, the time to
//! live) is never written while anybody but the table holds it: a time to
//! live, or an inline string, is changed in place only when the table's is
//! the only hold, and otherwise in a new entry. The position is the table's
//! own, written only with the shard locked for writing, and read by nothing
//! else. Compaction moves a block whole, and only when the table's is the
//! only hold.
//!
//! **Writes in place.** An inline string can be written over, grown or
//! shrunk in its own block when the new length keeps the block's size in
//! the same slab class: the block is freed by its size, which says its
//! class, so a length that would say another one needs a new block. The
//! slot's size hint does not change either: a class never spans two
//! counts of cache lines (its sizes are within one 16-, 32- or 64-byte
//! step, and lines are 64 bytes).
//!
//! **The access word is the exception.** When a key was last used, or how
//! often (Redis's `robj.lru`), is written in place with the shard locked
//! only for reading, and whoever holds the entry: it is an atomic word,
//! written with a relaxed store, and only when it changes. It is no part of
//! the key or the value, and no holder reads it: a frozen snapshot does not
//! save it. Two threads using one key at once may each write it, and one
//! write is then lost: a use not counted, which the policies, choosing from
//! a sample, can afford. A new entry for the same key (a write, a time to
//! live given or taken) starts from the old entry's word.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering, fence};

use super::shard_map::{StrRef, ValueRef};
use crate::shared_bytes::SharedBytes;
use crate::types::KiviDBValue;
use crate::value_slab;

#[repr(C)]
struct Header {
    word: AtomicU32,
    lru: AtomicU32,
    klen: u8,
    flags: u8,
    vlen: u16,
}

const HEADER: usize = std::mem::size_of::<Header>();
const _: () = assert!(HEADER == 12);

const FORM_SHIFT: u32 = 24;
const HOLDERS: u32 = (1 << FORM_SHIFT) - 1;
/// More holds than this and the process stops, as `Arc` does long before
/// its count could wrap into the form.
const MAX_HOLDERS: u32 = 1 << 23;

/// A string whose bytes follow the key.
const INLINE: u32 = 1;
/// A `KiviDBValue` kept whole after the key.
const WHOLE: u32 = 2;

/// The key has a time to live: its position and expiry time follow the
/// header.
const HAS_TTL: u8 = 1;
/// What a time to live takes in an entry: the position, then the time.
const TTL_ROOM: usize = 12;
/// Where the position in the shard's list of keys with a time to live is.
const POS_AT: usize = HEADER;
/// Where the expiry time is: 8-aligned, as blocks are 16-aligned.
const EXPIRES_AT: usize = HEADER + 4;
const _: () = assert!(EXPIRES_AT.is_multiple_of(8) && POS_AT + 4 == EXPIRES_AT);
const _: () = assert!(EXPIRES_AT + 8 == HEADER + TTL_ROOM);

/// `klen` of a key of 255 bytes or more: its length is the u32 after the
/// header (and after the time to live, if there is one).
const LONG_KEY: u8 = u8::MAX;

/// Blocks are aligned to this; slab blocks are, and the system allocator's
/// are asked to be.
const ALIGN: usize = 16;

/// The longest entry a string's bytes are kept inline in: the largest block
/// a slab holds.
pub const INLINE_MAX: usize = value_slab::MAX_BLOCK;

/// Where a long key's length is, in an entry with (`ttl`) or without a time
/// to live.
#[inline(always)]
const fn long_len_at(ttl: bool) -> usize {
    HEADER + TTL_ROOM * ttl as usize
}

/// Where the key starts, for a key of `klen` bytes. Without a branch: it
/// is asked for on every lookup.
#[inline(always)]
const fn key_at(klen: usize, ttl: bool) -> usize {
    long_len_at(ttl) + 4 * (klen >= LONG_KEY as usize) as usize
}

/// Where a whole `KiviDBValue` goes, after a key of `klen` bytes.
#[inline]
const fn whole_at(klen: usize, ttl: bool) -> usize {
    (key_at(klen, ttl) + klen).next_multiple_of(8)
}

#[inline]
fn inline_size(klen: usize, vlen: usize, ttl: bool) -> usize {
    key_at(klen, ttl) + klen + vlen
}

#[inline]
fn whole_size(klen: usize, ttl: bool) -> usize {
    whole_at(klen, ttl) + std::mem::size_of::<KiviDBValue>()
}

/// Whether a string of `vlen` bytes under a key of `klen` is kept inline.
#[inline]
fn fits_inline(klen: usize, vlen: usize, ttl: bool) -> bool {
    vlen <= u16::MAX as usize && inline_size(klen, vlen, ttl) <= INLINE_MAX
}

fn allocate(size: usize) -> NonNull<u8> {
    match value_slab::class_of(size) {
        Some(class) => value_slab::alloc(class),
        None => {
            let layout = Layout::from_size_align(size, ALIGN).expect("an entry's layout");
            // SAFETY: the size is never zero: there is a header.
            match NonNull::new(unsafe { std::alloc::alloc(layout) }) {
                Some(at) => at,
                None => std::alloc::handle_alloc_error(layout),
            }
        }
    }
}

/// # Safety
/// `at` came from [`allocate`] of `size`, and is not used again.
unsafe fn deallocate(at: NonNull<u8>, size: usize) {
    match value_slab::class_of(size) {
        // SAFETY: as the caller promises.
        Some(class) => unsafe { value_slab::free(at, class) },
        None => unsafe {
            std::alloc::dealloc(
                at.as_ptr(),
                Layout::from_size_align(size, ALIGN).expect("an entry's layout"),
            )
        },
    }
}

/// An entry's address: what a slot holds. Copying it takes no hold; whoever
/// uses one has to know the entry lives (a shard lock, or a hold).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Entry(NonNull<Header>);

// SAFETY: an entry is shared only as described above, and what it holds
// (bytes, `KiviDBValue`) is `Send` and `Sync`.
unsafe impl Send for Entry {}
unsafe impl Sync for Entry {}

impl Entry {
    /// A new entry, with one hold: the caller's (the table's, once in it).
    /// `expires` is its time to live, as the time it ends at (milliseconds
    /// since the Unix epoch), if it has one.
    pub(super) fn new(key: &[u8], value: KiviDBValue, expires: Option<u64>) -> Entry {
        if let KiviDBValue::String(bytes) = &value
            && fits_inline(key.len(), bytes.len(), expires.is_some())
        {
            return Self::inline(key, bytes, expires);
        }
        let klen = key.len();
        let ttl = expires.is_some();
        let at = Self::begin(key, whole_size(klen, ttl), WHOLE, 0, expires);
        // SAFETY: the block has room for the value at `whole_at`, aligned
        // for it, and nothing else has the block yet.
        unsafe {
            (at.as_ptr() as *mut u8)
                .add(whole_at(klen, ttl))
                .cast::<KiviDBValue>()
                .write(value);
        }
        Entry(at)
    }

    /// A new entry for the string `bytes`: inline if it fits, else in a
    /// `SharedBytes` of its own.
    pub(super) fn string(key: &[u8], bytes: &[u8], expires: Option<u64>) -> Entry {
        if fits_inline(key.len(), bytes.len(), expires.is_some()) {
            Self::inline(key, bytes, expires)
        } else {
            Self::new(key, KiviDBValue::String(SharedBytes::from(bytes)), expires)
        }
    }

    fn inline(key: &[u8], bytes: &[u8], expires: Option<u64>) -> Entry {
        let klen = key.len();
        let ttl = expires.is_some();
        let at = Self::begin(
            key,
            inline_size(klen, bytes.len(), ttl),
            INLINE,
            bytes.len(),
            expires,
        );
        // SAFETY: the block has room for the bytes after the key.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                (at.as_ptr() as *mut u8).add(key_at(klen, ttl) + klen),
                bytes.len(),
            );
        }
        Entry(at)
    }

    /// A block of `size`, its header, time to live and key written.
    fn begin(
        key: &[u8],
        size: usize,
        form: u32,
        vlen: usize,
        expires: Option<u64>,
    ) -> NonNull<Header> {
        let klen = key.len();
        assert!(klen <= u32::MAX as usize, "a key of {klen} bytes");
        let ttl = expires.is_some();
        let at = allocate(size).cast::<Header>();
        // SAFETY: `size` covers the header, the time to live, the long key's
        // length and the key, and the block is 16-aligned and nobody else's.
        unsafe {
            at.as_ptr().write(Header {
                word: AtomicU32::new(1 | form << FORM_SHIFT),
                lru: AtomicU32::new(0),
                klen: klen.min(LONG_KEY as usize) as u8,
                flags: if ttl { HAS_TTL } else { 0 },
                vlen: vlen as u16,
            });
            let base = at.as_ptr() as *mut u8;
            if let Some(ms) = expires {
                base.add(POS_AT)
                    .cast::<AtomicU32>()
                    .write(AtomicU32::new(0));
                base.add(EXPIRES_AT).cast::<u64>().write(ms);
            }
            if klen >= LONG_KEY as usize {
                base.add(long_len_at(ttl))
                    .cast::<u32>()
                    .write_unaligned(klen as u32);
            }
            std::ptr::copy_nonoverlapping(key.as_ptr(), base.add(key_at(klen, ttl)), klen);
        }
        at
    }

    #[inline]
    fn header(&self) -> &Header {
        // SAFETY: the entry lives (see the type).
        unsafe { self.0.as_ref() }
    }

    #[inline]
    pub(super) fn base(self) -> *const u8 {
        self.0.as_ptr() as *const u8
    }

    #[inline]
    fn form(&self) -> u32 {
        self.header().word.load(Ordering::Relaxed) >> FORM_SHIFT
    }

    /// Whether the key has a time to live.
    #[inline(always)]
    pub(super) fn has_ttl(self) -> bool {
        self.header().flags & HAS_TTL != 0
    }

    #[inline(always)]
    fn klen(&self) -> usize {
        let short = self.header().klen;
        if short == LONG_KEY {
            // SAFETY: a long key's length follows the header and the time
            // to live.
            unsafe {
                self.base()
                    .add(long_len_at(self.has_ttl()))
                    .cast::<u32>()
                    .read_unaligned() as usize
            }
        } else {
            short as usize
        }
    }

    /// Whether the entry's key is `key`. A key of 8 to 16 bytes (most keys
    /// are) is compared as two words, without a call to `memcmp`.
    #[inline(always)]
    pub(super) fn has_key(self, key: &[u8]) -> bool {
        let mine = self.key();
        let n = key.len();
        if mine.len() != n {
            return false;
        }
        if (8..=16).contains(&n) {
            let word = |s: &[u8], at: usize| u64::from_ne_bytes(s[at..at + 8].try_into().unwrap());
            return word(mine, 0) == word(key, 0) && word(mine, n - 8) == word(key, n - 8);
        }
        mine == key
    }

    /// The key's bytes, for as long as the entry lives.
    #[inline(always)]
    pub(super) fn key<'a>(self) -> &'a [u8] {
        let klen = self.klen();
        // SAFETY: `klen` bytes of key at `key_at`, never written again.
        unsafe { std::slice::from_raw_parts(self.base().add(key_at(klen, self.has_ttl())), klen) }
    }

    /// The key, as the `&str` it was made from.
    #[inline]
    pub(super) fn key_str<'a>(self) -> &'a str {
        // SAFETY: every key is made from a `&str` (`ShardMap::insert` and
        // friends take one).
        unsafe { std::str::from_utf8_unchecked(self.key()) }
    }

    /// The value, for as long as the entry lives.
    #[inline(always)]
    pub(super) fn value<'a>(self) -> ValueRef<'a> {
        let klen = self.klen();
        if self.form() == INLINE {
            let vlen = self.header().vlen as usize;
            // SAFETY: `vlen` bytes follow the key, never written again.
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    self.base().add(key_at(klen, self.has_ttl()) + klen),
                    vlen,
                )
            };
            ValueRef::String(StrRef::bytes(bytes))
        } else {
            ValueRef::from(self.whole())
        }
    }

    /// The `KiviDBValue` of a [`WHOLE`] entry.
    #[inline]
    fn whole<'a>(self) -> &'a KiviDBValue {
        debug_assert_eq!(self.form(), WHOLE);
        // SAFETY: a whole entry's value is at `whole_at`, aligned, and
        // lives as long as the entry.
        unsafe {
            &*self
                .base()
                .add(whole_at(self.klen(), self.has_ttl()))
                .cast::<KiviDBValue>()
        }
    }

    /// When the key's time to live ends, in milliseconds since the Unix
    /// epoch, if it has one.
    #[inline(always)]
    pub(super) fn expires(self) -> Option<u64> {
        if self.has_ttl() {
            // SAFETY: an entry with a time to live has its time at
            // `EXPIRES_AT`, aligned; it changes only as `set_expires` says.
            Some(unsafe { self.base().add(EXPIRES_AT).cast::<u64>().read() })
        } else {
            None
        }
    }

    /// Change the time to live the key has, in place.
    ///
    /// # Safety
    /// The entry has a time to live, the caller holds the shard for writing,
    /// and the table's is the only hold: nothing else reads the time.
    pub(super) unsafe fn set_expires(self, ms: u64) {
        debug_assert!(self.has_ttl() && self.holders() == 1);
        // SAFETY: as the caller promises.
        unsafe { (self.base().add(EXPIRES_AT) as *mut u64).write(ms) };
    }

    #[inline]
    fn pos_cell(&self) -> &AtomicU32 {
        debug_assert!(self.has_ttl());
        // SAFETY: an entry with a time to live has its position at
        // `POS_AT`, 4-aligned, made in `begin`.
        unsafe { &*self.base().add(POS_AT).cast::<AtomicU32>() }
    }

    /// Where the entry is in its shard's list of keys with a time to live.
    #[inline]
    pub(super) fn pos(self) -> usize {
        self.pos_cell().load(Ordering::Relaxed) as usize
    }

    /// Say where the entry is in its shard's list. The caller holds the
    /// shard for writing; the entry has a time to live.
    #[inline]
    pub(super) fn set_pos(self, pos: usize) {
        self.pos_cell().store(pos as u32, Ordering::Relaxed);
    }

    /// A new entry with the same key and value, and `expires` for its time
    /// to live: what changes whether a key has one, or changes it when the
    /// entry is held. One hold, the caller's; this entry is left as it was,
    /// and the new one has its access word.
    pub(super) fn remade(self, expires: Option<u64>) -> Entry {
        let new = match self.value() {
            ValueRef::String(bytes) if bytes.shared().is_none() => {
                Self::string(self.key(), bytes.as_bytes(), expires)
            }
            // Another handle on the same value: a collection is not copied.
            other => Self::new(self.key(), other.to_value(), expires),
        };
        new.header().lru.store(self.access(), Ordering::Relaxed);
        new
    }

    /// The access word: when the key was last used, or how often
    /// (`eviction::Access`). Zero in a new entry.
    #[inline(always)]
    pub(super) fn access(self) -> u32 {
        self.header().lru.load(Ordering::Relaxed)
    }

    /// Give the entry the access word `word`. The entry must live (the
    /// shard is locked, for reading is enough): see the module's note on
    /// the access word.
    #[inline(always)]
    pub(super) fn set_access(self, word: u32) {
        self.header().lru.store(word, Ordering::Relaxed);
    }

    /// The access word made over by `next`, and written only if that
    /// changes it: a key read many times in the same second leaves its line
    /// as it was, and does not take it from the other cores reading it.
    #[inline(always)]
    pub(super) fn touch(self, next: impl FnOnce(u32) -> u32) {
        let cell = &self.header().lru;
        let word = cell.load(Ordering::Relaxed);
        let new = next(word);
        if new != word {
            cell.store(new, Ordering::Relaxed);
        }
    }

    /// The length of the string, if it is kept inline.
    #[inline]
    pub(super) fn inline_len(self) -> Option<usize> {
        (self.form() == INLINE).then(|| self.header().vlen as usize)
    }

    /// Whether an inline string of `vlen` bytes can take this entry's value's
    /// place in its own block: the value is an inline string, the table's is
    /// the only hold, and the block's size stays in its slab class (see the
    /// module). Whether the key has a time to live stays as it is.
    #[inline]
    pub(super) fn fits_in_place(self, vlen: usize) -> bool {
        if self.form() != INLINE || self.holders() != 1 {
            return false;
        }
        let klen = self.klen();
        let ttl = self.has_ttl();
        let now = inline_size(klen, self.header().vlen as usize, ttl);
        // An inline entry is in a slab: its class is never `None`.
        value_slab::class_of(inline_size(klen, vlen, ttl)) == value_slab::class_of(now)
    }

    /// Where an inline string's bytes start.
    #[inline]
    fn inline_at(self) -> usize {
        key_at(self.klen(), self.has_ttl()) + self.klen()
    }

    /// The inline string made `bytes`, in this block.
    ///
    /// # Safety
    /// The caller holds the shard for writing, and `fits_in_place(bytes.len())`.
    pub(super) unsafe fn write_in_place(self, bytes: &[u8]) {
        debug_assert!(self.fits_in_place(bytes.len()));
        // SAFETY: nothing but the table holds the entry and the shard is
        // locked for writing: nothing reads the bytes or the length while
        // they change, and the block has room up to its class's size.
        unsafe {
            let base = self.0.as_ptr() as *mut u8;
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), base.add(self.inline_at()), bytes.len());
            self.set_vlen(bytes.len());
        }
    }

    /// `patch` written over the inline string from `offset`, which may be
    /// past its end: the gap is filled with zero bytes. The string grows to
    /// reach the patch's end, and never shrinks.
    ///
    /// # Safety
    /// As [`Entry::write_in_place`], for the string's length after it:
    /// `fits_in_place(max(len, offset + patch.len()))`.
    pub(super) unsafe fn write_at_in_place(self, offset: usize, patch: &[u8]) {
        let vlen = self.header().vlen as usize;
        let len = vlen.max(offset + patch.len());
        debug_assert!(self.fits_in_place(len));
        // SAFETY: as in `write_in_place`.
        unsafe {
            let at = (self.0.as_ptr() as *mut u8).add(self.inline_at());
            if offset > vlen {
                std::ptr::write_bytes(at.add(vlen), 0, offset - vlen);
            }
            std::ptr::copy_nonoverlapping(patch.as_ptr(), at.add(offset), patch.len());
            self.set_vlen(len);
        }
    }

    /// # Safety
    /// As [`Entry::write_in_place`]: nobody else reads the header.
    #[inline]
    unsafe fn set_vlen(self, vlen: usize) {
        debug_assert!(vlen <= u16::MAX as usize);
        // SAFETY: as the caller promises; written through the block's own
        // pointer, with no reference to the header alive.
        unsafe { std::ptr::addr_of_mut!((*self.0.as_ptr()).vlen).write(vlen as u16) };
    }

    /// The block's size.
    #[inline]
    pub(super) fn size(self) -> usize {
        let klen = self.klen();
        let ttl = self.has_ttl();
        if self.form() == INLINE {
            inline_size(klen, self.header().vlen as usize, ttl)
        } else {
            whole_size(klen, ttl)
        }
    }

    #[inline]
    pub(super) fn holders(self) -> u32 {
        self.header().word.load(Ordering::Acquire) & HOLDERS
    }

    /// One more hold. The entry must live: the caller holds the shard, or a
    /// hold of its own.
    #[inline]
    pub(super) fn retain(self) {
        let before = self.header().word.fetch_add(1, Ordering::Relaxed) & HOLDERS;
        if before >= MAX_HOLDERS {
            std::process::abort();
        }
    }

    /// Let go of one hold; the last frees the entry and its value.
    ///
    /// # Safety
    /// The caller has a hold, and does not use the entry again.
    pub(super) unsafe fn release(self) {
        if self.header().word.fetch_sub(1, Ordering::Release) & HOLDERS != 1 {
            return;
        }
        // What the other holders did with the entry comes before it goes.
        fence(Ordering::Acquire);
        let size = self.size();
        if self.form() == WHOLE {
            // SAFETY: the last hold: nothing reads the value any more.
            unsafe {
                std::ptr::drop_in_place(
                    self.base().add(whole_at(self.klen(), self.has_ttl())) as *mut KiviDBValue
                )
            };
        }
        // SAFETY: the block came from `allocate(size)`.
        unsafe { deallocate(self.0.cast(), size) };
    }

    /// The value, owned, and the caller's hold given up. A string kept
    /// inline is copied out; anything else is another handle on it.
    ///
    /// # Safety
    /// The caller has a hold, and does not use the entry again.
    pub(super) unsafe fn into_value(self) -> KiviDBValue {
        let value = self.value().to_value();
        // SAFETY: as the caller promises.
        unsafe { self.release() };
        value
    }

    /// Move the entry, or its string's bytes, out of a slab compaction is
    /// emptying (`regions` is `value_slab::regions_snapshot`), if the table's
    /// is its only hold. Returns the new entry to put in its slot, if the
    /// entry moved, and what was left behind, to free with the shard
    /// unlocked.
    ///
    /// # Safety
    /// The caller holds the shard for writing, and the table holds the entry.
    pub(super) unsafe fn relocate(self, regions: &[usize]) -> Relocation {
        let size = self.size();
        let entry_moves =
            value_slab::class_of(size).is_some() && value_slab::is_evacuating(self.base(), regions);
        let bytes_move = self.form() == WHOLE
            && matches!(self.whole(), KiviDBValue::String(b)
                if value_slab::is_evacuating(b.block(), regions));
        if !entry_moves && !bytes_move {
            return Relocation::Stays;
        }
        if self.holders() != 1 {
            return Relocation::Held;
        }
        if entry_moves {
            let to = allocate(size);
            // SAFETY: the only hold is the table's, and the shard is held
            // for writing: nothing reads the block while it is copied, and
            // nothing but the slot (and the shard's list of keys with a time
            // to live, which the caller fixes) knows where it is. The copy
            // is the value's move; the old block is freed without dropping
            // it.
            unsafe { std::ptr::copy_nonoverlapping(self.base(), to.as_ptr(), size) };
            return Relocation::Moved {
                to: Entry(to.cast()),
                left: Left(self.0.cast(), size),
            };
        }
        // SAFETY: as above, nothing else reads the value.
        let value = unsafe {
            &mut *(self.base().add(whole_at(self.klen(), self.has_ttl())) as *mut KiviDBValue)
        };
        match value {
            KiviDBValue::String(bytes) => match bytes.relocate(regions) {
                Some(old) => Relocation::BytesMoved(old),
                None => Relocation::Held,
            },
            _ => Relocation::Stays,
        }
    }
}

/// What [`Entry::relocate`] did.
pub(super) enum Relocation {
    Stays,
    Held,
    /// The entry moved to `to`; `left` is the old block.
    Moved {
        to: Entry,
        left: Left,
    },
    /// The entry stayed; its string's bytes moved, and this is the old block.
    BytesMoved(SharedBytes),
}

/// An entry's block left behind by compaction, freed when dropped.
pub struct Left(NonNull<u8>, usize);

// SAFETY: nothing but this knows the block.
unsafe impl Send for Left {}

impl Drop for Left {
    fn drop(&mut self) {
        // SAFETY: the block was an entry's, copied elsewhere; its value
        // moved with the copy.
        unsafe { deallocate(self.0, self.1) };
    }
}

/// A hold on an entry: its key, value and time to live can be read with no
/// shard locked, and none of them changes while it is held. What a frozen
/// snapshot keeps of a string.
pub struct EntryRef(Entry);

impl EntryRef {
    /// # Safety
    /// The entry lives: the caller holds its shard.
    pub(super) unsafe fn new(entry: Entry) -> Self {
        entry.retain();
        EntryRef(entry)
    }

    pub fn key(&self) -> &str {
        self.0.key_str()
    }

    pub fn value(&self) -> ValueRef<'_> {
        self.0.value()
    }

    /// When the key's time to live ended, as it was when this was taken:
    /// milliseconds since the Unix epoch.
    pub fn expires(&self) -> Option<u64> {
        self.0.expires()
    }

    /// Whether `entry` is this one.
    pub(super) fn is(&self, entry: Entry) -> bool {
        self.0 == entry
    }
}

impl Clone for EntryRef {
    fn clone(&self) -> Self {
        self.0.retain();
        EntryRef(self.0)
    }
}

impl Drop for EntryRef {
    fn drop(&mut self) {
        // SAFETY: this is a hold.
        unsafe { self.0.release() };
    }
}

/// An entry the table has let go of: a key's value as it was replaced or
/// removed. Its value can be read, or taken; it is freed when dropped (with
/// the shard unlocked, ideally: freeing is the allocator's time).
pub struct Taken(Entry);

impl Taken {
    /// # Safety
    /// The caller gives this the table's hold.
    pub(super) unsafe fn new(entry: Entry) -> Self {
        Taken(entry)
    }

    pub fn key(&self) -> &str {
        self.0.key_str()
    }

    pub fn value(&self) -> ValueRef<'_> {
        self.0.value()
    }

    /// When the key's time to live ended, if it had one.
    pub fn expires(&self) -> Option<u64> {
        self.0.expires()
    }

    /// The value, owned.
    pub fn into_value(self) -> KiviDBValue {
        let entry = self.0;
        std::mem::forget(self);
        // SAFETY: the hold was this one's.
        unsafe { entry.into_value() }
    }
}

impl Drop for Taken {
    fn drop(&mut self) {
        // SAFETY: the hold is this one's.
        unsafe { self.0.release() };
    }
}

/// What a bucket holds: the entry's address in the low 48 bits, and in the
/// top 16 a 12-bit fragment of the key's hash and a 4-bit hint of the
/// entry's size.
///
/// The fragment is compared before the entry is touched. hashbrown's 7-bit
/// tag lets one probed key in 128 through by chance, and each of those
/// was a visit to an entry somewhere in the whole dataset; with the fragment,
/// one in half a million is. When the fragment matches, the entry's first
/// lines are asked for at once, the hint saying how many: a large value's
/// copy otherwise waits for the header's line before the rest is asked for.
#[derive(Clone, Copy)]
pub(super) struct Slot(u64);

const ADDRESS_BITS: u32 = 48;
const ADDRESS: u64 = (1 << ADDRESS_BITS) - 1;
/// Cache lines for each size hint.
const HINT_LINES: [u8; 16] = [1, 1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32, 48, 64];
/// The most lines asked for ahead: past these the hardware's own prefetcher
/// has seen the stream.
pub(super) const PREFETCH_LINES: usize = 8;

impl Slot {
    #[inline]
    fn fragment(hash: u64) -> u64 {
        // Bits hashbrown uses for neither the bucket (the low bits) nor its
        // tag (the top seven).
        (hash >> 32) & 0xfff
    }

    pub(super) fn new(entry: Entry, hash: u64) -> Slot {
        // The address is exposed: the slot holds it as a number, and
        // `entry` turns it back into a pointer to the same block.
        let at = entry.0.as_ptr().expose_provenance() as u64;
        // Every address user space is given without asking for a high one
        // is below 2^47 (x86-64) or 2^48 (arm64), on Linux, Windows and
        // macOS alike.
        assert_eq!(at & !ADDRESS, 0, "an entry above 2^48");
        let lines = entry.size().div_ceil(64);
        let hint = HINT_LINES
            .iter()
            .position(|&l| l as usize >= lines)
            .unwrap_or(15) as u64;
        Slot(at | Self::fragment(hash) << 52 | hint << ADDRESS_BITS)
    }

    /// The same slot for the entry moved to `to` (by compaction): the
    /// fragment and the hint stay.
    #[inline]
    pub(super) fn moved_to(self, to: Entry) -> Slot {
        let at = to.0.as_ptr().expose_provenance() as u64;
        assert_eq!(at & !ADDRESS, 0, "an entry above 2^48");
        Slot(self.0 & !ADDRESS | at)
    }

    /// The fragment of this slot's key's hash, as a hash that has it: what
    /// a slot for another entry of the same key is made with.
    #[inline]
    pub(super) fn hash_part(self) -> u64 {
        (self.0 >> 52) << 32
    }

    #[inline(always)]
    pub(super) fn entry(self) -> Entry {
        // SAFETY: made from an entry's address, never zero.
        Entry(unsafe {
            NonNull::new_unchecked(std::ptr::with_exposed_provenance_mut::<Header>(
                (self.0 & ADDRESS) as usize,
            ))
        })
    }

    /// Whether this can be the entry of a key whose hash is `hash`; if it
    /// can, its first lines are asked for.
    #[inline(always)]
    pub(super) fn may_be(self, hash: u64) -> bool {
        if self.0 >> 52 != Self::fragment(hash) {
            return false;
        }
        let lines = HINT_LINES[((self.0 >> ADDRESS_BITS) & 0xf) as usize] as usize;
        prefetch((self.0 & ADDRESS) as *const u8, lines.min(PREFETCH_LINES));
        true
    }
}

impl Slot {
    /// Ask for the entry's first lines, as many as the hint says (up to
    /// [`PREFETCH_LINES`]): for a walk over buckets that will read them.
    #[inline(always)]
    pub(super) fn prefetch(self) {
        let lines = HINT_LINES[((self.0 >> ADDRESS_BITS) & 0xf) as usize] as usize;
        prefetch((self.0 & ADDRESS) as *const u8, lines.min(PREFETCH_LINES));
    }

    /// Ask for the entry's first line only.
    #[inline(always)]
    pub(super) fn prefetch_header(self) {
        prefetch((self.0 & ADDRESS) as *const u8, 1);
    }
}

/// Ask for `lines` cache lines from `at`. A hint: nothing is read.
#[inline(always)]
fn prefetch(at: *const u8, lines: usize) {
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    for line in 0..lines {
        // SAFETY: a prefetch reads nothing and faults on nothing.
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                at.wrapping_add(line * 64) as *const i8,
                std::arch::x86_64::_MM_HINT_T0,
            )
        };
    }
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    for line in 0..lines {
        // SAFETY: as above.
        unsafe {
            std::arch::asm!(
                "prfm pldl1keep, [{0}]",
                in(reg) at.wrapping_add(line * 64),
                options(nostack, readonly, preserves_flags)
            )
        };
    }
    #[cfg(any(miri, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
    let _ = (at, lines);
}

/// Ask for the line of an entry its time to live is in: what a pass over a
/// shard's keys with one reads of each.
#[inline(always)]
pub(super) fn prefetch_expiry(entry: Entry) {
    prefetch(entry.base().wrapping_add(EXPIRES_AT), 1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "above 2^48")]
    fn a_slot_refuses_an_address_past_48_bits() {
        // Refused before the entry is looked at: the address is never read.
        let far = Entry(NonNull::new(std::ptr::without_provenance_mut::<Header>(1 << 50)).unwrap());
        let _ = Slot::new(far, 0);
    }

    #[test]
    fn a_slot_keeps_its_entry_fragment_and_hint() {
        for (klen, vlen) in [(1, 0), (15, 100), (15, 1000), (300, 3000), (15, 70_000)] {
            for ttl in [None, Some(1_700_000_000_000)] {
                let key = vec![b'k'; klen];
                let entry = Entry::string(&key, &vec![7u8; vlen], ttl);
                let hash = 0xabc_u64 << 32 | 0x1234_5678;
                let slot = Slot::new(entry, hash);
                assert!(slot.entry() == entry);
                assert!(slot.may_be(hash));
                assert!(slot.may_be(slot.hash_part()));
                assert!(!slot.may_be(hash ^ 1 << 40));
                // The low bits are not the fragment's: hashbrown's index.
                assert!(slot.may_be(hash ^ 0xffff));
                let lines = HINT_LINES[((slot.0 >> ADDRESS_BITS) & 0xf) as usize] as usize;
                let need = entry.size().div_ceil(64);
                assert!(lines >= need.min(64), "{lines} lines for {need}");
                let moved = Entry::string(&key, &vec![7u8; vlen], ttl);
                let again = slot.moved_to(moved);
                assert!(again.entry() == moved);
                assert_eq!(again.0 >> ADDRESS_BITS, slot.0 >> ADDRESS_BITS);
                // SAFETY: each made here with one hold, used no more.
                unsafe {
                    entry.release();
                    moved.release();
                }
            }
        }
    }

    #[test]
    fn the_header_is_twelve_bytes_and_the_key_follows() {
        assert_eq!(HEADER, 12);
        let entry = Entry::string(b"key", b"value", None);
        assert_eq!(entry.size(), 12 + 3 + 5);
        assert_eq!(entry.key(), b"key");
        assert_eq!(entry.form(), INLINE);
        assert_eq!(entry.holders(), 1);
        assert!(!entry.has_ttl());
        assert_eq!(entry.expires(), None);
        // SAFETY: made here with one hold, used no more.
        unsafe { entry.release() };
        let long = vec![b'x'; 255];
        let entry = Entry::new(&long, KiviDBValue::List(Default::default()), None);
        assert_eq!(entry.key(), &long[..]);
        assert_eq!(entry.form(), WHOLE);
        assert_eq!(entry.size() % 8, 0);
        // SAFETY: as above.
        unsafe { entry.release() };
    }

    #[test]
    fn a_time_to_live_takes_twelve_bytes_before_the_key() {
        let at = 1_700_000_000_123u64;
        let entry = Entry::string(b"key", b"value", Some(at));
        assert_eq!(entry.size(), 12 + 12 + 3 + 5);
        assert!(entry.has_ttl());
        assert_eq!(entry.expires(), Some(at));
        assert_eq!(entry.key(), b"key");
        assert!(matches!(entry.value(), ValueRef::String(b) if b.as_bytes() == b"value"));
        entry.set_pos(77);
        assert_eq!(entry.pos(), 77);
        // SAFETY: the only hold, and nothing else has the entry.
        unsafe { entry.set_expires(at + 5) };
        assert_eq!(entry.expires(), Some(at + 5));
        // Remade without it, and with it again: key and value the same.
        let bare = entry.remade(None);
        assert_eq!(bare.size(), 12 + 3 + 5);
        assert_eq!(bare.expires(), None);
        let back = bare.remade(Some(at));
        assert_eq!(back.expires(), Some(at));
        assert!(matches!(back.value(), ValueRef::String(b) if b.as_bytes() == b"value"));
        // SAFETY: each made here with one hold, used no more.
        unsafe {
            entry.release();
            bare.release();
            back.release();
        }
        // A long key's length comes after the time.
        for klen in [254usize, 255, 1000] {
            let long = vec![b'y'; klen];
            let entry = Entry::new(&long, KiviDBValue::List(Default::default()), Some(at));
            assert_eq!(entry.key(), &long[..]);
            assert_eq!(entry.expires(), Some(at));
            assert_eq!(entry.size() % 8, 0);
            assert!(matches!(entry.value(), ValueRef::List(_)));
            let remade = entry.remade(None);
            assert_eq!(remade.key(), &long[..]);
            assert!(matches!(remade.value(), ValueRef::List(_)));
            // SAFETY: as above.
            unsafe {
                entry.release();
                remade.release();
            }
        }
    }

    #[test]
    fn a_string_at_the_inline_edge_goes_out_of_line_for_its_time_to_live() {
        // Fits inline without a time to live, not with one.
        let key = b"k";
        let vlen = INLINE_MAX - HEADER - key.len();
        let bytes = vec![3u8; vlen];
        let entry = Entry::string(key, &bytes, None);
        assert_eq!(entry.form(), INLINE);
        let timed = entry.remade(Some(1));
        assert_eq!(timed.form(), WHOLE);
        assert!(matches!(timed.value(), ValueRef::String(b) if b.as_bytes() == &bytes[..]));
        let again = timed.remade(None);
        assert_eq!(again.form(), INLINE);
        // SAFETY: each made here with one hold, used no more.
        unsafe {
            entry.release();
            timed.release();
            again.release();
        }
    }
}
