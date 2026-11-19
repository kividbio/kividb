//! The large form of hashes and sets (#764): [`CollectionTable`].
//!
//! Step 1 of the design on #764: the table alone. Nothing in the server uses
//! it yet; sets (step 2) and hashes (step 3) move onto it later.
//!
//! **Layout.** A power-of-two array of 64-byte groups, one cache line each:
//! seven control bytes, one meta byte and seven 8-byte slots (`Group`). A
//! control byte is 0 for an empty slot, or `0x80 | 7 bits of the hash` for
//! an element. When a group is full, slot 6 links to an overflow group
//! (control byte `LINK_BYTE`, meta bit `HAS_LINK`), and the element that
//! was there moves into it. Overflow groups come from the array's own
//! `Arena`, allocated in chunks, so it is never copied.
//!
//! **Elements.** One block per element, aligned to 16 ([`Memory`] says where
//! from): a hash entry is `[flen:7|TTL:1][vlen:7|LONG:1]`, then `u32 flen,
//! u32 vlen` when `LONG`, then `[u32 list position][u64 deadline]` when
//! `TTL`, then the field and the value. A set entry is `[len:7|LONG:1]`,
//! `u32 len` when `LONG`, then the member. A set member of at most seven
//! bytes is no block at all: it lives in the slot, tagged by its low bit
//! (a block's address is a multiple of 16).
//!
//! **Hash.** Per table, randomly seeded (`ahash`), so crafted keys cannot
//! build long chains. The home group is the TOP bits of the hash, the
//! fingerprint its low 7 bits, and an element's position for the cursor is
//! `hash >> 16`: with `2^b` groups, group `g` holds exactly the positions
//! `[g << (48 - b), (g + 1) << (48 - b))`, and a doubling splits `g` into
//! `2g` and `2g + 1` in order. That is what makes [`CollectionTable::scan`]
//! a plain increasing cursor.
//!
//! **Resizing.** Above the maximum fill (0.875 of the main slots) the table
//! doubles, below 1/8 it shrinks to about half full; either way a new array
//! is made and the elements move a few at a time on each write
//! ([`WRITE_STEP`]) or in [`CollectionTable::step_resize`], which the
//! background task will call. Until the old array is empty, lookups that miss
//! in the new one look in the old one; an element is in exactly one of them.
//! The worst write pays for a zeroed array (fresh pages from `calloc`) and
//! one step; freeing the old array when a resize ends can be handed to
//! another thread ([`CollectionTable::defer_free`]).
//!
//! **Unsafe.** The raw parts are few: `Group` (control-byte arithmetic,
//! no `unsafe`), the slot encodings (`inline_member`, the entry readers and
//! writers), the `Arena`, and the table's walks over raw group pointers.
//! The invariants each relies on are stated where they are, and checked by
//! `debug_assert!`s and, in tests, by `check()`.

// Nothing in the server uses the table yet (#764 step 1).
#![allow(dead_code)]

use std::alloc::Layout;
use std::marker::PhantomData;
use std::ptr::{self, NonNull};

use rand::Rng;

// ---------------------------------------------------------------------------
// Where the memory comes from
// ---------------------------------------------------------------------------

/// Where a table's blocks come from: its group arrays and arena chunks
/// (aligned to 64) and its elements' entries (aligned to 16). One small
/// interface so that the entries' allocator can change (open question 2 on
/// #764: `value_slab` once compaction can move a collection's entry), and so
/// that the tests can count.
///
/// # Safety
/// `alloc` returns a block of `layout`'s size and alignment, zeroed when
/// `zeroed` is true, and never fails silently. `free` takes back a block
/// `alloc` gave for the same layout.
pub unsafe trait Memory {
    /// A block of `layout`, zeroed if asked.
    fn alloc(layout: Layout, zeroed: bool) -> NonNull<u8>;
    /// Give back a block.
    ///
    /// # Safety
    /// `block` came from `alloc(layout, _)` and is not used again.
    unsafe fn free(block: NonNull<u8>, layout: Layout);
    /// What a block of `layout` counts for in `used_memory`.
    fn charged(layout: Layout) -> usize {
        layout.size()
    }
}

/// Everything from the system allocator (through the counting global
/// allocator, which counts what is asked for).
pub struct SystemMemory;

// SAFETY: `std::alloc` gives blocks of the layout asked for; failure aborts
// through `handle_alloc_error`.
unsafe impl Memory for SystemMemory {
    #[inline]
    fn alloc(layout: Layout, zeroed: bool) -> NonNull<u8> {
        debug_assert!(layout.size() > 0);
        // SAFETY: every layout the table asks for has a non-zero size (an
        // entry is at least 2 bytes, a group 64).
        let block = unsafe {
            if zeroed {
                std::alloc::alloc_zeroed(layout)
            } else {
                std::alloc::alloc(layout)
            }
        };
        NonNull::new(block).unwrap_or_else(|| std::alloc::handle_alloc_error(layout))
    }

    #[inline]
    unsafe fn free(block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller's promise: `alloc(layout)` gave it.
        unsafe { std::alloc::dealloc(block.as_ptr(), layout) }
    }
}

/// Entries from `value_slab`'s 16-byte classes, as keyspace entries are; the
/// group arrays and arena chunks from the system. Not what the table uses
/// yet: compaction cannot find a collection's entry to move it (see the PR
/// for #764 step 1), so a slab holding one could never be emptied. Here so
/// the two can be measured side by side.
pub struct SlabMemory;

impl SlabMemory {
    #[inline]
    fn class(layout: Layout) -> Option<usize> {
        if layout.align() == ENTRY_ALIGN {
            crate::value_slab::class_of(layout.size())
        } else {
            None
        }
    }
}

// SAFETY: a slab block of `class_of(size)` is at least `size` bytes and
// aligned to 16; anything else is `SystemMemory`'s. `free` picks the same
// side `alloc` did, from the same layout.
unsafe impl Memory for SlabMemory {
    fn alloc(layout: Layout, zeroed: bool) -> NonNull<u8> {
        match Self::class(layout) {
            Some(class) if !zeroed => crate::value_slab::alloc(class),
            _ => SystemMemory::alloc(layout, zeroed),
        }
    }

    unsafe fn free(block: NonNull<u8>, layout: Layout) {
        match Self::class(layout) {
            // SAFETY: `alloc` took it from this class (an entry is never
            // asked for zeroed).
            Some(class) => unsafe { crate::value_slab::free(block, class) },
            // SAFETY: the caller's promise.
            None => unsafe { SystemMemory::free(block, layout) },
        }
    }

    fn charged(layout: Layout) -> usize {
        Self::class(layout).map_or(layout.size(), crate::value_slab::block_size)
    }
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

/// Slots in a group.
const SLOTS: usize = 7;
/// The slot that links to an overflow group when the group has one.
const LINK: usize = SLOTS - 1;
const LO: u64 = 0x0101_0101_0101_0101;
const HI: u64 = 0x8080_8080_8080_8080;
/// The seven control bytes of a group's control word; the eighth is meta.
const CONTROL: u64 = (1 << 56) - 1;
/// Meta bit: slot [`LINK`] holds a link to an overflow group.
const HAS_LINK: u64 = 1 << 56;
/// The control byte of a link slot: not empty, and never a fingerprint
/// (whose high bit is set), so it matches no lookup.
const LINK_BYTE: u64 = 0x01;
/// The high bit of the link slot's control byte.
const LINK_HI: u64 = 0x80 << (8 * LINK);

/// One cache line: `ctrl` holds control byte `i` at bits `8i..8i+8` for
/// `i < 7` and the meta byte at bits 56..64; then seven slots.
///
/// Invariants: control byte `i` is 0 (slot `i` empty, its pointer unused),
/// `0x80 | fp` (slot `i` holds an element whose hash's low 7 bits are `fp`),
/// or, for `i == LINK` only and exactly when `HAS_LINK` is set,
/// [`LINK_BYTE`] (slot `i` points to the next group of the chain). All
/// zeroes is an empty group.
#[repr(C, align(64))]
struct Group {
    ctrl: u64,
    slots: [*mut u8; SLOTS],
}

const _: () = assert!(std::mem::size_of::<Group>() == 64);

impl Group {
    const EMPTY: Group = Group {
        ctrl: 0,
        slots: [ptr::null_mut(); SLOTS],
    };

    /// Slots (as `0x80` at byte `i`) whose control byte may be `fp`: every
    /// real match, and possibly other elements (the byte trick can borrow
    /// into the next byte), never an empty or link slot (their bytes have
    /// the high bit clear, `fp` has it set, so the xor does too).
    #[inline]
    fn matches(&self, fp: u8) -> u64 {
        let x = self.ctrl ^ (LO * u64::from(fp));
        x.wrapping_sub(LO) & !x & HI & CONTROL
    }

    /// Slots holding an element.
    #[inline]
    fn full(&self) -> u64 {
        self.ctrl & HI & CONTROL
    }

    /// Slots free for an element.
    #[inline]
    fn empty(&self) -> u64 {
        let empty = !self.ctrl & HI & CONTROL;
        if self.ctrl & HAS_LINK != 0 {
            empty & !LINK_HI
        } else {
            empty
        }
    }

    /// The next group of the chain, or null.
    #[inline]
    fn link(&self) -> *mut Group {
        if self.ctrl & HAS_LINK != 0 {
            self.slots[LINK].cast()
        } else {
            ptr::null_mut()
        }
    }

    #[inline]
    fn control(&self, i: usize) -> u8 {
        (self.ctrl >> (8 * i)) as u8
    }

    #[inline]
    fn is_element(&self, i: usize) -> bool {
        self.control(i) & 0x80 != 0
    }

    #[inline]
    fn put(&mut self, i: usize, control: u8, slot: *mut u8) {
        debug_assert!(i < SLOTS && control & 0x80 != 0);
        debug_assert!(self.control(i) == 0, "slot {i} is not empty");
        debug_assert!(i != LINK || self.ctrl & HAS_LINK == 0);
        self.ctrl |= u64::from(control) << (8 * i);
        self.slots[i] = slot;
    }

    #[inline]
    fn clear(&mut self, i: usize) {
        debug_assert!(self.is_element(i));
        self.ctrl &= !(0xFF << (8 * i));
        self.slots[i] = ptr::null_mut();
    }

    fn set_link(&mut self, next: *mut Group) {
        debug_assert!(self.control(LINK) == 0 && self.ctrl & HAS_LINK == 0);
        self.ctrl |= HAS_LINK | (LINK_BYTE << (8 * LINK));
        self.slots[LINK] = next.cast();
    }

    fn unlink(&mut self) {
        debug_assert!(self.ctrl & HAS_LINK != 0);
        self.ctrl &= !(HAS_LINK | (0xFF << (8 * LINK)));
        self.slots[LINK] = ptr::null_mut();
    }

    #[inline]
    fn count(&self) -> usize {
        self.full().count_ones() as usize
    }
}

/// The slot of the lowest byte set in a mask from [`Group::matches`],
/// [`Group::full`] or [`Group::empty`].
#[inline]
fn first(mask: u64) -> usize {
    mask.trailing_zeros() as usize / 8
}

/// A hash's fingerprint: its control byte.
#[inline]
fn fingerprint(h: u64) -> u8 {
    0x80 | (h as u8 & 0x7F)
}

/// An element's cursor position: 48 bits, as `helpers::scan_position`.
#[inline]
fn position(h: u64) -> u64 {
    h >> 16
}

/// Cursor positions are 48 bits, so at most 2^48 groups.
const MAX_BITS: u8 = 48;

// ---------------------------------------------------------------------------
// The overflow arena
// ---------------------------------------------------------------------------

/// The overflow groups of one group array, from chunks that are never moved
/// or copied: a group taken stays where it is until the arena is released.
///
/// Invariants: every chunk but the last is wholly handed out; the last is
/// handed out up to `used`, and the rest of it is uninitialised and never
/// read. A group handed out is either in a chain or on the free list (its
/// control word 0, its slot 0 the next free group).
struct Arena {
    chunks: Vec<(NonNull<Group>, usize)>,
    used: usize,
    free: *mut Group,
    /// Groups handed out of the chunks (in chains or free).
    groups: usize,
    /// Groups in chains.
    live: usize,
}

impl Arena {
    /// The largest chunk: 64 KiB.
    const MAX_CHUNK: usize = 1024;

    const fn new() -> Self {
        Arena {
            chunks: Vec::new(),
            used: 0,
            free: ptr::null_mut(),
            groups: 0,
            live: 0,
        }
    }

    fn chunk_layout(groups: usize) -> Layout {
        Layout::array::<Group>(groups).expect("an arena chunk fits in memory")
    }

    /// An empty group. `main_groups` sizes a new chunk: chunks double, from
    /// a 64th of the main array, up to [`Self::MAX_CHUNK`].
    fn take<A: Memory>(&mut self, main_groups: usize) -> NonNull<Group> {
        let group = if let Some(free) = NonNull::new(self.free) {
            // SAFETY: a free group is a handed-out group whose slot 0 is the
            // next free one (the invariant).
            self.free = unsafe { (*free.as_ptr()).slots[0].cast() };
            free
        } else {
            let room = self.chunks.last().is_some_and(|&(_, n)| self.used < n);
            if !room {
                let n = self.groups.max(main_groups / 64).clamp(1, Self::MAX_CHUNK);
                let chunk = A::alloc(Self::chunk_layout(n), false).cast::<Group>();
                self.chunks.push((chunk, n));
                self.used = 0;
            }
            let (chunk, _) = *self.chunks.last().expect("a chunk with room");
            // SAFETY: `used < n`: in the chunk.
            let group = unsafe { chunk.add(self.used) };
            self.used += 1;
            self.groups += 1;
            group
        };
        // SAFETY: a group of this arena's, not in any chain: ours to write.
        unsafe { group.as_ptr().write(Group::EMPTY) };
        self.live += 1;
        group
    }

    /// Take back a group unlinked from its chain.
    ///
    /// # Safety
    /// `group` came from [`Self::take`] on this arena, is empty, and is in
    /// no chain any more.
    unsafe fn give(&mut self, group: *mut Group) {
        // SAFETY: the caller's promise: it is ours and in no chain.
        unsafe {
            debug_assert!(
                (*group).ctrl == 0,
                "an arena group given back holds something"
            );
            (*group).slots[0] = self.free.cast();
        }
        self.free = group;
        self.live -= 1;
    }

    /// Calls `f` with each run of handed-out groups, in order.
    fn runs(&self, mut f: impl FnMut(*mut Group, usize) -> bool) -> bool {
        let last = self.chunks.len().wrapping_sub(1);
        for (i, &(chunk, n)) in self.chunks.iter().enumerate() {
            let n = if i == last { self.used } else { n };
            if n > 0 && f(chunk.as_ptr(), n) {
                return true;
            }
        }
        false
    }

    /// What it holds: its chunks and its list of them.
    fn bytes<A: Memory>(&self) -> usize {
        let chunks: usize = self
            .chunks
            .iter()
            .map(|&(_, n)| A::charged(Self::chunk_layout(n)))
            .sum();
        chunks + self.chunks.capacity() * std::mem::size_of::<(NonNull<Group>, usize)>()
    }

    /// Give every chunk back. The groups in them must not be used again.
    fn release<A: Memory>(&mut self) {
        for (chunk, n) in self.chunks.drain(..) {
            // SAFETY: taken with this layout in `take`.
            unsafe { A::free(chunk.cast(), Self::chunk_layout(n)) };
        }
        self.chunks = Vec::new();
        self.used = 0;
        self.free = ptr::null_mut();
        self.groups = 0;
        self.live = 0;
    }
}

// ---------------------------------------------------------------------------
// Group arrays
// ---------------------------------------------------------------------------

/// `2^bits` groups, zeroed when made.
///
/// The block is asked for aligned to 8 and 56 bytes longer, and the groups
/// start at its first multiple of 64 (`offset` bytes in). Asked for aligned
/// to 64, the system allocator zeroes a block with `memset` (Rust's
/// `alloc_zeroed` for an alignment above 16 is `posix_memalign` and a
/// fill), which for the 64 MiB of a 3M-element table took 13 ms on the
/// write that started the growth; `calloc` gets fresh pages from the kernel
/// already zeroed, and they are touched a few at a time by later writes.
#[derive(Clone, Copy)]
struct Array {
    groups: NonNull<Group>,
    bits: u8,
    /// Bytes from the block's start to `groups`: 0 to 56.
    offset: u8,
}

/// Room asked for beyond the groups, to align them to 64.
const ARRAY_SLACK: usize = 64 - 8;

impl Array {
    fn layout(bits: u8) -> Layout {
        let bytes = std::mem::size_of::<Group>()
            .checked_shl(u32::from(bits))
            .and_then(|b| b.checked_add(ARRAY_SLACK))
            .expect("a group array fits in memory");
        Layout::from_size_align(bytes, 8).expect("a group array fits in memory")
    }

    fn alloc<A: Memory>(bits: u8) -> Self {
        debug_assert!(bits <= MAX_BITS);
        let block = A::alloc(Self::layout(bits), true);
        let offset = (64 - block.as_ptr().addr() % 64) % 64;
        debug_assert!(offset <= ARRAY_SLACK && offset % 8 == 0);
        Array {
            // SAFETY: `offset <= ARRAY_SLACK`: the groups, `64 << bits`
            // bytes from there, are in the block.
            groups: unsafe { block.add(offset) }.cast(),
            bits,
            offset: offset as u8,
        }
    }

    /// # Safety
    /// Made by [`Self::alloc`] with `A`; no group of it is used again.
    unsafe fn free<A: Memory>(self) {
        // SAFETY: the caller's promise: the block starts `offset` bytes
        // before the groups.
        unsafe {
            let block = self.groups.cast::<u8>().sub(usize::from(self.offset));
            A::free(block, Self::layout(self.bits))
        }
    }

    #[inline]
    fn len(&self) -> usize {
        1 << self.bits
    }

    /// The home group of hash `h`: its top `bits` bits. (`h >> 1 >> (63 -
    /// bits)` is `h >> (64 - bits)` for `bits >= 1`, and 0 for `bits == 0`
    /// without a shift by 64.)
    #[inline]
    fn home(&self, h: u64) -> usize {
        ((h >> 1) >> (63 - u32::from(self.bits))) as usize
    }

    #[inline]
    fn group(&self, i: usize) -> *mut Group {
        debug_assert!(i < self.len());
        // SAFETY: `i < len`: in the array.
        unsafe { self.groups.as_ptr().add(i) }
    }
}

// ---------------------------------------------------------------------------
// Entries and slots
// ---------------------------------------------------------------------------

/// Lengths up to this fit in a header byte's 7 bits.
const SHORT: usize = 127;
/// An entry's alignment: so a block's low bit is free for the inline tag.
const ENTRY_ALIGN: usize = 16;
/// A field TTL's list position before step 5 lists it.
pub const NOT_LISTED: u32 = u32::MAX;

#[inline]
fn entry_layout(size: usize) -> Layout {
    // An entry is at most 2 + 8 + 12 + 2 * u32::MAX bytes: it fits.
    Layout::from_size_align(size, ENTRY_ALIGN).expect("an entry fits in memory")
}

#[inline]
fn len_u32(len: usize) -> u32 {
    u32::try_from(len).expect("a field, value or member is under 4 GiB")
}

/// # Safety
/// `at..at + 4` is readable.
#[inline]
unsafe fn read_u32(at: *const u8) -> u32 {
    let mut bytes = [0u8; 4];
    // SAFETY: the caller's promise.
    unsafe { ptr::copy_nonoverlapping(at, bytes.as_mut_ptr(), 4) };
    u32::from_le_bytes(bytes)
}

/// # Safety
/// `at..at + 8` is readable.
#[inline]
unsafe fn read_u64(at: *const u8) -> u64 {
    let mut bytes = [0u8; 8];
    // SAFETY: the caller's promise.
    unsafe { ptr::copy_nonoverlapping(at, bytes.as_mut_ptr(), 8) };
    u64::from_le_bytes(bytes)
}

/// # Safety
/// `at..at + bytes.len()` is writable and does not overlap `bytes`.
#[inline]
unsafe fn write_bytes(at: *mut u8, bytes: &[u8]) {
    // SAFETY: the caller's promise.
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), at, bytes.len()) }
}

/// Where the parts of a hash entry are.
#[derive(Clone, Copy)]
struct FieldHeader {
    flen: usize,
    vlen: usize,
    /// Where `[u32 position][u64 deadline]` is, if the field has a TTL.
    ttl_at: Option<usize>,
    /// Where the field is; its value follows.
    data_at: usize,
}

impl FieldHeader {
    fn size(&self) -> usize {
        self.data_at + self.flen + self.vlen
    }
}

#[inline]
fn field_header_for(flen: usize, vlen: usize, ttl: bool) -> FieldHeader {
    let long = flen > SHORT || vlen > SHORT;
    let mut at = 2 + if long { 8 } else { 0 };
    let ttl_at = ttl.then(|| {
        let ttl_at = at;
        at += 12;
        ttl_at
    });
    FieldHeader {
        flen,
        vlen,
        ttl_at,
        data_at: at,
    }
}

/// # Safety
/// `entry` is a hash entry written by [`write_field_entry`].
#[inline]
unsafe fn field_header(entry: *const u8) -> FieldHeader {
    // SAFETY: an entry is at least its 2 header bytes, and its LONG lengths
    // when the bit says so.
    unsafe {
        let b0 = *entry;
        let b1 = *entry.add(1);
        let ttl = b0 & 1 != 0;
        let (flen, vlen, mut at) = if b1 & 1 != 0 {
            (
                read_u32(entry.add(2)) as usize,
                read_u32(entry.add(6)) as usize,
                10,
            )
        } else {
            (usize::from(b0 >> 1), usize::from(b1 >> 1), 2)
        };
        let ttl_at = ttl.then(|| {
            let ttl_at = at;
            at += 12;
            ttl_at
        });
        FieldHeader {
            flen,
            vlen,
            ttl_at,
            data_at: at,
        }
    }
}

/// # Safety
/// `entry` is writable for `field_header_for(field.len(), value.len(),
/// ttl.is_some()).size()` bytes, and overlaps neither slice.
unsafe fn write_field_entry(entry: *mut u8, field: &[u8], value: &[u8], ttl: Option<u64>) {
    let header = field_header_for(field.len(), value.len(), ttl.is_some());
    let long = field.len() > SHORT || value.len() > SHORT;
    // SAFETY: the caller's promise: the block holds the whole entry.
    unsafe {
        if long {
            *entry = u8::from(ttl.is_some());
            *entry.add(1) = 1;
            write_bytes(entry.add(2), &len_u32(field.len()).to_le_bytes());
            write_bytes(entry.add(6), &len_u32(value.len()).to_le_bytes());
        } else {
            *entry = ((field.len() as u8) << 1) | u8::from(ttl.is_some());
            *entry.add(1) = (value.len() as u8) << 1;
        }
        if let (Some(at), Some(deadline)) = (header.ttl_at, ttl) {
            write_bytes(entry.add(at), &NOT_LISTED.to_le_bytes());
            write_bytes(entry.add(at + 4), &deadline.to_le_bytes());
        }
        write_bytes(entry.add(header.data_at), field);
        write_bytes(entry.add(header.data_at + field.len()), value);
    }
}

/// A hash's field, as the table holds it: borrowed until the table changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FieldRef<'a> {
    pub field: &'a [u8],
    pub value: &'a [u8],
    /// The field's deadline, if it has a TTL.
    pub ttl: Option<u64>,
    /// Where the field is in its hash's TTL list ([`NOT_LISTED`] until
    /// step 5 keeps one).
    pub ttl_position: Option<u32>,
}

/// # Safety
/// `entry` is a live hash entry, not freed or rewritten during `'a`.
#[inline]
unsafe fn field_ref<'a>(entry: *const u8) -> FieldRef<'a> {
    // SAFETY: the caller's promise; every offset is inside the entry.
    unsafe {
        let h = field_header(entry);
        let (ttl, ttl_position) = match h.ttl_at {
            Some(at) => (
                Some(read_u64(entry.add(at + 4))),
                Some(read_u32(entry.add(at))),
            ),
            None => (None, None),
        };
        FieldRef {
            field: std::slice::from_raw_parts(entry.add(h.data_at), h.flen),
            value: std::slice::from_raw_parts(entry.add(h.data_at + h.flen), h.vlen),
            ttl,
            ttl_position,
        }
    }
}

/// # Safety
/// `entry` is a live hash entry.
#[inline]
unsafe fn field_of<'a>(entry: *const u8) -> &'a [u8] {
    // SAFETY: the caller's promise.
    unsafe {
        let h = field_header(entry);
        std::slice::from_raw_parts(entry.add(h.data_at), h.flen)
    }
}

/// Members of up to this many bytes live in their slot.
pub const INLINE_MEMBER: usize = 7;

/// A member of at most [`INLINE_MEMBER`] bytes as its slot: low bit 1, its
/// length at bits 1..4, its bytes at bits 8..64. No provenance: it is no
/// pointer. Two members are equal exactly when their encodings are.
#[inline]
fn inline_member(member: &[u8]) -> Option<*mut u8> {
    if member.len() > INLINE_MEMBER {
        return None;
    }
    let word = 1 | (member.len() << 1) | ((pack7(member) as usize) << 8);
    Some(ptr::without_provenance_mut(word))
}

/// Up to 7 bytes as a little-endian number, without a call to `memcpy`
/// (on every lookup of a short member): two loads that overlap for 4 to 7
/// bytes, three for 1 to 3.
#[inline]
fn pack7(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    debug_assert!(len <= INLINE_MEMBER);
    if len >= 4 {
        let lo = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let at = len - 4;
        let hi = u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);
        u64::from(lo) | (u64::from(hi) << (8 * at))
    } else if len > 0 {
        u64::from(bytes[0])
            | (u64::from(bytes[len / 2]) << (8 * (len / 2)))
            | (u64::from(bytes[len - 1]) << (8 * (len - 1)))
    } else {
        0
    }
}

#[inline]
fn is_inline(slot: *mut u8) -> bool {
    slot.addr() & 1 != 0
}

/// The bytes of an inline member, and how many there are.
#[inline]
fn inline_bytes(slot: *mut u8) -> ([u8; INLINE_MEMBER], usize) {
    let word = slot.addr();
    let len = (word >> 1) & 7;
    let all = ((word >> 8) as u64).to_le_bytes();
    let mut bytes = [0u8; INLINE_MEMBER];
    bytes.copy_from_slice(&all[..INLINE_MEMBER]);
    (bytes, len)
}

#[inline]
fn member_size(len: usize) -> usize {
    1 + if len > SHORT { 4 } else { 0 } + len
}

/// # Safety
/// `entry` is writable for `member_size(member.len())` bytes and does not
/// overlap `member`.
unsafe fn write_member_entry(entry: *mut u8, member: &[u8]) {
    // SAFETY: the caller's promise.
    unsafe {
        if member.len() > SHORT {
            *entry = 1;
            write_bytes(entry.add(1), &len_u32(member.len()).to_le_bytes());
            write_bytes(entry.add(5), member);
        } else {
            *entry = (member.len() as u8) << 1;
            write_bytes(entry.add(1), member);
        }
    }
}

/// # Safety
/// `entry` is a live set entry, not freed during `'a`.
#[inline]
unsafe fn member_of<'a>(entry: *const u8) -> &'a [u8] {
    // SAFETY: the caller's promise.
    unsafe {
        let b0 = *entry;
        if b0 & 1 != 0 {
            std::slice::from_raw_parts(entry.add(5), read_u32(entry.add(1)) as usize)
        } else {
            std::slice::from_raw_parts(entry.add(1), usize::from(b0 >> 1))
        }
    }
}

/// A set's member, as the table holds it: borrowed until the table changes,
/// or a copy of the bytes of a member that lives in its slot.
#[derive(Clone, Copy)]
pub enum MemberRef<'a> {
    Inline([u8; INLINE_MEMBER], u8),
    Bytes(&'a [u8]),
}

impl MemberRef<'_> {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            MemberRef::Inline(bytes, len) => &bytes[..usize::from(*len)],
            MemberRef::Bytes(bytes) => bytes,
        }
    }
}

impl std::ops::Deref for MemberRef<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::fmt::Debug for MemberRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_bytes().fmt(f)
    }
}

/// # Safety
/// `slot` holds a live set element, not freed during `'a`.
#[inline]
unsafe fn member_ref<'a>(slot: *mut u8) -> MemberRef<'a> {
    if is_inline(slot) {
        let (bytes, len) = inline_bytes(slot);
        MemberRef::Inline(bytes, len as u8)
    } else {
        // SAFETY: the caller's promise; not inline, so an entry.
        MemberRef::Bytes(unsafe { member_of(slot) })
    }
}

mod sealed {
    pub trait Sealed {}
}

/// What a table holds: [`Fields`] (a hash's) or [`Members`] (a set's).
pub trait Kind: sealed::Sealed {
    /// The hash of the key of the element in `slot`.
    ///
    /// # Safety
    /// `slot` holds a live element of this kind.
    #[doc(hidden)]
    unsafe fn hash_of(slot: *mut u8, hasher: &ahash::RandomState) -> u64;

    /// The layout of the element's block, `None` if it has none.
    ///
    /// # Safety
    /// `slot` holds a live element of this kind.
    #[doc(hidden)]
    unsafe fn block(slot: *mut u8) -> Option<Layout>;
}

/// A hash's fields and their values (and TTLs).
pub struct Fields;
/// A set's members.
pub struct Members;

impl sealed::Sealed for Fields {}
impl sealed::Sealed for Members {}

impl Kind for Fields {
    #[inline]
    unsafe fn hash_of(slot: *mut u8, hasher: &ahash::RandomState) -> u64 {
        // SAFETY: the caller's promise: a hash entry.
        hasher.hash_one(unsafe { field_of(slot) })
    }

    #[inline]
    unsafe fn block(slot: *mut u8) -> Option<Layout> {
        // SAFETY: the caller's promise: a hash entry.
        Some(entry_layout(unsafe { field_header(slot) }.size()))
    }
}

impl Kind for Members {
    #[inline]
    unsafe fn hash_of(slot: *mut u8, hasher: &ahash::RandomState) -> u64 {
        if is_inline(slot) {
            let (bytes, len) = inline_bytes(slot);
            hasher.hash_one(&bytes[..len])
        } else {
            // SAFETY: the caller's promise: a set entry.
            hasher.hash_one(unsafe { member_of(slot) })
        }
    }

    #[inline]
    unsafe fn block(slot: *mut u8) -> Option<Layout> {
        if is_inline(slot) {
            None
        } else {
            // SAFETY: the caller's promise: a set entry.
            Some(entry_layout(member_size(unsafe { member_of(slot) }.len())))
        }
    }
}

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

/// The default maximum fill, per mille of the main slots (open question 1 on
/// #764; see the PR for the measurements).
pub const MAX_FILL: u16 = 875;

/// Work (groups visited plus elements moved) a write does on a resize in
/// progress: about one full group's worth while growing, a few sparse groups
/// while shrinking.
pub const WRITE_STEP: usize = 8;

/// Picks of a random slot before [`CollectionTable::random`] falls back to a
/// walk. The fill is at least 1/8 of the main slots, so a pick hits an
/// element with probability about 1/8 or more: 128 misses in a row are about
/// one in 25,000 at that fill (arena groups count too), and the walk is a
/// little biased, never wrong.
const RANDOM_PICKS: usize = 128;

/// The table: a hash's fields or a set's members (`K`), its memory from `A`.
pub struct CollectionTable<K: Kind, A: Memory = SystemMemory> {
    /// The main array (see [`Self::main`]): its groups and its size, apart
    /// so that the header packs into 80 bytes.
    groups: NonNull<Group>,
    bits: u8,
    offset: u8,
    len: usize,
    /// The length past which `main` doubles.
    grow_at: usize,
    /// What the elements' entries count for (`A::charged`).
    entry_bytes: usize,
    hasher: ahash::RandomState,
    /// The main array's arena and a resize in progress: behind one pointer,
    /// made on the first overflow or resize.
    rare: Option<Box<Rare>>,
    max_fill: u16,
    _marker: PhantomData<(K, A)>,
}

struct Rare {
    arena: Arena,
    old: Option<Old>,
    /// The array (and arena) of a finished resize, kept for the caller to
    /// free elsewhere ([`CollectionTable::defer_free`]).
    retired: Option<(Array, Arena)>,
    defer_free: bool,
}

impl Rare {
    fn new() -> Self {
        Rare {
            arena: Arena::new(),
            old: None,
            retired: None,
            defer_free: false,
        }
    }
}

/// The group array and overflow arena a finished resize left behind:
/// freed when dropped, on whatever thread drops it. Freeing them is the
/// one cost of a resize that grows with the table (the 16 MiB array left by
/// growing past 1.6M elements took 0.9 ms to free), so the server can hand
/// them to a thread of its own rather than pay it on a write
/// ([`CollectionTable::defer_free`]).
pub struct Retired<A: Memory = SystemMemory> {
    array: Array,
    arena: Arena,
    _memory: PhantomData<A>,
}

// SAFETY: it owns its blocks, and nothing else points into them.
unsafe impl<A: Memory> Send for Retired<A> {}

impl<A: Memory> Retired<A> {
    /// What freeing it gives back, as `allocation_size` counted it.
    pub fn bytes(&self) -> usize {
        A::charged(Array::layout(self.array.bits)) + self.arena.bytes::<A>()
    }
}

impl<A: Memory> Drop for Retired<A> {
    fn drop(&mut self) {
        // SAFETY: a finished resize's array: every element moved out of it.
        unsafe { self.array.free::<A>() };
        self.arena.release::<A>();
    }
}

/// The array a resize is moving elements out of.
struct Old {
    array: Array,
    arena: Arena,
    /// Groups before this one have been moved, and are empty.
    next: usize,
}

// SAFETY: the table owns every block its pointers reach, and gives out
// borrows only through `&self` (reads) or `&mut self` (writes): it is as
// thread-safe as a `Vec<Box<[u8]>>`.
unsafe impl<K: Kind, A: Memory> Send for CollectionTable<K, A> {}
// SAFETY: as above; `&self` methods only read.
unsafe impl<K: Kind, A: Memory> Sync for CollectionTable<K, A> {}

/// Where a lookup found an element.
#[derive(Clone, Copy)]
struct Found {
    group: *mut Group,
    slot: usize,
    /// The chain's first group (its home group).
    head: *mut Group,
    in_old: bool,
}

impl Found {
    #[inline]
    fn value(&self) -> *mut u8 {
        // SAFETY: a lookup returns a group of the table's.
        unsafe { (*self.group).slots[self.slot] }
    }
}

#[inline]
fn grow_at(bits: u8, max_fill: u16) -> usize {
    (SLOTS << bits) * usize::from(max_fill) / 1000
}

/// The fewest bits whose array holds `n` elements under `max_fill`.
fn bits_for(n: usize, max_fill: u16) -> u8 {
    let mut bits = 0;
    while bits < MAX_BITS && grow_at(bits, max_fill) < n {
        bits += 1;
    }
    bits
}

/// The fewest bits whose array holds `n` elements at most half full: where
/// a shrink goes.
fn shrink_bits(n: usize) -> u8 {
    let mut bits = 0;
    while n * 2 > SLOTS << bits {
        bits += 1;
    }
    bits
}

/// Put an element (`slot`, hash `h`) in `array`'s chain for it, taking an
/// overflow group from `arena` if the chain is full.
///
/// # Safety
/// `array` and `arena` are a table's live main array and its arena, made
/// with `A`; the element is in no array.
unsafe fn place<A: Memory>(array: Array, arena: &mut Arena, h: u64, slot: *mut u8) {
    let fp = fingerprint(h);
    let mut group = array.group(array.home(h));
    // SAFETY: every group reached is the array's or a live overflow group of
    // its chains (links point only to those).
    unsafe {
        loop {
            let empty = (*group).empty();
            if empty != 0 {
                (*group).put(first(empty), fp, slot);
                return;
            }
            let next = (*group).link();
            if next.is_null() {
                // Full with no link: slot 6's element moves to a new
                // overflow group, and slot 6 links to it.
                let overflow = arena.take::<A>(array.len()).as_ptr();
                let moved = (*group).control(LINK);
                let moved_slot = (*group).slots[LINK];
                (*group).clear(LINK);
                (*overflow).put(0, moved, moved_slot);
                (*overflow).put(1, fp, slot);
                (*group).set_link(overflow);
                return;
            }
            group = next;
        }
    }
}

impl<K: Kind, A: Memory> Default for CollectionTable<K, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Kind, A: Memory> CollectionTable<K, A> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Room for `n` elements before it grows.
    pub fn with_capacity(n: usize) -> Self {
        Self::build(n, ahash::RandomState::new())
    }

    /// With the hash seeded from `seed`, for tests and measurements that
    /// must repeat. The server never uses it: a fixed seed lets crafted keys
    /// build long chains.
    pub fn with_capacity_and_seed(n: usize, seed: u64) -> Self {
        let hasher = ahash::RandomState::with_seeds(
            seed,
            seed ^ 0x243F_6A88_85A3_08D3,
            seed.rotate_left(17) ^ 0x1319_8A2E_0370_7344,
            seed.rotate_left(41) ^ 0xA409_3822_299F_31D0,
        );
        Self::build(n, hasher)
    }

    fn build(n: usize, hasher: ahash::RandomState) -> Self {
        let bits = bits_for(n, MAX_FILL);
        let main = Array::alloc::<A>(bits);
        CollectionTable {
            groups: main.groups,
            bits,
            offset: main.offset,
            len: 0,
            grow_at: grow_at(bits, MAX_FILL),
            entry_bytes: 0,
            hasher,
            rare: None,
            max_fill: MAX_FILL,
            _marker: PhantomData,
        }
    }

    /// For measurements (open question 1): grow above `per_mille` of the
    /// main slots instead of [`MAX_FILL`]. Takes effect at once.
    #[doc(hidden)]
    pub fn set_max_fill(&mut self, per_mille: u16) {
        assert!((500..=1000).contains(&per_mille));
        self.max_fill = per_mille;
        self.grow_at = grow_at(self.main().bits, per_mille);
    }

    /// The main array: where new elements go.
    #[inline]
    fn main(&self) -> Array {
        Array {
            groups: self.groups,
            bits: self.bits,
            offset: self.offset,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether a resize is in progress: the background task should call
    /// [`Self::step_resize`] until it is not.
    #[inline]
    pub fn resizing(&self) -> bool {
        self.rare.as_ref().is_some_and(|rare| rare.old.is_some())
    }

    #[inline]
    fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// The element with hash `h` for which `eq` says yes: in the main array,
    /// then in the old one while a resize is in progress.
    #[inline]
    fn find(&self, h: u64, eq: impl Fn(*mut u8) -> bool) -> Option<Found> {
        let fp = fingerprint(h);
        let head = self.main().group(self.main().home(h));
        // SAFETY: `head` is the main array's.
        if let Some(found) = unsafe { Self::find_in(head, fp, &eq) } {
            return Some(found);
        }
        let old = self.rare.as_ref()?.old.as_ref()?;
        let index = old.array.home(h);
        if index < old.next {
            // Moved already, and empty.
            return None;
        }
        // SAFETY: the old array's, not yet moved.
        unsafe { Self::find_in(old.array.group(index), fp, &eq) }.map(|found| Found {
            in_old: true,
            ..found
        })
    }

    /// # Safety
    /// `head` is a live home group of the table's.
    #[inline]
    unsafe fn find_in(head: *mut Group, fp: u8, eq: &impl Fn(*mut u8) -> bool) -> Option<Found> {
        let mut group = head;
        // SAFETY: the caller's promise; links reach only live chain groups,
        // and `matches` only element slots.
        unsafe {
            loop {
                let mut candidates = (*group).matches(fp);
                while candidates != 0 {
                    let slot = first(candidates);
                    if eq((*group).slots[slot]) {
                        return Some(Found {
                            group,
                            slot,
                            head,
                            in_old: false,
                        });
                    }
                    candidates &= candidates - 1;
                }
                group = (*group).link();
                if group.is_null() {
                    return None;
                }
            }
        }
    }

    /// Add an element known not to be in the table.
    fn add(&mut self, h: u64, slot: *mut u8) {
        if self.len >= self.grow_at && self.main().bits < MAX_BITS {
            if self.resizing() {
                // Not reached at WRITE_STEP: a growth's moves finish long
                // before the table doubles again. Kept for safety.
                self.step_resize(usize::MAX);
            }
            self.start_resize(self.main().bits + 1);
        }
        let main = self.main();
        let rare = self.rare.get_or_insert_with(|| Box::new(Rare::new()));
        // SAFETY: the main array and its arena; the element is new.
        unsafe { place::<A>(main, &mut rare.arena, h, slot) };
        self.len += 1;
        if rare.old.is_some() {
            self.step_resize(WRITE_STEP);
        }
    }

    /// Take the element `found` out of the table, and give its slot value
    /// back for the caller to free.
    fn take_out(&mut self, found: Found) -> *mut u8 {
        let value = found.value();
        // SAFETY: `found` is a live element of the table's.
        unsafe { (*found.group).clear(found.slot) };
        if !found.in_old {
            // SAFETY: `found.head` is a main home group, `found.group` in
            // its chain.
            unsafe { self.compact(found.head, found.group) };
        }
        self.len -= 1;
        value
    }

    /// After a removal: shrink below 1/8, or take a step of a resize.
    fn after_remove(&mut self) {
        if self.resizing() {
            self.step_resize(WRITE_STEP);
        } else if self.main().bits > 0 && self.len * 8 < SLOTS << self.main().bits {
            self.start_resize(shrink_bits(self.len));
        }
    }

    /// After a removal from `hole` in `head`'s chain: keep the chain dense,
    /// every group but the last full. An element of the last group fills
    /// the hole, and if what is left of the last group fits in the group
    /// before it (whose link slot it frees), it moves there and the last
    /// group goes back to the arena. A chain is then as short as its
    /// contents allow, however its elements were removed.
    ///
    /// # Safety
    /// `head` is a main home group and `hole` a group of its chain.
    unsafe fn compact(&mut self, head: *mut Group, hole: *mut Group) {
        // SAFETY: the caller's promise; links reach only the chain's groups.
        unsafe {
            let mut before = ptr::null_mut::<Group>();
            let mut tail = head;
            loop {
                let next = (*tail).link();
                if next.is_null() {
                    break;
                }
                before = tail;
                tail = next;
            }
            if before.is_null() {
                return;
            }
            if hole != tail {
                let from = first((*tail).full());
                if (*tail).full() != 0 {
                    let to = first((*hole).empty());
                    (*hole).put(to, (*tail).control(from), (*tail).slots[from]);
                    (*tail).clear(from);
                }
            }
            let room = (*before).empty().count_ones() as usize + 1;
            if (*tail).count() > room {
                return;
            }
            (*before).unlink();
            let mut full = (*tail).full();
            while full != 0 {
                let i = first(full);
                let to = first((*before).empty());
                (*before).put(to, (*tail).control(i), (*tail).slots[i]);
                full &= full - 1;
            }
            (*tail).ctrl = 0;
            let rare = self.rare.as_mut().expect("a chain has an arena");
            rare.arena.give(tail);
        }
    }

    fn start_resize(&mut self, bits: u8) {
        debug_assert!(!self.resizing());
        let new = Array::alloc::<A>(bits);
        let main = self.main();
        let rare = self.rare.get_or_insert_with(|| Box::new(Rare::new()));
        let arena = std::mem::replace(&mut rare.arena, Arena::new());
        rare.old = Some(Old {
            array: main,
            arena,
            next: 0,
        });
        self.groups = new.groups;
        self.bits = new.bits;
        self.offset = new.offset;
        self.grow_at = grow_at(bits, self.max_fill);
    }

    /// Move elements of a resize in progress into the new array: about
    /// `budget` units of work, a unit being a group visited or an element
    /// moved (a home group's chain moves whole). Returns whether the resize
    /// is done (or none was in progress). What the background task calls,
    /// in bounded steps under the collection's write lock.
    pub fn step_resize(&mut self, budget: usize) -> bool {
        let main = self.main();
        let Some(rare) = self.rare.as_mut() else {
            return true;
        };
        let Rare {
            arena,
            old,
            retired,
            defer_free,
        } = &mut **rare;
        let Some(o) = old.as_mut() else {
            return true;
        };
        let mut work = 0usize;
        while o.next < o.array.len() && work < budget {
            let mut group = o.array.group(o.next);
            // SAFETY: an old home group not yet moved and its chain; each
            // element goes to the main array exactly once, and the old groups
            // are emptied as they are left.
            unsafe {
                loop {
                    let mut full = (*group).full();
                    while full != 0 {
                        let i = first(full);
                        let slot = (*group).slots[i];
                        let h = K::hash_of(slot, &self.hasher);
                        debug_assert_eq!(fingerprint(h), (*group).control(i));
                        place::<A>(main, arena, h, slot);
                        work += 1;
                        full &= full - 1;
                    }
                    let next = (*group).link();
                    (*group).ctrl = 0;
                    work += 1;
                    if next.is_null() {
                        break;
                    }
                    group = next;
                }
            }
            o.next += 1;
        }
        if o.next < o.array.len() {
            return false;
        }
        let o = old.take().expect("a resize in progress");
        // Every element has moved out of it; nothing points into it.
        let done = if *defer_free {
            // One not taken since an earlier resize is freed now.
            retired.replace((o.array, o.arena))
        } else {
            Some((o.array, o.arena))
        };
        if let Some((array, arena)) = done {
            drop(Retired::<A> {
                array,
                arena,
                _memory: PhantomData,
            });
        }
        true
    }

    /// Keep the array and arena a finished resize leaves for
    /// [`Self::take_retired`] instead of freeing them on the write that
    /// finished it. One not taken is freed when the next resize finishes.
    pub fn defer_free(&mut self, defer: bool) {
        let rare = self.rare.get_or_insert_with(|| Box::new(Rare::new()));
        rare.defer_free = defer;
        if !defer {
            drop(self.take_retired());
        }
    }

    /// The array and arena of the last finished resize, if
    /// [`Self::defer_free`] kept them.
    pub fn take_retired(&mut self) -> Option<Retired<A>> {
        let (array, arena) = self.rare.as_mut()?.retired.take()?;
        Some(Retired {
            array,
            arena,
            _memory: PhantomData,
        })
    }

    /// Allocate an entry block of `size` bytes.
    #[inline]
    fn new_block(&mut self, size: usize) -> NonNull<u8> {
        let layout = entry_layout(size);
        self.entry_bytes += A::charged(layout);
        A::alloc(layout, false)
    }

    /// Free an element's block, if it has one.
    ///
    /// # Safety
    /// `slot` held an element of the table's and is in it no more.
    #[inline]
    unsafe fn free_element(&mut self, slot: *mut u8) {
        // SAFETY: the caller's promise.
        if let Some(layout) = unsafe { K::block(slot) } {
            self.entry_bytes -= A::charged(layout);
            // SAFETY: allocated by `new_block` with this layout.
            unsafe { A::free(NonNull::new_unchecked(slot), layout) };
        }
    }

    /// Calls `f` with each run of groups the table holds: the main array,
    /// its arena's chunks, the old array and its arena's. Stops when `f`
    /// says so.
    fn runs(&self, mut f: impl FnMut(*mut Group, usize) -> bool) {
        if f(self.main().groups.as_ptr(), self.main().len()) {
            return;
        }
        let Some(rare) = self.rare.as_ref() else {
            return;
        };
        if rare.arena.runs(&mut f) {
            return;
        }
        if let Some(old) = rare.old.as_ref() {
            if f(old.array.groups.as_ptr(), old.array.len()) {
                return;
            }
            old.arena.runs(&mut f);
        }
    }

    /// A slot holding an element, every element equally likely: a uniformly
    /// random physical slot over the arrays and arenas, retried while it is
    /// empty.
    fn random_slot(&self, rng: &mut impl Rng) -> Option<*mut u8> {
        if self.len == 0 {
            return None;
        }
        let mut total = 0usize;
        self.runs(|_, n| {
            total += n;
            false
        });
        let slots = total * SLOTS;
        let pick_at = |mut at: usize| -> (*mut Group, usize) {
            let slot = at % SLOTS;
            at /= SLOTS;
            let mut found = ptr::null_mut();
            self.runs(|run, n| {
                if at < n {
                    // SAFETY: `at < n`: in the run.
                    found = unsafe { run.add(at) };
                    true
                } else {
                    at -= n;
                    false
                }
            });
            (found, slot)
        };
        for _ in 0..RANDOM_PICKS {
            let (group, slot) = pick_at(rng.gen_range(0..slots));
            // SAFETY: a handed-out group of the table's.
            unsafe {
                if (*group).is_element(slot) {
                    return Some((*group).slots[slot]);
                }
            }
        }
        // Rare: walk on from a random slot to the next element.
        let start = rng.gen_range(0..slots);
        for step in 0..slots {
            let (group, slot) = pick_at((start + step) % slots);
            // SAFETY: as above.
            unsafe {
                if (*group).is_element(slot) {
                    return Some((*group).slots[slot]);
                }
            }
        }
        unreachable!("a table of {} elements holds none", self.len)
    }

    /// The cursor walk of [`Self::scan`] over raw slots; also says how many
    /// groups it read.
    fn scan_raw(&self, cursor: u64, count: usize, mut f: impl FnMut(*mut u8)) -> (u64, usize) {
        if cursor >= 1 << 48 {
            return (0, 0);
        }
        let count = count.max(1);
        let old = self.rare.as_ref().and_then(|rare| rare.old.as_ref());
        let coarse = old.map_or(self.main().bits, |o| o.array.bits.min(self.main().bits));
        let units = 1u64 << coarse;
        let mut unit = cursor >> (48 - u32::from(coarse));
        let mut seen = 0usize;
        let mut groups = 0usize;
        // A run of empty groups ends a page early: SCAN may return fewer
        // than COUNT, never cost more than a bounded walk.
        let max_groups = count.saturating_mul(10);
        let mut visit =
            |array: &Array, unit: u64, from: usize, seen: &mut usize, groups: &mut usize| {
                let shift = array.bits - coarse;
                let lo = (unit as usize) << shift;
                let hi = (unit as usize + 1) << shift;
                for index in lo.max(from)..hi {
                    let mut group = array.group(index);
                    // SAFETY: a home group of the array and its chain.
                    unsafe {
                        loop {
                            *groups += 1;
                            let mut full = (*group).full();
                            while full != 0 {
                                f((*group).slots[first(full)]);
                                *seen += 1;
                                full &= full - 1;
                            }
                            group = (*group).link();
                            if group.is_null() {
                                break;
                            }
                        }
                    }
                }
            };
        while unit < units {
            visit(&self.main(), unit, 0, &mut seen, &mut groups);
            if let Some(o) = old {
                visit(&o.array, unit, o.next, &mut seen, &mut groups);
            }
            unit += 1;
            if seen >= count || groups >= max_groups {
                break;
            }
        }
        let next = if unit >= units {
            0
        } else {
            unit << (48 - u32::from(coarse))
        };
        (next, groups)
    }

    /// Every element's slot, main array first.
    fn slots(&self) -> Slots<'_> {
        let old = self
            .rare
            .as_ref()
            .and_then(|rare| rare.old.as_ref())
            .map(|o| (o.array, o.next));
        Slots {
            arrays: [Some((self.main(), 0)), old],
            which: 0,
            index: 0,
            group: ptr::null(),
            mask: 0,
            _table: PhantomData,
        }
    }

    /// The bytes the table holds, exactly as `used_memory` counts them for
    /// `A` = [`SystemMemory`]: the group arrays, the arenas, the rare part
    /// and the entries at what `A` charges. Not the table's own header,
    /// which is inside whatever holds the table.
    pub fn allocation_size(&self) -> usize {
        let mut bytes = A::charged(Array::layout(self.main().bits)) + self.entry_bytes;
        if let Some(rare) = self.rare.as_ref() {
            bytes += std::mem::size_of::<Rare>() + rare.arena.bytes::<A>();
            if let Some(old) = rare.old.as_ref() {
                bytes += A::charged(Array::layout(old.array.bits)) + old.arena.bytes::<A>();
            }
            if let Some((array, arena)) = rare.retired.as_ref() {
                bytes += A::charged(Array::layout(array.bits)) + arena.bytes::<A>();
            }
        }
        bytes
    }

    /// Shape of the table, for measurements (open question 1).
    #[doc(hidden)]
    pub fn stats(&self) -> TableStats {
        let mut stats = TableStats {
            groups: self.main().len(),
            ..TableStats::default()
        };
        if let Some(rare) = self.rare.as_ref() {
            stats.overflow_groups = rare.arena.live;
            stats.arena_groups = rare.arena.groups;
        }
        for index in 0..self.main().len() {
            let mut group = self.main().group(index);
            let mut depth = 0;
            // SAFETY: a main home group and its chain.
            unsafe {
                loop {
                    depth += 1;
                    stats.hit_groups += depth * (*group).count();
                    group = (*group).link();
                    if group.is_null() {
                        break;
                    }
                }
            }
            stats.miss_groups += depth;
            stats.max_chain = stats.max_chain.max(depth);
        }
        stats
    }
}

/// See [`CollectionTable::stats`].
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct TableStats {
    /// Main groups.
    pub groups: usize,
    /// Overflow groups in chains.
    pub overflow_groups: usize,
    /// Overflow groups handed out of the arena (in chains or free).
    pub arena_groups: usize,
    /// Groups read to find every element once, summed (main array only).
    pub hit_groups: usize,
    /// Groups read by a miss in each home group, summed.
    pub miss_groups: usize,
    /// The longest chain, in groups.
    pub max_chain: usize,
}

/// Iterator over every element's slot.
struct Slots<'a> {
    /// The arrays, and the first group of each still holding elements.
    arrays: [Option<(Array, usize)>; 2],
    which: usize,
    index: usize,
    group: *const Group,
    mask: u64,
    _table: PhantomData<&'a ()>,
}

impl Iterator for Slots<'_> {
    type Item = *mut u8;

    #[inline]
    fn next(&mut self) -> Option<*mut u8> {
        loop {
            if self.mask != 0 {
                let slot = first(self.mask);
                self.mask &= self.mask - 1;
                // SAFETY: `group` is a live group of the table, which the
                // borrow keeps unchanged.
                return Some(unsafe { (*self.group).slots[slot] });
            }
            if !self.group.is_null() {
                // SAFETY: as above.
                let next = unsafe { (*self.group).link() };
                if !next.is_null() {
                    self.group = next;
                    // SAFETY: a link reaches a live chain group.
                    self.mask = unsafe { (*next).full() };
                    continue;
                }
            }
            let (array, from) = (*self.arrays.get(self.which)?)?;
            let index = self.index.max(from);
            if index < array.len() {
                self.group = array.group(index);
                // SAFETY: the array's.
                self.mask = unsafe { (*self.group).full() };
                self.index = index + 1;
            } else {
                self.which += 1;
                self.index = 0;
                self.group = ptr::null();
            }
        }
    }
}

impl<K: Kind, A: Memory> Drop for CollectionTable<K, A> {
    fn drop(&mut self) {
        for slot in self.slots() {
            // SAFETY: each element once; nothing reads it after.
            if let Some(layout) = unsafe { K::block(slot) } {
                // SAFETY: allocated by `new_block` with this layout.
                unsafe { A::free(NonNull::new_unchecked(slot), layout) };
            }
        }
        // SAFETY: no element or group is used after this.
        unsafe { self.main().free::<A>() };
        drop(self.take_retired());
        if let Some(mut rare) = self.rare.take() {
            rare.arena.release::<A>();
            if let Some(mut old) = rare.old.take() {
                // SAFETY: as above.
                unsafe { old.array.free::<A>() };
                old.arena.release::<A>();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------------

/// # Safety
/// `slot` holds a live hash entry.
#[inline]
unsafe fn field_is(slot: *mut u8, field: &[u8]) -> bool {
    // SAFETY: the caller's promise.
    unsafe { field_of(slot) == field }
}

impl<A: Memory> CollectionTable<Fields, A> {
    /// The field, its value and its TTL.
    #[inline]
    pub fn get(&self, field: &[u8]) -> Option<FieldRef<'_>> {
        let h = self.hash(field);
        // SAFETY: the table's slots hold hash entries.
        let found = self.find(h, |slot| unsafe { field_is(slot, field) })?;
        // SAFETY: a live entry, unchanged while `self` is borrowed.
        Some(unsafe { field_ref(found.value()) })
    }

    #[inline]
    pub fn contains_key(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    /// Set the field's value and TTL (`None`: none); true if it is new.
    pub fn insert(&mut self, field: &[u8], value: &[u8], ttl: Option<u64>) -> bool {
        let h = self.hash(field);
        // SAFETY: as in `get`.
        if let Some(found) = self.find(h, |slot| unsafe { field_is(slot, field) }) {
            self.rewrite(found, field, value, ttl);
            if self.resizing() {
                self.step_resize(WRITE_STEP);
            }
            return false;
        }
        let size = field_header_for(field.len(), value.len(), ttl.is_some()).size();
        let block = self.new_block(size);
        // SAFETY: a new block of `size` bytes.
        unsafe { write_field_entry(block.as_ptr(), field, value, ttl) };
        self.add(h, block.as_ptr());
        true
    }

    /// Set the value and TTL of a field that is there; false (and nothing
    /// changed) if it is not.
    pub fn replace(&mut self, field: &[u8], value: &[u8], ttl: Option<u64>) -> bool {
        let h = self.hash(field);
        // SAFETY: as in `get`.
        let Some(found) = self.find(h, |slot| unsafe { field_is(slot, field) }) else {
            return false;
        };
        self.rewrite(found, field, value, ttl);
        if self.resizing() {
            self.step_resize(WRITE_STEP);
        }
        true
    }

    /// Write a found field's new value: in place if the entry's size is the
    /// same, else in a new block.
    fn rewrite(&mut self, found: Found, field: &[u8], value: &[u8], ttl: Option<u64>) {
        let old = found.value();
        // SAFETY: a live hash entry.
        let old_size = unsafe { field_header(old) }.size();
        let size = field_header_for(field.len(), value.len(), ttl.is_some()).size();
        if size == old_size {
            // SAFETY: the entry is `size` bytes, and `field`/`value` are the
            // caller's, not the entry's (`&mut self` rules out a borrow of
            // it).
            unsafe { write_field_entry(old, field, value, ttl) };
            return;
        }
        let block = self.new_block(size);
        // SAFETY: a new block of `size` bytes.
        unsafe {
            write_field_entry(block.as_ptr(), field, value, ttl);
            (*found.group).slots[found.slot] = block.as_ptr();
            self.free_element(old);
        }
    }

    /// Remove the field; true if it was there.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        let h = self.hash(field);
        // SAFETY: as in `get`.
        let Some(found) = self.find(h, |slot| unsafe { field_is(slot, field) }) else {
            return false;
        };
        let slot = self.take_out(found);
        // SAFETY: out of the table.
        unsafe { self.free_element(slot) };
        self.after_remove();
        true
    }

    /// Every field, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = FieldRef<'_>> + '_ {
        // SAFETY: live entries, unchanged while `self` is borrowed.
        self.slots().map(|slot| unsafe { field_ref(slot) })
    }

    /// Calls `f` with every field whose position is at or after `cursor`, a
    /// home group (and its chain) at a time, until at least `count` were
    /// visited; returns the next cursor, 0 at the end. A growth between two
    /// calls misses and repeats nothing; a shrink may repeat, never miss.
    pub fn scan(&self, cursor: u64, count: usize, mut f: impl FnMut(FieldRef<'_>)) -> u64 {
        // SAFETY: as in `iter`.
        self.scan_raw(cursor, count, |slot| f(unsafe { field_ref(slot) }))
            .0
    }

    /// [`Self::scan`], and how many groups it read.
    #[doc(hidden)]
    pub fn scan_counted(
        &self,
        cursor: u64,
        count: usize,
        mut f: impl FnMut(FieldRef<'_>),
    ) -> (u64, usize) {
        // SAFETY: as in `iter`.
        self.scan_raw(cursor, count, |slot| f(unsafe { field_ref(slot) }))
    }

    /// A field picked uniformly at random.
    pub fn random(&self, rng: &mut impl Rng) -> Option<FieldRef<'_>> {
        // SAFETY: as in `iter`.
        self.random_slot(rng).map(|slot| unsafe { field_ref(slot) })
    }
}

// ---------------------------------------------------------------------------
// Sets
// ---------------------------------------------------------------------------

/// What a member is compared as: its inline encoding (members of at most 7
/// bytes are always inline), or its bytes (longer ones never are).
#[derive(Clone, Copy)]
enum Probe<'a> {
    Inline(*mut u8),
    Bytes(&'a [u8]),
}

impl<'a> Probe<'a> {
    #[inline]
    fn of(member: &'a [u8]) -> Self {
        match inline_member(member) {
            Some(slot) => Probe::Inline(slot),
            None => Probe::Bytes(member),
        }
    }

    /// # Safety
    /// `slot` holds a live set element.
    #[inline]
    unsafe fn is(&self, slot: *mut u8) -> bool {
        match *self {
            Probe::Inline(word) => slot.addr() == word.addr(),
            // SAFETY: the caller's promise; not inline, so an entry.
            Probe::Bytes(bytes) => !is_inline(slot) && unsafe { member_of(slot) } == bytes,
        }
    }
}

impl<A: Memory> CollectionTable<Members, A> {
    #[inline]
    pub fn contains(&self, member: &[u8]) -> bool {
        let probe = Probe::of(member);
        // SAFETY: the table's slots hold set elements.
        self.find(self.hash(member), |slot| unsafe { probe.is(slot) })
            .is_some()
    }

    /// Add the member; true if it is new.
    pub fn insert(&mut self, member: &[u8]) -> bool {
        let h = self.hash(member);
        let probe = Probe::of(member);
        // SAFETY: as in `contains`.
        if self.find(h, |slot| unsafe { probe.is(slot) }).is_some() {
            return false;
        }
        let slot = match probe {
            Probe::Inline(slot) => slot,
            Probe::Bytes(bytes) => {
                let block = self.new_block(member_size(bytes.len()));
                // SAFETY: a new block of that size.
                unsafe { write_member_entry(block.as_ptr(), bytes) };
                block.as_ptr()
            }
        };
        self.add(h, slot);
        true
    }

    /// Remove the member; true if it was there.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        let h = self.hash(member);
        let probe = Probe::of(member);
        // SAFETY: as in `contains`.
        let Some(found) = self.find(h, |slot| unsafe { probe.is(slot) }) else {
            return false;
        };
        let slot = self.take_out(found);
        // SAFETY: out of the table.
        unsafe { self.free_element(slot) };
        self.after_remove();
        true
    }

    /// Every member, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = MemberRef<'_>> + '_ {
        // SAFETY: live elements, unchanged while `self` is borrowed.
        self.slots().map(|slot| unsafe { member_ref(slot) })
    }

    /// As [`CollectionTable::<Fields>::scan`], for members.
    pub fn scan(&self, cursor: u64, count: usize, mut f: impl FnMut(MemberRef<'_>)) -> u64 {
        // SAFETY: as in `iter`.
        self.scan_raw(cursor, count, |slot| f(unsafe { member_ref(slot) }))
            .0
    }

    /// [`Self::scan`], and how many groups it read.
    #[doc(hidden)]
    pub fn scan_counted(
        &self,
        cursor: u64,
        count: usize,
        mut f: impl FnMut(MemberRef<'_>),
    ) -> (u64, usize) {
        // SAFETY: as in `iter`.
        self.scan_raw(cursor, count, |slot| f(unsafe { member_ref(slot) }))
    }

    /// A member picked uniformly at random.
    pub fn random(&self, rng: &mut impl Rng) -> Option<MemberRef<'_>> {
        // SAFETY: as in `iter`.
        self.random_slot(rng)
            .map(|slot| unsafe { member_ref(slot) })
    }
}

/// The cursor position of `key` in a table: the order `scan` visits in. For
/// tests of the cursor.
#[cfg(test)]
impl<K: Kind, A: Memory> CollectionTable<K, A> {
    fn position_of(&self, key: &[u8]) -> u64 {
        position(self.hash(key))
    }

    /// Every invariant, checked the slow way.
    fn check(&self) {
        let mut count = 0usize;
        let mut entry_bytes = 0usize;
        let mut chain_groups = 0usize;
        let mut check_array = |array: &Array, from: usize, main: bool| {
            for index in 0..array.len() {
                let mut group = array.group(index);
                let mut depth = 0;
                // SAFETY: the table's groups.
                unsafe {
                    loop {
                        depth += 1;
                        assert!(depth < 1 << 20, "a chain loops");
                        let g = &*group;
                        if index < from {
                            assert_eq!(g.ctrl, 0, "a moved group holds something");
                        }
                        assert_eq!(g.ctrl & HAS_LINK != 0, g.control(LINK) == LINK_BYTE as u8);
                        for i in 0..SLOTS {
                            let c = g.control(i);
                            if i == LINK && g.ctrl & HAS_LINK != 0 {
                                continue;
                            }
                            assert!(c == 0 || c & 0x80 != 0, "control byte {c:#x}");
                            if c & 0x80 != 0 {
                                let slot = g.slots[i];
                                let h = K::hash_of(slot, &self.hasher);
                                assert_eq!(fingerprint(h), c);
                                assert_eq!(array.home(h), index, "an element away from home");
                                if let Some(layout) = K::block(slot) {
                                    assert_eq!(slot.addr() % ENTRY_ALIGN, 0);
                                    entry_bytes += A::charged(layout);
                                }
                                count += 1;
                            }
                        }
                        let next = g.link();
                        if next.is_null() {
                            break;
                        }
                        if main {
                            chain_groups += 1;
                        }
                        group = next;
                    }
                }
            }
        };
        check_array(&self.main(), 0, true);
        if let Some(old) = self.rare.as_ref().and_then(|rare| rare.old.as_ref()) {
            check_array(&old.array, old.next, false);
        }
        assert_eq!(count, self.len);
        assert_eq!(entry_bytes, self.entry_bytes);
        let live = self.rare.as_ref().map_or(0, |rare| rare.arena.live);
        assert_eq!(chain_groups, live, "overflow groups in chains vs the arena");
        if !self.resizing() {
            assert!(self.len <= self.grow_at.max(1) || self.main().bits == MAX_BITS);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::cell::Cell;
    use std::collections::{HashMap, HashSet};

    thread_local! {
        static BLOCKS: Cell<isize> = const { Cell::new(0) };
        static CHUNKS: Cell<isize> = const { Cell::new(0) };
        static BYTES: Cell<isize> = const { Cell::new(0) };
    }

    /// The system's memory, counted per thread: entries and arena chunks
    /// apart (a group array is counted as a chunk).
    struct Counted;

    // SAFETY: `SystemMemory`'s, with counters.
    unsafe impl Memory for Counted {
        fn alloc(layout: Layout, zeroed: bool) -> NonNull<u8> {
            let counter = if layout.align() == ENTRY_ALIGN {
                &BLOCKS
            } else {
                &CHUNKS
            };
            counter.with(|c| c.set(c.get() + 1));
            BYTES.with(|c| c.set(c.get() + layout.size() as isize));
            SystemMemory::alloc(layout, zeroed)
        }

        unsafe fn free(block: NonNull<u8>, layout: Layout) {
            let counter = if layout.align() == ENTRY_ALIGN {
                &BLOCKS
            } else {
                &CHUNKS
            };
            counter.with(|c| c.set(c.get() - 1));
            BYTES.with(|c| c.set(c.get() - layout.size() as isize));
            // SAFETY: the caller's promise.
            unsafe { SystemMemory::free(block, layout) }
        }
    }

    fn live() -> (isize, isize, isize) {
        (
            BLOCKS.with(Cell::get),
            CHUNKS.with(Cell::get),
            BYTES.with(Cell::get),
        )
    }

    type HashT = CollectionTable<Fields, Counted>;
    type SetT = CollectionTable<Members, Counted>;

    /// Scale a test down under Miri.
    fn scale(n: usize) -> usize {
        if cfg!(miri) { (n / 40).max(8) } else { n }
    }

    fn field_name(rng: &mut StdRng, universe: usize) -> Vec<u8> {
        let i = rng.gen_range(0..universe);
        match i % 4 {
            0 | 1 => format!("f{i}").into_bytes(),
            2 => format!("field-of-fourteen-{i}").into_bytes(),
            _ => {
                let mut long = format!("long-{i}-").into_bytes();
                long.resize(130 + i % 200, b'x');
                long
            }
        }
    }

    fn value_for(rng: &mut StdRng) -> Vec<u8> {
        match rng.gen_range(0..10) {
            0..=5 => vec![b'v'; rng.gen_range(0..40)],
            6..=8 => vec![b'w'; rng.gen_range(100..140)],
            _ => vec![b'z'; rng.gen_range(200..1000)],
        }
    }

    #[derive(Default)]
    struct Seen {
        growing: usize,
        shrinking: usize,
    }

    fn note_resize<K: Kind, A: Memory>(table: &CollectionTable<K, A>, seen: &mut Seen) {
        if let Some(old) = table.rare.as_ref().and_then(|r| r.old.as_ref()) {
            if old.array.bits < table.bits {
                seen.growing += 1;
            } else {
                seen.shrinking += 1;
            }
        }
    }

    fn check_hash(table: &HashT, model: &HashMap<Vec<u8>, (Vec<u8>, Option<u64>)>) {
        table.check();
        assert_eq!(table.len(), model.len());
        for (field, (value, ttl)) in model {
            let got = table.get(field).expect("a field of the model");
            assert_eq!(got.field, &field[..]);
            assert_eq!(got.value, &value[..]);
            assert_eq!(got.ttl, *ttl);
            assert_eq!(got.ttl_position, ttl.map(|_| NOT_LISTED));
        }
        let mut seen = HashSet::new();
        for entry in table.iter() {
            assert!(seen.insert(entry.field.to_vec()), "iter repeats");
            assert!(model.contains_key(entry.field));
        }
        assert_eq!(seen.len(), model.len());
    }

    #[test]
    fn hash_model() {
        let (blocks0, chunks0, _) = live();
        let mut rng = StdRng::seed_from_u64(764);
        let mut table = HashT::with_capacity_and_seed(0, 1);
        let mut model: HashMap<Vec<u8>, (Vec<u8>, Option<u64>)> = HashMap::new();
        let mut seen = Seen::default();
        let universe = scale(40_000);
        let rounds = if cfg!(miri) { 2 } else { 4 };
        for round in 0..rounds {
            // Up to many fields, then down to a few: growths, then shrinks.
            let high = universe / 2 + round * universe / 10;
            let mut ops = 0usize;
            while model.len() < high || ops < universe {
                ops += 1;
                let field = field_name(&mut rng, universe);
                match rng.gen_range(0..100) {
                    0..=59 => {
                        let value = value_for(&mut rng);
                        let ttl = rng.gen_bool(0.3).then(|| rng.r#gen::<u64>());
                        let new = table.insert(&field, &value, ttl);
                        assert_eq!(new, model.insert(field, (value, ttl)).is_none());
                    }
                    60..=69 => {
                        let value = value_for(&mut rng);
                        let ttl = rng.gen_bool(0.5).then_some(7);
                        let there = table.replace(&field, &value, ttl);
                        assert_eq!(there, model.contains_key(&field));
                        if there {
                            model.insert(field, (value, ttl));
                        }
                    }
                    70..=84 => {
                        assert_eq!(table.remove(&field), model.remove(&field).is_some());
                    }
                    _ => {
                        let got = table.get(&field).map(|e| (e.value.to_vec(), e.ttl));
                        assert_eq!(got, model.get(&field).cloned());
                    }
                }
                if table.resizing() && ops.is_multiple_of(97) {
                    note_resize(&table, &mut seen);
                    check_hash(&table, &model);
                }
            }
            check_hash(&table, &model);
            // Down to a few.
            let keep = universe / 50;
            let fields: Vec<Vec<u8>> = model.keys().cloned().collect();
            for (n, field) in fields.iter().enumerate() {
                if model.len() <= keep {
                    break;
                }
                assert!(table.remove(field));
                model.remove(field);
                if table.resizing() && n.is_multiple_of(31) {
                    note_resize(&table, &mut seen);
                    check_hash(&table, &model);
                }
            }
            check_hash(&table, &model);
        }
        assert!(
            seen.growing > 0 && seen.shrinking > 0,
            "growing {} shrinking {}",
            seen.growing,
            seen.shrinking
        );
        // The background task's steps finish a resize.
        while !table.step_resize(3) {}
        check_hash(&table, &model);
        drop(table);
        assert_eq!(live().0, blocks0);
        assert_eq!(live().1, chunks0);
    }

    fn member_for(rng: &mut StdRng, universe: usize) -> Vec<u8> {
        let i = rng.gen_range(0..universe);
        match i % 5 {
            0 | 1 => i.to_string().into_bytes(), // inline up to 7 digits
            2 => format!("{i:0>7}").into_bytes()[..rng.gen_range(0..=7)].to_vec(), // inline, any length 0..=7
            3 => format!("member-{i:0>7}").into_bytes(),                           // 14 bytes
            _ => {
                let mut long = format!("long-member-{i}-").into_bytes();
                long.resize(128 + i % 300, b'm');
                long
            }
        }
    }

    fn check_set(table: &SetT, model: &HashSet<Vec<u8>>) {
        table.check();
        assert_eq!(table.len(), model.len());
        for member in model {
            assert!(table.contains(member), "missing {member:?}");
        }
        let mut seen = HashSet::new();
        for member in table.iter() {
            assert!(seen.insert(member.to_vec()), "iter repeats");
            assert!(model.contains(&member[..]));
        }
        assert_eq!(seen.len(), model.len());
    }

    #[test]
    fn set_model() {
        let (blocks0, chunks0, _) = live();
        let mut rng = StdRng::seed_from_u64(17);
        let mut table = SetT::with_capacity_and_seed(0, 2);
        let mut model = HashSet::new();
        let mut seen = Seen::default();
        let universe = scale(60_000);
        let rounds = if cfg!(miri) { 2 } else { 4 };
        for round in 0..rounds {
            let high = universe / 3 + round * universe / 12;
            let mut ops = 0usize;
            while model.len() < high || ops < universe {
                ops += 1;
                let member = member_for(&mut rng, universe);
                match rng.gen_range(0..100) {
                    0..=59 => assert_eq!(table.insert(&member), model.insert(member)),
                    60..=79 => assert_eq!(table.remove(&member), model.remove(&member)),
                    _ => assert_eq!(table.contains(&member), model.contains(&member)),
                }
                if table.resizing() && ops.is_multiple_of(89) {
                    note_resize(&table, &mut seen);
                    check_set(&table, &model);
                }
            }
            check_set(&table, &model);
            let members: Vec<Vec<u8>> = model.iter().cloned().collect();
            for (n, member) in members.iter().enumerate() {
                if model.len() <= universe / 60 {
                    break;
                }
                assert!(table.remove(member));
                model.remove(member);
                if table.resizing() && n.is_multiple_of(29) {
                    note_resize(&table, &mut seen);
                    check_set(&table, &model);
                }
            }
            check_set(&table, &model);
        }
        assert!(
            seen.growing > 0 && seen.shrinking > 0,
            "growing {} shrinking {}",
            seen.growing,
            seen.shrinking
        );
        drop(table);
        assert_eq!(live().0, blocks0);
        assert_eq!(live().1, chunks0);
    }

    #[test]
    fn pack7_is_a_copy() {
        let src = [0x01u8, 0x80, 0xff, 0x7f, 0x00, 0x42, 0x99];
        for len in 0..=7 {
            let mut bytes = [0u8; 8];
            bytes[..len].copy_from_slice(&src[..len]);
            assert_eq!(pack7(&src[..len]), u64::from_le_bytes(bytes), "{len}");
        }
    }

    #[test]
    fn entry_encodings() {
        let mut table = HashT::with_capacity_and_seed(0, 3);
        let long = vec![b'L'; 300];
        let cases: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = vec![
            (b"".to_vec(), b"".to_vec(), None),
            (b"a".to_vec(), b"".to_vec(), Some(0)),
            (vec![b'f'; 127], vec![b'v'; 127], Some(u64::MAX)),
            (vec![b'f'; 128], b"v".to_vec(), None),
            (b"g".to_vec(), long.clone(), Some(5)),
            (long.clone(), long.clone(), None),
        ];
        for (f, v, t) in &cases {
            assert!(table.insert(f, v, *t));
        }
        for (f, v, t) in &cases {
            let e = table.get(f).unwrap();
            assert_eq!((e.field, e.value, e.ttl), (&f[..], &v[..], *t));
        }
        // Sizes: #764's fields are 2 + 8 + 1 = 11 bytes.
        assert_eq!(field_header_for(8, 1, false).size(), 11);
        assert_eq!(field_header_for(14, 32, false).size(), 48);
        assert_eq!(field_header_for(14, 32, true).size(), 60);
        assert_eq!(field_header_for(128, 1, false).size(), 2 + 8 + 129);
        assert_eq!(member_size(14), 15);
        assert_eq!(member_size(128), 133);
        // Inline members: every length 0..=7, and they differ from longer ones.
        let mut set = SetT::with_capacity_and_seed(0, 3);
        for len in 0..=9 {
            let m = &b"123456789"[..len];
            assert!(set.insert(m));
            assert!(!set.insert(m));
        }
        for len in 0..=9 {
            assert!(set.contains(&b"123456789"[..len]));
        }
        assert!(!set.contains(b"12345678\0"));
        assert!(!set.contains(b"\0"));
        assert_eq!(
            set.iter()
                .filter(|m| matches!(m, MemberRef::Inline(..)))
                .count(),
            8
        );
        assert_eq!(set.entry_bytes, 9 + 10);
    }

    /// `allocation_size` is what the counting global allocator counts.
    #[test]
    fn allocation_size_is_exact() {
        let fields: Vec<Vec<u8>> = (0..scale(20_000))
            .map(|i| format!("f{i}").into_bytes())
            .collect();
        let members: Vec<Vec<u8>> = (0..scale(20_000))
            .map(|i| {
                if i.is_multiple_of(2) {
                    i.to_string().into_bytes()
                } else {
                    format!("member-{i:0>7}").into_bytes()
                }
            })
            .collect();
        let Some(before) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        let mut hash = CollectionTable::<Fields>::with_capacity_and_seed(0, 4);
        let mut set = CollectionTable::<Members>::with_capacity_and_seed(0, 4);
        for (n, (f, m)) in fields.iter().zip(&members).enumerate() {
            hash.insert(f, b"v", n.is_multiple_of(3).then_some(1));
            set.insert(m);
            if n.is_multiple_of(1001) || hash.resizing() && n.is_multiple_of(7) {
                let now = crate::memory_use::allocated_by_this_thread().unwrap();
                assert_eq!(
                    (now - before) as usize,
                    hash.allocation_size() + set.allocation_size()
                );
            }
        }
        for (n, (f, m)) in fields.iter().zip(&members).enumerate() {
            hash.remove(f);
            set.remove(m);
            if n.is_multiple_of(1003) || set.resizing() && n.is_multiple_of(5) {
                let now = crate::memory_use::allocated_by_this_thread().unwrap();
                assert_eq!(
                    (now - before) as usize,
                    hash.allocation_size() + set.allocation_size()
                );
            }
        }
        drop(hash);
        drop(set);
        assert_eq!(
            crate::memory_use::allocated_by_this_thread().unwrap(),
            before
        );
    }

    /// Every entry block and every chunk (arena or array) is freed, whatever
    /// state the table is dropped in.
    #[test]
    fn drop_frees_everything() {
        let (blocks0, chunks0, bytes0) = live();
        let n = scale(30_000);
        for stop in [10, n / 3, n] {
            let mut hash = HashT::with_capacity_and_seed(0, 5);
            let mut set = SetT::with_capacity_and_seed(0, 5);
            for i in 0..stop {
                hash.insert(
                    format!("f{i}").as_bytes(),
                    &vec![b'v'; i % 300],
                    i.is_multiple_of(2).then_some(9),
                );
                set.insert(i.to_string().as_bytes());
                set.insert(format!("member-{i:0>7}").as_bytes());
            }
            for i in 0..stop / 2 {
                hash.remove(format!("f{i}").as_bytes());
                set.remove(format!("member-{i:0>7}").as_bytes());
            }
            assert!(live().0 > blocks0);
            assert_eq!(
                live().2 - bytes0,
                (hash.allocation_size() + set.allocation_size()) as isize
                    - extras(&hash)
                    - extras(&set)
            );
            drop(hash);
            drop(set);
            assert_eq!(live(), (blocks0, chunks0, bytes0), "after {stop}");
        }
        // Dropped in the middle of a resize.
        let mut set = SetT::with_capacity_and_seed(0, 6);
        let mut i = 0;
        while !set.resizing() || set.len() < 20 {
            set.insert(format!("member-{i:0>7}").as_bytes());
            i += 1;
        }
        drop(set);
        assert_eq!(live(), (blocks0, chunks0, bytes0));
    }

    /// What `allocation_size` counts besides `Memory`'s blocks: the rare
    /// part's box and the arenas' chunk lists.
    fn extras<K: Kind, A: Memory>(table: &CollectionTable<K, A>) -> isize {
        let Some(rare) = table.rare.as_ref() else {
            return 0;
        };
        let list = |a: &Arena| a.chunks.capacity() * std::mem::size_of::<(NonNull<Group>, usize)>();
        (std::mem::size_of::<Rare>()
            + list(&rare.arena)
            + rare.old.as_ref().map_or(0, |o| list(&o.arena))) as isize
    }

    /// One full scan of `table`, with `between` called after each page.
    fn scan_all(
        table: &mut SetT,
        count: usize,
        mut between: impl FnMut(&mut SetT, usize),
    ) -> (HashMap<Vec<u8>, usize>, usize) {
        let mut returned: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut cursor = 0;
        let mut pages = 0;
        loop {
            let mut page = 0usize;
            let (next, groups) = table.scan_counted(cursor, count, |m| {
                *returned.entry(m.to_vec()).or_default() += 1;
                page += 1;
            });
            // Cost per page: about the page, not the table.
            let unit = table
                .rare
                .as_ref()
                .and_then(|r| r.old.as_ref())
                .map_or(1, |o| 1 + (1usize << o.array.bits.abs_diff(table.bits)));
            assert!(
                groups <= count * 10 + 64 * unit,
                "{groups} groups for a page of {count}"
            );
            pages += 1;
            between(table, pages);
            if next == 0 {
                break;
            }
            assert!(next > cursor, "the cursor goes forward");
            cursor = next;
        }
        (returned, pages)
    }

    #[test]
    fn scan_without_resizes_returns_each_once() {
        let n = scale(20_000);
        let mut table = SetT::with_capacity_and_seed(0, 7);
        for i in 0..n {
            table.insert(format!("member-{i}").as_bytes());
        }
        while !table.step_resize(usize::MAX) {}
        let bits = table.bits;
        // Writes between pages that resize nothing: other members come and go.
        let mut extra = 0usize;
        let (returned, pages) = scan_all(&mut table, 10, |t, page| {
            if page.is_multiple_of(2) {
                t.insert(format!("extra-{extra}").as_bytes());
                extra += 1;
            } else if extra > 0 {
                t.remove(format!("extra-{}", extra - 1).as_bytes());
            }
        });
        assert_eq!(table.bits, bits, "no resize");
        for i in 0..n {
            assert_eq!(
                returned.get(format!("member-{i}").as_bytes()),
                Some(&1),
                "member-{i}"
            );
        }
        assert!(pages > n / 30, "{pages} pages");
    }

    #[test]
    fn scan_across_growth_and_shrink_misses_nothing() {
        let n = scale(300);
        let (mut all_growths, mut all_shrinks) = (0usize, 0usize);
        for (seed, count) in [(8u64, 10usize), (9, 1), (10, 100)] {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut table = SetT::with_capacity_and_seed(0, seed);
            for i in 0..n {
                table.insert(format!("stay-{i}").as_bytes());
            }
            // Between pages: hundreds of others come, then go, and so on:
            // growths and shrinks, and pages in the middle of both.
            let mut added = 0usize;
            let (mut growths, mut shrinks) = (0usize, 0usize);
            let mut was = table.bits;
            let swing = scale(400);
            let (returned, _) = scan_all(&mut table, count, |t, page| {
                let removing = page / (30 * 10 / count.max(1)).max(3) % 2 == 1;
                for _ in 0..rng.gen_range(1..swing) {
                    if removing && added > 0 {
                        added -= 1;
                        t.remove(format!("temp-{added}").as_bytes());
                    } else if !removing {
                        t.insert(format!("temp-{added}").as_bytes());
                        added += 1;
                    }
                }
                if t.bits > was {
                    growths += 1;
                } else if t.bits < was {
                    shrinks += 1;
                }
                was = t.bits;
            });
            for i in 0..n {
                assert!(
                    returned.contains_key(format!("stay-{i}").as_bytes()),
                    "missed stay-{i} (seed {seed})"
                );
            }
            all_growths += growths;
            all_shrinks += shrinks;
        }
        if !cfg!(miri) {
            assert!(
                all_growths >= 2 && all_shrinks >= 2,
                "{all_growths} growths, {all_shrinks} shrinks"
            );
        }
        // A shrink alone between two pages: repeats allowed, nothing missed.
        let mut table = SetT::with_capacity_and_seed(0, 11);
        for i in 0..scale(20_000) {
            table.insert(format!("m-{i}").as_bytes());
        }
        let keep = scale(500);
        let mut shrunk = false;
        let (returned, _) = scan_all(&mut table, 10, |t, page| {
            if page == 5 {
                for i in keep..scale(20_000) {
                    t.remove(format!("m-{i}").as_bytes());
                }
                shrunk = true;
            }
        });
        assert!(shrunk);
        for i in 0..keep {
            assert!(returned.contains_key(format!("m-{i}").as_bytes()));
        }
    }

    /// Chi-square of `random` over `n` members: `per` picks each expected.
    fn chi_square(table: &SetT, members: &[Vec<u8>], per: usize, seed: u64) {
        let index: HashMap<&[u8], usize> = members
            .iter()
            .enumerate()
            .map(|(i, m)| (&m[..], i))
            .collect();
        let mut counts = vec![0usize; members.len()];
        let mut rng = StdRng::seed_from_u64(seed);
        for _ in 0..members.len() * per {
            let m = table.random(&mut rng).expect("not empty");
            counts[index[&m[..]]] += 1;
        }
        let expected = per as f64;
        let chi: f64 = counts
            .iter()
            .map(|&c| (c as f64 - expected).powi(2) / expected)
            .sum();
        let df = (members.len() - 1) as f64;
        // Mean df, standard deviation sqrt(2 df): six of them is far out.
        assert!(
            chi < df + 6.0 * (2.0 * df).sqrt(),
            "chi-square {chi:.0} for {df} degrees of freedom"
        );
        assert!(
            chi > df - 6.0 * (2.0 * df).sqrt(),
            "chi-square {chi:.0} too good for {df}"
        );
    }

    #[test]
    fn random_is_uniform() {
        let n = scale(2_000);
        let per = if cfg!(miri) { 20 } else { 60 };
        let mut table = SetT::with_capacity_and_seed(0, 12);
        // Inline members and entries, and overflow chains.
        let members: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                if i.is_multiple_of(2) {
                    i.to_string().into_bytes()
                } else {
                    format!("member-{i}").into_bytes()
                }
            })
            .collect();
        for m in &members {
            table.insert(m);
        }
        while !table.step_resize(usize::MAX) {}
        chi_square(&table, &members, per, 1);

        // Sparse: just above the shrink point (1/8 of the main slots).
        let big = scale(40_000);
        let mut table = SetT::with_capacity_and_seed(0, 13);
        let all: Vec<Vec<u8>> = (0..big)
            .map(|i| format!("member-{i}").into_bytes())
            .collect();
        for m in &all {
            table.insert(m);
        }
        while !table.step_resize(usize::MAX) {}
        let slots = SLOTS << table.bits;
        let keep = slots / 8 + 1;
        for m in &all[keep..] {
            table.remove(m);
        }
        while !table.step_resize(usize::MAX) {}
        assert_eq!(SLOTS << table.bits, slots, "not shrunk yet");
        assert_eq!(table.len(), keep);
        chi_square(&table, &all[..keep], per, 2);

        // In the middle of a resize: both arrays.
        let mut table = SetT::with_capacity_and_seed(0, 14);
        let mut members = Vec::new();
        let mut i = 0;
        while !(table.resizing() && table.len() > scale(1_000)) {
            let m = format!("member-{i}").into_bytes();
            table.insert(&m);
            members.push(m);
            i += 1;
        }
        assert!(table.resizing());
        chi_square(&table, &members, per, 3);
    }

    #[test]
    fn hash_random_and_scan() {
        let mut table = HashT::with_capacity_and_seed(0, 15);
        for i in 0..scale(3_000) {
            table.insert(format!("f{i}").as_bytes(), b"v", None);
        }
        let mut rng = StdRng::seed_from_u64(4);
        assert!(table.random(&mut rng).unwrap().field.starts_with(b"f"));
        let mut n = 0;
        let mut cursor = 0;
        loop {
            cursor = table.scan(cursor, 10, |e| {
                assert_eq!(e.value, b"v");
                n += 1;
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(n, scale(3_000));
        assert!(CollectionTable::<Fields>::new().random(&mut rng).is_none());
    }

    /// Fields crafted to share a home group under one seed.
    fn colliding(seed: u64, n: usize, top_bits: u32) -> Vec<Vec<u8>> {
        let probe = HashT::with_capacity_and_seed(0, seed);
        let target = probe.hash(b"anchor") >> (64 - top_bits);
        (0u64..)
            .map(|i| format!("crafted-{i}").into_bytes())
            .filter(|f| probe.hash(f) >> (64 - top_bits) == target)
            .take(n)
            .collect()
    }

    #[test]
    fn long_chains_stay_correct() {
        let n = if cfg!(miri) { 30 } else { 300 };
        let top = if cfg!(miri) { 5 } else { 10 };
        let fields = colliding(16, n, top);
        let mut table = HashT::with_capacity_and_seed(0, 16);
        for f in &fields {
            assert!(table.insert(f, f, None));
        }
        assert!(table.bits <= top as u8);
        let stats = table.stats();
        assert!(
            stats.max_chain * 6 >= n,
            "one chain holds them all: {stats:?}"
        );
        table.check();
        for f in &fields {
            assert_eq!(table.get(f).unwrap().value, &f[..]);
        }
        let mut cursor = 0;
        let mut seen = 0;
        loop {
            cursor = table.scan(cursor, 5, |_| seen += 1);
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen, n);
        for f in &fields[..n / 2] {
            assert!(table.remove(f));
        }
        table.check();
        for f in &fields[n / 2..] {
            assert!(table.contains_key(f));
        }
        // The chain shrinks as it empties.
        assert!(table.stats().max_chain * 6 <= n / 2 + 12);

        // Another seed: the same fields spread out.
        let mut other = HashT::with_capacity_and_seed(0, 17);
        for f in &fields {
            other.insert(f, f, None);
        }
        assert!(other.stats().max_chain <= 3, "{:?}", other.stats());
    }

    #[test]
    fn with_capacity_does_not_grow() {
        let n = scale(10_000);
        let mut table = SetT::with_capacity_and_seed(n, 18);
        let bits = table.bits;
        for i in 0..n {
            table.insert(format!("member-{i}").as_bytes());
        }
        assert_eq!(table.bits, bits);
        assert!(!table.resizing());
        assert!(table.grow_at >= n);
    }

    #[test]
    fn header_fits_in_a_hash_value() {
        // `HashFields` asserts it is no larger than its `IndexMap`.
        assert!(
            std::mem::size_of::<CollectionTable<Fields>>()
                <= std::mem::size_of::<
                    indexmap::IndexMap<String, std::sync::Arc<[u8]>, ahash::RandomState>,
                >()
        );
    }

    #[test]
    fn deferred_free_hands_the_old_array_over() {
        let (blocks0, chunks0, bytes0) = live();
        let mut table = SetT::with_capacity_and_seed(0, 20);
        table.defer_free(true);
        let mut retired = Vec::new();
        let mut i = 0;
        while retired.len() < 3 {
            table.insert(format!("member-{i}").as_bytes());
            i += 1;
            if let Some(r) = table.take_retired() {
                assert!(!table.resizing());
                assert!(r.bytes() > 0);
                retired.push(r);
            }
        }
        table.check();
        let (_, chunks, bytes) = live();
        drop(retired);
        assert!(live().1 < chunks && live().2 < bytes);
        // One not taken is freed with the table, or by the next resize.
        while !table.resizing() {
            table.insert(format!("member-{i}").as_bytes());
            i += 1;
        }
        while !table.step_resize(usize::MAX) {}
        assert!(table.rare.as_ref().unwrap().retired.is_some());
        let before = live().2;
        let size = table.allocation_size();
        table.defer_free(false);
        assert!(live().2 < before);
        assert!(table.allocation_size() < size);
        drop(table);
        assert_eq!(live(), (blocks0, chunks0, bytes0));
        // `allocation_size` counts a retired array until it is taken.
        let names: Vec<Vec<u8>> = (0..scale(5_000))
            .map(|i| format!("m-{i}").into_bytes())
            .collect();
        let Some(start) = crate::memory_use::allocated_by_this_thread() else {
            return;
        };
        let mut table = CollectionTable::<Members>::with_capacity_and_seed(0, 21);
        table.defer_free(true);
        for name in &names {
            table.insert(name);
            let now = crate::memory_use::allocated_by_this_thread().unwrap();
            assert_eq!((now - start) as usize, table.allocation_size());
        }
        drop(table);
        assert_eq!(
            crate::memory_use::allocated_by_this_thread().unwrap(),
            start
        );
    }

    #[test]
    fn step_resize_is_bounded() {
        let mut table = SetT::with_capacity_and_seed(0, 19);
        let mut i = 0;
        while !table.resizing() {
            table.insert(format!("member-{i}").as_bytes());
            i += 1;
        }
        let mut steps = 0;
        while !table.step_resize(1) {
            steps += 1;
            table.check();
        }
        assert!(steps > 1);
        assert!(table.step_resize(1), "nothing left");
    }
}
