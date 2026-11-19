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
//! **Elements.** One block per element, aligned to 8 ([`Memory`] says where
//! from; by default the [`EntryPool`]): a hash entry is `[flen:7|TTL:1][vlen:7|LONG:1]`, then `u32 flen,
//! u32 vlen` when `LONG`, then `[u32 list position][u64 deadline]` when
//! `TTL`, then the field and the value. A set entry is `[len:7|LONG:1]`,
//! `u32 len` when `LONG`, then the member. A set member of at most seven
//! bytes is no block at all: it lives in the slot, tagged by its low bit
//! (a block's address is a multiple of 8). A field, value or member is at
//! most [`MAX_LEN`] bytes; a longer one is refused with [`TooLong`] before
//! anything is allocated.
//!
//! **Hash.** Per table, randomly seeded (`ahash`). The home group is the
//! TOP bits of the hash, the fingerprint its low 7 bits, and an element's
//! position for the cursor is `hash >> 16`: with `2^b` groups, group `g`
//! holds exactly the positions `[g << (48 - b), (g + 1) << (48 - b))`, and a
//! doubling splits `g` into `2g` and `2g + 1` in order. That is what makes
//! [`CollectionTable::scan`] a plain increasing cursor.
//!
//! **Chain cap.** A chain longer than [`CHAIN_CAP`] groups starts a resize
//! with a new seed: an array of the same size, hashed with a fresh random
//! seed, that the elements move into as in any resize. While it is in
//! progress a lookup hashes with both seeds (the new array by the new seed,
//! the old array by the old). Normal fill does not come near the cap: at
//! the fullest point (0.875 of the slots, 6.125 elements a home group on
//! average) a chain of 9 groups needs 50 elements in one home group, about
//! 1e-28 per group under the Poisson model, while the longest chain filling
//! to 3M is 4 groups (see the PR for #764 step 1b).
//!
//! **Scan guarantee.** A full scan, from cursor 0 to cursor 0, returns every
//! element that was there for the whole scan at least once, and returns
//! each exactly once if the table did not shrink meanwhile. This holds
//! except across a resize with a new seed (positions change with the seed),
//! which only a chain over the cap starts: a scan across one still ends,
//! and still never returns a cursor at or before the one it was given, but
//! may miss elements: a write between two of its pages takes a step of the
//! resize, which can move an element from an old-seed position the scan has
//! not reached to a new-seed one it has passed. A scan started after the
//! resize with a new seed has ended has the guarantee again. (One started
//! while it is in progress also returns every element if nothing is written
//! between its pages.)
//!
//! **Resizing.** Above the maximum fill (0.875 of the main slots) the table
//! doubles, below 1/8 it shrinks towards half full, by at most
//! [`MAX_SHRINK_BITS`] bits a resize (it shrinks again once that one is
//! done, if it is still sparse); either way a new array is made and the
//! elements move a few at a time on each write
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
/// ([`Memory::alloc`]) and its elements' entries ([`Memory::alloc_entry`],
/// aligned to [`ENTRY_ALIGN`]). One small interface so that the entries'
/// allocator can change, and so that the tests can count.
///
/// # Safety
/// `alloc` returns a block of `layout`'s size and alignment, zeroed when
/// `zeroed` is true; `alloc_entry` a block of at least `size` bytes aligned
/// to [`ENTRY_ALIGN`]; neither fails silently. `free` takes back a block
/// `alloc` gave for the same layout, `free_entry` one `alloc_entry` gave for
/// the same size.
///
/// A block may be freed on any thread, not only the one that allocated it:
/// a table is `Send` (a key's value is written by whichever thread runs the
/// command, and freed by `lazyfree`'s), and a [`Retired`] array is freed
/// wherever it is dropped.
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
    /// An entry's block: at least `size` bytes, aligned to [`ENTRY_ALIGN`].
    fn alloc_entry(size: usize) -> NonNull<u8> {
        Self::alloc(entry_layout(size), false)
    }
    /// Give back an entry's block.
    ///
    /// # Safety
    /// `block` came from `alloc_entry(size)` and is not used again.
    unsafe fn free_entry(block: NonNull<u8>, size: usize) {
        // SAFETY: the caller's promise; `alloc_entry` asked `alloc` for this
        // layout.
        unsafe { Self::free(block, entry_layout(size)) }
    }
    /// What an entry of `size` bytes counts for in `used_memory`.
    fn entry_charged(size: usize) -> usize {
        Self::charged(entry_layout(size))
    }
}

/// Everything from the system allocator (through the counting global
/// allocator, which counts what is asked for).
pub struct SystemMemory;

// SAFETY: `std::alloc` gives blocks of the layout asked for; failure aborts
// through `handle_alloc_error`. The global allocator frees a block on any
// thread.
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

/// Entries from `value_slab`'s keyspace classes (16 bytes apart), the slabs
/// active defrag empties; the group arrays and arena chunks from the
/// system. Not for the server: compaction cannot find a collection's entry
/// to move it, so a slab holding one could never be emptied. Here so that
/// the class steps can be measured against [`EntryPool`]'s: server code
/// cannot name it (`clippy.toml`).
pub struct SlabMemory;

// SAFETY: a slab block of `class_of(size)` is at least `size` bytes and
// aligned to 16 (so to 8); a larger entry is `SystemMemory`'s. `free_entry`
// picks the same side `alloc_entry` did, from the same size. Slab blocks
// are freed on any thread (`value_slab::free` puts another thread's block
// on the slab's remote list).
#[allow(clippy::disallowed_types)]
unsafe impl Memory for SlabMemory {
    fn alloc(layout: Layout, zeroed: bool) -> NonNull<u8> {
        SystemMemory::alloc(layout, zeroed)
    }

    unsafe fn free(block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller's promise.
        unsafe { SystemMemory::free(block, layout) }
    }

    fn alloc_entry(size: usize) -> NonNull<u8> {
        match crate::value_slab::class_of(size) {
            Some(class) => crate::value_slab::alloc(class),
            None => SystemMemory::alloc(entry_layout(size), false),
        }
    }

    unsafe fn free_entry(block: NonNull<u8>, size: usize) {
        match crate::value_slab::class_of(size) {
            // SAFETY: `alloc_entry` took it from this class.
            Some(class) => unsafe { crate::value_slab::free(block, class) },
            // SAFETY: the caller's promise.
            None => unsafe { SystemMemory::free(block, entry_layout(size)) },
        }
    }

    fn entry_charged(size: usize) -> usize {
        crate::value_slab::class_of(size).map_or(size, crate::value_slab::block_size)
    }
}

/// What tables use: entries from `value_slab`'s entry pool, a set of slab
/// classes of their own (8 bytes apart up to 64, then as the keyspace's)
/// that active defrag never empties, so a collection's entry, which
/// compaction cannot move, never pins a slab defrag is emptying. A block
/// costs, and is counted in `used_memory` at, its class's size; a slab
/// nobody owns gives its pages back as soon as it empties (one a thread
/// owns, when the thread lets go of it: INFO `value_slab_pool_bytes` counts
/// them). Entries over
/// `value_slab::MAX_BLOCK`, the group arrays and the arena chunks come from
/// the system allocator.
pub struct EntryPool;

// SAFETY: a pool block of `pool_class_of(size)` is at least `size` bytes
// and aligned to 8; a larger entry is `SystemMemory`'s. `free_entry` picks
// the same side `alloc_entry` did, from the same size. Slab blocks are
// freed on any thread (`value_slab::free` puts another thread's block on
// the slab's remote list).
unsafe impl Memory for EntryPool {
    fn alloc(layout: Layout, zeroed: bool) -> NonNull<u8> {
        SystemMemory::alloc(layout, zeroed)
    }

    unsafe fn free(block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller's promise.
        unsafe { SystemMemory::free(block, layout) }
    }

    #[inline]
    fn alloc_entry(size: usize) -> NonNull<u8> {
        match crate::value_slab::pool_class_of(size) {
            Some(class) => crate::value_slab::alloc(class),
            None => SystemMemory::alloc(entry_layout(size), false),
        }
    }

    #[inline]
    unsafe fn free_entry(block: NonNull<u8>, size: usize) {
        match crate::value_slab::pool_class_of(size) {
            // SAFETY: `alloc_entry` took it from this class.
            Some(class) => unsafe { crate::value_slab::free(block, class) },
            // SAFETY: the caller's promise.
            None => unsafe { SystemMemory::free(block, entry_layout(size)) },
        }
    }

    #[inline]
    fn entry_charged(size: usize) -> usize {
        crate::value_slab::pool_class_of(size).map_or(size, crate::value_slab::block_size)
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
    chunks: Vec<Chunk>,
    used: usize,
    free: *mut Group,
    /// Groups handed out of the chunks (in chains or free).
    groups: usize,
    /// Groups in chains.
    live: usize,
}

/// A chunk of an arena: `n` groups at `at`, the arena's groups numbered
/// `start..start + n`.
struct Chunk {
    at: NonNull<Group>,
    n: usize,
    start: usize,
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
            let room = self.chunks.last().is_some_and(|c| self.used < c.n);
            if !room {
                let n = self.groups.max(main_groups / 64).clamp(1, Self::MAX_CHUNK);
                let at = A::alloc(Self::chunk_layout(n), false).cast::<Group>();
                // Every earlier chunk is wholly handed out: `groups` is
                // their sum.
                self.chunks.push(Chunk {
                    at,
                    n,
                    start: self.groups,
                });
                self.used = 0;
            }
            let chunk = self.chunks.last().expect("a chunk with room").at;
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

    /// Calls `f` with each run of handed-out groups, in order: the groups
    /// numbered `0..self.groups`, as [`Self::group_at`] numbers them.
    fn runs(&self, mut f: impl FnMut(*mut Group, usize) -> bool) -> bool {
        let last = self.chunks.len().wrapping_sub(1);
        for (i, c) in self.chunks.iter().enumerate() {
            let n = if i == last { self.used } else { c.n };
            if n > 0 && f(c.at.as_ptr(), n) {
                return true;
            }
        }
        false
    }

    /// The chunk group number `i` is in, from the sizes [`Self::take`] gives
    /// chunks: `c`, `c`, `2c`, `4c` ... doubling up to [`Self::MAX_CHUNK`],
    /// then that size, where `c` (a power of two) is the first chunk's.
    #[inline]
    fn chunk_of(&self, i: usize) -> usize {
        let c = self.chunks[0].n;
        let max = Self::MAX_CHUNK;
        if i < c {
            0
        } else if i < max {
            1 + (i / c).ilog2() as usize
        } else {
            i / max + (max / c).ilog2() as usize
        }
    }

    /// Handed-out group number `i` (`i < self.groups`): its chunk worked out
    /// from the chunk sizes, and found by a binary search should that ever
    /// not hold.
    #[inline]
    fn group_at(&self, i: usize) -> *mut Group {
        debug_assert!(i < self.groups);
        let at = self.chunk_of(i);
        let c = match self.chunks.get(at) {
            Some(c) if c.start <= i && i < c.start + c.n => c,
            _ => {
                debug_assert!(false, "group {i} is not in chunk {at}");
                &self.chunks[self.chunks.partition_point(|c| c.start <= i) - 1]
            }
        };
        debug_assert!(i - c.start < c.n);
        // SAFETY: `start <= i < start + n` (every chunk but the last is
        // wholly handed out, and the last up to `used`): in the chunk.
        unsafe { c.at.as_ptr().add(i - c.start) }
    }

    /// What it holds: its chunks and its list of them.
    fn bytes<A: Memory>(&self) -> usize {
        let chunks: usize = self
            .chunks
            .iter()
            .map(|c| A::charged(Self::chunk_layout(c.n)))
            .sum();
        chunks + self.chunks.capacity() * std::mem::size_of::<Chunk>()
    }

    /// Give every chunk back. The groups in them must not be used again.
    fn release<A: Memory>(&mut self) {
        for c in self.chunks.drain(..) {
            // SAFETY: taken with this layout in `take`.
            unsafe { A::free(c.at.cast(), Self::chunk_layout(c.n)) };
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
pub const ENTRY_ALIGN: usize = 8;
/// A field TTL's list position before the hash's TTL list lists it.
pub const NOT_LISTED: u32 = u32::MAX;

/// The longest field, value or member a table holds, in bytes: its length
/// is kept in a `u32`. (The server's own limit, `proto-max-bulk-len`, is
/// 512 MiB by default.)
pub const MAX_LEN: usize = u32::MAX as usize;

/// A field, value or member longer than [`MAX_LEN`]: refused, with nothing
/// allocated and nothing changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooLong;

impl std::fmt::Display for TooLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a field, value or member is longer than {MAX_LEN} bytes")
    }
}

impl std::error::Error for TooLong {}

/// The limit [`check_len`] applies: [`MAX_LEN`], lowered in tests so the
/// limit can be tried without 4 GiB of bytes.
#[cfg(not(test))]
#[inline(always)]
fn max_len() -> usize {
    MAX_LEN
}

#[cfg(test)]
thread_local! {
    static TEST_MAX_LEN: std::cell::Cell<usize> = const { std::cell::Cell::new(MAX_LEN) };
}

#[cfg(test)]
fn max_len() -> usize {
    TEST_MAX_LEN.with(std::cell::Cell::get)
}

/// Whether a length is one a table holds: checked before any block is
/// allocated.
#[inline]
fn check_len(len: usize) -> Result<(), TooLong> {
    if len <= max_len() {
        Ok(())
    } else {
        Err(TooLong)
    }
}

#[inline]
fn entry_layout(size: usize) -> Layout {
    // An entry is at most 2 + 8 + 12 + 2 * MAX_LEN bytes: it fits.
    Layout::from_size_align(size, ENTRY_ALIGN).expect("an entry fits in memory")
}

/// A length [`check_len`] let through, as the `u32` an entry keeps.
#[inline]
fn len_u32(len: usize) -> u32 {
    debug_assert!(len <= MAX_LEN);
    len as u32
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

/// Write a hash entry; `ttl` is the field's deadline and its TTL list
/// position.
///
/// # Safety
/// `entry` is writable for `field_header_for(field.len(), value.len(),
/// ttl.is_some()).size()` bytes, and overlaps neither slice; both lengths
/// passed [`check_len`].
unsafe fn write_field_entry(entry: *mut u8, field: &[u8], value: &[u8], ttl: Option<(u64, u32)>) {
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
        if let (Some(at), Some((deadline, position))) = (header.ttl_at, ttl) {
            write_bytes(entry.add(at), &position.to_le_bytes());
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
    /// [`CollectionTable::set_ttl_position`] says).
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

    /// The size of the element's block, `None` if it has none.
    ///
    /// # Safety
    /// `slot` holds a live element of this kind.
    #[doc(hidden)]
    unsafe fn block(slot: *mut u8) -> Option<usize>;
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
    unsafe fn block(slot: *mut u8) -> Option<usize> {
        // SAFETY: the caller's promise: a hash entry.
        Some(unsafe { field_header(slot) }.size())
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
    unsafe fn block(slot: *mut u8) -> Option<usize> {
        if is_inline(slot) {
            None
        } else {
            // SAFETY: the caller's promise: a set entry.
            Some(member_size(unsafe { member_of(slot) }.len()))
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

/// A chain longer than this many groups starts a resize with a new seed (see
/// the module docs). Filling to 3M elements at the default fill, the longest
/// chain is 4 groups; a chain of 9 groups holds at least 50 elements of one
/// home group, which at the fullest point (6.125 elements a group on
/// average) the Poisson model puts at about 1e-28 per group.
pub const CHAIN_CAP: usize = 8;

/// A shrink takes the main array down by at most this many bits: a page of
/// [`CollectionTable::scan`] then reads at most four old groups (and their
/// chains) with each new one, and [`CollectionTable::random`]'s picks keep
/// hitting. A table still sparse when a shrink ends shrinks again.
pub const MAX_SHRINK_BITS: u8 = 2;

/// Picks of a random slot before [`CollectionTable::random`] falls back to a
/// walk. The fill is at least 1/8 of the main slots but while a shrink is in
/// progress, so a pick hits an element with probability about 1/8 or more:
/// 128 misses in a row are about one in 25,000 at that fill (arena groups
/// count too), and the walk is a little biased, never wrong.
const RANDOM_PICKS: usize = 128;

/// Header flag: a chain passed [`CHAIN_CAP`] while a resize was in
/// progress; a resize with a new seed starts when that one ends.
const RESEED_DUE: u8 = 1;

/// Resizes with a new seed in a row, with no growth between them, after
/// which a long chain starts none until the table grows: a chain that stays
/// long under three random seeds is not one the seed made, and the table
/// stays correct either way.
pub const MAX_RESEEDS_IN_A_ROW: u8 = 3;

/// The chain cap [`place`]'s callers apply: [`CHAIN_CAP`], lowered in tests
/// to make resizes with a new seed happen.
#[cfg(not(test))]
#[inline(always)]
fn chain_cap() -> usize {
    CHAIN_CAP
}

#[cfg(test)]
thread_local! {
    static TEST_CHAIN_CAP: std::cell::Cell<usize> = const { std::cell::Cell::new(CHAIN_CAP) };
}

#[cfg(test)]
fn chain_cap() -> usize {
    TEST_CHAIN_CAP.with(std::cell::Cell::get)
}

/// The table: a hash's fields or a set's members (`K`), its memory from `A`.
pub struct CollectionTable<K: Kind, A: Memory = EntryPool> {
    /// The main array (see [`Self::main`]): its groups and its size, apart
    /// so that the header packs into 80 bytes.
    groups: NonNull<Group>,
    bits: u8,
    offset: u8,
    /// [`RESEED_DUE`].
    flags: u8,
    len: usize,
    /// The length past which `main` doubles.
    grow_at: usize,
    /// What the elements' entries count for (`A::entry_charged`).
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
    /// Resizes with a new seed so far.
    reseeds: u32,
    /// Resizes with a new seed since the main array last grew.
    reseeds_in_a_row: u8,
    /// A growth [`CollectionTable::reserve`] asked for while a resize was in
    /// progress: to `2^reserve_bits` groups, started when that one ends (0:
    /// none).
    reserve_bits: u8,
}

impl Rare {
    fn new() -> Self {
        Rare {
            arena: Arena::new(),
            old: None,
            retired: None,
            defer_free: false,
            reseeds: 0,
            reseeds_in_a_row: 0,
            reserve_bits: 0,
        }
    }

    /// Whether a chain of `depth` groups makes a resize with a new seed due.
    #[inline]
    fn reseed_due(&self, depth: usize) -> bool {
        rare_reseed_due(depth, self.reseeds_in_a_row)
    }
}

/// The group array and overflow arena a finished resize left behind:
/// freed when dropped, on whatever thread drops it. Freeing them is the
/// one cost of a resize that grows with the table (the 16 MiB array left by
/// growing past 1.6M elements took 0.9 ms to free), so the server can hand
/// them to a thread of its own rather than pay it on a write
/// ([`CollectionTable::defer_free`]).
pub struct Retired<A: Memory = EntryPool> {
    array: Array,
    arena: Arena,
    _memory: PhantomData<A>,
}

// SAFETY: it owns its blocks, and nothing else points into them; `Memory`
// blocks may be freed on any thread.
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
    /// The seed the old array is hashed with, when it is not the table's:
    /// a resize with a new seed.
    hasher: Option<ahash::RandomState>,
}

// SAFETY: the table owns every block its pointers reach, and gives out
// borrows only through `&self` (reads) or `&mut self` (writes): it is as
// thread-safe as a `Vec<Box<[u8]>>`. `Memory` blocks may be freed on any
// thread, so the table may be dropped on another than made it.
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
/// a shrink heads.
fn shrink_bits(n: usize) -> u8 {
    let mut bits = 0;
    while n * 2 > SLOTS << bits {
        bits += 1;
    }
    bits
}

/// [`Rare::reseed_due`], for a caller that has the count borrowed apart.
#[inline]
fn rare_reseed_due(depth: usize, reseeds_in_a_row: u8) -> bool {
    depth > chain_cap() && reseeds_in_a_row < MAX_RESEEDS_IN_A_ROW
}

/// Put an element (`slot`, hash `h`) in `array`'s chain for it, taking an
/// overflow group from `arena` if the chain is full. Returns the groups of
/// the chain it walked, the home group included: the chain's length when
/// the element went to its last group.
///
/// # Safety
/// `array` and `arena` are a table's live main array and its arena, made
/// with `A`; the element is in no array.
unsafe fn place<A: Memory>(array: Array, arena: &mut Arena, h: u64, slot: *mut u8) -> usize {
    let fp = fingerprint(h);
    let mut group = array.group(array.home(h));
    let mut depth = 1;
    // SAFETY: every group reached is the array's or a live overflow group of
    // its chains (links point only to those).
    unsafe {
        loop {
            let empty = (*group).empty();
            if empty != 0 {
                (*group).put(first(empty), fp, slot);
                return depth;
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
                return depth + 1;
            }
            group = next;
            depth += 1;
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
        Self::build(n, ahash::RandomState::new(), MAX_FILL)
    }

    /// With the hash seeded from `seed`, for tests and measurements that
    /// must repeat. Server code must not use it (`clippy.toml` forbids it):
    /// a table's seed is random.
    #[doc(hidden)]
    pub fn with_capacity_and_seed(n: usize, seed: u64) -> Self {
        let hasher = ahash::RandomState::with_seeds(
            seed,
            seed ^ 0x243F_6A88_85A3_08D3,
            seed.rotate_left(17) ^ 0x1319_8A2E_0370_7344,
            seed.rotate_left(41) ^ 0xA409_3822_299F_31D0,
        );
        Self::build(n, hasher, MAX_FILL)
    }

    fn build(n: usize, hasher: ahash::RandomState, max_fill: u16) -> Self {
        let bits = bits_for(n, max_fill);
        let main = Array::alloc::<A>(bits);
        CollectionTable {
            groups: main.groups,
            bits,
            offset: main.offset,
            flags: 0,
            len: 0,
            grow_at: grow_at(bits, max_fill),
            entry_bytes: 0,
            hasher,
            rare: None,
            max_fill,
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

    /// Elements the main array takes before the table grows.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.grow_at
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
    /// then in the old one while a resize is in progress. `rehash` gives the
    /// element's hash under another seed, for an old array of a resize with
    /// a new seed.
    #[inline]
    fn find(
        &self,
        h: u64,
        rehash: impl FnOnce(&ahash::RandomState) -> u64,
        eq: impl Fn(*mut u8) -> bool,
    ) -> Option<Found> {
        let fp = fingerprint(h);
        let head = self.main().group(self.main().home(h));
        // SAFETY: `head` is the main array's.
        if let Some(found) = unsafe { Self::find_in(head, fp, &eq) } {
            return Some(found);
        }
        let old = self.rare.as_ref()?.old.as_ref()?;
        let h = match &old.hasher {
            Some(seed) => rehash(seed),
            None => h,
        };
        let index = old.array.home(h);
        if index < old.next {
            // Moved already, and empty.
            return None;
        }
        // SAFETY: the old array's, not yet moved.
        unsafe { Self::find_in(old.array.group(index), fingerprint(h), &eq) }.map(|found| Found {
            in_old: true,
            ..found
        })
    }

    /// Where the element `slot` (one of the table's) is: looked up by its
    /// own hash and its slot value.
    fn find_slot(&self, slot: *mut u8) -> Option<Found> {
        // SAFETY: a live element of the table's.
        let h = unsafe { K::hash_of(slot, &self.hasher) };
        // SAFETY: as above.
        self.find(h, |seed| unsafe { K::hash_of(slot, seed) }, |s| s == slot)
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
            // Not reached while a resize is in progress, but after
            // `set_max_fill`: a growth's or a shrink's moves finish long
            // before the table reaches its growth point (at WRITE_STEP), and
            // so do those of a resize with a new seed, which starts under 3/4
            // of it or else grows the array too (`start_reseed`). Kept as a
            // safety net: it finishes that resize in one call.
            while self.resizing() {
                self.step_resize(usize::MAX);
            }
            self.start_resize(self.main().bits + 1, false);
        }
        let main = self.main();
        let rare = self.rare.get_or_insert_with(|| Box::new(Rare::new()));
        // SAFETY: the main array and its arena; the element is new.
        let depth = unsafe { place::<A>(main, &mut rare.arena, h, slot) };
        if rare.reseed_due(depth) {
            self.flags |= RESEED_DUE;
        }
        self.len += 1;
        if self.resizing() {
            self.step_resize(WRITE_STEP);
        } else if self.flags & RESEED_DUE != 0 {
            self.start_reseed();
        }
    }

    /// After a write that added nothing: a step of a resize in progress.
    #[inline]
    fn after_write(&mut self) {
        if self.resizing() {
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

    /// Whether the table is sparse enough to shrink: under 1/8 of the main
    /// slots.
    #[inline]
    fn sparse(&self) -> bool {
        self.main().bits > 0 && self.len * 8 < SLOTS << self.main().bits
    }

    /// Start a shrink: to half full, by at most [`MAX_SHRINK_BITS`].
    fn start_shrink(&mut self) {
        let bits = shrink_bits(self.len).max(self.main().bits.saturating_sub(MAX_SHRINK_BITS));
        self.start_resize(bits, false);
    }

    /// After a removal: shrink below 1/8, or take a step of a resize.
    fn after_remove(&mut self) {
        if self.resizing() {
            self.step_resize(WRITE_STEP);
        } else if self.sparse() {
            self.start_shrink();
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

    /// Start a resize with a new seed: into an array of the same size, or
    /// of twice the size if the table is past 3/4 of its growth point (so
    /// that no growth comes due before it ends), or of the size a `reserve`
    /// waiting for the resize asked for.
    fn start_reseed(&mut self) {
        let mut bits = self.main().bits;
        if self.len * 4 > self.grow_at * 3 && bits < MAX_BITS {
            bits += 1;
        }
        if let Some(rare) = self.rare.as_mut() {
            bits = bits.max(std::mem::take(&mut rare.reserve_bits));
        }
        self.start_resize(bits, true);
    }

    /// Start moving the elements to a new array of `2^bits` groups; with a
    /// new random seed if `reseed`.
    fn start_resize(&mut self, bits: u8, reseed: bool) {
        debug_assert!(!self.resizing());
        let new = Array::alloc::<A>(bits);
        let main = self.main();
        let hasher = reseed.then(|| std::mem::replace(&mut self.hasher, ahash::RandomState::new()));
        let rare = self.rare.get_or_insert_with(|| Box::new(Rare::new()));
        if reseed {
            rare.reseeds += 1;
            self.flags &= !RESEED_DUE;
        }
        if bits > main.bits {
            rare.reseeds_in_a_row = 0;
        } else if reseed {
            rare.reseeds_in_a_row = rare.reseeds_in_a_row.saturating_add(1);
        }
        let arena = std::mem::replace(&mut rare.arena, Arena::new());
        rare.old = Some(Old {
            array: main,
            arena,
            next: 0,
            hasher,
        });
        self.groups = new.groups;
        self.bits = new.bits;
        self.offset = new.offset;
        self.grow_at = grow_at(bits, self.max_fill);
    }

    /// Move elements of a resize in progress into the new array: about
    /// `budget` units of work, a unit being a group visited or an element
    /// moved (a home group's chain moves whole). Returns whether no resize
    /// is in progress any more. What the background task calls, in bounded
    /// steps under the collection's write lock.
    ///
    /// When a resize ends, another may start: one with a new seed if a
    /// chain passed [`CHAIN_CAP`], or a further shrink if a shrink left the
    /// table still sparse ([`MAX_SHRINK_BITS`]).
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
            reseeds_in_a_row,
            ..
        } = &mut **rare;
        let Some(o) = old.as_mut() else {
            return true;
        };
        let mut work = 0usize;
        let mut deepest = 0usize;
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
                        debug_assert_eq!(
                            fingerprint(o.hasher.as_ref().map_or(h, |seed| K::hash_of(slot, seed))),
                            (*group).control(i)
                        );
                        deepest = deepest.max(place::<A>(main, arena, h, slot));
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
        if rare_reseed_due(deepest, *reseeds_in_a_row) {
            self.flags |= RESEED_DUE;
        }
        if o.next < o.array.len() {
            return false;
        }
        let o = old.take().expect("a resize in progress");
        let shrank = o.array.bits > main.bits;
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
        let reserved = self.rare.as_ref().map_or(0, |rare| rare.reserve_bits);
        if self.flags & RESEED_DUE != 0 {
            self.start_reseed();
        } else if reserved > self.main().bits {
            if let Some(rare) = self.rare.as_mut() {
                rare.reserve_bits = 0;
            }
            self.start_resize(reserved, false);
        } else if shrank && self.sparse() {
            self.start_shrink();
        }
        !self.resizing()
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

    /// Room for `more` elements besides those there before the table grows
    /// again: a growth to that size starts now, or, if a resize is in
    /// progress, when it ends (its moves are not hurried: no call does more
    /// than a step of them); either way the moves are made a few at a time,
    /// as any resize's. Until a growth waiting so starts, [`Self::capacity`]
    /// does not show it.
    pub fn reserve(&mut self, more: usize) {
        let want = bits_for(self.len.saturating_add(more), self.max_fill);
        if want <= self.main().bits {
            return;
        }
        if self.resizing() {
            let rare = self.rare.as_mut().expect("a resize in progress");
            rare.reserve_bits = rare.reserve_bits.max(want);
            return;
        }
        self.start_resize(want, false);
    }

    /// Remove every element; the table keeps no memory but an empty group.
    pub fn clear(&mut self) {
        let defer = self.rare.as_ref().is_some_and(|rare| rare.defer_free);
        let empty = Self::build(0, ahash::RandomState::new(), self.max_fill);
        drop(std::mem::replace(self, empty));
        if defer {
            self.defer_free(true);
        }
    }

    /// Allocate an entry block of `size` bytes.
    #[inline]
    fn new_block(&mut self, size: usize) -> NonNull<u8> {
        self.entry_bytes += A::entry_charged(size);
        A::alloc_entry(size)
    }

    /// Free an element's block, if it has one.
    ///
    /// # Safety
    /// `slot` held an element of the table's and is in it no more.
    #[inline]
    unsafe fn free_element(&mut self, slot: *mut u8) {
        // SAFETY: the caller's promise.
        if let Some(size) = unsafe { K::block(slot) } {
            self.entry_bytes -= A::entry_charged(size);
            // SAFETY: allocated by `new_block` with this size.
            unsafe { A::free_entry(NonNull::new_unchecked(slot), size) };
        }
    }

    /// Remove every element `keep` says no to.
    fn retain_slots(&mut self, mut keep: impl FnMut(*mut u8) -> bool) {
        let doomed: Vec<*mut u8> = self.slots().filter(|&slot| !keep(slot)).collect();
        for slot in doomed {
            // An element does not change its slot value as it moves.
            let found = self.find_slot(slot).expect("an element of the table");
            let slot = self.take_out(found);
            // SAFETY: out of the table.
            unsafe { self.free_element(slot) };
            self.after_remove();
        }
    }

    /// Calls `f` with each run of groups the table holds, and whether it is
    /// the old array's: the main array, its arena's chunks, the old array and
    /// its arena's, in the order [`Self::pick`] numbers them. Stops when `f`
    /// says so.
    fn runs(&self, mut f: impl FnMut(*mut Group, usize, bool) -> bool) {
        if f(self.main().groups.as_ptr(), self.main().len(), false) {
            return;
        }
        let Some(rare) = self.rare.as_ref() else {
            return;
        };
        if rare.arena.runs(|run, n| f(run, n, false)) {
            return;
        }
        if let Some(old) = rare.old.as_ref() {
            if f(old.array.groups.as_ptr(), old.array.len(), true) {
                return;
            }
            old.arena.runs(|run, n| f(run, n, true));
        }
    }

    /// A slot holding an element, every element equally likely: a uniformly
    /// random physical slot over the arrays and arenas, retried while it is
    /// empty, then a walk. Returns its group, the slot's index and whether
    /// it is in the old array. The groups are counted without a walk (each
    /// arena counts its own), and a group is found from its number at once
    /// in an array and by a binary search of the chunks in an arena.
    fn pick(&self, rng: &mut impl Rng) -> Option<(*mut Group, usize, bool)> {
        if self.len == 0 {
            return None;
        }
        let main = self.main();
        let rare = self.rare.as_deref();
        let arena = rare.map(|rare| &rare.arena);
        let old = rare.and_then(|rare| rare.old.as_ref());
        let in_main = main.len();
        let in_arena = arena.map_or(0, |a| a.groups);
        let in_old = old.map_or(0, |o| o.array.len());
        let in_old_arena = old.map_or(0, |o| o.arena.groups);
        let groups = in_main + in_arena + in_old + in_old_arena;
        let group_at = |mut g: usize| -> (*mut Group, bool) {
            if g < in_main {
                return (main.group(g), false);
            }
            g -= in_main;
            if g < in_arena {
                return (arena.expect("an arena").group_at(g), false);
            }
            g -= in_arena;
            let o = old.expect("an old array");
            if g < in_old {
                (o.array.group(g), true)
            } else {
                (o.arena.group_at(g - in_old), true)
            }
        };
        for _ in 0..RANDOM_PICKS {
            // One draw: its product with the group count is the group (the
            // high word) and, scaled again, the slot (from the low word).
            let wide = u128::from(rng.next_u64()) * groups as u128;
            let (group, old) = group_at((wide >> 64) as usize);
            let slot = ((u128::from(wide as u64) * SLOTS as u128) >> 64) as usize;
            // SAFETY: a handed-out group of the table's.
            if unsafe { (*group).is_element(slot) } {
                return Some((group, slot, old));
            }
        }
        // Rare: walk on from a random group to the next that holds an
        // element, run by run (a little biased, never wrong).
        let start = rng.gen_range(0..groups);
        for from in [start, 0] {
            let mut skip = from;
            let mut found = None;
            self.runs(|run, n, old| {
                if skip >= n {
                    skip -= n;
                    return false;
                }
                for i in skip..n {
                    // SAFETY: `i < n`: a group of the run.
                    let group = unsafe { run.add(i) };
                    // SAFETY: a handed-out group of the table's.
                    let full = unsafe { (*group).full() };
                    if full != 0 {
                        found = Some((group, first(full), old));
                        return true;
                    }
                }
                skip = 0;
                false
            });
            if found.is_some() {
                return found;
            }
        }
        unreachable!("a table of {} elements holds none", self.len)
    }

    /// A random element's slot value.
    #[inline]
    fn random_slot(&self, rng: &mut impl Rng) -> Option<*mut u8> {
        // SAFETY: `pick` gives an element's slot.
        self.pick(rng)
            .map(|(group, slot, _)| unsafe { (*group).slots[slot] })
    }

    /// Take a random element out of the table, as [`Self::take_out`]: one
    /// hash, to find the head of its chain, and only if it is in the main
    /// array (an old array's chains are not compacted).
    fn take_random(&mut self, rng: &mut impl Rng) -> Option<*mut u8> {
        let (group, slot, in_old) = self.pick(rng)?;
        // SAFETY: an element's slot.
        let value = unsafe { (*group).slots[slot] };
        let head = if in_old {
            ptr::null_mut()
        } else {
            // SAFETY: a live element of the table's.
            let h = unsafe { K::hash_of(value, &self.hasher) };
            self.main().group(self.main().home(h))
        };
        Some(self.take_out(Found {
            group,
            slot,
            head,
            in_old,
        }))
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
    /// `A` = [`EntryPool`] or [`SystemMemory`]: the group arrays, the arenas,
    /// the rare part and the entries at what `A` charges (a pool block at
    /// its class's size). Not the table's own header,
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
            stats.reseeds = rare.reseeds;
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
            stats.chains[(depth - 1).min(CHAIN_BUCKETS - 1)] += 1;
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
    /// Home groups by the length of their chain: `chains[i]` have `i + 1`
    /// groups, the last bucket all longer ones too.
    pub chains: [usize; CHAIN_BUCKETS],
    /// Resizes with a new seed so far.
    pub reseeds: u32,
}

/// Buckets of [`TableStats::chains`].
#[doc(hidden)]
pub const CHAIN_BUCKETS: usize = 12;

impl<K: Kind, A: Memory> Clone for CollectionTable<K, A> {
    /// A table of its own, under a seed of its own, with a copy of every
    /// element: each entry copied byte for byte (value, TTL and TTL list
    /// position alike), each inline member as it is. What `COPY` and a slot
    /// migration need of a key's value (`KiviDBValue::deep_clone`).
    fn clone(&self) -> Self {
        let mut copy = Self::build(self.len, ahash::RandomState::new(), self.max_fill);
        for slot in self.slots() {
            // SAFETY: a live element of `self`'s.
            let new = match unsafe { K::block(slot) } {
                None => slot,
                Some(size) => {
                    let block = copy.new_block(size);
                    // SAFETY: `size` bytes of a live entry into a new block
                    // of that size.
                    unsafe { ptr::copy_nonoverlapping(slot, block.as_ptr(), size) };
                    block.as_ptr()
                }
            };
            // SAFETY: a copy of a live element: the same kind.
            let h = unsafe { K::hash_of(new, &copy.hasher) };
            copy.add(h, new);
        }
        copy
    }
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
            if let Some(size) = unsafe { K::block(slot) } {
                // SAFETY: allocated by `new_block` with this size.
                unsafe { A::free_entry(NonNull::new_unchecked(slot), size) };
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

/// What [`CollectionTable::update_value`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Updated {
    /// Nothing: `f` said to leave the hash as it is.
    Unchanged,
    /// The field was not there, and is now, with no TTL.
    Inserted,
    /// The field's value was replaced, its TTL and TTL list position kept;
    /// `moved` says whether its entry is a new block now.
    Replaced { moved: bool },
}

impl<A: Memory> CollectionTable<Fields, A> {
    #[inline]
    fn find_field(&self, h: u64, field: &[u8]) -> Option<Found> {
        self.find(
            h,
            |seed| seed.hash_one(field),
            // SAFETY: the table's slots hold hash entries.
            |slot| unsafe { field_is(slot, field) },
        )
    }

    /// The field, its value and its TTL.
    #[inline]
    pub fn get(&self, field: &[u8]) -> Option<FieldRef<'_>> {
        let found = self.find_field(self.hash(field), field)?;
        // SAFETY: a live entry, unchanged while `self` is borrowed.
        Some(unsafe { field_ref(found.value()) })
    }

    #[inline]
    pub fn contains_key(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    /// Add a field known not to be there.
    fn add_field(&mut self, h: u64, field: &[u8], value: &[u8], ttl: Option<(u64, u32)>) {
        let size = field_header_for(field.len(), value.len(), ttl.is_some()).size();
        let block = self.new_block(size);
        // SAFETY: a new block of `size` bytes; the lengths were checked.
        unsafe { write_field_entry(block.as_ptr(), field, value, ttl) };
        self.add(h, block.as_ptr());
    }

    /// Set the field's value and TTL (`None`: none); true if it is new. A
    /// TTL given starts out [`NOT_LISTED`].
    pub fn insert(
        &mut self,
        field: &[u8],
        value: &[u8],
        ttl: Option<u64>,
    ) -> Result<bool, TooLong> {
        check_len(field.len())?;
        check_len(value.len())?;
        let h = self.hash(field);
        let ttl = ttl.map(|deadline| (deadline, NOT_LISTED));
        if let Some(found) = self.find_field(h, field) {
            self.rewrite(found, field, value, ttl);
            self.after_write();
            return Ok(false);
        }
        self.add_field(h, field, value, ttl);
        Ok(true)
    }

    /// Add the field only if it is not there (`HSETNX`), in one lookup:
    /// true if it was added; a field that is there is left as it is.
    pub fn insert_if_absent(
        &mut self,
        field: &[u8],
        value: &[u8],
        ttl: Option<u64>,
    ) -> Result<bool, TooLong> {
        check_len(field.len())?;
        check_len(value.len())?;
        let h = self.hash(field);
        if self.find_field(h, field).is_some() {
            return Ok(false);
        }
        self.add_field(h, field, value, ttl.map(|deadline| (deadline, NOT_LISTED)));
        Ok(true)
    }

    /// Set the value and TTL of a field that is there; false (and nothing
    /// changed) if it is not.
    pub fn replace(
        &mut self,
        field: &[u8],
        value: &[u8],
        ttl: Option<u64>,
    ) -> Result<bool, TooLong> {
        check_len(field.len())?;
        check_len(value.len())?;
        let Some(found) = self.find_field(self.hash(field), field) else {
            return Ok(false);
        };
        self.rewrite(
            found,
            field,
            value,
            ttl.map(|deadline| (deadline, NOT_LISTED)),
        );
        self.after_write();
        Ok(true)
    }

    /// Set the field's value to what `f` makes of the one it has (`None` if
    /// the field is not there), in one lookup: a field that is there keeps
    /// its TTL and its TTL list position (`HINCRBY` keeps a field's TTL), one
    /// that is not is added with no TTL. `f` returning `None` changes
    /// nothing. A value longer than [`MAX_LEN`] changes nothing either.
    pub fn update_value<B: AsRef<[u8]>>(
        &mut self,
        field: &[u8],
        f: impl FnOnce(Option<&[u8]>) -> Option<B>,
    ) -> Result<Updated, TooLong> {
        check_len(field.len())?;
        let h = self.hash(field);
        let found = self.find_field(h, field);
        // SAFETY: a live entry, not changed until after `f` and the reads of
        // its TTL below.
        let old = found.map(|found| unsafe { field_ref(found.value()) });
        let Some(new) = f(old.map(|entry| entry.value)) else {
            return Ok(Updated::Unchanged);
        };
        let value = new.as_ref();
        check_len(value.len())?;
        // The TTL copied out: `old` is not used past here, before `rewrite`
        // may free the entry.
        let ttl = old.and_then(|entry| entry.ttl.zip(entry.ttl_position));
        let Some(found) = found else {
            self.add_field(h, field, value, None);
            return Ok(Updated::Inserted);
        };
        let moved = self.rewrite(found, field, value, ttl);
        self.after_write();
        Ok(Updated::Replaced { moved })
    }

    /// Set or remove the field's TTL, keeping its value (`HEXPIRE`,
    /// `HPERSIST`): `None` if the field is not there, else whether its entry
    /// moved to a new block (a TTL added or removed changes its size). A TTL
    /// changed keeps its list position; one added starts [`NOT_LISTED`].
    pub fn set_ttl(&mut self, field: &[u8], ttl: Option<u64>) -> Option<bool> {
        let found = self.find_field(self.hash(field), field)?;
        let entry = found.value();
        // SAFETY: a live hash entry.
        let header = unsafe { field_header(entry) };
        match (header.ttl_at, ttl) {
            (None, None) => Some(false),
            (Some(at), Some(deadline)) => {
                // SAFETY: the entry's deadline, in place.
                unsafe { write_bytes(entry.add(at + 4), &deadline.to_le_bytes()) };
                Some(false)
            }
            _ => {
                // The size changes: a new block, written from the old one,
                // which is freed once nothing borrows it (not through
                // `rewrite`, whose arguments would still borrow it).
                let ttl = ttl.map(|deadline| (deadline, NOT_LISTED));
                let block = {
                    // SAFETY: a live entry, not changed while `old` lives.
                    let old = unsafe { field_ref(entry) };
                    let size =
                        field_header_for(old.field.len(), old.value.len(), ttl.is_some()).size();
                    let block = self.new_block(size);
                    // SAFETY: a new block of `size` bytes; the old entry's
                    // bytes are elsewhere.
                    unsafe { write_field_entry(block.as_ptr(), old.field, old.value, ttl) };
                    block
                };
                // SAFETY: the found slot; the old entry is out of it, and
                // freed with no borrow of it left.
                unsafe {
                    (*found.group).slots[found.slot] = block.as_ptr();
                    self.free_element(entry);
                }
                self.after_write();
                Some(true)
            }
        }
    }

    /// Set where the field is in its hash's TTL list (the list says where
    /// it put it). False if the field is not there or has no TTL.
    pub fn set_ttl_position(&mut self, field: &[u8], position: u32) -> bool {
        let Some(found) = self.find_field(self.hash(field), field) else {
            return false;
        };
        let entry = found.value();
        // SAFETY: a live hash entry.
        let Some(at) = (unsafe { field_header(entry) }).ttl_at else {
            return false;
        };
        // SAFETY: the entry's list position, in place.
        unsafe { write_bytes(entry.add(at), &position.to_le_bytes()) };
        true
    }

    /// Write a found field's new value and TTL (deadline, list position):
    /// in place if the entry's size is the same (header and all: an equal
    /// size can be another shape), else in a new block. Whether it moved.
    /// `field` and `value` are never the entry's own bytes: it may be freed
    /// here.
    fn rewrite(
        &mut self,
        found: Found,
        field: &[u8],
        value: &[u8],
        ttl: Option<(u64, u32)>,
    ) -> bool {
        let old = found.value();
        // SAFETY: a live hash entry.
        let old_size = unsafe { field_header(old) }.size();
        let size = field_header_for(field.len(), value.len(), ttl.is_some()).size();
        if size == old_size {
            // SAFETY: the entry is `size` bytes, and `field`/`value` are not
            // in it.
            unsafe { write_field_entry(old, field, value, ttl) };
            return false;
        }
        let block = self.new_block(size);
        // SAFETY: a new block of `size` bytes; the old entry is freed only
        // once it is written.
        unsafe {
            write_field_entry(block.as_ptr(), field, value, ttl);
            (*found.group).slots[found.slot] = block.as_ptr();
            self.free_element(old);
        }
        true
    }

    /// Remove the field; true if it was there.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        self.remove_with(field, |_| ()).is_some()
    }

    /// Remove the field, showing it to `f` on its way out; `None` if it was
    /// not there.
    pub fn remove_with<R>(&mut self, field: &[u8], f: impl FnOnce(FieldRef<'_>) -> R) -> Option<R> {
        let found = self.find_field(self.hash(field), field)?;
        let slot = self.take_out(found);
        // SAFETY: out of the table, not yet freed.
        let out = f(unsafe { field_ref(slot) });
        // SAFETY: out of the table.
        unsafe { self.free_element(slot) };
        self.after_remove();
        Some(out)
    }

    /// Keep the fields `keep` says to.
    pub fn retain(&mut self, mut keep: impl FnMut(FieldRef<'_>) -> bool) {
        // SAFETY: live entries.
        self.retain_slots(|slot| keep(unsafe { field_ref(slot) }));
    }

    /// Every field, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = FieldRef<'_>> + '_ {
        // SAFETY: live entries, unchanged while `self` is borrowed.
        self.slots().map(|slot| unsafe { field_ref(slot) })
    }

    /// Calls `f` with every field whose position is at or after `cursor`, a
    /// home group (and its chain) at a time, until at least `count` were
    /// visited; returns the next cursor, 0 at the end. A growth between two
    /// calls misses and repeats nothing; a shrink may repeat, never miss (but
    /// see the module docs on a resize with a new seed).
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

    /// Remove a field picked uniformly at random, showing it to `f` on its
    /// way out (one pick, one hash, no copy); `None` if there is none.
    pub fn remove_random<R>(
        &mut self,
        rng: &mut impl Rng,
        f: impl FnOnce(FieldRef<'_>) -> R,
    ) -> Option<R> {
        let slot = self.take_random(rng)?;
        // SAFETY: out of the table, not yet freed.
        let out = f(unsafe { field_ref(slot) });
        // SAFETY: out of the table.
        unsafe { self.free_element(slot) };
        self.after_remove();
        Some(out)
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
    fn find_member(&self, h: u64, member: &[u8], probe: Probe<'_>) -> Option<Found> {
        self.find(
            h,
            |seed| seed.hash_one(member),
            // SAFETY: the table's slots hold set elements.
            |slot| unsafe { probe.is(slot) },
        )
    }

    #[inline]
    pub fn contains(&self, member: &[u8]) -> bool {
        self.find_member(self.hash(member), member, Probe::of(member))
            .is_some()
    }

    /// The member as the table holds it.
    #[inline]
    pub fn get(&self, member: &[u8]) -> Option<MemberRef<'_>> {
        let found = self.find_member(self.hash(member), member, Probe::of(member))?;
        // SAFETY: a live element, unchanged while `self` is borrowed.
        Some(unsafe { member_ref(found.value()) })
    }

    /// Add the member, in one lookup (`SADD`); true if it is new.
    pub fn insert(&mut self, member: &[u8]) -> Result<bool, TooLong> {
        check_len(member.len())?;
        let h = self.hash(member);
        let probe = Probe::of(member);
        if self.find_member(h, member, probe).is_some() {
            return Ok(false);
        }
        let slot = match probe {
            Probe::Inline(slot) => slot,
            Probe::Bytes(bytes) => {
                let block = self.new_block(member_size(bytes.len()));
                // SAFETY: a new block of that size; the length was checked.
                unsafe { write_member_entry(block.as_ptr(), bytes) };
                block.as_ptr()
            }
        };
        self.add(h, slot);
        Ok(true)
    }

    /// Remove the member; true if it was there.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        self.remove_with(member, |_| ()).is_some()
    }

    /// Remove the member, showing it to `f` on its way out; `None` if it was
    /// not there.
    pub fn remove_with<R>(
        &mut self,
        member: &[u8],
        f: impl FnOnce(MemberRef<'_>) -> R,
    ) -> Option<R> {
        let found = self.find_member(self.hash(member), member, Probe::of(member))?;
        let slot = self.take_out(found);
        // SAFETY: out of the table, not yet freed.
        let out = f(unsafe { member_ref(slot) });
        // SAFETY: out of the table.
        unsafe { self.free_element(slot) };
        self.after_remove();
        Some(out)
    }

    /// Keep the members `keep` says to.
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8]) -> bool) {
        // SAFETY: live elements.
        self.retain_slots(|slot| keep(&unsafe { member_ref(slot) }));
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

    /// Remove a member picked uniformly at random (`SPOP`), showing it to
    /// `f` on its way out (one pick, one hash, no copy); `None` if there is
    /// none.
    pub fn remove_random<R>(
        &mut self,
        rng: &mut impl Rng,
        f: impl FnOnce(MemberRef<'_>) -> R,
    ) -> Option<R> {
        let slot = self.take_random(rng)?;
        // SAFETY: out of the table, not yet freed.
        let out = f(unsafe { member_ref(slot) });
        // SAFETY: out of the table.
        unsafe { self.free_element(slot) };
        self.after_remove();
        Some(out)
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
        let mut check_array =
            |array: &Array, from: usize, main: bool, hasher: &ahash::RandomState| {
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
                                    let h = K::hash_of(slot, hasher);
                                    assert_eq!(fingerprint(h), c);
                                    assert_eq!(array.home(h), index, "an element away from home");
                                    if let Some(size) = K::block(slot) {
                                        assert_eq!(slot.addr() % ENTRY_ALIGN, 0);
                                        entry_bytes += A::entry_charged(size);
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
        check_array(&self.main(), 0, true, &self.hasher);
        if let Some(old) = self.rare.as_ref().and_then(|rare| rare.old.as_ref()) {
            check_array(
                &old.array,
                old.next,
                false,
                old.hasher.as_ref().unwrap_or(&self.hasher),
            );
        }
        assert_eq!(count, self.len);
        assert_eq!(entry_bytes, self.entry_bytes);
        let live = self.rare.as_ref().map_or(0, |rare| rare.arena.live);
        assert_eq!(chain_groups, live, "overflow groups in chains vs the arena");
        if !self.resizing() {
            assert!(self.len <= self.grow_at.max(1) || self.main().bits == MAX_BITS);
            assert_eq!(self.flags & RESEED_DUE, 0, "a reseed due starts at once");
        }
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
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
            CHUNKS.with(|c| c.set(c.get() + 1));
            BYTES.with(|c| c.set(c.get() + layout.size() as isize));
            SystemMemory::alloc(layout, zeroed)
        }

        unsafe fn free(block: NonNull<u8>, layout: Layout) {
            CHUNKS.with(|c| c.set(c.get() - 1));
            BYTES.with(|c| c.set(c.get() - layout.size() as isize));
            // SAFETY: the caller's promise.
            unsafe { SystemMemory::free(block, layout) }
        }

        fn alloc_entry(size: usize) -> NonNull<u8> {
            BLOCKS.with(|c| c.set(c.get() + 1));
            BYTES.with(|c| c.set(c.get() + size as isize));
            SystemMemory::alloc_entry(size)
        }

        unsafe fn free_entry(block: NonNull<u8>, size: usize) {
            BLOCKS.with(|c| c.set(c.get() - 1));
            BYTES.with(|c| c.set(c.get() - size as isize));
            // SAFETY: the caller's promise.
            unsafe { SystemMemory::free_entry(block, size) }
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

    /// A hash's model: each field's value, and its deadline and TTL list
    /// position if it has a TTL.
    type HashModel = HashMap<Vec<u8>, (Vec<u8>, Option<(u64, u32)>)>;

    fn check_hash<A: Memory>(table: &CollectionTable<Fields, A>, model: &HashModel) {
        table.check();
        assert_eq!(table.len(), model.len());
        for (field, (value, ttl)) in model {
            let got = table.get(field).expect("a field of the model");
            assert_eq!(got.field, &field[..]);
            assert_eq!(got.value, &value[..]);
            assert_eq!(got.ttl, ttl.map(|(deadline, _)| deadline));
            assert_eq!(got.ttl_position, ttl.map(|(_, position)| position));
        }
        let mut seen = HashSet::new();
        for entry in table.iter() {
            assert!(seen.insert(entry.field.to_vec()), "iter repeats");
            assert!(model.contains_key(entry.field));
        }
        assert_eq!(seen.len(), model.len());
    }

    /// One random operation of every kind a hash has, against the model.
    fn hash_op<A: Memory>(
        table: &mut CollectionTable<Fields, A>,
        model: &mut HashModel,
        rng: &mut StdRng,
        universe: usize,
    ) {
        let field = field_name(rng, universe);
        match rng.gen_range(0..100) {
            0..=44 => {
                let value = value_for(rng);
                let ttl = rng.gen_bool(0.3).then(|| rng.r#gen::<u64>());
                let new = table.insert(&field, &value, ttl).unwrap();
                let was = model.insert(field, (value, ttl.map(|t| (t, NOT_LISTED))));
                assert_eq!(new, was.is_none());
            }
            45..=49 => {
                let value = value_for(rng);
                let ttl = rng.gen_bool(0.5).then_some(11);
                let added = table.insert_if_absent(&field, &value, ttl).unwrap();
                assert_eq!(added, !model.contains_key(&field));
                if added {
                    model.insert(field, (value, ttl.map(|t| (t, NOT_LISTED))));
                }
            }
            50..=56 => {
                // A value made from the old one, longer, shorter or the
                // same length (the TTL and its position stay).
                let grow = rng.gen_range(0..3usize);
                let mut called = false;
                let got = table
                    .update_value(&field, |old| {
                        called = true;
                        match (old, grow) {
                            (_, 0) if field.len().is_multiple_of(3) => None,
                            (None, _) => Some(b"new".to_vec()),
                            (Some(old), 0) => Some(old.iter().rev().copied().collect()),
                            (Some(old), 1) => Some([old, b"+"].concat()),
                            (Some(old), _) => Some(old[..old.len() / 2].to_vec()),
                        }
                    })
                    .unwrap();
                assert!(called);
                let skip = grow == 0 && field.len().is_multiple_of(3);
                match model.get_mut(&field) {
                    _ if skip => assert_eq!(got, Updated::Unchanged),
                    None => {
                        assert_eq!(got, Updated::Inserted);
                        model.insert(field, (b"new".to_vec(), None));
                    }
                    Some((value, _)) => {
                        let new: Vec<u8> = match grow {
                            0 => value.iter().rev().copied().collect(),
                            1 => [&value[..], b"+"].concat(),
                            _ => value[..value.len() / 2].to_vec(),
                        };
                        let moved = new.len() != value.len();
                        assert_eq!(got, Updated::Replaced { moved });
                        *value = new;
                    }
                }
            }
            57..=61 => {
                let value = value_for(rng);
                let ttl = rng.gen_bool(0.5).then_some(7);
                let there = table.replace(&field, &value, ttl).unwrap();
                assert_eq!(there, model.contains_key(&field));
                if there {
                    model.insert(field, (value, ttl.map(|t| (t, NOT_LISTED))));
                }
            }
            62..=65 => {
                let ttl = rng.gen_bool(0.6).then(|| rng.gen_range(0..1000u64));
                let got = table.set_ttl(&field, ttl);
                match model.get_mut(&field) {
                    None => assert_eq!(got, None),
                    Some((_, had)) => {
                        assert_eq!(got, Some(had.is_some() != ttl.is_some()));
                        let position = had.map_or(NOT_LISTED, |(_, p)| p);
                        *had = ttl.map(|t| (t, position));
                    }
                }
            }
            66..=68 => {
                let position = rng.gen_range(0..u32::MAX);
                let set = table.set_ttl_position(&field, position);
                match model.get_mut(&field) {
                    Some((_, Some((_, p)))) => {
                        assert!(set);
                        *p = position;
                    }
                    _ => assert!(!set),
                }
            }
            69..=76 => assert_eq!(table.remove(&field), model.remove(&field).is_some()),
            77..=80 => {
                let got =
                    table.remove_with(&field, |e| (e.value.to_vec(), e.ttl.zip(e.ttl_position)));
                assert_eq!(got, model.remove(&field));
            }
            81..=82 => {
                let got = table.remove_random(rng, |e| (e.field.to_vec(), e.value.to_vec()));
                match got {
                    None => assert!(model.is_empty()),
                    Some((f, v)) => assert_eq!(model.remove(&f).map(|(v, _)| v), Some(v)),
                }
            }
            _ => {
                let got = table
                    .get(&field)
                    .map(|e| (e.value.to_vec(), e.ttl.zip(e.ttl_position)));
                assert_eq!(got, model.get(&field).cloned());
            }
        }
    }

    /// The whole-table operations, once a round: a copy, `retain`.
    fn hash_round_ops(table: &mut HashT, model: &mut HashModel, round: usize) {
        let copy = table.clone();
        check_hash(&copy, model);
        drop(copy);
        let odd = |field: &[u8]| (field.len() + round).is_multiple_of(7);
        table.retain(|e| !odd(e.field));
        model.retain(|field, _| !odd(field));
        check_hash(table, model);
    }

    #[test]
    fn hash_model() {
        let (blocks0, chunks0, _) = live();
        let mut rng = StdRng::seed_from_u64(764);
        let mut table = HashT::with_capacity_and_seed(0, 1);
        let mut model = HashModel::new();
        let mut seen = Seen::default();
        let universe = scale(40_000);
        let rounds = if cfg!(miri) { 2 } else { 4 };
        for round in 0..rounds {
            // Up to many fields, then down to a few: growths, then shrinks.
            let high = universe / 2 + round * universe / 10;
            let mut ops = 0usize;
            while model.len() < high || ops < universe {
                ops += 1;
                hash_op(&mut table, &mut model, &mut rng, universe);
                if table.resizing() && ops.is_multiple_of(97) {
                    note_resize(&table, &mut seen);
                    check_hash(&table, &model);
                    if ops.is_multiple_of(97 * 13) {
                        // A copy made mid-resize.
                        check_hash(&table.clone(), &model);
                    }
                }
            }
            check_hash(&table, &model);
            hash_round_ops(&mut table, &mut model, round);
            // Down to a few.
            let keep = universe / 50;
            let fields: Vec<Vec<u8>> = model.keys().cloned().collect();
            for (n, field) in fields.iter().enumerate() {
                if model.len() <= keep {
                    break;
                }
                if !model.contains_key(field) {
                    // Removed by a write mid-shrink.
                    continue;
                }
                assert!(table.remove(field));
                model.remove(field);
                if table.resizing() && n.is_multiple_of(31) {
                    note_resize(&table, &mut seen);
                    check_hash(&table, &model);
                    // Writes of every kind mid-shrink.
                    for _ in 0..5 {
                        hash_op(&mut table, &mut model, &mut rng, universe);
                    }
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
        table.clear();
        model.clear();
        check_hash(&table, &model);
        for _ in 0..scale(2_000) {
            hash_op(&mut table, &mut model, &mut rng, universe);
        }
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

    fn check_set<A: Memory>(table: &CollectionTable<Members, A>, model: &HashSet<Vec<u8>>) {
        table.check();
        assert_eq!(table.len(), model.len());
        for member in model {
            assert!(table.contains(member), "missing {member:?}");
            assert_eq!(table.get(member).as_deref(), Some(&member[..]));
        }
        let mut seen = HashSet::new();
        for member in table.iter() {
            assert!(seen.insert(member.to_vec()), "iter repeats");
            assert!(model.contains(&member[..]));
        }
        assert_eq!(seen.len(), model.len());
    }

    fn set_op<A: Memory>(
        table: &mut CollectionTable<Members, A>,
        model: &mut HashSet<Vec<u8>>,
        rng: &mut StdRng,
        universe: usize,
    ) {
        let member = member_for(rng, universe);
        match rng.gen_range(0..100) {
            0..=59 => assert_eq!(table.insert(&member).unwrap(), model.insert(member)),
            60..=71 => assert_eq!(table.remove(&member), model.remove(&member)),
            72..=77 => {
                let got = table.remove_with(&member, |m| m.to_vec());
                assert_eq!(got, model.take(&member));
            }
            78..=79 => match table.remove_random(rng, |m| m.to_vec()) {
                None => assert!(model.is_empty()),
                Some(m) => assert!(model.remove(&m), "{m:?}"),
            },
            _ => {
                assert_eq!(table.contains(&member), model.contains(&member));
                assert_eq!(table.get(&member).is_some(), model.contains(&member));
            }
        }
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
                set_op(&mut table, &mut model, &mut rng, universe);
                if table.resizing() && ops.is_multiple_of(89) {
                    note_resize(&table, &mut seen);
                    check_set(&table, &model);
                }
            }
            check_set(&table, &model);
            check_set(&table.clone(), &model);
            let odd = |m: &[u8]| (m.len() + round).is_multiple_of(5);
            table.retain(|m| !odd(m));
            model.retain(|m| !odd(m));
            check_set(&table, &model);
            let members: Vec<Vec<u8>> = model.iter().cloned().collect();
            for (n, member) in members.iter().enumerate() {
                if model.len() <= universe / 60 {
                    break;
                }
                if !model.contains(member) {
                    // Removed by a write mid-shrink.
                    continue;
                }
                assert!(table.remove(member));
                model.remove(member);
                if table.resizing() && n.is_multiple_of(29) {
                    note_resize(&table, &mut seen);
                    check_set(&table, &model);
                    for _ in 0..5 {
                        set_op(&mut table, &mut model, &mut rng, universe);
                    }
                    check_set(&table, &model);
                    if n.is_multiple_of(29 * 7) {
                        check_set(&table.clone(), &model);
                    }
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
        table.clear();
        model.clear();
        check_set(&table, &model);
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
            assert!(table.insert(f, v, *t).unwrap());
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
            assert!(set.insert(m).unwrap());
            assert!(!set.insert(m).unwrap());
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
        let mut hash = CollectionTable::<Fields, SystemMemory>::with_capacity_and_seed(0, 4);
        let mut set = CollectionTable::<Members, SystemMemory>::with_capacity_and_seed(0, 4);
        for (n, (f, m)) in fields.iter().zip(&members).enumerate() {
            hash.insert(f, b"v", n.is_multiple_of(3).then_some(1))
                .unwrap();
            set.insert(m).unwrap();
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
                )
                .unwrap();
                set.insert(i.to_string().as_bytes()).unwrap();
                set.insert(format!("member-{i:0>7}").as_bytes()).unwrap();
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
            set.insert(format!("member-{i:0>7}").as_bytes()).unwrap();
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
        let list = |a: &Arena| a.chunks.capacity() * std::mem::size_of::<Chunk>();
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
            table.insert(format!("member-{i}").as_bytes()).unwrap();
        }
        while !table.step_resize(usize::MAX) {}
        let bits = table.bits;
        // Writes between pages that resize nothing: other members come and go.
        let mut extra = 0usize;
        let (returned, pages) = scan_all(&mut table, 10, |t, page| {
            if page.is_multiple_of(2) {
                t.insert(format!("extra-{extra}").as_bytes()).unwrap();
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
                table.insert(format!("stay-{i}").as_bytes()).unwrap();
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
                        t.insert(format!("temp-{added}").as_bytes()).unwrap();
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
            table.insert(format!("m-{i}").as_bytes()).unwrap();
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
            table.insert(m).unwrap();
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
            table.insert(m).unwrap();
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
            table.insert(&m).unwrap();
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
            table
                .insert(format!("f{i}").as_bytes(), b"v", None)
                .unwrap();
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

    /// Fields whose hashes share their top `top_bits` bits under one seed.
    fn colliding(seed: u64, n: usize, top_bits: u32) -> Vec<Vec<u8>> {
        let probe = HashT::with_capacity_and_seed(0, seed);
        let target = probe.hash(b"anchor") >> (64 - top_bits);
        (0u64..)
            .map(|i| format!("test-{i}").into_bytes())
            .filter(|f| probe.hash(f) >> (64 - top_bits) == target)
            .take(n)
            .collect()
    }

    fn reseeds<K: Kind, A: Memory>(table: &CollectionTable<K, A>) -> u32 {
        table.rare.as_ref().map_or(0, |rare| rare.reseeds)
    }

    /// Whether a resize with a new seed is in progress.
    fn reseeding<K: Kind, A: Memory>(table: &CollectionTable<K, A>) -> bool {
        table
            .rare
            .as_ref()
            .and_then(|rare| rare.old.as_ref())
            .is_some_and(|old| old.hasher.is_some())
    }

    /// A whole scan, from 0 to 0, with no writes between pages: how many
    /// times each field came back.
    fn scan_fields(table: &HashT, count: usize) -> HashMap<Vec<u8>, usize> {
        let mut returned: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut cursor = 0;
        loop {
            let next = table.scan(cursor, count, |e| {
                *returned.entry(e.field.to_vec()).or_default() += 1
            });
            if next == 0 {
                return returned;
            }
            assert!(next > cursor);
            cursor = next;
        }
    }

    #[test]
    fn a_long_chain_starts_a_resize_with_a_new_seed() {
        let n = if cfg!(miri) { 80 } else { 300 };
        let top = if cfg!(miri) { 5 } else { 10 };
        let fields = colliding(16, n, top);
        let mut rng = StdRng::seed_from_u64(16);
        let mut table = HashT::with_capacity_and_seed(0, 16);
        let mut model = HashModel::new();
        let mut during = 0usize;
        let mut scanned = false;
        for f in &fields {
            assert!(table.insert(f, f, None).unwrap());
            model.insert(f.clone(), (f.clone(), None));
            if reseeding(&table) {
                during += 1;
                if !scanned {
                    // A scan started while it is in progress, no writes
                    // between pages: every field exactly once.
                    let returned = scan_fields(&table, 5);
                    assert_eq!(returned.len(), model.len());
                    assert!(returned.values().all(|&n| n == 1));
                    scanned = true;
                }
                // Writes of every kind while both seeds are in use.
                for _ in 0..3 {
                    hash_op(&mut table, &mut model, &mut rng, 1_000);
                }
                check_hash(&table, &model);
            }
        }
        assert!(reseeds(&table) >= 1, "{:?}", table.stats());
        assert!(during > 0 && scanned);
        while !table.step_resize(usize::MAX) {}
        check_hash(&table, &model);
        // The chain is gone.
        assert!(table.stats().max_chain <= 4, "{:?}", table.stats());
        let returned = scan_fields(&table, 5);
        assert_eq!(returned.len(), model.len());
        assert!(returned.values().all(|&n| n == 1));
        for f in &fields[..n / 2] {
            assert!(table.remove(f));
            model.remove(f);
        }
        check_hash(&table, &model);
        // The same fields under another seed never get there.
        let mut other = HashT::with_capacity_and_seed(0, 17);
        for f in &fields {
            other.insert(f, f, None).unwrap();
        }
        assert_eq!(reseeds(&other), 0);
        assert!(other.stats().max_chain <= 3, "{:?}", other.stats());
    }

    /// A chain over the cap that a shrink's moves make starts a resize with
    /// a new seed too.
    #[test]
    fn a_shrink_onto_a_long_chain_reseeds() {
        // Sizes for which the test's members share a home group only once
        // the shrinks end (in an array of 2^7 groups).
        let (others, top, n) = if cfg!(miri) {
            (1_500usize, 7, 120)
        } else {
            (50_000, 8, 120)
        };
        let fields = colliding(18, n, top);
        let mut table = SetT::with_capacity_and_seed(0, 18);
        let mut model = HashSet::new();
        for i in 0..others {
            let m = format!("other-{i}").into_bytes();
            table.insert(&m).unwrap();
            model.insert(m);
        }
        for f in &fields {
            table.insert(f).unwrap();
            model.insert(f.clone());
        }
        while !table.step_resize(usize::MAX) {}
        assert_eq!(reseeds(&table), 0, "{:?}", table.stats());
        check_set(&table, &model);
        for i in 0..others {
            let m = format!("other-{i}").into_bytes();
            assert!(table.remove(&m));
            model.remove(&m);
            if reseeding(&table) && i.is_multiple_of(3) {
                check_set(&table, &model);
            }
        }
        while !table.step_resize(usize::MAX) {}
        check_set(&table, &model);
        assert!(reseeds(&table) >= 1, "{:?}", table.stats());
        assert!(table.stats().max_chain <= 4, "{:?}", table.stats());
    }

    #[test]
    fn scan_across_a_resize_with_a_new_seed_ends() {
        let n = if cfg!(miri) { 80 } else { 300 };
        let top = if cfg!(miri) { 5 } else { 10 };
        let fields = colliding(19, n, top);
        let mut table = HashT::with_capacity_and_seed(0, 19);
        for i in 0..scale(2_000) {
            table
                .insert(format!("other-{i}").as_bytes(), b"v", None)
                .unwrap();
        }
        while !table.step_resize(usize::MAX) {}
        let mut added = 0usize;
        let mut cursor = 0;
        let mut pages = 0usize;
        loop {
            let next = table.scan(cursor, 10, |_| {});
            pages += 1;
            assert!(pages < 1_000_000, "the scan ends");
            // Between pages: the test's fields arrive, and the table reseeds.
            for f in fields.iter().skip(added).take(5) {
                table.insert(f, b"x", None).unwrap();
            }
            added = (added + 5).min(n);
            if next == 0 {
                break;
            }
            assert!(next > cursor, "the cursor goes forward");
            cursor = next;
        }
        for f in fields.iter().skip(added) {
            table.insert(f, b"x", None).unwrap();
        }
        assert!(reseeds(&table) >= 1);
        while !table.step_resize(usize::MAX) {}
        table.check();
        // A scan from 0 after the reseed: every field exactly once.
        let returned = scan_fields(&table, 10);
        assert_eq!(returned.len(), table.len());
        assert!(returned.values().all(|&n| n == 1));
    }

    /// Random keys never reach the cap: filled far, the longest chain stays
    /// short and no resize with a new seed happens.
    #[test]
    fn random_keys_never_reseed() {
        let n = scale(300_000);
        let mut hash = HashT::with_capacity_and_seed(0, 20);
        let mut set = SetT::with_capacity_and_seed(0, 20);
        for i in 0..n {
            hash.insert(format!("f{i}").as_bytes(), b"v", None).unwrap();
            set.insert(format!("member-{i:0>7}").as_bytes()).unwrap();
            if i.is_multiple_of(10_007) {
                assert!(hash.stats().max_chain <= CHAIN_CAP / 2);
            }
        }
        assert_eq!(reseeds(&hash), 0);
        assert_eq!(reseeds(&set), 0);
        assert!(hash.stats().max_chain <= 5, "{:?}", hash.stats());
        assert!(set.stats().max_chain <= 5, "{:?}", set.stats());
    }

    #[test]
    fn with_capacity_does_not_grow() {
        let n = scale(10_000);
        let mut table = SetT::with_capacity_and_seed(n, 18);
        let bits = table.bits;
        for i in 0..n {
            table.insert(format!("member-{i}").as_bytes()).unwrap();
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
            table.insert(format!("member-{i}").as_bytes()).unwrap();
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
            table.insert(format!("member-{i}").as_bytes()).unwrap();
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
        let mut table = CollectionTable::<Members, SystemMemory>::with_capacity_and_seed(0, 21);
        table.defer_free(true);
        for name in &names {
            table.insert(name).unwrap();
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
            table.insert(format!("member-{i}").as_bytes()).unwrap();
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

    /// Whether the chi-square of `counts`, `per` expected each, is within
    /// six standard deviations of its mean.
    fn assert_uniform(counts: &[usize], per: usize) {
        let expected = per as f64;
        let chi: f64 = counts
            .iter()
            .map(|&c| (c as f64 - expected).powi(2) / expected)
            .sum();
        let df = (counts.len() - 1) as f64;
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
    fn remove_random_is_uniform_and_drains() {
        let n = scale(2_000);
        let per = if cfg!(miri) { 20 } else { 40 };
        let mut table = SetT::with_capacity_and_seed(0, 22);
        let members: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                if i.is_multiple_of(2) {
                    i.to_string().into_bytes()
                } else {
                    format!("member-{i}").into_bytes()
                }
            })
            .collect();
        let index: HashMap<Vec<u8>, usize> = members.iter().cloned().zip(0..).collect();
        for m in &members {
            table.insert(m).unwrap();
        }
        while !table.step_resize(usize::MAX) {}
        let mut rng = StdRng::seed_from_u64(22);
        let mut counts = vec![0usize; n];
        for _ in 0..n * per {
            let m = table.remove_random(&mut rng, |m| m.to_vec()).unwrap();
            counts[index[&m]] += 1;
            table.insert(&m).unwrap();
        }
        assert_uniform(&counts, per);
        table.check();
        // Drained: each member once, through shrinks.
        let mut out = HashSet::new();
        let mut k = 0usize;
        while let Some(m) = table.remove_random(&mut rng, |m| m.to_vec()) {
            assert!(out.insert(m));
            k += 1;
            if k.is_multiple_of(97) {
                table.check();
            }
        }
        assert_eq!(out.len(), n);
        assert!(table.is_empty());
        table.check();
    }

    /// After chains fold, the arena holds free groups between used ones:
    /// `random` still picks every member alike.
    #[test]
    fn random_is_uniform_with_free_overflow_groups() {
        let mut table = SetT::with_capacity_and_seed(0, 23);
        let mut members = Vec::new();
        let mut i = 0usize;
        // As full as the table gets: the most chains.
        while members.len() < scale(1_500) || table.len() < table.capacity() {
            let m = format!("member-{i}").into_bytes();
            table.insert(&m).unwrap();
            members.push(m);
            i += 1;
            if table.resizing() {
                while !table.step_resize(usize::MAX) {}
            }
        }
        let mut rng = StdRng::seed_from_u64(23);
        members.shuffle(&mut rng);
        let keep = members.len() * 2 / 3;
        for m in &members[keep..] {
            assert!(table.remove(m));
        }
        assert!(!table.resizing(), "a third out does not shrink it");
        let arena = &table.rare.as_ref().unwrap().arena;
        assert!(
            arena.groups > arena.live,
            "free groups in the arena: {} handed out, {} in chains",
            arena.groups,
            arena.live
        );
        table.check();
        chi_square(
            &table,
            &members[..keep],
            if cfg!(miri) { 20 } else { 60 },
            5,
        );
    }

    #[test]
    fn a_shrink_is_at_most_two_bits() {
        let big = if cfg!(miri) { 3_000 } else { 1_000_000 };
        let mut table = SetT::with_capacity_and_seed(big, 24);
        let bits = table.bits;
        assert!(bits > 2 * MAX_SHRINK_BITS);
        for i in 0..20 {
            table.insert(format!("member-{i}").as_bytes()).unwrap();
        }
        assert!(table.remove(b"member-0"));
        assert!(table.resizing());
        assert_eq!(table.bits, bits - MAX_SHRINK_BITS);
        // A page in the middle of the shrink stays within the 10 x COUNT
        // rule: a unit is one new group and four old ones.
        for count in [1usize, 10] {
            let mut returned = HashSet::new();
            let mut cursor = 0;
            loop {
                let (next, groups) = table.scan_counted(cursor, count, |m| {
                    returned.insert(m.to_vec());
                });
                assert!(
                    groups < 10 * count + 1 + (1 << MAX_SHRINK_BITS),
                    "{groups} groups for COUNT {count}"
                );
                if next == 0 {
                    break;
                }
                cursor = next;
            }
            assert_eq!(returned.len(), 19);
        }
        let mut rng = StdRng::seed_from_u64(24);
        for _ in 0..3 {
            assert!(table.random(&mut rng).is_some());
        }
        // It shrinks again, two bits at a time, until it is not sparse.
        let mut seen = vec![table.bits];
        while !table.step_resize(usize::MAX) {
            if seen.last() != Some(&table.bits) {
                seen.push(table.bits);
            }
        }
        assert!(!table.sparse());
        assert!(
            seen.windows(2).all(|w| w[0] - w[1] <= MAX_SHRINK_BITS),
            "{seen:?}"
        );
        assert!(table.bits <= shrink_bits(19) + MAX_SHRINK_BITS);
        table.check();
    }

    #[test]
    fn a_growth_forced_during_a_shrink() {
        let n = scale(20_000);
        let mut table = SetT::with_capacity_and_seed(0, 25);
        let mut model = HashSet::new();
        for i in 0..n {
            let m = format!("member-{i}").into_bytes();
            table.insert(&m).unwrap();
            model.insert(m);
        }
        while !table.step_resize(usize::MAX) {}
        let mut i = 0usize;
        while !table.resizing() {
            let m = format!("member-{i}").into_bytes();
            assert!(table.remove(&m));
            model.remove(&m);
            i += 1;
        }
        let old_bits = table
            .rare
            .as_ref()
            .unwrap()
            .old
            .as_ref()
            .unwrap()
            .array
            .bits;
        assert!(old_bits > table.bits, "a shrink");
        check_set(&table, &model);
        // A lower fill: the next insert must grow, in the middle of the shrink.
        table.set_max_fill(500);
        let bits = table.bits;
        let mut j = 0usize;
        while table.bits <= bits {
            let m = format!("extra-{j}").into_bytes();
            table.insert(&m).unwrap();
            model.insert(m);
            j += 1;
            if j.is_multiple_of(50) {
                check_set(&table, &model);
            }
        }
        check_set(&table, &model);
        while !table.step_resize(usize::MAX) {}
        check_set(&table, &model);
    }

    /// Writes of every kind to fields a growth has not moved yet.
    #[test]
    fn writes_to_fields_left_in_the_old_array() {
        let mut table = HashT::with_capacity_and_seed(0, 26);
        let mut model = HashModel::new();
        let mut i = 0usize;
        while !(table.resizing() && i > 200) {
            let f = format!("f{i}").into_bytes();
            table.insert(&f, b"value", None).unwrap();
            model.insert(f, (b"value".to_vec(), None));
            i += 1;
        }
        let in_old = |table: &HashT, f: &[u8]| {
            table
                .find_field(table.hash(f), f)
                .is_some_and(|found| found.in_old)
        };
        let mut candidates: Vec<Vec<u8>> = model
            .keys()
            .filter(|f| in_old(&table, f))
            .cloned()
            .collect();
        candidates.sort();
        assert!(
            candidates.len() > 50,
            "{} in the old array",
            candidates.len()
        );
        let mut next = |table: &HashT| loop {
            let f = candidates.pop().expect("a field still in the old array");
            if in_old(table, &f) {
                return f;
            }
        };
        // An overwrite of another size, then of the same size.
        let f = next(&table);
        assert!(!table.insert(&f, b"a longer value", Some(5)).unwrap());
        model.insert(
            f.clone(),
            (b"a longer value".to_vec(), Some((5, NOT_LISTED))),
        );
        check_hash(&table, &model);
        let f = next(&table);
        assert!(!table.insert(&f, b"VALUE", None).unwrap());
        model.insert(f.clone(), (b"VALUE".to_vec(), None));
        check_hash(&table, &model);
        let f = next(&table);
        assert!(table.replace(&f, b"replaced", None).unwrap());
        model.insert(f.clone(), (b"replaced".to_vec(), None));
        check_hash(&table, &model);
        let f = next(&table);
        assert!(!table.insert_if_absent(&f, b"no", None).unwrap());
        check_hash(&table, &model);
        let f = next(&table);
        assert_eq!(
            table.update_value(&f, |v| Some([v.unwrap(), b"!"].concat())),
            Ok(Updated::Replaced { moved: true })
        );
        model.insert(f.clone(), (b"value!".to_vec(), None));
        check_hash(&table, &model);
        let f = next(&table);
        assert_eq!(table.set_ttl(&f, Some(9)), Some(true));
        assert!(table.set_ttl_position(&f, 3));
        model.insert(f.clone(), (b"value".to_vec(), Some((9, 3))));
        check_hash(&table, &model);
        let f = next(&table);
        assert!(table.remove(&f));
        model.remove(&f);
        check_hash(&table, &model);
        let f = next(&table);
        assert_eq!(
            table.remove_with(&f, |e| e.value.to_vec()),
            Some(b"value".to_vec())
        );
        model.remove(&f);
        check_hash(&table, &model);
        while !table.step_resize(usize::MAX) {}
        check_hash(&table, &model);
    }

    /// An entry rewritten in place at the same size, but with another header:
    /// short lengths and a TTL (2 + 12 bytes) against LONG lengths and none
    /// (2 + 8 bytes).
    #[test]
    fn an_equal_size_rewrite_changes_the_header() {
        let mut table = HashT::with_capacity_and_seed(0, 27);
        let field = b"fffff";
        let short = vec![b's'; 124];
        let long = vec![b'L'; 128];
        assert_eq!(
            field_header_for(5, 124, true).size(),
            field_header_for(5, 128, false).size()
        );
        table.insert(field, &short, Some(77)).unwrap();
        let entry = |table: &HashT| table.find_field(table.hash(field), field).unwrap().value();
        let before = entry(&table);
        assert!(!table.insert(field, &long, None).unwrap());
        let e = table.get(field).unwrap();
        assert_eq!(entry(&table), before, "rewritten in place");
        assert_eq!((e.value, e.ttl, e.ttl_position), (&long[..], None, None));
        assert!(table.replace(field, &short, Some(78)).unwrap());
        let e = table.get(field).unwrap();
        assert_eq!(entry(&table), before, "rewritten in place");
        assert_eq!(
            (e.value, e.ttl, e.ttl_position),
            (&short[..], Some(78), Some(NOT_LISTED))
        );
        // `update_value` keeps the TTL, so the same size means the same shape.
        assert!(table.set_ttl_position(field, 12));
        let reversed: Vec<u8> = short.iter().rev().copied().collect();
        assert_eq!(
            table.update_value(field, |_| Some(reversed.clone())),
            Ok(Updated::Replaced { moved: false })
        );
        let e = table.get(field).unwrap();
        assert_eq!(
            (e.value, e.ttl, e.ttl_position),
            (&reversed[..], Some(78), Some(12))
        );
        table.check();
    }

    #[test]
    fn scan_from_any_cursor() {
        let n = scale(5_000);
        let mut table = SetT::with_capacity_and_seed(0, 28);
        let members: Vec<Vec<u8>> = (0..n).map(|i| format!("member-{i}").into_bytes()).collect();
        for m in &members {
            table.insert(m).unwrap();
        }
        while !table.step_resize(usize::MAX) {}
        for cursor in [1u64 << 48, (1 << 48) + 1, u64::MAX] {
            assert_eq!(table.scan(cursor, 10, |_| panic!("past the end")), 0);
        }
        // A cursor inside a group's range of positions: everything at or
        // after it comes back (some before it too, from its group).
        let group = 1u64 << (48 - u32::from(table.bits));
        for cursor in [
            1,
            group / 2,
            3 * group + 7,
            (1 << 47) + 12_345,
            (1 << 48) - 1,
        ] {
            let mut returned = HashSet::new();
            let mut c = cursor;
            loop {
                let next = table.scan(c, 7, |m| {
                    returned.insert(m.to_vec());
                });
                if next == 0 {
                    break;
                }
                assert!(next > c);
                c = next;
            }
            for m in &members {
                if table.position_of(m) >= cursor {
                    assert!(returned.contains(m), "missed a member after {cursor}");
                }
            }
        }
    }

    #[test]
    fn inline_members_of_any_bytes() {
        let members: Vec<Vec<u8>> = vec![
            vec![],
            vec![0],
            vec![0, 0],
            vec![0; 7],
            vec![0xff],
            vec![0xff; 7],
            vec![1, 0],
            vec![1, 0, 0],
            vec![1, 0, 0, 0, 0, 0, 0],
            vec![0, 1],
            vec![0xff, 0],
            vec![0, 0xff],
            vec![0x80, 0x7f, 0, 0xff],
            b"a".to_vec(),
            vec![b'a', 0],
            vec![0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        ];
        // Eight bytes: entries, not inline, and different members still.
        let longer: Vec<Vec<u8>> = vec![vec![0; 8], vec![1, 0, 0, 0, 0, 0, 0, 0], vec![0xff; 8]];
        let mut set = SetT::with_capacity_and_seed(0, 29);
        for m in members.iter().chain(&longer) {
            assert!(set.insert(m).unwrap(), "{m:?}");
            assert!(!set.insert(m).unwrap(), "{m:?}");
        }
        for m in members.iter().chain(&longer) {
            assert!(set.contains(m), "{m:?}");
            assert_eq!(set.get(m).as_deref(), Some(&m[..]));
        }
        let inline: HashSet<Vec<u8>> = set
            .iter()
            .filter(|m| matches!(m, MemberRef::Inline(..)))
            .map(|m| m.to_vec())
            .collect();
        assert_eq!(inline, members.iter().cloned().collect());
        assert_eq!(set.len(), members.len() + longer.len());
        set.check();
        for m in members.iter().chain(&longer) {
            assert_eq!(set.remove_with(m, |got| got.to_vec()), Some(m.clone()));
        }
        assert!(set.is_empty());
        set.check();
    }

    #[test]
    fn lengths_at_the_short_limit() {
        let mut set = SetT::with_capacity_and_seed(0, 30);
        let mut bytes = 0;
        for len in [126usize, 127, 128, 129] {
            for c in *b"mn" {
                assert!(set.insert(&vec![c; len]).unwrap());
                bytes += member_size(len);
            }
        }
        assert_eq!(member_size(127), 128);
        assert_eq!(member_size(128), 133);
        assert_eq!(set.entry_bytes, bytes);
        for len in [126usize, 127, 128, 129] {
            assert_eq!(
                set.get(&vec![b'm'; len]).as_deref(),
                Some(&vec![b'm'; len][..])
            );
        }
        set.check();
        let mut hash = HashT::with_capacity_and_seed(0, 30);
        for flen in [127usize, 128] {
            for vlen in [127usize, 128] {
                for ttl in [None, Some(3)] {
                    let tag = (vlen - 127) as u8 * 2 + u8::from(ttl.is_some());
                    let f = [vec![b'f'; flen - 1], vec![tag]].concat();
                    hash.insert(&f, &vec![b'v'; vlen], ttl).unwrap();
                    let e = hash.get(&f).unwrap();
                    assert_eq!((e.field, e.value.len(), e.ttl), (&f[..], vlen, ttl));
                    // Across the boundary: 127 <-> 128.
                    let other = 255 - vlen;
                    hash.insert(&f, &vec![b'w'; other], ttl).unwrap();
                    assert_eq!(hash.get(&f).unwrap().value, &vec![b'w'; other][..]);
                }
            }
        }
        hash.check();
    }

    #[test]
    fn lengths_over_the_limit_are_refused_before_allocating() {
        assert_eq!(check_len(MAX_LEN), Ok(()));
        assert_eq!(check_len(MAX_LEN + 1), Err(TooLong));
        TEST_MAX_LEN.with(|limit| limit.set(100));
        let (blocks0, ..) = live();
        let mut hash = HashT::with_capacity_and_seed(0, 31);
        assert_eq!(hash.insert(&[b'f'; 101], b"v", None), Err(TooLong));
        assert_eq!(hash.insert(b"f", &[b'v'; 101], None), Err(TooLong));
        assert_eq!(
            hash.insert_if_absent(b"f", &[b'v'; 101], None),
            Err(TooLong)
        );
        assert_eq!(
            hash.update_value(&[b'f'; 101], |_| Some(b"v")),
            Err(TooLong)
        );
        assert_eq!(live().0, blocks0, "nothing allocated");
        assert!(hash.is_empty());
        assert_eq!(hash.insert(&[b'f'; 100], &[b'v'; 100], None), Ok(true));
        assert_eq!(hash.replace(&[b'f'; 100], &[b'w'; 101], None), Err(TooLong));
        assert_eq!(
            hash.update_value(&[b'f'; 100], |_| Some(vec![b'u'; 101])),
            Err(TooLong)
        );
        assert_eq!(hash.get(&[b'f'; 100]).unwrap().value, &[b'v'; 100][..]);
        assert_eq!(live().0, blocks0 + 1);
        let mut set = SetT::with_capacity_and_seed(0, 31);
        assert_eq!(set.insert(&[b'm'; 101]), Err(TooLong));
        assert_eq!(set.insert(&[b'm'; 100]), Ok(true));
        TEST_MAX_LEN.with(|limit| limit.set(MAX_LEN));
        hash.check();
        set.check();
    }

    #[test]
    fn clone_copies_every_element() {
        let (blocks0, chunks0, bytes0) = live();
        let n = scale(3_000);
        let mut hash = HashT::with_capacity_and_seed(0, 32);
        let mut i = 0usize;
        // Copied in the middle of a resize.
        while i < n || !hash.resizing() {
            let ttl = i.is_multiple_of(3).then_some(i as u64);
            hash.insert(format!("f{i}").as_bytes(), &vec![b'v'; i % 150], ttl)
                .unwrap();
            i += 1;
        }
        assert!(hash.set_ttl_position(b"f3", 42));
        let copy = hash.clone();
        copy.check();
        assert_eq!(copy.len(), hash.len());
        for e in hash.iter() {
            assert_eq!(copy.get(e.field), Some(e));
        }
        // Its own: changing one leaves the other.
        hash.insert(b"f1", b"changed", None).unwrap();
        assert!(hash.remove(b"f2"));
        assert_eq!(copy.get(b"f1").unwrap().value, &vec![b'v'; 1][..]);
        assert!(copy.contains_key(b"f2"));
        assert_eq!(copy.get(b"f3").unwrap().ttl_position, Some(42));
        let mut set = SetT::with_capacity_and_seed(0, 32);
        for i in 0..n {
            set.insert(i.to_string().as_bytes()).unwrap();
            set.insert(format!("member-{i}").as_bytes()).unwrap();
        }
        let copy_set = set.clone();
        copy_set.check();
        assert_eq!(copy_set.len(), set.len());
        assert!(set.iter().all(|m| copy_set.contains(&m)));
        drop((copy, hash, copy_set, set));
        assert_eq!(live(), (blocks0, chunks0, bytes0));
    }

    /// The model tests' operations on the memory tables use, the entry
    /// pool, through growths, shrinks, copies and `retain`.
    #[test]
    fn model_on_the_entry_pool() {
        let mut rng = StdRng::seed_from_u64(37);
        let mut hash = CollectionTable::<Fields, EntryPool>::with_capacity_and_seed(0, 37);
        let mut set = CollectionTable::<Members, EntryPool>::with_capacity_and_seed(0, 37);
        let mut hash_model = HashModel::new();
        let mut set_model = HashSet::new();
        let universe = scale(20_000);
        for round in 0..2 {
            for n in 0..universe {
                hash_op(&mut hash, &mut hash_model, &mut rng, universe);
                set_op(&mut set, &mut set_model, &mut rng, universe);
                if n.is_multiple_of(997) {
                    check_hash(&hash, &hash_model);
                    check_set(&set, &set_model);
                }
            }
            check_hash(&hash.clone(), &hash_model);
            check_set(&set.clone(), &set_model);
            let odd = |len: usize| (len + round).is_multiple_of(3);
            hash.retain(|e| !odd(e.field.len()));
            hash_model.retain(|f, _| !odd(f.len()));
            set.retain(|m| !odd(m.len()));
            set_model.retain(|m| !odd(m.len()));
            // Most removed: shrinks.
            let fields: Vec<Vec<u8>> = hash_model.keys().cloned().collect();
            for f in fields.iter().skip(universe / 50) {
                assert!(hash.remove(f));
                hash_model.remove(f);
            }
            let members: Vec<Vec<u8>> = set_model.iter().cloned().collect();
            for m in members.iter().skip(universe / 50) {
                assert!(set.remove(m));
                set_model.remove(m);
            }
            check_hash(&hash, &hash_model);
            check_set(&set, &set_model);
        }
    }

    /// A table made on a thread that has ended and dropped on another: the
    /// pool's slabs it used are all given back.
    #[test]
    fn a_table_from_a_thread_that_ended_is_freed_on_another() {
        // Entries of 3,918 bytes, in a pool class (3,905-3,968) no other
        // test allocates, so its slabs can be counted.
        assert_eq!(field_header_for(8, 3_900, false).size(), 3_918);
        let class = crate::value_slab::pool_class_of(3_918).unwrap();
        let before = crate::value_slab::slabs_of_class(class);
        let n = scale(200);
        let table = std::thread::spawn(move || {
            let mut t = CollectionTable::<Fields>::with_capacity_and_seed(0, 38);
            for i in 0..n {
                t.insert(format!("f{i:07}").as_bytes(), &[b'v'; 3_900], None)
                    .unwrap();
            }
            t
        })
        .join()
        .unwrap();
        assert!(crate::value_slab::slabs_of_class(class) > before);
        table.check();
        assert_eq!(table.entry_bytes, n * 3_968);
        std::thread::spawn(move || drop(table)).join().unwrap();
        assert_eq!(crate::value_slab::slabs_of_class(class), before);
    }

    /// Writes between the pages of a scan while a resize with a new seed is
    /// in progress: the scan ends, its cursor only goes forward (it may miss
    /// elements); a scan started after the resize has ended returns every
    /// element exactly once.
    #[test]
    fn a_scan_with_writes_during_a_resize_with_a_new_seed() {
        let n = if cfg!(miri) { 80 } else { 300 };
        let top = if cfg!(miri) { 5 } else { 10 };
        let fields = colliding(39, n, top);
        let mut table = HashT::with_capacity_and_seed(0, 39);
        for f in &fields {
            table.insert(f, b"x", None).unwrap();
            if reseeding(&table) {
                break;
            }
        }
        assert!(reseeding(&table));
        let mut model: HashSet<Vec<u8>> = table.iter().map(|e| e.field.to_vec()).collect();
        let mut cursor = 0;
        let mut pages = 0usize;
        let mut extra = 0usize;
        let mut returned = HashSet::new();
        loop {
            let next = table.scan(cursor, 3, |e| {
                returned.insert(e.field.to_vec());
            });
            pages += 1;
            assert!(pages < 100_000, "the scan ends");
            // A write between pages: a step of the resize.
            let f = format!("extra-{extra}").into_bytes();
            table.insert(&f, b"y", None).unwrap();
            model.insert(f);
            extra += 1;
            if next == 0 {
                break;
            }
            assert!(next > cursor, "the cursor goes forward");
            cursor = next;
        }
        assert!(returned.iter().all(|f| model.contains(f)));
        while !table.step_resize(usize::MAX) {}
        assert!(!reseeding(&table));
        table.check();
        let all = scan_fields(&table, 3);
        assert_eq!(all.len(), model.len());
        assert!(all.values().all(|&n| n == 1));
    }

    /// A resize with a new seed that starts above 3/4 of the growth point
    /// grows the array too; one below it keeps its size.
    #[test]
    fn a_resize_with_a_new_seed_near_the_growth_point_grows() {
        let top = if cfg!(miri) { 8 } else { 10 };
        let fields = colliding(40, if cfg!(miri) { 60 } else { 100 }, top);
        for (fill, grows) in [(0.80f64, true), (0.40, false)] {
            let mut table = HashT::with_capacity_and_seed(0, 40);
            // To `fill` of the growth point of 2^(top - 1) groups (2^7
            // under Miri), with fields that never meet in one group.
            let mut i = 0usize;
            while table.bits < top as u8 - 1 || table.resizing() {
                table
                    .insert(format!("other-{i}").as_bytes(), b"v", None)
                    .unwrap();
                i += 1;
                while !table.step_resize(usize::MAX) {}
            }
            while (table.len() as f64) < fill * table.grow_at as f64 {
                table
                    .insert(format!("other-{i}").as_bytes(), b"v", None)
                    .unwrap();
                i += 1;
            }
            while !table.step_resize(usize::MAX) {}
            let bits = table.bits;
            let mut started = None;
            for f in &fields {
                table.insert(f, b"x", None).unwrap();
                if reseeding(&table) {
                    started = Some(table.bits);
                    break;
                }
            }
            let started = started.expect("a resize with a new seed");
            assert_eq!(started, if grows { bits + 1 } else { bits }, "fill {fill}");
            while !table.step_resize(usize::MAX) {}
            table.check();
        }
    }

    /// With the cap made 1 (every overflow is "long"), resizes with a new
    /// seed stop after MAX_RESEEDS_IN_A_ROW with no growth, and start again
    /// once the table has grown.
    #[test]
    fn resizes_with_a_new_seed_stop_after_three_in_a_row() {
        TEST_CHAIN_CAP.with(|cap| cap.set(1));
        // Not scaled under Miri: a smaller table may see no overflow at all.
        let mut table = HashT::with_capacity_and_seed(1_000, 41);
        let bits = table.bits;
        let mut i = 0usize;
        while table.len() < table.capacity() / 2 {
            table
                .insert(format!("f{i}").as_bytes(), b"v", None)
                .unwrap();
            i += 1;
            while !table.step_resize(usize::MAX) {}
        }
        assert_eq!(table.bits, bits, "no growth yet");
        assert_eq!(reseeds(&table), u32::from(MAX_RESEEDS_IN_A_ROW));
        table.check();
        // Grown: the count starts afresh.
        while table.bits == bits {
            table
                .insert(format!("f{i}").as_bytes(), b"v", None)
                .unwrap();
            i += 1;
        }
        while reseeds(&table) <= u32::from(MAX_RESEEDS_IN_A_ROW) && i < 20_000 {
            table
                .insert(format!("f{i}").as_bytes(), b"v", None)
                .unwrap();
            i += 1;
        }
        while !table.step_resize(usize::MAX) {}
        assert!(reseeds(&table) > u32::from(MAX_RESEEDS_IN_A_ROW));
        TEST_CHAIN_CAP.with(|cap| cap.set(CHAIN_CAP));
        table.check();
        for j in 0..i {
            assert!(table.contains_key(format!("f{j}").as_bytes()));
        }
    }

    /// A reserve while a resize is in progress hurries nothing: the growth
    /// it asks for starts when that resize ends.
    #[test]
    fn a_reserve_during_a_resize_waits_for_it() {
        let mut table = SetT::with_capacity_and_seed(0, 42);
        let mut i = 0usize;
        while !(table.resizing() && table.len() > scale(2_000)) {
            table.insert(format!("member-{i}").as_bytes()).unwrap();
            i += 1;
        }
        let next = table.rare.as_ref().unwrap().old.as_ref().unwrap().next;
        let bits = table.bits;
        table.reserve(scale(50_000));
        assert!(table.resizing());
        assert_eq!(table.bits, bits);
        assert_eq!(
            table.rare.as_ref().unwrap().old.as_ref().unwrap().next,
            next,
            "no moves made by the reserve"
        );
        while table.bits == bits {
            table.step_resize(64);
        }
        assert!(table.capacity() >= table.len() + scale(50_000));
        while !table.step_resize(usize::MAX) {}
        table.check();
    }

    /// Entries of the default memory, the entry pool, cost their class's
    /// size: 8 bytes apart up to 64.
    #[test]
    fn pool_entries_cost_their_class() {
        let mut hash = CollectionTable::<Fields>::with_capacity_and_seed(0, 35);
        let mut set = CollectionTable::<Members>::with_capacity_and_seed(0, 35);
        let n = scale(5_000);
        for i in 0..n {
            // 11-byte entries (16), 21-byte members (24), 60-byte entries (64).
            hash.insert(format!("f{i:07}").as_bytes(), b"v", None)
                .unwrap();
            set.insert(format!("member-{i:0>13}").as_bytes()).unwrap();
            hash.insert(format!("g{i:07}").as_bytes(), &[b'w'; 50], None)
                .unwrap();
        }
        assert_eq!(field_header_for(8, 1, false).size(), 11);
        assert_eq!(member_size(20), 21);
        assert_eq!(field_header_for(8, 50, false).size(), 60);
        assert_eq!(hash.entry_bytes, n * (16 + 64));
        assert_eq!(set.entry_bytes, n * 24);
        hash.check();
        set.check();
        // Freed on another thread, as `lazyfree` would.
        std::thread::spawn(move || drop((hash, set)))
            .join()
            .unwrap();
    }

    #[test]
    fn reserve_makes_room() {
        let more = scale(10_000);
        let mut table = SetT::with_capacity_and_seed(0, 33);
        for i in 0..100 {
            table.insert(format!("member-{i}").as_bytes()).unwrap();
        }
        table.reserve(more);
        // A growth in progress (from the 100) ends first, then the
        // reserved one starts.
        while !table.step_resize(usize::MAX) {}
        assert!(table.capacity() >= 100 + more);
        let bits = table.bits;
        for i in 100..100 + more {
            table.insert(format!("member-{i}").as_bytes()).unwrap();
        }
        assert_eq!(table.bits, bits, "no growth after the reserve");
        table.check();
        let mut empty = SetT::with_capacity_and_seed(0, 34);
        empty.reserve(1_000);
        let bits = empty.bits;
        assert!(empty.capacity() >= 1_000);
        for i in 0..1_000 {
            empty.insert(format!("member-{i}").as_bytes()).unwrap();
        }
        assert_eq!(empty.bits, bits);
        while !empty.step_resize(usize::MAX) {}
        assert_eq!(empty.bits, bits, "a reserve is not shrunk back");
        empty.check();
    }
}
