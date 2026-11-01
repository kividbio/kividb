//! Where an HNSW graph keeps what a search reads for every node it meets:
//! the node's vector, its layer-0 links and whether it is live. All three
//! are in blocks of fixed-size slots that never move, slot `id` for node
//! `id`, so that a node's vector is found from its id by arithmetic alone
//! and can be prefetched before it is measured.
//!
//! Before, each node's vector was a heap allocation of its own, reached
//! through the node, and its links a `Vec<Vec<usize>>` behind a mutex: two
//! dependent loads before the first byte of the vector, and a lock for each
//! node expanded. A search of a 1536-dimension graph reads some thousands of
//! 6 KB vectors from memory, and spent most of its time waiting for them.
//!
//! Blocks start small (8 slots, doubling up to 1024) so that a small graph
//! holds little more than it needs, and are then all of 1024 slots: at most
//! one block is partly empty.
//!
//! Layer-0 links are read without a lock: a writer (holding the node's own
//! lock, [`super::HnswNode`]) stores the ids first and the count after, and a
//! reader that races it may see a mix of the old list and the new one. Every
//! id ever stored names a node of the graph, so the mix only changes which
//! nodes the search measures, as hnswlib's lock-free reads do.

use std::alloc::{Layout, alloc, alloc_zeroed, dealloc};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

/// Slots of the first block.
const FIRST: usize = 8;
/// Blocks that double in size: 8, 16, ..., 512 slots.
const GEOMETRIC: usize = 7;
/// Slots in the doubling blocks together.
const GEOMETRIC_SLOTS: usize = FIRST * ((1 << GEOMETRIC) - 1);
/// Slots in each block after them.
const BLOCK: usize = 1024;
/// Each vector starts on a cache line.
const ALIGN: usize = 64;
/// Floats in a cache line: a vector's slot is a whole number of lines.
const LINE_FLOATS: usize = ALIGN / 4;

/// The block slot `id` is in, and its place there.
#[inline(always)]
fn locate(id: usize) -> (usize, usize) {
    if id < GEOMETRIC_SLOTS {
        let x = id / FIRST + 1;
        let b = (usize::BITS - 1 - x.leading_zeros()) as usize;
        (b, id - FIRST * ((1 << b) - 1))
    } else {
        let r = id - GEOMETRIC_SLOTS;
        (GEOMETRIC + r / BLOCK, r % BLOCK)
    }
}

/// Slots in block `b`.
#[inline(always)]
fn block_slots(b: usize) -> usize {
    if b < GEOMETRIC { FIRST << b } else { BLOCK }
}

/// One allocation: the vectors of its slots, then their layer-0 links, then
/// their live flags.
struct Block {
    base: NonNull<u8>,
    /// Byte offsets of the links and of the flags.
    links_at: usize,
    flags_at: usize,
}

// The block is shared memory reached only through `Slots`, whose methods
// say who may write what and when.
unsafe impl Send for Block {}
unsafe impl Sync for Block {}

/// The slots of one graph.
pub struct Slots {
    /// Floats in a vector.
    dim: usize,
    /// Floats in a vector's slot: `dim` rounded up to whole cache lines.
    stride: usize,
    /// u32s in a links slot: the count, then up to `m0` ids.
    links: usize,
    blocks: boxcar::Vec<Block>,
    /// Held to add a block.
    grow: parking_lot::Mutex<()>,
}

impl std::fmt::Debug for Slots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slots")
            .field("dim", &self.dim)
            .field("capacity", &self.capacity())
            .finish()
    }
}

impl Slots {
    /// Slots for vectors of `dim` floats and up to `m0` layer-0 links.
    pub fn new(dim: usize, m0: usize) -> Self {
        Self {
            dim,
            stride: dim.div_ceil(LINE_FLOATS).max(1) * LINE_FLOATS,
            links: m0 + 1,
            blocks: boxcar::Vec::new(),
            grow: parking_lot::Mutex::new(()),
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// The most layer-0 links a slot holds.
    pub fn max_links(&self) -> usize {
        self.links - 1
    }

    /// Byte offsets of the links and of the flags in a block of `slots`,
    /// and the block's layout.
    fn layout(&self, slots: usize) -> (usize, usize, Layout) {
        let links_at = slots * self.stride * 4;
        let flags_at = links_at + slots * self.links * 4;
        let size = (flags_at + slots).next_multiple_of(ALIGN);
        // A block of a huge page or more starts on one, so that the kernel
        // can back it with huge pages (see `advise_huge`).
        let align = if size >= HUGE_PAGE { HUGE_PAGE } else { ALIGN };
        let layout = Layout::from_size_align(size, align).expect("block layout");
        (links_at, flags_at, layout)
    }

    /// Slots there is room for.
    pub fn capacity(&self) -> usize {
        (0..self.blocks.count()).map(block_slots).sum()
    }

    /// Bytes the blocks take.
    pub fn bytes(&self) -> usize {
        (0..self.blocks.count())
            .map(|b| self.layout(block_slots(b)).2.size())
            .sum()
    }

    /// Make room for slot `id`. Zeroed: no links, not live.
    pub fn reserve(&self, id: usize) {
        let (b, _) = locate(id);
        if self.blocks.count() > b {
            return;
        }
        let _grow = self.grow.lock();
        while self.blocks.count() <= b {
            let next = self.blocks.count();
            let (links_at, flags_at, layout) = self.layout(block_slots(next));
            let huge = layout.align() == HUGE_PAGE;
            // SAFETY: the layout's size is never zero (a block has slots).
            // A huge block is advised before it is zeroed: the zeroing
            // faults its pages in, and they are huge only if asked for
            // before.
            let ptr = unsafe {
                if huge {
                    alloc(layout)
                } else {
                    alloc_zeroed(layout)
                }
            };
            let Some(base) = NonNull::new(ptr) else {
                std::alloc::handle_alloc_error(layout);
            };
            if huge {
                advise_huge(ptr, layout.size());
                // SAFETY: the allocation is `layout.size()` bytes.
                unsafe { ptr.write_bytes(0, layout.size()) };
            }
            self.blocks.push(Block {
                base,
                links_at,
                flags_at,
            });
        }
    }

    /// Whether slot `id` has been made room for.
    #[inline(always)]
    pub fn has(&self, id: usize) -> bool {
        locate(id).0 < self.blocks.count()
    }

    #[inline(always)]
    fn block(&self, b: usize) -> &Block {
        // Every id handed out was reserved first.
        self.blocks.get(b).expect("slot of an id never reserved")
    }

    #[inline(always)]
    fn vector_ptr(&self, id: usize) -> *mut f32 {
        let (b, i) = locate(id);
        // SAFETY: `i` is within block `b`, which holds `block_slots(b)`
        // vector slots of `stride` floats from its start.
        unsafe {
            self.block(b)
                .base
                .as_ptr()
                .cast::<f32>()
                .add(i * self.stride)
        }
    }

    /// The vector in slot `id`. Only to be read while nothing writes it:
    /// see [`Slots::write_vector`].
    #[inline(always)]
    pub fn vector(&self, id: usize) -> &[f32] {
        // SAFETY: the slot holds `dim` initialised floats (zeroed or
        // written), in a block that lives as long as `self`.
        unsafe { std::slice::from_raw_parts(self.vector_ptr(id), self.dim) }
    }

    /// Write slot `id`'s vector.
    ///
    /// # Safety
    /// No one may read the slot meanwhile: it is a slot no reader can have
    /// reached yet (one being filled for a new node), or the caller holds
    /// the graph for writing.
    pub unsafe fn write_vector(&self, id: usize, v: &[f32]) {
        debug_assert_eq!(v.len(), self.dim);
        let n = v.len().min(self.dim);
        // SAFETY: the slot has room for `dim` floats; no one reads it.
        unsafe { std::ptr::copy_nonoverlapping(v.as_ptr(), self.vector_ptr(id), n) };
    }

    /// Slot `id`'s layer-0 links: the count, then the ids.
    #[inline(always)]
    pub fn links(&self, id: usize) -> &[AtomicU32] {
        let (b, i) = locate(id);
        let block = self.block(b);
        // SAFETY: the links region of the block holds `block_slots(b)`
        // slots of `links` u32s, zeroed at allocation; AtomicU32 has the
        // layout of u32.
        unsafe {
            let p = block
                .base
                .as_ptr()
                .add(block.links_at)
                .cast::<AtomicU32>()
                .add(i * self.links);
            std::slice::from_raw_parts(p, self.links)
        }
    }

    /// Slot `id`'s live flag: 1 while a node of the graph is there.
    #[inline(always)]
    pub fn flag(&self, id: usize) -> &AtomicU8 {
        let (b, i) = locate(id);
        let block = self.block(b);
        // SAFETY: the flags region holds a byte a slot, zeroed at
        // allocation; AtomicU8 has the layout of u8.
        unsafe {
            &*block
                .base
                .as_ptr()
                .add(block.flags_at + i)
                .cast::<AtomicU8>()
        }
    }

    /// Whether slot `id` holds a live node. Acquire: the vector written
    /// before the flag was set is seen.
    #[inline(always)]
    pub fn is_live(&self, id: usize) -> bool {
        self.flag(id).load(Ordering::Acquire) != 0
    }

    /// Slot `id`'s layer-0 links, appended to `out`, read without a lock.
    #[inline]
    pub fn read_links(&self, id: usize, out: &mut Vec<u32>) {
        let slot = self.links(id);
        let n = (slot[0].load(Ordering::Acquire) as usize).min(self.links - 1);
        out.extend(slot[1..=n].iter().map(|x| x.load(Ordering::Relaxed)));
    }

    /// Replace slot `id`'s layer-0 links. The caller holds the node's lock
    /// (writers of one node's links take turns); readers may race it.
    #[inline]
    pub fn write_links(&self, id: usize, ids: impl IntoIterator<Item = u32>) {
        let slot = self.links(id);
        let mut n = 0;
        for x in ids.into_iter().take(self.links - 1) {
            slot[1 + n].store(x, Ordering::Relaxed);
            n += 1;
        }
        slot[0].store(n as u32, Ordering::Release);
    }

    /// Add `x` to slot `id`'s layer-0 links if there is room; false if
    /// they are full. The caller holds the node's lock.
    #[inline]
    pub fn push_link(&self, id: usize, x: u32) -> bool {
        let slot = self.links(id);
        let n = slot[0].load(Ordering::Relaxed) as usize;
        if n >= self.links - 1 {
            return false;
        }
        slot[1 + n].store(x, Ordering::Relaxed);
        slot[0].store(n as u32 + 1, Ordering::Release);
        true
    }

    /// Ask for the first `lines` cache lines of slot `id`'s vector (all of
    /// it if there are fewer) to be brought into the cache.
    #[inline(always)]
    pub fn prefetch(&self, id: usize, lines: usize) {
        let p = self.vector_ptr(id).cast::<u8>();
        let lines = lines.min(self.stride / LINE_FLOATS);
        for l in 0..lines {
            // SAFETY: within the slot; a prefetch never faults anyway.
            prefetch(unsafe { p.add(l * ALIGN) });
        }
    }
}

impl Drop for Slots {
    fn drop(&mut self) {
        for b in 0..self.blocks.count() {
            if let Some(block) = self.blocks.get(b) {
                let (_, _, layout) = self.layout(block_slots(b));
                // SAFETY: allocated in `reserve` with this layout.
                unsafe { dealloc(block.base.as_ptr(), layout) };
            }
        }
    }
}

/// The size of a huge page on Linux x86_64 and aarch64 (4 KB base pages).
const HUGE_PAGE: usize = 2 << 20;

/// Ask the kernel to back `len` bytes at `p` with transparent huge pages.
/// A search reads vectors from all over a large graph: with 4 KB pages
/// nearly every vector costs a page walk, and with 2 MB pages almost none.
/// Where THP is `madvise` (Ubuntu's default) nothing is huge unless asked
/// for; where it is `never`, or elsewhere than Linux, this does nothing.
/// Measured on dbpedia-openai-100K-1536 (D16s_v5): 5% more queries a
/// second, and builds 3% faster.
fn advise_huge(p: *mut u8, len: usize) {
    #[cfg(target_os = "linux")]
    // SAFETY: the range is one allocation of ours; the advice changes how
    // it is backed, never what it holds.
    unsafe {
        libc::madvise(p.cast(), len, libc::MADV_HUGEPAGE);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (p, len);
}

/// A hint that the cache line at `p` will be read soon.
#[inline(always)]
fn prefetch(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch has no effect on memory and never faults.
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(p.cast::<i8>());
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: as above.
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{p}]", p = in(reg) p, options(nostack, preserves_flags, readonly));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = p;
}

/// The nodes a search has met, one bit each. Emptied by clearing only the
/// words it set, so that a search of a large graph does not pay for the
/// whole graph to start.
#[derive(Default)]
pub struct Visited {
    words: Vec<u64>,
    dirty: Vec<u32>,
}

impl Visited {
    /// Empty, for a graph of up to `ids` nodes (it grows past that if the
    /// graph does meanwhile).
    pub fn reset(&mut self, ids: usize) {
        for w in self.dirty.drain(..) {
            self.words[w as usize] = 0;
        }
        let words = ids.div_ceil(64);
        if self.words.len() < words {
            self.words.resize(words, 0);
        }
    }

    /// Mark `id`; false if it was already marked.
    #[inline(always)]
    pub fn insert(&mut self, id: u32) -> bool {
        let w = (id >> 6) as usize;
        if w >= self.words.len() {
            self.words.resize(w + 1, 0);
        }
        let bit = 1u64 << (id & 63);
        let word = &mut self.words[w];
        if *word & bit != 0 {
            return false;
        }
        if *word == 0 {
            self.dirty.push(w as u32);
        }
        *word |= bit;
        true
    }

    #[cfg(test)]
    fn contains(&self, id: u32) -> bool {
        let w = (id >> 6) as usize;
        w < self.words.len() && self.words[w] & (1u64 << (id & 63)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_id_has_a_slot_of_its_own() {
        let mut seen = std::collections::HashSet::new();
        let mut last = (0, 0);
        for id in 0..10_000 {
            let (b, i) = locate(id);
            assert!(i < block_slots(b), "id {id}: {b}/{i}");
            assert!(seen.insert((b, i)));
            if id > 0 {
                // Consecutive ids fill a block before the next.
                assert!(
                    (b == last.0 && i == last.1 + 1) || (b == last.0 + 1 && i == 0),
                    "id {id}"
                );
            }
            last = (b, i);
        }
    }

    #[test]
    fn vectors_links_and_flags_are_kept_apart() {
        let slots = Slots::new(25, 32);
        for id in 0..3000 {
            slots.reserve(id);
            let v: Vec<f32> = (0..25).map(|j| (id * 100 + j) as f32).collect();
            unsafe { slots.write_vector(id, &v) };
            slots.write_links(id, (0..(id % 33) as u32).map(|x| x + id as u32));
            slots.flag(id).store(1, Ordering::Release);
        }
        let mut out = Vec::new();
        for id in 0..3000 {
            let v = slots.vector(id);
            assert_eq!(v.len(), 25);
            assert_eq!(v[0], (id * 100) as f32);
            assert_eq!(v[24], (id * 100 + 24) as f32);
            assert_eq!(v.as_ptr() as usize % ALIGN, 0, "slot {id} aligned");
            out.clear();
            slots.read_links(id, &mut out);
            assert_eq!(out.len(), id % 33);
            assert!(slots.is_live(id));
        }
        assert!(slots.capacity() >= 3000 && slots.capacity() < 3000 + BLOCK);
    }

    #[test]
    fn a_full_links_slot_takes_no_more() {
        let slots = Slots::new(4, 2);
        slots.reserve(0);
        assert!(slots.push_link(0, 7));
        assert!(slots.push_link(0, 8));
        assert!(!slots.push_link(0, 9));
        let mut out = Vec::new();
        slots.read_links(0, &mut out);
        assert_eq!(out, [7, 8]);
    }

    #[test]
    fn visited_resets_only_what_it_set() {
        let mut v = Visited::default();
        v.reset(1000);
        assert!(v.insert(5));
        assert!(!v.insert(5));
        assert!(v.insert(999));
        assert!(v.insert(5000), "grows past the size it was reset for");
        v.reset(1000);
        assert!(!v.contains(5) && !v.contains(999) && !v.contains(5000));
        assert!(v.insert(5));
    }
}
