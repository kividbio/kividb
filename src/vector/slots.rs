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
//! A FLAT index keeps its vectors in slots of its own, with no links
//! ([`Slots::vectors_only`], see `super::flat`).
//!
//! Blocks start small (8 slots, doubling up to 1024) so that a small graph
//! holds little more than it needs, and are then all of 1024 slots: at most
//! one block is partly empty.
//!
//! **Huge pages.** A block of 1,024 slots is under 2 MiB up to 464
//! dimensions (at `M` 16): in 4 KiB pages, an insert into a graph of 200,000
//! 64-d vectors walks the page tables for most of the neighbours it
//! measures, and a quarter of `search_layer`'s time goes to its prefetches of
//! their vectors. On Linux such blocks are cut, back to back, from chunks of
//! whole huge pages mapped for the graph ([`Chunk`]). A chunk is mapped with
//! huge pages off (`MADV_NOHUGEPAGE`), so that no fault makes one, whatever
//! transparent huge pages are set to. Once the blocks over one of its huge
//! pages are all in use, which is when the block after them is made, the
//! page is handed to a thread of its own that turns huge pages on for it and
//! makes it one (`MADV_COLLAPSE`): no command, and no insert holding its
//! graph, waits for that. A graph's blocks are then in huge pages but for the
//! last one or two, and nothing is in a huge page before it is used: a block
//! is counted in `used_memory` at its size, and the pages of a chunk no block
//! has reached are never touched. Where huge pages are set to `never`, or the
//! kernel cannot make one, the pages stay as they are. A chunk is one
//! mapping (two while part of it is huge pages); past [`MOST_MAPPINGS`] of
//! them in the process, new blocks are allocated on their own.
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
    /// Cut from a [`Chunk`] (given back with it), not allocated on its own.
    in_chunk: bool,
}

/// Whole huge pages mapped for one graph, from which blocks of 1,024 slots
/// are cut back to back (see the module's comment).
#[cfg(all(target_os = "linux", not(miri)))]
struct Chunk {
    map: std::sync::Arc<Mapping>,
    /// Bytes cut into blocks so far.
    used: usize,
    /// Bytes from the start already handed to be made huge pages.
    collapsed: usize,
}

/// Elsewhere, no chunk is ever made.
#[cfg(not(all(target_os = "linux", not(miri))))]
struct Chunk;

/// A chunk's mapping, held by its graph's `Slots` and by the collapses
/// waiting for it ([`Collapse`]): unmapped when the last of them lets go.
#[cfg(all(target_os = "linux", not(miri)))]
struct Mapping {
    base: NonNull<u8>,
    /// Bytes mapped.
    len: usize,
}

// SAFETY: the mapping is memory of its own, reached through the blocks cut
// from it (whose users `Slots` governs) and by `madvise`, which never
// changes what it holds.
#[cfg(all(target_os = "linux", not(miri)))]
unsafe impl Send for Mapping {}
#[cfg(all(target_os = "linux", not(miri)))]
unsafe impl Sync for Mapping {}

#[cfg(all(target_os = "linux", not(miri)))]
impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: our own mapping; no block cut from it is used any more
        // (its `Slots` is gone) and no collapse holds it.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
        MAPPINGS.fetch_sub(1, Ordering::Relaxed);
    }
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
    /// Held to add a block; the chunks blocks are cut from.
    grow: parking_lot::Mutex<Vec<Chunk>>,
    /// How a vector is laid out in its `dim` floats, and so measured.
    encoding: super::quant::Encoding,
    /// Whether the vectors of a block are zeroed when it is made: not those
    /// of a FLAT index ([`Slots::vectors_only`]), each written before it is
    /// read. A block of a huge page or more is zeroed by hand (see
    /// `reserve`): at 1536 dimensions, 600 MB more to write for 100,000
    /// vectors than the vectors themselves.
    zero_vectors: bool,
}

impl std::fmt::Debug for Slots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slots")
            .field("dim", &self.dim)
            .field("capacity", &self.capacity())
            .finish()
    }
}

/// [`Slots::measure`]: how two vectors of the slots are measured.
#[derive(Clone, Copy)]
pub struct Measure<'a> {
    how: How,
    metric: &'a super::DistanceMetric,
    kernels: super::simd::Kernels,
}

#[derive(Clone, Copy)]
enum How {
    /// f32 at unit length (cosine) or as given (inner product): 1 − a·b.
    OneMinusDot,
    /// f32, L2: Σ (a − b)².
    L2Sq,
    /// A vector set's encoding.
    Encoded(super::quant::Encoding),
}

impl Measure<'_> {
    /// The distance between `a` and `b`, as [`Slots::dist`] gives it.
    #[inline(always)]
    pub fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.how {
            How::OneMinusDot => 1.0 - self.kernels.dot(a, b),
            How::L2Sq => self.kernels.l2_sq(a, b),
            How::Encoded(encoding) => encoded_dist(encoding, self.metric, a, b),
        }
    }
}

/// A vector set's distance, kept out of line: inlined into every search
/// loop, the quantized arms cost FT.SEARCH's f32 graphs up to 13% of their
/// q/s at 64 dimensions.
#[inline(never)]
fn encoded_dist(
    encoding: super::quant::Encoding,
    metric: &super::DistanceMetric,
    a: &[f32],
    b: &[f32],
) -> f32 {
    encoding.distance(metric, a, b)
}

impl Slots {
    /// Slots for vectors of `dim` floats and up to `m0` layer-0 links.
    pub fn new(dim: usize, m0: usize) -> Self {
        Self::with_encoding(dim, m0, super::quant::Encoding::F32)
    }

    /// Slots for vectors of `dim` floats and no links: a FLAT index's
    /// (`super::flat`). Their links are never read or written; their live
    /// flags are not used. A slot's vector is not zeroed when its block is
    /// made: it is to be written ([`Slots::fill_vector`]) before it is read.
    pub fn vectors_only(dim: usize) -> Self {
        let mut slots = Self::new(dim, 0);
        slots.links = 0;
        slots.zero_vectors = false;
        slots
    }

    /// Slots for vectors laid out as `encoding` has them, in `dim` floats.
    pub fn with_encoding(dim: usize, m0: usize, encoding: super::quant::Encoding) -> Self {
        Self {
            dim,
            stride: dim.div_ceil(LINE_FLOATS).max(1) * LINE_FLOATS,
            links: m0 + 1,
            blocks: boxcar::Vec::new(),
            grow: parking_lot::Mutex::new(Vec::new()),
            encoding,
            zero_vectors: true,
        }
    }

    /// [`Slots::dist`] by `metric`, with the kernel looked up once: for a
    /// loop that measures many vectors (a search, a pruning). The same
    /// distances, to the bit: the same kernels.
    #[inline(always)]
    pub fn measure<'a>(&self, metric: &'a super::DistanceMetric) -> Measure<'a> {
        use super::DistanceMetric::{Cosine, InnerProduct, L2};
        let how = match (self.encoding, metric) {
            (super::quant::Encoding::F32, Cosine | InnerProduct) => How::OneMinusDot,
            (super::quant::Encoding::F32, L2) => How::L2Sq,
            (other, _) => How::Encoded(other),
        };
        Measure {
            how,
            metric,
            kernels: super::simd::Kernels::here(),
        }
    }

    /// How the vectors are laid out.
    #[inline(always)]
    pub fn encoding(&self) -> super::quant::Encoding {
        self.encoding
    }

    /// The distance between two vectors as these slots hold them (`a` and
    /// `b` from [`Slots::vector`], or a query laid out alike).
    #[inline(always)]
    pub fn dist(&self, metric: &super::DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
        match self.encoding {
            super::quant::Encoding::F32 => super::distance_prenorm(metric, a, b),
            other => encoded_dist(other, metric, a, b),
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// The most layer-0 links a slot holds.
    pub fn max_links(&self) -> usize {
        self.links.saturating_sub(1)
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
        // Huge pages to be made of what the new blocks leave in use: handed
        // to their own thread once the lock is let go (see `hand_collapses`).
        let mut collapses = Vec::new();
        let mut chunks = self.grow.lock();
        while self.blocks.count() <= b {
            let next = self.blocks.count();
            let (links_at, flags_at, layout) = self.layout(block_slots(next));
            if next >= GEOMETRIC
                && layout.align() != HUGE_PAGE
                && let Some(base) = cut_block(&mut chunks, layout.size(), &mut collapses)
            {
                self.blocks.push(Block {
                    base,
                    links_at,
                    flags_at,
                    in_chunk: true,
                });
                continue;
            }
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
                // Its links and flags, and its vectors unless they are each
                // written before they are read.
                let from = if self.zero_vectors { 0 } else { links_at };
                // SAFETY: the allocation is `layout.size()` bytes.
                unsafe { ptr.add(from).write_bytes(0, layout.size() - from) };
            }
            self.blocks.push(Block {
                base,
                links_at,
                flags_at,
                in_chunk: false,
            });
        }
        drop(chunks);
        hand_collapses(collapses);
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
    /// see [`Slots::write_vector`]; of [`Slots::vectors_only`], only once
    /// written.
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

    /// `f` of slot `id`'s vector, to write it in place: `f` writes each of
    /// its `dim` floats.
    ///
    /// # Safety
    /// As [`Slots::write_vector`]: no one reads the slot meanwhile.
    pub unsafe fn fill_vector(&self, id: usize, f: impl FnOnce(&mut [std::mem::MaybeUninit<f32>])) {
        // SAFETY: the slot has room for `dim` floats, in a block that lives
        // as long as `self`, which may not be written yet (`MaybeUninit`); no
        // one else reads or writes it meanwhile.
        f(unsafe {
            std::slice::from_raw_parts_mut(
                self.vector_ptr(id).cast::<std::mem::MaybeUninit<f32>>(),
                self.dim,
            )
        })
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

    /// `f` of each of slot `id`'s layer-0 links in turn, read without a
    /// lock as [`Slots::read_links`] reads them, without copying them out
    /// first: a search that keeps only the neighbours it has not met.
    #[inline(always)]
    pub fn each_link(&self, id: usize, mut f: impl FnMut(u32)) {
        let slot = self.links(id);
        let n = (slot[0].load(Ordering::Acquire) as usize).min(self.links - 1);
        for x in &slot[1..=n] {
            f(x.load(Ordering::Relaxed));
        }
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
                if block.in_chunk {
                    crate::memory_use::note(-(layout.size() as isize));
                } else {
                    // SAFETY: allocated in `reserve` with this layout.
                    unsafe { dealloc(block.base.as_ptr(), layout) };
                }
            }
        }
        // The chunks' mappings go with the last of their holders: this, or
        // a collapse still waiting for its thread.
        self.grow.get_mut().clear();
    }
}

/// The size of a huge page on Linux x86_64 and aarch64 (4 KB base pages).
const HUGE_PAGE: usize = 2 << 20;

/// The most blocks a chunk holds: chunks grow no larger than about 64 MiB.
#[cfg(all(target_os = "linux", not(miri)))]
const CHUNK_MOST_BYTES: usize = 64 << 20;

/// The most chunk mappings the process holds; past them a new block is
/// allocated on its own, as elsewhere than Linux. A chunk is one mapping, two
/// while only part of it is huge pages, against `vm.max_map_count` (65,530
/// by default) for the whole process.
#[cfg(all(target_os = "linux", not(miri)))]
const MOST_MAPPINGS: usize = 16 * 1024;

/// Chunk mappings the process holds.
#[cfg(all(target_os = "linux", not(miri)))]
static MAPPINGS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Bytes of a chunk for blocks of `block` bytes: whole huge pages, the
/// fewest that leave at most 1/32 of them past the last block that fits
/// (64-d: 5 blocks of 405 KiB in one huge page; 384-d: 6 of 1.6 MiB in 5).
#[cfg(all(target_os = "linux", not(miri)))]
fn chunk_bytes(block: usize) -> usize {
    let mut best = HUGE_PAGE.max(block.next_multiple_of(HUGE_PAGE));
    let mut pages = best / HUGE_PAGE;
    while pages * HUGE_PAGE <= CHUNK_MOST_BYTES {
        let len = pages * HUGE_PAGE;
        if len % block <= len / 32 {
            best = len;
            break;
        }
        pages += 1;
    }
    best
}

/// Huge pages to be made of `[from, to)` of a chunk's mapping, by the
/// collapse thread ([`hand_collapses`]).
#[cfg(all(target_os = "linux", not(miri)))]
struct Collapse {
    map: std::sync::Arc<Mapping>,
    from: usize,
    to: usize,
}

#[cfg(not(all(target_os = "linux", not(miri))))]
struct Collapse;

/// A block of `size` bytes cut from the graph's last chunk, or from a new
/// one: `None` where chunks are not used (not Linux), the process holds
/// [`MOST_MAPPINGS`] already, or a chunk cannot be mapped (the block is then
/// allocated on its own). Counted in `used_memory` at `size`, as a block of
/// its own is. The huge pages of the chunk that the blocks before this one
/// cover wholly are added to `collapses`.
fn cut_block(
    chunks: &mut Vec<Chunk>,
    size: usize,
    collapses: &mut Vec<Collapse>,
) -> Option<NonNull<u8>> {
    #[cfg(all(target_os = "linux", not(miri)))]
    {
        let fits = chunks.last().is_some_and(|c| c.used + size <= c.map.len);
        if !fits {
            // The last chunk's blocks are all in use, and no more are cut
            // from it: all of it can be made huge pages (what is left past
            // its last block is at most 1/32 of it).
            if let Some(last) = chunks.last_mut() {
                let len = last.map.len;
                collapse(last, len, collapses);
            }
            let len = chunk_bytes(size);
            let map = map_chunk(len)?;
            chunks.push(Chunk {
                map: std::sync::Arc::new(map),
                used: 0,
                collapsed: 0,
            });
        }
        let chunk = chunks.last_mut()?;
        // The blocks before this one are in use.
        let at = chunk.used;
        collapse(chunk, at, collapses);
        chunk.used += size;
        crate::memory_use::note(size as isize);
        // SAFETY: `at + size <= len`: within the mapping.
        Some(unsafe { chunk.map.base.add(at) })
    }
    #[cfg(not(all(target_os = "linux", not(miri))))]
    {
        let _ = (chunks, size, collapses);
        None
    }
}

/// Note the huge pages of `chunk` below `upto` bytes that have not been
/// noted yet, to be made huge pages ([`hand_collapses`]). Nothing where
/// transparent huge pages are set to `never`.
#[cfg(all(target_os = "linux", not(miri)))]
fn collapse(chunk: &mut Chunk, upto: usize, collapses: &mut Vec<Collapse>) {
    let end = (upto / HUGE_PAGE) * HUGE_PAGE;
    if end <= chunk.collapsed || !huge_pages_allowed() {
        return;
    }
    collapses.push(Collapse {
        map: std::sync::Arc::clone(&chunk.map),
        from: chunk.collapsed,
        to: end,
    });
    chunk.collapsed = end;
}

/// Hand `collapses` to the collapse thread, started on the first. Called
/// with no lock held. The thread asks the kernel to make each range huge
/// pages at once (`MADV_HUGEPAGE`, then `MADV_COLLAPSE`, Linux 6.1 and
/// later; on an older kernel khugepaged does it later), so that no command,
/// and no insert that holds its graph, waits for a collapse: one copies up
/// to 2 MiB, and takes longer where the kernel first has to free a huge
/// page. A page that cannot be made one stays as it is.
fn hand_collapses(collapses: Vec<Collapse>) {
    #[cfg(all(target_os = "linux", not(miri)))]
    {
        if collapses.is_empty() {
            return;
        }
        static START: std::sync::Once = std::sync::Once::new();
        START.call_once(|| {
            use crate::cpus::BuilderExt;
            let started = std::thread::Builder::new()
                .name("hnsw-huge-pages".into())
                .spawn_released(collapse_thread);
            if started.is_err() {
                COLLAPSES.lock().1 = true;
            }
        });
        let mut queue = COLLAPSES.lock();
        if queue.1 {
            return; // no thread: the pages stay as they are
        }
        queue.0.extend(collapses);
        drop(queue);
        COLLAPSE_READY.notify_one();
    }
    #[cfg(not(all(target_os = "linux", not(miri))))]
    let _ = collapses;
}

/// The collapses waiting for the collapse thread, and whether there is
/// none (it could not be started).
#[cfg(all(target_os = "linux", not(miri)))]
static COLLAPSES: parking_lot::Mutex<(std::collections::VecDeque<Collapse>, bool)> =
    parking_lot::const_mutex((std::collections::VecDeque::new(), false));
#[cfg(all(target_os = "linux", not(miri)))]
static COLLAPSE_READY: parking_lot::Condvar = parking_lot::Condvar::new();

#[cfg(all(target_os = "linux", not(miri)))]
fn collapse_thread() {
    loop {
        let job = {
            let mut queue = COLLAPSES.lock();
            loop {
                if let Some(job) = queue.0.pop_front() {
                    break job;
                }
                COLLAPSE_READY.wait(&mut queue);
            }
        };
        // SAFETY: `[from, to)` is within the mapping, which the job holds;
        // the advice changes how its pages are backed, never what they hold.
        unsafe {
            let p = job.map.base.as_ptr().add(job.from).cast::<libc::c_void>();
            libc::madvise(p, job.to - job.from, libc::MADV_HUGEPAGE);
            libc::madvise(p, job.to - job.from, MADV_COLLAPSE);
        }
        drop(job);
    }
}

/// Tests: wait until the collapse thread has nothing left to do.
#[cfg(all(test, target_os = "linux", not(miri)))]
fn collapses_done() {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !COLLAPSES.lock().0.is_empty() && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Whether transparent huge pages are on (`always` or `madvise`), read once:
/// `MADV_COLLAPSE` makes huge pages even where they are set to `never`, and
/// a server whose huge pages are off keeps them off.
#[cfg(all(target_os = "linux", not(miri)))]
fn huge_pages_allowed() -> bool {
    static ALLOWED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    // Not there to read (a sandbox without /sys): the kernel decides.
    *ALLOWED.get_or_init(|| {
        match std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled") {
            Ok(setting) => !setting.contains("[never]"),
            Err(_) => true,
        }
    })
}

/// `MADV_COLLAPSE` (Linux 6.1): collapse a range into huge pages now.
#[cfg(all(target_os = "linux", not(miri)))]
const MADV_COLLAPSE: libc::c_int = 25;

/// Map `len` bytes (whole huge pages) on a huge page's boundary, of
/// zeroes: a fresh anonymous mapping, none of it touched, and none of it to
/// be made huge pages on a fault (`MADV_NOHUGEPAGE`): where they are set to
/// `always`, the first touch of a chunk would otherwise take a whole huge
/// page for a block of 405 KiB. Only the collapses turn huge pages on, for
/// the pages in use. `None` once the process holds [`MOST_MAPPINGS`].
#[cfg(all(target_os = "linux", not(miri)))]
fn map_chunk(len: usize) -> Option<Mapping> {
    if MAPPINGS.fetch_add(1, Ordering::Relaxed) >= MOST_MAPPINGS {
        MAPPINGS.fetch_sub(1, Ordering::Relaxed);
        return None;
    }
    // SAFETY: an anonymous private mapping of no file; the slack before and
    // after the aligned stretch is unmapped again.
    let base = unsafe {
        let span = len + HUGE_PAGE;
        let at = libc::mmap(
            std::ptr::null_mut(),
            span,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if at == libc::MAP_FAILED {
            MAPPINGS.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        let start = at as usize;
        let aligned = start.next_multiple_of(HUGE_PAGE);
        if aligned > start {
            libc::munmap(at, aligned - start);
        }
        let tail = start + span - (aligned + len);
        if tail > 0 {
            libc::munmap((aligned + len) as *mut libc::c_void, tail);
        }
        libc::madvise(aligned as *mut libc::c_void, len, libc::MADV_NOHUGEPAGE);
        NonNull::new(aligned as *mut u8)
    };
    match base {
        Some(base) => Some(Mapping { base, len }),
        None => {
            MAPPINGS.fetch_sub(1, Ordering::Relaxed);
            None
        }
    }
}

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

    /// Blocks cut from chunks, across several chunks and the huge pages
    /// made of them, keep every slot's vector, links and flag apart, each
    /// vector on a cache line, and count what they take at their size.
    #[test]
    fn blocks_cut_from_chunks_keep_their_slots() {
        for dim in [64, 100, 384] {
            let slots = Slots::new(dim, 36);
            let n = 14_000;
            for id in 0..n {
                slots.reserve(id);
                assert!(slots.vector(id).iter().all(|x| *x == 0.0), "zeroed");
                assert!(!slots.is_live(id));
                let v: Vec<f32> = (0..dim).map(|j| (id * 7 + j) as f32).collect();
                unsafe { slots.write_vector(id, &v) };
                slots.write_links(id, (0..(id % 37) as u32).map(|x| x ^ id as u32));
                slots.flag(id).store(1, Ordering::Release);
            }
            let mut out = Vec::new();
            for id in 0..n {
                let v = slots.vector(id);
                assert_eq!(v[0], (id * 7) as f32, "{dim}-d slot {id}");
                assert_eq!(v[dim - 1], (id * 7 + dim - 1) as f32);
                assert_eq!(v.as_ptr() as usize % ALIGN, 0);
                out.clear();
                slots.read_links(id, &mut out);
                assert_eq!(out.len(), id % 37);
                assert!(
                    out.iter()
                        .enumerate()
                        .all(|(x, y)| *y == x as u32 ^ id as u32)
                );
                assert!(slots.is_live(id));
            }
            let blocks: usize = (0..slots.blocks.count())
                .map(|b| slots.layout(block_slots(b)).2.size())
                .sum();
            assert_eq!(slots.bytes(), blocks);
            #[cfg(target_os = "linux")]
            {
                assert!(!slots.grow.lock().is_empty(), "{dim}-d: no chunk was used");
                // The collapses run on their own thread, and change nothing
                // the slots hold.
                collapses_done();
                for id in (0..n).step_by(97) {
                    assert_eq!(slots.vector(id)[0], (id * 7) as f32);
                }
            }
        }
    }

    /// A chunk is whole huge pages, holds at least one block, and leaves at
    /// most 1/32 of itself past the blocks that fit (up to its most).
    /// Graphs dropped while their collapses still wait: the mappings go
    /// once the collapse thread is done with them, and every one does.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_graph_dropped_before_its_collapses_lets_its_mappings_go() {
        for _ in 0..20 {
            let slots = Slots::new(64, 36);
            for id in 0..12_000 {
                slots.reserve(id);
                unsafe { slots.write_vector(id, &[1.0; 64]) };
            }
            drop(slots);
        }
        collapses_done();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        // Other tests may hold chunks meanwhile; these 20 graphs held about
        // 50 mappings each at the most.
        while MAPPINGS.load(Ordering::Relaxed) > 200 {
            assert!(std::time::Instant::now() < until, "mappings kept");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn chunks_are_whole_huge_pages_with_little_left_over() {
        for dim in [1, 16, 64, 100, 128, 384, 512, 768, 1000] {
            let slots = Slots::new(dim, 36);
            let block = slots.layout(BLOCK).2.size();
            if block >= HUGE_PAGE {
                continue; // allocated on its own, as before
            }
            let len = chunk_bytes(block);
            assert_eq!(len % HUGE_PAGE, 0);
            assert!(len >= block);
            assert!(len <= CHUNK_MOST_BYTES.max(block.next_multiple_of(HUGE_PAGE)));
            assert!(
                len % block <= len / 32 || len == block.next_multiple_of(HUGE_PAGE),
                "{dim}-d: {len} for blocks of {block}"
            );
        }
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
