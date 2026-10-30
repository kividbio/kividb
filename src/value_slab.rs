//! Slabs of KiviDB's own for small string values (#113).
//!
//! The system allocator keeps a page for as long as one block on it is in
//! use. After a reshard, or a big delete at random, nearly every page still
//! has a key that stayed on it: with ten million keys of 100 bytes and half
//! of them deleted, the process held 1.86 times what it used, and no
//! allocator measured did better. None of them gives that memory back
//! without moving data, and none but jemalloc says which data to move.
//!
//! Here every block of up to [`MAX_BLOCK`] bytes, header
//! included, is in a slab of one size class whose fill is known, so that
//! compaction can tell the sparse slabs and empty them, and an empty slab's
//! pages go back to the kernel at once. A block also costs only its class's
//! size: the system allocator put a header in front of every block and
//! rounded it up, so a value of 100 bytes (a block of 108) took 128.
//!
//! **Layout.** Memory is taken from the system a [`REGION`] at a time,
//! aligned to its size. A region's first [`SLAB`] bytes hold the headers of
//! its slabs and nothing else, and are never given back, so a header
//! outlives the memory it describes: a free that comes late still finds it.
//! Each of the other slabs holds blocks of one class, sixteen bytes apart in
//! size. Masking a block's address finds its region, and its offset its
//! slab.
//!
//! **Ownership.** A slab with room is used by at most one thread at a time,
//! its owner, which takes blocks from it and gives its own back to it
//! without an atomic or a lock. Any other thread that frees a block pushes
//! it onto the slab's remote list, which the owner takes whole when its own
//! runs dry. A slab changes hands, fills, gets room again or empties under
//! its class's lock, and only then: once per slab per change, not once per
//! value.
//!
//! **Compaction.** [`plan_evacuation`] takes the sparsest slabs nobody owns
//! out of their lists: no block is handed out of them any more, and
//! `compaction` moves the values in them elsewhere, so that they empty.
//! [`finish_evacuation`] puts back those that did not.

use std::cell::{Cell, UnsafeCell};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU8, AtomicU32, AtomicUsize, Ordering};

use parking_lot::Mutex;

/// What memory is taken from the system in, and aligned to.
pub const REGION: usize = 2 << 20;
/// A slab: blocks of one size class.
pub const SLAB: usize = 64 << 10;
/// Slabs in a region, the first of them the headers.
const SLABS: usize = REGION / SLAB;
/// The largest block a slab holds. A block larger than this is the system
/// allocator's.
///
/// Up to 512 bytes the classes are 16 bytes apart, then 32 bytes apart to
/// 2 KiB and 64 to 4 KiB: 32, 48 and 32 classes. A key of 15 bytes and a
/// value of 1,024 make an entry of 1,051 bytes, in the class of 1,056.
pub const MAX_BLOCK: usize = 4096;
const CLASSES: usize = 112;
const _: () = assert!(block_size(CLASSES - 1) == MAX_BLOCK);

// What a slab is doing.
/// A thread's, which takes blocks from it.
const OWNED: u8 = 0;
/// Nobody's, and too full to be listed ([`listable`]).
const FULL: u8 = 1;
/// Nobody's, with room enough: in its class's list, for a thread to take.
const PARTIAL: u8 = 2;
/// Holding nothing, its pages given back: for any class to take.
const EMPTY: u8 = 3;
/// Nobody's, in no list: being emptied by compaction ([`plan_evacuation`]).
const EVACUATING: u8 = 4;

#[repr(C, align(64))]
struct Slab {
    state: AtomicU8,
    class: AtomicU8,
    /// One more each time the slab empties. A free that saw an older one
    /// leaves the slab alone: it has emptied since, and may be another
    /// class's by now.
    generation: AtomicU32,
    /// The thread whose slab it is, while it is [`OWNED`]: the address of
    /// that thread's [`Mine`]. Written only by that thread.
    owner: AtomicUsize,
    /// Blocks handed out and not given back, but for those the owner took
    /// and gave back itself, which it adds in when it lets the slab go.
    live: AtomicI32,
    /// Blocks other threads have given back, each pointing to the next.
    remote: AtomicPtr<u8>,
    /// Where the slab is in its class's list, while it is [`PARTIAL`].
    /// Under the class's lock.
    index: UnsafeCell<usize>,
    // The owner's alone, and handed over with the slab.
    /// Blocks given back on the owner's thread, each pointing to the next.
    local: UnsafeCell<*mut u8>,
    /// Where the part of the slab never handed out starts.
    bump: UnsafeCell<u32>,
    /// Blocks the owner took, less those it gave back, not yet in `live`.
    pending: UnsafeCell<i32>,
}

// SAFETY: the shared fields are atomics; `index` is only touched under the
// class's lock, and `local`, `bump` and `pending` only by the owner, or by
// whoever takes the slab from a list under that lock after the owner let go
// of it under the same lock.
unsafe impl Sync for Slab {}

impl Slab {
    const fn new() -> Self {
        Slab {
            state: AtomicU8::new(EMPTY),
            class: AtomicU8::new(0),
            generation: AtomicU32::new(0),
            owner: AtomicUsize::new(0),
            live: AtomicI32::new(0),
            remote: AtomicPtr::new(ptr::null_mut()),
            index: UnsafeCell::new(0),
            local: UnsafeCell::new(ptr::null_mut()),
            bump: UnsafeCell::new(0),
            pending: UnsafeCell::new(0),
        }
    }

    /// Where the slab's blocks are.
    #[inline]
    fn base(&self) -> *mut u8 {
        let at = self as *const Slab as usize;
        let region = at & !(REGION - 1);
        let index = (at - region) / std::mem::size_of::<Slab>();
        (region + index * SLAB) as *mut u8
    }
}

// The headers of a region's slabs fit in its first slab.
const _: () = assert!(SLABS * std::mem::size_of::<Slab>() <= SLAB);

/// The header of the slab `block` is in.
#[inline]
fn slab_of(block: *const u8) -> &'static Slab {
    let at = block as usize;
    let region = at & !(REGION - 1);
    let index = (at - region) / SLAB;
    // SAFETY: `block` is in a region, whose first bytes are its slabs'
    // headers, written when it was made and never given back.
    unsafe { &*(region as *const Slab).add(index) }
}

/// The class of a block of `size` bytes, if a slab holds it.
#[inline]
pub fn class_of(size: usize) -> Option<usize> {
    let size = size.max(1);
    Some(if size <= 512 {
        (size - 1) / 16
    } else if size <= 2048 {
        32 + (size - 513) / 32
    } else if size <= MAX_BLOCK {
        80 + (size - 2049) / 64
    } else {
        return None;
    })
}

/// Each class's block size, looked up: on every block given and taken.
const SIZES: [u16; CLASSES] = {
    let mut sizes = [0; CLASSES];
    let mut class = 0;
    while class < CLASSES {
        sizes[class] = size_of_class(class) as u16;
        class += 1;
    }
    sizes
};

/// The size of a block of class `class`.
#[inline]
pub const fn block_size(class: usize) -> usize {
    SIZES[class] as usize
}

const fn size_of_class(class: usize) -> usize {
    if class < 32 {
        (class + 1) * 16
    } else if class < 80 {
        512 + (class - 31) * 32
    } else {
        2048 + (class - 79) * 64
    }
}

/// Blocks in a slab of each class.
const CAPACITY: [i32; CLASSES] = {
    let mut capacity = [0; CLASSES];
    let mut class = 0;
    while class < CLASSES {
        capacity[class] = (SLAB / size_of_class(class)) as i32;
        class += 1;
    }
    capacity
};

#[inline]
fn capacity(class: usize) -> i32 {
    CAPACITY[class]
}

/// The most blocks a slab nobody owns may hold and be in its class's list:
/// a quarter of it free, or a sixty-fourth (at least one block) while memory
/// is near `maxmemory`.
///
/// Listed with the first block freed, a full slab went to the next thread
/// that needed one, gave it that block and was full again: under random
/// overwrites, where nearly every slab is full and every SET frees a block
/// of another, a SET of 100 bytes took the class's lock twice, and eight
/// threads on one class fell from 7.4 to 1.8 million SETs a second. Taken
/// with a quarter free, a slab gives its owner hundreds of blocks a lock.
///
/// But what a slab has free until then is memory the process holds and
/// cannot use. Where every write frees a block of another slab, the slabs
/// settle between three quarters full and full: at its `maxmemory` a server
/// evicts a key for every key it writes, and with 1 KB values and a limit of
/// 1 GiB it held 1,203 MiB (#202); 100-byte values wasted more still. So near
/// the limit (`memory_use::near_limit`) a slab is used again once a 64th of
/// it is free, and the slabs stay within about 1.5% of full. A slab then
/// gives its owner fewer blocks a lock (8 of 100 bytes, 1 of 1 KB), which
/// next to the eviction each of those writes makes is nothing.
#[inline]
fn listable(class: usize) -> i32 {
    let capacity = capacity(class);
    let free = if crate::memory_use::near_limit() {
        (capacity / 64).max(1)
    } else {
        capacity / 4
    };
    capacity - free
}

#[derive(Clone, Copy)]
struct SlabPtr(*const Slab);
// SAFETY: a header lives as long as the process.
unsafe impl Send for SlabPtr {}

/// Each class's slabs with room that nobody owns.
static PARTIAL_SLABS: [Mutex<Vec<SlabPtr>>; CLASSES] = [const { Mutex::new(Vec::new()) }; CLASSES];
/// Slabs that hold nothing.
static EMPTY_SLABS: Mutex<Vec<SlabPtr>> = Mutex::new(Vec::new());
/// Slabs that are not [`EMPTY`].
static IN_USE: AtomicUsize = AtomicUsize::new(0);
/// Regions taken from the system.
static REGIONS: AtomicUsize = AtomicUsize::new(0);
/// Where they are.
static REGION_LIST: Mutex<Vec<usize>> = Mutex::new(Vec::new());
/// The slabs compaction is emptying, each with its generation when chosen.
static EVACUATION: Mutex<Vec<(SlabPtr, u32)>> = Mutex::new(Vec::new());

/// A thread's slabs, one a class at most.
struct Mine {
    slabs: [Cell<*const Slab>; CLASSES],
}

impl Drop for Mine {
    fn drop(&mut self) {
        for (class, slab) in self.slabs.iter().enumerate() {
            let slab = slab.replace(ptr::null());
            if !slab.is_null() {
                // SAFETY: this thread owns it.
                unsafe { let_go(&*slab, class) };
            }
        }
    }
}

thread_local! {
    static MINE: Mine = const {
        Mine {
            slabs: [const { Cell::new(ptr::null()) }; CLASSES],
        }
    };
}

/// The owner of the slabs a thread with no [`Mine`] (one on its way out)
/// takes a block from, for as long as it takes it.
static NOBODY: u8 = 0;

/// A block of class `class`'s size, aligned to sixteen bytes.
pub fn alloc(class: usize) -> NonNull<u8> {
    debug_assert!(class < CLASSES);
    let block = MINE
        .try_with(|mine| {
            let owner = mine as *const Mine as usize;
            let cell = &mine.slabs[class];
            loop {
                let slab = cell.get();
                if !slab.is_null() {
                    // SAFETY: this thread owns it.
                    unsafe {
                        if let Some(block) = take(&*slab, class) {
                            return block;
                        }
                        let_go(&*slab, class);
                    }
                }
                cell.set(acquire(class, owner));
            }
        })
        .unwrap_or_else(|_| {
            let slab = acquire(class, &NOBODY as *const u8 as usize);
            // SAFETY: owned here until it is let go of, just after.
            unsafe {
                let block = take(&*slab, class).expect("a slab just taken has room");
                let_go(&*slab, class);
                block
            }
        });
    crate::memory_use::note(block_size(class) as isize);
    // SAFETY: blocks are in regions, never at address zero.
    unsafe { NonNull::new_unchecked(block) }
}

/// Give back a block [`alloc`] gave for `class`.
///
/// # Safety
/// `block` came from `alloc(class)`, and is not used again.
pub unsafe fn free(block: NonNull<u8>, class: usize) {
    crate::memory_use::note(-(block_size(class) as isize));
    let block = block.as_ptr();
    let slab = slab_of(block);
    debug_assert_eq!(slab.class.load(Ordering::Relaxed) as usize, class);
    debug_assert_ne!(slab.state.load(Ordering::Relaxed), EMPTY);
    let own = MINE
        .try_with(|mine| {
            // Only the owner ever writes its own address there.
            if slab.owner.load(Ordering::Relaxed) != mine as *const Mine as usize {
                return false;
            }
            // SAFETY: this thread owns the slab.
            unsafe {
                let local = &mut *slab.local.get();
                (block as *mut *mut u8).write(*local);
                *local = block;
                *slab.pending.get() -= 1;
            }
            true
        })
        .unwrap_or(false);
    if own {
        return;
    }
    // The slab cannot empty before this block is counted out, below.
    let generation = slab.generation.load(Ordering::Relaxed);
    let mut head = slab.remote.load(Ordering::Relaxed);
    loop {
        // SAFETY: the block is the caller's to give back, and at least the
        // size of a pointer.
        unsafe { (block as *mut *mut u8).write(head) };
        match slab
            .remote
            .compare_exchange_weak(head, block, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(now) => head = now,
        }
    }
    // Sequentially consistent, as `let_go`'s store of the state and
    // `settle`'s load of the count: this free counts itself out and then
    // reads the state, the owner letting go writes the state and then reads
    // the count. With weaker orders both could read the old value (seen
    // under Miri), and the last free of a slab would settle it nowhere.
    let now = slab.live.fetch_sub(1, Ordering::SeqCst) - 1;
    // A slab nobody owns that holds nothing goes back to the system, and a
    // full one with room enough now goes in its class's list. Not only the
    // free that brings it to [`listable`]: that moves with `maxmemory`, and a
    // slab already below it is listed by its next free. An owned one is
    // settled when its owner lets go of it.
    if now == 0 || now <= listable(class) {
        let state = slab.state.load(Ordering::SeqCst);
        if state != OWNED && (now == 0 || state == FULL) {
            let partial = PARTIAL_SLABS[class].lock();
            if slab.generation.load(Ordering::Relaxed) == generation {
                settle(partial, slab, class);
            }
        }
    }
}

/// A block of `slab`, if it has one.
///
/// # Safety
/// The calling thread owns `slab`, of class `class`.
#[inline]
unsafe fn take(slab: &Slab, class: usize) -> Option<*mut u8> {
    unsafe {
        let local = &mut *slab.local.get();
        if local.is_null() {
            *local = slab.remote.swap(ptr::null_mut(), Ordering::Acquire);
        }
        let block = if !local.is_null() {
            let block = *local;
            *local = (block as *const *mut u8).read();
            block
        } else {
            let bump = &mut *slab.bump.get();
            let size = block_size(class) as u32;
            if *bump as usize + size as usize > SLAB {
                return None;
            }
            let block = slab.base().add(*bump as usize);
            *bump += size;
            block
        };
        *slab.pending.get() += 1;
        Some(block)
    }
}

/// Stop owning `slab`: it goes to its class's list if it has room, back to
/// the system if it holds nothing.
///
/// # Safety
/// The calling thread owns `slab`, of class `class`.
unsafe fn let_go(slab: &Slab, class: usize) {
    let pending = std::mem::take(unsafe { &mut *slab.pending.get() });
    slab.live.fetch_add(pending, Ordering::AcqRel);
    let partial = PARTIAL_SLABS[class].lock();
    slab.owner.store(0, Ordering::Relaxed);
    // See `free`.
    slab.state.store(FULL, Ordering::SeqCst);
    settle(partial, slab, class);
}

/// Put `slab`, which nobody owns, where its fill says: in its class's list
/// if it has room, back to the system if it holds nothing.
fn settle(mut partial: parking_lot::MutexGuard<'_, Vec<SlabPtr>>, slab: &Slab, class: usize) {
    let state = slab.state.load(Ordering::Relaxed);
    if state != FULL && state != PARTIAL && state != EVACUATING {
        return;
    }
    let live = slab.live.load(Ordering::SeqCst);
    if live == 0 {
        if state == PARTIAL {
            unlist(&mut partial, slab);
        }
        slab.generation.fetch_add(1, Ordering::Relaxed);
        slab.state.store(EMPTY, Ordering::Relaxed);
        drop(partial);
        IN_USE.fetch_sub(1, Ordering::Relaxed);
        give_back(slab.base());
        EMPTY_SLABS.lock().push(SlabPtr(slab));
    } else if state == FULL && live <= listable(class) {
        // SAFETY: under the class's lock.
        unsafe { *slab.index.get() = partial.len() };
        partial.push(SlabPtr(slab));
        slab.state.store(PARTIAL, Ordering::Relaxed);
    }
}

/// Take `slab` out of its class's list, `partial`.
fn unlist(partial: &mut Vec<SlabPtr>, slab: &Slab) {
    // SAFETY: under the class's lock.
    let at = unsafe { *slab.index.get() };
    partial.swap_remove(at);
    if let Some(moved) = partial.get(at) {
        // SAFETY: under the class's lock.
        unsafe { *(*moved.0).index.get() = at };
    }
}

/// A slab of class `class` with room, owned by `owner`: one from the
/// class's list, or an empty one.
fn acquire(class: usize, owner: usize) -> *const Slab {
    if let Some(SlabPtr(slab)) = PARTIAL_SLABS[class].lock().pop() {
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*slab };
        s.owner.store(owner, Ordering::Relaxed);
        s.state.store(OWNED, Ordering::Relaxed);
        return slab;
    }
    let slab = empty_slab();
    // SAFETY: an empty slab is nobody's: nothing else reads or writes it.
    let s = unsafe { &*slab };
    unsafe {
        *s.local.get() = ptr::null_mut();
        *s.bump.get() = 0;
        *s.pending.get() = 0;
    }
    s.class.store(class as u8, Ordering::Relaxed);
    s.live.store(0, Ordering::Relaxed);
    s.remote.store(ptr::null_mut(), Ordering::Relaxed);
    s.owner.store(owner, Ordering::Relaxed);
    s.state.store(OWNED, Ordering::Release);
    IN_USE.fetch_add(1, Ordering::Relaxed);
    slab
}

/// A slab that holds nothing: one given back before, or one of a new region.
fn empty_slab() -> *const Slab {
    let mut empty = EMPTY_SLABS.lock();
    if let Some(SlabPtr(slab)) = empty.pop() {
        return slab;
    }
    let region = new_region();
    let headers = region as *mut Slab;
    for index in 0..SLABS {
        // SAFETY: the region's first slab is for the headers.
        unsafe { headers.add(index).write(Slab::new()) };
    }
    REGIONS.fetch_add(1, Ordering::Relaxed);
    REGION_LIST.lock().push(region);
    // Slab 0 is the headers'.
    for index in (2..SLABS).rev() {
        empty.push(SlabPtr(unsafe { headers.add(index) }));
    }
    unsafe { headers.add(1) }
}

#[cfg(all(unix, not(miri)))]
fn new_region() -> usize {
    // Twice the size, to find a stretch aligned to it, and the rest unmapped.
    // SAFETY: an anonymous private mapping of no file.
    unsafe {
        let at = libc::mmap(
            ptr::null_mut(),
            2 * REGION,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if at == libc::MAP_FAILED {
            std::alloc::handle_alloc_error(
                std::alloc::Layout::from_size_align(REGION, REGION).expect("a region's layout"),
            );
        }
        let start = at as usize;
        let aligned = (start + REGION - 1) & !(REGION - 1);
        if aligned > start {
            libc::munmap(at, aligned - start);
        }
        let end = aligned + REGION;
        let mapped_end = start + 2 * REGION;
        if mapped_end > end {
            libc::munmap(end as *mut libc::c_void, mapped_end - end);
        }
        aligned
    }
}

#[cfg(not(all(unix, not(miri))))]
fn new_region() -> usize {
    use std::alloc::GlobalAlloc;
    let layout = std::alloc::Layout::from_size_align(REGION, REGION).expect("a region's layout");
    // The system allocator's, not the counting one: blocks are counted as
    // they are handed out. Never freed.
    // SAFETY: the layout is not of size zero.
    let at = unsafe { std::alloc::System.alloc(layout) };
    if at.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    at as usize
}

/// Hand an empty slab's pages back to the kernel. They come back, zeroed,
/// when the slab is next written.
#[cfg(all(unix, not(miri)))]
fn give_back(base: *mut u8) {
    // SAFETY: the slab is mapped, and holds nothing anybody reads.
    unsafe { libc::madvise(base as *mut libc::c_void, SLAB, libc::MADV_DONTNEED) };
}

#[cfg(not(all(unix, not(miri))))]
fn give_back(_base: *mut u8) {}

/// What the slabs take, and what is in them.
pub struct Fill {
    /// Slabs that hold something (INFO `value_slab_bytes`).
    pub slab_bytes: usize,
    /// The blocks in them, counting a slab a thread owns as full: what it
    /// has handed out of it is not added in until it lets go of it (INFO
    /// `value_slab_live_bytes`).
    pub live_bytes: usize,
}

/// How full the slabs are, from every slab's header: a few thousand loads.
pub fn fill() -> Fill {
    let regions = REGION_LIST.lock().clone();
    let mut live_bytes = 0usize;
    for region in regions {
        for index in 1..SLABS {
            // SAFETY: a region's headers live as long as the process.
            let slab = unsafe { &*(region as *const Slab).add(index) };
            let class = slab.class.load(Ordering::Relaxed) as usize;
            live_bytes += match slab.state.load(Ordering::Relaxed) {
                EMPTY => 0,
                OWNED => SLAB,
                _ => slab.live.load(Ordering::Relaxed).max(0) as usize * block_size(class),
            };
        }
    }
    Fill {
        slab_bytes: slab_bytes(),
        live_bytes,
    }
}

/// Choose the sparsest slabs nobody owns, holding about `budget` bytes of
/// blocks between them, for compaction to empty: no block is handed out of
/// them from now on. Returns how many it chose.
pub fn plan_evacuation(budget: usize) -> usize {
    let mut candidates: Vec<(usize, usize, SlabPtr, usize)> = Vec::new();
    for (class, list) in PARTIAL_SLABS.iter().enumerate() {
        for &slab in list.lock().iter() {
            // SAFETY: a header lives as long as the process.
            let live = unsafe { &*slab.0 }.live.load(Ordering::Relaxed).max(0) as usize;
            let per_mille = live * 1000 / capacity(class) as usize;
            candidates.push((per_mille, class, slab, live * block_size(class)));
        }
    }
    candidates.sort_unstable_by_key(|candidate| candidate.0);
    let mut chosen = EVACUATION.lock();
    let mut held = 0usize;
    for (_, class, slab, bytes) in candidates {
        if held >= budget {
            break;
        }
        let mut list = PARTIAL_SLABS[class].lock();
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*slab.0 };
        // Taken by a thread since, or emptied and another class's now.
        if s.state.load(Ordering::Relaxed) != PARTIAL
            || s.class.load(Ordering::Relaxed) as usize != class
        {
            continue;
        }
        unlist(&mut list, s);
        s.state.store(EVACUATING, Ordering::Relaxed);
        chosen.push((slab, s.generation.load(Ordering::Relaxed)));
        held += bytes;
    }
    chosen.len()
}

/// Stop emptying the slabs [`plan_evacuation`] chose. Those that still hold
/// something (values that something besides their key held) go back to
/// their class's list, or stay out of it while too full.
pub fn finish_evacuation() {
    let chosen = std::mem::take(&mut *EVACUATION.lock());
    for (slab, generation) in chosen {
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*slab.0 };
        let class = s.class.load(Ordering::Relaxed) as usize;
        let list = PARTIAL_SLABS[class].lock();
        if s.generation.load(Ordering::Relaxed) != generation
            || s.state.load(Ordering::Relaxed) != EVACUATING
        {
            continue;
        }
        s.state.store(FULL, Ordering::Relaxed);
        settle(list, s, class);
    }
}

/// The regions taken so far, in order of address: for [`is_evacuating`].
pub fn regions_snapshot() -> Vec<usize> {
    let mut regions = REGION_LIST.lock().clone();
    regions.sort_unstable();
    regions
}

/// Whether `block` is in a slab compaction is emptying. `regions` is from
/// [`regions_snapshot`]: a block outside them is not in a slab, or is in one
/// made since, which is not being emptied.
#[inline]
pub fn is_evacuating(block: *const u8, regions: &[usize]) -> bool {
    let region = block as usize & !(REGION - 1);
    regions.binary_search(&region).is_ok()
        && slab_of(block).state.load(Ordering::Relaxed) == EVACUATING
}

/// Bytes of the slabs that hold something (INFO `value_slab_bytes`).
pub fn slab_bytes() -> usize {
    IN_USE.load(Ordering::Relaxed) * SLAB
}

/// Slabs that hold something.
// The library's, for tests; the server does not ask.
#[allow(dead_code)]
pub fn slabs_in_use() -> usize {
    IN_USE.load(Ordering::Relaxed)
}

/// Regions taken from the system so far.
// The library's, for tests; the server does not ask.
#[allow(dead_code)]
pub fn regions() -> usize {
    REGIONS.load(Ordering::Relaxed)
}

/// How many times the slab `block` is in has emptied. For tests.
#[doc(hidden)]
// The library's, for tests; the server does not ask.
#[allow(dead_code)]
pub fn generation_of(block: *const u8) -> u32 {
    slab_of(block).generation.load(Ordering::Relaxed)
}
