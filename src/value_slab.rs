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
//! Each of the other slabs holds blocks of one class (the keyspace's are
//! sixteen bytes apart in size, the entry pool's eight apart below 64, see
//! below). Masking a block's address finds its region, and its offset its
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
//!
//! **The lists.** Every slab nobody owns that holds something is in exactly
//! one of its class's two lists, under the class's lock: a [`PARTIAL`] one
//! in the list threads take slabs from, a [`FULL`] one in a list of its own,
//! linked through the slabs' headers (no allocation, and taken out in a few
//! stores). A slab moves between them, and out of them, only when its state
//! changes, under that lock, so a block taken or given back that changes no
//! state touches neither. The FULL list is what lets compaction choose a
//! full slab that has room (a slab that is three quarters full and more is
//! not used again for blocks, and its free blocks would otherwise stay so),
//! with the same check of its generation as a partial one
//! ([`plan_evacuation_with_full`]).
//!
//! **The entry pool.** A second set of classes, [`pool_class_of`]'s, for the
//! entries of large hashes and sets (`collection_table::EntryPool`), which
//! compaction cannot move: [`plan_evacuation`] never chooses their slabs,
//! and [`fill`] and [`slab_bytes`], which decide whether compaction runs,
//! leave them out (INFO shows them apart, as `value_slab_pool_bytes`).
//! Everything else is the same machinery: a block costs (and `used_memory`
//! counts) its class's size, any thread may free it, and a slab nobody owns
//! that empties gives its pages back at once. A slab a thread owns (the one
//! it takes blocks of that class from) is settled only when the thread lets
//! go of it: when it runs dry, or when the thread ends; until then its
//! blocks freed by that thread wait on its local list, so each thread can
//! hold up to one partly used slab per class it has used, the pool's
//! classes included. Its classes are 8 bytes apart up to 64 (an 11-byte
//! entry takes 16, a 20-byte one 24), then as the keyspace's, and its
//! blocks are aligned to 8.

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
/// The entry pool's classes ([`pool_class_of`]), numbered after the
/// keyspace's: 8 bytes apart to 64, 16 apart to 512, then the keyspace's
/// sizes (8 + 28 + 48 + 32).
const POOL_CLASSES: usize = 116;
/// Every class: the keyspace's, then the entry pool's.
const ALL_CLASSES: usize = CLASSES + POOL_CLASSES;
const _: () = assert!(block_size(ALL_CLASSES - 1) == MAX_BLOCK);
// A slab's class is kept in a byte.
const _: () = assert!(ALL_CLASSES <= 256);

// What a slab is doing.
/// A thread's, which takes blocks from it.
const OWNED: u8 = 0;
/// Nobody's, and too full to be listed ([`listable`]): in its class's list
/// of full slabs ([`Lists`]).
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
    /// Where the slab is in its class's list of partial slabs, while it is
    /// [`PARTIAL`]. Under the class's lock.
    index: UnsafeCell<u32>,
    /// Blocks other threads have given back, each pointing to the next.
    remote: AtomicPtr<u8>,
    // The owner's alone, and handed over with the slab.
    /// Blocks given back on the owner's thread, each pointing to the next.
    local: UnsafeCell<*mut u8>,
    /// Where the part of the slab never handed out starts.
    bump: UnsafeCell<u32>,
    /// Blocks the owner took, less those it gave back, not yet in `live`.
    pending: UnsafeCell<i32>,
    /// The slabs before and after it in its class's list of full slabs,
    /// while it is [`FULL`] (null at either end). Under the class's lock.
    prev: UnsafeCell<*const Slab>,
    next: UnsafeCell<*const Slab>,
}

// SAFETY: the shared fields are atomics; `index`, `prev` and `next` are only
// touched under the class's lock, and `local`, `bump` and `pending` only by
// the owner, or by whoever takes the slab from a list under that lock after
// the owner let go of it under the same lock.
unsafe impl Sync for Slab {}

// One line of cache a header: `fill` reads every one, and the owner's fields
// sit with the counts.
const _: () = assert!(std::mem::size_of::<Slab>() == 64);

impl Slab {
    const fn new() -> Self {
        Slab {
            state: AtomicU8::new(EMPTY),
            class: AtomicU8::new(0),
            generation: AtomicU32::new(0),
            owner: AtomicUsize::new(0),
            live: AtomicI32::new(0),
            index: UnsafeCell::new(0),
            remote: AtomicPtr::new(ptr::null_mut()),
            local: UnsafeCell::new(ptr::null_mut()),
            bump: UnsafeCell::new(0),
            pending: UnsafeCell::new(0),
            prev: UnsafeCell::new(ptr::null()),
            next: UnsafeCell::new(ptr::null()),
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

/// The entry pool's class of a block of `size` bytes, if a slab holds it:
/// classes compaction never empties (see the module docs).
#[inline]
pub fn pool_class_of(size: usize) -> Option<usize> {
    let size = size.max(1);
    Some(
        CLASSES
            + if size <= 64 {
                (size - 1) / 8
            } else if size <= 512 {
                8 + (size - 65) / 16
            } else {
                class_of(size)? + 4
            },
    )
}

/// Each class's block size, looked up: on every block given and taken.
const SIZES: [u16; ALL_CLASSES] = {
    let mut sizes = [0; ALL_CLASSES];
    let mut class = 0;
    while class < ALL_CLASSES {
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
    if class >= CLASSES {
        let pool = class - CLASSES;
        return if pool < 8 {
            (pool + 1) * 8
        } else if pool < 36 {
            64 + (pool - 7) * 16
        } else {
            // From 544 bytes, the keyspace's classes from its 32nd.
            size_of_class(pool - 4)
        };
    }
    if class < 32 {
        (class + 1) * 16
    } else if class < 80 {
        512 + (class - 31) * 32
    } else {
        2048 + (class - 79) * 64
    }
}

/// Blocks in a slab of each class.
const CAPACITY: [i32; ALL_CLASSES] = {
    let mut capacity = [0; ALL_CLASSES];
    let mut class = 0;
    while class < ALL_CLASSES {
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

/// A class's slabs that nobody owns and that hold something, but for those
/// being emptied: each in one of the two, as its state says. The lock around
/// it is the class's lock.
struct Lists {
    /// The [`PARTIAL`] slabs, each at its `index`: for threads to take.
    partial: Vec<SlabPtr>,
    /// The first [`FULL`] slab, the others after it through their `next`.
    full: SlabPtr,
    /// How many FULL slabs there are.
    full_len: usize,
}

impl Lists {
    const fn new() -> Self {
        Lists {
            partial: Vec::new(),
            full: SlabPtr(ptr::null()),
            full_len: 0,
        }
    }

    /// Put `slab`, now [`PARTIAL`], in the list threads take slabs from.
    fn list(&mut self, slab: &Slab) {
        // SAFETY: under the class's lock (`&mut self`).
        unsafe { *slab.index.get() = self.partial.len() as u32 };
        self.partial.push(SlabPtr(slab));
    }

    /// Take `slab`, which is [`PARTIAL`], out of the list threads take slabs
    /// from.
    fn unlist(&mut self, slab: &Slab) {
        // SAFETY: under the class's lock.
        let at = unsafe { *slab.index.get() } as usize;
        self.partial.swap_remove(at);
        if let Some(moved) = self.partial.get(at) {
            // SAFETY: under the class's lock.
            unsafe { *(*moved.0).index.get() = at as u32 };
        }
    }

    /// Put `slab`, now [`FULL`], at the front of the list of full slabs.
    fn link_full(&mut self, slab: &Slab) {
        self.insert_after(ptr::null(), slab);
        self.full_len += 1;
    }

    /// Take `slab`, which is [`FULL`], out of the list of full slabs.
    fn unlink_full(&mut self, slab: &Slab) {
        self.remove(slab);
        self.full_len -= 1;
    }

    /// Link `node` into the list of full slabs after `after`, or at its
    /// front if that is null.
    fn insert_after(&mut self, after: *const Slab, node: &Slab) {
        // SAFETY: under the class's lock (`&mut self`); `after` is in the
        // list, and `node` in no list.
        unsafe {
            let next = if after.is_null() {
                std::mem::replace(&mut self.full, SlabPtr(node)).0
            } else {
                std::mem::replace(&mut *(*after).next.get(), node)
            };
            *node.prev.get() = after;
            *node.next.get() = next;
            if !next.is_null() {
                *(*next).prev.get() = node;
            }
        }
    }

    /// Unlink `node` from the list of full slabs.
    fn remove(&mut self, node: &Slab) {
        // SAFETY: under the class's lock (`&mut self`); `node` is in the
        // list.
        unsafe {
            let prev = std::mem::replace(&mut *node.prev.get(), ptr::null());
            let next = std::mem::replace(&mut *node.next.get(), ptr::null());
            if prev.is_null() {
                self.full = SlabPtr(next);
            } else {
                *(*prev).next.get() = next;
            }
            if !next.is_null() {
                *(*next).prev.get() = prev;
            }
        }
    }

    /// Whether `node` is in the list of full slabs.
    fn has(&self, node: &Slab) -> bool {
        // SAFETY: under the class's lock.
        ptr::eq(self.full.0, node) || unsafe { !(*node.prev.get()).is_null() }
    }
}

/// Each class's lists of slabs nobody owns, under its lock: the class's
/// lock.
static LISTS: [Mutex<Lists>; ALL_CLASSES] = [const { Mutex::new(Lists::new()) }; ALL_CLASSES];

/// Where [`plan_evacuation_with_full`] is in each class's list of full
/// slabs: a header of no slab, linked into the list while it walks it, so
/// that it can let go of the class's lock every [`WALK`] slabs and go on
/// from there, whatever came and went meanwhile.
static MARKERS: [Slab; ALL_CLASSES] = [const { Slab::new() }; ALL_CLASSES];

/// Full slabs looked at under one hold of a class's lock.
const WALK: usize = 256;

/// One planning at a time: each class has one marker.
static PLANNING: Mutex<()> = Mutex::new(());
/// Slabs that hold nothing.
static EMPTY_SLABS: Mutex<Vec<SlabPtr>> = Mutex::new(Vec::new());
/// Slabs of the keyspace's classes that are not [`EMPTY`].
static IN_USE: AtomicUsize = AtomicUsize::new(0);
/// Slabs of the entry pool's classes that are not [`EMPTY`].
static POOL_IN_USE: AtomicUsize = AtomicUsize::new(0);

/// The count of slabs in use that a slab of `class` is in.
#[inline]
fn in_use(class: usize) -> &'static AtomicUsize {
    if class < CLASSES {
        &IN_USE
    } else {
        &POOL_IN_USE
    }
}
/// Regions taken from the system.
static REGIONS: AtomicUsize = AtomicUsize::new(0);
/// Where they are.
static REGION_LIST: Mutex<Vec<usize>> = Mutex::new(Vec::new());
/// The slabs compaction is emptying, each with its class and its generation
/// when chosen.
static EVACUATION: Mutex<Vec<(SlabPtr, usize, u32)>> = Mutex::new(Vec::new());

/// A thread's slabs, one a class at most.
struct Mine {
    slabs: [Cell<*const Slab>; ALL_CLASSES],
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
            slabs: [const { Cell::new(ptr::null()) }; ALL_CLASSES],
        }
    };
}

thread_local! {
    /// Slabs this thread took empty, for a class's blocks: what compaction
    /// moved into new slabs rather than into the room of partial ones
    /// ([`fresh_slabs_taken_here`]).
    static FRESH_HERE: Cell<u64> = const { Cell::new(0) };
}

/// How many slabs the calling thread has taken empty so far (not from a
/// class's list of partial slabs).
pub fn fresh_slabs_taken_here() -> u64 {
    FRESH_HERE.try_with(Cell::get).unwrap_or(0)
}

/// The owner of the slabs a thread with no [`Mine`] (one on its way out)
/// takes a block from, for as long as it takes it.
static NOBODY: u8 = 0;

/// A block of class `class`'s size, aligned to sixteen bytes for the
/// keyspace's classes and to eight for the entry pool's.
///
/// Must not panic (#358, #370): the active defrag thread lives on after a
/// panic in a pass, with its own slabs, and allocates again. A panic here
/// half way through a change of a slab's hands or lists would leave that
/// thread, or the lists, believing what is no longer so: two values given
/// the same block. Nothing in it panics but on a broken invariant, and a
/// failed allocation aborts.
pub fn alloc(class: usize) -> NonNull<u8> {
    debug_assert!(class < ALL_CLASSES);
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
///
/// Must not panic (#358, #370): the active defrag thread lives on after a
/// panic in a pass, with its own slabs, and allocates again. A panic here
/// half way through a change of a slab's hands or lists would leave that
/// thread, or the lists, believing what is no longer so: two values given
/// the same block. Nothing in it panics but on a broken invariant, and a
/// failed allocation aborts.
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
            let lists = LISTS[class].lock();
            if slab.generation.load(Ordering::Relaxed) == generation {
                settle(lists, slab, class);
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
///
/// Must not panic (#358, #370): the active defrag thread lives on after a
/// panic in a pass, with its own slabs, and allocates again. A panic here
/// half way through a change of a slab's hands or lists would leave that
/// thread, or the lists, believing what is no longer so: two values given
/// the same block. Nothing in it panics but on a broken invariant, and a
/// failed allocation aborts.
unsafe fn let_go(slab: &Slab, class: usize) {
    let pending = std::mem::take(unsafe { &mut *slab.pending.get() });
    slab.live.fetch_add(pending, Ordering::AcqRel);
    let mut lists = LISTS[class].lock();
    slab.owner.store(0, Ordering::Relaxed);
    lists.link_full(slab);
    // See `free`.
    slab.state.store(FULL, Ordering::SeqCst);
    settle(lists, slab, class);
}

/// Put `slab`, which nobody owns, where its fill says: in its class's list
/// of partial slabs if it has room, back to the system if it holds nothing.
/// A [`FULL`] slab is in the list of full slabs, and leaves it for either.
///
/// Must not panic (#358, #370): the active defrag thread lives on after a
/// panic in a pass, with its own slabs, and allocates again. A panic here
/// half way through a change of a slab's hands or lists would leave that
/// thread, or the lists, believing what is no longer so: two values given
/// the same block. Nothing in it panics but on a broken invariant, and a
/// failed allocation aborts.
fn settle(mut lists: parking_lot::MutexGuard<'_, Lists>, slab: &Slab, class: usize) {
    let state = slab.state.load(Ordering::Relaxed);
    if state != FULL && state != PARTIAL && state != EVACUATING {
        return;
    }
    let live = slab.live.load(Ordering::SeqCst);
    if live == 0 {
        match state {
            PARTIAL => lists.unlist(slab),
            FULL => lists.unlink_full(slab),
            _ => {}
        }
        slab.generation.fetch_add(1, Ordering::Relaxed);
        slab.state.store(EMPTY, Ordering::Relaxed);
        drop(lists);
        in_use(class).fetch_sub(1, Ordering::Relaxed);
        give_back(slab.base());
        EMPTY_SLABS.lock().push(SlabPtr(slab));
    } else if state == FULL && live <= listable(class) {
        lists.unlink_full(slab);
        lists.list(slab);
        slab.state.store(PARTIAL, Ordering::Relaxed);
    }
}

/// `slab`, being emptied and nobody's, made [`FULL`] again and put where its
/// fill says ([`settle`]), under its class's lock, `lists`.
fn put_back(mut lists: parking_lot::MutexGuard<'_, Lists>, slab: &Slab, class: usize) {
    lists.link_full(slab);
    slab.state.store(FULL, Ordering::Relaxed);
    settle(lists, slab, class);
}

/// A slab of class `class` with room, owned by `owner`: one from the
/// class's list, or an empty one.
///
/// Must not panic (#358, #370): the active defrag thread lives on after a
/// panic in a pass, with its own slabs, and allocates again. A panic here
/// half way through a change of a slab's hands or lists would leave that
/// thread, or the lists, believing what is no longer so: two values given
/// the same block. Nothing in it panics but on a broken invariant, and a
/// failed allocation aborts.
fn acquire(class: usize, owner: usize) -> *const Slab {
    if let Some(SlabPtr(slab)) = LISTS[class].lock().partial.pop() {
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*slab };
        s.owner.store(owner, Ordering::Relaxed);
        s.state.store(OWNED, Ordering::Relaxed);
        return slab;
    }
    let slab = empty_slab();
    // No destructor: there to read on a thread on its way out too.
    let _ = FRESH_HERE.try_with(|taken| taken.set(taken.get() + 1));
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
    in_use(class).fetch_add(1, Ordering::Relaxed);
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

/// What the keyspace's slabs take, and what is in them: the entry pool's are
/// left out, as compaction does not empty them.
pub struct Fill {
    /// Slabs that hold something (INFO `value_slab_bytes`).
    pub slab_bytes: usize,
    /// The blocks in them, counting a slab a thread owns as full: what it
    /// has handed out of it is not added in until it lets go of it (INFO
    /// `value_slab_live_bytes`).
    pub live_bytes: usize,
    /// What the [`FULL`] slabs have free: the slabs too full to be used for
    /// blocks again, whose room only compaction gets back (INFO
    /// `value_slab_full_free_bytes`). Free blocks only: the end of a slab no
    /// block fits in (3,520 bytes of a slab of 3,648-byte blocks) is no room.
    pub full_free_bytes: usize,
    /// The free blocks of the slabs nobody owns, [`FULL`] and [`PARTIAL`]:
    /// the room the slab-waste trigger looks at.
    pub free_bytes: usize,
    /// The bytes of the blocks handed out of the slabs, but for those of the
    /// slabs the calling thread owns (counted as `SLAB` each in
    /// `live_bytes`): what only other threads' allocations and frees move.
    /// Compaction's own slabs, which it fills with what it moves, then do
    /// not look like the clients' writes.
    pub others_bytes: usize,
}

/// How full the slabs are, from every slab's header: a few thousand loads.
pub fn fill() -> Fill {
    let regions = REGION_LIST.lock().clone();
    let mine = MINE.try_with(|m| m as *const Mine as usize).unwrap_or(0);
    let (mut live_bytes, mut full_free_bytes) = (0usize, 0usize);
    let (mut free_bytes, mut mine_bytes) = (0usize, 0usize);
    for region in regions {
        for index in 1..SLABS {
            // SAFETY: a region's headers live as long as the process.
            let slab = unsafe { &*(region as *const Slab).add(index) };
            // The state first: a slab taken empty gets its class before its
            // state says it is in use (`acquire`'s release store).
            let state = slab.state.load(Ordering::Acquire);
            let class = slab.class.load(Ordering::Relaxed) as usize;
            if state == EMPTY || class >= CLASSES {
                // Nothing, or the entry pool's.
                continue;
            }
            if state == OWNED {
                live_bytes += SLAB;
                if mine != 0 && slab.owner.load(Ordering::Relaxed) == mine {
                    mine_bytes += SLAB;
                }
                continue;
            }
            let live = slab.live.load(Ordering::Relaxed).max(0) as usize * block_size(class);
            live_bytes += live;
            let free = (capacity(class) as usize * block_size(class)).saturating_sub(live);
            if state == FULL {
                full_free_bytes += free;
            }
            if state == FULL || state == PARTIAL {
                free_bytes += free;
            }
        }
    }
    Fill {
        slab_bytes: slab_bytes(),
        live_bytes,
        full_free_bytes,
        free_bytes,
        others_bytes: live_bytes - mine_bytes,
    }
}

/// For tests: called by [`plan_evacuation`] between its two passes, with no
/// lock held, so that a test can change the slabs it measured.
#[doc(hidden)]
pub static PAUSE_BETWEEN_PASSES: Mutex<Option<fn()>> = Mutex::new(None);

/// Choose the sparsest slabs nobody owns from the classes' lists of partial
/// slabs, for compaction to empty: no block is handed out of them from now
/// on. `budget` is about how many bytes of their blocks may go into new
/// slabs (see [`plan_evacuation_with_full`]). Returns how many it chose. Only
/// the keyspace's classes: never a slab of the entry pool, whose blocks
/// compaction cannot move.
// The library's, for tests; the server asks for full slabs too.
#[allow(dead_code)]
pub fn plan_evacuation(budget: usize) -> usize {
    plan(budget, 0, 0..CLASSES)
}

/// [`plan_evacuation`], and the full slabs with at least
/// `full_free_per_mille` thousandths of their blocks free (none if 0) as
/// well, the sparsest of all first.
///
/// What is moved out of the slabs chosen goes first into the room of the
/// class's partial slabs that are not chosen (a thread, compaction's too,
/// takes a slab from its class's list before an empty one), then into new
/// slabs. Only those bytes count toward `budget`: they are what the process
/// holds twice, in the new slabs and in the ones being emptied, until the
/// pass is over. The room is counted from the slabs' headers when chosen, so
/// that writes taking room meanwhile can make it more than the budget.
pub fn plan_evacuation_with_full(budget: usize, full_free_per_mille: u32) -> usize {
    plan(budget, full_free_per_mille, 0..CLASSES)
}

/// A slab [`plan`] measured.
struct Candidate {
    /// Thousandths of its blocks in use.
    per_mille: usize,
    class: usize,
    slab: SlabPtr,
    /// Its generation and state when measured, under the class's lock.
    generation: u32,
    state: u8,
    /// Bytes of its blocks in use, and free.
    live_bytes: usize,
    free_bytes: usize,
}

fn plan(budget: usize, full_free_per_mille: u32, classes: std::ops::Range<usize>) -> usize {
    let _one = PLANNING.lock();
    let mut candidates: Vec<Candidate> = Vec::new();
    // The free bytes of each class's partial slabs: where what is moved out
    // goes before any new slab.
    let mut room = [0usize; ALL_CLASSES];
    for class in classes {
        let (blocks, size) = (capacity(class) as usize, block_size(class));
        let measured = |slab: SlabPtr, state: u8| {
            // SAFETY: a header lives as long as the process.
            let s = unsafe { &*slab.0 };
            let live = (s.live.load(Ordering::Relaxed).max(0) as usize).min(blocks);
            Candidate {
                per_mille: live * 1000 / blocks,
                class,
                slab,
                // Under the class's lock, as every change of a listed slab's
                // generation is.
                generation: s.generation.load(Ordering::Relaxed),
                state,
                live_bytes: live * size,
                free_bytes: (blocks - live) * size,
            }
        };
        for &slab in LISTS[class].lock().partial.iter() {
            let candidate = measured(slab, PARTIAL);
            room[class] += candidate.free_bytes;
            candidates.push(candidate);
        }
        if (1..1000).contains(&full_free_per_mille) {
            let most = 1000 - (full_free_per_mille.min(1000) as usize);
            walk_full(class, |slab| {
                let candidate = measured(slab, FULL);
                if candidate.per_mille <= most {
                    candidates.push(candidate);
                }
            });
        }
    }
    candidates.sort_unstable_by_key(|candidate| candidate.per_mille);
    let pause = *PAUSE_BETWEEN_PASSES.lock();
    if let Some(pause) = pause {
        pause();
    }
    let mut chosen = EVACUATION.lock();
    // Room for every slab it may choose, taken before any is: between a
    // slab's being taken out of its list and its being on `chosen`, nothing
    // can panic, so that no slab is left being emptied that
    // `finish_evacuation` does not see (#358).
    chosen.reserve(candidates.len());
    let mut fresh = 0usize;
    for candidate in candidates {
        if fresh >= budget {
            break;
        }
        let Candidate {
            class,
            slab,
            generation,
            state,
            ..
        } = candidate;
        let mut lists = LISTS[class].lock();
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*slab.0 };
        // Still the slab measured, and still in the list it was measured in.
        // Its generation is the same only if it has not emptied since: it
        // empties under this lock, so a later generation is seen here. And
        // until it empties it is this class's, and is listed, taken, let go
        // of, made full or partial only under this lock, so PARTIAL means it
        // is in the list of partial slabs at its `index`, FULL that it is in
        // the list of full slabs. A slab that emptied and was listed again,
        // in this class or another, is not the one measured: its state and
        // class alone do not tell, and its class, written with no lock by the
        // thread that took it empty, may be seen late.
        if s.generation.load(Ordering::Relaxed) != generation
            || s.state.load(Ordering::Relaxed) != state
        {
            continue;
        }
        if state == PARTIAL {
            lists.unlist(s);
        } else {
            lists.unlink_full(s);
        }
        s.state.store(EVACUATING, Ordering::Relaxed);
        chosen.push((slab, class, generation));
        drop(lists);
        // A partial slab chosen is room no more; what it holds, as what a
        // full one holds, goes into the room left, and the rest into new
        // slabs.
        if state == PARTIAL {
            room[class] = room[class].saturating_sub(candidate.free_bytes);
        }
        let into_room = candidate.live_bytes.min(room[class]);
        room[class] -= into_room;
        fresh += candidate.live_bytes - into_room;
    }
    chosen.len()
}

/// Call `each` for every slab in `class`'s list of full slabs, under the
/// class's lock but letting go of it every [`WALK`] slabs: a class can have
/// hundreds of thousands, and a thread that lets go of a slab or a free that
/// lists one waits for the lock meanwhile. The class's marker holds the
/// place. A slab that comes into the list meanwhile is put at its front, and
/// may be missed; one that leaves it is not seen after it has. The caller
/// holds [`PLANNING`]: there is one marker a class.
///
/// `each` must not panic, as nothing in it does but on a broken invariant
/// or a failed allocation (which aborts): the marker would be left linked.
/// Should one be, it is unlinked at the start of the next walk.
fn walk_full(class: usize, mut each: impl FnMut(SlabPtr)) {
    let marker = &MARKERS[class];
    {
        let mut lists = LISTS[class].lock();
        if lists.has(marker) {
            lists.remove(marker);
        }
        lists.insert_after(ptr::null(), marker);
    }
    loop {
        let mut lists = LISTS[class].lock();
        for _ in 0..WALK {
            // SAFETY: under the class's lock.
            let next = unsafe { *marker.next.get() };
            lists.remove(marker);
            if next.is_null() {
                return;
            }
            lists.insert_after(next, marker);
            each(SlabPtr(next));
        }
        // Handed to a thread waiting for the class (one letting go of a slab,
        // or taking one), where a plain unlock let this one take it back at
        // once, hold after hold.
        parking_lot::MutexGuard::unlock_fair(lists);
    }
}

/// Stop emptying the slabs [`plan_evacuation`] chose. Those that still hold
/// something (values that something besides their key held) go back to
/// their class's list, or stay out of it while too full.
///
/// Also what puts them back after a pass of compaction that panicked
/// (#358), at whatever point it did. Each is taken off the list as it is
/// put back, so that should this panic too, the rest are put back by the
/// next call.
///
/// Returns how many of them emptied.
pub fn finish_evacuation() -> usize {
    let mut emptied = 0;
    loop {
        let next = EVACUATION.lock().pop();
        let Some((slab, class, generation)) = next else {
            return emptied;
        };
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*slab.0 };
        // The class it had when chosen, not the one it has now: one that
        // emptied did so under this class's lock, so its generation shows
        // it here. Its class now may be another's, seen with no lock.
        let lists = LISTS[class].lock();
        if s.generation.load(Ordering::Relaxed) != generation {
            emptied += 1;
            continue;
        }
        if s.state.load(Ordering::Relaxed) != EVACUATING {
            continue;
        }
        put_back(lists, s, class);
        // For tests: putting back that panics with this slab put back and
        // the rest still on the list.
        fault_in_finish();
    }
}

/// For tests: how many of the next calls of [`finish_evacuation`] panic,
/// each once it has put one slab back.
#[cfg(test)]
pub(crate) static FAULTS_IN_FINISH: AtomicU32 = AtomicU32::new(0);

#[cfg(test)]
fn fault_in_finish() {
    if FAULTS_IN_FINISH.load(Ordering::Relaxed) > 0 {
        FAULTS_IN_FINISH.fetch_sub(1, Ordering::Relaxed);
        panic!("injected fault putting back the slabs being emptied");
    }
}

#[cfg(not(test))]
#[inline(always)]
fn fault_in_finish() {}

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

/// Bytes of the keyspace's slabs that hold something (INFO
/// `value_slab_bytes`).
pub fn slab_bytes() -> usize {
    IN_USE.load(Ordering::Relaxed) * SLAB
}

/// Bytes of the entry pool's slabs that hold something (INFO
/// `value_slab_pool_bytes`).
pub fn pool_slab_bytes() -> usize {
    POOL_IN_USE.load(Ordering::Relaxed) * SLAB
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

/// For tests: held by a test that empties slabs, or makes them be emptied:
/// the slabs are the process's, and one such test would take another's.
#[cfg(test)]
pub(crate) static EVACUATION_TESTS: Mutex<()> = Mutex::new(());

/// For tests: how many slabs of `class` hold something (are not
/// [`EMPTY`]).
#[cfg(test)]
pub(crate) fn slabs_of_class(class: usize) -> usize {
    let regions = REGION_LIST.lock().clone();
    regions
        .iter()
        .map(|&region| {
            (1..SLABS)
                .filter(|&index| {
                    // SAFETY: a region's headers live as long as the process.
                    let slab = unsafe { &*(region as *const Slab).add(index) };
                    slab.state.load(Ordering::Acquire) != EMPTY
                        && slab.class.load(Ordering::Relaxed) as usize == class
                })
                .count()
        })
        .sum()
}

/// For tests: how many slabs are being emptied.
#[cfg(test)]
pub(crate) fn evacuating_slabs() -> usize {
    let regions = REGION_LIST.lock().clone();
    regions
        .iter()
        .map(|&region| {
            (1..SLABS)
                .filter(|&index| {
                    // SAFETY: a region's headers live as long as the process.
                    let slab = unsafe { &*(region as *const Slab).add(index) };
                    slab.state.load(Ordering::Relaxed) == EVACUATING
                })
                .count()
        })
        .sum()
}

/// For tests: empty only the slab `block` is in, as [`plan_evacuation`]
/// would, until the guard is dropped, panic or not. No other slab changes,
/// and [`finish_evacuation`] does not see it. `None` unless nobody owns the
/// slab and it has room.
///
/// `block` is live, so its slab's class is the one it was given in.
#[cfg(test)]
pub(crate) fn evacuate_only(block: *const u8) -> Option<OnlyEvacuating> {
    let slab = slab_of(block);
    let class = slab.class.load(Ordering::Relaxed) as usize;
    let mut lists = LISTS[class].lock();
    if slab.state.load(Ordering::Relaxed) != PARTIAL {
        return None;
    }
    lists.unlist(slab);
    slab.state.store(EVACUATING, Ordering::Relaxed);
    Some(OnlyEvacuating {
        slab: SlabPtr(slab),
        class,
        generation: slab.generation.load(Ordering::Relaxed),
    })
}

/// The slab [`evacuate_only`] chose; dropped, it is put back as
/// [`finish_evacuation`] puts back its own.
#[cfg(test)]
pub(crate) struct OnlyEvacuating {
    slab: SlabPtr,
    class: usize,
    generation: u32,
}

#[cfg(test)]
impl OnlyEvacuating {
    /// The blocks the slab holds: nobody owns it, so none is uncounted.
    pub(crate) fn live(&self) -> usize {
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*self.slab.0 };
        s.live.load(Ordering::SeqCst).max(0) as usize
    }
}

#[cfg(test)]
impl Drop for OnlyEvacuating {
    fn drop(&mut self) {
        // SAFETY: a header lives as long as the process.
        let s = unsafe { &*self.slab.0 };
        let lists = LISTS[self.class].lock();
        if s.generation.load(Ordering::Relaxed) != self.generation
            || s.state.load(Ordering::Relaxed) != EVACUATING
        {
            return;
        }
        put_back(lists, s, self.class);
    }
}

/// Check what every class's lists hold against the slabs' headers, each
/// class under its lock in turn: each slab in a list is of that class and in
/// the state the list is for, no slab is in a list twice, the links run both
/// ways and the counts add up. With `quiescent` (nothing allocates or frees
/// meanwhile), also from the headers' side: every [`PARTIAL`] or [`FULL`]
/// slab is in its class's list, and every slab is counted in use or empty.
/// The first thing wrong, if any. For tests: it takes every class's lock.
#[doc(hidden)]
// The library's, for tests; the server does not ask.
#[allow(dead_code)]
pub fn check_lists(quiescent: bool) -> Result<(), String> {
    let regions = REGION_LIST.lock().clone();
    let headers = || {
        regions.iter().flat_map(|&region| {
            // SAFETY: a region's headers live as long as the process.
            (1..SLABS).map(move |index| unsafe { &*(region as *const Slab).add(index) })
        })
    };
    // Every slab listed, while `quiescent`; otherwise a class's alone, as a
    // slab may empty and be listed in another class meanwhile.
    let mut listed = std::collections::HashSet::new();
    for class in 0..ALL_CLASSES {
        if !quiescent {
            listed.clear();
        }
        let lists = LISTS[class].lock();
        for (at, slab) in lists.partial.iter().enumerate() {
            // SAFETY: a header lives as long as the process.
            let s = unsafe { &*slab.0 };
            let what = || format!("class {class}: partial slab {at} ({:p})", slab.0);
            if s.state.load(Ordering::Relaxed) != PARTIAL {
                return Err(format!(
                    "{} is in state {}",
                    what(),
                    s.state.load(Ordering::Relaxed)
                ));
            }
            if s.class.load(Ordering::Relaxed) as usize != class {
                return Err(format!(
                    "{} is of class {}",
                    what(),
                    s.class.load(Ordering::Relaxed)
                ));
            }
            // SAFETY: under the class's lock.
            if unsafe { *s.index.get() } as usize != at {
                return Err(format!("{} has index {}", what(), unsafe {
                    *s.index.get()
                }));
            }
            if lists.has(s) {
                return Err(format!("{} is in the list of full slabs too", what()));
            }
            if !listed.insert(slab.0 as usize) {
                return Err(format!("{} is listed twice", what()));
            }
        }
        let (mut at, mut prev, mut count) = (lists.full.0, ptr::null::<Slab>(), 0usize);
        while !at.is_null() {
            // SAFETY: a header lives as long as the process; its links are
            // read under the class's lock.
            let s = unsafe { &*at };
            let what = || format!("class {class}: full slab {count} ({at:p})");
            if unsafe { *s.prev.get() } != prev {
                return Err(format!("{} links back to another", what()));
            }
            if ptr::eq(s, &MARKERS[class]) {
                return Err(format!("{} is the marker, left in the list", what()));
            }
            if s.state.load(Ordering::Relaxed) != FULL {
                return Err(format!(
                    "{} is in state {}",
                    what(),
                    s.state.load(Ordering::Relaxed)
                ));
            }
            if s.class.load(Ordering::Relaxed) as usize != class {
                return Err(format!(
                    "{} is of class {}",
                    what(),
                    s.class.load(Ordering::Relaxed)
                ));
            }
            if !listed.insert(at as usize) {
                return Err(format!("{} is listed twice", what()));
            }
            count += 1;
            if count > lists.full_len {
                return Err(format!(
                    "class {class}: more full slabs than {}",
                    lists.full_len
                ));
            }
            prev = at;
            at = unsafe { *s.next.get() };
        }
        if count != lists.full_len {
            return Err(format!(
                "class {class}: {count} full slabs, counted {}",
                lists.full_len
            ));
        }
    }
    if quiescent {
        let (mut in_use, mut pool_in_use, mut empty) = (0, 0, 0);
        for s in headers() {
            let state = s.state.load(Ordering::Acquire);
            let class = s.class.load(Ordering::Relaxed) as usize;
            match state {
                EMPTY => empty += 1,
                _ if class < CLASSES => in_use += 1,
                _ => pool_in_use += 1,
            }
            if (state == PARTIAL || state == FULL) && !listed.contains(&(s as *const Slab as usize))
            {
                return Err(format!(
                    "a slab of class {class} in state {state} is in no list"
                ));
            }
            if (state != PARTIAL && state != FULL) && listed.contains(&(s as *const Slab as usize))
            {
                return Err(format!(
                    "a slab of class {class} in state {state} is listed"
                ));
            }
        }
        if in_use != IN_USE.load(Ordering::Relaxed)
            || pool_in_use != POOL_IN_USE.load(Ordering::Relaxed)
        {
            return Err(format!(
                "{in_use} slabs in use ({} counted), {pool_in_use} of the pool ({} counted)",
                IN_USE.load(Ordering::Relaxed),
                POOL_IN_USE.load(Ordering::Relaxed)
            ));
        }
        if empty != EMPTY_SLABS.lock().len() {
            return Err(format!(
                "{empty} empty slabs, {} in the list",
                EMPTY_SLABS.lock().len()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_classes_are_as_they_were() {
        for size in 1..=MAX_BLOCK {
            let class = class_of(size).unwrap();
            assert!(class < CLASSES);
            let step = if size <= 512 {
                16
            } else if size <= 2048 {
                32
            } else {
                64
            };
            assert_eq!(block_size(class), size.div_ceil(step) * step, "{size}");
        }
        assert_eq!(class_of(MAX_BLOCK + 1), None);
    }

    #[test]
    fn pool_classes_fit_tightly() {
        for size in 1..=MAX_BLOCK {
            let class = pool_class_of(size).unwrap();
            assert!((CLASSES..ALL_CLASSES).contains(&class), "{size}");
            let block = block_size(class);
            assert!(block >= size, "{size}");
            // The class below is too small: no size skips a class.
            if class > CLASSES {
                assert!(block_size(class - 1) < size.max(1), "{size}");
            }
            let step = if size <= 64 {
                8
            } else if size <= 512 {
                16
            } else if size <= 2048 {
                32
            } else {
                64
            };
            assert_eq!(block, size.div_ceil(step) * step, "{size}");
            // Room for the free list's pointer.
            assert!(block >= std::mem::size_of::<*mut u8>());
        }
        assert_eq!(pool_class_of(MAX_BLOCK + 1), None);
        assert_eq!(block_size(pool_class_of(11).unwrap()), 16);
        assert_eq!(block_size(pool_class_of(20).unwrap()), 24);
    }

    /// Compaction never chooses a slab of the entry pool, and a pool slab
    /// that empties is given back at once, its blocks freed on another
    /// thread.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn pool_slabs_are_never_evacuated_and_empty_at_once() {
        let _one = EVACUATION_TESTS.lock();
        // A class no other test allocates (blocks of 4,033-4,096 bytes, 16
        // a slab): `acquire` takes a listed slab of the class before an
        // empty one, so a test that allocated in the same class meanwhile
        // could take the very slabs this one asserts on (the small classes
        // are the tables' and `table_scan`'s, from many threads at once).
        let class = pool_class_of(MAX_BLOCK).unwrap();
        let per_slab = capacity(class) as usize;
        let slabs = if cfg!(miri) { 2 } else { 3 };
        // Whole slabs and one block more: the slabs before the last are let
        // go of (nobody's), the last is this thread's.
        let blocks: Vec<usize> = (0..slabs * per_slab + 1)
            .map(|_| alloc(class).as_ptr() as usize)
            .collect();
        let (full, rest) = blocks.split_at(slabs * per_slab);
        // Fresh slabs, one after the other.
        for slab in full.chunks(per_slab) {
            let first = slab_of(slab[0] as *const u8);
            assert!(std::ptr::eq(
                first,
                slab_of(slab[per_slab - 1] as *const u8)
            ));
        }
        let generations: Vec<u32> = full
            .chunks(per_slab)
            .map(|slab| generation_of(slab[0] as *const u8))
            .collect();
        // Most of each slab freed on another thread: the slabs get room,
        // and are listed for the taking.
        let freed: Vec<usize> = (0..full.len())
            .filter(|i| !i.is_multiple_of(8))
            .map(|i| full[i])
            .collect();
        let kept: Vec<usize> = (0..full.len())
            .filter(|i| i.is_multiple_of(8))
            .map(|i| full[i])
            .collect();
        std::thread::spawn(move || {
            for b in freed {
                // SAFETY: allocated above, freed once.
                unsafe { free(NonNull::new(b as *mut u8).unwrap(), class) };
            }
        })
        .join()
        .unwrap();
        for slab in full.chunks(per_slab) {
            let s = slab_of(slab[0] as *const u8);
            assert_eq!(s.state.load(Ordering::Relaxed), PARTIAL);
        }
        // Compaction with all the budget there is: none of them.
        plan_evacuation(usize::MAX);
        let regions = regions_snapshot();
        for &b in &kept {
            assert!(!is_evacuating(b as *const u8, &regions));
        }
        finish_evacuation();
        // The rest freed: each slab empties, and is given back at once.
        for &b in kept.iter().chain(rest) {
            // SAFETY: allocated above, freed once.
            unsafe { free(NonNull::new(b as *mut u8).unwrap(), class) };
        }
        for (slab, generation) in full.chunks(per_slab).zip(generations) {
            assert_ne!(generation_of(slab[0] as *const u8), generation, "emptied");
        }
    }

    /// The class the model tests use: one no other test allocates.
    fn model_class() -> usize {
        pool_class_of(3904).unwrap()
    }

    /// A small, reproducible sequence.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, below: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.0 >> 33) as usize) % below
        }
    }

    /// The slabs `blocks` are in, and how many of them each holds.
    fn per_slab(blocks: &[usize]) -> std::collections::BTreeMap<usize, usize> {
        let mut slabs = std::collections::BTreeMap::new();
        for &b in blocks {
            *slabs.entry(b & !(SLAB - 1)).or_insert(0) += 1;
        }
        slabs
    }

    /// Every slab holding one of `blocks` (all of `class`, which nothing
    /// else uses, and nobody owns), against the model: in the state its
    /// count says (or being emptied, if `evacuating` has it), in the list
    /// that state says, and nothing else in the class's lists; and the lists
    /// check out ([`check_lists`]).
    fn check_model(class: usize, blocks: &[usize], evacuating: &[usize]) {
        check_lists(false).unwrap();
        let slabs = per_slab(blocks);
        let lists = LISTS[class].lock();
        let mut listed = 0;
        for (&base, &n) in &slabs {
            let s = slab_of(base as *const u8);
            let state = s.state.load(Ordering::Relaxed);
            assert_eq!(s.live.load(Ordering::Relaxed) as usize, n, "slab {base:x}");
            if evacuating.contains(&base) {
                assert_eq!(state, EVACUATING, "slab {base:x}, {n} blocks");
                assert!(!lists.has(s));
                continue;
            }
            listed += 1;
            if n as i32 > listable(class) {
                assert_eq!(state, FULL, "slab {base:x}, {n} blocks");
                assert!(lists.has(s), "a full slab not in the list of full slabs");
            } else {
                assert_eq!(state, PARTIAL, "slab {base:x}, {n} blocks");
                // SAFETY: under the class's lock.
                let at = unsafe { *s.index.get() } as usize;
                assert!(ptr::eq(lists.partial[at].0, s));
            }
        }
        assert_eq!(lists.partial.len() + lists.full_len, listed, "slabs listed");
    }

    /// The lists through every change of state (#240), against a model:
    /// blocks made on threads that end (so that their slabs are let go of,
    /// full or partly used), freed on this thread (which owns none of the
    /// class's slabs), whole slabs emptied, and evacuations planned with
    /// either threshold, blocks freed while they run, and finished. After
    /// every step each slab is in the state and the list its count says.
    #[test]
    fn the_lists_follow_every_slab_through_its_states() {
        let _one = EVACUATION_TESTS.lock();
        // A class no other test allocates (the pool's of 3,841-3,904 bytes,
        // 16 a slab; the tables' tests and the other pool tests have the
        // three above it), which `plan` takes like any when asked.
        let class = model_class();
        assert!(check_lists(false).is_ok());
        let blocks_a_slab = capacity(class) as usize;
        let mut rng = Lcg(240);
        let mut blocks: Vec<usize> = Vec::new();
        let steps = if cfg!(miri) { 40 } else { 600 };
        let free_one = |b: usize| {
            // SAFETY: allocated in the model, freed once.
            unsafe { free(NonNull::new(b as *mut u8).unwrap(), class) }
        };
        for _ in 0..steps {
            match rng.next(5) {
                0 | 1 => {
                    let n = 1 + rng.next(3 * blocks_a_slab);
                    let made = std::thread::spawn(move || {
                        (0..n)
                            .map(|_| alloc(class).as_ptr() as usize)
                            .collect::<Vec<_>>()
                    })
                    .join()
                    .unwrap();
                    blocks.extend(made);
                }
                2 => {
                    // A few at random.
                    for _ in 0..rng.next(blocks.len() / 4 + 1) {
                        let b = blocks.swap_remove(rng.next(blocks.len()));
                        free_one(b);
                    }
                }
                3 => {
                    // Every block of one slab: it empties.
                    let Some((&base, _)) = per_slab(&blocks).iter().nth(rng.next(4)) else {
                        continue;
                    };
                    let (gone, kept): (Vec<usize>, Vec<usize>) = std::mem::take(&mut blocks)
                        .into_iter()
                        .partition(|&b| b & !(SLAB - 1) == base);
                    blocks = kept;
                    let generation = generation_of(base as *const u8);
                    gone.into_iter().for_each(free_one);
                    assert_ne!(generation_of(base as *const u8), generation, "not emptied");
                }
                _ => {
                    let threshold = [0, 62, 125, 250][rng.next(4)];
                    let most = 1000 - threshold;
                    // Which slabs it chooses, with no budget: every partial
                    // one, and every full one with room enough.
                    let expected: Vec<usize> = per_slab(&blocks)
                        .into_iter()
                        .filter(|&(_, n)| {
                            n as i32 <= listable(class)
                                || (threshold > 0 && n * 1000 / blocks_a_slab <= most)
                        })
                        .map(|(base, _)| base)
                        .collect();
                    plan(usize::MAX, threshold as u32, class..class + 1);
                    let chosen: Vec<usize> = per_slab(&blocks)
                        .into_keys()
                        .filter(|&base| {
                            slab_of(base as *const u8).state.load(Ordering::Relaxed) == EVACUATING
                        })
                        .collect();
                    assert_eq!(chosen, expected, "threshold {threshold}");
                    check_model(class, &blocks, &chosen);
                    // Blocks freed while being emptied: some slabs empty.
                    for _ in 0..rng.next(blocks.len() / 2 + 1) {
                        let b = blocks.swap_remove(rng.next(blocks.len()));
                        free_one(b);
                    }
                    let left: Vec<usize> = per_slab(&blocks).into_keys().collect();
                    let emptied = chosen.iter().filter(|b| !left.contains(b)).count();
                    check_model(class, &blocks, &chosen);
                    assert_eq!(finish_evacuation(), emptied);
                }
            }
            check_model(class, &blocks, &[]);
        }
        blocks.into_iter().for_each(free_one);
        check_model(class, &[], &[]);
        assert_eq!(slabs_of_class(class), 0);
        let lists = LISTS[class].lock();
        assert!(lists.partial.is_empty() && lists.full.0.is_null() && lists.full_len == 0);
    }

    /// The walk of a list of full slabs lets go of the class's lock every
    /// [`WALK`] slabs and still sees every slab that stays in the list once,
    /// with other slabs coming and going meanwhile, on another thread; a
    /// marker left linked (by a walk that panicked) is unlinked by the next
    /// walk.
    #[test]
    fn a_walk_sees_every_full_slab_once_however_the_list_changes() {
        let _one = EVACUATION_TESTS.lock();
        let _planning = PLANNING.lock();
        let class = model_class();
        // Headers of no region: only their links are used.
        let slabs: Vec<Box<Slab>> = (0..WALK * 40 + 7).map(|_| Box::new(Slab::new())).collect();
        let mut lists = LISTS[class].lock();
        for s in &slabs {
            lists.link_full(s);
        }
        // A marker left behind.
        lists.insert_after(slabs[5].as_ref(), &MARKERS[class]);
        drop(lists);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = std::sync::Arc::clone(&stop);
        // Slabs that come and go, at either end of the list and in the
        // middle, while the walk lets go of the lock.
        let churn = std::thread::spawn(move || {
            let extra: Vec<Box<Slab>> = (0..64).map(|_| Box::new(Slab::new())).collect();
            let mut turns = 0u64;
            while !stopping.load(Ordering::Relaxed) || turns < 100 {
                let mut lists = LISTS[class].lock();
                for s in &extra {
                    lists.link_full(s);
                }
                drop(lists);
                std::thread::yield_now();
                let mut lists = LISTS[class].lock();
                for s in &extra {
                    lists.unlink_full(s);
                }
                drop(lists);
                turns += 1;
            }
            extra
                .iter()
                .map(|s| s.as_ref() as *const Slab as usize)
                .collect::<Vec<_>>()
        });
        let mut seen = Vec::new();
        for _ in 0..if cfg!(miri) { 1 } else { 20 } {
            seen.clear();
            walk_full(class, |slab| seen.push(slab.0 as usize));
        }
        stop.store(true, Ordering::Relaxed);
        let extra = churn.join().unwrap();
        let all: std::collections::HashSet<usize> = slabs
            .iter()
            .map(|s| s.as_ref() as *const Slab as usize)
            .collect();
        let stayed: Vec<usize> = seen
            .iter()
            .copied()
            .filter(|s| !extra.contains(s))
            .collect();
        let unique: std::collections::HashSet<usize> = stayed.iter().copied().collect();
        assert_eq!(unique.len(), stayed.len(), "a slab seen twice");
        assert_eq!(unique, all, "a slab that stayed in the list not seen");
        let mut lists = LISTS[class].lock();
        assert!(!lists.has(&MARKERS[class]), "the marker left in the list");
        for s in &slabs {
            lists.unlink_full(s);
        }
        assert!(lists.full.0.is_null() && lists.full_len == 0);
    }
}
