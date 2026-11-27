//! Bytes that are shared and never changed, behind one pointer: what a string
//! key's value is.
//!
//! An `Arc<[u8]>` is two words where it is held, the pointer and the length,
//! and two more in front of the bytes, a count of holders and a count of weak
//! holders nobody here ever takes. As the value of a key that made every
//! entry of every table eight bytes larger than it had to be, in a table that
//! is two fifths empty, and every value eight bytes larger again.
//!
//! [`SharedBytes`] is one word where it is held. The length is in front of
//! the bytes, beside the count, in four bytes each: a value is at most 512 MB
//! ([`MAX_LEN`] is what fits), and a value held four thousand million times
//! over has not been met.
//!
//! A block of up to [`crate::value_slab::MAX_BLOCK`] bytes, header and all,
//! is in KiviDB's own slabs, not the system allocator's (#113): see
//! `value_slab`.
//!
//! **Room to grow.** A slab block's size is its class's, and the length
//! says the class: a slab block has the room its class leaves and no
//! more. A larger block is the system allocator's, and has a word of its
//! own in front of the header ([`SIZE_WORD`]): the block's size, which is
//! what it was allocated with and is freed with. A block made for a value
//! of a known length (a `SET`, a load, a copy) is that length's size; one
//! an `APPEND` makes or grows ([`SharedBytes::append`]) has room to spare
//! by [`room_for_append`], so that the next appends are written in place,
//! and a block that has none left is grown with `realloc`. Nothing outside
//! this file sees the room: the length is what every reader, writer and
//! copier of the bytes uses, and [`SharedBytes::heap_size`] (what memory
//! accounting reads) is the block's whole size.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error, realloc};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering, fence};

/// The longest value there can be. Nothing a client can send is longer than
/// 512 MB, and the commands that make a value longer stop there.
pub const MAX_LEN: usize = u32::MAX as usize;

/// What is in front of the bytes.
#[repr(C)]
struct Header {
    /// How many [`SharedBytes`] point here.
    holders: AtomicU32,
    len: u32,
}

const HEADER: usize = std::mem::size_of::<Header>();

/// In front of the header of a block of the system allocator's: the
/// block's size, this word included.
const SIZE_WORD: usize = std::mem::size_of::<usize>();
/// What a block of the system allocator's is aligned to: the size word's.
const BIG_ALIGN: usize = std::mem::align_of::<usize>();
const _: () = assert!(SIZE_WORD.is_multiple_of(std::mem::align_of::<Header>()));

/// The most room an append leaves after a value: 16 MiB.
const MAX_SPARE: usize = 16 << 20;
/// The longest value an `APPEND` makes (Redis's `proto-max-bulk-len`):
/// no room is left past it.
const APPEND_MAX: usize = 512 << 20;

/// The room an append that makes a value of `len` bytes leaves in a block
/// of the system allocator's: a quarter of `len` more, at most
/// [`MAX_SPARE`], and none past [`APPEND_MAX`]. Appends grow such a value
/// by a quarter at a time up to 64 MiB, so each byte appended is copied, on
/// average, at most four times there even if every growth moves the block;
/// past it the block grows 16 MiB at a time (about nine copies a byte at
/// 256 MiB if every growth moved it, though a block that large is one the
/// allocator grows by remapping its pages). The room left is at most a
/// fifth of the block.
#[inline]
pub fn room_for_append(len: usize) -> usize {
    let room = len + (len / 4).min(MAX_SPARE);
    room.min(APPEND_MAX.max(len))
}

/// Whether bytes of `len` are in a block of the system allocator's, not a
/// slab's: what says which of the two a block is, and so how it is freed.
#[inline(always)]
fn is_big(len: usize) -> bool {
    crate::value_slab::class_of(HEADER + len).is_none()
}

/// The layout of a block of the system allocator's of `size` bytes.
#[inline]
fn big_layout(size: usize) -> Layout {
    // Cannot fail: the size is under 2^34 and the alignment eight.
    Layout::from_size_align(size, BIG_ALIGN).expect("layout of a value")
}

/// More holders than this and the process stops, as `Arc` does long before
/// its own count can wrap: a count that wrapped would free bytes in use.
const MAX_HOLDERS: u32 = i32::MAX as u32;

pub struct SharedBytes {
    /// The header, with `len` bytes after it.
    to: NonNull<Header>,
}

// The bytes are written after they are made only by their one holder, with
// nothing else able to reach them ([`SharedBytes::write_in_place`]), and the
// count is atomic.
unsafe impl Send for SharedBytes {}
unsafe impl Sync for SharedBytes {}

impl SharedBytes {
    fn layout(len: usize) -> Layout {
        assert!(len <= MAX_LEN, "a value of {len} bytes");
        // Cannot fail: the size is under 2^33 and the alignment is four.
        Layout::from_size_align(HEADER + len, std::mem::align_of::<Header>())
            .expect("layout of a value")
    }

    /// `len` bytes, written by `fill`, which is given them uninitialized and
    /// must write every one.
    ///
    /// # Safety
    /// `fill` must initialize all `len` bytes at the pointer it is given.
    #[inline]
    unsafe fn make(len: usize, fill: impl FnOnce(*mut u8)) -> Self {
        // SAFETY: as the caller promises.
        unsafe { Self::make_with_room(len, len, fill) }
    }

    /// [`make`](Self::make), in a block with room for `room` bytes if it
    /// is the system allocator's (a slab block has its class's).
    ///
    /// # Safety
    /// As `make`'s.
    #[inline]
    unsafe fn make_with_room(len: usize, room: usize, fill: impl FnOnce(*mut u8)) -> Self {
        let layout = Self::layout(len);
        let to = match crate::value_slab::class_of(layout.size()) {
            Some(class) => crate::value_slab::alloc(class).cast::<Header>(),
            None => {
                let size = SIZE_WORD + HEADER + room.clamp(len, MAX_LEN);
                let layout = big_layout(size);
                // SAFETY: the layout is never of size zero, for the header.
                let base = unsafe { alloc(layout) };
                if base.is_null() {
                    handle_alloc_error(layout)
                }
                // SAFETY: the block is `size` bytes, aligned for the word,
                // and nobody else's; the header follows the word.
                unsafe {
                    base.cast::<usize>().write(size);
                    NonNull::new_unchecked(base.add(SIZE_WORD)).cast::<Header>()
                }
            }
        };
        let block = to.as_ptr();
        // SAFETY: `block` is `HEADER + len` bytes, aligned for a `Header`,
        // and nothing else has it yet.
        unsafe {
            block.write(Header {
                holders: AtomicU32::new(1),
                len: len as u32,
            });
            fill((block as *mut u8).add(HEADER));
        }
        Self { to }
    }

    /// The bytes of `parts`, one after another, in one block.
    pub fn concat(parts: &[&[u8]]) -> Self {
        let len = parts.iter().map(|part| part.len()).sum();
        // SAFETY: the parts are `len` bytes together, and all are copied.
        unsafe {
            Self::make(len, |mut at| {
                for part in parts {
                    std::ptr::copy_nonoverlapping(part.as_ptr(), at, part.len());
                    at = at.add(part.len());
                }
            })
        }
    }

    /// Copy `parts` into a new block and let the caller finish the bytes before
    /// the immutable value is exposed to readers.
    pub(crate) fn concat_with_mutation(parts: &[&[u8]], finish: impl FnOnce(&mut [u8])) -> Self {
        let len = parts.iter().map(|part| part.len()).sum();
        // SAFETY: the parts initialize every byte before `finish` receives the
        // exclusive slice; the new block is not visible until this returns.
        unsafe {
            Self::make(len, |mut at| {
                let start = at;
                for part in parts {
                    std::ptr::copy_nonoverlapping(part.as_ptr(), at, part.len());
                    at = at.add(part.len());
                }
                finish(std::slice::from_raw_parts_mut(start, len));
            })
        }
    }

    #[inline]
    fn header(&self) -> &Header {
        // SAFETY: the block lives as long as any holder does.
        unsafe { self.to.as_ref() }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        let len = self.header().len as usize;
        // SAFETY: `len` initialized bytes follow the header, written again
        // only by a sole holder that nothing else can reach.
        unsafe { std::slice::from_raw_parts((self.to.as_ptr() as *const u8).add(HEADER), len) }
    }

    /// Whether the two are the same block, not merely the same bytes.
    #[inline]
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        a.to == b.to
    }

    /// Where the block is: for compaction to ask whether its slab is being
    /// emptied (`value_slab::is_evacuating`).
    #[inline]
    pub fn block(&self) -> *const u8 {
        self.to.as_ptr() as *const u8
    }

    /// Move the bytes to a new block if theirs is in a slab compaction is
    /// emptying (#113) and this is their only holder; `regions` is
    /// `value_slab::regions_snapshot`. Returns the old block,
    /// to be let go of once whatever lock the caller holds is: freeing it can
    /// give a slab back to the kernel.
    ///
    /// With `&mut self` and one holder, nothing else can be reading the
    /// bytes, or get to them but through `self`.
    pub fn relocate(&mut self, regions: &[usize]) -> Option<SharedBytes> {
        let size = HEADER + self.len();
        let class = crate::value_slab::class_of(size)?;
        if !crate::value_slab::is_evacuating(self.block(), regions) {
            return None;
        }
        // Acquire: what a holder that let go just now did with the bytes
        // comes before they are copied and the block freed.
        if self.header().holders.load(Ordering::Acquire) != 1 {
            return None;
        }
        let to = crate::value_slab::alloc(class).cast::<Header>();
        // SAFETY: both blocks are `size` bytes of this class, and the new one
        // is nobody else's.
        unsafe {
            std::ptr::copy_nonoverlapping(self.block(), to.as_ptr() as *mut u8, size);
        }
        Some(std::mem::replace(self, SharedBytes { to }))
    }

    /// Whether this is the bytes' only holder: letting go of it frees them.
    #[inline]
    pub fn is_unique(&self) -> bool {
        self.header().holders.load(Ordering::Acquire) == 1
    }

    /// Whether `len` bytes can take these bytes' place in their block
    /// ([`write_in_place`](Self::write_in_place)): a block of the same slab
    /// class, or, the system allocator's, of the same length (it is freed by
    /// its size).
    #[inline]
    pub fn fits(&self, len: usize) -> bool {
        match crate::value_slab::class_of(HEADER + self.len()) {
            Some(now) => crate::value_slab::class_of(HEADER + len) == Some(now),
            None => len == self.len(),
        }
    }

    /// The bytes made `bytes`, in this block. A SET over a string of its own
    /// otherwise takes a new block and lets go of the old one: two blocks'
    /// first lines, not in the cache, about a tenth of the server's CPU for
    /// a SET of 4,072 bytes.
    ///
    /// # Safety
    /// This is the bytes' only holder ([`is_unique`](Self::is_unique)),
    /// nothing can get to them but through it until this returns (the
    /// caller holds what it is kept in for writing), and
    /// [`fits`](Self::fits)`(bytes.len())`.
    pub unsafe fn write_in_place(&self, bytes: &[u8]) {
        debug_assert!(self.is_unique() && self.fits(bytes.len()));
        // The length says how the block is freed: it keeps its kind.
        debug_assert_eq!(is_big(self.len()), is_big(bytes.len()));
        let block = self.to.as_ptr();
        // SAFETY: as the caller says, nothing reads the bytes or the length
        // while they change, and the block has room for `bytes` in its class.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                (block as *mut u8).add(HEADER),
                bytes.len(),
            );
            std::ptr::addr_of_mut!((*block).len).write(bytes.len() as u32);
        }
    }

    /// `patch` written over the bytes from `offset`, in this block: as
    /// [`write_at`] leaves them, with the length grown to the patch's end.
    ///
    /// # Safety
    /// As [`write_in_place`](Self::write_in_place)'s, for the length after
    /// it: `fits(max(len, offset + patch.len()))`. `patch` is not in the block.
    pub(crate) unsafe fn write_at_in_place(&self, offset: usize, patch: &[u8]) {
        let len = self.header().len as usize;
        let end = len.max(offset + patch.len());
        debug_assert!(self.is_unique() && self.fits(end));
        debug_assert_eq!(is_big(len), is_big(end));
        let block = self.to.as_ptr();
        // SAFETY: as in `write_in_place`; the block has room for `end` bytes.
        unsafe {
            write_at((block as *mut u8).add(HEADER), len, offset, patch);
            std::ptr::addr_of_mut!((*block).len).write(end as u32);
        }
    }

    /// `old` in a new block, with `patch` written from `offset` as
    /// [`write_at`] does: the old bytes copied once.
    pub(crate) fn written_at(old: &[u8], offset: usize, patch: &[u8]) -> Self {
        // Too long for `layout`, rather than wrapped, if the end overflows.
        let len = old.len().max(offset.saturating_add(patch.len()));
        // SAFETY: `old` and `write_at` initialize all `len` bytes.
        unsafe {
            Self::make(len, |at| {
                std::ptr::copy_nonoverlapping(old.as_ptr(), at, old.len());
                write_at(at, old.len(), offset, patch);
            })
        }
    }

    /// The bytes this takes on the heap, header and all.
    #[inline]
    pub fn heap_size(&self) -> usize {
        let len = self.len();
        if is_big(len) {
            self.big_size()
        } else {
            HEADER + len
        }
    }

    /// The size of a block of the system allocator's, its size word
    /// included: what it was allocated with.
    #[inline]
    fn big_size(&self) -> usize {
        debug_assert!(is_big(self.len()));
        // SAFETY: such a block has its size in the word before the header.
        unsafe {
            (self.to.as_ptr() as *const u8)
                .sub(SIZE_WORD)
                .cast::<usize>()
                .read()
        }
    }

    /// How many more bytes than its length the block has room for, kept
    /// for appends ([`room_for_append`]): 0 for a slab's block (whose class
    /// is what its length says) and for a block made for a value of a known
    /// length.
    #[inline]
    pub fn spare(&self) -> usize {
        let len = self.len();
        if is_big(len) {
            self.big_size() - SIZE_WORD - HEADER - len
        } else {
            0
        }
    }

    /// Whether the bytes can grow to `len` in this block: in the same slab
    /// class, or within the room of a block of the system allocator's.
    #[inline]
    pub fn has_room(&self, len: usize) -> bool {
        let now = self.len();
        match crate::value_slab::class_of(HEADER + now) {
            Some(class) => crate::value_slab::class_of(HEADER + len) == Some(class),
            None => len >= now && SIZE_WORD + HEADER + len <= self.big_size(),
        }
    }

    /// The bytes of `old`, `gap` zero bytes and then `extra`, in a new
    /// block, with the room an append leaves ([`room_for_append`]) if the
    /// block is the system allocator's.
    pub fn appended(old: &[u8], gap: usize, extra: &[u8]) -> Self {
        // Past `MAX_LEN` (`layout` refuses it) rather than wrapped.
        let len = old.len().saturating_add(gap).saturating_add(extra.len());
        // SAFETY: `old`, the gap and `extra` are `len` bytes together, and
        // `write_at` zeroes the gap and copies `extra`.
        unsafe {
            Self::make_with_room(len, room_for_append(len), |at| {
                std::ptr::copy_nonoverlapping(old.as_ptr(), at, old.len());
                write_at(at, old.len(), old.len() + gap, extra);
            })
        }
    }

    /// `gap` zero bytes and then `extra` added to the end of the bytes. In
    /// this block when it has room ([`has_room`](Self::has_room)); else a
    /// block of the system allocator's is grown with `realloc` to the room
    /// an append leaves ([`room_for_append`]), which the allocator may do
    /// where the block is or by moving it, and a slab's block is replaced
    /// by a new one ([`appended`](Self::appended)), the old one freed.
    /// Returns whether the bytes stayed where they were.
    ///
    /// # Safety
    /// This is the bytes' only holder ([`is_unique`](Self::is_unique)),
    /// nothing can get to them but through it until this returns (the
    /// caller holds what it is kept in for writing), and nothing keeps
    /// their address across it: the block may move. `extra` is not in the
    /// block, and the length after it is at most [`MAX_LEN`].
    pub(crate) unsafe fn append(&mut self, gap: usize, extra: &[u8]) -> bool {
        debug_assert!(self.is_unique());
        let len = self.len();
        let new = len.saturating_add(gap).saturating_add(extra.len());
        assert!(new <= MAX_LEN, "a value of {new} bytes");
        let mut stays = true;
        if !self.has_room(new) {
            if !is_big(len) {
                *self = Self::appended(self.as_slice(), gap, extra);
                return false;
            }
            let size = self.big_size();
            let grown = SIZE_WORD + HEADER + room_for_append(new);
            // SAFETY: the word is the block's first.
            let was = unsafe { (self.to.as_ptr() as *mut u8).sub(SIZE_WORD) };
            // SAFETY: the block was allocated with this layout, and
            // nothing but `self` knows where it is.
            let base = unsafe { realloc(was, big_layout(size), grown) };
            stays = base == was;
            if base.is_null() {
                // The old block is as it was, and still `self`'s.
                handle_alloc_error(big_layout(grown))
            }
            // SAFETY: the block is `grown` bytes now, its first `size` as
            // they were: the word, the header and the bytes.
            unsafe {
                base.cast::<usize>().write(grown);
                self.to = NonNull::new_unchecked(base.add(SIZE_WORD)).cast();
            }
        }
        let block = self.to.as_ptr();
        // SAFETY: the block has room for `new` bytes, and as the caller
        // says, nothing reads the bytes or the length while they change.
        unsafe {
            write_at((block as *mut u8).add(HEADER), len, len + gap, extra);
            std::ptr::addr_of_mut!((*block).len).write(new as u32);
        }
        stays
    }
}

/// `patch` written at `offset` of the `len` bytes at `to`. Bytes between
/// `len` and `offset` become zero, as Redis grows a string.
///
/// # Safety
/// `to` is writable for `max(len, offset + patch.len())` bytes, its first
/// `len` initialized, and `patch` is not among them.
pub(crate) unsafe fn write_at(to: *mut u8, len: usize, offset: usize, patch: &[u8]) {
    // SAFETY: as the caller promises.
    unsafe {
        if offset > len {
            std::ptr::write_bytes(to.add(len), 0, offset - len);
        }
        std::ptr::copy_nonoverlapping(patch.as_ptr(), to.add(offset), patch.len());
    }
}

impl Clone for SharedBytes {
    #[inline]
    fn clone(&self) -> Self {
        // Relaxed, as in `Arc`: a new holder is made from one that exists,
        // so the block cannot be freed meanwhile.
        let before = self.header().holders.fetch_add(1, Ordering::Relaxed);
        if before > MAX_HOLDERS {
            std::process::abort();
        }
        Self { to: self.to }
    }
}

impl Drop for SharedBytes {
    #[inline]
    fn drop(&mut self) {
        if self.header().holders.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        // As in `Arc`: what other threads did with the bytes before they let
        // go must be seen here before the block is freed.
        fence(Ordering::Acquire);
        let layout = Self::layout(self.header().len as usize);
        match crate::value_slab::class_of(layout.size()) {
            // SAFETY: this was the last holder, and a block of this size
            // came from this class.
            Some(class) => unsafe { crate::value_slab::free(self.to.cast(), class) },
            // SAFETY: this was the last holder, and the block was allocated
            // with the size its word says, from the word on.
            None => unsafe {
                let size = self.big_size();
                dealloc(
                    (self.to.as_ptr() as *mut u8).sub(SIZE_WORD),
                    big_layout(size),
                )
            },
        }
    }
}

impl std::ops::Deref for SharedBytes {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for SharedBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::borrow::Borrow<[u8]> for SharedBytes {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<&[u8]> for SharedBytes {
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        // SAFETY: all `bytes.len()` bytes are copied.
        unsafe {
            Self::make(bytes.len(), |at| {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), at, bytes.len())
            })
        }
    }
}

impl<const N: usize> From<&[u8; N]> for SharedBytes {
    #[inline]
    fn from(bytes: &[u8; N]) -> Self {
        Self::from(&bytes[..])
    }
}

impl From<&str> for SharedBytes {
    #[inline]
    fn from(text: &str) -> Self {
        Self::from(text.as_bytes())
    }
}

impl From<Vec<u8>> for SharedBytes {
    #[inline]
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(&bytes[..])
    }
}

impl From<&Vec<u8>> for SharedBytes {
    #[inline]
    fn from(bytes: &Vec<u8>) -> Self {
        Self::from(&bytes[..])
    }
}

impl From<String> for SharedBytes {
    #[inline]
    fn from(text: String) -> Self {
        Self::from(text.as_bytes())
    }
}

impl From<Box<[u8]>> for SharedBytes {
    #[inline]
    fn from(bytes: Box<[u8]>) -> Self {
        Self::from(&bytes[..])
    }
}

impl From<std::sync::Arc<[u8]>> for SharedBytes {
    #[inline]
    fn from(bytes: std::sync::Arc<[u8]>) -> Self {
        Self::from(&bytes[..])
    }
}

impl From<&std::sync::Arc<[u8]>> for SharedBytes {
    #[inline]
    fn from(bytes: &std::sync::Arc<[u8]>) -> Self {
        Self::from(&bytes[..])
    }
}

impl From<&SharedBytes> for std::sync::Arc<[u8]> {
    #[inline]
    fn from(bytes: &SharedBytes) -> Self {
        std::sync::Arc::from(bytes.as_slice())
    }
}

impl From<SharedBytes> for std::sync::Arc<[u8]> {
    #[inline]
    fn from(bytes: SharedBytes) -> Self {
        std::sync::Arc::from(bytes.as_slice())
    }
}

impl PartialEq for SharedBytes {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.to == other.to || self.as_slice() == other.as_slice()
    }
}

impl Eq for SharedBytes {}

impl std::hash::Hash for SharedBytes {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl std::fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_slice().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_is_one_word_and_so_is_an_option_of_it() {
        assert_eq!(std::mem::size_of::<SharedBytes>(), 8);
        assert_eq!(std::mem::size_of::<Option<SharedBytes>>(), 8);
        assert_eq!(HEADER, 8);
    }

    #[test]
    fn it_holds_what_it_was_given() {
        for len in [0usize, 1, 7, 8, 9, 100, 4096, 70_000] {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let shared = SharedBytes::from(&bytes[..]);
            assert_eq!(shared.len(), len);
            assert_eq!(&shared[..], &bytes[..]);
            // A block of the system allocator's has its size word too.
            let word = if len > 26_000 { 8 } else { 0 };
            assert_eq!(shared.heap_size(), len + 8 + word);
            assert_eq!(shared.spare(), 0);
        }
        assert_eq!(&SharedBytes::concat(&[b"ab", b"", b"cde"])[..], b"abcde");
        assert_eq!(&SharedBytes::from("text")[..], b"text");
    }

    #[test]
    fn an_append_leaves_a_quarter_more_room_up_to_16_mib_and_none_past_512_mib() {
        assert_eq!(room_for_append(0), 0);
        assert_eq!(room_for_append(100_000), 125_000);
        assert_eq!(room_for_append(64 << 20), 80 << 20);
        assert_eq!(room_for_append(100 << 20), 116 << 20);
        assert_eq!(room_for_append(505 << 20), 512 << 20);
        assert_eq!(room_for_append(512 << 20), 512 << 20);
        assert_eq!(room_for_append(600 << 20), 600 << 20);
    }

    /// What this thread's counter moved by while `f` ran, if it has one.
    fn counted(f: impl FnOnce()) -> Option<isize> {
        let before = crate::memory_use::allocated_by_this_thread();
        f();
        let after = crate::memory_use::allocated_by_this_thread();
        Some(after? - before?)
    }

    /// Appends onto bytes of every size, some after a gap of zeros: in
    /// their block while it has room, else a slab's block replaced and a
    /// large one grown by a quarter.
    /// The allocator counts each block at its whole size, room included,
    /// and a block is freed with the size it was allocated with.
    ///
    /// Each start is run twice and only the second run's counts are held to
    /// the blocks' sizes: the first slab of a class, or a new region, a
    /// process takes grows the slabs' own lists (`value_slab`'s `Vec`s of
    /// slabs and regions), which the allocator counts too, on the thread
    /// that took it. In a process of its own (nextest) that is this test's
    /// first run; the steps where it happened are printed.
    #[test]
    fn appends_write_in_the_room_and_grow_a_large_block_by_a_quarter() {
        let slab = crate::value_slab::MAX_BLOCK - HEADER;
        let largest = crate::value_slab::class_of(crate::value_slab::MAX_BLOCK);
        for (run, start) in [0, 100, 4000, 20_000, slab + 1, 1 << 20]
            .into_iter()
            .flat_map(|start| [(0, start), (1, start)])
        {
            let mut model: Vec<u8> = (0..start).map(|i| (i % 251) as u8).collect();
            model.reserve(4 << 20);
            // Some after a gap of zeros, over room the allocator may have
            // left with other bytes in it.
            let extras: Vec<(usize, Vec<u8>)> = (0..400)
                .map(|i| {
                    (
                        [0, 0, 1500, 0, 7][i % 5],
                        vec![(i % 7) as u8 + 1; [3, 1000, 9000][i % 3]],
                    )
                })
                .collect();
            let regions_before = crate::value_slab::regions();
            let made = counted(|| {
                let mut bytes = SharedBytes::from(&model[..]);
                assert_eq!(bytes.spare(), 0);
                let mut grown = 0;
                for (gap, extra) in &extras {
                    let len = bytes.len() + gap + extra.len();
                    // The largest slab class is a `value_slab` test's alone.
                    if crate::value_slab::class_of(HEADER + len) == largest {
                        continue;
                    }
                    let (room, spare, heap) =
                        (bytes.has_room(len), bytes.spare(), bytes.heap_size());
                    let at = bytes.as_ptr();
                    let mut stayed = false;
                    // SAFETY: the only holder, kept by this test alone.
                    let delta = counted(|| stayed = unsafe { bytes.append(*gap, extra) });
                    model.resize(model.len() + gap, 0);
                    model.extend_from_slice(extra);
                    assert!(bytes[..] == model[..], "{start}: {len}");
                    if room {
                        assert!(stayed && bytes.as_ptr() == at, "{start}: {len}");
                        if is_big(len) {
                            assert_eq!(bytes.heap_size(), heap, "{start}: {len}");
                        }
                    } else {
                        grown += 1;
                    }
                    if is_big(len) {
                        let want = if room {
                            spare - gap - extra.len()
                        } else {
                            len / 4
                        };
                        assert_eq!(bytes.spare(), want, "{start}: {len}");
                        assert_eq!(bytes.heap_size(), SIZE_WORD + HEADER + len + want);
                    } else {
                        assert_eq!(bytes.spare(), 0);
                    }
                    // What the allocator counted is what the block's size
                    // moved by, a slab's block at its class's size.
                    let counted_as = |size: usize| {
                        crate::value_slab::class_of(size)
                            .map_or(size, crate::value_slab::block_size)
                            as isize
                    };
                    if let Some(delta) = delta {
                        let want = counted_as(bytes.heap_size()) - counted_as(heap);
                        if run == 1 {
                            assert_eq!(delta, want, "{start}: {len}");
                        } else if delta != want {
                            let regions = crate::value_slab::regions() - regions_before;
                            eprintln!(
                                "first run, start {start}: {len} bytes counted {delta}, \
                                 the blocks {want} ({} more; {regions} regions so far)",
                                delta - want
                            );
                        }
                    }
                }
                // About 2 MiB in quarters from a few KiB: a few dozen growths.
                assert!(grown < 60, "{start}: grown {grown} times");
                drop(bytes);
            });
            if let Some(made) = made {
                eprintln!("run {run}, start {start}: {made} bytes still counted after the drop");
                if run == 1 {
                    assert_eq!(
                        made, 0,
                        "{start}: freed with another size than it was allocated with"
                    );
                }
            }
        }
    }

    #[test]
    fn concat_finishes_mutation_before_sharing_bytes() {
        let bytes = SharedBytes::concat_with_mutation(&[b"header", b"body"], |bytes| {
            bytes[6] = b'B';
        });
        assert_eq!(&bytes[..], b"headerBody");
    }

    #[test]
    fn a_copy_is_the_same_block_and_equal_bytes_are_equal() {
        let one = SharedBytes::from(&b"value"[..]);
        let two = one.clone();
        let other = SharedBytes::from(&b"value"[..]);
        assert!(SharedBytes::ptr_eq(&one, &two));
        assert!(!SharedBytes::ptr_eq(&one, &other));
        assert_eq!(one, other);
        assert_ne!(one, SharedBytes::from(&b"valuf"[..]));
        drop(one);
        assert_eq!(&two[..], b"value");
    }

    #[test]
    fn many_threads_take_and_let_go_of_it_and_it_is_freed_once() {
        let shared = SharedBytes::from(&[7u8; 1000][..]);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mine = shared.clone();
                crate::cpus::spawn(move || {
                    for _ in 0..20_000 {
                        let copy = mine.clone();
                        assert_eq!(copy[999], 7);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(shared.header().holders.load(Ordering::Relaxed), 1);
    }
}
