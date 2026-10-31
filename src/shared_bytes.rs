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

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
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

/// More holders than this and the process stops, as `Arc` does long before
/// its own count can wrap: a count that wrapped would free bytes in use.
const MAX_HOLDERS: u32 = i32::MAX as u32;

pub struct SharedBytes {
    /// The header, with `len` bytes after it.
    to: NonNull<Header>,
}

// The bytes are never written after they are made, and the count is atomic.
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
        let layout = Self::layout(len);
        let to = match crate::value_slab::class_of(layout.size()) {
            Some(class) => crate::value_slab::alloc(class).cast::<Header>(),
            None => {
                // SAFETY: the layout is never of size zero, for the header.
                let block = unsafe { alloc(layout) } as *mut Header;
                let Some(to) = NonNull::new(block) else {
                    handle_alloc_error(layout)
                };
                to
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
        // SAFETY: `len` initialized bytes follow the header, and are not
        // written again.
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

    /// The bytes this takes on the heap, header and all.
    #[inline]
    pub fn heap_size(&self) -> usize {
        HEADER + self.len()
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
            // with this layout.
            None => unsafe { dealloc(self.to.as_ptr() as *mut u8, layout) },
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
            assert_eq!(shared.heap_size(), len + 8);
        }
        assert_eq!(&SharedBytes::concat(&[b"ab", b"", b"cde"])[..], b"abcde");
        assert_eq!(&SharedBytes::from("text")[..], b"text");
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
                std::thread::spawn(move || {
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
