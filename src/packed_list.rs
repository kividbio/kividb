//! A list whose elements lie end to end in a few blocks of memory.
//!
//! A list as a `VecDeque<Arc<[u8]>>` pays, for every element, an 8-byte
//! slot, 16 bytes of `Arc` header and an allocation of its own: for a
//! 32-byte element, as much again as the element. Here elements are kept in
//! *chunks*: one buffer each, the elements in it one after another, each
//! between two copies of its length:
//!
//! ```text
//! [ len: u16 ][ element's bytes ][ len: u16 ]   [ len ][ bytes ][ len ]   ...
//! ```
//!
//! Four bytes an element besides its own. The length in front lets a chunk
//! be read forwards, the one behind backwards, so either end's element is
//! found in one step and popped by moving a mark; element `i` of a chunk is
//! reached by stepping from the nearer end, at most half a chunk.
//!
//! A chunk has room to grow at both ends: its bytes begin at `start`, not at
//! the buffer's beginning, so an element is added in front by writing it
//! before `start` and popped there by moving `start` up. A queue (pushed at
//! one end, popped at the other) and a stack both work a chunk without
//! moving its bytes.
//!
//! A list is one chunk while it has at most [`CHUNK_ENTRIES`] elements, and
//! a deque of chunks beyond that. While elements are only added and taken
//! at the ends, every chunk but the first and the last holds exactly
//! `CHUNK_ENTRIES` (the list is *regular*), and element `i` is found by
//! arithmetic: which chunk, and where in it. An insertion or removal in the
//! middle (`LINSERT`, `LREM`) may leave a chunk between the ends that is
//! not full; the list then finds its chunks by adding up their counts from
//! the nearer end, as a Redis quicklist always does.
//!
//! The list refuses an element longer than [`MAX_LEN`]: whoever holds it
//! then keeps the elements apart, as before.

use std::collections::VecDeque;

/// The longest element a packed list holds.
pub const MAX_LEN: usize = 1024;
/// The most elements in one chunk.
pub const CHUNK_ENTRIES: usize = 64;
/// The two lengths around an element.
const FRAME: usize = 4;
/// The least a buffer that holds something grows by: a few elements' room,
/// so that a list pushed to one element at a time is not reallocated at
/// every push.
const MIN_GROWTH: usize = 128;

/// How much room to make for `need` more bytes in a chunk holding `live`:
/// exactly `need` in an empty one (a list of one element takes no more),
/// else a quarter over what it holds, at least [`MIN_GROWTH`].
#[inline(always)]
fn growth(need: usize, live: usize) -> usize {
    if live == 0 {
        need
    } else {
        need.max(live / 4).max(MIN_GROWTH)
    }
}

#[inline(always)]
fn read_len(buf: &[u8], at: usize) -> usize {
    u16::from_le_bytes([buf[at], buf[at + 1]]) as usize
}

/// Write `element` between its two lengths at `at`; `at + element.len() +
/// FRAME <= buf.len()`.
#[inline]
fn write_frame(buf: &mut [u8], at: usize, element: &[u8]) {
    let len = (element.len() as u16).to_le_bytes();
    let n = element.len();
    buf[at..at + 2].copy_from_slice(&len);
    buf[at + 2..at + 2 + n].copy_from_slice(element);
    buf[at + 2 + n..at + FRAME + n].copy_from_slice(&len);
}

/// Up to [`CHUNK_ENTRIES`] elements in one buffer.
#[derive(Clone, Default)]
struct Chunk {
    buf: Vec<u8>,
    /// Where the first element's frame begins in `buf`.
    start: u32,
    /// Elements held.
    count: u16,
}

impl Chunk {
    #[inline]
    fn count(&self) -> usize {
        self.count as usize
    }

    /// The bytes of the frames held.
    #[inline]
    fn live(&self) -> usize {
        self.buf.len() - self.start as usize
    }

    fn push_back(&mut self, element: &[u8]) {
        let need = element.len() + FRAME;
        if self.buf.capacity() - self.buf.len() < need {
            let start = self.start as usize;
            let live = self.live();
            // What was popped in front is at least what is left: slide the
            // rest down over it (each byte is moved once for every byte
            // popped) and the room is behind.
            if start >= need && start >= live {
                self.buf.copy_within(start.., 0);
                self.buf.truncate(live);
                self.start = 0;
            }
            if self.buf.capacity() - self.buf.len() < need {
                // A quarter over, not the doubling a `Vec` does by itself.
                self.buf.reserve_exact(growth(need, live));
            }
        }
        let at = self.buf.len();
        self.buf.resize(at + need, 0);
        write_frame(&mut self.buf, at, element);
        self.count += 1;
    }

    fn push_front(&mut self, element: &[u8]) {
        let need = element.len() + FRAME;
        if (self.start as usize) < need {
            // Room in front: a quarter of what is held, at least.
            self.grow_front(growth(need, self.live()));
        }
        let at = self.start as usize - need;
        write_frame(&mut self.buf, at, element);
        self.start = at as u32;
        self.count += 1;
    }

    /// At least `room` bytes in front of the frames: moved up into the room
    /// behind them if there is that much, else into a new buffer.
    fn grow_front(&mut self, room: usize) {
        let start = self.start as usize;
        let end = self.buf.len();
        if self.buf.capacity() - end >= room {
            // There is room behind: move the frames up into it.
            self.buf.resize(end + room, 0);
            self.buf.copy_within(start..end, start + room);
            self.start = (start + room) as u32;
        } else {
            let mut buf = Vec::with_capacity(room + end - start);
            buf.resize(room, 0);
            buf.extend_from_slice(&self.buf[start..]);
            self.buf = buf;
            self.start = room as u32;
        }
    }

    /// Room for `bytes` more of frames in front: exactly that in an empty
    /// chunk (a batch that fills it), else as a push makes room, a quarter
    /// of what is held at least.
    fn reserve_front(&mut self, bytes: usize) {
        if (self.start as usize) < bytes {
            self.grow_front(bytes.max(self.live() / 4));
        }
    }

    /// Room for `bytes` more of frames behind, made as
    /// [`reserve_front`](Self::reserve_front) makes it in front.
    fn reserve_back(&mut self, bytes: usize) {
        if self.buf.capacity() - self.buf.len() < bytes {
            let start = self.start as usize;
            let live = self.live();
            if start >= bytes && start >= live {
                self.buf.copy_within(start.., 0);
                self.buf.truncate(live);
                self.start = 0;
            }
            if self.buf.capacity() - self.buf.len() < bytes {
                self.buf.reserve_exact(bytes.max(live / 4));
            }
        }
    }

    /// A bit for each element equal to `element`, the first element's the
    /// lowest: the first `limit` of them, or from the back the last.
    fn matches(&self, element: &[u8], limit: usize, from_back: bool) -> u64 {
        let mut mask = 0u64;
        let mut found = 0;
        let n = self.count();
        if from_back {
            let mut end = self.buf.len();
            for k in 0..n {
                let len = read_len(&self.buf, end - 2);
                let at = end - len - FRAME;
                if len == element.len() && &self.buf[at + 2..at + 2 + len] == element {
                    mask |= 1 << (n - 1 - k);
                    found += 1;
                    if found == limit {
                        break;
                    }
                }
                end = at;
            }
        } else {
            let mut at = self.start as usize;
            for j in 0..n {
                let len = read_len(&self.buf, at);
                if len == element.len() && &self.buf[at + 2..at + 2 + len] == element {
                    mask |= 1 << j;
                    found += 1;
                    if found == limit {
                        break;
                    }
                }
                at += len + FRAME;
            }
        }
        mask
    }

    /// Take out the elements whose bits are set in `mask` (not 0), what is
    /// kept between them moved down in runs.
    fn remove_mask(&mut self, mask: u64) {
        let last = 63 - mask.leading_zeros() as usize;
        let mut read = self.start as usize;
        let mut write = read;
        // Where the run of kept frames not yet moved begins.
        let mut run = read;
        for j in 0..=last {
            let frame = read_len(&self.buf, read) + FRAME;
            if mask & (1 << j) != 0 {
                if write != run {
                    self.buf.copy_within(run..read, write);
                }
                write += read - run;
                run = read + frame;
            }
            read += frame;
        }
        let end = self.buf.len();
        if write != run {
            self.buf.copy_within(run..end, write);
        }
        self.buf.truncate(write + end - run);
        self.count -= mask.count_ones() as u16;
        if self.count == 0 {
            self.emptied();
        }
    }

    /// Hand `each` up to `n` elements from element `i` on, first to last,
    /// while it says to go on. `false` if it said to stop.
    #[inline]
    fn each_from(&self, i: usize, n: usize, each: &mut impl FnMut(&[u8]) -> bool) -> bool {
        let at = if i == 0 {
            self.start as usize
        } else {
            self.at(i).0
        };
        let mut frames = &self.buf[at..];
        for _ in 0..n {
            let len = u16::from_le_bytes([frames[0], frames[1]]) as usize;
            let (frame, rest) = frames.split_at(len + FRAME);
            if !each(&frame[2..2 + len]) {
                return false;
            }
            frames = rest;
        }
        true
    }

    /// Hand `each` up to `n` elements before element `to`, last to first,
    /// while it says to go on. `false` if it said to stop.
    #[inline]
    fn each_back_from(&self, to: usize, n: usize, each: &mut impl FnMut(&[u8]) -> bool) -> bool {
        let end = if to == self.count() {
            self.buf.len()
        } else {
            self.at(to).0
        };
        let mut frames = &self.buf[self.start as usize..end];
        for _ in 0..n {
            let e = frames.len();
            let len = u16::from_le_bytes([frames[e - 2], frames[e - 1]]) as usize;
            let (rest, frame) = frames.split_at(e - len - FRAME);
            if !each(&frame[2..2 + len]) {
                return false;
            }
            frames = rest;
        }
        true
    }

    /// Keep elements `from` up to, not including, `to`. `from <= to <=
    /// count()`.
    fn keep(&mut self, from: usize, to: usize) {
        let n = self.count();
        debug_assert!(from <= to && to <= n);
        if from == 0 && to == n {
            return;
        }
        if from == to {
            self.count = 0;
            self.emptied();
            return;
        }
        // Both found in the chunk as it is, before either end moves.
        let begin = if from > 0 {
            self.at(from).0
        } else {
            self.start as usize
        };
        if to < n {
            let (end, _) = self.at(to);
            self.buf.truncate(end);
        }
        self.start = begin as u32;
        self.count = (to - from) as u16;
    }

    #[inline]
    fn front(&self) -> Option<&[u8]> {
        if self.count == 0 {
            return None;
        }
        let at = self.start as usize;
        let len = read_len(&self.buf, at);
        Some(&self.buf[at + 2..at + 2 + len])
    }

    #[inline]
    fn back(&self) -> Option<&[u8]> {
        if self.count == 0 {
            return None;
        }
        let end = self.buf.len();
        let len = read_len(&self.buf, end - 2);
        Some(&self.buf[end - 2 - len..end - 2])
    }

    /// Take the first element off. `count() > 0`.
    #[inline]
    fn drop_front(&mut self) {
        debug_assert!(self.count > 0);
        let at = self.start as usize;
        let len = read_len(&self.buf, at);
        self.start = (at + len + FRAME) as u32;
        self.count -= 1;
        if self.count == 0 {
            self.emptied();
        }
    }

    /// Take the last element off. `count() > 0`.
    #[inline]
    fn drop_back(&mut self) {
        debug_assert!(self.count > 0);
        let end = self.buf.len();
        let len = read_len(&self.buf, end - 2);
        self.buf.truncate(end - len - FRAME);
        self.count -= 1;
        if self.count == 0 {
            self.emptied();
        }
    }

    /// Nothing is held: the next element starts the buffer again. The
    /// buffer is kept, for a list that fills and empties.
    #[inline]
    fn emptied(&mut self) {
        self.buf.clear();
        self.start = 0;
    }

    /// Where element `i`'s frame begins, and the element's length.
    /// `i < count()`. Stepped to from the nearer end.
    #[inline]
    fn at(&self, i: usize) -> (usize, usize) {
        let n = self.count();
        debug_assert!(i < n);
        if i <= n / 2 {
            let mut at = self.start as usize;
            for _ in 0..i {
                at += read_len(&self.buf, at) + FRAME;
            }
            (at, read_len(&self.buf, at))
        } else {
            let mut end = self.buf.len();
            for _ in 0..n - 1 - i {
                end -= read_len(&self.buf, end - 2) + FRAME;
            }
            let len = read_len(&self.buf, end - 2);
            (end - len - FRAME, len)
        }
    }

    #[inline]
    fn get(&self, i: usize) -> &[u8] {
        let (at, len) = self.at(i);
        &self.buf[at + 2..at + 2 + len]
    }

    /// Make the `old` bytes at `at` into `new` bytes, moving what follows.
    fn resize_at(&mut self, at: usize, old: usize, new: usize) {
        let end = self.buf.len();
        if new > old {
            let more = new - old;
            if self.buf.capacity() - end < more {
                self.buf.reserve_exact(growth(more, self.live()));
            }
            self.buf.resize(end + more, 0);
            self.buf.copy_within(at + old..end, at + new);
        } else if new < old {
            self.buf.copy_within(at + old..end, at + new);
            self.buf.truncate(end - (old - new));
        }
    }

    /// Put `element` in element `i`'s place. `i < count()`.
    fn set(&mut self, i: usize, element: &[u8]) {
        let (at, len) = self.at(i);
        self.resize_at(at, len + FRAME, element.len() + FRAME);
        write_frame(&mut self.buf, at, element);
    }

    /// Put `element` before element `i`; after the last when `i ==
    /// count()`.
    fn insert(&mut self, i: usize, element: &[u8]) {
        debug_assert!(i <= self.count());
        if i == 0 {
            return self.push_front(element);
        }
        if i == self.count() {
            return self.push_back(element);
        }
        let (at, _) = self.at(i);
        self.resize_at(at, 0, element.len() + FRAME);
        write_frame(&mut self.buf, at, element);
        self.count += 1;
    }

    /// Take element `i` out. `i < count()`.
    #[allow(dead_code)] // the server takes elements out by value (`remove_matching`)
    fn remove(&mut self, i: usize) {
        if i == 0 {
            return self.drop_front();
        }
        if i + 1 == self.count() {
            return self.drop_back();
        }
        let (at, len) = self.at(i);
        self.resize_at(at, len + FRAME, 0);
        self.count -= 1;
    }

    /// Move the elements from `i` on into a chunk of their own.
    /// `0 < i < count()`.
    fn split_off(&mut self, i: usize) -> Chunk {
        debug_assert!(i > 0 && i < self.count());
        let (at, _) = self.at(i);
        let rest = Chunk {
            buf: self.buf[at..].to_vec(),
            start: 0,
            count: self.count - i as u16,
        };
        self.buf.truncate(at);
        self.count = i as u16;
        rest
    }

    /// Give back the room at both ends: for a chunk that has others on
    /// both sides, and so is not added to.
    fn tighten(&mut self) {
        let start = self.start as usize;
        if start > 0 {
            let live = self.live();
            self.buf.copy_within(start.., 0);
            self.buf.truncate(live);
            self.start = 0;
        }
        self.buf.shrink_to_fit();
    }

    /// The frames from element `i` on, with how many they are.
    #[inline]
    fn frames_from(&self, i: usize) -> (&[u8], usize) {
        if i >= self.count() {
            return (&[], 0);
        }
        let (at, _) = self.at(i);
        (&self.buf[at..], self.count() - i)
    }

    /// The frames of the first `n` elements, with `n`. `n <= count()`.
    #[inline]
    fn frames_to(&self, n: usize) -> (&[u8], usize) {
        let start = self.start as usize;
        if n == 0 {
            return (&[], 0);
        }
        if n == self.count() {
            return (&self.buf[start..], n);
        }
        let (at, _) = self.at(n);
        (&self.buf[start..at], n)
    }
}

#[derive(Clone)]
enum Chunks {
    One(Chunk),
    Many(VecDeque<Chunk>),
}

impl Default for Chunks {
    fn default() -> Self {
        Chunks::One(Chunk::default())
    }
}

#[derive(Clone, Default)]
pub struct PackedList {
    chunks: Chunks,
    len: usize,
    /// Whether some chunk between the first and the last is not full: the
    /// list then counts its way to a chunk.
    irregular: bool,
}

impl PackedList {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether an element of this length may be held at all.
    #[inline]
    pub fn takes(element: &[u8]) -> bool {
        element.len() <= MAX_LEN
    }

    /// Whether the list is a single chunk.
    #[inline]
    pub fn is_one_chunk(&self) -> bool {
        matches!(self.chunks, Chunks::One(_))
    }

    /// The bytes of the list's allocations.
    pub fn allocation_size(&self) -> usize {
        match &self.chunks {
            Chunks::One(chunk) => chunk.buf.capacity(),
            Chunks::Many(chunks) => {
                chunks.capacity() * std::mem::size_of::<Chunk>()
                    + chunks.iter().map(|c| c.buf.capacity()).sum::<usize>()
            }
        }
    }

    /// How many chunks the list is kept in.
    #[inline]
    pub fn chunk_count(&self) -> usize {
        match &self.chunks {
            Chunks::One(_) => 1,
            Chunks::Many(chunks) => chunks.len(),
        }
    }

    #[inline]
    fn chunk(&self, c: usize) -> &Chunk {
        match &self.chunks {
            Chunks::One(chunk) => {
                debug_assert_eq!(c, 0);
                chunk
            }
            Chunks::Many(chunks) => &chunks[c],
        }
    }

    #[inline]
    fn chunk_mut(&mut self, c: usize) -> &mut Chunk {
        match &mut self.chunks {
            Chunks::One(chunk) => {
                debug_assert_eq!(c, 0);
                chunk
            }
            Chunks::Many(chunks) => &mut chunks[c],
        }
    }

    /// The deque of chunks, the single chunk made its first.
    fn many(&mut self) -> &mut VecDeque<Chunk> {
        if let Chunks::One(chunk) = &mut self.chunks {
            // Room for two: as many as a list of up to 128 elements laid
            // down in order needs; a third chunk doubles it.
            let mut chunks = VecDeque::with_capacity(2);
            chunks.push_back(std::mem::take(chunk));
            self.chunks = Chunks::Many(chunks);
        }
        match &mut self.chunks {
            Chunks::Many(chunks) => chunks,
            Chunks::One(_) => unreachable!(),
        }
    }

    /// A new chunk after the last, with room for `bytes` of frames. The
    /// chunk that was last has one on each side from then on, unless it is
    /// also the first, and gives back its room.
    fn open_back(&mut self, bytes: usize) {
        let chunks = self.many();
        let last = chunks.len() - 1;
        if last > 0 {
            chunks[last].tighten();
        }
        chunks.push_back(Chunk {
            buf: Vec::with_capacity(bytes),
            start: 0,
            count: 0,
        });
    }

    /// A new chunk before the first, with room for `bytes` of frames in
    /// front.
    fn open_front(&mut self, bytes: usize) {
        let chunks = self.many();
        if chunks.len() > 1 {
            chunks[0].tighten();
        }
        chunks.push_front(Chunk {
            buf: vec![0; bytes],
            start: bytes as u32,
            count: 0,
        });
    }

    /// The frames' bytes of the elements of `elements` up to the first
    /// longer than [`MAX_LEN`] and no more than `most`, with how many.
    fn batch(elements: &[&[u8]], most: usize) -> (usize, usize) {
        let mut bytes = 0;
        for (k, element) in elements.iter().take(most).enumerate() {
            if element.len() > MAX_LEN {
                return (k, bytes);
            }
            bytes += element.len() + FRAME;
        }
        (elements.len().min(most), bytes)
    }

    /// Add `elements` after the last, in order, as that many
    /// [`push_back`](Self::push_back)s would, each chunk's buffer sized
    /// once for what goes into it. Returns how many were added: all, or
    /// those before the first longer than [`MAX_LEN`].
    pub fn extend_back(&mut self, elements: &[&[u8]]) -> usize {
        let mut done = 0;
        while done < elements.len() {
            let room = CHUNK_ENTRIES - self.chunk(self.chunk_count() - 1).count();
            let most = if room == 0 { CHUNK_ENTRIES } else { room };
            let (n, bytes) = Self::batch(&elements[done..], most);
            if n == 0 {
                break;
            }
            if room == 0 {
                self.open_back(bytes);
            }
            let last = self.chunk_count() - 1;
            let chunk = self.chunk_mut(last);
            chunk.reserve_back(bytes);
            for element in &elements[done..done + n] {
                chunk.push_back(element);
            }
            self.len += n;
            done += n;
            if n < most {
                // The elements ran out, or the next is too long.
                break;
            }
        }
        done
    }

    /// Add `elements` before the first, one after another (so the last of
    /// them ends up first), as that many [`push_front`](Self::push_front)s
    /// would. Returns how many were added.
    pub fn extend_front(&mut self, elements: &[&[u8]]) -> usize {
        let mut done = 0;
        while done < elements.len() {
            let room = CHUNK_ENTRIES - self.chunk(0).count();
            let most = if room == 0 { CHUNK_ENTRIES } else { room };
            let (n, bytes) = Self::batch(&elements[done..], most);
            if n == 0 {
                break;
            }
            if room == 0 {
                self.open_front(bytes);
            }
            let chunk = self.chunk_mut(0);
            chunk.reserve_front(bytes);
            for element in &elements[done..done + n] {
                chunk.push_front(element);
            }
            self.len += n;
            done += n;
            if n < most {
                break;
            }
        }
        done
    }

    /// Add `element` after the last. `false`, and nothing changed, if it is
    /// longer than [`MAX_LEN`].
    pub fn push_back(&mut self, element: &[u8]) -> bool {
        if element.len() > MAX_LEN {
            return false;
        }
        let last = self.chunk_count() - 1;
        if self.chunk(last).count() < CHUNK_ENTRIES {
            self.chunk_mut(last).push_back(element);
        } else {
            let chunks = self.many();
            // The chunk that was last has one on each side from now on,
            // unless it is also the first.
            if chunks.len() > 1 {
                chunks[last].tighten();
            }
            let mut chunk = Chunk::default();
            chunk.push_back(element);
            chunks.push_back(chunk);
        }
        self.len += 1;
        true
    }

    /// Add `element` before the first. `false`, and nothing changed, if it
    /// is longer than [`MAX_LEN`].
    pub fn push_front(&mut self, element: &[u8]) -> bool {
        if element.len() > MAX_LEN {
            return false;
        }
        if self.chunk(0).count() < CHUNK_ENTRIES {
            self.chunk_mut(0).push_front(element);
        } else {
            let chunks = self.many();
            if chunks.len() > 1 {
                chunks[0].tighten();
            }
            let mut chunk = Chunk::default();
            chunk.push_front(element);
            chunks.push_front(chunk);
        }
        self.len += 1;
        true
    }

    #[inline]
    pub fn front(&self) -> Option<&[u8]> {
        self.chunk(0).front()
    }

    #[inline]
    pub fn back(&self) -> Option<&[u8]> {
        self.chunk(self.chunk_count() - 1).back()
    }

    /// Take the first element off; nothing if the list is empty. Read it
    /// with [`front`](Self::front) first.
    pub fn drop_front(&mut self) {
        if self.len == 0 {
            return;
        }
        self.chunk_mut(0).drop_front();
        self.len -= 1;
        if self.chunk(0).count() == 0 {
            self.chunk_emptied(0);
        }
    }

    /// Take the last element off; nothing if the list is empty. Read it
    /// with [`back`](Self::back) first.
    pub fn drop_back(&mut self) {
        if self.len == 0 {
            return;
        }
        let last = self.chunk_count() - 1;
        self.chunk_mut(last).drop_back();
        self.len -= 1;
        if self.chunk(last).count() == 0 {
            self.chunk_emptied(last);
        }
    }

    /// Chunk `c` holds nothing: among others it goes, and a deque left with
    /// one chunk is that chunk again. A single chunk stays, empty.
    fn chunk_emptied(&mut self, c: usize) {
        let Chunks::Many(chunks) = &mut self.chunks else {
            return;
        };
        chunks.remove(c);
        if chunks.len() == 1 {
            let chunk = chunks.pop_front().unwrap_or_default();
            self.chunks = Chunks::One(chunk);
            self.irregular = false;
        } else if self.irregular {
            self.note_shape();
        }
    }

    /// Set `irregular` from the chunks as they are.
    fn note_shape(&mut self) {
        self.irregular = match &self.chunks {
            Chunks::One(_) => false,
            Chunks::Many(chunks) => chunks
                .iter()
                .skip(1)
                .take(chunks.len().saturating_sub(2))
                .any(|c| c.count() != CHUNK_ENTRIES),
        };
    }

    /// The chunk element `i` is in, and its place in that chunk.
    /// `i < len()`.
    #[inline]
    fn locate(&self, i: usize) -> (usize, usize) {
        debug_assert!(i < self.len);
        let chunks = match &self.chunks {
            Chunks::One(_) => return (0, i),
            Chunks::Many(chunks) => chunks,
        };
        let first = chunks[0].count();
        if i < first {
            return (0, i);
        }
        if !self.irregular {
            // Every chunk after the first holds `CHUNK_ENTRIES` but the
            // last, which holds the rest.
            let past = i - first;
            return (1 + past / CHUNK_ENTRIES, past % CHUNK_ENTRIES);
        }
        // The deque's two halves, walked as plain slices: a few
        // instructions a chunk.
        let (head, tail) = chunks.as_slices();
        if i < self.len / 2 {
            let mut before = 0;
            for (base, half) in [(0, head), (head.len(), tail)] {
                for (k, chunk) in half.iter().enumerate() {
                    let n = chunk.count();
                    if i < before + n {
                        return (base + k, i - before);
                    }
                    before += n;
                }
            }
        } else {
            let mut end = self.len;
            for (base, half) in [(head.len(), tail), (0, head)] {
                for (k, chunk) in half.iter().enumerate().rev() {
                    let begin = end - chunk.count();
                    if i >= begin {
                        return (base + k, i - begin);
                    }
                    end = begin;
                }
            }
        }
        unreachable!("element {i} of {} is in no chunk", self.len)
    }

    /// Element `i`, if there is one.
    #[inline]
    pub fn get(&self, i: usize) -> Option<&[u8]> {
        if i >= self.len {
            return None;
        }
        if i == 0 {
            return self.front();
        }
        if i + 1 == self.len {
            return self.back();
        }
        let (c, j) = self.locate(i);
        Some(self.chunk(c).get(j))
    }

    /// Put `element` in element `i`'s place. `false`, and nothing changed,
    /// if there is no element `i` or `element` is longer than [`MAX_LEN`].
    pub fn set(&mut self, i: usize, element: &[u8]) -> bool {
        if i >= self.len || element.len() > MAX_LEN {
            return false;
        }
        let (c, j) = self.locate(i);
        self.chunk_mut(c).set(j, element);
        true
    }

    /// Put `element` before element `i`; after the last when `i == len()`.
    /// `false`, and nothing changed, if `i > len()` or `element` is longer
    /// than [`MAX_LEN`].
    pub fn insert(&mut self, i: usize, element: &[u8]) -> bool {
        if i > self.len {
            return false;
        }
        if i == 0 {
            return self.push_front(element);
        }
        if i == self.len {
            return self.push_back(element);
        }
        if element.len() > MAX_LEN {
            return false;
        }
        let (c, j) = self.locate(i);
        if self.chunk(c).count() == CHUNK_ENTRIES {
            // Full: its second half goes into a chunk of its own, after it.
            let rest = self.chunk_mut(c).split_off(CHUNK_ENTRIES / 2);
            self.many().insert(c + 1, rest);
            if j <= CHUNK_ENTRIES / 2 {
                self.chunk_mut(c).insert(j, element);
            } else {
                self.chunk_mut(c + 1).insert(j - CHUNK_ENTRIES / 2, element);
            }
        } else {
            self.chunk_mut(c).insert(j, element);
        }
        self.len += 1;
        self.note_shape();
        true
    }

    /// Take element `i` out. `false` if there is none.
    #[allow(dead_code)] // the server takes elements out by value (`remove_matching`)
    pub fn remove(&mut self, i: usize) -> bool {
        if i >= self.len {
            return false;
        }
        let (c, j) = self.locate(i);
        self.chunk_mut(c).remove(j);
        self.len -= 1;
        if self.chunk(c).count() == 0 {
            self.chunk_emptied(c);
        }
        self.note_shape();
        true
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// `LREM`: take out the elements equal to `element`, `count` of them at
    /// most from the first on (`count > 0`), from the last back (`count <
    /// 0`), or all (`0`). Each chunk is gone through once and closed up
    /// once. Returns how many went.
    pub fn remove_matching(&mut self, element: &[u8], count: i64) -> usize {
        if element.len() > MAX_LEN {
            return 0;
        }
        let mut left = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let chunks = self.chunk_count();
        let mut removed = 0;
        for step in 0..chunks {
            if left == 0 {
                break;
            }
            let c = if count < 0 { chunks - 1 - step } else { step };
            let chunk = self.chunk_mut(c);
            let mask = chunk.matches(element, left, count < 0);
            if mask == 0 {
                continue;
            }
            let k = mask.count_ones() as usize;
            chunk.remove_mask(mask);
            removed += k;
            left -= k;
        }
        if removed > 0 {
            self.len -= removed;
            self.drop_empty_chunks();
        }
        removed
    }

    /// Chunks emptied in the middle go; the shape is noted again.
    fn drop_empty_chunks(&mut self) {
        let Chunks::Many(chunks) = &mut self.chunks else {
            return;
        };
        chunks.retain(|c| c.count() > 0);
        match chunks.len() {
            0 => self.chunks = Chunks::default(),
            1 => {
                let chunk = chunks.pop_front().unwrap_or_default();
                self.chunks = Chunks::One(chunk);
            }
            _ => {}
        }
        self.note_shape();
    }

    /// `LTRIM`: keep elements `from` up to, not including, `to`, and
    /// nothing else: whole chunks dropped, the two at the ends cut. `from <=
    /// to <= len()`.
    pub fn keep_range(&mut self, from: usize, to: usize) {
        debug_assert!(from <= to && to <= self.len);
        if from >= to {
            self.clear();
            return;
        }
        if from == 0 && to == self.len {
            return;
        }
        // A few off one end (a capped list's trim): popped there.
        if from == 0 && self.len - to <= CHUNK_ENTRIES {
            for _ in to..self.len {
                self.drop_back();
            }
            return;
        }
        if to == self.len && from <= CHUNK_ENTRIES {
            for _ in 0..from {
                self.drop_front();
            }
            return;
        }
        let (last, last_to) = self.locate(to - 1);
        let (first, first_from) = self.locate(from);
        match &mut self.chunks {
            Chunks::One(chunk) => chunk.keep(first_from, last_to + 1),
            Chunks::Many(chunks) => {
                chunks.truncate(last + 1);
                chunks.drain(..first);
                if chunks.len() == 1 {
                    let mut chunk = chunks.pop_front().unwrap_or_default();
                    chunk.keep(first_from, last_to + 1);
                    self.chunks = Chunks::One(chunk);
                } else {
                    let back = chunks.len() - 1;
                    chunks[back].keep(0, last_to + 1);
                    let n = chunks[0].count();
                    chunks[0].keep(first_from, n);
                }
            }
        }
        self.len = to - from;
        if self.irregular {
            self.note_shape();
        }
    }

    /// Hand `each` the elements from `from` up to, not including, `to`,
    /// first to last, while it says to go on (`true`); chunk by chunk, with
    /// no state kept between elements. `false` if it said to stop. `from <=
    /// to <= len()`.
    #[inline]
    pub fn for_each_in(&self, from: usize, to: usize, mut each: impl FnMut(&[u8]) -> bool) -> bool {
        debug_assert!(from <= to && to <= self.len);
        if from >= to {
            return true;
        }
        let (mut c, j) = self.locate(from);
        let mut left = to - from;
        let first = self.chunk(c);
        let n = (first.count() - j).min(left);
        if !first.each_from(j, n, &mut each) {
            return false;
        }
        left -= n;
        while left > 0 {
            c += 1;
            let chunk = self.chunk(c);
            let n = chunk.count().min(left);
            if !chunk.each_from(0, n, &mut each) {
                return false;
            }
            left -= n;
        }
        true
    }

    /// As [`for_each_in`](Self::for_each_in), last to first.
    #[inline]
    pub fn for_each_in_rev(
        &self,
        from: usize,
        to: usize,
        mut each: impl FnMut(&[u8]) -> bool,
    ) -> bool {
        debug_assert!(from <= to && to <= self.len);
        if from >= to {
            return true;
        }
        let (mut c, j) = self.locate(to - 1);
        let mut left = to - from;
        let n = (j + 1).min(left);
        if !self.chunk(c).each_back_from(j + 1, n, &mut each) {
            return false;
        }
        left -= n;
        while left > 0 {
            c -= 1;
            let chunk = self.chunk(c);
            let n = chunk.count().min(left);
            if !chunk.each_back_from(chunk.count(), n, &mut each) {
                return false;
            }
            left -= n;
        }
        true
    }

    /// The index of the first element equal to `element`.
    pub fn position(&self, element: &[u8]) -> Option<usize> {
        let mut i = 0;
        let found = !self.for_each_in(0, self.len, |e| {
            if e == element {
                return false;
            }
            i += 1;
            true
        });
        found.then_some(i)
    }

    /// The elements from `from` up to, not including, `to`, first to last.
    /// `from <= to <= len()`.
    pub fn range(&self, from: usize, to: usize) -> Iter<'_> {
        debug_assert!(from <= to && to <= self.len);
        if from >= to {
            return Iter {
                list: self,
                chunk: 0,
                frames: &[],
                in_chunk: 0,
                left: 0,
            };
        }
        let (chunk, j) = self.locate(from);
        let (frames, in_chunk) = self.chunk(chunk).frames_from(j);
        Iter {
            list: self,
            chunk,
            frames,
            in_chunk,
            left: to - from,
        }
    }

    /// Every element, first to last.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        self.range(0, self.len)
    }

    /// The elements from `from` up to, not including, `to`, last to first.
    /// `from <= to <= len()`.
    pub fn range_rev(&self, from: usize, to: usize) -> RevIter<'_> {
        debug_assert!(from <= to && to <= self.len);
        if from >= to {
            return RevIter {
                list: self,
                chunk: 0,
                frames: &[],
                in_chunk: 0,
                left: 0,
            };
        }
        let (chunk, j) = self.locate(to - 1);
        let (frames, in_chunk) = self.chunk(chunk).frames_to(j + 1);
        RevIter {
            list: self,
            chunk,
            frames,
            in_chunk,
            left: to - from,
        }
    }
}

/// A list laid down from its elements in order: full chunks, each buffer
/// exactly as long as its frames. For loads, where a list of millions of
/// elements would otherwise be that many pushes.
pub struct Builder {
    list: PackedList,
    /// The frames of the chunk being filled.
    frames: Vec<u8>,
    count: u16,
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            list: PackedList::new(),
            frames: Vec::new(),
            count: 0,
        }
    }

    /// Add `element` after the last. `false`, and nothing changed, if it is
    /// longer than [`MAX_LEN`].
    #[inline]
    pub fn push(&mut self, element: &[u8]) -> bool {
        if element.len() > MAX_LEN {
            return false;
        }
        let at = self.frames.len();
        self.frames.resize(at + element.len() + FRAME, 0);
        write_frame(&mut self.frames, at, element);
        self.count += 1;
        if self.count as usize == CHUNK_ENTRIES {
            self.close_chunk();
        }
        true
    }

    fn close_chunk(&mut self) {
        if self.count == 0 {
            return;
        }
        let chunk = Chunk {
            buf: self.frames.as_slice().to_vec(),
            start: 0,
            count: self.count,
        };
        self.list.len += self.count as usize;
        self.frames.clear();
        self.count = 0;
        if self.list.len == chunk.count() {
            self.list.chunks = Chunks::One(chunk);
        } else {
            self.list.many().push_back(chunk);
        }
    }

    pub fn finish(mut self) -> PackedList {
        self.close_chunk();
        self.list
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// Elements first to last.
pub struct Iter<'a> {
    list: &'a PackedList,
    /// The chunk `frames` is of.
    chunk: usize,
    /// The frames not yet given, of this chunk.
    frames: &'a [u8],
    /// How many elements `frames` holds.
    in_chunk: usize,
    /// How many elements are still to be given in all.
    left: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.left == 0 {
            return None;
        }
        while self.in_chunk == 0 {
            self.chunk += 1;
            let (frames, in_chunk) = self.list.chunk(self.chunk).frames_from(0);
            self.frames = frames;
            self.in_chunk = in_chunk;
        }
        let len = read_len(self.frames, 0);
        let (frame, rest) = self.frames.split_at(len + FRAME);
        self.frames = rest;
        self.in_chunk -= 1;
        self.left -= 1;
        Some(&frame[2..2 + len])
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for Iter<'_> {}

/// Elements last to first.
pub struct RevIter<'a> {
    list: &'a PackedList,
    chunk: usize,
    frames: &'a [u8],
    in_chunk: usize,
    left: usize,
}

impl<'a> Iterator for RevIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.left == 0 {
            return None;
        }
        while self.in_chunk == 0 {
            self.chunk -= 1;
            let chunk = self.list.chunk(self.chunk);
            let (frames, in_chunk) = chunk.frames_to(chunk.count());
            self.frames = frames;
            self.in_chunk = in_chunk;
        }
        let end = self.frames.len();
        let len = read_len(self.frames, end - 2);
        let (rest, frame) = self.frames.split_at(end - len - FRAME);
        self.frames = rest;
        self.in_chunk -= 1;
        self.left -= 1;
        Some(&frame[2..2 + len])
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for RevIter<'_> {}

impl std::fmt::Debug for PackedList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.iter().map(String::from_utf8_lossy))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element(i: usize) -> Vec<u8> {
        format!("element:{i:08}").into_bytes()
    }

    /// The list holds exactly `model`, read every way it can be read.
    fn assert_same(list: &PackedList, model: &VecDeque<Vec<u8>>) {
        assert_eq!(list.len(), model.len());
        assert_eq!(list.is_empty(), model.is_empty());
        let forward: Vec<&[u8]> = list.iter().collect();
        let expected: Vec<&[u8]> = model.iter().map(Vec::as_slice).collect();
        assert_eq!(forward, expected);
        let mut backward: Vec<&[u8]> = list.range_rev(0, list.len()).collect();
        backward.reverse();
        assert_eq!(backward, expected);
        assert_eq!(list.front(), model.front().map(Vec::as_slice));
        assert_eq!(list.back(), model.back().map(Vec::as_slice));
        // By index: every one of a short list, a spread of a long one.
        let step = (model.len() / 97).max(1);
        for i in (0..model.len()).step_by(step) {
            assert_eq!(list.get(i), Some(model[i].as_slice()), "element {i}");
        }
        assert_eq!(list.get(model.len()), None);
        // The chunks are what the list says they are.
        let counted: usize = (0..list.chunk_count()).map(|c| list.chunk(c).count()).sum();
        assert_eq!(counted, list.len());
        let middle_not_full = (1..list.chunk_count().saturating_sub(1))
            .any(|c| list.chunk(c).count() != CHUNK_ENTRIES);
        assert_eq!(list.irregular, middle_not_full);
        for c in 0..list.chunk_count() {
            let chunk = list.chunk(c);
            assert!(chunk.count() <= CHUNK_ENTRIES);
            assert!(
                list.chunk_count() == 1 || chunk.count() > 0,
                "an empty chunk is kept"
            );
        }
    }

    #[test]
    fn it_is_a_list() {
        let mut list = PackedList::new();
        assert!(list.is_empty());
        assert_eq!(list.front(), None);
        assert_eq!(list.back(), None);
        assert_eq!(list.get(0), None);
        assert_eq!(list.allocation_size(), 0);
        list.drop_front();
        list.drop_back();
        assert!(list.push_back(b"b"));
        assert!(list.push_front(b"a"));
        assert!(list.push_back(b""));
        assert_eq!(list.len(), 3);
        assert_eq!(list.iter().collect::<Vec<_>>(), [&b"a"[..], b"b", b""]);
        assert_eq!(list.front(), Some(&b"a"[..]));
        assert_eq!(list.back(), Some(&b""[..]));
        list.drop_back();
        list.drop_front();
        assert_eq!(list.iter().collect::<Vec<_>>(), [&b"b"[..]]);
        list.drop_front();
        assert!(list.is_empty());
        assert_eq!(list.front(), None);
        list.clear();
        assert_eq!(list.allocation_size(), 0);
    }

    #[test]
    fn what_is_too_long_is_refused_and_changes_nothing() {
        let long = vec![b'x'; MAX_LEN + 1];
        let longest = vec![b'x'; MAX_LEN];
        let mut list = PackedList::new();
        let mut model = VecDeque::new();
        assert!(!PackedList::takes(&long) && PackedList::takes(&longest));
        assert!(!list.push_back(&long));
        assert!(!list.push_front(&long));
        assert!(list.is_empty());
        assert!(list.push_back(&longest));
        model.push_back(longest.clone());
        assert!(list.push_back(b"short"));
        model.push_back(b"short".to_vec());
        assert!(!list.set(1, &long));
        assert!(!list.insert(1, &long));
        assert!(!list.set(2, b"x"));
        assert!(!list.insert(3, b"x"));
        assert!(!list.remove(2));
        assert_same(&list, &model);
    }

    #[test]
    fn it_grows_into_chunks_and_back_into_one() {
        let mut list = PackedList::new();
        let mut model = VecDeque::new();
        for i in 0..CHUNK_ENTRIES * 5 + 7 {
            assert!(list.push_back(&element(i)));
            model.push_back(element(i));
            if i % CHUNK_ENTRIES <= 1 {
                assert_same(&list, &model);
            }
        }
        assert_eq!(list.chunk_count(), 6);
        assert!(!list.irregular && !list.is_one_chunk());
        for i in 0..CHUNK_ENTRIES * 3 + 5 {
            assert!(list.push_front(&element(10_000 + i)));
            model.push_front(element(10_000 + i));
        }
        assert_same(&list, &model);
        assert!(!list.irregular);
        while model.len() > 3 {
            if model.len() % 3 == 0 {
                list.drop_front();
                model.pop_front();
            } else {
                list.drop_back();
                model.pop_back();
            }
            if model.len() % 50 == 0 {
                assert_same(&list, &model);
            }
        }
        assert_same(&list, &model);
        assert!(list.is_one_chunk());
    }

    #[test]
    fn a_queue_and_a_stack_do_not_grow_the_buffer_without_end() {
        // Pushed behind, popped in front, a few held at a time: the chunk's
        // bytes move up its buffer and are slid back, not grown for ever.
        let mut list = PackedList::new();
        for i in 0..100_000 {
            assert!(list.push_back(&element(i)));
            if i >= 5 {
                assert_eq!(list.front(), Some(element(i - 5).as_slice()));
                list.drop_front();
            }
        }
        assert_eq!(list.len(), 5);
        assert!(list.allocation_size() < 1024, "{}", list.allocation_size());
        // The same the other way round.
        let mut list = PackedList::new();
        for i in 0..100_000 {
            assert!(list.push_front(&element(i)));
            if i >= 5 {
                assert_eq!(list.back(), Some(element(i - 5).as_slice()));
                list.drop_back();
            }
        }
        assert_eq!(list.len(), 5);
        assert!(list.allocation_size() < 1024, "{}", list.allocation_size());
        // And a stack.
        let mut list = PackedList::new();
        for i in 0..100_000 {
            assert!(list.push_front(&element(i)));
            if i % 2 == 1 {
                list.drop_front();
            }
            if list.len() > 20 {
                list.drop_front();
            }
        }
        assert!(list.allocation_size() < 4096, "{}", list.allocation_size());
    }

    #[test]
    fn elements_are_set_inserted_and_removed_in_the_middle() {
        let mut list = PackedList::new();
        let mut model = VecDeque::new();
        for i in 0..CHUNK_ENTRIES * 4 {
            list.push_back(&element(i));
            model.push_back(element(i));
        }
        // Set: the same length, a shorter one, a longer one, in every chunk.
        for (i, value) in [
            (0, &b"first"[..]),
            (CHUNK_ENTRIES - 1, b"element:99999999"),
            (CHUNK_ENTRIES, b""),
            (
                CHUNK_ENTRIES * 2 + 17,
                b"a much longer element than the one that was there",
            ),
            (CHUNK_ENTRIES * 4 - 1, b"last"),
        ] {
            assert!(list.set(i, value));
            model[i] = value.to_vec();
            assert_same(&list, &model);
        }
        assert!(!list.irregular);
        // Insert into a full chunk in the middle: it splits.
        assert!(list.insert(CHUNK_ENTRIES + 3, b"inserted"));
        model.insert(CHUNK_ENTRIES + 3, b"inserted".to_vec());
        assert_same(&list, &model);
        assert!(list.irregular);
        assert!(list.insert(CHUNK_ENTRIES * 2 + 60, b"inserted again"));
        model.insert(CHUNK_ENTRIES * 2 + 60, b"inserted again".to_vec());
        assert_same(&list, &model);
        // Remove across the list, whole chunks among them.
        // The last and the middle as the list is at each removal.
        let picks: [fn(usize) -> usize; 5] =
            [|_| 0, |_| 1, |_| CHUNK_ENTRIES, |n| n - 1, |n| n / 2];
        for pick in picks {
            let i = pick(model.len());
            assert!(list.remove(i));
            model.remove(i);
            assert_same(&list, &model);
        }
        for _ in 0..CHUNK_ENTRIES + 10 {
            assert!(list.remove(CHUNK_ENTRIES / 2));
            model.remove(CHUNK_ENTRIES / 2);
        }
        assert_same(&list, &model);
        // An irregular list is still pushed to and popped from at its ends.
        for i in 0..CHUNK_ENTRIES * 2 {
            list.push_back(&element(i));
            model.push_back(element(i));
            list.push_front(&element(i));
            model.push_front(element(i));
        }
        assert_same(&list, &model);
    }

    #[test]
    fn ranges_are_read_both_ways() {
        let mut list = PackedList::new();
        let mut model = VecDeque::new();
        for i in 0..CHUNK_ENTRIES * 3 + 11 {
            list.push_back(&element(i));
            model.push_back(element(i));
        }
        let n = model.len();
        for (from, to) in [
            (0, 0),
            (0, 1),
            (0, n),
            (n - 1, n),
            (n, n),
            (5, CHUNK_ENTRIES),
            (CHUNK_ENTRIES - 1, CHUNK_ENTRIES + 1),
            (CHUNK_ENTRIES, CHUNK_ENTRIES * 2),
            (CHUNK_ENTRIES + 7, CHUNK_ENTRIES * 3 + 2),
            (n - 3, n),
        ] {
            let expected: Vec<&[u8]> = model.range(from..to).map(Vec::as_slice).collect();
            let forward: Vec<&[u8]> = list.range(from, to).collect();
            assert_eq!(forward, expected, "{from}..{to}");
            assert_eq!(list.range(from, to).len(), to - from);
            let mut backward: Vec<&[u8]> = list.range_rev(from, to).collect();
            backward.reverse();
            assert_eq!(backward, expected, "{from}..{to} backwards");
        }
    }

    #[test]
    fn it_matches_a_model_over_a_long_random_run() {
        // A fixed sequence (an xorshift), so a failure repeats.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut list = PackedList::new();
        let mut model: VecDeque<Vec<u8>> = VecDeque::new();
        for step in 0..60_000 {
            let r = next();
            let value = vec![b'a' + (r >> 8) as u8 % 26; ((r >> 16) % 40) as usize];
            // Phases: growing, shrinking, about level; so the list crosses
            // one chunk, many chunks and empty more than once.
            let bias = match (step / 6_000) % 3 {
                0 => 3,
                1 => 0,
                _ => 1,
            };
            match r % 16 {
                0..=2 => {
                    list.push_back(&value);
                    model.push_back(value);
                }
                3..=5 => {
                    list.push_front(&value);
                    model.push_front(value);
                }
                6..=8 if bias < 3 => {
                    assert_eq!(list.front(), model.front().map(Vec::as_slice));
                    list.drop_front();
                    model.pop_front();
                }
                9..=11 if bias < 2 => {
                    assert_eq!(list.back(), model.back().map(Vec::as_slice));
                    list.drop_back();
                    model.pop_back();
                }
                12 if !model.is_empty() => {
                    let i = (r >> 32) as usize % model.len();
                    assert!(list.set(i, &value));
                    model[i] = value;
                }
                13 => {
                    let i = (r >> 32) as usize % (model.len() + 1);
                    assert!(list.insert(i, &value));
                    model.insert(i, value);
                }
                14 if !model.is_empty() => {
                    let i = (r >> 32) as usize % model.len();
                    assert!(list.remove(i));
                    model.remove(i);
                }
                _ => {
                    if let Some(i) = (!model.is_empty()).then(|| (r >> 32) as usize % model.len()) {
                        assert_eq!(list.get(i), Some(model[i].as_slice()), "step {step}");
                    }
                }
            }
            if step % 257 == 0 {
                assert_same(&list, &model);
            }
        }
        assert_same(&list, &model);
    }

    #[test]
    fn the_blocks_are_a_few_bytes_an_element_over_the_elements_themselves() {
        let value = [b'v'; 32];
        let mut list = PackedList::new();
        for _ in 0..100_000 {
            list.push_back(&value);
        }
        // 36 bytes an element in its chunk; the chunks between the ends are
        // exact, and the deque of chunks is half a byte an element.
        let per_element = list.allocation_size() as f64 / 100_000.0;
        assert!(per_element < 38.0, "{per_element}");
        // A short list: its one buffer, a quarter over at most.
        let mut list = PackedList::new();
        for _ in 0..10 {
            list.push_back(&value);
        }
        assert!(
            list.allocation_size() <= 10 * 36 * 5 / 4 + 16,
            "{}",
            list.allocation_size()
        );
    }
}
