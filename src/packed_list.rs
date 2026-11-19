//! A list whose elements lie end to end in a few blocks of memory.
//!
//! A list as a `VecDeque<Arc<[u8]>>` pays, for every element, an 8-byte
//! slot, 16 bytes of `Arc` header and an allocation of its own: for a
//! 32-byte element, as much again as the element. Here elements are kept in
//! *chunks*: one buffer each, the elements in it one after another, each
//! between two copies of its length:
//!
//! ```text
//! [ len ][ element's bytes ][ len ]   [ len ][ bytes ][ len ]   ...
//! ```
//!
//! A length is one byte for an element shorter than 128 bytes, two for a
//! longer one (see `write_frame`): two bytes an element besides its own,
//! or four. Short elements are the common case of a list (job ids,
//! counters, small tokens), and for them the lengths were most of what the
//! element cost: a six-digit job id took ten bytes with two-byte lengths,
//! and takes eight. The length in front lets a chunk
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
/// An element shorter than this has one-byte lengths, a longer one
/// two-byte lengths (see [`write_frame`]).
const SHORT: usize = 128;
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

/// The bytes of each of the two lengths of an element of `len` bytes.
#[inline(always)]
fn hdr(len: usize) -> usize {
    1 + (len >= SHORT) as usize
}

/// The bytes of the frame of an element of `len` bytes: the element and
/// its two lengths.
#[inline(always)]
fn frame_len(len: usize) -> usize {
    len + 2 * hdr(len)
}

/// The length of the element whose frame begins at `at`.
#[inline(always)]
fn len_at(buf: &[u8], at: usize) -> usize {
    let b = buf[at] as usize;
    if b < 0x80 {
        b
    } else {
        ((b & 0x7f) << 8) | buf[at + 1] as usize
    }
}

/// The length of the element whose frame ends at `end`.
#[inline(always)]
fn len_before(buf: &[u8], end: usize) -> usize {
    let b = buf[end - 1] as usize;
    if b < 0x80 {
        b
    } else {
        ((b & 0x7f) << 8) | buf[end - 2] as usize
    }
}

/// What the walks over a chunk read of a frame: `MIXED` for a chunk that
/// may hold an element with two-byte lengths, else every length is one
/// byte and a frame is its element and two, as cheap to step over as the
/// fixed two-byte lengths were.
#[inline(always)]
fn len_fwd<const MIXED: bool>(buf: &[u8], at: usize) -> usize {
    if MIXED {
        len_at(buf, at)
    } else {
        buf[at] as usize
    }
}

#[inline(always)]
fn len_back<const MIXED: bool>(buf: &[u8], end: usize) -> usize {
    if MIXED {
        len_before(buf, end)
    } else {
        buf[end - 1] as usize
    }
}

#[inline(always)]
fn hdr_of<const MIXED: bool>(len: usize) -> usize {
    if MIXED { hdr(len) } else { 1 }
}

#[inline(always)]
fn frame_of<const MIXED: bool>(len: usize) -> usize {
    len + 2 * hdr_of::<MIXED>(len)
}

/// Write `element`'s frame at `at`; `at + frame_len(element.len()) <=
/// buf.len()`. An element shorter than [`SHORT`] is framed by one byte of
/// length on each side, a longer one by two: in front the high bits first,
/// with the top bit set, behind the low byte first, so that the byte next
/// to the element's outer edge says, from either side, which form it is.
#[inline]
fn write_frame(buf: &mut [u8], at: usize, element: &[u8]) {
    let n = element.len();
    if n < SHORT {
        buf[at] = n as u8;
        buf[at + 1..at + 1 + n].copy_from_slice(element);
        buf[at + 1 + n] = n as u8;
    } else {
        let (hi, lo) = (0x80 | (n >> 8) as u8, n as u8);
        buf[at] = hi;
        buf[at + 1] = lo;
        buf[at + 2..at + 2 + n].copy_from_slice(element);
        buf[at + 2 + n] = lo;
        buf[at + 3 + n] = hi;
    }
}

/// Up to [`CHUNK_ENTRIES`] elements in one buffer.
#[derive(Default)]
struct Chunk {
    buf: Vec<u8>,
    /// Where the first element's frame begins in `buf`.
    start: u32,
    /// Elements held.
    count: u16,
    /// Whether an element of [`SHORT`] bytes or more has been written since
    /// the chunk was last empty: the walks over a chunk without one read
    /// every length as one byte (`len_fwd`).
    mixed: bool,
}

/// A copy holds the frames, not the room before them (`COPY` of a queue
/// that has had much popped in front).
impl Clone for Chunk {
    fn clone(&self) -> Self {
        Chunk {
            buf: self.buf[self.start as usize..].to_vec(),
            start: 0,
            count: self.count,
            mixed: self.mixed,
        }
    }
}

/// A buffer is given back down to its frames when they take less than a
/// quarter of it and it has at least this much to give back.
const SHRINK_SLACK: usize = 1024;

impl Chunk {
    /// Give back the room of a buffer its frames take less than a quarter
    /// of: what `LTRIM` and `LREM` leave. Pops never do this, so a queue that
    /// fills and empties keeps its buffer.
    fn shrink_if_slack(&mut self) {
        let live = self.live();
        let capacity = self.buf.capacity();
        if capacity >= 4 * live && capacity - live >= SHRINK_SLACK {
            self.tighten();
        }
    }

    #[inline]
    fn count(&self) -> usize {
        self.count as usize
    }

    /// The bytes of the frames held.
    #[inline]
    fn live(&self) -> usize {
        self.buf.len() - self.start as usize
    }

    /// At least `need` bytes of room behind the frames.
    #[inline]
    fn room_back(&mut self, need: usize) {
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
    }

    fn push_back(&mut self, element: &[u8]) {
        let need = frame_len(element.len());
        self.room_back(need);
        let at = self.buf.len();
        self.buf.resize(at + need, 0);
        self.write(at, element);
        self.count += 1;
    }

    /// The last element's frame moved to the front, within the buffer.
    fn back_to_front(&mut self) {
        if self.count <= 1 {
            return;
        }
        let frame = frame_len(len_before(&self.buf, self.buf.len()));
        if (self.start as usize) < frame {
            self.grow_front(growth(frame, self.live()));
        }
        let end = self.buf.len();
        let at = self.start as usize - frame;
        self.buf.copy_within(end - frame..end, at);
        self.buf.truncate(end - frame);
        self.start = at as u32;
    }

    /// The first element's frame moved to the back, within the buffer.
    fn front_to_back(&mut self) {
        if self.count <= 1 {
            return;
        }
        let frame = frame_len(len_at(&self.buf, self.start as usize));
        self.room_back(frame);
        let start = self.start as usize;
        self.buf.extend_from_within(start..start + frame);
        self.start = (start + frame) as u32;
    }

    /// Take out the first element equal to `element` (from the back, the
    /// last), where it is found: a move of what follows it, or nothing at
    /// an end. `false` if there is none.
    fn remove_one(&mut self, element: &[u8], from_back: bool) -> bool {
        let want = element.len();
        if from_back {
            let mut end = self.buf.len();
            for _ in 0..self.count() {
                let len = len_before(&self.buf, end);
                let at = end - frame_len(len);
                let from = at + hdr(len);
                if len == want && &self.buf[from..from + len] == element {
                    self.cut(at, frame_len(len));
                    return true;
                }
                end = at;
            }
        } else {
            let mut at = self.start as usize;
            for _ in 0..self.count() {
                let len = len_at(&self.buf, at);
                let from = at + hdr(len);
                if len == want && &self.buf[from..from + len] == element {
                    self.cut(at, frame_len(len));
                    return true;
                }
                at += frame_len(len);
            }
        }
        false
    }

    /// Take out the frame of `frame` bytes at `at`.
    fn cut(&mut self, at: usize, frame: usize) {
        let end = self.buf.len();
        if at == self.start as usize {
            self.start = (at + frame) as u32;
        } else if at + frame == end {
            self.buf.truncate(at);
        } else {
            self.buf.copy_within(at + frame..end, at);
            self.buf.truncate(end - frame);
        }
        self.count -= 1;
        if self.count == 0 {
            self.emptied();
        }
    }

    /// `element`'s frame at `at`, as [`write_frame`] writes it; a long
    /// element marks the chunk [`mixed`](Self::mixed).
    #[inline(always)]
    fn write(&mut self, at: usize, element: &[u8]) {
        self.mixed |= element.len() >= SHORT;
        write_frame(&mut self.buf, at, element);
    }

    /// `next`'s elements after this chunk's: two neighbours that fit in one.
    fn append(&mut self, next: &Chunk) {
        let frames = &next.buf[next.start as usize..];
        self.reserve_back(frames.len());
        self.buf.extend_from_slice(frames);
        self.count += next.count;
        self.mixed |= next.mixed;
    }

    fn push_front(&mut self, element: &[u8]) {
        let need = frame_len(element.len());
        if (self.start as usize) < need {
            // Room in front: a quarter of what is held, at least.
            self.grow_front(growth(need, self.live()));
        }
        let at = self.start as usize - need;
        self.write(at, element);
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
                let len = len_before(&self.buf, end);
                let at = end - frame_len(len);
                let from = at + hdr(len);
                if len == element.len() && &self.buf[from..from + len] == element {
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
                let len = len_at(&self.buf, at);
                let from = at + hdr(len);
                if len == element.len() && &self.buf[from..from + len] == element {
                    mask |= 1 << j;
                    found += 1;
                    if found == limit {
                        break;
                    }
                }
                at += frame_len(len);
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
            let frame = frame_len(len_at(&self.buf, read));
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
        if self.mixed {
            self.each_from_in::<true>(i, n, each)
        } else {
            self.each_from_in::<false>(i, n, each)
        }
    }

    #[inline(always)]
    fn each_from_in<const MIXED: bool>(
        &self,
        i: usize,
        n: usize,
        each: &mut impl FnMut(&[u8]) -> bool,
    ) -> bool {
        let at = if i == 0 {
            self.start as usize
        } else {
            self.at_in::<MIXED>(i).0
        };
        let mut frames = &self.buf[at..];
        for _ in 0..n {
            let len = len_fwd::<MIXED>(frames, 0);
            let h = hdr_of::<MIXED>(len);
            let (frame, rest) = frames.split_at(len + 2 * h);
            if !each(&frame[h..h + len]) {
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
        if self.mixed {
            self.each_back_from_in::<true>(to, n, each)
        } else {
            self.each_back_from_in::<false>(to, n, each)
        }
    }

    #[inline(always)]
    fn each_back_from_in<const MIXED: bool>(
        &self,
        to: usize,
        n: usize,
        each: &mut impl FnMut(&[u8]) -> bool,
    ) -> bool {
        let end = if to == self.count() {
            self.buf.len()
        } else {
            self.at_in::<MIXED>(to).0
        };
        let mut frames = &self.buf[self.start as usize..end];
        for _ in 0..n {
            let e = frames.len();
            let len = len_back::<MIXED>(frames, e);
            let h = hdr_of::<MIXED>(len);
            let (rest, frame) = frames.split_at(e - len - 2 * h);
            if !each(&frame[h..h + len]) {
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
        let len = len_at(&self.buf, at);
        Some(&self.buf[at + hdr(len)..at + hdr(len) + len])
    }

    #[inline]
    fn back(&self) -> Option<&[u8]> {
        if self.count == 0 {
            return None;
        }
        let end = self.buf.len();
        let len = len_before(&self.buf, end);
        Some(&self.buf[end - hdr(len) - len..end - hdr(len)])
    }

    /// Take the first element off. `count() > 0`.
    #[inline]
    fn drop_front(&mut self) {
        debug_assert!(self.count > 0);
        let at = self.start as usize;
        let len = len_at(&self.buf, at);
        self.start = (at + frame_len(len)) as u32;
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
        let len = len_before(&self.buf, end);
        self.buf.truncate(end - frame_len(len));
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
        self.mixed = false;
    }

    /// Where element `i`'s frame begins, and the element's length.
    /// `i < count()`. Stepped to from the nearer end.
    #[inline]
    fn at(&self, i: usize) -> (usize, usize) {
        if self.mixed {
            self.at_in::<true>(i)
        } else {
            self.at_in::<false>(i)
        }
    }

    #[inline(always)]
    fn at_in<const MIXED: bool>(&self, i: usize) -> (usize, usize) {
        let n = self.count();
        debug_assert!(i < n);
        if i <= n / 2 {
            let mut at = self.start as usize;
            for _ in 0..i {
                at += frame_of::<MIXED>(len_fwd::<MIXED>(&self.buf, at));
            }
            (at, len_fwd::<MIXED>(&self.buf, at))
        } else {
            let mut end = self.buf.len();
            for _ in 0..n - 1 - i {
                end -= frame_of::<MIXED>(len_back::<MIXED>(&self.buf, end));
            }
            let len = len_back::<MIXED>(&self.buf, end);
            (end - frame_of::<MIXED>(len), len)
        }
    }

    #[inline]
    fn get(&self, i: usize) -> &[u8] {
        if self.mixed {
            let (at, len) = self.at_in::<true>(i);
            &self.buf[at + hdr(len)..at + hdr(len) + len]
        } else {
            let (at, len) = self.at_in::<false>(i);
            &self.buf[at + 1..at + 1 + len]
        }
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
        self.resize_at(at, frame_len(len), frame_len(element.len()));
        self.write(at, element);
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
        self.resize_at(at, 0, frame_len(element.len()));
        self.write(at, element);
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
        self.resize_at(at, frame_len(len), 0);
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
            mixed: self.mixed,
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
    Many(Box<Many>),
}

impl Default for Chunks {
    fn default() -> Self {
        Chunks::One(Chunk::default())
    }
}

/// What a list of several chunks keeps besides its chunks, and a list of
/// one does not need: behind a box, so that the list's own handle is no
/// larger than one chunk's.
#[derive(Clone)]
struct Many {
    chunks: VecDeque<Chunk>,
    len: usize,
    /// The run of chunks between the first and the last that may not be
    /// full: `lo` to `hi`, both included; none while `lo > hi`. Every chunk
    /// between the ends outside the run holds [`CHUNK_ENTRIES`], so an
    /// element before or after it is found by arithmetic, and only the
    /// chunks in it are counted.
    lo: u32,
    hi: u32,
}

impl Many {
    fn new(chunks: VecDeque<Chunk>, len: usize) -> Box<Many> {
        Box::new(Many {
            chunks,
            len,
            lo: 1,
            hi: 0,
        })
    }

    /// Whether every chunk between the ends is full.
    #[inline(always)]
    fn regular(&self) -> bool {
        self.lo > self.hi
    }

    /// Chunks `from` to `to` (both included) may no longer be full: into
    /// the run, those of them between the ends.
    fn widen(&mut self, from: usize, to: usize) {
        let n = self.chunks.len();
        if n < 3 {
            return;
        }
        let from = from.max(1);
        let to = to.min(n - 2);
        if from > to {
            return;
        }
        if self.regular() {
            self.lo = from as u32;
            self.hi = to as u32;
        } else {
            self.lo = self.lo.min(from as u32);
            self.hi = self.hi.max(to as u32);
        }
    }

    /// A chunk was put in at `at`: the chunks from there on move up one.
    fn inserted(&mut self, at: usize) {
        if self.regular() {
            return;
        }
        let at = at as u32;
        if self.lo >= at {
            self.lo += 1;
        }
        if self.hi >= at {
            self.hi += 1;
        }
    }

    /// The chunk at `at` was taken out: the chunks after it move down one.
    fn removed(&mut self, at: usize) {
        if self.regular() {
            return;
        }
        let at = at as u32;
        if self.lo > at {
            self.lo -= 1;
        }
        if self.hi >= at {
            self.hi = self.hi.saturating_sub(1);
        }
        self.clamp();
    }

    /// The run kept between the ends (a chunk that became an end leaves it).
    fn clamp(&mut self) {
        if self.regular() {
            return;
        }
        let last_middle = self.chunks.len().saturating_sub(2) as u32;
        self.lo = self.lo.max(1);
        self.hi = self.hi.min(last_middle);
        if self.lo > self.hi {
            self.lo = 1;
            self.hi = 0;
        }
    }

    /// Full chunks at the edges of the run leave it.
    fn tighten_run(&mut self) {
        while !self.regular() && self.chunks[self.lo as usize].count() == CHUNK_ENTRIES {
            self.lo += 1;
        }
        while !self.regular() && self.chunks[self.hi as usize].count() == CHUNK_ENTRIES {
            self.hi -= 1;
        }
        if self.regular() {
            self.lo = 1;
            self.hi = 0;
        }
    }

    /// The run as the chunks are: from the first to the last that is not
    /// full between the ends.
    fn recompute(&mut self) {
        self.lo = 1;
        self.hi = 0;
        let n = self.chunks.len();
        let middle = self.chunks.range(1..n.saturating_sub(1).max(1));
        for (k, chunk) in middle.enumerate() {
            if chunk.count() != CHUNK_ENTRIES {
                let c = k as u32 + 1;
                if self.regular() {
                    self.lo = c;
                }
                self.hi = c;
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct PackedList {
    chunks: Chunks,
}

impl PackedList {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.chunks {
            Chunks::One(chunk) => chunk.count(),
            Chunks::Many(m) => m.len,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    /// Whether some chunk between the first and the last may not be full.
    #[cfg(test)]
    pub fn irregular(&self) -> bool {
        match &self.chunks {
            Chunks::One(_) => false,
            Chunks::Many(m) => !m.regular(),
        }
    }

    /// The bytes of the list's allocations.
    pub fn allocation_size(&self) -> usize {
        match &self.chunks {
            Chunks::One(chunk) => chunk.buf.capacity(),
            Chunks::Many(m) => {
                std::mem::size_of::<Many>()
                    + m.chunks.capacity() * std::mem::size_of::<Chunk>()
                    + m.chunks.iter().map(|c| c.buf.capacity()).sum::<usize>()
            }
        }
    }

    /// How many chunks the list is kept in.
    #[inline]
    pub fn chunk_count(&self) -> usize {
        match &self.chunks {
            Chunks::One(_) => 1,
            Chunks::Many(m) => m.chunks.len(),
        }
    }

    #[inline]
    fn chunk(&self, c: usize) -> &Chunk {
        match &self.chunks {
            Chunks::One(chunk) => {
                debug_assert_eq!(c, 0);
                chunk
            }
            Chunks::Many(m) => &m.chunks[c],
        }
    }

    #[inline]
    fn chunk_mut(&mut self, c: usize) -> &mut Chunk {
        match &mut self.chunks {
            Chunks::One(chunk) => {
                debug_assert_eq!(c, 0);
                chunk
            }
            Chunks::Many(m) => &mut m.chunks[c],
        }
    }

    /// `n` elements more: a single chunk counts its own.
    #[inline(always)]
    fn grew(&mut self, n: usize) {
        if let Chunks::Many(m) = &mut self.chunks {
            m.len += n;
        }
    }

    #[inline(always)]
    fn shrank(&mut self, n: usize) {
        if let Chunks::Many(m) = &mut self.chunks {
            m.len -= n;
        }
    }

    /// The chunks, the single chunk made the first of several.
    fn many(&mut self) -> &mut Many {
        if let Chunks::One(chunk) = &mut self.chunks {
            // Room for two: as many as a list of up to 128 elements laid
            // down in order needs; a third chunk doubles it.
            let mut chunks = VecDeque::with_capacity(2);
            let len = chunk.count();
            chunks.push_back(std::mem::take(chunk));
            self.chunks = Chunks::Many(Many::new(chunks, len));
        }
        match &mut self.chunks {
            Chunks::Many(m) => m,
            Chunks::One(_) => unreachable!(),
        }
    }

    /// Back to a single chunk once a list of several is down to one; and,
    /// for a list that was much longer, the deque of chunks given back down
    /// to what it holds.
    fn settle(&mut self) {
        if let Chunks::Many(m) = &mut self.chunks {
            match m.chunks.len() {
                0 => self.chunks = Chunks::default(),
                1 => {
                    let chunk = m.chunks.pop_front().unwrap_or_default();
                    self.chunks = Chunks::One(chunk);
                }
                // A deque keeps its room when chunks leave it: a list of
                // 3,000,000 drained to 300,000 kept 2 MB of it, 7 bytes an
                // element left. Once it is under a quarter full it is given
                // back to twice what it holds, at once however much was cut
                // (`LTRIM` of most of a long list), so that a list going up
                // and down across the line does not move it every time.
                n if n < m.chunks.capacity() / 4 && m.chunks.capacity() > 8 => {
                    m.chunks.shrink_to((2 * n).max(8));
                }
                _ => {}
            }
        }
    }

    /// A new chunk after the last, with room for `bytes` of frames. The
    /// chunk that was last has one on each side from then on, unless it is
    /// also the first, and gives back its room.
    fn open_back(&mut self, bytes: usize) {
        let m = self.many();
        let last = m.chunks.len() - 1;
        if last > 0 {
            m.chunks[last].tighten();
        }
        m.chunks.push_back(Chunk {
            buf: Vec::with_capacity(bytes),
            start: 0,
            count: 0,
            mixed: false,
        });
    }

    /// A new chunk before the first, with room for `bytes` of frames in
    /// front.
    fn open_front(&mut self, bytes: usize) {
        let m = self.many();
        if m.chunks.len() > 1 {
            m.chunks[0].tighten();
        }
        m.chunks.push_front(Chunk {
            buf: vec![0; bytes],
            start: bytes as u32,
            count: 0,
            mixed: false,
        });
        m.inserted(0);
    }

    /// The frames' bytes of the elements of `elements` up to the first
    /// longer than [`MAX_LEN`] and no more than `most`, with how many.
    fn batch(elements: &[&[u8]], most: usize) -> (usize, usize) {
        let mut bytes = 0;
        for (k, element) in elements.iter().take(most).enumerate() {
            if element.len() > MAX_LEN {
                return (k, bytes);
            }
            bytes += frame_len(element.len());
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
            let room = CHUNK_ENTRIES - self.last().count();
            let most = if room == 0 { CHUNK_ENTRIES } else { room };
            let (n, bytes) = Self::batch(&elements[done..], most);
            if n == 0 {
                break;
            }
            if room == 0 {
                self.open_back(bytes);
            }
            let chunk = self.last_mut();
            chunk.reserve_back(bytes);
            for element in &elements[done..done + n] {
                chunk.push_back(element);
            }
            self.grew(n);
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
            let room = CHUNK_ENTRIES - self.first().count();
            let most = if room == 0 { CHUNK_ENTRIES } else { room };
            let (n, bytes) = Self::batch(&elements[done..], most);
            if n == 0 {
                break;
            }
            if room == 0 {
                self.open_front(bytes);
            }
            let chunk = self.first_mut();
            chunk.reserve_front(bytes);
            for element in &elements[done..done + n] {
                chunk.push_front(element);
            }
            self.grew(n);
            done += n;
            if n < most {
                break;
            }
        }
        done
    }

    /// The first chunk, found once.
    #[inline(always)]
    fn first(&self) -> &Chunk {
        match &self.chunks {
            Chunks::One(chunk) => chunk,
            Chunks::Many(m) => &m.chunks[0],
        }
    }

    /// The last chunk, found once.
    #[inline(always)]
    fn last(&self) -> &Chunk {
        match &self.chunks {
            Chunks::One(chunk) => chunk,
            Chunks::Many(m) => &m.chunks[m.chunks.len() - 1],
        }
    }

    #[inline(always)]
    fn first_mut(&mut self) -> &mut Chunk {
        match &mut self.chunks {
            Chunks::One(chunk) => chunk,
            Chunks::Many(m) => &mut m.chunks[0],
        }
    }

    #[inline(always)]
    fn last_mut(&mut self) -> &mut Chunk {
        match &mut self.chunks {
            Chunks::One(chunk) => chunk,
            Chunks::Many(m) => {
                let last = m.chunks.len() - 1;
                &mut m.chunks[last]
            }
        }
    }

    /// Add `element` after the last. `false`, and nothing changed, if it is
    /// longer than [`MAX_LEN`].
    pub fn push_back(&mut self, element: &[u8]) -> bool {
        if element.len() > MAX_LEN {
            return false;
        }
        let last = self.last_mut();
        if last.count() < CHUNK_ENTRIES {
            last.push_back(element);
        } else {
            // A list that filled a chunk at this end is growing here: the
            // new chunk gets room for a quarter of the full one, so that a
            // steady queue does not regrow it at every push.
            let room = frame_len(element.len()).max(last.live() / 4);
            self.open_back(room);
            self.last_mut().push_back(element);
        }
        self.grew(1);
        true
    }

    /// Add `element` before the first. `false`, and nothing changed, if it
    /// is longer than [`MAX_LEN`].
    pub fn push_front(&mut self, element: &[u8]) -> bool {
        if element.len() > MAX_LEN {
            return false;
        }
        let first = self.first_mut();
        if first.count() < CHUNK_ENTRIES {
            first.push_front(element);
        } else {
            let room = frame_len(element.len()).max(first.live() / 4);
            self.open_front(room);
            self.first_mut().push_front(element);
        }
        self.grew(1);
        true
    }

    #[inline]
    pub fn front(&self) -> Option<&[u8]> {
        self.first().front()
    }

    #[inline]
    pub fn back(&self) -> Option<&[u8]> {
        self.last().back()
    }

    /// Take the first element off; nothing if the list is empty. Read it
    /// with [`front`](Self::front) first.
    pub fn drop_front(&mut self) {
        if self.is_empty() {
            return;
        }
        let first = self.first_mut();
        first.drop_front();
        let emptied = first.count() == 0;
        self.shrank(1);
        if emptied {
            self.chunk_emptied(0);
        }
    }

    /// Take the last element off; nothing if the list is empty. Read it
    /// with [`back`](Self::back) first.
    pub fn drop_back(&mut self) {
        if self.is_empty() {
            return;
        }
        let last = self.last_mut();
        last.drop_back();
        let emptied = last.count() == 0;
        self.shrank(1);
        if emptied {
            self.chunk_emptied(self.chunk_count() - 1);
        }
    }

    /// Move the last element to the front (`from_back`), or the first to
    /// the back, handing it to `take` before it moves: `RPOPLPUSH k k`. A
    /// frame goes from one end of a chunk to the other, or from one end
    /// chunk into the other, copied once. `None` if the list is empty.
    pub fn rotate<R>(&mut self, from_back: bool, take: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let (taken, emptied) = match &mut self.chunks {
            Chunks::One(chunk) => {
                let element = if from_back {
                    chunk.back()?
                } else {
                    chunk.front()?
                };
                let taken = take(element);
                if from_back {
                    chunk.back_to_front();
                } else {
                    chunk.front_to_back();
                }
                return Some(taken);
            }
            Chunks::Many(m) => {
                let n = m.chunks.len();
                let mut ends = m.chunks.iter_mut();
                let (Some(first), Some(last)) = (ends.next(), ends.next_back()) else {
                    unreachable!("a list of several chunks has two ends")
                };
                let (from, to) = if from_back {
                    (last, first)
                } else {
                    (first, last)
                };
                let element = if from_back {
                    from.back()?
                } else {
                    from.front()?
                };
                let taken = take(element);
                if to.count() == CHUNK_ENTRIES {
                    // The other end is full: by way of a copy, rarely.
                    let element = element.to_vec();
                    if from_back {
                        self.drop_back();
                        self.push_front(&element);
                    } else {
                        self.drop_front();
                        self.push_back(&element);
                    }
                    return Some(taken);
                }
                if from_back {
                    to.push_front(element);
                    from.drop_back();
                } else {
                    to.push_back(element);
                    from.drop_front();
                }
                let emptied = (from.count() == 0).then_some(if from_back { n - 1 } else { 0 });
                (taken, emptied)
            }
        };
        if let Some(c) = emptied {
            self.chunk_emptied(c);
        }
        Some(taken)
    }

    /// Chunk `c`, at an end, holds nothing: among others it goes, and a
    /// list left with one chunk is that chunk again. A single chunk stays,
    /// empty.
    fn chunk_emptied(&mut self, c: usize) {
        let Chunks::Many(m) = &mut self.chunks else {
            return;
        };
        m.chunks.remove(c);
        m.removed(c);
        self.settle();
    }

    /// The chunk element `i` is in, and its place in that chunk.
    /// `i < len()`.
    #[inline]
    fn locate(&self, i: usize) -> (usize, usize) {
        let m = match &self.chunks {
            Chunks::One(_) => return (0, i),
            Chunks::Many(m) => m,
        };
        debug_assert!(i < m.len);
        let chunks = &m.chunks;
        let first = chunks[0].count();
        if i < first {
            return (0, i);
        }
        if m.regular() {
            // Every chunk after the first holds `CHUNK_ENTRIES` but the
            // last, which holds the rest.
            let past = i - first;
            return (1 + past / CHUNK_ENTRIES, past % CHUNK_ENTRIES);
        }
        let (lo, hi) = (m.lo as usize, m.hi as usize);
        // Before the run: full chunks from the second on.
        let run_begin = first + (lo - 1) * CHUNK_ENTRIES;
        if i < run_begin {
            let past = i - first;
            return (1 + past / CHUNK_ENTRIES, past % CHUNK_ENTRIES);
        }
        // The last chunk, and the full chunks before it, after the run.
        let n = chunks.len();
        let tail_begin = m.len - chunks[n - 1].count();
        if i >= tail_begin {
            return (n - 1, i - tail_begin);
        }
        let run_end = tail_begin - (n - 2 - hi) * CHUNK_ENTRIES;
        if i >= run_end {
            let past = i - run_end;
            return (hi + 1 + past / CHUNK_ENTRIES, past % CHUNK_ENTRIES);
        }
        // In the run: counted from its nearer edge, over the deque's two
        // halves as plain slices (a few instructions a chunk).
        let (head, tail) = chunks.as_slices();
        let split = head.len();
        // The run's part in each half, with the index of its first chunk.
        let parts = [
            (lo.min(split), &head[lo.min(split)..(hi + 1).min(split)]),
            (
                lo.max(split),
                &tail[lo.max(split) - split..(hi + 1).max(split) - split],
            ),
        ];
        if i - run_begin < run_end - i {
            let mut before = run_begin;
            for (base, part) in parts {
                for (k, chunk) in part.iter().enumerate() {
                    let count = chunk.count();
                    if i < before + count {
                        return (base + k, i - before);
                    }
                    before += count;
                }
            }
        } else {
            let mut end = run_end;
            for (base, part) in parts.into_iter().rev() {
                for (k, chunk) in part.iter().enumerate().rev() {
                    let begin = end - chunk.count();
                    if i >= begin {
                        return (base + k, i - begin);
                    }
                    end = begin;
                }
            }
        }
        unreachable!("element {i} of {} is in no chunk", m.len)
    }

    /// Element `i`, if there is one.
    #[inline]
    pub fn get(&self, i: usize) -> Option<&[u8]> {
        let len = self.len();
        if i >= len {
            return None;
        }
        if i == 0 {
            return self.front();
        }
        if i + 1 == len {
            return self.back();
        }
        let (c, j) = self.locate(i);
        Some(self.chunk(c).get(j))
    }

    /// Put `element` in element `i`'s place. `false`, and nothing changed,
    /// if there is no element `i` or `element` is longer than [`MAX_LEN`].
    pub fn set(&mut self, i: usize, element: &[u8]) -> bool {
        if i >= self.len() || element.len() > MAX_LEN {
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
        let len = self.len();
        if i > len {
            return false;
        }
        if i == 0 {
            return self.push_front(element);
        }
        if i == len {
            return self.push_back(element);
        }
        if element.len() > MAX_LEN {
            return false;
        }
        let (c, j) = self.locate(i);
        if self.chunk(c).count() == CHUNK_ENTRIES {
            // Full: its second half goes into a chunk of its own, after it.
            let rest = self.chunk_mut(c).split_off(CHUNK_ENTRIES / 2);
            let m = self.many();
            m.chunks.insert(c + 1, rest);
            m.inserted(c + 1);
            if j <= CHUNK_ENTRIES / 2 {
                m.chunks[c].insert(j, element);
            } else {
                m.chunks[c + 1].insert(j - CHUNK_ENTRIES / 2, element);
            }
            // Counted from before the split: a single chunk counted its own.
            m.len = len + 1;
            m.widen(c, c + 1);
        } else {
            self.chunk_mut(c).insert(j, element);
            self.grew(1);
            if let Chunks::Many(m) = &mut self.chunks {
                m.widen(c, c);
            }
        }
        true
    }

    /// Take element `i` out. `false` if there is none.
    #[allow(dead_code)] // the server takes elements out by value (`remove_matching`)
    pub fn remove(&mut self, i: usize) -> bool {
        if i >= self.len() {
            return false;
        }
        let (c, j) = self.locate(i);
        self.chunk_mut(c).remove(j);
        self.shrank(1);
        self.repair(c, c);
        true
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// `LREM`: take out the elements equal to `element`, `count` of them at
    /// most from the first on (`count > 0`), from the last back (`count <
    /// 0`), or all (`0`). Each chunk is gone through once and closed up
    /// once; the last one to take is taken where it is found. Returns how
    /// many went.
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
        // The chunks something was taken from.
        let (mut touched_lo, mut touched_hi) = (usize::MAX, 0);
        for step in 0..chunks {
            if left == 0 {
                break;
            }
            let c = if count < 0 { chunks - 1 - step } else { step };
            let chunk = self.chunk_mut(c);
            let k = if left == 1 {
                usize::from(chunk.remove_one(element, count < 0))
            } else {
                let mask = chunk.matches(element, left, count < 0);
                if mask != 0 {
                    chunk.remove_mask(mask);
                }
                mask.count_ones() as usize
            };
            if k > 0 {
                removed += k;
                left -= k;
                touched_lo = touched_lo.min(c);
                touched_hi = touched_hi.max(c);
            }
        }
        if removed > 0 {
            self.shrank(removed);
            self.repair(touched_lo, touched_hi);
        }
        removed
    }

    /// Chunks `from` to `to` lost elements: those emptied go, a chunk that
    /// fits into its neighbour with it is merged into it, and the run of
    /// chunks that may not be full takes in what changed and gives up
    /// full chunks at its edges.
    fn repair(&mut self, from: usize, to: usize) {
        let m = match &mut self.chunks {
            Chunks::One(chunk) => {
                chunk.shrink_if_slack();
                return;
            }
            Chunks::Many(m) => m,
        };
        if to - from < 2 {
            // One or two chunks changed (an `LREM` of a few): taken out or
            // merged where they are.
            m.widen(from, to);
            let mut to = to;
            let mut c = to + 1;
            while c > from {
                c -= 1;
                if m.chunks[c].count() == 0 {
                    #[cfg(test)]
                    work::deque_remove(m.chunks.len(), c);
                    m.chunks.remove(c);
                    m.removed(c);
                    to = to.saturating_sub(1);
                }
            }
            let mut c = from.saturating_sub(1);
            let mut end = (to + 1).min(m.chunks.len().saturating_sub(1));
            while c < end && c + 1 < m.chunks.len() {
                if m.chunks[c].count() + m.chunks[c + 1].count() <= CHUNK_ENTRIES {
                    #[cfg(test)]
                    work::deque_remove(m.chunks.len(), c + 1);
                    let next = m.chunks.remove(c + 1).unwrap_or_default();
                    m.chunks[c].append(&next);
                    m.removed(c + 1);
                    m.widen(c, c);
                    end -= 1;
                } else {
                    c += 1;
                }
            }
            if !m.chunks.is_empty() {
                let last = (to + 1).min(m.chunks.len() - 1);
                for chunk in m.chunks.range_mut(from.saturating_sub(1).min(last)..=last) {
                    chunk.shrink_if_slack();
                }
            }
            if m.chunks.len() > 1 {
                m.clamp();
                m.tighten_run();
            }
            self.settle();
            return;
        }
        // More: one pass over the chunks from just before the first changed to
        // just after the last: an emptied chunk is passed over, one that
        // fits into the chunk kept before it is appended to it, any other
        // is moved down into the next place; then the places left over go
        // at once. Linear in the chunks, however many changed (a dense
        // `LREM` over the whole list), where a removal from the deque at a
        // time was quadratic.
        let n = m.chunks.len();
        let first = from.saturating_sub(1);
        let last = (to + 1).min(n - 1);
        let mut write = first;
        #[cfg(test)]
        work::visits(last + 1 - first);
        for read in first..=last {
            let chunk = std::mem::take(&mut m.chunks[read]);
            if chunk.count() == 0 {
                continue;
            }
            if write > first && m.chunks[write - 1].count() + chunk.count() <= CHUNK_ENTRIES {
                m.chunks[write - 1].append(&chunk);
                continue;
            }
            m.chunks[write] = chunk;
            write += 1;
        }
        let gone = last + 1 - write;
        if gone > 0 {
            #[cfg(test)]
            work::visits(n - 1 - last);
            m.chunks.drain(write..=last);
        }
        for chunk in m.chunks.range_mut(first..write) {
            chunk.shrink_if_slack();
        }
        // The run, on the chunks as they are now: what was after the window
        // moved down by `gone`; the window's chunks may not be full.
        if !m.regular() {
            let to_now = |c: u32| -> u32 {
                let c = c as usize;
                let now = if c > last {
                    c - gone
                } else if c >= write {
                    write.saturating_sub(1)
                } else {
                    c
                };
                now as u32
            };
            m.lo = to_now(m.lo);
            m.hi = to_now(m.hi);
        }
        if write > first {
            m.widen(first, write - 1);
        }
        if m.chunks.len() > 1 {
            m.clamp();
            m.tighten_run();
        }
        self.settle();
    }

    /// `LTRIM`: keep elements `from` up to, not including, `to`, and
    /// nothing else: whole chunks dropped, the two at the ends cut. `from <=
    /// to <= len()`.
    pub fn keep_range(&mut self, from: usize, to: usize) {
        let len = self.len();
        debug_assert!(from <= to && to <= len);
        if from >= to {
            self.clear();
            return;
        }
        if from == 0 && to == len {
            return;
        }
        // A few off one end (a capped list's trim): popped there.
        if from == 0 && len - to <= CHUNK_ENTRIES {
            for _ in to..len {
                self.drop_back();
            }
            self.last_mut().shrink_if_slack();
            return;
        }
        if to == len && from <= CHUNK_ENTRIES {
            for _ in 0..from {
                self.drop_front();
            }
            self.first_mut().shrink_if_slack();
            return;
        }
        let (last, last_to) = self.locate(to - 1);
        let (first, first_from) = self.locate(from);
        match &mut self.chunks {
            Chunks::One(chunk) => chunk.keep(first_from, last_to + 1),
            Chunks::Many(m) => {
                m.chunks.truncate(last + 1);
                m.chunks.drain(..first);
                m.len = to - from;
                if m.chunks.len() == 1 {
                    m.chunks[0].keep(first_from, last_to + 1);
                } else {
                    let back = m.chunks.len() - 1;
                    m.chunks[back].keep(0, last_to + 1);
                    let n = m.chunks[0].count();
                    m.chunks[0].keep(first_from, n);
                    if !m.regular() {
                        m.recompute();
                    }
                }
            }
        }
        self.settle();
        self.first_mut().shrink_if_slack();
        self.last_mut().shrink_if_slack();
    }

    /// Hand `each` the elements from `from` up to, not including, `to`,
    /// first to last, while it says to go on (`true`); chunk by chunk, with
    /// no state kept between elements. `false` if it said to stop. `from <=
    /// to <= len()`.
    #[inline]
    pub fn for_each_in(&self, from: usize, to: usize, mut each: impl FnMut(&[u8]) -> bool) -> bool {
        debug_assert!(from <= to && to <= self.len());
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
        debug_assert!(from <= to && to <= self.len());
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
        let found = !self.for_each_in(0, self.len(), |e| {
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
        debug_assert!(from <= to && to <= self.len());
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
        self.range(0, self.len())
    }

    /// The elements from `from` up to, not including, `to`, last to first.
    /// `from <= to <= len()`.
    pub fn range_rev(&self, from: usize, to: usize) -> RevIter<'_> {
        debug_assert!(from <= to && to <= self.len());
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

    /// The run of chunks that may not be full, for tests.
    #[cfg(test)]
    fn run(&self) -> Option<(usize, usize)> {
        match &self.chunks {
            Chunks::Many(m) if !m.regular() => Some((m.lo as usize, m.hi as usize)),
            _ => None,
        }
    }

    /// Checks of the list's shape against its chunks, for tests: the
    /// counts add up, no chunk is empty or over full, every chunk between
    /// the ends outside the run is full, and `locate` finds the first and
    /// the last element of every chunk where a plain walk finds them; and a
    /// chunk not marked `mixed` holds no element of [`SHORT`] bytes or more,
    /// read with the decoding of both forms.
    #[cfg(test)]
    pub(crate) fn check_shape(&self) {
        for c in 0..self.chunk_count() {
            let chunk = self.chunk(c);
            let mut at = chunk.start as usize;
            for i in 0..chunk.count() {
                let len = len_at(&chunk.buf, at);
                assert!(
                    chunk.mixed || len < SHORT,
                    "element {i} of chunk {c} is {len} bytes in a chunk not marked mixed"
                );
                at += frame_len(len);
            }
            assert_eq!(at, chunk.buf.len(), "the frames of chunk {c}");
        }
        let Chunks::Many(m) = &self.chunks else {
            return;
        };
        let n = m.chunks.len();
        assert!(n >= 2, "a list of several chunks with {n}");
        assert_eq!(m.chunks.iter().map(Chunk::count).sum::<usize>(), m.len);
        assert!(
            m.chunks
                .iter()
                .all(|c| c.count() > 0 && c.count() <= CHUNK_ENTRIES),
            "an empty or over-full chunk"
        );
        if !m.regular() {
            assert!(
                m.lo >= 1 && (m.hi as usize) <= n - 2,
                "run {}..={} of {n}",
                m.lo,
                m.hi
            );
        }
        for c in 1..n - 1 {
            if m.chunks[c].count() != CHUNK_ENTRIES {
                assert!(
                    !m.regular() && (m.lo as usize) <= c && c <= m.hi as usize,
                    "chunk {c} of {n} holds {} outside the run {}..={}",
                    m.chunks[c].count(),
                    m.lo,
                    m.hi
                );
            }
        }
        let mut begin = 0;
        for (c, chunk) in m.chunks.iter().enumerate() {
            let count = chunk.count();
            assert_eq!(self.locate(begin), (c, 0), "first of chunk {c}");
            assert_eq!(
                self.locate(begin + count - 1),
                (c, count - 1),
                "last of chunk {c}"
            );
            if count > 2 {
                let mid = count / 2;
                assert_eq!(self.locate(begin + mid), (c, mid), "middle of chunk {c}");
            }
            begin += count;
        }
    }
}

/// A list laid down from its elements in order: full chunks, each buffer
/// exactly as long as its frames. For loads, where a list of millions of
/// elements would otherwise be that many pushes.
#[derive(Default)]
pub struct Builder {
    chunks: VecDeque<Chunk>,
    len: usize,
    /// The frames of the chunk being filled.
    frames: Vec<u8>,
    count: u16,
    /// Whether one of them has two-byte lengths (`Chunk::mixed`).
    mixed: bool,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `element` after the last. `false`, and nothing changed, if it is
    /// longer than [`MAX_LEN`].
    #[inline]
    pub fn push(&mut self, element: &[u8]) -> bool {
        if element.len() > MAX_LEN {
            return false;
        }
        let at = self.frames.len();
        self.frames.resize(at + frame_len(element.len()), 0);
        write_frame(&mut self.frames, at, element);
        self.mixed |= element.len() >= SHORT;
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
        self.chunks.push_back(Chunk {
            buf: self.frames.as_slice().to_vec(),
            start: 0,
            count: self.count,
            mixed: self.mixed,
        });
        self.mixed = false;
        self.len += self.count as usize;
        self.frames.clear();
        self.count = 0;
    }

    pub fn finish(mut self) -> PackedList {
        self.close_chunk();
        let mut list = PackedList {
            chunks: Chunks::Many(Many::new(self.chunks, self.len)),
        };
        list.settle();
        list
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
        let len = len_at(self.frames, 0);
        let (frame, rest) = self.frames.split_at(frame_len(len));
        self.frames = rest;
        self.in_chunk -= 1;
        self.left -= 1;
        Some(&frame[hdr(len)..hdr(len) + len])
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
        let len = len_before(self.frames, end);
        let (rest, frame) = self.frames.split_at(end - frame_len(len));
        self.frames = rest;
        self.in_chunk -= 1;
        self.left -= 1;
        Some(&frame[hdr(len)..hdr(len) + len])
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

/// What the chunk repair of `remove_matching` did, counted for the tests
/// (nothing of it is built outside them): chunk slots visited or moved. A
/// count, where a timing would depend on the machine and the build.
#[cfg(test)]
mod work {
    use std::cell::Cell;

    thread_local! {
        static SLOTS: Cell<usize> = const { Cell::new(0) };
    }

    pub fn reset() {
        SLOTS.with(|s| s.set(0));
    }

    pub fn slots() -> usize {
        SLOTS.with(Cell::get)
    }

    /// `n` chunk slots visited or moved.
    pub fn visits(n: usize) {
        SLOTS.with(|s| s.set(s.get() + n));
    }

    /// A `VecDeque::remove` at `at` of `len` slots: it moves the slots on
    /// the shorter side of `at`.
    pub fn deque_remove(len: usize, at: usize) {
        visits(at.min(len.saturating_sub(at + 1)));
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
        // The chunk walks LRANGE takes (one-byte lengths where a chunk is not
        // mixed): the whole list both ways, and ranges that begin and end
        // inside chunks.
        let n = model.len();
        let mut spans = vec![(0, n)];
        if n > 2 {
            spans.push((1, n - 1));
            spans.push((n / 3, n - n / 5));
        }
        if n > CHUNK_ENTRIES + 10 {
            spans.push((CHUNK_ENTRIES / 2 + 1, CHUNK_ENTRIES + 9));
        }
        for (from, to) in spans {
            let want: Vec<&[u8]> = model.range(from..to).map(Vec::as_slice).collect();
            let mut walked: Vec<Vec<u8>> = Vec::new();
            assert!(list.for_each_in(from, to, |e| {
                walked.push(e.to_vec());
                true
            }));
            assert_eq!(walked, want, "for_each_in {from}..{to}");
            let mut walked: Vec<Vec<u8>> = Vec::new();
            assert!(list.for_each_in_rev(from, to, |e| {
                walked.push(e.to_vec());
                true
            }));
            walked.reverse();
            assert_eq!(walked, want, "for_each_in_rev {from}..{to}");
        }
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
        assert!(!middle_not_full || list.irregular());
        list.check_shape();
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
        assert!(!list.irregular() && !list.is_one_chunk());
        for i in 0..CHUNK_ENTRIES * 3 + 5 {
            assert!(list.push_front(&element(10_000 + i)));
            model.push_front(element(10_000 + i));
        }
        assert_same(&list, &model);
        assert!(!list.irregular());
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
        assert!(!list.irregular());
        // Insert into a full chunk in the middle: it splits.
        assert!(list.insert(CHUNK_ENTRIES + 3, b"inserted"));
        model.insert(CHUNK_ENTRIES + 3, b"inserted".to_vec());
        assert_same(&list, &model);
        assert!(list.irregular());
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
            // Mostly short, one in eight with two-byte lengths or near the
            // edge between the forms (`SHORT`), a third of those right at
            // the edges of each form's byte values.
            let len = if (r >> 40) % 8 == 0 {
                if (r >> 20) % 3 == 0 {
                    [127, 128, 255, 256][(r >> 24) as usize % 4]
                } else {
                    100 + (r >> 16) % 200
                }
            } else {
                (r >> 16) % 40
            };
            let value = vec![b'a' + (r >> 8) as u8 % 26; len as usize];
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
                15 if (r >> 40) % 4 == 1 && !model.is_empty() => {
                    // LREM of an element there, with a count of either sign.
                    let target = model[(r >> 32) as usize % model.len()].clone();
                    let count = ((r >> 44) % 5) as i64 - 2;
                    let removed = list.remove_matching(&target, count);
                    let limit = if count == 0 {
                        usize::MAX
                    } else {
                        count.unsigned_abs() as usize
                    };
                    let mut want = 0;
                    if count >= 0 {
                        let mut i = 0;
                        while i < model.len() && want < limit {
                            if model[i] == target {
                                model.remove(i);
                                want += 1;
                            } else {
                                i += 1;
                            }
                        }
                    } else {
                        let mut i = model.len();
                        while i > 0 && want < limit {
                            i -= 1;
                            if model[i] == target {
                                model.remove(i);
                                want += 1;
                            }
                        }
                    }
                    assert_eq!(removed, want, "step {step}");
                    list.check_shape();
                }
                15 if (r >> 40) % 4 == 2 && !model.is_empty() && bias < 3 => {
                    // LTRIM of a little at either end, or of much.
                    let n = model.len();
                    let from = (r >> 44) as usize % (n / 8 + 1);
                    let to = (n - (r >> 52) as usize % (n / 8 + 1)).max(from);
                    list.keep_range(from, to);
                    model = model.range(from..to).cloned().collect();
                    list.check_shape();
                }
                15 if (r >> 40) % 4 == 3 => {
                    // A rotation either way.
                    let from_back = (r >> 44) & 1 == 1;
                    let got = list.rotate(from_back, |e| e.to_vec());
                    let e = if from_back {
                        model.pop_back()
                    } else {
                        model.pop_front()
                    };
                    assert_eq!(got, e, "step {step}");
                    if let Some(e) = e {
                        if from_back {
                            model.push_front(e);
                        } else {
                            model.push_back(e);
                        }
                    }
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
        // 34 bytes an element in its chunk; the chunks between the ends are
        // exact, and the deque of chunks is half a byte an element.
        let per_element = list.allocation_size() as f64 / 100_000.0;
        assert!(per_element < 35.0, "{per_element}");
        // A short list: its one buffer, a quarter over at most.
        let mut list = PackedList::new();
        for _ in 0..10 {
            list.push_back(&value);
        }
        assert!(
            list.allocation_size() <= 10 * 34 * 5 / 4 + 16,
            "{}",
            list.allocation_size()
        );
        // A job id of six digits: eight bytes, and the chunk's share.
        let mut list = PackedList::new();
        for i in 0..100_000 {
            list.push_back(format!("{}", 100_000 + i).as_bytes());
        }
        let per_element = list.allocation_size() as f64 / 100_000.0;
        assert!(per_element < 9.0, "{per_element}");
    }

    /// A list drained to a tenth gives back the deque's room with its
    /// chunks, at either end, and keeps it while it goes up and down a
    /// little.
    #[test]
    fn a_drained_list_gives_back_the_deque_of_chunks() {
        let value = [b'v'; 6];
        for from_back in [false, true] {
            let mut list = PackedList::new();
            for _ in 0..640_000 {
                list.push_front(&value);
            }
            let full = list.allocation_size() as f64 / 640_000.0;
            for _ in 0..576_000 {
                if from_back {
                    list.drop_back();
                } else {
                    list.drop_front();
                }
            }
            let drained = list.allocation_size() as f64 / 64_000.0;
            assert!(drained < full + 0.5, "{drained} {full}");
            let Chunks::Many(m) = &list.chunks else {
                panic!("one chunk")
            };
            let room = m.chunks.capacity();
            // Up and down by a few chunks: the room stays.
            for _ in 0..3 {
                for _ in 0..640 {
                    list.push_back(&value);
                }
                for _ in 0..640 {
                    list.drop_front();
                }
            }
            let Chunks::Many(m) = &list.chunks else {
                panic!("one chunk")
            };
            assert_eq!(m.chunks.capacity(), room);
        }
    }

    /// An `LTRIM` that keeps a tenth of a long list gives the deque of chunks
    /// back to about what is left in one go, not halved once; and the room
    /// left stays while the list goes up and down a little.
    #[test]
    fn a_big_trim_gives_back_the_deque_of_chunks_at_once() {
        let value = [b'v'; 6];
        let mut list = PackedList::new();
        let mut model: VecDeque<Vec<u8>> = VecDeque::new();
        for i in 0..640_000 {
            list.push_back(&value);
            if (100_000..164_000).contains(&i) {
                model.push_back(value.to_vec());
            }
        }
        let full = list.allocation_size() as f64 / 640_000.0;
        list.keep_range(100_000, 164_000);
        assert_same(&list, &model);
        let Chunks::Many(m) = &list.chunks else {
            panic!("one chunk")
        };
        let (n, room) = (m.chunks.len(), m.chunks.capacity());
        assert!(room >= n && room <= 2 * n + 8, "{room} for {n} chunks");
        let trimmed = list.allocation_size() as f64 / 64_000.0;
        assert!(trimmed < full + 0.5, "{trimmed} {full}");
        // MEMORY USAGE counts the room: exactly what is held.
        assert_eq!(
            list.allocation_size(),
            std::mem::size_of::<Many>()
                + room * std::mem::size_of::<Chunk>()
                + m.chunks.iter().map(|c| c.buf.capacity()).sum::<usize>()
        );
        // Up and down by a few chunks at either end: the room stays.
        for _ in 0..3 {
            for _ in 0..640 {
                list.push_back(&value);
                list.push_front(&value);
            }
            for _ in 0..640 {
                list.drop_front();
                list.drop_back();
            }
        }
        let Chunks::Many(m) = &list.chunks else {
            panic!("one chunk")
        };
        assert_eq!(m.chunks.capacity(), room);
        assert_same(&list, &model);
    }

    /// Lengths on both sides of the edge between one-byte and two-byte
    /// lengths, and at the ends of each form, read and changed every way.
    #[test]
    fn both_forms_of_length_are_read_from_either_side() {
        let lens = [0usize, 1, 2, 126, 127, 128, 129, 255, 256, 1000, MAX_LEN];
        for &n in &[
            1usize,
            5,
            CHUNK_ENTRIES,
            CHUNK_ENTRIES + 3,
            3 * CHUNK_ENTRIES,
        ] {
            let mut list = PackedList::new();
            let mut model: VecDeque<Vec<u8>> = VecDeque::new();
            for i in 0..n {
                let value = vec![b'a' + (i % 26) as u8; lens[i % lens.len()]];
                if i % 2 == 0 {
                    list.push_back(&value);
                    model.push_back(value);
                } else {
                    list.push_front(&value);
                    model.push_front(value);
                }
            }
            assert_same(&list, &model);
            // Each element given another length, of the other form.
            for i in 0..n {
                let value = vec![b'z'; lens[(i + 5) % lens.len()]];
                assert!(list.set(i, &value));
                model[i] = value;
            }
            assert_same(&list, &model);
            for _ in 0..n / 2 {
                assert_eq!(list.back(), model.back().map(Vec::as_slice));
                list.drop_back();
                model.pop_back();
                assert_eq!(list.front(), model.front().map(Vec::as_slice));
                list.drop_front();
                model.pop_front();
            }
            assert_same(&list, &model);
        }
    }

    #[test]
    fn a_rotation_moves_an_element_between_the_ends() {
        for n in [1usize, 2, 10, 64, 65, 128, 129, 300] {
            let mut list = PackedList::new();
            let mut model: VecDeque<Vec<u8>> = VecDeque::new();
            for i in 0..n {
                list.push_back(&element(i));
                model.push_back(element(i));
            }
            for step in 0..300 {
                let from_back = step % 3 != 0;
                let got = list.rotate(from_back, |e| e.to_vec());
                let e = if from_back {
                    model.pop_back()
                } else {
                    model.pop_front()
                };
                assert_eq!(got, e, "{n} {step}");
                let e = e.unwrap();
                if from_back {
                    model.push_front(e);
                } else {
                    model.push_back(e);
                }
                if step % 37 == 0 {
                    assert_same(&list, &model);
                }
            }
            assert_same(&list, &model);
        }
        assert_eq!(PackedList::new().rotate(true, |e| e.len()), None);
    }

    #[test]
    fn the_run_of_chunks_not_full_widens_and_shrinks() {
        let n = CHUNK_ENTRIES * 100;
        let mut list = PackedList::new();
        let mut model = VecDeque::new();
        for i in 0..n {
            list.push_back(&element(i));
            model.push_back(element(i));
        }
        assert_eq!(list.run(), None);
        let take = |list: &mut PackedList, model: &mut VecDeque<Vec<u8>>, i: usize| {
            assert_eq!(list.remove_matching(&element(i), 1), 1);
            let at = model.iter().position(|e| *e == element(i)).unwrap();
            model.remove(at);
        };
        // One removal in the middle: the run is its chunk.
        take(&mut list, &mut model, n / 2 + 5);
        assert_eq!(list.run(), Some((50, 50)));
        assert_same(&list, &model);
        // Removals spread over the list: from the first to the last.
        for k in 1..10 {
            take(&mut list, &mut model, k * n / 10 + 3);
        }
        let (lo, hi) = list.run().unwrap();
        assert!(lo == 10 && hi == 90, "{lo}..={hi}");
        assert_same(&list, &model);
        // Two neighbours taken down to half each become one full chunk, and
        // the run gives it up when it is at its edge.
        let mut list = PackedList::new();
        let mut model = VecDeque::new();
        for i in 0..CHUNK_ENTRIES * 10 {
            list.push_back(&element(i));
            model.push_back(element(i));
        }
        for i in 0..CHUNK_ENTRIES / 2 {
            take(&mut list, &mut model, 5 * CHUNK_ENTRIES + i);
        }
        assert_eq!(list.run(), Some((5, 5)));
        for i in 0..CHUNK_ENTRIES / 2 {
            take(&mut list, &mut model, 6 * CHUNK_ENTRIES + i);
        }
        assert_eq!(list.chunk_count(), 9);
        assert_eq!(list.run(), None);
        assert_same(&list, &model);
        // Removals that empty chunks at the ends and in the middle.
        for i in 0..CHUNK_ENTRIES * 10 {
            if i % 3 != 0 && model.iter().any(|e| *e == element(i)) {
                take(&mut list, &mut model, i);
            }
        }
        assert_same(&list, &model);
    }

    #[test]
    fn a_dense_lrem_is_one_pass_over_the_list() {
        // Every other element of a list of 20,000 chunks taken out: every
        // chunk falls to half, and neighbours merge. Counted, not timed: the
        // chunk slots the repair visits or moves (see `work`). One pass over
        // the chunks is a count about the chunks' own; removing emptied and
        // merged chunks from the deque one at a time moved slots in
        // proportion to the chunks squared (some 25 million here).
        let chunks = 20_000;
        let n = chunks * CHUNK_ENTRIES;
        let mut builder = Builder::new();
        for i in 0..n {
            builder.push(if i % 2 == 0 { b"x" } else { b"y" });
        }
        let mut list = builder.finish();
        assert_eq!(list.chunk_count(), chunks);
        work::reset();
        assert_eq!(list.remove_matching(b"x", 0), n / 2);
        let slots = work::slots();
        assert_eq!(list.len(), n / 2);
        assert_eq!(list.chunk_count(), n / 2 / CHUNK_ENTRIES);
        list.check_shape();
        assert!(
            slots > 0 && slots <= 2 * chunks,
            "the repair visited or moved {slots} chunk slots for {chunks} chunks"
        );
    }
}
