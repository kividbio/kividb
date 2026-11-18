//! A small sorted set in one block of memory.
//!
//! A sorted set of a few members does not need a hash table and a tree: the
//! two pay a separate allocation for every member (an `Arc<[u8]>` held by
//! both, 16 bytes of header before the member's bytes), a slot in the table
//! and a key in a tree node, and at ten members that is several times the
//! members themselves. Here the entries lie end to end in one buffer, in the
//! order of the set: by score, then by member, as Redis keeps a listpack.
//!
//! ```text
//! [ fingerprints ][ ends: u16 each ][ entry 0 | entry 1 | ... ]
//! entry: [ score: 1 to 9 bytes ][ the member's bytes ]
//! ```
//!
//! `ends[i]` is where entry `i` ends in the entries' bytes (and entry `i + 1`
//! begins), so entry `i` is reached in one step: a rank is an index, a range
//! of ranks a range of indexes, and a score is found by a binary search over
//! the entries. A member is found by scanning one byte an entry, its
//! *fingerprint* (as a packed set's, [`crate::packed_set`]), and comparing
//! only an entry whose fingerprint matches. Three bytes an entry besides its
//! score and its member.
//!
//! A score is written as its integer where it is one: in one byte up to
//! 247, in two to eight for the rest of the integers of up to 56 bits, and
//! as the eight bytes of the float otherwise (`-0`, fractions, infinities):
//! read back, it is the same float, bit for bit. Scores of a few digits are
//! the common case (ranks, counters, timestamps) and take one to five bytes.
//!
//! ```text
//! [ fingerprints: cap bytes ][ ends: cap x u16 ][ entries ... ]
//! ```
//!
//! Up to 16 entries the fingerprints and the ends have room for exactly the
//! entries there are (`cap == len`): a small set takes no byte it does not
//! use, and the few bytes of its entries move with every entry added or
//! taken out. Past 16 they have room for a multiple of 8, so an entry
//! added or taken out moves only the entries after it; the room is given
//! back once 16 places are spare. A score changed to one of the same width
//! moves the entry over only the entries between its old place and its new
//! one. The set refuses a member longer than [`MAX_LEN`] and an entry past
//! [`MAX_ENTRIES`]: whoever holds it then moves to a table and a tree.

use crate::packed_set::{fingerprint, same, scan_fingerprints};
use ordered_float::OrderedFloat;
use std::cmp::Ordering;

/// The most entries a block holds.
pub const MAX_ENTRIES: usize = 128;
/// The longest member a block holds.
pub const MAX_LEN: usize = 64;

/// Tags up to this one are the score itself, an integer from 0.
const LAST_SMALL: u8 = 0xF7;
/// The score as the eight bytes of the float.
const FLOAT: u8 = 0xFF;
/// The most bytes a score takes.
const MAX_SCORE_LEN: usize = 9;

/// A score as a block writes it: one to nine bytes.
#[derive(Clone, Copy)]
pub struct Score {
    bytes: [u8; 9],
    len: u8,
}

impl Score {
    #[inline]
    pub fn new(score: f64) -> Score {
        let mut bytes = [0u8; 9];
        // An integer, other than -0, of up to 56 bits: its own bytes. Any
        // `f64` with no fraction under 2^63 converts to `i64` and back to
        // the same float.
        let negative_zero = score == 0.0 && score.is_sign_negative();
        if score.fract() == 0.0 && score.abs() < 9.0e18 && !negative_zero {
            let v = score as i64;
            if (0..=LAST_SMALL as i64).contains(&v) {
                bytes[0] = v as u8;
                return Score { bytes, len: 1 };
            }
            // Bits of it, its sign one of them.
            let width = 65 - if v < 0 { !v } else { v }.leading_zeros();
            let k = width.div_ceil(8) as usize;
            if k <= 7 {
                bytes[0] = LAST_SMALL + k as u8;
                bytes[1..=k].copy_from_slice(&v.to_le_bytes()[..k]);
                return Score {
                    bytes,
                    len: 1 + k as u8,
                };
            }
        }
        bytes[0] = FLOAT;
        bytes[1..].copy_from_slice(&score.to_bits().to_le_bytes());
        Score { bytes, len: 9 }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

/// How many bytes the score that begins with `tag` takes.
#[inline(always)]
fn score_width(tag: u8) -> usize {
    if tag <= LAST_SMALL {
        1
    } else if tag == FLOAT {
        9
    } else {
        1 + (tag - LAST_SMALL) as usize
    }
}

/// The score an entry begins with.
#[inline(always)]
fn read_score(entry: &[u8]) -> f64 {
    let tag = entry[0];
    if tag <= LAST_SMALL {
        return tag as f64;
    }
    if tag == FLOAT {
        return f64::from_bits(u64::from_le_bytes(entry[1..9].try_into().unwrap()));
    }
    let k = (tag - LAST_SMALL) as usize;
    let mut bytes = [0u8; 8];
    bytes[..k].copy_from_slice(&entry[1..1 + k]);
    // Sign-extended from its top byte.
    let shift = 64 - 8 * k as u32;
    ((i64::from_le_bytes(bytes) << shift) >> shift) as f64
}

/// `(score, member)` against another, in the order of a sorted set: by
/// score (`-inf` lowest; NaN, never stored, highest), then by the member's
/// bytes.
#[inline(always)]
fn order(a: (f64, &[u8]), b: (f64, &[u8])) -> Ordering {
    OrderedFloat(a.0)
        .cmp(&OrderedFloat(b.0))
        .then_with(|| a.1.cmp(b.1))
}

#[derive(Clone, Default)]
pub struct PackedZset {
    buf: Vec<u8>,
    /// Entries held.
    len: u8,
    /// Entries there is room for in the fingerprints and the ends: see
    /// [`cap_for`].
    cap: u8,
}

/// The room in the fingerprints and the ends for `n` entries: exactly `n`
/// up to 16 (a small set takes no byte it does not use, and its bytes are
/// few to move), a multiple of 8 past that, so that adding or taking out
/// an entry moves only the entries after it, not every byte of the block.
#[inline]
fn cap_for(n: usize) -> usize {
    if n <= 16 { n } else { n.div_ceil(8) * 8 }
}

impl PackedZset {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The bytes of the set's one allocation.
    #[inline]
    pub fn allocation_size(&self) -> usize {
        self.buf.capacity()
    }

    /// Give back the room to spare.
    pub fn shrink_to_fit(&mut self) {
        self.buf.shrink_to_fit();
    }

    /// Where the entries' bytes begin.
    #[inline(always)]
    fn data_start(&self) -> usize {
        3 * self.cap as usize
    }

    /// The entries' bytes in all.
    #[inline]
    fn data_len(&self) -> usize {
        if self.len == 0 {
            0
        } else {
            self.bounds(self.len as usize - 1).1
        }
    }

    /// Lay the fingerprints, the ends and the entries out again for room
    /// for `cap` entries (`cap >= len()`); the buffer has room for what it
    /// grows by.
    fn relayout(&mut self, cap: usize) {
        let n = self.len();
        let old = self.cap as usize;
        debug_assert!(cap >= n && cap <= MAX_ENTRIES);
        if cap == old {
            return;
        }
        let data = self.data_len();
        let buf = &mut self.buf;
        if cap > old {
            buf.resize(3 * cap + data, 0);
            buf.copy_within(3 * old..3 * old + data, 3 * cap);
            buf.copy_within(old..old + 2 * n, cap);
        } else {
            buf.copy_within(old..old + 2 * n, cap);
            buf.copy_within(3 * old..3 * old + data, 3 * cap);
            buf.truncate(3 * cap + data);
        }
        self.cap = cap as u8;
    }

    /// Entry `i`'s first and last byte (past it), in the entries' bytes.
    #[inline(always)]
    fn bounds(&self, i: usize) -> (usize, usize) {
        let at = self.cap as usize + 2 * i;
        if i == 0 {
            (
                0,
                u16::from_le_bytes([self.buf[at], self.buf[at + 1]]) as usize,
            )
        } else {
            let both = u32::from_le_bytes(self.buf[at - 2..at + 2].try_into().unwrap());
            ((both & 0xffff) as usize, (both >> 16) as usize)
        }
    }

    /// Entry `i`: its score's bytes, then its member's.
    #[inline(always)]
    fn entry(&self, i: usize) -> &[u8] {
        let start = self.data_start();
        let (from, to) = self.bounds(i);
        &self.buf[start + from..start + to]
    }

    /// Entry `i`'s member and score. `i < len()`.
    #[inline]
    pub fn get(&self, i: usize) -> (&[u8], f64) {
        debug_assert!(i < self.len());
        let entry = self.entry(i);
        (&entry[score_width(entry[0])..], read_score(entry))
    }

    #[inline]
    pub fn score(&self, i: usize) -> f64 {
        read_score(self.entry(i))
    }

    /// Where `member` is, if it is a member.
    #[inline]
    pub fn find(&self, member: &[u8]) -> Option<usize> {
        if self.len == 0 || member.len() > MAX_LEN {
            return None;
        }
        self.find_by(member, fingerprint(member))
    }

    /// Where `member` is, or, if it is not a member, its fingerprint, for
    /// [`insert_new`](Self::insert_new): a write looks for a member and
    /// adds it with one fingerprint computed.
    #[inline]
    pub fn locate(&self, member: &[u8]) -> Result<usize, u8> {
        if member.len() > MAX_LEN {
            return Err(0);
        }
        let f = fingerprint(member);
        if self.len == 0 {
            return Err(f);
        }
        self.find_by(member, f).ok_or(f)
    }

    #[inline(always)]
    fn find_by(&self, member: &[u8], f: u8) -> Option<usize> {
        scan_fingerprints(&self.buf, self.len(), f, |at| {
            let entry = self.entry(at);
            let width = score_width(entry[0]);
            entry.len() - width == member.len() && same(&entry[width..], member)
        })
    }

    /// Entry `i` against `(score, member)`.
    #[inline(always)]
    fn order_at(&self, i: usize, score: f64, member: &[u8]) -> Ordering {
        let entry = self.entry(i);
        let width = score_width(entry[0]);
        order((read_score(entry), &entry[width..]), (score, member))
    }

    /// The first index whose entry is not below `(score, member)`; with
    /// `after`, the first whose entry is above it. A binary search.
    #[inline]
    pub fn lower_bound(&self, score: f64, member: &[u8], after: bool) -> usize {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let below = match self.order_at(mid, score, member) {
                Ordering::Less => true,
                Ordering::Equal => after,
                Ordering::Greater => false,
            };
            if below {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// [`insert_new`](Self::insert_new) into a block with no room to spare
    /// (`cap == len`) that has room for exactly one more after: the
    /// entries, the ends and the fingerprints each move once, from the
    /// right. `p` is where the entry goes in the entries' bytes, `data` all
    /// of them.
    fn insert_exact(
        &mut self,
        at: usize,
        p: usize,
        data: usize,
        score: &[u8],
        member: &[u8],
        f: u8,
    ) {
        let n = self.len();
        let e = score.len() + member.len();
        let old_start = 3 * n;
        let new_start = old_start + 3;
        let old_len = old_start + data;
        let more = 3 + e;
        if self.buf.capacity() - old_len < more {
            self.buf.reserve_exact(more.max(old_len / 4).max(32));
        }
        self.buf.resize(old_len + more, 0);
        let buf = &mut self.buf;
        // The entries after `at` by the new entry and three bytes, the ones
        // before it by three.
        buf.copy_within(old_start + p..old_len, new_start + p + e);
        buf.copy_within(old_start..old_start + p, new_start);
        buf[new_start + p..new_start + p + score.len()].copy_from_slice(score);
        buf[new_start + p + score.len()..new_start + p + e].copy_from_slice(member);
        // The ends after `at`, by three bytes and the new entry's length.
        buf.copy_within(n + 2 * at..3 * n, n + 2 * at + 3);
        for end in buf[n + 1 + 2 * (at + 1)..new_start].as_chunks_mut::<2>().0 {
            *end = (u16::from_le_bytes(*end) + e as u16).to_le_bytes();
        }
        // The ends before it, by one: the fingerprints have one more.
        buf.copy_within(n..n + 2 * at, n + 1);
        buf[n + 1 + 2 * at..n + 3 + 2 * at].copy_from_slice(&((p + e) as u16).to_le_bytes());
        buf.copy_within(at..n, at + 1);
        buf[at] = f;
        self.len += 1;
        self.cap += 1;
    }

    /// Add a member that is not one at index `at`, where it goes in the
    /// order (see [`lower_bound`](Self::lower_bound)), filed under the
    /// fingerprint `f` ([`locate`](Self::locate) gave it). The caller has
    /// made sure there is room: `len() < MAX_ENTRIES` and
    /// `member.len() <= MAX_LEN`.
    pub fn insert_new(&mut self, at: usize, member: &[u8], score: &Score, f: u8) {
        let n = self.len();
        assert!(n < MAX_ENTRIES && member.len() <= MAX_LEN && at <= n);
        let score = score.as_bytes();
        let e = score.len() + member.len();
        let data = self.data_len();
        let p = if at == 0 { 0 } else { self.bounds(at - 1).1 };
        let old_len = self.buf.len();
        debug_assert_eq!(old_len, self.data_start() + data);
        if n == self.cap as usize && cap_for(n + 1) == n + 1 {
            // A block with no room to spare that stays exact: everything
            // moves once.
            self.insert_exact(at, p, data, score, member, f);
            return;
        }
        let cap = if n == self.cap as usize {
            cap_for(n + 1)
        } else {
            self.cap as usize
        };
        // A quarter over what is there at most, not the doubling a `Vec`
        // does by itself: the block is the whole of a small set's memory.
        let more = 3 * (cap - self.cap as usize) + e;
        if self.buf.capacity() - old_len < more {
            self.buf.reserve_exact(more.max(old_len / 4).max(32));
        }
        self.relayout(cap);
        let start = 3 * cap;
        let buf = &mut self.buf;
        buf.resize(start + data + e, 0);
        // The entries after `at` move up by the new entry; it goes between.
        buf.copy_within(start + p..start + data, start + p + e);
        buf[start + p..start + p + score.len()].copy_from_slice(score);
        buf[start + p + score.len()..start + p + e].copy_from_slice(member);
        // The ends after `at` move up a place, by the new entry's length.
        buf.copy_within(cap + 2 * at..cap + 2 * n, cap + 2 * at + 2);
        for end in buf[cap + 2 * (at + 1)..cap + 2 * (n + 1)]
            .as_chunks_mut::<2>()
            .0
        {
            *end = (u16::from_le_bytes(*end) + e as u16).to_le_bytes();
        }
        buf[cap + 2 * at..cap + 2 * at + 2].copy_from_slice(&((p + e) as u16).to_le_bytes());
        buf.copy_within(at..n, at + 1);
        buf[at] = f;
        self.len += 1;
    }

    /// Take entries `from..to` out; the others keep their order.
    pub fn remove_range(&mut self, from: usize, to: usize) {
        let n = self.len();
        assert!(from <= to && to <= n);
        let k = to - from;
        if k == 0 {
            return;
        }
        let cap = self.cap as usize;
        let start = 3 * cap;
        let data = self.data_len();
        let p = if from == 0 {
            0
        } else {
            self.bounds(from - 1).1
        };
        let q = self.bounds(to - 1).1;
        let gone = q - p;
        let left = n - k;
        if left == 0 {
            self.buf = Vec::new();
            self.len = 0;
            self.cap = 0;
            return;
        }
        if cap == n && cap_for(left) == left {
            // No room to spare before or after: everything moves once,
            // from the left.
            let new_start = 3 * left;
            let buf = &mut self.buf;
            buf.copy_within(to..n, from);
            buf.copy_within(n..n + 2 * from, left);
            buf.copy_within(n + 2 * to..3 * n, left + 2 * from);
            for end in buf[left + 2 * from..new_start].as_chunks_mut::<2>().0 {
                *end = (u16::from_le_bytes(*end) - gone as u16).to_le_bytes();
            }
            buf.copy_within(start..start + p, new_start);
            buf.copy_within(start + q..start + data, new_start + p);
            buf.truncate(new_start + data - gone);
            self.len = left as u8;
            self.cap = left as u8;
            self.give_back();
            return;
        }
        let buf = &mut self.buf;
        // What comes after the entries taken out moves down: the
        // fingerprints and the ends by their places, the entries by their
        // bytes.
        buf.copy_within(to..n, from);
        buf.copy_within(cap + 2 * to..cap + 2 * n, cap + 2 * from);
        for end in buf[cap + 2 * from..cap + 2 * left].as_chunks_mut::<2>().0 {
            *end = (u16::from_le_bytes(*end) - gone as u16).to_le_bytes();
        }
        buf.copy_within(start + q..start + data, start + p);
        buf.truncate(start + data - gone);
        self.len = left as u8;
        // Room given back: all of it for a small set; past 16, once 16
        // places are spare (a set that takes one out and puts one back
        // moves nothing more).
        let want = cap_for(left);
        if want < cap && (left <= 16 || cap - left >= 16) {
            self.relayout(want);
        }
        self.give_back();
    }

    /// The buffer's room to spare given back once it is more than what the
    /// block holds (and at least 64 bytes): down to a quarter over, so a
    /// set that takes an entry out and puts one back, as a queue does,
    /// moves its buffer only when it has shrunk to half.
    #[inline]
    fn give_back(&mut self) {
        let len = self.buf.len();
        let spare = self.buf.capacity() - len;
        if spare > len && spare >= 64 {
            self.buf.shrink_to(len + len / 4);
        }
    }

    /// Take entry `i` out.
    #[inline]
    pub fn remove_at(&mut self, i: usize) {
        self.remove_range(i, i + 1);
    }

    /// Give entry `i` the score `score`: it moves to where that puts it.
    pub fn set_score(&mut self, i: usize, score: f64) {
        let n = self.len();
        debug_assert!(i < n);
        let encoded = Score::new(score);
        let (from, to) = self.bounds(i);
        let start = self.data_start();
        let width = score_width(self.buf[start + from]);
        let member_len = to - from - width;
        let stays = {
            let member = &self.buf[start + from + width..start + to];
            (i == 0 || self.order_at(i - 1, score, member) == Ordering::Less)
                && (i + 1 == n || self.order_at(i + 1, score, member) == Ordering::Greater)
        };
        if stays && encoded.len as usize == width {
            // In its place, the same length: only the score's bytes.
            self.buf[start + from..start + from + width].copy_from_slice(encoded.as_bytes());
            return;
        }
        let mut entry = [0u8; MAX_SCORE_LEN + MAX_LEN];
        let e = encoded.len as usize + member_len;
        entry[..encoded.len as usize].copy_from_slice(encoded.as_bytes());
        entry[encoded.len as usize..e].copy_from_slice(&self.buf[start + from + width..start + to]);
        let member_at = encoded.len as usize;
        let f = self.buf[i];
        if encoded.len as usize != width {
            // Another length: out, and in where it goes.
            let member = &entry[member_at..e];
            self.remove_at(i);
            let at = self.lower_bound(score, member, false);
            self.insert_new(at, member, &encoded, f);
            return;
        }
        // The same length: the entries between its old place and its new
        // one move over by it, and it goes into the room they leave.
        let j = {
            let at = self.lower_bound(score, &entry[member_at..e], false);
            if at > i { at - 1 } else { at }
        };
        let cap = self.cap as usize;
        let end_of = |set: &Self, k: usize| set.bounds(k).1;
        if j > i {
            let to_end = end_of(self, j);
            let buf = &mut self.buf;
            buf.copy_within(start + from + e..start + to_end, start + from);
            buf[start + to_end - e..start + to_end].copy_from_slice(&entry[..e]);
            for k in i..j {
                let at = cap + 2 * k;
                let next = u16::from_le_bytes([buf[at + 2], buf[at + 3]]) - e as u16;
                buf[at..at + 2].copy_from_slice(&next.to_le_bytes());
            }
            buf.copy_within(i + 1..=j, i);
            buf[j] = f;
        } else {
            let from_j = if j == 0 { 0 } else { end_of(self, j - 1) };
            let buf = &mut self.buf;
            buf.copy_within(start + from_j..start + from, start + from_j + e);
            buf[start + from_j..start + from_j + e].copy_from_slice(&entry[..e]);
            for k in (j + 1..=i).rev() {
                let at = cap + 2 * k;
                let before = u16::from_le_bytes([buf[at - 2], buf[at - 1]]) + e as u16;
                buf[at..at + 2].copy_from_slice(&before.to_le_bytes());
            }
            buf[cap + 2 * j..cap + 2 * j + 2].copy_from_slice(&((from_j + e) as u16).to_le_bytes());
            buf.copy_within(j..i, j + 1);
            buf[j] = f;
        }
    }

    /// Keep the entries `keep` says to; how many went.
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8], f64) -> bool) -> usize {
        let n = self.len();
        let kept: Vec<usize> = (0..n)
            .filter(|&i| {
                let (member, score) = self.get(i);
                keep(member, score)
            })
            .collect();
        if kept.len() == n {
            return 0;
        }
        let mut out = PackedZset::new();
        if !kept.is_empty() {
            let cap = cap_for(kept.len());
            let data: usize = kept.iter().map(|&i| self.entry(i).len()).sum();
            let mut buf = Vec::with_capacity(3 * cap + data);
            buf.resize(3 * cap, 0);
            for (j, &i) in kept.iter().enumerate() {
                buf[j] = self.buf[i];
                buf.extend_from_slice(self.entry(i));
                let end = (buf.len() - 3 * cap) as u16;
                buf[cap + 2 * j..cap + 2 * j + 2].copy_from_slice(&end.to_le_bytes());
            }
            out.buf = buf;
            out.len = kept.len() as u8;
            out.cap = cap as u8;
        }
        *self = out;
        n - kept.len()
    }

    /// A block of `entries`, already in the order of a sorted set, each
    /// above the one before it (no member twice), with no more than
    /// [`MAX_ENTRIES`] of them and no member longer than [`MAX_LEN`]. Sized
    /// exactly.
    pub fn from_sorted(entries: &[(&[u8], f64)]) -> Self {
        let n = entries.len();
        assert!(n <= MAX_ENTRIES);
        if n == 0 {
            return Self::new();
        }
        let scores: Vec<Score> = entries
            .iter()
            .map(|&(_, score)| Score::new(score))
            .collect();
        let data: usize = entries
            .iter()
            .zip(&scores)
            .map(|((member, _), score)| score.len as usize + member.len())
            .sum();
        let cap = cap_for(n);
        let mut buf = Vec::with_capacity(3 * cap + data);
        buf.resize(3 * cap, 0);
        for (i, ((member, _), score)) in entries.iter().zip(&scores).enumerate() {
            assert!(member.len() <= MAX_LEN);
            debug_assert!(
                i == 0
                    || order(
                        (entries[i - 1].1, entries[i - 1].0),
                        (entries[i].1, *member)
                    ) == Ordering::Less
            );
            buf[i] = fingerprint(member);
            buf.extend_from_slice(score.as_bytes());
            buf.extend_from_slice(member);
            let end = (buf.len() - 3 * cap) as u16;
            buf[cap + 2 * i..cap + 2 * i + 2].copy_from_slice(&end.to_le_bytes());
        }
        Self {
            buf,
            len: n as u8,
            cap: cap as u8,
        }
    }

    /// Whether a member is in the block twice (a block built from entries
    /// in order, by score, need not have its members apart): each member
    /// is looked for, and found first where it is.
    pub fn has_twice(&self) -> bool {
        (0..self.len()).any(|i| {
            let entry = self.entry(i);
            let member = &entry[score_width(entry[0])..];
            self.find_by(member, self.buf[i]) != Some(i)
        })
    }

    /// Entries `from..to`, in order, from either end.
    #[inline]
    pub fn range(&self, from: usize, to: usize) -> Range<'_> {
        debug_assert!(from <= to && to <= self.len());
        Range {
            set: self,
            front: from,
            back: to,
        }
    }

    /// Every entry, in order.
    #[inline]
    pub fn iter(&self) -> Range<'_> {
        self.range(0, self.len())
    }

    /// Whether the block is as it must be: the ends rise, the fingerprints
    /// are the members', the entries are in order with no member twice, and
    /// the buffer holds them and nothing more. For tests.
    #[cfg(test)]
    pub fn check(&self) {
        let n = self.len();
        if n == 0 {
            assert!(self.buf.is_empty() && self.cap == 0);
            return;
        }
        let mut last = 0;
        for i in 0..n {
            let (from, to) = self.bounds(i);
            assert_eq!(
                from,
                last,
                "entry {i} starts where {} ended",
                i.wrapping_sub(1)
            );
            assert!(to > from, "entry {i} is empty");
            last = to;
            let (member, score) = self.get(i);
            assert!(member.len() <= MAX_LEN);
            assert_eq!(self.buf[i], fingerprint(member), "fingerprint {i}");
            assert_eq!(
                Score::new(score).as_bytes(),
                &self.entry(i)[..score_width(self.entry(i)[0])]
            );
            if i > 0 {
                let (before, before_score) = self.get(i - 1);
                assert_eq!(
                    order((before_score, before), (score, member)),
                    Ordering::Less,
                    "order at {i}"
                );
            }
        }
        let cap = self.cap as usize;
        assert_eq!(self.buf.len(), 3 * cap + last);
        // Exact up to 16; past that a multiple of 8, with fewer than 16
        // places spare.
        if n <= 16 {
            assert_eq!(cap, n, "room of a small block");
        } else {
            assert!(
                cap.is_multiple_of(8) && cap >= n && cap - n < 16 + 8,
                "room {cap} for {n}"
            );
        }
    }
}

/// Entries of a block, in order, from either end.
#[derive(Clone)]
pub struct Range<'a> {
    set: &'a PackedZset,
    front: usize,
    back: usize,
}

impl<'a> Iterator for Range<'a> {
    type Item = (&'a [u8], f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            let entry = self.set.get(self.front);
            self.front += 1;
            Some(entry)
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.back - self.front;
        (left, Some(left))
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.front = self.front.saturating_add(n).min(self.back);
        self.next()
    }

    #[inline]
    fn count(self) -> usize {
        self.back - self.front
    }
}

impl DoubleEndedIterator for Range<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            self.back -= 1;
            Some(self.set.get(self.back))
        } else {
            None
        }
    }

    #[inline]
    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.back = self.back.saturating_sub(n).max(self.front);
        self.next_back()
    }
}

impl ExactSizeIterator for Range<'_> {}

impl std::fmt::Debug for PackedZset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.iter()
                    .map(|(member, score)| (String::from_utf8_lossy(member), score)),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The model: entries in order, (score, member).
    type Model = BTreeSet<(OrderedFloat<f64>, Vec<u8>)>;

    fn add(set: &mut PackedZset, member: &[u8], score: f64) {
        match set.locate(member) {
            Ok(i) => set.set_score(i, score),
            Err(f) => {
                let at = set.lower_bound(score, member, false);
                set.insert_new(at, member, &Score::new(score), f);
            }
        }
    }

    fn same_as(set: &PackedZset, model: &Model) {
        set.check();
        let held: Vec<(OrderedFloat<f64>, Vec<u8>)> = set
            .iter()
            .map(|(member, score)| (OrderedFloat(score), member.to_vec()))
            .collect();
        let want: Vec<(OrderedFloat<f64>, Vec<u8>)> = model.iter().cloned().collect();
        assert_eq!(held, want);
    }

    #[test]
    fn scores_are_kept_bit_for_bit() {
        let edges = [
            0.0,
            1.0,
            247.0,
            248.0,
            255.0,
            256.0,
            -1.0,
            -128.0,
            -129.0,
            65535.0,
            -65536.0,
            1_700_000_000.0,
            -1_700_000_000_123.0,
            (1u64 << 55) as f64 - 1.0,
            -((1u64 << 55) as f64),
            (1u64 << 55) as f64,
            (1u64 << 62) as f64,
            9.0e18,
            -9.0e18,
            113_000_000_000_001_000.0,
            0.5,
            -0.25,
            1.0e-300,
            5e-324,
            -0.0,
            f64::MAX,
            f64::MIN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            std::f64::consts::PI,
        ];
        for score in edges {
            let encoded = Score::new(score);
            let bytes = encoded.as_bytes();
            assert_eq!(score_width(bytes[0]), bytes.len(), "{score}");
            let read = read_score(bytes);
            assert_eq!(
                read.to_bits(),
                score.to_bits(),
                "{score} came back as {read}"
            );
        }
        // The short forms are short.
        assert_eq!(Score::new(7.0).as_bytes().len(), 1);
        assert_eq!(Score::new(-1.0).as_bytes().len(), 2);
        assert_eq!(Score::new(1_700_000_000.0).as_bytes().len(), 5);
        assert_eq!(Score::new(-0.0).as_bytes().len(), 9);
        assert_eq!(Score::new(0.5).as_bytes().len(), 9);
        // Every integer around each width's edges.
        for k in 1..64u32 {
            for d in [-2i64, -1, 0, 1, 2] {
                for sign in [1i64, -1] {
                    let v = sign * ((1i64 << (k - 1)) + d);
                    let score = v as f64;
                    assert_eq!(
                        read_score(Score::new(score).as_bytes()).to_bits(),
                        score.to_bits(),
                        "{v}"
                    );
                }
            }
        }
    }

    #[test]
    fn it_is_a_sorted_set() {
        let mut set = PackedZset::new();
        let mut model = Model::new();
        assert!(set.is_empty() && set.find(b"a").is_none());
        let pairs: [(&[u8], f64); 5] = [
            (b"b", 2.0),
            (b"a", 2.0),
            (b"", 1.0),
            (b"c", -0.5),
            (b"\xff\x00", 1e300),
        ];
        for (member, score) in pairs {
            add(&mut set, member, score);
            model.insert((OrderedFloat(score), member.to_vec()));
            same_as(&set, &model);
        }
        assert_eq!(set.get(0), (&b"c"[..], -0.5));
        assert_eq!(set.find(b""), Some(1));
        assert_eq!(set.find(b"a"), Some(2));
        assert_eq!(set.find(b"b"), Some(3));
        assert_eq!(set.find(b"z"), None);
        // A score that moves an entry from one end to the other, and back.
        add(&mut set, b"c", 1e301);
        model.remove(&(OrderedFloat(-0.5), b"c".to_vec()));
        model.insert((OrderedFloat(1e301), b"c".to_vec()));
        same_as(&set, &model);
        add(&mut set, b"c", f64::NEG_INFINITY);
        model.remove(&(OrderedFloat(1e301), b"c".to_vec()));
        model.insert((OrderedFloat(f64::NEG_INFINITY), b"c".to_vec()));
        same_as(&set, &model);
        while !set.is_empty() {
            let (member, score) = set.get(set.len() / 2);
            model.remove(&(OrderedFloat(score), member.to_vec()));
            set.remove_at(set.len() / 2);
            same_as(&set, &model);
        }
        assert_eq!(set.allocation_size(), 0);
    }

    #[test]
    fn the_longest_members_fit_at_the_limit() {
        // 128 members of 64 bytes, float scores: the last end is 9,344.
        let mut set = PackedZset::new();
        let mut model = Model::new();
        for i in 0..MAX_ENTRIES {
            let mut m = vec![b'a' + (i % 26) as u8; MAX_LEN];
            m[..8].copy_from_slice(format!("{i:08}").as_bytes());
            let score = (i as f64 * 7.3) % 50.0 + 0.1;
            add(&mut set, &m, score);
            model.insert((OrderedFloat(score), m));
        }
        same_as(&set, &model);
        for (score, m) in &model {
            let i = set.find(m).unwrap();
            assert_eq!(set.score(i), score.0);
        }
    }

    #[test]
    fn ranges_and_bounds_agree_with_a_walk() {
        let mut set = PackedZset::new();
        for i in 0..100u32 {
            let score = (i % 10) as f64 - 4.5;
            add(
                &mut set,
                format!("m{:03}", (i * 37) % 100).as_bytes(),
                score,
            );
        }
        set.check();
        let all: Vec<(Vec<u8>, f64)> = set.iter().map(|(m, s)| (m.to_vec(), s)).collect();
        for probe in [-10.0, -4.5, -1.5, 0.0, 0.5, 4.5, 10.0] {
            for after in [false, true] {
                for member in [&b""[..], b"m050", b"zzz"] {
                    let walk = all
                        .iter()
                        .position(|(m, s)| {
                            let o = order((*s, &m[..]), (probe, member));
                            if after {
                                o == Ordering::Greater
                            } else {
                                o != Ordering::Less
                            }
                        })
                        .unwrap_or(all.len());
                    assert_eq!(set.lower_bound(probe, member, after), walk);
                }
            }
        }
        // From both ends, skipping.
        let mut r = set.range(10, 90);
        assert_eq!(r.len(), 80);
        assert_eq!(r.nth(5).unwrap().0, &all[15].0[..]);
        assert_eq!(r.nth_back(5).unwrap().0, &all[84].0[..]);
        assert_eq!(r.len(), 68);
        assert_eq!(set.range(3, 3).next(), None);
    }

    #[test]
    fn ranges_of_entries_are_taken_out_in_one_go() {
        for (from, to) in [
            (0, 1),
            (0, 10),
            (5, 20),
            (90, 100),
            (0, 100),
            (99, 100),
            (40, 40),
        ] {
            let mut set = PackedZset::new();
            let mut all = Vec::new();
            for i in 0..100 {
                let m = format!("member{i:03}").into_bytes();
                add(&mut set, &m, i as f64 * 1.5);
                all.push((m, i as f64 * 1.5));
            }
            set.remove_range(from, to);
            all.drain(from..to);
            set.check();
            let held: Vec<(Vec<u8>, f64)> = set.iter().map(|(m, s)| (m.to_vec(), s)).collect();
            assert_eq!(held, all, "{from}..{to}");
            for (m, _) in &all {
                assert!(set.find(m).is_some());
            }
        }
    }

    #[test]
    fn retain_and_from_sorted_make_the_same_blocks() {
        let mut set = PackedZset::new();
        for i in 0..60 {
            add(&mut set, format!("m{i}").as_bytes(), (i % 7) as f64);
        }
        let gone = set.retain(|m, s| s != 3.0 && m.len() == 3);
        set.check();
        let held: Vec<(Vec<u8>, f64)> = set.iter().map(|(m, s)| (m.to_vec(), s)).collect();
        assert_eq!(gone, 60 - held.len());
        assert!(held.iter().all(|(m, s)| *s != 3.0 && m.len() == 3));
        let pairs: Vec<(&[u8], f64)> = held.iter().map(|(m, s)| (&m[..], *s)).collect();
        let built = PackedZset::from_sorted(&pairs);
        built.check();
        assert_eq!(built.buf, set.buf);
        assert_eq!(built.allocation_size(), built.buf.len());
        assert_eq!(set.retain(|_, _| false), held.len());
        assert!(set.is_empty());
        set.check();
    }

    #[test]
    fn it_agrees_with_a_model_through_every_change() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut set = PackedZset::new();
        let mut model = Model::new();
        let mut scores = std::collections::HashMap::<Vec<u8>, f64>::new();
        for step in 0..40_000 {
            let r = next();
            let id = (r >> 8) % 200;
            let len = (id as usize * 7) % (MAX_LEN + 1);
            let mut m = format!("{id}").into_bytes();
            m.resize(len.max(m.len()).min(MAX_LEN), b'.');
            let score = match (r >> 32) % 6 {
                0 => ((r >> 40) % 300) as f64,
                1 => -(((r >> 40) % 70_000) as f64),
                2 => ((r >> 40) % 1000) as f64 / 8.0,
                3 => [f64::INFINITY, f64::NEG_INFINITY, 0.0, 1e300, -1e-300]
                    [((r >> 40) % 5) as usize],
                _ => ((r >> 40) % 5) as f64,
            };
            match r % 5 {
                0..=2 => {
                    if let Some(old) = scores.get(&m) {
                        model.remove(&(OrderedFloat(*old), m.clone()));
                    } else if model.len() == MAX_ENTRIES {
                        continue;
                    }
                    add(&mut set, &m, score);
                    model.insert((OrderedFloat(score), m.clone()));
                    scores.insert(m, score);
                }
                3 => {
                    let i = set.find(&m);
                    assert_eq!(i.is_some(), scores.contains_key(&m), "step {step}");
                    if let Some(i) = i {
                        set.remove_at(i);
                        let old = scores.remove(&m).unwrap();
                        model.remove(&(OrderedFloat(old), m));
                    }
                }
                _ => {
                    let i = set.find(&m);
                    assert_eq!(
                        i.map(|i| set.score(i)),
                        scores.get(&m).copied(),
                        "step {step}"
                    );
                    if let Some(i) = i {
                        // Its rank is its place in the model.
                        let rank = model.range(..(OrderedFloat(scores[&m]), m.clone())).count();
                        assert_eq!(i, rank, "step {step}");
                    }
                }
            }
            assert_eq!(set.len(), model.len(), "step {step}");
            if step % 97 == 0 {
                same_as(&set, &model);
            }
        }
    }

    #[test]
    fn room_is_given_back_as_entries_go_but_not_at_every_pop() {
        let mut set = PackedZset::new();
        for i in 0..MAX_ENTRIES {
            let mut m = vec![b'm'; MAX_LEN];
            m[..8].copy_from_slice(format!("{i:08}").as_bytes());
            add(&mut set, &m, i as f64);
        }
        assert!(set.allocation_size() > 9_000);
        // A queue: one out, one in, at the full size: the buffer stays.
        let ptr = set.buf.as_ptr();
        for i in MAX_ENTRIES..MAX_ENTRIES + 50 {
            set.remove_at(0);
            let mut m = vec![b'm'; MAX_LEN];
            m[..8].copy_from_slice(format!("{i:08}").as_bytes());
            add(&mut set, &m, i as f64);
            assert_eq!(set.buf.as_ptr(), ptr, "moved at {i}");
        }
        while set.len() > 1 {
            set.remove_at(0);
            set.check();
            assert!(
                set.allocation_size() <= 2 * set.buf.len().max(64) + 64,
                "{} for {}",
                set.allocation_size(),
                set.buf.len()
            );
        }
        assert!(set.allocation_size() < 128, "{}", set.allocation_size());
    }

    #[test]
    fn a_small_set_is_a_few_bytes_an_entry_over_its_members() {
        // Ten members of 8 bytes, scores 0 to 9: 3 bytes an entry, one for
        // the score, besides the member.
        let pairs: Vec<(Vec<u8>, f64)> = (0..10)
            .map(|i| (format!("m{i:07}").into_bytes(), i as f64))
            .collect();
        let refs: Vec<(&[u8], f64)> = pairs.iter().map(|(m, s)| (&m[..], *s)).collect();
        let set = PackedZset::from_sorted(&refs);
        assert_eq!(set.allocation_size(), 10 * (3 + 1 + 8));
        let mut grown = PackedZset::new();
        for (m, s) in &pairs {
            add(&mut grown, m, *s);
        }
        assert_eq!(grown.buf, set.buf);
        assert!(
            grown.allocation_size() <= 10 * 12 + 32,
            "{}",
            grown.allocation_size()
        );
    }
}
