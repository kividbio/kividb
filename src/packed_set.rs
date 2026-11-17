//! A small set of short byte strings in one block of memory.
//!
//! A set of a few members does not need a hash table: a table pays a
//! separate allocation for every member (an `Arc<[u8]>` is 16 bytes of
//! header before the member's own bytes) and a slot for it, and at ten
//! members that is several times the members themselves. Here the members
//! lie end to end in one buffer, and a member is found by scanning one byte
//! a member: a *fingerprint* of it, kept together at the front, eight or
//! sixteen compared at a time. Only a member whose fingerprint matches is compared
//! in full, so a lookup that misses reads `len` bytes and nothing else, and
//! one that hits reads them and the member.
//!
//! The buffer, for a capacity of `cap` members (8, 16, 32, 64 or 128):
//!
//! ```text
//! [ fingerprints: cap bytes ][ offsets: (cap + 1) x u16 ][ members' bytes ... ]
//! ```
//!
//! `offsets[i]` is where member `i` begins in the bytes and `offsets[i + 1]`
//! where it ends, so a member's length is not stored and member `i` is
//! reached in one step: a random member is a random index. Three bytes a
//! member besides its own.
//!
//! Members keep the order they were added in; removing one closes the gap.
//! The set refuses a member longer than [`MAX_LEN`] and a member past
//! [`MAX_ENTRIES`]: whoever holds it then moves to a table.

/// The most members a packed set holds.
pub const MAX_ENTRIES: usize = 128;
/// The longest member a packed set holds.
pub const MAX_LEN: usize = 64;

/// What [`PackedSet::insert`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    /// The member is new and was added.
    Added,
    /// The member was there already.
    Present,
    /// The member is not there and cannot be added: it is too long, or the
    /// set is at its limit. The set is unchanged.
    NoRoom,
}

#[derive(Clone, Default)]
pub struct PackedSet {
    buf: Vec<u8>,
    /// Members held.
    len: u8,
    /// Members there is room for in the fingerprints and offsets.
    cap: u8,
}

/// `a == b` for two slices of one length, short ones (as members of a
/// block are) by a word or two from each end rather than a call.
#[inline(always)]
fn same(a: &[u8], b: &[u8]) -> bool {
    let n = a.len();
    debug_assert_eq!(n, b.len());
    let word = |s: &[u8], at: usize| u64::from_le_bytes(s[at..at + 8].try_into().unwrap());
    let half = |s: &[u8], at: usize| u32::from_le_bytes(s[at..at + 4].try_into().unwrap());
    match n {
        16.. => word(a, 0) == word(b, 0) && a[8..] == b[8..],
        8.. => word(a, 0) == word(b, 0) && word(a, n - 8) == word(b, n - 8),
        4.. => half(a, 0) == half(b, 0) && half(a, n - 4) == half(b, n - 4),
        _ => a == b,
    }
}

/// One byte of a member, for the scan: equal members have equal
/// fingerprints, and different ones seldom do. The first and the last eight
/// bytes take part (the whole member when it is shorter), with its length:
/// members that differ only in a counter at their end, or only in a prefix,
/// differ here.
#[inline]
fn fingerprint(member: &[u8]) -> u8 {
    let n = member.len();
    let x = if n >= 8 {
        let head = u64::from_le_bytes(member[..8].try_into().unwrap());
        let tail = u64::from_le_bytes(member[n - 8..].try_into().unwrap());
        head ^ tail.rotate_left(29)
    } else {
        let mut bytes = [0u8; 8];
        bytes[..n].copy_from_slice(member);
        u64::from_le_bytes(bytes)
    };
    ((x ^ n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as u8
}

impl PackedSet {
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

    /// Whether a member of this length may be held at all.
    #[allow(dead_code)] // The server binary has no caller; the library's API keeps it.
    #[inline]
    pub fn takes(member: &[u8]) -> bool {
        member.len() <= MAX_LEN
    }

    /// Where the members' bytes begin.
    #[inline]
    fn data_start(&self) -> usize {
        let cap = self.cap as usize;
        cap + 2 * (cap + 1)
    }

    #[inline(always)]
    fn offset(&self, i: usize) -> usize {
        let at = self.cap as usize + 2 * i;
        u16::from_le_bytes([self.buf[at], self.buf[at + 1]]) as usize
    }

    #[inline]
    fn set_offset(&mut self, i: usize, offset: usize) {
        let at = self.cap as usize + 2 * i;
        self.buf[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
    }

    /// Member `i`, in the order members were added. `i < len()`.
    #[inline]
    pub fn get(&self, i: usize) -> &[u8] {
        debug_assert!(i < self.len());
        let start = self.data_start();
        &self.buf[start + self.offset(i)..start + self.offset(i + 1)]
    }

    /// Where `member` is, if it is a member.
    #[inline]
    pub fn find(&self, member: &[u8]) -> Option<usize> {
        if self.len == 0 || member.len() > MAX_LEN {
            return None;
        }
        self.find_by(member, fingerprint(member))
    }

    /// The fingerprint member `i` was filed under. `i < len()`.
    #[inline]
    pub fn fingerprint_at(&self, i: usize) -> u8 {
        self.buf[i]
    }

    /// [`find`](Self::find) of a member of another block, whose fingerprint
    /// is known already ([`fingerprint_at`](Self::fingerprint_at)): the
    /// intersection of two blocks computes none.
    #[inline]
    pub fn find_fingerprinted(&self, member: &[u8], f: u8) -> Option<usize> {
        if self.len == 0 || member.len() > MAX_LEN {
            return None;
        }
        self.find_by(member, f)
    }

    /// The scan. The fingerprints take `cap` bytes, a multiple of eight,
    /// and the offsets follow them, so a whole word (or vector) is always
    /// there to be read; the places past the members are masked off, and
    /// there is no byte-at-a-time tail. Sixteen fingerprints at a time with
    /// SSE2 (every x86-64 has it), eight a word elsewhere.
    #[inline(always)]
    fn find_by(&self, member: &[u8], f: u8) -> Option<usize> {
        let n = self.len();
        let cap = self.cap as usize;
        debug_assert!(n <= cap && cap.is_multiple_of(8));
        let start = self.data_start();
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::{
                __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
            };
            // The block is at least `3 * cap + 2` bytes (fingerprints and
            // offsets), so 16 bytes from any `i < n <= cap` are in it.
            assert!(self.buf.len() >= cap + 16);
            let base = self.buf.as_ptr();
            let mut i = 0;
            while i < n {
                // SAFETY: SSE2 is part of x86-64; `i + 16 <= cap + 16 <=
                // buf.len()` (asserted above), and an unaligned load is
                // what `loadu` is for.
                let mut marks = unsafe {
                    let needle = _mm_set1_epi8(f as i8);
                    let chunk = _mm_loadu_si128(base.add(i).cast::<__m128i>());
                    _mm_movemask_epi8(_mm_cmpeq_epi8(chunk, needle)) as u32
                };
                // Past the members, nothing: no branch, `1 << 16` fits.
                marks &= (1u32 << (n - i).min(16)).wrapping_sub(1);
                while marks != 0 {
                    let at = i + marks.trailing_zeros() as usize;
                    if self.is_at(at, start, member) {
                        return Some(at);
                    }
                    marks &= marks - 1;
                }
                i += 16;
            }
            None
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            const LOW: u64 = 0x0101_0101_0101_0101;
            const HIGH: u64 = 0x8080_8080_8080_8080;
            let pattern = LOW.wrapping_mul(f as u64);
            let mut i = 0;
            while i < n {
                // A byte of `word` is zero where a fingerprint equals `f`.
                // The test below marks every zero byte, and may mark a byte
                // above one that is not: the full comparison settles it.
                let word = u64::from_le_bytes(self.buf[i..i + 8].try_into().unwrap()) ^ pattern;
                let mut marks = word.wrapping_sub(LOW) & !word & HIGH;
                if n - i < 8 {
                    marks &= (1u64 << ((n - i) * 8)) - 1;
                }
                while marks != 0 {
                    let at = i + (marks.trailing_zeros() as usize >> 3);
                    if self.is_at(at, start, member) {
                        return Some(at);
                    }
                    marks &= marks - 1;
                }
                i += 8;
            }
            None
        }
    }

    /// Whether member `at` is `member`: the length first, from the two
    /// offsets read as one word, then the bytes.
    #[inline(always)]
    fn is_at(&self, at: usize, start: usize, member: &[u8]) -> bool {
        let o = self.cap as usize + 2 * at;
        let both = u32::from_le_bytes(self.buf[o..o + 4].try_into().unwrap());
        let (from, to) = ((both & 0xffff) as usize, (both >> 16) as usize);
        to.wrapping_sub(from) == member.len() && same(&self.buf[start + from..start + to], member)
    }

    #[inline]
    pub fn contains(&self, member: &[u8]) -> bool {
        self.find(member).is_some()
    }

    /// Add `member` if it is not one.
    pub fn insert(&mut self, member: &[u8]) -> Insert {
        if member.len() > MAX_LEN {
            return Insert::NoRoom;
        }
        if self.find(member).is_some() {
            return Insert::Present;
        }
        let n = self.len();
        if n == MAX_ENTRIES {
            return Insert::NoRoom;
        }
        self.push(member);
        Insert::Added
    }

    /// Add a member known not to be one, with room known to be there
    /// (`len() < MAX_ENTRIES`, `member.len() <= MAX_LEN`).
    pub fn push(&mut self, member: &[u8]) {
        debug_assert!(self.len() < MAX_ENTRIES && member.len() <= MAX_LEN);
        let n = self.len();
        if n == self.cap as usize {
            self.grow();
        }
        let end = self.offset(n);
        // A quarter over what is there at most, not the doubling a `Vec`
        // does by itself: the block is the whole of a small set's memory.
        if self.buf.capacity() - self.buf.len() < member.len() {
            let more = member.len().max(self.buf.len() / 4).max(16);
            self.buf.reserve_exact(more);
        }
        self.buf.extend_from_slice(member);
        self.buf[n] = fingerprint(member);
        self.set_offset(n + 1, end + member.len());
        self.len += 1;
    }

    /// Room for twice the members (eight to begin with): the fingerprints
    /// and the offsets are laid out again, the members' bytes after them.
    fn grow(&mut self) {
        let n = self.len();
        let old_cap = self.cap as usize;
        let cap = if old_cap == 0 { 8 } else { old_cap * 2 };
        debug_assert!(cap <= MAX_ENTRIES);
        let data = if old_cap == 0 { 0 } else { self.offset(n) };
        let head = cap + 2 * (cap + 1);
        let mut buf = Vec::with_capacity(head + data + (data / 4).max(32));
        buf.resize(head, 0);
        if old_cap != 0 {
            buf[..n].copy_from_slice(&self.buf[..n]);
            buf[cap..cap + 2 * (n + 1)].copy_from_slice(&self.buf[old_cap..old_cap + 2 * (n + 1)]);
            let start = self.data_start();
            buf.extend_from_slice(&self.buf[start..start + data]);
        }
        self.buf = buf;
        self.cap = cap as u8;
    }

    /// Take member `i` out; the members after it keep their order.
    pub fn remove_at(&mut self, i: usize) {
        let n = self.len();
        debug_assert!(i < n);
        let start = self.data_start();
        let (from, to) = (self.offset(i), self.offset(i + 1));
        let gone = to - from;
        self.buf.drain(start + from..start + to);
        self.buf.copy_within(i + 1..n, i);
        for j in i + 1..n {
            let next = self.offset(j + 1);
            self.set_offset(j, next - gone);
        }
        self.len -= 1;
    }

    /// Remove `member` if it is one.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self.find(member) {
            Some(i) => {
                self.remove_at(i);
                true
            }
            None => false,
        }
    }

    /// Keep the members `keep` says to.
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8]) -> bool) {
        let mut i = 0;
        while i < self.len() {
            if keep(self.get(i)) {
                i += 1;
            } else {
                self.remove_at(i);
            }
        }
    }

    pub fn clear(&mut self) {
        self.buf = Vec::new();
        self.len = 0;
        self.cap = 0;
    }

    /// The members, in the order they were added.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        Iter { set: self, next: 0 }
    }
}

pub struct Iter<'a> {
    set: &'a PackedSet,
    next: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.next < self.set.len() {
            let member = self.set.get(self.next);
            self.next += 1;
            Some(member)
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.set.len() - self.next;
        (left, Some(left))
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl std::fmt::Debug for PackedSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set()
            .entries(self.iter().map(String::from_utf8_lossy))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn member(i: usize) -> Vec<u8> {
        format!("member:{i:08}").into_bytes()
    }

    #[test]
    fn it_is_a_set() {
        let mut set = PackedSet::new();
        assert!(set.is_empty());
        assert!(!set.contains(b"a"));
        assert_eq!(set.insert(b"a"), Insert::Added);
        assert_eq!(set.insert(b"a"), Insert::Present);
        assert_eq!(set.insert(b""), Insert::Added);
        assert_eq!(set.insert(b""), Insert::Present);
        assert_eq!(set.len(), 2);
        assert!(set.contains(b"a") && set.contains(b"") && !set.contains(b"b"));
        assert!(set.remove(b"a"));
        assert!(!set.remove(b"a"));
        assert_eq!(set.len(), 1);
        assert!(set.contains(b""));
        set.clear();
        assert!(set.is_empty() && !set.contains(b""));
        assert_eq!(set.allocation_size(), 0);
    }

    #[test]
    fn members_keep_the_order_they_were_added_in() {
        let mut set = PackedSet::new();
        for i in 0..MAX_ENTRIES {
            assert_eq!(set.insert(&member(i)), Insert::Added, "{i}");
        }
        let all: Vec<Vec<u8>> = set.iter().map(<[u8]>::to_vec).collect();
        assert_eq!(all, (0..MAX_ENTRIES).map(member).collect::<Vec<_>>());
        // One from the middle, the first and the last: the rest stay in order.
        for gone in [60, 0, MAX_ENTRIES - 1] {
            assert!(set.remove(&member(gone)));
        }
        let left: Vec<Vec<u8>> = set.iter().map(<[u8]>::to_vec).collect();
        let want: Vec<Vec<u8>> = (0..MAX_ENTRIES)
            .filter(|i| ![60, 0, MAX_ENTRIES - 1].contains(i))
            .map(member)
            .collect();
        assert_eq!(left, want);
        for (i, m) in want.iter().enumerate() {
            assert_eq!(set.get(i), &m[..]);
            assert_eq!(set.find(m), Some(i));
        }
    }

    #[test]
    fn it_takes_no_more_than_its_limits() {
        let mut set = PackedSet::new();
        assert_eq!(set.insert(&[b'x'; MAX_LEN]), Insert::Added);
        assert_eq!(set.insert(&[b'x'; MAX_LEN + 1]), Insert::NoRoom);
        assert!(!set.contains(&[b'x'; MAX_LEN + 1]));
        for i in 1..MAX_ENTRIES {
            assert_eq!(set.insert(&member(i)), Insert::Added);
        }
        assert_eq!(set.len(), MAX_ENTRIES);
        assert_eq!(set.insert(b"one more"), Insert::NoRoom);
        // One that is there is still found at the limit.
        assert_eq!(set.insert(&member(5)), Insert::Present);
        assert_eq!(set.len(), MAX_ENTRIES);
    }

    #[test]
    fn the_longest_members_fit_at_the_limit() {
        // 128 members of 64 bytes: the last offset is 8192, a u16.
        let mut set = PackedSet::new();
        for i in 0..MAX_ENTRIES {
            let mut m = vec![b'a' + (i % 26) as u8; MAX_LEN];
            m[..8].copy_from_slice(format!("{i:08}").as_bytes());
            assert_eq!(set.insert(&m), Insert::Added);
        }
        for i in 0..MAX_ENTRIES {
            let mut m = vec![b'a' + (i % 26) as u8; MAX_LEN];
            m[..8].copy_from_slice(format!("{i:08}").as_bytes());
            assert_eq!(set.find(&m), Some(i));
        }
    }

    #[test]
    fn members_with_one_fingerprint_are_told_apart() {
        // Enough members that fingerprints repeat (a byte has 256 values):
        // every one is still found at its own place, and none that is not
        // there is.
        let mut set = PackedSet::new();
        let mut by_fp = std::collections::HashMap::<u8, usize>::new();
        for i in 0..MAX_ENTRIES {
            set.insert(&member(i));
            *by_fp.entry(fingerprint(&member(i))).or_default() += 1;
        }
        assert!(by_fp.values().any(|n| *n > 1), "no fingerprint repeats");
        for i in 0..MAX_ENTRIES {
            assert_eq!(set.find(&member(i)), Some(i));
        }
        for i in MAX_ENTRIES..4 * MAX_ENTRIES {
            assert_eq!(set.find(&member(i)), None);
        }
    }

    #[test]
    fn it_agrees_with_a_set_through_adds_and_removes() {
        // A fixed sequence of adds, removes and lookups over a small space
        // of members of every length up to the limit, checked against a
        // `BTreeSet` at every step.
        let mut set = PackedSet::new();
        let mut model = BTreeSet::<Vec<u8>>::new();
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for step in 0..20_000 {
            let r = next();
            let id = (r >> 8) % 300;
            let len = (id as usize * 7) % (MAX_LEN + 1);
            let mut m = format!("{id}").into_bytes();
            m.resize(len.max(m.len()).min(MAX_LEN), b'.');
            match r % 4 {
                0 | 1 => {
                    let did = set.insert(&m);
                    if model.contains(&m) {
                        assert_eq!(did, Insert::Present, "step {step}");
                    } else if model.len() == MAX_ENTRIES {
                        assert_eq!(did, Insert::NoRoom, "step {step}");
                    } else {
                        assert_eq!(did, Insert::Added, "step {step}");
                        model.insert(m);
                    }
                }
                2 => assert_eq!(set.remove(&m), model.remove(&m), "step {step}"),
                _ => assert_eq!(set.contains(&m), model.contains(&m), "step {step}"),
            }
            assert_eq!(set.len(), model.len(), "step {step}");
            if step % 500 == 0 {
                let held: BTreeSet<Vec<u8>> = set.iter().map(<[u8]>::to_vec).collect();
                assert_eq!(held, model, "step {step}");
            }
        }
    }

    #[test]
    fn a_member_of_another_block_is_found_by_its_fingerprint() {
        let mut a = PackedSet::new();
        let mut b = PackedSet::new();
        for i in 0..100 {
            a.insert(&member(i));
            b.insert(&member(i + 50));
        }
        for i in 0..a.len() {
            let f = a.fingerprint_at(i);
            assert_eq!(f, fingerprint(a.get(i)));
            assert_eq!(b.find_fingerprinted(a.get(i), f), b.find(a.get(i)));
        }
        assert_eq!(
            PackedSet::new().find_fingerprinted(b"x", fingerprint(b"x")),
            None
        );
    }

    #[test]
    fn retain_keeps_what_it_is_told_to() {
        let mut set = PackedSet::new();
        for i in 0..100 {
            set.insert(&member(i));
        }
        set.retain(|m| m[m.len() - 1] % 2 == 0);
        assert_eq!(set.len(), 50);
        for i in 0..100 {
            assert_eq!(set.contains(&member(i)), i % 2 == 0, "{i}");
        }
    }

    #[test]
    fn a_small_set_is_a_few_bytes_a_member_over_its_members() {
        // Ten members of 14 bytes: the block, with its room to grow, is
        // under 20 bytes a member over the members' own 14.
        let mut set = PackedSet::new();
        for i in 0..10 {
            set.insert(format!("membr:{i:08}").as_bytes());
        }
        let per_member = set.allocation_size() as f64 / 10.0;
        assert!(per_member < 14.0 + 20.0, "{per_member} bytes a member");
        // A hundred: under 6 over.
        let mut set = PackedSet::new();
        for i in 0..100 {
            set.insert(format!("membr:{i:08}").as_bytes());
        }
        let per_member = set.allocation_size() as f64 / 100.0;
        assert!(per_member < 14.0 + 6.0, "{per_member} bytes a member");
    }
}
