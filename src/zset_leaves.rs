//! The large form of a sorted set with each entry stored once, inside the
//! leaves of its order (a prototype of collection storage for sorted sets).
//!
//! - **Leaves.** A leaf is one 1,024-byte block: a header, a slot directory
//!   and a heap of entries `[score: 8][tag: 1][member]`. A member of up to
//!   [`INLINE_MAX`] bytes is in the entry; a longer one is in a block of its
//!   own, the entry holding its number (tag [`LONG`]). The directory maps a
//!   slot id (0..[`SLOTS`]) to its entry's offset, and lists the slot ids in
//!   the set's order (`order`). A slot id stays its member's for as long as
//!   the member is in that leaf: compacting the heap rewrites offsets, not
//!   ids.
//! - **Order.** Inner nodes over the leaves, each child's member count kept
//!   for rank, and a separator between children: the score of the right
//!   child's first member, and the member's bytes too when the left child
//!   ends at that same score. Leaves are linked both ways for ranges.
//! - **Index.** A hash table of 4-byte words (`leaf + 1 : slot : 10 bits of
//!   the member's hash`), 8-byte words once a set has 65,535 leaves or more,
//!   with linear probing. A word is resolved through the leaf table (leaf
//!   number to block). Whoever moves an entry to another leaf (a split, a
//!   merge) rewrites its word, found again by the member's hash.
//!
//! Everything here is safe Rust: leaves, long members and inner nodes are
//! owned boxes, and a word is a number checked against the leaf it names.

#![allow(dead_code)]

use std::cmp::Ordering;
use std::ops::Bound;

use ordered_float::OrderedFloat;

use crate::collection_table::TooLong;
use crate::zset_table::{Probe, order};

/// Bytes of a leaf block.
const LEAF_BYTES: usize = 1024;
/// Entries a leaf holds at most (a slot id is 6 bits).
const SLOTS: usize = 48;
/// The header and the directory: `used` 8, `prev`/`next` 8, `top`/`dead`
/// 4, `len` and a pad 2, `order` 48, `offs` 96.
const HEAD: usize = 8 + 8 + 4 + 2 + SLOTS + 2 * SLOTS;
/// The entries' heap.
const HEAP: usize = LEAF_BYTES - HEAD;
/// The longest member kept in its entry.
const INLINE_MAX: usize = 120;
/// The tag of an entry whose member is out of line.
const LONG: u8 = 0xFF;
/// A leaf filled in order (each new member past the last) is closed at this
/// many heap bytes or this many entries: room for a few members that come
/// between later, without a split.
const APPEND_BYTES: usize = HEAP * 7 / 8;
const APPEND_SLOTS: usize = SLOTS - 4;
/// A leaf under this many bytes tries to merge with a neighbour.
const MERGE_BELOW: usize = HEAP / 4;
/// Children of an inner node, at most.
const CAP: usize = 32;
/// No leaf.
const NIL: u32 = u32::MAX;

#[repr(C)]
struct Leaf {
    used: u64,
    prev: u32,
    next: u32,
    top: u16,
    dead: u16,
    len: u8,
    _pad: u8,
    order: [u8; SLOTS],
    offs: [u16; SLOTS],
    heap: [u8; HEAP],
}

const _: () = assert!(std::mem::size_of::<Leaf>() == LEAF_BYTES);

#[inline]
fn entry_size(tag: u8) -> usize {
    if tag == LONG { 13 } else { 9 + tag as usize }
}

/// Members longer than [`INLINE_MAX`], each in a block of its own.
#[derive(Default)]
struct Longs {
    items: Vec<Box<[u8]>>,
    free: Vec<u32>,
    bytes: usize,
}

impl Longs {
    fn put(&mut self, member: &[u8]) -> u32 {
        self.bytes += member.len();
        let boxed: Box<[u8]> = member.into();
        match self.free.pop() {
            Some(i) => {
                self.items[i as usize] = boxed;
                i
            }
            None => {
                self.items.push(boxed);
                (self.items.len() - 1) as u32
            }
        }
    }

    fn take(&mut self, i: u32) {
        let gone = std::mem::take(&mut self.items[i as usize]);
        self.bytes -= gone.len();
        self.free.push(i);
    }

    #[inline]
    fn get(&self, i: u32) -> &[u8] {
        &self.items[i as usize]
    }

    fn heap_bytes(&self) -> usize {
        self.bytes + self.items.capacity() * 16 + self.free.capacity() * 4
    }
}

impl Leaf {
    fn new() -> Box<Leaf> {
        Box::new(Leaf {
            used: 0,
            prev: NIL,
            next: NIL,
            top: 0,
            dead: 0,
            len: 0,
            _pad: 0,
            order: [0; SLOTS],
            offs: [0; SLOTS],
            heap: [0; HEAP],
        })
    }

    #[inline]
    fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    fn off(&self, pos: usize) -> usize {
        self.offs[self.order[pos] as usize] as usize
    }

    #[inline]
    fn score_off(&self, o: usize) -> f64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.heap[o..o + 8]);
        f64::from_le_bytes(b)
    }

    #[inline]
    fn member_off<'a>(&'a self, o: usize, longs: &'a Longs) -> &'a [u8] {
        let tag = self.heap[o + 8];
        if tag == LONG {
            let mut b = [0u8; 4];
            b.copy_from_slice(&self.heap[o + 9..o + 13]);
            longs.get(u32::from_le_bytes(b))
        } else {
            &self.heap[o + 9..o + 9 + tag as usize]
        }
    }

    #[inline]
    fn score(&self, pos: usize) -> f64 {
        self.score_off(self.off(pos))
    }

    #[inline]
    fn member<'a>(&'a self, pos: usize, longs: &'a Longs) -> &'a [u8] {
        self.member_off(self.off(pos), longs)
    }

    #[inline]
    fn raw(&self, pos: usize) -> &[u8] {
        let o = self.off(pos);
        &self.heap[o..o + entry_size(self.heap[o + 8])]
    }

    /// The long member's number, if the entry's member is out of line.
    #[inline]
    fn long_of(&self, pos: usize) -> Option<u32> {
        let o = self.off(pos);
        (self.heap[o + 8] == LONG).then(|| {
            let mut b = [0u8; 4];
            b.copy_from_slice(&self.heap[o + 9..o + 13]);
            u32::from_le_bytes(b)
        })
    }

    #[inline]
    fn used_bytes(&self) -> usize {
        (self.top - self.dead) as usize
    }

    #[inline]
    fn fits(&self, size: usize) -> bool {
        self.len() < SLOTS && self.used_bytes() + size <= HEAP
    }

    /// Entries before `(score, member)` (`after`: up to and including it).
    #[inline]
    fn lower_bound(&self, score: f64, member: &[u8], longs: &Longs, after: bool) -> usize {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            let o = self.off(mid);
            let c = OrderedFloat(self.score_off(o))
                .cmp(&OrderedFloat(score))
                .then_with(|| self.member_off(o, longs).cmp(member));
            let before = match c {
                Ordering::Less => true,
                Ordering::Equal => after,
                Ordering::Greater => false,
            };
            if before {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Where `slot` is in the order.
    #[inline]
    fn pos_of(&self, slot: u8) -> usize {
        self.order[..self.len()]
            .iter()
            .position(|&s| s == slot)
            .expect("a slot in use")
    }

    fn compact(&mut self) {
        let mut heap = [0u8; HEAP];
        let mut top = 0;
        for pos in 0..self.len() {
            let slot = self.order[pos] as usize;
            let o = self.offs[slot] as usize;
            let size = entry_size(self.heap[o + 8]);
            heap[top..top + size].copy_from_slice(&self.heap[o..o + size]);
            self.offs[slot] = top as u16;
            top += size;
        }
        self.heap = heap;
        self.top = top as u16;
        self.dead = 0;
    }

    /// The raw entry put in at `pos` (it fits); its slot id.
    fn put(&mut self, pos: usize, raw: &[u8]) -> u8 {
        debug_assert!(self.fits(raw.len()));
        if self.top as usize + raw.len() > HEAP {
            self.compact();
        }
        let slot = (!self.used).trailing_zeros() as u8;
        let top = self.top as usize;
        self.heap[top..top + raw.len()].copy_from_slice(raw);
        self.offs[slot as usize] = top as u16;
        self.top += raw.len() as u16;
        let len = self.len();
        self.order.copy_within(pos..len, pos + 1);
        self.order[pos] = slot;
        self.len += 1;
        self.used |= 1 << slot;
        slot
    }

    /// The entry at `pos` taken out (its long member, if any, is the
    /// caller's to free).
    fn cut(&mut self, pos: usize) {
        let slot = self.order[pos];
        let o = self.offs[slot as usize] as usize;
        let size = entry_size(self.heap[o + 8]);
        if o + size == self.top as usize {
            self.top -= size as u16;
        } else {
            self.dead += size as u16;
        }
        self.used &= !(1 << slot);
        let len = self.len();
        self.order.copy_within(pos + 1..len, pos);
        self.len -= 1;
        if self.len == 0 {
            self.top = 0;
            self.dead = 0;
        }
    }
}

// ── the index ────────────────────────────────────────────────────────────────

trait Word: Copy + Eq + Default {
    const TOMB: Self;
    /// Leaf numbers this word holds, at most.
    const LEAVES: u32;
    fn make(leaf: u32, slot: u8, h: u64) -> Self;
    fn leaf(self) -> u32;
    fn slot(self) -> u8;
    fn fp_matches(self, h: u64) -> bool;
    fn live(self) -> bool;
}

impl Word for u32 {
    const TOMB: u32 = 1;
    const LEAVES: u32 = 0xFFFF - 1;
    #[inline]
    fn make(leaf: u32, slot: u8, h: u64) -> u32 {
        ((leaf + 1) << 16) | ((slot as u32) << 10) | (h as u32 & 0x3FF)
    }
    #[inline]
    fn leaf(self) -> u32 {
        (self >> 16) - 1
    }
    #[inline]
    fn slot(self) -> u8 {
        ((self >> 10) & 0x3F) as u8
    }
    #[inline]
    fn fp_matches(self, h: u64) -> bool {
        (self ^ h as u32) & 0x3FF == 0 && self >> 16 != 0
    }
    #[inline]
    fn live(self) -> bool {
        self >> 16 != 0
    }
}

impl Word for u64 {
    const TOMB: u64 = 1;
    const LEAVES: u32 = u32::MAX - 1;
    #[inline]
    fn make(leaf: u32, slot: u8, h: u64) -> u64 {
        ((leaf as u64 + 1) << 32) | ((slot as u64) << 26) | (h & 0x3FF_FFFF)
    }
    #[inline]
    fn leaf(self) -> u32 {
        ((self >> 32) - 1) as u32
    }
    #[inline]
    fn slot(self) -> u8 {
        ((self >> 26) & 0x3F) as u8
    }
    #[inline]
    fn fp_matches(self, h: u64) -> bool {
        (self ^ h) & 0x3FF_FFFF == 0 && self >> 32 != 0
    }
    #[inline]
    fn live(self) -> bool {
        self >> 32 != 0
    }
}

#[derive(Clone, Default)]
struct Table<W> {
    slots: Vec<W>,
    live: usize,
    tombs: usize,
}

impl<W: Word> Table<W> {
    fn with_room(members: usize) -> Self {
        let cap = (members * 8 / 5 + 16).next_multiple_of(16);
        Table {
            slots: vec![W::default(); cap],
            live: 0,
            tombs: 0,
        }
    }

    #[inline]
    fn home(&self, h: u64) -> usize {
        ((h as u128 * self.slots.len() as u128) >> 64) as usize
    }

    #[inline]
    fn next(&self, i: usize) -> usize {
        if i + 1 == self.slots.len() { 0 } else { i + 1 }
    }

    /// The word of the member with hash `h` for which `eq` says yes, and
    /// its position; or the position a new one goes in.
    #[inline]
    fn find(&self, h: u64, eq: impl Fn(u32, u8) -> bool) -> Result<(usize, W), usize> {
        let mut i = self.home(h);
        let mut vacant = None;
        loop {
            let w = self.slots[i];
            if w == W::default() {
                return Err(vacant.unwrap_or(i));
            }
            if w == W::TOMB {
                vacant.get_or_insert(i);
            } else if w.fp_matches(h) && eq(w.leaf(), w.slot()) {
                return Ok((i, w));
            }
            i = self.next(i);
        }
    }

    /// Where the word `w` (of hash `h`) is.
    #[inline]
    fn position(&self, h: u64, w: W) -> usize {
        let mut i = self.home(h);
        loop {
            let at = self.slots[i];
            if at == w {
                return i;
            }
            debug_assert!(at != W::default(), "a word of the index");
            i = self.next(i);
        }
    }

    #[inline]
    fn set(&mut self, i: usize, w: W) {
        let old = self.slots[i];
        if old == W::TOMB {
            self.tombs -= 1;
        }
        debug_assert!(!old.live());
        self.slots[i] = w;
        self.live += 1;
    }

    #[inline]
    fn clear(&mut self, i: usize) {
        debug_assert!(self.slots[i].live());
        self.live -= 1;
        if self.slots[self.next(i)] == W::default() {
            self.slots[i] = W::default();
            // The tombstones just before are the end of a run now.
            let mut j = i;
            loop {
                j = if j == 0 { self.slots.len() - 1 } else { j - 1 };
                if self.slots[j] != W::TOMB {
                    break;
                }
                self.slots[j] = W::default();
                self.tombs -= 1;
            }
        } else {
            self.slots[i] = W::TOMB;
            self.tombs += 1;
        }
    }

    #[inline]
    fn empty_at(&self, i: usize) -> bool {
        self.slots[i] == W::default()
    }

    #[inline]
    fn full(&self) -> bool {
        (self.live + self.tombs + 1) * 8 > self.slots.len() * 7
    }

    #[inline]
    fn sparse(&self) -> bool {
        self.slots.len() > 64 && self.live * 4 < self.slots.len()
    }

    /// Put `w` in the first free position from its home (a rebuild).
    #[inline]
    fn place(&mut self, h: u64, w: W) {
        let mut i = self.home(h);
        while self.slots[i] != W::default() {
            i = self.next(i);
        }
        self.slots[i] = w;
        self.live += 1;
    }
}

#[derive(Clone)]
enum Index {
    Narrow(Table<u32>),
    Wide(Table<u64>),
}

macro_rules! with_index {
    ($index:expr, $t:ident => $body:expr) => {
        match $index {
            Index::Narrow($t) => $body,
            Index::Wide($t) => $body,
        }
    };
}

impl Index {
    fn bytes(&self) -> usize {
        match self {
            Index::Narrow(t) => t.slots.capacity() * 4,
            Index::Wide(t) => t.slots.capacity() * 8,
        }
    }
}

// ── the order ────────────────────────────────────────────────────────────────

/// Between two children: the right one's first score, and its first
/// member when the left one ends at that score.
#[derive(Clone)]
struct Sep {
    score: f64,
    member: Option<Box<[u8]>>,
}

impl Sep {
    /// `Less`: the key is before the separator (it goes left).
    #[inline]
    fn cmp_key(&self, score: f64, member: &[u8]) -> Ordering {
        match OrderedFloat(score).cmp(&OrderedFloat(self.score)) {
            Ordering::Equal => match &self.member {
                None => Ordering::Greater,
                Some(m) => member.cmp(m),
            },
            o => o,
        }
    }
}

enum Kids {
    Leaves(Vec<u32>),
    Inners(Vec<Box<Inner>>),
}

struct Inner {
    seps: Vec<Sep>,
    counts: Vec<usize>,
    kids: Kids,
}

impl Inner {
    fn leaves() -> Box<Inner> {
        Box::new(Inner {
            seps: Vec::new(),
            counts: Vec::new(),
            kids: Kids::Leaves(Vec::new()),
        })
    }

    #[inline]
    fn width(&self) -> usize {
        self.counts.len()
    }

    #[inline]
    fn route(&self, score: f64, member: &[u8]) -> usize {
        self.seps
            .partition_point(|sep| sep.cmp_key(score, member) != Ordering::Less)
    }

    fn total(&self) -> usize {
        self.counts.iter().sum()
    }

    /// The upper half, and the separator before it.
    fn split(&mut self) -> (Sep, Box<Inner>) {
        let half = self.width() / 2;
        let counts = self.counts.split_off(half);
        let mut seps = self.seps.split_off(half - 1);
        let up = seps.remove(0);
        let kids = match &mut self.kids {
            Kids::Leaves(ids) => Kids::Leaves(ids.split_off(half)),
            Kids::Inners(kids) => Kids::Inners(kids.split_off(half)),
        };
        (up, Box::new(Inner { seps, counts, kids }))
    }
}

/// How a removal finds its member.
#[derive(Clone, Copy)]
enum Find<'k> {
    Key(f64, &'k [u8]),
    Rank(usize),
}

/// Leaves, long members and the index: what the order's nodes point into.
struct Store {
    leaves: Vec<Option<Box<Leaf>>>,
    free: Vec<u32>,
    nleaves: usize,
    longs: Longs,
    index: Index,
    hasher: ahash::RandomState,
    inners: usize,
    /// A removal leaves the member's word to its caller (who has it).
    keep_word: bool,
    /// Rebuilds of the index so far: a position found before a placement is
    /// good after it only if none came between.
    gen: u64,
}

/// A leaf split: the new leaf (right of the old one) and the separator.
struct LeafSplit {
    sep: Sep,
    id: u32,
}

impl Store {
    #[inline]
    fn leaf(&self, id: u32) -> &Leaf {
        self.leaves[id as usize].as_deref().expect("a leaf in use")
    }

    #[inline]
    fn leaf_mut(&mut self, id: u32) -> &mut Leaf {
        self.leaves[id as usize].as_deref_mut().expect("a leaf in use")
    }

    #[inline]
    fn hash(&self, member: &[u8]) -> u64 {
        self.hasher.hash_one(member)
    }

    fn new_leaf(&mut self) -> u32 {
        self.nleaves += 1;
        let id = match self.free.pop() {
            Some(id) => {
                self.leaves[id as usize] = Some(Leaf::new());
                id
            }
            None => {
                self.leaves.push(Some(Leaf::new()));
                (self.leaves.len() - 1) as u32
            }
        };
        if let Index::Narrow(_) = self.index
            && id >= <u32 as Word>::LEAVES
        {
            self.widen();
        }
        id
    }

    fn drop_leaf(&mut self, id: u32) {
        let (prev, next) = {
            let leaf = self.leaf(id);
            (leaf.prev, leaf.next)
        };
        if prev != NIL {
            self.leaf_mut(prev).next = next;
        }
        if next != NIL {
            self.leaf_mut(next).prev = prev;
        }
        self.leaves[id as usize] = None;
        self.free.push(id);
        self.nleaves -= 1;
    }

    /// Every word made again (a resize, or 8-byte words).
    fn rebuild(&mut self, wide: bool, room: usize) {
        let mut narrow = Table::<u32>::default();
        let mut widened = Table::<u64>::default();
        if wide {
            widened = Table::with_room(room);
        } else {
            narrow = Table::with_room(room);
        }
        for (id, leaf) in self.leaves.iter().enumerate() {
            let Some(leaf) = leaf else { continue };
            for pos in 0..leaf.len() {
                let h = self.hasher.hash_one(leaf.member(pos, &self.longs));
                let slot = leaf.order[pos];
                if wide {
                    widened.place(h, u64::make(id as u32, slot, h));
                } else {
                    narrow.place(h, u32::make(id as u32, slot, h));
                }
            }
        }
        self.gen += 1;
        self.index = if wide {
            Index::Wide(widened)
        } else {
            Index::Narrow(narrow)
        };
    }

    #[cold]
    fn widen(&mut self) {
        let live = with_index!(&self.index, t => t.live);
        self.rebuild(true, live);
    }

    fn wide(&self) -> bool {
        matches!(self.index, Index::Wide(_))
    }

    /// The member's word position, leaf and slot.
    #[inline]
    fn lookup(&self, member: &[u8], h: u64) -> Result<(usize, u32, u8), usize> {
        let eq = |leaf: u32, slot: u8| {
            let Some(Some(l)) = self.leaves.get(leaf as usize) else {
                return false;
            };
            if l.used & (1 << slot) == 0 {
                return false;
            }
            l.member_off(l.offs[slot as usize] as usize, &self.longs) == member
        };
        with_index!(&self.index, t => t.find(h, eq).map(|(i, w)| (i, w.leaf(), w.slot())))
    }

    /// The word of an entry that moved from `(from, s)` to `(to, t)`.
    #[inline]
    fn rehome(&mut self, h: u64, from: (u32, u8), to: (u32, u8)) {
        with_index!(&mut self.index, t => {
            let i = t.position(h, Word::make(from.0, from.1, h));
            t.slots[i] = Word::make(to.0, to.1, h);
        })
    }

    #[inline]
    fn unindex(&mut self, h: u64, at: (u32, u8)) {
        with_index!(&mut self.index, t => {
            let i = t.position(h, Word::make(at.0, at.1, h));
            t.clear(i);
        })
    }

    /// Move `pos..` of leaf `from` to the end of leaf `to`, rewriting their
    /// words.
    fn move_tail(&mut self, from: u32, pos: usize, to: u32) {
        let n = self.leaf(from).len();
        for p in pos..n {
            let (raw, old_slot, h) = {
                let leaf = self.leaf(from);
                let mut raw = [0u8; INLINE_MAX + 9];
                let r = leaf.raw(p);
                raw[..r.len()].copy_from_slice(r);
                (raw, leaf.order[p], self.hash(leaf.member(p, &self.longs)))
            };
            let size = entry_size(raw[8]);
            let dst = self.leaf_mut(to);
            let at = dst.len();
            let slot = dst.put(at, &raw[..size]);
            self.rehome(h, (from, old_slot), (to, slot));
        }
        let leaf = self.leaf_mut(from);
        for p in (pos..n).rev() {
            leaf.cut(p);
        }
    }

    /// Put `raw` (of key `score`, `member`) in leaf `id`; its slot, and the
    /// new leaf if the leaf split. `right_end`: the leaf is the last.
    fn leaf_insert(
        &mut self,
        id: u32,
        score: f64,
        member: &[u8],
        raw: &[u8],
        right_end: bool,
    ) -> ((u32, u8), Option<LeafSplit>) {
        let (pos, len, used, fits) = {
            let leaf = self.leaf(id);
            (
                leaf.lower_bound(score, member, &self.longs, false),
                leaf.len(),
                leaf.used_bytes(),
                leaf.fits(raw.len()),
            )
        };
        let appending = right_end
            && pos == len
            && len > 0
            && (used + raw.len() > APPEND_BYTES || len >= APPEND_SLOTS);
        if fits && !appending {
            let slot = self.leaf_mut(id).put(pos, raw);
            return ((id, slot), None);
        }
        let new = self.new_leaf();
        {
            let next = self.leaf(id).next;
            self.leaf_mut(new).prev = id;
            self.leaf_mut(new).next = next;
            if next != NIL {
                self.leaf_mut(next).prev = new;
            }
            self.leaf_mut(id).next = new;
        }
        let at = if appending {
            let slot = self.leaf_mut(new).put(0, raw);
            (new, slot)
        } else {
            // Half the bytes each side.
            let k = {
                let leaf = self.leaf(id);
                let mut acc = 0;
                let mut k = 0;
                while k < len && acc * 2 < used {
                    acc += leaf.raw(k).len();
                    k += 1;
                }
                k.clamp(1, len - 1)
            };
            self.move_tail(id, k, new);
            if pos <= k {
                let slot = self.leaf_mut(id).put(pos, raw);
                (id, slot)
            } else {
                let slot = self.leaf_mut(new).put(pos - k, raw);
                (new, slot)
            }
        };
        let sep = self.sep_between(id, new);
        (at, Some(LeafSplit { sep, id: new }))
    }

    /// The separator between adjacent leaves `l` and `r`.
    fn sep_between(&self, l: u32, r: u32) -> Sep {
        let (left, right) = (self.leaf(l), self.leaf(r));
        let last = left.score(left.len() - 1);
        let first = right.score(0);
        Sep {
            score: first,
            member: (OrderedFloat(last) == OrderedFloat(first))
                .then(|| right.member(0, &self.longs).into()),
        }
    }

    fn insert_rec(
        &mut self,
        node: &mut Inner,
        score: f64,
        member: &[u8],
        raw: &[u8],
        right_end: bool,
    ) -> ((u32, u8), Option<(Sep, Box<Inner>)>) {
        let i = node.route(score, member);
        let last = i + 1 == node.width();
        node.counts[i] += 1;
        let at = match &mut node.kids {
            Kids::Leaves(ids) => {
                let id = ids[i];
                let (at, split) = self.leaf_insert(id, score, member, raw, right_end && last);
                if let Some(split) = split {
                    node.counts[i] = self.leaf(id).len();
                    let n = self.leaf(split.id).len();
                    let Kids::Leaves(ids) = &mut node.kids else {
                        unreachable!()
                    };
                    ids.insert(i + 1, split.id);
                    node.counts.insert(i + 1, n);
                    node.seps.insert(i, split.sep);
                }
                at
            }
            Kids::Inners(kids) => {
                let (at, split) = self.insert_rec(&mut kids[i], score, member, raw, right_end && last);
                if let Some((sep, right)) = split {
                    node.counts[i] = kids[i].total();
                    node.counts.insert(i + 1, right.total());
                    kids.insert(i + 1, right);
                    node.seps.insert(i, sep);
                }
                at
            }
        };
        if node.width() > CAP {
            self.inners += 1;
            return (at, Some(node.split()));
        }
        (at, None)
    }

    /// Take out the member `find` names: `seen` is given it first. Returns
    /// whether the node is left under a quarter full.
    fn remove_rec(
        &mut self,
        node: &mut Inner,
        find: Find<'_>,
        seen: &mut dyn FnMut(&[u8], f64),
    ) -> bool {
        let (i, find) = match find {
            Find::Key(score, member) => (node.route(score, member), find),
            Find::Rank(mut rank) => {
                let mut i = 0;
                while rank >= node.counts[i] {
                    rank -= node.counts[i];
                    i += 1;
                }
                (i, Find::Rank(rank))
            }
        };
        node.counts[i] -= 1;
        match &mut node.kids {
            Kids::Leaves(ids) => {
                let id = ids[i];
                self.leaf_remove(id, find, seen);
                let (len, used) = {
                    let l = self.leaf(id);
                    (l.len(), l.used_bytes())
                };
                if len == 0 {
                    self.drop_leaf(id);
                    let Kids::Leaves(ids) = &mut node.kids else {
                        unreachable!()
                    };
                    ids.remove(i);
                    node.counts.remove(i);
                    if !node.seps.is_empty() {
                        node.seps.remove(i.saturating_sub(1));
                    }
                } else if used < MERGE_BELOW && ids.len() > 1 {
                    let (l, r) = if i + 1 < ids.len() { (i, i + 1) } else { (i - 1, i) };
                    let (lid, rid) = (ids[l], ids[r]);
                    let (a, b) = (self.leaf(lid), self.leaf(rid));
                    if a.len() + b.len() <= SLOTS && a.used_bytes() + b.used_bytes() <= APPEND_BYTES {
                        self.move_tail(rid, 0, lid);
                        self.drop_leaf(rid);
                        let Kids::Leaves(ids) = &mut node.kids else {
                            unreachable!()
                        };
                        ids.remove(r);
                        let moved = node.counts.remove(r);
                        node.counts[l] += moved;
                        node.seps.remove(l);
                    }
                }
            }
            Kids::Inners(kids) => {
                let under = self.remove_rec(&mut kids[i], find, seen);
                if kids[i].width() == 0 {
                    kids.remove(i);
                    node.counts.remove(i);
                    if !node.seps.is_empty() {
                        node.seps.remove(i.saturating_sub(1));
                    }
                    self.inners -= 1;
                } else if under && kids.len() > 1 {
                    let (l, r) = if i + 1 < kids.len() { (i, i + 1) } else { (i - 1, i) };
                    if kids[l].width() + kids[r].width() <= CAP {
                        let right = kids.remove(r);
                        let sep = node.seps.remove(l);
                        let moved = node.counts.remove(r);
                        node.counts[l] += moved;
                        let left = &mut kids[l];
                        let Inner {
                            seps,
                            counts,
                            kids: rk,
                        } = *right;
                        left.seps.push(sep);
                        left.seps.extend(seps);
                        left.counts.extend(counts);
                        match (&mut left.kids, rk) {
                            (Kids::Leaves(a), Kids::Leaves(b)) => a.extend(b),
                            (Kids::Inners(a), Kids::Inners(b)) => a.extend(b),
                            _ => unreachable!("children of one level"),
                        }
                        self.inners -= 1;
                    }
                }
            }
        }
        node.width() < CAP / 4
    }

    fn leaf_remove(&mut self, id: u32, find: Find<'_>, seen: &mut dyn FnMut(&[u8], f64)) {
        let (pos, slot, h, long) = {
            let leaf = self.leaf(id);
            let pos = match find {
                Find::Key(score, member) => leaf.lower_bound(score, member, &self.longs, false),
                Find::Rank(rank) => rank,
            };
            let member = leaf.member(pos, &self.longs);
            if let Find::Key(score, m) = find {
                debug_assert!(m == member && leaf.score(pos).to_bits() == score.to_bits());
            }
            seen(member, leaf.score(pos));
            let h = if self.keep_word { 0 } else { self.hash(member) };
            (pos, leaf.order[pos], h, leaf.long_of(pos))
        };
        if !self.keep_word {
            self.unindex(h, (id, slot));
        }
        self.leaf_mut(id).cut(pos);
        if let Some(n) = long {
            self.longs.take(n);
        }
    }

    /// Members before `(score, member)` (`after`: up to and including it).
    fn count_below(&self, mut node: &Inner, score: f64, member: &[u8], after: bool) -> usize {
        let mut acc = 0;
        loop {
            let i = node.route(score, member);
            acc += node.counts[..i].iter().sum::<usize>();
            match &node.kids {
                Kids::Leaves(ids) => {
                    return acc + self.leaf(ids[i]).lower_bound(score, member, &self.longs, after);
                }
                Kids::Inners(kids) => node = &kids[i],
            }
        }
    }

    /// The leaf and position of `rank` (within the set).
    fn locate(&self, mut node: &Inner, mut rank: usize) -> (u32, usize) {
        loop {
            let mut i = 0;
            while rank >= node.counts[i] {
                rank -= node.counts[i];
                i += 1;
            }
            match &node.kids {
                Kids::Leaves(ids) => return (ids[i], rank),
                Kids::Inners(kids) => node = &kids[i],
            }
        }
    }
}

/// A large sorted set: see the module's notes.
pub struct Large {
    root: Box<Inner>,
    st: Store,
    len: usize,
    /// Handed to the background (`sorted_set::after_write`).
    pub(crate) queued: bool,
}

/// An entry encoded for a leaf: inline, or a long member's number.
fn encode(score: f64, member: &[u8], longs: &mut Longs, buf: &mut [u8; INLINE_MAX + 9]) -> usize {
    buf[..8].copy_from_slice(&score.to_le_bytes());
    if member.len() <= INLINE_MAX {
        buf[8] = member.len() as u8;
        buf[9..9 + member.len()].copy_from_slice(member);
        9 + member.len()
    } else {
        buf[8] = LONG;
        let n = longs.put(member);
        buf[9..13].copy_from_slice(&n.to_le_bytes());
        13
    }
}

impl Large {
    pub fn with_capacity(members: usize) -> Self {
        Large {
            root: Inner::leaves(),
            st: Store {
                leaves: Vec::new(),
                free: Vec::new(),
                nleaves: 0,
                longs: Longs::default(),
                index: Index::Narrow(Table::with_room(members)),
                hasher: ahash::RandomState::new(),
                inners: 1,
                keep_word: false,
                gen: 0,
            },
            len: 0,
            queued: false,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Members the index takes before it grows.
    pub fn capacity(&self) -> usize {
        with_index!(&self.st.index, t => t.slots.len() * 7 / 8)
    }

    #[inline]
    fn entry(&self, leaf: u32, slot: u8) -> (&[u8], f64) {
        let l = self.st.leaf(leaf);
        let o = l.offs[slot as usize] as usize;
        (l.member_off(o, &self.st.longs), l.score_off(o))
    }

    #[inline]
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        let h = self.st.hash(member);
        let (_, leaf, slot) = self.st.lookup(member, h).ok()?;
        Some(self.entry(leaf, slot).1)
    }

    pub fn rank(&self, member: &[u8]) -> Option<(usize, f64)> {
        let score = self.score(member)?;
        Some((self.st.count_below(&self.root, score, member, false), score))
    }

    pub fn insert(&mut self, member: &[u8], score: f64) -> bool {
        self.upsert(member, |old| (Some(score), old.is_none()))
    }

    /// Put a member not in the set in the order; its leaf and slot.
    fn place(&mut self, score: f64, member: &[u8]) -> (u32, u8) {
        let mut buf = [0u8; INLINE_MAX + 9];
        let size = encode(score, member, &mut self.st.longs, &mut buf);
        let raw = &buf[..size];
        if self.root.width() == 0 {
            let id = self.st.new_leaf();
            let slot = self.st.leaf_mut(id).put(0, raw);
            self.root.counts.push(1);
            self.root.kids = Kids::Leaves(vec![id]);
            return (id, slot);
        }
        let (at, split) = self.st.insert_rec(&mut self.root, score, member, raw, true);
        if let Some((sep, right)) = split {
            let left = std::mem::replace(&mut self.root, Inner::leaves());
            let counts = vec![left.total(), right.total()];
            self.root = Box::new(Inner {
                seps: vec![sep],
                counts,
                kids: Kids::Inners(vec![left, right]),
            });
            self.st.inners += 1;
        }
        at
    }

    /// Take out the member `find` names, `seen` given it first.
    fn take_out(&mut self, find: Find<'_>, seen: &mut dyn FnMut(&[u8], f64)) {
        self.st.remove_rec(&mut self.root, find, seen);
        self.len -= 1;
        // One child left above the leaves: it is the root.
        while self.root.width() == 1 && matches!(self.root.kids, Kids::Inners(_)) {
            let Kids::Inners(kids) = &mut self.root.kids else {
                unreachable!()
            };
            let only = kids.pop().expect("one child");
            self.root = only;
            self.st.inners -= 1;
        }
        if self.root.width() == 0 {
            self.root.kids = Kids::Leaves(Vec::new());
        }
    }

    fn grow_index(&mut self) {
        let (full, live, wide) = (
            with_index!(&self.st.index, t => t.full()),
            self.len,
            self.st.wide(),
        );
        if full {
            self.st.rebuild(wide, live + 1);
        }
    }

    #[inline]
    pub fn upsert<R>(
        &mut self,
        member: &[u8],
        decide: impl FnOnce(Option<f64>) -> (Option<f64>, R),
    ) -> R {
        let h = self.st.hash(member);
        match self.st.lookup(member, h) {
            Ok((i, leaf, slot)) => {
                let old = self.entry(leaf, slot).1;
                let (score, out) = decide(Some(old));
                if let Some(score) = score {
                    let score = score + 0.0;
                    if score.to_bits() != old.to_bits() {
                        self.rescore(i, h, leaf, slot, old, score, member);
                    }
                }
                out
            }
            Err(mut vacant) => {
                let (score, out) = decide(None);
                if let Some(score) = score {
                    let score = score + 0.0;
                    if with_index!(&self.st.index, t => t.full()) {
                        self.grow_index();
                        vacant = self.st.lookup(member, h).expect_err("a new member");
                    }
                    let gen = self.st.gen;
                    let (leaf, slot) = self.place(score, member);
                    if gen == self.st.gen {
                        with_index!(&mut self.st.index, t => t.set(vacant, Word::make(leaf, slot, h)));
                    } else {
                        self.index_new(member, h, leaf, slot);
                    }
                    self.len += 1;
                }
                out
            }
        }
    }

    /// A member's new score: in place when it keeps its place between its
    /// neighbours in the leaf, else out and in again (its word rewritten
    /// where it is).
    #[allow(clippy::too_many_arguments)]
    fn rescore(&mut self, i: usize, h: u64, leaf: u32, slot: u8, old: f64, score: f64, member: &[u8]) {
        {
            let l = self.st.leaf(leaf);
            let pos = l.pos_of(slot);
            let ok = pos > 0
                && pos + 1 < l.len()
                && order(l.score(pos - 1), l.member(pos - 1, &self.st.longs), score, member)
                    == Ordering::Less
                && order(score, member, l.score(pos + 1), l.member(pos + 1, &self.st.longs))
                    == Ordering::Less;
            if ok {
                let o = l.offs[slot as usize] as usize;
                self.st.leaf_mut(leaf).heap[o..o + 8].copy_from_slice(&score.to_le_bytes());
                return;
            }
        }
        // Out and in again. Its word is held at its position as a
        // tombstone meanwhile (which keeps the probe run whole, and matches
        // no entry a split or a merge moves), and rewritten there after.
        with_index!(&mut self.st.index, t => {
            t.slots[i] = Word::TOMB;
            t.live -= 1;
            t.tombs += 1;
        });
        self.st.keep_word = true;
        self.take_out(Find::Key(old, member), &mut |_, _| {});
        self.st.keep_word = false;
        let gen = self.st.gen;
        let (leaf, slot) = self.place(score, member);
        if gen == self.st.gen {
            with_index!(&mut self.st.index, t => t.set(i, Word::make(leaf, slot, h)));
        } else {
            self.index_new(member, h, leaf, slot);
        }
        self.len += 1;
    }

    /// The word of a member just placed at `(leaf, slot)`: in the first room
    /// on its probe run. Looked for after the placement, which may have made
    /// the index again (8-byte words), with this member in it.
    #[inline]
    fn index_new(&mut self, member: &[u8], h: u64, leaf: u32, slot: u8) {
        if let Err(vacant) = self.st.lookup(member, h) {
            with_index!(&mut self.st.index, t => t.set(vacant, Word::make(leaf, slot, h)));
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        let h = self.st.hash(member);
        let Ok((i, leaf, slot)) = self.st.lookup(member, h) else {
            return false;
        };
        let score = self.entry(leaf, slot).1;
        with_index!(&mut self.st.index, t => t.clear(i));
        self.st.keep_word = true;
        self.take_out(Find::Key(score, member), &mut |_, _| {});
        self.st.keep_word = false;
        self.shrink_index();
        true
    }

    fn shrink_index(&mut self) {
        if with_index!(&self.st.index, t => t.sparse()) {
            let wide = self.st.wide();
            self.st.rebuild(wide, self.len);
        }
    }

    pub fn pop_with<R>(&mut self, max: bool, take: impl FnOnce(&[u8], f64) -> R) -> Option<R> {
        if self.len == 0 {
            return None;
        }
        let rank = if max { self.len - 1 } else { 0 };
        let mut take = Some(take);
        let mut out = None;
        self.take_out(Find::Rank(rank), &mut |member, score| {
            if let Some(take) = take.take() {
                out = Some(take(member, score));
            }
        });
        self.shrink_index();
        out
    }

    pub fn remove_ranks(&mut self, start: usize, end: usize) -> usize {
        let end = end.min(self.len);
        for _ in start..end {
            self.take_out(Find::Rank(start), &mut |_, _| {});
        }
        self.shrink_index();
        end.saturating_sub(start)
    }

    pub fn remove_range(&mut self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> usize {
        let (start, end) = self.rank_range(from, to);
        self.remove_ranks(start, end)
    }

    pub fn remove_where(&mut self, mut gone: impl FnMut(&[u8], f64) -> bool) -> usize {
        let doomed: Vec<(f64, Vec<u8>)> = self
            .iter()
            .filter(|(member, score)| gone(member, *score))
            .map(|(member, score)| (score, member.to_vec()))
            .collect();
        for (score, member) in &doomed {
            self.take_out(Find::Key(*score, member), &mut |_, _| {});
        }
        self.shrink_index();
        doomed.len()
    }

    pub fn iter(&self) -> Iter<'_> {
        self.range_by_rank(0, self.len)
    }

    pub fn range_by_rank(&self, start: usize, end: usize) -> Iter<'_> {
        let end = end.min(self.len);
        if start >= end {
            return Iter {
                st: &self.st,
                front: (NIL, 0),
                back: (NIL, 0),
                left: 0,
            };
        }
        Iter {
            st: &self.st,
            front: self.st.locate(&self.root, start),
            back: self.st.locate(&self.root, end - 1),
            left: end - start,
        }
    }

    pub fn range<'s>(&'s self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> Iter<'s> {
        let (start, end) = self.rank_range(from, to);
        self.range_by_rank(start, end)
    }

    pub fn count(&self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> usize {
        let (start, end) = self.rank_range(from, to);
        end - start
    }

    pub fn rank_range(&self, from: Bound<Probe<'_>>, to: Bound<Probe<'_>>) -> (usize, usize) {
        if self.len == 0 {
            return (0, 0);
        }
        let below = |p: Probe<'_>, after| self.st.count_below(&self.root, p.score, p.member, after);
        let start = match from {
            Bound::Unbounded => 0,
            Bound::Included(p) => below(p, false),
            Bound::Excluded(p) => below(p, true),
        };
        let end = match to {
            Bound::Unbounded => self.len,
            Bound::Included(p) => below(p, true),
            Bound::Excluded(p) => below(p, false),
        };
        (start, end.max(start))
    }

    pub fn iter_unordered(&self) -> Iter<'_> {
        self.iter()
    }

    /// A member picked uniformly at random (a random word of the index), its
    /// score, and a number that tells two picks of one member apart.
    pub fn random(&self, rng: &mut impl rand::Rng) -> Option<(&[u8], f64, usize)> {
        if self.len == 0 {
            return None;
        }
        with_index!(&self.st.index, t => loop {
            let w = t.slots[rng.gen_range(0..t.slots.len())];
            if w.live() {
                let (member, score) = self.entry(w.leaf(), w.slot());
                return Some((member, score, ((w.leaf() as usize) << 8) | w.slot() as usize));
            }
        })
    }

    /// One page of `ZSCAN` from `cursor` (a point of the hash space, so
    /// that a resize between pages changes nothing): `f` is given each
    /// member whose hash is in this page's span; the next cursor, 0 at the
    /// end.
    pub fn scan(&self, cursor: u64, count: usize, mut f: impl FnMut(&[u8], f64)) -> u64 {
        with_index!(&self.st.index, t => {
            let cap = t.slots.len() as u128;
            let first = t.home(cursor);
            let last = (first + count.max(1)).min(t.slots.len());
            // The span's end in hash space: the first hash whose home is `last`.
            let end: u128 = if last == t.slots.len() {
                1u128 << 64
            } else {
                ((last as u128) << 64).div_ceil(cap)
            };
            let mut i = first;
            let mut steps = 0;
            loop {
                let w = t.slots[i];
                if t.empty_at(i) && (i >= last || i < first) {
                    break;
                }
                if w.live() {
                    let (member, score) = self.entry(w.leaf(), w.slot());
                    let h = self.st.hash(member) as u128;
                    if h >= cursor as u128 && h < end {
                        f(member, score);
                    }
                }
                i = t.next(i);
                steps += 1;
                if steps > t.slots.len() {
                    break;
                }
            }
            if end >= 1u128 << 64 { 0 } else { end as u64 }
        })
    }

    pub fn shrink_if_sparse(&mut self) {
        self.shrink_index();
    }

    pub fn allocation_size(&self) -> usize {
        let st = &self.st;
        st.nleaves * std::mem::size_of::<Leaf>()
            + st.leaves.capacity() * 8
            + st.free.capacity() * 4
            + st.longs.heap_bytes()
            + st.index.bytes()
            + st.inners * (std::mem::size_of::<Inner>() + (CAP + 1) * (24 + 8 + 8))
    }

    pub fn table_bytes(&self) -> usize {
        self.allocation_size()
    }

    // Nothing for the background: the index is resized in one go.
    #[inline]
    pub fn resizing(&self) -> bool {
        false
    }
    #[inline]
    pub fn holds_retired(&self) -> bool {
        false
    }
    #[inline]
    pub fn transient_bytes(&self) -> usize {
        0
    }
    #[inline]
    pub fn step_resize(&mut self, _budget: usize) -> bool {
        true
    }
    #[inline]
    pub fn take_retired(&mut self) -> Option<crate::collection_table::Retired> {
        None
    }

    /// Every invariant, the slow way (tests).
    #[cfg(test)]
    pub(crate) fn check(&self) {
        let mut last: Option<(f64, Vec<u8>)> = None;
        let mut n = 0;
        for (member, score) in self.iter() {
            if let Some((s, m)) = &last {
                assert_eq!(order(*s, m, score, member), Ordering::Less, "out of order");
            }
            assert_eq!(self.score(member).map(f64::to_bits), Some(score.to_bits()));
            last = Some((score, member.to_vec()));
            n += 1;
        }
        assert_eq!(n, self.len);
        assert_eq!(self.iter().rev().count(), n);
        assert_eq!(with_index!(&self.st.index, t => t.live), n);
        assert_eq!(self.root.total(), n);
        fn walk(st: &Store, node: &Inner) -> usize {
            assert_eq!(node.seps.len() + 1, node.width().max(1));
            match &node.kids {
                Kids::Leaves(ids) => {
                    for (k, &id) in ids.iter().enumerate() {
                        assert_eq!(st.leaf(id).len(), node.counts[k], "leaf count");
                    }
                    ids.len()
                }
                Kids::Inners(kids) => {
                    for (k, kid) in kids.iter().enumerate() {
                        assert_eq!(kid.total(), node.counts[k], "inner count");
                        walk(st, kid);
                    }
                    kids.len()
                }
            }
        }
        walk(&self.st, &self.root);
    }
}

impl Clone for Large {
    fn clone(&self) -> Self {
        let mut built = Builder::with_capacity(self.len);
        for (member, score) in self.iter() {
            let _ = built.push(member, score);
        }
        built.finish()
    }
}

/// Members and scores in order, both ways.
pub struct Iter<'a> {
    st: &'a Store,
    front: (u32, usize),
    back: (u32, usize),
    left: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        let (id, pos) = self.front;
        let leaf = self.st.leaf(id);
        let out = (leaf.member(pos, &self.st.longs), leaf.score(pos));
        self.left -= 1;
        if self.left > 0 {
            self.front = if pos + 1 < leaf.len() { (id, pos + 1) } else { (leaf.next, 0) };
        }
        Some(out)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }

    fn nth(&mut self, mut n: usize) -> Option<Self::Item> {
        // Whole leaves at a time.
        while n > 0 && self.left > 0 {
            let (id, pos) = self.front;
            let leaf = self.st.leaf(id);
            let here = leaf.len() - pos;
            if n >= here && self.left > here {
                n -= here;
                self.left -= here;
                self.front = (leaf.next, 0);
            } else {
                let step = n.min(self.left);
                self.left -= step;
                self.front = (id, pos + step);
                n -= step;
            }
        }
        self.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        let (id, pos) = self.back;
        let leaf = self.st.leaf(id);
        let out = (leaf.member(pos, &self.st.longs), leaf.score(pos));
        self.left -= 1;
        if self.left > 0 {
            self.back = if pos > 0 {
                (id, pos - 1)
            } else {
                let prev = leaf.prev;
                (prev, self.st.leaf(prev).len() - 1)
            };
        }
        Some(out)
    }
}

impl ExactSizeIterator for Iter<'_> {}

/// A large set built from members given one at a time.
pub struct Builder(Large);

impl Builder {
    pub fn with_capacity(members: usize) -> Self {
        Builder(Large::with_capacity(members))
    }

    pub fn len(&self) -> usize {
        self.0.len
    }

    pub fn is_empty(&self) -> bool {
        self.0.len == 0
    }

    /// Add `member` at `score`; one given before takes this score.
    pub fn push(&mut self, member: &[u8], score: f64) -> Result<(), TooLong> {
        if member.len() > crate::collection_table::max_len() {
            return Err(TooLong);
        }
        self.0.insert(member, score);
        Ok(())
    }

    pub fn finish(self) -> Large {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use std::collections::{BTreeMap, HashMap};

    type Model = (BTreeMap<(OrderedFloat<f64>, Vec<u8>), ()>, HashMap<Vec<u8>, f64>);

    fn model_insert(m: &mut Model, member: &[u8], score: f64) -> bool {
        let score = score + 0.0;
        let new = match m.1.insert(member.to_vec(), score) {
            Some(old) => {
                m.0.remove(&(OrderedFloat(old), member.to_vec()));
                false
            }
            None => true,
        };
        m.0.insert((OrderedFloat(score), member.to_vec()), ());
        new
    }

    fn model_remove(m: &mut Model, member: &[u8]) -> bool {
        match m.1.remove(member) {
            Some(old) => {
                m.0.remove(&(OrderedFloat(old), member.to_vec()));
                true
            }
            None => false,
        }
    }

    const SCORES: [f64; 9] = [
        0.0,
        1.0,
        -1.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE / 4.0,
        -f64::MIN_POSITIVE / 8.0,
        -0.0,
        2.5,
    ];

    fn member(rng: &mut impl Rng, pool: usize, long: bool) -> Vec<u8> {
        let i = rng.gen_range(0..pool);
        let len = if long { (i * 37) % 301 } else { (i * 7) % 20 };
        let mut m = format!("{i}:").into_bytes();
        while m.len() < len {
            m.push(b'a' + (i % 26) as u8);
        }
        if len == 0 && i % 5 == 0 {
            m.clear();
        }
        m
    }

    fn score(rng: &mut impl Rng, dups: bool) -> f64 {
        if dups || rng.gen_bool(0.3) {
            SCORES[rng.gen_range(0..SCORES.len())]
        } else {
            (rng.gen_range(-1000i32..1000) as f64) * 0.25
        }
    }

    fn compare(set: &Large, m: &Model, rng: &mut impl Rng) {
        assert_eq!(set.len(), m.1.len());
        let want: Vec<(Vec<u8>, u64)> = m.0.keys().map(|(s, k)| (k.clone(), s.0.to_bits())).collect();
        let got: Vec<(Vec<u8>, u64)> = set.iter().map(|(k, s)| (k.to_vec(), s.to_bits())).collect();
        assert_eq!(got, want);
        let back: Vec<(Vec<u8>, u64)> = set.iter().rev().map(|(k, s)| (k.to_vec(), s.to_bits())).collect();
        assert_eq!(back.into_iter().rev().collect::<Vec<_>>(), want);
        if !want.is_empty() {
            for _ in 0..5 {
                let a = rng.gen_range(0..want.len());
                let b = rng.gen_range(a..=want.len());
                let got: Vec<_> = set.range_by_rank(a, b).map(|(k, s)| (k.to_vec(), s.to_bits())).collect();
                assert_eq!(got, want[a..b]);
                let (k, s) = &want[a];
                assert_eq!(set.rank(k), Some((a, f64::from_bits(*s))));
                let p = Probe { score: f64::from_bits(*s), member: k };
                let (lo, hi) = set.rank_range(Bound::Included(p), Bound::Unbounded);
                assert_eq!((lo, hi), (a, want.len()));
                let (lo, hi) = set.rank_range(Bound::Excluded(p), Bound::Included(p));
                assert_eq!(hi, lo);
                let mut it = set.iter();
                assert_eq!(it.nth(a).map(|(k, _)| k.to_vec()), Some(k.clone()));
            }
            let (m0, _, _) = set.random(rng).unwrap();
            assert!(set.score(m0).is_some());
        }
        // A whole scan, in small pages.
        let mut seen = Vec::new();
        let mut cursor = 0;
        loop {
            cursor = set.scan(cursor, 7, |k, _| seen.push(k.to_vec()));
            if cursor == 0 {
                break;
            }
        }
        seen.sort();
        let mut all: Vec<Vec<u8>> = m.1.keys().cloned().collect();
        all.sort();
        assert_eq!(seen, all, "scan");
        set.check();
    }

    fn churn(seed: u64, pool: usize, long: bool, dups: bool, steps: usize) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut set = Large::with_capacity(0);
        let mut m: Model = Default::default();
        for step in 0..steps {
            match rng.gen_range(0..10) {
                0..=4 => {
                    let k = member(&mut rng, pool, long);
                    let s = score(&mut rng, dups);
                    assert_eq!(set.insert(&k, s), model_insert(&mut m, &k, s));
                }
                5 | 6 => {
                    let k = member(&mut rng, pool, long);
                    assert_eq!(set.remove(&k), model_remove(&mut m, &k));
                }
                7 => {
                    let max = rng.gen_bool(0.5);
                    let want = if max { m.0.keys().next_back().cloned() } else { m.0.keys().next().cloned() };
                    let got = set.pop_with(max, |k, s| (k.to_vec(), s));
                    assert_eq!(got.as_ref().map(|(k, s)| (OrderedFloat(*s), k.clone())), want);
                    if let Some((k, _)) = got {
                        model_remove(&mut m, &k);
                    }
                }
                8 => {
                    if !m.1.is_empty() {
                        let a = rng.gen_range(0..m.1.len());
                        let b = (a + rng.gen_range(0..4)).min(m.1.len());
                        let gone: Vec<Vec<u8>> = m.0.keys().skip(a).take(b - a).map(|(_, k)| k.clone()).collect();
                        assert_eq!(set.remove_ranks(a, b), b - a);
                        for k in gone {
                            model_remove(&mut m, &k);
                        }
                    }
                }
                _ => {
                    let k = member(&mut rng, pool, long);
                    let s = score(&mut rng, dups);
                    let lo = Probe { score: s, member: &k };
                    let want = m.0.range((OrderedFloat(s), k.clone())..).count();
                    assert_eq!(set.count(Bound::Included(lo), Bound::Unbounded), want);
                }
            }
            if step % 97 == 0 {
                compare(&set, &m, &mut rng);
            }
        }
        compare(&set, &m, &mut rng);
        let copy = set.clone();
        compare(&copy, &m, &mut rng);
        while !m.1.is_empty() {
            let k = m.1.keys().next().unwrap().clone();
            assert!(set.remove(&k));
            model_remove(&mut m, &k);
        }
        compare(&set, &m, &mut rng);
    }

    #[test]
    fn model_short() {
        for seed in 0..6 {
            churn(seed, 3000, false, false, 20_000);
        }
    }

    #[test]
    fn model_long_members() {
        for seed in 10..14 {
            churn(seed, 2000, true, false, 15_000);
        }
    }

    #[test]
    fn model_duplicate_scores() {
        for seed in 20..24 {
            churn(seed, 2000, false, true, 15_000);
            churn(seed + 100, 500, true, true, 10_000);
        }
    }

    #[test]
    fn churn_in_one_leaf() {
        // A few members, written over and over: splits and merges of one or
        // two leaves.
        for seed in 30..34 {
            churn(seed, 60, true, true, 30_000);
        }
    }

    #[test]
    fn in_order_fills_leaves() {
        let mut set = Large::with_capacity(0);
        for i in 0..10_000u64 {
            let id = (1000 + i) * 100_000_000_000 + 12_345;
            set.insert(id.to_string().as_bytes(), id as f64);
        }
        set.check();
        let per = set.allocation_size() as f64 / 10_000.0;
        assert!(per < 45.0, "{per} B a member");
    }

    #[test]
    fn rescore_moves_between_leaves() {
        let mut set = Large::with_capacity(0);
        let mut m: Model = Default::default();
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for i in 0..3000 {
            let k = format!("m{i}");
            set.insert(k.as_bytes(), i as f64);
            model_insert(&mut m, k.as_bytes(), i as f64);
        }
        for _ in 0..20_000 {
            let k = format!("m{}", rng.gen_range(0..3000));
            let s = rng.gen_range(-5000.0..5000.0f64).round();
            set.insert(k.as_bytes(), s);
            model_insert(&mut m, k.as_bytes(), s);
        }
        compare(&set, &m, &mut rng);
    }
}
