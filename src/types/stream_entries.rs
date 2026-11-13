//! The entries of a stream, in ID order.
//!
//! Commands, persistence and memory accounting reach a stream's entries only
//! through [`StreamEntries`]: IDs come back by value and fields as borrowed
//! slices ([`EntryRef`]), so the way entries are stored can change without
//! touching them.
//!
//! # Layout
//!
//! Entries are packed, in ID order, into blocks of at most [`MAX_ENTRIES`]
//! entries and [`MAX_BYTES`] encoded bytes (an entry larger than that gets a
//! block of its own). A block's buffer is:
//!
//! ```text
//! names:  n, (len, name) * n          the field names of its first entry
//! entry:  flags                       DELETED | SAME_NAMES
//!         Δms, seq                    the ID (see below)
//!         SAME_NAMES: (len, value) * n
//!         otherwise:  m, (len, name, len, value) * m
//! ```
//!
//! Every number is a varint. An entry's ID is stored against the entry
//! before it in the block, or against the block's `key` for every
//! [`SEGMENT`]th entry (an anchor), so a lookup starts at the nearest anchor
//! instead of the block's start: `Δms` is the difference in milliseconds,
//! and `seq` is the difference in sequence when `Δms` is 0, the sequence
//! itself otherwise.
//!
//! A deleted entry keeps its bytes, flagged, until its block is rewritten:
//! once a quarter of a block is deleted entries, or when an entry has to go
//! in the middle. A block with nothing left in it is freed at once. Entries
//! deleted from the front of the first block (trimming) are skipped instead
//! of rewritten, and freed with the block.

use super::StreamId;
use std::collections::{BTreeMap, VecDeque};
use std::ops::{Bound, RangeBounds};

/// The most entries in a block.
const MAX_ENTRIES: usize = 64;
/// The most bytes in a block of more than one entry.
const MAX_BYTES: usize = 2048;
/// Entries from one anchor to the next.
const SEGMENT: usize = 16;
const ANCHORS: usize = MAX_ENTRIES / SEGMENT - 1;

const DELETED: u8 = 1;
const SAME_NAMES: u8 = 2;

// ── varints and strings ──────────────────────────────────────────────────────

fn put(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push(x as u8 | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}

fn vlen(x: u64) -> usize {
    (64 - (x | 1).leading_zeros() as usize).div_ceil(7)
}

fn get(b: &[u8], p: &mut usize) -> u64 {
    let mut x = 0u64;
    let mut shift = 0;
    loop {
        let c = b[*p];
        *p += 1;
        x |= u64::from(c & 0x7f) << shift;
        if c < 0x80 {
            return x;
        }
        shift += 7;
    }
}

fn put_str(buf: &mut Vec<u8>, s: &[u8]) {
    put(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

fn str_len(s: &[u8]) -> usize {
    vlen(s.len() as u64) + s.len()
}

fn take<'a>(b: &'a [u8], p: &mut usize) -> &'a [u8] {
    let n = get(b, p) as usize;
    let s = &b[*p..*p + n];
    *p += n;
    s
}

fn skip_str(b: &[u8], p: &mut usize) {
    let n = get(b, p) as usize;
    *p += n;
}

/// `id` against `base`, which is not after it.
fn delta(id: &StreamId, base: &StreamId) -> (u64, u64) {
    debug_assert!(id >= base);
    if id.ms == base.ms {
        (0, id.seq - base.seq)
    } else {
        (id.ms - base.ms, id.seq)
    }
}

fn undelta(base: &StreamId, dms: u64, seq: u64) -> StreamId {
    if dms == 0 {
        StreamId::new(base.ms, base.seq + seq)
    } else {
        StreamId::new(base.ms + dms, seq)
    }
}

// ── blocks ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Block {
    /// The ID of the first entry put in the block: not after any entry in
    /// it, and before every entry of the next block.
    key: StreamId,
    buf: Vec<u8>,
    /// Where the field names end and the entries begin.
    hdr: u32,
    /// Bytes of deleted entries, but for those the first block skips.
    dead: u32,
    /// Entries in `buf`, deleted ones included.
    count: u8,
    live: u8,
    /// Where entries `SEGMENT`, `2 * SEGMENT`, ... begin; 0 past the last.
    anchors: [u16; ANCHORS],
}

// The anchors fill what would be padding.
const _: () = assert!(std::mem::size_of::<Block>() == 56);

/// Where a scan of a block is: the entry at `off`, the `idx`th of the block,
/// and the ID of the entry before it.
#[derive(Debug, Clone)]
struct Cursor {
    off: usize,
    idx: usize,
    prev: StreamId,
}

/// One entry, decoded as far as its ID.
#[derive(Debug, Clone)]
struct Decoded {
    id: StreamId,
    flags: u8,
    /// How many field-value pairs.
    n: usize,
    /// Where its fields begin (after their count, if it has one) and end.
    body: usize,
    end: usize,
}

impl Decoded {
    fn live(&self) -> bool {
        self.flags & DELETED == 0
    }
}

fn entry_len<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    same: bool,
    (dms, seq): (u64, u64),
    fields: &[(K, V)],
) -> usize {
    let values: usize = fields.iter().map(|(_, v)| str_len(v.as_ref())).sum();
    let names = if same {
        0
    } else {
        vlen(fields.len() as u64)
            + fields
                .iter()
                .map(|(k, _)| str_len(k.as_ref()))
                .sum::<usize>()
    };
    1 + vlen(dms) + vlen(seq) + names + values
}

impl Block {
    /// A block of the one entry `id`, its field names the block's.
    fn start<K: AsRef<[u8]>, V: AsRef<[u8]>>(id: &StreamId, fields: &[(K, V)]) -> Block {
        let hdr = vlen(fields.len() as u64)
            + fields
                .iter()
                .map(|(k, _)| str_len(k.as_ref()))
                .sum::<usize>();
        let size = entry_len(true, (0, 0), fields);
        let mut buf = Vec::with_capacity(hdr + size);
        put(&mut buf, fields.len() as u64);
        for (k, _) in fields {
            put_str(&mut buf, k.as_ref());
        }
        let mut block = Block {
            key: id.clone(),
            buf,
            hdr: hdr as u32,
            dead: 0,
            count: 0,
            live: 0,
            anchors: [0; ANCHORS],
        };
        block.write((0, 0), true, fields, size);
        block
    }

    /// The number of field names, and their encoding.
    fn names(&self) -> (usize, &[u8]) {
        let mut p = 0;
        let n = get(&self.buf, &mut p) as usize;
        (n, &self.buf[p..self.hdr as usize])
    }

    fn same_names<K: AsRef<[u8]>, V>(&self, fields: &[(K, V)]) -> bool {
        let (n, names) = self.names();
        let mut p = 0;
        n == fields.len()
            && fields
                .iter()
                .all(|(k, _)| take(names, &mut p) == k.as_ref())
    }

    /// Append `id`, after `prev`, if it fits.
    fn try_push<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        id: &StreamId,
        prev: &StreamId,
        fields: &[(K, V)],
    ) -> bool {
        let i = self.count as usize;
        if i >= MAX_ENTRIES {
            return false;
        }
        let parts = delta(
            id,
            if i.is_multiple_of(SEGMENT) {
                &self.key
            } else {
                prev
            },
        );
        let same = self.same_names(fields);
        let size = entry_len(same, parts, fields);
        if self.buf.len() + size > MAX_BYTES {
            return false;
        }
        self.write(parts, same, fields, size);
        true
    }

    fn write<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        (dms, seq): (u64, u64),
        same: bool,
        fields: &[(K, V)],
        size: usize,
    ) {
        let i = self.count as usize;
        if i > 0 && i.is_multiple_of(SEGMENT) {
            self.anchors[i / SEGMENT - 1] = self.buf.len() as u16;
        }
        // Grow by doubling, but not past the block's limit.
        let buf = &mut self.buf;
        if buf.capacity() - buf.len() < size {
            let want = (buf.capacity() * 2).min(MAX_BYTES).max(buf.len() + size);
            buf.reserve_exact(want - buf.len());
        }
        buf.push(if same { SAME_NAMES } else { 0 });
        put(buf, dms);
        put(buf, seq);
        if !same {
            put(buf, fields.len() as u64);
        }
        for (k, v) in fields {
            if !same {
                put_str(buf, k.as_ref());
            }
            put_str(buf, v.as_ref());
        }
        self.count += 1;
        self.live += 1;
    }

    fn full(&self) -> bool {
        self.count as usize >= MAX_ENTRIES || self.buf.len() >= MAX_BYTES
    }

    fn at_start(&self) -> Cursor {
        Cursor {
            off: self.hdr as usize,
            idx: 0,
            prev: self.key.clone(),
        }
    }

    fn at_end(&self, c: &Cursor) -> bool {
        c.off >= self.buf.len()
    }

    /// The entry at `c`; `c` moves on to the next.
    fn decode(&self, c: &mut Cursor, names: usize) -> Decoded {
        let b = &self.buf;
        let mut p = c.off;
        let flags = b[p];
        p += 1;
        let dms = get(b, &mut p);
        let seq = get(b, &mut p);
        let base = if c.idx.is_multiple_of(SEGMENT) {
            &self.key
        } else {
            &c.prev
        };
        let id = undelta(base, dms, seq);
        let (n, strings) = if flags & SAME_NAMES != 0 {
            (names, names)
        } else {
            let n = get(b, &mut p) as usize;
            (n, 2 * n)
        };
        let body = p;
        for _ in 0..strings {
            skip_str(b, &mut p);
        }
        c.off = p;
        c.idx += 1;
        c.prev = id.clone();
        Decoded {
            id,
            flags,
            n,
            body,
            end: p,
        }
    }

    /// The ID of anchor `j`.
    fn anchor_id(&self, j: usize) -> StreamId {
        let mut p = self.anchors[j] as usize + 1;
        let dms = get(&self.buf, &mut p);
        let seq = get(&self.buf, &mut p);
        undelta(&self.key, dms, seq)
    }

    fn entry(&self, d: &Decoded) -> EntryRef<'_> {
        let (_, names) = self.names();
        EntryRef {
            names,
            body: &self.buf[d.body..d.end],
            n: d.n as u32,
            same: d.flags & SAME_NAMES != 0,
        }
    }

    /// Rewrite the block from `from` on without its deleted entries.
    fn compact(&mut self, from: Cursor) {
        let (names, _) = self.names();
        let mut out = Vec::with_capacity(self.buf.len() - from.off + self.hdr as usize);
        out.extend_from_slice(&self.buf[..self.hdr as usize]);
        let mut anchors = [0u16; ANCHORS];
        let mut prev = self.key.clone();
        let mut n = 0usize;
        let mut c = from;
        while !self.at_end(&c) {
            let start = c.off;
            let d = self.decode(&mut c, names);
            if !d.live() {
                continue;
            }
            if n > 0 && n.is_multiple_of(SEGMENT) {
                anchors[n / SEGMENT - 1] = out.len() as u16;
            }
            let base = if n.is_multiple_of(SEGMENT) {
                &self.key
            } else {
                &prev
            };
            let (dms, seq) = delta(&d.id, base);
            out.push(self.buf[start]);
            put(&mut out, dms);
            put(&mut out, seq);
            // The fields as they were, with their count if they have one.
            let mut p = start + 1;
            get(&self.buf, &mut p);
            get(&self.buf, &mut p);
            out.extend_from_slice(&self.buf[p..d.end]);
            prev = d.id;
            n += 1;
        }
        out.shrink_to_fit();
        self.buf = out;
        self.count = n as u8;
        self.live = n as u8;
        self.dead = 0;
        self.anchors = anchors;
    }
}

/// Owned field-value pairs, for the rare rewrites that need them.
type OwnedFields = Vec<(Vec<u8>, Vec<u8>)>;

/// Pack `items`, in ID order, into as few blocks as fit, evened out.
fn pack(items: &[(StreamId, OwnedFields)]) -> Vec<Block> {
    let greedy = pack_at_most(items, MAX_ENTRIES);
    if greedy.len() <= 1 {
        return greedy;
    }
    pack_at_most(items, items.len().div_ceil(greedy.len()))
}

fn pack_at_most(items: &[(StreamId, OwnedFields)], per_block: usize) -> Vec<Block> {
    let mut out: Vec<Block> = Vec::new();
    let mut prev = StreamId::min();
    for (id, fields) in items {
        let fits = out
            .last_mut()
            .is_some_and(|b| (b.count as usize) < per_block && b.try_push(id, &prev, fields));
        if !fits {
            if let Some(b) = out.last_mut() {
                b.buf.shrink_to_fit();
            }
            out.push(Block::start(id, fields));
        }
        prev = id.clone();
    }
    if let Some(b) = out.last_mut() {
        b.buf.shrink_to_fit();
    }
    out
}

// ── loading ──────────────────────────────────────────────────────────────────

/// A stream's entries as they are read from a file or a payload.
///
/// IDs may come in any order, and a repeated ID replaces the entry read
/// before it. In order, as KiviDB and Redis write them, they are appended as
/// they come; any out of order are put aside and the whole lot sorted and
/// packed once at the end.
#[derive(Default)]
pub struct LoadedEntries {
    entries: StreamEntries,
    staged: BTreeMap<StreamId, OwnedFields>,
}

impl LoadedEntries {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, id: StreamId, fields: &[(K, V)]) {
        let e = &mut self.entries;
        if self.staged.is_empty() && (e.blocks.is_empty() || id > e.tail_prev) {
            e.append(id, fields);
        } else {
            let fields = fields
                .iter()
                .map(|(k, v)| (k.as_ref().to_vec(), v.as_ref().to_vec()))
                .collect();
            self.staged.insert(id, fields);
        }
    }

    pub fn finish(mut self) -> StreamEntries {
        if self.staged.is_empty() {
            self.entries.seal();
            return self.entries;
        }
        let mut staged = std::mem::take(&mut self.staged).into_iter().peekable();
        let mut items: Vec<(StreamId, OwnedFields)> =
            Vec::with_capacity(self.entries.len() + staged.len());
        for (id, e) in self.entries.iter() {
            while let Some((sid, _)) = staged.peek() {
                if *sid > id {
                    break;
                }
                let (sid, f) = staged.next().unwrap();
                if sid == id {
                    // Read later: it replaces this one.
                    items.push((sid, f));
                    break;
                }
                items.push((sid, f));
            }
            if items.last().is_some_and(|(last, _)| *last == id) {
                continue;
            }
            let fields = e.fields().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
            items.push((id, fields));
        }
        items.extend(staged);
        let mut entries = StreamEntries::new();
        entries.len = items.len();
        if let Some((last, _)) = items.last() {
            entries.tail_prev = last.clone();
        }
        entries.blocks = pack(&items).into();
        entries
    }
}

// ── the entries ──────────────────────────────────────────────────────────────

/// A stream's entries, ordered by ID.
#[derive(Debug, Clone)]
pub struct StreamEntries {
    blocks: VecDeque<Block>,
    len: usize,
    /// The newest ID ever put in the last block: not before any entry.
    tail_prev: StreamId,
    /// Where scans of the first block begin, when entries at its front have
    /// been deleted: `head_off` is 0 when they begin at the start.
    head_prev: StreamId,
    head_off: u32,
    head_idx: u8,
    /// The last block takes appends; a block is sealed (shrunk to fit, and
    /// rewritten if a quarter of it is deleted) when the next one starts.
    open: bool,
}

/// One entry's field-value pairs, borrowed from the stream.
#[derive(Debug, Clone, Copy)]
pub struct EntryRef<'a> {
    /// The block's field names, used when `same`.
    names: &'a [u8],
    body: &'a [u8],
    n: u32,
    same: bool,
}

impl<'a> EntryRef<'a> {
    /// The field-value pairs in the order they were added.
    pub fn fields(self) -> Fields<'a> {
        Fields {
            names: self.names,
            body: self.body,
            left: self.n as usize,
            same: self.same,
        }
    }

    /// How many field-value pairs the entry has.
    pub fn field_count(self) -> usize {
        self.n as usize
    }
}

/// The field-value pairs of an [`EntryRef`].
#[derive(Debug, Clone)]
pub struct Fields<'a> {
    names: &'a [u8],
    body: &'a [u8],
    left: usize,
    same: bool,
}

impl<'a> Iterator for Fields<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let mut p = 0;
        let name = if self.same {
            let name = take(self.names, &mut p);
            self.names = &self.names[p..];
            p = 0;
            name
        } else {
            take(self.body, &mut p)
        };
        let value = take(self.body, &mut p);
        self.body = &self.body[p..];
        Some((name, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for Fields<'_> {}

type Item<'a> = (StreamId, EntryRef<'a>);

fn below(lo: &Bound<StreamId>, id: &StreamId) -> bool {
    match lo {
        Bound::Included(x) => id < x,
        Bound::Excluded(x) => id <= x,
        Bound::Unbounded => false,
    }
}

fn above(hi: &Bound<StreamId>, id: &StreamId) -> bool {
    match hi {
        Bound::Included(x) => id > x,
        Bound::Excluded(x) => id >= x,
        Bound::Unbounded => false,
    }
}

fn bound_id(b: &Bound<StreamId>) -> Option<&StreamId> {
    match b {
        Bound::Included(x) | Bound::Excluded(x) => Some(x),
        Bound::Unbounded => None,
    }
}

impl Default for StreamEntries {
    fn default() -> Self {
        StreamEntries {
            blocks: VecDeque::new(),
            len: 0,
            tail_prev: StreamId::min(),
            head_prev: StreamId::min(),
            head_off: 0,
            head_idx: 0,
            open: false,
        }
    }
}

impl StreamEntries {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn contains(&self, id: &StreamId) -> bool {
        self.locate(id).is_some()
    }

    pub fn get(&self, id: &StreamId) -> Option<EntryRef<'_>> {
        self.locate(id).map(|(bi, d, _)| self.blocks[bi].entry(&d))
    }

    /// The oldest entry.
    pub fn first(&self) -> Option<Item<'_>> {
        self.iter().next()
    }

    /// The newest entry.
    pub fn last(&self) -> Option<Item<'_>> {
        self.range_rev(..).next()
    }

    /// The ID of the oldest entry.
    pub fn first_id(&self) -> Option<StreamId> {
        self.first().map(|(id, _)| id)
    }

    /// The ID of the newest entry.
    pub fn last_id(&self) -> Option<StreamId> {
        self.last().map(|(id, _)| id)
    }

    /// The ID of the `n`th entry, oldest first, counting from 0. Whole
    /// blocks are passed over without reading their entries.
    pub fn nth_id(&self, mut n: usize) -> Option<StreamId> {
        for (bi, b) in self.blocks.iter().enumerate() {
            if n >= b.live as usize {
                n -= b.live as usize;
                continue;
            }
            let (names, _) = b.names();
            let mut c = self.begin(bi);
            while !b.at_end(&c) {
                let d = b.decode(&mut c, names);
                if d.live() {
                    if n == 0 {
                        return Some(d.id);
                    }
                    n -= 1;
                }
            }
        }
        None
    }

    /// All entries, oldest first.
    pub fn iter(&self) -> Range<'_> {
        self.range(..)
    }

    /// The entries with IDs in `range`, oldest first.
    pub fn range<R: RangeBounds<StreamId>>(&self, range: R) -> Range<'_> {
        let lo = range.start_bound().cloned();
        let hi = range.end_bound().cloned();
        let (bi, c) = match bound_id(&lo).map(|x| (x, self.floor(x))) {
            Some((x, Some(bi))) => (bi, self.seek(bi, x)),
            _ if self.blocks.is_empty() => (0, Cursor::start()),
            _ => (0, self.begin(0)),
        };
        Range {
            s: self,
            bi,
            c,
            lo,
            hi,
        }
    }

    /// The entries with IDs in `range`, newest first.
    pub fn range_rev<R: RangeBounds<StreamId>>(&self, range: R) -> RangeRev<'_> {
        let lo = range.start_bound().cloned();
        let hi = range.end_bound().cloned();
        let from = match bound_id(&hi) {
            Some(x) => self.floor(x).map(|bi| bi + 1).unwrap_or(0),
            None => self.blocks.len(),
        };
        RangeRev {
            s: self,
            lo,
            hi,
            unread: from,
            bi: 0,
            starts: std::array::from_fn(|_| Cursor::start()),
            nstarts: 0,
            slots: [const { None }; SEGMENT],
            nslots: 0,
            done: false,
        }
    }

    /// Add an entry from `XADD`, which has checked its ID against the
    /// stream's `last_id`.
    ///
    /// That is normally newer than every entry, and the entry is appended.
    /// But a stream loaded from a file or a payload can have a `last_id`
    /// below its newest entry, and XADD then lets through an ID that is not:
    /// the entry goes in ID order, replacing one with the same ID, as it
    /// always has.
    pub fn push_tail<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, id: StreamId, fields: &[(K, V)]) {
        if self.blocks.is_empty() || id > self.tail_prev {
            self.append(id, fields);
        } else {
            self.insert_sorted(id, fields);
        }
    }

    /// Remove the entry `id`; false if there is none.
    pub fn remove(&mut self, id: &StreamId) -> bool {
        match self.locate(id) {
            Some((bi, d, off)) => {
                self.kill(bi, off, &d);
                true
            }
            None => false,
        }
    }

    /// Remove the oldest entry, returning its ID.
    pub fn pop_first(&mut self) -> Option<StreamId> {
        let b = self.blocks.front()?;
        let (names, _) = b.names();
        let mut c = self.begin(0);
        loop {
            let off = c.off;
            let d = b.decode(&mut c, names);
            if d.live() {
                self.kill(0, off, &d);
                return Some(d.id);
            }
        }
    }

    /// Heap bytes held, as the allocator was asked for them.
    pub fn heap_bytes(&self) -> usize {
        self.blocks.capacity() * std::mem::size_of::<Block>()
            + self.blocks.iter().map(|b| b.buf.capacity()).sum::<usize>()
    }

    // ── internals ────────────────────────────────────────────────────────────

    /// The last block whose key is not after `id`.
    fn floor(&self, id: &StreamId) -> Option<usize> {
        self.blocks.partition_point(|b| b.key <= *id).checked_sub(1)
    }

    /// Where a scan of block `bi` begins.
    fn begin(&self, bi: usize) -> Cursor {
        if bi == 0 && self.head_off != 0 {
            Cursor {
                off: self.head_off as usize,
                idx: self.head_idx as usize,
                prev: self.head_prev.clone(),
            }
        } else {
            self.blocks[bi].at_start()
        }
    }

    /// The last place to begin a scan of block `bi` for `target`: its
    /// beginning, or the last anchor after that and not after `target`.
    fn seek(&self, bi: usize, target: &StreamId) -> Cursor {
        let mut at = self.begin(bi);
        let b = &self.blocks[bi];
        for j in 0..ANCHORS {
            let off = b.anchors[j] as usize;
            if off == 0 {
                break;
            }
            if off <= at.off {
                continue;
            }
            if b.anchor_id(j) > *target {
                break;
            }
            at = Cursor {
                off,
                idx: (j + 1) * SEGMENT,
                prev: b.key.clone(),
            };
        }
        at
    }

    /// The live entry `id`: its block, itself, and where it begins.
    fn locate(&self, id: &StreamId) -> Option<(usize, Decoded, usize)> {
        let bi = self.floor(id)?;
        let b = &self.blocks[bi];
        let (names, _) = b.names();
        let mut c = self.seek(bi, id);
        while !b.at_end(&c) {
            let off = c.off;
            let d = b.decode(&mut c, names);
            if d.id == *id {
                return d.live().then_some((bi, d, off));
            }
            if d.id > *id {
                return None;
            }
        }
        None
    }

    fn append<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, id: StreamId, fields: &[(K, V)]) {
        let pushed = self.open
            && self
                .blocks
                .back_mut()
                .is_some_and(|b| b.try_push(&id, &self.tail_prev, fields));
        if !pushed {
            self.seal();
            if self.blocks.capacity() == 0 {
                self.blocks.reserve_exact(1);
            }
            self.blocks.push_back(Block::start(&id, fields));
            self.open = true;
        }
        self.tail_prev = id;
        self.len += 1;
        if self.blocks.back().is_some_and(Block::full) {
            self.seal();
        }
    }

    /// Stop appending to the last block: shrink it to fit, and rewrite it if
    /// a quarter of it is deleted entries.
    fn seal(&mut self) {
        if !std::mem::take(&mut self.open) {
            return;
        }
        let bi = self.blocks.len() - 1;
        self.blocks[bi].buf.shrink_to_fit();
        if self.wasteful(bi) {
            self.compact(bi);
        }
    }

    /// Put `id` in ID order, replacing an entry with the same ID. Past the
    /// end of the block it falls in, it is added there or in a block of its
    /// own after it; inside, the block is rewritten.
    fn insert_sorted<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, id: StreamId, fields: &[(K, V)]) {
        let bi = self.floor(&id).unwrap_or(0);
        if bi + 1 == self.blocks.len() {
            self.seal();
        }
        let newest = {
            let b = &self.blocks[bi];
            let (names, _) = b.names();
            let mut c = b.at_start();
            let mut newest = None;
            while !b.at_end(&c) {
                newest = Some(b.decode(&mut c, names).id);
            }
            newest
        };
        if let Some(newest) = newest.filter(|n| id > *n) {
            let b = &mut self.blocks[bi];
            if b.try_push(&id, &newest, fields) {
                b.buf.shrink_to_fit();
            } else {
                self.blocks.insert(bi + 1, Block::start(&id, fields));
            }
            self.len += 1;
            return;
        }
        let owned = || -> OwnedFields {
            fields
                .iter()
                .map(|(k, v)| (k.as_ref().to_vec(), v.as_ref().to_vec()))
                .collect()
        };
        let mut items: Vec<(StreamId, OwnedFields)> = Vec::new();
        let mut placed = false;
        let mut replaced = false;
        {
            let b = &self.blocks[bi];
            let (names, _) = b.names();
            let mut c = self.begin(bi);
            while !b.at_end(&c) {
                let d = b.decode(&mut c, names);
                if !d.live() {
                    continue;
                }
                if !placed && d.id >= id {
                    items.push((id.clone(), owned()));
                    placed = true;
                    if d.id == id {
                        replaced = true;
                        continue;
                    }
                }
                let fields = b
                    .entry(&d)
                    .fields()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect();
                items.push((d.id, fields));
            }
        }
        if !placed {
            items.push((id.clone(), owned()));
        }
        let last = bi + 1 == self.blocks.len();
        if last {
            self.open = false;
        }
        if bi == 0 {
            self.reset_head();
        }
        self.blocks.remove(bi);
        for (i, block) in pack(&items).into_iter().enumerate() {
            self.blocks.insert(bi + i, block);
        }
        if id > self.tail_prev {
            self.tail_prev = id;
        }
        if !replaced {
            self.len += 1;
        }
    }

    /// Delete the entry `d`, at `off` in block `bi`.
    fn kill(&mut self, bi: usize, off: usize, d: &Decoded) {
        let b = &mut self.blocks[bi];
        b.buf[off] |= DELETED;
        b.live -= 1;
        b.dead += (d.end - off) as u32;
        self.len -= 1;
        if b.live == 0 {
            self.drop_block(bi);
            return;
        }
        if bi == 0 {
            self.skip_head();
        }
        let open_tail = self.open && bi + 1 == self.blocks.len();
        if !open_tail && self.wasteful(bi) {
            self.compact(bi);
        }
    }

    fn drop_block(&mut self, bi: usize) {
        if bi + 1 == self.blocks.len() {
            self.open = false;
        }
        self.blocks.remove(bi);
        if bi == 0 {
            self.reset_head();
            self.skip_head();
        }
        // Give back the slack once under half used: a doubling on the way
        // up, so no thrashing.
        let cap = self.blocks.capacity();
        if self.blocks.is_empty() || (cap > 4 && self.blocks.len() * 2 < cap) {
            self.blocks.shrink_to_fit();
        }
    }

    fn reset_head(&mut self) {
        self.head_off = 0;
        self.head_idx = 0;
        self.head_prev = StreamId::min();
    }

    /// Start scans of the first block past the deleted entries at its
    /// front, which then stop counting as dead.
    fn skip_head(&mut self) {
        let Some(b) = self.blocks.front_mut() else {
            return;
        };
        let (names, _) = b.names();
        let mut c = if self.head_off != 0 {
            Cursor {
                off: self.head_off as usize,
                idx: self.head_idx as usize,
                prev: self.head_prev.clone(),
            }
        } else {
            b.at_start()
        };
        let mut moved = false;
        loop {
            let at = c.clone();
            let d = b.decode(&mut c, names);
            if d.live() {
                c = at;
                break;
            }
            b.dead -= (d.end - at.off) as u32;
            moved = true;
        }
        if moved {
            self.head_off = c.off as u32;
            self.head_idx = c.idx as u8;
            self.head_prev = c.prev;
        }
    }

    /// Whether a quarter of block `bi`, past what the first block skips, is
    /// deleted entries.
    fn wasteful(&self, bi: usize) -> bool {
        let b = &self.blocks[bi];
        let skipped = if bi == 0 && self.head_off != 0 {
            self.head_off as usize - b.hdr as usize
        } else {
            0
        };
        b.dead as usize * 4 >= b.buf.len() - skipped && b.dead > 0
    }

    fn compact(&mut self, bi: usize) {
        let from = self.begin(bi);
        self.blocks[bi].compact(from);
        if bi == 0 {
            self.reset_head();
        }
    }

    /// Checks every invariant of the layout; for tests.
    #[cfg(test)]
    fn audit(&self) {
        let mut live = 0;
        let mut last: Option<StreamId> = None;
        for (bi, b) in self.blocks.iter().enumerate() {
            let open_tail = self.open && bi + 1 == self.blocks.len();
            assert!(b.live > 0, "block {bi} has nothing live");
            assert!(b.live <= b.count && b.count as usize <= MAX_ENTRIES);
            if b.count > 1 {
                assert!(b.buf.len() <= MAX_BYTES, "block {bi}: {} B", b.buf.len());
            }
            if let Some(next) = self.blocks.get(bi + 1) {
                assert!(b.key < next.key, "block keys out of order at {bi}");
            }
            if !open_tail {
                assert_eq!(b.buf.capacity(), b.buf.len(), "block {bi} not shrunk");
                assert!(!self.wasteful(bi), "block {bi} is a quarter deleted");
            }
            let (names, _) = b.names();
            // Every physical entry, from the block's start.
            let mut c = b.at_start();
            let mut idx = 0;
            let (mut seen_live, mut dead, mut count) = (0, 0, 0);
            let mut prev_in_block: Option<StreamId> = None;
            while !b.at_end(&c) {
                if idx > 0 && idx % SEGMENT == 0 {
                    assert_eq!(b.anchors[idx / SEGMENT - 1] as usize, c.off, "anchor");
                }
                let off = c.off;
                let d = b.decode(&mut c, names);
                assert!(d.id >= b.key, "entry before its block's key");
                if let Some(p) = &prev_in_block {
                    assert!(d.id > *p, "entries out of order in block {bi}");
                }
                prev_in_block = Some(d.id.clone());
                assert!(d.id <= self.tail_prev, "entry after tail_prev");
                let skipped = bi == 0 && self.head_off != 0 && off < self.head_off as usize;
                if skipped {
                    assert!(!d.live(), "a skipped entry is live");
                } else if d.live() {
                    if let Some(l) = &last {
                        assert!(d.id > *l, "IDs out of order");
                    }
                    last = Some(d.id.clone());
                    seen_live += 1;
                } else {
                    dead += d.end - off;
                }
                if bi == 0 && off == self.head_off as usize {
                    assert_eq!(c.idx - 1, self.head_idx as usize);
                }
                idx += 1;
                count += 1;
            }
            for j in (count.max(1) - 1) / SEGMENT..ANCHORS {
                if (j + 1) * SEGMENT >= count {
                    assert_eq!(b.anchors[j], 0, "stale anchor");
                }
            }
            if let Some(next) = self.blocks.get(bi + 1) {
                assert!(prev_in_block.unwrap() < next.key);
            }
            assert_eq!(count, b.count as usize);
            assert_eq!(seen_live, b.live as usize);
            assert_eq!(dead, b.dead as usize, "dead bytes in block {bi}");
            live += seen_live;
        }
        if let Some(b) = self.blocks.front() {
            let (names, _) = b.names();
            assert!(
                b.decode(&mut self.begin(0), names).live(),
                "head is deleted"
            );
        }
        if self.open {
            let b = self.blocks.back().unwrap();
            let (names, _) = b.names();
            let mut c = b.at_start();
            let mut newest = None;
            while !b.at_end(&c) {
                newest = Some(b.decode(&mut c, names).id);
            }
            assert_eq!(newest.as_ref(), Some(&self.tail_prev), "open tail's newest");
        }
        assert_eq!(live, self.len);
    }
}

impl Cursor {
    fn start() -> Cursor {
        Cursor {
            off: 0,
            idx: 0,
            prev: StreamId::min(),
        }
    }
}

/// Entries oldest first: [`StreamEntries::range`].
pub struct Range<'a> {
    s: &'a StreamEntries,
    bi: usize,
    c: Cursor,
    lo: Bound<StreamId>,
    hi: Bound<StreamId>,
}

impl<'a> Iterator for Range<'a> {
    type Item = Item<'a>;

    fn next(&mut self) -> Option<Item<'a>> {
        loop {
            let b = self.s.blocks.get(self.bi)?;
            if b.at_end(&self.c) {
                self.bi += 1;
                if self.bi < self.s.blocks.len() {
                    self.c = self.s.begin(self.bi);
                }
                continue;
            }
            let (names, _) = b.names();
            let d = b.decode(&mut self.c, names);
            if !d.live() || below(&self.lo, &d.id) {
                continue;
            }
            if above(&self.hi, &d.id) {
                self.bi = self.s.blocks.len();
                return None;
            }
            return Some((d.id.clone(), b.entry(&d)));
        }
    }
}

/// Entries newest first: [`StreamEntries::range_rev`].
///
/// IDs are stored against the entry before, so a block is read a segment
/// (from one anchor to the next) at a time, forward, and the segment's
/// entries given back in reverse. Nothing is allocated.
pub struct RangeRev<'a> {
    s: &'a StreamEntries,
    lo: Bound<StreamId>,
    hi: Bound<StreamId>,
    /// Blocks not yet read: those before this one.
    unread: usize,
    /// The block being read.
    bi: usize,
    /// Where its unread segments begin, oldest first: `starts[0]` is where
    /// the block's scan begins, the rest are anchors.
    starts: [Cursor; ANCHORS + 1],
    nstarts: usize,
    /// The entries of the segment being given back, oldest first.
    slots: [Option<Decoded>; SEGMENT],
    nslots: usize,
    done: bool,
}

impl<'a> Iterator for RangeRev<'a> {
    type Item = Item<'a>;

    fn next(&mut self) -> Option<Item<'a>> {
        loop {
            if self.nslots > 0 {
                self.nslots -= 1;
                let d = self.slots[self.nslots].take()?;
                return Some((d.id.clone(), self.s.blocks[self.bi].entry(&d)));
            }
            if self.done {
                return None;
            }
            if self.nstarts > 0 {
                self.nstarts -= 1;
                let start = self.starts[self.nstarts].clone();
                self.read_segment(start);
            } else if self.unread == 0 {
                self.done = true;
            } else {
                self.load_block();
            }
        }
    }
}

impl RangeRev<'_> {
    /// Move to the block before, listing where its segments begin.
    fn load_block(&mut self) {
        self.unread -= 1;
        self.bi = self.unread;
        let b = &self.s.blocks[self.bi];
        let begin = self.s.begin(self.bi);
        let begin_off = begin.off;
        self.starts[0] = begin;
        self.nstarts = 1;
        for j in 0..ANCHORS {
            let off = b.anchors[j] as usize;
            if off == 0 || above(&self.hi, &b.anchor_id(j)) {
                break;
            }
            if off > begin_off {
                self.starts[self.nstarts] = Cursor {
                    off,
                    idx: (j + 1) * SEGMENT,
                    prev: b.key.clone(),
                };
                self.nstarts += 1;
            }
        }
    }

    /// Read the segment at `c` of the current block into `slots`.
    fn read_segment(&mut self, mut c: Cursor) {
        let b = &self.s.blocks[self.bi];
        let end = b
            .anchors
            .iter()
            .map(|&a| a as usize)
            .find(|&a| a > c.off)
            .unwrap_or(b.buf.len());
        let (names, _) = b.names();
        while c.off < end {
            let d = b.decode(&mut c, names);
            if !d.live() {
                continue;
            }
            if below(&self.lo, &d.id) {
                // So is everything before it.
                self.done = true;
                continue;
            }
            if above(&self.hi, &d.id) {
                break;
            }
            self.slots[self.nslots] = Some(d);
            self.nslots += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId::new(ms, seq)
    }

    fn ids<'a>(it: impl Iterator<Item = Item<'a>>) -> Vec<StreamId> {
        it.map(|(id, _)| id).collect()
    }

    fn fields_of(e: EntryRef<'_>) -> Pairs {
        e.fields().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
    }

    fn fv(v: &[u8]) -> Pairs {
        vec![(b"f".to_vec(), v.to_vec())]
    }

    /// 1-0, 1-1, 2-0, 5-7
    fn sample() -> StreamEntries {
        let mut s = StreamEntries::new();
        for (ms, seq) in [(1, 0), (1, 1), (2, 0), (5, 7)] {
            s.push_tail(id(ms, seq), &fv(b"v"));
        }
        s
    }

    #[test]
    fn an_empty_stream_has_nothing_in_any_range() {
        let s = StreamEntries::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!((s.first_id(), s.last_id()), (None, None));
        assert!(s.get(&id(1, 0)).is_none());
        assert_eq!(s.iter().count(), 0);
        assert_eq!(s.range(..).count(), 0);
        assert_eq!(s.range_rev(..).count(), 0);
        assert_eq!(s.nth_id(0), None);
        assert_eq!(s.heap_bytes(), 0);
    }

    #[test]
    fn entries_come_back_in_id_order_both_ways() {
        let s = sample();
        let all = vec![id(1, 0), id(1, 1), id(2, 0), id(5, 7)];
        assert_eq!(ids(s.iter()), all);
        assert_eq!(ids(s.range(..)), all);
        let mut back = all.clone();
        back.reverse();
        assert_eq!(ids(s.range_rev(..)), back);
        assert_eq!(
            (s.first_id(), s.last_id()),
            (Some(id(1, 0)), Some(id(5, 7)))
        );
        assert_eq!(s.nth_id(2), Some(id(2, 0)));
        assert_eq!(s.nth_id(4), None);
        s.audit();
    }

    #[test]
    fn ranges_honour_inclusive_and_exclusive_bounds() {
        let s = sample();
        assert_eq!(ids(s.range(id(1, 1)..=id(2, 0))), [id(1, 1), id(2, 0)]);
        assert_eq!(ids(s.range(id(1, 1)..id(2, 0))), [id(1, 1)]);
        assert_eq!(
            ids(s.range((Excluded(id(1, 1)), Unbounded))),
            [id(2, 0), id(5, 7)]
        );
        assert_eq!(
            ids(s.range_rev(..=id(2, 0))),
            [id(2, 0), id(1, 1), id(1, 0)]
        );
        assert_eq!(
            ids(s.range_rev((Excluded(id(1, 0)), Included(id(5, 7))))),
            [id(5, 7), id(2, 0), id(1, 1)]
        );
        assert_eq!(ids(s.range(id(1, 2)..=id(4, 0))), [id(2, 0)]);
        assert_eq!(s.range(id(6, 0)..).count(), 0);
        assert_eq!(s.range_rev(..id(1, 0)).count(), 0);
    }

    #[test]
    fn fields_are_returned_byte_exact_and_in_order() {
        let mut s = StreamEntries::new();
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"", b"\0\xff"), (b"a", b"")];
        s.push_tail(id(1, 0), pairs);
        // The same names again, then other names: stored without and with
        // their names.
        s.push_tail(id(2, 0), pairs);
        s.push_tail(id(3, 0), &[(&b"\xfe"[..], &b"x"[..])]);
        for ms in 1..=2 {
            let e = s.get(&id(ms, 0)).unwrap();
            assert_eq!(e.field_count(), 3);
            assert_eq!(e.fields().len(), 3);
            assert_eq!(e.fields().collect::<Vec<_>>(), pairs);
        }
        let e = s.get(&id(3, 0)).unwrap();
        assert_eq!(e.fields().collect::<Vec<_>>(), [(&b"\xfe"[..], &b"x"[..])]);
        assert!(s.get(&id(1, 1)).is_none());
        assert!(!s.contains(&id(0, 9)));
        s.audit();
    }

    #[test]
    fn remove_and_pop_first_report_what_they_removed() {
        let mut s = sample();
        assert!(s.remove(&id(1, 1)));
        assert!(!s.remove(&id(1, 1)));
        assert!(!s.remove(&id(3, 0)));
        s.audit();
        assert_eq!(s.pop_first(), Some(id(1, 0)));
        assert_eq!(ids(s.iter()), [id(2, 0), id(5, 7)]);
        assert_eq!(s.pop_first(), Some(id(2, 0)));
        assert_eq!(s.pop_first(), Some(id(5, 7)));
        assert_eq!(s.pop_first(), None);
        assert!(s.is_empty());
        s.audit();
        assert_eq!(s.heap_bytes(), 0, "an emptied stream holds nothing");
    }

    #[test]
    fn loading_takes_ids_in_any_order_and_a_repeat_replaces() {
        let mut l = LoadedEntries::new();
        l.insert(id(5, 0), &fv(b"first"));
        l.insert(id(2, 0), &fv(b"x"));
        l.insert(id(5, 0), &fv(b"second"));
        let s = l.finish();
        assert_eq!(ids(s.iter()), [id(2, 0), id(5, 0)]);
        assert_eq!(fields_of(s.get(&id(5, 0)).unwrap()), fv(b"second"));
        s.audit();
    }

    #[test]
    fn appending_below_the_newest_loaded_entry_keeps_id_order() {
        // A loaded stream whose `last_id` (2-0) is below its newest entry
        // lets XADD through with 3-0 and 5-0.
        let mut l = LoadedEntries::new();
        l.insert(id(1, 0), &fv(b"a"));
        l.insert(id(5, 0), &fv(b"loaded"));
        let mut s = l.finish();
        s.push_tail(id(3, 0), &fv(b"between"));
        assert_eq!(ids(s.iter()), [id(1, 0), id(3, 0), id(5, 0)]);
        assert_eq!(ids(s.range_rev(..)), [id(5, 0), id(3, 0), id(1, 0)]);
        assert_eq!(s.last_id(), Some(id(5, 0)));
        s.audit();

        s.push_tail(id(5, 0), &fv(b"added"));
        assert_eq!(s.len(), 3);
        assert_eq!(fields_of(s.get(&id(5, 0)).unwrap()), fv(b"added"));
        // And appends after it go on as usual.
        s.push_tail(id(6, 0), &fv(b"next"));
        assert_eq!(ids(s.iter()), [id(1, 0), id(3, 0), id(5, 0), id(6, 0)]);
        s.audit();
    }

    #[test]
    fn blocks_end_at_64_entries_or_2_kib_and_a_big_entry_is_alone() {
        let mut s = StreamEntries::new();
        for ms in 1..=200 {
            s.push_tail(id(ms, 0), &fv(b"v"));
        }
        let counts: Vec<u8> = s.blocks.iter().map(|b| b.count).collect();
        assert_eq!(counts, [64, 64, 64, 8]);
        s.audit();

        let mut s = StreamEntries::new();
        for ms in 1..=40 {
            s.push_tail(id(ms, 0), &fv(&[7u8; 100]));
        }
        assert!(s.blocks.iter().all(|b| b.buf.len() <= MAX_BYTES));
        assert_eq!(s.blocks[0].count, 19);
        s.push_tail(id(41, 0), &fv(&[1u8; 5000]));
        s.push_tail(id(42, 0), &fv(b"small"));
        let n = s.blocks.len();
        assert_eq!(s.blocks[n - 2].count, 1, "the big entry is alone");
        assert_eq!(
            ids(s.range_rev(id(40, 0)..)),
            [id(42, 0), id(41, 0), id(40, 0)]
        );
        s.audit();
    }

    #[test]
    fn ids_far_apart_and_at_the_extremes() {
        let mut s = StreamEntries::new();
        let all = [
            id(0, 1),
            id(0, u64::MAX - 1),
            id(0, u64::MAX),
            id(1, 0),
            id(1 << 40, 0),
            id(1 << 40, 1 << 60),
            id(u64::MAX - 1, u64::MAX),
            id(u64::MAX, 0),
            id(u64::MAX, u64::MAX),
        ];
        for x in &all {
            s.push_tail(x.clone(), &fv(b"v"));
        }
        assert_eq!(ids(s.iter()), all);
        let mut back = all.to_vec();
        back.reverse();
        assert_eq!(ids(s.range_rev(..)), back);
        for x in &all {
            assert!(s.contains(x));
        }
        s.audit();
    }

    #[test]
    fn deleting_reclaims_whole_blocks_and_rewrites_wasteful_ones() {
        let mut s = StreamEntries::new();
        for ms in 1..=1000 {
            s.push_tail(id(ms, 0), &fv(b"value"));
        }
        let full = s.heap_bytes();
        // Every other entry: each sealed block is rewritten as it passes a
        // quarter deleted.
        for ms in (2..=1000).step_by(2) {
            assert!(s.remove(&id(ms, 0)));
            s.audit();
        }
        assert!(
            s.heap_bytes() < full * 3 / 4,
            "{} of {full}",
            s.heap_bytes()
        );
        // The rest, from the back: every block goes.
        for ms in (1..=999).rev().step_by(2) {
            assert!(s.remove(&id(ms, 0)));
        }
        s.audit();
        assert!(s.is_empty());
        assert_eq!(s.heap_bytes(), 0);
        // Added and deleted at once: nothing is kept.
        for ms in 2000..3000 {
            s.push_tail(id(ms, 0), &fv(b"value"));
            assert!(s.remove(&id(ms, 0)));
        }
        assert_eq!(s.heap_bytes(), 0);
        s.audit();
    }

    #[test]
    fn trimming_the_front_skips_instead_of_rewriting() {
        let mut s = StreamEntries::new();
        for ms in 1..=100 {
            s.push_tail(id(ms, 0), &fv(b"v"));
        }
        let buf = s.blocks[0].buf.as_ptr();
        for ms in 1..=40 {
            assert_eq!(s.pop_first(), Some(id(ms, 0)));
            s.audit();
        }
        assert_eq!(
            s.blocks[0].buf.as_ptr(),
            buf,
            "the first block is not rewritten"
        );
        assert_eq!(s.first_id(), Some(id(41, 0)));
        assert_eq!(ids(s.range_rev(..=id(42, 0))), [id(42, 0), id(41, 0)]);
        assert_eq!(s.nth_id(0), Some(id(41, 0)));
        // A deleted entry just past the skipped ones is skipped too.
        assert!(s.remove(&id(42, 0)));
        assert!(s.remove(&id(41, 0)));
        assert_eq!(s.first_id(), Some(id(43, 0)));
        s.audit();
        for ms in 43..=64 {
            assert_eq!(s.pop_first(), Some(id(ms, 0)));
        }
        assert_eq!(s.blocks.len(), 1, "the emptied block went");
        s.audit();
    }

    /// A small deterministic generator.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    fn random_fields(r: &mut Rng) -> Pairs {
        let names: [&[u8]; 5] = [b"f", b"", b"\x00\xff", b"name", b"f"];
        match r.below(10) {
            0 => Vec::new(),
            1 => vec![(
                b"big".to_vec(),
                vec![r.next() as u8; 2000 + r.below(4000) as usize],
            )],
            2..=5 => vec![(b"f".to_vec(), vec![b'v'; r.below(20) as usize])],
            _ => (0..1 + r.below(5))
                .map(|_| {
                    let k = names[r.below(5) as usize].to_vec();
                    let v = (0..r.below(30)).map(|_| r.next() as u8).collect();
                    (k, v)
                })
                .collect(),
        }
    }

    fn random_id(r: &mut Rng, after: &StreamId) -> StreamId {
        match r.below(8) {
            0 if after.seq < u64::MAX => id(after.ms, after.seq + 1),
            1 if after.seq < u64::MAX - (1 << 60) => id(after.ms, after.seq + (1 << 60)),
            2 if after.ms < u64::MAX - (1 << 40) => id(after.ms + (1 << 40), r.next()),
            3 if after.ms < u64::MAX => id(after.ms + 1, u64::MAX - r.below(3)),
            _ if after.ms < u64::MAX - 1000 => id(after.ms + 1 + r.below(1000), r.below(4)),
            _ => id(u64::MAX, u64::MAX),
        }
    }

    fn some_id(r: &mut Rng, model: &BTreeMap<StreamId, Pairs>) -> StreamId {
        let n = model.len();
        match model.keys().nth(r.below(n as u64 + 1) as usize) {
            Some(x) if r.below(4) > 0 => x.clone(),
            Some(x) => id(x.ms, x.seq.wrapping_add(1)),
            None => id(r.below(1 << 20), r.below(4)),
        }
    }

    fn check(s: &StreamEntries, model: &BTreeMap<StreamId, Pairs>, r: &mut Rng) {
        s.audit();
        assert_eq!(s.len(), model.len());
        let all: Vec<_> = model.keys().cloned().collect();
        assert_eq!(ids(s.iter()), all);
        let mut back = all.clone();
        back.reverse();
        assert_eq!(ids(s.range_rev(..)), back);
        for (k, v) in model {
            assert_eq!(&fields_of(s.get(k).unwrap()), v);
        }
        assert_eq!(s.first_id(), all.first().cloned());
        assert_eq!(s.last_id(), all.last().cloned());
        for _ in 0..20 {
            let (a, b) = (some_id(r, model), some_id(r, model));
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            let bounds = [
                (Included(lo.clone()), Included(hi.clone())),
                (Excluded(lo.clone()), Included(hi.clone())),
                (Included(lo.clone()), Excluded(hi.clone())),
                (Unbounded, Included(hi.clone())),
                (Excluded(lo.clone()), Unbounded),
            ];
            for bound in bounds {
                if matches!(&bound, (Excluded(x), Excluded(y)) if x == y) {
                    continue;
                }
                let want: Vec<_> = model.range(bound.clone()).map(|(k, _)| k.clone()).collect();
                assert_eq!(ids(s.range(bound.clone())), want, "{bound:?}");
                let rev: Vec<_> = want.iter().rev().cloned().collect();
                assert_eq!(ids(s.range_rev(bound.clone())), rev, "rev {bound:?}");
                for (got, (k, v)) in s.range(bound.clone()).zip(model.range(bound)) {
                    assert_eq!(&got.0, k);
                    assert_eq!(&fields_of(got.1), v);
                }
            }
            let n = r.below(model.len() as u64 + 2) as usize;
            assert_eq!(s.nth_id(n), all.get(n).cloned());
        }
    }

    #[test]
    fn matches_an_ordered_map_under_random_changes() {
        for seed in 1..=40u64 {
            let mut r = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            let mut s = StreamEntries::new();
            let mut model: BTreeMap<StreamId, Pairs> = BTreeMap::new();
            let mut top = StreamId::min();
            for step in 0..1500 {
                match r.below(20) {
                    0..=9 => {
                        let next = random_id(&mut r, &top);
                        if next > top {
                            let f = random_fields(&mut r);
                            s.push_tail(next.clone(), &f);
                            model.insert(next.clone(), f);
                            top = next;
                        }
                    }
                    10 => {
                        // Not after the newest (a stream loaded with a low
                        // `last_id`, or entries loaded out of order).
                        let x = some_id(&mut r, &model);
                        let f = random_fields(&mut r);
                        s.push_tail(x.clone(), &f);
                        model.insert(x.clone(), f);
                        if x > top {
                            top = x;
                        }
                    }
                    11..=14 => {
                        let x = some_id(&mut r, &model);
                        assert_eq!(s.remove(&x), model.remove(&x).is_some());
                    }
                    15..=17 => {
                        let n = r.below(80);
                        for _ in 0..n {
                            assert_eq!(s.pop_first(), model.pop_first().map(|(k, _)| k));
                        }
                    }
                    _ => {
                        // Every entry of a stretch.
                        let x = some_id(&mut r, &model);
                        let doomed: Vec<_> =
                            model.range(x..).take(70).map(|(k, _)| k.clone()).collect();
                        for k in doomed {
                            assert!(s.remove(&k));
                            model.remove(&k);
                        }
                    }
                }
                if step % 100 == 99 {
                    check(&s, &model, &mut r);
                }
            }
            check(&s, &model, &mut r);
            while s.pop_first().is_some() {}
            assert_eq!(s.heap_bytes(), 0);
        }
    }

    #[test]
    fn many_entries_loaded_backwards_end_up_in_fair_sized_blocks() {
        let mut l = LoadedEntries::new();
        for ms in (1..=2000).rev() {
            l.insert(id(ms, 0), &fv(b"v"));
        }
        let s = l.finish();
        s.audit();
        assert_eq!(s.len(), 2000);
        assert_eq!(s.first_id(), Some(id(1, 0)));
        let avg = 2000 / s.blocks.len();
        assert!(avg >= 30, "{avg} entries a block");
    }

    #[test]
    fn loads_in_any_order_with_repeats_agree_with_a_model() {
        for seed in 1..=40 {
            let mut r = Rng(seed);
            let mut l = LoadedEntries::new();
            let mut model = BTreeMap::new();
            let n = r.below(3000) as usize;
            // In order for a while, then not, as a crafted payload might be.
            let sorted_for = r.below(n as u64 + 1) as usize;
            for i in 0..n {
                let x = if i < sorted_for {
                    id(i as u64 * 3 + 1, 0)
                } else {
                    id(r.below(n as u64 * 3 + 2), r.below(3))
                };
                let f = random_fields(&mut r);
                l.insert(x.clone(), &f);
                model.insert(x, f);
            }
            let s = l.finish();
            s.audit();
            assert_eq!(s.len(), model.len());
            let got: Vec<_> = s.iter().map(|(k, e)| (k, fields_of(e))).collect();
            let want: Vec<_> = model.into_iter().collect();
            assert_eq!(got, want, "seed {seed}");
        }
    }

    #[test]
    fn inserting_past_a_big_entry_does_not_copy_it() {
        // A big lone entry, then a `last_id` below it lets XADD through
        // with IDs after it but before `tail_prev`.
        let mut s = StreamEntries::new();
        s.push_tail(id(1, 0), &[(b"big".as_slice(), vec![7u8; 5000])]);
        s.push_tail(id(1000, 0), &fv(b"v"));
        s.remove(&id(1000, 0));
        for ms in 2..500 {
            s.push_tail(id(ms, 0), &fv(b"v"));
        }
        s.audit();
        assert_eq!(s.len(), 499);
        assert!(s.blocks.len() <= 10, "{} blocks", s.blocks.len());
        assert_eq!(s.blocks[0].count, 1, "the big entry was rewritten");
    }

    #[test]
    fn a_large_load_in_reverse_is_quick() {
        let t = std::time::Instant::now();
        let mut l = LoadedEntries::new();
        for ms in (1..=200_000).rev() {
            l.insert(id(ms, 0), &fv(b"v"));
        }
        let s = l.finish();
        assert_eq!(s.len(), 200_000);
        assert!(t.elapsed().as_secs() < 5, "{:?}", t.elapsed());
    }

    #[test]
    fn cloned_entries_are_equal_and_independent() {
        let mut s = sample();
        let c = s.clone();
        s.remove(&id(2, 0));
        assert_eq!(ids(c.iter()), [id(1, 0), id(1, 1), id(2, 0), id(5, 7)]);
        c.audit();
    }
}
