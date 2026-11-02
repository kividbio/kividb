// TAG and NUMERIC postings: which documents hold which tag, and which hold a
// number in a range, without reading the documents.
//
// Each document an index covers is given a small number, its id, for as long
// as it is in the index: ids are what the sets below hold, so that a set of
// documents is a bitmap or a sorted list of `u32`s, and two of them are
// intersected word by word. A TAG field keeps, for each tag, the set of
// documents that hold it; a NUMERIC field keeps its values sorted, so that a
// range is read off in order. Both keep what each document put in, so that a
// document written again, or gone, is taken out of exactly the sets it was in.
//
// A query's TAG and NUMERIC clauses, and `AND`, `OR` and `NOT` of them, are
// then a set of ids computed from these (see `commands::search::plan`),
// instead of a filter tried on every document. An index without a TAG or a
// NUMERIC field keeps none of this; it gives out ids only if it has a TEXT
// field, whose index (`text`) is in terms of them.
//
// What a document holds is read as the filter reads it (`filter::eval`): a
// TAG value split at the field's separator, trimmed, lower-cased unless the
// field is CASESENSITIVE; a NUMERIC value as the number its text parses to.
// The two must agree, or a query would answer differently with postings than
// without them.

use super::{FieldType, SchemaField};
use ahash::AHashMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

/// A node of a graph whose document id is not known (yet): see
/// [`super::HnswNode::doc`].
pub const NO_DOC: u32 = u32::MAX;

// ── Bitmaps ──────────────────────────────────────────────────────────────────

/// A set of document ids, one bit each.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Bits {
    words: Vec<u64>,
}

impl Bits {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn set(&mut self, id: u32) {
        let (w, b) = ((id / 64) as usize, id % 64);
        if w >= self.words.len() {
            self.words.resize(w + 1, 0);
        }
        self.words[w] |= 1 << b;
    }

    #[inline]
    pub fn unset(&mut self, id: u32) {
        let (w, b) = ((id / 64) as usize, id % 64);
        if let Some(word) = self.words.get_mut(w) {
            *word &= !(1 << b);
        }
    }

    #[inline]
    pub fn get(&self, id: u32) -> bool {
        let (w, b) = ((id / 64) as usize, id % 64);
        self.words.get(w).is_some_and(|word| word & (1 << b) != 0)
    }

    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|w| *w == 0)
    }

    pub fn and_with(&mut self, other: &Bits) {
        self.words.truncate(other.words.len());
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= b;
        }
    }

    pub fn or_with(&mut self, other: &Bits) {
        if other.words.len() > self.words.len() {
            self.words.resize(other.words.len(), 0);
        }
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a |= b;
        }
    }

    /// Take out of this set every id that is in `other`.
    pub fn and_not(&mut self, other: &Bits) {
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= !b;
        }
    }

    /// The ids, in increasing order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(w, &word)| {
            let mut rest = word;
            std::iter::from_fn(move || {
                if rest == 0 {
                    return None;
                }
                let b = rest.trailing_zeros();
                rest &= rest - 1;
                Some(w as u32 * 64 + b)
            })
        })
    }

    /// Bytes held.
    pub fn bytes(&self) -> usize {
        self.words.capacity() * 8
    }
}

/// The documents that hold one tag: a sorted list while they are few, a
/// bitmap once the list would be the larger of the two.
#[derive(Debug, Clone)]
enum IdSet {
    Sparse(Vec<u32>),
    Dense { bits: Bits, len: usize },
}

impl Default for IdSet {
    fn default() -> Self {
        IdSet::Sparse(Vec::new())
    }
}

impl IdSet {
    fn len(&self) -> usize {
        match self {
            IdSet::Sparse(ids) => ids.len(),
            IdSet::Dense { len, .. } => *len,
        }
    }

    /// Add `id`. `slots` is how many ids there are, which a bitmap costs.
    fn insert(&mut self, id: u32, slots: usize) {
        match self {
            IdSet::Sparse(ids) => {
                if let Err(at) = ids.binary_search(&id) {
                    ids.insert(at, id);
                }
                // A list of n ids costs 32n bits; a bitmap one bit a slot.
                if ids.len() >= 64 && ids.len() * 32 > slots {
                    let mut bits = Bits::new();
                    for &id in ids.iter() {
                        bits.set(id);
                    }
                    *self = IdSet::Dense {
                        len: ids.len(),
                        bits,
                    };
                }
            }
            IdSet::Dense { bits, len } => {
                if !bits.get(id) {
                    bits.set(id);
                    *len += 1;
                }
            }
        }
    }

    fn remove(&mut self, id: u32, slots: usize) {
        match self {
            IdSet::Sparse(ids) => {
                if let Ok(at) = ids.binary_search(&id) {
                    ids.remove(at);
                }
            }
            IdSet::Dense { bits, len } => {
                if bits.get(id) {
                    bits.unset(id);
                    *len -= 1;
                }
                // Back to a list well below the point it became a bitmap, so
                // that a set at the boundary does not flip back and forth.
                if *len * 64 < slots {
                    *self = IdSet::Sparse(bits.iter().collect());
                }
            }
        }
    }

    fn or_into(&self, out: &mut Bits) {
        match self {
            IdSet::Sparse(ids) => ids.iter().for_each(|&id| out.set(id)),
            IdSet::Dense { bits, .. } => out.or_with(bits),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            IdSet::Sparse(ids) => ids.capacity() * 4,
            IdSet::Dense { bits, .. } => bits.bytes(),
        }
    }
}

// ── TAG ──────────────────────────────────────────────────────────────────────

/// What one document put into a TAG field: the ids of its tags.
#[derive(Debug, Clone, Default)]
enum Held {
    #[default]
    Nothing,
    One(u32),
    Many(Box<[u32]>),
}

impl Held {
    fn from_vec(mut tags: Vec<u32>) -> Self {
        tags.sort_unstable();
        tags.dedup();
        match tags.as_slice() {
            [] => Held::Nothing,
            [one] => Held::One(*one),
            _ => Held::Many(tags.into_boxed_slice()),
        }
    }

    fn as_slice(&self) -> &[u32] {
        match self {
            Held::Nothing => &[],
            Held::One(one) => std::slice::from_ref(one),
            Held::Many(many) => many,
        }
    }
}

#[derive(Debug, Clone)]
struct TagPostings {
    separator: char,
    case_sensitive: bool,
    /// Tag → its number, by which the sets and documents refer to it.
    numbers: AHashMap<Box<str>, u32>,
    /// By tag number: the tag and the documents holding it. A tag no
    /// document holds any more gives its number back (`free`).
    tags: Vec<(Box<str>, IdSet)>,
    free: Vec<u32>,
    /// By document id: what it holds.
    docs: Vec<Held>,
}

impl TagPostings {
    fn new(separator: char, case_sensitive: bool) -> Self {
        Self {
            separator,
            case_sensitive,
            numbers: AHashMap::new(),
            tags: Vec::new(),
            free: Vec::new(),
            docs: Vec::new(),
        }
    }

    /// The tags of a value, as `filter::eval` reads them.
    fn parse(&self, value: Option<&[u8]>) -> Vec<String> {
        let Some(text) = value.and_then(|v| std::str::from_utf8(v).ok()) else {
            return Vec::new();
        };
        super::filter::split_tags(text, self.separator)
            .map(|tag| {
                if self.case_sensitive {
                    tag.to_string()
                } else {
                    tag.to_lowercase()
                }
            })
            .collect()
    }

    fn number_of(&mut self, tag: String) -> u32 {
        if let Some(&n) = self.numbers.get(tag.as_str()) {
            return n;
        }
        let tag: Box<str> = tag.into_boxed_str();
        let n = match self.free.pop() {
            Some(n) => {
                self.tags[n as usize] = (tag.clone(), IdSet::default());
                n
            }
            None => {
                self.tags.push((tag.clone(), IdSet::default()));
                (self.tags.len() - 1) as u32
            }
        };
        self.numbers.insert(tag, n);
        n
    }

    fn set(&mut self, doc: u32, value: Option<&[u8]>, slots: usize) {
        let tags = self.parse(value);
        let numbers: Vec<u32> = tags.into_iter().map(|t| self.number_of(t)).collect();
        let new = Held::from_vec(numbers);
        let at = doc as usize;
        if at >= self.docs.len() {
            self.docs.resize(at + 1, Held::Nothing);
        }
        let old = std::mem::replace(&mut self.docs[at], new);
        let new = self.docs[at].as_slice().to_vec();
        for &n in old.as_slice() {
            if !new.contains(&n) {
                self.drop_from(n, doc, slots);
            }
        }
        for &n in &new {
            if !old.as_slice().contains(&n) {
                self.tags[n as usize].1.insert(doc, slots);
            }
        }
    }

    fn drop_from(&mut self, n: u32, doc: u32, slots: usize) {
        let set = &mut self.tags[n as usize].1;
        set.remove(doc, slots);
        if set.len() == 0 {
            self.release(n);
        }
    }

    fn release(&mut self, n: u32) {
        let (tag, _) = std::mem::take(&mut self.tags[n as usize]);
        if self.numbers.remove(&tag).is_some() {
            self.free.push(n);
        }
    }

    fn clear(&mut self, doc: u32, slots: usize) {
        if let Some(held) = self.docs.get_mut(doc as usize) {
            let old = std::mem::take(held);
            for &n in old.as_slice() {
                self.drop_from(n, doc, slots);
            }
        }
    }

    /// The documents holding any of `values`, or a tag starting with any of
    /// `prefixes`.
    fn any_of(&self, values: &[String], prefixes: &[String], out: &mut Bits) {
        for value in values {
            if let Some(&n) = self.numbers.get(value.as_str()) {
                self.tags[n as usize].1.or_into(out);
            }
        }
        if !prefixes.is_empty() {
            for (tag, &n) in &self.numbers {
                if prefixes.iter().any(|p| tag.starts_with(p.as_str())) {
                    self.tags[n as usize].1.or_into(out);
                }
            }
        }
    }

    fn bytes(&self) -> usize {
        let tags: usize = self
            .tags
            .iter()
            .map(|(tag, set)| 24 + tag.len() + set.bytes())
            .sum();
        let numbers =
            self.numbers.capacity() * 24 + self.numbers.keys().map(|k| k.len()).sum::<usize>();
        let docs = self.docs.capacity() * std::mem::size_of::<Held>()
            + self
                .docs
                .iter()
                .map(|h| match h {
                    Held::Many(m) => m.len() * 4,
                    _ => 0,
                })
                .sum::<usize>();
        tags + numbers + docs
    }
}

// ── NUMERIC ──────────────────────────────────────────────────────────────────

/// `v` as a key that sorts as the number does. Minus zero is zero: the two
/// are equal to a comparison, and so to a range.
#[inline]
fn order_key(v: f64) -> u64 {
    let v = if v == 0.0 { 0.0 } else { v };
    let bits = v.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    }
}

/// The (value, document) of every document whose value is a number other
/// than NaN, in the value's order, are in up to three places: most in a
/// sorted list (`settled`), which a range is a slice of, read in order;
/// those written since, in a small B-tree (`recent`). Once that holds an
/// eighth as many, it is set aside (`frozen`), a new one takes the writes,
/// and the two older ones are merged into a new list a few thousand entries
/// at a time, off the write path (see [`tidy`]). A document written again or
/// gone leaves its old entry where it was, passed over from then on
/// (`in_settled`, `frozen_gone`), until a merge drops it. A range read off
/// the list costs a fraction of one read off a B-tree of every value: an
/// entry after the other in memory, not a node after the other.
#[derive(Debug, Clone, Default)]
struct NumericPostings {
    settled: Vec<(u64, u32)>,
    /// The documents whose entry is the one in `settled`.
    in_settled: Bits,
    /// What `recent` held when the merge under way began.
    frozen: BTreeSet<(u64, u32)>,
    /// Documents whose entry in `frozen` is no longer theirs.
    frozen_gone: Bits,
    recent: BTreeSet<(u64, u32)>,
    merging: Option<NumericMerge>,
    /// Writes have paused: merge `recent` whatever its size.
    settle: bool,
    /// How many documents have a value other than NaN: entries of all three.
    ordered: usize,
    /// By document id: its value, if `has` it.
    values: Vec<f64>,
    /// The documents whose field reads as a number, NaN included.
    has: Bits,
}

/// A merge of `settled` and `frozen` into a new list, part way.
#[derive(Debug, Clone, Default)]
struct NumericMerge {
    out: Vec<(u64, u32)>,
    /// The documents whose entry in `out` is still theirs.
    out_live: Bits,
    /// The next entry of `settled` to look at.
    at: usize,
    /// The last entry of `frozen` looked at.
    last: Option<(u64, u32)>,
}

/// Entries a step of a merge looks at.
const NUMERIC_STEP: usize = 2048;

/// What a step of merging replaced, for the thread to free once it has let
/// go of the postings' lock: lists of millions of entries take a while to
/// give back.
pub type Trash = Vec<Box<dyn std::any::Any + Send>>;

impl NumericPostings {
    /// The number a value holds, as `filter::eval` reads it.
    fn parse(value: Option<&[u8]>) -> Option<f64> {
        std::str::from_utf8(value?).ok()?.trim().parse::<f64>().ok()
    }

    fn set(&mut self, doc: u32, value: Option<&[u8]>) {
        self.clear(doc);
        let Some(v) = Self::parse(value) else {
            return;
        };
        let at = doc as usize;
        if at >= self.values.len() {
            self.values.resize(at + 1, 0.0);
        }
        self.values[at] = v;
        self.has.set(doc);
        if !v.is_nan() {
            self.recent.insert((order_key(v), doc));
            self.ordered += 1;
        }
    }

    fn clear(&mut self, doc: u32) {
        if !self.has.get(doc) {
            return;
        }
        let v = self.values[doc as usize];
        if !v.is_nan() {
            if self.in_settled.get(doc) {
                self.in_settled.unset(doc);
            } else if !self.recent.remove(&(order_key(v), doc)) {
                // Neither in the list nor among the recent: set aside.
                self.frozen_gone.set(doc);
            }
            if let Some(merge) = &mut self.merging {
                merge.out_live.unset(doc);
            }
            self.ordered -= 1;
        }
        self.has.unset(doc);
    }

    /// Whether there is merging to do.
    fn behind(&self) -> bool {
        self.merging.is_some()
            || self.recent.len() > (self.settled.len() / 8).max(1024)
            || (self.settle && !self.recent.is_empty())
    }

    /// A step of merging, if there is any to do: true if there was.
    fn step(&mut self, trash: &mut Trash) -> bool {
        let Some(merge) = &mut self.merging else {
            if !self.behind() {
                self.settle = false;
                return false;
            }
            // Settling is this merge: values written meanwhile wait for the
            // next pause.
            self.settle = false;
            self.frozen = std::mem::take(&mut self.recent);
            self.frozen_gone = Bits::new();
            self.merging = Some(NumericMerge {
                out: Vec::with_capacity(self.settled.len() + self.frozen.len()),
                ..NumericMerge::default()
            });
            return true;
        };
        use std::ops::Bound::{Excluded, Unbounded};
        let mut frozen = match merge.last {
            None => self.frozen.range(..),
            Some(last) => self.frozen.range((Excluded(last), Unbounded)),
        }
        .peekable();
        for _ in 0..NUMERIC_STEP {
            let settled = self.settled.get(merge.at).copied();
            let set_aside = frozen.peek().copied().copied();
            let take_settled = match (settled, set_aside) {
                (Some(a), Some(b)) => a < b,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => {
                    let merge = self.merging.take().unwrap_or_default();
                    trash.push(Box::new(std::mem::replace(&mut self.settled, merge.out)));
                    trash.push(Box::new(std::mem::take(&mut self.frozen)));
                    self.in_settled = merge.out_live;
                    self.frozen_gone = Bits::new();
                    return true;
                }
            };
            let (entry, live) = if take_settled {
                merge.at += 1;
                let entry = settled.unwrap_or_default();
                (entry, self.in_settled.get(entry.1))
            } else {
                frozen.next();
                let entry = set_aside.unwrap_or_default();
                merge.last = Some(entry);
                (entry, !self.frozen_gone.get(entry.1))
            };
            if live {
                merge.out.push(entry);
                merge.out_live.set(entry.1);
            }
        }
        true
    }

    /// Every entry in order, ascending or not, into `each` until it returns
    /// false.
    fn walk(&self, ascending: bool, each: &mut dyn FnMut(u64, u32) -> bool) {
        let settled = |entry: &&(u64, u32)| self.in_settled.get(entry.1);
        let set_aside = |entry: &&(u64, u32)| !self.frozen_gone.get(entry.1);
        let entries: Box<dyn Iterator<Item = &(u64, u32)>> = if ascending {
            let before = |a: &(u64, u32), b: &(u64, u32)| a < b;
            Box::new(merged(
                merged(
                    self.settled.iter().filter(settled),
                    self.frozen.iter().filter(set_aside),
                    before,
                ),
                self.recent.iter(),
                before,
            ))
        } else {
            let before = |a: &(u64, u32), b: &(u64, u32)| a > b;
            Box::new(merged(
                merged(
                    self.settled.iter().rev().filter(settled),
                    self.frozen.iter().rev().filter(set_aside),
                    before,
                ),
                self.recent.iter().rev(),
                before,
            ))
        };
        for &(order, doc) in entries {
            if !each(order, doc) {
                return;
            }
        }
    }

    /// The documents whose value is in the range, ends included or not.
    fn range(&self, lo: f64, lo_exclusive: bool, hi: f64, hi_exclusive: bool, out: &mut Bits) {
        if lo.is_nan() || hi.is_nan() {
            return;
        }
        let start = match (order_key(lo), lo_exclusive) {
            (k, false) => (k, 0),
            (u64::MAX, true) => return,
            (k, true) => (k + 1, 0),
        };
        let end = match (order_key(hi), hi_exclusive) {
            (k, false) => (k, u32::MAX),
            (0, true) => return,
            (k, true) => (k - 1, u32::MAX),
        };
        if start > end {
            return;
        }
        let from = self.settled.partition_point(|entry| *entry < start);
        let to = self.settled.partition_point(|entry| *entry <= end);
        for &(_, doc) in &self.settled[from..to] {
            if self.in_settled.get(doc) {
                out.set(doc);
            }
        }
        for &(_, doc) in self.frozen.range(start..=end) {
            if !self.frozen_gone.get(doc) {
                out.set(doc);
            }
        }
        for &(_, doc) in self.recent.range(start..=end) {
            out.set(doc);
        }
    }

    fn bytes(&self) -> usize {
        // A B-tree node holds up to 11 entries of 16 bytes and two words; at
        // two thirds full, some 26 bytes an entry.
        let merging = self
            .merging
            .as_ref()
            .map_or(0, |m| m.out.capacity() * 16 + m.out_live.bytes());
        self.settled.capacity() * 16
            + self.in_settled.bytes()
            + (self.frozen.len() + self.recent.len()) * 26
            + self.frozen_gone.bytes()
            + merging
            + self.values.capacity() * 8
            + self.has.bytes()
    }
}

/// Two runs of entries, each in order, as one in that order: `before(a, b)`
/// says whether `a` comes before `b`.
fn merged<'a>(
    a: impl Iterator<Item = &'a (u64, u32)> + 'a,
    b: impl Iterator<Item = &'a (u64, u32)> + 'a,
    before: fn(&(u64, u32), &(u64, u32)) -> bool,
) -> impl Iterator<Item = &'a (u64, u32)> + 'a {
    let (mut a, mut b) = (a.peekable(), b.peekable());
    std::iter::from_fn(move || match (a.peek(), b.peek()) {
        (Some(x), Some(y)) if before(x, y) => a.next(),
        (Some(_), Some(_)) => b.next(),
        (Some(_), None) => a.next(),
        (None, _) => b.next(),
    })
}

// ── The order of the keys ────────────────────────────────────────────────────
//
// A page ordered by key (FT.SEARCH without SORTBY or words to score) is read
// off the ids in their keys' order, kept here: the first ten of 500,000
// matches are the first ten matches met walking down a list, not the least of
// 500,000 keys compared, each a cache miss away.
//
// The order is in four parts, each always whole to a query:
// * `sorted`, most of the ids in key order, with each one's place (`place`);
// * a few `runs`, each in key order, of ids given out since: sealed ones at
//   level 0, and eight of a level merged into one of the next (so an id is
//   copied once a level, not once a merge);
// * `waiting`, the latest ids, in no order: a few thousand at most, as a rule;
// * a merge under way, of runs together, or of the runs into `sorted`.
// A write only adds its id to `waiting`. The rest is done a step at a time,
// off the write path ([`tidy`]): a step seals up to 4,096 waiting ids into a
// run, or merges a few thousand entries. A merge into `sorted` writes the
// places of the new list beside the old ones, and notes each id written
// meanwhile; when it is done the two lists change places, and only the ids
// noted are looked at again. So no write and no query waits on more than one
// step, at any size.
//
// An id that left, or went to another key, keeps its old entry, which no
// longer matches its place, and is passed over until a merge drops it.

const NO_PLACE: u32 = u32::MAX;
/// The place of an id in `waiting`.
const WAITING: u32 = u32::MAX - 1;
/// Places from here up (below `WAITING`) name a run; below it, a rank in
/// `sorted`.
const RUN_TAGS: u32 = 1 << 31;
/// Ids sealed into a run at a time.
const SEAL_STEP: usize = 512;
/// Entries a step of a merge takes: each is a few comparisons of keys, each
/// a cache miss away, and the step holds the postings' write lock.
const KEY_STEP: usize = 256;
/// Waiting ids that make a run worth sealing.
const SEAL_AT: usize = 512;
/// Runs of one level that are merged into one of the next.
const MERGE_RUNS_AT: usize = 8;
/// Ids in runs that make it worth merging them into `sorted`: half as many
/// as it holds (each merge copies it whole), and at least this.
const MERGE_SORTED_AT: usize = 4096;

#[derive(Debug, Default)]
struct Run {
    tag: u32,
    ids: Vec<u32>,
    level: u32,
}

#[derive(Debug, Default)]
struct KeyMerge {
    /// Into `sorted` (with every run), or the `inputs` runs from `start`
    /// into one of `level`.
    into_sorted: bool,
    start: usize,
    inputs: usize,
    level: u32,
    /// For each input (`sorted` first if it is one), the next entry.
    at: Vec<usize>,
    out: Vec<u32>,
    /// A merge of runs: the tag of the new run, which its ids have as they
    /// are taken (`out` is a run, in part).
    tag: u32,
    /// A merge into `sorted`: the places of the new list, and the ids whose
    /// place changed meanwhile.
    place: Vec<u32>,
    touched: Vec<u32>,
    /// For each input, the entry its head was last read at and the first
    /// bytes of its key: most comparisons are of these.
    heads: Vec<(usize, u128)>,
}

#[derive(Debug, Default)]
struct KeyOrder {
    sorted: Vec<u32>,
    place: Vec<u32>,
    runs: Vec<Run>,
    waiting: Vec<u32>,
    merging: Option<KeyMerge>,
    next_tag: u32,
    /// Writes have paused: every waiting id and run into `sorted`, whatever
    /// their number.
    settle: bool,
}

/// The key of document `id`, from the postings' table of keys.
#[inline]
fn key_text(keys: &[Option<Arc<str>>], id: u32) -> &str {
    keys.get(id as usize)
        .and_then(|key| key.as_deref())
        .unwrap_or("")
}

/// The first sixteen bytes of `key`, as a number that sorts as they do,
/// shorter keys padded with zeros: two keys whose numbers differ are in that
/// order, and two whose numbers are equal are left to their whole keys.
#[inline]
fn key_prefix(key: &str) -> u128 {
    let mut bytes = [0u8; 16];
    let n = key.len().min(16);
    bytes[..n].copy_from_slice(&key.as_bytes()[..n]);
    u128::from_be_bytes(bytes)
}

/// `ids` in the order of their keys.
fn sort_by_key(ids: &mut [u32], keys: &[Option<Arc<str>>]) {
    // By the first bytes of the keys, in a list of their own, then by the
    // whole keys where those are equal: sorting by the keys themselves would
    // go to each, at random, at each comparison.
    let mut tagged: Vec<(u128, u32)> = ids
        .iter()
        .map(|&id| (key_prefix(key_text(keys, id)), id))
        .collect();
    tagged.sort_unstable();
    let mut start = 0;
    while start < tagged.len() {
        let mut end = start + 1;
        while end < tagged.len() && tagged[end].0 == tagged[start].0 {
            end += 1;
        }
        if end - start > 1 {
            tagged[start..end]
                .sort_unstable_by(|a, b| key_text(keys, a.1).cmp(key_text(keys, b.1)));
        }
        start = end;
    }
    for (slot, (_, id)) in ids.iter_mut().zip(tagged) {
        *slot = id;
    }
}

impl KeyOrder {
    fn set_place(&mut self, id: u32, place: u32) {
        let at = id as usize;
        if at >= self.place.len() {
            self.place.resize(at + 1, NO_PLACE);
        }
        self.place[at] = place;
        if let Some(merge) = &mut self.merging
            && merge.into_sorted
        {
            merge.touched.push(id);
        }
    }

    fn added(&mut self, id: u32) {
        self.set_place(id, WAITING);
        self.waiting.push(id);
    }

    fn removed(&mut self, id: u32) {
        if (id as usize) < self.place.len() {
            self.set_place(id, NO_PLACE);
        }
    }

    #[inline]
    fn place_of(&self, id: u32) -> u32 {
        self.place.get(id as usize).copied().unwrap_or(NO_PLACE)
    }

    fn runs_len(&self) -> usize {
        self.runs.iter().map(|run| run.ids.len()).sum()
    }

    /// Whether there is work for [`KeyOrder::step`].
    fn behind(&self) -> bool {
        self.merging.is_some()
            || self.waiting.len() >= SEAL_AT
            || self.runs_due().is_some()
            || self.sorted_due()
            || (self.settle && self.unsettled())
    }

    /// Ids not in `sorted` yet.
    fn unsettled(&self) -> bool {
        !self.waiting.is_empty() || !self.runs.is_empty()
    }

    /// The last runs, if `MERGE_RUNS_AT` or more of them are of one level:
    /// where they start, and how many.
    fn runs_due(&self) -> Option<(usize, usize)> {
        let level = self.runs.last()?.level;
        let same = self
            .runs
            .iter()
            .rev()
            .take_while(|run| run.level == level)
            .count();
        (same >= MERGE_RUNS_AT).then(|| (self.runs.len() - same, same))
    }

    fn sorted_due(&self) -> bool {
        !self.runs.is_empty() && self.runs_len() > (self.sorted.len() / 2).max(MERGE_SORTED_AT)
    }

    /// How large a list of places the next step wants, if it is to begin a
    /// merge into `sorted`: the thread makes it before it takes the lock.
    fn spare_wanted(&self, ids: usize) -> usize {
        let next_merges_sorted = if self.settle {
            self.merging.is_none() && self.waiting.is_empty() && !self.runs.is_empty()
        } else {
            self.merging.is_none()
                && self.waiting.len() < SEAL_AT
                && self.runs_due().is_none()
                && self.sorted_due()
        };
        if next_merges_sorted { ids } else { 0 }
    }

    fn new_tag(&mut self) -> u32 {
        if self.next_tag < RUN_TAGS || self.next_tag >= WAITING {
            self.next_tag = RUN_TAGS;
        }
        let tag = self.next_tag;
        self.next_tag += 1;
        tag
    }

    /// A step of the work, if there is any: true if there was.
    fn step(
        &mut self,
        keys: &[Option<Arc<str>>],
        spare: &mut Option<Vec<u32>>,
        trash: &mut Trash,
    ) -> bool {
        if self.waiting.len() >= SEAL_AT || (self.settle && !self.waiting.is_empty()) {
            self.seal(keys);
            return true;
        }
        if self.merging.is_some() {
            self.merge_step(keys, trash);
            return true;
        }
        if let Some((start, inputs)) = self.runs_due().filter(|_| !self.settle) {
            let tag = self.new_tag();
            let level = self.runs[start].level + 1;
            self.merging = Some(KeyMerge {
                start,
                inputs,
                level,
                at: vec![0; inputs],
                heads: vec![(usize::MAX, 0); inputs],
                tag,
                ..KeyMerge::default()
            });
            return true;
        }
        let inputs = self.runs.len();
        if self.sorted_due() || (self.settle && inputs > 0) {
            // Settling is this merge: ids written meanwhile wait for the
            // next pause, not another merge of everything.
            self.settle = false;
            // The ids waiting now keep their place through the swap.
            let touched: Vec<u32> = self.waiting.clone();
            // Made by the thread before it took the lock, if it could: all
            // `NO_PLACE`, long enough or nearly.
            let mut place = spare.take().unwrap_or_default();
            if place.len() < keys.len() {
                place.resize(keys.len(), NO_PLACE);
            }
            self.merging = Some(KeyMerge {
                into_sorted: true,
                inputs,
                at: vec![0; inputs + 1],
                heads: vec![(usize::MAX, 0); inputs + 1],
                out: Vec::with_capacity(self.sorted.len() + self.runs_len()),
                place,
                touched,
                ..KeyMerge::default()
            });
            return true;
        }
        self.settle = false;
        false
    }

    /// Up to `SEAL_STEP` waiting ids into a run of their own.
    fn seal(&mut self, keys: &[Option<Arc<str>>]) {
        let take = self.waiting.len().min(SEAL_STEP);
        let mut ids: Vec<u32> = self.waiting.drain(..take).collect();
        ids.sort_unstable();
        ids.dedup();
        ids.retain(|&id| self.place_of(id) == WAITING);
        if ids.is_empty() {
            return;
        }
        sort_by_key(&mut ids, keys);
        let tag = self.new_tag();
        for &id in &ids {
            self.set_place(id, tag);
        }
        self.runs.push(Run { tag, ids, level: 0 });
    }

    /// The entries of input `i` of the merge: `sorted` first if it is one.
    fn input<'a>(&'a self, merge: &KeyMerge, i: usize) -> (&'a [u32], Option<u32>) {
        match (merge.into_sorted, i) {
            (true, 0) => (&self.sorted, None),
            (true, i) => (&self.runs[i - 1].ids, Some(self.runs[i - 1].tag)),
            (false, i) => {
                let run = &self.runs[merge.start + i];
                (&run.ids, Some(run.tag))
            }
        }
    }

    /// Up to `KEY_STEP` entries of the merge under way, and its end.
    fn merge_step(&mut self, keys: &[Option<Arc<str>>], trash: &mut Trash) {
        let Some(mut merge) = self.merging.take() else {
            return;
        };
        let inputs = merge.at.len();
        let mut looked = 0;
        while looked < KEY_STEP {
            // The least key among the next live entry of each input.
            let mut least: Option<(usize, u32)> = None;
            for i in 0..inputs {
                let (ids, tag) = self.input(&merge, i);
                while let Some(&id) = ids.get(merge.at[i]) {
                    let live = match tag {
                        None => self.place_of(id) == merge.at[i] as u32,
                        Some(tag) => self.place_of(id) == tag,
                    };
                    if live {
                        break;
                    }
                    merge.at[i] += 1;
                    looked += 1;
                }
                let Some(&id) = ids.get(merge.at[i]) else {
                    continue;
                };
                if merge.heads[i].0 != merge.at[i] {
                    merge.heads[i] = (merge.at[i], key_prefix(key_text(keys, id)));
                }
                let prefix = merge.heads[i].1;
                let before = match least {
                    None => true,
                    Some((l, other)) => {
                        let other_prefix = merge.heads[l].1;
                        prefix < other_prefix
                            || (prefix == other_prefix
                                && key_text(keys, id) < key_text(keys, other))
                    }
                };
                if before {
                    least = Some((i, id));
                }
            }
            let Some((i, id)) = least else {
                self.finish(merge, keys, trash);
                return;
            };
            merge.at[i] += 1;
            looked += 1;
            if merge.into_sorted {
                merge.place[id as usize] = merge.out.len() as u32;
            } else {
                // Its place is now in the new run, which queries read.
                self.place[id as usize] = merge.tag;
            }
            merge.out.push(id);
        }
        self.merging = Some(merge);
    }

    fn finish(&mut self, merge: KeyMerge, keys: &[Option<Arc<str>>], trash: &mut Trash) {
        let inputs = merge.inputs;
        if merge.into_sorted {
            self.runs.drain(..inputs);
            let mut place = merge.place;
            place.resize(keys.len().max(self.place.len()), NO_PLACE);
            // Whatever an id written meanwhile has now is its place: the
            // entry the merge took for it, if any, is not.
            for id in merge.touched {
                place[id as usize] = self.place_of(id);
            }
            trash.push(Box::new(std::mem::replace(&mut self.place, place)));
            trash.push(Box::new(std::mem::replace(&mut self.sorted, merge.out)));
        } else {
            // Where its inputs were: the runs sealed meanwhile stay last.
            let run = Run {
                tag: merge.tag,
                ids: merge.out,
                level: merge.level,
            };
            let start = merge.start;
            trash.push(Box::new(
                self.runs
                    .splice(start..start + inputs, std::iter::once(run))
                    .collect::<Vec<Run>>(),
            ));
        }
    }

    fn bytes(&self) -> usize {
        let merging = self.merging.as_ref().map_or(0, |m| {
            m.out.capacity() + m.place.capacity() + m.touched.capacity() + m.at.len()
        });
        (self.sorted.capacity()
            + self.place.capacity()
            + self.waiting.capacity()
            + self
                .runs
                .iter()
                .map(|run| run.ids.capacity())
                .sum::<usize>()
            + merging)
            * 4
    }

    /// The first `need` of `ids` (`of` of them) in the order of their keys:
    /// see [`Postings::first_by_key`].
    fn first(
        &self,
        keys: &[Option<Arc<str>>],
        ids: &Bits,
        need: usize,
        of: usize,
        ran_out: &mut dyn FnMut() -> bool,
    ) -> Vec<u32> {
        // Lists in key order, each of the first `need` of `ids` in a part,
        // then merged.
        let mut lists: Vec<Vec<u32>> = Vec::new();
        // Ids in no order yet: picked by their keys.
        let mut late: Vec<u32> = Vec::new();
        // Walking a list meets the `need` first after about need * len / of
        // of its entries, if `ids` are spread through it; looking up the place
        // of each of `ids`, after `of`. Whichever is fewer. Where `ids` are
        // bunched up late in the order, a walk goes on far longer than that:
        // past four times as long, it gives up, and the places are looked up.
        let places = self.sorted.len() + self.runs_len();
        let expected = (need as u128) * (places as u128) / (of.max(1) as u128);
        let budget = std::cell::Cell::new((expected * 4).max(4096));
        let gave_up = std::cell::Cell::new(false);
        let walk =
            |list: &[u32], live: &dyn Fn(usize, u32) -> bool, ran_out: &mut dyn FnMut() -> bool| {
                let mut found = Vec::new();
                for (at, &id) in list.iter().enumerate() {
                    if ran_out() {
                        break;
                    }
                    let left = budget.get();
                    if left == 0 {
                        gave_up.set(true);
                        break;
                    }
                    budget.set(left - 1);
                    if live(at, id) && ids.get(id) {
                        found.push(id);
                        if found.len() == need {
                            break;
                        }
                    }
                }
                found
            };
        let mut look_up = (need as u128) * (places as u128) >= (of as u128) * (of as u128);
        if !look_up {
            lists.push(walk(
                &self.sorted,
                &|at, id| self.place_of(id) == at as u32,
                &mut *ran_out,
            ));
            for (r, run) in self.runs.iter().enumerate() {
                // Of a run being merged into another, what is before the
                // merge's place in it has gone to the merge's output (walked
                // below): passing over it, entry by entry, would use up the
                // walk's budget.
                let from = match &self.merging {
                    Some(merge)
                        if !merge.into_sorted
                            && (merge.start..merge.start + merge.inputs).contains(&r) =>
                    {
                        merge.at[r - merge.start].min(run.ids.len())
                    }
                    _ => 0,
                };
                lists.push(walk(
                    &run.ids[from..],
                    &|_, id| self.place_of(id) == run.tag,
                    &mut *ran_out,
                ));
            }
            if let Some(merge) = &self.merging
                && !merge.into_sorted
            {
                lists.push(walk(
                    &merge.out,
                    &|_, id| self.place_of(id) == merge.tag,
                    &mut *ran_out,
                ));
            }
            late.extend(
                self.waiting
                    .iter()
                    .copied()
                    .filter(|&id| ids.get(id) && self.place_of(id) == WAITING),
            );
            late.sort_unstable();
            late.dedup();
            if gave_up.get() {
                lists.clear();
                late.clear();
                look_up = true;
            }
        }
        if look_up {
            let mut ranked: Vec<(u32, u32)> = Vec::new();
            for id in ids.iter() {
                if ran_out() {
                    break;
                }
                match self.place_of(id) {
                    NO_PLACE => {}
                    place if place < RUN_TAGS => ranked.push((place, id)),
                    _ => late.push(id),
                }
            }
            if need < ranked.len() {
                ranked.select_nth_unstable(need);
                ranked.truncate(need);
            }
            ranked.sort_unstable();
            lists.push(ranked.into_iter().map(|(_, id)| id).collect());
        }
        let key = |id: u32| key_text(keys, id);
        if !late.is_empty() {
            if need < late.len() {
                late.select_nth_unstable_by(need, |&a, &b| key(a).cmp(key(b)));
                late.truncate(need);
            }
            late.sort_unstable_by(|&a, &b| key(a).cmp(key(b)));
            lists.push(late);
        }
        lists.retain(|list| !list.is_empty());
        match lists.len() {
            0 => Vec::new(),
            1 => {
                let mut only = lists.pop().unwrap_or_default();
                only.truncate(need);
                only
            }
            _ => {
                use std::cmp::Reverse;
                let mut heads: std::collections::BinaryHeap<Reverse<(&str, usize, usize)>> = lists
                    .iter()
                    .enumerate()
                    .map(|(l, list)| Reverse((key(list[0]), l, 0)))
                    .collect();
                let mut out = Vec::with_capacity(need);
                while out.len() < need {
                    let Some(Reverse((_, l, at))) = heads.pop() else {
                        break;
                    };
                    out.push(lists[l][at]);
                    if let Some(&next) = lists[l].get(at + 1) {
                        heads.push(Reverse((key(next), l, at + 1)));
                    }
                }
                out
            }
        }
    }
}

// ── The postings of an index ─────────────────────────────────────────────────

/// Key to id, in [`KEY_MAPS`] maps by a hash of the key. A map that grows
/// moves everything it holds, under the postings' write lock: one map of
/// 1.8 million keys took 150 ms to. Each of these holds a 256th of the keys,
/// and they grow at different writes.
#[derive(Debug)]
struct KeyIds {
    maps: Vec<AHashMap<Arc<str>, u32>>,
    len: usize,
}

const KEY_MAPS: usize = 256;

impl Default for KeyIds {
    fn default() -> Self {
        Self {
            maps: (0..KEY_MAPS).map(|_| AHashMap::new()).collect(),
            len: 0,
        }
    }
}

impl KeyIds {
    #[inline]
    fn map_of(key: &str) -> usize {
        const PICK: ahash::RandomState = ahash::RandomState::with_seeds(
            0x243f_6a88_85a3_08d3,
            0x1319_8a2e_0370_7344,
            0xa409_3822_299f_31d0,
            0x082e_fa98_ec4e_6c89,
        );
        (PICK.hash_one(key) >> 56) as usize % KEY_MAPS
    }

    #[inline]
    fn get(&self, key: &str) -> Option<u32> {
        self.maps[Self::map_of(key)].get(key).copied()
    }

    fn insert(&mut self, key: Arc<str>, id: u32) {
        if self.maps[Self::map_of(&key)].insert(key, id).is_none() {
            self.len += 1;
        }
    }

    fn remove(&mut self, key: &str) -> Option<u32> {
        let id = self.maps[Self::map_of(key)].remove(key)?;
        self.len -= 1;
        Some(id)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.maps.iter().map(|m| m.capacity()).sum()
    }
}

#[derive(Debug, Clone)]
enum Kind {
    Tag(TagPostings),
    Numeric(NumericPostings),
}

#[derive(Debug, Clone)]
struct FieldPostings {
    /// The field it is read from: a hash field's name, a JSON path.
    name: String,
    kind: Kind,
}

/// The TAG and NUMERIC postings of one index, and the document ids they are
/// in terms of. See the top of this file.
#[derive(Debug, Default)]
pub struct Postings {
    /// By id: the document's key.
    keys: Vec<Option<Arc<str>>>,
    ids: KeyIds,
    /// Ids given back by documents that left, to be given out again.
    free: Vec<u32>,
    /// Every id in use: every document of the index.
    live: Bits,
    fields: Vec<FieldPostings>,
    /// Queries do not use the postings (`FT.DEBUG POSTINGS index OFF`): every
    /// document is tried, as before there were any. For comparing the two.
    /// They are still kept.
    pub unused: bool,
    /// Queries read every document they match, not only those of the page
    /// they return (`FT.DEBUG PAGE index OFF`): see `search::page_from_postings`.
    /// For comparing the two.
    pub read_all: bool,
    /// The ids in the order of their keys: see `KeyOrder`.
    order: KeyOrder,
    /// Writes so far: the thread settles what is left once it stops moving.
    writes: u64,
    /// By id: its key's cluster hash slot, for a query in a cluster to keep
    /// only the documents of the slots its node serves without hashing
    /// every key it matches.
    slots: Vec<u16>,
}

/// How long writes have to pause before what is left unmerged (a few runs,
/// some waiting ids, recent numbers) is merged anyway.
const QUIET: std::time::Duration = std::time::Duration::from_millis(100);

/// The least and the most time the postings' thread spends merging each
/// time it takes the lock (see [`tidy`]).
const STEP_LEAST: std::time::Duration = std::time::Duration::from_micros(100);
const STEP_MOST: std::time::Duration = std::time::Duration::from_millis(4);

// ── Sharing the postings ─────────────────────────────────────────────────────
//
// A query reads the postings under a read lock for as long as it takes to
// build its sets and pick its page: a broad NUMERIC range is hundreds of
// thousands of entries, a millisecond or more. A write that waited for the
// lock would wait for every query in front of it, and the lock being fair,
// the queries behind it would wait for the write: with queries coming one
// after the other, writes went at the queries' pace. So a write never waits
// for the lock. If it is free, the write is applied there and then; if not,
// it is queued (`Pending`), and whoever takes the lock next applies the
// queue first, in order: a write, the merging thread, or a query, which so
// sees every write that returned before it began.

/// What a write or a removal also does with the document's id, under the
/// postings' lock: the TEXT index's part of it, which is in terms of ids
/// (see `vector::IndexDef::text_write`). Run in the same hold of the lock
/// that gives the document its id, or before the one that gives it back,
/// whether the write was applied at once or queued: so the TEXT index never
/// names an id the postings do not give that document, and an id is not
/// given out again while it does.
///
/// Lock order: the postings, then the TEXT index. Nothing that holds the
/// TEXT index's lock waits for the postings' lock.
pub type WithId = Box<dyn FnOnce(u32) + Send>;

/// A write that found the postings' lock taken.
enum Pending {
    Write {
        key: Box<str>,
        fields: Vec<(String, Arc<[u8]>)>,
        whole: bool,
        then: Option<WithId>,
    },
    Remove {
        key: Box<str>,
        first: Option<WithId>,
    },
}

/// Writes queued past this many wait for the lock instead: a bound on what
/// the queue holds when queries never let go of it.
const QUEUE_MAX: usize = 1 << 16;

/// The postings of an index and the writes queued for them: see above.
pub struct PostingsLock {
    lock: parking_lot::RwLock<Postings>,
    queue: parking_lot::Mutex<Vec<Pending>>,
    /// The queue is not empty: set and cleared under its mutex.
    queued: AtomicBool,
    /// A thread is doing the merging the postings are behind on ([`tidy`]).
    /// Set and cleared under the postings' write lock.
    tidying: AtomicBool,
}

impl std::fmt::Debug for PostingsLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostingsLock").finish_non_exhaustive()
    }
}

/// The postings of an index, shared by its clones.
pub type SharedPostings = Arc<PostingsLock>;

/// Exclusive access to the postings, the queue applied.
pub type PostingsWrite<'a> = parking_lot::RwLockWriteGuard<'a, Postings>;
/// Shared access to the postings, the queue applied.
pub type PostingsRead<'a> = parking_lot::RwLockReadGuard<'a, Postings>;

impl PostingsLock {
    pub fn new(postings: Postings) -> SharedPostings {
        Arc::new(Self {
            lock: parking_lot::RwLock::new(postings),
            queue: parking_lot::Mutex::new(Vec::new()),
            queued: AtomicBool::new(false),
            tidying: AtomicBool::new(false),
        })
    }

    /// Shared access, every write queued so far applied first.
    pub fn read(self: &Arc<Self>) -> PostingsRead<'_> {
        if !self.queued.load(SeqCst) {
            return self.lock.read();
        }
        let mut postings = self.lock.write();
        self.apply(&mut postings);
        let tidy_now = self.wants_tidy(&postings);
        let postings = parking_lot::RwLockWriteGuard::downgrade(postings);
        if tidy_now {
            tidy(self);
        }
        postings
    }

    /// Shared access to the postings as they are, what is queued left
    /// queued: for what writes do not change, or a query's second look at
    /// them, its first ([`PostingsLock::read`]) having applied the queue.
    pub fn peek(&self) -> PostingsRead<'_> {
        self.lock.read()
    }

    /// Exclusive access, every write queued so far applied first. Hand it
    /// back with [`PostingsLock::done`] once it has changed the postings, so
    /// that the merging it calls for is started.
    pub fn write(&self) -> PostingsWrite<'_> {
        let mut postings = self.lock.write();
        self.apply(&mut postings);
        postings
    }

    /// Let go of exclusive access from [`PostingsLock::write`]: start the
    /// merging the postings are now behind on, and apply what was queued
    /// meanwhile if the lock is free.
    pub fn done(self: &Arc<Self>, postings: PostingsWrite<'_>) {
        let tidy_now = self.wants_tidy(&postings);
        drop(postings);
        if tidy_now {
            tidy(self);
        }
        self.flush();
    }

    /// What the document at `key` holds now, from `fields` (see
    /// [`Postings::write`]), and `then` run with its id: applied at once if
    /// the lock is free, queued if not. Its id, or [`NO_DOC`] if it was
    /// queued.
    pub fn write_fields(
        self: &Arc<Self>,
        key: &str,
        fields: &[(String, Arc<[u8]>)],
        whole: bool,
        then: Option<WithId>,
    ) -> u32 {
        if let Some(mut postings) = self.lock.try_write() {
            self.apply(&mut postings);
            let doc = postings.write(key, fields, whole);
            if let Some(then) = then {
                then(doc);
            }
            self.done(postings);
            return doc;
        }
        self.enqueue(Pending::Write {
            key: key.into(),
            fields: fields.to_vec(),
            whole,
            then,
        });
        NO_DOC
    }

    /// The document at `key` has left the index ([`Postings::remove`]),
    /// `first` run with its id before it is given back: at once if the lock
    /// is free, queued if not.
    pub fn remove(self: &Arc<Self>, key: &str, first: Option<WithId>) {
        if let Some(mut postings) = self.lock.try_write() {
            self.apply(&mut postings);
            remove_with(&mut postings, key, first);
            self.done(postings);
            return;
        }
        self.enqueue(Pending::Remove {
            key: key.into(),
            first,
        });
    }

    fn enqueue(self: &Arc<Self>, write: Pending) {
        let full = {
            let mut queue = self.queue.lock();
            queue.push(write);
            self.queued.store(true, SeqCst);
            queue.len() >= QUEUE_MAX
        };
        if full {
            let postings = self.write();
            self.done(postings);
        } else {
            // Whoever held the lock may have let go of it before this was
            // queued, and found nothing to apply.
            self.flush();
        }
    }

    /// Apply what is queued while the lock is free; a holder of the lock
    /// applies it otherwise, or, at the latest, the next query.
    fn flush(self: &Arc<Self>) {
        while self.queued.load(SeqCst) {
            let Some(mut postings) = self.lock.try_write() else {
                return;
            };
            self.apply(&mut postings);
            let tidy_now = self.wants_tidy(&postings);
            drop(postings);
            if tidy_now {
                tidy(self);
            }
        }
    }

    /// Apply the queue, in order, under the write lock.
    fn apply(&self, postings: &mut Postings) {
        if !self.queued.load(SeqCst) {
            return;
        }
        let writes = {
            let mut queue = self.queue.lock();
            self.queued.store(false, SeqCst);
            std::mem::take(&mut *queue)
        };
        for write in writes {
            match write {
                Pending::Write {
                    key,
                    fields,
                    whole,
                    then,
                } => {
                    let doc = postings.write(&key, &fields, whole);
                    if let Some(then) = then {
                        then(doc);
                    }
                }
                Pending::Remove { key, first } => remove_with(postings, &key, first),
            }
        }
    }

    /// Whether merging is due that no thread is doing: then the caller,
    /// once it has let go of the lock, starts one with [`tidy`]. Called
    /// under the lock.
    fn wants_tidy(&self, postings: &Postings) -> bool {
        postings.behind() && !self.tidying.swap(true, SeqCst)
    }
}

/// [`Postings::remove`] of `key`, `first` run with its id before.
fn remove_with(postings: &mut Postings, key: &str, first: Option<WithId>) {
    if let (Some(first), Some(id)) = (first, postings.id(key)) {
        first(id);
    }
    postings.remove(key);
}

/// Do the merging `shared`'s postings are behind on, on a thread of its own,
/// a few steps at a time: each step holds the postings' write lock for a few
/// hundred entries, and the thread hands it to whoever waits after
/// [`STEP_LEAST`], or half as long as it waited for it. The caller had
/// [`PostingsLock::wants_tidy`] say so. Once nothing is due, what is left is
/// merged when writes have paused for [`QUIET`], so that a quiet index is
/// read off one list; then the thread ends.
fn tidy(shared: &SharedPostings) {
    let worker = Arc::clone(shared);
    let started = std::thread::Builder::new()
        .name("kividb-postings".into())
        .spawn(move || {
            let mut spare: Option<Vec<u32>> = None;
            loop {
                let wanted = worker.lock.read().spare_wanted();
                if wanted > 0 && spare.as_ref().is_none_or(|s| s.len() < wanted) {
                    spare = Some(vec![NO_PLACE; wanted + wanted / 8]);
                }
                let mut trash: Trash = Vec::new();
                let asked = std::time::Instant::now();
                let mut postings = worker.write();
                // Queries that hold the lock long make the thread wait long
                // for it, and the more they find unmerged, the longer they
                // hold it: so the longer it waited, the more it does, up to
                // half its wait.
                let budget = (asked.elapsed() / 2).clamp(STEP_LEAST, STEP_MOST);
                if postings.tidy_steps(&mut spare, &mut trash, budget) {
                    parking_lot::RwLockWriteGuard::unlock_fair(postings);
                    drop(trash);
                    continue;
                }
                if !postings.unsettled() {
                    // Under the lock: whoever holds it next and finds the
                    // postings behind starts another thread.
                    worker.tidying.store(false, SeqCst);
                    drop(postings);
                    worker.flush();
                    return;
                }
                let seen = postings.writes;
                drop(postings);
                std::thread::sleep(QUIET);
                let mut postings = worker.write();
                if postings.writes == seen {
                    postings.settle_all();
                }
            }
        });
    if started.is_err() {
        shared.tidying.store(false, SeqCst);
    }
}

impl Postings {
    /// Postings for the TAG and NUMERIC fields of `schema`.
    pub fn new(schema: &[SchemaField]) -> Self {
        let mut postings = Self::default();
        postings.add_fields(schema);
        postings
    }

    /// Start keeping postings for the TAG and NUMERIC fields of `fields`
    /// (`FT.ALTER`). They are empty: the caller fills them in.
    pub fn add_fields(&mut self, fields: &[SchemaField]) {
        for field in fields {
            let kind = match field.field_type {
                FieldType::Tag => Kind::Tag(TagPostings::new(
                    field.options.separator,
                    field.options.case_sensitive,
                )),
                FieldType::Numeric => Kind::Numeric(NumericPostings::default()),
                _ => continue,
            };
            self.fields.push(FieldPostings {
                name: field.name.clone(),
                kind,
            });
        }
    }

    /// Whether this index keeps postings at all: it has a TAG or a NUMERIC
    /// field.
    #[inline]
    pub fn enabled(&self) -> bool {
        !self.fields.is_empty()
    }

    /// Whether a write of the field `name` is one these postings keep.
    pub fn reads(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f.name == name)
    }

    pub fn id(&self, key: &str) -> Option<u32> {
        self.ids.get(key)
    }

    pub fn key(&self, id: u32) -> Option<&Arc<str>> {
        self.keys.get(id as usize).and_then(Option::as_ref)
    }

    /// The cluster hash slot of document `id`'s key.
    #[inline]
    pub fn slot(&self, id: u32) -> u16 {
        self.slots.get(id as usize).copied().unwrap_or(0)
    }

    /// Every document of the index.
    pub fn live(&self) -> &Bits {
        &self.live
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.len() == 0
    }

    /// The document at `key`'s id, given it one if it has none.
    pub fn ensure(&mut self, key: &str) -> u32 {
        if let Some(id) = self.ids.get(key) {
            return id;
        }
        let key: Arc<str> = Arc::from(key);
        let id = match self.free.pop() {
            Some(id) => {
                self.keys[id as usize] = Some(Arc::clone(&key));
                id
            }
            None => {
                self.keys.push(Some(Arc::clone(&key)));
                (self.keys.len() - 1) as u32
            }
        };
        let at = id as usize;
        if at >= self.slots.len() {
            self.slots.resize(at + 1, 0);
        }
        self.slots[at] = crate::cluster::key_hash_slot(key.as_bytes());
        self.ids.insert(key, id);
        self.live.set(id);
        self.order.added(id);
        id
    }

    fn behind(&self) -> bool {
        self.order.behind()
            || self.fields.iter().any(|f| match &f.kind {
                Kind::Numeric(n) => n.behind(),
                Kind::Tag(_) => false,
            })
    }

    /// Whether anything is left unmerged, due or not.
    fn unsettled(&self) -> bool {
        self.order.unsettled()
            || self.fields.iter().any(|f| match &f.kind {
                Kind::Numeric(n) => !n.recent.is_empty() || n.merging.is_some(),
                Kind::Tag(_) => false,
            })
    }

    /// Have the next steps merge everything left, due or not.
    pub fn settle_all(&mut self) {
        self.order.settle = true;
        for field in &mut self.fields {
            if let Kind::Numeric(numbers) = &mut field.kind {
                numbers.settle = true;
            }
        }
    }

    /// One step of the merging that is due, if any: true if there was.
    pub fn tidy_step(&mut self) -> bool {
        self.tidy_step_with(&mut None, &mut Vec::new())
    }

    /// [`Postings::tidy_step`], with a list of places made beforehand (see
    /// [`Postings::spare_wanted`]), and what it replaces left in `trash`.
    pub fn tidy_step_with(&mut self, spare: &mut Option<Vec<u32>>, trash: &mut Trash) -> bool {
        let mut did = self.order.step(&self.keys, spare, trash);
        for field in &mut self.fields {
            if let Kind::Numeric(numbers) = &mut field.kind {
                did |= numbers.step(trash);
            }
        }
        did
    }

    /// Steps of the merging that is due, one after the other for up to
    /// `budget`: true if there was any. It stops before a step that would
    /// make a list of places under the lock (see [`Postings::spare_wanted`])
    /// larger than `spare`.
    pub fn tidy_steps(
        &mut self,
        spare: &mut Option<Vec<u32>>,
        trash: &mut Trash,
        budget: std::time::Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        if !self.tidy_step_with(spare, trash) {
            return false;
        }
        while start.elapsed() < budget
            && self.spare_wanted() <= spare.as_ref().map_or(0, Vec::len)
            && self.tidy_step_with(spare, trash)
        {}
        true
    }

    /// The length of the list of places the next step would make under the
    /// lock: 0 if none.
    pub fn spare_wanted(&self) -> usize {
        self.order.spare_wanted(self.keys.len())
    }

    /// Record what the document at `key` holds now, from `fields`, which are
    /// the fields that were written. With `whole`, they are all it has: a
    /// field kept here that is not among them is one it no longer holds.
    /// Returns its id.
    pub fn write(&mut self, key: &str, fields: &[(String, Arc<[u8]>)], whole: bool) -> u32 {
        self.writes += 1;
        let id = self.ensure(key);
        let slots = self.keys.len();
        for field in &mut self.fields {
            let value = fields
                .iter()
                .find(|(n, _)| *n == field.name)
                .map(|(_, v)| &**v);
            if value.is_none() && !whole {
                continue;
            }
            match &mut field.kind {
                Kind::Tag(tags) => tags.set(id, value, slots),
                Kind::Numeric(numbers) => numbers.set(id, value),
            }
        }
        id
    }

    /// [`Postings::write`] of a document's fields as an index reads them.
    pub fn write_doc(&mut self, key: &str, doc: &super::Fields) -> u32 {
        self.writes += 1;
        let id = self.ensure(key);
        let slots = self.keys.len();
        for field in &mut self.fields {
            let value = doc.get(&field.name).map(|v| &**v);
            match &mut field.kind {
                Kind::Tag(tags) => tags.set(id, value, slots),
                Kind::Numeric(numbers) => numbers.set(id, value),
            }
        }
        id
    }

    /// The document at `key` has left the index: out of every set, and its
    /// id is given back.
    pub fn remove(&mut self, key: &str) {
        let Some(id) = self.ids.remove(key) else {
            return;
        };
        let slots = self.keys.len();
        for field in &mut self.fields {
            match &mut field.kind {
                Kind::Tag(tags) => tags.clear(id, slots),
                Kind::Numeric(numbers) => numbers.clear(id),
            }
        }
        self.keys[id as usize] = None;
        self.live.unset(id);
        self.free.push(id);
        self.order.removed(id);
    }

    /// Forget every document (the index is being filled again).
    pub fn clear(&mut self) {
        let mut fresh = Postings {
            unused: self.unused,
            read_all: self.read_all,
            ..Postings::default()
        };
        for field in &self.fields {
            fresh.fields.push(FieldPostings {
                name: field.name.clone(),
                kind: match &field.kind {
                    Kind::Tag(t) => Kind::Tag(TagPostings::new(t.separator, t.case_sensitive)),
                    Kind::Numeric(_) => Kind::Numeric(NumericPostings::default()),
                },
            });
        }
        *self = fresh;
    }

    /// The documents a `@field:{…}` clause matches, if this field's postings
    /// are kept with the clause's separator and case: `None` otherwise.
    pub fn tag(
        &self,
        field: &str,
        values: &[String],
        prefixes: &[String],
        separator: char,
        case_sensitive: bool,
    ) -> Option<Bits> {
        let tags = self.fields.iter().find_map(|f| match &f.kind {
            Kind::Tag(t)
                if f.name == field
                    && t.separator == separator
                    && t.case_sensitive == case_sensitive =>
            {
                Some(t)
            }
            _ => None,
        })?;
        let mut out = Bits::new();
        tags.any_of(values, prefixes, &mut out);
        Some(out)
    }

    /// How many documents hold tag `tag` in TAG field `field`: its document
    /// frequency, for relevance (see `score`).
    pub fn tag_docs(&self, field: &str, tag: &str) -> usize {
        self.fields
            .iter()
            .find_map(|f| match &f.kind {
                Kind::Tag(t) if f.name == field => {
                    t.numbers.get(tag).map(|&n| t.tags[n as usize].1.len())
                }
                _ => None,
            })
            .unwrap_or(0)
    }

    fn numeric_field(&self, field: &str) -> Option<&NumericPostings> {
        self.fields.iter().find_map(|f| match &f.kind {
            Kind::Numeric(n) if f.name == field => Some(n),
            _ => None,
        })
    }

    /// The first `need` of `ids` (`of` of them) in the order of their keys.
    /// `ran_out` is asked at each step of the walk that finds them, and once
    /// it says so they are the first of those found.
    pub fn first_by_key(
        &self,
        ids: &Bits,
        need: usize,
        of: usize,
        ran_out: &mut dyn FnMut() -> bool,
    ) -> Vec<u32> {
        if need == 0 {
            return Vec::new();
        }
        self.order.first(&self.keys, ids, need, of, ran_out)
    }

    /// The postings of the NUMERIC field read from `field`, to order by.
    pub fn numbers(&self, field: &str) -> Option<Numbers<'_>> {
        self.numeric_field(field).map(Numbers)
    }

    /// The documents a `@field:[lo hi]` clause matches.
    pub fn range(
        &self,
        field: &str,
        lo: f64,
        lo_exclusive: bool,
        hi: f64,
        hi_exclusive: bool,
    ) -> Option<Bits> {
        let numbers = self.numeric_field(field)?;
        let mut out = Bits::new();
        numbers.range(lo, lo_exclusive, hi, hi_exclusive, &mut out);
        Some(out)
    }

    /// The documents a `@field != value` clause matches: those with a number
    /// there (NaN too, which equals nothing) but not `value`.
    pub fn not_equal(&self, field: &str, value: f64) -> Option<Bits> {
        let numbers = self.numeric_field(field)?;
        let mut out = numbers.has.clone();
        let mut equal = Bits::new();
        numbers.range(value, false, value, false, &mut equal);
        out.and_not(&equal);
        Some(out)
    }

    /// Bytes held: the id tables, and each field's postings.
    pub fn bytes(&self) -> PostingsBytes {
        let ids = self.keys.capacity() * 16
            + self.ids.capacity() * 24
            + self
                .keys
                .iter()
                .flatten()
                .map(|k| 16 + k.len())
                .sum::<usize>()
            + self.free.capacity() * 4
            + self.slots.capacity() * 2
            + self.live.bytes()
            + self.order.bytes();
        let mut tags = 0;
        let mut numbers = 0;
        let mut tag_values = 0;
        for field in &self.fields {
            match &field.kind {
                Kind::Tag(t) => {
                    tags += t.bytes();
                    tag_values += t.numbers.len();
                }
                Kind::Numeric(n) => numbers += n.bytes(),
            }
        }
        PostingsBytes {
            ids,
            tags,
            numbers,
            tag_values,
        }
    }
}

/// The postings of one NUMERIC field, as `SORTBY` reads them.
pub struct Numbers<'a>(&'a NumericPostings);

impl<'a> Numbers<'a> {
    /// The documents whose field reads as a number, NaN included.
    pub fn has(&self) -> &'a Bits {
        &self.0.has
    }

    /// Whether some document's value is NaN: those are in no order here.
    pub fn any_nan(&self) -> bool {
        self.0.has.count() != self.0.ordered
    }

    /// How many documents have a value other than NaN: what [`Numbers::walk`]
    /// goes through.
    pub fn ordered(&self) -> usize {
        self.0.ordered
    }

    /// The number document `id` holds, if it holds one.
    #[inline]
    pub fn value(&self, id: u32) -> Option<f64> {
        if self.0.has.get(id) {
            Some(self.0.values[id as usize])
        } else {
            None
        }
    }

    /// Every document with a value other than NaN, in the order of its
    /// value, ascending or descending, into `each` until it returns false:
    /// each with the value's place in that order, which is the same for
    /// equal values (minus zero and zero too). Equal values come in the
    /// order of their ids, or the reverse.
    pub fn walk(&self, ascending: bool, mut each: impl FnMut(u64, u32) -> bool) {
        self.0.walk(ascending, &mut each);
    }
}

/// What [`Postings::bytes`] counts, for `FT.INFO`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostingsBytes {
    pub ids: usize,
    pub tags: usize,
    pub numbers: usize,
    /// Distinct tags, over all TAG fields.
    pub tag_values: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::FieldOptions;

    fn schema() -> Vec<SchemaField> {
        vec![
            SchemaField {
                name: "t".into(),
                field_type: FieldType::Tag,
                options: FieldOptions::default(),
            },
            SchemaField {
                name: "n".into(),
                field_type: FieldType::Numeric,
                options: FieldOptions::default(),
            },
        ]
    }

    fn put(p: &mut Postings, key: &str, t: &str, n: &str) -> u32 {
        p.write(
            key,
            &[
                ("t".to_string(), Arc::from(t.as_bytes())),
                ("n".to_string(), Arc::from(n.as_bytes())),
            ],
            true,
        )
    }

    fn keys(p: &Postings, bits: &Bits) -> Vec<String> {
        let mut out: Vec<String> = bits
            .iter()
            .map(|id| p.key(id).unwrap().to_string())
            .collect();
        out.sort();
        out
    }

    #[test]
    fn bits_set_count_iterate_and_combine() {
        let mut a = Bits::new();
        for id in [0, 63, 64, 1000] {
            a.set(id);
        }
        assert_eq!(a.count(), 4);
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![0, 63, 64, 1000]);
        let mut b = Bits::new();
        b.set(64);
        b.set(5);
        let mut and = a.clone();
        and.and_with(&b);
        assert_eq!(and.iter().collect::<Vec<_>>(), vec![64]);
        let mut or = b.clone();
        or.or_with(&a);
        assert_eq!(or.count(), 5);
        a.and_not(&b);
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![0, 63, 1000]);
        a.unset(1000);
        assert!(!a.get(1000));
    }

    #[test]
    fn tags_are_read_as_the_filter_reads_them() {
        let mut p = Postings::new(&schema());
        put(&mut p, "a", " Red , blue ", "1");
        put(&mut p, "b", "red", "2");
        put(&mut p, "c", "green", "3");
        let red = p.tag("t", &["red".into()], &[], ',', false).unwrap();
        assert_eq!(keys(&p, &red), ["a", "b"]);
        let pre = p
            .tag("t", &[], &["bl".into(), "gr".into()], ',', false)
            .unwrap();
        assert_eq!(keys(&p, &pre), ["a", "c"]);
        // Another separator is another way of reading: not these postings.
        assert!(p.tag("t", &["red".into()], &[], ';', false).is_none());
    }

    #[test]
    fn a_document_written_again_or_gone_leaves_its_old_sets() {
        let mut p = Postings::new(&schema());
        put(&mut p, "a", "red", "1");
        put(&mut p, "a", "blue", "5");
        assert!(
            p.tag("t", &["red".into()], &[], ',', false)
                .unwrap()
                .is_empty()
        );
        assert_eq!(p.range("n", 0.0, false, 2.0, false).unwrap().count(), 0);
        assert_eq!(p.range("n", 5.0, false, 5.0, false).unwrap().count(), 1);
        // A write of other fields leaves these alone.
        p.write("a", &[("x".to_string(), Arc::from(&b"y"[..]))], false);
        assert_eq!(
            p.tag("t", &["blue".into()], &[], ',', false)
                .unwrap()
                .count(),
            1
        );
        p.remove("a");
        assert!(
            p.tag("t", &["blue".into()], &[], ',', false)
                .unwrap()
                .is_empty()
        );
        assert!(p.live().is_empty());
        assert_eq!(p.slot(0), crate::cluster::key_hash_slot(b"a"));
        // Its id is given out again.
        assert_eq!(put(&mut p, "b", "red", "1"), 0);
    }

    #[test]
    fn ranges_and_their_ends() {
        let mut p = Postings::new(&schema());
        for (key, n) in [
            ("a", "-inf"),
            ("b", "-0"),
            ("c", "0"),
            ("d", "1.5"),
            ("e", "nan"),
            ("f", "x"),
            ("g", "inf"),
        ] {
            put(&mut p, key, "", n);
        }
        let r = |lo, lx, hi, hx| keys(&p, &p.range("n", lo, lx, hi, hx).unwrap());
        assert_eq!(r(0.0, false, 0.0, false), ["b", "c"]);
        assert_eq!(r(-0.0, true, f64::INFINITY, false), ["d", "g"]);
        assert_eq!(r(f64::NEG_INFINITY, false, 0.0, true), ["a"]);
        assert_eq!(
            r(f64::NEG_INFINITY, false, f64::INFINITY, false),
            ["a", "b", "c", "d", "g"]
        );
        assert!(r(2.0, false, 1.0, false).is_empty());
        assert!(r(1.5, true, 1.5, true).is_empty());
        assert!(r(f64::NAN, false, 1.0, false).is_empty());
        // NaN differs from everything; a value that is no number is not there.
        assert_eq!(
            keys(&p, &p.not_equal("n", 0.0).unwrap()),
            ["a", "d", "e", "g"]
        );
    }

    #[test]
    fn keys_come_in_their_order_through_writes_and_merges() {
        let mut p = Postings::new(&schema());
        let mut seed = 0x243f_6a88_85a3_08d3u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut ran_out = || false;
        for round in 0..30 {
            // Keys added, removed and added again: ids go to other keys, and
            // the waiting list grows past what makes a query merge it.
            for _ in 0..700 {
                let key = format!("k{}", next() % 6000);
                if next() % 3 == 0 {
                    p.remove(&key);
                } else {
                    put(&mut p, &key, "", "1");
                }
                // Merging a step at a time, as the thread does, and queries
                // in between: every state of it is read.
                if next() % 3 == 0 {
                    p.tidy_step();
                }
            }
            if round % 5 == 4 {
                while p.tidy_step() {}
            }
            if round % 7 == 6 {
                p.settle_all();
                while p.tidy_step() {}
                assert!(!p.order.unsettled(), "round {round}");
                assert!(!p.order.settle);
            }
            let keep = [100, 60, 20, 3][round % 4];
            let mut ids = Bits::new();
            for id in p.live().iter() {
                if next() % 100 < keep {
                    ids.set(id);
                }
            }
            let of = ids.count();
            let mut whole: Vec<u32> = ids.iter().collect();
            whole.sort_by(|&a, &b| p.key(a).unwrap().cmp(p.key(b).unwrap()));
            for need in [1, 2, 10, of / 3, of] {
                let need = need.min(of);
                let first = p.first_by_key(&ids, need, of, &mut ran_out);
                assert_eq!(
                    first.as_slice(),
                    &whole[..need],
                    "round {round}, {need} of {of}"
                );
            }
        }
        // Keys alike in their first sixteen bytes, and one a prefix of
        // another.
        let mut p = Postings::new(&schema());
        for key in [
            "same-sixteen-byteZ",
            "same-sixteen-byte",
            "same-sixteen-byteA",
            "s",
            "",
        ] {
            put(&mut p, key, "", "1");
        }
        while p.tidy_step() {}
        let all = p.live().clone();
        let first: Vec<String> = p
            .first_by_key(&all, 5, 5, &mut ran_out)
            .into_iter()
            .map(|id| p.key(id).unwrap().to_string())
            .collect();
        assert_eq!(
            first,
            [
                "",
                "s",
                "same-sixteen-byte",
                "same-sixteen-byteA",
                "same-sixteen-byteZ"
            ]
        );
    }

    #[test]
    fn numbers_settled_and_recent_answer_as_one() {
        // Enough writes for several merges, values written again and gone
        // between them, every range and both walks checked against the
        // values themselves.
        let mut p = Postings::new(&schema());
        let mut seed = 0x1319_8a2e_0370_7344u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut model: std::collections::BTreeMap<String, f64> = Default::default();
        for round in 0..12 {
            for _ in 0..1500 {
                let key = format!("k{}", next() % 3000);
                match next() % 5 {
                    0 => {
                        p.remove(&key);
                        model.remove(&key);
                    }
                    1 => {
                        // No number: out of the field.
                        put(&mut p, &key, "", "x");
                        model.remove(&key);
                    }
                    _ => {
                        let v = (next() % 200) as f64 - 100.0;
                        put(&mut p, &key, "", &v.to_string());
                        model.insert(key, v);
                    }
                }
                if next() % 4 == 0 {
                    p.tidy_step();
                }
            }
            let numbers = p.numbers("n").unwrap();
            assert_eq!(numbers.ordered(), model.len(), "round {round}");
            assert!(!numbers.any_nan());
            for _ in 0..20 {
                let a = (next() % 220) as f64 - 110.0;
                let b = (next() % 220) as f64 - 110.0;
                let (lo, hi) = (a.min(b), a.max(b));
                let (lx, hx) = (next() % 2 == 0, next() % 2 == 0);
                let mut want: Vec<String> = model
                    .iter()
                    .filter(|&(_, &v)| {
                        (if lx { v > lo } else { v >= lo }) && (if hx { v < hi } else { v <= hi })
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                want.sort();
                let got = keys(&p, &p.range("n", lo, lx, hi, hx).unwrap());
                assert_eq!(got, want, "round {round}: {lo} {lx} {hi} {hx}");
            }
            for ascending in [true, false] {
                let mut walked: Vec<(f64, u32)> = Vec::new();
                numbers.walk(ascending, |_, id| {
                    walked.push((numbers.value(id).unwrap(), id));
                    true
                });
                assert_eq!(walked.len(), model.len());
                let mut ids: Vec<u32> = walked.iter().map(|w| w.1).collect();
                ids.sort_unstable();
                ids.dedup();
                assert_eq!(ids.len(), walked.len(), "an id twice");
                assert!(walked.windows(2).all(|w| if ascending {
                    (w[0].0, w[0].1) < (w[1].0, w[1].1)
                } else {
                    (w[0].0, w[0].1) > (w[1].0, w[1].1)
                }));
            }
        }
    }

    #[test]
    fn numbers_are_walked_in_their_order_either_way() {
        let mut p = Postings::new(&schema());
        for (key, n) in [("a", "3"), ("b", "-1"), ("c", "3"), ("d", "x"), ("e", "-0")] {
            put(&mut p, key, "", n);
        }
        let numbers = p.numbers("n").unwrap();
        assert!(!numbers.any_nan());
        assert_eq!(numbers.ordered(), 4);
        assert_eq!(numbers.has().count(), 4);
        assert_eq!(numbers.value(p.id("d").unwrap()), None);
        assert_eq!(numbers.value(p.id("b").unwrap()), Some(-1.0));
        let walked = |ascending: bool| {
            let mut out = Vec::new();
            numbers.walk(ascending, |_, id| {
                out.push(p.key(id).unwrap().to_string());
                true
            });
            out
        };
        assert_eq!(walked(true), ["b", "e", "a", "c"]);
        assert_eq!(walked(false), ["c", "a", "e", "b"]);
        // Stopped by its callback.
        let mut seen = 0;
        numbers.walk(true, |_, _| {
            seen += 1;
            seen < 2
        });
        assert_eq!(seen, 2);
        put(&mut p, "f", "", "nan");
        assert!(p.numbers("n").unwrap().any_nan());
        assert!(p.numbers("t").is_none());
    }

    #[test]
    fn a_large_tag_set_becomes_a_bitmap_and_back() {
        let mut p = Postings::new(&schema());
        for i in 0..1000 {
            put(
                &mut p,
                &format!("k{i}"),
                if i % 2 == 0 { "even" } else { "odd" },
                "1",
            );
        }
        let even = p.tag("t", &["even".into()], &[], ',', false).unwrap();
        assert_eq!(even.count(), 500);
        for i in (0..1000).step_by(2).skip(5) {
            p.remove(&format!("k{i}"));
        }
        let even = p.tag("t", &["even".into()], &[], ',', false).unwrap();
        assert_eq!(keys(&p, &even), ["k0", "k2", "k4", "k6", "k8"]);
    }

    #[test]
    fn writes_that_find_the_lock_taken_are_queued_and_read_back() {
        // Two readers hold the lock most of the time, so that most writes
        // are queued, and the postings' thread merges meanwhile. Each writer
        // reads back what it wrote at once; in the end the postings are what
        // the writes, one after the other, make. Each write and removal also
        // runs a hook with the id, as the TEXT index's part does (`WithId`):
        // `named` is that index, by id; an id is never given to a key while
        // it still names another, and in the end it names what the postings
        // do.
        use std::collections::BTreeMap;
        use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
        let shared = PostingsLock::new(Postings::new(&schema()));
        let named: Arc<parking_lot::Mutex<BTreeMap<u32, String>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let shared = Arc::clone(&shared);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(SeqCst) {
                        let p = shared.read();
                        let all = p.live().clone();
                        let first = p.first_by_key(&all, 10, all.count(), &mut || false);
                        assert!(first.len() <= 10);
                        std::thread::sleep(std::time::Duration::from_micros(300));
                    }
                })
            })
            .collect();
        let writers: Vec<_> = (0..4u64)
            .map(|w| {
                let shared = Arc::clone(&shared);
                let named = Arc::clone(&named);
                std::thread::spawn(move || {
                    let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ ((w + 1) << 40);
                    let mut next = move || {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        seed
                    };
                    let mut model: BTreeMap<String, Option<(String, u64)>> = BTreeMap::new();
                    let mut queued = 0;
                    for _ in 0..3000 {
                        let key = format!("w{w}:{}", next() % 400);
                        if next() % 5 == 0 {
                            let (gone, of) = (key.clone(), Arc::clone(&named));
                            let first: WithId = Box::new(move |id| {
                                assert_eq!(of.lock().remove(&id).as_deref(), Some(&*gone));
                            });
                            shared.remove(&key, Some(first));
                            assert_eq!(shared.read().id(&key), None);
                            model.insert(key, None);
                            continue;
                        }
                        let tag = format!("t{}", next() % 7);
                        let n = next() % 100;
                        let fields = [
                            ("t".to_string(), Arc::from(tag.as_bytes())),
                            ("n".to_string(), Arc::from(n.to_string().as_bytes())),
                        ];
                        let (to, of) = (key.clone(), Arc::clone(&named));
                        let then: WithId = Box::new(move |id| {
                            if let Some(was) = of.lock().insert(id, to.clone()) {
                                assert_eq!(was, to, "id {id} given out while it names {was}");
                            }
                        });
                        if shared.write_fields(&key, &fields, true, Some(then)) == NO_DOC {
                            queued += 1;
                        }
                        let p = shared.read();
                        let id = p.id(&key).expect("a write read back");
                        let tagged = p.tag("t", std::slice::from_ref(&tag), &[], ',', false);
                        assert!(tagged.unwrap().get(id));
                        let n = n as f64;
                        assert!(p.range("n", n, false, n, false).unwrap().get(id));
                        drop(p);
                        model.insert(key, Some((tag, n as u64)));
                    }
                    (model, queued)
                })
            })
            .collect();
        let mut model = BTreeMap::new();
        let mut queued = 0;
        for writer in writers {
            let (written, q) = writer.join().unwrap();
            model.extend(written);
            queued += q;
        }
        stop.store(true, SeqCst);
        for reader in readers {
            reader.join().unwrap();
        }
        assert!(queued > 0, "no write found the lock taken");
        let p = shared.read();
        let named = named.lock();
        assert_eq!(named.len(), p.len());
        for (id, key) in named.iter() {
            assert_eq!(p.key(*id).map(|k| &**k), Some(key.as_str()));
        }
        let live: Vec<(&String, &(String, u64))> = model
            .iter()
            .filter_map(|(key, doc)| doc.as_ref().map(|doc| (key, doc)))
            .collect();
        assert_eq!(p.len(), live.len());
        assert_eq!(p.live().count(), live.len());
        for t in 0..7 {
            let tag = format!("t{t}");
            let want: Vec<String> = live
                .iter()
                .filter(|(_, doc)| doc.0 == tag)
                .map(|(key, _)| key.to_string())
                .collect();
            let got = p.tag("t", &[tag], &[], ',', false).unwrap();
            assert_eq!(keys(&p, &got), want);
        }
        for n in 0..100u64 {
            let want: Vec<String> = live
                .iter()
                .filter(|(_, doc)| doc.1 == n)
                .map(|(key, _)| key.to_string())
                .collect();
            let n = n as f64;
            assert_eq!(keys(&p, &p.range("n", n, false, n, false).unwrap()), want);
        }
        let all = p.live().clone();
        let first: Vec<String> = p
            .first_by_key(&all, live.len(), live.len(), &mut || false)
            .into_iter()
            .map(|id| p.key(id).unwrap().to_string())
            .collect();
        let want: Vec<String> = live.iter().map(|(key, _)| key.to_string()).collect();
        assert_eq!(first, want);
    }
}
