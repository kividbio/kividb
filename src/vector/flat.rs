//! Where a FLAT index keeps its vectors: in fixed blocks of slots that never
//! move ([`Slots::vectors_only`], as an HNSW graph keeps its own, without
//! links), each vector on a cache line and in huge pages where the kernel
//! gives them. Beside them: the slot of each key, the key of each slot, the
//! slots given back for the next vectors, and, for an index that gives its
//! documents ids (TAG, NUMERIC or TEXT fields), the slot of each document
//! id and the id of each slot.
//!
//! So a KNN query of the whole index reads the blocks in order, and one
//! filtered by postings goes from each document id the filter matched to its
//! slot and its vector, and copies a key only for a vector that is among the
//! nearest so far. Before, each vector was a heap allocation of its own,
//! found by its key in a map: for each document a filter matched, a key, a
//! hash and a jump to a separate 6 KB block (at 1536 dimensions).
//!
//! An `HSET` of a document already in the index writes its vector over the
//! one in its slot; a delete gives the slot back, and the next new document
//! takes it. Blocks are not given back while the index lives.
//!
//! **Locking.** Everything here is under one read-write lock (`map`). A
//! write (a vector written, written over, or taken out; a slot given out or
//! back; an id recorded) holds it for writing; whatever reads a vector holds
//! it for reading, and reads the slot's key in the same hold. So a reader
//! never sees a vector half written, and never a slot given to another
//! document between reading its vector and reading its key. A scan lets the
//! lock go between runs of [`RUN`] slots or ids (to a waiting writer only:
//! `bump`), so writes do not wait for a long scan; what it keeps across
//! those moments is distances and copied keys, never a slot. A key taken
//! out and written again meanwhile can be met twice, in its old slot and in
//! its new one: a scan that saw slots given back meanwhile (`frees`) and
//! finds a key twice among its nearest runs again holding the lock
//! throughout.
//!
//! The ids are recorded as the documents are written, with the id the
//! postings gave them. A write whose postings were queued has none yet; a
//! filtered query that meets the id later finds the slot by its key, and
//! records it. A document keeps its id while it is in the postings, and
//! leaves this index before it leaves them (`vector::unindex_from`), so an
//! id recorded here is that document's for as long as it is recorded. A
//! query's own set of ids is read before the scan, and an id given back and
//! given out again meanwhile names the new document here: a filtered KNN
//! whose postings gave back an id while it ran tries each hit against the
//! filter (`commands::vector::knn_planned`).

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::flat_int8::{self, Codec, HEADER};
use super::postings::{Bits, NO_DOC, PostingsLock, PostingsRead};
use super::slots::Slots;
use super::{KnnResult, PreparedQuery};
use crate::commands::vector::Best;

/// No slot (an id not recorded).
const NO_SLOT: u32 = u32::MAX;

/// Slots or ids a scan reads between offers of its lock to a writer: what a
/// filtered KNN let the lock go after before.
pub(crate) const RUN: usize = 256;

/// Runs between reads of the clock: 1,024 vectors.
const RUNS_PER_CLOCK: usize = 4;

// No vector is asked for ahead (`Slots::prefetch`): the processor fetches
// the slots after the one read on its own. On a Standard_D8s_v5 at 1536
// dimensions, asking for the whole next vector made a scan of every slot
// 30 % slower and the issue's filter of about 10,090 documents 3-9 %
// slower; asking only when the next vector is not in the next slot left
// that filter as it was and made one of 100 documents 1,000 slots apart
// 6-9 % slower.

/// A FLAT index's vectors. See the module's comment.
#[derive(Debug, Default)]
pub struct VectorIndex {
    /// Made with the first vector, which sets the dimension.
    slots: OnceLock<Slots>,
    map: RwLock<Map>,
    /// Slots given back so far.
    frees: AtomicU64,
    me: Weak<VectorIndex>,
    copy: OnceLock<Int8Copy>,
    coding: AtomicBool,
    cursor: AtomicUsize,
    declined: AtomicU64,
    writes_seen: AtomicU64,
    counts: Counts,
    #[cfg(test)]
    always: AtomicBool,
}

#[derive(Debug, Default)]
#[repr(align(128))]
struct Counts {
    skipped: AtomicU64,
    rescored: AtomicU64,
}

#[derive(Debug)]
struct Int8Copy {
    codec: Codec,
    slots: Slots,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Pass {
    Auto,
    #[cfg(test)]
    Off,
    #[cfg(test)]
    On,
}

const CODING_HOLD: std::time::Duration = std::time::Duration::from_micros(50);

#[derive(Debug, Default)]
struct Map {
    hasher: ahash::RandomState,
    /// The slots, found by the hash of their key (in `keys`).
    by_key: hashbrown::HashTable<u32>,
    /// By slot: its document's key, or `None` for a slot given back.
    keys: Vec<Option<Box<str>>>,
    /// Slots given back, to be used again first.
    free: Vec<u32>,
    /// By slot: its document's id, or [`NO_DOC`]. As long as `keys`.
    slot_doc: Vec<AtomicU32>,
    /// By document id: its slot, or [`NO_SLOT`]. Valid only where `slot_doc`
    /// says the same.
    doc_slot: Vec<AtomicU32>,
    dirty: Vec<AtomicU64>,
    writes: u64,
}

impl Map {
    #[inline]
    fn find(&self, key: &str) -> Option<u32> {
        let keys = &self.keys;
        self.by_key
            .find(self.hasher.hash_one(key), |&s| {
                keys[s as usize].as_deref() == Some(key)
            })
            .copied()
    }

    /// The slot of document `doc`, if recorded.
    #[inline(always)]
    fn slot_of_doc(&self, doc: u32) -> Option<u32> {
        let slot = self.doc_slot.get(doc as usize)?.load(Relaxed);
        let mine = self
            .slot_doc
            .get(slot as usize)
            .is_some_and(|d| d.load(Relaxed) == doc);
        mine.then_some(slot)
    }

    fn mark(&mut self, slot: u32) {
        let w = slot as usize / 64;
        if self.dirty.len() <= w {
            self.dirty.resize_with(w + 1, || AtomicU64::new(0));
        }
        self.dirty[w].fetch_or(1 << (slot % 64), Relaxed);
    }

    fn any_dirty(&self) -> bool {
        self.dirty.iter().any(|w| w.load(Relaxed) != 0)
    }

    /// Forget the id recorded for `slot`, if any.
    fn forget_doc(&self, slot: u32) {
        let doc = self.slot_doc[slot as usize].swap(NO_DOC, Relaxed);
        if let Some(entry) = self.doc_slot.get(doc as usize) {
            let _ = entry.compare_exchange(slot, NO_SLOT, Relaxed, Relaxed);
        }
    }

    /// Record that `slot` holds document `doc`. Under the write lock, or
    /// under the read lock where `doc_slot` has room (a query recording what
    /// it found by key): both say what the postings say, so two readers
    /// recording at once record the same.
    fn note_doc(&self, slot: u32, doc: u32) {
        let had = self.slot_doc[slot as usize].load(Relaxed);
        if had == doc && self.slot_of_doc(doc) == Some(slot) {
            return;
        }
        if had != NO_DOC {
            self.forget_doc(slot);
        }
        let Some(entry) = self.doc_slot.get(doc as usize) else {
            return;
        };
        let other = entry.load(Relaxed);
        if other != NO_SLOT
            && other != slot
            && let Some(d) = self.slot_doc.get(other as usize)
        {
            let _ = d.compare_exchange(doc, NO_DOC, Relaxed, Relaxed);
        }
        entry.store(slot, Relaxed);
        self.slot_doc[slot as usize].store(doc, Relaxed);
    }
}

/// What a scan measures: each slot with its key.
type Visit<'v> = dyn FnMut(usize, &str) + 'v;

struct Tally {
    on: bool,
    decided: usize,
    window: usize,
    skipped: u64,
    rescored: u64,
}

impl VectorIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_shared() -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            ..Self::default()
        })
    }

    /// `key`'s vector is `vec` now. For tests and callers that know no id.
    pub fn insert(&self, key: String, vec: Vec<f32>) {
        self.insert_doc(&key, &vec, NO_DOC);
    }

    /// `key`'s vector is `vec` now, and its document id `doc` (or
    /// [`NO_DOC`] if it is not known: the postings queued the write, or the
    /// index gives no ids). Written over its old one in place.
    ///
    /// A vector whose length is not the index's is not kept, and the key's
    /// old one goes: no query of the index's dimension measured it before
    /// either (the documents' vectors are decoded at the dimension of the
    /// schema, so all are of one length).
    pub fn insert_doc(&self, key: &str, vec: &[f32], doc: u32) {
        self.insert_with(key, vec.len(), doc, |slot| {
            for (to, x) in slot.iter_mut().zip(vec) {
                to.write(*x);
            }
        });
    }

    /// [`VectorIndex::insert_doc`] of a vector of `dim` floats that `fill`
    /// writes into the key's slot, each of them (a document's blob decoded
    /// there): with the lock held for writing, so no reader sees it half
    /// written.
    pub fn insert_with(
        &self,
        key: &str,
        dim: usize,
        doc: u32,
        fill: impl FnOnce(&mut [std::mem::MaybeUninit<f32>]),
    ) {
        let slots = self.slots.get_or_init(|| Slots::vectors_only(dim));
        if dim != slots.dim() {
            self.remove(key);
            return;
        }
        let mut map = self.map.write();
        // The key's slot, or the slot to give it (not its own yet).
        let (slot, new) = loop {
            if let Some(slot) = map.find(key) {
                break (slot, false);
            }
            let slot = match map.free.last() {
                Some(&slot) => slot,
                None => map.keys.len() as u32,
            };
            let copy = self.copy.get();
            if !slots.has(slot as usize) || copy.is_some_and(|c| !c.slots.has(slot as usize)) {
                // Room made with the lock let go: a block of 1,024 vectors
                // of 1536 dimensions is 6 MB. Then looked at again.
                drop(map);
                slots.reserve(slot as usize);
                if let Some(copy) = copy {
                    copy.slots.reserve(slot as usize);
                }
                map = self.map.write();
                continue;
            }
            break (slot, true);
        };
        let copy = self.copy.get();
        // SAFETY: the write lock is held, and every reader of a slot's
        // vector holds the read lock. A slot is read only for the key it
        // holds: a new slot is given its key once `fill` has written all of
        // its vector.
        unsafe { slots.fill_vector(slot as usize, fill) };
        if new {
            if map.free.pop().is_none() {
                map.keys.push(None);
                map.slot_doc.push(AtomicU32::new(NO_DOC));
            }
            map.keys[slot as usize] = Some(key.into());
            let Map {
                hasher,
                by_key,
                keys,
                ..
            } = &mut *map;
            by_key.insert_unique(hasher.hash_one(key), slot, |&s| {
                hasher.hash_one(keys[s as usize].as_deref().unwrap_or(""))
            });
        }
        if doc != NO_DOC {
            let at = doc as usize;
            if map.doc_slot.len() <= at {
                map.doc_slot.resize_with(at + 1, || AtomicU32::new(NO_SLOT));
            }
            map.note_doc(slot, doc);
        }
        if let Some(copy) = copy {
            // SAFETY: the write lock is held; readers of a slot's copy hold
            // the read lock.
            unsafe { copy.slots.write_bytes(slot as usize, 0, &[0; HEADER]) };
            map.mark(slot);
            map.writes += 1;
        }
        let count = map.by_key.len();
        drop(map);
        if copy.is_some() || self.wants_copy(|| count) {
            self.schedule();
        }
    }

    fn switched_on(&self) -> bool {
        #[cfg(test)]
        if self.always.load(Relaxed) {
            return true;
        }
        flat_int8::enabled()
    }

    fn wants_copy(&self, count: impl FnOnce() -> usize) -> bool {
        self.switched_on()
            && flat_int8::supported()
            && self.copy.get().is_none()
            && self.declined.load(Relaxed) != flat_int8::times_set() + 1
            && self.dim().is_some_and(|d| d >= flat_int8::DIM_MIN)
            && count() >= flat_int8::BUILD_AT
    }

    fn schedule(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        if self.coding.load(Relaxed) || self.coding.swap(true, SeqCst) {
            return;
        }
        match self.me.upgrade() {
            Some(me) => coder::hand(Arc::downgrade(&me)),
            None => self.coding.store(false, SeqCst),
        }
    }

    fn writes_arriving(&self) -> bool {
        let now = self.map.read().writes;
        self.writes_seen.swap(now, Relaxed) != now
    }

    fn code_some(&self) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        let Some(slots) = self.slots.get() else {
            self.coding.store(false, SeqCst);
            return false;
        };
        let Some(copy) = self.copy.get() else {
            if !self.wants_copy(|| usize::MAX) || !self.publish(slots) {
                self.coding.store(false, SeqCst);
                return false;
            }
            return true;
        };
        let mut picked: Vec<(usize, Vec<f32>)> = Vec::new();
        let mut found = false;
        {
            let map = self.map.read();
            let since = std::time::Instant::now();
            let words = map.dirty.len();
            let start = self.cursor.load(Relaxed).min(words);
            'words: for (n, w) in (start..words).chain(0..start).enumerate() {
                if n % 1024 == 1023 && since.elapsed() >= CODING_HOLD {
                    self.cursor.store(w, Relaxed);
                    found = true;
                    break;
                }
                let word = &map.dirty[w];
                let mut bits = word.load(Relaxed);
                while bits != 0 {
                    found = true;
                    let b = bits.trailing_zeros();
                    bits &= bits - 1;
                    word.fetch_and(!(1u64 << b), Relaxed);
                    let slot = w * 64 + b as usize;
                    if map.keys.get(slot).is_some_and(Option::is_some) {
                        picked.push((slot, slots.vector(slot).to_vec()));
                    }
                    if picked.len() >= RUN
                        || (picked.len().is_multiple_of(8) && since.elapsed() >= CODING_HOLD)
                    {
                        self.cursor.store(w, Relaxed);
                        break 'words;
                    }
                }
            }
        }
        if picked.is_empty() && found {
            return true;
        }
        if !found {
            self.coding.store(false, SeqCst);
            let more = self.map.read().any_dirty();
            return more && !self.coding.swap(true, SeqCst);
        }
        let bytes = copy.codec.bytes();
        let mut scratch = Vec::new();
        let mut codes = vec![0u8; picked.len() * bytes];
        for ((_, v), out) in picked.iter().zip(codes.chunks_exact_mut(bytes)) {
            copy.codec.encode(v, &mut scratch, out);
        }
        #[cfg(test)]
        if let Some(f) = MID_RUN.with(|h| h.borrow_mut().take()) {
            f();
        }
        let mut map = self.map.write();
        let mut since = std::time::Instant::now();
        for ((slot, v), code) in picked.iter().zip(codes.chunks_exact(bytes)) {
            let same = map.keys[*slot].is_some()
                && slots
                    .vector(*slot)
                    .iter()
                    .zip(v)
                    .all(|(a, b)| a.to_bits() == b.to_bits());
            if same {
                // SAFETY: the write lock is held.
                unsafe {
                    copy.slots.write_bytes(*slot, HEADER, &code[HEADER..]);
                    copy.slots.write_bytes(*slot, 0, &code[..HEADER]);
                }
            }
            if since.elapsed() >= CODING_HOLD {
                RwLockWriteGuard::bump(&mut map);
                since = std::time::Instant::now();
            }
        }
        true
    }

    fn publish(&self, slots: &Slots) -> bool {
        let codec = Codec::new(slots.dim());
        let set = flat_int8::times_set();
        let limit = crate::RUNTIME_MAXMEMORY.load(Relaxed);
        let len = self.map.read().keys.len();
        let bytes = Slots::bytes_only(codec.bytes());
        let need = bytes.bytes_for(len);
        if limit != 0 && crate::memory_use::counted().saturating_add(need) > limit {
            self.declined.store(set + 1, Relaxed);
            crate::log_warn!(
                "search-flat-int8: no int8 copy made for a FLAT index of {len} vectors: its \
                 {need} bytes would go past maxmemory ({limit}); set search-flat-int8 again \
                 to try once more"
            );
            return false;
        }
        loop {
            let len = self.map.read().keys.len();
            if len > 0 {
                bytes.reserve(len - 1);
            }
            let dirty: Vec<AtomicU64> = (0..len.div_ceil(64) + 64)
                .map(|_| AtomicU64::new(u64::MAX))
                .collect();
            let mut map = self.map.write();
            if !self.switched_on() || flat_int8::times_set() != set {
                return false;
            }
            let now = map.keys.len();
            if now > len + 64 * 64 || (now > 0 && !bytes.has(now - 1)) {
                continue;
            }
            map.dirty = dirty;
            let _ = self.copy.set(Int8Copy {
                codec,
                slots: bytes,
            });
            return true;
        }
    }

    #[cfg(test)]
    fn code_now(&self) {
        while self.code_some() {}
    }

    pub fn int8_copy(&self) -> Option<(usize, u64, u64)> {
        let copy = self.copy.get()?;
        Some((
            copy.slots.bytes(),
            self.counts.skipped.load(Relaxed),
            self.counts.rescored.load(Relaxed),
        ))
    }

    /// Take `key`'s vector out, if the index holds one.
    pub fn remove(&self, key: &str) {
        let mut map = self.map.write();
        let Map {
            hasher,
            by_key,
            keys,
            ..
        } = &mut *map;
        let Ok(entry) = by_key.find_entry(hasher.hash_one(key), |&s| {
            keys[s as usize].as_deref() == Some(key)
        }) else {
            return;
        };
        let (slot, _) = entry.remove();
        map.keys[slot as usize] = None;
        map.forget_doc(slot);
        map.free.push(slot);
        self.frees.fetch_add(1, Relaxed);
    }

    /// Vectors held.
    pub fn len(&self) -> usize {
        self.map.read().by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `key`'s vector, copied, if the index holds one.
    pub fn get(&self, key: &str) -> Option<Vec<f32>> {
        self.with_vector(key, <[f32]>::to_vec)
    }

    /// `f` of `key`'s vector, if the index holds one.
    #[inline]
    pub fn with_vector<R>(&self, key: &str, f: impl FnOnce(&[f32]) -> R) -> Option<R> {
        let slots = self.slots.get()?;
        let map = self.map.read();
        let slot = map.find(key)?;
        Some(f(slots.vector(slot as usize)))
    }

    /// The dimension of the vectors, once there has been one.
    pub fn dim(&self) -> Option<usize> {
        self.slots.get().map(Slots::dim)
    }

    /// Bytes the blocks of vectors take.
    pub fn block_bytes(&self) -> usize {
        self.slots.get().map_or(0, Slots::bytes)
    }

    /// The nearest `k` of every vector to `query`, nearest first, and
    /// whether `deadline` passed first (the clock read every 1,024 vectors):
    /// then the nearest of those measured.
    pub(crate) fn nearest(
        &self,
        query: &PreparedQuery<'_>,
        k: usize,
        deadline: Option<std::time::Instant>,
    ) -> (Vec<KnnResult>, bool) {
        let candidates = if self.copy.get().is_some() && flat_int8::enabled() {
            self.len()
        } else {
            0
        };
        self.nearest_by(
            query,
            k,
            candidates,
            Pass::Auto,
            &mut |index, hold, visit| index.each(hold, deadline, visit),
        )
    }

    /// The nearest `k` of the documents `ids` (by the ids `postings` gave
    /// them), nearest first, offered in the order of their ids, and whether
    /// `deadline` passed first: the clock is read every 1,024 ids.
    pub(crate) fn nearest_of_docs(
        &self,
        query: &PreparedQuery<'_>,
        k: usize,
        ids: &Bits,
        postings: &PostingsLock,
        deadline: Option<std::time::Instant>,
    ) -> (Vec<KnnResult>, bool) {
        let candidates = if self.copy.get().is_some() && flat_int8::enabled() {
            ids.count()
        } else {
            0
        };
        self.nearest_by(
            query,
            k,
            candidates,
            Pass::Auto,
            &mut |index, hold, visit| index.each_doc(ids, postings, hold, deadline, visit),
        )
    }

    fn pass(
        &self,
        query: &PreparedQuery<'_>,
        k: usize,
        candidates: usize,
        mode: Pass,
    ) -> Option<(&Int8Copy, flat_int8::Query)> {
        let wanted = match mode {
            Pass::Auto => {
                flat_int8::enabled()
                    && flat_int8::supported()
                    && candidates >= flat_int8::CANDIDATES_MIN
                    && k.saturating_mul(flat_int8::K_SHARE) <= candidates
            }
            #[cfg(test)]
            Pass::Off => false,
            #[cfg(test)]
            Pass::On => true,
        };
        if !wanted || k == 0 {
            return None;
        }
        let copy = self.copy.get()?;
        Some((copy, copy.codec.query(query.metric(), query.vector())?))
    }

    /// The nearest `k` of what `scan` measures: run again holding the lock
    /// if it may have met a key twice (see the module's comment).
    fn nearest_by(
        &self,
        query: &PreparedQuery<'_>,
        k: usize,
        candidates: usize,
        mode: Pass,
        scan: &mut dyn FnMut(&Self, bool, &mut Visit<'_>) -> bool,
    ) -> (Vec<KnnResult>, bool) {
        let Some(slots) = self.slots.get() else {
            return (Vec::new(), false);
        };
        if slots.dim() != query.vector().len() {
            return (Vec::new(), false);
        }
        if mode == Pass::Auto
            && flat_int8::enabled()
            && self.copy.get().is_none()
            && self.wants_copy(|| self.len())
        {
            self.schedule();
        }
        let pass = self.pass(query, k, candidates, mode);
        let mut hold = false;
        loop {
            let frees = self.frees.load(Relaxed);
            let mut best = Best::new(k);
            let mut tally = Tally {
                on: pass.is_some(),
                decided: 0,
                window: 0,
                skipped: 0,
                rescored: 0,
            };
            let cut = scan(self, hold, &mut |slot, key| {
                if let Some((copy, q)) = &pass
                    && tally.on
                {
                    if let Some(kth) = best.kth() {
                        // Once the cut-off turns the pass off, it stays off for the rest of
                        // this scan.
                        if tally.decided > 0 && tally.decided.is_multiple_of(flat_int8::CUT_EVERY) {
                            tally.on = tally.window * flat_int8::CUT_SHARE <= flat_int8::CUT_EVERY;
                            tally.window = 0;
                        }
                        tally.decided += 1;
                        if tally.on
                            && let Some(lb) = q.lower_bound(copy.slots.slot_bytes(slot))
                            && lb >= f64::from(kth)
                        {
                            tally.skipped += 1;
                            return;
                        }
                        tally.window += 1;
                    }
                    tally.rescored += 1;
                }
                best.offer(query.to_raw(slots.vector(slot)), key);
            });
            if pass.is_some() {
                self.counts.skipped.fetch_add(tally.skipped, Relaxed);
                self.counts.rescored.fetch_add(tally.rescored, Relaxed);
            }
            let found = best.into_sorted();
            if hold || self.frees.load(Relaxed) == frees || !twice(&found) {
                return (found, cut);
            }
            hold = true;
        }
    }

    /// `visit` of every vector, in the order of their slots, until
    /// `deadline` has passed: then true. With `hold`, the lock is held
    /// throughout.
    fn each(
        &self,
        hold: bool,
        deadline: Option<std::time::Instant>,
        visit: &mut Visit<'_>,
    ) -> bool {
        if self.slots.get().is_none() {
            return false;
        }
        let mut map = self.map.read();
        let mut at = 0;
        let mut runs = 0;
        while at < map.keys.len() {
            let end = (at + RUN).min(map.keys.len());
            // In order: the processor fetches the next slots on its own.
            for (i, key) in map.keys[at..end].iter().enumerate() {
                if let Some(key) = key {
                    visit(at + i, key);
                }
            }
            at = end;
            runs += 1;
            if runs % RUNS_PER_CLOCK == 0
                && at < map.keys.len()
                && crate::commands::search::past_deadline(deadline)
            {
                return true;
            }
            if !hold {
                RwLockReadGuard::bump(&mut map);
            }
        }
        false
    }

    /// `visit` of the vector of each document of `ids` that has one, in the
    /// order of the ids, until `deadline` has passed: then true. With
    /// `hold`, the lock is held throughout.
    ///
    /// A run at a time: the slots of its ids first, then their vectors.
    fn each_doc(
        &self,
        ids: &Bits,
        postings: &PostingsLock,
        hold: bool,
        deadline: Option<std::time::Instant>,
        visit: &mut Visit<'_>,
    ) -> bool {
        if self.slots.get().is_none() {
            return false;
        }
        let mut map = self.map.read();
        let mut ids = ids.iter().peekable();
        let mut run: Vec<u32> = Vec::with_capacity(RUN);
        let mut runs = 0;
        while ids.peek().is_some() {
            run.clear();
            {
                // The postings, for an id with no slot recorded: taken the
                // first time one comes up in a run. (Nothing that holds them
                // for writing waits for this index's lock.)
                let mut keys: Option<PostingsRead<'_>> = None;
                for id in ids.by_ref().take(RUN) {
                    let slot = match map.slot_of_doc(id) {
                        Some(slot) => slot,
                        None => {
                            let postings = keys.get_or_insert_with(|| postings.peek());
                            let Some(slot) = postings.key(id).and_then(|key| map.find(key)) else {
                                continue;
                            };
                            map.note_doc(slot, id);
                            slot
                        }
                    };
                    run.push(slot);
                }
            }
            for &slot in &run {
                if let Some(key) = map.keys[slot as usize].as_deref() {
                    visit(slot as usize, key);
                }
            }
            runs += 1;
            if runs % RUNS_PER_CLOCK == 0
                && ids.peek().is_some()
                && crate::commands::search::past_deadline(deadline)
            {
                return true;
            }
            if !hold {
                #[cfg(test)]
                between_runs(&mut map);
                RwLockReadGuard::bump(&mut map);
            }
        }
        false
    }
}

mod coder {
    use super::VectorIndex;
    use std::collections::VecDeque;
    use std::sync::Weak;

    type Queue = (VecDeque<Weak<VectorIndex>>, bool);

    const PACE_WRITES: u32 = 3;
    const PACE_BUILD: u32 = 1;
    const PACE_MOST: std::time::Duration = std::time::Duration::from_millis(20);

    static QUEUE: parking_lot::Mutex<Queue> = parking_lot::const_mutex((VecDeque::new(), false));
    static READY: parking_lot::Condvar = parking_lot::Condvar::new();

    pub(super) fn hand(index: Weak<VectorIndex>) {
        static START: std::sync::Once = std::sync::Once::new();
        START.call_once(|| {
            use crate::cpus::BuilderExt;
            let started = std::thread::Builder::new()
                .name("flat-int8".into())
                .spawn_released(run);
            if started.is_err() {
                QUEUE.lock().1 = true;
            }
        });
        let mut queue = QUEUE.lock();
        if queue.1 {
            return;
        }
        queue.0.push_back(index);
        drop(queue);
        READY.notify_one();
    }

    fn run() {
        loop {
            let index = {
                let mut queue = QUEUE.lock();
                loop {
                    if let Some(index) = queue.0.pop_front() {
                        break index;
                    }
                    READY.wait(&mut queue);
                }
            };
            let Some(strong) = index.upgrade() else {
                continue;
            };
            let started = std::time::Instant::now();
            let more =
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| strong.code_some()))
                {
                    Ok(more) => more,
                    Err(panic) => {
                        strong
                            .coding
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        crate::log_error!(
                            "search-flat-int8: coding an index's int8 copy panicked ({}); its \
                             slots not yet coded are measured exactly until a write codes them",
                            panic
                                .downcast_ref::<&str>()
                                .copied()
                                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                                .unwrap_or("no message")
                        );
                        false
                    }
                };
            let pace = if more && strong.writes_arriving() {
                PACE_WRITES
            } else {
                PACE_BUILD
            };
            drop(strong);
            if more {
                std::thread::sleep((started.elapsed() * pace).min(PACE_MOST));
                QUEUE.lock().0.push_back(index);
            }
        }
    }
}

#[cfg(test)]
thread_local! {
    static BETWEEN_RUNS: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static MID_RUN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn between_runs(map: &mut RwLockReadGuard<'_, Map>) {
    if let Some(f) = BETWEEN_RUNS.with(|h| h.borrow_mut().take()) {
        RwLockReadGuard::unlocked(map, f);
    }
}

/// Whether a key comes up twice among `found`.
fn twice(found: &[KnnResult]) -> bool {
    let mut seen = ahash::AHashSet::with_capacity(found.len());
    found.iter().any(|hit| !seen.insert(hit.key.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::DistanceMetric;
    use std::sync::Arc;

    fn rng(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    fn random_vector(dim: usize, seed: &mut u64) -> Vec<f32> {
        (0..dim)
            .map(|_| ((rng(seed) % 2001) as f32 - 1000.0) / 250.0)
            .collect()
    }

    /// What a FLAT index was before its slots, kept as the reference: a map
    /// from key to vector; the whole index measured into a list and the
    /// nearest `k` selected; the documents of a filter measured by key, in
    /// the order of their ids, into the same `Best`.
    #[derive(Default)]
    struct Reference {
        vectors: ahash::AHashMap<String, Vec<f32>>,
    }

    impl Reference {
        fn knn(&self, query: &[f32], k: usize, metric: &DistanceMetric) -> Vec<KnnResult> {
            let prepared = PreparedQuery::new(metric, query);
            let mut scored: Vec<(f32, &str)> = self
                .vectors
                .iter()
                .filter(|(_, v)| v.len() == query.len())
                .map(|(key, v)| (prepared.to_raw(v), key.as_str()))
                .collect();
            if scored.len() > k {
                scored.select_nth_unstable_by(k, |a, b| a.0.total_cmp(&b.0));
                scored.truncate(k);
            }
            scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
            scored
                .into_iter()
                .map(|(score, key)| KnnResult {
                    key: key.to_string(),
                    score,
                })
                .collect()
        }

        fn knn_of_keys(
            &self,
            query: &[f32],
            k: usize,
            metric: &DistanceMetric,
            keys: &[Arc<str>],
        ) -> Vec<KnnResult> {
            let prepared = PreparedQuery::new(metric, query);
            let mut best = Best::new(k);
            for key in keys {
                if let Some(v) = self.vectors.get(&**key)
                    && v.len() == query.len()
                {
                    best.offer(prepared.to_raw(v), key);
                }
            }
            best.into_sorted()
        }
    }

    /// The exact nearest `k` by brute force, in f64, with the keys of equal
    /// distance in key order: what any correct answer is, up to the order
    /// of ties.
    fn brute_force(
        data: &ahash::AHashMap<String, Vec<f32>>,
        query: &[f32],
        k: usize,
        metric: &DistanceMetric,
    ) -> Vec<(String, f32)> {
        let prepared = PreparedQuery::new(metric, query);
        let mut all: Vec<(String, f32)> = data
            .iter()
            .map(|(key, v)| (key.clone(), prepared.to_raw(v)))
            .collect();
        all.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        all.truncate(k);
        all
    }

    /// Order the keys of equal distance by key: the order of ties of a scan
    /// of the whole index followed the map's hash order before, which is
    /// chosen at random as the process starts.
    fn ties_by_key(found: &[KnnResult]) -> Vec<(String, f32)> {
        let mut out: Vec<(String, f32)> = found
            .iter()
            .map(|hit| (hit.key.clone(), hit.score))
            .collect();
        out.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        out
    }

    /// Random writes, overwrites and deletes to the slots and to the
    /// reference alike, ids given and taken back by real postings, some
    /// writes with their id not known (as when the postings queued them);
    /// every few, queries of every kind against both: the same distances,
    /// to the bit, the same keys, and the same order where the order was
    /// defined before (the documents of a filter, in the order of their
    /// ids); and the nearest of the whole index the exact ones by brute
    /// force.
    #[test]
    fn slots_answer_as_the_map_did() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut queries = 0usize;
        for (dim, metric) in [
            (8, DistanceMetric::Cosine),
            (37, DistanceMetric::L2),
            (64, DistanceMetric::InnerProduct),
            (129, DistanceMetric::Cosine),
        ] {
            let flat = VectorIndex::new();
            let mut reference = Reference::default();
            let postings = PostingsLock::new(super::super::postings::Postings::new(&[]));
            // A few vectors shared by several keys: ties.
            let shared: Vec<Vec<f32>> = (0..4).map(|_| random_vector(dim, &mut seed)).collect();
            for step in 0..6_000 {
                let key = format!("doc:{}", rng(&mut seed) % 700);
                match rng(&mut seed) % 10 {
                    0..=5 => {
                        let v = if rng(&mut seed).is_multiple_of(5) {
                            shared[(rng(&mut seed) % 4) as usize].clone()
                        } else {
                            random_vector(dim, &mut seed)
                        };
                        let doc = postings.write().ensure(&key);
                        let doc = if rng(&mut seed).is_multiple_of(7) {
                            NO_DOC
                        } else {
                            doc
                        };
                        flat.insert_doc(&key, &v, doc);
                        reference.vectors.insert(key, v);
                    }
                    6 => {
                        // In the postings, its vector gone.
                        flat.remove(&key);
                        reference.vectors.remove(&key);
                    }
                    7 | 8 => {
                        // Gone from the index: the vector first, then the id.
                        flat.remove(&key);
                        reference.vectors.remove(&key);
                        postings.write().remove(&key);
                    }
                    _ => {
                        // A document with no vector, given an id.
                        postings.write().ensure(&key);
                    }
                }
                if step % 25 != 0 {
                    continue;
                }
                let q = random_vector(dim, &mut seed);
                let k = [0usize, 1, 5, 10, 100, 10_000][(rng(&mut seed) % 6) as usize];
                let prepared = PreparedQuery::new(&metric, &q);
                // The whole index.
                let (got, cut) = flat.nearest(&prepared, k, None);
                assert!(!cut);
                let want = reference.knn(&q, k, &metric);
                let exact = brute_force(&reference.vectors, &q, k, &metric);
                let scores =
                    |r: &[KnnResult]| r.iter().map(|h| h.score.to_bits()).collect::<Vec<_>>();
                assert_eq!(scores(&got), scores(&want), "{dim}-d step {step}");
                assert_eq!(
                    scores(&got),
                    exact.iter().map(|h| h.1.to_bits()).collect::<Vec<_>>()
                );
                // The same keys but for which of equal distances the last
                // places went to; each at its own distance, once.
                assert!(!twice(&got));
                for hit in &got {
                    let v = &reference.vectors[&hit.key];
                    assert_eq!(prepared.to_raw(v).to_bits(), hit.score.to_bits());
                }
                let last = got.last().map(|h| h.score);
                let firm = |r: &[KnnResult]| {
                    ties_by_key(r)
                        .into_iter()
                        .filter(|h| Some(h.1) != last)
                        .collect::<Vec<_>>()
                };
                assert_eq!(firm(&got), firm(&want), "{dim}-d step {step}");
                // A filter's documents, by id: exactly as before, ties too.
                let mut ids = Bits::new();
                let keys: Vec<Arc<str>> = {
                    let p = postings.peek();
                    for id in 0..p.live().iter().max().map_or(0, |m| m + 1) {
                        if p.live().get(id) && !rng(&mut seed).is_multiple_of(3) {
                            ids.set(id);
                        }
                    }
                    ids.iter().filter_map(|id| p.key(id).cloned()).collect()
                };
                let (got, cut) = flat.nearest_of_docs(&prepared, k, &ids, &postings, None);
                assert!(!cut);
                let want = reference.knn_of_keys(&q, k, &metric, &keys);
                let pairs = |r: &[KnnResult]| {
                    r.iter()
                        .map(|h| (h.key.clone(), h.score.to_bits()))
                        .collect::<Vec<_>>()
                };
                assert_eq!(pairs(&got), pairs(&want), "{dim}-d step {step} k {k}");
                // A query of another dimension measures nothing.
                let other = random_vector(dim + 1, &mut seed);
                let other = PreparedQuery::new(&metric, &other);
                assert!(flat.nearest(&other, 10, None).0.is_empty());
                queries += 3;
            }
            assert_eq!(flat.len(), reference.vectors.len());
            for (key, v) in &reference.vectors {
                assert_eq!(flat.get(key).as_ref(), Some(v));
            }
        }
        assert!(queries > 2_000, "{queries} queries");
    }

    /// A vector of another length than the index's is not kept, and takes
    /// the key's old one out, as no query of the index's length found it
    /// before.
    #[test]
    fn a_vector_of_another_length_is_not_kept() {
        let flat = VectorIndex::new();
        flat.insert("a".into(), vec![1.0, 0.0, 0.0]);
        flat.insert("b".into(), vec![0.0, 1.0, 0.0]);
        flat.insert("a".into(), vec![1.0, 0.0]);
        assert_eq!(flat.len(), 1);
        assert!(flat.get("a").is_none());
        let q = [1.0, 0.0, 0.0];
        let prepared = PreparedQuery::new(&DistanceMetric::L2, &q);
        let (got, _) = flat.nearest(&prepared, 5, None);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].key, "b");
    }

    /// A small index holds little more than its vectors: blocks start at 8
    /// slots and double.
    #[test]
    fn a_small_index_takes_small_blocks() {
        let flat = VectorIndex::new();
        for i in 0..100 {
            flat.insert(format!("k{i}"), vec![i as f32; 64]);
        }
        // 8 + 16 + 32 + 64 slots of 256 bytes, and a byte each.
        assert!(
            flat.block_bytes() <= 120 * 257 + 4 * 64,
            "{}",
            flat.block_bytes()
        );
        // Slots given back are used again before the blocks grow.
        for i in 0..50 {
            flat.remove(&format!("k{i}"));
        }
        let before = flat.block_bytes();
        for i in 100..150 {
            flat.insert(format!("k{i}"), vec![i as f32; 64]);
        }
        assert_eq!(flat.block_bytes(), before);
        assert_eq!(flat.len(), 100);
    }

    /// Writers overwrite, delete and write again while scans of both kinds
    /// run. Each vector holds its key's number and the write's version in
    /// every place: a vector read half written would mix two versions, and
    /// a slot read as the document it held before would carry another
    /// key's number. Keys that are never deleted are always found. A scan
    /// by ids meets only the documents the ids named when they were taken,
    /// or documents written while it ran (one of which may have taken an id
    /// given back meanwhile: `knn_planned` then tries its hits against the
    /// filter).
    #[test]
    fn scans_never_see_a_vector_half_written_or_a_slot_of_another_document() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        const DIM: usize = 256;
        const KEYS: u32 = 400;
        const STEADY: u32 = 50; // keys 0..STEADY are never deleted
        let flat = Arc::new(VectorIndex::new());
        let postings = PostingsLock::new(super::super::postings::Postings::new(&[]));
        // Every place `n * 1000 + version % 1000`: exact in f32, and the
        // nearest to 0 are the lowest numbers whatever their versions.
        let vector = |n: u32, version: u32| vec![(n * 1000 + version % 1000) as f32; DIM];
        for n in 0..KEYS {
            let key = format!("doc:{n}");
            let doc = postings.write().ensure(&key);
            flat.insert_doc(&key, &vector(n, 0), doc);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let checked = Arc::new(AtomicUsize::new(0));
        // By key: twice the writes to it so far, and one more while one is
        // under way.
        let written: Arc<Vec<std::sync::atomic::AtomicU64>> = Arc::new(
            (0..KEYS)
                .map(|_| std::sync::atomic::AtomicU64::new(0))
                .collect(),
        );
        let mut threads = Vec::new();
        for w in 0..3u64 {
            let (flat, postings, stop, written) = (
                flat.clone(),
                postings.clone(),
                stop.clone(),
                written.clone(),
            );
            threads.push(std::thread::spawn(move || {
                let mut seed = 0x1234_5678 + w;
                let mut version: u32 = 1;
                while !stop.load(Ordering::Relaxed) {
                    // Each writer its own keys, as a server orders the writes to a key.
                    let n = (rng(&mut seed) % u64::from(KEYS / 3)) as u32 * 3 + w as u32;
                    let key = format!("doc:{n}");
                    version += 1;
                    written[n as usize].fetch_add(1, Ordering::SeqCst);
                    if n >= STEADY && rng(&mut seed).is_multiple_of(3) {
                        flat.remove(&key);
                        postings.write().remove(&key);
                    } else {
                        let doc = postings.write().ensure(&key);
                        let doc = if version.is_multiple_of(5) {
                            NO_DOC
                        } else {
                            doc
                        };
                        flat.insert_doc(&key, &vector(n, version), doc);
                    }
                    written[n as usize].fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for r in 0..3u64 {
            let (flat, postings, stop, checked, written) = (
                flat.clone(),
                postings.clone(),
                stop.clone(),
                checked.clone(),
                written.clone(),
            );
            threads.push(std::thread::spawn(move || {
                let check = |v: &[f32], key: &str| {
                    let n: u32 = key["doc:".len()..].parse().unwrap();
                    assert!(v.iter().all(|x| *x == v[0]), "{key}: half written");
                    assert_eq!(v[0] as u32 / 1000, n, "{key}: another document's");
                };
                let q = vec![0.0f32; DIM];
                let prepared = PreparedQuery::new(&DistanceMetric::L2, &q);
                while !stop.load(Ordering::Relaxed) {
                    let hold = r == 0;
                    let slots = flat.slots.get().unwrap();
                    flat.each(hold, None, &mut |slot, key| check(slots.vector(slot), key));
                    // The writes so far, then the ids and the keys they named.
                    let before: Vec<u64> =
                        written.iter().map(|w| w.load(Ordering::SeqCst)).collect();
                    let (ids, named) = {
                        let p = postings.peek();
                        let ids = p.live().clone();
                        let named: ahash::AHashSet<String> = ids
                            .iter()
                            .filter_map(|id| p.key(id).map(|k| k.to_string()))
                            .collect();
                        (ids, named)
                    };
                    let met_by_ids = |key: &str| {
                        let n: usize = key["doc:".len()..].parse().unwrap();
                        let meanwhile =
                            before[n] % 2 == 1 || written[n].load(Ordering::SeqCst) != before[n];
                        assert!(
                            named.contains(key) || meanwhile,
                            "{key}: neither named by the ids nor written meanwhile"
                        );
                    };
                    flat.each_doc(&ids, &postings, hold, None, &mut |slot, key| {
                        check(slots.vector(slot), key);
                        met_by_ids(key);
                    });
                    // The nearest to 0 are the lowest numbers, which stay.
                    let (got, _) = flat.nearest(&prepared, 10, None);
                    let names: Vec<&str> = got.iter().map(|h| h.key.as_str()).collect();
                    assert!(!twice(&got));
                    assert_eq!(names.len(), 10);
                    for (i, name) in names.iter().enumerate() {
                        assert_eq!(*name, format!("doc:{i}"));
                    }
                    let (got, _) = flat.nearest_of_docs(&prepared, 10, &ids, &postings, None);
                    assert!(!twice(&got));
                    assert_eq!(got.len(), 10);
                    for hit in &got {
                        met_by_ids(&hit.key);
                    }
                    checked.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        std::thread::sleep(std::time::Duration::from_secs(3));
        stop.store(true, Ordering::Relaxed);
        for t in threads {
            t.join().unwrap();
        }
        assert!(checked.load(Ordering::Relaxed) > 30);
        // At rest, every key that holds a vector is found by its id.
        let p = postings.peek();
        let map = flat.map.read();
        for n in 0..STEADY {
            let key = format!("doc:{n}");
            let id = p.id(&key).unwrap();
            let slot = map.find(&key).unwrap();
            if let Some(found) = map.slot_of_doc(id) {
                assert_eq!(found, slot);
            }
        }
    }

    fn nearest_with(
        flat: &VectorIndex,
        query: &PreparedQuery<'_>,
        k: usize,
        mode: Pass,
    ) -> Vec<KnnResult> {
        let (found, cut) = flat.nearest_by(query, k, 0, mode, &mut |index, hold, visit| {
            index.each(hold, None, visit)
        });
        assert!(!cut);
        found
    }

    fn nearest_of_docs_with(
        flat: &VectorIndex,
        query: &PreparedQuery<'_>,
        k: usize,
        ids: &Bits,
        postings: &PostingsLock,
        mode: Pass,
    ) -> Vec<KnnResult> {
        let (found, cut) = flat.nearest_by(query, k, 0, mode, &mut |index, hold, visit| {
            index.each_doc(ids, postings, hold, None, visit)
        });
        assert!(!cut);
        found
    }

    fn pairs(found: &[KnnResult]) -> Vec<(String, u32)> {
        found
            .iter()
            .map(|h| (h.key.clone(), h.score.to_bits()))
            .collect()
    }

    #[test]
    fn the_pass_answers_as_the_exact_scan() {
        let mut seed = 0x1033_1033u64;
        let mut skipped = 0u64;
        for (dim, metric) in [
            (256, DistanceMetric::Cosine),
            (300, DistanceMetric::L2),
            (768, DistanceMetric::InnerProduct),
        ] {
            let flat = VectorIndex::new();
            flat.always.store(true, Relaxed);
            let postings = PostingsLock::new(super::super::postings::Postings::new(&[]));
            let centers: Vec<Vec<f32>> = (0..8).map(|_| random_vector(dim, &mut seed)).collect();
            let near = |seed: &mut u64| {
                let c = &centers[(rng(seed) % 8) as usize];
                c.iter()
                    .map(|x| x + ((rng(seed) % 2001) as f32 - 1000.0) / 4000.0)
                    .collect::<Vec<f32>>()
            };
            for i in 0..2_000 {
                let key = format!("doc:{i}");
                let doc = postings.write().ensure(&key);
                flat.insert_doc(&key, &near(&mut seed), doc);
            }
            flat.code_now();
            for step in 0..400 {
                for _ in 0..10 {
                    let key = format!("doc:{}", rng(&mut seed) % 2_400);
                    match rng(&mut seed) % 10 {
                        0..=5 => {
                            let mut v = near(&mut seed);
                            match rng(&mut seed) % 50 {
                                0 => v[3] = f32::NAN,
                                1 => v.fill(0.0),
                                _ => {}
                            }
                            let doc = postings.write().ensure(&key);
                            flat.insert_doc(&key, &v, doc);
                        }
                        _ => {
                            flat.remove(&key);
                            postings.write().remove(&key);
                        }
                    }
                }
                if step % 3 == 0 {
                    flat.code_now();
                }
                let q = near(&mut seed);
                let prepared = PreparedQuery::new(&metric, &q);
                let k = [1usize, 10, 100][step % 3];
                let before = flat.counts.skipped.load(Relaxed);
                assert_eq!(
                    pairs(&nearest_with(&flat, &prepared, k, Pass::On)),
                    pairs(&nearest_with(&flat, &prepared, k, Pass::Off)),
                    "{dim}-d step {step} k {k}"
                );
                let mut ids = Bits::new();
                {
                    let p = postings.peek();
                    for id in p.live().iter() {
                        if !rng(&mut seed).is_multiple_of(3) {
                            ids.set(id);
                        }
                    }
                }
                if step % 2 == 0 {
                    let gone = {
                        let p = postings.peek();
                        p.live()
                            .iter()
                            .find(|&id| ids.get(id))
                            .and_then(|id| p.key(id).map(|k| (id, k.to_string())))
                    };
                    if let Some((id, key)) = gone {
                        flat.remove(&key);
                        postings.write().remove(&key);
                        let again = format!("again:{dim}:{step}");
                        let doc = postings.write().ensure(&again);
                        assert_eq!(doc, id);
                        flat.insert_doc(&again, &near(&mut seed), doc);
                    }
                }
                assert_eq!(
                    pairs(&nearest_of_docs_with(
                        &flat,
                        &prepared,
                        k,
                        &ids,
                        &postings,
                        Pass::On
                    )),
                    pairs(&nearest_of_docs_with(
                        &flat,
                        &prepared,
                        k,
                        &ids,
                        &postings,
                        Pass::Off
                    )),
                    "{dim}-d step {step} k {k}, by ids"
                );
                skipped += flat.counts.skipped.load(Relaxed) - before;
            }
        }
        assert!(skipped > 100_000, "skipped {skipped}");
    }

    #[test]
    fn a_document_deleted_and_its_id_given_again_between_two_runs_of_a_filtered_scan() {
        const DIM: usize = 256;
        let mut results = Vec::new();
        for mode in [Pass::Off, Pass::On] {
            let mut seed = 0x1033_0003u64;
            let flat = Arc::new(VectorIndex::new());
            flat.always.store(true, Relaxed);
            let postings = PostingsLock::new(super::super::postings::Postings::new(&[]));
            for i in 0..3_000 {
                let key = format!("doc:{i}");
                let doc = postings.write().ensure(&key);
                flat.insert_doc(&key, &random_vector(DIM, &mut seed), doc);
            }
            flat.code_now();
            let q = random_vector(DIM, &mut seed);
            let prepared = PreparedQuery::new(&DistanceMetric::L2, &q);
            let ids = postings.peek().live().clone();
            let (id, victim) = {
                let p = postings.peek();
                let id = ids.iter().nth(1_000).unwrap();
                (id, p.key(id).unwrap().to_string())
            };
            let (index, lock, near) = (flat.clone(), postings.clone(), q.clone());
            BETWEEN_RUNS.with(|h| {
                *h.borrow_mut() = Some(Box::new(move || {
                    index.remove(&victim);
                    lock.write().remove(&victim);
                    let doc = lock.write().ensure("again");
                    assert_eq!(doc, id);
                    index.insert_doc("again", &near, doc);
                }))
            });
            let found = nearest_of_docs_with(&flat, &prepared, 10, &ids, &postings, mode);
            assert!(BETWEEN_RUNS.with(|h| h.borrow().is_none()), "the hook ran");
            assert_eq!(found[0].key, "again");
            assert_eq!(found[0].score, 0.0);
            results.push(pairs(&found));
        }
        assert_eq!(results[0], results[1]);
    }

    #[test]
    fn writes_and_deletes_while_a_run_codes_leave_no_stale_copy() {
        const DIM: usize = 256;
        let flat = Arc::new(VectorIndex::new());
        flat.always.store(true, Relaxed);
        for i in 0..1_100 {
            flat.insert(format!("k{i}"), vec![i as f32 + 1.0; DIM]);
        }
        flat.code_now();
        for i in 0..200 {
            flat.insert(format!("k{i}"), vec![-(i as f32) - 1.0; DIM]);
        }
        let index = flat.clone();
        MID_RUN.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                for i in 0..100 {
                    index.remove(&format!("k{i}"));
                    index.insert(format!("n{i}"), vec![i as f32 + 0.25; DIM]);
                }
                for i in 100..150 {
                    index.insert(format!("k{i}"), vec![i as f32 * 3.0; DIM]);
                }
            }))
        });
        assert!(flat.code_some());
        assert!(MID_RUN.with(|h| h.borrow().is_none()), "the hook ran");
        let check = |all_coded: bool| {
            let copy = flat.copy.get().unwrap();
            let map = flat.map.read();
            let slots = flat.slots.get().unwrap();
            let mut scratch = Vec::new();
            let mut want = vec![0u8; copy.codec.bytes()];
            for (slot, key) in map.keys.iter().enumerate() {
                let have = &copy.slots.slot_bytes(slot)[..want.len()];
                let coded = have[..HEADER].iter().any(|&b| b != 0);
                if key.is_some() && (coded || all_coded) {
                    copy.codec
                        .encode(slots.vector(slot), &mut scratch, &mut want);
                    assert_eq!(have, &want[..], "slot {slot}");
                }
            }
        };
        check(false);
        flat.code_now();
        check(true);
    }

    #[test]
    fn the_pass_keeps_on_past_the_first_candidates_at_k_100() {
        let mut seed = 0x1033_0100u64;
        let dim = 256;
        let flat = VectorIndex::new();
        flat.always.store(true, Relaxed);
        let centers: Vec<Vec<f32>> = (0..16).map(|_| random_vector(dim, &mut seed)).collect();
        let near = |seed: &mut u64| {
            let c = &centers[(rng(seed) % 16) as usize];
            c.iter()
                .map(|x| x + ((rng(seed) % 2001) as f32 - 1000.0) / 4000.0)
                .collect::<Vec<f32>>()
        };
        for i in 0..20_000 {
            flat.insert(format!("doc:{i}"), near(&mut seed));
        }
        flat.code_now();
        for metric in [DistanceMetric::L2, DistanceMetric::Cosine] {
            let q = near(&mut seed);
            let prepared = PreparedQuery::new(&metric, &q);
            let before = flat.counts.skipped.load(Relaxed);
            assert_eq!(
                pairs(&nearest_with(&flat, &prepared, 100, Pass::On)),
                pairs(&nearest_with(&flat, &prepared, 100, Pass::Off))
            );
            let skipped = flat.counts.skipped.load(Relaxed) - before;
            assert!(skipped > 15_000, "{metric:?}: skipped {skipped} of 20,000");
        }
    }

    #[test]
    fn a_slot_written_over_never_keeps_the_old_copy() {
        let flat = VectorIndex::new();
        flat.always.store(true, Relaxed);
        for i in 0..1_100 {
            flat.insert(format!("k{i}"), vec![i as f32 + 1.0; 2048]);
        }
        flat.code_now();
        let header = |key: &str| {
            let map = flat.map.read();
            let slot = map.find(key).unwrap() as usize;
            flat.copy.get().unwrap().slots.slot_bytes(slot)[..HEADER].to_vec()
        };
        assert!(header("k5").iter().any(|&b| b != 0));
        let mut nan = vec![1.0f32; 2048];
        nan[9] = f32::NAN;
        flat.insert("k5".into(), nan);
        flat.code_now();
        assert!(header("k5").iter().all(|&b| b == 0));
        let before = header("k6");
        flat.insert("k6".into(), vec![-3.0; 2048]);
        assert!(header("k6").iter().all(|&b| b == 0));
        flat.code_now();
        let after = header("k6");
        assert!(after.iter().any(|&b| b != 0) && after != before);
        flat.remove("k7");
        flat.insert("new".into(), vec![2.5; 2048]);
        assert!(header("new").iter().all(|&b| b == 0));
        flat.code_now();
        assert!(header("new").iter().any(|&b| b != 0));
    }

    #[test]
    fn writes_arriving_is_true_only_after_a_write_since_it_last_looked() {
        let flat = VectorIndex::new();
        flat.always.store(true, Relaxed);
        for i in 0..1_100 {
            flat.insert(format!("k{i}"), vec![i as f32 + 1.0; 256]);
        }
        flat.code_now();
        flat.writes_arriving();
        assert!(!flat.writes_arriving());
        flat.insert("k3".into(), vec![-1.0; 256]);
        assert!(flat.writes_arriving());
        assert!(!flat.writes_arriving());
        flat.code_now();
        assert!(!flat.writes_arriving());
    }

    #[test]
    fn a_copy_block_is_zeroed_when_made() {
        let flat = VectorIndex::new();
        flat.always.store(true, Relaxed);
        for i in 0..1_100 {
            flat.insert(format!("k{i}"), vec![i as f32 + 1.0; 2048]);
        }
        flat.publish(flat.slots.get().unwrap());
        let copy = flat.copy.get().unwrap();
        assert!(copy.slots.bytes() >= 1_100 * 2_112);
        for slot in 0..1_100 {
            assert!(
                copy.slots.slot_bytes(slot)[..HEADER]
                    .iter()
                    .all(|&b| b == 0)
            );
        }
        flat.code_now();
        for slot in 0..1_100 {
            assert!(
                copy.slots.slot_bytes(slot)[..HEADER]
                    .iter()
                    .any(|&b| b != 0)
            );
        }
    }

    #[test]
    fn the_pass_under_writes_and_the_coding_thread() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        const DIM: usize = 256;
        const KEYS: u32 = 3_000;
        const STEADY: u32 = 50;
        let flat = VectorIndex::new_shared();
        flat.always.store(true, Relaxed);
        let postings = PostingsLock::new(super::super::postings::Postings::new(&[]));
        let vector = |n: u32, version: u32| {
            let mut v = vec![(n * 1000 + version % 1000) as f32; DIM];
            v[0] += 0.5;
            v
        };
        for n in 0..KEYS {
            let key = format!("doc:{n}");
            let doc = postings.write().ensure(&key);
            flat.insert_doc(&key, &vector(n, 0), doc);
        }
        flat.schedule();
        let stop = Arc::new(AtomicBool::new(false));
        let checked = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for w in 0..3u64 {
            let (flat, postings, stop) = (flat.clone(), postings.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                let mut seed = 0x5678 + w;
                let mut version: u32 = 1;
                while !stop.load(Ordering::Relaxed) {
                    let n = (rng(&mut seed) % u64::from(KEYS / 3)) as u32 * 3 + w as u32;
                    let key = format!("doc:{n}");
                    version += 1;
                    if n >= STEADY && rng(&mut seed).is_multiple_of(3) {
                        flat.remove(&key);
                        postings.write().remove(&key);
                    } else {
                        let doc = postings.write().ensure(&key);
                        flat.insert_doc(&key, &vector(n, version), doc);
                    }
                }
            }));
        }
        for _ in 0..3 {
            let (flat, postings, stop, checked) = (
                flat.clone(),
                postings.clone(),
                stop.clone(),
                checked.clone(),
            );
            threads.push(std::thread::spawn(move || {
                let q = vec![0.0f32; DIM];
                let prepared = PreparedQuery::new(&DistanceMetric::L2, &q);
                let mut scratch = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    if let Some(copy) = flat.copy.get() {
                        let map = flat.map.read();
                        let slots = flat.slots.get().unwrap();
                        let mut want = vec![0u8; copy.codec.bytes()];
                        for (slot, key) in map.keys.iter().enumerate() {
                            let have = &copy.slots.slot_bytes(slot)[..want.len()];
                            if key.is_some() && have[..HEADER].iter().any(|&b| b != 0) {
                                copy.codec
                                    .encode(slots.vector(slot), &mut scratch, &mut want);
                                assert_eq!(have, &want[..], "slot {slot} under writes");
                            }
                        }
                    }
                    let ids = postings.peek().live().clone();
                    for found in [
                        nearest_with(&flat, &prepared, 10, Pass::On),
                        nearest_of_docs_with(&flat, &prepared, 10, &ids, &postings, Pass::On),
                    ] {
                        assert!(!twice(&found));
                        let names: Vec<&str> = found.iter().map(|h| h.key.as_str()).collect();
                        let want: Vec<String> = (0..10).map(|i| format!("doc:{i}")).collect();
                        assert_eq!(names, want);
                        for hit in &found {
                            let n: u32 = hit.key["doc:".len()..].parse().unwrap();
                            let base = (n * 1000) as f32;
                            assert!(hit.score >= base * base * DIM as f32 * 0.99);
                        }
                    }
                    checked.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        std::thread::sleep(std::time::Duration::from_secs(3));
        stop.store(true, Ordering::Relaxed);
        for t in threads {
            t.join().unwrap();
        }
        assert!(checked.load(Ordering::Relaxed) > 30);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while flat.coding.load(Ordering::SeqCst) {
            assert!(std::time::Instant::now() < until, "coding never finished");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let copy = flat.copy.get().expect("the copy was built");
        let map = flat.map.read();
        let slots = flat.slots.get().unwrap();
        let mut scratch = Vec::new();
        let mut want = vec![0u8; copy.codec.bytes()];
        for (slot, key) in map.keys.iter().enumerate() {
            if key.is_some() {
                copy.codec
                    .encode(slots.vector(slot), &mut scratch, &mut want);
                assert_eq!(
                    &copy.slots.slot_bytes(slot)[..want.len()],
                    &want[..],
                    "slot {slot}"
                );
            }
        }
        assert!(!map.any_dirty());
    }
}
