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
//! id recorded here is that document's for as long as it is recorded.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

use parking_lot::{RwLock, RwLockReadGuard};

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
}

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

/// What a scan measures: each vector with its key.
type Visit<'v> = dyn FnMut(&[f32], &str) + 'v;

impl VectorIndex {
    pub fn new() -> Self {
        Self::default()
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
            if !slots.has(slot as usize) {
                // Room made with the lock let go: a block of 1,024 vectors
                // of 1536 dimensions is 6 MB. Then looked at again.
                drop(map);
                slots.reserve(slot as usize);
                map = self.map.write();
                continue;
            }
            break (slot, true);
        };
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
        self.nearest_by(query, k, &mut |index, hold, visit| {
            index.each(hold, deadline, visit)
        })
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
        self.nearest_by(query, k, &mut |index, hold, visit| {
            index.each_doc(ids, postings, hold, deadline, visit)
        })
    }

    /// The nearest `k` of what `scan` measures: run again holding the lock
    /// if it may have met a key twice (see the module's comment).
    fn nearest_by(
        &self,
        query: &PreparedQuery<'_>,
        k: usize,
        scan: &mut dyn FnMut(&Self, bool, &mut Visit<'_>) -> bool,
    ) -> (Vec<KnnResult>, bool) {
        if self.dim() != Some(query.vector().len()) {
            return (Vec::new(), false);
        }
        let mut hold = false;
        loop {
            let frees = self.frees.load(Relaxed);
            let mut best = Best::new(k);
            let cut = scan(self, hold, &mut |v, key| best.offer(query.to_raw(v), key));
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
        let Some(slots) = self.slots.get() else {
            return false;
        };
        let mut map = self.map.read();
        let mut at = 0;
        let mut runs = 0;
        while at < map.keys.len() {
            let end = (at + RUN).min(map.keys.len());
            // In order: the processor fetches the next slots on its own.
            for (i, key) in map.keys[at..end].iter().enumerate() {
                if let Some(key) = key {
                    visit(slots.vector(at + i), key);
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
        let Some(slots) = self.slots.get() else {
            return false;
        };
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
                    visit(slots.vector(slot as usize), key);
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
                RwLockReadGuard::bump(&mut map);
            }
        }
        false
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
    /// key's number. Keys
    /// that are never deleted are always found.
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
        let mut threads = Vec::new();
        for w in 0..3u64 {
            let (flat, postings, stop) = (flat.clone(), postings.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                let mut seed = 0x1234_5678 + w;
                let mut version = 1;
                while !stop.load(Ordering::Relaxed) {
                    let n = (rng(&mut seed) % u64::from(KEYS)) as u32;
                    let key = format!("doc:{n}");
                    version += 1;
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
                }
            }));
        }
        for r in 0..3u64 {
            let (flat, postings, stop, checked) = (
                flat.clone(),
                postings.clone(),
                stop.clone(),
                checked.clone(),
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
                    flat.each(hold, None, &mut |v, key| check(v, key));
                    let ids = postings.peek().live().clone();
                    flat.each_doc(&ids, &postings, hold, None, &mut |v, key| check(v, key));
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
}
