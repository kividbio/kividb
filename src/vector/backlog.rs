//! Vectors written to an HNSW index and linked into its graph by other
//! threads.
//!
//! Linking a vector into the graph is a search of the graph and some
//! hundreds of distances: about 0.6 ms for 384 dimensions with M 16 and
//! EF_CONSTRUCTION 200. Done inside the write, it ran on the thread of the
//! connection that wrote, so one client loading documents through a
//! pipeline built the graph on one core: 50,000 384-d documents in 30 s,
//! against 6 s for Redis 8.10, whose query engine links them on eight
//! threads (an Azure D8s_v5; KiviDB's CPU per document was the lower of
//! the two, 18 ms of CPU against 0.9).
//!
//! So a write puts the vector here, in its graph's backlog, and hands the
//! linking to a pool of helper threads. Until a helper has linked it, a
//! search measures the vector where it waits, beside the graph's own, so a
//! document is found by every search from the moment its write is
//! answered, as before: exactly, as a FLAT index finds it, where the graph
//! finds the others approximately. A key whose vector waits is left out of
//! what the graph finds: the graph may still hold its previous vector. The
//! walk that indexes the documents already there (`FT.CREATE`, a restart, a
//! replica's full sync) hands its vectors over the same way.
//!
//! A backlog holds at most [`limit`] vectors: 1,024, fewer the more
//! dimensions they have. A write that finds it full links its vector
//! itself, as every write did before, so a writer faster than the helpers
//! is slowed to their pace plus its own, and a search never measures more
//! than that many vectors more than it did.
//!
//! The writes to one key are ordered (`write_order`), and each takes a
//! ticket here; a helper links a vector only if its ticket is still the
//! key's, under the key's lock in the backlog ([`Backlog::lock_key`]),
//! which every insert and delete of the key in the graph takes. So a
//! vector written over before its turn is never linked, and a delete is
//! never undone by a helper that was linking the key meanwhile.
//!
//! Nothing here is saved: a snapshot holds the graph as it is, and loading
//! one inserts every vector of the documents that the graph does not hold
//! as the key does (`backfill_from_db_progress`), as it does for the
//! writes an AOF replays after its snapshot.

use super::{DistanceMetric, HnswIndex, HnswNode, KnnResult, distance_prenorm};
use ahash::AHashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// The most vectors a graph's backlog holds.
pub const LIMIT: usize = 1024;

/// The most floats a backlog holds: a search measures each of its vectors.
/// With 1,024 vectors of 1,536 dimensions (6 MB) waiting, one client's KNN
/// took 2.3 ms at the median against 0.86 ms beside the same load written
/// the 1.2 way (Azure D8s_v5, 20,000 documents in the graph, EF_RUNTIME 64):
/// the scan reads them from memory. 128 Ki floats is 512 KiB, which stays
/// in a core's cache.
const LIMIT_FLOATS: usize = 128 * 1024;

/// The most vectors of `dim` floats a backlog holds: [`LIMIT`], fewer for
/// long vectors, and never fewer than 64, enough to keep the helpers busy.
pub fn limit(dim: usize) -> usize {
    (LIMIT_FLOATS / dim.max(1)).clamp(64, LIMIT)
}

/// Below this many nodes a graph links each vector where it is written: an
/// insert into a small graph costs little, and its counts in `FT.INFO`
/// (`hnsw_live_count`, `hnsw_tombstone_count`) stay exact at once.
pub const HERE_BELOW: usize = 1000;

/// Locks a key's inserts and deletes take, by a hash of the key.
const KEY_LOCKS: usize = 64;

/// A vector waiting to be linked.
pub struct Waiting {
    ticket: u64,
    /// As the graph stores it (a cosine vector at unit length).
    pub vector: Arc<[f32]>,
    /// The document's id in the index's postings, or `postings::NO_DOC`.
    pub doc: u32,
}

/// One graph's vectors waiting to be linked, by key.
pub struct Backlog {
    waiting: parking_lot::RwLock<AHashMap<String, Waiting>>,
    /// How many there are, read without the lock.
    len: AtomicUsize,
    tickets: AtomicU64,
    keys: Box<[parking_lot::Mutex<()>]>,
}

impl Default for Backlog {
    fn default() -> Self {
        Self {
            waiting: parking_lot::RwLock::new(AHashMap::new()),
            len: AtomicUsize::new(0),
            tickets: AtomicU64::new(0),
            keys: (0..KEY_LOCKS)
                .map(|_| parking_lot::Mutex::new(()))
                .collect(),
        }
    }
}

impl std::fmt::Debug for Backlog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backlog").field("len", &self.len()).finish()
    }
}

impl Backlog {
    /// How many vectors wait.
    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Acquire)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes the waiting vectors hold (their keys and floats, and the map's
    /// entries): allocated, so counted in `used_memory` too.
    pub fn bytes(&self) -> usize {
        if self.is_empty() {
            return 0;
        }
        let waiting = self.waiting.read();
        waiting.capacity() * (std::mem::size_of::<(String, Waiting)>() + 1)
            + waiting
                .iter()
                .map(|(k, w)| k.capacity() + w.vector.len() * 4 + 16)
                .sum::<usize>()
    }

    /// The lock of `key`'s inserts and deletes in the graph.
    pub(crate) fn lock_key(&self, key: &str) -> parking_lot::MutexGuard<'_, ()> {
        // FNV-1a: a few instructions a byte, and the same on every run.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in key.bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        self.keys[(h as usize) % KEY_LOCKS].lock()
    }

    /// The waiting vectors, held for reading.
    pub(crate) fn read(&self) -> parking_lot::RwLockReadGuard<'_, AHashMap<String, Waiting>> {
        self.waiting.read()
    }

    /// `f` of the vector waiting for `key`, if one does.
    pub(crate) fn with_waiting<R>(&self, key: &str, f: impl FnOnce(&Waiting) -> R) -> Option<R> {
        if self.is_empty() {
            return None;
        }
        self.waiting.read().get(key).map(f)
    }

    /// Whether a vector waits for `key`.
    pub(crate) fn holds(&self, key: &str) -> bool {
        self.with_waiting(key, |_| ()).is_some()
    }

    /// Put `vector` in for `key`, in place of one that waited: its ticket,
    /// and how many wait now.
    fn put(&self, key: &str, vector: Arc<[f32]>, doc: u32) -> (u64, usize) {
        let ticket = self.tickets.fetch_add(1, Relaxed);
        let mut waiting = self.waiting.write();
        waiting.insert(
            key.to_string(),
            Waiting {
                ticket,
                vector,
                doc,
            },
        );
        self.len.store(waiting.len(), Release);
        (ticket, waiting.len())
    }

    /// The vector of `key` if `ticket` is still its own.
    fn current(&self, key: &str, ticket: u64) -> Option<(Arc<[f32]>, u32)> {
        let waiting = self.waiting.read();
        let w = waiting.get(key).filter(|w| w.ticket == ticket)?;
        Some((Arc::clone(&w.vector), w.doc))
    }

    /// Take `key`'s vector out if `ticket` is still its own: it is linked.
    fn done(&self, key: &str, ticket: u64) {
        let mut waiting = self.waiting.write();
        if waiting.get(key).is_some_and(|w| w.ticket == ticket) {
            waiting.remove(key);
            self.len.store(waiting.len(), Release);
        }
    }

    /// The ticket of `key`'s waiting vector, if one waits: taken, under the
    /// key's lock, by an insert or a delete of the key made here and now,
    /// to [`Backlog::forget`] that vector once the graph has the change.
    pub(crate) fn ticket(&self, key: &str) -> Option<u64> {
        self.with_waiting(key, |w| w.ticket)
    }

    /// Take out the vector of `key` that had ticket `seen` (from
    /// [`Backlog::ticket`]): the key was deleted, or a later vector of its
    /// inserted. Only after the graph has the change: before, a search
    /// would find the key in neither the backlog nor the graph as it now
    /// is, but as the graph held it before the vector that waited.
    pub(crate) fn forget(&self, key: &str, seen: Option<u64>) {
        if let Some(ticket) = seen {
            self.done(key, ticket);
        }
    }
}

/// Index `vector` of the document at `key` (`doc` its id): into the
/// backlog for a helper to link, or, when the backlog is full or there is
/// no helper, into the graph here and now.
pub(crate) fn write(
    index: &Arc<HnswIndex>,
    key: &str,
    vector: Vec<f32>,
    metric: &DistanceMetric,
    doc: u32,
) {
    let stored: Arc<[f32]> = super::as_stored(metric, vector).into();
    write_one(index, key, Arc::clone(&stored), metric, doc);
    // While a compaction builds the graph that replaces this one, the
    // write is made to that graph too, after this one: see `compact_hnsw`.
    if let Some(shadow) = index.shadow()
        && index.slots().is_some_and(|s| s.dim() == stored.len())
    {
        shadow.mirror(key, Some(stored.to_vec()), metric, doc);
    }
}

/// [`write`] to `index` alone, `vector` as the graph stores it.
fn write_one(
    index: &Arc<HnswIndex>,
    key: &str,
    vector: Arc<[f32]>,
    metric: &DistanceMetric,
    doc: u32,
) {
    // The first vector makes the slots (and sets the dimension); a
    // compaction copies the graph and must not miss a vector that waits.
    let here = index.slots().is_none_or(|s| s.dim() != vector.len())
        || index.live_count.load(Relaxed) < HERE_BELOW
        || index.compaction_in_progress.load(Acquire)
        || helpers() == 0;
    if here {
        let _key = index.backlog.lock_key(key);
        let seen = index.backlog.ticket(key);
        super::insert_unit(index, key.to_string(), vector.to_vec(), metric, doc);
        index.backlog.forget(key, seen);
        return;
    }
    let most = limit(vector.len());
    let (ticket, waiting) = index.backlog.put(key, vector, doc);
    if waiting > most {
        link(index, key, ticket, metric);
        return;
    }
    let mut jobs = POOL.jobs.lock();
    jobs.push_back(Job {
        index: Arc::clone(index),
        key: key.to_string(),
        ticket,
        metric: metric.clone(),
    });
    drop(jobs);
    POOL.ready.notify_one();
}

/// Link `key`'s waiting vector into the graph if `ticket` is still its own.
fn link(index: &Arc<HnswIndex>, key: &str, ticket: u64, metric: &DistanceMetric) {
    let _key = index.backlog.lock_key(key);
    let Some((vector, doc)) = index.backlog.current(key, ticket) else {
        return;
    };
    super::insert_unit(index, key.to_string(), vector.to_vec(), metric, doc);
    index.backlog.done(key, ticket);
}

/// The `k` nearest of `found` (the graph's, nearest first) and the
/// backlog's vectors (those `admit` lets in, if given): a key whose vector
/// waits is measured where it waits, and what the graph found for it is
/// left out. `query` is as the graph holds vectors.
pub(crate) fn merge(
    waiting: &AHashMap<String, Waiting>,
    query: &[f32],
    metric: &DistanceMetric,
    mut found: Vec<KnnResult>,
    k: usize,
    mut admit: Option<&mut dyn FnMut(&HnswNode) -> bool>,
) -> Vec<KnnResult> {
    if waiting.is_empty() {
        found.truncate(k);
        return found;
    }
    found.retain(|hit| !waiting.contains_key(&hit.key));
    found.truncate(k);
    // The nearest k of the waiting ones, by reference; keys are copied only
    // for those kept.
    let mut near: Vec<(f32, &str)> = Vec::with_capacity(k.min(waiting.len()) + 1);
    let mut bound = f32::INFINITY;
    for (key, w) in waiting.iter() {
        if w.vector.len() != query.len() {
            continue;
        }
        let d = distance_prenorm(metric, query, &w.vector);
        if near.len() >= k && d.total_cmp(&bound).is_ge() {
            continue;
        }
        if let Some(admit) = admit.as_mut() {
            let node = HnswNode::new(key.clone(), 0, Vec::new(), w.doc);
            if !admit(&node) {
                continue;
            }
        }
        near.push((d, key.as_str()));
        if near.len() > k {
            near.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
            near.truncate(k);
        }
        if near.len() >= k {
            bound = near.iter().map(|n| n.0).fold(f32::NEG_INFINITY, f32::max);
        }
    }
    found.extend(near.into_iter().map(|(score, key)| KnnResult {
        key: key.to_string(),
        score,
    }));
    found.sort_by(|a, b| a.score.total_cmp(&b.score));
    found.truncate(k);
    found
}

// ── The helpers ──────────────────────────────────────────────────────────────

struct Job {
    index: Arc<HnswIndex>,
    key: String,
    ticket: u64,
    metric: DistanceMetric,
}

struct Pool {
    jobs: parking_lot::Mutex<VecDeque<Job>>,
    ready: parking_lot::Condvar,
}

static POOL: Pool = Pool {
    jobs: parking_lot::const_mutex(VecDeque::new()),
    ready: parking_lot::Condvar::new(),
};

/// Helper threads running: 0 until the first write asks, and if none
/// could be started.
static HELPERS: AtomicUsize = AtomicUsize::new(0);
static START: std::sync::Once = std::sync::Once::new();

/// How many helpers link vectors, started once, on the first call: half the
/// CPUs the process may use (at least one).
///
/// They share those CPUs with the workers, which since #873 are one per
/// CPU. Measured on an Azure D8s_v5, the server on 6 vCPUs and memtier
/// (GET/SET 1:1, 40 connections) on the other 2, while one client loaded
/// 150,000 384-d vectors (medians of 3): one helper per CPU took GET/SET
/// from 137k a second (1.2.2's way, the vectors linked in the writes) to
/// 112k, p99 0.39 to 3.7 ms; half as many, run as `SCHED_IDLE`, served 144k
/// at a p99 of 1.08 ms (p99.9 11.6 ms against 25 ms), and built the index
/// in 41 s against 36 s with all of them and 130 s in the writes.
fn helpers() -> usize {
    START.call_once(|| {
        // The process's CPUs: the first write may come from a worker bound
        // to one (`worker-cpu-affinity`), and std would see only that one.
        let cpus = crate::cpus::available_parallelism().map_or(1, |n| n.get());
        let want = (cpus / 2).max(1);
        let mut started = 0;
        for n in 0..want {
            use crate::cpus::BuilderExt;
            let spawned = std::thread::Builder::new()
                .name(format!("hnsw-link-{n}"))
                .spawn_released(helper);
            if spawned.is_ok() {
                started += 1;
            }
        }
        HELPERS.store(started, Release);
    });
    HELPERS.load(Acquire)
}

fn helper() {
    // Only on CPU time the workers leave: a server busy serving links the
    // vectors in the writes again (the backlog fills, and a writer that finds
    // it full links its own), as 1.2 did. `nice 10` instead measured a p99
    // of 1.4-1.9 ms beside the helpers.
    #[cfg(target_os = "linux")]
    {
        let param = libc::sched_param { sched_priority: 0 };
        // SAFETY: changes this thread's own scheduling policy only; a
        // failure leaves it as it was.
        unsafe { libc::sched_setscheduler(0, libc::SCHED_IDLE, &param) };
    }
    loop {
        let job = {
            let mut jobs = POOL.jobs.lock();
            loop {
                if let Some(job) = jobs.pop_front() {
                    break job;
                }
                POOL.ready.wait(&mut jobs);
            }
        };
        // A panic would leave the vector waiting, where searches still
        // measure it, and the helper gone: it is caught, and the helper
        // goes on to the next.
        let linked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            link(&job.index, &job.key, job.ticket, &job.metric)
        }));
        if linked.is_err() {
            crate::log_warn!("an HNSW insert failed in a helper thread");
        }
    }
}

/// Wait until no vector waits in `index`'s backlog, or `timeout` passes:
/// whether it emptied.
#[cfg(test)]
pub fn settle(index: &HnswIndex, timeout: std::time::Duration) -> bool {
    let until = std::time::Instant::now() + timeout;
    while !index.backlog.is_empty() {
        if std::time::Instant::now() >= until {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::{hnsw_insert, knn_hnsw};

    fn vector(dim: usize, seed: u64) -> Vec<f32> {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..dim)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x % 2001) as f32 / 1000.0 - 1.0
            })
            .collect()
    }

    fn graph(n: usize, dim: usize) -> Arc<HnswIndex> {
        let g = Arc::new(HnswIndex::new(16, 64));
        for i in 0..n {
            hnsw_insert(
                &g,
                format!("k{i}"),
                vector(dim, i as u64),
                &DistanceMetric::L2,
            );
        }
        g
    }

    #[test]
    fn a_waiting_vector_is_found_and_hides_what_the_graph_held() {
        let g = graph(600, 8);
        let metric = DistanceMetric::L2;
        // k5 is written over with a vector far from everything, and a new
        // key with the query itself; neither is linked yet.
        let far = vec![100.0; 8];
        let q = vector(8, 777_777);
        g.backlog.put("k5", far.clone().into(), 0);
        g.backlog.put("new", q.clone().into(), 0);
        let got = knn_hnsw(&g, &q, 10, 64, &metric);
        assert_eq!(got[0].key, "new");
        assert_eq!(got[0].score, 0.0);
        assert!(
            got.iter().all(|r| r.key != "k5"),
            "k5's old vector was found"
        );
        let got = knn_hnsw(&g, &far, 1, 64, &metric);
        assert_eq!(got[0].key, "k5");
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn a_stale_ticket_is_not_linked_and_the_latest_is() {
        let g = graph(50, 4);
        let metric = DistanceMetric::L2;
        let (old, _) = g.backlog.put("x", vec![1.0, 0.0, 0.0, 0.0].into(), 0);
        let (new, _) = g.backlog.put("x", vec![0.0, 0.0, 0.0, 9.0].into(), 0);
        link(&g, "x", old, &metric);
        assert!(
            !g.key_to_id.read().contains_key("x"),
            "a written-over vector was linked"
        );
        assert_eq!(g.backlog.len(), 1);
        link(&g, "x", new, &metric);
        assert!(g.backlog.is_empty());
        let id = g.key_to_id.read()["x"];
        let nodes = g.nodes.read();
        assert_eq!(g.vector_of(&nodes, id, "x").unwrap(), &[0.0, 0.0, 0.0, 9.0]);
    }

    #[test]
    fn a_delete_takes_a_waiting_vector_out() {
        let g = graph(50, 4);
        let (t, _) = g.backlog.put("gone", vec![1.0, 2.0, 3.0, 4.0].into(), 0);
        let live = crate::vector::LiveIndex::Hnsw(Arc::clone(&g));
        crate::vector::unindex_vector(&live, "gone");
        assert!(g.backlog.is_empty());
        link(&g, "gone", t, &DistanceMetric::L2);
        assert!(!g.key_to_id.read().contains_key("gone"));
    }

    #[test]
    fn an_insert_here_and_now_drops_a_waiting_vector() {
        let g = graph(50, 4);
        let (t, _) = g.backlog.put("y", vec![1.0, 1.0, 1.0, 1.0].into(), 0);
        hnsw_insert(
            &g,
            "y".into(),
            vec![2.0, 2.0, 2.0, 2.0],
            &DistanceMetric::L2,
        );
        assert!(g.backlog.is_empty());
        link(&g, "y", t, &DistanceMetric::L2);
        let id = g.key_to_id.read()["y"];
        let nodes = g.nodes.read();
        assert_eq!(g.vector_of(&nodes, id, "y").unwrap(), &[2.0, 2.0, 2.0, 2.0]);
    }

    #[test]
    fn writes_through_the_helpers_end_up_in_the_graph() {
        let metric = DistanceMetric::Cosine;
        let g = Arc::new(HnswIndex::new(16, 64));
        for i in 0..HERE_BELOW {
            hnsw_insert(&g, format!("k{i}"), vector(16, i as u64), &metric);
        }
        let n = 3000;
        for i in 0..n {
            write(&g, &format!("w{i}"), vector(16, 10_000 + i), &metric, 0);
            // Each is found from the moment it is written.
            if i % 500 == 0 {
                let q = vector(16, 10_000 + i);
                assert_eq!(knn_hnsw(&g, &q, 1, 64, &metric)[0].key, format!("w{i}"));
            }
        }
        assert!(settle(&g, std::time::Duration::from_secs(60)));
        assert_eq!(g.live_count.load(Relaxed), n as usize + HERE_BELOW);
        assert!(g.backlog.len() <= LIMIT);
        let q = vector(16, 10_123);
        assert_eq!(knn_hnsw(&g, &q, 1, 64, &metric)[0].key, "w123");
    }

    #[test]
    fn a_filtered_search_lets_in_only_admitted_waiting_vectors() {
        let g = graph(600, 8);
        let metric = DistanceMetric::L2;
        let q = vector(8, 31_337);
        g.backlog.put("in", q.clone().into(), 7);
        g.backlog.put("out", q.clone().into(), 8);
        let mut admit = |n: &HnswNode| n.key == "in" || n.key.ends_with('1');
        let (got, _) =
            crate::vector::knn_hnsw_admitting(&g, &q, 5, 64, &metric, &mut admit, usize::MAX, None)
                .unwrap();
        assert_eq!(got[0].key, "in");
        assert!(got.iter().all(|r| r.key != "out"));
        assert!(got[1..].iter().all(|r| r.key.ends_with('1')));
    }
}

/// Writers that write over and delete the same keys while searches run and
/// a compaction may swap the graph: every result a search gives is a state
/// of its key between the last write acknowledged before the search began
/// and the last one begun before it ended, no key comes twice, and once the
/// writers stop and the backlog empties the graph holds exactly the keys and
/// vectors written last, with the recall of a graph built from them serially.
#[cfg(test)]
mod concurrency {
    use super::*;
    use crate::store::Store;
    use crate::types::KiviDBValue;
    use crate::vector::{
        FieldOptions, FieldType, IndexOn, IndexSchema, IndexStore, LiveIndex, SchemaField,
        VectorAlgo, VectorType, hnsw_insert, knn_hnsw, maybe_index, maybe_unindex,
    };
    use std::sync::atomic::AtomicBool;

    const DIM: usize = 8;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn vector(&mut self) -> Vec<f32> {
            (0..DIM)
                .map(|_| (self.below(2001) as f32) / 1000.0 - 1.0)
                .collect()
        }
    }

    fn setup(name: &str, metric: &DistanceMetric) -> (Store, IndexStore) {
        let store = crate::store::new_store(1);
        let indexes = crate::vector::new_index_store();
        let def = IndexSchema {
            name: name.into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec![format!("{name}:")],
            fields: vec![SchemaField {
                name: "v".into(),
                field_type: FieldType::Vector {
                    algo: VectorAlgo::Hnsw {
                        m: 16,
                        ef_construction: 32,
                        ef_runtime: 10,
                    },
                    dim: DIM,
                    metric: metric.clone(),
                    data_type: VectorType::Float32,
                },
                options: FieldOptions::default(),
            }],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
        }
        .into_def();
        indexes.write().insert(name.into(), def);
        (store, indexes)
    }

    /// An `HSET` or a `DEL` of `key`: the store, then the index. The caller
    /// holds the key's write order, as a command does.
    fn write(store: &Store, indexes: &IndexStore, key: &str, v: Option<&[f32]>) {
        match v {
            Some(v) => {
                let blob: Arc<[u8]> = v
                    .iter()
                    .flat_map(|x| x.to_le_bytes())
                    .collect::<Vec<u8>>()
                    .into();
                let pairs: Vec<(String, Arc<[u8]>)> = vec![("v".into(), blob)];
                let hash: crate::types::HashFields = pairs.iter().cloned().collect();
                crate::store::get_shard(&store[0], key).write().insert(
                    key,
                    KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(hash))),
                );
                maybe_index(indexes, 0, key, &pairs, crate::vector::Write::Whole);
            }
            None => {
                crate::store::get_shard(&store[0], key).write().remove(key);
                maybe_unindex(indexes, 0, key);
            }
        }
    }

    fn graph(indexes: &IndexStore, name: &str) -> Arc<HnswIndex> {
        match indexes.read().get(name).unwrap().indexes.get("v") {
            Some(LiveIndex::Hnsw(g)) => Arc::clone(g),
            _ => unreachable!(),
        }
    }

    /// The distance a search reports for `v`.
    fn dist(metric: &DistanceMetric, q: &[f32], v: &[f32]) -> f32 {
        let s = super::super::as_stored(metric, v.to_vec());
        let q = super::super::as_stored(metric, q.to_vec());
        distance_prenorm(metric, &q, &s)
    }

    /// Per key: every state written to it, in order (None: deleted), and how
    /// many of them are acknowledged.
    #[derive(Default)]
    struct History {
        states: Vec<Option<Vec<f32>>>,
        acked: usize,
    }

    type Log = parking_lot::Mutex<AHashMap<String, History>>;

    fn recall(found: &[KnnResult], want: &[String]) -> usize {
        found.iter().filter(|r| want.contains(&r.key)).count()
    }

    fn run(seed: u64, metric: DistanceMetric, compact: bool) {
        let name = format!("pc{seed}{}", if compact { "c" } else { "" });
        let (store, indexes) = setup(&name, &metric);
        let mut rng = Rng(seed | 1);
        // Past HERE_BELOW, so that writes go through the backlog.
        for i in 0..1200 {
            write(
                &store,
                &indexes,
                &format!("{name}:b{i}"),
                Some(&rng.vector()),
            );
        }
        let log: Arc<Log> = Arc::new(parking_lot::Mutex::new(AHashMap::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let hot = 150u64;
        let mut threads = Vec::new();
        for w in 0..4u64 {
            let (store, indexes, log, name) = (
                Arc::clone(&store),
                indexes.clone(),
                Arc::clone(&log),
                name.clone(),
            );
            threads.push(std::thread::spawn(move || {
                let mut rng = Rng((seed * 31 + w) | 1);
                for _ in 0..1500 {
                    let key = format!("{name}:h{}", rng.below(hot));
                    // Overwrite, delete, or overwrite + delete + re-add.
                    let ops: Vec<Option<Vec<f32>>> = match rng.below(20) {
                        0..=11 => vec![Some(rng.vector())],
                        12..=16 => vec![None],
                        _ => vec![Some(rng.vector()), None, Some(rng.vector())],
                    };
                    for op in ops {
                        let _order =
                            crate::write_order::lock_keys(0, std::iter::once(key.as_bytes()));
                        log.lock()
                            .entry(key.clone())
                            .or_default()
                            .states
                            .push(op.clone());
                        write(&store, &indexes, &key, op.as_deref());
                        log.lock().get_mut(&key).unwrap().acked += 1;
                    }
                }
            }));
        }
        let mut searchers = Vec::new();
        for s in 0..3u64 {
            let (indexes, log, stop, name, metric) = (
                indexes.clone(),
                Arc::clone(&log),
                Arc::clone(&stop),
                name.clone(),
                metric.clone(),
            );
            searchers.push(std::thread::spawn(move || {
                let mut rng = Rng((seed * 97 + s) | 1);
                let mut checked = 0usize;
                while !stop.load(Relaxed) {
                    let q = rng.vector();
                    let before: AHashMap<String, usize> = log
                        .lock()
                        .iter()
                        .map(|(k, h)| (k.clone(), h.acked))
                        .collect();
                    let found = knn_hnsw(&graph(&indexes, &name), &q, 20, 64, &metric);
                    let log = log.lock();
                    let mut keys = ahash::AHashSet::new();
                    for r in &found {
                        assert!(keys.insert(r.key.clone()), "{} came twice", r.key);
                        let Some(h) = log.get(&r.key) else {
                            continue; // a base key, never written again
                        };
                        // From the state acknowledged when the search began
                        // (none if no write was) to the last one begun.
                        let from = before.get(&r.key).copied().unwrap_or(0).saturating_sub(1);
                        let ok = h.states[from..].iter().any(|st| {
                            st.as_ref().is_some_and(|v| {
                                let d = dist(&metric, &q, v);
                                (d - r.score).abs() <= 1e-4 * (1.0 + d.abs())
                            })
                        });
                        assert!(
                            ok,
                            "{} at {} is no state of it since the search began",
                            r.key, r.score
                        );
                    }
                    checked += found.len();
                }
                checked
            }));
        }
        if compact {
            std::thread::sleep(std::time::Duration::from_millis(20));
            let _ = crate::vector::compact_hnsw(&indexes, &store, &name, "v");
        }
        for t in threads {
            t.join().unwrap();
        }
        stop.store(true, Relaxed);
        for s in searchers {
            assert!(s.join().unwrap() > 0);
        }
        let g = graph(&indexes, &name);
        assert!(settle(&g, std::time::Duration::from_secs(120)));
        // The graph holds exactly what was written last.
        let log = log.lock();
        let mut want: AHashMap<String, Vec<f32>> = AHashMap::new();
        for (k, h) in log.iter() {
            if let Some(Some(v)) = h.states.last() {
                want.insert(k.clone(), v.clone());
            }
        }
        {
            let nodes = g.nodes.read();
            let ids = g.key_to_id.read();
            for (k, v) in &want {
                let id = *ids.get(k).unwrap_or_else(|| panic!("{k} is missing"));
                let held = g.vector_of(&nodes, id, k).expect("live");
                let stored = super::super::as_stored(&metric, v.clone());
                assert_eq!(held, stored.as_slice(), "{k}");
            }
            let hot_held = ids.keys().filter(|k| k.contains(":h")).count();
            assert_eq!(hot_held, want.len(), "a deleted key is still in the graph");
        }
        assert_eq!(g.live_count.load(Relaxed), 1200 + want.len());
        // Recall against a graph built serially from the same vectors.
        let serial = Arc::new(HnswIndex::new(16, 32));
        let mut all: Vec<(String, Vec<f32>)> = Vec::new();
        {
            let nodes = g.nodes.read();
            for (k, &id) in g.key_to_id.read().iter() {
                all.push((k.clone(), g.vector_of(&nodes, id, k).unwrap().to_vec()));
            }
        }
        for (k, v) in &all {
            hnsw_insert(&serial, k.clone(), v.clone(), &metric);
        }
        let (mut got, mut base) = (0, 0);
        for _ in 0..60 {
            let q = rng.vector();
            let mut exact: Vec<(f32, &String)> =
                all.iter().map(|(k, v)| (dist(&metric, &q, v), k)).collect();
            exact.sort_by(|a, b| a.0.total_cmp(&b.0));
            let want: Vec<String> = exact.iter().take(10).map(|e| e.1.clone()).collect();
            got += recall(&knn_hnsw(&g, &q, 10, 50, &metric), &want);
            base += recall(&knn_hnsw(&serial, &q, 10, 50, &metric), &want);
        }
        assert!(
            got + 18 >= base,
            "recall {got}/600 against {base}/600 for a graph built serially"
        );
    }

    #[test]
    fn writers_searchers_and_compactions_agree_with_a_serial_order() {
        let helpers_before = helpers();
        for seed in 1..=8u64 {
            for metric in [DistanceMetric::L2, DistanceMetric::Cosine] {
                run(seed, metric, seed % 2 == 0);
            }
        }
        // The helpers were started once, not per index or per burst.
        assert_eq!(helpers(), helpers_before);
    }

    /// An index dropped while vectors wait and helpers link them: the
    /// helpers finish with its graph and let it go.
    #[test]
    fn a_dropped_index_is_let_go_by_the_helpers() {
        let metric = DistanceMetric::L2;
        let helpers_before = helpers();
        for round in 0..6u64 {
            let name = format!("dr{round}");
            let (store, indexes) = setup(&name, &metric);
            let mut rng = Rng(round + 11);
            for i in 0..3000 {
                let key = format!("{name}:{i}");
                let _order = crate::write_order::lock_keys(0, std::iter::once(key.as_bytes()));
                write(&store, &indexes, &key, Some(&rng.vector()));
            }
            let g = graph(&indexes, &name);
            if round % 2 == 0 {
                // FLUSHALL
                crate::vector::drop_all_indexes(&indexes);
            } else {
                // FT.DROPINDEX
                assert!(crate::vector::remove_index(&mut indexes.write(), &name));
            }
            assert!(settle(&g, std::time::Duration::from_secs(60)));
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while Arc::strong_count(&g) > 1 {
                assert!(
                    std::time::Instant::now() < until,
                    "a helper kept the dropped graph"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        assert_eq!(helpers(), helpers_before);
    }
}
