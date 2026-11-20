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
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
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
    /// A job for the key is in the helpers' queue and has not taken the
    /// vector yet: a later write of the key leaves the job to take its
    /// vector instead of queueing another.
    queued: bool,
}

/// The vectors that waited when a search began ([`Backlog::snapshot`]):
/// key, vector as the graph stores it, document id.
pub(crate) type Snapshot = Vec<(Arc<str>, Arc<[f32]>, u32)>;

/// One graph's vectors waiting to be linked, by key.
pub struct Backlog {
    waiting: parking_lot::RwLock<AHashMap<Arc<str>, Waiting>>,
    /// How many there are, read without the lock.
    len: AtomicUsize,
    /// Jobs of this graph in the helpers' queue (taken or not).
    jobs: AtomicUsize,
    tickets: AtomicU64,
    keys: Box<[parking_lot::Mutex<()>]>,
    /// The helpers leave this graph's jobs alone ([`Backlog::hold`]).
    #[cfg(test)]
    held: std::sync::atomic::AtomicBool,
}

impl Default for Backlog {
    fn default() -> Self {
        Self {
            waiting: parking_lot::RwLock::new(AHashMap::new()),
            len: AtomicUsize::new(0),
            jobs: AtomicUsize::new(0),
            #[cfg(test)]
            held: std::sync::atomic::AtomicBool::new(false),
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

    /// Jobs of this graph in the helpers' queue: at most one per waiting
    /// key, plus those a write that linked its own vector left behind.
    pub fn queued(&self) -> usize {
        self.jobs.load(Acquire)
    }

    /// Bytes the waiting vectors hold (their keys and floats, and the map's
    /// entries) and this graph's jobs in the helpers' queue: allocated, so
    /// counted in `used_memory` too.
    pub fn bytes(&self) -> usize {
        let jobs = self.queued() * std::mem::size_of::<Job>();
        if self.is_empty() {
            return jobs;
        }
        let waiting = self.waiting.read();
        jobs + waiting.capacity() * (std::mem::size_of::<(Arc<str>, Waiting)>() + 1)
            + waiting
                .iter()
                .map(|(k, w)| k.len() + 16 + w.vector.len() * 4 + 16)
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

    /// The waiting vectors, held for reading: only for as long as it takes
    /// to look a few keys up, never across a search of the graph (a write
    /// waits on it, and so then does every later search).
    pub(crate) fn read(&self) -> parking_lot::RwLockReadGuard<'_, AHashMap<Arc<str>, Waiting>> {
        self.waiting.read()
    }

    /// Every waiting vector, by reference count: taken by a search before
    /// it searches the graph, so that the backlog is not held meanwhile.
    ///
    /// A key in it is measured as it waited, and what the graph finds for
    /// it is left out. A key that waited and is not in it any more had its
    /// vector linked or was deleted before the snapshot (`done`/`forget`
    /// take a vector out only after the graph has the change), so the graph
    /// is right about it; one written after the snapshot is a write the
    /// search may miss, having begun before it.
    pub(crate) fn snapshot(&self) -> Snapshot {
        if self.is_empty() {
            return Vec::new();
        }
        let waiting = self.waiting.read();
        waiting
            .iter()
            .map(|(k, w)| (Arc::clone(k), Arc::clone(&w.vector), w.doc))
            .collect()
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
    /// how many wait now, and whether a job is to be queued for it (none is
    /// when one already is and has not taken the vector it waited for: it
    /// takes this one). The key (an `Arc`) is for that job.
    fn put(&self, key: &str, vector: Arc<[f32]>, doc: u32) -> (u64, usize, Option<Arc<str>>) {
        let ticket = self.tickets.fetch_add(1, Relaxed);
        let mut waiting = self.waiting.write();
        let job = if let Some(w) = waiting.get_mut(key) {
            w.ticket = ticket;
            w.vector = vector;
            w.doc = doc;
            if std::mem::replace(&mut w.queued, true) {
                None
            } else {
                waiting.get_key_value(key).map(|(k, _)| Arc::clone(k))
            }
        } else {
            let k: Arc<str> = Arc::from(key);
            waiting.insert(
                Arc::clone(&k),
                Waiting {
                    ticket,
                    vector,
                    doc,
                    queued: true,
                },
            );
            Some(k)
        };
        self.len.store(waiting.len(), Release);
        (ticket, waiting.len(), job)
    }

    /// The vector of `key` if `ticket` is still its own.
    fn current(&self, key: &str, ticket: u64) -> Option<(Arc<[f32]>, u32)> {
        let waiting = self.waiting.read();
        let w = waiting.get(key).filter(|w| w.ticket == ticket)?;
        Some((Arc::clone(&w.vector), w.doc))
    }

    /// A queued job of `key` takes the key's latest vector: its ticket,
    /// vector and document id, if the vector still waits for the job (a
    /// write that linked its own may have taken it out). A write of the key
    /// after this queues a job of its own.
    fn take(&self, key: &str) -> Option<(u64, Arc<[f32]>, u32)> {
        let mut waiting = self.waiting.write();
        let w = waiting.get_mut(key).filter(|w| w.queued)?;
        w.queued = false;
        Some((w.ticket, Arc::clone(&w.vector), w.doc))
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
        shadow.mirror(key, Some(stored), metric, doc);
    }
}

/// [`write`] to `index` alone, `vector` as the graph stores it.
///
/// A compaction going on changes nothing here: it copies the waiting
/// vectors with the graph's, and every write made once it began reaches
/// the new graph too ([`HnswIndex::mirror`]).
pub(super) fn write_one(
    index: &Arc<HnswIndex>,
    key: &str,
    vector: Arc<[f32]>,
    metric: &DistanceMetric,
    doc: u32,
) {
    // The first vector makes the slots (and sets the dimension).
    let here = index.slots().is_none_or(|s| s.dim() != vector.len())
        || index.live_count.load(Relaxed) < HERE_BELOW
        || helpers() == 0;
    if here {
        let _key = index.backlog.lock_key(key);
        let seen = index.backlog.ticket(key);
        let linking = Linking::start();
        super::insert_unit(index, key.to_string(), vector.to_vec(), metric, doc);
        linking.end();
        index.backlog.forget(key, seen);
        return;
    }
    let most = limit(vector.len());
    let (ticket, waiting, job) = index.backlog.put(key, vector, doc);
    // A full backlog, or a queue of this graph's jobs twice that long (left
    // behind by writes that linked their own vectors while the helpers had
    // no CPU time): the write links its vector itself.
    if waiting > most || (job.is_some() && index.backlog.queued() >= 2 * most) {
        link_own(index, key, ticket, metric);
        return;
    }
    let Some(key) = job else {
        return; // the key's queued job takes this vector
    };
    index.backlog.jobs.fetch_add(1, AcqRel);
    let mut jobs = POOL.jobs.lock();
    jobs.push_back(Job {
        index: Arc::downgrade(index),
        key,
        metric: metric.clone(),
    });
    drop(jobs);
    POOL.ready.notify_one();
}

/// Link `key`'s waiting vector into the graph if `ticket` is still its own:
/// a write that found the backlog full links its own vector.
fn link_own(index: &Arc<HnswIndex>, key: &str, ticket: u64, metric: &DistanceMetric) {
    let _key = index.backlog.lock_key(key);
    let Some((vector, doc)) = index.backlog.current(key, ticket) else {
        return;
    };
    let linking = Linking::start();
    insert_then_done(index, key, ticket, vector, metric, doc);
    linking.end();
}

/// A helper's job: link the latest vector of `key`, if one still waits for
/// a job.
fn link_queued(index: &Arc<HnswIndex>, key: &str, metric: &DistanceMetric) {
    let _key = index.backlog.lock_key(key);
    let Some((ticket, vector, doc)) = index.backlog.take(key) else {
        return;
    };
    insert_then_done(index, key, ticket, vector, metric, doc);
}

fn insert_then_done(
    index: &Arc<HnswIndex>,
    key: &str,
    ticket: u64,
    vector: Arc<[f32]>,
    metric: &DistanceMetric,
    doc: u32,
) {
    super::insert_unit(index, key.to_string(), vector.to_vec(), metric, doc);
    #[cfg(test)]
    crate::test_hooks::at("backlog: linked");
    index.backlog.done(key, ticket);
}

/// The `k` nearest of `found` (the graph's, nearest first) and the
/// vectors of `waiting` (those `admit` lets in, if given), taken before
/// the graph was searched: a key that waited is measured as it waited, and
/// what the graph found for it is left out. `query` is as the graph holds
/// vectors.
pub(crate) fn merge(
    waiting: &Snapshot,
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
    {
        let keys: ahash::AHashSet<&str> = waiting.iter().map(|(k, _, _)| &**k).collect();
        found.retain(|hit| !keys.contains(hit.key.as_str()));
    }
    found.truncate(k);
    // The nearest k of the waiting ones, by reference; keys are copied only
    // for those kept.
    let mut near: Vec<(f32, &str)> = Vec::with_capacity(k.min(waiting.len()) + 1);
    let mut bound = f32::INFINITY;
    for (key, vector, doc) in waiting.iter() {
        if vector.len() != query.len() {
            continue;
        }
        let d = distance_prenorm(metric, query, vector);
        if near.len() >= k && d.total_cmp(&bound).is_ge() {
            continue;
        }
        if let Some(admit) = admit.as_mut() {
            let node = HnswNode::new(key.to_string(), 0, Vec::new(), *doc);
            if !admit(&node) {
                continue;
            }
        }
        near.push((d, key));
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

/// A key of a graph to link. Weak: a graph dropped (`FT.DROPINDEX`,
/// `FLUSHALL`) or replaced by a compaction is freed at once, not when the
/// helpers get to its jobs, which then find nothing to do.
struct Job {
    index: std::sync::Weak<HnswIndex>,
    key: Arc<str>,
    metric: DistanceMetric,
}

struct Pool {
    jobs: parking_lot::Mutex<VecDeque<Job>>,
    /// Helpers that take jobs ([`ACTIVE`]) wait here for one.
    ready: parking_lot::Condvar,
    /// The others wait here for [`ACTIVE`] to take them in again. Apart, so
    /// that a write's `notify_one` always wakes a helper that takes jobs.
    parked: parking_lot::Condvar,
}

static POOL: Pool = Pool {
    jobs: parking_lot::const_mutex(VecDeque::new()),
    ready: parking_lot::Condvar::new(),
    parked: parking_lot::Condvar::new(),
};

/// Helper threads running: 0 until the first write asks, when
/// `search-index-threads` (or its `auto` rule) says none, and if none could
/// be started.
static HELPERS: AtomicUsize = AtomicUsize::new(0);
/// Of them, how many take jobs: helpers `0..ACTIVE`; the rest are parked.
/// All of them with a number; with `auto`, from [`AUTO_FLOOR`] to all,
/// as [`adapt`] decides. Changed under the queue's lock ([`set_active`]),
/// at least one while any runs.
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
/// `auto`'s count while the server serves ([`auto_helpers`]).
static AUTO_FLOOR: AtomicUsize = AtomicUsize::new(0);
/// Whether [`adapt`] changes [`ACTIVE`]: `auto`, with more helpers started
/// than its floor.
static ADAPTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static START: std::sync::Once = std::sync::Once::new();

/// `search-index-threads`: how many helper threads link the vectors written
/// to HNSW indexes. Read at startup only (the helpers start once, on the
/// first write that needs them).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexThreads {
    /// [`auto_most`] helpers, of which [`auto_helpers`]'s count take jobs
    /// while the server serves clients, and all of them while it only
    /// takes the writes that fill the backlogs ([`adapt`]).
    #[default]
    Auto,
    /// Exactly this many; 0: each write links its vector itself, as 1.2 did.
    Count(usize),
}

/// The most helpers `search-index-threads` takes.
pub const MAX_INDEX_THREADS: usize = 256;

impl IndexThreads {
    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("auto") {
            return Some(Self::Auto);
        }
        value
            .parse::<usize>()
            .ok()
            .filter(|n| *n <= MAX_INDEX_THREADS)
            .map(Self::Count)
    }

    pub fn name(self) -> String {
        match self {
            Self::Auto => "auto".to_string(),
            Self::Count(n) => n.to_string(),
        }
    }
}

/// `search-index-threads` as the server started with it.
static CONFIGURED: parking_lot::Mutex<IndexThreads> = parking_lot::const_mutex(IndexThreads::Auto);

/// Set `search-index-threads`, before the first write (at startup).
pub fn configure(threads: IndexThreads) {
    *CONFIGURED.lock() = threads;
}

/// With fewer CPUs than this, `auto` starts no helper.
pub const AUTO_FROM_CPUS: usize = 8;

/// `search-index-threads auto`: no helper under a CPU quota or with fewer
/// than [`AUTO_FROM_CPUS`] CPUs, otherwise (CPUs - 2) / 2, at least one.
///
/// The helpers run beside the workers, which are half the CPUs, and
/// take CPU time they would otherwise have: serving latency is what `auto`
/// keeps. Under a quota (a container's `--cpus`, a Kubernetes CPU limit)
/// `SCHED_IDLE` keeps them off busy CPUs but not off the quota: with idle
/// CPUs beside them they run until the whole process is throttled. In a
/// container with `--cpus 2` on an 8-vCPU D8s_v5, memtier GET/SET from the
/// host while one client loaded 384-d vectors (30 s, 3 runs): with one
/// helper 93-96k a second at a p99 of 8.0-9.3 ms, throttled in every
/// period, against 141-142k at 0.35-0.38 ms with the vectors linked in the
/// writes; with `--cpus 4` and two, 119-121k at 0.95-0.98 ms against
/// 147-150k at 0.32-0.35 ms.
///
/// On 4 vCPUs of a D8s_v5 with every one busy (`--threads 4`, memtier
/// GET/SET from 50 connections, one client loading 384-d vectors, a KNN
/// probe and one `HSET` every 2 ms on the other CPUs, 40 s, 3 runs): GET/SET
/// p99 1.6-1.7 ms with none (as master), 2.0-2.2 ms with one (1.3x), 2.5-2.7
/// ms with two (1.6x); KNN p50 0.85-0.90, 1.04-1.10 and 1.10-1.14 ms; the
/// build alone 1,540, 3,190 and 3,620 documents a second. Over 8 writer
/// connections, those on the bulk loader's worker waited behind its
/// pipelines in every build (median 15-25 ms with none, 12-16 ms with one);
/// on the others one helper took the `HSET` p99 from 2.3-2.7 ms to 4.5-4.7.
/// A small server that serves while it loads is better off without them, so
/// `auto` starts them from 8 CPUs; a number starts them on any machine.
pub fn auto_helpers(cpus: usize, quota: bool) -> usize {
    if quota || cpus < AUTO_FROM_CPUS {
        return 0;
    }
    (cpus.saturating_sub(2) / 2).max(1)
}

/// The most helpers `auto` runs, while the server serves nothing but the
/// writes that fill the backlogs ([`adapt`]): one fewer than the CPUs, the
/// CPU left being the loading connection's worker. None where
/// [`auto_helpers`] starts none.
///
/// On an Azure D8s_v5 (4 cores, 8 vCPUs; one client loading in `HSET`
/// pipelines of 1,000, the build timed until nothing waits), 50,000 384-d
/// documents took 7.9 s with 3 helpers, 6.7 s with 5 and 6.1 s with 7,
/// against 5.8 s for Redis 8.10; 200,000 64-d ones 21.2, 17.5 and 15.9 s,
/// against 12.2 s.
pub fn auto_most(cpus: usize, quota: bool) -> usize {
    let floor = auto_helpers(cpus, quota);
    if floor == 0 {
        return 0;
    }
    cpus.saturating_sub(1).max(floor)
}

/// How many helpers link vectors, started once, on the first call:
/// `search-index-threads`, or by [`auto_most`] for `auto`, of which
/// [`auto_helpers`]'s count take jobs while the server serves.
fn helpers() -> usize {
    START.call_once(|| {
        // The process's CPUs: the first write may come from a worker bound
        // to one (`worker-cpu-affinity`), and std would see only that one.
        let cpus = crate::cpus::available_parallelism().map_or(1, |n| n.get());
        let (floor, most) = match *CONFIGURED.lock() {
            IndexThreads::Count(n) => (n, n),
            // Unit tests have helpers whatever the machine, and some to
            // take in and park: they test them.
            IndexThreads::Auto if cfg!(test) => {
                let floor = (cpus / 2).max(1);
                (floor, cpus.saturating_sub(1).max(floor + 2))
            }
            IndexThreads::Auto => {
                let quota = crate::cpus::cpu_quota().is_some();
                (auto_helpers(cpus, quota), auto_most(cpus, quota))
            }
        };
        // Before they start: a helper that found 0 would park until the
        // count changed.
        ACTIVE.store(floor, Relaxed);
        let mut started = 0;
        for n in 0..most {
            use crate::cpus::BuilderExt;
            let spawned = std::thread::Builder::new()
                .name(format!("hnsw-link-{n}"))
                .spawn_released(move || helper(n));
            if spawned.is_ok() {
                started += 1;
            } else {
                break; // the helpers are numbered from 0 up, with no gap
            }
        }
        let floor = floor.min(started);
        AUTO_FLOOR.store(floor, Relaxed);
        set_active(floor);
        #[cfg(test)]
        let pinned_to_most = std::env::var("KIVIDB_TEST_INDEX_HELPERS").is_ok_and(|v| v == "max");
        #[cfg(not(test))]
        let pinned_to_most = false;
        if pinned_to_most {
            set_active(started);
        } else {
            ADAPTIVE.store(started > floor && floor > 0, Release);
        }
        HELPERS.store(started, Release);
        if started > floor {
            crate::log_info!(
                "HNSW index threads: {floor} while serving, up to {started} while only \
                 loading (search-index-threads {})",
                CONFIGURED.lock().name()
            );
        } else {
            crate::log_info!(
                "HNSW index threads: {started} (search-index-threads {})",
                CONFIGURED.lock().name()
            );
        }
    });
    HELPERS.load(Acquire)
}

/// Helper threads started, and how many of them take jobs now: `FT.INFO`'s
/// `hnsw_index_threads` and `hnsw_index_threads_active` (0 and 0 before the
/// first write that needs them).
pub fn threads() -> (usize, usize) {
    (HELPERS.load(Acquire), ACTIVE.load(Relaxed))
}

/// Let helpers `0..n` take jobs, and park the others once they finish the
/// job they have.
fn set_active(n: usize) {
    if ACTIVE.load(Relaxed) == n {
        return;
    }
    {
        let _jobs = POOL.jobs.lock();
        ACTIVE.store(n, Relaxed);
    }
    POOL.ready.notify_all();
    POOL.parked.notify_all();
}

fn helper(n: usize) {
    lower_priority();
    let mut clock = HelperClock::start();
    loop {
        let job = {
            let mut jobs = POOL.jobs.lock();
            loop {
                if n >= ACTIVE.load(Relaxed) {
                    POOL.parked.wait(&mut jobs);
                    continue;
                }
                if let Some(job) = jobs.pop_front() {
                    break job;
                }
                POOL.ready.wait(&mut jobs);
            }
        };
        if let Some(index) = job.index.upgrade() {
            link_job(&index, job);
        } // else the graph is gone
        if ADAPTIVE.load(Relaxed) {
            clock.job_done();
            adapt();
        }
    }
}

fn link_job(index: &Arc<HnswIndex>, job: Job) {
    #[cfg(test)]
    let Some(job) = put_aside(index, job) else {
        return;
    };
    // A panic would leave the vector waiting, where searches still
    // measure it, and the helper gone: it is caught, and the helper
    // goes on to the next.
    let linked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        link_queued(index, &job.key, &job.metric)
    }));
    index.backlog.jobs.fetch_sub(1, AcqRel);
    if linked.is_err() {
        crate::log_warn!("an HNSW insert failed in a helper thread");
    }
}

// ── How many helpers `auto` runs ─────────────────────────────────────────────
//
// `auto` starts `auto_most` helpers and lets `auto_helpers` of them take jobs
// while the server serves clients. While it does nothing but take the writes
// that fill the backlogs (a bulk load, `FT.CREATE` over existing keys, a
// replica's full sync), all of them take jobs. Nothing is added to the
// workers' loop to tell: every `TICK` the helper that has just finished a
// job reads counters that are kept anyway, and the server counts as serving
// over that tick if, besides the commands that wrote vectors, it served
// `SERVING_OPS` commands a second, or if its threads, besides the helpers,
// the links made in writes and the commands that wrote vectors (an estimate
// of what each costs, times their number), used `SERVING_MILLICPUS` of CPU
// time.

/// How often the helpers look at what the server is doing.
const TICK: std::time::Duration = std::time::Duration::from_millis(50);
/// Quiet looks in a row before the parked helpers are taken in: 200 ms.
const QUIET_TICKS: u32 = 4;
/// A look this long after the last one finds that the helpers had nothing
/// to do meanwhile: what was going on then says nothing about now, and the
/// count goes back to the floor until the next quiet looks.
const STALE: std::time::Duration = std::time::Duration::from_millis(200);
/// Commands a second, besides those that wrote a vector, from which the
/// server counts as serving.
pub const SERVING_OPS: u64 = 500;
/// CPU time a second (in thousandths of a CPU) the process uses besides the
/// helpers, the links made in writes and the commands that wrote vectors,
/// from which the server counts as serving.
pub const SERVING_MILLICPUS: u64 = 1000;

/// The helpers' CPU clocks (Linux): a look reads each, so a job that runs
/// across a look is counted up to that look, not all of it at the next.
#[cfg(target_os = "linux")]
static HELPER_CLOCKS: parking_lot::Mutex<Vec<libc::clockid_t>> =
    parking_lot::const_mutex(Vec::new());
/// Elsewhere, the CPU time the helpers used, added at the end of each job:
/// a look then misses the part of a job that ran before it (at most one
/// job's time per helper, about 0.6 ms at 384-d, against a look's 50 ms)
/// and the next look counts it.
#[cfg(not(target_os = "linux"))]
static HELPER_CPU_NS: AtomicU64 = AtomicU64::new(0);
/// CPU time writes spent linking their own vectors (a full backlog, a small
/// graph, a walk over the keys).
static LINK_CPU_NS: AtomicU64 = AtomicU64::new(0);
/// Commands that wrote a vector: [`count_write`].
static WRITES: AtomicU64 = AtomicU64::new(0);
/// What a command that writes a vector costs the thread that runs it,
/// besides linking its own vectors: reading it, storing the document,
/// indexing it, replying. A running estimate in nanoseconds, from one such
/// command in [`SAMPLE_EVERY`] on each thread ([`count_write`]).
static WRITE_CPU_NS: AtomicU64 = AtomicU64::new(0);
/// One in this many commands that write a vector, on each thread, is timed.
const SAMPLE_EVERY: u32 = 64;

/// A thread's commands that wrote vectors, for [`count_write`].
#[derive(Clone, Copy)]
struct Writer {
    /// The thread's command ([`crate::stats::commands_on_this_thread`])
    /// counted last.
    command: u64,
    /// Commands counted on this thread.
    counted: u32,
    /// A command being timed: its number, and the thread's CPU time and
    /// [`Writer::linked_ns`] when it was counted.
    timing: Option<(u64, u64, u64)>,
    /// CPU time this thread spent linking its own vectors ([`Linking`]).
    linked_ns: u64,
}

thread_local! {
    static WRITER: std::cell::Cell<Writer> = const {
        std::cell::Cell::new(Writer {
            command: 0,
            counted: 0,
            timing: None,
            linked_ns: 0,
        })
    };
}

/// Commands counted on this thread, for the tests.
#[cfg(test)]
pub(crate) fn counted_here() -> u32 {
    WRITER.with(|w| w.get().counted)
}

/// The command running on this thread writes a vector to an index: such
/// commands are the load the extra helpers are for, and are not counted as
/// serving. Counted once per command, however many indexes and vector
/// fields it writes: a command's number on its thread
/// ([`crate::stats::commands_on_this_thread`]) is the same from its start to
/// its end. Never on a thread that has run no command (number 0), and the
/// walks over the keys (`FT.CREATE`, loading a snapshot, a replica's full
/// sync: `backfill_from_db_progress`) do not call it.
///
/// One counted command in [`SAMPLE_EVERY`] is timed, on the thread's CPU
/// clock, from here to the same point of the next command the thread runs,
/// if that one writes a vector too: one command's whole cost, the parsing
/// and the reply included, less what the thread spent linking its own
/// vectors meanwhile. Otherwise the timing is dropped. Two clock reads per
/// timed command, none for the others.
pub(crate) fn count_write() {
    if !cfg!(test) && !ADAPTIVE.load(Relaxed) {
        return;
    }
    let command = crate::stats::commands_on_this_thread();
    WRITER.with(|cell| {
        let mut w = cell.get();
        if command == 0 || command == w.command {
            return;
        }
        w.command = command;
        w.counted = w.counted.wrapping_add(1);
        WRITES.fetch_add(1, Relaxed);
        if let Some((from, cpu, linked)) = w.timing.take() {
            if command == from.wrapping_add(1) {
                let used = thread_cpu_ns()
                    .saturating_sub(cpu)
                    .saturating_sub(w.linked_ns.saturating_sub(linked));
                note_write_cpu(used);
            }
        } else if w.counted.is_multiple_of(SAMPLE_EVERY) {
            w.timing = Some((command, thread_cpu_ns(), w.linked_ns));
        }
        cell.set(w);
    });
}

/// One timed command's CPU time into [`WRITE_CPU_NS`]: an eighth of the way
/// from the estimate to it. Samples from two threads at once may lose one.
fn note_write_cpu(used: u64) {
    let old = WRITE_CPU_NS.load(Relaxed);
    let new = if old == 0 {
        used
    } else {
        old - old / 8 + used / 8
    };
    WRITE_CPU_NS.store(new, Relaxed);
}

/// The CPU time a write spends linking a vector, when `auto` adapts.
struct Linking(Option<u64>);

impl Linking {
    fn start() -> Self {
        Self(ADAPTIVE.load(Relaxed).then(thread_cpu_ns))
    }

    fn end(self) {
        if let Some(from) = self.0 {
            let used = thread_cpu_ns().saturating_sub(from);
            LINK_CPU_NS.fetch_add(used, Relaxed);
            WRITER.with(|cell| {
                let mut w = cell.get();
                w.linked_ns = w.linked_ns.wrapping_add(used);
                cell.set(w);
            });
        }
    }
}

/// The helpers' CPU time so far.
fn helpers_cpu_ns() -> u64 {
    #[cfg(target_os = "linux")]
    {
        HELPER_CLOCKS
            .lock()
            .iter()
            .map(|&clock| cpu_clock_ns(clock))
            .sum()
    }
    #[cfg(not(target_os = "linux"))]
    {
        HELPER_CPU_NS.load(Relaxed)
    }
}

/// A helper's CPU clock, for [`helpers_cpu_ns`]: on Linux each helper
/// gives its clock once, as it starts; elsewhere it adds its time at the
/// end of each job.
struct HelperClock {
    #[cfg(not(target_os = "linux"))]
    cpu: u64,
}

impl HelperClock {
    fn start() -> Self {
        #[cfg(target_os = "linux")]
        {
            let mut clock: libc::clockid_t = 0;
            // SAFETY: writes `clock` only, for this thread, which never ends.
            if unsafe { libc::pthread_getcpuclockid(libc::pthread_self(), &mut clock) } == 0 {
                HELPER_CLOCKS.lock().push(clock);
            }
            Self {}
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self {
                cpu: thread_cpu_ns(),
            }
        }
    }

    /// A job is done.
    fn job_done(&mut self) {
        #[cfg(not(target_os = "linux"))]
        {
            let now = thread_cpu_ns();
            HELPER_CPU_NS.fetch_add(now.saturating_sub(self.cpu), Relaxed);
            self.cpu = now;
        }
    }
}

/// This thread's CPU time, in nanoseconds.
fn thread_cpu_ns() -> u64 {
    cpu_clock_ns(libc::CLOCK_THREAD_CPUTIME_ID)
}

fn cpu_clock_ns(clock: libc::clockid_t) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: writes `ts` only; a failure leaves it at zero.
    unsafe { libc::clock_gettime(clock, &mut ts) };
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// What the counters read at one look.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reading {
    /// Since some fixed point, nanoseconds.
    pub wall_ns: u64,
    /// The process's CPU time.
    pub cpu_ns: u64,
    /// Of it, the helpers' and the writes' links.
    pub linking_ns: u64,
    /// Commands served.
    pub commands: u64,
    /// Of them, those that wrote a vector.
    pub writes: u64,
    /// What one of those costs besides its links ([`WRITE_CPU_NS`]).
    pub write_cpu_ns: u64,
}

/// Whether the server served clients between two readings: by
/// [`SERVING_OPS`] and [`SERVING_MILLICPUS`].
///
/// The commands that wrote vectors are left out of both: their count, and
/// their CPU time as `to` estimates it, so that loading connections (each
/// with a worker parsing and storing documents) are not taken for clients.
pub fn serving(from: &Reading, to: &Reading) -> bool {
    let wall = to.wall_ns.saturating_sub(from.wall_ns).max(1);
    let writes = to.writes.saturating_sub(from.writes);
    let other_cpu = to
        .cpu_ns
        .saturating_sub(from.cpu_ns)
        .saturating_sub(to.linking_ns.saturating_sub(from.linking_ns))
        .saturating_sub(writes.saturating_mul(to.write_cpu_ns));
    let other_ops = to
        .commands
        .saturating_sub(from.commands)
        .saturating_sub(writes);
    let ops_per_s = u128::from(other_ops) * 1_000_000_000 / u128::from(wall);
    let millicpus = u128::from(other_cpu) * 1000 / u128::from(wall);
    ops_per_s >= u128::from(SERVING_OPS) || millicpus >= u128::from(SERVING_MILLICPUS)
}

/// The looks so far: the last reading and how many quiet ones came in a row.
#[derive(Debug, Default)]
pub struct Gauge {
    last: Option<Reading>,
    quiet: u32,
}

impl Gauge {
    /// Look at `now`: whether all the helpers take jobs from here on (true)
    /// or only the floor (false); `None` if it is too soon to look again.
    pub fn look(&mut self, now: Reading) -> Option<bool> {
        let tick = TICK.as_nanos() as u64;
        let stale = STALE.as_nanos() as u64;
        let Some(last) = self.last else {
            self.last = Some(now);
            self.quiet = 0;
            return Some(false);
        };
        let since = now.wall_ns.saturating_sub(last.wall_ns);
        if since < tick {
            return None;
        }
        self.last = Some(now);
        if since > stale || serving(&last, &now) {
            self.quiet = 0;
            return Some(false);
        }
        self.quiet = self.quiet.saturating_add(1);
        Some(self.quiet >= QUIET_TICKS)
    }
}

static GAUGE: parking_lot::Mutex<Gauge> = parking_lot::const_mutex(Gauge {
    last: None,
    quiet: 0,
});

/// Since the first look, for [`Reading::wall_ns`].
static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// Look, if it is time to, and take the parked helpers in or park them
/// again. By one helper at a time; the others go on.
fn adapt() {
    let Some(mut gauge) = GAUGE.try_lock() else {
        return;
    };
    let epoch = *EPOCH.get_or_init(std::time::Instant::now);
    let wall_ns = epoch.elapsed().as_nanos() as u64;
    if gauge
        .last
        .is_some_and(|last| wall_ns.saturating_sub(last.wall_ns) < TICK.as_nanos() as u64)
    {
        return;
    }
    let now = Reading {
        wall_ns,
        cpu_ns: cpu_clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID),
        linking_ns: helpers_cpu_ns() + LINK_CPU_NS.load(Relaxed),
        commands: crate::stats::TOTAL_COMMANDS_PROCESSED.total(),
        writes: WRITES.load(Relaxed),
        write_cpu_ns: WRITE_CPU_NS.load(Relaxed),
    };
    let Some(all) = gauge.look(now) else {
        return;
    };
    #[cfg(test)]
    if PINNED.load(Relaxed) != usize::MAX {
        return;
    }
    set_active(if all {
        HELPERS.load(Acquire)
    } else {
        AUTO_FLOOR.load(Relaxed)
    });
}

/// Tests: keep [`ACTIVE`] at `n` (at least 1, at most the helpers), or
/// (`None`) let `auto` decide again.
#[cfg(test)]
static PINNED: AtomicUsize = AtomicUsize::new(usize::MAX);

#[cfg(test)]
pub fn pin_active(n: Option<usize>) {
    helpers();
    match n {
        Some(n) => {
            PINNED.store(n, Relaxed);
            set_active(n.clamp(1, HELPERS.load(Acquire)));
        }
        None => {
            PINNED.store(usize::MAX, Relaxed);
            if std::env::var("KIVIDB_TEST_INDEX_HELPERS").is_ok_and(|v| v == "max") {
                set_active(HELPERS.load(Acquire));
            } else {
                set_active(AUTO_FLOOR.load(Relaxed));
            }
        }
    }
}

/// The helpers run on the CPU time the workers leave them: a server busy
/// serving links the vectors in the writes again (the backlog fills, and a
/// writer that finds it full links its own), as 1.2 did.
///
/// A helper holds locks a worker may want (the key's, the graph's nodes for
/// writing while it links, the queue's, the backlog's): preempted holding
/// one, it keeps that worker waiting, and the worker, parked, gives its CPU
/// to the helper. Measured with every server CPU busy (an Azure D8s_v5, the
/// server on 4 vCPUs, `--threads 4`; memtier GET/SET 1:1 from 50
/// connections, one client loading 384-d vectors, a KNN and a single-`HSET`
/// probe, 40 s, 3 runs): GET/SET p99 3.2 ms and `HSET` p99 6.5 ms as
/// `SCHED_IDLE`, 3.8-4.8 and 7.0 ms as `SCHED_BATCH` at nice 19, 4.0-4.8
/// and 7.9 ms at nice 19, against 0.8-1.5 and 2.0 ms with the vectors linked
/// in the writes (1.2's way); the build alone was as fast with each (3,960-
/// 4,030 documents a second on those 4 vCPUs, 1,710 in the writes).
///
/// Not in unit tests: they wait for the helpers to empty a backlog, and on a
/// runner whose CPUs the other tests keep busy an idle-class helper can go
/// unscheduled for longer than they wait.
fn lower_priority() {
    #[cfg(all(target_os = "linux", not(test)))]
    {
        let param = libc::sched_param { sched_priority: 0 };
        // SAFETY: changes this thread's own scheduling policy only; a
        // failure leaves it as it was.
        unsafe { libc::sched_setscheduler(0, libc::SCHED_IDLE, &param) };
    }
}

/// Jobs a helper found while their graph was held back
/// ([`Backlog::hold`]), kept (unlinked, and counted in the graph's
/// [`Backlog::queued`]) until it is let go.
#[cfg(test)]
static HELD: parking_lot::Mutex<Vec<Job>> = parking_lot::const_mutex(Vec::new());

/// `job`, unless its graph is held back: then it is put aside in [`HELD`]
/// (None). Under `HELD`'s lock, which [`Backlog::hold`] takes to let the
/// graph go: a job looked at as the graph is let go is either given back
/// to the helper or among those `hold(false)` gives back, never left aside.
#[cfg(test)]
fn put_aside(index: &HnswIndex, job: Job) -> Option<Job> {
    if !index.backlog.held.load(Acquire) {
        return Some(job);
    }
    crate::test_hooks::at("backlog: putting aside");
    let mut held = HELD.lock();
    if index.backlog.held.load(Acquire) {
        held.push(job);
        return None;
    }
    Some(job)
}

#[cfg(test)]
impl Backlog {
    /// Keep the helpers from linking this graph's vectors (`true`), or let
    /// them, giving them back the jobs they put aside (`false`).
    pub fn hold(&self, on: bool) {
        if on {
            self.held.store(true, Release);
        } else {
            // The flag goes down under `HELD`'s lock, as a helper reads it
            // before putting a job aside (see `helper`).
            let mut held = HELD.lock();
            self.held.store(false, Release);
            let mut jobs = POOL.jobs.lock();
            jobs.extend(held.drain(..));
            drop(jobs);
            POOL.ready.notify_all();
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
        let (old, _, _) = g.backlog.put("x", vec![1.0, 0.0, 0.0, 0.0].into(), 0);
        let (new, _, _) = g.backlog.put("x", vec![0.0, 0.0, 0.0, 9.0].into(), 0);
        link_own(&g, "x", old, &metric);
        assert!(
            !g.key_to_id.read().contains_key("x"),
            "a written-over vector was linked"
        );
        assert_eq!(g.backlog.len(), 1);
        link_own(&g, "x", new, &metric);
        assert!(g.backlog.is_empty());
        let id = g.key_to_id.read()["x"];
        let nodes = g.nodes.read();
        assert_eq!(g.vector_of(&nodes, id, "x").unwrap(), &[0.0, 0.0, 0.0, 9.0]);
    }

    #[test]
    fn a_delete_takes_a_waiting_vector_out() {
        let g = graph(50, 4);
        let (t, _, _) = g.backlog.put("gone", vec![1.0, 2.0, 3.0, 4.0].into(), 0);
        let live = crate::vector::LiveIndex::Hnsw(Arc::clone(&g));
        crate::vector::unindex_vector(&live, "gone");
        assert!(g.backlog.is_empty());
        link_own(&g, "gone", t, &DistanceMetric::L2);
        assert!(!g.key_to_id.read().contains_key("gone"));
    }

    #[test]
    fn an_insert_here_and_now_drops_a_waiting_vector() {
        let g = graph(50, 4);
        let (t, _, _) = g.backlog.put("y", vec![1.0, 1.0, 1.0, 1.0].into(), 0);
        hnsw_insert(
            &g,
            "y".into(),
            vec![2.0, 2.0, 2.0, 2.0],
            &DistanceMetric::L2,
        );
        assert!(g.backlog.is_empty());
        link_own(&g, "y", t, &DistanceMetric::L2);
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

    #[test]
    fn search_index_threads_parses_and_auto_keeps_small_servers_free() {
        assert_eq!(IndexThreads::parse("auto"), Some(IndexThreads::Auto));
        assert_eq!(IndexThreads::parse("AUTO"), Some(IndexThreads::Auto));
        assert_eq!(IndexThreads::parse("0"), Some(IndexThreads::Count(0)));
        assert_eq!(IndexThreads::parse("3"), Some(IndexThreads::Count(3)));
        assert_eq!(IndexThreads::parse("-1"), None);
        assert_eq!(IndexThreads::parse("many"), None);
        assert_eq!(
            IndexThreads::parse(&(MAX_INDEX_THREADS + 1).to_string()),
            None
        );
        assert_eq!(IndexThreads::Count(3).name(), "3");
        assert_eq!(IndexThreads::Auto.name(), "auto");
        // Under a quota, none; below AUTO_FROM_CPUS, none.
        assert_eq!(auto_helpers(16, true), 0);
        for cpus in 1..AUTO_FROM_CPUS {
            assert_eq!(auto_helpers(cpus, false), 0, "{cpus} CPUs");
        }
        assert_eq!(auto_helpers(8, false), 3);
        assert_eq!(auto_helpers(16, false), 7);
        assert!(auto_helpers(AUTO_FROM_CPUS, false) >= 1);
        // The most `auto` runs while only loading: none where it starts
        // none, otherwise one fewer than the CPUs.
        assert_eq!(auto_most(16, true), 0);
        for cpus in 1..AUTO_FROM_CPUS {
            assert_eq!(auto_most(cpus, false), 0, "{cpus} CPUs");
        }
        assert_eq!(auto_most(8, false), 7);
        assert_eq!(auto_most(16, false), 15);
        for cpus in AUTO_FROM_CPUS..300 {
            assert!(auto_most(cpus, false) >= auto_helpers(cpus, false));
        }
    }

    /// One look every `TICK` from `at`: CPU and commands besides the links
    /// and the vector writes, a second.
    fn reading(at: &mut Reading, ms: u64, millicpus: u64, ops: u64, writes: u64) -> Reading {
        at.wall_ns += ms * 1_000_000;
        at.cpu_ns += millicpus * ms * 1000 + 7 * ms * 1_000_000; // 7 CPUs linking
        at.linking_ns += 7 * ms * 1_000_000;
        at.commands += (ops + writes) * ms / 1000;
        at.writes += writes * ms / 1000;
        *at
    }

    #[test]
    fn auto_takes_helpers_in_after_quiet_looks_and_parks_them_at_once() {
        let tick = TICK.as_millis() as u64;
        let mut g = Gauge::default();
        let mut at = Reading::default();
        // The first look: the floor.
        assert_eq!(g.look(at), Some(false));
        // Too soon.
        assert_eq!(g.look(reading(&mut at.clone(), tick / 2, 0, 0, 0)), None);
        // A load alone (20,000 vector writes a second, the loader's worker
        // at 0.3 CPU, FT.INFO polled 100 times a second): quiet, and after
        // QUIET_TICKS looks, all of them.
        for i in 1..=QUIET_TICKS {
            let all = g.look(reading(&mut at, tick, 300, 100, 20_000));
            assert_eq!(all, Some(i >= QUIET_TICKS), "look {i}");
        }
        assert_eq!(g.look(reading(&mut at, tick, 300, 100, 20_000)), Some(true));
        // Clients arrive: back to the floor at the next look, by commands...
        let clients = reading(&mut at, tick, 300, SERVING_OPS, 20_000);
        assert_eq!(g.look(clients), Some(false));
        // ...and again only after QUIET_TICKS quiet looks.
        for i in 1..=QUIET_TICKS {
            let all = g.look(reading(&mut at, tick, 300, 0, 20_000));
            assert_eq!(all, Some(i >= QUIET_TICKS), "look {i}");
        }
        // ...or by CPU time: a few expensive commands.
        let expensive = reading(&mut at, tick, SERVING_MILLICPUS, 50, 0);
        assert_eq!(g.look(expensive), Some(false));
        for _ in 0..QUIET_TICKS {
            g.look(reading(&mut at, tick, 0, 0, 0));
        }
        assert_eq!(g.look(reading(&mut at, tick, 0, 0, 0)), Some(true));
        // A look long after the last (the helpers had nothing to do): the
        // floor, until quiet looks again.
        let stale = STALE.as_millis() as u64 + 1;
        assert_eq!(g.look(reading(&mut at, stale, 0, 0, 0)), Some(false));
        assert_eq!(g.look(reading(&mut at, tick, 0, 0, 0)), Some(false));
    }

    #[test]
    fn serving_counts_what_is_not_the_load() {
        let from = Reading::default();
        let second = 1_000_000_000;
        let to = |cpu_ns, linking_ns, commands, writes| Reading {
            wall_ns: second,
            cpu_ns,
            linking_ns,
            commands,
            writes,
            write_cpu_ns: 0,
        };
        let (busy, linking) = (8 * second, 15 * second / 2);
        // 8 CPUs busy, 7.5 of them linking; 100,000 commands, all writing
        // vectors: a load.
        assert!(!serving(&from, &to(busy, linking, 100_000, 100_000)));
        // The same and 500 other commands.
        assert!(serving(&from, &to(busy, linking, 100_500, 100_000)));
        // One CPU besides the links.
        assert!(serving(&from, &to(busy, 7 * second, 100_000, 100_000)));
        // Counters reset (CONFIG RESETSTAT) or behind: nothing, not a wrap.
        assert!(!serving(&from, &to(0, second, 0, 10)));
        // Four loading connections: their workers parse and store 100,000
        // documents a second at 20 us each, 2 CPUs, beside 6 CPUs linking.
        // Without an estimate of what a write costs, that is a CPU's worth
        // and more besides the links; with it, a load.
        let loaders = to(busy, 6 * second, 100_000, 100_000);
        assert!(serving(&from, &loaders));
        let estimated = |r: Reading| Reading {
            write_cpu_ns: 20_000,
            ..r
        };
        assert!(!serving(&from, &estimated(loaders)));
        // The same with 500 other commands, or another CPU busy: clients.
        let loaders_and_clients = to(busy, 6 * second, 100_500, 100_000);
        assert!(serving(&from, &estimated(loaders_and_clients)));
        let loaders_and_a_cpu = to(9 * second, 6 * second, 100_000, 100_000);
        assert!(serving(&from, &estimated(loaders_and_a_cpu)));
        // An estimate too high leaves nothing, not a wrap.
        let r = Reading {
            write_cpu_ns: u64::MAX,
            ..loaders
        };
        assert!(!serving(&from, &r));
    }

    /// A graph past [`HERE_BELOW`], so that writes go through the backlog.
    fn big_graph(dim: usize) -> Arc<HnswIndex> {
        graph(HERE_BELOW + 200, dim)
    }

    /// A filtered search, held in the middle of its walk of the graph (the
    /// filter waits there), does not keep writes to the same graph waiting:
    /// they took the backlog for writing, which the search held for reading
    /// from start to end.
    #[test]
    fn a_long_filtered_search_lets_writes_in() {
        let g = big_graph(8);
        let metric = DistanceMetric::L2;
        g.backlog.hold(true);
        // Something waits, so that the search takes the backlog's way.
        write(&g, "first", vector(8, 1), &metric, 0);
        assert_eq!(g.backlog.len(), 1);
        let q = vector(8, 2);
        let mut asked = false;
        let mut admit = |n: &HnswNode| {
            if !asked {
                asked = true;
                let (tx, rx) = std::sync::mpsc::channel();
                let writer = Arc::clone(&g);
                crate::cpus::spawn(move || {
                    for i in 0..50 {
                        let m = DistanceMetric::L2;
                        write(&writer, &format!("w{i}"), vector(8, 100 + i), &m, 0);
                    }
                    let _ = tx.send(());
                });
                assert!(
                    rx.recv_timeout(std::time::Duration::from_secs(20)).is_ok(),
                    "writes waited on a search"
                );
            }
            n.key.ends_with('3')
        };
        let (got, _) =
            crate::vector::knn_hnsw_admitting(&g, &q, 5, 64, &metric, &mut admit, usize::MAX, None)
                .unwrap();
        assert!(asked);
        assert!(got.iter().all(|r| r.key.ends_with('3')));
        assert_eq!(g.backlog.len(), 51);
        g.backlog.hold(false);
        assert!(settle(&g, std::time::Duration::from_secs(60)));
    }

    /// Writes over a few keys while the helpers are held back: one job per
    /// key waits in the queue, whatever the number of writes, and the job
    /// links the key's latest vector.
    #[test]
    fn overwrites_of_a_waiting_key_queue_no_more_jobs() {
        let g = big_graph(8);
        let metric = DistanceMetric::L2;
        g.backlog.hold(true);
        for i in 0..2000u64 {
            write(
                &g,
                &format!("hot{}", i % 5),
                vector(8, 50_000 + i),
                &metric,
                0,
            );
        }
        assert_eq!(g.backlog.len(), 5);
        assert_eq!(g.backlog.queued(), 5);
        assert!(g.backlog.bytes() >= 5 * std::mem::size_of::<Job>());
        g.backlog.hold(false);
        assert!(settle(&g, std::time::Duration::from_secs(60)));
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while g.backlog.queued() > 0 {
            assert!(std::time::Instant::now() < until, "jobs left in the queue");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let ids = g.key_to_id.read();
        let nodes = g.nodes.read();
        for k in 0..5u64 {
            let key = format!("hot{k}");
            let last = vector(8, 50_000 + 1995 + k);
            assert_eq!(g.vector_of(&nodes, ids[&key], &key).unwrap(), &last[..]);
        }
    }

    /// A helper that finds its job's graph held back, and the graph let go
    /// before the job is put aside: the job is the helper's to link, not
    /// left aside where nothing gives it back (its vector then waited for
    /// good, and a test waiting for the backlog to empty timed out).
    #[test]
    fn a_job_put_aside_as_its_graph_is_let_go_is_linked() {
        let g = big_graph(8);
        g.backlog.hold(true);
        let job = || Job {
            index: Arc::downgrade(&g),
            key: Arc::from("k1"),
            metric: DistanceMetric::L2,
        };
        // Counted as queued, as `write` counts a job it queues: the one put
        // aside goes back to the helpers, which count it done.
        g.backlog.jobs.fetch_add(1, AcqRel);
        assert!(put_aside(&g, job()).is_none(), "held: put aside");
        {
            let g = Arc::clone(&g);
            crate::test_hooks::set("backlog: putting aside", move || g.backlog.hold(false));
        }
        assert!(
            put_aside(&g, job()).is_some(),
            "a job of a graph let go was put aside"
        );
        assert!(put_aside(&g, job()).is_some());
    }

    /// A search that took the backlog while a key's vector waited, and then
    /// found the graph with that vector linked (a helper got to it between
    /// the two): the key comes once, as written.
    #[test]
    fn a_vector_linked_while_a_search_runs_is_found_once() {
        let g = big_graph(8);
        let metric = DistanceMetric::L2;
        g.backlog.hold(true);
        let far = vec![50.0; 8];
        write(&g, "k7", far.clone(), &metric, 0);
        {
            let g = Arc::clone(&g);
            crate::test_hooks::set("knn: backlog taken", move || {
                link_queued(&g, "k7", &DistanceMetric::L2);
            });
        }
        let got = knn_hnsw(&g, &far, 5, 64, &metric);
        crate::test_hooks::clear("knn: backlog taken");
        assert!(g.backlog.is_empty(), "the hook did not link k7");
        assert_eq!(got[0].key, "k7");
        assert_eq!(got[0].score, 0.0);
        assert_eq!(got.iter().filter(|r| r.key == "k7").count(), 1);
        g.backlog.hold(false);
    }

    /// The two orders the backlog keeps: a vector is taken out of the
    /// backlog only once the graph has it (a search in between finds it, and
    /// not the key's previous vector), and a delete takes a waiting vector
    /// out only once the graph's node is gone (a search in between finds
    /// the waiting vector or nothing, never the graph's previous one).
    #[test]
    fn a_search_between_graph_and_backlog_sees_no_older_vector() {
        let g = big_graph(8);
        let metric = DistanceMetric::L2;
        let old7 = vector(8, 7);
        let old9 = vector(8, 9);
        g.backlog.hold(true);
        let far = vec![50.0; 8];
        write(&g, "k7", far.clone(), &metric, 0);
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        {
            let (g, seen, far, old7) =
                (Arc::clone(&g), Arc::clone(&seen), far.clone(), old7.clone());
            crate::test_hooks::set("backlog: linked", move || {
                let m = DistanceMetric::L2;
                seen.lock().push(knn_hnsw(&g, &far, 5, 64, &m));
                seen.lock().push(knn_hnsw(&g, &old7, 5, 64, &m));
            });
        }
        link_queued(&g, "k7", &metric);
        crate::test_hooks::clear("backlog: linked");
        {
            let seen = seen.lock();
            assert_eq!(seen.len(), 2, "the hook did not run");
            assert_eq!(seen[0][0].key, "k7");
            assert_eq!(seen[0][0].score, 0.0);
            assert_eq!(seen[0].iter().filter(|r| r.key == "k7").count(), 1);
            assert!(
                seen[1].iter().all(|r| r.key != "k7" || r.score > 1.0),
                "k7's previous vector was found"
            );
        }
        // k9: in the graph, a later vector waiting, then deleted.
        let far9 = vec![-50.0; 8];
        write(&g, "k9", far9.clone(), &metric, 0);
        assert!(g.backlog.holds("k9"));
        seen.lock().clear();
        {
            let (g, seen, old9) = (Arc::clone(&g), Arc::clone(&seen), old9.clone());
            crate::test_hooks::set("unlink: deleted", move || {
                seen.lock()
                    .push(knn_hnsw(&g, &old9, 5, 64, &DistanceMetric::L2));
            });
        }
        let live = crate::vector::LiveIndex::Hnsw(Arc::clone(&g));
        crate::vector::unindex_vector(&live, "k9");
        crate::test_hooks::clear("unlink: deleted");
        let seen = seen.lock();
        assert_eq!(seen.len(), 1, "the hook did not run");
        assert!(
            seen[0].iter().all(|r| r.key != "k9" || r.score > 1.0),
            "k9's previous vector was found after its delete began"
        );
        assert!(!g.backlog.holds("k9"));
        g.backlog.hold(false);
    }

    /// Helpers started by a write, in a process where nothing else has set
    /// how many take jobs: they take them, and the backlog empties. Helpers
    /// that started parked and were never woken left a build's last
    /// vectors waiting for good, and tests in the same process hid that:
    /// [`pin_active`] wakes every helper. So this runs in a process of its
    /// own, the test binary again with this test alone, no pin and no
    /// `KIVIDB_TEST_INDEX_HELPERS`.
    #[test]
    fn helpers_started_by_a_write_take_jobs_with_nothing_pinned() {
        const ALONE: &str = "KIVIDB_TEST_HELPERS_ALONE";
        if std::env::var_os(ALONE).is_none() {
            let path = module_path!();
            let name = format!(
                "{}::helpers_started_by_a_write_take_jobs_with_nothing_pinned",
                path.split_once("::").map_or(path, |(_, rest)| rest)
            );
            let out = std::process::Command::new(std::env::current_exe().unwrap())
                .args([name.as_str(), "--exact", "--test-threads=1", "--nocapture"])
                .env(ALONE, "1")
                .env_remove("KIVIDB_TEST_INDEX_HELPERS")
                .output()
                .unwrap();
            let said = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            assert!(out.status.success(), "{said}");
            assert!(
                said.contains("1 passed"),
                "the test did not run alone: {said}"
            );
            return;
        }
        assert_eq!(
            HELPERS.load(Acquire),
            0,
            "helpers started before the first write"
        );
        assert_eq!(PINNED.load(Relaxed), usize::MAX);
        let g = big_graph(8);
        let metric = DistanceMetric::L2;
        let n = 3000;
        for i in 0..n {
            write(&g, &format!("u{i}"), vector(8, 90_000 + i), &metric, 0);
        }
        assert!(helpers() >= 3);
        assert!(threads().1 >= 1, "no helper takes jobs");
        assert!(
            settle(&g, std::time::Duration::from_secs(30)),
            "{} vectors still wait after 30 s",
            g.backlog.len()
        );
        assert_eq!(g.live_count.load(Relaxed), HERE_BELOW + 200 + n as usize);
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
            fields: vec![
                SchemaField {
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
                },
                SchemaField {
                    // So that documents have ids, for `knn_measured`.
                    name: "t".into(),
                    field_type: FieldType::Tag,
                    options: FieldOptions::default(),
                },
            ],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
            nofreqs: false,
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
                let pairs: Vec<(String, Arc<[u8]>)> =
                    vec![("v".into(), blob), ("t".into(), Arc::from(&b"a"[..]))];
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

    /// What the filtered searches let in: keys ending in an even digit.
    fn admitted(key: &str) -> bool {
        key.as_bytes().last().is_some_and(|b| b % 2 == 0)
    }

    /// A search of `g` (the index's graph) by `way` (see `run`).
    #[allow(clippy::too_many_arguments)]
    fn search(
        way: usize,
        indexes: &IndexStore,
        name: &str,
        g: &Arc<HnswIndex>,
        q: &[f32],
        k: usize,
        metric: &DistanceMetric,
        candidates: &[String],
    ) -> Vec<KnnResult> {
        let live = LiveIndex::Hnsw(Arc::clone(g));
        match way {
            0 => knn_hnsw(g, q, k, 64, metric),
            1 => {
                let stored = super::super::as_stored(metric, q.to_vec());
                let mut admit = |n: &HnswNode| admitted(&n.key);
                crate::vector::knn_hnsw_admitting(
                    g,
                    &stored,
                    k,
                    64,
                    metric,
                    &mut admit,
                    usize::MAX,
                    None,
                )
                .expect("no budget")
                .0
            }
            2 => {
                crate::commands::vector::knn_filtered(
                    &live,
                    q,
                    k,
                    metric,
                    candidates.iter().map(String::as_str),
                    None,
                )
                .0
            }
            _ => {
                let store = indexes.read();
                let def = store.get(name).unwrap();
                let mut ids = crate::vector::postings::Bits::new();
                {
                    // As a query's plan does: the writes queued while a
                    // reader held the postings applied first, so that every
                    // document written before has its id.
                    let p = def.postings.read();
                    for key in candidates {
                        if let Some(id) = p.id(key) {
                            ids.set(id);
                        }
                    }
                }
                let knn = crate::vector::filter::Knn {
                    k,
                    field: "v".into(),
                    vector: q.to_vec(),
                    alias: "d".into(),
                    ef_runtime: None,
                    policy: None,
                    batch_size: None,
                };
                crate::commands::vector::knn_measured(def, &ids, &knn, &live, metric, None).0
            }
        }
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
                let mut round = s as usize;
                let k = 20;
                // What a filter lets in, for the filtered ways.
                let candidates: Vec<String> = (0..1200)
                    .map(|i| format!("{name}:b{i}"))
                    .chain((0..hot).map(|i| format!("{name}:h{i}")))
                    .filter(|key| admitted(key))
                    .collect();
                while !stop.load(Relaxed) {
                    round += 1;
                    let q = rng.vector();
                    // 0: knn_hnsw; 1: the graph's filtered walk; 2: each
                    // candidate measured as offered (FilteredKnn); 3: the
                    // documents of a filter's ids measured (knn_measured).
                    let way = round % 4;
                    let before: AHashMap<String, (usize, usize)> = log
                        .lock()
                        .iter()
                        .map(|(k, h)| (k.clone(), (h.acked, h.states.len())))
                        .collect();
                    let g = graph(&indexes, &name);
                    let found = search(way, &indexes, &name, &g, &q, k, &metric, &candidates);
                    // Keys whose last write was acknowledged before the
                    // search began and whose vector still waits now: it
                    // waited all along.
                    let waited: Vec<String> = before
                        .iter()
                        .filter(|(key, (acked, len))| acked == len && g.backlog.holds(key))
                        .map(|(key, _)| key.clone())
                        .collect();
                    let log = log.lock();
                    let mut keys = ahash::AHashSet::new();
                    for r in &found {
                        assert!(keys.insert(r.key.clone()), "{} came twice", r.key);
                        if way == 1 || way == 2 {
                            assert!(admitted(&r.key), "{} is not let in", r.key);
                        }
                        let Some(h) = log.get(&r.key) else {
                            continue; // a base key, never written again
                        };
                        // From the state acknowledged when the search began
                        // (none if no write was) to the last one begun.
                        let from = before.get(&r.key).map_or(0, |b| b.0).saturating_sub(1);
                        let ok = h.states[from..].iter().any(|st| {
                            st.as_ref().is_some_and(|v| {
                                let d = dist(&metric, &q, v);
                                (d - r.score).abs() <= 1e-4 * (1.0 + d.abs())
                            })
                        });
                        assert!(
                            ok,
                            "{} at {} is no state of it since the search began (way {way})",
                            r.key, r.score
                        );
                    }
                    // Complete: a key that waited all along and is nearer
                    // than the k-th hit is among the hits (it is measured
                    // exactly, wherever the graph's walk went).
                    let kth = if found.len() >= k {
                        found[k - 1].score
                    } else {
                        f32::INFINITY
                    };
                    for key in waited {
                        let h = &log[&key];
                        if h.states.len() != before[&key].1 || (way != 0 && !admitted(&key)) {
                            continue;
                        }
                        let Some(Some(v)) = h.states.last() else {
                            continue;
                        };
                        let d = dist(&metric, &q, v);
                        if d < kth - 1e-4 * (1.0 + kth.abs()) {
                            assert!(
                                found.iter().any(|r| r.key == key),
                                "{key} waited from before the search, at {d}, nearer than hit \
                                 {k} at {kth}, and was not found (way {way})"
                            );
                        }
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

    /// Helpers taken in and parked again every few hundred microseconds, as
    /// `auto` does when clients come and go, while writers (`HSET`, `DEL`),
    /// searchers, compactions and drops race them: every check of `run`
    /// holds, a graph dropped while writers still write is let go, and no
    /// vector is left waiting.
    #[test]
    fn helpers_taken_in_and_parked_during_a_build_change_nothing() {
        let most = helpers();
        assert!(most >= 3, "the tests start some helpers to park");
        let stop = Arc::new(AtomicBool::new(false));
        let flipper = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut flips = 0usize;
                while !stop.load(Relaxed) {
                    // All, then one, two, ... in turn: every count is seen.
                    let n = if flips.is_multiple_of(2) {
                        most
                    } else {
                        1 + (flips / 2) % most
                    };
                    pin_active(Some(n));
                    assert!(threads().1 >= 1);
                    flips += 1;
                    std::thread::sleep(std::time::Duration::from_micros(300));
                }
                flips
            })
        };
        for seed in 21..=24u64 {
            let metric = if seed % 2 == 0 {
                DistanceMetric::Cosine
            } else {
                DistanceMetric::L2
            };
            run(seed, metric, seed % 2 == 0);
        }
        let metric = DistanceMetric::L2;
        for round in 0..4u64 {
            let name = format!("sd{round}");
            let (store, indexes) = setup(&name, &metric);
            let mut rng = Rng(round + 211);
            for i in 0..1200 {
                write(
                    &store,
                    &indexes,
                    &format!("{name}:{i}"),
                    Some(&rng.vector()),
                );
            }
            let g = graph(&indexes, &name);
            let writers: Vec<_> = (0..3u64)
                .map(|w| {
                    let (store, indexes, name) =
                        (Arc::clone(&store), indexes.clone(), name.clone());
                    std::thread::spawn(move || {
                        let mut rng = Rng((round * 7 + w) | 1);
                        for _ in 0..1500 {
                            let key = format!("{name}:{}", rng.below(2000));
                            let v = (rng.below(4) != 0).then(|| rng.vector());
                            let _order =
                                crate::write_order::lock_keys(0, std::iter::once(key.as_bytes()));
                            write(&store, &indexes, &key, v.as_deref());
                        }
                    })
                })
                .collect();
            std::thread::sleep(std::time::Duration::from_millis(5 + round * 5));
            if round % 2 == 0 {
                assert!(crate::vector::remove_index(&mut indexes.write(), &name));
            } else {
                crate::vector::drop_all_indexes(&indexes);
            }
            for w in writers {
                w.join().unwrap();
            }
            assert!(settle(&g, std::time::Duration::from_secs(60)));
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while Arc::strong_count(&g) > 1 {
                assert!(
                    std::time::Instant::now() < until,
                    "round {round}: a helper kept the dropped graph"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        stop.store(true, Relaxed);
        let flips = flipper.join().unwrap();
        pin_active(None);
        assert!(flips > 20, "{flips} changes of the count");
        assert_eq!(helpers(), most);
        // Let go, the count is `auto`'s again, and the helpers it leaves
        // taking jobs empty a backlog with nothing waking them. (Whether
        // helpers that start parked are woken is
        // `helpers_started_by_a_write_take_jobs_with_nothing_pinned`, in a
        // process of its own: here every pin woke them.)
        let (store, indexes) = setup("unpinned", &metric);
        let mut rng = Rng(4242);
        for i in 0..3000 {
            let key = format!("unpinned:{i}");
            let _order = crate::write_order::lock_keys(0, std::iter::once(key.as_bytes()));
            write(&store, &indexes, &key, Some(&rng.vector()));
        }
        let g = graph(&indexes, "unpinned");
        assert!(
            settle(&g, std::time::Duration::from_secs(60)),
            "{} vectors still wait, {} helpers taking jobs",
            g.backlog.len(),
            threads().1
        );
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

    /// An index whose graph has jobs queued (the helpers held back): a
    /// `FLUSHALL`, an `FT.DROPINDEX` or a compaction's swap frees the graph
    /// at once, not when the helpers get to its jobs.
    #[test]
    fn a_graph_let_go_with_jobs_queued_is_freed_at_once() {
        let metric = DistanceMetric::L2;
        for round in 0..3u64 {
            let name = format!("fq{round}");
            let (store, indexes) = setup(&name, &metric);
            let mut rng = Rng(round + 101);
            for i in 0..1200 {
                write(
                    &store,
                    &indexes,
                    &format!("{name}:{i}"),
                    Some(&rng.vector()),
                );
            }
            let g = graph(&indexes, &name);
            g.backlog.hold(true);
            for i in 1200..1400 {
                let key = format!("{name}:{i}");
                let _order = crate::write_order::lock_keys(0, std::iter::once(key.as_bytes()));
                write(&store, &indexes, &key, Some(&rng.vector()));
            }
            assert!(g.backlog.queued() > 0);
            let gone = Arc::downgrade(&g);
            drop(g);
            match round {
                0 => {
                    crate::vector::drop_all_indexes(&indexes);
                }
                1 => assert!(crate::vector::remove_index(&mut indexes.write(), &name)),
                _ => {
                    crate::vector::compact_hnsw(&indexes, &store, &name, "v").unwrap();
                    // The new graph has every key, the waiting ones too.
                    let new = graph(&indexes, &name);
                    assert_eq!(new.live_count.load(Relaxed), 1400);
                    new.backlog.hold(false);
                }
            }
            // A helper may hold it for a moment, putting a job aside; the
            // jobs themselves stay held back.
            let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while gone.upgrade().is_some() {
                assert!(
                    std::time::Instant::now() < until,
                    "round {round}: the graph was kept"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        // The jobs put aside go back to the helpers, which drop them.
        Backlog::default().hold(false);
    }

    /// `FT.DROPINDEX` while a compaction rebuilds the graph: the compaction
    /// gives up, and neither graph is kept.
    #[test]
    fn a_drop_during_a_compaction_lets_both_graphs_go() {
        let metric = DistanceMetric::L2;
        let name = "dc";
        let (store, indexes) = setup(name, &metric);
        let mut rng = Rng(77);
        for i in 0..1500 {
            write(
                &store,
                &indexes,
                &format!("{name}:{i}"),
                Some(&rng.vector()),
            );
        }
        let g = graph(&indexes, name);
        {
            let indexes = indexes.clone();
            crate::test_hooks::set("compact: rebuilt", move || {
                assert!(crate::vector::remove_index(&mut indexes.write(), "dc"));
            });
        }
        let got = crate::vector::compact_hnsw(&indexes, &store, name, "v");
        crate::test_hooks::clear("compact: rebuilt");
        assert!(got.is_err());
        assert!(g.shadow().is_none(), "the old graph kept the new one");
        assert!(indexes.read().get(name).is_none());
        let gone = Arc::downgrade(&g);
        drop(g);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while gone.upgrade().is_some() {
            assert!(std::time::Instant::now() < until, "the old graph was kept");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// An index of `prefix` with two HNSW fields, `v` and `w`.
    fn two_vectors(name: &str, prefix: &str) -> crate::vector::IndexDef {
        let hnsw = |field: &str| SchemaField {
            name: field.into(),
            field_type: FieldType::Vector {
                algo: VectorAlgo::Hnsw {
                    m: 16,
                    ef_construction: 32,
                    ef_runtime: 10,
                },
                dim: DIM,
                metric: DistanceMetric::L2,
                data_type: VectorType::Float32,
            },
            options: FieldOptions::default(),
        };
        IndexSchema {
            name: name.into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec![prefix.into()],
            fields: vec![hnsw("v"), hnsw("w")],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
            nofreqs: false,
        }
        .into_def()
    }

    /// `auto` leaves the commands that wrote vectors out of what counts as
    /// serving: each such command counts once, however many indexes and
    /// vector fields it writes, and the walks over the keys already there
    /// (`FT.CREATE`, a load, a full sync), or a thread that runs no command,
    /// count nothing. A loader writing into two indexes used to hide one
    /// client command per write.
    #[test]
    fn a_command_writing_vectors_counts_once_and_a_backfill_not_at_all() {
        let store = crate::store::new_store(1);
        let indexes = crate::vector::new_index_store();
        for name in ["cw1", "cw2"] {
            indexes
                .write()
                .insert(name.into(), two_vectors(name, "cw:"));
        }
        std::thread::spawn(move || {
            let mut rng = Rng(5);
            let mut doc = |key: &str| -> Vec<(String, Arc<[u8]>)> {
                let pairs: Vec<(String, Arc<[u8]>)> = ["v", "w"]
                    .iter()
                    .map(|f| {
                        let blob: Vec<u8> =
                            rng.vector().iter().flat_map(|x| x.to_le_bytes()).collect();
                        (f.to_string(), Arc::from(blob))
                    })
                    .collect();
                let hash: crate::types::HashFields = pairs.iter().cloned().collect();
                crate::store::get_shard(&store[0], key).write().insert(
                    key,
                    KiviDBValue::Hash(Arc::new(parking_lot::RwLock::new(hash))),
                );
                pairs
            };
            let whole = crate::vector::Write::Whole;
            // No command has run on this thread (a load, a field expiring on
            // a thread of its own): nothing is counted.
            maybe_index(&indexes, 0, "cw:0", &doc("cw:0"), whole);
            assert_eq!(counted_here(), 0);
            // One command: two indexes, two vector fields each, and a second
            // key (as `JSON.MSET` writes several): once.
            crate::stats::record_command_processed();
            maybe_index(&indexes, 0, "cw:1", &doc("cw:1"), whole);
            maybe_index(&indexes, 0, "cw:2", &doc("cw:2"), whole);
            assert_eq!(counted_here(), 1);
            // The next command: once more.
            crate::stats::record_command_processed();
            maybe_index(&indexes, 0, "cw:3", &doc("cw:3"), whole);
            assert_eq!(counted_here(), 2);
            // `FT.CREATE` over those keys, in a command of its own: its walk
            // indexes them and counts nothing.
            indexes
                .write()
                .insert("cw3".into(), two_vectors("cw3", "cw:"));
            crate::stats::record_command_processed();
            crate::vector::backfill_from_db(&store[0], indexes.read().get("cw3").unwrap());
            assert_eq!(counted_here(), 2);
            let g = graph(&indexes, "cw3");
            assert_eq!(g.live_count.load(Relaxed) + g.backlog.len(), 4);
        })
        .join()
        .unwrap();
    }
}
