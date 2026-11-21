//! The HNSW graph of a vector set: the graph FT.SEARCH's vector fields use
//! ([`HnswIndex`]), one per set, over the set's elements.
//!
//! The graph of a `Q8` or `BIN` set holds each element's vector as the set
//! stores it (`quant::Encoding`: the same bytes, measured by the same
//! arithmetic as a scan), and once the graph has linked an element the set
//! lets go of its own copy ([`absorb`]): the vector is then kept once, in the
//! graph, and read from there (`VectorSetInner::vector_of`) by `VEMB`, a
//! snapshot, `DUMP`, the AOF rewrite and `COPY`, which write exactly what the
//! set held. An f32 set's graph holds its own unit-length copy (a cosine
//! distance on the raw floats is not the scan's to the bit), and the set
//! keeps its vectors. Nothing about the graph is saved: it is made again from
//! the vectors when a set is loaded, restored or copied, so the files a 1.2
//! server reads stay the same.
//!
//! Every change to a set's elements is made under the set's lock, and the
//! change to its graph is made there too, in the same order: the element's
//! vector is put in the graph's backlog ([`backlog::put_ordered`]), where a
//! search measures it at once, and a helper thread links it; a removed
//! element is taken out of the graph (and of the backlog) before the set's
//! lock is let go. So a `VSIM` that holds the set finds every element the set
//! holds, with the vector it holds, either linked or waiting, and never one
//! it no longer holds. When no helper takes a vector, the writer links it
//! once it has let go of the set ([`Pending`]), and a later write of the same
//! element makes that link a no-op.
//!
//! A set below [`EXACT_BELOW`] elements has no graph: `VSIM` scans it, which
//! is exact and, at that size, as fast. The graph is made when a set reaches
//! that size, by the builder thread ([`request_build`]), which hands the
//! elements to the graph a few at a time under the set's lock, as writes do;
//! until it has handed over all of them, `VSIM` scans. `TRUTH` always scans.

use super::quant::{Encoding, StoredVector};
use super::{DistanceMetric, HnswIndex, KnnResult, backlog};
use crate::types::VectorSetInner;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Arc, Weak};

/// Below this many elements a set has no graph and `VSIM` scans it. A scan
/// of 4,096 vectors of 128 floats takes about the time of a graph search at
/// Redis's default `EF` of 100, and is exact.
pub const EXACT_BELOW: usize = 4096;

/// Redis's defaults: `M` and the `EF` an insert explores (`VADD`), and the
/// `EF` a search explores (`VSIM`).
pub const DEFAULT_M: usize = 16;
pub const DEFAULT_BUILD_EF: usize = 200;
pub const DEFAULT_SEARCH_EF: usize = 100;

/// Elements the builder hands to a graph for each time it takes the set.
const CHUNK: usize = 64;

/// Elements a long reader (a snapshot of the set, a scan) looks up in the
/// graph for each time it holds it ([`VsetGraph::reader`]), so that an insert
/// waiting to write the graph is not kept waiting for the whole set.
pub const READ_CHUNK: usize = 1024;

/// A set's graph.
pub struct VsetGraph {
    pub index: Arc<HnswIndex>,
    /// [`BUILDING`], [`READY`] (every element the set held when the graph
    /// was made has been handed to it, and each later write hands over its
    /// own: searches may use it) or [`FAILED`] (its build panicked as often
    /// as it is tried: `VSIM` scans the set).
    state: AtomicU8,
}

const BUILDING: u8 = 0;
const READY: u8 = 1;
const FAILED: u8 = 2;

impl VsetGraph {
    fn new(m: usize, ef_construction: usize, encoding: Encoding) -> Self {
        let mut index = HnswIndex::new(m, ef_construction);
        index.encoding = encoding;
        if encoding != Encoding::F32 {
            index.linked = Some(parking_lot::Mutex::new(Vec::new()));
        }
        Self {
            index: Arc::new(index),
            state: AtomicU8::new(BUILDING),
        }
    }

    /// Whether the graph's distances are the scan's own (it holds the
    /// vectors as the set stores them).
    pub fn exact(&self) -> bool {
        self.index.encoding != Encoding::F32
    }

    /// The vector the graph has linked for `name`, as the set stores it
    /// (None for an f32 set's graph, or a name it has not linked).
    pub fn stored(&self, name: &[u8]) -> Option<StoredVector> {
        let encoding = self.index.encoding;
        if encoding == Encoding::F32 {
            return None;
        }
        let reader = self.reader();
        encoding.decode(reader.encoded(name)?)
    }

    /// The graph held for reading, to look many names' vectors up. Nothing
    /// else of the graph is to be locked while it is held.
    ///
    /// What it gives for an element whose entry holds no vector is the same
    /// from one reader to the next while the set is held, for reading or
    /// writing: such an element's vector is linked and none of its waits
    /// ([`absorb`] makes sure, under the write lock), so only a write of it
    /// changes it, and a write takes the set's write lock. Inserts the helpers
    /// make meanwhile are of other elements, and a slot they fill again was
    /// a deleted one. So a reader can be let go and taken again between
    /// chunks of a long walk ([`READ_CHUNK`]).
    pub fn reader(&self) -> Reader<'_> {
        let index = &self.index;
        // The graph before its map, as inserts and searches take them.
        let nodes = index.nodes.read();
        let key_to_id = index.key_to_id.read();
        Reader {
            index,
            key_to_id,
            nodes,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Acquire) == READY
    }

    /// `VINFO`'s `graph-state`: `building`, `ready` or `failed`.
    pub fn state_name(&self) -> &'static str {
        match self.state.load(Acquire) {
            READY => "ready",
            FAILED => "failed",
            _ => "building",
        }
    }

    /// Bytes the graph holds: its slots (vectors, layer-0 links, flags),
    /// its nodes and their upper links, the map from names to nodes, and
    /// what waits in the backlog. `names` is the heap bytes of the element
    /// names, which the graph holds twice (in its node and in its map).
    pub fn bytes(&self, names: usize) -> usize {
        let index = &self.index;
        let nodes = index.nodes.read().count();
        // One node in M has an upper layer (more are rarer still), with up
        // to M links and its list's own header.
        let upper = index.live_count.load(Relaxed) * (index.m * 4 + 24) / index.m.max(2);
        index.slot_bytes()
            + nodes * std::mem::size_of::<super::HnswNode>()
            + index.key_to_id.read().capacity() * (std::mem::size_of::<(String, usize)>() + 1)
            + 2 * names
            + upper
            + index.backlog.bytes()
            + self.linked_bytes()
    }

    /// Bytes the names linked and not yet taken in by the set hold.
    fn linked_bytes(&self) -> usize {
        self.index.linked.as_ref().map_or(0, |q| {
            let q = q.lock();
            q.capacity() * std::mem::size_of::<Arc<str>>()
                + q.iter().map(|k| k.len() + 16).sum::<usize>()
        })
    }

    /// The top layer of the graph (`VINFO`'s `max-level`).
    pub fn max_level(&self) -> usize {
        let index = &self.index;
        let Some(ep) = *index.entry_point.read() else {
            return 0;
        };
        let nodes = index.nodes.read();
        nodes.get(ep).map_or(0, |n| n.level)
    }

    /// Node slots the graph has used (`VINFO`'s `hnsw-max-node-uid`).
    pub fn node_slots(&self) -> usize {
        self.index.nodes.read().count()
    }

    /// The links of `name` by layer, top layer first, as names; None if the
    /// graph has not linked it (yet).
    pub fn links_of(&self, name: &[u8]) -> Option<Vec<Vec<Vec<u8>>>> {
        let index = &self.index;
        let key = crate::key_bytes::text_of(name);
        if index.backlog.holds(&key) {
            return None;
        }
        let id = *index.key_to_id.read().get(&*key)?;
        let nodes = index.nodes.read();
        let node = nodes.get(id).filter(|n| n.key == *key)?;
        let mut out = Vec::with_capacity(node.level + 1);
        let mut ids = Vec::new();
        for layer in (0..=node.level).rev() {
            ids.clear();
            index.links(&nodes, id, layer, &mut ids);
            let names = ids
                .iter()
                .filter(|&&nb| nb as usize != id && index.is_live(nb as usize))
                .filter_map(|&nb| nodes.get(nb as usize))
                .map(|n| crate::key_bytes::bytes_of(&n.key).into_owned())
                .collect();
            out.push(names);
        }
        Some(out)
    }
}

/// A set's graph held for reading: the vectors it has linked, by name.
pub struct Reader<'a> {
    index: &'a HnswIndex,
    key_to_id: parking_lot::RwLockReadGuard<'a, ahash::AHashMap<String, usize>>,
    nodes: parking_lot::RwLockReadGuard<'a, super::HnswNodes>,
}

impl Reader<'_> {
    /// `name`'s vector as the graph lays it out, if the graph has linked it.
    pub fn encoded(&self, name: &[u8]) -> Option<&[f32]> {
        let key = crate::key_bytes::text_of(name);
        let id = *self.key_to_id.get(&*key)?;
        self.index.vector_of(&self.nodes, id, &key)
    }

    pub fn encoding(&self) -> Encoding {
        self.index.encoding
    }
}

/// Where a set keeps its graph: none until the set first reaches
/// [`EXACT_BELOW`] elements, then the same one for the rest of the set's
/// life. A copy of a set (`COPY`, a move between databases) starts with
/// none, and makes its own.
#[derive(Default)]
pub struct GraphCell(std::sync::OnceLock<Arc<VsetGraph>>);

impl Clone for GraphCell {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl GraphCell {
    pub fn get(&self) -> Option<&Arc<VsetGraph>> {
        self.0.get()
    }

    fn take(&mut self) -> Option<Arc<VsetGraph>> {
        self.0.take()
    }
}

/// The graph's key for element `name`: its bytes as text, any bytes
/// (`key_bytes::text_of`, which `bytes_of` reverses).
fn key_of(name: &[u8]) -> String {
    crate::key_bytes::text_of(name).into_owned()
}

/// A vector as `graph` lays it out: an f32 set's at unit length for a
/// cosine, a quantized set's as it is stored.
fn laid_out(graph: &VsetGraph, inner: &VectorSetInner, v: &StoredVector) -> Option<Vec<f32>> {
    match graph.index.encoding {
        Encoding::F32 => {
            let mut f = Vec::with_capacity(inner.dim);
            v.to_f32(inner.dim, &mut f);
            Some(super::as_stored(&inner.metric, f))
        }
        encoding => encoding.encode(v),
    }
}

/// The query `VSIM` gives the graph: the set's quantization of it, laid out
/// as the graph holds vectors (an f32 one as given: the search makes it unit
/// length itself).
pub fn query_for(graph: &VsetGraph, inner: &VectorSetInner, query: &StoredVector) -> Vec<f32> {
    match graph.index.encoding {
        Encoding::F32 => {
            let mut f = Vec::with_capacity(inner.dim);
            query.to_f32(inner.dim, &mut f);
            f
        }
        encoding => encoding.encode(query).unwrap_or_default(),
    }
}

/// A vector a write put in the graph's backlog that no helper will link:
/// [`Pending::link`] it once the set's lock (and the shard's) is let go.
pub struct Pending {
    /// Weak: a set deleted meanwhile takes its graph with it.
    index: Weak<HnswIndex>,
    key: String,
    ticket: u64,
    metric: DistanceMetric,
}

impl Pending {
    pub fn link(self) {
        let Some(index) = self.index.upgrade() else {
            return;
        };
        backlog::link_ordered(&index, &self.key, self.ticket, &self.metric);
        release(index);
    }
}

/// Let go of a graph: if this was the last reference (its set was deleted
/// meanwhile), it is freed by the lazy-free thread, not by a command's.
fn release(index: Arc<HnswIndex>) {
    if Arc::strong_count(&index) == 1 {
        let objects = index.live_count.load(Relaxed) as u64 + 1;
        crate::lazyfree::free_later(index, objects);
    }
}

/// Hand `name`'s vector, as the set now holds it, to the graph. Under the
/// set's lock (read or write).
fn hand_over(graph: &VsetGraph, inner: &VectorSetInner, name: &[u8]) -> Option<Pending> {
    // An entry without its own vector is one the graph already holds.
    let vector = inner.entries.get(name)?.vector.as_ref()?;
    let key = key_of(name);
    let vector: Arc<[f32]> = laid_out(graph, inner, vector)?.into();
    let most = backlog::limit(vector.len());
    let pending = backlog::put_ordered(&graph.index, &key, vector, &inner.metric).map(|ticket| {
        Pending {
            index: Arc::downgrade(&graph.index),
            key,
            ticket,
            metric: inner.metric.clone(),
        }
    })?;
    // No helper threads (`search-index-threads` 0, or `auto` below 8 CPUs):
    // the set's own linkers take it, while the backlog has room. A write
    // that linked its own vector would do so in its command, and the
    // commands writing one key run one after the other: a set's writes
    // would then be linked on one thread at a time.
    if !backlog::helpers_running() && graph.index.backlog.len() <= most {
        linkers::queue(pending);
        return None;
    }
    Some(pending)
}

/// The threads that link a vector set's vectors when there are no helper
/// threads: half the CPUs, at least one, at most four.
mod linkers {
    use super::Pending;
    use std::collections::VecDeque;

    static QUEUE: parking_lot::Mutex<VecDeque<Pending>> = parking_lot::const_mutex(VecDeque::new());
    static READY: parking_lot::Condvar = parking_lot::Condvar::new();
    static START: std::sync::Once = std::sync::Once::new();

    pub(super) fn queue(p: Pending) {
        START.call_once(|| {
            use crate::cpus::BuilderExt;
            let cpus = crate::cpus::available_parallelism().map_or(1, |n| n.get());
            for n in 0..(cpus / 2).clamp(1, 4) {
                let started = std::thread::Builder::new()
                    .name(format!("vset-link-{n}"))
                    .spawn_released(run);
                if started.is_err() {
                    break;
                }
            }
        });
        QUEUE.lock().push_back(p);
        READY.notify_one();
    }

    fn run() {
        loop {
            let p = {
                let mut q = QUEUE.lock();
                loop {
                    if let Some(p) = q.pop_front() {
                        break p;
                    }
                    READY.wait(&mut q);
                }
            };
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| p.link())).is_err() {
                crate::log_warn!("linking a vector set's vector failed");
            }
        }
    }
}

/// After `name` was added to the set or given a new vector, under the set's
/// write lock: its vector goes to the graph, and a set that has reached
/// [`EXACT_BELOW`] elements gets one.
pub fn written(
    set: &Arc<RwLock<VectorSetInner>>,
    inner: &mut VectorSetInner,
    name: &[u8],
) -> Option<Pending> {
    match inner.graph.get().cloned() {
        Some(graph) => {
            backlog::count_write();
            hand_over(&graph, inner, name)
        }
        None => {
            // The builder hands this element over with the others (not
            // during an AOF replay: [`request_builds`] after it).
            request_build(set, inner);
            None
        }
    }
}

/// Most vectors a write takes in after it ([`after_write`]).
const ABSORB_PER_WRITE: usize = 64;

/// After a write, with the shard let go: the set takes in up to a few of the
/// vectors its graph has linked since ([`absorb`]), if it can have its lock
/// at once; the builder takes in the rest when the set is quiet.
pub fn after_write(set: &Arc<RwLock<VectorSetInner>>) {
    let Some(mut inner) = set.try_write() else {
        return;
    };
    if let Some(graph) = inner.graph.get().cloned() {
        absorb(&graph, &mut inner, ABSORB_PER_WRITE);
    }
}

/// After `name` was taken out of the set, under the set's write lock.
pub fn removed(inner: &VectorSetInner, name: &[u8]) {
    if let Some(graph) = inner.graph.get() {
        graph.index.unlink(&key_of(name));
    }
}

/// A set that has shrunk under half of [`EXACT_BELOW`] lets go of its
/// graph: each vector the graph held goes back to its entry first, and the
/// graph's memory is given back (by the lazy-free thread). It gets a new
/// one if it grows again. Under the set's write lock, after its removals.
pub fn shrunk(inner: &mut VectorSetInner) {
    if inner.entries.len() >= EXACT_BELOW / 2 || inner.graph.get().is_none() {
        return;
    }
    let Some(graph) = inner.graph.take() else {
        return;
    };
    if graph.exact() {
        let reader = graph.reader();
        let encoding = reader.encoding();
        for (name, entry) in inner.entries.iter_mut() {
            if entry.vector.is_none() {
                entry.vector = reader.encoded(name.as_bytes()).and_then(|e| encoding.decode(e));
                assert!(
                    entry.vector.is_some(),
                    "a vector set element is in neither its entry nor its graph"
                );
            }
        }
    }
    let index = Arc::clone(&graph.index);
    drop(graph);
    release(index);
}

/// After an AOF replay, which writes no graph: every set of
/// [`EXACT_BELOW`] elements or more gets its graph made, in the background.
pub fn request_builds(store: &crate::store::Store) {
    for db in store.iter() {
        for shard in db.iter() {
            let sets: Vec<Arc<RwLock<VectorSetInner>>> = crate::read_lock!(shard)
                .values()
                .filter_map(|value| match value {
                    crate::store::ValueRef::VectorSet(vs) => Some(Arc::clone(vs)),
                    _ => None,
                })
                .collect();
            for set in sets {
                let inner = set.read();
                request_build(&set, &inner);
            }
        }
    }
}

/// Give a set of [`EXACT_BELOW`] elements or more a graph, if it has none,
/// and queue it for the builder. Under the set's lock (read is enough).
pub fn request_build(set: &Arc<RwLock<VectorSetInner>>, inner: &VectorSetInner) {
    if inner.entries.len() < EXACT_BELOW || inner.graph.get().is_some() {
        return;
    }
    // An AOF replay writes no graph: it links nothing in its writes, and
    // asks for the graphs once it is done ([`request_builds`]).
    if super::indexing_deferred() {
        return;
    }
    let mut made = false;
    let graph = inner.graph.0.get_or_init(|| {
        made = true;
        let encoding = Encoding::of(inner.quant, inner.dim);
        Arc::new(VsetGraph::new(inner.m, inner.ef_construction, encoding))
    });
    if made {
        // The elements there are now, under the lock that gave the set its
        // graph: each written after this hands itself over.
        let names = inner.entries.keys().cloned().collect();
        REGISTRY
            .lock()
            .push((Arc::downgrade(set), Arc::downgrade(graph)));
        queue(Build {
            set: Arc::downgrade(set),
            graph: Arc::downgrade(graph),
            names,
            attempt: 0,
            not_before: None,
        });
    }
}

/// The graph `VSIM` searches, if it should: the set is large enough, has a
/// graph that holds all of it, and the search is not for `truth` (an exact
/// scan) or so wide (`ef`) that a scan costs no more. A set that has
/// reached the size and has no graph yet is given one.
pub fn for_search(
    set: &Arc<RwLock<VectorSetInner>>,
    inner: &VectorSetInner,
    truth: bool,
    ef: usize,
) -> Option<Arc<VsetGraph>> {
    let n = inner.entries.len();
    if n < EXACT_BELOW {
        return None;
    }
    let Some(graph) = inner.graph.get() else {
        request_build(set, inner);
        return None;
    };
    (!truth && graph.is_ready() && ef.saturating_mul(2) < n).then(|| Arc::clone(graph))
}

/// The nearest `ef` candidates of `query` ([`query_for`]) the graph finds,
/// nearest first by the graph's distance, which is the scan's own when the
/// graph is [`VsetGraph::exact`]; otherwise the caller scores them from the
/// elements.
pub fn search(
    graph: &VsetGraph,
    query: &[f32],
    ef: usize,
    metric: &DistanceMetric,
) -> Vec<KnnResult> {
    super::knn_hnsw(&graph.index, query, ef, ef, metric)
}

/// [`search`] among the elements `admit` lets in (a `FILTER`), measuring at
/// most `budget` distances; None past it, when scanning is the way.
pub fn search_admitting(
    graph: &VsetGraph,
    query: &[f32],
    ef: usize,
    metric: &DistanceMetric,
    admit: &mut dyn FnMut(&[u8]) -> bool,
    budget: usize,
) -> Option<Vec<KnnResult>> {
    let query = match graph.index.encoding {
        Encoding::F32 => super::as_stored(metric, query.to_vec()),
        _ => query.to_vec(),
    };
    let mut by_node = |node: &super::HnswNode| admit(&crate::key_bytes::bytes_of(&node.key));
    super::knn_hnsw_admitting(
        &graph.index,
        &query,
        ef,
        ef,
        metric,
        &mut by_node,
        budget,
        None,
    )
    .map(|(found, _)| found)
}

/// Element name of a graph's result.
pub fn name_of(hit: &KnnResult) -> std::borrow::Cow<'_, [u8]> {
    crate::key_bytes::bytes_of(&hit.key)
}

// ── The builder ──────────────────────────────────────────────────────────────

/// A set to build the graph of. Weak, both: a set deleted (`DEL`,
/// `FLUSHALL`, its expiry) while its build waits in the queue is freed at
/// once, graph and all.
struct Build {
    set: Weak<RwLock<VectorSetInner>>,
    graph: Weak<VsetGraph>,
    names: Vec<crate::key_bytes::KeyBytes>,
    /// Builds of this graph that panicked so far.
    attempt: u32,
    /// Not tried again before this (a build that panicked waits a while).
    not_before: Option<std::time::Instant>,
}

/// Times a build is tried before its graph is left [`FAILED`].
pub(crate) const ATTEMPTS: u32 = 3;

struct Builds {
    queue: parking_lot::Mutex<VecDeque<Build>>,
    ready: parking_lot::Condvar,
}

static BUILDS: Builds = Builds {
    queue: parking_lot::const_mutex(VecDeque::new()),
    ready: parking_lot::Condvar::new(),
};
static BUILDER: std::sync::Once = std::sync::Once::new();

/// Every set with a graph, for the builder to take in, when it is idle, what
/// the helpers linked after the set's last write ([`take_in_linked`]).
static REGISTRY: parking_lot::Mutex<Vec<(Weak<RwLock<VectorSetInner>>, Weak<VsetGraph>)>> =
    parking_lot::const_mutex(Vec::new());

/// Builds that panic, in unit tests (`a_build_that_panics_is_tried_again`):
/// those of sets of this many dimensions, this many times.
#[cfg(test)]
pub(crate) static PANIC_BUILDS: parking_lot::Mutex<Vec<(usize, u32)>> =
    parking_lot::const_mutex(Vec::new());

fn queue(job: Build) {
    BUILDER.call_once(|| {
        use crate::cpus::BuilderExt;
        let started = std::thread::Builder::new()
            .name("vset-graph-build".into())
            .spawn_released(builder);
        if started.is_err() {
            crate::log_warn!("could not start the vector-set graph builder");
        }
    });
    BUILDS.queue.lock().push_back(job);
    BUILDS.ready.notify_one();
}

fn builder() {
    let idle = std::time::Duration::from_millis(500);
    let mut taken_in = std::time::Instant::now();
    loop {
        // Between builds too: a queue that never empties does not keep the
        // sets from taking in what their graphs linked.
        if taken_in.elapsed() >= idle {
            take_in_linked();
            taken_in = std::time::Instant::now();
        }
        let job = {
            let mut q = BUILDS.queue.lock();
            let now = std::time::Instant::now();
            match q.iter().position(|j| j.not_before.is_none_or(|t| t <= now)) {
                Some(at) => q.remove(at),
                None => {
                    BUILDS.ready.wait_for(&mut q, idle);
                    continue;
                }
            }
        };
        let Some(job) = job else { continue };
        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| build(&job)));
        if built.is_err() {
            retry(job);
        }
    }
}

/// A build that panicked: tried again a little later, up to [`ATTEMPTS`]
/// times, then its graph is left [`FAILED`] (`VINFO` `graph-state failed`)
/// and `VSIM` scans the set. What it handed over stays in the graph, which
/// holds those vectors for the set.
fn retry(mut job: Build) {
    job.attempt += 1;
    let Some(graph) = job.graph.upgrade() else { return };
    if job.attempt >= ATTEMPTS {
        graph.state.store(FAILED, Release);
        crate::log_warn!(
            "building a vector set's graph failed {} times; VSIM scans that set",
            job.attempt
        );
        return;
    }
    crate::log_warn!("building a vector set's graph failed; trying again");
    job.not_before =
        Some(std::time::Instant::now() + std::time::Duration::from_secs(1 << job.attempt));
    drop(graph);
    queue(job);
}

/// When there is no build to do: each set takes in, a few thousand at a
/// time, the vectors its graph linked after the set's last write took some
/// in ([`after_write`]), so that they are not kept twice, nor their names
/// queued, while the set is not written.
fn take_in_linked() {
    let sets: Vec<(Arc<RwLock<VectorSetInner>>, Arc<VsetGraph>)> = {
        let mut registry = REGISTRY.lock();
        registry.retain(|(s, g)| s.strong_count() > 0 && g.strong_count() > 0);
        registry
            .iter()
            .filter_map(|(s, g)| Some((s.upgrade()?, g.upgrade()?)))
            .collect()
    };
    for (set, graph) in sets {
        loop {
            let queued = graph.index.linked.as_ref().is_some_and(|q| !q.lock().is_empty());
            if !queued {
                break;
            }
            let mut inner = set.write();
            if !still_ours(&inner, &graph) || !absorb(&graph, &mut inner, 4096) {
                break;
            }
        }
    }
}

/// Whether `graph` is still the graph of the set `inner` is.
fn still_ours(inner: &VectorSetInner, graph: &Arc<VsetGraph>) -> bool {
    inner.graph.get().is_some_and(|g| Arc::ptr_eq(g, graph))
}

/// Hand every element the set held when it got its graph to the graph, then
/// mark it ready. Each element is handed over under the set's read lock,
/// with the vector the set holds at that moment: a write of the element after
/// that (under the write lock) hands over its own vector after this one, and
/// one before it was already in what this read. Neither the set nor its
/// graph is held between chunks: a set deleted meanwhile is freed then.
fn build(job: &Build) {
    #[cfg(test)]
    {
        let dim = job.set.upgrade().map(|s| s.read().dim);
        let mut panics = PANIC_BUILDS.lock();
        if let Some(p) = panics.iter_mut().find(|(d, n)| Some(*d) == dim && *n > 0) {
            p.1 -= 1;
            drop(panics);
            panic!("a build made to panic (a test)");
        }
    }
    let Some(graph) = job.graph.upgrade() else { return };
    let index = Arc::downgrade(&graph.index);
    drop(graph);
    let mut pending = Vec::with_capacity(CHUNK);
    for chunk in job.names.chunks(CHUNK) {
        // Room in the backlog for the chunk, so that the helpers, not this
        // thread, do the linking; a full backlog leaves it to this one.
        let mut waited = 0;
        loop {
            let Some(index) = index.upgrade() else { return };
            let room = backlog::limit(index.slots().map_or(1, |s| s.dim())).saturating_sub(CHUNK);
            if index.backlog.len() <= room || waited >= 2000 {
                break;
            }
            drop(index);
            std::thread::sleep(std::time::Duration::from_micros(500));
            waited += 1;
        }
        {
            let (Some(set), Some(graph)) = (job.set.upgrade(), job.graph.upgrade()) else {
                return;
            };
            let inner = set.read();
            if !still_ours(&inner, &graph) {
                return;
            }
            for name in chunk {
                if let Some(p) = hand_over(&graph, &inner, name.as_bytes()) {
                    pending.push(p);
                }
            }
        }
        for p in pending.drain(..) {
            p.link();
        }
    }
    let Some(graph) = job.graph.upgrade() else { return };
    graph.state.store(READY, Release);
    drop(graph);
    // What the helpers linked is taken in when the builder is next idle
    // ([`take_in_linked`]).
    if let Some(index) = index.upgrade() {
        backlog_settled(&index);
    }
}

/// Wait, up to a second, for `index`'s backlog to be linked.
fn backlog_settled(index: &HnswIndex) {
    for _ in 0..1000 {
        if index.backlog.is_empty() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Under the set's write lock: drop the set's own copy of each vector the
/// graph has linked since the last time (up to `most` of them), where the
/// graph holds that vector and none of the element's waits to be linked.
/// Whether some are left for the next time.
pub fn absorb(graph: &VsetGraph, inner: &mut VectorSetInner, most: usize) -> bool {
    let Some(linked) = &graph.index.linked else {
        return false;
    };
    let keys: Vec<Arc<str>> = {
        let mut q = linked.lock();
        if q.is_empty() {
            return false;
        }
        let take = q.len().min(most);
        q.drain(..take).collect()
    };
    let encoding = graph.index.encoding;
    let reader = graph.reader();
    for key in keys {
        if graph.index.backlog.holds(&key) {
            continue; // a later vector of it waits: it is linked again later
        }
        let name = crate::key_bytes::bytes_of(&key);
        let Some(entry) = inner.entries.get_mut(&*name) else {
            continue;
        };
        let Some(own) = entry.vector.as_ref() else {
            continue;
        };
        let same = match (encoding.encode(own), reader.encoded(&name)) {
            (Some(a), Some(b)) => a
                .iter()
                .map(|x| x.to_bits())
                .eq(b.iter().map(|x| x.to_bits())),
            _ => false,
        };
        if same {
            entry.vector = None;
        }
    }
    !linked.lock().is_empty()
}

/// Wait (tests) until no build is queued or running and `graph`'s backlog
/// is linked.
#[cfg(test)]
pub fn settle(graph: &VsetGraph) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while graph.state.load(Acquire) == BUILDING && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    backlog::settle(&graph.index, std::time::Duration::from_secs(60));
}
