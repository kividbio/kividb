pub use crate::shared_bytes::SharedBytes;
use crate::vector::DistanceMetric;
use ahash::AHashMap;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

pub type Cmd<'a> = Vec<&'a [u8]>;

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}
/// A stream entry ID: millisecond timestamp + sequence number.
/// Derives Ord so BTreeMap gives us free range queries by ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Parse from "ms-seq" or "ms" (seq defaults to 0)
    pub fn parse(s: &str) -> Option<Self> {
        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms = ms_str.parse().ok()?;
            let seq = seq_str.parse().ok()?;
            Some(Self { ms, seq })
        } else {
            let ms = s.parse().ok()?;
            Some(Self { ms, seq: 0 })
        }
    }

    /// The minimum possible ID
    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    /// The maximum possible ID
    pub fn max() -> Self {
        Self {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }
}

pub type FieldsType = Vec<(Arc<[u8]>, Arc<[u8]>)>;
/// A single entry in a stream.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Field-value pairs in insertion order.
    /// Vec<(field, value)> preserves order, which Redis guarantees.
    pub fields: FieldsType,
}

/// Pending Entry List entry — tracks unacknowledged messages per consumer.
#[derive(Debug, Clone)]
pub struct PelEntry {
    pub consumer: String,
    pub delivered_at: u64, // unix ms
    pub delivery_count: u64,
}

/// A consumer group.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub name: String,
    /// Last delivered ID — entries with id > last_id are "new"
    pub last_id: StreamId,
    /// How many of the entries ever added to the stream this group has been
    /// delivered — or [`ConsumerGroup::NO_COUNT`] when that is not known.
    pub entries_read: u64,
    /// Pending Entry List: id → PelEntry
    pub pel: BTreeMap<StreamId, PelEntry>,
    /// Consumers: name → last-seen ms
    pub consumers: AHashMap<String, u64>,
    /// Consumers that have been handed entries: name → when they last were,
    /// in ms. Not kept across a restart: a consumer is then one that has not
    /// been active yet.
    pub active: AHashMap<String, u64>,
}

impl ConsumerGroup {
    /// `entries_read` when the count is not known: the group was created at
    /// an ID in the middle of the stream, or entries ahead of it have been
    /// deleted. Redis's `-1`.
    pub const NO_COUNT: u64 = u64::MAX;

    /// The consumer has sent a command; with `got_entries`, one that handed
    /// it something.
    pub fn seen(&mut self, consumer: &str, now: u64, got_entries: bool) {
        match self.consumers.get_mut(consumer) {
            Some(seen) => *seen = now,
            None => {
                self.consumers.insert(consumer.to_string(), now);
            }
        }
        if got_entries {
            self.active.insert(consumer.to_string(), now);
        }
    }
}

/// One remembered idempotent ID: which entry it produced, and when.
#[derive(Debug, Clone)]
pub struct IdmpRecord {
    pub iid: Arc<[u8]>,
    pub entry_id: StreamId,
    pub added_ms: u64,
}

/// The idempotent IDs remembered for one producer, oldest first.
#[derive(Debug, Clone, Default)]
pub struct ProducerIdmp {
    pub order: std::collections::VecDeque<IdmpRecord>,
    pub by_iid: AHashMap<Arc<[u8]>, StreamId>,
}

/// A stream's idempotent-production state (`XADD … IDMP / IDMPAUTO`,
/// Redis 8.6): which `(producer, idempotent-id)` pairs have already produced
/// an entry, so a retried `XADD` returns that entry instead of adding another.
#[derive(Debug, Clone, Default)]
pub struct StreamIdmp {
    /// `XCFGSET … IDMP-DURATION`, in seconds. `None` = the server default.
    pub duration_secs: Option<u64>,
    /// `XCFGSET … IDMP-MAXSIZE`. `None` = the server default.
    pub maxsize: Option<u64>,
    pub producers: AHashMap<Arc<[u8]>, ProducerIdmp>,
    /// Idempotent IDs ever recorded, and retries ever recognised.
    pub iids_added: u64,
    pub iids_duplicates: u64,
}

impl StreamIdmp {
    /// Nothing configured and nothing remembered.
    pub fn is_unused(&self) -> bool {
        self.duration_secs.is_none()
            && self.maxsize.is_none()
            && self.producers.is_empty()
            && self.iids_added == 0
            && self.iids_duplicates == 0
    }
}

/// The inner data for a Stream key.
#[derive(Debug, Clone)]
pub struct StreamInner {
    /// All entries ordered by ID
    pub entries: BTreeMap<StreamId, StreamEntry>,
    /// The last generated ID — used for auto-ID generation
    pub last_id: StreamId,
    /// The max deleted ID — used by XSETID
    pub max_deleted_id: StreamId,
    /// Total entries ever added (not decremented on delete)
    pub entries_added: u64,
    /// Consumer groups
    pub groups: AHashMap<String, ConsumerGroup>,
    /// Persistent max-entries policy set by XCFGSET MAX-ENTRIES.
    /// When Some(n), every XADD trims the stream to at most n entries
    /// after inserting. None = no persistent policy.
    pub max_entries: Option<u64>,
    /// Idempotent-production state.
    pub idmp: StreamIdmp,
    /// Where the nodes Redis would have cut this stream into begin, as
    /// ordinals of `entries_added`, oldest first: recorded as entries are
    /// added, by `stream-node-max-entries` and `stream-node-max-bytes`. Only
    /// `~` trimming and `XINFO` look at them (`stream_extra::Approx`). Empty
    /// for a stream loaded from disk: its nodes are then taken to be
    /// `stream-node-max-entries` long, until new entries record their own.
    pub node_starts: VecDeque<u64>,
    /// What the newest of those nodes holds.
    pub node_fill: NodeFill,
}

/// The newest node of a stream (see `StreamInner::node_starts`).
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeFill {
    /// Entries in it.
    pub entries: u64,
    /// Its size, estimated as Redis's listpack would be.
    pub bytes: u64,
    /// A hash of the field names of its first entry: Redis stores them once
    /// per node, so entries with the same names cost only their values.
    pub fields: u64,
}

impl StreamInner {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::min(),
            max_deleted_id: StreamId::min(),
            entries_added: 0,
            groups: AHashMap::new(),
            max_entries: None,
            idmp: StreamIdmp::default(),
            node_starts: VecDeque::new(),
            node_fill: NodeFill::default(),
        }
    }
}

impl Default for StreamInner {
    fn default() -> Self {
        Self::new()
    }
}

/// A member name stored as a cheaply-cloneable Arc'd byte slice.
/// Using Arc<[u8]> means cloning the key between the BTreeMap and AHashMap
/// costs only an atomic refcount increment, not a heap allocation.
pub type ZMember = Arc<[u8]>;

/// The inner data for a Sorted Set key.
///
/// Dual-representation for O(log N) score-ordered iteration AND O(1) member lookup:
///
///   members:  AHashMap<ZMember, f64>
///             → "what is this member's score?" — O(1) average
///
///   by_score: BTreeMap<(OrderedFloat<f64>, ZMember), ()>
///             → "give me all members with scores in [lo, hi]" — O(log N) seek + O(M) scan
///
/// The composite key (score, member) in by_score ensures that members with
/// equal scores are ordered lexicographically — matching Redis semantics exactly.
///
/// INVARIANT: members and by_score are always in sync. Every write path must
/// update both atomically (they live inside a single RwLock, so this is safe).
#[derive(Clone)]
pub struct SortedSetInner {
    pub members: AHashMap<ZMember, f64>,
    pub by_score: BTreeMap<(OrderedFloat<f64>, ZMember), ()>,
}

impl SortedSetInner {
    pub fn new() -> Self {
        Self {
            members: AHashMap::new(),
            by_score: BTreeMap::new(),
        }
    }

    /// Insert or update a member. Returns true if the member is new.
    /// This is the ONLY place both maps should be written — keeps the invariant.
    pub fn insert(&mut self, member: ZMember, score: f64) -> bool {
        if let Some(&old_score) = self.members.get(&member) {
            // Member exists — remove old position in the score index first
            self.by_score
                .remove(&(OrderedFloat(old_score), member.clone()));
            self.members.insert(member.clone(), score);
            self.by_score.insert((OrderedFloat(score), member), ());
            false // not new
        } else {
            self.members.insert(member.clone(), score);
            self.by_score.insert((OrderedFloat(score), member), ());
            true // new member
        }
    }

    /// Remove a member. Returns true if it existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        // Retrieve the owned key directly, avoiding a linear scan of the member map.
        match self.members.remove_entry(member) {
            Some((arc_member, score)) => {
                let indexed = self.by_score.remove(&(OrderedFloat(score), arc_member));
                debug_assert!(indexed.is_some(), "member missing from by_score");
                true
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

impl Default for SortedSetInner {
    fn default() -> Self {
        Self::new()
    }
}

/// A borrowed key for BTreeMap range queries on by_score.
/// Avoids the Arc allocation that would be needed to construct an owned key.
/// Implements Ord identically to (OrderedFloat<f64>, Arc<[u8]>).
#[derive(PartialEq, Eq)]
pub struct ZScoreKey<'a> {
    pub score: OrderedFloat<f64>,
    pub member: &'a [u8],
}

impl<'a> PartialOrd for ZScoreKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ZScoreKey<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.member.cmp(other.member))
    }
}

#[derive(Clone)]
pub struct VSetEntry {
    pub vector: Vec<f32>,
    pub attrs: Option<String>, // JSON string, stored raw
}

/// The inner data for a VectorSet key.
/// All vectors must share the same dimension.
#[derive(Clone)]
pub struct VectorSetInner {
    pub dim: usize,
    pub entries: AHashMap<String, VSetEntry>,
    pub metric: DistanceMetric,
}

impl VectorSetInner {
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            entries: AHashMap::new(),
            metric: DistanceMetric::Cosine,
        }
    }

    // Add a constructor that accepts a metric
    pub fn with_metric(dim: usize, metric: DistanceMetric) -> Self {
        Self {
            dim,
            entries: AHashMap::new(),
            metric,
        }
    }
}

// ── Time Series types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DuplicatePolicy {
    Block,
    First,
    #[default]
    Last,
    Min,
    Max,
    Sum,
}

/// The duplicate policy of a series made without one (`TS.CREATE` or
/// `TS.ADD` with no `DUPLICATE_POLICY`): `ts-duplicate-policy`, `last`
/// unless set (#127). RedisTimeSeries's default is `block`; KiviDB's stays
/// `last`, so that a server upgraded in place, and an AOF it replays, keep
/// doing what they did. A series keeps the policy it was made with.
static DEFAULT_DUPLICATE_POLICY: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(2);

impl DuplicatePolicy {
    const ALL: [DuplicatePolicy; 6] = [
        DuplicatePolicy::Block,
        DuplicatePolicy::First,
        DuplicatePolicy::Last,
        DuplicatePolicy::Min,
        DuplicatePolicy::Max,
        DuplicatePolicy::Sum,
    ];

    /// See [`DEFAULT_DUPLICATE_POLICY`].
    pub fn server_default() -> Self {
        let at = DEFAULT_DUPLICATE_POLICY.load(std::sync::atomic::Ordering::Relaxed);
        Self::ALL[at as usize % Self::ALL.len()].clone()
    }

    /// Set [`DEFAULT_DUPLICATE_POLICY`].
    pub fn set_server_default(policy: &DuplicatePolicy) {
        let at = Self::ALL.iter().position(|p| p == policy).unwrap_or(2);
        DEFAULT_DUPLICATE_POLICY.store(at as u8, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "BLOCK" => Some(DuplicatePolicy::Block),
            "FIRST" => Some(DuplicatePolicy::First),
            "LAST" => Some(DuplicatePolicy::Last),
            "MIN" => Some(DuplicatePolicy::Min),
            "MAX" => Some(DuplicatePolicy::Max),
            "SUM" => Some(DuplicatePolicy::Sum),
            _ => None,
        }
    }

    /// Inverse of [`Self::parse`] — the token `TS.CREATE DUPLICATE_POLICY`
    /// accepts. Used by the AOF rewrite, which has to emit a series' policy in
    /// a form the loader will parse back; kept next to `parse` so the two
    /// cannot drift (see `duplicate_policy_round_trips`).
    pub fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "BLOCK",
            DuplicatePolicy::First => "FIRST",
            DuplicatePolicy::Last => "LAST",
            DuplicatePolicy::Min => "MIN",
            DuplicatePolicy::Max => "MAX",
            DuplicatePolicy::Sum => "SUM",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TsAggregation {
    Avg,
    First,
    Last,
    Min,
    Max,
    Sum,
    Range,
    Count,
    /// Number of NaN values (Redis 8.6).
    CountNan,
    /// Number of values, NaN or not (Redis 8.6).
    CountAll,
    StdP,
    StdS,
    VarP,
    VarS,
    Twa,
}

impl TsAggregation {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "AVG" => Some(TsAggregation::Avg),
            "FIRST" => Some(TsAggregation::First),
            "LAST" => Some(TsAggregation::Last),
            "MIN" => Some(TsAggregation::Min),
            "MAX" => Some(TsAggregation::Max),
            "SUM" => Some(TsAggregation::Sum),
            "RANGE" => Some(TsAggregation::Range),
            "COUNT" => Some(TsAggregation::Count),
            "COUNTNAN" => Some(TsAggregation::CountNan),
            "COUNTALL" => Some(TsAggregation::CountAll),
            "STD.P" => Some(TsAggregation::StdP),
            "STD.S" => Some(TsAggregation::StdS),
            "VAR.P" => Some(TsAggregation::VarP),
            "VAR.S" => Some(TsAggregation::VarS),
            "TWA" => Some(TsAggregation::Twa),
            _ => None,
        }
    }

    /// The aggregator's name as commands spell it.
    pub fn name(&self) -> &'static str {
        match self {
            TsAggregation::Avg => "avg",
            TsAggregation::First => "first",
            TsAggregation::Last => "last",
            TsAggregation::Min => "min",
            TsAggregation::Max => "max",
            TsAggregation::Sum => "sum",
            TsAggregation::Range => "range",
            TsAggregation::Count => "count",
            TsAggregation::CountNan => "countnan",
            TsAggregation::CountAll => "countall",
            TsAggregation::StdP => "std.p",
            TsAggregation::StdS => "std.s",
            TsAggregation::VarP => "var.p",
            TsAggregation::VarS => "var.s",
            TsAggregation::Twa => "twa",
        }
    }

    /// Apply this aggregation over a slice of values.
    #[allow(dead_code)]
    pub fn apply(&self, vals: &[f64]) -> f64 {
        if vals.is_empty() {
            return 0.0;
        }
        match self {
            TsAggregation::Avg => vals.iter().sum::<f64>() / vals.len() as f64,
            TsAggregation::First => vals[0],
            TsAggregation::Last => vals[vals.len() - 1],
            TsAggregation::Min => vals.iter().cloned().fold(f64::INFINITY, f64::min),
            TsAggregation::Max => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            TsAggregation::Sum => vals.iter().sum(),
            TsAggregation::Range => {
                let mn = vals.iter().cloned().fold(f64::INFINITY, f64::min);
                let mx = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                mx - mn
            }
            TsAggregation::Count => vals.iter().filter(|v| !v.is_nan()).count() as f64,
            TsAggregation::CountNan => vals.iter().filter(|v| v.is_nan()).count() as f64,
            TsAggregation::CountAll => vals.len() as f64,
            TsAggregation::StdP => {
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                let var = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64;
                var.sqrt()
            }
            TsAggregation::StdS => {
                if vals.len() < 2 {
                    return 0.0;
                }
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                let var =
                    vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (vals.len() - 1) as f64;
                var.sqrt()
            }
            TsAggregation::VarP => {
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64
            }
            TsAggregation::VarS => {
                if vals.len() < 2 {
                    return 0.0;
                }
                let mean = vals.iter().sum::<f64>() / vals.len() as f64;
                vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (vals.len() - 1) as f64
            }
            // TWA (time-weighted average) requires timestamps — handled separately where called
            TsAggregation::Twa => vals.iter().sum::<f64>() / vals.len() as f64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionRule {
    pub dest_key: String,
    pub aggregation: TsAggregation,
    pub bucket_duration: u64, // ms
    pub align_timestamp: u64, // ms, default 0
}

#[derive(Debug, Clone)]
pub struct TsInner {
    /// All samples: timestamp_ms → value.
    /// BTreeMap gives O(log N) insert + O(k) range scan for free.
    pub samples: BTreeMap<u64, f64>,

    /// Retention in milliseconds. 0 = keep forever.
    pub retention: u64,

    /// How to handle duplicate timestamps.
    pub duplicate_policy: DuplicatePolicy,

    /// Chunk size hint (we store it but use the BTreeMap regardless).
    pub chunk_size: usize,

    /// User-defined label pairs: label_name → label_value.
    pub labels: AHashMap<String, String>,

    /// Compaction rules sourced from this series.
    pub rules: Vec<CompactionRule>,

    /// Max timestamp ever seen (for TS.GET and INCRBY/DECRBY).
    pub last_ts: Option<u64>,

    /// The series this one is a compaction of (`TS.CREATERULE`): `sourceKey`
    /// in `TS.INFO`, and where `LATEST` finds the bucket still open (#128).
    pub source: Option<String>,
}

impl TsInner {
    pub fn new() -> Self {
        Self {
            samples: BTreeMap::new(),
            retention: 0,
            duplicate_policy: DuplicatePolicy::Last,
            chunk_size: 4096,
            labels: AHashMap::new(),
            rules: Vec::new(),
            last_ts: None,
            source: None,
        }
    }

    /// Add a sample, respecting duplicate policy and retention.
    /// Returns the timestamp actually stored, or an error string.
    pub fn add_sample(&mut self, ts: u64, value: f64) -> Result<u64, &'static str> {
        // Enforce retention: trim samples older than (ts - retention)
        if self.retention > 0 && ts >= self.retention {
            let cutoff = ts - self.retention;
            // Only trim if the oldest entry is actually expired
            // This avoids a full scan when the tree is clean
            if self
                .samples
                .keys()
                .next()
                .map(|&oldest| oldest <= cutoff)
                .unwrap_or(false)
            {
                // split_off gives us O(log N) to find the cutoff point,
                // then we discard the left side
                let keep = self.samples.split_off(&(cutoff + 1));
                self.samples = keep;
            }
        }

        if let Some(&existing) = self.samples.get(&ts) {
            match self.duplicate_policy {
                DuplicatePolicy::Block => return Err("TSDB: Duplicate sample in BLOCK policy"),
                DuplicatePolicy::First => { /* keep existing, do nothing */ }
                DuplicatePolicy::Last => {
                    self.samples.insert(ts, value);
                }
                DuplicatePolicy::Min => {
                    self.samples.insert(ts, existing.min(value));
                }
                DuplicatePolicy::Max => {
                    self.samples.insert(ts, existing.max(value));
                }
                DuplicatePolicy::Sum => {
                    self.samples.insert(ts, existing + value);
                }
            }
        } else {
            self.samples.insert(ts, value);
        }

        if self.last_ts.map(|l| ts > l).unwrap_or(true) {
            self.last_ts = Some(ts);
        }
        Ok(ts)
    }

    /// Current wall-clock time in milliseconds.
    pub fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl Default for TsInner {
    fn default() -> Self {
        Self::new()
    }
}

// ── HashInner — hash with per-field TTLs ──────────────────────────────────────
//
// This backs `KiviDBValue::HashEx`, the field-TTL-aware hash variant introduced
// for Redis 7.4+ commands (HEXPIRE, HTTL, HPERSIST, HGETDEL, HGETEX, HSETEX).
//
// Why a separate variant rather than retrofitting `KiviDBValue::Hash`:
// hashes that never use field TTLs pay nothing — same memory layout, same
// snapshot bytes, same hot-path code. A hash transitions to `HashEx` only
// when HEXPIRE/HEXPIREAT/HPEXPIRE/HPEXPIREAT/HSETEX is called on it. From
// then on it stays `HashEx` until the key is DELed.
//
// Lazy eviction:
//   `evict_expired_field` is the single place that decides "this field's
//   TTL has elapsed" and removes both the value entry and the expiry entry.
//   Every reader that wants TTL-aware behaviour calls this first; nobody
//   writes ad-hoc field-removal logic.
/// The fields of a hash, in the order they were first set — which is the
/// order `HGETALL`, `HKEYS`, `HVALS` and `HSCAN` give them in. Redis keeps
/// that order for a hash small enough for a listpack, and clients have come
/// to rely on it (BullMQ's tests among them).
pub type HashFields = indexmap::IndexMap<String, std::sync::Arc<[u8]>, ahash::RandomState>;

/// Up to this many fields, removing one keeps the others in order, as in a
/// Redis listpack (`hash-max-listpack-entries`). A larger hash is a hash
/// table in Redis too, with no order to keep: there the last field takes the
/// removed one's place, which costs nothing.
pub const HASH_ORDERED_FIELDS: usize = 512;

/// Remove `field` from `fields` (see `HASH_ORDERED_FIELDS`).
pub fn hash_remove(fields: &mut HashFields, field: &str) -> Option<std::sync::Arc<[u8]>> {
    if fields.len() <= HASH_ORDERED_FIELDS {
        fields.shift_remove(field)
    } else {
        fields.swap_remove(field)
    }
}

/// A hash's field TTLs, and a time no later than the earliest of them.
///
/// The map is read through `Deref`; it is changed only by these methods, so
/// that every new deadline lowers `earliest`. A removal leaves `earliest` as
/// it was, which can only be early: a reader then looks at the deadlines
/// themselves, as it would without it. A full pass over them under the
/// hash's write lock ([`Expiries::take_due`]) makes it exact again.
#[derive(Clone, Default)]
pub struct Expiries {
    map: ahash::AHashMap<String, std::time::Instant>,
    /// No later than every deadline in `map`; `None` when it is empty.
    earliest: Option<std::time::Instant>,
}

impl Expiries {
    pub fn insert(&mut self, field: String, at: std::time::Instant) -> Option<std::time::Instant> {
        if self.earliest.is_none_or(|earliest| at < earliest) {
            self.earliest = Some(at);
        }
        self.map.insert(field, at)
    }

    pub fn remove(&mut self, field: &str) -> Option<std::time::Instant> {
        let removed = self.map.remove(field);
        if self.map.is_empty() {
            self.earliest = None;
        }
        removed
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.earliest = None;
    }

    /// No later than the earliest deadline; `None` when there is none.
    #[inline]
    pub fn earliest(&self) -> Option<std::time::Instant> {
        self.earliest
    }

    /// Remove the deadlines due at `now` and give back their fields; what is
    /// left makes `earliest` exact, in the same pass.
    pub fn take_due(&mut self, now: std::time::Instant) -> Vec<String> {
        let mut due = Vec::new();
        let mut earliest: Option<std::time::Instant> = None;
        for (field, at) in &self.map {
            if now >= *at {
                due.push(field.clone());
            } else if earliest.is_none_or(|e| *at < e) {
                earliest = Some(*at);
            }
        }
        for field in &due {
            self.map.remove(field);
        }
        self.earliest = earliest;
        due
    }
}

impl std::ops::Deref for Expiries {
    type Target = ahash::AHashMap<String, std::time::Instant>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl From<ahash::AHashMap<String, std::time::Instant>> for Expiries {
    fn from(map: ahash::AHashMap<String, std::time::Instant>) -> Self {
        let earliest = map.values().min().copied();
        Self { map, earliest }
    }
}

pub struct HashInner {
    pub fields: HashFields,
    pub expiries: Expiries,
}

impl HashInner {
    pub fn new() -> Self {
        Self {
            fields: HashFields::default(),
            expiries: Expiries::default(),
        }
    }

    /// Construct from an existing field map (used when upgrading a plain
    /// Hash to a HashEx the first time HEXPIRE is called on it).
    pub fn from_fields(fields: HashFields) -> Self {
        Self {
            fields,
            expiries: Expiries::default(),
        }
    }

    /// True when the field has an expiry that has already elapsed.
    /// False if no expiry is set, or the expiry is still in the future,
    /// or the field doesn't exist. Never while this thread replays the AOF
    /// (`expiry::replaying`): the log's writes to the field were made while
    /// it lived.
    #[inline]
    pub fn is_field_expired(&self, field: &str) -> bool {
        match self.expiries.get(field) {
            Some(exp) => std::time::Instant::now() >= *exp && !crate::expiry::replaying(),
            None => false,
        }
    }

    /// The fields that have not expired, if any has; `None` when every field
    /// is live, and the fields can be read as they are. Nothing is evicted:
    /// for readers that must not show an expired field and must not write,
    /// such as the search indexes' document reads. Never while this thread
    /// replays the AOF, as [`Self::is_field_expired`].
    pub fn live_fields(&self) -> Option<HashFields> {
        self.live_fields_as_of(&mut None)
    }

    /// [`Self::live_fields`] as of one time for every document a query
    /// reads: the first of them with field TTLs reads the clock into `now`,
    /// and the others are held to that time.
    pub fn live_fields_as_of(&self, now: &mut Option<std::time::Instant>) -> Option<HashFields> {
        // No TTL, no clock read.
        self.expiries.earliest()?;
        self.live_fields_at(*now.get_or_insert_with(std::time::Instant::now))
    }

    /// [`Self::live_fields`] as of `now`: a field is expired from its
    /// deadline on. The deadlines are looked at only once the earliest
    /// could have come.
    pub(crate) fn live_fields_at(&self, now: std::time::Instant) -> Option<HashFields> {
        if self
            .expiries
            .earliest()
            .is_none_or(|earliest| now < earliest)
            || crate::expiry::replaying()
        {
            return None;
        }
        if !self.expiries.values().any(|at| now >= *at) {
            return None;
        }
        Some(
            self.fields
                .iter()
                .filter(|(field, _)| !self.expiries.get(*field).is_some_and(|at| now >= *at))
                .map(|(field, value)| (field.clone(), std::sync::Arc::clone(value)))
                .collect(),
        )
    }

    /// Remove a field's value AND its expiry entry. Returns the removed
    /// value if the field existed. This is the only place fields get
    /// deleted in TTL-aware code paths — call it for lazy eviction so
    /// the expiries map never accumulates orphans.
    pub fn evict_field(&mut self, field: &str) -> Option<std::sync::Arc<[u8]>> {
        self.expiries.remove(field);
        hash_remove(&mut self.fields, field)
    }

    /// Lazy-eviction check + removal. If the field is expired, removes it
    /// and returns true. Otherwise returns false. Caller holds the write
    /// lock.
    pub fn lazy_evict_if_expired(&mut self, field: &str) -> bool {
        if self.is_field_expired(field) {
            self.evict_field(field);
            true
        } else {
            false
        }
    }

    /// Sweep all expired fields. Used by HGETALL / HKEYS / HVALS so the
    /// caller gets a clean view in one pass under a single lock.
    /// Returns the number of fields evicted.
    pub fn sweep_expired(&mut self) -> usize {
        // No TTL, no clock read.
        if self.expiries.is_empty() {
            return 0;
        }
        self.sweep_expired_at(std::time::Instant::now())
    }

    /// [`Self::sweep_expired`] as of `now`. Every deadline is at or after
    /// `earliest`, so before it nothing can be due and the deadlines are
    /// not looked at; from it on they all are, which makes it exact again.
    pub(crate) fn sweep_expired_at(&mut self, now: std::time::Instant) -> usize {
        if self
            .expiries
            .earliest()
            .is_none_or(|earliest| now < earliest)
            || crate::expiry::replaying()
        {
            return 0;
        }
        let to_remove = self.expiries.take_due(now);
        for field in &to_remove {
            hash_remove(&mut self.fields, field);
        }
        to_remove.len()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl Default for HashInner {
    fn default() -> Self {
        Self::new()
    }
}

// ── Array — Redis 8.8 sparse array type (AR* commands) ───────────────────────

/// `array-slice-size`: how many indices one slice of an array covers, for
/// arrays made from now on (an array keeps the size it was made with).
pub static ARRAY_SLICE_SIZE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(4096);
/// `array-sparse-kmax`: a sparse slice holds at most this many elements, and
/// one more makes it dense. 0: slices are always dense.
pub static ARRAY_SPARSE_KMAX: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(10);
/// `array-sparse-kmin`: a dense slice left with this many elements or fewer
/// may become sparse again.
pub static ARRAY_SPARSE_KMIN: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(5);

fn array_kmax() -> u32 {
    ARRAY_SPARSE_KMAX.load(std::sync::atomic::Ordering::Relaxed)
}

fn array_kmin() -> u32 {
    ARRAY_SPARSE_KMIN.load(std::sync::atomic::Ordering::Relaxed)
}

/// How Redis would hold one slice of an array: what `ARINFO FULL` reports.
///
/// The elements themselves are in [`ArrayInner::entries`]. This is only the
/// shape Redis's `sparsearray.c` would have given the slice, kept as it
/// would keep it: a sparse slice (a sorted list with room for `cap`) turns
/// dense once it would pass `array-sparse-kmax` elements; a dense one (a
/// window of `winsize` slots from `offset`) grows by powers of two, and turns
/// sparse again when a deletion leaves it with `array-sparse-kmin` or fewer
/// and that saves memory. Which a slice is depends on how it got there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArraySliceShape {
    Sparse { cap: u32 },
    Dense { offset: u32, winsize: u32 },
}

#[derive(Clone, Copy, Debug)]
pub struct ArraySlice {
    pub count: u32,
    pub shape: ArraySliceShape,
}

/// Redis's initial dense window.
const AR_SLICE_MIN_ALLOC: u32 = 8;
/// `sizeof(arSlice)` on a 64-bit build.
const AR_SLICE_HEADER: usize = 32;

/// Highest addressable array index.
///
/// Bounded by `i64::MAX` rather than `u64::MAX` because every index that crosses
/// the wire does so as a RESP integer, which is signed 64-bit. An index that
/// can't be reported back is not a usable index.
pub const ARRAY_MAX_INDEX: u64 = i64::MAX as u64;

/// The inner data for an Array key.
///
/// Arrays are *sparse*: `ARSET k 1000000 v` stores one element, not a million.
/// Writes go to arbitrary indices, and the range commands (`ARGETRANGE`,
/// `ARSCAN`, `ARGREP`, `AROP`, `ARDELRANGE`) all document complexity
/// proportional to the number of *existing* elements touched rather than to the
/// numeric span of the requested range.
#[derive(Clone)]
pub struct ArrayInner {
    /// Sparse index → value.
    ///
    /// A `BTreeMap` rather than Redis's slice directory: ordered iteration gives
    /// the range commands their documented "proportional to existing elements"
    /// behaviour directly, and `range()` makes a bounded sub-scan O(log N + M)
    /// instead of requiring a slice walk. The cost versus real slices is per-
    /// element overhead on dense arrays, which is the trade this codebase
    /// already makes elsewhere (see `SortedSetInner`).
    pub entries: BTreeMap<u64, Arc<[u8]>>,
    /// Index the next `ARINSERT` / `ARRING` will write to. Advanced by those two
    /// commands, repositioned by `ARSEEK`, reported by `ARNEXT`. A value greater
    /// than [`ARRAY_MAX_INDEX`] means the cursor is exhausted — `ARNEXT` replies
    /// null and a further `ARINSERT` errors rather than silently wrapping.
    pub next_insert: u64,
    /// Ring window size set by `ARRING`. `None` for a plain array. Once set,
    /// `ARRING` writes at `next_insert % ring_size` so newer values overwrite
    /// older ones in a fixed-size window.
    pub ring_size: Option<u64>,
    /// The slices Redis would hold, by slice number (see [`ArraySlice`]).
    pub slices: BTreeMap<u64, ArraySlice>,
    /// `array-slice-size` when the array was made.
    pub slice_size: u32,
}

impl Default for ArrayInner {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            next_insert: 0,
            ring_size: None,
            slices: BTreeMap::new(),
            slice_size: ARRAY_SLICE_SIZE.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl ArrayInner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of non-empty elements — `ARCOUNT`.
    #[inline]
    pub fn count(&self) -> u64 {
        self.entries.len() as u64
    }

    /// Highest occupied index + 1, or 0 when empty — `ARLEN`.
    #[inline]
    pub fn len(&self) -> u64 {
        self.entries
            .last_key_value()
            .map(|(&idx, _)| idx + 1)
            .unwrap_or(0)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// True when the insert cursor can no longer produce a valid index.
    #[inline]
    pub fn cursor_exhausted(&self) -> bool {
        self.next_insert > ARRAY_MAX_INDEX
    }

    /// Store `value` at `idx`. Returns true when the slot was previously empty —
    /// this is what `ARSET`/`ARMSET` count for their integer reply.
    #[inline]
    pub fn set(&mut self, idx: u64, value: Arc<[u8]>) -> bool {
        if !self.entries.contains_key(&idx) {
            self.shape_insert(idx);
        }
        self.entries.insert(idx, value).is_none()
    }

    fn slice_of(&self, idx: u64) -> (u64, u32) {
        let size = u64::from(self.slice_size.max(1));
        (idx / size, (idx % size) as u32)
    }

    /// The offsets of the first and last elements in slice `id`.
    fn slice_bounds(&self, id: u64) -> Option<(u32, u32)> {
        let size = u64::from(self.slice_size.max(1));
        let range = self.entries.range(id * size..(id + 1).saturating_mul(size));
        let mut keys = range.map(|(&idx, _)| (idx % size) as u32);
        let first = keys.next()?;
        Some((first, keys.next_back().unwrap_or(first)))
    }

    fn dense_new(&self, rel: u32) -> ArraySliceShape {
        let winsize = AR_SLICE_MIN_ALLOC.min(self.slice_size);
        let offset = rel.min(self.slice_size - winsize);
        ArraySliceShape::Dense { offset, winsize }
    }

    /// `arSparsePromote`: the dense window a sparse slice becomes.
    fn promoted(&self, id: u64) -> ArraySliceShape {
        let Some((min, max)) = self.slice_bounds(id) else {
            return self.dense_new(0);
        };
        let size = self.slice_size;
        let winsize = (max - min + 1).next_power_of_two().max(AR_SLICE_MIN_ALLOC);
        if winsize >= size {
            return ArraySliceShape::Dense {
                offset: 0,
                winsize: size,
            };
        }
        ArraySliceShape::Dense {
            offset: min.min(size - winsize),
            winsize,
        }
    }

    /// `arSliceDenseGrowIfNeeded`.
    fn grown(&self, offset: u32, winsize: u32, rel: u32) -> ArraySliceShape {
        let size = self.slice_size;
        if rel >= offset + winsize {
            let mut w = winsize;
            while rel >= offset + w && w < size {
                w <<= 1;
            }
            if w >= size {
                return ArraySliceShape::Dense {
                    offset: 0,
                    winsize: size,
                };
            }
            return ArraySliceShape::Dense {
                offset: offset.min(size - w),
                winsize: w,
            };
        }
        if rel < offset {
            let end = offset + winsize;
            let w = (end - rel)
                .next_power_of_two()
                .max(AR_SLICE_MIN_ALLOC)
                .min(size);
            let offset = if w == size { 0 } else { end.saturating_sub(w) };
            return ArraySliceShape::Dense { offset, winsize: w };
        }
        ArraySliceShape::Dense { offset, winsize }
    }

    /// An element is about to be added at the empty `idx` (`arSet`).
    fn shape_insert(&mut self, idx: u64) {
        let (id, rel) = self.slice_of(idx);
        let kmax = array_kmax();
        let slice = match self.slices.get(&id) {
            Some(slice) => *slice,
            None => ArraySlice {
                count: 0,
                shape: if kmax > 0 {
                    ArraySliceShape::Sparse { cap: kmax.min(4) }
                } else {
                    self.dense_new(rel)
                },
            },
        };
        let shape = match slice.shape {
            ArraySliceShape::Dense { offset, winsize } => self.grown(offset, winsize, rel),
            ArraySliceShape::Sparse { .. } if slice.count >= kmax => match self.promoted(id) {
                ArraySliceShape::Dense { offset, winsize } => self.grown(offset, winsize, rel),
                sparse => sparse,
            },
            ArraySliceShape::Sparse { cap } if slice.count >= cap => ArraySliceShape::Sparse {
                cap: (cap * 2).min(kmax),
            },
            sparse => sparse,
        };
        self.slices.insert(
            id,
            ArraySlice {
                count: slice.count + 1,
                shape,
            },
        );
    }

    /// `n` elements have gone from slice `id`: it goes when empty, and a
    /// dense one may become sparse (`arDenseMaybeDemote`).
    fn shape_removed(&mut self, id: u64, n: u32) {
        let Some(slice) = self.slices.get_mut(&id) else {
            return;
        };
        slice.count = slice.count.saturating_sub(n);
        if slice.count == 0 {
            self.slices.remove(&id);
            return;
        }
        let (kmax, kmin) = (array_kmax(), array_kmin());
        let ArraySliceShape::Dense { winsize, .. } = slice.shape else {
            return;
        };
        let pointer = std::mem::size_of::<usize>();
        let sparse_bytes = |cap: u32| {
            let offsets = cap as usize * 2;
            let padding = (pointer - offsets % pointer) % pointer;
            AR_SLICE_HEADER + offsets + padding + cap as usize * pointer
        };
        let dense_bytes = AR_SLICE_HEADER + winsize as usize * pointer;
        if kmax == 0
            || slice.count > kmin
            || slice.count > kmax
            || winsize == AR_SLICE_MIN_ALLOC
            || winsize < 4 * kmin
            || dense_bytes < sparse_bytes(kmin) * 5 / 4
        {
            return;
        }
        slice.shape = ArraySliceShape::Sparse { cap: kmin };
    }

    /// Before values are written to every index from `lo` to `hi`: a slice
    /// they would take past `array-sparse-kmax` is made dense first, as
    /// Redis's `arMayPromoteToDenseForRangeSet` does for a bulk `ARSET` or
    /// `ARINSERT`.
    pub fn promote_for_range(&mut self, lo: u64, hi: u64) {
        let kmax = array_kmax();
        if kmax == 0 || lo > hi {
            return;
        }
        let size = u64::from(self.slice_size.max(1));
        for id in lo / size..=hi / size {
            let start = if id == lo / size { lo % size } else { 0 } as u32;
            let end = if id == hi / size { hi % size } else { size - 1 } as u32;
            let span = end - start + 1;
            match self.slices.get(&id).copied() {
                None if span > kmax => {
                    let shape = self.dense_new(start);
                    self.slices.insert(id, ArraySlice { count: 0, shape });
                }
                Some(ArraySlice {
                    count,
                    shape: ArraySliceShape::Sparse { .. },
                }) => {
                    let base = id * size;
                    let existing = self
                        .entries
                        .range(base + u64::from(start)..=base + u64::from(end))
                        .count() as u32;
                    if span > kmax || count + (span - existing) > kmax {
                        let shape = self.promoted(id);
                        self.slices.insert(id, ArraySlice { count, shape });
                    }
                }
                _ => {}
            }
        }
    }

    #[inline]
    pub fn get(&self, idx: u64) -> Option<&Arc<[u8]>> {
        self.entries.get(&idx)
    }

    /// Remove the element at `idx`. Returns true when something was removed.
    #[inline]
    pub fn del(&mut self, idx: u64) -> bool {
        if self.entries.remove(&idx).is_none() {
            return false;
        }
        let (id, _) = self.slice_of(idx);
        self.shape_removed(id, 1);
        true
    }

    /// Remove every element from `lo` to `hi`: how many there were. A slice
    /// is looked at for becoming sparse once, when its part of the range is
    /// gone (`arDeleteRange`), not after each element.
    pub fn del_range(&mut self, lo: u64, hi: u64) -> u64 {
        if lo > hi {
            return 0;
        }
        let doomed: Vec<u64> = self.entries.range(lo..=hi).map(|(&idx, _)| idx).collect();
        let mut per_slice: BTreeMap<u64, u32> = BTreeMap::new();
        for idx in &doomed {
            self.entries.remove(idx);
            *per_slice.entry(self.slice_of(*idx).0).or_default() += 1;
        }
        for (id, n) in per_slice {
            self.shape_removed(id, n);
        }
        doomed.len() as u64
    }
}

#[derive(Clone)]
pub enum KiviDBValue {
    String(SharedBytes),
    List(Arc<RwLock<VecDeque<Arc<[u8]>>>>),
    Hash(Arc<RwLock<HashFields>>),
    Json(Arc<RwLock<serde_json::Value>>),
    Set(Arc<RwLock<crate::member_set::MemberSet>>),
    VectorSet(Arc<RwLock<VectorSetInner>>),
    SortedSet(Arc<RwLock<SortedSetInner>>),
    Stream(Arc<RwLock<StreamInner>>),
    TimeSeries(Arc<RwLock<TsInner>>),
    HashEx(Arc<RwLock<HashInner>>),
    Array(Arc<RwLock<ArrayInner>>),
}

impl KiviDBValue {
    /// A copy of the value that shares nothing with it.
    ///
    /// `clone()` is a second handle on the same list, hash or set — which is
    /// what moving a value from one key to another wants, and not what
    /// `COPY` wants: `COPY a b` made `b` the same list as `a`, and a push to
    /// either was a push to both.
    pub fn deep_clone(&self) -> Self {
        fn fresh<T: Clone>(shared: &Arc<RwLock<T>>) -> Arc<RwLock<T>> {
            Arc::new(RwLock::new(shared.read().clone()))
        }
        match self {
            // The bytes of a string are never changed in place.
            KiviDBValue::String(bytes) => KiviDBValue::String(bytes.clone()),
            KiviDBValue::List(list) => KiviDBValue::List(fresh(list)),
            KiviDBValue::Hash(hash) => KiviDBValue::Hash(fresh(hash)),
            KiviDBValue::Json(json) => KiviDBValue::Json(fresh(json)),
            KiviDBValue::Set(set) => KiviDBValue::Set(fresh(set)),
            KiviDBValue::VectorSet(set) => KiviDBValue::VectorSet(fresh(set)),
            KiviDBValue::SortedSet(zset) => KiviDBValue::SortedSet(fresh(zset)),
            KiviDBValue::Stream(stream) => KiviDBValue::Stream(fresh(stream)),
            KiviDBValue::TimeSeries(series) => KiviDBValue::TimeSeries(fresh(series)),
            KiviDBValue::HashEx(hash) => {
                let hash = hash.read();
                KiviDBValue::HashEx(Arc::new(RwLock::new(HashInner {
                    fields: hash.fields.clone(),
                    expiries: hash.expiries.clone(),
                })))
            }
            KiviDBValue::Array(array) => KiviDBValue::Array(fresh(array)),
        }
    }

    /// A list, set, sorted set, hash or vector set with nothing left in it.
    /// Such a key does not exist as far as Redis is concerned: the command
    /// that removes the last element removes the key. (A stream is different:
    /// it can be empty and still be there.)
    pub fn is_empty_collection(&self) -> bool {
        match self {
            KiviDBValue::List(list) => list.read().is_empty(),
            KiviDBValue::Set(set) => set.read().is_empty(),
            KiviDBValue::SortedSet(zset) => zset.read().is_empty(),
            KiviDBValue::Hash(hash) => hash.read().is_empty(),
            KiviDBValue::HashEx(hash) => hash.read().is_empty(),
            KiviDBValue::VectorSet(vset) => vset.read().entries.is_empty(),
            KiviDBValue::String(_)
            | KiviDBValue::Json(_)
            | KiviDBValue::Stream(_)
            | KiviDBValue::TimeSeries(_)
            | KiviDBValue::Array(_) => false,
        }
    }

    /// Whether `self` and `other` are the very same value, not equal ones:
    /// a value replaced whole is another value, one changed in place is not.
    pub fn same_value(&self, other: &Self) -> bool {
        use KiviDBValue::*;
        match (self, other) {
            (String(a), String(b)) => SharedBytes::ptr_eq(a, b),
            (List(a), List(b)) => Arc::ptr_eq(a, b),
            (Hash(a), Hash(b)) => Arc::ptr_eq(a, b),
            (Json(a), Json(b)) => Arc::ptr_eq(a, b),
            (Set(a), Set(b)) => Arc::ptr_eq(a, b),
            (VectorSet(a), VectorSet(b)) => Arc::ptr_eq(a, b),
            (SortedSet(a), SortedSet(b)) => Arc::ptr_eq(a, b),
            (Stream(a), Stream(b)) => Arc::ptr_eq(a, b),
            (TimeSeries(a), TimeSeries(b)) => Arc::ptr_eq(a, b),
            (HashEx(a), HashEx(b)) => Arc::ptr_eq(a, b),
            (Array(a), Array(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            KiviDBValue::String(_) => "string",
            KiviDBValue::List(_) => "list",
            KiviDBValue::Hash(_) => "hash",
            KiviDBValue::Json(_) => "ReJSON-RL",
            KiviDBValue::Set(_) => "set",
            KiviDBValue::VectorSet(_) => "vectorset",
            KiviDBValue::SortedSet(_) => "zset",
            KiviDBValue::Stream(_) => "stream",
            KiviDBValue::TimeSeries(_) => "TSDB-TYPE",
            KiviDBValue::HashEx(_) => "hash",
            KiviDBValue::Array(_) => "array",
        }
    }
}

#[cfg(test)]
mod sorted_set_tests {
    use super::*;

    fn m(name: &[u8]) -> ZMember {
        Arc::from(name)
    }

    /// `members` and `by_score` hold exactly the same (member, score) pairs.
    /// (By value: after a score update the two keys can be different Arcs of
    /// the same bytes, as `insert` keeps the old key in `members`.)
    fn assert_in_sync(zs: &SortedSetInner) {
        assert_eq!(zs.members.len(), zs.by_score.len());
        for (score, member) in zs.by_score.keys() {
            assert_eq!(zs.members.get(member.as_ref()), Some(&score.0));
        }
    }

    fn order(zs: &SortedSetInner) -> Vec<(&[u8], f64)> {
        zs.by_score.keys().map(|(s, m)| (m.as_ref(), s.0)).collect()
    }

    #[test]
    fn remove_keeps_both_indexes_in_sync() {
        let mut zs = SortedSetInner::new();
        assert!(!zs.remove(b"a"), "empty set");
        assert!(zs.insert(m(b"a"), 1.0));
        assert!(zs.insert(m(b"b"), 2.0));
        assert!(zs.insert(m(b"c"), 2.0)); // ties b: ordered by name
        assert!(zs.insert(m(b"\xff\x00z"), 0.5)); // not UTF-8
        assert_in_sync(&zs);

        assert!(zs.remove(b"b"));
        assert!(!zs.remove(b"b"), "already removed");
        assert!(!zs.remove(b"missing"));
        assert_in_sync(&zs);
        assert_eq!(
            order(&zs),
            [(&b"\xff\x00z"[..], 0.5), (b"a", 1.0), (b"c", 2.0)]
        );

        assert!(zs.remove(b"\xff\x00z"));
        assert_in_sync(&zs);
        assert_eq!(order(&zs), [(&b"a"[..], 1.0), (b"c", 2.0)]);
    }

    #[test]
    fn update_remove_and_reinsert_keep_both_indexes_in_sync() {
        let mut zs = SortedSetInner::new();
        zs.insert(m(b"a"), 1.0);
        zs.insert(m(b"b"), 2.0);
        assert!(!zs.insert(m(b"a"), 3.0), "score update is not new");
        assert_in_sync(&zs);
        assert_eq!(order(&zs), [(&b"b"[..], 2.0), (b"a", 3.0)]);

        assert!(zs.remove(b"a"));
        assert!(zs.insert(m(b"a"), -1.0), "reinsertion is new");
        assert_in_sync(&zs);
        assert_eq!(order(&zs), [(&b"a"[..], -1.0), (b"b", 2.0)]);

        assert!(zs.remove(b"a"));
        assert!(zs.remove(b"b"));
        assert!(zs.is_empty());
        assert!(zs.by_score.is_empty());
    }

    /// Removal drops the member's last reference: neither index keeps it.
    #[test]
    fn remove_releases_the_member() {
        let mut zs = SortedSetInner::new();
        let a = m(b"a");
        zs.insert(Arc::clone(&a), 1.0);
        assert_eq!(Arc::strong_count(&a), 3); // ours, members, by_score
        assert!(zs.remove(b"a"));
        assert_eq!(Arc::strong_count(&a), 1);
    }
}

#[cfg(test)]
mod live_fields_tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// A hash of `names`, each field's value its name, with the times given.
    fn hash(names: &[&str], times: &[(&str, Instant)]) -> HashInner {
        let mut inner = HashInner::new();
        for name in names {
            inner
                .fields
                .insert(name.to_string(), Arc::from(name.as_bytes()));
        }
        for (name, at) in times {
            inner.expiries.insert(name.to_string(), *at);
        }
        inner
    }

    fn names(fields: &HashFields) -> Vec<&str> {
        fields.keys().map(String::as_str).collect()
    }

    #[test]
    fn nothing_is_copied_while_every_field_is_live() {
        let now = Instant::now();
        assert!(hash(&["a", "b"], &[]).live_fields_at(now).is_none());
        let later = now + Duration::from_secs(60);
        let inner = hash(&["a", "b"], &[("a", later), ("b", later)]);
        assert!(inner.live_fields_at(now).is_none());
    }

    #[test]
    fn expired_fields_are_left_out_in_order_and_nothing_is_evicted() {
        let now = Instant::now();
        let (past, later) = (now - Duration::from_secs(1), now + Duration::from_secs(60));
        let inner = hash(
            &["a", "b", "c", "d", "e"],
            &[("b", past), ("c", later), ("d", past), ("gone", past)],
        );
        let live = inner
            .live_fields_at(now)
            .expect("b and d are past their time");
        assert_eq!(names(&live), ["a", "c", "e"]);
        // The values are the hash's own, not copies of their bytes.
        assert!(Arc::ptr_eq(&live["a"], &inner.fields["a"]));
        assert_eq!(inner.fields.len(), 5);
        assert_eq!(inner.expiries.len(), 4);
    }

    #[test]
    fn a_field_is_expired_from_its_deadline_on() {
        let at = Instant::now() + Duration::from_secs(60);
        let inner = hash(&["a", "b"], &[("a", at)]);
        assert!(inner.live_fields_at(at - Duration::from_nanos(1)).is_none());
        assert_eq!(names(&inner.live_fields_at(at).unwrap()), ["b"]);
        assert_eq!(
            names(&inner.live_fields_at(at + Duration::from_nanos(1)).unwrap()),
            ["b"]
        );
    }

    /// `earliest` is no later than any deadline, and there is one exactly
    /// when there are deadlines.
    fn assert_bound(e: &Expiries) {
        match e.earliest() {
            None => assert!(e.is_empty()),
            Some(earliest) => {
                assert!(!e.is_empty());
                assert!(e.values().all(|at| earliest <= *at));
            }
        }
    }

    #[test]
    fn an_earlier_deadline_lowers_the_bound_and_a_later_one_does_not_raise_it() {
        let now = Instant::now();
        let s = |n: u64| now + Duration::from_secs(n);
        let mut e = Expiries::default();
        assert_eq!(e.earliest(), None);
        e.insert("a".into(), s(60));
        assert_eq!(e.earliest(), Some(s(60)));
        e.insert("b".into(), s(30));
        assert_eq!(e.earliest(), Some(s(30)), "an earlier deadline");
        e.insert("b".into(), s(90));
        assert_eq!(e.earliest(), Some(s(30)), "extended: early, not late");
        assert_bound(&e);
        e.remove("a");
        assert_eq!(e.earliest(), Some(s(30)), "removed: early, not late");
        assert_bound(&e);
        e.remove("b");
        assert_eq!(e.earliest(), None, "none left");
        e.insert("c".into(), s(5));
        e.clear();
        assert_eq!(e.earliest(), None);
        assert!(e.is_empty());
    }

    #[test]
    fn a_pass_over_the_deadlines_makes_the_bound_exact() {
        let now = Instant::now();
        let s = |n: u64| now + Duration::from_secs(n);
        let mut e = Expiries::default();
        e.insert("gone".into(), now - Duration::from_secs(1));
        e.insert("a".into(), s(10));
        e.insert("a".into(), s(50));
        e.insert("b".into(), s(40));
        assert_eq!(e.take_due(now), ["gone"]);
        assert_eq!(e.earliest(), Some(s(40)));
        assert_eq!(e.take_due(s(100)).len(), 2);
        assert_eq!(e.earliest(), None);
    }

    #[test]
    fn a_built_or_cloned_map_has_its_bound() {
        let now = Instant::now();
        let map: ahash::AHashMap<String, Instant> = [("a", 20), ("b", 10), ("c", 30)]
            .iter()
            .map(|(f, n)| (f.to_string(), now + Duration::from_secs(*n)))
            .collect();
        let e = Expiries::from(map);
        assert_eq!(e.earliest(), Some(now + Duration::from_secs(10)));
        assert_eq!(e.clone().earliest(), e.earliest());
        assert_eq!(Expiries::from(ahash::AHashMap::new()).earliest(), None);
        // A hash with field TTLs copied for COPY keeps them and the bound.
        let mut inner = hash(&["a", "b"], &[("a", now + Duration::from_secs(5))]);
        inner.fields.insert("x".into(), Arc::from(&b"x"[..]));
        let value = KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(inner)));
        let KiviDBValue::HashEx(copy) = value.deep_clone() else {
            panic!("not a hash with field TTLs");
        };
        assert_eq!(
            copy.read().expiries.earliest(),
            Some(now + Duration::from_secs(5))
        );
    }

    /// Any sequence of new, extended, shortened and removed deadlines, and
    /// passes over them, keeps the bound; and the live fields are the ones a
    /// look at every deadline finds.
    #[test]
    fn the_bound_holds_through_random_changes() {
        let start = Instant::now();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for _ in 0..200 {
            let mut inner = HashInner::new();
            for f in 0..8 {
                inner.fields.insert(format!("f{f}"), Arc::from(&b"v"[..]));
            }
            for _ in 0..60 {
                let field = format!("f{}", next(8));
                match next(6) {
                    0..=2 => {
                        let at = start + Duration::from_millis(next(1000));
                        inner.expiries.insert(field, at);
                    }
                    3 => {
                        inner.expiries.remove(&field);
                    }
                    4 => {
                        let now = start + Duration::from_millis(next(1000));
                        for gone in inner.expiries.take_due(now) {
                            inner.fields.insert(gone, Arc::from(&b"v"[..]));
                        }
                    }
                    _ => {
                        if next(10) == 0 {
                            inner.expiries.clear();
                        }
                    }
                }
                assert_bound(&inner.expiries);
                let now = start + Duration::from_millis(next(1100));
                let expected: Vec<&str> = inner
                    .fields
                    .keys()
                    .filter(|f| !inner.expiries.get(*f).is_some_and(|at| now >= *at))
                    .map(String::as_str)
                    .collect();
                match inner.live_fields_at(now) {
                    None => assert_eq!(expected.len(), inner.fields.len()),
                    Some(live) => assert_eq!(names(&live), expected),
                }
                if next(4) == 0 {
                    let expected: Vec<String> = expected.into_iter().map(String::from).collect();
                    let removed = inner.sweep_expired_at(now);
                    assert_eq!(names(&inner.fields), expected);
                    assert_eq!(removed, 8 - expected.len());
                    assert_bound(&inner.expiries);
                    for f in 0..8 {
                        let name = format!("f{f}");
                        if !inner.fields.contains_key(&name) {
                            inner.fields.insert(name, Arc::from(&b"v"[..]));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_sweep_before_the_earliest_deadline_looks_at_nothing() {
        let now = Instant::now();
        let s = |n: u64| now + Duration::from_secs(n);
        let mut inner = hash(&["a", "b"], &[]);
        assert_eq!(inner.sweep_expired(), 0);
        assert_eq!(inner.sweep_expired_at(now), 0);
        // Extended, so the bound is early; still in the future, so it
        // stays as it is: the deadlines were not looked at.
        inner.expiries.insert("a".into(), s(10));
        inner.expiries.insert("a".into(), s(50));
        inner.expiries.insert("b".into(), s(60));
        assert_eq!(inner.sweep_expired(), 0);
        assert_eq!(inner.sweep_expired_at(s(9)), 0);
        assert_eq!(inner.expiries.earliest(), Some(s(10)));
        assert_eq!((inner.fields.len(), inner.expiries.len()), (2, 2));
    }

    #[test]
    fn a_sweep_removes_a_field_from_its_deadline_on() {
        let at = Instant::now() + Duration::from_secs(60);
        let mut inner = hash(&["a", "b"], &[("a", at)]);
        assert_eq!(inner.sweep_expired_at(at - Duration::from_nanos(1)), 0);
        assert_eq!(names(&inner.fields), ["a", "b"]);
        assert_eq!(inner.sweep_expired_at(at), 1);
        assert_eq!(names(&inner.fields), ["b"]);
        assert_eq!(inner.expiries.earliest(), None);
    }

    #[test]
    fn a_sweep_removes_every_field_due_and_makes_the_bound_exact() {
        let now = Instant::now();
        let s = |n: u64| now + Duration::from_secs(n);
        let mut inner = hash(
            &["a", "b", "c", "d", "e"],
            &[
                ("a", s(1)),
                ("b", s(2)),
                ("c", s(3)),
                ("d", s(40)),
                ("e", s(30)),
            ],
        );
        assert_eq!(inner.sweep_expired_at(s(3)), 3);
        assert_eq!(names(&inner.fields), ["d", "e"]);
        assert_eq!(inner.expiries.earliest(), Some(s(30)));
    }

    /// HPERSIST and HDEL leave the bound early. Once it has come, a sweep
    /// looks at the deadlines: it finds nothing due, or an expired field the
    /// bound did not belong to, and leaves the bound exact.
    #[test]
    fn a_bound_left_early_brings_a_sweep_that_finds_what_is_due() {
        let now = Instant::now();
        let s = |n: u64| now + Duration::from_secs(n);
        let mut inner = hash(
            &["persisted", "deleted", "a", "b"],
            &[
                ("persisted", s(5)),
                ("deleted", s(6)),
                ("a", s(20)),
                ("b", s(30)),
            ],
        );
        inner.expiries.remove("persisted");
        inner.evict_field("deleted");
        assert_eq!(inner.expiries.earliest(), Some(s(5)));
        assert_eq!(inner.sweep_expired_at(s(10)), 0);
        assert_eq!(names(&inner.fields), ["persisted", "a", "b"]);
        assert_eq!(inner.expiries.earliest(), Some(s(20)), "exact again");

        // Early again, with a field due that the bound is not the time of.
        inner.expiries.insert("c".into(), s(21));
        inner.expiries.remove("a");
        assert_eq!(inner.expiries.earliest(), Some(s(20)));
        assert_eq!(inner.sweep_expired_at(s(25)), 1);
        assert_eq!(names(&inner.fields), ["persisted", "a", "b"]);
        assert_eq!(inner.expiries.earliest(), Some(s(30)));
    }

    #[test]
    fn an_earlier_deadline_after_an_extension_is_swept_on_time() {
        let now = Instant::now();
        let s = |n: u64| now + Duration::from_secs(n);
        let mut inner = hash(&["a", "b"], &[("a", s(10))]);
        inner.expiries.insert("a".into(), s(50));
        assert_eq!(inner.sweep_expired_at(s(20)), 0);
        assert_eq!(inner.expiries.earliest(), Some(s(50)));
        inner.expiries.insert("b".into(), s(30));
        assert_eq!(inner.sweep_expired_at(s(29)), 0);
        assert_eq!(inner.sweep_expired_at(s(30)), 1);
        assert_eq!(names(&inner.fields), ["a"]);
        assert_eq!(inner.expiries.earliest(), Some(s(50)));
    }

    #[test]
    fn nothing_is_swept_while_the_aof_is_replayed() {
        let now = Instant::now();
        let mut inner = hash(&["a", "b"], &[("a", now - Duration::from_secs(1))]);
        {
            let _replay = crate::expiry::replay();
            assert_eq!(inner.sweep_expired(), 0);
            assert_eq!(inner.sweep_expired_at(now), 0);
            assert_eq!(names(&inner.fields), ["a", "b"]);
        }
        assert_eq!(inner.sweep_expired(), 1);
        assert_eq!(names(&inner.fields), ["b"]);
    }

    #[test]
    fn nothing_is_left_out_while_the_aof_is_replayed() {
        let now = Instant::now();
        let inner = hash(&["a"], &[("a", now - Duration::from_secs(1))]);
        {
            let _replay = crate::expiry::replay();
            assert!(inner.live_fields_at(now).is_none());
            assert!(inner.live_fields().is_none());
            assert!(inner.live_fields_as_of(&mut Some(now)).is_none());
        }
        assert!(inner.live_fields().is_some_and(|live| live.is_empty()));
    }

    /// A query reads the clock once, at its first document with field TTLs,
    /// and holds the others to that time: a field is expired from it on.
    #[test]
    fn a_query_reads_the_clock_once_for_all_its_documents() {
        let start = Instant::now();
        let mut now = None;
        assert!(hash(&["a"], &[]).live_fields_as_of(&mut now).is_none());
        assert_eq!(now, None, "no field TTL, no clock read");
        let later = hash(&["a"], &[("a", start + Duration::from_secs(60))]);
        assert!(later.live_fields_as_of(&mut now).is_none());
        let t = now.expect("read by the first document with field TTLs");
        assert!(t >= start);
        let at_t = hash(&["a", "b"], &[("a", t)]);
        assert_eq!(names(&at_t.live_fields_as_of(&mut now).unwrap()), ["b"]);
        let after_t = hash(&["a", "b"], &[("a", t + Duration::from_nanos(1))]);
        assert!(after_t.live_fields_as_of(&mut now).is_none());
        assert_eq!(now, Some(t), "the time is kept");
    }

    /// A field whose time comes while a query runs is part of the documents
    /// that query reads after it; the next query reads the clock anew.
    #[test]
    fn a_field_that_expires_during_a_query_is_seen_by_the_rest_of_it() {
        let query = Instant::now() - Duration::from_secs(2);
        let inner = hash(
            &["a", "b"],
            &[("a", Instant::now() - Duration::from_secs(1))],
        );
        assert!(inner.live_fields_as_of(&mut Some(query)).is_none());
        assert_eq!(names(&inner.live_fields_as_of(&mut None).unwrap()), ["b"]);
        assert_eq!(names(&inner.live_fields().unwrap()), ["b"]);
    }
}
