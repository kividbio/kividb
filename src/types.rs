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
        // We need to find the member by byte slice, but our key is Arc<[u8]>.
        // AHashMap supports heterogeneous lookup via the Borrow trait.
        if let Some(&score) = self.members.get(member) {
            // We need the Arc to remove from by_score — get it from members first.
            // remove() returns the owned key, but we need it for by_score.
            // Clone the Arc before removing (cheap — just atomic increment).
            let arc_member: ZMember = self
                .members
                .iter()
                .find(|(k, _)| k.as_ref() == member)
                .map(|(k, _)| k.clone())
                .unwrap();
            self.members.remove(member);
            self.by_score.remove(&(OrderedFloat(score), arc_member));
            true
        } else {
            false
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

impl DuplicatePolicy {
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

pub struct HashInner {
    pub fields: HashFields,
    pub expiries: ahash::AHashMap<String, std::time::Instant>,
}

impl HashInner {
    pub fn new() -> Self {
        Self {
            fields: HashFields::default(),
            expiries: ahash::AHashMap::new(),
        }
    }

    /// Construct from an existing field map (used when upgrading a plain
    /// Hash to a HashEx the first time HEXPIRE is called on it).
    pub fn from_fields(fields: HashFields) -> Self {
        Self {
            fields,
            expiries: ahash::AHashMap::new(),
        }
    }

    /// True when the field has an expiry that has already elapsed.
    /// False if no expiry is set, or the expiry is still in the future,
    /// or the field doesn't exist.
    #[inline]
    pub fn is_field_expired(&self, field: &str) -> bool {
        match self.expiries.get(field) {
            Some(exp) => std::time::Instant::now() >= *exp,
            None => false,
        }
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
        if self.expiries.is_empty() {
            return 0;
        }
        let now = std::time::Instant::now();
        let mut to_remove: Vec<String> = Vec::new();
        for (field, exp) in self.expiries.iter() {
            if now >= *exp {
                to_remove.push(field.clone());
            }
        }
        for field in &to_remove {
            hash_remove(&mut self.fields, field);
            self.expiries.remove(field);
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

/// Nominal slice width reported by `ARINFO`.
///
/// Redis backs this type with a directory of fixed-width slices; KiviDB uses a
/// sparse `BTreeMap` instead (see [`ArrayInner::entries`]). The slice-shaped
/// `ARINFO` fields are derived from this constant so the reply carries the same
/// fields with comparable values, matching Redis's own 4096 default.
pub const ARRAY_SLICE_SIZE: u64 = 4096;

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
#[derive(Clone, Default)]
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
        self.entries.insert(idx, value).is_none()
    }

    #[inline]
    pub fn get(&self, idx: u64) -> Option<&Arc<[u8]>> {
        self.entries.get(&idx)
    }

    /// Remove the element at `idx`. Returns true when something was removed.
    #[inline]
    pub fn del(&mut self, idx: u64) -> bool {
        self.entries.remove(&idx).is_some()
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
