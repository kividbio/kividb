//! Stream machinery shared by several stream commands:
//!
//! * idempotent production — `XADD … IDMP / IDMPAUTO` and `XCFGSET` (Redis 8.6)
//! * the cumulative reply caps `MAXCOUNT` / `MAXSIZE` of `XREAD` and
//!   `XREADGROUP` (Redis 8.10)
//! * trimming with the `KEEPREF` / `DELREF` / `ACKED` reference policies
//!   (Redis 8.2), for `XADD` and `XTRIM`
//! * the replication form of consumer-group deliveries

use crate::types::{ConsumerGroup, IdmpRecord, NodeFill, StreamId, StreamInner};
use std::sync::Arc;

/// One field of a stream entry and its value.
type Field = (Arc<[u8]>, Arc<[u8]>);

// ── Idempotent production ────────────────────────────────────────────────────

pub(super) const IDMP_DURATION_RANGE: std::ops::RangeInclusive<u64> = 1..=86_400;
pub(super) const IDMP_MAXSIZE_RANGE: std::ops::RangeInclusive<u64> = 1..=10_000;

/// The idempotency request carried by one `XADD`.
pub(crate) enum IdmpArg<'a> {
    /// `IDMP producer-id idempotent-id`
    Explicit { producer: &'a [u8], iid: &'a [u8] },
    /// `IDMPAUTO producer-id` — the idempotent ID is derived from the entry's
    /// content, so a retry of the same message is recognised without the
    /// producer having to number its messages.
    Auto { producer: &'a [u8] },
}

impl IdmpArg<'_> {
    pub fn producer(&self) -> &[u8] {
        match self {
            IdmpArg::Explicit { producer, .. } | IdmpArg::Auto { producer } => producer,
        }
    }

    /// The idempotent ID this request stands for.
    pub fn iid(&self, fields: &[Field]) -> Arc<[u8]> {
        match self {
            IdmpArg::Explicit { iid, .. } => Arc::from(*iid),
            IdmpArg::Auto { .. } => content_iid(fields),
        }
    }
}

/// A 128-bit digest of an entry's fields and values, in order. Each string is
/// length-prefixed so `("ab","c")` and `("a","bc")` cannot collide by
/// concatenation.
fn content_iid(fields: &[Field]) -> Arc<[u8]> {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for (field, value) in fields {
        for part in [field, value] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part);
        }
    }
    Arc::from(&hasher.digest128().to_le_bytes()[..])
}

/// Effective retention settings for `stream`: its own `XCFGSET` values where
/// set, otherwise the server defaults (`stream-idmp-duration`,
/// `stream-idmp-maxsize`), clamped to the documented ranges.
fn idmp_limits(stream: &StreamInner, default_duration: u64, default_maxsize: u64) -> (u64, usize) {
    let duration = stream
        .idmp
        .duration_secs
        .unwrap_or(default_duration)
        .clamp(*IDMP_DURATION_RANGE.start(), *IDMP_DURATION_RANGE.end());
    let maxsize = stream
        .idmp
        .maxsize
        .unwrap_or(default_maxsize)
        .clamp(*IDMP_MAXSIZE_RANGE.start(), *IDMP_MAXSIZE_RANGE.end());
    (duration, maxsize as usize)
}

/// Forget `producer`'s idempotent IDs that have outlived the retention period.
fn idmp_expire(stream: &mut StreamInner, producer: &[u8], now_ms: u64, duration_secs: u64) {
    let Some(state) = stream.idmp.producers.get_mut(producer) else {
        return;
    };
    let horizon = now_ms.saturating_sub(duration_secs.saturating_mul(1000));
    while state.order.front().is_some_and(|r| r.added_ms < horizon) {
        if let Some(old) = state.order.pop_front() {
            state.by_iid.remove(&old.iid);
        }
    }
    if state.order.is_empty() {
        stream.idmp.producers.remove(producer);
    }
}

/// The entry already produced for `(producer, iid)`, if it is still
/// remembered. Counts the duplicate.
pub(super) fn idmp_lookup(
    stream: &mut StreamInner,
    producer: &[u8],
    iid: &[u8],
    now_ms: u64,
    defaults: (u64, u64),
) -> Option<StreamId> {
    let (duration, _) = idmp_limits(stream, defaults.0, defaults.1);
    idmp_expire(stream, producer, now_ms, duration);
    let found = stream
        .idmp
        .producers
        .get(producer)?
        .by_iid
        .get(iid)
        .cloned()?;
    stream.idmp.iids_duplicates += 1;
    Some(found)
}

/// Remember that `(producer, iid)` produced `entry_id`.
pub(super) fn idmp_record(
    stream: &mut StreamInner,
    producer: &[u8],
    iid: Arc<[u8]>,
    entry_id: StreamId,
    now_ms: u64,
    defaults: (u64, u64),
) {
    let (_, maxsize) = idmp_limits(stream, defaults.0, defaults.1);
    let state = stream
        .idmp
        .producers
        .entry(Arc::from(producer))
        .or_default();
    if state
        .by_iid
        .insert(Arc::clone(&iid), entry_id.clone())
        .is_some()
    {
        // Re-recorded (a replica applying its master's stream): keep one
        // record per iid.
        state.order.retain(|r| r.iid != iid);
    }
    state.order.push_back(IdmpRecord {
        iid,
        entry_id,
        added_ms: now_ms,
    });
    // At capacity the oldest IDs go, however recent.
    while state.order.len() > maxsize {
        if let Some(old) = state.order.pop_front() {
            state.by_iid.remove(&old.iid);
        }
    }
    stream.idmp.iids_added += 1;
}

/// `(producers tracked, idempotent IDs tracked)` for `XINFO STREAM`.
pub(super) fn idmp_tracked(stream: &StreamInner) -> (usize, usize) {
    (
        stream.idmp.producers.len(),
        stream.idmp.producers.values().map(|p| p.order.len()).sum(),
    )
}

/// The retention settings `XINFO STREAM` reports.
pub(super) fn idmp_settings(stream: &StreamInner, defaults: (u64, u64)) -> (u64, usize) {
    idmp_limits(stream, defaults.0, defaults.1)
}

// ── Cumulative reply caps ────────────────────────────────────────────────────

/// `MAXCOUNT` / `MAXSIZE`: one budget for the whole reply, across every stream
/// the command names. `COUNT` stays a per-stream limit and is not handled here.
pub(super) struct ReadBudget {
    max_count: usize,
    max_size: Option<usize>,
    emitted: usize,
    bytes: usize,
}

impl ReadBudget {
    pub fn new(max_count: Option<usize>, max_size: Option<usize>) -> Self {
        Self {
            max_count: max_count.unwrap_or(usize::MAX),
            max_size,
            emitted: 0,
            bytes: 0,
        }
    }

    /// Nothing more may be sent.
    pub fn exhausted(&self) -> bool {
        self.emitted >= self.max_count
    }

    /// Whether an entry that encodes to `len` bytes may be sent — and if so,
    /// charge it. The size cap is not applied to the first entry, so a single
    /// entry larger than `MAXSIZE` is returned rather than nothing. Once an
    /// entry has been refused the budget is closed: later, smaller entries
    /// must not overtake it.
    pub fn admit(&mut self, len: usize) -> bool {
        if self.exhausted() {
            return false;
        }
        if let Some(max) = self.max_size
            && self.emitted > 0
            && self.bytes.saturating_add(len) > max
        {
            self.max_count = self.emitted;
            return false;
        }
        self.emitted += 1;
        self.bytes = self.bytes.saturating_add(len);
        true
    }
}

/// Parsed `MAXCOUNT` / `MAXSIZE` arguments, validated against `COUNT`.
#[derive(Default, Clone, Copy)]
pub(super) struct ReadCaps {
    pub max_count: Option<usize>,
    pub max_size: Option<usize>,
}

impl ReadCaps {
    pub fn budget(&self) -> ReadBudget {
        ReadBudget::new(self.max_count, self.max_size)
    }

    /// Parse the value of `MAXCOUNT` or `MAXSIZE`: a positive integer.
    pub fn parse_positive(option: &str, arg: Option<&&[u8]>) -> Result<usize, String> {
        // Not a number at all is Redis's integer error; a number that is not
        // positive is this one.
        let n = arg
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?;
        usize::try_from(n)
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| format!("ERR {option} must be a positive integer"))
    }

    /// `MAXCOUNT` is a cumulative cap over the per-stream `COUNT`, so it
    /// cannot be the smaller of the two.
    pub fn check_against_count(&self, count: Option<usize>) -> Result<(), String> {
        match (self.max_count, count) {
            (Some(max), Some(count)) if max < count => {
                Err("ERR MAXCOUNT must be greater than or equal to COUNT".to_string())
            }
            _ => Ok(()),
        }
    }
}

// ── Trimming ─────────────────────────────────────────────────────────────────

/// What trimming does about consumer groups that still reference an entry.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) enum TrimRefs {
    /// Remove the entry; leave any pending-list references to it in place.
    #[default]
    KeepRef,
    /// Remove the entry and every pending-list reference to it.
    DelRef,
    /// Remove the entry only once every group has read and acknowledged it.
    Acked,
}

impl TrimRefs {
    pub fn parse(arg: &[u8]) -> Option<Self> {
        if arg.eq_ignore_ascii_case(b"KEEPREF") {
            Some(TrimRefs::KeepRef)
        } else if arg.eq_ignore_ascii_case(b"DELREF") {
            Some(TrimRefs::DelRef)
        } else if arg.eq_ignore_ascii_case(b"ACKED") {
            Some(TrimRefs::Acked)
        } else {
            None
        }
    }
}

/// How far to trim.
pub(crate) enum TrimTo {
    /// Keep at most this many entries (the newest).
    MaxLen(u64),
    /// Remove entries with an ID below this one.
    MinId(StreamId),
}

/// Every group has been delivered `id` and none still has it pending.
pub(super) fn acked_by_all_groups(stream: &StreamInner, id: &StreamId) -> bool {
    stream
        .groups
        .values()
        .all(|g| *id <= g.last_id && !g.pel.contains_key(id))
}

/// Parse `[KEEPREF | DELREF | ACKED] IDS numids id [id ...]`, the tail of
/// `XDELEX` and `XACKDEL`. The policy may come before or after the IDS block.
///
/// `legacy_ids` accepts the bare `id [id ...]` form `XACKDEL` took before it
/// followed Redis's syntax.
pub(super) fn parse_policy_and_ids(
    args: &[&[u8]],
    legacy_ids: bool,
) -> Result<(TrimRefs, Vec<StreamId>), Vec<u8>> {
    use crate::resp::write::resp_error;
    let parse_id = |raw: &&[u8]| {
        std::str::from_utf8(raw)
            .ok()
            .and_then(StreamId::parse)
            .ok_or_else(|| resp_error("ERR Invalid stream ID specified as stream command argument"))
    };

    let mut policy: Option<TrimRefs> = None;
    let mut ids: Option<Vec<StreamId>> = None;
    let mut i = 0;
    while i < args.len() {
        if let Some(refs) = TrimRefs::parse(args[i]).or_else(|| {
            // `REF` is what KiviDB called ACKED before Redis named it.
            args[i]
                .eq_ignore_ascii_case(b"REF")
                .then_some(TrimRefs::Acked)
        }) {
            if policy.replace(refs).is_some() {
                return Err(resp_error("ERR syntax error"));
            }
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"IDS") {
            let numids: usize = args
                .get(i + 1)
                .and_then(|n| std::str::from_utf8(n).ok())
                .and_then(|n| n.parse().ok())
                .filter(|n| *n > 0)
                .ok_or_else(|| resp_error("ERR Number of IDs must be a positive integer"))?;
            let Some(raw) = args.get(i + 2..(i + 2).saturating_add(numids)) else {
                return Err(resp_error(
                    "ERR The `numids` parameter must match the number of arguments",
                ));
            };
            if ids.is_some() {
                return Err(resp_error("ERR syntax error"));
            }
            // All of them before anything is touched: a malformed ID rejects
            // the whole command.
            ids = Some(raw.iter().map(parse_id).collect::<Result<_, _>>()?);
            i += 2 + numids;
        } else if legacy_ids && policy.is_none() && ids.is_none() && i == 0 {
            ids = Some(args.iter().map(parse_id).collect::<Result<_, _>>()?);
            break;
        } else {
            return Err(resp_error("ERR syntax error"));
        }
    }
    match ids {
        Some(ids) => Ok((policy.unwrap_or_default(), ids)),
        None => Err(resp_error("ERR syntax error")),
    }
}

/// Remove `id` from the stream under `refs`, for a command that deletes
/// entries by ID. False if the policy is `ACKED` and some group has yet to
/// read or acknowledge the entry: it stays.
pub(super) fn delete_entry(stream: &mut StreamInner, id: &StreamId, refs: TrimRefs) -> bool {
    if refs == TrimRefs::Acked && !acked_by_all_groups(stream, id) {
        return false;
    }
    if stream.entries.remove(id).is_some() && *id > stream.max_deleted_id {
        stream.max_deleted_id = id.clone();
    }
    if refs == TrimRefs::DelRef {
        for group in stream.groups.values_mut() {
            group.pel.remove(id);
        }
    }
    true
}

/// Trim `stream` from its oldest entry forward, stopping at the first entry
/// the policy does not allow to go. Returns the number removed.
pub(super) fn trim_stream(stream: &mut StreamInner, to: &TrimTo, refs: TrimRefs) -> usize {
    let mut removed = 0usize;
    while let Some(id) = stream.entries.keys().next() {
        let over = match to {
            TrimTo::MaxLen(max) => stream.entries.len() as u64 > *max,
            TrimTo::MinId(min) => id < min,
        };
        if !over || !remove_oldest(stream, refs) {
            break;
        }
        removed += 1;
    }
    removed
}

/// Remove the oldest entry, unless the policy says it has to stay.
fn remove_oldest(stream: &mut StreamInner, refs: TrimRefs) -> bool {
    let Some(id) = stream.entries.keys().next().cloned() else {
        return false;
    };
    if refs == TrimRefs::Acked && !acked_by_all_groups(stream, &id) {
        return false;
    }
    // `max_deleted_id` is not moved: it marks where an entry was deleted
    // from *between* others (`XDEL`), which is what makes a group's count of
    // what it has read uncertain. Entries trimmed from the front leave no
    // such gap. See `Shape`.
    stream.entries.remove(&id);
    if refs == TrimRefs::DelRef {
        for g in stream.groups.values_mut() {
            g.pel.remove(&id);
        }
    }
    true
}

/// `stream-node-max-entries`: how many entries Redis keeps in one node of a
/// stream. Here it only decides what `~` trims.
pub(crate) static NODE_MAX_ENTRIES: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(100);

/// `stream-node-max-bytes`: how large Redis lets one node of a stream grow
/// (0 for no limit). With entries of a few hundred bytes — BullMQ's events,
/// say — this, not `stream-node-max-entries`, is what ends a node, and so
/// what `~` trims: a node of 4096 bytes holds about 15 of them.
pub(crate) static NODE_MAX_BYTES: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(4096);

/// Record an entry about to be added to `stream` in the nodes Redis would
/// have: it opens a new node if the newest is full by
/// `stream-node-max-entries` or would pass `stream-node-max-bytes`. Called
/// before the entry is inserted and `entries_added` counts it.
pub(crate) fn record_append(stream: &mut StreamInner, fields: &crate::types::FieldsType) {
    use std::hash::{Hash, Hasher};
    let max_entries = Approx::new(None).node;
    let max_bytes = NODE_MAX_BYTES
        .load(std::sync::atomic::Ordering::Relaxed)
        .max(0) as u64;
    let mut names = std::collections::hash_map::DefaultHasher::new();
    for (field, _) in fields {
        field.hash(&mut names);
    }
    let names = names.finish();
    // A listpack entry costs its length and a couple of bytes of header; an
    // entry also has its ID, flags and count (Redis's t_stream.c layout).
    let values: u64 = fields.iter().map(|(_, v)| v.len() as u64 + 2).sum();
    let field_names: u64 = fields.iter().map(|(f, _)| f.len() as u64 + 2).sum();
    let fill = stream.node_fill;
    let size = |same_names: bool| 6 + values + if same_names { 0 } else { field_names + 1 };
    let new_node = stream.entries.is_empty()
        || fill.entries == 0
        || fill.entries >= max_entries
        || (max_bytes != 0 && fill.bytes + size(names == fill.fields) >= max_bytes);
    if new_node {
        stream.node_starts.push_back(stream.entries_added);
        // The first entry holds the node's field names (the "master entry").
        stream.node_fill = NodeFill {
            entries: 1,
            bytes: 12 + size(false),
            fields: names,
        };
    } else {
        stream.node_fill.entries += 1;
        stream.node_fill.bytes += size(names == fill.fields);
    }
    // Starts behind the oldest entry are of nodes already trimmed away.
    let first = stream
        .entries_added
        .saturating_sub(stream.entries.len() as u64);
    while stream.node_starts.len() > 1 && stream.node_starts[1] <= first {
        stream.node_starts.pop_front();
    }
}

/// How many entries are left in the node that holds the entry with ordinal
/// `first`, out of `len` from there on.
fn node_len(stream: &StreamInner, first: u64, len: u64, per_node: u64) -> u64 {
    // Nodes from before any were recorded (a stream loaded from disk) are
    // taken to be `stream-node-max-entries` long.
    let fixed = per_node - first % per_node;
    let end = match stream.node_starts.front() {
        Some(&recorded) if first >= recorded => stream
            .node_starts
            .iter()
            .find(|&&start| start > first)
            .map_or(len, |&start| start - first),
        Some(&recorded) => fixed.min(recorded - first),
        None => fixed,
    };
    end.min(len)
}

/// Trimming with `~`: "about this much, whatever is cheap".
///
/// In Redis a stream is a tree of nodes of up to `stream-node-max-entries`
/// entries, and `~` removes whole nodes only — never part of one — and at
/// most `LIMIT` entries in one go. There are no such nodes here, and an
/// exact trim costs the same as an approximate one; but how much a `~` trim
/// removes is something clients can see (`XLEN` after `XADD … MAXLEN ~ n`),
/// so the same entries go as would in Redis: the stream is treated as if cut
/// into nodes of that size in the order its entries were added.
#[derive(Clone, Copy)]
pub(crate) struct Approx {
    /// The most entries to remove; zero for no limit.
    limit: u64,
    node: u64,
}

impl Approx {
    /// `limit` is `LIMIT`'s argument, if it was given.
    pub fn new(limit: Option<u64>) -> Self {
        let node = match NODE_MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed) {
            // Zero is "no limit on entries" in Redis, where a node's size in
            // bytes then decides. Nothing here has such a size.
            n if n <= 0 => 100,
            n => n as u64,
        };
        Approx {
            limit: limit.unwrap_or_else(|| (100 * node).min(1_000_000)),
            node,
        }
    }
}

/// How many nodes `stream` comes to (see `record_append`).
pub(super) fn node_count(stream: &StreamInner) -> u64 {
    let per_node = Approx::new(None).node;
    let len = stream.entries.len() as u64;
    let mut at = stream.entries_added.saturating_sub(len);
    let mut nodes = 0;
    while at < stream.entries_added {
        at += node_len(stream, at, stream.entries_added - at, per_node).max(1);
        nodes += 1;
    }
    nodes
}

/// Trim `stream` as `~` does. Returns the number of entries removed.
pub(super) fn trim_stream_approx(
    stream: &mut StreamInner,
    to: &TrimTo,
    refs: TrimRefs,
    approx: Approx,
) -> usize {
    let mut removed = 0u64;
    loop {
        let len = stream.entries.len() as u64;
        if len == 0 {
            break;
        }
        // What is left of the node the oldest entry is in.
        let first = stream.entries_added.saturating_sub(len);
        let node = node_len(stream, first, len, approx.node);
        if approx.limit != 0 && removed + node > approx.limit {
            break;
        }
        let whole = match to {
            TrimTo::MaxLen(max) => len - node >= *max,
            TrimTo::MinId(min) => stream
                .entries
                .keys()
                .nth(node as usize - 1)
                .is_some_and(|last| last < min),
        };
        if !whole {
            break;
        }
        for _ in 0..node {
            if !remove_oldest(stream, refs) {
                return removed as usize;
            }
            removed += 1;
        }
    }
    removed as usize
}

/// Rewrite the trimming arguments in `parts[from..to]` to say exactly what a
/// `~` trim did, for propagation: a replica (or the AOF, replayed) given
/// `MAXLEN ~ 1000` would decide for itself how much "about" is — by its own
/// `stream-node-max-entries` — and end up with a different stream.
pub(super) fn exact_trim_args(
    parts: &mut Vec<Vec<u8>>,
    from: usize,
    to: usize,
    stream: &StreamInner,
) {
    let mut to = to.min(parts.len());
    let mut i = from;
    while i < to {
        let maxlen = parts[i].eq_ignore_ascii_case(b"MAXLEN");
        if (maxlen || parts[i].eq_ignore_ascii_case(b"MINID"))
            && i + 2 < parts.len()
            && parts[i + 1] == b"~"
        {
            parts[i + 1] = b"=".to_vec();
            if maxlen {
                parts[i + 2] = stream.entries.len().to_string().into_bytes();
            } else if let Some(first) = stream.entries.keys().next() {
                parts[i + 2] = first.to_string().into_bytes();
            }
            i += 3;
        } else if parts[i].eq_ignore_ascii_case(b"LIMIT") && i + 1 < to {
            parts.drain(i..i + 2);
            to -= 2;
        } else {
            i += 1;
        }
    }
}

// ── Replication of consumer-group deliveries ─────────────────────────────────

/// The command a replica runs to end up with the same pending-list entry the
/// master just wrote: who owns `id`, when it was delivered, how many times.
///
/// `XREADGROUP`, `XCLAIM` and `XAUTOCLAIM` cannot be replayed as typed: which
/// entries they pick depends on the clock (idle times) and on what else has
/// been delivered, so a replica re-running them reaches a different pending
/// list. Redis propagates this same explicit form.
pub(super) fn claim_effect(
    key: &str,
    group: &str,
    consumer: &str,
    id: &StreamId,
    delivered_at_ms: u64,
    delivery_count: u64,
) -> Vec<Vec<u8>> {
    [
        "XCLAIM",
        key,
        group,
        consumer,
        "0",
        &id.to_string(),
        "TIME",
        &delivered_at_ms.to_string(),
        "RETRYCOUNT",
        &delivery_count.to_string(),
        "FORCE",
        "JUSTID",
    ]
    .iter()
    .map(|s| s.as_bytes().to_vec())
    .collect()
}

// ── How far a group has read ──────────────────────────────────────────────────

/// What a stream looks like to the arithmetic of consumer-group lag: how
/// many entries it has and has ever had, where it starts and ends, and the
/// largest ID `XDEL` has removed.
///
/// A group's lag is `entries_added - entries_read` — as long as every entry
/// between the group's position and the end of the stream is still there.
/// An `XDEL` in that range makes the subtraction wrong by an amount nobody
/// has recorded, so the lag is then unknown (`nil`) until the group reads
/// past the hole. This is Redis's reckoning, case for case.
#[derive(Clone)]
pub(super) struct Shape {
    len: u64,
    entries_added: u64,
    /// The first entry's ID; `0-0` in an empty stream.
    first: StreamId,
    last_id: StreamId,
    max_deleted: StreamId,
}

impl Shape {
    pub fn of(stream: &StreamInner) -> Self {
        Shape {
            len: stream.entries.len() as u64,
            entries_added: stream.entries_added,
            first: stream
                .entries
                .keys()
                .next()
                .cloned()
                .unwrap_or_else(StreamId::min),
            last_id: stream.last_id.clone(),
            max_deleted: stream.max_deleted_id.clone(),
        }
    }

    /// Whether an entry has been deleted at or after `start`, among the
    /// entries that are left.
    fn has_tombstones_from(&self, start: &StreamId) -> bool {
        if self.len == 0 || self.max_deleted == StreamId::min() {
            return false;
        }
        // The last deletion was before everything that is left.
        if self.first > self.max_deleted {
            return false;
        }
        *start <= self.max_deleted
    }

    /// How many entries had been added when `id` was: the count a group
    /// that has read up to `id` has read. `None` when it cannot be known.
    fn counter_of(&self, id: &StreamId) -> Option<u64> {
        if self.entries_added == 0 {
            return Some(0);
        }
        if self.len == 0 && *id <= self.last_id {
            return Some(self.entries_added);
        }
        match id.cmp(&self.last_id) {
            std::cmp::Ordering::Equal => return Some(self.entries_added),
            std::cmp::Ordering::Greater => return None,
            std::cmp::Ordering::Less => {}
        }
        if self.max_deleted == StreamId::min() || self.max_deleted < self.first {
            // Nothing has been deleted from among what is left.
            match id.cmp(&self.first) {
                std::cmp::Ordering::Less => return Some(self.entries_added - self.len),
                std::cmp::Ordering::Equal => return Some(self.entries_added - self.len + 1),
                std::cmp::Ordering::Greater => {}
            }
        }
        None
    }

    /// The number of entries `group` has yet to be delivered, if it can be
    /// told.
    pub fn lag(&self, group: &ConsumerGroup) -> Option<u64> {
        if self.entries_added == 0 || self.len == 0 {
            return Some(0);
        }
        // The group and the last deletion are both behind everything that
        // is left: all of it is still to come.
        if group.last_id < self.first && self.max_deleted < self.first {
            return Some(self.len);
        }
        if group.entries_read != ConsumerGroup::NO_COUNT
            && !self.has_tombstones_from(&group.last_id)
        {
            return Some(self.entries_added.saturating_sub(group.entries_read));
        }
        self.counter_of(&group.last_id)
            .map(|read| self.entries_added.saturating_sub(read))
    }

    /// `group` is delivered the entry `id`, the next one after its position.
    pub fn deliver(&self, group: &mut ConsumerGroup, id: &StreamId) {
        if group.entries_read != ConsumerGroup::NO_COUNT
            && group.last_id >= self.first
            && !self.has_tombstones_from(&group.last_id)
        {
            group.entries_read += 1;
        } else if self.entries_added != 0 {
            group.entries_read = self.counter_of(id).unwrap_or(ConsumerGroup::NO_COUNT);
        }
        group.last_id = id.clone();
    }
}

/// `entries_read` as `ENTRIESREAD` takes it and `XINFO` shows it.
pub(super) fn entries_read_arg(entries_read: u64) -> String {
    if entries_read == ConsumerGroup::NO_COUNT {
        "-1".to_string()
    } else {
        entries_read.to_string()
    }
}

/// The command that moves a group's read position on a replica.
pub(super) fn setid_effect(
    key: &str,
    group: &str,
    last_id: &StreamId,
    entries_read: u64,
) -> Vec<Vec<u8>> {
    [
        "XGROUP",
        "SETID",
        key,
        group,
        &last_id.to_string(),
        "ENTRIESREAD",
        &entries_read_arg(entries_read),
    ]
    .iter()
    .map(|s| s.as_bytes().to_vec())
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PelEntry, StreamEntry};

    const DEFAULTS: (u64, u64) = (100, 100);

    fn id(ms: u64) -> StreamId {
        StreamId::new(ms, 0)
    }

    fn stream_with(n: u64) -> StreamInner {
        let mut s = StreamInner::new();
        for ms in 1..=n {
            s.entries.insert(id(ms), StreamEntry { fields: Vec::new() });
            s.last_id = id(ms);
        }
        s
    }

    #[test]
    fn a_repeated_idempotent_id_finds_the_original_entry() {
        let mut s = StreamInner::new();
        assert_eq!(idmp_lookup(&mut s, b"p", b"1", 1000, DEFAULTS), None);
        idmp_record(&mut s, b"p", Arc::from(&b"1"[..]), id(5), 1000, DEFAULTS);

        assert_eq!(idmp_lookup(&mut s, b"p", b"1", 2000, DEFAULTS), Some(id(5)));
        // Same iid, different producer: unrelated.
        assert_eq!(idmp_lookup(&mut s, b"q", b"1", 2000, DEFAULTS), None);
        assert_eq!(s.idmp.iids_added, 1);
        assert_eq!(s.idmp.iids_duplicates, 1);
        assert_eq!(idmp_tracked(&s), (1, 1));
    }

    #[test]
    fn idempotent_ids_expire_after_the_configured_duration() {
        let mut s = StreamInner::new();
        s.idmp.duration_secs = Some(10);
        idmp_record(&mut s, b"p", Arc::from(&b"1"[..]), id(5), 1_000, DEFAULTS);
        assert!(idmp_lookup(&mut s, b"p", b"1", 10_999, DEFAULTS).is_some());
        assert!(idmp_lookup(&mut s, b"p", b"1", 11_001, DEFAULTS).is_none());
        // The producer's map went with its last ID.
        assert_eq!(idmp_tracked(&s), (0, 0));
    }

    #[test]
    fn only_the_most_recent_maxsize_ids_are_kept_per_producer() {
        let mut s = StreamInner::new();
        s.idmp.maxsize = Some(2);
        for n in 1..=3u64 {
            let iid: Arc<[u8]> = Arc::from(n.to_string().as_bytes());
            idmp_record(&mut s, b"p", iid, id(n), 1000, DEFAULTS);
        }
        assert!(idmp_lookup(&mut s, b"p", b"1", 1000, DEFAULTS).is_none());
        assert!(idmp_lookup(&mut s, b"p", b"2", 1000, DEFAULTS).is_some());
        assert!(idmp_lookup(&mut s, b"p", b"3", 1000, DEFAULTS).is_some());
        assert_eq!(idmp_tracked(&s), (1, 2));
    }

    #[test]
    fn content_ids_distinguish_messages_and_ignore_nothing() {
        let f = |pairs: &[(&str, &str)]| -> Vec<Field> {
            pairs
                .iter()
                .map(|(a, b)| (Arc::from(a.as_bytes()), Arc::from(b.as_bytes())))
                .collect()
        };
        let auto = IdmpArg::Auto { producer: b"p" };
        assert_eq!(auto.iid(&f(&[("a", "1")])), auto.iid(&f(&[("a", "1")])));
        assert_ne!(auto.iid(&f(&[("a", "1")])), auto.iid(&f(&[("a", "2")])));
        // Field boundaries are part of the content.
        assert_ne!(auto.iid(&f(&[("ab", "c")])), auto.iid(&f(&[("a", "bc")])));
        // Order is too.
        assert_ne!(
            auto.iid(&f(&[("a", "1"), ("b", "2")])),
            auto.iid(&f(&[("b", "2"), ("a", "1")]))
        );
    }

    #[test]
    fn the_budget_caps_count_across_streams() {
        let mut b = ReadBudget::new(Some(3), None);
        assert!(b.admit(10) && b.admit(10) && b.admit(10));
        assert!(b.exhausted());
        assert!(!b.admit(10));
    }

    #[test]
    fn the_size_cap_always_lets_one_entry_through_and_then_closes() {
        let mut b = ReadBudget::new(None, Some(50));
        assert!(
            b.admit(5000),
            "the first entry is returned whatever its size"
        );
        assert!(!b.admit(1));
        assert!(b.exhausted(), "a refused entry closes the budget");

        let mut b = ReadBudget::new(None, Some(100));
        assert!(b.admit(60));
        assert!(!b.admit(60));
        assert!(
            !b.admit(1),
            "a smaller entry must not overtake a refused one"
        );
    }

    #[test]
    fn maxcount_may_not_be_below_count() {
        let caps = ReadCaps {
            max_count: Some(5),
            max_size: None,
        };
        assert!(caps.check_against_count(Some(5)).is_ok());
        assert!(caps.check_against_count(None).is_ok());
        assert!(caps.check_against_count(Some(6)).is_err());
        assert!(ReadCaps::parse_positive("MAXSIZE", Some(&&b"0"[..])).is_err());
        assert!(ReadCaps::parse_positive("MAXSIZE", Some(&&b"x"[..])).is_err());
        assert!(ReadCaps::parse_positive("MAXSIZE", None).is_err());
        assert_eq!(ReadCaps::parse_positive("MAXSIZE", Some(&&b"7"[..])), Ok(7));
    }

    fn group(last: u64, pending: &[u64]) -> ConsumerGroup {
        ConsumerGroup {
            name: "g".into(),
            last_id: id(last),
            entries_read: last,
            pel: pending
                .iter()
                .map(|ms| {
                    (
                        id(*ms),
                        PelEntry {
                            consumer: "c".into(),
                            delivered_at: 1,
                            delivery_count: 1,
                        },
                    )
                })
                .collect(),
            consumers: Default::default(),
            active: Default::default(),
        }
    }

    #[test]
    fn keepref_trims_and_leaves_pending_references() {
        let mut s = stream_with(5);
        s.groups.insert("g".into(), group(5, &[1, 2]));
        assert_eq!(
            trim_stream(&mut s, &TrimTo::MaxLen(2), TrimRefs::KeepRef),
            3
        );
        assert_eq!(s.entries.len(), 2);
        assert_eq!(s.groups["g"].pel.len(), 2);
        // Trimmed, not deleted: the mark `XDEL` leaves is where it was.
        assert_eq!(s.max_deleted_id, StreamId::min());
    }

    #[test]
    fn delref_trims_and_drops_pending_references() {
        let mut s = stream_with(5);
        s.groups.insert("g".into(), group(5, &[1, 4]));
        assert_eq!(
            trim_stream(&mut s, &TrimTo::MinId(id(4)), TrimRefs::DelRef),
            3
        );
        let pending: Vec<_> = s.groups["g"].pel.keys().cloned().collect();
        assert_eq!(pending, vec![id(4)]);
    }

    #[test]
    fn acked_stops_at_the_first_entry_a_group_still_needs() {
        // Read up to 4; 3 is still pending.
        let mut s = stream_with(5);
        s.groups.insert("g".into(), group(4, &[3]));
        assert_eq!(trim_stream(&mut s, &TrimTo::MaxLen(0), TrimRefs::Acked), 2);
        assert_eq!(s.entries.keys().next(), Some(&id(3)));

        // Nothing pending, but 5 has not been delivered yet.
        let mut s = stream_with(5);
        s.groups.insert("g".into(), group(4, &[]));
        assert_eq!(trim_stream(&mut s, &TrimTo::MaxLen(0), TrimRefs::Acked), 4);

        // No groups: nothing holds an entry back.
        let mut s = stream_with(5);
        assert_eq!(trim_stream(&mut s, &TrimTo::MaxLen(1), TrimRefs::Acked), 4);
    }

    #[test]
    fn lag_is_unknown_only_while_a_deletion_lies_ahead() {
        let mut s = stream_with(5);
        s.entries_added = 5;
        let mut g = group(0, &[]);
        g.entries_read = ConsumerGroup::NO_COUNT;
        // Created at 0: nothing read, everything ahead.
        assert_eq!(Shape::of(&s).lag(&g), Some(5));
        Shape::of(&s).deliver(&mut g, &id(1));
        assert_eq!((g.entries_read, Shape::of(&s).lag(&g)), (1, Some(4)));
        // An entry ahead of the group is deleted: how many are left to read
        // is no longer `added - read`.
        delete_entry(&mut s, &id(3), TrimRefs::KeepRef);
        assert_eq!(Shape::of(&s).lag(&g), None);
        Shape::of(&s).deliver(&mut g, &id(2));
        assert_eq!(g.entries_read, ConsumerGroup::NO_COUNT);
        Shape::of(&s).deliver(&mut g, &id(4));
        assert_eq!(Shape::of(&s).lag(&g), None);
        // At the last entry the count is known again: everything added.
        Shape::of(&s).deliver(&mut g, &id(5));
        assert_eq!((g.entries_read, Shape::of(&s).lag(&g)), (5, Some(0)));
        // Trimming leaves no hole, and does not move the deletion mark.
        let mut s = stream_with(5);
        s.entries_added = 5;
        let mut g = group(1, &[]);
        g.entries_read = 1;
        trim_stream(&mut s, &TrimTo::MaxLen(1), TrimRefs::KeepRef);
        assert_eq!(s.max_deleted_id, StreamId::min());
        assert_eq!(Shape::of(&s).lag(&g), Some(1));
        Shape::of(&s).deliver(&mut g, &id(5));
        assert_eq!((g.entries_read, Shape::of(&s).lag(&g)), (5, Some(0)));
        // An emptied stream has nothing to lag behind.
        trim_stream(&mut s, &TrimTo::MaxLen(0), TrimRefs::KeepRef);
        assert_eq!(Shape::of(&s).lag(&g), Some(0));
    }

    #[test]
    fn approximate_trimming_removes_whole_nodes() {
        let approx = |limit| Approx { limit, node: 10 };
        let mut s = StreamInner::new();
        let add = |s: &mut StreamInner, n: u64| {
            for _ in 0..n {
                s.entries_added += 1;
                s.entries
                    .insert(id(s.entries_added), StreamEntry { fields: Vec::new() });
            }
        };
        add(&mut s, 101);
        // Three nodes of ten are within the limit of thirty; a fourth is not.
        let to = TrimTo::MaxLen(55);
        assert_eq!(
            trim_stream_approx(&mut s, &to, TrimRefs::KeepRef, approx(30)),
            30
        );
        assert_eq!(s.entries.len(), 71);
        add(&mut s, 1);
        // One more node, and then removing another would leave fewer than 55.
        assert_eq!(
            trim_stream_approx(&mut s, &to, TrimRefs::KeepRef, approx(30)),
            10
        );
        assert_eq!(s.entries.len(), 62);
        // A node that is not wholly below the ID stays, all of it.
        let to = TrimTo::MinId(id(45));
        assert_eq!(
            trim_stream_approx(&mut s, &to, TrimRefs::KeepRef, approx(0)),
            0
        );
        let to = TrimTo::MinId(id(51));
        assert_eq!(
            trim_stream_approx(&mut s, &to, TrimRefs::KeepRef, approx(0)),
            10
        );
        assert_eq!(s.entries.keys().next(), Some(&id(51)));
        // A limit smaller than the node: nothing.
        let to = TrimTo::MaxLen(0);
        assert_eq!(
            trim_stream_approx(&mut s, &to, TrimRefs::KeepRef, approx(9)),
            0
        );
        // What the replica is told.
        let mut parts: Vec<Vec<u8>> = [
            "XADD", "s", "MAXLEN", "~", "5", "LIMIT", "9", "7-0", "f", "v",
        ]
        .iter()
        .map(|p| p.as_bytes().to_vec())
        .collect();
        exact_trim_args(&mut parts, 2, 7, &s);
        let parts: Vec<&str> = parts
            .iter()
            .map(|p| std::str::from_utf8(p).unwrap())
            .collect();
        assert_eq!(parts, ["XADD", "s", "MAXLEN", "=", "52", "7-0", "f", "v"]);
    }

    #[test]
    fn delivery_effects_are_explicit_and_clock_free() {
        let text = |parts: Vec<Vec<u8>>| -> String {
            parts
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect::<Vec<_>>()
                .join(" ")
        };
        assert_eq!(
            text(claim_effect("s", "g", "c", &StreamId::new(7, 1), 1234, 3)),
            "XCLAIM s g c 0 7-1 TIME 1234 RETRYCOUNT 3 FORCE JUSTID"
        );
        assert_eq!(
            text(setid_effect("s", "g", &StreamId::new(7, 1), 9)),
            "XGROUP SETID s g 7-1 ENTRIESREAD 9"
        );
    }
}
