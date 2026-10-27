//! Time Series command handlers.
//!
//! All 17 TS.* commands are implemented here. The entry point is
//! `execute_ts`, which is called from the main command dispatcher.
//!
//! Design principles:
//!   - Write-lock is taken per-key (one shard), never globally.
//!   - The label index (SharedTsIndex) is locked separately and briefly.
//!   - No allocation happens while a lock is held unless unavoidable.

use crate::commands::ctx::Ctx;
use crate::commands::ts_query;
use crate::resp::write::{
    resp_array, resp_bulk_str, resp_error, resp_integer, resp_null_bulk, resp_simple,
};
use crate::store::Key;
use crate::store::{self, SharedTsIndex, Store};
use crate::types::{CompactionRule, DuplicatePolicy, KiviDBValue, TsAggregation, TsInner};
use crate::{read_lock, write_lock};
use ahash::AHashSet;
use parking_lot::RwLock;
use std::sync::Arc;

// ── Label filter parsing ──────────────────────────────────────────────────────
//
// Filter syntax (from the Redis TS docs):
//   l=v      label equals value
//   l!=v     label not equals value
//   l=       label has no value (key exists but value is empty string, OR key absent)
//   l!=      label has a value
//   l=(v1,v2,...) label in set
//   l!=(v1,v2,...) label not in set

#[derive(Debug)]
pub(super) enum LabelFilter {
    Eq(String, String),
    Ne(String, String),
    Exists(String),    // l!=  (label must have some value)
    NotExists(String), // l=   (label must be absent or empty)
    In(String, Vec<String>),
    NotIn(String, Vec<String>),
}

impl LabelFilter {
    pub(super) fn parse(s: &str) -> Option<Self> {
        // l!=(v1,v2)
        if let Some(pos) = s.find("!=(") {
            let label = s[..pos].to_string();
            let vals_str = s[pos + 3..].trim_end_matches(')');
            let vals = vals_str.split(',').map(|v| v.to_string()).collect();
            return Some(LabelFilter::NotIn(label, vals));
        }
        // l=(v1,v2)
        if let Some(pos) = s.find("=(") {
            let label = s[..pos].to_string();
            let vals_str = s[pos + 2..].trim_end_matches(')');
            let vals = vals_str.split(',').map(|v| v.to_string()).collect();
            return Some(LabelFilter::In(label, vals));
        }
        // l!=v  or  l!=
        if let Some(pos) = s.find("!=") {
            let label = s[..pos].to_string();
            let val = &s[pos + 2..];
            return Some(if val.is_empty() {
                LabelFilter::Exists(label)
            } else {
                LabelFilter::Ne(label, val.to_string())
            });
        }
        // l=v  or  l=
        if let Some(pos) = s.find('=') {
            let label = s[..pos].to_string();
            let val = &s[pos + 1..];
            return Some(if val.is_empty() {
                LabelFilter::NotExists(label)
            } else {
                LabelFilter::Eq(label, val.to_string())
            });
        }
        None
    }

    pub(super) fn matches(&self, labels: &ahash::AHashMap<String, String>) -> bool {
        match self {
            LabelFilter::Eq(l, v) => labels.get(l).map(|s| s == v).unwrap_or(false),
            LabelFilter::Ne(l, v) => labels.get(l).map(|s| s != v).unwrap_or(true),
            LabelFilter::Exists(l) => labels.contains_key(l),
            LabelFilter::NotExists(l) => !labels.contains_key(l),
            LabelFilter::In(l, vs) => labels
                .get(l)
                .map(|s| vs.iter().any(|v| v == s))
                .unwrap_or(false),
            LabelFilter::NotIn(l, vs) => labels
                .get(l)
                .map(|s| vs.iter().all(|v| v != s))
                .unwrap_or(true),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

type TsOptions = (u64, usize, Option<DuplicatePolicy>, Vec<(String, String)>);
/// Parse optional common TS options from a token slice.
/// Returns (retention, chunk_size, duplicate_policy, labels, idx_consumed).
fn parse_ts_options(tokens: &[&[u8]]) -> Result<TsOptions, Vec<u8>> {
    let mut retention = 0u64;
    let mut chunk_size = 4096usize;
    let mut dup_policy: Option<DuplicatePolicy> = None;
    let mut labels: Vec<(String, String)> = Vec::new();
    let mut i = 0;

    while i < tokens.len() {
        let opt = match std::str::from_utf8(tokens[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return Err(resp_error("ERR invalid option")),
        };

        match opt.as_str() {
            "RETENTION" => {
                i += 1;
                if i >= tokens.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                retention = std::str::from_utf8(tokens[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| resp_error("ERR invalid RETENTION value"))?;
            }
            "CHUNK" => {
                // CHUNK SIZE <n>
                i += 1;
                if i >= tokens.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                // next token should be "SIZE"
                i += 1;
                if i >= tokens.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                chunk_size = std::str::from_utf8(tokens[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| resp_error("ERR invalid CHUNK SIZE value"))?;
            }
            "ENCODING" => {
                // ENCODING COMPRESSED | UNCOMPRESSED — we accept but ignore (we always use BTreeMap)
                i += 1;
            }
            "DUPLICATE_POLICY" | "ON" => {
                // ON DUPLICATE <policy>  OR  DUPLICATE_POLICY <policy>
                if opt == "ON" {
                    i += 1; // skip "DUPLICATE"
                }
                i += 1;
                if i >= tokens.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                let pol_str = std::str::from_utf8(tokens[i]).unwrap_or("");
                dup_policy = Some(
                    DuplicatePolicy::parse(pol_str)
                        .ok_or_else(|| resp_error("ERR invalid DUPLICATE POLICY"))?,
                );
            }
            "DUPLICATE" => {
                // DUPLICATE POLICY <policy>
                i += 1; // skip "POLICY"
                i += 1;
                if i >= tokens.len() {
                    return Err(resp_error("ERR syntax error"));
                }
                let pol_str = std::str::from_utf8(tokens[i]).unwrap_or("");
                dup_policy = Some(
                    DuplicatePolicy::parse(pol_str)
                        .ok_or_else(|| resp_error("ERR invalid DUPLICATE POLICY"))?,
                );
            }
            "LABELS" => {
                i += 1;
                // Consume pairs until we hit another known keyword or end
                while i + 1 < tokens.len() {
                    let next_opt = std::str::from_utf8(tokens[i])
                        .unwrap_or("")
                        .to_ascii_uppercase();
                    if matches!(
                        next_opt.as_str(),
                        "RETENTION"
                            | "CHUNK"
                            | "ENCODING"
                            | "DUPLICATE_POLICY"
                            | "ON"
                            | "DUPLICATE"
                    ) {
                        break;
                    }
                    let lk = std::str::from_utf8(tokens[i]).unwrap_or("").to_string();
                    let lv = std::str::from_utf8(tokens[i + 1]).unwrap_or("").to_string();
                    labels.push((lk, lv));
                    i += 2;
                }
                continue; // don't i += 1 at bottom
            }
            "UNCOMPRESSED" => { /* accepted, ignored */ }
            _ => {
                // Unknown token — stop consuming (caller handles remaining tokens)
                break;
            }
        }
        i += 1;
    }

    Ok((retention, chunk_size, dup_policy, labels))
}

/// Update the label index for a key. Clears old labels for this key and
/// inserts the new ones. Takes a write lock on the index.
fn update_label_index(
    ts_index: &SharedTsIndex,
    key: &str,
    old_labels: &ahash::AHashMap<String, String>,
    new_labels: &[(String, String)],
) {
    if old_labels.is_empty() && new_labels.is_empty() {
        return;
    }
    let mut idx = write_lock!(ts_index);

    // Remove key from all old label buckets
    for (lk, lv) in old_labels {
        let index_key = format!("{}={}", lk, lv);
        if let Some(set) = idx.get_mut(&index_key) {
            set.remove(key);
        }
    }

    // Insert key into new label buckets
    for (lk, lv) in new_labels {
        let index_key = format!("{}={}", lk, lv);
        idx.entry(index_key).or_default().insert(key.to_string());
    }
}

/// Collect all TS keys matching a set of label filters.
/// Uses the label index for the first Eq filter, then filters the rest in memory.
pub(super) fn keys_matching_filters(
    store: &Store,
    db_idx: usize,
    ts_index: &SharedTsIndex,
    filters: &[LabelFilter],
) -> Vec<String> {
    // Find the first Eq filter to use as a seed from the index
    let seed: Option<AHashSet<String>> = filters.iter().find_map(|f| {
        if let LabelFilter::Eq(l, v) = f {
            let idx = read_lock!(ts_index);
            let key = format!("{}={}", l, v);
            idx.get(&key).cloned()
        } else {
            None
        }
    });

    let db = &store[db_idx];

    // If we got a seed set, iterate only those keys; otherwise scan all shards
    let candidates: Vec<String> = match seed {
        Some(set) => set.into_iter().collect(),
        None => {
            let mut all = Vec::new();
            for shard in db.iter() {
                let map = read_lock!(shard);
                for (k, v) in map.iter() {
                    if matches!(v, KiviDBValue::TimeSeries(_)) {
                        all.push(k.to_string());
                    }
                }
            }
            all
        }
    };

    // Apply all filters
    candidates
        .into_iter()
        .filter(|k| {
            let shard = store::get_shard(db, k);
            let map = read_lock!(shard);
            if let Some(KiviDBValue::TimeSeries(inner)) = map.get(k.as_str()) {
                // Clone just the labels AHashMap — cheap for small label sets
                let labels = inner.read().labels.clone();
                filters.iter().all(|f| f.matches(&labels))
            } else {
                false
            }
        })
        .collect()
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

type Series = Arc<RwLock<TsInner>>;

/// The series at `key`, created empty if there is nothing there. The flag
/// says whether it was created.
fn series_for_append(ctx: &Ctx, key: &str) -> Result<(Series, bool), Vec<u8>> {
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    match map.get(key) {
        Some(KiviDBValue::TimeSeries(inner)) => Ok((Arc::clone(inner), false)),
        Some(_) => Err(resp_error(WRONGTYPE)),
        None => {
            let mut fresh = TsInner::new();
            fresh.duplicate_policy = DuplicatePolicy::server_default();
            let series: Series = Arc::new(RwLock::new(fresh));
            map.insert(Key::from(key), KiviDBValue::TimeSeries(Arc::clone(&series)));
            Ok((series, true))
        }
    }
}

/// What follows every sample written to `series`: its compactions are brought
/// up to date, and anyone blocked in `TS.READ` on it is woken.
///
/// Each compaction rule's bucket containing `ts` is recomputed from the
/// source's samples and written to the destination. It used to be folded in
/// incrementally as `agg(previous bucket value, new sample)`, which is only
/// right for `min`, `max`, `sum`, `first` and `last`: an `avg` rule produced
/// the average of the running average and the newest sample, and a `count`
/// rule never got past 2. It also ran for `TS.ADD` alone — samples written
/// with `TS.MADD`, `TS.INCRBY` or `TS.DECRBY` never reached a compaction.
///
/// As RedisTimeSeries: a bucket is written to the destination once it has
/// closed, when the first sample of a later one arrives, and again if a
/// sample is added to it after; the bucket still open is reported only to
/// `LATEST` ([`ts_query::open_bucket`]). The open bucket was written as
/// samples came, so a reader saw partial values as final (#128).
fn after_append(ctx: &Ctx, key: &str, series: &Series, ts: u64) {
    let updates: Vec<(String, u64, f64)> = {
        let source = series.read();
        let Some(&newest) = source.samples.keys().next_back() else {
            return;
        };
        source
            .rules
            .iter()
            .filter(|rule| rule.bucket_duration > 0 && rule.dest_key != key)
            .filter_map(|rule| {
                let bucket_of =
                    |t: u64| ts_query::bucket_start(t, rule.align_timestamp, rule.bucket_duration);
                let (start, open) = (bucket_of(ts), bucket_of(newest));
                let closed = if start < open {
                    // A bucket that had closed, changed.
                    start
                } else {
                    // The open one: if this is its first sample, the one
                    // before it has just closed.
                    let (first, last) = ts_query::bucket_bounds(start, rule.bucket_duration);
                    if source.samples.range(first..=last).nth(1).is_some() {
                        return None;
                    }
                    let (&before, _) = source.samples.range(..first).next_back()?;
                    bucket_of(before)
                };
                let (first, value) = ts_query::bucket_value(&source, rule, closed);
                Some((rule.dest_key.clone(), first, value))
            })
            .collect()
    };
    for (dest_key, bucket, value) in updates {
        let dest = {
            let shard = store::get_shard(&ctx.store[*ctx.selected_db], &dest_key);
            match read_lock!(shard).get(dest_key.as_str()) {
                Some(KiviDBValue::TimeSeries(inner)) => Arc::clone(inner),
                // The destination was deleted or replaced; the rule is dead.
                _ => continue,
            }
        };
        {
            let mut dest = dest.write();
            dest.samples.insert(bucket, value);
            if dest.last_ts.is_none_or(|last| bucket > last) {
                dest.last_ts = Some(bucket);
            }
        }
        ctx.wait_table.notify_signal(&dest_key);
    }
    ctx.wait_table.notify_signal(key);
}

// ── Main dispatcher ───────────────────────────────────────────────────────────

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "TS.CREATE" => ts_create(ctx),
        "TS.ALTER" => ts_alter(ctx),
        "TS.ADD" => ts_add(ctx),
        "TS.MADD" => ts_madd(ctx),
        "TS.INCRBY" => ts_incrby(ctx, 1.0),
        "TS.DECRBY" => ts_incrby(ctx, -1.0),
        "TS.DEL" => ts_del(ctx),
        "TS.GET" => ts_get(ctx),
        "TS.MGET" => ts_mget(ctx),
        "TS.RANGE" => ts_query::ts_range(ctx, false),
        "TS.REVRANGE" => ts_query::ts_range(ctx, true),
        "TS.MRANGE" => ts_query::ts_mrange(ctx, false),
        "TS.MREVRANGE" => ts_query::ts_mrange(ctx, true),
        "TS.NRANGE" => ts_query::ts_nrange(ctx, false),
        "TS.NREVRANGE" => ts_query::ts_nrange(ctx, true),
        "TS.READ" => ts_query::ts_read(ctx),
        "TS.QUERYLABELS" => ts_query::ts_querylabels(ctx),
        "TS.INFO" => ts_info(ctx),
        "TS.QUERYINDEX" => ts_queryindex(ctx),
        "TS.CREATERULE" => ts_createrule(ctx),
        "TS.DELETERULE" => ts_deleterule(ctx),
        _ => None,
    }
}

// ── TS.CREATE ─────────────────────────────────────────────────────────────────

fn ts_create(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.create' command",
        ));
    }
    let key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let (retention, chunk_size, dup_policy, labels) = match parse_ts_options(&cmd[2..]) {
        Ok(v) => v,
        Err(e) => return Some(e),
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &key);
    let mut map = write_lock!(shard);

    if map.contains_key(key.as_str()) {
        return Some(resp_error("TSDB: key already exists"));
    }

    let mut ts = TsInner::new();
    ts.retention = retention;
    ts.chunk_size = chunk_size;
    ts.duplicate_policy = dup_policy.unwrap_or_else(DuplicatePolicy::server_default);
    for (lk, lv) in &labels {
        ts.labels.insert(lk.clone(), lv.clone());
    }

    map.insert(
        Key::from(&*key),
        KiviDBValue::TimeSeries(Arc::new(RwLock::new(ts))),
    );
    drop(map);

    update_label_index(ctx.ts_index, &key, &ahash::AHashMap::new(), &labels);
    Some(resp_simple("OK"))
}

// ── TS.ALTER ──────────────────────────────────────────────────────────────────

fn ts_alter(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.alter' command",
        ));
    }
    let key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let (retention, chunk_size, dup_policy, new_labels) = match parse_ts_options(&cmd[2..]) {
        Ok(v) => v,
        Err(e) => return Some(e),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = write_lock!(shard);

    match map.get(key.as_str()) {
        Some(KiviDBValue::TimeSeries(inner)) => {
            let mut ts = inner.write();
            let old_labels = ts.labels.clone();

            if retention > 0 {
                ts.retention = retention;
            }
            if chunk_size != 4096 {
                ts.chunk_size = chunk_size;
            }
            if let Some(dp) = dup_policy {
                ts.duplicate_policy = dp;
            }

            if !new_labels.is_empty() {
                ts.labels.clear();
                for (lk, lv) in &new_labels {
                    ts.labels.insert(lk.clone(), lv.clone());
                }
                drop(ts);
                drop(map);
                update_label_index(ctx.ts_index, &key, &old_labels, &new_labels);
            }

            Some(resp_simple("OK"))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_error("TSDB: the key does not exist")),
    }
}

// ── TS.ADD ────────────────────────────────────────────────────────────────────
//
// Key teaching point: TS.ADD auto-creates the key if it doesn't exist,
// applying any supplied options. This requires a write lock from the start —
// we can't do read-check then write-upgrade because that's a TOCTOU race.

fn ts_add(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.add' command",
        ));
    }

    let key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let ts_str = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR invalid timestamp")),
    };

    // "*" means current time in milliseconds
    let timestamp: u64 = if ts_str == "*" {
        TsInner::now_ms()
    } else {
        match ts_str.parse() {
            Ok(n) => n,
            Err(_) => return Some(resp_error("ERR invalid timestamp")),
        }
    };

    let value: f64 = match std::str::from_utf8(cmd[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return Some(resp_error("ERR invalid value")),
    };

    let (retention, chunk_size, dup_policy, labels) = match parse_ts_options(&cmd[4..]) {
        Ok(v) => v,
        Err(e) => return Some(e),
    };

    // Auto-create if missing, applying the options given.
    let (series, created) = match series_for_append(ctx, &key) {
        Ok(found) => found,
        Err(e) => return Some(e),
    };
    let result = {
        let mut ts = series.write();
        if created {
            ts.retention = retention;
            ts.chunk_size = chunk_size;
            for (lk, lv) in &labels {
                ts.labels.insert(lk.clone(), lv.clone());
            }
        } else if retention > 0 {
            ts.retention = retention;
        }
        if let Some(dp) = dup_policy {
            ts.duplicate_policy = dp;
        }
        ts.add_sample(timestamp, value)
    };
    if created {
        update_label_index(ctx.ts_index, &key, &ahash::AHashMap::new(), &labels);
    }

    match result {
        Ok(stored_ts) => {
            after_append(ctx, &key, &series, stored_ts);
            Some(resp_integer(stored_ts as i64))
        }
        Err(e) => Some(resp_error(e)),
    }
}

// ── TS.MADD ───────────────────────────────────────────────────────────────────
//
// TS.MADD key ts val [key ts val ...]
// Teaching: we group insertions by shard to minimise lock acquisitions.
// Since different keys can land in different shards, we can't batch them
// under one lock. We process sequentially — each triplet is one write lock.

fn ts_madd(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 || !(cmd.len() - 1).is_multiple_of(3) {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.madd' command",
        ));
    }

    // One reply per sample: its timestamp, or the error for that sample alone.
    let mut results: Vec<Vec<u8>> = Vec::with_capacity((cmd.len() - 1) / 3);
    for triple in cmd[1..].as_chunks::<3>().0 {
        let Ok(key) = std::str::from_utf8(triple[0]) else {
            results.push(resp_error("ERR invalid key"));
            continue;
        };
        let ts_str = std::str::from_utf8(triple[1]).unwrap_or("bad");
        let timestamp: u64 = if ts_str == "*" {
            TsInner::now_ms()
        } else {
            match ts_str.parse() {
                Ok(n) => n,
                Err(_) => {
                    results.push(resp_error("ERR invalid timestamp"));
                    continue;
                }
            }
        };
        let Some(value) = std::str::from_utf8(triple[2])
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
        else {
            results.push(resp_error("ERR invalid value"));
            continue;
        };

        // TS.MADD adds to series that exist. A sample for a key that is
        // not there is an error — for that sample; the others are added.
        // (It used to create the series, with default settings.)
        let existing = {
            let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
            match read_lock!(shard).get(key) {
                Some(KiviDBValue::TimeSeries(inner)) => Ok(Arc::clone(inner)),
                Some(_) => Err(resp_error(WRONGTYPE)),
                None => Err(resp_error("TSDB: the key does not exist")),
            }
        };
        let series = match existing {
            Ok(series) => series,
            Err(e) => {
                results.push(e);
                continue;
            }
        };
        let added = series.write().add_sample(timestamp, value);
        results.push(match added {
            Ok(stored_ts) => {
                after_append(ctx, key, &series, stored_ts);
                resp_integer(stored_ts as i64)
            }
            Err(e) => resp_error(e),
        });
    }

    Some(resp_array(results))
}

// ── TS.INCRBY / TS.DECRBY ────────────────────────────────────────────────────
//
// sign = 1.0 for INCRBY, -1.0 for DECRBY.
// The last sample's value is read, then incremented/decremented, then stored
// at the given (or current) timestamp.

fn ts_incrby(ctx: &mut Ctx, sign: f64) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error("ERR wrong number of arguments"));
    }

    let key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let delta: f64 = match std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(v) => v * sign,
        None => return Some(resp_error("ERR invalid value")),
    };

    // Parse optional TIMESTAMP, RETENTION, CHUNK SIZE, LABELS from cmd[3..]
    let mut explicit_ts: Option<u64> = None;
    let mut i = 3;
    while i < cmd.len() {
        let opt = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        if opt == "TIMESTAMP" {
            i += 1;
            if i >= cmd.len() {
                return Some(resp_error("ERR syntax error"));
            }
            let ts_str = std::str::from_utf8(cmd[i]).unwrap_or("bad");
            explicit_ts = Some(if ts_str == "*" {
                TsInner::now_ms()
            } else {
                match ts_str.parse() {
                    Ok(n) => n,
                    Err(_) => return Some(resp_error("ERR invalid TIMESTAMP")),
                }
            });
        }
        i += 1;
    }

    let timestamp = explicit_ts.unwrap_or_else(TsInner::now_ms);

    let (series, _) = match series_for_append(ctx, &key) {
        Ok(found) => found,
        Err(e) => return Some(e),
    };
    let added = {
        let mut ts = series.write();
        // A counter only moves forward: the new sample goes at or after the
        // latest one, building on its value (or on 0 for an empty series).
        if ts.last_ts.is_some_and(|last| timestamp < last) {
            return Some(resp_error(
                "TSDB: timestamp must be equal to or higher than the maximum existing timestamp",
            ));
        }
        let last_val = ts
            .last_ts
            .and_then(|lt| ts.samples.get(&lt))
            .copied()
            .unwrap_or(0.0);
        // Not `add_sample`: at the latest timestamp this replaces the value
        // whatever the series' duplicate policy says.
        ts.samples.insert(timestamp, last_val + delta);
        ts.last_ts = Some(timestamp);
        timestamp
    };
    after_append(ctx, &key, &series, added);
    Some(resp_integer(added as i64))
}

// ── TS.DEL ────────────────────────────────────────────────────────────────────

fn ts_del(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.del' command",
        ));
    }
    let key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let from: u64 = match std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return Some(resp_error("ERR invalid from-timestamp")),
    };
    let to: u64 = match std::str::from_utf8(cmd[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return Some(resp_error("ERR invalid to-timestamp")),
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &key);
    let map = read_lock!(shard);

    match map.get(key.as_str()) {
        Some(KiviDBValue::TimeSeries(inner)) => {
            let mut ts = inner.write();
            // BTreeMap has no retain-with-range, so we collect keys to remove first
            // Teaching: this is O(N) over the range — unavoidable for correctness
            let to_remove: Vec<u64> = ts.samples.range(from..=to).map(|(&k, _)| k).collect();
            let count = to_remove.len() as i64;
            for k in to_remove {
                ts.samples.remove(&k);
            }
            // Rebuild last_ts if it was in the deleted range
            if ts.last_ts.map(|lt| lt >= from && lt <= to).unwrap_or(false) {
                ts.last_ts = ts.samples.keys().next_back().copied();
            }
            Some(resp_integer(count))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_error("TSDB: the key does not exist")),
    }
}

// ── TS.GET ────────────────────────────────────────────────────────────────────

fn ts_get(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.get' command",
        ));
    }
    let key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k,
        Err(_) => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let latest = match cmd.get(2) {
        None => false,
        Some(arg) if arg.eq_ignore_ascii_case(b"LATEST") && cmd.len() == 3 => true,
        Some(_) => return Some(resp_error("ERR syntax error")),
    };
    let inner = match ts_query::series_at(ctx, key) {
        Ok(Some(inner)) => inner,
        Ok(None) => return Some(resp_null_bulk()),
        Err(e) => return Some(e),
    };
    // A compaction's bucket still open, with `LATEST` (#128).
    if latest && let Some((at, value)) = ts_query::open_bucket(ctx, &inner, key) {
        return Some(ts_query::sample_reply(resp, at, value));
    }
    let ts = inner.read();
    match ts.last_ts {
        Some(lt) => {
            let val = ts.samples.get(&lt).copied().unwrap_or(0.0);
            Some(ts_query::sample_reply(resp, lt, val))
        }
        None => Some(resp_array(vec![])), // empty series
    }
}

// ── TS.MGET ───────────────────────────────────────────────────────────────────
//
// TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS l1 l2...] FILTER <filters>

fn ts_mget(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let cmd = &ctx.cmd;
    let mut withlabels = false;
    let mut selected_labels: Vec<String> = Vec::new();
    let mut filters: Vec<LabelFilter> = Vec::new();
    let mut filter_started = false;
    let mut latest = false;

    let mut i = 1;
    while i < cmd.len() {
        let tok = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        match tok.as_str() {
            "LATEST" => latest = true,
            "WITHLABELS" => {
                withlabels = true;
            }
            "SELECTED_LABELS" => {
                i += 1;
                while i < cmd.len() {
                    let t = std::str::from_utf8(cmd[i])
                        .unwrap_or("")
                        .to_ascii_uppercase();
                    if t == "FILTER" {
                        break;
                    }
                    selected_labels.push(std::str::from_utf8(cmd[i]).unwrap_or("").to_string());
                    i += 1;
                }
                continue;
            }
            "FILTER" => {
                filter_started = true;
                i += 1;
                while i < cmd.len() {
                    if let Some(f) = LabelFilter::parse(std::str::from_utf8(cmd[i]).unwrap_or("")) {
                        filters.push(f);
                    }
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }

    if !filter_started || filters.is_empty() {
        return Some(resp_error("ERR FILTER is required for TS.MGET"));
    }

    let matching_keys = keys_matching_filters(ctx.store, *ctx.selected_db, ctx.ts_index, &filters);
    let mut results = Vec::new();

    for k in &matching_keys {
        // The series, and no shard held while it is read: `LATEST` reads its
        // source's as well.
        if let Ok(Some(inner)) = ts_query::series_at(ctx, k) {
            let open = if latest {
                ts_query::open_bucket(ctx, &inner, k)
            } else {
                None
            };
            let ts = inner.read();

            // Labels portion
            let labels_resp = if withlabels {
                let pairs: Vec<Vec<u8>> = ts
                    .labels
                    .iter()
                    .map(|(lk, lv)| resp_array(vec![resp_bulk_str(lk), resp_bulk_str(lv)]))
                    .collect();
                resp_array(pairs)
            } else if !selected_labels.is_empty() {
                let pairs: Vec<Vec<u8>> = selected_labels
                    .iter()
                    .filter_map(|lk| {
                        ts.labels
                            .get(lk)
                            .map(|lv| resp_array(vec![resp_bulk_str(lk), resp_bulk_str(lv)]))
                    })
                    .collect();
                resp_array(pairs)
            } else {
                resp_array(vec![])
            };

            // Latest sample: with `LATEST`, a compaction's open bucket.
            let sample_resp = match (open, ts.last_ts) {
                (Some((at, value)), _) => ts_query::sample_reply(resp, at, value),
                (None, Some(lt)) => {
                    let val = ts.samples.get(&lt).copied().unwrap_or(0.0);
                    ts_query::sample_reply(resp, lt, val)
                }
                (None, None) => resp_array(vec![]),
            };

            results.push(resp_array(vec![resp_bulk_str(k), labels_resp, sample_resp]));
        }
    }

    Some(resp_array(results))
}

// ── TS.INFO ───────────────────────────────────────────────────────────────────

fn ts_info(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.info' command",
        ));
    }
    let key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k,
        Err(_) => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(KiviDBValue::TimeSeries(inner)) => {
            let ts = inner.read();
            let mut out = Vec::new();

            macro_rules! kv {
                ($k:expr, $v:expr) => {
                    out.push(resp_bulk_str($k));
                    out.push(resp_bulk_str(&$v.to_string()));
                };
            }

            kv!("totalSamples", ts.samples.len());
            kv!("memoryUsage", ts.samples.len() * 16); // rough estimate
            kv!(
                "firstTimestamp",
                ts.samples.keys().next().copied().unwrap_or(0)
            );
            kv!("lastTimestamp", ts.last_ts.unwrap_or(0));
            kv!("retentionTime", ts.retention);
            kv!("chunkSize", ts.chunk_size);
            kv!(
                "duplicatePolicy",
                format!("{:?}", ts.duplicate_policy).to_lowercase()
            );

            // Labels
            out.push(resp_bulk_str("labels"));
            let label_pairs: Vec<Vec<u8>> = ts
                .labels
                .iter()
                .map(|(lk, lv)| resp_array(vec![resp_bulk_str(lk), resp_bulk_str(lv)]))
                .collect();
            out.push(resp_array(label_pairs));

            out.push(resp_bulk_str("sourceKey"));
            out.push(match &ts.source {
                Some(source) => resp_bulk_str(source),
                // RESP3's null to a RESP3 client: `$-1` there breaks it.
                None => crate::resp::write::resp_null_for(resp),
            });

            // Rules
            out.push(resp_bulk_str("rules"));
            let rules_resp: Vec<Vec<u8>> = ts
                .rules
                .iter()
                .map(|r| {
                    resp_array(vec![
                        resp_bulk_str(&r.dest_key),
                        resp_bulk_str(&r.bucket_duration.to_string()),
                        resp_bulk_str(&format!("{:?}", r.aggregation).to_uppercase()),
                        resp_bulk_str(&r.align_timestamp.to_string()),
                    ])
                })
                .collect();
            out.push(resp_array(rules_resp));

            Some(resp_array(out))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_error("TSDB: the key does not exist")),
    }
}

// ── TS.QUERYINDEX ─────────────────────────────────────────────────────────────

fn ts_queryindex(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error("ERR FILTER is required for TS.QUERYINDEX"));
    }

    let filters: Vec<LabelFilter> = cmd[1..]
        .iter()
        .filter_map(|t| LabelFilter::parse(std::str::from_utf8(t).unwrap_or("")))
        .collect();

    if filters.is_empty() {
        return Some(resp_error("ERR FILTER is required for TS.QUERYINDEX"));
    }

    let mut keys = keys_matching_filters(ctx.store, *ctx.selected_db, ctx.ts_index, &filters);
    keys.sort();

    let items: Vec<Vec<u8>> = keys.iter().map(|k| resp_bulk_str(k)).collect();
    Some(resp_array(items))
}

// ── TS.CREATERULE ─────────────────────────────────────────────────────────────
//
// TS.CREATERULE sourceKey destKey AGGREGATION <agg> bucketDuration [alignTimestamp]

fn ts_createrule(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 6 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.createrule' command",
        ));
    }
    let src_key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR invalid source key")),
    };
    let dest_key = match std::str::from_utf8(cmd[2]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR invalid dest key")),
    };
    // cmd[3] is "AGGREGATION"
    let agg = match TsAggregation::parse(std::str::from_utf8(cmd[4]).unwrap_or("")) {
        Some(a) => a,
        None => return Some(resp_error("ERR invalid AGGREGATION")),
    };
    if !cmd[3].eq_ignore_ascii_case(b"AGGREGATION") {
        return Some(resp_error("ERR syntax error"));
    }
    // Zero would be a division by zero on the next sample written.
    let bucket_duration: u64 = match std::str::from_utf8(cmd[5])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(b) if b > 0 => b,
        _ => return Some(resp_error("TSDB: Couldn't parse bucketDuration")),
    };
    if src_key == dest_key {
        return Some(resp_error(
            "TSDB: the source key and destination key should be different",
        ));
    }
    let dest = match ts_query::series_at(ctx, &dest_key) {
        Ok(Some(dest)) => dest,
        Ok(None) => return Some(resp_error("TSDB: the key does not exist")),
        Err(e) => return Some(e),
    };
    // As RedisTimeSeries: a compaction has one source.
    if dest.read().source.is_some() {
        return Some(resp_error(
            "TSDB: the destination key already has a src rule",
        ));
    }
    let align: u64 = cmd
        .get(6)
        .and_then(|t| std::str::from_utf8(t).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let rule = CompactionRule {
        dest_key,
        aggregation: agg,
        bucket_duration,
        align_timestamp: align,
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &src_key);
    let map = write_lock!(shard);

    match map.get(src_key.as_str()) {
        Some(KiviDBValue::TimeSeries(inner)) => {
            let mut ts = inner.write();
            // Disallow duplicate destination rules
            if ts.rules.iter().any(|r| r.dest_key == rule.dest_key) {
                return Some(resp_error("TSDB: compaction rule already exists"));
            }
            ts.rules.push(rule);
            drop(ts);
            drop(map);
            dest.write().source = Some(src_key);
            Some(resp_simple("OK"))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_error("TSDB: the key does not exist")),
    }
}

// ── TS.DELETERULE ─────────────────────────────────────────────────────────────

fn ts_deleterule(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.deleterule' command",
        ));
    }
    let src_key = match std::str::from_utf8(cmd[1]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR invalid source key")),
    };
    let dest_key = match std::str::from_utf8(cmd[2]) {
        Ok(k) => k.to_string(),
        Err(_) => return Some(resp_error("ERR invalid dest key")),
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &src_key);
    let map = write_lock!(shard);

    match map.get(src_key.as_str()) {
        Some(KiviDBValue::TimeSeries(inner)) => {
            let mut ts = inner.write();
            let before = ts.rules.len();
            ts.rules.retain(|r| r.dest_key != dest_key);
            if ts.rules.len() == before {
                Some(resp_error("TSDB: compaction rule does not exist"))
            } else {
                drop(ts);
                drop(map);
                if let Ok(Some(dest)) = ts_query::series_at(ctx, &dest_key) {
                    let mut dest = dest.write();
                    if dest.source.as_deref() == Some(src_key.as_str()) {
                        dest.source = None;
                    }
                }
                Some(resp_simple("OK"))
            }
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_error("TSDB: the key does not exist")),
    }
}
