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
    resp_array, resp_bulk, resp_bulk_str, resp_error, resp_integer, resp_null_bulk, resp_simple,
};
use crate::store::ValueRef;
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

/// The command whose options are read: a few are one command's own.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OptionsOf {
    Create,
    Alter,
    Add,
    IncrBy,
}

/// The options of `TS.CREATE`, `TS.ALTER`, `TS.ADD`, `TS.INCRBY` and
/// `TS.DECRBY`, each `None` when not given. `TS.ALTER` changes only what was
/// given: `RETENTION 0`, `CHUNK_SIZE 4096` and a `LABELS` with no pairs too
/// (#565), where a value equal to the default used to count as not given.
#[derive(Default)]
struct TsOptions {
    retention: Option<u64>,
    chunk_size: Option<usize>,
    /// `DUPLICATE_POLICY` (and KiviDB's `ON DUPLICATE`): the series' policy.
    duplicate_policy: Option<DuplicatePolicy>,
    /// `ON_DUPLICATE` (`TS.ADD`): the policy for this one sample, as in
    /// RedisTimeSeries. The series keeps its own.
    on_duplicate: Option<DuplicatePolicy>,
    labels: Option<Vec<(String, String)>>,
    /// `TIMESTAMP` (`TS.INCRBY`, `TS.DECRBY`): the time the last one gives
    /// (`*`, the clock).
    timestamp: Option<u64>,
    /// Where each `TIMESTAMP` option is among the options.
    timestamp_at: Vec<usize>,
}

/// RedisTimeSeries's error for a `CHUNK_SIZE` out of its bounds.
const CHUNK_SIZE_RANGE: &str =
    "TSDB: CHUNK_SIZE value must be a multiple of 8 in the range [48 .. 1048576]";

/// The word after an option's name: its value.
fn option_value<'t>(tokens: &[&'t [u8]], at: usize) -> Result<&'t [u8], Vec<u8>> {
    tokens
        .get(at)
        .copied()
        .ok_or_else(|| resp_error("ERR syntax error"))
}

fn option_number(value: &[u8]) -> Option<u64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

/// A sample's time, as a command gives it (`*` is the caller's to read).
///
/// From a client, `0` to `i64::MAX`, as RedisTimeSeries, which reads it as
/// a `long long`: a time above that was taken, and then replied, sent to
/// replicas and logged as a negative number, which they refused: the
/// sample lost there (#592). From the AOF or a master, any `u64`, which an
/// older version took and kept.
fn sample_time(word: &[u8], strict: bool) -> Option<u64> {
    let text = std::str::from_utf8(word).ok()?;
    if strict {
        text.parse::<i64>().ok().and_then(|t| u64::try_from(t).ok())
    } else {
        text.parse().ok()
    }
}

/// A chunk size: as RedisTimeSeries, a multiple of 8 from 48 to 1048576.
/// Not checked when `strict` is off: 1.2.0 took any.
fn option_chunk_size(value: &[u8], strict: bool) -> Result<usize, Vec<u8>> {
    let size = std::str::from_utf8(value)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| resp_error("TSDB: Couldn't parse CHUNK_SIZE"))?;
    if strict && (!(48..=1_048_576).contains(&size) || !size.is_multiple_of(8)) {
        return Err(resp_error(CHUNK_SIZE_RANGE));
    }
    Ok(size)
}

fn option_policy(value: &[u8]) -> Result<DuplicatePolicy, Vec<u8>> {
    DuplicatePolicy::parse(std::str::from_utf8(value).unwrap_or(""))
        .ok_or_else(|| resp_error("ERR invalid DUPLICATE POLICY"))
}

/// Whether `tokens[at]` starts an option, which ends a `LABELS` list: its
/// name, or the first of `CHUNK SIZE`, `ON DUPLICATE` and `DUPLICATE
/// POLICY` followed by the second (1.2.0 ended the list at the first word
/// alone).
///
/// Not RedisTimeSeries's way: it takes every word after `LABELS` as label
/// pairs and finds options anywhere, so its `LABELS a b RETENTION 5` has the
/// labels `a=b` and `RETENTION=5`, and a retention of 5. Here the list ends
/// at `RETENTION`, as in 1.2.0, which a log 1.2.0 wrote replays by.
fn ends_labels(tokens: &[&[u8]], at: usize, of: OptionsOf) -> bool {
    const NAMES: [&[u8]; 6] = [
        b"RETENTION",
        b"CHUNK_SIZE",
        b"ENCODING",
        b"DUPLICATE_POLICY",
        b"ON_DUPLICATE",
        b"IGNORE",
    ];
    let word = tokens[at];
    NAMES.iter().any(|name| word.eq_ignore_ascii_case(name))
        || (word.eq_ignore_ascii_case(b"CHUNK") && second_word(tokens, at, b"SIZE"))
        || (word.eq_ignore_ascii_case(b"ON") && second_word(tokens, at, b"DUPLICATE"))
        || (word.eq_ignore_ascii_case(b"DUPLICATE") && second_word(tokens, at, b"POLICY"))
        || (of == OptionsOf::IncrBy && word.eq_ignore_ascii_case(b"TIMESTAMP"))
}

/// Whether the checks 1.2.0 did not make apply: `CHUNK_SIZE`'s bounds, an
/// `ENCODING` value, `IGNORE`. Not to a command from a master, or replayed
/// from the AOF: one an older version accepted is applied, rather than
/// refused and the series it made lost.
fn strict_options(ctx: &Ctx) -> bool {
    !(ctx.apply_replication || crate::expiry::replaying())
}

/// Whether the word after `tokens[at]` is `second`: `CHUNK SIZE`, `ON
/// DUPLICATE`, `DUPLICATE POLICY`. Not followed by it, the first word is
/// one RedisTimeSeries does not know, and passed over.
fn second_word(tokens: &[&[u8]], at: usize, second: &[u8]) -> bool {
    tokens
        .get(at + 1)
        .is_some_and(|word| word.eq_ignore_ascii_case(second))
}

/// Parse the options of a TS command.
///
/// The spellings are RedisTimeSeries's: `RETENTION`, `CHUNK_SIZE`,
/// `ENCODING`, `UNCOMPRESSED`, `COMPRESSED`, `DUPLICATE_POLICY`,
/// `ON_DUPLICATE` (`TS.ADD`), `TIMESTAMP` (`TS.INCRBY`, `TS.DECRBY`) and
/// `LABELS`; and KiviDB's own of 1.2.0, `CHUNK SIZE`, `ON DUPLICATE` and
/// `DUPLICATE POLICY`. A word that is none of these is passed over and the
/// parse goes on, as RedisTimeSeries, which looks for each option by name,
/// passes it over. It used to end the parse, with no error, dropping the
/// options after it: `CHUNK_SIZE`, unknown then, dropped `LABELS` (#565).
/// `IGNORE` is not supported, and says so. Where a `LABELS` list ends is
/// not as in RedisTimeSeries: see [`ends_labels`].
fn parse_ts_options(tokens: &[&[u8]], of: OptionsOf, strict: bool) -> Result<TsOptions, Vec<u8>> {
    let mut options = TsOptions::default();
    let mut i = 0;
    while i < tokens.len() {
        let word = tokens[i].to_ascii_uppercase();
        match word.as_slice() {
            b"RETENTION" => {
                let value = option_value(tokens, i + 1)?;
                options.retention = Some(
                    option_number(value)
                        .ok_or_else(|| resp_error("ERR invalid RETENTION value"))?,
                );
                i += 2;
            }
            b"CHUNK_SIZE" => {
                options.chunk_size = Some(option_chunk_size(option_value(tokens, i + 1)?, strict)?);
                i += 2;
            }
            // `CHUNK SIZE n`, `ON DUPLICATE policy` and `DUPLICATE POLICY
            // policy`: KiviDB's spellings, kept. 1.2.0 took any second word.
            b"CHUNK" if second_word(tokens, i, b"SIZE") => {
                options.chunk_size = Some(option_chunk_size(option_value(tokens, i + 2)?, strict)?);
                i += 3;
            }
            b"ON" | b"DUPLICATE"
                if second_word(
                    tokens,
                    i,
                    if word == b"ON" {
                        &b"DUPLICATE"[..]
                    } else {
                        &b"POLICY"[..]
                    },
                ) =>
            {
                options.duplicate_policy = Some(option_policy(option_value(tokens, i + 2)?)?);
                i += 3;
            }
            b"DUPLICATE_POLICY" => {
                options.duplicate_policy = Some(option_policy(option_value(tokens, i + 1)?)?);
                i += 2;
            }
            b"ON_DUPLICATE" if of == OptionsOf::Add => {
                options.on_duplicate = Some(option_policy(option_value(tokens, i + 1)?)?);
                i += 2;
            }
            // Accepted: KiviDB stores every series one way.
            b"ENCODING" => {
                let value = option_value(tokens, i + 1)?;
                if strict
                    && !value.eq_ignore_ascii_case(b"COMPRESSED")
                    && !value.eq_ignore_ascii_case(b"UNCOMPRESSED")
                {
                    return Err(resp_error("TSDB: unknown ENCODING parameter"));
                }
                i += 2;
            }
            b"UNCOMPRESSED" | b"COMPRESSED" => i += 1,
            b"TIMESTAMP" if of == OptionsOf::IncrBy => {
                let value = option_value(tokens, i + 1)?;
                let at = if value == b"*" {
                    TsInner::now_ms()
                } else {
                    sample_time(value, strict).ok_or_else(|| resp_error("ERR invalid TIMESTAMP"))?
                };
                options.timestamp = Some(at);
                options.timestamp_at.push(i);
                i += 2;
            }
            b"LABELS" => {
                i += 1;
                let mut labels = Vec::new();
                // Pairs, up to the next option.
                while i + 1 < tokens.len() && !ends_labels(tokens, i, of) {
                    let text = |t: &[u8]| std::str::from_utf8(t).unwrap_or("").to_string();
                    labels.push((text(tokens[i]), text(tokens[i + 1])));
                    i += 2;
                }
                options.labels = Some(labels);
            }
            b"IGNORE" if strict => {
                return Err(resp_error("TSDB: IGNORE is not supported"));
            }
            // Passed over, as RedisTimeSeries does.
            _ => i += 1,
        }
    }
    Ok(options)
}

/// A series made by a command: the options given, the defaults otherwise.
fn configure_new(ts: &mut TsInner, options: &TsOptions) {
    if let Some(retention) = options.retention {
        ts.retention = retention;
    }
    if let Some(chunk_size) = options.chunk_size {
        ts.chunk_size = chunk_size;
    }
    if let Some(policy) = &options.duplicate_policy {
        ts.duplicate_policy = policy.clone();
    }
    for (lk, lv) in options.labels.iter().flatten() {
        ts.labels.insert(lk.clone(), lv.clone());
    }
}

/// The word 1.2.1 puts right after the value of every `TS.INCRBY` and
/// `TS.DECRBY` it sends to its replicas and its AOF, with the time the
/// command used: `TIMESTAMP` in lower case. It tells such a record from one
/// 1.2.0 sent, which must be read as 1.2.0 read it. 1.2.0 took every
/// `TIMESTAMP` out, in any case, and added `TIMESTAMP` (upper case) at the
/// end; so a record of 1.2.0 never has this word at that place: either
/// another word is there, or, with nothing else to send, the `TIMESTAMP` it
/// added, in upper case. Read as an option, it is `TIMESTAMP`, by any
/// version.
pub(crate) const INCRBY_MARK: &[u8] = b"timestamp";

/// A `TS.INCRBY` or `TS.DECRBY` (its options, `tokens`) as 1.2.0 sent it to
/// its replicas and its AOF, read as 1.2.0 read it: its time, the one at
/// the end, and no other option. 1.2.0 read no option but `TIMESTAMP`,
/// made a series with the defaults, and did not trim to the retention.
/// Read as 1.2.1 reads a command, the record's options were applied, and
/// the series trimmed, where 1.2.0 kept the samples: data lost on replay,
/// and a replica of 1.2.0 not the same as its master. And the end could be
/// taken by what came before it (`LABELS a`, or a `RETENTION` with no
/// value, took `TIMESTAMP`): the sample at the replay's time, or the record
/// refused. `None` for a record that does not end in `TIMESTAMP <time>`,
/// which 1.2.0 did not send.
fn written_by_1_2_0(tokens: &[&[u8]]) -> Option<TsOptions> {
    let [.., word, at] = tokens else {
        return None;
    };
    if !word.eq_ignore_ascii_case(b"TIMESTAMP") {
        return None;
    }
    // 1.2.0 sent the time it replied, as an `i64`: one above `i64::MAX` as
    // a negative number. It is the same 64 bits as the time it kept (#592).
    let at = std::str::from_utf8(at).ok()?;
    let at = at
        .parse::<u64>()
        .ok()
        .or_else(|| at.parse::<i64>().ok().map(|t| t as u64))?;
    Some(TsOptions {
        timestamp: Some(at),
        ..Default::default()
    })
}

/// Where a `TS.INCRBY` or `TS.DECRBY` (the whole command) has its
/// `TIMESTAMP` options, as the command reads them: for the replication
/// feed, which gives each the timestamp the command used. A `TIMESTAMP`
/// that is a label's value is not one.
pub(crate) fn incrby_timestamp_options(cmd: &[&[u8]]) -> Vec<usize> {
    match cmd
        .get(3..)
        .map(|tokens| parse_ts_options(tokens, OptionsOf::IncrBy, false))
    {
        Some(Ok(options)) => options.timestamp_at.iter().map(|at| 3 + at).collect(),
        _ => Vec::new(),
    }
}

/// Update the label index for a key. Clears old labels for this key and
/// inserts the new ones. Takes a write lock on the index.
fn update_label_index(
    ts_index: &SharedTsIndex,
    key: &[u8],
    old_labels: &ahash::AHashMap<String, String>,
    new_labels: &[(String, String)],
) {
    if old_labels.is_empty() && new_labels.is_empty() {
        return;
    }
    let mut idx = write_lock!(ts_index);
    // The index holds names (`key_bytes::text_of`), as its rebuild does.
    let name = crate::key_bytes::text_of(key);
    let key: &str = &name;

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
                    if matches!(v, ValueRef::TimeSeries(_)) {
                        all.push(crate::key_bytes::text_of(k).into_owned());
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
            // A name: the key is its bytes_of.
            let raw = crate::key_bytes::bytes_of(k);
            let shard = store::get_shard(db, &*raw);
            let map = read_lock!(shard);
            if let Some(ValueRef::TimeSeries(inner)) = map.get(&*raw) {
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
fn series_for_append(ctx: &Ctx, key: &[u8]) -> Result<(Series, bool), Vec<u8>> {
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    match map.get(key) {
        Some(ValueRef::TimeSeries(inner)) => Ok((Arc::clone(inner), false)),
        Some(_) => Err(resp_error(WRONGTYPE)),
        None => {
            let mut fresh = TsInner::new();
            fresh.duplicate_policy = DuplicatePolicy::server_default();
            let series: Series = Arc::new(RwLock::new(fresh));
            map.insert(key, KiviDBValue::TimeSeries(Arc::clone(&series)));
            Ok((series, true))
        }
    }
}

/// Whether the series `source` has a compaction rule to `dest` (both names).
fn feeds(ctx: &Ctx, source: &str, dest: &str) -> bool {
    ts_query::series_at(ctx, &crate::key_bytes::bytes_of(source))
        .ok()
        .flatten()
        .is_some_and(|series| series.read().rules.iter().any(|r| r.dest_key == dest))
}

/// Whether the series `dest` names `source` as the series it is a
/// compaction of (both names).
fn fed_by(ctx: &Ctx, dest: &str, source: &str) -> bool {
    ts_query::series_at(ctx, &crate::key_bytes::bytes_of(dest))
        .ok()
        .flatten()
        .is_some_and(|series| series.read().source.as_deref() == Some(source))
}

/// A series' links that hold, as RedisTimeSeries checks them where they are
/// used: its source, if that series has the rule back, and the destinations
/// of its rules that name it back. A link is left behind by a `DEL`, a
/// `RESTORE … REPLACE` or a `MIGRATE` of the other series (#593).
fn live_links(ctx: &Ctx, key: &[u8], series: &Series) -> (Option<String>, Vec<String>) {
    let name = crate::key_bytes::text_of(key).into_owned();
    let (source, dests): (Option<String>, Vec<String>) = {
        let series = series.read();
        let dests = series.rules.iter().map(|r| r.dest_key.clone()).collect();
        (series.source.clone(), dests)
    };
    let source = source.filter(|source| feeds(ctx, source, &name));
    let dests = dests
        .into_iter()
        .filter(|dest| fed_by(ctx, dest, &name))
        .collect();
    (source, dests)
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
fn after_append(ctx: &Ctx, key: &[u8], series: &Series, ts: u64) {
    let updates: Vec<(String, u64, f64)> = {
        let source = series.read();
        let Some(&newest) = source.samples.keys().next_back() else {
            return;
        };
        source
            .rules
            .iter()
            .filter(|rule| {
                rule.bucket_duration > 0 && *rule.dest_key != *crate::key_bytes::text_of(key)
            })
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
    // A rule whose destination is gone, or names another source (it was
    // replaced, by `RESTORE … REPLACE` among others), is dead: dropped, as
    // RedisTimeSeries drops it on an append, and the same on a replica and
    // in a replay, which run this append on the same series (#593).
    let name = crate::key_bytes::text_of(key);
    let mut dead: Vec<String> = Vec::new();
    for (rule_dest, bucket, value) in updates {
        // A rule names its destination (`key_bytes::text_of`).
        let dest_key = crate::key_bytes::bytes_of(&rule_dest);
        let dest = {
            let shard = store::get_shard(&ctx.store[*ctx.selected_db], &*dest_key);
            match read_lock!(shard).get(&*dest_key) {
                Some(ValueRef::TimeSeries(inner)) => Arc::clone(inner),
                _ => {
                    dead.push(rule_dest);
                    continue;
                }
            }
        };
        if dest.read().source.as_deref() != Some(&*name) {
            dead.push(rule_dest);
            continue;
        }
        {
            let mut dest = dest.write();
            dest.samples.insert(bucket, value);
            if dest.last_ts.is_none_or(|last| bucket > last) {
                dest.last_ts = Some(bucket);
            }
        }
        ctx.wait_table.notify_signal(&dest_key);
    }
    if !dead.is_empty() {
        series.write().rules.retain(|rule| !dead.contains(&rule.dest_key));
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
    let strict = strict_options(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.create' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();

    let options = match parse_ts_options(&cmd[2..], OptionsOf::Create, strict) {
        Ok(v) => v,
        Err(e) => return Some(e),
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &key);
    let mut map = write_lock!(shard);

    if map.contains_key(key.as_slice()) {
        return Some(resp_error("TSDB: key already exists"));
    }

    let mut ts = TsInner::new();
    ts.duplicate_policy = DuplicatePolicy::server_default();
    configure_new(&mut ts, &options);

    map.insert(&*key, KiviDBValue::TimeSeries(Arc::new(RwLock::new(ts))));
    drop(map);

    let labels = options.labels.unwrap_or_default();
    update_label_index(ctx.ts_index, &key, &ahash::AHashMap::new(), &labels);
    Some(resp_simple("OK"))
}

// ── TS.ALTER ──────────────────────────────────────────────────────────────────

fn ts_alter(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let strict = strict_options(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.alter' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();

    let options = match parse_ts_options(&cmd[2..], OptionsOf::Alter, strict) {
        Ok(v) => v,
        Err(e) => return Some(e),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = write_lock!(shard);

    match map.get(key.as_slice()) {
        Some(ValueRef::TimeSeries(inner)) => {
            let mut ts = inner.write();

            // What was given, whatever the value: `RETENTION 0` takes the
            // retention away, `LABELS` alone the labels (#565).
            if let Some(retention) = options.retention {
                ts.retention = retention;
            }
            if let Some(chunk_size) = options.chunk_size {
                ts.chunk_size = chunk_size;
            }
            if let Some(dp) = options.duplicate_policy {
                ts.duplicate_policy = dp;
            }

            if let Some(new_labels) = options.labels {
                let old_labels = std::mem::take(&mut ts.labels);
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
    let strict = strict_options(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.add' command",
        ));
    }

    let key: Vec<u8> = cmd[1].to_vec();

    // "*" means current time in milliseconds
    let timestamp: u64 = if cmd[2] == b"*" {
        TsInner::now_ms()
    } else {
        match sample_time(cmd[2], strict) {
            Some(n) => n,
            None => return Some(resp_error("ERR invalid timestamp")),
        }
    };

    let value: f64 = match std::str::from_utf8(cmd[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return Some(resp_error("ERR invalid value")),
    };

    let options = match parse_ts_options(&cmd[4..], OptionsOf::Add, strict) {
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
        // The policy the sample is added under: `ON_DUPLICATE`'s, or the
        // series' as the options leave it. A sample it refuses fails the
        // command before anything changes: a failed command is not sent to
        // replicas or the AOF, so the options must not stay applied.
        let policy = options
            .on_duplicate
            .clone()
            .or_else(|| options.duplicate_policy.clone())
            .unwrap_or_else(|| ts.duplicate_policy.clone());
        if policy == DuplicatePolicy::Block && ts.samples.contains_key(&timestamp) {
            return Some(resp_error(crate::types::BLOCKED_DUPLICATE));
        }
        if created {
            configure_new(&mut ts, &options);
        } else {
            // As 1.2.0 did, which a log it wrote replays by: a retention, and
            // a duplicate policy (`DUPLICATE_POLICY`, `ON DUPLICATE`), given
            // to an existing series are its own from now on. RedisTimeSeries
            // passes them over there.
            if let Some(retention) = options.retention.filter(|&r| r > 0) {
                ts.retention = retention;
            }
            if let Some(dp) = &options.duplicate_policy {
                ts.duplicate_policy = dp.clone();
            }
        }
        ts.add_sample_as(timestamp, value, &policy)
    };
    if created {
        let labels = options.labels.unwrap_or_default();
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
    let strict = strict_options(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 4 || !(cmd.len() - 1).is_multiple_of(3) {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.madd' command",
        ));
    }

    // One reply per sample: its timestamp, or the error for that sample alone.
    let mut results: Vec<Vec<u8>> = Vec::with_capacity((cmd.len() - 1) / 3);
    for triple in cmd[1..].as_chunks::<3>().0 {
        let key: &[u8] = triple[0];
        let timestamp: u64 = if triple[1] == b"*" {
            TsInner::now_ms()
        } else {
            match sample_time(triple[1], strict) {
                Some(n) => n,
                None => {
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
                Some(ValueRef::TimeSeries(inner)) => Ok(Arc::clone(inner)),
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
    let strict = strict_options(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error("ERR wrong number of arguments"));
    }

    let key: Vec<u8> = cmd[1].to_vec();

    let delta: f64 = match std::str::from_utf8(cmd[2])
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(v) => v * sign,
        None => return Some(resp_error("ERR invalid value")),
    };

    // TIMESTAMP, and the options a series it creates is made with, as in
    // RedisTimeSeries: RETENTION, CHUNK_SIZE, DUPLICATE_POLICY, LABELS. They
    // were passed over, and the series made with the defaults (#555).
    //
    // From the AOF or a master, a record without 1.2.1's mark is one 1.2.0
    // sent, and is read as 1.2.0 read it (`written_by_1_2_0`).
    let of_1_2_0 = if !strict && cmd.get(3).is_none_or(|word| *word != INCRBY_MARK) {
        written_by_1_2_0(&cmd[3..])
    } else {
        None
    };
    let trim = of_1_2_0.is_none();
    let options = match of_1_2_0 {
        Some(v) => v,
        None => match parse_ts_options(&cmd[3..], OptionsOf::IncrBy, strict) {
            Ok(v) => v,
            Err(e) => return Some(e),
        },
    };
    let timestamp = options.timestamp.unwrap_or_else(TsInner::now_ms);

    let (series, created) = match series_for_append(ctx, &key) {
        Ok(found) => found,
        Err(e) => return Some(e),
    };
    let added = {
        let mut ts = series.write();
        if created {
            configure_new(&mut ts, &options);
        }
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
        // `LAST` whatever the series' duplicate policy: at the latest
        // timestamp this replaces the value. Through `add_sample_as`, as
        // `TS.ADD`, so the retention trims the series (#555); not for a
        // record of 1.2.0, which did not trim.
        if trim {
            ts.add_sample_as(timestamp, last_val + delta, &DuplicatePolicy::Last)
        } else {
            ts.samples.insert(timestamp, last_val + delta);
            ts.last_ts = Some(timestamp);
            Ok(timestamp)
        }
    };
    if created {
        let labels = options.labels.unwrap_or_default();
        update_label_index(ctx.ts_index, &key, &ahash::AHashMap::new(), &labels);
    }
    match added {
        Ok(stored_ts) => {
            after_append(ctx, &key, &series, stored_ts);
            Some(resp_integer(stored_ts as i64))
        }
        Err(e) => Some(resp_error(e)),
    }
}

// ── TS.DEL ────────────────────────────────────────────────────────────────────

fn ts_del(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'ts.del' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();
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

    match map.get(key.as_slice()) {
        Some(ValueRef::TimeSeries(inner)) => {
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
    let key: &[u8] = cmd[1];

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
    if latest
        && let Some((at, value)) =
            ts_query::open_bucket(ctx, &inner, &crate::key_bytes::text_of(key))
    {
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
        if let Ok(Some(inner)) = ts_query::series_at(ctx, &crate::key_bytes::bytes_of(k)) {
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

            results.push(resp_array(vec![
                resp_bulk(&crate::key_bytes::bytes_of(k)),
                labels_resp,
                sample_resp,
            ]));
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
    let key: &[u8] = cmd[1];

    // Its links that hold, read before the series is, with no lock held.
    let (live_source, live_dests) = match ts_query::series_at(ctx, key) {
        Ok(Some(series)) => live_links(ctx, key, &series),
        _ => (None, Vec::new()),
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::TimeSeries(inner)) => {
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
            out.push(match &live_source {
                Some(source) => resp_bulk(&crate::key_bytes::bytes_of(source)),
                // RESP3's null to a RESP3 client: `$-1` there breaks it.
                None => crate::resp::write::resp_null_for(resp),
            });

            // Rules
            out.push(resp_bulk_str("rules"));
            let rules_resp: Vec<Vec<u8>> = ts
                .rules
                .iter()
                .filter(|r| live_dests.contains(&r.dest_key))
                .map(|r| {
                    resp_array(vec![
                        resp_bulk(&crate::key_bytes::bytes_of(&r.dest_key)),
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

    // Names: the keys are their bytes_of, sorted as bytes.
    let mut keys: Vec<Vec<u8>> =
        keys_matching_filters(ctx.store, *ctx.selected_db, ctx.ts_index, &filters)
            .iter()
            .map(|name| crate::key_bytes::bytes_of(name).into_owned())
            .collect();
    keys.sort();

    let items: Vec<Vec<u8>> = keys.iter().map(|k| resp_bulk(k)).collect();
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
    let src_key: Vec<u8> = cmd[1].to_vec();
    let dest_key: Vec<u8> = cmd[2].to_vec();
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
    let dest = match ts_query::series_at(ctx, dest_key.as_slice()) {
        Ok(Some(dest)) => dest,
        Ok(None) => return Some(resp_error("TSDB: the key does not exist")),
        Err(e) => return Some(e),
    };
    // As RedisTimeSeries: a compaction has one source, one that still has
    // the rule to it.
    let source = dest.read().source.clone();
    if source.is_some_and(|source| {
        feeds(
            ctx,
            &source,
            &crate::key_bytes::text_of(dest_key.as_slice()),
        )
    }) {
        return Some(resp_error(
            "TSDB: the destination key already has a src rule",
        ));
    }
    let align: u64 = cmd
        .get(6)
        .and_then(|t| std::str::from_utf8(t).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Names (`key_bytes::text_of`), as a snapshot keeps them.
    let rule = CompactionRule {
        dest_key: crate::key_bytes::text_of(dest_key.as_slice()).into_owned(),
        aggregation: agg,
        bucket_duration,
        align_timestamp: align,
    };

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &src_key);
    let map = write_lock!(shard);

    match map.get(src_key.as_slice()) {
        Some(ValueRef::TimeSeries(inner)) => {
            let mut ts = inner.write();
            // Disallow duplicate destination rules
            if ts.rules.iter().any(|r| r.dest_key == rule.dest_key) {
                return Some(resp_error("TSDB: compaction rule already exists"));
            }
            ts.rules.push(rule);
            drop(ts);
            drop(map);
            dest.write().source = Some(crate::key_bytes::text_of(src_key.as_slice()).into_owned());
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
    let src_key: Vec<u8> = cmd[1].to_vec();
    let dest_key: Vec<u8> = cmd[2].to_vec();

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &src_key);
    let map = write_lock!(shard);

    match map.get(src_key.as_slice()) {
        Some(ValueRef::TimeSeries(inner)) => {
            let mut ts = inner.write();
            let before = ts.rules.len();
            let dest_name = crate::key_bytes::text_of(dest_key.as_slice());
            ts.rules.retain(|r| *r.dest_key != *dest_name);
            if ts.rules.len() == before {
                Some(resp_error("TSDB: compaction rule does not exist"))
            } else {
                drop(ts);
                drop(map);
                if let Ok(Some(dest)) = ts_query::series_at(ctx, dest_key.as_slice()) {
                    let mut dest = dest.write();
                    if dest.source.as_deref()
                        == Some(&*crate::key_bytes::text_of(src_key.as_slice()))
                    {
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
