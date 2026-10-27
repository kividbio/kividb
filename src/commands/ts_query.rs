//! Time-series range queries: the engine behind `TS.RANGE`, `TS.REVRANGE`,
//! `TS.MRANGE`, `TS.MREVRANGE`, `TS.NRANGE`, `TS.NREVRANGE` and `TS.READ`.
//!
//! One implementation of filtering, bucketing and aggregation, so the six
//! commands cannot disagree about what a bucket is.

use crate::commands::blocking::{self, BlockState, ParkOutcome};
use crate::commands::ctx::Ctx;
use crate::commands::ts::{LabelFilter, keys_matching_filters};
use crate::read_lock;
use crate::resp::write::*;
use crate::store;
use crate::types::{KiviDBValue, TsAggregation, TsInner};
use ahash::AHashMap;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NO_KEY: &str = "TSDB: the key does not exist";

/// `EMPTY` can ask for a bucket per millisecond across years. Past this many
/// buckets in one series the query is refused rather than attempted.
const MAX_EMPTY_BUCKETS: u128 = 1_000_000;

/// One row of a result: a timestamp and one value per aggregator (a single
/// value when the query does not aggregate).
pub(super) type Row = (u64, Vec<f64>);

// ── Options ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub(super) enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid,
}

#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub(super) enum Align {
    /// Buckets start at multiples of the bucket duration.
    #[default]
    Zero,
    /// Aligned to the query's `fromTimestamp`.
    Start,
    /// Aligned to the query's `toTimestamp`.
    End,
    At(u64),
}

/// The options every range command shares.
#[derive(Default)]
pub(super) struct RangeOptions {
    pub filter_ts: Option<Vec<u64>>,
    pub filter_value: Option<(f64, f64)>,
    pub count: Option<usize>,
    pub align: Align,
    /// One list of aggregators per series the command names: exactly one list
    /// for `TS.RANGE` / `TS.MRANGE`, one per key for `TS.NRANGE`. Empty when
    /// the query does not aggregate.
    pub aggregators: Vec<Vec<TsAggregation>>,
    pub bucket: u64,
    pub bucket_ts: BucketTimestamp,
    pub empty: bool,
    /// `LATEST`: a compaction's bucket still open is reported too.
    pub latest: bool,
}

fn text(arg: &[u8]) -> &str {
    std::str::from_utf8(arg).unwrap_or("")
}

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

/// A comma-separated aggregator list: `avg`, or `min,avg,max`.
fn parse_aggregators(arg: &[u8]) -> Result<Vec<TsAggregation>, Vec<u8>> {
    let list: Option<Vec<TsAggregation>> = text(arg).split(',').map(TsAggregation::parse).collect();
    match list {
        Some(list) if !list.is_empty() => Ok(list),
        _ => Err(resp_error("TSDB: Unknown aggregation type")),
    }
}

impl RangeOptions {
    /// Consume one range option starting at `tokens[i]`, if that is what is
    /// there. `Ok(Some(next))` — consumed, continue at `next`; `Ok(None)` —
    /// not a range option, the caller's to handle.
    ///
    /// `agg_lists` is how many aggregator arguments `AGGREGATION` takes.
    pub fn parse_at(
        &mut self,
        tokens: &[&[u8]],
        i: usize,
        agg_lists: usize,
    ) -> Result<Option<usize>, Vec<u8>> {
        let tok = tokens[i];
        let arg = |n: usize| tokens.get(i + n).copied();
        let syntax = || resp_error("ERR syntax error");

        if is(tok, "LATEST") {
            self.latest = true;
            return Ok(Some(i + 1));
        }
        if is(tok, "EMPTY") {
            self.empty = true;
            return Ok(Some(i + 1));
        }
        if is(tok, "FILTER_BY_TS") {
            let mut list = Vec::new();
            let mut j = i + 1;
            while let Some(ts) = tokens.get(j).and_then(|t| text(t).parse::<u64>().ok()) {
                list.push(ts);
                j += 1;
            }
            if list.is_empty() {
                return Err(resp_error(
                    "TSDB: FILTER_BY_TS one or more arguments are missing",
                ));
            }
            self.filter_ts = Some(list);
            return Ok(Some(j));
        }
        if is(tok, "FILTER_BY_VALUE") {
            let bound = |n: usize| arg(n).and_then(|a| text(a).parse::<f64>().ok());
            return match (bound(1), bound(2)) {
                (Some(min), Some(max)) if !min.is_nan() && !max.is_nan() => {
                    self.filter_value = Some((min, max));
                    Ok(Some(i + 3))
                }
                _ => Err(resp_error("TSDB: Couldn't parse MIN or MAX")),
            };
        }
        if is(tok, "COUNT") {
            return match arg(1).and_then(|a| text(a).parse::<usize>().ok()) {
                Some(n) => {
                    self.count = Some(n);
                    Ok(Some(i + 2))
                }
                None => Err(resp_error("TSDB: Couldn't parse COUNT")),
            };
        }
        if is(tok, "ALIGN") {
            let value = arg(1).ok_or_else(syntax)?;
            self.align = if is(value, "start") || value == b"-" {
                Align::Start
            } else if is(value, "end") || value == b"+" {
                Align::End
            } else {
                match text(value).parse::<u64>() {
                    Ok(ts) => Align::At(ts),
                    Err(_) => return Err(resp_error("TSDB: unknown ALIGN parameter")),
                }
            };
            return Ok(Some(i + 2));
        }
        if is(tok, "AGGREGATION") {
            let mut lists = Vec::with_capacity(agg_lists);
            for n in 1..=agg_lists {
                let list = arg(n).ok_or_else(|| {
                    resp_error("TSDB: wrong number of aggregators for the number of keys")
                })?;
                lists.push(parse_aggregators(list)?);
            }
            let bucket = arg(agg_lists + 1)
                .and_then(|a| text(a).parse::<u64>().ok())
                .filter(|d| *d > 0)
                .ok_or_else(|| resp_error("TSDB: Couldn't parse bucketDuration"))?;
            self.aggregators = lists;
            self.bucket = bucket;
            return Ok(Some(i + agg_lists + 2));
        }
        if is(tok, "BUCKETTIMESTAMP") {
            let value = arg(1).ok_or_else(syntax)?;
            self.bucket_ts = if is(value, "start") || value == b"-" {
                BucketTimestamp::Start
            } else if is(value, "end") || value == b"+" {
                BucketTimestamp::End
            } else if is(value, "mid") || value == b"~" {
                BucketTimestamp::Mid
            } else {
                return Err(resp_error("TSDB: unknown BUCKETTIMESTAMP parameter"));
            };
            return Ok(Some(i + 2));
        }
        Ok(None)
    }

    /// The reference timestamp buckets are aligned to.
    fn alignment(&self, from: Option<u64>, to: Option<u64>) -> Result<u64, Vec<u8>> {
        match self.align {
            Align::Zero => Ok(0),
            Align::At(ts) => Ok(ts),
            Align::Start => from.ok_or_else(|| {
                resp_error("TSDB: start alignment can only be used with explicit start timestamp")
            }),
            Align::End => to.ok_or_else(|| {
                resp_error("TSDB: end alignment can only be used with explicit end timestamp")
            }),
        }
    }
}

/// `fromTimestamp` / `toTimestamp`: a millisecond timestamp, or `-` / `+` for
/// "the earliest / latest sample". `None` is the open form.
fn parse_bound(arg: &[u8], open: &[u8], what: &str) -> Result<Option<u64>, Vec<u8>> {
    if arg == open {
        return Ok(None);
    }
    text(arg)
        .parse::<u64>()
        .map(Some)
        .map_err(|_| resp_error(&format!("TSDB: wrong {what}")))
}

// ── Aggregation ──────────────────────────────────────────────────────────────

/// The start of the bucket `ts` falls in. Signed, because with an alignment
/// the first bucket can begin before time zero.
pub(super) fn bucket_start(ts: u64, align: u64, bucket: u64) -> i128 {
    let offset = (ts as i128 - align as i128).rem_euclid(bucket as i128);
    ts as i128 - offset
}

/// The nearest sample with a real value strictly before `ts`.
fn sample_before(series: &TsInner, ts: i128) -> Option<(u64, f64)> {
    if ts <= 0 {
        return None;
    }
    let ts = ts.min(u64::MAX as i128) as u64;
    series
        .samples
        .range(..ts)
        .rev()
        .find(|(_, v)| !v.is_nan())
        .map(|(t, v)| (*t, *v))
}

/// The nearest sample with a real value at or after `ts`.
fn sample_from(series: &TsInner, ts: i128) -> Option<(u64, f64)> {
    if ts > u64::MAX as i128 {
        return None;
    }
    let ts = ts.max(0) as u64;
    series
        .samples
        .range(ts..)
        .find(|(_, v)| !v.is_nan())
        .map(|(t, v)| (*t, *v))
}

/// The value at time `at` on the straight line through `a` and `b`.
fn interpolate(a: (u64, f64), b: (u64, f64), at: f64) -> f64 {
    if a.0 == b.0 {
        return a.1;
    }
    a.1 + (b.1 - a.1) * (at - a.0 as f64) / (b.0 as f64 - a.0 as f64)
}

/// Time-weighted average of `points` (real values, by time) over the bucket
/// `[start, end)`: the mean height of the line through the samples, extended
/// to the bucket's edges through the neighbouring samples where there are any.
fn time_weighted_average(
    points: &[(u64, f64)],
    start: i128,
    end: i128,
    before: Option<(u64, f64)>,
    after: Option<(u64, f64)>,
) -> f64 {
    let (Some(first), Some(last)) = (points.first().copied(), points.last().copied()) else {
        // An empty bucket: the line from the sample before to the sample
        // after, if there are both.
        return match (before, after) {
            (Some(b), Some(a)) => {
                (interpolate(b, a, start as f64) + interpolate(b, a, end as f64)) / 2.0
            }
            _ => f64::NAN,
        };
    };

    // (time, value) vertices of the line across the bucket.
    let mut line: Vec<(f64, f64)> = Vec::with_capacity(points.len() + 2);
    if let Some(b) = before
        && (first.0 as i128) > start
    {
        line.push((start as f64, interpolate(b, first, start as f64)));
    }
    line.extend(points.iter().map(|(t, v)| (*t as f64, *v)));
    if let Some(a) = after
        && (last.0 as i128) < end
    {
        line.push((end as f64, interpolate(last, a, end as f64)));
    }

    let span = line[line.len() - 1].0 - line[0].0;
    if span <= 0.0 {
        return first.1;
    }
    let area: f64 = line
        .windows(2)
        .map(|w| (w[0].1 + w[1].1) / 2.0 * (w[1].0 - w[0].0))
        .sum();
    area / span
}

/// Variance of `values` around their mean, over `divisor` degrees of freedom.
fn variance(values: &[f64], divisor: f64) -> f64 {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / divisor
}

/// One aggregator over one group of values. NaN values are not data: every
/// aggregator ignores them except the two that exist to count them.
///
/// `empty_is_nan` selects what `sum` reports for a group with no real values:
/// `0` as a bucket aggregator, `NaN` as a `GROUPBY` reducer.
pub(super) fn reduce_values(agg: &TsAggregation, values: &[f64], empty_is_nan: bool) -> f64 {
    let real: Vec<f64> = values.iter().copied().filter(|v| !v.is_nan()).collect();
    let n = real.len();
    match agg {
        TsAggregation::Count => return n as f64,
        TsAggregation::CountNan => return (values.len() - n) as f64,
        TsAggregation::CountAll => return values.len() as f64,
        _ => {}
    }
    if n == 0 {
        return match agg {
            TsAggregation::Sum if !empty_is_nan => 0.0,
            _ => f64::NAN,
        };
    }
    let min = || real.iter().copied().fold(f64::INFINITY, f64::min);
    let max = || real.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    match agg {
        // Twa without timestamps degrades to the plain mean; callers that have
        // timestamps use `time_weighted_average`.
        TsAggregation::Avg | TsAggregation::Twa => real.iter().sum::<f64>() / n as f64,
        TsAggregation::Sum => real.iter().sum(),
        TsAggregation::Min => min(),
        TsAggregation::Max => max(),
        TsAggregation::Range => max() - min(),
        TsAggregation::First => real[0],
        TsAggregation::Last => real[n - 1],
        TsAggregation::VarP => variance(&real, n as f64),
        TsAggregation::StdP => variance(&real, n as f64).sqrt(),
        // A single sample has no spread to estimate.
        TsAggregation::VarS => {
            if n < 2 {
                0.0
            } else {
                variance(&real, (n - 1) as f64)
            }
        }
        TsAggregation::StdS => {
            if n < 2 {
                0.0
            } else {
                variance(&real, (n - 1) as f64).sqrt()
            }
        }
        TsAggregation::Count | TsAggregation::CountNan | TsAggregation::CountAll => unreachable!(),
    }
}

/// `agg` over the samples of one bucket `[start, end)` of `series`.
/// `samples` may be empty (an `EMPTY` bucket).
pub(super) fn aggregate_bucket(
    agg: &TsAggregation,
    samples: &[(u64, f64)],
    series: &TsInner,
    start: i128,
    end: i128,
) -> f64 {
    if *agg == TsAggregation::Twa {
        let points: Vec<(u64, f64)> = samples
            .iter()
            .copied()
            .filter(|(_, v)| !v.is_nan())
            .collect();
        return time_weighted_average(
            &points,
            start,
            end,
            sample_before(series, start),
            sample_from(series, end),
        );
    }
    if samples.is_empty() {
        return match agg {
            TsAggregation::Sum
            | TsAggregation::Count
            | TsAggregation::CountNan
            | TsAggregation::CountAll => 0.0,
            // The value still standing when the bucket began.
            TsAggregation::Last => sample_before(series, start).map_or(f64::NAN, |(_, v)| v),
            _ => f64::NAN,
        };
    }
    let values: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
    reduce_values(agg, &values, false)
}

/// The first and last timestamps of the bucket starting at `start`.
pub(super) fn bucket_bounds(start: i128, duration: u64) -> (u64, u64) {
    let end = start + duration as i128;
    (
        start.max(0) as u64,
        (end - 1).clamp(0, u64::MAX as i128) as u64,
    )
}

/// The bucket of `rule` starting at `start`, aggregated from `source`: its
/// first timestamp, and its value.
pub(super) fn bucket_value(
    source: &TsInner,
    rule: &crate::types::CompactionRule,
    start: i128,
) -> (u64, f64) {
    let (first, last) = bucket_bounds(start, rule.bucket_duration);
    let samples: Vec<(u64, f64)> = source
        .samples
        .range(first..=last)
        .map(|(t, v)| (*t, *v))
        .collect();
    let end = start + rule.bucket_duration as i128;
    (
        first,
        aggregate_bucket(&rule.aggregation, &samples, source, start, end),
    )
}

/// A compaction's bucket still open, from its source: what `LATEST` adds
/// (#128). `None` for a series that is no compaction, or whose source is
/// gone or empty. Taken before the compaction is read, so that no series is
/// held while the source's shard is.
pub(super) fn open_bucket(
    ctx: &Ctx,
    dest: &Arc<RwLock<TsInner>>,
    dest_key: &str,
) -> Option<(u64, f64)> {
    let source_key = dest.read().source.clone()?;
    let source = series_at(ctx, &source_key).ok()??;
    let source = source.read();
    let rule = source.rules.iter().find(|r| r.dest_key == dest_key)?;
    let &newest = source.samples.keys().next_back()?;
    let start = bucket_start(newest, rule.align_timestamp, rule.bucket_duration);
    Some(bucket_value(&source, rule, start))
}

/// Run one series through the query: the samples in `[from, to]` that pass
/// the filters, raw or bucketed. Rows come back in ascending time order with
/// `COUNT` not yet applied — the caller reverses and truncates. `open` is a
/// compaction's open bucket, for `LATEST` ([`open_bucket`]).
pub(super) fn query_series(
    series: &TsInner,
    opts: &RangeOptions,
    aggregators: Option<&[TsAggregation]>,
    from: Option<u64>,
    to: Option<u64>,
    open: Option<(u64, f64)>,
) -> Result<Vec<Row>, Vec<u8>> {
    let lo = from.unwrap_or(0);
    let hi = to.unwrap_or(u64::MAX);
    if lo > hi {
        return Ok(Vec::new());
    }
    let passes = |ts: u64, value: f64| {
        opts.filter_ts
            .as_ref()
            .is_none_or(|list| list.contains(&ts))
            && opts
                .filter_value
                .is_none_or(|(min, max)| value >= min && value <= max)
    };
    // The open bucket comes after every closed one; a value an older build
    // wrote for it is left out.
    let open = open.filter(|(ts, _)| (lo..=hi).contains(ts));
    let samples = series
        .samples
        .range(lo..=hi)
        .map(|(ts, v)| (*ts, *v))
        .filter(|(ts, _)| open.is_none_or(|(at, _)| *ts != at))
        .chain(open)
        .filter(|(ts, v)| passes(*ts, *v));

    let Some(aggregators) = aggregators else {
        return Ok(samples.map(|(ts, v)| (ts, vec![v])).collect());
    };

    let align = opts.alignment(from, to)?;
    let bucket = opts.bucket;
    let mut buckets: BTreeMap<i128, Vec<(u64, f64)>> = BTreeMap::new();
    for (ts, v) in samples {
        buckets
            .entry(bucket_start(ts, align, bucket))
            .or_default()
            .push((ts, v));
    }

    if opts.empty {
        // Every bucket the query covers within the series' own lifetime —
        // none before its first sample or after its last.
        let earliest = series.samples.keys().next().copied();
        let latest = series.samples.keys().next_back().copied();
        if let (Some(earliest), Some(latest)) = (earliest, latest) {
            let (lo, hi) = (lo.max(earliest), hi.min(latest));
            if lo <= hi {
                let first = bucket_start(lo, align, bucket);
                let last = bucket_start(hi, align, bucket);
                if ((last - first) / bucket as i128) as u128 >= MAX_EMPTY_BUCKETS {
                    return Err(resp_error(
                        "TSDB: EMPTY would report too many buckets; narrow the range or use a \
                         longer bucketDuration",
                    ));
                }
                let mut start = first;
                while start <= last {
                    buckets.entry(start).or_default();
                    start += bucket as i128;
                }
            }
        }
    }

    Ok(buckets
        .into_iter()
        .map(|(start, samples)| {
            let end = start + bucket as i128;
            let reported = match opts.bucket_ts {
                BucketTimestamp::Start => start,
                BucketTimestamp::End => end,
                BucketTimestamp::Mid => start + (bucket / 2) as i128,
            }
            .clamp(0, u64::MAX as i128) as u64;
            let values = aggregators
                .iter()
                .map(|agg| aggregate_bucket(agg, &samples, series, start, end))
                .collect();
            (reported, values)
        })
        .collect())
}

/// Reverse (for the `REV` commands) and apply `COUNT`.
fn finish_rows(mut rows: Vec<Row>, reverse: bool, count: Option<usize>) -> Vec<Row> {
    if reverse {
        rows.reverse();
    }
    if let Some(n) = count {
        rows.truncate(n);
    }
    rows
}

// ── Replies ──────────────────────────────────────────────────────────────────

/// A sample value: a simple string under RESP2, a double under RESP3.
pub(super) fn value_reply(resp: u8, value: f64) -> Vec<u8> {
    if resp >= 3 {
        resp_double_resp3(value)
    } else {
        resp_simple(&value.to_string())
    }
}

/// `[timestamp, value]`.
pub(super) fn sample_reply(resp: u8, ts: u64, value: f64) -> Vec<u8> {
    resp_array(vec![resp_integer(ts as i64), value_reply(resp, value)])
}

/// Rows as `[timestamp, value, value, ...]` tuples.
fn rows_reply(resp: u8, rows: &[Row]) -> Vec<u8> {
    resp_array(
        rows.iter()
            .map(|(ts, values)| {
                let mut row = Vec::with_capacity(values.len() + 1);
                row.push(resp_integer(*ts as i64));
                row.extend(values.iter().map(|v| value_reply(resp, *v)));
                resp_array(row)
            })
            .collect(),
    )
}

fn reply_resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

/// The series at `key`, with no lock held.
pub(super) fn series_at(ctx: &Ctx, key: &str) -> Result<Option<Arc<RwLock<TsInner>>>, Vec<u8>> {
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    match read_lock!(shard).get(key) {
        None => Ok(None),
        Some(KiviDBValue::TimeSeries(inner)) => Ok(Some(Arc::clone(inner))),
        Some(_) => Err(resp_error(WRONGTYPE)),
    }
}

// ── TS.RANGE / TS.REVRANGE ───────────────────────────────────────────────────

pub(super) fn ts_range(ctx: &mut Ctx, reverse: bool) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for TS.RANGE/REVRANGE",
        ));
    }
    let Ok(key) = std::str::from_utf8(cmd[1]) else {
        return Some(resp_error("ERR key must be valid UTF-8"));
    };

    let parsed = (|| -> Result<_, Vec<u8>> {
        let from = parse_bound(cmd[2], b"-", "fromTimestamp")?;
        let to = parse_bound(cmd[3], b"+", "toTimestamp")?;
        let mut opts = RangeOptions::default();
        let mut i = 4;
        while i < cmd.len() {
            i = opts
                .parse_at(cmd, i, 1)?
                .ok_or_else(|| resp_error("ERR syntax error"))?;
        }
        Ok((from, to, opts))
    })();
    let (from, to, opts): (Option<u64>, Option<u64>, RangeOptions) = match parsed {
        Ok(p) => p,
        Err(e) => return Some(e),
    };

    let series = match series_at(ctx, key) {
        Ok(Some(series)) => series,
        Ok(None) => return Some(resp_error(NO_KEY)),
        Err(e) => return Some(e),
    };
    let open = if opts.latest {
        open_bucket(ctx, &series, key)
    } else {
        None
    };
    let rows = query_series(
        &series.read(),
        &opts,
        opts.aggregators.first().map(Vec::as_slice),
        from,
        to,
        open,
    );
    Some(match rows {
        Ok(rows) => rows_reply(reply_resp(ctx), &finish_rows(rows, reverse, opts.count)),
        Err(e) => e,
    })
}

// ── TS.MRANGE / TS.MREVRANGE ─────────────────────────────────────────────────

enum LabelSelection {
    None,
    All,
    Selected(Vec<String>),
}

/// Label pairs for a reply: `[[name, value], ...]` under RESP2, a map under
/// RESP3. A selected label the series does not have is reported as null.
fn labels_reply(resp: u8, pairs: Vec<(String, Option<String>)>) -> Vec<u8> {
    if resp >= 3 {
        resp_map(
            pairs
                .into_iter()
                .flat_map(|(name, value)| {
                    [
                        resp_bulk_str(&name),
                        value.map_or_else(resp_null_resp3, |v| resp_bulk_str(&v)),
                    ]
                })
                .collect(),
        )
    } else {
        resp_array(
            pairs
                .into_iter()
                .map(|(name, value)| {
                    resp_array(vec![
                        resp_bulk_str(&name),
                        value.map_or_else(resp_null_bulk, |v| resp_bulk_str(&v)),
                    ])
                })
                .collect(),
        )
    }
}

fn selected_labels(
    selection: &LabelSelection,
    labels: &AHashMap<String, String>,
) -> Vec<(String, Option<String>)> {
    match selection {
        LabelSelection::None => Vec::new(),
        LabelSelection::All => {
            let mut pairs: Vec<_> = labels
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect();
            pairs.sort();
            pairs
        }
        LabelSelection::Selected(names) => names
            .iter()
            .map(|name| (name.clone(), labels.get(name).cloned()))
            .collect(),
    }
}

pub(super) fn ts_mrange(ctx: &mut Ctx, reverse: bool) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return Some(resp_error(
            "ERR wrong number of arguments for TS.MRANGE/MREVRANGE",
        ));
    }

    let mut selection = LabelSelection::None;
    let mut filters: Vec<LabelFilter> = Vec::new();
    let mut group_by: Option<(String, TsAggregation)> = None;
    let mut exclude_empty = false;

    let parsed = (|| -> Result<_, Vec<u8>> {
        let from = parse_bound(cmd[1], b"-", "fromTimestamp")?;
        let to = parse_bound(cmd[2], b"+", "toTimestamp")?;
        let mut opts = RangeOptions::default();
        let mut i = 3;
        while i < cmd.len() {
            if let Some(next) = opts.parse_at(cmd, i, 1)? {
                i = next;
            } else if is(cmd[i], "WITHLABELS") {
                selection = LabelSelection::All;
                i += 1;
            } else if is(cmd[i], "SELECTED_LABELS") {
                let stop = |t: &[u8]| {
                    [
                        "COUNT",
                        "ALIGN",
                        "AGGREGATION",
                        "FILTER",
                        "GROUPBY",
                        "FILTER_BY_TS",
                        "FILTER_BY_VALUE",
                        "LATEST",
                        "EXCLUDEEMPTY",
                    ]
                    .iter()
                    .any(|word| is(t, word))
                };
                let mut names = Vec::new();
                i += 1;
                while i < cmd.len() && !stop(cmd[i]) {
                    names.push(text(cmd[i]).to_string());
                    i += 1;
                }
                if names.is_empty() {
                    return Err(resp_error(
                        "TSDB: SELECTED_LABELS one or more arguments are missing",
                    ));
                }
                selection = LabelSelection::Selected(names);
            } else if is(cmd[i], "FILTER") {
                i += 1;
                while i < cmd.len() && !is(cmd[i], "GROUPBY") && !is(cmd[i], "EXCLUDEEMPTY") {
                    match LabelFilter::parse(text(cmd[i])) {
                        Some(filter) => filters.push(filter),
                        None => return Err(resp_error("TSDB: failed parsing labels")),
                    }
                    i += 1;
                }
            } else if is(cmd[i], "GROUPBY") {
                // GROUPBY label REDUCE reducer
                let (Some(label), Some(reduce), Some(reducer)) =
                    (cmd.get(i + 1), cmd.get(i + 2), cmd.get(i + 3))
                else {
                    return Err(resp_error("ERR syntax error"));
                };
                if !is(reduce, "REDUCE") {
                    return Err(resp_error("ERR syntax error"));
                }
                let reducer = TsAggregation::parse(text(reducer))
                    .filter(|r| {
                        // The reducers the command documents; `first`, `last`
                        // and `twa` are bucket aggregators only.
                        !matches!(
                            r,
                            TsAggregation::First | TsAggregation::Last | TsAggregation::Twa
                        )
                    })
                    .ok_or_else(|| resp_error("TSDB: failed parsing reducer"))?;
                group_by = Some((text(label).to_string(), reducer));
                i += 4;
            } else if is(cmd[i], "EXCLUDEEMPTY") {
                exclude_empty = true;
                i += 1;
            } else {
                return Err(resp_error("ERR syntax error"));
            }
        }
        Ok((from, to, opts))
    })();
    let (from, to, opts): (Option<u64>, Option<u64>, RangeOptions) = match parsed {
        Ok(p) => p,
        Err(e) => return Some(e),
    };

    if filters.is_empty() {
        return Some(resp_error("ERR FILTER is required for TS.MRANGE/MREVRANGE"));
    }
    let aggregators = opts.aggregators.first().map(Vec::as_slice);
    if group_by.is_some() {
        if exclude_empty {
            return Some(resp_error(
                "TSDB: EXCLUDEEMPTY cannot be used together with GROUPBY",
            ));
        }
        if aggregators.is_some_and(|list| list.len() > 1) {
            return Some(resp_error(
                "TSDB: GROUPBY/REDUCE is not permitted when multiple aggregators are specified",
            ));
        }
    }

    let resp = reply_resp(ctx);
    let mut keys = keys_matching_filters(ctx.store, *ctx.selected_db, ctx.ts_index, &filters);
    keys.sort_unstable();

    // (key, labels, rows) per matching series, ascending.
    let mut results: Vec<(String, AHashMap<String, String>, Vec<Row>)> = Vec::new();
    for key in keys {
        let Ok(Some(series)) = series_at(ctx, &key) else {
            continue;
        };
        let open = if opts.latest {
            open_bucket(ctx, &series, &key)
        } else {
            None
        };
        let series = series.read();
        match query_series(&series, &opts, aggregators, from, to, open) {
            Ok(rows) => results.push((key, series.labels.clone(), rows)),
            Err(e) => return Some(e),
        }
    }

    let aggregator_names: Vec<Vec<u8>> = aggregators
        .unwrap_or_default()
        .iter()
        .map(|agg| resp_bulk_str(agg.name()))
        .collect();

    let Some((label, reducer)) = group_by else {
        let series: Vec<(String, Vec<Vec<u8>>)> = results
            .into_iter()
            // A series whose only samples in range are NaN still has samples.
            .filter(|(_, _, rows)| !exclude_empty || !rows.is_empty())
            .map(|(key, labels, rows)| {
                let labels = labels_reply(resp, selected_labels(&selection, &labels));
                let samples = rows_reply(resp, &finish_rows(rows, reverse, opts.count));
                let body = if resp >= 3 {
                    vec![
                        labels,
                        resp_map(vec![
                            resp_bulk_str("aggregators"),
                            resp_array(aggregator_names.clone()),
                        ]),
                        samples,
                    ]
                } else {
                    vec![labels, samples]
                };
                (key, body)
            })
            .collect();
        return Some(series_reply(resp, series));
    };

    // ── GROUPBY label REDUCE reducer ─────────────────────────────────────
    // Series sharing a value of `label` form a group; at each timestamp any
    // of them reports, the reducer combines what they report. A series
    // without the label belongs to no group.
    // By label value: the keys of the group's series, and their values at
    // each timestamp.
    type Group = (Vec<String>, BTreeMap<u64, Vec<f64>>);
    let mut groups: BTreeMap<String, Group> = BTreeMap::new();
    for (key, labels, rows) in results {
        let Some(value) = labels.get(&label) else {
            continue;
        };
        let (sources, by_time) = groups.entry(value.clone()).or_default();
        sources.push(key);
        for (ts, values) in rows {
            by_time.entry(ts).or_default().extend(values);
        }
    }
    let series = groups
        .into_iter()
        .map(|(value, (sources, by_time))| {
            let rows: Vec<Row> = by_time
                .into_iter()
                .map(|(ts, values)| (ts, vec![reduce_values(&reducer, &values, true)]))
                .collect();
            let samples = rows_reply(resp, &finish_rows(rows, reverse, opts.count));
            let name = format!("{label}={value}");
            let group_label = (label.clone(), Some(value));
            let body = if resp >= 3 {
                let labels = match &selection {
                    LabelSelection::None => Vec::new(),
                    LabelSelection::All => vec![group_label],
                    LabelSelection::Selected(names) => names
                        .iter()
                        .map(|n| {
                            if *n == label {
                                group_label.clone()
                            } else {
                                (n.clone(), None)
                            }
                        })
                        .collect(),
                };
                vec![
                    labels_reply(resp, labels),
                    resp_map(vec![
                        resp_bulk_str("reducers"),
                        resp_array(vec![resp_bulk_str(reducer.name())]),
                    ]),
                    resp_map(vec![
                        resp_bulk_str("sources"),
                        resp_array(sources.iter().map(|k| resp_bulk_str(k)).collect()),
                    ]),
                    samples,
                ]
            } else {
                let labels = match &selection {
                    LabelSelection::None => Vec::new(),
                    LabelSelection::All => vec![
                        group_label,
                        ("__reducer__".to_string(), Some(reducer.name().to_string())),
                        ("__source__".to_string(), Some(sources.join(","))),
                    ],
                    LabelSelection::Selected(names) => names
                        .iter()
                        .map(|n| {
                            if *n == label {
                                group_label.clone()
                            } else {
                                (n.clone(), None)
                            }
                        })
                        .collect(),
                };
                vec![labels_reply(resp, labels), samples]
            };
            (name, body)
        })
        .collect();
    Some(series_reply(resp, series))
}

/// The outer shape of a multi-series reply: an array of `[name, ...]` under
/// RESP2, a map from name to the rest under RESP3.
fn series_reply(resp: u8, series: Vec<(String, Vec<Vec<u8>>)>) -> Vec<u8> {
    if resp >= 3 {
        resp_map(
            series
                .into_iter()
                .flat_map(|(name, body)| [resp_bulk_str(&name), resp_array(body)])
                .collect(),
        )
    } else {
        resp_array(
            series
                .into_iter()
                .map(|(name, mut body)| {
                    body.insert(0, resp_bulk_str(&name));
                    resp_array(body)
                })
                .collect(),
        )
    }
}

// ── TS.NRANGE / TS.NREVRANGE (Redis 8.10) ────────────────────────────────────

/// `TS.NRANGE numkeys key [key ...] from to [options]`: the same range over
/// several named series, joined by timestamp — one row per timestamp any of
/// them reports, with a value (or `NaN`) for each key in the order given.
pub(super) fn ts_nrange(ctx: &mut Ctx, reverse: bool) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    let name = if reverse { "TS.NREVRANGE" } else { "TS.NRANGE" };
    if cmd.len() < 5 {
        return Some(resp_error(&format!(
            "ERR wrong number of arguments for '{name}' command"
        )));
    }
    let numkeys = match text(cmd[1]).parse::<usize>() {
        Ok(n) if n > 0 => n,
        _ => return Some(resp_error("TSDB: numkeys must be a positive integer")),
    };
    // numkeys keys, then from and to.
    if numkeys > cmd.len() - 4 {
        return Some(resp_error(
            "TSDB: numkeys does not match the number of keys given",
        ));
    }
    let keys = &cmd[2..2 + numkeys];

    let parsed = (|| -> Result<_, Vec<u8>> {
        let from = parse_bound(cmd[2 + numkeys], b"-", "fromTimestamp")?;
        let to = parse_bound(cmd[3 + numkeys], b"+", "toTimestamp")?;
        let mut opts = RangeOptions::default();
        let mut i = 4 + numkeys;
        while i < cmd.len() {
            i = opts
                .parse_at(cmd, i, numkeys)?
                .ok_or_else(|| resp_error("ERR syntax error"))?;
        }
        Ok((from, to, opts))
    })();
    let (from, to, opts): (Option<u64>, Option<u64>, RangeOptions) = match parsed {
        Ok(p) => p,
        Err(e) => return Some(e),
    };

    // Every key must exist and be a series; a repeated key is a column of
    // its own each time it is named.
    let mut series = Vec::with_capacity(numkeys);
    for raw in keys {
        let Ok(key) = std::str::from_utf8(raw) else {
            return Some(resp_error("ERR key must be valid UTF-8"));
        };
        match series_at(ctx, key) {
            Ok(Some(s)) => series.push(s),
            Ok(None) => return Some(resp_error(NO_KEY)),
            Err(e) => return Some(e),
        }
    }

    // The values each key contributes per row, and where its columns start.
    let widths: Vec<usize> = (0..numkeys)
        .map(|k| opts.aggregators.get(k).map_or(1, Vec::len))
        .collect();
    let total: usize = widths.iter().sum();

    let mut joined: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
    let mut column = 0usize;
    for (k, series) in series.iter().enumerate() {
        let aggregators = opts.aggregators.get(k).map(Vec::as_slice);
        let rows = match query_series(&series.read(), &opts, aggregators, from, to, None) {
            Ok(rows) => rows,
            Err(e) => return Some(e),
        };
        for (ts, values) in rows {
            // A key with nothing at this timestamp stays NaN.
            let row = joined.entry(ts).or_insert_with(|| vec![f64::NAN; total]);
            row[column..column + values.len()].copy_from_slice(&values);
        }
        column += widths[k];
    }

    // COUNT applies to the joined rows: the lowest timestamps going forward,
    // the highest going backward.
    let rows = finish_rows(joined.into_iter().collect(), reverse, opts.count);
    let resp = reply_resp(ctx);
    Some(resp_array(
        rows.iter()
            .map(|(ts, values)| {
                resp_array(vec![
                    resp_integer(*ts as i64),
                    resp_array(values.iter().map(|v| value_reply(resp, *v)).collect()),
                ])
            })
            .collect(),
    ))
}

// ── TS.READ (Redis 8.10) ─────────────────────────────────────────────────────

/// `TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count]`
///
/// The samples at or after a cursor, oldest first. With `BLOCK` it waits
/// until `min_count` of them exist or the timeout passes, and then replies
/// with whatever there is — a short or empty reply on timeout is a success.
pub(super) fn ts_read(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let arity = || resp_error("ERR wrong number of arguments for 'TS.READ' command");
    if ctx.cmd.len() < 3 {
        return Some(arity());
    }
    let Ok(key) = std::str::from_utf8(ctx.cmd[1]) else {
        return Some(resp_error("ERR key must be valid UTF-8"));
    };
    let key = key.to_string();
    let cursor_arg = ctx.cmd[2];

    let mut block: Option<(u64, usize)> = None;
    let mut max_count: Option<usize> = None;
    let mut i = 3;
    while i < ctx.cmd.len() {
        let number = |n: usize| ctx.cmd.get(i + n).and_then(|a| text(a).parse::<u64>().ok());
        if is(ctx.cmd[i], "BLOCK") {
            if ctx.cmd.len() < i + 3 {
                return Some(arity());
            }
            let Some(ms) = number(1) else {
                return Some(resp_error("TSDB: invalid BLOCK milliseconds"));
            };
            let Some(min_count) = number(2).filter(|n| *n > 0) else {
                return Some(resp_error("TSDB: min_count must be a positive integer"));
            };
            block = Some((ms, min_count as usize));
            i += 3;
        } else if is(ctx.cmd[i], "MAX_COUNT") {
            if ctx.cmd.len() < i + 2 {
                return Some(arity());
            }
            let Some(n) = number(1).filter(|n| *n > 0) else {
                return Some(resp_error("TSDB: max_count must be a positive integer"));
            };
            max_count = Some(n as usize);
            i += 2;
        } else {
            return Some(arity());
        }
    }
    if let (Some((_, min_count)), Some(max_count)) = (block, max_count)
        && min_count > max_count
    {
        return Some(resp_error(
            "TSDB: min_count cannot be greater than max_count",
        ));
    }
    if block.is_some() && ctx.blocking.as_ref().is_none_or(|slot| !slot.awaitable) {
        return Some(resp_error(
            "ERR TS.READ with BLOCK is not allowed in this context",
        ));
    }

    // The cursor and the deadline are fixed when the command arrives and
    // carried across every park: `$` must not come to mean "after the sample
    // that just woke us".
    let resumed = blocking::take_resume(ctx);
    let carried = match blocking::take_state(ctx) {
        Some(BlockState::TsRead {
            deadline,
            cursor,
            existed,
        }) => Some((deadline, cursor, existed)),
        _ => None,
    };
    let timed_out = matches!(resumed, Some(None));

    // Registered before looking, so an append between the look and the park
    // still wakes us.
    let rx = (block.is_some() && !timed_out).then(|| ctx.wait_table.register_signal(&key));

    let series = match series_at(ctx, &key) {
        Ok(series) => series,
        Err(e) => return Some(e),
    };

    // The series was deleted (or replaced by something else) while this
    // client waited on it: there is nothing left to wait for.
    if let Some((_, _, true)) = carried
        && series.is_none()
    {
        drop(rx);
        ctx.wait_table.prune_signals(&key);
        return Some(resp_array(Vec::new()));
    }
    let existed = carried.map_or(series.is_some(), |(_, _, existed)| existed);

    let (deadline, cursor) = match carried {
        Some((deadline, cursor, _)) => (deadline, cursor),
        None => {
            let (earliest, latest) = series.as_ref().map_or((None, None), |s| {
                let s = s.read();
                (
                    s.samples.keys().next().copied(),
                    s.samples.keys().next_back().copied(),
                )
            });
            let cursor = match cursor_arg {
                b"-" => earliest.unwrap_or(0),
                b"+" => latest.unwrap_or(0),
                b"$" => latest.map_or(0, |ts| ts.saturating_add(1)),
                other => match text(other).parse::<u64>() {
                    Ok(ts) => ts,
                    Err(_) => return Some(resp_error("TSDB: invalid timestamp")),
                },
            };
            let deadline = block
                .and_then(|(ms, _)| blocking::timeout_from_ms(ms))
                .map(|timeout| Instant::now() + timeout);
            (deadline, cursor)
        }
    };

    let available: Vec<(u64, f64)> = series.as_ref().map_or_else(Vec::new, |s| {
        let limit = max_count.unwrap_or(usize::MAX);
        // One more than a blocked reader needs is never required, but the
        // reply is capped by max_count, not by min_count.
        s.read()
            .samples
            .range(cursor..)
            .take(limit)
            .map(|(ts, v)| (*ts, *v))
            .collect()
    });

    let resp = reply_resp(ctx);
    let reply = |samples: &[(u64, f64)]| {
        resp_array(
            samples
                .iter()
                .map(|(ts, v)| sample_reply(resp, *ts, *v))
                .collect(),
        )
    };

    let Some((_, min_count)) = block else {
        return Some(reply(&available));
    };
    let Some(rx) = rx else {
        // The wait ran out: whatever is there is the answer.
        ctx.wait_table.prune_signals(&key);
        return Some(reply(&available));
    };
    if available.len() >= min_count {
        return Some(reply(&available));
    }
    let remaining = match deadline {
        None => None,
        Some(deadline) => {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                drop(rx);
                ctx.wait_table.prune_signals(&key);
                return Some(reply(&available));
            }
            Some(left)
        }
    };
    match blocking::park_with_state(
        ctx,
        vec![(key.clone(), rx)],
        remaining,
        Some(BlockState::TsRead {
            deadline,
            cursor,
            existed,
        }),
    ) {
        ParkOutcome::Parked => Some(Vec::new()),
        ParkOutcome::CannotWait => Some(reply(&available)),
    }
}

// ── TS.QUERYLABELS (Redis 8.10) ──────────────────────────────────────────────

/// `TS.QUERYLABELS <LABELS | VALUES label> [FILTER filterExpr ...]`: the
/// distinct label names — or the distinct values of one label — across the
/// series that match, or across all series when there is no filter.
pub(super) fn ts_querylabels(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'TS.QUERYLABELS' command",
        ));
    }
    // What to collect: every label name, or the values of one label.
    let (values_of, mut i): (Option<&str>, usize) = if is(cmd[1], "LABELS") {
        (None, 2)
    } else if is(cmd[1], "VALUES") {
        match cmd.get(2) {
            Some(label) => (Some(text(label)), 3),
            None => return Some(resp_error("TSDB: VALUES requires a label")),
        }
    } else {
        return Some(resp_error("TSDB: expected LABELS or VALUES"));
    };

    let mut filters: Vec<LabelFilter> = Vec::new();
    if i < cmd.len() {
        if !is(cmd[i], "FILTER") || i + 1 >= cmd.len() {
            return Some(resp_error("ERR syntax error"));
        }
        i += 1;
        while i < cmd.len() {
            match LabelFilter::parse(text(cmd[i])) {
                Some(filter) => filters.push(filter),
                None => return Some(resp_error("TSDB: failed parsing labels")),
            }
            i += 1;
        }
    }

    // Series the user may not read contribute nothing — silently, unlike
    // TS.QUERYINDEX, which lists every matching key.
    let acl = ctx.acl_store.read();
    let restricted = acl
        .get(ctx.authenticated_user)
        .filter(|user| !user.all_keys());

    let keys = keys_matching_filters(ctx.store, *ctx.selected_db, ctx.ts_index, &filters);
    let mut found: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for key in keys {
        if restricted.is_some_and(|user| !crate::acl::may_read_key(user, &key)) {
            continue;
        }
        let Ok(Some(series)) = series_at(ctx, &key) else {
            continue;
        };
        let series = series.read();
        match values_of {
            None => found.extend(series.labels.keys().cloned()),
            Some(label) => found.extend(series.labels.get(label).cloned()),
        }
    }

    let items: Vec<Vec<u8>> = found.iter().map(|s| resp_bulk_str(s)).collect();
    Some(resp_set_for(reply_resp(ctx), items))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(samples: &[(u64, f64)]) -> TsInner {
        let mut s = TsInner::new();
        for (ts, v) in samples {
            s.samples.insert(*ts, *v);
            s.last_ts = Some(*ts);
        }
        s
    }

    fn opts(tokens: &[&str], agg_lists: usize) -> RangeOptions {
        let tokens: Vec<&[u8]> = tokens.iter().map(|t| t.as_bytes()).collect();
        let mut o = RangeOptions::default();
        let mut i = 0;
        while i < tokens.len() {
            i = o
                .parse_at(&tokens, i, agg_lists)
                .unwrap()
                .expect("a range option");
        }
        o
    }

    fn run(s: &TsInner, tokens: &[&str], from: Option<u64>, to: Option<u64>) -> Vec<Row> {
        let o = opts(tokens, 1);
        query_series(
            s,
            &o,
            o.aggregators.first().map(Vec::as_slice),
            from,
            to,
            None,
        )
        .unwrap()
    }

    fn stock() -> TsInner {
        series(&[
            (1000, 100.0),
            (1010, 110.0),
            (1020, 120.0),
            (2000, 200.0),
            (2010, 210.0),
            (2020, 220.0),
            (3000, 300.0),
            (3010, 310.0),
            (3020, 320.0),
        ])
    }

    fn starts(rows: &[Row]) -> Vec<u64> {
        rows.iter().map(|(ts, _)| *ts).collect()
    }

    /// The alignment examples on the Redis `TS.RANGE` page.
    #[test]
    fn buckets_follow_the_alignment() {
        let s = stock();
        let rows = run(&s, &["AGGREGATION", "min", "20"], None, None);
        assert_eq!(starts(&rows), [1000, 1020, 2000, 2020, 3000, 3020]);
        assert_eq!(rows[0].1, [100.0]);
        assert_eq!(rows[1].1, [120.0]);

        let rows = run(&s, &["ALIGN", "10", "AGGREGATION", "min", "20"], None, None);
        assert_eq!(starts(&rows), [990, 1010, 1990, 2010, 2990, 3010]);
        assert_eq!(rows[1].1, [110.0]);

        let rows = run(
            &s,
            &["ALIGN", "-", "AGGREGATION", "min", "20"],
            Some(5),
            None,
        );
        assert_eq!(starts(&rows), [985, 1005, 1985, 2005, 2985, 3005]);

        // `ALIGN start` needs an explicit start.
        let o = opts(&["ALIGN", "start", "AGGREGATION", "min", "20"], 1);
        assert!(query_series(&s, &o, Some(&o.aggregators[0]), None, None, None).is_err());
    }

    /// The multi-aggregator example on the Redis `TS.RANGE` page.
    #[test]
    fn several_aggregators_give_several_values_per_bucket() {
        let s = series(&[(1000, 30.0), (1010, 35.0), (1020, 9999.0), (1030, 40.0)]);
        let rows = run(
            &s,
            &[
                "FILTER_BY_VALUE",
                "-100",
                "100",
                "AGGREGATION",
                "min,avg,max",
                "1000",
            ],
            None,
            None,
        );
        assert_eq!(rows, vec![(1000, vec![30.0, 35.0, 40.0])]);
    }

    #[test]
    fn filters_apply_before_aggregation() {
        let s = stock();
        let rows = run(&s, &["FILTER_BY_TS", "1000", "1020", "5"], None, None);
        assert_eq!(starts(&rows), [1000, 1020]);
        let rows = run(&s, &["FILTER_BY_VALUE", "200", "250"], None, Some(2010));
        assert_eq!(starts(&rows), [2000, 2010]);
    }

    #[test]
    fn nan_values_are_counted_separately_and_otherwise_ignored() {
        let values = [1.0, f64::NAN, 3.0];
        let r = |agg| reduce_values(&agg, &values, false);
        assert_eq!(r(TsAggregation::Count), 2.0);
        assert_eq!(r(TsAggregation::CountNan), 1.0);
        assert_eq!(r(TsAggregation::CountAll), 3.0);
        assert_eq!(r(TsAggregation::Avg), 2.0);
        assert_eq!(r(TsAggregation::Sum), 4.0);
        assert_eq!(r(TsAggregation::Max), 3.0);
        assert_eq!(r(TsAggregation::Last), 3.0);

        // Nothing but NaN: the counts are still counts.
        let all_nan = [f64::NAN, f64::NAN];
        assert_eq!(reduce_values(&TsAggregation::Count, &all_nan, true), 0.0);
        assert_eq!(reduce_values(&TsAggregation::CountNan, &all_nan, true), 2.0);
        assert_eq!(reduce_values(&TsAggregation::CountAll, &all_nan, true), 2.0);
        assert!(reduce_values(&TsAggregation::Avg, &all_nan, false).is_nan());
        assert!(reduce_values(&TsAggregation::Min, &all_nan, false).is_nan());
        assert_eq!(reduce_values(&TsAggregation::Sum, &all_nan, false), 0.0);
        assert!(reduce_values(&TsAggregation::Sum, &all_nan, true).is_nan());
    }

    #[test]
    fn empty_reports_the_buckets_between_the_first_and_last_sample() {
        let s = series(&[(1000, 1.0), (4000, 4.0)]);
        let rows = run(
            &s,
            &["AGGREGATION", "sum,last,min,count", "1000", "EMPTY"],
            None,
            None,
        );
        assert_eq!(starts(&rows), [1000, 2000, 3000, 4000]);
        // sum 0, last = the value still standing, min NaN, count 0
        let empty = &rows[1].1;
        assert_eq!(empty[0], 0.0);
        assert_eq!(empty[1], 1.0);
        assert!(empty[2].is_nan());
        assert_eq!(empty[3], 0.0);

        // Nothing before the first sample or after the last, whatever the range.
        let rows = run(
            &s,
            &["AGGREGATION", "sum", "1000", "EMPTY"],
            Some(0),
            Some(9000),
        );
        assert_eq!(starts(&rows), [1000, 2000, 3000, 4000]);

        // Without EMPTY, only buckets that hold samples.
        let rows = run(&s, &["AGGREGATION", "sum", "1000"], None, None);
        assert_eq!(starts(&rows), [1000, 4000]);
    }

    #[test]
    fn empty_refuses_an_absurd_number_of_buckets() {
        let s = series(&[(0, 1.0), (10_000_000, 2.0)]);
        let o = opts(&["AGGREGATION", "sum", "1", "EMPTY"], 1);
        assert!(query_series(&s, &o, Some(&o.aggregators[0]), None, None, None).is_err());
    }

    #[test]
    fn bucket_timestamps_can_be_start_end_or_mid() {
        let s = series(&[(1005, 1.0)]);
        let at = |bt: &str| {
            starts(&run(
                &s,
                &["AGGREGATION", "sum", "100", "BUCKETTIMESTAMP", bt],
                None,
                None,
            ))
        };
        assert_eq!(at("-"), [1000]);
        assert_eq!(at("start"), [1000]);
        assert_eq!(at("+"), [1100]);
        assert_eq!(at("~"), [1050]);
        assert_eq!(at("mid"), [1050]);
    }

    #[test]
    fn time_weighted_average_weights_by_time() {
        // 10 for the first 900ms of the bucket, climbing to 20 only at the end.
        let s = series(&[(0, 10.0), (900, 10.0), (1000, 20.0)]);
        let rows = run(&s, &["AGGREGATION", "twa,avg", "1000"], Some(0), Some(999));
        let (twa, avg) = (rows[0].1[0], rows[0].1[1]);
        assert_eq!(avg, 10.0);
        // 900ms at 10, then 100ms rising linearly from 10 to 20: 10.5
        assert!((twa - 10.5).abs() < 1e-9, "{twa}");

        // An empty bucket takes the line between its neighbours.
        let s = series(&[(0, 0.0), (3000, 30.0)]);
        let rows = run(&s, &["AGGREGATION", "twa", "1000", "EMPTY"], None, None);
        assert!((rows[1].1[0] - 15.0).abs() < 1e-9, "{:?}", rows);
    }

    #[test]
    fn option_parsing_rejects_what_it_should() {
        let parse = |tokens: &[&str], lists: usize| {
            let tokens: Vec<&[u8]> = tokens.iter().map(|t| t.as_bytes()).collect();
            RangeOptions::default().parse_at(&tokens, 0, lists)
        };
        assert!(parse(&["AGGREGATION", "median", "10"], 1).is_err());
        assert!(parse(&["AGGREGATION", "avg", "0"], 1).is_err());
        assert!(parse(&["AGGREGATION", "avg"], 1).is_err());
        assert!(parse(&["FILTER_BY_VALUE", "1", "nan"], 1).is_err());
        assert!(parse(&["FILTER_BY_TS"], 1).is_err());
        assert!(parse(&["COUNT", "x"], 1).is_err());
        assert!(parse(&["ALIGN", "sideways"], 1).is_err());
        assert_eq!(parse(&["WITHLABELS"], 1), Ok(None));
        // One aggregator list per key.
        assert_eq!(
            parse(&["AGGREGATION", "avg,max", "sum", "1000"], 2),
            Ok(Some(4))
        );
        assert!(parse(&["AGGREGATION", "avg", "1000"], 2).is_err());
        let o = opts(&["AGGREGATION", "avg,max", "sum", "1000"], 2);
        assert_eq!(o.aggregators.len(), 2);
        assert_eq!(o.aggregators[0].len(), 2);
    }

    #[test]
    fn reverse_and_count_apply_last() {
        let rows = vec![(1, vec![1.0]), (2, vec![2.0]), (3, vec![3.0])];
        assert_eq!(starts(&finish_rows(rows.clone(), false, Some(2))), [1, 2]);
        assert_eq!(starts(&finish_rows(rows, true, Some(2))), [3, 2]);
    }
}
