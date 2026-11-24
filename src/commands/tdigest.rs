//! `TDIGEST.*`: t-digests, as Redis 8 has them built in (formerly
//! RedisBloom). The digest itself is `sketch::tdigest`.
//!
//! The replies and errors are Redis 8.10's, in RESP2 and RESP3 (doubles as
//! bulk strings in RESP2, `nan`, `inf` and `-inf` spelled as Redis spells
//! them), with the order in which Redis checks arguments. The numbers are
//! the published merging digest's, within the same bounds as Redis's and the
//! same as Redis's where nothing is approximated; a query never changes a
//! digest (see `sketch::tdigest`). The commands are deterministic, and go to
//! replicas and the AOF as they came.
//!
//! A write invalidates a `WATCH` on its key once it has read its arguments,
//! even when it fails, as Redis's do. No keyspace events, as in Redis.
//!
//! Beyond Redis: a compression of at most `sketch::tdigest::MAX_COMPRESSION`,
//! and a per-command limit on the work of a query of many values
//! ([`BUDGET`]) and of a merge ([`MERGE_BUDGET`]), checked before it starts.
//! Under `maxmemory`, a client's `TDIGEST.CREATE` or `TDIGEST.MERGE` that
//! would allocate more than is left evicts first, under the policy, before
//! it takes its keys (`sketch::room`), and is refused with `OOM` only if
//! that is not enough.

use std::cell::Cell;
use std::sync::Arc;

use crate::commands::bf::{parse_double, parse_ll};
use crate::commands::ctx::Ctx;
use crate::resp::write::{
    resp_array, resp_double_for, resp_error, resp_integer, resp_map_for, resp_simple,
};
use crate::sketch::Sketch;
use crate::sketch::tdigest::{
    DEFAULT_COMPRESSION, MAX_COMPRESSION, TDigest, capacity_of, info_memory,
};
use crate::store::{self, Key, ValueRef};
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NO_KEY: &str = "ERR T-Digest: key does not exist";
const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";
const OVER_BUDGET: &str =
    "ERR T-Digest: command reached the per-command limit; send fewer values per command";

/// What one query of many values may cost at most (values times
/// centroids), checked before it starts: some 15 ms of work (11,000
/// quantiles of a digest of compression 100).
pub const BUDGET: u64 = 4_000_000;

/// The most centroids one merge may read: one digest of the largest
/// compression, full, some 30 ms of work (or a thousand of compression 100).
pub const MERGE_BUDGET: u64 = 600_010;

fn too_large() -> Vec<u8> {
    resp_error(&format!(
        "ERR T-Digest: compression parameter is too large (at most {MAX_COMPRESSION})"
    ))
}

/// Each command's arity, as Redis's `COMMAND` gives it, for `COMMAND INFO`
/// alone.
pub(crate) fn arity(name: &str) -> Option<i16> {
    Some(match name {
        "TDIGEST.CREATE" => -2,
        "TDIGEST.ADD" | "TDIGEST.QUANTILE" | "TDIGEST.CDF" | "TDIGEST.RANK" | "TDIGEST.REVRANK"
        | "TDIGEST.BYRANK" | "TDIGEST.BYREVRANK" => -3,
        "TDIGEST.MERGE" => -4,
        "TDIGEST.RESET" | "TDIGEST.MIN" | "TDIGEST.MAX" | "TDIGEST.INFO" => 2,
        "TDIGEST.TRIMMED_MEAN" => 4,
        _ => return None,
    })
}

thread_local! {
    static TOUCHED: Cell<bool> = const { Cell::new(false) };
}

/// Whether the `TDIGEST.*` write that just failed had got as far as its key.
pub(crate) fn take_touched() -> bool {
    TOUCHED.replace(false)
}

fn wrong_args(name: &str) -> Vec<u8> {
    resp_error(&format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

fn resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

fn double(resp: u8, v: f64) -> Vec<u8> {
    resp_double_for(resp, v)
}

/// Room under `maxmemory` for `bytes` more, for a command from a client:
/// `OOM` if they do not fit. The room was made before the command took its
/// keys (`sketch::room`, which also stops a command here that is only asked
/// what it wants): nothing is evicted here.
fn room(ctx: &Ctx, bytes: u64) -> Result<(), Vec<u8>> {
    crate::sketch::room::check(ctx, |_| bytes).map_err(|_| resp_error(OOM))
}

/// A compression as Redis reads one.
fn compression(raw: &[u8]) -> Result<u64, Vec<u8>> {
    match parse_ll(raw) {
        None => Err(resp_error(
            "ERR T-Digest: error parsing compression parameter",
        )),
        Some(c) if c <= 0 => Err(resp_error(
            "ERR T-Digest: compression parameter needs to be a positive integer",
        )),
        Some(c) if c as u64 > MAX_COMPRESSION => Err(too_large()),
        Some(c) => Ok(c as u64),
    }
}

// ── Dispatch ─────────────────────────────────────────────────────────────

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("TDIGEST.") {
        return None;
    }
    Some(match name {
        "TDIGEST.CREATE" => create(ctx),
        "TDIGEST.ADD" => add(ctx),
        "TDIGEST.RESET" => reset(ctx),
        "TDIGEST.MERGE" => merge(ctx),
        "TDIGEST.MIN" => min_max(ctx, true),
        "TDIGEST.MAX" => min_max(ctx, false),
        "TDIGEST.QUANTILE" => quantile(ctx),
        "TDIGEST.CDF" => cdf(ctx),
        "TDIGEST.RANK" => rank(ctx, false),
        "TDIGEST.REVRANK" => rank(ctx, true),
        "TDIGEST.BYRANK" => byrank(ctx, false),
        "TDIGEST.BYREVRANK" => byrank(ctx, true),
        "TDIGEST.TRIMMED_MEAN" => trimmed_mean(ctx),
        "TDIGEST.INFO" => info(ctx),
        _ => return None,
    })
}

/// A copy of the digest at `key`: `Ok(None)` if missing, `Err(())` if it
/// holds another type.
fn digest_at(ctx: &Ctx, key: &[u8]) -> Result<Option<Arc<parking_lot::RwLock<Sketch>>>, ()> {
    if crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return Ok(None);
    }
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Ok(None),
        Some(ValueRef::Sketch(sketch)) => match &*sketch.read() {
            Sketch::TDigest(_) => Ok(Some(Arc::clone(sketch))),
            #[allow(unreachable_patterns)]
            _ => Err(()),
        },
        Some(_) => Err(()),
    }
}

/// The value at `key` if it is a sketch: `NO_KEY` if missing, `WRONGTYPE` if
/// it is no sketch. The caller tells a sketch of another type under its own
/// lock of it. `write`: expired first as a write expires it.
fn sketch_at(
    ctx: &Ctx,
    key: &[u8],
    write: bool,
    counted: bool,
) -> Result<Arc<parking_lot::RwLock<Sketch>>, Vec<u8>> {
    let gone = if write {
        crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
        false
    } else {
        crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key)
    };
    if gone {
        if counted {
            crate::stats::record_miss();
        }
        return Err(resp_error(NO_KEY));
    }
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    let found = map.get(key);
    if counted {
        crate::stats::record_lookup(found.is_some());
    }
    match found {
        None => Err(resp_error(NO_KEY)),
        Some(ValueRef::Sketch(sketch)) => Ok(Arc::clone(sketch)),
        Some(_) => Err(resp_error(WRONGTYPE)),
    }
}

/// Whether `key` holds a digest, or the error it gets: the look a command
/// takes before it reads its arguments. Counted in `keyspace_hits` /
/// `keyspace_misses` only when it ends the command (a key that is not there
/// is a miss, one of another type a hit); else the read after it counts.
fn probe_digest(ctx: &Ctx, key: &[u8]) -> Result<(), Vec<u8>> {
    let sketch = match sketch_at(ctx, key, false, false) {
        Ok(sketch) => sketch,
        Err(e) => {
            crate::stats::record_lookup(e != resp_error(NO_KEY));
            return Err(e);
        }
    };
    match &*sketch.read() {
        Sketch::TDigest(_) => Ok(()),
        #[allow(unreachable_patterns)]
        _ => {
            crate::stats::record_hit();
            Err(resp_error(WRONGTYPE))
        }
    }
}

/// Run `f` on the digest at `key`, for reading, under one hold of it.
fn with_digest<T>(ctx: &Ctx, key: &[u8], f: impl FnOnce(&TDigest) -> T) -> Result<T, Vec<u8>> {
    let sketch = sketch_at(ctx, key, false, true)?;
    match &*sketch.read() {
        Sketch::TDigest(t) => Ok(f(t)),
        #[allow(unreachable_patterns)]
        _ => Err(resp_error(WRONGTYPE)),
    }
}

/// Run `f` on the digest at `key`, for writing, under one hold of it.
fn with_digest_mut<T>(
    ctx: &Ctx,
    key: &[u8],
    f: impl FnOnce(&mut TDigest) -> T,
) -> Result<T, Vec<u8>> {
    let sketch = sketch_at(ctx, key, true, false)?;
    match &mut *sketch.write() {
        Sketch::TDigest(t) => Ok(f(t)),
        #[allow(unreachable_patterns)]
        _ => Err(resp_error(WRONGTYPE)),
    }
}

fn insert(ctx: &Ctx, key: &[u8], t: TDigest) {
    debug_assert!(!crate::sketch::room::asking());
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    map.insert(
        Key::from(key),
        KiviDBValue::Sketch(Arc::new(parking_lot::RwLock::new(Sketch::TDigest(t)))),
    );
}

// ── Commands ─────────────────────────────────────────────────────────────

/// `TDIGEST.CREATE key [COMPRESSION compression]`
fn create(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    let argc = ctx.cmd.len();
    if argc != 2 && argc != 4 {
        return wrong_args("TDIGEST.CREATE");
    }
    // The key first, as Redis looks.
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    match digest_at(ctx, key) {
        Ok(Some(_)) => return resp_error("ERR T-Digest: key already exists"),
        Err(()) => return resp_error(WRONGTYPE),
        Ok(None) => {}
    }
    let mut c = DEFAULT_COMPRESSION;
    if argc == 4 {
        if !ctx.cmd[2].eq_ignore_ascii_case(b"COMPRESSION") {
            return resp_error("ERR T-Digest: wrong keyword");
        }
        c = match compression(ctx.cmd[3]) {
            Ok(c) => c,
            Err(e) => return e,
        };
    }
    if let Err(e) = room(ctx, capacity_of(c) as u64 * 16) {
        return e;
    }
    insert(ctx, key, TDigest::new(c));
    resp_simple("OK")
}

/// A value `TDIGEST.ADD` takes: finite.
fn value(raw: &[u8]) -> Result<f64, Vec<u8>> {
    // A number too large for a double (`1e400`) is one Redis cannot parse;
    // `inf` itself is one it parses and refuses.
    let spelled_inf = raw
        .strip_prefix(b"-")
        .or_else(|| raw.strip_prefix(b"+"))
        .unwrap_or(raw)
        .get(..3)
        .is_some_and(|w| w.eq_ignore_ascii_case(b"inf"));
    match parse_double(raw) {
        Some(v) if !v.is_finite() && spelled_inf => Err(resp_error(
            "ERR T-Digest: val parameter needs to be a finite number",
        )),
        Some(v) if v.is_finite() => Ok(v),
        _ => Err(resp_error("ERR T-Digest: error parsing val parameter")),
    }
}

/// `TDIGEST.ADD key value [value ...]`: every value read before any is
/// added.
fn add(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    if ctx.cmd.len() < 3 {
        return wrong_args("TDIGEST.ADD");
    }
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    let values = &ctx.cmd[2..];
    // The one hold of the digest: its type, then the values, read before
    // any is added.
    match with_digest_mut(ctx, key, |t| {
        let mut read = Vec::with_capacity(values.len());
        for raw in values {
            read.push(value(raw)?);
        }
        for v in read {
            t.add(v, 1.0);
        }
        Ok(())
    }) {
        Ok(Ok(())) => resp_simple("OK"),
        Ok(Err(e)) | Err(e) => e,
    }
}

/// `TDIGEST.RESET key`
fn reset(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    if ctx.cmd.len() != 2 {
        return wrong_args("TDIGEST.RESET");
    }
    TOUCHED.set(true);
    match with_digest_mut(ctx, ctx.cmd[1], TDigest::reset) {
        Ok(()) => resp_simple("OK"),
        Err(e) => e,
    }
}

/// `TDIGEST.MERGE destination numkeys source [source ...] [COMPRESSION
/// compression] [OVERRIDE]`
fn merge(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    let argc = ctx.cmd.len();
    if argc < 4 {
        return wrong_args("TDIGEST.MERGE");
    }
    // The destination's type before anything else, as Redis checks it.
    let dest_key = ctx.cmd[1];
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, dest_key);
    if digest_at(ctx, dest_key).is_err() {
        return resp_error(WRONGTYPE);
    }
    let n = match parse_ll(ctx.cmd[2]) {
        Some(n) if n > 0 => n as u64,
        None => return resp_error("ERR T-Digest: error parsing numkeys"),
        _ => return resp_error("ERR T-Digest: numkeys needs to be a positive integer"),
    };
    if (argc as u64) < 3 + n {
        return wrong_args("TDIGEST.MERGE");
    }
    let n = n as usize;
    let mut given = None;
    let mut override_ = false;
    let mut at = 3 + n;
    while at < argc {
        let word = ctx.cmd[at];
        if word.eq_ignore_ascii_case(b"COMPRESSION") {
            let Some(raw) = ctx.cmd.get(at + 1) else {
                return wrong_args("TDIGEST.MERGE");
            };
            given = match compression(raw) {
                Ok(c) => Some(c),
                Err(e) => return e,
            };
            at += 2;
        } else if word.eq_ignore_ascii_case(b"OVERRIDE") {
            // Redis reads nothing past it.
            override_ = true;
            break;
        } else {
            return resp_error("ERR T-Digest: wrong keyword");
        }
    }
    let mut sources = Vec::with_capacity(n);
    let mut work = 0u64;
    for key in &ctx.cmd[3..3 + n] {
        match digest_at(ctx, key) {
            Ok(Some(s)) => {
                let copy = match &*s.read() {
                    Sketch::TDigest(t) => t.clone(),
                    #[allow(unreachable_patterns)]
                    _ => return resp_error(WRONGTYPE),
                };
                work = work.saturating_add((copy.merged.len() + copy.unmerged.len()) as u64);
                if !ctx.apply_replication && work > MERGE_BUDGET {
                    return resp_error(OVER_BUDGET);
                }
                sources.push(copy);
            }
            Ok(None) => return resp_error(NO_KEY),
            Err(()) => return resp_error(WRONGTYPE),
        }
    }
    // Redis opens the destination for writing once the sources are read:
    // a `WATCH` on it is invalidated from here.
    TOUCHED.set(true);
    let dest = match digest_at(ctx, dest_key) {
        Ok(d) => d,
        Err(()) => return resp_error(WRONGTYPE),
    };
    // A new digest, or one written over, has the compression given, or the
    // largest of the sources'.
    let fresh = || {
        TDigest::new(given.unwrap_or_else(|| {
            sources
                .iter()
                .map(|s| s.compression)
                .max()
                .unwrap_or(DEFAULT_COMPRESSION)
        }))
    };
    // The result is a new digest, as in Redis: of the compression given;
    // else the destination's, if it is kept (no `OVERRIDE`); else the
    // largest of the sources'. The destination's values, if kept, then the
    // sources'.
    let kept = match dest {
        Some(d) if !override_ => match &*d.read() {
            Sketch::TDigest(t) => Some(t.clone()),
            #[allow(unreachable_patterns)]
            _ => return resp_error(WRONGTYPE),
        },
        _ => None,
    };
    let mut t = fresh();
    if given.is_none()
        && let Some(k) = &kept
    {
        t = TDigest::new(k.compression);
    }
    if let Err(e) = room(ctx, t.capacity() as u64 * 16) {
        return e;
    }
    if let Some(k) = &kept {
        t.add_digest(k);
    }
    for s in &sources {
        t.add_digest(s);
    }
    insert(ctx, dest_key, t);
    resp_simple("OK")
}

/// `TDIGEST.MIN key` and `TDIGEST.MAX key`: `nan` for an empty digest.
fn min_max(ctx: &mut Ctx, min: bool) -> Vec<u8> {
    let name = if min { "TDIGEST.MIN" } else { "TDIGEST.MAX" };
    if ctx.cmd.len() != 2 {
        return wrong_args(name);
    }
    let resp = resp(ctx);
    match with_digest(ctx, ctx.cmd[1], |t| {
        if t.is_empty() {
            f64::NAN
        } else if min {
            t.min
        } else {
            t.max
        }
    }) {
        Ok(v) => double(resp, v),
        Err(e) => e,
    }
}

/// The values of a query, each read as Redis reads them, and the work it
/// may do held to the per-command limit.
fn query_values(
    ctx: &Ctx,
    what: &str,
    check: impl Fn(f64) -> Option<Vec<u8>>,
) -> Result<Vec<f64>, Vec<u8>> {
    let mut out = Vec::with_capacity(ctx.cmd.len() - 2);
    for raw in &ctx.cmd[2..] {
        let Some(v) = parse_double(raw) else {
            return Err(resp_error(&format!("ERR T-Digest: error parsing {what}")));
        };
        if let Some(e) = check(v) {
            return Err(e);
        }
        out.push(v);
    }
    Ok(out)
}

fn budget(ctx: &Ctx, t: &TDigest, values: usize) -> Result<(), Vec<u8>> {
    let work = (values as u64 + 1).saturating_mul((t.merged.len() + t.unmerged.len()) as u64 + 1);
    if !ctx.apply_replication && work > BUDGET {
        Err(resp_error(OVER_BUDGET))
    } else {
        Ok(())
    }
}

/// `TDIGEST.QUANTILE key quantile [quantile ...]`
fn quantile(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("TDIGEST.QUANTILE");
    }
    let resp = resp(ctx);
    let key = ctx.cmd[1];
    let probe = probe_digest(ctx, key);
    if let Err(e) = probe {
        return e;
    }
    let qs = match query_values(ctx, "quantile", |q| {
        (!(0.0..=1.0).contains(&q)).then(|| resp_error("ERR T-Digest: quantile should be in [0,1]"))
    }) {
        Ok(qs) => qs,
        Err(e) => return e,
    };
    match with_digest(ctx, key, |t| {
        budget(ctx, t, qs.len())?;
        Ok(t.quantiles(&qs))
    }) {
        Ok(Ok(vs)) => resp_array(vs.into_iter().map(|v| double(resp, v)).collect()),
        Ok(Err(e)) | Err(e) => e,
    }
}

/// `TDIGEST.CDF key value [value ...]`
fn cdf(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("TDIGEST.CDF");
    }
    let resp = resp(ctx);
    let key = ctx.cmd[1];
    if let Err(e) = probe_digest(ctx, key) {
        return e;
    }
    let xs = match query_values(ctx, "cdf", |_| None) {
        Ok(xs) => xs,
        Err(e) => return e,
    };
    match with_digest(ctx, key, |t| {
        budget(ctx, t, xs.len())?;
        Ok(t.cdfs(&xs))
    }) {
        Ok(Ok(vs)) => resp_array(vs.into_iter().map(|v| double(resp, v)).collect()),
        Ok(Err(e)) | Err(e) => e,
    }
}

/// `TDIGEST.RANK key value [value ...]` and `TDIGEST.REVRANK`: -1 below the
/// least value (above the greatest for `REVRANK`), the count past the
/// other end, -2 for an empty digest.
fn rank(ctx: &mut Ctx, rev: bool) -> Vec<u8> {
    let name = if rev {
        "TDIGEST.REVRANK"
    } else {
        "TDIGEST.RANK"
    };
    if ctx.cmd.len() < 3 {
        return wrong_args(name);
    }
    let key = ctx.cmd[1];
    if let Err(e) = probe_digest(ctx, key) {
        return e;
    }
    let xs = match query_values(ctx, "value", |_| None) {
        Ok(xs) => xs,
        Err(e) => return e,
    };
    match with_digest(ctx, key, |t| {
        budget(ctx, t, xs.len())?;
        let total = t.total_weight();
        if t.is_empty() {
            return Ok(vec![-2i64; xs.len()]);
        }
        let cdfs = t.cdfs(&xs);
        Ok(xs
            .iter()
            .zip(cdfs)
            .map(|(&x, c)| {
                if !rev {
                    if x < t.min {
                        -1
                    } else if x > t.max {
                        total as i64
                    } else {
                        (c * total).floor() as i64
                    }
                } else if x > t.max {
                    -1
                } else if x < t.min {
                    total as i64
                } else {
                    // The weight above, as a count less the rank: not
                    // `(1 - c) · total`, which rounds below a whole count.
                    (total - c * total).floor() as i64
                }
            })
            .collect())
    }) {
        Ok(Ok(vs)) => resp_array(vs.into_iter().map(resp_integer).collect()),
        Ok(Err(e)) | Err(e) => e,
    }
}

/// `TDIGEST.BYRANK key rank [rank ...]` and `TDIGEST.BYREVRANK`: `inf`
/// (`-inf`) past the last rank, `nan` for an empty digest.
fn byrank(ctx: &mut Ctx, rev: bool) -> Vec<u8> {
    let name = if rev {
        "TDIGEST.BYREVRANK"
    } else {
        "TDIGEST.BYRANK"
    };
    if ctx.cmd.len() < 3 {
        return wrong_args(name);
    }
    let resp = resp(ctx);
    let key = ctx.cmd[1];
    if let Err(e) = probe_digest(ctx, key) {
        return e;
    }
    let mut ranks = Vec::with_capacity(ctx.cmd.len() - 2);
    for raw in &ctx.cmd[2..] {
        match parse_ll(raw) {
            None => return resp_error("ERR T-Digest: error parsing rank"),
            Some(r) if r < 0 => return resp_error("ERR T-Digest: rank needs to be non negative"),
            Some(r) => ranks.push(r as u64),
        }
    }
    match with_digest(ctx, key, |t| {
        budget(ctx, t, ranks.len())?;
        let total = t.total_weight();
        if t.is_empty() {
            return Ok(vec![f64::NAN; ranks.len()]);
        }
        let qs: Vec<f64> = ranks
            .iter()
            .map(|&r| {
                if rev {
                    (total - 1.0 - r as f64) / total
                } else {
                    r as f64 / total
                }
            })
            .collect();
        let vs = t.quantiles(&qs.iter().map(|q| q.clamp(0.0, 1.0)).collect::<Vec<_>>());
        Ok(ranks
            .iter()
            .zip(vs)
            .map(|(&r, v)| {
                if r as f64 >= total {
                    if rev {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    }
                } else {
                    v
                }
            })
            .collect())
    }) {
        Ok(Ok(vs)) => resp_array(vs.into_iter().map(|v| double(resp, v)).collect()),
        Ok(Err(e)) | Err(e) => e,
    }
}

/// `TDIGEST.TRIMMED_MEAN key low_cut_quantile high_cut_quantile`
fn trimmed_mean(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 4 {
        return wrong_args("TDIGEST.TRIMMED_MEAN");
    }
    let resp = resp(ctx);
    let key = ctx.cmd[1];
    if let Err(e) = probe_digest(ctx, key) {
        return e;
    }
    let Some(lo) = parse_double(ctx.cmd[2]) else {
        return resp_error("ERR T-Digest: error parsing low_cut_percentile");
    };
    let Some(hi) = parse_double(ctx.cmd[3]) else {
        return resp_error("ERR T-Digest: error parsing high_cut_percentile");
    };
    if !(0.0..=1.0).contains(&lo) || !(0.0..=1.0).contains(&hi) {
        return resp_error(
            "ERR T-Digest: low_cut_percentile and high_cut_percentile should be in [0,1]",
        );
    }
    if lo >= hi {
        return resp_error(
            "ERR T-Digest: low_cut_percentile should be lower than high_cut_percentile",
        );
    }
    match with_digest(ctx, key, |t| t.trimmed_mean(lo, hi)) {
        Ok(v) => double(resp, v),
        Err(e) => e,
    }
}

/// `TDIGEST.INFO key`
fn info(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("TDIGEST.INFO");
    }
    let resp = resp(ctx);
    let n = |v: u64| resp_integer(i64::try_from(v).unwrap_or(i64::MAX));
    match with_digest(ctx, ctx.cmd[1], |t| {
        resp_map_for(
            resp,
            vec![
                resp_simple("Compression"),
                n(t.compression),
                resp_simple("Capacity"),
                n(t.capacity() as u64),
                resp_simple("Merged nodes"),
                n(t.merged.len() as u64),
                resp_simple("Unmerged nodes"),
                n(t.unmerged.len() as u64),
                resp_simple("Merged weight"),
                n(t.merged_weight as u64),
                resp_simple("Unmerged weight"),
                n(t.unmerged_weight as u64),
                resp_simple("Observations"),
                n(t.total_weight() as u64),
                resp_simple("Total compressions"),
                n(t.total_compressions),
                resp_simple("Memory usage"),
                n(info_memory(t.compression)),
            ],
        )
    }) {
        Ok(r) => r,
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arities_as_redis_reports_them() {
        assert_eq!(arity("TDIGEST.CREATE"), Some(-2));
        assert_eq!(arity("TDIGEST.MERGE"), Some(-4));
        assert_eq!(arity("TDIGEST.TRIMMED_MEAN"), Some(4));
        assert_eq!(arity("TDIGEST.INFO"), Some(2));
        assert_eq!(arity("TDIGEST.NOPE"), None);
    }
}
