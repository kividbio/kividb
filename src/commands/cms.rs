//! `CMS.*`: count-min sketches, as Redis 8 has them built in (formerly
//! RedisBloom). The sketch itself is `sketch::cms`.
//!
//! The replies and errors are Redis 8.10's, in RESP2 and RESP3, with the
//! order in which Redis checks a command's arguments: `CMS.INITBYDIM` and
//! `CMS.INITBYPROB` look at the key before the numbers (`key already
//! exists` for a key of any type); `CMS.INCRBY` and `CMS.MERGE` look up the
//! key (the destination) before they read anything else. Counts are
//! Redis's exactly, collisions and all, since items are placed as Redis
//! places them.
//!
//! A write that found its key invalidates a `WATCH` on it even when it
//! fails, as Redis's do (see [`take_touched`]). No keyspace events, as in
//! Redis.
//!
//! Beyond Redis: a sketch is held to `sketch::cms`'s size limits, a command
//! to a per-command amount of work, checked before it starts, and a
//! command from a client that would allocate more than `maxmemory` leaves
//! room for evicts first, under the policy, and is refused with `OOM` only
//! if that is not enough. A command from a master or the AOF is held to
//! neither `maxmemory` nor the per-command limits: it must do what the
//! master did. (The size limits are fixed, so the master never made what a
//! replica would refuse.)

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::commands::bf::{parse_double, parse_ll};
use crate::commands::ctx::Ctx;
use crate::resp::write::{resp_array, resp_error, resp_integer, resp_map_for, resp_simple};
use crate::sketch::Sketch;
use crate::sketch::cms::{
    Added, CmsError, CountMin, ITEM_BUDGET, MAX_COUNTERS, MAX_DEPTH, MERGE_BUDGET, dims_for, merged,
};
use crate::store::{self, Key, ValueRef};
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NO_KEY: &str = "CMS: key does not exist";
const EXISTS: &str = "CMS: key already exists";
const NO_MEMORY: &str = "CMS: Insufficient memory to create the key";
const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";
const OVER_BUDGET: &str =
    "CMS: command reached the per-command limit; send fewer items per command";

fn too_large() -> Vec<u8> {
    resp_error(&format!(
        "CMS: sketch is too large (at most {MAX_COUNTERS} counters and {MAX_DEPTH} rows)"
    ))
}

fn cms_error(e: CmsError) -> Vec<u8> {
    match e {
        CmsError::TooLarge => too_large(),
        CmsError::NoMemory => resp_error(NO_MEMORY),
    }
}

/// Each command's arity, as Redis's `COMMAND` gives it, for `COMMAND INFO`
/// alone: the handlers check their own arguments.
pub(crate) fn arity(name: &str) -> Option<i16> {
    Some(match name {
        "CMS.INITBYDIM" | "CMS.INITBYPROB" => 4,
        "CMS.INCRBY" | "CMS.MERGE" => -4,
        "CMS.QUERY" => -3,
        "CMS.INFO" => 2,
        _ => return None,
    })
}

thread_local! {
    /// Whether the running write got as far as its key.
    static TOUCHED: Cell<bool> = const { Cell::new(false) };
}

/// Whether the `CMS.*` write that just failed had got as far as its key,
/// so that a `WATCH` on the key is invalidated, as Redis invalidates it for
/// a write that opened its key whatever came of it. Redis opens the key of
/// every `CMS.*` write with the right number of arguments, there or not, so
/// every such write is.
pub(crate) fn take_touched() -> bool {
    TOUCHED.replace(false)
}

fn wrong_args(name: &str) -> Vec<u8> {
    resp_error(&format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    ))
}

/// Room under `maxmemory` for `bytes` more, for a command from a client:
/// keys are evicted first as the policy allows; `OOM` if that is not
/// enough. Before any lock is taken, as eviction takes them. Nothing for a
/// command from a master or the AOF, nor on a replica.
fn make_room(ctx: &mut Ctx, bytes: u64) -> Result<(), Vec<u8>> {
    let from_client = !ctx.apply_replication && !ctx.repl_state.is_slave();
    let limit = crate::RUNTIME_MAXMEMORY.load(Ordering::Relaxed) as u64;
    if from_client && limit != 0 && bytes != 0 && !crate::sketch::room_for(bytes) {
        let made = limit
            .checked_sub(bytes)
            .is_some_and(|room| crate::eviction::make_room(ctx, room as usize, true));
        if !made {
            return Err(resp_error(OOM));
        }
    }
    Ok(())
}

/// Whether the per-command limits apply: not to a command from a master
/// or the AOF.
fn budgeted(ctx: &Ctx) -> bool {
    !ctx.apply_replication
}

// ── Dispatch ─────────────────────────────────────────────────────────────

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("CMS.") {
        return None;
    }
    Some(match name {
        "CMS.INITBYDIM" => initbydim(ctx),
        "CMS.INITBYPROB" => initbyprob(ctx),
        "CMS.INCRBY" => incrby(ctx),
        "CMS.QUERY" => query(ctx),
        "CMS.MERGE" => merge(ctx),
        "CMS.INFO" => info(ctx),
        _ => return None,
    })
}

/// A copy of the sketch at `key` (its counters shared, so nothing is
/// copied): `Ok(None)` if there is no key, `Err(())` if it holds another
/// type.
fn sketch_at(ctx: &Ctx, key: &[u8]) -> Result<Option<CountMin>, ()> {
    if crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return Ok(None);
    }
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Ok(None),
        Some(ValueRef::Sketch(sketch)) => match &*sketch.read() {
            Sketch::CountMin(cms) => Ok(Some(cms.clone())),
            #[allow(unreachable_patterns)]
            _ => Err(()),
        },
        Some(_) => Err(()),
    }
}

/// Run `f` on the sketch at `key` under its lock, for reading.
fn with_sketch<T>(ctx: &Ctx, key: &[u8], f: impl FnOnce(&CountMin) -> T) -> Result<T, Vec<u8>> {
    if crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return Err(resp_error(NO_KEY));
    }
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Err(resp_error(NO_KEY)),
        Some(ValueRef::Sketch(sketch)) => match &*sketch.read() {
            Sketch::CountMin(cms) => Ok(f(cms)),
            #[allow(unreachable_patterns)]
            _ => Err(resp_error(WRONGTYPE)),
        },
        Some(_) => Err(resp_error(WRONGTYPE)),
    }
}

/// The sketch at `key` for writing, or the error a missing key or another
/// type gets. Finding it marks the write as having got to its key.
fn sketch_for_write(ctx: &Ctx, key: &[u8]) -> Result<Arc<parking_lot::RwLock<Sketch>>, Vec<u8>> {
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Err(resp_error(NO_KEY)),
        Some(ValueRef::Sketch(sketch)) => {
            TOUCHED.set(true);
            #[allow(irrefutable_let_patterns)]
            if let Sketch::CountMin(_) = &*sketch.read() {
                Ok(Arc::clone(sketch))
            } else {
                Err(resp_error(WRONGTYPE))
            }
        }
        Some(_) => {
            TOUCHED.set(true);
            Err(resp_error(WRONGTYPE))
        }
    }
}

/// Make a sketch of `width × depth` at `key`, which the caller found
/// missing; `EXISTS` if one came meanwhile.
fn create(ctx: &mut Ctx, key: &[u8], width: u64, depth: u64) -> Vec<u8> {
    if !crate::sketch::cms::fits(width, depth) {
        return too_large();
    }
    if let Err(e) = make_room(ctx, CountMin::bytes_for(width, depth)) {
        return e;
    }
    let cms = match CountMin::new(width, depth) {
        Ok(cms) => cms,
        Err(e) => return cms_error(e),
    };
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    TOUCHED.set(true);
    if map.get(key).is_some() {
        return resp_error(EXISTS);
    }
    map.insert(
        Key::from(key),
        KiviDBValue::Sketch(Arc::new(parking_lot::RwLock::new(Sketch::CountMin(cms)))),
    );
    resp_simple("OK")
}

/// Whether there is a key at `key` (of any type); finding one marks the
/// write as having got to it.
fn key_exists(ctx: &Ctx, key: &[u8]) -> bool {
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let found = read_lock!(shard).get(key).is_some();
    if found {
        TOUCHED.set(true);
    }
    found
}

/// A positive integer as Redis reads a width or depth.
fn positive(raw: &[u8]) -> Option<u64> {
    parse_ll(raw).filter(|&v| v > 0).map(|v| v as u64)
}

// ── Commands ─────────────────────────────────────────────────────────────

/// `CMS.INITBYDIM key width depth`
fn initbydim(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    if ctx.cmd.len() != 4 {
        return wrong_args("CMS.INITBYDIM");
    }
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    if key_exists(ctx, key) {
        return resp_error(EXISTS);
    }
    let Some(width) = positive(ctx.cmd[2]) else {
        return resp_error("CMS: invalid width");
    };
    let Some(depth) = positive(ctx.cmd[3]) else {
        return resp_error("CMS: invalid depth");
    };
    create(ctx, key, width, depth)
}

/// `CMS.INITBYPROB key error probability`
fn initbyprob(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    if ctx.cmd.len() != 4 {
        return wrong_args("CMS.INITBYPROB");
    }
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    if key_exists(ctx, key) {
        return resp_error(EXISTS);
    }
    let in_range = |v: f64| v > 0.0 && v < 1.0;
    let Some(error) = parse_double(ctx.cmd[2]).filter(|&v| in_range(v)) else {
        return resp_error("CMS: invalid overestimation value");
    };
    let Some(probability) = parse_double(ctx.cmd[3]).filter(|&v| in_range(v)) else {
        return resp_error("CMS: invalid prob value");
    };
    let Some((width, depth)) = dims_for(error, probability) else {
        return resp_error("CMS: invalid init arguments");
    };
    create(ctx, key, width, depth)
}

/// `CMS.INCRBY key item increment [item increment ...]`
fn incrby(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    let argc = ctx.cmd.len();
    if argc < 4 || !argc.is_multiple_of(2) {
        return wrong_args("CMS.INCRBY");
    }
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    let sketch = match sketch_for_write(ctx, key) {
        Ok(sketch) => sketch,
        Err(e) => return e,
    };
    let mut values = Vec::with_capacity((argc - 2) / 2);
    for pair in ctx.cmd[2..].as_chunks::<2>().0 {
        match parse_ll(pair[1]) {
            None => return resp_error("CMS: Cannot parse number"),
            Some(v) if v < 0 => return resp_error("CMS: Number cannot be negative"),
            Some(v) => values.push(v as u64),
        }
    }
    let items = values.len() as u64;
    let copy = {
        let guard = sketch.read();
        let Sketch::CountMin(cms) = &*guard else {
            return resp_error(WRONGTYPE);
        };
        if budgeted(ctx) && items.saturating_mul(cms.depth) > ITEM_BUDGET {
            return resp_error(OVER_BUDGET);
        }
        cms.copy_bytes()
    };
    if let Err(e) = make_room(ctx, copy) {
        return e;
    }
    let mut guard = sketch.write();
    #[allow(irrefutable_let_patterns)]
    let Sketch::CountMin(cms) = &mut *guard else {
        return resp_error(WRONGTYPE);
    };
    let mut replies = Vec::with_capacity(values.len());
    for (pair, value) in ctx.cmd[2..].as_chunks::<2>().0.iter().zip(values) {
        match cms.incr_by(pair[0], value) {
            Ok(Added::Count(n)) => replies.push(resp_integer(i64::from(n))),
            Ok(Added::Overflow) => replies.push(resp_error("CMS: INCRBY overflow")),
            Err(e) => return cms_error(e),
        }
    }
    resp_array(replies)
}

/// `CMS.QUERY key item [item ...]`
fn query(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("CMS.QUERY");
    }
    let key = ctx.cmd[1];
    let items = &ctx.cmd[2..];
    let budgeted = budgeted(ctx);
    match with_sketch(ctx, key, |cms| {
        if budgeted && (items.len() as u64).saturating_mul(cms.depth) > ITEM_BUDGET {
            return resp_error(OVER_BUDGET);
        }
        resp_array(
            items
                .iter()
                .map(|item| resp_integer(i64::from(cms.query(item))))
                .collect(),
        )
    }) {
        Ok(reply) | Err(reply) => reply,
    }
}

/// `CMS.INFO key`
fn info(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("CMS.INFO");
    }
    let key = ctx.cmd[1];
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);
    match with_sketch(ctx, key, |cms| {
        let as_int = |v: u64| resp_integer(i64::try_from(v).unwrap_or(i64::MAX));
        resp_map_for(
            resp,
            vec![
                resp_simple("width"),
                as_int(cms.width),
                resp_simple("depth"),
                as_int(cms.depth),
                resp_simple("count"),
                // Wrapped, and shown signed, as Redis shows it.
                resp_integer(cms.count as i64),
            ],
        )
    }) {
        Ok(reply) | Err(reply) => reply,
    }
}

/// `CMS.MERGE destination numKeys source [source ...] [WEIGHTS weight
/// [weight ...]]`
fn merge(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    let argc = ctx.cmd.len();
    if argc < 4 {
        return wrong_args("CMS.MERGE");
    }
    TOUCHED.set(true);
    let dest_key = ctx.cmd[1];
    let dest = match sketch_for_write(ctx, dest_key) {
        Ok(sketch) => sketch,
        Err(e) => return e,
    };
    let Some(numkeys) = parse_ll(ctx.cmd[2]) else {
        return resp_error("CMS: invalid numkeys");
    };
    if numkeys <= 0 {
        return resp_error("CMS: Number of keys must be positive");
    }
    // As Redis reads them: `WEIGHTS` is looked for among the sources (the
    // first one, in any case); if it is there, exactly `numKeys` sources
    // come before it and as many weights after; if not, exactly `numKeys`
    // sources. Checked against the arguments there are before anything is
    // sized by `numKeys`.
    let n = numkeys as u64;
    let weights_at = ctx.cmd[3..]
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"WEIGHTS"))
        .map(|i| i + 3);
    match weights_at {
        Some(at) if (at - 3) as u64 != n || (argc - at - 1) as u64 != n => {
            return resp_error("CMS: wrong number of keys/weights");
        }
        None if argc as u64 != 3 + n => return resp_error("CMS: wrong number of keys"),
        _ => {}
    }
    let n = n as usize;
    let sources = &ctx.cmd[3..3 + n];
    let weighted = weights_at.is_some();
    let (width, depth) = {
        let guard = dest.read();
        let Sketch::CountMin(d) = &*guard else {
            return resp_error(WRONGTYPE);
        };
        (d.width, d.depth)
    };
    // As Redis goes: each source's weight, then the source.
    let mut copies = Vec::with_capacity(n);
    let mut weights = Vec::with_capacity(n);
    for (i, key) in sources.iter().enumerate() {
        if weighted {
            match parse_ll(ctx.cmd[4 + n + i]) {
                Some(v) => weights.push(v),
                None => return resp_error("CMS: invalid weight value"),
            }
        } else {
            weights.push(1);
        }
        match sketch_at(ctx, key) {
            Ok(Some(cms)) if cms.width == width && cms.depth == depth => copies.push(cms),
            Ok(Some(_)) => return resp_error("CMS: width/depth is not equal"),
            Ok(None) => return resp_error(NO_KEY),
            Err(()) => return resp_error(WRONGTYPE),
        }
    }
    let counters = width * depth;
    if budgeted(ctx) && (n as u64).saturating_mul(counters) > MERGE_BUDGET {
        return resp_error(OVER_BUDGET);
    }
    if let Err(e) = make_room(ctx, counters * 4) {
        return e;
    }
    let weighted: Vec<(&CountMin, i64)> = copies.iter().zip(weights).collect();
    let (sums, count) = match merged(&weighted) {
        Ok(Some(done)) => done,
        Ok(None) => return resp_error("CMS: MERGE overflow"),
        Err(e) => return cms_error(e),
    };
    drop(weighted);
    drop(copies);
    let mut guard = dest.write();
    #[allow(irrefutable_let_patterns)]
    let Sketch::CountMin(d) = &mut *guard else {
        return resp_error(WRONGTYPE);
    };
    if d.width != width || d.depth != depth {
        return resp_error("CMS: width/depth is not equal");
    }
    d.counters = crate::sketch::cms::Counters::from_vec(sums);
    d.count = count;
    resp_simple("OK")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arities_as_redis_reports_them() {
        assert_eq!(arity("CMS.INITBYDIM"), Some(4));
        assert_eq!(arity("CMS.MERGE"), Some(-4));
        assert_eq!(arity("CMS.QUERY"), Some(-3));
        assert_eq!(arity("CMS.INFO"), Some(2));
        assert_eq!(arity("CMS.NOPE"), None);
    }
}
