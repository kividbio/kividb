//! `TOPK.*`: Top-K sketches, as Redis 8 has them built in (formerly
//! RedisBloom). The sketch itself is `sketch::topk`.
//!
//! The replies and errors are Redis 8.10's, in RESP2 and RESP3, with the
//! order in which Redis checks a command's arguments. With a decay of 1,
//! where nothing is left to chance, every answer is Redis's; below 1 the
//! decay's chances are drawn from a generator kept in the sketch, so that
//! a replica and an AOF replay make the master's choices exactly (the
//! commands are sent as they came), and answers are as good as Redis's
//! rather than the same.
//!
//! A write invalidates a `WATCH` on its key even when it fails, as Redis's
//! do (see [`take_touched`]). No keyspace events, as in Redis.
//!
//! Beyond Redis: a sketch is held to `sketch::topk`'s size limits, and a
//! command to a per-command amount of work, reckoned before it starts.
//! Under `maxmemory`, a client's `TOPK.RESERVE` that would not fit evicts
//! first, under the policy, and is refused with `OOM` only if that is not
//! enough. A command from a master or the AOF is held to neither the
//! per-command limit nor `maxmemory`.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::commands::bf::{parse_double, parse_ll};
use crate::commands::ctx::Ctx;
use crate::resp::write::{
    resp_array, resp_bool_resp3, resp_bulk, resp_double_for, resp_error, resp_integer,
    resp_map_for, resp_null_for, resp_simple,
};
use crate::sketch::Sketch;
use crate::sketch::topk::{
    BUDGET, MAX_BUCKETS, MAX_DEPTH, MAX_INCREMENT, MAX_K, TopK, TopKError, cost, decay_ok,
};
use crate::store::{self, Key, ValueRef};
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NO_KEY: &str = "TopK: key does not exist";
const EXISTS: &str = "TopK: key already exists";
const NO_MEMORY: &str = "TopK: Insufficient memory to create the key";
const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";
const OVER_BUDGET: &str =
    "TopK: command reached the per-command limit; send fewer items per command";
/// Redis's words, spaces and all.
const BAD_INCREMENT: &str = "TopK: increment must be an integer greater or equal to 0                            and smaller or equal to 100,000";

fn too_large() -> Vec<u8> {
    resp_error(&format!(
        "TopK: sketch is too large (at most k {MAX_K}, {MAX_BUCKETS} buckets and {MAX_DEPTH} rows)"
    ))
}

fn topk_error(e: TopKError) -> Vec<u8> {
    match e {
        TopKError::TooLarge => too_large(),
        TopKError::NoMemory => resp_error(NO_MEMORY),
    }
}

/// Each command's arity, as Redis's `COMMAND` gives it, for `COMMAND INFO`
/// alone: the handlers check their own arguments.
pub(crate) fn arity(name: &str) -> Option<i16> {
    Some(match name {
        "TOPK.ADD" | "TOPK.QUERY" | "TOPK.COUNT" | "TOPK.RESERVE" => -3,
        "TOPK.INCRBY" => -4,
        "TOPK.LIST" => -2,
        "TOPK.INFO" => 2,
        _ => return None,
    })
}

thread_local! {
    /// Whether the running write got as far as its key.
    static TOUCHED: Cell<bool> = const { Cell::new(false) };
}

/// Whether the `TOPK.*` write that just failed had got as far as its key,
/// so that a `WATCH` on it is invalidated: Redis opens the key of every
/// such write with the right number of arguments, there or not.
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

/// Room under `maxmemory` for `bytes` more, for a command from a client:
/// keys are evicted first as the policy allows; `OOM` if that is not
/// enough. Nothing for a command from a master or the AOF, nor on a
/// replica.
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

// ── Dispatch ─────────────────────────────────────────────────────────────

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if !name.starts_with("TOPK.") {
        return None;
    }
    Some(match name {
        "TOPK.RESERVE" => reserve(ctx),
        "TOPK.ADD" => add(ctx),
        "TOPK.INCRBY" => incrby(ctx),
        "TOPK.QUERY" => query(ctx),
        "TOPK.COUNT" => count(ctx),
        "TOPK.LIST" => list(ctx),
        "TOPK.INFO" => info(ctx),
        _ => return None,
    })
}

/// Run `f` on the sketch at `key` under its lock, for reading.
fn with_sketch<T>(ctx: &Ctx, key: &[u8], f: impl FnOnce(&TopK) -> T) -> Result<T, Vec<u8>> {
    if crate::commands::helpers::lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return Err(resp_error(NO_KEY));
    }
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Err(resp_error(NO_KEY)),
        Some(ValueRef::Sketch(sketch)) => match &*sketch.read() {
            Sketch::TopK(t) => Ok(f(t)),
            #[allow(unreachable_patterns)]
            _ => Err(resp_error(WRONGTYPE)),
        },
        Some(_) => Err(resp_error(WRONGTYPE)),
    }
}

/// The sketch at `key` for writing, or the error a missing key or another
/// type gets.
fn sketch_for_write(ctx: &Ctx, key: &[u8]) -> Result<Arc<parking_lot::RwLock<Sketch>>, Vec<u8>> {
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Err(resp_error(NO_KEY)),
        Some(ValueRef::Sketch(sketch)) => match &*sketch.read() {
            Sketch::TopK(_) => Ok(Arc::clone(sketch)),
            #[allow(unreachable_patterns)]
            _ => Err(resp_error(WRONGTYPE)),
        },
        Some(_) => Err(resp_error(WRONGTYPE)),
    }
}

// ── Commands ─────────────────────────────────────────────────────────────

/// `TOPK.RESERVE key topk [width depth decay]`: width 8, depth 7 and decay
/// 0.9 unless given, as in Redis.
fn reserve(ctx: &mut Ctx) -> Vec<u8> {
    TOUCHED.set(false);
    let argc = ctx.cmd.len();
    if argc != 3 && argc != 6 {
        return wrong_args("TOPK.RESERVE");
    }
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    // The key first, of any type, as Redis looks.
    crate::commands::helpers::lazy_expire_key(ctx.store, *ctx.selected_db, key);
    {
        let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
        if read_lock!(shard).get(key).is_some() {
            return resp_error(EXISTS);
        }
    }
    let in_u32 = |raw: &[u8]| parse_ll(raw).filter(|v| (1..=i64::from(u32::MAX)).contains(v));
    let Some(k) = in_u32(ctx.cmd[2]) else {
        return resp_error("TopK: invalid k");
    };
    let (mut width, mut depth, mut decay) = (8, 7, 0.9);
    if argc == 6 {
        let Some(w) = in_u32(ctx.cmd[3]) else {
            return resp_error("TopK: invalid width");
        };
        let Some(d) = in_u32(ctx.cmd[4]) else {
            return resp_error("TopK: invalid depth");
        };
        let Some(y) = parse_double(ctx.cmd[5]).filter(|&y| decay_ok(y)) else {
            return resp_error("TopK: invalid decay value. must be '<= 1' & '> 0'");
        };
        (width, depth, decay) = (w, d, y);
    }
    let (k, width, depth) = (k as u64, width as u64, depth as u64);
    if !crate::sketch::topk::fits(k, width, depth) {
        return too_large();
    }
    if let Err(e) = make_room(ctx, TopK::bytes_for(k, width, depth)) {
        return e;
    }
    let sketch = match TopK::new(k, width, depth, decay) {
        Ok(t) => t,
        Err(e) => return topk_error(e),
    };
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut map = write_lock!(shard);
    if map.get(key).is_some() {
        return resp_error(EXISTS);
    }
    map.insert(
        Key::from(key),
        KiviDBValue::Sketch(Arc::new(parking_lot::RwLock::new(Sketch::TopK(sketch)))),
    );
    resp_simple("OK")
}

/// The items' increments, each a reply of its own if it is not one Redis
/// takes.
fn increments(ctx: &Ctx, pairs: bool) -> Vec<Option<u32>> {
    if !pairs {
        return vec![Some(1); ctx.cmd.len() - 2];
    }
    ctx.cmd[2..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[_, raw]| {
            parse_ll(raw)
                .filter(|v| (0..=MAX_INCREMENT).contains(v))
                .map(|v| v as u32)
        })
        .collect()
}

/// `TOPK.ADD key item [item ...]` and `TOPK.INCRBY key item increment
/// [item increment ...]`: for each item, the item the heap let go of for
/// it, or nil; an increment Redis does not take is answered with its error
/// in that item's place.
fn add_items(ctx: &mut Ctx, name: &str, pairs: bool) -> Vec<u8> {
    TOUCHED.set(false);
    let argc = ctx.cmd.len();
    if argc < 3 || (pairs && (argc < 4 || !argc.is_multiple_of(2))) {
        return wrong_args(name);
    }
    TOUCHED.set(true);
    let key = ctx.cmd[1];
    let resp = resp(ctx);
    let sketch = match sketch_for_write(ctx, key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let incs = increments(ctx, pairs);
    let copy = {
        let guard = sketch.read();
        let Sketch::TopK(t) = &*guard else {
            return resp_error(WRONGTYPE);
        };
        if !ctx.apply_replication {
            let total = incs.iter().fold(0u64, |acc, inc| {
                acc.saturating_add(cost(t.k, t.depth, u64::from(inc.unwrap_or(0))))
            });
            if total > BUDGET {
                return resp_error(OVER_BUDGET);
            }
        }
        t.copy_bytes()
    };
    if let Err(e) = make_room(ctx, copy) {
        return e;
    }
    let step = if pairs { 2 } else { 1 };
    let mut guard = sketch.write();
    let Sketch::TopK(t) = &mut *guard else {
        return resp_error(WRONGTYPE);
    };
    // Redis takes the items in order up to one with an increment it does
    // not take, answers that one with the error and stops (and sends no
    // more of the reply it began, so that a client waits for the rest).
    // Here the items from that one on are each answered with the error.
    let n = incs.len();
    let mut replies = Vec::with_capacity(n);
    for (i, inc) in incs.into_iter().enumerate() {
        let item = ctx.cmd[2 + i * step];
        let Some(inc) = inc else {
            replies.resize(n, resp_error(BAD_INCREMENT));
            break;
        };
        match t.incr_by(item, inc) {
            Ok(Some(expelled)) => replies.push(resp_bulk(&expelled)),
            Ok(None) => replies.push(resp_null_for(resp)),
            Err(e) => return topk_error(e),
        }
    }
    resp_array(replies)
}

fn add(ctx: &mut Ctx) -> Vec<u8> {
    add_items(ctx, "TOPK.ADD", false)
}

fn incrby(ctx: &mut Ctx) -> Vec<u8> {
    add_items(ctx, "TOPK.INCRBY", true)
}

/// `TOPK.QUERY key item [item ...]`
fn query(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("TOPK.QUERY");
    }
    let resp = resp(ctx);
    let key = ctx.cmd[1];
    let items = &ctx.cmd[2..];
    let budgeted = !ctx.apply_replication;
    match with_sketch(ctx, key, |t| {
        if budgeted && (items.len() as u64).saturating_mul(1 + u64::from(t.k) / 16) > BUDGET {
            return resp_error(OVER_BUDGET);
        }
        resp_array(
            items
                .iter()
                .map(|item| {
                    let found = t.query(item);
                    if resp >= 3 {
                        resp_bool_resp3(found)
                    } else {
                        resp_integer(i64::from(found))
                    }
                })
                .collect(),
        )
    }) {
        Ok(r) | Err(r) => r,
    }
}

/// `TOPK.COUNT key item [item ...]`
fn count(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("TOPK.COUNT");
    }
    let key = ctx.cmd[1];
    let items = &ctx.cmd[2..];
    let budgeted = !ctx.apply_replication;
    match with_sketch(ctx, key, |t| {
        let each = u64::from(t.depth) + u64::from(t.k) / 16 + 1;
        if budgeted && (items.len() as u64).saturating_mul(each) > BUDGET {
            return resp_error(OVER_BUDGET);
        }
        resp_array(
            items
                .iter()
                .map(|item| resp_integer(i64::from(t.count(item))))
                .collect(),
        )
    }) {
        Ok(r) | Err(r) => r,
    }
}

/// `TOPK.LIST key [WITHCOUNT]`
fn list(ctx: &mut Ctx) -> Vec<u8> {
    let argc = ctx.cmd.len();
    if !(2..=3).contains(&argc) {
        return wrong_args("TOPK.LIST");
    }
    let key = ctx.cmd[1];
    let with_count = argc == 3;
    // The word before the key, as Redis checks them.
    if with_count && !ctx.cmd[2].eq_ignore_ascii_case(b"WITHCOUNT") {
        return resp_error("WITHCOUNT keyword expected");
    }
    let listed = match with_sketch(ctx, key, |t| t.list()) {
        Ok(listed) => listed,
        Err(e) => return e,
    };
    let mut out = Vec::with_capacity(listed.len() * if with_count { 2 } else { 1 });
    for (item, n) in listed {
        out.push(resp_bulk(&item));
        if with_count {
            out.push(resp_integer(i64::from(n)));
        }
    }
    resp_array(out)
}

/// `TOPK.INFO key`
fn info(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("TOPK.INFO");
    }
    let key = ctx.cmd[1];
    let resp = resp(ctx);
    match with_sketch(ctx, key, |t| {
        resp_map_for(
            resp,
            vec![
                resp_simple("k"),
                resp_integer(i64::from(t.k)),
                resp_simple("width"),
                resp_integer(i64::from(t.width)),
                resp_simple("depth"),
                resp_integer(i64::from(t.depth)),
                resp_simple("decay"),
                resp_double_for(resp, t.decay),
            ],
        )
    }) {
        Ok(r) | Err(r) => r,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arities_as_redis_reports_them() {
        assert_eq!(arity("TOPK.RESERVE"), Some(-3));
        assert_eq!(arity("TOPK.INCRBY"), Some(-4));
        assert_eq!(arity("TOPK.LIST"), Some(-2));
        assert_eq!(arity("TOPK.INFO"), Some(2));
        assert_eq!(arity("TOPK.NOPE"), None);
    }
}
