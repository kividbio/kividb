//! Array command handlers — the Redis 8.8 `AR*` command family.
//!
//! Arrays are sparse, index-addressed collections of string values with an
//! insert cursor. See [`crate::types::ArrayInner`] for the storage model and why
//! it's a `BTreeMap` rather than Redis's slice directory.
//!
//! Two spec ambiguities resolved here, both documented in `docs/REDIS_8_8_COMMANDS.md`:
//!
//!   - `ARSCAN` / `ARGREP WITHVALUES` reply with `[idx, val]` pairs, as Redis
//!     8.10 does (and the pages' examples show). They used to be flat, as the
//!     pages' "Return information" sections said before 8.10.
//!   - `ARGETRANGE` is the one range command that materialises empty slots as
//!     nils, so its reply size is the numeric span, not the element count. A
//!     span guard ([`MAX_MATERIALISED_RANGE`]) rejects requests that would
//!     otherwise let `ARGETRANGE key 0 9223372036854775806` exhaust memory.

use crate::commands::ctx::Ctx;
use crate::commands::helpers::{ERR_VALUE_NOT_INTEGER, glob_match, parse_utf8};
use crate::resp::write::{
    resp_array, resp_bulk, resp_bulk_str, resp_error, resp_integer, resp_map, resp_null_bulk,
    resp_null_for,
};
use crate::store;
use crate::store::ValueRef;
use crate::types::{ARRAY_MAX_INDEX, ArrayInner, ArraySliceShape, KiviDBValue};
use crate::{read_lock, write_lock};
use std::sync::Arc;

const ERR_WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const ERR_INDEX_RANGE: &str = "ERR index is out of range";
const ERR_CURSOR_EXHAUSTED: &str = "ERR insert index overflow";

/// Upper bound on how many slots `ARGETRANGE` will materialise in one reply.
///
/// Every other range command iterates only *existing* elements, so its cost is
/// bounded by what's actually stored. `ARGETRANGE` is specified to return a nil
/// for each empty slot in the range, which makes its reply proportional to
/// `|end - start|` — unbounded, and attacker-controlled, on a sparse array.
/// Redis's own limit here isn't documented; 1M keeps the worst-case reply in the
/// tens of MB rather than the exabytes a full-span request would need.
const MAX_MATERIALISED_RANGE: u64 = 1_000_000;

/// Redis's `ARGREP_MAX_PREDICATES` and `ARGREP_MAX_RE_LEN`.
const ARGREP_MAX_PREDICATES: usize = 250;
const ARGREP_MAX_RE_LEN: usize = 2048;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let reply = run(ctx, name)?;
    notify_array_event(ctx, name, &reply);
    Some(reply)
}

/// `notify-keyspace-events a` (Redis 8.10): the array commands that change
/// an array say so, by name; one that leaves it empty, and so deletes it,
/// says `del` after.
fn notify_array_event(ctx: &Ctx, name: &str, reply: &[u8]) {
    use crate::keyspace_events::{KS_ARRAY, KS_FLAGS, KS_GENERIC, notify};
    if KS_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0 || reply.starts_with(b"-") {
        return;
    }
    let Some(key) = ctx.cmd.get(1).and_then(|k| parse_utf8(k)) else {
        return;
    };
    let event = match name {
        "ARSET" => "arset",
        "ARMSET" => "armset",
        "ARINSERT" => "arinsert",
        "ARRING" => "arring",
        "ARSEEK" if reply == b":1\r\n" => "arseek",
        // Only when something was deleted.
        "ARDEL" | "ARDELRANGE" if !reply.starts_with(b":0\r") => {
            let event = if name == "ARDEL" {
                "ardel"
            } else {
                "ardelrange"
            };
            notify(ctx.pubsub, *ctx.selected_db, KS_ARRAY, event, key);
            let gone =
                !read_lock!(store::get_shard(&ctx.store[*ctx.selected_db], key)).contains_key(key);
            if gone {
                notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "del", key);
            }
            return;
        }
        _ => return,
    };
    notify(ctx.pubsub, *ctx.selected_db, KS_ARRAY, event, key);
}

fn run(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "ARSET" => Some(cmd_arset(ctx)),
        "ARMSET" => Some(cmd_armset(ctx)),
        "ARGET" => Some(cmd_arget(ctx)),
        "ARMGET" => Some(cmd_armget(ctx)),
        "ARGETRANGE" => Some(cmd_argetrange(ctx)),
        "ARDEL" => Some(cmd_ardel(ctx)),
        "ARDELRANGE" => Some(cmd_ardelrange(ctx)),
        "ARLEN" => Some(cmd_arlen(ctx)),
        "ARCOUNT" => Some(cmd_arcount(ctx)),
        "ARINSERT" => Some(cmd_arinsert(ctx)),
        "ARRING" => Some(cmd_arring(ctx)),
        "ARNEXT" => Some(cmd_arnext(ctx)),
        "ARSEEK" => Some(cmd_arseek(ctx)),
        "ARSCAN" => Some(cmd_arscan(ctx)),
        "ARGREP" => Some(cmd_argrep(ctx)),
        "AROP" => Some(cmd_arop(ctx)),
        "ARLASTITEMS" => Some(cmd_arlastitems(ctx)),
        "ARINFO" => Some(cmd_arinfo(ctx)),
        _ => None,
    }
}

// ── Argument helpers ─────────────────────────────────────────────────────────

/// Parse a wire argument as an array index.
///
/// Indices are documented as zero-based and non-negative. A negative value is
/// rejected rather than interpreted end-relative the way list/string commands
/// do — an array's `len` is "max index + 1" over a sparse space, so there is no
/// meaningful element for `-1` to name.
/// An index: what is not a whole number from 0 is `invalid array index`, as
/// in Redis 8.10 (which said `value is not an integer` before).
fn parse_index(raw: &[u8]) -> Result<u64, Vec<u8>> {
    parse_index_up_to(raw, u64::MAX - 1)
}

fn parse_index_up_to(raw: &[u8], max: u64) -> Result<u64, Vec<u8>> {
    parse_utf8(raw)
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|v| *v <= max)
        .ok_or_else(|| resp_error("ERR invalid array index"))
}

fn parse_count(raw: &[u8]) -> Result<i64, Vec<u8>> {
    let s = parse_utf8(raw).ok_or_else(|| resp_error(ERR_VALUE_NOT_INTEGER))?;
    s.parse().map_err(|_| resp_error(ERR_VALUE_NOT_INTEGER))
}

fn wrong_args(cmd: &str) -> Vec<u8> {
    resp_error(&format!(
        "ERR wrong number of arguments for '{}' command",
        cmd.to_lowercase()
    ))
}

/// A nil element *inside* an aggregate reply.
///
/// `resp3_normalize_null` in the dispatcher only rewrites a reply that is
/// nothing but a top-level null, so array-valued replies have to pick the right
/// encoding themselves.
///
/// Goes through `effective_resp_for_reply` rather than reading `ctx.resp`
/// directly so `redis.setresp` is honoured inside scripts, matching the rest of
/// the command surface.
#[inline]
fn nil_element(ctx: &Ctx) -> Vec<u8> {
    resp_null_for(crate::commands::handler::effective_resp_for_reply(ctx))
}

/// Run `f` against the array at `key`, read-only.
///
/// `None` key → `missing`. Wrong type → WRONGTYPE. Expired keys are swept first
/// so a logically-absent key never reads as present.
fn with_array_read<T>(
    ctx: &Ctx,
    key: &str,
    missing: T,
    f: impl FnOnce(&ArrayInner) -> T,
) -> Result<T, Vec<u8>> {
    if crate::commands::helpers::lazy_expire_on_read(
        ctx.store,
        ctx.expiry_store,
        *ctx.selected_db,
        key,
    ) {
        return Ok(missing);
    }
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => Ok(missing),
        Some(ValueRef::Array(arc)) => Ok(f(&arc.read())),
        Some(_) => Err(resp_error(ERR_WRONGTYPE)),
    }
}

/// Run `f` against the array at `key`, creating it when absent.
///
/// The key is removed again if `f` leaves it empty *and* it carries no cursor or
/// ring state — an array that has only had elements deleted still holds a
/// meaningful `next_insert`, and dropping it would silently rewind `ARNEXT`.
fn with_array_write<T>(
    ctx: &mut Ctx,
    key: &str,
    f: impl FnOnce(&mut ArrayInner) -> T,
) -> Result<T, Vec<u8>> {
    crate::commands::helpers::lazy_expire_key(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);
    let arc = match map.get_or_insert_with(key, || {
        KiviDBValue::Array(Arc::new(parking_lot::RwLock::new(ArrayInner::new())))
    }) {
        ValueRef::Array(arc) => Arc::clone(arc),
        // Made only when the key was absent, so anything but an array here
        // means the key already existed with another type.
        _ => return Err(resp_error(ERR_WRONGTYPE)),
    };
    let (out, drop_key) = {
        let mut inner = arc.write();
        let out = f(&mut inner);
        let drop_key = inner.is_empty() && inner.next_insert == 0 && inner.ring_size.is_none();
        (out, drop_key)
    };
    if drop_key {
        map.remove(key);
    }
    Ok(out)
}

/// Mutate the array at `key` only if it already exists — never creates it.
fn with_existing_array_write<T>(
    ctx: &mut Ctx,
    key: &str,
    missing: T,
    f: impl FnOnce(&mut ArrayInner) -> T,
) -> Result<T, Vec<u8>> {
    if crate::commands::helpers::lazy_expire_key(ctx.store, ctx.expiry_store, *ctx.selected_db, key)
    {
        return Ok(missing);
    }
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);
    match map.get(key) {
        None => Ok(missing),
        Some(ValueRef::Array(arc)) => {
            let arc = Arc::clone(arc);
            let (out, drop_key) = {
                let mut inner = arc.write();
                let out = f(&mut inner);
                let drop_key =
                    inner.is_empty() && inner.next_insert == 0 && inner.ring_size.is_none();
                (out, drop_key)
            };
            if drop_key {
                map.remove(key);
            }
            Ok(out)
        }
        Some(_) => Err(resp_error(ERR_WRONGTYPE)),
    }
}

/// Resolve a `(start, end)` request into an inclusive ascending span plus the
/// direction it should be reported in. `start > end` means reverse order.
fn ordered_span(start: u64, end: u64) -> (u64, u64, bool) {
    if start <= end {
        (start, end, false)
    } else {
        (end, start, true)
    }
}

/// Collect `(index, value)` for every existing element in `[lo, hi]`.
///
/// Uses the `BTreeMap` range directly, so cost is `O(log N + M)` in the number
/// of elements actually present — not the width of the span.
fn existing_in_span(inner: &ArrayInner, lo: u64, hi: u64, reverse: bool) -> Vec<(u64, Arc<[u8]>)> {
    let iter = inner.entries.range(lo..=hi);
    if reverse {
        iter.rev().map(|(&i, v)| (i, Arc::clone(v))).collect()
    } else {
        iter.map(|(&i, v)| (i, Arc::clone(v))).collect()
    }
}

// ── ARSET / ARMSET ───────────────────────────────────────────────────────────

/// `ARSET key index value [value ...]` → number of previously-empty slots filled.
fn cmd_arset(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 4 {
        return wrong_args("ARSET");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let start = match parse_index(ctx.cmd[2]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    // Values land at start, start+1, … — reject up front if the run would run
    // off the addressable end rather than writing a partial prefix.
    let n_values = (ctx.cmd.len() - 3) as u64;
    if start > ARRAY_MAX_INDEX || n_values - 1 > ARRAY_MAX_INDEX - start {
        return resp_error(ERR_INDEX_RANGE);
    }
    let values: Vec<Arc<[u8]>> = ctx.cmd[3..].iter().map(|v| Arc::from(*v)).collect();

    match with_array_write(ctx, &key, |inner| {
        if n_values > 1 {
            inner.promote_for_range(start, start + n_values - 1);
        }
        let mut new_slots = 0i64;
        for (offset, value) in values.into_iter().enumerate() {
            if inner.set(start + offset as u64, value) {
                new_slots += 1;
            }
        }
        new_slots
    }) {
        Ok(n) => resp_integer(n),
        Err(e) => e,
    }
}

/// `ARMSET key index value [index value ...]` → number of previously-empty slots filled.
fn cmd_armset(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 4 || !(ctx.cmd.len() - 2).is_multiple_of(2) {
        return wrong_args("ARMSET");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    // Parse every pair before touching the store: ARMSET is specified as a
    // single logical write, so a bad index in the last pair must not leave the
    // earlier ones applied.
    let mut pairs: Vec<(u64, Arc<[u8]>)> = Vec::with_capacity((ctx.cmd.len() - 2) / 2);
    let mut i = 2;
    while i < ctx.cmd.len() {
        let idx = match parse_index(ctx.cmd[i]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        pairs.push((idx, Arc::from(ctx.cmd[i + 1])));
        i += 2;
    }

    match with_array_write(ctx, &key, |inner| {
        let mut new_slots = 0i64;
        for (idx, value) in pairs {
            if inner.set(idx, value) {
                new_slots += 1;
            }
        }
        new_slots
    }) {
        Ok(n) => resp_integer(n),
        Err(e) => e,
    }
}

// ── ARGET / ARMGET / ARGETRANGE ──────────────────────────────────────────────

/// `ARGET key index` → value, or nil when the key or slot is empty.
fn cmd_arget(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return wrong_args("ARGET");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let idx = match parse_index(ctx.cmd[2]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    match with_array_read(ctx, &key, None, |inner| inner.get(idx).map(|v| v.to_vec())) {
        Ok(Some(v)) => resp_bulk(&v),
        Ok(None) => resp_null_bulk(),
        Err(e) => e,
    }
}

/// `ARMGET key index [index ...]` → one reply element per requested index, order preserved.
fn cmd_armget(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("ARMGET");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let mut indices = Vec::with_capacity(ctx.cmd.len() - 2);
    for raw in &ctx.cmd[2..] {
        match parse_index(raw) {
            Ok(i) => indices.push(i),
            Err(e) => return e,
        }
    }

    let nil = nil_element(ctx);
    // A missing key still yields one element per requested index, all nil —
    // same shape as a present key with every slot empty.
    let missing: Vec<Option<Vec<u8>>> = vec![None; indices.len()];
    match with_array_read(ctx, &key, missing, |inner| {
        indices
            .iter()
            .map(|&i| inner.get(i).map(|v| v.to_vec()))
            .collect::<Vec<_>>()
    }) {
        Ok(values) => resp_array(
            values
                .into_iter()
                .map(|v| v.map(|b| resp_bulk(&b)).unwrap_or_else(|| nil.clone()))
                .collect(),
        ),
        Err(e) => e,
    }
}

/// `ARGETRANGE key start end` → every slot in the span, nils included.
/// `start > end` returns the span in descending index order.
fn cmd_argetrange(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 4 {
        return wrong_args("ARGETRANGE");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let start = match parse_index(ctx.cmd[2]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    let end = match parse_index(ctx.cmd[3]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    let (lo, hi, reverse) = ordered_span(start, end);
    let span = hi - lo + 1;
    if span > MAX_MATERIALISED_RANGE {
        return resp_error(&format!(
            "ERR range exceeds maximum of {MAX_MATERIALISED_RANGE} items"
        ));
    }

    let nil = nil_element(ctx);
    // Missing key: the span is still reported, entirely as nils.
    let missing: Vec<Vec<u8>> = vec![nil.clone(); span as usize];
    match with_array_read(ctx, &key, missing, |inner| {
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(span as usize);
        // Walk the map once over the span instead of doing `span` point lookups:
        // O(log N + M) rather than O(span · log N).
        let mut present = inner.entries.range(lo..=hi).peekable();
        for idx in lo..=hi {
            match present.peek() {
                Some((pidx, val)) if **pidx == idx => {
                    out.push(resp_bulk(val));
                    present.next();
                }
                _ => out.push(nil.clone()),
            }
        }
        if reverse {
            out.reverse();
        }
        out
    }) {
        Ok(items) => resp_array(items),
        Err(e) => e,
    }
}

// ── ARDEL / ARDELRANGE ───────────────────────────────────────────────────────

/// `ARDEL key index [index ...]` → number of elements actually removed.
fn cmd_ardel(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("ARDEL");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let mut indices = Vec::with_capacity(ctx.cmd.len() - 2);
    for raw in &ctx.cmd[2..] {
        match parse_index(raw) {
            Ok(i) => indices.push(i),
            Err(e) => return e,
        }
    }

    match with_existing_array_write(ctx, &key, 0i64, |inner| {
        indices.iter().filter(|&&i| inner.del(i)).count() as i64
    }) {
        Ok(n) => resp_integer(n),
        Err(e) => e,
    }
}

/// `ARDELRANGE key start end [start end ...]` → number of elements removed.
/// Overlapping ranges count each element once.
fn cmd_ardelrange(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 4 || !(ctx.cmd.len() - 2).is_multiple_of(2) {
        return wrong_args("ARDELRANGE");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let mut spans: Vec<(u64, u64)> = Vec::with_capacity((ctx.cmd.len() - 2) / 2);
    let mut i = 2;
    while i < ctx.cmd.len() {
        let a = match parse_index(ctx.cmd[i]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let b = match parse_index(ctx.cmd[i + 1]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        // "If start is greater than end the range is processed in ascending
        // order regardless" — direction is irrelevant to a delete.
        let (lo, hi, _) = ordered_span(a, b);
        spans.push((lo, hi));
        i += 2;
    }

    match with_existing_array_write(ctx, &key, 0i64, |inner| {
        // One span after another, as Redis does: what an earlier span took
        // is not there for a later one to count again.
        spans
            .into_iter()
            .map(|(lo, hi)| inner.del_range(lo, hi))
            .sum::<u64>() as i64
    }) {
        Ok(n) => resp_integer(n),
        Err(e) => e,
    }
}

// ── ARLEN / ARCOUNT ──────────────────────────────────────────────────────────

/// `ARLEN key` → highest occupied index + 1, or 0.
fn cmd_arlen(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("ARLEN");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    match with_array_read(ctx, &key, 0u64, |inner| inner.len()) {
        Ok(n) => resp_integer(n as i64),
        Err(e) => e,
    }
}

/// `ARCOUNT key` → number of non-empty elements, or 0.
fn cmd_arcount(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("ARCOUNT");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    match with_array_read(ctx, &key, 0u64, |inner| inner.count()) {
        Ok(n) => resp_integer(n as i64),
        Err(e) => e,
    }
}

// ── ARINSERT / ARRING / ARNEXT / ARSEEK ──────────────────────────────────────

/// `ARINSERT key value [value ...]` → the last index written.
fn cmd_arinsert(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return wrong_args("ARINSERT");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let values: Vec<Arc<[u8]>> = ctx.cmd[2..].iter().map(|v| Arc::from(*v)).collect();

    match with_array_write(ctx, &key, |inner| {
        let n = values.len() as u64;
        // Refuse the whole batch if it would run past the addressable end —
        // a partial insert would leave the cursor in a state the caller can't
        // reason about from the reply.
        if inner.cursor_exhausted() || n - 1 > ARRAY_MAX_INDEX - inner.next_insert {
            return Err(resp_error(ERR_CURSOR_EXHAUSTED));
        }
        if n > 1 {
            inner.promote_for_range(inner.next_insert, inner.next_insert + n - 1);
        }
        let mut last = inner.next_insert;
        for value in values {
            last = inner.next_insert;
            inner.set(last, value);
            inner.next_insert = last + 1;
        }
        Ok(last)
    }) {
        Ok(Ok(last)) => resp_integer(last as i64),
        Ok(Err(e)) | Err(e) => e,
    }
}

/// `arRingRework`: from the newest position back, at most `keep` items, as
/// far as the first hole; written again oldest first from 0, the cursor after
/// the newest.
fn ring_rebuilt(inner: &ArrayInner, span: u64, keep: u64) -> ArrayInner {
    let anchor = if inner.next_insert == 0 {
        span - 1
    } else {
        (inner.next_insert - 1) % span
    };
    let mut kept: Vec<Arc<[u8]>> = Vec::new();
    let mut at = anchor;
    while (kept.len() as u64) < keep {
        let Some(v) = inner.get(at) else {
            break;
        };
        kept.push(Arc::clone(v));
        at = if at == 0 { span - 1 } else { at - 1 };
    }
    let mut rebuilt = ArrayInner::new();
    let n = kept.len() as u64;
    for (i, v) in kept.into_iter().rev().enumerate() {
        rebuilt.set(i as u64, v);
    }
    rebuilt.next_insert = n;
    rebuilt
}

/// `ARRING key size value [value ...]` → the last index written.
///
/// Values land at `next_insert % size`, so once the window is full each write
/// overwrites the oldest entry. Shrinking `size` truncates the array to the new
/// window.
fn cmd_arring(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 4 {
        return wrong_args("ARRING");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let size = match parse_count(ctx.cmd[2]) {
        Ok(n) if n > 0 => n as u64,
        Ok(_) => return resp_error("ERR ring size must be a positive integer"),
        Err(e) => return e,
    };
    if size > ARRAY_MAX_INDEX {
        return resp_error(ERR_INDEX_RANGE);
    }
    let values: Vec<Arc<[u8]>> = ctx.cmd[3..].iter().map(|v| Arc::from(*v)).collect();

    match with_array_write(ctx, &key, |inner| {
        // The ring is the array: its span is the array's length, and a size
        // that changes its shape rebuilds it first (Redis's
        // `arRingNeedsRework` / `arRingRework`).
        let span = inner.len();
        let keep = if span == 0 || size == span {
            None
        } else if size < span {
            Some(size)
        } else if inner.next_insert != 0 && inner.next_insert < span {
            // Grown after it had wrapped: the new room comes after the newest.
            Some(span)
        } else {
            None
        };
        if let Some(keep) = keep {
            *inner = ring_rebuilt(inner, span, keep);
        }
        let mut last = 0;
        for value in values {
            last = inner.next_insert % size;
            inner.set(last, value);
            inner.next_insert = last + 1;
        }
        last
    }) {
        Ok(last) => resp_integer(last as i64),
        Err(e) => e,
    }
}

/// `ARNEXT key` → the index the next `ARINSERT` would use.
/// 0 for a missing key; null once the cursor is exhausted.
fn cmd_arnext(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 {
        return wrong_args("ARNEXT");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    match with_array_read(ctx, &key, Some(0u64), |inner| {
        if inner.cursor_exhausted() {
            None
        } else {
            Some(inner.next_insert)
        }
    }) {
        Ok(Some(n)) => resp_integer(n as i64),
        Ok(None) => resp_null_bulk(),
        Err(e) => e,
    }
}

/// `ARSEEK key index` → 1 when the cursor moved, 0 when the key doesn't exist.
fn cmd_arseek(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return wrong_args("ARSEEK");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    // 2^64 - 1 too, as in Redis: where the cursor is left after the last
    // index (the next ARINSERT then fails).
    let idx = match parse_index_up_to(ctx.cmd[2], u64::MAX) {
        Ok(i) => i,
        Err(e) => return e,
    };
    // Explicitly does not create the key — the reply distinguishes "moved" from
    // "no such key", which only means something if a miss stays a miss.
    match with_existing_array_write(ctx, &key, 0i64, |inner| {
        inner.next_insert = idx;
        1
    }) {
        Ok(n) => resp_integer(n),
        Err(e) => e,
    }
}

// ── ARSCAN ───────────────────────────────────────────────────────────────────

/// `ARSCAN key start end [LIMIT limit]` → flat `[idx, val, …]` over existing elements.
fn cmd_arscan(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 4 && ctx.cmd.len() != 6 {
        return wrong_args("ARSCAN");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let start = match parse_index(ctx.cmd[2]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    let end = match parse_index(ctx.cmd[3]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    let mut limit: Option<u64> = None;
    if ctx.cmd.len() == 6 {
        if !ctx.cmd[4].eq_ignore_ascii_case(b"LIMIT") {
            return resp_error("ERR syntax error");
        }
        match parse_count(ctx.cmd[5]) {
            Ok(n) if n > 0 => limit = Some(n as u64),
            Ok(_) => return resp_error("ERR LIMIT must be a positive integer"),
            Err(e) => return e,
        }
    }

    let (lo, hi, reverse) = ordered_span(start, end);
    // LIMIT caps *existing* elements returned — empty slots are skipped
    // entirely, unlike ARGETRANGE — so it maps straight onto `take`.
    let cap = limit.unwrap_or(u64::MAX) as usize;
    match with_array_read(ctx, &key, Vec::new(), |inner| {
        let mut out: Vec<Vec<u8>> = Vec::new();
        for (idx, val) in existing_in_span(inner, lo, hi, reverse)
            .into_iter()
            .take(cap)
        {
            out.push(resp_array(vec![resp_integer(idx as i64), resp_bulk(&val)]));
        }
        out
    }) {
        Ok(items) => resp_array(items),
        Err(e) => e,
    }
}

// ── ARGREP ───────────────────────────────────────────────────────────────────

enum Predicate {
    /// Whole-value equality.
    Exact(Vec<u8>),
    /// Substring containment.
    Contains(Vec<u8>),
    /// Redis glob (`*`, `?`, `[...]`) — the same matcher `KEYS`/`SCAN MATCH` use.
    Glob(String),
    Regex(regex_lite::Regex),
}

impl Predicate {
    fn matches(&self, value: &[u8], nocase: bool) -> bool {
        match self {
            Predicate::Exact(needle) => {
                if nocase {
                    value.eq_ignore_ascii_case(needle)
                } else {
                    value == needle.as_slice()
                }
            }
            Predicate::Contains(needle) => contains_subslice(value, needle, nocase),
            Predicate::Glob(pattern) => {
                // The glob matcher is defined over `str`; a value that isn't
                // UTF-8 simply can't match a text pattern.
                let Ok(text) = std::str::from_utf8(value) else {
                    return false;
                };
                if nocase {
                    glob_match(&pattern.to_lowercase(), &text.to_lowercase())
                } else {
                    glob_match(pattern, text)
                }
            }
            // NOCASE is compiled into the pattern itself for RE (see parsing),
            // so nothing to vary here.
            Predicate::Regex(re) => match std::str::from_utf8(value) {
                Ok(text) => re.is_match(text),
                Err(_) => false,
            },
        }
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8], nocase: bool) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| {
        if nocase {
            w.eq_ignore_ascii_case(needle)
        } else {
            w == needle
        }
    })
}

/// `ARGREP key start end <predicate ...> [AND | OR | LIMIT n | WITHVALUES | NOCASE ...]`
///
/// `start`/`end` additionally accept `-` (first index) and `+` (last index).
fn cmd_argrep(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 6 {
        return wrong_args("ARGREP");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    // `-`/`+` resolve against the array's own bounds, so they're deferred until
    // the key is read below.
    let start_tok = ctx.cmd[2].to_vec();
    let end_tok = ctx.cmd[3].to_vec();

    // ── Parse predicates and options ─────────────────────────────────────
    // Predicates and options interleave freely in the grammar, so this is one
    // pass that accepts either at each position.
    let mut predicates: Vec<Predicate> = Vec::new();
    let mut require_all = false;
    let mut with_values = false;
    let mut nocase = false;
    let mut limit: Option<u64> = None;
    let mut pending_regex: Vec<String> = Vec::new();

    let mut i = 4;
    while i < ctx.cmd.len() {
        let tok = ctx.cmd[i];
        let needs_arg = |i: usize| i + 1 < ctx.cmd.len();
        if tok.eq_ignore_ascii_case(b"EXACT") {
            if !needs_arg(i) {
                return resp_error("ERR syntax error");
            }
            predicates.push(Predicate::Exact(ctx.cmd[i + 1].to_vec()));
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"MATCH") {
            if !needs_arg(i) {
                return resp_error("ERR syntax error");
            }
            predicates.push(Predicate::Contains(ctx.cmd[i + 1].to_vec()));
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"GLOB") {
            if !needs_arg(i) {
                return resp_error("ERR syntax error");
            }
            match parse_utf8(ctx.cmd[i + 1]) {
                Some(p) => predicates.push(Predicate::Glob(p.to_string())),
                None => return resp_error("ERR GLOB pattern must be valid UTF-8"),
            }
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"RE") {
            if !needs_arg(i) {
                return resp_error("ERR syntax error");
            }
            if ctx.cmd[i + 1].len() > ARGREP_MAX_RE_LEN {
                return resp_error(&format!(
                    "ERR regular expression is too long, maximum is {ARGREP_MAX_RE_LEN} bytes"
                ));
            }
            match parse_utf8(ctx.cmd[i + 1]) {
                // NOCASE may appear after the pattern, so regexes are compiled
                // once the whole argument list is known.
                Some(p) => pending_regex.push(p.to_string()),
                None => return resp_error("ERR RE pattern must be valid UTF-8"),
            }
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"AND") {
            require_all = true;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"OR") {
            // The last of AND and OR wins, as in Redis.
            require_all = false;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"WITHVALUES") {
            with_values = true;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"NOCASE") {
            nocase = true;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"LIMIT") {
            if !needs_arg(i) {
                return resp_error("ERR syntax error");
            }
            match parse_count(ctx.cmd[i + 1]) {
                Ok(n) if n > 0 => limit = Some(n as u64),
                Ok(_) => return resp_error("ERR LIMIT must be positive"),
                Err(e) => return e,
            }
            i += 2;
        } else {
            return resp_error("ERR syntax error");
        }
        if predicates.len() + pending_regex.len() > ARGREP_MAX_PREDICATES {
            return resp_error(&format!(
                "ERR too many predicates, maximum is {ARGREP_MAX_PREDICATES}"
            ));
        }
    }

    for pattern in pending_regex {
        if pattern.is_empty() {
            return resp_error("ERR regular expression is empty");
        }
        // `\1` … `\9`: backreferences, which Redis's engine does not do either.
        let bytes = pattern.as_bytes();
        let backreference = bytes.windows(2).enumerate().any(|(at, w)| {
            w[0] == b'\\'
                && w[1].is_ascii_digit()
                && w[1] != b'0'
                && bytes[..at]
                    .iter()
                    .rev()
                    .take_while(|b| **b == b'\\')
                    .count()
                    % 2
                    == 0
        });
        if backreference {
            return resp_error("ERR regular expression backreferences are not supported");
        }
        // `(?i)` is the engine's own case-insensitive flag — safer than
        // lowercasing the pattern, which would corrupt character classes.
        let source = if nocase {
            format!("(?i){pattern}")
        } else {
            pattern
        };
        match regex_lite::Regex::new(&source) {
            Ok(re) => predicates.push(Predicate::Regex(re)),
            Err(e) => return resp_error(&format!("ERR invalid regular expression: {e}")),
        }
    }

    if predicates.is_empty() {
        return resp_error("ERR syntax error");
    }

    // Resolve `-`/`+` against the array's own bounds and run the scan in a
    // single read of the key.
    let result = with_array_read(ctx, &key, Ok(Vec::new()), |inner| {
        let first = inner
            .entries
            .first_key_value()
            .map(|(&i, _)| i)
            .unwrap_or(0);
        let last = inner.entries.last_key_value().map(|(&i, _)| i).unwrap_or(0);

        let start = match resolve_grep_bound(&start_tok, first, last) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let end = match resolve_grep_bound(&end_tok, first, last) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let (lo, hi, reverse) = ordered_span(start, end);

        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut taken = 0u64;
        for (idx, val) in existing_in_span(inner, lo, hi, reverse) {
            if let Some(max) = limit
                && taken >= max
            {
                break;
            }
            let hit = if require_all {
                predicates.iter().all(|p| p.matches(&val, nocase))
            } else {
                predicates.iter().any(|p| p.matches(&val, nocase))
            };
            if !hit {
                continue;
            }
            out.push(if with_values {
                resp_array(vec![resp_integer(idx as i64), resp_bulk(&val)])
            } else {
                resp_integer(idx as i64)
            });
            taken += 1;
        }
        Ok(out)
    });

    match result {
        Ok(Ok(items)) => resp_array(items),
        Ok(Err(e)) | Err(e) => e,
    }
}

/// Resolve an `ARGREP` bound token, which may be `-` (first index) or `+` (last index).
fn resolve_grep_bound(tok: &[u8], first: u64, last: u64) -> Result<u64, Vec<u8>> {
    if tok == b"-" {
        Ok(first)
    } else if tok == b"+" {
        Ok(last)
    } else {
        parse_index(tok)
    }
}

// ── AROP ─────────────────────────────────────────────────────────────────────

/// `AROP key start end <SUM | MIN | MAX | AND | OR | XOR | MATCH value | USED>`
fn cmd_arop(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 5 {
        return wrong_args("AROP");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let start = match parse_index(ctx.cmd[2]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    let end = match parse_index(ctx.cmd[3]) {
        Ok(i) => i,
        Err(e) => return e,
    };
    let op_raw = ctx.cmd[4];
    // MATCH takes a value; every other operation is a bare token.
    let match_target: Option<Vec<u8>> = if op_raw.eq_ignore_ascii_case(b"MATCH") {
        if ctx.cmd.len() != 6 {
            return wrong_args("AROP");
        }
        Some(ctx.cmd[5].to_vec())
    } else {
        if ctx.cmd.len() != 5 {
            return resp_error("ERR syntax error");
        }
        None
    };

    #[derive(Clone, Copy, PartialEq)]
    enum Op {
        Sum,
        Min,
        Max,
        And,
        Or,
        Xor,
        Match,
        Used,
    }
    let op = if op_raw.eq_ignore_ascii_case(b"SUM") {
        Op::Sum
    } else if op_raw.eq_ignore_ascii_case(b"MIN") {
        Op::Min
    } else if op_raw.eq_ignore_ascii_case(b"MAX") {
        Op::Max
    } else if op_raw.eq_ignore_ascii_case(b"AND") {
        Op::And
    } else if op_raw.eq_ignore_ascii_case(b"OR") {
        Op::Or
    } else if op_raw.eq_ignore_ascii_case(b"XOR") {
        Op::Xor
    } else if op_raw.eq_ignore_ascii_case(b"MATCH") {
        Op::Match
    } else if op_raw.eq_ignore_ascii_case(b"USED") {
        Op::Used
    } else {
        return resp_error("ERR syntax error");
    };

    let (lo, hi, _) = ordered_span(start, end);

    enum OpResult {
        Int(i64),
        Num(f64),
        Nil,
    }

    let outcome = with_array_read(ctx, &key, OpResult::Nil, |inner| {
        let elems = inner.entries.range(lo..=hi);
        match op {
            Op::Used => OpResult::Int(elems.count() as i64),
            Op::Match => {
                let needle = match_target.as_deref().unwrap_or(b"");
                OpResult::Int(elems.filter(|(_, v)| v.as_ref() == needle).count() as i64)
            }
            Op::Sum | Op::Min | Op::Max => {
                // Non-numeric elements are skipped, not fatal: the reply is
                // documented as nil when the range holds *no numeric* elements.
                let mut acc: Option<f64> = None;
                for (_, v) in elems {
                    let Some(n) = parse_numeric(v) else { continue };
                    acc = Some(match (acc, op) {
                        (None, _) => n,
                        (Some(a), Op::Sum) => a + n,
                        (Some(a), Op::Min) => a.min(n),
                        (Some(a), Op::Max) => a.max(n),
                        (Some(a), _) => a,
                    });
                }
                match acc {
                    Some(v) => OpResult::Num(v),
                    None => OpResult::Nil,
                }
            }
            Op::And | Op::Or | Op::Xor => {
                // "treating each as an integer (floats are truncated toward zero)"
                let mut acc: Option<i64> = None;
                for (_, v) in elems {
                    let Some(n) = parse_numeric(v) else { continue };
                    let n = n.trunc() as i64;
                    acc = Some(match (acc, op) {
                        (None, _) => n,
                        (Some(a), Op::And) => a & n,
                        (Some(a), Op::Or) => a | n,
                        (Some(a), Op::Xor) => a ^ n,
                        (Some(a), _) => a,
                    });
                }
                match acc {
                    Some(v) => OpResult::Int(v),
                    None => OpResult::Nil,
                }
            }
        }
    });

    match outcome {
        Ok(OpResult::Int(n)) => resp_integer(n),
        Ok(OpResult::Num(v)) => crate::resp::write::resp_double_bulk(v),
        Ok(OpResult::Nil) => resp_null_bulk(),
        Err(e) => e,
    }
}

/// Parse an element as a number for `AROP`. Integers and floats both accepted;
/// NaN/inf are rejected so they can't poison MIN/MAX/SUM.
fn parse_numeric(raw: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(raw).ok()?.trim();
    let v: f64 = s.parse().ok()?;
    if v.is_finite() { Some(v) } else { None }
}

// ── ARLASTITEMS ──────────────────────────────────────────────────────────────

/// `ARLASTITEMS key count [REV]` → the most recently inserted elements.
///
/// "Most recent" is defined by the insert cursor, walking backwards from
/// `next_insert - 1`. For a ring the walk wraps within the window, which is what
/// makes `ARRING`'s own doc example (`v1, v2, v3` after `v0..v3` into a size-3
/// ring) come out in that order.
fn cmd_arlastitems(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 3 && ctx.cmd.len() != 4 {
        return wrong_args("ARLASTITEMS");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    // Zero or less is nothing to give, as in Redis.
    let count = match parse_count(ctx.cmd[2]) {
        Ok(n) => n.max(0) as u64,
        Err(e) => return e,
    };
    let mut rev = false;
    if ctx.cmd.len() == 4 {
        if !ctx.cmd[3].eq_ignore_ascii_case(b"REV") {
            return resp_error("ERR syntax error");
        }
        rev = true;
    }

    match with_array_read(ctx, &key, Vec::new(), |inner| {
        // From the newest position back, as many steps as asked (no more
        // than there are elements), wrapping at the end of the array; a hole
        // is a nil. Newest first, then flipped unless REV was asked for.
        let len = inner.len();
        let steps = count.min(inner.count());
        let mut newest_first: Vec<Option<Vec<u8>>> = Vec::new();
        if steps == 0 {
            return newest_first;
        }
        let mut at = if inner.next_insert == 0 {
            len - 1
        } else {
            inner.next_insert - 1
        };
        for _ in 0..steps {
            newest_first.push(inner.get(at).map(|v| v.to_vec()));
            at = if at == 0 { len - 1 } else { at - 1 };
        }
        newest_first
    }) {
        Ok(mut items) => {
            if !rev {
                items.reverse();
            }
            resp_array(
                items
                    .iter()
                    .map(|v| match v {
                        Some(v) => resp_bulk(v),
                        None => crate::resp::write::resp_null_bulk(),
                    })
                    .collect(),
            )
        }
        Err(e) => e,
    }
}

// ── ARINFO ───────────────────────────────────────────────────────────────────

/// What `ARINFO FULL` reports of an array's slices, as Redis counts it.
struct SliceStats {
    slices: u64,
    dense: u64,
    sparse: u64,
    dense_winsize: u64,
    dense_count: u64,
    sparse_cap: u64,
}

fn slice_stats(inner: &ArrayInner) -> SliceStats {
    let mut stats = SliceStats {
        slices: inner.slices.len() as u64,
        dense: 0,
        sparse: 0,
        dense_winsize: 0,
        dense_count: 0,
        sparse_cap: 0,
    };
    for slice in inner.slices.values() {
        match slice.shape {
            ArraySliceShape::Dense { winsize, .. } => {
                stats.dense += 1;
                stats.dense_winsize += u64::from(winsize);
                stats.dense_count += u64::from(slice.count);
            }
            ArraySliceShape::Sparse { cap } => {
                stats.sparse += 1;
                stats.sparse_cap += u64::from(cap);
            }
        }
    }
    stats
}

/// `ARINFO key [FULL]` → array metadata. RESP2 flat array, RESP3 map.
fn cmd_arinfo(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() != 2 && ctx.cmd.len() != 3 {
        return wrong_args("ARINFO");
    }
    let key = match parse_utf8(ctx.cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let mut full = false;
    if ctx.cmd.len() == 3 {
        if !ctx.cmd[2].eq_ignore_ascii_case(b"FULL") {
            return resp_error("ERR syntax error");
        }
        full = true;
    }

    let resp3 = ctx.resp == 3;
    match with_array_read(ctx, &key, None, |inner| {
        let stats = slice_stats(inner);
        let mut kv: Vec<(&'static str, Vec<u8>)> = vec![
            ("count", resp_integer(inner.count() as i64)),
            ("len", resp_integer(inner.len() as i64)),
            (
                "next-insert-index",
                resp_integer(inner.next_insert.min(ARRAY_MAX_INDEX) as i64),
            ),
            ("slices", resp_integer(stats.slices as i64)),
            ("directory-size", resp_integer(stats.slices as i64)),
            // Non-zero only once the directory outgrows a single level, which
            // for a 4096-wide slice means very large sparse arrays.
            (
                "super-dir-entries",
                resp_integer(
                    stats
                        .slices
                        .div_ceil(u64::from(inner.slice_size))
                        .saturating_sub(1) as i64,
                ),
            ),
            ("slice-size", resp_integer(i64::from(inner.slice_size))),
        ];
        if full {
            let ratio = |a: u64, b: u64| if b == 0 { 0.0 } else { a as f64 / b as f64 };
            kv.push(("dense-slices", resp_integer(stats.dense as i64)));
            kv.push(("sparse-slices", resp_integer(stats.sparse as i64)));
            kv.push((
                "avg-dense-size",
                crate::resp::write::resp_double_bulk(ratio(stats.dense_winsize, stats.dense)),
            ));
            kv.push((
                "avg-dense-fill",
                crate::resp::write::resp_double_bulk(ratio(stats.dense_count, stats.dense_winsize)),
            ));
            kv.push((
                "avg-sparse-size",
                crate::resp::write::resp_double_bulk(ratio(stats.sparse_cap, stats.sparse)),
            ));
        }
        Some(kv)
    }) {
        Ok(Some(kv)) => {
            let mut flat: Vec<Vec<u8>> = Vec::with_capacity(kv.len() * 2);
            for (name, value) in kv {
                flat.push(resp_bulk_str(name));
                flat.push(value);
            }
            if resp3 {
                resp_map(flat)
            } else {
                resp_array(flat)
            }
        }
        // Missing key: an empty aggregate, consistent with the other AR* reads
        // treating a miss as an empty array rather than an error.
        Ok(None) => {
            if resp3 {
                resp_map(vec![])
            } else {
                resp_array(vec![])
            }
        }
        Err(e) => e,
    }
}
