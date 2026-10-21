// src/commands/streams.rs

use crate::commands::blocking::{self, ParkOutcome};
use crate::commands::ctx::Ctx;
use crate::commands::helpers::{lazy_expire_on_read, parse_utf8};
use crate::commands::stream_extra::{
    self, Approx, IdmpArg, ReadCaps, TrimRefs, TrimTo, claim_effect, setid_effect, trim_stream,
    trim_stream_approx,
};
use crate::keyspace_events::{KS_STREAM, notify};
use crate::replication::script_feed::{effect, effect_of_metadata, effects_begin};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::types::{ConsumerGroup, KiviDBValue, PelEntry, StreamEntry, StreamId, StreamInner};
use crate::{read_lock, write_lock};
use ahash::AHashMap;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ── RESP3 shaping for XREAD / XREADGROUP ─────────────────────────────────────
//
// The reply is a set of `stream-name → entries` results. RESP2 has no map, so
// Redis sends an array of `[name, entries]` pairs; under RESP3 it switches to a
// real map keyed by stream name (`addReplyMapLen` when `c->resp > 2`). Clients
// rely on that: redis-py returns a list of tuples for RESP2 and a dict for
// RESP3, so getting this wrong doesn't just look different, it breaks callers.

/// Effective protocol for the reply being built (honours `redis.setresp`).
#[inline]
fn reply_resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

/// The wire items contributed by one stream's results.
///
/// RESP2 yields a single `[name, entries]` array; RESP3 yields the name and the
/// entries as two adjacent items, which [`stream_results_reply`] then frames as
/// a map. Returning a `Vec` keeps both shapes flowing through the same
/// `flat_map` at each call site.
#[inline]
fn stream_result_items(resp: u8, key: &str, entries: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    if resp >= 3 {
        vec![resp_bulk_str(key), resp_array(entries)]
    } else {
        vec![resp_array(vec![resp_bulk_str(key), resp_array(entries)])]
    }
}

/// Frame the collected [`stream_result_items`] as the whole XREAD reply.
#[inline]
fn stream_results_reply(resp: u8, items: Vec<Vec<u8>>) -> Vec<u8> {
    if resp >= 3 {
        resp_map(items)
    } else {
        resp_array(items)
    }
}

// ── handle ───────────────────────────────────────────────────────────────────

/// The keyspace event of a stream command that has just answered `reply`,
/// if what it did is one. (A consumer coming into being or going announces
/// itself: that is not in any reply.)
fn announce(ctx: &Ctx, name: &str, reply: &[u8]) {
    if crate::keyspace_events::KS_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return;
    }
    let ok = reply.starts_with(b"+OK");
    let some = reply.first() == Some(&b':') && !reply.starts_with(b":0\r\n");
    let sub = |word: &[u8]| {
        ctx.cmd
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(word))
    };
    let (event, key_at) = match name {
        "XADD" if reply.first() == Some(&b'$') && !reply.starts_with(b"$-1") => ("xadd", 1),
        "XDEL" | "XDELEX" | "XACKDEL" if some => ("xdel", 1),
        "XTRIM" if some => ("xtrim", 1),
        "XSETID" if ok => ("xsetid", 1),
        "XGROUP" if sub(b"CREATE") && ok => ("xgroup-create", 2),
        "XGROUP" if sub(b"SETID") && ok => ("xgroup-setid", 2),
        "XGROUP" if sub(b"DESTROY") && some => ("xgroup-destroy", 2),
        "XGROUP" if sub(b"CREATECONSUMER") && some => ("xgroup-createconsumer", 2),
        _ => return,
    };
    if let Some(key) = ctx
        .cmd
        .get(key_at)
        .and_then(|raw| std::str::from_utf8(raw).ok())
    {
        notify(ctx.pubsub, *ctx.selected_db, KS_STREAM, event, key);
    }
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let reply = run(ctx, name)?;
    announce(ctx, name, &reply);
    Some(reply)
}

fn run(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "XADD" => Some(xadd(ctx)),
        "XLEN" => Some(xlen(ctx)),
        "XRANGE" => Some(xrange(ctx)),
        "XREVRANGE" => Some(xrevrange(ctx)),
        "XREAD" => Some(xread(ctx)),
        "XDEL" => Some(xdel(ctx)),
        "XTRIM" => Some(xtrim(ctx)),
        "XSETID" => Some(xsetid(ctx)),
        "XGROUP" => Some(xgroup(ctx)),
        "XREADGROUP" => Some(xreadgroup(ctx)),
        "XACK" => Some(xack(ctx)),
        "XNACK" => Some(xnack(ctx)),
        "XPENDING" => Some(xpending(ctx)),
        "XCLAIM" => Some(xclaim(ctx)),
        "XAUTOCLAIM" => Some(xautoclaim(ctx)),
        "XINFO" => Some(xinfo(ctx)),
        "XACKDEL" => Some(xackdel(ctx)),
        "XDELEX" => Some(xdelex(ctx)),
        "XCFGSET" => Some(xcfgset(ctx)),
        _ => None,
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn get_or_create_stream(ctx: &mut Ctx, key: &str) -> Result<Arc<RwLock<StreamInner>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);

    match map.get(key) {
        Some(KiviDBValue::Stream(s)) => Ok(Arc::clone(s)),
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            let s = Arc::new(RwLock::new(StreamInner::new()));
            map.insert(Key::from(key), KiviDBValue::Stream(Arc::clone(&s)));
            Ok(s)
        }
    }
}

/// Parse a stream ID for range queries — handles "-" and "+" as bounds,
/// and "ms" (without seq) as "ms-0" for inclusive range starts.
fn parse_range_id_lo(s: &str) -> Option<StreamId> {
    match s {
        "-" => Some(StreamId::min()),
        "+" => Some(StreamId::max()),
        s => StreamId::parse(s).or_else(|| {
            // bare ms without seq → ms-0 as lower bound
            s.parse::<u64>().ok().map(|ms| StreamId::new(ms, 0))
        }),
    }
}

fn parse_range_id_hi(s: &str) -> Option<StreamId> {
    match s {
        "+" => Some(StreamId::max()),
        "-" => Some(StreamId::min()),
        s => {
            // If explicit ms-seq form, use as-is
            if s.contains('-') {
                StreamId::parse(s)
            } else {
                // Bare ms without seq: expand to ms-MAX as inclusive upper bound
                // so XRANGE s 1000 1000 captures 1000-0, 1000-1, 1000-2, etc.
                s.parse::<u64>().ok().map(|ms| StreamId::new(ms, u64::MAX))
            }
        }
    }
}

/// The ID after `id`, if there is one.
fn id_after(id: &StreamId) -> Option<StreamId> {
    match (id.ms, id.seq) {
        (u64::MAX, u64::MAX) => None,
        (ms, u64::MAX) => Some(StreamId::new(ms + 1, 0)),
        (ms, seq) => Some(StreamId::new(ms, seq + 1)),
    }
}

/// The ID before `id`, if there is one.
fn id_preceding(id: &StreamId) -> Option<StreamId> {
    match (id.ms, id.seq) {
        (0, 0) => None,
        (ms, 0) => Some(StreamId::new(ms - 1, u64::MAX)),
        (ms, seq) => Some(StreamId::new(ms, seq - 1)),
    }
}

/// The IDs from `start` to `end`, both included, that a range command's two
/// arguments name; `None` if there are none. Either may be `(id`, for
/// "not this one": the range then starts just after it, or ends just before.
fn id_interval(start: &str, end: &str) -> Result<Option<(StreamId, StreamId)>, Vec<u8>> {
    let invalid = || resp_error("ERR Invalid stream ID specified as stream command argument");
    // `(-` and `(+` mean nothing: there is no ID to leave out.
    let named = |s: &str| !matches!(s, "-" | "+");
    let lo = match start.strip_prefix('(') {
        Some(id) => parse_range_id_lo(id)
            .filter(|_| named(id))
            .ok_or_else(invalid)
            .and_then(|id| {
                id_after(&id).ok_or_else(|| resp_error("ERR invalid start ID for the interval"))
            })?,
        None => parse_range_id_lo(start).ok_or_else(invalid)?,
    };
    let hi = match end.strip_prefix('(') {
        Some(id) => parse_range_id_hi(id)
            .filter(|_| named(id))
            .ok_or_else(invalid)
            .and_then(|id| {
                id_preceding(&id).ok_or_else(|| resp_error("ERR invalid end ID for the interval"))
            })?,
        None => parse_range_id_hi(end).ok_or_else(invalid)?,
    };
    Ok((lo <= hi).then_some((lo, hi)))
}

/// Format a stream entry as the nested RESP array Redis uses:
/// ["id", ["field", "value", "field", "value", ...]]
fn format_entry(id: &StreamId, entry: &StreamEntry) -> Vec<u8> {
    let field_values: Vec<Vec<u8>> = entry
        .fields
        .iter()
        .flat_map(|(f, v)| vec![resp_bulk(f.as_ref()), resp_bulk(v.as_ref())])
        .collect();
    resp_array(vec![
        resp_bulk_str(&id.to_string()),
        resp_array(field_values),
    ])
}

// ── XADD ─────────────────────────────────────────────────────────────────────

/// Where `XADD`'s options end and its ID argument begins, plus the options
/// themselves.
///
/// ```text
/// XADD key [NOMKSTREAM] [KEEPREF | DELREF | ACKED]
///     [IDMPAUTO producer-id | IDMP producer-id idempotent-id]
///     [<MAXLEN | MINID> [= | ~] threshold [LIMIT count]] <* | id>
///     field value [field value ...]
/// ```
/// — the ID is positional but its index depends on which options were given, so
/// it cannot be read from a fixed offset.
///
/// Shared with `replication::script_feed`, which rewrites an auto-generated ID
/// (`*` or `ms-*`) into the concrete one before propagating. That rewrite has to
/// land on the argument this parser consumed as the ID; a second, drifting copy
/// of this scan would silently corrupt the replication stream instead of failing
/// a test. See `xadd_id_index_matches_executor` in `script_feed`.
pub(crate) struct XaddOptions<'a> {
    pub id_index: usize,
    pub nomkstream: bool,
    pub maxlen: Option<u64>,
    pub minid: Option<StreamId>,
    /// What trimming does about pending-list references (Redis 8.2).
    pub trim_refs: TrimRefs,
    /// Idempotent production (Redis 8.6).
    pub idmp: Option<IdmpArg<'a>>,
    /// `~`: trim by whole nodes. See [`Approx`].
    pub approx: bool,
    /// `LIMIT`'s argument, if it was given; negative if it was not a count.
    pub limit: Option<i64>,
}

pub(crate) fn parse_xadd_options<'a>(cmd: &[&'a [u8]]) -> XaddOptions<'a> {
    let mut i = 2usize;
    let mut nomkstream = false;
    let mut maxlen: Option<u64> = None;
    let mut minid: Option<StreamId> = None;
    let mut trim_refs = TrimRefs::default();
    let mut idmp: Option<IdmpArg<'a>> = None;
    let mut approx = false;
    let mut limit: Option<i64> = None;

    while i < cmd.len() {
        let tok = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        match tok.as_str() {
            "NOMKSTREAM" => {
                nomkstream = true;
                i += 1;
            }
            "KEEPREF" | "DELREF" | "ACKED" => {
                trim_refs = TrimRefs::parse(cmd[i]).unwrap_or_default();
                i += 1;
            }
            "IDMP" if i + 2 < cmd.len() => {
                idmp = Some(IdmpArg::Explicit {
                    producer: cmd[i + 1],
                    iid: cmd[i + 2],
                });
                i += 3;
            }
            "IDMPAUTO" if i + 1 < cmd.len() => {
                idmp = Some(IdmpArg::Auto {
                    producer: cmd[i + 1],
                });
                i += 2;
            }
            "MAXLEN" => {
                i += 1;
                if let Some(b) = cmd.get(i)
                    && (*b == b"~" || *b == b"=")
                {
                    approx = *b == b"~";
                    i += 1;
                }
                maxlen = cmd
                    .get(i)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok());
                i += 1;
            }
            "MINID" => {
                i += 1;
                if let Some(b) = cmd.get(i)
                    && (*b == b"~" || *b == b"=")
                {
                    approx = *b == b"~";
                    i += 1;
                }
                minid = cmd
                    .get(i)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(StreamId::parse);
                i += 1;
            }
            "LIMIT" => {
                limit = Some(
                    cmd.get(i + 1)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(-1),
                );
                i += 2;
            }
            _ => break,
        }
    }

    XaddOptions {
        id_index: i,
        nomkstream,
        maxlen,
        minid,
        trim_refs,
        idmp,
        approx,
        limit,
    }
}

/// What `LIMIT` and `~` came to, or the error in how they were given.
fn approx_of(approx: bool, limit: Option<i64>) -> Result<Option<Approx>, Vec<u8>> {
    match limit {
        Some(_) if !approx => Err(resp_error(
            "ERR syntax error, LIMIT cannot be used without the special ~ option",
        )),
        Some(n) if n < 0 => Err(resp_error("ERR The LIMIT argument must be >= 0.")),
        Some(n) => Ok(Some(Approx::new(Some(n as u64)))),
        None => Ok(approx.then(|| Approx::new(None))),
    }
}

/// The ID an `XADD` asks for.
enum NewId {
    /// `*`
    Auto,
    /// `ms-*`
    AutoSeq(u64),
    Explicit(StreamId),
}

pub fn xadd(ctx: &mut Ctx) -> Vec<u8> {
    let (key, id_str, id_index, fields, nomkstream, maxlen, minid, trim_refs, idmp, approx) = {
        let cmd = &ctx.cmd;
        if cmd.len() < 5 {
            return resp_error("ERR wrong number of arguments for 'xadd' command");
        }

        let key = match parse_utf8(cmd[1]) {
            Some(k) => k.to_string(),
            None => return resp_error("ERR key must be valid UTF-8"),
        };

        let XaddOptions {
            id_index,
            nomkstream,
            maxlen,
            minid,
            trim_refs,
            idmp,
            approx,
            limit,
        } = parse_xadd_options(cmd);
        let approx = match approx_of(approx, limit) {
            Ok(approx) => approx,
            Err(e) => return e,
        };
        let mut i = id_index;

        let id_str = match cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
            Some(s) => s.to_string(),
            None => return resp_error("ERR syntax error: missing ID"),
        };
        i += 1;

        if (cmd.len() - i) < 2 || !(cmd.len() - i).is_multiple_of(2) {
            return resp_error("ERR wrong number of arguments: field-value pairs required");
        }
        type FieldsType = Vec<(Arc<[u8]>, Arc<[u8]>)>;
        let fields: FieldsType = cmd[i..]
            .as_chunks::<2>()
            .0
            .iter()
            .map(|chunk| (Arc::from(chunk[0]), Arc::from(chunk[1])))
            .collect();

        // Owned (producer, idempotent ID): the borrow of `ctx.cmd` ends here.
        let idmp: Option<(Vec<u8>, Arc<[u8]>)> =
            idmp.map(|arg| (arg.producer().to_vec(), arg.iid(&fields)));

        (
            key, id_str, id_index, fields, nomkstream, maxlen, minid, trim_refs, idmp, approx,
        )
        // ← &ctx.cmd borrow ends here, all data moved into owned tuple
    };

    if idmp.is_some() {
        // Idempotency is about not producing an entry twice, so the entry's
        // ID has to be the server's to assign. The exception is an XADD that
        // another node already resolved to a concrete ID: a replica (or the
        // AOF) applying its master's stream, or a slot being migrated here.
        if id_str != "*" && !ctx.apply_replication && !ctx.bypass_cluster_check {
            return resp_error(
                "ERR IDMP and IDMPAUTO can only be used with an auto-generated ID (*)",
            );
        }
    }
    // What this command adds — possibly nothing — and how much a `~` trimmed
    // are recorded for propagation rather than inferred from its reply.
    let recorded = idmp.is_some() || approx.is_some();
    if recorded {
        effects_begin();
    }

    // The ID, before there is a stream to put it in: an `XADD` that fails
    // used to leave an empty stream behind where there had been no key.
    let new_id = if id_str == "*" {
        NewId::Auto
    } else if let Some(ms) = id_str.strip_suffix("-*") {
        match ms.parse() {
            Ok(ms) => NewId::AutoSeq(ms),
            Err(_) => {
                return resp_error("ERR Invalid stream ID specified as stream command argument");
            }
        }
    } else {
        match StreamId::parse(&id_str) {
            Some(id) if id == StreamId::min() => {
                return resp_error("ERR The ID specified in XADD must be greater than 0-0");
            }
            Some(id) => NewId::Explicit(id),
            None => {
                return resp_error("ERR Invalid stream ID specified as stream command argument");
            }
        }
    };

    if nomkstream {
        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, &key);
        let map = read_lock!(shard);
        if !map.contains_key(key.as_str()) {
            return resp_null_bulk();
        }
    }

    // Now safe — no live borrow of ctx.cmd
    let stream = match get_or_create_stream(ctx, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let mut inner = stream.write();
    let now = now_ms();
    let idmp_defaults = (
        ctx.config.stream_idmp_duration,
        ctx.config.stream_idmp_maxsize,
    );

    // A retry of a message this producer already delivered: reply with the
    // entry it produced the first time and add nothing. A replica does not
    // second-guess its master, which has already made this decision.
    if let Some((producer, iid)) = &idmp
        && !ctx.apply_replication
        && !ctx.bypass_cluster_check
        && let Some(existing) =
            stream_extra::idmp_lookup(&mut inner, producer, iid, now, idmp_defaults)
    {
        return resp_bulk_str(&existing.to_string());
    }

    // The next ID is worked out without ever adding to a sequence number
    // that is already the largest there is: that was an overflow, and took
    // the connection with it.
    let smaller = || {
        resp_error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        )
    };
    let last = inner.last_id.clone();
    let entry_id = match new_id {
        NewId::Auto if now > last.ms => StreamId::new(now, 0),
        NewId::Auto => match id_after(&last) {
            Some(id) => id,
            None => {
                return resp_error(
                    "ERR The stream has exhausted the last possible ID, unable to add more items",
                );
            }
        },
        NewId::AutoSeq(ms) if ms > last.ms => StreamId::new(ms, 0),
        NewId::AutoSeq(ms) if ms == last.ms && last.seq < u64::MAX => {
            StreamId::new(ms, last.seq + 1)
        }
        NewId::AutoSeq(_) => return smaller(),
        NewId::Explicit(id) => id,
    };

    if entry_id <= inner.last_id && inner.last_id != StreamId::min() {
        return resp_error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        );
    }

    let id_str_out = entry_id.to_string();
    stream_extra::record_append(&mut inner, &fields);
    inner
        .entries
        .insert(entry_id.clone(), StreamEntry { fields });
    inner.last_id = entry_id.clone();
    inner.entries_added += 1;

    if let Some((producer, iid)) = idmp {
        stream_extra::idmp_record(&mut inner, &producer, iid, entry_id, now, idmp_defaults);
    }

    // Trim before releasing the lock
    let mut trim = |to: TrimTo| match approx {
        Some(approx) => trim_stream_approx(&mut inner, &to, trim_refs, approx),
        None => trim_stream(&mut inner, &to, trim_refs),
    };
    if let Some(max) = maxlen {
        trim(TrimTo::MaxLen(max));
    }
    if let Some(min) = minid {
        trim(TrimTo::MinId(min));
    }
    if recorded {
        // Propagate the entry under the ID it was given — idempotency
        // arguments included, so the other side remembers the pair too —
        // and a `~` trim as what it came to.
        let mut parts: Vec<Vec<u8>> = ctx.cmd.iter().map(|b| b.to_vec()).collect();
        parts[id_index] = id_str_out.clone().into_bytes();
        if approx.is_some() {
            stream_extra::exact_trim_args(&mut parts, 2, id_index, &inner);
        }
        effect(parts);
    }
    // Persistent policy trim from XCFGSET MAX-ENTRIES.
    // Applied after the per-XADD trim so both can coexist. The more
    // restrictive of the two wins — if XCFGSET says 100 and XADD says
    // MAXLEN 50, we end up with at most 50.
    if let Some(max) = inner.max_entries {
        trim_stream(&mut inner, &TrimTo::MaxLen(max), TrimRefs::KeepRef);
    }

    // Release write lock before notifying waiters
    drop(inner);
    notify_xread_waiters(ctx, &key);

    resp_bulk_str(&id_str_out)
}

// ── XLEN ─────────────────────────────────────────────────────────────────────

pub fn xlen(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return resp_error("ERR wrong number of arguments for 'xlen' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let gone = lazy_expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key).filter(|_| !gone) {
        None => resp_integer(0),
        Some(KiviDBValue::Stream(s)) => resp_integer(s.read().entries.len() as i64),
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XRANGE / XREVRANGE ───────────────────────────────────────────────────────

pub fn xrange(ctx: &mut Ctx) -> Vec<u8> {
    // XRANGE key start end [COUNT count]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'xrange' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let start_str = std::str::from_utf8(cmd[2]).unwrap_or("-");
    let end_str = std::str::from_utf8(cmd[3]).unwrap_or("+");

    let mut count = usize::MAX;
    let mut i = 4;
    while i < cmd.len() {
        if std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .eq_ignore_ascii_case("COUNT")
        {
            count = cmd
                .get(i + 1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(usize::MAX);
            i += 2;
        } else {
            i += 1;
        }
    }

    // Nothing in the interval: nothing to return, and no range to ask the
    // map for — it does not take one that ends before it starts.
    let (lo, hi, count) = match id_interval(start_str, end_str) {
        Ok(Some((lo, hi))) => (lo, hi, count),
        Ok(None) => (StreamId::min(), StreamId::min(), 0),
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![]),
        Some(KiviDBValue::Stream(s)) => {
            let inner = s.read();
            let items: Vec<Vec<u8>> = inner
                .entries
                .range(lo..=hi)
                .take(count)
                .map(|(id, entry)| format_entry(id, entry))
                .collect();
            resp_array(items)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

pub fn xrevrange(ctx: &mut Ctx) -> Vec<u8> {
    // XREVRANGE key end start [COUNT count]  (note: end before start)
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'xrevrange' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let end_str = std::str::from_utf8(cmd[2]).unwrap_or("+");
    let start_str = std::str::from_utf8(cmd[3]).unwrap_or("-");

    let mut count = usize::MAX;
    let mut i = 4;
    while i < cmd.len() {
        if std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .eq_ignore_ascii_case("COUNT")
        {
            count = cmd
                .get(i + 1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(usize::MAX);
            i += 2;
        } else {
            i += 1;
        }
    }

    // Nothing in the interval: nothing to return, and no range to ask the
    // map for — it does not take one that ends before it starts.
    let (lo, hi, count) = match id_interval(start_str, end_str) {
        Ok(Some((lo, hi))) => (lo, hi, count),
        Ok(None) => (StreamId::min(), StreamId::min(), 0),
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_array(vec![]),
        Some(KiviDBValue::Stream(s)) => {
            let inner = s.read();
            // BTreeMap::range gives ascending; rev() flips it
            let items: Vec<Vec<u8>> = inner
                .entries
                .range(lo..=hi)
                .rev()
                .take(count)
                .map(|(id, entry)| format_entry(id, entry))
                .collect();
            resp_array(items)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XREAD ────────────────────────────────────────────────────────────────────

/// The ID just below `id`: reading "after" it yields `id` itself.
fn id_before(id: &StreamId) -> StreamId {
    if id.seq > 0 {
        StreamId::new(id.ms, id.seq - 1)
    } else {
        StreamId::new(id.ms.saturating_sub(1), u64::MAX)
    }
}

/// One non-blocking pass of `XREAD`: for each stream, the entries after its
/// ID — at most `count` per stream, and within `caps` across all of them.
fn read_streams(
    ctx: &Ctx,
    resp: u8,
    keys: &[&str],
    after: &[StreamId],
    count: usize,
    caps: ReadCaps,
) -> Vec<Vec<u8>> {
    let mut budget = caps.budget();
    let mut results = Vec::new();
    for (key, after_id) in keys.iter().zip(after) {
        if budget.exhausted() {
            break;
        }
        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, key);
        let map = read_lock!(shard);
        let Some(KiviDBValue::Stream(stream)) = map.get(*key) else {
            continue;
        };
        let inner = stream.read();

        let mut entries: Vec<Vec<u8>> = Vec::new();
        let newer = (Bound::Excluded(after_id.clone()), Bound::Unbounded);
        for (id, entry) in inner.entries.range(newer).take(count) {
            let item = format_entry(id, entry);
            if !budget.admit(item.len()) {
                break;
            }
            entries.push(item);
        }
        // Keys with nothing new are left out of the reply.
        if !entries.is_empty() {
            results.extend(stream_result_items(resp, key, entries));
        }
    }
    results
}

pub fn xread(ctx: &mut Ctx) -> Vec<u8> {
    // XREAD [COUNT count] [MAXCOUNT maxcount] [MAXSIZE maxsize] [BLOCK ms]
    //       STREAMS key [key ...] id [id ...]
    let resp = reply_resp(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'xread' command");
    }

    let mut count: Option<usize> = None;
    let mut caps = ReadCaps::default();
    let mut block_ms: Option<u64> = None;
    let mut i = 1usize;

    while i < cmd.len() {
        let tok = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        match tok.as_str() {
            "COUNT" => {
                count = cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok());
                i += 2;
            }
            "MAXCOUNT" => {
                match ReadCaps::parse_positive("MAXCOUNT", cmd.get(i + 1)) {
                    Ok(n) => caps.max_count = Some(n),
                    Err(e) => return resp_error(&e),
                }
                i += 2;
            }
            "MAXSIZE" => {
                match ReadCaps::parse_positive("MAXSIZE", cmd.get(i + 1)) {
                    Ok(n) => caps.max_size = Some(n),
                    Err(e) => return resp_error(&e),
                }
                i += 2;
            }
            "BLOCK" => {
                block_ms = cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok());
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => {
                i += 1;
            }
        }
    }
    if let Err(e) = caps.check_against_count(count) {
        return resp_error(&e);
    }
    let count = count.unwrap_or(usize::MAX);

    // After STREAMS: [key...] [id...]  — equal number of each
    let remaining = cmd.len().saturating_sub(i);
    if remaining < 2 || !remaining.is_multiple_of(2) {
        return resp_error(
            "ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.",
        );
    }
    let half = remaining / 2;
    let keys: Vec<&str> = cmd[i..i + half]
        .iter()
        .map(|b| std::str::from_utf8(b).unwrap_or(""))
        .collect();
    let ids: Vec<&str> = cmd[i + half..]
        .iter()
        .map(|b| std::str::from_utf8(b).unwrap_or(""))
        .collect();

    // Are we being resumed after a block? If so the IDs were already resolved
    // when the command first arrived and must be reused verbatim — see
    // `BlockState::StreamIds`. Take this before resolving anything.
    let resumed = blocking::take_resume(ctx);
    let (carried_ids, carried_deadline) = match blocking::take_state(ctx) {
        Some(blocking::BlockState::StreamIds { ids, deadline }) => (Some(ids), Some(deadline)),
        _ => (None, None),
    };
    let timed_out = matches!(resumed, Some(None));

    // Resolve each ID. "$" means "only entries added after this command";
    // "+" means "the last entry", whatever it is.
    let resolved_ids: Vec<StreamId> = match carried_ids {
        Some(ids) => ids,
        None => keys
            .iter()
            .zip(ids.iter())
            .map(|(key, id_str)| {
                if *id_str == "$" || *id_str == "+" {
                    let db_ref = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db_ref, key);
                    let map = read_lock!(shard);
                    let Some(KiviDBValue::Stream(s)) = map.get(*key) else {
                        return StreamId::min();
                    };
                    let inner = s.read();
                    if *id_str == "$" {
                        inner.last_id.clone()
                    } else {
                        match inner.entries.keys().next_back() {
                            Some(last) => id_before(last),
                            None => inner.last_id.clone(),
                        }
                    }
                } else {
                    StreamId::parse(id_str).unwrap_or(StreamId::min())
                }
            })
            .collect(),
    };

    let results = read_streams(ctx, resp, &keys, &resolved_ids, count, caps);

    if results.is_empty() {
        // Resumed, and the re-read above still found nothing. If the wait
        // timed out, that is the answer. If the stream was written but not
        // with anything to read — an entry added and deleted again in one
        // transaction, the key replaced by something else — the client goes
        // on waiting, as it does in Redis, for the rest of its time.
        if timed_out {
            return resp_null_array();
        }
        if let Some(ms) = block_ms {
            let deadline = carried_deadline.unwrap_or_else(|| {
                blocking::timeout_from_ms(ms).map(|timeout| std::time::Instant::now() + timeout)
            });
            let Some(remaining) = blocking::remaining_time(deadline) else {
                return resp_null_array();
            };
            // Register on all keys FIRST — before the second check.
            // This ensures we cannot miss a notification that arrives
            // between our first read (which found nothing) and registration.
            let rxs: Vec<(
                String,
                tokio::sync::oneshot::Receiver<crate::wait_table::Value>,
            )> = keys
                .iter()
                .map(|k| (k.to_string(), ctx.wait_table.register(k)))
                .collect();

            // SECOND CHECK: re-read immediately after registering.
            // If an XADD landed in the window between our first read and
            // registration, we catch it here and skip the block entirely.
            // This closes the TOCTOU gap.
            let immediate = read_streams(ctx, resp, &keys, &resolved_ids, count, caps);
            if !immediate.is_empty() {
                // Data arrived in the registration window — return it directly.
                // The registered receivers will be dropped here, which closes
                // the oneshot channels cleanly (senders see closed and skip).
                return stream_results_reply(resp, immediate);
            }

            // Nothing in the second check either — safe to park now. The
            // re-read on wake is the top of this function running again, which
            // is why the resolved IDs travel with the park.
            return match blocking::park_with_state(
                ctx,
                rxs,
                remaining,
                Some(blocking::BlockState::StreamIds {
                    ids: resolved_ids,
                    deadline,
                }),
            ) {
                ParkOutcome::Parked => Vec::new(),
                ParkOutcome::CannotWait => resp_null_array(),
            };
        }
        return resp_null_array();
    }

    stream_results_reply(resp, results)
}

// ── XDEL ─────────────────────────────────────────────────────────────────────

pub fn xdel(ctx: &mut Ctx) -> Vec<u8> {
    // XDEL key id [id ...]
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'xdel' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            let mut removed = 0i64;
            for id_bytes in &cmd[2..] {
                let id_str = std::str::from_utf8(id_bytes).unwrap_or("");
                if let Some(id) = StreamId::parse(id_str)
                    && inner.entries.remove(&id).is_some()
                {
                    removed += 1;
                    if id > inner.max_deleted_id {
                        inner.max_deleted_id = id;
                    }
                }
            }
            resp_integer(removed)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XTRIM ─────────────────────────────────────────────────────────────────────

pub fn xtrim(ctx: &mut Ctx) -> Vec<u8> {
    // XTRIM key <MAXLEN | MINID> [= | ~] threshold [LIMIT count]
    //       [KEEPREF | DELREF | ACKED]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'xtrim' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let mut i = 3usize;
    // Optional = or ~
    let mut approx = false;
    if let Some(b) = cmd.get(i)
        && (*b == b"=" || *b == b"~")
    {
        approx = *b == b"~";
        i += 1;
    }
    let threshold_str = match cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
        Some(s) => s,
        None => return resp_error("ERR syntax error"),
    };
    // A threshold that does not parse is an error. It used to be read as 0,
    // so `XTRIM s MAXLEN 1O0` — a typo — emptied the stream.
    let to = if cmd[2].eq_ignore_ascii_case(b"MAXLEN") {
        match threshold_str.parse::<u64>() {
            Ok(max) => TrimTo::MaxLen(max),
            Err(_) => return resp_error("ERR value is not an integer or out of range"),
        }
    } else if cmd[2].eq_ignore_ascii_case(b"MINID") {
        match StreamId::parse(threshold_str) {
            Some(min) => TrimTo::MinId(min),
            None => {
                return resp_error("ERR Invalid stream ID specified as stream command argument");
            }
        }
    } else {
        return resp_error("ERR syntax error");
    };
    i += 1;

    let mut refs = TrimRefs::default();
    let mut limit: Option<i64> = None;
    while i < cmd.len() {
        if cmd[i].eq_ignore_ascii_case(b"LIMIT") && i + 1 < cmd.len() {
            limit = Some(
                parse_utf8(cmd[i + 1])
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(-1),
            );
            i += 2;
        } else if let Some(mode) = TrimRefs::parse(cmd[i]) {
            refs = mode;
            i += 1;
        } else {
            return resp_error("ERR syntax error");
        }
    }

    let approx = match approx_of(approx, limit) {
        Ok(approx) => approx,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            let removed = match approx {
                None => trim_stream(&mut inner, &to, refs),
                Some(approx) => {
                    // Propagated as what it removed; see `exact_trim_args`.
                    effects_begin();
                    let removed = trim_stream_approx(&mut inner, &to, refs, approx);
                    if removed > 0 {
                        let mut parts: Vec<Vec<u8>> = cmd.iter().map(|b| b.to_vec()).collect();
                        let end = parts.len();
                        stream_extra::exact_trim_args(&mut parts, 2, end, &inner);
                        effect(parts);
                    }
                    removed
                }
            };
            resp_integer(removed as i64)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XSETID ───────────────────────────────────────────────────────────────────

pub fn xsetid(ctx: &mut Ctx) -> Vec<u8> {
    // XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'xsetid' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let id_str = std::str::from_utf8(cmd[2]).unwrap_or("");
    let new_last_id = match StreamId::parse(id_str) {
        Some(id) => id,
        None => return resp_error("ERR Invalid stream ID specified as stream command argument"),
    };

    let mut entries_added: Option<u64> = None;
    let mut max_deleted_id: Option<StreamId> = None;
    let mut i = 3usize;
    while i < cmd.len() {
        let Some(value) = cmd.get(i + 1) else {
            return resp_error("ERR syntax error");
        };
        if cmd[i].eq_ignore_ascii_case(b"ENTRIESADDED") {
            entries_added = match parse_utf8(value).and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if n >= 0 => Some(n as u64),
                Some(_) => return resp_error("ERR entries_added must be positive"),
                None => return resp_error("ERR value is not an integer or out of range"),
            };
        } else if cmd[i].eq_ignore_ascii_case(b"MAXDELETEDID") {
            max_deleted_id = match parse_utf8(value).and_then(StreamId::parse) {
                Some(id) if new_last_id < id => {
                    return resp_error(
                        "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                    );
                }
                Some(id) => Some(id),
                None => {
                    return resp_error(
                        "ERR Invalid stream ID specified as stream command argument",
                    );
                }
            };
        } else {
            return resp_error("ERR syntax error");
        }
        i += 2;
    }

    // The stream has to be there: `XSETID` used to create it.
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);
    let stream = match map.get(key.as_str()) {
        Some(KiviDBValue::Stream(s)) => s,
        Some(_) => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        None => return resp_error("ERR no such key"),
    };
    let mut inner = stream.write();
    // The last ID cannot be put behind what the stream has held: an entry
    // deleted from it, or the entry at its top.
    if new_last_id < inner.max_deleted_id {
        return resp_error(
            "ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
        );
    }
    if let Some(top) = inner.entries.keys().next_back() {
        if new_last_id < *top {
            return resp_error(
                "ERR The ID specified in XSETID is smaller than the target stream top item",
            );
        }
        if entries_added.is_some_and(|n| n < inner.entries.len() as u64) {
            return resp_error(
                "ERR The entries_added specified in XSETID is smaller than the target stream length",
            );
        }
    }
    inner.last_id = new_last_id;
    if let Some(n) = entries_added {
        inner.entries_added = n;
    }
    if let Some(id) = max_deleted_id.filter(|id| *id != StreamId::min()) {
        inner.max_deleted_id = id;
    }
    resp_simple("OK")
}

// ── XGROUP CREATE ─────────────────────────────────────────────────────────────

/// `ENTRIESREAD n`: a count, or -1 for "not known".
fn entries_read_arg(raw: &[u8]) -> Result<u64, Vec<u8>> {
    match parse_utf8(raw).and_then(|s| s.parse::<i64>().ok()) {
        Some(-1) => Ok(ConsumerGroup::NO_COUNT),
        Some(n) if n >= 0 => Ok(n as u64),
        Some(_) => Err(resp_error(
            "ERR value for ENTRIESREAD must be positive or -1",
        )),
        None => Err(resp_error("ERR value is not an integer or out of range")),
    }
}

pub fn xgroup_create(ctx: &mut Ctx) -> Vec<u8> {
    let (key, group, id_str, mkstream, entries_read) = {
        let cmd = &ctx.cmd;
        if cmd.len() < 5 {
            return resp_error("ERR wrong number of arguments for 'xgroup|create' command");
        }

        let key = match parse_utf8(cmd[2]) {
            Some(k) => k.to_string(),
            None => return resp_error("ERR key must be valid UTF-8"),
        };
        let group = match parse_utf8(cmd[3]) {
            Some(k) => k.to_string(),
            None => return resp_error("ERR group must be valid UTF-8"),
        };
        let id_str = std::str::from_utf8(cmd[4]).unwrap_or("$").to_string();

        let mkstream = cmd.iter().any(|b| {
            std::str::from_utf8(b)
                .map(|s| s.eq_ignore_ascii_case("MKSTREAM"))
                .unwrap_or(false)
        });
        let entries_read = match cmd[5..]
            .windows(2)
            .find(|w| w[0].eq_ignore_ascii_case(b"ENTRIESREAD"))
        {
            // Not said: not known. It is worked out, where it can be, when
            // the group first reads. See `stream_extra::Shape`.
            None => ConsumerGroup::NO_COUNT,
            Some(w) => match entries_read_arg(w[1]) {
                Ok(n) => n,
                Err(e) => return e,
            },
        };

        (key, group, id_str, mkstream, entries_read)
        // ← &ctx.cmd borrow ends here
    };

    {
        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, &key);
        let map = read_lock!(shard);
        if !map.contains_key(key.as_str()) && !mkstream {
            return resp_error("ERR The XGROUP subcommand requires the key to exist");
        }
    }

    let stream = match get_or_create_stream(ctx, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut inner = stream.write();

    if inner.groups.contains_key(&group) {
        return resp_error("BUSYGROUP Consumer Group name already exists");
    }

    let last_id = if id_str == "$" {
        inner.last_id.clone()
    } else {
        match parse_range_id_lo(&id_str) {
            Some(id) => id,
            None => {
                return resp_error("ERR Invalid stream ID specified as stream command argument");
            }
        }
    };

    inner.groups.insert(
        group.clone(),
        ConsumerGroup {
            name: group,
            last_id,
            entries_read,
            pel: BTreeMap::new(),
            consumers: AHashMap::new(),
            active: AHashMap::new(),
        },
    );

    resp_simple("OK")
}

pub fn xgroup_destroy(ctx: &mut Ctx) -> Vec<u8> {
    // XGROUP DESTROY key group
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return resp_error("ERR wrong number of arguments for 'xgroup|destroy' command");
    }
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[3]) {
        Some(k) => k,
        None => return resp_error("ERR group must be valid UTF-8"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            resp_integer(inner.groups.remove(group).is_some() as i64)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

pub fn xgroup_setid(ctx: &mut Ctx) -> Vec<u8> {
    // XGROUP SETID key group id|$ [ENTRIESREAD n]
    let cmd = &ctx.cmd;
    if cmd.len() != 5 && cmd.len() != 7 {
        return resp_error("ERR wrong number of arguments for 'xgroup|setid' command");
    }
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[3]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR group must be valid UTF-8"),
    };
    let id_str = std::str::from_utf8(cmd[4]).unwrap_or("$");
    let entries_read: Option<u64> = if cmd.len() == 7 {
        if !cmd[5].eq_ignore_ascii_case(b"ENTRIESREAD") {
            return resp_error("ERR syntax error");
        }
        match entries_read_arg(cmd[6]) {
            Ok(n) => Some(n),
            Err(e) => return e,
        }
    } else {
        None
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error("ERR no such key"),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            let new_id = if id_str == "$" {
                inner.last_id.clone()
            } else {
                match parse_range_id_lo(id_str) {
                    Some(id) => id,
                    None => {
                        return resp_error(
                            "ERR Invalid stream ID specified as stream command argument",
                        );
                    }
                }
            };
            match inner.groups.get_mut(&group) {
                None => resp_error(&format!(
                    "NOGROUP No such consumer group '{group}' for key name '{key}'"
                )),
                Some(g) => {
                    g.last_id = new_id;
                    // A new position without a count is a position whose
                    // count is not known.
                    g.entries_read = entries_read.unwrap_or(ConsumerGroup::NO_COUNT);
                    resp_simple("OK")
                }
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

pub fn xgroup_createconsumer(ctx: &mut Ctx) -> Vec<u8> {
    // XGROUP CREATECONSUMER key group consumer
    let cmd = &ctx.cmd;
    if cmd.len() != 5 {
        return resp_error("ERR wrong number of arguments for 'xgroup|createconsumer' command");
    }
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[3]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group name"),
    };
    let consumer = match parse_utf8(cmd[4]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid consumer name"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error("ERR no such key"),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            match inner.groups.get_mut(&group) {
                None => resp_error(&format!(
                    "NOGROUP No such consumer group '{group}' for key name '{key}'"
                )),
                Some(g) => {
                    if g.consumers.contains_key(&consumer) {
                        resp_integer(0) // already exists
                    } else {
                        g.seen(&consumer, now_ms(), false);
                        resp_integer(1)
                    }
                }
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

pub fn xgroup_delconsumer(ctx: &mut Ctx) -> Vec<u8> {
    // XGROUP DELCONSUMER key group consumer  → count of pending messages removed
    let cmd = &ctx.cmd;
    if cmd.len() != 5 {
        return resp_error("ERR wrong number of arguments for 'xgroup|delconsumer' command");
    }
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[3]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group name"),
    };
    let consumer = match parse_utf8(cmd[4]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid consumer name"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error("ERR no such key"),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            match inner.groups.get_mut(&group) {
                None => resp_error(&format!(
                    "NOGROUP No such consumer group '{group}' for key name '{key}'"
                )),
                Some(g) => {
                    if g.consumers.remove(&consumer).is_some() {
                        notify(
                            ctx.pubsub,
                            *ctx.selected_db,
                            KS_STREAM,
                            "xgroup-delconsumer",
                            key,
                        );
                    }
                    g.active.remove(&consumer);
                    // Remove all PEL entries for this consumer
                    let pending: i64 =
                        g.pel.values().filter(|p| p.consumer == consumer).count() as i64;
                    g.pel.retain(|_, p| p.consumer != consumer);
                    resp_integer(pending)
                }
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XREADGROUP ───────────────────────────────────────────────────────────────

/// Everything `XREADGROUP` was asked for, apart from the streams themselves.
struct GroupRead<'a> {
    group: &'a str,
    consumer: &'a str,
    /// `COUNT`: the per-stream limit.
    count: usize,
    noack: bool,
    /// `CLAIM min-idle-time` (Redis 8.4), in milliseconds: before reading new
    /// entries, take over pending ones that have been idle at least this long.
    claim_min_idle: Option<u64>,
    /// `MAXCOUNT` / `MAXSIZE` (Redis 8.10).
    caps: ReadCaps,
}

/// An entry as `XREADGROUP … CLAIM` reports it: the usual `[id, fields]` plus
/// how long ago it was last delivered and how many times it had been.
fn format_claimed_entry(
    id: &StreamId,
    entry: &StreamEntry,
    idle_ms: u64,
    deliveries: u64,
) -> Vec<u8> {
    let field_values: Vec<Vec<u8>> = entry
        .fields
        .iter()
        .flat_map(|(f, v)| vec![resp_bulk(f.as_ref()), resp_bulk(v.as_ref())])
        .collect();
    resp_array(vec![
        resp_bulk_str(&id.to_string()),
        resp_array(field_values),
        resp_integer(idle_ms.min(i64::MAX as u64) as i64),
        resp_integer(deliveries.min(i64::MAX as u64) as i64),
    ])
}

fn nogroup_error(key: &str, group: &str) -> Vec<u8> {
    resp_error(&format!(
        "NOGROUP No such key '{key}' or consumer group '{group}' in XREADGROUP with GROUP option"
    ))
}

pub fn xreadgroup(ctx: &mut Ctx) -> Vec<u8> {
    let resp = reply_resp(ctx);
    // XREADGROUP GROUP group consumer [COUNT n] [MAXCOUNT n] [MAXSIZE n]
    //            [BLOCK ms] [CLAIM min-idle-time] [NOACK]
    //            STREAMS key [key...] id [id...]
    let cmd = ctx.cmd.clone();
    if cmd.len() < 7 {
        return resp_error("ERR wrong number of arguments for 'xreadgroup' command");
    }

    // cmd[1] must be "GROUP"
    if !std::str::from_utf8(cmd[1])
        .unwrap_or("")
        .eq_ignore_ascii_case("GROUP")
    {
        return resp_error("ERR syntax error");
    }
    let group = match parse_utf8(cmd[2]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group"),
    };
    let consumer = match parse_utf8(cmd[3]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid consumer"),
    };

    let mut count: Option<usize> = None;
    let mut caps = ReadCaps::default();
    let mut noack = false;
    let mut claim_min_idle: Option<u64> = None;
    let mut i = 4usize;
    let mut block_ms: Option<u64> = None;

    while i < cmd.len() {
        let tok = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        match tok.as_str() {
            "COUNT" => {
                count = cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok());
                i += 2;
            }
            "MAXCOUNT" => {
                match ReadCaps::parse_positive("MAXCOUNT", cmd.get(i + 1)) {
                    Ok(n) => caps.max_count = Some(n),
                    Err(e) => return resp_error(&e),
                }
                i += 2;
            }
            "MAXSIZE" => {
                match ReadCaps::parse_positive("MAXSIZE", cmd.get(i + 1)) {
                    Ok(n) => caps.max_size = Some(n),
                    Err(e) => return resp_error(&e),
                }
                i += 2;
            }
            "BLOCK" => {
                block_ms = cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok());
                i += 2;
            }
            "CLAIM" => {
                claim_min_idle = match cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok())
                {
                    Some(ms) => Some(ms),
                    None => {
                        return resp_error("ERR min-idle-time is not an integer or out of range");
                    }
                };
                i += 2;
            }
            "NOACK" => {
                noack = true;
                i += 1;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => {
                i += 1;
            }
        }
    }
    if let Err(e) = caps.check_against_count(count) {
        return resp_error(&e);
    }

    let remaining = cmd.len().saturating_sub(i);
    if remaining < 2 || !remaining.is_multiple_of(2) {
        return resp_error(
            "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
        );
    }
    let half = remaining / 2;
    let keys: Vec<String> = cmd[i..i + half]
        .iter()
        .filter_map(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()))
        .collect();
    let ids: Vec<&str> = cmd[i + half..]
        .iter()
        .map(|b| std::str::from_utf8(b).unwrap_or(""))
        .collect();

    let spec = GroupRead {
        group: &group,
        consumer: &consumer,
        count: count.unwrap_or(usize::MAX),
        noack,
        claim_min_idle,
        caps,
    };

    // A resume just re-runs delivery: unlike XREAD, every blocking XREADGROUP
    // reads with ">" ("entries never delivered to this group"), which is
    // relative to group state rather than to when the command arrived — so
    // there is nothing to carry across the park except when the wait is to
    // end.
    //
    // It is woken by an entry being added, and by anything else that writes
    // the key: the stream deleted, or replaced by something that is not a
    // stream, or the group destroyed — which is an error to report, where
    // Redis reports it, rather than a wait that can no longer end. A write
    // that changes nothing for this reader sends it back to wait.
    //
    // Clients blocked on the same stream are woken one at a time, in the
    // order they blocked — the entry goes to the one that has waited longest
    // — and each gives the turn to the one behind it once it has looked,
    // which may be reading for another group.
    let resumed = blocking::take_resume(ctx);
    let timed_out = matches!(resumed, Some(None));
    let (carried_deadline, ticket) = match blocking::take_state(ctx) {
        Some(blocking::BlockState::GroupRead { deadline, ticket }) => (Some(deadline), ticket),
        _ => (None, ctx.wait_table.new_ticket()),
    };
    // The command is over: out of the line, and the next client's turn.
    let done = |ctx: &Ctx, reply: Vec<u8>| {
        if resumed.is_some() {
            for key in &keys {
                ctx.wait_table.reader_done(key, &ticket);
            }
        }
        reply
    };

    let results = match run_xreadgroup_delivery(ctx, &keys, &ids, &spec) {
        Ok(results) => results,
        Err(e) => return done(ctx, e),
    };
    if !results.is_empty() {
        return done(ctx, stream_results_reply(resp, results));
    }

    // Nothing to deliver. With CLAIM the wait may have been ended by its own
    // timer — "an entry may be claimable now" — rather than by the client's
    // timeout; and any wait may have been ended by a write that left nothing
    // for this reader. If the client's time is not up, it waits again.
    let claim_min_idle = spec.claim_min_idle;
    let waiting_on = match (resumed.is_some(), carried_deadline) {
        (true, Some(deadline)) if claim_min_idle.is_some() || !timed_out => Some(deadline),
        (true, _) => None,
        (false, _) => block_ms.map(|ms| {
            blocking::timeout_from_ms(ms).map(|timeout| std::time::Instant::now() + timeout)
        }),
    };
    let Some((deadline, remaining)) = waiting_on
        .and_then(|deadline| blocking::remaining_time(deadline).map(|left| (deadline, left)))
    else {
        return done(ctx, resp_null_array());
    };

    // Stand in line on every key BEFORE the second check — closes the TOCTOU
    // gap. (A history read never gets here: its reply always names the
    // stream, so it is never empty.)
    let rxs: Vec<(
        String,
        tokio::sync::oneshot::Receiver<crate::wait_table::Value>,
    )> = keys
        .iter()
        .map(|k| (k.clone(), ctx.wait_table.enqueue_reader(k, &ticket)))
        .collect();
    let leave = |ctx: &Ctx| {
        for key in &keys {
            ctx.wait_table.reader_done(key, &ticket);
        }
    };

    // Second check: data may have arrived between the first read and standing
    // in line. Re-run the full delivery path inline — we need group mutation,
    // not just a read.
    let second_check = match run_xreadgroup_delivery(ctx, &keys, &ids, &spec) {
        Ok(results) => results,
        Err(e) => {
            drop(rxs);
            leave(ctx);
            return e;
        }
    };
    if !second_check.is_empty() {
        drop(rxs);
        leave(ctx);
        // `stream_results_reply`, not `resp_array`: under RESP3 this is a map
        // keyed by stream name, and the non-blocking return above already
        // frames it that way. This path and the post-wake one used to emit a
        // flat array regardless, so a RESP3 client got a structurally
        // different reply whenever it happened to block — invisible to
        // RESP2, and only reachable when blocking, which is why it survived
        // the RESP3 sweep.
        return stream_results_reply(resp, second_check);
    }
    // Woken, and nothing for this reader: the one behind it has its turn.
    if resumed.is_some() {
        for key in &keys {
            ctx.wait_table.reader_pass_on(key, &ticket);
        }
    }

    // Nothing yet — park until the stream is written, or the timeout. On
    // resume this function re-runs and `run_xreadgroup_delivery` above does
    // the delivery, so there is no separate post-wake path to keep in sync.
    //
    // With CLAIM, the park ends no later than the moment the first pending
    // entry will have been idle for long enough.
    let wait = match claim_min_idle {
        Some(min_idle) => {
            let claimable = next_claimable_in(ctx, &keys, spec.group, min_idle);
            match (remaining, claimable) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            }
        }
        None => remaining,
    };
    let state = blocking::BlockState::GroupRead {
        deadline,
        ticket: Arc::clone(&ticket),
    };
    match blocking::park_with_state(ctx, rxs, wait, Some(state)) {
        ParkOutcome::Parked => Vec::new(),
        ParkOutcome::CannotWait => {
            leave(ctx);
            resp_null_array()
        }
    }
}

/// How long until some entry pending in `group`, in one of `keys`, will have
/// been idle for `min_idle` milliseconds — `None` if nothing is pending.
fn next_claimable_in(
    ctx: &Ctx,
    keys: &[String],
    group: &str,
    min_idle: u64,
) -> Option<std::time::Duration> {
    let now = now_ms();
    let mut soonest: Option<u64> = None;
    for key in keys {
        let shard = store::get_shard(&ctx.store[*ctx.selected_db], key.as_str());
        let map = read_lock!(shard);
        let Some(KiviDBValue::Stream(stream)) = map.get(key.as_str()) else {
            continue;
        };
        let inner = stream.read();
        let Some(g) = inner.groups.get(group) else {
            continue;
        };
        for pending in g.pel.values() {
            let at = pending.delivered_at.saturating_add(min_idle);
            soonest = Some(soonest.map_or(at, |s| s.min(at)));
        }
    }
    // Never zero: a park that ends at once, finds nothing and parks again
    // would spin.
    soonest.map(|at| std::time::Duration::from_millis(at.saturating_sub(now).max(1)))
}

/// Runs the non-blocking XREADGROUP delivery for each (key, id) pair.
/// Returns formatted response entries — empty vec means nothing available.
/// This is the core of xreadgroup without any blocking logic.
///
/// Every pending-list change made here is recorded as an effect (see
/// `script_feed::effects_begin`) and that record — not the command — is what
/// replicas and the AOF receive.
fn run_xreadgroup_delivery(
    ctx: &mut Ctx,
    keys: &[String],
    ids: &[&str],
    spec: &GroupRead,
) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    // Returns the flat item list; the caller frames it with
    // `stream_results_reply`, so both protocols share this code path.
    let resp = reply_resp(ctx);
    let now = now_ms();
    effects_begin();

    // Find every stream and group before delivering from any of them, so a
    // missing key or group fails the command with nothing half-delivered.
    // (They used to be skipped silently, and a missing group produced the
    // same nil as "no new entries".)
    let mut streams = Vec::with_capacity(keys.len());
    for key in keys {
        let db_ref = &ctx.store[*ctx.selected_db];
        let shard = store::get_shard(db_ref, key.as_str());
        let map = read_lock!(shard);
        match map.get(key.as_str()) {
            Some(KiviDBValue::Stream(s)) => {
                if !s.read().groups.contains_key(spec.group) {
                    return Err(nogroup_error(key, spec.group));
                }
                streams.push(Arc::clone(s));
            }
            Some(_) => {
                return Err(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            None => return Err(nogroup_error(key, spec.group)),
        }
    }

    let mut budget = spec.caps.budget();
    let mut results: Vec<Vec<u8>> = Vec::new();

    for ((key, id_str), stream) in keys.iter().zip(ids.iter()).zip(streams) {
        let mut guard = stream.write();
        let shape = stream_extra::Shape::of(&guard);
        // Split the borrow: entries are read while the group is updated.
        let StreamInner {
            entries, groups, ..
        } = &mut *guard;
        let Some(g) = groups.get_mut(spec.group) else {
            // Destroyed between the check above and here.
            continue;
        };

        let is_new = !g.consumers.contains_key(spec.consumer);
        g.seen(spec.consumer, now, false);
        if is_new {
            notify(
                ctx.pubsub,
                *ctx.selected_db,
                KS_STREAM,
                "xgroup-createconsumer",
                key,
            );
            effect(
                [
                    "XGROUP",
                    "CREATECONSUMER",
                    key.as_str(),
                    spec.group,
                    spec.consumer,
                ]
                .iter()
                .map(|s| s.as_bytes().to_vec())
                .collect(),
            );
        }

        let mut items: Vec<Vec<u8>> = Vec::new();
        let history = *id_str != ">";

        if !history {
            // ── 1. CLAIM: idle pending entries first ─────────────────────
            if let Some(min_idle) = spec.claim_min_idle {
                // Entries released with XNACK (delivery time 0) sort first,
                // then the longest idle.
                let mut candidates: Vec<(u64, StreamId)> = g
                    .pel
                    .iter()
                    .filter(|(_, p)| now.saturating_sub(p.delivered_at) >= min_idle)
                    .map(|(id, p)| (p.delivered_at, id.clone()))
                    .collect();
                candidates.sort();
                for (_, id) in candidates {
                    if items.len() >= spec.count || budget.exhausted() {
                        break;
                    }
                    // Pending but since deleted from the stream: nothing to
                    // hand over.
                    let Some(entry) = entries.get(&id) else {
                        continue;
                    };
                    let Some(pending) = g.pel.get_mut(&id) else {
                        continue;
                    };
                    let idle = if pending.delivered_at == 0 {
                        0
                    } else {
                        now.saturating_sub(pending.delivered_at)
                    };
                    let item = format_claimed_entry(&id, entry, idle, pending.delivery_count);
                    if !budget.admit(item.len()) {
                        break;
                    }
                    pending.consumer = spec.consumer.to_string();
                    pending.delivered_at = now;
                    pending.delivery_count = pending.delivery_count.saturating_add(1);
                    effect(claim_effect(
                        key,
                        spec.group,
                        spec.consumer,
                        &id,
                        now,
                        pending.delivery_count,
                    ));
                    items.push(item);
                }
            }

            // ── 2. Entries never delivered to this group ─────────────────
            let newer = (Bound::Excluded(g.last_id.clone()), Bound::Unbounded);
            let mut delivered = 0u64;
            for (id, entry) in entries.range(newer) {
                if items.len() >= spec.count || budget.exhausted() {
                    break;
                }
                // With CLAIM every entry has the extended shape, so a client
                // can parse the reply uniformly; a new entry has never been
                // delivered, hence 0 and 0.
                let item = if spec.claim_min_idle.is_some() {
                    format_claimed_entry(id, entry, 0, 0)
                } else {
                    format_entry(id, entry)
                };
                // Checked before the entry is delivered: one that does not
                // fit is neither sent nor added to the pending list.
                if !budget.admit(item.len()) {
                    break;
                }
                if !spec.noack {
                    g.pel.insert(
                        id.clone(),
                        PelEntry {
                            consumer: spec.consumer.to_string(),
                            delivered_at: now,
                            delivery_count: 1,
                        },
                    );
                    effect(claim_effect(key, spec.group, spec.consumer, id, now, 1));
                }
                shape.deliver(g, id);
                delivered += 1;
                items.push(item);
            }
            if delivered > 0 {
                effect(setid_effect(key, spec.group, &g.last_id, g.entries_read));
            }
        } else {
            // ── History: this consumer's own pending entries after `id` ──
            let from = StreamId::parse(id_str).unwrap_or(StreamId::min());
            let after = if from == StreamId::min() {
                Bound::Unbounded
            } else {
                Bound::Excluded(from)
            };
            let pending_ids: Vec<StreamId> = g
                .pel
                .range((after, Bound::Unbounded))
                .filter(|(_, p)| p.consumer == spec.consumer)
                .take(spec.count)
                .map(|(id, _)| id.clone())
                .collect();
            for id in pending_ids {
                if budget.exhausted() {
                    break;
                }
                let item = match entries.get(&id) {
                    Some(entry) => format_entry(&id, entry),
                    // Still pending, but deleted from the stream since.
                    None => resp_array(vec![resp_bulk_str(&id.to_string()), resp_null_for(resp)]),
                };
                if !budget.admit(item.len()) {
                    break;
                }
                if let Some(pending) = g.pel.get_mut(&id) {
                    pending.delivered_at = now;
                    pending.delivery_count = pending.delivery_count.saturating_add(1);
                    // Not a change of the dataset, to Redis (#27).
                    effect_of_metadata(claim_effect(
                        key,
                        spec.group,
                        spec.consumer,
                        &id,
                        now,
                        pending.delivery_count,
                    ));
                }
                items.push(item);
            }
        }

        if !items.is_empty() {
            g.seen(spec.consumer, now, true);
        }
        // A history read always names the stream, with an empty list when
        // nothing is pending — that empty list is how a consumer learns it
        // has caught up. A read of new entries leaves empty streams out.
        if history || !items.is_empty() {
            results.extend(stream_result_items(resp, key, items));
        }
    }

    Ok(results)
}
// ── XACK ─────────────────────────────────────────────────────────────────────

pub fn xack(ctx: &mut Ctx) -> Vec<u8> {
    // XACK key group id [id ...]
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'xack' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[2]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group"),
    };
    // All of the IDs, before any is acknowledged: one that is not an ID
    // fails the command, and it must not have done half its work by then.
    let mut ids = Vec::with_capacity(cmd.len() - 3);
    for raw in &cmd[3..] {
        match parse_utf8(raw).and_then(StreamId::parse) {
            Some(id) => ids.push(id),
            None => {
                return resp_error("ERR Invalid stream ID specified as stream command argument");
            }
        }
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            match inner.groups.get_mut(&group) {
                None => resp_integer(0),
                Some(g) => {
                    let acked = ids.iter().filter(|id| g.pel.remove(id).is_some()).count();
                    resp_integer(acked as i64)
                }
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XPENDING ─────────────────────────────────────────────────────────────────

pub fn xpending(ctx: &mut Ctx) -> Vec<u8> {
    let resp = reply_resp(ctx);
    // XPENDING key group [[IDLE min-idle] start end count [consumer]]
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'xpending' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[2]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error("ERR no such key"),
        Some(KiviDBValue::Stream(s)) => {
            let inner = s.read();
            let g = match inner.groups.get(&group) {
                None => return resp_error("NOGROUP No such consumer group"),
                Some(g) => g,
            };

            if cmd.len() == 3 {
                // Summary form: [count, min-id, max-id, [[consumer, count], ...]]
                let count = g.pel.len() as i64;
                let min_id = g
                    .pel
                    .keys()
                    .next()
                    .map(|id| id.to_string())
                    .unwrap_or_default();
                let max_id = g
                    .pel
                    .keys()
                    .next_back()
                    .map(|id| id.to_string())
                    .unwrap_or_default();

                // Consumer → pending count
                let mut per_consumer: AHashMap<&str, i64> = AHashMap::new();
                for pel in g.pel.values() {
                    *per_consumer.entry(pel.consumer.as_str()).or_insert(0) += 1;
                }
                let consumer_list: Vec<Vec<u8>> = per_consumer
                    .iter()
                    .map(|(name, cnt)| {
                        resp_array(vec![resp_bulk_str(name), resp_bulk_str(&cnt.to_string())])
                    })
                    .collect();

                // An empty PEL reports nil min/max IDs, nested inside the
                // summary array — not reachable by top-level normalisation.
                resp_array(vec![
                    resp_integer(count),
                    if min_id.is_empty() {
                        resp_null_for(resp)
                    } else {
                        resp_bulk_str(&min_id)
                    },
                    if max_id.is_empty() {
                        resp_null_for(resp)
                    } else {
                        resp_bulk_str(&max_id)
                    },
                    resp_array(consumer_list),
                ])
            } else {
                // Extended form: XPENDING key group [IDLE ms] start end count [consumer]
                let mut i = 3usize;
                let mut min_idle: Option<u64> = None;

                if std::str::from_utf8(cmd[i])
                    .unwrap_or("")
                    .eq_ignore_ascii_case("IDLE")
                {
                    min_idle = cmd
                        .get(i + 1)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse().ok());
                    i += 2;
                }

                let start_str =
                    std::str::from_utf8(cmd.get(i).copied().unwrap_or(b"-")).unwrap_or("-");
                let end_str =
                    std::str::from_utf8(cmd.get(i + 1).copied().unwrap_or(b"+")).unwrap_or("+");
                let count: usize = cmd
                    .get(i + 2)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10);
                let filter_consumer: Option<&str> =
                    cmd.get(i + 3).and_then(|b| std::str::from_utf8(b).ok());

                let (lo, hi, count) = match id_interval(start_str, end_str) {
                    Ok(Some((lo, hi))) => (lo, hi, count),
                    Ok(None) => (StreamId::min(), StreamId::min(), 0),
                    Err(e) => return e,
                };
                let now = now_ms();

                let items: Vec<Vec<u8>> = g
                    .pel
                    .range(lo..=hi)
                    .filter(|(_, p)| {
                        if let Some(fc) = filter_consumer {
                            p.consumer == fc
                        } else {
                            true
                        }
                    })
                    .filter(|(_, p)| {
                        if let Some(idle) = min_idle {
                            now.saturating_sub(p.delivered_at) >= idle
                        } else {
                            true
                        }
                    })
                    .take(count)
                    .map(|(id, p)| {
                        // `delivered_at == 0` is the XNACK marker for "released,
                        // never re-delivered". Redis reports that as an idle of
                        // -1 rather than "now", which would otherwise read as an
                        // absurd age measured from the epoch.
                        let idle = if p.delivered_at == 0 {
                            -1
                        } else {
                            now.saturating_sub(p.delivered_at) as i64
                        };
                        resp_array(vec![
                            resp_bulk_str(&id.to_string()),
                            resp_bulk_str(&p.consumer),
                            resp_integer(idle),
                            resp_integer(p.delivery_count as i64),
                        ])
                    })
                    .collect();

                resp_array(items)
            }
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XCLAIM / XAUTOCLAIM ───────────────────────────────────────────────────────

pub fn xclaim(ctx: &mut Ctx) -> Vec<u8> {
    // XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms]
    //        [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]
    let cmd = &ctx.cmd;
    if cmd.len() < 6 {
        return resp_error("ERR wrong number of arguments for 'xclaim' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[2]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group"),
    };
    let new_consumer = match parse_utf8(cmd[3]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid consumer"),
    };
    let min_idle: u64 = match parse_utf8(cmd[4]).and_then(|s| s.parse().ok()) {
        Some(ms) => ms,
        None => return resp_error("ERR Invalid min-idle-time argument for XCLAIM"),
    };

    // Collect IDs — stop when we hit an option keyword
    let is_option = |tok: &[u8]| {
        [
            &b"IDLE"[..],
            b"TIME",
            b"RETRYCOUNT",
            b"FORCE",
            b"JUSTID",
            b"LASTID",
        ]
        .iter()
        .any(|opt| tok.eq_ignore_ascii_case(opt))
    };
    let mut ids: Vec<StreamId> = Vec::new();
    let mut i = 5usize;
    while i < cmd.len() && !is_option(cmd[i]) {
        match parse_utf8(cmd[i]).and_then(StreamId::parse) {
            Some(id) => ids.push(id),
            None => {
                return resp_error("ERR Invalid stream ID specified as stream command argument");
            }
        }
        i += 1;
    }

    let mut justid = false;
    let mut force = false;
    let mut idle: Option<u64> = None;
    let mut time: Option<u64> = None;
    let mut retry_count: Option<u64> = None;
    let mut last_id: Option<StreamId> = None;

    let number = |arg: Option<&&[u8]>| -> Option<u64> {
        arg.and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse().ok())
    };
    while i < cmd.len() {
        let tok = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        match tok.as_str() {
            "JUSTID" => {
                justid = true;
                i += 1;
            }
            "FORCE" => {
                force = true;
                i += 1;
            }
            "IDLE" | "TIME" | "RETRYCOUNT" => {
                let Some(n) = number(cmd.get(i + 1)) else {
                    return resp_error("ERR value is not an integer or out of range");
                };
                match tok.as_str() {
                    "IDLE" => idle = Some(n),
                    "TIME" => time = Some(n),
                    _ => retry_count = Some(n),
                }
                i += 2;
            }
            "LASTID" => {
                last_id = match cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(StreamId::parse)
                {
                    Some(id) => Some(id),
                    None => {
                        return resp_error(
                            "ERR Invalid stream ID specified as stream command argument",
                        );
                    }
                };
                i += 2;
            }
            _ => return resp_error("ERR syntax error"),
        }
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error(&format!(
            "NOGROUP No such key '{key}' or consumer group '{group}'"
        )),
        Some(KiviDBValue::Stream(s)) => {
            let mut guard = s.write();
            let StreamInner {
                entries, groups, ..
            } = &mut *guard;
            let now = now_ms();
            // TIME is an absolute delivery time; IDLE is relative to now.
            let delivered_at = time.unwrap_or_else(|| now.saturating_sub(idle.unwrap_or(0)));

            let Some(g) = groups.get_mut(&group) else {
                return resp_error(&format!(
                    "NOGROUP No such key '{key}' or consumer group '{group}'"
                ));
            };
            effects_begin();
            if !g.consumers.contains_key(&new_consumer) {
                notify(
                    ctx.pubsub,
                    *ctx.selected_db,
                    KS_STREAM,
                    "xgroup-createconsumer",
                    key,
                );
            }
            g.seen(&new_consumer, now, false);
            if let Some(last) = last_id
                && last > g.last_id
            {
                g.last_id = last;
            }

            let mut claimed: Vec<Vec<u8>> = Vec::new();
            for id in &ids {
                let exists = entries.contains_key(id);
                match g.pel.get(id) {
                    // Pending, but the entry itself is gone: drop the
                    // reference rather than hand out nothing.
                    Some(_) if !exists => {
                        g.pel.remove(id);
                        effect(
                            ["XACK", key, group.as_str(), id.to_string().as_str()]
                                .iter()
                                .map(|s| s.as_bytes().to_vec())
                                .collect(),
                        );
                        continue;
                    }
                    Some(p) if now.saturating_sub(p.delivered_at) < min_idle => continue,
                    Some(_) => {}
                    // Not pending: only FORCE claims it, and only if it exists.
                    None if force && exists => {}
                    None => continue,
                }

                let pending = g.pel.entry(id.clone()).or_insert(PelEntry {
                    consumer: new_consumer.clone(),
                    delivered_at,
                    delivery_count: 0,
                });
                pending.consumer = new_consumer.clone();
                pending.delivered_at = delivered_at;
                pending.delivery_count = match retry_count {
                    Some(n) => n,
                    // JUSTID is a transfer of ownership, not a delivery.
                    None if justid => pending.delivery_count,
                    None => pending.delivery_count.saturating_add(1),
                };
                effect(claim_effect(
                    key,
                    &group,
                    &new_consumer,
                    id,
                    delivered_at,
                    pending.delivery_count,
                ));

                if justid {
                    claimed.push(resp_bulk_str(&id.to_string()));
                } else if let Some(entry) = entries.get(id) {
                    claimed.push(format_entry(id, entry));
                }
            }
            if !claimed.is_empty() {
                g.seen(&new_consumer, now, true);
            }
            resp_array(claimed)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

pub fn xautoclaim(ctx: &mut Ctx) -> Vec<u8> {
    // XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
    let cmd = &ctx.cmd;
    if cmd.len() < 6 {
        return resp_error("ERR wrong number of arguments for 'xautoclaim' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[2]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group"),
    };
    let new_consumer = match parse_utf8(cmd[3]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid consumer"),
    };
    let min_idle: u64 = match parse_utf8(cmd[4]).and_then(|s| s.parse().ok()) {
        Some(ms) => ms,
        None => return resp_error("ERR Invalid min-idle-time argument for XAUTOCLAIM"),
    };
    let start = match parse_range_id_lo(std::str::from_utf8(cmd[5]).unwrap_or("")) {
        Some(id) => id,
        None => {
            return resp_error("ERR Invalid stream ID specified as stream command argument");
        }
    };

    let mut count = 100usize;
    let mut justid = false;
    let mut i = 6usize;
    while i < cmd.len() {
        if cmd[i].eq_ignore_ascii_case(b"COUNT") {
            count = match cmd
                .get(i + 1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse::<usize>().ok())
            {
                // The reply is built in one piece, and the scan below looks
                // at ten pending entries for each one asked for.
                Some(n) if n > 0 && n <= (i64::MAX / 16) as usize => n,
                _ => return resp_error("ERR COUNT must be > 0"),
            };
            i += 2;
        } else if cmd[i].eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
            i += 1;
        } else {
            return resp_error("ERR syntax error");
        }
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error(&format!(
            "NOGROUP No such key '{key}' or consumer group '{group}'"
        )),
        Some(KiviDBValue::Stream(s)) => {
            let mut guard = s.write();
            let StreamInner {
                entries, groups, ..
            } = &mut *guard;
            let now = now_ms();

            let Some(g) = groups.get_mut(&group) else {
                return resp_error(&format!(
                    "NOGROUP No such key '{key}' or consumer group '{group}'"
                ));
            };
            effects_begin();
            if !g.consumers.contains_key(&new_consumer) {
                notify(
                    ctx.pubsub,
                    *ctx.selected_db,
                    KS_STREAM,
                    "xgroup-createconsumer",
                    key,
                );
            }
            g.seen(&new_consumer, now, false);

            // Walk the pending list from `start`. An entry that no longer
            // exists in the stream is dropped from the list and reported in
            // the third element of the reply; one that has been idle long
            // enough is claimed. Either counts towards `COUNT`, and the walk
            // looks at no more than ten pending entries for each one asked
            // for: a call's work is bounded however long the list is. The
            // cursor is the pending entry after the last one looked at —
            // where the next call resumes — or 0-0 past the end of the list.
            let scan: Vec<(StreamId, u64)> = g
                .pel
                .range(start..)
                .take(count.saturating_mul(10).saturating_add(1))
                .map(|(id, p)| (id.clone(), p.delivered_at))
                .collect();
            let mut claimed: Vec<Vec<u8>> = Vec::new();
            let mut deleted: Vec<StreamId> = Vec::new();
            let mut cursor = StreamId::min();
            let mut attempts = count.saturating_mul(10);
            for (id, last_delivered) in scan {
                if attempts == 0 || claimed.len() + deleted.len() >= count {
                    cursor = id;
                    break;
                }
                attempts -= 1;
                let Some(entry) = entries.get(&id) else {
                    g.pel.remove(&id);
                    deleted.push(id);
                    continue;
                };
                if now.saturating_sub(last_delivered) < min_idle {
                    continue;
                }
                let Some(pending) = g.pel.get_mut(&id) else {
                    continue;
                };
                pending.consumer = new_consumer.clone();
                pending.delivered_at = now;
                if !justid {
                    pending.delivery_count = pending.delivery_count.saturating_add(1);
                }
                effect(claim_effect(
                    key,
                    &group,
                    &new_consumer,
                    &id,
                    now,
                    pending.delivery_count,
                ));
                claimed.push(if justid {
                    resp_bulk_str(&id.to_string())
                } else {
                    format_entry(&id, entry)
                });
            }
            if !claimed.is_empty() {
                g.seen(&new_consumer, now, true);
            }
            if !deleted.is_empty() {
                let mut xack = vec![
                    b"XACK".to_vec(),
                    key.as_bytes().to_vec(),
                    group.clone().into_bytes(),
                ];
                xack.extend(deleted.iter().map(|id| id.to_string().into_bytes()));
                effect(xack);
            }

            resp_array(vec![
                resp_bulk_str(&cursor.to_string()),
                resp_array(claimed),
                resp_array(
                    deleted
                        .iter()
                        .map(|id| resp_bulk_str(&id.to_string()))
                        .collect(),
                ),
            ])
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XINFO ─────────────────────────────────────────────────────────────────────

pub fn xinfo(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'xinfo' command");
    }
    let subcommand = std::str::from_utf8(cmd[1])
        .unwrap_or("")
        .to_ascii_uppercase();
    match subcommand.as_str() {
        "STREAM" => xinfo_stream(ctx),
        "GROUPS" => xinfo_groups(ctx),
        "CONSUMERS" => xinfo_consumers(ctx),
        _ => resp_error("ERR unknown subcommand for XINFO"),
    }
}

fn xinfo_stream(ctx: &mut Ctx) -> Vec<u8> {
    // XINFO STREAM key [FULL [COUNT n]]
    //
    // Redis builds this with `addReplyMapLen`, which emits a flat 2N array
    // under RESP2 and a `%N` map under RESP3 — exactly what `resp_map_for`
    // does. The first-entry/last-entry nils are nested inside the reply, so
    // they need protocol-correct encoding too.
    let resp = reply_resp(ctx);
    let cmd = &ctx.cmd;
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error("ERR no such key"),
        Some(KiviDBValue::Stream(s)) => {
            let inner = s.read();
            // XINFO STREAM key FULL [COUNT count]
            if let Some(full) = cmd.get(3) {
                if !full.eq_ignore_ascii_case(b"FULL") {
                    return resp_error("ERR syntax error");
                }
                let count = match cmd.len() {
                    4 => 10,
                    6 if cmd[4].eq_ignore_ascii_case(b"COUNT") => {
                        match parse_utf8(cmd[5]).and_then(|s| s.parse::<i64>().ok()) {
                            // Zero is "all of them".
                            Some(0) => usize::MAX,
                            Some(n) if n > 0 => n as usize,
                            _ => {
                                return resp_error("ERR value is not an integer or out of range");
                            }
                        }
                    }
                    _ => return resp_error("ERR syntax error"),
                };
                let idmp_defaults = (
                    ctx.config.stream_idmp_duration,
                    ctx.config.stream_idmp_maxsize,
                );
                return xinfo_stream_full(resp, &inner, count, idmp_defaults);
            }
            let first_entry = inner
                .entries
                .iter()
                .next()
                .map(|(id, e)| format_entry(id, e))
                .unwrap_or_else(|| resp_null_for(resp));
            let last_entry = inner
                .entries
                .iter()
                .next_back()
                .map(|(id, e)| format_entry(id, e))
                .unwrap_or_else(|| resp_null_for(resp));

            let (idmp_duration, idmp_maxsize) = stream_extra::idmp_settings(
                &inner,
                (
                    ctx.config.stream_idmp_duration,
                    ctx.config.stream_idmp_maxsize,
                ),
            );
            let (pids_tracked, iids_tracked) = stream_extra::idmp_tracked(&inner);

            resp_map_for(
                resp,
                vec![
                    resp_bulk_str("length"),
                    resp_integer(inner.entries.len() as i64),
                    // There is no radix tree here. These are what Redis's
                    // would hold: a key for each node of entries (see
                    // `Approx`), and a node or so above them.
                    resp_bulk_str("radix-tree-keys"),
                    resp_integer(stream_extra::node_count(&inner) as i64),
                    resp_bulk_str("radix-tree-nodes"),
                    resp_integer(match stream_extra::node_count(&inner) {
                        0 => 1,
                        nodes => nodes as i64 + 1,
                    }),
                    resp_bulk_str("last-generated-id"),
                    resp_bulk_str(&inner.last_id.to_string()),
                    resp_bulk_str("max-deleted-entry-id"),
                    resp_bulk_str(&inner.max_deleted_id.to_string()),
                    resp_bulk_str("entries-added"),
                    resp_integer(inner.entries_added as i64),
                    resp_bulk_str("recorded-first-entry-id"),
                    resp_bulk_str(&first_entry_id(&inner).to_string()),
                    resp_bulk_str("groups"),
                    resp_integer(inner.groups.len() as i64),
                    // Idempotent production (Redis 8.6).
                    resp_bulk_str("idmp-duration"),
                    resp_integer(idmp_duration as i64),
                    resp_bulk_str("idmp-maxsize"),
                    resp_integer(idmp_maxsize as i64),
                    resp_bulk_str("pids-tracked"),
                    resp_integer(pids_tracked as i64),
                    resp_bulk_str("iids-tracked"),
                    resp_integer(iids_tracked as i64),
                    resp_bulk_str("iids-added"),
                    resp_integer(inner.idmp.iids_added as i64),
                    resp_bulk_str("iids-duplicates"),
                    resp_integer(inner.idmp.iids_duplicates as i64),
                    resp_bulk_str("first-entry"),
                    first_entry,
                    resp_bulk_str("last-entry"),
                    last_entry,
                ],
            )
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

/// The ID of the stream's first entry; `0-0` if it has none.
fn first_entry_id(stream: &StreamInner) -> StreamId {
    stream
        .entries
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(StreamId::min)
}

/// A group's `entries-read` and `lag`, as `XINFO` shows them: nil where
/// the number is not known.
fn read_and_lag(resp: u8, stream: &StreamInner, g: &ConsumerGroup) -> (Vec<u8>, Vec<u8>) {
    let entries_read = if g.entries_read == ConsumerGroup::NO_COUNT {
        resp_null_for(resp)
    } else {
        resp_integer(g.entries_read as i64)
    };
    let lag = match stream_extra::Shape::of(stream).lag(g) {
        Some(lag) => resp_integer(lag as i64),
        None => resp_null_for(resp),
    };
    (entries_read, lag)
}

/// The groups of a stream, by name: the order Redis lists them in.
fn groups_in_order(stream: &StreamInner) -> Vec<&ConsumerGroup> {
    let mut groups: Vec<&ConsumerGroup> = stream.groups.values().collect();
    groups.sort_by(|a, b| a.name.cmp(&b.name));
    groups
}

/// A group's consumers, by name.
fn consumers_in_order(g: &ConsumerGroup) -> Vec<(&String, u64)> {
    let mut consumers: Vec<(&String, u64)> = g
        .consumers
        .iter()
        .map(|(name, seen)| (name, *seen))
        .collect();
    consumers.sort();
    consumers
}

/// `XINFO STREAM key FULL`: the stream, its entries, and every group with
/// its pending list and its consumers with theirs — the first `count` of
/// each.
fn xinfo_stream_full(
    resp: u8,
    inner: &StreamInner,
    count: usize,
    idmp_defaults: (u64, u64),
) -> Vec<u8> {
    let entries: Vec<Vec<u8>> = inner
        .entries
        .iter()
        .take(count)
        .map(|(id, e)| format_entry(id, e))
        .collect();
    let groups: Vec<Vec<u8>> = groups_in_order(inner)
        .into_iter()
        .map(|g| {
            let (entries_read, lag) = read_and_lag(resp, inner, g);
            let pel: Vec<Vec<u8>> = g
                .pel
                .iter()
                .take(count)
                .map(|(id, p)| {
                    resp_array(vec![
                        resp_bulk_str(&id.to_string()),
                        resp_bulk_str(&p.consumer),
                        resp_integer(p.delivered_at as i64),
                        resp_integer(p.delivery_count as i64),
                    ])
                })
                .collect();
            let consumers: Vec<Vec<u8>> = consumers_in_order(g)
                .into_iter()
                .map(|(name, seen)| {
                    let own = || g.pel.iter().filter(|(_, p)| p.consumer == *name);
                    let pel: Vec<Vec<u8>> = own()
                        .take(count)
                        .map(|(id, p)| {
                            resp_array(vec![
                                resp_bulk_str(&id.to_string()),
                                resp_integer(p.delivered_at as i64),
                                resp_integer(p.delivery_count as i64),
                            ])
                        })
                        .collect();
                    resp_map_for(
                        resp,
                        vec![
                            resp_bulk_str("name"),
                            resp_bulk_str(name),
                            resp_bulk_str("seen-time"),
                            resp_integer(seen as i64),
                            resp_bulk_str("active-time"),
                            resp_integer(g.active.get(name).map_or(-1, |at| *at as i64)),
                            resp_bulk_str("pel-count"),
                            resp_integer(own().count() as i64),
                            resp_bulk_str("pel"),
                            resp_array(pel),
                        ],
                    )
                })
                .collect();
            resp_map_for(
                resp,
                vec![
                    resp_bulk_str("name"),
                    resp_bulk_str(&g.name),
                    resp_bulk_str("last-delivered-id"),
                    resp_bulk_str(&g.last_id.to_string()),
                    resp_bulk_str("entries-read"),
                    entries_read,
                    resp_bulk_str("lag"),
                    lag,
                    resp_bulk_str("pel-count"),
                    resp_integer(g.pel.len() as i64),
                    resp_bulk_str("pel"),
                    resp_array(pel),
                    resp_bulk_str("consumers"),
                    resp_array(consumers),
                ],
            )
        })
        .collect();
    let (idmp_duration, idmp_maxsize) = stream_extra::idmp_settings(inner, idmp_defaults);
    let (pids_tracked, iids_tracked) = stream_extra::idmp_tracked(inner);
    let nodes = stream_extra::node_count(inner);
    resp_map_for(
        resp,
        vec![
            resp_bulk_str("length"),
            resp_integer(inner.entries.len() as i64),
            resp_bulk_str("radix-tree-keys"),
            resp_integer(nodes as i64),
            resp_bulk_str("radix-tree-nodes"),
            resp_integer(if nodes == 0 { 1 } else { nodes as i64 + 1 }),
            resp_bulk_str("last-generated-id"),
            resp_bulk_str(&inner.last_id.to_string()),
            resp_bulk_str("max-deleted-entry-id"),
            resp_bulk_str(&inner.max_deleted_id.to_string()),
            resp_bulk_str("entries-added"),
            resp_integer(inner.entries_added as i64),
            resp_bulk_str("recorded-first-entry-id"),
            resp_bulk_str(&first_entry_id(inner).to_string()),
            // Idempotent production (Redis 8.6).
            resp_bulk_str("idmp-duration"),
            resp_integer(idmp_duration as i64),
            resp_bulk_str("idmp-maxsize"),
            resp_integer(idmp_maxsize as i64),
            resp_bulk_str("pids-tracked"),
            resp_integer(pids_tracked as i64),
            resp_bulk_str("iids-tracked"),
            resp_integer(iids_tracked as i64),
            resp_bulk_str("iids-added"),
            resp_integer(inner.idmp.iids_added as i64),
            resp_bulk_str("iids-duplicates"),
            resp_integer(inner.idmp.iids_duplicates as i64),
            resp_bulk_str("entries"),
            resp_array(entries),
            resp_bulk_str("groups"),
            resp_array(groups),
        ],
    )
}

fn xinfo_groups(ctx: &mut Ctx) -> Vec<u8> {
    // XINFO GROUPS key — an array of per-group maps (`addReplyMapLen` per group).
    let resp = reply_resp(ctx);
    let cmd = &ctx.cmd;
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error("ERR no such key"),
        Some(KiviDBValue::Stream(s)) => {
            let inner = s.read();
            let groups: Vec<Vec<u8>> = groups_in_order(&inner)
                .into_iter()
                .map(|g| {
                    let (entries_read, lag) = read_and_lag(resp, &inner, g);
                    resp_map_for(
                        resp,
                        vec![
                            resp_bulk_str("name"),
                            resp_bulk_str(&g.name),
                            resp_bulk_str("consumers"),
                            resp_integer(g.consumers.len() as i64),
                            resp_bulk_str("pending"),
                            resp_integer(g.pel.len() as i64),
                            resp_bulk_str("last-delivered-id"),
                            resp_bulk_str(&g.last_id.to_string()),
                            resp_bulk_str("entries-read"),
                            entries_read,
                            resp_bulk_str("lag"),
                            lag,
                        ],
                    )
                })
                .collect();
            resp_array(groups)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

fn xinfo_consumers(ctx: &mut Ctx) -> Vec<u8> {
    // XINFO CONSUMERS key group — an array of per-consumer maps.
    let resp = reply_resp(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'xinfo|consumers' command");
    }
    let key = match parse_utf8(cmd[2]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[3]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR invalid group"),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_error("ERR no such key"),
        Some(KiviDBValue::Stream(s)) => {
            let inner = s.read();
            let g = match inner.groups.get(&group) {
                None => return resp_error("NOGROUP No such consumer group"),
                Some(g) => g,
            };
            let now = now_ms();
            let consumers: Vec<Vec<u8>> = consumers_in_order(g)
                .into_iter()
                .map(|(name, last_seen)| {
                    let pending = g.pel.values().filter(|p| p.consumer == *name).count() as i64;
                    let idle = now.saturating_sub(last_seen);
                    // Since it was last handed entries; -1 if it never was.
                    let inactive = g
                        .active
                        .get(name)
                        .map_or(-1, |at| now.saturating_sub(*at) as i64);
                    resp_map_for(
                        resp,
                        vec![
                            resp_bulk_str("name"),
                            resp_bulk_str(name),
                            resp_bulk_str("pending"),
                            resp_integer(pending),
                            resp_bulk_str("idle"),
                            resp_integer(idle as i64),
                            resp_bulk_str("inactive"),
                            resp_integer(inactive),
                        ],
                    )
                })
                .collect();
            resp_array(consumers)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}

// ── XGROUP dispatcher ─────────────────────────────────────────────────────────

pub fn xgroup(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'xgroup' command");
    }
    match std::str::from_utf8(cmd[1])
        .unwrap_or("")
        .to_ascii_uppercase()
        .as_str()
    {
        "CREATE" => xgroup_create(ctx),
        "DESTROY" => xgroup_destroy(ctx),
        "SETID" => xgroup_setid(ctx),
        "CREATECONSUMER" => xgroup_createconsumer(ctx),
        "DELCONSUMER" => xgroup_delconsumer(ctx),
        _ => resp_error("ERR unknown subcommand for XGROUP"),
    }
}

/// Called after every successful XADD to wake any blocked XREAD clients.
fn notify_xread_waiters(ctx: &Ctx, key: &str) {
    ctx.wait_table.notify_stream_waiters(key);
}

// ── XDELEX ───────────────────────────────────────────────────────────────────
//
// XDELEX key [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
//
// Delete entries by ID, saying what happens to consumer groups' references:
//   KEEPREF (default) — delete the entry; pending-list references stay.
//   DELREF            — delete the entry and every pending-list reference to
//                       it, including references to an entry already gone.
//   ACKED             — delete the entry only if every consumer group has read
//                       and acknowledged it.
//
// Per ID:
//    1  → deleted
//   -1  → no such entry in the stream
//    2  → not deleted: still referenced (ACKED), or there are no consumer
//         groups for it to have been acknowledged by

pub fn xdelex(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return resp_error("ERR wrong number of arguments for 'xdelex' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let (refs, ids) = match stream_extra::parse_policy_and_ids(&cmd[2..], false) {
        Ok(parsed) => parsed,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);
    let stream = match map.get(key.as_str()) {
        // No stream → every id is -1.
        None => return resp_array(ids.iter().map(|_| resp_integer(-1)).collect()),
        Some(KiviDBValue::Stream(s)) => Arc::clone(s),
        Some(_) => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    drop(map);
    let mut inner = stream.write();

    let replies = ids
        .iter()
        .map(|id| {
            if !inner.entries.contains_key(id) {
                // DELREF still clears references left behind by an entry
                // that was deleted some other way.
                if refs == TrimRefs::DelRef {
                    stream_extra::delete_entry(&mut inner, id, refs);
                }
                return resp_integer(-1);
            }
            let acked = refs != TrimRefs::Acked || !inner.groups.is_empty();
            if acked && stream_extra::delete_entry(&mut inner, id, refs) {
                resp_integer(1)
            } else {
                resp_integer(2)
            }
        })
        .collect();
    resp_array(replies)
}

// ── XACKDEL ───────────────────────────────────────────────────────────────────
//
// XACKDEL key group [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
//
// XACK and XDELEX in one step: acknowledge each ID in the group and delete
// the entry, under one lock, so no other client sees one without the other.
// The older KiviDB form `XACKDEL key group id [id ...]` is still accepted.
//
// Per ID:
//    1  → acknowledged, and the entry deleted
//   -1  → not pending in this group: nothing acknowledged, nothing deleted
//    2  → acknowledged, but the entry stays: another group has still to read
//         or acknowledge it (ACKED)

pub fn xackdel(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return resp_error("ERR wrong number of arguments for 'xackdel' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group_name = match parse_utf8(cmd[2]) {
        Some(g) => g.to_string(),
        None => return resp_error("ERR group name must be valid UTF-8"),
    };
    let (refs, ids) = match stream_extra::parse_policy_and_ids(&cmd[3..], true) {
        Ok(parsed) => parsed,
        Err(e) => return e,
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let stream = {
        let map = read_lock!(shard);
        match map.get(key.as_str()) {
            Some(KiviDBValue::Stream(s)) => Arc::clone(s),
            Some(_) => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
            // No such key: -1 for every ID.
            None => return resp_array(ids.iter().map(|_| resp_integer(-1)).collect()),
        }
    };

    // One write lock covers the acknowledgement and the deletion.
    let mut inner = stream.write();
    if !inner.groups.contains_key(&group_name) {
        return resp_error("NOGROUP No such consumer group");
    }

    let replies = ids
        .iter()
        .map(|id| {
            let was_pending = inner
                .groups
                .get_mut(&group_name)
                .is_some_and(|group| group.pel.remove(id).is_some());
            if !was_pending {
                return resp_integer(-1);
            }
            if stream_extra::delete_entry(&mut inner, id, refs) {
                resp_integer(1)
            } else {
                resp_integer(2)
            }
        })
        .collect();
    resp_array(replies)
}

// ── XCFGSET ───────────────────────────────────────────────────────────────────
//
// XCFGSET key field value [field value ...]
//
// Sets per-stream configuration.
//
//   IDMP-DURATION seconds — how long an idempotent ID is remembered
//                           (1-86400; Redis 8.6).
//   IDMP-MAXSIZE n        — how many idempotent IDs are remembered per
//                           producer (1-10000; Redis 8.6).
//
// Setting either clears every producer's remembered IDs for the stream, as
// in Redis.
//
// Two further fields are KiviDB extensions:
//
//   MAX-ENTRIES n      — persistent MAXLEN policy. Every future XADD trims
//                        the stream to at most n entries after inserting.
//                        Set to 0 to remove the policy (no limit).
//
//   MAX-DELETED-ENTRY-ID id — update the stream's tracked max deleted ID.
//                             Used by replication to track the highest ID
//                             that has ever been deleted. Clients can use
//                             XINFO STREAM to read this back.
//
// Returns OK on success. Errors on:
//   - wrong type
//   - missing/non-existent key (XCFGSET does not create streams)
//   - unknown field name
//   - bad value format

pub fn xcfgset(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;

    // XCFGSET key field value [field value ...]
    if cmd.len() < 4 || !(cmd.len() - 2).is_multiple_of(2) {
        return resp_error("ERR wrong number of arguments for 'xcfgset' command");
    }

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    // XCFGSET does not create streams — get-only, not get-or-create.
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    let stream_arc = match map.get(key.as_str()) {
        Some(KiviDBValue::Stream(s)) => Arc::clone(s),
        Some(_) => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        None => return resp_error("ERR no such key"),
    };
    drop(map); // release shard read lock before taking StreamInner write lock

    // Parse all field/value pairs up-front so a bad pair rejects the whole
    // command rather than partially applying it. Mirrors how CONFIG SET works.
    struct Patch {
        max_entries: Option<Option<u64>>, // None = not specified, Some(None) = clear, Some(Some(n)) = set
        max_deleted_id: Option<StreamId>,
        idmp_duration: Option<u64>,
        idmp_maxsize: Option<u64>,
    }
    let mut patch = Patch {
        max_entries: None,
        max_deleted_id: None,
        idmp_duration: None,
        idmp_maxsize: None,
    };

    let mut i = 2usize;
    while i + 1 < cmd.len() {
        let field = match parse_utf8(cmd[i]) {
            Some(f) => f.to_ascii_uppercase(),
            None => return resp_error("ERR field name must be valid UTF-8"),
        };
        let value = match parse_utf8(cmd[i + 1]) {
            Some(v) => v.to_string(),
            None => return resp_error("ERR field value must be valid UTF-8"),
        };

        match field.as_str() {
            "MAX-ENTRIES" => {
                let n: u64 = match value.parse() {
                    Ok(n) => n,
                    Err(_) => return resp_error("ERR value is not an integer or out of range"),
                };
                // 0 means "remove the policy" — stored as None
                patch.max_entries = Some(if n == 0 { None } else { Some(n) });
            }
            "IDMP-DURATION" => match value.parse::<u64>() {
                Ok(n) if stream_extra::IDMP_DURATION_RANGE.contains(&n) => {
                    patch.idmp_duration = Some(n);
                }
                _ => return resp_error("ERR invalid duration"),
            },
            "IDMP-MAXSIZE" => match value.parse::<u64>() {
                Ok(n) if stream_extra::IDMP_MAXSIZE_RANGE.contains(&n) => {
                    patch.idmp_maxsize = Some(n);
                }
                _ => return resp_error("ERR invalid maxsize"),
            },
            "MAX-DELETED-ENTRY-ID" => match StreamId::parse(&value) {
                Some(id) => patch.max_deleted_id = Some(id),
                None => {
                    return resp_error(
                        "ERR Invalid stream ID specified as stream command argument",
                    );
                }
            },
            other => {
                return resp_error(&format!("ERR unknown field '{other}' for XCFGSET"));
            }
        }
        i += 2;
    }

    // Apply the patch atomically under a single write lock.
    let mut inner = stream_arc.write();

    if let Some(policy) = patch.max_entries {
        inner.max_entries = policy;

        // Trim immediately if the current length already exceeds the new limit.
        if let Some(max) = inner.max_entries {
            while inner.entries.len() as u64 > max {
                if let Some((id, _)) = inner.entries.pop_first()
                    && id > inner.max_deleted_id
                {
                    inner.max_deleted_id = id;
                }
            }
        }
    }

    if let Some(id) = patch.max_deleted_id {
        // Only advance — never move max_deleted_id backwards.
        if id > inner.max_deleted_id {
            inner.max_deleted_id = id;
        }
    }

    if patch.idmp_duration.is_some() || patch.idmp_maxsize.is_some() {
        if let Some(duration) = patch.idmp_duration {
            inner.idmp.duration_secs = Some(duration);
        }
        if let Some(maxsize) = patch.idmp_maxsize {
            inner.idmp.maxsize = Some(maxsize);
        }
        // The remembered IDs were kept under the old settings; start over.
        inner.idmp.producers.clear();
    }

    resp_simple("OK")
}

// ── XNACK (Redis 8.8) ────────────────────────────────────────────────────────

/// `XNACK key group <SILENT | FAIL | FATAL> IDS numids id [id ...]
///        [RETRYCOUNT count] [FORCE]`
///
/// Releases pending entries back to the group PEL *without* acknowledging them,
/// so another consumer can pick them up immediately instead of waiting out a
/// min-idle-time. Each released entry is marked unowned (empty consumer) and
/// given a delivery time of 0, which is what makes it claimable regardless of
/// the `min-idle-time` argument to XCLAIM/XAUTOCLAIM.
///
/// Deviation worth knowing: Redis keeps NACKed entries in a dedicated zone at
/// the head of the PEL so they are *prioritised* over other pending entries.
/// KiviDB's PEL is a `BTreeMap` keyed by stream ID, so ordering stays by ID.
/// The functional effect — immediate claimability — is preserved via the
/// delivery-time-0 marker; the relative ordering against other already-idle
/// entries is not. Documented in `docs/REDIS_8_8_COMMANDS.md`.
pub fn xnack(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    // key group MODE IDS numids id  → 7 minimum
    if cmd.len() < 7 {
        return resp_error("ERR wrong number of arguments for 'xnack' command");
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let group = match parse_utf8(cmd[2]) {
        Some(g) => g.to_string(),
        None => return resp_error("ERR invalid group"),
    };

    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        Silent,
        Fail,
        Fatal,
    }
    let mode = if cmd[3].eq_ignore_ascii_case(b"SILENT") {
        Mode::Silent
    } else if cmd[3].eq_ignore_ascii_case(b"FAIL") {
        Mode::Fail
    } else if cmd[3].eq_ignore_ascii_case(b"FATAL") {
        Mode::Fatal
    } else {
        return resp_error("ERR syntax error, expected one of SILENT, FAIL or FATAL");
    };

    if !cmd[4].eq_ignore_ascii_case(b"IDS") {
        return resp_error("ERR syntax error, expected IDS");
    }
    let numids: usize = match parse_utf8(cmd[5]).and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return resp_error("ERR value is not an integer or out of range"),
    };
    if numids == 0 {
        return resp_error("ERR numids must be greater than 0");
    }
    if numids > cmd.len().saturating_sub(6) {
        return resp_error("ERR syntax error, numids does not match the number of IDs given");
    }

    let mut ids: Vec<StreamId> = Vec::with_capacity(numids);
    for raw in &cmd[6..6 + numids] {
        match StreamId::parse(std::str::from_utf8(raw).unwrap_or("")) {
            Some(id) => ids.push(id),
            None => {
                return resp_error("ERR Invalid stream ID specified as stream command argument");
            }
        }
    }

    // Trailing options after the ID block.
    let mut retrycount: Option<u64> = None;
    let mut force = false;
    let mut i = 6 + numids;
    while i < cmd.len() {
        if cmd[i].eq_ignore_ascii_case(b"RETRYCOUNT") {
            if i + 1 >= cmd.len() {
                return resp_error("ERR syntax error");
            }
            match parse_utf8(cmd[i + 1]).and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if n >= 0 => retrycount = Some(n as u64),
                _ => return resp_error("ERR value is not an integer or out of range"),
            }
            i += 2;
        } else if cmd[i].eq_ignore_ascii_case(b"FORCE") {
            force = true;
            i += 1;
        } else {
            return resp_error("ERR syntax error");
        }
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => resp_integer(0),
        Some(KiviDBValue::Stream(s)) => {
            let mut inner = s.write();
            // FORCE needs to know which IDs exist in the stream; take that set
            // before borrowing `groups` mutably.
            let existing_ids: Vec<bool> = if force {
                ids.iter()
                    .map(|id| inner.entries.contains_key(id))
                    .collect()
            } else {
                Vec::new()
            };

            let Some(g) = inner.groups.get_mut(&group) else {
                return resp_error(&format!(
                    "NOGROUP No such consumer group '{group}' for key name '{key}'"
                ));
            };

            let mut released = 0i64;
            for (pos, id) in ids.iter().enumerate() {
                match g.pel.get_mut(id) {
                    Some(entry) => {
                        entry.delivery_count = match retrycount {
                            // RETRYCOUNT overrides the mode-based adjustment.
                            Some(n) => n,
                            None => match mode {
                                // "Undo" the delivery increment.
                                Mode::Silent => entry.delivery_count.saturating_sub(1),
                                // Leave the counter alone — another consumer's turn.
                                Mode::Fail => entry.delivery_count,
                                // Permanently failed: LLONG_MAX.
                                Mode::Fatal => i64::MAX as u64,
                            },
                        };
                        // Unowned, and immediately claimable.
                        entry.consumer = String::new();
                        entry.delivered_at = 0;
                        released += 1;
                    }
                    None if force => {
                        // FORCE creates an unowned PEL entry, but only for IDs
                        // that actually exist in the stream.
                        if !existing_ids.get(pos).copied().unwrap_or(false) {
                            continue;
                        }
                        let delivery_count = match (retrycount, mode) {
                            (Some(n), _) => n,
                            (None, Mode::Fatal) => i64::MAX as u64,
                            (None, _) => 0,
                        };
                        g.pel.insert(
                            id.clone(),
                            PelEntry {
                                consumer: String::new(),
                                delivered_at: 0,
                                delivery_count,
                            },
                        );
                        released += 1;
                    }
                    // Not pending and no FORCE — ignored, not counted.
                    None => {}
                }
            }
            resp_integer(released)
        }
        Some(_) => resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }
}
