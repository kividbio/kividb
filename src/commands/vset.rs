// src/commands/vset.rs
//
// Vector Set commands: VADD, VREM, VSIM, VCARD, VDIM, VEMB,
//                      VISMEMBER, VINFO, VLINKS, VRANDMEMBER,
//                      VGETATTR, VSETATTR, VRANGE
//
// These are Redis 8's standalone vector data structure.
// Unlike FT.* (which indexes existing hashes), VectorSets store
// vectors directly at a key — like a sorted set but with N-dim vectors.

use crate::commands::ctx::Ctx;
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;
use crate::types::{KiviDBValue, VSetEntry, VectorSetInner};
use crate::vector::quant::{Quant, StoredVector};
use crate::vector::vset_graph;
use crate::{read_lock, write_lock};
use parking_lot::RwLock;
use std::sync::Arc;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "VADD" => vadd(ctx),
        "VREM" => vrem(ctx),
        "VSIM" => vsim(ctx),
        "VCARD" => vcard(ctx),
        "VDIM" => vdim(ctx),
        "VEMB" => vemb(ctx),
        "VISMEMBER" => vismember(ctx),
        "VINFO" => vinfo(ctx),
        "VLINKS" => vlinks(ctx),
        "VRANDMEMBER" => vrandmember(ctx),
        "VGETATTR" => vgetattr(ctx),
        "VSETATTR" => vsetattr(ctx),
        "VRANGE" => vrange(ctx),
        _ => None,
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// One `VADD`'s element and the options it gave.
struct Addition {
    element: crate::key_bytes::KeyBytes,
    vector: Vec<f32>,
    attrs: Option<Box<str>>,
    metric: Option<crate::vector::DistanceMetric>,
    quant: Option<Quant>,
    /// `M` and `EF`, for a set this makes.
    m: Option<usize>,
    ef: Option<usize>,
}

/// What `VADD` answers (1 when the element is new), and a link into the
/// set's graph to make once the locks are let go.
type Added = (bool, Option<vset_graph::Pending>);

fn quantize(quant: Quant, vector: Vec<f32>) -> Result<StoredVector, Vec<u8>> {
    StoredVector::quantize(quant, vector)
        .map_err(|_| resp_error("ERR invalid vector specification"))
}

/// Adds to `key`'s set, never blocking on the set while holding its shard.
fn add_to_vset(db: &store::Db, key: &[u8], lenient: bool, add: Addition) -> Result<Added, Vec<u8>> {
    let shard = store::get_shard(db, key);
    loop {
        let mut map = write_lock!(shard);
        let vs = match map.get(key) {
            Some(ValueRef::VectorSet(vs)) => Arc::clone(vs),
            Some(_) => {
                return Err(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            None => {
                let metric = add.metric.unwrap_or(crate::vector::DistanceMetric::Cosine);
                let mut inner = VectorSetInner::with_metric(add.vector.len(), metric);
                // Default stays f32; Redis defaults to Q8.
                inner.quant = add.quant.unwrap_or(Quant::F32);
                inner.quant_chosen = add.quant.is_some();
                inner.m = add.m.unwrap_or(vset_graph::DEFAULT_M);
                inner.ef_construction = add.ef.unwrap_or(vset_graph::DEFAULT_BUILD_EF);
                // Quantized first, so a rejected vector leaves no empty set.
                let stored = quantize(inner.quant, add.vector)?;
                inner.entries.insert(
                    add.element,
                    VSetEntry {
                        vector: stored,
                        attrs: add.attrs,
                    },
                );
                map.insert(key, KiviDBValue::VectorSet(Arc::new(RwLock::new(inner))));
                return Ok((true, None));
            }
        };
        if let Some(mut inner) = vs.try_write() {
            return insert_into(&vs, &mut inner, lenient, add);
        }
        // A reader (`VSIM`) holds the set: wait for it with the shard free.
        drop(map);
        // Right before the wait, so a shard still held here is seen.
        #[cfg(test)]
        lock_tests::about_to_wait(&vs);
        let mut inner = vs.write();
        // Not `read()`: a shard writer may be waiting for this set.
        let Some(map) = shard.try_read() else {
            continue;
        };
        // Held until the insert is done, so no delete comes between.
        if matches!(map.get(key), Some(ValueRef::VectorSet(now)) if Arc::ptr_eq(now, &vs)) {
            return insert_into(&vs, &mut inner, lenient, add);
        }
        // Deleted, replaced or expired meanwhile: look again.
    }
}

/// `add` into an existing set, checked against it as Redis checks it. `M`
/// and `EF` are the set's own from the `VADD` that made it: given again,
/// they are taken and not compared (Redis refuses an `M` other than the
/// set's; KiviDB does not save `M`, so a set loaded from a file has 16).
fn insert_into(
    set: &Arc<RwLock<VectorSetInner>>,
    inner: &mut VectorSetInner,
    lenient: bool,
    add: Addition,
) -> Result<Added, Vec<u8>> {
    let dim = add.vector.len();
    if inner.dim != dim {
        // Redis's words.
        return Err(resp_error(&format!(
            "ERR Vector dimension mismatch - got {} but set has {}",
            dim, inner.dim
        )));
    }
    // A `DISTANCE` the set was not made with is refused, as Redis
    // refuses a quantization the set was not made with. One not
    // given takes the set's.
    if add.metric.as_ref().is_some_and(|m| *m != inner.metric) {
        return Err(resp_error(
            "ERR asked distance mismatch with existing vector set",
        ));
    }
    // Unchosen sets, replays and replicated writes accept any option.
    let strict = inner.quant_chosen && !lenient;
    if strict && add.quant.is_some_and(|q| q != inner.quant) {
        return Err(resp_error(
            "ERR asked quantization mismatch with existing vector set",
        ));
    }
    let stored = quantize(inner.quant, add.vector)?;
    let entry = VSetEntry {
        vector: stored,
        attrs: add.attrs,
    };
    let name = add.element.clone();
    let new = inner.entries.insert(add.element, entry).is_none();
    let pending = vset_graph::written(set, inner, name.as_bytes());
    Ok((new, pending))
}

/// An integer written as Redis reads one: digits with an optional `-`, no
/// `+`, no leading zero (`010` and `+8` are not numbers to Redis).
fn strict_integer(arg: &[u8]) -> Option<i64> {
    let digits = arg.strip_prefix(b"-").unwrap_or(arg);
    let leading_zero = digits.len() > 1 && digits[0] == b'0';
    if digits.is_empty() || leading_zero || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(arg).ok()?.parse().ok()
}

/// Parse a VALUES num v1 v2 ... segment into Vec<f32>.
fn parse_values_vector(args: &[&[u8]], start: usize) -> Result<(Vec<f32>, usize), Vec<u8>> {
    // args[start] should be the count
    let count_str = std::str::from_utf8(args[start])
        .map_err(|_| resp_error("ERR VALUES count must be UTF-8"))?;
    let count: usize = count_str
        .parse()
        .map_err(|_| resp_error("ERR VALUES count must be an integer"))?;

    if count > args.len().saturating_sub(start + 1) {
        return Err(resp_error("ERR not enough values for VALUES vector"));
    }

    let mut vec = Vec::with_capacity(count);
    for i in 0..count {
        let s = std::str::from_utf8(args[start + 1 + i])
            .map_err(|_| resp_error("ERR vector value must be UTF-8"))?;
        let f: f32 = s
            .parse()
            .map_err(|_| resp_error("ERR vector value must be a float"))?;
        vec.push(f);
    }
    Ok((vec, start + 1 + count)) // returns next cursor position
}

// ── VADD ──────────────────────────────────────────────────────────────────────
//
// VADD key [REDUCE dim] (FP32 | VALUES num) vector element
//      [CAS] [NOQUANT | Q8 | BIN] [EF n] [SETATTR attrs] [M n]
//
// and KiviDB's own `DISTANCE COSINE | L2 | IP` (before the vector, or among
// the options), which Redis 8.10 does not have: its sets are cosine only.
//
// The vectors are kept in VectorSetInner.entries; a set large enough to
// have a graph (`vector::vset_graph`) gets each write there too.
//
// Returns: 1 if added, 0 if updated.

fn vadd(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    // Minimum: VADD key (FP32|VALUES) ... element
    if cmd.len() < 5 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vadd' command",
        ));
    }

    let key: Vec<u8> = cmd[1].to_vec();

    // Walk arguments to find vector encoding and element name
    let mut i = 2usize;
    let mut attrs: Option<Box<str>> = None;
    let mut asked_quant: Option<Quant> = None;

    // REDUCE projects the vectors to fewer dimensions, at random, with a
    // projection kept with the set for every query after. It was taken and
    // ignored, and the set kept the dimension given (#132): refused.
    if cmd
        .get(i)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.eq_ignore_ascii_case("REDUCE"))
        .unwrap_or(false)
    {
        return Some(resp_error(
            "ERR REDUCE is not supported by KiviDB: vectors are kept at the dimension they are \
             given",
        ));
    }

    // `DISTANCE` is KiviDB's own (Redis's vector sets are cosine only): it
    // may come before the vector or among the options after the element. A
    // value that names no metric is refused; it was taken as COSINE.
    let mut parsed_metric: Option<crate::vector::DistanceMetric> = None;
    let metric_of = |arg: Option<&&[u8]>| {
        arg.and_then(|b| std::str::from_utf8(b).ok())
            .and_then(crate::vector::DistanceMetric::metric_from_str)
            .ok_or_else(|| resp_error("ERR invalid DISTANCE: use COSINE, L2 or IP"))
    };

    if cmd
        .get(i)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.eq_ignore_ascii_case("DISTANCE"))
        .unwrap_or(false)
    {
        match metric_of(cmd.get(i + 1)) {
            Ok(metric) => parsed_metric = Some(metric),
            Err(e) => return Some(e),
        }
        i += 2;
    }

    // What Redis answers for anything wrong in how the vector is given.
    let bad_vector = || Some(resp_error("ERR invalid vector specification"));

    // Next token must be FP32 or VALUES
    let encoding = match cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
        Some(s) => s.to_ascii_uppercase(),
        None => return bad_vector(),
    };
    i += 1;

    let (vec, next_i) = match encoding.as_str() {
        "FP32" => {
            // Next arg is the raw binary blob
            let blob = match cmd.get(i) {
                Some(b) => b,
                None => return bad_vector(),
            };
            // We don't know dim yet — infer from blob length
            if blob.is_empty() || blob.len() % 4 != 0 {
                return bad_vector();
            }
            let v: Vec<f32> = blob
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                .collect();
            (v, i + 1)
        }
        "VALUES" => match parse_values_vector(cmd, i) {
            Ok((v, n)) if !v.is_empty() => (v, n),
            _ => return bad_vector(),
        },
        _ => return bad_vector(),
    };
    i = next_i;
    if vec.iter().any(|x| x.is_nan()) {
        return bad_vector();
    }

    // Next: element name
    // Any bytes, as in Redis: a name that was not UTF-8 was taken for a
    // missing one.
    let element = match cmd.get(i) {
        Some(b) => crate::key_bytes::KeyBytes::from(*b),
        None => {
            return Some(resp_error(
                "ERR wrong number of arguments for 'vadd' command",
            ));
        }
    };
    i += 1;

    // The options after the element, checked as Redis checks them: an
    // option it does not know, or one missing its value, is an error, and
    // so is an `M` or `EF` out of Redis's range. They were skipped.
    let invalid_option = || Some(resp_error("ERR invalid option after element"));
    let (mut asked_m, mut asked_ef) = (None, None);
    while i < cmd.len() {
        let tok = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        let value = cmd.get(i + 1);
        match tok.as_str() {
            // Last one wins, as in Redis.
            "NOQUANT" | "Q8" | "BIN" => {
                asked_quant = Quant::parse_option(cmd[i]);
                i += 1;
                continue;
            }
            // Threading hint in Redis; no effect here.
            "CAS" => {
                i += 1;
                continue;
            }
            _ if value.is_none() => return invalid_option(),
            "EF" | "M" => {
                let (lo, hi) = if tok == "EF" {
                    (1, 1_000_000)
                } else {
                    (4, 4096)
                };
                let Some(n) = value
                    .and_then(|v| strict_integer(v))
                    .filter(|n| (lo..=hi).contains(n))
                else {
                    return Some(resp_error(&format!("ERR invalid {tok}")));
                };
                if tok == "EF" {
                    asked_ef = Some(n as usize);
                } else {
                    asked_m = Some(n as usize);
                }
            }
            "SETATTR" => {
                attrs = value.map(|a| String::from_utf8_lossy(a).into());
            }
            "DISTANCE" => match metric_of(value) {
                Ok(metric) => parsed_metric = Some(metric),
                Err(e) => return Some(e),
            },
            _ => return invalid_option(),
        }
        i += 2;
    }

    let add = Addition {
        element,
        vector: vec,
        attrs,
        metric: parsed_metric,
        quant: asked_quant,
        m: asked_m,
        ef: asked_ef,
    };
    let db = &ctx.store[*ctx.selected_db];
    match add_to_vset(db, &key, ctx.apply_replication, add) {
        Ok((is_new, pending)) => {
            // With the set and its shard let go: a link is a graph search.
            if let Some(pending) = pending {
                pending.link();
            }
            Some(resp_integer(is_new as i64))
        }
        Err(e) => Some(e),
    }
}

// ── VREM ──────────────────────────────────────────────────────────────────────
//
// VREM key element [element ...]
// Returns: number of elements actually removed.

fn vrem(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vrem' command",
        ));
    }

    let key: &[u8] = cmd[1];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let vs = match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => Arc::clone(vs),
        Some(_) => {
            return Some(resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ));
        }
        None => return Some(resp_integer(0)),
    };
    // Drop shard lock before acquiring VectorSet write lock
    drop(map);

    let mut inner = vs.write();
    let mut removed = 0i64;
    // Any bytes: a name that was not UTF-8 was passed over.
    for elem_bytes in &cmd[2..] {
        if inner.entries.remove(*elem_bytes).is_some() {
            // Out of the graph too before the set is let go: no search
            // finds it after this.
            vset_graph::removed(&inner, elem_bytes);
            removed += 1;
        }
    }

    Some(resp_integer(removed))
}

// ── VCARD ─────────────────────────────────────────────────────────────────────
//
// VCARD key — number of elements in the vector set.

fn vcard(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vcard' command",
        ));
    }
    let key: &[u8] = cmd[1];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => Some(resp_integer(vs.read().entries.len() as i64)),
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_integer(0)),
    }
}

// ── VDIM ──────────────────────────────────────────────────────────────────────
//
// VDIM key — returns the dimension of vectors in the set.

fn vdim(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vdim' command",
        ));
    }
    let key: &[u8] = cmd[1];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => Some(resp_integer(vs.read().dim as i64)),
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_null_bulk()),
    }
}

// ── VEMB ──────────────────────────────────────────────────────────────────────
//
// VEMB key element [RAW]
// Returns the vector as an array of bulk strings (floats) or as a raw FP32 blob.

fn vemb(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vemb' command",
        ));
    }
    let key: &[u8] = cmd[1];
    let element: &[u8] = cmd[2];
    // As the vector-sets module: RAW, as often as given, and nothing else.
    if cmd[3..].iter().any(|opt| !opt.eq_ignore_ascii_case(b"RAW")) {
        return Some(resp_error("ERR invalid option"));
    }
    let raw = cmd.len() > 3;
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            let entry = match inner.entries.get(element) {
                None => return Some(resp_null_bulk()),
                Some(entry) => entry,
            };
            if raw {
                // Redis shape: type, stored bytes, norm, and range for int8.
                let mut out = vec![resp_simple(entry.vector.quant().name())];
                match &entry.vector {
                    StoredVector::F32(v) => {
                        let (norm, unit) = StoredVector::f32_unit(v);
                        let bytes: Vec<u8> = unit.iter().flat_map(|f| f.to_le_bytes()).collect();
                        out.push(resp_bulk(&bytes));
                        out.push(resp_double_for(resp, norm as f64));
                    }
                    StoredVector::Q8 {
                        norm,
                        range,
                        values,
                    } => {
                        let bytes: Vec<u8> = values.iter().map(|&q| q as u8).collect();
                        out.push(resp_bulk(&bytes));
                        out.push(resp_double_for(resp, *norm as f64));
                        out.push(resp_double_for(resp, *range as f64));
                    }
                    StoredVector::Bin { norm, bits } => {
                        let bytes: Vec<u8> = bits.iter().flat_map(|w| w.to_le_bytes()).collect();
                        out.push(resp_bulk(&bytes));
                        out.push(resp_double_for(resp, *norm as f64));
                    }
                }
                Some(resp_array(out))
            } else {
                // Redis replies with doubles, not strings.
                let mut values = Vec::with_capacity(inner.dim);
                entry.vector.to_f32(inner.dim, &mut values);
                let items: Vec<Vec<u8>> = values
                    .iter()
                    .map(|&f| resp_double_for(resp, f as f64))
                    .collect();
                Some(resp_array(items))
            }
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_null_bulk()),
    }
}

// ── VISMEMBER ─────────────────────────────────────────────────────────────────
//
// VISMEMBER key element
// Returns 1 if the element exists, 0 otherwise.

fn vismember(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vismember' command",
        ));
    }
    let key: &[u8] = cmd[1];
    let element: &[u8] = cmd[2];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let found = vs.read().entries.contains_key(element);
            Some(resp_integer(found as i64))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_integer(0)),
    }
}

// ── VGETATTR / VSETATTR ───────────────────────────────────────────────────────
//
// VGETATTR key element         → returns the JSON attrs string or nil
// VSETATTR key element "json"  → sets attrs; use empty string "" to remove

fn vgetattr(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vgetattr' command",
        ));
    }
    let key: &[u8] = cmd[1];
    let element: &[u8] = cmd[2];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            match inner.entries.get(element) {
                None => Some(resp_null_bulk()),
                Some(entry) => match &entry.attrs {
                    None => Some(resp_null_bulk()),
                    Some(s) => Some(resp_bulk_str(s)),
                },
            }
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_null_bulk()),
    }
}

fn vsetattr(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vsetattr' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();
    let element: Vec<u8> = cmd[2].to_vec();
    let attr_str = match std::str::from_utf8(cmd[3]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR attrs must be valid UTF-8")),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_slice()) {
        Some(ValueRef::VectorSet(vs)) => {
            let mut inner = vs.write();
            // 1 when set, 0 when there is no such element or key, as the
            // vector-sets module replies (#561).
            match inner.entries.get_mut(element.as_slice()) {
                None => Some(resp_integer(0)),
                Some(entry) => {
                    entry.attrs = if attr_str.is_empty() {
                        None
                    } else {
                        Some(attr_str.into_boxed_str())
                    };
                    Some(resp_integer(1))
                }
            }
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_integer(0)),
    }
}

// ── VINFO ─────────────────────────────────────────────────────────────────────
//
// VINFO key — returns metadata about the vector set as a flat key-value array.
// Mirrors what Redis 8 returns.

fn vinfo(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vinfo' command",
        ));
    }
    let key: &[u8] = cmd[1];

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            let count = inner.entries.len();
            let graph = inner.graph.get();
            let ready = graph.filter(|g| g.is_ready());
            let attributes = inner.entries.values().filter(|e| e.attrs.is_some()).count();
            // Redis's nine fields, in its order, then KiviDB's: the count
            // under its earlier name, and whether VSIM searches a graph
            // (HNSW) or scans (FLAT: a set under `EXACT_BELOW` elements, or
            // one whose graph is still being made).
            let pairs = [
                ("quant-type", resp_simple(inner.quant.name())),
                ("hnsw-m", resp_integer(inner.m as i64)),
                ("vector-dim", resp_integer(inner.dim as i64)),
                ("projection-input-dim", resp_integer(0)),
                ("size", resp_integer(count as i64)),
                (
                    "max-level",
                    resp_integer(graph.map_or(0, |g| g.max_level()) as i64),
                ),
                ("attributes-count", resp_integer(attributes as i64)),
                ("vset-uid", resp_integer(inner.uid as i64)),
                (
                    "hnsw-max-node-uid",
                    resp_integer(graph.map_or(0, |g| g.node_slots()) as i64),
                ),
                ("vector-count", resp_integer(count as i64)),
                (
                    "index-type",
                    resp_simple(if ready.is_some() { "HNSW" } else { "FLAT" }),
                ),
            ];
            let out = pairs
                .into_iter()
                .flat_map(|(k, v)| [resp_simple(k), v])
                .collect();
            let resp = crate::commands::handler::effective_resp_for_reply(ctx);
            Some(resp_map_for(resp, out))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_null_for(
            crate::commands::handler::effective_resp_for_reply(ctx),
        )),
    }
}

// ── VLINKS ────────────────────────────────────────────────────────────────────
//
/// The score `VSIM` and `VLINKS` give an element, from its distance to the
/// query. Cosine, the only metric Redis's vector sets have, is Redis's
/// `1 - distance / 2`: from 1 (the same direction) through 0.5 (at right
/// angles) to 0 (opposite), as `(1 + cosine) / 2`. It was the cosine
/// itself, from -1 to 1, which put a threshold such as `score > 0.8` in
/// another place than on Redis (#171). The other metrics are KiviDB's own
/// (`METRIC`): the dot product, and `1 / (1 + distance)` for L2.
fn similarity(metric: &crate::vector::DistanceMetric, distance: f32) -> f32 {
    match metric {
        crate::vector::DistanceMetric::Cosine => 1.0 - distance / 2.0,
        crate::vector::DistanceMetric::InnerProduct => 1.0 - distance,
        crate::vector::DistanceMetric::L2 => 1.0 / (1.0 + distance),
    }
}

/// Ties are common with BIN; names keep the order stable across processes.
fn by_distance_then_name(a: &(f32, &[u8]), b: &(f32, &[u8])) -> std::cmp::Ordering {
    a.0.total_cmp(&b.0).then_with(|| a.1.cmp(b.1))
}

// VLINKS key element [WITHSCORES]
// The element's neighbours in the set's graph, one array per layer, top
// layer first, as Redis replies (with WITHSCORES, each neighbour's score
// beside it: a map in RESP3). A set with no graph (under
// `vset_graph::EXACT_BELOW` elements), or an element not linked yet, has one
// layer: its nearest `M * 2` elements, what layer 0 of a graph would hold.

fn vlinks(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    // Redis: `key element [WITHSCORES]`, and nothing else.
    if cmd.len() < 3
        || cmd.len() > 4
        || (cmd.len() == 4 && !cmd[3].eq_ignore_ascii_case(b"WITHSCORES"))
    {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vlinks' command",
        ));
    }
    let key: &[u8] = cmd[1];
    let element: &[u8] = cmd[2];
    let with_scores = cmd.len() == 4;
    let resp = crate::commands::handler::effective_resp_for_reply(ctx);

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            let query_vec = match inner.entries.get(element) {
                None => return Some(resp_null_bulk()),
                Some(e) => &e.vector,
            };
            let score_of = |name: &[u8]| {
                inner.entries.get(name).map(|e| {
                    similarity(
                        &inner.metric,
                        query_vec.distance(&e.vector, &inner.metric, inner.dim),
                    )
                })
            };

            let linked = inner
                .graph
                .get()
                .filter(|g| g.is_ready())
                .and_then(|g| g.links_of(element));
            let layers: Vec<Vec<Vec<u8>>> = match linked {
                Some(layers) => layers,
                None => {
                    // What layer 0 would hold: the nearest `M * 2`.
                    let mut scored: Vec<(f32, &[u8])> = inner
                        .entries
                        .iter()
                        .filter(|(name, _)| *name != element)
                        .map(|(name, entry)| {
                            (
                                query_vec.distance(&entry.vector, &inner.metric, inner.dim),
                                name.as_bytes(),
                            )
                        })
                        .collect();
                    let keep = inner.m * 2;
                    if scored.len() > keep {
                        scored.select_nth_unstable_by(keep, by_distance_then_name);
                        scored.truncate(keep);
                    }
                    scored.sort_unstable_by(by_distance_then_name);
                    vec![scored.into_iter().map(|(_, n)| n.to_vec()).collect()]
                }
            };

            let out = layers
                .iter()
                .map(|layer| {
                    if !with_scores {
                        return resp_array(layer.iter().map(|n| resp_bulk(n)).collect());
                    }
                    let items = layer
                        .iter()
                        .filter_map(|n| score_of(n).map(|s| (n, s)))
                        .flat_map(|(n, s)| [resp_bulk(n), resp_double_for(resp, s as f64)])
                        .collect();
                    resp_map_for(resp, items)
                })
                .collect();
            Some(resp_array(out))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_null_bulk()),
    }
}

// ── VRANDMEMBER ───────────────────────────────────────────────────────────────
//
// VRANDMEMBER key [count]
// Returns one or more random elements. No repeats when count > 0.

fn vrandmember(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 2 && cmd.len() != 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vrandmember' command",
        ));
    }
    let key: &[u8] = cmd[1];

    let count: Option<i64> = match cmd.get(2) {
        None => None,
        Some(arg) => match std::str::from_utf8(arg).ok().and_then(|s| s.parse().ok()) {
            Some(i64::MIN) => return Some(resp_error("ERR COUNT value is out of range")),
            Some(n) => Some(n),
            None => return Some(resp_error("ERR COUNT value is not an integer")),
        },
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            let names: Vec<&[u8]> = inner.entries.keys().map(|s| s.as_bytes()).collect();

            if names.is_empty() {
                return if count.is_some() {
                    Some(resp_array(vec![]))
                } else {
                    Some(resp_null_bulk())
                };
            }

            match count {
                None => {
                    // Return a single random element
                    let idx = rand::random::<usize>() % names.len();
                    Some(resp_bulk(names[idx]))
                }
                Some(n) => {
                    let n_abs = n.unsigned_abs() as usize;
                    let mut result: Vec<Vec<u8>> = Vec::new();

                    if n > 0 {
                        // Positive: distinct elements, up to count
                        // Fisher-Yates partial shuffle using indices
                        let mut indices: Vec<usize> = (0..names.len()).collect();
                        let take = n_abs.min(names.len());
                        for i in 0..take {
                            let j = i + rand::random::<usize>() % (names.len() - i);
                            indices.swap(i, j);
                            result.push(resp_bulk(names[indices[i]]));
                        }
                    } else {
                        // Negative: may repeat, return exactly |count| elements
                        for _ in 0..n_abs {
                            let idx = rand::random::<usize>() % names.len();
                            result.push(resp_bulk(names[idx]));
                        }
                    }
                    Some(resp_array(result))
                }
            }
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            if count.is_some() {
                Some(resp_array(vec![]))
            } else {
                Some(resp_null_bulk())
            }
        }
    }
}

// ── VRANGE ────────────────────────────────────────────────────────────────────
//
// VRANGE key start end [count]
// Returns elements in lexicographical range of element names.
// "start" and "end" follow the same bracket convention as ZRANGEBYLEX:
//   "[foo"  = inclusive from "foo"
//   "(foo"  = exclusive from "foo"
//   "-"     = negative infinity (beginning)
//   "+"     = positive infinity (end)
//
// Why a standalone fn instead of a closure?
// Closures that take &str and return Option<&str> referencing the *same*
// input can't express that relationship — Rust infers two separate lifetimes
// and refuses to unify them. A named fn with `'a` ties input to output
// explicitly, making the borrow checker happy.

fn parse_lex_bound(s: &[u8]) -> (Option<&[u8]>, bool) {
    match s {
        b"-" | b"+" => (None, true),
        [b'[', rest @ ..] => (Some(rest), true),
        [b'(', rest @ ..] => (Some(rest), false),
        s => (Some(s), true), // bare string: treat as inclusive
    }
}

fn vrange(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vrange' command",
        ));
    }
    let key: &[u8] = cmd[1];
    // Element names are any bytes, and so are the bounds.
    let start_raw: &[u8] = cmd[2];
    let end_raw: &[u8] = cmd[3];
    let count: Option<usize> = cmd
        .get(4)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok());

    let (start_str, start_inc) = parse_lex_bound(start_raw);
    let (end_str, end_inc) = parse_lex_bound(end_raw);

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            let mut names: Vec<&[u8]> = inner.entries.keys().map(|s| s.as_bytes()).collect();
            names.sort_unstable();

            let matches_start = |name: &[u8]| -> bool {
                match start_str {
                    None => true,
                    Some(s) => {
                        if start_inc {
                            name >= s
                        } else {
                            name > s
                        }
                    }
                }
            };
            let matches_end = |name: &[u8]| -> bool {
                match end_str {
                    None => true,
                    Some(s) => {
                        if end_inc {
                            name <= s
                        } else {
                            name < s
                        }
                    }
                }
            };

            let iter = names
                .iter()
                .filter(|&&name| matches_start(name) && matches_end(name));

            let result: Vec<Vec<u8>> = if let Some(n) = count {
                iter.take(n).map(|name| resp_bulk(name)).collect()
            } else {
                iter.map(|name| resp_bulk(name)).collect()
            };

            Some(resp_array(result))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_array(vec![])),
    }
}

// ── VSIM ──────────────────────────────────────────────────────────────────────
//
// VSIM key (ELE | FP32 | VALUES num) (vector | element)
//      [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF n]
//      [FILTER expr] [FILTER-EF n] [TRUTH] [NOTHREAD]
//
// The options are checked as Redis checks them. A set of
// `vset_graph::EXACT_BELOW` elements or more is searched in its HNSW graph,
// which explores `EF` candidates (100 unless given, and at least `COUNT`);
// they are then scored from the set's own vectors, as a scan scores every
// element, so a score is the same whichever way an element was found. `TRUTH`,
// a smaller set, a graph still being made, or an `EF` near the set's size,
// scan every element instead: exact. `EPSILON` keeps the results whose
// distance (one less the score) is at most it. With `FILTER`, the graph walk
// lets in only the elements the expression passes; after `FILTER-EF`
// (`COUNT * 100` unless given) nodes' worth of distances it stops, and the set
// is scanned, so a filter that passes few elements is answered exactly.

/// Redis's checks of an integer option: digits, more than 0, at most `max`.
fn positive_option(value: Option<&&[u8]>, max: i64) -> Option<usize> {
    value
        .and_then(|v| strict_integer(v))
        .filter(|n| (1..=max).contains(n))
        .map(|n| usize::try_from(n).unwrap_or(usize::MAX))
}

/// What `VSIM` asks of a set, its options checked.
#[derive(Clone, Copy)]
struct Search<'f> {
    count: usize,
    epsilon: f64,
    /// At least `count`.
    ef: usize,
    filter: Option<&'f super::vset_filter::Filter>,
    filter_ef: usize,
    truth: bool,
}

/// The nearest elements of `inner` (the set `vs` holds, held for reading)
/// to `query` (quantized as the set is), nearest first, with their
/// distances: what `VSIM` replies.
fn nearest<'s>(
    vs: &Arc<RwLock<VectorSetInner>>,
    inner: &'s VectorSetInner,
    query: &StoredVector,
    asked: &Search<'_>,
) -> Vec<(f32, &'s [u8])> {
    let Search {
        count,
        epsilon,
        ef,
        filter,
        filter_ef,
        truth,
    } = *asked;
    let passes = |entry: &VSetEntry| {
        filter
            .as_ref()
            .is_none_or(|filter| filter.matches(entry.attrs.as_deref()))
    };
    // Every element the filter passes, measured: exact.
    let scan = || -> Vec<(f32, &[u8])> {
        inner
            .entries
            .iter()
            .filter(|(_, entry)| passes(entry))
            .map(|(name, entry)| {
                (
                    query.distance(&entry.vector, &inner.metric, inner.dim),
                    name.as_bytes(),
                )
            })
            .collect()
    };

    let mut scored: Vec<(f32, &[u8])> = match vset_graph::for_search(vs, inner, truth, ef) {
        None => scan(),
        Some(graph) => {
            // The graph holds f32 vectors: the query as the set holds it.
            let mut as_f32 = Vec::with_capacity(inner.dim);
            query.to_f32(inner.dim, &mut as_f32);
            let hits = match filter {
                None => Some(vset_graph::search(&graph, &as_f32, ef, &inner.metric)),
                Some(_) => {
                    // Redis's FILTER-EF counts nodes expanded; each costs up
                    // to a layer-0 list of distances.
                    let budget = filter_ef.saturating_mul(inner.m * 2).max(ef);
                    let mut admit = |name: &[u8]| inner.entries.get(name).is_some_and(passes);
                    vset_graph::search_admitting(
                        &graph,
                        &as_f32,
                        ef,
                        &inner.metric,
                        &mut admit,
                        budget,
                    )
                }
            };
            match hits {
                Some(hits) => hits
                    .iter()
                    .filter_map(|hit| {
                        let name = vset_graph::name_of(hit);
                        let (name, entry) = inner.entries.get_key_value(&*name)?;
                        passes(entry).then(|| {
                            (
                                query.distance(&entry.vector, &inner.metric, inner.dim),
                                name.as_bytes(),
                            )
                        })
                    })
                    .collect(),
                None => scan(),
            }
        }
    };

    if scored.len() > count {
        scored.select_nth_unstable_by(count, by_distance_then_name);
        scored.truncate(count);
    }
    scored.sort_unstable_by(by_distance_then_name);
    // EPSILON: the distance as Redis has it, one less the score.
    let within = |d: f32| 1.0 - similarity(&inner.metric, d) as f64 <= epsilon;
    if let Some(far) = scored.iter().position(|(d, _)| !within(*d)) {
        scored.truncate(far);
    }

    scored
}

fn vsim(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vsim' command",
        ));
    }

    let key: Vec<u8> = cmd[1].to_vec();

    let encoding = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return Some(resp_error("ERR encoding must be UTF-8")),
    };

    // Parse the query vector
    let (query_vec, mut i) = match encoding.as_str() {
        // The element is looked up once the set is held.
        "ELE" => (Err(cmd[3].to_vec()), 4usize),
        "FP32" => {
            let blob = match cmd.get(3) {
                Some(b) => b,
                None => return Some(resp_error("ERR missing FP32 blob")),
            };
            if blob.len() % 4 != 0 {
                return Some(resp_error("ERR FP32 blob must be a multiple of 4 bytes"));
            }
            let v: Vec<f32> = blob
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                .collect();
            (Ok(v), 4usize)
        }
        "VALUES" => match parse_values_vector(cmd, 3) {
            Ok((v, n)) => (Ok(v), n),
            Err(e) => return Some(e),
        },
        _ => return Some(resp_error("ERR expected ELE, FP32, or VALUES")),
    };

    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count = 10usize;
    let mut epsilon = 2.0f64;
    let mut ef: Option<usize> = None;
    let mut filter_ef: Option<usize> = None;
    let mut truth = false;
    let mut filter: Option<super::vset_filter::Filter> = None;
    let syntax_error = || Some(resp_error("ERR syntax error in VSIM command"));

    while i < cmd.len() {
        let tok = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        let value = cmd.get(i + 1);
        match tok.as_str() {
            "WITHSCORES" => with_scores = true,
            "WITHATTRIBS" => with_attribs = true,
            "TRUTH" => truth = true,
            // Every VSIM runs on the connection's own thread.
            "NOTHREAD" => {}
            _ if value.is_none() => return syntax_error(),
            "COUNT" => match positive_option(value, i64::MAX) {
                Some(n) => count = n,
                None => return Some(resp_error("ERR invalid COUNT")),
            },
            "EPSILON" => {
                match value
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|e| *e > 0.0)
                {
                    Some(e) => epsilon = e,
                    None => return Some(resp_error("ERR invalid EPSILON")),
                }
            }
            "EF" => match positive_option(value, 1_000_000) {
                Some(n) => ef = Some(n),
                None => return Some(resp_error("ERR invalid EF")),
            },
            "FILTER-EF" => match positive_option(value, i64::MAX) {
                Some(n) => filter_ef = Some(n),
                None => return Some(resp_error("ERR invalid FILTER-EF")),
            },
            // Only the elements whose attributes pass are scored (#132).
            "FILTER" => {
                let Some(expr) = value.and_then(|b| std::str::from_utf8(b).ok()) else {
                    return Some(resp_error(super::vset_filter::SYNTAX_ERROR));
                };
                match super::vset_filter::Filter::compile(expr) {
                    Some(compiled) => filter = Some(compiled),
                    None => return Some(resp_error(super::vset_filter::SYNTAX_ERROR)),
                }
            }
            _ => return syntax_error(),
        }
        i += if matches!(
            tok.as_str(),
            "WITHSCORES" | "WITHATTRIBS" | "TRUTH" | "NOTHREAD"
        ) {
            1
        } else {
            2
        };
    }
    // As Redis: at least COUNT candidates, and a filtered walk's effort.
    let ef = ef.unwrap_or(vset_graph::DEFAULT_SEARCH_EF).max(count);
    let filter_ef = filter_ef.unwrap_or_else(|| count.saturating_mul(100));

    // Now acquire the VectorSet and do the search
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    let vs = match map.get(key.as_slice()) {
        Some(ValueRef::VectorSet(vs)) => Arc::clone(vs),
        Some(_) => {
            return Some(resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ));
        }
        None => return Some(resp_array(vec![])),
    };
    drop(map);

    let inner = vs.read();

    // Quantize the query like the set, as Redis does, so scores match.
    let given;
    let query: &StoredVector = match query_vec {
        Ok(v) => {
            if v.len() != inner.dim {
                return Some(resp_error(&format!(
                    "ERR Vector dimension mismatch - got {} but set has {}",
                    v.len(),
                    inner.dim
                )));
            }
            given = match StoredVector::quantize(inner.quant, v) {
                Ok(q) => q,
                Err(_) => return Some(resp_error("ERR invalid vector specification")),
            };
            &given
        }
        Err(elem_name) => match inner.entries.get(elem_name.as_slice()) {
            Some(e) => &e.vector,
            // Redis's words.
            None => return Some(resp_error("ERR element not found in set")),
        },
    };

    let asked = Search {
        count,
        epsilon,
        ef,
        filter: filter.as_ref(),
        filter_ef,
        truth,
    };
    let scored = nearest(&vs, &inner, query, &asked);

    // Build response
    // Redis VSIM returns: [element, [score?, attribs?], ...]
    // Flat array with optional interleaved scores/attribs
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (score, name) in &scored {
        out.push(resp_bulk(name));
        if with_scores {
            out.push(resp_bulk_str(&format!(
                "{:.6}",
                similarity(&inner.metric, *score)
            )));
        }
        if with_attribs {
            let attrs = inner
                .entries
                .get(*name)
                .and_then(|e| e.attrs.as_deref())
                .unwrap_or("{}");
            out.push(resp_bulk_str(attrs));
        }
    }

    Some(resp_array(out))
}

#[cfg(test)]
mod lock_tests {
    use super::*;
    use std::time::{Duration, Instant};

    type Set = Arc<RwLock<VectorSetInner>>;

    fn add(element: &str, vector: &[f32], quant: Option<Quant>) -> Addition {
        Addition {
            element: crate::key_bytes::KeyBytes::from(element.as_bytes()),
            vector: vector.to_vec(),
            attrs: None,
            metric: None,
            quant,
            m: None,
            ef: None,
        }
    }

    /// `add_to_vset`, its link (if any) made as `VADD` makes it.
    fn vadd_now(db: &store::Db, key: &[u8], lenient: bool, add: Addition) -> Result<bool, Vec<u8>> {
        add_to_vset(db, key, lenient, add).map(|(new, pending)| {
            if let Some(p) = pending {
                p.link();
            }
            new
        })
    }

    fn set_at(db: &store::Db, key: &str) -> Option<Set> {
        match store::get_shard(db, key).read().get(key) {
            Some(ValueRef::VectorSet(vs)) => Some(Arc::clone(vs)),
            _ => None,
        }
    }

    fn names(vs: &Set) -> Vec<String> {
        let mut names: Vec<String> = vs
            .read()
            .entries
            .keys()
            .map(|k| String::from_utf8_lossy(k.as_bytes()).into_owned())
            .collect();
        names.sort();
        names
    }

    fn stored(vs: &Set, element: &str) -> &'static str {
        match vs.read().entries.get(element.as_bytes()).map(|e| &e.vector) {
            Some(StoredVector::F32(_)) => "f32",
            Some(StoredVector::Q8 { .. }) => "int8",
            Some(StoredVector::Bin { .. }) => "bin",
            None => "none",
        }
    }

    /// A set at `key` holding `a`, made with `quant`.
    fn made(db: &store::Db, key: &str, quant: Option<Quant>) -> Set {
        assert_eq!(
            vadd_now(db, key.as_bytes(), false, add("a", &[1.0, 2.0, 3.0], quant)),
            Ok(true)
        );
        set_at(db, key).unwrap()
    }

    /// Bounded: until a writer queues on `vs`, which a held read keeps out.
    fn until_a_writer_waits(vs: &Set) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !vs.is_locked_exclusive() {
            assert!(
                Instant::now() < deadline,
                "no writer ever waited for the set"
            );
            std::thread::yield_now();
        }
    }

    /// How many `VADD`s came to wait for each set, by its address. Tests
    /// run at once, each with its own set.
    static WAITING: parking_lot::Mutex<Vec<(usize, usize)>> = parking_lot::const_mutex(Vec::new());

    fn address(vs: &Set) -> usize {
        Arc::as_ptr(vs) as usize
    }

    /// Called by `add_to_vset` just before it blocks on `vs`.
    pub(super) fn about_to_wait(vs: &Set) {
        let at = address(vs);
        let mut waiting = WAITING.lock();
        match waiting.iter_mut().find(|(a, _)| *a == at) {
            Some((_, n)) => *n += 1,
            None => waiting.push((at, 1)),
        }
    }

    fn waiting_for(vs: &Set) -> usize {
        let at = address(vs);
        let waiting = WAITING.lock();
        waiting
            .iter()
            .find(|(a, _)| *a == at)
            .map_or(0, |(_, n)| *n)
    }

    /// Forgets what was counted for an earlier set at `vs`'s address.
    fn count_from_zero(vs: &Set) {
        let at = address(vs);
        WAITING.lock().retain(|(a, _)| *a != at);
    }

    /// Whether `n` `VADD`s let go of the shard to wait for `vs` within 10 s.
    /// Returns rather than panics, so the caller can let go of the set first.
    fn came_to_wait(vs: &Set, n: usize) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while waiting_for(vs) < n {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::yield_now();
        }
        true
    }

    /// Another key that lives in `key`'s shard.
    fn neighbour(db: &store::Db, key: &str) -> String {
        let shard = store::get_shard(db, key);
        (0..100_000)
            .map(|i| format!("other{i}"))
            .find(|k| std::ptr::eq(store::get_shard(db, k), shard))
            .unwrap()
    }

    fn string(s: &str) -> KiviDBValue {
        KiviDBValue::String(crate::types::SharedBytes::from(s))
    }

    /// Runs `meanwhile` with the shard held, while `VADD` waits behind a `VSIM`.
    fn while_vadd_waits(
        db: &store::Db,
        key: &str,
        vs: &Set,
        addition: Addition,
        meanwhile: impl FnOnce(&mut store::ShardWrite<'_>),
    ) -> Result<bool, Vec<u8>> {
        let reading = vs.read();
        std::thread::scope(|s| {
            let vadd = s.spawn(|| vadd_now(db, key.as_bytes(), false, addition));
            until_a_writer_waits(vs);
            // Not `write()`: holding the shard there would hang this test.
            let shard = store::get_shard(db, key).try_write();
            let free = shard.is_some();
            // Its panic is raised after the set is let go of, not before.
            let done = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Some(mut map) = shard {
                    meanwhile(&mut map);
                }
            }));
            drop(reading);
            let result = vadd.join().unwrap();
            assert!(free, "VADD held the shard while it waited for the set");
            if let Err(panic) = done {
                std::panic::resume_unwind(panic);
            }
            result
        })
    }

    #[test]
    fn a_waiting_vadd_leaves_the_shard_to_other_keys() {
        let st = store::new_store(1);
        let db = &st[0];
        let vs = made(db, "k", None);
        let other = neighbour(db, "k");
        let r = while_vadd_waits(db, "k", &vs, add("b", &[4.0, 5.0, 6.0], None), |map| {
            map.insert(&other, string("v"));
        });
        assert_eq!(r, Ok(true));
        assert!(Arc::ptr_eq(&set_at(db, "k").unwrap(), &vs));
        assert_eq!(names(&vs), ["a", "b"]);
        assert!(store::get_shard(db, &other).read().get(&other).is_some());
    }

    #[test]
    fn a_set_deleted_meanwhile_is_made_again() {
        let st = store::new_store(1);
        let db = &st[0];
        let old = made(db, "k", Some(Quant::Q8));
        let r = while_vadd_waits(db, "k", &old, add("b", &[4.0, 5.0, 6.0], None), |map| {
            map.remove("k");
        });
        assert_eq!(r, Ok(true));
        let now = set_at(db, "k").unwrap();
        assert!(!Arc::ptr_eq(&now, &old));
        assert_eq!(names(&now), ["b"]);
        assert_eq!(stored(&now, "b"), "f32");
        // Nothing went into the set no key holds.
        assert_eq!(names(&old), ["a"]);
    }

    #[test]
    fn a_set_replaced_meanwhile_gets_the_element() {
        let st = store::new_store(1);
        let db = &st[0];
        let old = made(db, "k", None);
        let mut inner = VectorSetInner::with_metric(3, crate::vector::DistanceMetric::Cosine);
        inner.quant = Quant::Bin;
        inner.quant_chosen = true;
        let replacement: Set = Arc::new(RwLock::new(inner));
        let r = while_vadd_waits(db, "k", &old, add("b", &[4.0, -5.0, 6.0], None), |map| {
            map.insert("k", KiviDBValue::VectorSet(Arc::clone(&replacement)));
        });
        assert_eq!(r, Ok(true));
        assert!(Arc::ptr_eq(&set_at(db, "k").unwrap(), &replacement));
        assert_eq!(names(&replacement), ["b"]);
        assert_eq!(stored(&replacement, "b"), "bin");
        assert_eq!(names(&old), ["a"]);
    }

    #[test]
    fn a_set_replaced_by_a_string_meanwhile_is_wrongtype() {
        let st = store::new_store(1);
        let db = &st[0];
        let old = made(db, "k", None);
        let r = while_vadd_waits(db, "k", &old, add("b", &[4.0, 5.0, 6.0], None), |map| {
            map.insert("k", string("s"));
        });
        assert!(r.unwrap_err().starts_with(b"-WRONGTYPE"));
        assert!(set_at(db, "k").is_none());
        assert_eq!(names(&old), ["a"]);
    }

    #[test]
    fn a_set_expired_meanwhile_is_made_again() {
        let st = store::new_store(1);
        let db = &st[0];
        let old = made(db, "k", None);
        let r = while_vadd_waits(db, "k", &old, add("b", &[4.0, 5.0, 6.0], None), |map| {
            assert!(map.set_expires("k", 1));
            assert!(map.get("k").is_none());
        });
        assert_eq!(r, Ok(true));
        let now = set_at(db, "k").unwrap();
        assert!(!Arc::ptr_eq(&now, &old));
        assert_eq!(names(&now), ["b"]);
        assert_eq!(names(&old), ["a"]);
    }

    #[test]
    fn a_vrem_meanwhile_and_the_vadd_both_apply() {
        let st = store::new_store(1);
        let db = &st[0];
        let vs = made(db, "k", None);
        let reading = vs.read();
        std::thread::scope(|s| {
            let vadd = s.spawn(|| vadd_now(db, b"k", false, add("b", &[4.0, 5.0, 6.0], None)));
            until_a_writer_waits(&vs);
            // As `VREM` removes: the shard let go of, then the set's lock.
            let vrem = s.spawn(|| vs.write().entries.remove(b"a".as_slice()).is_some());
            drop(reading);
            assert_eq!(vadd.join().unwrap(), Ok(true));
            assert!(vrem.join().unwrap());
        });
        assert_eq!(names(&vs), ["b"]);
        assert!(Arc::ptr_eq(&set_at(db, "k").unwrap(), &vs));
    }

    #[test]
    fn concurrent_vadds_all_land_in_the_one_set() {
        let st = store::new_store(1);
        let db = &st[0];
        let vs = made(db, "k", Some(Quant::Q8));
        count_from_zero(&vs);
        let reading = vs.read();
        std::thread::scope(|s| {
            let vadds: Vec<_> = (0..8)
                .map(|n| {
                    s.spawn(move || {
                        let name = format!("e{n}");
                        vadd_now(db, b"k", false, add(&name, &[n as f32, 1.0, 2.0], None))
                    })
                })
                .collect();
            // Every one, not only the first: one still looking the set up
            // holds the shard for a moment, which is not waiting with it.
            // A VADD that waits with the shard held keeps the rest from
            // coming, and the shard from being free.
            let all_came = came_to_wait(&vs, 8);
            let free = store::get_shard(db, "k").try_write().is_some();
            drop(reading);
            for vadd in vadds {
                assert_eq!(vadd.join().unwrap(), Ok(true));
            }
            assert!(free, "a VADD held the shard while it waited for the set");
            assert!(
                all_came,
                "only {} of 8 VADDs came to wait",
                waiting_for(&vs)
            );
        });
        let mut want: Vec<String> = (0..8).map(|n| format!("e{n}")).collect();
        want.insert(0, "a".into());
        assert_eq!(names(&vs), want);
        assert!((0..8).all(|n| stored(&vs, &format!("e{n}")) == "int8"));
        assert!(Arc::ptr_eq(&set_at(db, "k").unwrap(), &vs));
    }

    #[test]
    fn a_rejected_vadd_changes_nothing() {
        let st = store::new_store(1);
        let db = &st[0];
        // A new set whose first vector is refused is not made.
        let inf = add("a", &[f32::INFINITY, 1.0, 2.0], Some(Quant::Q8));
        assert!(vadd_now(db, b"new", false, inf).is_err());
        assert!(store::get_shard(db, "new").read().get("new").is_none());
        // Refused after waiting: the dimension, then the quantization.
        let vs = made(db, "k", Some(Quant::Q8));
        let r = while_vadd_waits(db, "k", &vs, add("b", &[1.0, 2.0], None), |_| {});
        assert!(String::from_utf8_lossy(&r.unwrap_err()).contains("dimension mismatch"));
        let bin = add("b", &[1.0, 2.0, 3.0], Some(Quant::Bin));
        let r = while_vadd_waits(db, "k", &vs, bin, |_| {});
        assert!(String::from_utf8_lossy(&r.unwrap_err()).contains("quantization mismatch"));
        assert_eq!(names(&vs), ["a"]);
    }

    #[test]
    fn each_quantization_is_kept_after_waiting() {
        let st = store::new_store(1);
        let db = &st[0];
        for (key, quant, want) in [
            ("d", None, "f32"),
            ("q", Some(Quant::Q8), "int8"),
            ("b", Some(Quant::Bin), "bin"),
            ("n", Some(Quant::F32), "f32"),
        ] {
            let vs = made(db, key, quant);
            let r = while_vadd_waits(db, key, &vs, add("b", &[4.0, -5.0, 6.0], quant), |_| {});
            assert_eq!(r, Ok(true), "{key}");
            assert_eq!(stored(&vs, "a"), want, "{key}");
            assert_eq!(stored(&vs, "b"), want, "{key}");
            assert_eq!(vs.read().quant_chosen, quant.is_some(), "{key}");
        }
        // A replayed write takes any option, as before.
        let vs = set_at(db, "q").unwrap();
        let bin = add("c", &[1.0, 2.0, 3.0], Some(Quant::Bin));
        assert_eq!(vadd_now(db, b"q", true, bin), Ok(true));
        assert_eq!(stored(&vs, "c"), "int8");
    }
}

/// The graph against an exact model of the set: random writes, overwrites
/// and removals, searched by graph and by `TRUTH`.
#[cfg(test)]
mod graph_tests {
    use super::*;
    use crate::vector::DistanceMetric;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;

    type Set = Arc<RwLock<VectorSetInner>>;

    fn random_vector(rng: &mut StdRng, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| rng.r#gen::<f32>() * 2.0 - 1.0).collect()
    }

    fn addition(name: &str, v: &[f32], quant: Quant, metric: &DistanceMetric) -> Addition {
        Addition {
            element: crate::key_bytes::KeyBytes::from(name.as_bytes()),
            vector: v.to_vec(),
            attrs: Some(format!("{{\"n\":{}}}", name.len()).into()),
            metric: Some(metric.clone()),
            quant: Some(quant),
            m: None,
            ef: None,
        }
    }

    fn vadd(db: &store::Db, key: &str, add: Addition) -> bool {
        let (new, pending) = add_to_vset(db, key.as_bytes(), false, add).unwrap();
        if let Some(p) = pending {
            p.link();
        }
        new
    }

    fn set_at(db: &store::Db, key: &str) -> Option<Set> {
        match store::get_shard(db, key).read().get(key) {
            Some(ValueRef::VectorSet(vs)) => Some(Arc::clone(vs)),
            _ => None,
        }
    }

    /// As `VREM` removes one element.
    fn vrem(db: &store::Db, key: &str, name: &str) -> bool {
        let Some(vs) = set_at(db, key) else {
            return false;
        };
        let mut inner = vs.write();
        let gone = inner.entries.remove(name.as_bytes()).is_some();
        if gone {
            vset_graph::removed(&inner, name.as_bytes());
        }
        gone
    }

    /// `VSIM ... COUNT count [TRUTH]`: (name, distance), nearest first.
    fn vsim(vs: &Set, v: &[f32], count: usize, truth: bool) -> Vec<(String, f32)> {
        let inner = vs.read();
        let query = StoredVector::quantize(inner.quant, v.to_vec()).unwrap();
        let asked = Search {
            count,
            epsilon: 2.0,
            ef: vset_graph::DEFAULT_SEARCH_EF.max(count),
            filter: None,
            filter_ef: count * 100,
            truth,
        };
        nearest(vs, &inner, &query, &asked)
            .into_iter()
            .map(|(d, n)| (String::from_utf8_lossy(n).into_owned(), d))
            .collect()
    }

    /// The exact answer, from the model alone.
    fn exact(
        model: &HashMap<String, Vec<f32>>,
        quant: Quant,
        metric: &DistanceMetric,
        dim: usize,
        v: &[f32],
        count: usize,
    ) -> Vec<(String, f32)> {
        let q = StoredVector::quantize(quant, v.to_vec()).unwrap();
        let mut all: Vec<(String, f32)> = model
            .iter()
            .map(|(n, x)| {
                let s = StoredVector::quantize(quant, x.clone()).unwrap();
                (n.clone(), q.distance(&s, metric, dim))
            })
            .collect();
        all.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        all.truncate(count);
        all
    }

    fn graph_of(vs: &Set) -> Arc<vset_graph::VsetGraph> {
        Arc::clone(vs.read().graph.get().expect("the set has a graph"))
    }

    fn settled(vs: &Set) {
        vset_graph::settle(&graph_of(vs));
    }

    /// Recall of `got` against `want`: the share of `got` no farther than
    /// the farthest of `want` (by distance, so that ties count as found).
    fn recall(got: &[(String, f32)], want: &[(String, f32)]) -> f64 {
        let Some(bound) = want.last().map(|(_, d)| *d) else {
            return 1.0;
        };
        got.iter().filter(|(_, d)| *d <= bound).count() as f64 / want.len() as f64
    }

    fn run_model(seed: u64, quant: Quant, metric: DistanceMetric) {
        let mut rng = StdRng::seed_from_u64(seed);
        let dim = 12;
        let st = store::new_store(1);
        let db = &st[0];
        let key = "vs";
        let mut model: HashMap<String, Vec<f32>> = HashMap::new();
        let n0 = vset_graph::EXACT_BELOW + 1500;
        for i in 0..n0 {
            let v = random_vector(&mut rng, dim);
            let name = format!("e{i}");
            assert!(vadd(db, key, addition(&name, &v, quant, &metric)));
            model.insert(name, v);
        }
        let vs = set_at(db, key).unwrap();
        settled(&vs);
        assert!(graph_of(&vs).is_ready());

        let mut next = n0;
        let mut recall_sum = 0.0;
        let mut searches = 0;
        for step in 0..3000 {
            match rng.gen_range(0..10) {
                // A new element, found by the next search of its vector.
                0..=2 => {
                    let v = random_vector(&mut rng, dim);
                    let name = format!("e{next}");
                    next += 1;
                    assert!(vadd(db, key, addition(&name, &v, quant, &metric)));
                    let got = vsim(&vs, &v, 50, false);
                    assert!(
                        got.iter().any(|(n, _)| *n == name),
                        "seed {seed} step {step}: {name} just added is not found"
                    );
                    model.insert(name, v);
                }
                // A new vector for an element: found where it now is.
                3..=4 => {
                    let names: Vec<&String> = model.keys().collect();
                    let name = names[rng.gen_range(0..names.len())].clone();
                    let v = random_vector(&mut rng, dim);
                    assert!(!vadd(db, key, addition(&name, &v, quant, &metric)));
                    let got = vsim(&vs, &v, 50, false);
                    assert!(
                        got.iter().any(|(n, _)| *n == name),
                        "seed {seed} step {step}: {name} given a new vector is not found there"
                    );
                    model.insert(name, v);
                }
                // A removal: never found again.
                5..=6 => {
                    let names: Vec<&String> = model.keys().collect();
                    let name = names[rng.gen_range(0..names.len())].clone();
                    assert!(vrem(db, key, &name));
                    model.remove(&name);
                    let v = random_vector(&mut rng, dim);
                    let got = vsim(&vs, &v, 50, false);
                    assert!(
                        got.iter().all(|(n, _)| *n != name),
                        "seed {seed}: {name} found after VREM"
                    );
                }
                // Searches: TRUTH exact, the graph's scores exact and its
                // recall bounded.
                _ => {
                    let v = random_vector(&mut rng, dim);
                    let want = exact(&model, quant, &metric, dim, &v, 10);
                    let truth = vsim(&vs, &v, 10, true);
                    assert_eq!(truth, want, "seed {seed} step {step}: TRUTH is not exact");
                    let got = vsim(&vs, &v, 10, false);
                    assert_eq!(got.len(), 10);
                    for (n, d) in &got {
                        let x = model
                            .get(n)
                            .unwrap_or_else(|| panic!("{n} is not in the set"));
                        let q = StoredVector::quantize(quant, v.clone()).unwrap();
                        let s = StoredVector::quantize(quant, x.clone()).unwrap();
                        assert_eq!(*d, q.distance(&s, &metric, dim), "{n}'s score");
                    }
                    recall_sum += recall(&got, &want);
                    searches += 1;
                }
            }
        }
        // Once the helpers are done, the graph holds every element as the
        // set does, and nothing else.
        settled(&vs);
        let graph = graph_of(&vs);
        assert_eq!(graph.index.live_count.load(Relaxed), model.len());
        assert_eq!(vs.read().entries.len(), model.len());
        let mut after = 0.0;
        for _ in 0..200 {
            let v = random_vector(&mut rng, dim);
            let want = exact(&model, quant, &metric, dim, &v, 10);
            after += recall(&vs_graph_only(&vs, &v), &want);
        }
        let during = recall_sum / searches as f64;
        let after = after / 200.0;
        assert!(
            during >= 0.9,
            "seed {seed} {quant:?}: recall {during:.3} during the writes"
        );
        assert!(
            after >= 0.9,
            "seed {seed} {quant:?}: recall {after:.3} once settled"
        );
    }

    /// The graph's answer, checked to be the graph's.
    fn vs_graph_only(vs: &Set, v: &[f32]) -> Vec<(String, f32)> {
        assert!(graph_of(vs).is_ready());
        vsim(vs, v, 10, false)
    }

    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn the_graph_agrees_with_an_exact_model() {
        run_model(1, Quant::F32, DistanceMetric::Cosine);
        run_model(2, Quant::Q8, DistanceMetric::Cosine);
        run_model(3, Quant::F32, DistanceMetric::L2);
        run_model(4, Quant::Bin, DistanceMetric::Cosine);
    }

    /// Overwrites and removals made while the builder hands the elements
    /// over leave no element where its old vector was.
    #[test]
    fn writes_during_the_build_leave_no_stale_position() {
        let mut rng = StdRng::seed_from_u64(7);
        let dim = 16;
        let metric = DistanceMetric::Cosine;
        let st = store::new_store(1);
        let db = &st[0];
        let mut model: HashMap<String, Vec<f32>> = HashMap::new();
        let n = vset_graph::EXACT_BELOW * 3;
        for i in 0..n {
            let v = random_vector(&mut rng, dim);
            let name = format!("e{i}");
            vadd(db, "vs", addition(&name, &v, Quant::F32, &metric));
            model.insert(name, v);
        }
        let vs = set_at(db, "vs").unwrap();
        // The build is running (or queued): overwrite and remove meanwhile.
        let mut moved = Vec::new();
        for i in 0..2000 {
            let name = format!("e{}", rng.gen_range(0..n));
            if i % 3 == 0 {
                if vrem(db, "vs", &name) {
                    model.remove(&name);
                }
            } else {
                let v = random_vector(&mut rng, dim);
                vadd(db, "vs", addition(&name, &v, Quant::F32, &metric));
                model.insert(name.clone(), v);
                moved.push(name);
            }
        }
        settled(&vs);
        assert_eq!(graph_of(&vs).index.live_count.load(Relaxed), model.len());
        for name in &moved {
            let Some(v) = model.get(name) else { continue };
            let got = vs_graph_only(&vs, v);
            assert_eq!(got[0].0, *name, "{name} is not found at its new vector");
        }
        let mut sum = 0.0;
        for _ in 0..200 {
            let v = random_vector(&mut rng, dim);
            sum += recall(
                &vs_graph_only(&vs, &v),
                &exact(&model, Quant::F32, &metric, dim, &v, 10),
            );
        }
        assert!(sum / 200.0 >= 0.9, "recall {}", sum / 200.0);
    }

    /// Writers, searches and the key's deletion at once: every write a
    /// writer was answered for is found by its next search, nothing found is
    /// not in the set, and a deleted set's graph is let go.
    #[test]
    fn writers_searches_and_deletes_at_once() {
        let dim = 8;
        let cosine = DistanceMetric::Cosine;
        let metric = &cosine;
        let st = store::new_store(1);
        let db = &st[0];
        let mut rng = StdRng::seed_from_u64(11);
        for i in 0..vset_graph::EXACT_BELOW + 500 {
            let v = random_vector(&mut rng, dim);
            vadd(
                db,
                "vs",
                addition(&format!("base{i}"), &v, Quant::F32, metric),
            );
        }
        settled(&set_at(db, "vs").unwrap());
        let stop = &std::sync::atomic::AtomicBool::new(false);
        let deletes = &std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|s| {
            for w in 0..4u64 {
                s.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(100 + w);
                    let mut i = 0;
                    while !stop.load(Relaxed) {
                        let name = format!("w{w}-{}", i % 300);
                        i += 1;
                        let v = random_vector(&mut rng, dim);
                        if rng.gen_range(0..4) == 0 {
                            vrem(db, "vs", &name);
                            continue;
                        }
                        vadd(db, "vs", addition(&name, &v, Quant::F32, metric));
                        // Only this writer writes `name`: the set holds it
                        // with `v` unless the key was deleted meanwhile.
                        let Some(vs) = set_at(db, "vs") else { continue };
                        let before = deletes.load(Relaxed);
                        let got = vsim(&vs, &v, 10, false);
                        let holds = vs.read().entries.contains_key(name.as_bytes());
                        if holds && deletes.load(Relaxed) == before {
                            assert!(
                                got.iter().any(|(n, _)| *n == name),
                                "{name}, written and answered, is not found"
                            );
                        }
                    }
                });
            }
            for r in 0..2u64 {
                s.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(200 + r);
                    while !stop.load(Relaxed) {
                        let Some(vs) = set_at(db, "vs") else { continue };
                        let v = random_vector(&mut rng, dim);
                        let inner_names: Vec<String> = vsim(&vs, &v, 20, false)
                            .into_iter()
                            .map(|(n, _)| n)
                            .collect();
                        assert!(inner_names.len() <= 20);
                    }
                });
            }
            // The key deleted (as DEL, FLUSHALL or its expiry delete it)
            // while the writers and the helpers work, and made again.
            let mut rng = StdRng::seed_from_u64(300);
            let mut gone = Vec::new();
            for _ in 0..3 {
                std::thread::sleep(std::time::Duration::from_millis(300));
                let old = set_at(db, "vs").unwrap();
                gone.push(Arc::downgrade(&graph_of(&old).index));
                deletes.fetch_add(1, Relaxed);
                store::get_shard(db, "vs").write().remove("vs");
                drop(old);
                for i in 0..vset_graph::EXACT_BELOW + 100 {
                    let v = random_vector(&mut rng, dim);
                    vadd(
                        db,
                        "vs",
                        addition(&format!("again{i}"), &v, Quant::F32, metric),
                    );
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(300));
            stop.store(true, Relaxed);
            // Each deleted set's graph is freed once its last search, write
            // and build are done.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while gone.iter().any(|g| g.strong_count() > 0) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "a deleted set's graph was kept"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        });
        let vs = set_at(db, "vs").unwrap();
        settled(&vs);
        let graph = graph_of(&vs);
        assert_eq!(
            graph.index.live_count.load(Relaxed),
            vs.read().entries.len()
        );
    }

    /// A copy of a set (`COPY`) has no graph of its own until it makes one,
    /// and never shares its source's.
    #[test]
    fn a_copy_makes_its_own_graph() {
        let dim = 8;
        let metric = DistanceMetric::Cosine;
        let st = store::new_store(1);
        let db = &st[0];
        let mut rng = StdRng::seed_from_u64(5);
        for i in 0..vset_graph::EXACT_BELOW + 10 {
            let v = random_vector(&mut rng, dim);
            vadd(db, "a", addition(&format!("e{i}"), &v, Quant::F32, &metric));
        }
        let a = set_at(db, "a").unwrap();
        settled(&a);
        let copy = KiviDBValue::VectorSet(Arc::clone(&a)).deep_clone();
        let KiviDBValue::VectorSet(b) = copy else {
            unreachable!()
        };
        assert!(b.read().graph.get().is_none());
        // A search asks for its graph; until it is made, it scans.
        let v = random_vector(&mut rng, dim);
        let first = vsim(&b, &v, 10, false);
        assert_eq!(first, vsim(&b, &v, 10, true));
        settled(&b);
        assert!(!Arc::ptr_eq(&graph_of(&a), &graph_of(&b)));
        b.write().entries.clear();
        assert_eq!(
            graph_of(&a).index.live_count.load(Relaxed),
            a.read().entries.len()
        );
    }
}
