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

/// Quantizes before inserting, so a rejected vector never leaves an empty set.
fn add_to_vset(
    ctx: &mut Ctx,
    key: &[u8],
    element: crate::key_bytes::KeyBytes,
    vector: Vec<f32>,
    attrs: Option<Box<str>>,
    asked_metric: Option<crate::vector::DistanceMetric>,
    asked_quant: Option<Quant>,
) -> Result<bool, Vec<u8>> {
    let dim = vector.len();
    let quantize = |quant: Quant, vector: Vec<f32>| {
        StoredVector::quantize(quant, vector)
            .map_err(|_| resp_error("ERR invalid vector specification"))
    };
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let vs = Arc::clone(vs);
            let mut inner = vs.write();
            drop(map);
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
            if asked_metric.as_ref().is_some_and(|m| *m != inner.metric) {
                return Err(resp_error(
                    "ERR asked distance mismatch with existing vector set",
                ));
            }
            // Unchosen sets, replays and replicated writes accept any option.
            let strict = inner.quant_chosen && !ctx.apply_replication;
            if strict && asked_quant.is_some_and(|q| q != inner.quant) {
                return Err(resp_error(
                    "ERR asked quantization mismatch with existing vector set",
                ));
            }
            let stored = quantize(inner.quant, vector)?;
            let entry = VSetEntry {
                vector: stored,
                attrs,
            };
            Ok(inner.entries.insert(element, entry).is_none())
        }
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            let metric = asked_metric.unwrap_or(crate::vector::DistanceMetric::Cosine);
            let mut inner = VectorSetInner::with_metric(dim, metric);
            // Default stays f32; Redis defaults to Q8.
            inner.quant = asked_quant.unwrap_or(Quant::F32);
            inner.quant_chosen = asked_quant.is_some();
            let stored = quantize(inner.quant, vector)?;
            inner.entries.insert(
                element,
                VSetEntry {
                    vector: stored,
                    attrs,
                },
            );
            map.insert(key, KiviDBValue::VectorSet(Arc::new(RwLock::new(inner))));
            Ok(true)
        }
    }
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
// Redis uses HNSW internally for VectorSets. We store vectors in
// VectorSetInner.entries and do brute-force VSIM (same trade-off as FLAT).
// HNSW for VectorSets is a natural next step.
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
                if !value
                    .and_then(|v| strict_integer(v))
                    .is_some_and(|n| (lo..=hi).contains(&n))
                {
                    return Some(resp_error(&format!("ERR invalid {tok}")));
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

    // Copy an older master, which stored f32 whatever was asked.
    let relaxed = std::sync::atomic::Ordering::Relaxed;
    if ctx.apply_replication && !ctx.repl_state.master_keeps_vset_quant.load(relaxed) {
        asked_quant = None;
    }
    match add_to_vset(ctx, &key, element, vec, attrs, parsed_metric, asked_quant) {
        Ok(is_new) => Some(resp_integer(is_new as i64)),
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
    let resp = ctx.resp;

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
            let dim = inner.dim;
            // We always use brute-force (FLAT equivalent)
            let out = vec![
                resp_bulk_str("vector-dim"),
                resp_integer(dim as i64),
                resp_bulk_str("vector-count"),
                resp_integer(count as i64),
                resp_bulk_str("quant-type"),
                resp_bulk_str(inner.quant.name()),
                resp_bulk_str("index-type"),
                resp_bulk_str("FLAT"),
                resp_bulk_str("max-level"),
                resp_integer(0),
            ];
            Some(resp_array(out))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_null_bulk()),
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
// Returns the HNSW graph neighbours of an element.
// Since we use brute-force, every other element is a "neighbour".
// We return the top-16 nearest instead — this is honest and useful.

fn vlinks(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vlinks' command",
        ));
    }
    let key: &[u8] = cmd[1];
    let element: &[u8] = cmd[2];
    let with_scores = cmd
        .get(3)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
        .unwrap_or(false);

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

            // Brute-force: find 16 nearest neighbours
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
            scored.sort_unstable_by(by_distance_then_name);
            scored.truncate(16);

            // VLINKS returns an array of arrays (one per HNSW layer).
            // We return a single layer 0.
            let layer_items: Vec<Vec<u8>> = scored
                .iter()
                .flat_map(|(score, name)| {
                    let mut items = vec![resp_bulk(name)];
                    if with_scores {
                        let similarity = similarity(&inner.metric, *score);
                        items.push(resp_bulk_str(&format!("{:.6}", similarity)));
                    }
                    items
                })
                .collect();

            Some(resp_array(vec![resp_array(layer_items)]))
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
//      [WITHSCORES] [WITHATTRIBS] [COUNT num]
//      [EF n] [FILTER expr] [TRUTH] [NOTHREAD]
//
// Finds the most similar vectors.
// ELE: use an existing element's vector as query
// FP32: raw binary blob
// VALUES: space-separated floats

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
        "ELE" => {
            // The query is identified by an element name stored in the set
            let elem_name: Vec<u8> = cmd[3].to_vec();
            // We'll resolve it after acquiring the lock — placeholder for now
            // We store a sentinel and handle below
            (Err(elem_name), 4usize)
        }
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

    // Parse optional flags
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count = 10usize;
    let mut filter: Option<super::vset_filter::Filter> = None;

    while i < cmd.len() {
        let tok = match std::str::from_utf8(cmd[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        match tok.as_str() {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "WITHATTRIBS" => {
                with_attribs = true;
                i += 1;
            }
            "COUNT" => {
                if let Some(n) = cmd
                    .get(i + 1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse().ok())
                {
                    count = n;
                }
                i += 2;
            }
            // The search is exact: how far it looks is not a choice.
            "EF" | "FILTER-EF" => {
                i += 2;
            }
            // Only the elements whose attributes pass are scored (#132).
            "FILTER" => {
                let Some(expr) = cmd.get(i + 1).and_then(|b| std::str::from_utf8(b).ok()) else {
                    return Some(resp_error(super::vset_filter::SYNTAX_ERROR));
                };
                match super::vset_filter::Filter::compile(expr) {
                    Some(compiled) => filter = Some(compiled),
                    None => return Some(resp_error(super::vset_filter::SYNTAX_ERROR)),
                }
                i += 2;
            }
            "TRUTH" | "NOTHREAD" => {
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

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
            None => return Some(resp_error("ERR element not found in vector set")),
        },
    };

    // Brute-force search
    // Using select_nth_unstable: O(n) average, same trick as FT flat search
    let mut scored: Vec<(f32, &[u8])> = inner
        .entries
        .iter()
        .filter(|(_, entry)| {
            filter
                .as_ref()
                .is_none_or(|filter| filter.matches(entry.attrs.as_deref()))
        })
        .map(|(name, entry)| {
            (
                query.distance(&entry.vector, &inner.metric, inner.dim),
                name.as_bytes(),
            )
        })
        .collect();

    if scored.len() > count {
        scored.select_nth_unstable_by(count, by_distance_then_name);
        scored.truncate(count);
    }
    scored.sort_unstable_by(by_distance_then_name);

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
