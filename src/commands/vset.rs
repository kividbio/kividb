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
use crate::commands::helpers::parse_utf8;
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::{KiviDBValue, VSetEntry, VectorSetInner};
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

/// Get or create a VectorSet at `key` with a given dimension.
/// Returns Err(resp bytes) on type mismatch or dim mismatch.
fn get_or_create_vset(
    ctx: &mut Ctx,
    key: &str,
    dim: usize,
    metric: crate::vector::DistanceMetric,
) -> Result<Arc<RwLock<VectorSetInner>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            if inner.dim != dim {
                return Err(resp_error(&format!(
                    "ERR vector dimension mismatch: index has dim {}, got {}",
                    inner.dim, dim
                )));
            }
            // Metric mismatch on existing key: Redis ignores it silently, so do we
            Ok(Arc::clone(vs))
        }
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            let vs = Arc::new(RwLock::new(VectorSetInner::with_metric(dim, metric)));
            map.insert(Key::from(key), KiviDBValue::VectorSet(Arc::clone(&vs)));
            Ok(vs)
        }
    }
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
//      [NOQUANT | Q8 | BIN] [EF n] [SETATTR attrs] [M n]
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    // Walk arguments to find vector encoding and element name
    let mut i = 2usize;
    let mut attrs: Option<String> = None;

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

    let mut parsed_metric = crate::vector::DistanceMetric::Cosine;

    if cmd
        .get(i)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.eq_ignore_ascii_case("DISTANCE"))
        .unwrap_or(false)
    {
        i += 1; // skip "DISTANCE"
        if let Some(metric_str) = cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
            parsed_metric = crate::vector::DistanceMetric::metric_from_str(metric_str)
                .unwrap_or(crate::vector::DistanceMetric::Cosine);
            i += 1; // skip the metric value
        }
    }

    // Next token must be FP32 or VALUES
    let encoding = match cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
        Some(s) => s.to_ascii_uppercase(),
        None => return Some(resp_error("ERR expected FP32 or VALUES")),
    };
    i += 1;

    let (vec, next_i) = match encoding.as_str() {
        "FP32" => {
            // Next arg is the raw binary blob
            let blob = match cmd.get(i) {
                Some(b) => b,
                None => return Some(resp_error("ERR missing FP32 blob")),
            };
            // We don't know dim yet — infer from blob length
            if blob.len() % 4 != 0 {
                return Some(resp_error("ERR FP32 blob length must be a multiple of 4"));
            }
            let _dim = blob.len() / 4;
            let v: Vec<f32> = blob
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                .collect();
            (v, i + 1)
        }
        "VALUES" => match parse_values_vector(cmd, i) {
            Ok((v, n)) => (v, n),
            Err(e) => return Some(e),
        },
        _ => return Some(resp_error("ERR expected FP32 or VALUES after key")),
    };
    i = next_i;

    let dim = vec.len();

    // Next: element name
    let element = match cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
        Some(s) => s.to_string(),
        None => return Some(resp_error("ERR missing element name")),
    };
    i += 1;

    while i < cmd.len() {
        let tok = match std::str::from_utf8(cmd[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => {
                i += 1;
                continue;
            }
        };
        match tok.as_str() {
            // Kept at full precision whichever is asked for: `VINFO` says
            // so (`f32`).
            "NOQUANT" | "Q8" | "BIN" | "CAS" => {
                i += 1;
            }
            "EF" | "M" => {
                i += 2;
            } // skip value
            "SETATTR" => {
                i += 1;
                if let Some(a) = cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
                    attrs = Some(a.to_string());
                }
                i += 1;
            }
            "DISTANCE" => {
                i += 1;
                if let Some(metric_str) = cmd.get(i).and_then(|b| std::str::from_utf8(b).ok()) {
                    // Store metric_str for later — we'll use it when creating the vset
                    parsed_metric = crate::vector::DistanceMetric::metric_from_str(metric_str)
                        .unwrap_or(crate::vector::DistanceMetric::Cosine);
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Get or create the VectorSet
    let vs = match get_or_create_vset(ctx, &key, dim, parsed_metric) {
        Ok(v) => v,
        Err(e) => return Some(e),
    };

    let mut inner = vs.write();
    let is_new = !inner.entries.contains_key(&element);
    inner
        .entries
        .insert(element, VSetEntry { vector: vec, attrs });

    let result = resp_integer(if is_new { 1 } else { 0 });

    Some(result)
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

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
    for elem_bytes in &cmd[2..] {
        if let Ok(elem) = std::str::from_utf8(elem_bytes)
            && inner.entries.remove(elem).is_some()
        {
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let element = match parse_utf8(cmd[2]) {
        Some(e) => e,
        None => return Some(resp_error("ERR element must be valid UTF-8")),
    };
    let raw = cmd
        .get(3)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.eq_ignore_ascii_case("RAW"))
        .unwrap_or(false);

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            match inner.entries.get(element) {
                None => Some(resp_null_bulk()),
                Some(entry) => {
                    if raw {
                        // Return raw little-endian FP32 bytes
                        let bytes: Vec<u8> =
                            entry.vector.iter().flat_map(|f| f.to_le_bytes()).collect();
                        Some(resp_bulk(&bytes))
                    } else {
                        // Return as array of float strings — easier to read in redis-cli
                        let items: Vec<Vec<u8>> = entry
                            .vector
                            .iter()
                            .map(|f| resp_bulk_str(&f.to_string()))
                            .collect();
                        Some(resp_array(items))
                    }
                }
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let element = match parse_utf8(cmd[2]) {
        Some(e) => e,
        None => return Some(resp_error("ERR element must be valid UTF-8")),
    };

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let element = match parse_utf8(cmd[2]) {
        Some(e) => e,
        None => return Some(resp_error("ERR element must be valid UTF-8")),
    };

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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let element = match parse_utf8(cmd[2]) {
        Some(e) => e.to_string(),
        None => return Some(resp_error("ERR element must be valid UTF-8")),
    };
    let attr_str = match std::str::from_utf8(cmd[3]) {
        Ok(s) => s.to_string(),
        Err(_) => return Some(resp_error("ERR attrs must be valid UTF-8")),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_str()) {
        Some(ValueRef::VectorSet(vs)) => {
            let mut inner = vs.write();
            match inner.entries.get_mut(&element) {
                None => Some(resp_error("ERR no such element")),
                Some(entry) => {
                    entry.attrs = if attr_str.is_empty() {
                        None
                    } else {
                        Some(attr_str)
                    };
                    Some(resp_simple("OK"))
                }
            }
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => Some(resp_error("ERR no such key")),
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

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
                // Redis's name for vectors kept unquantized.
                resp_bulk_str("quant-type"),
                resp_bulk_str("f32"),
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let element = match parse_utf8(cmd[2]) {
        Some(e) => e,
        None => return Some(resp_error("ERR element must be valid UTF-8")),
    };
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
                Some(e) => e.vector.clone(),
            };

            // Brute-force: find 16 nearest neighbours
            let mut scored: Vec<(f32, &str)> = inner
                .entries
                .iter()
                .filter(|(name, _)| *name != element)
                .map(|(name, entry)| {
                    (
                        crate::vector::distance(&inner.metric, &query_vec, &entry.vector),
                        name.as_str(),
                    )
                })
                .collect();
            scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
            scored.truncate(16);

            // VLINKS returns an array of arrays (one per HNSW layer).
            // We return a single layer 0.
            let layer_items: Vec<Vec<u8>> = scored
                .iter()
                .flat_map(|(score, name)| {
                    let mut items = vec![resp_bulk_str(name)];
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
    if cmd.len() < 2 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'vrandmember' command",
        ));
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let count: Option<i64> = cmd
        .get(2)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok());

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        Some(ValueRef::VectorSet(vs)) => {
            let inner = vs.read();
            let names: Vec<&str> = inner.entries.keys().map(|s| s.as_str()).collect();

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
                    Some(resp_bulk_str(names[idx]))
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
                            result.push(resp_bulk_str(names[indices[i]]));
                        }
                    } else {
                        // Negative: may repeat, return exactly |count| elements
                        for _ in 0..n_abs {
                            let idx = rand::random::<usize>() % names.len();
                            result.push(resp_bulk_str(names[idx]));
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

fn parse_lex_bound(s: &str) -> (Option<&str>, bool) {
    match s {
        "-" | "+" => (None, true),
        s if s.starts_with('[') => (Some(&s[1..]), true),
        s if s.starts_with('(') => (Some(&s[1..]), false),
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let start_raw = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR start must be valid UTF-8")),
    };
    let end_raw = match std::str::from_utf8(cmd[3]) {
        Ok(s) => s,
        Err(_) => return Some(resp_error("ERR end must be valid UTF-8")),
    };
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
            let mut names: Vec<&str> = inner.entries.keys().map(|s| s.as_str()).collect();
            names.sort_unstable();

            let matches_start = |name: &str| -> bool {
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
            let matches_end = |name: &str| -> bool {
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
                iter.take(n).map(|name| resp_bulk_str(name)).collect()
            } else {
                iter.map(|name| resp_bulk_str(name)).collect()
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

    let encoding = match std::str::from_utf8(cmd[2]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return Some(resp_error("ERR encoding must be UTF-8")),
    };

    // Parse the query vector
    let (query_vec, mut i) = match encoding.as_str() {
        "ELE" => {
            // The query is identified by an element name stored in the set
            let elem_name = match std::str::from_utf8(cmd[3]) {
                Ok(s) => s.to_string(),
                Err(_) => return Some(resp_error("ERR element name must be valid UTF-8")),
            };
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

    let vs = match map.get(key.as_str()) {
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

    // Resolve ELE query — look up from the VectorSet
    let query_vec: Vec<f32> = match query_vec {
        Ok(v) => v,
        Err(elem_name) => match inner.entries.get(&elem_name) {
            Some(e) => e.vector.clone(),
            None => return Some(resp_error("ERR element not found in vector set")),
        },
    };

    // Brute-force cosine similarity search
    // Using select_nth_unstable: O(n) average, same trick as FT flat search
    let mut scored: Vec<(f32, &str)> = inner
        .entries
        .iter()
        .filter(|(_, entry)| entry.vector.len() == query_vec.len())
        .filter(|(_, entry)| {
            filter
                .as_ref()
                .is_none_or(|filter| filter.matches(entry.attrs.as_deref()))
        })
        .map(|(name, entry)| {
            (
                crate::vector::distance(&inner.metric, &query_vec, &entry.vector),
                name.as_str(),
            )
        })
        .collect();

    if scored.len() > count {
        scored.select_nth_unstable_by(count, |a, b| a.0.total_cmp(&b.0));
        scored.truncate(count);
    }
    scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));

    // Build response
    // Redis VSIM returns: [element, [score?, attribs?], ...]
    // Flat array with optional interleaved scores/attribs
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (score, name) in &scored {
        out.push(resp_bulk_str(name));
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
