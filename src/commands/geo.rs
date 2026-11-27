use crate::commands::ctx::Ctx;
use crate::commands::helpers::{expire_on_read, is_expired, lazy_expire_on_read, parse_utf8};
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;
use crate::types::{KiviDBValue, SortedSetInner, ZMember};
use crate::{read_lock, write_lock};
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::geohash::*;

/// Effective protocol for the reply being built (honours `redis.setresp`).
///
/// Geo replies carry coordinates, which Redis emits with `addReplyDouble`: the
/// double type (`,`) under RESP3, a bulk string of the same digits under
/// RESP2. Distances are bulk strings under both (`addReplyDoubleDistance`).
/// GEOHASH is unaffected: its values are genuine base32 strings, not numbers.
#[inline]
fn reply_resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

/// A coordinate pair, `[lon, lat]`, shaped for `resp`.
#[inline]
fn coord_pair(resp: u8, lon: f64, lat: f64) -> Vec<u8> {
    if resp >= 3 {
        resp_array(vec![resp_double_resp3(lon), resp_double_resp3(lat)])
    } else {
        resp_array(vec![resp_double_bulk(lon), resp_double_bulk(lat)])
    }
}

// ── Base32 geohash string encoding ───────────────────────────────────────────
//
// `GEOHASH` answers with the standard geohash string (geohash.org's), which
// scales latitude from -90..90 — not from the narrower range positions are
// indexed in. So the position is decoded and encoded again in that range,
// and its 52 bits written five at a time. That is ten characters; the
// eleventh, which Redis has always sent, is a `0`.

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

fn geohash_string(lon: f64, lat: f64) -> String {
    let bits = encode_cell(lon, lat, STEP_MAX, (-90.0, 90.0)).map_or(0, |cell| cell.bits);
    (0..11)
        .map(|i| {
            let index = if i == 10 {
                0
            } else {
                (bits >> (52 - (i + 1) * 5)) & 0x1f
            };
            BASE32[index as usize] as char
        })
        .collect()
}

// ── Sorted set helpers ────────────────────────────────────────────────────────

fn get_or_create_zset(ctx: &mut Ctx, key: &[u8]) -> Result<Arc<RwLock<SortedSetInner>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);

    match map.get(key) {
        Some(ValueRef::SortedSet(zs)) => Ok(Arc::clone(zs)),
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            let zs = crate::sorted_set::shared(SortedSetInner::new());
            map.insert(key, KiviDBValue::SortedSet(Arc::clone(&zs)));
            Ok(zs)
        }
    }
}

/// A longitude and a latitude, as two arguments.
fn lon_lat(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), Vec<u8>> {
    let number = |raw: &[u8]| {
        parse_utf8(raw)
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| !v.is_nan())
            .ok_or_else(|| resp_error("ERR value is not a valid float"))
    };
    let (lon, lat) = (number(lon)?, number(lat)?);
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return Err(resp_error(&format!(
            "ERR invalid longitude,latitude pair {lon:.6},{lat:.6}"
        )));
    }
    Ok((lon, lat))
}

// ── GEOADD ───────────────────────────────────────────────────────────────────
// GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...]
fn cmd_geoadd(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = ctx.cmd.clone();
    if cmd.len() < 5 {
        return resp_error("ERR wrong number of arguments for 'geoadd' command");
    }

    let key: Vec<u8> = cmd[1].to_vec();

    // Parse optional flags before the coordinate triples
    let mut i = 2usize;
    let mut flag_nx = false;
    let mut flag_xx = false;
    let mut flag_ch = false;

    loop {
        if i >= cmd.len() {
            break;
        }
        let tok = std::str::from_utf8(cmd[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        match tok.as_str() {
            "NX" => {
                flag_nx = true;
                i += 1;
            }
            "XX" => {
                flag_xx = true;
                i += 1;
            }
            "CH" => {
                flag_ch = true;
                i += 1;
            }
            _ => break,
        }
    }

    if flag_nx && flag_xx {
        return resp_error("ERR syntax error");
    }

    // Remaining args must be triples: lon lat member
    if !(cmd.len() - i).is_multiple_of(3) || i >= cmd.len() {
        return resp_error("ERR syntax error");
    }

    // Every position is checked before any is stored: a command that is
    // refused has changed nothing.
    for triple in cmd[i..].chunks(3) {
        if let Err(e) = lon_lat(triple[0], triple[1]) {
            return e;
        }
    }

    let zs = match get_or_create_zset(ctx, &key) {
        Ok(z) => z,
        Err(e) => return e,
    };

    let mut added = 0i64;
    let mut changed = 0i64;
    let mut inner = zs.write();
    // As Redis runs it, a `ZADD` of the command's pairs: a block they cannot
    // fit becomes the large form first.
    let given = (cmd.len() - i) / 3;
    inner.expect(given);

    while i + 2 < cmd.len() {
        let (lon, lat) = match lon_lat(cmd[i], cmd[i + 1]) {
            Ok(position) => position,
            Err(e) => return e,
        };
        let member: &[u8] = cmd[i + 2];
        i += 3;

        let score = encode_geohash(lon, lat);
        // One lookup a member.
        inner.upsert(member, |existing| match existing {
            None if flag_xx => (None, ()), // XX: only update existing
            None => {
                added += 1;
                changed += 1;
                (Some(score), ())
            }
            Some(_old_score) if flag_nx => (None, ()), // NX: only add new
            Some(old_score) => {
                if (old_score - score).abs() > f64::EPSILON {
                    changed += 1;
                    (Some(score), ())
                } else {
                    (None, ())
                }
            }
        });
    }
    // As `ZADD`: a table reserved for members given twice is made as small
    // as what came, and one left mid-resize goes to the background.
    inner.settle(given);
    crate::sorted_set::after_write(&zs, &mut inner);

    drop(inner);

    // CH flag: return count of added + changed; otherwise just added
    resp_integer(if flag_ch { changed } else { added })
}

// ── GEODIST ───────────────────────────────────────────────────────────────────
// GEODIST key member1 member2 [M|KM|FT|MI]
fn cmd_geodist(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 4 || cmd.len() > 5 {
        return resp_error("ERR wrong number of arguments for 'geodist' command");
    }

    let key: &[u8] = cmd[1];
    let unit = if cmd.len() == 5 {
        std::str::from_utf8(cmd[4]).unwrap_or("M")
    } else {
        "M"
    };

    if is_expired(ctx.store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, *ctx.selected_db, key);
        crate::stats::record_miss();
        return resp_null_bulk();
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let found = map.get(key);
    crate::stats::record_lookup(found.is_some());
    let zs = match found {
        Some(ValueRef::SortedSet(z)) => z.clone(),
        None => return resp_null_bulk(),
        _ => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    drop(map);

    let inner = zs.read();
    let score1 = match inner.score(cmd[2]) {
        Some(s) => s,
        None => return resp_null_bulk(),
    };
    let score2 = match inner.score(cmd[3]) {
        Some(s) => s,
        None => return resp_null_bulk(),
    };
    drop(inner);

    let (lon1, lat1) = decode_geohash(score1);
    let (lon2, lat2) = decode_geohash(score2);
    let dist = haversine_m(lon1, lat1, lon2, lat2);

    // Validate unit before returning
    if to_meters(1.0, unit).is_none() {
        return resp_error("ERR unsupported unit provided. please use M, KM, FT, MI");
    }

    resp_bulk_str(&format_dist(dist, unit))
}

// ── GEOPOS ────────────────────────────────────────────────────────────────────
// GEOPOS key [member [member ...]]
// Returns array of [lon, lat] pairs (or nil for missing members)
fn cmd_geopos(ctx: &mut Ctx) -> Vec<u8> {
    // Every nil below sits inside the outer array, so `resp3_normalize_null`
    // never sees it — each has to be encoded for the negotiated protocol here.
    let resp = reply_resp(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'geopos' command");
    }

    let key: &[u8] = cmd[1];

    if is_expired(ctx.store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, *ctx.selected_db, key);
        crate::stats::record_miss();
        let nils = vec![resp_null_for(resp); cmd.len() - 2];
        return resp_array(nils);
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let found = map.get(key);
    crate::stats::record_lookup(found.is_some());
    let zs = match found {
        Some(ValueRef::SortedSet(z)) => z.clone(),
        None => {
            let nils = vec![resp_null_for(resp); cmd.len() - 2];
            return resp_array(nils);
        }
        _ => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    drop(map);

    let inner = zs.read();
    let mut results: Vec<Vec<u8>> = Vec::with_capacity(cmd.len() - 2);

    for member_bytes in &cmd[2..] {
        match inner.score(member_bytes) {
            None => results.push(resp_null_for(resp)),
            Some(score) => {
                let (lon, lat) = decode_geohash(score);
                results.push(coord_pair(resp, lon, lat));
            }
        }
    }

    resp_array(results)
}

// ── GEOHASH ───────────────────────────────────────────────────────────────────
// GEOHASH key [member [member ...]]
// Returns base32 geohash strings
fn cmd_geohash(ctx: &mut Ctx) -> Vec<u8> {
    // GEOHASH's values are base32 strings under both protocols; only its
    // per-member nils need protocol-correct encoding.
    let resp = reply_resp(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'geohash' command");
    }

    let key: &[u8] = cmd[1];

    if is_expired(ctx.store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, *ctx.selected_db, key);
        crate::stats::record_miss();
        let nils = vec![resp_null_for(resp); cmd.len() - 2];
        return resp_array(nils);
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let found = map.get(key);
    crate::stats::record_lookup(found.is_some());
    let zs = match found {
        Some(ValueRef::SortedSet(z)) => z.clone(),
        None => {
            let nils = vec![resp_null_for(resp); cmd.len() - 2];
            return resp_array(nils);
        }
        _ => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    drop(map);

    let inner = zs.read();
    let mut results: Vec<Vec<u8>> = Vec::with_capacity(cmd.len() - 2);

    for member_bytes in &cmd[2..] {
        match inner.score(member_bytes) {
            None => results.push(resp_null_for(resp)),
            Some(score) => {
                let (lon, lat) = decode_geohash(score);
                results.push(resp_bulk_str(&geohash_string(lon, lat)));
            }
        }
    }

    resp_array(results)
}

// ── Searching an area ─────────────────────────────────────────────────────────
//
// GEOSEARCH, GEOSEARCHSTORE, GEORADIUS and GEORADIUSBYMEMBER (and the `_RO`
// two) are one command with four ways of being written: find the members
// inside a circle or a box, optionally sort them by distance and keep the
// first few, and either reply with them or store them.
//
// The area is covered with nine cells — the one its centre is in and the
// eight around it, of a size chosen so that nine are enough — and the
// members of each are a range of scores. Each member found is then measured
// against the area itself. This is how Redis does it, and in the same order,
// which matters for one thing a client can see: with `COUNT n ANY` the
// search stops at the first `n` it comes across.

/// Which way the command is written.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Form {
    /// `GEORADIUS key lon lat radius unit …`
    Radius,
    /// `GEORADIUSBYMEMBER key member radius unit …`
    RadiusByMember,
    /// `GEOSEARCH key …`
    Search,
    /// `GEOSEARCHSTORE destination key …`
    SearchStore,
}

/// A member found inside the area: borrowed from the set, whose read lock
/// the search holds until its reply is made (16 bytes, as the `Arc` it
/// was: the nearest are sorted out of them).
struct Found<'a> {
    member: &'a [u8],
    score: f64,
    distance_m: f64,
    lon: f64,
    lat: f64,
}

/// The members of `zset` inside `area`, in the order the search comes
/// across them; no more than `limit` of them, if there is a limit.
fn members_in<'a>(zset: &'a SortedSetInner, area: &Area, limit: Option<usize>) -> Vec<Found<'a>> {
    let mut found = Vec::new();
    let mut last: Option<Cell> = None;
    for cell in area.cells() {
        if cell == Cell::default() {
            continue;
        }
        // Around an area thousands of kilometres across, neighbours can be
        // the same cell.
        if last == Some(cell) {
            continue;
        }
        if limit.is_some_and(|limit| found.len() >= limit) {
            break;
        }
        last = Some(cell);
        // The scores of a cell: from its bits followed by zeros, up to
        // those of the cell after it.
        let shift = 52 - cell.step as u32 * 2;
        let (from, to) = (
            (cell.bits << shift) as f64,
            ((cell.bits + 1) << shift) as f64,
        );
        let first: ZMember = Arc::from(&[][..]);
        for (member, score) in zset.range_by_score((OrderedFloat(from), first)..) {
            if score >= to {
                break;
            }
            let (lon, lat) = decode_geohash(score);
            if let Some(distance_m) = area.contains(lon, lat) {
                found.push(Found {
                    member: member.borrowed(),
                    score,
                    distance_m,
                    lon,
                    lat,
                });
                if limit.is_some_and(|limit| found.len() >= limit) {
                    break;
                }
            }
        }
    }
    found
}

/// `radius unit`, as metres and metres per unit.
fn distance_arg(value: &[u8], unit: &[u8], what: &str) -> Result<(f64, f64), Vec<u8>> {
    let value = parse_utf8(value)
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| !v.is_nan())
        .ok_or_else(|| resp_error(&format!("ERR need numeric {what}")))?;
    let unit_m = parse_utf8(unit)
        .and_then(|unit| to_meters(1.0, unit))
        .ok_or_else(|| resp_error("ERR unsupported unit provided. please use M, KM, FT, MI"))?;
    Ok((value, unit_m))
}

fn geo_search(ctx: &mut Ctx, form: Form, read_only: bool) -> Vec<u8> {
    let resp = reply_resp(ctx);
    let cmd = ctx.cmd.clone();
    let name = match form {
        Form::SearchStore => "GEOSEARCHSTORE",
        Form::Search => "GEOSEARCH",
        _ => "STORE option in GEORADIUS",
    };
    let syntax = || resp_error("ERR syntax error");
    let source_at = if form == Form::SearchStore { 2 } else { 1 };
    let Some(key) = cmd.get(source_at).map(|raw| raw.to_vec()) else {
        return syntax();
    };

    // The set, if there is one. (Its members are looked at under its lock,
    // which the search takes for as long as it runs.)
    let gone = lazy_expire_on_read(ctx.store, *ctx.selected_db, &key);
    let zset = {
        let shard = store::get_shard(&ctx.store[*ctx.selected_db], &key);
        let map = read_lock!(shard);
        let found = map.get(key.as_slice()).filter(|_| !gone);
        crate::stats::record_lookup(found.is_some());
        match found {
            Some(ValueRef::SortedSet(zset)) => Some(Arc::clone(zset)),
            Some(_) => {
                return resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
            }
            None => None,
        }
    };
    let position_of = |member: &[u8]| -> Result<(f64, f64), Vec<u8>> {
        zset.as_ref()
            .and_then(|zset| zset.read().score(member))
            .map(decode_geohash)
            .ok_or_else(|| resp_error("ERR could not decode requested zset member"))
    };

    // The centre and the shape, where the form of the command fixes where
    // they are.
    let mut centre: Option<(f64, f64)> = None;
    let mut shape: Option<(Shape, f64)> = None;
    let mut store_key: Option<Vec<u8>> = None;
    let options_at = match form {
        Form::Radius => {
            if cmd.len() < 6 {
                return resp_error("ERR wrong number of arguments for 'georadius' command");
            }
            centre = match lon_lat(cmd[2], cmd[3]) {
                Ok(position) => Some(position),
                Err(e) => return e,
            };
            6
        }
        Form::RadiusByMember => {
            if cmd.len() < 5 {
                return resp_error("ERR wrong number of arguments for 'georadiusbymember' command");
            }
            // With no set there is nothing to find, but the rest of the
            // command is still read: what it replies depends on `STORE`.
            if zset.is_some() {
                centre = match position_of(cmd[2]) {
                    Ok(position) => Some(position),
                    Err(e) => return e,
                };
            }
            5
        }
        Form::Search => 2,
        Form::SearchStore => {
            store_key = Some(cmd[1].to_vec());
            3
        }
    };
    if matches!(form, Form::Radius | Form::RadiusByMember)
        && (form == Form::Radius || zset.is_some())
    {
        let (radius, unit_m) =
            match distance_arg(cmd[options_at - 2], cmd[options_at - 1], "radius") {
                Ok(distance) => distance,
                Err(e) => return e,
            };
        if radius < 0.0 {
            return resp_error("ERR radius cannot be negative");
        }
        shape = Some((
            Shape::Circle {
                radius_m: radius * unit_m,
            },
            unit_m,
        ));
    }

    // The options.
    let searching = matches!(form, Form::Search | Form::SearchStore);
    let (mut with_dist, mut with_hash, mut with_coord) = (false, false, false);
    let (mut any, mut store_dist) = (false, false);
    let mut descending: Option<bool> = None;
    let mut count: Option<usize> = None;
    let (mut from_member, mut from_lonlat, mut by_radius, mut by_box) =
        (false, false, false, false);
    let args = &cmd[options_at.min(cmd.len())..];
    let mut i = 0;
    while i < args.len() {
        let word = args[i].to_ascii_uppercase();
        let left = args.len() - i - 1;
        match word.as_slice() {
            b"WITHDIST" => with_dist = true,
            b"WITHHASH" => with_hash = true,
            b"WITHCOORD" => with_coord = true,
            b"ANY" => any = true,
            b"ASC" => descending = Some(false),
            b"DESC" => descending = Some(true),
            b"COUNT" if left >= 1 => {
                count = match parse_utf8(args[i + 1]).and_then(|s| s.parse::<i64>().ok()) {
                    Some(n) if n > 0 => Some(n as usize),
                    Some(_) => return resp_error("ERR COUNT must be > 0"),
                    None => return resp_error("ERR value is not an integer or out of range"),
                };
                i += 1;
            }
            b"STORE" | b"STOREDIST" if left >= 1 && !read_only && !searching => {
                store_key = Some(args[i + 1].to_vec());
                store_dist = word == b"STOREDIST";
                i += 1;
            }
            b"STOREDIST" if form == Form::SearchStore => store_dist = true,
            b"FROMMEMBER" if searching && left >= 1 && !from_lonlat => {
                if zset.is_some() {
                    centre = match position_of(args[i + 1]) {
                        Ok(position) => Some(position),
                        Err(e) => return e,
                    };
                }
                from_member = true;
                i += 1;
            }
            b"FROMLONLAT" if searching && left >= 2 && !from_member => {
                centre = match lon_lat(args[i + 1], args[i + 2]) {
                    Ok(position) => Some(position),
                    Err(e) => return e,
                };
                from_lonlat = true;
                i += 2;
            }
            b"BYRADIUS" if searching && left >= 2 && !by_box => {
                let (radius, unit_m) = match distance_arg(args[i + 1], args[i + 2], "radius") {
                    Ok(distance) => distance,
                    Err(e) => return e,
                };
                if radius < 0.0 {
                    return resp_error("ERR radius cannot be negative");
                }
                shape = Some((
                    Shape::Circle {
                        radius_m: radius * unit_m,
                    },
                    unit_m,
                ));
                by_radius = true;
                i += 2;
            }
            b"BYBOX" if searching && left >= 3 && !by_radius => {
                let (width, unit_m) = match distance_arg(args[i + 1], args[i + 3], "width") {
                    Ok(distance) => distance,
                    Err(e) => return e,
                };
                let (height, _) = match distance_arg(args[i + 2], args[i + 3], "height") {
                    Ok(distance) => distance,
                    Err(e) => return e,
                };
                if width < 0.0 || height < 0.0 {
                    return resp_error("ERR height or width cannot be negative");
                }
                shape = Some((
                    Shape::Box {
                        width_m: width * unit_m,
                        height_m: height * unit_m,
                    },
                    unit_m,
                ));
                by_box = true;
                i += 3;
            }
            _ => return syntax(),
        }
        i += 1;
    }

    if store_key.is_some() && (with_dist || with_hash || with_coord) {
        return resp_error(&format!(
            "ERR {name} is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        ));
    }
    if searching && !(from_member || from_lonlat) {
        return resp_error(&format!(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {name}"
        ));
    }
    if searching && !(by_radius || by_box) {
        return resp_error(&format!(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for {name}"
        ));
    }
    if any && count.is_none() {
        return resp_error("ERR the ANY argument requires COUNT argument");
    }

    // No set: nothing found. A destination is emptied of what an earlier
    // search left in it.
    // The set's read lock, once, for as long as what is found borrows its
    // members (nothing below reads the set again).
    let held = zset.as_ref().map(|zset| zset.read());
    let found = match (&held, centre, shape) {
        (Some(inner), Some((lon, lat)), Some((shape, unit_m))) => {
            let area = Area { lon, lat, shape };
            let mut found = members_in(inner, &area, if any { count } else { None });
            // The nearest `count` cannot be picked without sorting: `COUNT`
            // alone sorts by distance. With `ANY` it is the first `count`
            // found that are wanted, in whatever order.
            let descending = descending.or((count.is_some() && !any).then_some(false));
            if let Some(descending) = descending {
                found.sort_by(|a, b| {
                    let nearer = a.distance_m.total_cmp(&b.distance_m);
                    if descending { nearer.reverse() } else { nearer }
                });
            }
            if let Some(count) = count {
                found.truncate(count);
            }
            Some((found, unit_m))
        }
        _ => None,
    };
    let (found, unit_m) = found.unwrap_or((Vec::new(), 1.0));

    if let Some(dest) = store_key {
        let db = *ctx.selected_db;
        let count = found.len();
        // Built whole: a block, laid down once, if the points fit one.
        let stored = (count > 0).then(|| {
            SortedSetInner::from_pairs(found.iter().map(|point| {
                (
                    point.member,
                    if store_dist {
                        point.distance_m / unit_m
                    } else {
                        point.score
                    },
                )
            }))
        });
        // The source's read lock goes before the keyspace is written: the
        // destination may be the source, and taking it out of the keyspace
        // reads it again (a second read on this thread waits behind a
        // writer queued since the first, which waits for the first).
        drop(found);
        drop(held);
        // The destination is a new key: whatever was there, and its time
        // to live, are gone.
        crate::commands::helpers::delete_key(ctx.store, db, &dest);
        if let Some(stored) = stored {
            let shard = store::get_shard(&ctx.store[db], &dest);
            write_lock!(shard).insert(
                dest,
                KiviDBValue::SortedSet(crate::sorted_set::shared(stored)),
            );
        }
        return resp_integer(count as i64);
    }

    let plain = !with_dist && !with_hash && !with_coord;
    resp_array(
        found
            .iter()
            .map(|point| {
                if plain {
                    return resp_bulk(point.member);
                }
                let mut item = vec![resp_bulk(point.member)];
                if with_dist {
                    item.push(resp_bulk_str(&format!("{:.4}", point.distance_m / unit_m)));
                }
                if with_hash {
                    item.push(resp_integer(point.score as i64));
                }
                if with_coord {
                    item.push(coord_pair(resp, point.lon, point.lat));
                }
                resp_array(item)
            })
            .collect(),
    )
}

// ── Public handle ─────────────────────────────────────────────────────────────
pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "GEOADD" => Some(cmd_geoadd(ctx)),
        "GEODIST" => Some(cmd_geodist(ctx)),
        "GEOPOS" => Some(cmd_geopos(ctx)),
        "GEOHASH" => Some(cmd_geohash(ctx)),
        "GEOSEARCH" => Some(geo_search(ctx, Form::Search, false)),
        "GEOSEARCHSTORE" => Some(geo_search(ctx, Form::SearchStore, false)),
        "GEORADIUS" => Some(geo_search(ctx, Form::Radius, false)),
        "GEORADIUS_RO" => Some(geo_search(ctx, Form::Radius, true)),
        "GEORADIUSBYMEMBER" => Some(geo_search(ctx, Form::RadiusByMember, false)),
        "GEORADIUSBYMEMBER_RO" => Some(geo_search(ctx, Form::RadiusByMember, true)),
        _ => None,
    }
}
