use crate::commands::ctx::Ctx;
use crate::commands::helpers::{expire_on_read, is_expired, lazy_expire_on_read, parse_utf8};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::{KiviDBValue, SortedSetInner, ZMember};
use crate::{read_lock, write_lock};
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::sync::Arc;

/// Effective protocol for the reply being built (honours `redis.setresp`).
///
/// Geo replies carry distances and coordinates, which Redis emits with
/// `addReplyHumanLongDouble` — a bulk string under RESP2 and the double type
/// (`,`) under RESP3. GEOHASH is unaffected: its values are genuine base32
/// strings, not numbers.
#[inline]
fn reply_resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

/// A coordinate pair, `[lon, lat]`, shaped for `resp`.
///
/// The RESP2 branch keeps the existing fixed 17-decimal rendering so its bytes
/// are unchanged. RESP3 uses the shortest round-trip form: a client parses
/// either into the same `f64`, and the short form is what the double type is
/// for.
#[inline]
fn coord_pair(resp: u8, lon: f64, lat: f64) -> Vec<u8> {
    if resp >= 3 {
        resp_array(vec![resp_double_resp3(lon), resp_double_resp3(lat)])
    } else {
        resp_array(vec![
            resp_bulk_str(&format!("{:.17}", lon)),
            resp_bulk_str(&format!("{:.17}", lat)),
        ])
    }
}

// ── Geo constants ─────────────────────────────────────────────────────────────
//
// Redis uses 52 bits of geohash precision (26 bits each for lon and lat).
// The hash is stored as an f64 score. Since f64 has 53 bits of mantissa,
// a 52-bit integer fits exactly — no precision is lost.

const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;

const EARTH_RADIUS_M: f64 = 6372797.560856;

// ── Geohash encoding ──────────────────────────────────────────────────────────
//
// A position is kept as the score of a sorted-set member: a 52-bit geohash,
// the bits of its latitude and longitude interleaved — latitude in the even
// bits, longitude in the odd ones — after each has been scaled from its
// range to 26 bits. That is Redis's encoding, bit for bit: a score written
// by Redis means the same position here, `ZSCORE` on a geo key answers what
// Redis answers, and so does `GEOHASH`.
//
// (Until 1.0.4 the two were interleaved the other way round. See CHANGELOG.)
//
// A geohash cut short — its first `2 * step` bits — names a *cell*: the
// rectangle of all the positions whose hashes begin that way. Members in a
// cell are a contiguous run of scores, which is what makes a search by area
// a handful of range scans rather than a look at every member.

/// The first `2 * step` bits of a geohash: a cell. `step == 0` is "none".
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
struct Cell {
    bits: u64,
    step: u8,
}

const STEP_MAX: u8 = 26;

/// A zero between every two bits of `x`.
fn spread_bits(x: u32) -> u64 {
    let mut x = x as u64;
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
    x = (x | (x << 8)) & 0x00FF00FF00FF00FF;
    x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x << 2)) & 0x3333333333333333;
    x = (x | (x << 1)) & 0x5555555555555555;
    x
}

/// The inverse: every other bit of `x`, squeezed together.
fn compact_bits(x: u64) -> u32 {
    let mut x = x & 0x5555555555555555;
    x = (x | (x >> 1)) & 0x3333333333333333;
    x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x >> 4)) & 0x00FF00FF00FF00FF;
    x = (x | (x >> 8)) & 0x0000FFFF0000FFFF;
    x = (x | (x >> 16)) & 0x00000000FFFFFFFF;
    x as u32
}

/// The cell of `step` that (`lon`, `lat`) is in, with latitude scaled from
/// `lat_range`. `None` for a position outside what can be indexed.
fn encode_cell(lon: f64, lat: f64, step: u8, lat_range: (f64, f64)) -> Option<Cell> {
    if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat)
        || !(lat_range.0..=lat_range.1).contains(&lat)
    {
        return None;
    }
    let scale = (1u64 << step) as f64;
    let lat_offset = (lat - lat_range.0) / (lat_range.1 - lat_range.0) * scale;
    let lon_offset = (lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN) * scale;
    Some(Cell {
        bits: spread_bits(lat_offset as u32) | (spread_bits(lon_offset as u32) << 1),
        step,
    })
}

/// The rectangle a cell covers: `((lon_min, lon_max), (lat_min, lat_max))`.
fn cell_bounds(cell: Cell) -> ((f64, f64), (f64, f64)) {
    let lat = compact_bits(cell.bits) as f64;
    let lon = compact_bits(cell.bits >> 1) as f64;
    let scale = (1u64 << cell.step) as f64;
    let lat_span = GEO_LAT_MAX - GEO_LAT_MIN;
    let lon_span = GEO_LON_MAX - GEO_LON_MIN;
    (
        (
            GEO_LON_MIN + lon / scale * lon_span,
            GEO_LON_MIN + (lon + 1.0) / scale * lon_span,
        ),
        (
            GEO_LAT_MIN + lat / scale * lat_span,
            GEO_LAT_MIN + (lat + 1.0) / scale * lat_span,
        ),
    )
}

/// Encode (longitude, latitude) → 52-bit geohash as an f64 score.
fn encode_geohash(lon: f64, lat: f64) -> f64 {
    encode_cell(lon, lat, STEP_MAX, (GEO_LAT_MIN, GEO_LAT_MAX))
        .map(|cell| cell.bits as f64)
        .unwrap_or(0.0)
}

/// Decode a geohash score back to (longitude, latitude): the middle of the
/// cell the score names.
fn decode_geohash(score: f64) -> (f64, f64) {
    let ((lon_min, lon_max), (lat_min, lat_max)) = cell_bounds(Cell {
        bits: score as u64,
        step: STEP_MAX,
    });
    (
        ((lon_min + lon_max) / 2.0).clamp(GEO_LON_MIN, GEO_LON_MAX),
        ((lat_min + lat_max) / 2.0).clamp(GEO_LAT_MIN, GEO_LAT_MAX),
    )
}

/// The cell `dx` to the east and `dy` to the north of `cell` (each -1, 0 or
/// 1), wrapping round at the edges of the map.
fn cell_beside(cell: Cell, dx: i8, dy: i8) -> Cell {
    let shift = 64 - cell.step as u32 * 2;
    // Longitude is in the odd bits, latitude in the even ones.
    let odd = 0xaaaaaaaaaaaaaaaau64;
    let even = 0x5555555555555555u64;
    let mut x = cell.bits & odd;
    let mut y = cell.bits & even;
    // Adding one to a number whose bits alternate with another's: fill the
    // other's positions with ones, so that the carry runs through them.
    if dx != 0 {
        let fill = even >> shift;
        x = if dx > 0 {
            x.wrapping_add(fill + 1)
        } else {
            (x | fill).wrapping_sub(fill + 1)
        };
        x &= odd >> shift;
    }
    if dy != 0 {
        let fill = odd >> shift;
        y = if dy > 0 {
            y.wrapping_add(fill + 1)
        } else {
            (y | fill).wrapping_sub(fill + 1)
        };
        y &= even >> shift;
    }
    Cell {
        bits: x | y,
        step: cell.step,
    }
}

// ── Distances ─────────────────────────────────────────────────────────────────
//
// Great-circle distances on a sphere of Redis's radius, by the haversine
// formula — written as Redis writes it, so that a distance comes out the
// same to the last digit shown.

/// The distance between two latitudes on one meridian.
fn lat_distance_m(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_M * (lat2.to_radians() - lat1.to_radians()).abs()
}

fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let v = ((lon2.to_radians() - lon1.to_radians()) / 2.0).sin();
    // The same meridian: nothing to do but subtract.
    if v == 0.0 {
        return lat_distance_m(lat1, lat2);
    }
    let (lat1, lat2) = (lat1.to_radians(), lat2.to_radians());
    let u = ((lat2 - lat1) / 2.0).sin();
    let a = u * u + lat1.cos() * lat2.cos() * v * v;
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

// ── Unit conversion ───────────────────────────────────────────────────────────

fn to_meters(value: f64, unit: &str) -> Option<f64> {
    match unit.to_ascii_uppercase().as_str() {
        "M" => Some(value),
        "KM" => Some(value * 1000.0),
        // A mile is 1609.344 m; Redis's is 1609.34, and a distance in miles
        // has to come out as Redis's does.
        "MI" => Some(value * 1609.34),
        "FT" => Some(value * 0.3048),
        _ => None,
    }
}

fn from_meters(meters: f64, unit: &str) -> f64 {
    match unit.to_ascii_uppercase().as_str() {
        "M" => meters,
        "KM" => meters / 1000.0,
        "MI" => meters / 1609.34,
        "FT" => meters / 0.3048,
        _ => meters,
    }
}

fn format_dist(meters: f64, unit: &str) -> String {
    format!("{:.4}", from_meters(meters, unit))
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

fn get_or_create_zset(ctx: &mut Ctx, key: &str) -> Result<Arc<RwLock<SortedSetInner>>, Vec<u8>> {
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);

    match map.get(key) {
        Some(ValueRef::SortedSet(zs)) => Ok(Arc::clone(zs)),
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        None => {
            let zs = Arc::new(RwLock::new(SortedSetInner::new()));
            map.insert(Key::from(key), KiviDBValue::SortedSet(Arc::clone(&zs)));
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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

    while i + 2 < cmd.len() {
        let (lon, lat) = match lon_lat(cmd[i], cmd[i + 1]) {
            Ok(position) => position,
            Err(e) => return e,
        };
        let member: ZMember = Arc::from(cmd[i + 2]);
        i += 3;

        let score = encode_geohash(lon, lat);
        let existing = inner.members.get(member.as_ref()).copied();

        match existing {
            None if flag_xx => continue, // XX: only update existing
            None => {
                inner.insert(member, score);
                added += 1;
                changed += 1;
            }
            Some(_old_score) if flag_nx => continue, // NX: only add new
            Some(old_score) => {
                if (old_score - score).abs() > f64::EPSILON {
                    inner.insert(member, score);
                    changed += 1;
                }
            }
        }
    }

    drop(inner);

    // CH flag: return count of added + changed; otherwise just added
    resp_integer(if flag_ch { changed } else { added })
}

// ── GEODIST ───────────────────────────────────────────────────────────────────
// GEODIST key member1 member2 [M|KM|FT|MI]
fn cmd_geodist(ctx: &mut Ctx) -> Vec<u8> {
    let resp = reply_resp(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 4 || cmd.len() > 5 {
        return resp_error("ERR wrong number of arguments for 'geodist' command");
    }

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };
    let unit = if cmd.len() == 5 {
        std::str::from_utf8(cmd[4]).unwrap_or("M")
    } else {
        "M"
    };

    if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
        return resp_null_bulk();
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let zs = match map.get(key) {
        Some(ValueRef::SortedSet(z)) => z.clone(),
        None => return resp_null_bulk(),
        _ => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    drop(map);

    let inner = zs.read();
    let score1 = match inner.members.get(cmd[2]) {
        Some(&s) => s,
        None => return resp_null_bulk(),
    };
    let score2 = match inner.members.get(cmd[3]) {
        Some(&s) => s,
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

    // Redis replies with addReplyHumanLongDouble → the double type under RESP3.
    // Parse the already-formatted, unit-converted string back rather than
    // recomputing, so both protocols report the exact same number. A parse
    // failure is unreachable in practice; falling through to the RESP2 form
    // beats emitting a malformed double if it ever isn't.
    if resp >= 3
        && let Ok(d) = format_dist(dist, unit).parse::<f64>()
    {
        return resp_double_resp3(d);
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
        let nils = vec![resp_null_for(resp); cmd.len() - 2];
        return resp_array(nils);
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let zs = match map.get(key) {
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
        match inner.members.get(*member_bytes) {
            None => results.push(resp_null_for(resp)),
            Some(&score) => {
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

    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return resp_error("ERR key must be valid UTF-8"),
    };

    if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
        expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
        let nils = vec![resp_null_for(resp); cmd.len() - 2];
        return resp_array(nils);
    }

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    let zs = match map.get(key) {
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
        match inner.members.get(*member_bytes) {
            None => results.push(resp_null_for(resp)),
            Some(&score) => {
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

#[derive(Clone, Copy)]
enum Shape {
    Circle { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}

/// An area to search: its centre and its shape.
#[derive(Clone, Copy)]
struct Area {
    lon: f64,
    lat: f64,
    shape: Shape,
}

impl Area {
    /// The distance from the centre to a position, if the position is inside.
    fn contains(&self, lon: f64, lat: f64) -> Option<f64> {
        match self.shape {
            Shape::Circle { radius_m } => {
                let distance = haversine_m(self.lon, self.lat, lon, lat);
                (distance <= radius_m).then_some(distance)
            }
            // Inside a box that runs along the parallels and the meridians:
            // within half its height of the centre's latitude, and — along
            // its own parallel — within half its width of the centre's
            // longitude.
            Shape::Box { width_m, height_m } => {
                if lat_distance_m(lat, self.lat) > height_m / 2.0 {
                    return None;
                }
                if haversine_m(lon, lat, self.lon, lat) > width_m / 2.0 {
                    return None;
                }
                Some(haversine_m(self.lon, self.lat, lon, lat))
            }
        }
    }

    /// The smallest rectangle of longitudes and latitudes that holds the
    /// area: `(lon_min, lat_min, lon_max, lat_max)`. A parallel is shorter
    /// the nearer it is to a pole, so the rectangle is as wide as the area
    /// is at the edge where that is widest.
    fn bounding_box(&self) -> (f64, f64, f64, f64) {
        let (width_m, height_m) = match self.shape {
            Shape::Circle { radius_m } => (radius_m, radius_m),
            Shape::Box { width_m, height_m } => (width_m / 2.0, height_m / 2.0),
        };
        let lat_delta = (height_m / EARTH_RADIUS_M).to_degrees();
        let lon_delta = |lat: f64| (width_m / EARTH_RADIUS_M / lat.to_radians().cos()).to_degrees();
        let lon_delta = if self.lat < 0.0 {
            lon_delta(self.lat - lat_delta)
        } else {
            lon_delta(self.lat + lat_delta)
        };
        (
            self.lon - lon_delta,
            self.lat - lat_delta,
            self.lon + lon_delta,
            self.lat + lat_delta,
        )
    }

    /// The cells to look in: the centre's, then those to its north, south,
    /// east, west, north-east, north-west, south-east and south-west. One
    /// that cannot hold anything inside the area is `Cell::default()`.
    fn cells(&self) -> [Cell; 9] {
        let (lon_min, lat_min, lon_max, lat_max) = self.bounding_box();
        // From the centre to the furthest point of the area.
        let reach_m = match self.shape {
            Shape::Circle { radius_m } => radius_m,
            Shape::Box { width_m, height_m } => {
                ((width_m / 2.0).powi(2) + (height_m / 2.0).powi(2)).sqrt()
            }
        };
        let around = |cell: Cell| {
            [
                cell,
                cell_beside(cell, 0, 1),
                cell_beside(cell, 0, -1),
                cell_beside(cell, 1, 0),
                cell_beside(cell, -1, 0),
                cell_beside(cell, 1, 1),
                cell_beside(cell, -1, 1),
                cell_beside(cell, 1, -1),
                cell_beside(cell, -1, -1),
            ]
        };
        let centre = |step: u8| {
            encode_cell(self.lon, self.lat, step, (GEO_LAT_MIN, GEO_LAT_MAX)).unwrap_or_default()
        };

        let mut step = steps_for(reach_m, self.lat);
        let mut cells = around(centre(step));
        // Near the edge of its cell the centre's neighbours may not reach
        // far enough: cells of twice the size do.
        let ((_, _), (_, north_max)) = cell_bounds(cells[1]);
        let ((_, _), (south_min, _)) = cell_bounds(cells[2]);
        let ((_, east_max), _) = cell_bounds(cells[3]);
        let ((west_min, _), _) = cell_bounds(cells[4]);
        if step > 1
            && (north_max < lat_max
                || south_min > lat_min
                || east_max < lon_max
                || west_min > lon_min)
        {
            step -= 1;
            cells = around(centre(step));
        }
        // And cells the area does not reach into are left out.
        if step >= 2 {
            let ((cell_lon_min, cell_lon_max), (cell_lat_min, cell_lat_max)) =
                cell_bounds(cells[0]);
            let mut drop =
                |which: [usize; 3]| which.iter().for_each(|i| cells[*i] = Cell::default());
            if cell_lat_min < lat_min {
                drop([2, 7, 8]);
            }
            if cell_lat_max > lat_max {
                drop([1, 5, 6]);
            }
            if cell_lon_min < lon_min {
                drop([4, 6, 8]);
            }
            if cell_lon_max > lon_max {
                drop([3, 5, 7]);
            }
        }
        cells
    }
}

/// The size of cell — as the number of bits of each coordinate that name it
/// — at which nine cells cover a distance of `reach_m` around a point at
/// latitude `lat`.
fn steps_for(reach_m: f64, lat: f64) -> u8 {
    const MERCATOR_MAX: f64 = 20037726.37;
    if reach_m == 0.0 {
        return STEP_MAX;
    }
    let mut step = 1i32;
    let mut reach = reach_m;
    while reach < MERCATOR_MAX {
        reach *= 2.0;
        step += 1;
    }
    // So that the area is inside the cells in most cases.
    step -= 2;
    // Cells are narrower towards the poles.
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, STEP_MAX as i32) as u8
}

/// A member found inside the area.
struct Found {
    member: ZMember,
    score: f64,
    distance_m: f64,
    lon: f64,
    lat: f64,
}

/// The members of `zset` inside `area`, in the order the search comes
/// across them; no more than `limit` of them, if there is a limit.
fn members_in(zset: &SortedSetInner, area: &Area, limit: Option<usize>) -> Vec<Found> {
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
        for ((score, member), _) in zset.by_score.range((OrderedFloat(from), first)..) {
            if score.0 >= to {
                break;
            }
            let (lon, lat) = decode_geohash(score.0);
            if let Some(distance_m) = area.contains(lon, lat) {
                found.push(Found {
                    member: member.clone(),
                    score: score.0,
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
    let Some(key) = cmd.get(source_at).and_then(|raw| parse_utf8(raw)) else {
        return resp_error("ERR key must be valid UTF-8");
    };
    let key = key.to_string();

    // The set, if there is one. (Its members are looked at under its lock,
    // which the search takes for as long as it runs.)
    let gone = lazy_expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, &key);
    let zset = {
        let shard = store::get_shard(&ctx.store[*ctx.selected_db], &key);
        let map = read_lock!(shard);
        match map.get(key.as_str()).filter(|_| !gone) {
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
            .and_then(|zset| zset.read().members.get(member).copied())
            .map(decode_geohash)
            .ok_or_else(|| resp_error("ERR could not decode requested zset member"))
    };

    // The centre and the shape, where the form of the command fixes where
    // they are.
    let mut centre: Option<(f64, f64)> = None;
    let mut shape: Option<(Shape, f64)> = None;
    let mut store_key: Option<String> = None;
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
            store_key = parse_utf8(cmd[1]).map(str::to_string);
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
                store_key = parse_utf8(args[i + 1]).map(str::to_string);
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
    let found = match (&zset, centre, shape) {
        (Some(zset), Some((lon, lat)), Some((shape, unit_m))) => {
            let area = Area { lon, lat, shape };
            let mut found = members_in(&zset.read(), &area, if any { count } else { None });
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
        if found.is_empty() {
            crate::commands::helpers::delete_key(ctx.store, ctx.expiry_store, db, &dest);
            return resp_integer(0);
        }
        let mut stored = SortedSetInner::new();
        for point in &found {
            stored.insert(
                point.member.clone(),
                if store_dist {
                    point.distance_m / unit_m
                } else {
                    point.score
                },
            );
        }
        // The destination is a new key: whatever was there, and its time
        // to live, are gone.
        crate::commands::helpers::delete_key(ctx.store, ctx.expiry_store, db, &dest);
        let shard = store::get_shard(&ctx.store[db], &dest);
        write_lock!(shard).insert(
            Key::from(dest),
            KiviDBValue::SortedSet(Arc::new(RwLock::new(stored))),
        );
        return resp_integer(found.len() as i64);
    }

    let plain = !with_dist && !with_hash && !with_coord;
    resp_array(
        found
            .iter()
            .map(|point| {
                if plain {
                    return resp_bulk(&point.member);
                }
                let mut item = vec![resp_bulk(&point.member)];
                if with_dist {
                    // A distance: RESP3 sends it as a double — the same
                    // treatment as GEODIST.
                    let text = format!("{:.4}", point.distance_m / unit_m);
                    match (resp >= 3).then(|| text.parse::<f64>()) {
                        Some(Ok(d)) => item.push(resp_double_resp3(d)),
                        _ => item.push(resp_bulk_str(&text)),
                    }
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
