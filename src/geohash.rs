//! Geohashes, distances and areas on Redis's sphere: what `GEOSEARCH`
//! (`commands::geo`) and an index's GEO fields (`vector::postings`, #1004)
//! search by, shared so that the two agree with Redis, and each other, at
//! every boundary.

// ── Geo constants ─────────────────────────────────────────────────────────────
//
// Redis uses 52 bits of geohash precision (26 bits each for lon and lat).
// The hash is stored as an f64 score. Since f64 has 53 bits of mantissa,
// a 52-bit integer fits exactly — no precision is lost.

pub(crate) const GEO_LAT_MIN: f64 = -85.05112878;
pub(crate) const GEO_LAT_MAX: f64 = 85.05112878;
pub(crate) const GEO_LON_MIN: f64 = -180.0;
pub(crate) const GEO_LON_MAX: f64 = 180.0;

pub(crate) const EARTH_RADIUS_M: f64 = 6372797.560856;

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
pub(crate) struct Cell {
    pub(crate) bits: u64,
    pub(crate) step: u8,
}

pub(crate) const STEP_MAX: u8 = 26;

/// A zero between every two bits of `x`.
pub(crate) fn spread_bits(x: u32) -> u64 {
    let mut x = x as u64;
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
    x = (x | (x << 8)) & 0x00FF00FF00FF00FF;
    x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x << 2)) & 0x3333333333333333;
    x = (x | (x << 1)) & 0x5555555555555555;
    x
}

/// The inverse: every other bit of `x`, squeezed together.
pub(crate) fn compact_bits(x: u64) -> u32 {
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
pub(crate) fn encode_cell(lon: f64, lat: f64, step: u8, lat_range: (f64, f64)) -> Option<Cell> {
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
pub(crate) fn cell_bounds(cell: Cell) -> ((f64, f64), (f64, f64)) {
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
pub(crate) fn encode_geohash(lon: f64, lat: f64) -> f64 {
    encode_cell(lon, lat, STEP_MAX, (GEO_LAT_MIN, GEO_LAT_MAX))
        .map(|cell| cell.bits as f64)
        .unwrap_or(0.0)
}

/// Decode a geohash score back to (longitude, latitude): the middle of the
/// cell the score names.
pub(crate) fn decode_geohash(score: f64) -> (f64, f64) {
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
pub(crate) fn cell_beside(cell: Cell, dx: i8, dy: i8) -> Cell {
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
pub(crate) fn lat_distance_m(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_M * (lat2.to_radians() - lat1.to_radians()).abs()
}

pub(crate) fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
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

pub(crate) fn to_meters(value: f64, unit: &str) -> Option<f64> {
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

pub(crate) fn from_meters(meters: f64, unit: &str) -> f64 {
    match unit.to_ascii_uppercase().as_str() {
        "M" => meters,
        "KM" => meters / 1000.0,
        "MI" => meters / 1609.34,
        "FT" => meters / 0.3048,
        _ => meters,
    }
}

pub(crate) fn format_dist(meters: f64, unit: &str) -> String {
    format!("{:.4}", from_meters(meters, unit))
}

#[derive(Clone, Copy)]
pub(crate) enum Shape {
    Circle { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}

/// An area to search: its centre and its shape.
#[derive(Clone, Copy)]
pub(crate) struct Area {
    pub(crate) lon: f64,
    pub(crate) lat: f64,
    pub(crate) shape: Shape,
}

impl Area {
    /// The distance from the centre to a position, if the position is inside.
    pub(crate) fn contains(&self, lon: f64, lat: f64) -> Option<f64> {
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
    pub(crate) fn bounding_box(&self) -> (f64, f64, f64, f64) {
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
    pub(crate) fn cells(&self) -> [Cell; 9] {
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
pub(crate) fn steps_for(reach_m: f64, lat: f64) -> u8 {
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

// ── A radius around a point, as an index searches it ────────────────────────
//
// What a GEO field's postings read for `@loc:[lon lat radius unit]` and
// `GEOFILTER` (#1004): the same nine cells `GEOSEARCH … BYRADIUS` looks in,
// as ranges of 52-bit geohashes, and a point is in the radius if the centre
// of its geohash's cell is, as `GEOSEARCH` and Redis's query engine judge it.

/// How a radius is searched.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cover {
    /// The ranges of geohashes `[from, to)` to read, at most nine.
    Ranges(Vec<(u64, u64)>),
    /// Every point is inside: the radius is at least half the
    /// circumference, the furthest two points can be apart.
    Everything,
    /// No point is: the centre is further north or south than a geohash
    /// goes, where Redis 8.10's query engine finds nothing, whatever the
    /// radius.
    Nothing,
}

/// What to read for the points within `radius_m` of (`lon`, `lat`). The
/// coordinates are finite and the radius positive (the query refuses
/// anything else).
pub(crate) fn radius_cover(lon: f64, lat: f64, radius_m: f64) -> Cover {
    if !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return Cover::Nothing;
    }
    if radius_m >= std::f64::consts::PI * EARTH_RADIUS_M {
        return Cover::Everything;
    }
    // The same meridian as -180, whose cell's neighbours reach across it.
    // From 180 itself the cell is past the last one, and only the points to
    // the west are found: `GEOSEARCH` finds only those, as Redis's does, but
    // the query engine finds both sides.
    let lon = if lon == GEO_LON_MAX { GEO_LON_MIN } else { lon };
    let area = Area {
        lon,
        lat,
        shape: Shape::Circle { radius_m },
    };
    let mut cells: Vec<Cell> = area
        .cells()
        .into_iter()
        .filter(|cell| *cell != Cell::default())
        .collect();
    // Around an area thousands of kilometres across, neighbours can be the
    // same cell: each is read once.
    cells.sort_by_key(|cell| (cell.step, cell.bits));
    cells.dedup();
    // The centre's cell and its eight neighbours, by construction.
    assert!(cells.len() <= 9, "{} cells for one radius", cells.len());
    Cover::Ranges(
        cells
            .into_iter()
            .map(|cell| {
                let shift = 52 - cell.step as u32 * 2;
                (cell.bits << shift, (cell.bits + 1) << shift)
            })
            .collect(),
    )
}

/// Whether a radius around a centre at `lat` can hold any point: not past
/// the latitudes a geohash covers (see [`Cover::Nothing`]).
pub(crate) fn centre_searchable(lat: f64) -> bool {
    (GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat)
}

/// The 52-bit geohash of a point a GEO field holds, as `GEOADD` scores it.
pub(crate) fn geohash_bits(lon: f64, lat: f64) -> u64 {
    encode_geohash(lon, lat) as u64
}

/// Whether the point whose geohash is `bits` is within `radius_m` of
/// (`lon`, `lat`): the centre of its cell is, as `GEOSEARCH` measures a
/// member.
pub(crate) fn hash_within(bits: u64, lon: f64, lat: f64, radius_m: f64) -> bool {
    let (point_lon, point_lat) = decode_geohash(bits as f64);
    haversine_m(lon, lat, point_lon, point_lat) <= radius_m
}
