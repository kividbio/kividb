//! Count-min sketches (`CMS.*`), laid out as Redis 8 lays them out.
//!
//! A sketch is `depth` rows of `width` 32-bit counters. An item adds to one
//! counter in each row, the one at `murmur2(item, row) mod width`, and its
//! count is the least of them: never below what was added, and above it by
//! at most what other items that share its counters added.
//!
//! The placement is Redis's: established by checking a model against the
//! counts Redis 8.10.2 gives when items share counters (thousands of
//! queries over sketches 7 to 50 counters wide, every count equal), and
//! against the counters its `DUMP` holds. MurmurHash2 is Austin Appleby's
//! public-domain algorithm; using the row as the seed came from general
//! knowledge of how RedisBloom works. No Redis or RedisBloom source code was
//! opened or copied. So a sketch counts exactly as Redis's does, collisions
//! included, and one moved from Redis (an RDB file, `RESTORE`) or to it
//! (`DUMP`) answers the same on either side.
//!
//! What Redis does at the edges, observed with `redis-cli`:
//!
//! * A counter does not go past 2^32 − 1: an addition that would take it
//!   further leaves it there. An item whose count (the least of its
//!   counters) is then 2^32 − 1 is answered with an error. The sketch's
//!   total count takes the increment all the same, and wraps as a 64-bit
//!   number.
//! * `CMS.MERGE` replaces the destination with the weighted sum of the
//!   sources (the destination is one of them only if it is named), and is
//!   refused, changing nothing, if a sum is below 0 or above 2^32 − 1.
//!
//! Size limits, which Redis does not have, checked before anything is
//! allocated whether a sketch is made or loaded: at most [`MAX_COUNTERS`]
//! counters, and at most [`MAX_DEPTH`] rows. A command is held to a
//! per-command amount of work ([`ITEM_BUDGET`] and [`MERGE_BUDGET`]),
//! checked before it starts.
//!
//! The counters are shared between copies of a sketch (a snapshot being
//! written, `COPY`, `DUMP`) until one of them is written to, which then
//! copies them: see [`Counters`].

use std::io;
use std::sync::Arc;

use super::murmur::murmur2_32;

/// The most counters a sketch may have (`width × depth`): 2^27, 512 MiB.
pub const MAX_COUNTERS: u64 = 1 << 27;
/// The most rows a sketch may have: what one item costs is one hash and
/// one counter for each.
pub const MAX_DEPTH: u64 = 1 << 16;
/// The most counters one `CMS.INCRBY` or `CMS.QUERY` may visit (items ×
/// depth). A command over it is refused before it starts: some 35,000
/// items in a sketch of depth 7.
pub const ITEM_BUDGET: u64 = 250_000;
/// The most counters one `CMS.MERGE` may read (sources × counters):
/// 2^23, some 17 ms of work (two sources of 4,194,304 counters, or a
/// thousand of 8,192). A larger merge is refused before it starts.
pub const MERGE_BUDGET: u64 = 1 << 23;

/// Why a sketch could not be made, grown or merged into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmsError {
    /// Over [`MAX_COUNTERS`] or [`MAX_DEPTH`].
    TooLarge,
    /// The system would not allocate it.
    NoMemory,
}

/// A sketch's counters. Copies of a sketch share them, so that copying one
/// takes no time and no memory; the first write to shared counters copies
/// them then ([`Counters::make_mut`]), with an allocation that may fail
/// rather than abort.
#[derive(Clone, Debug, PartialEq)]
pub struct Counters(Arc<Box<[u32]>>);

impl Counters {
    /// `n` zeroed counters, or why not. The allocation is asked for, not
    /// assumed. The limits are the caller's to check first.
    pub fn zeroed(n: u64) -> Result<Counters, CmsError> {
        if n == 0 || n > MAX_COUNTERS {
            return Err(CmsError::TooLarge);
        }
        let v = super::zeroed::<u32>(n as usize).ok_or(CmsError::NoMemory)?;
        Ok(Counters(Arc::new(v.into_boxed_slice())))
    }

    pub fn from_vec(v: Vec<u32>) -> Counters {
        Counters(Arc::new(v.into_boxed_slice()))
    }

    /// The counters to write to: this sketch's own, copied first if another
    /// copy shares them.
    pub fn make_mut(&mut self) -> Result<&mut [u32], CmsError> {
        if Arc::get_mut(&mut self.0).is_none() {
            let mut copy: Vec<u32> = Vec::new();
            copy.try_reserve_exact(self.0.len())
                .map_err(|_| CmsError::NoMemory)?;
            copy.extend_from_slice(&self.0);
            self.0 = Arc::new(copy.into_boxed_slice());
        }
        Ok(Arc::get_mut(&mut self.0).expect("the counters are this copy's alone"))
    }

    /// Whether another copy shares them.
    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }
}

impl std::ops::Deref for Counters {
    type Target = [u32];

    fn deref(&self) -> &[u32] {
        &self.0
    }
}

/// A count-min sketch.
#[derive(Clone, Debug, PartialEq)]
pub struct CountMin {
    pub width: u64,
    pub depth: u64,
    /// What was added in all (`CMS.INFO`'s `count`): wraps, as Redis's does.
    pub count: u64,
    /// Row by row: row `r`'s counters are `[r · width, (r + 1) · width)`.
    pub counters: Counters,
}

/// Whether a sketch of `width × depth` is within the limits.
pub fn fits(width: u64, depth: u64) -> bool {
    width >= 1
        && (1..=MAX_DEPTH).contains(&depth)
        && width.checked_mul(depth).is_some_and(|n| n <= MAX_COUNTERS)
}

/// The width and depth `CMS.INITBYPROB` makes for `error` (the share of
/// the total count an answer may be over by) and `probability` (that it is
/// over by more), as Redis computes them: `ceil(2 / error)` and
/// `ceil(log10(p) / log10(0.5))`, the probability being taken as a 32-bit
/// float first. `None` where Redis says `invalid init arguments`: a width
/// that does not fit 64 bits, or a probability that is 0 as a float.
pub fn dims_for(error: f64, probability: f64) -> Option<(u64, u64)> {
    let width = (2.0 / error).ceil();
    if !(1.0..18_446_744_073_709_551_616.0).contains(&width) {
        return None;
    }
    let p = f64::from(probability as f32);
    if p <= 0.0 {
        return None;
    }
    let depth = (p.log10() / 0.5f64.log10()).ceil();
    if !(depth >= 1.0 && depth.is_finite()) {
        return None;
    }
    Some((width as u64, depth as u64))
}

/// The counter an item takes in `row` of a sketch `width` wide.
#[inline]
fn column(item: &[u8], row: u64, width: u64) -> u64 {
    u64::from(murmur2_32(item, row as u32)) % width
}

/// What `CMS.INCRBY` did to one item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Added {
    /// Its count now.
    Count(u32),
    /// A counter would have reached 2^32 − 1: it is held there.
    Overflow,
}

impl CountMin {
    /// An empty sketch, if it is within the limits and can be had.
    pub fn new(width: u64, depth: u64) -> Result<CountMin, CmsError> {
        if !fits(width, depth) {
            return Err(CmsError::TooLarge);
        }
        Ok(CountMin {
            width,
            depth,
            count: 0,
            counters: Counters::zeroed(width * depth)?,
        })
    }

    /// The bytes a sketch of `width × depth` takes.
    pub fn bytes_for(width: u64, depth: u64) -> u64 {
        width.saturating_mul(depth).saturating_mul(4)
    }

    /// What writing to it may allocate: a copy of its counters, if another
    /// copy shares them.
    pub fn copy_bytes(&self) -> u64 {
        if self.counters.is_shared() {
            self.counters.len() as u64 * 4
        } else {
            0
        }
    }

    /// The item's count: the least of its counters.
    pub fn query(&self, item: &[u8]) -> u32 {
        let mut least = u32::MAX;
        for row in 0..self.depth {
            let at = (row * self.width + column(item, row, self.width)) as usize;
            least = least.min(self.counters[at]);
        }
        least
    }

    /// Add `value` to the item's counters, each held at 2^32 − 1 at most.
    /// An item whose count is then 2^32 − 1 is answered `Overflow`; the
    /// total count takes `value` whatever happens.
    pub fn incr_by(&mut self, item: &[u8], value: u64) -> Result<Added, CmsError> {
        let (width, depth) = (self.width, self.depth);
        let counters = self.counters.make_mut()?;
        self.count = self.count.wrapping_add(value);
        let mut least = u32::MAX;
        for row in 0..depth {
            let at = (row * width + column(item, row, width)) as usize;
            let sum = u64::from(counters[at]).saturating_add(value);
            counters[at] = sum.min(u64::from(u32::MAX)) as u32;
            least = least.min(counters[at]);
        }
        Ok(if least == u32::MAX {
            Added::Overflow
        } else {
            Added::Count(least)
        })
    }

    /// What its counters take in memory beyond the struct.
    pub fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Box<[u32]>>() + 16 + self.counters.len() * 4
    }

    pub fn data_bytes(&self) -> u64 {
        self.counters.len() as u64 * 4
    }
}

/// `CMS.MERGE`'s sums: each counter the weighted sum of the sources' (all
/// of `width × depth`, checked by the caller), and the total count the
/// weighted sum of theirs, each count taken as a signed 64-bit number.
/// `None` if a counter's sum is below 0 or above 2^32 − 1, or the count's
/// below 0 or above 2^63 − 1, as Redis refuses them. The sums are exact
/// (128 bits), so a weight that would wrap a 64-bit product is refused too.
pub fn merged(sources: &[(&CountMin, i64)]) -> Result<Option<(Vec<u32>, u64)>, CmsError> {
    let count: i128 = sources
        .iter()
        .map(|(s, w)| i128::from(s.count as i64) * i128::from(*w))
        .sum();
    if !(0..=i128::from(i64::MAX)).contains(&count) {
        return Ok(None);
    }
    let n = sources.first().map_or(0, |(s, _)| s.counters.len());
    let mut out: Vec<u32> = Vec::new();
    out.try_reserve_exact(n).map_err(|_| CmsError::NoMemory)?;
    for at in 0..n {
        let mut sum: i128 = 0;
        for (s, w) in sources {
            sum += i128::from(s.counters[at]) * i128::from(*w);
        }
        if !(0..=i128::from(u32::MAX)).contains(&sum) {
            return Ok(None);
        }
        out.push(sum as u32);
    }
    Ok(Some((out, count as u64)))
}

// ── KDB encoding ─────────────────────────────────────────────────────────

/// `width`, `depth` and `count` as u64s, then the counters as u32s, all
/// little-endian.
pub fn write_kdb(w: &mut impl io::Write, cms: &CountMin) -> io::Result<()> {
    w.write_all(&cms.width.to_le_bytes())?;
    w.write_all(&cms.depth.to_le_bytes())?;
    w.write_all(&cms.count.to_le_bytes())?;
    let mut buf = Vec::with_capacity(64 * 1024);
    for chunk in cms.counters.chunks(16 * 1024) {
        buf.clear();
        for c in chunk {
            buf.extend_from_slice(&c.to_le_bytes());
        }
        w.write_all(&buf)?;
    }
    Ok(())
}

pub fn kdb_len(cms: &CountMin) -> u64 {
    24 + cms.data_bytes()
}

/// A sketch [`write_kdb`] wrote. The shape is checked against the limits
/// before any counter is read, and the counters are read as they arrive,
/// so that a corrupt length allocates no more than the stream holds.
pub fn read_kdb(r: &mut impl io::Read) -> io::Result<CountMin> {
    let width = super::read_u64(r)?;
    let depth = super::read_u64(r)?;
    let count = super::read_u64(r)?;
    if !fits(width, depth) {
        return Err(super::invalid(
            "a count-min sketch over the size limits is not supported",
        ));
    }
    let bytes = super::read_vec(r, (width * depth * 4) as usize)?;
    Ok(CountMin {
        width,
        depth,
        count,
        counters: counters_from_bytes(&bytes)?,
    })
}

/// Little-endian u32s from `bytes` (a whole number of them), into an
/// allocation that is asked for.
pub fn counters_from_bytes(bytes: &[u8]) -> io::Result<Counters> {
    let mut v: Vec<u32> = Vec::new();
    v.try_reserve_exact(bytes.len() / 4).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "no memory for a count-min sketch",
        )
    })?;
    let (words, _) = bytes.as_chunks::<4>();
    v.extend(words.iter().map(|w| u32::from_le_bytes(*w)));
    Ok(Counters::from_vec(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rng(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    /// Never under what was added; with ample width, exact.
    #[test]
    fn never_undercounts() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut cms = CountMin::new(50, 4).unwrap();
        let mut truth = std::collections::HashMap::new();
        for _ in 0..5_000 {
            let item = format!("i{}", rng(&mut seed) % 300);
            let v = rng(&mut seed) % 10;
            cms.incr_by(item.as_bytes(), v).unwrap();
            *truth.entry(item).or_insert(0u64) += v;
        }
        for (item, n) in &truth {
            assert!(u64::from(cms.query(item.as_bytes())) >= *n, "{item}");
        }
        let mut wide = CountMin::new(100_000, 5).unwrap();
        for (item, n) in &truth {
            wide.incr_by(item.as_bytes(), *n).unwrap();
        }
        for (item, n) in &truth {
            assert_eq!(u64::from(wide.query(item.as_bytes())), *n, "{item}");
        }
    }

    /// The error bound: with width `ceil(e / ε)`, an answer is over by
    /// more than `ε · total` for well under `δ` of the items.
    #[test]
    fn within_the_error_bound() {
        let (width, depth) = dims_for(0.01, 0.01).unwrap();
        assert_eq!((width, depth), (200, 7));
        let mut cms = CountMin::new(width, depth).unwrap();
        let mut seed = 42u64;
        let mut truth = std::collections::HashMap::new();
        let mut total = 0u64;
        for _ in 0..50_000 {
            // Zipf-ish: small ids far more often.
            let r = rng(&mut seed) % 1_000_000;
            let id = 1_000_000 / (r + 1);
            let item = format!("z{id}");
            cms.incr_by(item.as_bytes(), 1).unwrap();
            *truth.entry(item).or_insert(0u64) += 1;
            total += 1;
        }
        let over = truth
            .iter()
            .filter(|(item, n)| {
                u64::from(cms.query(item.as_bytes())) > **n + (0.01 * total as f64) as u64
            })
            .count();
        assert!((over as f64) < 0.01 * truth.len() as f64, "{over}");
    }

    /// A counter another item shares goes past 2^32 − 1 without an error
    /// for this item, as long as this item's count does not get there; it
    /// stays at the top.
    #[test]
    fn shared_counters_hold_at_the_top() {
        let width = 4;
        let col = |item: &[u8], row: u64| column(item, row, width);
        let names: Vec<String> = (0..200).map(|i| format!("i{i}")).collect();
        let (a, b) = names
            .iter()
            .flat_map(|x| names.iter().map(move |y| (x, y)))
            .find(|(x, y)| {
                x != y
                    && col(x.as_bytes(), 0) == col(y.as_bytes(), 0)
                    && col(x.as_bytes(), 1) != col(y.as_bytes(), 1)
            })
            .unwrap();
        let mut cms = CountMin::new(width, 2).unwrap();
        cms.incr_by(b.as_bytes(), 10).unwrap();
        assert_eq!(
            cms.incr_by(a.as_bytes(), 4_294_967_290).unwrap(),
            Added::Count(4_294_967_290)
        );
        assert_eq!(cms.query(b.as_bytes()), 10);
        assert_eq!(cms.count, 4_294_967_300);
    }

    #[test]
    fn overflow_holds_at_the_top() {
        let mut cms = CountMin::new(1, 1).unwrap();
        assert_eq!(
            cms.incr_by(b"x", 4_294_967_290).unwrap(),
            Added::Count(4_294_967_290)
        );
        assert_eq!(cms.incr_by(b"x", 4).unwrap(), Added::Count(4_294_967_294));
        assert_eq!(cms.incr_by(b"x", 1).unwrap(), Added::Overflow);
        assert_eq!(cms.query(b"x"), u32::MAX);
        assert_eq!(cms.incr_by(b"x", 0).unwrap(), Added::Overflow);
        assert_eq!(cms.count, 4_294_967_295);
        let mut fresh = CountMin::new(1, 1).unwrap();
        assert_eq!(
            fresh.incr_by(b"x", u64::from(u32::MAX)).unwrap(),
            Added::Overflow
        );
        fresh.incr_by(b"x", i64::MAX as u64).unwrap();
        fresh.incr_by(b"x", i64::MAX as u64).unwrap();
        // Wrapped, as Redis's does.
        assert!(fresh.count < u64::from(u32::MAX) * 2);
    }

    #[test]
    fn merge_sums_and_refuses_out_of_range() {
        let mut x = CountMin::new(1, 1).unwrap();
        let mut y = CountMin::new(1, 1).unwrap();
        x.incr_by(b"i", 5).unwrap();
        y.incr_by(b"i", 3).unwrap();
        let (c, n) = merged(&[(&x, 2), (&y, -1)]).unwrap().unwrap();
        assert_eq!((c[0], n), (7, 7));
        assert!(merged(&[(&x, -1), (&y, 1)]).unwrap().is_none());
        assert_eq!(merged(&[(&y, -1), (&x, 1)]).unwrap().unwrap().0[0], 2);
        assert_eq!(
            merged(&[(&x, 858_993_459)]).unwrap().unwrap().0[0],
            u32::MAX
        );
        assert!(merged(&[(&x, 858_993_460)]).unwrap().is_none());
        // A product that would wrap 64 bits to a small number is refused.
        let mut four = CountMin::new(1, 1).unwrap();
        four.incr_by(b"i", 4).unwrap();
        assert!(
            merged(&[(&four, 4_611_686_018_427_387_905)])
                .unwrap()
                .is_none()
        );
        // The count: a signed 64-bit number, which must stay in range.
        let mut big = CountMin::new(1, 1).unwrap();
        big.incr_by(b"i", i64::MAX as u64).unwrap();
        assert!(merged(&[(&big, 1)]).unwrap().is_some());
        assert!(merged(&[(&big, 2)]).unwrap().is_none());
        big.incr_by(b"j", i64::MAX as u64).unwrap();
        assert!(merged(&[(&big, 1)]).unwrap().is_none());
    }

    /// The dimensions `CMS.INITBYPROB` gives, as Redis 8.10.2 gave them.
    #[test]
    fn dims_as_redis_gives_them() {
        for (e, p, want) in [
            (0.01, 0.01, (200, 7)),
            (0.001, 0.002, (2000, 9)),
            (0.5, 0.5, (4, 1)),
            (0.0000001, 0.0000001, (20_000_000, 24)),
            (0.3, 0.9999, (7, 1)),
            (0.003, 0.05, (667, 5)),
            (0.0007, 0.123, (2858, 4)),
            (0.0625, 0.5, (32, 1)),
            (0.5, 1e-20, (4, 67)),
            (0.5, 1e-45, (4, 149)),
            (4.6e-10, 0.5, (4_347_826_087, 1)),
        ] {
            assert_eq!(dims_for(e, p), Some(want), "{e} {p}");
        }
        assert_eq!(dims_for(0.5, 1e-46), None);
        assert_eq!(dims_for(1e-19, 0.5), None);
        assert!(dims_for(1e-18, 0.5).is_some());
    }

    #[test]
    fn limits_before_allocating() {
        assert_eq!(CountMin::new(MAX_COUNTERS + 1, 1), Err(CmsError::TooLarge));
        assert_eq!(CountMin::new(1, MAX_DEPTH + 1), Err(CmsError::TooLarge));
        assert_eq!(CountMin::new(u64::MAX, u64::MAX), Err(CmsError::TooLarge));
        assert_eq!(CountMin::new(0, 1), Err(CmsError::TooLarge));
        assert!(CountMin::new(1, MAX_DEPTH).is_ok());
    }

    #[test]
    fn copies_share_counters_until_written() {
        let mut a = CountMin::new(10, 2).unwrap();
        a.incr_by(b"x", 3).unwrap();
        let b = a.clone();
        assert!(a.counters.is_shared());
        assert_eq!(a.copy_bytes(), 80);
        a.incr_by(b"x", 1).unwrap();
        assert!(!a.counters.is_shared());
        assert_eq!(b.query(b"x"), 3);
        assert_eq!(a.query(b"x"), 4);
    }

    #[test]
    fn kdb_round_trip() {
        let mut a = CountMin::new(33, 3).unwrap();
        for i in 0..100 {
            a.incr_by(format!("{i}").as_bytes(), i).unwrap();
        }
        let mut out = Vec::new();
        write_kdb(&mut out, &a).unwrap();
        assert_eq!(out.len() as u64, kdb_len(&a));
        assert_eq!(read_kdb(&mut &out[..]).unwrap(), a);
    }

    /// Corrupted encodings are refused or read as some sketch within the
    /// limits; never a panic, and never an allocation the stream does not
    /// back.
    #[test]
    fn corrupt_kdb_is_refused_cleanly() {
        let mut a = CountMin::new(16, 4).unwrap();
        for i in 0..50 {
            a.incr_by(format!("{i}").as_bytes(), i).unwrap();
        }
        let mut good = Vec::new();
        write_kdb(&mut good, &a).unwrap();
        let mut seed = 7u64;
        for _ in 0..4000 {
            let mut bad = good.clone();
            match rng(&mut seed) % 3 {
                0 => {
                    let at = (rng(&mut seed) as usize) % bad.len();
                    bad[at] = rng(&mut seed) as u8;
                }
                1 => bad.truncate((rng(&mut seed) as usize) % bad.len()),
                _ => {
                    // A huge width or depth.
                    let at = (rng(&mut seed) as usize % 2) * 8;
                    bad[at..at + 8].copy_from_slice(&rng(&mut seed).to_le_bytes());
                }
            }
            if let Ok(read) = read_kdb(&mut &bad[..]) {
                assert!(fits(read.width, read.depth));
                assert_eq!(read.counters.len() as u64, read.width * read.depth);
            }
        }
    }
}
