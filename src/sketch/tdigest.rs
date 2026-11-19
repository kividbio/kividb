//! t-digests (`TDIGEST.*`): quantiles of a stream of numbers in bounded
//! space, as Redis 8 has them built in (formerly RedisBloom).
//!
//! A digest keeps centroids (a mean and a weight each): those merged, in
//! order of their means, and those added since, unmerged. Once there are as
//! many as its capacity (`6 × compression + 10`, Redis's), the unmerged ones
//! are sorted in among the merged and neighbours are merged while the
//! merged centroid stays within the size the scale function allows at its
//! place: `k(q) = compression / 2π · asin(2q − 1)`, a step of at most 1 in
//! `k` for one centroid (Dunning and Ertl's merging digest, "Computing
//! Extremely Accurate Quantiles Using t-Digests", 2019). Small centroids at
//! the tails make the extreme quantiles accurate; a singleton is answered
//! exactly.
//!
//! The answers are approximations within the same bounds as Redis's, not
//! bit for bit the same: the algorithm is the published one, written from
//! the paper, not Redis's (whose source was not opened). Where there is
//! nothing to approximate (few enough values that every centroid is one
//! value), they are the same as Redis's. A query never changes the digest
//! here (Redis merges the unmerged centroids first, and keeps them merged),
//! so that a replica, which sees no queries, keeps the master's digest
//! byte for byte; `TDIGEST.INFO`'s counts of merged and unmerged nodes can
//! differ from Redis's for that.
//!
//! Size limits, which Redis does not have: a compression of at most
//! [`MAX_COMPRESSION`], so a digest holds at most `6 × 100,000 + 10`
//! centroids (some 9.6 MB).

use std::f64::consts::PI;
use std::io;

/// The largest compression a digest may have.
pub const MAX_COMPRESSION: u64 = 100_000;
/// The compression of a digest made without one.
pub const DEFAULT_COMPRESSION: u64 = 100;

/// A centroid: the mean of the values it holds, and how many.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

/// A t-digest.
#[derive(Clone, Debug, PartialEq)]
pub struct TDigest {
    pub compression: u64,
    pub merged: Vec<Centroid>,
    pub unmerged: Vec<Centroid>,
    pub merged_weight: f64,
    pub unmerged_weight: f64,
    pub min: f64,
    pub max: f64,
    pub total_compressions: u64,
    /// The merged view of a digest with unmerged centroids, made by the
    /// first query after a write and dropped by the next write. It is not
    /// part of the digest: never saved, sent or compared.
    pub view_cache: ViewCache,
}

/// A digest's cached merged view (see `TDigest::view`).
#[derive(Clone, Debug, Default)]
pub struct ViewCache(std::sync::OnceLock<Vec<Centroid>>);

impl PartialEq for ViewCache {
    fn eq(&self, _: &ViewCache) -> bool {
        true
    }
}

/// The capacity of a digest of `compression`: Redis's.
pub fn capacity_of(compression: u64) -> usize {
    (6 * compression + 10) as usize
}

/// The bytes Redis reckons a digest of `compression` takes
/// (`TDIGEST.INFO`'s `Memory usage`).
pub fn info_memory(compression: u64) -> u64 {
    capacity_of(compression) as u64 * 16 + 80
}

impl TDigest {
    /// An empty digest; the compression is the caller's to check.
    pub fn new(compression: u64) -> TDigest {
        TDigest {
            compression,
            merged: Vec::new(),
            unmerged: Vec::new(),
            merged_weight: 0.0,
            unmerged_weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            total_compressions: 0,
            view_cache: ViewCache::default(),
        }
    }

    pub fn capacity(&self) -> usize {
        capacity_of(self.compression)
    }

    pub fn total_weight(&self) -> f64 {
        self.merged_weight + self.unmerged_weight
    }

    pub fn is_empty(&self) -> bool {
        self.total_weight() <= 0.0
    }

    /// Empty it, keeping its compression.
    pub fn reset(&mut self) {
        *self = TDigest::new(self.compression);
    }

    /// What it takes in memory beyond the struct.
    pub fn memory_bytes(&self) -> usize {
        let cached = self.view_cache.0.get().map_or(0, Vec::capacity);
        (self.merged.capacity() + self.unmerged.capacity() + cached)
            * std::mem::size_of::<Centroid>()
    }

    /// Add `weight` of `value` (finite, checked by the caller).
    pub fn add(&mut self, value: f64, weight: f64) {
        self.view_cache = ViewCache::default();
        if self.merged.len() + self.unmerged.len() >= self.capacity() {
            self.compress();
        }
        self.unmerged.push(Centroid {
            mean: value,
            weight,
        });
        self.unmerged_weight += weight;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Add another digest's centroids (its min and max too).
    pub fn add_digest(&mut self, other: &TDigest) {
        self.view_cache = ViewCache::default();
        for c in other.merged.iter().chain(other.unmerged.iter()) {
            if self.merged.len() + self.unmerged.len() >= self.capacity() {
                self.compress();
            }
            self.unmerged.push(*c);
            self.unmerged_weight += c.weight;
        }
        if !other.is_empty() {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
    }

    /// The scale function at `q`.
    fn k(&self, q: f64) -> f64 {
        // A compression below 5 sizes centroids as 5 does: below it, the
        // scale function would merge everything into one centroid.
        let delta = self.compression.max(5) as f64;
        delta / (2.0 * PI) * (2.0 * q - 1.0).clamp(-1.0, 1.0).asin()
    }

    /// Merge the unmerged centroids in.
    pub fn compress(&mut self) {
        self.view_cache = ViewCache::default();
        if self.unmerged.is_empty() {
            return;
        }
        let mut all = std::mem::take(&mut self.merged);
        all.append(&mut self.unmerged);
        all.sort_by(|a, b| a.mean.total_cmp(&b.mean));
        let total = self.merged_weight + self.unmerged_weight;
        let mut out: Vec<Centroid> = Vec::with_capacity(all.len().min(self.capacity()));
        let mut so_far = 0.0;
        let mut current = all[0];
        let mut k_lower = self.k(0.0);
        for c in &all[1..] {
            let proposed = current.weight + c.weight;
            let q = (so_far + proposed) / total;
            if self.k(q) - k_lower <= 1.0 {
                // Merged: the weighted mean, kept within its two means.
                let mean = current.mean + (c.mean - current.mean) * c.weight / proposed;
                current.mean = mean.clamp(current.mean.min(c.mean), current.mean.max(c.mean));
                current.weight = proposed;
            } else {
                so_far += current.weight;
                k_lower = self.k(so_far / total);
                out.push(current);
                current = *c;
            }
        }
        out.push(current);
        self.merged = out;
        self.merged_weight = total;
        self.unmerged_weight = 0.0;
        self.total_compressions += 1;
    }

    /// Its centroids, merged, without changing it: if it has unmerged ones,
    /// those of a merged copy, made once and kept until the next write.
    fn view(&self) -> &[Centroid] {
        if self.unmerged.is_empty() {
            return &self.merged;
        }
        self.view_cache.0.get_or_init(|| {
            let mut copy = self.clone();
            copy.compress();
            copy.merged
        })
    }

    /// The value at quantile `q` in [0, 1]; NaN for an empty digest.
    pub fn quantile(&self, q: f64) -> f64 {
        self.quantiles(&[q])[0]
    }

    /// Several quantiles from one view.
    pub fn quantiles(&self, qs: &[f64]) -> Vec<f64> {
        let c = self.view();
        qs.iter().map(|&q| self.quantile_in(c, q)).collect()
    }

    fn quantile_in(&self, c: &[Centroid], q: f64) -> f64 {
        let total = self.total_weight();
        if c.is_empty() || total <= 0.0 {
            return f64::NAN;
        }
        if c.len() == 1 {
            return c[0].mean;
        }
        let n = c.len();
        let index = q * total;
        if index < 1.0 {
            return self.min;
        }
        // The first centroid holds the minimum: between them, interpolate.
        if c[0].weight > 1.0 && index < c[0].weight / 2.0 {
            return self.min + (index - 1.0) / (c[0].weight / 2.0 - 1.0) * (c[0].mean - self.min);
        }
        if index > total - 1.0 {
            return self.max;
        }
        if c[n - 1].weight > 1.0 && total - index <= c[n - 1].weight / 2.0 {
            return self.max
                - (total - index - 1.0) / (c[n - 1].weight / 2.0 - 1.0)
                    * (self.max - c[n - 1].mean);
        }
        // Between the centres of two neighbours; a singleton is a point.
        let mut so_far = c[0].weight / 2.0;
        for i in 0..n - 1 {
            let dw = (c[i].weight + c[i + 1].weight) / 2.0;
            if so_far + dw > index {
                let mut left_unit = 0.0;
                if c[i].weight == 1.0 {
                    if index - so_far < 0.5 {
                        return c[i].mean;
                    }
                    left_unit = 0.5;
                }
                let mut right_unit = 0.0;
                if c[i + 1].weight == 1.0 {
                    if so_far + dw - index <= 0.5 {
                        return c[i + 1].mean;
                    }
                    right_unit = 0.5;
                }
                let z1 = index - so_far - left_unit;
                let z2 = so_far + dw - index - right_unit;
                return weighted(c[i].mean, z2, c[i + 1].mean, z1);
            }
            so_far += dw;
        }
        let z1 = index - total - c[n - 1].weight / 2.0;
        let z2 = c[n - 1].weight / 2.0 - z1;
        weighted(c[n - 1].mean, z1, self.max, z2)
    }

    /// The share of the values at or below `x` (half of those at it); NaN
    /// for an empty digest.
    pub fn cdfs(&self, xs: &[f64]) -> Vec<f64> {
        let c = self.view();
        xs.iter().map(|&x| self.cdf_in(c, x)).collect()
    }

    fn cdf_in(&self, c: &[Centroid], x: f64) -> f64 {
        let total = self.total_weight();
        if c.is_empty() || total <= 0.0 {
            return f64::NAN;
        }
        if x < self.min {
            return 0.0;
        }
        if x > self.max {
            return 1.0;
        }
        let n = c.len();
        if n == 1 {
            if self.max - self.min <= 0.0 {
                return 0.5;
            }
            return (x - self.min) / (self.max - self.min);
        }
        if x < c[0].mean {
            if c[0].mean - self.min > 0.0 {
                if x == self.min {
                    return 0.5 / total;
                }
                return (1.0 + (x - self.min) / (c[0].mean - self.min) * (c[0].weight / 2.0 - 1.0))
                    / total;
            }
            return 0.0;
        }
        let mut so_far = 0.0;
        let mut i = 0;
        while i < n - 1 {
            if c[i].mean == x {
                let mut dw = 0.0;
                while i < n && c[i].mean == x {
                    dw += c[i].weight;
                    i += 1;
                }
                return (so_far + dw / 2.0) / total;
            }
            if c[i].mean <= x && x < c[i + 1].mean {
                if c[i + 1].mean - c[i].mean > 0.0 {
                    let (mut left_out, mut right_out) = (0.0, 0.0);
                    if c[i].weight == 1.0 {
                        if c[i + 1].weight == 1.0 {
                            return (so_far + 1.0) / total;
                        }
                        left_out = 0.5;
                    } else if c[i + 1].weight == 1.0 {
                        right_out = 0.5;
                    }
                    let dw = (c[i].weight + c[i + 1].weight) / 2.0;
                    let base = so_far + c[i].weight / 2.0 + left_out;
                    let span = dw - left_out - right_out;
                    return (base + span * (x - c[i].mean) / (c[i + 1].mean - c[i].mean)) / total;
                }
                let dw = (c[i].weight + c[i + 1].weight) / 2.0;
                return (so_far + dw) / total;
            }
            so_far += c[i].weight;
            i += 1;
        }
        if x == c[n - 1].mean {
            return 1.0 - 0.5 / total;
        }
        if self.max - c[n - 1].mean > 0.0 {
            let dw = c[n - 1].weight / 2.0 - 1.0;
            return 1.0 - (1.0 + (self.max - x) / (self.max - c[n - 1].mean) * dw) / total;
        }
        1.0
    }

    /// The mean of the values between quantiles `lo` and `hi`; NaN for an
    /// empty digest.
    pub fn trimmed_mean(&self, lo: f64, hi: f64) -> f64 {
        let total = self.total_weight();
        let c = self.view();
        if c.is_empty() || total <= 0.0 {
            return f64::NAN;
        }
        // Whole values, as Redis takes them: from the one at `floor(lo · n)`
        // to the one before `ceil(hi · n)`.
        let (from, to) = ((lo * total).floor(), (hi * total).ceil());
        let (mut sum, mut weight, mut so_far) = (0.0, 0.0, 0.0);
        for x in c.iter() {
            let (start, end) = (so_far, so_far + x.weight);
            let overlap = end.min(to) - start.max(from);
            if overlap > 0.0 {
                sum += x.mean * overlap;
                weight += overlap;
            }
            so_far = end;
        }
        if weight <= 0.0 {
            return f64::NAN;
        }
        sum / weight
    }
}

fn weighted(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {
    let (lo, hi) = if x1 <= x2 { (x1, x2) } else { (x2, x1) };
    ((x1 * w1 + x2 * w2) / (w1 + w2)).clamp(lo, hi)
}

// ── KDB encoding ─────────────────────────────────────────────────────────

/// Compression and total compressions (u64), min and max, the merged and
/// unmerged weights (f64), then the merged and the unmerged centroids, each
/// a u32 count and (mean, weight) pairs of f64. Little-endian.
pub fn write_kdb(w: &mut impl io::Write, t: &TDigest) -> io::Result<()> {
    w.write_all(&t.compression.to_le_bytes())?;
    w.write_all(&t.total_compressions.to_le_bytes())?;
    for v in [t.min, t.max, t.merged_weight, t.unmerged_weight] {
        w.write_all(&v.to_le_bytes())?;
    }
    for list in [&t.merged, &t.unmerged] {
        w.write_all(&(list.len() as u32).to_le_bytes())?;
        let mut buf = Vec::with_capacity(list.len().min(4096) * 16);
        for chunk in list.chunks(4096) {
            buf.clear();
            for c in chunk {
                buf.extend_from_slice(&c.mean.to_le_bytes());
                buf.extend_from_slice(&c.weight.to_le_bytes());
            }
            w.write_all(&buf)?;
        }
    }
    Ok(())
}

pub fn kdb_len(t: &TDigest) -> u64 {
    48 + 8 + 16 * (t.merged.len() + t.unmerged.len()) as u64
}

/// A digest [`write_kdb`] wrote: the compression within the limit, the
/// centroids no more than its capacity, every number finite and every
/// weight positive, checked before anything sized by them is allocated.
pub fn read_kdb(r: &mut impl io::Read) -> io::Result<TDigest> {
    let bad = || super::invalid("a t-digest of this shape is not supported");
    let compression = super::read_u64(r)?;
    if !(1..=MAX_COMPRESSION).contains(&compression) {
        return Err(bad());
    }
    let total_compressions = super::read_u64(r)?;
    let mut f = [0f64; 4];
    for v in &mut f {
        *v = f64::from_bits(super::read_u64(r)?);
    }
    let [min, max, merged_weight, unmerged_weight] = f;
    let capacity = capacity_of(compression);
    super::room::declared(capacity as u64 * 16)?;
    let mut lists = [Vec::new(), Vec::new()];
    let mut count = 0usize;
    for list in &mut lists {
        let n = super::read_u32(r)? as usize;
        count += n;
        if count > capacity {
            return Err(bad());
        }
        let raw = super::read_vec(r, n * 16)?;
        list.reserve_exact(n);
        for pair in raw.as_chunks::<16>().0 {
            let mean = f64::from_le_bytes(pair[..8].try_into().expect("8"));
            let weight = f64::from_le_bytes(pair[8..].try_into().expect("8"));
            if !mean.is_finite() || !weight.is_finite() || weight <= 0.0 {
                return Err(bad());
            }
            list.push(Centroid { mean, weight });
        }
    }
    let [merged, unmerged] = lists;
    let sum = |l: &[Centroid]| l.iter().map(|c| c.weight).sum::<f64>();
    let close = |a: f64, b: f64| (a - b).abs() <= 1e-6 * a.abs().max(1.0);
    if !close(sum(&merged), merged_weight) || !close(sum(&unmerged), unmerged_weight) {
        return Err(bad());
    }
    let empty = merged.is_empty() && unmerged.is_empty();
    if !empty && !(min.is_finite() && max.is_finite() && min <= max) {
        return Err(bad());
    }
    if merged.windows(2).any(|w| w[0].mean > w[1].mean) {
        return Err(bad());
    }
    Ok(TDigest {
        compression,
        merged,
        unmerged,
        merged_weight,
        unmerged_weight,
        min: if empty { f64::INFINITY } else { min },
        max: if empty { f64::NEG_INFINITY } else { max },
        total_compressions,
        view_cache: ViewCache::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rng(seed: &mut u64) -> f64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        (*seed >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Exact where every centroid is a value: Redis's answers for 1..10.
    /// A query's cached view is dropped by the next write: answers equal
    /// those of a copy without one, after every kind of write.
    #[test]
    fn view_cache_follows_writes() {
        let mut seed = 11;
        let mut t = TDigest::new(20);
        for i in 0..3000 {
            let v = rng(&mut seed) * 1000.0;
            match i % 5 {
                0 => {
                    let mut other = TDigest::new(30);
                    other.add(v, 2.0);
                    other.add(-v, 1.0);
                    t.add_digest(&other);
                }
                1 => t.compress(),
                _ => t.add(v, 1.0),
            }
            if i % 3 == 0 {
                let mut uncached = t.clone();
                uncached.view_cache = ViewCache::default();
                for q in [0.0, 0.01, 0.5, 0.99, 1.0] {
                    assert_eq!(t.quantile(q).to_bits(), uncached.quantile(q).to_bits());
                    assert_eq!(t.quantile(q).to_bits(), t.quantile(q).to_bits());
                }
                assert_eq!(t.cdfs(&[v])[0].to_bits(), uncached.cdfs(&[v])[0].to_bits());
                assert_eq!(
                    t.trimmed_mean(0.1, 0.9).to_bits(),
                    uncached.trimmed_mean(0.1, 0.9).to_bits()
                );
            }
        }
    }

    #[test]
    fn singletons_are_exact() {
        let mut t = TDigest::new(100);
        for v in 1..=10 {
            t.add(f64::from(v), 1.0);
        }
        assert_eq!(
            t.quantiles(&[0.0, 0.1, 0.5, 0.9, 1.0]),
            [1.0, 2.0, 6.0, 10.0, 10.0]
        );
        assert_eq!(
            t.cdfs(&[0.0, 1.0, 5.5, 10.0, 11.0]),
            [0.0, 0.05, 0.5, 0.95, 1.0]
        );
        assert_eq!(t.trimmed_mean(0.1, 0.9), 5.5);
        assert_eq!(t.trimmed_mean(0.0, 1.0), 5.5);
        // A query does not change the digest.
        assert_eq!(t.unmerged.len(), 10);
    }

    /// Quantile errors of a sorted stream of a million values, and of
    /// shuffled normal, uniform and lognormal ones, within what a
    /// compression of 100 gives (a few thousandths in rank, far less at the
    /// tails).
    #[test]
    fn errors_within_bounds() {
        let mut seed = 0x1234_5678u64;
        for dist in 0..4 {
            let mut values: Vec<f64> = (0..200_000)
                .map(|i| match dist {
                    0 => i as f64,
                    1 => rng(&mut seed),
                    2 => {
                        let (u, v) = (rng(&mut seed).max(1e-300), rng(&mut seed));
                        (-2.0 * u.ln()).sqrt() * (2.0 * PI * v).cos()
                    }
                    _ => {
                        let (u, v) = (rng(&mut seed).max(1e-300), rng(&mut seed));
                        ((-2.0 * u.ln()).sqrt() * (2.0 * PI * v).cos()).exp()
                    }
                })
                .collect();
            let mut t = TDigest::new(100);
            for &v in &values {
                t.add(v, 1.0);
            }
            values.sort_by(f64::total_cmp);
            for q in [0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999] {
                let got = t.quantile(q);
                let rank = values.partition_point(|&v| v < got) as f64 / values.len() as f64;
                let bound = if (0.01..=0.99).contains(&q) {
                    0.005
                } else {
                    0.0015
                };
                assert!((rank - q).abs() < bound, "dist {dist} q {q}: rank {rank}");
            }
            assert!(t.merged.len() + t.unmerged.len() <= t.capacity());
        }
    }

    #[test]
    fn kdb_round_trip_and_corruption() {
        let mut t = TDigest::new(20);
        let mut seed = 9u64;
        for _ in 0..1000 {
            t.add(rng(&mut seed) * 100.0, 1.0);
        }
        let mut good = Vec::new();
        write_kdb(&mut good, &t).unwrap();
        assert_eq!(good.len() as u64, kdb_len(&t));
        assert_eq!(read_kdb(&mut &good[..]).unwrap(), t);
        let mut s = 5u64;
        for _ in 0..4000 {
            let mut bad = good.clone();
            let r = (rng(&mut s) * 1e9) as usize;
            match r % 3 {
                0 => {
                    let at = r % bad.len();
                    bad[at] = (r >> 8) as u8;
                }
                1 => bad.truncate(r % bad.len()),
                _ => {
                    let at = [0usize, 48][r % 2];
                    bad[at..at + 4].copy_from_slice(&(r as u32).to_le_bytes());
                }
            }
            if let Ok(read) = read_kdb(&mut &bad[..]) {
                assert!(read.merged.len() + read.unmerged.len() <= read.capacity());
                assert!(read.compression <= MAX_COMPRESSION);
            }
        }
    }
}
