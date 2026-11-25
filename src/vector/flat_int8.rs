//! The int8 copy of a FLAT index's vectors (#1033): a first pass that skips
//! a candidate only when a lower bound on the distance `to_raw` returns
//! shows it cannot enter the nearest `k`, so a scan returns exactly what the
//! exact scan returns, ties and order included.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

use super::{DistanceMetric, simd};

pub(crate) const HEADER: usize = 16;

pub(crate) const DIM_MIN: usize = 256;

pub(crate) const BUILD_AT: usize = 1024;

pub(crate) const CANDIDATES_MIN: usize = 1024;

pub(crate) const K_SHARE: usize = 8;

pub(crate) const CUT_EVERY: usize = 1024;

pub(crate) const CUT_SHARE: usize = 2;

const MOST_DIM: usize = 1 << 16;
const NORM_MIN_SQ: f64 = 1.0 / (1u128 << 120) as f64;
const NORM_MAX_SQ: f64 = (1u128 << 120) as f64;
const U: f64 = 1.0 / (1u64 << 24) as f64;
const ETA: f64 = 1.0 / (1u64 << 30) as f64;
const EPSILON: f64 = 1.0 / (1u64 << 48) as f64;
const SUBNORMAL: f64 = 1.401_298_464_324_817e-45;

static ENABLED: AtomicBool = AtomicBool::new(false);
static SET: AtomicU64 = AtomicU64::new(0);

pub fn set_enabled(on: bool) {
    ENABLED.store(on, Relaxed);
    SET.fetch_add(1, Relaxed);
}

pub(crate) fn times_set() -> u64 {
    SET.load(Relaxed)
}

pub fn enabled() -> bool {
    ENABLED.load(Relaxed)
}

pub(crate) fn supported() -> bool {
    cfg!(target_arch = "x86_64")
}

fn gamma(n: usize) -> f64 {
    let nu = n as f64 * U;
    nu / (1.0 - nu)
}

fn rel(n: usize) -> f64 {
    (n as f64 + 8.0) / (1u64 << 52) as f64
}

fn f32_up(v: f64) -> f32 {
    let f = v as f32;
    if f64::from(f) < v { f.next_up() } else { f }
}

fn f32_down(v: f64) -> f32 {
    let f = v as f32;
    if f64::from(f) > v { f.next_down() } else { f }
}

fn guarded(v: &[f32]) -> Option<f64> {
    if v.len() > MOST_DIM || !v.iter().all(|a| a.is_finite()) {
        return None;
    }
    let sq: f64 = v.iter().map(|&a| f64::from(a) * f64::from(a)).sum();
    (NORM_MIN_SQ..=NORM_MAX_SQ).contains(&sq).then_some(sq)
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn fwht(v: &mut [f64]) {
    let b = v.len();
    let mut h = 1;
    while h < b {
        for i in (0..b).step_by(2 * h) {
            for j in i..i + h {
                let (x, y) = (v[j], v[j + h]);
                v[j] = x + y;
                v[j + h] = x - y;
            }
        }
        h *= 2;
    }
    let scale = 1.0 / (b as f64).sqrt();
    for x in v {
        *x *= scale;
    }
}

#[derive(Debug)]
pub(crate) struct Rotation {
    a: Box<[f64]>,
    p: Box<[u32]>,
    b: Box<[f64]>,
    block: Option<usize>,
    m: usize,
}

impl Rotation {
    pub(crate) fn new(n: usize) -> Self {
        let mut s = (0x1033_5eed ^ n as u64) | 1;
        let signs = |s: &mut u64| -> Box<[f64]> {
            (0..n)
                .map(|_| if xorshift(s) & 1 == 0 { 1.0 } else { -1.0 })
                .collect()
        };
        let a = signs(&mut s);
        let mut p: Vec<u32> = (0..n as u32).collect();
        for i in (1..n).rev() {
            let j = (xorshift(&mut s) % (i as u64 + 1)) as usize;
            p.swap(i, j);
        }
        let b = signs(&mut s);
        let divisor = 1usize << n.trailing_zeros();
        let block = (divisor >= 256 || divisor == n).then_some(divisor);
        let m = 1usize << (usize::BITS - 1 - n.leading_zeros());
        Self {
            a,
            p: p.into_boxed_slice(),
            b,
            block,
            m,
        }
    }

    pub(crate) fn apply(&self, v: &[f32], out: &mut Vec<f64>) {
        out.clear();
        out.extend(
            self.p
                .iter()
                .map(|&i| f64::from(v[i as usize]) * self.a[i as usize]),
        );
        if let Some(block) = self.block {
            for chunk in out.chunks_mut(block) {
                fwht(chunk);
            }
            return;
        }
        let n = out.len();
        fwht(&mut out[..self.m]);
        for (x, sb) in out.iter_mut().zip(&self.b) {
            *x *= sb;
        }
        fwht(&mut out[n - self.m..]);
    }

    #[cfg(test)]
    pub(crate) fn apply_inverse(&self, v: &mut [f64]) {
        let n = v.len();
        if let Some(block) = self.block {
            for chunk in v.chunks_mut(block) {
                fwht(chunk);
            }
        } else {
            fwht(&mut v[n - self.m..]);
            for (x, sb) in v.iter_mut().zip(&self.b) {
                *x *= sb;
            }
            fwht(&mut v[..self.m]);
        }
        let mut out = vec![0.0; n];
        for (k, &i) in self.p.iter().enumerate() {
            out[i as usize] = v[k] * self.a[i as usize];
        }
        v.copy_from_slice(&out);
    }
}

fn quantize(v: &[f64], codes: &mut [i8]) -> (f32, f64, f64) {
    let max = v.iter().fold(0.0f64, |m, &x| m.max(x.abs()));
    let s = (max / 127.0) as f32;
    let sd = f64::from(s);
    let (mut sq, mut ee) = (0.0f64, 0.0f64);
    for (c, &x) in codes.iter_mut().zip(v) {
        // Clamped: `s` rounded to f32 can put `x / s` past 127, and `e` is
        // taken from the clamped `q`.
        let q = if sd > 0.0 {
            (x / sd).round().clamp(-127.0, 127.0)
        } else {
            0.0
        };
        *c = q as i8;
        let e = x - sd * q;
        sq += (sd * q) * (sd * q);
        ee += e * e;
    }
    let r = rel(v.len());
    (s, sq.sqrt() * (1.0 + r), ee.sqrt() * (1.0 + r))
}

#[derive(Debug)]
pub(crate) struct Codec {
    n: usize,
    rotation: Rotation,
}

impl Codec {
    pub(crate) fn new(n: usize) -> Self {
        Self {
            n,
            rotation: Rotation::new(n),
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        HEADER + self.n
    }

    pub(crate) fn encode(&self, x: &[f32], scratch: &mut Vec<f64>, out: &mut [u8]) {
        let n = self.n;
        out.fill(0);
        let Some(x_sq) = guarded(x).filter(|_| x.len() == n) else {
            return;
        };
        self.rotation.apply(x, scratch);
        let (header, codes) = out.split_at_mut(HEADER);
        // SAFETY: i8 and u8 have the same size and alignment.
        let codes = unsafe { &mut *(std::ptr::from_mut::<[u8]>(&mut codes[..n]) as *mut [i8]) };
        let (s, sq, e) = quantize(scratch, codes);
        let x_lo = f32_down(x_sq * (1.0 - rel(n)));
        for (at, v) in [s, f32_up(sq), f32_up(e), x_lo].into_iter().enumerate() {
            header[at * 4..at * 4 + 4].copy_from_slice(&v.to_le_bytes());
        }
    }

    pub(crate) fn query(&self, metric: &DistanceMetric, y: &[f32]) -> Option<Query> {
        let n = self.n;
        if y.len() != n {
            return None;
        }
        let y_sq = guarded(y)?;
        let mut z = Vec::with_capacity(n);
        self.rotation.apply(y, &mut z);
        let mut p = vec![0i8; n];
        let (t, _, f_norm) = quantize(&z, &mut p);
        let z_norm = z.iter().map(|a| a * a).sum::<f64>().sqrt() * (1.0 + rel(n));
        Some(Query {
            metric: metric.clone(),
            n,
            p,
            t: f64::from(t),
            f_norm,
            z_norm,
            y_norm: y_sq.sqrt() * (1.0 + rel(n)),
            y_lo: y_sq * (1.0 - rel(n)),
        })
    }
}

pub(crate) struct Query {
    metric: DistanceMetric,
    n: usize,
    p: Vec<i8>,
    t: f64,
    f_norm: f64,
    z_norm: f64,
    y_norm: f64,
    y_lo: f64,
}

impl Query {
    #[inline]
    pub(crate) fn lower_bound(&self, copy: &[u8]) -> Option<f64> {
        let field = |at: usize| {
            f64::from(f32::from_le_bytes([
                copy[at],
                copy[at + 1],
                copy[at + 2],
                copy[at + 3],
            ]))
        };
        let x_lo = field(12);
        if x_lo == 0.0 {
            return None;
        }
        let codes = &copy[HEADER..HEADER + self.n];
        // SAFETY: i8 and u8 have the same size and alignment.
        let codes = unsafe { &*(std::ptr::from_ref::<[u8]>(codes) as *const [i8]) };
        let qp = simd::dot_i8(codes, &self.p) as f64;
        Some(self.bound(field(0), field(4), field(8), x_lo, qp))
    }

    fn bound(&self, s: f64, sq_norm: f64, e_norm: f64, x_lo: f64, qp: f64) -> f64 {
        let n = self.n;
        let gamma_n = gamma(n);
        let sub = n as f64 * SUBNORMAL;
        let x_norm_lo = x_lo.sqrt() * (1.0 - rel(n));
        let x_norm_hi = (x_lo * (1.0 + 4.0 * U)).sqrt() * (1.0 + rel(n));
        let y_norm = self.y_norm;
        let a = s * self.t * qp;
        let e = sq_norm * self.f_norm + e_norm * self.z_norm + ETA * x_norm_hi * y_norm;
        let d_hi = a + e;
        match self.metric {
            DistanceMetric::InnerProduct => {
                let tau = gamma_n * x_norm_hi * y_norm + sub;
                let m = 1.0 + a.abs() + e + tau;
                1.0 - d_hi - tau - (U + EPSILON) * m
            }
            DistanceMetric::L2 => {
                let gamma_n2 = gamma(n + 2);
                let y_lo = self.y_lo;
                let arith = EPSILON * (x_lo + y_lo + 2.0 * (a.abs() + e));
                (x_lo + y_lo - 2.0 * d_hi - arith).max(0.0) * (1.0 - gamma_n2) - sub
            }
            DistanceMetric::Cosine => {
                let rho = (a.abs() + e) / x_norm_lo;
                let alpha = gamma_n + sub / x_lo;
                let theta = (alpha + alpha * alpha) / 2.0 + 2.0 * U;
                let sigma = (gamma_n * y_norm + sub / x_norm_lo + rho * theta) / (1.0 - theta);
                let sigma_prime = sigma + U * (rho + sigma);
                let r_hi = if d_hi >= 0.0 {
                    d_hi / x_norm_lo
                } else {
                    d_hi / x_norm_hi
                };
                1.0 - r_hi - sigma_prime - (U + EPSILON) * (1.0 + rho + sigma_prime)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::PreparedQuery;

    fn uniform(s: &mut u64) -> f64 {
        (xorshift(s) >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
    }

    fn to_raw_at(level: simd::Level, metric: &DistanceMetric, y: &[f32], x: &[f32]) -> f32 {
        match metric {
            DistanceMetric::Cosine => {
                let (dot, _, nb) = simd::dot_norms_at(level, y, x);
                if nb == 0.0 {
                    1.0
                } else {
                    1.0 - dot / nb.sqrt()
                }
            }
            DistanceMetric::L2 => simd::l2_sq_at(level, y, x),
            DistanceMetric::InnerProduct => 1.0 - simd::dot_at(level, y, x),
        }
    }

    #[test]
    fn the_rotation_is_orthogonal_and_spreads_a_coordinate() {
        for n in [7usize, 96, 100, 512, 768, 1000, 1024, 1536, 2048] {
            let r = Rotation::new(n);
            let mut worst = 0.0f64;
            for i in (0..n).step_by((n / 64).max(1)) {
                let mut x = vec![0.0f32; n];
                x[i] = 1.0;
                let mut v = Vec::new();
                r.apply(&x, &mut v);
                let norm: f64 = v.iter().map(|a| a * a).sum();
                assert!((norm - 1.0).abs() < 1e-12, "{n}: norm {norm}");
                worst = worst.max(v.iter().fold(0.0f64, |m, a| m.max(a.abs())));
                r.apply_inverse(&mut v);
                for (j, a) in v.iter().enumerate() {
                    let want = if j == i { 1.0 } else { 0.0 };
                    assert!((a - want).abs() < 1e-12);
                }
            }
            if n >= 96 {
                assert!(worst <= 2.5 / (n as f64).sqrt(), "{n}: {worst}");
            }
        }
    }

    pub(crate) fn adversarial(n: usize, count: usize, seed: u64) -> (Vec<Vec<f32>>, Vec<Vec<f32>>) {
        let mut s = seed | 1;
        let protos: Vec<Vec<f64>> = (0..20)
            .map(|_| (0..n).map(|_| uniform(&mut s)).collect())
            .collect();
        let edges = [2f64.powi(-61), 2f64.powi(-59), 2f64.powi(59), 2f64.powi(61)];
        let norm = |v: &[f64]| v.iter().map(|a| a * a).sum::<f64>().sqrt();
        let rotation = Rotation::new(n);
        let grids: Vec<Vec<f64>> = (0..5)
            .map(|g| {
                let mut q: Vec<f64> = (0..n).map(|_| (uniform(&mut s) * 127.0).round()).collect();
                q[g % n] = 127.0;
                q
            })
            .collect();
        let on_grid = |q: &[f64], scale: f64| {
            let mut v: Vec<f64> = q.iter().map(|&c| c * scale).collect();
            rotation.apply_inverse(&mut v);
            v
        };
        let mut base = Vec::with_capacity(count);
        for i in 0..count {
            let p = &protos[i % protos.len()];
            let kind = i % 10;
            let row: Vec<f64> = match kind {
                1 => p.iter().map(|a| a * 1e-3).collect(),
                2 => p.iter().map(|a| a * 1e3).collect(),
                3 => p.iter().map(|a| a + uniform(&mut s) * 1e-6).collect(),
                4 => p.iter().map(|a| a + uniform(&mut s) * 1e-2).collect(),
                5 => p
                    .iter()
                    .enumerate()
                    .map(|(j, &a)| if j == 0 { a * 100.0 } else { a })
                    .collect(),
                6 => {
                    let up = uniform(&mut s) > 0.0;
                    p.iter()
                        .enumerate()
                        .map(|(j, &a)| {
                            let f = a as f32;
                            let f = match (j == i % n, up) {
                                (true, true) => f.next_up(),
                                (true, false) => f.next_down(),
                                _ => f,
                            };
                            f64::from(f)
                        })
                        .collect()
                }
                7 => {
                    let k = edges[(i / 10) % edges.len()] / norm(p);
                    p.iter().map(|a| a * k).collect()
                }
                8 => {
                    let g = &grids[(i / 130) % grids.len()];
                    let step = ((i / 10) % 13) as f64 * gamma(n) / 4.0;
                    let scale = [1.0 / 127.0, 0.01, 0.37][(i / 650) % 3] * (1.0 + step);
                    on_grid(g, scale)
                }
                _ => p.iter().map(|a| a + uniform(&mut s) * 0.3).collect(),
            };
            let mut row: Vec<f32> = row.iter().map(|&a| a as f32).collect();
            if kind == 8 && (i / 10) % 2 == 1 {
                let j = i % n;
                row[j] = if uniform(&mut s) > 0.0 {
                    row[j].next_up()
                } else {
                    row[j].next_down()
                };
            }
            base.push(row);
        }
        base[3].fill(0.0);
        base[5][0] = f32::NAN;
        base[6][1] = f32::INFINITY;
        let mut queries: Vec<Vec<f32>> =
            (0..6).map(|q| base[(q * 7 + 3) % count].clone()).collect();
        for q in 6..10 {
            let p = &protos[q % protos.len()];
            queries.push(
                p.iter()
                    .map(|&a| (a + uniform(&mut s) * 1e-4) as f32)
                    .collect(),
            );
        }
        for g in grids.iter().take(3) {
            queries.push(on_grid(g, 0.01).iter().map(|&a| a as f32).collect());
        }
        for &target in &edges {
            let k = target / norm(&protos[1]);
            queries.push(protos[1].iter().map(|a| (a * k) as f32).collect());
        }
        let mut nan: Vec<f32> = protos[2].iter().map(|&a| a as f32).collect();
        nan[0] = f32::NAN;
        queries.push(nan);
        let mut inf: Vec<f32> = protos[3].iter().map(|&a| a as f32).collect();
        inf[n - 1] = f32::NEG_INFINITY;
        queries.push(inf);
        (base, queries)
    }

    #[test]
    fn every_bound_is_at_most_what_every_kernel_level_returns() {
        let levels = simd::available();
        let mut checked = 0usize;
        let mut near = 0usize;
        for (n, count) in [
            (7usize, 700usize),
            (96, 1400),
            (100, 1400),
            (512, 1400),
            (768, 1400),
            (1000, 1400),
            (1024, 1400),
            (1536, 1400),
            (2048, 700),
        ] {
            let codec = Codec::new(n);
            let (base, queries) = adversarial(n, count, 0x1033 + n as u64);
            let mut scratch = Vec::new();
            let copies: Vec<Vec<u8>> = base
                .iter()
                .map(|x| {
                    let mut out = vec![0u8; codec.bytes()];
                    codec.encode(x, &mut scratch, &mut out);
                    out
                })
                .collect();
            for metric in [
                DistanceMetric::L2,
                DistanceMetric::InnerProduct,
                DistanceMetric::Cosine,
            ] {
                for y in &queries {
                    let prepared = PreparedQuery::new(&metric, y);
                    let Some(query) = codec.query(&metric, prepared.vector()) else {
                        continue;
                    };
                    let mut exact = Vec::with_capacity(count);
                    let mut bounds = Vec::with_capacity(count);
                    for (x, copy) in base.iter().zip(&copies) {
                        let raw = prepared.to_raw(x);
                        exact.push(raw);
                        let lb = query.lower_bound(copy);
                        bounds.push(lb);
                        let Some(lb) = lb else { continue };
                        for &level in &levels {
                            let t = f64::from(to_raw_at(level, &metric, prepared.vector(), x));
                            assert!(lb <= t, "{n}-d {metric:?} {level:?}: {lb:e} > {t:e}");
                            checked += 1;
                        }
                        assert!(lb <= f64::from(raw));
                    }
                    for k in [1usize, 10, 100] {
                        let mut best = crate::commands::vector::Best::new(k);
                        for (i, (&raw, lb)) in exact.iter().zip(&bounds).enumerate() {
                            if let (Some(kth), Some(lb)) = (best.kth(), lb)
                                && *lb >= f64::from(kth)
                            {
                                if *lb - f64::from(kth) < gamma(n) * (1.0 + f64::from(kth).abs()) {
                                    near += 1;
                                }
                                continue;
                            }
                            best.offer(raw, &i.to_string());
                        }
                        let mut all = crate::commands::vector::Best::new(k);
                        for (i, &raw) in exact.iter().enumerate() {
                            all.offer(raw, &i.to_string());
                        }
                        let pairs = |b: crate::commands::vector::Best| {
                            b.into_sorted()
                                .into_iter()
                                .map(|h| (h.key, h.score.to_bits()))
                                .collect::<Vec<_>>()
                        };
                        assert_eq!(pairs(best), pairs(all), "{n}-d {metric:?} k {k}");
                    }
                }
            }
        }
        assert!(checked > 100_000, "{checked}");
        assert!(near > 100, "near-slack skips {near}");
    }

    #[test]
    fn vectors_outside_the_guard_have_no_copy() {
        let codec = Codec::new(300);
        let mut scratch = Vec::new();
        let mut out = vec![1u8; codec.bytes()];
        for x in [
            vec![0.0f32; 300],
            vec![f32::NAN; 300],
            vec![2f32.powi(-70); 300],
            vec![2f32.powi(62); 300],
        ] {
            codec.encode(&x, &mut scratch, &mut out);
            assert!(out.iter().all(|&b| b == 0));
        }
        let y = vec![1.0f32; 300];
        let query = codec.query(&DistanceMetric::L2, &y).unwrap();
        assert!(query.lower_bound(&out).is_none());
        let mut inf = y.clone();
        inf[7] = f32::INFINITY;
        assert!(codec.query(&DistanceMetric::L2, &inf).is_none());
    }
}
