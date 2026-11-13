//! Vector-set storage: raw f32, or Redis-compatible Q8 and BIN quantization.

use super::DistanceMetric;

/// Set-wide quantization, fixed when the set is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quant {
    /// Raw floats, 4 bytes per component (`NOQUANT`).
    F32,
    /// 1 byte per component, plus norm and range (`Q8`).
    Q8,
    /// 1 bit per component, plus norm (`BIN`).
    Bin,
}

impl Quant {
    /// Name used by Redis in `VINFO` and `VEMB RAW`.
    pub fn name(self) -> &'static str {
        match self {
            Quant::F32 => "f32",
            Quant::Q8 => "int8",
            Quant::Bin => "bin",
        }
    }

    /// Matching `VADD` option.
    pub fn option(self) -> &'static str {
        match self {
            Quant::F32 => "NOQUANT",
            Quant::Q8 => "Q8",
            Quant::Bin => "BIN",
        }
    }

    pub fn parse_option(tok: &[u8]) -> Option<Quant> {
        if tok.eq_ignore_ascii_case(b"NOQUANT") {
            Some(Quant::F32)
        } else if tok.eq_ignore_ascii_case(b"Q8") {
            Some(Quant::Q8)
        } else if tok.eq_ignore_ascii_case(b"BIN") {
            Some(Quant::Bin)
        } else {
            None
        }
    }

    /// Snapshot encoding.
    pub fn to_byte(self) -> u8 {
        match self {
            Quant::F32 => 0,
            Quant::Q8 => 1,
            Quant::Bin => 2,
        }
    }

    pub fn from_byte(b: u8) -> Option<Quant> {
        match b {
            0 => Some(Quant::F32),
            1 => Some(Quant::Q8),
            2 => Some(Quant::Bin),
            _ => None,
        }
    }
}

/// Number of u64 words holding `dim` sign bits.
#[inline]
pub fn bin_words(dim: usize) -> usize {
    dim.div_ceil(64)
}

/// One element's vector, encoded with the set's quantization.
#[derive(Clone, Debug, PartialEq)]
pub enum StoredVector {
    /// Unnormalized input, so `VEMB` returns it exactly.
    F32(Box<[f32]>),
    Q8 {
        norm: f32,
        range: f32,
        values: Box<[i8]>,
    },
    Bin {
        norm: f32,
        bits: Box<[u64]>,
    },
}

/// Reason a vector cannot be quantized.
#[derive(Debug, PartialEq, Eq)]
pub enum QuantError {
    /// Non-finite component or norm leaves no direction to store.
    NotFinite,
}

/// Summed in f64 so large finite components cannot overflow the norm.
fn norm_of(v: &[f32]) -> f64 {
    v.iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt()
}

/// Normalizes in place; a zero vector gets norm 1, like Redis.
fn to_unit(v: &mut [f32]) -> Result<f32, QuantError> {
    if v.iter().any(|x| !x.is_finite()) {
        return Err(QuantError::NotFinite);
    }
    let norm = norm_of(v);
    if norm == 0.0 {
        v.fill(0.0);
        return Ok(1.0);
    }
    let norm32 = norm as f32;
    if !norm32.is_finite() {
        // Norm does not fit in f32 near f32::MAX.
        return Err(QuantError::NotFinite);
    }
    for x in v.iter_mut() {
        *x = (*x as f64 / norm) as f32;
    }
    Ok(norm32)
}

/// Same formula and rounding as Redis, so stored bytes match exactly.
fn q8_of_unit(unit: &[f32]) -> (Box<[i8]>, f32) {
    let max_abs = unit.iter().fold(0.0f32, |m, &x| m.max(x.abs()));
    if max_abs == 0.0 {
        return (vec![0i8; unit.len()].into_boxed_slice(), 0.0);
    }
    let scale = 127.0f32 / max_abs;
    let values = unit
        .iter()
        .map(|&x| (x * scale).round().clamp(-127.0, 127.0) as i8)
        .collect();
    (values, max_abs)
}

/// Bit set only for values above zero, so 0 and -0 read as -1.
fn bin_of(v: &[f32]) -> Box<[u64]> {
    let mut bits = vec![0u64; bin_words(v.len())].into_boxed_slice();
    for (i, &x) in v.iter().enumerate() {
        if x > 0.0 {
            bits[i / 64] |= 1u64 << (i % 64);
        }
    }
    bits
}

impl From<Vec<f32>> for StoredVector {
    fn from(v: Vec<f32>) -> Self {
        StoredVector::F32(v.into_boxed_slice())
    }
}

impl StoredVector {
    pub fn as_f32(&self) -> Option<&[f32]> {
        match self {
            StoredVector::F32(v) => Some(v),
            _ => None,
        }
    }

    /// Takes ownership to reuse the allocation instead of copying.
    pub fn quantize(quant: Quant, mut v: Vec<f32>) -> Result<StoredVector, QuantError> {
        match quant {
            Quant::F32 => Ok(StoredVector::F32(v.into_boxed_slice())),
            Quant::Q8 => {
                let norm = to_unit(&mut v)?;
                let (values, range) = q8_of_unit(&v);
                Ok(StoredVector::Q8 {
                    norm,
                    range,
                    values,
                })
            }
            Quant::Bin => {
                let norm = to_unit(&mut v)?;
                Ok(StoredVector::Bin {
                    norm,
                    bits: bin_of(&v),
                })
            }
        }
    }

    /// Norm and unit vector, the form `VEMB RAW` reports.
    pub fn f32_unit(v: &[f32]) -> (f32, Vec<f32>) {
        let mut unit = v.to_vec();
        let norm = to_unit(&mut unit).unwrap_or_else(|_| {
            // No meaningful norm, so return the input unchanged.
            unit.copy_from_slice(v);
            1.0
        });
        (norm, unit)
    }

    pub fn quant(&self) -> Quant {
        match self {
            StoredVector::F32(_) => Quant::F32,
            StoredVector::Q8 { .. } => Quant::Q8,
            StoredVector::Bin { .. } => Quant::Bin,
        }
    }

    /// BIN words are padded, so the set's dimension is needed.
    pub fn len_in(&self, set_dim: usize) -> usize {
        match self {
            StoredVector::F32(v) => v.len(),
            StoredVector::Q8 { values, .. } => values.len(),
            StoredVector::Bin { .. } => set_dim,
        }
    }

    /// Heap bytes, for `MEMORY USAGE`.
    pub fn heap_bytes(&self) -> usize {
        match self {
            StoredVector::F32(v) => std::mem::size_of_val::<[f32]>(v),
            StoredVector::Q8 { values, .. } => values.len(),
            StoredVector::Bin { bits, .. } => std::mem::size_of_val::<[u64]>(bits),
        }
    }

    /// Matches Redis `VEMB`: BIN gives unscaled +1/-1 values.
    pub fn to_f32(&self, dim: usize, out: &mut Vec<f32>) {
        out.clear();
        match self {
            StoredVector::F32(v) => out.extend_from_slice(v),
            StoredVector::Q8 {
                norm,
                range,
                values,
            } => {
                let step = range / 127.0;
                out.extend(values.iter().map(|&q| q as f32 * step * norm));
            }
            StoredVector::Bin { bits, .. } => out.extend((0..dim).map(|i| {
                if bits[i / 64] >> (i % 64) & 1 == 1 {
                    1.0
                } else {
                    -1.0
                }
            })),
        }
    }

    /// Works on quantized data directly; mixed kinds return infinity, not a panic.
    #[inline]
    pub fn distance(&self, other: &StoredVector, metric: &DistanceMetric, dim: usize) -> f32 {
        match (self, other) {
            (StoredVector::F32(a), StoredVector::F32(b)) => super::distance(metric, a, b),
            (
                StoredVector::Q8 {
                    norm: na,
                    range: ra,
                    values: a,
                },
                StoredVector::Q8 {
                    norm: nb,
                    range: rb,
                    values: b,
                },
            ) => {
                let dot = dot_i8(a, b) as f32 * (ra / 127.0) * (rb / 127.0);
                from_unit_dot(metric, dot, *na, *nb)
            }
            (StoredVector::Bin { norm: na, bits: a }, StoredVector::Bin { norm: nb, bits: b }) => {
                // Sign agreement estimates the cosine, as in Redis.
                let opposite = hamming(a, b) as f32;
                let dot = 1.0 - 2.0 * opposite / dim.max(1) as f32;
                from_unit_dot(metric, dot, *na, *nb)
            }
            _ => f32::INFINITY,
        }
    }
}

#[inline]
fn from_unit_dot(metric: &DistanceMetric, dot: f32, na: f32, nb: f32) -> f32 {
    match metric {
        DistanceMetric::Cosine => 1.0 - dot,
        DistanceMetric::InnerProduct => 1.0 - na * nb * dot,
        DistanceMetric::L2 => (na * na + nb * nb - 2.0 * na * nb * dot).max(0.0),
    }
}

/// Chunked so the i32 accumulator never overflows; the loop auto-vectorizes.
#[inline]
pub fn dot_i8(a: &[i8], b: &[i8]) -> i64 {
    const PIECE: usize = 1 << 16;
    let n = a.len().min(b.len());
    let (a, b) = (&a[..n], &b[..n]);
    let mut total = 0i64;
    for (pa, pb) in a.chunks(PIECE).zip(b.chunks(PIECE)) {
        let mut sum = 0i32;
        for (x, y) in pa.iter().zip(pb) {
            sum += *x as i32 * *y as i32;
        }
        total += sum as i64;
    }
    total
}

/// Count of components with opposite signs.
#[inline]
pub fn hamming(a: &[u64], b: &[u64]) -> u32 {
    a.iter().zip(b).map(|(x, y)| (x ^ y).count_ones()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(quant: Quant, v: &[f32]) -> StoredVector {
        StoredVector::quantize(quant, v.to_vec()).unwrap()
    }

    /// Expected values captured from Redis 8.10 `VEMB` and `VEMB RAW`.
    #[test]
    fn q8_is_what_redis_keeps() {
        let v = q(Quant::Q8, &[3.0, -4.0, 0.0]);
        let StoredVector::Q8 {
            norm,
            range,
            ref values,
        } = v
        else {
            panic!()
        };
        assert_eq!(&**values, &[95, -127, 0]);
        assert_eq!(norm, 5.0);
        assert_eq!(range, 0.8f32);
        let mut out = Vec::new();
        v.to_f32(3, &mut out);
        assert_eq!(out[0] as f64, 2.992125988006592);
        assert_eq!(out[1], -4.0);
        assert_eq!(out[2], 0.0);
    }

    /// Expected values captured from Redis 8.10 with `BIN`.
    #[test]
    fn bin_is_what_redis_keeps() {
        let v = q(Quant::Bin, &[3.0, -4.0, 0.0]);
        assert_eq!(
            v,
            StoredVector::Bin {
                norm: 5.0,
                bits: vec![1u64].into_boxed_slice()
            }
        );
        let mut out = Vec::new();
        v.to_f32(3, &mut out);
        assert_eq!(out, [1.0, -1.0, -1.0]);
        // -0 is not above zero.
        let v = q(Quant::Bin, &[-0.0, 1.0]);
        v.to_f32(2, &mut out);
        assert_eq!(out, [-1.0, 1.0]);
    }

    /// Redis stores a zero vector with norm 1 and range 0.
    #[test]
    fn zero_vector() {
        assert_eq!(
            q(Quant::Q8, &[0.0, -0.0, 0.0]),
            StoredVector::Q8 {
                norm: 1.0,
                range: 0.0,
                values: vec![0, 0, 0].into_boxed_slice()
            }
        );
        let zero = q(Quant::Q8, &[0.0; 3]);
        let other = q(Quant::Q8, &[1.0, 2.0, 3.0]);
        // Redis scores it 0.5 against anything.
        assert_eq!(zero.distance(&other, &DistanceMetric::Cosine, 3), 1.0);
    }

    #[test]
    fn not_finite_is_refused() {
        for v in [[f32::INFINITY, 1.0], [f32::NAN, 1.0], [f32::MAX, f32::MAX]] {
            assert_eq!(
                StoredVector::quantize(Quant::Q8, v.to_vec()),
                Err(QuantError::NotFinite)
            );
            assert_eq!(
                StoredVector::quantize(Quant::Bin, v.to_vec()),
                Err(QuantError::NotFinite)
            );
        }
        // Redis overflows here; the f64 norm keeps it.
        let v = q(Quant::Q8, &[1e30, -1e30]);
        let mut out = Vec::new();
        v.to_f32(2, &mut out);
        assert!((out[0] / 1e30 - 1.0).abs() < 1e-6, "{out:?}");
        assert!((out[1] / 1e30 + 1.0).abs() < 1e-6, "{out:?}");
    }

    /// Score from Redis 8.10, which also quantizes the query.
    #[test]
    fn q8_score_is_redis_score() {
        let a = q(Quant::Q8, &[1.0, 2.0, 3.0]);
        let query = q(Quant::Q8, &[1.0, 0.0, 0.0]);
        let d = a.distance(&query, &DistanceMetric::Cosine, 3);
        let score = 1.0 - d / 2.0;
        assert!((score as f64 - 0.6325784027576447).abs() < 1e-7, "{score}");
    }

    /// Redis gives 0.5 when half the signs differ.
    #[test]
    fn bin_score_is_redis_score() {
        let a = q(Quant::Bin, &[1.0; 70]);
        let mut half = vec![1.0f32; 35];
        half.extend([-1.0f32; 35]);
        let b = q(Quant::Bin, &half);
        let StoredVector::Bin { bits, .. } = &a else {
            panic!()
        };
        assert_eq!(&**bits, &[u64::MAX, 0x3f]);
        assert_eq!(1.0 - a.distance(&b, &DistanceMetric::Cosine, 70) / 2.0, 0.5);
    }

    /// Each component within half a step; cosine within 0.01.
    #[test]
    fn q8_error_is_bounded() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        };
        for dim in [1usize, 3, 128, 768, 1536] {
            for _ in 0..20 {
                let a: Vec<f32> = (0..dim).map(|_| next() * 10.0).collect();
                let b: Vec<f32> = (0..dim).map(|_| next() * 10.0).collect();
                let qa = q(Quant::Q8, &a);
                let mut back = Vec::new();
                qa.to_f32(dim, &mut back);
                let norm = norm_of(&a) as f32;
                let StoredVector::Q8 { range, .. } = qa else {
                    panic!()
                };
                let half_step = range / 127.0 / 2.0 * norm;
                for (x, y) in a.iter().zip(&back) {
                    assert!((x - y).abs() <= half_step * 1.001 + 1e-6, "{dim}: {x} {y}");
                }
                let exact = super::super::dist_cosine(&a, &b);
                let qb = q(Quant::Q8, &b);
                let approx = qa.distance(&qb, &DistanceMetric::Cosine, dim);
                assert!((exact - approx).abs() < 0.01, "{dim}: {exact} {approx}");
                // Inner product error scales with the norms.
                let ip = qa.distance(&qb, &DistanceMetric::InnerProduct, dim);
                let exact_ip = super::super::dist_ip(&a, &b);
                let scale = norm * norm_of(&b) as f32;
                assert!(
                    (ip - exact_ip).abs() <= 0.01 * scale,
                    "{dim}: {ip} {exact_ip}"
                );
            }
        }
    }

    /// Per-call cost of each kernel; run with `--release -- --ignored --nocapture`.
    #[test]
    #[ignore = "timing, not a check"]
    fn kernel_timings() {
        use std::hint::black_box;
        use std::time::Instant;
        let time = |what: &str, dim: usize, rounds: u32, mut f: Box<dyn FnMut()>| {
            for _ in 0..rounds / 10 {
                f();
            }
            let t = Instant::now();
            for _ in 0..rounds {
                f();
            }
            let ns = t.elapsed().as_nanos() as f64 / rounds as f64;
            println!("{dim:>5} {what:<16} {ns:>9.1} ns");
        };
        for dim in [128usize, 768, 1536] {
            let a: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.37).sin()).collect();
            let b: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.11).cos()).collect();
            let n = 200_000;
            for quant in [Quant::F32, Quant::Q8, Quant::Bin] {
                let v = a.clone();
                time(
                    &format!("quantize {}", quant.name()),
                    dim,
                    n,
                    Box::new(move || {
                        black_box(StoredVector::quantize(quant, black_box(v.clone())).unwrap());
                    }),
                );
                let (x, y) = (q(quant, &a), q(quant, &b));
                time(
                    &format!("distance {}", quant.name()),
                    dim,
                    n,
                    Box::new(move || {
                        black_box(black_box(&x).distance(
                            black_box(&y),
                            &DistanceMetric::Cosine,
                            dim,
                        ));
                    }),
                );
                let x = q(quant, &a);
                let mut out = Vec::with_capacity(dim);
                time(
                    &format!("dequantize {}", quant.name()),
                    dim,
                    n,
                    Box::new(move || {
                        x.to_f32(dim, &mut out);
                        black_box(&out);
                    }),
                );
            }
        }
    }

    /// Boxed attributes keep an element as small as a 1.2 f32 one.
    #[test]
    fn entry_size() {
        assert_eq!(std::mem::size_of::<crate::types::VSetEntry>(), 48);
    }

    #[test]
    fn dot_i8_past_an_i32() {
        let a = vec![-127i8; 200_000];
        assert_eq!(dot_i8(&a, &a), 127 * 127 * 200_000);
    }

    #[test]
    fn sizes() {
        let v = vec![0.5f32; 1536];
        assert_eq!(q(Quant::F32, &v).heap_bytes(), 6144);
        assert_eq!(q(Quant::Q8, &v).heap_bytes(), 1536);
        assert_eq!(q(Quant::Bin, &v).heap_bytes(), 192);
        assert_eq!(q(Quant::Bin, &[1.0; 65]).heap_bytes(), 16);
    }
}
