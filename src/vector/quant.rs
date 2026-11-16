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

/// Whether any database holds a Q8 or BIN set, which 1.2 cannot read.
pub fn store_holds_quantized_set(store: &crate::store::Store) -> bool {
    store.iter().flat_map(|db| db.iter()).any(|shard| {
        crate::read_lock!(shard).values().any(|value| match value {
            crate::store::ValueRef::VectorSet(vs) => vs.read().quant != Quant::F32,
            _ => false,
        })
    })
}

/// Where `VADD`'s options start: past the vector and the element.
fn vadd_options_start(cmd: &[Vec<u8>]) -> Option<usize> {
    let word = |i: usize, w: &[u8]| cmd.get(i).is_some_and(|a| a.eq_ignore_ascii_case(w));
    let mut i = 2;
    if word(i, b"DISTANCE") {
        i += 2;
    }
    if word(i, b"FP32") {
        i += 2;
    } else if word(i, b"VALUES") {
        let count: usize = std::str::from_utf8(cmd.get(i + 1)?).ok()?.parse().ok()?;
        i = i.checked_add(2)?.checked_add(count)?;
    } else {
        return None;
    }
    // The element.
    (i < cmd.len()).then_some(i + 1)
}

/// Where a `VADD`'s quantization options are; none for another command.
fn quant_options(cmd: &[Vec<u8>]) -> Vec<usize> {
    let mut found = Vec::new();
    if !cmd.first().is_some_and(|c| c.eq_ignore_ascii_case(b"VADD")) {
        return found;
    }
    let Some(start) = vadd_options_start(cmd) else {
        return found;
    };
    let mut i = start;
    // As `vadd` walks them: these and `CAS` stand alone, the rest take a value.
    while i < cmd.len() {
        if Quant::parse_option(&cmd[i]).is_some() {
            found.push(i);
            i += 1;
        } else if cmd[i].eq_ignore_ascii_case(b"CAS") {
            i += 1;
        } else {
            i += 2;
        }
    }
    found
}

/// A `VADD` with `NOQUANT`, `Q8` or `BIN`: its AOF record takes a marker.
pub fn carries_quant_option(cmd: &[Vec<u8>]) -> bool {
    !quant_options(cmd).is_empty()
}

/// Strips quantization options by position, so an element named `Q8` survives.
pub fn without_quant_options(cmd: Vec<Vec<u8>>) -> (Vec<Vec<u8>>, bool) {
    let dropped = quant_options(&cmd);
    if dropped.is_empty() {
        return (cmd, false);
    }
    let kept = cmd
        .into_iter()
        .enumerate()
        .filter(|(at, _)| !dropped.contains(at))
        .map(|(_, arg)| arg)
        .collect();
    (kept, true)
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

/// In f64: the same bits on every CPU, without f32 overflow or underflow.
#[inline(always)]
fn norm_of(v: &[f32]) -> f64 {
    v.iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt()
}

/// Normalizes in place; a zero vector gets norm 1, like Redis.
#[inline(always)]
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

/// Redis's formula and rounding; the f64 norm can change the last bit.
#[inline(always)]
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
#[inline(always)]
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
    ///
    /// On x86_64 with AVX2 the same code runs compiled for AVX2 (#793): the
    /// default target has no SSE4.1, so each `round` was a call into libm,
    /// and the loops ran a lane at a time. Rust does not fuse or reorder
    /// float operations for a target feature, so the result is the same
    /// bits; a test compares the two.
    pub fn quantize(quant: Quant, v: Vec<f32>) -> Result<StoredVector, QuantError> {
        Self::quantize_at(super::simd::int_level(), quant, v)
    }

    /// [`Self::quantize`] compiled for the given level's instruction set.
    /// Panics if this machine cannot run `level`.
    pub(crate) fn quantize_at(
        level: super::simd::IntLevel,
        quant: Quant,
        v: Vec<f32>,
    ) -> Result<StoredVector, QuantError> {
        use super::simd::IntLevel;
        match level {
            IntLevel::Portable => Self::quantize_portable(quant, v),
            #[cfg(target_arch = "x86_64")]
            // SAFETY: both levels need AVX2, which `has_int` saw on this CPU.
            IntLevel::Avx2 | IntLevel::Avx512 if super::simd::has_int(level) => unsafe {
                Self::quantize_avx2(quant, v)
            },
            #[allow(unreachable_patterns)]
            other => panic!("this CPU cannot run {other:?} quantization"),
        }
    }

    /// # Safety
    /// The CPU has AVX2.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn quantize_avx2(quant: Quant, v: Vec<f32>) -> Result<StoredVector, QuantError> {
        Self::quantize_portable(quant, v)
    }

    #[inline(always)]
    fn quantize_portable(quant: Quant, mut v: Vec<f32>) -> Result<StoredVector, QuantError> {
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
                // Rounding can leave |dot| above 1; Redis clamps distance to [0, 2].
                from_unit_dot(metric, dot.clamp(-1.0, 1.0), *na, *nb)
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

/// Exact int8 dot product: AVX-512BW or AVX2 picked at run time on x86_64.
#[inline]
pub fn dot_i8(a: &[i8], b: &[i8]) -> i64 {
    super::simd::dot_i8(a, b)
}

/// Count of components with opposite signs: POPCNT when the CPU has it.
#[inline]
pub fn hamming(a: &[u64], b: &[u64]) -> u32 {
    super::simd::hamming(a, b)
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

    /// Redis's f32 norm underflows here: it keeps norm 1, or reads 1e-45 as -0.
    #[test]
    fn tiny_vectors_keep_their_values() {
        for v in [[1e-23f32, 2e-23], [1e-45, 0.0]] {
            let stored = q(Quant::Q8, &v);
            let StoredVector::Q8 { norm, .. } = &stored else {
                panic!("{stored:?}")
            };
            let want = ((v[0] as f64).powi(2) + (v[1] as f64).powi(2)).sqrt() as f32;
            assert_eq!(*norm, want);
            let mut out = Vec::new();
            stored.to_f32(2, &mut out);
            for (got, want) in out.iter().zip(v) {
                assert!((got - want).abs() <= want * 0.01, "{out:?} for {v:?}");
            }
        }
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

    /// Q8 distance from `a` to `b`, and the dot product before the clamp.
    fn q8_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> (f32, f32) {
        let (qa, qb) = (q(Quant::Q8, a), q(Quant::Q8, b));
        let (
            StoredVector::Q8 {
                range: ra,
                values: va,
                ..
            },
            StoredVector::Q8 {
                range: rb,
                values: vb,
                ..
            },
        ) = (&qa, &qb)
        else {
            panic!()
        };
        let dot = dot_i8(va, vb) as f32 * (ra / 127.0) * (rb / 127.0);
        (qa.distance(&qb, &metric, a.len()), dot)
    }

    /// Rounding up past 1 is clamped as Redis clamps it: distance in [0, 2].
    #[test]
    fn q8_distance_is_clamped() {
        // 127 * 0.50315 = 63.9, stored as 64: the dot comes out above 1.
        let v = [1.0f32, 0.50315];
        let opposite = [-1.0f32, -0.50315];
        let (same, dot) = q8_distance(&v, &v, DistanceMetric::Cosine);
        assert!(dot > 1.0, "the case needs a dot above 1: {dot}");
        assert_eq!(same, 0.0);
        let (far, dot) = q8_distance(&v, &opposite, DistanceMetric::Cosine);
        assert!(dot < -1.0, "the case needs a dot below -1: {dot}");
        assert_eq!(far, 2.0);
        // Scores stay in [0, 1].
        assert_eq!(1.0 - same / 2.0, 1.0);
        assert_eq!(1.0 - far / 2.0, 0.0);
        // Nearly the same: small, and never below 0.
        let (near, _) = q8_distance(&v, &[1.0, 0.5032], DistanceMetric::Cosine);
        assert!((0.0..0.001).contains(&near), "{near}");
        // Unclamped, L2 between unit vectors fell below 0 and IP below -1.
        let (l2, _) = q8_distance(&v, &v, DistanceMetric::L2);
        assert!(l2 >= 0.0, "{l2}");
        let unit = [1.0f32, 0.0];
        let (ip, _) = q8_distance(&unit, &unit, DistanceMetric::InnerProduct);
        assert_eq!(ip, 0.0);
    }

    fn args(words: &[&str]) -> Vec<Vec<u8>> {
        words.iter().map(|w| w.as_bytes().to_vec()).collect()
    }

    /// Options go by position: an element, value or attribute named `Q8` stays.
    #[test]
    fn quant_options_are_dropped_by_position() {
        let blob = std::str::from_utf8(&[b'0'; 8]).unwrap().to_string();
        let cases: [(&[&str], &[&str], bool); 9] = [
            (
                &["VADD", "k", "VALUES", "3", "1", "2", "3", "e", "Q8"],
                &["VADD", "k", "VALUES", "3", "1", "2", "3", "e"],
                true,
            ),
            (
                &["VADD", "k", "VALUES", "3", "1", "2", "3", "Q8", "BIN"],
                &["VADD", "k", "VALUES", "3", "1", "2", "3", "Q8"],
                true,
            ),
            (
                &["vadd", "k", "FP32", &blob, "e", "NOQUANT", "EF", "200"],
                &["vadd", "k", "FP32", &blob, "e", "EF", "200"],
                true,
            ),
            (
                &["VADD", "k", "VALUES", "1", "1", "e", "SETATTR", "Q8"],
                &["VADD", "k", "VALUES", "1", "1", "e", "SETATTR", "Q8"],
                false,
            ),
            (
                &[
                    "VADD", "k", "DISTANCE", "L2", "VALUES", "1", "1", "e", "q8", "cas",
                ],
                &[
                    "VADD", "k", "DISTANCE", "L2", "VALUES", "1", "1", "e", "cas",
                ],
                true,
            ),
            (
                &[
                    "VADD", "k", "VALUES", "2", "Q8", "BIN", "e", "Q8", "M", "16", "BIN",
                ],
                &["VADD", "k", "VALUES", "2", "Q8", "BIN", "e", "M", "16"],
                true,
            ),
            (
                &["VADD", "k", "VALUES", "x", "Q8", "e", "Q8"],
                &["VADD", "k", "VALUES", "x", "Q8", "e", "Q8"],
                false,
            ),
            (
                &["VADD", "k", "VALUES", "9", "1", "Q8"],
                &["VADD", "k", "VALUES", "9", "1", "Q8"],
                false,
            ),
            (&["SET", "Q8", "BIN"], &["SET", "Q8", "BIN"], false),
        ];
        for (given, want, dropped) in cases {
            assert_eq!(carries_quant_option(&args(given)), dropped, "{given:?}");
            let got = without_quant_options(args(given));
            assert_eq!(got, (args(want), dropped), "{given:?}");
        }
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
            println!("{dim:>5} {what:<24} {ns:>9.1} ns");
        };
        for dim in [128usize, 768, 1536] {
            let a: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.37).sin()).collect();
            let b: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.11).cos()).collect();
            let n = 200_000;
            for quant in [Quant::F32, Quant::Q8, Quant::Bin] {
                for level in crate::vector::simd::available_int() {
                    let v = a.clone();
                    time(
                        &format!("quantize {} {level:?}", quant.name()),
                        dim,
                        n,
                        Box::new(move || {
                            black_box(
                                StoredVector::quantize_at(level, quant, black_box(v.clone()))
                                    .unwrap(),
                            );
                        }),
                    );
                }
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

    /// Quantizing compiled for AVX2 (#793) keeps every bit: random vectors
    /// of many lengths and scales, ties at .5 that `round` takes away from
    /// zero, zeros, signed zeros, subnormals and huge values.
    #[test]
    fn quantize_is_the_same_at_every_level() {
        use crate::vector::simd::{IntLevel, available_int};
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        };
        let mut cases: Vec<Vec<f32>> = Vec::new();
        for dim in [
            1usize, 2, 3, 7, 8, 15, 16, 17, 31, 33, 64, 65, 128, 768, 1536, 4096,
        ] {
            for scale in [1e-30f32, 1e-3, 1.0, 10.0, 1e20] {
                cases.push((0..dim).map(|_| next() * scale).collect());
            }
        }
        // Components that land on .5 steps, and the edges `to_unit` handles.
        cases.push(vec![
            1.0,
            0.5 / 127.0,
            1.5 / 127.0,
            -2.5 / 127.0,
            63.5 / 127.0,
        ]);
        cases.push(vec![0.0; 9]);
        cases.push(vec![-0.0, 0.0, -0.0]);
        cases.push(vec![1e-45, -1e-45, 0.0, 2e-45]);
        cases.push(vec![1e30, -1e30, 3e29]);
        cases.push(vec![f32::NAN, 1.0]);
        cases.push(vec![f32::INFINITY, 1.0]);
        cases.push(vec![f32::MAX, f32::MAX]);
        for v in &cases {
            for quant in [Quant::Q8, Quant::Bin] {
                let want = StoredVector::quantize_at(IntLevel::Portable, quant, v.clone());
                for level in available_int() {
                    let got = StoredVector::quantize_at(level, quant, v.clone());
                    // Compare bits, so that -0 and NaN count too.
                    let bits = |r: &Result<StoredVector, QuantError>| match r {
                        Ok(StoredVector::Q8 {
                            norm,
                            range,
                            values,
                        }) => Ok((norm.to_bits(), range.to_bits(), values.to_vec(), vec![])),
                        Ok(StoredVector::Bin { norm, bits }) => {
                            Ok((norm.to_bits(), 0, vec![], bits.to_vec()))
                        }
                        Ok(StoredVector::F32(_)) => unreachable!(),
                        Err(e) => Err(format!("{e:?}")),
                    };
                    assert_eq!(bits(&got), bits(&want), "{level:?} {quant:?} {v:?}");
                }
            }
        }
    }

    /// Distances through the dispatched kernels are the portable ones, bit
    /// for bit, for every metric.
    #[test]
    fn q8_and_bin_distances_are_the_portable_ones() {
        use crate::vector::simd::{IntLevel, dot_i8_at, hamming_at};
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        };
        for dim in [3usize, 100, 768, 1536, 4096] {
            for _ in 0..10 {
                let a: Vec<f32> = (0..dim).map(|_| next()).collect();
                let b: Vec<f32> = (0..dim).map(|_| next()).collect();
                for metric in [
                    DistanceMetric::Cosine,
                    DistanceMetric::L2,
                    DistanceMetric::InnerProduct,
                ] {
                    let (qa, qb) = (q(Quant::Q8, &a), q(Quant::Q8, &b));
                    let (
                        StoredVector::Q8 {
                            norm: na,
                            range: ra,
                            values: va,
                        },
                        StoredVector::Q8 {
                            norm: nb,
                            range: rb,
                            values: vb,
                        },
                    ) = (&qa, &qb)
                    else {
                        panic!()
                    };
                    let dot =
                        dot_i8_at(IntLevel::Portable, va, vb) as f32 * (ra / 127.0) * (rb / 127.0);
                    let want = from_unit_dot(&metric, dot.clamp(-1.0, 1.0), *na, *nb);
                    let got = qa.distance(&qb, &metric, dim);
                    assert_eq!(got.to_bits(), want.to_bits(), "q8 {dim} {metric:?}");

                    let (ba, bb) = (q(Quant::Bin, &a), q(Quant::Bin, &b));
                    let (
                        StoredVector::Bin { norm: na, bits: xa },
                        StoredVector::Bin { norm: nb, bits: xb },
                    ) = (&ba, &bb)
                    else {
                        panic!()
                    };
                    let opposite = hamming_at(IntLevel::Portable, xa, xb) as f32;
                    let dot = 1.0 - 2.0 * opposite / dim as f32;
                    let want = from_unit_dot(&metric, dot, *na, *nb);
                    let got = ba.distance(&bb, &metric, dim);
                    assert_eq!(got.to_bits(), want.to_bits(), "bin {dim} {metric:?}");
                }
            }
        }
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
