//! f32 distance kernels: dot product, squared L2, and the parts of a cosine,
//! for AVX-512, AVX2 with FMA, and NEON, picked at run time, with a portable
//! fallback.
//!
//! A plain `iter().map().sum()` is one chain of additions: the compiler may
//! not reorder float additions, so it cannot split the sum across the lanes
//! of a vector register, and every element waits for the one before it. Each
//! kernel here keeps several independent sums (four registers of 16 or 8
//! lanes, or eight scalars in the fallback) and adds them at the end. The
//! result differs from the sequential sum by rounding only: the tests below
//! hold every kernel to an f64 reference.
//!
//! Vectors of different lengths are measured over the shorter one, as the
//! `zip` of the scalar code did.

use std::sync::atomic::{AtomicU8, Ordering};

/// The instruction set the kernels run with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    /// Eight independent sums in plain Rust.
    Portable,
    /// x86_64 AVX2 and FMA: 8 lanes.
    Avx2,
    /// x86_64 AVX-512F: 16 lanes.
    Avx512,
    /// aarch64 NEON: 4 lanes. Always there on aarch64.
    Neon,
}

const UNKNOWN: u8 = 0;

/// The level found at first use: 0 until then.
static LEVEL: AtomicU8 = AtomicU8::new(UNKNOWN);

fn encode(level: Level) -> u8 {
    match level {
        Level::Portable => 1,
        Level::Avx2 => 2,
        Level::Avx512 => 3,
        Level::Neon => 4,
    }
}

fn decode(raw: u8) -> Level {
    match raw {
        2 => Level::Avx2,
        3 => Level::Avx512,
        4 => Level::Neon,
        _ => Level::Portable,
    }
}

/// The best level this CPU has.
fn detect() -> Level {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            return Level::Avx512;
        }
        if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma")
        {
            return Level::Avx2;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        return Level::Neon;
    }
    #[allow(unreachable_code)]
    Level::Portable
}

/// The level the kernels run with on this machine.
#[inline]
pub fn level() -> Level {
    let raw = LEVEL.load(Ordering::Relaxed);
    if raw != UNKNOWN {
        return decode(raw);
    }
    let found = detect();
    LEVEL.store(encode(found), Ordering::Relaxed);
    found
}

/// Every level this machine can run, the portable one first.
#[cfg(test)]
pub fn available() -> Vec<Level> {
    let mut out = vec![Level::Portable];
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma")
        {
            out.push(Level::Avx2);
        }
        if std::arch::is_x86_feature_detected!("avx512f") {
            out.push(Level::Avx512);
        }
    }
    #[cfg(target_arch = "aarch64")]
    out.push(Level::Neon);
    out
}

/// The two slices cut to the shorter length.
#[inline(always)]
fn same_len<'a>(a: &'a [f32], b: &'a [f32]) -> (&'a [f32], &'a [f32]) {
    let n = a.len().min(b.len());
    (&a[..n], &b[..n])
}

// ── Entry points ─────────────────────────────────────────────────────────────

/// Σ a·b.
#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    dot_at(level(), a, b)
}

/// Σ (a − b)².
#[inline]
pub fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    l2_sq_at(level(), a, b)
}

/// (Σ a·b, Σ a², Σ b²) in one pass: a cosine of two vectors neither of which
/// is known to be of unit length.
#[inline]
pub fn dot_norms(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    dot_norms_at(level(), a, b)
}

/// (Σ a·b, Σ b²) in one pass: a cosine against a query already of unit
/// length.
#[inline]
pub fn dot_norm_b(a: &[f32], b: &[f32]) -> (f32, f32) {
    let (dot, _, nb) = dot_norms_at(level(), a, b);
    (dot, nb)
}

/// Σ a².
#[inline]
pub fn norm_sq(a: &[f32]) -> f32 {
    dot_at(level(), a, a)
}

/// [`dot`] at a given level. Panics if this machine cannot run `level`.
pub fn dot_at(level: Level, a: &[f32], b: &[f32]) -> f32 {
    let (a, b) = same_len(a, b);
    match level {
        #[cfg(target_arch = "x86_64")]
        // SAFETY: the level is only chosen (or accepted below) when the CPU
        // has the features the function is compiled for.
        Level::Avx512 if has(level) => unsafe { x86::dot_avx512(a, b) },
        #[cfg(target_arch = "x86_64")]
        Level::Avx2 if has(level) => unsafe { x86::dot_avx2(a, b) },
        #[cfg(target_arch = "aarch64")]
        Level::Neon => neon::dot(a, b),
        Level::Portable => portable::dot(a, b),
        other => unsupported(other),
    }
}

/// [`l2_sq`] at a given level. Panics if this machine cannot run `level`.
pub fn l2_sq_at(level: Level, a: &[f32], b: &[f32]) -> f32 {
    let (a, b) = same_len(a, b);
    match level {
        #[cfg(target_arch = "x86_64")]
        // SAFETY: as in `dot_at`.
        Level::Avx512 if has(level) => unsafe { x86::l2_avx512(a, b) },
        #[cfg(target_arch = "x86_64")]
        Level::Avx2 if has(level) => unsafe { x86::l2_avx2(a, b) },
        #[cfg(target_arch = "aarch64")]
        Level::Neon => neon::l2(a, b),
        Level::Portable => portable::l2(a, b),
        other => unsupported(other),
    }
}

/// [`dot_norms`] at a given level. Panics if this machine cannot run `level`.
pub fn dot_norms_at(level: Level, a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    let (a, b) = same_len(a, b);
    match level {
        #[cfg(target_arch = "x86_64")]
        // SAFETY: as in `dot_at`.
        Level::Avx512 if has(level) => unsafe { x86::dot_norms_avx512(a, b) },
        #[cfg(target_arch = "x86_64")]
        Level::Avx2 if has(level) => unsafe { x86::dot_norms_avx2(a, b) },
        #[cfg(target_arch = "aarch64")]
        Level::Neon => neon::dot_norms(a, b),
        Level::Portable => portable::dot_norms(a, b),
        other => unsupported(other),
    }
}

/// Whether `level` is the detected one or one this CPU also has. The
/// detected level answers from the cached value without asking the CPU.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn has(level: Level) -> bool {
    let raw = LEVEL.load(Ordering::Relaxed);
    if raw == encode(level) {
        return true;
    }
    match level {
        Level::Avx512 => std::arch::is_x86_feature_detected!("avx512f"),
        Level::Avx2 => {
            std::arch::is_x86_feature_detected!("avx2")
                && std::arch::is_x86_feature_detected!("fma")
        }
        _ => true,
    }
}

#[cold]
fn unsupported(level: Level) -> ! {
    panic!("this CPU cannot run the {level:?} distance kernels")
}

// ── Portable: eight independent sums ─────────────────────────────────────────

mod portable {
    /// The sum of eight partial sums, pairwise.
    #[inline(always)]
    fn fold(acc: [f32; 8]) -> f32 {
        ((acc[0] + acc[4]) + (acc[1] + acc[5])) + ((acc[2] + acc[6]) + (acc[3] + acc[7]))
    }

    pub(super) fn dot(a: &[f32], b: &[f32]) -> f32 {
        let (ca, ta) = a.as_chunks::<8>();
        let (cb, tb) = b.as_chunks::<8>();
        let mut acc = [0f32; 8];
        for (x, y) in ca.iter().zip(cb) {
            for j in 0..8 {
                acc[j] += x[j] * y[j];
            }
        }
        let mut sum = fold(acc);
        for (x, y) in ta.iter().zip(tb) {
            sum += x * y;
        }
        sum
    }

    pub(super) fn l2(a: &[f32], b: &[f32]) -> f32 {
        let (ca, ta) = a.as_chunks::<8>();
        let (cb, tb) = b.as_chunks::<8>();
        let mut acc = [0f32; 8];
        for (x, y) in ca.iter().zip(cb) {
            for j in 0..8 {
                let d = x[j] - y[j];
                acc[j] += d * d;
            }
        }
        let mut sum = fold(acc);
        for (x, y) in ta.iter().zip(tb) {
            let d = x - y;
            sum += d * d;
        }
        sum
    }

    pub(super) fn dot_norms(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        let (ca, ta) = a.as_chunks::<8>();
        let (cb, tb) = b.as_chunks::<8>();
        let mut d = [0f32; 8];
        let mut na = [0f32; 8];
        let mut nb = [0f32; 8];
        for (x, y) in ca.iter().zip(cb) {
            for j in 0..8 {
                d[j] += x[j] * y[j];
                na[j] += x[j] * x[j];
                nb[j] += y[j] * y[j];
            }
        }
        let (mut d, mut na, mut nb) = (fold(d), fold(na), fold(nb));
        for (x, y) in ta.iter().zip(tb) {
            d += x * y;
            na += x * x;
            nb += y * y;
        }
        (d, na, nb)
    }
}

// ── x86_64: AVX2 + FMA, AVX-512F ─────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    /// The sum of the eight lanes.
    #[inline]
    #[target_feature(enable = "avx2,fma")]
    fn hsum256(v: __m256) -> f32 {
        let lo = _mm256_castps256_ps128(v);
        let hi = _mm256_extractf128_ps(v, 1);
        let s = _mm_add_ps(lo, hi);
        let s = _mm_add_ps(s, _mm_movehl_ps(s, s));
        let s = _mm_add_ss(s, _mm_shuffle_ps(s, s, 0b01));
        _mm_cvtss_f32(s)
    }

    /// # Safety
    /// The CPU has AVX2 and FMA; `a` and `b` are of the same length.
    #[target_feature(enable = "avx2,fma")]
    pub(super) unsafe fn dot_avx2(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len();
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: every load is of 8 floats at an offset at most n - 8.
        unsafe {
            let mut s0 = _mm256_setzero_ps();
            let mut s1 = _mm256_setzero_ps();
            let mut s2 = _mm256_setzero_ps();
            let mut s3 = _mm256_setzero_ps();
            let mut i = 0;
            while i + 32 <= n {
                s0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), s0);
                s1 = _mm256_fmadd_ps(
                    _mm256_loadu_ps(pa.add(i + 8)),
                    _mm256_loadu_ps(pb.add(i + 8)),
                    s1,
                );
                s2 = _mm256_fmadd_ps(
                    _mm256_loadu_ps(pa.add(i + 16)),
                    _mm256_loadu_ps(pb.add(i + 16)),
                    s2,
                );
                s3 = _mm256_fmadd_ps(
                    _mm256_loadu_ps(pa.add(i + 24)),
                    _mm256_loadu_ps(pb.add(i + 24)),
                    s3,
                );
                i += 32;
            }
            while i + 8 <= n {
                s0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), s0);
                i += 8;
            }
            let mut sum = hsum256(_mm256_add_ps(_mm256_add_ps(s0, s1), _mm256_add_ps(s2, s3)));
            while i < n {
                sum += *pa.add(i) * *pb.add(i);
                i += 1;
            }
            sum
        }
    }

    /// # Safety
    /// As [`dot_avx2`].
    #[target_feature(enable = "avx2,fma")]
    pub(super) unsafe fn l2_avx2(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len();
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: as in `dot_avx2`.
        unsafe {
            let mut s0 = _mm256_setzero_ps();
            let mut s1 = _mm256_setzero_ps();
            let mut s2 = _mm256_setzero_ps();
            let mut s3 = _mm256_setzero_ps();
            let mut i = 0;
            while i + 32 <= n {
                let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
                let d1 = _mm256_sub_ps(
                    _mm256_loadu_ps(pa.add(i + 8)),
                    _mm256_loadu_ps(pb.add(i + 8)),
                );
                let d2 = _mm256_sub_ps(
                    _mm256_loadu_ps(pa.add(i + 16)),
                    _mm256_loadu_ps(pb.add(i + 16)),
                );
                let d3 = _mm256_sub_ps(
                    _mm256_loadu_ps(pa.add(i + 24)),
                    _mm256_loadu_ps(pb.add(i + 24)),
                );
                s0 = _mm256_fmadd_ps(d0, d0, s0);
                s1 = _mm256_fmadd_ps(d1, d1, s1);
                s2 = _mm256_fmadd_ps(d2, d2, s2);
                s3 = _mm256_fmadd_ps(d3, d3, s3);
                i += 32;
            }
            while i + 8 <= n {
                let d = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
                s0 = _mm256_fmadd_ps(d, d, s0);
                i += 8;
            }
            let mut sum = hsum256(_mm256_add_ps(_mm256_add_ps(s0, s1), _mm256_add_ps(s2, s3)));
            while i < n {
                let d = *pa.add(i) - *pb.add(i);
                sum += d * d;
                i += 1;
            }
            sum
        }
    }

    /// # Safety
    /// As [`dot_avx2`].
    #[target_feature(enable = "avx2,fma")]
    pub(super) unsafe fn dot_norms_avx2(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        let n = a.len();
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: as in `dot_avx2`.
        unsafe {
            let mut d0 = _mm256_setzero_ps();
            let mut d1 = _mm256_setzero_ps();
            let mut a0 = _mm256_setzero_ps();
            let mut a1 = _mm256_setzero_ps();
            let mut b0 = _mm256_setzero_ps();
            let mut b1 = _mm256_setzero_ps();
            let mut i = 0;
            while i + 16 <= n {
                let x0 = _mm256_loadu_ps(pa.add(i));
                let y0 = _mm256_loadu_ps(pb.add(i));
                let x1 = _mm256_loadu_ps(pa.add(i + 8));
                let y1 = _mm256_loadu_ps(pb.add(i + 8));
                d0 = _mm256_fmadd_ps(x0, y0, d0);
                d1 = _mm256_fmadd_ps(x1, y1, d1);
                a0 = _mm256_fmadd_ps(x0, x0, a0);
                a1 = _mm256_fmadd_ps(x1, x1, a1);
                b0 = _mm256_fmadd_ps(y0, y0, b0);
                b1 = _mm256_fmadd_ps(y1, y1, b1);
                i += 16;
            }
            if i + 8 <= n {
                let x0 = _mm256_loadu_ps(pa.add(i));
                let y0 = _mm256_loadu_ps(pb.add(i));
                d0 = _mm256_fmadd_ps(x0, y0, d0);
                a0 = _mm256_fmadd_ps(x0, x0, a0);
                b0 = _mm256_fmadd_ps(y0, y0, b0);
                i += 8;
            }
            let mut d = hsum256(_mm256_add_ps(d0, d1));
            let mut na = hsum256(_mm256_add_ps(a0, a1));
            let mut nb = hsum256(_mm256_add_ps(b0, b1));
            while i < n {
                let (x, y) = (*pa.add(i), *pb.add(i));
                d += x * y;
                na += x * x;
                nb += y * y;
                i += 1;
            }
            (d, na, nb)
        }
    }

    /// The lanes of the last, partial block of 16: the low `left` of them.
    #[inline(always)]
    fn tail_mask(left: usize) -> __mmask16 {
        debug_assert!(left > 0 && left < 16);
        ((1u32 << left) - 1) as __mmask16
    }

    /// # Safety
    /// The CPU has AVX-512F; `a` and `b` are of the same length.
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn dot_avx512(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len();
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: full loads are of 16 floats at an offset at most n - 16;
        // the masked load of the tail reads only the `n - i` lanes that are
        // in the slice.
        unsafe {
            let mut s0 = _mm512_setzero_ps();
            let mut s1 = _mm512_setzero_ps();
            let mut s2 = _mm512_setzero_ps();
            let mut s3 = _mm512_setzero_ps();
            let mut i = 0;
            while i + 64 <= n {
                s0 = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)), s0);
                s1 = _mm512_fmadd_ps(
                    _mm512_loadu_ps(pa.add(i + 16)),
                    _mm512_loadu_ps(pb.add(i + 16)),
                    s1,
                );
                s2 = _mm512_fmadd_ps(
                    _mm512_loadu_ps(pa.add(i + 32)),
                    _mm512_loadu_ps(pb.add(i + 32)),
                    s2,
                );
                s3 = _mm512_fmadd_ps(
                    _mm512_loadu_ps(pa.add(i + 48)),
                    _mm512_loadu_ps(pb.add(i + 48)),
                    s3,
                );
                i += 64;
            }
            while i + 16 <= n {
                s0 = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)), s0);
                i += 16;
            }
            if i < n {
                let m = tail_mask(n - i);
                let x = _mm512_maskz_loadu_ps(m, pa.add(i));
                let y = _mm512_maskz_loadu_ps(m, pb.add(i));
                s1 = _mm512_fmadd_ps(x, y, s1);
            }
            _mm512_reduce_add_ps(_mm512_add_ps(_mm512_add_ps(s0, s1), _mm512_add_ps(s2, s3)))
        }
    }

    /// # Safety
    /// As [`dot_avx512`].
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn l2_avx512(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len();
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: as in `dot_avx512`.
        unsafe {
            let mut s0 = _mm512_setzero_ps();
            let mut s1 = _mm512_setzero_ps();
            let mut s2 = _mm512_setzero_ps();
            let mut s3 = _mm512_setzero_ps();
            let mut i = 0;
            while i + 64 <= n {
                let d0 = _mm512_sub_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)));
                let d1 = _mm512_sub_ps(
                    _mm512_loadu_ps(pa.add(i + 16)),
                    _mm512_loadu_ps(pb.add(i + 16)),
                );
                let d2 = _mm512_sub_ps(
                    _mm512_loadu_ps(pa.add(i + 32)),
                    _mm512_loadu_ps(pb.add(i + 32)),
                );
                let d3 = _mm512_sub_ps(
                    _mm512_loadu_ps(pa.add(i + 48)),
                    _mm512_loadu_ps(pb.add(i + 48)),
                );
                s0 = _mm512_fmadd_ps(d0, d0, s0);
                s1 = _mm512_fmadd_ps(d1, d1, s1);
                s2 = _mm512_fmadd_ps(d2, d2, s2);
                s3 = _mm512_fmadd_ps(d3, d3, s3);
                i += 64;
            }
            while i + 16 <= n {
                let d = _mm512_sub_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)));
                s0 = _mm512_fmadd_ps(d, d, s0);
                i += 16;
            }
            if i < n {
                let m = tail_mask(n - i);
                let d = _mm512_sub_ps(
                    _mm512_maskz_loadu_ps(m, pa.add(i)),
                    _mm512_maskz_loadu_ps(m, pb.add(i)),
                );
                s1 = _mm512_fmadd_ps(d, d, s1);
            }
            _mm512_reduce_add_ps(_mm512_add_ps(_mm512_add_ps(s0, s1), _mm512_add_ps(s2, s3)))
        }
    }

    /// # Safety
    /// As [`dot_avx512`].
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn dot_norms_avx512(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        let n = a.len();
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: as in `dot_avx512`.
        unsafe {
            let mut d0 = _mm512_setzero_ps();
            let mut d1 = _mm512_setzero_ps();
            let mut a0 = _mm512_setzero_ps();
            let mut a1 = _mm512_setzero_ps();
            let mut b0 = _mm512_setzero_ps();
            let mut b1 = _mm512_setzero_ps();
            let mut i = 0;
            while i + 32 <= n {
                let x0 = _mm512_loadu_ps(pa.add(i));
                let y0 = _mm512_loadu_ps(pb.add(i));
                let x1 = _mm512_loadu_ps(pa.add(i + 16));
                let y1 = _mm512_loadu_ps(pb.add(i + 16));
                d0 = _mm512_fmadd_ps(x0, y0, d0);
                d1 = _mm512_fmadd_ps(x1, y1, d1);
                a0 = _mm512_fmadd_ps(x0, x0, a0);
                a1 = _mm512_fmadd_ps(x1, x1, a1);
                b0 = _mm512_fmadd_ps(y0, y0, b0);
                b1 = _mm512_fmadd_ps(y1, y1, b1);
                i += 32;
            }
            if i + 16 <= n {
                let x0 = _mm512_loadu_ps(pa.add(i));
                let y0 = _mm512_loadu_ps(pb.add(i));
                d0 = _mm512_fmadd_ps(x0, y0, d0);
                a0 = _mm512_fmadd_ps(x0, x0, a0);
                b0 = _mm512_fmadd_ps(y0, y0, b0);
                i += 16;
            }
            if i < n {
                let m = tail_mask(n - i);
                let x = _mm512_maskz_loadu_ps(m, pa.add(i));
                let y = _mm512_maskz_loadu_ps(m, pb.add(i));
                d1 = _mm512_fmadd_ps(x, y, d1);
                a1 = _mm512_fmadd_ps(x, x, a1);
                b1 = _mm512_fmadd_ps(y, y, b1);
            }
            (
                _mm512_reduce_add_ps(_mm512_add_ps(d0, d1)),
                _mm512_reduce_add_ps(_mm512_add_ps(a0, a1)),
                _mm512_reduce_add_ps(_mm512_add_ps(b0, b1)),
            )
        }
    }
}

// ── aarch64: NEON ────────────────────────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
mod neon {
    use std::arch::aarch64::*;

    pub(super) fn dot(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: NEON is part of every aarch64 target; every load is of 4
        // floats at an offset at most n - 4.
        unsafe {
            let mut s0 = vdupq_n_f32(0.0);
            let mut s1 = vdupq_n_f32(0.0);
            let mut s2 = vdupq_n_f32(0.0);
            let mut s3 = vdupq_n_f32(0.0);
            let mut i = 0;
            while i + 16 <= n {
                s0 = vfmaq_f32(s0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
                s1 = vfmaq_f32(s1, vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
                s2 = vfmaq_f32(s2, vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
                s3 = vfmaq_f32(s3, vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
                i += 16;
            }
            while i + 4 <= n {
                s0 = vfmaq_f32(s0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
                i += 4;
            }
            let mut sum = vaddvq_f32(vaddq_f32(vaddq_f32(s0, s1), vaddq_f32(s2, s3)));
            while i < n {
                sum += *pa.add(i) * *pb.add(i);
                i += 1;
            }
            sum
        }
    }

    pub(super) fn l2(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: as in `dot`.
        unsafe {
            let mut s0 = vdupq_n_f32(0.0);
            let mut s1 = vdupq_n_f32(0.0);
            let mut s2 = vdupq_n_f32(0.0);
            let mut s3 = vdupq_n_f32(0.0);
            let mut i = 0;
            while i + 16 <= n {
                let d0 = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
                let d1 = vsubq_f32(vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
                let d2 = vsubq_f32(vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
                let d3 = vsubq_f32(vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
                s0 = vfmaq_f32(s0, d0, d0);
                s1 = vfmaq_f32(s1, d1, d1);
                s2 = vfmaq_f32(s2, d2, d2);
                s3 = vfmaq_f32(s3, d3, d3);
                i += 16;
            }
            while i + 4 <= n {
                let d = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
                s0 = vfmaq_f32(s0, d, d);
                i += 4;
            }
            let mut sum = vaddvq_f32(vaddq_f32(vaddq_f32(s0, s1), vaddq_f32(s2, s3)));
            while i < n {
                let d = *pa.add(i) - *pb.add(i);
                sum += d * d;
                i += 1;
            }
            sum
        }
    }

    pub(super) fn dot_norms(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // SAFETY: as in `dot`.
        unsafe {
            let mut d0 = vdupq_n_f32(0.0);
            let mut d1 = vdupq_n_f32(0.0);
            let mut a0 = vdupq_n_f32(0.0);
            let mut a1 = vdupq_n_f32(0.0);
            let mut b0 = vdupq_n_f32(0.0);
            let mut b1 = vdupq_n_f32(0.0);
            let mut i = 0;
            while i + 8 <= n {
                let x0 = vld1q_f32(pa.add(i));
                let y0 = vld1q_f32(pb.add(i));
                let x1 = vld1q_f32(pa.add(i + 4));
                let y1 = vld1q_f32(pb.add(i + 4));
                d0 = vfmaq_f32(d0, x0, y0);
                d1 = vfmaq_f32(d1, x1, y1);
                a0 = vfmaq_f32(a0, x0, x0);
                a1 = vfmaq_f32(a1, x1, x1);
                b0 = vfmaq_f32(b0, y0, y0);
                b1 = vfmaq_f32(b1, y1, y1);
                i += 8;
            }
            if i + 4 <= n {
                let x0 = vld1q_f32(pa.add(i));
                let y0 = vld1q_f32(pb.add(i));
                d0 = vfmaq_f32(d0, x0, y0);
                a0 = vfmaq_f32(a0, x0, x0);
                b0 = vfmaq_f32(b0, y0, y0);
                i += 4;
            }
            let mut d = vaddvq_f32(vaddq_f32(d0, d1));
            let mut na = vaddvq_f32(vaddq_f32(a0, a1));
            let mut nb = vaddvq_f32(vaddq_f32(b0, b1));
            while i < n {
                let (x, y) = (*pa.add(i), *pb.add(i));
                d += x * y;
                na += x * x;
                nb += y * y;
                i += 1;
            }
            (d, na, nb)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIMS: [usize; 13] = [0, 1, 3, 7, 8, 15, 16, 25, 33, 128, 512, 1536, 2048];

    /// A reproducible vector with values of both signs and of different sizes.
    fn vector(dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((state >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect()
    }

    /// The old scalar code, for comparison.
    fn scalar_dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    fn scalar_l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .zip(b)
            .map(|(x, y)| {
                let d = x - y;
                d * d
            })
            .sum()
    }

    /// Exact enough: in f64.
    fn ref_dot(a: &[f32], b: &[f32]) -> f64 {
        a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum()
    }

    fn ref_l2(a: &[f32], b: &[f32]) -> f64 {
        a.iter()
            .zip(b)
            .map(|(x, y)| {
                let d = *x as f64 - *y as f64;
                d * d
            })
            .sum()
    }

    /// Within float rounding of the f64 value: relative to the sum of the
    /// absolute terms, which bounds what reordering can change.
    fn close(got: f32, want: f64, magnitude: f64) -> bool {
        (got as f64 - want).abs() <= 1e-5 * magnitude.max(1.0)
    }

    #[test]
    fn every_level_matches_the_reference_at_every_length() {
        for level in available() {
            for &dim in &DIMS {
                let a = vector(dim, 1 + dim as u64);
                let b = vector(dim, 1000 + dim as u64);
                let mag: f64 = a
                    .iter()
                    .zip(&b)
                    .map(|(x, y)| (*x as f64 * *y as f64).abs())
                    .sum();

                let d = dot_at(level, &a, &b);
                assert!(close(d, ref_dot(&a, &b), mag), "{level:?} dot dim {dim}");
                assert!(
                    close(d, scalar_dot(&a, &b) as f64, mag),
                    "{level:?} dot vs scalar dim {dim}"
                );

                let l = l2_sq_at(level, &a, &b);
                let lw = ref_l2(&a, &b);
                assert!(close(l, lw, lw), "{level:?} l2 dim {dim}");
                assert!(
                    close(l, scalar_l2(&a, &b) as f64, lw),
                    "{level:?} l2 vs scalar dim {dim}"
                );

                let (dd, na, nb) = dot_norms_at(level, &a, &b);
                assert!(
                    close(dd, ref_dot(&a, &b), mag),
                    "{level:?} cos dot dim {dim}"
                );
                let (wa, wb) = (ref_dot(&a, &a), ref_dot(&b, &b));
                assert!(close(na, wa, wa), "{level:?} |a|^2 dim {dim}");
                assert!(close(nb, wb, wb), "{level:?} |b|^2 dim {dim}");
            }
        }
    }

    #[test]
    fn unequal_lengths_are_measured_over_the_shorter() {
        for level in available() {
            let a = vector(37, 5);
            let b = vector(20, 6);
            let mag: f64 = a
                .iter()
                .zip(&b)
                .map(|(x, y)| (*x as f64 * *y as f64).abs())
                .sum();
            assert!(close(dot_at(level, &a, &b), ref_dot(&a, &b), mag));
            assert!(close(dot_at(level, &b, &a), ref_dot(&a, &b), mag));
            let lw = ref_l2(&a, &b);
            assert!(close(l2_sq_at(level, &a, &b), lw, lw));
        }
    }

    #[test]
    fn every_offset_of_a_slice_is_read_correctly() {
        // Unaligned starts and every tail length of the wide kernels.
        let a = vector(200, 11);
        let b = vector(200, 12);
        for level in available() {
            for start in 0..8 {
                for len in 0..70 {
                    let (x, y) = (&a[start..start + len], &b[start..start + len]);
                    let mag: f64 = x
                        .iter()
                        .zip(y)
                        .map(|(p, q)| (*p as f64 * *q as f64).abs())
                        .sum();
                    assert!(
                        close(dot_at(level, x, y), ref_dot(x, y), mag),
                        "{level:?} {start} {len}"
                    );
                    let lw = ref_l2(x, y);
                    assert!(
                        close(l2_sq_at(level, x, y), lw, lw),
                        "{level:?} {start} {len}"
                    );
                }
            }
        }
    }

    #[test]
    fn the_detected_level_is_one_this_machine_has() {
        assert!(available().contains(&level()));
        #[cfg(target_arch = "aarch64")]
        assert_eq!(level(), Level::Neon);
    }

    /// Time per call of each kernel at each level: `cargo test --release
    /// --lib simd -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn timing() {
        for &dim in &[128usize, 512, 1536, 2048] {
            let n = 4096;
            let data: Vec<Vec<f32>> = (0..n).map(|i| vector(dim, i as u64)).collect();
            let q = vector(dim, 99_999);
            for level in available() {
                let start = std::time::Instant::now();
                let mut sink = 0f32;
                let rounds = 50;
                for _ in 0..rounds {
                    for v in &data {
                        sink += dot_at(level, &q, v);
                    }
                }
                let per = start.elapsed().as_nanos() as f64 / (rounds * n) as f64;
                let start = std::time::Instant::now();
                for _ in 0..rounds {
                    for v in &data {
                        sink += scalar_dot(&q, v);
                    }
                }
                let scalar = start.elapsed().as_nanos() as f64 / (rounds * n) as f64;
                println!("dim {dim:5} {level:?}: dot {per:8.1} ns (scalar {scalar:8.1} ns) {sink}");
            }
        }
    }
}
