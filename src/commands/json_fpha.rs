//! `JSON.SET … FPHA <FP16 | BF16 | FP32 | FP64>` (Redis 8.8).
//!
//! A *floating-point homogeneous array* is a JSON array whose elements are all
//! numbers — an embedding, typically. JSON's numbers are text, so nothing in
//! `[1, 2, 3]` says whether it came from 16-bit, 32-bit or 64-bit floats.
//! `FPHA` lets the client say: every such array in the value being set is
//! taken to hold floats of the given type.
//!
//! What a client can observe, and what is implemented here:
//!
//! * each element becomes a float — `[1, 2]` reads back as `[1.0, 2.0]`;
//! * each element is rounded to what the type can represent;
//! * an element too large for the type fails the whole command with
//!   `value out of range`, leaving the document untouched.
//!
//! Redis additionally stores such arrays in a packed binary form. That is an
//! internal memory optimisation with no effect on replies; the document here
//! stays an ordinary JSON tree.

use serde_json::{Number, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FpType {
    /// IEEE 754 half precision.
    Fp16,
    /// bfloat16: the top 16 bits of a single-precision float.
    Bf16,
    Fp32,
    Fp64,
}

impl FpType {
    pub fn parse(arg: &[u8]) -> Option<Self> {
        [
            (&b"FP16"[..], FpType::Fp16),
            (&b"BF16"[..], FpType::Bf16),
            (&b"FP32"[..], FpType::Fp32),
            (&b"FP64"[..], FpType::Fp64),
        ]
        .into_iter()
        .find(|(name, _)| arg.eq_ignore_ascii_case(name))
        .map(|(_, ty)| ty)
    }

    /// `value` as this type would hold it, or `None` if it does not fit.
    fn round(self, value: f64) -> Option<f64> {
        if !value.is_finite() {
            return None;
        }
        let single = value as f32;
        let stored: f32 = match self {
            FpType::Fp64 => return Some(value),
            FpType::Fp32 => single,
            FpType::Fp16 => f16_bits_to_f32(f32_to_f16_bits(single)),
            FpType::Bf16 => f32_to_bf16(single),
        };
        if !stored.is_finite() {
            // Finite going in, infinite coming out: it overflowed the type.
            return None;
        }
        // Read the narrow value back through its shortest decimal form, so a
        // single-precision 0.1 is 0.1 and not 0.10000000149011612.
        format!("{stored}").parse().ok()
    }
}

/// Convert every all-numeric array inside `value` to floats of type `ty`.
/// `Err` means some element does not fit; `value` may then be partly
/// converted and must be discarded.
pub(super) fn apply(value: &mut Value, ty: FpType) -> Result<(), ()> {
    match value {
        Value::Array(items) => {
            if !items.is_empty() && items.iter().all(Value::is_number) {
                for item in items.iter_mut() {
                    let rounded = item.as_f64().and_then(|n| ty.round(n)).ok_or(())?;
                    *item = Value::Number(Number::from_f64(rounded).ok_or(())?);
                }
                Ok(())
            } else {
                items.iter_mut().try_for_each(|item| apply(item, ty))
            }
        }
        Value::Object(fields) => fields.values_mut().try_for_each(|item| apply(item, ty)),
        _ => Ok(()),
    }
}

/// Single precision to half precision, rounding to nearest, ties to even.
pub(crate) fn f32_to_f16_bits(value: f32) -> u16 {
    let bits = value.to_bits();
    let sign = ((bits >> 16) & 0x8000) as u16;
    let exponent = ((bits >> 23) & 0xff) as i32;
    let mantissa = bits & 0x007f_ffff;

    if exponent == 0xff {
        // Infinity or NaN.
        return sign | 0x7c00 | if mantissa != 0 { 0x0200 } else { 0 };
    }
    let half_exponent = exponent - 127 + 15;
    if half_exponent >= 0x1f {
        // Too large: infinity.
        return sign | 0x7c00;
    }
    if half_exponent <= 0 {
        // Subnormal in half precision, or too small to represent at all.
        if half_exponent < -10 {
            return sign;
        }
        let mantissa = mantissa | 0x0080_0000;
        let shift = (14 - half_exponent) as u32;
        let half_mantissa = (mantissa >> shift) as u16;
        let round_bit = 1u32 << (shift - 1);
        if mantissa & round_bit != 0 && mantissa & (3 * round_bit - 1) != 0 {
            return sign | (half_mantissa + 1);
        }
        return sign | half_mantissa;
    }
    let half = sign | ((half_exponent as u16) << 10) | (mantissa >> 13) as u16;
    let round_bit = 0x1000u32;
    if mantissa & round_bit != 0 && mantissa & (3 * round_bit - 1) != 0 {
        // May carry into the exponent, which is exactly what should happen
        // (and at the top of the range, yields infinity).
        half + 1
    } else {
        half
    }
}

pub(crate) fn f16_bits_to_f32(half: u16) -> f32 {
    let sign = ((half & 0x8000) as u32) << 16;
    let exponent = (half & 0x7c00) as u32;
    let mantissa = (half & 0x03ff) as u32;

    if exponent == 0x7c00 {
        return if mantissa == 0 {
            f32::from_bits(sign | 0x7f80_0000)
        } else {
            f32::NAN
        };
    }
    if exponent == 0 {
        if mantissa == 0 {
            return f32::from_bits(sign);
        }
        // Subnormal: normalise.
        let shift = (mantissa as u16).leading_zeros() - 6;
        let exponent = (127 - 15 - shift) << 23;
        let mantissa = (mantissa << (14 + shift)) & 0x007f_ffff;
        return f32::from_bits(sign | exponent | mantissa);
    }
    let exponent = ((exponent >> 10) + 127 - 15) << 23;
    f32::from_bits(sign | exponent | (mantissa << 13))
}

/// Single precision rounded to bfloat16 (nearest, ties to even), returned as
/// the single-precision value those 16 bits stand for.
pub(crate) fn f32_to_bf16(value: f32) -> f32 {
    let bits = value.to_bits();
    let rounded = bits.wrapping_add(0x7fff + ((bits >> 16) & 1));
    f32::from_bits(rounded & 0xffff_0000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn half(v: f32) -> f32 {
        f16_bits_to_f32(f32_to_f16_bits(v))
    }

    #[test]
    fn half_precision_round_trips_what_it_can_hold() {
        for v in [0.0f32, 1.0, -1.0, 0.5, 2.0, 4000.0, 65504.0, -65504.0, 0.25] {
            assert_eq!(half(v), v, "{v}");
        }
        assert_eq!(f32_to_f16_bits(1.0), 0x3c00);
        assert_eq!(f32_to_f16_bits(-2.0), 0xc000);
        assert_eq!(f32_to_f16_bits(65504.0), 0x7bff);
        // The smallest subnormal, and something below it.
        assert_eq!(half(5.960_464_5e-8), 5.960_464_5e-8);
        assert_eq!(half(1e-10), 0.0);
        // Not representable: rounds to the nearest half.
        assert_eq!(half(0.1), 0.099_975_586);
        // Past the largest half: infinity.
        assert!(half(65520.0).is_infinite());
        assert!(half(1e6).is_infinite());
    }

    #[test]
    fn bfloat16_keeps_the_top_sixteen_bits() {
        assert_eq!(f32_to_bf16(1.0), 1.0);
        assert_eq!(f32_to_bf16(-3.0), -3.0);
        assert_eq!(f32_to_bf16(0.1).to_bits() & 0xffff, 0);
        assert!((f32_to_bf16(0.1) - 0.1).abs() < 1e-3);
        // bfloat16 has single precision's range.
        assert!(f32_to_bf16(1e30).is_finite());
        assert!(f32_to_bf16(f32::MAX).is_infinite());
    }

    /// The example on the Redis `JSON.SET` page.
    #[test]
    fn numeric_arrays_become_float_arrays() {
        let mut v = json!([[1, 2, 3, 4e3], [5, 6.0, 7, 8]]);
        apply(&mut v, FpType::Fp16).unwrap();
        assert_eq!(v.to_string(), "[[1.0,2.0,3.0,4000.0],[5.0,6.0,7.0,8.0]]");
    }

    #[test]
    fn only_all_numeric_arrays_are_touched() {
        let mut v = json!({"emb": [1, 2], "mixed": [1, "a"], "n": 7, "deep": {"e": [3]}});
        apply(&mut v, FpType::Fp32).unwrap();
        assert_eq!(v["emb"].to_string(), "[1.0,2.0]");
        assert_eq!(v["mixed"].to_string(), r#"[1,"a"]"#);
        assert_eq!(v["n"].to_string(), "7");
        assert_eq!(v["deep"]["e"].to_string(), "[3.0]");
    }

    #[test]
    fn values_are_rounded_to_the_type() {
        let rounded = |ty: FpType| {
            let mut v = json!([0.1]);
            apply(&mut v, ty).unwrap();
            v.to_string()
        };
        assert_eq!(rounded(FpType::Fp64), "[0.1]");
        assert_eq!(rounded(FpType::Fp32), "[0.1]");
        assert_eq!(rounded(FpType::Fp16), "[0.099975586]");
    }

    #[test]
    fn a_value_too_large_for_the_type_is_an_error() {
        assert!(apply(&mut json!([1e6]), FpType::Fp16).is_err());
        assert!(apply(&mut json!([1e6]), FpType::Bf16).is_ok());
        assert!(apply(&mut json!([1e39]), FpType::Fp32).is_err());
        assert!(apply(&mut json!([1e39]), FpType::Bf16).is_err());
        assert!(apply(&mut json!([1e39]), FpType::Fp64).is_ok());
        assert!(apply(&mut json!({"a": [1, 1e6]}), FpType::Fp16).is_err());
    }

    #[test]
    fn type_names() {
        assert_eq!(FpType::parse(b"fp16"), Some(FpType::Fp16));
        assert_eq!(FpType::parse(b"BF16"), Some(FpType::Bf16));
        assert_eq!(FpType::parse(b"FP32"), Some(FpType::Fp32));
        assert_eq!(FpType::parse(b"FP64"), Some(FpType::Fp64));
        assert_eq!(FpType::parse(b"FP8"), None);
    }
}
