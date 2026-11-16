//! The numbers of `INCRBYFLOAT` and `HINCRBYFLOAT`, as Redis has them on
//! x86-64: C's `long double`, the x87 80-bit extended format, with a 64-bit
//! significand (#387).
//!
//! Redis reads both numbers with `strtold`, adds them in `long double` and
//! writes the sum with 17 digits after the point, less the zeros at the end.
//! That is why `0.1 + 0.2` is `0.3` there: the sum is rounded to 64 bits,
//! not to a double's 53, and the 17 digits leave out the error. In a double
//! the same sum is `0.30000000000000004`. And it is why the format reads
//! numbers no double can hold (`1e4000`) and writes a large one with all its
//! digits.
//!
//! Rust has no such type, so here it is done by hand: a value is a 64-bit
//! integer times a power of two, and reading, adding and writing are exact
//! on big integers and rounded once, to nearest, ties to even, as the x87
//! and glibc round. The usual numbers, of up to 19 digits and not far from
//! 1, are read, added and written in 64- and 128-bit words, with the same
//! rounding; anything else goes the long way, on big integers.

use std::cmp::Ordering;
use std::fmt::Write as _;

/// The exponent of the lowest bit of the smallest subnormal: 2^-16445.
const MIN_EXP: i64 = -16445;
/// A value of 2^16384 or more is past the largest, and is infinite.
const MAX_TOP: i64 = 16384;
/// Redis refuses a number of this many bytes or more
/// (`MAX_LONG_DOUBLE_CHARS`).
const MAX_CHARS: usize = 5 * 1024;
/// The digits written after the point (`%.17Lf`).
const DECIMALS: u32 = 17;
/// 10^DECIMALS, under 2^57.
const SCALE: u128 = 10u128.pow(DECIMALS);
/// 10^n, n up to 38: every power of ten a `u128` holds.
const POW10: [u128; 39] = {
    let mut table = [1u128; 39];
    let mut at = 1;
    while at < table.len() {
        table[at] = table[at - 1] * 10;
        at += 1;
    }
    table
};

/// A finite value: `-1^neg × mant × 2^exp`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LongDouble {
    neg: bool,
    mant: u64,
    exp: i64,
}

/// A number as `strtold` reads it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Parsed {
    Finite(LongDouble),
    /// `inf`, which Redis reads. What it is added to is then infinite too.
    Infinite,
}

impl LongDouble {
    const ZERO: LongDouble = LongDouble {
        neg: false,
        mant: 0,
        exp: 0,
    };

    /// `text` as Redis reads a `long double` (`string2ld`): what `strtold`
    /// reads, all of it, but not with a space first, not NaN, and not a
    /// number too large for the type, or one so small it is read as zero.
    pub(crate) fn parse(text: &[u8]) -> Option<Parsed> {
        if text.is_empty() || text.len() >= MAX_CHARS || is_space(text[0]) {
            return None;
        }
        let (neg, rest) = match text[0] {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        if rest.eq_ignore_ascii_case(b"inf") || rest.eq_ignore_ascii_case(b"infinity") {
            return Some(Parsed::Infinite);
        }
        let rounded = if rest.len() > 2 && rest[0] == b'0' && (rest[1] | 0x20) == b'x' {
            parse_hex(neg, &rest[2..])?
        } else {
            parse_decimal(neg, rest)?
        };
        match rounded {
            Rounded::Value(value) => Some(Parsed::Finite(value)),
            // `strtold` gives ±HUGE_VALL or zero with ERANGE, and Redis
            // refuses both. A subnormal it keeps.
            Rounded::Overflow | Rounded::Underflow => None,
        }
    }

    /// `self + other`, or `None` if the sum is infinite.
    pub(crate) fn add(self, other: LongDouble) -> Option<LongDouble> {
        if self.mant == 0 {
            return Some(other);
        }
        if other.mant == 0 {
            return Some(self);
        }
        match self.add_in_words(other) {
            Some(sum) => Some(sum),
            None => self.add_long(other),
        }
    }

    /// [`LongDouble::add`] on big integers, for any two values but zero.
    fn add_long(self, other: LongDouble) -> Option<LongDouble> {
        let exp = self.exp.min(other.exp);
        let a = Big::from_u64(self.mant).shl((self.exp - exp) as usize);
        let b = Big::from_u64(other.mant).shl((other.exp - exp) as usize);
        let (neg, sum) = if self.neg == other.neg {
            (self.neg, a.add(&b))
        } else {
            match a.cmp(&b) {
                Ordering::Equal => return Some(LongDouble::ZERO),
                Ordering::Greater => (self.neg, a.sub(&b)),
                Ordering::Less => (other.neg, b.sub(&a)),
            }
        };
        match round(neg, sum, exp, false) {
            Rounded::Value(value) => Some(value),
            // A sum of two values is a multiple of the smallest subnormal:
            // it is never rounded to zero.
            Rounded::Underflow => Some(LongDouble::ZERO),
            Rounded::Overflow => None,
        }
    }

    /// [`LongDouble::add`] of two values whose lowest bits are no more than
    /// 63 apart, both lined up in a `u128`, and far from the ends of the
    /// range: `None` for the others. Neither is zero.
    fn add_in_words(self, other: LongDouble) -> Option<LongDouble> {
        let exp = self.exp.min(other.exp);
        let (a_shift, b_shift) = (self.exp - exp, other.exp - exp);
        // The sum is under 2^128, and rounded to 64 bits it is a normal
        // value, nowhere near infinite (a subnormal one is all there is
        // below `exp`, which a value read has already).
        if a_shift > 63 || b_shift > 63 || exp > MAX_TOP - 200 {
            return None;
        }
        let a = u128::from(self.mant) << a_shift;
        let b = u128::from(other.mant) << b_shift;
        let (neg, sum) = if self.neg == other.neg {
            (self.neg, a + b)
        } else {
            match a.cmp(&b) {
                Ordering::Equal => return Some(LongDouble::ZERO),
                Ordering::Greater => (self.neg, a - b),
                Ordering::Less => (other.neg, b - a),
            }
        };
        Some(round_u128(neg, sum, exp, false))
    }

    /// As Redis writes it (`ld2string`, `LD_STR_HUMAN`): `%.17Lf`, the zeros
    /// at the end of the fraction and then a point at the end left out, and
    /// `-0` as `0`.
    pub(crate) fn to_redis_string(self) -> String {
        match self.to_redis_string_in_words() {
            Some(text) => text,
            None => self.to_redis_string_long(),
        }
    }

    /// [`LongDouble::to_redis_string`] on big integers, for any value.
    fn to_redis_string_long(self) -> String {
        // The value times 10^17, rounded to an integer, ties to even.
        let scaled = Big::from_u64(self.mant).mul_pow10(DECIMALS);
        let whole = if self.exp >= 0 {
            scaled.shl(self.exp as usize)
        } else {
            let shift = (-self.exp) as usize;
            let (mut kept, half, below) = scaled.shr_rounding(shift);
            if half && (below || kept.is_odd()) {
                kept = kept.add(&Big::from_u64(1));
            }
            kept
        };
        if whole.is_zero() {
            return "0".to_string();
        }
        let digits = whole.to_decimal();
        let digits = format!("{digits:0>18}");
        let (int, frac) = digits.split_at(digits.len() - DECIMALS as usize);
        let frac = frac.trim_end_matches('0');
        let mut out = String::with_capacity(digits.len() + 2);
        if self.neg {
            out.push('-');
        }
        out.push_str(int);
        if !frac.is_empty() {
            out.push('.');
            out.push_str(frac);
        }
        out
    }

    /// [`LongDouble::to_redis_string`] of a value whose 10^17 times fits in
    /// a `u128` (under 3.4e21), or `None`.
    fn to_redis_string_in_words(self) -> Option<String> {
        let whole = if self.exp >= 0 {
            let bits = i64::from(64 - self.mant.leading_zeros());
            // 10^17 < 2^57.
            if bits + self.exp + 57 > 128 {
                return None;
            }
            (u128::from(self.mant) << self.exp) * SCALE
        } else {
            // Under 2^121, so under half of 2^128 and past.
            let scaled = u128::from(self.mant) * SCALE;
            let shift = self.exp.unsigned_abs();
            if shift >= 128 {
                0
            } else {
                let shift = shift as u32;
                let kept = scaled >> shift;
                let half = (scaled >> (shift - 1)) & 1 == 1;
                let below = scaled & ((1u128 << (shift - 1)) - 1) != 0;
                if half && (below || kept & 1 == 1) {
                    kept + 1
                } else {
                    kept
                }
            }
        };
        if whole == 0 {
            return Some("0".to_string());
        }
        let (int, mut frac) = (whole / SCALE, (whole % SCALE) as u64);
        let mut out = String::with_capacity(48);
        if self.neg {
            out.push('-');
        }
        match u64::try_from(int) {
            Ok(int) => write!(out, "{int}"),
            Err(_) => write!(out, "{int}"),
        }
        .ok()?;
        if frac != 0 {
            let mut places = DECIMALS as usize;
            while frac % 10 == 0 {
                frac /= 10;
                places -= 1;
            }
            write!(out, ".{frac:0places$}").ok()?;
        }
        Some(out)
    }
}

/// `isspace` in the C locale.
fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// The exponent after `e` or `p`: a sign and at least one digit. Past a
/// billion it is a billion: no number here reaches that far.
fn parse_exponent(text: &[u8]) -> Option<i64> {
    let (neg, digits) = match text.first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = digits.iter().fold(0i64, |acc, &d| {
        (acc * 10 + i64::from(d - b'0')).min(1_000_000_000)
    });
    Some(if neg { -value } else { value })
}

/// The digits of a number, before and after its point, and what follows
/// them, or `None` if there is no digit.
fn split_mantissa(text: &[u8], is_digit: fn(&u8) -> bool) -> Option<(&[u8], &[u8], &[u8])> {
    let int_len = text.iter().take_while(|b| is_digit(b)).count();
    let (int, rest) = text.split_at(int_len);
    let (frac, rest) = match rest.split_first() {
        Some((b'.', after)) => {
            let frac_len = after.iter().take_while(|b| is_digit(b)).count();
            after.split_at(frac_len)
        }
        _ => (&rest[..0], rest),
    };
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    Some((int, frac, rest))
}

fn parse_decimal(neg: bool, text: &[u8]) -> Option<Rounded> {
    let (int, frac, rest) = split_mantissa(text, u8::is_ascii_digit)?;
    let exp10 = match rest.split_first() {
        None => 0,
        Some((b'e' | b'E', after)) => parse_exponent(after)?,
        Some(_) => return None,
    };
    if let Some(rounded) = parse_decimal_in_words(neg, int, frac, exp10) {
        return Some(rounded);
    }
    parse_decimal_long(neg, int, frac, exp10)
}

/// [`parse_decimal`] on big integers, for any number: its digits before and
/// after the point, and the exponent after them.
fn parse_decimal_long(neg: bool, int: &[u8], frac: &[u8], exp10: i64) -> Option<Rounded> {
    // The value is `digits × 10^exp10`, its digits without leading zeros.
    let all: Vec<u8> = int.iter().chain(frac).copied().collect();
    let first = all.iter().position(|&d| d != b'0');
    let Some(first) = first else {
        return Some(Rounded::Value(LongDouble {
            neg,
            ..LongDouble::ZERO
        }));
    };
    let digits = &all[first..];
    let exp10 = exp10 - frac.len() as i64;
    // 10^(magnitude-1) ≤ value < 10^magnitude. Past the largest value
    // (1.19e4932), or under half the smallest (1.8e-4951), without the
    // arithmetic.
    let magnitude = exp10 + digits.len() as i64;
    if magnitude > 4933 {
        return Some(Rounded::Overflow);
    }
    if magnitude <= -4951 {
        return Some(Rounded::Underflow);
    }
    let mantissa = Big::from_decimal(digits);
    if exp10 >= 0 {
        return Some(round(neg, mantissa.mul_pow10(exp10 as u32), 0, false));
    }
    // digits / 10^-exp10, the one or the other shifted so that the quotient
    // has 66 or 67 bits, two more than are kept; the remainder says whether
    // anything is left below them.
    let divisor = Big::from_u64(1).mul_pow10((-exp10) as u32);
    let shift = 66 + divisor.bits() as i64 - mantissa.bits() as i64;
    let (dividend, divisor) = if shift >= 0 {
        (mantissa.shl(shift as usize), divisor)
    } else {
        (mantissa, divisor.shl((-shift) as usize))
    };
    let (quotient, remainder) = dividend.div_small_quotient(&divisor, 68);
    Some(round(neg, quotient, -shift, !remainder.is_zero()))
}

/// [`parse_decimal`] of the usual numbers, in machine words: at most 19
/// digits, leading zeros aside (10^19 < 2^64), times a power of ten from
/// 10^-18 to what a `u128` holds. `None` for the others.
///
/// A whole number is exact in a `u128` and rounded once. A fraction is the
/// digits, shifted to the top of a `u128`, divided by 10^k (k ≤ 18, under
/// 2^60): a quotient of 68 bits or more, four past the 64 kept, and the
/// remainder says whether anything is left below them.
fn parse_decimal_in_words(neg: bool, int: &[u8], frac: &[u8], exp10: i64) -> Option<Rounded> {
    let mut digits = 0u64;
    let mut count = 0;
    for &digit in int.iter().chain(frac) {
        if digits == 0 && digit == b'0' {
            continue;
        }
        count += 1;
        if count > 19 {
            return None;
        }
        digits = digits * 10 + u64::from(digit - b'0');
    }
    if digits == 0 {
        return Some(Rounded::Value(LongDouble {
            neg,
            ..LongDouble::ZERO
        }));
    }
    let exp10 = exp10 - frac.len() as i64;
    let value = if exp10 >= 0 {
        let power = *POW10.get(usize::try_from(exp10).ok()?)?;
        round_u128(neg, u128::from(digits).checked_mul(power)?, 0, false)
    } else {
        let power = *POW10
            .get(usize::try_from(-exp10).ok()?)
            .filter(|_| exp10 >= -18)?;
        let shift = u128::from(digits).leading_zeros();
        let dividend = u128::from(digits) << shift;
        round_u128(
            neg,
            dividend / power,
            -i64::from(shift),
            dividend % power != 0,
        )
    };
    Some(Rounded::Value(value))
}

fn parse_hex(neg: bool, text: &[u8]) -> Option<Rounded> {
    let (int, frac, rest) = split_mantissa(text, u8::is_ascii_hexdigit)?;
    let exp2 = match rest.split_first() {
        None => 0,
        Some((b'p' | b'P', after)) => parse_exponent(after)?,
        Some(_) => return None,
    };
    let mut mantissa = Big(Vec::with_capacity((int.len() + frac.len()) / 8 + 1));
    for &digit in int.iter().chain(frac) {
        let value = (digit as char).to_digit(16).unwrap_or(0);
        mantissa.mul_add_small(16, value);
    }
    if mantissa.is_zero() {
        return Some(Rounded::Value(LongDouble {
            neg,
            ..LongDouble::ZERO
        }));
    }
    Some(round(neg, mantissa, exp2 - 4 * frac.len() as i64, false))
}

enum Rounded {
    Value(LongDouble),
    Overflow,
    /// Not zero, but rounded to it.
    Underflow,
}

/// `value × 2^exp`, and a little more if `sticky`, rounded to the format: to
/// 64 bits, or to fewer for a subnormal. `value` is not zero, and if
/// `sticky`, it has two bits more than are kept.
fn round(neg: bool, value: Big, exp: i64, sticky: bool) -> Rounded {
    let bits = value.bits() as i64;
    // value < 2^top.
    let top = exp + bits;
    if top > MAX_TOP {
        return Rounded::Overflow;
    }
    // Under half the smallest subnormal: zero.
    if top < MIN_EXP {
        return Rounded::Underflow;
    }
    // The exponent of the lowest bit kept.
    let lowest = (top - 64).max(MIN_EXP);
    let (mant, exp) = if lowest <= exp {
        // Every bit fits.
        (value.low_u64(), exp)
    } else {
        let (kept, half, below) = value.shr_rounding((lowest - exp) as usize);
        let mut mant = kept.low_u64();
        let mut exp = lowest;
        if half && (below || sticky || mant & 1 == 1) {
            match mant.checked_add(1) {
                Some(next) => mant = next,
                None => {
                    mant = 1 << 63;
                    exp += 1;
                }
            }
        }
        if mant == 0 {
            return Rounded::Underflow;
        }
        if exp + 64 - i64::from(mant.leading_zeros()) > MAX_TOP {
            return Rounded::Overflow;
        }
        (mant, exp)
    };
    Rounded::Value(LongDouble { neg, mant, exp })
}

/// [`round`] of a value held in a `u128` and far from the ends of the
/// range, where the format's limits do not come in: to 64 bits, to nearest,
/// ties to even. If `sticky`, `value` has more than 64 bits.
fn round_u128(neg: bool, value: u128, exp: i64, sticky: bool) -> LongDouble {
    let bits = 128 - value.leading_zeros();
    if bits <= 64 {
        return LongDouble {
            neg,
            mant: value as u64,
            exp,
        };
    }
    let shift = bits - 64;
    let mut mant = (value >> shift) as u64;
    let mut exp = exp + i64::from(shift);
    let half = (value >> (shift - 1)) & 1 == 1;
    let below = sticky || value & ((1u128 << (shift - 1)) - 1) != 0;
    if half && (below || mant & 1 == 1) {
        match mant.checked_add(1) {
            Some(next) => mant = next,
            None => {
                mant = 1 << 63;
                exp += 1;
            }
        }
    }
    LongDouble { neg, mant, exp }
}

/// An unsigned integer of any size: 32-bit limbs, the lowest first, with no
/// zero limb at the top.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Big(Vec<u32>);

impl Big {
    fn from_u64(value: u64) -> Big {
        let mut big = Big(vec![value as u32, (value >> 32) as u32]);
        big.trim();
        big
    }

    fn from_u128(value: u128) -> Big {
        let mut big = Big(vec![
            value as u32,
            (value >> 32) as u32,
            (value >> 64) as u32,
            (value >> 96) as u32,
        ]);
        big.trim();
        big
    }

    fn from_decimal(digits: &[u8]) -> Big {
        // 9 digits to a limb, a little under 32 bits' worth.
        let mut big = Big(Vec::with_capacity(digits.len() / 9 + 2));
        for chunk in digits.chunks(9) {
            let value = chunk
                .iter()
                .fold(0u32, |acc, &d| acc * 10 + u32::from(d - b'0'));
            big.mul_add_small(10u32.pow(chunk.len() as u32), value);
        }
        big
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn is_odd(&self) -> bool {
        self.0.first().is_some_and(|limb| limb & 1 == 1)
    }

    fn bits(&self) -> usize {
        match self.0.last() {
            Some(top) => self.0.len() * 32 - top.leading_zeros() as usize,
            None => 0,
        }
    }

    fn low_u64(&self) -> u64 {
        let limb = |at: usize| u64::from(self.0.get(at).copied().unwrap_or(0));
        limb(0) | (limb(1) << 32)
    }

    fn bit(&self, at: usize) -> bool {
        self.0
            .get(at / 32)
            .is_some_and(|limb| limb >> (at % 32) & 1 == 1)
    }

    /// `self × factor + addend`, in place. `factor` is not zero.
    fn mul_add_small(&mut self, factor: u32, addend: u32) {
        let mut carry = u64::from(addend);
        for limb in &mut self.0 {
            let product = u64::from(*limb) * u64::from(factor) + carry;
            *limb = product as u32;
            carry = product >> 32;
        }
        if carry != 0 {
            self.0.push(carry as u32);
        }
    }

    fn mul_pow10(mut self, mut exp: u32) -> Big {
        self.0.reserve(exp as usize / 9 + 1);
        while exp >= 9 {
            self.mul_add_small(1_000_000_000, 0);
            exp -= 9;
        }
        self.mul_add_small(10u32.pow(exp), 0);
        self
    }

    fn add(&self, other: &Big) -> Big {
        let len = self.0.len().max(other.0.len());
        let mut out = Vec::with_capacity(len + 1);
        let mut carry = 0u64;
        for at in 0..len {
            let sum = u64::from(self.0.get(at).copied().unwrap_or(0))
                + u64::from(other.0.get(at).copied().unwrap_or(0))
                + carry;
            out.push(sum as u32);
            carry = sum >> 32;
        }
        out.push(carry as u32);
        let mut big = Big(out);
        big.trim();
        big
    }

    /// `self - other`, `other` being no larger.
    fn sub(&self, other: &Big) -> Big {
        let mut big = self.clone();
        big.sub_assign(other);
        big
    }

    /// `self -= other`, `other` being no larger.
    fn sub_assign(&mut self, other: &Big) {
        let mut borrow = 0i64;
        for (at, limb) in self.0.iter_mut().enumerate() {
            let take = other.0.get(at).copied();
            if take.is_none() && borrow == 0 {
                break;
            }
            let mut diff = i64::from(*limb) - i64::from(take.unwrap_or(0)) - borrow;
            borrow = 0;
            if diff < 0 {
                diff += 1 << 32;
                borrow = 1;
            }
            *limb = diff as u32;
        }
        self.trim();
    }

    /// `self >>= 1`.
    fn shr1_assign(&mut self) {
        let mut carry = 0u32;
        for limb in self.0.iter_mut().rev() {
            let low = *limb << 31;
            *limb = (*limb >> 1) | carry;
            carry = low;
        }
        self.trim();
    }

    fn cmp(&self, other: &Big) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.iter().rev().cmp(other.0.iter().rev()))
    }

    fn shl(&self, bits: usize) -> Big {
        if self.is_zero() {
            return Big(Vec::new());
        }
        let (limbs, bits) = (bits / 32, bits % 32);
        let mut out = vec![0u32; limbs];
        if bits == 0 {
            out.extend_from_slice(&self.0);
        } else {
            let mut carry = 0u32;
            for &limb in &self.0 {
                out.push((limb << bits) | carry);
                carry = limb >> (32 - bits);
            }
            out.push(carry);
        }
        let mut big = Big(out);
        big.trim();
        big
    }

    /// `self >> bits`, the highest bit shifted out, and whether any below it
    /// was set.
    fn shr_rounding(&self, bits: usize) -> (Big, bool, bool) {
        if bits == 0 {
            return (self.clone(), false, false);
        }
        let half = self.bit(bits - 1);
        let below_limbs = (bits - 1) / 32;
        let below = self.0.iter().take(below_limbs).any(|&limb| limb != 0)
            || self
                .0
                .get(below_limbs)
                .is_some_and(|limb| limb & ((1u32 << ((bits - 1) % 32)) - 1) != 0);
        let (limbs, shift) = (bits / 32, bits % 32);
        if limbs >= self.0.len() {
            return (Big(Vec::new()), half, below);
        }
        let mut out = Vec::with_capacity(self.0.len() - limbs);
        for at in limbs..self.0.len() {
            let low = self.0[at] >> shift;
            let high = if shift == 0 {
                0
            } else {
                self.0.get(at + 1).map_or(0, |&next| next << (32 - shift))
            };
            out.push(low | high);
        }
        let mut big = Big(out);
        big.trim();
        (big, half, below)
    }

    /// `self / divisor` and its remainder, for a quotient of at most
    /// `quotient_bits` bits (128 at most): one bit at a time, which for a
    /// quotient that small is as quick as the long way. In place: the
    /// divisor is shifted once to the top and then down a bit a step, and
    /// what is left of `self` is the remainder.
    fn div_small_quotient(mut self, divisor: &Big, quotient_bits: usize) -> (Big, Big) {
        let mut shifted = divisor.shl(quotient_bits - 1);
        let mut quotient = 0u128;
        for bit in (0..quotient_bits).rev() {
            if self.cmp(&shifted) != Ordering::Less {
                self.sub_assign(&shifted);
                quotient |= 1 << bit;
            }
            shifted.shr1_assign();
        }
        (Big::from_u128(quotient), self)
    }

    /// The decimal digits, without leading zeros.
    fn to_decimal(&self) -> String {
        let mut limbs = self.0.clone();
        let mut chunks = Vec::new();
        while !limbs.is_empty() {
            let mut rest = 0u64;
            for limb in limbs.iter_mut().rev() {
                let value = (rest << 32) | u64::from(*limb);
                *limb = (value / 1_000_000_000) as u32;
                rest = value % 1_000_000_000;
            }
            chunks.push(rest as u32);
            while limbs.last() == Some(&0) {
                limbs.pop();
            }
        }
        let mut out = String::new();
        for (at, chunk) in chunks.iter().rev().enumerate() {
            if at == 0 {
                out.push_str(&chunk.to_string());
            } else {
                out.push_str(&format!("{chunk:09}"));
            }
        }
        if out.is_empty() {
            out.push('0');
        }
        out
    }
}

/// `INCRBYFLOAT`'s and `HINCRBYFLOAT`'s sum of `current` (what is stored, or
/// `None` for nothing) and `increment`, as Redis writes it.
pub(crate) enum Increment {
    Sum(String),
    /// `ERR increment would produce NaN or Infinity`.
    NotFinite,
}

/// Add `increment` to `current`, both already read.
pub(crate) fn increment(current: Parsed, increment: Parsed) -> Increment {
    match (current, increment) {
        (Parsed::Finite(a), Parsed::Finite(b)) => match a.add(b) {
            Some(sum) => Increment::Sum(sum.to_redis_string()),
            None => Increment::NotFinite,
        },
        _ => Increment::NotFinite,
    }
}

/// Nothing stored: zero.
pub(crate) const ZERO: Parsed = Parsed::Finite(LongDouble::ZERO);

#[cfg(test)]
mod tests {
    use super::*;

    fn incr(current: &str, by: &str) -> String {
        let a = LongDouble::parse(current.as_bytes()).expect(current);
        let b = LongDouble::parse(by.as_bytes()).expect(by);
        match increment(a, b) {
            Increment::Sum(sum) => sum,
            Increment::NotFinite => "NotFinite".to_string(),
        }
    }

    /// What Redis 8.10.2 on x86-64 replied to `SET k a` then
    /// `INCRBYFLOAT k b`.
    #[test]
    fn sums_are_what_redis_replies() {
        let long_1e300 = "1000000000000000000008997324079559193870523944273290747938260082321265646596180935755849152083750497190350372508614274835903592556184672983913096260520748646287327135641843653294084255107606016789726652932370030551382947620994540294772781889620606179267611627097410650567187386105690089424915104006144";
        let max_double_twice = "359538626972463139991842093548208868096772893888658356970628841530353257046248792709403737216774171128857586838851041378617416726902272110715794556052955234146997955372576789671236326588091188869859074894581254961338834769297758245552436667197907705664206565009503223382416235440319971852753604933726679072768";
        for (current, by, sum) in [
            ("0.1", "0.2", "0.3"),
            ("0.3", "1e-17", "0.30000000000000001"),
            ("0", "1e20", "100000000000000000000"),
            ("100000000000000000000", "0.1", "100000000000000000000"),
            ("0", "1e300", long_1e300),
            ("0.00001", "1e-20", "0.00001"),
            ("0", "1e-20", "0"),
            ("0", "-0", "0"),
            ("-0", "0", "0"),
            ("3.0", "1.5e0", "4.5"),
            ("10.5", "0.1", "10.6"),
            ("0.07", "0.07", "0.14"),
            ("1.1", "2.2", "3.3"),
            ("123456789.123456789", "0", "123456789.12345678899873747"),
            ("12345678901234567890", "1", "12345678901234567891"),
            ("0", "0x10", "16"),
            ("16", "0x1p-3", "16.125"),
            ("16.125", ".5", "16.625"),
            ("16.625", "5.", "21.625"),
            ("21.625", "+2", "23.625"),
            ("23.625", "1e-4940", "23.625"),
            ("0", "5e-18", "0"),
            ("0", "1.5e-17", "0.00000000000000002"),
            ("0", "2.5e-17", "0.00000000000000002"),
            ("1", "-1", "0"),
            ("0.3", "-0.1", "0.2"),
            ("3", "0.000000000000000001", "3"),
            ("99999999999999999", "0.5", "99999999999999999.5"),
            (
                "1.7976931348623157e308",
                "1.7976931348623157e308",
                max_double_twice,
            ),
            ("0.9", "0.1", "1"),
            ("0.7", "0.1", "0.8"),
            ("5.0e3", "2.0e2", "5200"),
            (
                "1234567.1234567",
                "7654321.7654321",
                "8888888.8888888000001316",
            ),
            ("-1.5", "0.25", "-1.25"),
            ("-0.1", "0", "-0.1"),
            ("0", "-1e-20", "0"),
            ("1e4000", "inf", "NotFinite"),
        ] {
            assert_eq!(incr(current, by), sum, "{current} + {by}");
        }
        assert_eq!(incr("1e4000", "1").len(), 4000);
        assert!(incr("1e4000", "1").starts_with("99999999999999999999654638730996"));
        assert_eq!(incr("1e400", "0").len(), 401);
    }

    #[test]
    fn what_redis_refuses_to_read() {
        for text in [
            "", " 1", "1 ", "\t1", "1e", "1e+", "1_0", "nan", "NaN", "-nan", "1e5000", "1e-5000",
            "0x", "0xg", ".", "e5", "1.2.3", "abc", "--1", "inf1",
        ] {
            assert_eq!(LongDouble::parse(text.as_bytes()), None, "{text:?}");
        }
        assert_eq!(LongDouble::parse(&vec![b'1'; MAX_CHARS]), None);
        // 5119 bytes are read: `000…001` is 1.
        let mut longest = vec![b'0'; MAX_CHARS - 1];
        *longest.last_mut().unwrap() = b'1';
        assert!(LongDouble::parse(&longest).is_some());
        for text in ["inf", "-inf", "+INF", "Infinity"] {
            assert_eq!(
                LongDouble::parse(text.as_bytes()),
                Some(Parsed::Infinite),
                "{text}"
            );
        }
        for text in [
            "0e-99999",
            "0x0p99999",
            "-0",
            "1e-4950",
            "0X1P-16445",
            "1.18973e4932",
        ] {
            assert!(LongDouble::parse(text.as_bytes()).is_some(), "{text}");
        }
        // Under half the smallest subnormal, and past the largest value.
        for text in ["0x1p-16447", "1.19e4932", "0x1p16384"] {
            assert_eq!(LongDouble::parse(text.as_bytes()), None, "{text}");
        }
    }

    #[test]
    fn the_sum_past_the_largest_value_is_not_finite() {
        let max = Parsed::Finite(LongDouble {
            neg: false,
            mant: u64::MAX,
            exp: 16320,
        });
        assert!(matches!(increment(max, max), Increment::NotFinite));
        assert!(matches!(increment(max, ZERO), Increment::Sum(_)));
    }

    /// The words and the big integers give the same, in reading, adding and
    /// writing, for what both take.
    #[test]
    fn the_short_way_and_the_long_way_agree() {
        // The same value, however its bits are written.
        let canonical = |v: LongDouble| {
            if v.mant == 0 {
                return (false, 0, 0);
            }
            let zeros = v.mant.trailing_zeros();
            (v.neg, v.mant >> zeros, v.exp + i64::from(zeros))
        };
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut number = || {
            let digit = |n: u64| char::from(b'0' + (n % 10) as u8);
            let int_len = (next() % 12) as usize;
            let frac_len = (next() % 14) as usize;
            let mut text: String = (0..int_len).map(|_| digit(next())).collect();
            text.push('.');
            text.extend((0..frac_len).map(|_| digit(next())));
            if text == "." {
                text = "7".to_string();
            }
            if next() % 3 == 0 {
                write!(text, "e{}", (next() % 50) as i64 - 25).unwrap();
            }
            (next() % 2 == 0, text)
        };
        let mut short_reads = 0;
        let mut short_sums = 0;
        for _ in 0..20_000 {
            let (neg, text) = number();
            let (int, frac, rest) = split_mantissa(text.as_bytes(), u8::is_ascii_digit).unwrap();
            let exp10 = match rest.split_first() {
                None => 0,
                Some((_, after)) => parse_exponent(after).unwrap(),
            };
            let Some(Rounded::Value(long)) = parse_decimal_long(neg, int, frac, exp10) else {
                panic!("{text}");
            };
            if let Some(short) = parse_decimal_in_words(neg, int, frac, exp10) {
                let Rounded::Value(short) = short else {
                    panic!("{text}");
                };
                assert_eq!(canonical(short), canonical(long), "{text}");
                short_reads += 1;
            }
            if let Some(text) = long.to_redis_string_in_words() {
                assert_eq!(text, long.to_redis_string_long(), "{neg} {text}");
            }
            let (other_neg, other) = number();
            let Some(Parsed::Finite(other)) = LongDouble::parse(
                format!("{}{other}", if other_neg { "-" } else { "" }).as_bytes(),
            ) else {
                panic!("{other}");
            };
            if long.mant != 0
                && other.mant != 0
                && let Some(sum) = long.add_in_words(other)
            {
                let slow = long.add_long(other).unwrap();
                assert_eq!(canonical(sum), canonical(slow), "{text} + {other:?}");
                short_sums += 1;
            }
        }
        // Most of them take the short way.
        assert!(short_reads > 10_000, "{short_reads}");
        assert!(short_sums > 10_000, "{short_sums}");
    }

    /// 2,500 random pairs, with what Redis 8.10.2 on x86-64 replied to
    /// `SET k a` and `INCRBYFLOAT k b`: the sum, `ERRPARSE` for `ERR value
    /// is not a valid float`, `NOTFINITE` for `ERR increment would produce
    /// NaN or Infinity`. Half are numbers of up to 19 digits, which take the
    /// short way; half anything, hexadecimal, exponents near the ends of the
    /// range and text that is no number among them. (A trimmed run of the
    /// differential check against `redis-server`, 50,000 pairs a seed.)
    #[test]
    fn sums_are_what_redis_8_10_2_replied_to_random_pairs() {
        let table = include_str!("../../tests/fixtures/redis-8.10.2-incrbyfloat.tsv");
        let mut checked = 0;
        for line in table.split('\n') {
            let line = line.strip_suffix('\r').unwrap_or(line);
            if line.is_empty() {
                continue;
            }
            let mut fields = line.split('\t');
            let (Some(a), Some(b), Some(want), None) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                panic!("{line:?}");
            };
            let got = match (
                LongDouble::parse(a.as_bytes()),
                LongDouble::parse(b.as_bytes()),
            ) {
                (Some(a), Some(b)) => match increment(a, b) {
                    Increment::Sum(sum) => sum,
                    Increment::NotFinite => "NOTFINITE".to_string(),
                },
                _ => "ERRPARSE".to_string(),
            };
            assert_eq!(got, want, "{a:?} + {b:?}");
            checked += 1;
        }
        assert_eq!(checked, 2500);
    }

    /// A tie is rounded to the even neighbour, in reading and in writing.
    #[test]
    fn ties_go_to_even() {
        // 2^64 + 1 is halfway between 2^64 and 2^64 + 2: it reads as 2^64.
        assert_eq!(incr("18446744073709551617", "0"), "18446744073709551616");
        // 2^64 + 3 is halfway between 2^64 + 2 and 2^64 + 4: 2^64 + 4.
        assert_eq!(incr("18446744073709551619", "0"), "18446744073709551620");
        // 2^-18 and 3 × 2^-18 have 18 places, the last a 5: to 17, the
        // 2 before it stays, the 7 goes up.
        assert_eq!(incr("0x1p-18", "0"), "0.00000381469726562");
        assert_eq!(incr("0x3p-18", "0"), "0.00001144409179688");
        // A quotient of more than 64 bits, and a number with many digits.
        assert_eq!(
            incr("123456789012345678901234567890.5", "0"),
            "123456789012345678899921813504"
        );
    }
}
