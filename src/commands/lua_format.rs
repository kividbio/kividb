//! `string.format` in scripts, with Lua 5.1's rules for integer conversions
//! (#747).
//!
//! Scripts run on Lua 5.4, and Redis's on Lua 5.1. For `%d %i %o %u %x %X
//! %c`, 5.4 takes only a number with an integer value and fails on any
//! other (`string.format('%d', 1.5)`: "number has no integer
//! representation"), where 5.1 casts the number to a C integer, cutting a
//! fraction toward zero: Redis gives `1`. Scripts written for Redis lean on
//! that, such as `string.format("%d%03d", t[1], t[2] / 1000)` to make
//! milliseconds from `TIME`.
//!
//! The wrapper leaves a call alone when 5.4 would format it as 5.1 does:
//! every integer conversion is given an integer, with flags 5.4 takes. Only
//! otherwise does it format the integer conversions itself, as 5.1's C
//! `sprintf` does, splice the text into the format, and hand the rest to
//! Lua's own `string.format`. Everything else (`%s`, `%f`, `%q`, errors for
//! a missing or non-number argument) stays Lua 5.4's.

use mlua::ffi;
use std::ffi::c_int;

/// What one `%` conversion of a format is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Spec {
    minus: bool,
    plus: bool,
    space: bool,
    hash: bool,
    zero: bool,
    width: usize,
    precision: Option<usize>,
    conversion: u8,
    /// Where the conversion starts (at its `%`) and ends (after its letter).
    start: usize,
    end: usize,
}

impl Spec {
    fn is_integer(&self) -> bool {
        matches!(
            self.conversion,
            b'd' | b'i' | b'o' | b'u' | b'x' | b'X' | b'c'
        )
    }

    /// Whether Lua 5.4 takes these flags and this precision for an integer
    /// conversion (its `checkformat`), as 5.1 takes any.
    fn lua54_takes(&self) -> bool {
        let (plus_space, hash, zero, precision) = match self.conversion {
            b'd' | b'i' => (true, false, true, true),
            b'u' => (false, false, true, true),
            b'o' | b'x' | b'X' => (false, true, true, true),
            _ => (false, false, false, false), // c
        };
        (plus_space || !(self.plus || self.space))
            && (hash || !self.hash)
            && (zero || !self.zero)
            && (precision || self.precision.is_none())
    }
}

/// The conversion whose `%` is at `start`, read as Lua 5.1 reads one: at
/// most five flags, two digits of width and two of precision. `None` for
/// `%%` and for what is not a valid conversion, which Lua's own
/// `string.format` reports.
fn parse_spec(format: &[u8], start: usize) -> Option<Spec> {
    let mut at = start + 1;
    let mut spec = Spec {
        minus: false,
        plus: false,
        space: false,
        hash: false,
        zero: false,
        width: 0,
        precision: None,
        conversion: 0,
        start,
        end: 0,
    };
    let flags_from = at;
    while let Some(&byte) = format.get(at) {
        match byte {
            b'-' => spec.minus = true,
            b'+' => spec.plus = true,
            b' ' => spec.space = true,
            b'#' => spec.hash = true,
            b'0' => spec.zero = true,
            _ => break,
        }
        at += 1;
    }
    if at - flags_from > 5 {
        return None;
    }
    let digits = |at: &mut usize| {
        let mut value = 0;
        let from = *at;
        while let Some(byte @ b'0'..=b'9') = format.get(*at) {
            value = value * 10 + usize::from(byte - b'0');
            *at += 1;
        }
        (*at - from <= 2).then_some(value)
    };
    spec.width = digits(&mut at)?;
    if format.get(at) == Some(&b'.') {
        at += 1;
        spec.precision = Some(digits(&mut at)?);
    }
    spec.conversion = *format.get(at)?;
    if spec.conversion == b'%' {
        return None;
    }
    spec.end = at + 1;
    Some(spec)
}

/// The value an integer conversion is given.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Value {
    Integer(i64),
    Float(f64),
}

const TWO_63: f64 = 9_223_372_036_854_775_808.0;

/// A C `(long)` cast of a double as x86-64 makes it: cut toward zero, and
/// the lowest integer for what does not fit (NaN, infinities, 1e20).
fn c_long(n: f64) -> i64 {
    if (-TWO_63..TWO_63).contains(&n) {
        n as i64
    } else {
        i64::MIN
    }
}

/// A C `(unsigned long)` cast of a double as x86-64 makes it: below 2^63
/// through `(long)`, so -1.5 is 2^64 - 1; from 2^63, less 2^63 and the top
/// bit set again.
fn c_unsigned_long(n: f64) -> u64 {
    if n >= TWO_63 {
        (c_long(n - TWO_63) as u64) ^ (1 << 63)
    } else {
        c_long(n) as u64
    }
}

/// A C `(int)` cast of a double as x86-64 makes it.
fn c_int_cast(n: f64) -> i32 {
    if n > -2_147_483_649.0 && n < 2_147_483_648.0 {
        n as i32
    } else {
        i32::MIN
    }
}

/// `spec` applied to `value` as Lua 5.1's `string.format` does: the number
/// cast to a C integer, then C's `sprintf`. Appended to `out` with any `%`
/// doubled, to go into a format.
fn format_integer(spec: &Spec, value: Value, out: &mut Vec<u8>) {
    let mut text: Vec<u8> = Vec::with_capacity(24);
    if spec.conversion == b'c' {
        let byte = match value {
            Value::Integer(n) => n as u8,
            Value::Float(n) => c_int_cast(n) as u8,
        };
        let pad = spec.width.saturating_sub(1);
        if !spec.minus {
            text.resize(pad, b' ');
        }
        text.push(byte);
        if spec.minus {
            text.resize(pad + 1, b' ');
        }
    } else {
        let (negative, magnitude) = match spec.conversion {
            b'd' | b'i' => {
                let n = match value {
                    Value::Integer(n) => n,
                    Value::Float(n) => c_long(n),
                };
                (n < 0, n.unsigned_abs())
            }
            _ => (
                false,
                match value {
                    Value::Integer(n) => n as u64,
                    Value::Float(n) => c_unsigned_long(n),
                },
            ),
        };
        let (radix, digits_of): (u64, &[u8; 16]) = match spec.conversion {
            b'o' => (8, b"0123456789abcdef"),
            b'x' => (16, b"0123456789abcdef"),
            b'X' => (16, b"0123456789ABCDEF"),
            _ => (10, b"0123456789abcdef"),
        };
        let mut digits = Vec::with_capacity(24);
        let mut rest = magnitude;
        while rest > 0 {
            digits.push(digits_of[(rest % radix) as usize]);
            rest /= radix;
        }
        // No digits for zero only when a precision of 0 asks for none.
        if magnitude == 0 && spec.precision != Some(0) {
            digits.push(b'0');
        }
        if let Some(precision) = spec.precision {
            while digits.len() < precision {
                digits.push(b'0');
            }
        }
        if spec.hash && spec.conversion == b'o' && digits.last() != Some(&b'0') {
            digits.push(b'0');
        }
        digits.reverse();
        let prefix: &[u8] = match spec.conversion {
            b'd' | b'i' if negative => b"-",
            b'd' | b'i' if spec.plus => b"+",
            b'd' | b'i' if spec.space => b" ",
            b'x' if spec.hash && magnitude != 0 => b"0x",
            b'X' if spec.hash && magnitude != 0 => b"0X",
            _ => b"",
        };
        let length = prefix.len() + digits.len();
        let pad = spec.width.saturating_sub(length);
        if spec.minus {
            text.extend_from_slice(prefix);
            text.extend_from_slice(&digits);
            text.resize(length + pad, b' ');
        } else if spec.zero && spec.precision.is_none() {
            text.extend_from_slice(prefix);
            text.resize(prefix.len() + pad, b'0');
            text.extend_from_slice(&digits);
        } else {
            text.resize(pad, b' ');
            text.extend_from_slice(prefix);
            text.extend_from_slice(&digits);
        }
    }
    // Lua 5.1 adds what `sprintf` wrote up to its first zero byte.
    let written = text.iter().position(|&b| b == 0).unwrap_or(text.len());
    for &byte in &text[..written] {
        out.push(byte);
        if byte == b'%' {
            out.push(b'%');
        }
    }
}

/// The value at `index` as an integer conversion reads it, and whether Lua
/// 5.4 takes it as is: `None` if it is not a number or a string that reads
/// as one.
unsafe fn integer_argument(state: *mut ffi::lua_State, index: c_int) -> Option<(Value, bool)> {
    unsafe {
        let mut is_integer = 0;
        let n = ffi::lua_tointegerx(state, index, &mut is_integer);
        if is_integer != 0 {
            return Some((Value::Integer(n), true));
        }
        let mut is_number = 0;
        let n = ffi::lua_tonumberx(state, index, &mut is_number);
        (is_number != 0).then_some((Value::Float(n), false))
    }
}

/// The format and the arguments to hand Lua's `string.format` in place of
/// the ones given, when an integer conversion needs Lua 5.1's rules. `None`
/// when the call is to go through as it is: no conversion needs them, or
/// the format is one Lua's own reports an error for. A missing argument, or
/// one an integer conversion cannot take, is reported here, as Lua's own
/// words it, so that the error is for the first such argument.
///
/// # Safety
///
/// May raise a Lua error: nothing is allocated before it can.
unsafe fn rewrite(
    state: *mut ffi::lua_State,
    format: &[u8],
    top: c_int,
) -> Option<(Vec<u8>, Vec<c_int>)> {
    // Most calls need nothing: looked for first, with nothing allocated.
    let mut needed = false;
    let mut argument = 1;
    let mut at = 0;
    while let Some(found) = format[at..].iter().position(|&b| b == b'%') {
        let start = at + found;
        if format.get(start + 1) == Some(&b'%') {
            at = start + 2;
            continue;
        }
        let spec = parse_spec(format, start)?;
        argument += 1;
        if argument > top {
            // As Lua's own says it, for this argument and not a later one.
            unsafe { ffi::luaL_argerror(state, argument, c"no value".as_ptr()) };
            return None;
        }
        if spec.is_integer() {
            let Some((value, takes)) = (unsafe { integer_argument(state, argument) }) else {
                unsafe {
                    let message = ffi::lua_pushfstring(
                        state,
                        c"number expected, got %s".as_ptr(),
                        ffi::luaL_typename(state, argument),
                    );
                    ffi::luaL_argerror(state, argument, message);
                }
                return None;
            };
            // Lua 5.1 writes `%c` of a zero byte as nothing.
            let zero_byte =
                spec.conversion == b'c' && matches!(value, Value::Integer(n) if n as u8 == 0);
            needed |= !takes || !spec.lua54_takes() || zero_byte;
        }
        at = spec.end;
    }
    if !needed {
        return None;
    }
    let mut out = Vec::with_capacity(format.len() + 16);
    let mut kept = Vec::new();
    let mut argument = 1;
    let mut at = 0;
    while let Some(found) = format[at..].iter().position(|&b| b == b'%') {
        let start = at + found;
        out.extend_from_slice(&format[at..start]);
        if format.get(start + 1) == Some(&b'%') {
            out.extend_from_slice(b"%%");
            at = start + 2;
            continue;
        }
        let spec = parse_spec(format, start)?;
        argument += 1;
        if spec.is_integer() {
            let (value, _) = unsafe { integer_argument(state, argument) }?;
            format_integer(&spec, value, &mut out);
        } else {
            out.extend_from_slice(&format[spec.start..spec.end]);
            kept.push(argument);
        }
        at = spec.end;
    }
    out.extend_from_slice(&format[at..]);
    // Arguments past the format's are passed on, as given.
    kept.extend(argument + 1..=top);
    Some((out, kept))
}

/// `string.format` in scripts: the original, upvalue 1, with Lua 5.1's
/// integer conversions (see the module). The original is run in this
/// call's own frame, not called: its errors then name `format`, as they did.
///
/// # Safety
///
/// A Lua C function, with the original `string.format` as its upvalue.
pub(crate) unsafe extern "C-unwind" fn script_string_format(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        let Some(original) = ffi::lua_tocfunction(state, ffi::lua_upvalueindex(1)) else {
            return ffi::luaL_error(state, c"string.format is missing".as_ptr());
        };
        let top = ffi::lua_gettop(state);
        let mut plan = None;
        if ffi::lua_type(state, 1) == ffi::LUA_TSTRING {
            let mut length = 0;
            let pointer = ffi::lua_tolstring(state, 1, &mut length);
            let format = std::slice::from_raw_parts(pointer.cast::<u8>(), length);
            plan = rewrite(state, format, top);
        }
        let Some((format, kept)) = plan else {
            return original(state);
        };
        // The new format and the arguments kept, in place of the ones given.
        let count = kept.len() as c_int + 1;
        ffi::luaL_checkstack(state, count, c"too many arguments".as_ptr());
        ffi::lua_pushlstring(state, format.as_ptr().cast(), format.len());
        for &index in &kept {
            ffi::lua_pushvalue(state, index);
        }
        // Nothing to drop is left when Lua's `format` raises an error.
        drop(format);
        drop(kept);
        ffi::lua_rotate(state, 1, count);
        ffi::lua_settop(state, count);
        original(state)
    }
}

/// Puts [`script_string_format`] in place of `string.format`, which is what
/// `("%d"):format(x)` reaches too.
///
/// # Safety
///
/// Runs on a state that has the `string` library.
pub(crate) unsafe fn install(state: *mut ffi::lua_State) {
    unsafe {
        ffi::lua_checkstack(state, 3);
        ffi::lua_getglobal(state, c"string".as_ptr());
        ffi::lua_getfield(state, -1, c"format".as_ptr());
        ffi::lua_pushcclosure(state, script_string_format, 1);
        ffi::lua_setfield(state, -2, c"format".as_ptr());
        ffi::lua_pop(state, 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(format: &str, value: Value) -> String {
        let spec = parse_spec(format.as_bytes(), 0).expect("a conversion");
        assert_eq!(spec.end, format.len(), "{format}");
        let mut out = Vec::new();
        format_integer(&spec, value, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn integer_conversions_format_as_c_does() {
        use Value::*;
        let cases: &[(&str, Value, &str)] = &[
            ("%d", Float(1.5), "1"),
            ("%d", Float(-1.5), "-1"),
            ("%03d", Float(123.456), "123"),
            ("%03d", Float(5.9), "005"),
            ("%5d", Integer(42), "   42"),
            ("%-5d", Integer(42), "42   "),
            ("%+d", Float(3.2), "+3"),
            ("% d", Float(3.2), " 3"),
            ("%.3d", Float(7.7), "007"),
            ("%08.3d", Integer(-7), "    -007"),
            ("%08d", Integer(-7), "-0000007"),
            ("%.0d", Float(0.5), ""),
            ("%x", Float(255.9), "ff"),
            ("%X", Float(255.9), "FF"),
            ("%#x", Float(255.9), "0xff"),
            ("%#x", Float(0.5), "0"),
            ("%#o", Float(8.5), "010"),
            ("%#o", Integer(0), "0"),
            ("%o", Float(8.5), "10"),
            ("%u", Float(3.5), "3"),
            ("%x", Float(-1.5), "ffffffffffffffff"),
            ("%i", Float(3.99), "3"),
            ("%c", Float(72.9), "H"),
            ("%3c", Float(72.9), "  H"),
            ("%-3c", Float(72.9), "H  "),
            ("%c", Float(37.5), "%%"),
            (
                "%d",
                Float(9_223_372_036_854_775_808.0),
                "-9223372036854775808",
            ),
            ("%d", Float(1e20), "-9223372036854775808"),
            ("%d", Float(f64::NAN), "-9223372036854775808"),
            ("%d", Integer(i64::MIN), "-9223372036854775808"),
            ("%x", Float(9_223_372_036_854_775_808.0), "8000000000000000"),
        ];
        for (format, value, expected) in cases {
            assert_eq!(one(format, *value), *expected, "{format} {value:?}");
        }
    }

    #[test]
    fn specs_are_read_as_lua_5_1_reads_them() {
        assert_eq!(parse_spec(b"%%", 0), None);
        assert_eq!(parse_spec(b"%123d", 0), None);
        assert_eq!(parse_spec(b"%.123d", 0), None);
        assert_eq!(parse_spec(b"%-+ #0-d", 0), None);
        assert_eq!(parse_spec(b"%5", 0), None);
        let spec = parse_spec(b"x%-08.3dy", 1).unwrap();
        assert!(spec.minus && spec.zero && !spec.plus);
        assert_eq!((spec.width, spec.precision), (8, Some(3)));
        assert_eq!((spec.conversion, spec.start, spec.end), (b'd', 1, 8));
        assert!(parse_spec(b"%5.2f", 0).is_some_and(|s| !s.is_integer()));
        assert!(parse_spec(b"%+x", 0).is_some_and(|s| !s.lua54_takes()));
        assert!(parse_spec(b"%-08d", 0).is_some_and(|s| s.lua54_takes()));
        assert!(parse_spec(b"%.2c", 0).is_some_and(|s| !s.lua54_takes()));
    }
}
