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
//!
//! One thing differs after a splice: an error Lua's own `string.format`
//! raises for a later argument counts the arguments it was given, without
//! the integer ones formatted here. `string.format('%d %f', 1.5, 'x')`
//! reports bad argument #2 where Redis says #3. (FIXME: a patched copy of
//! Lua's own `str_format` would have neither this nor the wrapper's cost.)

use mlua::ffi;
use std::ffi::c_int;

const MINUS: u8 = 1;
const PLUS: u8 = 2;
const SPACE: u8 = 4;
const HASH: u8 = 8;
const ZERO: u8 = 16;

/// The flag a byte is in a conversion, or 0.
const FLAG_OF: [u8; 256] = {
    let mut table = [0; 256];
    table[b'-' as usize] = MINUS;
    table[b'+' as usize] = PLUS;
    table[b' ' as usize] = SPACE;
    table[b'#' as usize] = HASH;
    table[b'0' as usize] = ZERO;
    table
};

/// What one `%` conversion of a format is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Spec {
    /// [`MINUS`], [`PLUS`], [`SPACE`], [`HASH`] and [`ZERO`], as given.
    flags: u8,
    width: u8,
    precision: Option<u8>,
    conversion: u8,
    /// Where the conversion starts (at its `%`) and ends (after its letter).
    start: usize,
    end: usize,
}

impl Spec {
    fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    fn is_integer(&self) -> bool {
        matches!(
            self.conversion,
            b'd' | b'i' | b'o' | b'u' | b'x' | b'X' | b'c'
        )
    }

    /// Whether Lua 5.4 takes these flags and this precision for an integer
    /// conversion (its `checkformat`), as 5.1 takes any. (`rewrite` checks
    /// the same, inline.)
    #[cfg(test)]
    fn lua54_takes(&self) -> bool {
        let allowed = match self.conversion {
            b'd' | b'i' => MINUS | PLUS | SPACE | ZERO,
            b'u' => MINUS | ZERO,
            b'o' | b'x' | b'X' => MINUS | HASH | ZERO,
            _ => MINUS, // c, which takes no precision either
        };
        self.flags & !allowed == 0 && (self.conversion != b'c' || self.precision.is_none())
    }
}

/// Up to two digits at `at`, as Lua 5.1 reads a width or a precision:
/// their value, or `None` for a third.
#[inline(always)]
fn two_digits(format: &[u8], at: &mut usize) -> Option<u8> {
    let mut value = 0;
    for _ in 0..2 {
        match format.get(*at) {
            Some(byte @ b'0'..=b'9') => {
                value = value * 10 + (byte - b'0');
                *at += 1;
            }
            _ => return Some(value),
        }
    }
    (!format.get(*at).is_some_and(u8::is_ascii_digit)).then_some(value)
}

/// The conversion whose `%` is at `start`, read as Lua 5.1 reads one: at
/// most five flags, two digits of width and two of precision. `None` for
/// `%%` and for what is not a valid conversion, which Lua's own
/// `string.format` reports.
#[inline(always)]
fn parse_spec(format: &[u8], start: usize) -> Option<Spec> {
    let mut at = start + 1;
    let mut flags = 0;
    let mut count = 0;
    while let Some(&byte) = format.get(at) {
        let flag = FLAG_OF[usize::from(byte)];
        if flag == 0 {
            break;
        }
        flags |= flag;
        at += 1;
        count += 1;
    }
    if count > 5 {
        return None;
    }
    let width = two_digits(format, &mut at)?;
    let mut precision = None;
    if format.get(at) == Some(&b'.') {
        at += 1;
        precision = Some(two_digits(format, &mut at)?);
    }
    let conversion = *format.get(at)?;
    if conversion == b'%' {
        return None;
    }
    Some(Spec {
        flags,
        width,
        precision,
        conversion,
        start,
        end: at + 1,
    })
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

/// The most text one integer conversion writes, `%` doubled: a width and a
/// precision are at most 99, and 22 digits and a two-byte prefix fit in that.
const TEXT_MAX: usize = 256;

/// Text of at most [`TEXT_MAX`] bytes, on the Rust stack: nothing in it
/// needs dropping, so a Lua error may jump past it.
struct Text {
    bytes: [u8; TEXT_MAX],
    len: usize,
}

impl Text {
    fn new() -> Self {
        Text {
            bytes: [0; TEXT_MAX],
            len: 0,
        }
    }

    fn push(&mut self, byte: u8) {
        if self.len < TEXT_MAX {
            self.bytes[self.len] = byte;
            self.len += 1;
        }
    }

    fn extend(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.push(byte);
        }
    }

    fn pad(&mut self, count: usize, byte: u8) {
        for _ in 0..count {
            self.push(byte);
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// `spec` applied to `value` as Lua 5.1's `string.format` does: the number
/// cast to a C integer, then C's `sprintf`, with any `%` doubled, to go into
/// a format.
fn format_integer(spec: &Spec, value: Value) -> Text {
    let mut text = Text::new();
    if spec.conversion == b'c' {
        let byte = match value {
            Value::Integer(n) => n as u8,
            Value::Float(n) => c_int_cast(n) as u8,
        };
        let pad = usize::from(spec.width).saturating_sub(1);
        if !spec.has(MINUS) {
            text.pad(pad, b' ');
        }
        text.push(byte);
        if spec.has(MINUS) {
            text.pad(pad, b' ');
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
        // The digits, last first.
        let mut digits = Text::new();
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
            digits.pad(usize::from(precision).saturating_sub(digits.len), b'0');
        }
        if spec.has(HASH) && spec.conversion == b'o' && digits.as_slice().last() != Some(&b'0') {
            digits.push(b'0');
        }
        digits.bytes[..digits.len].reverse();
        let prefix: &[u8] = match spec.conversion {
            b'd' | b'i' if negative => b"-",
            b'd' | b'i' if spec.has(PLUS) => b"+",
            b'd' | b'i' if spec.has(SPACE) => b" ",
            b'x' if spec.has(HASH) && magnitude != 0 => b"0x",
            b'X' if spec.has(HASH) && magnitude != 0 => b"0X",
            _ => b"",
        };
        let pad = usize::from(spec.width).saturating_sub(prefix.len() + digits.len);
        if spec.has(MINUS) {
            text.extend(prefix);
            text.extend(digits.as_slice());
            text.pad(pad, b' ');
        } else if spec.has(ZERO) && spec.precision.is_none() {
            text.extend(prefix);
            text.pad(pad, b'0');
            text.extend(digits.as_slice());
        } else {
            text.pad(pad, b' ');
            text.extend(prefix);
            text.extend(digits.as_slice());
        }
    }
    // Lua 5.1 adds what `sprintf` wrote up to its first zero byte, and the
    // text goes into a format: `%` doubled.
    let mut out = Text::new();
    for &byte in text.as_slice() {
        if byte == 0 {
            break;
        }
        out.push(byte);
        if byte == b'%' {
            out.push(b'%');
        }
    }
    out
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

/// What [`plan`] found a call to need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    /// Lua's own `string.format`, on the arguments given: no integer
    /// conversion needs Lua 5.1's rules, or the format is one Lua's own
    /// reports an error for.
    Original,
    /// An error for an argument, reported as Lua's own words it.
    Fault(Fault),
    /// The integer conversions formatted here: this many of them.
    Rewrite(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    Missing(c_int),
    NotANumber(c_int),
}

/// Whether a call needs Lua 5.1's rules for an integer conversion, in one
/// pass that reads conversions as `parse_spec` does, raises nothing and
/// allocates nothing. A missing argument, or one an integer conversion
/// cannot take, is a [`Plan::Fault`] only when the call needs those rules
/// before it: otherwise Lua's own `string.format` reports it, as it did.
///
/// # Safety
///
/// `state` is the running call's, with `top` values.
unsafe fn plan(state: *mut ffi::lua_State, format: &[u8], top: c_int) -> Plan {
    // Most calls need nothing: looked for first, with nothing allocated,
    // in one pass that reads conversions as `parse_spec` does.
    let length = format.len();
    let mut needed = false;
    let mut integers = 0;
    let mut fault = None;
    let mut argument = 1;
    let mut at = 0;
    while at < length {
        if format[at] != b'%' {
            at += 1;
            continue;
        }
        at += 1;
        if at < length && format[at] == b'%' {
            at += 1;
            continue;
        }
        let flags_at = at;
        let mut flags = 0;
        while at < length && FLAG_OF[usize::from(format[at])] != 0 {
            flags |= FLAG_OF[usize::from(format[at])];
            at += 1;
        }
        let width_at = at;
        while at < length && format[at].is_ascii_digit() {
            at += 1;
        }
        if width_at - flags_at > 5 || at - width_at > 2 {
            return Plan::Original;
        }
        let mut precision = false;
        if at < length && format[at] == b'.' {
            precision = true;
            at += 1;
            let digits_at = at;
            while at < length && format[at].is_ascii_digit() {
                at += 1;
            }
            if at - digits_at > 2 {
                return Plan::Original;
            }
        }
        if at >= length || format[at] == b'%' {
            return Plan::Original;
        }
        let conversion = format[at];
        at += 1;
        argument += 1;
        if argument > top {
            fault = Some(Fault::Missing(argument));
            break;
        }
        let allowed = match conversion {
            b'd' | b'i' => MINUS | PLUS | SPACE | ZERO,
            b'u' => MINUS | ZERO,
            b'o' | b'x' | b'X' => MINUS | HASH | ZERO,
            b'c' => MINUS,
            _ => continue,
        };
        let Some((value, takes)) = (unsafe { integer_argument(state, argument) }) else {
            fault = Some(Fault::NotANumber(argument));
            break;
        };
        integers += 1;
        // Flags or a precision 5.4 refuses, and `%c` of a zero byte, which
        // Lua 5.1 writes as nothing.
        let refused = flags & !allowed != 0 || (conversion == b'c' && precision);
        let zero_byte = conversion == b'c' && matches!(value, Value::Integer(n) if n as u8 == 0);
        needed |= !takes || refused || zero_byte;
    }
    match (needed, fault) {
        (false, _) => Plan::Original,
        (true, Some(fault)) => Plan::Fault(fault),
        (true, None) => Plan::Rewrite(integers),
    }
}

/// Writes the format Lua's own `string.format` is to be given into `out`,
/// the integer conversions formatted, and moves the arguments the rest
/// take down to 2, 3 and on, in place (an argument only ever moves down,
/// past ones already used). Returns how many bytes it wrote and how many
/// arguments it kept. Nothing raises: [`plan`] has read the format.
///
/// # Safety
///
/// After a [`Plan::Rewrite`] of this format and these arguments, with
/// `out` as long as [`rewrite_bound`] says.
unsafe fn rewrite(
    state: *mut ffi::lua_State,
    format: &[u8],
    top: c_int,
    out: &mut [u8],
) -> (usize, c_int) {
    let mut written = 0;
    let mut write = |bytes: &[u8]| {
        let end = (written + bytes.len()).min(out.len());
        out[written..end].copy_from_slice(&bytes[..end - written]);
        written = end;
    };
    let mut kept = 0;
    let mut argument = 1;
    let mut at = 0;
    while let Some(found) = format[at..].iter().position(|&b| b == b'%') {
        let start = at + found;
        write(&format[at..start]);
        if format.get(start + 1) == Some(&b'%') {
            write(b"%%");
            at = start + 2;
            continue;
        }
        let Some(spec) = parse_spec(format, start) else {
            // Not after a plan to rewrite: the rest as it is.
            at = start;
            break;
        };
        argument += 1;
        match spec
            .is_integer()
            .then(|| unsafe { integer_argument(state, argument) })
        {
            Some(Some((value, _))) => write(format_integer(&spec, value).as_slice()),
            _ => {
                write(&format[spec.start..spec.end]);
                kept += 1;
                unsafe { ffi::lua_copy(state, argument, 1 + kept) };
            }
        }
        at = spec.end;
    }
    write(&format[at..]);
    // Arguments past the format's are passed on, as given.
    for extra in argument + 1..=top {
        kept += 1;
        unsafe { ffi::lua_copy(state, extra, 1 + kept) };
    }
    (written, kept)
}

/// How long the format [`rewrite`] writes can be: the format given, and
/// [`TEXT_MAX`] for each integer conversion.
fn rewrite_bound(format: &[u8], integers: usize) -> usize {
    format.len() + integers * TEXT_MAX
}

/// Lua's own `string.format` (`str_format`), the same for every VM.
static ORIGINAL: std::sync::OnceLock<ffi::lua_CFunction> = std::sync::OnceLock::new();

/// `string.format` in scripts: Lua's own, [`ORIGINAL`], with Lua 5.1's
/// integer conversions (see the module). The original is run in this
/// call's own frame, not called: its errors then name `format`, as they did.
///
/// # Safety
///
/// A Lua C function, installed by [`install`].
pub(crate) unsafe extern "C-unwind" fn script_string_format(state: *mut ffi::lua_State) -> c_int {
    unsafe {
        let Some(&original) = ORIGINAL.get() else {
            return ffi::luaL_error(state, c"string.format is missing".as_ptr());
        };
        if ffi::lua_type(state, 1) != ffi::LUA_TSTRING {
            return original(state);
        }
        let top = ffi::lua_gettop(state);
        let mut length = 0;
        let pointer = ffi::lua_tolstring(state, 1, &mut length);
        // The format stays at 1, and with it these bytes, until the new
        // one replaces it below.
        let format = std::slice::from_raw_parts(pointer.cast::<u8>(), length);
        // Nothing on the Rust heap from here on: a Lua error is a jump past
        // these frames, and anything that needed dropping would be lost.
        let integers = match plan(state, format, top) {
            Plan::Original => return original(state),
            Plan::Fault(Fault::Missing(argument)) => {
                return ffi::luaL_argerror(state, argument, c"no value".as_ptr());
            }
            Plan::Fault(Fault::NotANumber(argument)) => {
                let message = ffi::lua_pushfstring(
                    state,
                    c"number expected, got %s".as_ptr(),
                    ffi::luaL_typename(state, argument),
                );
                return ffi::luaL_argerror(state, argument, message);
            }
            Plan::Rewrite(integers) => integers,
        };
        // The new format is written into Lua's memory, a userdata at the
        // top, so that what an error leaves behind is Lua's to collect.
        let bound = rewrite_bound(format, integers);
        let buffer = ffi::lua_newuserdatauv(state, bound, 0).cast::<u8>();
        let out = std::slice::from_raw_parts_mut(buffer, bound);
        let (written, kept) = rewrite(state, format, top, out);
        ffi::lua_pushlstring(state, buffer.cast(), written);
        ffi::lua_replace(state, 1);
        // Drops the arguments used, and the userdata.
        ffi::lua_settop(state, kept + 1);
        original(state)
    }
}

/// Puts [`script_string_format`] in place of `string.format`, which is what
/// `("%d"):format(x)` reaches too. Only when that is Lua's own C function:
/// otherwise there is nothing for the wrapper to run, and it is left alone.
///
/// # Safety
///
/// Runs on a state that has the `string` library.
pub(crate) unsafe fn install(state: *mut ffi::lua_State) {
    unsafe {
        ffi::lua_checkstack(state, 3);
        ffi::lua_getglobal(state, c"string".as_ptr());
        ffi::lua_getfield(state, -1, c"format".as_ptr());
        let found = ffi::lua_tocfunction(state, -1);
        ffi::lua_pop(state, 1);
        if let Some(original) = found {
            ORIGINAL.get_or_init(|| original);
            ffi::lua_pushcfunction(state, script_string_format);
            ffi::lua_setfield(state, -2, c"format".as_ptr());
        }
        ffi::lua_pop(state, 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(format: &str, value: Value) -> String {
        let spec = parse_spec(format.as_bytes(), 0).expect("a conversion");
        assert_eq!(spec.end, format.len(), "{format}");
        String::from_utf8(format_integer(&spec, value).as_slice().to_vec()).unwrap()
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
        assert_eq!(spec.flags, MINUS | ZERO);
        assert_eq!((spec.width, spec.precision), (8, Some(3)));
        assert_eq!((spec.conversion, spec.start, spec.end), (b'd', 1, 8));
        assert!(parse_spec(b"%5.2f", 0).is_some_and(|s| !s.is_integer()));
        assert!(parse_spec(b"%+x", 0).is_some_and(|s| !s.lua54_takes()));
        assert!(parse_spec(b"%-08d", 0).is_some_and(|s| s.lua54_takes()));
        assert!(parse_spec(b"%.2c", 0).is_some_and(|s| !s.lua54_takes()));
    }
}
