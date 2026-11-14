//! The `bit` and `struct` libraries Redis gives scripts, as Lua C functions:
//! LuaBitOp's `bit.*` and Roberto Ierusalimschy's `struct.pack`,
//! `struct.unpack` and `struct.size`, with their rules for numbers, sizes,
//! alignment and errors (#747).
//!
//! These raise Lua errors, which jump out of the function: nothing in them
//! that needs dropping is alive when they do. `struct.pack` checks its
//! arguments in a first pass and writes the string in a second, into memory
//! Lua owns.

use mlua::ffi::{self, lua_State};
use mlua::prelude::*;
use std::ffi::{CStr, c_char, c_int};

type CFunction = unsafe extern "C-unwind" fn(*mut lua_State) -> c_int;

/// The `bit` library: its functions, by name.
pub(super) fn bit_library(lua: &Lua) -> LuaResult<LuaTable> {
    let functions: [(&str, CFunction); 12] = [
        ("tobit", bit_tobit),
        ("bnot", bit_bnot),
        ("band", bit_band),
        ("bor", bit_bor),
        ("bxor", bit_bxor),
        ("lshift", bit_lshift),
        ("rshift", bit_rshift),
        ("arshift", bit_arshift),
        ("rol", bit_rol),
        ("ror", bit_ror),
        ("bswap", bit_bswap),
        ("tohex", bit_tohex),
    ];
    library(lua, &functions)
}

/// The `struct` library: its functions, by name.
pub(super) fn struct_library(lua: &Lua) -> LuaResult<LuaTable> {
    let functions: [(&str, CFunction); 3] = [
        ("pack", struct_pack),
        ("unpack", struct_unpack),
        ("size", struct_size),
    ];
    library(lua, &functions)
}

fn library(lua: &Lua, functions: &[(&str, CFunction)]) -> LuaResult<LuaTable> {
    let table = lua.create_table()?;
    for (name, function) in functions {
        // SAFETY: each is a Lua C function that keeps to the Lua C API.
        table.raw_set(*name, unsafe { lua.create_c_function(*function)? })?;
    }
    Ok(table)
}

// ── bit ─────────────────────────────────────────────────────────────────────

/// Argument `index` as 32 bits: the number rounded to an integer and taken
/// modulo 2^32, as LuaBitOp does by adding 2^52 + 2^51 to a double and
/// keeping the low word. Not a number (nor a string that is one): an error.
unsafe fn bit_arg(state: *mut lua_State, index: c_int) -> u32 {
    unsafe {
        let n = ffi::lua_tonumber(state, index);
        let bits = (n + 6_755_399_441_055_744.0).to_bits() as u32;
        if bits == 0 && ffi::lua_isnumber(state, index) == 0 {
            type_error(state, index, c"number");
        }
        bits
    }
}

/// `luaL_typerror`: "bad argument #i to 'f' (number expected, got nil)".
unsafe fn type_error(state: *mut lua_State, index: c_int, expected: &CStr) -> c_int {
    unsafe {
        let message = ffi::lua_pushfstring(
            state,
            c"%s expected, got %s".as_ptr(),
            expected.as_ptr(),
            ffi::luaL_typename(state, index),
        );
        ffi::luaL_argerror(state, index, message)
    }
}

/// Push 32 bits as the signed number LuaBitOp returns.
unsafe fn bit_return(state: *mut lua_State, bits: u32) -> c_int {
    unsafe { ffi::lua_pushnumber(state, bits as i32 as f64) };
    1
}

unsafe extern "C-unwind" fn bit_tobit(state: *mut lua_State) -> c_int {
    unsafe { bit_return(state, bit_arg(state, 1)) }
}

unsafe extern "C-unwind" fn bit_bnot(state: *mut lua_State) -> c_int {
    unsafe { bit_return(state, !bit_arg(state, 1)) }
}

/// `band`, `bor`, `bxor`: the first argument with every other, folded
/// from the last to the second, as LuaBitOp folds them.
unsafe fn bit_fold(state: *mut lua_State, op: fn(u32, u32) -> u32) -> c_int {
    unsafe {
        let mut bits = bit_arg(state, 1);
        let mut index = ffi::lua_gettop(state);
        while index > 1 {
            bits = op(bits, bit_arg(state, index));
            index -= 1;
        }
        bit_return(state, bits)
    }
}

unsafe extern "C-unwind" fn bit_band(state: *mut lua_State) -> c_int {
    unsafe { bit_fold(state, |a, b| a & b) }
}

unsafe extern "C-unwind" fn bit_bor(state: *mut lua_State) -> c_int {
    unsafe { bit_fold(state, |a, b| a | b) }
}

unsafe extern "C-unwind" fn bit_bxor(state: *mut lua_State) -> c_int {
    unsafe { bit_fold(state, |a, b| a ^ b) }
}

/// The shifts and rotations: the count is taken modulo 32.
unsafe fn bit_shift(state: *mut lua_State, op: fn(u32, u32) -> u32) -> c_int {
    unsafe {
        let bits = bit_arg(state, 1);
        let count = bit_arg(state, 2) & 31;
        bit_return(state, op(bits, count))
    }
}

unsafe extern "C-unwind" fn bit_lshift(state: *mut lua_State) -> c_int {
    unsafe { bit_shift(state, |b, n| b << n) }
}

unsafe extern "C-unwind" fn bit_rshift(state: *mut lua_State) -> c_int {
    unsafe { bit_shift(state, |b, n| b >> n) }
}

unsafe extern "C-unwind" fn bit_arshift(state: *mut lua_State) -> c_int {
    unsafe { bit_shift(state, |b, n| ((b as i32) >> n) as u32) }
}

unsafe extern "C-unwind" fn bit_rol(state: *mut lua_State) -> c_int {
    unsafe { bit_shift(state, u32::rotate_left) }
}

unsafe extern "C-unwind" fn bit_ror(state: *mut lua_State) -> c_int {
    unsafe { bit_shift(state, u32::rotate_right) }
}

unsafe extern "C-unwind" fn bit_bswap(state: *mut lua_State) -> c_int {
    unsafe { bit_return(state, bit_arg(state, 1).swap_bytes()) }
}

/// `bit.tohex(n [, width])`: the low `width` hex digits (8 by default, at
/// most 8), upper case when `width` is negative.
unsafe extern "C-unwind" fn bit_tohex(state: *mut lua_State) -> c_int {
    unsafe {
        let mut bits = bit_arg(state, 1);
        let mut width = if ffi::lua_isnone(state, 2) != 0 {
            8
        } else {
            bit_arg(state, 2) as i32
        };
        let digits: &[u8; 16] = if width < 0 {
            width = width.saturating_neg();
            b"0123456789ABCDEF"
        } else {
            b"0123456789abcdef"
        };
        let width = width.min(8) as usize;
        let mut out = [0u8; 8];
        for at in (0..width).rev() {
            out[at] = digits[(bits & 15) as usize];
            bits >>= 4;
        }
        ffi::lua_pushlstring(state, out.as_ptr().cast(), width);
        1
    }
}

// ── struct ──────────────────────────────────────────────────────────────────

/// The largest integer `i`/`I` may give a size to.
const MAX_INT_SIZE: usize = 32;
/// What `!` alone aligns to: a double's alignment.
const MAX_ALIGN: usize = 8;

/// The state a format's control options set.
struct Header {
    little: bool,
    align: usize,
}

impl Header {
    fn new() -> Self {
        Header {
            little: cfg!(target_endian = "little"),
            align: 1,
        }
    }
}

/// A format string: Lua's bytes, read up to the first zero byte as C reads
/// it.
struct Format {
    bytes: *const u8,
    at: usize,
}

impl Format {
    unsafe fn new(state: *mut lua_State) -> Self {
        Format {
            bytes: unsafe { ffi::luaL_checklstring(state, 1, std::ptr::null_mut()) }.cast(),
            at: 0,
        }
    }

    fn peek(&self) -> u8 {
        // SAFETY: Lua's strings end with a zero byte, and reading stops there.
        unsafe { *self.bytes.add(self.at) }
    }

    /// The next option, or `None` at the end.
    fn next(&mut self) -> Option<u8> {
        let option = self.peek();
        if option == 0 {
            return None;
        }
        self.at += 1;
        Some(option)
    }

    /// The number after an option, or `default` when none follows.
    unsafe fn number(&mut self, state: *mut lua_State, default: usize) -> usize {
        if !self.peek().is_ascii_digit() {
            return default;
        }
        let mut n: i32 = 0;
        while self.peek().is_ascii_digit() {
            let digit = (self.peek() - b'0') as i32;
            if n > i32::MAX / 10 || n * 10 > i32::MAX - digit {
                unsafe { ffi::luaL_error(state, c"integral size overflow".as_ptr()) };
            }
            n = n * 10 + digit;
            self.at += 1;
        }
        n as usize
    }

    /// How many bytes `option` takes (0 for the control options).
    unsafe fn size(&mut self, state: *mut lua_State, option: u8) -> usize {
        match option {
            b'b' | b'B' => 1,
            b'h' | b'H' => 2,
            b'l' | b'L' | b'T' => 8,
            b'f' => 4,
            b'd' => 8,
            b'x' => 1,
            b'c' => unsafe { self.number(state, 1) },
            b'i' | b'I' => {
                let size = unsafe { self.number(state, 4) };
                if size > MAX_INT_SIZE {
                    unsafe {
                        ffi::luaL_error(
                            state,
                            c"integral size %d is larger than limit of %d".as_ptr(),
                            size as c_int,
                            MAX_INT_SIZE as c_int,
                        )
                    };
                }
                size
            }
            _ => 0,
        }
    }

    /// `<`, `>`, `!n` and space; anything else is not an option.
    unsafe fn control(&mut self, state: *mut lua_State, option: u8, header: &mut Header) {
        match option {
            b' ' => {}
            b'>' => header.little = false,
            b'<' => header.little = true,
            b'!' => {
                let align = unsafe { self.number(state, MAX_ALIGN) };
                if !align.is_power_of_two() {
                    unsafe {
                        ffi::luaL_error(
                            state,
                            c"alignment %d is not a power of 2".as_ptr(),
                            align as c_int,
                        )
                    };
                }
                header.align = align;
            }
            _ => unsafe {
                let message = ffi::lua_pushfstring(
                    state,
                    c"invalid format option '%c'".as_ptr(),
                    option as c_int,
                );
                ffi::luaL_argerror(state, 1, message);
            },
        }
    }
}

/// Padding before an item of `size` bytes at `len`.
fn padding(len: usize, header: &Header, option: u8, size: usize) -> usize {
    if size == 0 || option == b'c' {
        return 0;
    }
    let size = size.min(header.align);
    size.wrapping_sub(len & size.wrapping_sub(1)) & size.wrapping_sub(1)
}

fn is_integer_option(option: u8) -> bool {
    matches!(
        option,
        b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I'
    )
}

/// An integer item: a negative number through a signed 64-bit integer,
/// the rest through an unsigned one, then `size` bytes of it.
fn put_integer(out: &mut [u8], n: f64, little: bool) {
    let mut value = if n < 0.0 { n as i64 as u64 } else { n as u64 };
    let size = out.len();
    for at in 0..size {
        let byte = (value & 0xff) as u8;
        if little {
            out[at] = byte;
        } else {
            out[size - 1 - at] = byte;
        }
        value >>= 8;
    }
}

/// Bytes in the order asked for, from native order.
fn put_ordered(out: &mut [u8], native: &[u8], little: bool) {
    out.copy_from_slice(native);
    if little != cfg!(target_endian = "little") {
        out.reverse();
    }
}

/// `struct.pack(fmt, ...)`. The first pass raises every error there is and
/// counts the bytes; the second writes them.
unsafe extern "C-unwind" fn struct_pack(state: *mut lua_State) -> c_int {
    unsafe {
        // After the arguments, as lua_struct leaves its mark there: an
        // argument missing reads as nil.
        ffi::lua_pushnil(state);
        let total = pack_pass(state, None);
        let buffer = ffi::lua_newuserdata(state, total.max(1)).cast::<u8>();
        let out = std::slice::from_raw_parts_mut(buffer, total);
        pack_pass(state, Some(out));
        ffi::lua_pushlstring(state, buffer.cast::<c_char>(), total);
        1
    }
}

/// One pass of [`struct_pack`]: without `out`, check and count; with it,
/// write (the checks have passed, and raise nothing).
unsafe fn pack_pass(state: *mut lua_State, mut out: Option<&mut [u8]>) -> usize {
    unsafe {
        let mut format = Format::new(state);
        let mut header = Header::new();
        let mut arg: c_int = 2;
        let mut total = 0usize;
        while let Some(option) = format.next() {
            let mut size = format.size(state, option);
            let pad = padding(total, &header, option, size);
            if let Some(out) = out.as_deref_mut() {
                out[total..total + pad].fill(0);
            }
            total += pad;
            match option {
                _ if is_integer_option(option) => {
                    let n = ffi::luaL_checknumber(state, arg);
                    arg += 1;
                    if let Some(out) = out.as_deref_mut() {
                        put_integer(&mut out[total..total + size], n, header.little);
                    }
                }
                b'x' => {
                    if let Some(out) = out.as_deref_mut() {
                        out[total] = 0;
                    }
                }
                b'f' => {
                    let n = ffi::luaL_checknumber(state, arg) as f32;
                    arg += 1;
                    if let Some(out) = out.as_deref_mut() {
                        put_ordered(&mut out[total..total + 4], &n.to_ne_bytes(), header.little);
                    }
                }
                b'd' => {
                    let n = ffi::luaL_checknumber(state, arg);
                    arg += 1;
                    if let Some(out) = out.as_deref_mut() {
                        put_ordered(&mut out[total..total + 8], &n.to_ne_bytes(), header.little);
                    }
                }
                b'c' | b's' => {
                    let mut len = 0usize;
                    let text = ffi::luaL_checklstring(state, arg, &mut len).cast::<u8>();
                    arg += 1;
                    if size == 0 {
                        size = len;
                    }
                    if len < size {
                        // As lua_struct reports it: the argument after.
                        ffi::luaL_argerror(state, arg, c"string too short".as_ptr());
                    }
                    if let Some(out) = out.as_deref_mut() {
                        out[total..total + size]
                            .copy_from_slice(std::slice::from_raw_parts(text, size));
                    }
                    if option == b's' {
                        if let Some(out) = out.as_deref_mut() {
                            out[total + size] = 0;
                        }
                        size += 1;
                    }
                }
                _ => format.control(state, option, &mut header),
            }
            total += size;
        }
        total
    }
}

/// An integer item read back, as a number.
fn get_integer(bytes: &[u8], little: bool, signed: bool) -> f64 {
    let mut value: u64 = 0;
    let mut add = |byte: u8| value = (value << 8) | byte as u64;
    if little {
        bytes.iter().rev().for_each(|&b| add(b));
    } else {
        bytes.iter().for_each(|&b| add(b));
    }
    if !signed {
        return value as f64;
    }
    let mask = u64::MAX.wrapping_shl((bytes.len() * 8).wrapping_sub(1) as u32);
    if value & mask != 0 {
        value |= mask;
    }
    value as i64 as f64
}

fn get_ordered<const N: usize>(bytes: &[u8], little: bool) -> [u8; N] {
    let mut out = [0u8; N];
    put_ordered(&mut out, bytes, little);
    out
}

/// `struct.unpack(fmt, data [, at])`: the items, then the position after
/// them.
unsafe extern "C-unwind" fn struct_unpack(state: *mut lua_State) -> c_int {
    unsafe {
        let mut format = Format::new(state);
        let mut len = 0usize;
        let data = ffi::luaL_checklstring(state, 2, &mut len).cast::<u8>();
        let data = std::slice::from_raw_parts(data, len);
        let start = ffi::luaL_optinteger(state, 3, 1);
        if start <= 0 {
            ffi::luaL_argerror(state, 3, c"offset must be 1 or greater".as_ptr());
        }
        let mut pos = start as usize - 1;
        let mut header = Header::new();
        let mut results: c_int = 0;
        while let Some(option) = format.next() {
            let mut size = format.size(state, option);
            pos += padding(pos, &header, option, size);
            if size > len || pos > len - size {
                ffi::luaL_argerror(state, 2, c"data string too short".as_ptr());
            }
            ffi::luaL_checkstack(state, 2, c"too many results".as_ptr());
            match option {
                _ if is_integer_option(option) => {
                    let n = get_integer(
                        &data[pos..pos + size],
                        header.little,
                        option.is_ascii_lowercase(),
                    );
                    ffi::lua_pushnumber(state, n);
                    results += 1;
                }
                b'x' => {}
                b'f' => {
                    let n = f32::from_ne_bytes(get_ordered(&data[pos..pos + 4], header.little));
                    ffi::lua_pushnumber(state, n as f64);
                    results += 1;
                }
                b'd' => {
                    let n = f64::from_ne_bytes(get_ordered(&data[pos..pos + 8], header.little));
                    ffi::lua_pushnumber(state, n);
                    results += 1;
                }
                b'c' => {
                    if size == 0 {
                        if results == 0 || ffi::lua_isnumber(state, -1) == 0 {
                            ffi::luaL_error(state, c"format 'c0' needs a previous size".as_ptr());
                        }
                        size = ffi::lua_tonumber(state, -1) as usize;
                        ffi::lua_pop(state, 1);
                        results -= 1;
                        if size > len || pos > len - size {
                            ffi::luaL_argerror(state, 2, c"data string too short".as_ptr());
                        }
                    }
                    ffi::lua_pushlstring(state, data[pos..].as_ptr().cast(), size);
                    results += 1;
                }
                b's' => {
                    let Some(end) = memchr::memchr(0, &data[pos..]) else {
                        ffi::luaL_error(state, c"unfinished string in data".as_ptr());
                        return 0;
                    };
                    ffi::lua_pushlstring(state, data[pos..].as_ptr().cast(), end);
                    size = end + 1;
                    results += 1;
                }
                _ => format.control(state, option, &mut header),
            }
            pos += size;
        }
        ffi::lua_pushinteger(state, (pos + 1) as ffi::lua_Integer);
        results + 1
    }
}

/// `struct.size(fmt)`: the bytes a format packs to.
unsafe extern "C-unwind" fn struct_size(state: *mut lua_State) -> c_int {
    unsafe {
        let mut format = Format::new(state);
        let mut header = Header::new();
        let mut pos = 0usize;
        while let Some(option) = format.next() {
            let size = format.size(state, option);
            pos += padding(pos, &header, option, size);
            if option == b's' {
                ffi::luaL_argerror(state, 1, c"option 's' has no fixed size".as_ptr());
            } else if option == b'c' && size == 0 {
                ffi::luaL_argerror(state, 1, c"option 'c0' has no fixed size".as_ptr());
            }
            if !option.is_ascii_alphanumeric() {
                format.control(state, option, &mut header);
            }
            pos += size;
        }
        ffi::lua_pushinteger(state, pos as ffi::lua_Integer);
        1
    }
}

// ── unpack ──────────────────────────────────────────────────────────────────

/// `unpack(t [, i [, j]])`, as Lua 5.1's, but with the element count
/// computed without overflow: 5.1.5's `e - i + 1` overflows for
/// `unpack(t, -2, 2147483647)` and takes the server down. Too many is
/// "too many results to unpack", as in Redis.
pub(super) unsafe extern "C-unwind" fn unpack(state: *mut lua_State) -> c_int {
    unsafe {
        ffi::luaL_checktype(state, 1, ffi::LUA_TTABLE);
        let first = ffi::luaL_optinteger(state, 2, 1) as c_int;
        let last = if ffi::lua_isnoneornil(state, 3) != 0 {
            ffi::lua_objlen(state, 1) as c_int
        } else {
            ffi::luaL_checkinteger(state, 3) as c_int
        };
        if first > last {
            return 0;
        }
        let count = (last as u32).wrapping_sub(first as u32);
        if count >= c_int::MAX as u32 || ffi::lua_checkstack(state, (count + 1) as c_int) == 0 {
            return ffi::luaL_error(state, c"too many results to unpack".as_ptr());
        }
        let mut at = first;
        loop {
            ffi::lua_rawgeti(state, 1, at as ffi::lua_Integer);
            if at == last {
                break;
            }
            at += 1;
        }
        (count + 1) as c_int
    }
}
