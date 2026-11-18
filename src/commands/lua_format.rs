//! `string.format` in scripts, with Lua 5.1's rules for integer
//! conversions (#747), compiled to a C function so a call costs what a C
//! call costs (#771).
//!
//! Scripts run on Lua 5.4, and Redis's on Lua 5.1. For `%d %i %o %u %x
//! %X %c`, 5.4 takes only a number with an integer value and fails on
//! any other ("number has no integer representation"), where 5.1 casts
//! the number to a C integer, cutting a fraction toward zero. Scripts
//! written for Redis lean on that, such as `string.format("%d%03d",
//! t[1], t[2] / 1000)` to make milliseconds from `TIME`.
//!
//! The function is a patched copy of Lua 5.4.7's own `str_format`, the
//! C in `csrc/lua51_str_format.c`, built by build.rs against the headers
//! of the same vendored Lua the library was built with: integer
//! conversions take `lua_isinteger ? lua_tointeger : (lua_Integer)(long)
//! luaL_checknumber`, check against 5.1's flag set (the same `-+ #0` for
//! every conversion, where 5.4 restricts them per conversion), and the
//! formatted item is added up to its first NUL, as 5.1's `strlen(buff)`
//! did. It formats with the host's `sprintf`, so out-of-range results
//! are Redis's on the same architecture, and an error names the argument
//! at fault, which the Rust wrapper it replaces could not always do.

use mlua::ffi;
use std::ffi::c_int;

unsafe extern "C" {
    /// The patched `str_format`, compiled from `csrc/lua51_str_format.c`.
    fn kividb_str_format(state: *mut ffi::lua_State) -> c_int;
}

/// The `lua_CFunction` over the C entry: `C-unwind`, as the binding asks.
unsafe extern "C-unwind" fn str_format(state: *mut ffi::lua_State) -> c_int {
    unsafe { kividb_str_format(state) }
}

/// Install it as `string.format`, where the library's own was.
///
/// SAFETY: the state has the `string` library, and the calls leave the
/// stack as they found it.
pub(crate) unsafe fn install(state: *mut ffi::lua_State) {
    unsafe {
        ffi::lua_checkstack(state, 2);
        ffi::lua_getglobal(state, c"string".as_ptr());
        ffi::lua_pushcfunction(state, str_format);
        ffi::lua_setfield(state, -2, c"format".as_ptr());
        ffi::lua_pop(state, 1);
    }
}
