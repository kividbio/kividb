//! The Lua 5.1 library functions that Redis's scripts have and Lua 5.4 does
//! not (#898): `table.getn`, `table.setn`, `table.maxn`, `table.foreach`,
//! `table.foreachi`, `math.mod`, `string.gfind` and the global `gcinfo`.
//!
//! Older scripts use them (hibiken/asynq's `forwardCmd` ends with
//! `return table.getn(ids)`), and Redis keeps them in its Lua 5.1 build.
//!
//! They are made when a script first names one, not when the VM is made, so
//! that a VM whose scripts never do holds almost nothing for them (the
//! `used_memory_vm_functions` test of Redis's suite has little room to
//! spare). `table`, `math` and `string` share a metatable whose `__index`,
//! which runs only for a key the table does not have, makes all eight and
//! puts them in place; the global `gcinfo` is made by the globals' own
//! `__index` (see `new_lua_vm`). A function is there for `type(table.getn)`
//! and every other use as soon as it is looked at; only `pairs(table)` does
//! not list them until then. What they call is taken from the globals when they are made. A script
//! that does not use them pays nothing per call. Each does what Lua 5.1's C
//! function does, including its errors (at the line of the script that
//! called, as a C function's are).
//!
//! Where 5.4 and 5.1 differ and these cannot be the same, the numbers are
//! the ones 5.1 would print: an integral result is an integer, so that
//! `'n=' .. table.maxn(t)` is `n=3`, not `n=3.0`.

use mlua::{Lua, Table, Value};

/// Sets up the lazy installation in a new VM, before the sandbox closes.
/// Returns the function that makes the functions and gives `gcinfo`, for
/// the globals' `__index`.
pub(crate) fn prepare(lua: &Lua) -> mlua::Result<mlua::Function> {
    let g = lua.globals();
    // What they call is taken from the globals when they are first made, and
    // nothing is held for it before: the VM of Redis's own suite is measured
    // to the kilobyte (`used_memory_vm_functions`).
    let make = lua.create_function(|lua, ()| make(lua))?;
    let hook = make.clone();
    let meta = lua.create_table()?;
    meta.set(
        "__index",
        lua.create_function(move |_, (lib, key): (Table, Value)| {
            let wanted = match &key {
                Value::String(k) => matches!(
                    &*k.as_bytes(),
                    b"getn" | b"setn" | b"maxn" | b"foreach" | b"foreachi" | b"mod" | b"gfind"
                ),
                _ => false,
            };
            if !wanted {
                return Ok(Value::Nil);
            }
            hook.call::<Value>(())?;
            // The library asked has it now, if it is one that holds it.
            lib.raw_get(key)
        })?,
    )?;
    for name in ["table", "math", "string"] {
        g.get::<Table>(name)?.set_metatable(Some(meta.clone()));
    }
    Ok(make)
}

/// Makes the eight, puts seven in their libraries, and returns `gcinfo`.
fn make(lua: &Lua) -> mlua::Result<Value> {
    let g = lua.globals();
    let env = lua.create_table()?;
    for name in [
        "type",
        "next",
        "rawlen",
        "rawget",
        "select",
        "error",
        "tonumber",
        "collectgarbage",
        "table",
        "math",
        "string",
    ] {
        env.set(name, g.get::<Value>(name)?)?;
    }
    let math: Table = g.get("math")?;
    env.set("fmod", math.get::<Value>("fmod")?)?;
    env.set("tointeger", math.get::<Value>("tointeger")?)?;
    let string: Table = g.get("string")?;
    env.set("gmatch", string.get::<Value>("gmatch")?)?;
    let gcinfo: Value = lua.load(INSTALL).set_name("=lua51_compat").call(env)?;
    lua.globals().raw_set("gcinfo", gcinfo.clone())?;
    Ok(gcinfo)
}

/// The chunk that makes them. It is given what they call, puts seven in
/// their libraries, and returns `gcinfo`.
const INSTALL: &str = r#"
local S = ...
local type, next, rawlen, rawget, select, error, tonumber =
    S.type, S.next, S.rawlen, S.rawget, S.select, S.error, S.tonumber
local fmod, tointeger, collectgarbage = S.fmod, S.tointeger, S.collectgarbage
local gmatch = S.gmatch
local tbl, mth, str = S.table, S.math, S.string

-- luaL_typerror: "bad argument #n to 'f' (t expected, got x)", at the line
-- of the script that called `f` (level 3: past this function and `f`; a
-- caller that is not Lua, a `pcall`, gives no position, as in Redis).
local function bad(n, name, want, ...)
    local got = select('#', ...) < n and 'no value' or type((select(n, ...)))
    error("bad argument #" .. n .. " to '" .. name .. "' (" .. want .. " expected, got " .. got .. ")", 3)
end

-- luaL_checknumber takes a number, or a string that reads as one.
local function number(n, name, ...)
    local v = (select(n, ...))
    local t = type(v)
    if t == 'number' then return v end
    if t == 'string' then
        local x = tonumber(v)
        if x ~= nil then return x end
    end
    -- One level more: this function stands between `bad` and `mod`.
    error("bad argument #" .. n .. " to '" .. name .. "' (number expected, got "
        .. (select('#', ...) < n and 'no value' or type(v)) .. ")", 3)
end

tbl.getn = function(...)
    local t = ...
    if type(t) ~= 'table' then bad(1, 'getn', 'table', ...) end
    return rawlen(t)
end

-- Lua 5.1.5 is built with luaL_setn a macro that does nothing, and so
-- `setn` raises this for any table.
tbl.setn = function(...)
    local t = ...
    if type(t) ~= 'table' then bad(1, 'setn', 'table', ...) end
    error("'setn' is obsolete", 2)
end

tbl.maxn = function(...)
    local t = ...
    if type(t) ~= 'table' then bad(1, 'maxn', 'table', ...) end
    local max = 0
    for k in next, t do
        if type(k) == 'number' and k > max then max = k end
    end
    return max
end

tbl.foreach = function(...)
    local t, f = ...
    if type(t) ~= 'table' then bad(1, 'foreach', 'table', ...) end
    if type(f) ~= 'function' then bad(2, 'foreach', 'function', ...) end
    for k, v in next, t do
        local r = f(k, v)
        if r ~= nil then return r end
    end
end

tbl.foreachi = function(...)
    local t, f = ...
    if type(t) ~= 'table' then bad(1, 'foreachi', 'table', ...) end
    if type(f) ~= 'function' then bad(2, 'foreachi', 'function', ...) end
    for i = 1, rawlen(t) do
        local r = f(i, rawget(t, i))
        if r ~= nil then return r end
    end
end

-- C's fmod on doubles. Lua 5.4's own `math.fmod` takes two integers as
-- integers, and fails on a zero divisor where 5.1 gives nan.
mth.mod = function(...)
    -- The second is checked first: Redis's build evaluates `fmod`'s
    -- arguments right to left, so `math.mod()` names argument #2.
    local b = number(2, 'mod', ...)
    local a = number(1, 'mod', ...)
    local r = fmod(a + 0.0, b + 0.0)
    return tointeger(r) or r
end

str.gfind = gmatch

-- The memory in use, in whole kilobytes.
return function()
    local kb = collectgarbage('count')
    return tointeger(kb - kb % 1)
end
"#;
