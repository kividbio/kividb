//! The Lua 5.1 library functions that Redis's scripts have and Lua 5.4 does
//! not (#898): `table.getn`, `table.setn`, `table.maxn`, `table.foreach`,
//! `table.foreachi`, `math.mod`, `string.gfind` and the global `gcinfo`.
//!
//! Older scripts use them (hibiken/asynq's `forwardCmd` ends with
//! `return table.getn(ids)`), and Redis keeps them in its Lua 5.1 build.
//!
//! They are installed once, when a script VM is made, from a chunk that
//! holds everything it uses as a local: nothing a script later does to the
//! globals changes what they call. Scripts that do not use them pay nothing
//! per call. Each does what Lua 5.1's C function does, including its errors
//! (at the line of the script that called, as a C function's are).
//!
//! Where 5.4 and 5.1 differ and these cannot be the same, the numbers are
//! the ones 5.1 would print: an integral result is an integer, so that
//! `'n=' .. table.maxn(t)` is `n=3`, not `n=3.0`.

/// The chunk that installs them; it runs in the VM's globals before the
/// sandbox is closed.
pub(crate) const INSTALL: &str = r#"
local type, next, rawlen, rawget, select, error, tonumber =
    type, next, rawlen, rawget, select, error, tonumber
local fmod, tointeger = math.fmod, math.tointeger
local collectgarbage, floor = collectgarbage, math.floor
local gmatch = string.gmatch
local tbl, mth, str = table, math, string

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
gcinfo = function()
    return floor(collectgarbage('count'))
end
"#;
