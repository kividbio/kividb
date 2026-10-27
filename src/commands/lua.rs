//! Lua scripting (`EVAL`, `SCRIPT`, …) — compiled with `--features lua`.

use crate::commands::ctx::Ctx;
use crate::commands::handler::{client_connection_resp, execute_command_from_script};
use crate::commands::helpers::glob_match;
use crate::replication;
use crate::resp::write::{
    resp_array, resp_bignum_resp3, resp_bulk, resp_bulk_str, resp_double_bulk, resp_double_resp3,
    resp_error, resp_integer, resp_map, resp_null_bulk, resp_null_resp3, resp_set_resp3,
    resp_simple, resp_verbatim_resp3,
};
use crate::snapshot::{crc64_digest, redis_function_dump_sources};
use mlua::prelude::*;
use mlua::{HookTriggers, VmState};
use parking_lot::{Mutex as ParkingMutex, RwLock};
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

/// Redis's `struct` library (Roberto Ierusalimschy's lua_struct):
/// `struct.pack`, `struct.unpack`, `struct.size`, on Lua 5.4's `string.pack`,
/// one item at a time, so that `c0` (a string as long as the value before it)
/// works (#154). Its formats as Redis's: `s` is a zero-terminated string,
/// `l`, `L` and `T` are 8 bytes, `c` alone is one byte, numbers given for
/// integers are cut to integers. The chunk returns the library.
const STRUCT_LIBRARY: &str = r##"
local struct = {}
local function struct_items(fmt)
    local items, at = {}, 1
    while at <= #fmt do
        local option = fmt:sub(at, at)
        local size = fmt:match("^%d+", at + 1) or ""
        at = at + 1 + #size
        items[#items + 1] = { option, size }
    end
    return items
end
local struct_plain = {}
for option in ("xbBhHiIfd"):gmatch(".") do struct_plain[option] = true end
local function struct_option(option, size)
    if option == "s" then return "z" end
    if option == "l" then return "i8" end
    if option == "L" or option == "T" then return "I8" end
    if option == "c" then return "c" .. (size == "" and "1" or size) end
    if struct_plain[option] then return option .. size end
    error("invalid format option '" .. option .. "'")
end
local function struct_integer(value)
    local n = tonumber(value)
    if n == nil then error("bad argument to 'pack' (number expected)") end
    return math.tointeger(n) or (n >= 0 and math.floor(n) or math.ceil(n))
end
local function struct_state(option)
    return option == "<" or option == ">" or option == "=" or option == "!"
end
struct.pack = function(fmt, ...)
    local values, next_value, state, out = table.pack(...), 1, "", {}
    for _, item in ipairs(struct_items(fmt)) do
        local option, size = item[1], item[2]
        if struct_state(option) then
            state = state .. option .. size
        elseif option == " " then
        elseif option == "x" then
            out[#out + 1] = string.pack(state .. "x" .. size)
        elseif option == "c" and size == "0" then
            out[#out + 1] = tostring(values[next_value])
            next_value = next_value + 1
        else
            local value = values[next_value]
            next_value = next_value + 1
            if option == "s" or option == "c" then
                value = tostring(value)
            elseif option ~= "f" and option ~= "d" then
                value = struct_integer(value)
            end
            out[#out + 1] = string.pack(state .. struct_option(option, size), value)
        end
    end
    return table.concat(out)
end
struct.unpack = function(fmt, data, at)
    at = at or 1
    local state, out, last = "", {}, nil
    for _, item in ipairs(struct_items(fmt)) do
        local option, size = item[1], item[2]
        if struct_state(option) then
            state = state .. option .. size
        elseif option == " " then
        elseif option == "x" then
            at = string.unpack(state .. "x" .. size, data, at)
        elseif option == "c" and size == "0" then
            local length = math.tointeger(last)
            if length == nil then error("format 'c0' needs a previous size") end
            if at + length - 1 > #data then error("data string too short") end
            last = data:sub(at, at + length - 1)
            out[#out + 1] = last
            at = at + length
        else
            local value
            value, at = string.unpack(state .. struct_option(option, size), data, at)
            out[#out + 1] = value
            last = value
        end
    end
    out[#out + 1] = at
    return table.unpack(out)
end
struct.size = function(fmt)
    local state, total = "", 0
    for _, item in ipairs(struct_items(fmt)) do
        local option, size = item[1], item[2]
        if struct_state(option) then
            state = state .. option .. size
        elseif option == "s" or (option == "c" and size == "0") then
            error("options 'c0' - 's' have undefined sizes")
        elseif option ~= " " then
            total = total + string.packsize(state .. struct_option(option, size))
        end
    end
    return total
end
return struct
"##;

/// Scripts by SHA1. A source is shared with whoever runs it: scripts are a
/// few kilobytes each, and one was copied whole for every call.
static SCRIPT_STORE: OnceLock<RwLock<HashMap<String, Arc<str>>>> = OnceLock::new();

fn script_store() -> &'static RwLock<HashMap<String, Arc<str>>> {
    SCRIPT_STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// How many scripts sent only with `EVAL` the cache keeps, as Redis 7.4:
/// past this, the least recently used goes. A client that writes its values
/// into the script, rather than passing them as arguments, grew the cache
/// for ever (#155). `SCRIPT LOAD`ed scripts are not counted, and stay.
const EVAL_SCRIPTS_MAX: usize = 500;

/// The scripts sent only with `EVAL`, by when they were last run.
#[derive(Default)]
struct EvalScripts {
    by_use: std::collections::BTreeMap<u64, String>,
    last_used: HashMap<String, u64>,
    clock: u64,
}

static EVAL_SCRIPTS: std::sync::LazyLock<parking_lot::Mutex<EvalScripts>> =
    std::sync::LazyLock::new(Default::default);

/// A script `EVAL` has just put in the cache: counted, and the least
/// recently used let go past [`EVAL_SCRIPTS_MAX`].
fn eval_script_added(sha: &str) {
    let mut scripts = EVAL_SCRIPTS.lock();
    scripts.clock += 1;
    let now = scripts.clock;
    if let Some(was) = scripts.last_used.insert(sha.to_string(), now) {
        scripts.by_use.remove(&was);
    }
    scripts.by_use.insert(now, sha.to_string());
    while scripts.by_use.len() > EVAL_SCRIPTS_MAX {
        let Some((_, oldest)) = scripts.by_use.pop_first() else {
            break;
        };
        scripts.last_used.remove(&oldest);
        script_store().write().remove(&oldest);
        crate::stats::EVICTED_SCRIPTS.fetch_add(1, Ordering::Relaxed);
    }
}

/// A cached script has been run: if it is one `EVAL` sent, it is now the most
/// recently used.
fn eval_script_used(sha: &str) {
    let mut scripts = EVAL_SCRIPTS.lock();
    let Some(was) = scripts.last_used.get(sha).copied() else {
        return;
    };
    scripts.clock += 1;
    let now = scripts.clock;
    scripts.by_use.remove(&was);
    scripts.by_use.insert(now, sha.to_string());
    scripts.last_used.insert(sha.to_string(), now);
}

/// `SCRIPT LOAD` of a script: kept from now on, whatever `EVAL` sent.
fn eval_script_loaded(sha: &str) {
    let mut scripts = EVAL_SCRIPTS.lock();
    if let Some(was) = scripts.last_used.remove(sha) {
        scripts.by_use.remove(&was);
    }
}

/// Whether a script's first line is a shebang with the `no-writes` flag.
pub(crate) fn script_declares_no_writes(source: &[u8]) -> bool {
    let line = source.split(|b| *b == b'\n').next().unwrap_or_default();
    line.starts_with(b"#!")
        && std::str::from_utf8(line).is_ok_and(|line| {
            line.split_whitespace()
                .filter_map(|word| word.strip_prefix("flags="))
                .any(|flags| flags.split(',').any(|flag| flag.trim() == "no-writes"))
        })
}

/// Whether a script says it may write: a shebang without `no-writes`. One
/// with no shebang says nothing, and runs on a replica until it writes.
pub(crate) fn script_declares_writes(source: &[u8]) -> bool {
    source.starts_with(b"#!") && !script_declares_no_writes(source)
}

/// [`script_declares_writes`] of the cached script with this SHA1.
pub(crate) fn cached_script_declares_writes(sha: &[u8]) -> bool {
    let sha = String::from_utf8_lossy(sha).to_ascii_lowercase();
    script_store()
        .read()
        .get(&sha)
        .is_some_and(|source| script_declares_writes(source.as_bytes()))
}

/// The same, of the cached script with this SHA1.
pub(crate) fn cached_script_declares_no_writes(sha: &[u8]) -> bool {
    let sha = String::from_utf8_lossy(sha).to_ascii_lowercase();
    script_store()
        .read()
        .get(&sha)
        .is_some_and(|source| script_declares_no_writes(source.as_bytes()))
}

/// Functions registered with the `no-writes` flag.
static NO_WRITES_FUNCTIONS: RwLock<Option<std::collections::HashSet<String>>> = RwLock::new(None);

/// Whether the function of this name was registered with `no-writes`.
pub(crate) fn function_declares_no_writes(name: &[u8]) -> bool {
    let name = String::from_utf8_lossy(name);
    NO_WRITES_FUNCTIONS
        .read()
        .as_ref()
        .is_some_and(|functions| functions.contains(name.as_ref()))
}

/// Script bodies cached for `SCRIPT LOAD` / `EVALSHA` (Redis INFO field).
pub(crate) fn cached_script_count() -> usize {
    script_store().read().len()
}

/// `used_memory_scripts_eval`: what the cached scripts' text takes, with
/// their SHA1s.
pub(crate) fn cached_scripts_bytes() -> u64 {
    script_store()
        .read()
        .iter()
        .map(|(sha, body)| (sha.len() + body.len()) as u64)
        .sum()
}

#[derive(Clone)]
struct FunctionLibrary {
    name: String,
    engine: String,
    body: String,
    functions: Vec<String>,
    /// What each function was registered with, for `FUNCTION LIST`. Not
    /// part of a dump: running the library again says it again.
    registered: Vec<Registered>,
}

/// One `redis.register_function`.
#[derive(Clone, Debug, PartialEq)]
struct Registered {
    name: String,
    description: Option<String>,
    flags: Vec<String>,
}

/// The flags a function may be registered with, in the order Redis lists
/// them.
const FUNCTION_FLAGS: &[&str] = &[
    "no-writes",
    "allow-oom",
    "allow-stale",
    "no-cluster",
    "allow-cross-slot-keys",
];

static FUNCTION_STORE: OnceLock<RwLock<HashMap<String, FunctionLibrary>>> = OnceLock::new();

fn function_store() -> &'static RwLock<HashMap<String, FunctionLibrary>> {
    FUNCTION_STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Maps Lua function name → library shebang name (`#!lua name=<lib>`). Required because
/// `FCALL` resolves by function name only; scanning `function_store.values()` is nondeterministic
/// and can load the wrong library body into the VM after a registry miss.
static FN_NAME_TO_LIBRARY: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

fn fn_name_to_library() -> &'static RwLock<HashMap<String, String>> {
    FN_NAME_TO_LIBRARY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// The shared Lua VMs (Redis-compatible): scripts do not run in parallel. A
/// second script, and every other client's command, waits for the one that
/// is running — and is told `BUSY` only once that one has run for longer
/// than `lua-time-limit`. (They used to be told `BUSY` at once, so any two
/// clients using scripts at the same time saw errors.)
/// Requires mlua `send` so `Lua` is `Send` and can live behind a static `Mutex`.
///
/// Two VMs behind the one lock, as Redis has two: one for `EVAL` scripts
/// and one the function libraries are loaded into. What one holds (`EVAL`'s
/// compiled scripts, say) is not counted against the other
/// (`used_memory_lua`, `used_memory_vm_functions`), and flushing one leaves
/// the other. The lock is still one: scripts and functions take turns.
static LUA_VM: ParkingMutex<Vms> = ParkingMutex::new(Vms {
    eval: None,
    functions: None,
});

struct Vms {
    eval: Option<Lua>,
    functions: Option<Lua>,
}

impl Vms {
    fn of(&mut self, vm: Vm) -> &mut Option<Lua> {
        match vm {
            Vm::Eval => &mut self.eval,
            Vm::Functions => &mut self.functions,
        }
    }
}

/// Which of the two VMs (see [`LUA_VM`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Vm {
    Eval,
    Functions,
}

impl Vm {
    /// Its size when it was last let go, for `INFO`.
    fn bytes(self) -> &'static AtomicU64 {
        match self {
            Vm::Eval => &EVAL_VM_BYTES,
            Vm::Functions => &FUNCTIONS_VM_BYTES,
        }
    }

    /// How many times it has been flushed: each thread's own VM of this
    /// kind (parallel scripts) is made again when this has moved on.
    fn generation(self) -> &'static AtomicU64 {
        static EVAL: AtomicU64 = AtomicU64::new(0);
        static FUNCTIONS: AtomicU64 = AtomicU64::new(0);
        match self {
            Vm::Eval => &EVAL,
            Vm::Functions => &FUNCTIONS,
        }
    }
}
thread_local! {
    /// Points at the active [`Lua`] while the global VM mutex is held on this thread.
    static EXEC_LUA: RefCell<Option<std::ptr::NonNull<Lua>>> = const { RefCell::new(None) };
}
/// Nested `try_with_lua` / nested eval on the same OS thread.
static LUA_HOLD_DEPTH: AtomicUsize = AtomicUsize::new(0);
/// True while at least one thread holds `LUA_VM` (outermost acquisition).
static LUA_SCRIPT_BUSY_OTHERS: AtomicBool = AtomicBool::new(false);
/// Unix ms at which the script now holding the VM took it.
static LUA_SCRIPT_STARTED_MS: AtomicU64 = AtomicU64::new(0);
/// Whether the script now holding the VM has written. `SCRIPT KILL` comes
/// from another client, on another thread, and the script's own note of it
/// is in its thread's [`SCRIPT_EVAL`]: read there, it was always no, and a
/// script that had written was answered OK and ran on (#129).
static LUA_HOLDER_WROTE: AtomicBool = AtomicBool::new(false);
/// Bytes the functions' VM held when it was last let go: `INFO`'s
/// `used_memory_vm_functions`, read without waiting for a running script.
static FUNCTIONS_VM_BYTES: AtomicU64 = AtomicU64::new(0);
/// `used_memory_lua`: the `EVAL` VM, as [`FUNCTIONS_VM_BYTES`] is read.
static EVAL_VM_BYTES: AtomicU64 = AtomicU64::new(0);

/// `used_memory_lua`: the VM `EVAL` scripts run in.
pub(crate) fn eval_vm_bytes() -> u64 {
    EVAL_VM_BYTES.load(Ordering::Relaxed)
}

/// `used_memory_vm_functions`: the VM the functions are loaded into.
pub(crate) fn functions_vm_bytes() -> u64 {
    FUNCTIONS_VM_BYTES.load(Ordering::Relaxed)
}

/// Whether the script that holds the VM has been running for longer than
/// `lua-time-limit`: the point at which waiting for it stops and `BUSY`
/// begins. Never, with the limit set to 0.
pub(crate) fn running_script_is_over_its_time_limit() -> bool {
    let limit = LUA_TIME_LIMIT_MS.load(Ordering::Relaxed);
    let over = limit > 0
        && LUA_SCRIPT_BUSY_OTHERS.load(Ordering::Acquire)
        && crate::replication::unix_ms()
            .saturating_sub(LUA_SCRIPT_STARTED_MS.load(Ordering::Relaxed))
            > limit;
    if over {
        log_throttled!(
            10,
            crate::loglevel::WARN,
            "A script has run for more than lua-time-limit ({limit} ms): other clients now get \
             BUSY. SCRIPT KILL (or FUNCTION KILL) stops it if it has not written yet; otherwise \
             only SHUTDOWN NOSAVE does"
        );
    }
    over
}

/// How long one wait for the VM lasts before the time limit is looked at
/// again.
const VM_WAIT_SLICE: std::time::Duration = std::time::Duration::from_millis(10);
/// Max script runtime in ms (0 = no limit). Default matches Redis.
static LUA_TIME_LIMIT_MS: AtomicU64 = AtomicU64::new(5000);
static LUA_SCRIPT_KILL_REQUESTED: AtomicBool = AtomicBool::new(false);
/// After the first SCRIPT KILL hook fires, we escalate to `every_line` (Redis `script_lua.c`) so
/// `pcall` cannot swallow the kill error and spin forever on an outer `while` loop.
static LUA_KILL_HOOK_ESCALATED: AtomicBool = AtomicBool::new(false);

thread_local! {
    /// Populated during `FUNCTION LOAD` via `redis.register_function`.
    static REGISTER_PENDING: RefCell<Vec<Registered>> = const { RefCell::new(Vec::new()) };
    /// Only push to [`REGISTER_PENDING`] while loading a library (not FCALL re-init).
    static IN_FUNCTION_LOAD: RefCell<bool> = const { RefCell::new(false) };
    /// A library that is loaded is being run again, in a VM that has not
    /// run it (see `fcall_inner`): what it registers is callable at once.
    static RERUNNING_LIBRARY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Where a function a library registers is kept in the VM's registry.
fn function_key(name: &str) -> String {
    format!("__fn_{name}")
}

/// Where it is kept until the whole library has loaded: a load that fails
/// must leave nothing behind, and above all not a function of the same name
/// from the library it was to replace.
fn pending_function_key(name: &str) -> String {
    format!("__fn_pending_{name}")
}

fn lua_runtime_error<T>(message: &str) -> LuaResult<T> {
    Err(LuaError::RuntimeError(message.to_string()))
}

/// `redis.register_function`, as a library's code sees it while it loads.
///
/// ```text
/// redis.register_function('name', fn)
/// redis.register_function{function_name='name', callback=fn,
///                         description='…', flags={'no-writes', …}}
/// ```
fn register_function(lua: &Lua, args: LuaMultiValue) -> LuaResult<()> {
    let loading = IN_FUNCTION_LOAD.with(|load| *load.borrow());
    let rerunning = RERUNNING_LIBRARY.with(|rerun| rerun.get());
    if !loading && !rerunning {
        return lua_runtime_error(
            "redis.register_function can only be called on FUNCTION LOAD command",
        );
    }
    let args: Vec<LuaValue> = args.into_iter().collect();
    let mut description = None;
    let mut flags: Vec<String> = Vec::new();
    let (name, callback) = match args.as_slice() {
        [LuaValue::Table(named)] => {
            let mut name = None;
            let mut callback = None;
            for pair in named.pairs::<LuaValue, LuaValue>() {
                let (key, value) = pair?;
                let LuaValue::String(key) = key else {
                    return lua_runtime_error(
                        "named argument key given to redis.register_function is not a string",
                    );
                };
                match (key.as_bytes().as_ref(), value) {
                    (b"function_name", LuaValue::String(value)) => name = Some(value),
                    (b"function_name", _) => {
                        return lua_runtime_error(
                            "function_name argument given to redis.register_function must be a string",
                        );
                    }
                    (b"description", LuaValue::String(value)) => {
                        description = Some(value.to_string_lossy());
                    }
                    (b"description", _) => {
                        return lua_runtime_error(
                            "description argument given to redis.register_function must be a string",
                        );
                    }
                    (b"callback", LuaValue::Function(value)) => callback = Some(value),
                    (b"callback", _) => {
                        return lua_runtime_error(
                            "callback argument given to redis.register_function must be a function",
                        );
                    }
                    (b"flags", LuaValue::Table(given)) => {
                        for flag in given.sequence_values::<LuaValue>() {
                            let flag = match flag? {
                                LuaValue::String(flag) => flag.to_string_lossy(),
                                _ => return lua_runtime_error("unknown flag given"),
                            };
                            if !FUNCTION_FLAGS.contains(&flag.as_str()) {
                                return lua_runtime_error("unknown flag given");
                            }
                            if !flags.contains(&flag) {
                                flags.push(flag);
                            }
                        }
                    }
                    (b"flags", _) => {
                        return lua_runtime_error(
                            "flags argument to redis.register_function must be a table representing function flags",
                        );
                    }
                    _ => {
                        return lua_runtime_error(
                            "unknown argument given to redis.register_function",
                        );
                    }
                }
            }
            let Some(name) = name else {
                return lua_runtime_error(
                    "redis.register_function must get a function name argument",
                );
            };
            let Some(callback) = callback else {
                return lua_runtime_error("redis.register_function must get a callback argument");
            };
            (name, callback)
        }
        [_] => {
            return lua_runtime_error(
                "calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).",
            );
        }
        [name, callback] => {
            let LuaValue::String(name) = name else {
                return lua_runtime_error(
                    "first argument to redis.register_function must be a string",
                );
            };
            let LuaValue::Function(callback) = callback else {
                return lua_runtime_error(
                    "second argument to redis.register_function must be a function",
                );
            };
            (name.clone(), callback.clone())
        }
        _ => return lua_runtime_error("wrong number of arguments to redis.register_function"),
    };

    let name_bytes = name.as_bytes();
    if name_bytes.is_empty()
        || !name_bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        // The words are Redis's, which says "Library" of a function's name
        // too.
        return lua_runtime_error(
            "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long",
        );
    }
    let name = String::from_utf8_lossy(&name_bytes).into_owned();
    flags.sort_by_key(|flag| FUNCTION_FLAGS.iter().position(|known| known == flag));

    if rerunning {
        return lua.set_named_registry_value(&function_key(&name), callback);
    }
    if REGISTER_PENDING.with(|pending| pending.borrow().iter().any(|other| other.name == name)) {
        return lua_runtime_error("Function already exists in the library");
    }
    lua.set_named_registry_value(&pending_function_key(&name), callback)?;
    REGISTER_PENDING.with(|pending| {
        pending.borrow_mut().push(Registered {
            name,
            description,
            flags,
        })
    });
    Ok(())
}

/// The tables a library's code runs with: its environment, that table's
/// metatable, and what the metatable points at while the library loads.
///
/// A library is loaded with next to nothing in reach — `redis`, holding
/// `register_function`, `log` and little else — so that loading it cannot
/// touch the dataset or depend on what happens to be there. The functions
/// it registers are then called with everything a script has. Both have to
/// be the same environment, since the functions are closures made while the
/// library loaded: so the environment is an empty table that reads through
/// to one table while a library loads, and to the globals the rest of the
/// time. Nothing is ever written through it.
fn function_environment(lua: &Lua) -> LuaResult<(LuaTable, LuaTable, LuaTable)> {
    const ENV: &str = "__kividb_fn_env";
    const META: &str = "__kividb_fn_env_meta";
    const LOADING: &str = "__kividb_fn_loading";
    if let Ok(env) = lua.named_registry_value::<LuaTable>(ENV) {
        return Ok((
            env,
            lua.named_registry_value(META)?,
            lua.named_registry_value(LOADING)?,
        ));
    }

    let missing = lua.create_function(|_, (_, key): (LuaValue, LuaValue)| -> LuaResult<()> {
        let key = match &key {
            LuaValue::String(key) => key.to_string_lossy(),
            other => format!("{other:?}"),
        };
        lua_runtime_error(&format!(
            "Script attempted to access nonexistent global variable '{key}'"
        ))
    })?;
    let read_only = lua.create_function(|_, _: LuaMultiValue| -> LuaResult<()> {
        lua_runtime_error("Attempt to modify a readonly table")
    })?;
    let guarded = |table: &LuaTable| -> LuaResult<()> {
        let meta = lua.create_table()?;
        meta.raw_set("__index", missing.clone())?;
        meta.raw_set("__newindex", read_only.clone())?;
        table.set_metatable(Some(meta));
        Ok(())
    };

    let everything: LuaTable = lua.named_registry_value("__kividb_redis_inner")?;
    let api = lua.create_table()?;
    for key in [
        "log",
        "LOG_DEBUG",
        "LOG_VERBOSE",
        "LOG_NOTICE",
        "LOG_WARNING",
        "REDIS_VERSION",
        "REDIS_VERSION_NUM",
    ] {
        let value: LuaValue = everything.raw_get(key)?;
        if !value.is_nil() {
            api.raw_set(key, value)?;
        }
    }
    api.raw_set("register_function", lua.create_function(register_function)?)?;
    guarded(&api)?;
    let loading = lua.create_table()?;
    loading.raw_set("redis", api)?;
    guarded(&loading)?;

    let meta = lua.create_table()?;
    meta.raw_set("__index", lua.globals())?;
    meta.raw_set("__newindex", read_only)?;
    let env = lua.create_table()?;
    env.set_metatable(Some(meta.clone()));

    lua.set_named_registry_value(ENV, env.clone())?;
    lua.set_named_registry_value(META, meta.clone())?;
    lua.set_named_registry_value(LOADING, loading.clone())?;
    Ok((env, meta, loading))
}

/// Run a library's code (its shebang line removed).
fn run_library_code(lua: &Lua, code: &str) -> LuaResult<()> {
    let (env, meta, loading) = function_environment(lua)?;
    meta.raw_set("__index", loading)?;
    let result = table_errors(
        lua.load(code)
            .set_name("=library")
            .set_environment(env)
            .exec(),
    );
    meta.raw_set("__index", lua.globals())?;
    result
}

/// What a Lua error says, without the traceback and the wrapping.
fn lua_error_message(error: &LuaError) -> String {
    let message = match error {
        LuaError::CallbackError { cause, .. } => return lua_error_message(cause),
        LuaError::RuntimeError(message) => message.clone(),
        LuaError::SyntaxError { message, .. } => message.clone(),
        other => other.to_string(),
    };
    if let Some((_, msg)) = marked_table_error(&message) {
        return msg.to_string();
    }
    message
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// A step of the collector every [`GC_LOADS`] loads, as Redis's `luaGC`
/// takes one. A load leaves the chunk it ran behind, and the collector left
/// to itself lets a run of loads pile that up: 50,000 small functions held
/// 18.7 MB, of which 14.3 MB was live (what a full collection leaves).
/// Redis steps 50 KB, but a step of Lua 5.4's is of a debt in allocation,
/// not 5.1's: 50 KB left 16.3 MB, [`GC_STEP_KB`] leaves 14.4 MB.
fn gc_after_load(lua: &Lua) {
    static LOADS: AtomicU32 = AtomicU32::new(0);
    if LOADS.fetch_add(1, Ordering::Relaxed) + 1 >= GC_LOADS {
        LOADS.store(0, Ordering::Relaxed);
        let _ = lua.gc_step_kbytes(GC_STEP_KB);
    }
}

const GC_LOADS: u32 = 50;
const GC_STEP_KB: i32 = 1000;

/// Run a library as `FUNCTION LOAD` runs it, and return what it registered.
/// The functions are not callable yet: [`commit_registered`] makes them so,
/// [`discard_registered`] forgets them.
fn load_library_code(
    lua: &Lua,
    body: &str,
    time_limit: Option<std::time::Duration>,
) -> Result<Vec<Registered>, String> {
    REGISTER_PENDING.with(|cell| cell.borrow_mut().clear());
    IN_FUNCTION_LOAD.with(|c| *c.borrow_mut() = true);
    if let Some(limit) = time_limit {
        // Loading registers functions; it has no business taking long, and
        // while it runs nothing else does.
        let started = std::time::Instant::now();
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(1000),
            move |_, _| {
                if started.elapsed() > limit {
                    return lua_runtime_error("FUNCTION LOAD timeout");
                }
                Ok(VmState::Continue)
            },
        );
    }
    let result = run_library_code(lua, &strip_shebang_for_lua(body));
    if time_limit.is_some() {
        lua.remove_hook();
        // A VM has one hook: the next script installs its own again.
        lua.remove_app_data::<KillHookInstalled>();
    }
    IN_FUNCTION_LOAD.with(|c| *c.borrow_mut() = false);
    let registered = REGISTER_PENDING.with(|cell| std::mem::take(&mut *cell.borrow_mut()));

    if let Err(error) = result {
        discard_registered(lua, &registered);
        let what = match &error {
            LuaError::SyntaxError { .. } => "Error compiling function",
            _ => "Error registering functions",
        };
        return Err(format!("ERR {what}: {}", lua_error_message(&error)));
    }
    if registered.is_empty() {
        return Err("ERR No functions registered".into());
    }
    Ok(registered)
}

fn discard_registered(lua: &Lua, registered: &[Registered]) {
    for function in registered {
        let _ = lua.unset_named_registry_value(&pending_function_key(&function.name));
    }
}

/// Make the functions a library registered callable, and forget the ones
/// in `replaced` that it did not register again.
fn commit_registered(lua: &Lua, registered: &[Registered], replaced: &[String]) -> LuaResult<()> {
    for old in replaced {
        lua.unset_named_registry_value(&function_key(old))?;
    }
    for function in registered {
        let pending = pending_function_key(&function.name);
        let callback: LuaFunction = lua.named_registry_value(&pending)?;
        lua.set_named_registry_value(&function_key(&function.name), callback)?;
        lua.unset_named_registry_value(&pending)?;
    }
    let mut no_writes = NO_WRITES_FUNCTIONS.write();
    let no_writes = no_writes.get_or_insert_with(Default::default);
    for old in replaced {
        no_writes.remove(old);
    }
    for function in registered {
        if function.flags.iter().any(|flag| flag == "no-writes") {
            no_writes.insert(function.name.clone());
        } else {
            no_writes.remove(&function.name);
        }
    }
    Ok(())
}

/// The `err` of `redis.error_reply(s)`, as Redis's `luaPushErrorBuff` makes
/// it: `-CODE msg` and `CODE msg` keep their code, a message with no space
/// in it (so no code) is given `ERR`, and line breaks at the ends go.
fn error_reply_message(s: &str) -> String {
    let s = s.strip_prefix('-').unwrap_or(s);
    // Empty must still classify as an ERR reply (Tcl `errorrstat ERR`).
    if s.is_empty() {
        return "ERR".to_string();
    }
    match s.split_once(' ') {
        Some((code, msg)) => format!("{code} {}", msg.trim_matches(['\r', '\n'])),
        None => format!("ERR {}", s.trim_matches(['\r', '\n'])),
    }
}

fn sha1_hex(source: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(source.as_bytes());
    format!("{:x}", hasher.finalize())
}

thread_local! {
    /// With the [`Vm::generation`] it was made in: a flush after that
    /// makes it stale, and it is made again when next wanted.
    static LUA: RefCell<Option<(u64, Lua)>> = const { RefCell::new(None) };
    /// This thread's own VM for functions, beside [`LUA`] for scripts.
    static LUA_FUNCTIONS: RefCell<Option<(u64, Lua)>> = const { RefCell::new(None) };
}

/// Clears script evaluation context when leaving `EVAL` / `FCALL`.
struct ClearScriptEval<'a> {
    lua: &'a Lua,
}

impl Drop for ClearScriptEval<'_> {
    fn drop(&mut self) {
        SCRIPT_EVAL.with(|c| {
            *c.borrow_mut() = None;
        });
        // The arguments are let go of. What stands in for them until the
        // next script sets its own is one empty table kept with the VM: no
        // script sees it, each being given new tables before it runs.
        let empty = match self.lua.app_data_ref::<EmptyArguments>() {
            Some(empty) => Some(empty.0.clone()),
            None => self.lua.create_table().ok().inspect(|table| {
                self.lua.set_app_data(EmptyArguments(table.clone()));
            }),
        };
        if let Some(empty) = empty {
            let globals = self.lua.globals();
            let _ = globals.raw_set("KEYS", empty.clone());
            let _ = globals.raw_set("ARGV", empty);
        }
    }
}

/// See [`ClearScriptEval`]'s `drop`.
struct EmptyArguments(LuaTable);

struct ScriptEval {
    /// Opaque `*mut Ctx` — stored as integer to avoid naming `Ctx`'s lifetime on this struct.
    ctx_addr: usize,
    read_only: bool,
    /// Virtual RESP for nested `redis.call` replies (`redis.setresp`), default 2.
    script_resp: u8,
    /// After a successful mutating `redis.call`, SCRIPT KILL is rejected (UNKILLABLE).
    wrote: bool,
    /// `#!lua flags=allow-oom` — skip maxmemory gate inside `redis.call` (`no-writes` implies this).
    allow_oom: bool,
    /// Source began with `#!` — subject to `min-replicas-to-write` on `redis.call` even for reads.
    had_shebang: bool,
    /// `#!lua flags=no-writes` — exempt from NOREPLICAS on read-only `redis.call`.
    no_writes_flag: bool,
    /// `allow-cross-slot-keys`: in a cluster, `redis.call` may use keys of
    /// other slots than the script's, as long as this node serves them.
    allow_cross_slot: bool,
    /// In a cluster, the slot the script works in: its first key's, from
    /// `KEYS`, or else the first key a `redis.call` used.
    slot: Option<u16>,
}

thread_local! {
    static SCRIPT_EVAL: RefCell<Option<ScriptEval>> = const { RefCell::new(None) };
    /// `redis.call`s and `redis.pcall`s made on this thread, ever: what a
    /// script ran is the difference (see [`count_script_commands`]).
    static SCRIPT_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Run a script for `ctx`'s client, and count the commands it ran as the
/// client's, as Redis counts them (`CLIENT INFO`'s `tot-cmds`): once, when it
/// is over, and only if it ran any.
fn count_script_commands(
    registry: &crate::client_registry::ClientRegistry,
    client_id: u64,
    run: impl FnOnce() -> Vec<u8>,
) -> Vec<u8> {
    let before = SCRIPT_CALLS.get();
    let reply = run();
    let ran = SCRIPT_CALLS.get().wrapping_sub(before);
    if ran > 0 {
        registry.add_commands(client_id, ran);
    }
    reply
}

/// When present, `redis.setresp` overrides [`crate::commands::handler::effective_resp_for_reply`].
pub(crate) fn script_eval_script_resp() -> Option<u8> {
    SCRIPT_EVAL.with(|c| c.borrow().as_ref().map(|s| s.script_resp))
}

#[inline]
pub(crate) fn script_allow_oom() -> bool {
    SCRIPT_EVAL.with(|c| c.borrow().as_ref().is_some_and(|s| s.allow_oom))
}

/// The running script's slot, and whether it may use keys of other slots
/// (see [`ScriptEval::slot`]); `None` outside a script.
pub(crate) fn script_slot() -> Option<(Option<u16>, bool)> {
    SCRIPT_EVAL.with(|c| c.borrow().as_ref().map(|s| (s.slot, s.allow_cross_slot)))
}

/// Make `slot` the running script's slot, if it has none yet.
pub(crate) fn script_settle_slot(slot: u16) {
    SCRIPT_EVAL.with(|c| {
        if let Some(s) = c.borrow_mut().as_mut() {
            s.slot.get_or_insert(slot);
        }
    });
}

/// The slot of a script's first key, in a cluster: where the command that
/// runs it was routed.
fn first_key_slot(ctx: &Ctx, first_key: Option<&[u8]>) -> Option<u16> {
    if !ctx.cluster.enabled {
        return None;
    }
    first_key.map(crate::cluster::slot::key_hash_slot)
}

/// Redis `min-slaves-to-write` / `min-replicas-to-write` gate for nested `redis.call`.
pub(crate) fn script_min_replicas_gate(ctx: &Ctx<'_>, cmd0: &[u8]) -> Option<Vec<u8>> {
    if ctx.apply_replication || !ctx.repl_state.lacks_good_replicas() {
        return None;
    }
    SCRIPT_EVAL.with(|c| {
        let b = c.borrow();
        let Some(s) = b.as_ref() else {
            return Some(resp_error(replication::NOREPLICAS));
        };
        let is_write = replication::is_write_command_bytes(cmd0);
        let block = if s.had_shebang && !s.no_writes_flag {
            true
        } else {
            is_write
        };
        if block {
            Some(resp_error(replication::NOREPLICAS))
        } else {
            None
        }
    })
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LuaVmBusy;

fn try_with_lua<F, R>(vm: Vm, f: F) -> Result<R, LuaVmBusy>
where
    F: FnOnce(&Lua) -> R,
{
    // Same-thread recursion (e.g. redis.call → EVAL): outer hold keeps `EXEC_LUA` set.
    if let Some(p) = EXEC_LUA.with(|c| *c.borrow()) {
        let prev = LUA_HOLD_DEPTH.fetch_add(1, Ordering::SeqCst);
        struct NestedDepth;
        impl Drop for NestedDepth {
            fn drop(&mut self) {
                LUA_HOLD_DEPTH.fetch_sub(1, Ordering::SeqCst);
                // Do not touch [`LUA_SCRIPT_BUSY_OTHERS`] here — only the outermost guard in this
                // thread drops it when the Lua VM is truly released (Redis CMD_ALLOW_BUSY relies on
                // other connections still seeing `-BUSY` while nested `redis.call` unwinds).
            }
        }
        let _nested_depth = NestedDepth;
        debug_assert!(prev > 0 || crate::tag_locks::beside_others());
        return Ok(unsafe { f(p.as_ref()) });
    }

    // A script under its tag's lock runs on this thread's own VM, beside the
    // scripts of other threads. The shared VM below is for the scripts that
    // run alone, one at a time, and the functions' VM beside it.
    if crate::tag_locks::beside_others() {
        return Ok(with_this_threads_vm(vm, f));
    }

    // Wait for the script that is running, if one is: scripts take turns.
    let mut guard = loop {
        if let Some(guard) = LUA_VM.try_lock_for(VM_WAIT_SLICE) {
            break guard;
        }
        if running_script_is_over_its_time_limit() {
            return Err(LuaVmBusy);
        }
    };
    let prev = LUA_HOLD_DEPTH.fetch_add(1, Ordering::SeqCst);
    if prev == 0 {
        LUA_SCRIPT_STARTED_MS.store(crate::replication::unix_ms(), Ordering::Relaxed);
        LUA_HOLDER_WROTE.store(false, Ordering::SeqCst);
        LUA_SCRIPT_BUSY_OTHERS.store(true, Ordering::SeqCst);
    }
    struct OuterDepth;
    impl Drop for OuterDepth {
        fn drop(&mut self) {
            let p = LUA_HOLD_DEPTH.fetch_sub(1, Ordering::SeqCst);
            if p == 1 {
                LUA_SCRIPT_BUSY_OTHERS.store(false, Ordering::SeqCst);
            }
        }
    }
    let _outer_depth = OuterDepth;

    let lua = guard.of(vm).get_or_insert_with(new_lua_vm);
    let nn = std::ptr::NonNull::from(&mut *lua);
    EXEC_LUA.with(|c| {
        *c.borrow_mut() = Some(nn);
    });
    struct ClearExecLua;
    impl Drop for ClearExecLua {
        fn drop(&mut self) {
            EXEC_LUA.with(|c| {
                *c.borrow_mut() = None;
            });
        }
    }
    let _clear_exec = ClearExecLua;

    let started = std::time::Instant::now();
    VM_RUN_STARTED.with(|c| c.set(Some(started)));
    let out = f(lua);
    VM_RUN_STARTED.with(|c| c.set(None));
    LAST_VM_RUN_US.with(|c| c.set(started.elapsed().as_micros() as u64));
    vm.bytes()
        .store(lua.used_memory() as u64, Ordering::Relaxed);
    Ok(out)
}

/// Scripts running on a VM of their thread's own (`with_this_threads_vm`):
/// for `SCRIPT KILL`, which is told there is nothing to kill otherwise.
static TAG_SCRIPTS_RUNNING: AtomicUsize = AtomicUsize::new(0);

/// Run `f` on this thread's own VM, made the first time it is wanted.
///
/// To a script it is the same VM as any other: a script cannot leave
/// anything behind in one (globals are protected), and what it compiles to
/// is kept by each VM for itself.
fn with_this_threads_vm<F, R>(vm: Vm, f: F) -> R
where
    F: FnOnce(&Lua) -> R,
{
    TAG_SCRIPTS_RUNNING.fetch_add(1, Ordering::SeqCst);
    struct Running;
    impl Drop for Running {
        fn drop(&mut self) {
            TAG_SCRIPTS_RUNNING.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _running = Running;

    let which = match vm {
        Vm::Eval => &LUA,
        Vm::Functions => &LUA_FUNCTIONS,
    };
    which.with(|mine| {
        let mut mine = mine.borrow_mut();
        // Made again after `SCRIPT FLUSH` or `FUNCTION FLUSH`, as the shared
        // VMs are: this thread's is let go of when next wanted.
        let generation = vm.generation().load(Ordering::Acquire);
        if mine.as_ref().is_none_or(|(made, _)| *made != generation) {
            *mine = Some((generation, new_lua_vm()));
        }
        let (_, lua) = mine.as_mut().expect("made above");
        // For a script the script calls (`redis.call('EVALSHA', …)`).
        let nn = std::ptr::NonNull::from(&mut *lua);
        EXEC_LUA.with(|c| {
            *c.borrow_mut() = Some(nn);
        });
        struct ClearExecLua;
        impl Drop for ClearExecLua {
            fn drop(&mut self) {
                EXEC_LUA.with(|c| {
                    *c.borrow_mut() = None;
                });
            }
        }
        let _clear_exec = ClearExecLua;

        let started = std::time::Instant::now();
        VM_RUN_STARTED.with(|c| c.set(Some(started)));
        let out = f(lua);
        VM_RUN_STARTED.with(|c| c.set(None));
        LAST_VM_RUN_US.with(|c| c.set(started.elapsed().as_micros() as u64));
        out
    })
}

thread_local! {
    /// How long the last script this thread ran spent in the VM, in µs.
    static LAST_VM_RUN_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// When the script this thread is running started, while it runs.
    static VM_RUN_STARTED: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
    /// When [`keep_the_runtime_listening`] may next wake a worker.
    static NEXT_NUDGE: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

/// A script that has run this long on a tokio worker has other connections
/// waiting on it (see [`keep_the_runtime_listening`]).
const NUDGE_AFTER: std::time::Duration = std::time::Duration::from_millis(5);
/// How often, after that, another worker is woken.
const NUDGE_EVERY: std::time::Duration = std::time::Duration::from_millis(10);

/// Called from the script hook. A script that has shown itself quick runs on
/// the tokio worker that read it (`lua_vm_blocking`), and one that turns out
/// not to be holds that worker. If the worker had last been the one waiting
/// on the sockets, nobody is any more: the other workers sleep until they are
/// woken, and every connection goes unanswered — not told `BUSY`, not able to
/// `SCRIPT KILL` the script — until it ends, however long that is. So while
/// a script runs long, another worker is woken now and then, by a task sent
/// from outside the workers (sent from here, it would wait behind the script
/// on this worker's own queue); woken, it takes over the sockets. io_uring
/// workers each run on their own and have nothing to hand over.
fn keep_the_runtime_listening() {
    let Some(started) = VM_RUN_STARTED.with(|c| c.get()) else {
        return;
    };
    let now = std::time::Instant::now();
    if now.duration_since(started) < NUDGE_AFTER
        || NEXT_NUDGE.with(|c| c.get()).is_some_and(|next| now < next)
    {
        return;
    }
    NEXT_NUDGE.with(|c| c.set(Some(now + NUDGE_EVERY)));
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        let remote = handle.clone();
        handle.spawn_blocking(move || {
            remote.spawn(async {});
        });
    }
}

/// How long the script this thread has just run spent running — not waiting
/// for its turn, the gate or the VM — and zero if it never reached the VM.
/// Read once: it is cleared.
pub(crate) fn take_last_vm_run_us() -> u64 {
    LAST_VM_RUN_US.with(|c| c.replace(0))
}

pub(crate) fn lua_time_limit_ms() -> u64 {
    LUA_TIME_LIMIT_MS.load(Ordering::Relaxed)
}

pub(crate) fn lua_time_limit_ms_set(ms: u64) {
    LUA_TIME_LIMIT_MS.store(ms, Ordering::Relaxed);
}

/// Called after a successful mutating command executed from `redis.call`.
pub(crate) fn script_note_successful_write() {
    SCRIPT_EVAL.with(|c| {
        if let Some(ref mut se) = *c.borrow_mut() {
            se.wrote = true;
        }
    });
    // The script holding the shared VM: for `SCRIPT KILL`, from elsewhere.
    if EXEC_LUA.with(|c| c.borrow().is_some()) {
        LUA_HOLDER_WROTE.store(true, Ordering::SeqCst);
    }
}

/// Commands that run or compile the Lua VM for long stretches — must not block a Tokio worker
/// (see `crate::lua_vm_blocking`).
#[cfg(feature = "lua")]
pub(crate) fn lua_command_runs_long_sync_vm(cmd: &[&[u8]]) -> bool {
    if cmd.is_empty() {
        return false;
    }
    let name = cmd[0];
    if name.eq_ignore_ascii_case(b"EVAL")
        || name.eq_ignore_ascii_case(b"EVALSHA")
        || name.eq_ignore_ascii_case(b"EVAL_RO")
        || name.eq_ignore_ascii_case(b"EVALSHA_RO")
    {
        return true;
    }
    if name.eq_ignore_ascii_case(b"FCALL") || name.eq_ignore_ascii_case(b"FCALL_RO") {
        return true;
    }
    name.eq_ignore_ascii_case(b"FUNCTION") && cmd.len() >= 2 && cmd[1].eq_ignore_ascii_case(b"LOAD")
}

/// Called before every command. While another client's script is running,
/// this waits for it to finish — a script is one step, as far as everybody
/// else is concerned — and returns false. It returns true, meaning "reply
/// `-BUSY`", only if that script has been running for longer than
/// `lua-time-limit`.
///
/// With no script running — nearly always — it is one atomic load.
pub(crate) fn lua_server_busy_for_command(cmd: &[&[u8]]) -> bool {
    // Pair with SeqCst stores in `try_with_lua`: Relaxed loads could miss the busy transition on weak memory models.
    if !LUA_SCRIPT_BUSY_OTHERS.load(Ordering::Acquire) {
        return false;
    }
    // The script's own `redis.call`s, on the thread that is running it.
    if SCRIPT_EVAL.with(|c| c.borrow().is_some()) || EXEC_LUA.with(|c| c.borrow().is_some()) {
        return false;
    }
    if lua_command_allowed_while_busy(cmd) {
        return false;
    }
    // The VM's lock is released exactly when the script ends, so waiting for
    // it — and letting go of it at once — is waiting for the script.
    loop {
        if !LUA_SCRIPT_BUSY_OTHERS.load(Ordering::Acquire) {
            return false;
        }
        if running_script_is_over_its_time_limit() {
            return true;
        }
        if LUA_VM.try_lock_for(VM_WAIT_SLICE).is_some() {
            return false;
        }
    }
}

fn lua_command_allowed_while_busy(cmd: &[&[u8]]) -> bool {
    if cmd.is_empty() {
        return false;
    }
    let name = cmd[0];
    if name.eq_ignore_ascii_case(b"SCRIPT")
        && cmd.len() >= 2
        && cmd[1].eq_ignore_ascii_case(b"KILL")
    {
        return true;
    }
    // Redis functions path: `FUNCTION KILL` / `FUNCTION STATS` use CMD_ALLOW_BUSY (see Redis commands.def).
    if name.eq_ignore_ascii_case(b"FUNCTION")
        && cmd.len() >= 2
        && (cmd[1].eq_ignore_ascii_case(b"KILL") || cmd[1].eq_ignore_ascii_case(b"STATS"))
    {
        return true;
    }
    if name.eq_ignore_ascii_case(b"SHUTDOWN") {
        return true;
    }
    false
}

/// Shared by `SCRIPT KILL` and `FUNCTION KILL`.
/// The function that is running, if one is: its name and the command that
/// called it. (A script run with `EVAL` is not one.)
static RUNNING_FUNCTION: parking_lot::Mutex<Option<(String, Vec<Vec<u8>>)>> =
    parking_lot::Mutex::new(None);

const BUSY_WITH_SCRIPT: &str =
    "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.";
const BUSY_WITH_FUNCTION: &str =
    "BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.";

/// `SCRIPT KILL` (`function == false`) or `FUNCTION KILL`: each stops what
/// its own kind of call started, and is refused while the other kind runs.
fn kill_running(function: bool) -> Vec<u8> {
    // Scripts running under a tag's lock, each on its thread's VM, are all
    // told: there is no saying which of them the caller means. Those that
    // have written carry on, as ever.
    if !function
        && LUA_HOLD_DEPTH.load(Ordering::SeqCst) == 0
        && TAG_SCRIPTS_RUNNING.load(Ordering::SeqCst) > 0
    {
        LUA_SCRIPT_KILL_REQUESTED.store(true, Ordering::SeqCst);
        return resp_simple("OK");
    }
    if LUA_HOLD_DEPTH.load(Ordering::SeqCst) == 0 {
        return resp_error("ERR No scripts in execution.");
    }
    match (function, RUNNING_FUNCTION.lock().is_some()) {
        (false, true) => resp_error(BUSY_WITH_FUNCTION),
        (true, false) => resp_error(BUSY_WITH_SCRIPT),
        _ => dispatch_lua_script_kill(),
    }
}

fn dispatch_lua_script_kill() -> Vec<u8> {
    if LUA_HOLD_DEPTH.load(Ordering::SeqCst) == 0 {
        return resp_error("ERR No scripts in execution.");
    }
    if LUA_HOLDER_WROTE.load(Ordering::SeqCst) {
        return resp_error(
            "UNKILLABLE Sorry the script already executed write commands against the dataset.",
        );
    }
    LUA_SCRIPT_KILL_REQUESTED.store(true, Ordering::SeqCst);
    resp_simple("OK")
}

// ─────────────────────────── MessagePack helpers ────────────────────────────

/// Encode a single Lua value as MessagePack bytes appended to `buf`.
/// `depth` limits TABLE nesting to 16 levels (circular reference safety).
/// Scalars are always serialised regardless of depth.
fn msgpack_encode(val: &LuaValue, buf: &mut Vec<u8>, depth: u32) {
    match val {
        LuaNil => buf.push(0xc0),
        LuaValue::Boolean(false) => buf.push(0xc2),
        LuaValue::Boolean(true) => buf.push(0xc3),
        LuaValue::Integer(n) => msgpack_encode_int(*n, buf),
        LuaValue::Number(f) => {
            buf.push(0xcb);
            buf.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        LuaValue::String(s) => msgpack_encode_str(s.as_bytes().as_ref(), buf),
        LuaValue::Table(t) => {
            if depth >= 16 {
                buf.push(0xc0); // depth limit reached: encode as nil
            } else {
                msgpack_encode_table(t, buf, depth);
            }
        }
        _ => buf.push(0xc0),
    }
}

fn msgpack_encode_int(n: i64, buf: &mut Vec<u8>) {
    if n >= 0 {
        let u = n as u64;
        if u <= 127 {
            buf.push(u as u8);
        } else if u <= 0xFF {
            buf.extend_from_slice(&[0xcc, u as u8]);
        } else if u <= 0xFFFF {
            buf.push(0xcd);
            buf.extend_from_slice(&(u as u16).to_be_bytes());
        } else if u <= 0xFFFF_FFFF {
            buf.push(0xce);
            buf.extend_from_slice(&(u as u32).to_be_bytes());
        } else {
            buf.push(0xcf);
            buf.extend_from_slice(&u.to_be_bytes());
        }
    } else {
        if n >= -32 {
            buf.push((0x100u32 as i64 + n) as u8); // negative fixint
        } else if n >= -128 {
            buf.extend_from_slice(&[0xd0, n as i8 as u8]);
        } else if n >= -32768 {
            buf.push(0xd1);
            buf.extend_from_slice(&(n as i16).to_be_bytes());
        } else if n >= -2_147_483_648 {
            buf.push(0xd2);
            buf.extend_from_slice(&(n as i32).to_be_bytes());
        } else {
            buf.push(0xd3);
            buf.extend_from_slice(&n.to_be_bytes());
        }
    }
}

fn cmp_msgpack_map_key(a: &LuaValue, b: &LuaValue) -> std::cmp::Ordering {
    use LuaValue::*;
    match (a, b) {
        (Integer(i), Integer(j)) => i.cmp(j),
        (Integer(_), _) => std::cmp::Ordering::Less,
        (_, Integer(_)) => std::cmp::Ordering::Greater,
        // LuaJIT's cmsgpack encodes map keys in hash-table order; reverse lexicographic
        // matches the Redis reference output for typical `{x=..., y=...}` tables.
        (String(sa), String(sb)) => sb.as_bytes().cmp(&sa.as_bytes()),
        (String(_), _) => std::cmp::Ordering::Less,
        (_, String(_)) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

fn msgpack_encode_str(s: &[u8], buf: &mut Vec<u8>) {
    let len = s.len();
    if len <= 31 {
        buf.push(0xa0 | len as u8);
    } else if len <= 0xFF {
        buf.extend_from_slice(&[0xd9, len as u8]);
    } else if len <= 0xFFFF {
        buf.push(0xda);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xdb);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(s);
}

fn msgpack_encode_table(t: &LuaTable, buf: &mut Vec<u8>, depth: u32) {
    let seq_len = t.raw_len();
    // Collect all key-value pairs.
    let mut pairs: Vec<(LuaValue, LuaValue)> = t
        .clone()
        .pairs::<LuaValue, LuaValue>()
        .filter_map(|r| r.ok())
        .collect();
    pairs.sort_by(|(k1, _), (k2, _)| cmp_msgpack_map_key(k1, k2));
    let total = pairs.len();

    // Treat table as array if all keys are consecutive integers 1..seq_len.
    let is_array = seq_len > 0
        && total == seq_len
        && pairs
            .iter()
            .all(|(k, _)| matches!(k, LuaValue::Integer(n) if *n >= 1 && *n as usize <= seq_len));

    if is_array {
        if seq_len <= 15 {
            buf.push(0x90 | seq_len as u8);
        } else if seq_len <= 0xFFFF {
            buf.push(0xdc);
            buf.extend_from_slice(&(seq_len as u16).to_be_bytes());
        } else {
            buf.push(0xdd);
            buf.extend_from_slice(&(seq_len as u32).to_be_bytes());
        }
        for i in 1..=seq_len {
            let v = t.get::<LuaValue>(i as i64).unwrap_or(LuaNil);
            msgpack_encode(&v, buf, depth + 1);
        }
    } else {
        if total <= 15 {
            buf.push(0x80 | total as u8);
        } else if total <= 0xFFFF {
            buf.push(0xde);
            buf.extend_from_slice(&(total as u16).to_be_bytes());
        } else {
            buf.push(0xdf);
            buf.extend_from_slice(&(total as u32).to_be_bytes());
        }
        for (k, v) in &pairs {
            msgpack_encode(k, buf, depth + 1);
            msgpack_encode(v, buf, depth + 1);
        }
    }
}

/// How deep a MessagePack value may nest to be decoded.
const MSGPACK_DECODE_DEPTH_MAX: u32 = 256;

/// Decode one MessagePack value from `buf` starting at index 0.
/// Returns `(LuaValue, bytes_consumed)`.
fn msgpack_decode(lua: &Lua, buf: &[u8], depth: u32) -> LuaResult<(LuaValue, usize)> {
    // Each level is a call: without a limit, a string of nested one-element
    // arrays runs the thread out of stack, which takes the server down.
    if depth >= MSGPACK_DECODE_DEPTH_MAX {
        return Err(LuaError::RuntimeError("msgpack: nested too deeply".into()));
    }
    if buf.is_empty() {
        return Err(LuaError::RuntimeError(
            "msgpack: unexpected end of data".into(),
        ));
    }
    let b = buf[0];
    match b {
        // positive fixint
        0x00..=0x7f => Ok((LuaValue::Integer(b as i64), 1)),
        // fixmap
        0x80..=0x8f => {
            let count = (b & 0x0f) as usize;
            msgpack_decode_map(lua, buf, 1, count, depth)
        }
        // fixarray
        0x90..=0x9f => {
            let count = (b & 0x0f) as usize;
            msgpack_decode_array(lua, buf, 1, count, depth)
        }
        // fixstr
        0xa0..=0xbf => {
            let len = (b & 0x1f) as usize;
            let end = 1 + len;
            if buf.len() < end {
                return Err(LuaError::RuntimeError("msgpack: truncated string".into()));
            }
            Ok((LuaValue::String(lua.create_string(&buf[1..end])?), end))
        }
        0xc0 => Ok((LuaNil, 1)),
        0xc2 => Ok((LuaValue::Boolean(false), 1)),
        0xc3 => Ok((LuaValue::Boolean(true), 1)),
        // float32
        0xca => {
            if buf.len() < 5 {
                return Err(LuaError::RuntimeError("msgpack: truncated float32".into()));
            }
            let bits = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
            Ok((LuaValue::Number(f32::from_bits(bits) as f64), 5))
        }
        // float64
        0xcb => {
            if buf.len() < 9 {
                return Err(LuaError::RuntimeError("msgpack: truncated float64".into()));
            }
            let bits = u64::from_be_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((LuaValue::Number(f64::from_bits(bits)), 9))
        }
        // uint8
        0xcc => {
            if buf.len() < 2 {
                return Err(LuaError::RuntimeError("msgpack: truncated uint8".into()));
            }
            Ok((LuaValue::Integer(buf[1] as i64), 2))
        }
        // uint16
        0xcd => {
            if buf.len() < 3 {
                return Err(LuaError::RuntimeError("msgpack: truncated uint16".into()));
            }
            Ok((
                LuaValue::Integer(u16::from_be_bytes([buf[1], buf[2]]) as i64),
                3,
            ))
        }
        // uint32
        0xce => {
            if buf.len() < 5 {
                return Err(LuaError::RuntimeError("msgpack: truncated uint32".into()));
            }
            Ok((
                LuaValue::Integer(u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as i64),
                5,
            ))
        }
        // uint64
        0xcf => {
            if buf.len() < 9 {
                return Err(LuaError::RuntimeError("msgpack: truncated uint64".into()));
            }
            let v = u64::from_be_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((LuaValue::Integer(v as i64), 9))
        }
        // int8
        0xd0 => {
            if buf.len() < 2 {
                return Err(LuaError::RuntimeError("msgpack: truncated int8".into()));
            }
            Ok((LuaValue::Integer(buf[1] as i8 as i64), 2))
        }
        // int16
        0xd1 => {
            if buf.len() < 3 {
                return Err(LuaError::RuntimeError("msgpack: truncated int16".into()));
            }
            Ok((
                LuaValue::Integer(i16::from_be_bytes([buf[1], buf[2]]) as i64),
                3,
            ))
        }
        // int32
        0xd2 => {
            if buf.len() < 5 {
                return Err(LuaError::RuntimeError("msgpack: truncated int32".into()));
            }
            Ok((
                LuaValue::Integer(i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as i64),
                5,
            ))
        }
        // int64
        0xd3 => {
            if buf.len() < 9 {
                return Err(LuaError::RuntimeError("msgpack: truncated int64".into()));
            }
            Ok((
                LuaValue::Integer(i64::from_be_bytes([
                    buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
                ])),
                9,
            ))
        }
        // str8
        0xd9 => {
            if buf.len() < 2 {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated str8 header".into(),
                ));
            }
            let len = buf[1] as usize;
            let end = 2 + len;
            if buf.len() < end {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated str8 body".into(),
                ));
            }
            Ok((LuaValue::String(lua.create_string(&buf[2..end])?), end))
        }
        // str16
        0xda => {
            if buf.len() < 3 {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated str16 header".into(),
                ));
            }
            let len = u16::from_be_bytes([buf[1], buf[2]]) as usize;
            let end = 3 + len;
            if buf.len() < end {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated str16 body".into(),
                ));
            }
            Ok((LuaValue::String(lua.create_string(&buf[3..end])?), end))
        }
        // str32
        0xdb => {
            if buf.len() < 5 {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated str32 header".into(),
                ));
            }
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            let end = 5 + len;
            if buf.len() < end {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated str32 body".into(),
                ));
            }
            Ok((LuaValue::String(lua.create_string(&buf[5..end])?), end))
        }
        // array16
        0xdc => {
            if buf.len() < 3 {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated array16 header".into(),
                ));
            }
            let count = u16::from_be_bytes([buf[1], buf[2]]) as usize;
            msgpack_decode_array(lua, buf, 3, count, depth)
        }
        // array32
        0xdd => {
            if buf.len() < 5 {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated array32 header".into(),
                ));
            }
            let count = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            msgpack_decode_array(lua, buf, 5, count, depth)
        }
        // map16
        0xde => {
            if buf.len() < 3 {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated map16 header".into(),
                ));
            }
            let count = u16::from_be_bytes([buf[1], buf[2]]) as usize;
            msgpack_decode_map(lua, buf, 3, count, depth)
        }
        // map32
        0xdf => {
            if buf.len() < 5 {
                return Err(LuaError::RuntimeError(
                    "msgpack: truncated map32 header".into(),
                ));
            }
            let count = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            msgpack_decode_map(lua, buf, 5, count, depth)
        }
        // negative fixint
        0xe0..=0xff => Ok((LuaValue::Integer(b as i8 as i64), 1)),
        _ => Err(LuaError::RuntimeError(format!(
            "msgpack: unknown type byte 0x{b:02x}"
        ))),
    }
}

fn msgpack_decode_array(
    lua: &Lua,
    buf: &[u8],
    mut pos: usize,
    count: usize,
    depth: u32,
) -> LuaResult<(LuaValue, usize)> {
    let t = lua.create_table()?;
    for i in 1..=count {
        let (v, consumed) = msgpack_decode(lua, &buf[pos..], depth + 1)?;
        t.set(i, v)?;
        pos += consumed;
    }
    Ok((LuaValue::Table(t), pos))
}

fn msgpack_decode_map(
    lua: &Lua,
    buf: &[u8],
    mut pos: usize,
    count: usize,
    depth: u32,
) -> LuaResult<(LuaValue, usize)> {
    let t = lua.create_table()?;
    for _ in 0..count {
        let (k, kc) = msgpack_decode(lua, &buf[pos..], depth + 1)?;
        pos += kc;
        let (v, vc) = msgpack_decode(lua, &buf[pos..], depth + 1)?;
        pos += vc;
        match k {
            LuaNil => {} // skip nil keys
            _ => t.set(k, v)?,
        }
    }
    Ok((LuaValue::Table(t), pos))
}

fn lua_value_to_bytes(val: &LuaValue) -> LuaResult<Vec<u8>> {
    match val {
        LuaValue::String(s) => Ok(s.as_bytes().to_vec()),
        _ => Err(LuaError::RuntimeError(
            "cmsgpack: expected binary string".into(),
        )),
    }
}

// ─────────────────────────────────────────────────────────────────────────────

fn new_lua_vm() -> Lua {
    let lua = Lua::new();

    let redis_table = lua.create_table().unwrap();

    let status_reply = lua
        .create_function(|lua, s: String| {
            let t = lua.create_table()?;
            t.set("ok", s)?;
            Ok(LuaValue::Table(t))
        })
        .unwrap();
    redis_table.set("status_reply", status_reply).unwrap();

    let error_reply = lua
        .create_function(|lua, s: String| {
            let t = lua.create_table()?;
            t.set("err", error_reply_message(&s))?;
            Ok(LuaValue::Table(t))
        })
        .unwrap();
    redis_table.set("error_reply", error_reply).unwrap();

    redis_table.set("LOG_DEBUG", 0i64).unwrap();
    redis_table.set("LOG_VERBOSE", 1i64).unwrap();
    redis_table.set("LOG_NOTICE", 2i64).unwrap();
    redis_table.set("LOG_WARNING", 3i64).unwrap();
    // The version `INFO` reports, as a string and as 0x00MMmmpp.
    redis_table.set("REDIS_VERSION", "8.10.0").unwrap();
    redis_table
        .set("REDIS_VERSION_NUM", (8i64 << 16) | (10 << 8))
        .unwrap();

    // To the server's log, at Redis's level for it; it did nothing (#154).
    let log_fn = lua
        .create_function(|_, args: LuaMultiValue| {
            let mut args = args.into_iter();
            let (Some(level), Some(first)) = (args.next(), args.next()) else {
                return Err(LuaError::RuntimeError(
                    "redis.log() requires two arguments or more.".into(),
                ));
            };
            let level = match level {
                LuaValue::Integer(0) => crate::loglevel::DEBUG,
                LuaValue::Integer(1) => crate::loglevel::VERBOSE,
                LuaValue::Integer(2) => crate::loglevel::INFO,
                LuaValue::Integer(3) => crate::loglevel::WARN,
                LuaValue::Number(n) if n.fract() == 0.0 && (0.0..=3.0).contains(&n) => [
                    crate::loglevel::DEBUG,
                    crate::loglevel::VERBOSE,
                    crate::loglevel::INFO,
                    crate::loglevel::WARN,
                ][n as usize],
                _ => return Err(LuaError::RuntimeError("Invalid debug level.".into())),
            };
            if !crate::loglevel::enabled(level) {
                return Ok(());
            }
            // The arguments joined by spaces, as Redis writes them.
            let text = |value: LuaValue| match value {
                LuaValue::String(s) => s.to_string_lossy().to_string(),
                LuaValue::Integer(n) => n.to_string(),
                LuaValue::Number(n) => n.to_string(),
                LuaValue::Boolean(b) => b.to_string(),
                LuaValue::Nil => "nil".into(),
                other => other.type_name().to_string(),
            };
            let line: Vec<String> = std::iter::once(first).chain(args).map(text).collect();
            crate::log_at!(level, "{}", line.join(" "));
            Ok(())
        })
        .unwrap();
    redis_table.set("log", log_fn).unwrap();

    // Before Redis 7, how a script asked to be replicated as its effects:
    // here every script is, so it is true, as in Redis 7 (#154). A script
    // that opens with it failed before doing anything.
    let replicate_commands = lua.create_function(|_, _: LuaMultiValue| Ok(true)).unwrap();
    redis_table
        .set("replicate_commands", replicate_commands)
        .unwrap();
    redis_table.set("REPL_NONE", 0i64).unwrap();
    redis_table.set("REPL_AOF", 1i64).unwrap();
    redis_table.set("REPL_SLAVE", 2i64).unwrap();
    redis_table.set("REPL_REPLICA", 2i64).unwrap();
    redis_table.set("REPL_ALL", 3i64).unwrap();
    // What a script's writes go to: everything, which is what KiviDB does.
    // Keeping them from the AOF or the replicas is not supported, and is
    // refused rather than ignored.
    let set_repl = lua
        .create_function(|_, flags: LuaValue| match flags {
            LuaValue::Integer(3) => Ok(()),
            LuaValue::Number(3.0) => Ok(()),
            LuaValue::Integer(0..=2) | LuaValue::Number(_) => Err(LuaError::RuntimeError(
                "redis.set_repl() other than REDIS.REPL_ALL is not supported by KiviDB".into(),
            )),
            _ => Err(LuaError::RuntimeError(
                "Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE."
                    .into(),
            )),
        })
        .unwrap();
    redis_table.set("set_repl", set_repl).unwrap();

    let placeholder = lua
        .create_function(|_, _: LuaMultiValue| {
            Err::<LuaValue, _>(LuaError::RuntimeError(
                "redis.call called outside EVAL context".into(),
            ))
        })
        .unwrap();
    redis_table.set("call", placeholder.clone()).unwrap();
    redis_table.set("pcall", placeholder).unwrap();

    lua.globals().set("redis", redis_table).unwrap();

    lua.globals().set("io", LuaNil).unwrap();
    // Base library still registers print/loadfile/dofile; stripped after sandbox (see end of
    // `new_lua_vm`). package/require: leave alone unless Tcl requires removal.

    let os_table: LuaTable = lua.globals().get("os").unwrap();
    let safe_os = lua.create_table().unwrap();
    safe_os
        .set("clock", os_table.get::<LuaFunction>("clock").unwrap())
        .unwrap();
    safe_os
        .set("time", os_table.get::<LuaFunction>("time").unwrap())
        .unwrap();
    safe_os
        .set("date", os_table.get::<LuaFunction>("date").unwrap())
        .unwrap();
    safe_os
        .set("difftime", os_table.get::<LuaFunction>("difftime").unwrap())
        .unwrap();
    lua.globals().set("os", safe_os).unwrap();

    let sha1_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let args: Vec<_> = args.into_iter().collect();
            if args.len() != 1 {
                return Err(LuaError::RuntimeError(
                    "ERR wrong number of arguments for redis.sha1hex()".into(),
                ));
            }
            let s = match &args[0] {
                LuaValue::String(ls) => ls.clone(),
                LuaValue::Integer(n) => lua.create_string(n.to_string())?,
                LuaValue::Number(n) => lua.create_string((*n as i64).to_string())?,
                _ => {
                    return Err(LuaError::RuntimeError(
                        "ERR wrong number of arguments for redis.sha1hex()".into(),
                    ));
                }
            };
            let mut hasher = Sha1::new();
            hasher.update(s.as_bytes());
            let hex = format!("{:x}", hasher.finalize());
            Ok(LuaValue::String(lua.create_string(hex.as_str())?))
        })
        .unwrap();
    let redis_table: LuaTable = lua.globals().get("redis").unwrap();
    redis_table.set("sha1hex", sha1_fn).unwrap();

    let setresp_fn = lua
        .create_function(|_, args: LuaMultiValue| {
            let args: Vec<_> = args.into_iter().collect();
            if args.len() != 1 {
                return Err(LuaError::RuntimeError(
                    "ERR redis.setresp() requires one argument.".into(),
                ));
            }
            let n = match &args[0] {
                LuaValue::Integer(i) => *i,
                LuaValue::Number(f) => *f as i64,
                _ => {
                    return Err(LuaError::RuntimeError(
                        "ERR RESP version must be 2 or 3.".into(),
                    ));
                }
            };
            if n != 2 && n != 3 {
                return Err(LuaError::RuntimeError(
                    "ERR RESP version must be 2 or 3.".into(),
                ));
            }
            SCRIPT_EVAL.with(|c| {
                let mut b = c.borrow_mut();
                let Some(ref mut se) = *b else {
                    return Err(LuaError::RuntimeError(
                        "ERR redis.setresp outside script".into(),
                    ));
                };
                se.script_resp = n as u8;
                Ok(())
            })?;
            Ok(())
        })
        .unwrap();
    redis_table.set("setresp", setresp_fn).unwrap();

    let acl_check_cmd_fn = lua
        .create_function(|_, args: LuaMultiValue| {
            let addr = SCRIPT_EVAL.with(|c| {
                c.borrow().as_ref().map(|s| s.ctx_addr).ok_or_else(|| {
                    LuaError::RuntimeError("redis.acl_check_cmd outside script".into())
                })
            })?;
            let ctx = unsafe { &mut *(addr as *mut Ctx) };

            let mut parts: Vec<Vec<u8>> = Vec::with_capacity(args.len());
            for arg in args.into_iter() {
                match arg {
                    LuaValue::String(s) => parts.push(s.as_bytes().as_ref().to_vec()),
                    LuaValue::Integer(n) => parts.push(n.to_string().into_bytes()),
                    LuaValue::Number(n) => parts.push(lua_float_redis_arg(n)),
                    LuaValue::Boolean(true) => parts.push(b"1".to_vec()),
                    LuaValue::Boolean(false) => parts.push(b"0".to_vec()),
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "ERR Lua redis.acl_check_cmd arguments must be strings, numbers, or booleans".into(),
                        ));
                    }
                }
            }
            if parts.is_empty() {
                return Err(LuaError::RuntimeError(
                    "ERR wrong number of arguments for redis.acl_check_cmd()".into(),
                ));
            }

            let cmd_upper = String::from_utf8_lossy(&parts[0]).to_ascii_uppercase();
            if !crate::commands::server::is_registered_command(&cmd_upper) {
                return Err(LuaError::RuntimeError(
                    "ERR Invalid command passed to redis.acl_check_cmd()".into(),
                ));
            }
            let words: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
            if crate::commands::arity::check(&cmd_upper, &words).is_some() {
                return Err(LuaError::RuntimeError(
                    "ERR Wrong number of args for redis.acl_check_cmd()".into(),
                ));
            }

            let store = ctx.acl_store.read();
            let Some(user) = store.get(ctx.authenticated_user) else {
                return Ok(LuaValue::Boolean(false));
            };
            Ok(LuaValue::Boolean(
                crate::acl::check_permission(user, &cmd_upper, &parts).is_ok(),
            ))
        })
        .unwrap();
    redis_table.set("acl_check_cmd", acl_check_cmd_fn).unwrap();

    // `redis.register_function` is for a library's code while it loads
    // (`function_environment`). A script, or a function that is running, has
    // no such thing — in the words Redis's Lua has for calling what is not
    // there.
    let register_fn = lua
        .create_function(|_, _: LuaMultiValue| -> LuaResult<()> {
            lua_runtime_error("attempt to call field 'register_function' (a nil value)")
        })
        .unwrap();
    redis_table.set("register_function", register_fn).unwrap();

    lua.set_named_registry_value("__kividb_redis_inner", redis_table.clone())
        .expect("redis inner registry");

    let cjson = lua.create_table().unwrap();
    let encode_fn = lua
        .create_function(|lua, val: LuaValue| {
            let json = lua_to_json_value(lua, &val, 0)?;
            let s =
                serde_json::to_string(&json).map_err(|e| LuaError::RuntimeError(e.to_string()))?;
            Ok(LuaValue::String(lua.create_string(s)?))
        })
        .unwrap();
    let decode_fn = lua
        .create_function(|lua, s: String| {
            let val: serde_json::Value =
                serde_json::from_str(&s).map_err(|e| LuaError::RuntimeError(e.to_string()))?;
            let max_depth: i64 = match lua.globals().get::<LuaValue>("__cjson_decode_max_depth") {
                Ok(LuaValue::Integer(n)) => n,
                _ => 1000,
            };
            json_depth_check(&val, 0, max_depth).map_err(LuaError::RuntimeError)?;
            let mode = matches!(
                lua.globals().get::<LuaValue>("__cjson_array_mt_mode"),
                Ok(LuaValue::Boolean(true))
            );
            let array_mt: Option<LuaTable> = if mode {
                match lua.globals().get::<LuaValue>("__cjson_array_mt") {
                    Ok(LuaValue::Table(t)) => Some(t),
                    _ => None,
                }
            } else {
                None
            };
            json_to_lua(lua, &val, array_mt.as_ref())
        })
        .unwrap();
    cjson.set("encode", encode_fn).unwrap();
    cjson.set("decode", decode_fn).unwrap();

    let cmsgpack = lua.create_table().unwrap();

    let pack_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut buf = Vec::new();
            for val in args.iter() {
                msgpack_encode(val, &mut buf, 0);
            }
            Ok(LuaValue::String(lua.create_string(&buf)?))
        })
        .unwrap();

    let unpack_fn = lua
        .create_function(|lua, data: LuaValue| {
            let bytes = lua_value_to_bytes(&data)?;
            let (val, _) = msgpack_decode(lua, &bytes, 0)?;
            Ok(val)
        })
        .unwrap();

    // unpack_one(data, offset) → (next_offset, value); next_offset == -1 when exhausted.
    let unpack_one_fn = lua
        .create_function(|lua, (data, offset): (LuaValue, usize)| {
            let bytes = lua_value_to_bytes(&data)?;
            if offset >= bytes.len() {
                return Ok(LuaMultiValue::from_vec(vec![LuaValue::Integer(-1), LuaNil]));
            }
            let (val, next) = msgpack_decode(lua, &bytes[offset..], 0)?;
            let next_abs = offset + next;
            let next_out = if next_abs >= bytes.len() {
                -1i64
            } else {
                next_abs as i64
            };
            Ok(LuaMultiValue::from_vec(vec![
                LuaValue::Integer(next_out),
                val,
            ]))
        })
        .unwrap();

    // unpack_limit(data, count, offset) → (next_offset, value); count values decoded.
    let unpack_limit_fn = lua
        .create_function(|lua, (data, count, offset): (LuaValue, usize, usize)| {
            let bytes = lua_value_to_bytes(&data)?;
            let mut pos = offset;
            let mut results = Vec::new();
            for _ in 0..count {
                if pos >= bytes.len() {
                    break;
                }
                let (val, consumed) = msgpack_decode(lua, &bytes[pos..], 0)?;
                pos += consumed;
                results.push(val);
            }
            let next_out = if pos >= bytes.len() {
                -1i64
            } else {
                pos as i64
            };
            let mut out = vec![LuaValue::Integer(next_out)];
            out.extend(results);
            Ok(LuaMultiValue::from_vec(out))
        })
        .unwrap();

    cmsgpack.set("pack", pack_fn).unwrap();
    cmsgpack.set("unpack", unpack_fn).unwrap();
    cmsgpack.set("unpack_one", unpack_one_fn).unwrap();
    cmsgpack.set("unpack_limit", unpack_limit_fn).unwrap();

    // `decode_array_with_array_mt`: when true, decoded JSON arrays receive a special metatable
    // that makes the encoder treat them as arrays even when empty.
    let arr_mt_fn = lua
        .create_function(|lua, enabled: bool| {
            lua.globals().raw_set("__cjson_array_mt_mode", enabled)?;
            Ok(())
        })
        .unwrap();
    cjson.set("decode_array_with_array_mt", arr_mt_fn).unwrap();

    // `encode_keep_buffer` is a cjson performance hint; stub it out as a no-op.
    let noop_fn = lua.create_function(|_, _: LuaMultiValue| Ok(())).unwrap();
    cjson.set("encode_keep_buffer", noop_fn).unwrap();

    let enc_depth_fn = lua
        .create_function(|lua, n: i64| {
            lua.globals().raw_set("__cjson_encode_max_depth", n)?;
            Ok(())
        })
        .unwrap();
    cjson.set("encode_max_depth", enc_depth_fn).unwrap();

    let dec_depth_fn = lua
        .create_function(|lua, n: i64| {
            lua.globals().raw_set("__cjson_decode_max_depth", n)?;
            Ok(())
        })
        .unwrap();
    cjson.set("decode_max_depth", dec_depth_fn).unwrap();

    let enc_invalid_fn = lua
        .create_function(|lua, enabled: bool| {
            lua.globals()
                .raw_set("__cjson_encode_invalid_numbers", enabled)?;
            Ok(())
        })
        .unwrap();
    cjson.set("encode_invalid_numbers", enc_invalid_fn).unwrap();

    lua.globals().set("cjson", cjson).unwrap();
    lua.globals().set("cmsgpack", cmsgpack).unwrap();

    // LuaJIT BitOp library (bit.*): emulated using Lua 5.4's built-in bitwise operators.
    lua.load(
        r#"
bit = {}
bit.tobit  = function(n) return n & 0xFFFFFFFF end
bit.band   = function(a, ...) local r = a for _, v in ipairs({...}) do r = r & v end return r & 0xFFFFFFFF end
bit.bor    = function(a, ...) local r = a for _, v in ipairs({...}) do r = r | v end return r & 0xFFFFFFFF end
bit.bxor   = function(a, ...) local r = a for _, v in ipairs({...}) do r = r ~ v end return r & 0xFFFFFFFF end
bit.bnot   = function(a) return (~a) & 0xFFFFFFFF end
bit.lshift = function(a, n) return (a << n) & 0xFFFFFFFF end
bit.rshift = function(a, n) return (a >> n) & 0xFFFFFFFF end
bit.arshift= function(a, n) return (a >> n) end
bit.rol    = function(a, n) n = n & 31; return ((a << n) | (a >> (32 - n))) & 0xFFFFFFFF end
bit.ror    = function(a, n) n = n & 31; return ((a >> n) | (a << (32 - n))) & 0xFFFFFFFF end
bit.bswap  = function(a)
    return ((a & 0xFF) << 24) | (((a >> 8) & 0xFF) << 16) | (((a >> 16) & 0xFF) << 8) | ((a >> 24) & 0xFF)
end
bit.tohex = function(n, width)
    n = n & 0xFFFFFFFF
    width = width or 8
    if width < 0 then width = -width end
    -- LuaJIT widens INT_MIN negation; clamp so string.format never sees a bogus width.
    if width > 8 then width = 8 end
    if width < 1 then width = 8 end
    return string.format("%0" .. width .. "X", n)
end

"#,
    )
    .exec()
    .unwrap();

    // Redis's `struct` library: see [`STRUCT_LIBRARY`]. Compiled into a VM
    // the first time a script there reaches for it: every VM made it up
    // front, and the functions VM held a third of a megabyte more.
    let stub = lua.create_table().unwrap();
    let meta = lua.create_table().unwrap();
    meta.set(
        "__index",
        lua.create_function(|lua, (stub, key): (LuaTable, LuaValue)| {
            let library: LuaTable = lua.load(STRUCT_LIBRARY).set_name("=struct").eval()?;
            for pair in library.pairs::<LuaValue, LuaValue>() {
                let (name, value) = pair?;
                stub.raw_set(name, value)?;
            }
            stub.set_metatable(None);
            stub.raw_get::<LuaValue>(key)
        })
        .unwrap(),
    )
    .unwrap();
    stub.set_metatable(Some(meta));
    lua.globals().set("struct", stub).unwrap();

    // Lua 5.1 compat: `unpack` was a global; in 5.2+ it moved to `table.unpack`.
    // Redis's scripting environment re-exports it as a global, so scripts can use either form.
    lua.load("unpack = table.unpack").exec().unwrap();

    // A table raised by `error` is kept with its `err` and the line, as
    // Redis's error handler reads one: see [`remember_error_table`]. `pcall`
    // and `assert` are Redis's (#90).
    // SAFETY: C functions that do what Lua's own `error`, Lua 5.1's `assert`
    // and Redis's `pcall` do.
    unsafe {
        let globals = lua.globals();
        globals
            .set("error", lua.create_c_function(script_error).unwrap())
            .unwrap();
        globals
            .set("assert", lua.create_c_function(script_assert).unwrap())
            .unwrap();
        globals
            .set("pcall", lua.create_c_function(script_pcall).unwrap())
            .unwrap();
    }

    // Final setup: cjson globals, os sandbox, readonly proxies for API tables, pre-declared
    // globals, and globals protection. Must run LAST.
    lua.load(
        r#"
-- cjson state globals and the array metatable.
local _guard = {}
_guard.__is_cjson_array = true
_guard.__newindex = function(t, k, v)
    error("Attempt to modify a readonly table")
end
_guard.__index = _guard
local _mt = {}
setmetatable(_mt, _guard)
__cjson_array_mt             = _mt
__cjson_array_mt_mode        = false
__cjson_encode_max_depth     = 1000
__cjson_decode_max_depth     = 1000
__cjson_encode_invalid_numbers = false

local _G_ref = _G
local _setmetatable = setmetatable
local _real_getmetatable = getmetatable

-- Sandbox: only `clock` is a real field so pairs(os) sees exactly one entry (Redis test).
local _clock = os.clock
local _deny = { execute = true, exit = true, getenv = true, remove = true, rename = true, setlocale = true, tmpname = true }
os = { clock = _clock }
_setmetatable(os, {
    __index = function(t, k)
        if k == "clock" then return _clock end
        local v = rawget(t, k)
        if v ~= nil then return v end
        if _deny[k] then
            return function()
                error("ERR attempt to call field '" .. k .. "'", 2)
            end
        end
        return nil
    end,
})

-- Nothing that reaches outside the sandbox. `package.loaded` still held the
-- real `io` and `os` (and so `os.execute`, `io.popen`) after the globals were
-- replaced: a script could run programs on the host. `require`, `loadfile`
-- and `dofile` read files; `string.dump` and binary chunks given to `load`
-- are bytecode, which Lua does not check and which can corrupt memory. As
-- in Redis, `load` takes source text only.
package = nil
require = nil
loadfile = nil
dofile = nil
debug = nil
string.dump = nil
do
    local _load = load
    load = function(chunk, name, mode, ...)
        if select('#', ...) > 0 then
            return _load(chunk, name, "t", ...)
        end
        return _load(chunk, name, "t")
    end
end

-- Pre-declare KEYS and ARGV so set_global_keys_argv can raw-assign them after protection.
KEYS = {}
ARGV = {}

-- Capture API tables, then drop raw globals so `redis = …`, `_G = …`, etc. hit __newindex (Redis Tcl).
local _redis_inner = redis
local _cjson_inner = cjson
local _cmsgpack_inner = cmsgpack
local _bit_inner = bit

rawset(_G_ref, "redis", nil)
rawset(_G_ref, "cjson", nil)
rawset(_G_ref, "cmsgpack", nil)
rawset(_G_ref, "bit", nil)
rawset(_G_ref, "_G", nil)

-- Read-only proxies: Rust mutates the inner `redis` table via registry `__kividb_redis_inner`.
local _redis_pub = {}
_setmetatable(_redis_pub, {
    __index = _redis_inner,
    __newindex = function()
        error("Attempt to modify a readonly table", 2)
    end,
})

local _cjson_pub = {}
_setmetatable(_cjson_pub, {
    __index = _cjson_inner,
    __newindex = function()
        error("Attempt to modify a readonly table", 2)
    end,
})

local _cmsgpack_pub = {}
_setmetatable(_cmsgpack_pub, {
    __index = _cmsgpack_inner,
    __newindex = function()
        error("Attempt to modify a readonly table", 2)
    end,
})

local _bit_pub = {}
_setmetatable(_bit_pub, {
    __index = _bit_inner,
    __newindex = function()
        error("Attempt to modify a readonly table", 2)
    end,
})

local _globals_mt

local function env_index(t, k)
    if k == "_G" then
        return t
    end
    if k == "redis" then
        return _redis_pub
    end
    if k == "cjson" then
        return _cjson_pub
    end
    if k == "cmsgpack" then
        return _cmsgpack_pub
    end
    if k == "bit" then
        return _bit_pub
    end
    local v = rawget(t, k)
    if v ~= nil then
        return v
    end
    error("Script attempted to access nonexistent global variable '" .. k .. "'", 2)
end

local function env_newindex(t, k, v)
    error("Attempt to modify a readonly table", 2)
end

_globals_mt = {
    __newindex = env_newindex,
    __index = env_index,
}
_setmetatable(_globals_mt, {
    __newindex = function()
        error("Attempt to modify a readonly table", 2)
    end,
})

function setmetatable(t, mt)
    if t == _G_ref then
        error("Attempt to modify a readonly table", 2)
    end
    return _setmetatable(t, mt)
end

_setmetatable(_G_ref, _globals_mt)

-- Hide the real env metatable behind a readonly view so scripts cannot mutate __index/__newindex (Redis trick 2).
local _mt_view = {}
_setmetatable(_mt_view, {
    __index = function(_, k)
        return _globals_mt[k]
    end,
    __newindex = function()
        error("Attempt to modify a readonly table", 2)
    end,
})

-- Standard Lua metatables for string/number/etc. are mutable; Redis exposes them read-only so
-- `getmetatable('').__index = …` errors (tests/unit/scripting.tcl "basic types metatable").
local _readonly_builtin_mt_cache = setmetatable({}, { __mode = "k" })
local function readonly_builtin_metatable_view(mt)
    local p = _readonly_builtin_mt_cache[mt]
    if p ~= nil then
        return p
    end
    p = {}
    _setmetatable(p, {
        __index = mt,
        __newindex = function()
            error("Attempt to modify a readonly table", 2)
        end,
    })
    _readonly_builtin_mt_cache[mt] = p
    return p
end

function getmetatable(t)
    if t == _G_ref then
        return _mt_view
    end
    local mt = _real_getmetatable(t)
    if mt == nil then
        return nil
    end
    if type(mt) == "table" then
        return readonly_builtin_metatable_view(mt)
    end
    return mt
end
"#,
    )
    .exec()
    .unwrap();

    let loadstring_nil_call = lua
        .create_function(|_, _: LuaMultiValue| -> LuaResult<LuaMultiValue> {
            Err(LuaError::RuntimeError("attempt to call a nil value".into()))
        })
        .unwrap();
    lua.globals()
        .raw_set("loadstring", loadstring_nil_call)
        .unwrap();

    // Standard Lua base library registers these; remove so `_G.__index` reports undeclared globals
    // (Redis tests/unit/scripting.tcl: loadfile / dofile / print).
    for name in ["print", "loadfile", "dofile"] {
        lua.globals().raw_set(name, LuaNil).unwrap();
    }

    lua
}

/// Find `\r\n` starting from `start`; returns index of `\r`.
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    buf[start..]
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| p + start)
}

/// Parse one RESP value from `buf` at `pos`. Returns `(value, new_pos)`.
fn parse_resp_value(lua: &Lua, buf: &[u8], pos: usize) -> LuaResult<(LuaValue, usize)> {
    if pos >= buf.len() {
        return Err(LuaError::RuntimeError("RESP parse: empty buffer".into()));
    }

    match buf[pos] {
        b'+' => {
            let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                LuaError::RuntimeError("RESP parse: unterminated simple string".into())
            })?;
            let s = std::str::from_utf8(&buf[pos + 1..end])
                .map_err(|e| LuaError::RuntimeError(e.to_string()))?;
            // Every status reply is a table `{ok = "..."}` in Lua, as in
            // Redis — not only `+OK`. Scripts rely on it: BullMQ reads
            // `redis.call("TYPE", key)["ok"]`, and when that was nil its
            // removeOrphanedJobs found no job in any state and deleted live
            // ones.
            let t = lua.create_table()?;
            t.set("ok", s)?;
            Ok((LuaValue::Table(t), end + 2))
        }

        b'-' => {
            let end = find_crlf(buf, pos + 1)
                .ok_or_else(|| LuaError::RuntimeError("RESP parse: unterminated error".into()))?;
            let msg = std::str::from_utf8(&buf[pos + 1..end]).unwrap_or("ERR");
            Err(LuaError::RuntimeError(msg.to_string()))
        }

        b':' => {
            let end = find_crlf(buf, pos + 1)
                .ok_or_else(|| LuaError::RuntimeError("RESP parse: unterminated integer".into()))?;
            let n: i64 = std::str::from_utf8(&buf[pos + 1..end])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            Ok((LuaValue::Integer(n), end + 2))
        }

        b'$' => {
            let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                LuaError::RuntimeError("RESP parse: unterminated bulk length".into())
            })?;
            let len_str = std::str::from_utf8(&buf[pos + 1..end]).unwrap_or("-1");
            let len: i64 = len_str.parse().unwrap_or(-1);
            if len == -1 {
                // Redis nil bulk → Lua false (not nil). See Redis Lua type conversion rules.
                return Ok((LuaValue::Boolean(false), end + 2));
            }
            let len = len as usize;
            let data_start = end + 2;
            let data_end = data_start + len;
            if data_end + 2 > buf.len() {
                return Err(LuaError::RuntimeError(
                    "RESP parse: bulk data truncated".into(),
                ));
            }
            let val = LuaValue::String(lua.create_string(&buf[data_start..data_end])?);
            Ok((val, data_end + 2))
        }

        b'*' => {
            let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                LuaError::RuntimeError("RESP parse: unterminated array length".into())
            })?;
            let count_str = std::str::from_utf8(&buf[pos + 1..end]).unwrap_or("-1");
            let count: i64 = count_str.parse().unwrap_or(-1);
            if count == -1 {
                return Ok((LuaValue::Boolean(false), end + 2));
            }
            if count < 0 {
                return Err(LuaError::RuntimeError(
                    "RESP parse: invalid array count".into(),
                ));
            }
            let t = lua.create_table()?;
            let mut cur = end + 2;
            for i in 1..=(count as usize) {
                let (item, next) = parse_resp_value(lua, buf, cur)?;
                t.set(i, item)?;
                cur = next;
            }
            Ok((LuaValue::Table(t), cur))
        }

        b'%' => {
            let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                LuaError::RuntimeError("RESP parse: unterminated map length".into())
            })?;
            let count_str = std::str::from_utf8(&buf[pos + 1..end]).unwrap_or("0");
            let count: usize = count_str
                .parse()
                .map_err(|_| LuaError::RuntimeError("RESP parse: bad map length".into()))?;
            let inner = lua.create_table()?;
            let mut cur = end + 2;
            for _ in 0..count {
                let (k, next_k) = parse_resp_value(lua, buf, cur)?;
                let (v, next_v) = parse_resp_value(lua, buf, next_k)?;
                match k {
                    LuaValue::String(ks) => inner.set(ks, v)?,
                    LuaValue::Integer(i) => inner.set(i, v)?,
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "RESP parse: map key must be string or integer".into(),
                        ));
                    }
                }
                cur = next_v;
            }
            let outer = lua.create_table()?;
            outer.set("map", inner)?;
            Ok((LuaValue::Table(outer), cur))
        }

        b',' => {
            let end = find_crlf(buf, pos + 1)
                .ok_or_else(|| LuaError::RuntimeError("RESP parse: unterminated double".into()))?;
            let s = std::str::from_utf8(&buf[pos + 1..end])
                .map_err(|e| LuaError::RuntimeError(e.to_string()))?;
            let f: f64 = s
                .parse()
                .map_err(|_| LuaError::RuntimeError("RESP parse: bad double".into()))?;
            let t = lua.create_table()?;
            t.set("double", f)?;
            Ok((LuaValue::Table(t), end + 2))
        }

        b'_' => {
            let end = find_crlf(buf, pos + 1)
                .ok_or_else(|| LuaError::RuntimeError("RESP parse: unterminated null".into()))?;
            if end != pos + 1 {
                return Err(LuaError::RuntimeError("RESP parse: malformed null".into()));
            }
            Ok((LuaNil, end + 2))
        }

        b'#' => {
            if pos + 4 > buf.len() {
                return Err(LuaError::RuntimeError("RESP parse: truncated bool".into()));
            }
            let (bval, next) = match &buf[pos..pos + 4] {
                b"#t\r\n" => (true, pos + 4),
                b"#f\r\n" => (false, pos + 4),
                _ => {
                    return Err(LuaError::RuntimeError(
                        "RESP parse: malformed boolean".into(),
                    ));
                }
            };
            Ok((LuaValue::Boolean(bval), next))
        }

        b'(' => {
            let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                LuaError::RuntimeError("RESP parse: unterminated big number".into())
            })?;
            let s = std::str::from_utf8(&buf[pos + 1..end])
                .map_err(|e| LuaError::RuntimeError(e.to_string()))?;
            let t = lua.create_table()?;
            t.set("big_number", s)?;
            Ok((LuaValue::Table(t), end + 2))
        }

        b'=' => {
            let hdr_end = find_crlf(buf, pos + 1).ok_or_else(|| {
                LuaError::RuntimeError("RESP parse: unterminated verbatim header".into())
            })?;
            let n: usize = std::str::from_utf8(&buf[pos + 1..hdr_end])
                .map_err(|e| LuaError::RuntimeError(e.to_string()))?
                .parse()
                .map_err(|_| LuaError::RuntimeError("RESP parse: bad verbatim length".into()))?;
            let body_start = hdr_end + 2;
            let body_end = body_start + n;
            if body_end + 2 > buf.len() {
                return Err(LuaError::RuntimeError(
                    "RESP parse: verbatim payload truncated".into(),
                ));
            }
            let body = &buf[body_start..body_end];
            if body.len() < 4 || body[3] != b':' {
                return Err(LuaError::RuntimeError(
                    "RESP parse: bad verbatim body".into(),
                ));
            }
            let ext = std::str::from_utf8(&body[..3])
                .map_err(|e| LuaError::RuntimeError(e.to_string()))?;
            let payload = std::str::from_utf8(&body[4..])
                .map_err(|e| LuaError::RuntimeError(e.to_string()))?;
            let inner = lua.create_table()?;
            inner.set("string", payload)?;
            inner.set("format", ext)?;
            let outer = lua.create_table()?;
            outer.set("verbatim_string", inner)?;
            Ok((LuaValue::Table(outer), body_end + 2))
        }

        b'~' => {
            let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                LuaError::RuntimeError("RESP parse: unterminated set length".into())
            })?;
            let count: usize = std::str::from_utf8(&buf[pos + 1..end])
                .map_err(|e| LuaError::RuntimeError(e.to_string()))?
                .parse()
                .map_err(|_| LuaError::RuntimeError("RESP parse: bad set length".into()))?;
            let inner = lua.create_table()?;
            let mut cur = end + 2;
            for _ in 0..count {
                let (elem, next) = parse_resp_value(lua, buf, cur)?;
                match elem {
                    LuaValue::String(s) => inner.set(s, true)?,
                    LuaValue::Integer(i) => inner.set(i, true)?,
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "RESP parse: set member must be string or integer".into(),
                        ));
                    }
                }
                cur = next;
            }
            let outer = lua.create_table()?;
            outer.set("set", inner)?;
            Ok((LuaValue::Table(outer), cur))
        }

        byte => Err(LuaError::RuntimeError(format!(
            "RESP parse: unknown type byte 0x{byte:02x}"
        ))),
    }
}

/// Strip RESP3 push / attribute prefixes (Redis does not expose them to Lua).
fn skip_resp3_push_and_attrib(lua: &Lua, buf: &[u8], mut pos: usize) -> LuaResult<usize> {
    loop {
        if pos >= buf.len() {
            return Ok(pos);
        }
        match buf[pos] {
            b'|' => {
                let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                    LuaError::RuntimeError("RESP parse: unterminated attribute header".into())
                })?;
                let n: usize = std::str::from_utf8(&buf[pos + 1..end])
                    .map_err(|e| LuaError::RuntimeError(e.to_string()))?
                    .parse()
                    .map_err(|_| {
                        LuaError::RuntimeError("RESP parse: bad attribute count".into())
                    })?;
                pos = end + 2;
                for _ in 0..n {
                    let (_, p2) = parse_resp_value(lua, buf, pos)?;
                    let (_, p3) = parse_resp_value(lua, buf, p2)?;
                    pos = p3;
                }
            }
            b'>' => {
                let end = find_crlf(buf, pos + 1).ok_or_else(|| {
                    LuaError::RuntimeError("RESP parse: unterminated push header".into())
                })?;
                let n: usize = std::str::from_utf8(&buf[pos + 1..end])
                    .map_err(|e| LuaError::RuntimeError(e.to_string()))?
                    .parse()
                    .map_err(|_| LuaError::RuntimeError("RESP parse: bad push count".into()))?;
                pos = end + 2;
                for _ in 0..n {
                    let (_, p2) = parse_resp_value(lua, buf, pos)?;
                    pos = p2;
                }
            }
            _ => return Ok(pos),
        }
    }
}

fn resp_to_lua(lua: &Lua, buf: &[u8]) -> LuaResult<LuaValue> {
    if buf.is_empty() {
        return Ok(LuaNil);
    }
    let pos0 = skip_resp3_push_and_attrib(lua, buf, 0)?;
    let (val, mut rest_pos) = parse_resp_value(lua, buf, pos0)?;
    while rest_pos < buf.len() && matches!(buf[rest_pos], b'\r' | b'\n' | b' ' | b'\t') {
        rest_pos += 1;
    }
    if rest_pos != buf.len() {
        return Err(LuaError::RuntimeError(
            "RESP parse: trailing garbage after value".into(),
        ));
    }
    Ok(val)
}

/// Encode a Lua value to JSON. Returns a LuaError for invalid key types, NaN (when not
/// enabled), or excessive nesting depth (depth is only counted for container tables).
fn lua_to_json_value(lua: &Lua, val: &LuaValue, depth: i64) -> LuaResult<serde_json::Value> {
    let allow_invalid_numbers = matches!(
        lua.globals()
            .get::<LuaValue>("__cjson_encode_invalid_numbers"),
        Ok(LuaValue::Boolean(true))
    );

    match val {
        LuaNil => Ok(serde_json::Value::Null),
        LuaValue::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        LuaValue::Integer(n) => Ok(serde_json::Value::Number((*n).into())),
        LuaValue::Number(n) => {
            if !allow_invalid_numbers && (n.is_nan() || n.is_infinite()) {
                return Err(LuaError::RuntimeError(
                    "Cannot serialise number: NaN/Inf".into(),
                ));
            }
            Ok(serde_json::Number::from_f64(*n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        LuaValue::String(s) => Ok(serde_json::Value::String(
            String::from_utf8_lossy(s.as_bytes().as_ref()).into_owned(),
        )),
        LuaValue::Table(t) => {
            // Only tables (containers) count toward the depth limit.
            let max_depth: i64 = match lua.globals().get::<LuaValue>("__cjson_encode_max_depth") {
                Ok(LuaValue::Integer(n)) => n,
                _ => 1000,
            };
            if depth >= max_depth {
                return Err(LuaError::RuntimeError(
                    "Cannot serialise, excessive nesting".into(),
                ));
            }
            // When decode_array_with_array_mt(true) is active, decoded arrays carry a
            // metatable whose guard exposes __is_cjson_array=true via __index.
            // Use get (lua_gettable, respects __index) not raw_get so we read through the guard.
            let is_array_tagged = t
                .metatable()
                .map(|mt| {
                    matches!(
                        mt.get::<LuaValue>("__is_cjson_array"),
                        Ok(LuaValue::Boolean(true))
                    )
                })
                .unwrap_or(false);

            if is_array_tagged {
                // Force array encoding regardless of content (including empty tables).
                let mut arr = Vec::new();
                let mut i = 1i64;
                loop {
                    match t.get::<LuaValue>(i) {
                        Ok(LuaNil) | Err(_) => break,
                        Ok(v) => arr.push(lua_to_json_value(lua, &v, depth + 1)?),
                    }
                    i += 1;
                }
                return Ok(serde_json::Value::Array(arr));
            }

            // Inspect all key-value pairs to decide: pure integer-keyed array vs object.
            // A table is encoded as a JSON array only when all keys are consecutive integers
            // starting from 1 with no string/other keys mixed in (matching Redis cjson).
            let mut max_int_key = 0i64;
            let mut total_keys = 0i64;
            let mut has_string_key = false;

            for pair in t.clone().pairs::<LuaValue, LuaValue>() {
                let Ok((k, _)) = pair else {
                    continue;
                };
                total_keys += 1;
                match &k {
                    LuaValue::String(_) => has_string_key = true,
                    LuaValue::Integer(n) if *n >= 1 => {
                        if *n > max_int_key {
                            max_int_key = *n;
                        }
                    }
                    LuaValue::Integer(_) | LuaValue::Number(_) => has_string_key = true,
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "Cannot serialise boolean: invalid table key".into(),
                        ));
                    }
                }
            }

            // Pure sequential array: all keys are integers 1..n, no string keys, no gaps.
            let is_pure_array = !has_string_key && max_int_key == total_keys && total_keys > 0;

            if is_pure_array {
                let mut arr = Vec::new();
                let mut i = 1i64;
                loop {
                    match t.get::<LuaValue>(i) {
                        Ok(LuaNil) | Err(_) => break,
                        Ok(v) => arr.push(lua_to_json_value(lua, &v, depth + 1)?),
                    }
                    i += 1;
                }
                Ok(serde_json::Value::Array(arr))
            } else {
                let mut map = serde_json::Map::new();
                for pair in t.clone().pairs::<LuaValue, LuaValue>() {
                    let Ok((k, v)) = pair else {
                        continue;
                    };
                    let key = match &k {
                        LuaValue::String(s) => {
                            String::from_utf8_lossy(s.as_bytes().as_ref()).into_owned()
                        }
                        LuaValue::Integer(n) => n.to_string(),
                        LuaValue::Number(n) => format!("{}", *n as i64),
                        _ => {
                            return Err(LuaError::RuntimeError(
                                "Cannot serialise boolean: invalid table key".into(),
                            ));
                        }
                    };
                    map.insert(key, lua_to_json_value(lua, &v, depth + 1)?);
                }
                Ok(serde_json::Value::Object(map))
            }
        }
        _ => Ok(serde_json::Value::Null),
    }
}

/// Walk a parsed JSON value and return an error if the nesting depth exceeds `max_depth`.
fn json_depth_check(val: &serde_json::Value, depth: i64, max_depth: i64) -> Result<(), String> {
    if depth >= max_depth {
        return Err("Cannot parse, excessive nesting".into());
    }
    match val {
        serde_json::Value::Array(arr) => {
            for v in arr {
                json_depth_check(v, depth + 1, max_depth)?;
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                json_depth_check(v, depth + 1, max_depth)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn json_to_lua(
    lua: &Lua,
    val: &serde_json::Value,
    array_mt: Option<&LuaTable>,
) -> LuaResult<LuaValue> {
    match val {
        serde_json::Value::Null => Ok(LuaNil),
        serde_json::Value::Bool(b) => Ok(LuaValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                // Redis cjson: numbers with no fractional part (0.0, -5e3, 0e10) decode as
                // integers so that `table.concat` and Lua arithmetic produce integer output.
                if f.is_finite() && f.fract() == 0.0 {
                    let i = f as i64;
                    if i as f64 == f {
                        return Ok(LuaValue::Integer(i));
                    }
                }
                Ok(LuaValue::Number(f))
            } else {
                Ok(LuaValue::Number(0.0))
            }
        }
        serde_json::Value::String(s) => Ok(LuaValue::String(lua.create_string(s.as_str())?)),
        serde_json::Value::Array(arr) => {
            let t = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                t.set(i + 1, json_to_lua(lua, v, array_mt)?)?;
            }
            // When decode_array_with_array_mt(true) is active, tag this array table with the
            // special metatable so the encoder can distinguish it from an empty object.
            if let Some(mt) = array_mt {
                t.set_metatable(Some(mt.clone()));
            }
            Ok(LuaValue::Table(t))
        }
        serde_json::Value::Object(map) => {
            let t = lua.create_table()?;
            for (k, v) in map {
                t.set(k.as_str(), json_to_lua(lua, v, array_mt)?)?;
            }
            Ok(LuaValue::Table(t))
        }
    }
}

fn lua_value_to_resp(lua: &Lua, val: LuaValue) -> LuaResult<Vec<u8>> {
    lua_to_resp_inner(lua, val, 0)
}

/// Matches Redis `luaReplyToRedisReply` nesting limits (stack / safeguard); cyclic tables must not
/// blow the Rust stack or hang forever.
const LUA_SCRIPT_RESP_ENCODE_MAX_DEPTH: u32 = 64;
/// Singleton-array chains (`{ [1] = { ... } }`), including cycles — Redis streams `*1\r\n` until limit.
const LUA_SINGLETON_CHAIN_MAX_HOPS: u32 = 8000;

fn lua_table_key_is_one(k: &LuaValue) -> bool {
    match k {
        LuaValue::Integer(i) => *i == 1,
        LuaValue::Number(n) => *n == 1.0,
        _ => false,
    }
}

fn lua_table_is_pure_singleton_array(t: &LuaTable) -> LuaResult<bool> {
    let mut count = 0usize;
    let mut key_ok = false;
    for pair in t.clone().pairs::<LuaValue, LuaValue>() {
        let (k, _) = pair?;
        count += 1;
        if count > 1 {
            return Ok(false);
        }
        key_ok = lua_table_key_is_one(&k);
    }
    Ok(count == 1 && key_ok)
}

/// Fast path for nested `{ [1] = sub }` lists (and two-node cycles). Matches Redis `luaReplyToRedisReply`.
fn lua_try_encode_singleton_chain(
    lua: &Lua,
    start: LuaTable,
    depth: u32,
) -> LuaResult<Option<Vec<u8>>> {
    if !lua_table_is_pure_singleton_array(&start)? {
        return Ok(None);
    }
    let mut out = Vec::new();
    let mut cur = start;
    let mut hops = 0u32;

    loop {
        let v1 = cur.raw_get::<LuaValue>(1)?;
        match v1 {
            LuaValue::Table(next) => {
                if !lua_table_is_pure_singleton_array(&cur)? {
                    return Ok(None);
                }
                out.extend_from_slice(b"*1\r\n");
                hops += 1;
                if hops >= LUA_SINGLETON_CHAIN_MAX_HOPS {
                    out.extend_from_slice(b"-ERR reached lua stack limit\r\n");
                    return Ok(Some(out));
                }
                cur = next;
            }
            leaf => {
                if !lua_table_is_pure_singleton_array(&cur)? {
                    return Ok(None);
                }
                out.extend_from_slice(b"*1\r\n");
                let encoded = lua_to_resp_inner(lua, leaf, depth.saturating_add(1))?;
                out.extend_from_slice(&encoded);
                return Ok(Some(out));
            }
        }
    }
}

#[allow(clippy::only_used_in_recursion)]
fn lua_to_resp_inner(lua: &Lua, val: LuaValue, depth: u32) -> LuaResult<Vec<u8>> {
    if depth > LUA_SCRIPT_RESP_ENCODE_MAX_DEPTH {
        return Err(LuaError::RuntimeError("reached lua stack limit".into()));
    }
    let next_depth = depth.saturating_add(1);
    match val {
        LuaNil => {
            let eff_resp: u8 = SCRIPT_EVAL
                .with(|c| {
                    c.borrow().as_ref().map(|se| {
                        if se.script_resp != 2 {
                            se.script_resp
                        } else {
                            let ctx = unsafe { &*(se.ctx_addr as *const Ctx<'_>) };
                            client_connection_resp(ctx)
                        }
                    })
                })
                .unwrap_or(2);
            if eff_resp >= 3 {
                Ok(resp_null_resp3())
            } else {
                Ok(resp_null_bulk())
            }
        }
        LuaValue::Boolean(b) => {
            if b {
                Ok(resp_integer(1))
            } else {
                Ok(resp_null_bulk())
            }
        }
        LuaValue::Integer(n) => Ok(resp_integer(n)),
        LuaValue::Number(n) => Ok(resp_integer(n as i64)),
        LuaValue::String(s) => Ok(resp_bulk(s.as_bytes().as_ref())),
        LuaValue::Table(t) => {
            // Effective RESP for nested replies: `redis.setresp` overrides the connection.
            let eff_resp: u8 = SCRIPT_EVAL
                .with(|c| {
                    c.borrow().as_ref().map(|se| {
                        if se.script_resp != 2 {
                            se.script_resp
                        } else {
                            let ctx = unsafe { &*(se.ctx_addr as *const Ctx<'_>) };
                            client_connection_resp(ctx)
                        }
                    })
                })
                .unwrap_or(2);

            // Match Redis `luaReplyToRedisReply` in script_lua.c: status/error/array slots use
            // `lua_rawget`, and numeric indices use `lua_rawget` in a loop. Metamethods must not
            // run during RESP conversion (otherwise empty tables with __index would error or run
            // redis.call side effects).
            if let Ok(LuaValue::String(ok_s)) = t.raw_get::<LuaValue>("ok") {
                let st = ok_s.to_string_lossy();
                return Ok(resp_simple(st.as_ref()));
            }
            if let Ok(LuaValue::String(err_s)) = t.raw_get::<LuaValue>("err") {
                let st = err_s.to_string_lossy();
                return Ok(resp_error(st.as_ref()));
            }
            if let Ok(v) = t.raw_get::<LuaValue>("double") {
                match v {
                    LuaValue::Number(f) => {
                        if eff_resp >= 3 {
                            return Ok(resp_double_resp3(f));
                        }
                        return Ok(resp_double_bulk(f));
                    }
                    LuaValue::Integer(n) => {
                        let f = n as f64;
                        if eff_resp >= 3 {
                            return Ok(resp_double_resp3(f));
                        }
                        return Ok(resp_double_bulk(f));
                    }
                    _ => {}
                }
            }
            if let Ok(LuaValue::String(bs)) = t.raw_get::<LuaValue>("big_number") {
                let mut s = bs.to_string_lossy().to_string();
                s = s.replace("\r\n", "  ");
                let s = s.trim().to_string();
                if eff_resp >= 3 {
                    return Ok(resp_bignum_resp3(&s));
                }
                return Ok(resp_bulk_str(&s));
            }
            if let Ok(LuaValue::Table(vs)) = t.raw_get::<LuaValue>("verbatim_string") {
                let fmt = match vs.raw_get::<LuaValue>("format")? {
                    LuaValue::String(fs) => fs.to_string_lossy().to_string(),
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "ERR verbatim_string.format must be a string".into(),
                        ));
                    }
                };
                let body = match vs.raw_get::<LuaValue>("string")? {
                    LuaValue::String(ss) => ss.to_string_lossy().to_string(),
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "ERR verbatim_string.string must be a string".into(),
                        ));
                    }
                };
                if eff_resp >= 3 {
                    return Ok(resp_verbatim_resp3(fmt.trim(), body.as_str()));
                }
                return Ok(resp_bulk_str(body.as_str()));
            }
            if let Some(streamed) = lua_try_encode_singleton_chain(lua, t.clone(), depth)? {
                return Ok(streamed);
            }
            if let Ok(LuaValue::Table(set_inner)) = t.raw_get::<LuaValue>("set") {
                let mut parts: Vec<Vec<u8>> = Vec::new();
                for pair in set_inner.clone().pairs::<LuaValue, LuaValue>() {
                    let (k, _) = pair?;
                    parts.push(lua_to_resp_inner(lua, k, next_depth)?);
                }
                if eff_resp >= 3 {
                    return Ok(resp_set_resp3(parts));
                }
                return Ok(resp_array(parts));
            }
            if let Ok(LuaValue::Table(map_inner)) = t.raw_get::<LuaValue>("map") {
                let mut chunks: Vec<Vec<u8>> = Vec::new();
                for pair in map_inner.clone().pairs::<LuaValue, LuaValue>() {
                    let (k, v) = pair?;
                    chunks.push(lua_to_resp_inner(lua, k, next_depth)?);
                    chunks.push(lua_to_resp_inner(lua, v, next_depth)?);
                }
                if eff_resp >= 3 {
                    return Ok(resp_map(chunks));
                }
                return Ok(resp_array(chunks));
            }
            let mut items: Vec<Vec<u8>> = Vec::new();
            let mut i = 1i64;
            loop {
                match t.raw_get::<LuaValue>(i) {
                    Ok(LuaNil) => break,
                    Ok(v) => items.push(lua_to_resp_inner(lua, v, next_depth)?),
                    Err(e) => return Err(e),
                }
                i += 1;
            }
            Ok(resp_array(items))
        }
        _ => Ok(resp_null_bulk()),
    }
}

fn validate_library_name(name: &str) -> Result<(), String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(
            "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long".into(),
        );
    }
    Ok(())
}

fn parse_shebang(body: &str) -> Result<(String, String), String> {
    let first_line = body.lines().next().unwrap_or("").trim_end();
    let Some(shebang) = first_line.strip_prefix("#!") else {
        return Err("ERR Missing library metadata".into());
    };
    // `#!<engine> name=<library>`: the engine is what follows `#!` at once,
    // so `#! name=x` names no engine.
    let mut parts = shebang.split(' ');
    let engine = parts.next().unwrap_or("");
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!("ERR Engine '{engine}' not found"));
    }
    let mut name = None;
    for part in parts.filter(|part| !part.is_empty()) {
        let Some(given) = part.strip_prefix("name=") else {
            return Err(format!("ERR Invalid metadata value given: {part}"));
        };
        if name.is_some() {
            return Err(
                "ERR Invalid metadata value, name argument was given multiple times".into(),
            );
        }
        name = Some(given.trim_matches('"').to_string());
    }
    let name = name.ok_or_else(|| "ERR Library name was not given".to_string())?;
    validate_library_name(&name)?;
    Ok(("LUA".into(), name))
}

fn strip_shebang_for_lua(body: &str) -> String {
    let t = body.trim_start();
    if t.starts_with("#!") {
        if let Some(nl) = body.find('\n') {
            body[nl + 1..].to_string()
        } else {
            String::new()
        }
    } else {
        body.to_string()
    }
}

/// Format a Lua `number` for `redis.call` argv (must preserve `0.01` for `DEBUG SLEEP`, etc.).
#[inline]
fn lua_float_redis_arg(n: f64) -> Vec<u8> {
    if !n.is_finite() {
        return b"0".to_vec();
    }
    // Redis uses integer digits when the value is integral (INCRBY args, scores as ints).
    // Truncating every float to `i64` breaks sub-second sleeps: `0.01 as i64` → `"0"`.
    if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        ((n as i64).to_string()).into_bytes()
    } else {
        n.to_string().into_bytes()
    }
}

// ── redis.call ──────────────────────────────────────────────────────────────
//
// A script calls `redis.call` ten or twenty times, and what each call cost
// around the command it ran was more than the command: a wrapper that takes
// every argument out of Lua as a value with a reference of its own, hands
// them over as a list, and takes the result back the same way. So
// `redis.call` is a C function, as Lua means them: it reads its arguments
// where they are on Lua's stack, runs the command, and puts the reply on the
// stack as the Lua values it becomes.
//
// It makes the replies nearly every command gives — an integer, a string,
// nil, a status, a list of those — and nothing else. An error, a reply of
// RESP3's kinds, an argument that is not a string or a number: for those it
// sets what it has aside and calls what `redis.call` used to be, which
// raises the error or builds the value exactly as before. Nothing about what
// a script sees is decided here that was not decided there.

/// What `bridge` leaves for `reply_set_aside`.
enum SetAside {
    /// The command ran; this is its reply.
    Reply(Vec<u8>),
    /// It did not: this is why.
    Refused(&'static str),
}

thread_local! {
    static SET_ASIDE: RefCell<Option<SetAside>> = const { RefCell::new(None) };
}

fn set_aside(what: SetAside) {
    SET_ASIDE.with(|aside| *aside.borrow_mut() = Some(what));
}

/// The names, in Lua's registry, of what finishes a call `bridge` does not.
const REST_OF_CALL: &str = "__kividb_rest_of_call";
const REST_OF_PCALL: &str = "__kividb_rest_of_pcall";

/// What `redis.call` returns or raises for what was set aside.
fn reply_set_aside(lua: &Lua) -> LuaResult<LuaValue> {
    match SET_ASIDE.with(|aside| aside.borrow_mut().take()) {
        Some(SetAside::Reply(resp)) => resp_to_lua(lua, &resp),
        Some(SetAside::Refused(why)) => Err(LuaError::RuntimeError(why.into())),
        None => Err(LuaError::RuntimeError(
            "redis.call outside EVAL/FCALL".into(),
        )),
    }
}

unsafe extern "C-unwind" fn redis_call(state: *mut mlua::ffi::lua_State) -> std::ffi::c_int {
    unsafe { bridge(state, c"__kividb_rest_of_call") }
}

unsafe extern "C-unwind" fn redis_pcall(state: *mut mlua::ffi::lua_State) -> std::ffi::c_int {
    unsafe { bridge(state, c"__kividb_rest_of_pcall") }
}

/// `redis.call` and `redis.pcall`: run the command, and return its reply or
/// leave it to `rest`.
///
/// A Lua error leaves a C function by a jump past everything in it, so
/// nothing of Rust's that needs dropping may be alive when one can be
/// raised. All of it lives inside `run_and_push`; by the time `rest` is
/// called, which is the only thing here that raises, there is nothing.
unsafe fn bridge(state: *mut mlua::ffi::lua_State, rest: &std::ffi::CStr) -> std::ffi::c_int {
    use mlua::ffi;
    SCRIPT_CALLS.set(SCRIPT_CALLS.get().wrapping_add(1));
    // A panic must not travel through Lua's frames.
    let pushed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        run_and_push(state)
    }))
    .unwrap_or_else(|_| {
        set_aside(SetAside::Refused("ERR the command failed unexpectedly"));
        false
    });
    if pushed {
        return 1;
    }
    unsafe {
        ffi::lua_settop(state, 0);
        ffi::lua_getfield(state, ffi::LUA_REGISTRYINDEX, rest.as_ptr());
        ffi::lua_call(state, 0, 1);
    }
    1
}

/// Run the command on Lua's stack. True if its reply is now on the stack;
/// false if it, or the reason it was not run, has been set aside.
unsafe fn run_and_push(state: *mut mlua::ffi::lua_State) -> bool {
    use mlua::ffi;
    let Some((addr, read_only, resp)) = SCRIPT_EVAL.with(|c| {
        c.borrow()
            .as_ref()
            .map(|s| (s.ctx_addr, s.read_only, s.script_resp))
    }) else {
        set_aside(SetAside::Refused("redis.call outside EVAL/FCALL"));
        return false;
    };

    let count = unsafe { ffi::lua_gettop(state) };
    if count == 0 {
        set_aside(SetAside::Refused(
            "ERR Please specify at least one argument for redis.call()",
        ));
        return false;
    }
    // A string is used where Lua has it; a number is written out.
    enum Arg {
        Lua(*const u8, usize),
        Written(usize),
    }
    let mut written: Vec<Vec<u8>> = Vec::new();
    let mut args: Vec<Arg> = Vec::with_capacity(count as usize);
    for index in 1..=count {
        let bytes = match unsafe { ffi::lua_type(state, index) } {
            ffi::LUA_TSTRING => {
                let mut len = 0usize;
                let ptr = unsafe { ffi::lua_tolstring(state, index, &mut len) };
                args.push(Arg::Lua(ptr.cast(), len));
                continue;
            }
            ffi::LUA_TNUMBER => {
                if unsafe { ffi::lua_isinteger(state, index) } != 0 {
                    unsafe { ffi::lua_tointeger(state, index) }
                        .to_string()
                        .into_bytes()
                } else {
                    lua_float_redis_arg(unsafe { ffi::lua_tonumber(state, index) })
                }
            }
            ffi::LUA_TBOOLEAN => {
                if unsafe { ffi::lua_toboolean(state, index) } != 0 {
                    b"1".to_vec()
                } else {
                    b"0".to_vec()
                }
            }
            _ => {
                set_aside(SetAside::Refused(
                    "ERR Lua redis lib command arguments must be strings or integers",
                ));
                return false;
            }
        };
        written.push(bytes);
        args.push(Arg::Written(written.len() - 1));
    }
    let cmd: Vec<&[u8]> = args
        .iter()
        .map(|arg| match arg {
            // SAFETY: Lua keeps a string that is on its stack, and these
            // stay on it until this function returns.
            Arg::Lua(ptr, len) => unsafe { std::slice::from_raw_parts(*ptr, *len) },
            Arg::Written(at) => written[*at].as_slice(),
        })
        .collect();

    let ctx = unsafe { &mut *(addr as *mut Ctx<'_>) };
    let reply = execute_command_from_script(ctx, cmd, read_only);
    // RESP3's kinds (`redis.setresp(3)`) are the old path's.
    if resp == 2 && unsafe { push_reply(state, &reply) } {
        return true;
    }
    set_aside(SetAside::Reply(reply));
    false
}

/// Put `reply` on Lua's stack as the value it is in Lua. False, with the
/// stack as it was, if it is not one of the kinds made here.
unsafe fn push_reply(state: *mut mlua::ffi::lua_State, reply: &[u8]) -> bool {
    use mlua::ffi;
    let top = unsafe { ffi::lua_gettop(state) };
    match unsafe { push_value(state, reply, 0, 0) } {
        Some(end) if end == reply.len() => true,
        _ => {
            unsafe { ffi::lua_settop(state, top) };
            false
        }
    }
}

/// One value of `buf`, from `pos`: where the next begins, or `None` for a
/// kind not made here (with whatever was pushed on the way left for the
/// caller to remove).
unsafe fn push_value(
    state: *mut mlua::ffi::lua_State,
    buf: &[u8],
    pos: usize,
    depth: u32,
) -> Option<usize> {
    use mlua::ffi;
    let kind = *buf.get(pos)?;
    let line_end = pos + 1 + memchr::memchr(b'\r', buf.get(pos + 1..)?)?;
    if buf.get(line_end + 1) != Some(&b'\n') {
        return None;
    }
    let line = &buf[pos + 1..line_end];
    let number = |line: &[u8]| std::str::from_utf8(line).ok()?.parse::<i64>().ok();
    // A table and the value going into it, for each level of nesting.
    if depth > 64 || unsafe { ffi::lua_checkstack(state, 4) } == 0 {
        return None;
    }
    match kind {
        b':' => {
            unsafe { ffi::lua_pushinteger(state, number(line)?) };
            Some(line_end + 2)
        }
        b'$' => {
            let len = number(line)?;
            if len == -1 {
                // A nil bulk is false in Lua, as in Redis.
                unsafe { ffi::lua_pushboolean(state, 0) };
                return Some(line_end + 2);
            }
            let start = line_end + 2;
            let end = start.checked_add(usize::try_from(len).ok()?)?;
            let data = buf.get(start..end)?;
            if buf.get(end..end + 2) != Some(b"\r\n") {
                return None;
            }
            unsafe { ffi::lua_pushlstring(state, data.as_ptr().cast(), data.len()) };
            Some(end + 2)
        }
        b'+' => {
            // Every status is a table `{ok = "..."}`, as in Redis.
            std::str::from_utf8(line).ok()?;
            unsafe {
                ffi::lua_createtable(state, 0, 1);
                ffi::lua_pushlstring(state, line.as_ptr().cast(), line.len());
                ffi::lua_setfield(state, -2, c"ok".as_ptr());
            }
            Some(line_end + 2)
        }
        b'*' => {
            let count = number(line)?;
            if count == -1 {
                unsafe { ffi::lua_pushboolean(state, 0) };
                return Some(line_end + 2);
            }
            let count = usize::try_from(count).ok()?;
            unsafe { ffi::lua_createtable(state, count.min(1 << 20) as std::ffi::c_int, 0) };
            let mut next = line_end + 2;
            for index in 1..=count {
                next = unsafe { push_value(state, buf, next, depth + 1) }?;
                unsafe { ffi::lua_rawseti(state, -2, index as ffi::lua_Integer) };
            }
            Some(next)
        }
        // An error, and everything of RESP3.
        _ => None,
    }
}

/// Marks a VM whose `redis.call` and `redis.pcall` are the real ones.
struct RedisHandlersInstalled;

fn install_redis_eval_handlers(lua: &Lua) -> Result<(), String> {
    // Once for a VM: the two functions capture nothing and nothing takes them
    // out again, and making them anew for every script was a twentieth of
    // the time a short script took.
    if lua.app_data_ref::<RedisHandlersInstalled>().is_some() {
        return Ok(());
    }
    // What a script calls is a C function of Lua's own (`bridge`). What that
    // leaves — an error, a reply in a shape it does not make itself — it
    // hands, already run, to one of these two, which are what `redis.call`
    // and `redis.pcall` used to be from that point on.
    let call_fn = lua
        .create_function(|lua, ()| reply_set_aside(lua))
        .map_err(|e| e.to_string())?;
    lua.set_named_registry_value(REST_OF_CALL, call_fn)
        .map_err(|e| e.to_string())?;

    let redis_table: LuaTable = lua
        .named_registry_value("__kividb_redis_inner")
        .map_err(|e| e.to_string())?;
    // SAFETY: `redis_call` keeps to the rules of a Lua C function: see `bridge`.
    let fast_call = unsafe { lua.create_c_function(redis_call) }.map_err(|e| e.to_string())?;
    redis_table
        .set("call", fast_call)
        .map_err(|e| e.to_string())?;

    // `redis.pcall` mirrors `redis.call` but catches Redis error replies and returns them as
    // `{err = bare_msg}` instead of raising a Lua error.  We call `execute_redis_from_lua`
    // directly (not through `call.call()`) so that mlua's `protect_lua!` traceback handler never
    // wraps the message — the `err` field must contain the bare Redis error string, matching Redis.
    let pcall_fn = lua
        .create_function(|lua, ()| match reply_set_aside(lua) {
            Ok(v) => Ok(v),
            Err(LuaError::RuntimeError(msg)) => {
                let t = lua.create_table()?;
                t.set("err", msg)?;
                Ok(LuaValue::Table(t))
            }
            Err(e) => Err(e),
        })
        .map_err(|e| e.to_string())?;
    lua.set_named_registry_value(REST_OF_PCALL, pcall_fn)
        .map_err(|e| e.to_string())?;
    // SAFETY: as `redis_call`.
    let fast_pcall = unsafe { lua.create_c_function(redis_pcall) }.map_err(|e| e.to_string())?;
    redis_table
        .set("pcall", fast_pcall)
        .map_err(|e| e.to_string())?;
    lua.set_app_data(RedisHandlersInstalled);
    Ok(())
}

fn set_global_keys_argv<A: AsRef<[u8]>>(lua: &Lua, keys: &[A], argv: &[A]) -> Result<(), String> {
    // Both tables are made on Lua's own stack, in one protected call: the
    // strings go from the command straight into the tables, with no value
    // of mlua's made for any of them on the way.
    //
    // SAFETY: the closure leaves the stack as it found it, and runs
    // protected, so that Lua running out of memory is an error returned and
    // not a jump out of here.
    unsafe {
        lua.exec_raw::<()>((), |state| {
            use mlua::ffi;
            ffi::lua_checkstack(state, 6);
            ffi::lua_rawgeti(state, ffi::LUA_REGISTRYINDEX, ffi::LUA_RIDX_GLOBALS);
            for (name, items) in [(c"KEYS", keys), (c"ARGV", argv)] {
                ffi::lua_pushstring(state, name.as_ptr());
                ffi::lua_createtable(state, items.len() as std::ffi::c_int, 0);
                for (at, item) in items.iter().enumerate() {
                    let item = item.as_ref();
                    ffi::lua_pushlstring(state, item.as_ptr().cast(), item.len());
                    ffi::lua_rawseti(state, -2, (at + 1) as ffi::lua_Integer);
                }
                // Raw: past the `__newindex` that protects the globals.
                ffi::lua_rawset(state, -3);
            }
            ffi::lua_pop(state, 1);
        })
    }
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "EVAL" => Some(eval(ctx, false, false)),
        "EVALSHA" => Some(eval(ctx, true, false)),
        "EVAL_RO" => Some(eval(ctx, false, true)),
        "EVALSHA_RO" => Some(eval(ctx, true, true)),
        "SCRIPT" => Some(script_cmd(ctx)),
        "FUNCTION" => Some(redis_function_dispatch(ctx)),
        "FCALL" => Some(fcall(ctx, false)),
        "FCALL_RO" => Some(fcall(ctx, true)),
        _ => None,
    }
}

fn eval(ctx: &mut Ctx, use_sha: bool, read_only: bool) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'eval' command");
    }

    let source: Arc<str> = if use_sha {
        let sha = match std::str::from_utf8(ctx.cmd[1]) {
            Ok(s) => s,
            Err(_) => return resp_error("ERR invalid SHA1"),
        };
        // As clients send it, nearly always: lower case already.
        let lowered;
        let sha = if sha.bytes().any(|b| b.is_ascii_uppercase()) {
            lowered = sha.to_lowercase();
            lowered.as_str()
        } else {
            sha
        };
        let found = script_store().read().get(sha).cloned();
        match found {
            Some(src) => {
                eval_script_used(sha);
                src
            }
            None => return resp_error("NOSCRIPT No matching script. Please use EVAL."),
        }
    } else {
        let source = match std::str::from_utf8(ctx.cmd[1]) {
            Ok(s) => s,
            Err(_) => return resp_error("ERR script is not valid UTF-8"),
        };
        // The stored copy, so that a script sent again and again is the one
        // already compiled (see `compiled_script`).
        let sha = sha1_hex(source);
        let stored = script_store().read().get(&sha).cloned();
        if let Some(stored) = stored {
            eval_script_used(&sha);
            stored
        } else {
            let mut added = false;
            let stored = script_store()
                .write()
                .entry(sha.clone())
                .or_insert_with(|| {
                    added = true;
                    Arc::from(source)
                })
                .clone();
            if added {
                eval_script_added(&sha);
            }
            stored
        }
    };

    let numkeys_raw: i64 = match std::str::from_utf8(ctx.cmd[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_error("ERR value is not an integer or out of range"),
    };
    if numkeys_raw < 0 {
        return resp_error("ERR Number of keys can't be negative");
    }
    let numkeys = numkeys_raw as usize;

    if ctx.cmd.len() < 3 + numkeys {
        return resp_error("ERR wrong number of arguments for 'eval' command");
    }

    // The arguments where they are, not copies: they are copied once, into
    // the Lua strings the script sees. (What they point at is the caller's
    // and outlives the script; `ctx.cmd` itself is swapped by each
    // `redis.call`, which is why these are taken out of it first.)
    let keys: Vec<&[u8]> = ctx.cmd[3..3 + numkeys].to_vec();
    let argv: Vec<&[u8]> = ctx.cmd[3 + numkeys..].to_vec();

    let ctx_ptr: *mut Ctx = ctx;

    let registry = ctx.client_registry;
    let client_id = ctx.client_id;
    count_script_commands(registry, client_id, || {
        match try_with_lua(Vm::Eval, |lua| {
            eval_script(lua, ctx_ptr, &source, keys, argv, read_only)
        }) {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => script_error_to_resp(&e, &sha1_hex(&source)),
            Err(LuaVmBusy) => resp_error(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            ),
        }
    })
}

fn strip_after_line_colon(s: &str) -> Option<&str> {
    for (i, c) in s.char_indices() {
        if c == ':' {
            let rest = &s[i + 1..];
            let digit_len = rest.chars().take_while(|c| c.is_ascii_digit()).count();
            if digit_len == 0 {
                continue;
            }
            let after_digits = &rest[digit_len..];
            if let Some(rest) = after_digits.strip_prefix(':') {
                return Some(rest.trim_start());
            }
        }
    }
    None
}

/// Redis Tcl (`tests/unit/scripting.tcl`) matches `*attempt to index a nil value script*` — the
/// substring must appear contiguously after chunk-prefix stripping; Lua often emits only
/// `attempt to index a nil value` or adds `(field '…')` in the middle.
fn normalize_redis_eval_nil_index_error(s: String) -> String {
    let t = s.trim();
    if t.starts_with("attempt to index a nil value")
        && !t.contains("attempt to index a nil value script")
    {
        return "attempt to index a nil value script".to_string();
    }
    s
}

/// Remove `[chunk]:line:` / `library:line:` prefixes so Tcl `assert_match ERR ...` matches.
fn strip_lua_chunk_prefix(mut s: &str) -> &str {
    loop {
        let next = if s.starts_with("[string ") {
            s.find("]:")
                .and_then(|idx| strip_after_line_colon(&s[idx + 2..]))
        } else {
            strip_after_line_colon(s)
        };
        match next {
            Some(rest) if rest.len() < s.len() => s = rest,
            _ => break,
        }
    }
    s
}

fn redis_simple_error_has_category_prefix(s: &str) -> bool {
    let first = s.split_whitespace().next().unwrap_or("");
    first.len() >= 2
        && first
            .chars()
            .all(|c| c.is_ascii_uppercase() || c == '_' || c == '-')
}

/// What [`remember_error_table`] puts before a table's `err`, then the line
/// and a `\u{1}`. Not NUL: mlua reads an error's message as a C string.
const TABLE_ERROR_MARK: &str = "\u{1}kividb-error\u{1}";

thread_local! {
    /// What [`remember_error_table`] kept of the table a script raised last
    /// on this thread: what `tostring` makes of it, then its message, marked.
    /// It lasts until the call it was raised in returns ([`table_errors`]),
    /// or until `error` raises another table, or a value that is not a
    /// string with the place before it. A finalizer's raise is not
    /// kept ([`in_finalizer`]).
    static RAISED_TABLE: RefCell<Option<Box<str>>> = const { RefCell::new(None) };
    /// Whether [`RAISED_TABLE`] holds anything: a call that succeeds reads
    /// this, which is one load, and not that.
    static TABLE_KEPT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The line and the message of a table [`remember_error_table`] kept, from a
/// Lua error's text.
fn marked_table_error(s: &str) -> Option<(&str, &str)> {
    let at = s.find(TABLE_ERROR_MARK)?;
    let rest = &s[at + TABLE_ERROR_MARK.len()..];
    let rest = &rest[..rest.find('\n').unwrap_or(rest.len())];
    Some(rest.split_once('\u{1}').unwrap_or(("1", rest)))
}

/// `error` in scripts: Lua's own (`luaB_error`), but a table it raises is
/// kept by [`remember_error_table`] first.
unsafe extern "C-unwind" fn script_error(state: *mut mlua::ffi::lua_State) -> std::ffi::c_int {
    unsafe {
        let level = mlua::ffi::luaL_optinteger(state, 2, 1) as std::ffi::c_int;
        mlua::ffi::lua_settop(state, 1);
        raise_script_error(state, level)
    }
}

/// `assert` in scripts: Lua 5.1's, which Redis has (#90). Its message is a
/// string or a number, given the place it was raised at, and
/// `assertion failed!` without one; a table is refused.
unsafe extern "C-unwind" fn script_assert(state: *mut mlua::ffi::lua_State) -> std::ffi::c_int {
    use mlua::ffi;
    unsafe {
        if ffi::lua_toboolean(state, 1) != 0 {
            return ffi::lua_gettop(state);
        }
        if ffi::lua_type(state, 1) == ffi::LUA_TNONE {
            return ffi::luaL_error(
                state,
                c"bad argument #1 to '%s' (value expected)".as_ptr(),
                called_as(state),
            );
        }
        if ffi::lua_type(state, 2) > ffi::LUA_TNIL && ffi::lua_isstring(state, 2) == 0 {
            return ffi::luaL_error(
                state,
                c"bad argument #2 to '%s' (string expected, got %s)".as_ptr(),
                called_as(state),
                ffi::luaL_typename(state, 2),
            );
        }
        let message = ffi::luaL_optstring(state, 2, c"assertion failed!".as_ptr());
        ffi::luaL_error(state, c"%s".as_ptr(), message)
    }
}

/// The name the running C function was called by, as Lua 5.1 gives it in a
/// bad-argument error: `'?'` when the call gives none, as from `pcall` (5.4
/// would look for a global of that value).
unsafe fn called_as(state: *mut mlua::ffi::lua_State) -> *const std::ffi::c_char {
    use mlua::ffi;
    unsafe {
        let mut ar: ffi::lua_Debug = std::mem::zeroed();
        if ffi::lua_getstack(state, 0, &mut ar) != 0
            && ffi::lua_getinfo(state, c"n".as_ptr(), &mut ar) != 0
            && !ar.name.is_null()
        {
            ar.name
        } else {
            c"?".as_ptr()
        }
    }
}

/// `pcall` in scripts: Lua's own (`luaB_pcall`, so a coroutine can still
/// yield inside it), but what it catches is handed back as Redis's `pcall`
/// (`luaRedisPcall`) hands it back (#90): see [`caught_as_redis`].
unsafe extern "C-unwind" fn script_pcall(state: *mut mlua::ffi::lua_State) -> std::ffi::c_int {
    use mlua::ffi;
    unsafe {
        ffi::luaL_checkany(state, 1);
        ffi::lua_pushboolean(state, 1);
        ffi::lua_insert(state, 1);
        let status = ffi::lua_pcallk(
            state,
            ffi::lua_gettop(state) - 2,
            ffi::LUA_MULTRET,
            0,
            0,
            Some(finish_script_pcall),
        );
        finish_script_pcall(state, status, 0)
    }
}

/// The rest of [`script_pcall`], as `finishpcall` is of `luaB_pcall`.
unsafe extern "C-unwind" fn finish_script_pcall(
    state: *mut mlua::ffi::lua_State,
    status: std::ffi::c_int,
    extra: mlua::ffi::lua_KContext,
) -> std::ffi::c_int {
    use mlua::ffi;
    unsafe {
        if status != ffi::LUA_OK && status != ffi::LUA_YIELD {
            let caught = caught_as_redis(state);
            ffi::lua_pushboolean(state, 0);
            ffi::lua_rotate(state, -(caught + 1), 1);
            return caught + 1;
        }
        ffi::lua_gettop(state) - extra as std::ffi::c_int
    }
}

/// What a script's `pcall` hands back for the error at the top of the stack,
/// left at the top: how many values that is. A table whose `err` is a string
/// or a number is its `err`, as in Redis, where `redis.call` raises such a
/// table; a table with any other `err` is followed by it, as Redis leaves it.
/// An error from Rust (`redis.call`, `cjson`, ...) is mlua's object, whose
/// `tostring` is `runtime error: ` and the message, then a traceback: it is
/// the message, as Redis's is. Anything else is left as it is.
///
/// The bytes read are Lua's, on the stack until they are copied: a Lua error
/// here unwinds through no Rust value that needs dropping.
unsafe fn caught_as_redis(state: *mut mlua::ffi::lua_State) -> std::ffi::c_int {
    use mlua::ffi;
    unsafe {
        match ffi::lua_type(state, -1) {
            ffi::LUA_TTABLE => {
                ffi::lua_getfield(state, -1, c"err".as_ptr());
                if ffi::lua_isstring(state, -1) == 0 {
                    return 2;
                }
                ffi::lua_replace(state, -2);
            }
            ffi::LUA_TUSERDATA => {
                let mut len = 0;
                let text = ffi::luaL_tolstring(state, -1, &mut len);
                let text = std::slice::from_raw_parts(text.cast::<u8>(), len);
                let text =
                    memchr::memmem::find(text, b"\nstack traceback").map_or(text, |at| &text[..at]);
                let text = text.strip_prefix(b"runtime error: ").unwrap_or(text);
                let text = text.trim_ascii_end();
                ffi::lua_pushlstring(state, text.as_ptr().cast(), text.len());
                ffi::lua_replace(state, -3);
                ffi::lua_pop(state, 1);
            }
            _ => {}
        }
        1
    }
}

/// Raise the value at 1 as `luaB_error` does, for `error`: a string with the
/// place at `level` before it, a table kept.
unsafe fn raise_script_error(state: *mut mlua::ffi::lua_State, level: std::ffi::c_int) -> ! {
    use mlua::ffi;
    unsafe {
        if ffi::lua_type(state, 1) == ffi::LUA_TSTRING && level > 0 {
            ffi::luaL_where(state, level);
            ffi::lua_pushvalue(state, 1);
            ffi::lua_concat(state, 2);
        } else {
            remember_error_table(state);
        }
        ffi::lua_error(state)
    }
}

/// Without this a table given to `error()` (`error({err = ...})`,
/// `error(redis.error_reply(...))`) comes back as `table: 0x…`: mlua makes
/// a string of any error that is not one, with `tostring`. So the table at 1
/// is kept in [`RAISED_TABLE`] as it is raised, while the line it is raised
/// on is known: what `tostring` makes of it, then its `err` (`ERR unknown
/// error` without one) and that line behind [`TABLE_ERROR_MARK`], with line
/// breaks made spaces as Redis's reply has them. [`table_errors`] puts that
/// in place of the `tostring` if the call fails with it.
///
/// Redis reads the table in a message handler its scripts are called under.
/// Here that cost every call about 2,400 instructions (an `EVALSHA` of a
/// `GET` 11% more) for the rare one that fails (#80); this costs nothing
/// until a table is raised. The table itself is left alone: a script that
/// catches it with `xpcall` has it as it made it.
///
/// The Lua calls that can fail (out of memory) come before any Rust value
/// is made, so a Lua error unwinds through none that needs dropping.
unsafe fn remember_error_table(state: *mut mlua::ffi::lua_State) {
    use mlua::ffi;
    unsafe {
        // A finalizer runs in whatever call collects its table, another
        // client's too: what it raises is not that call's.
        if in_finalizer(state) {
            return;
        }
        // A table raised before and caught is not what this fails with. A
        // string with the place before it is kept to: a `__tostring` of the
        // table's own that fails, while mlua makes the message, raises one.
        if ffi::lua_type(state, 1) != ffi::LUA_TTABLE || ffi::lua_checkstack(state, 4) == 0 {
            forget_raised_table();
            return;
        }
        // The frame that raised it: past `error` itself.
        let mut line = 1;
        let mut ar: ffi::lua_Debug = std::mem::zeroed();
        for level in 1..=3 {
            if ffi::lua_getstack(state, level, &mut ar) != 0
                && ffi::lua_getinfo(state, c"Sl".as_ptr(), &mut ar) != 0
                && std::ffi::CStr::from_ptr(ar.what).to_bytes() != b"C"
            {
                line = ar.currentline;
                break;
            }
        }
        // As mlua will, with a `__tostring` of the table's own too. If that
        // fails, mlua's message is what it failed with.
        ffi::lua_pushcfunction(state, value_tostring);
        ffi::lua_pushvalue(state, 1);
        if ffi::lua_pcall(state, 1, 1, 0) != ffi::LUA_OK
            && ffi::lua_type(state, -1) != ffi::LUA_TSTRING
        {
            ffi::lua_settop(state, 1);
            forget_raised_table();
            return;
        }
        ffi::lua_pushstring(state, c"err".as_ptr());
        match ffi::lua_rawget(state, 1) {
            ffi::LUA_TSTRING => {}
            ffi::LUA_TNUMBER => {
                let mut integer = 0;
                let n = ffi::lua_tointegerx(state, -1, &mut integer);
                if integer != 0 {
                    ffi::lua_pushfstring(state, c"%I".as_ptr(), n);
                } else {
                    ffi::lua_pushfstring(state, c"%f".as_ptr(), ffi::lua_tonumber(state, -1));
                }
                ffi::lua_replace(state, -2);
            }
            _ => {
                ffi::lua_pop(state, 1);
                ffi::lua_pushstring(state, c"ERR unknown error".as_ptr());
            }
        }
        // Two strings now, that `lua_tolstring` reads as they are.
        let bytes = |index| {
            let mut len = 0;
            let p = ffi::lua_tolstring(state, index, &mut len);
            String::from_utf8_lossy(std::slice::from_raw_parts(p.cast::<u8>(), len))
        };
        // The `err` up to a NUL, as Redis reads it.
        let msg = bytes(-1);
        let msg = msg.split('\0').next().unwrap_or_default();
        let kept = format!(
            "{}{TABLE_ERROR_MARK}{line}\u{1}{}",
            bytes(-2),
            msg.replace(['\r', '\n'], " ")
        );
        if RAISED_TABLE
            .try_with(|cell| *cell.borrow_mut() = Some(kept.into()))
            .is_ok()
        {
            let _ = TABLE_KEPT.try_with(|kept| kept.set(true));
        }
        ffi::lua_settop(state, 1);
    }
}

/// Forget what [`remember_error_table`] kept, if anything.
fn forget_raised_table() {
    if TABLE_KEPT
        .try_with(|kept| kept.replace(false))
        .unwrap_or(false)
    {
        let _ = RAISED_TABLE.try_with(|kept| kept.take());
    }
}

/// Whether a `__gc` metamethod is on the stack.
unsafe fn in_finalizer(state: *mut mlua::ffi::lua_State) -> bool {
    use mlua::ffi;
    unsafe {
        let mut ar: ffi::lua_Debug = std::mem::zeroed();
        let mut level = 0;
        while ffi::lua_getstack(state, level, &mut ar) != 0 {
            if ffi::lua_getinfo(state, c"n".as_ptr(), &mut ar) != 0
                && !ar.name.is_null()
                && std::ffi::CStr::from_ptr(ar.name).to_bytes() == b"__gc"
            {
                return true;
            }
            level += 1;
        }
        false
    }
}

/// `tostring` of the value at 1, for [`remember_error_table`] to call protected.
unsafe extern "C-unwind" fn value_tostring(state: *mut mlua::ffi::lua_State) -> std::ffi::c_int {
    unsafe {
        mlua::ffi::luaL_tolstring(state, 1, std::ptr::null_mut());
    }
    1
}

/// mlua makes a string of an error that is not one with `tostring`, so a
/// table a script raised comes back as `table: 0x…`. If a call failed with
/// the one [`remember_error_table`] kept, its message is put in its place.
/// What was kept goes either way: it does not outlive the call.
#[inline]
fn table_errors<T>(result: LuaResult<T>) -> LuaResult<T> {
    if !TABLE_KEPT.with(|kept| kept.get()) {
        return result;
    }
    with_kept_message(result)
}

/// [`table_errors`], with something kept.
#[cold]
fn with_kept_message<T>(result: LuaResult<T>) -> LuaResult<T> {
    TABLE_KEPT.with(|kept| kept.set(false));
    let kept = RAISED_TABLE.with(|kept| kept.take());
    let (Err(LuaError::RuntimeError(text)), Some(kept)) = (&result, kept) else {
        return result;
    };
    let Some(at) = kept.find(TABLE_ERROR_MARK) else {
        return result;
    };
    // mlua's message is that `tostring`, then its traceback on a new line.
    match text.strip_prefix(&kept[..at]) {
        Some(rest) if rest.is_empty() || rest.starts_with('\n') => {
            Err(LuaError::RuntimeError(format!("{}{rest}", &kept[at..])))
        }
        _ => result,
    }
}

/// Shape Lua `error()` / runtime abort strings into Redis-style `user_script:… script:` messages.
fn redis_lua_runtime_abort_shape(stripped: &str) -> String {
    let t = stripped.trim();
    if t.is_empty() {
        return "unknown error script: ".to_string();
    }

    // What a read-only script was refused is reported as the script's error,
    // with where it happened.
    if t.starts_with("ERR Write commands are not allowed from read-only scripts") {
        return format!("{t} script: ");
    }

    // Match Redis `luaReplyToRedisReply` stack guard — must not get the `user_script:` wrapper.
    if t.contains("reached lua stack limit") {
        return "ERR reached lua stack limit".to_string();
    }

    if redis_simple_error_has_category_prefix(t)
        || t.starts_with("ERR ")
        || t.starts_with("Script killed")
        || t.starts_with("attempt to ")
        || t.contains("Script attempted")
        || t.starts_with("BUSY ")
        || t.starts_with("NOREPLICAS ")
        || t.starts_with("MASTERDOWN ")
        || t.starts_with("LOADING ")
        || t.starts_with("READONLY ")
    {
        return t.to_string();
    }

    if t.contains("ERR table error") || t == "ERR table error" {
        return "ERR table error script: ".to_string();
    }

    if t.contains("user_script:") && t.contains("script:") {
        return t.to_string();
    }

    format!("user_script:1: {} script: ", t)
}

/// `name` is the script's SHA1, or the function's name.
fn script_error_to_resp(cleaned: &str, name: &str) -> Vec<u8> {
    if let Some(rest) = cleaned.strip_prefix(TABLE_ERROR_MARK) {
        let (line, msg) = rest.split_once('\u{1}').unwrap_or(("1", rest));
        return resp_error(&format!("{msg} script: {name}, on @user_script:{line}."));
    }
    let stripped = strip_lua_chunk_prefix(cleaned.trim());
    let shaped = redis_lua_runtime_abort_shape(stripped);
    // Match Redis wire errors: `-OOM …`, `-READONLY …`, or `-ERR …`. Do not turn `OOM …` into
    // `ERR OOM …` (Tcl `assert_error` matches `OOM*` for deny-oom scripting tests).
    let mut body = if shaped.starts_with("ERR ") || redis_simple_error_has_category_prefix(&shaped)
    {
        shaped
    } else {
        format!("ERR {}", shaped)
    };
    // Tcl `assert_match … script: *` expects a literal space after `script:` before the glob tail.
    if body.contains("user_script:") && body.ends_with("script:") {
        body.push(' ');
    }
    if body.ends_with("script: ") {
        body.push_str(name);
        body.push_str(", on @user_script:1.");
    }
    // Avoid double-counting INFO errorstats: `redis.call` errors already hit `resp_error`, then
    // Lua re-wraps them here. Pure Lua/user_script errors must still increment stats once.
    let likely_redis_call_forward = !body.contains("user_script:")
        && !body.contains("attempt to index")
        && !body.contains("attempt to call")
        && !body.contains("Script attempted")
        && !body.contains("readonly table")
        && !body.contains("Modify a readonly");
    if likely_redis_call_forward {
        crate::resp::write::resp_error_wire(&body)
    } else {
        resp_error(&body)
    }
}

/// Strip Lua's "runtime error: " prefix and any stack traceback appended by mlua's
/// `protect_lua!` traceback handler.  The resulting string is safe to embed in a RESP simple
/// error (`-MSG\r\n`) which must not contain embedded newlines.
fn clean_lua_error(e: &LuaError) -> String {
    let raw = match e {
        LuaError::RuntimeError(msg) => msg.clone(),
        LuaError::CallbackError { cause, .. } => return clean_lua_error(cause),
        _ => e.to_string(),
    };
    let s = raw.strip_prefix("runtime error: ").unwrap_or(raw.as_str());
    // A table's `err` (see `remember_error_table`): already one line, up to the
    // traceback mlua adds.
    if let Some(at) = s.find(TABLE_ERROR_MARK) {
        let marked = &s[at..];
        return marked[..marked.find('\n').unwrap_or(marked.len())].to_string();
    }
    // Truncate at the first newline so RESP framing is never broken by multi-line tracebacks.
    let one_line = if let Some(pos) = s.find('\n') {
        s[..pos].to_string()
    } else {
        s.to_string()
    };
    normalize_redis_eval_nil_index_error(strip_lua_chunk_prefix(one_line.trim()).to_string())
}

struct LuaHookGuard<'a>(&'a Lua);
impl Drop for LuaHookGuard<'_> {
    fn drop(&mut self) {
        LUA_SCRIPT_KILL_REQUESTED.store(false, Ordering::SeqCst);
        // The hook stays for the next script, which wants the same one —
        // unless a kill replaced it with the one that fires on every line:
        // that one goes, and the next script installs the ordinary one.
        if LUA_KILL_HOOK_ESCALATED.swap(false, Ordering::SeqCst) {
            self.0.remove_hook();
            self.0.remove_app_data::<KillHookInstalled>();
        }
    }
}

/// Marks a VM that has the hook `install_eval_timeout_hook` installs.
struct KillHookInstalled;

fn script_kill_hook_reply_if_requested() -> mlua::Result<VmState> {
    if LUA_SCRIPT_KILL_REQUESTED.load(Ordering::SeqCst) {
        let may_kill = SCRIPT_EVAL.with(|c| c.borrow().as_ref().map(|s| !s.wrote).unwrap_or(true));
        if may_kill {
            return Err(LuaError::RuntimeError("Script killed by user".into()));
        }
    }
    Ok(VmState::Continue)
}

fn install_eval_timeout_hook(lua: &Lua) -> Result<(), String> {
    // Once for a VM: the hook holds nothing of the script that installed
    // it, and installing it boxes a closure each time.
    if lua.app_data_ref::<KillHookInstalled>().is_some() {
        return Ok(());
    }
    lua.set_app_data(KillHookInstalled);
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(2000),
        move |lua_vm, _dbg| {
            if LUA_SCRIPT_KILL_REQUESTED.load(Ordering::SeqCst) {
                let may_kill =
                    SCRIPT_EVAL.with(|c| c.borrow().as_ref().map(|s| !s.wrote).unwrap_or(true));
                if may_kill {
                    // Redis `luaMaskCountHook`: reinstall so user code cannot catch with pcall and
                    // respin — clear kill only when eval unwinds (`LuaHookGuard::drop`).
                    if !LUA_KILL_HOOK_ESCALATED.swap(true, Ordering::SeqCst) {
                        lua_vm.set_hook(HookTriggers::new().every_line(), move |_lua2, _dbg2| {
                            script_kill_hook_reply_if_requested()
                        });
                    }
                    return Err(LuaError::RuntimeError("Script killed by user".into()));
                }
            }
            keep_the_runtime_listening();
            // Redis: exceeding lua-time-limit makes the server BUSY for other clients; the script
            // keeps running until SCRIPT KILL (read-only) — do not abort here on time alone.
            Ok(VmState::Continue)
        },
    );
    Ok(())
}

#[derive(Default)]
struct EvalShebangFlags {
    allow_oom: bool,
    no_writes: bool,
    allow_cross_slot: bool,
}

/// Redis `#lua` / `#!lua flags=…` first line — strip before `lua.load`, validate engine/options.
///
/// What is left to run is a part of `source`, not a copy of it.
fn preprocess_eval_script(source: &str) -> Result<(&str, EvalShebangFlags), String> {
    let mut flags = EvalShebangFlags::default();
    if !source.starts_with("#!") {
        return Ok((source, flags));
    }
    let nl = source
        .find('\n')
        .ok_or_else(|| "Invalid script shebang".to_string())?;
    let shebang_line = source[..nl].trim_end_matches('\r');
    let remainder = &source[nl..];

    let tokens: Vec<&str> = shebang_line.split_whitespace().collect();
    if tokens.is_empty() {
        return Err("Invalid engine in script shebang".into());
    }
    if tokens[0] != "#!lua" {
        return Err(format!(
            "Unexpected engine in script shebang: {}",
            tokens[0]
        ));
    }
    for tok in tokens.iter().skip(1) {
        if let Some(rest) = tok.strip_prefix("flags=") {
            for f in rest.split(',') {
                let f = f.trim();
                if f.is_empty() {
                    continue;
                }
                match f {
                    "allow-oom" => flags.allow_oom = true,
                    "no-writes" => flags.no_writes = true,
                    "allow-cross-slot-keys" => flags.allow_cross_slot = true,
                    "allow-stale" | "no-cluster" => {}
                    _ => return Err(format!("Unexpected flag in script shebang: {}", f)),
                }
            }
        } else {
            return Err(format!("Unknown lua shebang option: {}", tok));
        }
    }
    Ok((remainder, flags))
}

fn eval_script(
    lua: &Lua,
    ctx_ptr: *mut Ctx,
    source: &Arc<str>,
    keys: Vec<&[u8]>,
    argv: Vec<&[u8]>,
    read_only: bool,
) -> Result<Vec<u8>, String> {
    let ctx = unsafe { &mut *ctx_ptr };
    let saved_db = *ctx.selected_db;

    let had_shebang = source.starts_with("#!");
    let (lua_body, sb) = preprocess_eval_script(source)?;
    let effective_ro = read_only || sb.no_writes;
    let allow_oom = sb.allow_oom || sb.no_writes;

    // Redis `script.c`: shebang (non-compat) scripts must set allow-oom or no-writes to run under maxmemory.
    if had_shebang && !allow_oom && crate::commands::handler::check_memory_limit() {
        return Err(
            "OOM allow-oom flag is not set on the script, can not run it when used memory > 'maxmemory'"
                .into(),
        );
    }

    crate::script_clock::enter_script_execution();
    struct ScriptClockGuard;
    impl Drop for ScriptClockGuard {
        fn drop(&mut self) {
            crate::script_clock::exit_script_execution();
        }
    }
    let _script_clock_guard = ScriptClockGuard;

    let slot = first_key_slot(ctx, keys.first().copied());
    SCRIPT_EVAL.with(|c| {
        *c.borrow_mut() = Some(ScriptEval {
            ctx_addr: ctx_ptr as usize,
            read_only: effective_ro,
            script_resp: 2,
            wrote: false,
            allow_oom,
            had_shebang,
            no_writes_flag: sb.no_writes,
            allow_cross_slot: sb.allow_cross_slot,
            slot,
        });
    });

    let _clear_eval = ClearScriptEval { lua };
    // What the script writes goes to the AOF as one piece, when it ends.
    let _aof_group = crate::aof::RecordGroup::begin(ctx.aof_tx);

    install_redis_eval_handlers(lua)?;
    set_global_keys_argv(lua, &keys, &argv)?;

    install_eval_timeout_hook(lua)?;
    let _hook_guard = LuaHookGuard(lua);

    let result = match table_errors(
        compiled_script(lua, source, lua_body).and_then(|f| f.call::<LuaValue>(())),
    ) {
        Ok(v) => v,
        Err(e) => {
            *ctx.selected_db = saved_db;
            return Err(clean_lua_error(&e));
        }
    };

    let resp = match lua_value_to_resp(lua, result) {
        Ok(r) => r,
        Err(e) => {
            *ctx.selected_db = saved_db;
            return Err(clean_lua_error(&e));
        }
    };

    *ctx.selected_db = saved_db;
    drop(_clear_eval);
    Ok(resp)
}

/// Scripts already compiled, by their source: kept with the VM that compiled
/// them (its app data), and gone with it.
///
/// A script used to be compiled on every call — twice, as a rule: once as
/// an expression, which most are not, and once as written. For the scripts a
/// job queue runs by the thousand that was half of all the time spent on
/// them. Redis compiles a script once and keeps the function; so does this.
///
/// A script is known by where its text is and how long, which takes no
/// reading of the text: the entry keeps the source it is a part of, so the
/// address cannot come to mean another script while the entry is there.
#[derive(Default)]
struct CompiledScripts(ahash::AHashMap<(usize, usize), (Arc<str>, LuaFunction)>);

/// Scripts written out anew for every call (arguments pasted into the body)
/// would grow the cache without end: past this it starts again.
const COMPILED_SCRIPTS_MAX: usize = 4096;

/// The function `body` compiles to: what `Chunk::eval` would run, kept.
///
/// `body` is `source`, or the part of it after a shebang line.
fn compiled_script(lua: &Lua, source: &Arc<str>, body: &str) -> LuaResult<LuaFunction> {
    let key = (body.as_ptr() as usize, body.len());
    if let Some(cache) = lua.app_data_ref::<CompiledScripts>()
        && let Some((_, function)) = cache.0.get(&key)
    {
        return Ok(function.clone());
    }
    // As `eval` does: an expression if it is one (`1 + 1`), else a chunk.
    //
    // `=user_script` avoids mlua's default `[string "path/to/rust.rs:LINE"]` chunk labels.
    // Those contain `[...]` which Tcl's Redis test client parses as nested commands and crashes.
    let function = match lua
        .load(format!("return {body}"))
        .set_name("=user_script")
        .into_function()
    {
        Ok(function) => function,
        Err(_) => lua.load(body).set_name("=user_script").into_function()?,
    };
    if lua.app_data_ref::<CompiledScripts>().is_none() {
        lua.set_app_data(CompiledScripts::default());
    }
    if let Some(mut cache) = lua.app_data_mut::<CompiledScripts>() {
        if cache.0.len() >= COMPILED_SCRIPTS_MAX {
            cache.0.clear();
        }
        cache.0.insert(key, (Arc::clone(source), function.clone()));
    }
    Ok(function)
}

fn eval_function_call(
    lua: &Lua,
    ctx_ptr: *mut Ctx,
    func: LuaFunction,
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
    run: FunctionRun,
) -> Result<Vec<u8>, String> {
    let ctx = unsafe { &mut *ctx_ptr };
    let saved_db = *ctx.selected_db;

    crate::script_clock::enter_script_execution();
    struct ScriptClockGuard;
    impl Drop for ScriptClockGuard {
        fn drop(&mut self) {
            crate::script_clock::exit_script_execution();
        }
    }
    let _script_clock_guard = ScriptClockGuard;

    let slot = first_key_slot(ctx, keys.first().map(|k| k.as_slice()));
    SCRIPT_EVAL.with(|c| {
        *c.borrow_mut() = Some(ScriptEval {
            ctx_addr: ctx_ptr as usize,
            read_only: run.read_only,
            script_resp: 2,
            wrote: false,
            allow_oom: run.allow_oom,
            // A function is held to what a script with a first line is.
            had_shebang: true,
            no_writes_flag: run.no_writes,
            allow_cross_slot: run.allow_cross_slot,
            slot,
        });
    });

    let _clear_eval = ClearScriptEval { lua };
    // What the script writes goes to the AOF as one piece, when it ends.
    let _aof_group = crate::aof::RecordGroup::begin(ctx.aof_tx);

    install_redis_eval_handlers(lua)?;
    set_global_keys_argv(lua, &keys, &argv)?;

    install_eval_timeout_hook(lua)?;
    let _hook_guard = LuaHookGuard(lua);

    let keys_t: LuaTable = lua.globals().get("KEYS").map_err(|e| e.to_string())?;
    let argv_t: LuaTable = lua.globals().get("ARGV").map_err(|e| e.to_string())?;
    let result = match table_errors(
        func.call::<LuaValue>((LuaValue::Table(keys_t), LuaValue::Table(argv_t))),
    ) {
        Ok(v) => v,
        Err(e) => {
            *ctx.selected_db = saved_db;
            return Err(clean_lua_error(&e));
        }
    };

    let resp = match lua_value_to_resp(lua, result) {
        Ok(r) => r,
        Err(e) => {
            *ctx.selected_db = saved_db;
            return Err(clean_lua_error(&e));
        }
    };

    *ctx.selected_db = saved_db;
    drop(_clear_eval);
    Ok(resp)
}

/// Redis resolves `FCALL` function names case-insensitively against registered names.
fn resolve_registered_function_name(requested: &str) -> Result<String, String> {
    let m = fn_name_to_library().read();
    if m.contains_key(requested) {
        return Ok(requested.to_string());
    }
    for fname in m.keys() {
        if fname.eq_ignore_ascii_case(requested) {
            return Ok(fname.clone());
        }
    }
    Err("ERR Function not found".into())
}

fn fcall_inner(
    lua: &Lua,
    ctx_ptr: *mut Ctx,
    fn_name: &str,
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
    read_only: bool,
) -> Result<Vec<u8>, String> {
    let canon_name = resolve_registered_function_name(fn_name)?;
    let registry_key = function_key(&canon_name);
    if lua
        .named_registry_value::<LuaFunction>(&registry_key)
        .is_err()
    {
        let lib_name = fn_name_to_library()
            .read()
            .get(&canon_name)
            .cloned()
            .ok_or_else(|| "ERR Function not found".to_string())?;
        let body = function_store()
            .read()
            .get(&lib_name)
            .map(|lib| lib.body.clone())
            .ok_or_else(|| "ERR Function not found".to_string())?;
        RERUNNING_LIBRARY.with(|rerun| rerun.set(true));
        let ran = run_library_code(lua, &strip_shebang_for_lua(&body));
        RERUNNING_LIBRARY.with(|rerun| rerun.set(false));
        ran.map_err(|e| {
            format!(
                "ERR could not load function library into this thread's VM: {}",
                lua_error_message(&e)
            )
        })?;
    }

    let func: LuaFunction = lua
        .named_registry_value(&registry_key)
        .map_err(|_| "ERR Function not found".to_string())?;

    // What the function was registered with decides what it may do, as a
    // script's first line does for a script.
    let flags: Vec<String> = fn_name_to_library()
        .read()
        .get(&canon_name)
        .and_then(|lib| {
            let store = function_store().read();
            let lib = store.get(lib)?;
            let function = lib.registered.iter().find(|f| f.name == canon_name)?;
            Some(function.flags.clone())
        })
        .unwrap_or_default();
    let has = |flag: &str| flags.iter().any(|given| given == flag);
    let no_writes = has("no-writes");
    if read_only && !no_writes {
        return Err("ERR Can not execute a script with write flag using *_ro command.".into());
    }
    let allow_oom = has("allow-oom") || no_writes;
    if !allow_oom && crate::commands::handler::check_memory_limit() {
        return Err(
            "OOM allow-oom flag is not set on the script, can not run it when used memory > 'maxmemory'"
                .into(),
        );
    }

    eval_function_call(
        lua,
        ctx_ptr,
        func,
        keys,
        argv,
        FunctionRun {
            read_only: read_only || no_writes,
            allow_oom,
            no_writes,
            allow_cross_slot: has("allow-cross-slot-keys"),
        },
    )
}

/// What a function that is called may do.
struct FunctionRun {
    read_only: bool,
    allow_oom: bool,
    no_writes: bool,
    allow_cross_slot: bool,
}

fn fcall(ctx: &mut Ctx, read_only: bool) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'fcall' command");
    }

    let fn_name = match std::str::from_utf8(ctx.cmd[1]) {
        Ok(s) => s,
        Err(_) => return resp_error("ERR Function name is not valid UTF-8"),
    };

    // For `FUNCTION STATS`, and for which of the two KILLs applies.
    struct Running;
    impl Drop for Running {
        fn drop(&mut self) {
            *RUNNING_FUNCTION.lock() = None;
        }
    }
    *RUNNING_FUNCTION.lock() = Some((
        fn_name.to_string(),
        ctx.cmd.iter().map(|arg| arg.to_vec()).collect(),
    ));
    let _running = Running;

    let numkeys_raw: i64 = match std::str::from_utf8(ctx.cmd[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return resp_error("ERR Bad number of keys provided"),
    };
    if numkeys_raw < 0 {
        return resp_error("ERR Number of keys can't be negative");
    }
    let numkeys = numkeys_raw as usize;

    if ctx.cmd.len() < 3 + numkeys {
        return resp_error("ERR Number of keys can't be greater than number of args");
    }

    let keys: Vec<Vec<u8>> = ctx
        .cmd
        .iter()
        .skip(3)
        .take(numkeys)
        .map(|s| s.to_vec())
        .collect();
    let argv: Vec<Vec<u8>> = ctx
        .cmd
        .iter()
        .skip(3 + numkeys)
        .map(|s| s.to_vec())
        .collect();

    let ctx_ptr: *mut Ctx = ctx;

    let registry = ctx.client_registry;
    let client_id = ctx.client_id;
    count_script_commands(registry, client_id, || {
        match try_with_lua(Vm::Functions, |lua| {
            fcall_inner(lua, ctx_ptr, fn_name, keys, argv, read_only)
        }) {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => script_error_to_resp(&e, fn_name),
            Err(LuaVmBusy) => resp_error(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            ),
        }
    })
}

fn function_load(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'function|load' command");
    }

    let body_idx = cmd.len() - 1;
    let mut replace = false;
    for tok in cmd.iter().take(body_idx).skip(2) {
        let tok = match std::str::from_utf8(tok) {
            Ok(s) => s,
            Err(_) => return resp_error("ERR Unknown option given"),
        };
        if tok.eq_ignore_ascii_case("REPLACE") {
            replace = true;
        } else {
            return resp_error("ERR Unknown option given");
        }
    }

    let body = match std::str::from_utf8(cmd[body_idx]) {
        Ok(s) => s.to_string(),
        Err(_) => return resp_error("ERR Library code not valid UTF-8"),
    };

    let (engine, lib_name) = match parse_shebang(&body) {
        Ok(p) => p,
        Err(e) => return resp_error(&e),
    };

    // A library's name is its name exactly, as in Redis: looked up, not
    // looked for. (It used to be compared with every library's, whatever the
    // case of the letters — so loading the ten-thousandth library took ten
    // thousand times as long as the first.)
    //
    // The library it replaces, if it does.
    let replaced: Vec<String> = match function_store().read().get(&lib_name) {
        Some(_) if !replace => {
            return resp_error(&format!(
                "ERR Library '{lib_name}' already exists. Use REPLACE to overwrite it."
            ));
        }
        Some(lib) => lib.functions.clone(),
        None => Vec::new(),
    };

    let loaded = try_with_lua(Vm::Functions, |lua| -> Result<Vec<Registered>, String> {
        let registered =
            load_library_code(lua, &body, Some(std::time::Duration::from_millis(500)))?;
        // A function's name is its own across libraries: one that another
        // library has is not taken from it.
        let taken = {
            let owners = fn_name_to_library().read();
            registered.iter().find_map(|function| {
                owners
                    .get(&function.name)
                    .filter(|owner| **owner != lib_name)
                    .map(|_| function.name.clone())
            })
        };
        if let Some(name) = taken {
            discard_registered(lua, &registered);
            return Err(format!("ERR Function {name} already exists"));
        }
        let gone: Vec<String> = replaced
            .iter()
            .filter(|old| !registered.iter().any(|new| new.name == **old))
            .cloned()
            .collect();
        commit_registered(lua, &registered, &gone)
            .map_err(|e| format!("ERR Error registering functions: {}", lua_error_message(&e)))?;
        gc_after_load(lua);
        Ok(registered)
    });
    let registered = match loaded {
        Ok(Ok(registered)) => registered,
        Ok(Err(e)) => return resp_error(&e),
        Err(LuaVmBusy) => {
            return resp_error(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            );
        }
    };
    let names: Vec<String> = registered.iter().map(|f| f.name.clone()).collect();

    let lib = FunctionLibrary {
        name: lib_name.clone(),
        engine,
        body,
        functions: names.clone(),
        registered,
    };

    function_store().write().insert(lib_name.clone(), lib);

    {
        let mut m = fn_name_to_library().write();
        for fname in &replaced {
            m.remove(fname);
        }
        for fname in &names {
            m.insert(fname.clone(), lib_name.clone());
        }
    }

    resp_bulk_str(&lib_name)
}

fn function_delete(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 3 {
        return resp_error("ERR wrong number of arguments for 'function|delete' command");
    }
    let name = match std::str::from_utf8(ctx.cmd[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return resp_error("ERR Library name is not valid UTF-8"),
    };
    let removed = function_store().write().remove(&name);
    if let Some(lib) = removed {
        let mut m = fn_name_to_library().write();
        for fname in &lib.functions {
            m.remove(fname);
        }
        return resp_simple("OK");
    }
    resp_error("ERR Library not found")
}

fn function_list(ctx: &mut Ctx) -> Vec<u8> {
    let mut with_code = false;
    let mut lib_filter: Option<String> = None;
    let mut args = ctx.cmd.iter().skip(2);
    while let Some(arg) = args.next() {
        if arg.eq_ignore_ascii_case(b"WITHCODE") && !with_code {
            with_code = true;
        } else if arg.eq_ignore_ascii_case(b"LIBRARYNAME") && lib_filter.is_none() {
            let Some(pattern) = args.next() else {
                return resp_error("ERR library name argument was not given");
            };
            lib_filter = Some(String::from_utf8_lossy(pattern).into_owned());
        } else {
            return resp_error(&format!(
                "ERR Unknown argument {}",
                String::from_utf8_lossy(arg)
            ));
        }
    }

    let store = function_store().read();
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for lib in store.values() {
        if let Some(ref pat) = lib_filter
            && !glob_match(pat, &lib.name)
        {
            continue;
        }

        let mut fn_entries: Vec<Vec<u8>> = Vec::new();
        for fname in &lib.functions {
            let registered = lib.registered.iter().find(|f| f.name == *fname);
            let description = registered
                .and_then(|f| f.description.as_deref())
                .map_or_else(resp_null_bulk, resp_bulk_str);
            let flags = registered
                .map(|f| f.flags.iter().map(|flag| resp_bulk_str(flag)).collect())
                .unwrap_or_default();
            fn_entries.push(resp_array(vec![
                resp_bulk_str("name"),
                resp_bulk_str(fname),
                resp_bulk_str("description"),
                description,
                resp_bulk_str("flags"),
                resp_array(flags),
            ]));
        }

        let mut parts: Vec<Vec<u8>> = vec![
            resp_bulk_str("library_name"),
            resp_bulk_str(&lib.name),
            resp_bulk_str("engine"),
            resp_bulk_str(&lib.engine),
            resp_bulk_str("functions"),
            resp_array(fn_entries),
        ];
        if with_code {
            parts.push(resp_bulk_str("library_code"));
            parts.push(resp_bulk(lib.body.as_bytes()));
        }
        rows.push(resp_array(parts));
    }

    resp_array(rows)
}

fn function_flush(ctx: &Ctx) -> Vec<u8> {
    // `FUNCTION FLUSH [ASYNC | SYNC]`: either way it is done before the
    // reply.
    match ctx.cmd.len() {
        2 => {}
        3 if ctx.cmd[2].eq_ignore_ascii_case(b"SYNC")
            || ctx.cmd[2].eq_ignore_ascii_case(b"ASYNC") => {}
        3 => return resp_error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option"),
        _ => {
            return resp_error(
                "ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.",
            );
        }
    }
    let libraries = std::mem::take(&mut *function_store().write());
    let names = std::mem::take(&mut *fn_name_to_library().write());
    let vm = match lua_vms_renew_for_admin(&[Vm::Functions]) {
        Ok(vm) => vm,
        Err(LuaVmBusy) => {
            return resp_error(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            );
        }
    };
    // `ASYNC`: the VM and the libraries are freed in the background, and
    // counted as Redis counts them, a function each and one for the VM.
    if ctx.cmd.len() == 3 && ctx.cmd[2].eq_ignore_ascii_case(b"ASYNC") {
        let functions: u64 = libraries.values().map(|l| l.functions.len() as u64).sum();
        crate::lazyfree::free_later((vm, libraries, names), functions + 1);
    }
    resp_simple("OK")
}

/// Take each of `vms` out under one hold of the lock, for the next script
/// or function to make a new one, and make each thread's own VMs of those
/// kinds stale. Only when no script holds the lock (`LUA_HOLD_DEPTH == 0`):
/// otherwise nothing is changed. Returns the VMs taken out, for the caller to
/// drop or free in the background.
///
/// Redis puts a new VM in at once and reports its size; here the new one is
/// made when first wanted, and 0 is reported until then. An empty VM of ours
/// (with cjson, cmsgpack, bit and the read-only proxies) is about 50 KB,
/// where Redis's suite allows a flushed functions VM 40000 bytes.
fn lua_vms_renew_for_admin(vms: &[Vm]) -> Result<Vec<Lua>, LuaVmBusy> {
    let mut guard = LUA_VM.try_lock().ok_or(LuaVmBusy)?;
    if LUA_HOLD_DEPTH.load(Ordering::SeqCst) != 0 {
        return Err(LuaVmBusy);
    }
    let mut old = Vec::with_capacity(vms.len());
    for &vm in vms {
        vm.bytes().store(0, Ordering::Relaxed);
        vm.generation().fetch_add(1, Ordering::Release);
        old.extend(guard.of(vm).take());
    }
    Ok(old)
}

fn function_stats() -> Vec<u8> {
    let store = function_store().read();
    let lib_count = store.len() as i64;
    let fn_count: i64 = store.values().map(|l| l.functions.len() as i64).sum();
    drop(store);

    // The function that is running, if one is, and for how long it has been.
    let running = match RUNNING_FUNCTION.lock().clone() {
        Some((name, command)) if LUA_SCRIPT_BUSY_OTHERS.load(Ordering::Acquire) => {
            let duration = crate::replication::unix_ms()
                .saturating_sub(LUA_SCRIPT_STARTED_MS.load(Ordering::Relaxed));
            resp_array(vec![
                resp_bulk_str("name"),
                resp_bulk_str(&name),
                resp_bulk_str("command"),
                resp_array(command.iter().map(|arg| resp_bulk(arg)).collect()),
                resp_bulk_str("duration_ms"),
                resp_integer(duration as i64),
            ])
        }
        _ => resp_null_bulk(),
    };
    resp_array(vec![
        resp_bulk_str("running_script"),
        running,
        resp_bulk_str("engines"),
        // A map from engine to its counts, as a flat array.
        resp_array(vec![
            resp_bulk_str("LUA"),
            resp_array(vec![
                resp_bulk_str("libraries_count"),
                resp_integer(lib_count),
                resp_bulk_str("functions_count"),
                resp_integer(fn_count),
            ]),
        ]),
    ])
}

/// Binary blob written by [`function_dump_payload`] / read by [`decode_function_dump_payload`].
const FUNCTION_DUMP_MAGIC: &[u8] = b"KVFN";
const FUNCTION_DUMP_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy)]
enum FunctionRestorePolicy {
    /// Default — merge with existing libraries; abort on name collision (Redis default).
    Append,
    Replace,
    Flush,
}

fn write_blob_u32_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn read_u32_le(data: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 4 > data.len() {
        return None;
    }
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
    *pos += 4;
    Some(v)
}

fn read_blob_string(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32_le(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

fn sorted_function_libs(store: &HashMap<String, FunctionLibrary>) -> Vec<FunctionLibrary> {
    let mut names: Vec<String> = store.keys().cloned().collect();
    names.sort();
    names
        .into_iter()
        .filter_map(|n| store.get(&n).cloned())
        .collect()
}

fn same_fn_set(reg: &[String], stored: &[String]) -> bool {
    let mut a: Vec<&str> = reg.iter().map(String::as_str).collect();
    let mut b: Vec<&str> = stored.iter().map(String::as_str).collect();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

fn function_dump_payload() -> Vec<u8> {
    let store = function_store().read();
    let libs = sorted_function_libs(&store);
    drop(store);

    let mut buf = Vec::new();
    buf.extend_from_slice(FUNCTION_DUMP_MAGIC);
    buf.extend_from_slice(&FUNCTION_DUMP_FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&(libs.len() as u32).to_le_bytes());
    for lib in &libs {
        write_blob_u32_str(&mut buf, &lib.name);
        write_blob_u32_str(&mut buf, &lib.engine);
        write_blob_u32_str(&mut buf, &lib.body);
        let mut fnames = lib.functions.clone();
        fnames.sort();
        buf.extend_from_slice(&(fnames.len() as u32).to_le_bytes());
        for f in &fnames {
            write_blob_u32_str(&mut buf, f);
        }
    }

    let crc = crc64_digest(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

fn decode_kividb_function_dump_payload(data: &[u8]) -> Result<Vec<FunctionLibrary>, ()> {
    if data.len() < FUNCTION_DUMP_MAGIC.len() + 4 + 4 + 8 {
        return Err(());
    }
    let body_end = data.len() - 8;
    let crc_expected = u64::from_le_bytes(data[body_end..].try_into().map_err(|_| ())?);
    if crc64_digest(&data[..body_end]) != crc_expected {
        return Err(());
    }

    let mut pos = 0usize;
    if data.get(pos..pos + FUNCTION_DUMP_MAGIC.len()) != Some(FUNCTION_DUMP_MAGIC) {
        return Err(());
    }
    pos += FUNCTION_DUMP_MAGIC.len();

    let ver = read_u32_le(data, &mut pos).ok_or(())?;
    if ver != FUNCTION_DUMP_FORMAT_VERSION {
        return Err(());
    }

    let n = read_u32_le(data, &mut pos).ok_or(())? as usize;
    // `n` is the payload's word: each library takes some of its bytes.
    let mut out = Vec::with_capacity(n.min(data.len()));
    for _ in 0..n {
        let name = read_blob_string(data, &mut pos).ok_or(())?;
        let engine = read_blob_string(data, &mut pos).ok_or(())?;
        let body = read_blob_string(data, &mut pos).ok_or(())?;
        let nf = read_u32_le(data, &mut pos).ok_or(())? as usize;
        let mut functions = Vec::with_capacity(nf.min(data.len()));
        for _ in 0..nf {
            functions.push(read_blob_string(data, &mut pos).ok_or(())?);
        }
        out.push(FunctionLibrary {
            name,
            engine,
            body,
            functions,
            registered: Vec::new(),
        });
    }

    if pos != body_end {
        return Err(());
    }
    Ok(out)
}

/// Decode KiviDB's historical private format or Redis's RDB-based format.
/// The returned names identify Redis libraries whose function-name metadata
/// must be learned by running their source.
fn decode_function_dump_payload(
    data: &[u8],
) -> Result<(Vec<FunctionLibrary>, HashSet<String>), ()> {
    if data.starts_with(FUNCTION_DUMP_MAGIC) {
        return Ok((decode_kividb_function_dump_payload(data)?, HashSet::new()));
    }

    let mut libraries = Vec::new();
    let mut infer_functions = HashSet::new();
    for body in redis_function_dump_sources(data).ok_or(())? {
        let (engine, name) = parse_shebang(&body).map_err(|_| ())?;
        if !infer_functions.insert(name.clone()) {
            return Err(());
        }
        libraries.push(FunctionLibrary {
            name,
            engine,
            body,
            functions: Vec::new(),
            registered: Vec::new(),
        });
    }
    Ok((libraries, infer_functions))
}

fn compute_merged_store(
    incoming: Vec<FunctionLibrary>,
    policy: FunctionRestorePolicy,
) -> Result<HashMap<String, FunctionLibrary>, String> {
    let merged = merge_function_libraries(incoming, policy)?;
    // A function's name is its own across libraries: a payload that brings
    // one a library here already has is refused whole.
    let mut owners: HashMap<&str, &str> = HashMap::new();
    for lib in merged.values() {
        for function in &lib.functions {
            if owners.insert(function, &lib.name).is_some() {
                return Err(format!("ERR Function {function} already exists"));
            }
        }
    }
    Ok(merged)
}

fn merge_function_libraries(
    incoming: Vec<FunctionLibrary>,
    policy: FunctionRestorePolicy,
) -> Result<HashMap<String, FunctionLibrary>, String> {
    match policy {
        FunctionRestorePolicy::Flush => {
            let mut out = HashMap::new();
            for lib in incoming {
                out.insert(lib.name.clone(), lib);
            }
            Ok(out)
        }
        FunctionRestorePolicy::Append => {
            let mut current = function_store().read().clone();
            for lib in incoming {
                if current.contains_key(&lib.name) {
                    return Err(format!("ERR Library {} already exists", lib.name));
                }
                current.insert(lib.name.clone(), lib);
            }
            Ok(current)
        }
        FunctionRestorePolicy::Replace => {
            let mut current = function_store().read().clone();
            for lib in incoming {
                current.insert(lib.name.clone(), lib);
            }
            Ok(current)
        }
    }
}

fn rebuild_fn_name_map_from_store() {
    let mut m = fn_name_to_library().write();
    m.clear();
    let store = function_store().read();
    for lib in store.values() {
        for fname in &lib.functions {
            m.insert(fname.clone(), lib.name.clone());
        }
    }
}

/// Reload the Lua VM from [`function_store`] (must match what was just compiled).
fn persist_merged_and_sync_maps(merged: HashMap<String, FunctionLibrary>) {
    let mut w = function_store().write();
    w.clear();
    for (k, v) in merged {
        w.insert(k, v);
    }
    drop(w);
    rebuild_fn_name_map_from_store();
}

fn load_merged_into_vm(
    merged: &mut HashMap<String, FunctionLibrary>,
    infer_functions: &HashSet<String>,
) -> Result<(), String> {
    let libs = sorted_function_libs(merged);
    let loaded = try_with_lua(
        Vm::Functions,
        |lua| -> Result<Vec<(String, Vec<Registered>)>, String> {
            let mut loaded = Vec::new();
            let mut owners: HashMap<String, String> = HashMap::new();
            for lib in &libs {
                let registered = load_library_code(lua, &lib.body, None)?;
                let names: Vec<String> = registered.iter().map(|f| f.name.clone()).collect();
                if !infer_functions.contains(&lib.name) && !same_fn_set(&names, &lib.functions) {
                    discard_registered(lua, &registered);
                    return Err(
                        "ERR restored library metadata does not match registered functions".into(),
                    );
                }
                if let Some(name) = names
                    .iter()
                    .find(|name| owners.insert((*name).clone(), lib.name.clone()).is_some())
                {
                    discard_registered(lua, &registered);
                    return Err(format!("ERR Function {name} already exists"));
                }
                commit_registered(lua, &registered, &[]).map_err(|e| {
                    format!("ERR Error registering functions: {}", lua_error_message(&e))
                })?;
                loaded.push((lib.name.clone(), registered));
            }
            Ok(loaded)
        },
    );
    let loaded = match loaded {
        Err(LuaVmBusy) => {
            return Err(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."
                    .into(),
            );
        }
        Ok(Ok(loaded)) => loaded,
        Ok(Err(error)) => {
            // The store was not changed. Drop any functions committed before
            // a later library failed; the next call rebuilds the VM from it.
            let _ = lua_vms_renew_for_admin(&[Vm::Functions]);
            return Err(error);
        }
    };
    for (name, registered) in loaded {
        if let Some(lib) = merged.values_mut().find(|lib| lib.name == name) {
            lib.functions = registered
                .iter()
                .map(|function| function.name.clone())
                .collect();
            lib.registered = registered;
        }
    }
    Ok(())
}

fn function_dump() -> Vec<u8> {
    resp_bulk(&function_dump_payload())
}

fn function_restore(ctx: &mut Ctx) -> Vec<u8> {
    let cmd = &ctx.cmd;
    if cmd.len() < 3 || cmd.len() > 4 {
        return resp_error(
            "unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
        );
    }

    let policy = if cmd.len() == 4 {
        let m = match std::str::from_utf8(cmd[3]) {
            Ok(s) => s,
            Err(_) => {
                return resp_error(
                    "Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.",
                );
            }
        };
        if m.eq_ignore_ascii_case("APPEND") {
            FunctionRestorePolicy::Append
        } else if m.eq_ignore_ascii_case("REPLACE") {
            FunctionRestorePolicy::Replace
        } else if m.eq_ignore_ascii_case("FLUSH") {
            FunctionRestorePolicy::Flush
        } else {
            return resp_error(
                "Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.",
            );
        }
    } else {
        FunctionRestorePolicy::Append
    };

    match restore_function_payload(cmd[2], policy, "DUMP payload version or checksum are wrong") {
        Ok(()) => resp_simple("OK"),
        Err(e) => resp_error(&e),
    }
}

/// Load a `FUNCTION DUMP` payload under `policy`, as `FUNCTION RESTORE` does.
/// `unreadable` is the error for a payload that cannot be decoded.
fn restore_function_payload(
    payload: &[u8],
    policy: FunctionRestorePolicy,
    unreadable: &str,
) -> Result<(), String> {
    let (incoming, infer_functions) =
        decode_function_dump_payload(payload).map_err(|()| unreadable.to_string())?;
    let mut merged = compute_merged_store(incoming, policy)?;
    lua_vms_renew_for_admin(&[Vm::Functions])
        .map_err(|LuaVmBusy| BUSY_RUNNING_A_SCRIPT.to_string())?;
    load_merged_into_vm(&mut merged, &infer_functions)?;
    persist_merged_and_sync_maps(merged);
    Ok(())
}

/// Every library, as a `FUNCTION DUMP` payload, for a snapshot to keep.
/// `None` when no library is loaded, so that the snapshot is what it was
/// before it kept them.
pub(crate) fn library_snapshot() -> Option<Vec<u8>> {
    if function_store().read().is_empty() {
        return None;
    }
    Some(function_dump_payload())
}

const BUSY_RUNNING_A_SCRIPT: &str =
    "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.";

/// The libraries a snapshot kept, in place of those loaded, as Redis loads
/// the ones in an RDB file: a snapshot without any leaves none.
pub(crate) fn restore_library_snapshot(payload: Option<&[u8]>) -> Result<(), String> {
    match payload {
        // The server made this payload itself, so a decoding failure is in
        // a library's code, not in the version or checksum.
        Some(payload) => restore_function_payload(
            payload,
            FunctionRestorePolicy::Flush,
            "a library's code cannot be read: it must be UTF-8 and start with \
             `#!lua name=<library>`, and two libraries cannot share a name",
        ),
        None if function_store().read().is_empty() => Ok(()),
        None => {
            lua_vms_renew_for_admin(&[Vm::Functions])
                .map_err(|LuaVmBusy| BUSY_RUNNING_A_SCRIPT.to_string())?;
            persist_merged_and_sync_maps(HashMap::new());
            Ok(())
        }
    }
}

fn redis_function_dispatch(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'function' command");
    }
    let sub = String::from_utf8_lossy(ctx.cmd[1]).to_ascii_uppercase();
    match sub.as_str() {
        "LOAD" => function_load(ctx),
        "KILL" => kill_running(true),
        "DELETE" => function_delete(ctx),
        "LIST" => function_list(ctx),
        "FLUSH" => function_flush(ctx),
        "STATS" => function_stats(),
        "DUMP" => function_dump(),
        "RESTORE" => function_restore(ctx),
        "HELP" => crate::commands::server::help(
            "FUNCTION",
            &[
                (
                    "LOAD [REPLACE] <FUNCTION CODE>",
                    "Create a new library with the given library name and code.",
                ),
                ("DELETE <LIBRARY NAME>", "Delete the given library."),
                (
                    "LIST [LIBRARYNAME <pattern>] [WITHCODE]",
                    "Return general information on all the libraries.",
                ),
                (
                    "STATS",
                    "Return information about the current function running and the available execution engines.",
                ),
                ("KILL", "Kill the current running function."),
                ("FLUSH [ASYNC|SYNC]", "Delete all the libraries."),
                (
                    "DUMP",
                    "Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command.",
                ),
                (
                    "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
                    "Restore the libraries represented by the given payload.",
                ),
            ],
        ),
        _ => resp_error(&format!(
            "ERR unknown subcommand '{}'",
            String::from_utf8_lossy(ctx.cmd[1])
        )),
    }
}

fn script_cmd(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'script' command");
    }
    let sub = String::from_utf8_lossy(ctx.cmd[1]).to_ascii_uppercase();
    match sub.as_str() {
        "LOAD" => {
            if ctx.cmd.len() < 3 {
                return resp_error("ERR wrong number of arguments for 'script|load' command");
            }
            let source = match std::str::from_utf8(ctx.cmd[2]) {
                Ok(s) => s,
                Err(_) => return resp_error("ERR script is not valid UTF-8"),
            };
            let sha = sha1_hex(source);
            // Loaded again, it stays the copy that is already compiled.
            script_store()
                .write()
                .entry(sha.clone())
                .or_insert_with(|| Arc::from(source));
            eval_script_loaded(&sha);
            resp_bulk_str(&sha)
        }
        "EXISTS" => {
            let store = script_store().read();
            let mut parts: Vec<Vec<u8>> = Vec::new();
            for sha_bytes in ctx.cmd.iter().skip(2) {
                let sha = std::str::from_utf8(sha_bytes)
                    .map(|s| s.to_lowercase())
                    .unwrap_or_default();
                let n = if store.contains_key(&sha) { 1 } else { 0 };
                parts.push(resp_integer(n));
            }
            resp_array(parts)
        }
        "FLUSH" => {
            // `SCRIPT FLUSH [ASYNC|SYNC]`: the scripts go, and so does the VM
            // that compiled them, as Redis resets its scripting VM. (A script
            // running now keeps it, and it goes next time.) `ASYNC` frees
            // them in the background when there are more than 64, as Redis
            // does (`freeLuaScriptsAsync`), counting the scripts.
            let async_free = match ctx.cmd.get(2) {
                None => false,
                Some(word) if ctx.cmd.len() == 3 && word.eq_ignore_ascii_case(b"SYNC") => false,
                Some(word) if ctx.cmd.len() == 3 && word.eq_ignore_ascii_case(b"ASYNC") => true,
                Some(_) => return resp_error("ERR SCRIPT FLUSH only support SYNC|ASYNC option"),
            };
            let scripts = std::mem::take(&mut *script_store().write());
            *EVAL_SCRIPTS.lock() = EvalScripts::default();
            let vm = lua_vms_renew_for_admin(&[Vm::Eval]).unwrap_or_default();
            if async_free && scripts.len() > 64 {
                let n = scripts.len() as u64;
                crate::lazyfree::free_later((vm, scripts), n);
            }
            resp_simple("OK")
        }
        "RESET" => match lua_vms_renew_for_admin(&[Vm::Eval, Vm::Functions]).map(drop) {
            Ok(()) => resp_simple("OK"),
            Err(LuaVmBusy) => resp_error(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            ),
        },
        "KILL" => kill_running(false),
        "DEBUG" => resp_simple("OK"),
        _ => resp_error(&format!(
            "ERR unknown subcommand '{}' for SCRIPT",
            sub.to_lowercase()
        )),
    }
}

#[cfg(test)]
mod lua_busy_allow_tests {
    use super::*;

    #[test]
    fn redis_style_cmds_allowed_while_vm_busy_for_others() {
        assert!(lua_command_allowed_while_busy(&[b"FUNCTION", b"STATS"]));
        assert!(lua_command_allowed_while_busy(&[b"function", b"stats"]));
        assert!(lua_command_allowed_while_busy(&[b"FUNCTION", b"KILL"]));
        assert!(lua_command_allowed_while_busy(&[b"SCRIPT", b"KILL"]));
        assert!(!lua_command_allowed_while_busy(&[b"FUNCTION", b"LIST"]));
        assert!(!lua_command_allowed_while_busy(&[b"PING"]));
    }
}
