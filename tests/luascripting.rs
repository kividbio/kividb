// tests/lua_scripting.rs
// Requires --features lua. Skipped entirely when built without it.
// Run with: cargo test --features lua --test lua_scripting

#![cfg(feature = "lua")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// Lua function/script state is process-global; integration tests must not run in parallel.
static LUA_SCRIPTING_SERIAL: Mutex<()> = Mutex::new(());

// ── Minimal RESP client (same pattern as other test files) ────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..20)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let owned: Vec<Vec<u8>> = args.iter().map(|s| s.as_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        self.cmd(&refs)
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => RespValue::SimpleStr(line[1..].to_string()),
            Some(b'-') => RespValue::Error(line[1..].to_string()),
            Some(b':') => RespValue::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len == -1 {
                    return RespValue::Nil;
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                RespValue::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP: {:?}", line),
        }
    }
}

#[derive(Debug, PartialEq)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap(),
            _ => panic!("not a string: {:?}", self),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            _ => panic!("not an int: {:?}", self),
        }
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn as_error(&self) -> &str {
        match self {
            RespValue::Error(s) => s,
            _ => panic!("not an error: {:?}", self),
        }
    }
}

/// Whether nothing has `port`. It is asked with a socket that is bound and
/// never listens. One that listened, as this used to, could be given a
/// connection: a server another test is starting holds, between its fork and
/// its exec, a copy of every socket this process has open, and a client that
/// dialled the port in that moment was connected to the copy and then reset
/// ("Connection reset by peer", at the first command of a test).
fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    // As the server's own socket, so that a copy of this one, while it
    // lasts, does not keep the server from the port.
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

/// A port nothing is listening on — and not one the system hands out to
/// outgoing connections. A client that starts dialling before its server is
/// listening can otherwise be given, as its own end, the very port it is
/// dialling: it is then connected to itself and reads back what it writes.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(31);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn start_server(port: u16) {
    thread::spawn(move || {
        let cfg = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(cfg);
    });
    thread::sleep(Duration::from_millis(150));
}

// ── SELECT isolation ──────────────────────────────────────────────────────────

/// SELECT inside EVAL must not affect the caller's DB after the script exits.
/// This is the Redis contract: scripts run in an isolated DB context.
#[test]
fn test_eval_select_does_not_leak_db() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Write a key on DB 0
    c.cmds(&["SET", "sentinel", "db0_value"]);

    // Script selects DB 9 and writes there
    c.cmds(&[
        "EVAL",
        "redis.call('SELECT', 9); redis.call('SET', 'sentinel', 'db9_value')",
        "0",
    ]);

    // Caller must still be on DB 0
    let val = c.cmds(&["GET", "sentinel"]);
    assert_eq!(
        val.as_str(),
        "db0_value",
        "SELECT inside EVAL leaked to caller: got {:?}",
        val
    );

    // Verify the write actually went to DB 9
    c.cmds(&["SELECT", "9"]);
    let db9_val = c.cmds(&["GET", "sentinel"]);
    assert_eq!(db9_val.as_str(), "db9_value");
}

/// FCALL must also sandbox SELECT.
#[test]
fn test_fcall_select_does_not_leak_db() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "sentinel", "db0_value"]);

    c.cmds(&[
        "FUNCTION", "LOAD",
        "#!lua name=dbtest\nredis.register_function('selecttest', function(KEYS, ARGV)\n  redis.call('SELECT', 9)\n  redis.call('SET', 'sentinel', 'db9_value')\n  return 1\nend)",
    ]);
    c.cmds(&["FCALL", "selecttest", "0"]);

    // Caller stays on DB 0
    let val = c.cmds(&["GET", "sentinel"]);
    assert_eq!(val.as_str(), "db0_value");
}

// ── Nil bulk → Lua false ──────────────────────────────────────────────────────

/// When a Redis command returns nil (key not found), Lua must see false, not nil.
/// redis.call('GET', 'nosuch') → Lua false → RESP integer 0 when returned.
#[test]
fn test_nil_bulk_becomes_lua_false() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Script returns the result of GET on a missing key.
    // Lua false → RESP nil bulk (not integer 0) per Redis convention.
    let result = c.cmds(&["EVAL", "return redis.call('GET', 'nosuchkey')", "0"]);

    // Redis returns nil bulk for Lua false
    assert!(
        result.is_nil(),
        "Lua false from nil bulk reply should return RESP nil, got {:?}",
        result
    );
}

/// Every status reply reaches Lua as `{ok = ...}`, not only `+OK`: BullMQ's
/// paginate and removeOrphanedJobs scripts read `redis.call('TYPE', k)['ok']`.
#[test]
fn test_status_replies_are_ok_tables() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "a_set", "m"]);
    let script = "return {type(redis.call('PING')), redis.call('PING')['ok'], \
                  redis.call('TYPE', 'a_set')['ok'], redis.call('TYPE', 'nosuch')['ok'], \
                  redis.call('SET', 'k', 'v')['ok']}";
    let RespValue::Array(items) = c.cmds(&["EVAL", script, "0"]) else {
        panic!("expected an array");
    };
    let got: Vec<&str> = items.iter().map(|v| v.as_str()).collect();
    assert_eq!(got, ["table", "PONG", "set", "none", "OK"]);

    // And a status reply returned as is goes back to the client as one.
    let reply = c.cmds(&["EVAL", "return redis.call('TYPE', 'a_set')", "0"]);
    assert!(
        matches!(reply, RespValue::SimpleStr(ref s) if s == "set"),
        "{reply:?}"
    );
}

/// pcall on a missing key: result['err'] is not set, result is false (nil bulk).
#[test]
fn test_pcall_nil_result_not_error() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Using HGET on missing key — returns nil, not an error
    let result = c.cmds(&[
        "EVAL",
        "local r = redis.call('GET', 'nosuchkey'); if r == false then return 1 else return 0 end",
        "0",
    ]);

    assert_eq!(
        result.as_int(),
        1,
        "Lua should see nil bulk as false, so the if-branch should return 1, got {:?}",
        result
    );
}

// ── redis.call error propagation ──────────────────────────────────────────────

/// redis.call on a command that errors must raise a Lua error (not return a table).
/// The error propagates out of the script as a RESP error reply.
#[test]
fn test_redis_call_propagates_error() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "notanint", "abc"]);

    // INCR on a non-integer — redis.call raises, script aborts with error
    let result = c.cmds(&["EVAL", "return redis.call('INCR', 'notanint')", "0"]);

    assert!(
        result.is_error(),
        "redis.call error should propagate as RESP error, got {:?}",
        result
    );
    assert!(
        result.as_error().contains("not an integer") || result.as_error().contains("integer"),
        "error message should mention integer, got: {}",
        result.as_error()
    );
}

/// redis.pcall on a command that errors must return {err = "..."} table.
/// The script itself returns OK (pcall catches the error).
#[test]
fn test_redis_pcall_catches_error() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "notanint", "abc"]);

    // pcall catches the error; script returns the err string
    let result = c.cmds(&[
        "EVAL",
        "local r = redis.pcall('INCR', 'notanint'); return r['err']",
        "0",
    ]);

    // Should get the error message as a bulk string
    assert!(
        !result.is_nil() && !result.is_error(),
        "pcall should catch error and return err field, got {:?}",
        result
    );
    let msg = result.as_str();
    assert!(
        msg.contains("integer") || msg.contains("not an integer"),
        "err field should describe integer error, got: {msg}"
    );
}

// ── Status replies inside Lua ─────────────────────────────────────────────────

/// Redis converts every status reply to a `{ok = <status>}` table inside Lua,
/// not only `+OK`. `redis.call('TYPE', k)['ok']` is a common idiom (BullMQ's
/// pagination and orphan-cleanup scripts use it); a bare string makes it nil.
/// Returning the table from the script must still produce a status reply.
#[test]
fn test_every_status_reply_is_an_ok_table() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s", "a"]);
    c.cmds(&["SET", "str", "v"]);
    let probe = r#"
local out = {}
for _, r in ipairs({redis.call('TYPE', KEYS[1]), redis.call('TYPE', KEYS[2]),
                    redis.call('TYPE', 'missing'), redis.call('PING'),
                    redis.call('SET', 'k', 'v')}) do
    out[#out + 1] = type(r) .. ':' .. tostring(type(r) == 'table' and r['ok'] or r)
end
return table.concat(out, ',')
"#;
    assert_eq!(
        c.cmds(&["EVAL", probe, "2", "s", "str"]).as_str(),
        "table:set,table:string,table:none,table:PONG,table:OK"
    );

    // The table round-trips back to the client as a status reply.
    assert_eq!(
        c.cmds(&["EVAL", "return redis.call('PING')", "0"]).as_str(),
        "PONG"
    );
    assert_eq!(
        c.cmds(&["EVAL", "return redis.call('TYPE', KEYS[1])", "1", "s"])
            .as_str(),
        "set"
    );
}

// ── Metamethod redis.call ─────────────────────────────────────────────────────

/// A table with a __index metamethod that calls redis.call must work when the
/// script evaluates `t.val` — metamethods run during chunk execution, not during
/// RESP encoding (Redis uses rawget for array/status/error slots).
#[test]
fn test_metamethod_can_call_redis() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "counter", "0"]);

    let result = c.cmds(&[
        "EVAL",
        r#"
local mt = {}
mt.__index = function(t, k)
    if k == 'val' then
        return redis.call('INCR', 'counter')
    end
end
local t = setmetatable({}, mt)
return t.val
"#,
        "0",
    ]);

    assert_eq!(
        result.as_int(),
        1,
        "metamethod should call redis.call and return INCR result, got {:?}",
        result
    );

    // Counter was incremented
    assert_eq!(c.cmds(&["GET", "counter"]).as_str(), "1");
}

// ── FUNCTION LOAD + FCALL basic contract ─────────────────────────────────────

#[test]
fn test_function_load_and_fcall() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Libraries belong to the process, and other tests in this file load one
    // of the same name: start from none, whichever of them ran first.
    c.cmds(&["FUNCTION", "FLUSH"]);
    let load = c.cmds(&[
        "FUNCTION", "LOAD",
        "#!lua name=mylib\nredis.register_function('myfn', function(KEYS, ARGV)\n  return ARGV[1]\nend)",
    ]);
    assert_eq!(load.as_str(), "mylib");

    let result = c.cmds(&["FCALL", "myfn", "0", "hello"]);
    assert_eq!(result.as_str(), "hello");
}

#[test]
fn redis_function_dump_payloads_restore_and_run() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for (version, payload) in [
        (
            "7.4",
            &include_bytes!("fixtures/redis-7.4-function-dump.bin")[..],
        ),
        (
            "8.10",
            &include_bytes!("fixtures/redis-8.10-function-dump.bin")[..],
        ),
    ] {
        let restored = c.cmd(&[b"FUNCTION", b"RESTORE", payload, b"FLUSH"]);
        assert_eq!(restored.as_str(), "OK", "Redis {version} payload");
        assert_eq!(c.cmds(&["FCALL", "hello", "0"]).as_str(), "world");
    }
}

/// As Redis 7.4: past 500 scripts sent only with `EVAL`, the least recently
/// used goes, counted in `evicted_scripts`; a `SCRIPT LOAD`ed one stays. The
/// cache grew for ever (#155).
#[test]
fn the_script_cache_keeps_500_scripts_sent_with_eval() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let stat = |c: &mut Conn, section: &str, name: &str| -> i64 {
        let info = c.cmds(&["INFO", section]);
        info.as_str()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("no {name}"))
            .trim()
            .parse()
            .unwrap()
    };
    assert_eq!(c.cmds(&["SCRIPT", "FLUSH"]).as_str(), "OK");
    let evicted_before = stat(&mut c, "stats", "evicted_scripts");
    let loaded = c.cmds(&["SCRIPT", "LOAD", "return 'loaded'"]);
    let loaded = loaded.as_str().to_string();
    let first = c.cmds(&["SCRIPT", "LOAD", "return 0"]).as_str().to_string();
    c.cmds(&["EVAL", "return 0", "0"]);
    for i in 1..=600 {
        c.cmds(&["EVAL", &format!("return {i}"), "0"]);
        if i == 300 {
            // Used again: the most recent, and kept.
            c.cmds(&["EVAL", "return 1", "0"]);
        }
    }
    c.cmds(&["EVAL", "return 1", "0"]);
    assert_eq!(stat(&mut c, "memory", "number_of_cached_scripts"), 502);
    assert_eq!(
        stat(&mut c, "stats", "evicted_scripts") - evicted_before,
        100
    );
    let exists = c.cmds(&[
        "SCRIPT",
        "EXISTS",
        &loaded,
        &first,
        // `return 1`, `return 2`
        "e0e1f9fabfc9d4800c877a703b823ac0578ff8db",
        "7f923f79fe76194c868d7e1d0820de36700eb649",
    ]);
    let RespValue::Array(exists) = exists else {
        panic!("{exists:?}")
    };
    let exists: Vec<i64> = exists.iter().map(|v| v.as_int()).collect();
    // The loaded ones stay (`return 0` was loaded before EVAL sent it); the
    // one used again stays; one of the oldest went.
    assert_eq!(exists, [1, 1, 1, 0], "{exists:?}");
}

/// Redis's `struct` library: `pack`, `unpack` and `size`, with its formats.
/// It was missing, and a script that used it failed (#154).
#[test]
fn the_struct_library_packs_and_unpacks_as_redis_s() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let eval = |c: &mut Conn, script: &str| c.cmds(&["EVAL", script, "0"]);
    assert_eq!(
        eval(&mut c, "return struct.pack('>I', 1)"),
        RespValue::Bulk(vec![0, 0, 0, 1])
    );
    assert_eq!(
        eval(&mut c, "return struct.pack('<HH', 1, 2)"),
        RespValue::Bulk(vec![1, 0, 2, 0])
    );
    let ints =
        |values: &[i64]| RespValue::Array(values.iter().map(|v| RespValue::Int(*v)).collect());
    assert_eq!(
        eval(
            &mut c,
            "return {struct.unpack('<HH', struct.pack('<HH', 1, 2))}"
        ),
        ints(&[1, 2, 5])
    );
    assert_eq!(
        eval(&mut c, "return struct.size('>iHd')"),
        RespValue::Int(14)
    );
    assert_eq!(
        eval(&mut c, "return struct.size('lLT')"),
        RespValue::Int(24)
    );
    assert_eq!(
        eval(&mut c, "return struct.pack('s', 'ab')"),
        RespValue::Bulk(b"ab\0".to_vec())
    );
    assert_eq!(
        eval(
            &mut c,
            "local n, s, at = struct.unpack('Bc0', struct.pack('Bc0', 5, 'hello')) \
             return {n, s, at}"
        ),
        RespValue::Array(vec![
            RespValue::Int(5),
            RespValue::Bulk(b"hello".to_vec()),
            RespValue::Int(7)
        ])
    );
    // A number given for an integer is cut to one, as Redis's struct does.
    assert_eq!(
        eval(&mut c, "return struct.pack('>i', 2.7)"),
        RespValue::Bulk(vec![0, 0, 0, 2])
    );
    assert!(matches!(
        eval(&mut c, "return struct.pack('q', 1)"),
        RespValue::Error(_)
    ));
}

#[test]
fn a_redis_function_dump_with_two_libraries_restores_both() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let payload = &include_bytes!("fixtures/redis-7.4-two-library-function-dump.bin")[..];

    assert_eq!(
        c.cmd(&[b"FUNCTION", b"RESTORE", payload, b"FLUSH"])
            .as_str(),
        "OK"
    );
    assert_eq!(c.cmds(&["FCALL", "first", "0"]).as_str(), "one");
    assert_eq!(c.cmds(&["FCALL", "second", "0"]).as_str(), "two");
}

#[test]
fn a_bad_redis_function_dump_payload_leaves_existing_functions_alone() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["FUNCTION", "FLUSH"]);
    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=existing\nredis.register_function('old', function() return 'here' end)",
    ]);
    let mut payload = include_bytes!("fixtures/redis-7.4-function-dump.bin").to_vec();
    payload[10] ^= 1;

    assert_eq!(
        c.cmd(&[b"FUNCTION", b"RESTORE", &payload, b"FLUSH"])
            .as_error(),
        "DUMP payload version or checksum are wrong"
    );
    assert_eq!(c.cmds(&["FCALL", "old", "0"]).as_str(), "here");
}

#[test]
fn kividb_function_dump_payload_still_round_trips() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["FUNCTION", "FLUSH"]);
    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=ownformat\nredis.register_function('own', function() return 'yes' end)",
    ]);
    let payload = match c.cmds(&["FUNCTION", "DUMP"]) {
        RespValue::Bulk(payload) => payload,
        other => panic!("not a dump payload: {other:?}"),
    };
    assert_eq!(
        c.cmd(&[b"FUNCTION", b"RESTORE", &payload, b"FLUSH"])
            .as_str(),
        "OK"
    );
    assert_eq!(c.cmds(&["FCALL", "own", "0"]).as_str(), "yes");
}

#[test]
fn redis_function_restore_refuses_an_existing_function_name_atomically() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["FUNCTION", "FLUSH"]);
    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=existing\nredis.register_function('hello', function() return 'old' end)",
    ]);
    let payload = &include_bytes!("fixtures/redis-7.4-function-dump.bin")[..];
    let restored = c.cmd(&[b"FUNCTION", b"RESTORE", payload]);
    assert!(
        restored
            .as_error()
            .contains("Function hello already exists")
    );
    assert_eq!(c.cmds(&["FCALL", "hello", "0"]).as_str(), "old");
}

#[test]
fn test_function_load_replace() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=mylib\nredis.register_function('ver', function(KEYS, ARGV) return 'v1' end)",
    ]);

    // REPLACE should succeed
    let result = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "REPLACE",
        "#!lua name=mylib\nredis.register_function('ver', function(KEYS, ARGV) return 'v2' end)",
    ]);
    assert_eq!(result.as_str(), "mylib");

    assert_eq!(c.cmds(&["FCALL", "ver", "0"]).as_str(), "v2");
}

#[test]
fn test_function_load_duplicate_without_replace_errors() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=duplib\nredis.register_function('f', function(KEYS, ARGV) return 1 end)",
    ]);

    let result = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=duplib\nredis.register_function('f', function(KEYS, ARGV) return 2 end)",
    ]);
    assert!(
        result.is_error(),
        "duplicate LOAD without REPLACE should error"
    );
}

#[test]
fn test_function_flush() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=flushtest\nredis.register_function('ff', function(KEYS, ARGV) return 1 end)",
    ]);
    assert_eq!(c.cmds(&["FCALL", "ff", "0"]).as_int(), 1);

    c.cmds(&["FUNCTION", "FLUSH"]);

    let result = c.cmds(&["FCALL", "ff", "0"]);
    assert!(result.is_error(), "FCALL after FUNCTION FLUSH should error");
}

#[test]
fn test_function_keys_and_argv() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "mykey", "myvalue"]);

    c.cmds(&[
        "FUNCTION", "LOAD",
        "#!lua name=klib\nredis.register_function('getkey', function(KEYS, ARGV)\n  return redis.call('GET', KEYS[1])\nend)",
    ]);

    let result = c.cmds(&["FCALL", "getkey", "1", "mykey"]);
    assert_eq!(result.as_str(), "myvalue");
}

// ── SCRIPT LOAD + EVALSHA ─────────────────────────────────────────────────────

#[test]
fn test_script_load_and_evalsha() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let sha = c.cmds(&["SCRIPT", "LOAD", "return 'loaded'"]);
    assert_eq!(sha.as_str().len(), 40, "SHA1 should be 40 hex chars");

    let result = c.cmds(&["EVALSHA", sha.as_str(), "0"]);
    assert_eq!(result.as_str(), "loaded");
}

#[test]
fn test_evalsha_noscript_error() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["EVALSHA", "0000000000000000000000000000000000000000", "0"]);
    assert!(result.is_error());
    assert!(
        result.as_error().starts_with("NOSCRIPT"),
        "should return NOSCRIPT error, got: {}",
        result.as_error()
    );
}

#[test]
fn test_script_exists() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let sha = c.cmds(&["SCRIPT", "LOAD", "return 1"]).as_str().to_string();

    let result = c.cmds(&[
        "SCRIPT",
        "EXISTS",
        sha.as_str(),
        "0000000000000000000000000000000000000000",
    ]);
    let arr = result;
    match &arr {
        RespValue::Array(items) => {
            assert_eq!(items[0].as_int(), 1, "loaded script should exist");
            assert_eq!(items[1].as_int(), 0, "fake sha should not exist");
        }
        _ => panic!("SCRIPT EXISTS should return array, got {:?}", arr),
    }
}

// ── cjson ─────────────────────────────────────────────────────────────────────

#[test]
fn test_cjson_encode_decode_roundtrip() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&[
        "EVAL",
        r#"
local obj = {name = "kivi", version = 1}
local encoded = cjson.encode(obj)
local decoded = cjson.decode(encoded)
return decoded['name']
"#,
        "0",
    ]);
    assert_eq!(result.as_str(), "kivi");
}

#[test]
fn test_cjson_decode_array() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&[
        "EVAL",
        r#"local t = cjson.decode('[1,2,3]'); return t[2]"#,
        "0",
    ]);
    assert_eq!(result.as_int(), 2);
}

// ── redis.sha1hex ─────────────────────────────────────────────────────────────

#[test]
fn test_redis_sha1hex() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Known SHA1 of empty string
    let result = c.cmds(&["EVAL", r#"return redis.sha1hex('')"#, "0"]);
    assert_eq!(
        result.as_str(),
        "da39a3ee5e6b4b0d3255bfef95601890afd80709",
        "SHA1 of empty string mismatch"
    );
}

// ── EVAL_RO blocks writes ─────────────────────────────────────────────────────

#[test]
fn test_eval_ro_blocks_write_commands() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["EVAL_RO", "return redis.call('SET', 'x', '1')", "0"]);
    assert!(
        result.is_error(),
        "EVAL_RO should block SET, got {:?}",
        result
    );
}

#[test]
fn test_eval_ro_allows_read_commands() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "ro_key", "ro_val"]);

    let result = c.cmds(&["EVAL_RO", "return redis.call('GET', 'ro_key')", "0"]);
    assert_eq!(result.as_str(), "ro_val");
}

/// A write made from a script leaves the keyspace as the same write from a
/// client would: the key it empties is deleted — and the script itself sees
/// that — and a client blocked on a key it pushes to is woken.
#[test]
fn test_script_writes_settle_the_keys_they_touch() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let script = "redis.call('RPUSH', KEYS[1], '1'); \
                  redis.call('LPOP', KEYS[1]); \
                  return redis.call('EXISTS', KEYS[1])";
    assert_eq!(c.cmds(&["EVAL", script, "1", "emptied"]).as_int(), 0);
    assert_eq!(c.cmds(&["DBSIZE"]).as_int(), 0);

    // A client blocked in BLPOP: the command is sent and not yet answered.
    let mut waiter = Conn::connect(port);
    waiter
        .stream
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nqueued\r\n$1\r\n5\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(150));
    let pushed = c.cmds(&[
        "EVAL",
        "return redis.call('RPUSH', KEYS[1], 'job')",
        "1",
        "queued",
    ]);
    assert_eq!(pushed.as_int(), 1);
    assert_eq!(
        waiter.read_value(),
        RespValue::Array(vec![
            RespValue::Bulk(b"queued".to_vec()),
            RespValue::Bulk(b"job".to_vec()),
        ])
    );
    assert_eq!(c.cmds(&["EXISTS", "queued"]).as_int(), 0);
}

// ── FUNCTION LOAD: all of a library, or none of it ───────────────────────────

/// A load that fails leaves the library it was to replace as it was — its
/// functions too, including one the failed library had already registered
/// under the same name.
#[test]
fn test_function_load_that_fails_changes_nothing() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["FUNCTION", "FLUSH"]);

    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=keeps\nredis.register_function('kept', function() return 'old' end)",
    ]);
    let failed = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "REPLACE",
        "#!lua name=keeps\n\
         redis.register_function('kept', function() return 'new' end)\n\
         redis.register_function('other', 'not a function')",
    ]);
    assert!(
        failed
            .as_error()
            .contains("second argument to redis.register_function must be a function"),
        "{failed:?}"
    );
    assert_eq!(c.cmds(&["FCALL", "kept", "0"]).as_str(), "old");
    assert!(c.cmds(&["FCALL", "other", "0"]).is_error());
}

/// An error is one line, whatever the Lua error said: a message with a line
/// break in it used to end the reply early, and the client read the rest of
/// it as the reply to its next command.
#[test]
fn test_function_load_error_is_one_line() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["FUNCTION", "FLUSH"]);

    let failed = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=breaks\nerror('first line\nsecond line')",
    ]);
    assert!(failed.is_error(), "{failed:?}");
    assert!(!failed.as_error().contains('\n'), "{failed:?}");
    // And the connection is where it should be.
    assert_eq!(c.cmds(&["PING"]).as_str(), "PONG");
    assert_eq!(c.cmds(&["ECHO", "still here"]).as_str(), "still here");
}

/// A function's name belongs to one library.
#[test]
fn test_function_name_taken_by_another_library() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["FUNCTION", "FLUSH"]);

    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=first\nredis.register_function('shared', function() return 1 end)",
    ]);
    let taken = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=second\nredis.register_function('shared', function() return 2 end)",
    ]);
    assert!(
        taken.as_error().contains("Function shared already exists"),
        "{taken:?}"
    );
    assert_eq!(c.cmds(&["FCALL", "shared", "0"]).as_int(), 1);

    let twice = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=third\n\
         redis.register_function('twice', function() return 1 end)\n\
         redis.register_function('twice', function() return 2 end)",
    ]);
    assert!(
        twice
            .as_error()
            .contains("Function already exists in the library"),
        "{twice:?}"
    );
    assert!(c.cmds(&["FCALL", "twice", "0"]).is_error());
}

/// While it loads, a library has `redis.register_function` and little else:
/// not the dataset, not the other libraries, and no globals to leave behind.
#[test]
fn test_function_load_runs_with_nothing_in_reach() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["FUNCTION", "FLUSH"]);

    for (code, said) in [
        (
            "return redis.call('ping')",
            "nonexistent global variable 'call'",
        ),
        ("return math.random()", "nonexistent global variable 'math'"),
        ("a = 1", "Attempt to modify a readonly table"),
        ("return 1", "No functions registered"),
    ] {
        let reply = c.cmds(&["FUNCTION", "LOAD", &format!("#!lua name=bare\n{code}")]);
        assert!(reply.as_error().contains(said), "{code}: {reply:?}");
    }
    // What the functions are called with is everything a script has.
    let loaded = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=bare\n\
         redis.register_function('uses', function(keys)\n\
           redis.call('SET', keys[1], math.floor(2.5))\n\
           return redis.call('GET', keys[1])\n\
         end)",
    ]);
    assert_eq!(loaded.as_str(), "bare");
    assert_eq!(c.cmds(&["FCALL", "uses", "1", "fn:k"]).as_str(), "2");
    // But not `register_function`: that is for loading.
    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=late\n\
         redis.register_function('registers', function()\n\
           redis.register_function('more', function() return 1 end)\n\
         end)",
    ]);
    assert!(c.cmds(&["FCALL", "registers", "0"]).is_error());
    assert!(c.cmds(&["FCALL", "more", "0"]).is_error());
}

/// The flags a function is registered with are listed, and are kept to.
#[test]
fn test_function_flags() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["FUNCTION", "FLUSH"]);

    let loaded = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=flagged\n\
         redis.register_function{function_name='reads', description='only reads',\n\
           callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}\n\
         redis.register_function{function_name='lies',\n\
           callback=function(keys) return redis.call('SET', keys[1], 1) end, flags={'no-writes'}}\n\
         redis.register_function('writes', function(keys) return redis.call('SET', keys[1], 'v') end)",
    ]);
    assert_eq!(loaded.as_str(), "flagged");

    // Every string in the reply, whatever it is nested in.
    fn strings(value: &RespValue, into: &mut Vec<String>) {
        match value {
            RespValue::Bulk(bytes) => into.push(String::from_utf8_lossy(bytes).into_owned()),
            RespValue::Array(items) => items.iter().for_each(|item| strings(item, into)),
            _ => {}
        }
    }
    let mut list = Vec::new();
    strings(&c.cmds(&["FUNCTION", "LIST"]), &mut list);
    assert!(list.iter().any(|s| s == "only reads"), "{list:?}");
    assert!(list.iter().any(|s| s == "no-writes"), "{list:?}");

    assert_eq!(c.cmds(&["FCALL", "writes", "1", "fl:k"]).as_str(), "OK");
    assert_eq!(c.cmds(&["FCALL_RO", "reads", "1", "fl:k"]).as_str(), "v");
    // A function that may write cannot be called as one that does not…
    let refused = c.cmds(&["FCALL_RO", "writes", "1", "fl:k"]);
    assert!(refused.as_error().contains("write flag"), "{refused:?}");
    // …and one that said it does not write, does not.
    let refused = c.cmds(&["FCALL", "lies", "1", "fl:k"]);
    assert!(
        refused
            .as_error()
            .contains("Write commands are not allowed from read-only scripts"),
        "{refused:?}"
    );
    assert_eq!(c.cmds(&["GET", "fl:k"]).as_str(), "v");

    let unknown = c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=badflag\n\
         redis.register_function{function_name='f', callback=function() end, flags={'sometimes'}}",
    ]);
    assert!(
        unknown.as_error().contains("unknown flag given"),
        "{unknown:?}"
    );
}

/// Quick scripts take turns: the first of several runs where it arrived, and
/// the rest are run one after another by the script-runner thread. Whichever
/// way each call goes, a script is one step — a read-then-write in Lua loses
/// no update — and every caller gets its own reply.
#[test]
fn test_scripts_from_many_clients_at_once_run_one_at_a_time() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    Conn::connect(port).cmds(&["SET", "tally", "0"]);

    // Not INCR: a GET and a SET, which only add up if nothing runs between.
    const SCRIPT: &str = "local n = tonumber(redis.call('GET', KEYS[1])) + 1 \
                          redis.call('SET', KEYS[1], n) \
                          redis.call('RPUSH', KEYS[2], ARGV[1]) \
                          return {n, ARGV[1]}";
    const CLIENTS: usize = 16;
    const CALLS: usize = 250;
    let handles: Vec<_> = (0..CLIENTS)
        .map(|client| {
            thread::spawn(move || {
                let mut c = Conn::connect(port);
                let mut seen = Vec::with_capacity(CALLS);
                for call in 0..CALLS {
                    let tag = format!("{client}:{call}");
                    let reply = c.cmds(&["EVAL", SCRIPT, "2", "tally", "order", &tag]);
                    let RespValue::Array(items) = reply else {
                        panic!("client {client}: {reply:?}");
                    };
                    // Its own argument back, not another caller's.
                    assert_eq!(items[1].as_str(), tag);
                    seen.push(items[0].as_int());
                }
                seen
            })
        })
        .collect();
    let mut counts: Vec<i64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    counts.sort_unstable();

    // Every count from 1 to the total, once: no update lost, none repeated.
    let total = (CLIENTS * CALLS) as i64;
    assert_eq!(counts, (1..=total).collect::<Vec<_>>());
    let mut c = Conn::connect(port);
    assert_eq!(c.cmds(&["GET", "tally"]).as_str(), total.to_string());
    assert_eq!(c.cmds(&["LLEN", "order"]).as_int(), total);
}

/// A time to live is told in full by the script that set it, and outside a
/// script is never told as more than a millisecond short. It used to be
/// told as 249 of 250 every time: the clock it was set by dropped the part
/// of a millisecond that the clock it was told by kept.
#[test]
fn a_time_to_live_is_told_in_full_by_the_script_that_set_it() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for i in 0..50 {
        let key = format!("k{i}");
        let told = c.cmds(&[
            "EVAL",
            "redis.call('set', KEYS[1], 'v', 'PX', 250) return redis.call('pttl', KEYS[1])",
            "1",
            &key,
        ]);
        assert_eq!(told.as_int(), 250, "SET PX, round {i}");
        let told = c.cmds(&[
            "EVAL",
            "redis.call('pexpire', KEYS[1], 400) return redis.call('pttl', KEYS[1])",
            "1",
            &key,
        ]);
        assert_eq!(told.as_int(), 400, "PEXPIRE, round {i}");
        c.cmds(&["SET", &key, "v", "PX", "250"]);
        let told = c.cmds(&["PTTL", &key]).as_int();
        assert!(
            (249..=250).contains(&told),
            "next command, round {i}: {told}"
        );
    }
}

/// A key is there for at least as long as it was given.
#[test]
fn a_key_is_not_gone_before_its_time() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for i in 0..8 {
        let key = format!("k{i}");
        // Timed from before the command is sent: the server set it later.
        let sent = std::time::Instant::now();
        c.cmds(&["SET", &key, "v", "PX", "120"]);
        loop {
            let there = !c.cmds(&["GET", &key]).is_nil();
            let after = sent.elapsed();
            if !there {
                assert!(
                    after >= std::time::Duration::from_millis(120),
                    "gone after {after:?}, round {i}"
                );
                break;
            }
            assert!(
                after < std::time::Duration::from_secs(2),
                "never gone, round {i}"
            );
        }
    }
}

/// Nothing in a script reaches outside the sandbox: no `package` (whose
/// `loaded` table held the real `io` and `os`, and so `os.execute` and
/// `io.popen`), no `require`, `loadfile`, `dofile` or `debug`, and no
/// bytecode — `string.dump` is gone and `load` takes source text only, as in
/// Redis.
#[test]
fn a_script_cannot_reach_outside_the_sandbox() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    // Gone, and with the globals protected, not even there to read.
    for name in ["package", "require", "loadfile", "dofile", "debug", "io"] {
        let script = format!("return type({name})");
        let reply = c.cmds(&["EVAL", &script, "0"]);
        assert!(reply.is_error(), "{name}");
    }
    assert_eq!(
        c.cmds(&["EVAL", "return type(string.dump)", "0"]).as_str(),
        "nil"
    );
    assert_eq!(
        c.cmds(&["EVAL", "return type(os.execute)", "0"]).as_str(),
        "function",
        "a stub that refuses, as Redis has"
    );
    assert!(
        c.cmds(&["EVAL", "return os.execute('true')", "0"])
            .is_error()
    );
    // Source text still loads; bytecode does not.
    assert!(matches!(
        c.cmds(&["EVAL", "return load('return 7')()", "0"]),
        RespValue::Int(7)
    ));
    let bytecode = c.cmds(&[
        "EVAL",
        r"local f, err = load('\27Lua') return tostring(err)",
        "0",
    ]);
    assert!(
        bytecode.as_str().contains("binary"),
        "{:?}",
        bytecode.as_str()
    );
}

/// A function that has shown itself quick runs on the worker that read it.
/// Redefined to run for ever, it held that worker; and when that worker had
/// last been the one listening on the sockets, nobody listened any more:
/// a new connection was not even accepted, and the function could not be
/// killed. Other connections must be answered, `BUSY`, and `FUNCTION KILL`
/// must end it.
#[test]
fn a_quick_function_that_turns_endless_does_not_stop_the_server() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let quick = "#!lua name=turnslib\nredis.register_function('turns', function() return 1 end)";
    assert!(!c.cmds(&["FUNCTION", "LOAD", "REPLACE", quick]).is_error());
    for _ in 0..3 {
        assert_eq!(c.cmds(&["FCALL", "turns", "0"]).as_int(), 1);
    }
    let endless =
        "#!lua name=turnslib\nredis.register_function('turns', function() while true do end end)";
    assert!(!c.cmds(&["FUNCTION", "LOAD", "REPLACE", endless]).is_error());
    c.cmds(&["CONFIG", "SET", "lua-time-limit", "10"]);

    let mut runner = Conn::connect(port);
    runner
        .stream
        .write_all(b"*3\r\n$5\r\nFCALL\r\n$5\r\nturns\r\n$1\r\n0\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(300));

    // Several new connections, so that some land on other workers and some
    // on the one running the function.
    for _ in 0..6 {
        let mut other = Conn::connect(port);
        let reply = other.cmds(&["PING"]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e.starts_with("BUSY")),
            "{reply:?}"
        );
    }
    assert_eq!(c.cmds(&["FUNCTION", "KILL"]).as_str(), "OK");
    assert!(runner.read_value().is_error());
    assert_eq!(Conn::connect(port).cmds(&["PING"]).as_str(), "PONG");
    c.cmds(&["CONFIG", "SET", "lua-time-limit", "5000"]);
    c.cmds(&["FUNCTION", "DELETE", "turnslib"]);
}

// ── error(): tables, the reply's tail, and acl_check_cmd's arity ─────────────

/// As Redis 8.10: `error()` given a table is reported by its `err` (or
/// `ERR unknown error`), not as `table: 0x…`, and every script error ends
/// with the script and the place.
#[test]
fn test_error_with_a_table_is_its_err() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let script = "error('simple string error')";
    let sha = c.cmds(&["SCRIPT", "LOAD", script]).as_str().to_string();
    let r = c.cmds(&["EVAL", script, "0"]);
    assert_eq!(
        r.as_error(),
        format!("ERR user_script:1: simple string error script: {sha}, on @user_script:1.")
    );

    let r = c.cmds(&["EVAL", "error({err='ERR table error'})", "0"]);
    assert!(
        r.as_error().starts_with("ERR table error script: "),
        "{r:?}"
    );
    assert!(r.as_error().ends_with(", on @user_script:1."), "{r:?}");

    let r = c.cmds(&["EVAL", "error({})", "0"]);
    assert!(
        r.as_error().starts_with("ERR unknown error script: "),
        "{r:?}"
    );

    // The line it was raised on.
    let r = c.cmds(&["EVAL", "local a = 1\nerror({err='ERR second'})", "0"]);
    assert!(r.as_error().ends_with(", on @user_script:2."), "{r:?}");

    // A line break in the message is a space, and cannot start a reply.
    let r = c.cmds(&[
        "EVAL",
        "error(redis.error_reply(\"X\\r\\n+INJECTED\"))",
        "0",
    ]);
    assert!(r.as_error().starts_with("ERR X  +INJECTED"), "{r:?}");
    assert_eq!(c.cmds(&["PING"]), RespValue::SimpleStr("PONG".into()));

    // `pcall` hands back its `err`, as Redis's does (#90).
    let r = c.cmds(&[
        "EVAL",
        "local ok, e = pcall(error, {err='ERR caught'}); return e",
        "0",
    ]);
    assert_eq!(r.as_str(), "ERR caught");
}

/// `error` and `assert` are Lua's own but for a table they raise (#80):
/// strings keep the place at the level asked for, `assert` passes its
/// arguments on and takes only a string for its message (#90), and a table
/// is its `err` from a function as from a script.
#[test]
fn test_error_and_assert_raise_as_lua_does() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let script = "assert(false)";
    let sha = c.cmds(&["SCRIPT", "LOAD", script]).as_str().to_string();
    let r = c.cmds(&["EVAL", script, "0"]);
    assert_eq!(
        r.as_error(),
        format!("ERR user_script:1: assertion failed! script: {sha}, on @user_script:1.")
    );

    let r = c.cmds(&["EVAL", "return {assert(1, 2, 3)}", "0"]);
    assert_eq!(
        r,
        RespValue::Array(vec![
            RespValue::Int(1),
            RespValue::Int(2),
            RespValue::Int(3)
        ])
    );

    let r = c.cmds(&[
        "EVAL",
        "local x = 1\nassert(false, {err='ERR asserted'})",
        "0",
    ]);
    // A table is not a message `assert` takes, as in Redis (#90).
    assert!(
        r.as_error()
            .contains(": bad argument #2 to 'assert' (string expected, got table) script: "),
        "{r:?}"
    );

    // Level 2 names the caller's line (the reply always says line 1).
    let r = c.cmds(&[
        "EVAL",
        "local function f() error('deep', 2) end\nlocal ok, e = pcall(function()\n  f()\nend)\nreturn e",
        "0",
    ]);
    assert_eq!(r.as_str(), "user_script:3: deep");

    let r = c.cmds(&[
        "EVAL",
        "local ok, e = pcall(assert, false, 'kept'); return e",
        "0",
    ]);
    assert_eq!(r.as_str(), "kept");

    let lib = "#!lua name=errlib\nredis.register_function('raise', function() error({err='ERR from fn'}) end)";
    assert_eq!(c.cmds(&["FUNCTION", "LOAD", lib]).as_str(), "errlib");
    let r = c.cmds(&["FCALL", "raise", "0"]);
    assert!(
        r.as_error().starts_with("ERR from fn script: raise"),
        "{r:?}"
    );
    assert_eq!(c.cmds(&["PING"]), RespValue::SimpleStr("PONG".into()));
}

/// A table `error` raises is left as the script made it: one with a
/// metatable of its own replies with its `err` too, one raised again replies
/// with its `err` as it is then, and one caught with `xpcall` is still a
/// plain table to `tostring` and `getmetatable`.
#[test]
fn test_error_leaves_the_table_it_raises_alone() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&[
        "EVAL",
        "error(setmetatable({err='ERR meta'}, {__index = {}}))",
        "0",
    ]);
    assert!(r.as_error().starts_with("ERR meta script: "), "{r:?}");
    assert!(r.as_error().ends_with(", on @user_script:1."), "{r:?}");

    let r = c.cmds(&[
        "EVAL",
        "local t = {err='ERR first'}\nlocal ok = pcall(error, t)\nt.err = 'ERR second'\nerror(t)",
        "0",
    ]);
    assert!(r.as_error().starts_with("ERR second script: "), "{r:?}");
    assert!(r.as_error().ends_with(", on @user_script:4."), "{r:?}");

    let r = c.cmds(&[
        "EVAL",
        "local ok, e = xpcall(error, function(m) return m end, {err='ERR x'})\nreturn {tostring(e):sub(1, 7), getmetatable(e) == nil and 1 or 0}",
        "0",
    ]);
    assert_eq!(
        r,
        RespValue::Array(vec![
            RespValue::Bulk(b"table: ".to_vec()),
            RespValue::Int(1)
        ])
    );

    // A table caught before does not stand in for a later error with the
    // same text: in the same script, or in a later one.
    let r = c.cmds(&[
        "EVAL",
        "local t = {err='ERR caught'}\npcall(error, t)\nerror(tostring(t), 0)",
        "0",
    ]);
    assert!(r.as_error().contains(" table: 0x"), "{r:?}");
    let r = c.cmds(&[
        "EVAL",
        "pcall(error, setmetatable({err='ERR earlier'}, {__tostring = function() return 'boom' end}))\nreturn 1",
        "0",
    ]);
    assert_eq!(r, RespValue::Int(1));
    let r = c.cmds(&["EVAL", "error('boom', 0)", "0"]);
    assert!(r.as_error().contains(" boom script: "), "{r:?}");
    // Nor in a later script that fails without `error`, with the same text.
    let r = c.cmds(&[
        "EVAL",
        "pcall(error, setmetatable({err='ERR earlier'}, {__tostring = function() return \"user_script:1: attempt to call a nil value (local 'f')\" end}))\nreturn 1",
        "0",
    ]);
    assert_eq!(r, RespValue::Int(1));
    let r = c.cmds(&["EVAL", "local f = nil f()", "0"]);
    assert!(
        r.as_error().contains("attempt to call a nil value"),
        "{r:?}"
    );

    let r = c.cmds(&[
        "EVAL",
        "error(setmetatable({err='ERR own'}, {__tostring = function() return 'own' end}))",
        "0",
    ]);
    assert!(r.as_error().starts_with("ERR own script: "), "{r:?}");
    let r = c.cmds(&[
        "EVAL",
        "error(setmetatable({err='ERR own'}, {__tostring = function() error('boom') end}))",
        "0",
    ]);
    assert!(r.as_error().starts_with("ERR own script: "), "{r:?}");

    // Nor does one a finalizer raises and catches while another script runs.
    let forged = "user_script:2: Script attempted to access nonexistent global variable 'nosuch'";
    let finalizers = format!(
        "for i = 1, 50 do setmetatable({{}}, {{__gc = function() pcall(error, setmetatable({{err='ERR forged'}}, {{__tostring = function() return \"{forged}\" end}})) end}}) end\nreturn 1"
    );
    assert_eq!(c.cmds(&["EVAL", &finalizers, "0"]), RespValue::Int(1));
    let r = c.cmds(&[
        "EVAL",
        "local t = {} for i = 1, 300000 do t[i] = {} end\nreturn nosuch",
        "0",
    ]);
    assert!(
        r.as_error()
            .contains("nonexistent global variable 'nosuch'"),
        "{r:?}"
    );

    // An `err` ends at a NUL, as Redis reads it.
    let r = c.cmds(&["EVAL", "error({err='ERR a\\0b'})", "0"]);
    assert!(r.as_error().starts_with("ERR a script: "), "{r:?}");

    let lib = "#!lua name=metalib\nredis.register_function('raise_meta', function() error(setmetatable({err='ERR meta fn'}, {})) end)";
    assert_eq!(c.cmds(&["FUNCTION", "LOAD", lib]).as_str(), "metalib");
    let r = c.cmds(&["FCALL", "raise_meta", "0"]);
    assert!(
        r.as_error().starts_with("ERR meta fn script: raise_meta"),
        "{r:?}"
    );
}

/// `pcall` hands back what it caught as Redis's does (#90): a table's `err`
/// if that is a string or a number, and the message of an error from
/// `redis.call` and the rest of the API. `xpcall` and a table without such an
/// `err` are left as they are. `assert` takes only a string or a number for
/// its message, as Lua 5.1's does.
#[test]
fn test_pcall_gives_the_err_and_assert_takes_a_string() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let bulk = |s: &str| RespValue::Bulk(s.as_bytes().to_vec());
    assert_eq!(
        c.cmds(&["SET", "s", "abc"]),
        RespValue::SimpleStr("OK".into())
    );

    // What `pcall` hands back, by what was raised.
    for (raise, want) in [
        ("error({err='ERR x'})", vec![bulk("string"), bulk("ERR x")]),
        (
            "error(redis.error_reply('MY x'))",
            vec![bulk("string"), bulk("MY x")],
        ),
        ("error({err=42})", vec![bulk("number"), RespValue::Int(42)]),
        (
            "error(setmetatable({}, {__index = {err='ERR idx'}}))",
            vec![bulk("string"), bulk("ERR idx")],
        ),
        (
            "local ok, e = pcall(error, {err='ERR in'}) error({err='ERR out'})",
            vec![bulk("string"), bulk("ERR out")],
        ),
        (
            "redis.call('INCR', 's')",
            vec![
                bulk("string"),
                bulk("ERR value is not an integer or out of range"),
            ],
        ),
        (
            "redis.call('NOSUCHCMD')",
            vec![
                bulk("string"),
                bulk("ERR Unknown Redis command called from script"),
            ],
        ),
        ("error({})", vec![bulk("table")]),
        ("error({err={1}})", vec![bulk("table")]),
        ("error({err=true})", vec![bulk("table")]),
    ] {
        let script = format!(
            "local ok, e = pcall(function() {raise} end)\nif type(e) == 'table' then return {{type(e)}} end\nreturn {{type(e), e}}"
        );
        assert_eq!(
            c.cmds(&["EVAL", &script, "0"]),
            RespValue::Array(want),
            "{raise}"
        );
    }
    let r = c.cmds(&[
        "EVAL",
        "local ok, e = pcall(function() error({err='ERR l2'}, 2) end) return e",
        "0",
    ]);
    assert_eq!(r.as_str(), "ERR l2");
    let r = c.cmds(&[
        "EVAL",
        "local ok, e = pcall(cjson.decode, '{bad') return type(e)",
        "0",
    ]);
    assert_eq!(r.as_str(), "string");
    let r = c.cmds(&[
        "EVAL_RO",
        "local ok, e = pcall(redis.call, 'SET', 'x', '1') return e",
        "0",
    ]);
    assert_eq!(
        r.as_str(),
        "ERR Write commands are not allowed from read-only scripts."
    );
    // A table whose `err` is neither comes back with that `err` after it, as
    // Redis's `pcall` leaves it.
    let r = c.cmds(&[
        "EVAL",
        "return {select('#', pcall(error, {})), select('#', pcall(error, {err='x'}))}",
        "0",
    ]);
    assert_eq!(
        r,
        RespValue::Array(vec![RespValue::Int(3), RespValue::Int(2)])
    );
    let r = c.cmds(&["EVAL", "return {select(3, pcall(error, {err=true}))}", "0"]);
    assert_eq!(r, RespValue::Array(vec![RespValue::Int(1)]));
    // What succeeds comes back as it was.
    let r = c.cmds(&["EVAL", "return {pcall(function() return 1, 2 end)}", "0"]);
    assert_eq!(
        r,
        RespValue::Array(vec![
            RespValue::Int(1),
            RespValue::Int(1),
            RespValue::Int(2)
        ])
    );
    // `xpcall` is Lua's own.
    let r = c.cmds(&[
        "EVAL",
        "local ok, e = xpcall(function() error({err='ERR x'}) end, function(m) return m end) return type(e)",
        "0",
    ]);
    assert_eq!(r.as_str(), "table");
    // A coroutine can still yield inside `pcall`.
    let r = c.cmds(&[
        "EVAL",
        "local co = coroutine.wrap(function() pcall(function() coroutine.yield(7) end) end) return co()",
        "0",
    ]);
    assert_eq!(r, RespValue::Int(7));

    // `assert`'s message.
    for (script, want) in [
        ("assert(false, 'boom')", "ERR user_script:1: boom script: "),
        ("assert(false, 42)", "ERR user_script:1: 42 script: "),
        (
            "assert(false, nil)",
            "ERR user_script:1: assertion failed! script: ",
        ),
        (
            "assert(false, {err='ERR asserted'})",
            "ERR user_script:1: bad argument #2 to 'assert' (string expected, got table) script: ",
        ),
    ] {
        let r = c.cmds(&["EVAL", script, "0"]);
        assert!(r.as_error().starts_with(want), "{script}: {r:?}");
    }
    let r = c.cmds(&[
        "EVAL",
        "local ok, e = pcall(assert, false, {err='x'}) return e",
        "0",
    ]);
    assert_eq!(
        r.as_str(),
        "bad argument #2 to '?' (string expected, got table)"
    );
    let r = c.cmds(&["EVAL", "local ok, e = pcall(assert) return e", "0"]);
    assert_eq!(r.as_str(), "bad argument #1 to '?' (value expected)");
    let r = c.cmds(&["EVAL", "assert()", "0"]);
    assert!(
        r.as_error().starts_with(
            "ERR user_script:1: bad argument #1 to 'assert' (value expected) script: "
        ),
        "{r:?}"
    );

    // Functions have the same.
    let lib = "#!lua name=pcalllib\nredis.register_function('try_incr', function(keys) local ok, e = pcall(redis.call, 'INCR', keys[1]) return e end)";
    assert_eq!(c.cmds(&["FUNCTION", "LOAD", lib]).as_str(), "pcalllib");
    let r = c.cmds(&["FCALL", "try_incr", "1", "s"]);
    assert_eq!(r.as_str(), "ERR value is not an integer or out of range");
}

/// `redis.error_reply` gives a message without a code the code `ERR`, as
/// Redis does, and keeps one that has a code.
#[test]
fn test_error_reply_without_a_code_is_err() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["EVAL", "return redis.error_reply('X')", "0"]);
    assert_eq!(r.as_error(), "ERR X");
    let r = c.cmds(&[
        "EVAL",
        "return redis.error_reply('MY_CODE custom msg')",
        "0",
    ]);
    assert_eq!(r.as_error(), "MY_CODE custom msg");
    let r = c.cmds(&[
        "EVAL",
        "return redis.error_reply('-MY_CODE custom msg')",
        "0",
    ]);
    assert_eq!(r.as_error(), "MY_CODE custom msg");
}

/// `redis.acl_check_cmd` refuses a command with the wrong number of
/// arguments, as Redis does, instead of answering for it.
#[test]
fn test_acl_check_cmd_checks_the_arity() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["EVAL", "return redis.acl_check_cmd('eval', 'script')", "0"]);
    assert!(r.as_error().contains("Wrong number of args"), "{r:?}");
    let r = c.cmds(&["EVAL", "return redis.acl_check_cmd('set', 'x', '1')", "0"]);
    assert_eq!(r.as_int(), 1);
}

/// A FUNCTION LOAD leaves the chunk it ran behind as garbage. As in Redis
/// the collector is stepped as functions are loaded, so loading many keeps
/// `used_memory_vm_functions` near what they hold.
#[test]
fn test_function_loads_are_collected() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["FUNCTION", "FLUSH"]);

    let n = 50_000;
    for i in 0..n {
        let body = format!(
            "#!lua name=gc{i}\nredis.register_function('gc{i}', function(KEYS, ARGV)\n --xxxxxxxxxx\nreturn {i:06} \nend)"
        );
        let r = c.cmds(&["FUNCTION", "LOAD", &body]);
        assert!(!r.is_error(), "{r:?}");
    }
    let info = c.cmds(&["INFO", "memory"]);
    let bytes: u64 = info
        .as_str()
        .lines()
        .find_map(|l| l.strip_prefix("used_memory_vm_functions:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    c.cmds(&["FUNCTION", "FLUSH"]);
    eprintln!("used_memory_vm_functions after {n} loads: {bytes}");
    // Redis 8.10's suite allows 14,500,000 for this.
    assert!(bytes < 14_500_000, "{bytes}");
}

/// What a script runs counts as its client's (`tot-cmds`), as in Redis:
/// `EVAL` and its `redis.call('ping')` are two.
#[test]
fn test_a_script_s_commands_count_as_its_client_s() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let tot_cmds = |info: &RespValue| -> i64 {
        info.as_str()
            .split_whitespace()
            .find_map(|kv| kv.strip_prefix("tot-cmds="))
            .unwrap()
            .parse()
            .unwrap()
    };
    let before = tot_cmds(&c.cmds(&["CLIENT", "INFO"]));
    c.cmds(&["EVAL", "redis.call('ping')", "0"]);
    let after = tot_cmds(&c.cmds(&["CLIENT", "INFO"]));
    // The first CLIENT INFO, EVAL and PING; not the second CLIENT INFO.
    assert_eq!(after - before, 3);
}

/// `EVAL` scripts and functions have a VM each, as in Redis: what scripts
/// hold is in `used_memory_lua`, not `used_memory_vm_functions`, `SCRIPT
/// FLUSH` lets it go, and `FUNCTION FLUSH` leaves it.
#[test]
fn test_scripts_and_functions_have_a_vm_each() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let info = |c: &mut Conn, field: &str| -> u64 {
        c.cmds(&["INFO", "memory"])
            .as_str()
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    c.cmds(&["FUNCTION", "FLUSH"]);
    c.cmds(&["SCRIPT", "FLUSH"]);
    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=vms\nredis.register_function('vms', function() return 1 end)",
    ]);
    assert_eq!(c.cmds(&["FCALL", "vms", "0"]).as_int(), 1);
    let functions = info(&mut c, "used_memory_vm_functions");
    assert!(functions > 0);

    // A thousand scripts, each compiled and kept by the EVAL VM.
    for i in 0..1000 {
        c.cmds(&["EVAL", &format!("return {i}"), "0"]);
    }
    let scripts = info(&mut c, "used_memory_lua");
    assert!(scripts > 100_000, "{scripts}");
    assert_eq!(info(&mut c, "used_memory_vm_functions"), functions);

    // FUNCTION FLUSH leaves the scripts' VM, SCRIPT FLUSH lets it go.
    c.cmds(&["FUNCTION", "FLUSH"]);
    assert_eq!(info(&mut c, "used_memory_lua"), scripts);
    c.cmds(&["SCRIPT", "FLUSH"]);
    // The VM goes; a new one is made when a script next runs.
    assert_eq!(info(&mut c, "used_memory_lua"), 0);
    assert_eq!(info(&mut c, "used_memory_vm_eval"), 0);
    assert_eq!(
        info(&mut c, "used_memory_vm_total"),
        info(&mut c, "used_memory_vm_functions")
    );
    assert_eq!(c.cmds(&["EVAL", "return 7", "0"]).as_int(), 7);
    assert!(info(&mut c, "used_memory_scripts_eval") > 0);
}

/// What one flush does to the other kind (vimoppa's review of #75): a
/// function still runs after `SCRIPT FLUSH`, a script cached before
/// `FUNCTION FLUSH` still runs after it, and `SCRIPT RESET` renews both VMs
/// with everything still working after.
#[test]
fn test_each_flush_leaves_the_other_kind_working() {
    let _serial = LUA_SCRIPTING_SERIAL.lock().unwrap();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["FUNCTION", "FLUSH"]);
    c.cmds(&["SCRIPT", "FLUSH"]);
    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=keep\nredis.register_function('keep', function() return 5 end)",
    ]);
    let sha = c.cmds(&["SCRIPT", "LOAD", "return 6"]).as_str().to_string();

    // SCRIPT FLUSH: the function is untouched; the script is gone.
    c.cmds(&["SCRIPT", "FLUSH"]);
    assert_eq!(c.cmds(&["FCALL", "keep", "0"]).as_int(), 5);
    assert!(c.cmds(&["EVALSHA", &sha, "0"]).is_error());

    // FUNCTION FLUSH: a script cached before it still runs.
    let sha = c.cmds(&["SCRIPT", "LOAD", "return 6"]).as_str().to_string();
    assert_eq!(c.cmds(&["EVALSHA", &sha, "0"]).as_int(), 6);
    c.cmds(&["FUNCTION", "FLUSH", "ASYNC"]);
    assert_eq!(c.cmds(&["EVALSHA", &sha, "0"]).as_int(), 6);
    assert!(c.cmds(&["FCALL", "keep", "0"]).is_error());

    // SCRIPT RESET renews both VMs in one go; scripts and functions run after.
    c.cmds(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=keep\nredis.register_function('keep', function() return 5 end)",
    ]);
    assert_eq!(
        c.cmds(&["SCRIPT", "RESET"]),
        RespValue::SimpleStr("OK".into())
    );
    assert_eq!(c.cmds(&["EVAL", "return 8", "0"]).as_int(), 8);
    assert_eq!(c.cmds(&["FCALL", "keep", "0"]).as_int(), 5);

    // SCRIPT FLUSH takes SYNC or ASYNC and nothing else.
    assert_eq!(
        c.cmds(&["SCRIPT", "FLUSH", "ASYNC"]),
        RespValue::SimpleStr("OK".into())
    );
    assert!(c.cmds(&["SCRIPT", "FLUSH", "NOW"]).is_error());
}
