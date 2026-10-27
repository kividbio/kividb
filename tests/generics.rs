use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

static PAUSE_TEST_LOCK: Mutex<()> = Mutex::new(());

fn pause_lock() -> std::sync::MutexGuard<'static, ()> {
    PAUSE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct PauseGuard;
impl Drop for PauseGuard {
    fn drop(&mut self) {
        kividb::pause::clear_pause();
    }
}
// ── Minimal RESP client (same as vector_hash.rs) ──────────────────────────────

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
            Some(b'>') | Some(b'~') => {
                // RESP3 push frame or set — same structure as array
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            Some(b'%') => {
                // RESP3 map — read as the flat array RESP2 would have sent
                let count: i64 = line[1..].parse().unwrap();
                let items = (0..count * 2).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            Some(b'_') => RespValue::Nil,
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
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            // Some commands return integers as bulk strings (e.g. INCRBYFLOAT, JSON.NUMINCRBY)
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("not an int: {:?}", self),
        }
    }
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap(),
            _ => panic!("not a string: {:?}", self),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
            || matches!(self, RespValue::Bulk(b) if b.as_slice() == b"OK")
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn error_msg(&self) -> &str {
        match self {
            RespValue::Error(s) => s,
            _ => panic!("not an error: {:?}", self),
        }
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

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
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            query_buffer_idle_resize_after_ms: 150,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

// ═════════════════════════════════════════════════════════════════════════════
// EXPIRE FAMILY SUBCOMMAND TESTS (NX / XX / GT / LT)
// ═════════════════════════════════════════════════════════════════════════════

// EXPIRE NX — set TTL only if key has no current expiry
#[test]
fn test_expire_nx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    // NX on key with no TTL → sets it
    assert_eq!(c.cmds(&["EXPIRE", "k", "100", "NX"]).as_int(), 1);
    assert!(c.cmds(&["TTL", "k"]).as_int() > 0);

    // NX on key that already has TTL → no-op
    assert_eq!(c.cmds(&["EXPIRE", "k", "999", "NX"]).as_int(), 0);
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl <= 100, "TTL should not have been updated, got {ttl}");
}

// EXPIRE XX — set TTL only if key already has an expiry
#[test]
fn test_expire_xx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    // XX on key with no TTL → no-op
    assert_eq!(c.cmds(&["EXPIRE", "k", "100", "XX"]).as_int(), 0);
    assert_eq!(c.cmds(&["TTL", "k"]).as_int(), -1);

    // XX on key that has TTL → updates it
    c.cmds(&["EXPIRE", "k", "50"]);
    assert_eq!(c.cmds(&["EXPIRE", "k", "200", "XX"]).as_int(), 1);
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl > 100, "TTL should have been extended, got {ttl}");
}

// EXPIRE GT — set TTL only if new TTL is greater than current
#[test]
fn test_expire_gt() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    c.cmds(&["EXPIRE", "k", "100"]);

    // GT with larger TTL → updates
    assert_eq!(c.cmds(&["EXPIRE", "k", "200", "GT"]).as_int(), 1);
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl > 100, "TTL should have been extended, got {ttl}");

    // GT with smaller TTL → no-op
    assert_eq!(c.cmds(&["EXPIRE", "k", "10", "GT"]).as_int(), 0);
    let ttl2 = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl2 > 100, "TTL should not have been reduced, got {ttl2}");

    // GT on key with no TTL → no-op: no TTL is for ever, and no finite TTL
    // is greater than that.
    c.cmds(&["SET", "k2", "world"]);
    assert_eq!(c.cmds(&["EXPIRE", "k2", "50", "GT"]).as_int(), 0);
    assert_eq!(c.cmds(&["TTL", "k2"]).as_int(), -1);
}

// EXPIRE LT — set TTL only if new TTL is less than current
#[test]
fn test_expire_lt() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    c.cmds(&["EXPIRE", "k", "100"]);

    // LT with smaller TTL → updates
    assert_eq!(c.cmds(&["EXPIRE", "k", "50", "LT"]).as_int(), 1);
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl <= 50, "TTL should have been reduced, got {ttl}");

    // LT with larger TTL → no-op
    assert_eq!(c.cmds(&["EXPIRE", "k", "999", "LT"]).as_int(), 0);
    let ttl2 = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl2 <= 50, "TTL should not have been extended, got {ttl2}");

    // LT on key with no TTL → sets it (no TTL = ∞, finite < ∞)
    c.cmds(&["SET", "k2", "world"]);
    assert_eq!(c.cmds(&["EXPIRE", "k2", "50", "LT"]).as_int(), 1);
    assert!(c.cmds(&["TTL", "k2"]).as_int() > 0);
}

// PEXPIRE NX / XX / GT / LT
#[test]
fn test_pexpire_conditions() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    // NX: no existing TTL → sets
    assert_eq!(c.cmds(&["PEXPIRE", "k", "10000", "NX"]).as_int(), 1);
    assert!(c.cmds(&["PTTL", "k"]).as_int() > 0);

    // NX: has TTL → no-op
    assert_eq!(c.cmds(&["PEXPIRE", "k", "99999", "NX"]).as_int(), 0);

    // GT: larger → sets
    assert_eq!(c.cmds(&["PEXPIRE", "k", "50000", "GT"]).as_int(), 1);
    let pttl = c.cmds(&["PTTL", "k"]).as_int();
    assert!(pttl > 10000, "expected PTTL extended, got {pttl}");

    // LT: smaller → sets
    assert_eq!(c.cmds(&["PEXPIRE", "k", "5000", "LT"]).as_int(), 1);
    let pttl2 = c.cmds(&["PTTL", "k"]).as_int();
    assert!(pttl2 <= 5000, "expected PTTL reduced, got {pttl2}");
}

// EXPIREAT NX / XX / GT / LT
#[test]
fn test_expireat_conditions() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    c.cmds(&["SET", "k", "hello"]);

    // NX on key with no TTL → sets
    let ts100 = (now + 100).to_string();
    assert_eq!(c.cmds(&["EXPIREAT", "k", &ts100, "NX"]).as_int(), 1);
    assert!(c.cmds(&["TTL", "k"]).as_int() > 0);

    // NX on key with TTL → no-op
    assert_eq!(
        c.cmds(&["EXPIREAT", "k", &(now + 999).to_string(), "NX"])
            .as_int(),
        0
    );

    // GT with later timestamp → updates
    assert_eq!(
        c.cmds(&["EXPIREAT", "k", &(now + 500).to_string(), "GT"])
            .as_int(),
        1
    );
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl > 100, "expected TTL extended, got {ttl}");

    // LT with earlier timestamp → updates
    assert_eq!(
        c.cmds(&["EXPIREAT", "k", &(now + 50).to_string(), "LT"])
            .as_int(),
        1
    );
    let ttl2 = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl2 <= 50, "expected TTL reduced, got {ttl2}");
}

// PEXPIREAT NX / XX / GT / LT
#[test]
fn test_pexpireat_conditions() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    c.cmds(&["SET", "k", "hello"]);

    // NX: no TTL → sets
    let ts = (now_ms + 10_000).to_string();
    assert_eq!(c.cmds(&["PEXPIREAT", "k", &ts, "NX"]).as_int(), 1);
    assert!(c.cmds(&["PTTL", "k"]).as_int() > 0);

    // XX: has TTL → updates
    let ts2 = (now_ms + 50_000).to_string();
    assert_eq!(c.cmds(&["PEXPIREAT", "k", &ts2, "XX"]).as_int(), 1);
    let pttl = c.cmds(&["PTTL", "k"]).as_int();
    assert!(pttl > 10_000, "expected PTTL extended, got {pttl}");

    // LT: smaller → updates
    let ts3 = (now_ms + 5_000).to_string();
    assert_eq!(c.cmds(&["PEXPIREAT", "k", &ts3, "LT"]).as_int(), 1);
    let pttl2 = c.cmds(&["PTTL", "k"]).as_int();
    assert!(pttl2 <= 5_000, "expected PTTL reduced, got {pttl2}");
}

// All four commands return 0 on missing key regardless of flag
#[test]
fn test_expire_family_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["EXPIRE", "nosuch", "100", "NX"]).as_int(), 0);
    assert_eq!(c.cmds(&["PEXPIRE", "nosuch", "100", "NX"]).as_int(), 0);
    assert_eq!(
        c.cmds(&["EXPIREAT", "nosuch", "9999999999", "NX"]).as_int(),
        0
    );
    assert_eq!(
        c.cmds(&["PEXPIREAT", "nosuch", "9999999999999", "NX"])
            .as_int(),
        0
    );
}

// RENAME: basic rename
//   SET src "hello"
//   RENAME src dst  → OK
//   GET src         → nil
//   GET dst         → "hello"
#[test]
fn test_rename_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "rename:src", "hello"]);
    assert!(c.cmds(&["RENAME", "rename:src", "rename:dst"]).is_ok());
    assert!(c.cmds(&["GET", "rename:src"]).is_nil());
    assert_eq!(c.cmds(&["GET", "rename:dst"]).as_str(), "hello");
}

// RENAME preserves TTL
#[test]
fn test_rename_preserves_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "rttl:src", "v", "EX", "100"]);
    assert!(c.cmds(&["RENAME", "rttl:src", "rttl:dst"]).is_ok());
    let ttl = c.cmds(&["TTL", "rttl:dst"]).as_int();
    assert!(ttl > 0 && ttl <= 100);
}

// RENAME non-existent key returns error
#[test]
fn test_rename_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["RENAME", "rename:nosuch", "rename:dst"]);
    assert!(r.is_error());
    assert!(r.error_msg().contains("no such key"));
}

// RENAMENX: only renames if destination does not exist
#[test]
fn test_renamenx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "rnx:src", "hello"]);

    // Destination does not exist → rename succeeds → 1
    assert_eq!(c.cmds(&["RENAMENX", "rnx:src", "rnx:dst"]).as_int(), 1);
    assert!(c.cmds(&["GET", "rnx:src"]).is_nil());
    assert_eq!(c.cmds(&["GET", "rnx:dst"]).as_str(), "hello");

    // Now set src again; dst exists → rename fails → 0
    c.cmds(&["SET", "rnx:src", "world"]);
    assert_eq!(c.cmds(&["RENAMENX", "rnx:src", "rnx:dst"]).as_int(), 0);
    assert_eq!(c.cmds(&["GET", "rnx:src"]).as_str(), "world"); // unchanged
    assert_eq!(c.cmds(&["GET", "rnx:dst"]).as_str(), "hello"); // unchanged
}

// COPY: basic copy within same db
#[test]
fn test_copy_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "copy:src", "hello"]);
    assert_eq!(c.cmds(&["COPY", "copy:src", "copy:dst"]).as_int(), 1);
    assert_eq!(c.cmds(&["GET", "copy:src"]).as_str(), "hello"); // src still exists
    assert_eq!(c.cmds(&["GET", "copy:dst"]).as_str(), "hello");
}

// COPY does not overwrite by default
#[test]
fn test_copy_no_overwrite() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "copy:s", "src_val"]);
    c.cmds(&["SET", "copy:d", "dst_val"]);

    // dst exists, no REPLACE → returns 0
    assert_eq!(c.cmds(&["COPY", "copy:s", "copy:d"]).as_int(), 0);
    assert_eq!(c.cmds(&["GET", "copy:d"]).as_str(), "dst_val"); // unchanged
}

// COPY REPLACE overwrites destination
#[test]
fn test_copy_replace() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "cpyr:s", "new_val"]);
    c.cmds(&["SET", "cpyr:d", "old_val"]);

    assert_eq!(c.cmds(&["COPY", "cpyr:s", "cpyr:d", "REPLACE"]).as_int(), 1);
    assert_eq!(c.cmds(&["GET", "cpyr:d"]).as_str(), "new_val");
}

// COPY to different DB
#[test]
fn test_copy_different_db() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "0"]);
    c.cmds(&["SET", "xdb:k", "hello"]);

    assert_eq!(c.cmds(&["COPY", "xdb:k", "xdb:k", "DB", "1"]).as_int(), 1);

    // Verify in db 1
    c.cmds(&["SELECT", "1"]);
    assert_eq!(c.cmds(&["GET", "xdb:k"]).as_str(), "hello");

    // Cleanup
    c.cmds(&["SELECT", "0"]);
}

// MOVE: move key to another database
#[test]
fn test_move_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "0"]);
    c.cmds(&["SET", "move:k", "value"]);

    assert_eq!(c.cmds(&["MOVE", "move:k", "2"]).as_int(), 1);
    assert!(c.cmds(&["GET", "move:k"]).is_nil()); // gone from db 0

    c.cmds(&["SELECT", "2"]);
    assert_eq!(c.cmds(&["GET", "move:k"]).as_str(), "value"); // in db 2

    // Cleanup
    c.cmds(&["SELECT", "0"]);
}

// MOVE fails if key exists in target db
#[test]
fn test_move_target_exists() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "0"]);
    c.cmds(&["SET", "move:conflict", "src"]);
    c.cmds(&["SELECT", "3"]);
    c.cmds(&["SET", "move:conflict", "dst"]);
    c.cmds(&["SELECT", "0"]);

    assert_eq!(c.cmds(&["MOVE", "move:conflict", "3"]).as_int(), 0);
    // Key should still be in db 0
    assert_eq!(c.cmds(&["GET", "move:conflict"]).as_str(), "src");
}

// SWAPDB: swap two databases
#[test]
fn test_swapdb() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "4"]);
    c.cmds(&["SET", "swap:k", "from_db4"]);
    c.cmds(&["SELECT", "5"]);
    c.cmds(&["SET", "swap:other", "from_db5"]);
    c.cmds(&["SELECT", "0"]);

    assert!(c.cmds(&["SWAPDB", "4", "5"]).is_ok());

    // db4 should now have what was in db5
    c.cmds(&["SELECT", "4"]);
    assert!(c.cmds(&["GET", "swap:k"]).is_nil());
    assert_eq!(c.cmds(&["GET", "swap:other"]).as_str(), "from_db5");

    // db5 should now have what was in db4
    c.cmds(&["SELECT", "5"]);
    assert_eq!(c.cmds(&["GET", "swap:k"]).as_str(), "from_db4");
    assert!(c.cmds(&["GET", "swap:other"]).is_nil());

    // Cleanup
    c.cmds(&["SELECT", "0"]);
}

// SWAPDB same db is a no-op
#[test]
fn test_swapdb_same_db() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["SWAPDB", "0", "0"]).is_ok());
}

// RANDOMKEY returns a key when db is non-empty, nil when empty
#[test]
fn test_randomkey() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Use a fresh db to avoid pollution from other tests
    c.cmds(&["SELECT", "6"]);
    assert!(c.cmds(&["RANDOMKEY"]).is_nil());

    c.cmds(&["SET", "rk:one", "1"]);
    let r = c.cmds(&["RANDOMKEY"]);
    assert!(!r.is_nil());
    assert_eq!(r.as_str(), "rk:one");

    c.cmds(&["SELECT", "0"]);
}

// ═════════════════════════════════════════════════════════════════════════════
// SCAN
// ═════════════════════════════════════════════════════════════════════════════

// Regression test for a real bug: SCAN used to collect + clone + sort every
// key in the database on EVERY call and treat the cursor as a plain index
// into that fresh snapshot -- making one full keyspace walk O(N^2 log N)
// instead of ~O(N) (found live: cleaning up a 1.18M-key benchmark database
// took ~18 minutes). The fix packs (shard_index, offset-within-shard) into
// the cursor so each call only touches the shard(s) needed for one batch.
// This drives SCAN to completion with a small COUNT against enough keys to
// span many shards, and checks the classic SCAN contract holds: every live
// key is returned at least once, no key is fabricated, and the cursor
// eventually returns to 0.
#[test]
fn test_scan_drains_full_keyspace_without_duplicates_or_omissions() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "7"]);
    c.cmds(&["FLUSHDB"]);

    const N: usize = 5000;
    let expected: std::collections::HashSet<String> =
        (0..N).map(|i| format!("scankey:{i}")).collect();
    for k in &expected {
        assert!(c.cmds(&["SET", k, "v"]).is_ok());
    }

    let mut found: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut cursor = "0".to_string();
    let mut calls = 0;
    loop {
        calls += 1;
        assert!(calls <= N, "SCAN never converged back to cursor 0");
        let r = c.cmds(&["SCAN", &cursor, "COUNT", "50"]);
        let parts = r.as_array();
        cursor = parts[0].as_str().to_string();
        for item in parts[1].as_array() {
            let key = item.as_str().to_string();
            assert!(
                expected.contains(&key),
                "SCAN returned a key that was never set: {key}"
            );
            found.insert(key);
        }
        if cursor == "0" {
            break;
        }
    }

    assert_eq!(
        found.len(),
        expected.len(),
        "SCAN did not return every live key exactly once (missing {} keys)",
        expected.difference(&found).count()
    );

    c.cmds(&["FLUSHDB"]);
    c.cmds(&["SELECT", "0"]);
}

#[test]
fn test_scan_match_filters_without_dropping_unmatched_from_cursor_progress() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "8"]);
    c.cmds(&["FLUSHDB"]);

    for i in 0..300 {
        c.cmds(&["SET", &format!("keep:{i}"), "v"]);
        c.cmds(&["SET", &format!("skip:{i}"), "v"]);
    }

    let mut found: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut cursor = "0".to_string();
    loop {
        let r = c.cmds(&["SCAN", &cursor, "MATCH", "keep:*", "COUNT", "25"]);
        let parts = r.as_array();
        cursor = parts[0].as_str().to_string();
        for item in parts[1].as_array() {
            let key = item.as_str().to_string();
            assert!(
                key.starts_with("keep:"),
                "MATCH leaked a non-matching key: {key}"
            );
            found.insert(key);
        }
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(
        found.len(),
        300,
        "MATCH-filtered SCAN missed some matching keys"
    );

    c.cmds(&["FLUSHDB"]);
    c.cmds(&["SELECT", "0"]);
}

// ═════════════════════════════════════════════════════════════════════════════
// SERVER COMMANDS
// ═════════════════════════════════════════════════════════════════════════════

fn start_server_with_password(port: u16, password: &'static str) {
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: password.to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

// FLUSHALL removes keys from all databases
#[test]
fn test_flushall() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "0"]);
    c.cmds(&["SET", "fa:k0", "v0"]);
    c.cmds(&["SELECT", "7"]);
    c.cmds(&["SET", "fa:k7", "v7"]);
    c.cmds(&["SELECT", "0"]);

    assert!(c.cmds(&["FLUSHALL"]).is_ok());

    assert!(c.cmds(&["GET", "fa:k0"]).is_nil());
    c.cmds(&["SELECT", "7"]);
    assert!(c.cmds(&["GET", "fa:k7"]).is_nil());
    c.cmds(&["SELECT", "0"]);
}

// FLUSHDB only removes keys from the current database
#[test]
fn test_flushdb_only_current() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SELECT", "0"]);
    c.cmds(&["SET", "fdb:k0", "v0"]);
    c.cmds(&["SELECT", "8"]);
    c.cmds(&["SET", "fdb:k8", "v8"]);

    // Flush only db 8
    assert!(c.cmds(&["FLUSHDB"]).is_ok());
    assert!(c.cmds(&["GET", "fdb:k8"]).is_nil());

    // db 0 should be untouched
    c.cmds(&["SELECT", "0"]);
    assert_eq!(c.cmds(&["GET", "fdb:k0"]).as_str(), "v0");
}

// ═════════════════════════════════════════════════════════════════════════════
// AUTH COMMANDS
// ═════════════════════════════════════════════════════════════════════════════

// AUTH <password> on a server with no password is Redis's error (#135);
// nothing else is held up by it
#[test]
fn test_auth_no_password_configured() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["AUTH", "anything"]).is_error());
    // Normal commands still work
    assert!(c.cmds(&["SET", "auth:k", "v"]).is_ok());
}

// AUTH with correct password succeeds
#[test]
fn test_auth_correct_password() {
    let port = free_port();
    start_server_with_password(port, "secretpass");
    let mut c = Conn::connect(port);

    // Before auth, commands should fail
    let r = c.cmds(&["SET", "auth:k", "v"]);
    assert!(r.is_error());
    assert!(r.error_msg().contains("NOAUTH"));

    // Auth with correct password
    assert!(c.cmds(&["AUTH", "secretpass"]).is_ok());

    // Now commands work
    assert!(c.cmds(&["SET", "auth:k", "v"]).is_ok());
    assert_eq!(c.cmds(&["GET", "auth:k"]).as_str(), "v");
}

// AUTH with wrong password fails
#[test]
fn test_auth_wrong_password() {
    let port = free_port();
    start_server_with_password(port, "correctpass");
    let mut c = Conn::connect(port);

    let r = c.cmds(&["AUTH", "wrongpass"]);
    assert!(r.is_error());
    assert!(r.error_msg().contains("WRONGPASS"));

    // Still blocked
    assert!(c.cmds(&["SET", "k", "v"]).is_error());
}

// PING requires AUTH, same as real Redis (pre-auth allowlist is
// AUTH/HELLO/RESET/QUIT only).
#[test]
fn test_auth_ping_requires_auth() {
    let port = free_port();
    start_server_with_password(port, "mypass");
    let mut c = Conn::connect(port);

    let r = c.cmds(&["PING"]);
    assert!(r.is_error(), "PING should require AUTH, got: {:?}", r);
    assert!(r.error_msg().contains("NOAUTH"));
}

// Redis 6 ACL-style AUTH: AUTH username password — password is the third argument
#[test]
fn test_auth_acl_style() {
    let port = free_port();
    start_server_with_password(port, "mypassword");
    let mut c = Conn::connect(port);

    // AUTH default mypassword (ACL style with username)
    assert!(c.cmds(&["AUTH", "default", "mypassword"]).is_ok());
    assert!(c.cmds(&["SET", "k", "v"]).is_ok());
}

// ═════════════════════════════════════════════════════════════════════════════
// CLIENT COMMANDS
// ═════════════════════════════════════════════════════════════════════════════

// CLIENT SETNAME / GETNAME round-trip
#[test]
fn test_client_setname_getname() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Initially no name
    assert!(c.cmds(&["CLIENT", "GETNAME"]).is_nil());

    assert!(c.cmds(&["CLIENT", "SETNAME", "myconn"]).is_ok());
    assert_eq!(c.cmds(&["CLIENT", "GETNAME"]).as_str(), "myconn");

    // Update name
    assert!(c.cmds(&["CLIENT", "SETNAME", "newname"]).is_ok());
    assert_eq!(c.cmds(&["CLIENT", "GETNAME"]).as_str(), "newname");
}

// CLIENT SETNAME rejects names with spaces
#[test]
fn test_client_setname_invalid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["CLIENT", "SETNAME", "bad name"]);
    assert!(r.is_error());
}

// CLIENT ID returns a positive integer
#[test]
fn test_client_id() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let id = c.cmds(&["CLIENT", "ID"]).as_int();
    assert!(id > 0, "expected positive client ID, got {id}");

    // Each connection gets a different ID
    let mut c2 = Conn::connect(port);
    let id2 = c2.cmds(&["CLIENT", "ID"]).as_int();
    assert_ne!(id, id2, "expected different IDs for different connections");
}

// CLIENT LIST returns info about all connections
#[test]
fn test_client_list() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "SETNAME", "testconn"]);
    let list = c.cmds(&["CLIENT", "LIST"]);
    let text = list.as_str();

    // Should contain our connection
    assert!(text.contains("id="), "CLIENT LIST missing id field: {text}");
    assert!(
        text.contains("addr="),
        "CLIENT LIST missing addr field: {text}"
    );
    assert!(
        text.contains("cmd="),
        "CLIENT LIST missing cmd field: {text}"
    );
}

// CLIENT INFO returns info about the current connection
#[test]
fn test_client_info() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "SETNAME", "infotest"]);
    let info = c.cmds(&["CLIENT", "INFO"]);
    let text = info.as_str();

    assert!(text.contains("id="));
    assert!(text.contains("addr="));
    assert!(text.contains("qbuf=0"));
    assert!(text.contains("qbuf-free="));
}

/// `CLIENT LIST` reports the seconds a client has been idle, growing while it
/// stays quiet, restarting when it runs a command, and left alone by the
/// inspection itself.
#[test]
fn test_client_list_reports_and_preserves_idle_time() {
    let idle_field = |line: &str| -> u64 {
        client_list_field(line, "idle=")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("no idle= in {line:?}"))
    };

    let port = free_port();
    start_server(port);
    // An observer that does the `CLIENT LIST`; a separate connection that
    // goes idle and is watched.
    let mut observer = Conn::connect(port);
    let mut watched = Conn::connect(port);
    watched.cmds(&["CLIENT", "SETNAME", "idle-watched"]);

    let read_idle = |observer: &mut Conn| -> u64 {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains("name=idle-watched"))
            .unwrap_or_else(|| panic!("watched client missing from {listing:?}"))
            .to_string();
        idle_field(&line)
    };

    // Freshly idle: seconds at most, and never negative.
    let fresh = read_idle(&mut observer);
    assert!(fresh <= 1, "a just-created client reported idle={fresh}");

    // The clock runs: after ~2 s of quiet the client shows at least that.
    thread::sleep(Duration::from_millis(2200));
    let after_wait = read_idle(&mut observer);
    assert!(
        after_wait >= 2,
        "idle time did not grow with the clock: {after_wait}"
    );

    // Three more inspections in quick succession: reading the listing must
    // not count as the watched client's interaction, so idle cannot fall.
    for _ in 0..3 {
        let again = read_idle(&mut observer);
        assert!(
            again >= after_wait,
            "inspection reset the watched client's idle timer: {again} after {after_wait}"
        );
    }

    // A command from the watched client is an interaction: idle restarts.
    watched.cmds(&["PING"]);
    let reset = read_idle(&mut observer);
    assert!(
        reset <= 1,
        "a command should reset the idle timer, but it read {reset}"
    );
}

fn client_list_field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix(name))
}

#[test]
fn test_client_list_reports_partial_query_buffer() {
    let port = free_port();
    start_server(port);
    let mut partial = Conn::connect(port);
    let mut observer = Conn::connect(port);
    let partial_addr = format!("addr={}", partial.stream.local_addr().unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let idle_line = loop {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains(&partial_addr))
            .unwrap_or("")
            .to_string();
        if client_list_field(&line, "qbuf-free=").is_some() || std::time::Instant::now() >= deadline
        {
            break line;
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        client_list_field(&idle_line, "qbuf=") == Some("0")
            && client_list_field(&idle_line, "qbuf-free=") == Some("0"),
        "idle client should report no private query buffer: {idle_line}"
    );

    partial.cmds(&["CLIENT", "SETNAME", "partial-query"]);

    // Pipeline one complete command, then leave the next one short of its
    // terminating CRLF. Only the second request should remain in qbuf.
    partial
        .stream
        .write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING")
        .unwrap();
    assert_eq!(partial.read_value().as_str(), "PONG");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let line = loop {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains("name=partial-query"))
            .unwrap_or("")
            .to_string();
        if client_list_field(&line, "qbuf=") == Some("12") || std::time::Instant::now() >= deadline
        {
            break line;
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        client_list_field(&line, "qbuf=") == Some("12"),
        "partial query not reflected: {line}"
    );
    let qbuf_free = client_list_field(&line, "qbuf-free=")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert!(
        qbuf_free == 16 * 1024 - 12,
        "expected spare capacity after buffering the 12-byte partial request: {line}"
    );

    partial.stream.write_all(b"\r\n").unwrap();
    assert_eq!(partial.read_value().as_str(), "PONG");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let line = loop {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains("name=partial-query"))
            .unwrap_or("")
            .to_string();
        if client_list_field(&line, "qbuf=") == Some("0") || std::time::Instant::now() >= deadline {
            break line;
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        client_list_field(&line, "qbuf=") == Some("0"),
        "completed query still buffered: {line}"
    );
    assert_eq!(
        client_list_field(&line, "qbuf-free="),
        Some("16384"),
        "completed partial query should retain its private query buffer: {line}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    let line = loop {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains("name=partial-query"))
            .unwrap_or("")
            .to_string();
        if client_list_field(&line, "qbuf-free=") == Some("0")
            || std::time::Instant::now() >= deadline
        {
            break line;
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(client_list_field(&line, "qbuf="), Some("0"), "{line}");
    assert_eq!(
        client_list_field(&line, "qbuf-free="),
        Some("0"),
        "idle private query buffer was not released: {line}"
    );

    assert_eq!(partial.cmds(&["PING"]).as_str(), "PONG");
    let line = observer
        .cmds(&["CLIENT", "LIST"])
        .as_str()
        .lines()
        .find(|line| line.contains("name=partial-query"))
        .unwrap_or("")
        .to_string();
    assert_eq!(client_list_field(&line, "qbuf="), Some("0"), "{line}");
    assert_eq!(
        client_list_field(&line, "qbuf-free="),
        Some("0"),
        "a released reusable buffer must not be marked private again: {line}"
    );
}

#[test]
fn test_client_list_reserves_query_buffer_for_declared_large_argument() {
    let port = free_port();
    start_server(port);
    let mut client = Conn::connect(port);
    let mut observer = Conn::connect(port);
    client.cmds(&["CLIENT", "SETNAME", "large-query"]);

    // The command is incomplete: only its framing and declared 1 MB bulk
    // argument length have arrived. Redis reserves private query-buffer space
    // from that length before receiving the argument body.
    client
        .stream
        .write_all(b"*3\r\n$3\r\nset\r\n$1\r\na\r\n$1000000\r\n")
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let line = loop {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains("name=large-query"))
            .unwrap_or("")
            .to_string();
        let capacity = client_list_field(&line, "qbuf=")
            .and_then(|qbuf| qbuf.parse::<usize>().ok())
            .zip(client_list_field(&line, "qbuf-free=").and_then(|free| free.parse::<usize>().ok()))
            .map(|(qbuf, free)| qbuf + free);
        if capacity.is_some_and(|capacity| capacity > 1_000_000)
            || std::time::Instant::now() >= deadline
        {
            break line;
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(client_list_field(&line, "qbuf="), Some("30"), "{line}");
    let capacity = client_list_field(&line, "qbuf-free=")
        .and_then(|free| free.parse::<usize>().ok())
        .map(|free| free + 30);
    assert!(
        capacity.is_some_and(|capacity| capacity > 1_000_000),
        "declared large argument did not reserve private query-buffer capacity: {line}"
    );

    client.stream.write_all(b"a").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let line = loop {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains("name=large-query"))
            .unwrap_or("")
            .to_string();
        if client_list_field(&line, "qbuf=") == Some("31") || std::time::Instant::now() >= deadline
        {
            break line;
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(client_list_field(&line, "qbuf="), Some("31"), "{line}");
    let capacity = client_list_field(&line, "qbuf-free=")
        .and_then(|free| free.parse::<usize>().ok())
        .map(|free| free + 31);
    assert!(
        capacity.is_some_and(|capacity| capacity > 1_000_000),
        "partial large argument lost its reserved query-buffer capacity: {line}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    let line = loop {
        let listing = observer.cmds(&["CLIENT", "LIST"]);
        let line = listing
            .as_str()
            .lines()
            .find(|line| line.contains("name=large-query"))
            .unwrap_or("")
            .to_string();
        let capacity = client_list_field(&line, "qbuf=")
            .and_then(|qbuf| qbuf.parse::<usize>().ok())
            .zip(client_list_field(&line, "qbuf-free=").and_then(|free| free.parse::<usize>().ok()))
            .map(|(qbuf, free)| qbuf + free);
        if capacity.is_some_and(|capacity| capacity < 1_000_000)
            || std::time::Instant::now() >= deadline
        {
            break line;
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(client_list_field(&line, "qbuf="), Some("31"), "{line}");
    let capacity = client_list_field(&line, "qbuf-free=")
        .and_then(|free| free.parse::<usize>().ok())
        .map(|free| free + 31);
    assert!(
        capacity.is_some_and(|capacity| capacity < 1_000_000),
        "idle partial request retained excess query-buffer capacity: {line}"
    );
}

// CLIENT NO-EVICT is accepted
#[test]
fn test_client_no_evict() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "NO-EVICT", "ON"]).is_ok());
    assert!(c.cmds(&["CLIENT", "NO-EVICT", "OFF"]).is_ok());
}

// CLIENT KILL by ID — kills other connection, not self
#[test]
fn test_client_kill_by_id() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    let c2_id = c2.cmds(&["CLIENT", "ID"]).as_int();
    let r = c1.cmds(&["CLIENT", "KILL", "ID", &c2_id.to_string()]);
    // Should return 1 (killed one client) or an error — both are acceptable
    // depending on whether kill flag is fully implemented
    assert!(!r.is_nil());
}

// CLIENT KILL of the caller itself: skipped unless SKIPME no says otherwise,
// and the reply is how many were killed.
#[test]
fn test_client_kill_self() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let my_id = c.cmds(&["CLIENT", "ID"]).as_int().to_string();
    assert_eq!(c.cmds(&["CLIENT", "KILL", "ID", &my_id]).as_int(), 0);
    // Still connected.
    assert_eq!(c.cmds(&["PING"]).as_str(), "PONG");

    // With SKIPME no it is killed too — after it has been told so.
    assert_eq!(
        c.cmds(&["CLIENT", "KILL", "ID", &my_id, "SKIPME", "no"])
            .as_int(),
        1
    );
    let mut rest = Vec::new();
    c.stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(c.stream.read_to_end(&mut rest).unwrap_or(0), 0);
}

// CLIENT PAUSE blocks writes for the duration, UNPAUSE clears it early
#[test]
fn test_client_pause_unpause() {
    let _lock = pause_lock();
    let _reset = PauseGuard;
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Pause for 500ms
    assert!(c.cmds(&["CLIENT", "PAUSE", "500"]).is_ok());

    // Write on a second connection should block until pause expires
    // We use a thread with a timeout to detect the block
    let port2 = port;
    let handle = std::thread::spawn(move || {
        let mut c2 = Conn::connect(port2);
        let t0 = std::time::Instant::now();
        c2.cmds(&["SET", "pause_test_key", "val"]);
        t0.elapsed()
    });

    // Unpause early from first connection
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(c.cmds(&["CLIENT", "UNPAUSE"]).is_ok());

    let elapsed = handle.join().unwrap();
    // SET should have completed shortly after UNPAUSE, well under 500ms
    assert!(
        elapsed < std::time::Duration::from_millis(450),
        "SET took {elapsed:?}, expected to unblock before 500ms pause expired"
    );
}

// CLIENT PAUSE WRITE mode: reads pass through, writes block
#[test]
fn test_client_pause_write_mode() {
    let _lock = pause_lock();
    let _reset = PauseGuard;
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Seed a key before pausing
    c.cmds(&["SET", "readable", "yes"]);

    // Pause writes only for 2000ms — plenty of margin
    assert!(c.cmds(&["CLIENT", "PAUSE", "2000", "WRITE"]).is_ok());

    // GET should pass through immediately — reads are not paused
    let t0 = std::time::Instant::now();
    let val = c.cmds(&["GET", "readable"]);
    let read_elapsed = t0.elapsed();
    assert_eq!(val.as_str(), "yes");
    assert!(
        read_elapsed < std::time::Duration::from_millis(50),
        "GET blocked during WRITE-only pause: {read_elapsed:?}"
    );

    // SET on a second connection should block
    let port2 = port;
    let handle = std::thread::spawn(move || {
        let mut c2 = Conn::connect(port2);
        let t0 = std::time::Instant::now();
        c2.cmds(&["SET", "blocked_key", "val"]);
        t0.elapsed()
    });

    let elapsed = handle.join().unwrap();
    assert!(
        elapsed >= std::time::Duration::from_millis(300),
        "SET returned too fast during WRITE pause: {elapsed:?}"
    );

    // Explicitly unpause so the test doesn't hold up others for 2s
    c.cmds(&["CLIENT", "UNPAUSE"]);
}

fn expired_keys(c: &mut Conn) -> u64 {
    let info = c.cmds(&["INFO", "stats"]);
    info.as_str()
        .lines()
        .find_map(|line| line.strip_prefix("expired_keys:"))
        .expect("expired_keys in INFO stats")
        .trim()
        .parse()
        .unwrap()
}

// While writes are paused nothing expires, as in Redis (#22): a read takes a
// key whose time has passed as gone, but the key is not deleted or counted
// until the pause is over, the background sweep leaves it alone, and
// RANDOMKEY still answers with it rather than nil. The pause is the whole
// process's, so no other test's key expires meanwhile either; once it is
// over, theirs may, hence "at least two" at the end.
#[test]
fn test_client_pause_write_defers_expiry() {
    let _lock = pause_lock();
    let _reset = PauseGuard;
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["SET", "foo", "bar", "PX", "200"]).is_ok());
    assert!(c.cmds(&["SET", "bar", "foo", "PX", "200"]).is_ok());
    let before = expired_keys(&mut c);
    assert!(c.cmds(&["CLIENT", "PAUSE", "20000", "WRITE"]).is_ok());

    // Past the keys' time, and several turns of the sweep (every 100 ms).
    std::thread::sleep(Duration::from_millis(600));
    assert!(c.cmds(&["GET", "foo"]).is_nil());
    assert!(c.cmds(&["GET", "bar"]).is_nil());
    assert_eq!(c.cmds(&["EXISTS", "foo", "bar"]).as_int(), 0);
    assert_eq!(c.cmds(&["TTL", "foo"]).as_int(), -2);
    assert_eq!(c.cmds(&["TYPE", "bar"]).as_str(), "none");
    assert!(c.cmds(&["KEYS", "*"]).as_array().is_empty());
    let picked = c.cmds(&["RANDOMKEY"]);
    assert!(
        !picked.is_nil() && matches!(picked.as_str(), "foo" | "bar"),
        "RANDOMKEY during a write pause should name an expired key, got {picked:?}"
    );
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        expired_keys(&mut c),
        before,
        "a key was deleted as expired while writes were paused"
    );

    assert!(c.cmds(&["CLIENT", "UNPAUSE"]).is_ok());
    // Now they go, on the next read if the sweep has not been first.
    assert!(c.cmds(&["GET", "foo"]).is_nil());
    assert!(c.cmds(&["GET", "bar"]).is_nil());
    let after = expired_keys(&mut c);
    assert!(
        after >= before + 2,
        "expected the two keys counted once unpaused: {before} -> {after}"
    );
    assert!(c.cmds(&["RANDOMKEY"]).is_nil());
}

// CLIENT TRACKING stub
#[test]
fn test_client_tracking_stub() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "TRACKING", "OFF"]).is_ok());
}

// CLIENT TRACKINGINFO returns expected shape
#[test]
fn test_client_trackinginfo() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = r.as_array();
    // Should have at least flags and redirect fields
    assert!(arr.len() >= 2);
}

// CLIENT SETINFO is accepted
#[test]
fn test_client_setinfo() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "SETINFO", "LIB-NAME", "mylib"]).is_ok());
    assert!(c.cmds(&["CLIENT", "SETINFO", "LIB-VER", "1.0.0"]).is_ok());
}

// ═════════════════════════════════════════════════════════════════════════════
// EDGE CASES AND INTEGRATION SCENARIOS
// ═════════════════════════════════════════════════════════════════════════════

// Transaction with a failing command: error is returned inline, others succeed
//   Redis spec: errors inside EXEC don't abort the transaction
#[test]
fn test_multi_exec_error_inline() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "txerr:k", "not_an_int"]);

    assert!(c.cmds(&["MULTI"]).is_ok());
    assert_eq!(c.cmds(&["INCR", "txerr:k"]).as_str(), "QUEUED"); // will error at exec
    assert_eq!(c.cmds(&["SET", "txerr:ok", "worked"]).as_str(), "QUEUED");
    assert_eq!(c.cmds(&["GET", "txerr:ok"]).as_str(), "QUEUED");

    let results = c.cmds(&["EXEC"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 3);
    assert!(arr[0].is_error()); // INCR on non-integer → error
    assert!(arr[1].is_ok()); // SET succeeded
    assert_eq!(arr[2].as_str(), "worked"); // GET returned value
}

// COPY missing source returns 0
#[test]
fn test_copy_missing_source() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["COPY", "nosuch:src", "nosuch:dst"]).as_int(), 0);
}

// RENAME overwrites destination atomically
#[test]
fn test_rename_overwrites_destination() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "ren:src", "srcval"]);
    c.cmds(&["SET", "ren:dst", "old_dst_val"]);

    assert!(c.cmds(&["RENAME", "ren:src", "ren:dst"]).is_ok());
    assert!(c.cmds(&["GET", "ren:src"]).is_nil());
    assert_eq!(c.cmds(&["GET", "ren:dst"]).as_str(), "srcval");
}

// Transactions interact correctly with FLUSHDB
#[test]
fn test_multi_exec_with_flushdb() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "pre:k", "before"]);

    assert!(c.cmds(&["MULTI"]).is_ok());
    assert_eq!(c.cmds(&["SET", "post:k", "after"]).as_str(), "QUEUED");
    assert_eq!(c.cmds(&["FLUSHDB"]).as_str(), "QUEUED");
    assert_eq!(c.cmds(&["GET", "post:k"]).as_str(), "QUEUED");

    let results = c.cmds(&["EXEC"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 3);
    assert!(arr[0].is_ok()); // SET
    assert!(arr[1].is_ok()); // FLUSHDB
    assert!(arr[2].is_nil()); // GET after FLUSHDB returns nil
}

// CLIENT ID is stable within a connection session
#[test]
fn test_client_id_stable() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let id1 = c.cmds(&["CLIENT", "ID"]).as_int();
    c.cmds(&["SET", "some:key", "val"]);
    let id2 = c.cmds(&["CLIENT", "ID"]).as_int();
    assert_eq!(id1, id2, "client ID changed within same connection");
}

// WATCH on multiple keys — any modification aborts
#[test]
fn test_watch_multiple_keys_any_aborts() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c1.cmds(&["SET", "mw:a", "a"]);
    c1.cmds(&["SET", "mw:b", "b"]);
    c1.cmds(&["SET", "mw:c", "c"]);

    // Watch all three
    assert!(c1.cmds(&["WATCH", "mw:a", "mw:b", "mw:c"]).is_ok());

    // Only modify mw:b from another connection
    c2.cmds(&["SET", "mw:b", "modified"]);

    assert!(c1.cmds(&["MULTI"]).is_ok());
    assert_eq!(c1.cmds(&["SET", "mw:a", "new_a"]).as_str(), "QUEUED");
    let result = c1.cmds(&["EXEC"]);
    // Should be aborted because mw:b was modified
    assert!(result.is_nil());
}

// ═════════════════════════════════════════════════════════════════════════════
// EXPIRETIME / PEXPIRETIME
// ═════════════════════════════════════════════════════════════════════════════

// EXPIRETIME on a key that doesn't exist → -2
#[test]
fn test_expiretime_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["EXPIRETIME", "nosuch"]).as_int(), -2);
    assert_eq!(c.cmds(&["PEXPIRETIME", "nosuch"]).as_int(), -2);
}

// EXPIRETIME on a key without a TTL → -1
#[test]
fn test_expiretime_no_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    assert_eq!(c.cmds(&["EXPIRETIME", "k"]).as_int(), -1);
    assert_eq!(c.cmds(&["PEXPIRETIME", "k"]).as_int(), -1);
}

// EXPIRETIME after EXPIREAT — round-trips with at most 1s drift
#[test]
fn test_expiretime_roundtrip_via_expireat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let target = now + 300;

    c.cmds(&["SET", "k", "v"]);
    assert_eq!(c.cmds(&["EXPIREAT", "k", &target.to_string()]).as_int(), 1);

    let got = c.cmds(&["EXPIRETIME", "k"]).as_int();
    let drift = (got - target).abs();
    assert!(drift <= 1, "EXPIRETIME drifted: target={target} got={got}");
}

// PEXPIRETIME after PEXPIREAT — round-trips with at most 50ms drift
#[test]
fn test_pexpiretime_roundtrip_via_pexpireat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let target_ms = now_ms + 300_000;

    c.cmds(&["SET", "k", "v"]);
    assert_eq!(
        c.cmds(&["PEXPIREAT", "k", &target_ms.to_string()]).as_int(),
        1
    );

    let got = c.cmds(&["PEXPIRETIME", "k"]).as_int();
    let drift = (got - target_ms).abs();
    assert!(
        drift <= 50,
        "PEXPIRETIME drifted: target={target_ms} got={got}"
    );
}

// EXPIRETIME after EXPIRE (relative TTL) — should be roughly now + N
#[test]
fn test_expiretime_after_relative_expire() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v"]);
    c.cmds(&["EXPIRE", "k", "120"]);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let got = c.cmds(&["EXPIRETIME", "k"]).as_int();

    // Should be in the window [now+118, now+121] — allow generous slack
    // for slow CI runners.
    assert!(
        got >= now + 118 && got <= now + 121,
        "EXPIRETIME out of window: now={now} got={got}"
    );
}

// EXPIRETIME on a key whose TTL has already elapsed — lazy-evicts and returns -2
#[test]
fn test_expiretime_already_expired() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v"]);
    c.cmds(&["PEXPIRE", "k", "50"]);
    std::thread::sleep(std::time::Duration::from_millis(120));

    assert_eq!(c.cmds(&["EXPIRETIME", "k"]).as_int(), -2);
    // After the call, the key should be gone (lazy eviction).
    assert!(c.cmds(&["GET", "k"]).is_nil());
}

// Wrong arity — both commands take exactly one argument
#[test]
fn test_expiretime_arity() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["EXPIRETIME"]).is_error());
    assert!(c.cmds(&["EXPIRETIME", "a", "b"]).is_error());
    assert!(c.cmds(&["PEXPIRETIME"]).is_error());
    assert!(c.cmds(&["PEXPIRETIME", "a", "b"]).is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// CLIENT TRACKING / CLIENT CACHING (client-side cache invalidation)
// ═══════════════════════════════════════════════════════════════════════════════
//
// Invalidation messages are RESP3 push frames sent out-of-band to the tracking
// client's socket after a write on a tracked key. Because the Conn harness is
// synchronous, we read them with a short timeout after triggering the write.

/// Try to read one RESP value with a short timeout.
/// Returns None if nothing arrives within the deadline.
fn try_read_with_timeout(c: &mut Conn, timeout: Duration) -> Option<Vec<u8>> {
    c.stream.set_read_timeout(Some(timeout)).unwrap();
    let mut buf = vec![0u8; 512];
    let result = c.stream.read(&mut buf);
    c.stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    match result {
        Ok(n) if n > 0 => Some(buf[..n].to_vec()),
        _ => None,
    }
}

/// Switch `c` to RESP3.
///
/// An invalidation reaches the connection that is tracking only as a push
/// message, which RESP2 cannot carry: a RESP2 client that tracks has to name
/// another connection to be told on (`REDIRECT`), and is otherwise sent
/// nothing — as in Redis. The tests of what is and is not invalidated are
/// about a connection that can hear it.
fn resp3(c: &mut Conn) {
    let hello = c.cmds(&["HELLO", "3"]);
    assert!(matches!(hello, RespValue::Array(_)), "{hello:?}");
}

/// A tracking client that does not read its invalidations is closed once
/// too many wait for it, rather than having them dropped: one dropped would
/// leave it serving a key that changed, with nothing to tell it so (#161).
#[test]
fn test_a_tracking_client_that_does_not_read_is_closed_not_left_stale() {
    let port = free_port();
    start_server(port);
    let mut tracker = Conn::connect(port);
    resp3(&mut tracker);
    let id = tracker.cmds(&["CLIENT", "ID"]).as_int();
    assert!(tracker.cmds(&["CLIENT", "TRACKING", "ON", "BCAST"]).is_ok());

    // Every write is an invalidation for the tracker, which reads none of
    // them: its socket fills, then its queue.
    let mut writer = Conn::connect(port);
    let batch = 1000;
    for round in 0..200 {
        let mut buf = Vec::new();
        for i in 0..batch {
            let key = format!("k{}", round * batch + i);
            buf.extend_from_slice(
                format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len()).as_bytes(),
            );
        }
        writer.stream.write_all(&buf).unwrap();
        for _ in 0..batch {
            assert!(writer.read_value().is_ok());
        }
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let list = writer.cmds(&["CLIENT", "LIST"]);
        if !list.as_str().contains(&format!("id={id} ")) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the tracker that reads nothing is still connected"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

// CLIENT TRACKING ON → returns OK
#[test]
fn test_client_tracking_on_returns_ok() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "TRACKING", "ON"]).is_ok());
}

// CLIENT TRACKING OFF → returns OK
#[test]
fn test_client_tracking_off_returns_ok() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "TRACKING", "OFF"]).is_ok());
}

// CLIENT TRACKING ON → TRACKINGINFO reflects state
#[test]
fn test_trackinginfo_reflects_on_state() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Before enabling: flags should contain "off"
    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        flags.contains(&"off"),
        "expected off before enabling, got {flags:?}"
    );

    // Enable tracking
    assert!(c.cmds(&["CLIENT", "TRACKING", "ON"]).is_ok());

    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        flags.contains(&"on"),
        "expected on after enabling, got {flags:?}"
    );
    assert!(!flags.contains(&"off"));
}

// TRACKINGINFO after OFF returns "off" again
#[test]
fn test_trackinginfo_reflects_off_state() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "TRACKING", "ON"]);
    c.cmds(&["CLIENT", "TRACKING", "OFF"]);

    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(flags.contains(&"off"));
}

// TRACKINGINFO with BCAST shows "bcast" flag
#[test]
fn test_trackinginfo_bcast_flag() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "TRACKING", "ON", "BCAST"]);

    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        flags.contains(&"bcast"),
        "expected bcast flag, got {flags:?}"
    );
}

// TRACKINGINFO with REDIRECT shows the redirect client ID
#[test]
fn test_trackinginfo_redirect() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    let c1_id = c1.cmds(&["CLIENT", "ID"]).as_int();

    // c2 redirects invalidations to c1
    assert!(
        c2.cmds(&["CLIENT", "TRACKING", "ON", "REDIRECT", &c1_id.to_string()])
            .is_ok()
    );

    let info = c2.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    assert_eq!(
        map["redirect"].as_int(),
        c1_id,
        "redirect should point to c1"
    );
}

// Re-sending TRACKING ON must not double-count TRACKING_CLIENT_COUNT
// (verified indirectly: TRACKINGINFO still shows "on" and no panic/corruption)
#[test]
fn test_tracking_on_idempotent() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "TRACKING", "ON"]).is_ok());
    assert!(c.cmds(&["CLIENT", "TRACKING", "ON"]).is_ok()); // re-send

    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(flags.contains(&"on"));

    // Disable once — should go off cleanly (not need two OFFs)
    c.cmds(&["CLIENT", "TRACKING", "OFF"]);
    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        flags.contains(&"off"),
        "one OFF should be enough after idempotent ON"
    );
}

// Core invalidation test:
// c1 enables tracking, reads "foo", c2 writes "foo" →
// c1 receives an invalidation push message.
#[test]
fn test_invalidation_received_after_tracked_key_written() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    // Seed the key
    c2.cmds(&["SET", "foo", "bar"]);

    // c1 enables tracking and reads the key
    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON"]);
    assert_eq!(c1.cmds(&["GET", "foo"]).as_str(), "bar");

    // c2 writes the key — should trigger invalidation to c1
    c2.cmds(&["SET", "foo", "newval"]);

    // Give the server a moment to push the invalidation
    std::thread::sleep(Duration::from_millis(50));

    // c1 should receive an invalidation push frame
    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(500));
    assert!(
        msg.is_some(),
        "c1 should have received an invalidation push after c2 wrote 'foo'"
    );
}

// The tests above only assert that *something* arrived — which is why a
// malformed frame went unnoticed. These two pin the actual bytes on the wire,
// per protocol.

// RESP2: the invalidation goes to the connection named with REDIRECT, which
// has subscribed to __redis__:invalidate, as an ordinary pub/sub message. A
// `>` push frame is unparseable by a RESP2 client.
//
// The tracking connection itself is sent nothing, and neither is a RESP2
// connection that tracks without REDIRECT: it could not tell the message
// from the reply to its next command. That is Redis's behaviour; kividb used
// to write the pub/sub frame to such a connection unasked.
#[test]
fn test_invalidation_frame_bytes_resp2() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);
    let mut told = Conn::connect(port);

    c2.cmds(&["SET", "wire2", "v1"]);
    let told_id = told.cmds(&["CLIENT", "ID"]).as_int().to_string();
    told.cmds(&["SUBSCRIBE", "__redis__:invalidate"]);
    assert!(
        c1.cmds(&["CLIENT", "TRACKING", "ON", "REDIRECT", &told_id])
            .is_ok()
    );
    c1.cmds(&["GET", "wire2"]);

    c2.cmds(&["SET", "wire2", "v2"]);
    std::thread::sleep(Duration::from_millis(50));

    let msg = try_read_with_timeout(&mut told, Duration::from_millis(500))
        .expect("the redirect target should receive an invalidation");
    assert_eq!(
        msg,
        b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$5\r\nwire2\r\n".to_vec(),
        "RESP2 invalidation must be a pub/sub message frame"
    );
    assert!(
        try_read_with_timeout(&mut c1, Duration::from_millis(200)).is_none(),
        "the tracking connection itself is sent nothing"
    );
}

// A RESP2 connection that tracks without REDIRECT is sent nothing at all.
#[test]
fn test_resp2_tracking_without_redirect_is_sent_nothing() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "quiet", "v1"]);
    assert!(c1.cmds(&["CLIENT", "TRACKING", "ON"]).is_ok());
    c1.cmds(&["GET", "quiet"]);
    c2.cmds(&["SET", "quiet", "v2"]);

    assert!(try_read_with_timeout(&mut c1, Duration::from_millis(300)).is_none());
    // And its next reply is the reply.
    assert_eq!(c1.cmds(&["GET", "quiet"]).as_str(), "v2");
}

// RESP3 target: must get a 2-element push frame with no literal "push" element.
#[test]
fn test_invalidation_frame_bytes_resp3() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "wire3", "v1"]);

    // HELLO 3 replies with a RESP3 map, which this harness's reader doesn't
    // parse — write it raw and drain the reply bytes instead.
    c1.stream
        .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
        .unwrap();
    let hello =
        try_read_with_timeout(&mut c1, Duration::from_millis(500)).expect("HELLO 3 should reply");
    assert!(
        hello.starts_with(b"%") || hello.starts_with(b"*"),
        "expected a HELLO map/array reply, got {:?}",
        String::from_utf8_lossy(&hello)
    );

    c1.cmds(&["CLIENT", "TRACKING", "ON"]);
    c1.cmds(&["GET", "wire3"]);

    c2.cmds(&["SET", "wire3", "v2"]);
    std::thread::sleep(Duration::from_millis(50));

    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(500))
        .expect("RESP3 client should receive an invalidation");
    assert_eq!(
        msg,
        b">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nwire3\r\n".to_vec(),
        "RESP3 invalidation must be a 2-element push frame (Redis addReplyPushLen(c,2))"
    );
}

// No invalidation when tracking is OFF
#[test]
fn test_no_invalidation_when_tracking_off() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "bar", "1"]);

    // c1 does NOT enable tracking
    c1.cmds(&["GET", "bar"]);

    c2.cmds(&["SET", "bar", "2"]);

    // c1 should receive nothing
    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        msg.is_none(),
        "c1 must not receive invalidation when tracking is off"
    );
}

// No invalidation after TRACKING OFF
#[test]
fn test_no_invalidation_after_tracking_disabled() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "baz", "1"]);

    // c1 enables, reads, then disables before the write
    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON"]);
    c1.cmds(&["GET", "baz"]);
    c1.cmds(&["CLIENT", "TRACKING", "OFF"]);

    c2.cmds(&["SET", "baz", "2"]);

    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        msg.is_none(),
        "c1 must not receive invalidation after tracking is disabled"
    );
}

// Invalidation is per-key: writing an untracked key does not produce a message
#[test]
fn test_no_invalidation_for_untracked_key() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "tracked", "1"]);
    c2.cmds(&["SET", "untracked", "1"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON"]);
    c1.cmds(&["GET", "tracked"]); // only "tracked" is registered

    c2.cmds(&["SET", "untracked", "2"]); // different key

    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        msg.is_none(),
        "writing an untracked key must not produce an invalidation"
    );
}

// After invalidation is sent, the key is deregistered — a second write
// on the same key should NOT produce a second invalidation
#[test]
fn test_invalidation_deregisters_key() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "once", "1"]);
    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON"]);
    c1.cmds(&["GET", "once"]);

    // First write — should produce invalidation
    c2.cmds(&["SET", "once", "2"]);
    std::thread::sleep(Duration::from_millis(50));
    let first = try_read_with_timeout(&mut c1, Duration::from_millis(300));
    assert!(first.is_some(), "first write should produce invalidation");

    // Second write — key was deregistered after first invalidation
    c2.cmds(&["SET", "once", "3"]);
    let second = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        second.is_none(),
        "second write must not produce a second invalidation — key should be deregistered"
    );
}

// Multiple keys can be tracked simultaneously
#[test]
fn test_multiple_tracked_keys() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "k1", "a"]);
    c2.cmds(&["SET", "k2", "b"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON"]);
    c1.cmds(&["GET", "k1"]);
    c1.cmds(&["GET", "k2"]);

    // Write k1
    c2.cmds(&["SET", "k1", "A"]);
    std::thread::sleep(Duration::from_millis(50));
    let inv1 = try_read_with_timeout(&mut c1, Duration::from_millis(300));
    assert!(inv1.is_some(), "k1 write should invalidate");

    // Write k2
    c2.cmds(&["SET", "k2", "B"]);
    std::thread::sleep(Duration::from_millis(50));
    let inv2 = try_read_with_timeout(&mut c1, Duration::from_millis(300));
    assert!(inv2.is_some(), "k2 write should invalidate");
}

// TRACKING ON arity check
#[test]
fn test_tracking_arity_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Missing ON/OFF argument
    assert!(c.cmds(&["CLIENT", "TRACKING"]).is_error());
}

// Invalid mode returns error
#[test]
fn test_tracking_invalid_mode_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "TRACKING", "MAYBE"]).is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// CLIENT TRACKING — OPTIN / OPTOUT / CLIENT CACHING
// ═══════════════════════════════════════════════════════════════════════════════

// ── TRACKINGINFO reflects optin flag ─────────────────────────────────────────

#[test]
fn test_trackinginfo_optin_flag() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);

    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(flags.contains(&"on"), "expected on, got {flags:?}");
    assert!(flags.contains(&"optin"), "expected optin, got {flags:?}");
    assert!(!flags.contains(&"optout"));
}

#[test]
fn test_trackinginfo_optout_flag() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "TRACKING", "ON", "OPTOUT"]);

    let info = c.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(flags.contains(&"on"));
    assert!(flags.contains(&"optout"), "expected optout, got {flags:?}");
    assert!(!flags.contains(&"optin"));
}

// ── CLIENT CACHING arity and value checks ─────────────────────────────────────

#[test]
fn test_client_caching_yes_no_accepted() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // `yes` is what an OPTIN client says and `no` an OPTOUT one; the other
    // word has nothing to act on, and is refused as Redis refuses it.
    c.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);
    assert!(c.cmds(&["CLIENT", "CACHING", "yes"]).is_ok());
    assert!(c.cmds(&["CLIENT", "CACHING", "no"]).is_error());

    c.cmds(&["CLIENT", "TRACKING", "OFF"]);
    c.cmds(&["CLIENT", "TRACKING", "ON", "OPTOUT"]);
    assert!(c.cmds(&["CLIENT", "CACHING", "no"]).is_ok());
    assert!(c.cmds(&["CLIENT", "CACHING", "yes"]).is_error());
}

#[test]
fn test_client_caching_invalid_value_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);
    assert!(c.cmds(&["CLIENT", "CACHING", "maybe"]).is_error());
    assert!(c.cmds(&["CLIENT", "CACHING", "1"]).is_error());
}

#[test]
fn test_client_caching_arity_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "CACHING"]).is_error());
}

// ── OPTIN mode ────────────────────────────────────────────────────────────────

// In OPTIN mode, a plain GET does NOT register the key — no invalidation.
#[test]
fn test_optin_no_invalidation_without_caching_yes() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "optin_key", "v1"]);

    // c1 enables OPTIN — reads do NOT auto-register
    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);
    c1.cmds(&["GET", "optin_key"]); // no CACHING yes before this

    c2.cmds(&["SET", "optin_key", "v2"]);

    // c1 should receive NO invalidation
    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        msg.is_none(),
        "OPTIN without CLIENT CACHING yes must not register the key"
    );
}

// In OPTIN mode, CLIENT CACHING yes before a read DOES register the key.
#[test]
fn test_optin_invalidation_with_caching_yes() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "optin_key2", "v1"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);
    // Opt in for the next read
    c1.cmds(&["CLIENT", "CACHING", "yes"]);
    c1.cmds(&["GET", "optin_key2"]);

    c2.cmds(&["SET", "optin_key2", "v2"]);
    std::thread::sleep(Duration::from_millis(50));

    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(400));
    assert!(
        msg.is_some(),
        "OPTIN with CLIENT CACHING yes must register the key and produce invalidation"
    );
}

// CLIENT CACHING yes is single-shot — second read is NOT registered.
#[test]
fn test_optin_caching_yes_is_single_shot() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "shot_a", "1"]);
    c2.cmds(&["SET", "shot_b", "1"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);
    c1.cmds(&["CLIENT", "CACHING", "yes"]);
    c1.cmds(&["GET", "shot_a"]); // registered
    c1.cmds(&["GET", "shot_b"]); // NOT registered — flag already consumed

    // Write shot_b first — should produce no invalidation
    c2.cmds(&["SET", "shot_b", "2"]);
    let msg_b = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        msg_b.is_none(),
        "shot_b must not be registered (caching yes was single-shot)"
    );

    // Write shot_a — should produce invalidation
    c2.cmds(&["SET", "shot_a", "2"]);
    std::thread::sleep(Duration::from_millis(50));
    let msg_a = try_read_with_timeout(&mut c1, Duration::from_millis(400));
    assert!(
        msg_a.is_some(),
        "shot_a must be registered (caching yes was sent before its read)"
    );
}

// Multiple CACHING yes calls each cover exactly one subsequent read.
#[test]
fn test_optin_caching_yes_per_read() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "per_a", "1"]);
    c2.cmds(&["SET", "per_b", "1"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);

    // Opt in for per_a
    c1.cmds(&["CLIENT", "CACHING", "yes"]);
    c1.cmds(&["GET", "per_a"]);

    // Opt in for per_b
    c1.cmds(&["CLIENT", "CACHING", "yes"]);
    c1.cmds(&["GET", "per_b"]);

    // Both should produce invalidations
    c2.cmds(&["SET", "per_a", "2"]);
    std::thread::sleep(Duration::from_millis(50));
    let inv_a = try_read_with_timeout(&mut c1, Duration::from_millis(400));
    assert!(inv_a.is_some(), "per_a should be registered");

    c2.cmds(&["SET", "per_b", "2"]);
    std::thread::sleep(Duration::from_millis(50));
    let inv_b = try_read_with_timeout(&mut c1, Duration::from_millis(400));
    assert!(inv_b.is_some(), "per_b should be registered");
}

// ── OPTOUT mode ───────────────────────────────────────────────────────────────

// In OPTOUT mode, plain GET auto-registers — invalidation fires normally.
#[test]
fn test_optout_auto_registers_reads() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "optout_key", "v1"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTOUT"]);
    c1.cmds(&["GET", "optout_key"]); // auto-registered

    c2.cmds(&["SET", "optout_key", "v2"]);
    std::thread::sleep(Duration::from_millis(50));

    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(400));
    assert!(
        msg.is_some(),
        "OPTOUT mode must auto-register reads like default mode"
    );
}

// In OPTOUT mode, CLIENT CACHING no before a read suppresses registration.
#[test]
fn test_optout_caching_no_suppresses_registration() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "optout_key2", "v1"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTOUT"]);
    // Opt out of tracking for the next read
    c1.cmds(&["CLIENT", "CACHING", "no"]);
    c1.cmds(&["GET", "optout_key2"]);

    c2.cmds(&["SET", "optout_key2", "v2"]);

    let msg = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        msg.is_none(),
        "OPTOUT with CLIENT CACHING no must suppress registration"
    );
}

// CLIENT CACHING no is single-shot — next read after it IS registered again.
#[test]
fn test_optout_caching_no_is_single_shot() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "opt_x", "1"]);
    c2.cmds(&["SET", "opt_y", "1"]);

    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTOUT"]);

    // Suppress opt_x
    c1.cmds(&["CLIENT", "CACHING", "no"]);
    c1.cmds(&["GET", "opt_x"]); // NOT registered
    c1.cmds(&["GET", "opt_y"]); // registered — caching no was consumed

    // opt_x write — no invalidation
    c2.cmds(&["SET", "opt_x", "2"]);
    let msg_x = try_read_with_timeout(&mut c1, Duration::from_millis(200));
    assert!(
        msg_x.is_none(),
        "opt_x must not be registered (caching no applied)"
    );

    // opt_y write — invalidation fires
    c2.cmds(&["SET", "opt_y", "2"]);
    std::thread::sleep(Duration::from_millis(50));
    let msg_y = try_read_with_timeout(&mut c1, Duration::from_millis(400));
    assert!(
        msg_y.is_some(),
        "opt_y must be registered (caching no was already consumed)"
    );
}

// ── Mode switch: OPTIN → default ──────────────────────────────────────────────

// Re-enabling tracking without OPTIN/OPTOUT resets to default mode.
#[test]
fn test_mode_switch_optin_to_default() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c2.cmds(&["SET", "sw_key", "v1"]);

    // Start in OPTIN — reads don't auto-register
    resp3(&mut c1);
    c1.cmds(&["CLIENT", "TRACKING", "ON", "OPTIN"]);
    c1.cmds(&["GET", "sw_key"]);

    c2.cmds(&["SET", "sw_key", "v2"]);
    let no_inv = try_read_with_timeout(&mut c1, Duration::from_millis(150));
    assert!(
        no_inv.is_none(),
        "OPTIN: should not register without CACHING yes"
    );

    // Switch to default mode
    c1.cmds(&["CLIENT", "TRACKING", "ON"]);
    c1.cmds(&["GET", "sw_key"]); // now auto-registered

    c2.cmds(&["SET", "sw_key", "v3"]);
    std::thread::sleep(Duration::from_millis(50));
    let inv = try_read_with_timeout(&mut c1, Duration::from_millis(400));
    assert!(inv.is_some(), "default mode: read must auto-register");

    // Verify TRACKINGINFO no longer shows optin
    let info = c1.cmds(&["CLIENT", "TRACKINGINFO"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> =
        arr.chunks(2).map(|p| (p[0].as_str(), &p[1])).collect();
    let flags: Vec<&str> = map["flags"].as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        !flags.contains(&"optin"),
        "optin flag must be cleared after re-enabling in default mode"
    );
}

// Without NOLOOP, the writer DOES receive its own invalidation
#[test]
fn test_self_invalidation_without_noloop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v1"]);
    resp3(&mut c);
    c.cmds(&["CLIENT", "TRACKING", "ON"]);
    c.cmds(&["GET", "k"]);

    // Write the SET command manually, then drain responses until we see
    // both the push frame and the +OK (in either order).
    let set_cmd = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv2\r\n";
    c.stream.write_all(set_cmd.as_bytes()).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    // Read two responses: one push, one +OK (order may vary)
    let mut got_push = false;
    let mut got_ok = false;
    for _ in 0..3 {
        let r = c.read_value();
        match &r {
            RespValue::Array(_) => got_push = true, // push frame parsed as array
            RespValue::SimpleStr(s) if s == "OK" => got_ok = true,
            _ => {}
        }
        if got_push && got_ok {
            break;
        }
    }

    assert!(
        got_push,
        "without NOLOOP, writer must receive self-invalidation push"
    );
    assert!(got_ok, "SET must still return OK");
}

// With NOLOOP, the writer does NOT receive its own invalidation
#[test]
fn test_no_self_invalidation_with_noloop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v1"]);
    resp3(&mut c);
    c.cmds(&["CLIENT", "TRACKING", "ON", "NOLOOP"]);
    c.cmds(&["GET", "k"]);
    c.cmds(&["SET", "k", "v2"]);

    let msg = try_read_with_timeout(&mut c, Duration::from_millis(200));
    assert!(
        msg.is_none(),
        "with NOLOOP, writer must not receive self-invalidation"
    );
}

// CLIENT GETREDIR returns -1 when no redirect is set
#[test]
fn test_client_getredir_no_redirect() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["CLIENT", "GETREDIR"]).as_int(), -1);
}

// CLIENT GETREDIR returns -1 when tracking is off
#[test]
fn test_client_getredir_tracking_off() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "TRACKING", "OFF"]);
    assert_eq!(c.cmds(&["CLIENT", "GETREDIR"]).as_int(), -1);
}

// CLIENT GETREDIR returns the redirect client ID when set
#[test]
fn test_client_getredir_with_redirect() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    let c1_id = c1.cmds(&["CLIENT", "ID"]).as_int();
    c2.cmds(&["CLIENT", "TRACKING", "ON", "REDIRECT", &c1_id.to_string()]);

    assert_eq!(
        c2.cmds(&["CLIENT", "GETREDIR"]).as_int(),
        c1_id,
        "GETREDIR must return the redirect client ID"
    );
}

// CLIENT GETREDIR returns -1 after TRACKING OFF clears the redirect
#[test]
fn test_client_getredir_clears_on_tracking_off() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    let c1_id = c1.cmds(&["CLIENT", "ID"]).as_int();
    c2.cmds(&["CLIENT", "TRACKING", "ON", "REDIRECT", &c1_id.to_string()]);
    assert_eq!(c2.cmds(&["CLIENT", "GETREDIR"]).as_int(), c1_id);

    c2.cmds(&["CLIENT", "TRACKING", "OFF"]);
    assert_eq!(
        c2.cmds(&["CLIENT", "GETREDIR"]).as_int(),
        -1,
        "GETREDIR must return -1 after TRACKING OFF"
    );
}

// CLIENT SETINFO LIB-NAME stores the value
#[test]
fn test_client_setinfo_lib_name() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "SETINFO", "LIB-NAME", "mylib"]).is_ok());

    let info = c.cmds(&["CLIENT", "INFO"]);
    assert!(
        info.as_str().contains("lib-name=mylib"),
        "CLIENT INFO must reflect LIB-NAME: {}",
        info.as_str()
    );
}

// CLIENT SETINFO LIB-VER stores the value
#[test]
fn test_client_setinfo_lib_ver() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "SETINFO", "LIB-VER", "1.2.3"]).is_ok());

    let info = c.cmds(&["CLIENT", "INFO"]);
    assert!(
        info.as_str().contains("lib-ver=1.2.3"),
        "CLIENT INFO must reflect LIB-VER: {}",
        info.as_str()
    );
}

// CLIENT SETINFO both fields visible in CLIENT LIST
#[test]
fn test_client_setinfo_visible_in_client_list() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["CLIENT", "SETINFO", "LIB-NAME", "testlib"]);
    c.cmds(&["CLIENT", "SETINFO", "LIB-VER", "9.9.9"]);

    let list = c.cmds(&["CLIENT", "LIST"]);
    let text = list.as_str();
    assert!(
        text.contains("lib-name=testlib"),
        "CLIENT LIST missing lib-name: {text}"
    );
    assert!(
        text.contains("lib-ver=9.9.9"),
        "CLIENT LIST missing lib-ver: {text}"
    );
}

// CLIENT SETINFO unknown field returns error
#[test]
fn test_client_setinfo_unknown_field_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["CLIENT", "SETINFO", "BOGUS-FIELD", "val"])
            .is_error()
    );
}

// CLIENT SETINFO arity check
#[test]
fn test_client_setinfo_arity() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CLIENT", "SETINFO", "LIB-NAME"]).is_error());
    assert!(c.cmds(&["CLIENT", "SETINFO"]).is_error());
}

/// A `BCAST` client is told only of the keys its user may read, by the user
/// it is now, as in Redis 8.10 (unit/tracking).
#[test]
fn test_bcast_tells_only_what_the_user_may_read() {
    let port = free_port();
    start_server(port);
    let mut admin = Conn::connect(port);
    for rule in [["usr_a", ">passA", "~a:*"], ["usr_b", ">passB", "~b:*"]] {
        let r = admin.cmds(&["ACL", "SETUSER", rule[0], "on", rule[1], rule[2], "+@all"]);
        assert!(r.is_ok(), "{r:?}");
    }
    let mut tc = Conn::connect(port);
    assert!(tc.cmds(&["AUTH", "usr_a", "passA"]).is_ok());
    tc.cmds(&["HELLO", "3"]);
    assert!(
        tc.cmds(&[
            "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "a:", "PREFIX", "b:"
        ])
        .is_ok()
    );

    let invalidated = |tc: &mut Conn| -> Vec<String> {
        let push = tc.read_value();
        let parts = push.as_array();
        assert_eq!(parts[0].as_str(), "invalidate", "{push:?}");
        parts[1]
            .as_array()
            .iter()
            .map(|k| k.as_str().to_string())
            .collect()
    };

    admin.cmds(&["SET", "a:1", "v"]);
    admin.cmds(&["SET", "b:1", "v"]);
    assert_eq!(invalidated(&mut tc), ["a:1"]);

    assert!(tc.cmds(&["AUTH", "usr_b", "passB"]).is_ok());
    admin.cmds(&["SET", "a:2", "v"]);
    admin.cmds(&["SET", "b:2", "v"]);
    assert_eq!(invalidated(&mut tc), ["b:2"]);

    // Of one command's keys, the ones it may read.
    admin.cmds(&["MSET", "a:3", "v", "b:3", "v"]);
    assert_eq!(invalidated(&mut tc), ["b:3"]);
    assert_eq!(tc.cmds(&["PING"]), RespValue::SimpleStr("PONG".into()));
}

/// A write and an `ACL SETUSER` that takes the key away, in one
/// transaction: the BCAST client is told of the key under the permissions it
/// had when the key was written, as in Redis 8.10 (unit/tracking).
#[test]
fn test_bcast_pending_is_told_before_acl_setuser_changes_the_user() {
    let port = free_port();
    start_server(port);
    let mut admin = Conn::connect(port);
    assert!(
        admin
            .cmds(&["ACL", "SETUSER", "setu", "on", ">setpass", "~p:*", "+@all"])
            .is_ok()
    );
    let mut tc = Conn::connect(port);
    assert!(tc.cmds(&["AUTH", "setu", "setpass"]).is_ok());
    tc.cmds(&["HELLO", "3"]);
    assert!(
        tc.cmds(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "p:"])
            .is_ok()
    );

    admin.cmds(&["MULTI"]);
    admin.cmds(&["SET", "p:x", "1"]);
    admin.cmds(&["ACL", "SETUSER", "setu", "resetkeys", "~q:*"]);
    admin.cmds(&["EXEC"]);

    let push = tc.read_value();
    let parts = push.as_array();
    assert_eq!(parts[0].as_str(), "invalidate", "{push:?}");
    assert_eq!(parts[1].as_array()[0].as_str(), "p:x");
}

/// `CLIENT LIST` reports a client's transaction (`multi`, the commands it
/// has queued, -1 outside one) and the keys it watches (`watch`), and `INFO clients` the clients blocked with a
/// timeout. They were fixed (#158).
#[test]
fn test_client_list_reports_transactions_and_blocked_clients() {
    let field = |line: &str, name: &str| -> i64 {
        client_list_field(line, name)
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("no {name} in {line:?}"))
    };
    let port = free_port();
    start_server(port);
    let mut observer = Conn::connect(port);
    let mut client = Conn::connect(port);
    client.cmds(&["CLIENT", "SETNAME", "in-tx"]);
    let line = |observer: &mut Conn| -> String {
        observer
            .cmds(&["CLIENT", "LIST"])
            .as_str()
            .lines()
            .find(|line| line.contains("name=in-tx"))
            .expect("the client")
            .to_string()
    };
    let now = line(&mut observer);
    assert_eq!((field(&now, "multi="), field(&now, "watch=")), (-1, 0));
    // An idle client has given its query buffer back: nothing to count.
    assert_eq!(field(&now, "tot-mem="), 0, "{now}");

    client.cmds(&["WATCH", "w1", "w2"]);
    client.cmds(&["MULTI"]);
    client.cmds(&["SET", "w1", "1"]);
    client.cmds(&["INCR", "w2"]);
    let now = line(&mut observer);
    assert_eq!((field(&now, "multi="), field(&now, "watch=")), (2, 2));
    client.cmds(&["EXEC"]);
    let now = line(&mut observer);
    assert_eq!((field(&now, "multi="), field(&now, "watch=")), (-1, 0));

    // Blocked with a timeout: in the table while it waits.
    let in_table = |observer: &mut Conn| -> i64 {
        observer
            .cmds(&["INFO", "clients"])
            .as_str()
            .lines()
            .find_map(|line| line.strip_prefix("clients_in_timeout_table:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let before = in_table(&mut observer);
    let mut waiter = Conn::connect(port);
    let blocked = std::thread::spawn(move || waiter.cmds(&["BLPOP", "never-pushed", "3"]));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while in_table(&mut observer) <= before {
        assert!(std::time::Instant::now() < deadline, "never counted");
        thread::sleep(Duration::from_millis(20));
    }
    observer.cmds(&["LPUSH", "never-pushed", "x"]);
    blocked.join().unwrap();
}
