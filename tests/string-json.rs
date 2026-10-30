// Integration tests for string and JSON commands.
// Same harness pattern as vector_hash.rs — each test gets its own server
// on a random port so tests are fully isolated and can run in parallel.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

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
            // Some commands return integers as bulk strings (e.g. INCRBYFLOAT, JSON.NUMINCRBY)
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("not an int: {:?}", self),
        }
    }
    fn as_f64(&self) -> f64 {
        self.as_str().parse().unwrap()
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn error_str(&self) -> &str {
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
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

// ═════════════════════════════════════════════════════════════════════════════
// STRING COMMAND TESTS
// ═════════════════════════════════════════════════════════════════════════════

// Manual test scenario:
//   SET mykey hello        → OK
//   GET mykey              → "hello"
//   GET nosuchkey          → nil
//   SET mykey world        → OK  (overwrites)
//   GET mykey              → "world"
#[test]
fn test_set_get_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["SET", "k", "hello"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
    assert!(c.cmds(&["GET", "nosuchkey"]).is_nil());
    assert!(c.cmds(&["SET", "k", "world"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");
}

// Manual test scenario:
//   APPEND mykey "Hello"   → 5
//   APPEND mykey " World"  → 11
//   GET mykey              → "Hello World"
//   APPEND newkey "first"  → 5  (creates key)
#[test]
fn test_append() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["APPEND", "k", "Hello"]).as_int(), 5);
    assert_eq!(c.cmds(&["APPEND", "k", " World"]).as_int(), 11);
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "Hello World");
    // Creates key if missing
    assert_eq!(c.cmds(&["APPEND", "newkey", "first"]).as_int(), 5);
    assert_eq!(c.cmds(&["GET", "newkey"]).as_str(), "first");
}

// Manual test scenario:
//   SET mykey "Hello World"
//   STRLEN mykey           → 11
//   STRLEN nosuchkey       → 0
#[test]
fn test_strlen() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "Hello World"]);
    assert_eq!(c.cmds(&["STRLEN", "k"]).as_int(), 11);
    assert_eq!(c.cmds(&["STRLEN", "nosuchkey"]).as_int(), 0);
}

// Manual test scenario:
//   SET k "hello"
//   GETDEL k               → "hello"
//   GET k                  → nil  (deleted)
//   GETDEL nosuchkey       → nil
#[test]
fn test_getdel() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    assert_eq!(c.cmds(&["GETDEL", "k"]).as_str(), "hello");
    assert!(c.cmds(&["GET", "k"]).is_nil());
    assert!(c.cmds(&["GETDEL", "nosuchkey"]).is_nil());
}

// Manual test scenario:
//   SET k "hello"
//   GETSET k "world"       → "hello"  (returns old)
//   GET k                  → "world"  (now updated)
//   GETSET newkey "val"    → nil      (no old value)
#[test]
fn test_getset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    assert_eq!(c.cmds(&["GETSET", "k", "world"]).as_str(), "hello");
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");
    assert!(c.cmds(&["GETSET", "newkey", "val"]).is_nil());
    assert_eq!(c.cmds(&["GET", "newkey"]).as_str(), "val");
}

// Manual test scenario:
//   SETNX k "hello"        → 1  (set, key didn't exist)
//   SETNX k "world"        → 0  (not set, key exists)
//   GET k                  → "hello"  (unchanged)
#[test]
fn test_setnx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["SETNX", "k", "hello"]).as_int(), 1);
    assert_eq!(c.cmds(&["SETNX", "k", "world"]).as_int(), 0);
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
}

// Manual test scenario:
//   SETEX k 100 "hello"    → OK
//   GET k                  → "hello"
//   TTL k                  → (positive number ≤ 100)
//   PSETEX k2 5000 "world" → OK
//   TTL k2                 → (positive number, about 5s)
#[test]
fn test_setex_psetex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["SETEX", "k", "100", "hello"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl > 0 && ttl <= 100);

    assert!(c.cmds(&["PSETEX", "k2", "5000", "world"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k2"]).as_str(), "world");
    let ttl2 = c.cmds(&["TTL", "k2"]).as_int();
    assert!(ttl2 > 0 && ttl2 <= 5);
}

// Manual test scenario:
//   MSET k1 v1 k2 v2 k3 v3  → OK
//   MGET k1 k2 k3 nosuch     → ["v1", "v2", "v3", nil]
#[test]
fn test_mset_mget() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"])
            .is_ok()
    );
    let got = c.cmds(&["MGET", "k1", "k2", "k3", "nosuch"]);
    let arr = got.as_array();
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0].as_str(), "v1");
    assert_eq!(arr[1].as_str(), "v2");
    assert_eq!(arr[2].as_str(), "v3");
    assert!(arr[3].is_nil());
}

// Manual test scenario:
//   MSETNX k1 v1 k2 v2      → 1  (none existed)
//   MSETNX k2 x  k3 v3      → 0  (k2 exists, nothing written)
//   GET k3                   → nil  (not written due to k2 existing)
//   GET k2                   → "v2"  (unchanged)
#[test]
fn test_msetnx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["MSETNX", "k1", "v1", "k2", "v2"]).as_int(), 1);
    assert_eq!(c.cmds(&["MSETNX", "k2", "x", "k3", "v3"]).as_int(), 0);
    assert!(c.cmds(&["GET", "k3"]).is_nil());
    assert_eq!(c.cmds(&["GET", "k2"]).as_str(), "v2");
}

// Manual test scenario:
//   INCR counter             → 1  (starts at 0)
//   INCR counter             → 2
//   INCRBY counter 10        → 12
//   INCRBY counter -3        → 9
//   DECR counter             → 8
//   DECRBY counter 3         → 5
//   INCR strkey              → ERR (not an integer)
#[test]
fn test_incr_decr_variants() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["INCR", "n"]).as_int(), 1);
    assert_eq!(c.cmds(&["INCR", "n"]).as_int(), 2);
    assert_eq!(c.cmds(&["INCRBY", "n", "10"]).as_int(), 12);
    assert_eq!(c.cmds(&["INCRBY", "n", "-3"]).as_int(), 9);
    assert_eq!(c.cmds(&["DECR", "n"]).as_int(), 8);
    assert_eq!(c.cmds(&["DECRBY", "n", "3"]).as_int(), 5);

    c.cmds(&["SET", "strkey", "notanumber"]);
    assert!(c.cmds(&["INCR", "strkey"]).is_error());
    // At the edge: an error, and the value as it was; INCR wrapped to
    // i64::MIN (#124).
    c.cmds(&["SET", "max", "9223372036854775807"]);
    assert!(c.cmds(&["INCR", "max"]).is_error());
    assert_eq!(c.cmds(&["GET", "max"]).as_str(), "9223372036854775807");
}

// Manual test scenario:
//   SET k 10.5
//   INCRBYFLOAT k 0.1        → "10.6"
//   INCRBYFLOAT k -1.0       → "9.6"
//   INCRBYFLOAT newkey 3.14  → "3.14"  (creates key at 0)
#[test]
fn test_incrbyfloat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "10.5"]);
    let r1 = c.cmds(&["INCRBYFLOAT", "k", "0.1"]).as_f64();
    assert!((r1 - 10.6).abs() < 1e-9, "got {r1}");

    let r2 = c.cmds(&["INCRBYFLOAT", "k", "-1.0"]).as_f64();
    assert!((r2 - 9.6).abs() < 1e-9, "got {r2}");

    let r3 = c.cmds(&["INCRBYFLOAT", "newkey", "3.15"]).as_f64();
    assert!((r3 - 3.15).abs() < 1e-9, "got {r3}");
}

// Manual test scenario:
//   SET k "Hello, World!"
//   GETRANGE k 0 4           → "Hello"
//   GETRANGE k 7 11          → "World"
//   GETRANGE k -6 -1         → "World!"  (negative indices)
//   GETRANGE k 0 -1          → "Hello, World!"  (whole string)
//   SUBSTR k 0 4             → "Hello"  (alias)
#[test]
fn test_getrange_substr() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "Hello, World!"]);
    assert_eq!(c.cmds(&["GETRANGE", "k", "0", "4"]).as_str(), "Hello");
    assert_eq!(c.cmds(&["GETRANGE", "k", "7", "11"]).as_str(), "World");
    assert_eq!(c.cmds(&["GETRANGE", "k", "-6", "-1"]).as_str(), "World!");
    assert_eq!(
        c.cmds(&["GETRANGE", "k", "0", "-1"]).as_str(),
        "Hello, World!"
    );
    assert_eq!(c.cmds(&["SUBSTR", "k", "0", "4"]).as_str(), "Hello");
}

// Manual test scenario:
//   SET k "Hello World"
//   SETRANGE k 6 "Redis"     → 11  (returns new length)
//   GET k                    → "Hello Redis"
//   SETRANGE newkey 5 "abc"  → 8   (zero-pads: "\x00\x00\x00\x00\x00abc")
//   STRLEN newkey            → 8
#[test]
fn test_setrange() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "Hello World"]);
    assert_eq!(c.cmds(&["SETRANGE", "k", "6", "Redis"]).as_int(), 11);
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "Hello Redis");

    // Zero-padding when offset > current length
    assert_eq!(c.cmds(&["SETRANGE", "newkey", "5", "abc"]).as_int(), 8);
    assert_eq!(c.cmds(&["STRLEN", "newkey"]).as_int(), 8);
}

// Manual test scenario:
//   SET k "hello"
//   GETEX k EX 100           → "hello"  (sets TTL)
//   TTL k                    → (positive)
//   GETEX k PERSIST          → "hello"  (removes TTL)
//   TTL k                    → -1  (no expiry)
//   GETEX k PX 5000          → "hello"  (ms TTL)
//   GETEX nosuch EX 10       → nil
#[test]
fn test_getex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    assert_eq!(c.cmds(&["GETEX", "k", "EX", "100"]).as_str(), "hello");
    assert!(c.cmds(&["TTL", "k"]).as_int() > 0);

    assert_eq!(c.cmds(&["GETEX", "k", "PERSIST"]).as_str(), "hello");
    assert_eq!(c.cmds(&["TTL", "k"]).as_int(), -1);

    assert_eq!(c.cmds(&["GETEX", "k", "PX", "5000"]).as_str(), "hello");
    assert!(c.cmds(&["TTL", "k"]).as_int() > 0);

    assert!(c.cmds(&["GETEX", "nosuch", "EX", "10"]).is_nil());
}

// Manual test scenario:
//   GETEX k EXAT <future unix ts>   → value, TTL is positive
//   GETEX k EXAT <past unix ts>     → value, key effectively expired
//   GETEX k PXAT <future unix ms>   → value, TTL is positive
#[test]
fn test_getex_exat_pxat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    // EXAT: set expiry to 100 seconds from now as a unix timestamp
    let future_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 100;
    let future_str = future_secs.to_string();
    assert_eq!(
        c.cmds(&["GETEX", "k", "EXAT", &future_str]).as_str(),
        "hello"
    );
    assert!(c.cmds(&["TTL", "k"]).as_int() > 0);

    // PXAT: set expiry to 10 seconds from now in milliseconds
    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 10_000;
    let future_ms_str = future_ms.to_string();
    assert_eq!(
        c.cmds(&["GETEX", "k", "PXAT", &future_ms_str]).as_str(),
        "hello"
    );
    assert!(c.cmds(&["TTL", "k"]).as_int() > 0);
}

// Manual test scenario:
//   SET strkey "hello"
//   INCR strkey              → WRONGTYPE error
//   HSET hashkey f v
//   GET hashkey              → WRONGTYPE error
//   APPEND hashkey "x"       → WRONGTYPE error
#[test]
fn test_string_wrongtype_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "hello"]);
    assert!(c.cmds(&["HSET", "strkey", "f", "v"]).is_error());

    c.cmds(&["HSET", "hashkey", "f", "v"]);
    assert!(c.cmds(&["GET", "hashkey"]).is_error());
    assert!(c.cmds(&["APPEND", "hashkey", "x"]).is_error());
    assert!(c.cmds(&["STRLEN", "hashkey"]).is_error());
    assert!(c.cmds(&["INCR", "hashkey"]).is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// SET SUBCOMMAND TESTS
// ═════════════════════════════════════════════════════════════════════════════

// SET key value NX — only set if key does not exist
#[test]
fn test_set_nx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // NX on missing key → OK, key is set
    assert!(c.cmds(&["SET", "k", "hello", "NX"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");

    // NX on existing key → nil, value unchanged
    assert!(c.cmds(&["SET", "k", "world", "NX"]).is_nil());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
}

// SET key value XX — only set if key exists
#[test]
fn test_set_xx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // XX on missing key → nil, key not created
    assert!(c.cmds(&["SET", "k", "hello", "XX"]).is_nil());
    assert!(c.cmds(&["GET", "k"]).is_nil());

    // XX on existing key → OK, value updated
    c.cmds(&["SET", "k", "hello"]);
    assert!(c.cmds(&["SET", "k", "world", "XX"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");
}

// SET key value EX seconds
#[test]
fn test_set_ex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["SET", "k", "hello", "EX", "100"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl > 0 && ttl <= 100, "expected TTL in (0,100], got {ttl}");
}

// SET key value PX milliseconds
#[test]
fn test_set_px() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["SET", "k", "hello", "PX", "10000"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
    let pttl = c.cmds(&["PTTL", "k"]).as_int();
    assert!(
        pttl > 0 && pttl <= 10000,
        "expected PTTL in (0,10000], got {pttl}"
    );
}

// SET key value EXAT unix-timestamp
#[test]
fn test_set_exat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 100;
    assert!(
        c.cmds(&["SET", "k", "hello", "EXAT", &future.to_string()])
            .is_ok()
    );
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl > 0 && ttl <= 100, "expected TTL in (0,100], got {ttl}");

    // Past timestamp → key immediately expired
    let past = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 1;
    c.cmds(&["SET", "k2", "world", "EXAT", &past.to_string()]);
    assert!(c.cmds(&["GET", "k2"]).is_nil());
}

// SET key value PXAT unix-timestamp-ms
#[test]
fn test_set_pxat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 10_000;
    assert!(
        c.cmds(&["SET", "k", "hello", "PXAT", &future_ms.to_string()])
            .is_ok()
    );
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
    let pttl = c.cmds(&["PTTL", "k"]).as_int();
    assert!(
        pttl > 0 && pttl <= 10_000,
        "expected PTTL in (0,10000], got {pttl}"
    );
}

// SET key value KEEPTTL — preserve existing TTL
#[test]
fn test_set_keepttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello", "EX", "100"]);
    let ttl_before = c.cmds(&["TTL", "k"]).as_int();

    // KEEPTTL: value changes but TTL is preserved
    assert!(c.cmds(&["SET", "k", "world", "KEEPTTL"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");
    let ttl_after = c.cmds(&["TTL", "k"]).as_int();
    assert!(
        ttl_after > 0 && ttl_after <= ttl_before,
        "expected TTL preserved, before={ttl_before} after={ttl_after}"
    );
}

// SET key value GET — return old value
#[test]
fn test_set_get_option() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // GET on missing key → nil
    assert!(c.cmds(&["SET", "k", "hello", "GET"]).is_nil());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");

    // GET on existing key → returns old value
    assert_eq!(c.cmds(&["SET", "k", "world", "GET"]).as_str(), "hello");
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");
}

// SET NX + GET combination
#[test]
fn test_set_nx_get() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // NX + GET on missing key → nil (key is set)
    assert!(c.cmds(&["SET", "k", "hello", "NX", "GET"]).is_nil());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");

    // NX + GET on existing key → returns existing value, no update
    assert_eq!(
        c.cmds(&["SET", "k", "world", "NX", "GET"]).as_str(),
        "hello"
    );
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
}

// SET key value IFEQ cmp — only set if current value equals cmp
#[test]
fn test_set_ifeq() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    // IFEQ matching value → OK, value updated
    assert!(c.cmds(&["SET", "k", "world", "IFEQ", "hello"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");

    // IFEQ non-matching value → nil, value unchanged
    assert!(c.cmds(&["SET", "k", "new", "IFEQ", "wrong"]).is_nil());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");

    // IFEQ on missing key → nil
    assert!(c.cmds(&["SET", "nosuch", "val", "IFEQ", "x"]).is_nil());
}

// SET key value IFNE cmp — only set if current value does not equal cmp
#[test]
fn test_set_ifne() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    // IFNE non-matching value → OK, value updated
    assert!(c.cmds(&["SET", "k", "world", "IFNE", "other"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");

    // IFNE matching value → nil, value unchanged
    assert!(c.cmds(&["SET", "k", "new", "IFNE", "world"]).is_nil());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "world");

    // IFNE on missing key → set proceeds (absent != anything)
    assert!(c.cmds(&["SET", "nosuch", "val", "IFNE", "x"]).is_ok());
    assert_eq!(c.cmds(&["GET", "nosuch"]).as_str(), "val");
}

// SET with EX + NX together
#[test]
fn test_set_ex_nx_combined() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // NX + EX on missing key → set with TTL
    assert!(c.cmds(&["SET", "k", "hello", "NX", "EX", "100"]).is_ok());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
    assert!(c.cmds(&["TTL", "k"]).as_int() > 0);

    // NX + EX on existing key → nil, TTL unchanged
    assert!(c.cmds(&["SET", "k", "world", "NX", "EX", "999"]).is_nil());
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "hello");
}

// ═════════════════════════════════════════════════════════════════════════════
// JSON COMMAND TESTS
// ═════════════════════════════════════════════════════════════════════════════

// Manual test scenario:
//   JSON.SET doc $ '{"name":"alice","age":30}'   → OK
//   JSON.GET doc $                               → '{"name":"alice","age":30}'
//   JSON.GET doc $.name                          → '"alice"'
//   JSON.GET doc $.age                           → '30'
//   JSON.GET nosuch $                            → nil
// $.name now returns ["alice"], not "alice"
#[test]
fn test_json_set_get_root() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["JSON.SET", "doc", "$", r#"{"name":"alice","age":30}"#])
            .is_ok()
    );

    // $ path → returns [{...}], unwrap the outer array
    let raw = c.cmds(&["JSON.GET", "doc", "$"]).as_str().to_string();
    let outer: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let parsed = &outer[0];
    assert_eq!(parsed["name"], "alice");
    assert_eq!(parsed["age"], 30);

    let name_raw = c.cmds(&["JSON.GET", "doc", "$.name"]).as_str().to_string();
    let name_arr: serde_json::Value = serde_json::from_str(&name_raw).unwrap();
    assert_eq!(name_arr[0], "alice");

    assert!(c.cmds(&["JSON.GET", "nosuch", "$"]).is_nil());
}

// Manual test scenario:
//   JSON.SET doc $ '{"a":1}'   → OK
//   JSON.SET doc $.b '"hello"' → OK  (adds new field)
//   JSON.GET doc $             → {"a":1,"b":"hello"}
//   JSON.SET doc $.a '99'      → OK  (overwrites)
//   JSON.GET doc $.a           → 99
// ── test_json_set_subpath ─────────────────────────────────────────────────────
#[test]
fn test_json_set_subpath() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"a":1}"#]);
    assert!(c.cmds(&["JSON.SET", "doc", "$.b", r#""hello""#]).is_ok());

    // $ path → [{...}]
    let raw = c.cmds(&["JSON.GET", "doc", "$"]).as_str().to_string();
    let outer: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let parsed = &outer[0];
    assert_eq!(parsed["b"], "hello");

    c.cmds(&["JSON.SET", "doc", "$.a", "99"]);
    let a_raw = c.cmds(&["JSON.GET", "doc", "$.a"]).as_str().to_string();
    let a_arr: serde_json::Value = serde_json::from_str(&a_raw).unwrap();
    assert_eq!(a_arr[0], 99);
}

// Manual test scenario:
//   JSON.SET doc $ '{"x":1}'
//   JSON.SET doc $.y '2' NX  → OK   (y doesn't exist)
//   JSON.SET doc $.x '9' NX  → nil  (x already exists)
//   JSON.GET doc $.x         → 1    (unchanged)
//   JSON.SET doc $.x '9' XX  → OK   (x exists)
//   JSON.GET doc $.x         → 9
//   JSON.SET doc $.z '9' XX  → nil  (z doesn't exist)
// ── test_json_set_nx_xx ───────────────────────────────────────────────────────
#[test]
fn test_json_set_nx_xx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"x":1}"#]);

    assert!(
        c.cmds(&["JSON.SET", "doc", "$", r#"{"x":1}"#, "NX"])
            .is_nil()
    );
    assert!(c.cmds(&["JSON.SET", "doc", "$.y", "2", "NX"]).is_ok());
    assert!(c.cmds(&["JSON.SET", "doc", "$.x", "9", "NX"]).is_nil());

    // $.x → ["1"] — check via array unwrap
    let x_raw = c.cmds(&["JSON.GET", "doc", "$.x"]).as_str().to_string();
    let x_arr: serde_json::Value = serde_json::from_str(&x_raw).unwrap();
    assert_eq!(x_arr[0], 1);

    assert!(c.cmds(&["JSON.SET", "doc", "$.x", "9", "XX"]).is_ok());
    let x2_raw = c.cmds(&["JSON.GET", "doc", "$.x"]).as_str().to_string();
    let x2_arr: serde_json::Value = serde_json::from_str(&x2_raw).unwrap();
    assert_eq!(x2_arr[0], 9);

    assert!(c.cmds(&["JSON.SET", "doc", "$.z", "9", "XX"]).is_nil());
}

// Manual test scenario:
//   JSON.SET doc $ '{"name":"alice"}'
//   JSON.TYPE doc $       → "object"
//   JSON.TYPE doc $.name  → "string"
//   JSON.SET doc2 $ '{"n":42,"f":3.14,"b":true,"arr":[],"nil":null}'
//   JSON.TYPE doc2 $.n    → "integer"
//   JSON.TYPE doc2 $.f    → "number"
//   JSON.TYPE doc2 $.b    → "boolean"
//   JSON.TYPE doc2 $.arr  → "array"
//   JSON.TYPE doc2 $.nil  → "null"
// ── test_json_type ────────────────────────────────────────────────────────────
// JSON.TYPE with $ path returns an array of type strings
#[test]
fn test_json_type() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"name":"alice"}"#]);
    // $ → array with one element
    assert_eq!(
        c.cmds(&["JSON.TYPE", "doc", "$"]).as_array()[0].as_str(),
        "object"
    );
    assert_eq!(
        c.cmds(&["JSON.TYPE", "doc", "$.name"]).as_array()[0].as_str(),
        "string"
    );

    c.cmds(&[
        "JSON.SET",
        "doc2",
        "$",
        r#"{"n":42,"f":3.14,"b":true,"arr":[],"nil":null}"#,
    ]);
    assert_eq!(
        c.cmds(&["JSON.TYPE", "doc2", "$.n"]).as_array()[0].as_str(),
        "integer"
    );
    assert_eq!(
        c.cmds(&["JSON.TYPE", "doc2", "$.f"]).as_array()[0].as_str(),
        "number"
    );
    assert_eq!(
        c.cmds(&["JSON.TYPE", "doc2", "$.b"]).as_array()[0].as_str(),
        "boolean"
    );
    assert_eq!(
        c.cmds(&["JSON.TYPE", "doc2", "$.arr"]).as_array()[0].as_str(),
        "array"
    );
    assert_eq!(
        c.cmds(&["JSON.TYPE", "doc2", "$.nil"]).as_array()[0].as_str(),
        "null"
    );
    assert!(c.cmds(&["JSON.TYPE", "nosuch", "$"]).is_nil());
}

// Manual test scenario:
//   JSON.SET doc $ '{"name":"hello"}'
//   JSON.STRLEN doc $.name   → 5
//   JSON.STRAPPEND doc $.name '" world"'  → 11
//   JSON.GET doc $.name      → '"hello world"'
//   JSON.STRLEN doc $.name   → 11
// ── test_json_strlen_strappend ────────────────────────────────────────────────
// JSON.STRLEN with $ path returns array of lengths
#[test]
fn test_json_strlen_strappend() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"name":"hello"}"#]);

    // $.name → [5]
    assert_eq!(
        c.cmds(&["JSON.STRLEN", "doc", "$.name"]).as_array()[0].as_int(),
        5
    );

    assert_eq!(
        c.cmds(&["JSON.STRAPPEND", "doc", "$.name", r#"" world""#])
            .as_array()[0]
            .as_int(),
        11
    );

    // $.name → ["hello world"]
    let raw = c.cmds(&["JSON.GET", "doc", "$.name"]).as_str().to_string();
    let arr: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(arr[0], "hello world");

    assert_eq!(
        c.cmds(&["JSON.STRLEN", "doc", "$.name"]).as_array()[0].as_int(),
        11
    );
}

// Manual test scenario:
//   JSON.SET doc $ '{"active":true}'
//   JSON.TOGGLE doc $.active  → 0  (false)
//   JSON.TOGGLE doc $.active  → 1  (true)
//   JSON.GET doc $.active     → true
// ── test_json_toggle ──────────────────────────────────────────────────────────
// JSON.TOGGLE with $ path returns array
#[test]
fn test_json_toggle() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"active":true}"#]);
    assert_eq!(
        c.cmds(&["JSON.TOGGLE", "doc", "$.active"]).as_array()[0].as_int(),
        0
    );
    assert_eq!(
        c.cmds(&["JSON.TOGGLE", "doc", "$.active"]).as_array()[0].as_int(),
        1
    );

    let raw = c
        .cmds(&["JSON.GET", "doc", "$.active"])
        .as_str()
        .to_string();
    let arr: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(arr[0], true);
}

// Manual test scenario:
//   JSON.SET doc $ '{"score":10}'
//   JSON.NUMINCRBY doc $.score 5    → "15"
//   JSON.NUMINCRBY doc $.score -3   → "12"
//   JSON.NUMMULTBY doc $.score 2    → "24"
//   JSON.NUMMULTBY doc $.score 0.5  → "12"
// ── test_json_numincrby_nummultby ─────────────────────────────────────────────
// $ path returns array for NUMINCRBY/NUMMULTBY too
#[test]
fn test_json_numincrby_nummultby() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"score":10}"#]);

    // A JSONPath answers with the new values as one JSON array.
    assert_eq!(
        c.cmds(&["JSON.NUMINCRBY", "doc", "$.score", "5"]).as_str(),
        "[15]"
    );
    assert_eq!(
        c.cmds(&["JSON.NUMINCRBY", "doc", "$.score", "-3"]).as_str(),
        "[12]"
    );
    assert_eq!(
        c.cmds(&["JSON.NUMMULTBY", "doc", "$.score", "2"]).as_str(),
        "[24]"
    );
    assert_eq!(
        c.cmds(&["JSON.NUMMULTBY", "doc", "$.score", "0.5"])
            .as_str(),
        "[12]"
    );
    assert_eq!(
        c.cmds(&["JSON.NUMINCRBY", "doc", "$.score", "0.5"])
            .as_str(),
        "[12.5]"
    );
    // A legacy path answers with the new value on its own.
    assert_eq!(
        c.cmds(&["JSON.NUMINCRBY", "doc", ".score", "1"]).as_str(),
        "13.5"
    );
}

// Manual test scenario:
//   JSON.SET doc $ '{"a":1,"b":2}'
//   JSON.OBJKEYS doc $     → ["a","b"]
//   JSON.OBJLEN doc $      → 2
//   JSON.OBJLEN nosuch $   → nil
// ── test_json_objkeys_objlen ──────────────────────────────────────────────────
// $ path: OBJKEYS returns array of arrays, OBJLEN returns array of ints
#[test]
fn test_json_objkeys_objlen() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2}"#]);

    // OBJKEYS with $ → [[\"a\",\"b\"]]
    let outer = c.cmds(&["JSON.OBJKEYS", "doc", "$"]);
    let outer_arr = outer.as_array();
    assert_eq!(outer_arr.len(), 1); // one match (the root)
    let mut ks: Vec<&str> = outer_arr[0].as_array().iter().map(|v| v.as_str()).collect();
    ks.sort();
    assert_eq!(ks, vec!["a", "b"]);

    // OBJLEN with $ → [2]
    assert_eq!(
        c.cmds(&["JSON.OBJLEN", "doc", "$"]).as_array()[0].as_int(),
        2
    );
    assert!(c.cmds(&["JSON.OBJLEN", "nosuch", "$"]).is_nil());
}

// Manual test scenario:
//   JSON.SET doc $ '{"tags":["a","b","c"]}'
//   JSON.ARRLEN doc $.tags           → 3
//   JSON.ARRAPPEND doc $.tags '"d"' '"e"'  → 5
//   JSON.GET doc $.tags              → ["a","b","c","d","e"]
//   JSON.ARRPOP doc $.tags           → "\"e\""  (last element)
//   JSON.ARRPOP doc $.tags 0         → "\"a\""  (first element)
//   JSON.ARRLEN doc $.tags           → 3
#[test]
fn test_json_arrlen_arrappend_arrpop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"tags":["a","b","c"]}"#]);

    // $.tags → [3]
    assert_eq!(
        c.cmds(&["JSON.ARRLEN", "doc", "$.tags"]).as_array()[0].as_int(),
        3
    );

    assert_eq!(
        c.cmds(&["JSON.ARRAPPEND", "doc", "$.tags", r#""d""#, r#""e""#])
            .as_array()[0]
            .as_int(),
        5
    );

    let raw = c.cmds(&["JSON.GET", "doc", "$.tags"]).as_str().to_string();
    let arr: serde_json::Value = serde_json::from_str(&raw).unwrap();
    // $.tags → [["a","b","c","d","e"]]
    assert_eq!(arr[0], serde_json::json!(["a", "b", "c", "d", "e"]));

    assert_eq!(
        c.cmds(&["JSON.ARRPOP", "doc", "$.tags"]).as_array()[0].as_str(),
        r#""e""#
    );
    // A legacy path answers with the element itself.
    assert_eq!(
        c.cmds(&["JSON.ARRPOP", "doc", ".tags", "0"]).as_str(),
        r#""a""#
    );
    assert_eq!(
        c.cmds(&["JSON.ARRLEN", "doc", "$.tags"]).as_array()[0].as_int(),
        3
    );
}

// Manual test scenario:
//   JSON.SET doc $ '{"nums":[1,2,3,4,5]}'
//   JSON.ARRINSERT doc $.nums 2 '10' '11'  → 7  (inserted before index 2)
//   JSON.GET doc $.nums  → [1,2,10,11,3,4,5]
//   JSON.ARRTRIM doc $.nums 1 3            → 3  (keep indices 1-3)
//   JSON.GET doc $.nums  → [2,10,11]
#[test]
fn test_json_arrinsert_arrtrim() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"nums":[1,2,3,4,5]}"#]);
    assert_eq!(
        c.cmds(&["JSON.ARRINSERT", "doc", "$.nums", "2", "10", "11"])
            .as_array()[0]
            .as_int(),
        7
    );

    let raw = c.cmds(&["JSON.GET", "doc", "$.nums"]).as_str().to_string();
    let arr: serde_json::Value = serde_json::from_str(&raw).unwrap();
    // $.nums → [[1,2,10,11,3,4,5]]
    assert_eq!(arr[0], serde_json::json!([1, 2, 10, 11, 3, 4, 5]));

    assert_eq!(
        c.cmds(&["JSON.ARRTRIM", "doc", "$.nums", "1", "3"])
            .as_array()[0]
            .as_int(),
        3
    );
    let raw2 = c.cmds(&["JSON.GET", "doc", "$.nums"]).as_str().to_string();
    let arr2: serde_json::Value = serde_json::from_str(&raw2).unwrap();
    assert_eq!(arr2[0], serde_json::json!([2, 10, 11]));
}

// Manual test scenario:
//   JSON.SET doc $ '{"vals":[1,"hello",true,"hello",2]}'
//   JSON.ARRINDEX doc $.vals '"hello"'       → 1  (first occurrence)
//   JSON.ARRINDEX doc $.vals '"hello"' 2     → 3  (search from index 2)
//   JSON.ARRINDEX doc $.vals '"nothere"'     → -1
#[test]
fn test_json_arrindex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "JSON.SET",
        "doc",
        "$",
        r#"{"vals":[1,"hello",true,"hello",2]}"#,
    ]);
    assert_eq!(
        c.cmds(&["JSON.ARRINDEX", "doc", "$.vals", r#""hello""#])
            .as_array()[0]
            .as_int(),
        1
    );
    assert_eq!(
        c.cmds(&["JSON.ARRINDEX", "doc", "$.vals", r#""hello""#, "2"])
            .as_array()[0]
            .as_int(),
        3
    );
    assert_eq!(
        c.cmds(&["JSON.ARRINDEX", "doc", "$.vals", r#""nothere""#])
            .as_array()[0]
            .as_int(),
        -1
    );
    // A legacy path answers for the one array, as a plain integer.
    assert_eq!(
        c.cmds(&["JSON.ARRINDEX", "doc", ".vals", r#""hello""#])
            .as_int(),
        1
    );
}

// Manual test scenario:
//   JSON.SET doc $ '{"a":[1,2,3],"b":{"x":5},"n":42}'
//   JSON.CLEAR doc $.a    → 1  (array cleared)
//   JSON.GET doc $.a      → []
//   JSON.CLEAR doc $.b    → 1  (object cleared)
//   JSON.GET doc $.b      → {}
//   JSON.CLEAR doc $.n    → 1  (number → 0)
//   JSON.GET doc $.n      → 0
#[test]
fn test_json_clear() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "JSON.SET",
        "doc",
        "$",
        r#"{"a":[1,2,3],"b":{"x":5},"n":42}"#,
    ]);

    assert_eq!(c.cmds(&["JSON.CLEAR", "doc", "$.a"]).as_int(), 1);
    // $.a → [[]] after clear
    let a_raw = c.cmds(&["JSON.GET", "doc", "$.a"]).as_str().to_string();
    let a_arr: serde_json::Value = serde_json::from_str(&a_raw).unwrap();
    assert_eq!(a_arr[0], serde_json::json!([]));

    assert_eq!(c.cmds(&["JSON.CLEAR", "doc", "$.b"]).as_int(), 1);
    let b_raw = c.cmds(&["JSON.GET", "doc", "$.b"]).as_str().to_string();
    let b_arr: serde_json::Value = serde_json::from_str(&b_raw).unwrap();
    assert_eq!(b_arr[0], serde_json::json!({}));

    assert_eq!(c.cmds(&["JSON.CLEAR", "doc", "$.n"]).as_int(), 1);
    let n_raw = c.cmds(&["JSON.GET", "doc", "$.n"]).as_str().to_string();
    let n_arr: serde_json::Value = serde_json::from_str(&n_raw).unwrap();
    assert_eq!(n_arr[0], serde_json::json!(0));
}

// Manual test scenario:
//   JSON.SET doc $ '{"name":"alice","age":30}'
//   JSON.DEL doc $.age           → 1
//   JSON.GET doc $               → {"name":"alice"}
//   JSON.DEL doc $               → 1  (deletes whole key)
//   JSON.GET doc $               → nil
//   JSON.FORGET acts same as DEL
#[test]
fn test_json_del_forget() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"name":"alice","age":30}"#]);
    assert_eq!(c.cmds(&["JSON.DEL", "doc", "$.age"]).as_int(), 1);

    // $ path → [{...}]
    let raw = c.cmds(&["JSON.GET", "doc", "$"]).as_str().to_string();
    let outer: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let parsed = &outer[0];
    assert!(parsed.get("age").is_none());
    assert_eq!(parsed["name"], "alice");

    assert_eq!(c.cmds(&["JSON.DEL", "doc"]).as_int(), 1);
    assert!(c.cmds(&["JSON.GET", "doc", "$"]).is_nil());

    c.cmds(&["JSON.SET", "doc2", "$", r#"{"x":1}"#]);
    assert_eq!(c.cmds(&["JSON.FORGET", "doc2"]).as_int(), 1);
    assert!(c.cmds(&["JSON.GET", "doc2", "$"]).is_nil());
}

// Manual test scenario:
//   JSON.SET k1 $ '{"score":10}'
//   JSON.SET k2 $ '{"score":20}'
//   JSON.MGET k1 k2 nosuch $.score  → ["10", "20", nil]
#[test]
fn test_json_mget() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "k1", "$", r#"{"score":10}"#]);
    c.cmds(&["JSON.SET", "k2", "$", r#"{"score":20}"#]);

    let got = c.cmds(&["JSON.MGET", "k1", "k2", "nosuch", "$.score"]);
    let arr = got.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_str(), "[10]");
    assert_eq!(arr[1].as_str(), "[20]");
    assert!(arr[2].is_nil());

    // A legacy path gives each key's value as it is.
    let got = c.cmds(&["JSON.MGET", "k1", "k2", "nosuch", ".score"]);
    let arr = got.as_array();
    assert_eq!(arr[0].as_str(), "10");
    assert_eq!(arr[1].as_str(), "20");
    assert!(arr[2].is_nil());
}

// Manual test scenario:
//   JSON.MSET k1 $ '{"a":1}' k2 $ '{"b":2}'  → OK
//   JSON.GET k1 $  → {"a":1}
//   JSON.GET k2 $  → {"b":2}
#[test]
fn test_json_mset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&[
            "JSON.MSET",
            "k1",
            "$",
            r#"{"a":1}"#,
            "k2",
            "$",
            r#"{"b":2}"#
        ])
        .is_ok()
    );

    // $ path → [{...}]
    let outer1: serde_json::Value =
        serde_json::from_str(c.cmds(&["JSON.GET", "k1", "$"]).as_str()).unwrap();
    assert_eq!(outer1[0]["a"], 1);

    let outer2: serde_json::Value =
        serde_json::from_str(c.cmds(&["JSON.GET", "k2", "$"]).as_str()).unwrap();
    assert_eq!(outer2[0]["b"], 2);
}

// Manual test scenario:
//   JSON.SET doc $ '{"a":{"x":1},"b":2}'
//   JSON.MERGE doc $ '{"a":{"y":2},"c":3}'  → OK
//   JSON.GET doc $   → {"a":{"x":1,"y":2},"b":2,"c":3}  (merged)
//   JSON.MERGE doc $ '{"b":null}'            → OK  (null deletes key)
//   JSON.GET doc $.b → nil
#[test]
fn test_json_merge() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"a":{"x":1},"b":2}"#]);
    assert!(
        c.cmds(&["JSON.MERGE", "doc", "$", r#"{"a":{"y":2},"c":3}"#])
            .is_ok()
    );

    // $ path → [{...}]
    let raw = c.cmds(&["JSON.GET", "doc", "$"]).as_str().to_string();
    let outer: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let parsed = &outer[0];
    assert_eq!(parsed["a"]["x"], 1);
    assert_eq!(parsed["a"]["y"], 2);
    assert_eq!(parsed["c"], 3);

    assert!(c.cmds(&["JSON.MERGE", "doc", "$", r#"{"b":null}"#]).is_ok());
    // A JSONPath that matches nothing is an empty list, not nil.
    assert_eq!(c.cmds(&["JSON.GET", "doc", "$.b"]).as_str(), "[]");
}

// Manual test scenario:
//   JSON.SET doc $ '{"name":"alice"}'
//   JSON.DEBUG MEMORY doc $   → (positive integer, bytes)
//   JSON.DEBUG MEMORY nosuch  → nil
#[test]
fn test_json_debug_memory() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"name":"alice"}"#]);
    let mem = c.cmds(&["JSON.DEBUG", "MEMORY", "doc", "$"]).as_array()[0].as_int();
    assert!(mem > 0);
    assert_eq!(c.cmds(&["JSON.DEBUG", "MEMORY", "doc"]).as_int(), mem);
    assert!(c.cmds(&["JSON.DEBUG", "MEMORY", "nosuch", "$"]).is_nil());
}

// Manual test scenario:
//   SET strkey "hello"
//   JSON.GET strkey $   → WRONGTYPE error
//   JSON.SET strkey $ '{}' → WRONGTYPE error (key is a string)
#[test]
fn test_json_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "hello"]);
    assert!(c.cmds(&["JSON.GET", "strkey", "$"]).is_error());
    assert!(c.cmds(&["JSON.SET", "strkey", "$.x", "1"]).is_error());
}

// Manual test scenario:
//   JSON.SET doc $ '{"name":"alice"}'
//   TYPE doc   → "ReJSON-RL"  (not "string" or "hash")
#[test]
fn test_json_type_command() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"name":"alice"}"#]);
    assert_eq!(c.cmds(&["TYPE", "doc"]).as_str(), "ReJSON-RL");
}

// ═════════════════════════════════════════════════════════════════════════════
// GENERICS: millisecond-precision expiry commands
// ═════════════════════════════════════════════════════════════════════════════

// Manual scenario:
//   SET k "hello"
//   PEXPIRE k 5000       → 1  (TTL set in ms)
//   PTTL k               → positive number close to 5000
//   PTTL nosuch          → -2
//   SET k2 "world"
//   PTTL k2              → -1  (no expiry)
#[test]
fn test_pexpire_pttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    assert_eq!(c.cmds(&["PEXPIRE", "k", "5000"]).as_int(), 1);

    let pttl = c.cmds(&["PTTL", "k"]).as_int();
    assert!(
        pttl > 0 && pttl <= 5000,
        "expected 0 < PTTL <= 5000, got {pttl}"
    );

    // Key with no expiry
    c.cmds(&["SET", "k2", "world"]);
    assert_eq!(c.cmds(&["PTTL", "k2"]).as_int(), -1);

    // Missing key
    assert_eq!(c.cmds(&["PTTL", "nosuch"]).as_int(), -2);

    // PEXPIRE on missing key returns 0
    assert_eq!(c.cmds(&["PEXPIRE", "nosuch", "1000"]).as_int(), 0);
}

// Manual scenario:
//   SET k "hello"
//   EXPIREAT k <future unix secs>  → 1
//   TTL k                          → positive, roughly the offset we gave
//   EXPIREAT k <past unix secs>    → 1  (key immediately gone)
//   GET k                          → nil
#[test]
fn test_expireat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 100;
    assert_eq!(c.cmds(&["EXPIREAT", "k", &future.to_string()]).as_int(), 1);
    let ttl = c.cmds(&["TTL", "k"]).as_int();
    assert!(ttl > 0 && ttl <= 100, "expected TTL in (0,100], got {ttl}");

    // Past timestamp → key expires immediately on next access
    let past = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 10;
    c.cmds(&["SET", "k2", "world"]);
    assert_eq!(c.cmds(&["EXPIREAT", "k2", &past.to_string()]).as_int(), 1);
    assert!(c.cmds(&["GET", "k2"]).is_nil());

    // Missing key returns 0
    assert_eq!(
        c.cmds(&["EXPIREAT", "nosuch", &future.to_string()])
            .as_int(),
        0
    );
}

// Manual scenario:
//   SET k "hello"
//   PEXPIREAT k <future unix ms>  → 1
//   PTTL k                        → positive, close to the offset we gave
//   PEXPIREAT k <past unix ms>    → 1  (key immediately gone)
//   GET k                         → nil
#[test]
fn test_pexpireat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);

    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 10_000;
    assert_eq!(
        c.cmds(&["PEXPIREAT", "k", &future_ms.to_string()]).as_int(),
        1
    );
    let pttl = c.cmds(&["PTTL", "k"]).as_int();
    assert!(
        pttl > 0 && pttl <= 10_000,
        "expected PTTL in (0,10000], got {pttl}"
    );

    // Past timestamp
    let past_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 1000;
    c.cmds(&["SET", "k2", "world"]);
    assert_eq!(
        c.cmds(&["PEXPIREAT", "k2", &past_ms.to_string()]).as_int(),
        1
    );
    assert!(c.cmds(&["GET", "k2"]).is_nil());
}

// Manual scenario — TTL and PTTL agree (within rounding):
//   SET k "hello"
//   EXPIRE k 10
//   TTL k    → ~10
//   PTTL k   → ~10000  (same expiry, finer resolution)
#[test]
fn test_ttl_pttl_consistency() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    c.cmds(&["EXPIRE", "k", "10"]);

    let ttl = c.cmds(&["TTL", "k"]).as_int();
    let pttl = c.cmds(&["PTTL", "k"]).as_int();

    assert!(ttl > 0 && ttl <= 10);
    assert!(pttl > 0 && pttl <= 10_000);
    // PTTL should be roughly ttl * 1000 (within 1 second of slop)
    assert!(
        (pttl - ttl * 1000).abs() <= 1000,
        "PTTL {pttl} and TTL*1000 {} diverge by more than 1s",
        ttl * 1000
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// JSON WILDCARD PATH TESTS
// ═════════════════════════════════════════════════════════════════════════════

// Helper: parse a bulk string as JSON, panic with context on failure.
// Used throughout the wildcard tests where we need to inspect structure.
fn parse_json(v: &RespValue) -> serde_json::Value {
    serde_json::from_str(v.as_str())
        .unwrap_or_else(|e| panic!("failed to parse JSON {:?}: {e}", v.as_str()))
}

// Manual scenario — $.*  matches all object values:
//   JSON.SET doc $ '{"a":1,"b":2,"c":3}'
//   JSON.GET doc $.*      → [1,2,3]  (order not guaranteed)
//   JSON.TYPE doc $.*     → array of "integer" strings
#[test]
fn test_json_wildcard_object_children() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2,"c":3}"#]);

    let raw = c.cmds(&["JSON.GET", "doc", "$.*"]);
    let parsed = parse_json(&raw);
    let arr = parsed.as_array().expect("expected JSON array");
    assert_eq!(arr.len(), 3);
    // Values are 1, 2, 3 in any order
    let mut nums: Vec<i64> = arr.iter().map(|v| v.as_i64().unwrap()).collect();
    nums.sort();
    assert_eq!(nums, vec![1, 2, 3]);

    // JSON.TYPE with wildcard returns an array of type strings
    let types = c.cmds(&["JSON.TYPE", "doc", "$.*"]);
    let type_arr = types.as_array();
    assert_eq!(type_arr.len(), 3);
    assert!(type_arr.iter().all(|v| v.as_str() == "integer"));
}

// Manual scenario — $[*]  matches all array elements:
//   JSON.SET doc $ '[10,20,30]'
//   JSON.GET doc $[*]     → [10,20,30]
//   JSON.ARRLEN doc $     → [3]  ($ path → result wrapped in array)
#[test]
fn test_json_wildcard_array_elements() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"[10,20,30]"#]);

    let raw = c.cmds(&["JSON.GET", "doc", "$[*]"]);
    let parsed = parse_json(&raw);
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    let mut nums: Vec<i64> = arr.iter().map(|v| v.as_i64().unwrap()).collect();
    nums.sort();
    assert_eq!(nums, vec![10, 20, 30]);

    // ARRLEN via $ path returns array of lengths
    let len_resp = c.cmds(&["JSON.ARRLEN", "doc", "$"]);
    let len_arr = len_resp.as_array();
    assert_eq!(len_arr.len(), 1);
    assert_eq!(len_arr[0].as_int(), 3);
}

// Manual scenario — $.items[*].price  picks a field from every array element:
//   JSON.SET doc $ '{"items":[{"price":10},{"price":20},{"price":30}]}'
//   JSON.GET doc $.items[*].price  → [10,20,30]
//   JSON.NUMINCRBY doc $.items[*].price 5  → [15,25,35]
// ── test_json_wildcard_nested_field ──────────────────────────────────────────
// This one failed because $.items[*].price uses the multi-value path,
// and the as_f64() helper wasn't on RespValue. Add it to the test harness
// and fix the assertion to handle the array result from NUMINCRBY.
#[test]
fn test_json_wildcard_nested_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "JSON.SET",
        "doc",
        "$",
        r#"{"items":[{"price":10},{"price":20},{"price":30}]}"#,
    ]);

    let raw = c.cmds(&["JSON.GET", "doc", "$.items[*].price"]);
    let parsed: serde_json::Value = serde_json::from_str(raw.as_str()).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    let mut prices: Vec<i64> = arr.iter().map(|v| v.as_i64().unwrap()).collect();
    prices.sort();
    assert_eq!(prices, vec![10, 20, 30]);

    // NUMINCRBY answers with the new values as one JSON array
    let results = c.cmds(&["JSON.NUMINCRBY", "doc", "$.items[*].price", "5"]);
    assert_eq!(results.as_str(), "[15,25,35]");
}

// Manual scenario — $..field  recursive descent finds field at any depth:
//   JSON.SET doc $ '{"a":{"score":1},"b":{"score":2},"score":3}'
//   JSON.GET doc $..score  → [3,1,2]  (all "score" values, depth-first)
#[test]
fn test_json_recursive_descent_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "JSON.SET",
        "doc",
        "$",
        r#"{"a":{"score":1},"b":{"score":2},"score":3}"#,
    ]);

    let raw = c.cmds(&["JSON.GET", "doc", "$..score"]);
    let parsed = parse_json(&raw);
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 3, "expected 3 score values, got {arr:?}");

    let mut scores: Vec<i64> = arr.iter().map(|v| v.as_i64().unwrap()).collect();
    scores.sort();
    assert_eq!(scores, vec![1, 2, 3]);
}

// Manual scenario — $..field  deep nesting:
//   JSON.SET doc $ '{"x":{"y":{"z":{"val":42}}}}'
//   JSON.GET doc $..val  → [42]
#[test]
fn test_json_recursive_descent_deep() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"x":{"y":{"z":{"val":42}}}}"#]);

    let raw = c.cmds(&["JSON.GET", "doc", "$..val"]);
    let parsed = parse_json(&raw);
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].as_i64().unwrap(), 42);
}

// Manual scenario — TOGGLE with wildcard flips all booleans:
//   JSON.SET doc $ '{"a":true,"b":false,"c":true}'
//   JSON.TOGGLE doc $.*  → [0,1,0]  (all flipped)
//   JSON.GET doc $.*     → [false,true,false]
#[test]
fn test_json_wildcard_toggle() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"a":true,"b":false,"c":true}"#]);

    let results = c.cmds(&["JSON.TOGGLE", "doc", "$.*"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 3);
    // Should have two 0s (true→false) and one 1 (false→true)
    let mut vals: Vec<i64> = arr.iter().map(|v| v.as_int()).collect();
    vals.sort();
    assert_eq!(vals, vec![0, 0, 1]);

    // Verify the values actually changed in the document
    let raw = c.cmds(&["JSON.GET", "doc", "$.*"]);
    let parsed = parse_json(&raw);
    let bool_arr = parsed.as_array().unwrap();
    let mut bools: Vec<bool> = bool_arr.iter().map(|v| v.as_bool().unwrap()).collect();
    bools.sort();
    assert_eq!(bools, vec![false, false, true]);
}

// Manual scenario — STRLEN with wildcard on mixed types returns nulls for non-strings:
//   JSON.SET doc $ '{"name":"alice","age":30,"city":"paris"}'
//   JSON.STRLEN doc $.*  → [5, null, 5]  (order varies, null for integer)
#[test]
fn test_json_wildcard_strlen_mixed() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "JSON.SET",
        "doc",
        "$",
        r#"{"name":"alice","age":30,"city":"paris"}"#,
    ]);

    let results = c.cmds(&["JSON.STRLEN", "doc", "$.*"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 3);

    // Collect non-nil lengths and count nils
    let lengths: Vec<i64> = arr
        .iter()
        .filter(|v| !v.is_nil())
        .map(|v| v.as_int())
        .collect();
    let nil_count = arr.iter().filter(|v| v.is_nil()).count();

    let mut sorted_lengths = lengths.clone();
    sorted_lengths.sort();
    assert_eq!(sorted_lengths, vec![5, 5], "alice=5, paris=5");
    assert_eq!(nil_count, 1, "age:30 is not a string, should be nil");
}

// Manual scenario — OBJLEN with wildcard on array of objects:
//   JSON.SET doc $ '[{"a":1,"b":2},{"c":3}]'
//   JSON.OBJLEN doc $[*]  → [2, 1]
#[test]
fn test_json_wildcard_objlen() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"[{"a":1,"b":2},{"c":3}]"#]);

    let results = c.cmds(&["JSON.OBJLEN", "doc", "$[*]"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 2);
    let mut lens: Vec<i64> = arr.iter().map(|v| v.as_int()).collect();
    lens.sort();
    assert_eq!(lens, vec![1, 2]);
}

// Manual scenario — OBJKEYS with wildcard returns array of key arrays:
//   JSON.SET doc $ '[{"x":1,"y":2},{"z":3}]'
//   JSON.OBJKEYS doc $[*]  → [["x","y"], ["z"]]
#[test]
fn test_json_wildcard_objkeys() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"[{"x":1,"y":2},{"z":3}]"#]);

    let results = c.cmds(&["JSON.OBJKEYS", "doc", "$[*]"]);
    let outer = results.as_array();
    assert_eq!(outer.len(), 2);

    // First element's keys, sorted
    let mut first: Vec<&str> = outer[0].as_array().iter().map(|v| v.as_str()).collect();
    first.sort();
    assert_eq!(first, vec!["x", "y"]);

    // Second element's keys
    let second: Vec<&str> = outer[1].as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(second, vec!["z"]);
}

// Manual scenario — NUMMULTBY with recursive descent:
//   JSON.SET doc $ '{"prices":{"a":10,"b":20},"discount":{"a":5}}'
//   JSON.NUMMULTBY doc $..a 2  → [20, 10]  (both "a" fields doubled)
#[test]
fn test_json_recursive_nummultby() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "JSON.SET",
        "doc",
        "$",
        r#"{"prices":{"a":10,"b":20},"discount":{"a":5}}"#,
    ]);

    // In document order: prices.a, then discount.a.
    let results = c.cmds(&["JSON.NUMMULTBY", "doc", "$..a", "2"]);
    assert_eq!(results.as_str(), "[20,10]");
}

// Manual scenario — $ path always wraps in array, legacy . path returns scalar:
//   JSON.SET doc $ '{"n":42}'
//   JSON.GET doc $.n   → [42]   (array, JSONPath form)
//   JSON.GET doc .n    → 42    (scalar, legacy form)
//   JSON.TYPE doc $.n  → ["integer"]
//   JSON.TYPE doc .n   → "integer"
#[test]
fn test_json_jsonpath_vs_legacy_path() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"n":42}"#]);

    // $ path → result is a JSON array
    let jsonpath_result = parse_json(&c.cmds(&["JSON.GET", "doc", "$.n"]));
    assert!(jsonpath_result.is_array(), "$.n should return an array");
    assert_eq!(jsonpath_result.as_array().unwrap()[0].as_i64().unwrap(), 42);

    // Legacy . path → result is the value directly
    let legacy_result = parse_json(&c.cmds(&["JSON.GET", "doc", ".n"]));
    assert!(legacy_result.is_number(), ".n should return the scalar");
    assert_eq!(legacy_result.as_i64().unwrap(), 42);

    // JSON.TYPE $ → array of type strings
    let jsonpath_type = c.cmds(&["JSON.TYPE", "doc", "$.n"]);
    let type_arr = jsonpath_type.as_array();
    assert_eq!(type_arr.len(), 1);
    assert_eq!(type_arr[0].as_str(), "integer");

    // JSON.TYPE legacy → single type string
    let legacy_type = c.cmds(&["JSON.TYPE", "doc", ".n"]);
    assert_eq!(legacy_type.as_str(), "integer");
}

// Manual scenario — wildcard on missing path returns empty array, not nil:
//   JSON.SET doc $ '{"a":1}'
//   JSON.GET doc $.missing.*   → []
//   JSON.STRLEN doc $.missing  → []
#[test]
fn test_json_wildcard_no_match() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["JSON.SET", "doc", "$", r#"{"a":1}"#]);

    // Missing intermediate path — wildcard on nothing
    let raw = c.cmds(&["JSON.GET", "doc", "$.missing"]);
    // A JSONPath that matches nothing is an empty list
    assert_eq!(raw.as_str(), "[]");

    // Wildcard on a scalar returns empty array (no children)
    let raw2 = c.cmds(&["JSON.GET", "doc", "$.*"]);
    let parsed = parse_json(&raw2);
    // $.*  on {"a":1} should return [1] — the one child
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].as_i64().unwrap(), 1);
}

// ═════════════════════════════════════════════════════════════════════════════
// INCREX (Redis 8.8)
//
// Transcribed from the worked examples on the redis.io INCREX page.
// ═════════════════════════════════════════════════════════════════════════════

/// Default increment is 1, starting from 0 for a missing key.
#[test]
fn test_increx_default_increment() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["DEL", "mykey1"]);

    let r = c.cmds(&["INCREX", "mykey1"]);
    assert_eq!(r.as_array()[0].as_int(), 1);
    assert_eq!(r.as_array()[1].as_int(), 1);

    let r = c.cmds(&["INCREX", "mykey1"]);
    assert_eq!(r.as_array()[0].as_int(), 2);
    assert_eq!(r.as_array()[1].as_int(), 1);
}

/// BYINT, including a negative increment.
#[test]
fn test_increx_byint() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "mykey2", "100"]);
    let r = c.cmds(&["INCREX", "mykey2", "BYINT", "5"]);
    assert_eq!(r.as_array()[0].as_int(), 105);
    assert_eq!(r.as_array()[1].as_int(), 5);

    let r = c.cmds(&["INCREX", "mykey2", "BYINT", "-10"]);
    assert_eq!(r.as_array()[0].as_int(), 95);
    assert_eq!(r.as_array()[1].as_int(), -10);
}

/// BYFLOAT replies with bulk strings.
#[test]
fn test_increx_byfloat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "mykey3", "1.5"]);
    let r = c.cmds(&["INCREX", "mykey3", "BYFLOAT", "0.25"]);
    assert_eq!(r.as_array()[0].as_str(), "1.75");
    assert_eq!(r.as_array()[1].as_str(), "0.25");
}

/// BYINT requires an integer-typed stored value — a stored float is an error,
/// matching INCR/INCRBY.
#[test]
fn test_increx_byint_rejects_stored_float() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "mykeyf", "1.5"]);
    assert!(c.cmds(&["INCREX", "mykeyf", "BYINT", "1"]).is_error());
    // BYFLOAT accepts it, since integers promote to float losslessly.
    assert!(!c.cmds(&["INCREX", "mykeyf", "BYFLOAT", "1"]).is_error());
}

/// EX sets an expiration alongside the increment.
#[test]
fn test_increx_with_expiry() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["DEL", "mykey4"]);

    let r = c.cmds(&["INCREX", "mykey4", "BYINT", "1", "EX", "100"]);
    assert_eq!(r.as_array()[0].as_int(), 1);
    let ttl = c.cmds(&["TTL", "mykey4"]).as_int();
    assert!((90..=100).contains(&ttl), "expected ~100s TTL, got {ttl}");
}

/// ENX sets the TTL only when the key has none — the increment always applies.
#[test]
fn test_increx_enx_only_sets_ttl_when_absent() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // No existing TTL → ENX sets it.
    c.cmds(&["SET", "mykey5", "10"]);
    let r = c.cmds(&["INCREX", "mykey5", "BYINT", "1", "EX", "100", "ENX"]);
    assert_eq!(r.as_array()[0].as_int(), 11);
    let ttl = c.cmds(&["TTL", "mykey5"]).as_int();
    assert!((90..=100).contains(&ttl), "expected ~100s, got {ttl}");

    // Existing TTL → ENX leaves it alone, but still increments.
    c.cmds(&["SET", "mykey5", "10", "EX", "500"]);
    let r = c.cmds(&["INCREX", "mykey5", "BYINT", "1", "EX", "10", "ENX"]);
    assert_eq!(r.as_array()[0].as_int(), 11);
    let ttl = c.cmds(&["TTL", "mykey5"]).as_int();
    assert!(ttl > 100, "ENX must not shorten an existing TTL, got {ttl}");
}

/// PERSIST removes the expiration while incrementing.
#[test]
fn test_increx_persist() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "mykey6", "5", "EX", "1000"]);
    assert!(c.cmds(&["TTL", "mykey6"]).as_int() > 0);
    let r = c.cmds(&["INCREX", "mykey6", "BYINT", "1", "PERSIST"]);
    assert_eq!(r.as_array()[0].as_int(), 6);
    assert_eq!(c.cmds(&["TTL", "mykey6"]).as_int(), -1);
}

/// UBOUND: default is to skip the operation and report a zero delta;
/// SATURATE caps at the bound instead.
#[test]
fn test_increx_ubound_and_saturate() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "mykey7", "99"]);
    let r = c.cmds(&["INCREX", "mykey7", "BYINT", "5", "UBOUND", "100"]);
    assert_eq!(r.as_array()[0].as_int(), 99, "skipped: value unchanged");
    assert_eq!(r.as_array()[1].as_int(), 0, "skipped: zero delta");
    assert_eq!(c.cmds(&["GET", "mykey7"]).as_str(), "99");

    c.cmds(&["SET", "mykey7", "99"]);
    let r = c.cmds(&[
        "INCREX", "mykey7", "BYINT", "5", "UBOUND", "100", "SATURATE",
    ]);
    assert_eq!(r.as_array()[0].as_int(), 100, "saturated to the bound");
    assert_eq!(r.as_array()[1].as_int(), 1, "delta reflects the saturation");
}

/// LBOUND behaves symmetrically.
#[test]
fn test_increx_lbound() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "lb", "5"]);
    let r = c.cmds(&["INCREX", "lb", "BYINT", "-10", "LBOUND", "0"]);
    assert_eq!(r.as_array()[0].as_int(), 5);
    assert_eq!(r.as_array()[1].as_int(), 0);

    let r = c.cmds(&["INCREX", "lb", "BYINT", "-10", "LBOUND", "0", "SATURATE"]);
    assert_eq!(r.as_array()[0].as_int(), 0);
    assert_eq!(r.as_array()[1].as_int(), -5);
}

/// A skipped (out-of-bounds) operation must leave the TTL untouched too.
#[test]
fn test_increx_skip_leaves_ttl_untouched() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "skipttl", "100", "EX", "500"]);
    let before = c.cmds(&["TTL", "skipttl"]).as_int();
    // Out of bounds → skipped; the EX must not be applied.
    let r = c.cmds(&[
        "INCREX", "skipttl", "BYINT", "1", "UBOUND", "100", "EX", "10",
    ]);
    assert_eq!(r.as_array()[1].as_int(), 0, "operation should be skipped");
    let after = c.cmds(&["TTL", "skipttl"]).as_int();
    assert!(
        after > 100,
        "skipped INCREX must not apply its expiry (before {before}, after {after})"
    );
}

/// Integer overflow with no explicit bound counts as a bound violation.
#[test]
fn test_increx_overflow_is_a_bound_violation() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "ovf", "9223372036854775807"]);
    let r = c.cmds(&["INCREX", "ovf", "BYINT", "1"]);
    assert_eq!(r.as_array()[0].as_int(), 9223372036854775807);
    assert_eq!(r.as_array()[1].as_int(), 0, "overflow must be skipped");

    // With SATURATE it pins to the type limit.
    let r = c.cmds(&["INCREX", "ovf", "BYINT", "1", "SATURATE"]);
    assert_eq!(r.as_array()[0].as_int(), 9223372036854775807);
    assert_eq!(r.as_array()[1].as_int(), 0);
}

/// The documented rate-limiter pattern: UBOUND caps, ENX keeps the window.
#[test]
fn test_increx_rate_limiter_pattern() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["DEL", "ratelimit:u1"]);

    // Cap of 3 for a compact test.
    for expected in 1..=3 {
        let r = c.cmds(&[
            "INCREX",
            "ratelimit:u1",
            "BYINT",
            "1",
            "UBOUND",
            "3",
            "EX",
            "60",
            "ENX",
        ]);
        assert_eq!(r.as_array()[0].as_int(), expected);
        assert_eq!(r.as_array()[1].as_int(), 1, "under the cap: allowed");
    }
    // Fourth request is over the cap: zero delta signals rejection.
    let r = c.cmds(&[
        "INCREX",
        "ratelimit:u1",
        "BYINT",
        "1",
        "UBOUND",
        "3",
        "EX",
        "60",
        "ENX",
    ]);
    assert_eq!(r.as_array()[0].as_int(), 3);
    assert_eq!(r.as_array()[1].as_int(), 0, "over the cap: rejected");
}

/// Errors: wrong type, bad syntax, inverted bounds.
#[test]
fn test_increx_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["DEL", "ilist"]);
    c.cmds(&["LPUSH", "ilist", "x"]);
    let r = c.cmds(&["INCREX", "ilist"]);
    assert!(r.is_error() && r.error_str().starts_with("WRONGTYPE"));

    assert!(c.cmds(&["INCREX"]).is_error());
    assert!(c.cmds(&["INCREX", "k", "NOPE"]).is_error());
    assert!(c.cmds(&["INCREX", "k", "BYINT"]).is_error());
    // BYINT and BYFLOAT are mutually exclusive.
    assert!(
        c.cmds(&["INCREX", "k", "BYINT", "1", "BYFLOAT", "1"])
            .is_error()
    );
    // LBOUND must be <= UBOUND.
    assert!(
        c.cmds(&["INCREX", "k", "LBOUND", "10", "UBOUND", "1"])
            .is_error()
    );
    // ENX requires an expiry to condition on.
    assert!(c.cmds(&["INCREX", "k", "ENX"]).is_error());
    // PERSIST and EX are mutually exclusive.
    assert!(c.cmds(&["INCREX", "k", "EX", "10", "PERSIST"]).is_error());
}

/// Keys enough to be in many shards, whatever the machine.
fn many_keys(prefix: &str) -> Vec<String> {
    (0..24).map(|i| format!("{prefix}:{i}")).collect()
}

/// `MSET` is one step: a reader of all its keys finds them all with the
/// same value, never some from one `MSET` and some from the next. And
/// `MGET` reads one moment. Each used to take its keys one at a time.
#[test]
fn mset_and_mget_are_each_one_step() {
    let port = free_port();
    start_server(port);
    let keys = many_keys("step");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..2)
        .map(|w| {
            let keys = keys.clone();
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut c = Conn::connect(port);
                let mut round = 0u32;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let value = format!("w{w}-{round}");
                    let mut args = vec!["MSET"];
                    for key in &keys {
                        args.push(key);
                        args.push(&value);
                    }
                    assert_eq!(c.cmds(&args).as_str(), "OK");
                    round += 1;
                }
            })
        })
        .collect();

    let mut c = Conn::connect(port);
    let mut args = vec!["MGET"];
    args.extend(keys.iter().map(String::as_str));
    let mut seen = 0;
    let started = std::time::Instant::now();
    while started.elapsed() < std::time::Duration::from_millis(1500) {
        let reply = c.cmds(&args);
        let values: Vec<String> = reply
            .as_array()
            .iter()
            .filter(|v| !v.is_nil())
            .map(|v| v.as_str().to_string())
            .collect();
        // Before the first MSET there is nothing; after it, everything.
        assert!(
            values.is_empty() || values.len() == keys.len(),
            "{values:?}"
        );
        if let Some(first) = values.first() {
            assert!(values.iter().all(|v| v == first), "{values:?}");
            seen += 1;
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for writer in writers {
        writer.join().unwrap();
    }
    assert!(seen > 10, "only {seen} reads saw anything");
}

/// Of several clients that `MSETNX` the same keys at once, one is told it
/// set them, and the keys hold what that one wrote. Looking and writing
/// used to be two passes, and two clients could both be told yes.
#[test]
fn msetnx_lets_one_of_several_set_the_keys() {
    let port = free_port();
    start_server(port);
    for round in 0..30 {
        let keys = many_keys(&format!("nx{round}"));
        let start = std::sync::Arc::new(std::sync::Barrier::new(4));
        let clients: Vec<_> = (0..4)
            .map(|who| {
                let keys = keys.clone();
                let start = std::sync::Arc::clone(&start);
                std::thread::spawn(move || {
                    let mut c = Conn::connect(port);
                    let value = format!("client{who}");
                    let mut args = vec!["MSETNX"];
                    for key in &keys {
                        args.push(key);
                        args.push(&value);
                    }
                    start.wait();
                    c.cmds(&args).as_int()
                })
            })
            .collect();
        let told: Vec<i64> = clients.into_iter().map(|c| c.join().unwrap()).collect();
        assert_eq!(told.iter().sum::<i64>(), 1, "round {round}: {told:?}");
        let winner = format!("client{}", told.iter().position(|t| *t == 1).unwrap());
        let mut c = Conn::connect(port);
        let mut args = vec!["MGET"];
        args.extend(keys.iter().map(String::as_str));
        let reply = c.cmds(&args);
        for value in reply.as_array() {
            assert_eq!(value.as_str(), winner, "round {round}");
        }
    }
}

/// `MSET` drops a time to live, as a plain `SET` does, and `MSETNX` writes
/// over a key whose time has passed.
#[test]
fn mset_drops_a_time_to_live_and_msetnx_sees_past_an_expired_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "a", "old", "EX", "100"]);
    assert_eq!(c.cmds(&["MSET", "a", "new", "b", "new"]).as_str(), "OK");
    assert_eq!(c.cmds(&["TTL", "a"]).as_int(), -1);
    assert_eq!(c.cmds(&["GET", "a"]).as_str(), "new");

    c.cmds(&["SET", "gone", "old", "PX", "30"]);
    std::thread::sleep(std::time::Duration::from_millis(80));
    assert_eq!(
        c.cmds(&["MSETNX", "gone", "fresh", "other", "fresh"])
            .as_int(),
        1
    );
    assert_eq!(c.cmds(&["GET", "gone"]).as_str(), "fresh");
    assert_eq!(c.cmds(&["TTL", "gone"]).as_int(), -1);
    assert_eq!(c.cmds(&["MSETNX", "gone", "x", "third", "x"]).as_int(), 0);
    assert!(c.cmds(&["GET", "third"]).is_nil());
}

/// `RESTORE` of what Redis 8.10's `DUMP` gives for `{"a":1,"b":[1,2]}`, as
/// `MIGRATE` from Redis sends it: the array is a CBOR typed array, and was
/// refused as bad data (#157).
#[test]
fn test_restore_of_a_redis_8_10_json_document() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let payload: &[u8] = &[
        0x07, 0x81, 0x45, 0xe2, 0x52, 0x38, 0xdf, 0x91, 0x2c, 0x04, 0x05, 0x0b, 0xa2, 0x61, 0x61,
        0x01, 0x61, 0x62, 0xd8, 0x48, 0x42, 0x01, 0x02, 0x00, 0x0f, 0x00, 0x4f, 0x7f, 0x18, 0x31,
        0xbf, 0x44, 0xd6, 0xe4,
    ];
    let r = c.cmd(&[b"RESTORE", b"doc", b"0", payload]);
    assert!(!r.is_error(), "RESTORE: {r:?}");
    let got = c.cmds(&["JSON.GET", "doc"]);
    let text = match &got {
        RespValue::Bulk(b) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("JSON.GET: {other:?}"),
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value, serde_json::json!({"a": 1, "b": [1, 2]}));
}
