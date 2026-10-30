// tests/pr1_commands.rs
//
// Integration tests for all PR1 commands:
//   TOUCH, OBJECT REFCOUNT, DUMP / RESTORE, SORT / SORT_RO,
//   TIME, LOLWUT, COMMAND LIST, COMMAND GETKEYS, COMMAND GETKEYSANDFLAGS
//
// Same harness as tests/string-json.rs — each test spawns its own server
// on a random port so tests run fully in parallel with no shared state.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── Minimal RESP client ───────────────────────────────────────────────────────

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
            _ => panic!("unexpected RESP byte sequence: {:?}", line),
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
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("not an int: {:?}", self),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
    fn as_bytes(&self) -> &[u8] {
        match self {
            RespValue::Bulk(b) => b,
            _ => panic!("not a bulk: {:?}", self),
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

/// `DEBUG POPULATE` makes the keys it says it makes, as Redis: it replied
/// `OK` and made none (#173).
#[test]
fn debug_populate_makes_its_keys() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert_eq!(c.cmds(&["DEBUG", "POPULATE", "100"]).as_str(), "OK");
    assert_eq!(c.cmds(&["DBSIZE"]).as_int(), 100);
    assert_eq!(c.cmds(&["GET", "key:0"]).as_str(), "value:0");
    assert_eq!(c.cmds(&["GET", "key:99"]).as_str(), "value:99");
    // A prefix, and a size: cut, or padded with zero bytes.
    c.cmds(&["DEBUG", "POPULATE", "3", "p", "4"]);
    assert_eq!(c.cmds(&["GET", "p:2"]).as_str(), "valu");
    c.cmds(&["DEBUG", "POPULATE", "1", "q", "10"]);
    assert_eq!(c.cmds(&["STRLEN", "q:0"]).as_int(), 10);
    // A key that is there is left alone.
    c.cmds(&["SET", "key:5", "mine"]);
    c.cmds(&["DEBUG", "POPULATE", "10"]);
    assert_eq!(c.cmds(&["GET", "key:5"]).as_str(), "mine");
    assert_eq!(c.cmds(&["DBSIZE"]).as_int(), 104);
    assert!(c.cmds(&["DEBUG", "POPULATE", "-1"]).is_error());
    assert!(c.cmds(&["DEBUG", "POPULATE", "many"]).is_error());
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
// TOUCH
// ═════════════════════════════════════════════════════════════════════════════

// Manual scenario:
//   SET a "1"
//   SET b "2"
//   TOUCH a b nosuch   → 2  (only a and b exist)
//   TOUCH nosuch       → 0
#[test]
fn test_touch_existing_keys() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "a", "1"]);
    c.cmds(&["SET", "b", "2"]);

    assert_eq!(c.cmds(&["TOUCH", "a", "b", "nosuch"]).as_int(), 2);
    assert_eq!(c.cmds(&["TOUCH", "nosuch"]).as_int(), 0);
}

// TOUCH on a key that has expired returns 0 (expired = absent).
#[test]
fn test_touch_expired_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v"]);
    c.cmds(&["PEXPIRE", "k", "50"]); // expire in 50 ms
    thread::sleep(Duration::from_millis(150));

    assert_eq!(c.cmds(&["TOUCH", "k"]).as_int(), 0);
}

// TOUCH with no args returns an error.
#[test]
fn test_touch_no_args() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TOUCH"]).is_error());
}

// TOUCH does not modify the key's TTL.
#[test]
fn test_touch_preserves_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v"]);
    c.cmds(&["EXPIRE", "k", "100"]);
    let ttl_before = c.cmds(&["TTL", "k"]).as_int();

    c.cmds(&["TOUCH", "k"]);
    let ttl_after = c.cmds(&["TTL", "k"]).as_int();

    // TTL should be unchanged (within 1 second of drift)
    assert!((ttl_before - ttl_after).abs() <= 1);
}

// ═════════════════════════════════════════════════════════════════════════════
// OBJECT REFCOUNT
// ═════════════════════════════════════════════════════════════════════════════

// Manual scenario:
//   SET k "hello"
//   OBJECT REFCOUNT k   → 1
//   OBJECT REFCOUNT nosuch → nil
#[test]
fn test_object_refcount_existing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "hello"]);
    assert_eq!(c.cmds(&["OBJECT", "REFCOUNT", "k"]).as_int(), 1);
}

#[test]
fn test_object_refcount_missing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["OBJECT", "REFCOUNT", "nosuch"]).is_nil());
}

// OBJECT REFCOUNT works on all value types, always returns 1.
#[test]
fn test_object_refcount_all_types() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "str_k", "hello"]);
    c.cmds(&["LPUSH", "list_k", "a"]);
    c.cmds(&["HSET", "hash_k", "f", "v"]);
    c.cmds(&["SADD", "set_k", "m"]);
    c.cmds(&["ZADD", "zset_k", "1.0", "m"]);

    for key in &["str_k", "list_k", "hash_k", "set_k", "zset_k"] {
        assert_eq!(
            c.cmds(&["OBJECT", "REFCOUNT", key]).as_int(),
            1,
            "OBJECT REFCOUNT {key} should be 1"
        );
    }
}

// OBJECT REFCOUNT on an expired key returns nil.
#[test]
fn test_object_refcount_expired() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v"]);
    c.cmds(&["PEXPIRE", "k", "50"]);
    thread::sleep(Duration::from_millis(150));

    assert!(c.cmds(&["OBJECT", "REFCOUNT", "k"]).is_nil());
}

// ═════════════════════════════════════════════════════════════════════════════
// DUMP / RESTORE
// ═════════════════════════════════════════════════════════════════════════════

// Round-trip a string value: DUMP → RESTORE → GET.
#[test]
fn test_dump_restore_string() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "src", "hello_world"]);
    let payload = c.cmds(&["DUMP", "src"]);
    assert!(
        !payload.is_nil(),
        "DUMP should return non-nil for existing key"
    );

    let payload_bytes = payload.as_bytes().to_vec();

    // Restore into a different key with no TTL
    let payload_arg: Vec<&[u8]> = vec![b"RESTORE", b"dst", b"0", payload_bytes.as_slice()];
    assert!(c.cmd(&payload_arg).is_ok());
    assert_eq!(c.cmds(&["GET", "dst"]).as_str(), "hello_world");
}

// DUMP on a missing key returns nil.
#[test]
fn test_dump_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["DUMP", "nosuch"]).is_nil());
}

// Round-trip a list value.
#[test]
fn test_dump_restore_list() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "src", "a"]);
    c.cmds(&["RPUSH", "src", "b"]);
    c.cmds(&["RPUSH", "src", "c"]);

    let payload = c.cmds(&["DUMP", "src"]);
    assert!(!payload.is_nil());
    let payload_bytes = payload.as_bytes().to_vec();

    let args: Vec<&[u8]> = vec![b"RESTORE", b"dst", b"0", payload_bytes.as_slice()];
    assert!(c.cmd(&args).is_ok());

    let range = c.cmds(&["LRANGE", "dst", "0", "-1"]);
    let elems: Vec<&str> = range.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(elems, vec!["a", "b", "c"]);
}

// Round-trip a hash value.
#[test]
fn test_dump_restore_hash() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "src", "field1", "value1", "field2", "value2"]);

    let payload = c.cmds(&["DUMP", "src"]);
    assert!(!payload.is_nil());
    let payload_bytes = payload.as_bytes().to_vec();

    let args: Vec<&[u8]> = vec![b"RESTORE", b"dst", b"0", payload_bytes.as_slice()];
    assert!(c.cmd(&args).is_ok());

    assert_eq!(c.cmds(&["HGET", "dst", "field1"]).as_str(), "value1");
    assert_eq!(c.cmds(&["HGET", "dst", "field2"]).as_str(), "value2");
}

// RESTORE with TTL — the restored key should have an expiry.
#[test]
fn test_restore_with_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "src", "ttl_test"]);
    let payload = c.cmds(&["DUMP", "src"]);
    let payload_bytes = payload.as_bytes().to_vec();

    // Restore with 10 second TTL
    let args: Vec<&[u8]> = vec![b"RESTORE", b"dst", b"10000", payload_bytes.as_slice()];
    assert!(c.cmd(&args).is_ok());

    let pttl = c.cmds(&["PTTL", "dst"]).as_int();
    assert!(
        pttl > 0 && pttl <= 10_000,
        "expected PTTL in (0,10000], got {pttl}"
    );
}

// RESTORE without REPLACE fails if destination key exists.
#[test]
fn test_restore_busykey() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "src", "hello"]);
    c.cmds(&["SET", "dst", "already_here"]);

    let payload = c.cmds(&["DUMP", "src"]);
    let payload_bytes = payload.as_bytes().to_vec();

    let args: Vec<&[u8]> = vec![b"RESTORE", b"dst", b"0", payload_bytes.as_slice()];
    assert!(c.cmd(&args).is_error(), "RESTORE should fail with BUSYKEY");
}

// RESTORE with REPLACE overwrites an existing key.
#[test]
fn test_restore_with_replace() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "src", "new_value"]);
    c.cmds(&["SET", "dst", "old_value"]);

    let payload = c.cmds(&["DUMP", "src"]);
    let payload_bytes = payload.as_bytes().to_vec();

    let args: Vec<&[u8]> = vec![
        b"RESTORE",
        b"dst",
        b"0",
        payload_bytes.as_slice(),
        b"REPLACE",
    ];
    assert!(c.cmd(&args).is_ok());
    assert_eq!(c.cmds(&["GET", "dst"]).as_str(), "new_value");
}

// DUMP on an expired key returns nil.
#[test]
fn test_dump_expired_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "k", "v"]);
    c.cmds(&["PEXPIRE", "k", "50"]);
    thread::sleep(Duration::from_millis(150));

    assert!(c.cmds(&["DUMP", "k"]).is_nil());
}

// Round-trip a set value.
#[test]
fn test_dump_restore_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "src", "alpha", "beta", "gamma"]);

    let payload = c.cmds(&["DUMP", "src"]);
    assert!(!payload.is_nil());
    let payload_bytes = payload.as_bytes().to_vec();

    let args: Vec<&[u8]> = vec![b"RESTORE", b"dst", b"0", payload_bytes.as_slice()];
    assert!(c.cmd(&args).is_ok());

    assert_eq!(c.cmds(&["SCARD", "dst"]).as_int(), 3);
    assert_eq!(c.cmds(&["SISMEMBER", "dst", "alpha"]).as_int(), 1);
    assert_eq!(c.cmds(&["SISMEMBER", "dst", "beta"]).as_int(), 1);
    assert_eq!(c.cmds(&["SISMEMBER", "dst", "gamma"]).as_int(), 1);
}

// Round-trip a sorted set value.
#[test]
fn test_dump_restore_sortedset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "src", "1.0", "one"]);
    c.cmds(&["ZADD", "src", "2.0", "two"]);
    c.cmds(&["ZADD", "src", "3.0", "three"]);

    let payload = c.cmds(&["DUMP", "src"]);
    assert!(!payload.is_nil());
    let payload_bytes = payload.as_bytes().to_vec();

    let args: Vec<&[u8]> = vec![b"RESTORE", b"dst", b"0", payload_bytes.as_slice()];
    assert!(c.cmd(&args).is_ok());

    assert_eq!(c.cmds(&["ZCARD", "dst"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZSCORE", "dst", "two"]).as_str(), "2");
}

// ═════════════════════════════════════════════════════════════════════════════
// SORT / SORT_RO
// ═════════════════════════════════════════════════════════════════════════════

// Basic numeric sort on a list (ascending, the default).
//   RPUSH nums 3 1 2
//   SORT nums   → [1, 2, 3]
#[test]
fn test_sort_list_numeric_asc() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "3", "1", "2"]);
    let result = c.cmds(&["SORT", "nums"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["1", "2", "3"]);
}

// Numeric sort descending.
#[test]
fn test_sort_list_numeric_desc() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "3", "1", "2"]);
    let result = c.cmds(&["SORT", "nums", "DESC"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["3", "2", "1"]);
}

// ALPHA sort on a list.
#[test]
fn test_sort_list_alpha() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "words", "banana", "apple", "cherry"]);
    let result = c.cmds(&["SORT", "words", "ALPHA"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["apple", "banana", "cherry"]);
}

// ALPHA DESC.
#[test]
fn test_sort_list_alpha_desc() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "words", "banana", "apple", "cherry"]);
    let result = c.cmds(&["SORT", "words", "ALPHA", "DESC"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["cherry", "banana", "apple"]);
}

// LIMIT offset count.
//   RPUSH nums 5 3 1 4 2
//   SORT nums LIMIT 1 3   → [2, 3, 4]  (sorted=[1,2,3,4,5], skip 1, take 3)
#[test]
fn test_sort_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "5", "3", "1", "4", "2"]);
    let result = c.cmds(&["SORT", "nums", "LIMIT", "1", "3"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["2", "3", "4"]);
}

// LIMIT with offset beyond the list length returns empty array.
#[test]
fn test_sort_limit_offset_beyond_end() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "1", "2", "3"]);
    let result = c.cmds(&["SORT", "nums", "LIMIT", "10", "5"]);
    assert_eq!(result.as_array().len(), 0);
}

// SORT on a set.
#[test]
fn test_sort_set_numeric() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "nums", "30", "10", "20"]);
    let result = c.cmds(&["SORT", "nums"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["10", "20", "30"]);
}

// SORT on a sorted set (members only, numeric).
#[test]
fn test_sort_zset_alpha() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "cherry"]);
    c.cmds(&["ZADD", "z", "2", "apple"]);
    c.cmds(&["ZADD", "z", "3", "banana"]);
    let result = c.cmds(&["SORT", "z", "ALPHA"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["apple", "banana", "cherry"]);
}

// SORT STORE writes result as a list and returns the count.
#[test]
fn test_sort_store() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "src", "3", "1", "2"]);
    let count = c.cmds(&["SORT", "src", "STORE", "dst"]).as_int();
    assert_eq!(count, 3);

    // Result should be stored as a sorted list
    let range = c.cmds(&["LRANGE", "dst", "0", "-1"]);
    let vals: Vec<&str> = range.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["1", "2", "3"]);

    // Stored key is a list
    assert_eq!(c.cmds(&["TYPE", "dst"]).as_str(), "list");
}

// SORT STORE on empty result deletes the destination key.
#[test]
fn test_sort_store_empty_deletes_dst() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // dst already exists
    c.cmds(&["SET", "dst", "old_value"]);
    // src doesn't exist — SORT of missing key returns empty
    let count = c.cmds(&["SORT", "nosuch_src", "STORE", "dst"]).as_int();
    assert_eq!(count, 0);
    // dst should be deleted since result was empty
    assert_eq!(c.cmds(&["EXISTS", "dst"]).as_int(), 0);
}

// SORT on missing key returns empty array (not error).
#[test]
fn test_sort_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["SORT", "nosuch"]);
    assert_eq!(result.as_array().len(), 0);
}

// SORT on non-numeric values without ALPHA returns error.
#[test]
fn test_sort_non_numeric_without_alpha_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "words", "foo", "bar"]);
    assert!(c.cmds(&["SORT", "words"]).is_error());
}

// SORT on a wrong-type key (e.g., a hash) returns WRONGTYPE error.
#[test]
fn test_sort_wrong_type() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "f", "v"]);
    assert!(c.cmds(&["SORT", "h"]).is_error());
}

// SORT_RO is identical to SORT but rejects STORE.
#[test]
fn test_sort_ro_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "3", "1", "2"]);
    let result = c.cmds(&["SORT_RO", "nums"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["1", "2", "3"]);
}

#[test]
fn test_sort_ro_rejects_store() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "1", "2", "3"]);
    assert!(c.cmds(&["SORT_RO", "nums", "STORE", "dst"]).is_error());
}

// SORT with LIMIT 0 -1 means all (count=-1 treated as unlimited).
// Redis uses -1 to mean "rest of the list", but our parser uses i64 and
// treats negative count as unlimited.
#[test]
fn test_sort_limit_all() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "3", "1", "2"]);
    // LIMIT 0 -1 means skip 0, take all
    let result = c.cmds(&["SORT", "nums", "LIMIT", "0", "-1"]);
    let vals: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["1", "2", "3"]);
}

// ═════════════════════════════════════════════════════════════════════════════
// TIME
// ═════════════════════════════════════════════════════════════════════════════

// TIME returns a two-element array of bulk strings:
//   [unix_seconds, microseconds_within_second]
#[test]
fn test_time_returns_two_element_array() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["TIME"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2, "TIME must return exactly 2 elements");

    // First element: Unix timestamp in seconds
    let secs: u64 = arr[0]
        .as_str()
        .parse()
        .expect("TIME[0] must be a valid u64");
    // Second element: microseconds within the second (0..999999)
    let micros: u64 = arr[1]
        .as_str()
        .parse()
        .expect("TIME[1] must be a valid u64");

    // Sanity check: seconds should be a plausible Unix timestamp (after 2024-01-01)
    assert!(secs > 1_700_000_000, "TIME seconds looks wrong: {secs}");
    // Microseconds must be in range
    assert!(
        micros < 1_000_000,
        "TIME microseconds out of range: {micros}"
    );
}

// Two successive TIME calls should return non-decreasing values.
#[test]
fn test_time_is_monotonically_non_decreasing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let t1 = c.cmds(&["TIME"]);
    let t2 = c.cmds(&["TIME"]);

    let secs1: u64 = t1.as_array()[0].as_str().parse().unwrap();
    let micros1: u64 = t1.as_array()[1].as_str().parse().unwrap();
    let secs2: u64 = t2.as_array()[0].as_str().parse().unwrap();
    let micros2: u64 = t2.as_array()[1].as_str().parse().unwrap();

    let ts1 = secs1 * 1_000_000 + micros1;
    let ts2 = secs2 * 1_000_000 + micros2;
    assert!(ts2 >= ts1, "TIME went backwards: {ts1} then {ts2}");
}

// TIME takes no arguments (extra args should not cause a crash — Redis ignores them).
#[test]
fn test_time_no_args_required() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Should succeed with no args
    let result = c.cmds(&["TIME"]);
    assert_eq!(result.as_array().len(), 2);
}

// ═════════════════════════════════════════════════════════════════════════════
// LOLWUT
// ═════════════════════════════════════════════════════════════════════════════

// LOLWUT returns a bulk string containing the version number.
#[test]
fn test_lolwut_contains_version() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["LOLWUT"]);
    // Must be a non-empty string
    assert!(!result.as_str().is_empty(), "LOLWUT returned empty string");
    // Must mention "KiviDB" or version info somewhere
    let text = result.as_str().to_lowercase();
    assert!(
        text.contains("kivi") || text.contains("version"),
        "LOLWUT output doesn't mention KiviDB or version: {text}"
    );
}

// LOLWUT with VERSION argument still succeeds.
#[test]
fn test_lolwut_with_version_arg() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["LOLWUT", "VERSION", "6"]);
    assert!(!result.as_str().is_empty());
}

// ═════════════════════════════════════════════════════════════════════════════
// COMMAND LIST
// ═════════════════════════════════════════════════════════════════════════════

// COMMAND LIST returns a non-empty array of command name strings.
#[test]
fn test_command_list_returns_array() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "LIST"]);
    let arr = result.as_array();
    assert!(!arr.is_empty(), "COMMAND LIST returned empty array");

    // Every element should be a non-empty string
    for item in arr {
        let s = item.as_str();
        assert!(!s.is_empty(), "COMMAND LIST contained empty string");
    }
}

// COMMAND LIST contains all known PR1 commands.
#[test]
fn test_command_list_contains_pr1_commands() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "LIST"]);
    let commands: Vec<String> = result
        .as_array()
        .iter()
        .map(|v| v.as_str().to_uppercase())
        .collect();

    for cmd in &[
        "TOUCH", "DUMP", "RESTORE", "SORT", "SORT_RO", "TIME", "LOLWUT",
    ] {
        assert!(
            commands.contains(&cmd.to_string()),
            "COMMAND LIST missing: {cmd}"
        );
    }
}

// COMMAND LIST FILTERBY PATTERN filters by glob.
#[test]
fn test_command_list_filterby_pattern() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "z*"]);
    let commands: Vec<String> = result
        .as_array()
        .iter()
        .map(|v| v.as_str().to_uppercase())
        .collect();

    // All returned commands must start with Z
    for cmd in &commands {
        assert!(
            cmd.starts_with('Z'),
            "COMMAND LIST FILTERBY PATTERN z* returned non-Z command: {cmd}"
        );
    }
    // And known Z commands must be present
    assert!(commands.contains(&"ZADD".to_string()));
    assert!(commands.contains(&"ZRANGE".to_string()));
}

// COMMAND LIST FILTERBY MODULE returns empty (no modules loaded).
#[test]
fn test_command_list_filterby_module_empty() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "LIST", "FILTERBY", "MODULE", "nonexistent"]);
    assert_eq!(result.as_array().len(), 0);
}

// COMMAND LIST FILTERBY MODULE names a built-in module's commands, as Redis
// lists FT.* under `search` and the vector set commands under `vectorset`.
#[test]
fn test_command_list_filterby_module_built_in() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let names = |c: &mut Conn, module: &str| -> Vec<String> {
        c.cmds(&["COMMAND", "LIST", "FILTERBY", "MODULE", module])
            .as_array()
            .iter()
            .map(|v| v.as_str().to_string())
            .collect()
    };
    let search = names(&mut c, "search");
    assert!(search.contains(&"ft.search".to_string()), "{search:?}");
    assert!(
        search
            .iter()
            .all(|n| n.starts_with("ft.") || n.starts_with("_ft."))
    );
    assert!(names(&mut c, "ReJSON").contains(&"json.set".to_string()));
    assert!(names(&mut c, "timeseries").contains(&"ts.add".to_string()));
    let vset = names(&mut c, "vectorset");
    assert!(vset.contains(&"vadd".to_string()) && vset.contains(&"vsim".to_string()));
    assert!(names(&mut c, "bf").is_empty());
}

// COMMAND LIST FILTERBY ACLCAT returns commands in that category.
#[test]
fn test_command_list_filterby_aclcat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "LIST", "FILTERBY", "ACLCAT", "string"]);
    let commands: Vec<String> = result
        .as_array()
        .iter()
        .map(|v| v.as_str().to_uppercase())
        .collect();

    // String category must contain GET and SET
    assert!(commands.contains(&"GET".to_string()));
    assert!(commands.contains(&"SET".to_string()));
    assert!(commands.contains(&"APPEND".to_string()));
}

// ═════════════════════════════════════════════════════════════════════════════
// COMMAND DOCS
// ═════════════════════════════════════════════════════════════════════════════

fn docs_value<'a>(docs: &'a RespValue, name: &str) -> &'a RespValue {
    let arr = docs.as_array();
    for pair in arr.chunks(2) {
        if pair[0].as_str() == name {
            return &pair[1];
        }
    }
    panic!("COMMAND DOCS entry has no {name}: {docs:?}");
}

fn docs_field<'a>(docs: &'a RespValue, name: &str) -> &'a str {
    docs_value(docs, name).as_str()
}

// COMMAND DOCS GET names the command and gives the summary, group,
// complexity, since, and the argument list redis-cli builds hints from.
#[test]
fn test_command_docs_get() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "DOCS", "GET"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2, "{result:?}");
    assert_eq!(arr[0].as_str(), "get");
    assert_eq!(
        docs_field(&arr[1], "summary"),
        "Returns the string value of a key."
    );
    assert_eq!(docs_field(&arr[1], "group"), "string");
    assert_eq!(docs_field(&arr[1], "complexity"), "O(1)");
    assert_eq!(docs_field(&arr[1], "since"), "1.0.0");
    let args = docs_value(&arr[1], "arguments").as_array();
    assert_eq!(args.len(), 1, "{args:?}");
    assert_eq!(docs_field(&args[0], "name"), "key");
    assert_eq!(docs_field(&args[0], "type"), "key");
    assert_eq!(docs_field(&args[0], "display_text"), "key");
    assert_eq!(docs_value(&args[0], "key_spec_index").as_int(), 0);
}

// Names are case-insensitive, and several names come back in the order asked.
#[test]
fn test_command_docs_several_names() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "DOCS", "ft.search", "SET"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 4, "{result:?}");
    assert_eq!(arr[0].as_str(), "ft.search");
    assert_eq!(docs_field(&arr[1], "group"), "module");
    assert_eq!(docs_field(&arr[1], "module"), "search");
    assert_eq!(arr[2].as_str(), "set");
    assert_eq!(docs_field(&arr[3], "group"), "string");
}

// A name that is not a command is omitted, not nil.
#[test]
fn test_command_docs_unknown_name_is_omitted() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "DOCS", "nosuch", "PING"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2, "{result:?}");
    assert_eq!(arr[0].as_str(), "ping");
    assert_eq!(docs_field(&arr[1], "group"), "connection");
}

// With no names, every command is documented, including GET.
#[test]
fn test_command_docs_all_includes_get() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "DOCS"]);
    let arr = result.as_array();
    assert!(
        arr.len() >= 2 && arr.len().is_multiple_of(2),
        "len {}",
        arr.len()
    );
    let names: Vec<&str> = arr.iter().step_by(2).map(|v| v.as_str()).collect();
    assert!(names.contains(&"get"), "{names:?}");
    assert!(names.contains(&"ft.search"), "{names:?}");
}

// With no names, only top-level commands are listed; subcommands are nested
// under their container's `subcommands`, as Redis does.
#[test]
fn test_command_docs_all_nests_subcommands() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "DOCS"]);
    let arr = result.as_array();
    let names: Vec<&str> = arr.iter().step_by(2).map(|v| v.as_str()).collect();
    assert!(
        names.iter().all(|n| !n.contains('|')),
        "subcommand at top level: {:?}",
        names.iter().filter(|n| n.contains('|')).collect::<Vec<_>>()
    );
    let i = names.iter().position(|n| *n == "acl").expect("acl");
    let subs = docs_value(&arr[i * 2 + 1], "subcommands");
    let sub_names: Vec<&str> = subs
        .as_array()
        .iter()
        .step_by(2)
        .map(|v| v.as_str())
        .collect();
    assert!(sub_names.contains(&"acl|cat"), "{sub_names:?}");
}

// Module-style commands report group `module` plus the module's name.
#[test]
fn test_command_docs_module_commands() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for (name, module) in [
        ("JSON.SET", "ReJSON"),
        ("TS.ADD", "timeseries"),
        ("VADD", "vectorset"),
        ("FT.CREATE", "search"),
    ] {
        let result = c.cmds(&["COMMAND", "DOCS", name]);
        let arr = result.as_array();
        assert_eq!(arr.len(), 2, "{name} {result:?}");
        assert_eq!(docs_field(&arr[1], "group"), "module", "{name}");
        assert_eq!(docs_field(&arr[1], "module"), module, "{name}");
    }
}

// `since` comes from the command JSON: XADD 5.0.0, GETEX 6.2.0, and
// HEXPIRE 9.0.0 (the Valkey release that added it).
#[test]
fn test_command_docs_since_from_command_json() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "DOCS", "XADD", "GETEX", "HEXPIRE"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 6, "{result:?}");
    assert_eq!(arr[0].as_str(), "xadd");
    assert_eq!(docs_field(&arr[1], "since"), "5.0.0");
    assert_eq!(arr[2].as_str(), "getex");
    assert_eq!(docs_field(&arr[3], "since"), "6.2.0");
    assert_eq!(arr[4].as_str(), "hexpire");
    assert_eq!(docs_field(&arr[5], "since"), "9.0.0");
}

// SADD / HSET / LPUSH complexity is the O(1)-per-element wording, not O(N).
#[test]
fn test_command_docs_complexity_o1_style() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for name in ["SADD", "HSET", "LPUSH"] {
        let result = c.cmds(&["COMMAND", "DOCS", name]);
        let arr = result.as_array();
        assert_eq!(arr.len(), 2, "{name} {result:?}");
        let complexity = docs_field(&arr[1], "complexity");
        assert!(complexity.starts_with("O(1)"), "{name}: {complexity}");
    }
}

// SET's argument tree is present so redis-cli can hint `key value [NX|XX]…`.
#[test]
fn test_command_docs_set_has_arguments() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "DOCS", "SET"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2, "{result:?}");
    let args = docs_value(&arr[1], "arguments").as_array();
    assert!(args.len() >= 2, "{args:?}");
    assert_eq!(docs_field(&args[0], "name"), "key");
    assert_eq!(docs_field(&args[1], "name"), "value");
    let names: Vec<&str> = args.iter().map(|a| docs_field(a, "name")).collect();
    assert!(names.contains(&"condition"), "{names:?}");
    assert!(names.contains(&"expiration"), "{names:?}");
}

// Every command COMMAND LIST names has documentation.
#[test]
fn test_command_docs_covers_command_list() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let listed: Vec<String> = c
        .cmds(&["COMMAND", "LIST"])
        .as_array()
        .iter()
        .map(|v| v.as_str().to_string())
        .filter(|n| !n.contains('|'))
        .collect();
    let docs = c.cmds(&["COMMAND", "DOCS"]);
    let documented: std::collections::HashSet<&str> = docs
        .as_array()
        .iter()
        .step_by(2)
        .map(|v| v.as_str())
        .collect();
    let missing: Vec<&str> = listed
        .iter()
        .map(|s| s.as_str())
        .filter(|n| !documented.contains(n))
        .collect();
    assert!(missing.is_empty(), "COMMAND DOCS missing {missing:?}");
}

// ═════════════════════════════════════════════════════════════════════════════
// COMMAND GETKEYS
// ═════════════════════════════════════════════════════════════════════════════

// COMMAND GETKEYS SET key value → [key]
#[test]
fn test_command_getkeys_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "SET", "mykey", "myval"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["mykey"]);
}

// COMMAND GETKEYS GET key → [key]
#[test]
fn test_command_getkeys_get() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "GET", "mykey"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["mykey"]);
}

// COMMAND GETKEYS MSET k1 v1 k2 v2 → [k1, k2]
#[test]
fn test_command_getkeys_mset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "MSET", "k1", "v1", "k2", "v2"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["k1", "k2"]);
}

// COMMAND GETKEYS MGET k1 k2 k3 → [k1, k2, k3]
#[test]
fn test_command_getkeys_mget() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "MGET", "k1", "k2", "k3"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["k1", "k2", "k3"]);
}

// COMMAND GETKEYS RENAME src dst → [src, dst]
#[test]
fn test_command_getkeys_rename() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "RENAME", "src", "dst"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["src", "dst"]);
}

// COMMAND GETKEYS DEL k1 k2 k3 → [k1, k2, k3]
#[test]
fn test_command_getkeys_del() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "DEL", "k1", "k2", "k3"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["k1", "k2", "k3"]);
}

// COMMAND GETKEYS ZADD key score member → [key]
#[test]
fn test_command_getkeys_zadd() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "ZADD", "myzset", "1.0", "member"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["myzset"]);
}

// COMMAND GETKEYS XREAD STREAMS k1 k2 0-0 0-0 → [k1, k2]
#[test]
fn test_command_getkeys_xread() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&[
        "COMMAND", "GETKEYS", "XREAD", "STREAMS", "stream1", "stream2", "0-0", "0-0",
    ]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["stream1", "stream2"]);
}

// COMMAND GETKEYS with unknown command returns error.
#[test]
fn test_command_getkeys_unknown_command() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["COMMAND", "GETKEYS", "NOTACOMMAND", "arg"])
            .is_error()
    );
}

// COMMAND GETKEYS with no subcommand args returns error.
#[test]
fn test_command_getkeys_no_args() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["COMMAND", "GETKEYS"]).is_error());
}

// COMMAND GETKEYS SORT key → [key]
#[test]
fn test_command_getkeys_sort() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "SORT", "mylist"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["mylist"]);
}

// COMMAND GETKEYS TOUCH k1 k2 → [k1, k2]
#[test]
fn test_command_getkeys_touch() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "TOUCH", "k1", "k2"]);
    let keys: Vec<&str> = result.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys, vec!["k1", "k2"]);
}

// COMMAND GETKEYS TIME → an error, as in Redis: TIME takes no keys
#[test]
fn test_command_getkeys_time_no_keys() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYS", "TIME"]);
    assert_eq!(
        result,
        RespValue::Error("ERR The command has no key arguments".to_string())
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// COMMAND GETKEYSANDFLAGS
// ═════════════════════════════════════════════════════════════════════════════

// The flags are Redis's key-specification flags: how the key is opened
// (RO, RW, OW, RM) and what is done with its value (access, update, insert,
// delete).

// COMMAND GETKEYSANDFLAGS SET key value → [[key, [OW, update]]]
#[test]
fn test_command_getkeysandflags_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYSANDFLAGS", "SET", "mykey", "myval"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 1);

    let entry = arr[0].as_array();
    assert_eq!(entry.len(), 2);
    assert_eq!(entry[0].as_str(), "mykey");

    let flags: Vec<&str> = entry[1].as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(flags, ["OW", "update"]);
}

// COMMAND GETKEYSANDFLAGS GET key → [[key, [RO, access]]]
#[test]
fn test_command_getkeysandflags_get() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYSANDFLAGS", "GET", "mykey"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 1);

    let entry = arr[0].as_array();
    let flags: Vec<&str> = entry[1].as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(flags, ["RO", "access"]);
}

// COMMAND GETKEYSANDFLAGS RENAME src dst → [[src, [RW, access, delete]], [dst, [OW, update]]]
#[test]
fn test_command_getkeysandflags_rename() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let result = c.cmds(&["COMMAND", "GETKEYSANDFLAGS", "RENAME", "src", "dst"]);
    let arr = result.as_array();
    assert_eq!(arr.len(), 2);

    let src_entry = arr[0].as_array();
    assert_eq!(src_entry[0].as_str(), "src");
    let src_flags: Vec<&str> = src_entry[1].as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(src_flags, ["RW", "access", "delete"]);

    let dst_entry = arr[1].as_array();
    assert_eq!(dst_entry[0].as_str(), "dst");
    let dst_flags: Vec<&str> = dst_entry[1].as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(dst_flags, ["OW", "update"]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SORT BY <external-pattern> and GET <external-pattern>
// ═══════════════════════════════════════════════════════════════════════════════

// SORT BY <pattern>: sort a list by external string key values.
// weight_* → for each element e, look up key "weight_{e}" as a string,
// parse as f64, use as the sort key.
#[test]
fn test_sort_by_external_string_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // List elements are IDs; weight_* keys hold their sort scores
    c.cmds(&["RPUSH", "items", "item3", "item1", "item2"]);
    c.cmds(&["SET", "weight_item1", "10"]);
    c.cmds(&["SET", "weight_item2", "5"]);
    c.cmds(&["SET", "weight_item3", "20"]);

    let r = c.cmds(&["SORT", "items", "BY", "weight_*", "ALPHA"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    // Sorted by weight: item2(5) < item1(10) < item3(20)
    assert_eq!(vals, vec!["item2", "item1", "item3"]);
}

// SORT BY DESC with external keys
#[test]
fn test_sort_by_external_key_desc() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "a", "b", "c"]);
    c.cmds(&["SET", "score_a", "1"]);
    c.cmds(&["SET", "score_b", "3"]);
    c.cmds(&["SET", "score_c", "2"]);

    let r = c.cmds(&["SORT", "ids", "BY", "score_*", "DESC", "ALPHA"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["b", "c", "a"]);
}

// SORT BY missing external key: missing keys sort as 0.0 (numeric default)
#[test]
fn test_sort_by_missing_external_key_sorts_as_zero() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "x", "y", "z"]);
    c.cmds(&["SET", "score_z", "5"]);
    // score_x and score_y don't exist — treated as 0.0

    let r = c.cmds(&["SORT", "ids", "BY", "score_*", "ALPHA"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    // x(0) and y(0) tie — stable within ties; z(5) is last
    assert_eq!(vals[2], "z", "z with score 5 must be last");
    // x and y (both 0) must be in the first two positions
    let first_two: std::collections::HashSet<&str> = vals[..2].iter().copied().collect();
    assert!(first_two.contains("x") && first_two.contains("y"));
}

// SORT BY hash field: "hash_*->field" syntax
#[test]
fn test_sort_by_hash_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "users", "u1", "u2", "u3"]);
    c.cmds(&["HSET", "user_u1", "age", "30"]);
    c.cmds(&["HSET", "user_u2", "age", "25"]);
    c.cmds(&["HSET", "user_u3", "age", "35"]);

    let r = c.cmds(&["SORT", "users", "BY", "user_*->age", "ALPHA"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    // Sorted by age: u2(25) < u1(30) < u3(35)
    assert_eq!(vals, vec!["u2", "u1", "u3"]);
}

// SORT BY hash field DESC
#[test]
fn test_sort_by_hash_field_desc() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "users", "u1", "u2", "u3"]);
    c.cmds(&["HSET", "user_u1", "score", "100"]);
    c.cmds(&["HSET", "user_u2", "score", "200"]);
    c.cmds(&["HSET", "user_u3", "score", "150"]);

    let r = c.cmds(&["SORT", "users", "BY", "user_*->score", "DESC", "ALPHA"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["u2", "u3", "u1"]);
}

// GET <pattern>: return external string key values instead of elements
#[test]
fn test_sort_get_external_string() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "3", "1", "2"]);
    c.cmds(&["SET", "name_1", "alice"]);
    c.cmds(&["SET", "name_2", "bob"]);
    c.cmds(&["SET", "name_3", "carol"]);

    // Sort numerically (1, 2, 3), GET the name for each
    let r = c.cmds(&["SORT", "ids", "GET", "name_*"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["alice", "bob", "carol"]);
}

// GET hash->field: return a hash field for each element
#[test]
fn test_sort_get_hash_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "3", "1", "2"]);
    c.cmds(&["HSET", "obj_1", "name", "alice", "city", "NYC"]);
    c.cmds(&["HSET", "obj_2", "name", "bob", "city", "LA"]);
    c.cmds(&["HSET", "obj_3", "name", "carol", "city", "SF"]);

    // Sort numerically (1→alice, 2→bob, 3→carol)
    let r = c.cmds(&["SORT", "ids", "GET", "obj_*->name"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["alice", "bob", "carol"]);
}

// GET # and GET hash->field together: multiple GET patterns → multiple values per element
#[test]
fn test_sort_get_hash_field_with_self() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "2", "1"]);
    c.cmds(&["HSET", "obj_1", "name", "alice"]);
    c.cmds(&["HSET", "obj_2", "name", "bob"]);

    // GET # returns the element itself; GET obj_*->name returns the name
    // For element "1": ["1", "alice"], for element "2": ["2", "bob"]
    let r = c.cmds(&["SORT", "ids", "GET", "#", "GET", "obj_*->name"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    // Sorted numerically: 1 then 2
    // Interleaved: [id, name, id, name]
    assert_eq!(vals, vec!["1", "alice", "2", "bob"]);
}

// GET missing external key → nil in results
#[test]
fn test_sort_get_missing_key_returns_nil() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "1", "2"]);
    c.cmds(&["SET", "name_1", "alice"]);
    // name_2 doesn't exist

    let r = c.cmds(&["SORT", "ids", "GET", "name_*"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "alice");
    assert!(arr[1].is_nil(), "missing GET key should return nil");
}

// BY + GET combined: sort by weight, return name
#[test]
fn test_sort_by_and_get_combined() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "users", "u1", "u2", "u3"]);
    c.cmds(&["HSET", "user_u1", "age", "30", "name", "alice"]);
    c.cmds(&["HSET", "user_u2", "age", "25", "name", "bob"]);
    c.cmds(&["HSET", "user_u3", "age", "35", "name", "carol"]);

    // Sort by age, return name
    let r = c.cmds(&[
        "SORT",
        "users",
        "BY",
        "user_*->age",
        "GET",
        "user_*->name",
        "ALPHA",
    ]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    // u2(25) → bob, u1(30) → alice, u3(35) → carol
    assert_eq!(vals, vec!["bob", "alice", "carol"]);
}

// BY + GET + LIMIT
#[test]
fn test_sort_by_get_with_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "a", "b", "c", "d", "e"]);
    c.cmds(&["SET", "weight_a", "5"]);
    c.cmds(&["SET", "weight_b", "2"]);
    c.cmds(&["SET", "weight_c", "8"]);
    c.cmds(&["SET", "weight_d", "1"]);
    c.cmds(&["SET", "weight_e", "4"]);
    c.cmds(&["SET", "label_a", "A"]);
    c.cmds(&["SET", "label_b", "B"]);
    c.cmds(&["SET", "label_c", "C"]);
    c.cmds(&["SET", "label_d", "D"]);
    c.cmds(&["SET", "label_e", "E"]);

    // Sort by weight ASC, return label, take middle 3 (LIMIT 1 3)
    // Sorted order by weight: d(1), b(2), e(4), a(5), c(8)
    // LIMIT 1 3 → skip d, take b, e, a
    let r = c.cmds(&[
        "SORT", "ids", "BY", "weight_*", "GET", "label_*", "LIMIT", "1", "3", "ALPHA",
    ]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["B", "E", "A"]);
}

// BY + GET + STORE: store sorted-by-external results as a list
#[test]
fn test_sort_by_external_store() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "ids", "x", "y", "z"]);
    c.cmds(&["SET", "w_x", "30"]);
    c.cmds(&["SET", "w_y", "10"]);
    c.cmds(&["SET", "w_z", "20"]);

    let count = c
        .cmds(&["SORT", "ids", "BY", "w_*", "STORE", "sorted", "ALPHA"])
        .as_int();
    assert_eq!(count, 3);

    let binding = c.cmds(&["LRANGE", "sorted", "0", "-1"]);
    let vals: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["y", "z", "x"]);
}

// SORT on a set BY external hash field
#[test]
fn test_sort_set_by_hash_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "members", "p1", "p2", "p3"]);
    c.cmds(&["HSET", "data_p1", "rank", "3"]);
    c.cmds(&["HSET", "data_p2", "rank", "1"]);
    c.cmds(&["HSET", "data_p3", "rank", "2"]);

    let r = c.cmds(&["SORT", "members", "BY", "data_*->rank", "ALPHA"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["p2", "p3", "p1"]);
}

// BY nosort still works (regression: must not be broken by external key path)
#[test]
fn test_sort_by_nosort_unchanged() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "items", "c", "a", "b"]);
    let r = c.cmds(&["SORT", "items", "BY", "nosort", "ALPHA"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    // BY nosort = keep insertion order
    assert_eq!(vals, vec!["c", "a", "b"]);
}

// GET # still works (regression)
#[test]
fn test_sort_get_hash_self_unchanged() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "nums", "3", "1", "2"]);
    let r = c.cmds(&["SORT", "nums", "GET", "#"]);
    let vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(vals, vec!["1", "2", "3"]);
}

// ═════════════════════════════════════════════════════════════════════════════
// RESTORE of payloads DUMPed by Redis (#39)
// ═════════════════════════════════════════════════════════════════════════════

// Captured from redis:6.2 (RDB 9), redis:7.0 (RDB 10), redis:7.4 (RDB 12)
// and redis:8.10 (RDB 15): each type DUMPed in the encodings those versions write for a
// small value and a large one, with what the value holds. One JSON object a
// line: redis, rdb, key, type, encoding, hex (the payload), expect.
fn redis_dump_fixtures() -> Vec<serde_json::Value> {
    include_str!("fixtures/redis-dump-payloads.jsonl")
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

fn reply_text(v: &RespValue) -> String {
    String::from_utf8_lossy(v.as_bytes()).into_owned()
}

fn json_strings(v: &serde_json::Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

// Data moves in from Redis by DUMP/RESTORE (MIGRATE, redis-shake, RIOT): a
// payload from Redis 6.2, 7.0, 7.4 or 8.10 restores to the same value, whatever
// its type and encoding.
#[test]
fn test_restore_reads_payloads_dumped_by_redis() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let fixtures = redis_dump_fixtures();
    assert_eq!(fixtures.len(), 56, "4 Redis versions x 14 values");
    for f in &fixtures {
        let field = |name: &str| f[name].as_str().unwrap().to_string();
        let label = format!(
            "Redis {} {} ({})",
            field("redis"),
            field("key"),
            field("encoding")
        );
        let key = format!("{}:{}", field("redis"), field("key"));
        let payload = from_hex(&field("hex"));

        let reply = c.cmd(&[b"RESTORE", key.as_bytes(), b"0", &payload]);
        assert!(reply.is_ok(), "{label}: RESTORE gave {reply:?}");
        assert_eq!(c.cmds(&["TYPE", &key]).as_str(), field("type"), "{label}");

        let e = &f["expect"];
        if let Some(v) = e.get("get") {
            assert_eq!(
                reply_text(&c.cmds(&["GET", &key])),
                v.as_str().unwrap(),
                "{label}"
            );
        }
        if let Some(v) = e.get("get_hex") {
            assert_eq!(
                c.cmds(&["GET", &key]).as_bytes(),
                from_hex(v.as_str().unwrap()),
                "{label}"
            );
        }
        if let Some(v) = e.get("lrange") {
            let got: Vec<String> = c
                .cmds(&["LRANGE", &key, "0", "-1"])
                .as_array()
                .iter()
                .map(reply_text)
                .collect();
            assert_eq!(got, json_strings(v), "{label}");
        }
        if let Some(v) = e.get("llen") {
            assert_eq!(
                c.cmds(&["LLEN", &key]).as_int(),
                v.as_i64().unwrap(),
                "{label}"
            );
        }
        if let Some(v) = e.get("lindex") {
            let at = v[0].as_i64().unwrap().to_string();
            assert_eq!(
                reply_text(&c.cmds(&["LINDEX", &key, &at])),
                v[1].as_str().unwrap(),
                "{label}"
            );
        }
        if let Some(v) = e.get("lindex_last") {
            assert_eq!(
                reply_text(&c.cmds(&["LINDEX", &key, "-1"])),
                v.as_str().unwrap(),
                "{label}"
            );
        }
        if let Some(v) = e.get("smembers") {
            let mut got: Vec<String> = c
                .cmds(&["SMEMBERS", &key])
                .as_array()
                .iter()
                .map(reply_text)
                .collect();
            let mut want = json_strings(v);
            got.sort();
            want.sort();
            assert_eq!(got, want, "{label}");
        }
        if let Some(v) = e.get("scard") {
            assert_eq!(
                c.cmds(&["SCARD", &key]).as_int(),
                v.as_i64().unwrap(),
                "{label}"
            );
        }
        if let Some(v) = e.get("sismember") {
            assert_eq!(
                c.cmds(&["SISMEMBER", &key, v.as_str().unwrap()]).as_int(),
                1,
                "{label}"
            );
        }
        if let Some(v) = e.get("zrange") {
            let got: Vec<String> = c
                .cmds(&["ZRANGE", &key, "0", "-1", "WITHSCORES"])
                .as_array()
                .iter()
                .map(reply_text)
                .collect();
            let want: Vec<String> = v
                .as_array()
                .unwrap()
                .iter()
                .flat_map(json_strings)
                .collect();
            assert_eq!(got, want, "{label}");
        }
        if let Some(v) = e.get("zcard") {
            assert_eq!(
                c.cmds(&["ZCARD", &key]).as_int(),
                v.as_i64().unwrap(),
                "{label}"
            );
        }
        if let Some(v) = e.get("zscore") {
            let got = reply_text(&c.cmds(&["ZSCORE", &key, v[0].as_str().unwrap()]));
            assert_eq!(got, v[1].as_str().unwrap(), "{label}");
        }
        if let Some(v) = e.get("hgetall") {
            let flat: Vec<String> = c
                .cmds(&["HGETALL", &key])
                .as_array()
                .iter()
                .map(reply_text)
                .collect();
            let got: std::collections::BTreeMap<String, String> = flat
                .chunks(2)
                .map(|pair| (pair[0].clone(), pair[1].clone()))
                .collect();
            let want: std::collections::BTreeMap<String, String> = v
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                .collect();
            assert_eq!(got, want, "{label}");
        }
        if let Some(v) = e.get("hlen") {
            assert_eq!(
                c.cmds(&["HLEN", &key]).as_int(),
                v.as_i64().unwrap(),
                "{label}"
            );
        }
        if let Some(v) = e.get("hget") {
            let got = reply_text(&c.cmds(&["HGET", &key, v[0].as_str().unwrap()]));
            assert_eq!(got, v[1].as_str().unwrap(), "{label}");
        }
    }
}

// Redis's checksum, by its published check value.
#[test]
fn test_redis_crc64_is_redis_crc64() {
    assert_eq!(
        kividb::snapshot::redis_crc64(b"123456789"),
        0xe9c6d914c4b8d9ca
    );
}

// A Redis payload is taken only whole and from an RDB version this reads:
// one that fails its checksum or comes from a newer version than Redis
// 8.10's is refused as Redis refuses it; one sealed right that holds a type
// the loader does not know, or more than one value, is `Bad data format`.
#[test]
fn test_restore_refuses_redis_payloads_it_cannot_read() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    const REFUSED: &str = "ERR DUMP payload version or checksum are wrong";
    const BAD_DATA: &str = "ERR Bad data format";

    let fixture = redis_dump_fixtures()
        .into_iter()
        .find(|f| f["redis"] == "8.10" && f["key"] == "str:raw")
        .unwrap();
    let good = from_hex(fixture["hex"].as_str().unwrap());
    // A payload with its checksum made right for whatever came before it.
    let resealed = |mut body: Vec<u8>| {
        let crc = kividb::snapshot::redis_crc64(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        body
    };
    let refused_with = |c: &mut Conn, key: &str, payload: &[u8], error: &str| {
        let reply = c.cmd(&[b"RESTORE", key.as_bytes(), b"0", payload]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e == error),
            "{key}: expected {error}, got {reply:?}"
        );
        assert!(c.cmds(&["EXISTS", key]).as_int() == 0, "{key} was created");
    };
    let refused = |c: &mut Conn, key: &str, payload: &[u8]| refused_with(c, key, payload, REFUSED);

    let mut corrupt = good.clone();
    corrupt[3] ^= 0x20;
    refused(&mut c, "corrupt", &corrupt);

    let mut newer = good[..good.len() - 8].to_vec();
    let at = newer.len() - 2;
    newer[at..].copy_from_slice(&16u16.to_le_bytes());
    refused(&mut c, "newer", &resealed(newer));

    let mut unknown_type = good[..good.len() - 8].to_vec();
    unknown_type[0] = 200;
    refused_with(&mut c, "unknown-type", &resealed(unknown_type), BAD_DATA);

    // Bytes left over after the value: not one value.
    let mut trailing = good[..good.len() - 10].to_vec();
    trailing.push(0);
    trailing.extend_from_slice(&15u16.to_le_bytes());
    refused_with(&mut c, "trailing", &resealed(trailing), BAD_DATA);

    // The payload itself, for contrast.
    assert!(c.cmd(&[b"RESTORE", b"good", b"0", &good]).is_ok());
    assert_eq!(c.cmds(&["GET", "good"]).as_str(), "hello");
}

// Streams (every RDB stream type from Redis 6.2 to 8.10: consumer groups,
// pending entries, an idle consumer, a deleted entry, entries with the first
// entry's fields and with others, a released entry on 8.10, idempotent
// production on 8.10), hashes whose fields have TTLs (Redis 7.4 and 8.10, in
// both encodings) and a RedisJSON document (Redis 8.10), captured from real
// servers with what each server answered about them.
fn redis_dump_complex_fixtures() -> Vec<serde_json::Value> {
    include_str!("fixtures/redis-dump-payloads-complex.jsonl")
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// A RESP2 reply of field, value pairs, as text.
fn reply_pairs(v: &RespValue) -> std::collections::BTreeMap<String, String> {
    v.as_array()
        .chunks(2)
        .filter_map(|pair| match &pair[1] {
            RespValue::Bulk(b) => Some((
                reply_text(&pair[0]),
                String::from_utf8_lossy(b).into_owned(),
            )),
            RespValue::Int(n) => Some((reply_text(&pair[0]), n.to_string())),
            RespValue::SimpleStr(s) => Some((reply_text(&pair[0]), s.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn test_restore_reads_streams_field_ttl_hashes_and_json_dumped_by_redis() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let fixtures = redis_dump_complex_fixtures();
    assert_eq!(fixtures.len(), 15);
    for f in &fixtures {
        let field = |name: &str| f[name].as_str().unwrap().to_string();
        let label = format!(
            "Redis {} {} (RDB type {})",
            field("redis"),
            field("key"),
            f["rdb_type"]
        );
        let key = format!("{}:{}", field("redis"), field("key"));
        let payload = from_hex(&field("hex"));
        let reply = c.cmd(&[b"RESTORE", key.as_bytes(), b"0", &payload]);
        assert!(reply.is_ok(), "{label}: RESTORE gave {reply:?}");
        let e = &f["expect"];

        if field("type") == "stream" {
            let got: Vec<(String, Vec<String>)> = c
                .cmds(&["XRANGE", &key, "-", "+"])
                .as_array()
                .iter()
                .map(|entry| {
                    let entry = entry.as_array();
                    (
                        reply_text(&entry[0]),
                        entry[1].as_array().iter().map(reply_text).collect(),
                    )
                })
                .collect();
            let want: Vec<(String, Vec<String>)> = e["xrange"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| {
                    (
                        entry[0].as_str().unwrap().to_string(),
                        json_strings(&entry[1]),
                    )
                })
                .collect();
            assert_eq!(got, want, "{label}: entries");

            let info = reply_pairs(&c.cmds(&["XINFO", "STREAM", &key]));
            assert_eq!(info["length"], e["length"].to_string(), "{label}");
            assert_eq!(
                info["last-generated-id"],
                e["last_id"].as_str().unwrap(),
                "{label}"
            );
            if let Some(v) = e.get("max_deleted_id") {
                assert_eq!(info["max-deleted-entry-id"], v.as_str().unwrap(), "{label}");
                assert_eq!(
                    info["entries-added"],
                    e["entries_added"].to_string(),
                    "{label}"
                );
            }

            let groups = c.cmds(&["XINFO", "GROUPS", &key]);
            let mut got_groups: Vec<std::collections::BTreeMap<String, String>> =
                groups.as_array().iter().map(reply_pairs).collect();
            got_groups.sort_by(|a, b| a["name"].cmp(&b["name"]));
            let want_groups = e["groups"].as_array().unwrap();
            assert_eq!(got_groups.len(), want_groups.len(), "{label}: groups");
            for (got, want) in got_groups.iter().zip(want_groups) {
                let group = want["name"].as_str().unwrap();
                assert_eq!(got["name"], group, "{label}");
                assert_eq!(
                    got["last-delivered-id"],
                    want["last_id"].as_str().unwrap(),
                    "{label} {group}"
                );

                let mut consumers: Vec<String> = c
                    .cmds(&["XINFO", "CONSUMERS", &key, group])
                    .as_array()
                    .iter()
                    .map(|consumer| reply_pairs(consumer)["name"].clone())
                    .collect();
                consumers.sort();
                assert_eq!(
                    consumers,
                    json_strings(&want["consumers"]),
                    "{label} {group}: consumers"
                );

                let pending: Vec<(String, String, i64)> = c
                    .cmds(&["XPENDING", &key, group, "-", "+", "100"])
                    .as_array()
                    .iter()
                    .map(|p| {
                        let p = p.as_array();
                        (reply_text(&p[0]), reply_text(&p[1]), p[3].as_int())
                    })
                    .collect();
                let want_pending: Vec<(String, String, i64)> = want["pending"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| {
                        (
                            p[0].as_str().unwrap().into(),
                            p[1].as_str().unwrap().into(),
                            p[2].as_i64().unwrap(),
                        )
                    })
                    .collect();
                assert_eq!(pending, want_pending, "{label} {group}: pending entries");
            }
        } else if field("type") == "hash" {
            assert_eq!(
                c.cmds(&["HLEN", &key]).as_int(),
                e["hlen"].as_i64().unwrap(),
                "{label}"
            );
            let fields: Vec<String> = c
                .cmds(&["HKEYS", &key])
                .as_array()
                .iter()
                .map(reply_text)
                .collect();
            let mut args = vec![
                "HTTL".to_string(),
                key.clone(),
                "FIELDS".into(),
                fields.len().to_string(),
            ];
            args.extend(fields.iter().cloned());
            let refs: Vec<&str> = args.iter().map(String::as_str).collect();
            let ttls = c.cmds(&refs);
            let mut with_ttl: Vec<String> = fields
                .iter()
                .zip(ttls.as_array())
                .filter(|(_, t)| t.as_int() > 0)
                .map(|(f, _)| f.clone())
                .collect();
            with_ttl.sort();
            assert_eq!(
                with_ttl,
                json_strings(&e["with_ttl"]),
                "{label}: fields with a TTL"
            );
            let sample = &e["hget"];
            let got = reply_text(&c.cmds(&["HGET", &key, sample[0].as_str().unwrap()]));
            assert_eq!(got, sample[1].as_str().unwrap(), "{label}");
        } else {
            assert_eq!(field("type"), "ReJSON-RL", "{label}");
            let got: serde_json::Value =
                serde_json::from_str(&reply_text(&c.cmds(&["JSON.GET", &key, "$"]))).unwrap();
            assert_eq!(got, serde_json::json!([e["json"]]), "{label}");
        }
    }
}

// A stream payload from Redis 5 (RDB type 15) records no group's
// entries-read: as Redis does, it is estimated from the group's last ID. The
// payload is the Redis suite's own ("Loading from legacy (Redis <= v6.2.x,
// rdb_ver < 10) persistence"), DUMPed after XADD 1-0 to 6-0, XDEL 3-0, two
// groups, XREADGROUP of 4 entries by g1, and XTRIM MAXLEN = 2.
#[test]
fn test_restore_estimates_entries_read_for_a_stream_from_redis_5() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let payload = from_hex(&[
        "0f011000000000000000010000000000000000c3404a4057165700000023000201040101018464617461050001030100",
        "20010381610204200a0001400a0062600a0002400a0063600a4022018164200a4039200a0065600a0005400a0066200a",
        "00ff02060002026731050004000000000000000100000000000000003ef783437a010000010000000000000002000000",
        "00000000003ef783437a01000001000000000000000400000000000000003ef783437a01000001000000000000000500",
        "000000000000003ef783437a0100000101036331313ef783437a01000004000000000000000100000000000000000000",
        "000000000002000000000000000000000000000000040000000000000000000000000000000500000000000000000267",
        "320000000009003d52ef6867521dfa",
    ]
    .concat());
    assert!(c.cmd(&[b"RESTORE", b"x", b"0", &payload]).is_ok());

    let info = reply_pairs(&c.cmds(&["XINFO", "STREAM", "x"]));
    assert_eq!(info["max-deleted-entry-id"], "0-0");
    assert_eq!(info["entries-added"], "2");
    let groups: Vec<std::collections::BTreeMap<String, String>> = c
        .cmds(&["XINFO", "GROUPS", "x"])
        .as_array()
        .iter()
        .map(reply_pairs)
        .collect();
    let group = |name: &str| groups.iter().find(|g| g["name"] == name).unwrap().clone();
    // g1 was delivered up to 5-0, now the first entry: 2 added - 2 kept + 1.
    assert_eq!(group("g1")["entries-read"], "1");
    assert_eq!(group("g1")["lag"], "1");
    assert_eq!(group("g2")["entries-read"], "0");
    assert_eq!(group("g2")["lag"], "2");
}

/// A payload whose string claims a length near 2^64 is refused as Redis
/// refuses it, `Bad data format`, and the server stays up. Room for the
/// claimed length used to be made at once, and the process aborted — with a
/// checksum that matched, since anyone can compute one, as with none: a zero
/// checksum is none, as Redis writes with checksums off (the Redis suite's
/// "RESTORE fail with invalid payload size").
#[test]
fn test_restore_of_a_huge_claimed_length_is_bad_data_not_a_crash() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // String type, a 64-bit length of 2^64 - 9, no data, RDB version 12.
    let mut sealed = b"\x00\x81\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xF7\x0c\x00".to_vec();
    let crc = kividb::snapshot::redis_crc64(&sealed);
    sealed.extend_from_slice(&crc.to_le_bytes());
    let mut unsealed = b"\x00\x81\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xF7\x0c\x00".to_vec();
    unsealed.extend_from_slice(&[0u8; 8]);
    // An LZF string whose claimed original length is far past what LZF makes.
    let mut lzf = b"\xC3\x01\x81\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xF7\x00\x0c\x00".to_vec();
    lzf.extend_from_slice(&[0u8; 8]);
    for payload in [&sealed, &unsealed, &lzf] {
        let reply = c.cmd(&[b"RESTORE", b"huge", b"0", payload]);
        assert!(
            matches!(&reply, RespValue::Error(e) if e == "ERR Bad data format"),
            "{reply:?}"
        );
    }
    assert!(c.cmds(&["PING"]).is_ok() || c.cmds(&["PING"]).as_str() == "PONG");

    // A wrong checksum is still that.
    let mut wrong = b"\x00\x01a\x0c\x00".to_vec();
    wrong.extend_from_slice(&[1u8; 8]);
    let reply = c.cmd(&[b"RESTORE", b"k", b"0", &wrong]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e == "ERR DUMP payload version or checksum are wrong"),
        "{reply:?}"
    );
}

// Without Lua, every script command says which build has it: BullMQ and
// Sidekiq show this error to their users.
#[cfg(not(feature = "lua"))]
#[test]
fn test_script_commands_without_lua_name_the_build() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for cmd in [
        &["EVAL", "return 1", "0"][..],
        &["EVALSHA", "0000000000000000000000000000000000000000", "0"],
        &["SCRIPT", "LOAD", "return 1"],
        &["FUNCTION", "LIST"],
        &["FCALL", "f", "0"],
    ] {
        let reply = c.cmds(cmd);
        assert!(
            matches!(&reply, RespValue::Error(e) if e.starts_with("ERR this build of KiviDB has no Lua")
                && e.contains(&format!("kividb/kividb:v{}-lua", env!("CARGO_PKG_VERSION")))),
            "{cmd:?}: {reply:?}"
        );
    }
}
