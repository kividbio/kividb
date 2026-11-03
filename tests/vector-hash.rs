// Integration tests for vector search and hash commands.
// Each test spins up a KiviDB server on a random port, runs commands
// via a raw TCP connection, and asserts the RESP responses.
//
// We don't use a Redis client crate to keep dependencies minimal —
// instead we have a tiny RESP writer/reader that's easy to audit.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use ordered_float::Float;

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        // Retry a few times — server needs a moment to bind
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

    /// Send a RESP array command and return the raw response string.
    fn cmd(&mut self, args: &[&[u8]]) -> RespValue {
        // Encode as RESP array
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    /// Convenience: send string args
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
            _ => panic!("unexpected RESP byte: {:?}", line),
        }
    }
}

// ── RESP value type ───────────────────────────────────────────────────────────

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

/// Find a free port by binding to :0 and reading what the OS assigned.
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

/// Spawn a KiviDB server on `port` in a background thread.
/// Returns when the port is accepting connections.
fn start_server(port: u16) {
    thread::spawn(move || {
        // Build a Config directly rather than going through clap
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

// ── Vector helpers ────────────────────────────────────────────────────────────

/// Pack a slice of f32 into little-endian bytes — same as Python's struct.pack('Nf', ...)
fn pack_f32(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Parse a score string like "0.020000" back to f32
fn parse_score(v: &RespValue) -> f32 {
    v.as_str().parse().unwrap()
}

// ── Hash command tests ────────────────────────────────────────────────────────

#[test]
fn test_hset_hget_hgetall() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["HSET", "user:1", "name", "alice", "age", "30"])
            .as_int()
            == 2
    );
    assert_eq!(c.cmds(&["HGET", "user:1", "name"]).as_str(), "alice");
    assert_eq!(c.cmds(&["HGET", "user:1", "age"]).as_str(), "30");
    assert!(c.cmds(&["HGET", "user:1", "missing"]).is_nil());

    let all = c.cmds(&["HGETALL", "user:1"]);
    let pairs = all.as_array();
    // HGETALL returns field, value, field, value... in any order
    assert_eq!(pairs.len(), 4);
}

#[test]
fn test_hmset_hmget() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["HMSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"])
            .is_ok()
    );

    let got = c.cmds(&["HMGET", "h", "f1", "f3", "missing"]);
    let arr = got.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_str(), "v1");
    assert_eq!(arr[1].as_str(), "v3");
    assert!(arr[2].is_nil());
}

#[test]
fn test_hdel_hexists_hlen() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "1", "b", "2", "c", "3"]);

    assert_eq!(c.cmds(&["HEXISTS", "h", "a"]).as_int(), 1);
    assert_eq!(c.cmds(&["HEXISTS", "h", "z"]).as_int(), 0);
    assert_eq!(c.cmds(&["HLEN", "h"]).as_int(), 3);

    // Delete two fields, one of which doesn't exist
    assert_eq!(c.cmds(&["HDEL", "h", "a", "z"]).as_int(), 1);
    assert_eq!(c.cmds(&["HLEN", "h"]).as_int(), 2);
    assert_eq!(c.cmds(&["HEXISTS", "h", "a"]).as_int(), 0);
}

#[test]
fn test_hkeys_hvals() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "x", "10", "y", "20"]);

    let keys = c.cmds(&["HKEYS", "h"]);
    let kv = keys.as_array();
    assert_eq!(kv.len(), 2);
    // Order not guaranteed — sort and compare
    let mut ks: Vec<&str> = kv.iter().map(|v| v.as_str()).collect();
    ks.sort();
    assert_eq!(ks, vec!["x", "y"]);

    let vals = c.cmds(&["HVALS", "h"]);
    let vv = vals.as_array();
    assert_eq!(vv.len(), 2);
    let mut vs: Vec<&str> = vv.iter().map(|v| v.as_str()).collect();
    vs.sort();
    assert_eq!(vs, vec!["10", "20"]);
}

#[test]
fn test_hincrby_hincrbyfloat() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Field doesn't exist yet — should start from 0
    assert_eq!(c.cmds(&["HINCRBY", "h", "n", "5"]).as_int(), 5);
    assert_eq!(c.cmds(&["HINCRBY", "h", "n", "3"]).as_int(), 8);
    assert_eq!(c.cmds(&["HINCRBY", "h", "n", "-2"]).as_int(), 6);

    let f: f64 = c
        .cmds(&["HINCRBYFLOAT", "h", "f", "1.5"])
        .as_str()
        .parse()
        .unwrap();
    assert!((f - 1.5).abs() < 1e-9);
    let f2: f64 = c
        .cmds(&["HINCRBYFLOAT", "h", "f", "0.5"])
        .as_str()
        .parse()
        .unwrap();
    assert!((f2 - 2.0).abs() < 1e-9);
}

#[test]
fn test_hsetnx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["HSETNX", "h", "field", "original"]).as_int(), 1);
    assert_eq!(c.cmds(&["HSETNX", "h", "field", "overwrite"]).as_int(), 0);
    assert_eq!(c.cmds(&["HGET", "h", "field"]).as_str(), "original");
}

#[test]
fn test_hscan() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "apple", "1", "apricot", "2", "banana", "3"]);

    // Full scan from cursor 0 should return cursor=0 (done) and all fields
    let result = c.cmds(&["HSCAN", "h", "0"]);
    let parts = result.as_array();
    assert_eq!(parts[0].as_str(), "0"); // cursor wrapped back to 0 = complete
    let items = parts[1].as_array();
    assert_eq!(items.len(), 6); // 3 field-value pairs

    // MATCH filter
    let result2 = c.cmds(&["HSCAN", "h", "0", "MATCH", "ap*"]);
    let parts2 = result2.as_array();
    let items2 = parts2[1].as_array();
    // apple and apricot match, banana doesn't
    assert_eq!(items2.len(), 4);
}

#[test]
fn test_wrongtype_hash() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "value"]);
    assert!(c.cmds(&["HSET", "strkey", "f", "v"]).is_error());
    assert!(c.cmds(&["HGET", "strkey", "f"]).is_error());
    assert!(c.cmds(&["HLEN", "strkey"]).is_error());
}

// ── Vector search tests ───────────────────────────────────────────────────────

#[test]
fn test_ft_create_list_info_drop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Create
    assert!(
        c.cmds(&[
            "FT.CREATE",
            "idx:test",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "item:",
            "SCHEMA",
            "label",
            "TEXT",
            "vec",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "L2",
        ])
        .is_ok()
    );

    // _LIST
    let list = c.cmds(&["FT._LIST"]);
    let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
    assert!(names.contains(&"idx:test"));

    // INFO
    let info = c.cmds(&["FT.INFO", "idx:test"]);
    let arr = info.as_array();
    // Find index_name key and check value
    let pos = arr.iter().position(|v| v.as_str() == "index_name").unwrap();
    assert_eq!(arr[pos + 1].as_str(), "idx:test");

    // DROPINDEX
    assert!(c.cmds(&["FT.DROPINDEX", "idx:test"]).is_ok());
    let list2 = c.cmds(&["FT._LIST"]);
    assert!(list2.as_array().is_empty());
}

/// FLUSHALL must drop FT indexes, not just the hash keys. Vector graphs live
/// in `index_store` and were previously left resident after a vector benchmark.
#[test]
fn test_flushall_drops_hnsw_index() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&[
            "FT.CREATE",
            "idx:hnsw",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "v:",
            "SCHEMA",
            "vec",
            "VECTOR",
            "HNSW",
            "12",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "L2",
            "M",
            "16",
            "EF_CONSTRUCTION",
            "64",
        ])
        .is_ok()
    );

    for i in 0..32 {
        let key = format!("v:{i}");
        let v = [i as f32, 0.0, 0.0, 0.0];
        c.cmd(&[b"HSET", key.as_bytes(), b"vec", &pack_f32(&v)]);
    }

    let info = c.cmds(&["FT.INFO", "idx:hnsw"]);
    let arr = info.as_array();
    let pos = arr.iter().position(|v| v.as_str() == "index_name").unwrap();
    assert_eq!(arr[pos + 1].as_str(), "idx:hnsw");

    assert_eq!(c.cmds(&["DBSIZE"]).as_int(), 32);
    assert!(c.cmds(&["FLUSHALL"]).is_ok());
    assert_eq!(c.cmds(&["DBSIZE"]).as_int(), 0);

    let list = c.cmds(&["FT._LIST"]);
    assert!(
        list.as_array().is_empty(),
        "FLUSHALL must drop FT indexes, got {list:?}"
    );
    let missing = c.cmds(&["FT.INFO", "idx:hnsw"]);
    assert!(
        missing.is_error(),
        "index schema must not survive FLUSHALL: {missing:?}"
    );
}

/// FLUSHDB only drops indexes created in that database.
#[test]
fn test_flushdb_drops_indexes_for_selected_db_only() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx:db0",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "a:",
        "SCHEMA",
        "t",
        "TEXT",
    ]);
    c.cmds(&["SELECT", "1"]);
    c.cmds(&[
        "FT.CREATE",
        "idx:db1",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "b:",
        "SCHEMA",
        "t",
        "TEXT",
    ]);
    c.cmds(&["SET", "b:k", "1"]);

    assert!(c.cmds(&["FLUSHDB"]).is_ok());
    let list = c.cmds(&["FT._LIST"]);
    let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        !names.contains(&"idx:db1"),
        "FLUSHDB on db 1 must drop idx:db1, got {names:?}"
    );
    assert!(
        names.contains(&"idx:db0"),
        "FLUSHDB on db 1 must keep idx:db0, got {names:?}"
    );
}

#[test]
fn test_flat_knn_l2() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    // Insert 4 orthogonal unit vectors
    let vecs: &[(&str, &[f32])] = &[
        ("v:1", &[1.0, 0.0, 0.0, 0.0]),
        ("v:2", &[0.0, 1.0, 0.0, 0.0]),
        ("v:3", &[0.0, 0.0, 1.0, 0.0]),
        ("v:4", &[0.9, 0.1, 0.0, 0.0]),
    ];
    for (key, vec) in vecs {
        c.cmd(&[b"HSET", key.as_bytes(), b"vec", &pack_f32(vec)]);
    }

    // Query: [1, 0, 0, 0] — should return v:1 (dist=0) and v:4 (dist≈0.02)
    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 2 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    // arr[0] = count, arr[1] = key, arr[2] = fields, arr[3] = key, arr[4] = fields
    assert_eq!(arr[0].as_int(), 2);
    assert_eq!(arr[1].as_str(), "v:1");

    // Extract score from fields array: [b"score", b"0.000000"]
    let fields1 = arr[2].as_array();
    let score1 = parse_score(&fields1[1]);
    assert!(score1 < 1e-5, "v:1 score should be ~0, got {}", score1);

    assert_eq!(arr[3].as_str(), "v:4");
    let fields4 = arr[4].as_array();
    let score4 = parse_score(&fields4[1]);
    assert!(score4 < 0.05, "v:4 score should be ~0.02, got {}", score4);
}

#[test]
fn test_flat_knn_cosine() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "COSINE",
    ]);

    c.cmd(&[b"HSET", b"v:1", b"vec", &pack_f32(&[1.0, 0.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"v:2", b"vec", &pack_f32(&[0.0, 1.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"v:3", b"vec", &pack_f32(&[0.9, 0.1, 0.0, 0.0])]);

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 2 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    assert_eq!(arr[0].as_int(), 2);
    assert_eq!(arr[1].as_str(), "v:1");
    assert_eq!(arr[3].as_str(), "v:3");
}

#[test]
fn test_hnsw_knn() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
        "M",
        "16",
        "EF_CONSTRUCTION",
        "200",
    ]);

    let vecs: &[(&str, &[f32])] = &[
        ("v:1", &[1.0, 0.0, 0.0, 0.0]),
        ("v:2", &[0.0, 1.0, 0.0, 0.0]),
        ("v:3", &[0.0, 0.0, 1.0, 0.0]),
        ("v:4", &[0.9, 0.1, 0.0, 0.0]),
    ];
    for (key, vec) in vecs {
        c.cmd(&[b"HSET", key.as_bytes(), b"vec", &pack_f32(vec)]);
    }

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 2 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    assert_eq!(arr[0].as_int(), 2);
    assert_eq!(arr[1].as_str(), "v:1");

    let score1 = parse_score(&arr[2].as_array()[1]);
    assert!(score1 < 1e-5);
}

#[test]
fn test_tombstone_counts_track_correctly() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
        "M",
        "16",
        "EF_CONSTRUCTION",
        "200",
    ]);
    c.cmd(&[b"HSET", b"v:1", b"vec", &pack_f32(&[1.0, 0.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"v:2", b"vec", &pack_f32(&[0.0, 1.0, 0.0, 0.0])]);
    c.cmds(&["DEL", "v:1"]);
    // A KNN should still find v:2 even though entry point may be tombstoned.
    let q = pack_f32(&[0.0, 1.0, 0.0, 0.0]);
    let r = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 1 @vec $q AS s]",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"DIALECT",
        b"2",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_str(), "v:2");
}

#[test]
fn test_ft_info_hnsw_stats_and_debug_compact() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
        "M",
        "16",
        "EF_CONSTRUCTION",
        "200",
    ]);

    c.cmd(&[b"HSET", b"v:1", b"vec", &pack_f32(&[1.0, 0.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"v:2", b"vec", &pack_f32(&[0.0, 1.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"v:3", b"vec", &pack_f32(&[0.0, 0.0, 1.0, 0.0])]);

    fn find<'a>(arr: &'a [RespValue], key: &str) -> Option<&'a RespValue> {
        let mut i = 0;
        while i + 1 < arr.len() {
            let matches = match &arr[i] {
                RespValue::Bulk(b) => b.as_slice() == key.as_bytes(),
                RespValue::SimpleStr(s) => s == key,
                _ => false,
            };
            if matches {
                return Some(&arr[i + 1]);
            }
            i += 2;
        }
        None
    }

    let info = c.cmds(&["FT.INFO", "idx"]);
    let arr = info.as_array();

    // Flat back-compat keys — these mirror the FIRST hnsw field
    assert_eq!(find(arr, "hnsw_live_count").unwrap().as_str(), "3");
    assert_eq!(find(arr, "hnsw_tombstone_count").unwrap().as_str(), "0");

    // Canonical nested array — one entry per hnsw field
    let nested = find(arr, "hnsw_indexes").expect("hnsw_indexes missing");
    let entries = nested.as_array();
    assert_eq!(entries.len(), 1, "one hnsw field declared");
    let entry = entries[0].as_array();
    assert_eq!(find(entry, "field").unwrap().as_str(), "vec");
    assert_eq!(find(entry, "live_count").unwrap().as_str(), "3");
    assert_eq!(find(entry, "tombstone_count").unwrap().as_str(), "0");

    // Delete bumps tombstone count
    c.cmds(&["DEL", "v:2"]);
    let info2 = c.cmds(&["FT.INFO", "idx"]);
    let arr2 = info2.as_array();
    assert_eq!(find(arr2, "hnsw_live_count").unwrap().as_str(), "2");
    assert_eq!(find(arr2, "hnsw_tombstone_count").unwrap().as_str(), "1");

    // Force compaction with the now-required field argument
    assert!(c.cmds(&["FT.DEBUG", "COMPACT", "idx", "vec"]).is_ok());
    std::thread::sleep(std::time::Duration::from_millis(100));

    let info3 = c.cmds(&["FT.INFO", "idx"]);
    let arr3 = info3.as_array();
    assert_eq!(
        find(arr3, "hnsw_tombstone_count").unwrap().as_str(),
        "0",
        "compaction should have cleared tombstones"
    );

    // Error paths
    assert!(c.cmds(&["FT.DEBUG", "COMPACT", "nosuch", "vec"]).is_error());
    assert!(c.cmds(&["FT.DEBUG", "COMPACT", "idx", "nosuch"]).is_error());
    assert!(c.cmds(&["FT.DEBUG", "COMPACT", "idx"]).is_error()); // missing field
    assert!(c.cmds(&["FT.DEBUG", "BOGUS", "idx", "vec"]).is_error());
    assert!(c.cmds(&["FT.DEBUG"]).is_error());
}

#[test]
fn test_ft_alter_adds_second_vector_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "v1",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    // Populate some docs with both fields, even though v2 isn't indexed yet.
    c.cmd(&[
        b"HSET",
        b"d:1",
        b"v1",
        &pack_f32(&[1.0, 0.0, 0.0, 0.0]),
        b"v2",
        &pack_f32(&[0.0, 1.0, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"d:2",
        b"v1",
        &pack_f32(&[0.0, 1.0, 0.0, 0.0]),
        b"v2",
        &pack_f32(&[1.0, 0.0, 0.0, 0.0]),
    ]);

    // Add v2 to the schema. Backfill should pick up the existing values.
    c.cmds(&[
        "FT.ALTER",
        "idx",
        "SCHEMA",
        "ADD",
        "v2",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    // KNN on v1: closest to [1,0,0,0] should be d:1.
    let q1 = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r1 = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 1 @v1 $q]",
        b"PARAMS",
        b"2",
        b"q",
        &q1,
        b"DIALECT",
        b"2",
    ]);
    let arr1 = r1.as_array();
    assert_eq!(arr1[0].as_int(), 1);
    assert_eq!(arr1[1].as_str(), "d:1");

    // KNN on v2: closest to [1,0,0,0] should be d:2 (because d:2's v2 = [1,0,0,0]).
    let r2 = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 1 @v2 $q]",
        b"PARAMS",
        b"2",
        b"q",
        &q1,
        b"DIALECT",
        b"2",
    ]);
    let arr2 = r2.as_array();
    assert_eq!(arr2[0].as_int(), 1);
    assert_eq!(
        arr2[1].as_str(),
        "d:2",
        "v2's KNN should resolve to d:2, not d:1 — proves the right field's index is queried"
    );

    // Adding a *third* vector field should also work.
    c.cmds(&[
        "FT.ALTER",
        "idx",
        "SCHEMA",
        "ADD",
        "v3",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);
    // No data for v3 yet — search should return zero results, not error.
    let r3 = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 1 @v3 $q]",
        b"PARAMS",
        b"2",
        b"q",
        &q1,
        b"DIALECT",
        b"2",
    ]);
    let arr3 = r3.as_array();
    assert_eq!(arr3[0].as_int(), 0);
}

#[test]
fn test_ft_spellcheck_uses_incremental_vocab() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "body",
        "TEXT",
    ]);

    // Seed some docs at index creation time, then add more dynamically.
    c.cmds(&["HSET", "doc:1", "body", "hello world programming"]);
    c.cmds(&["HSET", "doc:2", "body", "rust is great"]);

    // First spell-check — should find suggestions from the seeded docs
    let r1 = c.cmds(&["FT.SPELLCHECK", "idx", "helo", "DISTANCE", "1"]);
    let arr1 = r1.as_array();
    assert!(!arr1.is_empty());
    let suggestions1 = arr1[0].as_array()[2].as_array();
    assert!(
        suggestions1
            .iter()
            .any(|s| s.as_array()[1].as_str() == "hello")
    );

    // Add a new doc with new vocabulary
    c.cmds(&["HSET", "doc:3", "body", "fantastic"]);

    // The new word should now appear in spell-check candidates
    let r2 = c.cmds(&["FT.SPELLCHECK", "idx", "fantasic", "DISTANCE", "2"]);
    let arr2 = r2.as_array();
    assert!(!arr2.is_empty());
    let suggestions2 = arr2[0].as_array()[2].as_array();
    assert!(
        suggestions2
            .iter()
            .any(|s| s.as_array()[1].as_str() == "fantastic"),
        "newly added word should be found in vocabulary"
    );

    // Document deletion should NOT remove tokens from vocabulary
    // (this is intentional — see comments in vector/mod.rs).
    c.cmds(&["DEL", "doc:1"]);
    let r3 = c.cmds(&["FT.SPELLCHECK", "idx", "helo", "DISTANCE", "1"]);
    let arr3 = r3.as_array();
    assert!(!arr3.is_empty(), "vocab should retain tokens after delete");
    let suggestions3 = arr3[0].as_array()[2].as_array();
    assert!(
        suggestions3
            .iter()
            .any(|s| s.as_array()[1].as_str() == "hello"),
        "hello should still be in vocab after doc:1 delete (append-only vocab)"
    );
}

#[test]
fn test_backfill_on_ft_create() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Write hashes BEFORE creating the index
    c.cmd(&[b"HSET", b"item:1", b"vec", &pack_f32(&[1.0, 0.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"item:2", b"vec", &pack_f32(&[0.0, 1.0, 0.0, 0.0])]);

    // Create index AFTER — backfill should pick up existing keys
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "item:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 1 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_str(), "item:1");
}

#[test]
fn test_delete_removes_from_index() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    c.cmd(&[b"HSET", b"v:1", b"vec", &pack_f32(&[1.0, 0.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"v:2", b"vec", &pack_f32(&[0.0, 1.0, 0.0, 0.0])]);
    c.cmd(&[b"HSET", b"v:3", b"vec", &pack_f32(&[0.0, 0.0, 1.0, 0.0])]);

    // Delete the exact match
    c.cmds(&["DEL", "v:1"]);

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 1 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    assert_eq!(arr[0].as_int(), 1);
    // v:1 is gone — next closest should be returned, not v:1
    assert_ne!(arr[1].as_str(), "v:1");
}

#[test]
fn test_hybrid_tag_filter() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "category",
        "TAG",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    // Two electronics, one furniture — all close to query vector
    c.cmd(&[
        b"HSET",
        b"p:1",
        b"category",
        b"electronics",
        b"vec",
        &pack_f32(&[1.0, 0.0, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:2",
        b"category",
        b"furniture",
        b"vec",
        &pack_f32(&[0.95, 0.05, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:3",
        b"category",
        b"electronics",
        b"vec",
        &pack_f32(&[0.9, 0.1, 0.0, 0.0]),
    ]);

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"(@category:{electronics})=>[KNN 3 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    // Only 2 electronics, even though we asked for 3
    assert_eq!(arr[0].as_int(), 2);

    // furniture (p:2) must not appear
    let keys: Vec<&str> = arr[1..].chunks(2).map(|chunk| chunk[0].as_str()).collect();
    assert!(!keys.contains(&"p:2"), "furniture should be filtered out");
    assert!(keys.contains(&"p:1"));
    assert!(keys.contains(&"p:3"));
}

#[test]
fn test_hybrid_numeric_filter() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "price",
        "NUMERIC",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    c.cmd(&[
        b"HSET",
        b"p:1",
        b"price",
        b"99",
        b"vec",
        &pack_f32(&[1.0, 0.0, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:2",
        b"price",
        b"199",
        b"vec",
        &pack_f32(&[0.99, 0.01, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:3",
        b"price",
        b"49",
        b"vec",
        &pack_f32(&[0.95, 0.05, 0.0, 0.0]),
    ]);

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"(@price:[0 100])=>[KNN 3 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    // p:1 ($99) and p:3 ($49) pass — p:2 ($199) does not
    assert_eq!(arr[0].as_int(), 2);
    let keys: Vec<&str> = arr[1..].chunks(2).map(|chunk| chunk[0].as_str()).collect();
    assert!(!keys.contains(&"p:2"));
    assert!(keys.contains(&"p:1"));
    assert!(keys.contains(&"p:3"));
}

#[test]
fn test_hybrid_text_filter() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    c.cmd(&[
        b"HSET",
        b"p:1",
        b"name",
        b"hdmi cable",
        b"vec",
        &pack_f32(&[1.0, 0.0, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:2",
        b"name",
        b"usb cable",
        b"vec",
        &pack_f32(&[0.9, 0.1, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:3",
        b"name",
        b"keyboard",
        b"vec",
        &pack_f32(&[0.8, 0.2, 0.0, 0.0]),
    ]);

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"(@name:cable)=>[KNN 3 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    assert_eq!(arr[0].as_int(), 2); // only cable products
    let keys: Vec<&str> = arr[1..].chunks(2).map(|chunk| chunk[0].as_str()).collect();
    assert!(!keys.contains(&"p:3")); // keyboard filtered out
}

#[test]
fn test_hybrid_combined_filter() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "category",
        "TAG",
        "price",
        "NUMERIC",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    c.cmd(&[
        b"HSET",
        b"p:1",
        b"category",
        b"electronics",
        b"price",
        b"99",
        b"vec",
        &pack_f32(&[1.0, 0.0, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:2",
        b"category",
        b"electronics",
        b"price",
        b"299",
        b"vec",
        &pack_f32(&[0.95, 0.05, 0.0, 0.0]),
    ]);
    c.cmd(&[
        b"HSET",
        b"p:3",
        b"category",
        b"furniture",
        b"price",
        b"49",
        b"vec",
        &pack_f32(&[0.9, 0.1, 0.0, 0.0]),
    ]);

    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // Electronics AND price <= 100 → only p:1 qualifies
    let result = c.cmd(&[
        b"FT.SEARCH",
        b"idx",
        b"(@category:{electronics} @price:[0 100])=>[KNN 3 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"RETURN",
        b"1",
        b"score",
        b"DIALECT",
        b"2",
    ]);

    let arr = result.as_array();
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_str(), "p:1");
}

// ── NEW TESTS: paste these at the bottom of tests/vector-hash.rs ─────────────
//
// Covers commands added since the original file:
//   FT.ALTER, FT.AGGREGATE, FT.TAGVALS,
//   FT.SYNUPDATE, FT.SYNDUMP, FT.SPELLCHECK,
//   FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE,
//   FT.DICTADD, FT.DICTDEL, FT.DICTDUMP

// ── FT.ALTER ─────────────────────────────────────────────────────────────────

#[test]
fn test_ft_alter_adds_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);

    // Helper: safely find a string key in a flat key-value INFO array,
    // skipping over nested array values without panicking.
    fn find_info_value<'a>(arr: &'a [RespValue], key: &str) -> Option<&'a RespValue> {
        let mut i = 0;
        while i + 1 < arr.len() {
            let is_match = match &arr[i] {
                RespValue::Bulk(b) => b.as_slice() == key.as_bytes(),
                RespValue::SimpleStr(s) => s == key,
                _ => false,
            };
            if is_match {
                return Some(&arr[i + 1]);
            }
            // Skip by 2 — but the value at i+1 might itself be an array,
            // which is fine since we're just advancing the cursor.
            i += 2;
        }
        None
    }

    let info = c.cmds(&["FT.INFO", "idx"]);
    let arr = info.as_array();
    let nf = find_info_value(arr, "num_fields").expect("num_fields missing");
    assert_eq!(nf.as_str(), "1");

    assert!(
        c.cmds(&[
            "FT.ALTER", "idx", "SCHEMA", "ADD", "category", "TAG", "price", "NUMERIC",
        ])
        .is_ok()
    );

    let info2 = c.cmds(&["FT.INFO", "idx"]);
    let arr2 = info2.as_array();
    let nf2 = find_info_value(arr2, "num_fields").expect("num_fields missing after ALTER");
    assert_eq!(nf2.as_str(), "3");
}

#[test]
fn test_ft_alter_duplicate_field_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);

    // Trying to add a field that already exists should error
    let r = c.cmds(&["FT.ALTER", "idx", "SCHEMA", "ADD", "name", "TAG"]);
    assert!(r.is_error(), "duplicate field name should return error");
}

#[test]
fn test_ft_alter_missing_index_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["FT.ALTER", "nosuch", "SCHEMA", "ADD", "field", "TEXT"]);
    assert!(r.is_error());
}

#[test]
fn test_ft_alter_skipinitialscan() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);

    // SKIPINITIALSCAN should be accepted without error
    assert!(
        c.cmds(&[
            "FT.ALTER",
            "idx",
            "SKIPINITIALSCAN",
            "SCHEMA",
            "ADD",
            "score",
            "NUMERIC",
        ])
        .is_ok()
    );
}

// ── FT.AGGREGATE ─────────────────────────────────────────────────────────────

#[test]
fn test_ft_aggregate_count_all() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "item:",
        "SCHEMA",
        "category",
        "TAG",
        "price",
        "NUMERIC",
    ]);

    for (key, cat, price) in &[
        ("item:1", "electronics", "99"),
        ("item:2", "electronics", "199"),
        ("item:3", "furniture", "49"),
        ("item:4", "furniture", "299"),
        ("item:5", "electronics", "149"),
    ] {
        c.cmds(&["HSET", key, "category", cat, "price", price]);
    }

    // GROUP BY category, REDUCE COUNT → 2 groups
    let r = c.cmds(&[
        "FT.AGGREGATE",
        "idx",
        "*",
        "GROUPBY",
        "1",
        "@category",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "cnt",
    ]);
    let arr = r.as_array();

    // First element is the count of result rows
    assert_eq!(arr[0].as_int(), 2, "should have 2 category groups");

    // Each row is a flat key-value array; find the electronics row
    let rows: Vec<&[RespValue]> = arr[1..].iter().map(|v| v.as_array()).collect();
    let mut electronics_count = None;
    let mut furniture_count = None;
    for row in rows {
        // row is [field, value, field, value, ...]
        let cat_pos = row.iter().position(|v| v.as_str() == "category");
        let cnt_pos = row.iter().position(|v| v.as_str() == "cnt");
        if let (Some(cp), Some(np)) = (cat_pos, cnt_pos) {
            let cat = row[cp + 1].as_str();
            let cnt: i64 = row[np + 1].as_str().parse().unwrap();
            match cat {
                "electronics" => electronics_count = Some(cnt),
                "furniture" => furniture_count = Some(cnt),
                _ => {}
            }
        }
    }
    assert_eq!(electronics_count, Some(3), "3 electronics items");
    assert_eq!(furniture_count, Some(2), "2 furniture items");
}

#[test]
fn test_ft_aggregate_sum_avg() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "dept",
        "TAG",
        "salary",
        "NUMERIC",
    ]);

    c.cmds(&["HSET", "p:1", "dept", "eng", "salary", "100000"]);
    c.cmds(&["HSET", "p:2", "dept", "eng", "salary", "120000"]);
    c.cmds(&["HSET", "p:3", "dept", "hr", "salary", "80000"]);

    let r = c.cmds(&[
        "FT.AGGREGATE",
        "idx",
        "*",
        "GROUPBY",
        "1",
        "@dept",
        "REDUCE",
        "SUM",
        "1",
        "@salary",
        "AS",
        "total",
        "REDUCE",
        "AVG",
        "1",
        "@salary",
        "AS",
        "avg_salary",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 2);

    let rows: Vec<&[RespValue]> = arr[1..].iter().map(|v| v.as_array()).collect();
    for row in rows {
        let dept_pos = row.iter().position(|v| v.as_str() == "dept").unwrap();
        let dept = row[dept_pos + 1].as_str();
        let total_pos = row.iter().position(|v| v.as_str() == "total").unwrap();
        let total: f64 = row[total_pos + 1].as_str().parse().unwrap();
        let avg_pos = row.iter().position(|v| v.as_str() == "avg_salary").unwrap();
        let avg: f64 = row[avg_pos + 1].as_str().parse().unwrap();

        match dept {
            "eng" => {
                assert!((total - 220000.0).abs() < 1.0, "eng total {total}");
                assert!((avg - 110000.0).abs() < 1.0, "eng avg {avg}");
            }
            "hr" => {
                assert!((total - 80000.0).abs() < 1.0, "hr total {total}");
                assert!((avg - 80000.0).abs() < 1.0, "hr avg {avg}");
            }
            _ => panic!("unexpected dept {dept}"),
        }
    }
}

#[test]
fn test_ft_aggregate_min_max() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "score",
        "NUMERIC",
    ]);

    c.cmds(&["HSET", "p:1", "score", "10"]);
    c.cmds(&["HSET", "p:2", "score", "50"]);
    c.cmds(&["HSET", "p:3", "score", "30"]);

    // No GROUPBY — aggregate everything into one group
    let r = c.cmds(&[
        "FT.AGGREGATE",
        "idx",
        "*",
        "GROUPBY",
        "0",
        "REDUCE",
        "MIN",
        "1",
        "@score",
        "AS",
        "lo",
        "REDUCE",
        "MAX",
        "1",
        "@score",
        "AS",
        "hi",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1, "one group when GROUPBY 0");

    let row = arr[1].as_array();
    let lo_pos = row.iter().position(|v| v.as_str() == "lo").unwrap();
    let hi_pos = row.iter().position(|v| v.as_str() == "hi").unwrap();
    let lo: f64 = row[lo_pos + 1].as_str().parse().unwrap();
    let hi: f64 = row[hi_pos + 1].as_str().parse().unwrap();
    assert!((lo - 10.0).abs() < 1.0, "min should be 10, got {lo}");
    assert!((hi - 50.0).abs() < 1.0, "max should be 50, got {hi}");
}

#[test]
fn test_ft_aggregate_sortby_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "category",
        "TAG",
        "price",
        "NUMERIC",
    ]);

    for (key, cat, price) in &[
        ("p:1", "a", "30"),
        ("p:2", "b", "10"),
        ("p:3", "c", "20"),
        ("p:4", "d", "40"),
        ("p:5", "e", "50"),
    ] {
        c.cmds(&["HSET", key, "category", cat, "price", price]);
    }

    // GROUPBY category, COUNT, then SORTBY count ASC, LIMIT 0 3
    let r = c.cmds(&[
        "FT.AGGREGATE",
        "idx",
        "*",
        "GROUPBY",
        "1",
        "@category",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "cnt",
        "SORTBY",
        "2",
        "@category",
        "ASC",
        "LIMIT",
        "0",
        "3",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 3, "LIMIT 3 should return 3 rows");

    // Rows should be sorted by category ASC: a, b, c
    let rows: Vec<&[RespValue]> = arr[1..].iter().map(|v| v.as_array()).collect();
    let cats: Vec<&str> = rows
        .iter()
        .map(|row| {
            let pos = row.iter().position(|v| v.as_str() == "category").unwrap();
            row[pos + 1].as_str()
        })
        .collect();
    assert_eq!(
        cats,
        vec!["a", "b", "c"],
        "should be sorted ascending by category"
    );
}

#[test]
fn test_ft_aggregate_with_filter_query() {
    // Aggregate with a pre-filter query (not just *)
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "active",
        "TAG",
        "region",
        "TAG",
        "revenue",
        "NUMERIC",
    ]);

    c.cmds(&[
        "HSET", "p:1", "active", "yes", "region", "us", "revenue", "1000",
    ]);
    c.cmds(&[
        "HSET", "p:2", "active", "yes", "region", "eu", "revenue", "2000",
    ]);
    c.cmds(&[
        "HSET", "p:3", "active", "no", "region", "us", "revenue", "500",
    ]);
    c.cmds(&[
        "HSET", "p:4", "active", "yes", "region", "us", "revenue", "1500",
    ]);

    // Only aggregate active=yes items
    let r = c.cmds(&[
        "FT.AGGREGATE",
        "idx",
        "@active:{yes}",
        "GROUPBY",
        "1",
        "@region",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "cnt",
        "REDUCE",
        "SUM",
        "1",
        "@revenue",
        "AS",
        "total",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 2, "2 regions with active=yes");

    let rows: Vec<&[RespValue]> = arr[1..].iter().map(|v| v.as_array()).collect();
    for row in rows {
        let region_pos = row.iter().position(|v| v.as_str() == "region").unwrap();
        let region = row[region_pos + 1].as_str();
        let cnt_pos = row.iter().position(|v| v.as_str() == "cnt").unwrap();
        let cnt: i64 = row[cnt_pos + 1].as_str().parse().unwrap();
        let total_pos = row.iter().position(|v| v.as_str() == "total").unwrap();
        let total: f64 = row[total_pos + 1].as_str().parse().unwrap();

        match region {
            "us" => {
                assert_eq!(cnt, 2, "2 active us items");
                assert!((total - 2500.0).abs() < 1.0, "us revenue {total}");
            }
            "eu" => {
                assert_eq!(cnt, 1, "1 active eu item");
                assert!((total - 2000.0).abs() < 1.0, "eu revenue {total}");
            }
            _ => panic!("unexpected region {region}"),
        }
    }
}

// ── FT.TAGVALS ───────────────────────────────────────────────────────────────

#[test]
fn test_ft_tagvals_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "color",
        "TAG",
    ]);

    c.cmds(&["HSET", "p:1", "color", "red"]);
    c.cmds(&["HSET", "p:2", "color", "blue"]);
    c.cmds(&["HSET", "p:3", "color", "red"]); // duplicate — should deduplicate
    c.cmds(&["HSET", "p:4", "color", "green"]);

    let r = c.cmds(&["FT.TAGVALS", "idx", "color"]);
    let arr = r.as_array();
    let mut vals: Vec<&str> = arr.iter().map(|v| v.as_str()).collect();
    vals.sort();

    assert_eq!(
        vals,
        vec!["blue", "green", "red"],
        "should return deduplicated sorted values"
    );
}

#[test]
fn test_ft_tagvals_empty_index() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "category",
        "TAG",
    ]);

    // No keys — should return empty array
    let r = c.cmds(&["FT.TAGVALS", "idx", "category"]);
    assert_eq!(r.as_array().len(), 0);
}

#[test]
fn test_ft_tagvals_wrong_field_type() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
        "score",
        "NUMERIC",
    ]);

    // Querying a TEXT field should error (not TAG)
    assert!(c.cmds(&["FT.TAGVALS", "idx", "name"]).is_error());
    assert!(c.cmds(&["FT.TAGVALS", "idx", "score"]).is_error());
}

#[test]
fn test_ft_tagvals_pipe_separated() {
    // A TAG field's values are separated by a comma unless the schema says
    // otherwise — here it says `|`.
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "tags",
        "TAG",
        "SEPARATOR",
        "|",
    ]);

    // Pipe-separated multi-value TAG
    c.cmds(&["HSET", "p:1", "tags", "electronics|gadgets"]);
    c.cmds(&["HSET", "p:2", "tags", "electronics|furniture"]);

    let r = c.cmds(&["FT.TAGVALS", "idx", "tags"]);
    let mut vals: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    vals.sort();
    assert_eq!(vals, vec!["electronics", "furniture", "gadgets"]);
}

// ── FT.SYNUPDATE / FT.SYNDUMP ─────────────────────────────────────────────────

#[test]
fn test_ft_synupdate_syndump_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);

    // Add a synonym group
    assert!(
        c.cmds(&["FT.SYNUPDATE", "idx", "grp1", "hello", "hi", "hey"])
            .is_ok()
    );

    // SYNDUMP should show the inverted mapping: term → [group_id]
    let r = c.cmds(&["FT.SYNDUMP", "idx"]);
    let arr = r.as_array();
    // Format: [term1, [grp], term2, [grp], ...]
    assert!(arr.len() >= 2, "should have at least one term entry");

    // Collect term → groups mapping from the flat array
    let mut found_terms = std::collections::HashSet::new();
    let mut i = 0;
    while i + 1 < arr.len() {
        let term = arr[i].as_str();
        found_terms.insert(term);
        let groups = arr[i + 1].as_array();
        assert!(
            groups.iter().any(|g| g.as_str() == "grp1"),
            "term {term} should belong to grp1"
        );
        i += 2;
    }
    assert!(found_terms.contains("hello"));
    assert!(found_terms.contains("hi"));
    assert!(found_terms.contains("hey"));
}

#[test]
fn test_ft_synupdate_multiple_groups() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);

    c.cmds(&["FT.SYNUPDATE", "idx", "grp1", "world", "earth"]);
    c.cmds(&["FT.SYNUPDATE", "idx", "grp2", "world", "globe"]);

    let r = c.cmds(&["FT.SYNDUMP", "idx"]);
    let arr = r.as_array();

    // "world" belongs to both grp1 and grp2
    let mut i = 0;
    while i + 1 < arr.len() {
        if arr[i].as_str() == "world" {
            let groups = arr[i + 1].as_array();
            let gids: Vec<&str> = groups.iter().map(|g| g.as_str()).collect();
            assert!(gids.contains(&"grp1"), "world should be in grp1");
            assert!(gids.contains(&"grp2"), "world should be in grp2");
        }
        i += 2;
    }
}

#[test]
fn test_ft_syndump_empty() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);

    let r = c.cmds(&["FT.SYNDUMP", "idx"]);
    assert_eq!(r.as_array().len(), 0, "no synonyms → empty array");
}

// ── FT.SPELLCHECK ─────────────────────────────────────────────────────────────

#[test]
fn test_ft_spellcheck_finds_suggestions() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "body",
        "TEXT",
    ]);

    // Populate vocabulary
    c.cmds(&["HSET", "doc:1", "body", "hello world programming"]);
    c.cmds(&["HSET", "doc:2", "body", "database indexing search"]);

    // "helo" is one edit from "hello"
    let r = c.cmds(&["FT.SPELLCHECK", "idx", "helo", "DISTANCE", "1"]);
    let arr = r.as_array();

    // Should return at least one TERM entry
    assert!(!arr.is_empty(), "should have spell-check results");

    // Each entry: ["TERM", term, [[score, suggestion], ...]]
    let entry = arr[0].as_array();
    assert_eq!(entry[0].as_str(), "TERM");
    assert_eq!(entry[1].as_str(), "helo");
    let suggestions = entry[2].as_array();
    assert!(
        !suggestions.is_empty(),
        "should have at least one suggestion"
    );

    // Each suggestion: [score, word]
    let found_hello = suggestions.iter().any(|s| {
        let pair = s.as_array();
        pair[1].as_str() == "hello"
    });
    assert!(found_hello, "hello should be suggested for helo");
}

#[test]
fn test_ft_spellcheck_correct_word_skipped() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "body",
        "TEXT",
    ]);
    c.cmds(&["HSET", "doc:1", "body", "hello world"]);

    // "hello" is already in the vocabulary — should not appear in results
    let r = c.cmds(&["FT.SPELLCHECK", "idx", "hello"]);
    assert_eq!(
        r.as_array().len(),
        0,
        "correctly spelled word should produce no results"
    );
}

#[test]
fn test_ft_spellcheck_distance_zero_no_results() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "body",
        "TEXT",
    ]);
    c.cmds(&["HSET", "doc:1", "body", "hello world"]);

    // DISTANCE 0 means exact match only — typo gets no suggestions
    let r = c.cmds(&["FT.SPELLCHECK", "idx", "helo", "DISTANCE", "0"]);
    let arr = r.as_array();
    // Either empty or the term entry has no suggestions
    if !arr.is_empty() {
        let entry = arr[0].as_array();
        let suggestions = entry[2].as_array();
        assert!(
            suggestions.is_empty(),
            "distance 0 should give no suggestions"
        );
    }
}

// ── FT.ALIAS ─────────────────────────────────────────────────────────────────

#[test]
fn test_ft_alias_add_del_update() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "myidx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);

    // ALIASADD
    assert!(c.cmds(&["FT.ALIASADD", "myalias", "myidx"]).is_ok());

    // An alias is a name for the index, listed by FT.ALIASLIST. It is not an
    // index of its own, so FT._LIST does not show it.
    let aliases = c.cmds(&["FT.ALIASLIST", "myidx"]);
    let names: Vec<&str> = aliases.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(names, ["myalias"]);
    let list = c.cmds(&["FT._LIST"]);
    let names: Vec<&str> = list.as_array().iter().map(|v| v.as_str()).collect();
    assert!(!names.contains(&"myalias"), "an alias is not an index");

    // Can't add the same alias twice
    assert!(c.cmds(&["FT.ALIASADD", "myalias", "myidx"]).is_error());

    // ALIASUPDATE overwrites
    c.cmds(&[
        "FT.CREATE",
        "otheridx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "x:",
        "SCHEMA",
        "tag",
        "TAG",
    ]);
    assert!(c.cmds(&["FT.ALIASUPDATE", "myalias", "otheridx"]).is_ok());

    // ALIASDEL
    assert!(c.cmds(&["FT.ALIASDEL", "myalias"]).is_ok());
    let list2 = c.cmds(&["FT._LIST"]);
    let names2: Vec<&str> = list2.as_array().iter().map(|v| v.as_str()).collect();
    assert!(
        !names2.contains(&"myalias"),
        "alias should be gone after ALIASDEL"
    );
}

#[test]
fn test_ft_alias_missing_index_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Aliasing a non-existent index should error
    assert!(c.cmds(&["FT.ALIASADD", "alias", "doesnotexist"]).is_error());
    assert!(
        c.cmds(&["FT.ALIASUPDATE", "alias", "doesnotexist"])
            .is_error()
    );
}

#[test]
fn test_ft_alias_searchable() {
    // An alias should be usable in FT.SEARCH and FT.INFO
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "real",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    c.cmds(&["FT.ALIASADD", "alias", "real"]);

    let blob = pack_f32(&[1.0, 0.0]);
    c.cmd(&[b"HSET", b"v:1", b"vec", &blob]);

    // FT.INFO via alias
    let info = c.cmds(&["FT.INFO", "alias"]);
    assert!(!info.is_error(), "FT.INFO via alias should work");

    // FT.SEARCH via alias
    let query = pack_f32(&[1.0, 0.0]);
    let r = c.cmd(&[
        b"FT.SEARCH",
        b"alias",
        b"*=>[KNN 1 @vec $q AS score]",
        b"PARAMS",
        b"2",
        b"q",
        &query,
        b"DIALECT",
        b"2",
    ]);
    assert!(!r.is_error(), "FT.SEARCH via alias should work");
    assert_eq!(r.as_array()[0].as_int(), 1);
}

// ── FT.DICTADD / FT.DICTDEL / FT.DICTDUMP ────────────────────────────────────

#[test]
fn test_ft_dict_add_dump_del() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // DICTADD — returns count of newly added words
    let added = c.cmds(&["FT.DICTADD", "mydict", "apple", "banana", "cherry"]);
    assert_eq!(added.as_int(), 3);

    // Adding the same words again returns 0 (already exist)
    let added2 = c.cmds(&["FT.DICTADD", "mydict", "apple", "banana"]);
    assert_eq!(added2.as_int(), 0);

    // DICTDUMP returns all words sorted
    let r = c.cmds(&["FT.DICTDUMP", "mydict"]);
    let mut words: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    words.sort();
    assert_eq!(words, vec!["apple", "banana", "cherry"]);

    // DICTDEL — returns count of words removed
    let del = c.cmds(&["FT.DICTDEL", "mydict", "apple", "nosuch"]);
    assert_eq!(del.as_int(), 1, "only apple should be removed");

    // DICTDUMP after delete
    let r2 = c.cmds(&["FT.DICTDUMP", "mydict"]);
    let mut words2: Vec<&str> = r2.as_array().iter().map(|v| v.as_str()).collect();
    words2.sort();
    assert_eq!(words2, vec!["banana", "cherry"]);
}

#[test]
fn test_ft_dictdump_missing_dict() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // DICTDUMP on a dict that doesn't exist returns empty array
    let r = c.cmds(&["FT.DICTDUMP", "nosuch"]);
    assert_eq!(r.as_array().len(), 0);
}

#[test]
fn test_ft_dictdel_missing_dict() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // DICTDEL on a dict that doesn't exist returns 0
    let r = c.cmds(&["FT.DICTDEL", "nosuch", "word"]);
    assert_eq!(r.as_int(), 0);
}

#[test]
fn test_ft_dict_multiple_dicts() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Dicts are independent namespaces
    c.cmds(&["FT.DICTADD", "fruits", "apple", "pear"]);
    c.cmds(&["FT.DICTADD", "colors", "red", "blue"]);

    let fruits = c.cmds(&["FT.DICTDUMP", "fruits"]);
    let mut fw: Vec<&str> = fruits.as_array().iter().map(|v| v.as_str()).collect();
    fw.sort();
    assert_eq!(fw, vec!["apple", "pear"]);

    let colors = c.cmds(&["FT.DICTDUMP", "colors"]);
    let mut cw: Vec<&str> = colors.as_array().iter().map(|v| v.as_str()).collect();
    cw.sort();
    assert_eq!(cw, vec!["blue", "red"]);
}

// ── FT.SPELLCHECK with TERMS INCLUDE/EXCLUDE ──────────────────────────────────

#[test]
fn test_ft_spellcheck_terms_include() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "body",
        "TEXT",
    ]);
    // Sparse text so vocabulary is small
    c.cmds(&["HSET", "doc:1", "body", "car"]);

    // Add a dict with additional words
    c.cmds(&["FT.DICTADD", "extra", "cart", "card", "care"]);

    // Query "carz" — with INCLUDE extra, suggestions from dict are also considered
    let r = c.cmds(&[
        "FT.SPELLCHECK",
        "idx",
        "carz",
        "DISTANCE",
        "1",
        "TERMS",
        "INCLUDE",
        "extra",
    ]);
    let arr = r.as_array();
    if !arr.is_empty() {
        let entry = arr[0].as_array();
        let suggestions = entry[2].as_array();
        let words: Vec<&str> = suggestions
            .iter()
            .map(|s| s.as_array()[1].as_str())
            .collect();
        // Should suggest "cart", "card", or "care" from the included dict
        let has_dict_word = words
            .iter()
            .any(|&w| matches!(w, "cart" | "card" | "care" | "car"));
        assert!(
            has_dict_word,
            "included dict words should appear in suggestions: {:?}",
            words
        );
    }
}

#[test]
fn test_ft_hybrid_rrf_default() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let v_match = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let v_far = pack_f32(&[0.0, 0.0, 1.0, 0.0]);

    c.cmd(&[
        b"HSET",
        b"d:1",
        b"title",
        b"rust programming",
        b"v",
        &v_match,
    ]);
    c.cmd(&[b"HSET", b"d:2", b"title", b"python tutorial", b"v", &v_far]);
    c.cmd(&[b"HSET", b"d:3", b"title", b"rust deep dive", b"v", &v_match]);

    // Hybrid: text "rust" + vector near [1,0,0,0]
    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    let count = arr[0].as_int();
    assert!(count >= 2, "should rank at least the two matching docs");

    // The two top results must be d:1 and d:3 (matched text AND vector).
    let top1 = arr[1].as_str();
    let top2 = arr[3].as_str();
    let mut tops = vec![top1, top2];
    tops.sort();
    assert_eq!(tops, vec!["d:1", "d:3"], "top two should be d:1 and d:3");
}

#[test]
fn test_ft_hybrid_linear_alpha_text_heavy() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let v_close = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let v_medium = pack_f32(&[0.7, 0.7, 0.0, 0.0]);

    c.cmd(&[
        b"HSET",
        b"d:text",
        b"title",
        b"rust programming",
        b"v",
        &v_medium,
    ]);
    c.cmd(&[
        b"HSET",
        b"d:vec",
        b"title",
        b"unrelated content",
        b"v",
        &v_close,
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // alpha=0.9 → text dominates → d:text wins despite worse vector match
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"COMBINE",
        b"LINEAR",
        b"4",
        b"ALPHA",
        b"0.9",
        b"BETA",
        b"0.1",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    assert_eq!(
        arr[1].as_str(),
        "d:text",
        "with ALPHA=0.9 the text-matching doc should rank first"
    );

    // alpha=0.1 → vector dominates → d:vec wins despite worse text match.
    // Note: d:vec doesn't match the SEARCH filter so it actually shouldn't
    // appear at all — text_search filters first, vsim filters by KNN top-k.
    // This is the right semantics: hybrid is a fusion of two SETS, not
    // a re-ranking of a single set. Either side can produce a doc.
    let r2 = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"COMBINE",
        b"LINEAR",
        b"4",
        b"ALPHA",
        b"0.1",
        b"BETA",
        b"0.9",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr2 = r2.as_array();
    assert_eq!(
        arr2[1].as_str(),
        "d:vec",
        "with BETA=0.9 the vector-closer doc should rank first"
    );
}

#[test]
fn test_ft_hybrid_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    assert!(
        c.cmds(&[
            "FT.HYBRID",
            "nosuch",
            "SEARCH",
            "x",
            "VSIM",
            "@v",
            "$q",
            "PARAMS",
            "2",
            "q",
            "x"
        ])
        .is_error()
    );

    assert!(c.cmds(&["FT.HYBRID", "idx", "SEARCH", "x"]).is_error());

    assert!(
        c.cmds(&[
            "FT.HYBRID",
            "idx",
            "VSIM",
            "@v",
            "$q",
            "PARAMS",
            "2",
            "q",
            "x"
        ])
        .is_error()
    );

    assert!(
        c.cmds(&[
            "FT.HYBRID",
            "idx",
            "SEARCH",
            "@title:x",
            "VSIM",
            "@v",
            "$q",
            "PARAMS",
            "0"
        ])
        .is_error()
    );
}

#[test]
fn test_ft_hybrid_yield_score_as() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);
    let v = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    c.cmd(&[b"HSET", b"d:1", b"title", b"rust", b"v", &v]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"YIELD_SCORE_AS",
        b"my_text",
        b"VSIM",
        b"@v",
        b"$q",
        b"YIELD_SCORE_AS",
        b"my_vec",
        b"COMBINE",
        b"RRF",
        b"2",
        b"YIELD_SCORE_AS",
        b"my_hybrid",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1);
    let row = arr[2].as_array();

    // Field-name presence test — order-agnostic.
    // The row is a flat array of alternating field/value pairs; collect
    // the names at even indices and assert each alias is among them.
    let names: Vec<&str> = (0..row.len()).step_by(2).map(|i| row[i].as_str()).collect();
    assert!(
        names.contains(&"my_hybrid"),
        "my_hybrid missing in {names:?}"
    );
    assert!(names.contains(&"my_text"), "my_text missing in {names:?}");
    assert!(names.contains(&"my_vec"), "my_vec missing in {names:?}");
}

#[test]
fn test_ft_hybrid_sortby_text_score() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let v_close = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let v_far = pack_f32(&[0.0, 0.0, 1.0, 0.0]);

    // d:a — strong text match, far vector. d:b — weak text, close vector.
    c.cmd(&[b"HSET", b"d:a", b"title", b"rust rust rust", b"v", &v_far]);
    c.cmd(&[b"HSET", b"d:b", b"title", b"rust", b"v", &v_close]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // SORTBY text_score DESC → d:a first
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"SORTBY",
        b"2",
        b"text_score",
        b"DESC",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    assert_eq!(
        arr[1].as_str(),
        "d:a",
        "SORTBY text_score DESC should put d:a first"
    );

    // SORTBY vsim_score ASC (lower distance = better) → d:b first
    let r2 = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"SORTBY",
        b"2",
        b"vsim_score",
        b"ASC",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr2 = r2.as_array();
    assert_eq!(
        arr2[1].as_str(),
        "d:b",
        "SORTBY vsim_score ASC should put d:b first"
    );
}

#[test]
fn test_ft_hybrid_sortby_loaded_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "price",
        "NUMERIC",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);
    let v = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    c.cmd(&[
        b"HSET", b"d:1", b"title", b"rust", b"price", b"100", b"v", &v,
    ]);
    c.cmd(&[
        b"HSET", b"d:2", b"title", b"rust", b"price", b"200", b"v", &v,
    ]);
    c.cmd(&[
        b"HSET", b"d:3", b"title", b"rust", b"price", b"50", b"v", &v,
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // SORTBY price DESC — d:2 (200), d:1 (100), d:3 (50)
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"SORTBY",
        b"2",
        b"price",
        b"DESC",
        b"LOAD",
        b"1",
        b"@price",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    assert_eq!(arr[1].as_str(), "d:2");
    assert_eq!(arr[3].as_str(), "d:1");
    assert_eq!(arr[5].as_str(), "d:3");
}

#[test]
fn test_ft_hybrid_load_star_returns_all_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);
    let v = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    c.cmd(&[
        b"HSET", b"d:1", b"title", b"rust", b"author", b"Steve", b"v", &v,
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"LOAD",
        b"*",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1);
    let row = arr[2].as_array();
    // Scores first (6 elements), then loaded fields. Find "title" and "author".
    let mut found_title = false;
    let mut found_author = false;
    let mut i = 0;
    while i + 1 < row.len() {
        match row[i].as_str() {
            "title" => found_title = row[i + 1].as_str() == "rust",
            "author" => found_author = row[i + 1].as_str() == "Steve",
            _ => {}
        }
        i += 2;
    }
    assert!(found_title, "LOAD * should include title");
    assert!(found_author, "LOAD * should include author");
}

#[test]
fn test_ft_hybrid_load_specific_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "title",
        "TEXT",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);
    let v = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    c.cmd(&[
        b"HSET", b"d:1", b"title", b"rust", b"author", b"Steve", b"secret", b"shh", b"v", &v,
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:rust",
        b"VSIM",
        b"@v",
        b"$q",
        b"LOAD",
        b"2",
        b"@title",
        b"@author",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    let row = arr[2].as_array();

    let names: Vec<&str> = (0..row.len()).step_by(2).map(|i| row[i].as_str()).collect();
    assert!(names.contains(&"title"));
    assert!(names.contains(&"author"));
    assert!(!names.contains(&"secret"), "secret should not be loaded");
}

#[test]
fn test_ft_hybrid_with_full_pipeline() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "title",
        "TEXT",
        "category",
        "TAG",
        "price",
        "NUMERIC",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let v = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    c.cmd(&[
        b"HSET",
        b"p:1",
        b"title",
        b"laptop",
        b"category",
        b"electronics",
        b"price",
        b"1000",
        b"v",
        &v,
    ]);
    c.cmd(&[
        b"HSET",
        b"p:2",
        b"title",
        b"phone",
        b"category",
        b"electronics",
        b"price",
        b"500",
        b"v",
        &v,
    ]);
    c.cmd(&[
        b"HSET",
        b"p:3",
        b"title",
        b"tablet",
        b"category",
        b"electronics",
        b"price",
        b"300",
        b"v",
        &v,
    ]);
    c.cmd(&[
        b"HSET",
        b"p:4",
        b"title",
        b"chair",
        b"category",
        b"furniture",
        b"price",
        b"100",
        b"v",
        &v,
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // GROUPBY category, REDUCE COUNT and AVG, FILTER on count.
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"*",
        b"VSIM",
        b"@v",
        b"$q",
        b"LOAD",
        b"2",
        b"@category",
        b"@price",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"GROUPBY",
        b"1",
        b"@category",
        b"REDUCE",
        b"COUNT",
        b"0",
        b"AS",
        b"cnt",
        b"REDUCE",
        b"AVG",
        b"1",
        b"@price",
        b"AS",
        b"avg_price",
        b"FILTER",
        b"@cnt > 1",
    ]);
    let arr = r.as_array();

    // Only electronics has cnt > 1.
    assert_eq!(
        arr[0].as_int(),
        1,
        "FILTER @cnt > 1 should leave only electronics"
    );
    let row = arr[1].as_array();

    // Find category and avg_price in the flat field array
    let mut cat = "";
    let mut avg = 0.0;
    let mut cnt = 0i64;
    let mut i = 0;
    while i + 1 < row.len() {
        match row[i].as_str() {
            "category" => cat = row[i + 1].as_str(),
            "avg_price" => avg = row[i + 1].as_str().parse().unwrap_or(0.0),
            "cnt" => cnt = row[i + 1].as_str().parse().unwrap_or(0),
            _ => {}
        }
        i += 2;
    }
    assert_eq!(cat, "electronics");
    assert_eq!(cnt, 3);
    assert!((avg - 600.0).abs() < 0.5, "avg should be 600, got {avg}");
}

#[test]
fn test_ft_hybrid_apply_after_fusion() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "title",
        "TEXT",
        "price",
        "NUMERIC",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let v = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    c.cmd(&[b"HSET", b"p:1", b"title", b"a", b"price", b"100", b"v", &v]);
    c.cmd(&[b"HSET", b"p:2", b"title", b"a", b"price", b"200", b"v", &v]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // APPLY without GROUPBY — adds a computed field to each per-doc row.
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:a",
        b"VSIM",
        b"@v",
        b"$q",
        b"LOAD",
        b"1",
        b"@price",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"APPLY",
        b"@price * 1.1",
        b"AS",
        b"with_tax",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 2);

    // Each result row should include the with_tax field
    let mut found_with_tax = 0;
    for row_idx in [2, 4] {
        let row = arr[row_idx].as_array();
        for i in (0..row.len()).step_by(2) {
            if row[i].as_str() == "with_tax" {
                let v: f64 = row[i + 1].as_str().parse().unwrap();
                assert!(v > 0.0);
                found_with_tax += 1;
            }
        }
    }
    assert_eq!(
        found_with_tax, 2,
        "with_tax should be computed for both rows"
    );
}

#[test]
fn test_ft_hybrid_filter_before_groupby() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "title",
        "TEXT",
        "price",
        "NUMERIC",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let v = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    c.cmd(&[b"HSET", b"p:1", b"title", b"a", b"price", b"100", b"v", &v]);
    c.cmd(&[b"HSET", b"p:2", b"title", b"a", b"price", b"200", b"v", &v]);
    c.cmd(&[b"HSET", b"p:3", b"title", b"a", b"price", b"50", b"v", &v]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // FILTER as a pipeline clause — runs after fusion, prunes per-doc rows.
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:a",
        b"VSIM",
        b"@v",
        b"$q",
        b"LOAD",
        b"1",
        b"@price",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"FILTER",
        b"@price > 75",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 2, "FILTER @price > 75 should drop p:3");
}

#[test]
fn test_ft_aggregate_geodistance_and_timefmt() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "events",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "e:",
        "SCHEMA",
        "name",
        "TEXT",
        "lat",
        "NUMERIC",
        "lon",
        "NUMERIC",
        "ts",
        "NUMERIC",
    ]);

    // San Francisco → New York is roughly 4129 km
    c.cmds(&[
        "HSET",
        "e:1",
        "name",
        "trip",
        "lat",
        "37.7749",
        "lon",
        "-122.4194", // SF
        "ts",
        "1700000000", // 2023-11-14
    ]);

    // geodistance from doc point to NYC (40.7128, -74.0060)
    let r1 = c.cmds(&[
        "FT.AGGREGATE",
        "events",
        "*",
        "APPLY",
        "geodistance(@lat, @lon, 40.7128, -74.0060)",
        "AS",
        "dist_m",
    ]);
    let arr1 = r1.as_array();
    let row1 = arr1[1].as_array();
    let dist_pos = row1.iter().position(|v| v.as_str() == "dist_m").unwrap();
    let dist: f64 = row1[dist_pos + 1].as_str().parse().unwrap();
    assert!(
        (dist - 4_129_000.0).abs() < 30_000.0,
        "SF→NYC should be ~4129 km, got {} m",
        dist
    );

    // timefmt with default ISO format
    let r2 = c.cmds(&[
        "FT.AGGREGATE",
        "events",
        "*",
        "APPLY",
        "timefmt(@ts)",
        "AS",
        "iso",
    ]);
    let arr2 = r2.as_array();
    let row2 = arr2[1].as_array();
    let iso_pos = row2.iter().position(|v| v.as_str() == "iso").unwrap();
    let iso = row2[iso_pos + 1].as_str();
    assert_eq!(iso, "2023-11-14T22:13:20Z");

    // timefmt with custom format
    let r3 = c.cmds(&[
        "FT.AGGREGATE",
        "events",
        "*",
        "APPLY",
        r#"timefmt(@ts, "%Y-%m-%d")"#,
        "AS",
        "date",
    ]);
    let arr3 = r3.as_array();
    let row3 = arr3[1].as_array();
    let date_pos = row3.iter().position(|v| v.as_str() == "date").unwrap();
    assert_eq!(row3[date_pos + 1].as_str(), "2023-11-14");
}

#[test]
fn test_p1_multi_vector_hybrid_with_compaction() {
    // Exercises all P1 items in one flow:
    //   - multi-VECTOR schema (image + text embedding)
    //   - FT.HYBRID searching across one of them
    //   - delete enough docs to cross the compaction threshold
    //   - confirm compaction happens, search still returns correct results
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "title",
        "TEXT",
        "category",
        "TAG",
        "img_vec",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
        "M",
        "16",
        "EF_CONSTRUCTION",
        "200",
        "txt_vec",
        "VECTOR",
        "HNSW",
        "10",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "COSINE",
        "M",
        "16",
        "EF_CONSTRUCTION",
        "200",
    ]);

    // Seed 200 docs with both vectors populated
    for i in 0..200 {
        let img = pack_f32(&[(i as f32) / 200.0, ((200 - i) as f32) / 200.0, 0.0, 0.0]);
        let txt = pack_f32(&[
            ((i * 3) % 200) as f32 / 200.0,
            ((i * 7) % 200) as f32 / 200.0,
            0.0,
            0.0,
        ]);
        let cat = if i % 2 == 0 { "even" } else { "odd" };
        let key = format!("p:{i}");
        c.cmd(&[
            b"HSET",
            key.as_bytes(),
            b"title",
            format!("doc {i}").as_bytes(),
            b"category",
            cat.as_bytes(),
            b"img_vec",
            &img,
            b"txt_vec",
            &txt,
        ]);
    }

    // Hybrid search using only img_vec
    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@category:{even}",
        b"VSIM",
        b"@img_vec",
        b"$q",
        b"COMBINE",
        b"RRF",
        b"2",
        b"LIMIT",
        b"0",
        b"5",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    let count = arr[0].as_int();
    assert!(count > 0, "hybrid search should return results");

    // Delete 100 docs to cross the compaction threshold (>= 64 tombstones,
    // > 20% ratio means >40 tombstones out of 200).
    for i in 0..100 {
        c.cmds(&["DEL", &format!("p:{i}")]);
    }

    // Wait for the background compaction worker to drain
    std::thread::sleep(std::time::Duration::from_millis(500));

    fn find_info_value<'a>(arr: &'a [RespValue], key: &str) -> Option<&'a RespValue> {
        let mut i = 0;
        while i + 1 < arr.len() {
            let is_match = match &arr[i] {
                RespValue::Bulk(b) => b.as_slice() == key.as_bytes(),
                RespValue::SimpleStr(s) => s == key,
                _ => false,
            };
            if is_match {
                return Some(&arr[i + 1]);
            }
            i += 2;
        }
        None
    }

    // After compaction, tombstone count should be back to 0 for img_vec
    let info = c.cmds(&["FT.INFO", "idx"]);
    let arr = info.as_array();
    let nested = find_info_value(arr, "hnsw_indexes")
        .expect("hnsw_indexes missing")
        .as_array();
    for entry in nested {
        let entry_arr = entry.as_array();
        let mut field_name = "";
        let mut tombstone = "";
        let mut i = 0;
        while i + 1 < entry_arr.len() {
            match entry_arr[i].as_str() {
                "field" => field_name = entry_arr[i + 1].as_str(),
                "tombstone_count" => tombstone = entry_arr[i + 1].as_str(),
                _ => {}
            }
            i += 2;
        }
        assert_eq!(
            tombstone, "0",
            "field {field_name} should have 0 tombstones after compaction"
        );
    }

    // Search still returns correct results
    let r2 = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@category:{odd}",
        b"VSIM",
        b"@img_vec",
        b"$q",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr2 = r2.as_array();
    assert!(
        arr2[0].as_int() > 0,
        "search should still work post-compaction"
    );
    // No deleted keys in results
    for i in (1..arr2.len()).step_by(2) {
        let key = arr2[i].as_str();
        let n: usize = key.strip_prefix("p:").and_then(|s| s.parse().ok()).unwrap();
        assert!(n >= 100, "deleted key {key} should not appear in results");
    }
}

#[test]
fn test_spellcheck_vocab_across_multiple_text_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
        "body",
        "TEXT",
        "tags",
        "TEXT",
    ]);

    // Each TEXT field contributes different tokens to the vocab
    c.cmds(&[
        "HSET",
        "doc:1",
        "title",
        "ergonomic keyboard",
        "body",
        "mechanical switches comfortable typing",
        "tags",
        "office productivity",
    ]);
    c.cmds(&[
        "HSET",
        "doc:2",
        "title",
        "wireless mouse",
        "body",
        "bluetooth ergonomic",
        "tags",
        "office accessories",
    ]);

    // Misspelled "ergonomik" should suggest "ergonomic" — this token
    // appears in both title (doc:1) and body (doc:2).
    let r = c.cmds(&["FT.SPELLCHECK", "idx", "ergonomik", "DISTANCE", "1"]);
    let arr = r.as_array();
    assert!(!arr.is_empty(), "should find suggestions");
    let suggestions = arr[0].as_array()[2].as_array();
    let words: Vec<&str> = suggestions
        .iter()
        .map(|s| s.as_array()[1].as_str())
        .collect();
    assert!(
        words.contains(&"ergonomic"),
        "vocab should aggregate across all TEXT fields, got {words:?}"
    );

    // Add a doc with a brand-new term in only the tags field
    c.cmds(&[
        "HSET",
        "doc:3",
        "title",
        "stand",
        "body",
        "for laptop",
        "tags",
        "ergonomical",
    ]);

    let r2 = c.cmds(&["FT.SPELLCHECK", "idx", "ergonomicl", "DISTANCE", "1"]);
    let arr2 = r2.as_array();
    let suggestions2 = arr2[0].as_array()[2].as_array();
    let words2: Vec<&str> = suggestions2
        .iter()
        .map(|s| s.as_array()[1].as_str())
        .collect();
    assert!(
        words2.contains(&"ergonomical"),
        "incremental vocab should include the new tag-only term"
    );
}

#[test]
fn test_hybrid_picks_correct_vector_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "title",
        "TEXT",
        "v1",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
        "v2",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "COSINE",
    ]);

    // d:1 is closest to [1,0,0,0] in v1 but to [0,0,0,1] in v2
    let v1_close = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let v2_close = pack_f32(&[0.0, 0.0, 0.0, 1.0]);
    c.cmd(&[
        b"HSET", b"p:1", b"title", b"alpha", b"v1", &v1_close, b"v2", &v2_close,
    ]);

    // d:2 is the opposite
    c.cmd(&[
        b"HSET", b"p:2", b"title", b"alpha", b"v1", &v2_close, b"v2", &v1_close,
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // Hybrid on v1 — d:1 should rank highest
    // Hybrid on v1, vector-only weighting — d:1 should rank first
    let r1 = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:alpha",
        b"VSIM",
        b"@v1",
        b"$q",
        b"COMBINE",
        b"LINEAR",
        b"4",
        b"ALPHA",
        b"0.0",
        b"BETA",
        b"1.0",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr1 = r1.as_array();
    assert_eq!(
        arr1[1].as_str(),
        "p:1",
        "v1-anchored hybrid (vector-only) should rank d:1 first"
    );

    // Hybrid on v2, vector-only weighting — d:2 should rank first
    let r2 = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:alpha",
        b"VSIM",
        b"@v2",
        b"$q",
        b"COMBINE",
        b"LINEAR",
        b"4",
        b"ALPHA",
        b"0.0",
        b"BETA",
        b"1.0",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr2 = r2.as_array();
    assert_eq!(
        arr2[1].as_str(),
        "p:2",
        "v2-anchored hybrid (vector-only) should rank d:2 first"
    );
}

#[test]
fn test_aggregate_geodistance_and_timefmt_combined() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "events",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "e:",
        "SCHEMA",
        "name",
        "TEXT",
        "lat",
        "NUMERIC",
        "lon",
        "NUMERIC",
        "ts",
        "NUMERIC",
    ]);

    // Three events at different cities and times
    c.cmds(&[
        "HSET",
        "e:1",
        "name",
        "sf",
        "lat",
        "37.7749",
        "lon",
        "-122.4194",
        "ts",
        "1700000000",
    ]);
    c.cmds(&[
        "HSET",
        "e:2",
        "name",
        "nyc",
        "lat",
        "40.7128",
        "lon",
        "-74.0060",
        "ts",
        "1700100000",
    ]);
    c.cmds(&[
        "HSET",
        "e:3",
        "name",
        "london",
        "lat",
        "51.5074",
        "lon",
        "-0.1278",
        "ts",
        "1700200000",
    ]);

    // Compute distance from each event to a reference (Tokyo)
    // and format the timestamp as YYYY-MM-DD
    let r = c.cmds(&[
        "FT.AGGREGATE",
        "events",
        "*",
        // A row shows what is loaded, computed or sorted by — `name` is
        // none of the last two.
        "LOAD",
        "1",
        "@name",
        "APPLY",
        "geodistance(@lat, @lon, 35.6762, 139.6503)",
        "AS",
        "to_tokyo_m",
        "APPLY",
        r#"timefmt(@ts, "%F")"#,
        "AS",
        "date",
        "FILTER",
        "@to_tokyo_m < 12000000", // closer than 12,000 km
        "SORTBY",
        "2",
        "@to_tokyo_m",
        "ASC",
    ]);
    let arr = r.as_array();
    let count = arr[0].as_int();
    assert!(count >= 1, "at least one event within 12,000 km of Tokyo");

    // First row should be the closest — that's SF (~ 8,250 km)
    let first = arr[1].as_array();
    let mut name = "";
    let mut date = "";
    let mut dist = 0.0;
    let mut i = 0;
    while i + 1 < first.len() {
        match first[i].as_str() {
            "name" => name = first[i + 1].as_str(),
            "date" => date = first[i + 1].as_str(),
            "to_tokyo_m" => dist = first[i + 1].as_str().parse().unwrap_or(0.0),
            _ => {}
        }
        i += 2;
    }
    assert_eq!(name, "sf");
    assert_eq!(date, "2023-11-14");
    assert!(
        dist > 8_000_000.0 && dist < 9_000_000.0,
        "SF→Tokyo should be roughly 8,250 km, got {dist} m"
    );
}

#[test]
fn test_hybrid_full_pipeline_with_geodistance() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "stores",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "s:",
        "SCHEMA",
        "name",
        "TEXT",
        "city",
        "TAG",
        "lat",
        "NUMERIC",
        "lon",
        "NUMERIC",
        "embedding",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "COSINE",
    ]);

    let v_a = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let v_b = pack_f32(&[0.7, 0.7, 0.0, 0.0]);
    let v_c = pack_f32(&[0.0, 1.0, 0.0, 0.0]);

    c.cmd(&[
        b"HSET",
        b"s:1",
        b"name",
        b"cafe alpha",
        b"city",
        b"sf",
        b"lat",
        b"37.77",
        b"lon",
        b"-122.41",
        b"embedding",
        &v_a,
    ]);
    c.cmd(&[
        b"HSET",
        b"s:2",
        b"name",
        b"cafe beta",
        b"city",
        b"sf",
        b"lat",
        b"37.78",
        b"lon",
        b"-122.42",
        b"embedding",
        &v_b,
    ]);
    c.cmd(&[
        b"HSET",
        b"s:3",
        b"name",
        b"cafe gamma",
        b"city",
        b"nyc",
        b"lat",
        b"40.71",
        b"lon",
        b"-74.00",
        b"embedding",
        &v_c,
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // Hybrid retrieval, then groupby city to get per-city stats
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"stores",
        b"SEARCH",
        b"@name:cafe",
        b"VSIM",
        b"@embedding",
        b"$q",
        b"COMBINE",
        b"LINEAR",
        b"4",
        b"ALPHA",
        b"0.5",
        b"BETA",
        b"0.5",
        b"LOAD",
        b"3",
        b"@city",
        b"@lat",
        b"@lon",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"GROUPBY",
        b"1",
        b"@city",
        b"REDUCE",
        b"COUNT",
        b"0",
        b"AS",
        b"cnt",
        b"FILTER",
        b"@cnt > 1",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1, "only sf has cnt > 1");
    let row = arr[1].as_array();
    let mut city = "";
    let mut cnt = 0i64;
    let mut i = 0;
    while i + 1 < row.len() {
        match row[i].as_str() {
            "city" => city = row[i + 1].as_str(),
            "cnt" => cnt = row[i + 1].as_str().parse().unwrap_or(0),
            _ => {}
        }
        i += 2;
    }
    assert_eq!(city, "sf");
    assert_eq!(cnt, 2);
}

#[test]
fn test_alter_then_hybrid_on_new_vector_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "title",
        "TEXT",
        "v1",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let v_match = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let v_other = pack_f32(&[0.0, 1.0, 0.0, 0.0]);
    c.cmd(&[
        b"HSET", b"p:1", b"title", b"alpha", b"v1", &v_match, b"v2", &v_other,
    ]); // v2 not in schema yet
    c.cmd(&[
        b"HSET", b"p:2", b"title", b"alpha", b"v1", &v_other, b"v2", &v_match,
    ]);

    // Add v2 to the schema; backfill should pick up existing values
    c.cmds(&[
        "FT.ALTER",
        "idx",
        "SCHEMA",
        "ADD",
        "v2",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ]);

    let q = pack_f32(&[1.0, 0.0, 0.0, 0.0]);

    // Hybrid using the just-added v2 field — p:2 should win (better VSIM on @v2).
    //
    // Use COMBINE LINEAR with ALPHA=0 BETA=1 so ordering follows vector rank only.
    // Default RRF gives both docs the same fused score when each is rank-1 in one
    // list and rank-2 in the other; `sort_unstable_by` then picks first arbitrarily.
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"idx",
        b"SEARCH",
        b"@title:alpha",
        b"VSIM",
        b"@v2",
        b"$q",
        b"COMBINE",
        b"LINEAR",
        b"4",
        b"ALPHA",
        b"0",
        b"BETA",
        b"1",
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    let arr = r.as_array();
    assert_eq!(
        arr[1].as_str(),
        "p:2",
        "hybrid on FT.ALTER-added field should work end-to-end"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// HEXPIRE / HPEXPIRE — Redis 7.4 hash field TTL commands
// ═════════════════════════════════════════════════════════════════════════════

// HEXPIRE on a missing key returns -2 for every field
#[test]
fn test_hexpire_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["HEXPIRE", "nosuch", "100", "FIELDS", "2", "a", "b"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_int(), -2);
    assert_eq!(arr[1].as_int(), -2);
}

// HEXPIRE returns -2 for fields that don't exist on a real hash
#[test]
fn test_hexpire_missing_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "1"]);
    let r = c.cmds(&["HEXPIRE", "h", "100", "FIELDS", "2", "a", "missing"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1); // a got the TTL
    assert_eq!(arr[1].as_int(), -2); // missing didn't
}

// Basic happy path: set TTL, check it actually evicts the field after expiry
#[test]
fn test_hexpire_basic_eviction() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x", "b", "y"]);
    let r = c.cmds(&["HPEXPIRE", "h", "100", "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_int(), 1);

    // a still alive briefly
    assert_eq!(c.cmds(&["HGET", "h", "a"]).as_str(), "x");

    std::thread::sleep(std::time::Duration::from_millis(220));

    // After expiry, HGET returns nil for a but b is intact
    assert!(c.cmds(&["HGET", "h", "a"]).is_nil());
    assert_eq!(c.cmds(&["HGET", "h", "b"]).as_str(), "y");
}

// HEXPIRE and HPEXPIRE take 0 as now: the field goes, with 2, as in Redis
// 8.10. A negative time is refused.
#[test]
fn test_hexpire_zero_is_now_and_negative_is_refused() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "1", "b", "2"]);
    assert_eq!(
        c.cmds(&["HEXPIRE", "h", "0", "FIELDS", "1", "a"])
            .as_array()[0]
            .as_int(),
        2
    );
    assert!(c.cmds(&["HEXISTS", "h", "a"]).as_int() == 0);
    assert_eq!(
        c.cmds(&["HPEXPIRE", "h", "0", "FIELDS", "1", "b"])
            .as_array()[0]
            .as_int(),
        2
    );
    c.cmds(&["HSET", "h", "c", "3"]);
    for args in [["HEXPIRE", "h", "-5"], ["HPEXPIRE", "h", "-5"]] {
        let mut cmd = args.to_vec();
        cmd.extend_from_slice(&["FIELDS", "1", "c"]);
        assert!(c.cmds(&cmd).is_error(), "{args:?}");
    }
    assert!(c.cmds(&["HEXISTS", "h", "c"]).as_int() == 1);
}

// HEXPIRE NX: only sets if no TTL is currently set
#[test]
fn test_hexpire_nx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);

    // First NX → sets it
    let r1 = c.cmds(&["HEXPIRE", "h", "100", "NX", "FIELDS", "1", "a"]);
    assert_eq!(r1.as_array()[0].as_int(), 1);

    // Second NX → blocked
    let r2 = c.cmds(&["HEXPIRE", "h", "200", "NX", "FIELDS", "1", "a"]);
    assert_eq!(r2.as_array()[0].as_int(), 0);
}

// HEXPIRE XX: only sets if a TTL is currently set
#[test]
fn test_hexpire_xx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);

    // XX with no current TTL → blocked
    let r1 = c.cmds(&["HEXPIRE", "h", "100", "XX", "FIELDS", "1", "a"]);
    assert_eq!(r1.as_array()[0].as_int(), 0);

    // Set one, then XX → sets
    c.cmds(&["HEXPIRE", "h", "50", "FIELDS", "1", "a"]);
    let r2 = c.cmds(&["HEXPIRE", "h", "200", "XX", "FIELDS", "1", "a"]);
    assert_eq!(r2.as_array()[0].as_int(), 1);
}

// HEXPIRE GT: only sets if new TTL is greater than current
#[test]
fn test_hexpire_gt() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);
    c.cmds(&["HEXPIRE", "h", "100", "FIELDS", "1", "a"]);

    // GT with smaller → blocked
    let r1 = c.cmds(&["HEXPIRE", "h", "10", "GT", "FIELDS", "1", "a"]);
    assert_eq!(r1.as_array()[0].as_int(), 0);

    // GT with larger → sets
    let r2 = c.cmds(&["HEXPIRE", "h", "300", "GT", "FIELDS", "1", "a"]);
    assert_eq!(r2.as_array()[0].as_int(), 1);
}

// HEXPIRE LT: only sets if new TTL is less than current
#[test]
fn test_hexpire_lt() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);
    c.cmds(&["HEXPIRE", "h", "100", "FIELDS", "1", "a"]);

    let r1 = c.cmds(&["HEXPIRE", "h", "999", "LT", "FIELDS", "1", "a"]);
    assert_eq!(r1.as_array()[0].as_int(), 0);

    let r2 = c.cmds(&["HEXPIRE", "h", "50", "LT", "FIELDS", "1", "a"]);
    assert_eq!(r2.as_array()[0].as_int(), 1);
}

// HEXPIRE on a non-hash key returns WRONGTYPE
#[test]
fn test_hexpire_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "s", "string"]);
    assert!(
        c.cmds(&["HEXPIRE", "s", "100", "FIELDS", "1", "a"])
            .is_error()
    );
}

// FIELDS keyword and numfields validation
#[test]
fn test_hexpire_arity_and_syntax() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);

    // Missing FIELDS keyword
    assert!(c.cmds(&["HEXPIRE", "h", "100", "1", "a"]).is_error());
    // numfields=0 is invalid
    assert!(c.cmds(&["HEXPIRE", "h", "100", "FIELDS", "0"]).is_error());
    // numfields doesn't match argument count
    assert!(
        c.cmds(&["HEXPIRE", "h", "100", "FIELDS", "3", "a"])
            .is_error()
    );
}

// Multiple fields in one call — mix of existing and missing
#[test]
fn test_hexpire_multiple_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    let r = c.cmds(&[
        "HEXPIRE", "h", "100", "FIELDS", "4", "a", "b", "c", "missing",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_int(), 1);
    assert_eq!(arr[2].as_int(), 1);
    assert_eq!(arr[3].as_int(), -2);
}

// Calling HEXPIRE upgrades a plain Hash to HashEx; subsequent HSET on the
// same key continues to work normally.
#[test]
fn test_hexpire_upgrades_then_hset_still_works() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "1"]);
    c.cmds(&["HEXPIRE", "h", "300", "FIELDS", "1", "a"]);

    // HSET on the now-HashEx key should still add new fields
    assert_eq!(c.cmds(&["HSET", "h", "b", "2"]).as_int(), 1);
    assert_eq!(c.cmds(&["HGET", "h", "b"]).as_str(), "2");
    assert_eq!(c.cmds(&["HLEN", "h"]).as_int(), 2);
}

// ═════════════════════════════════════════════════════════════════════════════
// HEXPIREAT / HPEXPIREAT
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hexpireat_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 300;
    let r = c.cmds(&["HEXPIREAT", "h", &future.to_string(), "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_int(), 1);

    // Field is still there
    assert_eq!(c.cmds(&["HGET", "h", "a"]).as_str(), "x");
}

#[test]
fn test_hexpireat_past_timestamp_deletes_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x", "b", "y"]);
    let past = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 10;
    let r = c.cmds(&["HEXPIREAT", "h", &past.to_string(), "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_int(), 2); // deleted due to past time
    assert!(c.cmds(&["HGET", "h", "a"]).is_nil());
    assert_eq!(c.cmds(&["HGET", "h", "b"]).as_str(), "y");
}

#[test]
fn test_hpexpireat_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);
    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 300_000;
    let r = c.cmds(&[
        "HPEXPIREAT",
        "h",
        &future_ms.to_string(),
        "FIELDS",
        "1",
        "a",
    ]);
    assert_eq!(r.as_array()[0].as_int(), 1);
}

#[test]
fn test_hpexpireat_past_deletes() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);
    let past_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 5_000;
    let r = c.cmds(&["HPEXPIREAT", "h", &past_ms.to_string(), "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_int(), 2);
    assert!(c.cmds(&["HGET", "h", "a"]).is_nil());
}

#[test]
fn test_hexpireat_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 100;
    let r = c.cmds(&[
        "HEXPIREAT",
        "nosuch",
        &future.to_string(),
        "FIELDS",
        "2",
        "a",
        "b",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), -2);
    assert_eq!(arr[1].as_int(), -2);
}

#[test]
fn test_hexpireat_conditions() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "x"]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // NX with no TTL → sets
    let r1 = c.cmds(&[
        "HEXPIREAT",
        "h",
        &(now + 100).to_string(),
        "NX",
        "FIELDS",
        "1",
        "a",
    ]);
    assert_eq!(r1.as_array()[0].as_int(), 1);

    // NX with existing TTL → blocked
    let r2 = c.cmds(&[
        "HEXPIREAT",
        "h",
        &(now + 999).to_string(),
        "NX",
        "FIELDS",
        "1",
        "a",
    ]);
    assert_eq!(r2.as_array()[0].as_int(), 0);

    // GT with later → sets
    let r3 = c.cmds(&[
        "HEXPIREAT",
        "h",
        &(now + 500).to_string(),
        "GT",
        "FIELDS",
        "1",
        "a",
    ]);
    assert_eq!(r3.as_array()[0].as_int(), 1);

    // LT with earlier → sets
    let r4 = c.cmds(&[
        "HEXPIREAT",
        "h",
        &(now + 50).to_string(),
        "LT",
        "FIELDS",
        "1",
        "a",
    ]);
    assert_eq!(r4.as_array()[0].as_int(), 1);
}

#[test]
fn test_hexpireat_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "s", "string"]);
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 100;
    assert!(
        c.cmds(&["HEXPIREAT", "s", &future.to_string(), "FIELDS", "1", "a"])
            .is_error()
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_httl_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let r = c.cmds(&["HTTL", "nosuch", "FIELDS", "2", "a", "b"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), -2);
    assert_eq!(arr[1].as_int(), -2);
}

#[test]
fn test_httl_no_ttl_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x", "b", "y"]);
    let r = c.cmds(&["HTTL", "h", "FIELDS", "3", "a", "b", "missing"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), -1);
    assert_eq!(arr[1].as_int(), -1);
    assert_eq!(arr[2].as_int(), -2);
}

#[test]
fn test_httl_after_hexpire_returns_remaining_secs() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    c.cmds(&["HEXPIRE", "h", "300", "FIELDS", "1", "a"]);
    let r = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]);
    let v = r.as_array()[0].as_int();
    assert!(v > 290 && v <= 300, "expected ~300, got {v}");
}

/// A field's time is told in full when it is asked for at once: ten seconds
/// as ten, where cutting what had gone by since said nine, and the time of
/// day it was given as that same time.
#[test]
fn a_fields_time_is_told_in_full_when_asked_at_once() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x", "b", "y", "c", "z"]);
    for _ in 0..20 {
        c.cmds(&["HEXPIRE", "h", "10", "FIELDS", "1", "a"]);
        let told = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]).as_array()[0].as_int();
        assert_eq!(told, 10, "HTTL");
        c.cmds(&["HPEXPIRE", "h", "5000", "FIELDS", "1", "b"]);
        let told = c.cmds(&["HPTTL", "h", "FIELDS", "1", "b"]).as_array()[0].as_int();
        assert!((4990..=5000).contains(&told), "HPTTL {told}");
        let at = "4102444800123";
        c.cmds(&["HPEXPIREAT", "h", at, "FIELDS", "1", "c"]);
        let told = c
            .cmds(&["HPEXPIRETIME", "h", "FIELDS", "1", "c"])
            .as_array()[0]
            .as_int();
        assert_eq!(told.to_string(), at, "HPEXPIRETIME");
        let told = c.cmds(&["HEXPIRETIME", "h", "FIELDS", "1", "c"]).as_array()[0].as_int();
        assert_eq!(told, 4102444800, "HEXPIRETIME");
    }
}

#[test]
fn test_hpttl_after_hpexpire_returns_remaining_ms() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    c.cmds(&["HPEXPIRE", "h", "300000", "FIELDS", "1", "a"]);
    let r = c.cmds(&["HPTTL", "h", "FIELDS", "1", "a"]);
    let v = r.as_array()[0].as_int();
    assert!(v > 295_000 && v <= 300_000, "expected ~300000, got {v}");
}

#[test]
fn test_hexpiretime_roundtrip() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let target = now + 600;
    c.cmds(&["HEXPIREAT", "h", &target.to_string(), "FIELDS", "1", "a"]);

    let r = c.cmds(&["HEXPIRETIME", "h", "FIELDS", "1", "a"]);
    let got = r.as_array()[0].as_int();
    assert!(
        (got - target).abs() <= 1,
        "HEXPIRETIME drift: target={target} got={got}"
    );
}

#[test]
fn test_hpexpiretime_roundtrip() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let target_ms = now_ms + 600_000;
    c.cmds(&[
        "HPEXPIREAT",
        "h",
        &target_ms.to_string(),
        "FIELDS",
        "1",
        "a",
    ]);

    let r = c.cmds(&["HPEXPIRETIME", "h", "FIELDS", "1", "a"]);
    let got = r.as_array()[0].as_int();
    assert!(
        (got - target_ms).abs() <= 50,
        "HPEXPIRETIME drift: target={target_ms} got={got}"
    );
}

#[test]
fn test_httl_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "s", "string"]);
    assert!(c.cmds(&["HTTL", "s", "FIELDS", "1", "a"]).is_error());
}

#[test]
fn test_httl_arity() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    assert!(c.cmds(&["HTTL", "h"]).is_error());
    assert!(c.cmds(&["HTTL", "h", "FIELDS"]).is_error());
    assert!(c.cmds(&["HTTL", "h", "FIELDS", "0"]).is_error());
    assert!(c.cmds(&["HTTL", "h", "FIELDS", "2", "a"]).is_error()); // count mismatch
}

#[test]
fn test_httl_after_field_expires_returns_minus_two() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    c.cmds(&["HPEXPIRE", "h", "50", "FIELDS", "1", "a"]);
    std::thread::sleep(std::time::Duration::from_millis(120));
    // After expiry, the field is gone — HTTL should report -2
    let r = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_int(), -2);
}

// ═════════════════════════════════════════════════════════════════════════════
// HPERSIST
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hpersist_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let r = c.cmds(&["HPERSIST", "nosuch", "FIELDS", "2", "a", "b"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), -2);
    assert_eq!(arr[1].as_int(), -2);
}

#[test]
fn test_hpersist_plain_hash_no_ttls() {
    // A hash that's never had HEXPIRE called → all fields return -1 if
    // present, -2 if missing.
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x", "b", "y"]);
    let r = c.cmds(&["HPERSIST", "h", "FIELDS", "3", "a", "b", "missing"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), -1);
    assert_eq!(arr[1].as_int(), -1);
    assert_eq!(arr[2].as_int(), -2);
}

#[test]
fn test_hpersist_removes_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x", "b", "y"]);
    c.cmds(&["HEXPIRE", "h", "300", "FIELDS", "1", "a"]);

    // a has TTL, b does not, missing doesn't exist
    let r = c.cmds(&["HPERSIST", "h", "FIELDS", "3", "a", "b", "missing"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1); // removed
    assert_eq!(arr[1].as_int(), -1); // had no TTL
    assert_eq!(arr[2].as_int(), -2); // missing

    // After HPERSIST, HTTL on a should report -1 (no TTL)
    let ttl = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]);
    assert_eq!(ttl.as_array()[0].as_int(), -1);
}

#[test]
fn test_hpersist_idempotent() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    c.cmds(&["HEXPIRE", "h", "300", "FIELDS", "1", "a"]);

    let r1 = c.cmds(&["HPERSIST", "h", "FIELDS", "1", "a"]);
    assert_eq!(r1.as_array()[0].as_int(), 1);

    // Second call: TTL is already gone → -1
    let r2 = c.cmds(&["HPERSIST", "h", "FIELDS", "1", "a"]);
    assert_eq!(r2.as_array()[0].as_int(), -1);
}

#[test]
fn test_hpersist_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "s", "string"]);
    assert!(c.cmds(&["HPERSIST", "s", "FIELDS", "1", "a"]).is_error());
}

#[test]
fn test_hpersist_arity() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    assert!(c.cmds(&["HPERSIST", "h"]).is_error());
    assert!(c.cmds(&["HPERSIST", "h", "FIELDS"]).is_error());
    assert!(c.cmds(&["HPERSIST", "h", "FIELDS", "0"]).is_error());
    assert!(c.cmds(&["HPERSIST", "h", "FIELDS", "2", "a"]).is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// HGETDEL
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hgetdel_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    let r = c.cmds(&["HGETDEL", "h", "FIELDS", "3", "a", "b", "missing"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "1");
    assert_eq!(arr[1].as_str(), "2");
    assert!(arr[2].is_nil());

    // a and b are gone, c remains
    assert!(c.cmds(&["HGET", "h", "a"]).is_nil());
    assert!(c.cmds(&["HGET", "h", "b"]).is_nil());
    assert_eq!(c.cmds(&["HGET", "h", "c"]).as_str(), "3");
}

#[test]
fn test_hgetdel_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let r = c.cmds(&["HGETDEL", "nosuch", "FIELDS", "2", "a", "b"]);
    let arr = r.as_array();
    assert!(arr[0].is_nil());
    assert!(arr[1].is_nil());
}

#[test]
fn test_hgetdel_clears_field_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "x"]);
    c.cmds(&["HEXPIRE", "h", "300", "FIELDS", "1", "a"]);

    let r = c.cmds(&["HGETDEL", "h", "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_str(), "x");

    // HTTL should report -2 (no such field) since it was deleted
    let ttl = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]);
    assert_eq!(ttl.as_array()[0].as_int(), -2);
}

#[test]
fn test_hgetdel_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "s", "string"]);
    assert!(c.cmds(&["HGETDEL", "s", "FIELDS", "1", "a"]).is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// HGETEX
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hgetex_no_action_just_reads() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "1", "b", "2"]);
    let r = c.cmds(&["HGETEX", "h", "FIELDS", "2", "a", "missing"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "1");
    assert!(arr[1].is_nil());
}

#[test]
fn test_hgetex_ex_sets_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "1"]);
    let r = c.cmds(&["HGETEX", "h", "EX", "300", "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_str(), "1");

    let ttl = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]);
    let v = ttl.as_array()[0].as_int();
    assert!(v > 290 && v <= 300, "expected ~300, got {v}");
}

#[test]
fn test_hgetex_persist_clears_ttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "1"]);
    c.cmds(&["HEXPIRE", "h", "300", "FIELDS", "1", "a"]);

    let r = c.cmds(&["HGETEX", "h", "PERSIST", "FIELDS", "1", "a"]);
    assert_eq!(r.as_array()[0].as_str(), "1");

    let ttl = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]);
    assert_eq!(ttl.as_array()[0].as_int(), -1);
}

#[test]
fn test_hgetex_exat_past_deletes_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "1"]);

    let past = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 10;
    let r = c.cmds(&["HGETEX", "h", "EXAT", &past.to_string(), "FIELDS", "1", "a"]);
    // We still get the value back, even though the field is gone afterwards
    assert_eq!(r.as_array()[0].as_str(), "1");
    assert!(c.cmds(&["HGET", "h", "a"]).is_nil());
}

#[test]
fn test_hgetex_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let r = c.cmds(&["HGETEX", "nosuch", "EX", "100", "FIELDS", "1", "a"]);
    assert!(r.as_array()[0].is_nil());
}

// ═════════════════════════════════════════════════════════════════════════════
// HSETEX
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hsetex_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["HSETEX", "h", "300", "FIELDS", "2", "a", "1", "b", "2"]);
    assert_eq!(r.as_int(), 1);

    assert_eq!(c.cmds(&["HGET", "h", "a"]).as_str(), "1");
    assert_eq!(c.cmds(&["HGET", "h", "b"]).as_str(), "2");

    let ttl = c.cmds(&["HTTL", "h", "FIELDS", "2", "a", "b"]);
    let arr = ttl.as_array();
    assert!(arr[0].as_int() > 290);
    assert!(arr[1].as_int() > 290);
}

#[test]
fn test_hsetex_fnx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "old"]);

    // FNX with one existing field → blocked
    let r = c.cmds(&[
        "HSETEX", "h", "300", "FNX", "FIELDS", "2", "a", "new", "b", "new",
    ]);
    assert_eq!(r.as_int(), 0);
    assert_eq!(c.cmds(&["HGET", "h", "a"]).as_str(), "old");
    assert!(c.cmds(&["HGET", "h", "b"]).is_nil());

    // FNX with no existing fields → succeeds
    let r2 = c.cmds(&["HSETEX", "h", "300", "FNX", "FIELDS", "1", "fresh", "v"]);
    assert_eq!(r2.as_int(), 1);
}

#[test]
fn test_hsetex_fxx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "1"]);

    // FXX: a exists but b doesn't → blocked
    let r = c.cmds(&[
        "HSETEX", "h", "300", "FXX", "FIELDS", "2", "a", "x", "b", "y",
    ]);
    assert_eq!(r.as_int(), 0);

    // FXX with all fields existing → succeeds
    c.cmds(&["HSET", "h", "b", "2"]);
    let r2 = c.cmds(&[
        "HSETEX", "h", "300", "FXX", "FIELDS", "2", "a", "x", "b", "y",
    ]);
    assert_eq!(r2.as_int(), 1);
    assert_eq!(c.cmds(&["HGET", "h", "a"]).as_str(), "x");
}

#[test]
fn test_hsetex_keepttl() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "1"]);
    c.cmds(&["HEXPIRE", "h", "1000", "FIELDS", "1", "a"]);

    // KEEPTTL: don't overwrite a's existing 1000s TTL with 100s
    let r = c.cmds(&[
        "HSETEX", "h", "100", "KEEPTTL", "FIELDS", "1", "a", "newval",
    ]);
    assert_eq!(r.as_int(), 1);

    let ttl = c.cmds(&["HTTL", "h", "FIELDS", "1", "a"]);
    let v = ttl.as_array()[0].as_int();
    assert!(v > 900, "KEEPTTL should preserve original ~1000, got {v}");
}

#[test]
fn test_hsetex_invalid_seconds() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(
        c.cmds(&["HSETEX", "h", "0", "FIELDS", "1", "a", "v"])
            .is_error()
    );
    assert!(
        c.cmds(&["HSETEX", "h", "-1", "FIELDS", "1", "a", "v"])
            .is_error()
    );
}

#[test]
fn test_hsetex_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "s", "string"]);
    assert!(
        c.cmds(&["HSETEX", "s", "300", "FIELDS", "1", "a", "v"])
            .is_error()
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// HSTRLEN
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_hstrlen_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "hello", "b", ""]);
    assert_eq!(c.cmds(&["HSTRLEN", "h", "a"]).as_int(), 5);
    assert_eq!(c.cmds(&["HSTRLEN", "h", "b"]).as_int(), 0);
    assert_eq!(c.cmds(&["HSTRLEN", "h", "missing"]).as_int(), 0);
    assert_eq!(c.cmds(&["HSTRLEN", "nosuch", "a"]).as_int(), 0);
}

#[test]
fn test_hstrlen_after_hexpire() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["HSET", "h", "a", "abcdef"]);
    c.cmds(&["HEXPIRE", "h", "300", "FIELDS", "1", "a"]);
    // HSTRLEN should still see the field (TTL hasn't elapsed)
    assert_eq!(c.cmds(&["HSTRLEN", "h", "a"]).as_int(), 6);
}

#[test]
fn test_hstrlen_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "s", "string"]);
    assert!(c.cmds(&["HSTRLEN", "s", "f"]).is_error());
}

/// A hash gives its fields back in the order they were first set, as a Redis
/// hash small enough for a listpack does: clients rely on it (BullMQ's
/// paginate tests). Setting a field again does not move it; deleting one
/// leaves the others in order; one set afterwards goes to the end.
#[test]
fn test_hash_fields_keep_insertion_order() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let names = ["j", "a", "h", "b", "i", "c", "g", "d", "f", "e"];
    for name in names {
        c.cmds(&["HSET", "ordered", name, &format!("v-{name}")]);
    }
    let strs = |v: &RespValue| -> Vec<String> {
        v.as_array()
            .iter()
            .map(|x| x.as_str().to_string())
            .collect()
    };
    assert_eq!(strs(&c.cmds(&["HKEYS", "ordered"])), names);
    let values: Vec<String> = names.iter().map(|n| format!("v-{n}")).collect();
    assert_eq!(strs(&c.cmds(&["HVALS", "ordered"])), values);
    let all = strs(&c.cmds(&["HGETALL", "ordered"]));
    let fields: Vec<&str> = all.iter().step_by(2).map(String::as_str).collect();
    assert_eq!(fields, names);
    let scan = c.cmds(&["HSCAN", "ordered", "0"]);
    let scanned = strs(&scan.as_array()[1]);
    let fields: Vec<&str> = scanned.iter().step_by(2).map(String::as_str).collect();
    assert_eq!(fields, names);

    c.cmds(&["HSET", "ordered", "h", "again"]);
    c.cmds(&["HDEL", "ordered", "b", "g"]);
    c.cmds(&["HSET", "ordered", "z", "last"]);
    assert_eq!(
        strs(&c.cmds(&["HKEYS", "ordered"])),
        ["j", "a", "h", "i", "c", "d", "f", "e", "z"]
    );

    // The order survives a save and a reload of the key.
    let dumped = c.cmd(&[b"DUMP", b"ordered"]);
    let RespValue::Bulk(payload) = dumped else {
        panic!("DUMP: {dumped:?}");
    };
    c.cmd(&[b"RESTORE", b"restored", b"0", &payload]);
    assert_eq!(
        strs(&c.cmds(&["HKEYS", "restored"])),
        ["j", "a", "h", "i", "c", "d", "f", "e", "z"]
    );
}

// ── Vector element types ──────────────────────────────────────────────────────

/// `values` as little-endian elements of `ty`. The test values are exactly
/// representable in every type, so half precision needs no rounding here.
fn pack_as(ty: &str, values: &[f32]) -> Vec<u8> {
    let half = |v: f32| -> u16 {
        if v == 0.0 {
            return 0;
        }
        let bits = v.to_bits();
        let exponent = ((bits >> 23) & 0xff) as i32 - 127 + 15;
        (((bits >> 16) & 0x8000) | ((exponent as u32) << 10) | ((bits >> 13) & 0x3ff)) as u16
    };
    values
        .iter()
        .flat_map(|v| match ty {
            "FLOAT32" => v.to_le_bytes().to_vec(),
            "FLOAT64" => f64::from(*v).to_le_bytes().to_vec(),
            "FLOAT16" => half(*v).to_le_bytes().to_vec(),
            "BFLOAT16" => ((v.to_bits() >> 16) as u16).to_le_bytes().to_vec(),
            other => panic!("no packing for {other}"),
        })
        .collect()
}

/// FLOAT64, FLOAT16 and BFLOAT16 vector fields, as Redis has them: the blobs
/// in the hash, and the query blob, are elements of the field's type. The
/// distances are what Redis 8.10.2 returns for the same data.
#[test]
fn test_vector_types_other_than_float32_on_hash() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for ty in ["FLOAT64", "FLOAT16", "BFLOAT16"] {
        for algo in ["FLAT", "HNSW"] {
            let index = format!("idx_{ty}_{algo}");
            let prefix = format!("{ty}_{algo}:");
            let created = c.cmds(&[
                "FT.CREATE",
                &index,
                "ON",
                "HASH",
                "PREFIX",
                "1",
                &prefix,
                "SCHEMA",
                "v",
                "VECTOR",
                algo,
                "6",
                "TYPE",
                ty,
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ]);
            assert!(created.is_ok(), "{index}: {created:?}");

            let key = |k: &str| format!("{prefix}{k}");
            c.cmd(&[
                b"HSET",
                key("a").as_bytes(),
                b"v",
                &pack_as(ty, &[0.0, 0.0]),
            ]);
            c.cmd(&[
                b"HSET",
                key("b").as_bytes(),
                b"v",
                &pack_as(ty, &[3.0, 4.0]),
            ]);
            // A FLOAT32 blob is the wrong size for every one of these types.
            c.cmd(&[
                b"HSET",
                key("bad").as_bytes(),
                b"v",
                &pack_f32(&[1.0, 1.0, 1.0]),
            ]);

            let r = c.cmd(&[
                b"FT.SEARCH",
                index.as_bytes(),
                b"*=>[KNN 3 @v $q AS d]",
                b"PARAMS",
                b"2",
                b"q",
                &pack_as(ty, &[3.0, 3.5]),
                b"SORTBY",
                b"d",
                b"RETURN",
                b"1",
                b"d",
                b"DIALECT",
                b"2",
            ]);
            let arr = r.as_array();
            assert_eq!(arr.first().map(|v| v.as_int()), Some(2), "{index}: {r:?}");
            assert_eq!(arr[1].as_str(), key("b"), "{index}");
            assert_eq!(parse_score(&arr[2].as_array()[1]), 0.25, "{index}");
            assert_eq!(arr[3].as_str(), key("a"), "{index}");
            assert_eq!(parse_score(&arr[4].as_array()[1]), 21.25, "{index}");

            let r = c.cmd(&[
                b"FT.SEARCH",
                index.as_bytes(),
                b"@v:[VECTOR_RANGE 1 $q]",
                b"PARAMS",
                b"2",
                b"q",
                &pack_as(ty, &[3.0, 4.0]),
                b"NOCONTENT",
                b"DIALECT",
                b"2",
            ]);
            assert_eq!(r.as_array().len(), 2, "{index}: {r:?}");
            assert_eq!(r.as_array()[1].as_str(), key("b"), "{index}");

            // A query blob of the wrong size is refused, with its size.
            let r = c.cmd(&[
                b"FT.SEARCH",
                index.as_bytes(),
                b"*=>[KNN 1 @v $q AS d]",
                b"PARAMS",
                b"2",
                b"q",
                &pack_f32(&[3.0, 4.0, 5.0]),
                b"DIALECT",
                b"2",
            ]);
            let want = if ty == "FLOAT64" { 16 } else { 4 };
            assert!(
                r.is_error()
                    && format!("{r:?}").contains(&format!(
                        "query vector blob size (12) does not match index's expected size ({want})"
                    )),
                "{index}: {r:?}"
            );

            let info = c.cmds(&["FT.INFO", &index]);
            let info = format!("{info:?}");
            assert!(
                info.contains(&format!("{:?}", ty.as_bytes())),
                "{index}: FT.INFO must report data_type {ty}: {info}"
            );
        }
    }
}

/// A FLOAT64 vector field of a JSON index: the document holds a number array
/// and the query blob is FLOAT64.
#[test]
fn test_vector_type_float64_on_json() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let created = c.cmds(&[
        "FT.CREATE",
        "j",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "j:",
        "SCHEMA",
        "$.v",
        "AS",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT64",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert!(created.is_ok(), "{created:?}");
    c.cmds(&["JSON.SET", "j:a", "$", r#"{"v":[0,0]}"#]);
    c.cmds(&["JSON.SET", "j:b", "$", r#"{"v":[3,4]}"#]);

    let r = c.cmd(&[
        b"FT.SEARCH",
        b"j",
        b"*=>[KNN 1 @v $q AS d]",
        b"PARAMS",
        b"2",
        b"q",
        &pack_as("FLOAT64", &[3.0, 3.5]),
        b"RETURN",
        b"1",
        b"d",
        b"DIALECT",
        b"2",
    ]);
    let arr = r.as_array();
    assert_eq!(arr.first().map(|v| v.as_int()), Some(1), "{r:?}");
    assert_eq!(arr[1].as_str(), "j:b");
    assert_eq!(parse_score(&arr[2].as_array()[1]), 0.25);
}

/// A type Redis does not know is refused at FT.CREATE, with the reply Redis
/// 8.2.10 gives (Redis 8.10 puts `SEARCH_PARSE_ARGS` in front of it).
#[test]
fn test_vector_type_unknown_is_refused() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for algo in ["FLAT", "HNSW"] {
        let index = format!("x_{algo}");
        let r = c.cmds(&[
            "FT.CREATE",
            &index,
            "SCHEMA",
            "v",
            "VECTOR",
            algo,
            "6",
            "TYPE",
            "FLOAT8",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
        ]);
        let want =
            format!("Bad arguments for vector similarity {algo} index `TYPE`: Unknown argument");
        assert!(
            matches!(&r, RespValue::Error(e) if *e == want),
            "{algo}: {r:?}"
        );
    }
}

// ── #162: BM25 statistics, FT.DROPINDEX DD, KNN in FT.AGGREGATE ───────────────

fn score_of(reply: &RespValue) -> String {
    reply.as_array()[2].as_str().to_string()
}

/// What a document scores for a word does not change when other documents
/// come and go: the BM25 statistics count what is there. They only grew,
/// and a score fell a hundredfold after 200 documents were added and
/// deleted (#162).
#[test]
fn test_bm25_statistics_follow_updates_and_deletes() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(
        !c.cmds(&[
            "FT.CREATE",
            "docs",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "body",
            "TEXT"
        ])
        .is_error()
    );
    c.cmds(&["HSET", "doc:keep", "body", "rare word here"]);
    c.cmds(&["HSET", "doc:other", "body", "common filler text"]);
    let before = score_of(&c.cmds(&["FT.SEARCH", "docs", "rare", "WITHSCORES", "NOCONTENT"]));
    for i in 0..200 {
        c.cmds(&[
            "HSET",
            &format!("doc:{i}"),
            "body",
            "rare common filler text",
        ]);
    }
    for i in 0..200 {
        c.cmds(&["DEL", &format!("doc:{i}")]);
    }
    // Written again, with the same words: counted once.
    for _ in 0..50 {
        c.cmds(&["HSET", "doc:other", "body", "common filler text"]);
    }
    let after = score_of(&c.cmds(&["FT.SEARCH", "docs", "rare", "WITHSCORES", "NOCONTENT"]));
    assert_eq!(before, after);

    // A field written again no longer matches what it held.
    c.cmds(&["HSET", "doc:keep", "body", "another thing"]);
    let found = c.cmds(&["FT.SEARCH", "docs", "rare", "NOCONTENT"]);
    assert_eq!(found.as_array()[0].as_int(), 0, "{found:?}");
    let found = c.cmds(&["FT.SEARCH", "docs", "another", "NOCONTENT"]);
    assert_eq!(found.as_array()[0].as_int(), 1, "{found:?}");
}

/// `FT.DROPINDEX index DD` deletes the documents the index covered; they
/// were kept (#162).
#[test]
fn test_dropindex_dd_deletes_the_documents() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(
        !c.cmds(&[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "item:",
            "SCHEMA",
            "name",
            "TEXT"
        ])
        .is_error()
    );
    c.cmds(&["HSET", "item:1", "name", "a"]);
    c.cmds(&["HSET", "item:2", "name", "b"]);
    c.cmds(&["SET", "item:3", "a string, not a document"]);
    c.cmds(&["HSET", "other:1", "name", "c"]);
    assert!(c.cmds(&["FT.DROPINDEX", "idx", "BOGUS"]).is_error());
    assert!(matches!(
        c.cmds(&["FT.DROPINDEX", "idx", "DD"]),
        RespValue::SimpleStr(_)
    ));
    assert_eq!(c.cmds(&["EXISTS", "item:1", "item:2"]).as_int(), 0);
    assert_eq!(c.cmds(&["EXISTS", "item:3", "other:1"]).as_int(), 2);
    assert!(c.cmds(&["FT.INFO", "idx"]).is_error());
}

/// A KNN clause in `FT.AGGREGATE` keeps the nearest `k` of what the filter
/// left, with the distance to group, sort or load by; it was dropped, and
/// every document went on (#162).
#[test]
fn test_aggregate_with_knn_keeps_the_nearest() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let r = c.cmds(&[
        "FT.CREATE",
        "p",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "name",
        "TEXT",
        "cat",
        "TAG",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert!(!r.is_error(), "{r:?}");
    for (key, name, cat, v) in [
        ("p:1", "near", "a", [1.0f32, 0.0]),
        ("p:2", "far", "a", [9.0, 9.0]),
        ("p:3", "nearest", "b", [0.5, 0.0]),
    ] {
        let bytes = pack_f32(&v);
        c.cmd(&[
            b"HSET",
            key.as_bytes(),
            b"name",
            name.as_bytes(),
            b"cat",
            cat.as_bytes(),
            b"v",
            &bytes,
        ]);
    }
    let q = pack_f32(&[0.0, 0.0]);
    let r = c.cmd(&[
        b"FT.AGGREGATE",
        b"p",
        b"*=>[KNN 1 @v $q AS d]",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"LOAD",
        b"2",
        b"@name",
        b"@d",
        b"DIALECT",
        b"2",
    ]);
    let rows = r.as_array();
    assert_eq!(rows.len(), 2, "{r:?}");
    let row = rows[1].as_array();
    let field = |name: &str| {
        row.chunks(2)
            .find(|p| p[0].as_str() == name)
            .map(|p| p[1].as_str().to_string())
    };
    assert_eq!(field("name").as_deref(), Some("nearest"), "{r:?}");
    assert!(field("d").is_some(), "{r:?}");

    // With a filter: the nearest of those it left.
    let r = c.cmd(&[
        b"FT.AGGREGATE",
        b"p",
        b"@cat:{a}=>[KNN 1 @v $q AS d]",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"LOAD",
        b"1",
        b"@name",
        b"DIALECT",
        b"2",
    ]);
    let rows = r.as_array();
    assert_eq!(rows.len(), 2, "{r:?}");
    assert_eq!(rows[1].as_array()[1].as_str(), "near", "{r:?}");
}
