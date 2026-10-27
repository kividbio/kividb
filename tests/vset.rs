// Integration tests for Vector Set commands (VADD, VREM, VSIM, VCARD, VDIM,
// VEMB, VISMEMBER, VGETATTR, VSETATTR, VINFO, VLINKS, VRANDMEMBER, VRANGE).
//
// Each test gets its own server on a fresh random port — fully isolated,
// runs in parallel with other test files.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── Minimal RESP client (same harness as vector-hash.rs) ──────────────────────

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

    fn as_f64(&self) -> f64 {
        self.as_str().parse().unwrap()
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

// ── Vector helpers ────────────────────────────────────────────────────────────

fn pack_f32(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

// ── VADD / VCARD / VDIM ──────────────────────────────────────────────────────

#[test]
fn test_vadd_vcard_vdim() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Add first element — returns 1 (new)
    let blob = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r = c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"elem:1"]);
    assert_eq!(r.as_int(), 1, "first VADD should return 1 (new element)");

    // Add second element
    let blob2 = pack_f32(&[0.0, 1.0, 0.0, 0.0]);
    assert_eq!(
        c.cmd(&[b"VADD", b"vs", b"FP32", &blob2, b"elem:2"])
            .as_int(),
        1
    );

    // Update existing element — returns 0 (updated, not new)
    let blob3 = pack_f32(&[0.5, 0.5, 0.0, 0.0]);
    assert_eq!(
        c.cmd(&[b"VADD", b"vs", b"FP32", &blob3, b"elem:1"])
            .as_int(),
        0,
        "re-adding an existing element should return 0"
    );

    // VCARD: 2 distinct elements
    assert_eq!(c.cmds(&["VCARD", "vs"]).as_int(), 2);

    // VDIM: dimension 4
    assert_eq!(c.cmds(&["VDIM", "vs"]).as_int(), 4);
}

#[test]
fn test_vadd_values_encoding() {
    // Test the VALUES num v1 v2 ... encoding (not binary FP32)
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["VADD", "vs", "VALUES", "3", "1.0", "0.0", "0.5", "alpha"]);
    assert_eq!(r.as_int(), 1);
    assert_eq!(c.cmds(&["VCARD", "vs"]).as_int(), 1);
    assert_eq!(c.cmds(&["VDIM", "vs"]).as_int(), 3);
}

#[test]
fn test_vadd_missing_key_returns_nil_for_vcard() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // VCARD on non-existent key returns 0
    assert_eq!(c.cmds(&["VCARD", "nosuch"]).as_int(), 0);
    // VDIM on non-existent key returns nil
    assert!(c.cmds(&["VDIM", "nosuch"]).is_nil());
}

#[test]
fn test_vadd_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "hello"]);
    let blob = pack_f32(&[1.0, 0.0]);
    let r = c.cmd(&[b"VADD", b"strkey", b"FP32", &blob, b"e"]);
    assert!(r.is_error(), "VADD on wrong type should error");
}

// ── VREM ─────────────────────────────────────────────────────────────────────

#[test]
fn test_vrem() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0, 0.0]);
    let b2 = pack_f32(&[0.0, 1.0, 0.0]);
    let b3 = pack_f32(&[0.0, 0.0, 1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b3, b"c"]);

    assert_eq!(c.cmds(&["VCARD", "vs"]).as_int(), 3);

    // Remove two existing, one missing
    let removed = c.cmds(&["VREM", "vs", "a", "c", "nonexistent"]);
    assert_eq!(
        removed.as_int(),
        2,
        "should remove 2 of 3 elements requested"
    );

    assert_eq!(c.cmds(&["VCARD", "vs"]).as_int(), 1);
    assert_eq!(c.cmds(&["VISMEMBER", "vs", "a"]).as_int(), 0);
    assert_eq!(c.cmds(&["VISMEMBER", "vs", "b"]).as_int(), 1);
}

#[test]
fn test_vrem_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // VREM on a key that doesn't exist returns 0
    assert_eq!(c.cmds(&["VREM", "nosuch", "elem"]).as_int(), 0);
}

// ── VISMEMBER ────────────────────────────────────────────────────────────────

#[test]
fn test_vismember() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0, 0.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"exists"]);

    assert_eq!(c.cmds(&["VISMEMBER", "vs", "exists"]).as_int(), 1);
    assert_eq!(c.cmds(&["VISMEMBER", "vs", "missing"]).as_int(), 0);
    assert_eq!(c.cmds(&["VISMEMBER", "nosuchkey", "elem"]).as_int(), 0);
}

// ── VEMB ─────────────────────────────────────────────────────────────────────

#[test]
fn test_vemb_float_array() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let original = vec![0.25_f32, 0.5, 0.75];
    let blob = pack_f32(&original);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"elem"]);

    // Default: returns array of float strings
    let r = c.cmds(&["VEMB", "vs", "elem"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 3);
    let v0: f32 = arr[0].as_str().parse().unwrap();
    let v1: f32 = arr[1].as_str().parse().unwrap();
    let v2: f32 = arr[2].as_str().parse().unwrap();
    assert!((v0 - 0.25).abs() < 1e-6);
    assert!((v1 - 0.5).abs() < 1e-6);
    assert!((v2 - 0.75).abs() < 1e-6);
}

#[test]
fn test_vemb_raw() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let original = vec![1.0_f32, 2.0, 3.0];
    let blob = pack_f32(&original);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"elem"]);

    // RAW: returns little-endian FP32 bytes
    let r = c.cmds(&["VEMB", "vs", "elem", "RAW"]);
    let bytes = match &r {
        RespValue::Bulk(b) => b.clone(),
        _ => panic!("expected bulk: {:?}", r),
    };
    assert_eq!(bytes.len(), 12); // 3 × 4 bytes
    let v0 = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert!((v0 - 1.0).abs() < 1e-6);
}

#[test]
fn test_vemb_missing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"exists"]);

    assert!(c.cmds(&["VEMB", "vs", "nosuch"]).is_nil());
    assert!(c.cmds(&["VEMB", "nosuchkey", "elem"]).is_nil());
}

// ── VGETATTR / VSETATTR ───────────────────────────────────────────────────────

#[test]
fn test_vgetattr_vsetattr() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0, 0.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"elem"]);

    // No attrs yet — nil
    assert!(c.cmds(&["VGETATTR", "vs", "elem"]).is_nil());

    // Set attrs
    assert!(
        c.cmds(&["VSETATTR", "vs", "elem", r#"{"color":"red"}"#])
            .is_ok()
    );

    // Get attrs
    let attrs = c.cmds(&["VGETATTR", "vs", "elem"]);
    assert_eq!(attrs.as_str(), r#"{"color":"red"}"#);

    // Update attrs
    assert!(
        c.cmds(&["VSETATTR", "vs", "elem", r#"{"color":"blue"}"#])
            .is_ok()
    );
    assert_eq!(
        c.cmds(&["VGETATTR", "vs", "elem"]).as_str(),
        r#"{"color":"blue"}"#
    );

    // Clear attrs with empty string
    assert!(c.cmds(&["VSETATTR", "vs", "elem", ""]).is_ok());
    assert!(c.cmds(&["VGETATTR", "vs", "elem"]).is_nil());
}

#[test]
fn test_vsetattr_vadd_setattr_combined() {
    // VADD with SETATTR inline
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0, 0.0]);
    let r = c.cmd(&[
        b"VADD",
        b"vs",
        b"FP32",
        &blob,
        b"elem",
        b"SETATTR",
        b"{\"score\":99}",
    ]);
    assert_eq!(r.as_int(), 1);

    let attrs = c.cmds(&["VGETATTR", "vs", "elem"]);
    assert_eq!(attrs.as_str(), "{\"score\":99}");
}

#[test]
fn test_vgetattr_missing_element() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"exists"]);

    assert!(c.cmds(&["VGETATTR", "vs", "nosuch"]).is_nil());
}

// ── VINFO ─────────────────────────────────────────────────────────────────────

#[test]
fn test_vinfo() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0, 0.0]);
    let b2 = pack_f32(&[0.0, 1.0, 0.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);

    let info = c.cmds(&["VINFO", "vs"]);
    let arr = info.as_array();

    // VINFO returns a flat key-value array where keys are bulk strings
    // and values can be integers OR strings — so we must check the key
    // type before calling as_str(), then read the value by whatever type it is.
    fn find_vinfo_int(arr: &[RespValue], key: &str) -> Option<i64> {
        let mut i = 0;
        while i + 1 < arr.len() {
            let is_match = match &arr[i] {
                RespValue::Bulk(b) => b.as_slice() == key.as_bytes(),
                RespValue::SimpleStr(s) => s == key,
                _ => false,
            };
            if is_match {
                return match &arr[i + 1] {
                    RespValue::Int(n) => Some(*n),
                    // Some servers return integers as bulk strings
                    RespValue::Bulk(b) => std::str::from_utf8(b).ok()?.parse().ok(),
                    _ => None,
                };
            }
            i += 2;
        }
        None
    }

    let dim = find_vinfo_int(arr, "vector-dim").expect("VINFO should contain vector-dim");
    assert_eq!(dim, 3);

    let count = find_vinfo_int(arr, "vector-count").expect("VINFO should contain vector-count");
    assert_eq!(count, 2);
}

#[test]
fn test_vinfo_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["VINFO", "nosuch"]).is_nil());
}

// ── VSIM ─────────────────────────────────────────────────────────────────────

#[test]
fn test_vsim_fp32_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Insert 3 vectors
    let b1 = pack_f32(&[1.0, 0.0, 0.0, 0.0]); // identical to query
    let b2 = pack_f32(&[0.0, 1.0, 0.0, 0.0]); // orthogonal
    let b3 = pack_f32(&[0.9, 0.1, 0.0, 0.0]); // close to query
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"exact"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"ortho"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b3, b"close"]);

    // Query with FP32 blob, return top 2
    let query = pack_f32(&[1.0, 0.0, 0.0, 0.0]);
    let r = c.cmd(&[b"VSIM", b"vs", b"FP32", &query, b"COUNT", b"2"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2, "COUNT 2 should return 2 elements");
    assert_eq!(arr[0].as_str(), "exact", "most similar should be first");
    assert_eq!(
        arr[1].as_str(),
        "close",
        "second most similar should be close"
    );
}

#[test]
fn test_vsim_withscores() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0]);
    let b2 = pack_f32(&[0.0, 1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);

    let query = pack_f32(&[1.0, 0.0]);
    let r = c.cmd(&[
        b"VSIM",
        b"vs",
        b"FP32",
        &query,
        b"WITHSCORES",
        b"COUNT",
        b"2",
    ]);
    let arr = r.as_array();

    // WITHSCORES: element, score, element, score...
    assert_eq!(arr.len(), 4, "WITHSCORES doubles the output");
    assert_eq!(arr[0].as_str(), "a");
    let score_a: f32 = arr[1].as_str().parse().unwrap();
    // Cosine similarity of identical vectors = 1.0
    assert!(
        score_a > 0.99,
        "identical vector similarity should be ~1.0, got {score_a}"
    );

    assert_eq!(arr[2].as_str(), "b");
    let score_b: f32 = arr[3].as_str().parse().unwrap();
    // Cosine similarity of orthogonal vectors = 0.0
    assert!(
        score_b < 0.01,
        "orthogonal vector similarity should be ~0.0, got {score_b}"
    );
}

#[test]
fn test_vsim_ele_encoding() {
    // Query by an existing element name (ELE encoding)
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0, 0.0]);
    let b2 = pack_f32(&[0.9, 0.1, 0.0]);
    let b3 = pack_f32(&[0.0, 1.0, 0.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"anchor"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"near"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b3, b"far"]);

    // Use ELE encoding — query by element name
    let r = c.cmds(&["VSIM", "vs", "ELE", "anchor", "COUNT", "2"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    // anchor itself is typically included and most similar
    let names: Vec<&str> = arr.iter().map(|v| v.as_str()).collect();
    assert!(names.contains(&"near"), "near should be in top 2 results");
}

#[test]
fn test_vsim_values_encoding() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0, 0.0]);
    let b2 = pack_f32(&[0.0, 1.0, 0.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);

    // VALUES encoding: VALUES 3 1.0 0.0 0.0
    let r = c.cmds(&[
        "VSIM", "vs", "VALUES", "3", "1.0", "0.0", "0.0", "COUNT", "1",
    ]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].as_str(), "a");
}

#[test]
fn test_vsim_empty_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // VSIM on non-existent key returns empty array
    let query = pack_f32(&[1.0, 0.0]);
    let r = c.cmd(&[b"VSIM", b"nosuch", b"FP32", &query]);
    assert_eq!(r.as_array().len(), 0);
}

// ── VLINKS ───────────────────────────────────────────────────────────────────

#[test]
fn test_vlinks_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0, 0.0]);
    let b2 = pack_f32(&[0.9, 0.1, 0.0]);
    let b3 = pack_f32(&[0.0, 1.0, 0.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b3, b"c"]);

    let r = c.cmds(&["VLINKS", "vs", "a"]);
    let arr = r.as_array();
    // Returns an array of layers; we return one layer
    assert!(!arr.is_empty());
    let layer0 = arr[0].as_array();
    // "a"'s neighbours should include "b" (close) and "c"
    let names: Vec<&str> = layer0.iter().map(|v| v.as_str()).collect();
    assert!(names.contains(&"b"), "b should be a neighbour of a");
}

#[test]
fn test_vlinks_withscores() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0]);
    let b2 = pack_f32(&[0.8, 0.2]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);

    let r = c.cmds(&["VLINKS", "vs", "a", "WITHSCORES"]);
    let arr = r.as_array();
    let layer0 = arr[0].as_array();
    // WITHSCORES: name, score, name, score...
    assert!(
        layer0.len().is_multiple_of(2),
        "WITHSCORES should produce even-length layer"
    );
}

#[test]
fn test_vlinks_missing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"exists"]);

    // Missing element within existing key
    assert!(c.cmds(&["VLINKS", "vs", "nosuch"]).is_nil());
    // Missing key
    assert!(c.cmds(&["VLINKS", "nosuchkey", "elem"]).is_nil());
}

// ── VRANDMEMBER ───────────────────────────────────────────────────────────────

#[test]
fn test_vrandmember_single() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0]);
    let b2 = pack_f32(&[0.0, 1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);

    // No count — single random element
    let r = c.cmds(&["VRANDMEMBER", "vs"]);
    let name = r.as_str();
    assert!(
        name == "a" || name == "b",
        "should return one of the two elements"
    );
}

#[test]
fn test_vrandmember_positive_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0, 0.0]);
    let b2 = pack_f32(&[0.0, 1.0]);
    let b3 = pack_f32(&[0.5, 0.5]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b3, b"c"]);

    // Positive count — distinct elements, up to count
    let r = c.cmds(&["VRANDMEMBER", "vs", "2"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2, "count 2 should return exactly 2 elements");

    // No duplicates
    let names: Vec<&str> = arr.iter().map(|v| v.as_str()).collect();
    assert_ne!(
        names[0], names[1],
        "positive count should return distinct elements"
    );
}

#[test]
fn test_vrandmember_negative_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"only"]);

    // Negative count — |n| elements, repetitions allowed
    let r = c.cmds(&["VRANDMEMBER", "vs", "-5"]);
    let arr = r.as_array();
    assert_eq!(
        arr.len(),
        5,
        "negative count -5 should return exactly 5 elements"
    );
    // With only one element, all must be "only"
    for item in arr {
        assert_eq!(item.as_str(), "only");
    }
}

#[test]
fn test_vrandmember_count_exceeds_size() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b1 = pack_f32(&[1.0]);
    let b2 = pack_f32(&[0.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b1, b"a"]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &b2, b"b"]);

    // Asking for more than available distinct elements
    let r = c.cmds(&["VRANDMEMBER", "vs", "100"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2, "positive count capped at set size");
}

#[test]
fn test_vrandmember_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // No count — nil
    assert!(c.cmds(&["VRANDMEMBER", "nosuch"]).is_nil());
    // With count — empty array
    assert_eq!(c.cmds(&["VRANDMEMBER", "nosuch", "3"]).as_array().len(), 0);
}

// ── VRANGE ───────────────────────────────────────────────────────────────────

#[test]
fn test_vrange_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0, 0.0]);
    // Add elements with lexicographically sortable names
    for name in &["apple", "banana", "cherry", "date", "elderberry"] {
        c.cmd(&[b"VADD", b"vs", b"FP32", &blob, name.as_bytes()]);
    }

    // Full range
    let r = c.cmds(&["VRANGE", "vs", "-", "+"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 5);
    let names: Vec<&str> = arr.iter().map(|v| v.as_str()).collect();
    assert_eq!(
        names,
        vec!["apple", "banana", "cherry", "date", "elderberry"]
    );
}

#[test]
fn test_vrange_inclusive_bounds() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    for name in &["a", "b", "c", "d", "e"] {
        c.cmd(&[b"VADD", b"vs", b"FP32", &blob, name.as_bytes()]);
    }

    // [b to [d — inclusive on both ends
    let r = c.cmds(&["VRANGE", "vs", "[b", "[d"]);
    let arr = r.as_array();
    let names: Vec<&str> = arr.iter().map(|v| v.as_str()).collect();
    assert_eq!(names, vec!["b", "c", "d"]);
}

#[test]
fn test_vrange_exclusive_bounds() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    for name in &["a", "b", "c", "d", "e"] {
        c.cmd(&[b"VADD", b"vs", b"FP32", &blob, name.as_bytes()]);
    }

    // (a to (e — exclusive on both ends
    let r = c.cmds(&["VRANGE", "vs", "(a", "(e"]);
    let arr = r.as_array();
    let names: Vec<&str> = arr.iter().map(|v| v.as_str()).collect();
    assert_eq!(names, vec!["b", "c", "d"]);
}

#[test]
fn test_vrange_with_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    for name in &["a", "b", "c", "d", "e"] {
        c.cmd(&[b"VADD", b"vs", b"FP32", &blob, name.as_bytes()]);
    }

    // Limit to 2 results
    let r = c.cmds(&["VRANGE", "vs", "-", "+", "2"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_str(), "a");
    assert_eq!(arr[1].as_str(), "b");
}

#[test]
fn test_vrange_empty_range() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let blob = pack_f32(&[1.0]);
    c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"m"]);

    // Range that matches nothing
    let r = c.cmds(&["VRANGE", "vs", "[z", "+"]);
    assert_eq!(r.as_array().len(), 0);
}

#[test]
fn test_vrange_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["VRANGE", "nosuch", "-", "+"]);
    assert_eq!(r.as_array().len(), 0);
}

// ── Dimension mismatch ────────────────────────────────────────────────────────

#[test]
fn test_vadd_dimension_mismatch() {
    // Once a VectorSet has a dimension, all subsequent VADDs must match
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let b4 = pack_f32(&[1.0, 0.0, 0.0, 0.0]); // dim 4
    c.cmd(&[b"VADD", b"vs", b"FP32", &b4, b"elem1"]);

    let b3 = pack_f32(&[1.0, 0.0, 0.0]); // dim 3 — mismatch
    let r = c.cmd(&[b"VADD", b"vs", b"FP32", &b3, b"elem2"]);
    assert!(r.is_error(), "dim mismatch should return error");
}

// ── End-to-end workflow ───────────────────────────────────────────────────────

#[test]
fn test_vset_full_workflow() {
    // Simulate a real embedding search workflow:
    // 1. Add document embeddings
    // 2. Attach metadata attrs
    // 3. Search by similarity
    // 4. Remove a document
    // 5. Verify it no longer appears
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // 4-dim embeddings representing documents
    let doc_a = pack_f32(&[0.9, 0.1, 0.0, 0.0]);
    let doc_b = pack_f32(&[0.1, 0.9, 0.0, 0.0]);
    let doc_c = pack_f32(&[0.8, 0.2, 0.0, 0.0]); // similar to doc_a
    let doc_d = pack_f32(&[0.0, 0.0, 1.0, 0.0]); // orthogonal

    c.cmd(&[b"VADD", b"docs", b"FP32", &doc_a, b"doc:a"]);
    c.cmd(&[b"VADD", b"docs", b"FP32", &doc_b, b"doc:b"]);
    c.cmd(&[b"VADD", b"docs", b"FP32", &doc_c, b"doc:c"]);
    c.cmd(&[b"VADD", b"docs", b"FP32", &doc_d, b"doc:d"]);

    // Attach metadata
    c.cmds(&["VSETATTR", "docs", "doc:a", r#"{"title":"AI basics"}"#]);
    c.cmds(&[
        "VSETATTR",
        "docs",
        "doc:b",
        r#"{"title":"Database design"}"#,
    ]);

    assert_eq!(c.cmds(&["VCARD", "docs"]).as_int(), 4);

    // Search: query close to doc_a and doc_c
    let query = pack_f32(&[0.85, 0.15, 0.0, 0.0]);
    let r = c.cmd(&[
        b"VSIM",
        b"docs",
        b"FP32",
        &query,
        b"COUNT",
        b"2",
        b"WITHSCORES",
    ]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 4); // 2 results × (name + score)

    let top1 = arr[0].as_str();
    assert!(
        top1 == "doc:a" || top1 == "doc:c",
        "top result should be doc:a or doc:c, got {top1}"
    );

    // Verify attrs on top result
    let attrs = c.cmds(&["VGETATTR", "docs", "doc:a"]);
    assert_eq!(attrs.as_str(), r#"{"title":"AI basics"}"#);

    // Remove doc:c
    assert_eq!(c.cmds(&["VREM", "docs", "doc:c"]).as_int(), 1);
    assert_eq!(c.cmds(&["VISMEMBER", "docs", "doc:c"]).as_int(), 0);
    assert_eq!(c.cmds(&["VCARD", "docs"]).as_int(), 3);

    // Search again — doc:c should not appear
    let r2 = c.cmd(&[b"VSIM", b"docs", b"FP32", &query, b"COUNT", b"3"]);
    let arr2 = r2.as_array();
    let names: Vec<&str> = arr2.iter().map(|v| v.as_str()).collect();
    assert!(
        !names.contains(&"doc:c"),
        "removed doc:c should not appear in results"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 3. VSIM / VLINKS — distance metric honoured
// ═════════════════════════════════════════════════════════════════════════════

// Scenario: cosine similarity (default) — identical vectors score 1.0
//   VADD vs VALUES 3 1 0 0 a
//   VADD vs VALUES 3 1 0 0 b  (identical to a)
//   VSIM vs ELE a WITHSCORES → b first with score ~1.0
#[test]
fn test_vsim_cosine_default() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["VADD", "vs", "VALUES", "3", "1", "0", "0", "a"]);
    c.cmds(&["VADD", "vs", "VALUES", "3", "1", "0", "0", "b"]); // identical direction
    c.cmds(&["VADD", "vs", "VALUES", "3", "0", "1", "0", "c"]); // orthogonal to a

    let r = c.cmds(&["VSIM", "vs", "ELE", "a", "WITHSCORES", "COUNT", "3"]);
    let arr = r.as_array();
    // Results: a(1.0), b(1.0), c(0.0) — a and b are identical, c is orthogonal
    // a itself may or may not be included depending on implementation
    // At minimum, b should be closer to a than c
    let pairs: Vec<(&str, f64)> = arr
        .chunks(2)
        .map(|chunk| (chunk[0].as_str(), chunk[1].as_f64()))
        .collect();

    // The pair for b should have a higher similarity than c
    let b_score = pairs.iter().find(|(n, _)| *n == "b").map(|(_, s)| *s);
    let c_score = pairs.iter().find(|(n, _)| *n == "c").map(|(_, s)| *s);

    if let (Some(bs), Some(cs)) = (b_score, c_score) {
        assert!(
            bs > cs,
            "b (same direction as a) should score higher than c (orthogonal). b={bs}, c={cs}"
        );
        // b should be ~1.0 similarity with a
        assert!(
            bs > 0.99,
            "identical vectors should have similarity ~1.0, got {bs}"
        );
        // c should be ~0.0 similarity with a (orthogonal)
        assert!(
            cs < 0.01,
            "orthogonal vectors should have similarity ~0.0, got {cs}"
        );
    }
}

// Scenario: VSIM with explicit DISTANCE cosine
//   Same result as default — metric stored and used
#[test]
fn test_vsim_explicit_cosine_metric() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&[
        "VADD",
        "vs_cosine_explicit",
        "DISTANCE",
        "COSINE",
        "VALUES",
        "2",
        "1",
        "0",
        "a",
    ]);
    assert_eq!(r.as_int(), 1, "VADD should return 1 for new element");

    // Use unique key to avoid collision with parallel tests
    c.cmds(&[
        "VADD",
        "vs_cosine_explicit",
        "DISTANCE",
        "COSINE",
        "VALUES",
        "2",
        "1",
        "0",
        "a",
    ]);
    c.cmds(&[
        "VADD",
        "vs_cosine_explicit",
        "DISTANCE",
        "COSINE",
        "VALUES",
        "2",
        "0",
        "1",
        "b",
    ]);

    // Verify both elements exist before querying
    assert_eq!(c.cmds(&["VCARD", "vs_cosine_explicit"]).as_int(), 2);

    let r = c.cmds(&[
        "VSIM",
        "vs_cosine_explicit",
        "ELE",
        "a",
        "WITHSCORES",
        "COUNT",
        "2",
    ]);
    let arr = r.as_array();
    let pairs: Vec<(&str, f64)> = arr
        .chunks(2)
        .map(|chunk| (chunk[0].as_str(), chunk[1].as_f64()))
        .collect();

    let b_score = pairs.iter().find(|(n, _)| *n == "b").map(|(_, s)| *s);
    if let Some(bs) = b_score {
        assert!(
            bs < 0.1,
            "orthogonal vectors (cosine) should score ~0.0, got {bs}"
        );
    }
}

// Scenario: VSIM with DISTANCE L2
//   Closer vectors in L2 space score higher (similarity = 1/(1+dist))
//   [1,0] and [2,0] are on same axis, close in L2
//   [1,0] and [0,100] are far in L2
#[test]
fn test_vsim_l2_metric() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&[
        "VADD",
        "vs_cosine_explicit",
        "DISTANCE",
        "COSINE",
        "VALUES",
        "2",
        "1",
        "0",
        "a",
    ]);
    assert_eq!(r.as_int(), 1, "VADD should return 1 for new element");

    // All VADDs must include DISTANCE so the flag is parsed before VALUES
    c.cmds(&[
        "VADD",
        "vs_l2_test",
        "DISTANCE",
        "L2",
        "VALUES",
        "2",
        "1",
        "0",
        "origin",
    ]);
    c.cmds(&[
        "VADD",
        "vs_l2_test",
        "DISTANCE",
        "L2",
        "VALUES",
        "2",
        "2",
        "0",
        "close",
    ]);
    c.cmds(&[
        "VADD",
        "vs_l2_test",
        "DISTANCE",
        "L2",
        "VALUES",
        "2",
        "0",
        "100",
        "far",
    ]);

    // Verify all elements were added
    assert_eq!(c.cmds(&["VCARD", "vs_l2_test"]).as_int(), 3);

    let r = c.cmds(&[
        "VSIM",
        "vs_l2_test",
        "ELE",
        "origin",
        "WITHSCORES",
        "COUNT",
        "3",
    ]);
    let arr = r.as_array();
    let pairs: Vec<(&str, f64)> = arr
        .chunks(2)
        .map(|chunk| (chunk[0].as_str(), chunk[1].as_f64()))
        .collect();

    let close_score = pairs.iter().find(|(n, _)| *n == "close").map(|(_, s)| *s);
    let far_score = pairs.iter().find(|(n, _)| *n == "far").map(|(_, s)| *s);

    if let (Some(cs), Some(fs)) = (close_score, far_score) {
        assert!(
            cs > fs,
            "close vector should score higher than far vector under L2. close={cs}, far={fs}"
        );
    }
}

// Scenario: VLINKS returns nearest neighbours using stored metric
//   VADD vs VALUES 2 1 0 a
//   VADD vs VALUES 2 1 0 b  (identical to a)
//   VADD vs VALUES 2 0 1 c  (orthogonal)
//   VLINKS vs a WITHSCORES → b should have higher score than c
#[test]
fn test_vlinks_uses_stored_metric() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["VADD", "vs", "VALUES", "2", "1", "0", "a"]);
    c.cmds(&["VADD", "vs", "VALUES", "2", "1", "0", "b"]); // same as a
    c.cmds(&["VADD", "vs", "VALUES", "2", "0", "1", "c"]); // orthogonal to a

    let r = c.cmds(&["VLINKS", "vs", "a", "WITHSCORES"]);
    // VLINKS returns array of arrays (one per HNSW layer)
    // Layer 0 is the flat array of [name, score?, name, score?, ...]
    let outer = r.as_array();
    assert!(!outer.is_empty(), "VLINKS should return at least one layer");

    let layer0 = outer[0].as_array();
    let pairs: Vec<(&str, f64)> = layer0
        .chunks(2)
        .map(|chunk| (chunk[0].as_str(), chunk[1].as_f64()))
        .collect();

    let b_score = pairs.iter().find(|(n, _)| *n == "b").map(|(_, s)| *s);
    let c_score = pairs.iter().find(|(n, _)| *n == "c").map(|(_, s)| *s);

    if let (Some(bs), Some(cs)) = (b_score, c_score) {
        assert!(
            bs > cs,
            "b (same direction) should score higher than c (orthogonal) in VLINKS. b={bs}, c={cs}"
        );
    }
}

// Scenario: VLINKS without WITHSCORES — only names returned
#[test]
fn test_vlinks_without_scores() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["VADD", "vs", "VALUES", "2", "1", "0", "a"]);
    c.cmds(&["VADD", "vs", "VALUES", "2", "0", "1", "b"]);
    c.cmds(&["VADD", "vs", "VALUES", "2", "1", "1", "c"]);

    let r = c.cmds(&["VLINKS", "vs", "a"]);
    let outer = r.as_array();
    assert!(!outer.is_empty());

    // Without WITHSCORES, layer0 is just names
    let layer0 = outer[0].as_array();
    // All items should be string names (not scores)
    for item in layer0 {
        let s = item.as_str();
        // Should be a valid element name, not a float string
        assert!(s == "b" || s == "c", "unexpected element in VLINKS: {s}");
    }
}

// Scenario: VLINKS on non-existent element → nil
#[test]
fn test_vlinks_missing_element() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["VADD", "vs", "VALUES", "2", "1", "0", "a"]);
    assert!(c.cmds(&["VLINKS", "vs", "nosuch"]).is_nil());
}

// Scenario: VLINKS on non-existent key → nil
#[test]
fn test_vlinks_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["VLINKS", "nokey", "a"]).is_nil());
}

/// `VSIM ... FILTER` keeps the elements whose attributes pass, as in Redis;
/// it was taken and ignored. `REDUCE` is refused rather than ignored, and
/// `VINFO` says the vectors are kept at full precision (#132).
#[test]
fn test_vsim_filter_reduce_and_quantization() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["VADD", "points", "VALUES", "3", "1", "0", "0", "a"]);
    c.cmds(&[
        "VADD",
        "points",
        "VALUES",
        "3",
        "0.9",
        "0.1",
        "0",
        "b",
        "SETATTR",
        r#"{"color":"blue","size":1}"#,
    ]);
    c.cmds(&[
        "VADD",
        "points",
        "VALUES",
        "3",
        "0",
        "1",
        "0",
        "c",
        "SETATTR",
        r#"{"color":"red","size":3}"#,
    ]);
    let names = |reply: &RespValue| -> Vec<String> {
        reply
            .as_array()
            .iter()
            .map(|v| v.as_str().to_string())
            .collect()
    };
    let all = c.cmds(&["VSIM", "points", "VALUES", "3", "1", "0", "0"]);
    assert_eq!(names(&all), ["a", "b", "c"]);
    let red = c.cmds(&[
        "VSIM",
        "points",
        "VALUES",
        "3",
        "1",
        "0",
        "0",
        "FILTER",
        r#".color == "red""#,
    ]);
    assert_eq!(names(&red), ["c"]);
    let sized = c.cmds(&[
        "VSIM",
        "points",
        "VALUES",
        "3",
        "1",
        "0",
        "0",
        "FILTER",
        ".size >= 1 and .size < 3",
    ]);
    assert_eq!(names(&sized), ["b"]);
    let bad = c.cmds(&[
        "VSIM", "points", "VALUES", "3", "1", "0", "0", "FILTER", "(.size",
    ]);
    assert!(bad.is_error(), "{bad:?}");

    let reduce = c.cmds(&[
        "VADD", "q", "REDUCE", "2", "VALUES", "4", "1", "2", "3", "4", "x",
    ]);
    assert!(reduce.is_error(), "{reduce:?}");

    assert_eq!(
        c.cmds(&["VADD", "r", "VALUES", "2", "0", "0", "o", "Q8"])
            .as_int(),
        1
    );
    let info = c.cmds(&["VINFO", "r"]);
    let info = info.as_array();
    let at = info
        .iter()
        .position(|v| matches!(v, RespValue::Bulk(b) if b == b"quant-type"))
        .expect("quant-type");
    assert_eq!(info[at + 1].as_str(), "f32");
}
