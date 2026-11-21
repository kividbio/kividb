#![allow(clippy::disallowed_methods)]
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

    // RAW: type, unit vector bytes, then the norm.
    let r = c.cmds(&["VEMB", "vs", "elem", "RAW"]);
    let parts = r.as_array();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0].as_str(), "f32");
    let bytes = match &parts[1] {
        RespValue::Bulk(b) => b.clone(),
        other => panic!("expected bulk: {:?}", other),
    };
    assert_eq!(bytes.len(), 12); // 3 × 4 bytes
    let norm = parts[2].as_f64();
    assert!((norm - 14f64.sqrt()).abs() < 1e-5);
    let v0 = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert!((v0 as f64 * norm - 1.0).abs() < 1e-5);
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
    assert_eq!(
        c.cmds(&["VSETATTR", "vs", "elem", r#"{"color":"red"}"#])
            .as_int(),
        1
    );

    // Get attrs
    let attrs = c.cmds(&["VGETATTR", "vs", "elem"]);
    assert_eq!(attrs.as_str(), r#"{"color":"red"}"#);

    // Update attrs
    assert_eq!(
        c.cmds(&["VSETATTR", "vs", "elem", r#"{"color":"blue"}"#])
            .as_int(),
        1
    );
    assert_eq!(
        c.cmds(&["VGETATTR", "vs", "elem"]).as_str(),
        r#"{"color":"blue"}"#
    );

    // Clear attrs with empty string
    assert_eq!(c.cmds(&["VSETATTR", "vs", "elem", ""]).as_int(), 1);
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
    // Orthogonal vectors: 0.5, as Redis scores cosine, (1 + cosine) / 2 (#171)
    assert!(
        (score_b - 0.5).abs() < 0.01,
        "orthogonal vector similarity should be ~0.5, got {score_b}"
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

// Scenario: cosine similarity (default), scored as Redis does (#171):
// (1 + cosine) / 2, so identical vectors score 1.0, vectors at right angles
// 0.5, and opposite vectors 0.0
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
    c.cmds(&["VADD", "vs", "VALUES", "3", "-1", "0", "0", "d"]); // opposite to a

    let r = c.cmds(&["VSIM", "vs", "ELE", "a", "WITHSCORES", "COUNT", "4"]);
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
        // c is at right angles to a: 0.5, as in Redis
        assert!(
            (cs - 0.5).abs() < 0.01,
            "orthogonal vectors should have similarity ~0.5, got {cs}"
        );
        let d_score = pairs.iter().find(|(n, _)| *n == "d").map(|(_, s)| *s);
        // d is opposite: 0.0, the lowest score, never negative
        assert!(
            d_score.is_some_and(|ds| ds.abs() < 0.01),
            "opposite vectors should have similarity ~0.0, got {d_score:?}"
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
            (bs - 0.5).abs() < 0.1,
            "orthogonal vectors (cosine) should score ~0.5, got {bs}"
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

/// FILTER applies, REDUCE is refused, and VINFO shows the real quantization.
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
        .position(|v| v.as_str() == "quant-type")
        .expect("quant-type");
    assert_eq!(info[at + 1].as_str(), "int8");
}

/// #235: `VADD` took a `DISTANCE` it did not know as COSINE and skipped any
/// option it did not know. The errors are those Redis 8.10.2 gives for the
/// same commands (it has no `DISTANCE`: that one is KiviDB's own).
#[test]
fn vadd_refuses_what_it_does_not_know() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let err = |c: &mut Conn, args: &[&str]| match c.cmds(args) {
        RespValue::Error(e) => e,
        other => panic!("{args:?}: {other:?}"),
    };
    let v = ["VALUES", "2", "1", "2", "e"];
    let vadd = |key: &'static str, tail: &[&'static str]| {
        let mut args = vec!["VADD", key];
        args.extend_from_slice(&v);
        args.extend_from_slice(tail);
        args
    };

    for (tail, want) in [
        (
            &["DISTANCE", "FOO"][..],
            "ERR invalid DISTANCE: use COSINE, L2 or IP",
        ),
        (&["DISTANCE"][..], "ERR invalid option after element"),
        (&["METRIC", "IP"][..], "ERR invalid option after element"),
        (&["FOO"][..], "ERR invalid option after element"),
        (&["CAS", "FOO"][..], "ERR invalid option after element"),
        (&["M"][..], "ERR invalid option after element"),
        (&["EF"][..], "ERR invalid option after element"),
        (&["SETATTR"][..], "ERR invalid option after element"),
        (&["M", "3"][..], "ERR invalid M"),
        (&["M", "4097"][..], "ERR invalid M"),
        (&["M", "+8"][..], "ERR invalid M"),
        (&["M", "abc"][..], "ERR invalid M"),
        (&["EF", "0"][..], "ERR invalid EF"),
        (&["EF", "1000001"][..], "ERR invalid EF"),
        (&["EF", "010"][..], "ERR invalid EF"),
    ] {
        assert_eq!(err(&mut c, &vadd("bad", tail)), want, "{tail:?}");
    }
    for args in [
        &[
            "VADD", "bad", "DISTANCE", "FOO", "VALUES", "2", "1", "2", "e",
        ][..],
        &[
            "VADD", "bad", "DISTANCE", "IP", "FOO", "VALUES", "2", "1", "2", "e",
        ][..],
    ] {
        assert!(err(&mut c, args).starts_with("ERR invalid "), "{args:?}");
    }
    for args in [
        &["VADD", "bad", "FOO", "VALUES", "2", "1", "2", "e"][..],
        &["VADD", "bad", "VALUES", "0", "e"][..],
        &["VADD", "bad", "VALUES", "abc", "1", "2", "e"][..],
        &["VADD", "bad", "VALUES", "2", "1", "x", "e"][..],
        &["VADD", "bad", "VALUES", "2", "nan", "2", "e"][..],
        &["VADD", "bad", "FP32", "abc", "e"][..],
    ] {
        assert_eq!(
            err(&mut c, args),
            "ERR invalid vector specification",
            "{args:?}"
        );
    }
    // Nothing of the above made the key.
    assert_eq!(c.cmds(&["EXISTS", "bad"]).as_int(), 0);
    // An option is checked before the key's type, as in Redis.
    c.cmds(&["SET", "s", "x"]);
    assert_eq!(
        err(&mut c, &vadd("s", &["FOO"])),
        "ERR invalid option after element"
    );

    // What is valid still is, in any case, last of each winning.
    assert_eq!(c.cmds(&vadd("ok1", &["M", "4", "EF", "1"])).as_int(), 1);
    assert_eq!(
        c.cmds(&vadd("ok2", &["m", "4096", "ef", "1000000", "cas", "q8"]))
            .as_int(),
        1
    );
    assert_eq!(
        c.cmds(&vadd(
            "ok3",
            &["SETATTR", "{\"a\":1}", "SETATTR", "{\"b\":1}"]
        ))
        .as_int(),
        1
    );
    assert_eq!(c.cmds(&["VGETATTR", "ok3", "e"]).as_str(), "{\"b\":1}");

    // `DISTANCE`, before the vector or after the element; once a set has
    // one, another is refused and none means the set's.
    assert_eq!(c.cmds(&vadd("l2", &["DISTANCE", "l2"])).as_int(), 1);
    assert_eq!(
        c.cmds(&["VADD", "l2", "DISTANCE", "L2", "VALUES", "2", "3", "4", "f"])
            .as_int(),
        1
    );
    assert_eq!(
        c.cmds(&["VADD", "l2", "VALUES", "2", "5", "6", "g"])
            .as_int(),
        1
    );
    assert_eq!(
        err(&mut c, &vadd("l2", &["DISTANCE", "IP"])),
        "ERR asked distance mismatch with existing vector set"
    );
    assert_eq!(
        err(&mut c, &vadd("l2", &["DISTANCE", "COSINE"])),
        "ERR asked distance mismatch with existing vector set"
    );
    assert_eq!(
        err(&mut c, &["VADD", "l2", "VALUES", "3", "1", "2", "3", "h"]),
        "ERR Vector dimension mismatch - got 3 but set has 2"
    );
    assert_eq!(c.cmds(&["VCARD", "l2"]).as_int(), 3);
    assert_eq!(c.cmds(&vadd("ip", &["DISTANCE", "IP"])).as_int(), 1);
    assert_eq!(c.cmds(&vadd("cos", &["DISTANCE", "COSINE"])).as_int(), 1);
}

/// What the vector-sets module refuses, and how VSETATTR answers (#561).
#[test]
fn vset_arguments_are_checked_as_the_module_checks_them() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let err = |c: &mut Conn, args: &[&str]| match c.cmds(args) {
        RespValue::Error(e) => e,
        other => panic!("{args:?}: {other:?}"),
    };
    assert_eq!(
        c.cmds(&["VADD", "v", "VALUES", "2", "1", "2", "e"])
            .as_int(),
        1
    );

    // VSETATTR: 1 when set, 0 for no such element or key; attributes are
    // stored as given, JSON or not, as the module stores them.
    assert_eq!(c.cmds(&["VSETATTR", "v", "e", "not json"]).as_int(), 1);
    assert_eq!(c.cmds(&["VGETATTR", "v", "e"]).as_str(), "not json");
    assert_eq!(c.cmds(&["VSETATTR", "v", "nope", "{}"]).as_int(), 0);
    assert_eq!(c.cmds(&["VSETATTR", "nokey", "e", "{}"]).as_int(), 0);

    // VRANDMEMBER key [count]: an integer count, and no more.
    assert_eq!(
        err(&mut c, &["VRANDMEMBER", "v", "abc"]),
        "ERR COUNT value is not an integer"
    );
    assert!(err(&mut c, &["VRANDMEMBER", "v", "1", "2"]).starts_with("ERR wrong number"));
    assert_eq!(c.cmds(&["VRANDMEMBER", "v", "1"]).as_array().len(), 1);

    // VEMB key element [RAW]: nothing else.
    assert_eq!(
        err(&mut c, &["VEMB", "v", "e", "foo"]),
        "ERR invalid option"
    );
    assert_eq!(
        err(&mut c, &["VEMB", "v", "e", "RAW", "x"]),
        "ERR invalid option"
    );
    assert!(!c.cmds(&["VEMB", "v", "e", "RAW"]).is_error());
    assert_eq!(c.cmds(&["VEMB", "v", "e"]).as_array().len(), 2);
}

// â”€â”€ The graph (#710) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

/// More elements than a set scans (`vset_graph::EXACT_BELOW`, 4,096): such a
/// set is searched in its graph once the graph is made.
const GRAPH_N: usize = 4096 + 600;

/// A deterministic vector of `dim` components in [-1, 1).
fn seeded(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn vinfo_field(c: &mut Conn, key: &str, field: &str) -> RespValue {
    let info = c.cmds(&["VINFO", key]);
    let items = info.as_array();
    let at = items
        .iter()
        .step_by(2)
        .position(|k| k.as_str() == field)
        .unwrap_or_else(|| panic!("no {field} in VINFO"));
    match &items[at * 2 + 1] {
        RespValue::Int(n) => RespValue::Int(*n),
        RespValue::SimpleStr(s) => RespValue::SimpleStr(s.clone()),
        RespValue::Bulk(b) => RespValue::Bulk(b.clone()),
        other => panic!("{field}: {other:?}"),
    }
}

/// A set of `GRAPH_N` elements `e<i>` of `dim` components, its graph made.
fn graph_set(c: &mut Conn, key: &str, dim: usize, opts: &[&str]) {
    for i in 0..GRAPH_N as u64 {
        let blob = pack_f32(&seeded(i, dim));
        let name = format!("e{i}");
        let mut args: Vec<&[u8]> = vec![b"VADD", key.as_bytes(), b"FP32", &blob, name.as_bytes()];
        args.extend(opts.iter().map(|o| o.as_bytes()));
        assert_eq!(c.cmd(&args).as_int(), 1);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while vinfo_field(c, key, "index-type").as_str() != "HNSW" {
        assert!(
            std::time::Instant::now() < deadline,
            "{key}'s graph was not made"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn sim(c: &mut Conn, key: &str, v: &[f32], extra: &[&str]) -> Vec<String> {
    let blob = pack_f32(v);
    let mut args: Vec<&[u8]> = vec![b"VSIM", key.as_bytes(), b"FP32", &blob];
    args.extend(extra.iter().map(|o| o.as_bytes()));
    c.cmd(&args)
        .as_array()
        .iter()
        .map(|v| v.as_str().to_string())
        .collect()
}

/// `VSIM` of a large set searches its graph; `TRUTH` scans. Each finds what
/// the other does, mostly, and scores an element alike.
#[test]
fn vsim_searches_the_graph_and_truth_scans() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let dim = 24;
    graph_set(&mut c, "vs", dim, &[]);
    assert_eq!(vinfo_field(&mut c, "vs", "size").as_int(), GRAPH_N as i64);
    assert_eq!(vinfo_field(&mut c, "vs", "hnsw-m").as_int(), 16);
    // Node slots used so far: the helpers may still be linking.
    assert!(vinfo_field(&mut c, "vs", "hnsw-max-node-uid").as_int() > 0);

    let mut found = 0;
    for q in 0..50u64 {
        let v = seeded(1_000_000 + q, dim);
        let truth = sim(&mut c, "vs", &v, &["TRUTH", "COUNT", "10"]);
        let graph = sim(&mut c, "vs", &v, &["COUNT", "10"]);
        assert_eq!(truth.len(), 10);
        assert_eq!(graph.len(), 10);
        found += graph.iter().filter(|n| truth.contains(n)).count();
        // A name and its score: the same whichever way it was found.
        let t = sim(&mut c, "vs", &v, &["TRUTH", "COUNT", "10", "WITHSCORES"]);
        let g = sim(&mut c, "vs", &v, &["COUNT", "10", "WITHSCORES"]);
        let scores: std::collections::HashMap<&String, &String> =
            t.chunks(2).map(|p| (&p[0], &p[1])).collect();
        for pair in g.chunks(2) {
            if let Some(s) = scores.get(&pair[0]) {
                assert_eq!(*s, &pair[1], "query {q}: {}'s score", pair[0]);
            }
        }
    }
    let recall = found as f64 / 500.0;
    assert!(recall >= 0.9, "recall@10 {recall}");

    // An element added now is found by the next search of its vector, and
    // one removed is not; a new vector is found where it now is.
    let v = seeded(9_999_999, dim);
    let blob = pack_f32(&v);
    assert_eq!(c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"new"]).as_int(), 1);
    assert_eq!(sim(&mut c, "vs", &v, &["COUNT", "1"]), ["new"]);
    assert_eq!(c.cmd(&[b"VADD", b"vs", b"FP32", &blob, b"e5"]).as_int(), 0);
    let near = sim(&mut c, "vs", &v, &["COUNT", "2"]);
    assert!(
        near.contains(&"e5".to_string()) && near.contains(&"new".to_string()),
        "{near:?}"
    );
    assert_eq!(c.cmds(&["VREM", "vs", "new", "e5"]).as_int(), 2);
    let after = sim(&mut c, "vs", &v, &["COUNT", "50"]);
    assert!(!after.iter().any(|n| n == "new" || n == "e5"), "{after:?}");
    let old5 = seeded(5, dim);
    assert!(!sim(&mut c, "vs", &old5, &["COUNT", "50"]).contains(&"e5".to_string()));
}

/// The options Redis checks, checked as it checks them, and what they do.
#[test]
fn vsim_options_are_checked_and_honoured_as_redis_does() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    graph_set(&mut c, "vs", 8, &[]);
    let err = |c: &mut Conn, args: &[&str]| match c.cmds(args) {
        RespValue::Error(e) => e,
        other => panic!("{args:?}: {other:?}"),
    };
    let base = ["VSIM", "vs", "ELE", "e1"];
    for (extra, want) in [
        (&["COUNT", "0"][..], "ERR invalid COUNT"),
        (&["COUNT", "x"], "ERR invalid COUNT"),
        (&["EF", "0"], "ERR invalid EF"),
        (&["EF", "1000001"], "ERR invalid EF"),
        (&["EPSILON", "0"], "ERR invalid EPSILON"),
        (&["EPSILON", "x"], "ERR invalid EPSILON"),
        (&["FILTER-EF", "0"], "ERR invalid FILTER-EF"),
        (&["FOO"], "ERR syntax error in VSIM command"),
        (&["COUNT"], "ERR syntax error in VSIM command"),
    ] {
        let args: Vec<&str> = base.iter().chain(extra).copied().collect();
        assert_eq!(err(&mut c, &args), want, "{extra:?}");
    }
    assert_eq!(
        err(&mut c, &["VSIM", "vs", "ELE", "nope"]),
        "ERR element not found in set"
    );
    // COUNT past the set's size: all of it, by a scan.
    let all = c.cmds(&["VSIM", "vs", "ELE", "e1", "COUNT", "100000"]);
    assert_eq!(all.as_array().len(), GRAPH_N);
    // EPSILON: only the elements within that distance (one less the score).
    let near = c.cmds(&[
        "VSIM",
        "vs",
        "ELE",
        "e1",
        "COUNT",
        "50",
        "WITHSCORES",
        "EPSILON",
        "0.05",
    ]);
    let items = near.as_array();
    assert!(!items.is_empty());
    assert_eq!(items[0].as_str(), "e1");
    for pair in items.chunks(2) {
        assert!(pair[1].as_f64() >= 0.95, "{:?}", pair);
    }
    let fewer = c.cmds(&[
        "VSIM",
        "vs",
        "ELE",
        "e1",
        "COUNT",
        "50",
        "EPSILON",
        "0.0000001",
    ]);
    assert_eq!(fewer.as_array().len(), 1);
    // NOTHREAD and TRUTH take no value.
    assert_eq!(
        c.cmds(&["VSIM", "vs", "ELE", "e1", "NOTHREAD", "TRUTH", "COUNT", "1"])
            .as_array()[0]
            .as_str(),
        "e1"
    );
}

/// FILTER through the graph: only elements that pass, and with a filter
/// that passes few, the graph walk gives up and the set is scanned: the
/// answer is the exact one.
#[test]
fn vsim_filter_on_a_large_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let dim = 8;
    graph_set(&mut c, "vs", dim, &[]);
    for i in 0..GRAPH_N {
        let attrs = format!(r#"{{"i":{i}}}"#);
        assert_eq!(
            c.cmds(&["VSETATTR", "vs", &format!("e{i}"), &attrs])
                .as_int(),
            1
        );
    }
    for q in 0..20u64 {
        let v = seeded(5_000 + q, dim);
        let got = sim(
            &mut c,
            "vs",
            &v,
            &["COUNT", "10", "FILTER", ".i % 2 == 0", "WITHATTRIBS"],
        );
        assert_eq!(got.len(), 20);
        for pair in got.chunks(2) {
            let i: u64 = pair[0][1..].parse().unwrap();
            assert_eq!(i % 2, 0, "{pair:?}");
            assert_eq!(pair[1], format!(r#"{{"i":{i}}}"#));
        }
        // Three elements pass: all three, as a scan finds them.
        let rare = sim(&mut c, "vs", &v, &["COUNT", "10", "FILTER", ".i < 3"]);
        let exact = sim(
            &mut c,
            "vs",
            &v,
            &["COUNT", "10", "FILTER", ".i < 3", "TRUTH"],
        );
        assert_eq!(rare.len(), 3);
        assert_eq!(rare, exact);
    }
}

/// VLINKS reports the graph: a layer per level, the top one first, each
/// neighbour with its score under WITHSCORES; VINFO's max-level is the
/// graph's top layer. Only `WITHSCORES` may follow the element.
#[test]
fn vlinks_and_vinfo_report_the_graph() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    graph_set(&mut c, "vs", 8, &["M", "8"]);
    assert_eq!(vinfo_field(&mut c, "vs", "hnsw-m").as_int(), 8);
    let max_level = vinfo_field(&mut c, "vs", "max-level").as_int();
    assert!(
        max_level >= 1,
        "a graph of {GRAPH_N} at M 8 has upper layers"
    );
    let mut tallest = 0;
    for i in 0..200 {
        let e = format!("e{i}");
        let layers = c.cmds(&["VLINKS", "vs", &e]);
        let layers = layers.as_array();
        assert!(!layers.is_empty() && layers.len() as i64 <= max_level + 1);
        tallest = tallest.max(layers.len());
        // Layer 0 holds up to 2 * M links (and some slack), none to itself.
        let layer0 = layers.last().unwrap().as_array();
        assert!(
            !layer0.is_empty() && layer0.len() <= 2 * 8 + 4,
            "{}",
            layer0.len()
        );
        assert!(layer0.iter().all(|n| n.as_str() != e));
        let scored = c.cmds(&["VLINKS", "vs", &e, "WITHSCORES"]);
        let scored = scored.as_array();
        // The same layers, unless the element was still waiting to be
        // linked at one of the calls (one layer, its nearest elements).
        assert!(
            scored.len() == layers.len() || scored.len() == 1 || layers.len() == 1,
            "{} and {} layers",
            scored.len(),
            layers.len()
        );
        for (plain, with) in layers.iter().zip(scored) {
            let with = with.as_array();
            // Helpers may still prune links between the two calls: the
            // same shape, not the same list.
            assert!(with.len().is_multiple_of(2), "{}", plain.as_array().len());
            for pair in with.chunks(2) {
                let s = pair[1].as_f64();
                assert!((0.0..=1.0).contains(&s), "{s}");
            }
        }
    }
    assert!(tallest >= 2, "no element of 200 is above layer 0");
    assert!(c.cmds(&["VLINKS", "vs", "e1", "FOO"]).is_error());
    assert!(
        c.cmds(&["VLINKS", "vs", "e1", "WITHSCORES", "x"])
            .is_error()
    );
    // A small set has no graph: one layer, its nearest elements.
    for i in 0..5 {
        let b = pack_f32(&seeded(i, 3));
        c.cmd(&[b"VADD", b"small", b"FP32", &b, format!("s{i}").as_bytes()]);
    }
    assert_eq!(vinfo_field(&mut c, "small", "index-type").as_str(), "FLAT");
    let one = c.cmds(&["VLINKS", "small", "s0"]);
    assert_eq!(one.as_array().len(), 1);
    assert_eq!(one.as_array()[0].as_array().len(), 4);
}

/// MEMORY USAGE counts the graph once the set has one.
#[test]
fn memory_usage_counts_the_graph() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let dim = 64;
    graph_set(&mut c, "vs", dim, &["Q8"]);
    let usage = c.cmds(&["MEMORY", "USAGE", "vs", "SAMPLES", "0"]).as_int();
    // The int8 vectors alone are 64 bytes an element; the graph's f32 copy
    // (256) and its links come on top.
    let per = usage as f64 / GRAPH_N as f64;
    assert!(per > 64.0 + 256.0 + 64.0, "{per} bytes an element");
}

/// Another `M` than the set's is refused, as Redis refuses it (16 when none
/// is given); the set's own is taken.
#[test]
fn vadd_refuses_another_m_than_the_sets() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let add = |c: &mut Conn, e: &str, extra: &[&str]| {
        let mut args = vec!["VADD", "m", "VALUES", "2", "1", "2", e];
        args.extend_from_slice(extra);
        c.cmds(&args)
    };
    assert_eq!(add(&mut c, "a", &["M", "32"]).as_int(), 1);
    assert_eq!(add(&mut c, "b", &["M", "32"]).as_int(), 1);
    for extra in [&[][..], &["M", "16"]] {
        match add(&mut c, "c", extra) {
            RespValue::Error(e) => {
                assert_eq!(e, "ERR asked M value mismatch with existing vector set")
            }
            other => panic!("{extra:?}: {other:?}"),
        }
    }
    assert_eq!(c.cmds(&["VCARD", "m"]).as_int(), 2);
    // A set made without M has 16: M 16 is its own.
    assert_eq!(
        c.cmds(&["VADD", "d", "VALUES", "2", "1", "2", "x"])
            .as_int(),
        1
    );
    assert_eq!(
        c.cmds(&["VADD", "d", "VALUES", "2", "1", "2", "y", "M", "16"])
            .as_int(),
        1
    );
}
