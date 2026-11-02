//! Integration tests for all TS.* (Time Series) commands.
//!
//! Each test spins up an isolated server on a random port.
//! Tests can run in parallel safely — no shared state.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

// ── RESP client ───────────────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        // Retry up to 40 times (2 seconds total) for the server to start
        for _ in 0..40 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                let reader = BufReader::new(s.try_clone().unwrap());
                return Conn { stream: s, reader };
            }
            thread::sleep(std::time::Duration::from_millis(50));
        }
        panic!("server on port {port} did not start in time");
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
                use std::io::Read;
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
            _ => panic!("expected string, got: {:?}", self),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("expected int, got: {:?}", self),
        }
    }
    fn as_f64(&self) -> f64 {
        self.as_str().parse().unwrap()
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("expected array, got: {:?}", self),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ── TS.CREATE ─────────────────────────────────────────────────────────────────

/// TS.CREATE basic — key created, TS.INFO returns correct metadata
#[test]
fn test_ts_create_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TS.CREATE", "ts:basic"]).is_ok());

    let info = c.cmds(&["TS.INFO", "ts:basic"]);
    let arr = info.as_array();
    // Find totalSamples field
    let pos = arr
        .iter()
        .position(|v| v.as_str() == "totalSamples")
        .unwrap();
    assert_eq!(arr[pos + 1].as_int(), 0);
}

/// TS.CREATE with all options
#[test]
fn test_ts_create_with_options() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&[
            "TS.CREATE",
            "ts:opts",
            "RETENTION",
            "86400000",
            "CHUNK",
            "SIZE",
            "2048",
            "DUPLICATE_POLICY",
            "LAST",
            "LABELS",
            "region",
            "us-east",
            "sensor",
            "temp"
        ])
        .is_ok()
    );

    let info = c.cmds(&["TS.INFO", "ts:opts"]);
    let arr = info.as_array();

    let ret_pos = arr
        .iter()
        .position(|v| v.as_str() == "retentionTime")
        .unwrap();
    assert_eq!(arr[ret_pos + 1].as_int(), 86400000);

    let dup_pos = arr
        .iter()
        .position(|v| v.as_str() == "duplicatePolicy")
        .unwrap();
    assert_eq!(arr[dup_pos + 1].as_str(), "last");
}

/// TS.CREATE on existing key returns error
#[test]
fn test_ts_create_duplicate_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TS.CREATE", "ts:dup"]).is_ok());
    assert!(c.cmds(&["TS.CREATE", "ts:dup"]).is_error());
}

/// TS.CREATE WRONGTYPE — key exists as string
#[test]
fn test_ts_create_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "ts:str", "hello"]);
    // SET creates a string key; TS.INFO on it should WRONGTYPE
    assert!(c.cmds(&["TS.INFO", "ts:str"]).is_error());
}

// ── TS.ALTER ─────────────────────────────────────────────────────────────────

/// TS.ALTER updates retention and duplicate policy
#[test]
fn test_ts_alter_retention_and_policy() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:alter"]);
    assert!(
        c.cmds(&[
            "TS.ALTER",
            "ts:alter",
            "RETENTION",
            "7200000",
            "DUPLICATE_POLICY",
            "MIN"
        ])
        .is_ok()
    );

    let info = c.cmds(&["TS.INFO", "ts:alter"]);
    let arr = info.as_array();
    let pos = arr
        .iter()
        .position(|v| v.as_str() == "retentionTime")
        .unwrap();
    assert_eq!(arr[pos + 1].as_int(), 7200000);

    let dp = arr
        .iter()
        .position(|v| v.as_str() == "duplicatePolicy")
        .unwrap();
    assert_eq!(arr[dp + 1].as_str(), "min");
}

/// TS.ALTER updates labels — old label removed from index, new one added
#[test]
fn test_ts_alter_labels() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:alter:labels", "LABELS", "env", "prod"]);

    // Query should find it
    let r = c.cmds(&["TS.QUERYINDEX", "env=prod"]);
    assert!(r.as_array().iter().any(|v| v.as_str() == "ts:alter:labels"));

    // Alter the labels
    assert!(
        c.cmds(&["TS.ALTER", "ts:alter:labels", "LABELS", "env", "staging"])
            .is_ok()
    );

    // Old label no longer matches
    let r2 = c.cmds(&["TS.QUERYINDEX", "env=prod"]);
    assert!(
        !r2.as_array()
            .iter()
            .any(|v| v.as_str() == "ts:alter:labels")
    );

    // New label matches
    let r3 = c.cmds(&["TS.QUERYINDEX", "env=staging"]);
    assert!(
        r3.as_array()
            .iter()
            .any(|v| v.as_str() == "ts:alter:labels")
    );
}

/// TS.ALTER on non-existent key
#[test]
fn test_ts_alter_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["TS.ALTER", "ts:nosuchkey", "RETENTION", "1000"])
            .is_error()
    );
}

// ── TS.ADD ────────────────────────────────────────────────────────────────────

/// TS.ADD basic — returns stored timestamp, TS.GET returns it
#[test]
fn test_ts_add_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:add"]);
    let ts = 1000u64;
    let r = c.cmds(&["TS.ADD", "ts:add", "1000", "42.5"]);
    assert_eq!(r.as_int(), 1000);

    let g = c.cmds(&["TS.GET", "ts:add"]);
    let arr = g.as_array();
    assert_eq!(arr[0].as_int(), 1000);
    assert_eq!(arr[1].as_f64(), 42.5);
    let _ = ts;
}

/// TS.ADD auto-creates the key if it doesn't exist
#[test]
fn test_ts_add_auto_create() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Key doesn't exist yet — ADD should create it
    let r = c.cmds(&["TS.ADD", "ts:autocreate", "2000", "99.0"]);
    assert_eq!(r.as_int(), 2000);

    let g = c.cmds(&["TS.GET", "ts:autocreate"]);
    assert_eq!(g.as_array()[1].as_f64(), 99.0);
}

/// TS.ADD with * timestamp (auto current time)
#[test]
fn test_ts_add_auto_timestamp() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let before = now_ms();
    let r = c.cmds(&["TS.ADD", "ts:autotime", "*", "1.0"]);
    let after = now_ms();

    let stored_ts = r.as_int() as u64;
    assert!(
        stored_ts >= before,
        "stored_ts {stored_ts} < before {before}"
    );
    assert!(stored_ts <= after, "stored_ts {stored_ts} > after {after}");
}

/// TS.ADD duplicate policy: BLOCK
#[test]
fn test_ts_add_dup_block() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:dup:block", "DUPLICATE_POLICY", "BLOCK"]);
    c.cmds(&["TS.ADD", "ts:dup:block", "1000", "10.0"]);
    // Second add at same timestamp should error
    assert!(
        c.cmds(&["TS.ADD", "ts:dup:block", "1000", "20.0"])
            .is_error()
    );
}

/// TS.ADD duplicate policy: FIRST — keeps original value
#[test]
fn test_ts_add_dup_first() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:dup:first", "DUPLICATE_POLICY", "FIRST"]);
    c.cmds(&["TS.ADD", "ts:dup:first", "1000", "10.0"]);
    c.cmds(&["TS.ADD", "ts:dup:first", "1000", "99.0"]); // should be ignored
    let g = c.cmds(&["TS.GET", "ts:dup:first"]);
    assert_eq!(g.as_array()[1].as_f64(), 10.0);
}

/// TS.ADD duplicate policy: LAST — keeps latest value
#[test]
fn test_ts_add_dup_last() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:dup:last", "DUPLICATE_POLICY", "LAST"]);
    c.cmds(&["TS.ADD", "ts:dup:last", "1000", "10.0"]);
    c.cmds(&["TS.ADD", "ts:dup:last", "1000", "99.0"]);
    let g = c.cmds(&["TS.GET", "ts:dup:last"]);
    assert_eq!(g.as_array()[1].as_f64(), 99.0);
}

/// TS.ADD duplicate policy: MIN — keeps smaller value
#[test]
fn test_ts_add_dup_min() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:dup:min", "DUPLICATE_POLICY", "MIN"]);
    c.cmds(&["TS.ADD", "ts:dup:min", "1000", "50.0"]);
    c.cmds(&["TS.ADD", "ts:dup:min", "1000", "30.0"]);
    c.cmds(&["TS.ADD", "ts:dup:min", "1000", "80.0"]);
    let g = c.cmds(&["TS.GET", "ts:dup:min"]);
    assert_eq!(g.as_array()[1].as_f64(), 30.0);
}

/// TS.ADD duplicate policy: MAX — keeps larger value
#[test]
fn test_ts_add_dup_max() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:dup:max", "DUPLICATE_POLICY", "MAX"]);
    c.cmds(&["TS.ADD", "ts:dup:max", "1000", "50.0"]);
    c.cmds(&["TS.ADD", "ts:dup:max", "1000", "30.0"]);
    c.cmds(&["TS.ADD", "ts:dup:max", "1000", "80.0"]);
    let g = c.cmds(&["TS.GET", "ts:dup:max"]);
    assert_eq!(g.as_array()[1].as_f64(), 80.0);
}

/// TS.ADD duplicate policy: SUM — accumulates values
#[test]
fn test_ts_add_dup_sum() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:dup:sum", "DUPLICATE_POLICY", "SUM"]);
    c.cmds(&["TS.ADD", "ts:dup:sum", "1000", "10.0"]);
    c.cmds(&["TS.ADD", "ts:dup:sum", "1000", "20.0"]);
    c.cmds(&["TS.ADD", "ts:dup:sum", "1000", "5.0"]);
    let g = c.cmds(&["TS.GET", "ts:dup:sum"]);
    assert_eq!(g.as_array()[1].as_f64(), 35.0);
}

// ── TS.MADD ───────────────────────────────────────────────────────────────────

/// TS.MADD adds to multiple series in one call
#[test]
fn test_ts_madd_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:madd:a"]);
    c.cmds(&["TS.CREATE", "ts:madd:b"]);

    let r = c.cmds(&[
        "TS.MADD",
        "ts:madd:a",
        "1000",
        "1.0",
        "ts:madd:b",
        "2000",
        "2.0",
        "ts:madd:a",
        "3000",
        "3.0",
    ]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_int(), 1000);
    assert_eq!(arr[1].as_int(), 2000);
    assert_eq!(arr[2].as_int(), 3000);

    // Verify via GET (returns last sample)
    let ga = c.cmds(&["TS.GET", "ts:madd:a"]);
    assert_eq!(ga.as_array()[0].as_int(), 3000);
    assert_eq!(ga.as_array()[1].as_f64(), 3.0);
}

/// TS.MADD adds to series that exist: a sample for a missing key is an error
/// for that sample, and the others are added.
#[test]
fn test_ts_madd_does_not_create_series() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["TS.CREATE", "ts:madd:there"]);

    let r = c.cmds(&[
        "TS.MADD",
        "ts:madd:missing",
        "100",
        "5.5",
        "ts:madd:there",
        "200",
        "6.6",
    ]);
    let replies = r.as_array();
    assert_eq!(replies.len(), 2);
    assert!(replies[0].is_error(), "{:?}", replies[0]);
    assert_eq!(replies[1].as_int(), 200);

    assert_eq!(c.cmds(&["EXISTS", "ts:madd:missing"]).as_int(), 0);
    assert_eq!(
        c.cmds(&["TS.GET", "ts:madd:there"]).as_array()[1].as_f64(),
        6.6
    );
}

// ── TS.INCRBY / TS.DECRBY ────────────────────────────────────────────────────

/// TS.INCRBY creates series, increments from 0
#[test]
fn test_ts_incrby_from_zero() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.INCRBY", "ts:inc", "5.0", "TIMESTAMP", "1000"]);
    c.cmds(&["TS.INCRBY", "ts:inc", "3.0", "TIMESTAMP", "2000"]);
    c.cmds(&["TS.INCRBY", "ts:inc", "2.0", "TIMESTAMP", "3000"]);

    let g = c.cmds(&["TS.GET", "ts:inc"]);
    // Last value should be 0 + 5 + 3 + 2 = 10
    assert_eq!(g.as_array()[1].as_f64(), 10.0);
}

/// TS.DECRBY decrements the running value
#[test]
fn test_ts_decrby() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.INCRBY", "ts:dec", "100.0", "TIMESTAMP", "1000"]);
    c.cmds(&["TS.DECRBY", "ts:dec", "30.0", "TIMESTAMP", "2000"]);
    c.cmds(&["TS.DECRBY", "ts:dec", "20.0", "TIMESTAMP", "3000"]);

    let g = c.cmds(&["TS.GET", "ts:dec"]);
    assert_eq!(g.as_array()[1].as_f64(), 50.0);
}

// ── TS.DEL ────────────────────────────────────────────────────────────────────

/// TS.DEL removes samples in range, returns count
#[test]
fn test_ts_del_range() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:del"]);
    for ts in [1000, 2000, 3000, 4000, 5000] {
        c.cmds(&["TS.ADD", "ts:del", &ts.to_string(), "1.0"]);
    }

    // Delete 2000..=4000 (3 samples)
    let r = c.cmds(&["TS.DEL", "ts:del", "2000", "4000"]);
    assert_eq!(r.as_int(), 3);

    // Range query should return only 1000 and 5000
    let range = c.cmds(&["TS.RANGE", "ts:del", "0", "9999"]);
    let arr = range.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_array()[0].as_int(), 1000);
    assert_eq!(arr[1].as_array()[0].as_int(), 5000);
}

/// TS.DEL on non-existent key returns error
#[test]
fn test_ts_del_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TS.DEL", "ts:nokey", "0", "9999"]).is_error());
}

// ── TS.GET ────────────────────────────────────────────────────────────────────

/// TS.GET on empty series returns empty array
#[test]
fn test_ts_get_empty_series() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:empty"]);
    let r = c.cmds(&["TS.GET", "ts:empty"]);
    assert_eq!(r.as_array().len(), 0);
}

/// TS.GET returns the latest sample even with many samples
#[test]
fn test_ts_get_returns_latest() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:getlatest"]);
    for (ts, val) in [(1000, 1.0), (2000, 2.0), (3000, 3.0), (500, 0.5)] {
        c.cmds(&["TS.ADD", "ts:getlatest", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&["TS.GET", "ts:getlatest"]);
    let arr = r.as_array();
    // 3000 is the highest timestamp
    assert_eq!(arr[0].as_int(), 3000);
    assert_eq!(arr[1].as_f64(), 3.0);
}

/// TS.GET on non-existent key returns nil
#[test]
fn test_ts_get_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TS.GET", "ts:nokey"]).is_nil());
}

// ── TS.RANGE / TS.REVRANGE ───────────────────────────────────────────────────

/// TS.RANGE returns samples in forward order
#[test]
fn test_ts_range_forward() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:range"]);
    for (ts, val) in [
        (1000, 10.0),
        (2000, 20.0),
        (3000, 30.0),
        (4000, 40.0),
        (5000, 50.0),
    ] {
        c.cmds(&["TS.ADD", "ts:range", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&["TS.RANGE", "ts:range", "2000", "4000"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_array()[0].as_int(), 2000);
    assert_eq!(arr[1].as_array()[0].as_int(), 3000);
    assert_eq!(arr[2].as_array()[0].as_int(), 4000);
}

/// TS.REVRANGE returns samples in reverse order
#[test]
fn test_ts_revrange() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:revrange"]);
    for (ts, val) in [(1000, 10.0), (2000, 20.0), (3000, 30.0)] {
        c.cmds(&["TS.ADD", "ts:revrange", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&["TS.REVRANGE", "ts:revrange", "0", "9999"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 3);
    // Reversed: 3000, 2000, 1000
    assert_eq!(arr[0].as_array()[0].as_int(), 3000);
    assert_eq!(arr[1].as_array()[0].as_int(), 2000);
    assert_eq!(arr[2].as_array()[0].as_int(), 1000);
}

/// TS.RANGE with COUNT limits results
#[test]
fn test_ts_range_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:count"]);
    for ts in [1000, 2000, 3000, 4000, 5000] {
        c.cmds(&["TS.ADD", "ts:count", &ts.to_string(), "1.0"]);
    }

    let r = c.cmds(&["TS.RANGE", "ts:count", "0", "9999", "COUNT", "3"]);
    assert_eq!(r.as_array().len(), 3);
}

/// TS.RANGE with FILTER_BY_VALUE
#[test]
fn test_ts_range_filter_by_value() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:filtval"]);
    for (ts, val) in [(1000, 5.0), (2000, 15.0), (3000, 25.0), (4000, 8.0)] {
        c.cmds(&["TS.ADD", "ts:filtval", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&[
        "TS.RANGE",
        "ts:filtval",
        "0",
        "9999",
        "FILTER_BY_VALUE",
        "10",
        "20",
    ]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].as_array()[1].as_f64(), 15.0);
}

/// TS.RANGE with AGGREGATION AVG
#[test]
fn test_ts_range_aggregation_avg() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:agg"]);
    // Two samples per 1000ms bucket
    for (ts, val) in [(100, 10.0), (200, 20.0), (1100, 30.0), (1200, 40.0)] {
        c.cmds(&["TS.ADD", "ts:agg", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&[
        "TS.RANGE",
        "ts:agg",
        "0",
        "9999",
        "AGGREGATION",
        "AVG",
        "1000",
    ]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    // Bucket 0: avg(10, 20) = 15
    assert_eq!(arr[0].as_array()[1].as_f64(), 15.0);
    // Bucket 1000: avg(30, 40) = 35
    assert_eq!(arr[1].as_array()[1].as_f64(), 35.0);
}

/// TS.RANGE with AGGREGATION SUM
#[test]
fn test_ts_range_aggregation_sum() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:agg:sum"]);
    for (ts, val) in [(100, 10.0), (200, 20.0), (1100, 5.0), (1200, 5.0)] {
        c.cmds(&["TS.ADD", "ts:agg:sum", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&[
        "TS.RANGE",
        "ts:agg:sum",
        "0",
        "9999",
        "AGGREGATION",
        "SUM",
        "1000",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_array()[1].as_f64(), 30.0);
    assert_eq!(arr[1].as_array()[1].as_f64(), 10.0);
}

/// TS.RANGE with AGGREGATION COUNT
#[test]
fn test_ts_range_aggregation_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:agg:count"]);
    for ts in [100, 200, 300, 1100, 1200] {
        c.cmds(&["TS.ADD", "ts:agg:count", &ts.to_string(), "1.0"]);
    }

    let r = c.cmds(&[
        "TS.RANGE",
        "ts:agg:count",
        "0",
        "9999",
        "AGGREGATION",
        "COUNT",
        "1000",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_array()[1].as_f64(), 3.0); // 100, 200, 300
    assert_eq!(arr[1].as_array()[1].as_f64(), 2.0); // 1100, 1200
}

/// TS.RANGE with AGGREGATION MIN and MAX
#[test]
fn test_ts_range_aggregation_min_max() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:agg:minmax"]);
    for (ts, val) in [(100, 5.0), (200, 15.0), (300, 10.0)] {
        c.cmds(&["TS.ADD", "ts:agg:minmax", &ts.to_string(), &val.to_string()]);
    }

    let min_r = c.cmds(&[
        "TS.RANGE",
        "ts:agg:minmax",
        "0",
        "9999",
        "AGGREGATION",
        "MIN",
        "1000",
    ]);
    assert_eq!(min_r.as_array()[0].as_array()[1].as_f64(), 5.0);

    let max_r = c.cmds(&[
        "TS.RANGE",
        "ts:agg:minmax",
        "0",
        "9999",
        "AGGREGATION",
        "MAX",
        "1000",
    ]);
    assert_eq!(max_r.as_array()[0].as_array()[1].as_f64(), 15.0);
}

/// TS.RANGE on non-existent key returns error
#[test]
fn test_ts_range_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(
        c.cmds(&["TS.RANGE", "ts:nosuchkey", "0", "9999"])
            .is_error()
    );
}

// ── TS.MGET ───────────────────────────────────────────────────────────────────

/// TS.MGET returns latest sample from each matching series
#[test]
fn test_ts_mget_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:mget:a", "LABELS", "type", "sensor"]);
    c.cmds(&["TS.CREATE", "ts:mget:b", "LABELS", "type", "sensor"]);
    c.cmds(&["TS.CREATE", "ts:mget:c", "LABELS", "type", "other"]);

    c.cmds(&["TS.ADD", "ts:mget:a", "1000", "10.0"]);
    c.cmds(&["TS.ADD", "ts:mget:b", "2000", "20.0"]);
    c.cmds(&["TS.ADD", "ts:mget:c", "3000", "30.0"]);

    let r = c.cmds(&["TS.MGET", "FILTER", "type=sensor"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);

    // Each entry: [key, labels, [ts, val]]
    let keys: Vec<&str> = arr.iter().map(|e| e.as_array()[0].as_str()).collect();
    assert!(keys.contains(&"ts:mget:a"));
    assert!(keys.contains(&"ts:mget:b"));
    assert!(!keys.contains(&"ts:mget:c"));
}

/// TS.MGET WITHLABELS includes label pairs
#[test]
fn test_ts_mget_withlabels() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:mget:wl", "LABELS", "region", "eu"]);
    c.cmds(&["TS.ADD", "ts:mget:wl", "1000", "5.0"]);

    let r = c.cmds(&["TS.MGET", "WITHLABELS", "FILTER", "region=eu"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 1);
    // labels array should not be empty
    let labels = arr[0].as_array()[1].as_array();
    assert!(!labels.is_empty());
    // find region=eu label pair
    let found = labels.iter().any(|pair| {
        let p = pair.as_array();
        p[0].as_str() == "region" && p[1].as_str() == "eu"
    });
    assert!(found);
}

/// TS.MGET without FILTER returns error
#[test]
fn test_ts_mget_no_filter() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TS.MGET"]).is_error());
}

// ── TS.MRANGE / TS.MREVRANGE ─────────────────────────────────────────────────

/// TS.MRANGE queries range across multiple series
#[test]
fn test_ts_mrange_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:mr:a", "LABELS", "group", "x"]);
    c.cmds(&["TS.CREATE", "ts:mr:b", "LABELS", "group", "x"]);
    c.cmds(&["TS.CREATE", "ts:mr:c", "LABELS", "group", "y"]);

    for ts in [1000u64, 2000, 3000] {
        c.cmds(&["TS.ADD", "ts:mr:a", &ts.to_string(), "1.0"]);
        c.cmds(&["TS.ADD", "ts:mr:b", &ts.to_string(), "2.0"]);
        c.cmds(&["TS.ADD", "ts:mr:c", &ts.to_string(), "3.0"]);
    }

    let r = c.cmds(&["TS.MRANGE", "0", "9999", "FILTER", "group=x"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);

    for entry in arr {
        let ea = entry.as_array();
        let key = ea[0].as_str();
        assert!(key == "ts:mr:a" || key == "ts:mr:b");
        // Each series should have 3 samples
        assert_eq!(ea[2].as_array().len(), 3);
    }
}

/// TS.MREVRANGE returns data in reverse order per series
#[test]
fn test_ts_mrevrange() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:mrr:a", "LABELS", "cat", "rev"]);
    for ts in [1000u64, 2000, 3000] {
        c.cmds(&["TS.ADD", "ts:mrr:a", &ts.to_string(), "1.0"]);
    }

    let r = c.cmds(&["TS.MREVRANGE", "0", "9999", "FILTER", "cat=rev"]);
    let arr = r.as_array();
    let samples = arr[0].as_array()[2].as_array();
    // Should be in reverse: 3000, 2000, 1000
    assert_eq!(samples[0].as_array()[0].as_int(), 3000);
    assert_eq!(samples[2].as_array()[0].as_int(), 1000);
}

/// TS.MRANGE with COUNT limits per-series results
#[test]
fn test_ts_mrange_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:mrc", "LABELS", "lim", "yes"]);
    for ts in [1000u64, 2000, 3000, 4000, 5000] {
        c.cmds(&["TS.ADD", "ts:mrc", &ts.to_string(), "1.0"]);
    }

    let r = c.cmds(&["TS.MRANGE", "0", "9999", "COUNT", "2", "FILTER", "lim=yes"]);
    let samples = r.as_array()[0].as_array()[2].as_array();
    assert_eq!(samples.len(), 2);
}

/// TS.MRANGE GROUPBY aggregates across series sharing a label value
#[test]
fn test_ts_mrange_groupby() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:gb:1", "LABELS", "zone", "east"]);
    c.cmds(&["TS.CREATE", "ts:gb:2", "LABELS", "zone", "east"]);
    c.cmds(&["TS.CREATE", "ts:gb:3", "LABELS", "zone", "west"]);

    c.cmds(&["TS.ADD", "ts:gb:1", "1000", "10.0"]);
    c.cmds(&["TS.ADD", "ts:gb:2", "1000", "20.0"]);
    c.cmds(&["TS.ADD", "ts:gb:3", "1000", "50.0"]);

    let r = c.cmds(&[
        "TS.MRANGE",
        "0",
        "9999",
        "FILTER",
        "zone=(east,west)",
        "GROUPBY",
        "zone",
        "REDUCE",
        "SUM",
    ]);
    let arr = r.as_array();

    // Should return 2 groups: east (10+20=30) and west (50)
    assert_eq!(arr.len(), 2);
    let east = arr
        .iter()
        .find(|e| e.as_array()[0].as_str().contains("east"))
        .unwrap();
    let samples = east.as_array()[2].as_array();
    assert_eq!(samples[0].as_array()[1].as_f64(), 30.0);
}

// ── TS.INFO ───────────────────────────────────────────────────────────────────

/// TS.INFO returns all expected fields
#[test]
fn test_ts_info_full() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "TS.CREATE",
        "ts:info",
        "RETENTION",
        "5000",
        "LABELS",
        "host",
        "srv1",
    ]);
    c.cmds(&["TS.ADD", "ts:info", "1000", "1.0"]);
    c.cmds(&["TS.ADD", "ts:info", "2000", "2.0"]);

    let info = c.cmds(&["TS.INFO", "ts:info"]);
    let arr = info.as_array();

    // Helper: find a field by name, skipping non-string values safely
    let find_field = |name: &str| -> Option<usize> {
        arr.iter().position(|v| match v {
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap_or("") == name,
            RespValue::SimpleStr(s) => s.as_str() == name,
            _ => false,
        })
    };

    let expected_fields = [
        "totalSamples",
        "firstTimestamp",
        "lastTimestamp",
        "retentionTime",
        "chunkSize",
        "duplicatePolicy",
        "labels",
        "rules",
    ];
    for field in expected_fields {
        assert!(find_field(field).is_some(), "missing field: {field}");
    }

    let samples_pos = find_field("totalSamples").unwrap();
    assert_eq!(arr[samples_pos + 1].as_int(), 2);

    let first_pos = find_field("firstTimestamp").unwrap();
    assert_eq!(arr[first_pos + 1].as_int(), 1000);

    let last_pos = find_field("lastTimestamp").unwrap();
    assert_eq!(arr[last_pos + 1].as_int(), 2000);
}

/// TS.INFO on missing key returns error
#[test]
fn test_ts_info_missing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TS.INFO", "ts:no"]).is_error());
}

// ── TS.QUERYINDEX ─────────────────────────────────────────────────────────────

/// TS.QUERYINDEX finds keys by label equality
#[test]
fn test_ts_queryindex_eq() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:qi:1", "LABELS", "env", "prod", "dc", "us1"]);
    c.cmds(&["TS.CREATE", "ts:qi:2", "LABELS", "env", "prod", "dc", "us2"]);
    c.cmds(&["TS.CREATE", "ts:qi:3", "LABELS", "env", "dev", "dc", "us1"]);

    let r = c.cmds(&["TS.QUERYINDEX", "env=prod"]);
    let keys: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"ts:qi:1"));
    assert!(keys.contains(&"ts:qi:2"));
    assert!(!keys.contains(&"ts:qi:3"));
}

/// TS.QUERYINDEX with inequality filter
#[test]
fn test_ts_queryindex_ne() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:qi:ne:1", "LABELS", "env", "prod"]);
    c.cmds(&["TS.CREATE", "ts:qi:ne:2", "LABELS", "env", "dev"]);
    c.cmds(&["TS.CREATE", "ts:qi:ne:3", "LABELS", "env", "staging"]);

    let r = c.cmds(&["TS.QUERYINDEX", "env!=prod"]);
    let keys: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    // dev and staging should be returned, prod should not
    assert!(!keys.contains(&"ts:qi:ne:1"));
    assert!(keys.contains(&"ts:qi:ne:2"));
    assert!(keys.contains(&"ts:qi:ne:3"));
}

/// TS.QUERYINDEX with set filter l=(v1,v2)
#[test]
fn test_ts_queryindex_in_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:qi:set:a", "LABELS", "dc", "us1"]);
    c.cmds(&["TS.CREATE", "ts:qi:set:b", "LABELS", "dc", "us2"]);
    c.cmds(&["TS.CREATE", "ts:qi:set:c", "LABELS", "dc", "eu1"]);

    let r = c.cmds(&["TS.QUERYINDEX", "dc=(us1,us2)"]);
    let keys: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"ts:qi:set:a"));
    assert!(keys.contains(&"ts:qi:set:b"));
    assert!(!keys.contains(&"ts:qi:set:c"));
}

/// TS.QUERYINDEX without filter returns error
#[test]
fn test_ts_queryindex_no_filter() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["TS.QUERYINDEX"]).is_error());
}

// ── TS.CREATERULE / TS.DELETERULE ────────────────────────────────────────────

/// TS.CREATERULE creates a compaction rule; TS.DELETERULE removes it
#[test]
fn test_ts_createrule_deleterule() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:src"]);
    c.cmds(&["TS.CREATE", "ts:dst"]);

    assert!(
        c.cmds(&[
            "TS.CREATERULE",
            "ts:src",
            "ts:dst",
            "AGGREGATION",
            "AVG",
            "1000"
        ])
        .is_ok()
    );

    let find_field = |arr: &[RespValue], name: &str| -> Option<usize> {
        arr.iter().position(|v| match v {
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap_or("") == name,
            RespValue::SimpleStr(s) => s.as_str() == name,
            _ => false,
        })
    };

    let info = c.cmds(&["TS.INFO", "ts:src"]);
    let arr = info.as_array();
    let rules_pos = find_field(arr, "rules").expect("rules field not found");
    assert!(
        !arr[rules_pos + 1].as_array().is_empty(),
        "rules should be non-empty after CREATERULE"
    );

    assert!(c.cmds(&["TS.DELETERULE", "ts:src", "ts:dst"]).is_ok());

    let info2 = c.cmds(&["TS.INFO", "ts:src"]);
    let arr2 = info2.as_array();
    let rules_pos2 = find_field(arr2, "rules").expect("rules field not found after delete");
    assert!(
        arr2[rules_pos2 + 1].as_array().is_empty(),
        "rules should be empty after DELETERULE"
    );
}

/// As RedisTimeSeries: a compaction gets a bucket once it has closed, and
/// the bucket still open is reported only with `LATEST`. It was written as
/// samples came, and `LATEST` changed nothing (#128). The compaction knows
/// its source (`sourceKey`), across a `DUMP` and `RESTORE` too.
#[test]
fn test_ts_compaction_writes_closed_buckets_and_latest_adds_the_open_one() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["TS.CREATE", "ob:src"]);
    c.cmds(&["TS.CREATE", "ob:dst"]);
    assert!(
        c.cmds(&[
            "TS.CREATERULE",
            "ob:src",
            "ob:dst",
            "AGGREGATION",
            "sum",
            "10000"
        ])
        .is_ok()
    );
    c.cmds(&["TS.ADD", "ob:src", "1000", "1"]);
    c.cmds(&["TS.ADD", "ob:src", "2000", "1"]);

    let range = |c: &mut Conn, extra: &[&str]| -> Vec<(i64, String)> {
        let mut args = vec!["TS.RANGE", "ob:dst", "-", "+"];
        args.extend_from_slice(extra);
        c.cmds(&args)
            .as_array()
            .iter()
            .map(|row| {
                let row = row.as_array();
                (row[0].as_int(), row[1].as_str().to_string())
            })
            .collect()
    };
    assert!(range(&mut c, &[]).is_empty(), "the open bucket was written");
    assert_eq!(range(&mut c, &["LATEST"]), [(0, "2".to_string())]);
    let got = c.cmds(&["TS.GET", "ob:dst", "LATEST"]);
    assert_eq!(got.as_array()[1].as_str(), "2");
    assert!(c.cmds(&["TS.GET", "ob:dst"]).as_array().is_empty());

    // The first sample of the next bucket closes this one.
    c.cmds(&["TS.ADD", "ob:src", "12000", "5"]);
    assert_eq!(range(&mut c, &[]), [(0, "2".to_string())]);
    assert_eq!(
        range(&mut c, &["LATEST"]),
        [(0, "2".to_string()), (10000, "5".to_string())]
    );
    // A sample added to a closed bucket updates it.
    c.cmds(&["TS.ADD", "ob:src", "3000", "1"]);
    assert_eq!(range(&mut c, &[]), [(0, "3".to_string())]);

    let source_key = |c: &mut Conn, key: &str| {
        let info = c.cmds(&["TS.INFO", key]);
        let arr = info.as_array();
        let at = arr
            .iter()
            .position(|v| *v == RespValue::Bulk(b"sourceKey".to_vec()))
            .expect("sourceKey");
        match &arr[at + 1] {
            RespValue::Bulk(b) => Some(String::from_utf8(b.clone()).unwrap()),
            _ => None,
        }
    };
    assert_eq!(source_key(&mut c, "ob:dst").as_deref(), Some("ob:src"));
    assert_eq!(source_key(&mut c, "ob:src"), None);
    // One source to a compaction.
    c.cmds(&["TS.CREATE", "ob:other"]);
    assert!(
        c.cmds(&[
            "TS.CREATERULE",
            "ob:other",
            "ob:dst",
            "AGGREGATION",
            "sum",
            "10000"
        ])
        .is_error()
    );

    // Kept by the snapshot format.
    let dump = c.cmd(&[b"DUMP", b"ob:dst"]);
    let RespValue::Bulk(payload) = dump else {
        panic!("{dump:?}")
    };
    assert!(c.cmd(&[b"RESTORE", b"ob:copy", b"0", &payload]).is_ok());
    assert_eq!(source_key(&mut c, "ob:copy").as_deref(), Some("ob:src"));

    assert!(c.cmds(&["TS.DELETERULE", "ob:src", "ob:dst"]).is_ok());
    assert_eq!(source_key(&mut c, "ob:dst"), None);
}

/// TS.CREATERULE duplicate rule rejected
#[test]
fn test_ts_createrule_duplicate() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:cr:src"]);
    c.cmds(&["TS.CREATE", "ts:cr:dst"]);
    c.cmds(&[
        "TS.CREATERULE",
        "ts:cr:src",
        "ts:cr:dst",
        "AGGREGATION",
        "SUM",
        "1000",
    ]);
    assert!(
        c.cmds(&[
            "TS.CREATERULE",
            "ts:cr:src",
            "ts:cr:dst",
            "AGGREGATION",
            "AVG",
            "1000"
        ])
        .is_error()
    );
}

/// TS.CREATERULE propagates samples to destination on TS.ADD
#[test]
fn test_ts_createrule_propagation() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:prop:src"]);
    c.cmds(&["TS.CREATE", "ts:prop:dst"]);
    c.cmds(&[
        "TS.CREATERULE",
        "ts:prop:src",
        "ts:prop:dst",
        "AGGREGATION",
        "SUM",
        "1000",
    ]);

    // Add samples to source
    c.cmds(&["TS.ADD", "ts:prop:src", "100", "10.0"]);
    c.cmds(&["TS.ADD", "ts:prop:src", "200", "20.0"]);
    c.cmds(&["TS.ADD", "ts:prop:src", "1100", "5.0"]);

    // Destination should have compacted values
    let dst_range = c.cmds(&["TS.RANGE", "ts:prop:dst", "0", "9999"]);
    let arr = dst_range.as_array();
    assert!(!arr.is_empty(), "destination should have compacted samples");
}

/// TS.DELETERULE on missing rule returns error
#[test]
fn test_ts_deleterule_missing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:dr:src"]);
    c.cmds(&["TS.CREATE", "ts:dr:dst"]);
    assert!(
        c.cmds(&["TS.DELETERULE", "ts:dr:src", "ts:dr:dst"])
            .is_error()
    );
}

// ── Retention ─────────────────────────────────────────────────────────────────

/// Retention trims old samples — after retention period they should not be returned
#[test]
fn test_ts_retention_trims_old_samples() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Retention = 1000ms
    c.cmds(&["TS.CREATE", "ts:ret", "RETENTION", "1000"]);

    // Add a sample at ts=1000
    c.cmds(&["TS.ADD", "ts:ret", "1000", "old"]);

    // Add a sample at ts=3000 — this should cause ts=1000 to fall outside retention
    // (3000 - 1000 = 2000 > 1000ms retention, so ts=1000 should be trimmed)
    c.cmds(&["TS.ADD", "ts:ret", "3000", "new"]);

    let range = c.cmds(&["TS.RANGE", "ts:ret", "0", "9999"]);
    let arr = range.as_array();

    // ts=1000 should be gone (outside 1000ms retention window from ts=3000)
    assert!(
        !arr.iter().any(|s| s.as_array()[0].as_int() == 1000),
        "old sample at ts=1000 should have been trimmed by retention"
    );
}

// ── Multiple aggregation functions ───────────────────────────────────────────

/// RANGE — FIRST aggregation returns first sample in each bucket
#[test]
fn test_ts_range_agg_first_last() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:fl"]);
    for (ts, val) in [(100, 1.0f64), (200, 2.0), (300, 3.0)] {
        c.cmds(&["TS.ADD", "ts:fl", &ts.to_string(), &val.to_string()]);
    }

    let first_r = c.cmds(&[
        "TS.RANGE",
        "ts:fl",
        "0",
        "9999",
        "AGGREGATION",
        "FIRST",
        "1000",
    ]);
    assert_eq!(first_r.as_array()[0].as_array()[1].as_f64(), 1.0);

    let last_r = c.cmds(&[
        "TS.RANGE",
        "ts:fl",
        "0",
        "9999",
        "AGGREGATION",
        "LAST",
        "1000",
    ]);
    assert_eq!(last_r.as_array()[0].as_array()[1].as_f64(), 3.0);
}

/// RANGE — RANGE aggregation returns max - min in each bucket
#[test]
fn test_ts_range_agg_range() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:rng"]);
    for (ts, val) in [(100, 5.0f64), (200, 15.0), (300, 8.0)] {
        c.cmds(&["TS.ADD", "ts:rng", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&[
        "TS.RANGE",
        "ts:rng",
        "0",
        "9999",
        "AGGREGATION",
        "RANGE",
        "1000",
    ]);
    // max=15, min=5, range=10
    assert_eq!(r.as_array()[0].as_array()[1].as_f64(), 10.0);
}

/// RANGE — STD.P aggregation (population std dev)
#[test]
fn test_ts_range_agg_std() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["TS.CREATE", "ts:std"]);
    // Values 2, 4, 4, 4, 5, 5, 7, 9 — known std.p = 2
    for (ts, val) in [
        (100, 2.0f64),
        (200, 4.0),
        (300, 4.0),
        (400, 4.0),
        (500, 5.0),
        (600, 5.0),
        (700, 7.0),
        (800, 9.0),
    ] {
        c.cmds(&["TS.ADD", "ts:std", &ts.to_string(), &val.to_string()]);
    }

    let r = c.cmds(&[
        "TS.RANGE",
        "ts:std",
        "0",
        "9999",
        "AGGREGATION",
        "STD.P",
        "10000",
    ]);
    let std_val = r.as_array()[0].as_array()[1].as_f64();
    // Should be 2.0 exactly for this dataset
    assert!(
        (std_val - 2.0).abs() < 0.01,
        "expected std.p ≈ 2.0, got {std_val}"
    );
}
