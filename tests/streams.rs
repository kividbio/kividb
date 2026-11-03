// Integration tests for Stream commands.
// Each test gets its own server on a fresh random port — fully isolated,
// runs in parallel with other test files.

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
            .set_read_timeout(Some(Duration::from_secs(10)))
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
            _ => panic!("unexpected RESP line: {:?}", line),
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
    fn is_nil(&self) -> bool {
        matches!(self, RespValue::Nil)
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
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

/// Poll `INFO clients` until `blocked_clients` reaches `want` (XREAD/XREADGROUP BLOCK
/// register in the wait table strictly before bumping this counter — see
/// `commands::streams`). Tests that need a deterministic "this client is now blocked"
/// signal should use this instead of a fixed sleep: a sleep only asserts a probability,
/// and under a loaded/virtualized runner it can lose the race (the producer thread's
/// XADD firing before the blocking XREAD has actually registered).
fn wait_for_blocked_clients(port: u16, want: i64) {
    let mut c = Conn::connect(port);
    for _ in 0..100 {
        let info = c.cmds(&["INFO", "clients"]);
        let text = info.as_str();
        if let Some(line) = text.lines().find(|l| l.starts_with("blocked_clients:")) {
            let n: i64 = line["blocked_clients:".len()..].trim().parse().unwrap_or(0);
            if n >= want {
                return;
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for blocked_clients >= {want} on port {port}");
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract field-value pairs from an entry's field array.
/// Entry format: ["id", ["field", "value", ...]]
fn entry_fields(entry: &RespValue) -> Vec<(&str, &str)> {
    let arr = entry.as_array();
    let fields = arr[1].as_array();
    fields
        .chunks(2)
        .map(|pair| (pair[0].as_str(), pair[1].as_str()))
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════════
// XADD / XLEN
// ═══════════════════════════════════════════════════════════════════════════════

// XADD key * field value [field value ...]  → "ms-seq" ID string
// XLEN key                                  → count
#[test]
fn test_xadd_xlen_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let id1 = c.cmds(&["XADD", "s", "*", "name", "alice", "age", "30"]);
    assert!(id1.as_str().contains('-'), "ID should be ms-seq format");

    let id2 = c.cmds(&["XADD", "s", "*", "name", "bob"]);
    assert!(id2.as_str().contains('-'));

    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 2);
    assert_eq!(c.cmds(&["XLEN", "nosuch"]).as_int(), 0);
}

// Explicit ID
#[test]
fn test_xadd_explicit_id() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["XADD", "s", "1000-0", "f", "v"]);
    assert_eq!(r.as_str(), "1000-0");

    // ID must be greater than last
    let r2 = c.cmds(&["XADD", "s", "999-0", "f", "v"]);
    assert!(r2.is_error());
}

// Auto-seq: "ms-*" fills in sequence within the millisecond
#[test]
fn test_xadd_auto_seq() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r1 = c.cmds(&["XADD", "s", "5000-*", "f", "v"]);
    assert_eq!(r1.as_str(), "5000-0");

    let r2 = c.cmds(&["XADD", "s", "5000-*", "f", "v"]);
    assert_eq!(r2.as_str(), "5000-1");
}

// NOMKSTREAM: don't create if key doesn't exist
#[test]
fn test_xadd_nomkstream() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["XADD", "s", "NOMKSTREAM", "*", "f", "v"]).is_nil());
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);

    // After creating the stream, NOMKSTREAM should work
    c.cmds(&["XADD", "s", "*", "f", "v"]);
    assert!(
        !c.cmds(&["XADD", "s", "NOMKSTREAM", "*", "f2", "v2"])
            .is_nil()
    );
}

// MAXLEN trimming
#[test]
fn test_xadd_maxlen() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for i in 0..10 {
        c.cmds(&["XADD", "s", "MAXLEN", "5", "*", "i", &i.to_string()]);
    }
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 5);
}

// MINID trimming
#[test]
fn test_xadd_minid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1000-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2000-0", "f", "v"]);
    c.cmds(&["XADD", "s", "MINID", "2000", "3000-0", "f", "v"]);
    // Entries with id < 2000 should be trimmed
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XRANGE / XREVRANGE
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xrange_xrevrange() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "name", "alice"]);
    c.cmds(&["XADD", "s", "2-0", "name", "bob"]);
    c.cmds(&["XADD", "s", "3-0", "name", "carol"]);

    // Full range
    let r = c.cmds(&["XRANGE", "s", "-", "+"]);
    let entries = r.as_array();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].as_array()[0].as_str(), "1-0");
    assert_eq!(entry_fields(&entries[0]), vec![("name", "alice")]);

    // Bounded range
    let r2 = c.cmds(&["XRANGE", "s", "2-0", "3-0"]);
    assert_eq!(r2.as_array().len(), 2);

    // COUNT limit
    let r3 = c.cmds(&["XRANGE", "s", "-", "+", "COUNT", "2"]);
    assert_eq!(r3.as_array().len(), 2);
    assert_eq!(r3.as_array()[0].as_array()[0].as_str(), "1-0");

    // XREVRANGE: newest first
    let r4 = c.cmds(&["XREVRANGE", "s", "+", "-"]);
    let entries4 = r4.as_array();
    assert_eq!(entries4.len(), 3);
    assert_eq!(entries4[0].as_array()[0].as_str(), "3-0");
    assert_eq!(entries4[2].as_array()[0].as_str(), "1-0");

    // XREVRANGE with COUNT
    let r5 = c.cmds(&["XREVRANGE", "s", "+", "-", "COUNT", "1"]);
    assert_eq!(r5.as_array()[0].as_array()[0].as_str(), "3-0");

    // Missing key
    assert_eq!(c.cmds(&["XRANGE", "nosuch", "-", "+"]).as_array().len(), 0);
}

// Bare ms ID expands correctly as range bound
#[test]
fn test_xrange_bare_ms_bound() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1000-0", "f", "v"]);
    c.cmds(&["XADD", "s", "1000-1", "f", "v"]);
    c.cmds(&["XADD", "s", "2000-0", "f", "v"]);

    // "1000" as lo expands to 1000-0
    let r = c.cmds(&["XRANGE", "s", "1000", "1000"]);
    assert_eq!(r.as_array().len(), 2); // 1000-0 and 1000-1
}

// ═══════════════════════════════════════════════════════════════════════════════
// XREAD
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xread_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v1"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v2"]);
    c.cmds(&["XADD", "s", "3-0", "f", "v3"]);

    // Read all entries after 0-0
    let r = c.cmds(&["XREAD", "STREAMS", "s", "0-0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_array()[0].as_str(), "s");
    let entries = arr[0].as_array()[1].as_array();
    assert_eq!(entries.len(), 3);

    // Read only after 1-0
    let r2 = c.cmds(&["XREAD", "STREAMS", "s", "1-0"]);
    let entries2 = r2.as_array()[0].as_array()[1].as_array();
    assert_eq!(entries2.len(), 2);

    // COUNT limit
    let r3 = c.cmds(&["XREAD", "COUNT", "1", "STREAMS", "s", "0-0"]);
    let entries3 = r3.as_array()[0].as_array()[1].as_array();
    assert_eq!(entries3.len(), 1);
}

// XREAD on multiple streams
#[test]
fn test_xread_multi_stream() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s1", "1-0", "f", "v1"]);
    c.cmds(&["XADD", "s2", "1-0", "f", "v2"]);

    let r = c.cmds(&["XREAD", "STREAMS", "s1", "s2", "0-0", "0-0"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);

    let stream_names: Vec<&str> = arr.iter().map(|s| s.as_array()[0].as_str()).collect();
    assert!(stream_names.contains(&"s1"));
    assert!(stream_names.contains(&"s2"));
}

// XREAD returns nil if no entries after the given ID
#[test]
fn test_xread_no_new_entries() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);

    // Read after the last ID — nothing new
    assert!(c.cmds(&["XREAD", "STREAMS", "s", "1-0"]).is_nil());
}

// XREAD BLOCK: wakeup path — XADD fires after block starts
#[test]
fn test_xread_block_wakeup() {
    let port = free_port();
    start_server(port);

    let mut c = Conn::connect(port);
    c.stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    thread::spawn(move || {
        wait_for_blocked_clients(port, 1);
        let mut pusher = Conn::connect(port);
        pusher.cmds(&["XADD", "wake", "*", "msg", "hello"]);
    });

    let r = c.cmds(&["XREAD", "BLOCK", "3000", "STREAMS", "wake", "$"]);
    assert!(!r.is_nil());
    let arr = r.as_array();
    assert_eq!(arr[0].as_array()[0].as_str(), "wake");
    let entries = arr[0].as_array()[1].as_array();
    assert_eq!(entry_fields(&entries[0]), vec![("msg", "hello")]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XDEL / XTRIM
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xdel() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v1"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v2"]);
    c.cmds(&["XADD", "s", "3-0", "f", "v3"]);

    assert_eq!(c.cmds(&["XDEL", "s", "2-0"]).as_int(), 1);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 2);
    // Non-existent ID
    assert_eq!(c.cmds(&["XDEL", "s", "99-0"]).as_int(), 0);
    // Bulk delete
    assert_eq!(c.cmds(&["XDEL", "s", "1-0", "3-0"]).as_int(), 2);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
}

#[test]
fn test_xtrim_maxlen() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for i in 1..=5u64 {
        c.cmds(&["XADD", "s", &format!("{}-0", i), "f", "v"]);
    }
    assert_eq!(c.cmds(&["XTRIM", "s", "MAXLEN", "3"]).as_int(), 2);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 3);
    // Oldest entries removed
    assert_eq!(
        c.cmds(&["XRANGE", "s", "-", "+", "COUNT", "1"]).as_array()[0].as_array()[0].as_str(),
        "3-0"
    );
}

#[test]
fn test_xtrim_minid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1000-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2000-0", "f", "v"]);
    c.cmds(&["XADD", "s", "3000-0", "f", "v"]);

    // Trim entries with id < 2000-0
    assert_eq!(c.cmds(&["XTRIM", "s", "MINID", "2000-0"]).as_int(), 1);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XSETID
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xsetid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    assert!(c.cmds(&["XSETID", "s", "100-0"]).is_ok());

    // XADD with * should now start from 100-0
    let id = c.cmds(&["XADD", "s", "*", "f", "v"]);
    let ms: u64 = id.as_str().split('-').next().unwrap().parse().unwrap();
    assert!(ms >= 100);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XINFO
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xinfo_stream() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "name", "alice"]);
    c.cmds(&["XADD", "s", "2-0", "name", "bob"]);

    let r = c.cmds(&["XINFO", "STREAM", "s"]);
    let arr = r.as_array();
    // Response is flat key-value pairs
    let map: std::collections::HashMap<&str, &RespValue> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();

    assert_eq!(map["length"].as_int(), 2);
    assert_eq!(map["last-generated-id"].as_str(), "2-0");
    assert_eq!(map["entries-added"].as_int(), 2);
}

#[test]
fn test_xinfo_groups_consumers() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "consumer1",
        "STREAMS",
        "s",
        ">",
    ]);

    let r = c.cmds(&["XINFO", "GROUPS", "s"]);
    let groups = r.as_array();
    assert_eq!(groups.len(), 1);
    let map: std::collections::HashMap<&str, &RespValue> = groups[0]
        .as_array()
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();
    assert_eq!(map["name"].as_str(), "grp");
    assert_eq!(map["consumers"].as_int(), 1);
    assert_eq!(map["pending"].as_int(), 1);

    let r2 = c.cmds(&["XINFO", "CONSUMERS", "s", "grp"]);
    let consumers = r2.as_array();
    assert_eq!(consumers.len(), 1);
    let cmap: std::collections::HashMap<&str, &RespValue> = consumers[0]
        .as_array()
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();
    assert_eq!(cmap["name"].as_str(), "consumer1");
    assert_eq!(cmap["pending"].as_int(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XGROUP
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xgroup_create_destroy() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);

    assert!(c.cmds(&["XGROUP", "CREATE", "s", "grp1", "0"]).is_ok());
    // Duplicate group name → error
    assert!(c.cmds(&["XGROUP", "CREATE", "s", "grp1", "0"]).is_error());

    // Create with $ (start from latest)
    assert!(c.cmds(&["XGROUP", "CREATE", "s", "grp2", "$"]).is_ok());

    // DESTROY
    assert_eq!(c.cmds(&["XGROUP", "DESTROY", "s", "grp1"]).as_int(), 1);
    assert_eq!(c.cmds(&["XGROUP", "DESTROY", "s", "nosuch"]).as_int(), 0);
}

// MKSTREAM: create key if it doesn't exist
#[test]
fn test_xgroup_mkstream() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Key doesn't exist yet — without MKSTREAM should error
    assert!(c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]).is_error());
    // With MKSTREAM creates the key
    assert!(
        c.cmds(&["XGROUP", "CREATE", "s", "grp", "0", "MKSTREAM"])
            .is_ok()
    );
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
}

#[test]
fn test_xgroup_setid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);

    assert!(c.cmds(&["XGROUP", "SETID", "s", "grp", "1-0"]).is_ok());

    // Only entries after 1-0 should be delivered
    let r = c.cmds(&["XREADGROUP", "GROUP", "grp", "con", "STREAMS", "s", ">"]);
    let entries = r.as_array()[0].as_array()[1].as_array();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].as_array()[0].as_str(), "2-0");
}

#[test]
fn test_xgroup_createconsumer_delconsumer() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "*", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);

    // Explicit consumer creation
    assert_eq!(
        c.cmds(&["XGROUP", "CREATECONSUMER", "s", "grp", "con1"])
            .as_int(),
        1
    );
    assert_eq!(
        c.cmds(&["XGROUP", "CREATECONSUMER", "s", "grp", "con1"])
            .as_int(),
        0
    ); // already exists

    // Read to create PEL entries
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    // Delete consumer — returns pending count
    assert_eq!(
        c.cmds(&["XGROUP", "DELCONSUMER", "s", "grp", "con1"])
            .as_int(),
        1
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// XREADGROUP / XACK
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xreadgroup_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "name", "alice"]);
    c.cmds(&["XADD", "s", "2-0", "name", "bob"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);

    // Read with ">" delivers new entries
    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "consumer1",
        "STREAMS",
        "s",
        ">",
    ]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_array()[0].as_str(), "s");
    let entries = arr[0].as_array()[1].as_array();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].as_array()[0].as_str(), "1-0");

    // Second read with ">" returns nothing (already delivered)
    assert!(
        c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "consumer1",
            "STREAMS",
            "s",
            ">"
        ])
        .is_nil()
    );
}

#[test]
fn test_xreadgroup_with_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for i in 1..=5u64 {
        c.cmds(&["XADD", "s", &format!("{}-0", i), "i", &i.to_string()]);
    }
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);

    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "con",
        "COUNT",
        "3",
        "STREAMS",
        "s",
        ">",
    ]);
    let entries = r.as_array()[0].as_array()[1].as_array();
    assert_eq!(entries.len(), 3);
}

// Re-delivering pending entries with ID "0"
#[test]
fn test_xreadgroup_redeliver_pending() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);

    // First read — entry goes into PEL
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con", "STREAMS", "s", ">"]);

    // Re-read pending with ID "0" — same entry returned
    let r2 = c.cmds(&["XREADGROUP", "GROUP", "grp", "con", "STREAMS", "s", "0"]);
    let entries = r2.as_array()[0].as_array()[1].as_array();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].as_array()[0].as_str(), "1-0");
}

// NOACK: delivers without adding to PEL
#[test]
fn test_xreadgroup_noack() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "con",
        "NOACK",
        "STREAMS",
        "s",
        ">",
    ]);

    // Pending should be empty
    let r = c.cmds(&["XPENDING", "s", "grp"]);
    assert_eq!(r.as_array()[0].as_int(), 0);
}

#[test]
fn test_xack() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con", "STREAMS", "s", ">"]);

    // Both entries pending
    assert_eq!(c.cmds(&["XPENDING", "s", "grp"]).as_array()[0].as_int(), 2);

    // Ack one
    assert_eq!(c.cmds(&["XACK", "s", "grp", "1-0"]).as_int(), 1);
    assert_eq!(c.cmds(&["XPENDING", "s", "grp"]).as_array()[0].as_int(), 1);

    // Ack non-existent
    assert_eq!(c.cmds(&["XACK", "s", "grp", "99-0"]).as_int(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XPENDING
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xpending_summary() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    // Summary form: [count, min-id, max-id, [[consumer, count], ...]]
    let r = c.cmds(&["XPENDING", "s", "grp"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 2);
    assert_eq!(arr[1].as_str(), "1-0");
    assert_eq!(arr[2].as_str(), "2-0");
    let per_consumer = arr[3].as_array();
    assert_eq!(per_consumer.len(), 1);
    assert_eq!(per_consumer[0].as_array()[0].as_str(), "con1");
    assert_eq!(per_consumer[0].as_array()[1].as_str(), "2");
}

#[test]
fn test_xpending_extended() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    // Extended form: each item is [id, consumer, idle-ms, delivery-count]
    let r = c.cmds(&["XPENDING", "s", "grp", "-", "+", "10"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_array()[0].as_str(), "1-0");
    assert_eq!(arr[0].as_array()[1].as_str(), "con1");
    assert_eq!(arr[0].as_array()[3].as_int(), 1); // delivery count

    // Filter by consumer
    let r2 = c.cmds(&["XPENDING", "s", "grp", "-", "+", "10", "con1"]);
    assert_eq!(r2.as_array().len(), 2);

    // Filter by non-existent consumer
    let r3 = c.cmds(&["XPENDING", "s", "grp", "-", "+", "10", "nobody"]);
    assert_eq!(r3.as_array().len(), 0);
}

#[test]
fn test_xpending_empty() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "*", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);

    // Nothing read yet → all counts zero
    let r = c.cmds(&["XPENDING", "s", "grp"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 0);
    assert!(arr[1].is_nil());
    assert!(arr[2].is_nil());
}

// ═══════════════════════════════════════════════════════════════════════════════
// XCLAIM / XAUTOCLAIM
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xclaim_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    // Claim with min-idle-time 0 (always eligible)
    let r = c.cmds(&["XCLAIM", "s", "grp", "con2", "0", "1-0"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].as_array()[0].as_str(), "1-0");

    // Verify con2 owns the PEL entry now
    let pending = c.cmds(&["XPENDING", "s", "grp", "-", "+", "10", "con2"]);
    assert_eq!(pending.as_array().len(), 1);
}

#[test]
fn test_xclaim_justid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    let r = c.cmds(&["XCLAIM", "s", "grp", "con2", "0", "1-0", "JUSTID"]);
    // JUSTID returns bulk string IDs, not full entries
    assert_eq!(r.as_array()[0].as_str(), "1-0");
}

#[test]
fn test_xclaim_min_idle_time() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    // min-idle-time = 60000ms (1 minute) — entry not old enough
    let r = c.cmds(&["XCLAIM", "s", "grp", "con2", "60000", "1-0"]);
    assert_eq!(r.as_array().len(), 0); // nothing claimed
}

#[test]
fn test_xautoclaim() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v1"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v2"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    // XAUTOCLAIM with min-idle 0 — claims everything
    let r = c.cmds(&["XAUTOCLAIM", "s", "grp", "con2", "0", "0-0"]);
    let arr = r.as_array();
    // Returns [next-start-id, entries, deleted-ids]
    assert_eq!(arr.len(), 3);
    let claimed = arr[1].as_array();
    assert_eq!(claimed.len(), 2);

    // Verify ownership transferred
    let pending = c.cmds(&["XPENDING", "s", "grp", "-", "+", "10", "con2"]);
    assert_eq!(pending.as_array().len(), 2);
}

#[test]
fn test_xautoclaim_justid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "con1", "STREAMS", "s", ">"]);

    let r = c.cmds(&["XAUTOCLAIM", "s", "grp", "con2", "0", "0-0", "JUSTID"]);
    assert_eq!(r.as_array()[1].as_array()[0].as_str(), "1-0");
}

// ═══════════════════════════════════════════════════════════════════════════════
// WRONGTYPE errors
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_stream_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "hello"]);
    assert!(c.cmds(&["XADD", "strkey", "*", "f", "v"]).is_error());
    assert!(c.cmds(&["XLEN", "strkey"]).is_error());
    assert!(c.cmds(&["XRANGE", "strkey", "-", "+"]).is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// TYPE command
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_type_stream() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "*", "f", "v"]);
    assert_eq!(c.cmds(&["TYPE", "s"]).as_str(), "stream");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Multi-consumer group isolation
// ═══════════════════════════════════════════════════════════════════════════════

// Two groups on the same stream each get independent delivery cursors
#[test]
fn test_two_groups_independent() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v1"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v2"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp1", "0"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp2", "0"]);

    // grp1 reads entry 1-0
    let r1 = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp1",
        "con",
        "COUNT",
        "1",
        "STREAMS",
        "s",
        ">",
    ]);
    let entries1 = r1.as_array()[0].as_array()[1].as_array();
    assert_eq!(entries1[0].as_array()[0].as_str(), "1-0");

    // grp2 still sees 1-0 — independent cursor
    let r2 = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp2",
        "con",
        "COUNT",
        "1",
        "STREAMS",
        "s",
        ">",
    ]);
    let entries2 = r2.as_array()[0].as_array()[1].as_array();
    assert_eq!(entries2[0].as_array()[0].as_str(), "1-0");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Edge cases
// ═══════════════════════════════════════════════════════════════════════════════

// XRANGE on empty stream
#[test]
fn test_xrange_empty_stream() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0", "MKSTREAM"]);
    assert_eq!(c.cmds(&["XRANGE", "s", "-", "+"]).as_array().len(), 0);
}

// XADD with multiple field-value pairs
#[test]
fn test_xadd_multi_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f1", "v1", "f2", "v2", "f3", "v3"]);
    let r = c.cmds(&["XRANGE", "s", "-", "+"]);
    let fields = entry_fields(&r.as_array()[0]);
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0], ("f1", "v1"));
    assert_eq!(fields[1], ("f2", "v2"));
    assert_eq!(fields[2], ("f3", "v3"));
}

// Concurrent XADD — IDs always strictly increasing
#[test]
fn test_xadd_concurrent_ids_monotonic() {
    let port = free_port();
    start_server(port);

    let handles: Vec<_> = (0..5)
        .map(|_| {
            thread::spawn(move || {
                let mut c = Conn::connect(port);
                c.cmds(&["XADD", "s", "*", "f", "v"]).as_str().to_string()
            })
        })
        .collect();

    let mut ids: Vec<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    ids.sort();
    // All IDs should be unique
    let unique: std::collections::HashSet<&str> = ids.iter().map(|s| s.as_str()).collect();
    assert_eq!(unique.len(), 5);
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. XREAD BLOCK — TOCTOU fix: no missed entries
// ═════════════════════════════════════════════════════════════════════════════

// Scenario: basic non-blocking XREAD
//   XADD stream * name alice
//   XREAD COUNT 1 STREAMS stream 0-0
//   → returns the entry
#[test]
fn test_xread_nonblocking_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let id = c.cmds(&["XADD", "stream", "*", "name", "alice"]);
    let id_str = id.as_str();

    let r = c.cmds(&["XREAD", "COUNT", "1", "STREAMS", "stream", "0-0"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 1); // one stream

    let stream_result = arr[0].as_array();
    assert_eq!(stream_result[0].as_str(), "stream");
    let entries = stream_result[1].as_array();
    assert_eq!(entries.len(), 1);

    let entry = entries[0].as_array();
    assert_eq!(entry[0].as_str(), id_str); // ID matches
    let fields = entry[1].as_array();
    assert_eq!(fields[0].as_str(), "name");
    assert_eq!(fields[1].as_str(), "alice");
}

// Scenario: XREAD with $ reads only new entries
//   XADD stream * k v        → existing entry
//   XREAD STREAMS stream $   → nil ($ = read only future entries)
#[test]
fn test_xread_dollar_id_no_existing() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "stream", "*", "k", "v"]);

    // $ means "only entries added after this command" — non-blocking returns nil
    let r = c.cmds(&["XREAD", "COUNT", "10", "STREAMS", "stream", "$"]);
    assert!(r.is_nil(), "XREAD $ on existing data should return nil");
}

// Scenario: XREAD BLOCK wakes when XADD fires
//   Thread 1: XREAD BLOCK 5000 STREAMS stream $  (blocks)
//   Thread 2: sleep 100ms, XADD stream * k v
//   Thread 1: should receive the entry within timeout
#[test]
fn test_xread_block_wakes_on_xadd() {
    let port = free_port();
    start_server(port);

    // Producer: waits 150ms then XADDs
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let mut producer = Conn::connect(port);
        producer.cmds(&["XADD", "blocking_stream", "*", "event", "fired"]);
    });

    // Consumer: blocks waiting for new entries
    let mut consumer = Conn::connect(port);
    // Use $ to only get entries added after this command
    let r = consumer.cmds(&["XREAD", "BLOCK", "5000", "STREAMS", "blocking_stream", "$"]);

    assert!(!r.is_nil(), "XREAD BLOCK should have received the entry");
    let arr = r.as_array();
    assert_eq!(arr.len(), 1);
    let stream_result = arr[0].as_array();
    assert_eq!(stream_result[0].as_str(), "blocking_stream");
    let entries = stream_result[1].as_array();
    assert_eq!(entries.len(), 1);
    let entry = entries[0].as_array();
    let fields = entry[1].as_array();
    assert_eq!(fields[0].as_str(), "event");
    assert_eq!(fields[1].as_str(), "fired");
}

// Scenario: XREAD BLOCK timeout returns nil
//   XREAD BLOCK 200 STREAMS emptystream $
//   → nil after ~200ms (nothing added)
#[test]
fn test_xread_block_timeout() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let start = std::time::Instant::now();
    let r = c.cmds(&["XREAD", "BLOCK", "200", "STREAMS", "emptystream", "$"]);
    let elapsed = start.elapsed();

    assert!(r.is_nil(), "timed out XREAD BLOCK should return nil");
    // Should have waited roughly 200ms, not returned immediately
    assert!(
        elapsed >= Duration::from_millis(150),
        "XREAD BLOCK returned too quickly: {elapsed:?}"
    );
}

// Scenario: TOCTOU fix — XADD fires between the non-blocking read and registration
//   This is the race the second-check fix closes.
//   We simulate it by pre-adding an entry with a known ID, then using that
//   ID as the cursor so XREAD thinks there's nothing new, but a concurrent
//   XADD actually beats the registration.
//
//   The test structure:
//   1. Pre-populate stream with entry A (id = last_id_before)
//   2. Consumer reads non-blocking → gets entry A
//   3. Consumer immediately calls XREAD BLOCK with the ID of A
//   4. Producer fires XADD with entry B *immediately* (simulating the race window)
//   5. Consumer should receive entry B, not miss it
#[test]
fn test_xread_block_toctou_no_missed_entry() {
    let port = free_port();
    start_server(port);

    let mut setup = Conn::connect(port);
    // Pre-populate
    let first_id = setup.cmds(&["XADD", "toctou_stream", "*", "seq", "1"]);
    let first_id_str = first_id.as_str().to_string();

    // Producer thread fires very quickly — simulates the race window
    let port_clone = port;
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10)); // tiny delay, within race window
        let mut producer = Conn::connect(port_clone);
        producer.cmds(&["XADD", "toctou_stream", "*", "seq", "2"]);
    });

    // Consumer blocks starting from first_id — so it's waiting for seq=2
    let mut consumer = Conn::connect(port);
    let r = consumer.cmds(&[
        "XREAD",
        "BLOCK",
        "3000",
        "STREAMS",
        "toctou_stream",
        first_id_str.as_str(),
    ]);

    assert!(
        !r.is_nil(),
        "XREAD BLOCK must not miss entry added in the TOCTOU window"
    );
    let arr = r.as_array();
    let stream_result = arr[0].as_array();
    let entries = stream_result[1].as_array();
    assert_eq!(entries.len(), 1);
    let entry = entries[0].as_array();
    let fields = entry[1].as_array();
    assert_eq!(fields[0].as_str(), "seq");
    assert_eq!(fields[1].as_str(), "2");
}

// Scenario: multiple consumers blocking on same stream — both wake on XADD
#[test]
fn test_xread_block_multiple_consumers() {
    let port = free_port();
    start_server(port);

    let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    // Spawn 3 consumers all blocking on the same stream
    for _ in 0..3 {
        let results = std::sync::Arc::clone(&results);
        thread::spawn(move || {
            let mut c = Conn::connect(port);
            let r = c.cmds(&["XREAD", "BLOCK", "5000", "STREAMS", "multi_stream", "$"]);
            let got = !r.is_nil();
            results.lock().unwrap().push(got);
        });
    }

    // Give consumers time to register
    thread::sleep(Duration::from_millis(100));

    // Producer fires once
    let mut producer = Conn::connect(port);
    producer.cmds(&["XADD", "multi_stream", "*", "k", "v"]);

    // Wait for consumers to wake and record results
    thread::sleep(Duration::from_millis(500));

    let r = results.lock().unwrap();
    // All 3 consumers should have woken and received the entry
    let received = r.iter().filter(|&&got| got).count();
    assert_eq!(
        received, 3,
        "all 3 blocked consumers should wake on XADD, got {received}/3"
    );
}

// Scenario: XREAD multiple streams — wakes when any of them gets XADD
#[test]
fn test_xread_block_multiple_streams() {
    let port = free_port();
    start_server(port);

    let mut c = Conn::connect(port);

    // Producer fires on stream2 (not stream1)
    thread::spawn(move || {
        wait_for_blocked_clients(port, 1);
        let mut p = Conn::connect(port);
        p.cmds(&["XADD", "ms_stream2", "*", "from", "stream2"]);
    });

    let r = c.cmds(&[
        "XREAD",
        "BLOCK",
        "3000",
        "STREAMS",
        "ms_stream1",
        "ms_stream2",
        "$",
        "$",
    ]);

    assert!(!r.is_nil());
    let arr = r.as_array();
    // Should return only stream2 (stream1 had no new entries)
    assert_eq!(arr.len(), 1);
    let stream_result = arr[0].as_array();
    assert_eq!(stream_result[0].as_str(), "ms_stream2");
}

// Scenario: XREAD COUNT limits entries returned
//   XADD stream * k1 v1
//   XADD stream * k2 v2
//   XADD stream * k3 v3
//   XREAD COUNT 2 STREAMS stream 0-0 → only first 2 entries
#[test]
fn test_xread_count_limits_entries() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "stream", "*", "k", "1"]);
    c.cmds(&["XADD", "stream", "*", "k", "2"]);
    c.cmds(&["XADD", "stream", "*", "k", "3"]);

    let r = c.cmds(&["XREAD", "COUNT", "2", "STREAMS", "stream", "0-0"]);
    let arr = r.as_array();
    let entries = arr[0].as_array()[1].as_array();
    assert_eq!(entries.len(), 2, "COUNT 2 should return exactly 2 entries");
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Set up: create stream with N entries, create group starting from 0-0.
/// Returns the IDs of the added entries.
fn setup_stream_with_group(c: &mut Conn, stream: &str, group: &str, n: usize) -> Vec<String> {
    let mut ids = Vec::new();
    for i in 0..n {
        let id = c
            .cmds(&["XADD", stream, "*", "seq", &i.to_string()])
            .as_str()
            .to_string();
        ids.push(id);
    }
    c.cmds(&["XGROUP", "CREATE", stream, group, "0-0"]);
    ids
}

/// Extract field value from a stream entry array.
/// Entry format: [id, [field, value, field, value, ...]]
fn entry_field(entry: &RespValue, field: &str) -> Option<String> {
    let arr = entry.as_array();
    let fields = arr[1].as_array();
    let mut i = 0;
    while i + 1 < fields.len() {
        if fields[i].as_str() == field {
            return Some(fields[i + 1].as_str().to_string());
        }
        i += 2;
    }
    None
}

/// Get entries from an XREADGROUP result for a specific stream.
/// Result format: [[stream_name, [[id, fields], ...]]]
fn get_entries<'a>(result: &'a RespValue, stream: &str) -> &'a [RespValue] {
    let arr = result.as_array();
    for item in arr {
        let pair = item.as_array();
        if pair[0].as_str() == stream {
            return pair[1].as_array();
        }
    }
    panic!("stream {stream} not found in result");
}

// ═════════════════════════════════════════════════════════════════════════════
// Non-blocking baseline — verifying the non-blocking path still works
// ═════════════════════════════════════════════════════════════════════════════

// Scenario: basic non-blocking read with ">"
//   XADD s * seq 0
//   XGROUP CREATE s grp 0-0
//   XREADGROUP GROUP grp c1 STREAMS s > → returns the entry, adds to PEL
#[test]
fn test_xreadgroup_nonblocking_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let ids = setup_stream_with_group(&mut c, "s_nb_basic", "grp", 1);

    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "STREAMS",
        "s_nb_basic",
        ">",
    ]);
    assert!(!r.is_nil());
    let entries = get_entries(&r, "s_nb_basic");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].as_array()[0].as_str(), ids[0]);
}

// Scenario: second XREADGROUP with ">" on same consumer returns nil (all delivered)
#[test]
fn test_xreadgroup_no_new_after_delivery() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    setup_stream_with_group(&mut c, "s_nonew", "grp", 2);

    // First read delivers both
    c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "STREAMS",
        "s_nonew",
        ">",
    ]);

    // Second read — nothing new for this group
    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "STREAMS",
        "s_nonew",
        ">",
    ]);
    assert!(r.is_nil(), "no new entries should return nil");
}

// Scenario: re-delivery with specific ID (not ">") returns pending entries
#[test]
fn test_xreadgroup_redelivery_pending() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    setup_stream_with_group(&mut c, "s_redel", "grp", 3);

    // Deliver all to c1
    c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "STREAMS",
        "s_redel",
        ">",
    ]);

    // Re-read pending from 0-0 — should get all 3 back
    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "STREAMS",
        "s_redel",
        "0-0",
    ]);
    assert!(!r.is_nil());
    let entries = get_entries(&r, "s_redel");
    assert_eq!(entries.len(), 3);
}

// Scenario: NOACK — entries delivered but NOT added to PEL
#[test]
fn test_xreadgroup_noack_no_pel_entry() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    setup_stream_with_group(&mut c, "s_noack", "grp", 2);

    // Deliver with NOACK
    c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "NOACK",
        "STREAMS",
        "s_noack",
        ">",
    ]);

    // XPENDING should show 0 pending (nothing in PEL)
    let pending = c.cmds(&["XPENDING", "s_noack", "grp", "-", "+", "10"]);
    assert!(
        pending.is_nil() || pending.as_array().is_empty(),
        "NOACK should not add entries to PEL"
    );
}

// Scenario: XACK removes from PEL
#[test]
fn test_xreadgroup_xack_clears_pel() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let ids = setup_stream_with_group(&mut c, "s_ack", "grp", 2);

    // Deliver both
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s_ack", ">"]);

    // ACK first entry
    let ack_result = c.cmds(&["XACK", "s_ack", "grp", &ids[0]]);
    assert_eq!(ack_result.as_int(), 1);

    // Re-read pending — only second entry should remain
    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "STREAMS",
        "s_ack",
        "0-0",
    ]);
    let entries = get_entries(&r, "s_ack");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].as_array()[0].as_str(), ids[1]);
}

// Scenario: COUNT limits entries delivered
#[test]
fn test_xreadgroup_count_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    setup_stream_with_group(&mut c, "s_count", "grp", 5);

    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "COUNT",
        "2",
        "STREAMS",
        "s_count",
        ">",
    ]);
    let entries = get_entries(&r, "s_count");
    assert_eq!(entries.len(), 2, "COUNT 2 should deliver exactly 2 entries");
}

// ═════════════════════════════════════════════════════════════════════════════
// BLOCK path — the limitation we fixed
// ═════════════════════════════════════════════════════════════════════════════

// Scenario: XREADGROUP BLOCK wakes when XADD fires
//   Create group at $ (only future entries)
//   Thread 1: XREADGROUP BLOCK 5000 STREAMS s > (blocks)
//   Thread 2: sleep 150ms, XADD s * k v
//   Thread 1: should receive the entry
#[test]
fn test_xreadgroup_block_wakes_on_xadd() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);

    // Create stream and group at $ (only future entries)
    setup.cmds(&["XADD", "s_blk_wake", "*", "k", "setup"]);
    setup.cmds(&["XGROUP", "CREATE", "s_blk_wake", "grp", "$"]);

    // Producer fires after 150ms
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let mut p = Conn::connect(port);
        p.cmds(&["XADD", "s_blk_wake", "*", "event", "fired"]);
    });

    let mut consumer = Conn::connect(port);
    let r = consumer.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "BLOCK",
        "5000",
        "STREAMS",
        "s_blk_wake",
        ">",
    ]);

    assert!(!r.is_nil(), "XREADGROUP BLOCK should receive the entry");
    let entries = get_entries(&r, "s_blk_wake");
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_field(&entries[0], "event").as_deref(), Some("fired"));
}

// Scenario: XREADGROUP BLOCK timeout returns nil
#[test]
fn test_xreadgroup_block_timeout() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s_blk_timeout", "*", "k", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s_blk_timeout", "grp", "$"]);

    let start = std::time::Instant::now();
    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "BLOCK",
        "200",
        "STREAMS",
        "s_blk_timeout",
        ">",
    ]);
    let elapsed = start.elapsed();

    assert!(r.is_nil(), "timeout XREADGROUP BLOCK should return nil");
    assert!(
        elapsed >= Duration::from_millis(150),
        "should have waited ~200ms, elapsed={elapsed:?}"
    );
}

// Scenario: TOCTOU — XADD fires between the first read and registration
//   Group created at $. XADD fires ~10ms after XREADGROUP starts.
//   The second-check must catch this and not block indefinitely.
#[test]
fn test_xreadgroup_block_toctou_no_missed_entry() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);

    setup.cmds(&["XADD", "s_toctou_grp", "*", "k", "setup"]);
    setup.cmds(&["XGROUP", "CREATE", "s_toctou_grp", "grp", "$"]);

    // Producer fires very quickly — within the TOCTOU window
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        let mut p = Conn::connect(port);
        p.cmds(&["XADD", "s_toctou_grp", "*", "seq", "toctou"]);
    });

    let mut consumer = Conn::connect(port);
    let r = consumer.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "BLOCK",
        "3000",
        "STREAMS",
        "s_toctou_grp",
        ">",
    ]);

    assert!(
        !r.is_nil(),
        "XREADGROUP BLOCK must not miss entry added in TOCTOU window"
    );
    let entries = get_entries(&r, "s_toctou_grp");
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_field(&entries[0], "seq").as_deref(), Some("toctou"));
}

// Scenario: multiple consumers blocked on same group — all wake on XADD
//   Two consumers block with different consumer names on the same group.
//   One XADD should deliver to BOTH (each gets the entry independently
//   because consumer groups fan-out: each consumer gets all new entries
//   for their own delivery tracking).
//
//   Note: unlike pub/sub, XREADGROUP delivers each new entry to one consumer
//   in a group. With two consumers both blocking with ">", the single XADD
//   entry is delivered to whichever consumer's request is processed first.
//   The other consumer wakes but finds nothing (entry already delivered to c1).
//   This is correct Redis behaviour.
#[test]
fn test_xreadgroup_block_two_consumers_one_entry() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);

    setup.cmds(&["XADD", "s_two_consumers", "*", "k", "setup"]);
    setup.cmds(&["XGROUP", "CREATE", "s_two_consumers", "grp", "$"]);

    let received = std::sync::Arc::new(std::sync::Mutex::new(0usize));

    // Consumer 1
    let received1 = std::sync::Arc::clone(&received);
    thread::spawn(move || {
        let mut c = Conn::connect(port);
        let r = c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "BLOCK",
            "3000",
            "STREAMS",
            "s_two_consumers",
            ">",
        ]);
        if !r.is_nil() {
            *received1.lock().unwrap() += 1;
        }
    });

    // Consumer 2
    let received2 = std::sync::Arc::clone(&received);
    thread::spawn(move || {
        let mut c = Conn::connect(port);
        let r = c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c2",
            "BLOCK",
            "3000",
            "STREAMS",
            "s_two_consumers",
            ">",
        ]);
        if !r.is_nil() {
            *received2.lock().unwrap() += 1;
        }
    });

    thread::sleep(Duration::from_millis(100));

    // One XADD — one entry in the stream
    let mut producer = Conn::connect(port);
    producer.cmds(&["XADD", "s_two_consumers", "*", "k", "v"]);

    thread::sleep(Duration::from_millis(500));

    let count = *received.lock().unwrap();
    // Exactly one consumer gets the entry (consumer group semantics)
    assert_eq!(
        count, 1,
        "exactly one consumer should receive the entry in a consumer group, got {count}"
    );
}

// Scenario: blocking delivery adds entry to PEL
//   Block until XADD, then verify PEL contains the delivered entry
#[test]
fn test_xreadgroup_block_adds_to_pel() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);

    setup.cmds(&["XADD", "s_blk_pel", "*", "k", "setup"]);
    setup.cmds(&["XGROUP", "CREATE", "s_blk_pel", "grp", "$"]);

    let delivered_id = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let delivered_id_clone = std::sync::Arc::clone(&delivered_id);

    thread::spawn(move || {
        let mut c = Conn::connect(port);
        let r = c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "BLOCK",
            "3000",
            "STREAMS",
            "s_blk_pel",
            ">",
        ]);
        if !r.is_nil() {
            let entries = get_entries(&r, "s_blk_pel");
            if !entries.is_empty() {
                let id = entries[0].as_array()[0].as_str().to_string();
                *delivered_id_clone.lock().unwrap() = id;
            }
        }
    });

    thread::sleep(Duration::from_millis(100));

    let mut producer = Conn::connect(port);
    producer.cmds(&["XADD", "s_blk_pel", "*", "k", "v"]);

    thread::sleep(Duration::from_millis(300));

    // Check PEL — should contain exactly one entry
    let pending = setup.cmds(&["XPENDING", "s_blk_pel", "grp", "-", "+", "10"]);
    let pel_entries = pending.as_array();
    assert_eq!(
        pel_entries.len(),
        1,
        "blocking delivery should add entry to PEL"
    );

    // PEL entry ID should match delivered ID
    let id_in_pel = pel_entries[0].as_array()[0].as_str();
    let delivered = delivered_id.lock().unwrap().clone();
    if !delivered.is_empty() {
        assert_eq!(
            id_in_pel, delivered,
            "PEL entry ID should match delivered entry ID"
        );
    }
}

// Scenario: blocking with NOACK — no PEL entry after wake
#[test]
fn test_xreadgroup_block_noack_no_pel() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);

    setup.cmds(&["XADD", "s_blk_noack", "*", "k", "setup"]);
    setup.cmds(&["XGROUP", "CREATE", "s_blk_noack", "grp", "$"]);

    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_clone = std::sync::Arc::clone(&done);

    thread::spawn(move || {
        let mut c = Conn::connect(port);
        c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "NOACK",
            "BLOCK",
            "3000",
            "STREAMS",
            "s_blk_noack",
            ">",
        ]);
        done_clone.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(100));
    let mut producer = Conn::connect(port);
    producer.cmds(&["XADD", "s_blk_noack", "*", "k", "v"]);

    // Wait for consumer to finish
    let start = std::time::Instant::now();
    while !done.load(std::sync::atomic::Ordering::SeqCst)
        && start.elapsed() < Duration::from_secs(3)
    {
        thread::sleep(Duration::from_millis(50));
    }

    // PEL should be empty (NOACK)
    let pending = setup.cmds(&["XPENDING", "s_blk_noack", "grp", "-", "+", "10"]);
    assert!(
        pending.is_nil() || pending.as_array().is_empty(),
        "NOACK blocking delivery should not add to PEL"
    );
}

// Scenario: re-delivery path (non-">" id) never blocks even with BLOCK
//   Per Redis semantics: BLOCK only applies to ">" (new entries).
//   Re-delivery of pending entries is always non-blocking.
#[test]
fn test_xreadgroup_block_redelivery_nonblocking() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    setup_stream_with_group(&mut c, "s_redel_blk", "grp", 2);

    // Deliver entries to create PEL
    c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "STREAMS",
        "s_redel_blk",
        ">",
    ]);

    // Re-read with specific ID and BLOCK — should return immediately (non-blocking)
    let start = std::time::Instant::now();
    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "BLOCK",
        "5000", // long timeout — should NOT wait
        "STREAMS",
        "s_redel_blk",
        "0-0",
    ]);
    let elapsed = start.elapsed();

    // Should return immediately with pending entries
    assert!(!r.is_nil(), "re-delivery should return pending entries");
    assert!(
        elapsed < Duration::from_secs(1),
        "re-delivery with BLOCK should not wait, elapsed={elapsed:?}"
    );
    let entries = get_entries(&r, "s_redel_blk");
    assert_eq!(entries.len(), 2);
}

// Scenario: multiple streams — wakes on the stream that gets XADD
#[test]
fn test_xreadgroup_block_multiple_streams() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);

    // Set up two streams, each with its own group
    setup.cmds(&["XADD", "ms_s1", "*", "k", "v"]);
    setup.cmds(&["XADD", "ms_s2", "*", "k", "v"]);
    setup.cmds(&["XGROUP", "CREATE", "ms_s1", "grp", "$"]);
    setup.cmds(&["XGROUP", "CREATE", "ms_s2", "grp", "$"]);

    // Producer fires on stream 2
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let mut p = Conn::connect(port);
        p.cmds(&["XADD", "ms_s2", "*", "from", "stream2"]);
    });

    let mut consumer = Conn::connect(port);
    let r = consumer.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "BLOCK",
        "5000",
        "STREAMS",
        "ms_s1",
        "ms_s2",
        ">",
        ">",
    ]);

    assert!(!r.is_nil(), "should receive entry from stream 2");
    // Result should be for ms_s2, not ms_s1
    let arr = r.as_array();
    assert_eq!(arr.len(), 1, "only one stream should have new entries");
    assert_eq!(
        arr[0].as_array()[0].as_str(),
        "ms_s2",
        "entry should come from ms_s2"
    );
}

// Scenario: BLOCK 0 — blocks indefinitely until XADD (not a timeout case)
#[test]
fn test_xreadgroup_block_zero_waits_for_data() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);

    setup.cmds(&["XADD", "s_blk_zero", "*", "k", "v"]);
    setup.cmds(&["XGROUP", "CREATE", "s_blk_zero", "grp", "$"]);

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        let mut p = Conn::connect(port);
        p.cmds(&["XADD", "s_blk_zero", "*", "k", "arrived"]);
    });

    let mut consumer = Conn::connect(port);
    // BLOCK 0 = block forever
    let r = consumer.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "BLOCK",
        "0",
        "STREAMS",
        "s_blk_zero",
        ">",
    ]);

    assert!(!r.is_nil(), "BLOCK 0 should wait and receive the entry");
    let entries = get_entries(&r, "s_blk_zero");
    assert_eq!(entries.len(), 1);
    assert_eq!(entry_field(&entries[0], "k").as_deref(), Some("arrived"));
}

// ═════════════════════════════════════════════════════════════════════════════
// XDELEX
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_xdelex_keepref_default() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v1"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v2"]);
    c.cmds(&["XADD", "s", "3-0", "f", "v3"]);

    let r = c.cmds(&["XDELEX", "s", "IDS", "3", "1-0", "2-0", "99-0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_int(), 1);
    assert_eq!(arr[2].as_int(), -1);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 1);
}

#[test]
fn test_xdelex_explicit_keepref() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // KEEPREF: delete the entry but leave PEL ref intact
    let r = c.cmds(&["XDELEX", "s", "KEEPREF", "IDS", "1", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), 1);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
    // Pending list still has the dangling reference
    let pending = c.cmds(&["XPENDING", "s", "grp"]);
    assert_eq!(pending.as_array()[0].as_int(), 1);
}

#[test]
fn test_xdelex_delref_prunes_pel() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // Confirm PEL has it
    let pending_before = c.cmds(&["XPENDING", "s", "grp"]);
    assert_eq!(pending_before.as_array()[0].as_int(), 1);

    // DELREF should both remove the entry AND prune PEL
    let r = c.cmds(&["XDELEX", "s", "DELREF", "IDS", "1", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), 1);

    let pending_after = c.cmds(&["XPENDING", "s", "grp"]);
    assert_eq!(pending_after.as_array()[0].as_int(), 0);
}

#[test]
fn test_xdelex_acked_keeps_an_entry_a_group_still_references() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // ACKED: the entry is pending in the group, so it stays → 2
    let r = c.cmds(&["XDELEX", "s", "ACKED", "IDS", "1", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), 2);
    // Entry still there
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 1);

    // Once the group has acknowledged it, it can go. (`REF` is the name this
    // policy had before Redis called it ACKED; it is still accepted.)
    c.cmds(&["XACK", "s", "grp", "1-0"]);
    let r2 = c.cmds(&["XDELEX", "s", "REF", "IDS", "1", "1-0"]);
    assert_eq!(r2.as_array()[0].as_int(), 1);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
}

#[test]
fn test_xdelex_acked_waits_for_every_group() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "read", "0"]);
    c.cmds(&["XGROUP", "CREATE", "s", "unread", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "read", "c1", "STREAMS", "s", ">"]);
    c.cmds(&["XACK", "s", "read", "1-0"]);

    // One group has read and acknowledged it; the other has not read it yet.
    let r = c.cmds(&["XDELEX", "s", "ACKED", "IDS", "1", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), 2);
    c.cmds(&["XREADGROUP", "GROUP", "unread", "c1", "STREAMS", "s", ">"]);
    c.cmds(&["XACK", "s", "unread", "1-0"]);
    // The policy may also follow the IDS block.
    let r = c.cmds(&["XDELEX", "s", "IDS", "1", "1-0", "ACKED"]);
    assert_eq!(r.as_array()[0].as_int(), 1);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
}

#[test]
fn test_xdelex_acked_without_groups_deletes() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);

    // With no consumer groups nothing refers to an entry: ACKED deletes it,
    // as in Redis 8.10.
    let r = c.cmds(&["XDELEX", "s", "ACKED", "IDS", "1", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), 1);

    // And once the last group that held it back is destroyed.
    c.cmds(&["XGROUP", "CREATE", "s", "g", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"]);
    let r = c.cmds(&["XDELEX", "s", "ACKED", "IDS", "1", "2-0"]);
    assert_eq!(r.as_array()[0].as_int(), 2);
    c.cmds(&["XGROUP", "DESTROY", "s", "g"]);
    let r = c.cmds(&["XDELEX", "s", "ACKED", "IDS", "1", "2-0"]);
    assert_eq!(r.as_array()[0].as_int(), 1);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
}

#[test]
fn test_xdelex_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["XDELEX", "nosuch", "IDS", "2", "1-0", "2-0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), -1);
    assert_eq!(arr[1].as_int(), -1);
}

#[test]
fn test_xdelex_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "s", "string"]);
    assert!(c.cmds(&["XDELEX", "s", "IDS", "1", "1-0"]).is_error());
}

#[test]
fn test_xdelex_arity_and_syntax() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);

    // Missing IDS keyword
    assert!(c.cmds(&["XDELEX", "s", "1-0"]).is_error());
    // numids = 0
    assert!(c.cmds(&["XDELEX", "s", "IDS", "0"]).is_error());
    // numids mismatches arg count
    assert!(c.cmds(&["XDELEX", "s", "IDS", "3", "1-0"]).is_error());
    // Bad mode token
    assert!(
        c.cmds(&["XDELEX", "s", "BOGUS", "IDS", "1", "1-0"])
            .is_error()
    );
    // Malformed ID
    assert!(c.cmds(&["XDELEX", "s", "IDS", "1", "not-an-id"]).is_error());
}

#[test]
fn test_xdelex_multiple_ids_mixed_results() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // ACKED: 1-0 and 2-0 are still pending (kept → 2), 99-0 doesn't exist (-1)
    let r = c.cmds(&["XDELEX", "s", "ACKED", "IDS", "3", "1-0", "2-0", "99-0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 2);
    assert_eq!(arr[1].as_int(), 2);
    assert_eq!(arr[2].as_int(), -1);
    // Both entries still there
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XACKDEL
// ═══════════════════════════════════════════════════════════════════════════════

// Basic case: ack + delete in one shot → returns 1
#[test]
fn test_xackdel_basic_ack_and_delete() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // Verify the entry is pending before XACKDEL
    let pending_before = c.cmds(&["XPENDING", "s", "grp"]);
    assert_eq!(pending_before.as_array()[0].as_int(), 1);

    let r = c.cmds(&["XACKDEL", "s", "grp", "1-0"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].as_int(), 1, "acked and deleted → 1");

    // Entry gone from stream
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);

    // PEL is clear
    let pending_after = c.cmds(&["XPENDING", "s", "grp"]);
    assert_eq!(pending_after.as_array()[0].as_int(), 0);
}

// Multiple IDs in one call — per-id result array
#[test]
fn test_xackdel_multiple_ids() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v1"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v2"]);
    c.cmds(&["XADD", "s", "3-0", "f", "v3"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // XACKDEL all three
    let r = c.cmds(&["XACKDEL", "s", "grp", "1-0", "2-0", "3-0"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_int(), 1);
    assert_eq!(arr[2].as_int(), 1);

    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
    assert_eq!(c.cmds(&["XPENDING", "s", "grp"]).as_array()[0].as_int(), 0);
}

// ID not in PEL → returns -1, stream entry untouched
#[test]
fn test_xackdel_id_not_in_pel_returns_minus_one() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    // Do NOT read — 1-0 is never delivered, so not in PEL

    let r = c.cmds(&["XACKDEL", "s", "grp", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), -1, "not in PEL → -1");

    // Entry must still be in the stream (not deleted)
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 1);
}

// Entry already deleted from stream but still in PEL → returns 0
#[test]
fn test_xackdel_acknowledges_an_entry_that_is_already_gone() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // Delete the stream entry manually, leaving the PEL ref dangling
    c.cmds(&["XDEL", "s", "1-0"]);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
    // PEL still has it (dangling ref)
    assert_eq!(c.cmds(&["XPENDING", "s", "grp"]).as_array()[0].as_int(), 1);

    // XACKDEL acknowledges it; there is nothing left to delete, and the
    // entry is gone either way → 1
    let r = c.cmds(&["XACKDEL", "s", "grp", "IDS", "1", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), 1);

    // PEL now clean
    assert_eq!(c.cmds(&["XPENDING", "s", "grp"]).as_array()[0].as_int(), 0);
}

// Mixed result: some in PEL+stream, some in PEL only, some not in PEL
#[test]
fn test_xackdel_mixed_results() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp", "0"]);
    // Deliver both into PEL
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"]);

    // Delete 2-0 from stream (leaves dangling PEL ref)
    c.cmds(&["XDEL", "s", "2-0"]);

    // 1-0: pending, in the stream → 1
    // 2-0: pending, already gone from the stream → 1
    // 3-0: not pending → -1
    let r = c.cmds(&["XACKDEL", "s", "grp", "1-0", "2-0", "3-0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_int(), 1);
    assert_eq!(arr[2].as_int(), -1);

    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
}

// The Redis syntax, and the three reference policies.
#[test]
fn test_xackdel_reference_policies() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ints = |r: &RespValue| -> Vec<i64> { r.as_array().iter().map(|v| v.as_int()).collect() };

    for id in ["1-0", "2-0", "3-0"] {
        c.cmds(&["XADD", "s", id, "f", "v"]);
    }
    c.cmds(&["XGROUP", "CREATE", "s", "a", "0"]);
    c.cmds(&["XGROUP", "CREATE", "s", "b", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "a", "c1", "STREAMS", "s", ">"]);
    c.cmds(&["XREADGROUP", "GROUP", "b", "c1", "STREAMS", "s", ">"]);

    // ACKED: acknowledged in `a`, but `b` still has it pending → 2, kept.
    let r = c.cmds(&["XACKDEL", "s", "a", "ACKED", "IDS", "1", "1-0"]);
    assert_eq!(ints(&r), [2]);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 3);
    // …and deleted once `b` acknowledges it too.
    let r = c.cmds(&["XACKDEL", "s", "b", "ACKED", "IDS", "1", "1-0"]);
    assert_eq!(ints(&r), [1]);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 2);

    // KEEPREF (the default): deleted; `b` keeps its reference to it.
    let r = c.cmds(&["XACKDEL", "s", "a", "KEEPREF", "IDS", "1", "2-0"]);
    assert_eq!(ints(&r), [1]);
    assert_eq!(c.cmds(&["XPENDING", "s", "b"]).as_array()[0].as_int(), 2);

    // DELREF: deleted, and `b`'s reference with it.
    let r = c.cmds(&["XACKDEL", "s", "a", "DELREF", "IDS", "2", "3-0", "9-9"]);
    assert_eq!(ints(&r), [1, -1]);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 0);
    assert_eq!(c.cmds(&["XPENDING", "s", "b"]).as_array()[0].as_int(), 1);
    assert_eq!(c.cmds(&["XPENDING", "s", "a"]).as_array()[0].as_int(), 0);

    // A count that does not match, or a policy given twice, is refused.
    assert!(c.cmds(&["XACKDEL", "s", "a", "IDS", "2", "1-0"]).is_error());
    assert!(
        c.cmds(&["XACKDEL", "s", "a", "ACKED", "DELREF", "IDS", "1", "1-0"])
            .is_error()
    );
}

// Multiple consumer groups — XACKDEL only affects the named group's PEL
#[test]
fn test_xackdel_only_affects_named_group() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp1", "0"]);
    c.cmds(&["XGROUP", "CREATE", "s", "grp2", "0"]);
    // Deliver to both groups
    c.cmds(&["XREADGROUP", "GROUP", "grp1", "c1", "STREAMS", "s", ">"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp2", "c2", "STREAMS", "s", ">"]);

    assert_eq!(c.cmds(&["XPENDING", "s", "grp1"]).as_array()[0].as_int(), 1);
    assert_eq!(c.cmds(&["XPENDING", "s", "grp2"]).as_array()[0].as_int(), 1);

    // XACKDEL on grp1 only
    let r = c.cmds(&["XACKDEL", "s", "grp1", "1-0"]);
    assert_eq!(r.as_array()[0].as_int(), 1);

    // grp1 PEL clear, grp2 PEL still has the entry (but stream entry is gone)
    assert_eq!(c.cmds(&["XPENDING", "s", "grp1"]).as_array()[0].as_int(), 0);
    assert_eq!(c.cmds(&["XPENDING", "s", "grp2"]).as_array()[0].as_int(), 1);
}

// Wrong type → error
#[test]
fn test_xackdel_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "s", "string"]);
    assert!(c.cmds(&["XACKDEL", "s", "grp", "1-0"]).is_error());
}

// Missing key → all -1
#[test]
fn test_xackdel_missing_key() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["XACKDEL", "nosuch", "grp", "1-0", "2-0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), -1);
    assert_eq!(arr[1].as_int(), -1);
}

// No such group → -1 for every ID, as for no such key (Redis 8.10)
#[test]
fn test_xackdel_no_such_group() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    let r = c.cmds(&["XACKDEL", "s", "nosuchgroup", "IDS", "2", "1-0", "2-0"]);
    let ids: Vec<i64> = r.as_array().iter().map(RespValue::as_int).collect();
    assert_eq!(ids, [-1, -1]);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 1);
}

// Arity check
#[test]
fn test_xackdel_arity() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Too few args
    assert!(c.cmds(&["XACKDEL", "s", "grp"]).is_error());
    assert!(c.cmds(&["XACKDEL", "s"]).is_error());
    assert!(c.cmds(&["XACKDEL"]).is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// XCFGSET
// ═══════════════════════════════════════════════════════════════════════════════

// MAX-ENTRIES: trim immediately when policy is set on an oversized stream
#[test]
fn test_xcfgset_max_entries_trims_immediately() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for i in 1..=10u64 {
        c.cmds(&["XADD", "s", &format!("{}-0", i), "f", "v"]);
    }
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 10);

    assert!(c.cmds(&["XCFGSET", "s", "MAX-ENTRIES", "5"]).is_ok());

    // Should have trimmed to 5 immediately
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 5);

    // Oldest entries removed — first remaining should be 6-0
    let first = c.cmds(&["XRANGE", "s", "-", "+", "COUNT", "1"]);
    assert_eq!(first.as_array()[0].as_array()[0].as_str(), "6-0");
}

// MAX-ENTRIES: enforced on every subsequent XADD
#[test]
fn test_xcfgset_max_entries_enforced_on_xadd() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    assert!(c.cmds(&["XCFGSET", "s", "MAX-ENTRIES", "3"]).is_ok());

    // Add more entries — stream should never exceed 3
    c.cmds(&["XADD", "s", "2-0", "f", "v"]);
    c.cmds(&["XADD", "s", "3-0", "f", "v"]);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 3);

    c.cmds(&["XADD", "s", "4-0", "f", "v"]);
    assert_eq!(
        c.cmds(&["XLEN", "s"]).as_int(),
        3,
        "policy must cap at 3 after XADD"
    );

    c.cmds(&["XADD", "s", "5-0", "f", "v"]);
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 3);

    // Newest entries survive — oldest are trimmed
    let entries = c.cmds(&["XRANGE", "s", "-", "+"]);
    let ids: Vec<&str> = entries
        .as_array()
        .iter()
        .map(|e| e.as_array()[0].as_str())
        .collect();
    assert_eq!(ids, vec!["3-0", "4-0", "5-0"]);
}

// MAX-ENTRIES 0: clears the policy — no limit applied on subsequent XADDs
#[test]
fn test_xcfgset_max_entries_zero_clears_policy() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Set a policy
    for i in 1..=5u64 {
        c.cmds(&["XADD", "s", &format!("{}-0", i), "f", "v"]);
    }
    assert!(c.cmds(&["XCFGSET", "s", "MAX-ENTRIES", "3"]).is_ok());
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 3);

    // Clear the policy
    assert!(c.cmds(&["XCFGSET", "s", "MAX-ENTRIES", "0"]).is_ok());

    // Now add many entries — no trimming should occur
    for i in 6..=15u64 {
        c.cmds(&["XADD", "s", &format!("{}-0", i), "f", "v"]);
    }
    assert_eq!(
        c.cmds(&["XLEN", "s"]).as_int(),
        13,
        "policy cleared — no trimming"
    );
}

// MAX-DELETED-ENTRY-ID: advances the tracked value
#[test]
fn test_xcfgset_max_deleted_entry_id() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);

    assert!(
        c.cmds(&["XCFGSET", "s", "MAX-DELETED-ENTRY-ID", "999-0"])
            .is_ok()
    );

    // Verify via XINFO STREAM
    let info = c.cmds(&["XINFO", "STREAM", "s"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();
    assert_eq!(
        map["max-deleted-entry-id"].as_str(),
        "999-0",
        "XINFO STREAM must reflect the updated max-deleted-entry-id"
    );
}

// MAX-DELETED-ENTRY-ID: does not move backwards
#[test]
fn test_xcfgset_max_deleted_entry_id_no_backwards() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    c.cmds(&["XCFGSET", "s", "MAX-DELETED-ENTRY-ID", "500-0"]);

    // Attempt to set a lower value — should be ignored
    c.cmds(&["XCFGSET", "s", "MAX-DELETED-ENTRY-ID", "100-0"]);

    let info = c.cmds(&["XINFO", "STREAM", "s"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();
    assert_eq!(
        map["max-deleted-entry-id"].as_str(),
        "500-0",
        "max-deleted-entry-id must not move backwards"
    );
}

// Multiple fields in one XCFGSET call
#[test]
fn test_xcfgset_multiple_fields() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for i in 1..=10u64 {
        c.cmds(&["XADD", "s", &format!("{}-0", i), "f", "v"]);
    }

    assert!(
        c.cmds(&[
            "XCFGSET",
            "s",
            "MAX-ENTRIES",
            "5",
            "MAX-DELETED-ENTRY-ID",
            "777-0",
        ])
        .is_ok()
    );

    // Both took effect
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 5);

    let info = c.cmds(&["XINFO", "STREAM", "s"]);
    let arr = info.as_array();
    let map: std::collections::HashMap<&str, &RespValue> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), &pair[1]))
        .collect();
    // max-deleted-entry-id set to max(777-0, ids trimmed during immediate trim)
    // The trimmed IDs were 1-0 through 5-0, all less than 777-0, so 777-0 wins.
    assert_eq!(map["max-deleted-entry-id"].as_str(), "777-0");
}

// Missing key → error (XCFGSET does not create streams)
#[test]
fn test_xcfgset_missing_key_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["XCFGSET", "nosuch", "MAX-ENTRIES", "10"]);
    assert!(r.is_error(), "XCFGSET on missing key must error");
}

// Wrong type → error
#[test]
fn test_xcfgset_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "s", "string"]);
    assert!(c.cmds(&["XCFGSET", "s", "MAX-ENTRIES", "10"]).is_error());
}

// Unknown field → error
#[test]
fn test_xcfgset_unknown_field() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);
    let r = c.cmds(&["XCFGSET", "s", "BOGUS-FIELD", "42"]);
    assert!(r.is_error());
}

// Bad value for MAX-ENTRIES → error, stream unchanged
#[test]
fn test_xcfgset_bad_value_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for i in 1..=5u64 {
        c.cmds(&["XADD", "s", &format!("{}-0", i), "f", "v"]);
    }

    let r = c.cmds(&["XCFGSET", "s", "MAX-ENTRIES", "notanumber"]);
    assert!(r.is_error());

    // Stream must be unchanged
    assert_eq!(c.cmds(&["XLEN", "s"]).as_int(), 5);
}

// Arity: odd number of field/value args → error
#[test]
fn test_xcfgset_arity() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["XADD", "s", "1-0", "f", "v"]);

    // Missing value for the field
    assert!(c.cmds(&["XCFGSET", "s", "MAX-ENTRIES"]).is_error());
    // No fields at all
    assert!(c.cmds(&["XCFGSET", "s"]).is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// XNACK (Redis 8.8)
//
// Releases pending entries back to the group PEL without acknowledging them, so
// another consumer can claim them immediately rather than waiting out a
// min-idle-time.
// ═════════════════════════════════════════════════════════════════════════════

/// Set up a stream with `n` entries, a group, and every entry delivered to
/// `consumer1`. Returns the entry IDs in order.
fn setup_pending(c: &mut Conn, key: &str, group: &str, n: usize) -> Vec<String> {
    c.cmds(&["DEL", key]);
    let mut ids = Vec::new();
    for i in 0..n {
        let v = format!("value{i}");
        let id = c.cmds(&["XADD", key, "*", "field", &v]);
        ids.push(id.as_str().to_string());
    }
    c.cmds(&["XGROUP", "CREATE", key, group, "0"]);
    c.cmds(&[
        "XREADGROUP",
        "GROUP",
        group,
        "consumer1",
        "STREAMS",
        key,
        ">",
    ]);
    ids
}

/// Basic FAIL mode: entries are released, counted, and become unowned.
#[test]
fn test_xnack_fail_releases_entries() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:fail", "g", 2);

    let r = c.cmds(&[
        "XNACK", "xn:fail", "g", "FAIL", "IDS", "2", &ids[0], &ids[1],
    ]);
    assert_eq!(r.as_int(), 2);

    // Both now show an empty consumer and idle -1, per the XNACK page.
    let pending = c.cmds(&["XPENDING", "xn:fail", "g", "-", "+", "10"]);
    let rows = pending.as_array();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let f = row.as_array();
        assert_eq!(f[1].as_str(), "", "released entries must be unowned");
        assert_eq!(f[2].as_int(), -1, "released entries report idle -1");
        // FAIL leaves the delivery counter unchanged (it was 1).
        assert_eq!(f[3].as_int(), 1, "FAIL must not change the delivery count");
    }
}

/// SILENT decrements the delivery counter, undoing the delivery.
#[test]
fn test_xnack_silent_decrements_delivery_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:silent", "g", 1);

    let r = c.cmds(&["XNACK", "xn:silent", "g", "SILENT", "IDS", "1", &ids[0]]);
    assert_eq!(r.as_int(), 1);

    let pending = c.cmds(&["XPENDING", "xn:silent", "g", "-", "+", "10"]);
    let f = pending.as_array()[0].as_array();
    assert_eq!(f[3].as_int(), 0, "SILENT must decrement 1 → 0");
}

/// FATAL pins the delivery counter to LLONG_MAX.
#[test]
fn test_xnack_fatal_sets_max_delivery_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:fatal", "g", 1);

    let r = c.cmds(&["XNACK", "xn:fatal", "g", "FATAL", "IDS", "1", &ids[0]]);
    assert_eq!(r.as_int(), 1);

    let pending = c.cmds(&["XPENDING", "xn:fatal", "g", "-", "+", "10"]);
    let f = pending.as_array()[0].as_array();
    assert_eq!(f[3].as_int(), i64::MAX, "FATAL must set LLONG_MAX");
}

/// RETRYCOUNT overrides the mode-based adjustment.
#[test]
fn test_xnack_retrycount_overrides_mode() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:retry", "g", 1);

    let r = c.cmds(&[
        "XNACK",
        "xn:retry",
        "g",
        "FATAL",
        "IDS",
        "1",
        &ids[0],
        "RETRYCOUNT",
        "7",
    ]);
    assert_eq!(r.as_int(), 1);

    let pending = c.cmds(&["XPENDING", "xn:retry", "g", "-", "+", "10"]);
    let f = pending.as_array()[0].as_array();
    assert_eq!(f[3].as_int(), 7, "RETRYCOUNT must win over FATAL");
}

/// XNACK does not remove entries from the PEL — that is what XACK is for.
#[test]
fn test_xnack_does_not_acknowledge() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:noack", "g", 1);

    c.cmds(&["XNACK", "xn:noack", "g", "FAIL", "IDS", "1", &ids[0]]);
    let pending = c.cmds(&["XPENDING", "xn:noack", "g", "-", "+", "10"]);
    assert_eq!(
        pending.as_array().len(),
        1,
        "XNACK must leave the entry pending"
    );
}

/// A released entry is immediately claimable regardless of min-idle-time.
#[test]
fn test_xnack_makes_entry_immediately_claimable() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:claim", "g", 1);

    // Before XNACK the entry was just delivered, so a large min-idle-time
    // excludes it.
    let before = c.cmds(&["XCLAIM", "xn:claim", "g", "consumer2", "999999", &ids[0]]);
    assert_eq!(
        before.as_array().len(),
        0,
        "a freshly delivered entry should not be claimable at a high min-idle"
    );

    c.cmds(&["XNACK", "xn:claim", "g", "FAIL", "IDS", "1", &ids[0]]);

    let after = c.cmds(&["XCLAIM", "xn:claim", "g", "consumer2", "999999", &ids[0]]);
    assert_eq!(
        after.as_array().len(),
        1,
        "a released entry must be claimable at any min-idle"
    );
}

/// IDs that are not pending are ignored and not counted.
#[test]
fn test_xnack_ignores_non_pending_ids() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:ignore", "g", 1);

    let r = c.cmds(&[
        "XNACK",
        "xn:ignore",
        "g",
        "FAIL",
        "IDS",
        "2",
        &ids[0],
        "9999999999999-0",
    ]);
    assert_eq!(r.as_int(), 1, "only the pending ID counts");
}

/// FORCE creates an unowned PEL entry for an ID that exists in the stream but
/// is not currently pending.
#[test]
fn test_xnack_force_creates_pel_entry() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let key = "xn:force";
    c.cmds(&["DEL", key]);
    let id = c.cmds(&["XADD", key, "*", "f", "v"]).as_str().to_string();
    c.cmds(&["XGROUP", "CREATE", key, "g", "0"]);
    // Never read, so nothing is pending.
    assert_eq!(
        c.cmds(&["XPENDING", key, "g", "-", "+", "10"])
            .as_array()
            .len(),
        0
    );

    // Without FORCE the ID is simply ignored.
    assert_eq!(
        c.cmds(&["XNACK", key, "g", "FAIL", "IDS", "1", &id])
            .as_int(),
        0
    );

    // With FORCE it is created.
    assert_eq!(
        c.cmds(&["XNACK", key, "g", "FAIL", "IDS", "1", &id, "FORCE"])
            .as_int(),
        1
    );
    let pending = c.cmds(&["XPENDING", key, "g", "-", "+", "10"]);
    assert_eq!(pending.as_array().len(), 1);
    let f = pending.as_array()[0].as_array();
    assert_eq!(f[1].as_str(), "", "FORCE-created entries are unowned");
    assert_eq!(f[3].as_int(), 0, "FORCE-created entries start at 0");

    // FORCE must not invent entries for IDs that aren't in the stream at all.
    assert_eq!(
        c.cmds(&[
            "XNACK",
            key,
            "g",
            "FAIL",
            "IDS",
            "1",
            "9999999999999-0",
            "FORCE"
        ])
        .as_int(),
        0
    );
}

/// Error cases.
#[test]
fn test_xnack_errors() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let ids = setup_pending(&mut c, "xn:err", "g", 1);

    // Missing key → NOGROUP, as for a missing group (Redis 8.10).
    let r = c.cmds(&["XNACK", "xn:absent", "g", "FAIL", "IDS", "1", &ids[0]]);
    assert!(format!("{r:?}").contains("NOGROUP"), "{r:?}");
    // Options before the IDS block as well as after; one it does not know
    // is named.
    assert_eq!(
        c.cmds(&[
            "XNACK",
            "xn:err",
            "g",
            "FAIL",
            "RETRYCOUNT",
            "3",
            "IDS",
            "1",
            &ids[0]
        ])
        .as_int(),
        1
    );
    let r = c.cmds(&[
        "XNACK", "xn:err", "g", "FAIL", "BADOPT", "IDS", "1", &ids[0],
    ]);
    assert!(
        format!("{r:?}").contains("Unrecognized XNACK option 'BADOPT'"),
        "{r:?}"
    );
    // Unknown group is NOGROUP.
    let r = c.cmds(&["XNACK", "xn:err", "nosuch", "FAIL", "IDS", "1", &ids[0]]);
    assert!(r.is_error(), "unknown group should error, got {r:?}");

    // Bad mode, missing IDS keyword, mismatched numids.
    assert!(
        c.cmds(&["XNACK", "xn:err", "g", "NOPE", "IDS", "1", &ids[0]])
            .is_error()
    );
    assert!(
        c.cmds(&["XNACK", "xn:err", "g", "FAIL", "NOPE", "1", &ids[0]])
            .is_error()
    );
    assert!(
        c.cmds(&["XNACK", "xn:err", "g", "FAIL", "IDS", "5", &ids[0]])
            .is_error()
    );
    assert!(c.cmds(&["XNACK", "xn:err", "g", "FAIL"]).is_error());
    // Malformed stream ID.
    assert!(
        c.cmds(&["XNACK", "xn:err", "g", "FAIL", "IDS", "1", "not-an-id"])
            .is_error()
    );

    // Wrong type.
    c.cmds(&["DEL", "xn:str"]);
    c.cmds(&["SET", "xn:str", "x"]);
    let r = c.cmds(&["XNACK", "xn:str", "g", "FAIL", "IDS", "1", &ids[0]]);
    assert!(r.is_error(), "wrong type should error, got {r:?}");
}

/// `MAXLEN ~` removes whole nodes, and Redis ends a node at
/// `stream-node-max-bytes` (4096) as well as at `stream-node-max-entries`
/// (100). With entries of ~200 bytes a node holds about 15, so `~ 20` keeps
/// the stream near 20 entries (Redis: 26). Counting by entries alone kept
/// 100. BullMQ's event stream is trimmed this way.
#[test]
fn test_approximate_trim_ends_nodes_at_stream_node_max_bytes() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let value = "x".repeat(200);
    for i in 0..400 {
        let id = i.to_string();
        c.cmds(&[
            "XADD",
            "big",
            "MAXLEN",
            "~",
            "20",
            "*",
            "event",
            "completed",
            "jobId",
            &id,
            "returnvalue",
            &value,
        ]);
    }
    let len = c.cmds(&["XLEN", "big"]).as_int();
    assert!(
        (20..=40).contains(&len),
        "XLEN after MAXLEN ~ 20 with large entries: {len}"
    );

    // Small entries: nodes end at 100 entries, as before.
    for i in 0..400 {
        let id = i.to_string();
        c.cmds(&["XADD", "small", "MAXLEN", "~", "20", "*", "n", &id]);
    }
    assert_eq!(c.cmds(&["XLEN", "small"]).as_int(), 100);

    // With the byte limit off, large entries are counted 100 to a node too.
    assert_eq!(
        c.cmds(&["CONFIG", "SET", "stream-node-max-bytes", "0"])
            .as_str(),
        "OK"
    );
    for i in 0..400 {
        let id = i.to_string();
        c.cmds(&[
            "XADD", "nobytes", "MAXLEN", "~", "20", "*", "v", &value, "i", &id,
        ]);
    }
    assert_eq!(c.cmds(&["XLEN", "nobytes"]).as_int(), 100);
    let r = c.cmds(&["CONFIG", "GET", "stream-node-max-bytes"]);
    assert_eq!(r.as_array()[1].as_str(), "0");
    c.cmds(&["CONFIG", "SET", "stream-node-max-bytes", "4kb"]);
    let r = c.cmds(&["CONFIG", "GET", "stream-node-max-bytes"]);
    assert_eq!(r.as_array()[1].as_str(), "4096");
}

/// `XTRIM … ACKED`, with `~` or without, passes over an entry a group still
/// needs and goes on to the next, as Redis does (8.10's unit/type/stream).
#[test]
fn test_xtrim_acked_passes_over_what_a_group_still_needs() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    for approx in [true, false] {
        c.cmds(&["DEL", "s"]);
        for id in ["1-0", "2-0", "3-0", "4-0", "5-0"] {
            c.cmds(&["XADD", "s", id, "f", "v"]);
        }
        c.cmds(&["XGROUP", "CREATE", "s", "g", "0"]);
        c.cmds(&["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"]);
        c.cmds(&["XACK", "s", "g", "1-0", "2-0", "4-0"]);

        let how = if approx { "~" } else { "=" };
        let r = c.cmds(&["XTRIM", "s", "MINID", how, "6-0", "ACKED"]);
        assert_eq!(r.as_int(), 3, "{how}");
        let left = c.cmds(&["XRANGE", "s", "-", "+"]);
        let ids: Vec<&str> = left
            .as_array()
            .iter()
            .map(|e| e.as_array()[0].as_str())
            .collect();
        assert_eq!(ids, ["3-0", "5-0"], "{how}");
    }
}

/// Redis 8.10's stream-cgroups checks: ENTRIESREAD past what was added is
/// capped to it; CLAIM is XREADGROUP's, its time a strict non-negative
/// integer; an entry XCLAIM FORCE makes starts as one delivery.
#[test]
fn test_entriesread_cap_and_claim_arguments() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let entries_read = |c: &mut Conn| -> i64 {
        let info = c.cmds(&["XINFO", "GROUPS", "cg:s"]);
        let g = info.as_array()[0].as_array();
        let at = g
            .iter()
            .position(|v| matches!(v, RespValue::Bulk(b) if b == b"entries-read"))
            .unwrap();
        g[at + 1].as_int()
    };
    c.cmds(&["DEL", "cg:s"]);
    c.cmds(&["XADD", "cg:s", "*", "f", "v"]);
    assert!(
        c.cmds(&["XGROUP", "CREATE", "cg:s", "g", "$", "ENTRIESREAD", "9999"])
            .is_ok()
    );
    assert_eq!(entries_read(&mut c), 1);
    assert!(
        c.cmds(&["XGROUP", "SETID", "cg:s", "g", "$", "ENTRIESREAD", "9999"])
            .is_ok()
    );
    assert_eq!(entries_read(&mut c), 1);

    let err = |r: RespValue| format!("{r:?}");
    assert!(
        err(c.cmds(&["XREAD", "CLAIM", "10", "STREAMS", "cg:s", "0"]))
            .contains("The CLAIM option is only supported by XREADGROUP")
    );
    assert!(
        err(c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "CLAIM",
            "+10",
            "STREAMS",
            "cg:s",
            ">"
        ]))
        .contains("min-idle-time is not an integer")
    );
    assert!(
        err(c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "CLAIM",
            "-1",
            "STREAMS",
            "cg:s",
            ">"
        ]))
        .contains("min-idle-time must be a positive integer")
    );

    c.cmds(&["DEL", "cg:s"]);
    for id in ["1-0", "2-0", "3-0"] {
        c.cmds(&["XADD", "cg:s", id, "f", "v"]);
    }
    c.cmds(&["XGROUP", "CREATE", "cg:s", "g", "0"]);
    c.cmds(&[
        "XCLAIM", "cg:s", "g", "c3", "0", "1-0", "2-0", "FORCE", "JUSTID",
    ]);
    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c2",
        "CLAIM",
        "0",
        "COUNT",
        "2",
        "STREAMS",
        "cg:s",
        ">",
    ]);
    let messages = r.as_array()[0].as_array()[1].as_array();
    assert_eq!(messages.len(), 2);
    for m in messages {
        assert_eq!(m.as_array()[3].as_int(), 1, "{m:?}");
    }
}

/// Entries XNACK releases are claimed in the order they were released, and
/// one released again goes to the back, as in Redis 8.10's NACK zone.
/// `CLAIM -0` is not an integer, as Redis reads one.
#[test]
fn test_renack_goes_to_the_back() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["DEL", "nz:s"]);
    for id in ["1-0", "2-0", "3-0"] {
        c.cmds(&["XADD", "nz:s", id, "f", "v"]);
    }
    c.cmds(&["XGROUP", "CREATE", "nz:s", "grp", "0"]);
    c.cmds(&["XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "nz:s", ">"]);
    c.cmds(&[
        "XNACK", "nz:s", "grp", "FAIL", "IDS", "3", "1-0", "2-0", "3-0",
    ]);
    c.cmds(&["XNACK", "nz:s", "grp", "FAIL", "IDS", "1", "1-0"]);
    let mut order = Vec::new();
    for _ in 0..3 {
        let r = c.cmds(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c2",
            "COUNT",
            "1",
            "CLAIM",
            "0",
            "STREAMS",
            "nz:s",
            ">",
        ]);
        order.push(
            r.as_array()[0].as_array()[1].as_array()[0].as_array()[0]
                .as_str()
                .to_string(),
        );
    }
    assert_eq!(order, ["2-0", "3-0", "1-0"]);

    // XINFO STREAM FULL shows a released entry's delivery time as 0.
    c.cmds(&["XNACK", "nz:s", "grp", "FAIL", "IDS", "1", "1-0"]);
    // The pending entry `[id, consumer, delivery time, count]` for 1-0.
    fn find<'a>(v: &'a RespValue, id: &[u8]) -> Option<&'a [RespValue]> {
        let RespValue::Array(items) = v else {
            return None;
        };
        if items.len() == 4 && matches!(&items[0], RespValue::Bulk(b) if b == id) {
            return Some(items);
        }
        items.iter().find_map(|item| find(item, id))
    }
    let full = c.cmds(&["XINFO", "STREAM", "nz:s", "FULL"]);
    let entry = find(&full, b"1-0").expect("1-0 pending");
    assert_eq!(entry[2], RespValue::Int(0), "{entry:?}");

    let r = c.cmds(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c2",
        "CLAIM",
        "-0",
        "STREAMS",
        "nz:s",
        ">",
    ]);
    assert!(
        format!("{r:?}").contains("min-idle-time is not an integer"),
        "{r:?}"
    );
}
