// Integration tests for List and Set commands.
// Same harness as string-json.rs and vector_hash.rs — each test gets its
// own server on a random port so tests run fully isolated and in parallel.

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

/// Poll `INFO clients` until `blocked_clients` reaches `want` (BLPOP registers in
/// the wait table strictly before bumping this counter — see `commands::lists`).
/// Tests that need a deterministic "this client is now blocked" signal should use
/// this instead of a fixed sleep: a sleep only asserts a probability, and under a
/// loaded/virtualized runner (e.g. a cold Docker container) it can lose the race.
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

// ═════════════════════════════════════════════════════════════════════════════
// LIST COMMAND TESTS
// ═════════════════════════════════════════════════════════════════════════════

// Manual test scenario:
//   LPUSH mylist c b a   → 3  (pushed in order, list is [a, b, c])
//   LLEN mylist          → 3
//   LRANGE mylist 0 -1   → ["a", "b", "c"]
#[test]
fn test_lpush_llen_lrange() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["LPUSH", "l", "c", "b", "a"]).as_int(), 3);
    assert_eq!(c.cmds(&["LLEN", "l"]).as_int(), 3);

    let r = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["a", "b", "c"]);
}

// Manual test scenario:
//   RPUSH mylist a b c   → 3  (list is [a, b, c])
//   LRANGE mylist 0 -1   → ["a", "b", "c"]
//   LRANGE mylist 0 1    → ["a", "b"]
//   LRANGE mylist -2 -1  → ["b", "c"]
//   LRANGE mylist 5 10   → []  (out of range)
#[test]
fn test_rpush_lrange_slices() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["RPUSH", "l", "a", "b", "c"]).as_int(), 3);

    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let full: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(full, vec!["a", "b", "c"]);

    let binding = c.cmds(&["LRANGE", "l", "0", "1"]);
    let first_two: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(first_two, vec!["a", "b"]);

    let binding = c.cmds(&["LRANGE", "l", "-2", "-1"]);
    let last_two: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(last_two, vec!["b", "c"]);

    assert_eq!(c.cmds(&["LRANGE", "l", "5", "10"]).as_array().len(), 0);
}

// Manual test scenario:
//   RPUSH l a b c
//   LPOP l          → "a"
//   RPOP l          → "c"
//   LLEN l          → 1
//   LPOP nosuch     → nil
#[test]
fn test_lpop_rpop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c"]);
    assert_eq!(c.cmds(&["LPOP", "l"]).as_str(), "a");
    assert_eq!(c.cmds(&["RPOP", "l"]).as_str(), "c");
    assert_eq!(c.cmds(&["LLEN", "l"]).as_int(), 1);
    assert!(c.cmds(&["LPOP", "nosuch"]).is_nil());
}

// Manual test scenario:
//   RPUSH l a b c
//   LINDEX l 0    → "a"
//   LINDEX l -1   → "c"
//   LINDEX l 10   → nil  (out of range)
#[test]
fn test_lindex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c"]);
    assert_eq!(c.cmds(&["LINDEX", "l", "0"]).as_str(), "a");
    assert_eq!(c.cmds(&["LINDEX", "l", "-1"]).as_str(), "c");
    assert!(c.cmds(&["LINDEX", "l", "10"]).is_nil());
}

// Manual test scenario:
//   RPUSH l a b c
//   LSET l 1 "B"
//   LRANGE l 0 -1  → ["a", "B", "c"]
//   LSET l 10 "x"  → ERR (out of range)
#[test]
fn test_lset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c"]);
    assert!(c.cmds(&["LSET", "l", "1", "B"]).is_ok());

    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["a", "B", "c"]);
    assert!(c.cmds(&["LSET", "l", "10", "x"]).is_error());
}

// Manual test scenario:
//   RPUSH l a b c
//   LINSERT l BEFORE b "X"  → 4
//   LRANGE l 0 -1           → ["a", "X", "b", "c"]
//   LINSERT l AFTER b "Y"   → 5
//   LRANGE l 0 -1           → ["a", "X", "b", "Y", "c"]
//   LINSERT l BEFORE nosuch "Z"  → -1
#[test]
fn test_linsert() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c"]);

    assert_eq!(c.cmds(&["LINSERT", "l", "BEFORE", "b", "X"]).as_int(), 4);
    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["a", "X", "b", "c"]);

    assert_eq!(c.cmds(&["LINSERT", "l", "AFTER", "b", "Y"]).as_int(), 5);
    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["a", "X", "b", "Y", "c"]);

    assert_eq!(
        c.cmds(&["LINSERT", "l", "BEFORE", "nosuch", "Z"]).as_int(),
        -1
    );
}

// Manual test scenario:
//   RPUSH l a b a c a
//   LREM l 2 a          → 2  (removes first 2 occurrences from head)
//   LRANGE l 0 -1       → ["b", "c", "a"]
//   LREM l -1 a         → 1  (removes last occurrence)
//   LRANGE l 0 -1       → ["b", "c"]
//   LREM l 0 b          → 1  (removes all occurrences)
#[test]
fn test_lrem() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "a", "c", "a"]);

    assert_eq!(c.cmds(&["LREM", "l", "2", "a"]).as_int(), 2);
    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["b", "c", "a"]);

    assert_eq!(c.cmds(&["LREM", "l", "-1", "a"]).as_int(), 1);
    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["b", "c"]);

    assert_eq!(c.cmds(&["LREM", "l", "0", "b"]).as_int(), 1);
}

// Manual test scenario:
//   RPUSH l a b c d e
//   LTRIM l 1 3
//   LRANGE l 0 -1   → ["b", "c", "d"]
//   LTRIM l 0 0
//   LRANGE l 0 -1   → ["b"]
#[test]
fn test_ltrim() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c", "d", "e"]);
    assert!(c.cmds(&["LTRIM", "l", "1", "3"]).is_ok());
    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["b", "c", "d"]);

    assert!(c.cmds(&["LTRIM", "l", "0", "0"]).is_ok());
    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["b"]);
}

// Manual test scenario:
//   RPUSH l a b c
//   LPOS l b        → 1
//   LPOS l a        → 0
//   LPOS l nosuch   → nil
#[test]
fn test_lpos() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c"]);
    assert_eq!(c.cmds(&["LPOS", "l", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["LPOS", "l", "a"]).as_int(), 0);
    assert!(c.cmds(&["LPOS", "l", "nosuch"]).is_nil());
}

// Manual test scenario:
//   RPUSH l a b c
//   LPUSHX l x      → 4  (key exists, prepend)
//   LPUSHX nosuch x → 0  (key missing, no-op)
//   RPUSHX l y      → 5  (key exists, append)
//   RPUSHX nosuch y → 0
//   LRANGE l 0 -1   → ["x", "a", "b", "c", "y"]
#[test]
fn test_lpushx_rpushx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c"]);
    assert_eq!(c.cmds(&["LPUSHX", "l", "x"]).as_int(), 4);
    assert_eq!(c.cmds(&["LPUSHX", "nosuch", "x"]).as_int(), 0);
    assert_eq!(c.cmds(&["RPUSHX", "l", "y"]).as_int(), 5);
    assert_eq!(c.cmds(&["RPUSHX", "nosuch", "y"]).as_int(), 0);

    let binding = c.cmds(&["LRANGE", "l", "0", "-1"]);
    let items: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(items, vec!["x", "a", "b", "c", "y"]);
}

// Manual test scenario:
//   RPUSH src a b c
//   LMOVE src dst LEFT RIGHT   → "a"  (pop left, push right)
//   LRANGE src 0 -1            → ["b", "c"]
//   LRANGE dst 0 -1            → ["a"]
//   LMOVE src dst RIGHT LEFT   → "c"  (pop right, push left)
//   LRANGE dst 0 -1            → ["c", "a"]
#[test]
fn test_lmove() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "src", "a", "b", "c"]);

    assert_eq!(
        c.cmds(&["LMOVE", "src", "dst", "LEFT", "RIGHT"]).as_str(),
        "a"
    );
    let binding = c.cmds(&["LRANGE", "src", "0", "-1"]);
    let src: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(src, vec!["b", "c"]);
    let binding = c.cmds(&["LRANGE", "dst", "0", "-1"]);
    let dst: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(dst, vec!["a"]);

    assert_eq!(
        c.cmds(&["LMOVE", "src", "dst", "RIGHT", "LEFT"]).as_str(),
        "c"
    );
    let binding = c.cmds(&["LRANGE", "dst", "0", "-1"]);
    let dst: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(dst, vec!["c", "a"]);
}

// Manual test scenario:
//   RPUSH src a b c
//   RPOPLPUSH src dst   → "c"
//   LRANGE src 0 -1     → ["a", "b"]
//   LRANGE dst 0 -1     → ["c"]
#[test]
fn test_rpoplpush() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "src", "a", "b", "c"]);
    assert_eq!(c.cmds(&["RPOPLPUSH", "src", "dst"]).as_str(), "c");

    let binding = c.cmds(&["LRANGE", "src", "0", "-1"]);

    let src: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(src, vec!["a", "b"]);

    let binding = c.cmds(&["LRANGE", "dst", "0", "-1"]);
    let dst: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(dst, vec!["c"]);
}

// Manual test scenario:
//   RPUSH l a b c
//   LMPOP 1 l LEFT         → ["l", ["a"]]
//   LMPOP 1 l LEFT COUNT 2 → ["l", ["b", "c"]]
//   LMPOP 1 l LEFT         → nil  (empty)
#[test]
fn test_lmpop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "l", "a", "b", "c"]);

    let r = c.cmds(&["LMPOP", "1", "l", "LEFT"]);
    let parts = r.as_array();
    assert_eq!(parts[0].as_str(), "l");
    let popped: Vec<&str> = parts[1].as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(popped, vec!["a"]);

    let r2 = c.cmds(&["LMPOP", "1", "l", "LEFT", "COUNT", "2"]);
    let parts2 = r2.as_array();
    assert_eq!(parts2[0].as_str(), "l");
    let popped2: Vec<&str> = parts2[1].as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(popped2, vec!["b", "c"]);

    assert!(c.cmds(&["LMPOP", "1", "l", "LEFT"]).is_nil());
}

// Manual test scenario:
//   BLPOP l 0.1      → nil  (timeout, list empty)
//   RPUSH l hello    (in background thread)
//   BLPOP l 2        → ["l", "hello"]  (woken up by push)
#[test]
fn test_blpop_timeout_and_wakeup() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Timeout path — list doesn't exist
    assert!(c.cmds(&["BLPOP", "bltest", "0.1"]).is_nil());

    // Wakeup path — push arrives after block starts
    let port2 = port;
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let mut pusher = Conn::connect(port2);
        pusher.cmds(&["RPUSH", "bltest2", "hello"]);
    });

    let r = c.cmds(&["BLPOP", "bltest2", "2"]);
    let parts = r.as_array();
    assert_eq!(parts[0].as_str(), "bltest2");
    assert_eq!(parts[1].as_str(), "hello");
}

// Manual test scenario:
//   BRPOP l 0.1      → nil  (timeout, list empty)
//   LPUSH l world    (in background thread)
//   BRPOP l 2        → ["l", "world"]  (woken up by push)
#[test]
fn test_brpop_timeout_and_wakeup() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["BRPOP", "brtest", "0.1"]).is_nil());

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let mut pusher = Conn::connect(port);
        pusher.cmds(&["LPUSH", "brtest2", "world"]);
    });

    let r = c.cmds(&["BRPOP", "brtest2", "2"]);
    let parts = r.as_array();
    assert_eq!(parts[0].as_str(), "brtest2");
    assert_eq!(parts[1].as_str(), "world");
}

// Manual test scenario:
//   BLPOP proves FIFO: two clients block, two pushes happen
//   First client to block gets first push
#[test]
fn test_blpop_fifo_ordering() {
    let port = free_port();
    start_server(port);

    // Two clients block in order — c1 first, c2 second
    let port2 = port;
    let h1 = thread::spawn(move || {
        let mut c1 = Conn::connect(port2);
        c1.cmds(&["BLPOP", "fifo", "2"])
    });

    // Wait for c1's BLPOP to actually register before spawning c2 — a fixed sleep
    // can't guarantee registration order under a loaded/virtualized runner.
    wait_for_blocked_clients(port, 1);

    let port3 = port;
    let h2 = thread::spawn(move || {
        let mut c2 = Conn::connect(port3);
        c2.cmds(&["BLPOP", "fifo", "2"])
    });

    // Let both register before pushing.
    wait_for_blocked_clients(port, 2);

    let mut pusher = Conn::connect(port);
    pusher.cmds(&["RPUSH", "fifo", "first", "second"]);

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    assert_eq!(r1.as_array()[1].as_str(), "first");
    assert_eq!(r2.as_array()[1].as_str(), "second");
}

// Manual test scenario:
//   WRONGTYPE: RPUSH on a key that holds a string
#[test]
fn test_list_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "hello"]);
    assert!(c.cmds(&["LPUSH", "strkey", "val"]).is_error());
    assert!(c.cmds(&["LRANGE", "strkey", "0", "-1"]).is_error());
    assert!(c.cmds(&["BLPOP", "strkey", "0.1"]).is_error());
}

// ═════════════════════════════════════════════════════════════════════════════
// LPOP / RPOP WITH COUNT
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_lpop_with_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "k", "a", "b", "c", "d", "e"]);

    // count=2 returns array of first 2, removes them
    let got = c.cmds(&["LPOP", "k", "2"]);
    let arr = got.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_str(), "a");
    assert_eq!(arr[1].as_str(), "b");

    // remaining list starts at c
    assert_eq!(c.cmds(&["LRANGE", "k", "0", "-1"]).as_array().len(), 3);

    // count larger than list → returns all remaining
    let got2 = c.cmds(&["LPOP", "k", "99"]);
    assert_eq!(got2.as_array().len(), 3);

    // list now empty → nil (no count)
    assert!(c.cmds(&["LPOP", "k"]).is_nil());

    // count on missing key → nil
    assert!(c.cmds(&["LPOP", "nosuch", "3"]).is_nil());
}

#[test]
fn test_rpop_with_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "k", "a", "b", "c", "d", "e"]);

    // count=2 returns array of last 2 (e, d — tail first)
    let got = c.cmds(&["RPOP", "k", "2"]);
    let arr = got.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_str(), "e");
    assert_eq!(arr[1].as_str(), "d");

    // remaining: a b c
    assert_eq!(c.cmds(&["LLEN", "k"]).as_int(), 3);

    // count=0 on existing list → empty array
    let got2 = c.cmds(&["RPOP", "k", "0"]);
    assert_eq!(got2.as_array().len(), 0);

    // no count → single bulk string
    assert_eq!(c.cmds(&["RPOP", "k"]).as_str(), "c");
}

// ═════════════════════════════════════════════════════════════════════════════
// LPOS WITH RANK / COUNT / MAXLEN
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_lpos_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "k", "a", "b", "c", "b", "b"]);
    //  indices:              0    1    2    3    4

    // No options → first match
    assert_eq!(c.cmds(&["LPOS", "k", "b"]).as_int(), 1);

    // Missing element → nil
    assert!(c.cmds(&["LPOS", "k", "z"]).is_nil());

    // Missing key → nil
    assert!(c.cmds(&["LPOS", "nosuch", "b"]).is_nil());
}

#[test]
fn test_lpos_rank() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "k", "a", "b", "c", "b", "b"]);
    //  indices:              0    1    2    3    4

    // RANK 1 → first match (index 1)
    assert_eq!(c.cmds(&["LPOS", "k", "b", "RANK", "1"]).as_int(), 1);

    // RANK 2 → second match (index 3)
    assert_eq!(c.cmds(&["LPOS", "k", "b", "RANK", "2"]).as_int(), 3);

    // RANK 3 → third match (index 4)
    assert_eq!(c.cmds(&["LPOS", "k", "b", "RANK", "3"]).as_int(), 4);

    // RANK 4 → no fourth match → nil
    assert!(c.cmds(&["LPOS", "k", "b", "RANK", "4"]).is_nil());

    // Negative RANK: scan from tail
    // RANK -1 → last match (index 4)
    assert_eq!(c.cmds(&["LPOS", "k", "b", "RANK", "-1"]).as_int(), 4);

    // RANK -2 → second-to-last match (index 3)
    assert_eq!(c.cmds(&["LPOS", "k", "b", "RANK", "-2"]).as_int(), 3);
}

#[test]
fn test_lpos_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "k", "a", "b", "c", "b", "b"]);

    // COUNT 0 → all matches
    let all = c.cmds(&["LPOS", "k", "b", "COUNT", "0"]);
    let arr = all.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_int(), 3);
    assert_eq!(arr[2].as_int(), 4);

    // COUNT 2 → first 2 matches
    let two = c.cmds(&["LPOS", "k", "b", "COUNT", "2"]);
    let arr2 = two.as_array();
    assert_eq!(arr2.len(), 2);
    assert_eq!(arr2[0].as_int(), 1);
    assert_eq!(arr2[1].as_int(), 3);

    // COUNT 0 on missing element → empty array (not nil)
    let empty = c.cmds(&["LPOS", "k", "z", "COUNT", "0"]);
    assert_eq!(empty.as_array().len(), 0);

    // COUNT 0 on missing key → empty array
    let empty2 = c.cmds(&["LPOS", "nosuch", "b", "COUNT", "0"]);
    assert_eq!(empty2.as_array().len(), 0);
}

#[test]
fn test_lpos_maxlen() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "k", "a", "b", "c", "b", "b"]);

    // MAXLEN 2 → only scan first 2 elements (indices 0,1)
    // "b" appears at index 1 within that window
    assert_eq!(c.cmds(&["LPOS", "k", "b", "MAXLEN", "2"]).as_int(), 1);

    // MAXLEN 1 → only scan index 0 ("a"), "b" not found → nil
    assert!(c.cmds(&["LPOS", "k", "b", "MAXLEN", "1"]).is_nil());

    // COUNT 0 + MAXLEN 3 → matches within first 3 elements only
    let got = c.cmds(&["LPOS", "k", "b", "COUNT", "0", "MAXLEN", "3"]);
    let arr = got.as_array();
    assert_eq!(arr.len(), 1); // only index 1 is within first 3
    assert_eq!(arr[0].as_int(), 1);
}

#[test]
fn test_lpos_rank_and_count_combined() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "k", "b", "b", "b", "b", "b"]);
    //  indices:              0    1    2    3    4

    // RANK 2 COUNT 2 → skip first match, return next 2 (indices 1 and 2)
    let got = c.cmds(&["LPOS", "k", "b", "RANK", "2", "COUNT", "2"]);
    let arr = got.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_int(), 2);
}

// ═════════════════════════════════════════════════════════════════════════════
// SET COMMAND TESTS
// ═════════════════════════════════════════════════════════════════════════════

// Manual test scenario:
//   SADD s a b c    → 3
//   SADD s a d      → 1  (a already exists, only d is new)
//   SCARD s         → 4
#[test]
fn test_sadd_scard() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(c.cmds(&["SADD", "s", "a", "b", "c"]).as_int(), 3);
    assert_eq!(c.cmds(&["SADD", "s", "a", "d"]).as_int(), 1);
    assert_eq!(c.cmds(&["SCARD", "s"]).as_int(), 4);
    assert_eq!(c.cmds(&["SCARD", "nosuch"]).as_int(), 0);
}

// Manual test scenario:
//   SADD s a b c
//   SISMEMBER s a    → 1
//   SISMEMBER s z    → 0
//   SMISMEMBER s a b z  → [1, 1, 0]
#[test]
fn test_sismember_smismember() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s", "a", "b", "c"]);
    assert_eq!(c.cmds(&["SISMEMBER", "s", "a"]).as_int(), 1);
    assert_eq!(c.cmds(&["SISMEMBER", "s", "z"]).as_int(), 0);
    assert_eq!(c.cmds(&["SISMEMBER", "nosuch", "a"]).as_int(), 0);

    let r = c.cmds(&["SMISMEMBER", "s", "a", "b", "z"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_int(), 1);
    assert_eq!(arr[1].as_int(), 1);
    assert_eq!(arr[2].as_int(), 0);
}

// Manual test scenario:
//   SADD s a b c
//   SMEMBERS s     → {"a", "b", "c"}  (order not guaranteed)
#[test]
fn test_smembers() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s", "a", "b", "c"]);
    let r = c.cmds(&["SMEMBERS", "s"]);
    let mut members: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);

    assert_eq!(c.cmds(&["SMEMBERS", "nosuch"]).as_array().len(), 0);
}

// Manual test scenario:
//   SADD s a b c
//   SREM s a z     → 1  (a removed, z didn't exist)
//   SCARD s        → 2
//   SISMEMBER s a  → 0
#[test]
fn test_srem() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s", "a", "b", "c"]);
    assert_eq!(c.cmds(&["SREM", "s", "a", "z"]).as_int(), 1);
    assert_eq!(c.cmds(&["SCARD", "s"]).as_int(), 2);
    assert_eq!(c.cmds(&["SISMEMBER", "s", "a"]).as_int(), 0);
}

// Manual test scenario:
//   SADD s a b c
//   SPOP s         → one of {a, b, c}
//   SCARD s        → 2
//   SPOP s 2       → two distinct members
//   SCARD s        → 0
#[test]
fn test_spop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s", "a", "b", "c"]);

    let popped = c.cmds(&["SPOP", "s"]);
    assert!(["a", "b", "c"].contains(&popped.as_str()));
    assert_eq!(c.cmds(&["SCARD", "s"]).as_int(), 2);

    let popped2 = c.cmds(&["SPOP", "s", "2"]);
    assert_eq!(popped2.as_array().len(), 2);
    assert_eq!(c.cmds(&["SCARD", "s"]).as_int(), 0);
}

// Manual test scenario:
//   SADD s a b c
//   SRANDMEMBER s         → one of {a, b, c}
//   SRANDMEMBER s 2       → 2 distinct members
//   SRANDMEMBER s -5      → 5 members (may repeat)
//   SRANDMEMBER nosuch    → nil
#[test]
fn test_srandmember() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s", "a", "b", "c"]);

    let r = c.cmds(&["SRANDMEMBER", "s"]);
    assert!(["a", "b", "c"].contains(&r.as_str()));

    let r2 = c.cmds(&["SRANDMEMBER", "s", "2"]);
    assert_eq!(r2.as_array().len(), 2);

    // negative count — repetitions allowed, must return exactly 5
    let r3 = c.cmds(&["SRANDMEMBER", "s", "-5"]);
    assert_eq!(r3.as_array().len(), 5);

    assert!(c.cmds(&["SRANDMEMBER", "nosuch"]).is_nil());
}

// Manual test scenario:
//   SADD src a b c
//   SMOVE src dst b   → 1
//   SISMEMBER src b   → 0
//   SISMEMBER dst b   → 1
//   SMOVE src dst z   → 0  (z not in src)
#[test]
fn test_smove() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "src", "a", "b", "c"]);
    assert_eq!(c.cmds(&["SMOVE", "src", "dst", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["SISMEMBER", "src", "b"]).as_int(), 0);
    assert_eq!(c.cmds(&["SISMEMBER", "dst", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["SMOVE", "src", "dst", "z"]).as_int(), 0);
}

// Manual test scenario:
//   SADD s1 a b c
//   SADD s2 b c d
//   SDIFF s1 s2     → {"a"}
//   SDIFF s2 s1     → {"d"}
//   SDIFFSTORE dst s1 s2  → 1
//   SMEMBERS dst          → {"a"}
#[test]
fn test_sdiff_sdiffstore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s1", "a", "b", "c"]);
    c.cmds(&["SADD", "s2", "b", "c", "d"]);

    let r = c.cmds(&["SDIFF", "s1", "s2"]);
    let mut diff: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    diff.sort();
    assert_eq!(diff, vec!["a"]);

    let r2 = c.cmds(&["SDIFF", "s2", "s1"]);
    let mut diff2: Vec<&str> = r2.as_array().iter().map(|v| v.as_str()).collect();
    diff2.sort();
    assert_eq!(diff2, vec!["d"]);

    assert_eq!(c.cmds(&["SDIFFSTORE", "dst", "s1", "s2"]).as_int(), 1);
    let binding = c.cmds(&["SMEMBERS", "dst"]);
    let stored: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(stored, vec!["a"]);
}

// Manual test scenario:
//   SADD s1 a b c
//   SADD s2 b c d
//   SINTER s1 s2         → {"b", "c"}
//   SINTERCARD 2 s1 s2   → 2
//   SINTERCARD 2 s1 s2 LIMIT 1  → 1
//   SINTERSTORE dst s1 s2       → 2
#[test]
fn test_sinter_sinterstore_sintercard() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s1", "a", "b", "c"]);
    c.cmds(&["SADD", "s2", "b", "c", "d"]);

    let r = c.cmds(&["SINTER", "s1", "s2"]);
    let mut inter: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    inter.sort();
    assert_eq!(inter, vec!["b", "c"]);

    assert_eq!(c.cmds(&["SINTERCARD", "2", "s1", "s2"]).as_int(), 2);
    assert_eq!(
        c.cmds(&["SINTERCARD", "2", "s1", "s2", "LIMIT", "1"])
            .as_int(),
        1
    );

    assert_eq!(c.cmds(&["SINTERSTORE", "dst", "s1", "s2"]).as_int(), 2);
    let binding = c.cmds(&["SMEMBERS", "dst"]);
    let stored: Vec<&str> = binding.as_array().iter().map(|v| v.as_str()).collect();
    let mut stored_sorted = stored.clone();
    stored_sorted.sort();
    assert_eq!(stored_sorted, vec!["b", "c"]);
}

// Manual test scenario:
//   SADD s1 a b
//   SADD s2 c d
//   SUNION s1 s2           → {"a", "b", "c", "d"}
//   SUNIONSTORE dst s1 s2  → 4
//   SCARD dst              → 4
#[test]
fn test_sunion_sunionstore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s1", "a", "b"]);
    c.cmds(&["SADD", "s2", "c", "d"]);

    let r = c.cmds(&["SUNION", "s1", "s2"]);
    let mut union: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    union.sort();
    assert_eq!(union, vec!["a", "b", "c", "d"]);

    assert_eq!(c.cmds(&["SUNIONSTORE", "dst", "s1", "s2"]).as_int(), 4);
    assert_eq!(c.cmds(&["SCARD", "dst"]).as_int(), 4);
}

// Manual test scenario:
//   SADD s apple apricot banana
//   SSCAN s 0              → cursor=0, all 3 members
//   SSCAN s 0 MATCH a*     → cursor=0, ["apple", "apricot"]
//   SSCAN s 0 COUNT 1      → cursor wraps, partial results
#[test]
fn test_sscan() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SADD", "s", "apple", "apricot", "banana"]);

    // Full scan
    let r = c.cmds(&["SSCAN", "s", "0"]);
    let parts = r.as_array();
    assert_eq!(parts[0].as_str(), "0"); // cursor back to 0 = complete
    assert_eq!(parts[1].as_array().len(), 3);

    // MATCH filter
    let r2 = c.cmds(&["SSCAN", "s", "0", "MATCH", "a*"]);
    let parts2 = r2.as_array();
    let mut matched: Vec<&str> = parts2[1].as_array().iter().map(|v| v.as_str()).collect();
    matched.sort();
    assert_eq!(matched, vec!["apple", "apricot"]);
}

// Manual test scenario:
//   WRONGTYPE: SADD on a key that holds a string
//   WRONGTYPE: SCARD on a list key
#[test]
fn test_set_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "hello"]);
    assert!(c.cmds(&["SADD", "strkey", "val"]).is_error());
    assert!(c.cmds(&["SMEMBERS", "strkey"]).is_error());

    c.cmds(&["RPUSH", "listkey", "a"]);
    assert!(c.cmds(&["SCARD", "listkey"]).is_error());
}

// Manual test scenario:
//   TYPE command reflects correct type for lists and sets
#[test]
fn test_type_command_list_and_set() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["RPUSH", "mylist", "a"]);
    assert_eq!(c.cmds(&["TYPE", "mylist"]).as_str(), "list");

    c.cmds(&["SADD", "myset", "a"]);
    assert_eq!(c.cmds(&["TYPE", "myset"]).as_str(), "set");
}

#[test]
fn test_brpop_pops_from_tail() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // List already has elements — fast path executes, must pop from tail
    c.cmds(&["RPUSH", "l", "first", "second", "third"]);
    let r = c.cmds(&["BRPOP", "l", "1"]);
    let parts = r.as_array();
    assert_eq!(parts[0].as_str(), "l");
    assert_eq!(parts[1].as_str(), "third"); // tail, not head
}

/// A set, hash or sorted set small enough for Redis's compact encodings
/// (intset, listpack) is scanned whole: every element in one reply, cursor
/// 0, whatever COUNT says. BullMQ's dependency pagination relies on it. A
/// larger one is still paged.
#[test]
fn test_scan_of_a_small_collection_returns_it_whole() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let members: Vec<String> = (0..65).map(|i| format!("job:{i}")).collect();
    let mut sadd = vec!["SADD", "small:set"];
    sadd.extend(members.iter().map(String::as_str));
    c.cmds(&sadd);
    let mut hset = vec!["HSET", "small:hash"];
    let mut zadd = vec!["ZADD", "small:zset"];
    for m in &members {
        hset.extend([m.as_str(), "v"]);
        zadd.extend(["1", m.as_str()]);
    }
    c.cmds(&hset);
    c.cmds(&zadd);

    for (cmd, key, per_member) in [
        ("SSCAN", "small:set", 1),
        ("HSCAN", "small:hash", 2),
        ("ZSCAN", "small:zset", 2),
    ] {
        let r = c.cmds(&[cmd, key, "0", "COUNT", "10"]);
        let parts = r.as_array();
        assert_eq!(parts[0].as_str(), "0", "{cmd}: one page, cursor 0");
        assert_eq!(
            parts[1].as_array().len(),
            65 * per_member,
            "{cmd}: every element"
        );
    }

    // Past what a compact encoding holds (128 members), COUNT pages again.
    let big: Vec<String> = (0..300).map(|i| format!("m{i}")).collect();
    let mut sadd = vec!["SADD", "big:set"];
    sadd.extend(big.iter().map(String::as_str));
    c.cmds(&sadd);
    let r = c.cmds(&["SSCAN", "big:set", "0", "COUNT", "10"]);
    let parts = r.as_array();
    assert_ne!(parts[0].as_str(), "0", "a large set is paged");
    assert!(parts[1].as_array().len() < 300);
}

/// `RPUSH` onto a list whose time to live has passed, before active expiry
/// reaps it, makes a new list without one. It pushed onto the old list,
/// which then went, with the new element, at its old deadline.
#[test]
fn test_rpush_onto_an_expired_list_starts_fresh_and_stays() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let keys: Vec<String> = (0..10).map(|i| format!("queue:{i}")).collect();
    for key in &keys {
        assert_eq!(c.cmds(&["RPUSH", key, "a"]).as_int(), 1);
        assert_eq!(c.cmds(&["PEXPIRE", key, "20"]).as_int(), 1);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(c.cmds(&["RPUSH", key, "b"]).as_int(), 1, "RPUSH {key}");
    }
    thread::sleep(Duration::from_millis(300));
    for key in &keys {
        let items = c.cmds(&["LRANGE", key, "0", "-1"]);
        let items: Vec<&str> = items.as_array().iter().map(|v| v.as_str()).collect();
        assert_eq!(items, vec!["b"], "LRANGE {key}");
        assert_eq!(c.cmds(&["TTL", key]).as_int(), -1, "TTL {key}");
    }
}

/// `SORT ... STORE` from a list whose time to live has passed sorts nothing,
/// as from a missing key: the destination goes and the reply is 0. It
/// answered with an empty array and left the destination as it was.
#[test]
fn test_sort_store_from_an_expired_list_empties_the_destination() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    for i in 0..10 {
        let (src, dst) = (format!("sort:src:{i}"), format!("sort:dst:{i}"));
        assert_eq!(c.cmds(&["RPUSH", &src, "3", "1", "2"]).as_int(), 3);
        assert_eq!(c.cmds(&["RPUSH", &dst, "old"]).as_int(), 1);
        assert_eq!(c.cmds(&["PEXPIRE", &src, "20"]).as_int(), 1);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(c.cmds(&["SORT", &src, "STORE", &dst]).as_int(), 0, "{src}");
        assert_eq!(c.cmds(&["EXISTS", &dst]).as_int(), 0, "{dst}");
        assert_eq!(c.cmds(&["SORT", &src]).as_array().len(), 0);
    }
}
