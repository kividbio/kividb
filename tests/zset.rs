// Integration tests for Sorted Set (ZSET) commands.
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
    fn as_f64(&self) -> f64 {
        self.as_str().parse().unwrap()
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

// ═══════════════════════════════════════════════════════════════════════════════
// ZADD / ZCARD
// ═══════════════════════════════════════════════════════════════════════════════

// ZADD key score member [score member ...]
//   ZADD z 1 a 2 b 3 c   → 3  (3 new)
//   ZCARD z              → 3
//   ZCARD nosuch         → 0
#[test]
fn test_zadd_zcard_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert_eq!(
        c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"])
            .as_int(),
        3
    );
    assert_eq!(c.cmds(&["ZCARD", "z"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZCARD", "nosuch"]).as_int(), 0);
}

// ZADD with NX: only add new members, never update existing
//   ZADD z 1 a            → 1
//   ZADD z NX 99 a 2 b   → 1  (b added, a score unchanged)
//   ZSCORE z a            → "1"
#[test]
fn test_zadd_nx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a"]);
    assert_eq!(
        c.cmds(&["ZADD", "z", "NX", "99", "a", "2", "b"]).as_int(),
        1
    );
    assert_eq!(c.cmds(&["ZSCORE", "z", "a"]).as_f64(), 1.0);
    assert_eq!(c.cmds(&["ZSCORE", "z", "b"]).as_f64(), 2.0);
}

// ZADD with XX: only update existing, never add new
//   ZADD z 1 a            → 1
//   ZADD z XX 99 a 2 b   → 0  (a updated, b NOT added)
//   ZSCORE z a            → "99"
//   ZCARD z               → 1
#[test]
fn test_zadd_xx() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a"]);
    assert_eq!(
        c.cmds(&["ZADD", "z", "XX", "99", "a", "2", "b"]).as_int(),
        0
    );
    assert_eq!(c.cmds(&["ZSCORE", "z", "a"]).as_f64(), 99.0);
    assert!(c.cmds(&["ZSCORE", "z", "b"]).is_nil());
    assert_eq!(c.cmds(&["ZCARD", "z"]).as_int(), 1);
}

// ZADD with GT: only update if new score > current
//   ZADD z 5 a
//   ZADD z GT 10 a  → updates (10 > 5)
//   ZADD z GT 3 a   → skips  (3 < 10)
#[test]
fn test_zadd_gt_lt() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "5", "a"]);
    c.cmds(&["ZADD", "z", "GT", "10", "a"]);
    assert_eq!(c.cmds(&["ZSCORE", "z", "a"]).as_f64(), 10.0);
    c.cmds(&["ZADD", "z", "GT", "3", "a"]);
    assert_eq!(c.cmds(&["ZSCORE", "z", "a"]).as_f64(), 10.0); // unchanged

    c.cmds(&["ZADD", "z2", "5", "b"]);
    c.cmds(&["ZADD", "z2", "LT", "2", "b"]);
    assert_eq!(c.cmds(&["ZSCORE", "z2", "b"]).as_f64(), 2.0);
    c.cmds(&["ZADD", "z2", "LT", "9", "b"]);
    assert_eq!(c.cmds(&["ZSCORE", "z2", "b"]).as_f64(), 2.0); // unchanged
}

// ZADD with CH: return count of added + updated (not just added)
//   ZADD z 1 a
//   ZADD z CH 2 a 3 b  → 2  (a updated + b added)
#[test]
fn test_zadd_ch() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a"]);
    assert_eq!(c.cmds(&["ZADD", "z", "CH", "2", "a", "3", "b"]).as_int(), 2);
}

// ZADD with INCR: treat score as increment, return new score
//   ZADD z 5 a
//   ZADD z INCR 3 a  → "8"
#[test]
fn test_zadd_incr() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "5", "a"]);
    let r = c.cmds(&["ZADD", "z", "INCR", "3", "a"]);
    assert_eq!(r.as_f64(), 8.0);
}

// ZADD NX+INCR: returns nil if member already exists (NX skips it)
#[test]
fn test_zadd_nx_incr_nil() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "5", "a"]);
    assert!(c.cmds(&["ZADD", "z", "NX", "INCR", "3", "a"]).is_nil());
}

// NX and XX together → error
#[test]
fn test_zadd_nx_xx_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(c.cmds(&["ZADD", "z", "NX", "XX", "1", "a"]).is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZSCORE / ZMSCORE
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zscore_zmscore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1.5", "a", "2.5", "b", "3.5", "c"]);

    assert_eq!(c.cmds(&["ZSCORE", "z", "a"]).as_f64(), 1.5);
    assert_eq!(c.cmds(&["ZSCORE", "z", "b"]).as_f64(), 2.5);
    assert!(c.cmds(&["ZSCORE", "z", "missing"]).is_nil());
    assert!(c.cmds(&["ZSCORE", "nosuch", "a"]).is_nil());

    let r = c.cmds(&["ZMSCORE", "z", "a", "missing", "c"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_f64(), 1.5);
    assert!(arr[1].is_nil());
    assert_eq!(arr[2].as_f64(), 3.5);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZINCRBY
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zincrby() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "10", "a"]);
    assert_eq!(c.cmds(&["ZINCRBY", "z", "5", "a"]).as_f64(), 15.0);
    assert_eq!(c.cmds(&["ZINCRBY", "z", "-3", "a"]).as_f64(), 12.0);
    // ZINCRBY on non-existent member creates it at 0 + increment
    assert_eq!(c.cmds(&["ZINCRBY", "z", "7", "new"]).as_f64(), 7.0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZRANK / ZREVRANK
// ═══════════════════════════════════════════════════════════════════════════════

// z: a=1, b=2, c=3
//   ZRANK z a     → 0  (lowest score = rank 0)
//   ZRANK z c     → 2
//   ZRANK z x     → nil
//   ZREVRANK z a  → 2  (lowest score = highest revrank)
//   ZREVRANK z c  → 0
#[test]
fn test_zrank_zrevrank() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);

    assert_eq!(c.cmds(&["ZRANK", "z", "a"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZRANK", "z", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["ZRANK", "z", "c"]).as_int(), 2);
    assert!(c.cmds(&["ZRANK", "z", "missing"]).is_nil());
    assert!(c.cmds(&["ZRANK", "nosuch", "a"]).is_nil());

    assert_eq!(c.cmds(&["ZREVRANK", "z", "c"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "a"]).as_int(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZREM
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zrem() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
    assert_eq!(c.cmds(&["ZREM", "z", "a", "c", "missing"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZCARD", "z"]).as_int(), 1);
    assert!(c.cmds(&["ZSCORE", "z", "a"]).is_nil());
    assert_eq!(c.cmds(&["ZSCORE", "z", "b"]).as_f64(), 2.0);
    assert_eq!(c.cmds(&["ZREM", "nosuch", "a"]).as_int(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZCOUNT / ZLEXCOUNT
// ═══════════════════════════════════════════════════════════════════════════════

// z: a=1, b=2, c=3, d=4, e=5
//   ZCOUNT z 2 4        → 3  (b,c,d)
//   ZCOUNT z (2 4       → 2  (c,d — exclusive lo)
//   ZCOUNT z -inf +inf  → 5  (all)
//   ZCOUNT z 6 10       → 0  (none)
#[test]
fn test_zcount() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ]);

    assert_eq!(c.cmds(&["ZCOUNT", "z", "2", "4"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZCOUNT", "z", "(2", "4"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZCOUNT", "z", "2", "(4"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZCOUNT", "z", "-inf", "+inf"]).as_int(), 5);
    assert_eq!(c.cmds(&["ZCOUNT", "z", "6", "10"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZCOUNT", "nosuch", "-inf", "+inf"]).as_int(), 0);
}

// ZLEXCOUNT: all members have score 0, count by lex range
//   ZADD z 0 a 0 b 0 c 0 d 0 e
//   ZLEXCOUNT z [b [d  → 3  (b, c, d)
//   ZLEXCOUNT z - +    → 5  (all)
#[test]
fn test_zlexcount() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e",
    ]);

    assert_eq!(c.cmds(&["ZLEXCOUNT", "z", "[b", "[d"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZLEXCOUNT", "z", "(b", "[d"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZLEXCOUNT", "z", "-", "+"]).as_int(), 5);
    assert_eq!(c.cmds(&["ZLEXCOUNT", "z", "[f", "+"]).as_int(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZRANGE (unified)
// ═══════════════════════════════════════════════════════════════════════════════

// Index-based
#[test]
fn test_zrange_by_index() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"]);

    let r = c.cmds(&["ZRANGE", "z", "0", "-1"]);
    let members: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members, vec!["a", "b", "c", "d"]);

    let r2 = c.cmds(&["ZRANGE", "z", "1", "2"]);
    let members2: Vec<&str> = r2.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members2, vec!["b", "c"]);

    // REV reverses output
    let r3 = c.cmds(&["ZRANGE", "z", "0", "-1", "REV"]);
    let members3: Vec<&str> = r3.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members3, vec!["d", "c", "b", "a"]);
}

// WITHSCORES
#[test]
fn test_zrange_withscores() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b"]);
    let r = c.cmds(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[0].as_str(), "a");
    assert_eq!(arr[1].as_f64(), 1.0);
    assert_eq!(arr[2].as_str(), "b");
    assert_eq!(arr[3].as_f64(), 2.0);
}

// BYSCORE mode
#[test]
fn test_zrange_byscore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"]);

    let r = c.cmds(&["ZRANGE", "z", "2", "3", "BYSCORE"]);
    let members: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members, vec!["b", "c"]);

    // Exclusive bounds
    let r2 = c.cmds(&["ZRANGE", "z", "(1", "3", "BYSCORE"]);
    let members2: Vec<&str> = r2.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members2, vec!["b", "c"]);

    // LIMIT offset count
    let r3 = c.cmds(&["ZRANGE", "z", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2"]);
    let members3: Vec<&str> = r3.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members3, vec!["b", "c"]);
}

// BYLEX mode
#[test]
fn test_zrange_bylex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d"]);

    let r = c.cmds(&["ZRANGE", "z", "[b", "[c", "BYLEX"]);
    let members: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members, vec!["b", "c"]);

    let r2 = c.cmds(&["ZRANGE", "z", "-", "+", "BYLEX"]);
    let members2: Vec<&str> = r2.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members2, vec!["a", "b", "c", "d"]);
}

// BYSCORE REV — descending score range
#[test]
fn test_zrange_byscore_rev() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
    // In REV+BYSCORE mode: start=max, stop=min
    let r = c.cmds(&["ZRANGE", "z", "3", "1", "BYSCORE", "REV"]);
    let members: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(members, vec!["c", "b", "a"]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Legacy range aliases
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zrangebyscore_zrevrangebyscore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"]);

    let r = c.cmds(&["ZRANGEBYSCORE", "z", "2", "3"]);
    let m: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m, vec!["b", "c"]);

    let r2 = c.cmds(&["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2"]);
    let m2: Vec<&str> = r2.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m2, vec!["b", "c"]);

    // ZREVRANGEBYSCORE: max before min
    let r3 = c.cmds(&["ZREVRANGEBYSCORE", "z", "3", "2"]);
    let m3: Vec<&str> = r3.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m3, vec!["c", "b"]);
}

#[test]
fn test_zrevrange() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
    let r = c.cmds(&["ZREVRANGE", "z", "0", "-1"]);
    let m: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m, vec!["c", "b", "a"]);
}

#[test]
fn test_zrangebylex_zrevrangebylex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d"]);

    let r = c.cmds(&["ZRANGEBYLEX", "z", "[b", "[c"]);
    let m: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m, vec!["b", "c"]);

    let r2 = c.cmds(&["ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "2"]);
    let m2: Vec<&str> = r2.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m2, vec!["b", "c"]);

    // ZREVRANGEBYLEX: max before min
    let r3 = c.cmds(&["ZREVRANGEBYLEX", "z", "[c", "[a"]);
    let m3: Vec<&str> = r3.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m3, vec!["c", "b", "a"]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZRANGESTORE
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zrangestore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d"]);

    // Store score range 2..3 into dst
    let n = c.cmds(&["ZRANGESTORE", "dst", "src", "2", "3", "BYSCORE"]);
    assert_eq!(n.as_int(), 2);
    assert_eq!(c.cmds(&["ZCARD", "dst"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZSCORE", "dst", "b"]).as_f64(), 2.0);
    assert_eq!(c.cmds(&["ZSCORE", "dst", "c"]).as_f64(), 3.0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZPOPMIN / ZPOPMAX
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zpopmin_zpopmax() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);

    let r = c.cmds(&["ZPOPMIN", "z"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "a");
    assert_eq!(arr[1].as_f64(), 1.0);
    assert_eq!(c.cmds(&["ZCARD", "z"]).as_int(), 2);

    let r2 = c.cmds(&["ZPOPMAX", "z"]);
    let arr2 = r2.as_array();
    assert_eq!(arr2[0].as_str(), "c");
    assert_eq!(arr2[1].as_f64(), 3.0);

    // count arg
    c.cmds(&["ZADD", "z2", "1", "a", "2", "b", "3", "c"]);
    let r3 = c.cmds(&["ZPOPMIN", "z2", "2"]);
    let arr3 = r3.as_array();
    assert_eq!(arr3.len(), 4); // 2 pairs
    assert_eq!(arr3[0].as_str(), "a");
    assert_eq!(arr3[2].as_str(), "b");

    // empty key
    assert_eq!(c.cmds(&["ZPOPMIN", "nosuch"]).as_array().len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Remove-range family
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zremrangebyscore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"]);
    assert_eq!(c.cmds(&["ZREMRANGEBYSCORE", "z", "2", "3"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZCARD", "z"]).as_int(), 2);
    assert!(c.cmds(&["ZSCORE", "z", "b"]).is_nil());
    assert!(c.cmds(&["ZSCORE", "z", "c"]).is_nil());
}

#[test]
fn test_zremrangebyrank() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"]);
    // Remove indices 1 and 2 (b and c)
    assert_eq!(c.cmds(&["ZREMRANGEBYRANK", "z", "1", "2"]).as_int(), 2);
    let r = c.cmds(&["ZRANGE", "z", "0", "-1"]);
    let m: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m, vec!["a", "d"]);
}

#[test]
fn test_zremrangebylex() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d"]);
    assert_eq!(c.cmds(&["ZREMRANGEBYLEX", "z", "[b", "[c"]).as_int(), 2);
    let r = c.cmds(&["ZRANGE", "z", "0", "-1"]);
    let m: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    assert_eq!(m, vec!["a", "d"]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Set operations: ZDIFF / ZINTER / ZUNION
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zdiff_zdiffstore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z1", "1", "a", "2", "b", "3", "c"]);
    c.cmds(&["ZADD", "z2", "1", "b", "2", "d"]);

    // ZDIFF: a and c are in z1 but not z2
    let r = c.cmds(&["ZDIFF", "2", "z1", "z2"]);
    let mut m: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
    m.sort();
    assert_eq!(m, vec!["a", "c"]);

    // ZDIFFSTORE
    assert_eq!(c.cmds(&["ZDIFFSTORE", "dst", "2", "z1", "z2"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZCARD", "dst"]).as_int(), 2);
}

#[test]
fn test_zinter_zinterstore_zintercard() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z1", "1", "a", "2", "b", "3", "c"]);
    c.cmds(&["ZADD", "z2", "10", "b", "20", "c", "30", "d"]);

    // ZINTER: b and c appear in both; scores summed by default
    let r = c.cmds(&["ZINTER", "2", "z1", "z2", "WITHSCORES"]);
    let arr = r.as_array();
    // arr is [member, score, member, score] sorted by score
    assert_eq!(arr.len(), 4);
    // b: 2+10=12, c: 3+20=23
    let mut pairs: Vec<(&str, f64)> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), pair[1].as_f64()))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    assert_eq!(pairs[0], ("b", 12.0));
    assert_eq!(pairs[1], ("c", 23.0));

    // ZINTERCARD
    assert_eq!(c.cmds(&["ZINTERCARD", "2", "z1", "z2"]).as_int(), 2);
    assert_eq!(
        c.cmds(&["ZINTERCARD", "2", "z1", "z2", "LIMIT", "1"])
            .as_int(),
        1
    );

    // ZINTERSTORE
    assert_eq!(c.cmds(&["ZINTERSTORE", "dst", "2", "z1", "z2"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZSCORE", "dst", "b"]).as_f64(), 12.0);
}

#[test]
fn test_zunion_zunionstore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z1", "1", "a", "2", "b"]);
    c.cmds(&["ZADD", "z2", "10", "b", "20", "c"]);

    let r = c.cmds(&["ZUNION", "2", "z1", "z2", "WITHSCORES"]);
    let arr = r.as_array();
    let mut pairs: Vec<(&str, f64)> = arr
        .chunks(2)
        .map(|pair| (pair[0].as_str(), pair[1].as_f64()))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    // a=1, b=2+10=12, c=20
    assert_eq!(pairs[0], ("a", 1.0));
    assert_eq!(pairs[1], ("b", 12.0));
    assert_eq!(pairs[2], ("c", 20.0));

    // AGGREGATE MIN
    let r2 = c.cmds(&["ZUNION", "2", "z1", "z2", "AGGREGATE", "MIN", "WITHSCORES"]);
    let arr2 = r2.as_array();
    let mut pairs2: Vec<(&str, f64)> = arr2
        .chunks(2)
        .map(|pair| (pair[0].as_str(), pair[1].as_f64()))
        .collect();
    pairs2.sort_by(|a, b| a.0.cmp(b.0));
    assert_eq!(pairs2[1], ("b", 2.0)); // min(2, 10) = 2

    assert_eq!(c.cmds(&["ZUNIONSTORE", "dst", "2", "z1", "z2"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZCARD", "dst"]).as_int(), 3);
}

// WEIGHTS option
#[test]
fn test_zinter_weights() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z1", "2", "a"]);
    c.cmds(&["ZADD", "z2", "3", "a"]);

    // WEIGHTS 2 1: z1*2 + z2*1 = 4 + 3 = 7
    let r = c.cmds(&["ZINTER", "2", "z1", "z2", "WEIGHTS", "2", "1", "WITHSCORES"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "a");
    assert_eq!(arr[1].as_f64(), 7.0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZRANDMEMBER
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zrandmember() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);

    // No count — returns single member
    let r = c.cmds(&["ZRANDMEMBER", "z"]);
    assert!(["a", "b", "c"].contains(&r.as_str()));

    // Positive count — distinct members
    let r2 = c.cmds(&["ZRANDMEMBER", "z", "2"]);
    assert_eq!(r2.as_array().len(), 2);

    // Count >= size — returns all members
    let r3 = c.cmds(&["ZRANDMEMBER", "z", "10"]);
    assert_eq!(r3.as_array().len(), 3);

    // Negative count — allows repetition
    let r4 = c.cmds(&["ZRANDMEMBER", "z", "-5"]);
    assert_eq!(r4.as_array().len(), 5);

    // WITHSCORES
    let r5 = c.cmds(&["ZRANDMEMBER", "z", "2", "WITHSCORES"]);
    assert_eq!(r5.as_array().len(), 4); // 2 pairs

    // Missing key
    assert!(c.cmds(&["ZRANDMEMBER", "nosuch"]).is_nil());
    assert_eq!(c.cmds(&["ZRANDMEMBER", "nosuch", "3"]).as_array().len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZSCAN
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zscan() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "apple", "2", "apricot", "3", "banana"]);

    // Full scan
    let r = c.cmds(&["ZSCAN", "z", "0"]);
    let parts = r.as_array();
    assert_eq!(parts[0].as_str(), "0"); // cursor back to 0 = complete
    let items = parts[1].as_array();
    assert_eq!(items.len(), 6); // 3 member+score pairs

    // MATCH filter
    let r2 = c.cmds(&["ZSCAN", "z", "0", "MATCH", "ap*"]);
    let items2 = r2.as_array()[1].as_array();
    // apple and apricot match
    assert_eq!(items2.len(), 4);

    // Missing key
    let r3 = c.cmds(&["ZSCAN", "nosuch", "0"]);
    let parts3 = r3.as_array();
    assert_eq!(parts3[0].as_str(), "0");
    assert_eq!(parts3[1].as_array().len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZMPOP
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zmpop() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);

    let r = c.cmds(&["ZMPOP", "1", "z", "MIN"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "z");
    let popped = arr[1].as_array();
    // Each entry is a [member, score] sub-array
    assert_eq!(popped.len(), 1);
    let pair = popped[0].as_array();
    assert_eq!(pair[0].as_str(), "a");
    assert_eq!(pair[1].as_f64(), 1.0);
    assert_eq!(c.cmds(&["ZCARD", "z"]).as_int(), 2);

    // COUNT 2 — returns 2 pairs (each a sub-array), not 4 flat elements
    let r2 = c.cmds(&["ZMPOP", "1", "z", "MAX", "COUNT", "2"]);
    let arr2 = r2.as_array();
    assert_eq!(arr2[1].as_array().len(), 2);

    // All empty → nil
    assert!(c.cmds(&["ZMPOP", "1", "nosuch", "MIN"]).is_nil());
}

// ═══════════════════════════════════════════════════════════════════════════════
// BZPOPMIN / BZPOPMAX (blocking)
// ═══════════════════════════════════════════════════════════════════════════════

// Fast path: data already present, no blocking
#[test]
fn test_bzpopmin_fast_path() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b"]);
    let r = c.cmds(&["BZPOPMIN", "z", "0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "z");
    assert_eq!(arr[1].as_str(), "a");
    assert_eq!(arr[2].as_f64(), 1.0);
}

#[test]
fn test_bzpopmax_fast_path() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a", "2", "b"]);
    let r = c.cmds(&["BZPOPMAX", "z", "0"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "z");
    assert_eq!(arr[1].as_str(), "b");
    assert_eq!(arr[2].as_f64(), 2.0);
}

// Timeout path: nothing in set
#[test]
fn test_bzpopmin_timeout() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    assert!(c.cmds(&["BZPOPMIN", "empty", "0.1"]).is_nil());
}

// Wakeup path: ZADD arrives after block starts
#[test]
fn test_bzpopmin_wakeup() {
    let port = free_port();
    start_server(port);

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let mut pusher = Conn::connect(port);
        pusher.cmds(&["ZADD", "wake", "42", "hello"]);
    });

    let mut c = Conn::connect(port);
    c.stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let r = c.cmds(&["BZPOPMIN", "wake", "3"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "wake");
    assert_eq!(arr[1].as_str(), "hello");
    assert_eq!(arr[2].as_f64(), 42.0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// WRONGTYPE errors
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zset_wrongtype() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "strkey", "hello"]);
    assert!(c.cmds(&["ZADD", "strkey", "1", "a"]).is_error());
    assert!(c.cmds(&["ZCARD", "strkey"]).is_error());
    assert!(c.cmds(&["ZSCORE", "strkey", "a"]).is_error());
    assert!(c.cmds(&["ZRANGE", "strkey", "0", "-1"]).is_error());
}

// ═══════════════════════════════════════════════════════════════════════════════
// TYPE command
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_type_zset() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1", "a"]);
    assert_eq!(c.cmds(&["TYPE", "z"]).as_str(), "zset");
}

// ═══════════════════════════════════════════════════════════════════════════════
// inf / -inf score handling
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_zadd_inf_scores() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "-inf", "lo", "0", "mid", "+inf", "hi"]);
    let r = c.cmds(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]);
    let arr = r.as_array();
    assert_eq!(arr[0].as_str(), "lo");
    assert_eq!(arr[1].as_str(), "-inf");
    assert_eq!(arr[4].as_str(), "hi");
    assert_eq!(arr[5].as_str(), "inf");

    // ZCOUNT with inf bounds
    assert_eq!(c.cmds(&["ZCOUNT", "z", "-inf", "+inf"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZCOUNT", "z", "-inf", "0"]).as_int(), 2);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Collect member names from a flat RESP array (ZRANGE output without WITHSCORES).
fn members(r: &RespValue) -> Vec<&str> {
    r.as_array().iter().map(|v| v.as_str()).collect()
}

/// Collect (member, score) pairs from a WITHSCORES RESP array.
fn members_scores(r: &RespValue) -> Vec<(&str, f64)> {
    let arr = r.as_array();
    assert_eq!(arr.len() % 2, 0, "WITHSCORES array must have even length");
    arr.chunks(2)
        .map(|c| (c[0].as_str(), c[1].as_f64()))
        .collect()
}

// ═════════════════════════════════════════════════════════════════════════════
// 1. ZRANK / ZREVRANK — correct rank values, WITHSCORE, missing member
// ═════════════════════════════════════════════════════════════════════════════

// Scenario:
//   ZADD z 1 a  2 b  3 c  4 d  5 e
//   ZRANK z a   → 0  (lowest score)
//   ZRANK z c   → 2
//   ZRANK z e   → 4  (highest score)
//   ZREVRANK z e → 0  (highest = rank 0 in reverse)
//   ZREVRANK z a → 4
//   ZRANK z nosuch → nil
#[test]
fn test_zrank_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ]);

    assert_eq!(c.cmds(&["ZRANK", "z", "a"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZRANK", "z", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["ZRANK", "z", "c"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZRANK", "z", "d"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZRANK", "z", "e"]).as_int(), 4);

    assert_eq!(c.cmds(&["ZREVRANK", "z", "e"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "d"]).as_int(), 1);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "c"]).as_int(), 2);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "b"]).as_int(), 3);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "a"]).as_int(), 4);

    assert!(c.cmds(&["ZRANK", "z", "nosuch"]).is_nil());
    assert!(c.cmds(&["ZREVRANK", "z", "nosuch"]).is_nil());
    assert!(c.cmds(&["ZRANK", "nokey", "a"]).is_nil());
}

// Scenario: WITHSCORE flag returns [rank, score] array
//   ZADD z 1.5 a  3.7 b
//   ZRANK z a WITHSCORE    → [0, "1.5"]
//   ZREVRANK z a WITHSCORE → [1, "1.5"]
#[test]
fn test_zrank_withscore() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "1.5", "a", "3.7", "b"]);

    let r = c.cmds(&["ZRANK", "z", "a", "WITHSCORE"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_int(), 0);
    assert!((arr[1].as_f64() - 1.5).abs() < 1e-9);

    let r2 = c.cmds(&["ZREVRANK", "z", "a", "WITHSCORE"]);
    let arr2 = r2.as_array();
    assert_eq!(arr2.len(), 2);
    assert_eq!(arr2[0].as_int(), 1); // a is last in reverse order
    assert!((arr2[1].as_f64() - 1.5).abs() < 1e-9);
}

// Scenario: equal scores — members with same score ordered lexicographically
//   ZADD z 5 b  5 a  5 c
//   Lex order: a < b < c → ranks 0, 1, 2
#[test]
fn test_zrank_equal_scores_lex_order() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "5", "b", "5", "a", "5", "c"]);

    assert_eq!(c.cmds(&["ZRANK", "z", "a"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZRANK", "z", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["ZRANK", "z", "c"]).as_int(), 2);

    assert_eq!(c.cmds(&["ZREVRANK", "z", "c"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "b"]).as_int(), 1);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "a"]).as_int(), 2);
}

// Scenario: single-member set
//   ZADD z 42 only
//   ZRANK z only    → 0
//   ZREVRANK z only → 0
#[test]
fn test_zrank_single_member() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["ZADD", "z", "42", "only"]);
    assert_eq!(c.cmds(&["ZRANK", "z", "only"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZREVRANK", "z", "only"]).as_int(), 0);
}

// Scenario: large set — rank is consistent with ZRANGE output
//   ZADD z 1 m1  2 m2  ...  100 m100
//   ZRANK z m50 → 49  (0-based)
//   ZREVRANK z m50 → 50
#[test]
fn test_zrank_large_set_consistency() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Build ZADD command with 100 members
    let mut args = vec!["ZADD", "bigz"];
    let scores_and_members: Vec<String> = (1..=100)
        .flat_map(|i| vec![i.to_string(), format!("m{i:03}")])
        .collect();
    for s in &scores_and_members {
        args.push(s.as_str());
    }
    c.cmds(&args);

    // m050 has score 50, rank should be 49 (0-based, 49 members with lower scores)
    assert_eq!(c.cmds(&["ZRANK", "bigz", "m050"]).as_int(), 49);
    assert_eq!(c.cmds(&["ZREVRANK", "bigz", "m050"]).as_int(), 50);

    // First and last
    assert_eq!(c.cmds(&["ZRANK", "bigz", "m001"]).as_int(), 0);
    assert_eq!(c.cmds(&["ZRANK", "bigz", "m100"]).as_int(), 99);
    assert_eq!(c.cmds(&["ZREVRANK", "bigz", "m001"]).as_int(), 99);
    assert_eq!(c.cmds(&["ZREVRANK", "bigz", "m100"]).as_int(), 0);
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. ZRANGESTORE — LIMIT applied during iteration, not after full collection
// ═════════════════════════════════════════════════════════════════════════════

// Scenario: BYSCORE with LIMIT offset count
//   ZADD src 1 a  2 b  3 c  4 d  5 e
//   ZRANGESTORE dst src 1 5 BYSCORE LIMIT 1 2
//   → stores b, c (skip 1, take 2 from score range [1,5])
//   ZRANGE dst 0 -1 WITHSCORES → [b 2, c 3]
#[test]
fn test_zrangestore_byscore_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ]);

    let stored = c.cmds(&[
        "ZRANGESTORE",
        "dst",
        "src",
        "1",
        "5",
        "BYSCORE",
        "LIMIT",
        "1",
        "2",
    ]);
    assert_eq!(stored.as_int(), 2);

    let result = c.cmds(&["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    let pairs = members_scores(&result);
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].0, "b");
    assert!((pairs[0].1 - 2.0).abs() < 1e-9);
    assert_eq!(pairs[1].0, "c");
    assert!((pairs[1].1 - 3.0).abs() < 1e-9);
}

// Scenario: BYSCORE REV with LIMIT
//   ZADD src 1 a  2 b  3 c  4 d  5 e
//   ZRANGESTORE dst src 5 1 BYSCORE REV LIMIT 0 3
//   → stores e, d, c  (descending, skip 0, take 3)
#[test]
fn test_zrangestore_byscore_rev_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ]);

    let stored = c.cmds(&[
        "ZRANGESTORE",
        "dst",
        "src",
        "5",
        "1",
        "BYSCORE",
        "REV",
        "LIMIT",
        "0",
        "3",
    ]);
    assert_eq!(stored.as_int(), 3);

    // dst is stored in ascending order regardless
    let result = c.cmds(&["ZRANGE", "dst", "0", "-1", "WITHSCORES"]);
    let pairs = members_scores(&result);
    assert_eq!(pairs.len(), 3);
    let stored_members: Vec<&str> = pairs.iter().map(|p| p.0).collect();
    // c, d, e in ascending score order
    assert_eq!(stored_members, vec!["c", "d", "e"]);
}

// Scenario: BYLEX with LIMIT
//   ZADD src 0 a  0 b  0 c  0 d  0 e  (all same score, lex order)
//   ZRANGESTORE dst src [b [e BYLEX LIMIT 0 2
//   → stores b, c
#[test]
fn test_zrangestore_bylex_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "src", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e",
    ]);

    let stored = c.cmds(&[
        "ZRANGESTORE",
        "dst",
        "src",
        "[b",
        "[e",
        "BYLEX",
        "LIMIT",
        "0",
        "2",
    ]);
    assert_eq!(stored.as_int(), 2);

    let result = c.cmds(&["ZRANGE", "dst", "0", "-1"]);
    assert_eq!(members(&result), vec!["b", "c"]);
}

// Scenario: BYLEX REV with LIMIT — offset into descending lex range
//   ZADD src 0 a  0 b  0 c  0 d  0 e
//   ZRANGESTORE dst src [e [a BYLEX REV LIMIT 1 2
//   → descending from e: e d c b a — skip 1 (e), take 2 → d, c
#[test]
fn test_zrangestore_bylex_rev_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "src", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e",
    ]);

    let stored = c.cmds(&[
        "ZRANGESTORE",
        "dst",
        "src",
        "[e",
        "[a",
        "BYLEX",
        "REV",
        "LIMIT",
        "1",
        "2",
    ]);
    assert_eq!(stored.as_int(), 2);

    let result = c.cmds(&["ZRANGE", "dst", "0", "-1"]);
    let m = members(&result);
    // Stored in ascending order: c, d
    assert_eq!(m, vec!["c", "d"]);
}

// Scenario: LIMIT with count -1 means unlimited — same as no LIMIT
//   ZADD src 1 a  2 b  3 c  4 d  5 e
//   ZRANGESTORE dst src 1 5 BYSCORE LIMIT 2 -1
//   → skip 2, take all remaining → c, d, e
#[test]
fn test_zrangestore_limit_negative_count() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ]);

    let stored = c.cmds(&[
        "ZRANGESTORE",
        "dst",
        "src",
        "1",
        "5",
        "BYSCORE",
        "LIMIT",
        "2",
        "-1",
    ]);
    assert_eq!(stored.as_int(), 3);

    let result = c.cmds(&["ZRANGE", "dst", "0", "-1"]);
    assert_eq!(members(&result), vec!["c", "d", "e"]);
}

// Scenario: LIMIT offset beyond range → empty result
//   ZADD src 1 a  2 b  3 c
//   ZRANGESTORE dst src 1 3 BYSCORE LIMIT 10 5  → 0 (offset past end)
#[test]
fn test_zrangestore_limit_offset_beyond_range() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Use unique src/dst names to avoid state from parallel tests
    c.cmds(&["ZADD", "src_oob", "1", "a", "2", "b", "3", "c"]);

    let stored = c.cmds(&[
        "ZRANGESTORE",
        "dst_oob",
        "src_oob",
        "1",
        "3",
        "BYSCORE",
        "LIMIT",
        "10",
        "5",
    ]);
    assert_eq!(stored.as_int(), 0);
    // When ZRANGESTORE stores 0 elements it should not create the destination key
    assert_eq!(c.cmds(&["EXISTS", "dst_oob"]).as_int(), 0);
}

// Scenario: exclusive score bounds with LIMIT
//   ZADD src 1 a  2 b  3 c  4 d  5 e
//   ZRANGESTORE dst src (1 (5 BYSCORE LIMIT 1 2
//   → in-range: b(2) c(3) d(4) — skip 1 → c, d
#[test]
fn test_zrangestore_byscore_exclusive_bounds_limit() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&[
        "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
    ]);

    let stored = c.cmds(&[
        "ZRANGESTORE",
        "dst",
        "src",
        "(1",
        "(5",
        "BYSCORE",
        "LIMIT",
        "1",
        "2",
    ]);
    assert_eq!(stored.as_int(), 2);

    let result = c.cmds(&["ZRANGE", "dst", "0", "-1"]);
    assert_eq!(members(&result), vec!["c", "d"]);
}

/// The members of a reply, as strings.
fn names(reply: &RespValue) -> Vec<String> {
    reply
        .as_array()
        .iter()
        .map(|v| v.as_str().to_string())
        .collect()
}

/// `ZRANGE` by index, forwards and backwards, for every start and stop of a
/// set of forty: the range is walked to from whichever end is nearer, and
/// each of those four ways must give what counting by hand gives.
#[test]
fn zrange_by_index_is_right_from_either_end() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let n = 40i64;
    let expected: Vec<String> = (0..n).map(|i| format!("m{i:02}")).collect();
    for (i, member) in expected.iter().enumerate() {
        c.cmds(&["ZADD", "z", &i.to_string(), member]);
    }
    let by_hand = |start: i64, stop: i64, rev: bool| -> Vec<String> {
        let order: Vec<String> = if rev {
            expected.iter().rev().cloned().collect()
        } else {
            expected.clone()
        };
        let start = if start < 0 { (n + start).max(0) } else { start };
        let stop = if stop < 0 { n + stop } else { stop }.min(n - 1);
        if start > stop || start >= n {
            return vec![];
        }
        order[start as usize..=stop as usize].to_vec()
    };
    for start in -45..45i64 {
        for stop in [-45, -40, -21, -20, -1, 0, 1, 19, 20, 21, 38, 39, 40, 99] {
            let (s, e) = (start.to_string(), stop.to_string());
            assert_eq!(
                names(&c.cmds(&["ZRANGE", "z", &s, &e])),
                by_hand(start, stop, false),
                "ZRANGE {start} {stop}"
            );
            assert_eq!(
                names(&c.cmds(&["ZRANGE", "z", &s, &e, "REV"])),
                by_hand(start, stop, true),
                "ZRANGE {start} {stop} REV"
            );
            assert_eq!(
                names(&c.cmds(&["ZREVRANGE", "z", &s, &e])),
                by_hand(start, stop, true),
                "ZREVRANGE {start} {stop}"
            );
        }
    }
    let reply = c.cmds(&["ZRANGE", "z", "38", "39", "WITHSCORES"]);
    assert_eq!(names(&reply), ["m38", "38", "m39", "39"]);
}

/// A range of scores begins at its first score and ends at its last, with
/// either end kept or left out, with infinities, and with several members
/// to a score.
#[test]
fn zrangebyscore_keeps_and_leaves_out_its_ends() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    // Three members at each of the scores -2, -1, 0, 1, 2, and one at each
    // infinity.
    let mut all: Vec<(f64, String)> = Vec::new();
    for score in -2..=2i32 {
        for which in ["a", "b", "c"] {
            let member = format!("{which}{score}");
            c.cmds(&["ZADD", "z", &score.to_string(), &member]);
            all.push((score as f64, member));
        }
    }
    c.cmds(&["ZADD", "z", "-inf", "low", "+inf", "high"]);
    all.push((f64::NEG_INFINITY, "low".into()));
    all.push((f64::INFINITY, "high".into()));
    all.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap().then(x.1.cmp(&y.1)));

    let bound = |text: &str| -> (f64, bool) {
        let (text, open) = match text.strip_prefix('(') {
            Some(rest) => (rest, true),
            None => (text, false),
        };
        let value = match text {
            "-inf" => f64::NEG_INFINITY,
            "+inf" | "inf" => f64::INFINITY,
            other => other.parse().unwrap(),
        };
        (value, open)
    };
    let by_hand = |min: &str, max: &str| -> Vec<String> {
        let ((lo, lo_open), (hi, hi_open)) = (bound(min), bound(max));
        all.iter()
            .filter(|(score, _)| if lo_open { *score > lo } else { *score >= lo })
            .filter(|(score, _)| if hi_open { *score < hi } else { *score <= hi })
            .map(|(_, member)| member.clone())
            .collect()
    };
    let ends = [
        "-inf", "(-inf", "-2", "(-2", "-1", "(-1", "0", "(0", "0.5", "1", "(1", "2", "(2", "+inf",
        "(+inf",
    ];
    for min in ends {
        for max in ends {
            let want = by_hand(min, max);
            assert_eq!(
                names(&c.cmds(&["ZRANGEBYSCORE", "z", min, max])),
                want,
                "ZRANGEBYSCORE {min} {max}"
            );
            let mut backwards = want.clone();
            backwards.reverse();
            assert_eq!(
                names(&c.cmds(&["ZREVRANGEBYSCORE", "z", max, min])),
                backwards,
                "ZREVRANGEBYSCORE {max} {min}"
            );
            let limited: Vec<String> = want.iter().skip(1).take(4).cloned().collect();
            assert_eq!(
                names(&c.cmds(&["ZRANGEBYSCORE", "z", min, max, "LIMIT", "1", "4"])),
                limited,
                "ZRANGEBYSCORE {min} {max} LIMIT 1 4"
            );
            assert_eq!(
                c.cmds(&["ZCOUNT", "z", min, max]).as_int() as usize,
                want.len(),
                "ZCOUNT {min} {max}"
            );
        }
    }
}
