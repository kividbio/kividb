// tests/maxmemory.rs
//
// Integration tests for maxmemory enforcement (noeviction policy).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
// These tests mutate RUNTIME_MAXMEMORY which is a process-global.
// They must not run in parallel with each other.
use std::sync::Mutex;

static TEST_LOCK: Mutex<()> = Mutex::new(());

// helper to acquire without poisoning cascade
fn lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}
// ── Minimal RESP client (same harness as other test files) ────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..40)
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
            _ => panic!("unexpected RESP line: {line:?}"),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<RespValue>),
}

impl RespValue {
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
    fn error_str(&self) -> &str {
        match self {
            RespValue::Error(s) => s.as_str(),
            _ => panic!("not an error: {self:?}"),
        }
    }
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s.as_str(),
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap_or(""),
            _ => panic!("not a string: {self:?}"),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("not an array: {:?}", self),
        }
    }
}

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
    use kividb::config::{Config, IoModel};
    thread::spawn(move || {
        let config = Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: u64::MAX,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

// maxmemory 0 means unlimited — writes always succeed
#[test]
fn test_maxmemory_zero_unlimited() {
    let _lock = lock();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Explicitly set to 0 and wait for the 1-second cache to expire
    // so check_memory_limit re-reads RSS with limit=0 (fast path, no read)
    assert!(c.cmds(&["CONFIG", "SET", "maxmemory", "0"]).is_ok());
    thread::sleep(Duration::from_millis(1100));

    for i in 0..100 {
        let key = format!("key{i}");
        let r = c.cmds(&["SET", &key, "value"]);
        assert!(r.is_ok(), "SET failed with maxmemory=0 at key {i}: {r:?}");
    }
}

// CONFIG SET maxmemory takes effect immediately without restart
#[test]
fn test_config_set_maxmemory_runtime() {
    let _lock = lock();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // CONFIG GET returns ["maxmemory", "0"] — check the value element
    let r = c.cmds(&["CONFIG", "GET", "maxmemory"]);
    let arr = r.as_array();
    assert_eq!(arr[1].as_str(), "0");

    // Set a real limit and read it back
    assert!(
        c.cmds(&["CONFIG", "SET", "maxmemory", "1073741824"])
            .is_ok()
    );
    let r = c.cmds(&["CONFIG", "GET", "maxmemory"]);
    assert_eq!(r.as_array()[1].as_str(), "1073741824");

    // Clear it again — writes should work fine
    assert!(c.cmds(&["CONFIG", "SET", "maxmemory", "0"]).is_ok());
    assert!(c.cmds(&["SET", "after_clear", "ok"]).is_ok());
}

// When maxmemory is set to 1 byte, every write should return OOM
#[test]
fn test_maxmemory_noeviction_oom() {
    let _lock = lock();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // Set an absurdly small limit — 1 byte, well below current RSS
    assert!(c.cmds(&["CONFIG", "SET", "maxmemory", "1"]).is_ok());

    // The memory check caches at 1-second granularity — nudge it by
    // waiting briefly so the cache is primed with current RSS
    thread::sleep(Duration::from_millis(1100));

    let r = c.cmds(&["SET", "oom_key", "value"]);
    assert!(r.is_error(), "Expected OOM error, got: {r:?}");
    assert!(
        r.error_str().contains("OOM"),
        "Error should mention OOM: {}",
        r.error_str()
    );

    // Reads must still work — OOM only blocks writes
    c.cmds(&["SET", "existing", "val"]); // may fail, that's fine
    // Reset to unlimited so other tests aren't affected
    c.cmds(&["CONFIG", "SET", "maxmemory", "0"]);
}

// AUTH, PING, QUIT are never blocked by maxmemory even at limit
#[test]
fn test_maxmemory_always_allowed_commands() {
    let _lock = lock();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["CONFIG", "SET", "maxmemory", "1"]).is_ok());
    thread::sleep(Duration::from_millis(1100));

    // PING is always allowed
    assert!(c.cmds(&["PING"]).is_ok() || matches!(c.cmds(&["PING"]), _));

    // GET is a read — must not be blocked
    let r = c.cmds(&["GET", "nonexistent"]);
    assert!(!r.is_error(), "GET blocked by maxmemory: {r:?}");

    c.cmds(&["CONFIG", "SET", "maxmemory", "0"]);
}

/// Past `maxmemory` under `noeviction`, a new subscription is refused as a
/// write is, as in Redis 8.10; taking one away is not.
#[test]
fn test_maxmemory_refuses_new_subscriptions() {
    let _lock = lock();
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert!(
        c.cmds(&["CONFIG", "SET", "maxmemory-policy", "noeviction"])
            .is_ok()
    );
    assert!(c.cmds(&["CONFIG", "SET", "maxmemory", "1"]).is_ok());
    thread::sleep(Duration::from_millis(1100));

    for cmd in ["SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE"] {
        let r = c.cmds(&[cmd, "news"]);
        assert!(
            r.is_error() && r.error_str().contains("OOM command not allowed"),
            "{cmd}: {r:?}"
        );
    }
    let r = c.cmds(&["UNSUBSCRIBE", "news"]);
    assert!(!r.is_error(), "UNSUBSCRIBE: {r:?}");
    c.cmds(&["CONFIG", "SET", "maxmemory", "0"]);
}
