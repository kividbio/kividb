// A function library on a replica, in a test binary of its own.
//
// The function libraries are process-global, and every in-process server
// shares them: a master and its replica here, and every server of every
// other test in the same binary. A replica's full sync loads its master's
// libraries in place of the loaded ones, so in tests/replication.rs, where
// dozens of replicas sync in parallel, another test's full sync, captured
// before this test's FUNCTION LOAD and loaded after it, took the library
// away again, and the replica never had it (timed out after 10s waiting for
// the replica to have the library). Here nothing else syncs.

#![cfg(feature = "lua")]

use kividb::config::{Config, IoModel};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..100)
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

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
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
    fn is_ok(&self) -> bool {
        matches!(self, RespValue::SimpleStr(s) if s == "OK")
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see tests/replication.rs).
fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

/// A port nothing is listening on, and not one the system hands out to
/// outgoing connections (see tests/replication.rs).
fn free_port() -> u16 {
    in_scratch_dir();
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

/// The servers write their files where the process runs: a directory of
/// its own, not the repository's.
fn in_scratch_dir() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let dir = tempfile::tempdir().expect("a temporary directory").keep();
        std::env::set_current_dir(dir).expect("into the temporary directory");
    });
}

fn spawn_server(mut config: Config) -> u16 {
    let port = free_port();
    config.port = port;
    config.bind = "127.0.0.1".to_string();
    config.threads = 2;
    config.loglevel = "error".to_string();
    config.io_model = IoModel::Tokio;
    config.configfile = "./kividb.conf".to_string();
    thread::spawn(move || kividb::run_for_test(config));
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return port;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server on port {port} did not start listening within 5s");
}

fn replica_of(master_port: u16) -> Config {
    Config {
        replicaof: Some(vec!["127.0.0.1".to_string(), master_port.to_string()]),
        ..Default::default()
    }
}

/// One field of `INFO replication`, if present.
fn repl_info(c: &mut Conn, field: &str) -> Option<String> {
    let raw = c.cmds(&["INFO", "replication"]);
    let text = raw.as_str().to_string();
    text.lines().find_map(|l| {
        l.strip_prefix(field)
            .and_then(|rest| rest.strip_prefix(':'))
            .map(|v| v.trim().to_string())
    })
}

/// Poll `pred` until it holds or `timeout` passes.
fn wait_until(what: &str, timeout: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
}

fn wait_link_up(c: &mut Conn) {
    wait_until("replica link up", Duration::from_secs(10), || {
        repl_info(c, "master_link_status").as_deref() == Some("up")
    });
}

/// A script runs on a replica, and only a write it makes is refused, as in
/// Redis; every EVAL, EVALSHA and FCALL was refused before it ran (#145).
/// One that declares it may write is refused at once; one that declares
/// `no-writes` runs.
#[test]
fn test_scripts_that_read_run_on_a_replica() {
    let master = spawn_server(Config::default());
    let replica = spawn_server(replica_of(master));
    let mut r = Conn::connect(replica);
    wait_link_up(&mut r);
    let mut m = Conn::connect(master);
    assert!(m.cmds(&["SET", "first", "1"]).is_ok());
    assert!(m
        .cmds(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=replica_lib\n\
             redis.register_function{function_name='reads', \
             callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}\n\
             redis.register_function('writes', function(keys) return redis.call('SET', keys[1], 'x') end)",
        ])
        .as_str()
        .contains("replica_lib"));
    wait_until("the replica to have first", Duration::from_secs(10), || {
        r.cmds(&["GET", "first"]) == RespValue::Bulk(b"1".to_vec())
    });
    let refused =
        |reply: &RespValue| matches!(reply, RespValue::Error(e) if e.contains("READONLY"));

    let read = r.cmds(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "first"]);
    assert_eq!(read, RespValue::Bulk(b"1".to_vec()));
    let write = r.cmds(&[
        "EVAL",
        "return redis.call('SET', KEYS[1], 'x')",
        "1",
        "first",
    ]);
    assert!(refused(&write), "{write:?}");
    let declared = r.cmds(&["EVAL", "#!lua\nreturn 1", "0"]);
    assert!(refused(&declared), "{declared:?}");
    let no_writes = r.cmds(&["EVAL", "#!lua flags=no-writes\nreturn 2", "0"]);
    assert_eq!(no_writes, RespValue::Int(2));

    wait_until(
        "the replica to have the library",
        Duration::from_secs(10),
        || r.cmds(&["FCALL", "reads", "1", "first"]) == RespValue::Bulk(b"1".to_vec()),
    );
    let write = r.cmds(&["FCALL", "writes", "1", "first"]);
    assert!(refused(&write), "{write:?}");
    assert_eq!(r.cmds(&["GET", "first"]), RespValue::Bulk(b"1".to_vec()));
}
