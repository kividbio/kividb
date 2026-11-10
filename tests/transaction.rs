// tests/replication_transactions_generic.rs
//
// Integration tests for everything implemented in this session:
//   - Replication commands: ROLE, INFO replication, REPLICAOF, WAIT, WAITAOF
//   - Transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH
//   - Generic commands: RENAME, RENAMENX, COPY, MOVE, SWAPDB, RANDOMKEY
//   - Server commands: FLUSHALL
//   - AUTH / password protection
//   - CLIENT commands: SETNAME, GETNAME, ID, LIST, INFO, NO-EVICT
//
// Each test gets its own server on a free port — fully isolated, run in parallel.

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
        let stream = (0..30)
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
    fn error_msg(&self) -> &str {
        match self {
            RespValue::Error(s) => s,
            _ => panic!("not an error: {:?}", self),
        }
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

/// A server on `port`, on the connection loop `KIVIDB_TEST_IO_MODEL` names
/// (`tokio`, the default, or `uring`).
fn start_server(port: u16) {
    let io_model = match std::env::var("KIVIDB_TEST_IO_MODEL").as_deref() {
        Ok("uring") => kividb::config::IoModel::Uring,
        _ => kividb::config::IoModel::Tokio,
    };
    start_server_on(port, io_model);
}

fn start_server_on(port: u16, io_model: kividb::config::IoModel) {
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: String::new(),
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
// TRANSACTION COMMANDS
// ═════════════════════════════════════════════════════════════════════════════

// Basic MULTI / EXEC — commands are queued then executed atomically
//   MULTI              → OK
//   SET tx:k1 "hello"  → QUEUED
//   SET tx:k2 "world"  → QUEUED
//   EXEC               → [OK, OK]
//   GET tx:k1          → "hello"
#[test]
fn test_multi_exec_basic() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["MULTI"]).is_ok());
    assert_eq!(c.cmds(&["SET", "tx:k1", "hello"]).as_str(), "QUEUED");
    assert_eq!(c.cmds(&["SET", "tx:k2", "world"]).as_str(), "QUEUED");
    assert_eq!(c.cmds(&["INCR", "tx:counter"]).as_str(), "QUEUED");

    let results = c.cmds(&["EXEC"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 3);
    assert!(arr[0].is_ok());
    assert!(arr[1].is_ok());
    assert_eq!(arr[2].as_int(), 1);

    assert_eq!(c.cmds(&["GET", "tx:k1"]).as_str(), "hello");
    assert_eq!(c.cmds(&["GET", "tx:k2"]).as_str(), "world");
}

// DISCARD clears the queue and exits transaction mode
//   MULTI              → OK
//   SET tx:k "hello"   → QUEUED
//   DISCARD            → OK
//   GET tx:k           → nil (was never executed)
#[test]
fn test_multi_discard() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["MULTI"]).is_ok());
    assert_eq!(c.cmds(&["SET", "tx:k", "hello"]).as_str(), "QUEUED");
    assert!(c.cmds(&["DISCARD"]).is_ok());
    assert!(c.cmds(&["GET", "tx:k"]).is_nil());
}

// Nested MULTI is rejected
#[test]
fn test_multi_nested_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["MULTI"]).is_ok());
    let r = c.cmds(&["MULTI"]);
    assert!(r.is_error());
    assert!(r.error_msg().contains("nested"));
    assert!(c.cmds(&["DISCARD"]).is_ok());
}

// EXEC without MULTI returns error
#[test]
fn test_exec_without_multi() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["EXEC"]);
    assert!(r.is_error());
    assert!(r.error_msg().contains("EXEC without MULTI"));
}

// DISCARD without MULTI returns error
#[test]
fn test_discard_without_multi() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["DISCARD"]);
    assert!(r.is_error());
    assert!(r.error_msg().contains("DISCARD without MULTI"));
}

// WATCH + MULTI + EXEC — no modification: transaction executes normally
//   SET watch:k "original"
//   WATCH watch:k          → OK
//   MULTI                  → OK
//   SET watch:k "new"      → QUEUED
//   EXEC                   → [OK]  (watch:k was not modified externally)
#[test]
fn test_watch_exec_no_modification() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    c.cmds(&["SET", "watch:k", "original"]);
    assert!(c.cmds(&["WATCH", "watch:k"]).is_ok());
    assert!(c.cmds(&["MULTI"]).is_ok());
    assert_eq!(c.cmds(&["SET", "watch:k", "new"]).as_str(), "QUEUED");

    let results = c.cmds(&["EXEC"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 1);
    assert!(arr[0].is_ok());
    assert_eq!(c.cmds(&["GET", "watch:k"]).as_str(), "new");
}

// WATCH + concurrent modification → EXEC returns nil (aborted)
//   SET watch:k "original"
//   WATCH watch:k          → OK
//   [second connection modifies watch:k]
//   MULTI                  → OK
//   SET watch:k "new"      → QUEUED
//   EXEC                   → nil (aborted because watch:k was modified)
#[test]
fn test_watch_exec_aborted_on_modification() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c1.cmds(&["SET", "watch:dirty", "original"]);
    assert!(c1.cmds(&["WATCH", "watch:dirty"]).is_ok());

    // c2 modifies the watched key before c1 calls EXEC
    c2.cmds(&["SET", "watch:dirty", "modified_by_c2"]);

    assert!(c1.cmds(&["MULTI"]).is_ok());
    assert_eq!(
        c1.cmds(&["SET", "watch:dirty", "c1_value"]).as_str(),
        "QUEUED"
    );

    // EXEC should return nil array (aborted)
    let result = c1.cmds(&["EXEC"]);
    assert!(
        result.is_nil(),
        "expected nil (aborted tx), got {:?}",
        result
    );

    // The value should still be what c2 set, not what c1 tried to set
    assert_eq!(c1.cmds(&["GET", "watch:dirty"]).as_str(), "modified_by_c2");
}

// UNWATCH clears watches — subsequent modification doesn't abort
#[test]
fn test_unwatch_clears_watches() {
    let port = free_port();
    start_server(port);
    let mut c1 = Conn::connect(port);
    let mut c2 = Conn::connect(port);

    c1.cmds(&["SET", "unwatch:k", "original"]);
    assert!(c1.cmds(&["WATCH", "unwatch:k"]).is_ok());
    assert!(c1.cmds(&["UNWATCH"]).is_ok());

    // Modify watched key after UNWATCH — should NOT abort
    c2.cmds(&["SET", "unwatch:k", "modified"]);

    assert!(c1.cmds(&["MULTI"]).is_ok());
    assert_eq!(c1.cmds(&["SET", "unwatch:k", "c1"]).as_str(), "QUEUED");
    let results = c1.cmds(&["EXEC"]);
    // Should succeed — UNWATCH cleared the watch
    let arr = results.as_array();
    assert_eq!(arr.len(), 1);
    assert!(arr[0].is_ok());
}

// WATCH inside MULTI returns error
#[test]
fn test_watch_inside_multi_error() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["MULTI"]).is_ok());
    let r = c.cmds(&["WATCH", "somekey"]);
    assert!(r.is_error());
    assert!(r.error_msg().contains("WATCH inside MULTI"));
    assert!(c.cmds(&["DISCARD"]).is_ok());
}

// Empty EXEC returns empty array
#[test]
fn test_multi_exec_empty() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["MULTI"]).is_ok());
    let results = c.cmds(&["EXEC"]);
    assert_eq!(results.as_array().len(), 0);
}

// Transaction preserves selected_db changes across queued commands
#[test]
fn test_multi_exec_preserves_db_state() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["MULTI"]).is_ok());
    assert_eq!(c.cmds(&["SET", "tx:db:k", "v1"]).as_str(), "QUEUED");
    assert_eq!(c.cmds(&["GET", "tx:db:k"]).as_str(), "QUEUED");

    let results = c.cmds(&["EXEC"]);
    let arr = results.as_array();
    assert_eq!(arr.len(), 2);
    assert!(arr[0].is_ok());
    assert_eq!(arr[1].as_str(), "v1");
}

// ── A transaction is one step ────────────────────────────────────────────────

/// Money moved between two accounts by `MULTI` / `DECRBY` / `INCRBY` / `EXEC`
/// is never seen half-moved: a reader's `MGET` cannot land between the two
/// commands. It could — one command ran at a time, but nothing kept another
/// client's command from running between them.
/// A command refused by ACL inside MULTI aborts the transaction, as one
/// refused for any other reason does: EXEC ran the others (#159).
#[test]
fn test_a_command_refused_by_acl_aborts_the_transaction() {
    let port = free_port();
    start_server(port);
    let mut admin = Conn::connect(port);
    assert!(
        admin
            .cmds(&[
                "ACL",
                "SETUSER",
                "limited",
                "on",
                ">pw",
                "~*",
                "+@all",
                "-flushall"
            ])
            .is_ok()
    );
    let mut c = Conn::connect(port);
    assert!(c.cmds(&["AUTH", "limited", "pw"]).is_ok());
    assert!(c.cmds(&["MULTI"]).is_ok());
    c.cmds(&["SET", "a", "1"]);
    assert!(c.cmds(&["FLUSHALL"]).error_msg().starts_with("NOPERM"));
    c.cmds(&["SET", "b", "2"]);
    assert!(c.cmds(&["EXEC"]).error_msg().starts_with("EXECABORT"));
    assert!(admin.cmds(&["GET", "a"]).is_nil());
    assert!(admin.cmds(&["GET", "b"]).is_nil());
}

/// A write that changed nothing leaves a WATCH on its key alone, as in
/// Redis: `SET ... NX` on a key that exists, `SADD` of a member there
/// already (#159).
#[test]
fn test_a_write_that_changes_nothing_does_not_fail_a_watch() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let mut other = Conn::connect(port);
    c.cmds(&["SET", "w", "1"]);
    c.cmds(&["SADD", "s", "m"]);
    assert!(c.cmds(&["WATCH", "w", "s"]).is_ok());
    assert!(other.cmds(&["SET", "w", "2", "NX"]).is_nil());
    assert_eq!(other.cmds(&["SADD", "s", "m"]).as_int(), 0);
    assert_eq!(other.cmds(&["EXPIRE", "missing", "10"]).as_int(), 0);
    assert!(c.cmds(&["MULTI"]).is_ok());
    c.cmds(&["SET", "w", "3"]);
    assert_eq!(c.cmds(&["EXEC"]).as_array().len(), 1);

    // One that does change it still fails it.
    assert!(c.cmds(&["WATCH", "w"]).is_ok());
    assert!(other.cmds(&["SET", "w", "4", "XX"]).is_ok());
    assert!(c.cmds(&["MULTI"]).is_ok());
    c.cmds(&["SET", "w", "5"]);
    assert!(c.cmds(&["EXEC"]).is_nil());
}

/// `SWAPDB` dirties the watches on the keys of the two databases it swaps
/// that are in either of them, and no other: not a watch in another
/// database, nor one on a key in neither. A database swapped with itself
/// changes nothing (#556). Every watch in every database used to fail.
#[test]
fn test_swapdb_dirties_only_the_watches_on_the_swapped_keys() {
    let port = free_port();
    start_server(port);
    let mut other = Conn::connect(port);
    let watch = |db: &str, key: &str| {
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["SELECT", db]).is_ok());
        assert!(c.cmds(&["WATCH", key]).is_ok());
        c
    };
    let exec = |c: &mut Conn| {
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["PING"]).as_str(), "QUEUED");
        c.cmds(&["EXEC"])
    };
    for (db, key) in [("5", "k"), ("0", "in0"), ("1", "in1")] {
        assert!(other.cmds(&["SELECT", db]).is_ok());
        assert!(other.cmds(&["SET", key, "v"]).is_ok());
    }

    let mut elsewhere = watch("5", "k");
    let mut in_neither = watch("1", "absent");
    let mut arrives = watch("1", "in0");
    let mut leaves = watch("1", "in1");
    let mut goes = watch("0", "in0");
    assert!(other.cmds(&["SWAPDB", "0", "1"]).is_ok());
    assert_eq!(exec(&mut elsewhere).as_array().len(), 1);
    assert_eq!(exec(&mut in_neither).as_array().len(), 1);
    assert!(exec(&mut arrives).is_nil());
    assert!(exec(&mut leaves).is_nil());
    assert!(exec(&mut goes).is_nil());

    let mut itself = watch("1", "in0");
    assert!(other.cmds(&["SWAPDB", "1", "1"]).is_ok());
    assert_eq!(exec(&mut itself).as_array().len(), 1);
}

/// For `SWAPDB`, a key whose time is up is not there, as for a flush: a
/// watch on one that had expired, swapped for none, holds, as does a watch
/// on a key that is in neither database but for an expired one. A key that
/// was there and is swapped for an expired one is gone: its watch fails
/// (#556). Active expiry is off, so that the keys stay where they are.
#[test]
fn test_swapdb_takes_an_expired_key_as_absent() {
    let port = free_port();
    start_server(port);
    let mut other = Conn::connect(port);
    assert!(other.cmds(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).is_ok());
    let watch = |db: &str, key: &str| {
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["SELECT", db]).is_ok());
        assert!(c.cmds(&["WATCH", key]).is_ok());
        c
    };
    let exec = |c: &mut Conn| {
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["PING"]).as_str(), "QUEUED");
        c.cmds(&["EXEC"])
    };
    assert!(other.cmds(&["SELECT", "2"]).is_ok());
    assert!(other.cmds(&["SET", "gone", "v", "PX", "1"]).is_ok());
    assert!(other.cmds(&["SET", "live", "v", "PX", "1"]).is_ok());
    assert!(other.cmds(&["SELECT", "3"]).is_ok());
    assert!(other.cmds(&["SET", "live", "v"]).is_ok());
    thread::sleep(Duration::from_millis(20));

    let mut expired_here = watch("2", "gone");
    let mut expired_there = watch("3", "gone");
    let mut replaced = watch("3", "live");
    assert!(other.cmds(&["SWAPDB", "2", "3"]).is_ok());
    let held = (
        exec(&mut expired_here),
        exec(&mut expired_there),
        exec(&mut replaced),
    );
    assert!(other.cmds(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).is_ok());
    assert_eq!(held.0.as_array().len(), 1);
    assert_eq!(held.1.as_array().len(), 1);
    assert!(held.2.is_nil(), "{:?}", held.2);
}

/// A pop that takes nothing changes nothing, and a watch on its key holds,
/// as in Redis: a `BLPOP` that times out, an `LPOP` of a missing key, and a
/// `BLPOP` in a transaction that finds nothing (#558).
#[test]
fn test_a_pop_that_takes_nothing_does_not_fail_a_watch() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let mut other = Conn::connect(port);
    assert!(c.cmds(&["WATCH", "empty"]).is_ok());
    assert!(other.cmds(&["BLPOP", "empty", "0.01"]).is_nil());
    assert!(other.cmds(&["LPOP", "empty"]).is_nil());
    assert!(
        other
            .cmds(&["BLMOVE", "empty", "d", "LEFT", "LEFT", "0.01"])
            .is_nil()
    );
    assert!(other.cmds(&["MULTI"]).is_ok());
    other.cmds(&["BLPOP", "empty", "0"]);
    other.cmds(&["BZPOPMIN", "empty", "0"]);
    assert_eq!(other.cmds(&["EXEC"]).as_array().len(), 2);
    assert!(c.cmds(&["MULTI"]).is_ok());
    c.cmds(&["PING"]);
    assert_eq!(c.cmds(&["EXEC"]).as_array().len(), 1);

    // One that takes something fails it.
    assert_eq!(other.cmds(&["RPUSH", "full", "a", "b"]).as_int(), 2);
    assert!(c.cmds(&["WATCH", "full"]).is_ok());
    assert_eq!(other.cmds(&["BLPOP", "full", "0"]).as_array().len(), 2);
    assert!(c.cmds(&["MULTI"]).is_ok());
    c.cmds(&["PING"]);
    assert!(c.cmds(&["EXEC"]).is_nil());
}

/// A blocking command in a transaction never waits: it runs as its
/// non-blocking form and takes what is there, as in Redis, and answers its
/// empty reply when nothing is (#558).
#[test]
fn test_blocking_commands_in_a_transaction_take_what_is_there() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    assert_eq!(c.cmds(&["RPUSH", "l", "a", "b", "c", "d"]).as_int(), 4);
    assert_eq!(
        c.cmds(&["ZADD", "z", "1", "m1", "2", "m2", "3", "m3"])
            .as_int(),
        3
    );
    assert!(c.cmds(&["MULTI"]).is_ok());
    for command in [
        &["BLPOP", "l", "0"][..],
        &["BRPOP", "l", "0"],
        &["BLMOVE", "l", "dst", "LEFT", "RIGHT", "0"],
        &["BLMPOP", "0", "1", "l", "LEFT"],
        &["BRPOPLPUSH", "dst", "l", "0"],
        &["BZPOPMIN", "z", "0"],
        &["BZPOPMAX", "z", "0"],
        &["BZMPOP", "0", "1", "z", "MIN"],
        &["BLPOP", "empty", "0"],
        &["BRPOPLPUSH", "empty", "dst", "0"],
    ] {
        assert_eq!(c.cmds(command).as_str(), "QUEUED");
    }
    let replies = c.cmds(&["EXEC"]);
    let replies = replies.as_array();
    let strings = |reply: &RespValue| {
        reply
            .as_array()
            .iter()
            .map(|part| part.as_str().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(strings(&replies[0]), ["l", "a"]);
    assert_eq!(strings(&replies[1]), ["l", "d"]);
    assert_eq!(replies[2].as_str(), "b");
    assert_eq!(replies[3].as_array()[0].as_str(), "l");
    assert_eq!(strings(&replies[3].as_array()[1]), ["c"]);
    assert_eq!(replies[4].as_str(), "b");
    assert_eq!(strings(&replies[5]), ["z", "m1", "1"]);
    assert_eq!(strings(&replies[6]), ["z", "m3", "3"]);
    assert_eq!(replies[7].as_array()[0].as_str(), "z");
    assert_eq!(
        strings(&replies[7].as_array()[1].as_array()[0]),
        ["m2", "2"]
    );
    assert!(replies[8].is_nil(), "{:?}", replies[8]);
    assert!(replies[9].is_nil(), "{:?}", replies[9]);
    assert_eq!(c.cmds(&["LRANGE", "l", "0", "-1"]).as_array().len(), 1);
    assert_eq!(c.cmds(&["EXISTS", "z", "dst"]).as_int(), 0);
}

/// A DEL of keys on several shards is one step: a reader never finds some
/// of them gone and others not. Nor a RENAME, the source gone and the
/// destination not there yet (#159).
#[test]
fn test_readers_never_see_a_multi_key_write_half_done() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let port = free_port();
    start_server(port);
    let keys: Vec<String> = (0..200).map(|i| format!("d{i}")).collect();
    let mut w = Conn::connect(port);
    // Before the reader starts: a reader that came first found neither
    // `from` nor `to`, and counted that as a RENAME half done.
    w.cmds(&["SET", "from", "x"]);
    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let reader = {
        let (stop, torn, keys) = (Arc::clone(&stop), Arc::clone(&torn), keys.clone());
        thread::spawn(move || {
            let mut r = Conn::connect(port);
            let mut args = vec!["MGET"];
            args.extend(keys.iter().map(|k| k.as_str()));
            while !stop.load(Ordering::Relaxed) {
                let got = r.cmds(&args);
                let present = got.as_array().iter().filter(|v| !v.is_nil()).count();
                if present != 0 && present != keys.len() {
                    torn.fetch_add(1, Ordering::Relaxed);
                }
                let renamed = r.cmds(&["MGET", "from", "to"]);
                if renamed.as_array().iter().all(|v| v.is_nil()) {
                    torn.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };
    for _ in 0..300 {
        let mut mset = vec!["MSET"];
        for k in &keys {
            mset.push(k);
            mset.push("v");
        }
        assert!(w.cmds(&mset).is_ok());
        let mut del = vec!["DEL"];
        del.extend(keys.iter().map(|k| k.as_str()));
        assert_eq!(w.cmds(&del).as_int(), keys.len() as i64);
        assert!(w.cmds(&["RENAME", "from", "to"]).is_ok());
        assert!(w.cmds(&["RENAME", "to", "from"]).is_ok());
    }
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert_eq!(
        torn.load(Ordering::Relaxed),
        0,
        "a reader saw a write half done"
    );
}

#[test]
fn test_readers_never_see_a_transaction_half_applied() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "acct:a", "100000"]);
    c.cmds(&["SET", "acct:b", "0"]);

    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (stop, torn, reads) = (Arc::clone(&stop), Arc::clone(&torn), Arc::clone(&reads));
            thread::spawn(move || {
                let mut r = Conn::connect(port);
                while !stop.load(Ordering::Relaxed) {
                    let both = r.cmds(&["MGET", "acct:a", "acct:b"]);
                    let total: i64 = both
                        .as_array()
                        .iter()
                        .map(|v| v.as_str().parse::<i64>().unwrap())
                        .sum();
                    if total != 100_000 {
                        torn.fetch_add(1, Ordering::Relaxed);
                    }
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    // Two clients moving money, one of them through four commands at a time.
    let movers: Vec<_> = (0..2)
        .map(|who| {
            thread::spawn(move || {
                let mut m = Conn::connect(port);
                for _ in 0..400 {
                    assert!(m.cmds(&["MULTI"]).is_ok());
                    m.cmds(&["DECRBY", "acct:a", "7"]);
                    if who == 1 {
                        m.cmds(&["INCRBY", "acct:b", "3"]);
                        m.cmds(&["INCRBY", "acct:b", "3"]);
                        m.cmds(&["INCRBY", "acct:b", "1"]);
                    } else {
                        m.cmds(&["INCRBY", "acct:b", "7"]);
                    }
                    let done = m.cmds(&["EXEC"]);
                    assert!(done.as_array().len() >= 2);
                }
            })
        })
        .collect();
    for m in movers {
        m.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    assert!(reads.load(Ordering::Relaxed) > 100, "the readers did read");
    assert_eq!(
        torn.load(Ordering::Relaxed),
        0,
        "a reader saw money that had left one account and not reached the other"
    );
    assert_eq!(c.cmds(&["GET", "acct:a"]).as_str(), "94400");
    assert_eq!(c.cmds(&["GET", "acct:b"]).as_str(), "5600");
}

/// A write made outside any transaction cannot land between an `EXEC`'s
/// check of its watched keys and its commands: either it comes first and the
/// transaction is refused, or it comes after the transaction has run.
#[test]
fn test_a_plain_write_cannot_slip_into_a_watched_transaction() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&["SET", "cas:n", "0"]);

    // One client increments with plain INCR; three with WATCH / GET / MULTI /
    // SET / EXEC, retrying when refused. Every increment — of either kind —
    // must be in the final value: a refused transaction is retried, and one
    // that ran cannot have overwritten an INCR it did not see.
    let stop = Arc::new(AtomicBool::new(false));
    let plain = {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut p = Conn::connect(port);
            let mut done = 0i64;
            while !stop.load(Ordering::Relaxed) {
                p.cmds(&["INCR", "cas:n"]);
                done += 1;
                // Often enough to land inside many transactions; not so
                // often that none of them ever gets through.
                thread::sleep(std::time::Duration::from_micros(300));
            }
            done
        })
    };
    let optimistic: Vec<_> = (0..3)
        .map(|_| {
            thread::spawn(move || {
                let mut o = Conn::connect(port);
                for _ in 0..150 {
                    loop {
                        assert!(o.cmds(&["WATCH", "cas:n"]).is_ok());
                        let seen: i64 = o.cmds(&["GET", "cas:n"]).as_str().parse().unwrap();
                        assert!(o.cmds(&["MULTI"]).is_ok());
                        o.cmds(&["SET", "cas:n", &(seen + 1).to_string()]);
                        if !o.cmds(&["EXEC"]).is_nil() {
                            break;
                        }
                    }
                }
            })
        })
        .collect();
    for o in optimistic {
        o.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    let plain_increments = plain.join().unwrap();

    let total: i64 = c.cmds(&["GET", "cas:n"]).as_str().parse().unwrap();
    assert_eq!(
        total,
        plain_increments + 3 * 150,
        "an increment was lost: a transaction ran over a write it had not seen"
    );
}

// ── Inside MULTI, only the transaction's own commands act at once (#550) ─────
//
// In Redis, inside MULTI only EXEC, DISCARD, MULTI, WATCH, QUIT and RESET act
// at once; every other command is queued. UNWATCH, AUTH, HELLO, MONITOR, WAIT
// and WAITAOF were answered by the connection loop before it looked at the
// transaction, so they ran at once. Each test runs on both connection loops.

/// The I/O models to run each of these tests on: both, where io_uring
/// exists, or the one `KIVIDB_TEST_IO_MODEL` names.
fn io_models() -> Vec<kividb::config::IoModel> {
    use kividb::config::IoModel;
    match std::env::var("KIVIDB_TEST_IO_MODEL").as_deref() {
        Ok("tokio") => vec![IoModel::Tokio],
        Ok("uring") => vec![IoModel::Uring],
        _ if cfg!(target_os = "linux") => vec![IoModel::Tokio, IoModel::Uring],
        _ => vec![IoModel::Tokio],
    }
}

/// A server of its own for `run`, on each I/O model in turn.
fn on_each_io_model(run: impl Fn(u16)) {
    for io_model in io_models() {
        let port = free_port();
        start_server_on(port, io_model);
        run(port);
    }
}

/// An UNWATCH sent inside MULTI is queued. Run at once, it dropped the
/// watches, and EXEC ran over a write to a watched key: the other client's
/// write was lost.
#[test]
fn test_unwatch_inside_multi_is_queued_and_a_changed_watch_still_aborts() {
    on_each_io_model(|port| {
        let mut a = Conn::connect(port);
        let mut b = Conn::connect(port);

        assert!(a.cmds(&["WATCH", "uw:k"]).is_ok());
        assert!(a.cmds(&["MULTI"]).is_ok());
        assert_eq!(a.cmds(&["SET", "uw:k", "from-a"]).as_str(), "QUEUED");
        assert_eq!(a.cmds(&["UNWATCH"]).as_str(), "QUEUED");

        assert!(b.cmds(&["SET", "uw:k", "from-b"]).is_ok());

        let exec = a.cmds(&["EXEC"]);
        assert!(
            exec.is_nil(),
            "EXEC ran after a watched key changed: {exec:?}"
        );
        assert_eq!(a.cmds(&["GET", "uw:k"]).as_str(), "from-b");
    });
}

/// A queued UNWATCH answers OK when EXEC runs it, and changes nothing else.
#[test]
fn test_a_queued_unwatch_answers_ok_in_exec() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["WATCH", "uw2:k"]).is_ok());
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["UNWATCH"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["SET", "uw2:k", "v"]).as_str(), "QUEUED");
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert_eq!(replies.len(), 2);
        assert!(replies[0].is_ok());
        assert!(replies[1].is_ok());
        assert_eq!(c.cmds(&["GET", "uw2:k"]).as_str(), "v");
    });
}

/// AUTH inside MULTI is queued, and the connection becomes the user only
/// when EXEC runs it; the commands after it in the transaction run as that
/// user. A wrong password is EXEC's reply for it.
#[test]
fn test_auth_inside_multi_is_queued() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "txuser", "on", ">pw", "~*", "+@all"])
                .is_ok()
        );
        let mut c = Conn::connect(port);

        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["AUTH", "txuser", "pw"]).as_str(), "QUEUED");
        assert!(c.cmds(&["DISCARD"]).is_ok());
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "default");

        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["AUTH", "txuser", "pw"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["AUTH", "txuser", "wrong"]).as_str(), "QUEUED");
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert_eq!(replies.len(), 4);
        assert_eq!(replies[0].as_str(), "default");
        assert!(replies[1].is_ok());
        assert_eq!(replies[2].as_str(), "txuser");
        assert!(
            replies[3].error_msg().starts_with("WRONGPASS"),
            "{:?}",
            replies[3]
        );
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "txuser");
    });
}

/// HELLO inside MULTI is queued: the protocol and the name change only when
/// EXEC runs it.
#[test]
fn test_hello_inside_multi_is_queued() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);

        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["HELLO", "3"]).as_str(), "QUEUED");
        assert!(c.cmds(&["DISCARD"]).is_ok());
        let info = c.cmds(&["CLIENT", "INFO"]);
        assert!(info.as_str().contains(" resp=2 "), "{}", info.as_str());

        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(
            c.cmds(&["HELLO", "2", "SETNAME", "txname"]).as_str(),
            "QUEUED"
        );
        assert!(c.cmds(&["CLIENT", "GETNAME"]).as_str() == "QUEUED");
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert_eq!(replies.len(), 2);
        let hello = replies[0].as_array();
        assert_eq!(hello[0].as_str(), "server");
        assert_eq!(replies[1].as_str(), "txname");
    });
}

/// MONITOR inside MULTI is queued, and refused when EXEC runs it, as Redis
/// refuses it there: the connection does not start monitoring.
#[test]
fn test_monitor_inside_multi_is_queued_and_refused_in_exec() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["MONITOR"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["PING"]).as_str(), "QUEUED");
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert_eq!(replies.len(), 2);
        assert_eq!(
            replies[0].error_msg(),
            "ERR MONITOR isn't allowed for DENY BLOCKING client"
        );
        assert_eq!(replies[1].as_str(), "PONG");
        // Not a monitor: a command is answered, not echoed.
        assert!(c.cmds(&["SET", "mon:k", "v"]).is_ok());
        assert_eq!(c.cmds(&["GET", "mon:k"]).as_str(), "v");
    });
}

/// WAIT and WAITAOF inside MULTI are queued, and EXEC answers them without
/// waiting (#499).
#[test]
fn test_wait_and_waitaof_inside_multi_are_queued() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["SET", "wait:k", "v"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["WAIT", "1", "0"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["WAITAOF", "0", "0", "0"]).as_str(), "QUEUED");
        let started = std::time::Instant::now();
        let exec = c.cmds(&["EXEC"]);
        assert!(started.elapsed() < Duration::from_secs(2));
        let replies = exec.as_array();
        assert_eq!(replies.len(), 3);
        assert!(replies[0].is_ok());
        assert_eq!(replies[1].as_int(), 0);
        assert_eq!(
            replies[2],
            RespValue::Array(vec![RespValue::Int(0), RespValue::Int(0)])
        );
    });
}

/// The transaction's own commands with the wrong number of arguments are
/// refused, as in Redis, and inside MULTI the transaction with them. An EXEC
/// so refused ends the transaction.
#[test]
fn test_transaction_commands_check_their_arguments() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        let wrong = |name: &str| format!("ERR wrong number of arguments for '{name}' command");

        assert_eq!(c.cmds(&["UNWATCH", "x"]).error_msg(), wrong("unwatch"));
        assert_eq!(c.cmds(&["DISCARD", "x"]).error_msg(), wrong("discard"));
        assert_eq!(c.cmds(&["MULTI", "x"]).error_msg(), wrong("multi"));

        // UNWATCH, DISCARD, WATCH, MULTI: refused, the transaction goes on,
        // and EXEC refuses it.
        for refused in [
            &["UNWATCH", "x"][..],
            &["DISCARD", "x"],
            &["WATCH"],
            &["MULTI", "x"],
        ] {
            assert!(c.cmds(&["MULTI"]).is_ok());
            assert_eq!(c.cmds(&["SET", "arity:k", "v"]).as_str(), "QUEUED");
            let name = refused[0].to_ascii_lowercase();
            assert_eq!(c.cmds(refused).error_msg(), wrong(&name));
            assert_eq!(c.cmds(&["SET", "arity:k", "w"]).as_str(), "QUEUED");
            assert!(
                c.cmds(&["EXEC"]).error_msg().starts_with("EXECABORT"),
                "{refused:?}"
            );
            assert!(c.cmds(&["GET", "arity:k"]).is_nil(), "{refused:?}");
        }

        // EXEC: refused, and the transaction with it.
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["SET", "arity:k", "v"]).as_str(), "QUEUED");
        assert_eq!(
            c.cmds(&["EXEC", "x"]).error_msg(),
            "EXECABORT Transaction discarded because of: ERR wrong number of arguments for 'exec' command"
        );
        assert!(c.cmds(&["EXEC"]).error_msg().contains("EXEC without MULTI"));
        assert!(c.cmds(&["GET", "arity:k"]).is_nil());
    });
}
