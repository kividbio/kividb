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
            // RESP3, for a connection that a HELLO 3 has switched.
            Some(b'%') => {
                let count: usize = line[1..].parse().unwrap();
                let pairs = (0..count)
                    .map(|_| (self.read_value(), self.read_value()))
                    .collect();
                RespValue::Map(pairs)
            }
            Some(b'_') => RespValue::Nil,
            Some(b',') => RespValue::SimpleStr(line[1..].to_string()),
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
    Map(Vec<(RespValue, RespValue)>),
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
    start_server_with(port, io_model, None, None);
}

/// A server on `port`, a replica of the server on `replica_of` if that is
/// given, working in `dir` if that is.
fn start_server_with(
    port: u16,
    io_model: kividb::config::IoModel,
    replica_of: Option<u16>,
    dir: Option<&std::path::Path>,
) {
    let replicaof = replica_of.map(|master| vec!["127.0.0.1".to_string(), master.to_string()]);
    let dir = dir
        .map(|dir| dir.to_string_lossy().into_owned())
        .unwrap_or_default();
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
            replicaof,
            dir,
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

/// A write fails the watches on the keys it changed and on no other key it
/// names, as Redis signals them (#584): of a pop from the first of several
/// keys that has something, the key it took from; of `SUNIONSTORE`, `COPY`,
/// `BITOP`, `PFMERGE`, `SORT … STORE` and the like, the destination and not
/// the sources; of a `DEL`, the keys that were there; of `SMOVE`, the
/// destination only if the member was not in it. `MOVE` and `COPY … DB`
/// change the key in the other database too. Every key named used to fail.
#[test]
fn test_a_write_fails_only_the_watches_on_the_keys_it_changed() {
    let port = free_port();
    start_server(port);
    let mut setup = Conn::connect(port);
    let mut c = Conn::connect(port);
    // Which of `watched` (database, key) fail once `command` has run.
    let mut failed = |watched: &[(&str, &str)], command: &[&str]| -> Vec<String> {
        let mut watchers: Vec<(String, Conn)> = watched
            .iter()
            .map(|(db, key)| {
                let mut w = Conn::connect(port);
                assert!(w.cmds(&["SELECT", db]).is_ok());
                assert!(w.cmds(&["WATCH", key]).is_ok());
                (format!("{db}/{key}"), w)
            })
            .collect();
        let reply = c.cmds(command);
        assert!(!reply.is_error(), "{command:?}: {reply:?}");
        watchers
            .iter_mut()
            .filter_map(|(name, w)| {
                assert!(w.cmds(&["MULTI"]).is_ok());
                w.cmds(&["PING"]);
                w.cmds(&["EXEC"]).is_nil().then(|| name.clone())
            })
            .collect()
    };
    let mut set_up = |commands: &[&[&str]]| {
        for command in commands {
            let reply = setup.cmds(command);
            assert!(!reply.is_error(), "{command:?}: {reply:?}");
        }
    };

    // Pops from the first of several keys that has something.
    set_up(&[&["RPUSH", "p:b", "x"], &["RPUSH", "r:b", "x", "y"]]);
    set_up(&[
        &["ZADD", "z:b", "1", "m"],
        &["ZADD", "zx:b", "1", "m", "2", "n"],
    ]);
    set_up(&[&["RPUSH", "lm:b", "x"], &["ZADD", "zm:b", "1", "m"]]);
    set_up(&[&["RPUSH", "blm:b", "x"], &["ZADD", "bzm:b", "1", "m"]]);
    for (command, a, b) in [
        (&["BLPOP", "p:a", "p:b", "0"][..], "p:a", "p:b"),
        (&["BRPOP", "r:a", "r:b", "0"], "r:a", "r:b"),
        (&["BZPOPMIN", "z:a", "z:b", "0"], "z:a", "z:b"),
        (&["BZPOPMAX", "zx:a", "zx:b", "0"], "zx:a", "zx:b"),
        (&["LMPOP", "2", "lm:a", "lm:b", "LEFT"], "lm:a", "lm:b"),
        (&["ZMPOP", "2", "zm:a", "zm:b", "MIN"], "zm:a", "zm:b"),
        (
            &["BLMPOP", "0", "2", "blm:a", "blm:b", "LEFT"],
            "blm:a",
            "blm:b",
        ),
        (
            &["BZMPOP", "0", "2", "bzm:a", "bzm:b", "MIN"],
            "bzm:a",
            "bzm:b",
        ),
    ] {
        assert_eq!(
            failed(&[("0", a), ("0", b)], command),
            [format!("0/{b}")],
            "{command:?}"
        );
    }

    // Commands that read some keys and write another.
    set_up(&[&["SADD", "s:1", "a"], &["SADD", "s:2", "b"]]);
    assert_eq!(
        failed(
            &[("0", "s:1"), ("0", "s:2"), ("0", "s:d")],
            &["SUNIONSTORE", "s:d", "s:1", "s:2"]
        ),
        ["0/s:d"]
    );
    set_up(&[&["ZADD", "zs:1", "1", "a"]]);
    assert_eq!(
        failed(
            &[("0", "zs:1"), ("0", "zs:d")],
            &["ZUNIONSTORE", "zs:d", "1", "zs:1"]
        ),
        ["0/zs:d"]
    );
    set_up(&[&["ZADD", "zr:s", "1", "a"]]);
    assert_eq!(
        failed(
            &[("0", "zr:s"), ("0", "zr:d")],
            &["ZRANGESTORE", "zr:d", "zr:s", "0", "-1"]
        ),
        ["0/zr:d"]
    );
    set_up(&[&["SET", "bo:1", "a"], &["SET", "bo:2", "b"]]);
    assert_eq!(
        failed(
            &[("0", "bo:1"), ("0", "bo:2"), ("0", "bo:d")],
            &["BITOP", "AND", "bo:d", "bo:1", "bo:2"]
        ),
        ["0/bo:d"]
    );
    set_up(&[&["PFADD", "hl:1", "a"]]);
    assert_eq!(
        failed(
            &[("0", "hl:1"), ("0", "hl:d")],
            &["PFMERGE", "hl:d", "hl:1"]
        ),
        ["0/hl:d"]
    );
    set_up(&[&["RPUSH", "so:s", "3", "1", "2"]]);
    assert_eq!(
        failed(
            &[("0", "so:s"), ("0", "so:d")],
            &["SORT", "so:s", "STORE", "so:d"]
        ),
        ["0/so:d"]
    );
    set_up(&[&["GEOADD", "g:s", "13.36", "38.11", "a"]]);
    assert_eq!(
        failed(
            &[("0", "g:s"), ("0", "g:d")],
            &[
                "GEOSEARCHSTORE",
                "g:d",
                "g:s",
                "FROMLONLAT",
                "13.36",
                "38.11",
                "BYRADIUS",
                "10",
                "km"
            ]
        ),
        ["0/g:d"]
    );
    set_up(&[&["SET", "cp:s", "v"]]);
    assert_eq!(
        failed(&[("0", "cp:s"), ("0", "cp:d")], &["COPY", "cp:s", "cp:d"]),
        ["0/cp:d"]
    );
    assert_eq!(
        failed(
            &[("0", "cp:s"), ("0", "cp:e"), ("3", "cp:e")],
            &["COPY", "cp:s", "cp:e", "DB", "3"]
        ),
        ["3/cp:e"]
    );
    // Not copied: the destination is there.
    assert!(failed(&[("0", "cp:s"), ("0", "cp:d")], &["COPY", "cp:s", "cp:d"]).is_empty());

    // DEL: the keys that were there.
    set_up(&[&["SET", "de:x", "v"]]);
    assert_eq!(
        failed(
            &[("0", "de:x"), ("0", "de:missing")],
            &["DEL", "de:x", "de:missing"]
        ),
        ["0/de:x"]
    );
    // A key named twice, deleted the first time: it changed.
    set_up(&[&["SET", "de:twice", "v"], &["SET", "un:twice", "v"]]);
    assert_eq!(
        failed(&[("0", "de:twice")], &["DEL", "de:twice", "de:twice"]),
        ["0/de:twice"]
    );
    assert_eq!(
        failed(&[("0", "un:twice")], &["UNLINK", "un:twice", "un:twice"]),
        ["0/un:twice"]
    );
    // XGROUP CREATE … MKSTREAM of a stream that is not there makes it.
    assert_eq!(
        failed(
            &[("0", "xs:new")],
            &["XGROUP", "CREATE", "xs:new", "g", "$", "MKSTREAM"]
        ),
        ["0/xs:new"]
    );
    // MOVE: the key here and in the other database.
    set_up(&[&["SET", "mv:k", "v"]]);
    assert_eq!(
        failed(
            &[("0", "mv:k"), ("4", "mv:k"), ("4", "mv:other")],
            &["MOVE", "mv:k", "4"]
        ),
        ["0/mv:k", "4/mv:k"]
    );
    // SMOVE of a member the destination has: the source only.
    set_up(&[&["SADD", "sm:s", "a"], &["SADD", "sm:d", "a"]]);
    assert_eq!(
        failed(
            &[("0", "sm:s"), ("0", "sm:d")],
            &["SMOVE", "sm:s", "sm:d", "a"]
        ),
        ["0/sm:s"]
    );

    // Consumer-group commands do not fail a watch on the stream, as in
    // Redis (#588); XADD does.
    set_up(&[
        &["XADD", "xs:1", "1-1", "f", "v"],
        &["XADD", "xs:2", "1-1", "f", "v"],
        &["XGROUP", "CREATE", "xs:1", "g", "0"],
        &["XGROUP", "CREATE", "xs:2", "g", "0"],
    ]);
    for command in [
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "STREAMS",
            "xs:1",
            "xs:2",
            ">",
            ">",
        ][..],
        &["XACK", "xs:1", "g", "1-1"],
        &["XCLAIM", "xs:2", "g", "c2", "0", "1-1"],
        &["XAUTOCLAIM", "xs:2", "g", "c3", "0", "0"],
        &["XGROUP", "CREATECONSUMER", "xs:1", "g", "c4"],
        &["XSETID", "xs:1", "5-0"],
    ] {
        assert!(
            failed(&[("0", "xs:1"), ("0", "xs:2")], command).is_empty(),
            "{command:?}"
        );
    }
    assert_eq!(
        failed(
            &[("0", "xs:1"), ("0", "xs:2")],
            &["XADD", "xs:1", "*", "f", "v"]
        ),
        ["0/xs:1"]
    );

    // Writes that change every key they name still fail every watch.
    set_up(&[&["RPUSH", "lmv:a", "x"], &["SET", "rn:a", "v"]]);
    for (command, a, b) in [
        (&["MSET", "ms:a", "1", "ms:b", "2"][..], "ms:a", "ms:b"),
        (&["RENAME", "rn:a", "rn:b"], "rn:a", "rn:b"),
        (
            &["LMOVE", "lmv:a", "lmv:b", "LEFT", "RIGHT"],
            "lmv:a",
            "lmv:b",
        ),
    ] {
        assert_eq!(
            failed(&[("0", a), ("0", b)], command),
            [format!("0/{a}"), format!("0/{b}")],
            "{command:?}"
        );
    }
}

/// For `SWAPDB`: a watch on a key that had expired when it was watched,
/// swapped for none, holds, as does a watch on a key that is in neither
/// database but for an expired one. A key that was there and is swapped for
/// an expired one is modified: its watch fails (#556). Active expiry is off,
/// so that the keys stay where they are.
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

/// A flush and a `SWAPDB` take each watch by whether its key had run out of
/// time when it was watched, as Redis 8.10's `touchAllWatchedKeysInDb` does
/// (#666). A key that was alive when watched and expired after is still
/// there until something deletes it, and the flush that deletes it is a
/// change: the `EXEC` fails (it ran: the flush took the key for absent, and
/// at `EXEC` there was no expired key left to see). A key that had expired
/// when it was watched is no change. A `SWAPDB` that puts an expired key
/// where the watched one was absent marks the watch expired, and the `EXEC`
/// runs (it failed: the key was found expired at `EXEC`). Active expiry is
/// off, so that the keys stay where they are.
#[test]
fn test_a_flush_or_swapdb_takes_each_watch_by_when_its_key_expired() {
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

    assert!(other.cmds(&["SELECT", "4"]).is_ok());
    assert!(other.cmds(&["SET", "later", "v", "PX", "200"]).is_ok());
    let mut expired_later = watch("4", "later");
    assert!(other.cmds(&["SET", "before", "v", "PX", "1"]).is_ok());
    thread::sleep(Duration::from_millis(400));
    let mut expired_before = watch("4", "before");
    assert!(other.cmds(&["FLUSHDB"]).is_ok());

    assert!(other.cmds(&["SELECT", "5"]).is_ok());
    assert!(other.cmds(&["SET", "x", "v", "PX", "1"]).is_ok());
    thread::sleep(Duration::from_millis(20));
    let mut swapped_in = watch("6", "x");
    assert!(other.cmds(&["SWAPDB", "5", "6"]).is_ok());

    let replies = (
        exec(&mut expired_later),
        exec(&mut expired_before),
        exec(&mut swapped_in),
    );
    assert!(other.cmds(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).is_ok());
    assert!(replies.0.is_nil(), "{:?}", replies.0);
    assert_eq!(replies.1.as_array().len(), 1, "{:?}", replies.1);
    assert_eq!(replies.2.as_array().len(), 1, "{:?}", replies.2);
}

/// A watch a `SWAPDB` marked expired (its key was absent and an expired one
/// came in) is broken by a write that stores over the expired key, as in
/// Redis, where the write expires the key first (no change) and then
/// writes it (a change). `GEOSEARCHSTORE` and the `STORE` forms of
/// `GEORADIUS` delete and insert their destination without expiring it
/// first: the watch held and the `EXEC` ran (review of #672).
#[test]
fn test_a_store_over_an_expired_key_a_swapdb_brought_fails_the_watch() {
    let port = free_port();
    start_server(port);
    let mut other = Conn::connect(port);
    assert!(other.cmds(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).is_ok());
    let stores: [&[&str]; 4] = [
        &[
            "GEOSEARCHSTORE",
            "KEY",
            "g",
            "FROMLONLAT",
            "0",
            "0",
            "BYRADIUS",
            "10",
            "km",
        ],
        &[
            "GEOSEARCHSTORE",
            "KEY",
            "g",
            "FROMLONLAT",
            "0",
            "0",
            "BYRADIUS",
            "10",
            "km",
            "STOREDIST",
        ],
        &["GEORADIUS", "g", "0", "0", "10", "km", "STORE", "KEY"],
        &["GEORADIUSBYMEMBER", "g", "m", "10", "km", "STORE", "KEY"],
    ];
    let mut replies = Vec::new();
    for (i, store) in stores.iter().enumerate() {
        let key = format!("geo{i}");
        let db = (1 + i).to_string();
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["SELECT", "15"]).is_ok());
        assert!(other.cmds(&["SELECT", &db]).is_ok());
        assert!(other.cmds(&["SET", &key, "v", "PX", "30"]).is_ok());
        thread::sleep(Duration::from_millis(60));
        assert!(c.cmds(&["WATCH", &key]).is_ok());
        // `c` watches `key` in database 15, absent; the swap brings the
        // expired one in, and marks the watch.
        assert!(other.cmds(&["SWAPDB", &db, "15"]).is_ok());
        assert!(other.cmds(&["SELECT", "15"]).is_ok());
        assert!(other.cmds(&["GEOADD", "g", "0", "0", "m"]).as_int() >= 0);
        let args: Vec<&str> = store
            .iter()
            .map(|a| if *a == "KEY" { key.as_str() } else { a })
            .collect();
        assert_eq!(other.cmds(&args).as_int(), 1, "{args:?}");
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["PING"]).as_str(), "QUEUED");
        replies.push((args.join(" "), c.cmds(&["EXEC"])));
    }
    assert!(other.cmds(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).is_ok());
    for (store, reply) in replies {
        assert!(reply.is_nil(), "{store}: {reply:?}");
    }
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

/// A queued WAIT, with a replica connected, counts the replicas that have
/// what was written before EXEC, and does not wait: neither `WAIT 1 0` nor
/// `WAIT 5 0`, which outside a transaction would wait for ever.
#[test]
fn test_wait_inside_multi_with_a_replica_answers_without_waiting() {
    for io_model in io_models() {
        let dir = tempfile::tempdir().unwrap();
        let (master_dir, replica_dir) = (dir.path().join("m"), dir.path().join("r"));
        std::fs::create_dir_all(&master_dir).unwrap();
        std::fs::create_dir_all(&replica_dir).unwrap();
        let master = free_port();
        start_server_with(master, io_model, None, Some(&master_dir));
        let replica = free_port();
        start_server_with(
            replica,
            kividb::config::IoModel::Tokio,
            Some(master),
            Some(&replica_dir),
        );
        let mut m = Conn::connect(master);
        let _r = Conn::connect(replica);

        assert!(m.cmds(&["SET", "wr:before", "1"]).is_ok());
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while m.cmds(&["WAIT", "1", "200"]).as_int() != 1 {
            assert!(
                std::time::Instant::now() < deadline,
                "the replica never caught up"
            );
        }

        let mut counted = false;
        for _ in 0..20 {
            assert!(m.cmds(&["MULTI"]).is_ok());
            assert_eq!(m.cmds(&["SET", "wr:k", "v"]).as_str(), "QUEUED");
            assert_eq!(m.cmds(&["WAIT", "1", "0"]).as_str(), "QUEUED");
            assert_eq!(m.cmds(&["WAIT", "5", "0"]).as_str(), "QUEUED");
            let started = std::time::Instant::now();
            let exec = m.cmds(&["EXEC"]);
            assert!(started.elapsed() < Duration::from_secs(2), "EXEC waited");
            let replies = exec.as_array();
            assert_eq!(replies.len(), 3);
            assert!(replies[0].is_ok());
            let (one, five) = (replies[1].as_int(), replies[2].as_int());
            assert!(
                (0..=1).contains(&one) && (0..=1).contains(&five),
                "{replies:?}"
            );
            if one == 1 && five == 1 {
                counted = true;
                break;
            }
            // Something reached the replica's stream since: let it catch up.
            m.cmds(&["WAIT", "1", "1000"]);
        }
        assert!(counted, "a caught-up replica was never counted");
    }
}

/// The commands after a queued AUTH run under the rules of the user it
/// made the connection; and a command queued after an AUTH that has not run
/// yet is checked against the user the connection is now, as in Redis.
#[test]
fn test_acl_rules_follow_a_queued_auth() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        for user in [
            &[
                "ACL", "SETUSER", "noget", "on", ">pw", "~*", "+@all", "-get",
            ][..],
            &["ACL", "SETUSER", "full", "on", ">pw", "~*", "+@all"],
        ] {
            assert!(admin.cmds(user).is_ok());
        }
        assert!(admin.cmds(&["SET", "acl:k", "v"]).is_ok());
        let mut c = Conn::connect(port);

        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["AUTH", "noget", "pw"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["GET", "acl:k"]).as_str(), "QUEUED");
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert_eq!(replies.len(), 2);
        assert!(replies[0].is_ok());
        assert!(
            replies[1].error_msg().starts_with("NOPERM"),
            "{:?}",
            replies[1]
        );

        // Now `noget`: a GET queued after an AUTH to `full` is refused when
        // it is queued, and the transaction with it; the AUTH never runs.
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["AUTH", "full", "pw"]).as_str(), "QUEUED");
        assert!(c.cmds(&["GET", "acl:k"]).error_msg().starts_with("NOPERM"));
        assert!(c.cmds(&["EXEC"]).error_msg().starts_with("EXECABORT"));
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "noget");
    });
}

/// HELLO 3 run by EXEC answers with a map, and the replies after it in the
/// same EXEC are RESP3.
#[test]
fn test_hello_3_inside_multi_switches_the_rest_of_the_exec() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert_eq!(c.cmds(&["HSET", "h3:h", "f", "v"]).as_int(), 1);
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["HELLO", "3"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["HGETALL", "h3:h"]).as_str(), "QUEUED");
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert_eq!(replies.len(), 2);
        let RespValue::Map(hello) = &replies[0] else {
            panic!("HELLO 3 did not answer with a map: {:?}", replies[0]);
        };
        assert!(
            hello
                .iter()
                .any(|(k, v)| k.as_str() == "proto" && *v == RespValue::Int(3)),
            "{hello:?}"
        );
        assert_eq!(
            replies[1],
            RespValue::Map(vec![(
                RespValue::Bulk(b"f".to_vec()),
                RespValue::Bulk(b"v".to_vec())
            )])
        );
    });
}

/// A HELLO whose AUTH fails leaves the protocol as it was, as in Redis.
#[test]
fn test_hello_with_a_failed_auth_keeps_the_protocol() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert!(
            c.cmds(&["ACL", "SETUSER", "hu", "on", ">pw", "~*", "+@all"])
                .is_ok()
        );
        let r = c.cmds(&["HELLO", "3", "AUTH", "hu", "wrong"]);
        assert!(r.error_msg().starts_with("WRONGPASS"), "{r:?}");
        let info = c.cmds(&["CLIENT", "INFO"]);
        assert!(info.as_str().contains(" resp=2 "), "{}", info.as_str());
        assert_eq!(c.cmds(&["HSET", "hp:h", "f", "v"]).as_int(), 1);
        assert!(matches!(c.cmds(&["HGETALL", "hp:h"]), RespValue::Array(_)));
    });
}

/// PSYNC and SYNC inside MULTI are refused, and the transaction with them,
/// as in Redis (they are `no-multi`): they used to hand the connection over
/// as a replica.
#[test]
fn test_psync_and_sync_inside_multi_are_refused() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        for sync in [&["PSYNC", "?", "-1"][..], &["SYNC"]] {
            assert!(c.cmds(&["MULTI"]).is_ok());
            assert_eq!(
                c.cmds(sync).error_msg(),
                "ERR Command not allowed inside a transaction"
            );
            assert!(c.cmds(&["EXEC"]).error_msg().starts_with("EXECABORT"));
            assert_eq!(c.cmds(&["PING"]).as_str(), "PONG");
        }
    });
}

/// An EXEC refused by the user's rules discards the transaction, as Redis
/// does: it used to be failed and left open.
#[test]
fn test_an_exec_refused_by_acl_discards_the_transaction() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&[
                    "ACL", "SETUSER", "noexec", "on", ">pw", "~*", "+@all", "-exec"
                ])
                .is_ok()
        );
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "noexec", "pw"]).is_ok());
        assert!(c.cmds(&["WATCH", "ae:k"]).is_ok());
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["SET", "ae:k", "v"]).as_str(), "QUEUED");
        let r = c.cmds(&["EXEC"]);
        assert!(
            r.error_msg()
                .starts_with("EXECABORT Transaction discarded because of: NOPERM"),
            "{r:?}"
        );
        // Not in a transaction any more.
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert!(c.cmds(&["DISCARD"]).is_ok());
        assert!(c.cmds(&["GET", "ae:k"]).is_nil());
    });
}

/// A command with the wrong number of arguments, or no such command, is
/// refused for that before the user's rules are looked at, as in Redis.
#[test]
fn test_arity_and_existence_come_before_acl() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&[
                    "ACL", "SETUSER", "nowait", "on", ">pw", "~*", "+@all", "-wait"
                ])
                .is_ok()
        );
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "nowait", "pw"]).is_ok());
        assert_eq!(
            c.cmds(&["WAIT"]).error_msg(),
            "ERR wrong number of arguments for 'wait' command"
        );
        assert!(
            c.cmds(&["WAIT", "0", "0"])
                .error_msg()
                .starts_with("NOPERM")
        );
    });
}

/// A reply as text: a string as it is, anything else as Rust shows it.
fn text(v: &RespValue) -> String {
    match v {
        RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        RespValue::SimpleStr(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// The entries of the ACL log, newest first, each as its fields by name.
fn acl_log(c: &mut Conn) -> Vec<Vec<(String, String)>> {
    let log = c.cmds(&["ACL", "LOG"]);
    log.as_array()
        .iter()
        .map(|entry| {
            let fields: Vec<(String, String)> = match entry {
                RespValue::Array(flat) => flat
                    .chunks(2)
                    .map(|kv| (kv[0].as_str().to_string(), text(&kv[1])))
                    .collect(),
                RespValue::Map(pairs) => pairs
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), text(v)))
                    .collect(),
                other => panic!("not an ACL log entry: {other:?}"),
            };
            fields
        })
        .collect()
}

fn field<'a>(entry: &'a [(String, String)], name: &str) -> &'a str {
    &entry.iter().find(|(k, _)| k == name).unwrap().1
}

/// No such command, the wrong number of arguments, and a command that may
/// not be part of a transaction are each refused for that, before the
/// user's ACL rules, as in Redis; and none of them goes to the ACL log.
#[test]
fn test_refusals_before_acl_are_not_logged() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&[
                    "ACL", "SETUSER", "getonly", "on", ">pw", "~*", "-@all", "+get", "+multi",
                    "+exec"
                ])
                .is_ok()
        );
        assert!(admin.cmds(&["ACL", "LOG", "RESET"]).is_ok());
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "getonly", "pw"]).is_ok());

        let r = c.cmds(&["FOO", "bar"]);
        assert!(
            r.error_msg().starts_with("ERR unknown command 'FOO'"),
            "{r:?}"
        );
        assert_eq!(
            c.cmds(&["GET"]).error_msg(),
            "ERR wrong number of arguments for 'get' command"
        );
        assert_eq!(
            c.cmds(&["WAIT"]).error_msg(),
            "ERR wrong number of arguments for 'wait' command"
        );
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(
            c.cmds(&["PSYNC", "?", "-1"]).error_msg(),
            "ERR Command not allowed inside a transaction"
        );
        assert!(c.cmds(&["EXEC"]).error_msg().starts_with("EXECABORT"));
        assert!(acl_log(&mut admin).is_empty(), "{:?}", acl_log(&mut admin));

        // What the rules refuse is logged.
        assert!(c.cmds(&["SET", "k", "v"]).error_msg().starts_with("NOPERM"));
        assert_eq!(acl_log(&mut admin).len(), 1);
    });
}

/// An EXEC refused by the ACL rules drops the connection's watches with the
/// transaction, and MONITOR sees it, as Redis's `execCommandAbort` does.
#[test]
fn test_an_exec_refused_by_acl_drops_its_watches_and_is_monitored() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&[
                    "ACL", "SETUSER", "noexec2", "on", ">pw", "~*", "+@all", "-exec"
                ])
                .is_ok()
        );
        let mut monitor = Conn::connect(port);
        assert!(monitor.cmds(&["MONITOR"]).is_ok());
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "noexec2", "pw"]).is_ok());
        assert!(c.cmds(&["WATCH", "aw:k"]).is_ok());
        let info = c.cmds(&["CLIENT", "INFO"]);
        assert!(info.as_str().contains(" watch=1 "), "{}", info.as_str());
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["SET", "aw:k", "v"]).as_str(), "QUEUED");
        assert!(c.cmds(&["EXEC"]).error_msg().starts_with("EXECABORT"));
        let info = c.cmds(&["CLIENT", "INFO"]);
        assert!(info.as_str().contains(" watch=0 "), "{}", info.as_str());
        assert!(info.as_str().contains(" multi=-1 "), "{}", info.as_str());

        // MONITOR: the MULTI, and then the EXEC that was refused.
        let mut seen = Vec::new();
        loop {
            let line = monitor.read_value();
            let line = line.as_str().to_string();
            if line.contains("\"EXEC\"") {
                break;
            }
            seen.push(line);
            assert!(seen.len() < 50, "MONITOR never showed the EXEC: {seen:?}");
        }
    });
}

/// A command run by EXEC goes to the slow log under the client's name as
/// it is when the command runs.
#[test]
fn test_the_slow_log_names_the_client_as_it_is_inside_exec() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert!(
            c.cmds(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
                .is_ok()
        );
        assert!(c.cmds(&["CLIENT", "SETNAME", "before"]).is_ok());
        assert!(c.cmds(&["SLOWLOG", "RESET"]).is_ok());
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["CLIENT", "SETNAME", "during"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["ECHO", "slow-name"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["EXEC"]).as_array().len(), 2);
        let log = c.cmds(&["SLOWLOG", "GET", "100"]);
        let echoes: Vec<&[RespValue]> = log
            .as_array()
            .iter()
            .map(|entry| entry.as_array())
            .filter(|entry| entry[3].as_array().first().map(|a| a.as_str()) == Some("ECHO"))
            .collect();
        assert!(!echoes.is_empty(), "{log:?}");
        for entry in echoes {
            assert_eq!(entry[5].as_str(), "during", "{entry:?}");
        }
    });
}

/// HELLO as Redis's `helloCommand`: protocols 2 and 3 only, AUTH and
/// SETNAME in any order, the name checked before the AUTH, and a failed
/// AUTH in the ACL log.
#[test]
fn test_hello_options_as_redis() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "hu2", "on", ">pw", "~*", "+@all"])
                .is_ok()
        );
        assert!(admin.cmds(&["ACL", "LOG", "RESET"]).is_ok());
        let mut c = Conn::connect(port);

        for ver in ["0", "1", "4"] {
            let r = c.cmds(&["HELLO", ver]);
            assert!(r.error_msg().starts_with("NOPROTO"), "{ver}: {r:?}");
        }
        assert_eq!(
            c.cmds(&["HELLO", "x"]).error_msg(),
            "ERR Protocol version is not an integer or out of range"
        );
        assert_eq!(
            c.cmds(&["HELLO", "2", "AUTH", "hu2"]).error_msg(),
            "ERR Syntax error in HELLO option 'AUTH'"
        );
        assert_eq!(
            c.cmds(&["HELLO", "2", "BOGUS"]).error_msg(),
            "ERR Syntax error in HELLO option 'BOGUS'"
        );
        // The name is refused before the AUTH is tried: nothing logged.
        assert_eq!(
            c.cmds(&["HELLO", "2", "SETNAME", "bad name", "AUTH", "hu2", "wrong"])
                .error_msg(),
            "ERR Client names cannot contain spaces, newlines or special characters."
        );
        assert!(acl_log(&mut admin).is_empty());

        // A failed AUTH is logged.
        let r = c.cmds(&["HELLO", "2", "AUTH", "hu2", "wrong"]);
        assert!(r.error_msg().starts_with("WRONGPASS"), "{r:?}");
        let log = acl_log(&mut admin);
        assert_eq!(log.len(), 1, "{log:?}");
        assert!(field(&log[0], "reason").contains("auth"), "{log:?}");
        assert!(field(&log[0], "username").contains("hu2"), "{log:?}");
        assert_eq!(field(&log[0], "object"), "HELLO", "{log:?}");
        assert_eq!(field(&log[0], "context"), "toplevel", "{log:?}");

        // SETNAME before AUTH.
        let hello = c.cmds(&["HELLO", "2", "SETNAME", "n1", "AUTH", "hu2", "pw"]);
        assert_eq!(hello.as_array()[0].as_str(), "server");
        assert_eq!(c.cmds(&["CLIENT", "GETNAME"]).as_str(), "n1");
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "hu2");
    });
}

/// A failed AUTH or HELLO AUTH run by EXEC is logged in context `multi`,
/// with the command as the client sent it as the object.
#[test]
fn test_a_failed_auth_inside_exec_is_logged_in_context_multi() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "mu", "on", ">pw", "~*", "+@all"])
                .is_ok()
        );
        assert!(admin.cmds(&["ACL", "LOG", "RESET"]).is_ok());
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["auth", "mu", "wrong"]).as_str(), "QUEUED");
        assert_eq!(
            c.cmds(&["Hello", "2", "AUTH", "mu", "bad"]).as_str(),
            "QUEUED"
        );
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert!(replies[0].error_msg().starts_with("WRONGPASS"));
        assert!(replies[1].error_msg().starts_with("WRONGPASS"));
        let log = acl_log(&mut admin);
        assert_eq!(log.len(), 2, "{log:?}");
        // Newest first.
        assert_eq!(field(&log[0], "object"), "Hello");
        assert_eq!(field(&log[1], "object"), "auth");
        for entry in &log {
            assert_eq!(field(entry, "reason"), "auth");
            assert_eq!(field(entry, "context"), "multi");
            assert_eq!(field(entry, "username"), "mu");
        }
    });
}

/// More of HELLO, as Redis's `helloCommand`.
#[test]
fn test_hello_edge_cases_as_redis() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        // No version: the protocol stays as it is.
        let hello = c.cmds(&["HELLO"]);
        let fields = hello.as_array();
        let proto = fields.iter().position(|f| f.as_str() == "proto").unwrap();
        assert_eq!(fields[proto + 1], RespValue::Int(2));
        // An option without its value.
        assert_eq!(
            c.cmds(&["HELLO", "3", "SETNAME"]).error_msg(),
            "ERR Syntax error in HELLO option 'SETNAME'"
        );
        // The version is checked before the options.
        assert!(
            c.cmds(&["HELLO", "4", "BOGUS"])
                .error_msg()
                .starts_with("NOPROTO")
        );
        // An integer as Redis reads one.
        for ver in ["+3", "03", " 3", "3 "] {
            assert_eq!(
                c.cmds(&["HELLO", ver]).error_msg(),
                "ERR Protocol version is not an integer or out of range",
                "{ver:?}"
            );
        }
        // Options given twice: the last one counts.
        assert!(matches!(
            c.cmds(&["HELLO", "2", "SETNAME", "first", "SETNAME", "second"]),
            RespValue::Array(_)
        ));
        assert_eq!(c.cmds(&["CLIENT", "GETNAME"]).as_str(), "second");
        let info = c.cmds(&["CLIENT", "INFO"]);
        assert!(info.as_str().contains(" resp=2 "), "{}", info.as_str());
    });
}

/// AUTH as Redis: no more than a user and a password; a password is any
/// bytes, so a `nopass` user takes one that is not UTF-8, and a user set
/// by the hash of such bytes is logged in with them; a user name that is
/// not UTF-8 is nobody's, not `default`'s.
#[test]
fn test_auth_arguments_as_redis() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "au", "on", ">pw", "~*", "+@all"])
                .is_ok()
        );
        let mut c = Conn::connect(port);
        assert_eq!(
            c.cmds(&["AUTH", "au", "pw", "extra"]).error_msg(),
            "ERR syntax error"
        );
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "default");
        let r = c.cmd(&[&b"AUTH"[..], &b"\xff\xfe"[..], &b"pw"[..]]);
        assert!(r.error_msg().starts_with("WRONGPASS"), "{r:?}");
        let r = c.cmd(&[&b"AUTH"[..], &b"au"[..], &b"\xff"[..]]);
        assert!(r.error_msg().starts_with("WRONGPASS"), "{r:?}");
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "default");
        assert!(c.cmds(&["AUTH", "au", "pw"]).is_ok());

        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "np", "on", "nopass", "~*", "+@all"])
                .is_ok()
        );
        assert!(c.cmd(&[&b"AUTH"[..], &b"np"[..], &b"\xff"[..]]).is_ok());
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "np");

        let raw_password = b"\xff\x00pw";
        let hash = format!("#{}", kividb::acl::sha256_hex(raw_password));
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "hb", "on", &hash, "~*", "+@all"])
                .is_ok()
        );
        assert!(
            c.cmd(&[&b"AUTH"[..], &b"hb"[..], &raw_password[..]])
                .is_ok()
        );
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "hb");
    });
}

/// The ACL log merges an event into an entry like it, as Redis does: same
/// reason, context, object and user, within 60 s. A refusal inside MULTI is
/// in context `multi`, and so a separate entry.
#[test]
fn test_the_acl_log_merges_entries_like_redis() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "lu", "on", ">pw", "~*", "+@all", "-get"])
                .is_ok()
        );
        assert!(admin.cmds(&["ACL", "LOG", "RESET"]).is_ok());
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "lu", "pw"]).is_ok());
        for _ in 0..3 {
            assert!(c.cmds(&["GET", "k"]).error_msg().starts_with("NOPERM"));
        }
        let log = acl_log(&mut admin);
        assert_eq!(log.len(), 1, "{log:?}");
        assert_eq!(field(&log[0], "count"), "Int(3)");
        assert_eq!(field(&log[0], "context"), "toplevel");
        assert_eq!(field(&log[0], "object"), "get");
        let created: u64 = field(&log[0], "timestamp-created")
            .trim_start_matches("Int(")
            .trim_end_matches(')')
            .parse()
            .unwrap();
        let updated: u64 = field(&log[0], "timestamp-last-updated")
            .trim_start_matches("Int(")
            .trim_end_matches(')')
            .parse()
            .unwrap();
        assert!(updated >= created);

        assert!(c.cmds(&["MULTI"]).is_ok());
        assert!(c.cmds(&["GET", "k"]).error_msg().starts_with("NOPERM"));
        assert!(c.cmds(&["DISCARD"]).is_ok());
        let log = acl_log(&mut admin);
        assert_eq!(log.len(), 2, "{log:?}");
        assert_eq!(field(&log[0], "context"), "multi");
        assert_eq!(field(&log[0], "count"), "Int(1)");
    });
}

/// A command refused for want of AUTH is not in the ACL log, as in Redis:
/// no user was refused anything.
#[test]
fn test_noauth_writes_no_acl_log_entry() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "default", "resetpass", ">pw"])
                .is_ok()
        );
        assert!(admin.cmds(&["ACL", "LOG", "RESET"]).is_ok());
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["GET", "k"]).error_msg().starts_with("NOAUTH"));
        assert!(c.cmds(&["SET", "k", "v"]).error_msg().starts_with("NOAUTH"));
        assert!(acl_log(&mut admin).is_empty());
    });
}

/// BGREWRITEAOF and FAILOVER are not `no-multi` in Redis: they are queued,
/// and run by EXEC.
#[test]
fn test_bgrewriteaof_and_failover_are_queued() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["BGREWRITEAOF"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["FAILOVER"]).as_str(), "QUEUED");
        assert_eq!(c.cmds(&["PING"]).as_str(), "QUEUED");
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert_eq!(replies.len(), 3);
        assert_eq!(replies[2].as_str(), "PONG");
    });
}

/// A queued command refused by rules changed since it was queued is
/// logged in context `multi`, as Redis's EXEC re-check logs it.
#[test]
fn test_the_exec_recheck_is_logged_in_context_multi() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "rc", "on", ">pw", "~*", "+@all"])
                .is_ok()
        );
        assert!(admin.cmds(&["ACL", "LOG", "RESET"]).is_ok());
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "rc", "pw"]).is_ok());
        assert!(c.cmds(&["MULTI"]).is_ok());
        assert_eq!(c.cmds(&["GET", "rc:k"]).as_str(), "QUEUED");
        assert!(admin.cmds(&["ACL", "SETUSER", "rc", "-get"]).is_ok());
        let exec = c.cmds(&["EXEC"]);
        let replies = exec.as_array();
        assert!(replies[0].error_msg().starts_with("NOPERM"), "{replies:?}");
        let log = acl_log(&mut admin);
        assert_eq!(log.len(), 1, "{log:?}");
        assert_eq!(field(&log[0], "context"), "multi");
        assert_eq!(field(&log[0], "object"), "get");
    });
}

/// ACL LOG as Redis writes it: ten fields, in its order, `entry-id` from
/// 0, the client as CLIENT INFO describes it, `age-seconds` a double.
#[test]
fn test_acl_log_reply_as_redis() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "lf", "on", ">pw", "~*", "+@all", "-get"])
                .is_ok()
        );
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "lf", "pw"]).is_ok());
        assert!(c.cmds(&["CLIENT", "SETNAME", "logged"]).is_ok());
        assert!(c.cmds(&["GET", "k"]).error_msg().starts_with("NOPERM"));
        let log = acl_log(&mut admin);
        assert_eq!(log.len(), 1);
        let names: Vec<&str> = log[0].iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            names,
            [
                "count",
                "reason",
                "context",
                "object",
                "username",
                "age-seconds",
                "client-info",
                "entry-id",
                "timestamp-created",
                "timestamp-last-updated"
            ]
        );
        assert_eq!(field(&log[0], "entry-id"), "Int(0)");
        let client = field(&log[0], "client-info");
        assert!(
            client.starts_with("id=")
                && client.contains(" addr=")
                && client.contains(" name=logged "),
            "{client}"
        );
        assert!(field(&log[0], "age-seconds").parse::<f64>().is_ok());
    });
}

/// `acllog-max-len`: the ACL log keeps that many entries, and none at 0.
#[test]
fn test_acllog_max_len() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        let got = admin.cmds(&["CONFIG", "GET", "acllog-max-len"]);
        assert_eq!(got.as_array()[1].as_str(), "128");
        assert!(
            admin
                .cmds(&[
                    "ACL", "SETUSER", "ml", "on", ">pw", "~*", "+@all", "-get", "-set", "-del"
                ])
                .is_ok()
        );
        assert!(
            admin
                .cmds(&["CONFIG", "SET", "acllog-max-len", "2"])
                .is_ok()
        );
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "ml", "pw"]).is_ok());
        assert!(c.cmds(&["GET", "k"]).error_msg().starts_with("NOPERM"));
        assert!(c.cmds(&["SET", "k", "v"]).error_msg().starts_with("NOPERM"));
        assert!(c.cmds(&["DEL", "k"]).error_msg().starts_with("NOPERM"));
        let log = acl_log(&mut admin);
        let objects: Vec<&str> = log.iter().map(|e| field(e, "object")).collect();
        assert_eq!(objects, ["del", "set"]);
        assert!(
            admin
                .cmds(&["CONFIG", "SET", "acllog-max-len", "0"])
                .is_ok()
        );
        assert!(c.cmds(&["GET", "k"]).error_msg().starts_with("NOPERM"));
        assert!(acl_log(&mut admin).is_empty());
    });
}

/// ACL LOG's count, as Redis's: ten entries without one, none for a
/// negative one, and nothing after it.
#[test]
fn test_acl_log_count_as_redis() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        assert!(
            admin
                .cmds(&["ACL", "SETUSER", "none", "on", ">pw", "~*", "-@all"])
                .is_ok()
        );
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["AUTH", "none", "pw"]).is_ok());
        let refused: [&[&str]; 12] = [
            &["GET", "k"],
            &["SET", "k", "v"],
            &["DEL", "k"],
            &["INCR", "k"],
            &["DECR", "k"],
            &["APPEND", "k", "v"],
            &["STRLEN", "k"],
            &["LPUSH", "k", "v"],
            &["RPUSH", "k", "v"],
            &["SADD", "k", "v"],
            &["HSET", "k", "f", "v"],
            &["ZADD", "k", "1", "m"],
        ];
        for cmd in refused {
            assert!(c.cmds(cmd).error_msg().starts_with("NOPERM"), "{cmd:?}");
        }
        assert_eq!(admin.cmds(&["ACL", "LOG"]).as_array().len(), 10);
        assert_eq!(admin.cmds(&["ACL", "LOG", "20"]).as_array().len(), 12);
        assert_eq!(admin.cmds(&["ACL", "LOG", "-1"]).as_array().len(), 0);
        assert_eq!(
            admin.cmds(&["ACL", "LOG", "+1"]).error_msg(),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            admin.cmds(&["ACL", "log", "1", "2"]).error_msg(),
            "ERR unknown subcommand or wrong number of arguments for 'log'. Try ACL HELP."
        );

        // Under RESP3, each entry is a map.
        assert!(matches!(admin.cmds(&["HELLO", "3"]), RespValue::Map(_)));
        let log = admin.cmds(&["ACL", "LOG", "1"]);
        let RespValue::Map(entry) = &log.as_array()[0] else {
            panic!("not a map: {log:?}");
        };
        assert_eq!(entry.len(), 10);
        assert_eq!(entry[0].0.as_str(), "count");
        assert_eq!(entry[5].0.as_str(), "age-seconds");
        assert!(entry[5].1.as_str().parse::<f64>().is_ok(), "{entry:?}");
    });
}

/// A negative `acllog-max-len` is refused, and changes nothing.
#[test]
fn test_a_negative_acllog_max_len_is_refused() {
    on_each_io_model(|port| {
        let mut c = Conn::connect(port);
        assert!(
            c.cmds(&["CONFIG", "SET", "acllog-max-len", "-1"])
                .is_error()
        );
        let got = c.cmds(&["CONFIG", "GET", "acllog-max-len"]);
        assert_eq!(got.as_array()[1].as_str(), "128");
    });
}

/// ACL SETUSER takes a password that is not UTF-8, as Redis does, to add
/// (`>`) and to remove (`<`).
#[test]
fn test_setuser_takes_a_binary_password() {
    on_each_io_model(|port| {
        let mut admin = Conn::connect(port);
        let set = |admin: &mut Conn, rule: &[u8]| {
            admin.cmd(&[
                &b"ACL"[..],
                &b"SETUSER"[..],
                &b"bu"[..],
                &b"on"[..],
                rule,
                &b"~*"[..],
                &b"+@all"[..],
            ])
        };
        assert!(set(&mut admin, b">\xff\x01").is_ok());
        let mut c = Conn::connect(port);
        assert!(c.cmd(&[&b"AUTH"[..], &b"bu"[..], &b"\xff\x01"[..]]).is_ok());
        assert_eq!(c.cmds(&["ACL", "WHOAMI"]).as_str(), "bu");
        assert!(set(&mut admin, b"<\xff\x01").is_ok());
        let r = c.cmd(&[&b"AUTH"[..], &b"bu"[..], &b"\xff\x01"[..]]);
        assert!(r.error_msg().starts_with("WRONGPASS"), "{r:?}");
        // Anything else that is not UTF-8 is still no rule.
        assert_eq!(set(&mut admin, b"~\xff").error_msg(), "ERR invalid rule");
    });
}
