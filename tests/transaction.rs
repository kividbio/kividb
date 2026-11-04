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
