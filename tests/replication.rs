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

use kividb::config::{Config, IoModel};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

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

    // Block until the listener is actually accepting connections before returning.
    // Tests that immediately spawn a replica pointed at `port` (e.g.
    // test_wait_ack_tracking) race the replica's *own* internal connect attempt
    // against this thread's startup; that client only retries after a multi-second
    // backoff on refusal, which the test's short wait can't absorb. Without this,
    // "server not bound yet" and "replica lost the race" are indistinguishable
    // flakes under CI load.
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server on port {port} did not start listening within 5s");
}

// ═════════════════════════════════════════════════════════════════════════════
// REPLICATION COMMANDS
// ═════════════════════════════════════════════════════════════════════════════

// ROLE on a standalone master returns ["master", offset, []]
#[test]
fn test_role_master() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let r = c.cmds(&["ROLE"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_str(), "master");
    // offset is an integer
    let _ = arr[1].as_int();
    // replica list is empty array
    assert_eq!(arr[2].as_array().len(), 0);
}

// INFO replication returns role:master on standalone
#[test]
fn test_info_replication_master() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    let info = c.cmds(&["INFO", "replication"]);
    let text = info.as_str();
    assert!(text.contains("# Replication"));
    assert!(text.contains("role:master"));
    assert!(text.contains("connected_slaves:"));
    assert!(text.contains("master_replid:"));
    assert!(text.contains("master_repl_offset:"));
}

// WAIT returns 0 when no replicas are connected
#[test]
fn test_wait_no_replicas() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // WAIT 1 0 — wait for 1 replica, 0ms timeout
    let r = c.cmds(&["WAIT", "1", "0"]);
    assert_eq!(r.as_int(), 0);
}

// WAITAOF returns [1, 0] — local AOF acked, no replica AOF
#[test]
fn test_waitaof() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // With the AOF off, nothing is on disk by one: asking for a local fsync
    // is an error, as in Redis, and none is reported. It reported one (#137).
    assert!(matches!(
        c.cmds(&["WAITAOF", "1", "0", "0"]),
        RespValue::Error(_)
    ));
    let r = c.cmds(&["WAITAOF", "0", "0", "0"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_int(), 0);
    assert_eq!(arr[1].as_int(), 0);
}

// REPLICAOF NO ONE on a standalone master stays as master
#[test]
fn test_replicaof_no_one() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["REPLICAOF", "NO", "ONE"]).is_ok());

    // Still reports as master
    let info = c.cmds(&["INFO", "replication"]);
    assert!(info.as_str().contains("role:master"));
}

// SLAVEOF is an alias for REPLICAOF
#[test]
fn test_slaveof_no_one() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["SLAVEOF", "NO", "ONE"]).is_ok());
}

// REPLCONF is accepted and returns OK (used by replicas during handshake)
#[test]
fn test_replconf_accepted() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    assert!(c.cmds(&["REPLCONF", "listening-port", "6399"]).is_ok());
    assert!(c.cmds(&["REPLCONF", "capa", "psync2"]).is_ok());
    assert!(c.cmds(&["REPLCONF", "ACK", "0"]).is_ok());
}

// WAIT with a real replica — verifies ack offset tracking
// This test spins up a master and a replica in-process, issues a write
// on the master, then WAIT 1 <timeout> and confirms it returns 1
// once the replica has caught up.
#[test]
fn test_wait_ack_tracking() {
    let master_port = free_port();
    let replica_port = free_port();

    // Start master
    start_server(master_port);

    // Start replica pointed at master
    thread::spawn(move || {
        let config = Config {
            port: replica_port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: IoModel::Tokio,
            replicaof: Some(vec!["127.0.0.1".to_string(), master_port.to_string()]),
            replica_announced_port: replica_port,
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

    let mut master = Conn::connect(master_port);
    let mut replica = Conn::connect(replica_port);

    // Give the replication handshake time to complete
    thread::sleep(std::time::Duration::from_millis(1000));

    // Write on master
    assert!(master.cmds(&["SET", "replicated_key", "hello"]).is_ok());

    // Give the hub time to process the Propagate message and update repl_offset
    thread::sleep(std::time::Duration::from_millis(50));

    // WAIT 1 replica, up to 2000ms
    let r = master.cmds(&["WAIT", "1", "3000"]);
    assert_eq!(r.as_int(), 1, "WAIT should return 1 once replica has acked");

    // Confirm the replica actually has the key
    let val = replica.cmds(&["GET", "replicated_key"]);
    assert_eq!(val.as_str(), "hello");
}

#[test]
fn test_waitaof_with_aof_enabled() {
    let port = free_port();
    thread::spawn(move || {
        let config = Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: true, // AOF enabled
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

    let mut c = Conn::connect(port);

    // Issue a write — this increments AOF_WRITTEN
    assert!(c.cmds(&["SET", "aof_wait_key", "val"]).is_ok());

    // WAITAOF 1 0 2000 — wait for local fsync, up to 2 seconds
    // The AOF writer fsyncs every 1 second so this should resolve within ~1s
    let r = c.cmds(&["WAITAOF", "1", "0", "2000"]);
    let arr = r.as_array();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_int(), 1, "local AOF should be fsynced");
    assert_eq!(arr[1].as_int(), 0, "replica AOF not tracked");
}

// ═════════════════════════════════════════════════════════════════════════════
// REPLICA RE-POINTING
//
// A replica that is re-pointed at a *different* master stays "slave" the whole
// time, so the old exit condition ("role changed away from slave") never fired
// and the previously-spawned replication task ran forever alongside the new
// one. The fix is a replication epoch on ReplState: every start_replica claims
// a new one and exits as soon as it sees a newer one.
// ═════════════════════════════════════════════════════════════════════════════

/// INFO/ROLE must report the master a REPLICAOF actually pointed us at.
///
/// These used to read `config.replicaof_addr()` — the boot-time `--replicaof`
/// value, which REPLICAOF never updates — so a re-pointed replica reported the
/// old host forever. That is the symptom an orchestrator read as "never
/// converged", independently of whether the re-point itself worked.
#[test]
fn test_replicaof_updates_reported_master_addr() {
    let master_port = free_port();
    let replica_port = free_port();
    start_server(master_port);
    start_server(replica_port);

    let mut c = Conn::connect(replica_port);
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &master_port.to_string()])
            .is_ok()
    );

    // Give the replica client a moment to publish its target and connect.
    let mut info = String::new();
    for _ in 0..40 {
        thread::sleep(Duration::from_millis(50));
        info = c.cmds(&["INFO", "replication"]).as_str().to_string();
        if info.contains(&format!("master_port:{master_port}")) {
            break;
        }
    }
    assert!(
        info.contains("role:slave"),
        "expected role:slave, got:\n{info}"
    );
    assert!(
        info.contains(&format!("master_port:{master_port}")),
        "INFO must report the master REPLICAOF pointed at, got:\n{info}"
    );

    // ROLE must agree with INFO.
    let role = c.cmds(&["ROLE"]);
    let arr = role.as_array();
    assert_eq!(arr[0].as_str(), "slave");
    assert_eq!(arr[2].as_int(), master_port as i64);
}

/// Re-pointing at a second master must actually move — the stale task for the
/// first master must not keep the old address reported.
#[test]
fn test_replicaof_repoint_between_masters_converges() {
    let master_a = free_port();
    let master_b = free_port();
    let replica_port = free_port();
    start_server(master_a);
    start_server(master_b);
    start_server(replica_port);

    let mut c = Conn::connect(replica_port);
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &master_a.to_string()])
            .is_ok()
    );

    for _ in 0..40 {
        thread::sleep(Duration::from_millis(50));
        if c.cmds(&["INFO", "replication"])
            .as_str()
            .contains(&format!("master_port:{master_a}"))
        {
            break;
        }
    }

    // Re-point without ever leaving "slave" — the exact case the old
    // role-only exit check could not detect.
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &master_b.to_string()])
            .is_ok()
    );

    let mut info = String::new();
    for _ in 0..60 {
        thread::sleep(Duration::from_millis(50));
        info = c.cmds(&["INFO", "replication"]).as_str().to_string();
        if info.contains(&format!("master_port:{master_b}")) {
            break;
        }
    }
    assert!(
        info.contains(&format!("master_port:{master_b}")),
        "replica must converge on the new master, got:\n{info}"
    );
    assert!(
        !info.contains(&format!("master_port:{master_a}")),
        "replica must not still report the old master, got:\n{info}"
    );

    // The stale task must actually be *gone*, not merely out-voted on what the
    // replica's own INFO reports — the originally-reported symptom was the old
    // master still seeing a live replica connection because nothing ever
    // cancelled the superseded task.
    //
    // The old master only observes the closed socket when it next tries to push
    // to it, so drive a write rather than waiting on a counter it has no reason
    // to refresh on its own. A still-running stale task would keep the
    // connection open and stay registered across these writes.
    let mut a = Conn::connect(master_a);
    let mut old_master_info = String::new();
    for i in 0..40 {
        a.cmds(&["SET", &format!("probe:{i}"), "1"]);
        thread::sleep(Duration::from_millis(50));
        old_master_info = a.cmds(&["INFO", "replication"]).as_str().to_string();
        if old_master_info.contains("connected_slaves:0") {
            break;
        }
    }
    assert!(
        old_master_info.contains("connected_slaves:0"),
        "superseded replication task must drop its connection to the old \
         master, got:\n{old_master_info}"
    );

    // The re-pointed replica must be attached to the new master, and exactly
    // once — not one registration per REPLICAOF issued.
    let mut b = Conn::connect(master_b);
    let mut new_master_info = String::new();
    for _ in 0..60 {
        thread::sleep(Duration::from_millis(50));
        new_master_info = b.cmds(&["INFO", "replication"]).as_str().to_string();
        if new_master_info.contains("connected_slaves:1") {
            break;
        }
    }
    assert!(
        new_master_info.contains("connected_slaves:1"),
        "new master should see exactly one replica, got:\n{new_master_info}"
    );
}

/// Re-issuing REPLICAOF for the master we already follow is a no-op.
///
/// Operators re-issue REPLICAOF on every reconcile pass; without this, each
/// pass would supersede the running task and force a fresh full resync.
#[test]
fn test_replicaof_same_master_is_noop() {
    let master_port = free_port();
    let replica_port = free_port();
    start_server(master_port);
    start_server(replica_port);

    let mut c = Conn::connect(replica_port);
    let addr = master_port.to_string();
    assert!(c.cmds(&["REPLICAOF", "127.0.0.1", &addr]).is_ok());

    for _ in 0..40 {
        thread::sleep(Duration::from_millis(50));
        if c.cmds(&["INFO", "replication"])
            .as_str()
            .contains("master_link_status:up")
        {
            break;
        }
    }

    // Repeated identical REPLICAOF calls all succeed and leave the link up.
    for _ in 0..5 {
        assert!(c.cmds(&["REPLICAOF", "127.0.0.1", &addr]).is_ok());
    }
    let info = c.cmds(&["INFO", "replication"]).as_str().to_string();
    assert!(
        info.contains("master_link_status:up"),
        "repeated REPLICAOF must not tear down a healthy link, got:\n{info}"
    );
}

/// REPLICAOF NO ONE promotes and clears the reported master.
#[test]
fn test_replicaof_no_one_clears_master_addr() {
    let master_port = free_port();
    let replica_port = free_port();
    start_server(master_port);
    start_server(replica_port);

    let mut c = Conn::connect(replica_port);
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &master_port.to_string()])
            .is_ok()
    );
    for _ in 0..40 {
        thread::sleep(Duration::from_millis(50));
        if c.cmds(&["INFO", "replication"])
            .as_str()
            .contains("role:slave")
        {
            break;
        }
    }

    assert!(c.cmds(&["REPLICAOF", "NO", "ONE"]).is_ok());
    let info = c.cmds(&["INFO", "replication"]).as_str().to_string();
    assert!(info.contains("role:master"), "got:\n{info}");
    // A master reports no master_host line at all.
    assert!(
        !info.contains(&format!("master_port:{master_port}")),
        "promotion must clear the recorded master, got:\n{info}"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// LINK LIVENESS, FULL-RESYNC GUARD, STREAM FIDELITY
// (problems found by orchestration tooling driving replicas)
// ═════════════════════════════════════════════════════════════════════════════

/// Start a server from an arbitrary config and wait until it accepts
/// connections. Returns its port.
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
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
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
#[cfg(feature = "lua")]
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

/// `mem_replication_backlog` is what the backlog holds: it was 0 (#152).
#[test]
fn test_info_memory_reports_the_replication_backlog() {
    let master = spawn_server(Config::default());
    let replica = spawn_server(replica_of(master));
    let mut r = Conn::connect(replica);
    wait_link_up(&mut r);
    let mut m = Conn::connect(master);
    m.cmds(&["SET", "backlog:k", &"v".repeat(1000)]);
    let backlog = |m: &mut Conn| -> u64 {
        m.cmds(&["INFO", "memory"])
            .as_str()
            .lines()
            .find_map(|line| line.strip_prefix("mem_replication_backlog:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    wait_until(
        "the backlog to hold the write",
        Duration::from_secs(10),
        || backlog(&mut m) >= 1000,
    );
}

/// A shard channel's message reaches a replica's subscribers, as a
/// channel's does: `SPUBLISH` was not sent down the replication stream
/// (#161).
#[test]
fn test_spublish_reaches_a_replicas_subscribers() {
    let master = spawn_server(Config::default());
    let replica = spawn_server(replica_of(master));
    let mut r = Conn::connect(replica);
    wait_link_up(&mut r);
    let mut sub = Conn::connect(replica);
    let ack = sub.cmds(&["SSUBSCRIBE", "news"]);
    assert_eq!(ack.as_array()[0].as_str(), "ssubscribe");
    let mut m = Conn::connect(master);
    m.cmds(&["SPUBLISH", "news", "hello"]);
    let message = sub.read_value();
    let parts = message.as_array();
    assert_eq!(parts[0].as_str(), "smessage");
    assert_eq!(parts[2].as_str(), "hello");
}

/// A replica given a `masterauth` still replicates a master that has no
/// password, and one that has that password. `AUTH <password>` is refused
/// where no password is set (#135): the replica logs in as the default user
/// by name.
#[test]
fn test_masterauth_works_with_and_without_a_password_on_the_master() {
    for master_pass in ["", "s3cret"] {
        let master = spawn_server(Config {
            requirepass: master_pass.to_string(),
            ..Default::default()
        });
        let replica = spawn_server(Config {
            masterauth: "s3cret".to_string(),
            ..replica_of(master)
        });
        let mut r = Conn::connect(replica);
        wait_link_up(&mut r);
        let mut m = Conn::connect(master);
        if !master_pass.is_empty() {
            assert!(m.cmds(&["AUTH", master_pass]).is_ok());
        }
        assert!(m.cmds(&["SET", "auth:k", "v"]).is_ok());
        wait_until(
            "the replica to have auth:k",
            Duration::from_secs(10),
            || r.cmds(&["GET", "auth:k"]) == RespValue::Bulk(b"v".to_vec()),
        );
    }
}

/// On a replica, WATCH sees the writes its master sends: an EXEC after the
/// master changed a watched key runs nothing. It ran (#159).
#[test]
fn test_watch_on_a_replica_sees_its_masters_writes() {
    let master = spawn_server(Config::default());
    let replica = spawn_server(replica_of(master));
    let mut r = Conn::connect(replica);
    wait_link_up(&mut r);
    let mut m = Conn::connect(master);
    assert!(m.cmds(&["SET", "w", "1"]).is_ok());
    wait_until("the replica to have w", Duration::from_secs(10), || {
        r.cmds(&["GET", "w"]) == RespValue::Bulk(b"1".to_vec())
    });
    assert!(r.cmds(&["WATCH", "w"]).is_ok());
    assert!(m.cmds(&["SET", "w", "from-master"]).is_ok());
    wait_until(
        "the replica to have the new w",
        Duration::from_secs(10),
        || {
            let mut probe = Conn::connect(replica);
            probe.cmds(&["GET", "w"]) == RespValue::Bulk(b"from-master".to_vec())
        },
    );
    assert!(r.cmds(&["MULTI"]).is_ok());
    r.cmds(&["GET", "w"]);
    assert_eq!(r.cmds(&["EXEC"]), RespValue::Nil);
}

/// A replica does not take replicas of its own. One that did sent its data
/// once and nothing after, while the replica of it reported its link up
/// (#144). It now says why, and the would-be replica's link stays down; a
/// master made a replica lets go of the replicas it had.
#[test]
fn test_a_replica_refuses_replicas_of_its_own() {
    let master = spawn_server(Config::default());
    let replica = spawn_server(replica_of(master));
    let mut r = Conn::connect(replica);
    wait_link_up(&mut r);

    let mut psync = Conn::connect(replica);
    let reply = psync.cmds(&["PSYNC", "?", "-1"]);
    assert!(
        matches!(&reply, RespValue::Error(e) if e.contains("does not chain replicas")),
        "PSYNC to a replica: {reply:?}"
    );

    let sub = spawn_server(replica_of(replica));
    let mut s = Conn::connect(sub);
    thread::sleep(Duration::from_secs(2));
    assert_eq!(
        repl_info(&mut s, "master_link_status").as_deref(),
        Some("down")
    );
    assert_eq!(repl_info(&mut r, "connected_slaves"), None);

    // A master with a replica, made a replica itself: the replica it had is
    // let go, and its link goes down rather than staying up with nothing in it.
    let other = spawn_server(Config::default());
    let follower = spawn_server(replica_of(other));
    let mut f = Conn::connect(follower);
    wait_link_up(&mut f);
    let mut o = Conn::connect(other);
    assert!(
        o.cmds(&["REPLICAOF", "127.0.0.1", &master.to_string()])
            .is_ok()
    );
    wait_until(
        "the follower's link to go down",
        Duration::from_secs(10),
        || repl_info(&mut f, "master_link_status").as_deref() == Some("down"),
    );
}

/// Link liveness, the reported symptom: a replica must not say its link is up when
/// it has no working connection to the master. Following a master that cannot
/// be reached at all, the node is a read-only replica with the link down from
/// the moment `REPLICAOF` returns — it used to stay a writable `master` until
/// the first sync succeeded.
#[test]
fn test_unreachable_master_is_reported_down_and_replica_is_read_only_at_once() {
    let port = spawn_server(Config::default());
    let mut c = Conn::connect(port);
    assert!(c.cmds(&["SET", "local", "data"]).is_ok());

    // Nothing listens here.
    let dead_port = free_port();
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &dead_port.to_string()])
            .is_ok()
    );

    assert_eq!(repl_info(&mut c, "role").as_deref(), Some("slave"));
    assert_eq!(
        repl_info(&mut c, "master_link_status").as_deref(),
        Some("down")
    );
    assert_eq!(
        repl_info(&mut c, "master_last_io_seconds_ago").as_deref(),
        Some("-1"),
        "no I/O has happened, so this must not read 0"
    );
    assert!(repl_info(&mut c, "master_link_down_since_seconds").is_some());

    let role = c.cmds(&["ROLE"]);
    let role = role.as_array();
    assert_eq!(role[0].as_str(), "slave");
    assert_ne!(
        role[3].as_str(),
        "connected",
        "ROLE must not claim a connected link to a master that is not there"
    );

    // Read-only immediately, and the local data is still served.
    assert!(matches!(c.cmds(&["SET", "x", "1"]), RespValue::Error(e) if e.starts_with("READONLY")));
    assert_eq!(c.cmds(&["GET", "local"]).as_str(), "data");

    // Promotion works while the replication client is stuck retrying.
    assert!(c.cmds(&["REPLICAOF", "NO", "ONE"]).is_ok());
    assert_eq!(repl_info(&mut c, "role").as_deref(), Some("master"));
    assert!(c.cmds(&["SET", "x", "1"]).is_ok());
}

/// A stand-in master that completes the handshake, sends an empty dataset and
/// one heartbeat, and then goes silent *without closing the connection* — what
/// a powered-off or partitioned master looks like from the replica's side.
/// Returns its port.
fn spawn_master_that_goes_silent() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                loop {
                    // Read one RESP array command.
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        return;
                    }
                    let argc: usize = line.trim().trim_start_matches('*').parse().unwrap_or(0);
                    let mut args = Vec::new();
                    for _ in 0..argc {
                        let mut len_line = String::new();
                        reader.read_line(&mut len_line).unwrap();
                        let len: usize = len_line.trim().trim_start_matches('$').parse().unwrap();
                        let mut data = vec![0u8; len + 2];
                        reader.read_exact(&mut data).unwrap();
                        data.truncate(len);
                        args.push(String::from_utf8_lossy(&data).to_ascii_uppercase());
                    }
                    match args.first().map(String::as_str) {
                        Some("PING") => stream.write_all(b"+PONG\r\n").unwrap(),
                        Some("REPLCONF") => stream.write_all(b"+OK\r\n").unwrap(),
                        Some("PSYNC") => {
                            let payload = kividb::snapshot::full_sync_payload(
                                &kividb::store::new_store(16),
                                None,
                                None,
                            )
                            .unwrap();
                            stream
                                .write_all(
                                    b"+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n",
                                )
                                .unwrap();
                            stream
                                .write_all(format!("${}\r\n", payload.len()).as_bytes())
                                .unwrap();
                            stream.write_all(&payload).unwrap();
                            // One heartbeat, so the replica knows this master
                            // sends them...
                            stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
                            // ...and then nothing, forever. Keep reading the
                            // replica's ACKs so the socket stays healthy at
                            // the TCP level.
                            let mut sink = [0u8; 256];
                            while reader.read(&mut sink).unwrap_or(0) > 0 {}
                            return;
                        }
                        _ => return,
                    }
                }
            });
        }
    });
    port
}

/// Link liveness, the mechanism: the master stops sending anything but the TCP
/// connection stays open. Nothing errors, so the only signal is silence. The
/// replica must give up on the link after `repl-timeout` and say so, rather
/// than report `up` until the kernel finally tears the socket down.
#[test]
fn test_replica_declares_a_silent_master_down_after_repl_timeout() {
    let master_port = spawn_master_that_goes_silent();
    let replica_port = spawn_server(Config {
        repl_timeout: 2,
        ..replica_of(master_port)
    });
    let mut c = Conn::connect(replica_port);

    wait_link_up(&mut c);
    assert_eq!(
        c.cmds(&["ROLE"]).as_array()[3].as_str(),
        "connected",
        "synced and streaming"
    );

    // No more bytes arrive. Within repl-timeout (2s) plus slack, the link is down.
    wait_until(
        "link down after the master went silent",
        Duration::from_secs(8),
        || repl_info(&mut c, "master_link_status").as_deref() == Some("down"),
    );
    assert_eq!(
        repl_info(&mut c, "master_last_io_seconds_ago").as_deref(),
        Some("-1")
    );
}

/// The flip side: a healthy master with nothing to replicate must *not* be
/// timed out. It writes a PING into the stream every
/// `repl-ping-replica-period`, which keeps the link demonstrably alive and
/// keeps the offsets moving — so they can be used to judge replication
/// progress even on an idle instance.
#[test]
fn test_idle_link_stays_up_and_offsets_advance_on_heartbeats() {
    let master_port = spawn_server(Config {
        repl_ping_replica_period: 1,
        ..Default::default()
    });
    let replica_port = spawn_server(Config {
        repl_timeout: 2,
        ..replica_of(master_port)
    });
    let mut master = Conn::connect(master_port);
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);

    // Well past repl-timeout, with no writes at all.
    thread::sleep(Duration::from_millis(4500));

    assert_eq!(
        repl_info(&mut replica, "master_link_status").as_deref(),
        Some("up"),
        "an idle but healthy link must not be timed out"
    );
    let last_io: i64 = repl_info(&mut replica, "master_last_io_seconds_ago")
        .unwrap()
        .parse()
        .unwrap();
    assert!((0..=2).contains(&last_io), "last io {last_io}s ago");

    let replica_offset: u64 = repl_info(&mut replica, "slave_repl_offset")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        replica_offset > 0,
        "heartbeats are stream content and must advance the replica's offset"
    );

    // The master's view of this replica converges on its own offset.
    wait_until(
        "replica offset to match the master's",
        Duration::from_secs(5),
        || {
            let master_offset = repl_info(&mut master, "master_repl_offset").unwrap();
            repl_info(&mut master, "slave0")
                .is_some_and(|row| row.contains(&format!("offset={master_offset},")))
        },
    );
    let row = repl_info(&mut master, "slave0").unwrap();
    assert!(
        row.contains(&format!("port={replica_port},")),
        "the replica must be listed by the port it serves clients on: {row}"
    );
    assert!(row.contains("lag=0") || row.contains("lag=1"), "{row}");

    // ROLE on the master lists the replica too.
    let role = master.cmds(&["ROLE"]);
    let replicas = role.as_array()[2].as_array();
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].as_array()[1].as_str(), replica_port.to_string());
}

/// Full-resync guard: a replica holding data is pointed at a master that has none. The
/// full resync must be refused and the local dataset kept.
#[test]
fn test_full_resync_from_an_empty_master_is_refused() {
    let empty_master = spawn_server(Config::default());
    let node = spawn_server(Config::default());
    let mut c = Conn::connect(node);
    for i in 0..502 {
        assert!(c.cmds(&["SET", &format!("key:{i}"), "v"]).is_ok());
    }

    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &empty_master.to_string()])
            .is_ok()
    );

    wait_until(
        "the refusal to be recorded",
        Duration::from_secs(10),
        || {
            repl_info(&mut c, "replica_full_syncs_refused")
                .is_some_and(|n| n.parse::<u64>().unwrap() >= 1)
        },
    );

    assert_eq!(
        c.cmds(&["DBSIZE"]).as_int(),
        502,
        "the local dataset must survive a refused resync"
    );
    assert_eq!(
        repl_info(&mut c, "master_link_status").as_deref(),
        Some("down"),
        "a replica that refused to sync is not in sync and must say so"
    );
    let reason = repl_info(&mut c, "replica_last_sync_refusal").expect("a reason is reported");
    assert!(reason.contains("502"), "{reason}");

    // The operator's explicit opt-in: with the guard off, the resync goes
    // through and the master's (empty) dataset replaces the local one.
    assert!(
        c.cmds(&["CONFIG", "SET", "replica-full-sync-guard", "off"])
            .is_ok()
    );
    wait_link_up(&mut c);
    assert_eq!(c.cmds(&["DBSIZE"]).as_int(), 0);
    assert!(repl_info(&mut c, "replica_last_sync_refusal").is_none());
}

/// The guard must not get in the way of the ordinary case: a master that has
/// data is the authority, even over a replica that has other data.
#[test]
fn test_full_resync_from_a_populated_master_replaces_local_data() {
    let master_port = spawn_server(Config::default());
    let mut master = Conn::connect(master_port);
    assert!(master.cmds(&["SET", "from-master", "1"]).is_ok());

    let node = spawn_server(Config::default());
    let mut c = Conn::connect(node);
    for i in 0..50 {
        assert!(c.cmds(&["SET", &format!("stale:{i}"), "v"]).is_ok());
    }
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &master_port.to_string()])
            .is_ok()
    );
    wait_link_up(&mut c);
    assert_eq!(c.cmds(&["DBSIZE"]).as_int(), 1);
    assert_eq!(c.cmds(&["GET", "from-master"]).as_str(), "1");
}

/// Key TTLs must cross a full resync. They used to be dropped: every expiring
/// key arrived on the replica as a permanent one.
#[test]
fn test_full_resync_carries_key_ttls() {
    let master_port = spawn_server(Config::default());
    let mut master = Conn::connect(master_port);
    assert!(master.cmds(&["SET", "session", "v", "EX", "1000"]).is_ok());
    assert!(master.cmds(&["SET", "forever", "v"]).is_ok());

    let replica_port = spawn_server(replica_of(master_port));
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);

    let ttl = replica.cmds(&["TTL", "session"]).as_int();
    assert!((1..=1000).contains(&ttl), "TTL on replica was {ttl}");
    assert_eq!(replica.cmds(&["TTL", "forever"]).as_int(), -1);
}

/// Writes that were missing from the propagation list ran on the master and
/// went nowhere. Each of these left the replica holding stale data.
#[test]
fn test_previously_unpropagated_writes_reach_the_replica() {
    let master_port = spawn_server(Config::default());
    let replica_port = spawn_server(replica_of(master_port));
    let mut master = Conn::connect(master_port);
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);

    assert_eq!(
        master
            .cmds(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"])
            .as_int(),
        4
    );
    assert_eq!(master.cmds(&["RPUSH", "src", "x", "y"]).as_int(), 2);
    assert!(master.cmds(&["SET", "gone", "v"]).is_ok());

    assert_eq!(
        master.cmds(&["ZREMRANGEBYSCORE", "z", "1", "1"]).as_int(),
        1
    );
    assert_eq!(master.cmds(&["ZREMRANGEBYRANK", "z", "0", "0"]).as_int(), 1);
    assert_eq!(
        master.cmds(&["ZREMRANGEBYLEX", "z", "[c", "[c"]).as_int(),
        1
    );
    // 1: every key set (Redis 8.10's reply).
    assert_eq!(
        master
            .cmds(&["MSETEX", "1", "msetex", "v", "EX", "100"])
            .as_int(),
        1
    );
    assert_eq!(
        master.cmds(&["BRPOPLPUSH", "src", "dst", "1"]).as_str(),
        "y"
    );

    assert_eq!(master.cmds(&["WAIT", "1", "3000"]).as_int(), 1);

    assert_eq!(replica.cmds(&["ZCARD", "z"]).as_int(), 1);
    assert_eq!(replica.cmds(&["GET", "msetex"]).as_str(), "v");
    assert!(replica.cmds(&["TTL", "msetex"]).as_int() > 0);
    assert_eq!(replica.cmds(&["LLEN", "src"]).as_int(), 1);
    assert_eq!(replica.cmds(&["LLEN", "dst"]).as_int(), 1);
}

/// A replica attached while the master is taking non-idempotent writes must
/// end up identical to it. The full-sync payload and the stream used to
/// overlap — a write landing during the dump was in both — so `INCR` counters
/// came out ahead on the replica and lists gained duplicate elements.
#[test]
fn test_replica_attached_under_write_load_matches_the_master() {
    let master_port = spawn_server(Config::default());
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Enough data that the dump takes a moment, plus writers that never pause.
    {
        let mut c = Conn::connect(master_port);
        for i in 0..2000 {
            c.cmds(&["RPUSH", &format!("filler:{i}"), "a", "b", "c", "d"]);
        }
    }
    let writers: Vec<_> = (0..3)
        .map(|w| {
            let stop = std::sync::Arc::clone(&stop);
            thread::spawn(move || {
                let mut c = Conn::connect(master_port);
                let mut n = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    c.cmds(&["INCR", &format!("counter:{}", n % 8)]);
                    c.cmds(&["RPUSH", &format!("list:{w}"), &n.to_string()]);
                    c.cmds(&["APPEND", &format!("log:{w}"), "x"]);
                    n += 1;
                }
            })
        })
        .collect();

    // Attach several replicas one after another, all mid-load.
    let mut replicas = Vec::new();
    for _ in 0..3 {
        thread::sleep(Duration::from_millis(150));
        let port = spawn_server(replica_of(master_port));
        let mut c = Conn::connect(port);
        wait_link_up(&mut c);
        replicas.push(c);
    }

    thread::sleep(Duration::from_millis(300));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }

    let mut master = Conn::connect(master_port);
    assert_eq!(
        master.cmds(&["WAIT", "3", "5000"]).as_int(),
        3,
        "every replica must acknowledge everything it was sent"
    );

    for (i, replica) in replicas.iter_mut().enumerate() {
        for k in 0..8 {
            let key = format!("counter:{k}");
            assert_eq!(
                replica.cmds(&["GET", &key]).as_str(),
                master.cmds(&["GET", &key]).as_str(),
                "replica {i}: {key} diverged"
            );
        }
        for w in 0..3 {
            let list = format!("list:{w}");
            assert_eq!(
                replica.cmds(&["LLEN", &list]).as_int(),
                master.cmds(&["LLEN", &list]).as_int(),
                "replica {i}: {list} length diverged"
            );
            let log = format!("log:{w}");
            assert_eq!(
                replica.cmds(&["STRLEN", &log]).as_int(),
                master.cmds(&["STRLEN", &log]).as_int(),
                "replica {i}: {log} length diverged"
            );
        }
        assert_eq!(
            replica.cmds(&["DBSIZE"]).as_int(),
            master.cmds(&["DBSIZE"]).as_int(),
            "replica {i}: key count diverged"
        );
    }
}

/// A master with `requirepass` refuses an unauthenticated handshake. The
/// replica needs `masterauth`, and until now there was no such setting — so a
/// password-protected master could not be replicated from at all.
#[test]
fn test_replica_authenticates_with_masterauth() {
    let master_port = spawn_server(Config {
        requirepass: "s3cret".to_string(),
        ..Default::default()
    });
    let mut master = Conn::connect(master_port);
    assert!(master.cmds(&["AUTH", "s3cret"]).is_ok());
    assert!(master.cmds(&["SET", "k", "v"]).is_ok());

    // Without the password: the link stays down.
    let no_auth = spawn_server(replica_of(master_port));
    let mut c = Conn::connect(no_auth);
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        repl_info(&mut c, "master_link_status").as_deref(),
        Some("down")
    );
    assert!(matches!(c.cmds(&["GET", "k"]), RespValue::Nil));

    // Supplying it at runtime is enough; the client retries every second.
    assert!(c.cmds(&["CONFIG", "SET", "masterauth", "s3cret"]).is_ok());
    wait_link_up(&mut c);
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "v");

    // And from the start, via configuration.
    let with_auth = spawn_server(Config {
        masterauth: "s3cret".to_string(),
        ..replica_of(master_port)
    });
    let mut c = Conn::connect(with_auth);
    wait_link_up(&mut c);
    assert_eq!(c.cmds(&["GET", "k"]).as_str(), "v");
    // The credential is never echoed back.
    let got = c.cmds(&["CONFIG", "GET", "masterauth"]);
    assert_eq!(got.as_array()[1].as_str(), "**redacted**");
}

/// Derived state has to be rebuilt after a full resync: the time-series label
/// index and search-index definitions are not values, so a replica that
/// received values only could not answer label or index queries — and still
/// could not after being promoted.
#[test]
fn test_full_resync_rebuilds_label_and_search_indexes() {
    let master_port = spawn_server(Config::default());
    let mut master = Conn::connect(master_port);
    assert!(
        master
            .cmds(&["TS.CREATE", "temp:1", "LABELS", "room", "kitchen"])
            .is_ok()
    );
    master.cmds(&["TS.ADD", "temp:1", "1000", "21.5"]);
    assert!(
        master
            .cmds(&[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
            ])
            .is_ok()
    );
    master.cmds(&["HSET", "doc:1", "title", "hello world"]);

    let replica_port = spawn_server(replica_of(master_port));
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);

    let by_label = replica.cmds(&["TS.QUERYINDEX", "room=kitchen"]);
    assert_eq!(
        by_label.as_array().len(),
        1,
        "label index missing on replica"
    );

    let found = replica.cmds(&["FT.SEARCH", "idx", "hello"]);
    assert_eq!(
        found.as_array()[0].as_int(),
        1,
        "search index missing on replica"
    );
}

fn error_of(reply: &RespValue) -> &str {
    match reply {
        RespValue::Error(e) => e,
        other => panic!("not an error: {other:?}"),
    }
}

fn start_replica_of(replica_port: u16, master_port: u16) {
    thread::spawn(move || {
        let config = Config {
            port: replica_port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            loglevel: "error".to_string(),
            io_model: IoModel::Tokio,
            replicaof: Some(vec!["127.0.0.1".to_string(), master_port.to_string()]),
            replica_announced_port: replica_port,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
}

/// `min-replicas-to-write`: a master with fewer good replicas than that
/// refuses every write, and a transaction with a write in it whole, with
/// Redis's NOREPLICAS. Reads go on. A replica that is online and
/// acknowledging is good. `min-replicas-max-lag 0` turns it off. It used to
/// gate only the writes a script made: a master cut off from its replicas
/// went on acknowledging ordinary writes, and lost them.
#[test]
fn a_master_without_enough_good_replicas_refuses_writes() {
    let master_port = free_port();
    start_server(master_port);
    let mut m = Conn::connect(master_port);
    assert!(m.cmds(&["SET", "a", "1"]).is_ok(), "off unless set");
    assert_eq!(
        m.cmds(&["CONFIG", "GET", "min-replicas-max-lag"])
            .as_array()[1]
            .as_str(),
        "10"
    );

    assert!(
        m.cmds(&["CONFIG", "SET", "min-replicas-to-write", "1"])
            .is_ok()
    );
    let refused = m.cmds(&["SET", "a", "2"]);
    assert!(error_of(&refused).starts_with("NOREPLICAS"), "{refused:?}");
    assert!(error_of(&m.cmds(&["INCR", "n"])).starts_with("NOREPLICAS"));
    assert_eq!(m.cmds(&["GET", "a"]).as_str(), "1", "reads go on");

    // A transaction with a write is refused whole, and nothing of it runs.
    assert!(m.cmds(&["MULTI"]).is_ok());
    assert_eq!(m.cmds(&["GET", "a"]).as_str(), "QUEUED");
    assert_eq!(m.cmds(&["SET", "a", "3"]).as_str(), "QUEUED");
    let exec = m.cmds(&["EXEC"]);
    let why = error_of(&exec);
    assert!(
        why.starts_with("EXECABORT") && why.contains("NOREPLICAS"),
        "{why}"
    );
    assert_eq!(m.cmds(&["GET", "a"]).as_str(), "1");
    // One that only reads runs.
    assert!(m.cmds(&["MULTI"]).is_ok());
    assert_eq!(m.cmds(&["GET", "a"]).as_str(), "QUEUED");
    assert_eq!(m.cmds(&["EXEC"]).as_array().len(), 1);

    // A replica comes, catches up and acknowledges: it is good.
    let replica_port = free_port();
    start_replica_of(replica_port, master_port);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if m.cmds(&["SET", "a", "4"]).is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writes were not accepted with a good replica"
        );
        thread::sleep(Duration::from_millis(100));
    }

    let info = m.cmds(&["INFO", "replication"]);
    assert!(
        info.as_str().contains("min_slaves_good_slaves:1"),
        "{info:?}"
    );

    // Two are asked for, one is there.
    assert!(
        m.cmds(&["CONFIG", "SET", "min-replicas-to-write", "2"])
            .is_ok()
    );
    assert!(error_of(&m.cmds(&["SET", "a", "5"])).starts_with("NOREPLICAS"));
    // The Redis names of old are the same settings.
    assert!(
        m.cmds(&["CONFIG", "SET", "min-slaves-max-lag", "0"])
            .is_ok()
    );
    assert_eq!(
        m.cmds(&["CONFIG", "GET", "min-replicas-max-lag"])
            .as_array()[1]
            .as_str(),
        "0"
    );
    assert!(
        m.cmds(&["SET", "a", "6"]).is_ok(),
        "a lag of 0 turns it off"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// PROMOTION DURING A FULL SYNC
// ═════════════════════════════════════════════════════════════════════════════

/// Read one CRLF-terminated line from `r`, without the CRLF.
fn read_line_raw(r: &mut impl BufRead) -> String {
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    line.trim_end().to_string()
}

/// A real full-sync payload: what `master_port` sends a new replica.
fn full_sync_payload_of(master_port: u16) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", master_port)).unwrap();
    stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let mut reader = BufReader::new(stream);
    let greeting = read_line_raw(&mut reader);
    assert!(greeting.starts_with("+FULLRESYNC"), "{greeting}");
    let header = read_line_raw(&mut reader);
    let len: usize = header.trim_start_matches('$').parse().unwrap();
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).unwrap();
    payload
}

/// A stand-in master that answers the handshake and then sends `payload` a
/// little at a time, never pausing long enough for the replica's idle-link
/// timer to fire — a large payload on a busy link, made slow enough to promote
/// the replica in the middle of it. `started` gets a message once the payload
/// has begun. Returns its port.
fn spawn_master_trickling(payload: Vec<u8>, started: std::sync::mpsc::Sender<()>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        let Some(Ok(mut stream)) = listener.incoming().next() else {
            return;
        };
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        loop {
            let line = read_line_raw(&mut reader);
            let argc: usize = line.trim_start_matches('*').parse().unwrap_or(0);
            if argc == 0 {
                return;
            }
            let mut args = Vec::new();
            for _ in 0..argc {
                let len: usize = read_line_raw(&mut reader)
                    .trim_start_matches('$')
                    .parse()
                    .unwrap();
                let mut data = vec![0u8; len + 2];
                reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                args.push(String::from_utf8_lossy(&data).to_ascii_uppercase());
            }
            match args[0].as_str() {
                "PING" => stream.write_all(b"+PONG\r\n").unwrap(),
                "REPLCONF" => stream.write_all(b"+OK\r\n").unwrap(),
                "PSYNC" => break,
                _ => return,
            }
        }
        stream
            .write_all(b"+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n")
            .unwrap();
        stream
            .write_all(format!("${}\r\n", payload.len()).as_bytes())
            .unwrap();
        let _ = started.send(());
        for chunk in payload.chunks(64) {
            if stream.write_all(chunk).is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(15));
        }
        let _ = stream.write_all(b"*1\r\n$4\r\nPING\r\n");
        let mut sink = [0u8; 256];
        while reader.read(&mut sink).unwrap_or(0) > 0 {}
    });
    port
}

/// `REPLICAOF NO ONE` while a full-sync payload is still arriving. The node is
/// a master from then on, with its own dataset: the session it superseded must
/// stop without touching it. It used to read the payload to the end, drop the
/// promoted node's keys and load the old master's — found in a staging
/// failover, where the promoted node also lost every write since its sync.
#[test]
fn test_promotion_during_a_full_sync_keeps_the_promoted_nodes_dataset() {
    let source = spawn_server(Config::default());
    let mut s = Conn::connect(source);
    for i in 0..300 {
        assert!(s.cmds(&["SET", &format!("from-master:{i}"), "x"]).is_ok());
    }
    let payload = full_sync_payload_of(source);
    assert!(
        payload.len() > 64 * 20,
        "the payload must take a while to trickle: {} bytes",
        payload.len()
    );

    let node = spawn_server(Config::default());
    let mut c = Conn::connect(node);
    for i in 0..50 {
        assert!(c.cmds(&["SET", &format!("mine:{i}"), "v"]).is_ok());
    }

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let old_master = spawn_master_trickling(payload, started_tx);
    assert!(
        c.cmds(&["REPLICAOF", "127.0.0.1", &old_master.to_string()])
            .is_ok()
    );
    started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the full sync never started");
    thread::sleep(Duration::from_millis(100));
    assert!(c.cmds(&["REPLICAOF", "NO", "ONE"]).is_ok());

    // Long enough for the rest of the payload to have arrived, been loaded and
    // swapped in, had the old session not stopped.
    thread::sleep(Duration::from_secs(4));
    assert_eq!(repl_info(&mut c, "role").as_deref(), Some("master"));
    assert_eq!(
        c.cmds(&["DBSIZE"]).as_int(),
        50,
        "the promoted node's dataset was replaced"
    );
    assert_eq!(c.cmds(&["GET", "mine:0"]).as_str(), "v");
    assert!(c.cmds(&["SET", "after-promotion", "1"]).is_ok());
}

/// `REPLICAOF NO ONE` on one of a master's replicas, then its sibling
/// pointed at it: the sibling is continued from where it was, not resynced
/// in full (#109). The promoted replica keeps the stream it followed as its
/// second id, and kept, as a replica, what it read of it.
#[test]
fn test_a_promoted_replica_continues_its_sibling() {
    let (m, a, b) = (free_port(), free_port(), free_port());
    start_server(m);
    start_server(a);
    start_server(b);
    let field = |c: &mut Conn, section: &str, name: &str| -> String {
        let info = c.cmds(&["INFO", section]).as_str().to_string();
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")))
            .unwrap_or("")
            .trim()
            .to_string()
    };
    let wait = |what: &str, mut ok: Box<dyn FnMut() -> bool>| {
        for _ in 0..200 {
            if ok() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("timed out waiting for {what}");
    };

    let mut mc = Conn::connect(m);
    for i in 0..100 {
        assert!(mc.cmds(&["SET", &format!("k:{i}"), "v"]).is_ok());
    }
    for port in [a, b] {
        let mut c = Conn::connect(port);
        assert!(c.cmds(&["REPLICAOF", "127.0.0.1", &m.to_string()]).is_ok());
    }
    let master_offset = field(&mut mc, "replication", "master_repl_offset");
    let old_id = field(&mut mc, "replication", "master_replid");
    for port in [a, b] {
        let want = master_offset.clone();
        wait(
            "a replica to catch up",
            Box::new(move || {
                let mut c = Conn::connect(port);
                field(&mut c, "replication", "master_link_status") == "up"
                    && field(&mut c, "replication", "slave_repl_offset") == want
            }),
        );
    }

    let mut ac = Conn::connect(a);
    assert!(ac.cmds(&["REPLICAOF", "NO", "ONE"]).is_ok());
    assert_eq!(field(&mut ac, "replication", "master_replid2"), old_id);
    assert_eq!(
        field(&mut ac, "replication", "master_repl_offset"),
        master_offset
    );
    // A write on the new master before its sibling arrives.
    assert!(ac.cmds(&["SET", "k:0", "after"]).is_ok());

    let mut bc = Conn::connect(b);
    assert!(bc.cmds(&["REPLICAOF", "127.0.0.1", &a.to_string()]).is_ok());
    wait(
        "the sibling to follow the promoted replica",
        Box::new(move || {
            let mut c = Conn::connect(b);
            field(&mut c, "replication", "master_link_status") == "up"
                && field(&mut c, "replication", "master_port") == a.to_string()
        }),
    );
    assert_eq!(field(&mut ac, "stats", "sync_full"), "0");
    assert_eq!(field(&mut ac, "stats", "sync_partial_ok"), "1");
    wait(
        "the write to reach the sibling",
        Box::new(move || {
            let mut c = Conn::connect(b);
            c.cmds(&["GET", "k:0"]).as_str() == "after"
        }),
    );
    assert_eq!(bc.cmds(&["GET", "k:99"]).as_str(), "v");
}
