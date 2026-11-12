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
use std::time::{Duration, Instant};

const REDIS_HLL_SPARSE: &[u8] = include_bytes!("fixtures/redis-8.0-hll-small-sparse.bin");

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

    /// The connection, with `timeout` to wait for each reply.
    #[cfg(feature = "lua")]
    fn with_read_timeout(self, timeout: Duration) -> Self {
        self.stream.set_read_timeout(Some(timeout)).unwrap();
        self
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
    fn as_bulk(&self) -> &[u8] {
        match self {
            RespValue::Bulk(bytes) => bytes,
            other => panic!("not a bulk string: {other:?}"),
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

struct ShutdownServers(Vec<u16>);

impl Drop for ShutdownServers {
    fn drop(&mut self) {
        for port in &self.0 {
            if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", *port)) {
                let _ = stream.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$5\r\nNOSAVE\r\n");
            }
        }
    }
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

#[test]
fn redis_hll_replication_keeps_bytes_and_digest_in_step_including_pfdebug() {
    let master_port = free_port();
    let replica_port = free_port();
    start_server(master_port);
    start_server(replica_port);
    let _cleanup = ShutdownServers(vec![master_port, replica_port]);
    let mut master = Conn::connect(master_port);
    let mut replica = Conn::connect(replica_port);

    for conn in [&mut master, &mut replica] {
        assert!(
            conn.cmds(&["CONFIG", "SET", "hll-write-format", "redis"])
                .is_ok()
        );
        assert_eq!(
            conn.cmds(&["CONFIG", "GET", "hll-write-format"]).as_array()[1].as_str(),
            "redis"
        );
    }
    assert!(
        replica
            .cmds(&["REPLICAOF", "127.0.0.1", &master_port.to_string()])
            .is_ok()
    );

    assert_eq!(master.cmds(&["PFADD", "source", "one", "two"]).as_int(), 1);
    assert!(master.cmds(&["PFMERGE", "merged", "source"]).is_ok());

    let wait_for_equal = |master: &mut Conn, replica: &mut Conn, keys: &[&str]| {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let equal = keys.iter().all(|key| {
                let master_bytes = master.cmds(&["GET", key]);
                let replica_bytes = replica.cmds(&["GET", key]);
                if !matches!(&master_bytes, RespValue::Bulk(_)) {
                    return false;
                }
                if master_bytes != replica_bytes {
                    return false;
                }
                let master_digest = master.cmds(&["DIGEST", key]);
                let replica_digest = replica.cmds(&["DIGEST", key]);
                master_digest == replica_digest
            });
            if equal {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "replica did not converge for {keys:?}"
            );
            thread::sleep(Duration::from_millis(25));
        }
    };

    wait_for_equal(&mut master, &mut replica, &["source", "merged"]);
    for key in ["source", "merged"] {
        assert_eq!(master.cmds(&["PFCOUNT", key]).as_int(), 2);
        assert_eq!(replica.cmds(&["PFCOUNT", key]).as_int(), 2);
        assert_eq!(master.cmds(&["GET", key]), replica.cmds(&["GET", key]));
        assert_eq!(
            master.cmds(&["DIGEST", key]),
            replica.cmds(&["DIGEST", key])
        );
        assert_eq!(
            master.cmds(&["GET", key]).as_bulk()[4],
            0,
            "Redis dense encoding"
        );
    }

    assert!(master.cmd(&[b"SET", b"sparse", REDIS_HLL_SPARSE]).is_ok());
    wait_for_equal(&mut master, &mut replica, &["sparse"]);
    let sparse_before = master.cmds(&["GET", "sparse"]).as_bulk().to_vec();
    let master_registers = master.cmds(&["PFDEBUG", "GETREG", "sparse"]);
    let replica_registers = replica.cmds(&["PFDEBUG", "GETREG", "sparse"]);
    assert_eq!(master_registers.as_array().len(), 16384);
    assert_eq!(replica_registers.as_array().len(), 16384);
    assert_eq!(master_registers, replica_registers);
    assert_eq!(
        master.cmds(&["GET", "sparse"]).as_bulk(),
        sparse_before.as_slice()
    );
    assert_eq!(
        replica.cmds(&["GET", "sparse"]).as_bulk(),
        sparse_before.as_slice(),
        "GETREG must not rewrite the sparse value on a replica"
    );

    assert_eq!(master.cmds(&["PFDEBUG", "TODENSE", "sparse"]).as_int(), 1);
    wait_for_equal(&mut master, &mut replica, &["sparse"]);
    let master_sparse = master.cmds(&["GET", "sparse"]);
    let replica_sparse = replica.cmds(&["GET", "sparse"]);
    assert_eq!(master_sparse.as_bulk(), replica_sparse.as_bulk());
    assert_eq!(
        master_sparse.as_bulk()[4],
        0,
        "TODENSE writes Redis dense encoding"
    );
    assert_eq!(master.cmds(&["PFCOUNT", "sparse"]).as_int(), 3);
    assert_eq!(replica.cmds(&["PFCOUNT", "sparse"]).as_int(), 3);
    assert_eq!(
        master.cmds(&["DIGEST", "sparse"]),
        replica.cmds(&["DIGEST", "sparse"])
    );

    assert!(master.cmds(&["PFDEBUG", "RESET", "sparse"]).is_ok());
    wait_for_equal(&mut master, &mut replica, &["sparse"]);
    assert_eq!(master.cmds(&["PFCOUNT", "sparse"]).as_int(), 0);
    assert_eq!(replica.cmds(&["PFCOUNT", "sparse"]).as_int(), 0);
    assert_eq!(
        master.cmds(&["DIGEST", "sparse"]),
        replica.cmds(&["DIGEST", "sparse"])
    );
}

// WAIT returns 0 when no replicas are connected
#[test]
fn test_wait_no_replicas() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);

    // WAIT 1 100: no replica acknowledges; after the timeout, as in Redis,
    // the answer is 0. (`WAIT 1 0` waits for ever.)
    let r = c.cmds(&["WAIT", "1", "100"]);
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
/// The servers these tests run write their files (`dump.kdb`,
/// `dump.kdb.gen`, `dump.kdb.repl`) where the process runs, which was the
/// repository's own directory. The process moves to a directory of its own
/// before the first server starts (every test asks for a port first).
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

/// A replica answers a `SAVE` while it applies writes of keys of two shards
/// from its master. Such a write takes the command gate alone. The applier
/// took it holding its own lock, which a `SAVE` waits for inside the gate:
/// neither went on, the replica answered nothing more and applied nothing
/// more, and its master dropped it (#373). Each `SAVE` here has the
/// connection's 5 s to answer.
#[test]
fn a_replica_saves_while_it_applies_writes_of_two_shards() {
    saves_beside_writes_of_two_shards(IoModel::Tokio);
}

/// [`a_replica_saves_while_it_applies_writes_of_two_shards`], the replica
/// on io_uring, which takes the gate on a loop of its own.
#[cfg(target_os = "linux")]
#[test]
fn a_replica_on_io_uring_saves_while_it_applies_writes_of_two_shards() {
    saves_beside_writes_of_two_shards(IoModel::Uring);
}

fn saves_beside_writes_of_two_shards(io_model: IoModel) {
    let master = spawn_server(Config::default());
    let dir = tempfile::tempdir().unwrap();
    let replica = spawn_server_with(
        Config {
            dir: dir.path().to_string_lossy().into_owned(),
            ..replica_of(master)
        },
        2,
        io_model,
    );
    let mut r = Conn::connect(replica);
    wait_link_up(&mut r);
    let mut m = Conn::connect(master);
    assert!(m.cmds(&["SET", "a", "1"]).is_ok());
    let until = Instant::now() + Duration::from_secs(3);
    let writer = thread::spawn(move || {
        let mut m = Conn::connect(master);
        let mut copies = 0u64;
        while Instant::now() < until {
            let to = format!("b{}", copies % 64);
            assert_eq!(m.cmds(&["COPY", "a", &to, "REPLACE"]), RespValue::Int(1));
            copies += 1;
        }
        copies
    });
    let mut saves = 0u64;
    while Instant::now() < until {
        let reply = r.cmds(&["SAVE"]);
        assert!(reply.is_ok(), "{reply:?}");
        saves += 1;
    }
    let copies = writer.join().unwrap();
    assert!(copies > 0 && saves > 0, "{copies} copies, {saves} saves");
    // And the replica applied every one of them.
    let at: u64 = repl_info(&mut m, "master_repl_offset")
        .unwrap()
        .parse()
        .unwrap();
    wait_until(
        "the replica to apply them all",
        Duration::from_secs(10),
        || repl_info(&mut r, "slave_repl_offset").is_some_and(|o| o.parse::<u64>().unwrap() >= at),
    );
}

/// A replica answers a `SAVE` while it applies its master's writes and runs a
/// client's read-only scripts. A script takes the command gate alone, slot by
/// slot, in the order the threads got them. It took the applier's slot, then
/// waited for that of a `SAVE`, which was waiting for the applier's lock;
/// and the applier, holding that lock, waited for its slot behind the script.
/// None of the three went on (#373). The threads that run the `SAVE`s and
/// the scripts here get their slots after the applier has its own: the
/// replica has more of them than have run a command yet.
#[cfg(feature = "lua")]
#[test]
fn a_replica_saves_while_it_applies_writes_and_runs_scripts() {
    let master = spawn_server(Config::default());
    let dir = tempfile::tempdir().unwrap();
    let replica = spawn_server_with(
        Config {
            dir: dir.path().to_string_lossy().into_owned(),
            ..replica_of(master)
        },
        8,
        IoModel::Tokio,
    );
    // Nothing is asked of the replica until its applier has its slot: the
    // link is seen from the master.
    let mut m = Conn::connect(master);
    wait_until("the replica online", Duration::from_secs(10), || {
        repl_info(&mut m, "slave0").is_some_and(|s| s.contains("state=online"))
    });
    let until = Instant::now() + Duration::from_secs(4);
    let writer = thread::spawn(move || {
        let mut m = Conn::connect(master);
        let mut sets = 0u64;
        while Instant::now() < until {
            let key = format!("k{}", sets % 64);
            assert!(m.cmds(&["SET", &key, "v"]).is_ok());
            sets += 1;
        }
        sets
    });
    // The applier at work, with its slot, before the others start.
    wait_until(
        "the replica to apply a write",
        Duration::from_secs(10),
        || repl_info(&mut m, "slave0").is_some_and(|s| !s.contains("offset=0,")),
    );
    thread::sleep(Duration::from_millis(200));
    let mut r = Conn::connect(replica);
    let clients: Vec<_> = (0..8)
        .map(|i| {
            thread::spawn(move || {
                // A `SAVE` has 30 s: a slow disk is not a deadlock.
                let mut c = Conn::connect(replica).with_read_timeout(Duration::from_secs(30));
                let mut done = 0u64;
                while Instant::now() < until {
                    if i % 2 == 0 {
                        let reply = c.cmds(&["SAVE"]);
                        assert!(reply.is_ok(), "{reply:?}");
                    } else {
                        assert_eq!(c.cmds(&["EVAL_RO", "return 1", "0"]), RespValue::Int(1));
                    }
                    done += 1;
                }
                done
            })
        })
        .collect();
    let sets = writer.join().unwrap();
    let done: Vec<u64> = clients.into_iter().map(|c| c.join().unwrap()).collect();
    assert!(sets > 0 && done.iter().all(|&n| n > 0), "{sets} {done:?}");
    let at: u64 = repl_info(&mut m, "master_repl_offset")
        .unwrap()
        .parse()
        .unwrap();
    wait_until(
        "the replica to apply them all",
        Duration::from_secs(10),
        || repl_info(&mut r, "slave_repl_offset").is_some_and(|o| o.parse::<u64>().unwrap() >= at),
    );
}

/// [`spawn_server`], with `threads` worker threads and `io_model`.
fn spawn_server_with(mut config: Config, threads: usize, io_model: IoModel) -> u16 {
    let port = free_port();
    config.port = port;
    config.bind = "127.0.0.1".to_string();
    config.threads = threads;
    config.loglevel = "error".to_string();
    config.io_model = io_model;
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

// `test_scripts_that_read_run_on_a_replica` is in
// tests/replication_functions.rs: the function libraries are process-global,
// and the full syncs of the other tests here took its library away.

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
                        Some("INFO") => stream.write_all(b"-ERR unknown command\r\n").unwrap(),
                        Some("PSYNC") => {
                            let payload = kividb::snapshot::full_sync_payload(
                                &kividb::store::new_store(16),
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

/// A blocking pop is sent to replicas and the AOF as the pop it made, as in
/// Redis: `BLPOP` as `LPOP` of the key it took from, `BLMPOP` as `LPOP key
/// count`, `BZPOPMIN` as `ZPOPMIN`, from a client that did not wait, one
/// that did, one in a transaction and one in a script; and as nothing when
/// it took nothing (#558). A `BLPOP k1 k2` that leaves `k1` to a client that
/// has waited longer, and takes from `k2`, is sent as `LPOP k2`: sent as it
/// was, the replica ran it on `k1`, and kept `k2`'s element for good.
#[test]
fn test_blocking_pops_are_sent_as_the_pop_they_made() {
    let master_port = spawn_server(Config::default());
    let mut master = Conn::connect(master_port);
    let replica_port = spawn_server(replica_of(master_port));
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);
    let mut tap = attach_stream_reader(master_port);
    let request = |args: &[&str]| {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            out.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        out
    };
    let blocked = |master: &mut Conn| {
        wait_until("the client is blocked", Duration::from_secs(10), || {
            master
                .cmds(&["INFO", "clients"])
                .as_str()
                .contains("blocked_clients:1")
        })
    };

    assert_eq!(
        master.cmds(&["RPUSH", "l1", "a", "b", "c", "d"]).as_int(),
        4
    );
    assert_eq!(
        master
            .cmds(&["ZADD", "z", "1", "m1", "2", "m2", "3", "m3", "4", "m4"])
            .as_int(),
        4
    );
    master.cmds(&["BLPOP", "none", "l1", "0"]);
    master.cmds(&["BRPOP", "l1", "0"]);
    master.cmds(&["BLMPOP", "0", "2", "none", "l1", "LEFT", "COUNT", "5"]);
    master.cmds(&["BZPOPMIN", "z", "0"]);
    master.cmds(&["BZPOPMAX", "z", "0"]);
    master.cmds(&["BZMPOP", "0", "1", "z", "MAX", "COUNT", "9"]);
    assert_eq!(master.cmds(&["BLPOP", "none", "0.01"]), RespValue::Nil);

    assert_eq!(master.cmds(&["RPUSH", "l2", "x"]).as_int(), 1);
    // A transaction of more than one write is sent wrapped in MULTI/EXEC; its
    // pop that took nothing is not sent.
    assert!(master.cmds(&["MULTI"]).is_ok());
    master.cmds(&["BLPOP", "l2", "0"]);
    master.cmds(&["BLPOP", "l2", "0"]);
    master.cmds(&["RPUSH", "l2", "y"]);
    master.cmds(&["BLPOP", "l2", "0"]);
    assert_eq!(master.cmds(&["EXEC"]).as_array().len(), 4);

    // One that waits, and is given an element.
    let mut waiter = Conn::connect(master_port);
    waiter
        .stream
        .write_all(&request(&["BLPOP", "lw", "0"]))
        .unwrap();
    blocked(&mut master);
    assert_eq!(master.cmds(&["RPUSH", "lw", "v"]).as_int(), 1);
    assert_eq!(waiter.read_value().as_array().len(), 2);

    // `k1`'s element belongs to the client blocked on it: a `BLPOP k1 k2`
    // sent with the pushes takes `k2`'s, whether it runs before that client
    // is woken or after.
    let mut first = Conn::connect(master_port);
    first
        .stream
        .write_all(&request(&["BLPOP", "k1", "0"]))
        .unwrap();
    blocked(&mut master);
    let mut later = Conn::connect(master_port);
    let mut batch = request(&["RPUSH", "k1", "a"]);
    batch.extend(request(&["RPUSH", "k2", "b"]));
    batch.extend(request(&["BLPOP", "k1", "k2", "0"]));
    later.stream.write_all(&batch).unwrap();
    assert_eq!(later.read_value().as_int(), 1);
    assert_eq!(later.read_value().as_int(), 1);
    let took = |reply: RespValue| {
        reply
            .as_array()
            .iter()
            .map(|part| part.as_str().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(took(later.read_value()), ["k2", "b"]);
    assert_eq!(took(first.read_value()), ["k1", "a"]);

    #[cfg(feature = "lua")]
    {
        // A script's pops are sent as the pops they made, in MULTI/EXEC, and
        // the one that took nothing is not.
        assert_eq!(master.cmds(&["RPUSH", "l3", "s", "t"]).as_int(), 2);
        let reply = master.cmds(&[
            "EVAL",
            "local first = redis.call('BLPOP', KEYS[1], 0) \
             redis.call('BRPOP', KEYS[1], 0) \
             redis.call('BLPOP', KEYS[1], 0) \
             return first",
            "1",
            "l3",
        ]);
        assert_eq!(took(reply), ["l3", "s"]);
    }
    assert!(master.cmds(&["SET", "end", "1"]).is_ok());

    let mut seen = stream_until(&mut tap, |command| command.starts_with("SET end"));
    // The pushes to `k1` and `k2` and the two pops are made by two clients at
    // once. `first`, woken by the push to `k1`, may pop it before the push to
    // `k2` is applied, or after; and either pop may be sent first. What must
    // hold is that the two pushes are in the order they were sent, and each
    // pop comes after the push it took from. Those four are then put in one
    // order, to compare the rest.
    let racing = ["RPUSH k1 a", "RPUSH k2 b", "LPOP k1", "LPOP k2"];
    let from = seen
        .iter()
        .position(|command| command == "LPOP lw")
        .map(|at| at + 1)
        .filter(|from| from + racing.len() <= seen.len())
        .unwrap_or_else(|| panic!("no pushes and pops of k1 and k2: {seen:?}"));
    let window = &seen[from..from + racing.len()];
    let at = |command: &str| {
        window
            .iter()
            .position(|sent| sent == command)
            .unwrap_or_else(|| panic!("{command} not sent with the others: {seen:?}"))
    };
    assert!(at("RPUSH k1 a") < at("RPUSH k2 b"), "{seen:?}");
    assert!(at("RPUSH k1 a") < at("LPOP k1"), "{seen:?}");
    assert!(at("RPUSH k2 b") < at("LPOP k2"), "{seen:?}");
    for (sent, command) in seen[from..from + racing.len()].iter_mut().zip(racing) {
        *sent = command.to_string();
    }
    let mut expected = vec![
        "SELECT 0",
        "RPUSH l1 a b c d",
        "ZADD z 1 m1 2 m2 3 m3 4 m4",
        "LPOP l1",
        "RPOP l1",
        "LPOP l1 2",
        "ZPOPMIN z",
        "ZPOPMAX z",
        "ZPOPMAX z 2",
        "RPUSH l2 x",
        "MULTI",
        "LPOP l2",
        "RPUSH l2 y",
        "LPOP l2",
        "EXEC",
        "RPUSH lw v",
        "LPOP lw",
        "RPUSH k1 a",
        "RPUSH k2 b",
        "LPOP k1",
        "LPOP k2",
    ];
    if cfg!(feature = "lua") {
        expected.extend(["RPUSH l3 s t", "MULTI", "LPOP l3", "RPOP l3", "EXEC"]);
    }
    expected.push("SET end 1");
    assert_eq!(seen, expected);

    // The replica holds what the master does, whatever order the stream
    // had: every list and set popped empty, and the one key written last.
    assert_eq!(master.cmds(&["WAIT", "1", "5000"]).as_int(), 1);
    for key in ["l1", "l2", "l3", "lw", "k1", "k2", "z"] {
        assert_eq!(master.cmds(&["EXISTS", key]).as_int(), 0, "{key}");
        assert_eq!(replica.cmds(&["EXISTS", key]).as_int(), 0, "{key}");
    }
    assert_eq!(master.cmds(&["DBSIZE"]).as_int(), 1);
    assert_eq!(replica.cmds(&["DBSIZE"]).as_int(), 1);
    assert_eq!(replica.cmds(&["GET", "end"]).as_str(), "1");
}

/// Send `args` and read the one reply, whatever its RESP3 type. Returns the
/// reply's first line.
fn resp3_call(c: &mut Conn, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
    }
    c.stream.write_all(&out).unwrap();
    read_resp3(&mut c.reader)
}

fn read_resp3(r: &mut impl BufRead) -> String {
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    let head = line.trim_end().to_string();
    let n = head
        .get(1..)
        .and_then(|n| n.parse::<i64>().ok())
        .unwrap_or(0);
    match head.as_bytes().first() {
        Some(b'$' | b'=' | b'!') if n >= 0 => {
            let mut body = vec![0u8; n as usize + 2];
            r.read_exact(&mut body).unwrap();
        }
        Some(b'*' | b'~' | b'>') if n > 0 => {
            for _ in 0..n {
                read_resp3(r);
            }
        }
        Some(b'%' | b'|') if n > 0 => {
            for _ in 0..2 * n {
                read_resp3(r);
            }
        }
        _ => {}
    }
    head
}

/// A RESP3 client's writes reach replicas and the AOF as their effects, as
/// a RESP2 client's do. `SPOP` with a count answers it with a set (`~`),
/// which was not read: nothing was sent, and the replicas kept the members
/// the master had popped. So from a script after `redis.setresp(3)`.
#[test]
fn test_a_resp3_client_s_writes_are_sent_as_their_effects() {
    let master_port = spawn_server(Config::default());
    let replica_port = spawn_server(replica_of(master_port));
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);
    let mut tap = attach_stream_reader(master_port);
    let mut c = Conn::connect(master_port);
    assert!(resp3_call(&mut c, &["HELLO", "3"]).starts_with('%'));

    resp3_call(&mut c, &["SADD", "s", "a", "b", "c", "d", "e"]);
    assert_eq!(resp3_call(&mut c, &["SPOP", "s", "2"]), "~2");
    assert_eq!(resp3_call(&mut c, &["SPOP", "s"]), "$1");
    assert_eq!(
        resp3_call(&mut c, &["SPOP", "none", "2"]).as_bytes()[0],
        b'~'
    );
    resp3_call(&mut c, &["RPUSH", "l", "a", "b"]);
    resp3_call(&mut c, &["BLPOP", "l", "0"]);
    resp3_call(&mut c, &["BLMOVE", "l", "l2", "LEFT", "RIGHT", "0"]);
    resp3_call(&mut c, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
    resp3_call(&mut c, &["BZPOPMIN", "z", "0"]);
    resp3_call(&mut c, &["BZMPOP", "0", "1", "z", "MAX", "COUNT", "5"]);
    resp3_call(&mut c, &["SET", "f", "1"]);
    resp3_call(&mut c, &["INCRBYFLOAT", "f", "1.5"]);
    #[cfg(feature = "lua")]
    {
        resp3_call(&mut c, &["SADD", "s2", "x", "y", "z"]);
        let reply = resp3_call(
            &mut c,
            &[
                "EVAL",
                "redis.setresp(3) return redis.call('SPOP', KEYS[1], 2)",
                "1",
                "s2",
            ],
        );
        assert!(!reply.starts_with('-'), "{reply}");
    }
    assert!(resp3_call(&mut c, &["SET", "end", "1"]).starts_with('+'));

    let seen = stream_until(&mut tap, |command| command.starts_with("SET end"));
    for sent in &seen {
        let name = sent.split(' ').next().unwrap_or("");
        assert!(
            !matches!(
                name,
                "SPOP" | "BLPOP" | "BLMOVE" | "BZPOPMIN" | "BZMPOP" | "INCRBYFLOAT"
            ),
            "{sent} was sent as it was: {seen:?}"
        );
    }
    let has = |prefix: &str| seen.iter().filter(|sent| sent.starts_with(prefix)).count();
    assert_eq!(has("SREM s "), 2, "{seen:?}");
    assert_eq!(has("LPOP l"), 1, "{seen:?}");
    assert_eq!(has("LMOVE l l2 LEFT RIGHT"), 1, "{seen:?}");
    assert_eq!(has("ZPOPMIN z"), 1, "{seen:?}");
    assert_eq!(has("ZPOPMAX z 2"), 1, "{seen:?}");
    assert_eq!(has("SET f 2.5 KEEPTTL"), 1, "{seen:?}");
    if cfg!(feature = "lua") {
        assert_eq!(has("SREM s2 "), 1, "{seen:?}");
    }

    // The replica holds what the master does.
    let mut master = Conn::connect(master_port);
    assert_eq!(master.cmds(&["WAIT", "1", "5000"]).as_int(), 1);
    for key in ["s", "s2"] {
        let members = |c: &mut Conn| {
            let mut m: Vec<String> = c
                .cmds(&["SMEMBERS", key])
                .as_array()
                .iter()
                .map(|v| v.as_str().to_string())
                .collect();
            m.sort();
            m
        };
        assert_eq!(members(&mut replica), members(&mut master), "{key}");
    }
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

/// `SELECT` writes nothing: a master without enough good replicas runs
/// it, as Redis does (#411), and a transaction that only selects and
/// reads. Writes are still refused, in whichever database.
#[test]
fn select_is_not_refused_for_want_of_good_replicas() {
    let master_port = free_port();
    start_server(master_port);
    let mut m = Conn::connect(master_port);
    assert!(
        m.cmds(&["CONFIG", "SET", "min-replicas-to-write", "1"])
            .is_ok()
    );
    assert!(error_of(&m.cmds(&["SET", "a", "1"])).starts_with("NOREPLICAS"));

    let r = m.cmds(&["SELECT", "1"]);
    assert!(r.is_ok(), "SELECT: {r:?}");
    assert!(m.cmds(&["MULTI"]).is_ok());
    assert_eq!(m.cmds(&["SELECT", "2"]).as_str(), "QUEUED");
    assert_eq!(m.cmds(&["GET", "a"]).as_str(), "QUEUED");
    let exec = m.cmds(&["EXEC"]);
    assert_eq!(exec.as_array().len(), 2, "{exec:?}");
    assert!(exec.as_array()[0].is_ok(), "{exec:?}");
    assert!(error_of(&m.cmds(&["SET", "a", "1"])).starts_with("NOREPLICAS"));
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

/// What `min-replicas-to-write` refuses is judged by the arguments, as Redis
/// judges it by the `write` flag. `GEORADIUS ... STORE` and `STOREDIST`
/// write, and are refused, alone and in a transaction; `GEORADIUS` without
/// them reads. `SELECT` and `SCRIPT LOAD` / `FLUSH` are propagated but are
/// not writes: they run, alone and in a transaction, as in Redis.
#[test]
fn min_replicas_judges_writes_by_their_arguments() {
    let port = free_port();
    start_server(port);
    let mut m = Conn::connect(port);
    m.cmds(&["GEOADD", "src", "13.361389", "38.115556", "palermo"]);
    assert!(
        m.cmds(&["CONFIG", "SET", "min-replicas-to-write", "1"])
            .is_ok()
    );

    let stored = m.cmds(&["GEORADIUS", "src", "15", "37", "200", "km", "STORE", "dst"]);
    assert!(error_of(&stored).starts_with("NOREPLICAS"), "{stored:?}");
    let dist = m.cmds(&[
        "GEORADIUSBYMEMBER",
        "src",
        "palermo",
        "200",
        "km",
        "STOREDIST",
        "dst",
    ]);
    assert!(error_of(&dist).starts_with("NOREPLICAS"), "{dist:?}");
    assert!(m.cmds(&["MULTI"]).is_ok());
    m.cmds(&["GEORADIUS", "src", "15", "37", "200", "km", "STORE", "dst"]);
    let exec = m.cmds(&["EXEC"]);
    assert!(error_of(&exec).starts_with("EXECABORT"), "{exec:?}");
    assert_eq!(m.cmds(&["EXISTS", "dst"]).as_int(), 0);
    let read = m.cmds(&["GEORADIUS", "src", "15", "37", "200", "km"]);
    assert_eq!(read.as_array().len(), 1, "{read:?}");

    #[cfg(feature = "lua")]
    {
        let sha = m.cmds(&["SCRIPT", "LOAD", "return 1"]);
        assert!(!matches!(sha, RespValue::Error(_)), "SCRIPT LOAD: {sha:?}");
        assert_eq!(sha.as_str().len(), 40);
        assert!(m.cmds(&["MULTI"]).is_ok());
        m.cmds(&["SCRIPT", "LOAD", "return 2"]);
        let exec = m.cmds(&["EXEC"]);
        assert!(
            !matches!(exec, RespValue::Error(_)),
            "MULTI SCRIPT LOAD: {exec:?}"
        );
        assert_eq!(exec.as_array().len(), 1);
        assert!(m.cmds(&["SCRIPT", "FLUSH"]).is_ok());
    }
    assert!(m.cmds(&["SELECT", "1"]).is_ok());
    assert!(m.cmds(&["SELECT", "0"]).is_ok());
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
    let info_reply = info_server_reply(None);
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
                "INFO" => stream.write_all(&info_reply).unwrap(),
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

// ═════════════════════════════════════════════════════════════════════════════
// TRANSACTIONS AND SCRIPTS AS ONE STEP ON A REPLICA (#172)
// ═════════════════════════════════════════════════════════════════════════════

/// Run `transfer` on the master many times while a reader on its replica
/// reads both keys at once: `a` and `b` start at 0 and every transfer moves 1
/// from one to the other, so a sum other than 0 is half a transfer.
fn assert_replica_never_shows_half_of(transfer: impl Fn(&mut Conn) + Send + 'static) {
    let master = spawn_server(Config::default());
    let replica = spawn_server(replica_of(master));
    let mut r = Conn::connect(replica);
    wait_link_up(&mut r);
    let mut m = Conn::connect(master);
    assert!(m.cmds(&["MSET", "a", "0", "b", "0"]).is_ok());
    wait_until(
        "the replica to have a and b",
        Duration::from_secs(10),
        || {
            r.cmds(&["MGET", "a", "b"])
                == RespValue::Array(vec![
                    RespValue::Bulk(b"0".to_vec()),
                    RespValue::Bulk(b"0".to_vec()),
                ])
        },
    );

    const TRANSFERS: i64 = 3000;
    let writer = thread::spawn(move || {
        for _ in 0..TRANSFERS {
            transfer(&mut m);
        }
    });
    let mut reads = 0u64;
    let mut torn = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let both = r.cmds(&["MGET", "a", "b"]);
        let both = both.as_array();
        let (a, b) = (both[0].as_int(), both[1].as_int());
        reads += 1;
        if a + b != 0 && torn.len() < 5 {
            torn.push((a, b));
        }
        // Until the replica has the last transfer.
        if writer.is_finished() && a == -TRANSFERS {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the replica stopped at a = {a}"
        );
    }
    writer.join().unwrap();
    assert!(
        torn.is_empty(),
        "a reader on the replica saw half a transfer (a, b): {torn:?} in {reads} reads"
    );
}

/// A transaction is one step on a replica too: a reader there never sees its
/// first write without its second. Its writes reached the replica one by
/// one, and were applied one by one (#172).
#[test]
fn test_a_replica_never_shows_half_a_transaction() {
    assert_replica_never_shows_half_of(|m| {
        assert!(m.cmds(&["MULTI"]).is_ok());
        m.cmds(&["DECRBY", "a", "1"]);
        m.cmds(&["INCRBY", "b", "1"]);
        assert_eq!(m.cmds(&["EXEC"]).as_array().len(), 2);
    });
}

/// The same for a script that writes twice.
#[cfg(feature = "lua")]
#[test]
fn test_a_replica_never_shows_half_a_script() {
    assert_replica_never_shows_half_of(|m| {
        let reply = m.cmds(&[
            "EVAL",
            "redis.call('DECRBY', KEYS[1], 1) return redis.call('INCRBY', KEYS[2], 1)",
            "2",
            "a",
            "b",
        ]);
        assert!(matches!(reply, RespValue::Int(_)), "{reply:?}");
    });
}

/// One command of the replication stream, as a replica reads it.
fn read_stream_command(r: &mut impl BufRead) -> Vec<String> {
    let header = read_line_raw(r);
    let count: usize = header
        .strip_prefix('*')
        .unwrap_or_else(|| panic!("not a command: {header:?}"))
        .parse()
        .unwrap();
    (0..count)
        .map(|_| {
            let len: usize = read_line_raw(r).trim_start_matches('$').parse().unwrap();
            let mut arg = vec![0u8; len + 2];
            r.read_exact(&mut arg).unwrap();
            arg.truncate(len);
            String::from_utf8(arg).unwrap()
        })
        .collect()
}

/// What a replica is sent for a transaction: its writes between a `MULTI`
/// and an `EXEC`, when it wrote more than once; as they are, when it wrote
/// once; nothing at all, when it wrote nothing.
#[test]
fn test_a_transaction_reaches_replicas_between_multi_and_exec() {
    let master = spawn_server(Config::default());
    // A replica by hand: the full sync, then the stream.
    let stream = TcpStream::connect(("127.0.0.1", master)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    writer
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let mut reader = BufReader::new(stream);
    let greeting = read_line_raw(&mut reader);
    assert!(greeting.starts_with("+FULLRESYNC"), "{greeting}");
    let len: usize = read_line_raw(&mut reader)
        .trim_start_matches('$')
        .parse()
        .unwrap();
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).unwrap();

    let mut m = Conn::connect(master);
    wait_until(
        "the master to count its replica",
        Duration::from_secs(10),
        || repl_info(&mut m, "connected_slaves").as_deref() == Some("1"),
    );
    for transaction in [
        &[&["SET", "x", "1"][..], &["GET", "x"], &["SET", "y", "2"]][..],
        &[&["SET", "z", "3"]],
        &[&["GET", "z"]],
    ] {
        assert!(m.cmds(&["MULTI"]).is_ok());
        for command in transaction {
            m.cmds(command);
        }
        assert_eq!(m.cmds(&["EXEC"]).as_array().len(), transaction.len());
    }
    assert!(m.cmds(&["SET", "last", "4"]).is_ok());

    let mut sent = Vec::new();
    while sent
        .last()
        .is_none_or(|c: &Vec<String>| c[0] != "SET" || c[1] != "last")
    {
        let command = read_stream_command(&mut reader);
        if !["PING", "SELECT", "REPLCONF"].contains(&command[0].as_str()) {
            sent.push(command);
        }
    }
    let words = |c: &[&str]| c.iter().map(|w| w.to_string()).collect::<Vec<_>>();
    assert_eq!(
        sent,
        vec![
            words(&["MULTI"]),
            words(&["SET", "x", "1"]),
            words(&["SET", "y", "2"]),
            words(&["EXEC"]),
            words(&["SET", "z", "3"]),
            words(&["SET", "last", "4"]),
        ]
    );
}

/// A replica that stops sending `REPLCONF ACK` without closing its
/// connection (its VM terminated, its container paused) is dropped after
/// `repl-timeout`, as in Redis. It was listed `online`, its lag growing, and
/// counted in `connected_slaves` until the master restarted. A replica that
/// goes on acknowledging stays.
#[test]
fn test_a_silent_replica_is_dropped_after_repl_timeout() {
    let master_port = spawn_server(Config::default());
    let mut master = Conn::connect(master_port);
    let live_port = spawn_server(replica_of(master_port));
    wait_link_up(&mut Conn::connect(live_port));

    // A replica that syncs, acknowledges once, and then says nothing more,
    // its connection left open.
    let mut silent = Conn::connect(master_port);
    assert!(silent.cmds(&["REPLCONF", "listening-port", "7777"]).is_ok());
    silent
        .stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let mut line = String::new();
    silent.reader.read_line(&mut line).unwrap();
    let offset = line
        .trim_end()
        .strip_prefix("+FULLRESYNC ")
        .and_then(|rest| rest.split(' ').nth(1))
        .unwrap_or_else(|| panic!("not a FULLRESYNC: {line:?}"))
        .to_string();
    // The payload, after any newlines the master sends while it prepares it.
    let len = loop {
        line.clear();
        silent.reader.read_line(&mut line).unwrap();
        if let Some(len) = line.trim_end().strip_prefix('$') {
            break len.parse::<usize>().unwrap();
        }
    };
    let mut payload = vec![0u8; len];
    silent.reader.read_exact(&mut payload).unwrap();
    let ack = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{offset}\r\n",
        offset.len()
    );
    silent.stream.write_all(ack.as_bytes()).unwrap();
    wait_until("both replicas online", Duration::from_secs(10), || {
        repl_info(&mut master, "connected_slaves").as_deref() == Some("2")
            && master
                .cmds(&["INFO", "replication"])
                .as_str()
                .contains("port=7777,state=online")
    });

    assert!(master.cmds(&["CONFIG", "SET", "repl-timeout", "2"]).is_ok());
    wait_until(
        "the silent replica dropped",
        Duration::from_secs(10),
        || repl_info(&mut master, "connected_slaves").as_deref() == Some("1"),
    );
    // The one that is left is the live one, and it stays.
    thread::sleep(Duration::from_secs(4));
    assert_eq!(
        repl_info(&mut master, "connected_slaves").as_deref(),
        Some("1")
    );
    let slave0 = repl_info(&mut master, "slave0").unwrap();
    assert!(
        !slave0.contains("port=7777") && slave0.contains("state=online"),
        "{slave0}"
    );
    // And the silent one's connection was closed.
    let mut rest = Vec::new();
    silent
        .stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let closed = loop {
        let mut buf = [0u8; 4096];
        match silent.reader.read(&mut buf) {
            Ok(0) => break true,
            Ok(n) => rest.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break true,
            Err(_) => break false,
        }
    };
    assert!(closed, "the silent replica's connection is still open");
}

// ═════════════════════════════════════════════════════════════════════════════
// FT.DROPINDEX DD — a DEL of each document, then the drop
// ═════════════════════════════════════════════════════════════════════════════

/// A replica of `master_port` made by hand: synced, and then reading the
/// stream itself. Returns the connection, positioned after the payload.
fn attach_stream_reader(master_port: u16) -> Conn {
    let mut tap = Conn::connect(master_port);
    assert!(tap.cmds(&["REPLCONF", "listening-port", "7778"]).is_ok());
    tap.stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let mut line = String::new();
    tap.reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("+FULLRESYNC"), "{line:?}");
    // The payload, after any newlines the master sends while it prepares it.
    let len = loop {
        line.clear();
        tap.reader.read_line(&mut line).unwrap();
        if let Some(len) = line.trim_end().strip_prefix('$') {
            break len.parse::<usize>().unwrap();
        }
    };
    let mut payload = vec![0u8; len];
    tap.reader.read_exact(&mut payload).unwrap();
    tap
}

/// The commands `tap` is sent, each as its arguments joined by spaces, up to
/// and including the one `last` picks. The link's own (`PING`, `REPLCONF`)
/// are left out.
fn stream_until(tap: &mut Conn, last: impl Fn(&str) -> bool) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        let command = tap
            .read_value()
            .as_array()
            .iter()
            .map(|part| part.as_str().to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let name = command.split(' ').next().unwrap_or("").to_ascii_uppercase();
        if name == "PING" || name == "REPLCONF" {
            continue;
        }
        let done = last(&command);
        seen.push(command);
        if done {
            return seen;
        }
    }
}

/// `FT.DROPINDEX … DD` reaches replicas as Redis 8.10 sends it: a DEL of
/// each document deleted, and then the drop without `DD` — for a HASH index,
/// a JSON one and one in another database, whose DELs are sent in it. The
/// replica deletes what it is sent: it used to be sent the `DD` and delete
/// whatever its own copy of the index covered.
#[test]
fn test_dropindex_dd_is_sent_as_a_del_of_each_document_then_the_drop() {
    let master_port = spawn_server(Config::default());
    let mut master = Conn::connect(master_port);
    let replica_port = spawn_server(replica_of(master_port));
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);
    let mut tap = attach_stream_reader(master_port);

    let create = |c: &mut Conn, name: &str, on: &str, prefix: &str, field: &[&str]| {
        let mut args = vec!["FT.CREATE", name, "ON", on, "PREFIX", "1", prefix, "SCHEMA"];
        args.extend_from_slice(field);
        let reply = c.cmds(&args);
        assert!(reply.is_ok(), "{reply:?}");
    };
    create(&mut master, "idx:h", "HASH", "h:", &["t", "TEXT"]);
    master.cmds(&["HSET", "h:1", "t", "a"]);
    master.cmds(&["HSET", "h:2", "t", "b"]);
    master.cmds(&["SET", "h:3", "a string, not a document"]);
    master.cmds(&["HSET", "other:1", "t", "c"]);
    create(
        &mut master,
        "idx:j",
        "JSON",
        "j:",
        &["$.t", "AS", "t", "TEXT"],
    );
    master.cmds(&["JSON.SET", "j:1", "$", r#"{"t":"a"}"#]);
    master.cmds(&["HSET", "j:2", "t", "a hash, not a JSON document"]);
    let mut db3 = Conn::connect(master_port);
    assert!(db3.cmds(&["SELECT", "3"]).is_ok());
    create(&mut db3, "idx:d", "HASH", "d:", &["t", "TEXT"]);
    db3.cmds(&["HSET", "d:1", "t", "a"]);
    db3.cmds(&["HSET", "d:2", "t", "b"]);
    // The same name in database 0: not a document of the index.
    master.cmds(&["HSET", "d:1", "t", "database 0's"]);
    wait_until("the replica has the keys", Duration::from_secs(10), || {
        replica
            .cmds(&[
                "EXISTS", "h:1", "h:2", "h:3", "other:1", "j:1", "j:2", "d:1",
            ])
            .as_int()
            == 7
    });

    assert!(master.cmds(&["FT.DROPINDEX", "idx:h", "DD"]).is_ok());
    assert!(master.cmds(&["FT.DROPINDEX", "idx:j", "dd"]).is_ok());
    assert!(db3.cmds(&["FT.DROPINDEX", "idx:d", "DD"]).is_ok());

    let stream = stream_until(&mut tap, |command| command == "FT.DROPINDEX idx:d");
    let first_del = stream
        .iter()
        .position(|command| command.starts_with("DEL "))
        .unwrap_or_else(|| panic!("no DEL sent: {stream:?}"));
    // Once a database other than 0 has been used, every command carries its
    // own `SELECT`: only those that change the database are kept here.
    let mut db = "0";
    let mut sent: Vec<String> = Vec::new();
    for command in &stream[first_del..] {
        if let Some(selected) = command.strip_prefix("SELECT ") {
            if selected == db {
                continue;
            }
            db = selected;
        }
        sent.push(command.clone());
    }
    assert_eq!(
        dels_merged(&sent),
        [
            "DEL h:1 h:2",
            "FT.DROPINDEX idx:h",
            "DEL j:1",
            "FT.DROPINDEX idx:j",
            "SELECT 3",
            "DEL d:1 d:2",
            "FT.DROPINDEX idx:d",
        ]
    );

    wait_until(
        "the replica deletes the documents",
        Duration::from_secs(10),
        || replica.cmds(&["EXISTS", "h:1", "h:2", "j:1"]).as_int() == 0,
    );
    assert_eq!(
        replica
            .cmds(&["EXISTS", "h:3", "other:1", "j:2", "d:1"])
            .as_int(),
        4
    );
    assert!(replica.cmds(&["SELECT", "3"]).is_ok());
    wait_until(
        "the replica deletes the documents of database 3",
        Duration::from_secs(10),
        || replica.cmds(&["EXISTS", "d:1", "d:2"]).as_int() == 0,
    );
    // The drop comes after the DELs: the replica can have applied them and
    // not it yet.
    wait_until(
        "the replica drops the indexes",
        Duration::from_secs(10),
        || replica.cmds(&["FT._LIST"]).as_array().is_empty(),
    );
    // `FT._LIST` lists the selected database's indexes: database 0's went
    // before database 3's.
    assert!(replica.cmds(&["SELECT", "0"]).is_ok());
    assert!(replica.cmds(&["FT._LIST"]).as_array().is_empty());
}

/// `commands` with each run of DELs made one, its keys in order: the
/// documents of an index are deleted in no particular order, and sent in
/// as many DELs as it takes.
fn dels_merged(commands: &[String]) -> Vec<String> {
    let mut merged: Vec<String> = Vec::new();
    for command in commands {
        match (command.strip_prefix("DEL "), merged.last_mut()) {
            (Some(keys), Some(last)) if last.starts_with("DEL ") => {
                last.push(' ');
                last.push_str(keys);
            }
            _ => merged.push(command.clone()),
        }
    }
    for command in &mut merged {
        if let Some(keys) = command.strip_prefix("DEL ") {
            let mut keys: Vec<&str> = keys.split(' ').collect();
            keys.sort();
            *command = format!("DEL {}", keys.join(" "));
        }
    }
    merged
}

/// What a stand-in master answers `INFO server` with: the KiviDB release
/// `version`, or with none, an error, as a server that does not say.
fn info_server_reply(version: Option<&str>) -> Vec<u8> {
    match version {
        Some(version) => {
            let body = format!("# Server\r\nredis_version:8.10.0\r\nkividb_version:{version}\r\n");
            format!("${}\r\n{body}\r\n", body.len()).into_bytes()
        }
        None => b"-ERR unknown command\r\n".to_vec(),
    }
}

/// A stand-in master that answers the handshake as KiviDB `version` (see
/// [`info_server_reply`]), sends `payload` and then `commands`, and keeps
/// the link open. Returns its port.
fn spawn_master_sending(
    version: Option<&str>,
    payload: Vec<u8>,
    commands: Vec<Vec<String>>,
) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let info_reply = info_server_reply(version);
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
                "INFO" => stream.write_all(&info_reply).unwrap(),
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
        stream.write_all(&payload).unwrap();
        for command in &commands {
            let mut encoded = format!("*{}\r\n", command.len()).into_bytes();
            for part in command {
                encoded.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
            }
            if stream.write_all(&encoded).is_err() {
                return;
            }
        }
        let mut sink = [0u8; 256];
        while reader.read(&mut sink).unwrap_or(0) > 0 {}
    });
    port
}

/// A replica of a master `version` (`None`: one that does not say) is sent
/// a drop of this release's kind, after the DEL of the one document its
/// master deleted, and then a drop with `DD`. It returns whether the
/// documents of the second were deleted. The first is checked here.
fn replica_fed_a_dd(version: Option<&str>) -> bool {
    let empty = spawn_server(Config::default());
    let payload = full_sync_payload_of(empty);
    let commands: Vec<Vec<String>> = [
        &["SELECT", "0"][..],
        &[
            "FT.CREATE",
            "idx:new",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "n:",
            "SCHEMA",
            "t",
            "TEXT",
        ],
        &["HSET", "n:1", "t", "a"],
        // One its master kept: written to the master after the DD found
        // the documents, say, or deleted there by something else first.
        &["HSET", "n:2", "t", "b"],
        &["DEL", "n:1"],
        &["FT.DROPINDEX", "idx:new"],
        &[
            "FT.CREATE",
            "idx:old",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "o:",
            "SCHEMA",
            "t",
            "TEXT",
        ],
        &["HSET", "o:1", "t", "a"],
        &["HSET", "o:2", "t", "b"],
        &["FT.DROPINDEX", "idx:old", "DD"],
        &["HSET", "o:3", "t", "after the drop"],
        &["SET", "done", "1"],
    ]
    .iter()
    .map(|command| command.iter().map(|part| part.to_string()).collect())
    .collect();
    let master = spawn_master_sending(version, payload, commands);
    let replica_port = spawn_server(replica_of(master));
    let mut replica = Conn::connect(replica_port);
    wait_until("the stream is applied", Duration::from_secs(10), || {
        matches!(replica.cmds(&["EXISTS", "done"]), RespValue::Int(1))
    });

    assert_eq!(replica.cmds(&["EXISTS", "n:1"]).as_int(), 0);
    assert_eq!(
        replica.cmds(&["EXISTS", "n:2"]).as_int(),
        1,
        "the replica deleted a document its master did not"
    );
    assert_eq!(replica.cmds(&["EXISTS", "o:3"]).as_int(), 1);
    assert!(replica.cmds(&["FT._LIST"]).as_array().is_empty());
    match replica.cmds(&["EXISTS", "o:1", "o:2"]).as_int() {
        0 => true,
        2 => false,
        n => panic!("{n} of the two documents left"),
    }
}

/// A replica deletes the documents its master sends it a DEL of, and no
/// others: the drop it is sent after them, without `DD`, drops the index
/// only. A `DD` it is sent comes from a release before this one, and is run
/// as that release ran it: a 1.2.0 release candidate deleted the documents
/// and left each replica to delete its own.
#[test]
fn test_a_replica_of_a_1_2_0_candidate_deletes_the_documents_of_its_dd() {
    assert!(replica_fed_a_dd(Some("1.2.0-rc2")));
}

/// 1.1 kept the documents of `DD`: a replica of a 1.1 master keeps them
/// too, as one of that release did. They were deleted, and the replica no
/// longer had what its master had.
#[test]
fn test_a_replica_of_a_1_1_master_keeps_the_documents_of_its_dd() {
    assert!(!replica_fed_a_dd(Some("1.1.2")));
}

/// A master that does not say which release it is is taken to have kept
/// the documents of a `DD`: deleting what it kept would lose data.
#[test]
fn test_a_replica_of_a_master_that_does_not_say_keeps_the_documents_of_its_dd() {
    assert!(!replica_fed_a_dd(None));
}

/// The empty key crosses a full sync (#441). In the payload it was written
/// with the key length that ends a database: the replica found the payload
/// unreadable, kept what it had, and never came up.
#[test]
fn test_full_resync_carries_the_empty_key() {
    let master_port = spawn_server(Config::default());
    let mut master = Conn::connect(master_port);
    assert!(master.cmds(&["SET", "", "x"]).is_ok());
    assert_eq!(master.cmds(&["HSET", "hash", "", "v"]).as_int(), 1);
    assert_eq!(master.cmds(&["SADD", "set", ""]).as_int(), 1);
    assert_eq!(master.cmds(&["ZADD", "zset", "1.5", ""]).as_int(), 1);
    assert!(master.cmds(&["SELECT", "1"]).is_ok());
    assert_eq!(master.cmds(&["RPUSH", "", ""]).as_int(), 1);
    assert!(master.cmds(&["SET", "after", "1"]).is_ok());

    let replica_port = spawn_server(replica_of(master_port));
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);
    assert_eq!(replica.cmds(&["GET", ""]).as_str(), "x");
    assert_eq!(replica.cmds(&["HGET", "hash", ""]).as_str(), "v");
    assert_eq!(replica.cmds(&["SISMEMBER", "set", ""]).as_int(), 1);
    assert_eq!(replica.cmds(&["ZSCORE", "zset", ""]).as_str(), "1.5");
    assert!(replica.cmds(&["SELECT", "1"]).is_ok());
    assert_eq!(replica.cmds(&["LLEN", ""]).as_int(), 1);
    assert_eq!(replica.cmds(&["GET", "after"]).as_str(), "1");
}

/// A replica holds a JSON document's numbers exactly as its master does,
/// from the full sync and from the stream after it (#445).
#[test]
fn a_replica_holds_json_numbers_exactly() {
    let mut state: u64 = 0x1234_5678_9ABC_DEF1;
    let mut doubles = Vec::new();
    while doubles.len() < 2000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let d = if doubles.len() % 2 == 0 {
            f64::from_bits(state)
        } else {
            (state >> 11) as f64 / (1u64 << 53) as f64 * 2e6 - 1e6
        };
        if d.is_finite() {
            doubles.push(d);
        }
    }
    let text = |chunk: &[f64]| {
        let items: Vec<String> = chunk.iter().map(|d| format!("{d:?}")).collect();
        format!("[{}]", items.join(","))
    };
    let exact = |c: &mut Conn, key: &str, want: &[f64]| {
        let got = c.cmds(&["JSON.GET", key]);
        let got: Vec<f64> = got
            .as_str()
            .trim_matches(|c| c == '[' || c == ']')
            .split(',')
            .map(|n| n.parse().unwrap())
            .collect();
        want.len() == got.len()
            && want
                .iter()
                .zip(&got)
                .all(|(w, g)| w.to_bits() == g.to_bits())
    };

    let master_port = free_port();
    start_server(master_port);
    let _shutdown = ShutdownServers(vec![master_port]);
    let mut master = Conn::connect(master_port);
    let (synced, streamed) = doubles.split_at(1000);
    assert!(
        master
            .cmds(&["JSON.SET", "jr:sync", "$", &text(synced)])
            .is_ok()
    );
    assert!(exact(&mut master, "jr:sync", synced));

    let replica_port = spawn_server(replica_of(master_port));
    let _shutdown_replica = ShutdownServers(vec![replica_port]);
    let mut replica = Conn::connect(replica_port);
    wait_link_up(&mut replica);
    assert!(
        master
            .cmds(&["JSON.SET", "jr:stream", "$", &text(streamed)])
            .is_ok()
    );
    wait_until("the replica has jr:stream", Duration::from_secs(10), || {
        matches!(replica.cmds(&["EXISTS", "jr:stream"]), RespValue::Int(1))
    });
    assert!(exact(&mut replica, "jr:sync", synced), "from the full sync");
    assert!(
        exact(&mut replica, "jr:stream", streamed),
        "from the stream"
    );
}

/// A stand-in master that answers the handshake, sends `payload` as a full
/// sync and then `commands`, as a KeyDB primary would.
fn keydb_master(payload: Vec<u8>, commands: &[&[&str]]) -> u16 {
    let commands = commands
        .iter()
        .map(|c| c.iter().map(|s| s.to_string()).collect())
        .collect();
    spawn_master_sending(None, payload, commands)
}

/// REDIS0009, database 0: a set s {m1} and a hash h {f: v}.
fn keydb_payload() -> Vec<u8> {
    let mut payload = b"REDIS0009".to_vec();
    payload.extend_from_slice(&[0xFE, 0]);
    payload.extend_from_slice(&[2, 1, b's', 1, 2, b'm', b'1']);
    payload.extend_from_slice(&[4, 1, b'h', 1, 1, b'f', 1, b'v']);
    payload.push(0xFF);
    payload.extend_from_slice(&[0; 8]);
    payload
}

/// Waits for the link to end on a refusal, and returns its reason.
fn refusal_of(c: &mut Conn) -> String {
    wait_until(
        "the link to end on a refusal",
        Duration::from_secs(10),
        || repl_info(c, "replica_last_sync_refusal").is_some(),
    );
    assert_eq!(repl_info(c, "master_link_status").as_deref(), Some("down"));
    repl_info(c, "replica_last_sync_refusal").unwrap()
}

/// KeyDB's `EXPIREMEMBER` on a hash field is the field's TTL here. On a set
/// member, which can't have one, it ends the link, and `INFO replication`
/// says why; nothing after it is applied. Before, it was an unknown command,
/// refused while the link stayed up (#452).
#[test]
fn test_a_replica_of_keydb_applies_field_ttls_and_stops_at_a_set_member_ttl() {
    let master = keydb_master(
        keydb_payload(),
        &[
            &["EXPIREMEMBER", "h", "f", "1000"],
            &["SET", "plain", "1"],
            &["EXPIREMEMBER", "s", "m1", "1000"],
            &["SET", "after", "1"],
        ],
    );
    let node = spawn_server(replica_of(master));
    let mut c = Conn::connect(node);
    let why = refusal_of(&mut c);
    assert!(why.contains("m1") && why.contains("not a hash"), "{why}");
    assert_eq!(c.cmds(&["GET", "plain"]).as_str(), "1");
    let ttl = c.cmds(&["HTTL", "h", "FIELDS", "1", "f"]);
    let ttl = ttl.as_array()[0].as_int();
    assert!((990..=1000).contains(&ttl), "h f's TTL is {ttl}");
    assert_eq!(c.cmds(&["EXISTS", "after"]).as_int(), 0);
}

/// A KeyDB primary in active replication sends every write wrapped in
/// `RREPLAY`, nested once more for each master of the mesh it went through.
/// It was an unknown command, refused while the link showed as up. It is
/// not followed (not in 1.2.1): the link ends, saying so, and nothing after
/// it is applied (#452).
#[test]
fn test_a_replica_of_keydb_in_active_replication_stops_at_rreplay() {
    let origin = "4f1c2b52-0d0f-4a5e-9a3c-2b1d5f6e7a8b";
    let master = keydb_master(
        keydb_payload(),
        &[
            &["SET", "before", "1"],
            &[
                "RREPLAY",
                origin,
                "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
                "0",
                "5",
            ],
            &["SET", "after", "1"],
        ],
    );
    let node = spawn_server(replica_of(master));
    let mut c = Conn::connect(node);
    let why = refusal_of(&mut c);
    assert!(
        why.contains("KeyDB active replication (RREPLAY) is not supported"),
        "{why}"
    );
    assert_eq!(c.cmds(&["GET", "before"]).as_str(), "1");
    assert_eq!(c.cmds(&["EXISTS", "k"]).as_int(), 0);
    assert_eq!(c.cmds(&["EXISTS", "after"]).as_int(), 0);
}

/// A 1.2.0 master sends `TS.INCRBY` with `TIMESTAMP <time>` at the end, and
/// read no other option. A 1.2.1 replica reads it so: the sample at that
/// time, the options passed over, no trim to the retention. It is the
/// master's dataset; read as 1.2.1 reads a command, the replica trimmed
/// samples the master kept, made a label of `TIMESTAMP`, and put a sample
/// at its own time (#555).
#[test]
fn test_a_replica_of_1_2_0_reads_its_ts_incrby_as_1_2_0_did() {
    let mut payload = b"REDIS0009".to_vec();
    payload.push(0xFF);
    payload.extend_from_slice(&[0; 8]);
    let master = keydb_master(
        payload,
        &[
            &["TS.CREATE", "r", "RETENTION", "10"],
            &["TS.INCRBY", "r", "1", "TIMESTAMP", "1"],
            &["TS.INCRBY", "r", "1", "TIMESTAMP", "100"],
            &[
                "TS.INCRBY",
                "c",
                "1",
                "RETENTION",
                "10",
                "LABELS",
                "a",
                "TIMESTAMP",
                "5",
            ],
            &["TS.INCRBY", "c", "1", "LABELS", "x", "TIMESTAMP", "50"],
            &["SET", "done", "1"],
        ],
    );
    let node = spawn_server(replica_of(master));
    let mut c = Conn::connect(node);
    wait_until("the stream to be applied", Duration::from_secs(10), || {
        matches!(c.cmds(&["EXISTS", "done"]), RespValue::Int(1))
    });
    let samples = |c: &mut Conn, key: &str| -> Vec<(i64, String)> {
        c.cmds(&["TS.RANGE", key, "-", "+"])
            .as_array()
            .iter()
            .map(|s| {
                (
                    s.as_array()[0].as_int(),
                    s.as_array()[1].as_str().to_string(),
                )
            })
            .collect()
    };
    let num = |n: &str| n.to_string();
    assert_eq!(samples(&mut c, "r"), [(1, num("1")), (100, num("2"))]);
    assert_eq!(samples(&mut c, "c"), [(5, num("1")), (50, num("2"))]);
    let info = c.cmds(&["TS.INFO", "c"]);
    let info = info.as_array();
    let field = |name: &str| {
        let at = info
            .iter()
            .position(|v| matches!(v, RespValue::Bulk(b) if b == name.as_bytes()))
            .unwrap_or_else(|| panic!("no {name}"));
        &info[at + 1]
    };
    assert_eq!(field("retentionTime").as_str(), "0");
    assert_eq!(field("labels"), &RespValue::Array(vec![]));
}

/// A Redis full sync that does not read is a broken link, retried as
/// before: not a refusal of what the master holds (nothing in `INFO`), which
/// waits a minute or more between tries.
#[test]
fn test_a_redis_payload_that_does_not_read_is_a_broken_link_not_a_refusal() {
    let mut payload = b"REDIS0011".to_vec();
    payload.extend_from_slice(&[0xFE, 0, 0xF1, 1, 2, 3]);
    payload.push(0xFF);
    payload.extend_from_slice(&[0; 8]);
    let master = keydb_master(payload, &[]);
    let node = spawn_server(replica_of(master));
    let mut c = Conn::connect(node);
    wait_until("the full sync to fail", Duration::from_secs(10), || {
        repl_info(&mut c, "master_link_status").as_deref() == Some("down")
            && repl_info(&mut c, "master_sync_in_progress").as_deref() != Some("1")
    });
    thread::sleep(Duration::from_secs(2));
    assert_eq!(repl_info(&mut c, "replica_last_sync_refusal"), None);
}
