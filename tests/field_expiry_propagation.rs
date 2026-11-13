// tests/field_expiry_propagation.rs
//
// A hash field's expiry, as the readers of the write stream see it: a
// replica, one that lags, one in the middle of a full sync, and the AOF
// replayed after a kill (#415). Real `kividb` processes.
//
// Only the master removes a field for its time, and it sends the removal
// on as an `HDEL` (the `DEL` of the hash, if that emptied it), ahead of the
// write that found the field so. Nothing removes a field for its time while
// the AOF is replayed, a dataset loaded or a master's stream applied, and a
// replica shows a field past its time as gone and leaves it for the
// master's `HDEL`. Before, each removed fields by its own clock, and a write
// the master made to a field while it lived, applied after its time, did
// something else there.
//
// The I/O model is the platform's own unless `KIVIDB_TEST_IO_MODEL` says.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn s(&self) -> &str {
        match self {
            V::Str(s) | V::Err(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn arr(&self) -> &[V] {
        match self {
            V::Arr(v) => v,
            other => panic!("not an array: {other:?}"),
        }
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                let writer = s.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(s),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Str(String::from_utf8_lossy(&data).into_owned())
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Arr((0..n).map(|_| self.read()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }

    /// One field of `INFO <section>`.
    fn info(&mut self, section: &str, field: &str) -> String {
        let reply = self.cmd(&["INFO", section]);
        reply
            .s()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .trim()
            .to_string()
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

/// Whether nothing has `port`: asked with a socket that is bound and never
/// listens (see `tests/resync.rs`).
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

/// A port nothing listens on, outside the range handed to outgoing
/// connections.
fn free_port() -> u16 {
    use std::sync::atomic::AtomicU16;
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(41);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn kividb(dir: &Path, port: u16, extra: &[String], env: &[(String, String)]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
    command.current_dir(dir).args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--threads",
        "4",
        "--configfile",
        "kividb.conf",
    ]);
    if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
        command.args(["--io-model", &model]);
    }
    command
        .args(extra)
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
    args: Vec<String>,
    /// The environment it is started with, besides the test's.
    env: Vec<(String, String)>,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let child = kividb(&dir, port, &args, &[]);
        let server = Server {
            port,
            dir,
            child,
            args,
            env: Vec::new(),
        };
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn aof(&self) -> Vec<u8> {
        std::fs::read(self.dir.join("appendonly.aof")).unwrap_or_default()
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    /// `SHUTDOWN`: the final save, and then the process is gone.
    fn shutdown(&mut self) {
        let mut c = self.conn();
        let _ = c.writer.write_all(b"*1\r\n$8\r\nSHUTDOWN\r\n");
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "the server did not shut down");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Killed, with no chance to save or to expire anything more.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Started again on the same port and directory. Under io_uring a
    /// connection made the moment it comes up can be reset (master does the
    /// same): it is tried again.
    fn restart(&mut self) {
        self.child = kividb(&self.dir, self.port, &self.args, &self.env);
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let up = TcpStream::connect(("127.0.0.1", self.port)).and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(10)))?;
                s.write_all(b"*1\r\n$4\r\nPING\r\n")?;
                let mut reply = [0u8; 7];
                s.read_exact(&mut reply)?;
                Ok(&reply == b"+PONG\r\n")
            });
            if up.unwrap_or(false) {
                return;
            }
            assert!(Instant::now() < deadline, "the server did not come back");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Stopped where it stands (SIGSTOP), or let go on (SIGCONT): a replica
    /// that lags, with its link still open.
    #[cfg(unix)]
    fn signal(&self, signal: libc::c_int) {
        // SAFETY: a signal to our own child, which has not been reaped.
        assert_eq!(
            unsafe { libc::kill(self.child.id() as libc::pid_t, signal) },
            0
        );
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        #[cfg(unix)]
        self.signal(libc::SIGCONT);
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn wait_until(what: &str, secs: u64, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_link_up(replica: &mut Conn) {
    wait_until("the replica's link to come up", 30, || {
        replica.info("replication", "master_link_status") == "up"
    });
}

/// The replica has applied everything its master has sent.
fn wait_caught_up(master: &mut Conn, replica: &mut Conn) {
    wait_until("the replica to catch up", 60, || {
        let offset = master.info("replication", "master_repl_offset");
        replica.info("replication", "slave_repl_offset") == offset
            && master.info("replication", "master_repl_offset") == offset
    });
}

fn count(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|w| *w == needle)
        .count()
}

fn position(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ── Field helpers ─────────────────────────────────────────────────────────────

/// Milliseconds since the Unix epoch, now.
fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// A server started with `conf` as its configuration file.
fn start_with_conf(extra: &[&str], conf: &str) -> Server {
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::write(dir.join("kividb.conf"), conf).unwrap();
    let port = free_port();
    let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    let child = kividb(&dir, port, &args, &[]);
    let server = Server {
        port,
        dir,
        child,
        args,
        env: Vec::new(),
    };
    assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
    server
}

/// What `key` holds, as clients see it: a hash's fields, sorted, each with
/// its value and whether it has a time to live (1) or none (-1); a list's
/// elements; or the type.
fn state(c: &mut Conn, key: &str) -> String {
    match c.cmd(&["TYPE", key]).s() {
        "hash" => {
            let all = c.cmd(&["HGETALL", key]);
            let mut fields: Vec<(String, String, i64)> = all
                .arr()
                .chunks(2)
                .map(|pair| {
                    let field = pair[0].s().to_string();
                    let ttl = c.cmd(&["HPTTL", key, "FIELDS", "1", &field]).arr()[0].i();
                    (field, pair[1].s().to_string(), ttl.signum())
                })
                .collect();
            fields.sort();
            format!("hash {fields:?}")
        }
        "list" => format!("list {:?}", c.cmd(&["LRANGE", key, "0", "-1"])),
        other => other.to_string(),
    }
}

/// The keys `{kind}:{i}` hold the same on both, `i` below `n`.
fn assert_same_keys(master: &mut Conn, replica: &mut Conn, kinds: &[&str], n: usize) {
    wait_caught_up(master, replica);
    let mut differ = Vec::new();
    for kind in kinds {
        for i in 0..n {
            let key = format!("{kind}:{i}");
            let (m, r) = (state(master, &key), state(replica, &key));
            if m != r {
                differ.push(format!("{key}: master {m}, replica {r}"));
            }
        }
    }
    assert!(
        differ.is_empty(),
        "{} keys differ, e.g. {:?}",
        differ.len(),
        &differ[..differ.len().min(5)]
    );
    assert_eq!(master.cmd(&["DBSIZE"]), replica.cmd(&["DBSIZE"]));
}

/// `HSET {kind}:{i} f 5 keep 1` and `HPEXPIREAT … at FIELDS 1 f`, of each
/// kind, `i` below `n`.
fn hashes_with_f_due_at(c: &mut Conn, kinds: &[&str], n: usize, at: u64) {
    for kind in kinds {
        for i in 0..n {
            let key = format!("{kind}:{i}");
            assert_eq!(c.cmd(&["HSET", &key, "f", "5", "keep", "1"]).i(), 2);
            assert_eq!(
                c.cmd(&["HPEXPIREAT", &key, &at.to_string(), "FIELDS", "1", "f"]),
                V::Arr(vec![V::Int(1)])
            );
        }
    }
}

/// Until no field of `{kind}:{i}` is past its time on the master: each gone,
/// and its `HDEL` on its way.
fn wait_fields_expired(master: &mut Conn, kinds: &[&str], n: usize) {
    wait_until(
        "the master to expire the fields past their time",
        30,
        || {
            kinds.iter().all(|kind| {
                (0..n).all(|i| {
                    let key = format!("{kind}:{i}");
                    let ttl = master.cmd(&["HPTTL", &key, "FIELDS", "1", "f"]);
                    !matches!(ttl.arr().first(), Some(V::Int(t)) if *t > 0)
                })
            })
        },
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// `HINCRBY`, `HINCRBYFLOAT`, `HSETNX` and `HPERSIST` of a field made on
/// the master just before the field's deadline, applied by a replica that
/// lags (stopped, its link open) after it. The replica removed the field by
/// its own clock first: `HINCRBY` made it again, `1` with no deadline, for
/// good, `HSETNX` set it, and `HPERSIST` found nothing to persist and the
/// field went on the replica, while the master kept it for ever (100 of 100
/// tries with 20 ms of lag in #415). Now the replica applies each to the
/// field as the master had it, and removes the field when the master's
/// `HDEL` comes.
#[cfg(unix)]
#[test]
fn writes_made_just_before_a_field_deadline_reach_a_lagging_replica_as_made() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);
    let (kinds, n) = (["incr", "float", "nx", "persist"], 30);
    let at = unix_ms_now() + 1_500;
    hashes_with_f_due_at(&mut m, &kinds, n, at);
    wait_caught_up(&mut m, &mut r);
    replica.signal(libc::SIGSTOP);
    for i in 0..n {
        assert_eq!(m.cmd(&["HINCRBY", &format!("incr:{i}"), "f", "1"]).i(), 6);
        assert_eq!(
            m.cmd(&["HINCRBYFLOAT", &format!("float:{i}"), "f", "1.5"])
                .s(),
            "6.5"
        );
        assert_eq!(m.cmd(&["HSETNX", &format!("nx:{i}"), "f", "new"]).i(), 0);
        assert_eq!(
            m.cmd(&["HPERSIST", &format!("persist:{i}"), "FIELDS", "1", "f"]),
            V::Arr(vec![V::Int(1)])
        );
    }
    assert!(unix_ms_now() < at, "the writes came after the deadline");
    std::thread::sleep(Duration::from_millis(at - unix_ms_now() + 500));
    replica.signal(libc::SIGCONT);
    wait_fields_expired(&mut m, &kinds, n);
    assert_same_keys(&mut m, &mut r, &kinds, n);
    for i in 0..n {
        assert_eq!(state(&mut r, &format!("incr:{i}")), state(&mut m, "incr:0"));
        assert_eq!(
            r.cmd(&["HGET", &format!("persist:{i}"), "f"]).s(),
            "5",
            "persist:{i} lost its field"
        );
    }
    assert_eq!(
        state(&mut m, "incr:0"),
        r#"hash [("keep", "1", -1)]"#,
        "the incremented field went at its time"
    );
    // Still so a while later, the replica's own sweep having had its turn.
    std::thread::sleep(Duration::from_millis(500));
    assert_same_keys(&mut m, &mut r, &kinds, n);
}

/// A write that finds a field past its time, before the master's sweep
/// has removed it, takes it as gone, as Redis does: `HINCRBY` and
/// `HINCRBYFLOAT` start it over, `HSETNX` sets it (it answered 0, the field
/// still there to it), and `HPEXPIRE … XX` or `GT` finds no field (-2; it
/// gave the field a new time, and it lived on, where a replica that had
/// removed it had none). The removal reaches the replica ahead of the
/// write.
#[test]
fn a_write_that_finds_a_field_past_its_time_takes_it_as_gone_on_both() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);
    let (kinds, n) = (["incr", "float", "nx", "xx", "gt"], 30);
    // The master's sweep held off: each write finds its field itself.
    assert_eq!(m.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).s(), "OK");
    let at = unix_ms_now() + 300;
    hashes_with_f_due_at(&mut m, &kinds, n, at);
    std::thread::sleep(Duration::from_millis(at - unix_ms_now() + 300));
    for i in 0..n {
        assert_eq!(m.cmd(&["HINCRBY", &format!("incr:{i}"), "f", "1"]).i(), 1);
        assert_eq!(
            m.cmd(&["HINCRBYFLOAT", &format!("float:{i}"), "f", "1.5"])
                .s(),
            "1.5"
        );
        assert_eq!(
            m.cmd(&["HSETNX", &format!("nx:{i}"), "f", "new"]).i(),
            1,
            "nx:{i}: its f was past its time"
        );
        for (kind, cond) in [("xx", "XX"), ("gt", "GT")] {
            assert_eq!(
                m.cmd(&[
                    "HPEXPIRE",
                    &format!("{kind}:{i}"),
                    "60000",
                    cond,
                    "FIELDS",
                    "1",
                    "f"
                ]),
                V::Arr(vec![V::Int(-2)]),
                "{kind}:{i}: its f was past its time"
            );
        }
    }
    assert_eq!(m.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).s(), "OK");
    assert_eq!(
        state(&mut m, "nx:0"),
        r#"hash [("f", "new", -1), ("keep", "1", -1)]"#
    );
    assert_eq!(state(&mut m, "xx:0"), r#"hash [("keep", "1", -1)]"#);
    assert_same_keys(&mut m, &mut r, &kinds, n);
    // Still so a while later, the replica's own sweep having had its turn.
    std::thread::sleep(Duration::from_millis(500));
    assert_same_keys(&mut m, &mut r, &kinds, n);
}

/// A hash whose only field expires: the master removes the field and the
/// hash, and an `LPUSH` makes the key a list. The replica, stopped, then
/// applied the `LPUSH` to the hash it still had and refused it with
/// `WRONGTYPE` (99 of 100 in #415): the master had sent nothing of the
/// hash's going. Now it is sent its deletion, ahead of the `LPUSH`.
#[cfg(unix)]
#[test]
fn a_hash_emptied_by_its_last_field_expiring_reaches_a_lagging_replica_before_a_push() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);
    let n = 50;
    let at = unix_ms_now() + 800;
    for i in 0..n {
        let key = format!("only:{i}");
        assert_eq!(m.cmd(&["HSET", &key, "f", "5"]).i(), 1);
        assert_eq!(
            m.cmd(&["HPEXPIREAT", &key, &at.to_string(), "FIELDS", "1", "f"]),
            V::Arr(vec![V::Int(1)])
        );
    }
    wait_caught_up(&mut m, &mut r);
    replica.signal(libc::SIGSTOP);
    std::thread::sleep(Duration::from_millis(at - unix_ms_now() + 200));
    wait_until("the master's sweep to take the hashes", 30, || {
        m.cmd(&["DBSIZE"]).i() == 0
    });
    for i in 0..n {
        assert_eq!(m.cmd(&["LPUSH", &format!("only:{i}"), "x"]).i(), 1);
    }
    replica.signal(libc::SIGCONT);
    assert_same_keys(&mut m, &mut r, &["only"], n);
    for i in 0..n {
        assert_eq!(r.cmd(&["TYPE", &format!("only:{i}")]).s(), "list");
    }
}

/// Writes after field deadlines, logged with `appendfsync always` (which
/// KiviDB takes, and syncs each second: a killed process loses nothing it
/// wrote), the server killed (SIGKILL) and started again: the replay leaves
/// what the master held, key by key and field by field. The AOF held no `HDEL` or `DEL` for an expired
/// field, and each case diverged (20 of 20 in #415): an `HINCRBY`,
/// `HINCRBYFLOAT` or `HSETNX` after the field's time went into the old
/// field, which then went; an `HPERSIST` or `HPEXPIRE … XX` that found
/// nothing was logged all the same and brought the field back; and an
/// `LPUSH` to a key whose hash had gone with its last field met `WRONGTYPE`,
/// and the list was lost.
#[test]
fn writes_after_field_deadlines_replay_as_made_after_a_kill() {
    let mut server = start_with_conf(&["--aof"], "appendfsync always\n");
    let mut c = server.conn();
    // The hash whose only field goes, with the sweep, then the push.
    assert_eq!(c.cmd(&["HSET", "push", "f", "5"]).i(), 1);
    assert_eq!(
        c.cmd(&["HPEXPIRE", "push", "200", "FIELDS", "1", "f"]),
        V::Arr(vec![V::Int(1)])
    );
    wait_until("the sweep to take the hash", 20, || {
        c.cmd(&["EXISTS", "push"]).i() == 0
    });
    assert_eq!(c.cmd(&["LPUSH", "push", "x"]).i(), 1);
    // The others, each write finding its field itself.
    assert_eq!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).s(), "OK");
    let kinds = ["incr", "float", "nx", "persist", "xx"];
    let at = unix_ms_now() + 300;
    hashes_with_f_due_at(&mut c, &kinds, 1, at);
    std::thread::sleep(Duration::from_millis(at - unix_ms_now() + 300));
    // What each answers is the business of the test above; here, only
    // that the replay leaves what they left.
    c.cmd(&["HINCRBY", "incr:0", "f", "1"]);
    c.cmd(&["HINCRBYFLOAT", "float:0", "f", "1.5"]);
    c.cmd(&["HSETNX", "nx:0", "f", "new"]);
    c.cmd(&["HPERSIST", "persist:0", "FIELDS", "1", "f"]);
    c.cmd(&["HPEXPIRE", "xx:0", "60000", "XX", "FIELDS", "1", "f"]);
    assert_eq!(c.cmd(&["SET", "last", "1"]).s(), "OK");
    let keys: Vec<String> = kinds
        .iter()
        .map(|kind| format!("{kind}:0"))
        .chain(["push".to_string()])
        .collect();
    let before: Vec<String> = keys.iter().map(|key| state(&mut c, key)).collect();
    wait_until("the writes to reach the AOF", 10, || {
        count(&server.aof(), b"$4\r\nlast\r\n") == 1
    });
    server.kill();
    server.restart();
    let mut c = server.conn();
    // And after the fields the replay found past their time have gone.
    std::thread::sleep(Duration::from_millis(500));
    let after: Vec<String> = keys.iter().map(|key| state(&mut c, key)).collect();
    assert_eq!(after, before, "{keys:?}");
}

/// `HINCRBYFLOAT` goes to the AOF and to a replica as the value it left,
/// `HSETEX … KEEPTTL FIELDS 1 f <value>`, as in Redis 8.10.2: it was sent
/// as typed, and run again (an AOF replayed over a snapshot that already
/// had it) it added twice. The field keeps its time to live, on the
/// replica and after the AOF is replayed.
#[test]
fn hincrbyfloat_is_sent_as_the_value_it_left() {
    let master = start_with_conf(&["--aof"], "appendfsync always\n");
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);
    assert_eq!(m.cmd(&["HSET", "h", "f", "1", "g", "2"]).i(), 2);
    assert_eq!(
        m.cmd(&["HEXPIRE", "h", "1000", "FIELDS", "1", "f"]),
        V::Arr(vec![V::Int(1)])
    );
    assert_eq!(m.cmd(&["HINCRBYFLOAT", "h", "f", "0.5"]).s(), "1.5");
    assert_eq!(m.cmd(&["HINCRBYFLOAT", "h", "g", "0.25"]).s(), "2.25");
    assert_eq!(m.cmd(&["HINCRBYFLOAT", "new", "n", "3"]).s(), "3");
    assert_eq!(m.cmd(&["SET", "last", "1"]).s(), "OK");
    let keys = ["h", "new"];
    let before: Vec<String> = keys.iter().map(|key| state(&mut m, key)).collect();
    assert_eq!(before[0], r#"hash [("f", "1.5", 1), ("g", "2.25", -1)]"#);
    wait_caught_up(&mut m, &mut r);
    for key in keys {
        assert_eq!(
            state(&mut r, key),
            state(&mut m, key),
            "{key} on the replica"
        );
    }
    wait_until("the writes to reach the AOF", 10, || {
        count(&master.aof(), b"$4\r\nlast\r\n") == 1
    });
    let aof = master.aof();
    assert_eq!(
        count(&aof, b"HINCRBYFLOAT"),
        0,
        "{}",
        String::from_utf8_lossy(&aof)
    );
    for (key, field, value) in [("h", "f", "1.5"), ("h", "g", "2.25"), ("new", "n", "3")] {
        let sent = format!(
            "*7\r\n$6\r\nHSETEX\r\n${}\r\n{key}\r\n$7\r\nKEEPTTL\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$1\r\n{field}\r\n${}\r\n{value}\r\n",
            key.len(),
            value.len()
        );
        assert_eq!(count(&aof, sent.as_bytes()), 1, "{sent:?} in the AOF");
    }
    let mut master = master;
    master.kill();
    master.restart();
    let mut c = master.conn();
    let after: Vec<String> = keys.iter().map(|key| state(&mut c, key)).collect();
    assert_eq!(after, before);
}

// ── SORT BY / GET of hash fields (#415 review) ───────────────────────────────

/// `ids` = [1, 2, 3] and `obj:{id}` = { w } with w 30, 40, 20; `obj:2`'s `w`
/// due at `at`. Sorted by `w` while it lives: 3, 1, 2. Without it: 2 (no
/// weight sorts as 0), 3, 1.
fn sort_fixture(c: &mut Conn, at: u64) {
    assert_eq!(c.cmd(&["RPUSH", "ids", "1", "2", "3"]).i(), 3);
    for (id, w) in [("1", "30"), ("2", "40"), ("3", "20")] {
        assert_eq!(
            c.cmd(&["HSET", &format!("obj:{id}"), "w", w, "keep", "1"])
                .i(),
            2
        );
    }
    assert_eq!(
        c.cmd(&["HPEXPIREAT", "obj:2", &at.to_string(), "FIELDS", "1", "w"]),
        V::Arr(vec![V::Int(1)])
    );
}

/// `SORT ids BY obj:*->w GET # GET obj:*->w STORE dst`: the number stored.
fn sort_store(c: &mut Conn, dst: &str) -> i64 {
    c.cmd(&[
        "SORT", "ids", "BY", "obj:*->w", "GET", "#", "GET", "obj:*->w", "STORE", dst,
    ])
    .i()
}

/// A `SORT … STORE` whose `BY` and `GET` read a field the master's sort
/// found alive, applied by a replica that lags past the field's deadline.
/// The `SORT` goes to the replica as it was run, and the replica, which
/// hid the field by its own clock, stored another list than the master's.
/// Then one run after the deadline, the master's sweep held off: the master
/// takes the field as gone, sends its `HDEL` ahead of the `SORT`, and both
/// sort without it.
#[cfg(unix)]
#[test]
fn a_sort_store_by_a_hash_field_reaches_a_lagging_replica_as_sorted() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);
    let at = unix_ms_now() + 1_000;
    sort_fixture(&mut m, at);
    wait_caught_up(&mut m, &mut r);

    replica.signal(libc::SIGSTOP);
    assert_eq!(sort_store(&mut m, "before"), 6);
    assert!(unix_ms_now() < at, "the sort came after the deadline");
    let sorted = m.cmd(&["LRANGE", "before", "0", "-1"]);
    assert_eq!(
        format!("{sorted:?}"),
        format!(
            "{:?}",
            V::Arr(
                ["3", "20", "1", "30", "2", "40"]
                    .iter()
                    .map(|s| V::Str(s.to_string()))
                    .collect()
            )
        )
    );
    std::thread::sleep(Duration::from_millis(at - unix_ms_now() + 300));
    replica.signal(libc::SIGCONT);
    wait_caught_up(&mut m, &mut r);
    assert_eq!(r.cmd(&["LRANGE", "before", "0", "-1"]), sorted);

    // After the deadline, the field not yet swept on the master.
    assert_eq!(m.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).s(), "OK");
    assert_eq!(m.cmd(&["HSET", "obj:1", "w", "35"]).i(), 0);
    assert_eq!(
        m.cmd(&["HPEXPIRE", "obj:1", "200", "FIELDS", "1", "w"]),
        V::Arr(vec![V::Int(1)])
    );
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(sort_store(&mut m, "after"), 6);
    assert_eq!(m.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).s(), "OK");
    let after = m.cmd(&["LRANGE", "after", "0", "-1"]);
    assert_eq!(
        after.arr()[0],
        V::Str("1".into()),
        "obj:1's w was past its time"
    );
    wait_caught_up(&mut m, &mut r);
    assert_eq!(r.cmd(&["LRANGE", "after", "0", "-1"]), after);
    for key in ["obj:1", "obj:2", "obj:3"] {
        assert_eq!(state(&mut r, key), state(&mut m, key), "{key}");
    }
}

/// The same `SORT … STORE`s, logged to the AOF, the server killed and
/// started again: the replay stores what was stored. It hid the field by
/// the clock of the replay, after the deadline, and stored another list.
#[test]
fn a_sort_store_by_a_hash_field_replays_as_sorted_after_a_kill() {
    let mut server = start_with_conf(&["--aof"], "appendfsync always\n");
    let mut c = server.conn();
    let at = unix_ms_now() + 600;
    sort_fixture(&mut c, at);
    assert_eq!(sort_store(&mut c, "before"), 6);
    assert!(unix_ms_now() < at, "the sort came after the deadline");

    std::thread::sleep(Duration::from_millis(at - unix_ms_now() + 100));
    assert_eq!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).s(), "OK");
    assert_eq!(c.cmd(&["HSET", "obj:1", "w", "35"]).i(), 0);
    assert_eq!(
        c.cmd(&["HPEXPIRE", "obj:1", "200", "FIELDS", "1", "w"]),
        V::Arr(vec![V::Int(1)])
    );
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(sort_store(&mut c, "after"), 6);
    assert_eq!(c.cmd(&["SET", "last", "1"]).s(), "OK");

    let keys = ["before", "after", "obj:1", "obj:2", "obj:3"];
    let before: Vec<String> = keys.iter().map(|key| state(&mut c, key)).collect();
    assert!(before[0].contains(r#""40""#), "{}", before[0]);
    wait_until("the writes to reach the AOF", 10, || {
        count(&server.aof(), b"$4\r\nlast\r\n") == 1
    });
    server.kill();
    server.restart();
    let mut c = server.conn();
    std::thread::sleep(Duration::from_millis(500));
    let after: Vec<String> = keys.iter().map(|key| state(&mut c, key)).collect();
    assert_eq!(after, before, "{keys:?}");
}
