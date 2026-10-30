// tests/expiry_propagation.rs
//
// A key's expiry, as the readers of the write stream see it: the AOF replayed
// at a restart, and a replica. Real `kividb` processes, because a restart and
// a replica that stops are the points of interest.
//
// The master logs and propagates a `DEL` (`UNLINK` under
// `lazyfree-lazy-expire yes`, the default) for every key it expires, and
// nothing expires while the AOF is replayed or the master's stream applied.
// Before, each reader expired keys by its own clock: an `INCR` made while its
// key lived, replayed after the key's time, made the key again, `1` with no
// time to live, for good; and a replica expiring keys under its master's
// writes to them diverged from it.
//
// The I/O model is the platform's own unless `KIVIDB_TEST_IO_MODEL` says.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

fn kividb(dir: &Path, port: u16, extra: &[String]) -> Child {
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
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let child = kividb(&dir, port, &args);
        let server = Server {
            port,
            dir,
            child,
            args,
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

    /// Killed, with no chance to save or to expire anything more.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Started again on the same port and directory. Under io_uring a
    /// connection made the moment it comes up can be reset (master does the
    /// same): it is tried again.
    fn restart(&mut self) {
        self.child = kividb(&self.dir, self.port, &self.args);
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
        assert_eq!(unsafe { libc::kill(self.child.id() as libc::pid_t, signal) }, 0);
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

/// How many times `stream` holds `DEL key` or `UNLINK key`, as RESP.
fn deletions(stream: &[u8], key: &str) -> usize {
    ["DEL", "UNLINK"]
        .iter()
        .map(|cmd| {
            let record = format!(
                "*2\r\n${}\r\n{cmd}\r\n${}\r\n{key}\r\n",
                cmd.len(),
                key.len()
            );
            count(stream, record.as_bytes())
        })
        .sum()
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

/// Every key, with what it holds and whether it has a time to live: the
/// dataset, compared key by key.
fn dataset(c: &mut Conn) -> Vec<(String, String, bool)> {
    let mut keys: Vec<String> = c
        .cmd(&["KEYS", "*"])
        .arr()
        .iter()
        .map(|k| k.s().to_string())
        .collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|key| {
            let value = match c.cmd(&["TYPE", &key]).s() {
                "string" => match c.cmd(&["GET", &key]) {
                    V::Str(s) => s,
                    _ => return None,
                },
                "list" => format!("{:?}", c.cmd(&["LRANGE", &key, "0", "-1"])),
                "none" => return None,
                other => other.to_string(),
            };
            let ttl = c.cmd(&["PTTL", &key]).i();
            (ttl != -2).then_some((key, value, ttl >= 0))
        })
        .collect()
}

/// No key left with a time to live: everything that was to expire has, on
/// the master, and its `DEL` is in the stream.
fn wait_all_expired(master: &mut Conn) {
    wait_until("every key with a time to live to expire", 60, || {
        dataset(master).iter().all(|(_, _, ttl)| !ttl)
    });
}

fn assert_same(master: &mut Conn, replica: &mut Conn) {
    wait_caught_up(master, replica);
    let (m, r) = (dataset(master), dataset(replica));
    let differ: Vec<_> = m
        .iter()
        .filter(|kv| !r.contains(kv))
        .chain(r.iter().filter(|kv| !m.contains(kv)))
        .take(10)
        .collect();
    assert!(
        differ.is_empty(),
        "master ({} keys) and replica ({} keys) differ, e.g. {differ:?}",
        m.len(),
        r.len()
    );
    assert_eq!(master.cmd(&["DBSIZE"]), replica.cmd(&["DBSIZE"]));
}

/// Writes to keys that expire as they are written to, from `clients`
/// connections at once, until `stop`. Each round of a client:
///
/// * `SET p:<c>:<n> 100 PX <ttl>`, then, about when it expires, `INCR` of
///   it. `101`: the key lived, and goes at its time. `1`: it had expired,
///   and the write made a new key, with no time to live, which must stay.
///   Such keys are returned: an acknowledged write that must not be lost.
/// * writes to a small pool of keys shared with the other clients, some
///   with short times to live: `INCR`, `RPUSH`, `SET ... PX`, `PEXPIRE`.
fn expiring_writes(port: u16, clients: usize, stop: Arc<AtomicBool>) -> Vec<(String, String)> {
    let made = Arc::new(AtomicU64::new(0));
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let (stop, made) = (Arc::clone(&stop), Arc::clone(&made));
            std::thread::spawn(move || {
                let mut conn = Conn::connect(port);
                let mut must_stay = Vec::new();
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    n += 1;
                    let ttl = 2 + (n * 7 + c as u64) % 9;
                    let probe = format!("p:{c}:{n}");
                    assert_eq!(
                        conn.cmd(&["SET", &probe, "100", "PX", &ttl.to_string()]).s(),
                        "OK"
                    );
                    std::thread::sleep(Duration::from_micros(ttl * 1000 - 500 + (n % 3) * 500));
                    match conn.cmd(&["INCR", &probe]).i() {
                        1 => must_stay.push((probe, "1".to_string())),
                        101 => {}
                        other => panic!("INCR {probe} said {other}"),
                    }
                    let shared = format!("s:{}", (n * 13 + c as u64) % 50);
                    let list = format!("l:{}", (n * 17 + c as u64) % 50);
                    let _ = conn.cmd(&["INCR", &shared]);
                    let _ = conn.cmd(&["RPUSH", &list, &format!("{c}-{n}")]);
                    match n % 4 {
                        0 => {
                            let _ = conn.cmd(&["PEXPIRE", &shared, &ttl.to_string()]);
                        }
                        1 => {
                            let _ = conn.cmd(&["PEXPIRE", &list, &ttl.to_string()]);
                        }
                        2 => {
                            let _ = conn.cmd(&["SET", &shared, "7", "PX", &ttl.to_string()]);
                        }
                        _ => {}
                    }
                    made.fetch_add(1, Ordering::Relaxed);
                }
                must_stay
            })
        })
        .collect();
    let must_stay: Vec<(String, String)> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    assert!(made.load(Ordering::Relaxed) > 0);
    must_stay
}

/// Each `(key, value)` is there, as it is, with no time to live: none of
/// those acknowledged writes was lost.
fn assert_kept(c: &mut Conn, who: &str, must_stay: &[(String, String)]) {
    let lost: Vec<_> = must_stay
        .iter()
        .filter(|(key, value)| {
            c.cmd(&["GET", key]) != V::Str(value.clone()) || c.cmd(&["PTTL", key]) != V::Int(-1)
        })
        .take(10)
        .collect();
    assert!(
        lost.is_empty(),
        "{who}: acknowledged writes lost ({} kept), e.g. {lost:?}",
        must_stay.len()
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// `SET k 5 PX t`, `INCR k` while it lives, and the process killed before
/// `t`: no `DEL` in the log. Restarted after `t`, the replay applied the
/// INCR to a key past its time, which it took as missing: the key came back
/// as `1`, with no time to live, for good. 100 keys of 100.
#[test]
fn a_restart_after_the_deadline_does_not_bring_a_key_back() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    for i in 0..100 {
        let key = format!("k:{i}");
        assert_eq!(c.cmd(&["SET", &key, "5", "PX", "1500"]).s(), "OK");
        assert_eq!(c.cmd(&["INCR", &key]).i(), 6);
        let live = format!("live:{i}");
        assert_eq!(c.cmd(&["SET", &live, "5", "PX", "3600000"]).s(), "OK");
        assert_eq!(c.cmd(&["INCR", &live]).i(), 6);
    }
    // Into the file (not only the page cache: it is the process that dies),
    // and killed before any key's time.
    wait_until("the writes to reach the AOF", 10, || {
        deletions(&server.aof(), "k:99") == 0 && count(&server.aof(), b"$4\r\nINCR\r\n") >= 200
    });
    server.kill();
    assert_eq!(count(&server.aof(), b"UNLINK"), 0, "nothing expired yet");
    std::thread::sleep(Duration::from_millis(1700));

    server.restart();
    let mut c = server.conn();
    for i in 0..100 {
        let key = format!("k:{i}");
        assert_eq!(c.cmd(&["GET", &key]), V::Nil, "{key} came back");
        assert_eq!(c.cmd(&["EXISTS", &key]).i(), 0);
        let live = format!("live:{i}");
        assert_eq!(c.cmd(&["GET", &live]).s(), "6", "{live}");
        assert!(c.cmd(&["PTTL", &live]).i() > 0, "{live} kept its time to live");
    }
    // Taken out once the server is up, each with its `DEL` in the log: a
    // second restart finds them gone by the log itself.
    wait_until("the expired keys' DELs to reach the AOF", 20, || {
        let aof = server.aof();
        (0..100).all(|i| deletions(&aof, &format!("k:{i}")) == 1)
    });
    server.kill();
    server.restart();
    let mut c = server.conn();
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 100, "only live:* is left");
    for i in 0..100 {
        assert_eq!(c.cmd(&["GET", &format!("k:{i}")]), V::Nil);
    }
}

/// The same with the server up past the deadline: active expiry takes the
/// keys out and logs their `DEL`s, and a restart replays them.
#[test]
fn a_key_that_expires_while_the_server_runs_stays_gone_after_a_restart() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    for i in 0..100 {
        let key = format!("k:{i}");
        assert_eq!(c.cmd(&["SET", &key, "5", "PX", "200"]).s(), "OK");
        assert_eq!(c.cmd(&["INCR", &key]).i(), 6);
    }
    wait_until("every key to expire and its DEL to be logged", 20, || {
        let aof = server.aof();
        (0..100).all(|i| deletions(&aof, &format!("k:{i}")) == 1)
    });
    server.kill();
    server.restart();
    let mut c = server.conn();
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 0);
    for i in 0..100 {
        assert_eq!(c.cmd(&["GET", &format!("k:{i}")]), V::Nil);
    }
}

/// Every key the master expires is logged as deleted, once, whichever way
/// it went: active expiry, a read that found it past its time, or a write
/// that put a new value over it, whose `DEL` comes before the write's own
/// record.
#[test]
fn each_expired_key_is_deleted_once_in_the_aof() {
    let server = Server::start(&["--aof"]);
    let mut c = server.conn();
    assert_eq!(c.cmd(&["CONFIG", "GET", "lazyfree-lazy-expire"]).arr()[1].s(), "yes");
    // Read, or swept, whichever comes first, some each way.
    for i in 0..2000 {
        assert_eq!(c.cmd(&["SET", &format!("r:{i}"), "v", "PX", "30"]).s(), "OK");
    }
    // Written over, after their time.
    for i in 0..200 {
        assert_eq!(c.cmd(&["SET", &format!("w:{i}"), "5", "PX", "30"]).s(), "OK");
    }
    std::thread::sleep(Duration::from_millis(40));
    for i in 0..2000 {
        assert_eq!(c.cmd(&["GET", &format!("r:{i}")]), V::Nil);
    }
    let mut written_over = 0;
    for i in 0..200 {
        // `1`: the key had expired (by a read just before, or the sweep, or
        // the write itself), and this is a new one.
        assert_eq!(c.cmd(&["INCR", &format!("w:{i}")]).i(), 1);
        written_over += 1;
    }
    let expired: u64 = c.info("stats", "expired_keys").parse().unwrap();
    let active: u64 = c.info("stats", "expired_keys_active").parse().unwrap();
    assert_eq!(expired, 2200, "each key counted once");
    eprintln!("{active} of 2200 expired by the sweep, the rest by a read or a write");

    wait_until("every DEL to be logged", 20, || {
        let aof = server.aof();
        count(&aof, b"$6\r\nUNLINK\r\n") >= 2200 && count(&aof, b"$4\r\nINCR\r\n") >= written_over
    });
    let aof = server.aof();
    for i in 0..2000 {
        assert_eq!(deletions(&aof, &format!("r:{i}")), 1, "r:{i}");
    }
    for i in 0..200 {
        let key = format!("w:{i}");
        assert_eq!(deletions(&aof, &key), 1, "{key}");
        let del = format!("$6\r\nUNLINK\r\n${}\r\n{key}\r\n", key.len());
        let incr = format!("$4\r\nINCR\r\n${}\r\n{key}\r\n", key.len());
        assert!(
            position(&aof, del.as_bytes()) < position(&aof, incr.as_bytes()),
            "{key}: its UNLINK is logged before the INCR that wrote over it"
        );
    }
    // With `lazyfree-lazy-expire no`, `DEL`.
    assert_eq!(c.cmd(&["CONFIG", "SET", "lazyfree-lazy-expire", "no"]).s(), "OK");
    assert_eq!(c.cmd(&["SET", "d", "v", "PX", "20"]).s(), "OK");
    wait_until("the DEL of d", 10, || {
        count(&server.aof(), b"*2\r\n$3\r\nDEL\r\n$1\r\nd\r\n") == 1
    });
}

/// What the master expires reaches its replica as a `DEL` (`UNLINK`), which
/// the replica applies, and logs in its own AOF. The replica deletes no key
/// itself: past its time, a key is not there to the replica's clients, and
/// stays in its table until the master says.
#[test]
fn expired_keys_reach_the_replica_as_deletions() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port, "--aof"]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);
    for i in 0..100 {
        assert_eq!(m.cmd(&["SET", &format!("a:{i}"), "v", "PX", "100"]).s(), "OK");
        assert_eq!(m.cmd(&["SET", &format!("g:{i}"), "v", "PX", "20"]).s(), "OK");
    }
    std::thread::sleep(Duration::from_millis(30));
    // Lazily, on the master: read past their time.
    for i in 0..100 {
        assert_eq!(m.cmd(&["GET", &format!("g:{i}")]), V::Nil);
    }
    wait_until("the replica to apply every deletion", 20, || {
        let aof = replica.aof();
        (0..100).all(|i| {
            deletions(&aof, &format!("a:{i}")) == 1 && deletions(&aof, &format!("g:{i}")) == 1
        })
    });
    wait_caught_up(&mut m, &mut r);
    assert_eq!(r.cmd(&["DBSIZE"]).i(), 0);
    assert_eq!(r.info("stats", "expired_keys"), "0", "the replica expired nothing itself");

    // A key past its time on a replica whose master has not deleted it yet:
    // stopped, the master cannot. Here, a master that is paused.
    assert_eq!(m.cmd(&["SET", "held", "v", "PX", "300"]).s(), "OK");
    wait_caught_up(&mut m, &mut r);
    assert_eq!(m.cmd(&["CLIENT", "PAUSE", "3000", "WRITE"]).s(), "OK");
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(r.cmd(&["GET", "held"]), V::Nil, "past its time: not there to clients");
    assert_eq!(r.cmd(&["EXISTS", "held"]).i(), 0);
    assert_eq!(r.cmd(&["DBSIZE"]).i(), 1, "but still in the replica's table");
    assert_eq!(m.cmd(&["CLIENT", "UNPAUSE"]).s(), "OK");
    wait_until("the master's DEL of held", 20, || r.cmd(&["DBSIZE"]).i() == 0);
}

/// Writes from many clients to keys that expire as they are written: the
/// replica holds what the master holds, key by key, and no acknowledged
/// write is lost on either. The replica used to expire keys by its own
/// clock while its master's writes to them were on their way (572 keys
/// differed after 15 s).
#[test]
fn a_replica_converges_with_keys_expiring_under_writes() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);

    let stop = Arc::new(AtomicBool::new(false));
    let writers = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || expiring_writes(master.port, 32, stop))
    };
    std::thread::sleep(Duration::from_secs(10));
    stop.store(true, Ordering::Relaxed);
    let must_stay = writers.join().unwrap();
    assert!(!must_stay.is_empty(), "some writes found their key expired");

    wait_all_expired(&mut m);
    assert_same(&mut m, &mut r);
    assert_kept(&mut m, "master", &must_stay);
    assert_kept(&mut r, "replica", &must_stay);
    drop(master);
}

/// The same with a replica that stops (SIGSTOP) for a while under the
/// writes, its link still open, while the keys it holds expire: when it
/// goes on, it applies the writes made to them while they lived, then their
/// `DEL`s. Expiring them itself first, it turned those writes into new keys
/// (170 of 200).
#[cfg(unix)]
#[test]
fn a_replica_that_lags_converges_with_keys_expiring_meanwhile() {
    let master = Server::start(&[]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_link_up(&mut r);

    // Keys the replica has, then writes to them while they live, made while
    // the replica is stopped, and their time passing before it goes on.
    for i in 0..200 {
        assert_eq!(m.cmd(&["SET", &format!("c:{i}"), "5", "PX", "1500"]).s(), "OK");
        assert_eq!(m.cmd(&["RPUSH", &format!("q:{i}"), "a"]).i(), 1);
        assert_eq!(m.cmd(&["PEXPIRE", &format!("q:{i}"), "1500"]).i(), 1);
    }
    wait_caught_up(&mut m, &mut r);
    replica.signal(libc::SIGSTOP);

    let stop = Arc::new(AtomicBool::new(false));
    let writers = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || expiring_writes(master.port, 16, stop))
    };
    for i in 0..200 {
        assert_eq!(m.cmd(&["INCR", &format!("c:{i}")]).i(), 6);
        assert_eq!(m.cmd(&["RPUSH", &format!("q:{i}"), "b"]).i(), 2);
    }
    std::thread::sleep(Duration::from_millis(3000));
    replica.signal(libc::SIGCONT);
    std::thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Relaxed);
    let must_stay = writers.join().unwrap();

    wait_all_expired(&mut m);
    assert_same(&mut m, &mut r);
    for i in 0..200 {
        assert_eq!(r.cmd(&["EXISTS", &format!("c:{i}"), &format!("q:{i}")]).i(), 0);
    }
    assert_kept(&mut m, "master", &must_stay);
    assert_kept(&mut r, "replica", &must_stay);
    drop(master);
}
