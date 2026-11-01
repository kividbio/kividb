// A key that goes for its time, as the AOF and replicas see it.
//
// Only the master deletes a key for its time, and it sends the deletion on
// (`UNLINK`, or `DEL` with `lazyfree-lazy-expire no`) before any write that
// needs the key gone. A replay of the AOF, and a replica, apply that stream
// and never decide by their own clock that a key is gone: `SET k 5 PXAT t`
// and an `INCR k` the master ran while `k` lived, replayed after `t`, used
// to come back as a `k` of 1 with no time to live that the master never had.
//
// Each test runs its own servers, as processes: an AOF is replayed by a
// restart, and a replica is held back by stopping its process.
//
// The I/O model is the platform's own unless `KIVIDB_TEST_IO_MODEL` says.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
enum V {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<V>),
    Nil,
}

impl V {
    fn int(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }

    fn text(&self) -> Option<String> {
        match self {
            V::Bulk(b) => Some(String::from_utf8_lossy(b).into_owned()),
            V::Simple(s) => Some(s.clone()),
            V::Nil => None,
            other => panic!("not a string: {other:?}"),
        }
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(20);
        let stream = loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                break stream;
            }
            assert!(Instant::now() < deadline, "no server on {port}");
            thread::sleep(Duration::from_millis(20));
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn encode(args: &[&str]) -> Vec<u8> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        buf
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.stream.write_all(&Self::encode(args)).unwrap();
        self.read()
    }

    /// Several commands in one write, and their replies.
    fn pipeline(&mut self, commands: &[Vec<&str>]) -> Vec<V> {
        let mut buf = Vec::new();
        for args in commands {
            buf.extend_from_slice(&Self::encode(args));
        }
        self.stream.write_all(&buf).unwrap();
        commands.iter().map(|_| self.read()).collect()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => V::Simple(line[1..].to_string()),
            Some(b'-') => V::Error(line[1..].to_string()),
            Some(b':') => V::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count < 0 {
                    return V::Nil;
                }
                V::Array((0..count).map(|_| self.read()).collect())
            }
            _ => panic!("unexpected RESP: {line:?}"),
        }
    }

    /// The next message within `wait`, if one comes.
    fn read_within(&mut self, wait: Duration) -> Option<V> {
        self.stream.set_read_timeout(Some(wait)).unwrap();
        let got = matches!(self.reader.fill_buf(), Ok(buf) if !buf.is_empty());
        self.stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        got.then(|| self.read())
    }

    fn ok(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args), V::Simple("OK".into()), "{args:?}");
    }

    fn get(&mut self, key: &str) -> Option<String> {
        self.cmd(&["GET", key]).text()
    }

    fn info(&mut self, field: &str) -> i64 {
        let info = self.cmd(&["INFO", "all"]).text().unwrap();
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO"))
            .trim()
            .parse()
            .unwrap()
    }
}

fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
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

/// A server process in a directory of its own, which outlives it, for a
/// restart to replay.
struct Server {
    dir: PathBuf,
    port: u16,
    extra: Vec<String>,
    child: Option<Child>,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let mut server = Server {
            dir,
            port: free_port(),
            extra: extra.iter().map(|s| s.to_string()).collect(),
            child: None,
        };
        server.run();
        server
    }

    fn with_aof() -> Self {
        Self::start(&["--aof"])
    }

    fn replica_of(master: &Server) -> Self {
        let port = master.port.to_string();
        let replica = Self::start(&["--replicaof", "127.0.0.1", &port]);
        let mut c = replica.conn();
        wait_for("the replica's link", || {
            c.cmd(&["INFO", "replication"])
                .text()
                .unwrap()
                .contains("master_link_status:up")
        });
        replica
    }

    fn run(&mut self) {
        let io_model = std::env::var("KIVIDB_TEST_IO_MODEL").ok();
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&self.dir)
            .args([
                "--port",
                &self.port.to_string(),
                "--bind",
                "127.0.0.1",
                "--threads",
                "4",
                "--configfile",
                "kividb.conf",
            ])
            .args(io_model.iter().flat_map(|m| ["--io-model", m.as_str()]))
            .args(&self.extra)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(self.dir.join("server.log")).unwrap())
            .spawn()
            .expect("could not start kividb");
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", self.port)).is_err() {
            let exited = self.child.as_mut().unwrap().try_wait().unwrap();
            if exited.is_some() || Instant::now() > deadline {
                let log = std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default();
                panic!(
                    "the server on {} did not start ({exited:?}):\n{log}",
                    self.port
                );
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(self.conn().cmd(&["PING"]), V::Simple("PONG".into()));
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// Killed, as a crash: what is in the AOF is what is left.
    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// On a port of its own: the old one may be another test's by now.
    fn restart(&mut self) {
        self.kill();
        self.port = free_port();
        self.run();
    }

    fn signal(&self, signal: &str) -> bool {
        let pid = self.child.as_ref().unwrap().id().to_string();
        Command::new("kill")
            .args([signal, &pid])
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    }

    fn aof(&self) -> Vec<Vec<String>> {
        records(&std::fs::read(self.dir.join("appendonly.aof")).unwrap_or_default())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.child.is_some() {
            self.signal("-CONT");
        }
        self.kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The commands of an AOF, in order. Whatever is not a command (a header)
/// is passed over.
fn records(buf: &[u8]) -> Vec<Vec<String>> {
    fn line(buf: &[u8], at: &mut usize) -> Option<String> {
        let end = buf[*at..].windows(2).position(|w| w == b"\r\n")? + *at;
        let text = String::from_utf8_lossy(&buf[*at..end]).into_owned();
        *at = end + 2;
        Some(text)
    }
    let mut out = Vec::new();
    let mut at = 0;
    while at < buf.len() {
        if buf[at] != b'*' {
            at += 1;
            continue;
        }
        let start = at;
        let parsed = (|| {
            let n: usize = line(buf, &mut at)?[1..].parse().ok()?;
            let mut parts = Vec::with_capacity(n);
            for _ in 0..n {
                let len: usize = line(buf, &mut at)?.strip_prefix('$')?.parse().ok()?;
                let end = at.checked_add(len)?;
                if end + 2 > buf.len() {
                    return None;
                }
                parts.push(String::from_utf8_lossy(&buf[at..end]).into_owned());
                at = end + 2;
            }
            Some(parts)
        })();
        match parsed {
            Some(parts) => out.push(parts),
            None => at = start + 1,
        }
    }
    out
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn until(ms: u64) {
    while now_ms() <= ms {
        thread::sleep(Duration::from_millis(5));
    }
}

/// Where in the AOF each record of `command` on `key` is.
fn positions(aof: &[Vec<String>], command: &str, key: &str) -> Vec<usize> {
    aof.iter()
        .enumerate()
        .filter(|(_, r)| r.len() >= 2 && r[0].eq_ignore_ascii_case(command) && r[1] == key)
        .map(|(i, _)| i)
        .collect()
}

/// Wait until the AOF holds `n` records of `command`.
fn aof_holds(server: &Server, command: &str, n: usize) {
    wait_for(&format!("{n} {command} in the AOF"), || {
        server
            .aof()
            .iter()
            .filter(|r| r[0].eq_ignore_ascii_case(command))
            .count()
            >= n
    });
}

/// Writes are held, and with them the sweep, until `then` is sent: the keys
/// past their time when it is meet the commands of `then`, which are sent in
/// the same write as the `CLIENT UNPAUSE`, before the sweep can get to them.
fn after_a_pause(c: &mut Conn, deadline: u64, then: &[Vec<&str>]) -> Vec<V> {
    c.ok(&["CLIENT", "PAUSE", "30000", "WRITE"]);
    until(deadline + 20);
    let mut commands = vec![vec!["CLIENT", "UNPAUSE"]];
    commands.extend(then.iter().cloned());
    let mut replies = c.pipeline(&commands);
    assert_eq!(replies.remove(0), V::Simple("OK".into()));
    replies
}

/// The replica has applied everything its master sent before this.
fn caught_up(master: &mut Conn, replica: &mut Conn) {
    let mark = format!("{}", now_ms());
    master.ok(&["SET", "caught-up", &mark]);
    wait_for("the replica to catch up", || {
        replica.get("caught-up").as_deref() == Some(mark.as_str())
    });
}

/// Every key, its value and when it expires.
fn dataset(c: &mut Conn) -> Vec<(String, Option<String>, i64)> {
    let V::Array(keys) = c.cmd(&["KEYS", "*"]) else {
        panic!("KEYS");
    };
    let mut out: Vec<_> = keys
        .iter()
        .map(|k| k.text().unwrap())
        .filter(|k| k != "caught-up")
        .map(|k| {
            let value = match c.cmd(&["DUMP", &k]) {
                V::Bulk(b) => Some(b.iter().map(|x| format!("{x:02x}")).collect()),
                _ => None,
            };
            let at = c.cmd(&["PEXPIRETIME", &k]).int();
            (k, value, at)
        })
        .collect();
    out.sort();
    out
}

// ── The AOF ──────────────────────────────────────────────────────────────

/// The maintainer's case: `SET k 5 PXAT t`, `INCR k` while `k` lives, and
/// the process gone before `t`. Replayed after `t`, the `INCR` is applied as
/// the master ran it: `k` is 6 with the time `t`, past, so absent. It came
/// back as a `k` of 1 with no time to live, for every key. The restarted
/// master then deletes them, and sends each deletion once.
#[test]
fn a_replay_after_the_deadline_has_what_the_master_had() {
    let mut s = Server::with_aof();
    let mut c = s.conn();
    let t = now_ms() + 1_500;
    let at = t.to_string();
    let keys: Vec<String> = (0..100).map(|i| format!("k:{i}")).collect();
    for key in &keys {
        c.ok(&["SET", key, "5", "PXAT", &at]);
        assert_eq!(c.cmd(&["INCR", key]).int(), 6);
    }
    aof_holds(&s, "INCR", keys.len());
    assert!(now_ms() < t, "the INCRs ran while the keys lived");
    s.kill();
    until(t + 50);

    s.run();
    let mut c = s.conn();
    for key in &keys {
        assert_eq!(c.get(key), None, "{key} is past its time");
    }
    // Deleted by the restarted master, as it would have been by the old.
    wait_for("the keys to go", || c.cmd(&["DBSIZE"]).int() == 0);
    aof_holds(&s, "UNLINK", keys.len());
    let aof = s.aof();
    for key in &keys {
        assert_eq!(
            positions(&aof, "SET", key).len(),
            1,
            "{key}: replayed, not logged again"
        );
        assert_eq!(positions(&aof, "INCR", key).len(), 1, "{key}");
        assert_eq!(
            positions(&aof, "UNLINK", key).len(),
            1,
            "{key}: one deletion"
        );
    }

    // And replayed again, with the deletions: still nothing.
    s.restart();
    let mut c = s.conn();
    assert_eq!(c.cmd(&["DBSIZE"]).int(), 0);
    assert_eq!(
        s.aof().iter().filter(|r| r[0] == "UNLINK").count(),
        keys.len(),
        "no deletion sent twice"
    );
}

/// The sweep deletes a key past its time on a running master: the deletion
/// is in the AOF after the key's writes, once, and a replay has no key.
#[test]
fn the_sweep_logs_each_deletion_once_and_a_replay_follows_it() {
    let mut s = Server::with_aof();
    let mut c = s.conn();
    let t = now_ms() + 300;
    let at = t.to_string();
    let keys: Vec<String> = (0..100).map(|i| format!("swept:{i}")).collect();
    let active_before = c.info("expired_keys_active");
    for key in &keys {
        c.ok(&["SET", key, "5", "PXAT", &at]);
        assert_eq!(c.cmd(&["INCR", key]).int(), 6);
    }
    wait_for("the sweep", || c.cmd(&["DBSIZE"]).int() == 0);
    assert_eq!(
        c.info("expired_keys_active") - active_before,
        keys.len() as i64
    );
    aof_holds(&s, "UNLINK", keys.len());
    let aof = s.aof();
    for key in &keys {
        let incr = positions(&aof, "INCR", key);
        let unlink = positions(&aof, "UNLINK", key);
        assert_eq!(unlink.len(), 1, "{key}");
        assert!(incr[0] < unlink[0], "{key}: deleted after its write");
    }
    s.restart();
    let mut c = s.conn();
    assert_eq!(c.cmd(&["DBSIZE"]).int(), 0);
}

/// A write that finds its key past its time: the deletion goes into the AOF
/// before the write, once, and a replay applies them in that order, to the
/// master's values and times to live.
#[test]
fn a_deletion_is_logged_before_the_write_that_needs_the_key_gone() {
    let mut s = Server::with_aof();
    let mut c = s.conn();
    let t = now_ms() + 200;
    let at = t.to_string();
    let counters: Vec<String> = (0..50).map(|i| format!("counter:{i}")).collect();
    let lists: Vec<String> = (0..50).map(|i| format!("list:{i}")).collect();
    for (counter, list) in counters.iter().zip(&lists) {
        c.ok(&["SET", counter, "5", "PXAT", &at]);
        assert_eq!(c.cmd(&["RPUSH", list, "old"]).int(), 1);
        assert_eq!(c.cmd(&["PEXPIREAT", list, &at]).int(), 1);
    }
    let lazy_before = c.info("expired_keys") - c.info("expired_keys_active");
    let mut writes = Vec::new();
    for (counter, list) in counters.iter().zip(&lists) {
        writes.push(vec!["INCR", counter.as_str()]);
        writes.push(vec!["RPUSH", list.as_str(), "new"]);
    }
    let replies = after_a_pause(&mut c, t, &writes);
    assert!(replies.iter().all(|r| *r == V::Int(1)), "{replies:?}");
    let lazy = c.info("expired_keys") - c.info("expired_keys_active") - lazy_before;
    eprintln!("found past their time by the writes: {lazy} of 100");

    aof_holds(&s, "UNLINK", 100);
    let aof = s.aof();
    for (key, old, new) in counters
        .iter()
        .map(|k| (k, "SET", "INCR"))
        .chain(lists.iter().map(|k| (k, "RPUSH", "RPUSH")))
    {
        let unlink = positions(&aof, "UNLINK", key);
        let (old, new) = (positions(&aof, old, key), positions(&aof, new, key));
        assert_eq!(unlink.len(), 1, "{key}: one deletion");
        assert!(
            old[0] < unlink[0] && unlink[0] < *new.last().unwrap(),
            "{key}: deleted between the old write and the new"
        );
    }

    s.restart();
    let mut c = s.conn();
    for (counter, list) in counters.iter().zip(&lists) {
        assert_eq!(c.get(counter).as_deref(), Some("1"), "{counter}");
        assert_eq!(c.cmd(&["PTTL", counter]).int(), -1, "{counter}");
        assert_eq!(
            c.cmd(&["LRANGE", list, "0", "-1"]),
            V::Array(vec![V::Bulk(b"new".to_vec())]),
            "{list}"
        );
        assert_eq!(c.cmd(&["PTTL", list]).int(), -1, "{list}");
    }
}

/// With `lazyfree-lazy-expire no` the deletion is a `DEL`.
#[test]
fn without_lazyfree_lazy_expire_the_deletion_is_del() {
    let s = Server::with_aof();
    let mut c = s.conn();
    c.ok(&["CONFIG", "SET", "lazyfree-lazy-expire", "no"]);
    c.ok(&["SET", "k", "v", "PX", "50"]);
    wait_for("the sweep", || c.cmd(&["DBSIZE"]).int() == 0);
    aof_holds(&s, "DEL", 1);
    assert!(positions(&s.aof(), "UNLINK", "k").is_empty());
}

/// A key of another database is deleted there, and the AOF goes back to
/// the database its next write is in.
#[test]
fn a_deletion_in_another_database_is_replayed_there() {
    let mut s = Server::with_aof();
    let mut c = s.conn();
    c.ok(&["SELECT", "3"]);
    c.ok(&["SET", "k", "in-3", "PX", "100"]);
    c.ok(&["SELECT", "0"]);
    c.ok(&["SET", "k", "in-0"]);
    wait_for("the sweep", || c.info("expired_keys_active") >= 1);
    c.ok(&["SET", "after", "in-0"]);
    aof_holds(&s, "UNLINK", 1);
    // In the file before the kill: a write can be answered before its
    // record is written.
    wait_for("the last write to reach the AOF", || {
        !positions(&s.aof(), "SET", "after").is_empty()
    });
    s.restart();
    let mut c = s.conn();
    assert_eq!(c.get("k").as_deref(), Some("in-0"));
    assert_eq!(c.get("after").as_deref(), Some("in-0"));
    c.ok(&["SELECT", "3"]);
    assert_eq!(c.cmd(&["DBSIZE"]).int(), 0);
}

// ── Replicas ─────────────────────────────────────────────────────────────

/// A replica held back past the keys' time applies `SET k 5 PXAT t` and the
/// `INCR k` its master ran while `k` lived, then the master's own writes to
/// `k`, as the master did: it does not take `k` for gone by its own clock.
/// Keys the master let go are gone on the replica too, by the master's
/// deletions, and the replica deletes nothing by itself.
#[test]
fn a_replica_held_back_past_the_deadline_converges() {
    let master = Server::start(&[]);
    let replica = Server::replica_of(&master);
    let (mut m, mut r) = (master.conn(), replica.conn());
    caught_up(&mut m, &mut r);

    assert!(replica.signal("-STOP"));
    let t = now_ms() + 300;
    let at = t.to_string();
    let later = (now_ms() + 600_000).to_string();
    for i in 0..100 {
        let kept = format!("kept:{i}");
        let gone = format!("gone:{i}");
        m.ok(&["SET", &kept, "5", "PXAT", &at]);
        assert_eq!(m.cmd(&["INCR", &kept]).int(), 6);
        assert_eq!(m.cmd(&["PEXPIREAT", &kept, &later]).int(), 1);
        m.ok(&["SET", &gone, "5", "PXAT", &at]);
        assert_eq!(m.cmd(&["INCR", &gone]).int(), 6);
    }
    assert!(now_ms() < t);
    // The master deletes `gone:*` meanwhile; the replica has not even seen
    // them yet.
    // 100 kept, and caught-up.
    wait_for("the master's sweep", || m.cmd(&["DBSIZE"]).int() == 101);
    assert!(replica.signal("-CONT"));

    caught_up(&mut m, &mut r);
    assert_eq!(dataset(&mut r), dataset(&mut m));
    for i in 0..100 {
        assert_eq!(r.get(&format!("kept:{i}")).as_deref(), Some("6"));
    }
    assert_eq!(r.cmd(&["DBSIZE"]).int(), 101, "100 kept, and caught-up");
    assert_eq!(
        r.info("expired_keys"),
        0,
        "the replica deleted nothing itself"
    );
}

/// The master deletes a key for its time, then writes it anew: the replica
/// gets the deletion first, and ends with the master's key.
#[test]
fn a_replica_gets_the_deletion_before_the_write() {
    let master = Server::start(&[]);
    let replica = Server::replica_of(&master);
    let (mut m, mut r) = (master.conn(), replica.conn());

    // Deleted by the sweep, then written.
    m.ok(&["SET", "swept", "5", "PX", "100"]);
    wait_for("the sweep", || m.cmd(&["EXISTS", "swept"]).int() == 0);
    assert_eq!(m.cmd(&["INCR", "swept"]).int(), 1);
    // Found past its time by the writes themselves.
    let t = now_ms() + 100;
    let at = t.to_string();
    for i in 0..50 {
        m.ok(&["SET", &format!("found:{i}"), "5", "PXAT", &at]);
        assert_eq!(m.cmd(&["RPUSH", &format!("queue:{i}"), "old"]).int(), 1);
        assert_eq!(m.cmd(&["PEXPIREAT", &format!("queue:{i}"), &at]).int(), 1);
    }
    let keys: Vec<(String, String)> = (0..50)
        .map(|i| (format!("found:{i}"), format!("queue:{i}")))
        .collect();
    let mut writes = Vec::new();
    for (found, queue) in &keys {
        writes.push(vec!["INCR", found.as_str()]);
        writes.push(vec!["RPUSH", queue.as_str(), "new"]);
    }
    let replies = after_a_pause(&mut m, t, &writes);
    assert!(replies.iter().all(|r| *r == V::Int(1)), "{replies:?}");

    caught_up(&mut m, &mut r);
    assert_eq!(dataset(&mut r), dataset(&mut m));
    assert_eq!(r.get("swept").as_deref(), Some("1"));
    for (found, queue) in &keys {
        assert_eq!(r.get(found).as_deref(), Some("1"));
        assert_eq!(r.cmd(&["PTTL", found]).int(), -1);
        assert_eq!(
            r.cmd(&["LRANGE", queue, "0", "-1"]),
            V::Array(vec![V::Bulk(b"new".to_vec())])
        );
    }
    assert_eq!(r.info("expired_keys"), 0);
}

/// Clients writing keys while they expire, by the sweep and by the writes:
/// the replica, and a replay of the master's AOF, end with the master's
/// dataset.
#[test]
fn writes_racing_expiry_converge_on_the_replica_and_the_aof() {
    let mut master = Server::with_aof();
    let replica = Server::replica_of(&master);
    let port = master.port;
    let stop_at = Instant::now() + Duration::from_millis(1_500);
    let writers: Vec<_> = (0..4)
        .map(|w| {
            thread::spawn(move || {
                let mut c = Conn::connect(port);
                let mut i: u64 = w;
                while Instant::now() < stop_at {
                    i = i
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let key = format!("race:{}", (i >> 33) % 64);
                    match (i >> 20) % 4 {
                        0 => {
                            let px = (10 + (i >> 40) % 40).to_string();
                            c.ok(&["SET", &key, "0", "PX", &px]);
                        }
                        1 | 2 => {
                            let _ = c.cmd(&["INCR", &key]);
                        }
                        _ => {
                            let _ = c.cmd(&["GET", &key]);
                        }
                    }
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    let (mut m, mut r) = (master.conn(), replica.conn());
    // And every key written once more: over a live key, one past its time
    // the sweep has not got to, or one it has. Half of them only once every
    // time to live (50 ms at most) has run out, so that some keys stay: the
    // writers may have given all 64 a time to live in their last 50 ms, and
    // an INCR keeps it, so with every key written at once nothing need stay.
    for i in (0..64).step_by(2) {
        let _ = m.cmd(&["INCR", &format!("race:{i}")]);
    }
    thread::sleep(Duration::from_millis(60));
    for i in (1..64).step_by(2) {
        let _ = m.cmd(&["INCR", &format!("race:{i}")]);
    }
    // Until every time to live has run out, and the sweep has been.
    wait_for("the last keys with a time to live to go", || {
        dataset(&mut m).iter().all(|(_, _, at)| *at == -1)
    });
    caught_up(&mut m, &mut r);
    let expected = dataset(&mut m);
    assert!(!expected.is_empty());
    assert_eq!(dataset(&mut r), expected, "the replica");
    assert_eq!(r.info("expired_keys"), 0);

    let last = m.cmd(&["DBSIZE"]).int();
    m.ok(&["SET", "aof-mark", "1"]);
    aof_holds(&master, "SET", 1);
    wait_for("the AOF to have everything", || {
        master
            .aof()
            .iter()
            .any(|r| r.len() >= 2 && r[0] == "SET" && r[1] == "aof-mark")
    });
    master.restart();
    let mut m = master.conn();
    assert_eq!(m.cmd(&["DBSIZE"]).int(), last + 1);
    m.cmd(&["DEL", "aof-mark"]);
    assert_eq!(dataset(&mut m), expected, "the replay");
}

// ── What else a key that goes for its time does ──────────────────────────

/// A watched key that goes for its time, whether the sweep finds it or a
/// write does, fails the transaction.
#[test]
fn a_watched_key_that_expires_fails_exec() {
    let s = Server::start(&[]);
    let (mut a, mut b) = (s.conn(), s.conn());

    a.ok(&["SET", "swept", "v", "PX", "50"]);
    a.ok(&["WATCH", "swept"]);
    wait_for("the sweep", || b.cmd(&["EXISTS", "swept"]).int() == 0);
    a.ok(&["MULTI"]);
    assert_eq!(a.cmd(&["SET", "other", "x"]), V::Simple("QUEUED".into()));
    assert_eq!(a.cmd(&["EXEC"]), V::Nil);

    let t = now_ms() + 100;
    b.ok(&["SET", "written", "5", "PXAT", &t.to_string()]);
    a.ok(&["WATCH", "written"]);
    let replies = after_a_pause(&mut b, t, &[vec!["INCR", "written"]]);
    assert_eq!(replies, vec![V::Int(1)]);
    a.ok(&["MULTI"]);
    assert_eq!(a.cmd(&["SET", "other", "x"]), V::Simple("QUEUED".into()));
    assert_eq!(a.cmd(&["EXEC"]), V::Nil);

    // Written over in place (a list).
    let t = now_ms() + 100;
    assert_eq!(b.cmd(&["RPUSH", "queue", "old"]).int(), 1);
    assert_eq!(b.cmd(&["PEXPIREAT", "queue", &t.to_string()]).int(), 1);
    a.ok(&["WATCH", "queue"]);
    let replies = after_a_pause(&mut b, t, &[vec!["RPUSH", "queue", "new"]]);
    assert_eq!(replies, vec![V::Int(1)]);
    a.ok(&["MULTI"]);
    assert_eq!(a.cmd(&["SET", "other", "x"]), V::Simple("QUEUED".into()));
    assert_eq!(a.cmd(&["EXEC"]), V::Nil);
}

/// A client caching a key is told once when a write finds it past its time,
/// and once when the sweep deletes it.
#[test]
fn a_tracking_client_is_told_once() {
    let s = Server::start(&[]);
    let (mut sub, mut reader, mut writer) = (s.conn(), s.conn(), s.conn());
    let sub_id = sub.cmd(&["CLIENT", "ID"]).int().to_string();
    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "__redis__:invalidate"]),
        V::Array(vec![
            V::Bulk(b"subscribe".to_vec()),
            V::Bulk(b"__redis__:invalidate".to_vec()),
            V::Int(1)
        ])
    );
    reader.ok(&["CLIENT", "TRACKING", "ON", "REDIRECT", &sub_id]);
    let invalidations = |sub: &mut Conn| {
        let mut keys = Vec::new();
        while let Some(msg) = sub.read_within(Duration::from_millis(300)) {
            if let V::Array(parts) = msg
                && let Some(V::Array(ks)) = parts.get(2)
            {
                keys.extend(ks.iter().map(|k| k.text().unwrap()));
            }
        }
        keys
    };

    let t = now_ms() + 100;
    writer.ok(&["SET", "written", "5", "PXAT", &t.to_string()]);
    assert_eq!(reader.get("written").as_deref(), Some("5"));
    let replies = after_a_pause(&mut writer, t, &[vec!["INCR", "written"]]);
    assert_eq!(replies, vec![V::Int(1)]);
    assert_eq!(invalidations(&mut sub), vec!["written".to_string()]);

    // Written over in place (a list).
    assert_eq!(writer.cmd(&["RPUSH", "queue", "old"]).int(), 1);
    let t = now_ms() + 100;
    assert_eq!(writer.cmd(&["PEXPIREAT", "queue", &t.to_string()]).int(), 1);
    assert_eq!(reader.cmd(&["LLEN", "queue"]).int(), 1);
    let replies = after_a_pause(&mut writer, t, &[vec!["RPUSH", "queue", "new"]]);
    assert_eq!(replies, vec![V::Int(1)]);
    assert_eq!(invalidations(&mut sub), vec!["queue".to_string()]);

    writer.ok(&["SET", "swept", "5", "PX", "2000"]);
    assert_eq!(reader.get("swept").as_deref(), Some("5"));
    wait_for("the sweep", || writer.cmd(&["EXISTS", "swept"]).int() == 0);
    assert_eq!(invalidations(&mut sub), vec!["swept".to_string()]);
}

/// A document that goes for its time leaves its index, whether the sweep
/// finds it or a write of another type does.
#[test]
fn an_expired_document_leaves_its_index() {
    let s = Server::start(&[]);
    let mut c = s.conn();
    c.ok(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "tag",
        "TAG",
    ]);
    let found = |c: &mut Conn| match c.cmd(&["FT.SEARCH", "idx", "@tag:{a}", "NOCONTENT"]) {
        V::Array(parts) => parts[0].int(),
        other => panic!("FT.SEARCH: {other:?}"),
    };
    assert_eq!(c.cmd(&["HSET", "doc:swept", "tag", "a"]).int(), 1);
    assert_eq!(c.cmd(&["PEXPIRE", "doc:swept", "50"]).int(), 1);
    let t = now_ms() + 100;
    assert_eq!(c.cmd(&["HSET", "doc:written", "tag", "a"]).int(), 1);
    assert_eq!(
        c.cmd(&["PEXPIREAT", "doc:written", &t.to_string()]).int(),
        1
    );
    assert_eq!(found(&mut c), 2);
    wait_for("the sweep", || c.cmd(&["EXISTS", "doc:swept"]).int() == 0);
    // A list where the document was: nothing for the index to hold.
    let replies = after_a_pause(&mut c, t, &[vec!["RPUSH", "doc:written", "x"]]);
    assert_eq!(replies, vec![V::Int(1)]);
    assert_eq!(found(&mut c), 0);
}

/// A key a write puts a new value over, past its time, is a new key to
/// `allkeys-lfu`: it starts at a new key's count, not the one of the value
/// that went. (A list: `RPUSH` writes over the old entry where `SET` deletes
/// it first.)
#[test]
fn a_key_written_over_past_its_time_starts_a_new_count() {
    let s = Server::start(&[]);
    let mut c = s.conn();
    c.ok(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"]);
    assert_eq!(c.cmd(&["RPUSH", "fresh", "a"]).int(), 1);
    let fresh = c.cmd(&["OBJECT", "FREQ", "fresh"]).int();
    let t = now_ms() + 3_000;
    assert_eq!(c.cmd(&["RPUSH", "hot", "a"]).int(), 1);
    assert_eq!(c.cmd(&["PEXPIREAT", "hot", &t.to_string()]).int(), 1);
    assert_eq!(c.cmd(&["RPUSH", "live", "a"]).int(), 1);
    for _ in 0..1_000 {
        c.cmd(&["LLEN", "hot"]);
        c.cmd(&["LLEN", "live"]);
    }
    assert!(now_ms() < t);
    let hot = c.cmd(&["OBJECT", "FREQ", "hot"]).int();
    assert!(hot > fresh + 5, "{hot} against {fresh}");
    let replies = after_a_pause(
        &mut c,
        t,
        &[vec!["RPUSH", "hot", "b"], vec!["RPUSH", "live", "b"]],
    );
    assert_eq!(replies, vec![V::Int(1), V::Int(2)]);
    assert_eq!(c.cmd(&["OBJECT", "FREQ", "hot"]).int(), fresh);
    assert!(
        c.cmd(&["OBJECT", "FREQ", "live"]).int() > fresh + 5,
        "a live key keeps its count"
    );
}

/// Keys that go for their time, raced by reads, writes and the sweep: one
/// `expired` event, one count and one deletion in the AOF, each.
#[test]
fn each_expiry_is_announced_counted_and_logged_once() {
    let s = Server::with_aof();
    let mut c = s.conn();
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "Ex"]);
    let mut sub = s.conn();
    assert!(matches!(
        sub.cmd(&["PSUBSCRIBE", "__keyevent@0__:expired"]),
        V::Array(_)
    ));
    let expired_before = c.info("expired_keys");
    let t = now_ms() + 200;
    let at = t.to_string();
    let keys: Vec<String> = (0..60).map(|i| format!("once:{i}")).collect();
    for key in &keys {
        c.ok(&["SET", key, "5", "PXAT", &at]);
    }
    let port = s.port;
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let keys = keys.clone();
            thread::spawn(move || {
                let mut c = Conn::connect(port);
                until(t);
                for _ in 0..3 {
                    for key in &keys {
                        c.get(key);
                    }
                }
            })
        })
        .collect();
    // Half of them written over as they go.
    let writes: Vec<Vec<&str>> = keys
        .iter()
        .step_by(2)
        .map(|k| vec!["INCR", k.as_str()])
        .collect();
    let replies = after_a_pause(&mut c, t, &writes);
    for r in readers {
        r.join().unwrap();
    }
    assert!(replies.iter().all(|r| *r == V::Int(1)), "{replies:?}");
    wait_for("the sweep", || {
        c.cmd(&["DBSIZE"]).int() == writes.len() as i64
    });

    let mut events = std::collections::BTreeMap::<String, usize>::new();
    while let Some(msg) = sub.read_within(Duration::from_millis(300)) {
        if let V::Array(parts) = msg {
            *events.entry(parts[3].text().unwrap()).or_default() += 1;
        }
    }
    for key in &keys {
        assert_eq!(events.get(key), Some(&1), "{key}: one expired event");
    }
    assert_eq!(c.info("expired_keys") - expired_before, keys.len() as i64);
    aof_holds(&s, "UNLINK", keys.len());
    let aof = s.aof();
    for key in &keys {
        assert_eq!(
            positions(&aof, "UNLINK", key).len(),
            1,
            "{key}: one deletion"
        );
    }
}
