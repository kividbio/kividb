// tests/hash_field_expiry.rs
//
// A hash field's expiry, as the readers of the write stream see it: a
// replica that lags behind a delaying proxy, one that syncs while deadlines
// pass, and the AOF replayed after a kill. Only the master removes a field
// for its deadline, and its `HDEL` (or the `DEL` of a hash it emptied) is in
// the stream before any write that found the field gone.
//
// The I/O model is the platform's own unless `KIVIDB_TEST_IO_MODEL` says.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
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

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ── Delaying proxy ────────────────────────────────────────────────────────────

/// Between a replica and its master: what the master sends reaches the
/// replica `delay` later, in order; what the replica sends goes straight
/// through. [`Proxy::cut`] drops every connection it carries.
struct Proxy {
    port: u16,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn to(master: u16, delay: Duration) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let sockets: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let (stop, sockets) = (Arc::clone(&stop), Arc::clone(&sockets));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let Ok((down, _)) = listener.accept() else {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    };
                    down.set_nonblocking(false).unwrap();
                    let Ok(up) = TcpStream::connect(("127.0.0.1", master)) else {
                        continue;
                    };
                    sockets
                        .lock()
                        .unwrap()
                        .extend([down.try_clone().unwrap(), up.try_clone().unwrap()]);
                    pipe(up.try_clone().unwrap(), down.try_clone().unwrap(), delay);
                    pipe(down, up, Duration::ZERO);
                }
            });
        }
        Proxy {
            port,
            stop,
            sockets,
        }
    }

    /// Every connection it carries closed, as a network that fails.
    fn cut(&self) {
        for socket in self.sockets.lock().unwrap().drain(..) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.cut();
    }
}

/// Pieces read and when, waiting to be written.
type Held = Arc<(Mutex<VecDeque<(Instant, Vec<u8>)>>, Condvar)>;

/// Copy `from` to `to`, each piece held back `delay` from when it was read.
fn pipe(mut from: TcpStream, mut to: TcpStream, delay: Duration) {
    let queue: Held = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
    let done = Arc::new(AtomicBool::new(false));
    {
        let (queue, done) = (Arc::clone(&queue), Arc::clone(&done));
        std::thread::spawn(move || {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match from.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let (lock, ready) = &*queue;
                        lock.lock()
                            .unwrap()
                            .push_back((Instant::now() + delay, buf[..n].to_vec()));
                        ready.notify_one();
                    }
                }
            }
            done.store(true, Ordering::Relaxed);
            queue.1.notify_one();
        });
    }
    std::thread::spawn(move || {
        loop {
            let next = {
                let (lock, ready) = &*queue;
                let mut pending = lock.lock().unwrap();
                while pending.is_empty() && !done.load(Ordering::Relaxed) {
                    pending = ready
                        .wait_timeout(pending, Duration::from_millis(50))
                        .unwrap()
                        .0;
                }
                pending.pop_front()
            };
            let Some((at, bytes)) = next else {
                let _ = to.shutdown(Shutdown::Both);
                return;
            };
            if let Some(wait) = at.checked_duration_since(Instant::now()) {
                std::thread::sleep(wait);
            }
            if to.write_all(&bytes).is_err() {
                return;
            }
        }
    });
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// What `key` holds, as a reader sees it: its type and contents, fields in
/// order of name.
fn value(c: &mut Conn, key: &str) -> String {
    match c.cmd(&["TYPE", key]).s() {
        "hash" => {
            let flat = c.cmd(&["HGETALL", key]);
            let flat = flat.arr();
            let mut pairs: Vec<String> = flat
                .chunks(2)
                .map(|p| format!("{}={}", p[0].s(), p[1].s()))
                .collect();
            pairs.sort();
            format!("hash{pairs:?}")
        }
        "list" => format!("list{:?}", c.cmd(&["LRANGE", key, "0", "-1"])),
        "none" => "none".to_string(),
        other => other.to_string(),
    }
}

/// Master and replica agree on every key of `keys`, once the replica has
/// caught up and the master's sweep has had time to send what came due.
fn assert_same_keys(master: &mut Conn, replica: &mut Conn, keys: &[String]) {
    std::thread::sleep(Duration::from_millis(400));
    wait_caught_up(master, replica);
    let mut differ = Vec::new();
    for key in keys {
        let (m, r) = (value(master, key), value(replica, key));
        if m != r {
            differ.push(format!("{key}: master {m}, replica {r}"));
        }
    }
    assert!(
        differ.is_empty(),
        "{} of {} keys differ:\n{}",
        differ.len(),
        keys.len(),
        differ.join("\n")
    );
}

/// A replica of `master` behind a proxy that holds back the master's stream
/// `lag`.
fn replica_behind(master: &Server, lag: Duration) -> (Server, Proxy) {
    let proxy = Proxy::to(master.port, lag);
    let replica = Server::start(&[]);
    let mut r = replica.conn();
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &proxy.port.to_string()])
            .s(),
        "OK"
    );
    wait_link_up(&mut r);
    (replica, proxy)
}

/// The writes of the issue's reproduction, each made just before, at, or
/// just after a field's deadline: `HINCRBY`, `HINCRBYFLOAT`, `HSETNX`,
/// `HPEXPIRE … XX` and `… GT`, `HPERSIST`. Each key is `{f: 5, keep: 1}`,
/// `f` due 40 to 100 ms on. Returns the keys.
fn writes_near_deadlines(c: &mut Conn, prefix: &str, n: usize) -> Vec<String> {
    let start = unix_ms_now() + 40;
    let mut plan = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        let at = start + (i as u64 % 60);
        c.cmd(&["HSET", &key, "f", "5", "keep", "1"]);
        c.cmd(&["HPEXPIREAT", &key, &at.to_string(), "FIELDS", "1", "f"]);
        // From 2 ms before the deadline to 2 ms after it.
        plan.push((at + (i as u64 % 5)).saturating_sub(2));
    }
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| plan[i]);
    for i in order {
        let key = format!("{prefix}:{i}");
        let wait = plan[i].saturating_sub(unix_ms_now());
        std::thread::sleep(Duration::from_millis(wait));
        match i % 7 {
            0 => c.cmd(&["HINCRBY", &key, "f", "1"]),
            1 => c.cmd(&["HINCRBYFLOAT", &key, "f", "0.5"]),
            2 => c.cmd(&["HSETNX", &key, "f", "new"]),
            3 => c.cmd(&["HPEXPIRE", &key, "60000", "XX", "FIELDS", "1", "f"]),
            4 => c.cmd(&["HPEXPIRE", &key, "60000", "GT", "FIELDS", "1", "f"]),
            5 => c.cmd(&["HPERSIST", &key, "FIELDS", "1", "f"]),
            _ => c.cmd(&["HGETEX", &key, "PERSIST", "FIELDS", "1", "f"]),
        };
    }
    (0..n).map(|i| format!("{prefix}:{i}")).collect()
}

// ── Replication ───────────────────────────────────────────────────────────────

#[test]
fn the_master_says_it_sends_the_deletion_of_the_fields_it_expires() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    let info = c.cmd(&["INFO", "server"]);
    assert!(
        info.s()
            .lines()
            .any(|l| l.trim() == "kividb_expired_fields_propagated:1"),
        "{info:?}"
    );
}

#[test]
fn a_lagging_replica_agrees_with_writes_near_field_deadlines() {
    let master = Server::start(&[]);
    let (replica, _proxy) = replica_behind(&master, Duration::from_millis(20));
    let (mut m, mut r) = (master.conn(), replica.conn());
    let keys = writes_near_deadlines(&mut m, "lag", 140);
    assert_same_keys(&mut m, &mut r, &keys);
}

/// `HPERSIST` reaches a lagging replica after the field's deadline has
/// passed there: it is applied all the same, and the field lives on both.
#[test]
fn hpersist_that_arrives_after_the_deadline_is_applied() {
    let master = Server::start(&[]);
    let (replica, _proxy) = replica_behind(&master, Duration::from_millis(60));
    let (mut m, mut r) = (master.conn(), replica.conn());
    let mut keys = Vec::new();
    for i in 0..20 {
        let key = format!("persist:{i}");
        m.cmd(&["HSET", &key, "f", "5"]);
        m.cmd(&["HPEXPIRE", &key, "30", "FIELDS", "1", "f"]);
        assert_eq!(
            m.cmd(&["HPERSIST", &key, "FIELDS", "1", "f"]),
            V::Arr(vec![V::Int(1)])
        );
        keys.push(key);
    }
    std::thread::sleep(Duration::from_millis(300));
    assert_same_keys(&mut m, &mut r, &keys);
    for key in &keys {
        assert_eq!(r.cmd(&["HGET", key, "f"]).s(), "5", "{key}");
        assert_eq!(
            r.cmd(&["HPTTL", key, "FIELDS", "1", "f"]),
            V::Arr(vec![V::Int(-1)])
        );
    }
}

/// The hash's last field expires, and the key is then written as a list:
/// the replica has the hash's deletion before the `LPUSH`.
#[test]
fn a_hash_emptied_by_its_deadlines_can_become_another_type_on_the_replica() {
    let master = Server::start(&[]);
    let (replica, _proxy) = replica_behind(&master, Duration::from_millis(20));
    let (mut m, mut r) = (master.conn(), replica.conn());
    let keys: Vec<String> = (0..30).map(|i| format!("type:{i}")).collect();
    for key in &keys {
        m.cmd(&["HSET", key, "f", "5"]);
        m.cmd(&["HPEXPIRE", key, "20", "FIELDS", "1", "f"]);
    }
    std::thread::sleep(Duration::from_millis(60));
    for key in &keys {
        // Whether the sweep has come to it yet or not, a read takes it.
        assert_eq!(m.cmd(&["HGET", key, "f"]), V::Nil);
        assert_eq!(m.cmd(&["LPUSH", key, "x"]).i(), 1);
    }
    assert_same_keys(&mut m, &mut r, &keys);
    for key in &keys {
        assert_eq!(r.cmd(&["TYPE", key]).s(), "list");
    }
}

/// A replica that waits for its master's `HDEL` hides the field from its
/// readers meanwhile, and does not remove it itself.
#[test]
fn a_replica_hides_a_due_field_until_the_master_sends_its_deletion() {
    let master = Server::start(&[]);
    let (replica, _proxy) = replica_behind(&master, Duration::ZERO);
    let (mut m, mut r) = (master.conn(), replica.conn());
    m.cmd(&["HSET", "h", "f", "5", "keep", "1"]);
    m.cmd(&["HPEXPIRE", "h", "300", "FIELDS", "1", "f"]);
    wait_caught_up(&mut m, &mut r);
    // The master stopped: no `HDEL` can come.
    master.signal(libc::SIGSTOP);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(r.cmd(&["HGET", "h", "f"]), V::Nil);
    assert_eq!(r.cmd(&["HLEN", "h"]).i(), 1);
    assert_eq!(r.cmd(&["HKEYS", "h"]), V::Arr(vec![V::Str("keep".into())]));
    master.signal(libc::SIGCONT);
    std::thread::sleep(Duration::from_millis(300));
    assert_same_keys(&mut m, &mut r, &["h".to_string()]);
}

/// Deadlines that pass while a replica syncs: the master's writes made just
/// before them are applied to the fields as the master had them.
#[test]
fn a_full_sync_across_field_deadlines_agrees_with_the_master() {
    let master = Server::start(&[]);
    let mut m = master.conn();
    // Enough for the sync to take a while.
    for i in 0..20_000 {
        m.cmd(&["SET", &format!("filler:{i}"), "x"]);
    }
    let keys: Vec<String> = (0..100).map(|i| format!("sync:{i}")).collect();
    let at = unix_ms_now() + 400;
    for key in &keys {
        m.cmd(&["HSET", key, "f", "5", "keep", "1"]);
        m.cmd(&["HPEXPIREAT", key, &at.to_string(), "FIELDS", "1", "f"]);
    }
    let dir = tempfile::tempdir().unwrap().keep();
    let port = free_port();
    let env = vec![(
        "KIVIDB_TEST_FULL_SYNC_LOAD_DELAY_MS".to_string(),
        "600".to_string(),
    )];
    let child = kividb(&dir, port, &[], &env);
    let replica = Server {
        port,
        dir,
        child,
        args: Vec::new(),
        env,
    };
    let mut r = replica.conn();
    r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]);
    // Just before the deadline, while the replica is in its sync.
    std::thread::sleep(Duration::from_millis(
        at.saturating_sub(unix_ms_now()).saturating_sub(5),
    ));
    for key in &keys {
        m.cmd(&["HINCRBY", key, "f", "1"]);
    }
    wait_link_up(&mut r);
    assert_same_keys(&mut m, &mut r, &keys);
}

/// A replica cut off while fields expire continues by a partial resync and
/// ends where its master is.
#[test]
fn a_partial_resync_across_field_deadlines_agrees_with_the_master() {
    let master = Server::start(&[]);
    let (replica, proxy) = replica_behind(&master, Duration::ZERO);
    let (mut m, mut r) = (master.conn(), replica.conn());
    let keys: Vec<String> = (0..50).map(|i| format!("psync:{i}")).collect();
    for key in &keys {
        m.cmd(&["HSET", key, "f", "5", "keep", "1"]);
        m.cmd(&["HPEXPIRE", key, "150", "FIELDS", "1", "f"]);
    }
    wait_caught_up(&mut m, &mut r);
    proxy.cut();
    std::thread::sleep(Duration::from_millis(100));
    for key in &keys {
        m.cmd(&["HINCRBY", key, "f", "1"]);
    }
    std::thread::sleep(Duration::from_millis(200));
    for key in &keys {
        m.cmd(&["HINCRBY", key, "f", "1"]);
    }
    wait_link_up(&mut r);
    assert_same_keys(&mut m, &mut r, &keys);
    assert!(
        replica.log().contains("Partial resync"),
        "not a partial resync: {}",
        replica.log()
    );
}

/// A transaction and a script that find a field due: its `HDEL` goes with
/// their writes, before the write that found it gone.
#[test]
fn a_transaction_and_a_script_that_find_a_field_due_agree_on_the_replica() {
    let master = Server::start(&[]);
    let (replica, _proxy) = replica_behind(&master, Duration::from_millis(10));
    let (mut m, mut r) = (master.conn(), replica.conn());
    for key in ["tx", "lua"] {
        m.cmd(&["HSET", key, "f", "5", "keep", "1"]);
        m.cmd(&["HPEXPIRE", key, "20", "FIELDS", "1", "f"]);
    }
    std::thread::sleep(Duration::from_millis(60));
    m.cmd(&["MULTI"]);
    m.cmd(&["HGET", "tx", "f"]);
    m.cmd(&["HINCRBY", "tx", "f", "1"]);
    assert_eq!(m.cmd(&["EXEC"]), V::Arr(vec![V::Nil, V::Int(1)]));
    let script = "redis.call('HINCRBY', KEYS[1], 'f', 1) return redis.call('HGET', KEYS[1], 'f')";
    if let V::Str(out) = m.cmd(&["EVAL", script, "1", "lua"]) {
        assert_eq!(out, "1");
    }
    assert_same_keys(&mut m, &mut r, &["tx".to_string(), "lua".to_string()]);
}

/// A field that expires on the master breaks a `WATCH` of its hash, is
/// announced as `hexpired`, and the emptied hash as `del`.
#[test]
fn an_expired_field_breaks_a_watch_and_is_announced() {
    let master = Server::start(&[]);
    let mut c = master.conn();
    c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "Ehg"]);
    let mut sub = master.conn();
    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "__keyevent@0__:hexpired", "__keyevent@0__:del"])
            .arr()
            .len(),
        3
    );
    sub.read();
    c.cmd(&["HSET", "w", "f", "5"]);
    c.cmd(&["HPEXPIRE", "w", "50", "FIELDS", "1", "f"]);
    let mut watcher = master.conn();
    watcher.cmd(&["WATCH", "w"]);
    std::thread::sleep(Duration::from_millis(300));
    watcher.cmd(&["MULTI"]);
    watcher.cmd(&["SET", "other", "1"]);
    assert_eq!(watcher.cmd(&["EXEC"]), V::Nil, "the watch held");
    let first = sub.read();
    assert_eq!(first.arr()[1].s(), "__keyevent@0__:hexpired");
    assert_eq!(first.arr()[2].s(), "w");
    let second = sub.read();
    assert_eq!(second.arr()[1].s(), "__keyevent@0__:del");
}

/// A document whose indexed field expires leaves the index's answers, on
/// the master and on its replica.
#[test]
fn an_expired_indexed_field_leaves_the_index_on_master_and_replica() {
    let master = Server::start(&[]);
    let (replica, _proxy) = replica_behind(&master, Duration::from_millis(10));
    let (mut m, mut r) = (master.conn(), replica.conn());
    let created = m.cmd(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "n",
        "NUMERIC",
    ]);
    assert_eq!(created.s(), "OK");
    m.cmd(&["HSET", "doc:1", "n", "7", "other", "x"]);
    m.cmd(&["HPEXPIRE", "doc:1", "50", "FIELDS", "1", "n"]);
    std::thread::sleep(Duration::from_millis(400));
    wait_caught_up(&mut m, &mut r);
    for c in [&mut m, &mut r] {
        let found = c.cmd(&["FT.SEARCH", "idx", "@n:[0 10]", "NOCONTENT"]);
        assert_eq!(found.arr()[0], V::Int(0), "{found:?}");
    }
}

/// A master that does not say it sends the deletion of the fields it
/// expires is followed as before: the replica expires fields itself.
#[test]
fn a_replica_of_a_master_that_does_not_say_so_expires_fields_itself() {
    let mut master = Server::start(&[]);
    master.env = vec![(
        "KIVIDB_TEST_NO_FIELD_EXPIRY_CAPABILITY".to_string(),
        "1".to_string(),
    )];
    master.kill();
    master.restart();
    let (replica, _proxy) = replica_behind(&master, Duration::ZERO);
    let (mut m, mut r) = (master.conn(), replica.conn());
    m.cmd(&["HSET", "h", "f", "5", "keep", "1"]);
    m.cmd(&["HPEXPIRE", "h", "200", "FIELDS", "1", "f"]);
    wait_caught_up(&mut m, &mut r);
    assert!(
        replica
            .log()
            .contains("this replica expires hash fields itself"),
        "{}",
        replica.log()
    );
    master.signal(libc::SIGSTOP);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(r.cmd(&["HGETALL", "h"]).arr().len(), 2);
    master.signal(libc::SIGCONT);
    assert_same_keys(&mut m, &mut r, &["h".to_string()]);
}

// ── AOF ───────────────────────────────────────────────────────────────────────

/// What each key holds on `c`.
fn snapshot_of(c: &mut Conn, keys: &[String]) -> Vec<String> {
    keys.iter()
        .map(|k| format!("{k}: {}", value(c, k)))
        .collect()
}

/// Writes after field deadlines, and a type change after the last field
/// went: each comes back after a kill as the master had it.
fn writes_after_deadlines(c: &mut Conn) -> Vec<String> {
    let mut keys = Vec::new();
    for i in 0..5 {
        let key = format!("aof:{i}");
        c.cmd(&["HSET", &key, "f", "5", "keep", "1"]);
        c.cmd(&["HPEXPIRE", &key, "20", "FIELDS", "1", "f"]);
        keys.push(key);
    }
    c.cmd(&["HSET", "aof:type", "f", "5"]);
    c.cmd(&["HPEXPIRE", "aof:type", "20", "FIELDS", "1", "f"]);
    keys.push("aof:type".to_string());
    // Past every deadline, before the sweep could come to all of them.
    std::thread::sleep(Duration::from_millis(30));
    let writes: [&[&str]; 5] = [
        &["HINCRBY", "aof:0", "f", "1"],
        &["HINCRBYFLOAT", "aof:1", "f", "1.5"],
        &["HSETNX", "aof:2", "f", "new"],
        &["HPERSIST", "aof:3", "FIELDS", "1", "f"],
        &["HPEXPIRE", "aof:4", "60000", "XX", "FIELDS", "1", "f"],
    ];
    for write in writes {
        c.cmd(write);
    }
    c.cmd(&["HGET", "aof:type", "f"]);
    assert_eq!(c.cmd(&["LPUSH", "aof:type", "x"]).i(), 1);
    keys
}

#[test]
fn writes_after_field_deadlines_come_back_after_a_kill() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    c.cmd(&["CONFIG", "SET", "appendfsync", "always"]);
    let keys = writes_after_deadlines(&mut c);
    let before = snapshot_of(&mut c, &keys);
    wait_until("the HDELs to reach the AOF", 10, || {
        count(&server.aof(), b"HDEL") >= 1
    });
    server.kill();
    server.restart();
    let mut c = server.conn();
    assert_eq!(snapshot_of(&mut c, &keys), before);
    // The no-ops are not logged.
    let aof = server.aof();
    assert_eq!(count(&aof, b"HPERSIST"), 0, "a no-op HPERSIST was logged");
}

#[test]
fn writes_after_field_deadlines_come_back_after_a_rewrite_and_a_kill() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    c.cmd(&["CONFIG", "SET", "appendfsync", "always"]);
    let keys = writes_after_deadlines(&mut c);
    let before = snapshot_of(&mut c, &keys);
    assert!(!matches!(c.cmd(&["BGREWRITEAOF"]), V::Err(_)));
    wait_until("the rewrite to end", 30, || {
        c.info("persistence", "aof_rewrite_in_progress") == "0"
    });
    // And writes after the rewrite.
    c.cmd(&["HSET", "after", "f", "5"]);
    c.cmd(&["HPEXPIRE", "after", "20", "FIELDS", "1", "f"]);
    std::thread::sleep(Duration::from_millis(30));
    c.cmd(&["HINCRBY", "after", "f", "1"]);
    let mut keys = keys;
    keys.push("after".to_string());
    let before: Vec<String> = before
        .into_iter()
        .chain(snapshot_of(&mut c, &["after".to_string()]))
        .collect();
    server.kill();
    server.restart();
    let mut c = server.conn();
    assert_eq!(snapshot_of(&mut c, &keys), before);
}
