#![allow(clippy::disallowed_methods)]
// Long commands on io_uring, run off their connection's thread (#762).
//
// On io_uring each worker thread serves its own connections. A long command
// (`KEYS` over a million keys, the free of a hash of half a million fields
// after `DEL`) held up every other connection of its thread for as long.
// Now the work goes to a thread of its own and the connection waits for it,
// while its thread serves the others (`deferred`). The servers here run one
// worker thread, so every connection is on the long command's thread.
//
// What must still hold: the connection's replies come in order, writes reach
// a replica and the AOF in the order they ran, a client that goes, is killed,
// paused or blocked meanwhile is handled, a shutdown meanwhile ends cleanly,
// and the slow log, `INFO commandstats` and `CLIENT LIST` count the time.
// Each test runs on both I/O models where the machine has io_uring.

mod common;

use common::{restart_verified, start_verified};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

/// The tests here measure time: they take turns.
fn one_at_a_time() -> MutexGuard<'static, ()> {
    static TURN: Mutex<()> = Mutex::new(());
    TURN.lock().unwrap_or_else(|e| e.into_inner())
}

/// Elements in each big value: well past `helpers::LONG_WORK` (65,536).
const BIG: usize = 300_000;

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<V>),
    /// A RESP3 map, its keys and values one after the other.
    Map(Vec<V>),
    Nil,
}

impl V {
    fn arr(self) -> Vec<V> {
        match self {
            V::Arr(items) => items,
            other => panic!("not an array: {other:?}"),
        }
    }

    fn text(&self) -> String {
        match self {
            V::Bulk(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            V::Str(text) => text.clone(),
            other => panic!("not a string: {other:?}"),
        }
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

fn request(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream.set_nodelay(true).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(120)))
                    .unwrap();
                let writer = stream.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(stream),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn send(&mut self, args: &[&str]) {
        self.writer.write_all(&request(args)).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    fn ok(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args), V::Str("OK".into()), "{args:?}");
    }

    fn int(&mut self, args: &[&str]) -> i64 {
        match self.cmd(args) {
            V::Int(n) => n,
            other => panic!("{args:?}: {other:?}"),
        }
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        assert!(!line.is_empty(), "the connection was closed");
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "_" => V::Nil,
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Bulk(data)
            }
            "*" | "~" | ">" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Arr((0..n).map(|_| self.read()).collect())
            }
            "%" => {
                let n: usize = rest.parse().unwrap();
                V::Map((0..2 * n).map(|_| self.read()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }

    /// Whether the server closes this connection within `wait`, whatever
    /// it sends before it does.
    fn closed_within(&mut self, wait: Duration) -> bool {
        self.writer
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let deadline = Instant::now() + wait;
        let mut buf = [0u8; 4096];
        while Instant::now() < deadline {
            match self.reader.read(&mut buf) {
                Ok(0) => return true,
                Ok(_) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => return true,
            }
        }
        false
    }

    /// One field of `INFO <section>`.
    fn info(&mut self, section: &str, field: &str) -> String {
        let info = self.cmd(&["INFO", section]).text();
        info.lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")).map(str::to_string))
            .unwrap_or_default()
            .trim()
            .to_string()
    }

    /// `name` of `kind` (`HSET`, `SADD`, `RPUSH`) with `n` elements, sent a
    /// thousand at a time, pipelined.
    fn big(&mut self, kind: &str, name: &str, n: usize) {
        let mut sent = 0;
        for start in (0..n).step_by(1000) {
            let items: Vec<String> = (start..n.min(start + 1000))
                .map(|i| i.to_string())
                .collect();
            let mut args = vec![kind, name];
            for item in &items {
                args.push(item);
                if kind == "HSET" {
                    args.push("v");
                }
            }
            self.send(&args);
            sent += 1;
        }
        for _ in 0..sent {
            let reply = self.read();
            assert!(matches!(reply, V::Int(_)), "{kind} {name}: {reply:?}");
        }
    }
}

// ── Servers ─────────────────────────────────────────────────────────────────

fn io_models() -> &'static [&'static str] {
    match std::env::var("KIVIDB_TEST_IO_MODEL").as_deref() {
        Ok("tokio") => &["tokio"],
        Ok("uring") => &["uring"],
        _ if cfg!(target_os = "linux") => &["tokio", "uring"],
        _ => &["tokio"],
    }
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
    args: Vec<String>,
    env: Vec<(String, String)>,
}

fn spawn(dir: &Path, port: u16, args: &[String], env: &[(String, String)]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    Command::new(env!("CARGO_BIN_EXE_kividb"))
        .current_dir(dir)
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--configfile",
            "kividb.conf",
            "--enable-debug-command",
            "yes",
        ])
        .args(args)
        .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

impl Server {
    /// On `io`, with one worker thread: every connection is served by the
    /// thread a long command runs on.
    fn start(io: &str, extra: &[&str]) -> Self {
        Self::start_env(io, extra, &[])
    }

    /// [`Server::start`], with `env` in the server's environment.
    fn start_env(io: &str, extra: &[&str], env: &[(&str, &str)]) -> Self {
        let env: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let dir = tempfile::tempdir().unwrap().keep();
        let mut args: Vec<String> = ["--io-model", io, "--threads", "1"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        args.extend(extra.iter().map(|s| s.to_string()));
        let (child, port) = start_verified(|port| spawn(&dir, port, &args, &env));
        let server = Server {
            port,
            dir,
            child,
            args,
            env,
        };
        server.wait_up();
        if io == "uring" {
            assert!(
                server.log().contains("I/O model      : io_uring"),
                "{}",
                server.log()
            );
        }
        server
    }

    fn wait_up(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let up = TcpStream::connect(("127.0.0.1", self.port)).and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(10)))?;
                // DBSIZE, not PING: PING is answered while loading.
                s.write_all(b"*1\r\n$6\r\nDBSIZE\r\n")?;
                let mut reply = [0u8; 1];
                s.read_exact(&mut reply)?;
                Ok(reply[0] == b':')
            });
            if up.unwrap_or(false) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "kividb did not come up: {}",
                self.log()
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    /// Killed if still running, and started again on the same directory.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let (child, port, _) = restart_verified(
            |port| spawn(&self.dir, port, &self.args, &self.env),
            self.port,
        );
        self.child = child;
        self.port = port;
        self.wait_up();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn eventually(secs: u64, what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

/// Run `heavy` on its own connection while another one sends `PING` every
/// millisecond: how long it took, and the longest a `PING` sent while it
/// ran waited for its answer.
fn probe_while(port: u16, long: &mut Conn, heavy: impl FnOnce(&mut Conn)) -> (Duration, Duration) {
    let stop = Arc::new(AtomicBool::new(false));
    let prober = {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut probe = Conn::connect(port);
            let mut times = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let sent = Instant::now();
                assert_eq!(probe.cmd(&["PING"]), V::Str("PONG".into()));
                times.push((sent, sent.elapsed()));
                thread::sleep(Duration::from_millis(1));
            }
            times
        })
    };
    thread::sleep(Duration::from_millis(100));
    assert_eq!(long.cmd(&["PING"]), V::Str("PONG".into()));
    let started = Instant::now();
    heavy(long);
    let ended = Instant::now();
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    let times = prober.join().unwrap();
    let worst = times
        .iter()
        .filter(|(sent, waited)| *sent <= ended && *sent + *waited >= started)
        .map(|(_, waited)| *waited)
        .max()
        .unwrap_or_default();
    (ended - started, worst)
}

fn assert_not_held_up(io: &str, what: &str, (took, worst): (Duration, Duration)) {
    eprintln!("{io}: {what} took {took:?}, longest PING {worst:?}");
    assert!(
        took >= Duration::from_millis(20),
        "{io}: {what} took {took:?}: too short to tell a stall from noise"
    );
    assert!(
        worst < took / 2,
        "{io}: a PING on the same thread waited {worst:?} while {what} took {took:?}"
    );
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn long_commands_do_not_hold_up_the_other_connections_of_their_thread() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &[]);
        let port = server.port;
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "1000000", "user"]);
        let mut long = server.conn();

        let keys = probe_while(port, &mut long, |c| {
            assert_eq!(c.cmd(&["KEYS", "user:99*"]).arr().len(), 11_111);
        });
        assert_not_held_up(io, "KEYS", keys);

        let scan = probe_while(port, &mut long, |c| {
            let reply = c
                .cmd(&["SCAN", "0", "MATCH", "user:99*", "COUNT", "2000000"])
                .arr();
            assert_eq!(reply[0], V::Bulk(b"0".to_vec()));
            assert_eq!(reply[1].clone().arr().len(), 11_111);
        });
        assert_not_held_up(io, "SCAN", scan);
        c.ok(&["FLUSHALL"]);

        c.big("HSET", "hash", BIG);
        c.big("SADD", "set", BIG);
        c.big("RPUSH", "list", BIG);

        let hgetall = probe_while(port, &mut long, |c| {
            assert_eq!(c.cmd(&["HGETALL", "hash"]).arr().len(), 2 * BIG);
        });
        assert_not_held_up(io, "HGETALL", hgetall);
        let smembers = probe_while(port, &mut long, |c| {
            assert_eq!(c.cmd(&["SMEMBERS", "set"]).arr().len(), BIG);
        });
        assert_not_held_up(io, "SMEMBERS", smembers);
        let lrange = probe_while(port, &mut long, |c| {
            assert_eq!(c.cmd(&["LRANGE", "list", "0", "-1"]).arr().len(), BIG);
        });
        assert_not_held_up(io, "LRANGE", lrange);
        let sort = probe_while(port, &mut long, |c| {
            let sorted = c.cmd(&["SORT", "list", "DESC"]).arr();
            assert_eq!(sorted.len(), BIG);
            assert_eq!(sorted[0], V::Bulk((BIG - 1).to_string().into_bytes()));
        });
        assert_not_held_up(io, "SORT", sort);

        // Frees: one key, and several (which takes the command gate alone).
        let del = probe_while(port, &mut long, |c| {
            assert_eq!(c.int(&["DEL", "hash"]), 1);
        });
        assert_not_held_up(io, "DEL of a big hash", del);
        c.big("HSET", "hash", BIG);
        c.ok(&["SET", "plain", "x"]);
        let del_many = probe_while(port, &mut long, |c| {
            assert_eq!(c.int(&["DEL", "hash", "plain", "missing"]), 2);
        });
        assert_not_held_up(io, "DEL of several keys, one big", del_many);
        assert_eq!(c.int(&["EXISTS", "hash", "plain"]), 0);
    }
}

/// What is pipelined behind long work is run after it, and its replies come
/// after its reply; and the replies before it are not held back for it.
#[test]
fn replies_keep_their_order_around_long_work() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &[]);
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "200000", "k"]);
        c.big("HSET", "h", BIG);
        let mut pipe = Vec::new();
        for args in [
            &["SET", "a", "1"][..],
            &["KEYS", "k:19999*"],
            &["SET", "a", "2"],
            &["GET", "a"],
            &["DEL", "h"],
            &["EXISTS", "h"],
            &["HSET", "h", "f", "v"],
            &["SCAN", "0", "MATCH", "k:19999*", "COUNT", "1000000"],
            &["INCR", "n"],
            &["HGETALL", "h"],
            &["PING"],
        ] {
            pipe.extend_from_slice(&request(args));
        }
        c.writer.write_all(&pipe).unwrap();
        assert_eq!(c.read(), V::Str("OK".into()));
        assert_eq!(c.read().arr().len(), 11);
        assert_eq!(c.read(), V::Str("OK".into()));
        assert_eq!(c.read(), V::Bulk(b"2".to_vec()));
        assert_eq!(c.read(), V::Int(1));
        assert_eq!(c.read(), V::Int(0));
        assert_eq!(c.read(), V::Int(1));
        let scan = c.read().arr();
        assert_eq!(scan[0], V::Bulk(b"0".to_vec()), "{io}");
        assert_eq!(scan[1].clone().arr().len(), 11);
        assert_eq!(c.read(), V::Int(1));
        assert_eq!(
            c.read().arr(),
            vec![V::Bulk(b"f".to_vec()), V::Bulk(b"v".to_vec())]
        );
        assert_eq!(c.read(), V::Str("PONG".into()));

        // A reply in RESP3 is what the connection speaks, run anywhere.
        c.big("HSET", "h3", BIG);
        assert!(matches!(c.cmd(&["HELLO", "3"]), V::Map(_)));
        let V::Map(fields) = c.cmd(&["HGETALL", "h3"]) else {
            panic!("{io}: HGETALL in RESP3 is a map");
        };
        assert_eq!(fields.len(), 2 * BIG);
    }
}

/// The writes around a free that waits reach the replica, and the AOF, in
/// the order they ran: a `DEL` of a big value, then a write of the same key.
#[test]
fn writes_around_long_work_reach_the_replica_and_the_aof_in_order() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let mut master = Server::start(io, &["--aof"]);
        let port = master.port.to_string();
        let replica = Server::start(io, &["--replicaof", "127.0.0.1", &port]);
        let mut r = replica.conn();
        eventually(30, "the replica to connect", || {
            r.info("replication", "master_link_status") == "up"
        });
        let mut c = master.conn();
        // Each write on disk before its reply: the restart kills the server.
        c.ok(&["CONFIG", "SET", "appendfsync", "always"]);
        for round in 0..3 {
            c.int(&["DEL", "k", "s"]);
            c.big("HSET", "k", BIG);
            c.big("SADD", "s", BIG);
            let mut pipe = Vec::new();
            pipe.extend_from_slice(&request(&["DEL", "k"]));
            pipe.extend_from_slice(&request(&["SET", "k", &format!("after-{round}")]));
            pipe.extend_from_slice(&request(&["SET", "s", "string"]));
            pipe.extend_from_slice(&request(&["DEL", "s"]));
            pipe.extend_from_slice(&request(&["RPUSH", "s", "a"]));
            c.writer.write_all(&pipe).unwrap();
            assert_eq!(c.read(), V::Int(1));
            assert_eq!(c.read(), V::Str("OK".into()));
            assert_eq!(c.read(), V::Str("OK".into()));
            assert_eq!(c.read(), V::Int(1));
            assert_eq!(c.read(), V::Int(1));
        }
        let want = (V::Bulk(b"after-2".to_vec()), vec![V::Bulk(b"a".to_vec())]);
        assert_eq!(c.cmd(&["GET", "k"]), want.0);
        eventually(30, "the replica to follow", || {
            r.cmd(&["GET", "k"]) == want.0
                && r.cmd(&["LRANGE", "s", "0", "-1"]) == V::Arr(want.1.clone())
        });
        assert_eq!(r.int(&["DBSIZE"]), 2, "{io}");
        drop(r);
        drop(replica);

        drop(c);
        master.restart();
        let mut c = master.conn();
        assert_eq!(
            c.cmd(&["GET", "k"]),
            want.0,
            "{io}: after replaying the AOF"
        );
        assert_eq!(c.cmd(&["LRANGE", "s", "0", "-1"]).arr(), want.1);
        assert_eq!(c.int(&["DBSIZE"]), 2);
    }
}

/// A client that goes, or is killed, while its command runs elsewhere: the
/// work is done, nothing is sent, the server and the other connections go on.
#[test]
fn a_client_that_goes_or_is_killed_while_its_command_runs_elsewhere() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &[]);
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "1000000", "user"]);
        let clients = |c: &mut Conn| c.info("clients", "connected_clients");
        eventually(30, "one client", || clients(&mut c) == "1");

        // Gone before the reply.
        for _ in 0..3 {
            let mut gone = server.conn();
            gone.send(&["KEYS", "user:9*"]);
            thread::sleep(Duration::from_millis(5));
            drop(gone);
        }
        c.big("HSET", "h", BIG);
        let mut gone = server.conn();
        gone.send(&["DEL", "h"]);
        thread::sleep(Duration::from_millis(5));
        drop(gone);
        eventually(30, "the clients that went to be gone", || {
            clients(&mut c) == "1"
        });
        assert_eq!(c.int(&["EXISTS", "h"]), 0, "{io}: the DEL ran");

        // Killed.
        let mut killed = server.conn();
        let id = killed.int(&["CLIENT", "ID"]).to_string();
        killed.send(&["KEYS", "user:9*"]);
        thread::sleep(Duration::from_millis(5));
        assert_eq!(c.int(&["CLIENT", "KILL", "ID", &id]), 1, "{io}");
        assert!(
            killed.closed_within(Duration::from_secs(30)),
            "{io}: killed"
        );
        eventually(30, "the killed client to be gone", || {
            clients(&mut c) == "1"
        });
        assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        assert_eq!(c.cmd(&["KEYS", "user:99999*"]).arr().len(), 11);
    }
}

/// A pause holds what it holds whatever runs elsewhere, and a blocked client
/// on the same thread is woken while a long command runs.
#[test]
fn paused_and_blocked_clients_around_long_work() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &[]);
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "1000000", "user"]);
        c.big("HSET", "h", BIG);

        // Writes paused: a long read runs; a DEL waits for the end of the
        // pause, and its free after it.
        let mut other = server.conn();
        let paused = Instant::now();
        c.ok(&["CLIENT", "PAUSE", "5000", "WRITE"]);
        assert_eq!(other.cmd(&["KEYS", "user:99999*"]).arr().len(), 11);
        assert!(
            paused.elapsed() < Duration::from_millis(4500),
            "{io}: KEYS held by the pause"
        );
        assert_eq!(other.int(&["DEL", "h"]), 1);
        assert!(
            paused.elapsed() >= Duration::from_millis(4900),
            "{io}: DEL held by the pause"
        );
        assert_eq!(c.int(&["EXISTS", "h"]), 0);

        // A client blocked on the thread is woken, and answered, while a
        // long KEYS runs.
        let mut blocked = server.conn();
        blocked.send(&["BLPOP", "q", "0"]);
        thread::sleep(Duration::from_millis(50));
        let mut long = server.conn();
        long.send(&["KEYS", "user:*"]);
        thread::sleep(Duration::from_millis(2));
        let pushed = Instant::now();
        assert_eq!(c.int(&["LPUSH", "q", "x"]), 1);
        assert_eq!(
            blocked.read().arr(),
            vec![V::Bulk(b"q".to_vec()), V::Bulk(b"x".to_vec())]
        );
        let woke = pushed.elapsed();
        let keys = long.read().arr();
        let took = pushed.elapsed();
        assert_eq!(keys.len(), 1_000_000);
        eprintln!("{io}: BLPOP answered {woke:?} after the push, KEYS {took:?}");
        assert!(woke < took / 2, "{io}: the blocked client waited for KEYS");
    }
}

/// The slow log, `INFO commandstats` and `CLIENT LIST` count what ran
/// elsewhere, once; `MONITOR` shows it once.
#[test]
fn stats_count_the_time_spent_elsewhere() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &[]);
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "1000000", "user"]);
        c.big("HSET", "h", BIG);
        c.ok(&["CONFIG", "SET", "slowlog-log-slower-than", "10000"]);
        c.ok(&["SLOWLOG", "RESET"]);
        c.ok(&["CONFIG", "RESETSTAT"]);
        let mut monitor = server.conn();
        monitor.ok(&["MONITOR"]);

        let mut long = server.conn();
        let id = long.int(&["CLIENT", "ID"]).to_string();
        let started = Instant::now();
        long.send(&["KEYS", "user:*"]);
        // While it runs, `CLIENT LIST` is answered, and has it as the
        // command it is running: not yet among the commands it has run.
        let listed = |c: &mut Conn| c.cmd(&["CLIENT", "LIST", "ID", &id]).text().to_lowercase();
        let mut seen_running = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && !seen_running {
            let list = listed(&mut c);
            seen_running = list.contains("cmd=keys") && list.contains(" tot-cmds=1 ");
        }
        assert_eq!(long.read().arr().len(), 1_000_000);
        let keys_took = started.elapsed();
        assert!(
            seen_running,
            "{io}: CLIENT LIST while KEYS ran: {}",
            listed(&mut c)
        );
        eventually(10, "KEYS to be counted", || {
            listed(&mut c).contains(" tot-cmds=2 ")
        });
        assert_eq!(long.int(&["DEL", "h"]), 1);

        let stats = c.cmd(&["INFO", "commandstats"]).text();
        let line = |name: &str| {
            stats
                .lines()
                .find(|line| line.starts_with(&format!("cmdstat_{name}:")))
                .unwrap_or_else(|| panic!("{io}: no {name} in {stats}"))
                .to_string()
        };
        let field = |line: &str, field: &str| -> u64 {
            line.split([':', ','])
                .find_map(|kv| kv.strip_prefix(&format!("{field}=")))
                .unwrap()
                .parse::<f64>()
                .unwrap() as u64
        };
        let keys = line("keys");
        assert_eq!(field(&keys, "calls"), 1, "{io}: {keys}");
        assert!(
            field(&keys, "usec") >= 10_000,
            "{io}: {keys} ({keys_took:?})"
        );
        let del = line("del");
        assert_eq!(field(&del, "calls"), 1, "{io}: {del}");
        assert!(
            field(&del, "usec") >= 5_000,
            "{io}: DEL timed with its free: {del}"
        );

        let slow = c.cmd(&["SLOWLOG", "GET", "10"]).arr();
        let logged: Vec<String> = slow
            .iter()
            .map(|entry| {
                let V::Arr(fields) = entry else {
                    panic!("{entry:?}")
                };
                fields[3].clone().arr()[0].text().to_ascii_lowercase()
            })
            .collect();
        assert_eq!(
            logged.iter().filter(|n| *n == "keys").count(),
            1,
            "{io}: {logged:?}"
        );
        assert_eq!(
            logged.iter().filter(|n| *n == "del").count(),
            1,
            "{io}: {logged:?}"
        );

        // MONITOR: each once.
        let mut shown = Vec::new();
        monitor
            .writer
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        while shown.len() < 2 {
            let V::Str(line) = monitor.read() else {
                continue;
            };
            if line.contains("\"KEYS\"") || line.contains("\"DEL\"") {
                shown.push(line);
            }
        }
        assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        monitor
            .writer
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut byte = [0u8; 1];
        let mut rest = Vec::new();
        while let Ok(n) = monitor.reader.read(&mut byte) {
            if n == 0 {
                break;
            }
            rest.push(byte[0]);
        }
        let rest = String::from_utf8_lossy(&rest);
        assert!(
            !rest.contains("\"KEYS\"") && !rest.contains("\"DEL\""),
            "{io}: shown twice: {shown:?} then {rest}"
        );
    }
}

/// Inside `MULTI`/`EXEC` long work runs where it is, with the rest of the
/// transaction.
#[test]
fn a_transaction_runs_long_work_where_it_is() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &[]);
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "200000", "k"]);
        c.big("HSET", "h", BIG);
        c.ok(&["MULTI"]);
        for args in [
            &["KEYS", "k:19999*"][..],
            &["DEL", "h"],
            &["SET", "h", "x"],
            &["HGETALL", "h"],
        ] {
            assert_eq!(c.cmd(args), V::Str("QUEUED".into()));
        }
        let replies = c.cmd(&["EXEC"]).arr();
        assert_eq!(replies[0].clone().arr().len(), 11, "{io}");
        assert_eq!(replies[1], V::Int(1));
        assert_eq!(replies[2], V::Str("OK".into()));
        assert!(matches!(&replies[3], V::Err(e) if e.starts_with("WRONGTYPE")));
    }
}

/// A shutdown while a command runs elsewhere ends the server cleanly, with
/// what that command did, and what came before it, in the final save.
#[test]
fn a_shutdown_while_a_command_runs_elsewhere() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let mut server = Server::start(io, &[]);
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "1000000", "user"]);
        c.big("HSET", "h", BIG);
        c.ok(&["SET", "kept", "yes"]);
        let mut long = server.conn();
        long.send(&["DEL", "h"]);
        let mut walk = server.conn();
        walk.send(&["KEYS", "user:*"]);
        thread::sleep(Duration::from_millis(2));
        let mut s = server.conn();
        s.send(&["SHUTDOWN", "SAVE"]);
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "{io}: did not shut down: {}",
                server.log()
            );
            thread::sleep(Duration::from_millis(20));
        };
        assert!(status.success(), "{io}: {status:?}: {}", server.log());
        server.restart();
        let mut c = server.conn();
        assert_eq!(
            c.int(&["EXISTS", "h"]),
            0,
            "{io}: the DEL before the shutdown"
        );
        assert_eq!(c.cmd(&["GET", "kept"]), V::Bulk(b"yes".to_vec()));
        assert_eq!(c.int(&["DBSIZE"]), 1_000_001);
    }
}

/// A client that sends its commands and shuts its side for writing (`nc
/// -N`, a batch loader) is answered all of them, long work or not, and what
/// it sent after the long command runs: as when the command ran on its
/// connection's thread.
#[test]
fn a_client_that_shuts_its_side_for_writing_gets_every_reply() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &[]);
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "1000000", "user"]);
        for (round, long) in [
            &["DEL", "h"][..],
            &["KEYS", "user:99999*"],
            &["HGETALL", "h"],
        ]
        .into_iter()
        .enumerate()
        {
            c.big("HSET", "h", BIG);
            let key = format!("after-{round}");
            let mut batch = request(long);
            batch.extend_from_slice(&request(&["SET", &key, "v"]));
            batch.extend_from_slice(&request(&["GET", &key]));
            let mut half = server.conn();
            half.writer.write_all(&batch).unwrap();
            half.writer.shutdown(std::net::Shutdown::Write).unwrap();
            let first = half.read();
            match long[0] {
                "DEL" => assert_eq!(first, V::Int(1), "{io}"),
                "KEYS" => assert_eq!(first.arr().len(), 11, "{io}"),
                _ => assert_eq!(first.arr().len(), 2 * BIG, "{io}"),
            }
            assert_eq!(half.read(), V::Str("OK".into()), "{io}: {long:?}");
            assert_eq!(half.read(), V::Bulk(b"v".to_vec()), "{io}: {long:?}");
            assert!(
                half.closed_within(Duration::from_secs(30)),
                "{io}: closed after"
            );
            assert_eq!(c.cmd(&["GET", &key]), V::Bulk(b"v".to_vec()), "{io}");
        }
    }
}

/// While a `DEL`'s free waits, its value is not counted against
/// `maxmemory`: at the limit, writes on another connection evict nothing
/// and are refused nothing for memory that is already gone. The free is
/// held (`deferred-free-held`) until the writes are done.
#[test]
fn a_free_that_waits_is_not_counted_against_the_limit() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start_env(
            io,
            &["--maxmemory-policy", "allkeys-lru"],
            &[("KIVIDB_TEST_FAILPOINTS", "deferred-free-held")],
        );
        let mut c = server.conn();
        c.ok(&["DEBUG", "POPULATE", "100000", "k", "100"]);
        c.big("HSET", "h", BIG);
        let usage = c.int(&["MEMORY", "USAGE", "h"]);
        let used: i64 = c.info("memory", "used_memory").parse().unwrap();
        let mut del = server.conn();
        del.send(&["DEL", "h"]);
        let held = server.dir.join("failpoint.deferred-free-held.1");
        eventually(30, "the free to be held", || held.exists());
        // Under what was used with the hash, over what is used without it.
        let limit = used - usage / 2;
        c.ok(&["CONFIG", "SET", "maxmemory", &limit.to_string()]);
        let value = "w".repeat(100);
        for i in 0..2_000 {
            let reply = c.cmd(&["SET", &format!("k:{}", i * 37 % 100_000), &value]);
            assert_eq!(reply, V::Str("OK".into()), "{io}: write {i} refused");
        }
        let evicted = c.info("stats", "evicted_keys");
        assert_eq!(
            evicted, "0",
            "{io}: keys evicted for a value already deleted"
        );
        assert_eq!(c.int(&["DBSIZE"]), 100_000, "{io}");
        std::fs::remove_file(&held).unwrap();
        assert_eq!(del.read(), V::Int(1), "{io}");
        c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
    }
}

/// What tracking tells a client of the keys its own command evicted, before
/// that command parks (`BLPOP`) or stops to be run again (a long
/// `HGETALL`), is delivered to that client before its reply, and nothing of
/// it to another client of the same thread.
#[test]
fn invalidations_go_to_the_client_whose_command_waits() {
    let _turn = one_at_a_time();
    for &io in io_models() {
        let server = Server::start(io, &["--maxmemory-policy", "volatile-lru"]);
        let mut c = server.conn();
        c.big("HSET", "h", BIG);
        let invalidated = |v: &V| {
            *v == V::Arr(vec![
                V::Bulk(b"invalidate".to_vec()),
                V::Arr(vec![V::Bulk(b"x".to_vec())]),
            ])
        };
        for waits in ["HGETALL", "BLPOP"] {
            // `x` is the only key with a time to live: the one evicted, and
            // what evicting it gives back is enough for a write to go on.
            let x = "x".repeat(400_000);
            c.ok(&["SET", "x", &x, "EX", "100000"]);
            let mut a = server.conn();
            assert!(matches!(a.cmd(&["HELLO", "3"]), V::Map(_)));
            a.ok(&["CLIENT", "TRACKING", "ON"]);
            assert_eq!(a.cmd(&["GET", "x"]), V::Bulk(x.into_bytes()));
            let mut b = server.conn();
            assert_eq!(b.cmd(&["GET", "nothere"]), V::Nil);
            // A limit under what is in use, set while nothing is evicted
            // for it: the next command that runs makes room, by evicting
            // `x`. That is `a`'s, and its connection is the one told.
            let used: i64 = c.info("memory", "used_memory").parse().unwrap();
            c.ok(&["CONFIG", "SET", "maxmemory-policy", "noeviction"]);
            c.ok(&["CONFIG", "SET", "maxmemory", &(used - 65_536).to_string()]);
            c.ok(&["CONFIG", "SET", "maxmemory-policy", "volatile-lru"]);
            if waits == "HGETALL" {
                a.send(&["HGETALL", "h"]);
            } else {
                a.send(&["BLPOP", "q", "0"]);
            }
            thread::sleep(Duration::from_millis(20));
            // Another client of the thread, meanwhile: its reply alone.
            for _ in 0..3 {
                assert_eq!(b.cmd(&["GET", "nothere"]), V::Nil, "{io} {waits}");
            }
            c.ok(&["CONFIG", "SET", "maxmemory", "0"]);
            let first = a.read();
            assert!(invalidated(&first), "{io} {waits}: {first:?}");
            if waits == "HGETALL" {
                let V::Map(fields) = a.read() else {
                    panic!("{io}: HGETALL in RESP3 is a map");
                };
                assert_eq!(fields.len(), 2 * BIG);
            } else {
                assert_eq!(c.int(&["LPUSH", "q", "v"]), 1);
                assert_eq!(
                    a.read().arr(),
                    vec![V::Bulk(b"q".to_vec()), V::Bulk(b"v".to_vec())],
                    "{io}"
                );
            }
            assert_eq!(b.cmd(&["GET", "nothere"]), V::Nil, "{io} {waits}");
            assert_eq!(c.int(&["EXISTS", "x"]), 0, "{io} {waits}: x was evicted");
        }
    }
}
