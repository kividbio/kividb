// tests/tag_wait.rs
//
// A client's command that finds a script on its hash tag waits for it off its
// connection's thread (#882): the thread goes on serving its other
// connections, and the command runs as soon as the script lets the tag go,
// in the order the commands came, with what was pipelined behind it waiting
// with it. Real `kividb` processes (debug builds): a script is held part-way
// through by `KIVIDB_TEST_SCRIPT_STALL_MS` (`pause::test_stall_in_script`),
// and not by a loop sized to the machine. On both I/O loops.
#![allow(clippy::disallowed_methods)]
#![cfg(feature = "lua")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::start_verified;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

fn encode(args: &[&str]) -> Vec<u8> {
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
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
                s.set_nodelay(true).unwrap();
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

    fn send(&mut self, args: &[&str]) {
        self.writer.write_all(&encode(args)).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        let read = self.reader.read_line(&mut line).expect("reply");
        assert!(read > 0, "connection closed");
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

    fn info(&mut self, section: &str, field: &str) -> String {
        let V::Str(text) = self.cmd(&["INFO", section]) else {
            panic!("INFO did not answer with text")
        };
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_default()
            .trim()
            .to_string()
    }
}

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
}

/// How long a script is held where it calls `ECHO script-stall:...`.
const STALL_MS: u64 = 800;

impl Server {
    fn start(io: &str, threads: u32) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let (child, port) = start_verified(|port| {
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("server.log"))
                .unwrap();
            Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(&dir)
                .env("KIVIDB_TEST_SCRIPT_STALL_MS", STALL_MS.to_string())
                .args([
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--threads",
                    &threads.to_string(),
                    "--update-check",
                    "no",
                    "--io-model",
                    io,
                    "--lua-parallel-scripts",
                    "true",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .expect("could not start kividb")
        });
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// Whether a script is held where it calls `ECHO script-stall:...` now
    /// (`pause::test_stall_in_script` keeps a file there meanwhile).
    fn holding(&self) -> bool {
        self.dir.join("script-stall.held").exists()
    }

    /// Waits until a script is held there.
    fn wait_until_held(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.holding() {
            assert!(
                Instant::now() < deadline,
                "the script never got to its stall"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Writes `KEYS[1]`, is held `STALL_MS` when `ARGV[2]` says so, and writes
/// it again: what a client of its tag must never see is the first write.
const WRITES_TWICE: &str = "redis.call('SET', KEYS[1], ARGV[1] .. ':half') \
     redis.call('ECHO', ARGV[2]) \
     redis.call('SET', KEYS[1], ARGV[1]) return 1";

/// Loads the script and runs it quickly a few times on `tag`'s key, so that
/// the server runs it beside others from then on.
fn quick_script(c: &mut Conn, tags: &[&str]) -> String {
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", WRITES_TWICE]) else {
        panic!("no digest")
    };
    for tag in tags {
        let key = format!("{{{tag}}}:k");
        for _ in 0..3 {
            assert_eq!(
                c.cmd(&["EVALSHA", &sha, "1", &key, "warm", "no-stall"]),
                V::Int(1)
            );
        }
    }
    sha
}

/// Starts the held script on `{tag}:k`, writing `value`, on a connection of
/// its own, and waits until it is under way.
fn start_held_script(
    server: &Server,
    sha: &str,
    tag: &str,
    value: &str,
) -> std::thread::JoinHandle<(V, Instant)> {
    let mut c = server.conn();
    let key = format!("{{{tag}}}:k");
    c.send(&["EVALSHA", sha, "1", &key, value, "script-stall:1"]);
    let handle = std::thread::spawn(move || {
        let reply = c.read();
        (reply, Instant::now())
    });
    server.wait_until_held();
    handle
}

/// A new connection on a thread that is not busy running a held script:
/// the first of several to answer a `PING`, within a quarter of the stall.
/// (On the io_uring loop the kernel places connections; on Tokio a
/// connection whose thread is busy waits for it too.)
fn free_conn(server: &Server) -> Conn {
    try_free_conn(server).expect("no new connection was answered at once")
}

/// [`free_conn`], or `None` if no connection was answered at once: every
/// thread is held, as by a script that runs alone. (A script whose run took
/// over five milliseconds runs alone next time: a held run does, and so can
/// a quick one on a loaded machine.)
fn try_free_conn(server: &Server) -> Option<Conn> {
    let asked = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    for _ in 0..8 {
        let mut c = server.conn();
        let tx = tx.clone();
        std::thread::spawn(move || {
            if c.cmd(&["PING"]) == V::Str("PONG".into()) {
                let _ = tx.send(c);
            }
        });
    }
    let c = rx.recv_timeout(Duration::from_millis(STALL_MS / 4)).ok()?;
    (asked.elapsed() < Duration::from_millis(STALL_MS / 4)).then_some(c)
}

/// `args`' reply on whichever of several new connections answers first,
/// which is not one on the thread busy running a held script; within a
/// quarter of the stall.
fn answered_at_once(server: &Server, args: &[&str]) -> V {
    let asked = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    for _ in 0..8 {
        let mut c = server.conn();
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        let tx = tx.clone();
        std::thread::spawn(move || {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let _ = tx.send(c.cmd(&args));
        });
    }
    let reply = rx.recv().unwrap();
    assert!(
        asked.elapsed() < Duration::from_millis(STALL_MS / 4),
        "no connection was answered at once"
    );
    reply
}

/// A connection's command on the script's tag waits for the script, and the
/// thread it is on goes on serving connections of other keys meanwhile.
/// Readers of the tag on every thread (many connections, spread by the
/// kernel or the runtime), then clients of a key nobody holds: those not on
/// the script's own thread — which is busy running it — are answered at
/// once. Before #882 every thread had a reader waiting on the tag's lock,
/// and none of them was answered before the script ended.
fn a_waiting_command_leaves_its_thread_free(io: &str) {
    let server = Server::start(io, 2);
    let mut c = server.conn();
    let sha = quick_script(&mut c, &["a"]);
    assert_eq!(c.cmd(&["SET", "plain", "v"]), V::Str("OK".into()));
    let script = start_held_script(&server, &sha, "a", "after");

    let mut readers: Vec<Conn> = (0..10).map(|_| server.conn()).collect();
    for reader in &mut readers {
        reader.send(&["GET", "{a}:k"]);
    }
    std::thread::sleep(Duration::from_millis(50));
    let others: Vec<std::thread::JoinHandle<Duration>> = (0..10)
        .map(|_| {
            let mut other = server.conn();
            std::thread::spawn(move || {
                let asked = Instant::now();
                assert_eq!(other.cmd(&["GET", "plain"]), V::Str("v".into()));
                asked.elapsed()
            })
        })
        .collect();
    let took: Vec<Duration> = others.into_iter().map(|t| t.join().unwrap()).collect();
    let (reply, script_done) = script.join().unwrap();
    assert_eq!(reply, V::Int(1));
    for reader in &mut readers {
        // After the script, whole: never its first write.
        assert_eq!(reader.read(), V::Str("after".into()));
    }
    let quick = took
        .iter()
        .filter(|t| **t < Duration::from_millis(STALL_MS / 4))
        .count();
    assert!(
        quick >= 1,
        "no client of another key was answered while the script held its tag: {took:?}"
    );
    assert!(script_done.elapsed() < Duration::from_secs(30));
}

#[test]
fn a_waiting_command_leaves_its_thread_free_tokio() {
    a_waiting_command_leaves_its_thread_free("tokio");
}

#[test]
fn a_waiting_command_leaves_its_thread_free_uring() {
    a_waiting_command_leaves_its_thread_free("uring");
}

/// A command naming keys of two tags, one held by the script, holds neither
/// while it waits: a client of the other tag is answered at once. And the
/// command, a pipeline behind it, and commands of the same tag sent later by
/// other clients run after the script, in the order they came; the replies
/// of what was pipelined before it are not held back.
fn waits_hold_nothing_and_keep_their_order(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    let sha = quick_script(&mut c, &["a"]);
    assert_eq!(c.cmd(&["SET", "{b}:k", "b0"]), V::Str("OK".into()));
    let script = start_held_script(&server, &sha, "a", "after");

    // Before the waiting command, the waiting command, and two behind it.
    let mut piped = free_conn(&server);
    let mut batch = Vec::new();
    for args in [
        &["SET", "plain:before", "1"][..],
        &["MGET", "{b}:k", "{a}:k"],
        &["SET", "plain:behind", "1"],
    ] {
        batch.extend_from_slice(&encode(args));
    }
    piped.writer.write_all(&batch).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    // The reply before it is out, and nothing behind it has run.
    assert_eq!(piped.read(), V::Str("OK".into()));
    // Asked on connections that are not on the script's own thread, which
    // is busy running it: the first answer is from one of those.
    assert_eq!(
        answered_at_once(&server, &["EXISTS", "plain:before"]),
        V::Int(1)
    );
    assert_eq!(
        answered_at_once(&server, &["EXISTS", "plain:behind"]),
        V::Int(0)
    );
    let mut look = server.conn();
    // Two other clients of the same tag, one after the other.
    let mut first = free_conn(&server);
    first.send(&["SET", "{a}:k", "first"]);
    std::thread::sleep(Duration::from_millis(50));
    let mut later = free_conn(&server);
    later.send(&["SET", "{a}:k", "second"]);
    std::thread::sleep(Duration::from_millis(50));

    // The other tag of the waiting MGET is free: several clients, so that
    // one is on a thread that is not running the script.
    let others: Vec<std::thread::JoinHandle<Duration>> = (0..6)
        .map(|i| {
            let mut other = server.conn();
            std::thread::spawn(move || {
                let asked = Instant::now();
                let value = format!("b{}", i + 1);
                assert_eq!(other.cmd(&["SET", "{b}:k2", &value]), V::Str("OK".into()));
                asked.elapsed()
            })
        })
        .collect();
    let took: Vec<Duration> = others.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(
        took.iter()
            .any(|t| *t < Duration::from_millis(STALL_MS / 4)),
        "a write to the other tag of a waiting command waited: {took:?}"
    );

    let (reply, _) = script.join().unwrap();
    assert_eq!(reply, V::Int(1));
    assert_eq!(
        piped.read(),
        V::Arr(vec![V::Str("b0".into()), V::Str("after".into())])
    );
    assert_eq!(piped.read(), V::Str("OK".into()));
    assert_eq!(first.read(), V::Str("OK".into()));
    assert_eq!(later.read(), V::Str("OK".into()));
    // In the order they came.
    assert_eq!(look.cmd(&["GET", "{a}:k"]), V::Str("second".into()));
    assert_eq!(look.cmd(&["EXISTS", "plain:behind"]), V::Int(1));
}

#[test]
fn waits_hold_nothing_and_keep_their_order_tokio() {
    waits_hold_nothing_and_keep_their_order("tokio");
}

#[test]
fn waits_hold_nothing_and_keep_their_order_uring() {
    waits_hold_nothing_and_keep_their_order("uring");
}

/// A client that goes, or is killed, while its command waits for a script
/// leaves nothing behind: the tag is free once the script is done, the
/// script runs again, and no client is counted as blocked.
fn a_client_that_goes_while_it_waits_leaves_nothing(io: &str) {
    let server = Server::start(io, 2);
    let mut c = server.conn();
    let sha = quick_script(&mut c, &["a"]);
    let script = start_held_script(&server, &sha, "a", "after");

    let mut closed = free_conn(&server);
    closed.send(&["SET", "{a}:k", "from-a-closed-client"]);
    let mut killed = free_conn(&server);
    let V::Int(id) = killed.cmd(&["CLIENT", "ID"]) else {
        panic!("CLIENT ID")
    };
    // `CLIENT ID` first: it runs at once.
    killed.send(&["SET", "{a}:k", "from-a-killed-client"]);
    std::thread::sleep(Duration::from_millis(50));
    drop(closed);
    assert_eq!(
        free_conn(&server).cmd(&["CLIENT", "KILL", "ID", &id.to_string()]),
        V::Int(1)
    );
    let (reply, _) = script.join().unwrap();
    assert_eq!(reply, V::Int(1));

    // Whatever the gone clients' writes did — a write sent before its
    // client went may run, as in Redis — the tag is free now.
    let asked = Instant::now();
    assert_eq!(c.cmd(&["SET", "{a}:k", "free"]), V::Str("OK".into()));
    assert!(asked.elapsed() < Duration::from_secs(2));
    assert_eq!(
        c.cmd(&["EVALSHA", &sha, "1", "{a}:k", "again", "no-stall"]),
        V::Int(1)
    );
    assert_eq!(c.cmd(&["GET", "{a}:k"]), V::Str("again".into()));
    assert_eq!(c.info("clients", "blocked_clients"), "0");
}

#[test]
fn a_client_that_goes_while_it_waits_leaves_nothing_tokio() {
    a_client_that_goes_while_it_waits_leaves_nothing("tokio");
}

#[test]
fn a_client_that_goes_while_it_waits_leaves_nothing_uring() {
    a_client_that_goes_while_it_waits_leaves_nothing("uring");
}

/// A blocking pop woken by the script that holds its tag is resumed only
/// once the script is done, with what it was woken for: the resume waits
/// for the tag as a command does, keeping its delivery.
fn a_woken_blocking_pop_waits_for_the_script(io: &str) {
    let server = Server::start(io, 2);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&[
        "SCRIPT",
        "LOAD",
        "redis.call('RPUSH', KEYS[1], ARGV[1]) redis.call('ECHO', ARGV[2]) \
         redis.call('SET', KEYS[2], ARGV[1]) return 1",
    ]) else {
        panic!("no digest")
    };
    for _ in 0..3 {
        assert_eq!(
            c.cmd(&["EVALSHA", &sha, "2", "{q}:l", "{q}:k", "warm", "no-stall"]),
            V::Int(1)
        );
    }
    assert_eq!(c.cmd(&["DEL", "{q}:l"]), V::Int(1));
    let mut popper = server.conn();
    popper.send(&["BLPOP", "{q}:l", "0"]);
    std::thread::sleep(Duration::from_millis(100));
    let mut runner = server.conn();
    runner.send(&[
        "EVALSHA",
        &sha,
        "2",
        "{q}:l",
        "{q}:k",
        "job",
        "script-stall:1",
    ]);
    assert_eq!(runner.read(), V::Int(1));
    assert_eq!(
        popper.read(),
        V::Arr(vec![V::Str("{q}:l".into()), V::Str("job".into())])
    );
    assert_eq!(c.cmd(&["LLEN", "{q}:l"]), V::Int(0));
    assert_eq!(c.info("clients", "blocked_clients"), "0");
}

#[test]
fn a_woken_blocking_pop_waits_for_the_script_tokio() {
    a_woken_blocking_pop_waits_for_the_script("tokio");
}

#[test]
fn a_woken_blocking_pop_waits_for_the_script_uring() {
    a_woken_blocking_pop_waits_for_the_script("uring");
}

/// A write that waited for a script and is then held by a `CLIENT PAUSE` set
/// meanwhile runs once the pause ends, not before. A round counts only if
/// the script ran beside others and the pause was sent while it was still
/// held (a loaded machine may have been slower than the stall, or have
/// timed a warm-up run over the five milliseconds that make a script run
/// alone); up to five rounds.
fn a_waiting_write_is_then_held_by_a_pause(io: &str) {
    let server = Server::start(io, 2);
    let mut c = server.conn();
    for _round in 0..5 {
        // Quick again: a held run is a slow one, which runs alone next time.
        let sha = quick_script(&mut c, &["a"]);
        let script = start_held_script(&server, &sha, "a", "after");
        let Some(mut writer) = try_free_conn(&server) else {
            // The script ran alone.
            assert_eq!(script.join().unwrap().0, V::Int(1));
            continue;
        };
        writer.send(&["SET", "{a}:k", "paused"]);
        std::thread::sleep(Duration::from_millis(50));
        let Some(mut pauser) = try_free_conn(&server) else {
            assert_eq!(script.join().unwrap().0, V::Int(1));
            assert_eq!(writer.read(), V::Str("OK".into()));
            continue;
        };
        let paused_at = Instant::now();
        // Sent while the script is held: the pause itself waits for the
        // script, and is set before the waiting write runs again.
        pauser.send(&["CLIENT", "PAUSE", &(STALL_MS * 2).to_string(), "WRITE"]);
        let in_time = !script.is_finished() && server.holding();
        assert_eq!(pauser.read(), V::Str("OK".into()));
        let (reply, _) = script.join().unwrap();
        assert_eq!(reply, V::Int(1));
        if !in_time {
            // The script ended before the pause: the write may have run
            // before it. Again.
            assert_eq!(pauser.cmd(&["CLIENT", "UNPAUSE"]), V::Str("OK".into()));
            assert_eq!(writer.read(), V::Str("OK".into()));
            continue;
        }
        assert_eq!(writer.read(), V::Str("OK".into()));
        assert!(
            paused_at.elapsed() >= Duration::from_millis(STALL_MS * 2 - 50),
            "the write ran during the pause: {:?}",
            paused_at.elapsed()
        );
        assert_eq!(c.cmd(&["GET", "{a}:k"]), V::Str("paused".into()));
        return;
    }
    panic!("the pause never came while the script was held");
}

#[test]
fn a_waiting_write_is_then_held_by_a_pause_tokio() {
    a_waiting_write_is_then_held_by_a_pause("tokio");
}

#[test]
fn a_waiting_write_is_then_held_by_a_pause_uring() {
    a_waiting_write_is_then_held_by_a_pause("uring");
}

/// Of new connections, the first `want` to answer a `PING` within a quarter
/// of the stall while a script is held: connections on threads that are
/// not running it.
fn free_conns(server: &Server, want: usize) -> Vec<Conn> {
    let asked = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    for _ in 0..want * 3 {
        let mut c = server.conn();
        let tx = tx.clone();
        std::thread::spawn(move || {
            if c.cmd(&["PING"]) == V::Str("PONG".into()) {
                let _ = tx.send(c);
            }
        });
    }
    let deadline = asked + Duration::from_millis(STALL_MS / 4);
    let mut free = Vec::new();
    while free.len() < want {
        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(c) => free.push(c),
            Err(_) => break,
        }
    }
    free
}

/// Sends `before`, `MULTI`, `queued`, `EXEC` and `behind` in one write, and
/// reads the replies up to the `EXEC`'s: `before`'s, `+OK` and a `+QUEUED`
/// for each queued command.
fn send_exec(c: &mut Conn, before: &[&[&str]], queued: &[&[&str]], behind: &[&[&str]]) {
    let mut batch = Vec::new();
    for args in before {
        batch.extend_from_slice(&encode(args));
    }
    batch.extend_from_slice(&encode(&["MULTI"]));
    for args in queued {
        batch.extend_from_slice(&encode(args));
    }
    batch.extend_from_slice(&encode(&["EXEC"]));
    for args in behind {
        batch.extend_from_slice(&encode(args));
    }
    c.writer.write_all(&batch).unwrap();
    for _ in before {
        assert_eq!(c.read(), V::Str("OK".into()));
    }
    assert_eq!(c.read(), V::Str("OK".into()));
    for _ in queued {
        assert_eq!(c.read(), V::Str("QUEUED".into()));
    }
}

/// An `EXEC` that finds a script running beside others waits for it asleep
/// (#882): connections of the other threads, served before the `EXEC` came,
/// are served while it waits. The replies its client had before it come at
/// once; what it sent behind it waits behind it; the `EXEC` runs whole,
/// after the script.
///
/// Sixteen probe connections that answered while the script was held; then
/// the `EXEC`; then a `GET` on each probe: none may take as long as a
/// quarter of the stall.
fn an_exec_waits_for_a_script_asleep(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    let sha = quick_script(&mut c, &["a"]);
    for i in 0..16 {
        assert_eq!(
            c.cmd(&["SET", &format!("probe:{i}"), "v"]),
            V::Str("OK".into())
        );
    }
    let script = start_held_script(&server, &sha, "a", "after");
    let probes = free_conns(&server, 16);
    assert_eq!(
        probes.len(),
        16,
        "too few connections were answered at once"
    );

    let mut tx = free_conn(&server);
    let mut batch = Vec::new();
    for args in [
        &["SET", "plain:before", "1"][..],
        &["MULTI"],
        &["SET", "{a}:k", "exec"],
        &["SET", "{b}:k", "exec"],
        &["EXEC"],
        &["SET", "plain:behind", "1"],
    ] {
        batch.extend_from_slice(&encode(args));
    }
    tx.writer.write_all(&batch).unwrap();
    // The replies before the EXEC, and when each came.
    let before = std::thread::spawn(move || {
        let got: Vec<(V, Instant)> = (0..4).map(|_| (tx.read(), Instant::now())).collect();
        (tx, got)
    });
    std::thread::sleep(Duration::from_millis(50));

    let probed: Vec<std::thread::JoinHandle<Duration>> = probes
        .into_iter()
        .enumerate()
        .map(|(i, mut probe)| {
            std::thread::spawn(move || {
                let asked = Instant::now();
                assert_eq!(
                    probe.cmd(&["GET", &format!("probe:{i}")]),
                    V::Str("v".into())
                );
                asked.elapsed()
            })
        })
        .collect();
    let took: Vec<Duration> = probed.into_iter().map(|p| p.join().unwrap()).collect();
    let held_throughout = server.holding();
    eprintln!(
        "{io}: {} of 16 probes over {} ms, slowest {:?}",
        took.iter()
            .filter(|t| **t >= Duration::from_millis(STALL_MS / 4))
            .count(),
        STALL_MS / 4,
        took.iter().max()
    );
    let stalled = took
        .iter()
        .filter(|t| **t >= Duration::from_millis(STALL_MS / 4))
        .count();
    assert!(
        held_throughout,
        "the script was not held while the probes ran"
    );
    assert_eq!(
        stalled, 0,
        "probes stalled behind the waiting EXEC: {took:?}"
    );
    assert_eq!(
        answered_at_once(&server, &["EXISTS", "plain:behind"]),
        V::Int(0),
        "what was sent behind the EXEC ran before it"
    );
    assert_eq!(answered_at_once(&server, &["GET", "{b}:k"]), V::Nil);

    let (reply, script_done) = script.join().unwrap();
    assert_eq!(reply, V::Int(1));
    let (mut tx, got) = before.join().unwrap();
    let replies: Vec<V> = got.iter().map(|(reply, _)| reply.clone()).collect();
    assert_eq!(
        replies,
        vec![
            V::Str("OK".into()),
            V::Str("OK".into()),
            V::Str("QUEUED".into()),
            V::Str("QUEUED".into())
        ]
    );
    // They came while the script was held, not after it.
    assert!(
        got.iter().all(|(_, at)| *at < script_done),
        "the replies before the EXEC waited for the script"
    );
    assert_eq!(
        tx.read(),
        V::Arr(vec![V::Str("OK".into()), V::Str("OK".into())])
    );
    assert_eq!(tx.read(), V::Str("OK".into()));
    // After the script, whole.
    assert_eq!(c.cmd(&["GET", "{a}:k"]), V::Str("exec".into()));
    assert_eq!(c.cmd(&["GET", "{b}:k"]), V::Str("exec".into()));
    assert_eq!(c.cmd(&["EXISTS", "plain:behind"]), V::Int(1));
    assert_eq!(c.info("clients", "blocked_clients"), "0");
}

#[test]
fn an_exec_waits_for_a_script_asleep_tokio() {
    an_exec_waits_for_a_script_asleep("tokio");
}

#[test]
fn an_exec_waits_for_a_script_asleep_uring() {
    an_exec_waits_for_a_script_asleep("uring");
}

/// Held first, and then fails or writes `KEYS[1]`.
const STALLS_FIRST: &str = "redis.call('ECHO', ARGV[2]) \
     if ARGV[3] == 'fail' then error('failed on purpose') end \
     redis.call('SET', KEYS[1], ARGV[1]) return 1";

/// How the held script ends while an `EXEC` waits for it.
#[derive(Clone, Copy, Debug)]
enum Ends {
    InError,
    Killed,
    OverItsTimeLimit,
}

/// An `EXEC` waiting for a script runs, whole, however the script ends:
/// with an error, after a `SCRIPT KILL`, or past `lua-time-limit`. And the
/// threads are served meanwhile.
fn an_exec_runs_however_the_script_ends(io: &str, ends: Ends) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", STALLS_FIRST]) else {
        panic!("no digest")
    };
    for _ in 0..3 {
        assert_eq!(
            c.cmd(&["EVALSHA", &sha, "1", "{a}:k", "warm", "no-stall", "ok"]),
            V::Int(1)
        );
    }
    if let Ends::OverItsTimeLimit = ends {
        assert_eq!(
            c.cmd(&["CONFIG", "SET", "lua-time-limit", "100"]),
            V::Str("OK".into())
        );
    }
    let mut runner = server.conn();
    let fails = if let Ends::InError = ends {
        "fail"
    } else {
        "ok"
    };
    runner.send(&[
        "EVALSHA",
        &sha,
        "1",
        "{a}:k",
        "script",
        "script-stall:1",
        fails,
    ]);
    let script = std::thread::spawn(move || runner.read());
    server.wait_until_held();

    let mut tx = free_conn(&server);
    send_exec(
        &mut tx,
        &[],
        &[&["SET", "{a}:k", "exec"], &["INCR", "{b}:n"]],
        &[],
    );
    if let Ends::Killed = ends {
        // Whatever it says: the script is beside others, or has not written.
        let _ = free_conn(&server).cmd(&["SCRIPT", "KILL"]);
    }
    assert_eq!(answered_at_once(&server, &["PING"]), V::Str("PONG".into()));
    assert_eq!(answered_at_once(&server, &["EXISTS", "{b}:n"]), V::Int(0));

    let reply = script.join().unwrap();
    match ends {
        Ends::InError => assert!(
            matches!(&reply, V::Err(e) if e.contains("failed on purpose")),
            "{reply:?}"
        ),
        Ends::Killed => assert!(matches!(reply, V::Int(1) | V::Err(_)), "{reply:?}"),
        Ends::OverItsTimeLimit => assert_eq!(reply, V::Int(1)),
    }
    assert_eq!(
        tx.read(),
        V::Arr(vec![V::Str("OK".into()), V::Int(1)]),
        "{ends:?}"
    );
    assert_eq!(c.cmd(&["GET", "{a}:k"]), V::Str("exec".into()));
    // Nothing is left waiting: a script runs beside others at once again.
    let asked = Instant::now();
    assert_eq!(
        c.cmd(&["EVALSHA", &sha, "1", "{a}:k", "again", "no-stall", "ok"]),
        V::Int(1)
    );
    assert_eq!(c.cmd(&["MULTI"]), V::Str("OK".into()));
    assert_eq!(c.cmd(&["INCR", "{b}:n"]), V::Str("QUEUED".into()));
    assert_eq!(c.cmd(&["EXEC"]), V::Arr(vec![V::Int(2)]));
    assert!(asked.elapsed() < Duration::from_secs(2));
}

#[test]
fn an_exec_runs_however_the_script_ends_tokio() {
    for ends in [Ends::InError, Ends::Killed, Ends::OverItsTimeLimit] {
        an_exec_runs_however_the_script_ends("tokio", ends);
    }
}

#[test]
fn an_exec_runs_however_the_script_ends_uring() {
    for ends in [Ends::InError, Ends::Killed, Ends::OverItsTimeLimit] {
        an_exec_runs_however_the_script_ends("uring", ends);
    }
}

/// A client that goes, or is killed, while its `EXEC` waits for a script
/// leaves nothing behind, and its transaction ran whole or not at all.
fn an_exec_whose_client_goes_while_it_waits(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    let sha = quick_script(&mut c, &["a"]);
    let script = start_held_script(&server, &sha, "a", "after");

    let mut closed = free_conn(&server);
    send_exec(
        &mut closed,
        &[],
        &[&["SET", "{c}:1", "closed"], &["SET", "{c}:2", "closed"]],
        &[],
    );
    let mut killed = free_conn(&server);
    let V::Int(id) = killed.cmd(&["CLIENT", "ID"]) else {
        panic!("CLIENT ID")
    };
    send_exec(
        &mut killed,
        &[],
        &[&["SET", "{k}:1", "killed"], &["SET", "{k}:2", "killed"]],
        &[],
    );
    std::thread::sleep(Duration::from_millis(50));
    drop(closed);
    assert_eq!(
        free_conn(&server).cmd(&["CLIENT", "KILL", "ID", &id.to_string()]),
        V::Int(1)
    );
    let (reply, _) = script.join().unwrap();
    assert_eq!(reply, V::Int(1));

    let asked = Instant::now();
    for tag in ["c", "k"] {
        let first = c.cmd(&["GET", &format!("{{{tag}}}:1")]);
        let second = c.cmd(&["GET", &format!("{{{tag}}}:2")]);
        assert_eq!(first, second, "a transaction ran in part");
    }
    assert_eq!(
        c.cmd(&["EVALSHA", &sha, "1", "{a}:k", "again", "no-stall"]),
        V::Int(1)
    );
    assert_eq!(c.cmd(&["MULTI"]), V::Str("OK".into()));
    assert_eq!(c.cmd(&["SET", "{a}:k", "free"]), V::Str("QUEUED".into()));
    assert_eq!(c.cmd(&["EXEC"]), V::Arr(vec![V::Str("OK".into())]));
    assert!(asked.elapsed() < Duration::from_secs(2));
    assert_eq!(c.info("clients", "blocked_clients"), "0");
}

#[test]
fn an_exec_whose_client_goes_while_it_waits_tokio() {
    an_exec_whose_client_goes_while_it_waits("tokio");
}

#[test]
fn an_exec_whose_client_goes_while_it_waits_uring() {
    an_exec_whose_client_goes_while_it_waits("uring");
}

/// An `EXEC` that waited for a script and is then held by a `CLIENT PAUSE`
/// set meanwhile runs once the pause ends, not before, and whole. As in
/// `a_waiting_write_is_then_held_by_a_pause`, a round counts only if the
/// pause was sent while the script was still held; up to five rounds.
fn a_waiting_exec_is_then_held_by_a_pause(io: &str) {
    let server = Server::start(io, 2);
    let mut c = server.conn();
    for round in 0..5 {
        let sha = quick_script(&mut c, &["a"]);
        let script = start_held_script(&server, &sha, "a", "after");
        let Some(mut tx) = try_free_conn(&server) else {
            assert_eq!(script.join().unwrap().0, V::Int(1));
            continue;
        };
        let value = format!("paused-{round}");
        send_exec(
            &mut tx,
            &[],
            &[&["SET", "{a}:k", &value], &["SET", "{b}:k", &value]],
            &[],
        );
        std::thread::sleep(Duration::from_millis(50));
        let Some(mut pauser) = try_free_conn(&server) else {
            assert_eq!(script.join().unwrap().0, V::Int(1));
            assert!(matches!(tx.read(), V::Arr(_)));
            continue;
        };
        let paused_at = Instant::now();
        pauser.send(&["CLIENT", "PAUSE", &(STALL_MS * 2).to_string(), "WRITE"]);
        let in_time = !script.is_finished() && server.holding();
        assert_eq!(pauser.read(), V::Str("OK".into()));
        let (reply, _) = script.join().unwrap();
        assert_eq!(reply, V::Int(1));
        if !in_time {
            assert_eq!(pauser.cmd(&["CLIENT", "UNPAUSE"]), V::Str("OK".into()));
            assert!(matches!(tx.read(), V::Arr(_)));
            continue;
        }
        assert_eq!(
            tx.read(),
            V::Arr(vec![V::Str("OK".into()), V::Str("OK".into())])
        );
        assert!(
            paused_at.elapsed() >= Duration::from_millis(STALL_MS * 2 - 50),
            "the transaction ran during the pause: {:?}",
            paused_at.elapsed()
        );
        assert_eq!(c.cmd(&["GET", "{a}:k"]), V::Str(value.clone()));
        assert_eq!(c.cmd(&["GET", "{b}:k"]), V::Str(value));
        return;
    }
    panic!("the pause never came while the script was held");
}

#[test]
fn a_waiting_exec_is_then_held_by_a_pause_tokio() {
    a_waiting_exec_is_then_held_by_a_pause("tokio");
}

#[test]
fn a_waiting_exec_is_then_held_by_a_pause_uring() {
    a_waiting_exec_is_then_held_by_a_pause("uring");
}
