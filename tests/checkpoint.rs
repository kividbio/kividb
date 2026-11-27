#![allow(clippy::disallowed_methods)]
// tests/checkpoint.rs
//
// AOF checkpoints (`BGREWRITEAOF`, `SAVE`, `BGSAVE`) one at a time, and a
// crash at any step of one: real `kividb` processes, each in its own data
// directory, killed with SIGKILL and started again on the same files.
//
// The steps are reached exactly, not by timing: the server is started with
// `KIVIDB_TEST_FAILPOINTS`, and at each step named there it creates
// `failpoint.<step>.<n>` in its data directory and waits while that file is
// there. A test waits for the file, does what it needs at that step (starts
// another checkpoint, or kills the server), and removes the file to let the
// server go on.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

mod common;
use common::{port_is_free, restart_verified, start_verified};

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
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
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
                // The watchdog: a server that hangs fails the test here.
                s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
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

    /// One command. A server that answers `LOADING` (its dataset or indexes
    /// are still being loaded after a start or restart) did not run it, so it
    /// is asked again, for at most 10 seconds.
    fn cmd(&mut self, args: &[&str]) -> V {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let reply = self.cmd_once(args);
            match &reply {
                V::Err(e) if e.starts_with("LOADING") && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                _ => return reply,
            }
        }
    }

    fn cmd_once(&mut self, args: &[&str]) -> V {
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
        self.reader
            .read_line(&mut line)
            .expect("no reply in 60 s: the server hangs");
        let line = line.trim_end_matches(['\r', '\n']);
        assert!(!line.is_empty(), "connection closed");
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
}

// ── Server fixture ────────────────────────────────────────────────────────────

/// The I/O models to run each test on: both, where io_uring exists.
fn io_models() -> &'static [&'static str] {
    if cfg!(target_os = "linux") {
        &["tokio", "uring"]
    } else {
        &["tokio"]
    }
}

/// Long enough for any step to be reached on a loaded machine.
const LONG: Duration = Duration::from_secs(30);

/// A server in `dir`, with the AOF on if `aof`, its output appended to
/// `server.log`.
fn launch(dir: &Path, port: u16, io: &str, failpoints: &str, aof: bool) -> Child {
    launch_bin(
        Path::new(env!("CARGO_BIN_EXE_kividb")),
        dir,
        port,
        io,
        failpoints,
        aof,
    )
}

/// [`launch`], of the binary `bin`.
fn launch_bin(bin: &Path, dir: &Path, port: u16, io: &str, failpoints: &str, aof: bool) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    Command::new(bin)
        .current_dir(dir)
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--io-model",
            io,
            "--threads",
            "4",
            "--configfile",
            "kividb.conf",
        ])
        .args(if aof { &["--aof"][..] } else { &[] })
        .env("KIVIDB_TEST_FAILPOINTS", failpoints)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
    io: &'static str,
    failpoints: String,
    /// Whether it is started with the AOF on: it is, unless a test says not.
    aof: bool,
    /// The binary: this build's, unless a test runs a release's.
    bin: PathBuf,
}

impl Server {
    fn start(io: &'static str, failpoints: &str) -> Self {
        Self::start_in(tempfile::tempdir().unwrap().keep(), io, failpoints)
    }

    /// [`Server::start`], in the data directory `dir` as it is.
    fn start_in(dir: PathBuf, io: &'static str, failpoints: &str) -> Self {
        Self::start_bin(Path::new(env!("CARGO_BIN_EXE_kividb")), dir, io, failpoints)
    }

    /// [`Server::start_in`], of the binary `bin`.
    fn start_bin(bin: &Path, dir: PathBuf, io: &'static str, failpoints: &str) -> Self {
        // Verified to be the process started: see tests/common/mod.rs.
        let (child, port) =
            start_verified(|port| launch_bin(bin, &dir, port, io, failpoints, true));
        let server = Server {
            port,
            dir,
            child,
            io,
            failpoints: failpoints.to_string(),
            aof: true,
            bin: bin.to_path_buf(),
        };
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// SIGKILL: nothing of the server's own runs after this.
    ///
    /// And then wait for the port: an io_uring server's listening socket
    /// outlives the process for a moment (the kernel takes its ring down
    /// afterwards), and a client of the restarted server that dialled then
    /// was put in the old socket's queue and reset.
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        let deadline = Instant::now() + LONG;
        while !port_is_free(self.port) {
            assert!(Instant::now() < deadline, "port {} still taken", self.port);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn restart(&mut self) -> Conn {
        // On the same port if it is still free, and on another if a test of
        // another process has taken it in the gap (the data directory is what
        // matters; nothing in this file keeps the port).
        let (child, port, _) = restart_verified(
            |port| {
                launch_bin(
                    &self.bin,
                    &self.dir,
                    port,
                    self.io,
                    &self.failpoints,
                    self.aof,
                )
            },
            self.port,
        );
        self.child = child;
        self.port = port;
        self.conn()
    }

    /// Whether the server reaches `step` (`<name>.<n>`) within `within`.
    fn reached(&self, step: &str, within: Duration) -> bool {
        let path = self.dir.join(format!("failpoint.{step}"));
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if path.exists() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        path.exists()
    }

    fn release(&self, step: &str) {
        std::fs::remove_file(self.dir.join(format!("failpoint.{step}")))
            .unwrap_or_else(|e| panic!("{}: {step} was not held: {e}", self.io));
    }

    /// Wait for `step`, then let the server go on past it.
    fn pass(&self, step: &str) {
        assert!(self.reached(step, LONG), "{}: {step} not reached", self.io);
        self.release(step);
    }

    /// The save numbers the checkpoints of this data directory took, in the
    /// order they finished, across restarts.
    fn generations(&self) -> Vec<u64> {
        let log = std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default();
        log.lines()
            .filter_map(|line| {
                let at = line.find("rebased onto dump.kdb (generation ")?;
                let rest = &line[at + "rebased onto dump.kdb (generation ".len()..];
                rest.split(')').next()?.parse().ok()
            })
            .collect()
    }

    fn assert_generations_increase(&self) {
        let generations = self.generations();
        assert!(!generations.is_empty(), "{}: no checkpoint logged", self.io);
        assert!(
            generations.windows(2).all(|pair| pair[0] < pair[1]),
            "{}: checkpoints shared a save number, or finished out of order: {generations:?}",
            self.io
        );
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        } else {
            eprintln!("data directory kept: {}", self.dir.display());
        }
    }
}

/// `n` keys `<prefix>:<i>`, in `MSET`s of 200.
fn fill(c: &mut Conn, prefix: &str, n: usize) {
    let all: Vec<usize> = (0..n).collect();
    for chunk in all.chunks(200) {
        let mut args = vec!["MSET".to_string()];
        for i in chunk {
            args.push(format!("{prefix}:{i}"));
            args.push(format!("value-{i}"));
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(c.cmd(&args).ok());
    }
}

/// Everything acknowledged so far is in the log on disk. Patiently: on a
/// small cloud disk shared by tests running at once, an fsync has been seen
/// to take over 10 seconds.
fn durable(c: &mut Conn) {
    let reply = c.cmd(&["WAITAOF", "1", "0", "50000"]);
    assert!(reply.arr()[0].i() >= 1, "WAITAOF: {reply:?}");
}

fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.cmd(&["INFO", "persistence"]);
    info.s()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .unwrap_or_else(|| panic!("no {field} in INFO"))
        .trim()
        .to_string()
}

/// No checkpoint or background save running, or asked for.
fn wait_idle(c: &mut Conn) {
    let deadline = Instant::now() + LONG;
    loop {
        if info_field(c, "aof_rewrite_in_progress") == "0"
            && info_field(c, "rdb_bgsave_in_progress") == "0"
        {
            return;
        }
        assert!(Instant::now() < deadline, "checkpoints did not finish");
        std::thread::sleep(Duration::from_millis(10));
    }
}

const STARTED: &str = "Background append only file rewriting started";

// ── Overlapping checkpoints ───────────────────────────────────────────────────

/// A `BGREWRITEAOF` asked for while one runs used to run beside it, with the
/// same save number, and whichever finished last put its snapshot in place:
/// the older one, last, brought back every key a `FLUSHALL` had deleted and
/// lost the key written after it. The second now runs after the first.
#[test]
fn an_older_checkpoint_never_replaces_a_newer_one() {
    for &io in io_models() {
        let mut server = Server::start(io, "checkpoint-turned-over,checkpoint-finished");
        let mut c = server.conn();
        fill(&mut c, "old", 2000);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("checkpoint-turned-over.1", LONG));

        // The first has captured the 2000 keys and turned the log over.
        assert!(c.cmd(&["FLUSHALL"]).ok());
        assert!(c.cmd(&["SET", "newkey", "v"]).ok());
        durable(&mut c);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);

        let overlapped = server.reached("checkpoint-turned-over.2", Duration::from_secs(1));
        if overlapped {
            // The second finishes first, the first last.
            server.release("checkpoint-turned-over.2");
            server.pass("checkpoint-finished.1");
            server.release("checkpoint-turned-over.1");
            server.pass("checkpoint-finished.2");
        } else {
            server.release("checkpoint-turned-over.1");
            server.pass("checkpoint-finished.1");
            // The one asked for while the first ran, after it.
            server.pass("checkpoint-turned-over.2");
            server.pass("checkpoint-finished.2");
        }
        wait_idle(&mut c);
        drop(c);
        server.kill();

        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["DBSIZE"]).i(),
            1,
            "{io}: keys deleted by FLUSHALL came back, or the key written after it was lost"
        );
        assert_eq!(c.cmd(&["GET", "newkey"]).s(), "v");
        assert!(
            !overlapped,
            "{io}: a second checkpoint ran beside the first"
        );
        server.assert_generations_increase();
    }
}

/// A checkpoint that finished removed `appendonly.aof.prev` even while
/// another, still running, needed it: the writes in it (acknowledged, and on
/// disk) were in neither snapshot, and a crash before the other finished
/// lost them.
#[test]
fn an_old_log_is_kept_while_a_checkpoint_still_needs_it() {
    for &io in io_models() {
        let mut server = Server::start(io, "checkpoint-turned-over,checkpoint-finished");
        let mut c = server.conn();
        fill(&mut c, "old", 1000);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("checkpoint-turned-over.1", LONG));

        // After the first one's capture: in no snapshot yet.
        fill(&mut c, "acked", 5000);
        durable(&mut c);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        let overlapped = server.reached("checkpoint-turned-over.2", Duration::from_secs(1));

        // The first finishes; the second is part-way when the server dies.
        server.release("checkpoint-turned-over.1");
        server.pass("checkpoint-finished.1");
        assert!(server.reached("checkpoint-turned-over.2", LONG));
        drop(c);
        server.kill();

        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["DBSIZE"]).i(),
            6000,
            "{io}: writes acknowledged on disk were lost"
        );
        assert_eq!(c.cmd(&["GET", "acked:4999"]).s(), "value-4999");
        assert!(
            !overlapped,
            "{io}: a second checkpoint ran beside the first"
        );
    }
}

/// A client asking for a rewrite every few milliseconds had a thread, and a
/// checkpoint, for each: hundreds at once, with writes held back by each in
/// turn. There is one thread, and one checkpoint at a time.
#[test]
fn a_flood_of_rewrites_runs_one_checkpoint_at_a_time() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        fill(&mut c, "base", 20_000);
        let threads_before = threads(server.child.id());

        let stop = Arc::new(AtomicBool::new(false));
        let port = server.port;
        let flood = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut c = Conn::connect(port);
                for _ in 0..300 {
                    assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
                    std::thread::sleep(Duration::from_millis(5));
                }
                stop.store(true, Ordering::Relaxed);
            })
        };
        let mut written = 0usize;
        let mut most_threads = threads_before;
        let mut slowest = Duration::ZERO;
        while !stop.load(Ordering::Relaxed) {
            let started = Instant::now();
            let key = format!("w:{written}");
            assert!(c.cmd(&["SET", &key, "x"]).ok());
            slowest = slowest.max(started.elapsed());
            written += 1;
            if written.is_multiple_of(20) {
                most_threads = most_threads.max(threads(server.child.id()));
            }
        }
        flood.join().unwrap();
        eprintln!(
            "{io}: {written} writes during the flood, slowest {slowest:?}, threads {threads_before} -> {most_threads}"
        );
        if cfg!(target_os = "linux") {
            assert!(
                most_threads <= threads_before + 4,
                "{io}: threads went from {threads_before} to {most_threads}"
            );
        }
        assert!(written > 0);
        durable(&mut c);
        wait_idle(&mut c);
        drop(c);
        server.kill();

        let mut c = server.restart();
        assert_eq!(c.cmd(&["DBSIZE"]).i(), 20_000 + written as i64);
        server.assert_generations_increase();
    }
}

/// Threads of process `pid`; 0 where that cannot be read.
fn threads(pid: u32) -> usize {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("Threads:"))
                .and_then(|n| n.trim().parse().ok())
        })
        .unwrap_or(0)
}

/// `SAVE` and `BGSAVE` while a `BGREWRITEAOF` runs wait for it, and then
/// save. A checkpoint has its turn while it waits for the write gate, so a
/// `SAVE` from inside a write — from a script, which holds the gate — would
/// be waiting for a checkpoint that waits for it. A script may not run
/// `SAVE` at all, as in Redis, during a checkpoint or not.
#[test]
fn saves_during_a_rewrite_wait_for_it_and_never_deadlock() {
    for &io in io_models() {
        let mut server = Server::start(io, "checkpoint-cutting");
        let mut c = server.conn();
        fill(&mut c, "k", 1000);

        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("checkpoint-cutting.1", LONG));
        let port = server.port;
        let saver = std::thread::spawn(move || Conn::connect(port).cmd(&["SAVE"]));
        std::thread::sleep(Duration::from_millis(200));
        let releasing = release_all(&server.dir);
        assert!(saver.join().unwrap().ok(), "{io}: SAVE during BGREWRITEAOF");

        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert_eq!(c.cmd(&["BGSAVE"]).s(), "Background saving started");
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(c.cmd(&["SAVE"]).ok());
        wait_idle(&mut c);
        releasing.stop();

        #[cfg(feature = "lua")]
        {
            let script = "return redis.call('SAVE')";
            let not_allowed =
                |reply: &V| matches!(reply, V::Err(e) if e.contains("not allowed from script"));
            assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
            let held = wait_for_any(&server.dir, "failpoint.checkpoint-cutting.");
            let refused = c.cmd(&["EVAL", script, "0"]);
            assert!(
                not_allowed(&refused),
                "{io}: SAVE in a script during a checkpoint: {refused:?}"
            );
            std::fs::remove_file(&held).unwrap();
            wait_idle(&mut c);
            let refused = c.cmd(&["EVAL", script, "0"]);
            assert!(not_allowed(&refused), "{io}: SAVE in a script: {refused:?}");
        }

        assert!(c.cmd(&["SET", "after", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        server.failpoints.clear();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["DBSIZE"]).i(), 1001);
        server.assert_generations_increase();
    }
}

struct Releasing {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

impl Releasing {
    fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().unwrap();
    }
}

/// Let the server past every step it reaches, until stopped.
fn release_all(dir: &Path) -> Releasing {
    let stop = Arc::new(AtomicBool::new(false));
    let dir = dir.to_path_buf();
    let stopped = Arc::clone(&stop);
    let thread = std::thread::spawn(move || {
        while !stopped.load(Ordering::Relaxed) {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("failpoint.")
                {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    Releasing { stop, thread }
}

#[cfg_attr(not(feature = "lua"), allow(dead_code))]
fn wait_for_any(dir: &Path, prefix: &str) -> PathBuf {
    let deadline = Instant::now() + LONG;
    loop {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            if entry.file_name().to_string_lossy().starts_with(prefix) {
                return entry.path();
            }
        }
        assert!(Instant::now() < deadline, "{prefix}* not reached");
        std::thread::sleep(Duration::from_millis(5));
    }
}

// ── A crash part-way through a turn-over ─────────────────────────────────────

/// A crash after the log was moved aside and before the new one took its
/// name left `appendonly.aof.prev` and no log, and the next start, finding
/// no log, replayed nothing: every write since the last snapshot was gone.
#[test]
fn a_crash_while_the_log_is_turned_over_loses_nothing() {
    for &io in io_models() {
        let mut server = Server::start(io, "turn-over-moved-aside");
        let mut c = server.conn();
        fill(&mut c, "k", 500);
        durable(&mut c);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("turn-over-moved-aside.1", LONG));
        drop(c);
        server.kill();

        server.failpoints.clear();
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["DBSIZE"]).i(),
            500,
            "{io}: writes in the log moved aside were lost"
        );
        // And it goes on as usual: the next checkpoint finishes the job.
        assert!(c.cmd(&["SET", "after", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert!(!server.dir.join("appendonly.aof.prev").exists());
        assert!(c.cmd(&["SET", "later", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["DBSIZE"]).i(), 502);
    }
}

/// What a crash in that window left before 1.1.3 — `appendonly.aof.prev`,
/// and no log or an empty one (the writer creates it at start) — is
/// recovered from too.
#[test]
fn an_old_log_with_no_log_beside_it_is_replayed() {
    for &io in io_models() {
        for log in ["missing", "empty"] {
            let mut server = Server::start(io, "");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "in-dump", "1"]).ok());
            assert!(c.cmd(&["SAVE"]).ok());
            fill(&mut c, "in-log", 300);
            assert_eq!(c.cmd(&["INCR", "counter"]).i(), 1);
            durable(&mut c);
            drop(c);
            server.kill();

            let aof = server.dir.join("appendonly.aof");
            std::fs::rename(&aof, server.dir.join("appendonly.aof.prev")).unwrap();
            if log == "empty" {
                std::fs::write(&aof, b"").unwrap();
            }
            let mut c = server.restart();
            assert_eq!(
                c.cmd(&["DBSIZE"]).i(),
                302,
                "{io}, log {log}: the old log was not replayed"
            );
            assert_eq!(c.cmd(&["GET", "counter"]).s(), "1");

            // Restarted again before any checkpoint: still all there, once.
            assert_eq!(c.cmd(&["INCR", "counter"]).i(), 2);
            durable(&mut c);
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["DBSIZE"]).i(), 302, "{io}, log {log}");
            assert_eq!(c.cmd(&["GET", "counter"]).s(), "2", "{io}, log {log}");
            assert!(c.cmd(&["SAVE"]).ok());
            assert!(!server.dir.join("appendonly.aof.prev").exists());
        }
    }
}

/// A turn-over that finds an old log still needed (a checkpoint before it
/// did not finish) adds the log's records to it. Stopped after adding them
/// and before the new log took the log's name, the records were in both, and
/// replayed twice: a counter incremented once came back incremented twice.
#[test]
fn a_crash_while_records_are_added_to_an_old_log_does_not_replay_them_twice() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        assert!(c.cmd(&["SET", "in-dump", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert_eq!(c.cmd(&["INCR", "counter"]).i(), 1);
        durable(&mut c);
        drop(c);
        server.kill();

        // As a checkpoint whose snapshot did not reach the disk leaves it:
        // the log moved aside, and a new delta for the next save number,
        // with a write in it.
        let aof = server.dir.join("appendonly.aof");
        let old = std::fs::read(&aof).unwrap();
        assert!(old.starts_with(b"KIVIDLT1"));
        let number = u64::from_le_bytes(old[8..16].try_into().unwrap());
        std::fs::rename(&aof, server.dir.join("appendonly.aof.prev")).unwrap();
        let mut fresh = b"KIVIDLT1".to_vec();
        fresh.extend_from_slice(&(number + 1).to_le_bytes());
        fresh.extend_from_slice(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        std::fs::write(&aof, &fresh).unwrap();

        server.failpoints = "turn-over-appended".into();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "counter"]).s(), "2");
        assert!(server.dir.join("appendonly.aof.prev").exists());
        assert_eq!(c.cmd(&["INCR", "counter"]).i(), 3);
        durable(&mut c);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("turn-over-appended.1", LONG));
        drop(c);
        server.kill();

        server.failpoints.clear();
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "counter"]).s(),
            "3",
            "{io}: records were replayed twice"
        );
        assert_eq!(c.cmd(&["GET", "in-dump"]).s(), "1");
        assert!(c.cmd(&["SAVE"]).ok());
        assert!(!server.dir.join("appendonly.aof.prev").exists());
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "counter"]).s(), "3", "{io}");
        assert_eq!(c.cmd(&["DBSIZE"]).i(), 2);
    }
}

// ── Writes while a turn-over waits for the disk ──────────────────────────────

/// A turn-over synced the old log, and the directory, in whichever thread
/// reached its mark, holding the log's lock: every write waited for the
/// disk. On a slow disk with a backlog that was seconds at a time, and
/// replies went out without their writes after 5 (#243). The old log is now
/// synced by the checkpoint with the lock let go; the step is held here, as
/// a slow disk would hold it.
#[test]
fn writes_go_on_while_a_turn_over_syncs_the_old_log() {
    for &io in io_models() {
        for end in ["finishes", "is killed"] {
            let mut server = Server::start(io, "turn-over-syncing");
            let mut c = server.conn();
            fill(&mut c, "before", 500);
            assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
            assert!(server.reached("turn-over-syncing.1", LONG));

            let started = Instant::now();
            for i in 0..200 {
                assert!(c.cmd(&["SET", &format!("during:{i}"), "1"]).ok());
            }
            let took = started.elapsed();
            assert!(
                took < Duration::from_secs(3),
                "{io}: writes waited for the old log's sync ({took:?})"
            );
            assert_eq!(info_field(&mut c, "aof_acks_before_write"), "0", "{io}");

            if end == "finishes" {
                server.release("turn-over-syncing.1");
                wait_idle(&mut c);
                assert!(!server.dir.join("appendonly.aof.prev").exists(), "{io}");
                durable(&mut c);
            }
            // Acknowledged, so in the new log: killed with the old log not
            // yet synced, or after the checkpoint, nothing is lost.
            drop(c);
            server.kill();
            server.failpoints.clear();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["DBSIZE"]).i(), 700, "{io}, checkpoint {end}");
            assert!(c.cmd(&["SAVE"]).ok());
            assert!(!server.dir.join("appendonly.aof.prev").exists());
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["DBSIZE"]).i(), 700, "{io}, checkpoint {end}");
        }
    }
}

/// The writer syncs the old log of a turn-over itself when the checkpoint
/// has not, and then marks it synced. It marked it so even when a second
/// turn-over had moved another log aside meanwhile: the directory, with the
/// second's renames, unsynced, and the records after it reported synced
/// once the new log alone was (#529). The writer is held here between its
/// sync of the first old log and its mark, as a slow disk would hold it, and
/// the second turn-over made then.
#[test]
fn a_second_turn_over_during_the_writers_sync_of_the_first_is_synced_too() {
    for &io in io_models() {
        let server = Server::start(io, "turn-over-syncing,old-log-synced");
        let mut c = server.conn();
        fill(&mut c, "before", 10);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("turn-over-syncing.1", LONG), "{io}");
        assert!(c.cmd(&["SET", "first", "1"]).ok());
        // The second's sync, or a WAITAOF's: the writer syncs the old log
        // the checkpoint is held from syncing.
        let mut w = server.conn();
        send(&mut w, &["WAITAOF", "1", "0", "0"]);
        assert!(server.reached("old-log-synced.1", LONG), "{io}");

        // The first checkpoint ends, and a second turns the log over while
        // the writer is still held. Its mark is taken by the connection that
        // writes next, the writer being held.
        server.release("turn-over-syncing.1");
        wait_idle(&mut c);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        let deadline = Instant::now() + LONG;
        let mut i = 0;
        while !server.dir.join("failpoint.turn-over-syncing.2").exists() {
            assert!(Instant::now() < deadline, "{io}: no second turn-over");
            assert!(c.cmd(&["SET", &format!("second:{i}"), "1"]).ok());
            i += 1;
            std::thread::sleep(Duration::from_millis(5));
        }

        // The writer goes on. What is written now is in the second's new
        // log: it is not synced before the second's old log, and the
        // directory, are — by the writer, again.
        server.release("old-log-synced.1");
        assert!(c.cmd(&["SET", "after", "1"]).ok());
        send(&mut c, &["WAITAOF", "1", "0", "0"]);
        let deadline = Instant::now() + LONG;
        loop {
            if server.dir.join("failpoint.old-log-synced.2").exists() {
                break;
            }
            let socket = c.reader.get_ref();
            socket.set_nonblocking(true).unwrap();
            let reply = socket.peek(&mut [0]);
            socket.set_nonblocking(false).unwrap();
            assert!(
                matches!(reply, Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock),
                "{io}: a write after the second turn-over was reported synced, the second's old \
                 log and the directory not synced: {reply:?}"
            );
            assert!(
                Instant::now() < deadline,
                "{io}: WAITAOF neither answered nor synced"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        server.release("old-log-synced.2");
        assert_eq!(c.read().arr()[0].i(), 1, "{io}");
        assert_eq!(w.read().arr()[0].i(), 1, "{io}");
        server.release("turn-over-syncing.2");
        wait_idle(&mut c);
    }
}

// ── The AOF turned on at runtime ─────────────────────────────────────────────

/// Send a command and do not wait for its reply: for one the server is held
/// in at a step.
fn send(c: &mut Conn, args: &[&str]) {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    c.writer.write_all(&buf).unwrap();
}

/// `CONFIG SET appendonly yes` appended to the `appendonly.aof` an earlier
/// run with the AOF on had left, and the turn-over of the checkpoint that
/// follows made all of it `appendonly.aof.prev`. Killed before that
/// checkpoint's snapshot was on disk, the server replayed the earlier run's
/// records on the next start: a value overwritten since came back, and so
/// did a key deleted since (#350). The AOF turned on at runtime starts a new
/// log. What was written with the AOF off and never saved is not kept, as
/// with the AOF off.
#[test]
fn the_aof_turned_on_at_runtime_never_replays_an_earlier_runs_log() {
    for &io in io_models() {
        for saved in [true, false] {
            // The earlier run, with the AOF on: killed with records in its
            // log that its last snapshot does not have.
            let mut server = Server::start(io, "");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "k", "in-dump"]).ok());
            assert!(c.cmd(&["SET", "gone", "in-dump"]).ok());
            assert!(c.cmd(&["SAVE"]).ok());
            assert!(c.cmd(&["SET", "k", "stale"]).ok());
            assert!(c.cmd(&["SET", "gone", "stale"]).ok());
            durable(&mut c);
            drop(c);
            server.kill();

            // Started again with the AOF off: the snapshot is loaded, the
            // log is not. Changed, and turned on, the server is killed once
            // the enable's checkpoint has turned the log over, before its
            // snapshot is on disk.
            server.aof = false;
            server.failpoints = "checkpoint-turned-over".into();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "k"]).s(), "in-dump", "{io}");
            assert!(c.cmd(&["SET", "k", "new"]).ok());
            assert_eq!(c.cmd(&["DEL", "gone"]).i(), 1);
            assert!(c.cmd(&["SET", "fresh", "1"]).ok());
            if saved {
                assert!(c.cmd(&["SAVE"]).ok());
            }
            send(&mut c, &["CONFIG", "SET", "appendonly", "yes"]);
            assert!(server.reached("checkpoint-turned-over.1", LONG), "{io}");
            drop(c);
            server.kill();

            server.aof = true;
            server.failpoints.clear();
            let mut c = server.restart();
            let (k, gone, fresh) = if saved {
                ("new", V::Nil, V::Str("1".into()))
            } else {
                ("in-dump", V::Str("in-dump".into()), V::Nil)
            };
            assert_eq!(
                c.cmd(&["GET", "k"]).s(),
                k,
                "{io}, saved {saved}: the earlier run's write came back"
            );
            assert_eq!(
                c.cmd(&["GET", "gone"]),
                gone,
                "{io}, saved {saved}: the earlier run's write came back"
            );
            assert_eq!(c.cmd(&["GET", "fresh"]), fresh, "{io}, saved {saved}");

            // And it goes on as usual: the next checkpoint finishes the job.
            assert!(c.cmd(&["SET", "after", "1"]).ok());
            assert!(c.cmd(&["SAVE"]).ok());
            assert!(!server.dir.join("appendonly.aof.prev").exists(), "{io}");
            durable(&mut c);
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "k"]).s(), k, "{io}, saved {saved}");
            assert_eq!(c.cmd(&["GET", "gone"]), gone, "{io}, saved {saved}");
            assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{io}, saved {saved}");
        }
    }
}

/// The same within one process: the log kept open through `appendonly no`
/// is older than a `SAVE` made in the meantime, and was replayed on top of
/// it after a crash during the enable. The AOF turned on again starts a new
/// log then. With no save in between, the log is kept: what was written with
/// the AOF on is in no snapshot, and a crash during the enable must not lose
/// it. A write made with the AOF off and never saved is not kept, as with
/// the AOF off.
#[test]
fn the_aof_turned_off_and_on_again_replays_only_what_no_snapshot_has() {
    for &io in io_models() {
        for saved in [true, false] {
            let mut server = Server::start(io, "checkpoint-turned-over");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "k", "logged"]).ok());
            assert!(c.cmd(&["SET", "gone", "logged"]).ok());
            durable(&mut c);
            assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
            assert!(c.cmd(&["SET", "k", "new"]).ok());
            assert_eq!(c.cmd(&["DEL", "gone"]).i(), 1);
            if saved {
                assert!(c.cmd(&["SAVE"]).ok());
            }
            send(&mut c, &["CONFIG", "SET", "appendonly", "yes"]);
            assert!(server.reached("checkpoint-turned-over.1", LONG), "{io}");
            drop(c);
            server.kill();

            server.failpoints.clear();
            let mut c = server.restart();
            let (k, gone) = if saved {
                ("new", V::Nil)
            } else {
                ("logged", V::Str("logged".into()))
            };
            assert_eq!(
                c.cmd(&["GET", "k"]).s(),
                k,
                "{io}, saved {saved}: the log was replayed over a newer dump, or lost"
            );
            assert_eq!(
                c.cmd(&["GET", "gone"]),
                gone,
                "{io}, saved {saved}: the log was replayed over a newer dump, or lost"
            );
            assert!(c.cmd(&["SAVE"]).ok());
            assert!(!server.dir.join("appendonly.aof.prev").exists(), "{io}");
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "k"]).s(), k, "{io}, saved {saved}");
            assert_eq!(c.cmd(&["GET", "gone"]), gone, "{io}, saved {saved}");
        }
    }
}

// ── Logs older than the dump (#354) ──────────────────────────────────────────

impl Server {
    /// Wait for the server to exit on its own (`SHUTDOWN`), and for its port.
    fn wait_exit(&mut self) {
        let deadline = Instant::now() + LONG;
        while self.child.try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "{}: the server did not exit",
                self.io
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        while !port_is_free(self.port) {
            assert!(Instant::now() < deadline, "port {} still taken", self.port);
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// A header-only delta log for save number `number`.
fn delta_header(number: u64) -> Vec<u8> {
    let mut header = b"KIVIDLT1".to_vec();
    header.extend_from_slice(&number.to_le_bytes());
    header
}

/// A save with the AOF off wrote `dump.kdb` and not its save number, so the
/// log beside it still read as going on from it: started again with the AOF
/// on, the server replayed the log over the newer dump. An `INCR` made with
/// the AOF on was applied twice, and a key deleted with the AOF off came
/// back (#354). No crash is needed: a `SHUTDOWN` saves.
#[test]
fn a_restart_with_the_aof_on_after_a_save_with_it_off_applies_nothing_twice() {
    for &io in io_models() {
        for end in ["SHUTDOWN", "SAVE"] {
            let mut server = Server::start(io, "");
            let mut c = server.conn();
            assert_eq!(c.cmd(&["INCR", "x"]).i(), 1);
            assert!(c.cmd(&["SET", "gone", "1"]).ok());
            durable(&mut c);
            assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
            assert_eq!(c.cmd(&["DEL", "gone"]).i(), 1);
            assert!(c.cmd(&["SET", "k", "off"]).ok());
            if end == "SHUTDOWN" {
                send(&mut c, &["SHUTDOWN"]);
                drop(c);
                server.wait_exit();
            } else {
                assert!(c.cmd(&["SAVE"]).ok());
                drop(c);
                server.kill();
            }

            let mut c = server.restart();
            assert_eq!(
                c.cmd(&["GET", "x"]).s(),
                "1",
                "{io}, {end}: the log was replayed over the newer dump"
            );
            assert_eq!(c.cmd(&["GET", "gone"]), V::Nil, "{io}, {end}");
            assert_eq!(c.cmd(&["GET", "k"]).s(), "off", "{io}, {end}");

            // And the log goes on from the dump.
            assert_eq!(c.cmd(&["INCR", "x"]).i(), 2);
            durable(&mut c);
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "x"]).s(), "2", "{io}, {end}");
        }
    }
}

/// What 1.2.0 left after a save with the AOF off: a newer `dump.kdb`, and
/// beside it the save number and the log of the dump before. Recovery tells
/// from the fingerprint that the log does not go with this dump, leaves it
/// out, and starts the log again on the dump: what is written next is kept.
#[test]
fn a_log_left_beside_a_dump_saved_by_1_2_0_with_the_aof_off_is_not_replayed() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        assert_eq!(c.cmd(&["INCR", "x"]).i(), 1);
        assert!(c.cmd(&["SAVE"]).ok());
        assert_eq!(c.cmd(&["INCR", "x"]).i(), 2);
        durable(&mut c);
        drop(c);
        server.kill();
        let log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();
        let saved = std::fs::read(server.dir.join("dump.kdb.gen")).unwrap();

        // With the AOF off: the dump is loaded, the log is not.
        server.aof = false;
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "x"]).s(), "1", "{io}");
        assert_eq!(c.cmd(&["INCR", "x"]).i(), 2);
        assert!(c.cmd(&["SET", "k", "off"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();
        // As 1.2.0's save left them: the log and the save number untouched.
        std::fs::write(server.dir.join("appendonly.aof"), &log).unwrap();
        std::fs::write(server.dir.join("dump.kdb.gen"), &saved).unwrap();

        server.aof = true;
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "x"]).s(),
            "2",
            "{io}: the log was replayed over the newer dump"
        );
        assert_eq!(c.cmd(&["GET", "k"]).s(), "off", "{io}");
        assert_eq!(c.cmd(&["INCR", "x"]).i(), 3);
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "x"]).s(),
            "3",
            "{io}: what was written after the restart was lost"
        );
    }
}

/// A `BGREWRITEAOF` waiting behind a `BGSAVE` ran after `CONFIG SET
/// appendonly no`, and saved a dump with the AOF off, the open log left
/// beside it. Turned on again, the AOF kept that log, and a crash during the
/// enable replayed it over the newer dump: an `INCR` twice, an overwritten
/// value back.
#[test]
fn a_rewrite_that_finds_the_aof_turned_off_leaves_no_log_to_replay() {
    for &io in io_models() {
        let mut server = Server::start(io, "checkpoint-turned-over");
        let mut c = server.conn();
        assert!(c.cmd(&["SET", "k", "v1"]).ok());
        assert_eq!(c.cmd(&["INCR", "c"]).i(), 1);
        durable(&mut c);
        assert_eq!(c.cmd(&["BGSAVE"]).s(), "Background saving started");
        assert!(server.reached("checkpoint-turned-over.1", LONG), "{io}");
        // In the log the BGSAVE's checkpoint has just started.
        assert_eq!(c.cmd(&["INCR", "c"]).i(), 2);
        assert!(c.cmd(&["SET", "k", "v1b"]).ok());
        durable(&mut c);
        // Waits for the BGSAVE's turn, then finds the AOF off.
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
        assert!(c.cmd(&["SET", "k", "v2"]).ok());
        server.release("checkpoint-turned-over.1");
        server.pass("checkpoint-turned-over.2");
        wait_idle(&mut c);

        send(&mut c, &["CONFIG", "SET", "appendonly", "yes"]);
        assert!(server.reached("checkpoint-turned-over.3", LONG), "{io}");
        drop(c);
        server.kill();
        server.failpoints.clear();
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "k"]).s(),
            "v2",
            "{io}: an older log was replayed"
        );
        assert_eq!(
            c.cmd(&["GET", "c"]).s(),
            "2",
            "{io}: an INCR was applied twice"
        );
    }
}

/// A `CONFIG SET appendonly no` that came while `CONFIG SET appendonly yes`
/// was turning the AOF on left it off, and the enable still answered OK.
#[test]
fn an_enable_overtaken_by_a_disable_does_not_answer_ok() {
    for &io in io_models() {
        let mut server = Server::start(io, "aof-enabling");
        let mut c = server.conn();
        assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
        assert!(c.cmd(&["SET", "k", "1"]).ok());
        // The enable holds its thread while it waits at the step. An io_uring
        // server spreads connections over its threads, and the disable is
        // sent on several: one on another thread is answered. A tokio server
        // runs the enable off the runtime, and answers any of them; it used
        // to keep the enable on a worker, and about one run in a hundred
        // nothing else polled the sockets until the enable was done.
        let others: Vec<Conn> = (0..8).map(|_| server.conn()).collect();
        send(&mut c, &["CONFIG", "SET", "appendonly", "yes"]);
        assert!(server.reached("aof-enabling.1", LONG), "{io}");
        let (answered, answers) = std::sync::mpsc::channel();
        let disables: Vec<_> = others
            .into_iter()
            .map(|mut other| {
                let answered = answered.clone();
                std::thread::spawn(move || {
                    let ok = other.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok();
                    let _ = answered.send(ok);
                })
            })
            .collect();
        assert_eq!(
            answers.recv_timeout(LONG),
            Ok(true),
            "{io}: no disable was answered"
        );
        server.release("aof-enabling.1");
        let reply = c.read();
        assert!(
            matches!(&reply, V::Err(e) if e.contains("turned off")),
            "{io}: the enable answered {reply:?} with the AOF off"
        );
        for disable in disables {
            disable.join().unwrap();
        }
        let mut other = server.conn();
        let on = other.cmd(&["CONFIG", "GET", "appendonly"]);
        assert_eq!(on.arr()[1].s(), "no", "{io}");

        // What it saved is the dump, alone.
        assert!(c.cmd(&["SET", "k", "2"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop((c, other));
        server.kill();
        server.failpoints.clear();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "k"]).s(), "2", "{io}");
    }
}

/// An `appendonly.aof.prev` left by a crash during a checkpoint of an earlier
/// run with the AOF on, still there when the AOF is turned on at runtime: the
/// turn-over found it, took it for one still needed, and added the log to it.
/// A crash before the enable's snapshot was on disk replayed it.
#[test]
fn an_old_log_left_by_an_earlier_run_is_not_replayed_after_an_enable() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        assert!(c.cmd(&["SET", "k", "in-dump"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert!(c.cmd(&["SET", "k", "stale"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        // Killed during a checkpoint, the log moved aside.
        server.failpoints = "checkpoint-turned-over".into();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("checkpoint-turned-over.1", LONG), "{io}");
        drop(c);
        server.kill();
        assert!(server.dir.join("appendonly.aof.prev").exists(), "{io}");
        let _ = std::fs::remove_file(server.dir.join("failpoint.checkpoint-turned-over.1"));

        server.aof = false;
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "k"]).s(), "in-dump", "{io}");
        assert!(c.cmd(&["SET", "k", "new"]).ok());
        send(&mut c, &["CONFIG", "SET", "appendonly", "yes"]);
        assert!(server.reached("checkpoint-turned-over.1", LONG), "{io}");
        drop(c);
        server.kill();

        server.aof = true;
        server.failpoints.clear();
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "k"]).s(),
            "in-dump",
            "{io}: the earlier run's old log was replayed"
        );
    }
}

/// Stopped while it removes what old logs left (`appendonly.aof.prev`, and a
/// turn-over's notes), a new log must leave files the next start reads.
/// Removed first, `appendonly.aof.prev` left the notes without it, and the
/// next start refused to go on.
#[test]
fn a_crash_while_a_new_log_replaces_the_old_ones_starts_and_replays_nothing_old() {
    for &io in io_models() {
        for stop in 1..=3 {
            let mut server = Server::start(io, "");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "k", "in-dump"]).ok());
            assert!(c.cmd(&["SAVE"]).ok());
            assert!(c.cmd(&["SET", "k", "stale"]).ok());
            durable(&mut c);
            drop(c);
            server.kill();

            // As a crash part-way through adding a log to an old one left
            // them: the old log, the log, the new log under its own name,
            // and the note of how long the old log was.
            let dir = server.dir.clone();
            let aof = dir.join("appendonly.aof");
            let old = std::fs::read(&aof).unwrap();
            let number = u64::from_le_bytes(old[8..16].try_into().unwrap());
            std::fs::rename(&aof, dir.join("appendonly.aof.prev")).unwrap();
            std::fs::write(&aof, delta_header(number + 1)).unwrap();
            std::fs::write(
                dir.join("appendonly.aof.prev.len"),
                (old.len() as u64).to_le_bytes(),
            )
            .unwrap();
            std::fs::write(
                dir.join("appendonly.aof.rewrite.tmp"),
                delta_header(number + 2),
            )
            .unwrap();

            server.aof = false;
            server.failpoints = "new-log-removing".into();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "k"]).s(), "in-dump", "{io}");
            send(&mut c, &["CONFIG", "SET", "appendonly", "yes"]);
            for hit in 1..stop {
                server.pass(&format!("new-log-removing.{hit}"));
            }
            assert!(
                server.reached(&format!("new-log-removing.{stop}"), LONG),
                "{io}"
            );
            drop(c);
            server.kill();

            server.aof = true;
            server.failpoints.clear();
            let mut c = server.restart();
            assert_eq!(
                c.cmd(&["GET", "k"]).s(),
                "in-dump",
                "{io}, stopped at removal {stop}: an old log was replayed"
            );
            assert!(c.cmd(&["SET", "k", "after"]).ok());
            durable(&mut c);
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "k"]).s(), "after", "{io}, stop {stop}");
        }
    }
}

// ── Logs left out are kept aside, never destroyed ────────────────────────────

/// The logs kept aside in `dir` (`superseded.*`), with what is in them.
fn kept_aside(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut kept: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.starts_with("superseded.")
                .then(|| (name, std::fs::read(entry.path()).unwrap()))
        })
        .collect();
    kept.sort();
    kept
}

fn holds(bytes: &[u8], what: &[u8]) -> bool {
    bytes.windows(what.len()).any(|w| w == what)
}

/// `SET <key> <value>` as the log holds it.
fn set_record(key: &str, value: &str) -> Vec<u8> {
    format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n{value}\r\n",
        key.len(),
        value.len()
    )
    .into_bytes()
}

/// A run with the AOF on (a key in its dump, one in its log), then a save
/// with the AOF off on a newer dump: what 1.2.0 left, given back as 1.2.0
/// left it (the save number of the older dump, the log of the older run).
/// Returns the log and the save number as they were before the save.
fn a_dump_saved_by_1_2_0_with_the_aof_off(server: &mut Server) -> (Vec<u8>, Vec<u8>) {
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "in-dump", "1"]).ok());
    assert!(c.cmd(&["SAVE"]).ok());
    assert!(c.cmd(&["SET", "in-log", "1"]).ok());
    durable(&mut c);
    drop(c);
    server.kill();
    let log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();
    let saved = std::fs::read(server.dir.join("dump.kdb.gen")).unwrap();
    assert!(log.starts_with(b"KIVIDLT1"));

    server.aof = false;
    let mut c = server.restart();
    assert!(c.cmd(&["SET", "newer", "1"]).ok());
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    server.kill();
    std::fs::write(server.dir.join("dump.kdb.gen"), &saved).unwrap();
    (log, saved)
}

/// 1.2.0, after a save with the AOF off, was started with the AOF on: it
/// replayed the old log over the newer dump, and went on adding to it,
/// acknowledged writes, until it was killed. The files look like those of
/// the save alone, and recovery leaves the log out; it was overwritten, and
/// the writes made after the restart were gone. It is kept aside, whole.
#[test]
fn a_log_left_out_after_a_restart_on_1_2_0_is_kept_aside_with_its_writes() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let (mut log, _) = a_dump_saved_by_1_2_0_with_the_aof_off(&mut server);
        log.extend_from_slice(&set_record("acked-after-restart", "1"));
        std::fs::write(server.dir.join("appendonly.aof"), &log).unwrap();

        server.aof = true;
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "newer"]).s(), "1", "{io}");
        let kept = kept_aside(&server.dir);
        assert!(
            kept.iter()
                .any(|(_, bytes)| holds(bytes, b"acked-after-restart")),
            "{io}: the log left out was not kept: {:?}",
            kept.iter().map(|(name, _)| name).collect::<Vec<_>>()
        );
    }
}

/// 1.2.0 crashed during the checkpoint of a `CONFIG SET appendonly yes`,
/// after a save with the AOF off: the old log, with the writes made after
/// the enable at its end, and a new one. Recovery leaves both out; the old
/// one was removed, and those writes with it. It is kept aside.
#[test]
fn an_old_log_left_out_after_a_1_2_0_enable_crashed_is_kept_aside_with_its_writes() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let (mut log, saved) = a_dump_saved_by_1_2_0_with_the_aof_off(&mut server);
        log.extend_from_slice(&set_record("acked-after-enable", "1"));
        std::fs::write(server.dir.join("appendonly.aof.prev"), &log).unwrap();
        let number = u64::from_le_bytes(saved[0..8].try_into().unwrap());
        std::fs::write(server.dir.join("appendonly.aof"), delta_header(number + 1)).unwrap();

        server.aof = true;
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "newer"]).s(), "1", "{io}");
        let kept = kept_aside(&server.dir);
        assert!(
            kept.iter()
                .any(|(_, bytes)| holds(bytes, b"acked-after-enable")),
            "{io}: the old log left out was not kept: {:?}",
            kept.iter().map(|(name, _)| name).collect::<Vec<_>>()
        );
    }
}

/// A run with the AOF on, killed: an acknowledged write is in its log and
/// nowhere else. Started again with `appendonly no`, a `SAVE` or `SHUTDOWN`
/// emptied that log. With the AOF off the AOF's files are left alone, as in
/// 1.2.0 and Redis; a start with the AOF on then leaves the log out, as older
/// than the dump, and keeps it aside, and says so at every start.
#[test]
fn a_save_with_the_aof_off_leaves_the_log_alone() {
    for &io in io_models() {
        for end in ["SAVE", "SHUTDOWN"] {
            let mut server = Server::start(io, "");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "acked-in-run-1", "1"]).ok());
            durable(&mut c);
            drop(c);
            server.kill();
            let log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();

            server.aof = false;
            let mut c = server.restart();
            assert!(c.cmd(&["SET", "run-2", "1"]).ok());
            if end == "SHUTDOWN" {
                send(&mut c, &["SHUTDOWN"]);
                drop(c);
                server.wait_exit();
            } else {
                assert!(c.cmd(&["SAVE"]).ok());
                drop(c);
                server.kill();
            }
            assert_eq!(
                std::fs::read(server.dir.join("appendonly.aof")).unwrap(),
                log,
                "{io}, {end}: a save with the AOF off changed the log"
            );

            server.aof = true;
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "run-2"]).s(), "1", "{io}, {end}");
            assert_eq!(
                c.cmd(&["GET", "acked-in-run-1"]),
                V::Nil,
                "{io}, {end}: the older log was replayed over the dump"
            );
            let kept = kept_aside(&server.dir);
            assert!(
                kept.iter()
                    .any(|(_, bytes)| holds(bytes, b"acked-in-run-1")),
                "{io}, {end}: the log left out was not kept"
            );
            drop(c);
            server.kill();
            let _ = server.restart();
            let said = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
            assert!(
                said.matches(kept[0].0.as_str()).count() >= 2,
                "{io}, {end}: the next start did not name the kept log"
            );
        }
    }
}

/// A log made at a start with the AOF on had no header: a log of an age no
/// recovery could tell, replayed over any dump saved after it.
#[test]
fn a_log_made_at_a_start_with_the_aof_on_has_a_header() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();
        assert!(
            log.starts_with(b"KIVIDLT1"),
            "{io}: a new log with no header: {log:?}"
        );
        // And an empty one is given one too.
        server.kill();
        std::fs::write(server.dir.join("appendonly.aof"), b"").unwrap();
        let _ = server.restart();
        let log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();
        assert!(
            log.starts_with(b"KIVIDLT1"),
            "{io}: an empty log was opened as it was"
        );
    }
}

// ── Logs added to after a save with the AOF off (#359) ───────────────────────

/// `INCR <key>` as the log holds it.
fn incr_record(key: &str) -> Vec<u8> {
    format!("*2\r\n$4\r\nINCR\r\n${}\r\n{key}\r\n", key.len()).into_bytes()
}

/// The save number in `dump.kdb.gen`, and whether it marks the dump as one
/// no log goes with.
fn saved_number(dir: &Path) -> (u64, bool) {
    let saved = std::fs::read(dir.join("dump.kdb.gen")).unwrap();
    (
        u64::from_le_bytes(saved[0..8].try_into().unwrap()),
        saved.get(24) == Some(&1),
    )
}

/// A save with the AOF off and no log in `dir`: `dump.kdb.gen` marks the
/// dump as one no log goes with. 1.2.0, started on that directory with the
/// AOF on, made a log with no header on top of the dump, added acknowledged
/// writes to it, and was killed before any checkpoint. Recovery took the
/// log for one older than the dump: those writes were left out, and were
/// only in the copy kept aside. The log has been added to since the save,
/// so it goes on from the dump: it is replayed, whole, as 1.2.0 replayed it.
#[test]
fn a_log_with_no_header_made_after_a_save_with_the_aof_off_is_replayed() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        server.kill();
        std::fs::remove_file(server.dir.join("appendonly.aof")).unwrap();
        server.aof = false;
        let mut c = server.restart();
        assert!(c.cmd(&["SET", "in-dump", "1"]).ok());
        assert_eq!(c.cmd(&["INCR", "n"]).i(), 1);
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();
        assert!(!server.dir.join("appendonly.aof").exists(), "{io}");
        assert!(saved_number(&server.dir).1, "{io}: the dump is not marked");

        // What 1.2.0 made of it: a log with no header, of writes on top of
        // the dump, each acknowledged.
        let mut log = set_record("acked-on-1.2.0", "1");
        log.extend_from_slice(&incr_record("n"));
        std::fs::write(server.dir.join("appendonly.aof"), &log).unwrap();

        server.aof = true;
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "acked-on-1.2.0"]),
            V::Str("1".into()),
            "{io}: the writes made on top of the dump were left out"
        );
        assert_eq!(c.cmd(&["GET", "n"]).s(), "2", "{io}");
        assert_eq!(c.cmd(&["GET", "in-dump"]).s(), "1", "{io}");

        // And the log goes on: what is written next is kept too.
        assert!(c.cmd(&["SET", "after", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "acked-on-1.2.0"]).s(), "1", "{io}");
        assert_eq!(c.cmd(&["GET", "n"]).s(), "2", "{io}");
        assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{io}");
    }
}

/// A log with no header that was there at a save with the AOF off, and is
/// as it was then, is older than the dump: still left out, kept aside, and
/// counted in `INFO persistence` until it is removed.
#[test]
fn a_log_with_no_header_as_it_was_at_a_save_with_the_aof_off_is_left_out() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        server.kill();
        let log = set_record("older", "1");
        std::fs::write(server.dir.join("appendonly.aof"), &log).unwrap();
        server.aof = false;
        let mut c = server.restart();
        assert!(c.cmd(&["SET", "in-dump", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();

        server.aof = true;
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "in-dump"]).s(), "1", "{io}");
        assert_eq!(
            c.cmd(&["GET", "older"]),
            V::Nil,
            "{io}: a log older than the dump was replayed over it"
        );
        let kept = kept_aside(&server.dir);
        assert_eq!(kept.len(), 1, "{io}: {kept:?}");
        assert_eq!(kept[0].1, log, "{io}");
        assert_eq!(info_field(&mut c, "aof_superseded_files"), "1", "{io}");
        assert_eq!(
            info_field(&mut c, "aof_superseded_bytes"),
            log.len().to_string(),
            "{io}"
        );
        std::fs::remove_file(server.dir.join(&kept[0].0)).unwrap();
        assert_eq!(info_field(&mut c, "aof_superseded_files"), "0", "{io}");
        assert_eq!(info_field(&mut c, "aof_superseded_bytes"), "0", "{io}");
    }
}

/// 1.2.0 leaves out a log whose save number is older than the dump's, and
/// then adds to it. Started with the AOF on after a save with the AOF off, it
/// did that to the log the save left alone, and was killed before any
/// checkpoint: alone, or part-way through one, with the log moved to
/// `appendonly.aof.prev` and a new log after it. What was added goes on from
/// the dump, and is replayed; what the log held before the save, which the
/// dump holds, is not.
#[test]
fn an_older_log_added_to_after_a_save_with_the_aof_off_replays_what_was_added() {
    for &io in io_models() {
        for left in ["log", "old log and log"] {
            let mut server = Server::start(io, "");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "a", "1"]).ok());
            assert!(c.cmd(&["SAVE"]).ok());
            assert_eq!(c.cmd(&["INCR", "n"]).i(), 1);
            durable(&mut c);
            assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
            assert!(c.cmd(&["SAVE"]).ok());
            drop(c);
            server.kill();
            let (number, marked) = saved_number(&server.dir);
            assert!(marked, "{io}");
            let mut log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();
            assert!(holds(&log, b"INCR"), "{io}: {log:?}");

            // 1.2.0 on it, with the AOF on: n was 1 in the dump; it served 2.
            log.extend_from_slice(&incr_record("n"));
            log.extend_from_slice(&set_record("acked-on-1.2.0", "1"));
            if left == "log" {
                std::fs::write(server.dir.join("appendonly.aof"), &log).unwrap();
            } else {
                std::fs::write(server.dir.join("appendonly.aof.prev"), &log).unwrap();
                let mut new = delta_header(number + 1);
                new.extend_from_slice(&set_record("acked-after-the-cut", "1"));
                std::fs::write(server.dir.join("appendonly.aof"), &new).unwrap();
            }

            server.aof = true;
            let mut c = server.restart();
            assert_eq!(
                c.cmd(&["GET", "acked-on-1.2.0"]),
                V::Str("1".into()),
                "{io}, {left}: what was added was left out"
            );
            assert_eq!(
                c.cmd(&["GET", "n"]).s(),
                "2",
                "{io}, {left}: the records from before the save were replayed too"
            );
            assert_eq!(c.cmd(&["GET", "a"]).s(), "1", "{io}, {left}");
            if left != "log" {
                assert_eq!(
                    c.cmd(&["GET", "acked-after-the-cut"]),
                    V::Str("1".into()),
                    "{io}, {left}"
                );
            }
            assert!(
                kept_aside(&server.dir)
                    .iter()
                    .any(|(_, bytes)| *bytes == log),
                "{io}, {left}: the old log was not kept aside whole"
            );

            // It goes on from the dump: what is written next is kept, and
            // nothing is replayed twice.
            assert_eq!(c.cmd(&["INCR", "n"]).i(), 3);
            durable(&mut c);
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "n"]).s(), "3", "{io}, {left}");
            assert_eq!(c.cmd(&["GET", "acked-on-1.2.0"]).s(), "1", "{io}, {left}");
        }
    }
}

/// Stopped after it puts its save number in place and before it puts its
/// dump in place, a checkpoint leaves a number that does not describe the
/// dump there: recovery replays the old log and the new one on the old dump,
/// and a save with the AOF off is as if it had not run. The directory is
/// synced between the two renames, so a power cut, too, can leave only this
/// order, not the dump renamed without its number (#359).
#[test]
fn a_checkpoint_stopped_between_its_number_and_its_dump_loses_nothing() {
    for &io in io_models() {
        for aof_at_save in [true, false] {
            let mut server = Server::start(io, "dump-installing");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "before", "1"]).ok());
            send(&mut c, &["SAVE"]);
            server.pass("dump-installing.1");
            assert!(c.read().ok());
            assert!(c.cmd(&["SET", "in-log", "1"]).ok());
            durable(&mut c);
            if aof_at_save {
                // In the background: a `SAVE` holds the server.
                assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
            } else {
                assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
                send(&mut c, &["SAVE"]);
            }
            assert!(
                server.reached("dump-installing.2", LONG),
                "{io}, aof {aof_at_save}"
            );
            if aof_at_save {
                // After the cut: in the new log.
                let mut w = server.conn();
                assert!(w.cmd(&["SET", "during", "1"]).ok());
                durable(&mut w);
            }
            drop(c);
            server.kill();

            server.failpoints.clear();
            let mut c = server.restart();
            assert_eq!(
                c.cmd(&["GET", "before"]).s(),
                "1",
                "{io}, aof {aof_at_save}"
            );
            assert_eq!(
                c.cmd(&["GET", "in-log"]),
                V::Str("1".into()),
                "{io}, aof {aof_at_save}: an acknowledged write was lost"
            );
            if aof_at_save {
                assert_eq!(
                    c.cmd(&["GET", "during"]),
                    V::Str("1".into()),
                    "{io}: a write after the cut was lost"
                );
            }
            assert!(
                kept_aside(&server.dir).is_empty(),
                "{io}, aof {aof_at_save}"
            );
        }
    }
}

/// The names in `dir` of the files a dump is written to before it is put in
/// place, `dump.kdb.<pid>.<n>.tmp`.
fn temp_dumps(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| {
            name.strip_prefix("dump.kdb.")
                .and_then(|rest| rest.strip_suffix(".tmp"))
                .and_then(|rest| rest.split_once('.'))
                .is_some_and(|(pid, n)| {
                    [pid, n]
                        .iter()
                        .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
                })
        })
        .collect();
    names.sort();
    names
}

/// A checkpoint cut short (a kill -9, the OOM killer, a liveness probe)
/// leaves the file its dump was being written to, as big as the dump, which
/// nothing ever reads. The next start, once it holds the data directory,
/// removes those, whichever process wrote them -- in a container every start
/// has the same pid -- and says so. Each crash used to leave one more, until
/// the volume was full (#545). Files named otherwise are left alone.
#[test]
fn a_start_removes_the_dumps_of_checkpoints_cut_short() {
    for &io in io_models() {
        for (save, started) in [
            ("BGSAVE", "Background saving started"),
            ("BGREWRITEAOF", STARTED),
        ] {
            let mut server = Server::start(io, "dump-installing");
            let mut c = server.conn();
            assert!(c.cmd(&["SET", "k", "1"]).ok());
            durable(&mut c);
            assert_eq!(c.cmd(&[save]).s(), started, "{io}, {save}");
            assert!(server.reached("dump-installing.1", LONG), "{io}, {save}");
            let cut_short = temp_dumps(&server.dir);
            assert_eq!(cut_short.len(), 1, "{io}, {save}: {cut_short:?}");
            // As a start with the same pid would have left it.
            std::fs::write(server.dir.join("dump.kdb.1.0.tmp"), b"cut short").unwrap();
            let others = [
                "dump.kdb.1.tmp",
                "dump.kdb.x.0.tmp",
                "dump.kdb.1.0.tmp.bak",
                "old.dump.kdb.1.0.tmp",
            ];
            for name in others {
                std::fs::write(server.dir.join(name), b"not a dump of ours").unwrap();
            }
            drop(c);
            server.kill();

            server.failpoints.clear();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "k"]).s(), "1", "{io}, {save}");
            assert_eq!(
                temp_dumps(&server.dir),
                Vec::<String>::new(),
                "{io}, {save}"
            );
            for name in others {
                assert!(
                    server.dir.join(name).exists(),
                    "{io}, {save}: {name} removed"
                );
            }
            let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
            for name in [cut_short[0].as_str(), "dump.kdb.1.0.tmp"] {
                assert!(
                    log.lines()
                        .any(|line| line.contains("Removed") && line.contains(name)),
                    "{io}, {save}: no line says {name} was removed"
                );
            }
        }
    }
}

/// A save with the AOF off beside an old log and a log, both holding
/// records the dataset has: a checkpoint was stopped part-way, the next start
/// replayed both, and the AOF was then turned off and the dataset saved.
/// `n` and `m` are 1 in the dump, each from one `INCR` in one of the logs.
/// Returns the old log and the log as they were at that save, and the save
/// number.
fn a_save_with_the_aof_off_beside_an_old_log_and_a_log(
    server: &mut Server,
) -> (Vec<u8>, Vec<u8>, u64) {
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "a", "1"]).ok());
    assert!(c.cmd(&["SAVE"]).ok());
    assert_eq!(c.cmd(&["INCR", "n"]).i(), 1);
    durable(&mut c);
    drop(c);
    server.kill();
    // As a checkpoint stopped after its turn-over left them.
    let dir = server.dir.clone();
    let old = std::fs::read(dir.join("appendonly.aof")).unwrap();
    let number = u64::from_le_bytes(old[8..16].try_into().unwrap());
    std::fs::rename(dir.join("appendonly.aof"), dir.join("appendonly.aof.prev")).unwrap();
    let mut log = delta_header(number + 1);
    log.extend_from_slice(&incr_record("m"));
    std::fs::write(dir.join("appendonly.aof"), &log).unwrap();

    let mut c = server.restart();
    assert_eq!(c.cmd(&["GET", "n"]).s(), "1", "{}", server.io);
    assert_eq!(c.cmd(&["GET", "m"]).s(), "1", "{}", server.io);
    assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    server.kill();
    let (saved, marked) = saved_number(&dir);
    assert!(marked, "{}", server.io);
    assert_eq!(std::fs::read(dir.join("appendonly.aof.prev")).unwrap(), old);
    assert_eq!(std::fs::read(dir.join("appendonly.aof")).unwrap(), log);
    (old, log, saved)
}

/// 1.2.0, started with the AOF on after that save, added a write to the log
/// (`INCR m`: it served 2), and a checkpoint of its own then added the log's
/// records to the old log, which it still needed, and was stopped before it
/// wrote its save number. The old log has been added to since the save, and
/// what was added to it starts with the log's records from before the save,
/// which the dump holds: replayed from where the old log was at the save,
/// `INCR m` was applied a second time (#359). Those are skipped.
#[test]
fn an_old_log_a_turn_over_added_the_log_to_replays_only_what_came_after_the_save() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let (old, log, number) = a_save_with_the_aof_off_beside_an_old_log_and_a_log(&mut server);
        let dir = server.dir.clone();
        let mut prev = old.clone();
        prev.extend_from_slice(&log[16..]);
        prev.extend_from_slice(&incr_record("m"));
        std::fs::write(dir.join("appendonly.aof.prev"), &prev).unwrap();
        let mut new = delta_header(number + 1);
        new.extend_from_slice(&set_record("after-the-cut", "1"));
        std::fs::write(dir.join("appendonly.aof"), &new).unwrap();

        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "m"]).s(),
            "2",
            "{io}: m must be the dump's 1 plus the INCR added since (3: a record the dump              holds was applied again; 1: the INCR added since was left out)"
        );
        assert_eq!(c.cmd(&["GET", "n"]).s(), "1", "{io}");
        assert_eq!(c.cmd(&["GET", "after-the-cut"]), V::Str("1".into()), "{io}");
        assert_eq!(c.cmd(&["GET", "a"]).s(), "1", "{io}");
        assert!(
            kept_aside(&dir).iter().any(|(_, bytes)| *bytes == prev),
            "{io}: the old log was not kept aside whole"
        );

        // It goes on from there.
        assert_eq!(c.cmd(&["INCR", "m"]).i(), 3);
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "m"]).s(), "3", "{io}");
    }
}

/// 1.2.0, started with the AOF on after that save, added a write to the log
/// and was killed. The old log is as it was at the save, older than the dump;
/// the log beside it was left out with it, and the write added was only in
/// the copy kept aside (#359). What was added to the log is replayed.
#[test]
fn a_log_added_to_beside_an_older_old_log_replays_what_was_added() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let (old, mut log, _) = a_save_with_the_aof_off_beside_an_old_log_and_a_log(&mut server);
        let dir = server.dir.clone();
        log.extend_from_slice(&incr_record("m"));
        log.extend_from_slice(&set_record("acked-on-1.2.0", "1"));
        std::fs::write(dir.join("appendonly.aof"), &log).unwrap();

        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "acked-on-1.2.0"]),
            V::Str("1".into()),
            "{io}: what was added to the log was left out"
        );
        assert_eq!(c.cmd(&["GET", "m"]).s(), "2", "{io}");
        assert_eq!(c.cmd(&["GET", "n"]).s(), "1", "{io}");
        let kept = kept_aside(&dir);
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == old),
            "{io}: the old log was not kept"
        );
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == log),
            "{io}: the log was not kept"
        );

        assert_eq!(c.cmd(&["INCR", "m"]).i(), 3);
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "m"]).s(), "3", "{io}");
        assert_eq!(c.cmd(&["GET", "acked-on-1.2.0"]).s(), "1", "{io}");
    }
}

// ── A log beside an old log older than the dump is judged on its own (#363) ──

/// A log with no header beside an old log, both replayed at a start, then a
/// save with the AOF off. 1.2.0, started with the AOF on, replayed both over
/// the dump and added to the log, which has no header. The old log is as it
/// was at the save, older than the dump, and the log was left out with it:
/// the writes added were only in the copy kept aside. Judged on its own, a
/// log with no header that has changed since the save is replayed, whole, as
/// 1.2.0 replayed it.
#[test]
fn a_log_with_no_header_written_to_beside_an_older_old_log_is_replayed() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        assert!(c.cmd(&["SET", "a", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert_eq!(c.cmd(&["INCR", "n"]).i(), 1);
        durable(&mut c);
        drop(c);
        server.kill();
        let dir = server.dir.clone();
        std::fs::rename(dir.join("appendonly.aof"), dir.join("appendonly.aof.prev")).unwrap();
        let old = std::fs::read(dir.join("appendonly.aof.prev")).unwrap();
        // A log of an earlier release, with no header.
        let mut log = incr_record("m");
        std::fs::write(dir.join("appendonly.aof"), &log).unwrap();

        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "n"]).s(), "1", "{io}");
        assert_eq!(c.cmd(&["GET", "m"]).s(), "1", "{io}");
        assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();
        assert!(saved_number(&dir).1, "{io}");
        assert_eq!(std::fs::read(dir.join("appendonly.aof")).unwrap(), log);

        // 1.2.0 on it, with the AOF on: it replayed the old log and the log
        // over the dump (m was 2), then added to the log.
        log.extend_from_slice(&incr_record("m"));
        log.extend_from_slice(&set_record("acked-on-1.2.0", "1"));
        std::fs::write(dir.join("appendonly.aof"), &log).unwrap();

        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "acked-on-1.2.0"]),
            V::Str("1".into()),
            "{io}: the log was left out with the old log"
        );
        assert_eq!(
            c.cmd(&["GET", "m"]).s(),
            "3",
            "{io}: the log is replayed whole, as 1.2.0 replayed it"
        );
        assert_eq!(
            c.cmd(&["GET", "n"]).s(),
            "1",
            "{io}: the old log was replayed again"
        );
        let kept = kept_aside(&dir);
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == old),
            "{io}: the old log was not kept"
        );
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == log),
            "{io}: the log was not kept"
        );

        assert_eq!(c.cmd(&["INCR", "m"]).i(), 4);
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "m"]).s(), "4", "{io}");
        assert_eq!(c.cmd(&["GET", "acked-on-1.2.0"]).s(), "1", "{io}");
    }
}

/// An old log with no log beside it (a checkpoint stopped after moving the
/// log aside), left alone by a start with the AOF off, then a save. 1.2.0,
/// started with the AOF on, put a new log in place, a delta on the next save
/// number, and added acknowledged writes to it. The old log is older than the
/// dump, and the log was left out with it. There was no log at the save, so
/// all of the log was written since: it is replayed.
#[test]
fn a_log_made_after_a_save_with_no_log_beside_an_older_old_log_is_replayed() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        assert!(c.cmd(&["SET", "a", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert_eq!(c.cmd(&["INCR", "n"]).i(), 1);
        durable(&mut c);
        drop(c);
        server.kill();
        let dir = server.dir.clone();
        std::fs::rename(dir.join("appendonly.aof"), dir.join("appendonly.aof.prev")).unwrap();
        let old = std::fs::read(dir.join("appendonly.aof.prev")).unwrap();

        server.aof = false;
        let mut c = server.restart();
        assert!(c.cmd(&["SET", "b", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();
        let (number, marked) = saved_number(&dir);
        assert!(marked, "{io}");
        assert!(!dir.join("appendonly.aof").exists(), "{io}");

        // 1.2.0 on it, with the AOF on.
        let mut log = delta_header(number + 1);
        log.extend_from_slice(&set_record("acked-on-1.2.0", "1"));
        log.extend_from_slice(&incr_record("m"));
        std::fs::write(dir.join("appendonly.aof"), &log).unwrap();

        server.aof = true;
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "acked-on-1.2.0"]),
            V::Str("1".into()),
            "{io}: the log was left out with the old log"
        );
        assert_eq!(c.cmd(&["GET", "m"]).s(), "1", "{io}");
        assert_eq!(c.cmd(&["GET", "b"]).s(), "1", "{io}");
        assert_eq!(c.cmd(&["GET", "a"]).s(), "1", "{io}");
        let kept = kept_aside(&dir);
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == old),
            "{io}: the old log was not kept"
        );
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == log),
            "{io}: the log was not kept"
        );

        assert_eq!(c.cmd(&["INCR", "m"]).i(), 2);
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "m"]).s(), "2", "{io}");
        assert_eq!(c.cmd(&["GET", "acked-on-1.2.0"]).s(), "1", "{io}");
    }
}

// ── A turn-over stopped, then a save with the AOF off (#368) ─────────────────

/// A run with the AOF on, killed by a checkpoint after the log was moved
/// aside and before the new log took its name: `appendonly.aof.prev` holds
/// an acknowledged write (`r1`) that is in no dump, the new log is under a
/// name of its own, and there is no log. Returns the old log.
fn a_turn_over_stopped_after_moving_the_log_aside(server: &mut Server) -> Vec<u8> {
    let io = server.io;
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "a", "1"]).ok());
    send(&mut c, &["SAVE"]);
    server.pass("turn-over-moved-aside.1");
    assert!(c.read().ok(), "{io}");
    assert!(c.cmd(&["SET", "r1", "acked"]).ok());
    durable(&mut c);
    assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
    assert!(server.reached("turn-over-moved-aside.2", LONG), "{io}");
    drop(c);
    server.kill();
    let dir = &server.dir;
    assert!(!dir.join("appendonly.aof").exists(), "{io}");
    assert!(dir.join("appendonly.aof.rewrite.tmp").exists(), "{io}");
    let old = std::fs::read(dir.join("appendonly.aof.prev")).unwrap();
    assert!(holds(&old, b"r1"), "{io}");
    server.failpoints.clear();
    old
}

/// Then a run with the AOF off, which never loads the old log, saved; the
/// save took the new log's save number, which it did not count. The next
/// start with the AOF on put the new log in place, read the old log as that
/// of a checkpoint that finished, and deleted it: `r1` was gone (#368). The
/// old log is kept aside.
#[test]
fn an_old_log_left_by_a_stopped_turn_over_is_kept_after_a_save_with_the_aof_off() {
    for &io in io_models() {
        for end in ["SAVE", "SHUTDOWN"] {
            let mut server = Server::start(io, "turn-over-moved-aside");
            let old = a_turn_over_stopped_after_moving_the_log_aside(&mut server);
            let dir = server.dir.clone();

            server.aof = false;
            let mut c = server.restart();
            assert!(c.cmd(&["SET", "b", "1"]).ok());
            if end == "SHUTDOWN" {
                send(&mut c, &["SHUTDOWN"]);
                drop(c);
                server.wait_exit();
            } else {
                assert!(c.cmd(&["SAVE"]).ok());
                drop(c);
                server.kill();
            }

            server.aof = true;
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "b"]).s(), "1", "{io}, {end}");
            assert_eq!(c.cmd(&["GET", "a"]).s(), "1", "{io}, {end}");
            assert!(
                kept_aside(&dir).iter().any(|(_, bytes)| *bytes == old),
                "{io}, {end}: the old log, with r1, was deleted"
            );
            assert_ne!(
                info_field(&mut c, "aof_superseded_files"),
                "0",
                "{io}, {end}"
            );

            // And the log goes on from the dump.
            assert!(c.cmd(&["SET", "after", "1"]).ok());
            durable(&mut c);
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{io}, {end}");
            assert!(
                kept_aside(&dir).iter().any(|(_, bytes)| *bytes == old),
                "{io}, {end}"
            );
        }
    }
}

/// The same, with two saves with the AOF off, and then 1.2.0 started with
/// the AOF on: it put the turn-over's new log in place, a delta on a number
/// below the dump's, and added an acknowledged write to it. There was no log
/// at the save, so all of the log was written since: it is replayed, not
/// left out as older than the dump.
#[test]
fn a_log_made_after_a_save_with_no_log_is_replayed_whatever_its_number() {
    for &io in io_models() {
        let mut server = Server::start(io, "turn-over-moved-aside");
        let old = a_turn_over_stopped_after_moving_the_log_aside(&mut server);
        let dir = server.dir.clone();

        server.aof = false;
        let mut c = server.restart();
        assert!(c.cmd(&["SET", "b", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert!(c.cmd(&["SET", "b", "2"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();

        // 1.2.0 on it, with the AOF on.
        std::fs::rename(
            dir.join("appendonly.aof.rewrite.tmp"),
            dir.join("appendonly.aof"),
        )
        .unwrap();
        let mut log = std::fs::read(dir.join("appendonly.aof")).unwrap();
        assert_eq!(log.len(), 16, "{io}");
        log.extend_from_slice(&set_record("acked-on-1.2.0", "1"));
        std::fs::write(dir.join("appendonly.aof"), &log).unwrap();

        server.aof = true;
        let mut c = server.restart();
        assert_eq!(
            c.cmd(&["GET", "acked-on-1.2.0"]),
            V::Str("1".into()),
            "{io}: a log written after the save was left out as older"
        );
        assert_eq!(c.cmd(&["GET", "b"]).s(), "2", "{io}");
        let kept = kept_aside(&dir);
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == old),
            "{io}: the old log was not kept"
        );
        assert!(
            kept.iter().any(|(_, bytes)| *bytes == log),
            "{io}: the log was not kept"
        );

        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "acked-on-1.2.0"]).s(), "1", "{io}");
    }
}

// ── The old log of a checkpoint that finished, not removable (#375) ─────────

/// An `appendonly.aof.prev` that cannot be removed: a directory under its
/// name, which `remove_file` refuses on every platform.
fn an_old_log_that_cannot_be_removed(dir: &Path) {
    let prev = dir.join("appendonly.aof.prev");
    std::fs::create_dir(&prev).unwrap();
    std::fs::write(prev.join("inside"), b"*1\r\n$4\r\nPING\r\n").unwrap();
}

/// A checkpoint finished and its old log was not removed; at the next start
/// it cannot be. The dump holds its writes, so it is left in place, with a
/// warning: the server used to ignore this, and 1.2.1 refused to start.
#[test]
fn an_old_log_that_cannot_be_removed_is_left_in_place() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        assert!(c.cmd(&["SET", "k", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert!(c.cmd(&["SET", "k2", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        an_old_log_that_cannot_be_removed(&server.dir);

        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "k"]).s(), "1", "{io}");
        assert_eq!(c.cmd(&["GET", "k2"]).s(), "1", "{io}");
        let said = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
        assert!(
            said.contains(
                "AOF: appendonly.aof.prev, left by a checkpoint that finished, could not be \
                 removed: "
            ),
            "{io}: no warning"
        );
        assert!(
            said.contains(
                "Until it is removed, every checkpoint fails (INFO persistence shows \
                 aof_last_bgrewrite_status:err)"
            ),
            "{io}"
        );
        assert!(server.dir.join("appendonly.aof.prev").exists(), "{io}");
    }
}

/// The same after a save with the AOF off: the old log may hold writes the
/// dump does not, and it cannot be kept aside, so the server does not start,
/// and says what to do, not the generic "start empty" advice. Nothing is
/// removed.
#[test]
fn an_old_log_that_cannot_be_kept_aside_stops_the_start_saying_what_to_do() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        server.kill();
        std::fs::remove_file(server.dir.join("appendonly.aof")).unwrap();
        server.aof = false;
        let mut c = server.restart();
        assert!(c.cmd(&["SET", "k", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();
        assert!(saved_number(&server.dir).1, "{io}");

        // A log on that dump's number, as a start with the AOF on makes.
        server.aof = true;
        let mut c = server.restart();
        assert!(c.cmd(&["SET", "k2", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        let (number, _) = saved_number(&server.dir);
        let log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();
        assert_eq!(log[..16], delta_header(number)[..], "{io}");
        an_old_log_that_cannot_be_removed(&server.dir);

        // Not `restart`: it waits for the server to answer.
        server.child = launch(&server.dir, server.port, server.io, "", true);
        server.wait_exit();
        let status = server.child.wait().unwrap();
        assert!(!status.success(), "{io}: {status:?}");
        let said = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
        assert!(
            said.contains(
                "appendonly.aof.prev, left by a checkpoint that finished, cannot be kept aside: "
            ),
            "{io}: {said}"
        );
        assert!(
            said.contains(
                ". dump.kdb was saved with the AOF off since, and may not hold its writes, so it \
                 is not removed without a copy. Free disk space in the data directory, or move \
                 appendonly.aof.prev out of it, and start again"
            ),
            "{io}: {said}"
        );
        assert!(
            !said.contains("Refusing to start with an empty dataset"),
            "{io}: the generic advice"
        );
        assert!(
            server
                .dir
                .join("appendonly.aof.prev")
                .join("inside")
                .exists(),
            "{io}"
        );
        assert_eq!(
            std::fs::read(server.dir.join("appendonly.aof")).unwrap(),
            log,
            "{io}"
        );
    }
}

/// After a save with the AOF off and a start with the AOF on, a turn-over
/// finds an old log beside the log, on the dump's number: one of a
/// checkpoint that finished. It may hold writes the dump does not, and it
/// is kept aside before the turn-over removes it, the keeping done before
/// the log is locked (#375).
#[test]
fn a_turn_over_keeps_a_finished_checkpoints_old_log_aside_after_a_save_with_the_aof_off() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        server.kill();
        std::fs::remove_file(server.dir.join("appendonly.aof")).unwrap();
        server.aof = false;
        let mut c = server.restart();
        assert!(c.cmd(&["SET", "k", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        drop(c);
        server.kill();

        server.aof = true;
        let mut c = server.restart();
        let (number, _) = saved_number(&server.dir);
        let mut old = delta_header(number - 1);
        old.extend_from_slice(&set_record("only-in-the-old-log", "1"));
        std::fs::write(server.dir.join("appendonly.aof.prev"), &old).unwrap();
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        wait_idle(&mut c);
        assert!(!server.dir.join("appendonly.aof.prev").exists(), "{io}");
        assert!(
            kept_aside(&server.dir)
                .iter()
                .any(|(_, bytes)| *bytes == old),
            "{io}: the old log was removed without a copy"
        );
        assert_eq!(c.cmd(&["GET", "k"]).s(), "1", "{io}");
    }
}

/// What the warning says of an old log that cannot be removed: each
/// checkpoint fails on it, `INFO persistence` says so, and nothing is lost.
#[test]
fn a_checkpoint_fails_while_an_old_log_cannot_be_removed() {
    for &io in io_models() {
        let mut server = Server::start(io, "");
        let mut c = server.conn();
        assert!(c.cmd(&["SET", "k", "1"]).ok());
        assert!(c.cmd(&["SAVE"]).ok());
        assert!(c.cmd(&["SET", "k2", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        an_old_log_that_cannot_be_removed(&server.dir);

        let mut c = server.restart();
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        wait_idle(&mut c);
        assert_eq!(
            info_field(&mut c, "aof_last_bgrewrite_status"),
            "err",
            "{io}"
        );
        assert!(c.cmd(&["SET", "k3", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        for key in ["k", "k2", "k3"] {
            assert_eq!(c.cmd(&["GET", key]).s(), "1", "{io}: {key}");
        }
    }
}

/// The old log of a checkpoint that finished, beside a dump saved with the
/// AOF off, is kept aside before it is removed. If an attempt kept it and
/// did not get to remove it, it is not kept a second time: each start or
/// checkpoint that could not remove it added another copy. A kept file with
/// the same length but other bytes is not taken for it.
#[test]
fn a_log_kept_aside_already_is_not_kept_again() {
    for &io in io_models() {
        for other_copy in [false, true] {
            let mut server = Server::start(io, "");
            server.kill();
            std::fs::remove_file(server.dir.join("appendonly.aof")).unwrap();
            server.aof = false;
            let mut c = server.restart();
            assert!(c.cmd(&["SET", "k", "1"]).ok());
            assert!(c.cmd(&["SAVE"]).ok());
            drop(c);
            server.kill();
            // A log on that dump's number, as a start with the AOF on makes.
            server.aof = true;
            let mut c = server.restart();
            assert!(c.cmd(&["SET", "k2", "1"]).ok());
            durable(&mut c);
            drop(c);
            server.kill();
            let dir = server.dir.clone();
            let (number, marked) = saved_number(&dir);
            assert!(marked, "{io}");

            let mut old = delta_header(number - 1);
            old.extend_from_slice(&set_record("only-in-the-old-log", "1"));
            std::fs::write(dir.join("appendonly.aof.prev"), &old).unwrap();
            // What an earlier attempt kept.
            std::fs::write(dir.join("superseded.1.0.appendonly.aof.prev"), &old).unwrap();
            let mut different = old.clone();
            *different.last_mut().unwrap() ^= 1;
            if other_copy {
                std::fs::write(dir.join("superseded.1.1.appendonly.aof.prev"), &different).unwrap();
            }

            let mut c = server.restart();
            assert_eq!(c.cmd(&["GET", "k2"]).s(), "1", "{io}");
            assert!(!dir.join("appendonly.aof.prev").exists(), "{io}");
            let kept = kept_aside(&dir);
            let copies = kept.iter().filter(|(_, bytes)| *bytes == old).count();
            assert_eq!(copies, 1, "{io}, {other_copy}: {kept:?}");
            assert_eq!(
                kept.len(),
                1 + usize::from(other_copy),
                "{io}, {other_copy}"
            );
            if other_copy {
                assert!(kept.iter().any(|(_, bytes)| *bytes == different), "{io}");
            }
            drop(c);
        }
    }
}

// ── A power failure while the log is turned over (#277) ──────────────────────
//
// `kill -9` keeps the page cache: what a killed server wrote is all there. A
// power failure keeps what was synced and, of the rest, whatever pages the
// kernel happened to write back, in any order. These tests build that: the
// server notes every sync of a log (`note-syncs`: the file, by inode, and
// its length when the sync began), and after it is killed a copy of its
// data directory is cut back to what those syncs made durable — every log,
// or the old log alone while the new one keeps everything written to it,
// which write-back can leave as well.

/// `w:<from>` .. `w:<from + n - 1>`, in that order, in `MSET`s of 100: one
/// sequence of writes across a test, so that what a restart loads can be
/// checked to be a beginning of it (see [`loaded_of`]).
fn fill_seq(c: &mut Conn, from: usize, n: usize) {
    let all: Vec<usize> = (from..from + n).collect();
    for chunk in all.chunks(100) {
        let mut args = vec!["MSET".to_string()];
        for i in chunk {
            args.push(format!("w:{i}"));
            args.push("1".to_string());
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(c.cmd(&args).ok());
    }
}

/// How many of `<prefix>:0` .. `<prefix>:<n - 1>` the server has, asserting
/// that they are the first ones: a restart loads a beginning of the writes,
/// never later writes without earlier ones.
fn loaded_of(c: &mut Conn, prefix: &str, n: usize, what: &str) -> usize {
    let mut there = Vec::with_capacity(n);
    let all: Vec<usize> = (0..n).collect();
    for chunk in all.chunks(500) {
        let mut args = vec!["MGET".to_string()];
        args.extend(chunk.iter().map(|i| format!("{prefix}:{i}")));
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        there.extend(c.cmd(&args).arr().iter().map(|v| *v != V::Nil));
    }
    let loaded = there.iter().take_while(|&&t| t).count();
    if let Some(at) = there[loaded..].iter().position(|&t| t) {
        panic!(
            "{what}: {prefix}:{loaded} is not there and {prefix}:{} is: writes were replayed \
             after a hole",
            loaded + at
        );
    }
    loaded
}

/// What the server noted of its syncs (see `note-syncs`).
#[cfg(unix)]
struct Synced {
    /// The length each file, by inode, was last synced to.
    lengths: std::collections::HashMap<u64, u64>,
    /// The directory was synced after the last turn-over.
    dir_since_turn_over: bool,
}

#[cfg(unix)]
impl Synced {
    fn read(dir: &Path) -> Self {
        let notes = std::fs::read_to_string(dir.join("synced-lengths.test")).unwrap_or_default();
        let mut lengths = std::collections::HashMap::new();
        let mut dir_since_turn_over = true;
        for line in notes.lines() {
            match line.split(' ').collect::<Vec<_>>().as_slice() {
                ["file", inode, len] => {
                    lengths.insert(inode.parse().unwrap(), len.parse().unwrap());
                }
                ["dir"] => dir_since_turn_over = true,
                ["turned-over"] => dir_since_turn_over = false,
                other => panic!("a sync noted as {other:?}"),
            }
        }
        Synced {
            lengths,
            dir_since_turn_over,
        }
    }

    /// How much of the file `name` in `dir` was synced; `None` if it is not
    /// there.
    fn of(&self, dir: &Path, name: &str) -> Option<u64> {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(dir.join(name)).ok()?;
        Some(
            self.lengths
                .get(&meta.ino())
                .copied()
                .unwrap_or(0)
                .min(meta.len()),
        )
    }
}

/// What a power failure leaves of the logs.
#[derive(Clone, Copy, Debug)]
enum Loss {
    /// Every log what was synced of it, no more.
    Unsynced,
    /// The old log what was synced of it; the new log all that was written
    /// to it, written back by the kernel.
    OldLogsEnd,
    /// As [`Loss::OldLogsEnd`], with the old log's length on disk and zeros
    /// where its unsynced bytes were.
    OldLogsEndAsZeros,
    /// The old log what was synced of it; of the new log, the unsynced rest
    /// of the page where its synced part ends lost, and the pages after it
    /// written back.
    NewLogsStart,
    /// The turn-over's renames not on disk (the directory not synced since
    /// they were made): the logs under their names from before, each with
    /// what was synced of it.
    Renames,
    /// Of the turn-over's renames, only the old log's on disk: the new log
    /// under the name it was written under, with all that was written to it
    /// (written back), and the old log what was synced of it.
    NewLogsName,
}

impl Loss {
    const ALL: [Loss; 6] = [
        Loss::Unsynced,
        Loss::OldLogsEnd,
        Loss::OldLogsEndAsZeros,
        Loss::NewLogsStart,
        Loss::Renames,
        Loss::NewLogsName,
    ];
}

const LOG: &str = "appendonly.aof";
const OLD_LOG: &str = "appendonly.aof.prev";
const NEW_LOG: &str = "appendonly.aof.rewrite.tmp";

/// A copy of the data directory `dir` of a killed server, as the power
/// failure `loss` would have left it; `None` for one that cannot happen
/// there, or that this test does not cover (renames lost after the
/// directory was synced; a hole inside a log).
#[cfg(unix)]
fn after_power_failure(dir: &Path, loss: Loss, renamed: bool) -> Option<PathBuf> {
    let synced = Synced::read(dir);
    if matches!(loss, Loss::Renames | Loss::NewLogsName) && (!renamed || synced.dir_since_turn_over)
    {
        return None;
    }
    let image = tempfile::tempdir().unwrap().keep();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let skip = name.starts_with("failpoint.")
            || name == "synced-lengths.test"
            || name == "server.log"
            || name == "kividb.lock";
        if !skip && entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), image.join(&name)).unwrap();
        }
    }
    let durable = |name: &str| synced.of(dir, name);
    let cut = |name: &str| {
        if let Some(len) = durable(name) {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(image.join(name))
                .unwrap();
            file.set_len(len).unwrap();
        }
    };
    let zero = |name: &str, from: u64, to: u64| {
        let path = image.join(name);
        let mut bytes = std::fs::read(&path).unwrap();
        let to = (to as usize).min(bytes.len());
        if (from as usize) < to {
            bytes[from as usize..to].fill(0);
        }
        std::fs::write(&path, bytes).unwrap();
    };
    match loss {
        Loss::Unsynced => {
            for name in [LOG, OLD_LOG, NEW_LOG] {
                cut(name);
            }
        }
        Loss::OldLogsEnd => cut(OLD_LOG),
        Loss::OldLogsEndAsZeros => {
            if let Some(len) = durable(OLD_LOG) {
                zero(OLD_LOG, len, u64::MAX);
            }
        }
        Loss::NewLogsStart => {
            // Only a start never synced: a hole further on, inside a log,
            // is refused at the start as damage, with or without a
            // turn-over.
            let start = 16 + 83;
            match durable(LOG) {
                Some(len) if len < start && image.join(OLD_LOG).exists() => {
                    cut(OLD_LOG);
                    zero(LOG, len, 4096);
                }
                _ => return None,
            }
        }
        Loss::Renames => {
            for name in [LOG, OLD_LOG, NEW_LOG] {
                cut(name);
            }
            if image.join(OLD_LOG).exists() {
                if image.join(LOG).exists() {
                    std::fs::rename(image.join(LOG), image.join(NEW_LOG)).unwrap();
                }
                std::fs::rename(image.join(OLD_LOG), image.join(LOG)).unwrap();
            }
        }
        Loss::NewLogsName => {
            if !image.join(OLD_LOG).exists() || !image.join(LOG).exists() {
                return None;
            }
            cut(OLD_LOG);
            std::fs::rename(image.join(LOG), image.join(NEW_LOG)).unwrap();
        }
    }
    Some(image)
}

/// Checkpoint steps, in order, and whether the log has been turned over at
/// each (its renames made).
const CHECKPOINT_STEPS: [(&str, bool); 7] = [
    ("checkpoint-cutting", false),
    ("turn-over-prepared", false),
    ("turn-over-moved-aside", true),
    ("turn-over-syncing", true),
    ("checkpoint-turned-over", true),
    ("dump-installing", true),
    ("checkpoint-finished", true),
];

/// The gap of #277, as it was made: records the old log had not synced at
/// the turn-over (the second's sync held, as a slow disk holds it), and
/// records in the new log, which the kernel wrote back while the old log's
/// sync waited. A power failure kept the new log's and lost the old log's
/// last ones, and a restart replayed the old log and then the new one: the
/// later writes without the earlier ones, a dataset no client ever saw. Now
/// the old log is found short of where the new log goes on from: it alone
/// is replayed, and the new log is kept aside.
#[cfg(unix)]
#[test]
fn a_power_failure_before_the_old_log_is_synced_never_replays_writes_after_a_hole() {
    for &io in io_models() {
        let server = Server::start(io, "note-syncs,fsync,turn-over-prepared,turn-over-syncing");
        let mut c = server.conn();
        // Synced: the second's sync let go once.
        fill_seq(&mut c, 0, 300);
        server.pass("fsync.1");
        let synced = |server: &Server| {
            let len = std::fs::metadata(server.dir.join(LOG)).unwrap().len();
            Synced::read(&server.dir).of(&server.dir, LOG) == Some(len)
        };
        let deadline = Instant::now() + LONG;
        while !synced(&server) {
            assert!(Instant::now() < deadline, "{io}: the log was not synced");
            std::thread::sleep(Duration::from_millis(5));
        }
        // From here the second's sync is held, and the checkpoint is held
        // after it has prepared its new log: these reach the old log, and
        // are not synced by its turn-over.
        fill_seq(&mut c, 300, 300);
        assert!(server.reached("fsync.2", LONG), "{io}");
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("turn-over-prepared.1", LONG), "{io}");
        fill_seq(&mut c, 600, 300);
        server.release("turn-over-prepared.1");
        // The writer is held: the first write after the checkpoint's cut
        // takes the turn-over, and is the new log's first. The old log's
        // sync is then held.
        let mut next = 900;
        let deadline = Instant::now() + LONG;
        while !server.dir.join("failpoint.turn-over-syncing.1").exists() {
            assert!(Instant::now() < deadline, "{io}: no turn-over");
            fill_seq(&mut c, next, 10);
            next += 10;
            std::thread::sleep(Duration::from_millis(2));
        }
        fill_seq(&mut c, next, 300);
        let total = next + 300;
        drop(c);
        let mut server = server;
        server.kill();

        for loss in [Loss::OldLogsEnd, Loss::OldLogsEndAsZeros] {
            let what = format!("{io}, {loss:?}");
            let synced = Synced::read(&server.dir);
            let old_len = std::fs::metadata(server.dir.join(OLD_LOG)).unwrap().len();
            assert!(
                synced.of(&server.dir, OLD_LOG).unwrap() < old_len,
                "{what}: the old log was synced to its end, so this test shows nothing"
            );
            let image = after_power_failure(&server.dir, loss, true).unwrap();
            let restarted = Server::start_in(image, io, "");
            let mut c = restarted.conn();
            let loaded = loaded_of(&mut c, "w", total, &what);
            assert!(
                loaded >= 300,
                "{what}: synced writes lost ({loaded} loaded)"
            );
            assert!(loaded < total, "{what}: {loaded} loaded");
            let log = std::fs::read_to_string(restarted.dir.join("server.log")).unwrap();
            assert!(
                log.contains("does not reach where appendonly.aof goes on from"),
                "{what}: no word of the old log cut short"
            );
            let kept = kept_aside(&restarted.dir);
            assert_eq!(kept.len(), 1, "{what}: {kept:?}");
            assert!(kept[0].0.ends_with(".appendonly.aof"), "{what}: {kept:?}");
            assert!(
                holds(&kept[0].1, format!("w:{}", total - 1).as_bytes()),
                "{what}: the new log is not kept"
            );

            // Started again on the old log: what is written now is kept,
            // after the same writes as before, restart after restart.
            assert!(c.cmd(&["SET", "after", "1"]).ok());
            durable(&mut c);
            drop(c);
            let mut restarted = restarted;
            restarted.kill();
            let mut c = restarted.restart();
            assert_eq!(loaded_of(&mut c, "w", total, &what), loaded, "{what}");
            assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{what}");
            assert_eq!(kept_aside(&restarted.dir).len(), 1, "{what}");
            assert!(c.cmd(&["SAVE"]).ok());
            assert!(c.cmd(&["SET", "later", "1"]).ok());
            durable(&mut c);
            drop(c);
            restarted.kill();
            let mut c = restarted.restart();
            assert_eq!(loaded_of(&mut c, "w", total, &what), loaded, "{what}");
            assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{what}");
            assert_eq!(c.cmd(&["GET", "later"]).s(), "1", "{what}");
        }
    }
}

/// A client writing `w:0`, `w:1`, ... with `SET`s, 20 at a time without
/// waiting for replies, until it is stopped or the server goes away,
/// counting what was acknowledged.
struct Writer {
    stop: Arc<AtomicBool>,
    acked: Arc<std::sync::atomic::AtomicUsize>,
    sent: Arc<std::sync::atomic::AtomicUsize>,
    thread: std::thread::JoinHandle<()>,
}

impl Writer {
    fn start(port: u16) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let thread = {
            let (stop, acked, sent) = (stop.clone(), acked.clone(), sent.clone());
            std::thread::spawn(move || {
                let mut c = Conn::connect(port);
                let mut next = 0;
                while !stop.load(Ordering::Relaxed) {
                    let mut batch = Vec::new();
                    for i in next..next + 20 {
                        let key = format!("w:{i}");
                        batch.extend_from_slice(
                            format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\n1\r\n", key.len())
                                .as_bytes(),
                        );
                    }
                    sent.store(next + 20, Ordering::SeqCst);
                    if c.writer.write_all(&batch).is_err() {
                        return;
                    }
                    for _ in 0..20 {
                        let mut line = String::new();
                        match c.reader.read_line(&mut line) {
                            Ok(n) if n > 0 && line.starts_with("+OK") => {
                                acked.fetch_add(1, Ordering::SeqCst);
                            }
                            _ => return,
                        }
                    }
                    next += 20;
                }
            })
        };
        Writer {
            stop,
            acked,
            sent,
            thread,
        }
    }

    fn acked(&self) -> usize {
        self.acked.load(Ordering::SeqCst)
    }

    /// Stop, once the server is gone: how many writes were acknowledged,
    /// and how many sent.
    fn stop(self) -> (usize, usize) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.thread.join();
        (
            self.acked.load(Ordering::SeqCst),
            self.sent.load(Ordering::SeqCst),
        )
    }
}

/// `appendfsync always` with `aof-ack-after-write yes`: a write is
/// acknowledged once it is synced. A power failure at any step of a
/// checkpoint, with whatever was not synced lost or written back in any
/// order, loses none of the writes acknowledged, and a restart loads a
/// beginning of the writes sent, with no hole. A client writes all along.
#[cfg(unix)]
#[test]
fn under_appendfsync_always_a_power_failure_at_any_step_of_a_checkpoint_keeps_every_acknowledged_write()
 {
    for &io in io_models() {
        for (step, renamed) in CHECKPOINT_STEPS {
            let mut server = Server::start(io, &format!("note-syncs,{step}"));
            let mut c = server.conn();
            assert!(c.cmd(&["CONFIG", "SET", "appendfsync", "always"]).ok());
            let ack = c.cmd(&["CONFIG", "GET", "aof-ack-after-write"]);
            assert_eq!(ack.arr()[1].s(), "yes", "{io}");
            fill(&mut c, "base", 400);
            let writer = Writer::start(server.port);
            let deadline = Instant::now() + LONG;
            while writer.acked() < 200 {
                assert!(Instant::now() < deadline, "{io}: no writes acknowledged");
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
            assert!(
                server.reached(&format!("{step}.1"), LONG),
                "{io}: {step} not reached"
            );
            // Writes go on, where the step lets them.
            std::thread::sleep(Duration::from_millis(300));
            drop(c);
            server.kill();
            let (acked, sent) = writer.stop();

            for loss in Loss::ALL {
                let Some(image) = after_power_failure(&server.dir, loss, renamed) else {
                    continue;
                };
                let what = format!("{io}, a power failure at {step}, {loss:?}");
                let restarted = Server::start_in(image, io, "");
                let mut c = restarted.conn();
                assert_eq!(loaded_of(&mut c, "base", 400, &what), 400, "{what}");
                let loaded = loaded_of(&mut c, "w", sent, &what);
                assert!(
                    loaded >= acked,
                    "{what}: {acked} writes acknowledged, {loaded} loaded"
                );
            }
        }
    }
}

/// A checkpoint over one that did not finish adds the log's records to the
/// old log the unfinished one left (`appendonly.aof.prev`), syncs it, and
/// starts a new log after it. A power failure after that, with the new
/// log's records written back and the logs' unsynced bytes lost, loads a
/// beginning of the writes: the old log is synced to the end the new log
/// records before the new log takes its name.
#[cfg(unix)]
#[test]
fn a_power_failure_after_a_turn_over_onto_an_unfinished_ones_old_log_loads_a_beginning() {
    for &io in io_models() {
        let mut server = Server::start(io, "note-syncs,dump-installing");
        let mut c = server.conn();
        fill_seq(&mut c, 0, 300);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("dump-installing.1", LONG), "{io}");
        fill_seq(&mut c, 300, 300);
        durable(&mut c);
        drop(c);
        server.kill();
        // Held by the process killed: the next one's is numbered 1 again.
        server.release("dump-installing.1");

        // The unfinished checkpoint's old log is still needed: the next
        // turn-over adds the log's records to it.
        let mut c = server.restart();
        assert_eq!(loaded_of(&mut c, "w", 900, io), 600, "{io}");
        assert!(server.dir.join(OLD_LOG).exists(), "{io}");
        fill_seq(&mut c, 600, 300);
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("dump-installing.1", LONG), "{io}");
        fill_seq(&mut c, 900, 300);
        drop(c);
        server.kill();

        for loss in Loss::ALL {
            let Some(image) = after_power_failure(&server.dir, loss, true) else {
                continue;
            };
            let what = format!("{io}, {loss:?}");
            let restarted = Server::start_in(image, io, "");
            let mut c = restarted.conn();
            let loaded = loaded_of(&mut c, "w", 1200, &what);
            // What was synced before the restart, and the old log the
            // second turn-over synced.
            assert!(loaded >= 900, "{what}: {loaded} loaded");
        }
    }
}

/// The header of a delta log and the record of where its old log ends, as
/// the server writes them.
fn old_log_end(old: &[u8]) -> Vec<u8> {
    let from = old.len().saturating_sub(4096);
    format!(
        "*3\r\n$22\r\nKIVIDB.AOF-OLD-LOG-END\r\n$20\r\n{:020}\r\n$16\r\n{:016x}\r\n",
        old.len(),
        xxhash_rust::xxh3::xxh3_64(&old[from..])
    )
    .into_bytes()
}

/// Torn tails on either side of a turn-over, in a checkpoint that did not
/// finish (an old log and a new one, no snapshot): a restart loads a
/// beginning of the writes, keeps aside what it does not load, and goes on
/// from what it loaded.
#[test]
fn torn_tails_either_side_of_a_turn_over_load_a_beginning_of_the_writes() {
    let n = 200;
    let mut old = delta_header(1);
    for i in 0..n {
        old.extend_from_slice(&set_record(&format!("old:{i}"), "1"));
    }
    let new_records = |log: &mut Vec<u8>| {
        for i in 0..n {
            log.extend_from_slice(&set_record(&format!("new:{i}"), "1"));
        }
    };
    let mut new = delta_header(2);
    new.extend_from_slice(&old_log_end(&old));
    new_records(&mut new);
    let cut = |bytes: &[u8], by: usize| bytes[..bytes.len() - by].to_vec();
    let zeros_at_end = |bytes: &[u8], by: usize| {
        let mut bytes = bytes.to_vec();
        let len = bytes.len();
        bytes[len - by..].fill(0);
        bytes
    };
    // The old log as it was when it ended in half a record (a write cut
    // short): the new log records that length.
    let mut half = old.clone();
    half.extend_from_slice(&set_record("old:half", "1")[..10]);
    let mut after_half = delta_header(2);
    after_half.extend_from_slice(&old_log_end(&half));
    new_records(&mut after_half);
    let mut head_lost = new.clone();
    head_lost[16..16 + 83 + 30].fill(0);
    // What 1.2.2 writes: no record.
    let mut unrecorded = delta_header(2);
    new_records(&mut unrecorded);

    /// What, the old log, the new log, the old writes loaded, the new
    /// writes loaded, the new log kept aside.
    type Case = (&'static str, Vec<u8>, Vec<u8>, usize, usize, bool);
    let cases: Vec<Case> = vec![
        ("both whole", old.clone(), new.clone(), n, n, false),
        ("old log torn", cut(&old, 5), new.clone(), n - 1, 0, true),
        (
            "old log zeros",
            zeros_at_end(&old, 5),
            new.clone(),
            n - 1,
            0,
            true,
        ),
        ("new log torn", old.clone(), cut(&new, 5), n, n - 1, false),
        ("both torn", cut(&old, 5), cut(&new, 5), n - 1, 0, true),
        (
            "old log torn inside its end",
            half.clone(),
            after_half,
            n,
            n,
            false,
        ),
        (
            "the record lost, records after it",
            old.clone(),
            head_lost,
            n,
            0,
            true,
        ),
        (
            "the record torn, nothing after it",
            old.clone(),
            new[..16 + 40].to_vec(),
            n,
            0,
            false,
        ),
        ("no record", old.clone(), unrecorded, n, n, false),
    ];
    for (what, old_log, new_log, old_loaded, new_loaded, kept) in cases {
        let dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(dir.join(OLD_LOG), &old_log).unwrap();
        std::fs::write(dir.join(LOG), &new_log).unwrap();
        let mut server = Server::start_in(dir, "tokio", "");
        let mut c = server.conn();
        let check = |c: &mut Conn, server: &Server| {
            assert_eq!(loaded_of(c, "old", n, what), old_loaded, "{what}");
            assert_eq!(loaded_of(c, "new", n, what), new_loaded, "{what}");
            assert!(!(new_loaded > 0 && old_loaded < n), "{what}");
            let kept_logs = kept_aside(&server.dir);
            assert_eq!(kept_logs.len(), usize::from(kept), "{what}: {kept_logs:?}");
        };
        check(&mut c, &server);
        // Again, and with a write after: the same, and the write kept.
        assert!(c.cmd(&["SET", "after", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        check(&mut c, &server);
        assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{what}");
        assert!(c.cmd(&["SAVE"]).ok());
        assert!(!server.dir.join(OLD_LOG).exists(), "{what}");
        drop(c);
        server.kill();
        let mut c = server.restart();
        check(&mut c, &server);
        assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{what}");
    }
}

/// The 1.2 release (`KIVIDB_1_2_BIN`), which writes no record of where the
/// old log ends: a checkpoint of its that did not finish, and one that did,
/// load here whole, as they did there. And the other way, what is written
/// here (the new log opening with the record) loads in 1.2 whole: it
/// replays the record as an unknown command, and passes over it.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn logs_of_1_2_load_here_and_1_2_loads_logs_written_here() {
    let release = PathBuf::from(std::env::var("KIVIDB_1_2_BIN").expect("KIVIDB_1_2_BIN"));
    let out = Command::new(&release).arg("--version").output().unwrap();
    let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(version.starts_with("kividb 1.2."), "{version:?}");
    let this = PathBuf::from(env!("CARGO_BIN_EXE_kividb"));
    for (writes, reads) in [(&release, &this), (&this, &release)] {
        for finished in [false, true] {
            let what = format!(
                "written by {}, read by {}, checkpoint finished: {finished}",
                writes.display(),
                reads.display()
            );
            let dir = tempfile::tempdir().unwrap().keep();
            let failpoints = if finished { "" } else { "turn-over-syncing" };
            let mut server = Server::start_bin(writes, dir, "tokio", failpoints);
            let mut c = server.conn();
            fill_seq(&mut c, 0, 300);
            // A checkpoint that finishes: let past the step it holds at.
            send(&mut c, &["SAVE"]);
            if !finished {
                server.pass("turn-over-syncing.1");
            }
            assert!(c.read().ok(), "{what}");
            fill_seq(&mut c, 300, 300);
            assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED, "{what}");
            if finished {
                wait_idle(&mut c);
            } else {
                assert!(server.reached("turn-over-syncing.2", LONG), "{what}");
            }
            fill_seq(&mut c, 600, 300);
            durable(&mut c);
            drop(c);
            server.kill();
            assert_eq!(server.dir.join(OLD_LOG).exists(), !finished, "{what}");
            let _ = std::fs::remove_file(server.dir.join("failpoint.turn-over-syncing.2"));

            server.bin = reads.clone();
            server.failpoints.clear();
            let mut c = server.restart();
            assert_eq!(loaded_of(&mut c, "w", 900, &what), 900, "{what}");
            assert!(kept_aside(&server.dir).is_empty(), "{what}");
            // And it goes on from there.
            fill_seq(&mut c, 900, 100);
            durable(&mut c);
            assert!(c.cmd(&["SAVE"]).ok(), "{what}");
            fill_seq(&mut c, 1000, 100);
            durable(&mut c);
            drop(c);
            server.kill();
            let mut c = server.restart();
            assert_eq!(loaded_of(&mut c, "w", 1100, &what), 1100, "{what}");
        }
    }
}

// ── An old log cut short, and a turn-over after it ───────────────────────────

/// A server in `dir`, held at its first `failpoint` during its start, and
/// killed there.
fn killed_at_start(dir: &Path, failpoint: &str) {
    let mut child = launch(dir, common::free_port(), "tokio", failpoint, true);
    let held = dir.join(format!("failpoint.{failpoint}.1"));
    let deadline = Instant::now() + LONG;
    while !held.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "the server ended before {failpoint}"
        );
        assert!(Instant::now() < deadline, "{failpoint} not reached");
        std::thread::sleep(Duration::from_millis(5));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    std::fs::remove_file(held).unwrap();
}

/// An old log of `n` records `old:<i>` with `tail` after them.
fn old_log_with(n: usize, tail: &[u8]) -> Vec<u8> {
    let mut old = delta_header(1);
    for i in 0..n {
        old.extend_from_slice(&set_record(&format!("old:{i}"), "1"));
    }
    old.extend_from_slice(tail);
    old
}

/// An old log that ends in a record cut short, beside a new log, of a
/// checkpoint that did not finish: the replay leaves the record out, and
/// it is cut off. Left there, the next checkpoint's turn-over added the
/// log's records after it (the old log still being needed), and a restart
/// before that checkpoint finished could not read past it: the server
/// refused to start, or, when the record cut short declared a value longer
/// than the rest of the file, read the records after it as that value and
/// lost them. Then the same with a second stop in the middle of cutting it.
#[test]
fn an_old_log_cut_short_is_cut_before_records_are_added_to_it() {
    let n = 100;
    // Cut inside a value: the records added after it are misread from there
    // (a cut just after a length can happen to line up with them again).
    let torn = set_record("old:torn", "1")[..21].to_vec();
    let huge = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$999999999\r\nab".to_vec();
    // (what, old log, new log, a stop in the middle of the cut)
    type Case = (&'static str, Vec<u8>, Vec<u8>, Option<&'static str>);
    let mut cases: Vec<Case> = Vec::new();
    for (name, tail) in [("cut short", &torn), ("a huge value cut short", &huge)] {
        // As 1.2.2 leaves it, and as a power failure leaves this
        // version's new log with its first page lost: no record.
        cases.push((name, old_log_with(n, tail), delta_header(2), None));
        // The old log ended in the record cut short when the log was turned
        // over (a write cut short): the new log's record says so.
        let old = old_log_with(n, tail);
        let mut new = delta_header(2);
        new.extend_from_slice(&old_log_end(&old));
        cases.push((name, old.clone(), new.clone(), None));
        cases.push((name, old, new, Some("old-log-end-rewritten")));
    }
    for (what, old, new, stop) in cases {
        let what = format!(
            "{what}, {} record, stopped: {stop:?}",
            if new.len() > 16 { "a" } else { "no" }
        );
        let dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(dir.join(OLD_LOG), &old).unwrap();
        std::fs::write(dir.join(LOG), &new).unwrap();
        if let Some(stop) = stop {
            killed_at_start(&dir, stop);
        }
        let mut server = Server::start_in(dir, "tokio", "dump-installing");
        let mut c = server.conn();
        assert_eq!(loaded_of(&mut c, "old", n, &what), n, "{what}");
        let prev = std::fs::read(server.dir.join(OLD_LOG)).unwrap();
        assert_eq!(prev, old_log_with(n, b""), "{what}: not cut");
        fill_seq(&mut c, 0, 300);
        durable(&mut c);
        // The next checkpoint adds those to the old log, and is stopped
        // before its snapshot is in place.
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("dump-installing.1", LONG), "{what}");
        fill_seq(&mut c, 300, 100);
        durable(&mut c);
        drop(c);
        server.kill();
        server.release("dump-installing.1");
        server.failpoints.clear();
        let mut c = server.restart();
        assert_eq!(loaded_of(&mut c, "old", n, &what), n, "{what}");
        assert_eq!(loaded_of(&mut c, "w", 400, &what), 400, "{what}");
        assert!(kept_aside(&server.dir).is_empty(), "{what}");
    }
}

/// A second stop in the middle of what a start does after an old log found
/// short of where the new log goes on from: the next start loads the same,
/// keeps the new log aside once, and goes on from there.
#[test]
fn a_stop_while_a_log_after_a_short_old_log_is_started_again_loads_the_same() {
    let n = 100;
    let old = old_log_with(n, b"");
    let mut new = delta_header(2);
    new.extend_from_slice(&old_log_end(&old));
    for i in 0..n {
        new.extend_from_slice(&set_record(&format!("new:{i}"), "1"));
    }
    let torn = &old[..old.len() - 5];
    for stop in ["short-old-log-kept", "short-old-log-cut"] {
        let dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(dir.join(OLD_LOG), torn).unwrap();
        std::fs::write(dir.join(LOG), &new).unwrap();
        killed_at_start(&dir, stop);
        let mut server = Server::start_in(dir, "tokio", "");
        let mut c = server.conn();
        assert_eq!(loaded_of(&mut c, "old", n, stop), n - 1, "{stop}");
        assert_eq!(loaded_of(&mut c, "new", n, stop), 0, "{stop}");
        let kept = kept_aside(&server.dir);
        assert_eq!(kept.len(), 1, "{stop}: {kept:?}");
        assert_eq!(kept[0].1, new, "{stop}");
        assert!(c.cmd(&["SET", "after", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(loaded_of(&mut c, "old", n, stop), n - 1, "{stop}");
        assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{stop}");
        assert_eq!(kept_aside(&server.dir).len(), 1, "{stop}");
    }
}

/// The turn-over's new log under the name it was written under, holding
/// records, beside the old log and no log: the old log's rename reached the
/// disk and the new log's did not. Put in place and replayed when the old
/// log is there to where the new one records it ended; kept aside, not
/// replayed, when it is not.
#[test]
fn a_new_log_whose_name_was_lost_is_replayed_when_the_old_log_is_whole() {
    let n = 100;
    let old = old_log_with(n, b"");
    let mut new = delta_header(2);
    new.extend_from_slice(&old_log_end(&old));
    for i in 0..n {
        new.extend_from_slice(&set_record(&format!("new:{i}"), "1"));
    }
    for whole in [true, false] {
        let what = format!("old log whole: {whole}");
        let dir = tempfile::tempdir().unwrap().keep();
        let on_disk = if whole {
            &old[..]
        } else {
            &old[..old.len() - 5]
        };
        std::fs::write(dir.join(OLD_LOG), on_disk).unwrap();
        std::fs::write(dir.join(NEW_LOG), &new).unwrap();
        let mut server = Server::start_in(dir, "tokio", "");
        let mut c = server.conn();
        let (old_loaded, new_loaded) = if whole { (n, n) } else { (n - 1, 0) };
        assert_eq!(loaded_of(&mut c, "old", n, &what), old_loaded, "{what}");
        assert_eq!(loaded_of(&mut c, "new", n, &what), new_loaded, "{what}");
        assert_eq!(kept_aside(&server.dir).len(), usize::from(!whole), "{what}");
        assert!(!server.dir.join(NEW_LOG).exists(), "{what}");
        assert!(c.cmd(&["SET", "after", "1"]).ok());
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(loaded_of(&mut c, "new", n, &what), new_loaded, "{what}");
        assert_eq!(c.cmd(&["GET", "after"]).s(), "1", "{what}");
    }
}

// ── A write the disk takes only part of ──────────────────────────────────────

/// Whether `c` has a reply waiting, without reading it.
fn has_reply(c: &mut Conn) -> bool {
    let socket = c.reader.get_ref();
    socket.set_nonblocking(true).unwrap();
    let peeked = socket.peek(&mut [0]);
    socket.set_nonblocking(false).unwrap();
    !c.reader.buffer().is_empty() || matches!(peeked, Ok(n) if n > 0)
}

/// A write to the log that the disk takes only part of (a full disk): what
/// it wrote is taken back, and nothing is written after it until all of it
/// is. Its reply waits, `WAITAOF` counts it not, and `INFO` says the log is
/// failing; once the disk takes it, all goes on in order. Killed while it
/// fails, the log loads, with every write acknowledged before.
#[test]
fn a_write_to_the_log_cut_short_is_taken_back_and_written_again() {
    for &io in io_models() {
        for end in ["the disk takes it again", "killed"] {
            let what = format!("{io}, {end}");
            let mut server = Server::start(io, "short-writes");
            let mut c = server.conn();
            fill_seq(&mut c, 0, 300);
            durable(&mut c);
            std::fs::write(server.dir.join("short-write.test"), b"").unwrap();
            send(&mut c, &["MSET", "w:300", "1", "w:301", "1"]);
            std::thread::sleep(Duration::from_millis(1000));
            assert!(!has_reply(&mut c), "{what}: acknowledged, not written");
            // From then on, writes are refused, as Redis refuses them.
            let mut w = server.conn();
            let refused = w.cmd(&["SET", "w:302", "1"]);
            assert_eq!(
                refused,
                V::Err("MISCONF Errors writing to the AOF file: No space left on device".into()),
                "{what}"
            );
            let mut i = server.conn();
            let waited = i.cmd(&["WAITAOF", "1", "0", "200"]);
            assert_eq!(waited.arr()[0].i(), 0, "{what}: WAITAOF counted it");
            assert_eq!(info_field(&mut i, "aof_last_write_status"), "err", "{what}");
            if end == "killed" {
                drop((c, w, i));
                server.kill();
                std::fs::remove_file(server.dir.join("short-write.test")).unwrap();
                let mut c = server.restart();
                assert!(loaded_of(&mut c, "w", 303, &what) >= 300, "{what}");
                continue;
            }
            std::fs::remove_file(server.dir.join("short-write.test")).unwrap();
            assert!(c.read().ok(), "{what}");
            // Accepted again once the disk takes what is held.
            assert!(w.cmd(&["SET", "w:302", "1"]).ok(), "{what}");
            assert_eq!(info_field(&mut i, "aof_last_write_status"), "ok", "{what}");
            assert_eq!(info_field(&mut i, "aof_acks_before_write"), "0", "{what}");
            fill_seq(&mut c, 303, 100);
            durable(&mut c);
            drop((c, w, i));
            server.kill();
            let mut c = server.restart();
            assert_eq!(loaded_of(&mut c, "w", 403, &what), 403, "{what}");
        }
    }
}

/// The record of where the old log ends, written only in part at a
/// turn-over: taken off again, and the log goes on without it. If it cannot
/// be taken off either, the server stops rather than write records after
/// half of one.
#[test]
fn a_record_of_the_old_logs_end_cut_short_is_taken_off_or_stops_the_server() {
    for taken_off in [true, false] {
        let what = format!("taken off: {taken_off}");
        let mut server = Server::start("tokio", "short-writes");
        let mut c = server.conn();
        fill_seq(&mut c, 0, 300);
        durable(&mut c);
        std::fs::write(server.dir.join("short-write.record.test"), b"").unwrap();
        if !taken_off {
            std::fs::write(server.dir.join("short-write.cut.test"), b"").unwrap();
            send(&mut c, &["BGREWRITEAOF"]);
            // The turn-over is made by the next write, or the writer.
            let mut w = server.conn();
            send(&mut w, &["SET", "w:300", "1"]);
            let deadline = Instant::now() + LONG;
            while server.child.try_wait().unwrap().is_none() {
                assert!(Instant::now() < deadline, "{what}: the server did not stop");
                std::thread::sleep(Duration::from_millis(10));
            }
            let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
            assert!(
                log.contains("exiting rather than write records after half of one"),
                "{what}"
            );
            for f in ["short-write.record.test", "short-write.cut.test"] {
                std::fs::remove_file(server.dir.join(f)).unwrap();
            }
            let mut c = server.restart();
            assert!(loaded_of(&mut c, "w", 301, &what) >= 300, "{what}");
            continue;
        }
        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        wait_idle(&mut c);
        std::fs::remove_file(server.dir.join("short-write.record.test")).unwrap();
        let log = std::fs::read(server.dir.join(LOG)).unwrap();
        assert!(
            !holds(&log, b"KIVIDB.AOF-OLD-LOG-END"),
            "{what}: half a record is left"
        );
        fill_seq(&mut c, 300, 100);
        durable(&mut c);
        drop(c);
        server.kill();
        let mut c = server.restart();
        assert_eq!(loaded_of(&mut c, "w", 400, &what), 400, "{what}");
    }
}

// ── While the log cannot be written ──────────────────────────────────────────

const MISCONF: &str = "MISCONF Errors writing to the AOF file: No space left on device";

/// A server whose next write to the log fails (`short-writes`), with `INCR
/// k` sent and held: written to memory, and not to the log.
fn with_a_write_held(failpoints: &str) -> (Server, Conn) {
    let server = Server::start("tokio", failpoints);
    let mut c = server.conn();
    fill_seq(&mut c, 0, 100);
    durable(&mut c);
    std::fs::write(server.dir.join("short-write.test"), b"").unwrap();
    let mut held = server.conn();
    send(&mut held, &["INCR", "k"]);
    let deadline = Instant::now() + LONG;
    while info_field(&mut c, "aof_last_write_status") != "err" {
        assert!(Instant::now() < deadline, "the write was not held");
        std::thread::sleep(Duration::from_millis(5));
    }
    (server, held)
}

/// What would be logged is refused while the log cannot be written, as
/// Redis refuses it; reads are served; nothing more is held; and writes are
/// accepted again once the disk takes what is held.
#[test]
fn writes_are_refused_while_the_log_cannot_be_written() {
    let (server, mut held) = with_a_write_held("short-writes");
    let mut c = server.conn();
    let big = "x".repeat(10_000);
    for i in 0..1000 {
        assert_eq!(
            c.cmd(&["SET", &format!("refused:{i}"), &big]),
            V::Err(MISCONF.into())
        );
    }
    assert_eq!(c.cmd(&["GET", "w:0"]).s(), "1");
    assert_eq!(c.cmd(&["PING"]).s(), "PONG");
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 101);
    // A transaction that writes is refused whole, at its EXEC.
    assert!(c.cmd(&["MULTI"]).ok());
    assert_eq!(c.cmd(&["SET", "in-tx", "1"]).s(), "QUEUED");
    assert_eq!(
        c.cmd(&["EXEC"]),
        V::Err(format!(
            "EXECABORT Transaction discarded because of: {MISCONF}"
        ))
    );
    assert_eq!(c.cmd(&["EXISTS", "in-tx"]).i(), 0);
    // Nothing of the refused writes is held: what is held is the INCR.
    let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
    assert!(!log.contains("past the limit"), "held more");
    std::fs::remove_file(server.dir.join("short-write.test")).unwrap();
    assert_eq!(held.read().i(), 1);
    assert!(c.cmd(&["SET", "after", "1"]).ok());
    durable(&mut c);
    drop((c, held));
    let mut server = server;
    server.kill();
    let mut c = server.restart();
    assert_eq!(c.cmd(&["GET", "k"]).s(), "1");
    assert_eq!(c.cmd(&["GET", "after"]).s(), "1");
    assert_eq!(c.cmd(&["EXISTS", "refused:0"]).i(), 0);
}

/// A save with the AOF off records the logs beside its dump. Made while a
/// write was held, the log recorded did not hold it; written later, it made
/// the log read as added to since the dump, and replayed over it: an `INCR`
/// applied twice. The save now fails until what is held is written. The
/// same for the AOF turned on again, whose checkpoint cannot turn the log
/// over past what is held.
#[test]
fn a_save_or_an_enable_waits_for_what_the_log_could_not_take() {
    for then in ["BGSAVE", "appendonly yes"] {
        let (mut server, mut held) = with_a_write_held("short-writes");
        let mut c = server.conn();
        assert!(c.cmd(&["CONFIG", "SET", "appendonly", "no"]).ok(), "{then}");
        if then == "BGSAVE" {
            let started = c.cmd(&["BGSAVE"]);
            if started.s().contains("started") {
                wait_idle(&mut c);
                assert_eq!(
                    info_field(&mut c, "rdb_last_bgsave_status"),
                    "err",
                    "{then}"
                );
            }
            assert!(!server.dir.join("dump.kdb").exists(), "{then}: saved");
        } else {
            let enabled = c.cmd(&["CONFIG", "SET", "appendonly", "yes"]);
            assert!(matches!(enabled, V::Err(_)), "{then}: {enabled:?}");
        }
        std::fs::remove_file(server.dir.join("short-write.test")).unwrap();
        assert_eq!(held.read().i(), 1, "{then}");
        // The second's sync writes it.
        let deadline = Instant::now() + LONG;
        while info_field(&mut c, "aof_last_write_status") != "ok" {
            assert!(Instant::now() < deadline, "{then}");
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(1500));
        drop((c, held));
        server.kill();
        let mut c = server.restart();
        assert_eq!(c.cmd(&["GET", "k"]).s(), "1", "{then}: applied twice");
    }
}

/// A shutdown with writes held in memory says so. `SHUTDOWN` keeps the
/// server running, its final save having failed; `SHUTDOWN NOSAVE` exits
/// with status 1, saying how many bytes were lost.
#[test]
fn a_shutdown_with_writes_held_says_so() {
    let (mut server, held) = with_a_write_held("short-writes");
    let mut c = server.conn();
    let refused = c.cmd(&["SHUTDOWN"]);
    assert!(matches!(refused, V::Err(_)), "{refused:?}");
    assert_eq!(c.cmd(&["PING"]).s(), "PONG");
    let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
    assert!(log.contains("in memory only"), "the final save's error");
    send(&mut c, &["SHUTDOWN", "NOSAVE"]);
    let status = server.child.wait().unwrap();
    assert_eq!(status.code(), Some(1));
    let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
    assert!(
        log.contains("are lost with the process"),
        "no word of the loss"
    );
    drop((c, held));
}

/// Under `appendfsync always`, a reply's sync counted for `WAITAOF` all
/// that was written, records the disk had not taken included, after a
/// switch to `everysec` left barriers for it. `WAITAOF` counts nothing while
/// records are held, and answers once the second's sync has written them.
#[test]
fn waitaof_counts_no_record_the_disk_has_not_taken() {
    let server = Server::start("tokio", "short-writes,aof-fsync-before-reply");
    let mut c = server.conn();
    assert!(c.cmd(&["CONFIG", "SET", "appendfsync", "always"]).ok());
    // The reply's sync for x is held; z's barrier waits behind it.
    send(&mut c, &["SET", "x", "1"]);
    assert!(server.reached("aof-fsync-before-reply.1", LONG));
    let mut z = server.conn();
    send(&mut z, &["SET", "z", "1"]);
    std::thread::sleep(Duration::from_millis(200));
    let mut a = server.conn();
    assert!(a.cmd(&["CONFIG", "SET", "appendfsync", "everysec"]).ok());
    std::fs::write(server.dir.join("short-write.test"), b"").unwrap();
    let mut y = server.conn();
    send(&mut y, &["SET", "y", "1"]);
    let deadline = Instant::now() + LONG;
    while info_field(&mut a, "aof_last_write_status") != "err" {
        assert!(Instant::now() < deadline, "y was not held");
        std::thread::sleep(Duration::from_millis(5));
    }
    // A WAITAOF from now waits for y.
    let mut w = server.conn();
    send(&mut w, &["WAITAOF", "1", "0", "20000"]);
    server.release("aof-fsync-before-reply.1");
    // The sync for z's barrier, held in turn.
    assert!(server.reached("aof-fsync-before-reply.2", LONG));
    server.release("aof-fsync-before-reply.2");
    assert!(c.read().ok());
    assert!(z.read().ok());
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        !has_reply(&mut w),
        "WAITAOF counted y, which is not in the file"
    );
    // Once the disk takes it, the second's sync writes it, and WAITAOF
    // answers.
    std::fs::remove_file(server.dir.join("short-write.test")).unwrap();
    let started = Instant::now();
    assert_eq!(w.read().arr()[0].i(), 1);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(y.read().ok());
}

/// A replica whose log cannot be written keeps applying its master's
/// stream, and holds its records. A full sync replaces its dataset: what is
/// held is of the dataset replaced, and is dropped; the rebase onto the new
/// dataset turns the log over, and a restart loads the new dataset, not the
/// old one's records over it.
#[test]
fn a_full_sync_drops_what_the_replicas_log_could_not_take() {
    let first = Server::start("tokio", "");
    let mut m = first.conn();
    for i in 0..100 {
        assert!(m.cmd(&["SET", &format!("a:{i}"), "1"]).ok());
    }
    let mut replica = Server::start("tokio", "short-writes");
    let mut r = replica.conn();
    let port = first.port.to_string();
    assert!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]).ok());
    let until = |r: &mut Conn, keys: i64| {
        let deadline = Instant::now() + LONG;
        while r.cmd(&["DBSIZE"]).i() != keys {
            assert!(
                Instant::now() < deadline,
                "the replica did not reach {keys} keys"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    until(&mut r, 100);
    std::fs::write(replica.dir.join("short-write.test"), b"").unwrap();
    for i in 0..50 {
        assert!(m.cmd(&["SET", &format!("b:{i}"), "1"]).ok());
    }
    until(&mut r, 150);
    let deadline = Instant::now() + LONG;
    while info_field(&mut r, "aof_last_write_status") != "err" {
        assert!(Instant::now() < deadline, "the replica's log did not fail");
        std::thread::sleep(Duration::from_millis(10));
    }
    // Another master, another dataset: a full sync.
    let second = Server::start("tokio", "");
    let mut m2 = second.conn();
    for i in 0..200 {
        assert!(m2.cmd(&["SET", &format!("c:{i}"), "1"]).ok());
    }
    drop(first);
    let port = second.port.to_string();
    assert!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]).ok());
    let deadline = Instant::now() + LONG;
    while r.cmd(&["EXISTS", "c:199"]).i() != 1 || r.cmd(&["DBSIZE"]).i() != 200 {
        assert!(Instant::now() < deadline, "no full sync");
        std::thread::sleep(Duration::from_millis(20));
    }
    std::fs::remove_file(replica.dir.join("short-write.test")).unwrap();
    for i in 0..10 {
        assert!(m2.cmd(&["SET", &format!("d:{i}"), "1"]).ok());
    }
    until(&mut r, 210);
    let deadline = Instant::now() + LONG;
    while info_field(&mut r, "aof_last_write_status") != "ok" {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(1500));
    drop((r, m2));
    replica.kill();
    drop(second);
    let mut r = replica.restart();
    assert_eq!(
        r.cmd(&["EXISTS", "b:0"]).i(),
        0,
        "the replaced dataset's writes"
    );
    assert_eq!(r.cmd(&["EXISTS", "a:0"]).i(), 0, "the replaced dataset");
    assert_eq!(r.cmd(&["DBSIZE"]).i(), 210);
}

/// An old log that ends in a record cut short, left by a checkpoint that
/// did not finish, and then a save with the AOF off, which records the
/// logs beside its dump: the next start with the AOF on finds the old log
/// older than that dump and leaves it out (kept aside), without cutting it,
/// and the start after that judges it the same way.
#[test]
fn a_torn_old_log_beside_a_dump_saved_with_the_aof_off_is_judged_the_same_twice() {
    let mut server = Server::start("tokio", "dump-installing");
    let mut c = server.conn();
    fill_seq(&mut c, 0, 100);
    assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
    assert!(server.reached("dump-installing.1", LONG));
    fill_seq(&mut c, 100, 100);
    durable(&mut c);
    drop(c);
    server.kill();
    server.release("dump-installing.1");
    // The old log cut short.
    let prev = server.dir.join(OLD_LOG);
    let len = std::fs::metadata(&prev).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&prev)
        .unwrap()
        .set_len(len - 5)
        .unwrap();
    // A run with the AOF off saves a dump beside the logs.
    server.failpoints.clear();
    server.aof = false;
    let mut c = server.restart();
    assert!(c.cmd(&["SET", "off", "1"]).ok());
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    server.kill();
    server.aof = true;
    let prev_before = std::fs::read(&prev).ok();
    let mut c = server.restart();
    let first = (c.cmd(&["DBSIZE"]).i(), c.cmd(&["GET", "off"]));
    assert_eq!(first.1.s(), "1");
    let kept = kept_aside(&server.dir).len();
    if let Some(before) = &prev_before
        && server.dir.join(OLD_LOG).exists()
    {
        assert_eq!(
            &std::fs::read(&prev).unwrap(),
            before,
            "the old log was cut"
        );
    }
    drop(c);
    server.kill();
    let mut c = server.restart();
    assert_eq!((c.cmd(&["DBSIZE"]).i(), c.cmd(&["GET", "off"])), first);
    assert_eq!(kept_aside(&server.dir).len(), kept);
}

/// `INFO` shows a `BGSAVE` and a `BGREWRITEAOF` while they run, how long
/// each has taken, and neither once it is done. No fork is made, so
/// `latest_fork_usec` stays 0.
#[test]
fn info_shows_a_save_and_a_rewrite_while_they_run() {
    let field = |c: &mut Conn, section: &str, name: &str| -> i64 {
        let info = c.cmd(&["INFO", section]);
        info.s()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("no {name} in INFO {section}"))
            .trim()
            .parse()
            .unwrap()
    };
    for &io in io_models() {
        let server = Server::start(io, "checkpoint-cutting");
        let mut c = server.conn();
        fill(&mut c, "k", 100);
        assert_eq!(
            field(&mut c, "persistence", "rdb_current_bgsave_time_sec"),
            -1
        );
        assert_eq!(
            field(&mut c, "persistence", "aof_current_rewrite_time_sec"),
            -1
        );

        assert_eq!(c.cmd(&["BGREWRITEAOF"]).s(), STARTED);
        assert!(server.reached("checkpoint-cutting.1", LONG), "{io}");
        assert_eq!(field(&mut c, "persistence", "aof_rewrite_in_progress"), 1);
        assert!(field(&mut c, "persistence", "aof_current_rewrite_time_sec") >= 0);
        assert_eq!(field(&mut c, "persistence", "rdb_bgsave_in_progress"), 0);
        assert_eq!(
            field(&mut c, "persistence", "rdb_current_bgsave_time_sec"),
            -1
        );
        server.release("checkpoint-cutting.1");
        wait_idle(&mut c);
        assert_eq!(
            field(&mut c, "persistence", "aof_current_rewrite_time_sec"),
            -1
        );

        assert_eq!(c.cmd(&["BGSAVE"]).s(), "Background saving started");
        assert!(server.reached("checkpoint-cutting.2", LONG), "{io}");
        assert_eq!(field(&mut c, "persistence", "rdb_bgsave_in_progress"), 1);
        assert!(field(&mut c, "persistence", "rdb_current_bgsave_time_sec") >= 0);
        server.release("checkpoint-cutting.2");
        wait_idle(&mut c);
        assert_eq!(
            field(&mut c, "persistence", "rdb_current_bgsave_time_sec"),
            -1
        );
        assert!(field(&mut c, "persistence", "rdb_last_bgsave_time_sec") >= 0);
        assert_eq!(field(&mut c, "stats", "latest_fork_usec"), 0);
    }
}
