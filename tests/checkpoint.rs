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

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see tests/backup.rs).
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

/// A port nothing is listening on, outside the range the system hands out
/// to outgoing connections.
fn free_port() -> u16 {
    use std::sync::atomic::AtomicU16;
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(37);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

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
}

impl Server {
    fn start(io: &'static str, failpoints: &str) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let child = launch(&dir, port, io, failpoints, true);
        let server = Server {
            port,
            dir,
            child,
            io,
            failpoints: failpoints.to_string(),
            aof: true,
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
        self.child = launch(&self.dir, self.port, self.io, &self.failpoints, self.aof);
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
        // Connections are spread over the server's threads, and the enable
        // holds its own while it waits at the step: the disable is sent on
        // several, and one on another thread is answered.
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
