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

/// A server with the AOF on, in `dir`, its output appended to `server.log`.
fn launch(dir: &Path, port: u16, io: &str, failpoints: &str) -> Child {
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
            "--aof",
        ])
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
}

impl Server {
    fn start(io: &'static str, failpoints: &str) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let child = launch(&dir, port, io, failpoints);
        let server = Server {
            port,
            dir,
            child,
            io,
            failpoints: failpoints.to_string(),
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
        self.child = launch(&self.dir, self.port, self.io, &self.failpoints);
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
