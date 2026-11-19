// tests/keyspace_walks.rs
//
// With `lua-parallel-scripts` on, a walk of the keyspace (`KEYS`, `SCAN`)
// keeps the scripts that run beside others out while it runs, and nothing
// else (#772; `DBSIZE` and `RANDOMKEY` still take the command gate alone): it finds every script's keys as they were
// before it or as they are after it, never between, as in Redis, where a
// script is one step; and connections that are not walking are served
// meanwhile, where the walk used to take the command gate alone and stop them
// all. Real `kividb` processes (debug builds): a script is held part-way
// through by `KIVIDB_TEST_SCRIPT_STALL_MS` (`pause::test_stall_in_script`).
// On both I/O loops.
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
        Self::start_in(io, threads, &[], |_| {})
    }

    fn start_with(io: &str, threads: u32, extra: &[&str]) -> Self {
        Self::start_in(io, threads, extra, |_| {})
    }

    /// In a directory of its own, `prepare`d before the server starts.
    fn start_in(
        io: &str,
        threads: u32,
        extra: &[&str],
        prepare: impl FnOnce(&std::path::Path),
    ) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        prepare(&dir);
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
                .args(extra)
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

    /// The file `pause::test_stall_in_script` keeps in the server's
    /// directory while a script is held where it calls
    /// `ECHO script-stall:...`.
    fn stall_flag(&self) -> std::path::PathBuf {
        self.dir.join("script-stall.held")
    }

    /// Whether a script is held there now.
    fn holding(&self) -> bool {
        self.stall_flag().exists()
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

/// Makes `{t}:a` `{t}:b`, holds when `ARGV[1]` says to, then makes `{t}:c`,
/// deletes `{t}:a` and renames `{t}:b` to `{t}:d`. Before a run of it on an
/// empty tag there is nothing; after it, `{t}:c` and `{t}:d`. `{t}:a` and
/// `{t}:b` exist only part-way through.
const MAKES_AND_UNMAKES: &str = "redis.call('SET', KEYS[1], 1) redis.call('SET', KEYS[2], 1) \
     redis.call('ECHO', ARGV[1]) \
     redis.call('SET', KEYS[3], 1) redis.call('DEL', KEYS[1]) \
     redis.call('RENAME', KEYS[2], KEYS[4]) return 1";

fn keys_of(tag: &str) -> Vec<String> {
    ["a", "b", "c", "d"]
        .iter()
        .map(|k| format!("{{{tag}}}:{k}"))
        .collect()
}

/// Runs the script on `tag`'s keys, held part-way through or not, and
/// empties the tag again unless held.
fn run_script(c: &mut Conn, sha: &str, tag: &str, hold: bool) {
    let keys = keys_of(tag);
    let mut args = vec!["EVALSHA", sha, "4"];
    args.extend(keys.iter().map(String::as_str));
    args.push(if hold { "script-stall:1" } else { "no-stall" });
    assert_eq!(c.cmd(&args), V::Int(1));
    let mut del = vec!["DEL"];
    del.extend(keys.iter().map(String::as_str));
    c.cmd(&del);
}

/// Loads the script and runs it quickly a few times on each of `tags`, so
/// that the server runs it beside others from then on; the tags are left
/// empty.
fn quick_script(c: &mut Conn, tags: &[&str]) -> String {
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", MAKES_AND_UNMAKES]) else {
        panic!("no digest")
    };
    for tag in tags {
        for _ in 0..3 {
            run_script(c, &sha, tag, false);
        }
    }
    sha
}

/// Starts the script on `tag`, held part-way through, on a connection of its
/// own, beside others ([`held_beside_others`]); its reply is what the
/// handle gives.
fn start_held_script(server: &Server, sha: &str, tag: &str) -> std::thread::JoinHandle<V> {
    held_beside_others(
        server,
        || {
            let mut c = server.conn();
            let keys = keys_of(tag);
            let mut args = vec!["EVALSHA", sha, "4"];
            args.extend(keys.iter().map(String::as_str));
            args.push("script-stall:1");
            c.send(&args);
            let handle = std::thread::spawn(move || c.read());
            server.wait_until_held();
            handle
        },
        || {
            let mut c = server.conn();
            for _ in 0..3 {
                run_script(&mut c, sha, tag, false);
            }
        },
    )
}

/// `INFO stats`' `scripts_run_beside_others`, from whichever of several new
/// connections answers first (one on the thread running a held script
/// waits for it).
fn scripts_beside_others(server: &Server) -> u64 {
    let (tx, rx) = std::sync::mpsc::channel();
    for _ in 0..8 {
        let mut c = server.conn();
        let tx = tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(beside_others_on(&mut c));
        });
    }
    drop(tx);
    rx.recv().expect("INFO stats has scripts_run_beside_others")
}

/// `INFO stats`' `scripts_run_beside_others`, asked on `c`.
fn beside_others_on(c: &mut Conn) -> u64 {
    let V::Str(text) = c.cmd(&["INFO", "stats"]) else {
        panic!("INFO stats")
    };
    text.lines()
        .find_map(|line| line.strip_prefix("scripts_run_beside_others:"))
        .and_then(|n| n.trim().parse().ok())
        .expect("INFO stats has scripts_run_beside_others")
}

/// `start()`'s held script, once it runs beside others. A run that took over
/// five milliseconds makes the script run alone next time, holding every
/// thread: a debug build on a loaded machine can take that long for a warm
/// run. Then it is let finish, made quick again (`warm`), and started again;
/// five times at most.
fn held_beside_others<T>(
    server: &Server,
    mut start: impl FnMut() -> std::thread::JoinHandle<T>,
    mut warm: impl FnMut(),
) -> std::thread::JoinHandle<T> {
    for _ in 0..5 {
        let before = scripts_beside_others(server);
        let held = start();
        if scripts_beside_others(server) > before {
            return held;
        }
        let _ = held.join();
        warm();
    }
    panic!("the held script never ran beside others");
}

/// A new connection on a thread that is not busy running a held script:
/// the first of several to answer a `PING`, within a quarter of the stall.
/// (On the io_uring loop the kernel places connections; on Tokio a
/// connection whose thread is busy waits for it too.)
fn free_conn(server: &Server) -> Conn {
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
    let c = rx
        .recv_timeout(Duration::from_millis(STALL_MS / 4))
        .expect("no new connection was answered at once");
    assert!(asked.elapsed() < Duration::from_millis(STALL_MS / 4));
    c
}

fn names(reply: &V) -> Vec<String> {
    let V::Arr(items) = reply else {
        panic!("not an array: {reply:?}")
    };
    let mut names: Vec<String> = items
        .iter()
        .map(|item| match item {
            V::Str(s) => s.clone(),
            other => panic!("not a name: {other:?}"),
        })
        .collect();
    names.sort();
    names
}

/// The keys of each reply of a whole `SCAN` with `extra` options.
fn scan_pages(c: &mut Conn, extra: &[&str]) -> Vec<Vec<String>> {
    let mut cursor = "0".to_string();
    let mut pages = Vec::new();
    loop {
        let mut args = vec!["SCAN", cursor.as_str()];
        args.extend_from_slice(extra);
        let reply = c.cmd(&args);
        let V::Arr(parts) = reply else {
            panic!("SCAN: {reply:?}")
        };
        let V::Str(next) = &parts[0] else {
            panic!("cursor")
        };
        pages.push(names(&parts[1]));
        cursor = next.clone();
        if cursor == "0" {
            return pages;
        }
    }
}

/// A walk started while a script is part-way through finds the script's
/// keys as they are after it: never `{t}:a` or `{t}:b`, which exist only
/// part-way through. `KEYS`, a whole `SCAN` (with `MATCH`, and with
/// `TYPE`), `DBSIZE` and `RANDOMKEY`.
fn a_walk_never_finds_a_script_part_way(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    let sha = quick_script(&mut c, &["t"]);
    let after: Vec<String> = vec!["{t}:c".into(), "{t}:d".into()];

    let script = start_held_script(&server, &sha, "t");
    let walkers: Vec<std::thread::JoinHandle<()>> = (0..4)
        .map(|kind| {
            let mut w = server.conn();
            let after = after.clone();
            // Each walk starts while the script is held.
            assert!(server.holding(), "the script was done before walk {kind}");
            std::thread::spawn(move || match kind {
                0 => assert_eq!(names(&w.cmd(&["KEYS", "{t}:*"])), after),
                1 => {
                    let mut found = scan_pages(&mut w, &["MATCH", "{t}:*"]).concat();
                    found.sort();
                    assert_eq!(found, after);
                }
                2 => {
                    let mut found = scan_pages(&mut w, &["TYPE", "string"]).concat();
                    found.sort();
                    assert_eq!(found, after);
                }
                _ => {
                    assert_eq!(w.cmd(&["DBSIZE"]), V::Int(2));
                    for _ in 0..20 {
                        let V::Str(key) = w.cmd(&["RANDOMKEY"]) else {
                            panic!("RANDOMKEY")
                        };
                        assert!(after.contains(&key), "{key}");
                    }
                }
            })
        })
        .collect();
    // Sent while it was held.
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        server.holding(),
        "the script was done before the walks were sent"
    );
    for walker in walkers {
        walker.join().unwrap();
    }
    assert_eq!(script.join().unwrap(), V::Int(1));
}

#[test]
fn a_walk_never_finds_a_script_part_way_tokio() {
    a_walk_never_finds_a_script_part_way("tokio");
}

#[test]
fn a_walk_never_finds_a_script_part_way_uring() {
    a_walk_never_finds_a_script_part_way("uring");
}

/// While a walk waits for a script that is part-way through, the clients
/// of other keys are served. Where the walk took the command gate alone, its
/// wait held back every command that came after it, on every thread, until
/// the script was done.
fn a_waiting_walk_holds_back_no_other_command(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    let sha = quick_script(&mut c, &["t"]);
    assert_eq!(c.cmd(&["SET", "plain", "v"]), V::Str("OK".into()));
    let script = start_held_script(&server, &sha, "t");
    let mut walker = free_conn(&server);
    walker.send(&["KEYS", "*"]);
    std::thread::sleep(Duration::from_millis(50));
    let others: Vec<std::thread::JoinHandle<Duration>> = (0..8)
        .map(|_| {
            let mut other = server.conn();
            std::thread::spawn(move || {
                let asked = Instant::now();
                assert_eq!(other.cmd(&["GET", "plain"]), V::Str("v".into()));
                assert_eq!(other.cmd(&["SET", "plain2", "w"]), V::Str("OK".into()));
                asked.elapsed()
            })
        })
        .collect();
    let took: Vec<Duration> = others.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(script.join().unwrap(), V::Int(1));
    let keys = names(&walker.read());
    assert!(keys.contains(&"{t}:c".to_string()), "{keys:?}");
    assert!(!keys.contains(&"{t}:a".to_string()), "{keys:?}");
    // Those on the script's own thread, or the walk's, wait: the script
    // runs on its thread, and the walk waits on its own. The rest are
    // answered at once.
    let quick = took
        .iter()
        .filter(|t| **t < Duration::from_millis(STALL_MS / 4))
        .count();
    assert!(
        quick >= 1,
        "clients of other keys waited for a walk that waited for a script: {took:?}"
    );
}

#[test]
fn a_waiting_walk_holds_back_no_other_command_tokio() {
    a_waiting_walk_holds_back_no_other_command("tokio");
}

#[test]
fn a_waiting_walk_holds_back_no_other_command_uring() {
    a_waiting_walk_holds_back_no_other_command("uring");
}

/// Scripts that run beside others and walks of the keyspace, all at once on
/// many connections, for a while: no reply of a walk ever holds a key a
/// script makes and unmakes in one run, every script runs, and the walks
/// keep going (a stream of walks keeps no script out for ever, nor the
/// reverse).
fn walks_and_scripts_at_once(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    let tags = ["s0", "s1", "s2", "s3"];
    let sha = quick_script(&mut c, &tags);
    let until = Instant::now() + Duration::from_secs(3);
    let scripts: Vec<std::thread::JoinHandle<u64>> = tags
        .iter()
        .map(|tag| {
            let mut s = server.conn();
            let sha = sha.clone();
            let tag = tag.to_string();
            std::thread::spawn(move || {
                let mut runs = 0;
                while Instant::now() < until {
                    run_script(&mut s, &sha, &tag, false);
                    runs += 1;
                }
                runs
            })
        })
        .collect();
    let walkers: Vec<std::thread::JoinHandle<u64>> = (0..4)
        .map(|kind| {
            let mut w = server.conn();
            std::thread::spawn(move || {
                let mut walks = 0;
                while Instant::now() < until {
                    let found = if kind % 2 == 0 {
                        names(&w.cmd(&["KEYS", "{s*"]))
                    } else {
                        scan_pages(&mut w, &["MATCH", "{s*", "COUNT", "3"]).concat()
                    };
                    for key in &found {
                        assert!(
                            key.ends_with(":c") || key.ends_with(":d"),
                            "a walk found {key}, which a script makes and unmakes in one run"
                        );
                    }
                    walks += 1;
                }
                walks
            })
        })
        .collect();
    let runs: Vec<u64> = scripts.into_iter().map(|t| t.join().unwrap()).collect();
    let walks: Vec<u64> = walkers.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(runs.iter().all(|&n| n > 10), "{runs:?}");
    assert!(walks.iter().all(|&n| n > 10), "{walks:?}");
}

#[test]
fn walks_and_scripts_at_once_tokio() {
    walks_and_scripts_at_once("tokio");
}

#[test]
fn walks_and_scripts_at_once_uring() {
    walks_and_scripts_at_once("uring");
}

// ── Commands that read keys they do not name ────────────────────────────────

/// Writes three keys of `{t}` (a hash an index covers, a string a `SORT`
/// pattern reads, a time series with a label), is held when `ARGV[1]` says
/// so, and writes three more like them. Before a run on an empty tag none of
/// the six is there; after it, all six. Three exist only part-way through.
const THREE_THEN_THREE: &str = "\
    redis.call('HSET', KEYS[1], 'f', 'hello') \
    redis.call('SET', KEYS[2], 'x') \
    redis.call('TS.ADD', KEYS[3], 1, 1, 'LABELS', 'grp', 'w772') \
    redis.call('ECHO', ARGV[1]) \
    redis.call('HSET', KEYS[4], 'f', 'hello') \
    redis.call('SET', KEYS[5], 'y') \
    redis.call('TS.ADD', KEYS[6], 1, 1, 'LABELS', 'grp', 'w772') \
    return 1";

const SIX: [&str; 6] = [
    "{t}:doc1", "{t}:v_1", "{t}:ts1", "{t}:doc2", "{t}:v_2", "{t}:ts2",
];

fn run_six(c: &mut Conn, sha: &str, hold: bool) -> V {
    let mut args = vec!["EVALSHA", sha, "6"];
    args.extend(SIX);
    args.push(if hold { "script-stall:1" } else { "no-stall" });
    c.cmd(&args)
}

fn delete_six(c: &mut Conn) {
    let mut del = vec!["DEL"];
    del.extend(SIX);
    c.cmd(&del);
}

/// Loads the script and runs it quickly a few times, leaving the tag empty:
/// from then on it runs beside others.
fn quick_six(c: &mut Conn) -> String {
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", THREE_THEN_THREE]) else {
        panic!("no digest")
    };
    for _ in 0..3 {
        assert_eq!(run_six(c, &sha, false), V::Int(1));
        delete_six(c);
    }
    sha
}

fn held_six(server: &Server, sha: &str) -> std::thread::JoinHandle<V> {
    held_beside_others(
        server,
        || {
            let mut c = server.conn();
            let mut args = vec!["EVALSHA", sha, "6"];
            args.extend(SIX);
            args.push("script-stall:1");
            c.send(&args);
            let handle = std::thread::spawn(move || c.read());
            server.wait_until_held();
            handle
        },
        || {
            let mut c = server.conn();
            for _ in 0..3 {
                assert_eq!(run_six(&mut c, sha, false), V::Int(1));
                delete_six(&mut c);
            }
        },
    )
}

fn count(reply: &V) -> usize {
    match reply {
        V::Arr(items) => items.len(),
        other => panic!("not an array: {other:?}"),
    }
}

/// What a reader found while the script was held: its name, whether it
/// was the state before the run or after it (`None`: neither), and whether
/// the script was still held once the reader had been sent.
type Seen = (&'static str, Option<&'static str>, bool);

/// Every reader of keys it does not name, started while a script that runs
/// beside others is held between its first three writes and its last
/// three, finds the six keys all there or none there: `INFO keyspace`,
/// `DBSIZE`, `FT.SEARCH`, `FT.AGGREGATE`, `FT.INFO`, `FT.PROFILE`, `SORT`
/// with a `GET` pattern, `TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`,
/// `TS.QUERYINDEX`; and `FT.CURSOR READ` of a cursor made before is
/// answered only once the script is done. The readers are sent from
/// connections on threads not busy with the script, while it is held.
fn readers_see_a_script_whole_or_not_at_all(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&[
            "FT.CREATE",
            "idx772",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "{t}:doc",
            "SCHEMA",
            "f",
            "TEXT"
        ]),
        V::Str("OK".into())
    );
    assert_eq!(c.cmd(&["RPUSH", "plainlist", "1", "2"]), V::Int(2));
    // A cursor over another index, made before the script runs.
    assert_eq!(
        c.cmd(&[
            "FT.CREATE",
            "idxc",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "{c}:",
            "SCHEMA",
            "f",
            "TEXT"
        ]),
        V::Str("OK".into())
    );
    for doc in ["{c}:1", "{c}:2", "{c}:3"] {
        assert_eq!(c.cmd(&["HSET", doc, "f", "hello"]), V::Int(1));
    }
    let V::Arr(made) = c.cmd(&[
        "FT.AGGREGATE",
        "idxc",
        "hello",
        "LOAD",
        "1",
        "@f",
        "WITHCURSOR",
        "COUNT",
        "1",
    ]) else {
        panic!("WITHCURSOR")
    };
    let V::Int(cursor) = made[1] else {
        panic!("cursor {made:?}")
    };
    let cursor = cursor.to_string();
    let sha = quick_six(&mut c);
    let base = 4; // plainlist and the cursor's documents
    let script = held_six(&server, &sha);

    // Every connection first: a reader that waits for the script holds the
    // thread it is served on (`tag_locks::walk`), which then answers no
    // new connection.
    let conns: Vec<Conn> = (0..12).map(|_| free_conn(&server)).collect();
    let readers: Vec<std::thread::JoinHandle<Seen>> = conns
        .into_iter()
        .enumerate()
        .map(|(kind, mut r)| {
            let flag = server.stall_flag();
            let cursor = cursor.clone();
            std::thread::spawn(move || {
                // Sent while the script is held, and the reply read: what the
                // reader saw, and whether the script was still held then.
                let mut ask = |args: &[&str]| -> (V, bool) {
                    r.send(args);
                    let overlapped = flag.exists();
                    (r.read(), overlapped)
                };
                let whole = |n: usize, none: usize, all: usize| match n {
                    n if n == none => Some("before"),
                    n if n == all => Some("after"),
                    _ => None,
                };
                match kind {
                    0 => {
                        let (V::Str(text), sent_held) = ask(&["INFO", "keyspace"]) else {
                            panic!("INFO")
                        };
                        let keys: usize = text
                            .split("keys=")
                            .nth(1)
                            .and_then(|rest| rest.split(',').next())
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(0);
                        ("INFO keyspace", whole(keys, base, base + 6), sent_held)
                    }
                    1 => {
                        // `MEMORY STATS` counts bytes, not keys: `DBSIZE`,
                        // which takes the gate alone, stands in.
                        let (V::Int(keys), sent_held) = ask(&["DBSIZE"]) else {
                            panic!("DBSIZE")
                        };
                        ("DBSIZE", whole(keys as usize, base, base + 6), sent_held)
                    }
                    2 => {
                        let (reply, sent_held) =
                            ask(&["FT.SEARCH", "idx772", "hello", "NOCONTENT"]);
                        let V::Arr(items) = &reply else {
                            panic!("FT.SEARCH {reply:?}")
                        };
                        let V::Int(total) = items[0] else {
                            panic!("total")
                        };
                        ("FT.SEARCH", whole(total as usize, 0, 2), sent_held)
                    }
                    3 => {
                        let (reply, sent_held) = ask(&["FT.AGGREGATE", "idx772", "hello"]);
                        let V::Arr(items) = &reply else {
                            panic!("FT.AGGREGATE {reply:?}")
                        };
                        ("FT.AGGREGATE", whole(items.len() - 1, 0, 2), sent_held)
                    }
                    4 => {
                        let (reply, sent_held) = ask(&["FT.INFO", "idx772"]);
                        let V::Arr(items) = &reply else {
                            panic!("FT.INFO {reply:?}")
                        };
                        let at = items
                            .iter()
                            .position(|item| *item == V::Str("num_docs".into()))
                            .expect("num_docs");
                        let docs: usize = match &items[at + 1] {
                            V::Int(n) => *n as usize,
                            V::Str(n) => n.parse().unwrap(),
                            other => panic!("num_docs {other:?}"),
                        };
                        ("FT.INFO num_docs", whole(docs, 0, 2), sent_held)
                    }
                    5 => {
                        let (reply, sent_held) =
                            ask(&["SORT", "plainlist", "BY", "nosort", "GET", "{t}:v_*"]);
                        let seen = match reply {
                            V::Arr(items) if items == [V::Nil, V::Nil] => Some("before"),
                            V::Arr(items) if items == [V::Str("x".into()), V::Str("y".into())] => {
                                Some("after")
                            }
                            _ => None,
                        };
                        ("SORT ... GET", seen, sent_held)
                    }
                    6 => {
                        let (reply, sent_held) = ask(&["TS.MGET", "FILTER", "grp=w772"]);
                        ("TS.MGET", whole(count(&reply), 0, 2), sent_held)
                    }
                    7 => {
                        let (reply, sent_held) =
                            ask(&["TS.MRANGE", "-", "+", "FILTER", "grp=w772"]);
                        ("TS.MRANGE", whole(count(&reply), 0, 2), sent_held)
                    }
                    8 => {
                        let (reply, sent_held) =
                            ask(&["TS.MREVRANGE", "-", "+", "FILTER", "grp=w772"]);
                        ("TS.MREVRANGE", whole(count(&reply), 0, 2), sent_held)
                    }
                    9 => {
                        let (reply, sent_held) = ask(&["TS.QUERYINDEX", "grp=w772"]);
                        ("TS.QUERYINDEX", whole(count(&reply), 0, 2), sent_held)
                    }
                    10 => {
                        let (reply, sent_held) = ask(&[
                            "FT.PROFILE",
                            "idx772",
                            "SEARCH",
                            "QUERY",
                            "hello",
                            "NOCONTENT",
                        ]);
                        // The search's own reply comes first.
                        let V::Arr(parts) = &reply else {
                            panic!("FT.PROFILE {reply:?}")
                        };
                        let V::Arr(found) = &parts[0] else {
                            panic!("FT.PROFILE {reply:?}")
                        };
                        let V::Int(total) = found[0] else {
                            panic!("total")
                        };
                        ("FT.PROFILE", whole(total as usize, 0, 2), sent_held)
                    }
                    _ => {
                        // Read while the script is held, it is answered after
                        // the script: the state after it.
                        let (reply, sent_held) = ask(&["FT.CURSOR", "READ", "idxc", &cursor]);
                        assert!(matches!(reply, V::Arr(_)), "FT.CURSOR READ {reply:?}");
                        let after = !flag.exists();
                        ("FT.CURSOR READ", after.then_some("after"), sent_held)
                    }
                }
            })
        })
        .collect();
    let seen: Vec<Seen> = readers.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(script.join().unwrap(), V::Int(1));
    let half: Vec<&str> = seen
        .iter()
        .filter(|(_, whole, _)| whole.is_none())
        .map(|(name, _, _)| *name)
        .collect();
    assert!(
        half.is_empty(),
        "saw the script part-way through: {half:?} ({seen:?})"
    );
    let late: Vec<&str> = seen
        .iter()
        .filter(|(_, _, sent_held)| !sent_held)
        .map(|(name, _, _)| *name)
        .collect();
    assert!(
        late.is_empty(),
        "sent after the script was done, so they prove nothing: {late:?}"
    );
}

#[test]
fn readers_see_a_script_whole_or_not_at_all_tokio() {
    readers_see_a_script_whole_or_not_at_all("tokio");
}

#[test]
fn readers_see_a_script_whole_or_not_at_all_uring() {
    readers_see_a_script_whole_or_not_at_all("uring");
}

/// On a cluster node, `CLUSTER COUNTKEYSINSLOT` and `CLUSTER GETKEYSINSLOT`
/// of the script's slot find its keys all there or none there.
#[test]
fn a_slot_is_counted_with_a_script_whole_or_not_at_all() {
    let server = Server::start_with("uring", 4, &["--cluster-enabled", "yes"]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]),
        V::Str("OK".into())
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while c.cmd(&["SET", "{t}:probe", "1"]) != V::Str("OK".into()) {
        assert!(Instant::now() < deadline, "the slots did not come up");
        std::thread::sleep(Duration::from_millis(50));
    }
    c.cmd(&["DEL", "{t}:probe"]);
    let V::Int(slot) = c.cmd(&["CLUSTER", "KEYSLOT", "{t}"]) else {
        panic!("KEYSLOT")
    };
    let slot = slot.to_string();
    // Its keys are of one tag, and so of one slot: the index and the time
    // series are not needed here; the hashes and strings are written.
    let sha = quick_six(&mut c);
    let script = held_six(&server, &sha);
    // Every connection first: a reader that waits for the script holds its
    // thread.
    let (mut r, mut r2, mut r3) = (free_conn(&server), free_conn(&server), free_conn(&server));
    r.send(&["CLUSTER", "COUNTKEYSINSLOT", &slot]);
    r2.send(&["CLUSTER", "GETKEYSINSLOT", &slot, "100"]);
    r3.send(&["CLUSTER", "SLOT-STATS", "SLOTSRANGE", &slot, &slot]);
    assert!(server.holding(), "the script was done before the readers");
    let counted = r.read();
    let listed = count(&r2.read());
    let stats = r3.read();
    assert_eq!(script.join().unwrap(), V::Int(1));
    assert!(
        counted == V::Int(0) || counted == V::Int(6),
        "COUNTKEYSINSLOT found {counted:?}"
    );
    assert!(listed == 0 || listed == 6, "GETKEYSINSLOT found {listed}");
    let stated = key_count(&stats).unwrap_or_else(|| panic!("SLOT-STATS {stats:?}"));
    assert!(stated == 0 || stated == 6, "SLOT-STATS found {stated}");
}

/// The `key-count` of a `CLUSTER SLOT-STATS` reply of one slot.
fn key_count(reply: &V) -> Option<i64> {
    match reply {
        V::Arr(items) => {
            if let Some(at) = items.iter().position(|v| *v == V::Str("key-count".into()))
                && let Some(V::Int(n)) = items.get(at + 1)
            {
                return Some(*n);
            }
            items.iter().find_map(key_count)
        }
        _ => None,
    }
}

/// A snapshot taken while a script runs beside others holds the script
/// whole or not at all: `SAVE` cuts between whole writes, and a script is
/// one of them (it holds the write gate for its whole run).
#[test]
fn a_snapshot_holds_a_script_whole_or_not_at_all() {
    let server = Server::start("uring", 4);
    let mut c = server.conn();
    let sha = quick_six(&mut c);
    let script = held_six(&server, &sha);
    let mut s = free_conn(&server);
    assert_eq!(s.cmd(&["SAVE"]), V::Str("OK".into()));
    assert_eq!(script.join().unwrap(), V::Int(1));
    // The snapshot, loaded by a server of its own.
    let dump = std::fs::read(server.dir.join("dump.kdb")).expect("dump.kdb");
    let other = Server::start_in("uring", 4, &[], |dir| {
        std::fs::write(dir.join("dump.kdb"), &dump).unwrap()
    });
    let mut o = other.conn();
    let keys = o.cmd(&["DBSIZE"]);
    assert!(
        keys == V::Int(0) || keys == V::Int(6),
        "the snapshot holds {keys:?} keys"
    );
}

/// Makes the index the readers of the script's keys look through.
fn create_index(c: &mut Conn) {
    assert_eq!(
        c.cmd(&[
            "FT.CREATE",
            "idx772",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "{t}:doc",
            "SCHEMA",
            "f",
            "TEXT"
        ]),
        V::Str("OK".into())
    );
}

/// A script that runs beside others may not call a command that reads or
/// changes keys it does not name: it gets the error a `KEYS` gets there,
/// while another such script is held part-way through. One that runs alone
/// may; and a section of `INFO` that counts no keys is allowed.
fn a_script_beside_others_reads_no_keys_it_does_not_name(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    create_index(&mut c);
    let held = quick_six(&mut c);
    // Runs what it is given, on a tag of its own; warmed up with nothing to
    // run, so that from then on it runs beside others.
    let V::Str(reader) = c.cmd(&[
        "SCRIPT",
        "LOAD",
        "if ARGV[1] == 'warm' then return 1 end return redis.pcall(unpack(ARGV))",
    ]) else {
        panic!("no digest")
    };
    for _ in 0..3 {
        assert_eq!(
            c.cmd(&["EVALSHA", &reader, "1", "{r}:x", "warm"]),
            V::Int(1)
        );
    }
    let script = held_six(&server, &held);
    let refused: [&[&str]; 9] = [
        &["INFO", "keyspace"],
        &["INFO"],
        &["FT.SEARCH", "idx772", "hello", "NOCONTENT"],
        &["FT.AGGREGATE", "idx772", "hello"],
        &["FT.INFO", "idx772"],
        &["TS.MGET", "FILTER", "grp=w772"],
        &["TS.QUERYINDEX", "grp=w772"],
        &["MEMORY", "STATS"],
        &["FT.DROPINDEX", "idx772", "DD"],
    ];
    for call in refused {
        let mut r = free_conn(&server);
        let mut args = vec!["EVALSHA", reader.as_str(), "1", "{r}:x"];
        args.extend_from_slice(call);
        let reply = r.cmd(&args);
        assert!(
            matches!(&reply, V::Err(e) if e.contains("which looks at keys it does not name")),
            "{call:?} from a script beside others: {reply:?}"
        );
    }
    let mut r = free_conn(&server);
    let mut args = vec!["EVALSHA", reader.as_str(), "1", "{r}:x"];
    args.extend_from_slice(&["INFO", "server"]);
    assert!(matches!(r.cmd(&args), V::Str(_)), "INFO server");
    assert!(server.holding(), "the script was done before the calls");
    assert_eq!(script.join().unwrap(), V::Int(1));
    // A script of no keys runs alone.
    let alone = c.cmd(&["EVAL", "return redis.call('INFO', 'keyspace')", "0"]);
    assert!(
        matches!(alone, V::Str(ref text) if text.contains("keys=")),
        "{alone:?}"
    );
    // The index is still there.
    assert!(matches!(c.cmd(&["FT.INFO", "idx772"]), V::Arr(_)));
}

#[test]
fn a_script_beside_others_reads_no_keys_it_does_not_name_tokio() {
    a_script_beside_others_reads_no_keys_it_does_not_name("tokio");
}

#[test]
fn a_script_beside_others_reads_no_keys_it_does_not_name_uring() {
    a_script_beside_others_reads_no_keys_it_does_not_name("uring");
}

/// `FT.DROPINDEX … DD` sent while a script that runs beside others is held
/// between the writes of two documents of the index runs after the script:
/// both documents go.
fn a_drop_of_documents_waits_for_scripts(io: &str) {
    let server = Server::start(io, 4);
    let mut c = server.conn();
    create_index(&mut c);
    let sha = quick_six(&mut c);
    let script = held_six(&server, &sha);
    let mut d = free_conn(&server);
    d.send(&["FT.DROPINDEX", "idx772", "DD"]);
    std::thread::sleep(Duration::from_millis(20));
    assert!(server.holding(), "the script was done before the drop");
    let reply = d.read();
    let after = !server.holding();
    assert_eq!(reply, V::Str("OK".into()));
    assert!(
        after,
        "the documents were dropped while the script was held"
    );
    assert_eq!(script.join().unwrap(), V::Int(1));
    assert_eq!(c.cmd(&["EXISTS", "{t}:doc1", "{t}:doc2"]), V::Int(0));
}

#[test]
fn a_drop_of_documents_waits_for_scripts_tokio() {
    a_drop_of_documents_waits_for_scripts("tokio");
}

#[test]
fn a_drop_of_documents_waits_for_scripts_uring() {
    a_drop_of_documents_waits_for_scripts("uring");
}
