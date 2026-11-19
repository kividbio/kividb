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
/// own; its reply is what the handle gives.
fn start_held_script(server: &Server, sha: &str, tag: &str) -> std::thread::JoinHandle<V> {
    let mut c = server.conn();
    let keys = keys_of(tag);
    let mut args = vec!["EVALSHA", sha, "4"];
    args.extend(keys.iter().map(String::as_str));
    args.push("script-stall:1");
    c.send(&args);
    let handle = std::thread::spawn(move || c.read());
    // Under way, and held: the stall is long, and the script reaches it at
    // once.
    std::thread::sleep(Duration::from_millis(150));
    handle
}

/// A new connection on a thread that is not busy running a held script:
/// the first of several to answer a `PING`. (On the io_uring loop the
/// kernel places connections; on Tokio a connection whose thread is busy
/// waits for it too.)
fn free_conn(server: &Server) -> Conn {
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
    rx.recv().unwrap()
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
    let mut c = server.conn();
    let mut args = vec!["EVALSHA", sha, "6"];
    args.extend(SIX);
    args.push("script-stall:1");
    c.send(&args);
    let handle = std::thread::spawn(move || c.read());
    std::thread::sleep(Duration::from_millis(150));
    handle
}

fn count(reply: &V) -> usize {
    match reply {
        V::Arr(items) => items.len(),
        other => panic!("not an array: {other:?}"),
    }
}

/// What a reader found while the script was held: its name, and whether it
/// was the state before the run or after it (`None`: neither).
type Seen = (&'static str, Option<&'static str>);

/// Every reader of keys it does not name, started while a script that runs
/// beside others is held between its first three writes and its last
/// three, finds the six keys all there or none there: `INFO keyspace`,
/// `DBSIZE`, `FT.SEARCH`, `FT.AGGREGATE`, `FT.INFO`, `SORT` with a `GET`
/// pattern, `TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`, `TS.QUERYINDEX`. The
/// readers are sent from connections on threads not busy with the script.
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
    let sha = quick_six(&mut c);
    let base = 1; // plainlist
    let script = held_six(&server, &sha);

    let readers: Vec<std::thread::JoinHandle<Seen>> = (0..10)
        .map(|kind| {
            let mut r = free_conn(&server);
            std::thread::spawn(move || {
                let whole = |n: usize, none: usize, all: usize| match n {
                    n if n == none => Some("before"),
                    n if n == all => Some("after"),
                    _ => None,
                };
                match kind {
                    0 => {
                        let V::Str(text) = r.cmd(&["INFO", "keyspace"]) else {
                            panic!("INFO")
                        };
                        let keys: usize = text
                            .split("keys=")
                            .nth(1)
                            .and_then(|rest| rest.split(',').next())
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(0);
                        ("INFO keyspace", whole(keys, base, base + 6))
                    }
                    1 => {
                        // `MEMORY STATS` counts bytes, not keys: `DBSIZE`,
                        // which takes the gate alone, stands in.
                        let V::Int(keys) = r.cmd(&["DBSIZE"]) else {
                            panic!("DBSIZE")
                        };
                        ("DBSIZE", whole(keys as usize, base, base + 6))
                    }
                    2 => {
                        let reply = r.cmd(&["FT.SEARCH", "idx772", "hello", "NOCONTENT"]);
                        let V::Arr(items) = &reply else {
                            panic!("FT.SEARCH {reply:?}")
                        };
                        let V::Int(total) = items[0] else {
                            panic!("total")
                        };
                        ("FT.SEARCH", whole(total as usize, 0, 2))
                    }
                    3 => {
                        let reply = r.cmd(&["FT.AGGREGATE", "idx772", "hello"]);
                        let V::Arr(items) = &reply else {
                            panic!("FT.AGGREGATE {reply:?}")
                        };
                        ("FT.AGGREGATE", whole(items.len() - 1, 0, 2))
                    }
                    4 => {
                        let reply = r.cmd(&["FT.INFO", "idx772"]);
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
                        ("FT.INFO num_docs", whole(docs, 0, 2))
                    }
                    5 => {
                        let reply = r.cmd(&["SORT", "plainlist", "BY", "nosort", "GET", "{t}:v_*"]);
                        let seen = match reply {
                            V::Arr(items) if items == [V::Nil, V::Nil] => Some("before"),
                            V::Arr(items) if items == [V::Str("x".into()), V::Str("y".into())] => {
                                Some("after")
                            }
                            _ => None,
                        };
                        ("SORT ... GET", seen)
                    }
                    6 => (
                        "TS.MGET",
                        whole(count(&r.cmd(&["TS.MGET", "FILTER", "grp=w772"])), 0, 2),
                    ),
                    7 => (
                        "TS.MRANGE",
                        whole(
                            count(&r.cmd(&["TS.MRANGE", "-", "+", "FILTER", "grp=w772"])),
                            0,
                            2,
                        ),
                    ),
                    8 => (
                        "TS.MREVRANGE",
                        whole(
                            count(&r.cmd(&["TS.MREVRANGE", "-", "+", "FILTER", "grp=w772"])),
                            0,
                            2,
                        ),
                    ),
                    _ => (
                        "TS.QUERYINDEX",
                        whole(count(&r.cmd(&["TS.QUERYINDEX", "grp=w772"])), 0, 2),
                    ),
                }
            })
        })
        .collect();
    let seen: Vec<Seen> = readers.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(script.join().unwrap(), V::Int(1));
    let half: Vec<&str> = seen
        .iter()
        .filter(|(_, whole)| whole.is_none())
        .map(|(name, _)| *name)
        .collect();
    assert!(
        half.is_empty(),
        "saw the script part-way through: {half:?} ({seen:?})"
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
    let mut r = free_conn(&server);
    let counted = r.cmd(&["CLUSTER", "COUNTKEYSINSLOT", &slot]);
    let mut r2 = free_conn(&server);
    let listed = count(&r2.cmd(&["CLUSTER", "GETKEYSINSLOT", &slot, "100"]));
    assert_eq!(script.join().unwrap(), V::Int(1));
    assert!(
        counted == V::Int(0) || counted == V::Int(6),
        "COUNTKEYSINSLOT found {counted:?}"
    );
    assert!(listed == 0 || listed == 6, "GETKEYSINSLOT found {listed}");
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
