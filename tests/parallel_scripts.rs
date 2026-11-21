#![allow(clippy::disallowed_methods)]
// tests/parallel_scripts.rs
//
// `lua-parallel-scripts`: scripts whose KEYS share a hash tag, run beside
// each other under their tags' locks (`src/tag_locks.rs`). Real `kividb`
// processes: the setting is the whole process's, and so is the set of
// scripts a server has seen to be quick.
#![cfg(feature = "lua")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::{restart_verified, start_verified};

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
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
    }
    fn is_err(&self) -> bool {
        matches!(self, V::Err(_))
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
    dir: PathBuf,
    args: Vec<String>,
}

/// What `extra` may hold besides the server's arguments: the server is
/// started with `KIVIDB_TEST_LOOK` set, so a queued script's connection looks
/// for its reply whatever the CPUs of the machine (`lua_vm_blocking`).
const LOOK: &str = "@look";

/// `kividb` on `port`, in `dir`.
fn spawn(dir: &std::path::Path, port: u16, extra: &[String]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let look = extra.iter().any(|a| a.as_str() == LOOK);
    let extra: Vec<&String> = extra.iter().filter(|a| a.as_str() != LOOK).collect();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
    if look {
        command.env("KIVIDB_TEST_LOOK", "1");
    }
    command
        .current_dir(dir)
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--threads",
            "4",
            "--update-check",
            "no",
        ])
        .args(
            (!extra.iter().any(|a| a.as_str() == "--io-model"))
                .then_some(["--io-model", "tokio"])
                .into_iter()
                .flatten(),
        )
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        // Verified to be the process started: see tests/common/mod.rs.
        let (child, port) = start_verified(|port| spawn(&dir, port, &args));
        let server = Server {
            port,
            child,
            dir,
            args,
        };
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// Killed, with no chance to save or expire anything more, and started
    /// again on the same port and directory: what it holds is what its AOF
    /// replays to.
    fn kill_and_restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // On the same port if it is still free, and on another if a test of
        // another process has taken it in the gap (the data directory is what
        // matters): `self.port` is the one it is on.
        let (child, port, _) =
            restart_verified(|port| spawn(&self.dir, port, &self.args), self.port);
        self.child = child;
        self.port = port;
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

    fn aof(&self) -> Vec<u8> {
        std::fs::read(self.dir.join("appendonly.aof")).unwrap_or_default()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const READS_A_KEY: &str = "return redis.call('GET', ARGV[1])";

/// On unless switched off: a server started with nothing said holds a tagged
/// script to its tag.
#[test]
fn it_is_on_unless_switched_off() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "lua-parallel-scripts"]),
        V::Arr(vec![
            V::Str("lua-parallel-scripts".into()),
            V::Str("yes".into())
        ])
    );
    assert!(c.cmd(&["SET", "elsewhere", "v"]).ok());
    let refused = c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]);
    assert!(refused.is_err(), "{refused:?}");
}

/// Switched off, a script may touch any key, as it always could.
#[test]
fn switched_off_a_script_may_touch_any_key() {
    let server = Server::start(&["--lua-parallel-scripts", "false"]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "lua-parallel-scripts"]),
        V::Arr(vec![
            V::Str("lua-parallel-scripts".into()),
            V::Str("no".into())
        ])
    );
    assert!(c.cmd(&["SET", "elsewhere", "v"]).ok());
    for _ in 0..5 {
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]),
            V::Str("v".into())
        );
    }
}

/// On, a script whose keys share a hash tag is held to that tag — from its
/// first run, and every time after, so that it never works until the day it
/// is run another way. A script with other keys may touch what it likes.
#[test]
fn a_tagged_script_is_held_to_its_tag() {
    let server = Server::start(&["--lua-parallel-scripts", "true"]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "lua-parallel-scripts"]),
        V::Arr(vec![
            V::Str("lua-parallel-scripts".into()),
            V::Str("yes".into())
        ])
    );
    assert!(c.cmd(&["SET", "elsewhere", "v"]).ok());
    assert!(c.cmd(&["SET", "{q}:b", "mine"]).ok());
    assert!(c.cmd(&["SET", "{r}:b", "theirs"]).ok());

    // Many times: the first runs and the later ones take different roads.
    for run in 0..8 {
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "{q}:b"]),
            V::Str("mine".into()),
            "run {run}"
        );
        for outside in ["elsewhere", "{r}:b"] {
            let refused = c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", outside]);
            assert!(
                refused.is_err(),
                "run {run}: {outside} was read: {refused:?}"
            );
            assert!(
                refused.s().contains("outside the hash tag 'q'"),
                "run {run}: {refused:?}"
            );
        }
        // No keys, an untagged key: not held to anything.
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "0", "elsewhere"]),
            V::Str("v".into())
        );
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "1", "plain", "{r}:b"]),
            V::Str("theirs".into())
        );
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "2", "{q}:a", "plain", "elsewhere"]),
            V::Str("v".into())
        );
        // Keys of two tags: held to both (#871).
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "2", "{r}:a", "{q}:a", "{q}:b"]),
            V::Str("mine".into()),
            "run {run}"
        );
        assert_eq!(
            c.cmd(&["EVAL", READS_A_KEY, "2", "{r}:a", "{q}:a", "{r}:b"]),
            V::Str("theirs".into()),
            "run {run}"
        );
        for outside in ["elsewhere", "{s}:b"] {
            let refused = c.cmd(&["EVAL", READS_A_KEY, "2", "{r}:a", "{q}:a", outside]);
            assert!(
                refused.is_err()
                    && refused
                        .s()
                        .contains("outside the hash tags 'r', 'q' of its KEYS"),
                "run {run}: {outside}: {refused:?}"
            );
        }
    }

    // Nor may it look at keys it does not name.
    let refused = c.cmd(&["EVAL", "return redis.call('DBSIZE')", "1", "{q}:a"]);
    assert!(refused.is_err(), "{refused:?}");
    assert!(
        refused.s().contains("looks at keys it does not name"),
        "{refused:?}"
    );
    // A client may, and a script with no tag.
    assert!(matches!(c.cmd(&["DBSIZE"]), V::Int(3)));
    assert!(matches!(
        c.cmd(&["EVAL", "return redis.call('DBSIZE')", "0"]),
        V::Int(3)
    ));

    // Inside a transaction too.
    assert!(c.cmd(&["MULTI"]).ok());
    assert_eq!(
        c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]).s(),
        "QUEUED"
    );
    let V::Arr(replies) = c.cmd(&["EXEC"]) else {
        panic!("EXEC did not answer with an array")
    };
    assert!(
        replies[0].s().contains("outside the hash tag 'q'"),
        "{replies:?}"
    );

    // Switched off, it may again.
    assert!(c.cmd(&["CONFIG", "SET", "lua-parallel-scripts", "no"]).ok());
    assert_eq!(
        c.cmd(&["EVAL", READS_A_KEY, "1", "{q}:a", "elsewhere"]),
        V::Str("v".into())
    );
}

/// Counts to ARGV[1], writes KEYS[1]. Quick or slow as it is asked to be.
const COUNTS: &str = "local n = 0 for i = 1, tonumber(ARGV[1]) do n = n + 1 end \
                      redis.call('SET', KEYS[1], ARGV[2]) return n";

/// A script that is busy on one tag does not hold up a script on another,
/// nor a client's command on another; a client's command on its own tag
/// waits for it, and sees what it wrote.
#[test]
fn scripts_on_two_tags_run_at_once_and_a_command_waits_for_its_own() {
    let server = Server::start(&["--lua-parallel-scripts", "true"]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", COUNTS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    assert!(c.cmd(&["SET", "{a}:k", "before"]).ok());
    assert!(c.cmd(&["SET", "{b}:k", "before"]).ok());
    // What the reads below read: a key of the other tag that nothing writes,
    // so that they all read the same whichever comes first.
    assert!(c.cmd(&["SET", "{b}:still", "still"]).ok());
    // Seen to be quick, on both tags, so that it is run beside others.
    for _ in 0..4 {
        assert_eq!(
            c.cmd(&["EVALSHA", &sha, "1", "{a}:k", "1", "before"]),
            V::Int(1)
        );
        assert_eq!(
            c.cmd(&["EVALSHA", &sha, "1", "{b}:k", "1", "before"]),
            V::Int(1)
        );
    }

    // How many steps make the slow script last about a second here. A fixed
    // count did on some machines and not others: a fast CI runner finished
    // 60 million in 325 ms, too soon to tell waiting from not waiting, and
    // the guard below failed the test (#50). It is measured with a copy of
    // the script under another digest, on a tag of its own: a script the
    // server has seen to be slow runs alone from then on, and the one timed
    // here must not be the one the test needs run beside others.
    let steps = {
        const MEASURE: u64 = 5_000_000;
        let copy = format!("{COUNTS} -- timed once to size the slow run");
        let timed = Instant::now();
        let reply = c.cmd(&["EVAL", &copy, "1", "{c}:k", &MEASURE.to_string(), "x"]);
        assert_eq!(reply, V::Int(MEASURE as i64));
        let per_step = timed.elapsed().as_secs_f64() / MEASURE as f64;
        // Never fewer than the 60 million this used to run.
        ((1.0 / per_step) as u64).clamp(60_000_000, 2_000_000_000)
    };

    let port = server.port;
    let started = Instant::now();
    let slow_sha = sha.clone();
    let slow_steps = steps.to_string();
    let slow = std::thread::spawn(move || {
        let mut c = Conn::connect(port);
        let reply = c.cmd(&["EVALSHA", &slow_sha, "1", "{a}:k", &slow_steps, "after"]);
        (reply, started.elapsed())
    });
    std::thread::sleep(Duration::from_millis(100));

    let probe = |args: Vec<String>| {
        std::thread::spawn(move || {
            let mut c = Conn::connect(port);
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let reply = c.cmd(&args);
            (reply, started.elapsed())
        })
    };
    let words = |words: &[&str]| words.iter().map(|w| w.to_string()).collect::<Vec<_>>();
    // Several connections for each thing that must not wait: the slow script
    // is run by the thread its connection is on, and a connection that
    // lands on that same thread waits for the thread, whatever its key. One
    // of several that is answered early shows that the key did not wait.
    let several = |args: Vec<String>| (0..6).map(|_| probe(args.clone())).collect::<Vec<_>>();
    let other_script = several(words(&["EVALSHA", &sha, "1", "{b}:k", "1", "other"]));
    let other_read = several(words(&["GET", "{b}:still"]));
    let untagged = several(words(&["SET", "plain", "v"]));
    let same_tag = probe(words(&["GET", "{a}:k"]));
    let earliest = |probes: Vec<std::thread::JoinHandle<(V, Duration)>>| {
        let done: Vec<(V, Duration)> = probes.into_iter().map(|p| p.join().unwrap()).collect();
        let reply = done[0].0.clone();
        assert!(done.iter().all(|(r, _)| *r == reply), "{done:?}");
        (reply, done.iter().map(|(_, at)| *at).min().unwrap())
    };

    let (slow_reply, slow_done) = slow.join().unwrap();
    assert_eq!(slow_reply, V::Int(steps as i64));
    assert!(
        slow_done > Duration::from_millis(400),
        "the slow script took only {slow_done:?} for {steps} steps, sized to take about a \
         second: the test proves nothing on this machine"
    );

    let margin = Duration::from_millis(100);
    let (reply, done) = earliest(other_script);
    assert_eq!(reply, V::Int(1));
    assert!(
        done + margin < slow_done,
        "a script on another tag waited: {done:?} of {slow_done:?}"
    );
    let (reply, done) = earliest(other_read);
    assert!(matches!(reply, V::Str(_)), "{reply:?}");
    assert!(
        done + margin < slow_done,
        "a read on another tag waited: {done:?} of {slow_done:?}"
    );
    let (reply, done) = earliest(untagged);
    assert!(reply.ok());
    assert!(
        done + margin < slow_done,
        "a key with no tag waited: {done:?} of {slow_done:?}"
    );

    // Its own tag: not before the script was done, and with what it wrote.
    let (reply, done) = same_tag.join().unwrap();
    assert_eq!(reply, V::Str("after".into()));
    assert!(
        done + Duration::from_millis(20) >= slow_done,
        "a read of the script's own tag did not wait: {done:?} of {slow_done:?}"
    );
}

// ── SORT patterns in a script held to a tag (#415 review) ────────────────────

impl Conn {
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

    /// What `key` holds, as clients see it: a hash's fields, sorted, each
    /// with its value and whether it has a time to live (1) or none (-1); a
    /// list's elements; or the type.
    fn state(&mut self, key: &str) -> String {
        match self.cmd(&["TYPE", key]).s() {
            "hash" => {
                let V::Arr(all) = self.cmd(&["HGETALL", key]) else {
                    panic!("HGETALL {key} did not answer with an array")
                };
                let mut fields: Vec<(String, String, i64)> = all
                    .chunks(2)
                    .map(|pair| {
                        let field = pair[0].s().to_string();
                        let ttl = match self.cmd(&["HPTTL", key, "FIELDS", "1", &field]) {
                            V::Arr(ttls) => match ttls[..] {
                                [V::Int(ttl)] => ttl.signum(),
                                _ => panic!("HPTTL {key} {field}: {ttls:?}"),
                            },
                            other => panic!("HPTTL {key} {field}: {other:?}"),
                        };
                        (field, pair[1].s().to_string(), ttl)
                    })
                    .collect();
                fields.sort();
                format!("hash {fields:?}")
            }
            "list" => format!("list {:?}", self.cmd(&["LRANGE", key, "0", "-1"])),
            other => other.to_string(),
        }
    }
}

fn wait_until(what: &str, secs: u64, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn count(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|w| *w == needle)
        .count()
}

/// The replica has applied everything its master has sent.
fn wait_caught_up(master: &mut Conn, replica: &mut Conn) {
    wait_until("the replica to catch up", 60, || {
        let offset = master.info("replication", "master_repl_offset");
        replica.info("replication", "slave_repl_offset") == offset
            && master.info("replication", "master_repl_offset") == offset
    });
}

/// Runs its `ARGV` as one command, with `KEYS[1]` as the key it names.
const CALLS_ARGV: &str = "return redis.call(unpack(ARGV))";

/// A script held to the tag `q` may not `SORT` or `SORT_RO` by a pattern
/// that can name keys outside `q`: such a key is one it holds no lock of,
/// and a field of it past its time would be removed by the `SORT` with a
/// client's write to it able to fall between the removal and its `HDEL`
/// (the command gate a `SORT` takes alone is already the script's). It is
/// refused before anything is read: nothing is removed, nothing sent. A
/// pattern that keeps to the tag, or names no key, is let through, as is the
/// same `SORT` from a client or with the setting off. The replica and a
/// replay of the AOF end as the master does.
#[test]
fn a_tagged_script_may_not_sort_by_a_pattern_outside_its_tag() {
    let mut master = Server::start(&[
        "--lua-parallel-scripts",
        "true",
        "--aof",
        "--appendfsync",
        "always",
        "--enable-debug-command",
        "yes",
    ]);
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_until("the replica's link to come up", 30, || {
        r.info("replication", "master_link_status") == "up"
    });

    // Each write finds its field itself: the master's sweep held off.
    assert!(m.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).ok());
    assert_eq!(m.cmd(&["RPUSH", "{q}:ids", "1", "2", "3"]), V::Int(3));
    let hashes = [
        "obj:1",
        "obj:2",
        "obj:3",
        "{q}:obj:1",
        "{q}:obj:2",
        "{q}:obj:3",
    ];
    for key in hashes {
        let w = &key[key.len() - 1..];
        assert_eq!(m.cmd(&["HSET", key, "w", w, "keep", "1"]), V::Int(2));
    }
    assert!(m.cmd(&["SET", "w_1", "1"]).ok());
    for key in ["obj:2", "{q}:obj:2"] {
        assert_eq!(
            m.cmd(&["HPEXPIRE", key, "100", "FIELDS", "1", "w"]),
            V::Arr(vec![V::Int(1)])
        );
    }
    std::thread::sleep(Duration::from_millis(300));
    fn script<'a>(args: &[&'a str]) -> Vec<&'a str> {
        let mut eval = vec!["EVAL", CALLS_ARGV, "1", "{q}:ids"];
        eval.extend_from_slice(args);
        eval
    }

    // Refused, every run: the first runs and the later ones (seen to be
    // quick, beside others) take different roads.
    let offset = m.info("replication", "master_repl_offset");
    let aof = master.aof().len();
    let refused: &[(&[&str], &str, &str)] = &[
        (&["SORT", "{q}:ids", "BY", "obj:*->w"], "sort", "obj:*->w"),
        (&["SORT", "{q}:ids", "GET", "obj:*->w"], "sort", "obj:*->w"),
        (
            &[
                "SORT", "{q}:ids", "BY", "nosort", "GET", "#", "GET", "obj:*->w", "STORE",
                "{q}:dst",
            ],
            "sort",
            "obj:*->w",
        ),
        (
            &["SORT_RO", "{q}:ids", "BY", "obj:*->w"],
            "sort_ro",
            "obj:*->w",
        ),
        (
            &["SORT_RO", "{q}:ids", "GET", "#", "GET", "obj:*->w"],
            "sort_ro",
            "obj:*->w",
        ),
        // Options and the command in any case.
        (
            &["sort", "{q}:ids", "bY", "obj:*->w", "Get", "#"],
            "sort",
            "obj:*->w",
        ),
        (
            &["sort_ro", "{q}:ids", "limit", "0", "3", "gEt", "obj:*->w"],
            "sort_ro",
            "obj:*->w",
        ),
        // Another tag's hashes, a tag the element would have to close, and
        // keys of strings: all outside `q`.
        (
            &["SORT", "{q}:ids", "BY", "{r}:obj:*->w"],
            "sort",
            "{r}:obj:*->w",
        ),
        (&["SORT", "{q}:ids", "BY", "{q*->w"], "sort", "{q*->w"),
        (&["SORT", "{q}:ids", "BY", "w_*"], "sort", "w_*"),
        (&["SORT_RO", "{q}:ids", "GET", "w_*"], "sort_ro", "w_*"),
    ];
    for run in 0..8 {
        for (args, name, pattern) in refused {
            let reply = m.cmd(&script(args));
            assert!(reply.is_err(), "run {run}: {args:?} ran: {reply:?}");
            let message = reply.s();
            assert!(
                message.contains(&format!(
                    "Script attempted to run '{name}' with the pattern '{pattern}', which looks \
                     at keys it does not name outside the hash tag 'q' of its KEYS"
                )),
                "run {run}: {args:?}: {message}"
            );
        }
        // A read-only script, too.
        let mut eval_ro = script(&["SORT_RO", "{q}:ids", "BY", "obj:*->w"]);
        eval_ro[0] = "EVAL_RO";
        let reply = m.cmd(&eval_ro);
        assert!(
            reply.is_err() && reply.s().contains("outside the hash tag 'q'"),
            "run {run}: {reply:?}"
        );
    }
    // Nothing was removed, nothing sent, nothing stored.
    assert_eq!(m.info("replication", "master_repl_offset"), offset);
    assert_eq!(master.aof().len(), aof);
    assert_eq!(m.cmd(&["EXISTS", "{q}:dst"]), V::Int(0));

    // Let through: no pattern, `#`, a pattern without `*`, patterns kept to
    // `q`. Its own `{q}:obj:2`'s `w` is past its time and goes with them.
    let sorted = |c: &mut Conn, args: &[&str]| match c.cmd(args) {
        V::Arr(items) => items,
        other => panic!("{args:?}: {other:?}"),
    };
    let s = |items: &[&str]| {
        items
            .iter()
            .map(|i| V::Str(i.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        sorted(&mut m, &script(&["SORT", "{q}:ids"])),
        s(&["1", "2", "3"])
    );
    assert_eq!(
        sorted(
            &mut m,
            &script(&["SORT_RO", "{q}:ids", "BY", "nosort", "GET", "#"])
        ),
        s(&["1", "2", "3"])
    );
    assert_eq!(
        sorted(
            &mut m,
            &script(&["sort", "{q}:ids", "by", "obj->w", "DESC"])
        ),
        s(&["3", "2", "1"])
    );
    assert_eq!(
        m.cmd(&script(&[
            "SORT",
            "{q}:ids",
            "BY",
            "{q}:obj:*->w",
            "GET",
            "#",
            "GET",
            "{q}:obj:*->w",
            "STORE",
            "{q}:dst",
        ])),
        V::Int(6)
    );
    assert_eq!(
        sorted(
            &mut m,
            &script(&[
                "SORT_RO",
                "{q}:ids",
                "bY",
                "{q}:obj:*->w",
                "gEt",
                "{q}:obj:*->w"
            ])
        ),
        vec![V::Nil, V::Str("1".into()), V::Str("3".into())]
    );

    // From a client, as before: the `SORT` takes `obj:2`'s `w` as gone, and
    // its `HDEL` goes ahead of the write after it.
    assert_eq!(
        sorted(
            &mut m,
            &["SORT", "{q}:ids", "BY", "obj:*->w", "GET", "obj:*->w"]
        ),
        vec![V::Nil, V::Str("1".into()), V::Str("3".into())]
    );
    assert_eq!(
        sorted(&mut m, &["SORT_RO", "{q}:ids", "GET", "obj:*->w"]),
        vec![V::Str("1".into()), V::Nil, V::Str("3".into())]
    );
    assert_eq!(m.cmd(&["HINCRBY", "obj:2", "w", "5"]), V::Int(5));

    // And with the setting off, from a script as from a client.
    assert!(m.cmd(&["CONFIG", "SET", "lua-parallel-scripts", "no"]).ok());
    assert_eq!(
        sorted(
            &mut m,
            &script(&["SORT", "{q}:ids", "BY", "obj:*->w", "GET", "w_*"])
        ),
        vec![V::Str("1".into()), V::Nil, V::Nil]
    );
    assert!(
        m.cmd(&["CONFIG", "SET", "lua-parallel-scripts", "yes"])
            .ok()
    );
    assert!(m.cmd(&["SET", "last", "1"]).ok());
    assert!(m.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).ok());

    let keys = [
        "obj:1",
        "obj:2",
        "obj:3",
        "{q}:obj:1",
        "{q}:obj:2",
        "{q}:obj:3",
        "{q}:dst",
        "w_1",
    ];
    let before: Vec<String> = keys.iter().map(|key| m.state(key)).collect();
    assert_eq!(before[1], r#"hash [("keep", "1", -1), ("w", "5", -1)]"#);
    assert_eq!(before[4], r#"hash [("keep", "1", -1)]"#);
    assert_eq!(
        before[6],
        format!("list {:?}", V::Arr(s(&["2", "", "1", "1", "3", "3"])))
    );
    wait_caught_up(&mut m, &mut r);
    let on_replica: Vec<String> = keys.iter().map(|key| r.state(key)).collect();
    assert_eq!(on_replica, before, "replica: {keys:?}");

    wait_until("the writes to reach the AOF", 10, || {
        count(&master.aof(), b"$4\r\nlast\r\n") == 1
    });
    master.kill_and_restart();
    let mut m = master.conn();
    let replayed: Vec<String> = keys.iter().map(|key| m.state(key)).collect();
    assert_eq!(replayed, before, "AOF replay: {keys:?}");
}

/// A script that runs beside others may not create the first search index
/// (#570): other commands run meanwhile, and the first index waits until
/// every write already under way is done, which a script holding its keys'
/// order cannot. Once an index exists, nothing has to be waited for, and it
/// may.
#[test]
fn a_script_beside_others_may_not_create_the_first_index() {
    const CREATES: &str = "if ARGV[1] == '' then return redis.call('SET', KEYS[1], 'v') end \
                           return redis.call('FT.CREATE', ARGV[1], 'ON', 'HASH', \
                           'PREFIX', '1', '{q}:', 'SCHEMA', 'f', 'TEXT')";
    let server = Server::start(&["--lua-parallel-scripts", "true"]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", CREATES]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    // Seen to be quick, so that it is run beside others.
    for _ in 0..4 {
        assert!(c.cmd(&["EVALSHA", &sha, "1", "{q}:k", ""]).ok());
    }
    let refused = c.cmd(&["EVALSHA", &sha, "1", "{q}:k", "first"]);
    assert!(
        refused.is_err() && refused.s().contains("first search index"),
        "{refused:?}"
    );
    assert_eq!(
        c.cmd(&["FT._LIST"]),
        V::Arr(Vec::new()),
        "an index was made"
    );

    assert!(c.cmd(&["FT.CREATE", "outside", "SCHEMA", "f", "TEXT"]).ok());
    let second = c.cmd(&["EVALSHA", &sha, "1", "{q}:k", "second"]);
    assert!(second.ok(), "{second:?}");
}

// ── INFO: how scripts were run (#871) ───────────────────────────────────────

/// The `scripts_*` counts of `INFO stats`.
fn script_counts(c: &mut Conn) -> std::collections::BTreeMap<String, u64> {
    c.cmd(&["INFO", "stats"])
        .s()
        .lines()
        .filter(|line| line.starts_with("scripts_"))
        .map(|line| {
            let (name, n) = line.trim().split_once(':').unwrap();
            (name.to_string(), n.parse().unwrap())
        })
        .collect()
}

/// The counts that `run` moved, and by how much.
fn moved(c: &mut Conn, run: impl FnOnce(&mut Conn)) -> Vec<(String, u64)> {
    let before = script_counts(c);
    run(c);
    let after = script_counts(c);
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    after
        .into_iter()
        .filter_map(|(name, n)| {
            let was = before[&name];
            assert!(n >= was, "{name} went down");
            (n > was).then(|| (name, n - was))
        })
        .collect()
}

fn one(names: &[&str]) -> Vec<(String, u64)> {
    let mut all: Vec<(String, u64)> = names.iter().map(|n| (n.to_string(), 1)).collect();
    all.sort();
    all
}

/// `INFO stats` says how scripts were run: beside others, or alone and why,
/// and which were handed to the script-runner thread or offloaded to the
/// blocking pool. Each count moves in the case it names, and in no other;
/// a command that is not a script moves none; `CONFIG RESETSTAT` sets them
/// all to 0.
#[test]
fn info_counts_how_scripts_were_run() {
    let server = Server::start(&["--lua-parallel-scripts", "true"]);
    let mut c = server.conn();
    let counts = script_counts(&mut c);
    assert_eq!(
        counts.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "scripts_alone_runs",
            "scripts_handed_to_runner",
            "scripts_in_alone_runs",
            "scripts_offloaded_to_pool",
            "scripts_run_alone_function",
            "scripts_run_alone_in_transaction",
            "scripts_run_alone_no_keys",
            "scripts_run_alone_no_tag",
            "scripts_run_alone_not_quick",
            "scripts_run_alone_parallel_off",
            "scripts_run_beside_others",
        ]
    );
    assert!(counts.values().all(|n| *n == 0), "{counts:?}");
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", READS_A_KEY]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    // Not scripts.
    assert_eq!(
        moved(&mut c, |c| {
            assert!(c.cmd(&["SET", "{q}:a", "v"]).ok());
            assert!(c.cmd(&["GET", "{q}:a"]).s() == "v");
        }),
        one(&[])
    );

    // Tagged, not yet seen to be quick: alone, on the pool.
    let tagged = |c: &mut Conn| {
        assert_eq!(
            c.cmd(&["EVALSHA", &sha, "1", "{q}:a", "{q}:a"]),
            V::Str("v".into())
        )
    };
    assert_eq!(
        moved(&mut c, tagged),
        one(&["scripts_offloaded_to_pool", "scripts_run_alone_not_quick"])
    );
    // Seen to be quick: beside others, on the connection's own thread.
    let mut runs = 0;
    loop {
        let counted = moved(&mut c, tagged);
        if counted == one(&["scripts_run_beside_others"]) {
            break;
        }
        assert_eq!(
            counted,
            one(&["scripts_offloaded_to_pool", "scripts_run_alone_not_quick"])
        );
        runs += 1;
        assert!(runs < 20, "the script was never run beside others");
    }

    // No keys: alone (its first run on the pool, as every script's).
    let no_keys = |c: &mut Conn| assert_eq!(c.cmd(&["EVAL", "return 7", "0"]), V::Int(7));
    assert_eq!(
        moved(&mut c, no_keys),
        one(&["scripts_offloaded_to_pool", "scripts_run_alone_no_keys"])
    );
    let mut runs = 0;
    // Inline, it runs alone in a run of its own (`command_gate::for_a_run`):
    // the gate closed once, and one script under it.
    while moved(&mut c, no_keys)
        != one(&[
            "scripts_alone_runs",
            "scripts_in_alone_runs",
            "scripts_run_alone_no_keys",
        ])
    {
        runs += 1;
        assert!(runs < 20, "the script was never run inline");
    }
    // A key with no tag, and keys of two tags: alone.
    assert_eq!(
        moved(&mut c, |c| {
            c.cmd(&["EVALSHA", &sha, "1", "plain", "{q}:a"]);
        }),
        one(&[
            "scripts_alone_runs",
            "scripts_in_alone_runs",
            "scripts_run_alone_no_tag"
        ])
    );
    assert_eq!(
        moved(&mut c, |c| {
            c.cmd(&["EVALSHA", &sha, "2", "{q}:a", "{r}:a", "{r}:a"]);
        }),
        one(&["scripts_run_beside_others"])
    );
    // Inside a transaction: alone.
    assert_eq!(
        moved(&mut c, |c| {
            assert!(c.cmd(&["MULTI"]).ok());
            assert_eq!(
                c.cmd(&["EVALSHA", &sha, "1", "{q}:a", "{q}:a"]).s(),
                "QUEUED"
            );
            assert!(matches!(c.cmd(&["EXEC"]), V::Arr(_)));
        }),
        one(&["scripts_run_alone_in_transaction"])
    );
    // A function: alone. Loading one is no script run.
    assert_eq!(
        moved(&mut c, |c| {
            let loaded = c.cmd(&[
                "FUNCTION",
                "LOAD",
                "#!lua name=counts\nredis.register_function('f', function(keys, args) \
                 return 1 end)",
            ]);
            assert_eq!(loaded, V::Str("counts".into()));
        }),
        one(&[])
    );
    assert_eq!(
        moved(&mut c, |c| assert_eq!(
            c.cmd(&["FCALL", "f", "1", "{q}:a"]),
            V::Int(1)
        )),
        one(&["scripts_offloaded_to_pool", "scripts_run_alone_function"])
    );
    // Switched off: alone, whatever its keys.
    assert!(c.cmd(&["CONFIG", "SET", "lua-parallel-scripts", "no"]).ok());
    assert_eq!(
        moved(&mut c, tagged),
        one(&[
            "scripts_alone_runs",
            "scripts_in_alone_runs",
            "scripts_run_alone_parallel_off"
        ])
    );
    assert!(
        c.cmd(&["CONFIG", "SET", "lua-parallel-scripts", "yes"])
            .ok()
    );

    // A quick script that comes while another is running is handed to the
    // runner thread: many at once, from several connections, until one is.
    let port = server.port;
    let deadline = Instant::now() + Duration::from_secs(60);
    let before = script_counts(&mut c);
    let mut sent = 0;
    while script_counts(&mut c)["scripts_handed_to_runner"] == 0 {
        assert!(
            Instant::now() < deadline,
            "no script was handed to the runner"
        );
        let clients: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(move || {
                    let mut c = Conn::connect(port);
                    for _ in 0..500 {
                        assert_eq!(c.cmd(&["EVAL", "return 7", "0"]), V::Int(7));
                    }
                })
            })
            .collect();
        for client in clients {
            client.join().unwrap();
        }
        sent += 8 * 500;
    }
    // Each of them counted once, as what it is, and nothing else.
    let after = script_counts(&mut c);
    for (name, n) in &after {
        let was = before[name];
        match name.as_str() {
            "scripts_run_alone_no_keys" => assert_eq!(n - was, sent, "{name}"),
            "scripts_handed_to_runner" => assert!(*n > 0 && n - was <= sent, "{name}"),
            // Those run inline or handed on run in runs, a close of the gate
            // for one or more of them; not those offloaded.
            "scripts_in_alone_runs" => assert!(*n > was && n - was <= sent, "{name}"),
            "scripts_alone_runs" => assert!(
                *n > was
                    && n - was <= after["scripts_in_alone_runs"] - before["scripts_in_alone_runs"],
                "{name}"
            ),
            // A run stretched past the demotion limit on a busy machine is
            // offloaded until it is seen to be quick again.
            "scripts_offloaded_to_pool" => assert!(n - was <= sent, "{name}"),
            _ => assert_eq!(*n, was, "{name}"),
        }
    }

    assert!(c.cmd(&["CONFIG", "RESETSTAT"]).ok());
    let counts = script_counts(&mut c);
    assert!(counts.values().all(|n| *n == 0), "{counts:?}");
}

// ── Scripts of several hash tags (#871) ──────────────────────────────────────

/// Counts to ARGV[1], then writes ARGV[2] to every key of `KEYS`.
const COUNTS_ALL: &str = "local n = 0 for i = 1, tonumber(ARGV[1]) do n = n + 1 end \
                          for _, key in ipairs(KEYS) do redis.call('SET', key, ARGV[2]) end \
                          return n";

/// A script whose `KEYS` carry two tags runs beside others once it has been
/// seen to be quick (#871): a script or a command on a third tag, or on a key
/// with no tag, does not wait for it; a command on either of its tags does,
/// and sees what it wrote; so does a script on one of them.
#[test]
fn a_script_of_two_tags_runs_beside_others_and_holds_both() {
    let server = Server::start(&["--lua-parallel-scripts", "true"]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", COUNTS_ALL]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    // The probes' scripts are a copy under another digest: the slow run
    // makes its own digest one that is offloaded from then on, and the
    // probes must not be sent to the pool for that.
    let probe_script = format!("{COUNTS_ALL} -- the probes");
    let V::Str(probe_sha) = c.cmd(&["SCRIPT", "LOAD", &probe_script]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    for key in ["{a}:k", "{b}:k", "{c}:k"] {
        assert!(c.cmd(&["SET", key, "before"]).ok());
    }
    assert!(c.cmd(&["SET", "{c}:still", "still"]).ok());
    // Sized to last about a second, as in the test of one tag above, with a
    // copy of the script on a tag of its own.
    let steps = {
        const MEASURE: u64 = 5_000_000;
        let copy = format!("{COUNTS_ALL} -- timed once to size the slow run");
        let timed = Instant::now();
        let reply = c.cmd(&["EVAL", &copy, "1", "{z}:k", &MEASURE.to_string(), "x"]);
        assert_eq!(reply, V::Int(MEASURE as i64));
        let per_step = timed.elapsed().as_secs_f64() / MEASURE as f64;
        ((1.0 / per_step) as u64).clamp(60_000_000, 2_000_000_000)
    };

    // On a busy machine a quick run can be stretched past the demotion limit
    // and the script sent to the pool, where it runs alone and everything
    // waits for it. An attempt in which any script went to the pool, or the
    // machine was too slow to send the waiting commands while the script
    // ran, proves nothing, and is made again.
    let mut attempt = 0;
    loop {
        attempt += 1;
        // Seen to be quick, so that it is run beside others.
        for _ in 0..4 {
            assert_eq!(
                c.cmd(&["EVALSHA", &sha, "2", "{a}:k", "{b}:k", "1", "before"]),
                V::Int(1)
            );
            assert_eq!(
                c.cmd(&["EVALSHA", &probe_sha, "1", "{c}:k", "1", "before"]),
                V::Int(1)
            );
            assert_eq!(
                c.cmd(&["EVALSHA", &probe_sha, "1", "{b}:k2", "1", "before"]),
                V::Int(1)
            );
        }
        let pooled_before = script_counts(&mut c)["scripts_offloaded_to_pool"];

        let port = server.port;
        let started = Instant::now();
        let slow_sha = sha.clone();
        let slow_steps = steps.to_string();
        let slow = std::thread::spawn(move || {
            let mut c = Conn::connect(port);
            let reply = c.cmd(&[
                "EVALSHA",
                &slow_sha,
                "2",
                "{a}:k",
                "{b}:k",
                &slow_steps,
                "after",
            ]);
            (reply, started.elapsed())
        });
        std::thread::sleep(Duration::from_millis(100));

        let probe = |args: Vec<String>| {
            std::thread::spawn(move || {
                let mut c = Conn::connect(port);
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                let reply = c.cmd(&args);
                (reply, started.elapsed())
            })
        };
        let words = |words: &[&str]| words.iter().map(|w| w.to_string()).collect::<Vec<_>>();
        // Several connections for what must not wait: one that lands on the
        // slow script's thread waits for the thread, whatever its key.
        let several = |args: Vec<String>| (0..6).map(|_| probe(args.clone())).collect::<Vec<_>>();
        let earliest = |probes: Vec<std::thread::JoinHandle<(V, Duration)>>| {
            let done: Vec<(V, Duration)> = probes.into_iter().map(|p| p.join().unwrap()).collect();
            let reply = done[0].0.clone();
            assert!(done.iter().all(|(r, _)| *r == reply), "{done:?}");
            (reply, done.iter().map(|(_, at)| *at).min().unwrap())
        };
        let other_script = several(words(&["EVALSHA", &probe_sha, "1", "{c}:k", "1", "other"]));
        let other_read = several(words(&["GET", "{c}:still"]));
        let untagged = several(words(&["SET", "plain", "v"]));
        let other_script = earliest(other_script);
        let other_read = earliest(other_read);
        let untagged = earliest(untagged);
        // Then, while it still runs, what must wait for it. Sent only now:
        // a command waiting for a tag's lock waits on its connection's
        // thread, and with three of them on the other three threads of the
        // server, nothing else could be answered until the script ended.
        let sent = started.elapsed();
        let first_tag = probe(words(&["GET", "{a}:k"]));
        let second_tag = probe(words(&["GET", "{b}:k"]));
        let both = probe(words(&["MGET", "{b}:k", "{a}:k"]));
        // On another key of its second tag, so that the reads above find what
        // the slow script wrote whichever of them goes first after it.
        let script_on_second = probe(words(&["EVALSHA", &probe_sha, "1", "{b}:k2", "1", "later"]));

        let (slow_reply, slow_done) = slow.join().unwrap();
        let first_tag = first_tag.join().unwrap();
        let second_tag = second_tag.join().unwrap();
        let both = both.join().unwrap();
        let script_on_second = script_on_second.join().unwrap();
        let pooled = script_counts(&mut c)["scripts_offloaded_to_pool"] - pooled_before;
        if pooled > 0 && attempt < 5 {
            continue;
        }
        assert_eq!(pooled, 0, "a script went to the pool in every attempt");

        assert_eq!(slow_reply, V::Int(steps as i64));
        assert!(
            slow_done > Duration::from_millis(400),
            "the slow script took only {slow_done:?} for {steps} steps, sized to take about a \
             second: the test proves nothing on this machine"
        );

        let margin = Duration::from_millis(100);
        let (reply, done) = other_script;
        assert_eq!(reply, V::Int(1));
        assert!(
            done + margin < slow_done,
            "a script on a third tag waited: {done:?} of {slow_done:?}"
        );
        let (reply, done) = other_read;
        assert_eq!(reply, V::Str("still".into()));
        assert!(
            done + margin < slow_done,
            "a read on a third tag waited: {done:?} of {slow_done:?}"
        );
        let (reply, done) = untagged;
        assert!(reply.ok());
        assert!(
            done + margin < slow_done,
            "a key with no tag waited: {done:?} of {slow_done:?}"
        );

        // And the commands that must wait were sent well before it ended.
        let late = sent + Duration::from_millis(200) > slow_done;
        if late && attempt < 5 {
            continue;
        }
        assert!(
            !late,
            "the waiting commands were sent too late in every attempt"
        );

        // Either of its tags: not before the script was done, and with what
        // it wrote.
        let waited = |what: &str, (reply, done): (V, Duration), want: V| {
            assert_eq!(reply, want, "{what}");
            assert!(
                done + Duration::from_millis(20) >= slow_done,
                "{what} did not wait: {done:?} of {slow_done:?}"
            );
        };
        waited("a read of its first tag", first_tag, V::Str("after".into()));
        waited(
            "a read of its second tag",
            second_tag,
            V::Str("after".into()),
        );
        waited(
            "a read of both",
            both,
            V::Arr(vec![V::Str("after".into()), V::Str("after".into())]),
        );
        waited("a script on its second tag", script_on_second, V::Int(1));
        assert_eq!(c.cmd(&["GET", "{b}:k2"]), V::Str("later".into()));
        break;
    }
}

/// Moves one unit from the first tag's account to the second's (with one
/// key, none) and logs the
/// move on both, under a key built from each declared key (`..':log'`), with
/// what it read: an order the replica or the AOF applied differently from
/// the master would show in the logs and the balances.
const TRANSFERS: &str = "\
local a = tonumber(redis.call('GET', KEYS[1]) or '0') \
local b = 0 \
if #KEYS > 1 then \
  b = tonumber(redis.call('GET', KEYS[2]) or '0') \
  redis.call('SET', KEYS[1], a - 1) \
  redis.call('SET', KEYS[2], b + 1) \
else \
  redis.call('SET', KEYS[1], a) \
end \
for i, key in ipairs(KEYS) do \
  redis.call('RPUSH', key .. ':log', ARGV[1] .. '@' .. a .. '/' .. b .. '#' .. \
    (redis.call('GET', key .. ':x') or '')) \
end \
return a";

/// Scripts of two tags, overlapping, from many clients at once, and a
/// client's `MSET` of keys of two tags among them, which each script reads
/// and logs: the master, its replica and the master's AOF replayed end with
/// the same data, key by key. Each script holds its tags for the whole of its
/// run, and the `MSET` takes the same tags' locks, so the order in which
/// their effects reach the stream is one in which the master could have run
/// them one after another. On both I/O loops.
#[test]
fn scripts_of_overlapping_tags_end_the_same_on_master_replica_and_aof() {
    overlapping_tags_end_the_same("tokio");
}

#[test]
fn scripts_of_overlapping_tags_end_the_same_on_master_replica_and_aof_uring() {
    overlapping_tags_end_the_same("uring");
}

fn overlapping_tags_end_the_same(io: &str) {
    let mut master = Server::start(&[
        "--io-model",
        io,
        "--lua-parallel-scripts",
        "true",
        "--aof",
        "--appendfsync",
        "always",
    ]);
    let port = master.port.to_string();
    let replica = Server::start(&["--io-model", io, "--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_until("the replica's link to come up", 30, || {
        r.info("replication", "master_link_status") == "up"
    });
    let V::Str(sha) = m.cmd(&["SCRIPT", "LOAD", TRANSFERS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };

    const TAGS: usize = 5;
    let key = |tag: usize| format!("{{acct{tag}}}:balance");
    let mp = master.port;
    // A client's write of two tags at once, read by the scripts.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut c = Conn::connect(mp);
            let mut n = 0usize;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) || n < 200 {
                let (i, j) = (n % TAGS, (n * 3 + 1) % TAGS);
                let (ki, kj) = (format!("{}:x", key(i)), format!("{}:x", key(j)));
                let v = format!("m{n}");
                assert!(c.cmd(&["MSET", &ki, &v, &kj, &v]).ok());
                n += 1;
            }
            n
        })
    };
    let clients: Vec<_> = (0..8)
        .map(|client| {
            let sha = sha.clone();
            std::thread::spawn(move || {
                let mut c = Conn::connect(mp);
                let mut ran = 0;
                for i in 0..400 {
                    // Every pair of tags, both ways round, and now and then
                    // one tag alone or three.
                    let from = (client + i) % TAGS;
                    let to = (client * 3 + i * 7 + 1) % TAGS;
                    let op = format!("c{client}.{i}");
                    let keys: Vec<String> = match i % 10 {
                        0 => vec![key(from)],
                        1 => vec![key(from), key((from + 1) % TAGS), key((from + 2) % TAGS)],
                        _ if from == to => vec![key(from)],
                        _ => vec![key(from), key(to)],
                    };
                    let mut args = vec!["EVALSHA".to_string(), sha.clone(), keys.len().to_string()];
                    args.extend(keys);
                    args.push(op);
                    let args: Vec<&str> = args.iter().map(String::as_str).collect();
                    let reply = c.cmd(&args);
                    assert!(matches!(reply, V::Int(_)), "{reply:?}");
                    ran += 1;
                }
                ran
            })
        })
        .collect();
    let ran: usize = clients.into_iter().map(|c| c.join().unwrap()).sum();
    assert_eq!(ran, 8 * 400);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(writer.join().unwrap() >= 200);
    assert!(m.cmd(&["SET", "last", "1"]).ok());

    let keys: Vec<String> = (0..TAGS)
        .flat_map(|tag| {
            [
                key(tag),
                format!("{}:log", key(tag)),
                format!("{}:x", key(tag)),
            ]
        })
        .collect();
    let dump = |c: &mut Conn| -> Vec<V> {
        keys.iter()
            .map(|k| match c.cmd(&["TYPE", k]).s() {
                "list" => c.cmd(&["LRANGE", k, "0", "-1"]),
                _ => c.cmd(&["GET", k]),
            })
            .collect()
    };
    let before = dump(&mut m);
    // Every unit moved is somewhere: the balances add up to 0.
    let total: i64 = before[..]
        .iter()
        .step_by(3)
        .map(|v| v.s().parse::<i64>().unwrap())
        .sum();
    assert_eq!(total, 0);
    wait_caught_up(&mut m, &mut r);
    let on_replica = dump(&mut r);
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(on_replica[i], before[i], "replica: {k}");
    }

    wait_until("the writes to reach the AOF", 10, || {
        count(&master.aof(), b"$4\r\nlast\r\n") == 1
    });
    master.kill_and_restart();
    let mut m = master.conn();
    let replayed = dump(&mut m);
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(replayed[i], before[i], "AOF replay: {k}");
    }
}

// ── Plain commands beside scripts that run alone (#880) ──────────────────────

impl Conn {
    /// `calls`, sent at once, and their replies, in order.
    fn pipeline(&mut self, calls: &[Vec<String>]) -> Vec<V> {
        let mut buf = Vec::new();
        for args in calls {
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(a.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        self.writer.write_all(&buf).unwrap();
        calls.iter().map(|_| self.read()).collect()
    }
}

/// Untagged keys: the script runs alone, and every other command waits for it.
/// It writes three keys, one `redis.call` at a time.
const THREE_KEYS: &str = "local n = redis.call('INCR', KEYS[1]) \
     redis.call('SET', KEYS[2], n) redis.call('SET', KEYS[3], n) return n";

/// `connections` connections, each sending `EVALSHA` of `sha` on `keys` in
/// pipelines of 16 until `stop`: the scripts they ran.
fn script_stream(
    port: u16,
    sha: &str,
    keys: &[&str],
    connections: usize,
    stop: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Vec<std::thread::JoinHandle<usize>> {
    (0..connections)
        .map(|_| {
            let stop = stop.clone();
            let mut call = vec![
                "EVALSHA".to_string(),
                sha.to_string(),
                keys.len().to_string(),
            ];
            call.extend(keys.iter().map(|k| k.to_string()));
            std::thread::spawn(move || {
                let mut c = Conn::connect(port);
                let calls = vec![call; 16];
                let mut ran = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    for reply in c.pipeline(&calls) {
                        assert!(matches!(reply, V::Int(_)), "{reply:?}");
                        ran += 1;
                    }
                }
                ran
            })
        })
        .collect()
}

/// While connections send script after script that runs alone (its key has no
/// tag), a plain command on another connection is answered within a bound
/// each time, and the scripts go on running meanwhile: neither keeps the
/// other out. On both I/O loops.
#[test]
fn a_plain_command_is_answered_beside_a_stream_of_alone_scripts() {
    plain_beside_alone_scripts("tokio");
}

#[test]
fn a_plain_command_is_answered_beside_a_stream_of_alone_scripts_uring() {
    plain_beside_alone_scripts("uring");
}

fn plain_beside_alone_scripts(io: &str) {
    let server = Server::start(&["--io-model", io]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", THREE_KEYS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    assert!(c.cmd(&["SET", "plain", "v"]).ok());
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let streams = script_stream(server.port, &sha, &["n", "n:a", "n:b"], 8, &stop);
    // Under way, and quick: handed from one thread to another.
    wait_until("the scripts to run", 30, || {
        c.info("stats", "scripts_run_alone_no_tag")
            .parse::<u64>()
            .unwrap()
            > 2000
    });
    let before: u64 = c.info("stats", "scripts_run_alone_no_tag").parse().unwrap();
    let mut probe = server.conn();
    let mut worst = Duration::ZERO;
    let mut waits = Vec::new();
    for _ in 0..2000 {
        let t = Instant::now();
        assert_eq!(probe.cmd(&["GET", "plain"]), V::Str("v".into()));
        let took = t.elapsed();
        worst = worst.max(took);
        waits.push(took);
    }
    let after: u64 = c.info("stats", "scripts_run_alone_no_tag").parse().unwrap();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let ran: usize = streams.into_iter().map(|s| s.join().unwrap()).sum();
    waits.sort();
    let p99 = waits[waits.len() * 99 / 100];
    eprintln!(
        "{io}: GET beside alone scripts: p50 {:?}, p99 {p99:?}, worst {worst:?}; {} scripts \
         meanwhile, {ran} in all",
        waits[waits.len() / 2],
        after - before
    );
    // A debug build on a loaded test machine: bounds far above a wait for a
    // few scripts, far below a command kept out until the stream ends.
    assert!(
        worst < Duration::from_secs(2) && p99 < Duration::from_millis(250),
        "a GET waited {worst:?} (p99 {p99:?}) beside the scripts"
    );
    assert!(
        after > before + 1000,
        "the scripts stopped while the GETs ran: {before} -> {after}"
    );
    assert!(ran > 2000);
    // They ran in runs: more scripts than closes of the gate, a close for
    // more than one of them.
    let counts = script_counts(&mut c);
    let (closes, in_runs) = (
        counts["scripts_alone_runs"],
        counts["scripts_in_alone_runs"],
    );
    eprintln!("{io}: {in_runs} scripts under {closes} closes of the gate");
    assert!(
        closes > 0 && in_runs > closes,
        "{in_runs} scripts, {closes} closes"
    );
}

/// A script that runs alone is never seen half done, however many of them
/// run back to back from many connections: each `MGET` of its three keys
/// finds them all equal (none of them written yet, or all by the same run).
/// On both I/O loops.
#[test]
fn a_reader_never_sees_an_alone_script_half_done() {
    alone_script_never_half_done("tokio");
}

#[test]
fn a_reader_never_sees_an_alone_script_half_done_uring() {
    alone_script_never_half_done("uring");
}

fn alone_script_never_half_done(io: &str) {
    let server = Server::start(&["--io-model", io]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", THREE_KEYS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let streams = script_stream(server.port, &sha, &["k", "k:a", "k:b"], 6, &stop);
    let port = server.port;
    let readers: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(move || {
                let mut r = Conn::connect(port);
                let (mut reads, mut last) = (0u64, 0i64);
                while reads < 20_000 {
                    let V::Arr(values) = r.cmd(&["MGET", "k", "k:a", "k:b"]) else {
                        panic!("MGET did not answer with an array")
                    };
                    let seen: Vec<Option<i64>> = values
                        .iter()
                        .map(|v| match v {
                            V::Nil => None,
                            v => Some(v.s().parse().unwrap()),
                        })
                        .collect();
                    assert!(
                        seen.iter().all(|v| *v == seen[0]),
                        "a script seen half done: {seen:?}"
                    );
                    let n = seen[0].unwrap_or(0);
                    assert!(n >= last, "went back from {last} to {n}");
                    last = n;
                    reads += 1;
                }
                last
            })
        })
        .collect();
    let lasts: Vec<i64> = readers.into_iter().map(|r| r.join().unwrap()).collect();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let ran: usize = streams.into_iter().map(|s| s.join().unwrap()).sum();
    let V::Str(k) = c.cmd(&["GET", "k"]) else {
        panic!("no k")
    };
    assert_eq!(k.parse::<usize>().unwrap(), ran);
    // The readers saw the scripts run, not just before or after all of them.
    assert!(
        lasts.iter().any(|&n| n > 0 && (n as usize) < ran),
        "{lasts:?} of {ran}"
    );
}

// ── A run of alone scripts, and what waits beside it (#880) ──────────────────

/// Writes ARGV[1] to KEYS[1].
const SETS_A_KEY: &str = "return redis.call('SET', KEYS[1], ARGV[1])";

/// A destination that takes connections, reads what it is sent, and never
/// answers: a `MIGRATE` to it waits for its timeout. Its port, and whether
/// anything has been sent to it: a `MIGRATE` sends its keys once it has
/// marked them as moving (`key_moves`), so from then on a write to them
/// waits.
fn silent_destination() -> (u16, std::sync::Arc<std::sync::atomic::AtomicBool>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let sent = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = sent.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let seen = seen.clone();
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                while matches!(stream.read(&mut buf), Ok(n) if n > 0) {
                    seen.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            });
        }
    });
    (port, sent)
}

/// Of `n` new connections to `port`, those that answer a `PING` within
/// `within`: on io_uring, those on a thread a `MIGRATE` waiting for its
/// destination does not hold (a `MIGRATE` waits on its own thread). The
/// others are left to be answered when it ends.
fn free_connections(port: u16, n: usize, within: Duration) -> Vec<Conn> {
    let (tx, rx) = std::sync::mpsc::channel();
    for _ in 0..n {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let mut c = Conn::connect(port);
            // Read by hand: one never answered is left when the server
            // stops, and must not panic then.
            let mut line = String::new();
            if c.writer.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok()
                && c.reader.read_line(&mut line).is_ok()
                && line == "+PONG\r\n"
            {
                let _ = tx.send(c);
            }
        });
    }
    let deadline = Instant::now() + within;
    let mut free = Vec::new();
    while let Ok(c) = rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        free.push(c);
        if free.len() == n {
            break;
        }
    }
    free
}

/// `MIGRATE` of `key` to a destination that never answers, on a connection
/// of its own, timing out after `ms`: its reply and how long it took, once
/// it has sent the key (so a write to it waits from then on).
fn migrate_to_nowhere(port: u16, key: &str, ms: u64) -> std::thread::JoinHandle<(V, Duration)> {
    let (destination, sent) = silent_destination();
    let key = key.to_string();
    let migrate = std::thread::spawn(move || {
        let mut m = Conn::connect(port);
        timed(|| {
            m.cmd(&[
                "MIGRATE",
                "127.0.0.1",
                &destination.to_string(),
                &key,
                "0",
                &ms.to_string(),
            ])
        })
    });
    wait_until("the MIGRATE to send its key", 30, || {
        sent.load(std::sync::atomic::Ordering::SeqCst)
    });
    migrate
}

/// What `run` gave, and how long it took.
fn timed<T>(run: impl FnOnce() -> T) -> (T, Duration) {
    let t = Instant::now();
    let out = run();
    (out, t.elapsed())
}

/// While a `MIGRATE` holds a key for its timeout, a quick script that runs
/// alone and declares that key waits for it, with the command gate open:
/// other clients' `PING` and `GET` of another key are answered at once, but
/// on the worker thread the script waits on. On both I/O loops.
#[test]
fn a_script_waiting_for_a_migrate_leaves_the_gate_open() {
    script_waiting_for_a_migrate("tokio");
}

#[test]
fn a_script_waiting_for_a_migrate_leaves_the_gate_open_uring() {
    script_waiting_for_a_migrate("uring");
}

fn script_waiting_for_a_migrate(io: &str) {
    let server = Server::start(&["--io-model", io]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", SETS_A_KEY]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    // Seen to be quick: run alone, inline, by whoever has the turn.
    for _ in 0..4 {
        assert!(c.cmd(&["EVALSHA", &sha, "1", "moving", "v0"]).ok());
    }
    assert!(c.cmd(&["SET", "other", "o"]).ok());
    let port = server.port;
    let migrate = migrate_to_nowhere(port, "moving", 4000);
    let writer = std::thread::spawn(move || {
        let mut b = Conn::connect(port);
        timed(|| b.cmd(&["EVALSHA", &sha, "1", "moving", "v1"]))
    });
    std::thread::sleep(Duration::from_millis(300));
    // Connections on every worker thread: each one's worst wait.
    let probes: Vec<_> = (0..16)
        .map(|_| {
            std::thread::spawn(move || {
                let mut probe = Conn::connect(port);
                let mut worst = Duration::ZERO;
                for _ in 0..20 {
                    let (pong, took) = timed(|| probe.cmd(&["PING"]));
                    assert_eq!(pong.s(), "PONG");
                    worst = worst.max(took);
                    let (got, took) = timed(|| probe.cmd(&["GET", "other"]));
                    assert_eq!(got, V::Str("o".into()));
                    worst = worst.max(took);
                }
                worst
            })
        })
        .collect();
    let worst: Vec<Duration> = probes.into_iter().map(|p| p.join().unwrap()).collect();
    let (written, waited) = writer.join().unwrap();
    let (migrated, migrate_took) = migrate.join().unwrap();
    eprintln!(
        "{io}: worst waits beside the waiting script {worst:?}; it waited {waited:?}; \
         MIGRATE took {migrate_took:?}: {migrated:?}"
    );
    // With the gate closed, every one of them would have waited as long as
    // the script did. Those on the thread the script waits on, or on the
    // one the MIGRATE waits on, do.
    let quick = worst
        .iter()
        .filter(|w| **w < Duration::from_millis(1000))
        .count();
    assert!(
        quick >= 3,
        "commands waited {worst:?} while a script waited for the MIGRATE"
    );
    // It did wait for the MIGRATE, which gave up, and then ran here.
    assert!(migrated.is_err(), "{migrated:?}");
    assert!(written.ok(), "{written:?}");
    assert!(waited > Duration::from_millis(2000), "{waited:?}");
    assert_eq!(c.cmd(&["GET", "moving"]), V::Str("v1".into()));
}

/// While other connections keep the gate busy with commands that hold it for
/// milliseconds each (`LRANGE` of a long list), so that closing it for a run
/// takes longer than a run lasts, queued alone scripts are still all
/// answered, none of their pipelines waits long, and every one ran (the
/// count the scripts keep matches). On both I/O loops.
#[test]
fn alone_scripts_go_on_while_the_gate_is_slow_to_close() {
    alone_scripts_beside_long_commands("tokio");
}

#[test]
fn alone_scripts_go_on_while_the_gate_is_slow_to_close_uring() {
    alone_scripts_beside_long_commands("uring");
}

fn alone_scripts_beside_long_commands(io: &str) {
    let server = Server::start(&["--io-model", io]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", THREE_KEYS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    let elements: Vec<String> = (0..1000).map(|i| format!("element-{i}")).collect();
    for _ in 0..100 {
        let mut push = vec!["RPUSH", "long"];
        push.extend(elements.iter().map(String::as_str));
        assert!(matches!(c.cmd(&push), V::Int(_)));
    }
    let (_, lrange) = timed(|| c.cmd(&["LRANGE", "long", "0", "-1"]));
    eprintln!("{io}: one LRANGE of the list takes {lrange:?}");
    let before = script_counts(&mut c);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let port = server.port;
    let long: Vec<_> = (0..3)
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut l = Conn::connect(port);
                let mut n = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    assert!(matches!(l.cmd(&["LRANGE", "long", "0", "-1"]), V::Arr(_)));
                    n += 1;
                }
                n
            })
        })
        .collect();
    // Each connection's scripts, and its longest wait for the replies of
    // one pipeline of 16.
    let streams: Vec<_> = (0..6)
        .map(|_| {
            let (stop, sha) = (stop.clone(), sha.clone());
            std::thread::spawn(move || {
                let mut s = Conn::connect(port);
                let call: Vec<String> = ["EVALSHA", &sha, "3", "s", "s:a", "s:b"]
                    .iter()
                    .map(|a| a.to_string())
                    .collect();
                let calls = vec![call; 16];
                let (mut ran, mut worst) = (0usize, Duration::ZERO);
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let (replies, took) = timed(|| s.pipeline(&calls));
                    for reply in replies {
                        assert!(matches!(reply, V::Int(_)), "{reply:?}");
                        ran += 1;
                    }
                    worst = worst.max(took);
                }
                (ran, worst)
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(4));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let ranges: usize = long.into_iter().map(|l| l.join().unwrap()).sum();
    let streams: Vec<(usize, Duration)> = streams.into_iter().map(|s| s.join().unwrap()).collect();
    let ran: Vec<usize> = streams.iter().map(|(n, _)| *n).collect();
    let worst = streams.iter().map(|(_, w)| *w).max().unwrap();
    let after = script_counts(&mut c);
    let closes = after["scripts_alone_runs"] - before["scripts_alone_runs"];
    let in_runs = after["scripts_in_alone_runs"] - before["scripts_in_alone_runs"];
    eprintln!(
        "{io}: {ranges} LRANGEs; scripts per connection {ran:?}, longest wait for a \
         pipeline {worst:?}; {in_runs} takes under {closes} closes"
    );
    assert!(ranges > 3, "{ranges}");
    // Every reply came, an `Int` each, and no pipeline of 16 waited long:
    // the scripts went on beside the long commands.
    assert!(
        worst < Duration::from_secs(3),
        "a pipeline of scripts waited {worst:?}"
    );
    let total: usize = ran.iter().sum();
    let V::Str(n) = c.cmd(&["GET", "s"]) else {
        panic!("no s")
    };
    assert_eq!(n.parse::<usize>().unwrap(), total);
    // They ran in runs: the gate was closed for them, again and again.
    assert!(
        closes > 0 && in_runs > 0,
        "{in_runs} takes, {closes} closes"
    );
}

/// `SCRIPT KILL` of a slow script that has the turn, with quick scripts queued
/// behind it: the slow one is told it was killed, and the queued ones are
/// answered afterwards. On both I/O loops.
#[test]
fn script_kill_in_a_run_lets_the_queued_scripts_run() {
    kill_in_a_run("tokio");
}

#[test]
fn script_kill_in_a_run_lets_the_queued_scripts_run_uring() {
    kill_in_a_run("uring");
}

fn kill_in_a_run(io: &str) {
    let server = Server::start(&["--io-model", io]);
    let mut c = server.conn();
    // Counts to ARGV[1], and writes only at the end: killable while it counts.
    let counts = "local n = 0 for i = 1, tonumber(ARGV[1]) do n = n + 1 end \
                  redis.call('SET', KEYS[1], n) return n";
    let V::Str(slow) = c.cmd(&["SCRIPT", "LOAD", counts]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    let V::Str(quick) = c.cmd(&["SCRIPT", "LOAD", THREE_KEYS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    // Both seen to be quick, so that both run inline, taking turns.
    for _ in 0..4 {
        assert_eq!(c.cmd(&["EVALSHA", &slow, "1", "counted", "1"]), V::Int(1));
        assert!(matches!(
            c.cmd(&["EVALSHA", &quick, "3", "q", "q:a", "q:b"]),
            V::Int(_)
        ));
    }
    let was: i64 = c.cmd(&["GET", "q"]).s().parse().unwrap();
    let handed_before = script_counts(&mut c)["scripts_handed_to_runner"];
    let port = server.port;
    // Seen to be quick, so run inline with the turn, counting for far longer
    // than the test.
    let killed = std::thread::spawn(move || {
        let mut a = Conn::connect(port);
        a.cmd(&["EVALSHA", &slow, "1", "counted", "100000000000"])
    });
    std::thread::sleep(Duration::from_millis(200));
    let queued: Vec<_> = (0..6)
        .map(|_| {
            let quick = quick.clone();
            std::thread::spawn(move || {
                let mut q = Conn::connect(port);
                timed(|| q.cmd(&["EVALSHA", &quick, "3", "q", "q:a", "q:b"]))
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(500));
    // From several connections at once: one on the thread the slow script
    // holds is answered only once it has been killed (`NOTBUSY`).
    let kills: Vec<_> = (0..8)
        .map(|_| std::thread::spawn(move || Conn::connect(port).cmd(&["SCRIPT", "KILL"])))
        .collect();
    let kills: Vec<V> = kills.into_iter().map(|k| k.join().unwrap()).collect();
    assert!(kills.iter().any(V::ok), "{kills:?}");
    let reply = killed.join().unwrap();
    assert!(reply.is_err(), "{reply:?}");
    let mut values = Vec::new();
    for q in queued {
        let (reply, took) = q.join().unwrap();
        let V::Int(n) = reply else {
            panic!("a queued script: {reply:?}")
        };
        assert!(took < Duration::from_secs(15), "{took:?}");
        values.push(n);
    }
    values.sort();
    let expected: Vec<i64> = (was + 1..=was + 6).collect();
    assert_eq!(values, expected);
    let handed = script_counts(&mut c)["scripts_handed_to_runner"];
    eprintln!(
        "{io}: {} of the 6 queued behind the killed script",
        handed - handed_before
    );
    assert_eq!(c.cmd(&["PING"]).s(), "PONG");
    assert_eq!(c.cmd(&["GET", "q"]), V::Str((was + 6).to_string()));
}

/// A worker thread that has the turn runs the scripts queued meanwhile; one
/// of them that has to wait for a `MIGRATE` of its key must not wait there,
/// where the thread's other connections would wait with it: it is left to
/// the script-runner thread. A pipelines about 150 ms of quick scripts that
/// run alone, so that its thread has the turn nearly all the while and runs
/// what is queued after each of its own; B's script on the key the `MIGRATE` holds
/// comes meanwhile, from a connection on another thread (found beforehand
/// by a `FUNCTION STATS`, which a slow script of A's holds back on no other
/// thread), and is queued. The `MIGRATE` itself waits on its own
/// thread, so A, B and the probes are connections on the others
/// (`free_connections`), and the probes on B's own thread are left out.
/// On io_uring, where a connection stays on its thread, the probes that tell
/// are those on A's thread, the one that would have run B. While B waits out the `MIGRATE`, a `PING` and a
/// `GET` on each probe are answered at once. On both I/O loops.
#[test]
fn a_queued_script_waiting_for_a_migrate_does_not_hold_a_worker() {
    queued_script_waiting_for_a_migrate("tokio", false);
}

#[test]
fn a_queued_script_waiting_for_a_migrate_does_not_hold_a_worker_uring() {
    queued_script_waiting_for_a_migrate("uring", false);
}

/// The same with the queued scripts' connections looking for their replies
/// (`KIVIDB_TEST_LOOK`): while keys move, a connection that looks never
/// takes the turn to run what is queued, so B's script is the runner's.
#[test]
fn a_queued_script_waiting_for_a_migrate_is_not_run_by_a_connection_that_looks() {
    queued_script_waiting_for_a_migrate("tokio", true);
}

#[test]
fn a_queued_script_waiting_for_a_migrate_is_not_run_by_a_connection_that_looks_uring() {
    queued_script_waiting_for_a_migrate("uring", true);
}

fn queued_script_waiting_for_a_migrate(io: &str, look: bool) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let server = if look {
        Server::start(&["--io-model", io, LOOK])
    } else {
        Server::start(&["--io-model", io])
    };
    let mut c = server.conn();
    // Counts to ARGV[1], then writes KEYS[1]: quick at 1, slow at a million.
    let counts = "local n = 0 for i = 1, tonumber(ARGV[1]) do n = n + 1 end \
                  redis.call('SET', KEYS[1], n) return n";
    let V::Str(slow) = c.cmd(&["SCRIPT", "LOAD", counts]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    let V::Str(sets) = c.cmd(&["SCRIPT", "LOAD", SETS_A_KEY]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    let V::Str(quick) = c.cmd(&["SCRIPT", "LOAD", THREE_KEYS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    for _ in 0..4 {
        assert!(c.cmd(&["EVALSHA", &sets, "1", "m", "v0"]).ok());
        assert!(matches!(
            c.cmd(&["EVALSHA", &quick, "3", "q", "q:a", "q:b"]),
            V::Int(_)
        ));
    }
    // How many counts take about 400 ms here.
    let (_, took) = timed(|| c.cmd(&["EVALSHA", &slow, "1", "a", "1000000"]));
    let count = ((1_000_000.0 * 0.4 / took.as_secs_f64().max(0.0001)) as u64).max(1);
    // That run was slow, and sent the script back to the pool: quick again.
    for _ in 0..4 {
        assert_eq!(c.cmd(&["EVALSHA", &slow, "1", "a", "1"]), V::Int(1));
    }
    assert!(c.cmd(&["SET", "other", "o"]).ok());
    let port = server.port;
    let migrate = migrate_to_nowhere(port, "m", 4000);
    let mut free = free_connections(port, 40, Duration::from_secs(1));
    assert!(free.len() >= 12, "only {} connections answered", free.len());
    let probe_conns: Vec<Conn> = free.split_off(9);
    assert!(probe_conns.len() >= 3, "{} probes", probe_conns.len());
    let candidates: Vec<Conn> = free.split_off(1);
    let mut a = free.pop().unwrap();
    // B: a connection on another thread than A's. While A's slow script
    // runs (about 400 ms), the first of the candidates to answer a
    // `FUNCTION STATS` at once.
    let slow_a = slow.clone();
    let a_slow = std::thread::spawn(move || {
        let reply = a.cmd(&["EVALSHA", &slow_a, "1", "a", &count.to_string()]);
        (a, reply)
    });
    std::thread::sleep(Duration::from_millis(100));
    // Meanwhile, which probes are on A's thread: those A's script holds back.
    let probing: Vec<_> = probe_conns
        .into_iter()
        .map(|mut p| {
            std::thread::spawn(move || {
                let (_, took) = timed(|| p.cmd(&["FUNCTION", "STATS"]));
                (p, took >= Duration::from_millis(50))
            })
        })
        .collect();
    let chosen = std::sync::Arc::new(AtomicBool::new(false));
    let picked: Vec<Conn> = candidates
        .into_iter()
        .map(|mut b| {
            let chosen = chosen.clone();
            std::thread::spawn(move || {
                let (_, took) = timed(|| b.cmd(&["FUNCTION", "STATS"]));
                (took < Duration::from_millis(50)
                    && chosen
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok())
                .then_some(b)
            })
        })
        .filter_map(|t| t.join().unwrap())
        .collect();
    let probe_conns: Vec<(Conn, bool)> = probing.into_iter().map(|t| t.join().unwrap()).collect();
    let (mut a, reply) = a_slow.join().unwrap();
    assert!(matches!(reply, V::Int(_)), "{reply:?}");
    let [mut b] = <[Conn; 1]>::try_from(picked)
        .unwrap_or_else(|_| panic!("no connection answered FUNCTION STATS at once"));
    for _ in 0..4 {
        assert_eq!(b.cmd(&["EVALSHA", &slow, "1", "b", "1"]), V::Int(1));
    }
    // And the probes on B's thread are left out: should B find the turn free
    // when it comes, it runs on its own thread and waits there, as any
    // script that is not queued waits for a `MIGRATE` of its key. Found as
    // B's, by a slow script of B's.
    let slow_b = slow.clone();
    let b_slow = std::thread::spawn(move || {
        let reply = b.cmd(&["EVALSHA", &slow_b, "1", "b", &count.to_string()]);
        (b, reply)
    });
    std::thread::sleep(Duration::from_millis(100));
    let probe_conns: Vec<(Conn, bool)> = probe_conns
        .into_iter()
        .map(|(mut p, on_a)| {
            std::thread::spawn(move || {
                let (_, took) = timed(|| p.cmd(&["FUNCTION", "STATS"]));
                (took < Duration::from_millis(50)).then_some((p, on_a))
            })
        })
        .filter_map(|t| t.join().unwrap())
        .collect();
    let (mut b, reply) = b_slow.join().unwrap();
    assert!(matches!(reply, V::Int(_)), "{reply:?}");
    assert!(probe_conns.len() >= 2, "{} probes left", probe_conns.len());
    for _ in 0..4 {
        assert_eq!(a.cmd(&["EVALSHA", &slow, "1", "a", "1"]), V::Int(1));
    }
    // A sends quick scripts, one pipeline of them: its thread has the turn
    // nearly all the while, and runs what is queued after each of its own.
    // About 150 ms of them (measured first), so that they are over by the
    // time the probes ask: a thread serves the rest of one connection's
    // pipeline before its other connections.
    let call: Vec<String> = ["EVALSHA", &slow, "1", "a", "1"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let (_, took) = timed(|| a.pipeline(&vec![call.clone(); 1000]));
    let n = ((1000.0 * 0.15 / took.as_secs_f64().max(0.0001)) as usize).clamp(200, 20_000);
    let a_stream = std::thread::spawn(move || {
        let (replies, took) = timed(|| a.pipeline(&vec![call; n]));
        assert!(replies.iter().all(|r| *r == V::Int(1)));
        took
    });
    std::thread::sleep(Duration::from_millis(20));
    // B's script on `m` comes meanwhile, and is queued.
    let b = std::thread::spawn(move || timed(|| b.cmd(&["EVALSHA", &sets, "1", "m", "v1"])));
    std::thread::sleep(Duration::from_millis(500));
    // While B waits: connections on every thread but the MIGRATE's.
    let probes: Vec<_> = probe_conns
        .into_iter()
        .map(|(mut probe, on_a)| {
            std::thread::spawn(move || {
                let mut worst = Duration::ZERO;
                for _ in 0..10 {
                    let (pong, took) = timed(|| probe.cmd(&["PING"]));
                    assert_eq!(pong.s(), "PONG");
                    worst = worst.max(took);
                    let (got, took) = timed(|| probe.cmd(&["GET", "other"]));
                    assert_eq!(got, V::Str("o".into()));
                    worst = worst.max(took);
                }
                (worst, on_a)
            })
        })
        .collect();
    let worst: Vec<(Duration, bool)> = probes.into_iter().map(|p| p.join().unwrap()).collect();
    let (written, waited) = b.join().unwrap();
    let a_took = a_stream.join().unwrap();
    let (migrated, _) = migrate.join().unwrap();
    eprintln!(
        "{io}: probes' worst waits {worst:?}; B waited {waited:?}; A's {n} scripts took {a_took:?}"
    );
    // On io_uring a connection stays on its thread: A's thread is the one
    // that would have run B, and its probes are the ones that tell. With
    // Tokio a thread's connections move: every probe.
    let telling: Vec<Duration> = worst
        .iter()
        .filter(|(_, on_a)| io != "uring" || *on_a)
        .map(|(w, _)| *w)
        .collect();
    assert!(!telling.is_empty(), "no probe on A's thread: {worst:?}");
    assert!(
        telling.iter().all(|w| *w < Duration::from_millis(1000)),
        "connections waited {worst:?} (true: on A's thread) while a queued script waited out \
         the MIGRATE"
    );
    // B did wait for the MIGRATE, which gave up, and then ran.
    assert!(migrated.is_err(), "{migrated:?}");
    assert!(written.ok(), "{written:?}");
    assert!(waited > Duration::from_millis(2000), "{waited:?}");
    assert_eq!(c.cmd(&["GET", "m"]), V::Str("v1".into()));
}

// ── A queued script's connection looks for its reply (#900) ──────────────────

/// Untagged: runs alone, and with several connections at it most calls are
/// queued behind another one's.
const APPEND: &str = "return redis.call('RPUSH', KEYS[1], ARGV[1])";

/// Untagged scripts appending to one list from 8 connections, pipelined, on
/// 2 workers (so the queued scripts' connections look for their replies
/// where the machine has CPUs to spare): each connection's appends are in
/// the list in the order it sent them, every reply is the list's length
/// after its own append, and the list is the same on the master, its
/// replica and after the AOF is replayed. On both I/O loops.
#[test]
fn queued_scripts_append_in_order_on_master_replica_and_aof() {
    queued_appends_end_the_same("tokio");
}

#[test]
fn queued_scripts_append_in_order_on_master_replica_and_aof_uring() {
    queued_appends_end_the_same("uring");
}

fn queued_appends_end_the_same(io: &str) {
    let mut master = Server::start(&[
        "--io-model",
        io,
        "--threads",
        "2",
        "--aof",
        "--appendfsync",
        "always",
        LOOK,
    ]);
    let port = master.port.to_string();
    let replica = Server::start(&["--io-model", io, "--replicaof", "127.0.0.1", &port]);
    let (mut m, mut r) = (master.conn(), replica.conn());
    wait_until("the replica's link to come up", 30, || {
        r.info("replication", "master_link_status") == "up"
    });
    let V::Str(sha) = m.cmd(&["SCRIPT", "LOAD", APPEND]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    const CONNS: usize = 8;
    const CALLS: usize = 600;
    let mp = master.port;
    let clients: Vec<_> = (0..CONNS)
        .map(|client| {
            let sha = sha.clone();
            std::thread::spawn(move || {
                let mut c = Conn::connect(mp);
                let mut lengths = Vec::new();
                for batch in 0..CALLS / 20 {
                    let calls: Vec<Vec<String>> = (0..20)
                        .map(|i| {
                            let n = batch * 20 + i;
                            vec![
                                "EVALSHA".into(),
                                sha.clone(),
                                "1".into(),
                                "log".into(),
                                format!("c{client}.{n}"),
                            ]
                        })
                        .collect();
                    for reply in c.pipeline(&calls) {
                        let V::Int(len) = reply else {
                            panic!("{reply:?}")
                        };
                        lengths.push(len);
                    }
                }
                lengths
            })
        })
        .collect();
    let mut all_lengths: Vec<i64> = Vec::new();
    for c in clients {
        let lengths = c.join().unwrap();
        // A connection's appends land in the order it sent them.
        assert!(lengths.windows(2).all(|w| w[0] < w[1]), "{lengths:?}");
        all_lengths.extend(lengths);
    }
    // Every reply is the length after its own append: each one once.
    all_lengths.sort_unstable();
    let want: Vec<i64> = (1..=(CONNS * CALLS) as i64).collect();
    assert_eq!(all_lengths, want);
    let V::Arr(on_master) = m.cmd(&["LRANGE", "log", "0", "-1"]) else {
        panic!("LRANGE")
    };
    assert_eq!(on_master.len(), CONNS * CALLS);
    for client in 0..CONNS {
        let prefix = format!("c{client}.");
        let mine: Vec<usize> = on_master
            .iter()
            .filter_map(|v| v.s().strip_prefix(&prefix)?.parse().ok())
            .collect();
        assert_eq!(mine, (0..CALLS).collect::<Vec<_>>(), "connection {client}");
    }
    assert!(m.cmd(&["SET", "last", "1"]).ok());
    wait_caught_up(&mut m, &mut r);
    let V::Arr(on_replica) = r.cmd(&["LRANGE", "log", "0", "-1"]) else {
        panic!("LRANGE")
    };
    assert_eq!(on_replica, on_master, "replica");
    wait_until("the writes to reach the AOF", 10, || {
        count(&master.aof(), b"$4\r\nlast\r\n") == 1
    });
    master.kill_and_restart();
    let mut m = master.conn();
    let V::Arr(replayed) = m.cmd(&["LRANGE", "log", "0", "-1"]) else {
        panic!("LRANGE")
    };
    assert_eq!(replayed, on_master, "AOF replay");
}

/// A connection that goes while its script is queued: its script still runs
/// (it was read before the connection went), and the other connections'
/// scripts are all answered. On both I/O loops.
#[test]
fn a_connection_gone_while_its_script_is_queued_leaves_the_others_answered() {
    gone_while_queued("tokio");
}

#[test]
fn a_connection_gone_while_its_script_is_queued_leaves_the_others_answered_uring() {
    gone_while_queued("uring");
}

fn gone_while_queued(io: &str) {
    let server = Server::start(&["--io-model", io, "--threads", "2", LOOK]);
    let mut c = server.conn();
    let V::Str(sha) = c.cmd(&["SCRIPT", "LOAD", APPEND]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    // Seen to be quick, so the next ones run on the workers and queue there.
    assert!(matches!(
        c.cmd(&["EVALSHA", &sha, "1", "warm", "x"]),
        V::Int(1)
    ));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let V::Str(three) = c.cmd(&["SCRIPT", "LOAD", THREE_KEYS]) else {
        panic!("SCRIPT LOAD did not answer with a digest")
    };
    assert!(matches!(
        c.cmd(&["EVALSHA", &three, "3", "b1", "b2", "b3"]),
        V::Int(1)
    ));
    let streams = script_stream(server.port, &three, &["b1", "b2", "b3"], 4, &stop);
    // Connections that each send one append and go without reading.
    const GONE: usize = 40;
    for i in 0..GONE {
        let mut g = server.conn();
        let v = format!("g{i}");
        let call = format!(
            "*5\r\n$7\r\nEVALSHA\r\n${}\r\n{}\r\n$1\r\n1\r\n$4\r\ngone\r\n${}\r\n{}\r\n",
            sha.len(),
            sha,
            v.len(),
            v
        );
        g.writer.write_all(call.as_bytes()).unwrap();
        // Read by the server before the connection goes.
        std::thread::sleep(Duration::from_millis(2));
        drop(g);
    }
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let ran: usize = streams.into_iter().map(|s| s.join().unwrap()).sum();
    assert!(ran > 0);
    wait_until("every gone connection's append", 10, || {
        c.cmd(&["LLEN", "gone"]) == V::Int(GONE as i64)
    });
    // Every script of the others ran once, and was answered.
    assert_eq!(c.cmd(&["GET", "b1"]).s(), (ran + 1).to_string());
}
