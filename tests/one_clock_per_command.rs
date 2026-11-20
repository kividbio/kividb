// One command, one reading of the clock (#632): a key is live for the whole
// of a command or expired for the whole of it, as Redis's
// `commandTimeSnapshot` has it.
//
// A write on a master looks for the keys it names that are past their time,
// deletes them and sends their `UNLINK` ahead of itself
// (`handler::expire_named_keys_first`), then runs. Everything after that
// look goes by the time it read (`script_clock::command`): what the write
// reads is what the look found, so the write sent on to the replica and the
// AOF, where a key goes only when the master's `UNLINK` says so, is applied
// there to the same keys.
//
// The first test opens the window on purpose: a debug build started with
// `KIVIDB_TEST_CLOCK_AHEAD_MS` moves the clock that far ahead, right after
// the look, in every write naming a `clock-ahead:` key
// (`pause::test_move_clock_after_the_expiry_check`). A source given a
// minute to live is then past its time by the clock, and must still be
// read live by the write. The second runs the writes with sources a
// millisecond or three from their deadline, many times, and compares the
// replica's dataset with the master's.
//
// Each test runs its own servers, as processes. The I/O model is the
// platform's own unless `KIVIDB_TEST_IO_MODEL` says.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod common;
use common::{free_port, restart_verified};

/// How far a write naming a `clock-ahead:` key finds the clock moved once
/// it has looked for the keys past their time: ten minutes.
const AHEAD_MS: &str = "600000";
/// The time to live of a source in the first test: past by the moved
/// clock, far off by the real one.
#[cfg_attr(not(debug_assertions), allow(dead_code))]
const MINUTE_MS: &str = "60000";

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
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn encode(args: &[&str]) -> Vec<u8> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.stream.write_all(&Self::encode(args)).unwrap();
        self.read()
    }

    /// Several commands in one write, and their replies.
    fn pipeline(&mut self, commands: &[Vec<String>]) -> Vec<V> {
        let mut buf = Vec::new();
        for args in commands {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            buf.extend_from_slice(&Self::encode(&args));
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
            Some(b'_') => V::Nil,
            _ => panic!("unexpected RESP: {line:?}"),
        }
    }

    fn ok(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args), V::Simple("OK".into()), "{args:?}");
    }

    fn get(&mut self, key: &str) -> Option<String> {
        self.cmd(&["GET", key]).text()
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
        let (dir, extra) = (self.dir.clone(), self.extra.clone());
        // Accepted only when the server answering on the port is the process
        // started (it can be another test's, started on the port while it was
        // free); if not, started again on another port. See tests/common/mod.rs.
        let (child, port, _) = restart_verified(
            |port| {
                Command::new(env!("CARGO_BIN_EXE_kividb"))
                    .current_dir(&dir)
                    .env("KIVIDB_TEST_CLOCK_AHEAD_MS", AHEAD_MS)
                    .args([
                        "--port",
                        &port.to_string(),
                        "--bind",
                        "127.0.0.1",
                        "--threads",
                        "4",
                        "--enable-debug-command",
                        "yes",
                        "--configfile",
                        "kividb.conf",
                    ])
                    .args(io_model.iter().flat_map(|m| ["--io-model", m.as_str()]))
                    .args(&extra)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(std::fs::File::create(dir.join("server.log")).unwrap())
                    .spawn()
                    .expect("could not start kividb")
            },
            self.port,
        );
        self.child = Some(child);
        self.port = port;
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
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
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

/// The replica has applied everything its master sent before this.
fn caught_up(master: &mut Conn, replica: &mut Conn) {
    let mark = format!("{}", now_ms());
    master.ok(&["SET", "caught-up", &mark]);
    wait_for("the replica to catch up", || {
        replica.get("caught-up").as_deref() == Some(mark.as_str())
    });
}

/// `DEBUG DIGEST-VALUE` of `key`: never an error, which two nodes would
/// agree on whatever they held.
fn digest(c: &mut Conn, key: &str) -> String {
    let reply = c.cmd(&["DEBUG", "DIGEST-VALUE", key]);
    let V::Array(digest) = &reply else {
        panic!("DEBUG DIGEST-VALUE {key}: {reply:?}");
    };
    assert!(
        matches!(digest.as_slice(), [V::Bulk(_)]),
        "DEBUG DIGEST-VALUE {key}: {reply:?}"
    );
    digest[0].text().unwrap()
}

/// Every key, the digest of its value and when it expires: what two nodes
/// holding the same data agree on, whatever order a set's members were
/// put in.
fn dataset(c: &mut Conn) -> Vec<(String, String, i64)> {
    let reply = c.cmd(&["KEYS", "*"]);
    let V::Array(keys) = reply else {
        panic!("KEYS: {reply:?}");
    };
    let mut out: Vec<_> = keys
        .iter()
        .map(|k| k.text().unwrap())
        .filter(|k| k != "caught-up")
        .map(|k| {
            let digest = digest(c, &k);
            let at = c.cmd(&["PEXPIRETIME", &k]).int();
            (k, digest, at)
        })
        .collect();
    out.sort();
    out
}

/// What a key holds, for a message.
fn shown(c: &mut Conn, key: &str) -> String {
    let kind = c.cmd(&["TYPE", key]).text().unwrap_or_default();
    let value = match kind.as_str() {
        "string" => c.cmd(&["GET", key]),
        "list" => c.cmd(&["LRANGE", key, "0", "-1"]),
        "set" => c.cmd(&["SMEMBERS", key]),
        "zset" => c.cmd(&["ZRANGE", key, "0", "-1", "WITHSCORES"]),
        "hash" => c.cmd(&["HGETALL", key]),
        _ => V::Nil,
    };
    format!("{kind} {value:?}")
}

/// A master with an AOF and its replica, in step.
struct Pair {
    master: Server,
    replica: Server,
}

impl Pair {
    fn new() -> Self {
        let master = Server::start(&["--aof"]);
        let replica = Server::replica_of(&master);
        Pair { master, replica }
    }

    /// The master and the replica hold the same dataset, which `after`
    /// left them with.
    fn same(&self, m: &mut Conn, r: &mut Conn, after: &str) {
        caught_up(m, r);
        let (on_master, on_replica) = (dataset(m), dataset(r));
        if on_master != on_replica {
            let keys: std::collections::BTreeSet<String> = on_master
                .iter()
                .chain(&on_replica)
                .map(|(k, _, _)| k.clone())
                .collect();
            let diff: Vec<String> = keys
                .iter()
                .filter(|k| {
                    on_master.iter().find(|(m, _, _)| m == *k)
                        != on_replica.iter().find(|(r, _, _)| r == *k)
                })
                .map(|k| format!("{k}: master {} / replica {}", shown(m, k), shown(r, k)))
                .collect();
            panic!(
                "after {after} the replica differs from its master:\n{}",
                diff.join("\n")
            );
        }
    }

    /// The master restarted, replaying its AOF, holds what it held.
    fn replays_as_it_ran(&mut self) {
        let before = dataset(&mut self.master.conn());
        self.master.restart();
        let after = dataset(&mut self.master.conn());
        assert_eq!(
            after, before,
            "the AOF replayed differs from what the master held"
        );
    }
}

#[cfg_attr(not(debug_assertions), allow(dead_code))]
fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| a.to_string()).collect()
}

/// The `*STORE` writes and the others whose effect lands on another key,
/// each reading a source with a minute to live, while the clock is moved
/// ten minutes ahead between the look for keys past their time and the
/// write's own lookups. The write goes by the time of the look: the source
/// is live to it, as it is to the replica and the AOF. Debug builds only:
/// the clock is moved by a test hook.
#[cfg(debug_assertions)]
#[test]
fn a_write_reads_its_sources_by_the_time_it_looked_at_them() {
    let mut pair = Pair::new();
    let (mut m, mut r) = (pair.master.conn(), pair.replica.conn());

    // (setup, the source given a minute to live, the write, its reply as it
    // is with the source live).
    type Case = (Vec<Vec<&'static str>>, &'static str, Vec<&'static str>, V);
    let cases: Vec<Case> = vec![
        (
            vec![vec!["SADD", "a", "x", "y"], vec!["SADD", "b", "z"]],
            "a",
            vec!["SUNIONSTORE", "clock-ahead:d", "a", "b"],
            V::Int(3),
        ),
        (
            vec![vec!["SADD", "ia", "x", "y"], vec!["SADD", "ib", "x", "y"]],
            "ia",
            vec!["SINTERSTORE", "clock-ahead:id", "ia", "ib"],
            V::Int(2),
        ),
        (
            vec![
                vec!["ZADD", "za", "1", "x", "2", "y"],
                vec!["ZADD", "zb", "3", "z"],
            ],
            "za",
            vec!["ZUNIONSTORE", "clock-ahead:zd", "2", "za", "zb"],
            V::Int(3),
        ),
        (
            vec![vec!["ZADD", "zi", "1", "x"], vec!["ZADD", "zj", "2", "x"]],
            "zi",
            vec!["ZINTERSTORE", "clock-ahead:zk", "2", "zi", "zj"],
            V::Int(1),
        ),
        (
            vec![vec!["SET", "ba", "\u{7f}"], vec!["SET", "bb", "\u{1}"]],
            "ba",
            vec!["BITOP", "OR", "clock-ahead:bd", "ba", "bb"],
            V::Int(1),
        ),
        (
            vec![vec!["PFADD", "ha", "x", "y"], vec!["PFADD", "hb", "z"]],
            "ha",
            vec!["PFMERGE", "clock-ahead:hd", "ha", "hb"],
            V::Simple("OK".into()),
        ),
        (
            vec![vec!["RPUSH", "l", "3", "1", "2"]],
            "l",
            vec!["SORT", "l", "STORE", "clock-ahead:sd"],
            V::Int(3),
        ),
        (
            vec![vec!["GEOADD", "g", "13.361389", "38.115556", "Palermo"]],
            "g",
            vec![
                "GEOSEARCHSTORE",
                "clock-ahead:gd",
                "g",
                "FROMLONLAT",
                "13.361389",
                "38.115556",
                "BYRADIUS",
                "10",
                "km",
            ],
            V::Int(1),
        ),
        (
            vec![vec!["SET", "src", "v"]],
            "src",
            vec!["COPY", "src", "clock-ahead:dst"],
            V::Int(1),
        ),
        (
            vec![vec!["SET", "r1", "v"]],
            "r1",
            vec!["RENAME", "r1", "clock-ahead:r2"],
            V::Simple("OK".into()),
        ),
        (
            vec![vec!["RPUSH", "ls", "a", "b"]],
            "ls",
            vec!["LMOVE", "ls", "clock-ahead:ld", "LEFT", "RIGHT"],
            V::Bulk(b"a".to_vec()),
        ),
        (
            vec![
                vec!["SADD", "ms", "m", "n"],
                vec!["SADD", "clock-ahead:md", "o"],
            ],
            "ms",
            vec!["SMOVE", "ms", "clock-ahead:md", "m"],
            V::Int(1),
        ),
    ];
    for (setup, source, write, expected) in cases {
        for args in &setup {
            let reply = m.cmd(args);
            assert!(!matches!(reply, V::Error(_)), "{args:?}: {reply:?}");
        }
        assert_eq!(m.cmd(&["PEXPIRE", source, MINUTE_MS]), V::Int(1));
        let reply = m.cmd(&write);
        assert_eq!(reply, expected, "{write:?} did not find {source} live");
        // The clock is its own again for the next command: the source, if
        // the write left it, still has most of its minute.
        if write[0] != "RENAME" {
            assert!(
                m.cmd(&["PTTL", source]).int() > 1_000,
                "{source} after {write:?}"
            );
        }
        pair.same(&mut m, &mut r, &format!("{write:?}"));
    }

    // A transaction goes by the time of its `EXEC`.
    m.cmd(&["SADD", "ta", "x", "y"]);
    m.cmd(&["SADD", "tb", "z"]);
    assert_eq!(m.cmd(&["PEXPIRE", "ta", MINUTE_MS]), V::Int(1));
    let replies = m.pipeline(&[
        strings(&["MULTI"]),
        strings(&["SUNIONSTORE", "clock-ahead:td", "ta", "tb"]),
        strings(&["SCARD", "ta"]),
        strings(&["COPY", "ta", "clock-ahead:tc"]),
        strings(&["EXEC"]),
    ]);
    assert_eq!(
        replies[4],
        V::Array(vec![V::Int(3), V::Int(2), V::Int(1)]),
        "the EXEC found ta there throughout"
    );
    pair.same(&mut m, &mut r, "MULTI SUNIONSTORE SCARD COPY EXEC");

    drop((m, r));
    pair.replays_as_it_ran();
}

/// Sources a millisecond or three from their deadline, read by
/// `SUNIONSTORE`, `BITOP`, `COPY`, `LMOVE` and `SMOVE` on a master, many
/// times over. Whatever each found, the replica, which applies the master's
/// stream and never expires a key by its own clock, ends with the same
/// destinations, and so does the AOF replayed.
#[test]
fn writes_racing_their_sources_deadlines_leave_the_replica_as_the_master() {
    let mut pair = Pair::new();
    let (mut m, mut r) = (pair.master.conn(), pair.replica.conn());
    const ROUNDS: usize = 400;
    for i in 0..ROUNDS {
        let ttl = (1 + i % 3).to_string();
        let k = |name: &str| format!("{name}:{i}");
        let mut batch: Vec<Vec<String>> = vec![
            vec!["SADD".into(), k("a"), "x".into(), "y".into()],
            vec!["SADD".into(), k("b"), "z".into()],
            vec!["SET".into(), k("ba"), "\u{7f}".into()],
            vec!["SET".into(), k("bb"), "\u{1}".into()],
            vec!["SET".into(), k("src"), "v".into()],
            vec!["RPUSH".into(), k("ls"), "p".into(), "q".into()],
            vec!["SADD".into(), k("ms"), "m".into(), "n".into()],
            vec!["SADD".into(), k("md"), "o".into()],
        ];
        for source in ["a", "ba", "src", "ls", "ms"] {
            batch.push(vec!["PEXPIRE".into(), k(source), ttl.clone()]);
        }
        // A moment for the deadlines to come close: the writes then land
        // just before, on, or just after them.
        let spin = Instant::now();
        let replies = m.pipeline(&batch);
        for (args, reply) in batch.iter().zip(&replies) {
            assert!(!matches!(reply, V::Error(_)), "{args:?}: {reply:?}");
        }
        while spin.elapsed() < Duration::from_micros(500 * (i % 5) as u64) {
            std::hint::spin_loop();
        }
        let writes = vec![
            vec!["SUNIONSTORE".into(), k("d"), k("a"), k("b")],
            vec!["BITOP".into(), "OR".into(), k("bd"), k("ba"), k("bb")],
            vec!["COPY".into(), k("src"), k("cd")],
            vec![
                "LMOVE".into(),
                k("ls"),
                k("ld"),
                "LEFT".into(),
                "RIGHT".into(),
            ],
            vec!["SMOVE".into(), k("ms"), k("md"), "m".into()],
        ];
        for (args, reply) in writes.iter().zip(m.pipeline(&writes)) {
            assert!(!matches!(reply, V::Error(_)), "{args:?}: {reply:?}");
        }
    }
    // Every source's deadline has passed and the master has deleted it,
    // and sent the deletion: a `COPY`'s destination took its source's time.
    thread::sleep(Duration::from_millis(20));
    wait_for("every deadline to pass", || {
        let V::Array(keys) = m.cmd(&["KEYS", "*"]) else {
            panic!("KEYS");
        };
        let keys: Vec<String> = keys.iter().map(|k| k.text().unwrap()).collect();
        keys.iter().all(|k| m.cmd(&["PTTL", k]).int() == -1)
    });
    pair.same(&mut m, &mut r, "the racing writes");
    drop((m, r));
    pair.replays_as_it_ran();
}
