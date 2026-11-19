#![allow(clippy::disallowed_methods)]
// tests/ts_feed.rs
//
// Time series writes as the readers of the write stream see them: a live
// replica, the replica's own AOF replayed at its restart, and the master's
// AOF replayed at its restart. Real `kividb` processes, because a restart is
// one of the points of interest.
//
// Each write is sent with the time it replied, so every reader holds the
// samples the master holds. A `TS.INCRBY` of a 1.2.0 master, which a replica
// applies as 1.2.0 read it, is logged again as 1.2.0 wrote it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::{free_port, restart_verified, start_verified};

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

fn kividb(dir: &Path, port: u16, extra: &[String]) -> Child {
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
            "--threads",
            "4",
            "--configfile",
            "kividb.conf",
            "--enable-debug-command",
            "yes",
        ])
        .args(extra)
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
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        // Verified to be the process started: see tests/common/mod.rs.
        let (child, port) = start_verified(|port| kividb(&dir, port, &args));
        let server = Server {
            port,
            dir,
            child,
            args,
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

    /// Killed, with no chance to save anything more.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Started again on its data directory with `args`, and answering.
    fn restart_with(&mut self, args: &[&str]) {
        self.args = args.iter().map(|s| s.to_string()).collect();
        let (child, port, _) =
            restart_verified(|port| kividb(&self.dir, port, &self.args), self.port);
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
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ── A 1.2.0 master ────────────────────────────────────────────────────────────

/// What a new replica of an empty master is sent: a real full-sync payload.
fn empty_payload() -> Vec<u8> {
    let master = Server::start(&["--io-model", "tokio"]);
    let link = TcpStream::connect(("127.0.0.1", master.port)).unwrap();
    link.set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    (&link)
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let mut reader = BufReader::new(link);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("+FULLRESYNC"), "{line:?}");
    line.clear();
    reader.read_line(&mut line).unwrap();
    let len: usize = line.trim().trim_start_matches('$').parse().unwrap();
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).unwrap();
    payload
}

/// One RESP array of bulk strings from `reader`, its words upper-cased.
fn read_command(reader: &mut BufReader<TcpStream>) -> std::io::Result<Vec<String>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    let argc: usize = line.trim().trim_start_matches('*').parse().unwrap_or(0);
    let mut args = Vec::with_capacity(argc);
    for _ in 0..argc {
        line.clear();
        reader.read_line(&mut line)?;
        let len: usize = line.trim().trim_start_matches('$').parse().unwrap_or(0);
        let mut data = vec![0u8; len + 2];
        reader.read_exact(&mut data)?;
        data.truncate(len);
        args.push(String::from_utf8_lossy(&data).to_ascii_uppercase());
    }
    Ok(args)
}

/// A master that says it is KiviDB 1.2.0: it answers one replica's
/// handshake, sends `payload` and then `commands` as they are given, and
/// keeps the link open until the replica closes it. Returns its port.
fn master_1_2_0(payload: Vec<u8>, commands: &[&[&str]]) -> u16 {
    let port = free_port();
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    let info = "# Server\r\nredis_version:8.10.0\r\nkividb_version:1.2.0\r\n\
                uptime_in_seconds:100\r\nkividb_expired_keys_propagated:1\r\n";
    let stream: Vec<u8> = commands
        .iter()
        .flat_map(|command| {
            let mut encoded = format!("*{}\r\n", command.len()).into_bytes();
            for part in *command {
                encoded.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
            }
            encoded
        })
        .collect();
    std::thread::spawn(move || -> std::io::Result<()> {
        let (mut link, _) = listener.accept()?;
        let mut reader = BufReader::new(link.try_clone()?);
        loop {
            let args = read_command(&mut reader)?;
            match args.first().map(String::as_str) {
                Some("PING") => link.write_all(b"+PONG\r\n")?,
                Some("REPLCONF") | Some("AUTH") => link.write_all(b"+OK\r\n")?,
                Some("INFO") => {
                    link.write_all(format!("${}\r\n{info}\r\n", info.len()).as_bytes())?
                }
                Some("PSYNC") => break,
                _ => return Ok(()),
            }
        }
        link.write_all(b"+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n")?;
        link.write_all(format!("${}\r\n", payload.len()).as_bytes())?;
        link.write_all(&payload)?;
        link.write_all(&stream)?;
        // The replica's acknowledgements, until it goes.
        let mut sink = [0u8; 256];
        while reader.read(&mut sink)? > 0 {}
        Ok(())
    });
    port
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// The I/O models a test is run on: io_uring is Linux's.
fn io_models() -> &'static [&'static str] {
    if cfg!(target_os = "linux") {
        &["tokio", "uring"]
    } else {
        &["tokio"]
    }
}

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

/// `word` as a RESP bulk string, as a log or a stream holds an argument.
fn bulk(word: &str) -> Vec<u8> {
    format!("${}\r\n{word}\r\n", word.len()).into_bytes()
}

fn holds(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Everything a server holds, key by key: each series' samples and the
/// digest of its value, and how many keys there are.
fn dataset(c: &mut Conn) -> Vec<(String, V, V)> {
    let mut keys: Vec<String> = c
        .cmd(&["KEYS", "*"])
        .arr()
        .iter()
        .map(|k| k.s().to_string())
        .collect();
    keys.sort();
    let mut all: Vec<(String, V, V)> = keys
        .iter()
        .map(|key| {
            let samples = match c.cmd(&["TYPE", key]).s() {
                "TSDB-TYPE" => c.cmd(&["TS.RANGE", key, "-", "+"]),
                _ => V::Nil,
            };
            (key.clone(), samples, c.cmd(&["DEBUG", "DIGEST-VALUE", key]))
        })
        .collect();
    all.push(("DBSIZE".into(), c.cmd(&["DBSIZE"]), V::Nil));
    all
}

/// The `done` record in `server`'s AOF: everything before it is there too,
/// and is read by a restart after a kill.
fn wait_logged(server: &Server, what: &str) {
    wait_until(what, 30, || holds(&server.aof(), &bulk("done")));
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Every time series write with a time — given, `*`, none, and given in
/// the other forms a client may write a number in — is sent to replicas
/// and the AOF with the time it replied, as the plain number. The master,
/// its replica, the replica restarted from its AOF and the master restarted
/// from its AOF hold the same samples. `-0` is refused, as RedisTimeSeries
/// refuses it.
#[test]
fn ts_writes_feed_the_time_they_replied() {
    let max = i64::MAX.to_string();
    let writes: &[&[&str]] = &[
        &["TS.CREATE", "r", "RETENTION", "1000"],
        &["TS.CREATE", "m"],
        &["TS.ADD", "a", "1000", "1"],
        &["TS.ADD", "a", "*", "2"],
        &["TS.ADD", "a", "+2000", "3"],
        &["TS.ADD", "a", "0002500", "4"],
        &["TS.ADD", "a", "0", "5"],
        &["TS.ADD", "a", &max, "6"],
        &["TS.ADD", "r", "1", "1"],
        &["TS.ADD", "r", "+1500", "2"],
        &["TS.ADD", "r", "*", "3", "ON_DUPLICATE", "SUM"],
        &[
            "TS.MADD", "m", "*", "1", "m", "+3000", "2", "m", "003500", "3", "m", "4000", "4",
        ],
        &["TS.INCRBY", "c", "1"],
        &["TS.INCRBY", "c", "1", "TIMESTAMP", "*"],
        &["TS.INCRBY", "c", "1", "TIMESTAMP", "+4000000000000"],
        &[
            "TS.DECRBY",
            "c",
            "1",
            "TIMESTAMP",
            "04000000000001",
            "RETENTION",
            "5",
        ],
        &["TS.INCRBY", "d", "2", "TIMESTAMP", "+7", "LABELS", "x", "y"],
        &["TS.DECRBY", "d", "1", "TIMESTAMP", "0008"],
    ];
    // Given in a form other than the number replied: none of them is sent.
    let as_given = [
        "+2000",
        "0002500",
        "+1500",
        "+3000",
        "003500",
        "+4000000000000",
        "04000000000001",
        "+7",
        "0008",
        "-0",
    ];
    for io in io_models() {
        let mut master = Server::start(&["--aof", "--io-model", io]);
        let port = master.port.to_string();
        let mut replica =
            Server::start(&["--replicaof", "127.0.0.1", &port, "--aof", "--io-model", io]);
        let (mut m, mut r) = (master.conn(), replica.conn());
        wait_link_up(&mut r);

        for args in writes {
            let reply = m.cmd(args);
            assert!(!matches!(reply, V::Err(_)), "{io}: {args:?}: {reply:?}");
        }
        for args in [
            &["TS.ADD", "a", "-0", "9"][..],
            &["TS.INCRBY", "c", "1", "TIMESTAMP", "-0"],
            &["TS.DECRBY", "z", "1", "TIMESTAMP", "-0"],
        ] {
            let reply = m.cmd(args);
            assert!(matches!(reply, V::Err(_)), "{io}: {args:?}: {reply:?}");
        }
        let reply = m.cmd(&["TS.MADD", "m", "-0", "9", "m", "5000", "5"]);
        assert!(matches!(reply.arr()[0], V::Err(_)), "{io}: {reply:?}");
        assert_eq!(reply.arr()[1].i(), 5000, "{io}");
        assert_eq!(m.cmd(&["SET", "done", "1"]).s(), "OK");

        let want = dataset(&mut m);
        assert_eq!(m.cmd(&["EXISTS", "z"]).i(), 0, "{io}");
        wait_caught_up(&mut m, &mut r);
        assert_eq!(dataset(&mut r), want, "{io}: live replica");

        // Written to both logs as the number replied.
        assert!(m.cmd(&["WAITAOF", "1", "0", "5000"]).arr()[0].i() >= 1);
        wait_logged(&master, "the master's log");
        wait_logged(&replica, "the replica's log");
        for (who, log) in [("master", master.aof()), ("replica", replica.aof())] {
            for word in as_given {
                assert!(!holds(&log, &bulk(word)), "{io}: {who}'s AOF holds {word}");
            }
        }
        // Not in a snapshot: what a restart holds is its AOF's.
        assert_ne!(m.info("persistence", "rdb_changes_since_last_save"), "0");

        // The replica, killed and started on its own: from its AOF.
        drop(r);
        replica.kill();
        replica.restart_with(&["--aof", "--io-model", io]);
        let mut r = replica.conn();
        assert_eq!(
            dataset(&mut r),
            want,
            "{io}: replica restarted from its AOF\n{}",
            replica.log()
        );

        // The master, killed and started again: from its AOF.
        drop(m);
        master.kill();
        master.restart_with(&["--aof", "--io-model", io]);
        let mut m = master.conn();
        assert_eq!(
            dataset(&mut m),
            want,
            "{io}: master restarted from its AOF\n{}",
            master.log()
        );
    }
}

/// A replica of a 1.2.0 master applies its `TS.INCRBY` and `TS.DECRBY` as
/// 1.2.0 did (the time at the end, the options passed over, no trim), and
/// logs each again as 1.2.0 wrote it: restarted from its AOF, it holds the
/// samples it held as a replica, which are its master's.
#[test]
fn a_1_2_0_incrby_record_is_logged_again_as_1_2_0_wrote_it() {
    let below_zero = u64::MAX.to_string();
    let payload = empty_payload();
    for io in io_models() {
        let master = master_1_2_0(
            payload.clone(),
            &[
                &["SELECT", "0"],
                &["TS.CREATE", "c", "RETENTION", "100"],
                &["TS.INCRBY", "c", "1", "TIMESTAMP", "1000"],
                &["TS.INCRBY", "c", "1", "TIMESTAMP", "2000"],
                &[
                    "TS.INCRBY",
                    "bad",
                    "1",
                    "RETENTION",
                    "abc",
                    "TIMESTAMP",
                    "5",
                ],
                &["TS.INCRBY", "l", "1", "LABELS", "x", "TIMESTAMP", "50"],
                &["TS.DECRBY", "l", "1", "RETENTION", "10", "TIMESTAMP", "500"],
                &["TS.INCRBY", "big", "1", "TIMESTAMP", "-1"],
                &["SET", "done", "1"],
            ],
        );
        let port = master.to_string();
        let mut replica =
            Server::start(&["--replicaof", "127.0.0.1", &port, "--aof", "--io-model", io]);
        let mut r = replica.conn();
        wait_until("the stream to be applied", 30, || {
            r.cmd(&["EXISTS", "done"]) == V::Int(1)
        });
        let samples = |c: &mut Conn, key: &str| -> Vec<(i64, String)> {
            c.cmd(&["TS.RANGE", key, "-", "+"])
                .arr()
                .iter()
                .map(|s| (s.arr()[0].i(), s.arr()[1].s().to_string()))
                .collect()
        };
        let one = |t: i64, v: &str| (t, v.to_string());
        // As 1.2.0 holds them.
        assert_eq!(
            samples(&mut r, "c"),
            [one(1000, "1"), one(2000, "2")],
            "{io}"
        );
        assert_eq!(samples(&mut r, "bad"), [one(5, "1")], "{io}");
        assert_eq!(samples(&mut r, "l"), [one(50, "1"), one(500, "0")], "{io}");
        let want = dataset(&mut r);

        wait_logged(&replica, "the replica's log");
        let log = replica.aof();
        assert!(
            !holds(&log, &bulk("timestamp")),
            "{io}: logged in another form"
        );
        assert!(holds(&log, &bulk(&below_zero)), "{io}");
        drop(r);
        replica.kill();
        replica.restart_with(&["--aof", "--io-model", io]);
        let mut r = replica.conn();
        assert_eq!(
            dataset(&mut r),
            want,
            "{io}: replica restarted from its AOF\n{}",
            replica.log()
        );
    }
}
