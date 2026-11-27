// A write command that fails changes nothing.
//
// Only a write whose reply is not an error is sent to replicas and the AOF.
// So a command that changed something and then failed left the change on
// the master alone: a replica, and a replay of the AOF, never had it.
// `JSON.MSET` kept the triples before the one that failed (#551),
// `XGROUP CREATE … MKSTREAM` with an ID that is no ID kept the stream it had
// made, and `TS.ADD` of a sample BLOCK refuses kept the options it was given
// and the samples retention had trimmed.
//
// Each test runs its own servers, as processes: an AOF is replayed by a
// restart.
//
// The I/O model is the platform's own unless `KIVIDB_TEST_IO_MODEL` says.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod common;
use common::{free_port, restart_verified};

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
    fn text(&self) -> Option<String> {
        match self {
            V::Bulk(b) => Some(String::from_utf8_lossy(b).into_owned()),
            V::Simple(s) => Some(s.clone()),
            V::Nil => None,
            other => panic!("not a string: {other:?}"),
        }
    }

    fn error(&self) -> &str {
        match self {
            V::Error(e) => e,
            other => panic!("not an error: {other:?}"),
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
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        self.stream.write_all(&buf).unwrap();
        self.read()
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
            _ => panic!("unexpected RESP: {line:?}"),
        }
    }

    fn ok(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args), V::Simple("OK".into()), "{args:?}");
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
                    .args([
                        "--port",
                        &port.to_string(),
                        "--bind",
                        "127.0.0.1",
                        "--threads",
                        "4",
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

    fn aof(&self) -> Vec<Vec<String>> {
        records(&std::fs::read(self.dir.join("appendonly.aof")).unwrap_or_default())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The commands of an AOF, in order. Whatever is not a command (a header)
/// is passed over.
fn records(buf: &[u8]) -> Vec<Vec<String>> {
    fn line(buf: &[u8], at: &mut usize) -> Option<String> {
        let end = buf[*at..].windows(2).position(|w| w == b"\r\n")? + *at;
        let text = String::from_utf8_lossy(&buf[*at..end]).into_owned();
        *at = end + 2;
        Some(text)
    }
    let mut out = Vec::new();
    let mut at = 0;
    while at < buf.len() {
        if buf[at] != b'*' {
            at += 1;
            continue;
        }
        let start = at;
        let parsed = (|| {
            let n: usize = line(buf, &mut at)?[1..].parse().ok()?;
            let mut parts = Vec::with_capacity(n);
            for _ in 0..n {
                let len: usize = line(buf, &mut at)?.strip_prefix('$')?.parse().ok()?;
                let end = at.checked_add(len)?;
                if end + 2 > buf.len() {
                    return None;
                }
                parts.push(String::from_utf8_lossy(&buf[at..end]).into_owned());
                at = end + 2;
            }
            Some(parts)
        })();
        match parsed {
            Some(parts) => out.push(parts),
            None => at = start + 1,
        }
    }
    out
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
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

// ── The writes ────────────────────────────────────────────────────────────

/// The keys the failing writes go at, as they are before them.
fn set_up(c: &mut Conn) {
    c.ok(&["JSON.SET", "a", "$", "{}"]);
    c.ok(&["JSON.SET", "d", "$", r#"{"x":{}}"#]);
    c.ok(&["SET", "s", "plain"]);
    // A series that refuses a second sample at a timestamp, holding samples
    // a retention of 10 would trim, and one that keeps the last.
    c.ok(&["TS.CREATE", "t", "DUPLICATE_POLICY", "BLOCK"]);
    for at in ["1000", "2000", "5000"] {
        assert_eq!(
            c.cmd(&["TS.ADD", "t", at, "1"]),
            V::Int(at.parse().unwrap())
        );
    }
    c.ok(&["TS.CREATE", "u", "DUPLICATE_POLICY", "LAST"]);
    assert_eq!(c.cmd(&["TS.ADD", "u", "1000", "1"]), V::Int(1000));
}

/// Writes that fail, each after the point where it used to have changed
/// something; and one that does not fail, so a replica or a replay that
/// had nothing at all would not pass.
fn failing_writes(c: &mut Conn) {
    // The issue's case: `c` does not exist, and a new key needs the root.
    let r = c.cmd(&["JSON.MSET", "a", "$.n", "10", "c", "$.x", "1"]);
    assert!(r.error().contains("created at the root"), "{r:?}");
    let r = c.cmd(&["JSON.MSET", "a", "$.n", "10", "s", "$", "1"]);
    assert_eq!(r.error(), "Existing key has wrong Redis type", "{r:?}");
    let r = c.cmd(&[
        "JSON.MSET",
        "a",
        "$.n",
        "10",
        "d",
        "$",
        "{}",
        "d",
        ".x.y",
        "1",
    ]);
    assert!(r.error().contains("path does not exist"), "{r:?}");
    let r = c.cmd(&["JSON.MSET", "b", "$", "{}", "b", ".x.y", "1"]);
    assert!(r.error().contains("path does not exist"), "{r:?}");

    let r = c.cmd(&["XGROUP", "CREATE", "stream", "g", "not-an-id", "MKSTREAM"]);
    assert!(r.error().contains("Invalid stream ID"), "{r:?}");

    let r = c.cmd(&["TS.ADD", "t", "5000", "2", "RETENTION", "10"]);
    assert!(r.error().contains("BLOCK"), "{r:?}");
    let r = c.cmd(&["TS.ADD", "u", "1000", "2", "DUPLICATE_POLICY", "BLOCK"]);
    assert!(r.error().contains("BLOCK"), "{r:?}");

    c.ok(&["JSON.MSET", "a", "$.ok", "1", "e", "$", "[1]"]);
}

fn ts_info(c: &mut Conn, key: &str, field: &str) -> V {
    let V::Array(info) = c.cmd(&["TS.INFO", key]) else {
        panic!("TS.INFO {key}");
    };
    let at = info
        .iter()
        .position(|v| v.text().as_deref() == Some(field))
        .unwrap_or_else(|| panic!("no {field} in TS.INFO"));
    info[at + 1].clone()
}

/// What the writes are about, as `c`'s server has it.
fn state(c: &mut Conn) -> Vec<String> {
    let mut out = Vec::new();
    for key in ["a", "b", "c", "d", "e"] {
        out.push(format!("{key}: {:?}", c.cmd(&["JSON.GET", key])));
    }
    out.push(format!("s: {:?}", c.cmd(&["GET", "s"])));
    out.push(format!("stream: {:?}", c.cmd(&["EXISTS", "stream"])));
    out.push(format!("t: {:?}", c.cmd(&["TS.RANGE", "t", "-", "+"])));
    out.push(format!(
        "t retention: {:?}",
        ts_info(c, "t", "retentionTime")
    ));
    out.push(format!("u: {:?}", c.cmd(&["TS.RANGE", "u", "-", "+"])));
    out.push(format!(
        "u policy: {:?}",
        ts_info(c, "u", "duplicatePolicy")
    ));
    out
}

/// What the master must have after them: as before, but for the one write
/// that succeeded.
fn check_master(c: &mut Conn) {
    assert_eq!(
        c.cmd(&["JSON.GET", "a"]).text().as_deref(),
        Some(r#"{"ok":1}"#)
    );
    assert_eq!(
        c.cmd(&["JSON.GET", "d"]).text().as_deref(),
        Some(r#"{"x":{}}"#)
    );
    assert_eq!(c.cmd(&["EXISTS", "b", "c", "stream"]), V::Int(0));
    let V::Array(samples) = c.cmd(&["TS.RANGE", "t", "-", "+"]) else {
        panic!("TS.RANGE t");
    };
    assert_eq!(samples.len(), 3, "nothing trimmed: {samples:?}");
    assert_eq!(
        ts_info(c, "t", "retentionTime").text().as_deref(),
        Some("0")
    );
    assert_eq!(
        ts_info(c, "u", "duplicatePolicy").text().as_deref(),
        Some("last")
    );
}

#[test]
fn a_replica_matches_its_master_after_writes_that_fail() {
    let master = Server::start(&[]);
    let replica = Server::replica_of(&master);
    let mut m = master.conn();
    let mut r = replica.conn();
    set_up(&mut m);
    failing_writes(&mut m);
    let mark = now_ms().to_string();
    m.ok(&["SET", "caught-up", &mark]);
    wait_for("the replica to catch up", || {
        r.cmd(&["GET", "caught-up"]).text().as_deref() == Some(mark.as_str())
    });
    check_master(&mut m);
    assert_eq!(state(&mut r), state(&mut m));
}

#[test]
fn a_replay_of_the_aof_matches_after_writes_that_fail() {
    let mut s = Server::start(&["--aof"]);
    let mut c = s.conn();
    set_up(&mut c);
    failing_writes(&mut c);
    check_master(&mut c);
    let before = state(&mut c);
    c.ok(&["SET", "logged", "1"]);
    wait_for("the AOF", || {
        s.aof()
            .iter()
            .any(|r| r.len() >= 2 && r[0] == "SET" && r[1] == "logged")
    });
    // Nothing that failed was logged.
    let msets = s
        .aof()
        .iter()
        .filter(|r| r[0].eq_ignore_ascii_case("JSON.MSET"))
        .count();
    assert_eq!(msets, 1, "only the JSON.MSET that succeeded");

    s.restart();
    let mut c = s.conn();
    assert_eq!(state(&mut c), before);
}

// ── Bitmap commands that write nothing ──────────────────────────────────────

/// Strings kept in an entry, in a slab and in an allocation of their own.
const BITMAPS: [(&str, usize); 3] = [("b-entry", 100), ("b-slab", 16 << 10), ("b-own", 64 << 10)];

/// On each of `BITMAPS`, the bitmap commands that write nothing, a
/// `BITFIELD` that fails after a `SET`, and three that write. Returns the
/// writes, in order.
fn bitmap_commands(c: &mut Conn) -> Vec<Vec<String>> {
    let mut writes = Vec::new();
    for (key, len) in BITMAPS {
        // 'U' is 0x55: the first bit of each byte clear, the second set.
        c.ok(&["SET", key, &"U".repeat(len)]);
        let last = ((len - 1) * 8).to_string();
        let second = ((len - 1) * 8 + 1).to_string();
        let nothing: [&[&str]; 5] = [
            &["SETBIT", key, &last, "0"],
            &["SETBIT", key, &second, "1"],
            &["BITFIELD", key, "GET", "u8", &last],
            &["BITFIELD_RO", key, "GET", "u8", &last],
            &[
                "BITFIELD", key, "OVERFLOW", "FAIL", "INCRBY", "u8", &last, "200",
            ],
        ];
        let before = changes(c);
        for cmd in nothing {
            assert!(!matches!(c.cmd(cmd), V::Error(_)), "{cmd:?}");
        }
        let r = c.cmd(&["BITFIELD", key, "SET", "u8", "0", "1", "GET"]);
        assert_eq!(r.error(), "ERR syntax error");
        assert_eq!(changes(c), before, "{key}: nothing written");
        let write: [&[&str]; 3] = [
            &["SETBIT", key, &last, "1"],
            &[
                "BITFIELD", key, "SET", "u8", "0", "1", "OVERFLOW", "FAIL", "INCRBY", "u8", "0",
                "255",
            ],
            &[
                "BITFIELD", key, "OVERFLOW", "FAIL", "INCRBY", "u8", "8", "255", "SET", "u8", "8",
                "2",
            ],
        ];
        for cmd in write {
            assert!(!matches!(c.cmd(cmd), V::Error(_)), "{cmd:?}");
            writes.push(cmd.iter().map(|s| s.to_string()).collect());
        }
        assert_eq!(changes(c), before + 3, "{key}: one change a write");
    }
    writes
}

/// `rdb_changes_since_last_save`: the writes counted as changes.
fn changes(c: &mut Conn) -> u64 {
    let info = c.cmd(&["INFO", "persistence"]).text().unwrap();
    let line = info
        .lines()
        .find_map(|line| line.strip_prefix("rdb_changes_since_last_save:"))
        .expect("rdb_changes_since_last_save");
    line.trim().parse().unwrap()
}

/// `BITMAPS`' bytes, as `c`'s server has them.
fn bitmaps(c: &mut Conn) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for (key, len) in BITMAPS {
        let V::Bulk(bytes) = c.cmd(&["GET", key]) else {
            panic!("GET {key}");
        };
        assert_eq!(bytes.len(), len, "{key}");
        out.push(bytes);
    }
    out
}

fn check_bitmaps(c: &mut Conn) {
    for ((key, len), bytes) in BITMAPS.iter().zip(bitmaps(c)) {
        assert_eq!((bytes[0], bytes[1], bytes[len - 1]), (1, 2, 0xD5), "{key}");
        assert!(bytes[2..len - 1].iter().all(|&b| b == 0x55), "{key}");
    }
}

#[test]
fn a_replica_matches_its_master_after_bitmap_commands_that_write_nothing() {
    let master = Server::start(&[]);
    let replica = Server::replica_of(&master);
    let mut m = master.conn();
    let mut r = replica.conn();
    bitmap_commands(&mut m);
    let mark = now_ms().to_string();
    m.ok(&["SET", "caught-up", &mark]);
    wait_for("the replica to catch up", || {
        r.cmd(&["GET", "caught-up"]).text().as_deref() == Some(mark.as_str())
    });
    check_bitmaps(&mut m);
    assert_eq!(bitmaps(&mut r), bitmaps(&mut m));
}

#[test]
fn the_aof_has_only_the_bitmap_commands_that_write() {
    let mut s = Server::start(&["--aof"]);
    let mut c = s.conn();
    let writes = bitmap_commands(&mut c);
    check_bitmaps(&mut c);
    c.ok(&["SET", "logged", "1"]);
    wait_for("the AOF", || {
        s.aof()
            .iter()
            .any(|r| r.len() >= 2 && r[0] == "SET" && r[1] == "logged")
    });
    let logged: Vec<Vec<String>> = s
        .aof()
        .into_iter()
        .filter(|r| {
            r[0].to_ascii_uppercase().starts_with("BIT") || r[0].eq_ignore_ascii_case("SETBIT")
        })
        .collect();
    assert_eq!(logged, writes);

    let before = bitmaps(&mut c);
    s.restart();
    let mut c = s.conn();
    assert_eq!(bitmaps(&mut c), before);
}
