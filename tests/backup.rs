// tests/backup.rs
//
// The BACKUP command family (Redis 8.10) and `preload-file`, end to end:
// real `kividb` processes, each in its own working directory, because the
// point of a backup is that a *different* process can be started from it.
//
// Two more things that need a process to themselves live here too: replaying
// an AOF after a restart, and HOTKEYS, whose tracking session is per process.

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
    fn is_err(&self) -> bool {
        matches!(self, V::Err(_))
    }
    fn strings(&self) -> Vec<String> {
        self.arr().iter().map(|v| v.s().to_string()).collect()
    }
    fn field(&self, name: &str) -> &V {
        self.arr()
            .chunks(2)
            .find(|pair| matches!(&pair[0], V::Str(n) if n == name))
            .map(|pair| &pair[1])
            .unwrap_or_else(|| panic!("no field {name} in {self:?}"))
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

    /// An `INCR`'s reply, or `None` once the server is gone.
    fn try_cmd(&mut self, args: &[&str]) -> Option<V> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).ok()?;
        let mut line = String::new();
        if self.reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        line.trim_end().strip_prefix(':')?.parse().ok().map(V::Int)
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

// ── Server fixture ────────────────────────────────────────────────────────────

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
}

/// Whether nothing has `port`. It is asked with a socket that is bound and
/// never listens. One that listened, as this used to, could be given a
/// connection: a server another test is starting holds, between its fork and
/// its exec, a copy of every socket this process has open, and a client that
/// dialled the port in that moment was connected to the copy and then reset
/// ("Connection reset by peer", at the first command of a test).
fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    // As the server's own socket, so that a copy of this one, while it
    // lasts, does not keep the server from the port.
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

/// A port nothing is listening on — and not one the system hands out to
/// outgoing connections. A client that starts dialling before its server is
/// listening can otherwise be given, as its own end, the very port it is
/// dialling: it is then connected to itself and reads back what it writes.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(31);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn kividb(dir: &Path, port: u16, extra: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kividb"));
    cmd.current_dir(dir)
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--io-model",
            "tokio",
            "--threads",
            "4",
            "--configfile",
            "kividb.conf",
        ])
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null());
    cmd
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let child = kividb(&dir, port, extra)
            .stderr(Stdio::null())
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, dir, child };
        // Connecting waits for the listener.
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
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
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn status(c: &mut Conn) -> (String, String) {
    let reply = c.cmd(&["BACKUP", "STATUS"]);
    (
        reply.field("state").s().to_string(),
        reply.field("error").s().to_string(),
    )
}

fn wait_for_state(c: &mut Conn, wanted: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (state, error) = status(c);
        if state == wanted {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backup did not reach {wanted}: {state} ({error})"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Take a sealed backup and copy its files to `to`, as a control plane would.
fn copy_backup(c: &mut Conn, to: &Path) -> PathBuf {
    std::fs::create_dir_all(to).unwrap();
    let files = c.cmd(&["BACKUP", "LIST"]).strings();
    for file in &files {
        let file = Path::new(file);
        std::fs::copy(file, to.join(file.file_name().unwrap())).unwrap();
    }
    to.join("appendonly.aof.manifest")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Two keys on different shards, as the server will lay them out.
fn keys_on_two_shards() -> (String, String) {
    let shards = kividb::store::num_shards();
    let first = "pair:0".to_string();
    let other = (1..10_000)
        .map(|i| format!("pair:{i}"))
        .find(|k| {
            kividb::store::shard_index(shards, k) != kividb::store::shard_index(shards, &first)
        })
        .expect("a key on another shard");
    (first, other)
}

fn counter(snapshot: &kividb::snapshot::LoadedSnapshot, key: &str) -> i64 {
    match snapshot.dbs[0].get(key) {
        Some(kividb::types::KiviDBValue::String(b)) => {
            std::str::from_utf8(b.as_ref()).unwrap().parse().unwrap()
        }
        None => 0,
        Some(_) => panic!("{key} is not a string"),
    }
}

/// An AOF turned on with `CONFIG SET appendonly yes` is rewritten on its
/// own once it has grown, as one turned on at startup is. It grew until the
/// next restart (#146).
#[test]
fn an_aof_turned_on_at_runtime_is_rewritten_on_its_own() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(c.cmd(&["CONFIG", "SET", "appendonly", "yes"]).ok());
    assert!(
        c.cmd(&["CONFIG", "SET", "auto-aof-rewrite-min-size", "1024"])
            .ok()
    );
    assert!(
        c.cmd(&["CONFIG", "SET", "auto-aof-rewrite-percentage", "10"])
            .ok()
    );
    for i in 0..2000 {
        assert!(c.cmd(&["SET", &format!("key:{i}"), &"x".repeat(50)]).ok());
    }
    let aof = server.dir.join("appendonly.aof");
    let grown = std::fs::metadata(&aof).unwrap().len();
    assert!(grown > 100_000, "the log should have grown: {grown} bytes");
    // The check runs every 10 seconds.
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::fs::metadata(&aof).unwrap().len() >= grown {
        assert!(
            Instant::now() < deadline,
            "appendonly.aof was never rewritten"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 2000);
}

/// A snapshot is the dataset at one moment, with the AOF off as with it on:
/// a transaction that changes two keys on two shards is in it whole or not
/// at all. With the AOF off it was copied a shard at a time, and a SAVE
/// could keep half of one (#148).
#[test]
fn a_snapshot_never_holds_half_a_transaction() {
    for aof in [false, true] {
        let server = Server::start(if aof { &["--aof"] } else { &[] });
        let (a, b) = keys_on_two_shards();
        // Enough keys that copying the dataset takes a while, for a
        // transaction to land in the middle of it.
        let mut fill = server.conn();
        for batch in 0..100 {
            let mut args = vec!["MSET".to_string()];
            for i in 0..2000 {
                args.push(format!("fill:{batch}:{i}"));
                args.push("x".repeat(32));
            }
            let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            assert!(fill.cmd(&refs).ok());
        }
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (stop, a, b) = (Arc::clone(&stop), a.clone(), b.clone());
            let mut c = server.conn();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    assert!(c.cmd(&["MULTI"]).ok());
                    c.cmd(&["INCR", &a]);
                    c.cmd(&["INCR", &b]);
                    assert_eq!(c.cmd(&["EXEC"]).arr().len(), 2);
                }
            })
        };
        let mut c = server.conn();
        // BGSAVE: it copies the dataset on a thread of its own, beside the
        // commands (SAVE is a command, and a transaction waits for it).
        for _ in 0..15 {
            assert!(!c.cmd(&["BGSAVE"]).is_err());
            let deadline = Instant::now() + Duration::from_secs(30);
            while !c
                .cmd(&["INFO", "persistence"])
                .s()
                .contains("rdb_bgsave_in_progress:0")
            {
                assert!(Instant::now() < deadline, "BGSAVE did not finish");
                std::thread::sleep(Duration::from_millis(20));
            }
            let path = server.dir.join("dump.kdb");
            let snapshot = kividb::snapshot::load_snapshot(path.to_str().unwrap()).unwrap();
            let (x, y) = (counter(&snapshot, &a), counter(&snapshot, &b));
            assert_eq!(x, y, "aof {aof}: the snapshot holds half a transaction");
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }
}

/// A backup taken while writes are running restores to the dataset as it was
/// at SEAL: everything before START, everything between START and SEAL, and
/// nothing after — with no write counted twice and none half-applied.
#[test]
fn a_backup_taken_under_load_restores_to_the_seal_boundary() {
    for aof in [false, true] {
        let source = Server::start(if aof { &["--aof"] } else { &[] });
        let mut c = source.conn();

        for i in 0..500 {
            c.cmd(&["SET", &format!("k:{i}"), &format!("v{i}")]);
        }
        c.cmd(&["SET", "ttl", "v", "EX", "10000"]);
        c.cmd(&["RPUSH", "list", "a", "b", "c"]);
        c.cmd(&["XADD", "stream", "IDMP", "p", "m1", "*", "f", "v"]);
        c.cmd(&["TS.CREATE", "ts:src"]);
        c.cmd(&["TS.CREATE", "ts:dst"]);
        c.cmd(&[
            "TS.CREATERULE",
            "ts:src",
            "ts:dst",
            "AGGREGATION",
            "avg",
            "1000",
        ]);
        c.cmd(&["TS.ADD", "ts:src", "1000", "10"]);
        c.cmd(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "t",
            "TEXT",
        ]);
        c.cmd(&["FT.ALIASADD", "idxalias", "idx"]);
        c.cmd(&["HSET", "doc:1", "t", "hello world"]);
        c.cmd(&["SELECT", "3"]);
        c.cmd(&["SET", "in-db-3", "yes"]);
        c.cmd(&["SELECT", "0"]);
        c.cmd(&["SET", "counter", "0"]);

        // A writer running across START and SEAL: one INCR, one RPUSH, repeat.
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let stop = Arc::clone(&stop);
            let mut w = source.conn();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    w.cmd(&["INCR", "counter"]);
                    w.cmd(&["RPUSH", "log", "x"]);
                }
            })
        };
        std::thread::sleep(Duration::from_millis(100));

        assert!(c.cmd(&["BACKUP", "START"]).ok());
        assert!(c.cmd(&["BACKUP", "START"]).is_err(), "one backup at a time");
        wait_for_state(&mut c, "incrementing");
        assert!(
            c.cmd(&["INFO", "persistence"])
                .s()
                .contains("backup_in_progress:1")
        );

        // Before SEAL only the BASE snapshot is listed, by absolute path, and
        // it does not change while writes continue.
        let listed = c.cmd(&["BACKUP", "LIST"]).strings();
        assert_eq!(listed.len(), 1, "{listed:?}");
        let base = Path::new(&listed[0]);
        assert!(
            base.is_absolute() && listed[0].ends_with(".base.kdb"),
            "{listed:?}"
        );
        let base_len = base.metadata().unwrap().len();

        c.cmd(&["SET", "during", "window"]);
        c.cmd(&["DEL", "k:0"]);
        c.cmd(&["SELECT", "3"]);
        c.cmd(&["SET", "during-db-3", "window"]);
        c.cmd(&["SELECT", "0"]);
        c.cmd(&["TS.ADD", "ts:src", "1500", "30"]);
        c.cmd(&["HSET", "doc:2", "t", "hello again"]);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(base.metadata().unwrap().len(), base_len);

        assert!(c.cmd(&["BACKUP", "SEAL"]).ok());
        let counter_after_seal: i64 = c.cmd(&["GET", "counter"]).s().parse().unwrap();
        c.cmd(&["SET", "after-seal", "not in the backup"]);
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();

        assert_eq!(status(&mut c), ("sealed".to_string(), String::new()));
        let names: Vec<String> = c
            .cmd(&["BACKUP", "LIST"])
            .strings()
            .iter()
            .map(|p| {
                Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names.len(), 3, "{names:?}");
        assert!(names[0].ends_with(".base.kdb") && names[1].ends_with(".incr.aof"));
        assert_eq!(names[2], "appendonly.aof.manifest");
        assert!(
            c.cmd(&["BACKUP", "START"]).is_err(),
            "sealed: CLEANUP first"
        );
        assert!(
            c.cmd(&["BACKUP", "ABORT"]).is_err(),
            "sealed: too late to abort"
        );
        assert!(
            c.cmd(&["INFO", "persistence"])
                .s()
                .contains("backup_in_progress:0")
        );

        let manifest = copy_backup(&mut c, &source.dir.join("copied"));
        assert!(c.cmd(&["BACKUP", "CLEANUP"]).ok());
        assert_eq!(status(&mut c).0, "idle");
        assert!(c.cmd(&["BACKUP", "LIST"]).arr().is_empty());
        assert_eq!(
            std::fs::read_dir(base.parent().unwrap()).unwrap().count(),
            0
        );

        // ── Restore into a fresh process ─────────────────────────────────
        let preload = format!("aof:{}", manifest.display());
        let restored = Server::start(&["--preload-file", &preload]);
        let mut r = restored.conn();

        assert_eq!(r.cmd(&["GET", "k:499"]).s(), "v499");
        assert_eq!(r.cmd(&["GET", "k:0"]), V::Nil, "deleted during the window");
        assert_eq!(r.cmd(&["GET", "during"]).s(), "window");
        assert_eq!(r.cmd(&["GET", "after-seal"]), V::Nil);
        let ttl = r.cmd(&["TTL", "ttl"]).i();
        assert!((1..=10000).contains(&ttl), "TTL was {ttl}");
        assert_eq!(
            r.cmd(&["LRANGE", "list", "0", "-1"]).strings(),
            ["a", "b", "c"]
        );
        r.cmd(&["SELECT", "3"]);
        assert_eq!(r.cmd(&["GET", "in-db-3"]).s(), "yes");
        assert_eq!(r.cmd(&["GET", "during-db-3"]).s(), "window");
        r.cmd(&["SELECT", "0"]);

        // The seal boundary: no later than the SEAL command, and whole.
        let counter: i64 = r.cmd(&["GET", "counter"]).s().parse().unwrap();
        let log = r.cmd(&["LLEN", "log"]).i();
        assert!(
            counter > 0 && counter <= counter_after_seal,
            "{counter} vs {counter_after_seal}"
        );
        assert!((log - counter).abs() <= 1, "counter {counter}, log {log}");

        // What a stream remembers about its producers, and a series' rules,
        // are part of the dataset.
        assert_eq!(
            r.cmd(&["XINFO", "STREAM", "stream"])
                .field("iids-tracked")
                .i(),
            1
        );
        // The bucket is still open: with LATEST, from the source the
        // compaction still knows (#128).
        let compacted = r.cmd(&["TS.RANGE", "ts:dst", "-", "+", "LATEST"]);
        assert_eq!(compacted.arr().len(), 1);
        assert_eq!(compacted.arr()[0].arr()[1].s(), "20", "avg of 10 and 30");

        // The index, its alias, and documents from both sides of START.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let found = r.cmd(&["FT.SEARCH", "idxalias", "hello"]);
            if matches!(&found, V::Arr(items) if items.first() == Some(&V::Int(2))) {
                break;
            }
            assert!(Instant::now() < deadline, "index not restored: {found:?}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[test]
fn an_unsealed_backup_can_be_aborted_and_started_again() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    c.cmd(&["SET", "k", "v"]);

    assert_eq!(status(&mut c).0, "idle");
    assert!(c.cmd(&["BACKUP", "SEAL"]).is_err());
    assert!(c.cmd(&["BACKUP", "ABORT"]).is_err());
    assert!(c.cmd(&["BACKUP", "CLEANUP"]).is_err());

    assert!(c.cmd(&["BACKUP", "START"]).ok());
    wait_for_state(&mut c, "incrementing");
    assert!(c.cmd(&["BACKUP", "ABORT"]).ok());
    let (state, error) = status(&mut c);
    assert_eq!(state, "failed");
    assert!(!error.is_empty());
    assert!(c.cmd(&["BACKUP", "SEAL"]).is_err());

    // A failed backup does not stand in the way of the next one.
    std::thread::sleep(Duration::from_millis(200));
    assert!(c.cmd(&["BACKUP", "START"]).ok());
    wait_for_state(&mut c, "incrementing");
    assert!(c.cmd(&["BACKUP", "SEAL"]).ok());
    assert_eq!(c.cmd(&["BACKUP", "LIST"]).arr().len(), 3);
    assert!(c.cmd(&["BACKUP", "CLEANUP"]).ok());

    // The backup directory must be empty: what is in it is the backup.
    std::fs::write(server.dir.join("backupdir").join("stray"), "x").unwrap();
    let reply = c.cmd(&["BACKUP", "START"]);
    assert!(
        reply.is_err() && reply.s().contains("not empty"),
        "{reply:?}"
    );

    assert!(c.cmd(&["BACKUP", "BOGUS"]).is_err());
    assert!(matches!(c.cmd(&["BACKUP", "HELP"]), V::Arr(_)));
}

fn persistence_field(c: &mut Conn, name: &str) -> i64 {
    let info = c.cmd(&["INFO", "persistence"]);
    info.s()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name}:")))
        .unwrap_or_else(|| panic!("no {name} in INFO persistence"))
        .trim()
        .parse()
        .unwrap()
}

fn wait_for_field(c: &mut Conn, name: &str, wanted: i64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while persistence_field(c, name) != wanted {
        assert!(Instant::now() < deadline, "{name} never became {wanted}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// `LASTSAVE` was 0 after every start, and moved only for `SAVE` and
/// `BGSAVE`; the AOF's rewrite fields were never set, and
/// `rdb_last_bgsave_time_sec` held the time of the save, not how long it
/// took (#149).
#[test]
fn lastsave_starts_at_the_start_and_moves_with_every_save() {
    let before = unix_now();
    let server = Server::start(&["--aof"]);
    let mut c = server.conn();
    let started = c.cmd(&["LASTSAVE"]).i();
    assert!(started >= before && started <= unix_now(), "{started}");
    assert_eq!(persistence_field(&mut c, "aof_last_rewrite_time_ms"), -1);
    assert_eq!(persistence_field(&mut c, "rdb_last_bgsave_time_sec"), -1);

    // LASTSAVE counts in seconds.
    std::thread::sleep(Duration::from_millis(1100));
    assert!(c.cmd(&["SET", "k", "v"]).ok());
    assert!(!c.cmd(&["BGREWRITEAOF"]).is_err());
    wait_for_field(&mut c, "aof_rewrite_in_progress", 0);
    let deadline = Instant::now() + Duration::from_secs(20);
    while c.cmd(&["LASTSAVE"]).i() == started {
        assert!(
            Instant::now() < deadline,
            "BGREWRITEAOF did not move LASTSAVE"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(persistence_field(&mut c, "aof_last_rewrite_time_ms") >= 0);
    assert!(persistence_field(&mut c, "aof_last_rewrite_time_sec") >= 0);

    assert!(c.cmd(&["BGSAVE"]).s().contains("started"));
    wait_for_field(&mut c, "rdb_bgsave_in_progress", 0);
    let took = persistence_field(&mut c, "rdb_last_bgsave_time_sec");
    assert!((0..60).contains(&took), "{took}");
}

/// With the AOF on, a write is acknowledged once its record is in the file:
/// a server killed with no warning keeps every write its clients were told
/// was made. It acknowledged the write once the record was queued, and lost
/// the last few (#147).
#[test]
fn a_killed_server_keeps_every_acknowledged_write() {
    let mut server = Server::start(&["--aof"]);
    let port = server.port;
    // The default; `no` gives up the guarantee for speed (1.1.2's way).
    let mut c = server.conn();
    let setting = c.cmd(&["CONFIG", "GET", "aof-ack-after-write"]);
    assert_eq!(setting.arr()[1].s(), "yes");
    assert!(c.cmd(&["CONFIG", "SET", "aof-ack-after-write", "no"]).ok());
    assert!(c.cmd(&["CONFIG", "SET", "aof-ack-after-write", "yes"]).ok());
    assert!(
        c.cmd(&["CONFIG", "SET", "aof-ack-after-write", "maybe"])
            .is_err()
    );
    drop(c);
    for round in 0..3 {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..6)
            .map(|w| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut c = Conn::connect(port);
                    let key = format!("acked:{round}:{w}");
                    let mut acked = 0i64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let reply = c.try_cmd(&["INCR", &key]);
                        match reply {
                            Some(V::Int(n)) => acked = n,
                            _ => break,
                        }
                    }
                    (key, acked)
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(700));
        server.child.kill().unwrap();
        server.child.wait().unwrap();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let acked: Vec<(String, i64)> = writers.into_iter().map(|w| w.join().unwrap()).collect();
        server.child = kividb(&server.dir, port, &["--aof"])
            .stderr(Stdio::null())
            .spawn()
            .expect("could not restart kividb");
        let mut c = server.conn();
        for (key, acked) in acked {
            let kept: i64 = match c.cmd(&["GET", &key]) {
                V::Str(n) => n.parse().unwrap(),
                _ => 0,
            };
            assert!(
                kept >= acked,
                "round {round}: {key} kept {kept}, {acked} acknowledged"
            );
        }
    }
}

/// A backup's state was only in memory: after a restart, a sealed backup
/// could not be cleaned up, and no other could start (#150).
#[test]
fn a_backup_from_before_a_restart_can_be_listed_and_cleaned_up() {
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    c.cmd(&["SET", "k", "v"]);
    assert!(c.cmd(&["BACKUP", "START"]).ok());
    wait_for_state(&mut c, "incrementing");
    assert!(c.cmd(&["BACKUP", "SEAL"]).ok());
    let listed = c.cmd(&["BACKUP", "LIST"]).strings();
    drop(c);
    server.child.kill().unwrap();
    server.child.wait().unwrap();

    server.child = kividb(&server.dir, server.port, &[])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");
    let mut c = server.conn();
    assert_eq!(status(&mut c).0, "sealed");
    assert_eq!(c.cmd(&["BACKUP", "LIST"]).strings(), listed);
    assert!(c.cmd(&["BACKUP", "CLEANUP"]).ok());
    assert_eq!(status(&mut c).0, "idle");
    assert!(c.cmd(&["BACKUP", "START"]).ok());
    wait_for_state(&mut c, "incrementing");
    drop(c);

    // Cut short by the restart: a failed backup, which the next one clears.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    server.child = kividb(&server.dir, server.port, &[])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");
    let mut c = server.conn();
    assert_eq!(status(&mut c).0, "failed");
    assert!(c.cmd(&["BACKUP", "START"]).ok());
    wait_for_state(&mut c, "incrementing");
    assert!(c.cmd(&["BACKUP", "SEAL"]).ok());
    let files = c.cmd(&["BACKUP", "LIST"]).strings();
    assert_eq!(files.len(), 3, "{files:?}");
}

/// A sealed backup's `backup-sealed-ttl` counts from its seal, across a
/// restart (#150).
#[test]
fn a_sealed_backup_from_before_a_restart_still_runs_out() {
    let mut server = Server::start(&["--backup-sealed-ttl", "3"]);
    let mut c = server.conn();
    assert!(c.cmd(&["BACKUP", "START"]).ok());
    wait_for_state(&mut c, "incrementing");
    assert!(c.cmd(&["BACKUP", "SEAL"]).ok());
    drop(c);
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    server.child = kividb(&server.dir, server.port, &["--backup-sealed-ttl", "3"])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");
    let mut c = server.conn();
    assert_eq!(status(&mut c).0, "sealed");
    wait_for_state(&mut c, "idle");
    let left = std::fs::read_dir(server.dir.join("backupdir"))
        .unwrap()
        .count();
    assert_eq!(left, 0);
}

/// `preload-file` means "start from exactly this". If it cannot be loaded the
/// server must not come up on some other dataset, or an empty one.
#[test]
fn a_preload_target_that_cannot_be_loaded_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    for target in ["aof:missing.manifest", "rdb:missing.kdb", "nonsense"] {
        let out = kividb(dir.path(), free_port(), &["--preload-file", target])
            .output()
            .expect("could not run kividb");
        assert!(!out.status.success(), "{target} must not start");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("preload-file"), "{target}: {stderr}");
    }
}

/// `rdb:<file>` preloads a single snapshot — here, one written by SAVE.
#[test]
fn a_single_snapshot_can_be_preloaded() {
    let source = Server::start(&[]);
    let mut c = source.conn();
    c.cmd(&["SET", "from-snapshot", "yes"]);
    assert!(c.cmd(&["SAVE"]).ok());

    let preload = format!("rdb:{}", source.dir.join("dump.kdb").display());
    let restored = Server::start(&["--preload-file", &preload]);
    assert_eq!(restored.conn().cmd(&["GET", "from-snapshot"]).s(), "yes");
}

// ── AOF replay ────────────────────────────────────────────────────────────────

/// Writes that give a TTL as "this long from now", in every spelling, and the
/// keys (and hash fields) they leave with one.
const RELATIVE_TTL_WRITES: &[&[&str]] = &[
    &["SET", "x:set-ex", "v", "EX", "1000"],
    &["SET", "x:set-px", "v", "PX", "1000000"],
    &["SET", "x:set-ifeq", "EX"],
    &["SET", "x:set-ifeq", "v", "IFEQ", "EX", "PX", "1000000"],
    &["SETEX", "x:setex", "1000", "v"],
    &["PSETEX", "x:psetex", "1000000", "v"],
    &["SET", "x:expire", "v"],
    &["EXPIRE", "x:expire", "1000"],
    &["SET", "x:pexpire", "v"],
    &["PEXPIRE", "x:pexpire", "1000000"],
    &["SET", "x:getex", "v"],
    &["GETEX", "x:getex", "EX", "1000"],
    &["MSETEX", "2", "x:{m}1", "v", "x:{m}2", "v", "EX", "1000"],
    &["INCREX", "x:increx", "BYINT", "5", "EX", "1000"],
    &["INCREX", "x:increx", "BYINT", "5", "EX", "5000", "ENX"],
    &["HSET", "x:hash", "a", "1", "b", "2"],
    &["HEXPIRE", "x:hash", "1000", "FIELDS", "1", "a"],
    &["HPEXPIRE", "x:hash", "1000000", "FIELDS", "1", "b"],
    &["HSETEX", "x:hash", "EX", "1000", "FIELDS", "1", "c", "3"],
    &["HSET", "x:hash", "d", "4"],
    &["HGETEX", "x:hash", "PX", "1000000", "FIELDS", "1", "d"],
    // A TTL that is not in the future deletes the key there and then.
    &["SET", "x:gone-1", "v"],
    &["EXPIRE", "x:gone-1", "-1"],
    &["SET", "x:gone-2", "v"],
    &["PEXPIRE", "x:gone-2", "0"],
    // Taking a TTL away.
    &["SET", "x:persist", "v", "EX", "1000"],
    &["GETEX", "x:persist", "PERSIST"],
];

const KEYS_WITH_A_TTL: &[&str] = &[
    "x:set-ex",
    "x:set-px",
    "x:set-ifeq",
    "x:setex",
    "x:psetex",
    "x:expire",
    "x:pexpire",
    "x:getex",
    "x:{m}1",
    "x:{m}2",
    "x:increx",
];

/// When each key, and each field of the hash, expires: Unix milliseconds.
fn expiry_times(c: &mut Conn) -> Vec<i64> {
    let mut times: Vec<i64> = KEYS_WITH_A_TTL
        .iter()
        .map(|key| c.cmd(&["PEXPIRETIME", key]).i())
        .collect();
    let fields = c.cmd(&["HPEXPIRETIME", "x:hash", "FIELDS", "4", "a", "b", "c", "d"]);
    times.extend(fields.arr().iter().map(|t| t.i()));
    times
}

/// The same moments, give or take the time one command takes to run.
fn assert_same_expiries(expected: &[i64], actual: &[i64]) {
    assert_eq!(expected.len(), actual.len());
    for (i, (want, got)) in expected.iter().zip(actual).enumerate() {
        assert!(*want > 0, "entry {i} has no TTL: {expected:?}");
        assert!(
            (want - got).abs() < 250,
            "entry {i} expires at {got}, expected {want}"
        );
    }
}

/// A TTL does not start over because the server was restarted: the AOF
/// records when a key expires, not how long it had left when it was written.
#[test]
fn ttls_survive_an_aof_replay_unchanged() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    for write in RELATIVE_TTL_WRITES {
        let reply = c.cmd(write);
        assert!(!reply.is_err(), "{write:?}: {reply:?}");
    }
    let before = expiry_times(&mut c);
    drop(c);

    // Let the AOF reach the file, stop the server without a shutdown snapshot,
    // and stay down long enough that a TTL counted again from the restart
    // would be visibly later.
    std::thread::sleep(Duration::from_millis(1500));
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    server.child = kividb(&server.dir, server.port, &["--aof"])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");

    let mut c = server.conn();
    assert_same_expiries(&before, &expiry_times(&mut c));
    assert_eq!(c.cmd(&["GET", "x:increx"]).s(), "10");
    assert_eq!(c.cmd(&["EXISTS", "x:gone-1", "x:gone-2"]).i(), 0);
    assert_eq!(c.cmd(&["TTL", "x:persist"]).i(), -1);
    assert_eq!(c.cmd(&["HGETALL", "x:hash"]).arr().len(), 8);
}

// ── A persistence file that cannot be loaded ─────────────────────────────────

/// A `dump.kdb` or an
/// `appendonly.aof` that is there but cannot be loaded used to be skipped with
/// a warning: the server came up empty, served, and overwrote the file at its
/// next save. It must not start.
#[test]
fn a_persistence_file_that_cannot_be_loaded_is_fatal() {
    let garbage = b"this is not a snapshot\x00\x01\x02".repeat(20);
    for (file, extra) in [
        ("dump.kdb", &[][..]),
        ("dump.rdb", &[][..]),
        ("appendonly.aof", &["--aof"][..]),
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(file), &garbage).unwrap();
        let out = kividb(dir.path(), free_port(), extra)
            .output()
            .expect("could not run kividb");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{file}: started anyway\n{stderr}");
        assert!(
            stderr.contains("[FATAL]") && stderr.contains(file),
            "{file}: {stderr}"
        );
        // And it left the file alone.
        assert_eq!(std::fs::read(dir.path().join(file)).unwrap(), garbage);
    }

    // Something that cannot even be opened as a file.
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("dump.kdb")).unwrap();
    let out = kividb(dir.path(), free_port(), &[])
        .output()
        .expect("could not run kividb");
    assert!(!out.status.success());

    // A good log with damage in the middle: nothing of it is loaded.
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    for i in 0..50 {
        assert!(c.cmd(&["SET", &format!("k{i}"), "v"]).ok());
    }
    drop(c);
    std::thread::sleep(Duration::from_millis(1500));
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    let aof = server.dir.join("appendonly.aof");
    let mut log = std::fs::read(&aof).unwrap();
    let middle = log.len() / 2;
    log.splice(middle..middle, b"!!damage!!".iter().copied());
    std::fs::write(&aof, &log).unwrap();
    let out = kividb(&server.dir, server.port, &["--aof"])
        .output()
        .expect("could not run kividb");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains("bad file format"), "{stderr}");
}

/// A log that ends in half a record — the process was killed while appending —
/// is not damage. It loads, the half record is cut off, and the log goes on
/// being appended to and loads again.
#[test]
fn a_truncated_aof_tail_is_cut_off_and_the_log_stays_usable() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    for i in 0..20 {
        assert!(c.cmd(&["SET", &format!("k{i}"), "v"]).ok());
    }
    drop(c);
    std::thread::sleep(Duration::from_millis(1500));
    server.child.kill().unwrap();
    server.child.wait().unwrap();

    let aof = server.dir.join("appendonly.aof");
    let whole = std::fs::read(&aof).unwrap();
    let mut torn = whole.clone();
    torn.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\nhalf");
    std::fs::write(&aof, &torn).unwrap();

    server.child = kividb(&server.dir, server.port, &["--aof"])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");
    let mut c = server.conn();
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 20);
    assert_eq!(c.cmd(&["EXISTS", "half"]).i(), 0);
    assert!(c.cmd(&["SET", "after", "v"]).ok());
    drop(c);
    std::thread::sleep(Duration::from_millis(1500));
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    let log = std::fs::read(&aof).unwrap();
    assert!(
        !log.windows(4).any(|w| w == b"half"),
        "the partial record was left in the file"
    );

    // The record written after the tear follows whole records, so this loads.
    server.child = kividb(&server.dir, server.port, &["--aof"])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");
    let mut c = server.conn();
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 21);
    assert_eq!(c.cmd(&["GET", "after"]).s(), "v");
}

/// A server with the AOF on, a key written, a checkpoint (SAVE), another
/// key written after it, and then killed: the files a checkpoint starts from.
fn checkpointed_then_written(server: &mut Server) {
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "in-dump", "1"]).ok());
    assert!(c.cmd(&["SAVE"]).ok());
    assert!(c.cmd(&["SET", "in-log", "2"]).ok());
    drop(c);
    std::thread::sleep(Duration::from_millis(1500));
    server.child.kill().unwrap();
    server.child.wait().unwrap();
}

fn restart_with_aof(server: &mut Server) -> Conn {
    server.child = kividb(&server.dir, server.port, &["--aof"])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");
    server.conn()
}

/// A checkpoint turns the log over (the old log becomes `.prev`, a new delta
/// for the next save number starts) and only then writes its snapshot, with
/// writes no longer held. Stopped in between, the snapshot on disk is still
/// the old one: the old log and then the new one are replayed on top of it,
/// and nothing written on either side of the turn-over is lost.
/// `auto-aof-rewrite-percentage` and `-min-size` take effect when set, as in
/// Redis: 0 stops automatic rewrites. They were read once at startup, and
/// `CONFIG SET` was answered OK with no effect.
#[test]
fn automatic_rewrites_follow_config_set() {
    let server = Server::start(&["--aof"]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "auto-aof-rewrite-percentage", "0"])
            .ok()
    );
    assert!(
        c.cmd(&["CONFIG", "SET", "auto-aof-rewrite-min-size", "1mb"])
            .ok()
    );
    let get = |c: &mut Conn, name: &str| c.cmd(&["CONFIG", "GET", name]).arr()[1].s().to_string();
    assert_eq!(get(&mut c, "auto-aof-rewrite-percentage"), "0");
    assert_eq!(get(&mut c, "auto-aof-rewrite-min-size"), "1048576");
}

#[test]
fn a_checkpoint_stopped_before_its_snapshot_is_resumed_from_both_logs() {
    let mut server = Server::start(&["--aof"]);
    checkpointed_then_written(&mut server);

    // As the turn-over leaves it: the log moved aside, and a new delta that
    // goes on from the next save number, with a write made after it.
    let aof = server.dir.join("appendonly.aof");
    let old = std::fs::read(&aof).unwrap();
    assert!(
        old.starts_with(b"KIVIDLT1"),
        "the log is a delta after a SAVE"
    );
    let number = u64::from_le_bytes(old[8..16].try_into().unwrap());
    std::fs::rename(&aof, server.dir.join("appendonly.aof.prev")).unwrap();
    let mut fresh = b"KIVIDLT1".to_vec();
    fresh.extend_from_slice(&(number + 1).to_le_bytes());
    fresh.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$9\r\nafter-cut\r\n$1\r\n3\r\n");
    std::fs::write(&aof, &fresh).unwrap();

    let mut c = restart_with_aof(&mut server);
    assert_eq!(c.cmd(&["GET", "in-dump"]).s(), "1");
    assert_eq!(c.cmd(&["GET", "in-log"]).s(), "2");
    assert_eq!(c.cmd(&["GET", "after-cut"]).s(), "3");
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 3);

    // And the next checkpoint finishes what that one did not.
    assert!(c.cmd(&["SET", "later", "4"]).ok());
    assert!(c.cmd(&["SAVE"]).ok());
    assert!(!server.dir.join("appendonly.aof.prev").exists());
    drop(c);
    std::thread::sleep(Duration::from_millis(1500));
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    let mut c = restart_with_aof(&mut server);
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 4);
    assert_eq!(c.cmd(&["GET", "after-cut"]).s(), "3");
}

/// A checkpoint that finished, but was stopped before it removed the old
/// log: the snapshot already holds what that log holds. Replaying it would
/// apply those writes twice, so it is set aside.
#[test]
fn the_old_log_of_a_finished_checkpoint_is_not_replayed() {
    let mut server = Server::start(&["--aof"]);
    checkpointed_then_written(&mut server);
    // What the finished checkpoint's old log held: already in its snapshot,
    // and a counter shows it if it is applied again.
    std::fs::write(
        server.dir.join("appendonly.aof.prev"),
        b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n",
    )
    .unwrap();

    let mut c = restart_with_aof(&mut server);
    assert_eq!(c.cmd(&["EXISTS", "counter"]).i(), 0);
    assert_eq!(c.cmd(&["GET", "in-dump"]).s(), "1");
    assert_eq!(c.cmd(&["GET", "in-log"]).s(), "2");
    assert!(!server.dir.join("appendonly.aof.prev").exists());
}

// ── The config file ──────────────────────────────────────────────────────────

/// `appendonly yes` in the
/// config file did nothing, and an unknown directive or an unreadable value
/// was dropped without a word.
#[test]
fn the_config_file_is_read_strictly() {
    for (body, needle) in [
        ("port 0\nthis-directive-does-not-exist 42\n", "line 2"),
        ("maxmemory notanumber\n", "maxmemory"),
        ("appendonly perhaps\n", "appendonly"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("kividb.conf"), body).unwrap();
        let out = kividb(dir.path(), free_port(), &[])
            .output()
            .expect("could not run kividb");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{body:?} started anyway");
        assert!(
            stderr.contains("Bad directive") && stderr.contains(needle),
            "{body:?}: {stderr}"
        );
    }

    // `appendonly yes` turns the AOF on; `dir` is where the files go; an
    // option given on the command line beats the file.
    let dir = tempfile::tempdir().unwrap().keep();
    let data = dir.join("data");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(
        dir.join("kividb.conf"),
        format!(
            "appendonly yes\nmaxmemory 100mb\nport 1\ndir \"{}\"\nsave 900 1\n",
            data.display().to_string().replace('\\', "\\\\")
        ),
    )
    .unwrap();
    let port = free_port();
    let child = kividb(&dir, port, &[])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not start kividb");
    let server = Server { port, dir, child };
    let mut c = server.conn();
    let persistence = c.cmd(&["INFO", "persistence"]);
    assert!(persistence.s().contains("aof_enabled:1"), "{persistence:?}");
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "maxmemory"]).strings(),
        ["maxmemory", "104857600"]
    );
    assert!(c.cmd(&["SET", "k", "v"]).ok());
    std::thread::sleep(Duration::from_millis(1500));
    assert!(data.join("appendonly.aof").exists());
    assert!(!server.dir.join("appendonly.aof").exists());
}

// ── HOTKEYS (Redis 8.6) ───────────────────────────────────────────────────────

#[test]
fn hotkeys_session() {
    // A server process of its own: the tracking session belongs to the
    // process and sees every command it runs.
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(c.cmd(&["HOTKEYS", "STOP"]).is_err(), "nothing to stop");
    assert!(c.cmd(&["HOTKEYS", "START"]).is_err(), "METRICS is required");
    assert!(
        c.cmd(&["HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "1", "5"])
            .is_err()
    );

    assert!(
        c.cmd(&[
            "HOTKEYS", "START", "METRICS", "2", "CPU", "NET", "COUNT", "3"
        ])
        .ok()
    );
    assert!(
        c.cmd(&["HOTKEYS", "START", "METRICS", "1", "CPU"]).is_err(),
        "already running"
    );
    assert!(
        c.cmd(&["HOTKEYS", "RESET"]).is_err(),
        "must be stopped first"
    );

    let payload = "x".repeat(2000);
    for _ in 0..50 {
        c.cmd(&["SET", "hk:hot", &payload]);
        c.cmd(&["GET", "hk:hot"]);
    }
    c.cmd(&["SET", "hk:cold", "v"]);
    c.cmd(&["MSET", "hk:m1", "1", "hk:m2", "2"]);

    let running = c.cmd(&["HOTKEYS", "GET"]);
    assert_eq!(running.arr().len(), 1);
    assert_eq!(running.arr()[0].field("tracking-active").i(), 1);

    assert!(c.cmd(&["HOTKEYS", "STOP"]).ok());
    let report = c.cmd(&["HOTKEYS", "GET"]);
    let report = &report.arr()[0];
    assert_eq!(report.field("tracking-active").i(), 0);
    assert_eq!(report.field("sample-ratio").i(), 1);
    assert_eq!(
        report.field("selected-slots"),
        &V::Arr(vec![V::Arr(vec![V::Int(0), V::Int(16383)])])
    );
    assert!(report.field("net-bytes-all-commands-all-slots").i() > 200_000);
    assert!(report.field("collection-start-time-unix-ms").i() > 1_600_000_000_000);

    let by_net = report.field("by-net-bytes").arr();
    assert!(by_net.len() <= 6, "top 3 as key/value pairs: {by_net:?}");
    assert_eq!(by_net[0].s(), "hk:hot");
    assert!(by_net[1].i() > 200_000);
    let by_cpu = report.field("by-cpu-time-us").strings_or_ints();
    assert!(by_cpu.iter().any(|k| k == "hk:hot"), "{by_cpu:?}");

    // The results stay until reset.
    assert!(c.cmd(&["HOTKEYS", "RESET"]).ok());
    assert_eq!(c.cmd(&["HOTKEYS", "GET"]), V::Nil);
    assert!(matches!(c.cmd(&["HOTKEYS", "HELP"]), V::Arr(_)));
}

impl V {
    fn strings_or_ints(&self) -> Vec<String> {
        self.arr()
            .iter()
            .map(|v| match v {
                V::Int(n) => n.to_string(),
                other => other.s().to_string(),
            })
            .collect()
    }
}

// ── Scripts ───────────────────────────────────────────────────────────────────

/// What Redis's scripts call that did nothing or was missing (#154):
/// `redis.log` writes to the server's log, `redis.replicate_commands()` is
/// true, `redis.set_repl(redis.REPL_ALL)` is taken and the others refused.
#[cfg(feature = "lua")]
#[test]
fn a_script_logs_and_asks_for_effects_replication() {
    let server = Server::start(&["--logfile", "kividb.log", "--loglevel", "notice"]);
    let mut c = server.conn();
    let reply = c.cmd(&[
        "EVAL",
        "redis.log(redis.LOG_WARNING, 'from', 'a script', 7) \
         redis.log(redis.LOG_DEBUG, 'not at notice') \
         return redis.replicate_commands()",
        "0",
    ]);
    assert_eq!(reply.i(), 1, "{reply:?}");
    let reply = c.cmd(&[
        "EVAL",
        "redis.set_repl(redis.REPL_ALL) return redis.call('SET', KEYS[1], 'v')",
        "1",
        "k",
    ]);
    assert!(reply.ok(), "{reply:?}");
    for script in [
        "redis.set_repl(redis.REPL_NONE)",
        "redis.log(redis.LOG_WARNING)",
        "redis.log(9, 'x')",
    ] {
        assert!(c.cmd(&["EVAL", script, "0"]).is_err(), "{script}");
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let log = std::fs::read_to_string(server.dir.join("kividb.log")).unwrap_or_default();
        if log.contains("from a script 7") {
            assert!(!log.contains("not at notice"), "{log}");
            break;
        }
        assert!(Instant::now() < deadline, "not in the log: {log}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `SCRIPT KILL` does not stop a script that has written: half its writes
/// would stay. It is answered UNKILLABLE, as in Redis, and the script runs
/// on; it was answered OK and ran on all the same (#129).
#[cfg(feature = "lua")]
#[test]
fn a_script_that_has_written_is_unkillable() {
    let server = Server::start(&[]);
    let port = server.port;
    let mut c = server.conn();
    assert!(c.cmd(&["CONFIG", "SET", "lua-time-limit", "200"]).ok());

    std::thread::spawn(move || {
        let mut r = Conn::connect(port);
        r.cmd(&["EVAL", "redis.call('SET', 'w', '1') while true do end", "0"])
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let reply = c.cmd(&["GET", "w"]);
        if reply.is_err() && reply.s().starts_with("BUSY") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the script never got busy: {reply:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let reply = c.cmd(&["SCRIPT", "KILL"]);
    assert!(
        reply.is_err() && reply.s().starts_with("UNKILLABLE"),
        "{reply:?}"
    );
    // It runs on: the server is still busy with it.
    let reply = c.cmd(&["PING"]);
    assert!(reply.is_err() && reply.s().starts_with("BUSY"), "{reply:?}");
}

/// While a script runs, other clients wait for it; once it has overrun
/// `lua-time-limit` they are told BUSY — and SCRIPT KILL still gets through
/// to stop it. (A process of its own: the script engine, and its time limit,
/// are shared by every server in a process.)
#[cfg(feature = "lua")]
#[test]
fn a_script_past_its_time_limit_is_busy_and_can_be_killed() {
    let server = Server::start(&[]);
    let port = server.port;
    let mut c = server.conn();
    assert!(c.cmd(&["CONFIG", "SET", "lua-time-limit", "300"]).ok());
    assert!(c.cmd(&["SET", "k", "v"]).ok());

    let runner = std::thread::spawn(move || {
        let mut r = Conn::connect(port);
        r.cmd(&["EVAL", "while true do end", "0"])
    });
    std::thread::sleep(Duration::from_millis(100));

    // Sent while the script is within its limit: it waits, and is refused
    // only when the limit has passed.
    let started = Instant::now();
    let reply = c.cmd(&["GET", "k"]);
    assert!(reply.is_err() && reply.s().starts_with("BUSY"), "{reply:?}");
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "refused after {:?}: it did not wait",
        started.elapsed()
    );
    // From here on, at once.
    let reply = c.cmd(&["SET", "k", "other"]);
    assert!(reply.is_err() && reply.s().starts_with("BUSY"), "{reply:?}");

    assert!(c.cmd(&["SCRIPT", "KILL"]).ok());
    assert!(runner.join().unwrap().is_err());
    // And the server is its usual self again.
    assert_eq!(c.cmd(&["GET", "k"]).s(), "v");
    // A script that finishes in time holds nobody up for long, and nobody
    // is told BUSY.
    let reply = c.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]);
    assert_eq!(reply.s(), "v");
}
