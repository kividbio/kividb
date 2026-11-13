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

/// Wait for a server started from a dataset with search indexes to have
/// built them: until then, in the background, most commands answer
/// `LOADING` (`spawn_snapshot_index_backfill_thread` in src/main.rs).
fn wait_until_loaded(c: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match c.cmd(&["DBSIZE"]) {
            V::Err(e) if e.starts_with("LOADING") => {
                assert!(Instant::now() < deadline, "still loading: {e}");
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => return,
        }
    }
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
    match snapshot.dbs[0].get(key.as_bytes()) {
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
    // The check runs every 10 seconds. While the log is turned over it is
    // renamed to `appendonly.aof.prev` before the new one takes its name:
    // not there for a moment is the rewrite under way.
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::fs::metadata(&aof).map_or(0, |m| m.len()) >= grown {
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
        wait_until_loaded(&mut r);

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

/// `appendfsync always` (#513): a reply to a write is sent only once its
/// record is synced to disk, whether the setting comes from the
/// configuration file, the command line or `CONFIG SET`. It was accepted
/// and the file synced once a second, so a power failure could lose a
/// second of acknowledged writes. The sync is held at a test failpoint, and
/// no reply may come while it is held.
#[test]
fn appendfsync_always_replies_only_once_the_write_is_synced() {
    const SET: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    for how in ["file", "command line", "CONFIG SET"] {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let mut extra = vec!["--aof"];
        match how {
            "file" => std::fs::write(dir.join("kividb.conf"), "appendfsync always\n").unwrap(),
            "command line" => extra.extend(["--appendfsync", "always"]),
            _ => {}
        }
        let child = kividb(&dir, port, &extra)
            .env("KIVIDB_TEST_FAILPOINTS", "aof-fsync-before-reply")
            .stderr(Stdio::null())
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, dir, child };
        let mut c = server.conn();
        if how == "CONFIG SET" {
            let before = c.cmd(&["CONFIG", "GET", "appendfsync"]);
            assert_eq!(before.arr()[1].s(), "everysec");
            assert!(
                c.cmd(&["CONFIG", "SET", "appendfsync", "sometimes"])
                    .is_err()
            );
            assert!(c.cmd(&["CONFIG", "SET", "appendfsync", "always"]).ok());
        }
        let setting = c.cmd(&["CONFIG", "GET", "appendfsync"]);
        assert_eq!(setting.arr()[1].s(), "always", "{how}");

        let held = server.dir.join("failpoint.aof-fsync-before-reply.1");
        c.writer.write_all(SET).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !held.exists() {
            assert!(
                Instant::now() < deadline,
                "{how}: the write was acknowledged without a sync"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // The sync is held: no reply yet.
        c.writer
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut line = String::new();
        let early = c.reader.read_line(&mut line);
        assert!(
            early.is_err() && line.is_empty(),
            "{how}: replied {line:?} before its write was synced"
        );
        c.writer
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        std::fs::remove_file(&held).unwrap();
        assert!(c.read().ok(), "{how}");

        // Under `everysec` the reply does not wait for a sync.
        assert!(c.cmd(&["CONFIG", "SET", "appendfsync", "everysec"]).ok());
        assert!(c.cmd(&["SET", "k2", "v"]).ok());
        assert!(
            !server
                .dir
                .join("failpoint.aof-fsync-before-reply.2")
                .exists(),
            "{how}: synced for a reply under everysec"
        );
    }
}

/// The test hold of the sync for replies under `appendfsync always` is
/// reached by that sync only, not by the second's sync, which answers no
/// reply (#515). The second's sync took it too: when it ran first, with a
/// write's record in the file, it took the hit a test waited for, and the
/// write's own sync then waited at the next hit, which the test above never
/// lets go of. Here the second's sync is held (at `fsync`) with a record
/// unsynced while the setting turns to `always`, and a write is sent: the
/// first hold of the reply's sync is the write's, and its reply comes once
/// that is let go.
#[test]
fn the_reply_sync_failpoint_is_the_replys_not_the_seconds_sync() {
    const SET: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nw\r\n";
    let dir = tempfile::tempdir().unwrap().keep();
    let port = free_port();
    let child = kividb(&dir, port, &["--aof"])
        .env("KIVIDB_TEST_FAILPOINTS", "fsync,aof-fsync-before-reply")
        .stderr(Stdio::null())
        .spawn()
        .expect("could not start kividb");
    let server = Server { port, dir, child };
    let wait_for = |path: &std::path::Path, what: &str| {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !path.exists() {
            assert!(Instant::now() < deadline, "{what} was never reached");
            std::thread::sleep(Duration::from_millis(5));
        }
    };
    let mut c = server.conn();
    // A record written and not yet synced, under `everysec`.
    assert!(c.cmd(&["SET", "k", "v"]).ok());
    // The second's sync, asked for, holds the writer with that record
    // unsynced.
    let mut waiter = server.conn();
    waiter
        .writer
        .write_all(b"*4\r\n$7\r\nWAITAOF\r\n$1\r\n1\r\n$1\r\n0\r\n$1\r\n0\r\n")
        .unwrap();
    let fsync = server.dir.join("failpoint.fsync.1");
    wait_for(&fsync, "the second's sync");
    // Any later second's sync goes on at once.
    let done = Arc::new(AtomicBool::new(false));
    let releaser = {
        let (dir, done) = (server.dir.clone(), done.clone());
        std::thread::spawn(move || {
            let mut next = 2;
            while !done.load(Ordering::Relaxed) {
                if std::fs::remove_file(dir.join(format!("failpoint.fsync.{next}"))).is_ok() {
                    next += 1;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    };
    // Now every reply to a write waits for its sync, and one is sent.
    assert!(c.cmd(&["CONFIG", "SET", "appendfsync", "always"]).ok());
    c.writer.write_all(SET).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    std::fs::remove_file(&fsync).unwrap();
    // The first hold of a reply's sync is the write's.
    let held = server.dir.join("failpoint.aof-fsync-before-reply.1");
    wait_for(&held, "the reply's sync");
    std::fs::remove_file(&held).unwrap();
    c.writer
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut line = String::new();
    let replied = c.reader.read_line(&mut line);
    assert!(
        replied.is_ok() && line == "+OK\r\n",
        "no reply once the reply's sync was let go ({replied:?}, {line:?}); held again: {}",
        server
            .dir
            .join("failpoint.aof-fsync-before-reply.2")
            .exists()
    );
    c.writer
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    assert_eq!(c.cmd(&["GET", "k"]).s(), "w");
    done.store(true, Ordering::Relaxed);
    releaser.join().unwrap();
}

/// Under `appendfsync always`, a sync of the AOF that fails stops the
/// server (#514's review). The checkpoint's sync of the old log used to be
/// logged and passed over, its mark cleared: Linux reports a failed
/// writeback once to each open file, so the next sync of the log returned
/// success and answered replies whose writes could be lost. Here the old
/// log's sync is made to fail while writers keep going: the server exits,
/// and every write it acknowledged is there after a restart.
#[test]
fn appendfsync_always_stops_at_a_failed_sync() {
    let dir = tempfile::tempdir().unwrap().keep();
    let port = free_port();
    let args = ["--aof", "--appendfsync", "always"];
    let mut child = kividb(&dir, port, &args)
        .env("KIVIDB_TEST_FAILPOINTS", "turn-over-sync-fails")
        .stderr(Stdio::piped())
        .spawn()
        .expect("could not start kividb");
    let stderr = child.stderr.take().unwrap();
    let said = std::thread::spawn(move || {
        let mut said = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut said);
        said
    });
    let mut server = Server { port, dir, child };
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "before", "1"]).ok());

    let writers: Vec<_> = (0..4)
        .map(|w| {
            std::thread::spawn(move || {
                let mut c = Conn::connect(port);
                let key = format!("acked:{w}");
                let mut acked = 0i64;
                while let Some(V::Int(n)) = c.try_cmd(&["INCR", &key]) {
                    acked = n;
                }
                (key, acked)
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(200));
    let _ = c.cmd(&["BGREWRITEAOF"]);

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the server carried on after a failed sync under appendfsync always"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(!status.success(), "{status:?}");
    let acked: Vec<(String, i64)> = writers.into_iter().map(|w| w.join().unwrap()).collect();
    let said = said.join().unwrap();
    assert!(said.contains("failed under appendfsync always"), "{said}");

    server.child = kividb(&server.dir, port, &args)
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");
    let mut c = server.conn();
    assert_eq!(c.cmd(&["GET", "before"]).s(), "1");
    for (key, acked) in acked {
        let kept: i64 = match c.cmd(&["GET", &key]) {
            V::Str(n) => n.parse().unwrap(),
            _ => 0,
        };
        assert!(kept >= acked, "{key} kept {kept}, {acked} acknowledged");
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
    let mut server = Server::start(&["--backup-sealed-ttl", "10"]);
    let mut c = server.conn();
    assert!(c.cmd(&["BACKUP", "START"]).ok());
    wait_for_state(&mut c, "incrementing");
    assert!(c.cmd(&["BACKUP", "SEAL"]).ok());
    drop(c);
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    server.child = kividb(&server.dir, server.port, &["--backup-sealed-ttl", "10"])
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

/// A hash loaded from `dump.kdb` whose fields all expire after the restart
/// is deleted when they do, like one written since: the sweep watches it.
#[test]
fn hashes_loaded_from_a_snapshot_lose_their_expired_fields_and_go() {
    // Long enough that the restart below finishes before the fields are due,
    // on a slow runner too; the wait after it counts from when they were set.
    const TTL_MS: u64 = 10_000;
    let ttl = TTL_MS.to_string();
    let ttl = ttl.as_str();
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    let set_at = std::time::Instant::now();
    c.cmd(&["HSET", "h1", "f1", "v1"]);
    c.cmd(&["HPEXPIRE", "h1", ttl, "FIELDS", "1", "f1"]);
    c.cmd(&["HSET", "h2", "f1", "v1", "f2", "v2"]);
    c.cmd(&["HPEXPIRE", "h2", ttl, "FIELDS", "2", "f1", "f2"]);
    c.cmd(&["HSET", "h3", "f1", "v1", "f2", "v2"]);
    c.cmd(&["HPEXPIRE", "h3", ttl, "FIELDS", "1", "f1"]);
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    server.child = kividb(&server.dir, server.port, &[])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not restart kividb");

    let mut c = server.conn();
    assert!(
        set_at.elapsed() < Duration::from_millis(TTL_MS),
        "the restart took longer than the fields' TTL"
    );
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 3);
    // Past the deadline, plus time for the background sweep (10 a second).
    let due = Duration::from_millis(TTL_MS + 1_500);
    std::thread::sleep(due.saturating_sub(set_at.elapsed()));
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 1);
    assert_eq!(c.cmd(&["HLEN", "h3"]).i(), 1);
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

/// `parts` as a record of the AOF.
fn record(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
    }
    out
}

fn holds(log: &[u8], records: &[&[&str]]) -> bool {
    let wanted: Vec<u8> = records.iter().flat_map(|r| record(r)).collect();
    log.windows(wanted.len()).any(|w| w == wanted.as_slice())
}

/// A transaction's records are in the AOF between a `MULTI` and an `EXEC`,
/// as in Redis, so that a replay can tell a whole one from one cut off by a
/// crash; a script's too. One that wrote once is logged as that one write
/// (#172).
#[test]
fn a_transaction_is_logged_between_multi_and_exec() {
    let server = Server::start(&["--aof"]);
    let mut c = server.conn();
    // The stream's first write says which database it is for.
    assert!(c.cmd(&["SET", "first", "0"]).ok());
    assert!(c.cmd(&["MULTI"]).ok());
    c.cmd(&["SET", "x", "1"]);
    c.cmd(&["GET", "x"]);
    c.cmd(&["SET", "y", "2"]);
    assert_eq!(c.cmd(&["EXEC"]).arr().len(), 3);
    assert!(c.cmd(&["MULTI"]).ok());
    c.cmd(&["SET", "z", "3"]);
    assert_eq!(c.cmd(&["EXEC"]).arr().len(), 1);
    #[cfg(feature = "lua")]
    {
        c.cmd(&[
            "EVAL",
            "redis.call('SET', KEYS[1], 'a') return redis.call('SET', KEYS[2], 'b')",
            "2",
            "s1",
            "s2",
        ]);
        c.cmd(&["EVAL", "return redis.call('SET', KEYS[1], 'c')", "1", "s3"]);
    }
    assert!(c.cmd(&["SET", "last", "4"]).ok());

    // Acknowledged after it is written: the file holds it now.
    let log = std::fs::read(server.dir.join("appendonly.aof")).unwrap();
    assert!(
        holds(
            &log,
            &[
                &["MULTI"],
                &["SET", "x", "1"],
                &["SET", "y", "2"],
                &["EXEC"]
            ]
        ),
        "{}",
        String::from_utf8_lossy(&log)
    );
    let multis = log
        .windows(record(&["MULTI"]).len())
        .filter(|w| *w == record(&["MULTI"]).as_slice())
        .count();
    #[cfg(feature = "lua")]
    {
        assert!(holds(
            &log,
            &[
                &["MULTI"],
                &["SET", "s1", "a"],
                &["SET", "s2", "b"],
                &["EXEC"]
            ]
        ));
        assert!(holds(&log, &[&["SET", "s3", "c"], &["SET", "last", "4"]]));
        assert_eq!(multis, 2, "{}", String::from_utf8_lossy(&log));
    }
    #[cfg(not(feature = "lua"))]
    assert_eq!(multis, 1, "{}", String::from_utf8_lossy(&log));
    assert!(holds(&log, &[&["EXEC"], &["SET", "z", "3"]]));
}

/// A transaction cut off at the end of the AOF â€” its `EXEC` never written â€”
/// is dropped whole and cut off the file, as Redis reverts it; everything
/// before it is kept. Whether the cut fell between two of its records or in
/// the middle of one (#172).
#[test]
fn a_transaction_cut_off_at_the_end_of_the_aof_is_dropped_whole() {
    for torn in [false, true] {
        let mut server = Server::start(&["--aof"]);
        let mut c = server.conn();
        for i in 0..10 {
            assert!(c.cmd(&["SET", &format!("k{i}"), "v"]).ok());
        }
        assert!(c.cmd(&["MULTI"]).ok());
        c.cmd(&["SET", "whole1", "v"]);
        c.cmd(&["SET", "whole2", "v"]);
        assert_eq!(c.cmd(&["EXEC"]).arr().len(), 2);
        drop(c);
        std::thread::sleep(Duration::from_millis(1500));
        server.child.kill().unwrap();
        server.child.wait().unwrap();

        let aof = server.dir.join("appendonly.aof");
        let mut log = std::fs::read(&aof).unwrap();
        for r in [&["MULTI"][..], &["SET", "cut1", "v"], &["SET", "cut2", "v"]] {
            log.extend_from_slice(&record(r));
        }
        if torn {
            log.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\ncu");
        }
        std::fs::write(&aof, &log).unwrap();

        let mut c = restart_with_aof(&mut server);
        assert_eq!(c.cmd(&["DBSIZE"]).i(), 12, "torn {torn}");
        assert_eq!(c.cmd(&["EXISTS", "cut1", "cut2"]).i(), 0, "torn {torn}");
        assert_eq!(c.cmd(&["EXISTS", "whole1", "whole2"]).i(), 2, "torn {torn}");
        assert!(c.cmd(&["SET", "after", "v"]).ok());
        drop(c);
        std::thread::sleep(Duration::from_millis(1500));
        server.child.kill().unwrap();
        server.child.wait().unwrap();
        let log = std::fs::read(&aof).unwrap();
        assert!(
            !log.windows(3).any(|w| w == b"cut"),
            "torn {torn}: the unfinished transaction was left in the file"
        );

        // What was written after it follows whole transactions: this loads,
        // and has nothing to revert.
        let mut c = restart_with_aof(&mut server);
        assert_eq!(c.cmd(&["DBSIZE"]).i(), 13, "torn {torn}");
        assert_eq!(c.cmd(&["GET", "after"]).s(), "v");
    }
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
    // Nothing to stop: nil, as in Redis 8.10.
    assert_eq!(c.cmd(&["HOTKEYS", "STOP"]), V::Nil);
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

// ── The AOF off: no file made, and none needed (#47) ─────────────────────────

/// Start a server in `dir`, prepared by the test, and wait for it to listen.
/// One that exits instead fails the test with what it said.
fn start_in(dir: &Path, extra: &[&str]) -> Server {
    let port = free_port();
    let mut child = kividb(dir, port, extra)
        .stderr(Stdio::piped())
        .spawn()
        .expect("could not start kividb");
    let mut stderr = child.stderr.take().unwrap();
    let said = std::thread::spawn(move || {
        let mut said = String::new();
        let _ = stderr.read_to_string(&mut said);
        said
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "kividb exited ({status}) instead of starting: {}",
                said.join().unwrap()
            );
        }
        assert!(Instant::now() < deadline, "kividb never listened on {port}");
        std::thread::sleep(Duration::from_millis(50));
    }
    Server {
        port,
        dir: dir.to_path_buf(),
        child,
    }
}

/// Make `dir` read-only, or writable again. A no-op off unix, and for root,
/// which writes anyway: the tests that need a failure also put a directory
/// where the log would go.
fn set_read_only(dir: &Path, read_only: bool) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if read_only { 0o555 } else { 0o755 };
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    #[cfg(not(unix))]
    let _ = (dir, read_only);
}

/// With `appendonly no`, a server makes no `appendonly.aof`, and starts in a
/// data directory it cannot write to: the wrong owner, a full disk, a
/// hardened systemd unit. Every start used to create the log, and one that
/// could not stopped the server.
#[test]
fn the_aof_off_makes_no_log_and_starts_in_a_read_only_directory() {
    let dir = tempfile::tempdir().unwrap().keep();
    set_read_only(&dir, true);
    let server = start_in(&dir, &[]);
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "k", "v"]).ok());
    assert_eq!(c.cmd(&["GET", "k"]).s(), "v");
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "appendonly"]).strings(),
        ["appendonly", "no"]
    );
    set_read_only(&dir, false);
    assert!(
        !dir.join("appendonly.aof").exists(),
        "appendonly.aof was made with the AOF off"
    );
}

/// The AOF turned on with `CONFIG SET` makes its log then, and keeps what
/// was written before and after across a restart that loads it.
#[test]
fn the_aof_turned_on_at_runtime_keeps_the_writes_across_a_restart() {
    let mut server = Server::start(&[]);
    let aof = server.dir.join("appendonly.aof");
    let mut c = server.conn();
    assert!(c.cmd(&["SET", "before", "1"]).ok());
    assert!(!aof.exists(), "appendonly.aof was made with the AOF off");
    assert!(c.cmd(&["CONFIG", "SET", "appendonly", "yes"]).ok());
    assert!(aof.exists(), "CONFIG SET appendonly yes made no log");
    assert!(c.cmd(&["SET", "after", "2"]).ok());
    drop(c);
    // Killed: what is kept is what the log and the snapshot hold.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    let mut c = restart_with_aof(&mut server);
    assert_eq!(c.cmd(&["GET", "before"]).s(), "1");
    assert_eq!(c.cmd(&["GET", "after"]).s(), "2");
}

/// Turned on where its log cannot be made, the AOF is an error reply, as in
/// Redis, not an exit: it stays off, the server goes on serving, and it can
/// be turned on once the log can be made.
#[test]
fn the_aof_turned_on_where_no_log_can_be_made_is_an_error_not_an_exit() {
    let dir = tempfile::tempdir().unwrap().keep();
    // A directory where the log would go fails the open for root too.
    let blocker = dir.join("appendonly.aof");
    std::fs::create_dir(&blocker).unwrap();
    set_read_only(&dir, true);
    let server = start_in(&dir, &[]);
    let mut c = server.conn();
    let reply = c.cmd(&["CONFIG", "SET", "appendonly", "yes"]);
    assert!(
        reply.is_err() && reply.s().contains("Unable to turn on AOF"),
        "{reply:?}"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "appendonly"]).strings(),
        ["appendonly", "no"]
    );
    assert!(c.cmd(&["SET", "k", "v"]).ok());
    assert_eq!(c.cmd(&["GET", "k"]).s(), "v");

    set_read_only(&dir, false);
    std::fs::remove_dir(&blocker).unwrap();
    assert!(c.cmd(&["CONFIG", "SET", "appendonly", "yes"]).ok());
    assert!(c.cmd(&["SET", "logged", "1"]).ok());
    let log = std::fs::read(&blocker).unwrap();
    assert!(
        log.windows(6).any(|w| w == b"logged"),
        "the write after turning the AOF on is not in the log"
    );
}

/// Started with the AOF on and a log it cannot open, the server still
/// exits, before it serves: it would take writes and keep none of them. The
/// log is set up before the writer's thread is spawned now, not in it.
#[cfg(unix)]
#[test]
fn the_aof_on_with_a_log_that_cannot_be_opened_still_exits() {
    let dir = tempfile::tempdir().unwrap().keep();
    // A link into a directory that is not there: to recovery there is no
    // log, and none can be made through it, for root too.
    std::os::unix::fs::symlink(
        dir.join("missing").join("appendonly.aof"),
        dir.join("appendonly.aof"),
    )
    .unwrap();
    let output = std::fs::File::create(dir.join("server.log")).unwrap();
    let port = free_port();
    let mut child = kividb(&dir, port, &["--aof"])
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .expect("could not start kividb");
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("kividb went on with a log it cannot open");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let said = std::fs::read_to_string(dir.join("server.log")).unwrap();
    assert!(!status.success(), "{status}: {said}");
    assert!(
        said.contains("appendonly.aof cannot be opened for appending"),
        "{said}"
    );
    assert!(
        TcpStream::connect(("127.0.0.1", port)).is_err(),
        "something listens on {port}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A backup into the data directory would share its files' prefix with the
/// server's own, and `BACKUP CLEANUP` would delete `appendonly.aof.prev` with
/// them. It is refused.
#[test]
fn a_backup_into_the_data_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::write(dir.join("kividb.conf"), "backupdirname .\n").unwrap();
    let server = start_in(&dir, &[]);
    let mut c = server.conn();
    let reply = c.cmd(&["BACKUP", "START"]);
    assert!(
        matches!(&reply, V::Err(e) if e.contains("is the data directory")),
        "{reply:?}"
    );
}

/// Every file of `dir` and what it holds.
fn directory(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            (name, std::fs::read(entry.path()).unwrap_or_default())
        })
        .collect();
    files.sort();
    files
}

/// Whether a server started in `dir` with `args` exited with an error within
/// 30 s (one that starts is stopped), and what it logged.
fn fails_to_start(dir: &Path, args: &[&str]) -> (bool, String) {
    let mut child = kividb(dir, free_port(), args)
        .stderr(Stdio::piped())
        .spawn()
        .expect("could not run kividb");
    let deadline = Instant::now() + Duration::from_secs(30);
    let failed = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break !status.success();
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break false;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    let _ = child.stderr.take().unwrap().read_to_string(&mut stderr);
    (failed, stderr)
}

/// A preload target that is not a whole snapshot is fatal, and the data
/// directory is left exactly as it was: the same files, holding the same
/// bytes (#444).
///
/// A Dragonfly snapshot in Dragonfly's own format (a summary with no keys,
/// and a file for each shard) started the server empty: "preload-file: 0
/// keys from the base snapshot". Its files are refused by name (`.dfs`),
/// from every Dragonfly version; renamed, by the fields Dragonfly 1.32 and
/// later write in them, and a summary from before 1.32 as a Dragonfly file
/// with no keys. The fixtures are from Dragonfly 2.1, 1.31 and 1.15. A cut
/// file, one with a byte changed, and a KiviDB snapshot cut short loaded as
/// far as they went.
#[test]
fn a_preload_file_that_is_not_a_whole_snapshot_is_fatal() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let fixture = |name: &str| std::fs::read(fixtures.join(name)).unwrap();
    let source = Server::start(&[]);
    let mut c = source.conn();
    for i in 0..50 {
        c.cmd(&["SET", &format!("key:{i}"), &"v".repeat(100)]);
    }
    assert!(c.cmd(&["SAVE"]).ok());
    let kdb = std::fs::read(source.dir.join("dump.kdb")).unwrap();
    let rdb = fixture("redis-8.10.rdb");
    let mut flipped = rdb.clone();
    let at = flipped.windows(5).position(|w| w == b"hello").unwrap();
    flipped[at] = b'j';

    // Each target, and what the refusal says.
    let targets: Vec<(&str, Vec<u8>, &str)> = vec![
        // As Dragonfly names them.
        (
            "dump-2026-11-06T01:48:42-summary.dfs",
            fixture("dragonfly-2.1-summary.dfs"),
            "Dragonfly",
        ),
        (
            "dump-2026-11-06T01:48:42-0000.dfs",
            fixture("dragonfly-2.1-0000.dfs"),
            "Dragonfly",
        ),
        (
            "dump-2026-11-06T01:48:17-summary.dfs",
            fixture("dragonfly-1.31-summary.dfs"),
            "Dragonfly",
        ),
        (
            "dump-2026-11-06T01:48:17-0000.DFS",
            fixture("dragonfly-1.31-0000.dfs"),
            "Dragonfly",
        ),
        (
            "dump-2026-11-06T01:47:48-summary.dfs",
            fixture("dragonfly-1.15-summary.dfs"),
            "Dragonfly",
        ),
        // Renamed: by what is in them.
        (
            "summary-2.1.rdb",
            fixture("dragonfly-2.1-summary.dfs"),
            "Dragonfly",
        ),
        (
            "shard-2.1.rdb",
            fixture("dragonfly-2.1-0000.dfs"),
            "Dragonfly",
        ),
        (
            "summary-1.31.rdb",
            fixture("dragonfly-1.31-summary.dfs"),
            "Dragonfly",
        ),
        (
            "summary-1.15.rdb",
            fixture("dragonfly-1.15-summary.dfs"),
            "could not be loaded",
        ),
        ("empty", Vec::new(), "could not be loaded"),
        ("cut.rdb", rdb[..rdb.len() - 8].to_vec(), "checksum"),
        ("damaged.rdb", flipped, "rdbchecksum no"),
        (
            "cut.kdb",
            kdb[..kdb.len() / 2].to_vec(),
            "could not be loaded",
        ),
        ("header.kdb", kdb[..12].to_vec(), "cut short"),
    ];
    // The data directory holds a dump of its own and the lock file a server
    // makes, all of which must stay as they are.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(data.join("dump.rdb"), b"REDIS0011\xFF\0\0\0\0\0\0\0\0").unwrap();
    std::fs::write(data.join("kividb.lock"), b"").unwrap();
    let before = directory(&data);
    for (name, bytes, says) in targets {
        let file = dir.path().join(name);
        std::fs::write(&file, bytes).unwrap();
        let target = format!("rdb:{}", file.display());
        let (failed, stderr) = fails_to_start(&data, &["--preload-file", &target]);
        assert!(failed, "{name} must not start: {stderr}");
        assert!(stderr.contains("could not be loaded"), "{name}: {stderr}");
        assert!(stderr.contains(says), "{name}: {stderr}");
        assert!(
            directory(&data) == before,
            "{name} changed the data directory"
        );
    }

    // A Dragonfly shard's file as `dump.rdb` does not start the server on
    // that shard's keys either.
    std::fs::write(data.join("dump.rdb"), fixture("dragonfly-2.1-0000.dfs")).unwrap();
    let before = directory(&data);
    let (failed, stderr) = fails_to_start(&data, &[]);
    assert!(failed, "{stderr}");
    assert!(stderr.contains("Dragonfly"), "{stderr}");
    assert!(
        directory(&data) == before,
        "dump.rdb changed the data directory"
    );
}

/// `rdbchecksum no` skips the check of an RDB file's checksum as it is
/// loaded, as in Redis, so that a file whose only fault is its checksum can
/// still be loaded on purpose (#444).
#[test]
fn rdbchecksum_no_loads_a_file_whose_checksum_is_wrong() {
    let mut damaged =
        std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/redis-8.10.rdb"))
            .unwrap();
    let at = damaged.windows(5).position(|w| w == b"hello").unwrap();
    damaged[at] = b'j';
    let dir = tempfile::tempdir().unwrap().keep();
    let file = dir.join("damaged.rdb");
    std::fs::write(&file, damaged).unwrap();
    std::fs::write(dir.join("kividb.conf"), "rdbchecksum no\n").unwrap();
    let port = free_port();
    let target = format!("rdb:{}", file.display());
    let child = kividb(&dir, port, &["--preload-file", &target])
        .stderr(Stdio::null())
        .spawn()
        .expect("could not start kividb");
    let server = Server { port, dir, child };
    let mut c = server.conn();
    assert_eq!(c.cmd(&["DBSIZE"]).i(), 9);
    let reply = c.cmd(&["CONFIG", "GET", "rdbchecksum"]);
    assert_eq!(reply.arr()[1].s(), "no");
}

// ── The empty key (#441) ─────────────────────────────────────────────────────
//
// Redis allows the empty key. In a snapshot it was written with the key
// length that ends a database, and the next start failed: "dump.kdb cannot
// be loaded: failed to fill whole buffer".

/// Stop `server` with no warning and start another in its directory, as
/// `extra` says. One that does not start fails the test with what it said.
fn killed_and_started_again(server: &mut Server, extra: &[&str]) -> Server {
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    start_in(&server.dir, extra)
}

/// The empty key of each kind of value, and empty members and fields.
fn write_empty_keys(c: &mut Conn) {
    assert!(c.cmd(&["SET", "", "x", "EX", "100000"]).ok());
    assert_eq!(c.cmd(&["HSET", "hash", "", "v"]).i(), 1);
    assert_eq!(c.cmd(&["SADD", "set", ""]).i(), 1);
    assert_eq!(c.cmd(&["ZADD", "zset", "1.5", ""]).i(), 1);
    assert_eq!(c.cmd(&["RPUSH", "list", "", "e"]).i(), 2);
    assert!(c.cmd(&["SELECT", "1"]).ok());
    assert_eq!(c.cmd(&["HSET", "", "", "in-1"]).i(), 1);
    assert_eq!(c.cmd(&["EXPIRE", "", "200000"]).i(), 1);
    assert!(c.cmd(&["SET", "after", "1"]).ok());
    assert!(c.cmd(&["SELECT", "0"]).ok());
}

fn assert_empty_keys(c: &mut Conn) {
    assert_eq!(c.cmd(&["GET", ""]).s(), "x");
    let ttl = c.cmd(&["TTL", ""]).i();
    assert!((1..=100000).contains(&ttl), "TTL of the empty key: {ttl}");
    assert_eq!(c.cmd(&["HGET", "hash", ""]).s(), "v");
    assert_eq!(c.cmd(&["SISMEMBER", "set", ""]).i(), 1);
    assert_eq!(c.cmd(&["ZSCORE", "zset", ""]).s(), "1.5");
    assert_eq!(c.cmd(&["LRANGE", "list", "0", "-1"]).strings(), ["", "e"]);
    assert!(c.cmd(&["SELECT", "1"]).ok());
    assert_eq!(c.cmd(&["HGET", "", ""]).s(), "in-1");
    let ttl = c.cmd(&["TTL", ""]).i();
    assert!(
        (100001..=200000).contains(&ttl),
        "TTL of the empty key in database 1: {ttl}"
    );
    assert_eq!(c.cmd(&["GET", "after"]).s(), "1");
    assert!(c.cmd(&["SELECT", "0"]).ok());
}

#[test]
fn the_empty_key_survives_a_save_and_a_restart() {
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    write_empty_keys(&mut c);
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    // Written under the stand-in length: the stand-in, the string tag, a
    // length of 1 and the value. The server also reads 1.2.0's layout, so
    // a restart alone would not show the writer had gone back to it.
    let dump = std::fs::read(server.dir.join("dump.kdb")).unwrap();
    let entry = [0xFF, 0xFF, 0xFF, 0xFF, 1, 1, 0, 0, 0, b'x'];
    assert!(
        dump.windows(entry.len()).any(|w| w == entry),
        "the empty key is not under the stand-in length"
    );
    let restarted = killed_and_started_again(&mut server, &[]);
    let mut c = restarted.conn();
    assert_empty_keys(&mut c);
    // And the image the restarted server writes loads again.
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    let mut restarted = restarted;
    let again = killed_and_started_again(&mut restarted, &[]);
    assert_empty_keys(&mut again.conn());
}

/// With the AOF on, a `SAVE` is a checkpoint: the snapshot it writes is
/// where the next start begins, and the log after it is replayed on top.
#[test]
fn the_empty_key_survives_an_aof_checkpoint_and_a_restart() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    write_empty_keys(&mut c);
    assert!(c.cmd(&["SAVE"]).ok());
    assert!(c.cmd(&["SELECT", "2"]).ok());
    assert!(c.cmd(&["SET", "", "in-the-log"]).ok());
    drop(c);
    std::thread::sleep(Duration::from_millis(1500));
    let restarted = killed_and_started_again(&mut server, &["--aof"]);
    let mut c = restarted.conn();
    assert_empty_keys(&mut c);
    assert!(c.cmd(&["SELECT", "2"]).ok());
    assert_eq!(c.cmd(&["GET", ""]).s(), "in-the-log");
}

/// `MIGRATE` of the empty key, a hash with an empty field, named after
/// `KEYS`, to another server.
#[test]
fn the_empty_key_can_be_migrated() {
    let from = Server::start(&[]);
    let to = Server::start(&[]);
    let mut c = from.conn();
    assert_eq!(c.cmd(&["HSET", "", "", "v"]).i(), 1);
    let port = to.port.to_string();
    let reply = c.cmd(&["MIGRATE", "127.0.0.1", &port, "", "0", "5000", "KEYS", ""]);
    assert!(reply.ok(), "MIGRATE: {reply:?}");
    assert_eq!(c.cmd(&["EXISTS", ""]).i(), 0);
    assert_eq!(to.conn().cmd(&["HGET", "", ""]).s(), "v");
}

/// A `dump.kdb` as 1.2.0 wrote it, with the empty key under the length that
/// ends a database: `databases` databases, the empty key in `at`, and every
/// key with a time to live.
fn dump_1_2_0(databases: usize, at: usize) -> Vec<u8> {
    use kividb::shared_bytes::SharedBytes;
    use kividb::types::KiviDBValue;
    let string = |v: &str| KiviDBValue::String(SharedBytes::from(v.as_bytes()));
    let far = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 100_000_000;
    let mut image = Vec::new();
    kividb::snapshot::write_kdb_header(&mut image, databases as u32).unwrap();
    let mut expiries = Vec::new();
    for db in 0..databases {
        let mut keys = Vec::new();
        let before = format!("before-{db}");
        kividb::snapshot::write_kdb_entry(&mut image, &before, string("1").view()).unwrap();
        keys.push(before);
        if db == at {
            image.extend(0u32.to_le_bytes());
            kividb::snapshot::write_kdb_value(&mut image, string("x").view()).unwrap();
            keys.push(String::new());
        }
        let after = format!("after-{db}");
        kividb::snapshot::write_kdb_entry(&mut image, &after, string("2").view()).unwrap();
        keys.push(after);
        kividb::snapshot::write_kdb_db_end(&mut image).unwrap();
        expiries.push(keys);
    }
    // The expiry section, as `write_kdb_expiry_maps` lays it out.
    image.extend(b"KIVIEX1\0");
    image.extend((databases as u32).to_le_bytes());
    for keys in expiries {
        image.extend((keys.len() as u32).to_le_bytes());
        for key in keys {
            image.extend((key.len() as u32).to_le_bytes());
            image.extend(key.as_bytes());
            image.extend(far.to_le_bytes());
        }
    }
    image
}

/// 1.2.0 wrote the empty key with the length that ends a database. With it
/// in the first of 16 databases neither 1.2.0 nor 1.2.1 started; with it in
/// the only database, both started without the keys after it and without
/// any time to live. Each such file now loads whole.
#[test]
fn a_1_2_0_dump_with_the_empty_key_loads_whole() {
    for (databases, at) in [(16, 0), (16, 7), (16, 15), (1, 0)] {
        let dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(dir.join("dump.kdb"), dump_1_2_0(databases, at)).unwrap();
        let databases_arg = databases.to_string();
        let server = start_in(&dir, &["--databases", &databases_arg]);
        let mut c = server.conn();
        for db in 0..databases {
            assert!(c.cmd(&["SELECT", &db.to_string()]).ok());
            let mut keys = vec![format!("before-{db}"), format!("after-{db}")];
            if db == at {
                assert_eq!(
                    c.cmd(&["GET", ""]).s(),
                    "x",
                    "{databases} databases, at {at}"
                );
                keys.push(String::new());
            }
            assert_eq!(c.cmd(&["DBSIZE"]).i(), keys.len() as i64, "database {db}");
            for key in keys {
                let ttl = c.cmd(&["TTL", &key]).i();
                assert!(
                    ttl > 99_000,
                    "{databases} databases, at {at}: TTL of {key:?} in {db}: {ttl}"
                );
            }
        }
    }
}

/// An `appendonly.aof` that opens with a KDB preamble, as earlier releases
/// wrote them, with the empty key in the preamble.
#[test]
fn the_empty_key_in_an_aof_preamble_loads() {
    use kividb::shared_bytes::SharedBytes;
    use kividb::types::KiviDBValue;
    let string = |v: &str| KiviDBValue::String(SharedBytes::from(v.as_bytes()));
    let db0 = std::collections::HashMap::from([
        (String::new(), string("x")),
        ("k".to_string(), string("1")),
    ]);
    let mut image = Vec::new();
    kividb::snapshot::write_kdb_image(&mut image, &[db0], None).unwrap();
    let mut log = b"KIVIAOF1".to_vec();
    log.push(1); // a preamble follows
    log.extend((image.len() as u64).to_le_bytes());
    log.extend(&image);
    log.extend(record(&["SET", "tail", "2"]));
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::write(dir.join("appendonly.aof"), &log).unwrap();
    let server = start_in(&dir, &["--aof"]);
    let mut c = server.conn();
    assert_eq!(c.cmd(&["GET", ""]).s(), "x");
    assert_eq!(c.cmd(&["GET", "k"]).s(), "1");
    assert_eq!(c.cmd(&["GET", "tail"]).s(), "2");
}
