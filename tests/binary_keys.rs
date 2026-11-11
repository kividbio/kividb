// tests/binary_keys.rs
//
// Keys that are not text (#437): every command takes one, and a Redis RDB
// file can hold one (`--preload-file`). Everything that reads the keyspace
// back has to carry its bytes: SCAN and KEYS, expiry and the `DEL` it sends to replicas, eviction,
// MIGRATE, a snapshot and the full sync made from it. Before, the store
// handed such a key back through `from_utf8_unchecked`, and what read it as
// text lost it, or stopped the thread that read it.
//
// Real `kividb` processes. The I/O model is the platform's own unless
// `KIVIDB_TEST_IO_MODEL` says.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

// ── RESP client, of bytes ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn bytes(&self) -> &[u8] {
        match self {
            V::Str(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }
    fn int(&self) -> i64 {
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

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).expect("reply");
        let line = String::from_utf8_lossy(&line).trim_end().to_string();
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.as_bytes().to_vec()),
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
                V::Str(data)
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

    /// DBSIZE, or -1 while the server answers LOADING.
    fn dbsize(&mut self) -> i64 {
        match self.cmd(&[b"DBSIZE"]) {
            V::Int(n) => n,
            _ => -1,
        }
    }

    /// Every key, by `SCAN`, sorted.
    fn scan_all(&mut self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let mut cursor = b"0".to_vec();
        loop {
            let mut args: Vec<&[u8]> = vec![b"SCAN", &cursor, b"COUNT", b"1000"];
            if let Some(pattern) = pattern {
                args.extend_from_slice(&[b"MATCH", pattern]);
            }
            let reply = self.cmd(&args);
            let reply = reply.arr();
            keys.extend(reply[1].arr().iter().map(|k| k.bytes().to_vec()));
            cursor = reply[0].bytes().to_vec();
            if cursor == b"0" {
                break;
            }
        }
        keys.sort();
        keys
    }

    /// One field of `INFO <section>`.
    fn info(&mut self, section: &str, field: &str) -> String {
        let reply = self.cmd(&[b"INFO", section.as_bytes()]);
        String::from_utf8_lossy(reply.bytes())
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")).map(str::to_string))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .trim()
            .to_string()
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

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

/// A port nothing listens on, outside the range handed to outgoing
/// connections.
fn free_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(43);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
    args: Vec<String>,
}

fn spawn(dir: &Path, port: u16, args: &[String]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
    command.current_dir(dir).args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--threads",
        "2",
    ]);
    if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
        command.args(["--io-model", &model]);
    }
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

impl Server {
    fn start(args: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        Self::start_in(dir, args)
    }

    fn start_in(dir: PathBuf, args: &[&str]) -> Self {
        let port = free_port();
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let child = spawn(&dir, port, &args);
        let server = Server {
            port,
            dir,
            child,
            args,
        };
        server.wait_up();
        server
    }

    fn wait_up(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let up = TcpStream::connect(("127.0.0.1", self.port)).and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(10)))?;
                // DBSIZE, not PING: PING is answered while the dataset is
                // still loading, and the tests read the data straight away.
                s.write_all(b"*1\r\n$6\r\nDBSIZE\r\n")?;
                let mut reply = [0u8; 1];
                s.read_exact(&mut reply)?;
                Ok(reply[0] == b':')
            });
            if up.unwrap_or(false) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "kividb did not come up: {}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    /// Stopped (killed), and started again on the same directory.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let args: Vec<String> = self
            .args
            .iter()
            .filter(|a| !a.starts_with("rdb:") && *a != "--preload-file")
            .cloned()
            .collect();
        self.args = args;
        self.child = spawn(&self.dir, self.port, &self.args);
        self.wait_up();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Wait, at most `secs` seconds, for `ok`.
fn eventually(secs: u64, what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ── A Redis RDB file ──────────────────────────────────────────────────────────

fn rdb_len(n: usize) -> Vec<u8> {
    if n < 64 {
        vec![n as u8]
    } else if n < 16384 {
        vec![0x40 | (n >> 8) as u8, n as u8]
    } else {
        let mut v = vec![0x80];
        v.extend_from_slice(&(n as u32).to_be_bytes());
        v
    }
}

fn rdb_str(s: &[u8]) -> Vec<u8> {
    let mut v = rdb_len(s.len());
    v.extend_from_slice(s);
    v
}

/// An RDB file of strings in database 0, each with its expiry time (in
/// milliseconds since the epoch) if it has one.
fn rdb(keys: &[(&[u8], &[u8], Option<u64>)]) -> Vec<u8> {
    let mut out = b"REDIS0011".to_vec();
    out.push(0xFE);
    out.extend_from_slice(&rdb_len(0));
    for (key, value, expires) in keys {
        if let Some(ms) = expires {
            out.push(0xFC);
            out.extend_from_slice(&ms.to_le_bytes());
        }
        out.push(0); // a string
        out.extend_from_slice(&rdb_str(key));
        out.extend_from_slice(&rdb_str(value));
    }
    out.push(0xFF);
    out.extend_from_slice(&[0u8; 8]); // no checksum
    out
}

fn preloaded(file: &[u8], args: &[&str]) -> Server {
    let dir = tempfile::tempdir().unwrap().keep();
    std::fs::write(dir.join("seed.rdb"), file).unwrap();
    let mut all = vec!["--preload-file", "rdb:seed.rdb"];
    all.extend_from_slice(args);
    Server::start_in(dir, &all)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

const A: &[u8] = b"bin:\xff\xfe";
const B: &[u8] = b"bin:\xfe\xff";
const T: &[u8] = b"ttl:\xff\x00";

/// Two keys that are not text, which used to load as one (`bin:` and two
/// U+FFFD), and a third that expires: loaded, scanned, matched, expired with
/// a `DEL` its replica applies, saved and loaded again.
#[test]
fn keys_that_are_not_text_are_scanned_expired_and_kept() {
    let file = rdb(&[
        (A, b"a", None),
        (B, b"b", None),
        (T, b"t", Some(now_ms() + 8_000)),
        (b"text", b"x", None),
    ]);
    let mut master = preloaded(&file, &[]);
    let mut c = master.conn();
    assert_eq!(c.dbsize(), 4, "{}", master.log());
    let mut want: Vec<Vec<u8>> = [A, B, T, b"text"].iter().map(|k| k.to_vec()).collect();
    want.sort();
    assert_eq!(c.scan_all(None), want);
    let keys = c.cmd(&[b"KEYS", b"*"]);
    let mut keys: Vec<Vec<u8>> = keys.arr().iter().map(|k| k.bytes().to_vec()).collect();
    keys.sort();
    assert_eq!(keys, want);
    // A pattern of any bytes: `?` is one byte, not one U+FFFD.
    assert_eq!(c.scan_all(Some(b"bin:\xff*")), vec![A.to_vec()]);
    assert_eq!(c.scan_all(Some(b"bin:??")), {
        let mut both = vec![A.to_vec(), B.to_vec()];
        both.sort();
        both
    });
    let keys = c.cmd(&[b"KEYS", b"ttl:\xff?"]);
    assert_eq!(keys.arr().len(), 1);

    // A replica's full sync carries them.
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    eventually(20, "the replica's full sync", || r.dbsize() == 4);
    assert_eq!(r.scan_all(None), want);

    // The key that expires goes, on the master by its own clock and on the
    // replica by the master's DEL; neither stops on a key that is not text.
    eventually(20, "the key to expire", || c.dbsize() == 3);
    eventually(20, "the replica to apply the DEL", || r.dbsize() == 3);
    let mut left: Vec<Vec<u8>> = [A, B, b"text"].iter().map(|k| k.to_vec()).collect();
    left.sort();
    assert_eq!(c.scan_all(None), left);
    assert_eq!(r.scan_all(None), left);
    assert_eq!(c.info("stats", "expired_keys"), "1");

    // Saved, and loaded from the snapshot.
    assert_eq!(c.cmd(&[b"SAVE"]).bytes(), b"OK");
    drop(c);
    master.restart();
    let mut c = master.conn();
    assert_eq!(c.scan_all(None), left, "{}", master.log());
}

/// Keys that are not text are evicted like any other, with their `DEL` sent
/// to the replica.
#[test]
fn keys_that_are_not_text_are_evicted() {
    let value = vec![b'v'; 1024];
    let names: Vec<Vec<u8>> = (0..2_000u32)
        .map(|i| {
            let mut k = b"evict:\xff".to_vec();
            k.extend_from_slice(&i.to_be_bytes());
            k
        })
        .collect();
    let keys: Vec<(&[u8], &[u8], Option<u64>)> = names
        .iter()
        .map(|k| (k.as_slice(), value.as_slice(), None))
        .collect();
    let master = preloaded(&rdb(&keys), &["--maxmemory-policy", "allkeys-random"]);
    let mut c = master.conn();
    assert_eq!(c.dbsize(), 2_000, "{}", master.log());
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    eventually(20, "the replica's full sync", || r.dbsize() == 2_000);

    let used: u64 = c.info("memory", "used_memory").parse().unwrap();
    let limit = (used - 1_000_000).to_string();
    assert_eq!(
        c.cmd(&[b"CONFIG", b"SET", b"maxmemory", limit.as_bytes()])
            .bytes(),
        b"OK"
    );
    // A write makes room: keys that are not text go.
    for i in 0..50u32 {
        let reply = c.cmd(&[b"SET", format!("new:{i}").as_bytes(), &value]);
        assert!(matches!(reply, V::Str(_)), "{reply:?}");
    }
    let evicted: u64 = c.info("stats", "evicted_keys").parse().unwrap();
    assert!(evicted > 0, "nothing was evicted");
    // Equal counts can hide different keys while eviction and its DELs
    // are still in flight. Wait for the actual key sets to converge (#500).
    eventually(20, "the replica to apply the evictions", || {
        r.scan_all(None) == c.scan_all(None)
    });
}

/// MIGRATE of a key that is not text: it was skipped, and the command
/// answered OK; then the destination's `RESTORE` refused it. It moves, with
/// its value and its time to live, as any other key does.
#[test]
fn migrate_moves_a_key_that_is_not_text() {
    let later = now_ms() + 600_000;
    let source = preloaded(&rdb(&[(A, b"a", None), (B, b"b", Some(later))]), &[]);
    let destination = Server::start(&[]);
    let mut c = source.conn();
    let port = destination.port.to_string();
    let reply = c.cmd(&[
        b"MIGRATE",
        b"127.0.0.1",
        port.as_bytes(),
        b"",
        b"0",
        b"5000",
        b"KEYS",
        A,
        B,
    ]);
    assert_eq!(reply, V::Str(b"OK".to_vec()));
    assert_eq!(c.cmd(&[b"DBSIZE"]).int(), 0);
    let mut d = destination.conn();
    assert_eq!(d.cmd(&[b"GET", A]).bytes(), b"a");
    assert_eq!(d.cmd(&[b"GET", B]).bytes(), b"b");
    let left = d.cmd(&[b"PTTL", B]).int();
    assert!(left > 500_000 && left <= 600_000, "{left}");
    assert_eq!(d.cmd(&[b"PTTL", A]).int(), -1);
}

// ── Names: the search indexes and the time series ─────────────────────────────
//
// What keeps a key as text keeps its `key_bytes::text_of`, and goes back to
// the key with `bytes_of`. Two kinds of key are what tells a half-applied
// convention from a whole one: a key that is not text, and a UTF-8 key with
// a character of the escape's own block (U+F780 – U+F7FF) in it, which the
// escape writes in another form.

/// A UTF-8 key with a character of the escape's block in it.
const PUA: &str = "doc:\u{F7C3}x";
/// A key that is not text.
const RAW: &[u8] = b"doc:\xff\xfe";

/// A hash: its key and its fields.
type Hash<'a> = (&'a [u8], &'a [(&'a [u8], &'a [u8])]);

/// An RDB file of hashes in database 0.
fn rdb_hashes(keys: &[Hash<'_>]) -> Vec<u8> {
    let mut out = b"REDIS0011".to_vec();
    out.push(0xFE);
    out.extend_from_slice(&rdb_len(0));
    for (key, fields) in keys {
        out.push(4); // a hash
        out.extend_from_slice(&rdb_str(key));
        out.extend_from_slice(&rdb_len(fields.len()));
        for (field, value) in *fields {
            out.extend_from_slice(&rdb_str(field));
            out.extend_from_slice(&rdb_str(value));
        }
    }
    out.push(0xFF);
    out.extend_from_slice(&[0u8; 8]);
    out
}

fn f32s(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// The keys of an `FT.SEARCH … NOCONTENT` reply, sorted.
fn found(reply: &V) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = reply.arr()[1..]
        .iter()
        .map(|k| k.bytes().to_vec())
        .collect();
    keys.sort();
    keys
}

/// A key that is not text and a key with a character of the escape's block
/// are indexed when the index is made (the walk over the keyspace) and when
/// written, are found by a text query and by KNN, are answered with their
/// own bytes, and still are after a restart.
#[test]
fn search_finds_documents_of_any_key() {
    let near = f32s(&[1.0, 0.0]);
    let far = f32s(&[0.0, 1.0]);
    let file = rdb_hashes(&[
        (RAW, &[(b"title", b"hello raw"), (b"vec", &near)]),
        (PUA.as_bytes(), &[(b"title", b"hello pua"), (b"vec", &near)]),
        (b"doc:plain", &[(b"title", b"other"), (b"vec", &far)]),
    ]);
    let mut server = preloaded(&file, &[]);
    let mut c = server.conn();
    let created = c.cmd(&[
        b"FT.CREATE",
        b"idx",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"doc:",
        b"SCHEMA",
        b"title",
        b"TEXT",
        b"vec",
        b"VECTOR",
        b"FLAT",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        b"2",
        b"DISTANCE_METRIC",
        b"L2",
    ]);
    assert_eq!(created, V::Str(b"OK".to_vec()), "{}", server.log());
    // Written after the index was made: indexed by the write.
    let added = "doc:\u{F7C3}new";
    c.cmd(&[
        b"HSET",
        added.as_bytes(),
        b"title",
        b"hello added",
        b"vec",
        &near,
    ]);

    let mut want: Vec<Vec<u8>> = vec![
        RAW.to_vec(),
        PUA.as_bytes().to_vec(),
        added.as_bytes().to_vec(),
    ];
    want.sort();
    let text = |c: &mut Conn| found(&c.cmd(&[b"FT.SEARCH", b"idx", b"hello", b"NOCONTENT"]));
    let knn = |c: &mut Conn| {
        found(&c.cmd(&[
            b"FT.SEARCH",
            b"idx",
            b"*=>[KNN 3 @vec $q]",
            b"PARAMS",
            b"2",
            b"q",
            &near,
            b"NOCONTENT",
            b"DIALECT",
            b"2",
        ]))
    };
    eventually(20, "the walk to index the documents", || {
        text(&mut c) == want
    });
    assert_eq!(knn(&mut c), want);

    assert_eq!(c.cmd(&[b"SAVE"]).bytes(), b"OK");
    drop(c);
    server.restart();
    let mut c = server.conn();
    eventually(20, "the indexes after a restart", || text(&mut c) == want);
    assert_eq!(knn(&mut c), want, "{}", server.log());
}

/// `FT.SEARCH … INKEYS` names a key that is not text, or one with a
/// character of the escape's block, as the document it is: each finds its
/// own document and only it, by a text query and by `*`.
#[test]
fn search_inkeys_of_any_key() {
    let file = rdb_hashes(&[
        (RAW, &[(b"title", b"hello raw")]),
        (PUA.as_bytes(), &[(b"title", b"hello pua")]),
        (b"doc:plain", &[(b"title", b"hello plain")]),
    ]);
    let server = preloaded(&file, &[]);
    let mut c = server.conn();
    let created = c.cmd(&[
        b"FT.CREATE",
        b"idx",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"doc:",
        b"SCHEMA",
        b"title",
        b"TEXT",
    ]);
    assert_eq!(created, V::Str(b"OK".to_vec()), "{}", server.log());
    eventually(20, "the walk to index the documents", || {
        found(&c.cmd(&[b"FT.SEARCH", b"idx", b"hello", b"NOCONTENT"])).len() == 3
    });
    for query in [b"hello".as_ref(), b"*".as_ref()] {
        for key in [RAW, PUA.as_bytes()] {
            let reply = c.cmd(&[
                b"FT.SEARCH",
                b"idx",
                query,
                b"INKEYS",
                b"1",
                key,
                b"NOCONTENT",
            ]);
            assert_eq!(
                found(&reply),
                vec![key.to_vec()],
                "INKEYS {key:?}, query {:?}: {reply:?}",
                String::from_utf8_lossy(query)
            );
        }
    }
}

/// Time series named with a character of the escape's block: the label
/// index finds them (TS.QUERYINDEX, TS.MRANGE), TS.QUERYLABELS checks a
/// user's access to the key they are (`é` and U+F7C3 U+F7A9 are not the
/// same key, though the escape could take them for the same bytes), and a
/// compaction rule writes to its destination before and after a restart.
#[test]
fn time_series_of_any_name() {
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    let open = "\u{e9}1";
    let secret = "\u{F7C3}\u{F7A9}secret";
    for (key, kind) in [(open, "open"), (secret, "secret")] {
        let reply = c.cmd(&[
            b"TS.CREATE",
            key.as_bytes(),
            b"LABELS",
            b"grp",
            b"g",
            b"kind",
            kind.as_bytes(),
        ]);
        assert_eq!(reply, V::Str(b"OK".to_vec()));
        assert_eq!(c.cmd(&[b"TS.ADD", key.as_bytes(), b"1", b"1"]).int(), 1);
    }
    let mut both = vec![open.as_bytes().to_vec(), secret.as_bytes().to_vec()];
    both.sort();
    let index = |c: &mut Conn| {
        let reply = c.cmd(&[b"TS.QUERYINDEX", b"grp=g"]);
        let mut keys: Vec<Vec<u8>> = reply.arr().iter().map(|k| k.bytes().to_vec()).collect();
        keys.sort();
        keys
    };
    let mrange = |c: &mut Conn| {
        let reply = c.cmd(&[b"TS.MRANGE", b"-", b"+", b"FILTER", b"grp=g"]);
        let mut keys: Vec<Vec<u8>> = reply
            .arr()
            .iter()
            .map(|s| s.arr()[0].bytes().to_vec())
            .collect();
        keys.sort();
        keys
    };
    assert_eq!(index(&mut c), both);
    assert_eq!(mrange(&mut c), both);

    // A user who may read `é*` reads the series of `é1`, and not the other.
    assert_eq!(
        c.cmd(&[
            b"ACL",
            b"SETUSER",
            b"alice",
            b"on",
            b"nopass",
            "~\u{e9}*".as_bytes(),
            b"+@all"
        ]),
        V::Str(b"OK".to_vec())
    );
    let mut a = server.conn();
    assert_eq!(a.cmd(&[b"AUTH", b"alice", b"x"]), V::Str(b"OK".to_vec()));
    let values = a.cmd(&[b"TS.QUERYLABELS", b"VALUES", b"kind", b"FILTER", b"grp=g"]);
    let values: Vec<&[u8]> = values.arr().iter().map(V::bytes).collect();
    assert_eq!(
        values,
        [b"open".as_slice()],
        "a user of ~\u{e9}* read the other series"
    );

    // A rule: its destination, and the compaction's source, by name.
    let (src, dst) = ("\u{F7C3}src", "\u{F7C3}dst");
    for key in [src, dst] {
        assert_eq!(
            c.cmd(&[b"TS.CREATE", key.as_bytes()]),
            V::Str(b"OK".to_vec())
        );
    }
    let rule = c.cmd(&[
        b"TS.CREATERULE",
        src.as_bytes(),
        dst.as_bytes(),
        b"AGGREGATION",
        b"sum",
        b"10",
    ]);
    assert_eq!(rule, V::Str(b"OK".to_vec()));
    let add = |c: &mut Conn, at: &str| {
        c.cmd(&[b"TS.ADD", src.as_bytes(), at.as_bytes(), b"1"])
            .int()
    };
    add(&mut c, "1");
    add(&mut c, "11");
    let samples = |c: &mut Conn| {
        c.cmd(&[b"TS.RANGE", dst.as_bytes(), b"-", b"+"])
            .arr()
            .len()
    };
    assert_eq!(samples(&mut c), 1);
    let info = |c: &mut Conn, key: &str, field: &[u8]| {
        let reply = c.cmd(&[b"TS.INFO", key.as_bytes()]);
        let pairs = reply.arr().to_vec();
        let at = pairs
            .iter()
            .position(|v| *v == V::Str(field.to_vec()))
            .expect("field");
        pairs[at + 1].clone()
    };
    assert_eq!(
        info(&mut c, dst, b"sourceKey"),
        V::Str(src.as_bytes().to_vec())
    );
    assert_eq!(
        info(&mut c, src, b"rules").arr()[0].arr()[0],
        V::Str(dst.as_bytes().to_vec())
    );

    assert_eq!(c.cmd(&[b"SAVE"]).bytes(), b"OK");
    drop((c, a));
    server.restart();
    let mut c = server.conn();
    assert_eq!(index(&mut c), both, "{}", server.log());
    assert_eq!(mrange(&mut c), both);
    add(&mut c, "21");
    assert_eq!(
        samples(&mut c),
        2,
        "the rule wrote elsewhere after the restart"
    );
    assert_eq!(
        info(&mut c, dst, b"sourceKey"),
        V::Str(src.as_bytes().to_vec())
    );
    assert_eq!(
        info(&mut c, src, b"rules").arr()[0].arr()[0],
        V::Str(dst.as_bytes().to_vec())
    );
}

// ── Commands: every family takes keys of any bytes (#437, step 3) ─────────────

/// A script of commands over keys, hash fields, stream group and consumer
/// names and vector-set elements that are not UTF-8, and the replies
/// redis:8 (8.10.3) gives to it, byte for byte. The replies were taken from a
/// redis:8 server running the same script; KiviDB answered every key here
/// with "ERR key must be valid UTF-8", or passed it over (EXISTS, TOUCH,
/// PFCOUNT, BITOP, SUNIONCARD, SORT ... STORE, the geo STORE options, VREM).
///
/// The keys: with a NUL, with CRLF, a cut-off sequence, one that ends in the
/// middle of a character, bytes of the hash-field escape's block, 24 bytes
/// ending in 0xFF (the heap form of the owned key type), and 30 bytes.
#[test]
fn every_command_family_takes_keys_of_any_bytes() {
    use V::*;
    let script: Vec<(Vec<&[u8]>, V)> = vec![
        (vec![b"SET", b"\xffA\x00", b"v"], Str(b"OK".to_vec())),
        (vec![b"GET", b"\xffA\x00"], Str(b"v".to_vec())),
        (vec![b"SET", b"\xffA\x00", b"w", b"NX"], Nil),
        (vec![b"SET", b"\xffA\x00", b"x", b"XX", b"GET"], Str(b"v".to_vec())),
        (vec![b"GETSET", b"\xffA\x00", b"y"], Str(b"x".to_vec())),
        (vec![b"APPEND", b"\xffA\x00", b"z"], Int(2)),
        (vec![b"STRLEN", b"\xffA\x00"], Int(2)),
        (vec![b"GETRANGE", b"\xffA\x00", b"0", b"1"], Str(b"yz".to_vec())),
        (vec![b"SETRANGE", b"\xffA\x00", b"1", b"Q"], Int(2)),
        (vec![b"GET", b"\xffA\x00"], Str(b"yQ".to_vec())),
        (vec![b"INCR", b"\xfeB\x0d\x0a"], Int(1)),
        (vec![b"INCRBY", b"\xfeB\x0d\x0a", b"5"], Int(6)),
        (vec![b"DECR", b"\xfeB\x0d\x0a"], Int(5)),
        (vec![b"DECRBY", b"\xfeB\x0d\x0a", b"2"], Int(3)),
        (vec![b"INCRBYFLOAT", b"\xfeB\x0d\x0a", b"1.5"], Str(b"4.5".to_vec())),
        (vec![b"SETNX", b"\xe7\x80C", b"1"], Int(1)),
        (vec![b"MSET", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"f", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"g"], Str(b"OK".to_vec())),
        (vec![b"MGET", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"\xffA\x00", b"nokey"], Arr(vec![Str(b"f".to_vec()), Str(b"g".to_vec()), Str(b"yQ".to_vec()), Nil])),
        (vec![b"MSETNX", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"x", b"new", b"y"], Int(0)),
        (vec![b"GETDEL", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"], Str(b"f".to_vec())),
        (vec![b"LCS", b"\xffA\x00", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"], Str(b"".to_vec())),
        (vec![b"EXISTS", b"\xffA\x00", b"\xfeB\x0d\x0a", b"\xe7\x80C", b"k\xc3", b"zz"], Int(3)),
        (vec![b"MSETEX", b"2", b"k\xc3", b"1", b"\xf7\x80\xef", b"2", b"EXAT", b"4000000000"], Int(1)),
        (vec![b"EXPIRETIME", b"k\xc3"], Int(4000000000)),
        (vec![b"DELEX", b"k\xc3"], Int(1)),
        (vec![b"DEL", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(2)),
        (vec![b"EXISTS", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(0)),
        (vec![b"SET", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"long"], Str(b"OK".to_vec())),
        (vec![b"GET", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"], Str(b"long".to_vec())),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"SETBIT", b"\xffA\x00", b"7", b"1"], Int(0)),
        (vec![b"GETBIT", b"\xffA\x00", b"7"], Int(1)),
        (vec![b"BITCOUNT", b"\xffA\x00"], Int(1)),
        (vec![b"BITPOS", b"\xffA\x00", b"1"], Int(7)),
        (vec![b"SETBIT", b"\xfeB\x0d\x0a", b"1", b"1"], Int(0)),
        (vec![b"BITOP", b"OR", b"\xe7\x80C", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(1)),
        (vec![b"GET", b"\xe7\x80C"], Str(b"A".to_vec())),
        (vec![b"BITFIELD", b"\xf7\x80\xef", b"SET", b"u8", b"0", b"200", b"GET", b"u8", b"0"], Arr(vec![Int(0), Int(200)])),
        (vec![b"PFADD", b"k\xc3", b"a", b"b"], Int(1)),
        (vec![b"PFADD", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"c"], Int(1)),
        (vec![b"PFCOUNT", b"k\xc3", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"], Int(3)),
        (vec![b"PFMERGE", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"k\xc3", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"], Str(b"OK".to_vec())),
        (vec![b"PFCOUNT", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"], Int(3)),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"SET", b"\xffA\x00", b"1"], Str(b"OK".to_vec())),
        (vec![b"TYPE", b"\xffA\x00"], Str(b"string".to_vec())),
        (vec![b"EXPIREAT", b"\xffA\x00", b"4000000000"], Int(1)),
        (vec![b"EXPIRETIME", b"\xffA\x00"], Int(4000000000)),
        (vec![b"PERSIST", b"\xffA\x00"], Int(1)),
        (vec![b"TTL", b"\xffA\x00"], Int(-1)),
        (vec![b"RENAME", b"\xffA\x00", b"\xfeB\x0d\x0a"], Str(b"OK".to_vec())),
        (vec![b"EXISTS", b"\xffA\x00"], Int(0)),
        (vec![b"GET", b"\xfeB\x0d\x0a"], Str(b"1".to_vec())),
        (vec![b"RENAMENX", b"\xfeB\x0d\x0a", b"\xe7\x80C"], Int(1)),
        (vec![b"COPY", b"\xe7\x80C", b"k\xc3"], Int(1)),
        (vec![b"GET", b"k\xc3"], Str(b"1".to_vec())),
        (vec![b"MOVE", b"k\xc3", b"1"], Int(1)),
        (vec![b"SELECT", b"1"], Str(b"OK".to_vec())),
        (vec![b"GET", b"k\xc3"], Str(b"1".to_vec())),
        (vec![b"SELECT", b"0"], Str(b"OK".to_vec())),
        (vec![b"TOUCH", b"\xe7\x80C", b"k\xc3"], Int(1)),
        (vec![b"OBJECT", b"ENCODING", b"\xe7\x80C"], Str(b"int".to_vec())),
        (vec![b"UNLINK", b"\xe7\x80C"], Int(1)),
        (vec![b"EXISTS", b"\xe7\x80C"], Int(0)),
        (vec![b"SET", b"\xf7\x80\xef", b"e"], Str(b"OK".to_vec())),
        (vec![b"KEYS", b"*"], Arr(vec![Str(b"\xf7\x80\xef".to_vec())])),
        (vec![b"RPUSH", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"3", b"1", b"2"], Int(3)),
        (vec![b"SORT", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"], Arr(vec![Str(b"1".to_vec()), Str(b"2".to_vec()), Str(b"3".to_vec())])),
        (vec![b"SORT", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"ALPHA", b"DESC", b"STORE", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80"], Int(3)),
        (vec![b"LRANGE", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"0", b"-1"], Arr(vec![Str(b"3".to_vec()), Str(b"2".to_vec()), Str(b"1".to_vec())])),
        (vec![b"SET", b"w\xff1", b"10"], Str(b"OK".to_vec())),
        (vec![b"SET", b"w\xff2", b"5"], Str(b"OK".to_vec())),
        (vec![b"SET", b"w\xff3", b"7"], Str(b"OK".to_vec())),
        (vec![b"SORT", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"BY", b"w\xff*", b"GET", b"w\xff*", b"GET", b"#"], Arr(vec![Str(b"5".to_vec()), Str(b"2".to_vec()), Str(b"7".to_vec()), Str(b"3".to_vec()), Str(b"10".to_vec()), Str(b"1".to_vec())])),
        (vec![b"HSET", b"h\xfe1", b"f\xff", b"9"], Int(1)),
        (vec![b"HSET", b"h\xfe2", b"f\xff", b"8"], Int(1)),
        (vec![b"SORT", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"BY", b"h\xfe*->f\xff", b"GET", b"h\xfe*->f\xff"], Arr(vec![Nil, Str(b"8".to_vec()), Str(b"9".to_vec())])),
        (vec![b"SET", b"s3", b""], Str(b"OK".to_vec())),
        (vec![b"SORT", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"BY", b"nosort", b"GET", b"s*"], Arr(vec![Str(b"".to_vec()), Nil, Nil])),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"RPUSH", b"\xffA\x00", b"a", b"b", b"c"], Int(3)),
        (vec![b"LPUSH", b"\xffA\x00", b"z"], Int(4)),
        (vec![b"LLEN", b"\xffA\x00"], Int(4)),
        (vec![b"LRANGE", b"\xffA\x00", b"0", b"-1"], Arr(vec![Str(b"z".to_vec()), Str(b"a".to_vec()), Str(b"b".to_vec()), Str(b"c".to_vec())])),
        (vec![b"LINDEX", b"\xffA\x00", b"1"], Str(b"a".to_vec())),
        (vec![b"LSET", b"\xffA\x00", b"0", b"Y"], Str(b"OK".to_vec())),
        (vec![b"LPOS", b"\xffA\x00", b"b"], Int(2)),
        (vec![b"RPOPLPUSH", b"\xffA\x00", b"\xfeB\x0d\x0a"], Str(b"c".to_vec())),
        (vec![b"LMOVE", b"\xffA\x00", b"\xfeB\x0d\x0a", b"LEFT", b"RIGHT"], Str(b"Y".to_vec())),
        (vec![b"LRANGE", b"\xfeB\x0d\x0a", b"0", b"-1"], Arr(vec![Str(b"c".to_vec()), Str(b"Y".to_vec())])),
        (vec![b"LMPOP", b"2", b"\xe7\x80C", b"\xfeB\x0d\x0a", b"LEFT"], Arr(vec![Str(b"\xfeB\x0d\x0a".to_vec()), Arr(vec![Str(b"c".to_vec())])])),
        (vec![b"BLPOP", b"\xe7\x80C", b"\xfeB\x0d\x0a", b"0.1"], Arr(vec![Str(b"\xfeB\x0d\x0a".to_vec()), Str(b"Y".to_vec())])),
        (vec![b"RPUSH", b"k\xc3", b"1", b"2"], Int(2)),
        (vec![b"BLMOVE", b"k\xc3", b"\xf7\x80\xef", b"LEFT", b"LEFT", b"0.1"], Str(b"1".to_vec())),
        (vec![b"BLMPOP", b"0.1", b"2", b"\xe7\x80C", b"\xf7\x80\xef", b"RIGHT", b"COUNT", b"2"], Arr(vec![Str(b"\xf7\x80\xef".to_vec()), Arr(vec![Str(b"1".to_vec())])])),
        (vec![b"LRANGE", b"k\xc3", b"0", b"-1"], Arr(vec![Str(b"2".to_vec())])),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"SADD", b"\xffA\x00", b"a", b"b", b"c"], Int(3)),
        (vec![b"SADD", b"\xfeB\x0d\x0a", b"b", b"c", b"d"], Int(3)),
        (vec![b"SCARD", b"\xffA\x00"], Int(3)),
        (vec![b"SMISMEMBER", b"\xffA\x00", b"a", b"x"], Arr(vec![Int(1), Int(0)])),
        (vec![b"SMOVE", b"\xffA\x00", b"\xfeB\x0d\x0a", b"a"], Int(1)),
        (vec![b"SINTERSTORE", b"\xe7\x80C", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(2)),
        (vec![b"SUNIONSTORE", b"k\xc3", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(4)),
        (vec![b"SDIFFSTORE", b"\xf7\x80\xef", b"\xfeB\x0d\x0a", b"\xffA\x00"], Int(2)),
        (vec![b"SINTERCARD", b"2", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(2)),
        (vec![b"SUNIONCARD", b"2", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(4)),
        (vec![b"SDIFFCARD", b"2", b"\xfeB\x0d\x0a", b"\xffA\x00"], Int(2)),
        (vec![b"SCARD", b"k\xc3"], Int(4)),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"ZADD", b"\xffA\x00", b"1", b"a", b"2", b"b", b"3", b"c"], Int(3)),
        (vec![b"ZADD", b"\xfeB\x0d\x0a", b"2", b"b", b"5", b"e"], Int(2)),
        (vec![b"ZINCRBY", b"\xffA\x00", b"2", b"a"], Str(b"3".to_vec())),
        (vec![b"ZRANGE", b"\xffA\x00", b"0", b"-1", b"WITHSCORES"], Arr(vec![Str(b"b".to_vec()), Str(b"2".to_vec()), Str(b"a".to_vec()), Str(b"3".to_vec()), Str(b"c".to_vec()), Str(b"3".to_vec())])),
        (vec![b"ZRANGEBYLEX", b"\xffA\x00", b"-", b"+"], Arr(vec![Str(b"b".to_vec()), Str(b"a".to_vec()), Str(b"c".to_vec())])),
        (vec![b"ZUNIONSTORE", b"\xe7\x80C", b"2", b"\xffA\x00", b"\xfeB\x0d\x0a"], Int(4)),
        (vec![b"ZRANGE", b"\xe7\x80C", b"0", b"-1", b"WITHSCORES"], Arr(vec![Str(b"a".to_vec()), Str(b"3".to_vec()), Str(b"c".to_vec()), Str(b"3".to_vec()), Str(b"b".to_vec()), Str(b"4".to_vec()), Str(b"e".to_vec()), Str(b"5".to_vec())])),
        (vec![b"ZINTERSTORE", b"k\xc3", b"2", b"\xffA\x00", b"\xfeB\x0d\x0a", b"WEIGHTS", b"1", b"2"], Int(1)),
        (vec![b"ZDIFF", b"2", b"\xffA\x00", b"\xfeB\x0d\x0a"], Arr(vec![Str(b"a".to_vec()), Str(b"c".to_vec())])),
        (vec![b"ZRANGESTORE", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"\xffA\x00", b"0", b"-1"], Int(3)),
        (vec![b"ZMPOP", b"2", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"\xe7\x80C", b"MIN"], Arr(vec![Str(b"\xe7\x80C".to_vec()), Arr(vec![Arr(vec![Str(b"a".to_vec()), Str(b"3".to_vec())])])])),
        (vec![b"BZPOPMAX", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"\xe7\x80C", b"0.1"], Arr(vec![Str(b"\xe7\x80C".to_vec()), Str(b"e".to_vec()), Str(b"5".to_vec())])),
        (vec![b"BZMPOP", b"0.1", b"2", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"k\xc3", b"MAX"], Arr(vec![Str(b"k\xc3".to_vec()), Arr(vec![Arr(vec![Str(b"b".to_vec()), Str(b"6".to_vec())])])])),
        (vec![b"GEOADD", b"\xf7\x80\xef", b"13.361389", b"38.115556", b"Palermo", b"15.087269", b"37.502669", b"Catania"], Int(2)),
        (vec![b"GEOSEARCHSTORE", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"\xf7\x80\xef", b"FROMLONLAT", b"15", b"37", b"BYRADIUS", b"200", b"km"], Int(2)),
        (vec![b"ZRANGE", b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", b"0", b"-1"], Arr(vec![Str(b"Palermo".to_vec()), Str(b"Catania".to_vec())])),
        (vec![b"GEORADIUS", b"\xf7\x80\xef", b"15", b"37", b"200", b"km", b"STORE", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"], Int(2)),
        (vec![b"ZRANGE", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"0", b"-1"], Arr(vec![Str(b"Palermo".to_vec()), Str(b"Catania".to_vec())])),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"HSET", b"\xffA\x00", b"f", b"v", b"\xff\x00g", b"\xfew"], Int(2)),
        (vec![b"HGET", b"\xffA\x00", b"\xff\x00g"], Str(b"\xfew".to_vec())),
        (vec![b"HMGET", b"\xffA\x00", b"f", b"x"], Arr(vec![Str(b"v".to_vec()), Nil])),
        (vec![b"HINCRBY", b"\xffA\x00", b"h", b"2"], Int(2)),
        (vec![b"HLEN", b"\xffA\x00"], Int(3)),
        (vec![b"HGETALL", b"\xffA\x00"], Arr(vec![Str(b"f".to_vec()), Str(b"v".to_vec()), Str(b"\xff\x00g".to_vec()), Str(b"\xfew".to_vec()), Str(b"h".to_vec()), Str(b"2".to_vec())])),
        (vec![b"HDEL", b"\xffA\x00", b"h"], Int(1)),
        (vec![b"HEXPIREAT", b"\xffA\x00", b"4000000000", b"FIELDS", b"1", b"\xff\x00g"], Arr(vec![Int(1)])),
        (vec![b"HEXPIRETIME", b"\xffA\x00", b"FIELDS", b"1", b"\xff\x00g"], Arr(vec![Int(4000000000)])),
        (vec![b"HPERSIST", b"\xffA\x00", b"FIELDS", b"1", b"\xff\x00g"], Arr(vec![Int(1)])),
        (vec![b"HGETDEL", b"\xffA\x00", b"FIELDS", b"1", b"f"], Arr(vec![Str(b"v".to_vec())])),
        (vec![b"HGETALL", b"\xffA\x00"], Arr(vec![Str(b"\xff\x00g".to_vec()), Str(b"\xfew".to_vec())])),
        (vec![b"HSETEX", b"\xfeB\x0d\x0a", b"EXAT", b"4000000000", b"FIELDS", b"1", b"x", b"y"], Int(1)),
        (vec![b"HGETALL", b"\xfeB\x0d\x0a"], Arr(vec![Str(b"x".to_vec()), Str(b"y".to_vec())])),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"XADD", b"\xffA\x00", b"1-1", b"f", b"v"], Str(b"1-1".to_vec())),
        (vec![b"XADD", b"\xffA\x00", b"2-1", b"g", b"w"], Str(b"2-1".to_vec())),
        (vec![b"XLEN", b"\xffA\x00"], Int(2)),
        (vec![b"XRANGE", b"\xffA\x00", b"-", b"+"], Arr(vec![Arr(vec![Str(b"1-1".to_vec()), Arr(vec![Str(b"f".to_vec()), Str(b"v".to_vec())])]), Arr(vec![Str(b"2-1".to_vec()), Arr(vec![Str(b"g".to_vec()), Str(b"w".to_vec())])])])),
        (vec![b"XREAD", b"STREAMS", b"\xffA\x00", b"\xfeB\x0d\x0a", b"0", b"0"], Arr(vec![Arr(vec![Str(b"\xffA\x00".to_vec()), Arr(vec![Arr(vec![Str(b"1-1".to_vec()), Arr(vec![Str(b"f".to_vec()), Str(b"v".to_vec())])]), Arr(vec![Str(b"2-1".to_vec()), Arr(vec![Str(b"g".to_vec()), Str(b"w".to_vec())])])])])])),
        (vec![b"XGROUP", b"CREATE", b"\xffA\x00", b"\xffg\x01", b"0"], Str(b"OK".to_vec())),
        (vec![b"XGROUP", b"CREATECONSUMER", b"\xffA\x00", b"\xffg\x01", b"\xfec\x0d\x0a"], Int(1)),
        (vec![b"XREADGROUP", b"GROUP", b"\xffg\x01", b"\xfec\x0d\x0a", b"COUNT", b"1", b"STREAMS", b"\xffA\x00", b">"], Arr(vec![Arr(vec![Str(b"\xffA\x00".to_vec()), Arr(vec![Arr(vec![Str(b"1-1".to_vec()), Arr(vec![Str(b"f".to_vec()), Str(b"v".to_vec())])])])])])),
        (vec![b"XPENDING", b"\xffA\x00", b"\xffg\x01"], Arr(vec![Int(1), Str(b"1-1".to_vec()), Str(b"1-1".to_vec()), Arr(vec![Arr(vec![Str(b"\xfec\x0d\x0a".to_vec()), Str(b"1".to_vec())])])])),
        (vec![b"XCLAIM", b"\xffA\x00", b"\xffg\x01", b"\xe7c2", b"0", b"1-1", b"JUSTID"], Arr(vec![Str(b"1-1".to_vec())])),
        (vec![b"XACK", b"\xffA\x00", b"\xffg\x01", b"1-1"], Int(1)),
        (vec![b"XGROUP", b"SETID", b"\xffA\x00", b"\xffg\x01", b"0"], Str(b"OK".to_vec())),
        (vec![b"XGROUP", b"DELCONSUMER", b"\xffA\x00", b"\xffg\x01", b"\xe7c2"], Int(0)),
        (vec![b"XREADGROUP", b"GROUP", b"\xfeno", b"c", b"STREAMS", b"\xffA\x00", b">"], Err(String::from_utf8_lossy(b"NOGROUP No such key '\xffA' or consumer group '\xfeno' in XREADGROUP with GROUP option").into_owned())),
        (vec![b"XGROUP", b"DESTROY", b"\xffA\x00", b"\xffg\x01"], Int(1)),
        (vec![b"XDEL", b"\xffA\x00", b"1-1"], Int(1)),
        (vec![b"XTRIM", b"\xffA\x00", b"MAXLEN", b"0"], Int(1)),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"SET", b"\xffA\x00", b"str"], Str(b"OK".to_vec())),
        (vec![b"RPUSH", b"\xfeB\x0d\x0a", b"x"], Int(1)),
        (vec![b"HSET", b"\xe7\x80C", b"\xfff", b"v"], Int(1)),
        (vec![b"ZADD", b"k\xc3", b"1", b"z"], Int(1)),
        (vec![b"__DUMPRESTORE__", b"\xffA\x00"], Str(b"OK".to_vec())),
        (vec![b"__DUMPRESTORE__", b"\xfeB\x0d\x0a"], Str(b"OK".to_vec())),
        (vec![b"__DUMPRESTORE__", b"\xe7\x80C"], Str(b"OK".to_vec())),
        (vec![b"__DUMPRESTORE__", b"k\xc3"], Str(b"OK".to_vec())),
        (vec![b"GET", b"r\xffA\x00"], Str(b"str".to_vec())),
        (vec![b"LRANGE", b"r\xfeB\x0d\x0a", b"0", b"-1"], Arr(vec![Str(b"x".to_vec())])),
        (vec![b"HGETALL", b"r\xe7\x80C"], Arr(vec![Str(b"\xfff".to_vec()), Str(b"v".to_vec())])),
        (vec![b"ZRANGE", b"rk\xc3", b"0", b"-1"], Arr(vec![Str(b"z".to_vec())])),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"JSON.SET", b"\xffA\x00", b"$", b"{\x22a\x22:1,\x22b\x22:[1,2]}"], Str(b"OK".to_vec())),
        (vec![b"JSON.GET", b"\xffA\x00", b"$.a"], Str(b"[1]".to_vec())),
        (vec![b"JSON.NUMINCRBY", b"\xffA\x00", b"$.a", b"2"], Str(b"[3]".to_vec())),
        (vec![b"JSON.ARRAPPEND", b"\xffA\x00", b"$.b", b"3"], Arr(vec![Int(3)])),
        (vec![b"JSON.MSET", b"\xfeB\x0d\x0a", b"$", b"1", b"\xe7\x80C", b"$", b"\x22s\x22"], Str(b"OK".to_vec())),
        (vec![b"JSON.MGET", b"\xffA\x00", b"\xfeB\x0d\x0a", b"$.a"], Arr(vec![Str(b"[3]".to_vec()), Str(b"[]".to_vec())])),
        (vec![b"JSON.STRAPPEND", b"\xe7\x80C", b"\x22t\x22"], Int(2)),
        (vec![b"JSON.GET", b"\xe7\x80C"], Str(b"\x22st\x22".to_vec())),
        (vec![b"TS.CREATE", b"k\xc3"], Str(b"OK".to_vec())),
        (vec![b"TS.CREATE", b"\xf7\x80\xef"], Str(b"OK".to_vec())),
        (vec![b"TS.CREATE", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"], Str(b"OK".to_vec())),
        (vec![b"TS.CREATERULE", b"k\xc3", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"AGGREGATION", b"avg", b"1000"], Str(b"OK".to_vec())),
        (vec![b"TS.ADD", b"k\xc3", b"1000", b"1.5"], Int(1000)),
        (vec![b"TS.MADD", b"k\xc3", b"2000", b"3", b"\xf7\x80\xef", b"2000", b"4"], Arr(vec![Int(2000), Int(2000)])),
        (vec![b"TS.ADD", b"k\xc3", b"3000", b"5"], Int(3000)),
        (vec![b"TS.RANGE", b"k\xc3", b"-", b"+"], Arr(vec![Arr(vec![Int(1000), Str(b"1.5".to_vec())]), Arr(vec![Int(2000), Str(b"3".to_vec())]), Arr(vec![Int(3000), Str(b"5".to_vec())])])),
        (vec![b"TS.RANGE", b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", b"-", b"+"], Arr(vec![Arr(vec![Int(1000), Str(b"1.5".to_vec())]), Arr(vec![Int(2000), Str(b"3".to_vec())])])),
        (vec![b"TS.GET", b"k\xc3"], Arr(vec![Int(3000), Str(b"5".to_vec())])),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
        (vec![b"VADD", b"\xffA\x00", b"VALUES", b"2", b"1", b"0", b"\xff\x00e1"], Int(1)),
        (vec![b"VADD", b"\xffA\x00", b"VALUES", b"2", b"0", b"1", b"\xfee2"], Int(1)),
        (vec![b"VCARD", b"\xffA\x00"], Int(2)),
        (vec![b"VISMEMBER", b"\xffA\x00", b"\xff\x00e1"], Int(1)),
        (vec![b"VSIM", b"\xffA\x00", b"ELE", b"\xff\x00e1", b"COUNT", b"1"], Arr(vec![Str(b"\xff\x00e1".to_vec())])),
        (vec![b"VRANGE", b"\xffA\x00", b"-", b"+"], Arr(vec![Str(b"\xfee2".to_vec()), Str(b"\xff\x00e1".to_vec())])),
        (vec![b"VRANGE", b"\xffA\x00", b"[\xfe", b"+"], Arr(vec![Str(b"\xfee2".to_vec()), Str(b"\xff\x00e1".to_vec())])),
        (vec![b"VREM", b"\xffA\x00", b"\xfee2"], Int(1)),
        (vec![b"VCARD", b"\xffA\x00"], Int(1)),
        (vec![b"FLUSHALL"], Str(b"OK".to_vec())),
    ];
    let server = Server::start(&[]);
    let mut c = server.conn();
    for (i, (args, want)) in script.iter().enumerate() {
        let got = if args[0] == b"__DUMPRESTORE__" {
            let payload = c.cmd(&[b"DUMP", args[1]]);
            let to = [b"r", args[1]].concat();
            c.cmd(&[b"RESTORE", &to, b"0", payload.bytes()])
        } else {
            c.cmd(args)
        };
        assert_eq!(
            &got,
            want,
            "step {i}: {:?}",
            String::from_utf8_lossy(&args.join(&b' '))
        );
    }
}

/// Everything a key that is not text can hold, written by commands, with
/// hash fields, group and consumer names and vector-set elements that are
/// not text either; and what the dataset is, by `TYPE` and
/// `DEBUG DIGEST-VALUE` of every key and the names inside them.
fn write_every_type(c: &mut Conn) {
    let ok = |v: V| assert!(!matches!(v, V::Err(_)), "{v:?}");
    ok(c.cmd(&[b"SET", A, b"v\x00\xff"]));
    ok(c.cmd(&[b"PEXPIREAT", A, b"4000000000000"]));
    ok(c.cmd(&[b"RPUSH", B, b"x", b"y"]));
    ok(c.cmd(&[b"SADD", T, b"m\xff"]));
    ok(c.cmd(&[b"ZADD", b"z\xfe\r\n", b"1", b"a\xff"]));
    ok(c.cmd(&[b"HSET", b"h\xfe", b"\xff\x00f", b"1", b"\xef\x9f\x83", b"2"]));
    ok(c.cmd(&[
        b"HEXPIREAT",
        b"h\xfe",
        b"4000000000",
        b"FIELDS",
        b"1",
        b"\xff\x00f",
    ]));
    ok(c.cmd(&[b"XADD", b"s\xff", b"1-1", b"f", b"v"]));
    ok(c.cmd(&[b"XGROUP", b"CREATE", b"s\xff", b"g\xff", b"0"]));
    ok(c.cmd(&[
        b"XREADGROUP",
        b"GROUP",
        b"g\xff",
        b"c\xfe\x00",
        b"STREAMS",
        b"s\xff",
        b">",
    ]));
    ok(c.cmd(&[b"VADD", b"v\xff", b"VALUES", b"2", b"1", b"0", b"e\xff\x00"]));
    ok(c.cmd(&[b"JSON.SET", b"j\xff", b"$", b"{\"a\":1}"]));
    ok(c.cmd(&[b"TS.ADD", b"t\xff", b"1000", b"2"]));
    ok(c.cmd(&[b"PFADD", b"p\xff", b"a"]));
}

fn dataset(c: &mut Conn) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for key in c.scan_all(None) {
        let ty = c.cmd(&[b"TYPE", &key]).bytes().to_vec();
        let digest = c.cmd(&[b"DEBUG", b"DIGEST-VALUE", &key]).arr()[0]
            .bytes()
            .to_vec();
        let ttl = c
            .cmd(&[b"PEXPIRETIME", &key])
            .int()
            .to_string()
            .into_bytes();
        out.push([key.as_slice(), b" ", &ty, b" ", &digest, b" ", &ttl].concat());
    }
    let names = [
        c.cmd(&[b"HGETALL", b"h\xfe"]),
        c.cmd(&[b"HPEXPIRETIME", b"h\xfe", b"FIELDS", b"1", b"\xff\x00f"]),
        c.cmd(&[b"XINFO", b"GROUPS", b"s\xff"]).arr()[0].arr()[1].clone(),
        c.cmd(&[b"XPENDING", b"s\xff", b"g\xff", b"-", b"+", b"10"])
            .arr()[0]
            .arr()[1]
            .clone(),
        c.cmd(&[b"VRANGE", b"v\xff", b"-", b"+"]),
    ];
    out.push(format!("{names:?}").into_bytes());
    out
}

/// Keys and names that are not text, written by commands, come back the
/// same through every way a dataset travels: a snapshot and a restart, the
/// AOF (its tail replayed, and its rewrite), a replica's full sync and its
/// replication stream, and DUMP / RESTORE.
#[test]
fn keys_and_names_of_any_bytes_survive_every_copy() {
    // The snapshot.
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    write_every_type(&mut c);
    let want = dataset(&mut c);
    assert_eq!(want.len(), 11, "{want:?}");
    assert_eq!(c.cmd(&[b"SAVE"]).bytes(), b"OK");
    drop(c);
    server.restart();
    let mut c = server.conn();
    assert_eq!(dataset(&mut c), want, "after a restart from the snapshot");

    // DUMP / RESTORE of every key, into another server.
    let other = Server::start(&[]);
    let mut o = other.conn();
    for key in c.scan_all(None) {
        let payload = c.cmd(&[b"DUMP", &key]);
        let pttl = c.cmd(&[b"PTTL", &key]).int().max(0).to_string();
        let reply = o.cmd(&[b"RESTORE", &key, pttl.as_bytes(), payload.bytes()]);
        assert_eq!(reply.bytes(), b"OK", "{key:?}");
    }
    let mut copied = dataset(&mut o);
    let mut original = want.clone();
    // A time to live travels as what is left of it: compare without it.
    for rows in [&mut copied, &mut original] {
        for row in rows.iter_mut() {
            if let Some(cut) = row.iter().rposition(|&b| b == b' ') {
                row.truncate(cut);
            }
        }
    }
    assert_eq!(copied, original, "after DUMP and RESTORE");
    drop((o, other));

    // A replica: its full sync, then the stream.
    let port = server.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    eventually(30, "the replica's full sync", || {
        r.dbsize() == want.len() as i64 - 1
    });
    assert_eq!(dataset(&mut r), want, "after the full sync");
    assert_eq!(c.cmd(&[b"RENAME", A, b"moved\xff"]).bytes(), b"OK");
    assert_eq!(c.cmd(&[b"HSET", b"h\xfe", b"\xfe\xfe", b"3"]).int(), 1);
    let now = dataset(&mut c);
    eventually(30, "the replica to follow the stream", || {
        dataset(&mut r) == now
    });
    drop((r, replica));

    // The AOF: its tail replayed, and then its rewrite loaded.
    let mut aof = Server::start(&["--aof"]);
    let mut a = aof.conn();
    // Each write on disk before its reply: the restarts below kill the server.
    assert_eq!(
        a.cmd(&[b"CONFIG", b"SET", b"appendfsync", b"always"])
            .bytes(),
        b"OK"
    );
    write_every_type(&mut a);
    // A stream's digest changes across an AOF replay whatever its names
    // (its consumers' times are those of the replay; text names do the
    // same): compare the stream by its names (the last row), not its digest.
    let aof_view = |rows: Vec<Vec<u8>>| -> Vec<Vec<u8>> {
        rows.into_iter()
            .filter(|row| !row.starts_with(b"s\xff stream"))
            .collect()
    };
    let want = aof_view(dataset(&mut a));
    drop(a);
    aof.restart();
    let mut a = aof.conn();
    assert_eq!(aof_view(dataset(&mut a)), want, "after replaying the AOF");
    assert!(!matches!(a.cmd(&[b"BGREWRITEAOF"]), V::Err(_)));
    eventually(30, "the AOF rewrite", || {
        a.info("persistence", "aof_rewrite_in_progress") == "0"
            && a.info("persistence", "aof_rewrite_scheduled") == "0"
    });
    drop(a);
    aof.restart();
    let mut a = aof.conn();
    assert_eq!(
        aof_view(dataset(&mut a)),
        want,
        "after loading the rewritten AOF"
    );
}
