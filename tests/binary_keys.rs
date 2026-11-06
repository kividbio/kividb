// tests/binary_keys.rs
//
// Keys that are not text, in a keyspace that holds them (#437). No command
// takes such a key yet, but a Redis RDB file can hold one (`--preload-file`),
// and from there everything that reads the keyspace back has to carry its
// bytes: SCAN and KEYS, expiry and the `DEL` it sends to replicas, eviction,
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
                s.write_all(b"*1\r\n$4\r\nPING\r\n")?;
                let mut reply = [0u8; 7];
                s.read_exact(&mut reply)?;
                Ok(&reply == b"+PONG\r\n")
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
/// answered OK. Until a destination takes such a key (`RESTORE` refuses one,
/// as every command does yet), it is refused there, and stays here, whole.
#[test]
fn migrate_does_not_skip_a_key_that_is_not_text() {
    let source = preloaded(&rdb(&[(A, b"a", None)]), &[]);
    let destination = Server::start(&[]);
    let mut c = source.conn();
    let port = destination.port.to_string();
    let reply = c.cmd(&[b"MIGRATE", b"127.0.0.1", port.as_bytes(), A, b"0", b"5000"]);
    assert!(matches!(reply, V::Err(_)), "{reply:?}");
    assert_eq!(c.cmd(&[b"DBSIZE"]).int(), 1);
    assert_eq!(c.scan_all(None), vec![A.to_vec()]);
    assert_eq!(destination.conn().dbsize(), 0);
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
