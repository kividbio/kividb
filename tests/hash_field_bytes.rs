#![allow(clippy::disallowed_methods)]
// Hash field names of any bytes (#510): a hash keeps a field as the bytes it
// was sent as, and KDB files, `DUMP` payloads, the AOF and replication carry
// it so that what one version writes, the other reads to the same bytes.
//
// The ignored tests need `KIVIDB_1_2_BIN`: a KiviDB 1.2 release binary, or
// another KiviDB build to check against (one of master, say).
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::{restart_verified, start_verified};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Bulk(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn s(&self) -> String {
        match self {
            V::Str(s) | V::Err(s) => s.clone(),
            V::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("not a string: {other:?}"),
        }
    }
    fn b(&self) -> Vec<u8> {
        match self {
            V::Bulk(b) => b.clone(),
            V::Str(s) => s.as_bytes().to_vec(),
            other => panic!("not a bulk string: {other:?}"),
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

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    buf
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
        self.writer.write_all(&encode(args)).unwrap();
        self.read()
    }

    fn ok(&mut self, args: &[&[u8]]) -> V {
        let reply = self.cmd(args);
        assert!(
            !matches!(reply, V::Err(_)),
            "{:?}: {reply:?}",
            args.iter()
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect::<Vec<_>>()
        );
        reply
    }

    fn read(&mut self) -> V {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).expect("reply");
        let line = String::from_utf8_lossy(&line);
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
                V::Bulk(data)
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

    fn info(&mut self, section: &str, field: &str) -> String {
        let reply = self.cmd(&[b"INFO", section.as_bytes()]).s();
        reply
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .trim()
            .to_string()
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

fn spawn(bin: &Path, dir: &Path, port: u16, extra: &[String]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let mut command = Command::new(bin);
    command.current_dir(dir).args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--threads",
        "2",
        "--configfile",
        "kividb.conf",
    ]);
    if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
        command.args(["--io-model", &model]);
    }
    // 1.2 takes `DEBUG` from anyone, and has no such setting.
    if has_debug_setting(bin) {
        command.args(["--enable-debug-command", "yes"]);
    }
    command
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

/// Whether `bin` has the `enable-debug-command` setting.
fn has_debug_setting(bin: &Path) -> bool {
    Command::new(bin)
        .arg("--help")
        .output()
        .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).contains("--enable-debug-command"))
}

fn this_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kividb"))
}

/// The other binary. Asked for by the ignored tests: never skipped.
fn old_bin() -> PathBuf {
    let bin = std::env::var("KIVIDB_1_2_BIN")
        .expect("KIVIDB_1_2_BIN must name another KiviDB binary (a 1.2 release)");
    let bin = PathBuf::from(bin);
    let out = Command::new(&bin)
        .arg("--version")
        .output()
        .unwrap_or_else(|e| panic!("{} does not run: {e}", bin.display()));
    let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(
        version.starts_with("kividb 1."),
        "{} is {version:?}, not a KiviDB binary",
        bin.display()
    );
    bin
}

struct Server {
    port: u16,
    dir: PathBuf,
    tmp: Option<tempfile::TempDir>,
    child: Child,
    bin: PathBuf,
    args: Vec<String>,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        Self::start_bin(this_bin(), tempfile::tempdir().unwrap(), extra)
    }

    fn start_bin(bin: PathBuf, tmp: tempfile::TempDir, extra: &[&str]) -> Self {
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let dir = tmp.path().to_path_buf();
        let (child, port) = start_verified(|port| spawn(&bin, &dir, port, &args));
        let mut server = Server {
            port,
            dir,
            tmp: Some(tmp),
            child,
            bin,
            args,
        };
        server.wait_up();
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    fn shutdown(&mut self) {
        let mut c = self.conn();
        let _ = c.writer.write_all(b"*1\r\n$8\r\nSHUTDOWN\r\n");
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "the server did not shut down");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// No final save: only the AOF can bring writes back.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn restart_as(&mut self, bin: PathBuf, extra: &[&str]) {
        self.bin = bin;
        self.args = extra.iter().map(|s| s.to_string()).collect();
        assert!(self.child.try_wait().unwrap().is_some(), "still running");
        let (child, port, _) = restart_verified(
            |port| spawn(&self.bin, &self.dir, port, &self.args),
            self.port,
        );
        self.child = child;
        self.port = port;
        self.wait_up();
    }

    fn wait_up(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
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
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!("the server exited ({status}):\n{}", self.log());
            }
            assert!(Instant::now() < deadline, "the server did not come up");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking()
            && let Some(tmp) = self.tmp.take()
        {
            let kept = tmp.keep();
            eprintln!("kept {} for its server.log", kept.display());
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

fn wait_caught_up(master: &mut Conn, replica: &mut Conn) {
    wait_until("the replica's link to come up", 30, || {
        replica.info("replication", "master_link_status") == "up"
    });
    wait_until("the replica to catch up", 60, || {
        let offset = master.info("replication", "master_repl_offset");
        replica.info("replication", "slave_repl_offset") == offset
    });
}

// ── The fields ────────────────────────────────────────────────────────────────

/// Field names that the escape 1.1 and 1.2 kept in memory would spell some
/// other way: bytes that are not UTF-8, a NUL, CRLF, a sequence cut short,
/// the characters of the escape's own block (U+F7FF is what `\xff` was
/// kept as; U+F780 is `\x80`), and the empty name.
fn names() -> Vec<Vec<u8>> {
    vec![
        b"plain".to_vec(),
        b"\xff\xfe".to_vec(),
        b"\xff".to_vec(),
        "\u{F7FF}".as_bytes().to_vec(),
        b"a\x00b".to_vec(),
        b"\r\n".to_vec(),
        b"cut\xc3".to_vec(),
        "\u{F780}x".as_bytes().to_vec(),
        "caf\u{e9}".as_bytes().to_vec(),
        "\u{F7EF}\u{F79E}\u{F780}".as_bytes().to_vec(),
        Vec::new(),
    ]
}

/// The value of field `i` of a hash.
fn value(i: usize) -> Vec<u8> {
    format!("v{i}").into_bytes()
}

const FAR: &[u8] = b"4102444800123";

/// Three hashes: `h:small` (a block), `h:large` (a table: 600 fields), and
/// `h:ttl`, whose fields with names that are not text have field TTLs far
/// off. Written by commands, so any version can write them.
fn populate(c: &mut Conn) {
    let names = names();
    for (i, name) in names.iter().enumerate() {
        c.ok(&[b"HSET", b"h:small", name, &value(i)]);
        c.ok(&[b"HSET", b"h:ttl", name, &value(i)]);
    }
    for i in 0..600usize {
        let mut name = names[i % names.len()].clone();
        name.extend_from_slice(format!("#{i}").as_bytes());
        c.ok(&[b"HSET", b"h:large", &name, &value(i)]);
    }
    c.ok(&[
        b"HPEXPIREAT",
        b"h:ttl",
        FAR,
        b"FIELDS",
        b"3",
        b"\xff\xfe",
        "\u{F7FF}".as_bytes(),
        b"a\x00b",
    ]);
}

/// Fields and their values.
type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, PartialEq)]
struct Seen {
    /// Each hash's fields and values, sorted.
    fields: Vec<(Vec<u8>, Pairs)>,
    /// `HPEXPIRETIME` of every name in `h:ttl`.
    deadlines: Vec<V>,
}

fn capture(c: &mut Conn) -> Seen {
    let mut fields = Vec::new();
    for key in [&b"h:small"[..], b"h:large", b"h:ttl"] {
        let reply = c.ok(&[b"HGETALL", key]);
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = reply
            .arr()
            .chunks(2)
            .map(|p| (p[0].b(), p[1].b()))
            .collect();
        pairs.sort();
        fields.push((key.to_vec(), pairs));
    }
    let names = names();
    let n = names.len().to_string();
    let mut args: Vec<&[u8]> = vec![b"HPEXPIRETIME", b"h:ttl", b"FIELDS", n.as_bytes()];
    args.extend(names.iter().map(Vec::as_slice));
    let deadlines = c.ok(&args).arr().to_vec();
    Seen { fields, deadlines }
}

/// What `populate` wrote, as it should read back anywhere.
fn expected() -> Seen {
    let names = names();
    let mut small: Vec<(Vec<u8>, Vec<u8>)> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.clone(), value(i)))
        .collect();
    small.sort();
    let mut large: Vec<(Vec<u8>, Vec<u8>)> = (0..600usize)
        .map(|i| {
            let mut name = names[i % names.len()].clone();
            name.extend_from_slice(format!("#{i}").as_bytes());
            (name, value(i))
        })
        .collect();
    large.sort();
    let far: i64 = std::str::from_utf8(FAR).unwrap().parse().unwrap();
    let deadlines = names
        .iter()
        .map(|n| {
            if [&b"\xff\xfe"[..], "\u{F7FF}".as_bytes(), b"a\x00b"].contains(&n.as_slice()) {
                V::Int(far)
            } else {
                V::Int(-1)
            }
        })
        .collect();
    Seen {
        fields: vec![
            (b"h:small".to_vec(), small.clone()),
            (b"h:large".to_vec(), large),
            (b"h:ttl".to_vec(), small),
        ],
        deadlines,
    }
}

fn digest(c: &mut Conn) -> V {
    c.ok(&[b"DEBUG", b"DIGEST-VALUE", b"h:small", b"h:large", b"h:ttl"])
}

/// [`digest`] of the hashes without field TTLs: what two versions can be
/// compared by. How a field's deadline goes into the digest changed after
/// 1.2 (#519), whatever the names.
fn digest_across(c: &mut Conn) -> V {
    c.ok(&[b"DEBUG", b"DIGEST-VALUE", b"h:small", b"h:large"])
}

// ── This version ──────────────────────────────────────────────────────────────

#[test]
fn every_hash_command_takes_a_field_as_its_bytes() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    populate(&mut c);
    assert_eq!(capture(&mut c), expected());

    // Each name is told apart from every other, the escape's lookalikes too.
    for (i, name) in names().iter().enumerate() {
        assert_eq!(c.ok(&[b"HGET", b"h:small", name]), V::Bulk(value(i)));
        assert_eq!(c.ok(&[b"HEXISTS", b"h:small", name]), V::Int(1));
        assert_eq!(
            c.ok(&[b"HSTRLEN", b"h:small", name]),
            V::Int(value(i).len() as i64)
        );
    }
    let keys = c.ok(&[b"HKEYS", b"h:small"]);
    let keys: Vec<Vec<u8>> = keys.arr().iter().map(V::b).collect();
    assert_eq!(keys, names(), "in the order they were set");

    // HSCAN MATCH is on the bytes, in a block and in a table: `\xff*` is
    // the two names that begin with the byte, never the text U+F7FF.
    for key in [&b"h:small"[..], b"h:large"] {
        let reply = c.ok(&[b"HSCAN", key, b"0", b"MATCH", b"\xff*", b"COUNT", b"10000"]);
        let mut found: Vec<Vec<u8>> = reply.arr()[1].arr().chunks(2).map(|p| p[0].b()).collect();
        found.sort();
        assert!(!found.is_empty());
        assert!(found.iter().all(|f| f[0] == 0xff), "{found:?}");
        let reply = c.ok(&[
            b"HSCAN",
            key,
            b"0",
            b"MATCH",
            "\u{F7FF}*".as_bytes(),
            b"COUNT",
            b"10000",
        ]);
        let found: Vec<Vec<u8>> = reply.arr()[1].arr().chunks(2).map(|p| p[0].b()).collect();
        assert!(!found.is_empty());
        assert!(
            found.iter().all(|f| f.starts_with("\u{F7FF}".as_bytes())),
            "{found:?}"
        );
    }

    // HRANDFIELD hands back the names themselves.
    let picked = c.ok(&[b"HRANDFIELD", b"h:small", b"-200"]);
    for f in picked.arr() {
        assert!(names().contains(&f.b()), "{f:?}");
    }

    // HDEL and HGETDEL by the bytes, the lookalikes left.
    assert_eq!(c.ok(&[b"HDEL", b"h:small", b"\xff"]), V::Int(1));
    assert_eq!(
        c.ok(&[b"HGET", b"h:small", "\u{F7FF}".as_bytes()]),
        V::Bulk(value(3))
    );

    // The limits count the bytes: a field of 40 bytes that are not text is
    // 40 bytes, a block's, and the hash costs what one of 40 letters does.
    let binary = vec![0xfe; 40];
    let text = vec![b'f'; 40];
    c.ok(&[b"HSET", b"enc:bin", &binary, b"v"]);
    c.ok(&[b"HSET", b"enc:txt", &text, b"v"]);
    assert_eq!(c.ok(&[b"OBJECT", b"ENCODING", b"enc:bin"]).s(), "listpack");
    assert_eq!(
        c.ok(&[b"MEMORY", b"USAGE", b"enc:bin"]),
        c.ok(&[b"MEMORY", b"USAGE", b"enc:txt"])
    );
    let over = vec![0xfe; 65];
    c.ok(&[b"HSET", b"enc:bin", &over, b"v"]);
    assert_eq!(c.ok(&[b"OBJECT", b"ENCODING", b"enc:bin"]).s(), "hashtable");

    // SORT ... BY/GET ->field reads the field by its bytes.
    c.ok(&[b"RPUSH", b"list", b"1"]);
    c.ok(&[b"HSET", b"obj:1", b"\xff\x00", b"x"]);
    assert_eq!(
        c.ok(&[
            b"SORT",
            b"list",
            b"BY",
            b"nosort",
            b"GET",
            b"obj:*->\xff\x00"
        ]),
        V::Arr(vec![V::Bulk(b"x".to_vec())])
    );
}

#[test]
fn binary_fields_survive_every_copy() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    populate(&mut c);
    let want = expected();
    let digest_before = digest(&mut c);

    // DUMP / RESTORE into another server.
    let other = Server::start(&[]);
    let mut o = other.conn();
    for key in [&b"h:small"[..], b"h:large", b"h:ttl"] {
        let payload = c.ok(&[b"DUMP", key]).b();
        o.ok(&[b"RESTORE", key, b"0", &payload]);
    }
    assert_eq!(capture(&mut o), want, "DUMP / RESTORE");
    assert_eq!(digest(&mut o), digest_before);

    // A replica: its full sync, then its stream.
    let replica = Server::start(&["--replicaof", "127.0.0.1", &server.port.to_string()]);
    let mut r = replica.conn();
    wait_caught_up(&mut c, &mut r);
    assert_eq!(capture(&mut r), want, "full sync");
    c.ok(&[b"HSET", b"h:small", b"\xfe\xfd", b"new"]);
    c.ok(&[b"HDEL", b"h:small", b"\xff"]);
    c.ok(&[b"HSET", b"h:small", b"\xff", &value(2)]);
    c.ok(&[
        b"HSETEX",
        b"h:ttl",
        b"PXAT",
        FAR,
        b"FIELDS",
        b"1",
        b"\xff\xfe",
        &value(1),
    ]);
    c.ok(&[
        b"HIMPORT",
        b"PREPARE",
        b"fs",
        b"\xff\x01",
        "\u{F7FF}".as_bytes(),
    ]);
    c.ok(&[b"HIMPORT", b"SET", b"h:import", b"fs", b"a", b"b"]);
    wait_caught_up(&mut c, &mut r);
    for key in [&b"h:small"[..], b"h:ttl", b"h:import"] {
        assert_eq!(
            c.ok(&[b"HGETALL", key]),
            r.ok(&[b"HGETALL", key]),
            "{}",
            String::from_utf8_lossy(key)
        );
        assert_eq!(
            c.ok(&[b"DEBUG", b"DIGEST-VALUE", key]),
            r.ok(&[b"DEBUG", b"DIGEST-VALUE", key])
        );
    }
    assert_eq!(
        c.ok(&[b"HGET", b"h:import", b"\xff\x01"]),
        V::Bulk(b"a".to_vec())
    );
    assert_eq!(
        r.ok(&[b"HGET", b"h:import", "\u{F7FF}".as_bytes()]),
        V::Bulk(b"b".to_vec())
    );
    c.ok(&[b"HDEL", b"h:small", b"\xfe\xfd"]);
    c.ok(&[b"DEL", b"h:import"]);

    // A field expired by the master goes from the replica by its bytes.
    c.ok(&[b"HSET", b"h:short", b"\xff\x02", b"x", b"keep", b"y"]);
    c.ok(&[b"HPEXPIRE", b"h:short", b"50", b"FIELDS", b"1", b"\xff\x02"]);
    wait_until("the field to expire", 20, || {
        c.cmd(&[b"HLEN", b"h:short"]) == V::Int(1)
    });
    wait_caught_up(&mut c, &mut r);
    assert_eq!(
        r.ok(&[b"HKEYS", b"h:short"]),
        V::Arr(vec![V::Bulk(b"keep".to_vec())])
    );
    c.ok(&[b"DEL", b"h:short"]);
    wait_caught_up(&mut c, &mut r);
    assert_eq!(digest(&mut c), digest(&mut r));
    drop(r);
    drop(replica);

    // SAVE and a restart.
    assert_eq!(capture(&mut c), want);
    c.ok(&[b"SAVE"]);
    drop(c);
    server.shutdown();
    server.restart_as(this_bin(), &[]);
    let mut c = server.conn();
    assert_eq!(capture(&mut c), want, "snapshot");
    assert_eq!(digest(&mut c), digest_before);
    drop(c);

    // The AOF: its tail replayed, then a rewrite loaded.
    server.shutdown();
    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    c.ok(&[b"HSET", b"h:small", b"\xfa\xfb", b"tail"]);
    c.ok(&[b"HDEL", b"h:small", b"\xfa\xfb"]);
    drop(c);
    server.kill();
    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(capture(&mut c), want, "AOF replay");
    c.ok(&[b"BGREWRITEAOF"]);
    wait_until("the rewrite", 30, || {
        c.info("persistence", "aof_rewrite_in_progress") == "0"
    });
    drop(c);
    server.kill();
    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(capture(&mut c), want, "AOF rewrite");
    assert_eq!(digest(&mut c), digest_before);
}

// ── Between versions ──────────────────────────────────────────────────────────

/// A snapshot the other binary saved loads here to the same fields, and one
/// saved here loads there to the same fields.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn snapshots_read_the_same_both_ways() {
    let old = old_bin();
    let mut server = Server::start_bin(old.clone(), tempfile::tempdir().unwrap(), &[]);
    let mut c = server.conn();
    populate(&mut c);
    let want = expected();
    assert_eq!(capture(&mut c), want, "as the other writes it");
    let old_digest = digest_across(&mut c);
    c.ok(&[b"SAVE"]);
    drop(c);
    server.shutdown();

    server.restart_as(this_bin(), &[]);
    let mut c = server.conn();
    assert_eq!(capture(&mut c), want, "its snapshot, here");
    assert_eq!(digest_across(&mut c), old_digest, "the same digest");
    c.ok(&[b"HSET", b"h:small", b"\xfa", b"here"]);
    c.ok(&[b"SAVE"]);
    drop(c);
    server.shutdown();

    server.restart_as(old, &[]);
    let mut c = server.conn();
    assert_eq!(
        c.ok(&[b"HGET", b"h:small", b"\xfa"]),
        V::Bulk(b"here".to_vec())
    );
    c.ok(&[b"HDEL", b"h:small", b"\xfa"]);
    assert_eq!(capture(&mut c), want, "this snapshot, there");
    assert_eq!(digest_across(&mut c), old_digest);
}

/// The AOF the other binary wrote replays here, and this one's there.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn aofs_replay_the_same_both_ways() {
    let old = old_bin();
    let mut server = Server::start_bin(old.clone(), tempfile::tempdir().unwrap(), &["--aof"]);
    let mut c = server.conn();
    populate(&mut c);
    let want = expected();
    drop(c);
    server.kill();

    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(capture(&mut c), want, "its log, here");
    c.ok(&[b"HSET", b"h:small", b"\xfa", b"here"]);
    c.ok(&[b"HDEL", b"h:small", b"\xfa"]);
    c.ok(&[b"HSET", b"h:here", b"\xfa\x00", b"x"]);
    drop(c);
    server.kill();

    server.restart_as(old.clone(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(capture(&mut c), want, "this log, there");
    assert_eq!(
        c.ok(&[b"HGET", b"h:here", b"\xfa\x00"]),
        V::Bulk(b"x".to_vec())
    );
    // And a rewrite made here, read there.
    drop(c);
    server.kill();
    server.restart_as(this_bin(), &["--aof"]);
    let mut c = server.conn();
    c.ok(&[b"BGREWRITEAOF"]);
    wait_until("the rewrite", 30, || {
        c.info("persistence", "aof_rewrite_in_progress") == "0"
    });
    drop(c);
    server.kill();
    server.restart_as(old, &["--aof"]);
    let mut c = server.conn();
    assert_eq!(capture(&mut c), want, "this rewrite, there");
    assert_eq!(
        c.ok(&[b"HGET", b"h:here", b"\xfa\x00"]),
        V::Bulk(b"x".to_vec())
    );
}

/// A `DUMP` payload of one restores on the other, both ways.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn dump_payloads_restore_the_same_both_ways() {
    let old = Server::start_bin(old_bin(), tempfile::tempdir().unwrap(), &[]);
    let new = Server::start(&[]);
    let (mut o, mut n) = (old.conn(), new.conn());
    populate(&mut o);
    let want = expected();
    for key in [&b"h:small"[..], b"h:large", b"h:ttl"] {
        let payload = o.ok(&[b"DUMP", key]).b();
        n.ok(&[b"RESTORE", key, b"0", &payload]);
    }
    assert_eq!(capture(&mut n), want, "its payloads, here");
    assert_eq!(digest_across(&mut n), digest_across(&mut o));
    o.ok(&[b"FLUSHALL"]);
    for key in [&b"h:small"[..], b"h:large", b"h:ttl"] {
        let payload = n.ok(&[b"DUMP", key]).b();
        o.ok(&[b"RESTORE", key, b"0", &payload]);
    }
    assert_eq!(capture(&mut o), want, "these payloads, there");
}

/// A replica of one version follows a master of the other, both ways: its
/// full sync, its stream, field TTLs and their expiry.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn replicas_follow_masters_of_the_other_version() {
    for old_is_master in [true, false] {
        let (master_bin, replica_bin) = if old_is_master {
            (old_bin(), this_bin())
        } else {
            (this_bin(), old_bin())
        };
        let master = Server::start_bin(master_bin, tempfile::tempdir().unwrap(), &[]);
        let mut m = master.conn();
        populate(&mut m);
        let replica = Server::start_bin(
            replica_bin,
            tempfile::tempdir().unwrap(),
            &["--replicaof", "127.0.0.1", &master.port.to_string()],
        );
        let mut r = replica.conn();
        wait_caught_up(&mut m, &mut r);
        let want = expected();
        assert_eq!(capture(&mut r), want, "full sync ({old_is_master})");
        assert_eq!(digest_across(&mut r), digest_across(&mut m));

        m.ok(&[b"HSET", b"h:small", b"\xfe\xfd", b"new"]);
        m.ok(&[b"HDEL", b"h:small", b"\xff"]);
        m.ok(&[
            b"HSETEX",
            b"h:ttl",
            b"PXAT",
            FAR,
            b"FIELDS",
            b"1",
            b"\xfa\xfb",
            b"x",
        ]);
        m.ok(&[b"HSET", b"h:short", b"\xff\x02", b"x", b"keep", b"y"]);
        m.ok(&[b"HPEXPIRE", b"h:short", b"50", b"FIELDS", b"1", b"\xff\x02"]);
        wait_until("the field to expire", 20, || {
            m.cmd(&[b"HLEN", b"h:short"]) == V::Int(1)
        });
        wait_caught_up(&mut m, &mut r);
        for key in [&b"h:small"[..], b"h:ttl", b"h:short"] {
            assert_eq!(
                m.ok(&[b"HGETALL", key]),
                r.ok(&[b"HGETALL", key]),
                "{} ({old_is_master})",
                String::from_utf8_lossy(key)
            );
        }
        assert_eq!(
            r.ok(&[b"HPEXPIRETIME", b"h:ttl", b"FIELDS", b"1", b"\xfa\xfb"]),
            V::Arr(vec![V::Int(4102444800123)])
        );
        assert_eq!(digest_across(&mut m), digest_across(&mut r));
    }
}
