// tests/compressed_snapshot.rs
//
// Compressed snapshots (#879). With `rdbcompression yes`, the default, a
// snapshot is the plain KDB image (`KIVIDB01`), unchanged byte for byte, in
// blocks compressed with zstd (`KIVIDBZ1`); with `no`, the plain image as
// before. Both load; a damaged or partial compressed snapshot never does.
//
// Real `kividb` processes, each in its own directory.

mod common;

use common::{free_port, start_verified};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const PLAIN: &[u8; 8] = b"KIVIDB01";
const COMPRESSED: &[u8; 8] = b"KIVIDBZ1";

fn kividb(dir: &Path, port: u16, extra: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kividb"));
    cmd.current_dir(dir)
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--configfile",
            "kividb.conf",
        ])
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(dir: &Path, extra: &[&str]) -> Server {
    let (child, port) = start_verified(|port| kividb(dir, port, extra).spawn().unwrap());
    Server { child, port }
}

/// A reply, as the test compares replies: bulk strings as their bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Reply {
    Bulk(Vec<u8>),
    Simple(String),
    Error(String),
    Int(i64),
    Nil,
    Array(Vec<Reply>),
}

impl Reply {
    fn text(&self) -> String {
        match self {
            Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Reply::Simple(s) | Reply::Error(s) => s.clone(),
            Reply::Int(n) => n.to_string(),
            Reply::Nil => "(nil)".into(),
            Reply::Array(items) => items.iter().map(Reply::text).collect::<Vec<_>>().join("\n"),
        }
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Conn {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                let reader = BufReader::new(stream.try_clone().unwrap());
                return Conn { stream, reader };
            }
            assert!(Instant::now() < deadline, "no server on {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn send<A: AsRef<[u8]>>(&mut self, args: &[A]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            let arg = arg.as_ref();
            out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).unwrap();
    }

    fn call<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Reply {
        self.send(args);
        self.read()
    }

    fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> String {
        self.call(args).text()
    }

    fn ok<A: AsRef<[u8]>>(&mut self, args: &[A]) {
        let reply = self.call(args);
        assert!(
            !matches!(reply, Reply::Error(_)),
            "{:?}: {reply:?}",
            args.iter()
                .map(|a| String::from_utf8_lossy(a.as_ref()).into_owned())
                .collect::<Vec<_>>()
        );
    }

    fn read(&mut self) -> Reply {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        let (kind, rest) = line.split_at(1);
        match kind {
            "$" | "=" if rest == "-1" => Reply::Nil,
            "$" | "=" => {
                let len: usize = rest.parse().unwrap();
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                Reply::Bulk(data)
            }
            "*" | "~" | ">" if rest == "-1" => Reply::Nil,
            "*" | "~" | ">" => {
                let n: usize = rest.parse().unwrap();
                Reply::Array((0..n).map(|_| self.read()).collect())
            }
            "%" => {
                let n: usize = rest.parse().unwrap();
                Reply::Array((0..2 * n).map(|_| self.read()).collect())
            }
            "_" => Reply::Nil,
            "+" => Reply::Simple(rest.to_string()),
            "-" => Reply::Error(rest.to_string()),
            ":" => Reply::Int(rest.parse().unwrap()),
            "," | "#" | "(" => Reply::Simple(rest.to_string()),
            _ => panic!("unexpected reply line {line:?}"),
        }
    }

    fn info(&mut self, field: &str) -> String {
        self.cmd(&["INFO", "persistence"])
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")).map(str::to_string))
            .unwrap_or_else(|| panic!("INFO has no {field}"))
    }
}

/// Every key of every database, with its type, its expiry and everything
/// that says what it holds: what two servers loaded from the same data agree
/// on, whatever order their tables keep.
fn key_dump(c: &mut Conn) -> Vec<Vec<Reply>> {
    let mut all = Vec::new();
    for db in 0..16 {
        c.ok(&["SELECT", &db.to_string()]);
        let mut cursor = "0".to_string();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        loop {
            let reply = c.call(&["SCAN", &cursor, "COUNT", "1000"]);
            let Reply::Array(parts) = reply else {
                panic!("SCAN {cursor} in db {db}: {reply:?}");
            };
            cursor = parts[0].text();
            if let Reply::Array(found) = &parts[1] {
                for k in found {
                    if let Reply::Bulk(k) = k {
                        keys.push(k.clone());
                    }
                }
            }
            if cursor == "0" {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        for key in keys {
            let kind = c.call(&[&b"TYPE"[..], &key]);
            let mut row = vec![Reply::Int(db), Reply::Bulk(key.clone()), kind.clone()];
            row.push(c.call(&[&b"PEXPIRETIME"[..], &key]));
            // A vector set's digest and `DUMP` follow its graph's order,
            // which a load builds again: compared by its elements.
            if kind.text() != "vectorset" {
                row.push(c.call(&[&b"DEBUG"[..], b"DIGEST-VALUE", &key]));
            }
            // `DUMP` of a hash or a set lays its members out in the table's
            // order, which differs between two servers: compared sorted. That
            // of a stream holds its consumers' times, which go on: compared
            // by what they hold.
            let kind_text = kind.text();
            if !["hash", "set", "stream", "vectorset"].contains(&kind_text.as_str()) {
                row.push(c.call(&[&b"DUMP"[..], &key]));
            }
            match kind_text.as_str() {
                "hash" => {
                    let Reply::Array(all_fields) = c.call(&[&b"HGETALL"[..], &key]) else {
                        panic!("HGETALL");
                    };
                    let mut pairs: Vec<Vec<Reply>> =
                        all_fields.chunks(2).map(<[Reply]>::to_vec).collect();
                    pairs.sort();
                    let mut cmd: Vec<Vec<u8>> = vec![
                        b"HPEXPIRETIME".to_vec(),
                        key.clone(),
                        b"FIELDS".to_vec(),
                        pairs.len().to_string().into_bytes(),
                    ];
                    cmd.extend(pairs.iter().map(|p| p[0].text().into_bytes()));
                    row.push(Reply::Array(pairs.concat()));
                    row.push(c.call(&cmd));
                }
                "set" => {
                    let Reply::Array(mut members) = c.call(&[&b"SMEMBERS"[..], &key]) else {
                        panic!("SMEMBERS");
                    };
                    members.sort();
                    row.push(Reply::Array(members));
                }
                "stream" => {
                    row.push(c.call(&[&b"XRANGE"[..], &key, b"-", b"+"]));
                    row.push(c.call(&[&b"XINFO"[..], b"GROUPS", &key]));
                    row.push(c.call(&[&b"XPENDING"[..], &key, b"g"]));
                }
                "vectorset" => {
                    row.push(c.call(&[&b"VCARD"[..], &key]));
                    row.push(c.call(&[&b"VDIM"[..], &key]));
                    for i in 0..50 {
                        let e = format!("e{i}").into_bytes();
                        row.push(c.call(&[&b"VEMB"[..], &key, &e]));
                    }
                }
                "TSDB-TYPE" => {
                    row.push(c.call(&[&b"TS.RANGE"[..], &key, b"-", b"+"]));
                }
                "ReJSON-RL" => row.push(c.call(&[&b"JSON.GET"[..], &key])),
                _ => {}
            }
            all.push(row);
        }
    }
    c.ok(&["SELECT", "0"]);
    all
}

/// What the server holds besides its keys.
fn extras(c: &mut Conn) -> Vec<String> {
    let mut out = vec![c.cmd(&["FT._LIST"])];
    out.push(
        c.cmd(&["FT.SEARCH", "idx", "*", "NOCONTENT", "LIMIT", "0", "0"])
            .lines()
            .next()
            .unwrap_or_default()
            .to_string(),
    );
    out.push(c.cmd(&["FT.INFO", "al"]).lines().take(2).collect());
    if cfg!(feature = "lua") {
        out.push(c.cmd(&["FCALL", "f879", "0"]));
    }
    out
}

/// A server holding every kind of value, in every representation, in more
/// than one database, with times to live on keys and on hash fields, a
/// search index with an alias, and with Lua a function library.
fn populate(c: &mut Conn) {
    // Strings: small, large, integers, binary, one that does not compress.
    c.ok(&["SET", "s:small", "v"]);
    c.ok(&["SET", "s:int", "12345"]);
    c.ok(&["SET", "s:large", &"kividb ".repeat(100_000)]);
    let mut x = 88172645463325252u64;
    let noise: Vec<u8> = (0..300_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    c.ok(&[&b"SET"[..], b"s:noise", &noise]);
    c.ok(&[&b"SET"[..], b"s:binary\x00\xff", b"\x00\x01\x02\xfe\xff"]);
    c.ok(&["SET", "", "the empty key"]);
    c.ok(&["SET", "s:ttl", "v", "PX", "86400000"]);
    for i in 0..20_000 {
        c.send(&["SET", &format!("bulk:{i}"), &format!("value-{i}-{}", i * 7)]);
    }
    for _ in 0..20_000 {
        c.read();
    }
    // Sets: integers, packed, plain.
    c.ok(&["SADD", "set:int", "1", "2", "3", "400000"]);
    c.ok(&["SADD", "set:packed", "a", "b", "c"]);
    let many: Vec<String> = (0..1000).map(|i| format!("member-{i}")).collect();
    let mut cmd = vec!["SADD".to_string(), "set:plain".into()];
    cmd.extend(many.iter().cloned());
    c.ok(&cmd);
    // Hashes: packed, plain, with field TTLs.
    c.ok(&["HSET", "h:packed", "f1", "v1", "f2", "v2"]);
    let mut cmd = vec!["HSET".to_string(), "h:plain".into()];
    for i in 0..1000 {
        cmd.push(format!("field-{i}"));
        cmd.push(format!("value-{i}"));
    }
    c.ok(&cmd);
    c.ok(&["HSET", "h:ttl", "a", "1", "b", "2", "c", "3"]);
    c.ok(&["HPEXPIRE", "h:ttl", "86400000", "FIELDS", "2", "a", "b"]);
    // Lists: packed, plain.
    c.ok(&["RPUSH", "l:packed", "a", "b", "c"]);
    let mut cmd = vec!["RPUSH".to_string(), "l:plain".into()];
    cmd.extend((0..5000).map(|i| format!("element-{i}")));
    c.ok(&cmd);
    // Sorted sets, small and large, and a geo set.
    c.ok(&["ZADD", "z:small", "1", "a", "2.5", "b", "-inf", "c"]);
    let mut cmd = vec!["ZADD".to_string(), "z:large".into()];
    for i in 0..2000 {
        cmd.push(format!("{}", i as f64 / 3.0));
        cmd.push(format!("m{i}"));
    }
    c.ok(&cmd);
    c.ok(&["GEOADD", "geo", "13.361389", "38.115556", "Palermo"]);
    // A stream with a consumer group and pending entries.
    for i in 0..50 {
        c.ok(&["XADD", "st", "*", "n", &i.to_string()]);
    }
    c.ok(&["XGROUP", "CREATE", "st", "g", "0"]);
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "10",
        "STREAMS",
        "st",
        ">",
    ]);
    // A vector set, a time series, a Bloom filter, a JSON document, an
    // HyperLogLog.
    for i in 0..50 {
        c.ok(&[
            "VADD",
            "vs",
            "VALUES",
            "3",
            &format!("{}", i as f32),
            "1",
            "0.5",
            &format!("e{i}"),
        ]);
    }
    c.ok(&["TS.CREATE", "ts", "LABELS", "room", "attic"]);
    for i in 1..200 {
        c.ok(&["TS.ADD", "ts", &(i * 1000).to_string(), &i.to_string()]);
    }
    c.ok(&["BF.RESERVE", "bf", "0.01", "1000"]);
    c.ok(&["BF.MADD", "bf", "a", "b", "c"]);
    c.ok(&["JSON.SET", "doc", "$", r#"{"a":[1,2,{"b":"c"}],"n":null}"#]);
    c.ok(&["PFADD", "hll", "a", "b", "c", "d"]);
    // A search index with an alias, over hashes.
    c.ok(&[
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
        "n",
        "NUMERIC",
    ]);
    for i in 0..100 {
        c.ok(&[
            "HSET",
            &format!("doc:{i}"),
            "t",
            "hello world",
            "n",
            &i.to_string(),
        ]);
    }
    c.ok(&["FT.ALIASADD", "al", "idx"]);
    if cfg!(feature = "lua") {
        c.ok(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=lib879\nredis.register_function('f879', function() return 879 end)",
        ]);
    }
    // Another database.
    c.ok(&["SELECT", "3"]);
    c.ok(&["SET", "db3", "three", "EX", "86400"]);
    c.ok(&["HSET", "db3:h", "f", "v"]);
    c.ok(&["SELECT", "0"]);
}

fn run_tool(args: &[&Path], name: &str) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_kividb"))
        .arg(name)
        .args(args)
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr),
    )
}

fn save(c: &mut Conn, dir: &Path, compression: &str) -> Vec<u8> {
    c.ok(&["CONFIG", "SET", "rdbcompression", compression]);
    assert_eq!(c.cmd(&["SAVE"]), "OK");
    std::fs::read(dir.join("dump.kdb")).unwrap()
}

/// What a compressed snapshot holds is the plain image, byte for byte, and
/// it loads to the same dataset: every key, value, expiry, field expiry,
/// index, alias and library.
#[test]
fn a_compressed_snapshot_holds_the_plain_image_and_loads_the_same() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), &[]);
    let mut c = Conn::connect(server.port);
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "rdbcompression"]),
        "rdbcompression\nyes"
    );
    populate(&mut c);
    let expected = key_dump(&mut c);
    let expected_extras = extras(&mut c);
    assert!(expected.len() > 20_000);

    let plain = save(&mut c, dir.path(), "no");
    assert!(plain.starts_with(PLAIN));
    assert_eq!(c.info("rdb_last_save_raw_size"), plain.len().to_string());
    assert_eq!(c.info("rdb_last_save_stored_size"), plain.len().to_string());
    let compressed = save(&mut c, dir.path(), "yes");
    assert!(compressed.starts_with(COMPRESSED));
    assert_eq!(c.info("rdb_last_save_raw_size"), plain.len().to_string());
    assert_eq!(
        c.info("rdb_last_save_stored_size"),
        compressed.len().to_string()
    );
    assert!(
        compressed.len() * 2 < plain.len(),
        "{} compressed for {} plain",
        compressed.len(),
        plain.len()
    );
    drop(c);
    drop(server);

    // The image in it is the plain one, byte for byte.
    let tools = tempfile::tempdir().unwrap();
    let zpath = tools.path().join("z.kdb");
    let ppath = tools.path().join("p.kdb");
    std::fs::write(&zpath, &compressed).unwrap();
    std::fs::write(&ppath, &plain).unwrap();
    let out = tools.path().join("out.kdb");
    let (ok, said) = run_tool(&[&zpath, &out], "kdb-decompress");
    assert!(ok, "{said}");
    assert_eq!(std::fs::read(&out).unwrap(), plain, "decompressed");
    let again = tools.path().join("again.kdb");
    let (ok, said) = run_tool(&[&ppath, &again], "kdb-compress");
    assert!(ok, "{said}");
    assert!(std::fs::read(&again).unwrap().starts_with(COMPRESSED));
    let (ok, said) = run_tool(&[&again, &out], "kdb-decompress");
    assert!(ok, "{said}");
    assert_eq!(std::fs::read(&out).unwrap(), plain, "compressed again");
    let (ok, _) = run_tool(&[&zpath], "kdb-decompress");
    assert!(!ok, "a missing argument");

    // Each loads to the dataset it was saved from.
    for (what, image) in [("plain", &plain), ("compressed", &compressed)] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dump.kdb"), image).unwrap();
        let server = start(dir.path(), &[]);
        let mut c = Conn::connect(server.port);
        let loaded = key_dump(&mut c);
        assert_eq!(loaded.len(), expected.len(), "{what}");
        for (a, b) in loaded.iter().zip(&expected) {
            assert_eq!(a, b, "{what}");
        }
        assert_eq!(extras(&mut c), expected_extras, "{what}");
    }
}

/// `rdbcompression` takes yes or no, from `CONFIG SET` and from the config
/// file, and nothing else.
#[test]
fn rdbcompression_is_a_setting_in_effect() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("kividb.conf"), "rdbcompression no\n").unwrap();
    let server = start(dir.path(), &[]);
    let mut c = Conn::connect(server.port);
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "rdbcompression"]),
        "rdbcompression\nno"
    );
    c.ok(&["SET", "k", "v"]);
    assert!(save(&mut c, dir.path(), "no").starts_with(PLAIN));
    assert!(
        c.cmd(&["CONFIG", "SET", "rdbcompression", "maybe"])
            .contains("argument must be 'yes' or 'no'")
    );
    assert!(save(&mut c, dir.path(), "YES").starts_with(COMPRESSED));
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "rdbcompression"]),
        "rdbcompression\nyes"
    );
    // BGSAVE too.
    c.ok(&["SET", "k2", "v2"]);
    assert_eq!(c.cmd(&["BGSAVE"]), "Background saving started");
    let deadline = Instant::now() + Duration::from_secs(30);
    while c.info("rdb_bgsave_in_progress") != "0" || c.info("rdb_last_bgsave_status") != "ok" {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        std::fs::read(dir.path().join("dump.kdb"))
            .unwrap()
            .starts_with(COMPRESSED)
    );
    // And the snapshot written at shutdown.
    c.ok(&["SET", "k3", "v3"]);
    c.ok(&["CONFIG", "SET", "rdbcompression", "no"]);
    c.send(&["SHUTDOWN", "SAVE"]);
    drop(c);
    let mut server = server;
    let _ = server.child.wait();
    let dump = std::fs::read(dir.path().join("dump.kdb")).unwrap();
    assert!(dump.starts_with(PLAIN));
    let restarted = start(dir.path(), &[]);
    let mut c = Conn::connect(restarted.port);
    assert_eq!(c.cmd(&["GET", "k3"]), "v3");
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

/// Whether a server started in `dir` exited with an error within 30 s, and
/// what it logged.
fn fails_to_start(dir: &Path) -> (bool, String) {
    let mut child = kividb(dir, free_port(), &[])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let said = std::thread::spawn(move || {
        let mut said = String::new();
        let _ = stderr.read_to_string(&mut said);
        said
    });
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
    (failed, said.join().unwrap())
}

/// Where each block of a compressed snapshot starts, and its end.
fn block_starts(c: &[u8]) -> Vec<usize> {
    let mut at = 20;
    let mut all = vec![at];
    while c[at] != b'E' {
        let stored = u32::from_le_bytes(c[at + 5..at + 9].try_into().unwrap()) as usize;
        at += 17 + stored;
        all.push(at);
    }
    all
}

/// A compressed `dump.kdb` cut short anywhere, or damaged anywhere, does not
/// start the server, which says which block and leaves its directory as it
/// was.
#[test]
fn a_damaged_compressed_dump_does_not_start_the_server() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path(), &[]);
    let mut c = Conn::connect(server.port);
    c.ok(&["SET", "timed", "v", "EX", "100000"]);
    for i in 0..30_000 {
        c.send(&["SET", &format!("key:{i}"), &format!("{i:x}-{}", i * 31)]);
    }
    for _ in 0..30_000 {
        c.read();
    }
    let image = save(&mut c, dir.path(), "yes");
    drop(c);
    drop(server);
    let blocks = block_starts(&image);
    assert!(blocks.len() > 2, "{} blocks", blocks.len());
    let end = *blocks.last().unwrap();

    let mut damaged: Vec<(String, Vec<u8>)> = Vec::new();
    for &at in &blocks {
        damaged.push((format!("cut at {at}"), image[..at].to_vec()));
        damaged.push((format!("cut at {}", at + 9), image[..at + 9].to_vec()));
    }
    damaged.push(("cut mid-block".into(), image[..blocks[1] + 100].to_vec()));
    damaged.push((
        "cut inside the end".into(),
        image[..image.len() - 3].to_vec(),
    ));
    damaged.push(("cut inside the header".into(), image[..12].to_vec()));
    let mut flip = |what: &str, at: usize| {
        let mut d = image.clone();
        d[at] ^= 0x08;
        damaged.push((what.to_string(), d));
    };
    flip("a bit in block 1's bytes", blocks[0] + 17 + 50);
    flip("a bit in block 2's checksum", blocks[1] + 9);
    flip("a bit in block 2's length", blocks[1] + 2);
    flip("a bit in the header", 13);
    flip("a bit in the end marker", end + 3);
    let mut d = image.clone();
    d.extend_from_slice(b"garbage");
    damaged.push(("bytes after the end".into(), d));

    for (what, bytes) in damaged {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dump.kdb"), &bytes).unwrap();
        std::fs::write(dir.path().join("kividb.lock"), b"").unwrap();
        let before = directory(dir.path());
        let (failed, stderr) = fails_to_start(dir.path());
        assert!(failed, "{what}: the server started");
        assert!(
            stderr.contains("dump.kdb cannot be loaded") && stderr.contains("compressed snapshot"),
            "{what}: {stderr}"
        );
        assert!(
            directory(dir.path()) == before,
            "{what}: the directory changed"
        );
    }

    // The whole file starts it, with the key's time to live.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("dump.kdb"), &image).unwrap();
    let server = start(dir.path(), &[]);
    let mut c = Conn::connect(server.port);
    assert_eq!(c.cmd(&["DBSIZE"]), "30001");
    assert!(c.cmd(&["TTL", "timed"]).parse::<i64>().unwrap() > 0);
}

/// With the AOF on, its base snapshot is written as `rdbcompression` says,
/// and restarts load it, and the log after it, either way; a rewrite after
/// the setting changes writes the other form.
#[test]
fn the_aof_base_follows_rdbcompression_and_restarts_load_both() {
    let dir = tempfile::tempdir().unwrap();
    let extra = ["--appendonly", "yes"];
    let mut server = start(dir.path(), &extra);
    let mut c = Conn::connect(server.port);
    let mut written = 0;
    let mut write = |c: &mut Conn, n: usize| {
        for _ in 0..n {
            c.ok(&["SET", &format!("k{written}"), &format!("v{written}")]);
            c.ok(&["RPUSH", "list", &written.to_string()]);
            written += 1;
        }
        written
    };
    let base = |dir: &Path| -> Vec<u8> {
        let found = ["dump.kdb", "appendonlydir/dump.kdb"]
            .iter()
            .map(|p| dir.join(p))
            .find(|p| p.exists())
            .expect("no base snapshot");
        std::fs::read(found).unwrap()
    };
    let rewrite = |c: &mut Conn, dir: &Path, magic: &[u8; 8]| {
        assert!(c.cmd(&["BGREWRITEAOF"]).starts_with("Background"));
        let deadline = Instant::now() + Duration::from_secs(30);
        while c.info("aof_rewrite_in_progress") != "0" || !base(dir).starts_with(magic) {
            assert!(Instant::now() < deadline, "the rewrite did not finish");
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    write(&mut c, 200);
    rewrite(&mut c, dir.path(), COMPRESSED);
    let mut n = write(&mut c, 100);
    drop(c);
    drop(server);
    for step in 0..2 {
        server = start(dir.path(), &extra);
        let mut c = Conn::connect(server.port);
        assert_eq!(
            c.cmd(&["GET", &format!("k{}", n - 1)]),
            format!("v{}", n - 1)
        );
        assert_eq!(c.cmd(&["LLEN", "list"]), n.to_string());
        assert_eq!(c.cmd(&["DBSIZE"]), (n + 1).to_string());
        if step == 0 {
            // A base written compressed, then the setting turned off.
            c.ok(&["CONFIG", "SET", "rdbcompression", "no"]);
            write(&mut c, 50);
            rewrite(&mut c, dir.path(), PLAIN);
            n = write(&mut c, 50);
        }
        drop(c);
        drop(server);
    }
}

/// A replica of this build is sent its full sync compressed, and one that
/// does not say it reads that (KiviDB 1.2 and earlier, Redis) the plain
/// image; a master with `rdbcompression no` sends every replica the plain one.
#[test]
fn a_full_sync_is_compressed_only_for_a_replica_that_takes_it() {
    let dir = tempfile::tempdir().unwrap();
    let master = start(dir.path(), &[]);
    let mut c = Conn::connect(master.port);
    for i in 0..5000 {
        c.send(&["SET", &format!("key:{i}"), &format!("value {i} value {i}")]);
    }
    for _ in 0..5000 {
        c.read();
    }
    c.ok(&["HSET", "h", "f", "v"]);
    c.ok(&["HPEXPIRE", "h", "86400000", "FIELDS", "1", "f"]);
    c.ok(&["SET", "timed", "v", "EX", "86400"]);

    // What a replica is sent, as a replica of 1.2 asks (no `kdb-blocks`) and
    // as one of this build asks.
    let payload_magic = |capas: &[&str]| -> Vec<u8> {
        let mut r = Conn::connect(master.port);
        let mut cmd = vec!["REPLCONF", "listening-port", "1"];
        r.ok(&cmd);
        cmd = vec!["REPLCONF"];
        for capa in capas {
            cmd.extend(["capa", capa]);
        }
        r.ok(&cmd);
        r.send(&["PSYNC", "?", "-1"]);
        let mut line = String::new();
        r.reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("+FULLRESYNC"), "{line}");
        line.clear();
        r.reader.read_line(&mut line).unwrap();
        assert!(line.starts_with('$'), "{line}");
        let mut magic = vec![0u8; 8];
        r.reader.read_exact(&mut magic).unwrap();
        magic
    };
    assert_eq!(payload_magic(&["psync2"]), PLAIN);
    assert_eq!(
        payload_magic(&["dataset-digest", "lost-data", "vset-quant"]),
        PLAIN
    );
    assert_eq!(payload_magic(&["psync2", "kdb-blocks"]), COMPRESSED);
    c.ok(&["CONFIG", "SET", "rdbcompression", "no"]);
    assert_eq!(payload_magic(&["psync2", "kdb-blocks"]), PLAIN);
    c.ok(&["CONFIG", "SET", "rdbcompression", "yes"]);

    // A replica of this build: synced from a compressed payload, whole.
    let rdir = tempfile::tempdir().unwrap();
    let replica = start(
        rdir.path(),
        &["--replicaof", "127.0.0.1", &master.port.to_string()],
    );
    let mut r = Conn::connect(replica.port);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = r.cmd(&["INFO", "replication"]);
        if info.contains("master_link_status:up") && r.cmd(&["DBSIZE"]) == "5002" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the replica did not sync: {info}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(r.cmd(&["GET", "key:4999"]), "value 4999 value 4999");
    assert!(r.cmd(&["TTL", "timed"]).parse::<i64>().unwrap() > 0);
    assert_eq!(
        r.cmd(&["HPEXPIRETIME", "h", "FIELDS", "1", "f"]),
        c.cmd(&["HPEXPIRETIME", "h", "FIELDS", "1", "f"])
    );
    c.ok(&["SET", "after", "sync"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while r.cmd(&["GET", "after"]) != "sync" {
        assert!(Instant::now() < deadline, "the stream did not follow");
        std::thread::sleep(Duration::from_millis(50));
    }
    let info = c.cmd(&["INFO", "stats"]);
    let full: u64 = info
        .lines()
        .find_map(|l| l.strip_prefix("sync_full:"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(full, 5, "one full sync each, no more: {info}");
}
