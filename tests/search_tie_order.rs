// tests/search_tie_order.rs
//
// Documents a query finds level come in the order they were written, as in
// Redis, which gives them in the order of its document ids: under SORTBY,
// ascending, and its reverse descending; without SORTBY, where every match
// scores the same; in FT.AGGREGATE. A write that names a field of the index
// makes its document the last written; one of other fields of the hash
// leaves it where it is. The order is the same after a restart from a
// snapshot or the AOF, and on a replica.
//
// Keys are named by a hash of the document's number, so that their order is
// not the order they were written in. Each document holds its number in `i`.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
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
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(s) => {
                    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
                    let writer = s.try_clone().unwrap();
                    return Self {
                        reader: BufReader::new(s),
                        writer,
                    };
                }
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Err(e) => panic!("could not connect to port {port}: {e}"),
            }
        }
    }

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    /// `args`, which must not fail.
    fn ok(&mut self, args: &[&str]) -> V {
        let reply = self.cmd(args);
        assert!(!matches!(reply, V::Err(_)), "{args:?}: {reply:?}");
        reply
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

    /// The numbers (`i`) of an FT.SEARCH reply's documents, in order.
    fn search(&mut self, args: &[&str]) -> Vec<String> {
        let mut full = vec!["FT.SEARCH"];
        full.extend_from_slice(args);
        let reply = self.ok(&full);
        reply.arr()[1..]
            .chunks(2)
            .map(|pair| field(pair[1].arr(), "i"))
            .collect()
    }

    /// The numbers (`i`) of an FT.AGGREGATE reply's rows, in order.
    fn aggregate(&mut self, args: &[&str]) -> Vec<String> {
        let mut full = vec!["FT.AGGREGATE"];
        full.extend_from_slice(args);
        let reply = self.ok(&full);
        reply.arr()[1..]
            .iter()
            .map(|row| field(row.arr(), "i"))
            .collect()
    }
}

/// The value of `name` in a flat list of names and values.
fn field(pairs: &[V], name: &str) -> String {
    pairs
        .chunks(2)
        .find(|pair| pair[0].s() == name)
        .map(|pair| pair[1].s().to_string())
        .unwrap_or_else(|| panic!("no {name} in {pairs:?}"))
}

// ── Servers ───────────────────────────────────────────────────────────────────

fn kividb_bin() -> String {
    std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        format!("{manifest}/target/debug/kividb")
    })
}

/// A data directory of its own, taken away when the test is done with it.
struct Dir(PathBuf);

impl std::ops::Deref for Dir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn data_dir(tag: &str) -> Dir {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("kivi_tie_order_{tag}_{}_{ts}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    Dir(dir)
}

struct Server {
    child: Child,
    port: u16,
    /// The data directory of a server that has it to itself.
    _dir: Option<Dir>,
}

impl Server {
    /// A server in `dir`, with `args` besides its port.
    fn start(dir: &Path, args: &[String]) -> Self {
        let (child, port) = common::start_verified(|port| {
            Command::new(kividb_bin())
                .arg("--port")
                .arg(port.to_string())
                .args(args)
                .current_dir(dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn kividb")
        });
        let server = Self {
            child,
            port,
            _dir: None,
        };
        server.wait_loaded();
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// Until the server answers commands (its indexes rebuilt).
    fn wait_loaded(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let reply = self.conn().cmd(&["FT._LIST"]);
            if !matches!(&reply, V::Err(e) if e.starts_with("LOADING")) {
                return;
            }
            assert!(Instant::now() < deadline, "still loading");
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn server() -> Server {
    let dir = data_dir("live");
    let mut server = Server::start(&dir, &["--save".into(), String::new()]);
    server._dir = Some(dir);
    server
}

// ── Documents ─────────────────────────────────────────────────────────────────

/// The key of document `i` under `prefix`: a hash of its number, so that
/// keys do not sort in the order the documents are written.
fn key(prefix: &str, i: usize) -> String {
    let mut x = (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    x ^= x >> 31;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 29;
    format!("{prefix}{:012x}", x & 0xffff_ffff_ffff)
}

/// `0..n` as text.
fn numbers(range: impl Iterator<Item = usize>) -> Vec<String> {
    range.map(|i| i.to_string()).collect()
}

/// An index `ix` over hashes under `prefix` with a TAG `p` and a sortable
/// NUMERIC `t`, a sortable TAG `g` and a sortable TEXT `x`, and `n` of them
/// written in order, each `p = pp`, `t = 5`, `g = gg`, `x = same words`.
fn index_with(c: &mut Conn, ix: &str, prefix: &str, n: usize) -> Vec<String> {
    c.ok(&[
        "FT.CREATE",
        ix,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "p",
        "TAG",
        "t",
        "NUMERIC",
        "SORTABLE",
        "g",
        "TAG",
        "SORTABLE",
        "x",
        "TEXT",
        "SORTABLE",
    ]);
    let keys: Vec<String> = (0..n).map(|i| key(prefix, i)).collect();
    for (i, k) in keys.iter().enumerate() {
        let i = i.to_string();
        c.ok(&[
            "HSET",
            k,
            "i",
            &i,
            "p",
            "pp",
            "t",
            "5",
            "g",
            "gg",
            "x",
            "same words",
        ]);
    }
    let mut sorted = keys.clone();
    sorted.sort();
    assert_ne!(sorted, keys, "the keys sort in the order they were written");
    keys
}

/// FT.SEARCH `ix` for `@p:{pp}`, RETURN `i`, with `rest`.
fn tied(c: &mut Conn, ix: &str, rest: &[&str]) -> Vec<String> {
    let mut args = vec![ix, "@p:{pp}", "RETURN", "1", "i"];
    args.extend_from_slice(rest);
    args.extend_from_slice(&["DIALECT", "2"]);
    c.search(&args)
}

fn reversed(mut v: Vec<String>) -> Vec<String> {
    v.reverse();
    v
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn level_documents_come_in_the_order_they_were_written() {
    let server = server();
    let mut c = server.conn();
    index_with(&mut c, "ix", "e:", 8);
    let written = numbers(0..8);

    // The issue's query: the first three written.
    assert_eq!(
        tied(&mut c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "3"]),
        numbers(0..3)
    );
    // Every sortable kind of field, both ways: ascending the order written,
    // descending its reverse.
    for f in ["t", "g", "x"] {
        let asc = tied(&mut c, "ix", &["SORTBY", f, "ASC", "LIMIT", "0", "8"]);
        assert_eq!(asc, written, "SORTBY {f} ASC");
        let desc = tied(&mut c, "ix", &["SORTBY", f, "DESC", "LIMIT", "0", "8"]);
        assert_eq!(desc, reversed(written.clone()), "SORTBY {f} DESC");
    }
    // Without SORTBY, every match scoring the same.
    assert_eq!(tied(&mut c, "ix", &["LIMIT", "0", "8"]), written);
    let star = c.search(&["ix", "*", "RETURN", "1", "i", "LIMIT", "0", "8"]);
    assert_eq!(star, written, "*");
    let range = c.search(&["ix", "@t:[5 5]", "RETURN", "1", "i", "LIMIT", "0", "8"]);
    assert_eq!(range, written, "@t:[5 5]");
    let words = c.search(&["ix", "same", "RETURN", "1", "i", "LIMIT", "0", "8"]);
    assert_eq!(words, written, "a word every document holds");

    // The same from every document read, without the postings.
    c.ok(&["FT.DEBUG", "POSTINGS", "ix", "OFF"]);
    assert_eq!(
        tied(&mut c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "3"]),
        numbers(0..3)
    );
    assert_eq!(
        tied(&mut c, "ix", &["SORTBY", "t", "DESC", "LIMIT", "0", "8"]),
        reversed(written.clone())
    );
    assert_eq!(tied(&mut c, "ix", &["LIMIT", "0", "8"]), written);
    c.ok(&["FT.DEBUG", "POSTINGS", "ix", "ON"]);
    server.stop();
}

#[test]
fn documents_without_the_value_come_last_in_the_order_written() {
    let server = server();
    let mut c = server.conn();
    c.ok(&[
        "FT.CREATE",
        "ix",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "c:",
        "SCHEMA",
        "p",
        "TAG",
        "t",
        "NUMERIC",
        "SORTABLE",
    ]);
    for i in 0..8 {
        let n = i.to_string();
        if i % 2 == 0 {
            c.ok(&["HSET", &key("c:", i), "i", &n, "p", "pp", "t", "5"]);
        } else {
            c.ok(&["HSET", &key("c:", i), "i", &n, "p", "pp"]);
        }
    }
    let asc = tied(&mut c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "8"]);
    assert_eq!(asc, ["0", "2", "4", "6", "1", "3", "5", "7"]);
    let desc = tied(&mut c, "ix", &["SORTBY", "t", "DESC", "LIMIT", "0", "8"]);
    assert_eq!(desc, ["6", "4", "2", "0", "7", "5", "3", "1"]);
    // Pages of them continue each other, both ways.
    for (way, whole) in [("ASC", asc), ("DESC", desc)] {
        let mut paged = Vec::new();
        for from in [0, 3, 6] {
            let from = from.to_string();
            paged.extend(tied(
                &mut c,
                "ix",
                &["SORTBY", "t", way, "LIMIT", &from, "3"],
            ));
        }
        assert_eq!(paged, whole, "{way}");
    }
    server.stop();
}

#[test]
fn a_write_that_names_an_indexed_field_makes_its_document_the_last() {
    let server = server();
    let mut c = server.conn();
    // Each case on an index of its own: what it does to document 2.
    type Case = fn(&mut Conn, &[String]);
    let cases: [(&str, Case, bool); 11] = [
        (
            "HSET of every field",
            |c, k| {
                c.ok(&["HSET", &k[2], "i", "2", "p", "pp", "t", "5"]);
            },
            true,
        ),
        (
            "HSET of an indexed field, same value",
            |c, k| {
                c.ok(&["HSET", &k[2], "t", "5"]);
            },
            true,
        ),
        (
            "HSET of a field no index reads",
            |c, k| {
                c.ok(&["HSET", &k[2], "zz", "1"]);
            },
            false,
        ),
        (
            "HINCRBY of an indexed field",
            |c, k| {
                c.ok(&["HINCRBY", &k[2], "t", "0"]);
            },
            true,
        ),
        (
            "HINCRBY of a field no index reads",
            |c, k| {
                c.ok(&["HINCRBY", &k[2], "cnt", "1"]);
            },
            false,
        ),
        (
            "HDEL of a field no index reads",
            |c, k| {
                c.ok(&["HSET", &k[2], "zz", "1"]);
                c.ok(&["HDEL", &k[2], "zz"]);
            },
            false,
        ),
        (
            "HDEL of an indexed field",
            |c, k| {
                c.ok(&["HDEL", &k[2], "p"]);
                c.ok(&["HSET", &k[2], "p", "pp"]);
            },
            true,
        ),
        (
            "DEL, then written again",
            |c, k| {
                c.ok(&["DEL", &k[2]]);
                c.ok(&["HSET", &k[2], "i", "2", "p", "pp", "t", "5"]);
            },
            true,
        ),
        (
            "expired, then written again",
            |c, k| {
                c.ok(&["PEXPIRE", &k[2], "20"]);
                thread::sleep(Duration::from_millis(200));
                c.ok(&["EXISTS", &k[2]]);
                c.ok(&["HSET", &k[2], "i", "2", "p", "pp", "t", "5"]);
            },
            true,
        ),
        (
            "EXPIRE",
            |c, k| {
                c.ok(&["EXPIRE", &k[2], "1000"]);
            },
            false,
        ),
        (
            "PERSIST",
            |c, k| {
                c.ok(&["EXPIRE", &k[2], "1000"]);
                c.ok(&["PERSIST", &k[2]]);
            },
            false,
        ),
    ];
    for (n, (what, write, moves)) in cases.iter().enumerate() {
        let ix = format!("ix{n}");
        let prefix = format!("d{n}:");
        c.ok(&[
            "FT.CREATE",
            &ix,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            &prefix,
            "SCHEMA",
            "p",
            "TAG",
            "t",
            "NUMERIC",
            "SORTABLE",
        ]);
        let keys: Vec<String> = (0..8).map(|i| key(&prefix, i)).collect();
        for (i, k) in keys.iter().enumerate() {
            c.ok(&["HSET", k, "i", &i.to_string(), "p", "pp", "t", "5"]);
        }
        write(&mut c, &keys);
        let want: Vec<String> = if *moves {
            ["0", "1", "3", "4", "5", "6", "7", "2"]
                .map(String::from)
                .to_vec()
        } else {
            numbers(0..8)
        };
        let asc = tied(&mut c, &ix, &["SORTBY", "t", "ASC", "LIMIT", "0", "9"]);
        assert_eq!(asc, want, "{what}: SORTBY t ASC");
        let desc = tied(&mut c, &ix, &["SORTBY", "t", "DESC", "LIMIT", "0", "9"]);
        assert_eq!(desc, reversed(want.clone()), "{what}: SORTBY t DESC");
        assert_eq!(
            tied(&mut c, &ix, &["LIMIT", "0", "9"]),
            want,
            "{what}: no SORTBY"
        );
    }
    // A copy is a document of its own, written last.
    c.ok(&[
        "FT.CREATE",
        "cp",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "cp:",
        "SCHEMA",
        "p",
        "TAG",
        "t",
        "NUMERIC",
        "SORTABLE",
    ]);
    for i in 0..4 {
        c.ok(&[
            "HSET",
            &key("cp:", i),
            "i",
            &i.to_string(),
            "p",
            "pp",
            "t",
            "5",
        ]);
    }
    c.ok(&["COPY", &key("cp:", 1), "cp:copy"]);
    let asc = tied(&mut c, "cp", &["SORTBY", "t", "ASC", "LIMIT", "0", "9"]);
    assert_eq!(asc, ["0", "1", "2", "3", "1"]);
    server.stop();
}

#[test]
fn pages_continue_each_other() {
    let server = server();
    let mut c = server.conn();
    index_with(&mut c, "ix", "h:", 20);
    for rest in [
        &["SORTBY", "t", "ASC"][..],
        &["SORTBY", "t", "DESC"][..],
        &[][..],
    ] {
        let mut whole = rest.to_vec();
        whole.extend_from_slice(&["LIMIT", "0", "20"]);
        let whole = tied(&mut c, "ix", &whole);
        let mut paged = Vec::new();
        for from in (0..20).step_by(5) {
            let from = from.to_string();
            let mut args = rest.to_vec();
            args.extend_from_slice(&["LIMIT", &from, "5"]);
            paged.extend(tied(&mut c, "ix", &args));
        }
        assert_eq!(paged, whole, "{rest:?}");
        let written = numbers(0..20);
        if rest.contains(&"DESC") {
            assert_eq!(whole, reversed(written), "{rest:?}");
        } else {
            assert_eq!(whole, written, "{rest:?}");
        }
    }
    server.stop();
}

#[test]
fn aggregate_rows_come_in_the_order_written() {
    let server = server();
    let mut c = server.conn();
    c.ok(&[
        "FT.CREATE",
        "ix",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "g:",
        "SCHEMA",
        "p",
        "TAG",
        "t",
        "NUMERIC",
        "SORTABLE",
        "u",
        "NUMERIC",
        "SORTABLE",
    ]);
    for i in 0..12 {
        let (n, u) = (i.to_string(), (i % 2).to_string());
        c.ok(&["HSET", &key("g:", i), "i", &n, "p", "pp", "t", "5", "u", &u]);
    }
    let agg = |c: &mut Conn, rest: &[&str]| {
        let mut args = vec!["ix", "@p:{pp}", "LOAD", "1", "@i"];
        args.extend_from_slice(rest);
        args.extend_from_slice(&["DIALECT", "2"]);
        c.aggregate(&args)
    };
    let written = numbers(0..12);
    assert_eq!(agg(&mut c, &["LIMIT", "0", "12"]), written, "no SORTBY");
    assert_eq!(
        agg(&mut c, &["SORTBY", "2", "@t", "ASC", "LIMIT", "0", "12"]),
        written
    );
    assert_eq!(
        agg(&mut c, &["SORTBY", "2", "@t", "DESC", "LIMIT", "0", "12"]),
        reversed(written.clone())
    );
    // Level on every key: in the direction of the last.
    assert_eq!(
        agg(
            &mut c,
            &["SORTBY", "4", "@t", "ASC", "@u", "DESC", "LIMIT", "0", "12"]
        ),
        ["11", "9", "7", "5", "3", "1", "10", "8", "6", "4", "2", "0"]
    );
    assert_eq!(
        agg(
            &mut c,
            &["SORTBY", "4", "@u", "ASC", "@t", "ASC", "LIMIT", "0", "12"]
        ),
        ["0", "2", "4", "6", "8", "10", "1", "3", "5", "7", "9", "11"]
    );
    assert_eq!(
        agg(&mut c, &["SORTBY", "2", "@t", "ASC", "MAX", "3"]),
        numbers(0..3)
    );
    assert_eq!(
        agg(&mut c, &["SORTBY", "2", "@u", "ASC", "MAX", "4"]),
        ["0", "2", "4", "6"]
    );
    // Every document read, without the postings.
    c.ok(&["FT.DEBUG", "POSTINGS", "ix", "OFF"]);
    assert_eq!(
        agg(&mut c, &["SORTBY", "2", "@t", "ASC", "MAX", "3"]),
        numbers(0..3)
    );
    assert_eq!(
        agg(&mut c, &["LIMIT", "0", "12"]),
        written,
        "no SORTBY, no postings"
    );
    server.stop();
}

#[test]
fn equally_near_vectors_come_in_the_order_written() {
    let server = server();
    let mut c = server.conn();
    // Two FLOAT32s whose bytes are text, so that the client sends them as
    // they are: every document holds the same vector.
    let vector = "AAAABBBB".to_string();
    for algo in ["FLAT", "HNSW"] {
        let ix = format!("k{algo}");
        let prefix = format!("{algo}:");
        c.ok(&[
            "FT.CREATE",
            &ix,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            &prefix,
            "SCHEMA",
            "v",
            "VECTOR",
            algo,
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
            "p",
            "TAG",
        ]);
        for i in 0..8 {
            c.ok(&[
                "HSET",
                &key(&prefix, i),
                "i",
                &i.to_string(),
                "p",
                "pp",
                "v",
                &vector,
            ]);
        }
        for query in ["*=>[KNN 8 @v $q]", "(@p:{pp})=>[KNN 8 @v $q]"] {
            let got = c.search(&[
                &ix, query, "PARAMS", "2", "q", &vector, "RETURN", "1", "i", "LIMIT", "0", "8",
                "DIALECT", "2",
            ]);
            assert_eq!(got, numbers(0..8), "{algo} {query}");
        }
    }
    server.stop();
}

/// octelium's eviction: of 1,100 entries with the same `t`, the 256 it
/// removes are the first 256 written.
#[test]
fn the_first_rows_of_many_level_ones_are_the_first_written() {
    let server = server();
    let mut c = server.conn();
    c.ok(&[
        "FT.CREATE",
        "ix",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "o:",
        "SCHEMA",
        "c",
        "TAG",
        "p",
        "TAG",
        "t",
        "NUMERIC",
        "SORTABLE",
    ]);
    for i in 0..1100 {
        let n = i.to_string();
        let k = key("o:", i);
        c.send(&[
            "HSET",
            &k,
            "i",
            &n,
            "c",
            "cc",
            "p",
            "pp",
            "t",
            "9007199254740991",
        ]);
    }
    for _ in 0..1100 {
        assert!(!matches!(c.read(), V::Err(_)));
    }
    let reply = c.ok(&[
        "FT.SEARCH",
        "ix",
        "@c:{cc} @p:{pp}",
        "RETURN",
        "1",
        "t",
        "SORTBY",
        "t",
        "ASC",
        "LIMIT",
        "0",
        "256",
        "DIALECT",
        "2",
    ]);
    let keys: Vec<String> = reply.arr()[1..]
        .chunks(2)
        .map(|pair| pair[0].s().to_string())
        .collect();
    let first: Vec<String> = (0..256).map(|i| key("o:", i)).collect();
    assert_eq!(keys, first);
    server.stop();
}

/// Documents written, one written over and one deleted and written again,
/// in an index `ix` (prefix `p:`): the order they then have.
fn mixed_writes(c: &mut Conn) -> Vec<String> {
    index_with(c, "ix", "p:", 10);
    c.ok(&["HSET", &key("p:", 3), "t", "5"]);
    c.ok(&["DEL", &key("p:", 1)]);
    // Whole again: a document of fewer words would score otherwise.
    c.ok(&[
        "HSET",
        &key("p:", 1),
        "i",
        "1",
        "p",
        "pp",
        "t",
        "5",
        "g",
        "gg",
        "x",
        "same words",
    ]);
    let want = ["0", "2", "4", "5", "6", "7", "8", "9", "3", "1"]
        .map(String::from)
        .to_vec();
    assert_eq!(
        tied(c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "10"]),
        want
    );
    want
}

/// The order after a restart, both ways and without SORTBY.
fn check_order(c: &mut Conn, want: &[String], when: &str) {
    assert_eq!(
        tied(c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "10"]),
        want,
        "{when}: ASC"
    );
    assert_eq!(
        tied(c, "ix", &["SORTBY", "t", "DESC", "LIMIT", "0", "10"]),
        reversed(want.to_vec()),
        "{when}: DESC"
    );
    assert_eq!(
        tied(c, "ix", &["LIMIT", "0", "10"]),
        want,
        "{when}: no SORTBY"
    );
}

#[test]
fn the_order_survives_a_restart_from_a_snapshot() {
    let dir = data_dir("snapshot");
    let args = ["--save".to_string(), String::new()];
    let server = Server::start(&dir, &args);
    let want = mixed_writes(&mut server.conn());
    server.conn().ok(&["SAVE"]);
    server.stop();

    let server = Server::start(&dir, &args);
    check_order(&mut server.conn(), &want, "after a restart");
    // Written after the restart: last.
    let mut c = server.conn();
    c.ok(&["HSET", &key("p:", 0), "t", "5"]);
    let mut then = want[1..].to_vec();
    then.push("0".into());
    check_order(&mut c, &then, "written after the restart");
    server.stop();
}

#[test]
fn the_order_survives_a_restart_from_the_aof() {
    let dir = data_dir("aof");
    let args: Vec<String> = [
        "--save",
        "",
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
    ]
    .map(String::from)
    .to_vec();
    let server = Server::start(&dir, &args);
    let want = mixed_writes(&mut server.conn());
    server.stop();

    // Every write replayed from the log.
    let server = Server::start(&dir, &args);
    check_order(&mut server.conn(), &want, "replayed");

    // A rewrite, then writes after it: the snapshot's order, then the
    // log's.
    let mut c = server.conn();
    c.ok(&["BGREWRITEAOF"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = c.ok(&["INFO", "persistence"]);
        if info.s().contains("aof_rewrite_in_progress:0") {
            break;
        }
        assert!(Instant::now() < deadline, "the rewrite did not finish");
        thread::sleep(Duration::from_millis(50));
    }
    c.ok(&["HSET", &key("p:", 0), "t", "5"]);
    c.ok(&["HSET", &key("p:", 4), "zz", "not indexed"]);
    let mut then = want[1..].to_vec();
    then.push("0".into());
    check_order(&mut c, &then, "before the second restart");
    server.stop();

    let server = Server::start(&dir, &args);
    check_order(
        &mut server.conn(),
        &then,
        "from the rewrite and the log after it",
    );
    server.stop();
}

#[test]
fn a_replica_gives_the_order_of_its_master() {
    let master_dir = data_dir("master");
    let master = Server::start(&master_dir, &["--save".into(), String::new()]);
    let mut m = master.conn();
    // Before the replica: it has them from the full sync.
    let mut want = mixed_writes(&mut m);

    let replica_dir = data_dir("replica");
    let replica = Server::start(
        &replica_dir,
        &[
            "--save".into(),
            String::new(),
            "--replicaof".into(),
            "127.0.0.1".into(),
            master.port.to_string(),
        ],
    );
    // After it: from the stream.
    m.ok(&["HSET", &key("p:", 5), "t", "5"]);
    for i in 10..14 {
        let n = i.to_string();
        m.ok(&[
            "HSET",
            &key("p:", i),
            "i",
            &n,
            "p",
            "pp",
            "t",
            "5",
            "g",
            "gg",
            "x",
            "same words",
        ]);
    }
    want.retain(|n| n != "5");
    want.push("5".into());
    want.extend(numbers(10..14));

    let mut r = replica.conn();
    let deadline = Instant::now() + Duration::from_secs(30);
    let query = ["SORTBY", "t", "ASC", "LIMIT", "0", "20"];
    loop {
        let reply = r.cmd(&[
            "FT.SEARCH",
            "ix",
            "@p:{pp}",
            "RETURN",
            "1",
            "i",
            "SORTBY",
            "t",
            "ASC",
            "LIMIT",
            "0",
            "20",
            "DIALECT",
            "2",
        ]);
        if let V::Arr(items) = &reply
            && items.len() == 1 + 2 * want.len()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the replica did not catch up: {reply:?}"
        );
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(tied(&mut m, "ix", &query), want, "master");
    assert_eq!(tied(&mut r, "ix", &query), want, "replica");
    let desc = ["SORTBY", "t", "DESC", "LIMIT", "0", "20"];
    assert_eq!(tied(&mut r, "ix", &desc), tied(&mut m, "ix", &desc), "DESC");
    let plain = ["LIMIT", "0", "20"];
    assert_eq!(
        tied(&mut r, "ix", &plain),
        tied(&mut m, "ix", &plain),
        "no SORTBY"
    );
    replica.stop();
    master.stop();
}

#[test]
fn a_renamed_document_keeps_its_place() {
    let dir = data_dir("rename");
    let args: Vec<String> = [
        "--save",
        "",
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
    ]
    .map(String::from)
    .to_vec();
    let server = Server::start(&dir, &args);
    let mut c = server.conn();
    index_with(&mut c, "ix", "r:", 8);
    let written = numbers(0..8);
    c.ok(&["RENAME", &key("r:", 2), "r:renamed"]);
    // Onto a key that is there: nothing renamed, nothing moved.
    c.ok(&["RENAMENX", &key("r:", 3), &key("r:", 4)]);
    let check = |c: &mut Conn, when: &str| {
        let asc = tied(c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "8"]);
        assert_eq!(asc, written, "{when}: ASC");
        let desc = tied(c, "ix", &["SORTBY", "t", "DESC", "LIMIT", "0", "8"]);
        assert_eq!(desc, reversed(written.clone()), "{when}: DESC");
    };
    check(&mut c, "renamed");
    server.stop();

    // Replayed from the log.
    let server = Server::start(&dir, &args);
    check(&mut server.conn(), "replayed");
    // After a rewrite: a rename in the log after it keeps the snapshot's
    // place.
    let mut c = server.conn();
    c.ok(&["BGREWRITEAOF"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !c
        .ok(&["INFO", "persistence"])
        .s()
        .contains("aof_rewrite_in_progress:0")
    {
        assert!(Instant::now() < deadline, "the rewrite did not finish");
        thread::sleep(Duration::from_millis(50));
    }
    c.ok(&["RENAME", &key("r:", 5), "r:renamed-again"]);
    check(&mut c, "renamed again");
    server.stop();
    let server = Server::start(&dir, &args);
    check(&mut server.conn(), "from the rewrite and the log after it");
    server.stop();
}

/// A snapshot written by 1.2.2 (index `ix` over `f:`, documents 0 to 5
/// written in that order) holds no order of its documents: they load in
/// the order of their keys, and documents written after come after them.
#[test]
fn a_snapshot_of_1_2_2_loads_its_documents_in_key_order() {
    let dir = data_dir("v122");
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/tie_order_1_2_2.kdb"
    );
    std::fs::copy(fixture, dir.join("dump.kdb")).unwrap();
    let server = Server::start(&dir, &["--save".into(), String::new()]);
    let mut c = server.conn();
    let by_key: Vec<String> = {
        let mut keys: Vec<(String, usize)> = (0..6).map(|i| (key("f:", i), i)).collect();
        keys.sort();
        keys.into_iter().map(|(_, i)| i.to_string()).collect()
    };
    assert_ne!(by_key, numbers(0..6));
    let asc = tied(&mut c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "6"]);
    assert_eq!(asc, by_key, "ASC");
    let desc = tied(&mut c, "ix", &["SORTBY", "t", "DESC", "LIMIT", "0", "6"]);
    assert_eq!(desc, reversed(by_key.clone()), "DESC");
    c.ok(&["HSET", &key("f:", 0), "t", "5"]);
    let mut then: Vec<String> = by_key.into_iter().filter(|n| n != "0").collect();
    then.push("0".into());
    assert_eq!(
        tied(&mut c, "ix", &["SORTBY", "t", "ASC", "LIMIT", "0", "6"]),
        then
    );
    server.stop();
}
