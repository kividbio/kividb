#![allow(clippy::disallowed_methods)]
// tests/search_resp3_shape.rs
//
// The shape of the search commands' replies over RESP3: `search-resp3-reply`,
// and the valkey-glide clients that `auto` answers as valkey-search does.
//
// Byte-level: every reply is read raw and compared with a capture. In
// tests/fixtures:
//   valkey_search_9_2_0_resp3.hex   what valkey/valkey-bundle:9.2.0 answered
//                                   over RESP3 to the commands below;
//   kividb_search_redis_resp3.hex   what KiviDB answers in the Redis shape;
//   kividb_search_resp2.hex         what KiviDB answers over RESP2.
// search_capture.py sends the commands and writes such a file.
//
// valkey-search lists a document's fields in the order of its own table, so a
// list of name/value pairs is compared as a set of pairs; the text of a score
// that is a fused rank, a cursor's id and the time a reply took are not
// compared, nor the score of a KNN hit (see `masked`).
//
// Every test starts a server process of its own: the setting is the server's.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod common;
use common::start_verified;

const VALKEY: &str = include_str!("fixtures/valkey_search_9_2_0_resp3.hex");
const REDIS: &str = include_str!("fixtures/kividb_search_redis_resp3.hex");
const RESP2: &str = include_str!("fixtures/kividb_search_resp2.hex");

// ── Server ────────────────────────────────────────────────────────────────────

struct Server {
    child: Option<Child>,
    port: u16,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        Self::start_with(extra, "")
    }

    /// With `conf` as the configuration file.
    fn start_with(extra: &[&str], conf: &str) -> Self {
        let dir: PathBuf = std::env::temp_dir().join(format!(
            "kividb-search-resp3-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("kividb.conf"), conf).unwrap();
        let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let io_model = std::env::var("KIVIDB_TEST_IO_MODEL").ok();
        let (child, port) = start_verified(|port| {
            Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(&dir)
                .args([
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--threads",
                    "2",
                    "--configfile",
                    "kividb.conf",
                ])
                .args(io_model.iter().flat_map(|m| ["--io-model", m.as_str()]))
                .args(&extra)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("could not start kividb")
        });
        Self {
            child: Some(child),
            port,
        }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// ── Raw client ────────────────────────────────────────────────────────────────

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
                    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
                    let writer = s.try_clone().unwrap();
                    return Self {
                        reader: BufReader::new(s),
                        writer,
                    };
                }
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                Err(e) => panic!("could not connect to port {port}: {e}"),
            }
        }
    }

    /// The reply's bytes, as sent.
    fn raw(&mut self, args: &[&[u8]]) -> Vec<u8> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        let mut out = Vec::new();
        self.frame(&mut out);
        out
    }

    fn cmd(&mut self, args: &[&str]) -> Vec<u8> {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
    }

    fn frame(&mut self, out: &mut Vec<u8>) {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).expect("reply");
        assert!(line.ends_with(b"\r\n"), "short reply {line:?}");
        out.extend_from_slice(&line);
        let head = String::from_utf8_lossy(&line[1..line.len() - 2]).into_owned();
        match line[0] {
            b'$' | b'=' | b'!' => {
                let len: i64 = head.parse().unwrap();
                if len >= 0 {
                    let mut data = vec![0u8; len as usize + 2];
                    self.reader.read_exact(&mut data).unwrap();
                    out.extend_from_slice(&data);
                }
            }
            b'*' | b'~' | b'>' => {
                let n: i64 = head.parse().unwrap();
                for _ in 0..n.max(0) {
                    self.frame(out);
                }
            }
            b'%' => {
                let n: i64 = head.parse().unwrap();
                for _ in 0..2 * n.max(0) {
                    self.frame(out);
                }
            }
            _ => {}
        }
    }

    fn ok(&mut self, args: &[&str]) {
        let r = self.cmd(args);
        assert_eq!(r, b"+OK\r\n", "{args:?}: {}", show(&r));
    }
}

fn show(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace("\r\n", "|")
}

// ── Replies as trees ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Node {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Bulk(Vec<u8>),
    Int(i64),
    Double(String),
    Null,
    Array(Vec<Node>),
    Map(Vec<(Node, Node)>),
}

fn parse(bytes: &[u8]) -> Node {
    let mut at = 0;
    let node = parse_at(bytes, &mut at);
    assert_eq!(at, bytes.len(), "bytes after the reply: {}", show(bytes));
    node
}

fn parse_at(bytes: &[u8], at: &mut usize) -> Node {
    let end = *at + bytes[*at..].windows(2).position(|w| w == b"\r\n").unwrap();
    let kind = bytes[*at];
    let head = &bytes[*at + 1..end];
    let text = String::from_utf8_lossy(head).into_owned();
    *at = end + 2;
    match kind {
        b'+' => Node::Simple(head.to_vec()),
        b'-' => Node::Error(head.to_vec()),
        b':' => Node::Int(text.parse().unwrap()),
        b',' => Node::Double(text),
        b'_' => Node::Null,
        b'$' | b'=' => {
            let len: i64 = text.parse().unwrap();
            if len < 0 {
                return Node::Null;
            }
            let data = bytes[*at..*at + len as usize].to_vec();
            *at += len as usize + 2;
            Node::Bulk(data)
        }
        b'*' | b'~' => {
            let n: i64 = text.parse().unwrap();
            Node::Array((0..n.max(0)).map(|_| parse_at(bytes, at)).collect())
        }
        b'%' => {
            let n: usize = text.parse().unwrap();
            Node::Map(
                (0..n)
                    .map(|_| (parse_at(bytes, at), parse_at(bytes, at)))
                    .collect(),
            )
        }
        other => panic!(
            "unexpected RESP type {:?} in {}",
            other as char,
            show(bytes)
        ),
    }
}

/// A list of name/value pairs in any order is one order: any array inside
/// the reply, with strings only and an even number of them.
fn canonical(node: Node, depth: usize) -> Node {
    match node {
        Node::Array(items) => {
            let strings = items
                .iter()
                .all(|n| matches!(n, Node::Bulk(_) | Node::Simple(_)));
            let mut items: Vec<Node> = items.into_iter().map(|n| canonical(n, depth + 1)).collect();
            if depth > 0 && strings && !items.is_empty() && items.len().is_multiple_of(2) {
                let mut pairs: Vec<(Node, Node)> = items
                    .chunks(2)
                    .map(|p| (p[0].clone(), p[1].clone()))
                    .collect();
                pairs.sort();
                items = pairs.into_iter().flat_map(|(a, b)| [a, b]).collect();
            }
            Node::Array(items)
        }
        Node::Map(pairs) => Node::Map(
            pairs
                .into_iter()
                .map(|(k, v)| (canonical(k, depth + 1), canonical(v, depth + 1)))
                .collect(),
        ),
        other => other,
    }
}

/// What differs from one run to the next, or between the two servers, and is
/// not the shape: a cursor's id, the time a reply took, and a score that is
/// the result of fusing ranks (valkey-search computes it in single precision).
fn masked(name: &str, node: Node) -> Node {
    fn is_score(n: &Node) -> bool {
        matches!(n, Node::Bulk(b) | Node::Simple(b) if b == b"__score")
    }
    fn walk(name: &str, node: Node) -> Node {
        match node {
            Node::Double(d) if name.starts_with("hybrid") => {
                assert!(d.parse::<f64>().is_ok(), "{d}");
                Node::Double("0".into())
            }
            Node::Array(mut items) => {
                for i in 0..items.len() {
                    if name == "hybrid_key" && is_score(&items[i]) && i + 1 < items.len() {
                        assert!(matches!(items[i + 1], Node::Bulk(_)), "{:?}", items[i + 1]);
                        items[i + 1] = Node::Bulk(b"0".to_vec());
                    }
                }
                Node::Array(items.into_iter().map(|n| walk(name, n)).collect())
            }
            Node::Map(pairs) => Node::Map(
                pairs
                    .into_iter()
                    .map(|(k, v)| {
                        let v = if name == "hybrid_key" && is_score(&k) {
                            assert!(matches!(v, Node::Bulk(_)), "{v:?}");
                            Node::Bulk(b"0".to_vec())
                        } else {
                            v
                        };
                        (k, walk(name, v))
                    })
                    .collect(),
            ),
            other => other,
        }
    }
    let mut node = walk(name, node);
    // The score of a KNN hit: valkey-search says 0, as a bulk string;
    // KiviDB, as Redis does, the distance.
    if name == "knn_withscores"
        && let Node::Array(items) = &mut node
    {
        for i in (2..items.len()).step_by(3) {
            assert!(matches!(items[i], Node::Bulk(_)), "{:?}", items[i]);
            items[i] = Node::Bulk(b"0".to_vec());
        }
    }
    if name == "agg_cursor"
        && let Node::Array(items) = &mut node
        && let Some(last) = items.last_mut()
    {
        assert!(matches!(last, Node::Int(id) if *id > 0), "{last:?}");
        *last = Node::Int(0);
    }
    node
}

fn fixture(file: &str, name: &str) -> Vec<u8> {
    file.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| {
            let (n, hex) = l.split_once('\t')?;
            (n == name).then(|| {
                (0..hex.trim().len() / 2)
                    .map(|i| u8::from_str_radix(&hex.trim()[2 * i..2 * i + 2], 16).unwrap())
                    .collect()
            })
        })
        .unwrap_or_else(|| panic!("no fixture {name}"))
}

fn same(file: &str, name: &str, got: &[u8]) {
    let want = fixture(file, name);
    let a = canonical(masked(name, parse(got)), 0);
    let b = canonical(masked(name, parse(&want)), 0);
    assert_eq!(a, b, "{name}\n  got  {}\n  want {}", show(got), show(&want));
}

// ── The cases ─────────────────────────────────────────────────────────────────

fn vector(x: f32, y: f32) -> Vec<u8> {
    [x, y].iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// The data of the captures.
fn load(c: &mut Conn) {
    let r = c.cmd(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
        "t",
        "TAG",
        "n",
        "NUMERIC",
        "v",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert_eq!(r, b"+OK\r\n");
    for (key, tag, n, v, extra) in [
        ("d:1", "a", "1", vector(0.0, 0.0), "x1"),
        ("d:2", "a", "2", vector(1.0, 0.0), "x2"),
        ("d:3", "b", "3", vector(0.0, 2.0), "x3"),
    ] {
        c.raw(&[
            b"HSET",
            key.as_bytes(),
            b"t",
            tag.as_bytes(),
            b"n",
            n.as_bytes(),
            b"v",
            &v,
            b"extra",
            extra.as_bytes(),
        ]);
    }
}

/// The commands of search_capture.py, by name.
fn cases() -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let b = |s: &str| s.as_bytes().to_vec();
    let q = "*=>[KNN 3 @v $B AS dist]";
    let blob = vector(0.0, 0.0);
    let p = |mut v: Vec<Vec<u8>>| {
        v.extend([
            b("PARAMS"),
            b("2"),
            b("B"),
            blob.clone(),
            b("DIALECT"),
            b("2"),
        ]);
        v
    };
    let c = |words: &[&str]| -> Vec<Vec<u8>> { words.iter().map(|w| b(w)).collect() };
    let hy = |tail: &[&str]| -> Vec<Vec<u8>> {
        let mut v = c(&["FT.HYBRID", "idx", "SEARCH", "@t:{a}", "VSIM", "@v", "$B"]);
        v.extend(c(tail));
        v.extend([b("PARAMS"), b("2"), b("B"), blob.clone()]);
        v
    };
    vec![
        ("knn_all", p(c(&["FT.SEARCH", "idx", q]))),
        (
            "knn_return",
            p(c(&["FT.SEARCH", "idx", q, "RETURN", "2", "t", "dist"])),
        ),
        (
            "knn_return_vec",
            p(c(&["FT.SEARCH", "idx", q, "RETURN", "2", "v", "dist"])),
        ),
        ("knn_return0", p(c(&["FT.SEARCH", "idx", q, "RETURN", "0"]))),
        ("knn_nocontent", p(c(&["FT.SEARCH", "idx", q, "NOCONTENT"]))),
        (
            "knn_limit00",
            p(c(&["FT.SEARCH", "idx", q, "LIMIT", "0", "0"])),
        ),
        (
            "knn_limit11",
            p(c(&[
                "FT.SEARCH",
                "idx",
                q,
                "RETURN",
                "1",
                "dist",
                "LIMIT",
                "1",
                "1",
            ])),
        ),
        (
            "knn_withscores",
            p(c(&[
                "FT.SEARCH",
                "idx",
                q,
                "WITHSCORES",
                "RETURN",
                "1",
                "dist",
            ])),
        ),
        (
            "knn_filter",
            p(c(&[
                "FT.SEARCH",
                "idx",
                "(@t:{a})=>[KNN 3 @v $B AS dist]",
                "RETURN",
                "1",
                "dist",
            ])),
        ),
        (
            "knn_empty",
            p(c(&[
                "FT.SEARCH",
                "idx",
                "(@t:{zzz})=>[KNN 3 @v $B AS dist]",
            ])),
        ),
        (
            "knn_empty_nocontent",
            p(c(&[
                "FT.SEARCH",
                "idx",
                "(@t:{zzz})=>[KNN 3 @v $B AS dist]",
                "NOCONTENT",
            ])),
        ),
        ("tag_one", c(&["FT.SEARCH", "idx", "@t:{b}"])),
        (
            "tag_one_return",
            c(&["FT.SEARCH", "idx", "@t:{b}", "RETURN", "1", "n"]),
        ),
        ("tag_empty", c(&["FT.SEARCH", "idx", "@t:{zzz}"])),
        ("num_one", c(&["FT.SEARCH", "idx", "@n:[3 3]", "NOCONTENT"])),
        (
            "num_sortby",
            c(&[
                "FT.SEARCH",
                "idx",
                "@n:[1 3]",
                "SORTBY",
                "n",
                "DESC",
                "NOCONTENT",
            ]),
        ),
        (
            "agg_basic",
            c(&[
                "FT.AGGREGATE",
                "idx",
                "@n:[1 3]",
                "LOAD",
                "1",
                "@n",
                "SORTBY",
                "2",
                "@n",
                "ASC",
            ]),
        ),
        (
            "agg_groupby",
            c(&[
                "FT.AGGREGATE",
                "idx",
                "*",
                "GROUPBY",
                "1",
                "@t",
                "REDUCE",
                "COUNT",
                "0",
                "AS",
                "c",
                "SORTBY",
                "2",
                "@t",
                "ASC",
            ]),
        ),
        (
            "agg_empty",
            c(&["FT.AGGREGATE", "idx", "@t:{zzz}", "LOAD", "1", "@n"]),
        ),
        (
            "agg_knn",
            p(c(&["FT.AGGREGATE", "idx", q, "LOAD", "1", "@dist"])),
        ),
        ("list", c(&["FT._LIST"])),
        ("hybrid_key", hy(&[])),
        (
            "hybrid_load",
            hy(&["COMBINE", "RRF", "2", "CONSTANT", "60", "LOAD", "1", "@n"]),
        ),
        (
            "hybrid_load_key",
            hy(&[
                "KNN", "2", "K", "2", "COMBINE", "RRF", "2", "CONSTANT", "60", "LOAD", "2", "@n",
                "@__key",
            ]),
        ),
        (
            "hybrid_empty",
            c(&[
                "FT.HYBRID",
                "idx",
                "SEARCH",
                "@t:{zzz}",
                "VSIM",
                "@v",
                "$B",
                "KNN",
                "2",
                "K",
                "2",
                "FILTER",
                "@t:{zzz}",
                "PARAMS",
                "2",
                "B",
            ])
            .into_iter()
            .chain([blob.clone()])
            .collect(),
        ),
    ]
}

/// The cursor, which needs the id the first reply gave.
fn cursor_cases(c: &mut Conn) -> Vec<(&'static str, Vec<u8>)> {
    let first = c.cmd(&[
        "FT.AGGREGATE",
        "idx",
        "@n:[1 3]",
        "LOAD",
        "1",
        "@n",
        "SORTBY",
        "2",
        "@n",
        "ASC",
        "WITHCURSOR",
        "COUNT",
        "2",
    ]);
    let text = String::from_utf8_lossy(&first).into_owned();
    let id = text
        .trim_end()
        .rsplit(':')
        .next()
        .unwrap()
        .trim()
        .to_string();
    let read = c.cmd(&["FT.CURSOR", "READ", "idx", &id]);
    vec![("agg_cursor", first), ("agg_cursor_read", read)]
}

/// Every case, run on `c` (which has the data), as (name, reply).
fn run_all(c: &mut Conn) -> Vec<(&'static str, Vec<u8>)> {
    let mut out: Vec<(&'static str, Vec<u8>)> = cases()
        .into_iter()
        .map(|(name, args)| {
            let args: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
            (name, c.raw(&args))
        })
        .collect();
    out.extend(cursor_cases(c));
    out
}

/// A connection to a server that has the data, speaking `proto`.
fn prepared(server: &Server, proto: &str) -> Conn {
    let mut c = server.conn();
    load(&mut c);
    let r = c.cmd(&["HELLO", proto]);
    assert!(
        r.starts_with(if proto == "3" { b"%" } else { b"*" }),
        "{}",
        show(&r)
    );
    c
}

fn assert_all(file: &str, replies: &[(&str, Vec<u8>)]) {
    for (name, reply) in replies {
        same(file, name, reply);
    }
}

fn is_array(reply: &[u8]) -> bool {
    reply.starts_with(b"*")
}

fn is_map(reply: &[u8]) -> bool {
    reply.starts_with(b"%")
}

fn knn(c: &mut Conn) -> Vec<u8> {
    let blob = vector(0.0, 0.0);
    c.raw(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 3 @v $B AS dist]",
        b"RETURN",
        b"1",
        b"dist",
        b"PARAMS",
        b"2",
        b"B",
        &blob,
        b"DIALECT",
        b"2",
    ])
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// `valkey`: every reply is valkey-search's, byte for byte (field order and
/// the digits of a fused score apart).
#[test]
fn valkey_mode_answers_as_valkey_search_does() {
    let server = Server::start(&["--search-resp3-reply", "valkey"]);
    let mut c = prepared(&server, "3");
    let replies = run_all(&mut c);
    assert_all(VALKEY, &replies);
    // The reply to FT.INFO: a flat array, a simple string for each name,
    // and none of the strings valkey-glide reads as an error.
    let info = c.cmd(&["FT.INFO", "idx"]);
    assert!(is_array(&info), "{}", show(&info));
    let text = show(&info);
    assert!(
        text.contains("|+index_name|$3|idx|+index_options|*0|+index_definition|*6|"),
        "{text}"
    );
    assert!(text.contains("|+num_docs|:3|"), "{text}");
    assert!(!text.contains("fields"), "{text}");
    // The names of every list in it, as valkey-search writes them.
    let Node::Array(items) = parse(&info) else {
        panic!()
    };
    let simple_names = |list: &[Node], what: &str| {
        for pair in list.chunks(2) {
            assert!(matches!(pair[0], Node::Simple(_)), "{what}: {:?}", pair[0]);
        }
    };
    simple_names(&items, "FT.INFO");
    let mut checked = 0;
    for pair in items.chunks(2) {
        let Node::Simple(name) = &pair[0] else {
            panic!()
        };
        match (name.as_slice(), &pair[1]) {
            (b"index_definition", Node::Array(list)) => {
                simple_names(list, "index_definition");
                checked += 1;
            }
            (b"attributes" | b"hnsw_indexes", Node::Array(lists)) => {
                for list in lists {
                    let Node::Array(list) = list else { panic!() };
                    simple_names(list, "attribute");
                    checked += 1;
                }
            }
            _ => {}
        }
    }
    assert!(checked >= 5, "{checked}");
    assert!(text.contains("|+attributes|*3|"), "{text}");
    // A vector field keeps what the index holds in an `index` list, as
    // valkey-search lays it out (valkey-glide's callers read `dimensions`
    // there), where the Redis shape lists them one after another.
    assert!(
        text.contains(
            "|+type|$6|VECTOR|+index|*8|+dimensions|:2|+distance_metric|+L2|\
             +data_type|+FLOAT32|+algorithm|*8|+name|+HNSW|+m|:"
        ),
        "{text}"
    );
    assert!(text.contains("|+ef_construction|:"), "{text}");
    assert!(text.contains("|+ef_runtime|:"), "{text}");
    assert!(!text.contains("|+dim|"), "{text}");
}

/// FT.PROFILE answers Redis's shape (a map), and the search inside it
/// Redis's too. A connection that takes valkey-search's shape (`valkey`, or a
/// GLIDE client under `auto`) gets the layout valkey-glide's converter reads:
/// an array of the search's reply in that shape and a flat list of names
/// (simple strings) and numbers.
#[test]
fn profile_is_a_map_for_redis_and_the_glide_layout_for_valkey() {
    // Times are not the shape.
    fn untimed(node: Node) -> Node {
        match node {
            Node::Double(_) => Node::Double("0".into()),
            Node::Array(items) => Node::Array(items.into_iter().map(untimed).collect()),
            Node::Map(pairs) => Node::Map(
                pairs
                    .into_iter()
                    .map(|(k, v)| {
                        let timed = matches!(&k, Node::Bulk(b) if b.ends_with(b" time"));
                        (
                            k,
                            if timed {
                                Node::Bulk(Vec::new())
                            } else {
                                untimed(v)
                            },
                        )
                    })
                    .collect(),
            ),
            other => other,
        }
    }
    let profiles = |c: &mut Conn| -> Vec<Vec<u8>> {
        vec![
            c.cmd(&["FT.PROFILE", "idx", "SEARCH", "QUERY", "*"]),
            c.cmd(&[
                "FT.PROFILE",
                "idx",
                "AGGREGATE",
                "QUERY",
                "@n:[1 3]",
                "LOAD",
                "1",
                "@n",
            ]),
        ]
    };
    let server = Server::start(&["--search-resp3-reply", "redis"]);
    let mut c = prepared(&server, "3");
    let want = profiles(&mut c);
    for reply in &want {
        assert!(is_map(reply), "{}", show(reply));
    }
    // The search inside is a map too.
    assert!(
        show(&want[0]).contains("$7|results|*3|%"),
        "{}",
        show(&want[0])
    );
    for (mode, glide) in [("valkey", false), ("auto", true)] {
        let server = Server::start(&["--search-resp3-reply", mode]);
        let mut c = prepared(&server, "3");
        if glide {
            c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "GlidePy"]);
        }
        // The same command is a flat array for the commands that have a shape.
        assert!(is_array(&knn(&mut c)), "{mode}");
        for got in profiles(&mut c) {
            let Node::Array(parts) = parse(&got) else {
                panic!("{mode}: {}", show(&got));
            };
            assert_eq!(parts.len(), 2, "{mode}: {}", show(&got));
            // The search's own reply: an array, not a map.
            assert!(matches!(parts[0], Node::Array(_)), "{mode}: {}", show(&got));
            let Node::Array(stats) = &parts[1] else {
                panic!("{mode}: {}", show(&got));
            };
            assert_eq!(stats.len(), 6, "{mode}: {}", show(&got));
            for pair in stats.chunks(2) {
                assert!(matches!(pair[0], Node::Simple(_)), "{mode}: {:?}", pair[0]);
                let Node::Bulk(number) = &pair[1] else {
                    panic!("{mode}: {:?}", pair[1]);
                };
                assert!(
                    String::from_utf8_lossy(number).parse::<f64>().is_ok(),
                    "{mode}: {number:?}"
                );
            }
        }
        // The same connection, named something else, is back to the map.
        if glide {
            c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "redis-py"]);
            let got = profiles(&mut c);
            for (got, want) in got.iter().zip(&want) {
                assert_eq!(untimed(parse(got)), untimed(parse(want)), "{mode}");
            }
        }
    }
}

/// A `CONFIG SET` of several settings that is refused on a later one puts
/// `search-resp3-reply` back.
#[test]
fn a_refused_config_set_leaves_the_setting_as_it_was() {
    let server = Server::start(&[]);
    let mut c = prepared(&server, "3");
    c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "GlidePy"]);
    let bad = c.cmd(&[
        "CONFIG",
        "SET",
        "search-resp3-reply",
        "redis",
        "search-default-dialect",
        "9",
    ]);
    assert!(bad.starts_with(b"-ERR CONFIG SET failed"), "{}", show(&bad));
    let got = c.cmd(&["CONFIG", "GET", "search-resp3-reply"]);
    assert!(show(&got).contains("$4|auto"), "{}", show(&got));
    assert!(is_array(&knn(&mut c)));
    // And when it is the setting that is refused, the one before stays unset.
    let bad = c.cmd(&[
        "CONFIG",
        "SET",
        "search-default-dialect",
        "2",
        "search-resp3-reply",
        "both",
    ]);
    assert!(bad.starts_with(b"-ERR CONFIG SET failed"), "{}", show(&bad));
    let got = c.cmd(&["CONFIG", "GET", "search-default-dialect"]);
    assert!(show(&got).contains("$1|1"), "{}", show(&got));
}

/// `redis` and `auto` for a client that is not GLIDE: what the build before
/// gave.
#[test]
fn redis_shape_is_what_it_was() {
    for mode in ["redis", "auto"] {
        let server = Server::start(&["--search-resp3-reply", mode]);
        let mut c = prepared(&server, "3");
        let replies = run_all(&mut c);
        assert_all(REDIS, &replies);
        let info = c.cmd(&["FT.INFO", "idx"]);
        assert!(is_map(&info), "{mode}: {}", show(&info));
        assert!(show(&info).contains("$10|index_name|$3|idx|"), "{mode}");
    }
}

/// Over RESP2 every mode answers alike.
#[test]
fn resp2_is_the_same_in_every_mode() {
    for mode in ["auto", "redis", "valkey"] {
        let server = Server::start(&["--search-resp3-reply", mode]);
        let mut c = prepared(&server, "2");
        // A GLIDE client too.
        c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "GlidePy"]);
        let replies = run_all(&mut c);
        for (name, reply) in &replies {
            if *name == "hybrid_key" || name.starts_with("hybrid") {
                // RESP2 has its own bytes for a time taken.
                continue;
            }
            same(RESP2, name, reply);
        }
        let info = c.cmd(&["FT.INFO", "idx"]);
        assert!(
            is_array(&info) && show(&info).contains("$10|index_name|"),
            "{mode}"
        );
    }
}

/// `auto` follows `CLIENT SETINFO LIB-NAME`, whenever it is given.
#[test]
fn auto_follows_the_library_name() {
    let server = Server::start(&[]);
    let mut c = prepared(&server, "3");
    assert!(is_map(&knn(&mut c)));

    // Named after HELLO 3.
    c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "GlidePy"]);
    assert_all(VALKEY, &run_all(&mut c));

    // Named something else.
    c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "redis-py"]);
    assert_all(REDIS, &run_all(&mut c));

    // Each name valkey-glide sends, and a tag after it; in any case.
    for name in [
        "GlidePy",
        "GlidePySync",
        "GlideJS",
        "GlideJava",
        "GlideGo",
        "GlideFFI",
        "Glide",
        "GlideJava(spring:1.2)",
        "glidego",
        "GLIDEJS",
    ] {
        c.ok(&["CLIENT", "SETINFO", "LIB-NAME", name]);
        assert!(is_array(&knn(&mut c)), "{name}");
    }
    for name in ["redis-py", "go-redis", "myglide", "lettuce", "glid"] {
        c.ok(&["CLIENT", "SETINFO", "LIB-NAME", name]);
        assert!(is_map(&knn(&mut c)), "{name}");
    }
    // Back to nothing.
    c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "GlideGo"]);
    assert!(is_array(&knn(&mut c)));
    c.ok(&["CLIENT", "SETINFO", "LIB-NAME", "plain"]);
    assert!(is_map(&knn(&mut c)));

    // Named before HELLO 3: the connection was RESP2, then it is not.
    let mut d = server.conn();
    d.ok(&["CLIENT", "SETINFO", "LIB-NAME", "GlideJS"]);
    assert!(is_array(&knn(&mut d)));
    d.cmd(&["HELLO", "3"]);
    assert_all(VALKEY, &run_all(&mut d));
    // The name is the connection's: RESET leaves it, and RESP2 is back until
    // HELLO 3.
    assert_eq!(d.cmd(&["RESET"]), b"+RESET\r\n");
    let r = knn(&mut d);
    assert!(is_array(&r));
    assert!(r.starts_with(b"*7"), "{}", show(&r));
    d.cmd(&["HELLO", "3"]);
    assert_all(VALKEY, &run_all(&mut d));
    // Another connection is not changed by it.
    let mut e = server.conn();
    e.cmd(&["HELLO", "3"]);
    assert!(is_map(&knn(&mut e)));
}

/// `CONFIG SET` takes effect on connections that are open.
#[test]
fn config_set_changes_live_connections() {
    let server = Server::start(&[]);
    let mut plain = prepared(&server, "3");
    let mut glide = server.conn();
    glide.cmd(&["HELLO", "3"]);
    glide.ok(&["CLIENT", "SETINFO", "LIB-NAME", "GlidePy"]);
    let mut admin = server.conn();
    let get = |a: &mut Conn| show(&a.cmd(&["CONFIG", "GET", "search-resp3-reply"]));
    assert!(
        get(&mut admin).contains("search-resp3-reply|$4|auto"),
        "{}",
        get(&mut admin)
    );
    assert!(is_map(&knn(&mut plain)));
    assert!(is_array(&knn(&mut glide)));

    admin.ok(&["CONFIG", "SET", "search-resp3-reply", "valkey"]);
    assert!(get(&mut admin).contains("$6|valkey"));
    assert!(is_array(&knn(&mut plain)));
    assert!(is_array(&knn(&mut glide)));
    assert_all(VALKEY, &run_all(&mut plain));

    admin.ok(&["CONFIG", "SET", "search-resp3-reply", "redis"]);
    assert!(get(&mut admin).contains("$5|redis"));
    assert!(is_map(&knn(&mut plain)));
    assert!(is_map(&knn(&mut glide)));
    assert_all(REDIS, &run_all(&mut glide));

    admin.ok(&["CONFIG", "SET", "search-resp3-reply", "AUTO"]);
    assert!(get(&mut admin).contains("$4|auto"));
    assert!(is_map(&knn(&mut plain)));
    assert!(is_array(&knn(&mut glide)));

    let bad = admin.cmd(&["CONFIG", "SET", "search-resp3-reply", "both"]);
    assert!(bad.starts_with(b"-ERR CONFIG SET failed"), "{}", show(&bad));
    assert!(show(&bad).contains("search-resp3-reply"));
    assert!(get(&mut admin).contains("$4|auto"));
}

/// The command line and the configuration file take the setting, and a bad
/// value does not start the server.
#[test]
fn the_setting_is_read_at_startup() {
    // The file.
    let server = Server::start_with(
        &[],
        "search-resp3-reply valkey
",
    );
    let mut c = prepared(&server, "3");
    assert!(is_array(&knn(&mut c)));
    // An unknown value is refused.
    let out = Command::new(env!("CARGO_BIN_EXE_kividb"))
        .args(["--search-resp3-reply", "both", "--port", "0"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
}

/// MULTI/EXEC: the reply to EXEC holds the connection's shape.
#[test]
fn transactions_use_the_connections_shape() {
    let server = Server::start(&["--search-resp3-reply", "valkey"]);
    let mut c = prepared(&server, "3");
    let blob = vector(0.0, 0.0);
    c.cmd(&["MULTI"]);
    for _ in 0..2 {
        let queued = c.raw(&[
            b"FT.SEARCH",
            b"idx",
            b"*=>[KNN 3 @v $B AS dist]",
            b"RETURN",
            b"1",
            b"dist",
            b"PARAMS",
            b"2",
            b"B",
            &blob,
            b"DIALECT",
            b"2",
        ]);
        assert_eq!(queued, b"+QUEUED\r\n");
    }
    let done = c.cmd(&["EXEC"]);
    let Node::Array(items) = parse(&done) else {
        panic!("{}", show(&done))
    };
    assert_eq!(items.len(), 2);
    let want = canonical(parse(&fixture(VALKEY, "knn_return")), 0);
    // `knn_return` returns t and dist; this asks for dist alone.
    for item in items {
        let Node::Array(parts) = item else { panic!() };
        assert_eq!(parts[0], Node::Int(3));
        assert_eq!(parts.len(), 7);
        let Node::Array(want) = &want else { panic!() };
        assert_eq!(parts[1], want[1]);
    }
}

/// A script reads `FT.SEARCH` as RESP2, whatever the setting says; one that
/// asks for RESP3 gets the map.
#[cfg(feature = "lua")]
#[test]
fn scripts_keep_their_own_protocol() {
    let server = Server::start(&["--search-resp3-reply", "valkey"]);
    let mut c = prepared(&server, "3");
    let blob = vector(0.0, 0.0);
    let script = "return redis.call('FT.SEARCH', 'idx', '*=>[KNN 3 @v $B AS dist]', \
                  'RETURN', '1', 'dist', 'PARAMS', '2', 'B', ARGV[1], 'DIALECT', '2')";
    let in_valkey = c.raw(&[b"EVAL", script.as_bytes(), b"0", &blob]);
    let mut other = server.conn();
    other.ok(&["CONFIG", "SET", "search-resp3-reply", "redis"]);
    let in_redis = c.raw(&[b"EVAL", script.as_bytes(), b"0", &blob]);
    assert_eq!(in_valkey, in_redis);
    assert!(is_array(&in_valkey));
    other.ok(&["CONFIG", "SET", "search-resp3-reply", "valkey"]);
    let script3 = "redis.setresp(3); local r = redis.call('FT.SEARCH', 'idx', \
                   '*=>[KNN 3 @v $B AS dist]', 'RETURN', '1', 'dist', 'PARAMS', '2', 'B', \
                   ARGV[1], 'DIALECT', '2'); return r.map['total_results']";
    let total = c.raw(&[b"EVAL", script3.as_bytes(), b"0", &blob]);
    assert_eq!(total, b":3\r\n", "{}", show(&total));
}

/// Every command that takes an index name answers a missing one as Redis 8.10
/// does (`SEARCH_INDEX_NOT_FOUND Index not found: <name>`), and a client that
/// takes the valkey-search shape (a GLIDE client under `auto`, or `valkey`) as
/// valkey-search does (`Index with name '<name>' not found in database <n>`),
/// on RESP2 and RESP3 alike.
#[test]
fn a_missing_index_is_worded_for_the_client_that_asks() {
    let q = "*=>[KNN 3 @v $B AS dist]";
    let blob = String::from_utf8(vec![0u8; 8]).unwrap();
    let commands: Vec<Vec<&str>> = vec![
        vec!["FT.INFO", "nx"],
        vec!["FT.SEARCH", "nx", "*"],
        vec![
            "FT.SEARCH",
            "nx",
            q,
            "PARAMS",
            "2",
            "B",
            &blob,
            "DIALECT",
            "2",
        ],
        vec!["FT.AGGREGATE", "nx", "*"],
        vec!["FT.PROFILE", "nx", "SEARCH", "QUERY", "*"],
        vec!["FT.EXPLAIN", "nx", "*"],
        vec!["FT.EXPLAINCLI", "nx", "*"],
        vec!["FT.DROPINDEX", "nx"],
        vec!["FT.ALTER", "nx", "SCHEMA", "ADD", "f", "TEXT"],
        vec!["FT.TAGVALS", "nx", "f"],
        vec!["FT.SYNDUMP", "nx"],
        vec!["FT.SYNUPDATE", "nx", "g", "a", "b"],
        vec!["FT.SPELLCHECK", "nx", "hello"],
        vec!["FT.ALIASADD", "al", "nx"],
        vec!["FT.CURSOR", "READ", "nx", "1"],
        vec!["FT.CURSOR", "DEL", "nx", "1"],
        vec!["FT.HYBRID", "nx", "SEARCH", "*"],
    ];
    let redis: &[u8] = b"-SEARCH_INDEX_NOT_FOUND Index not found: nx\r\n";
    let valkey: &[u8] = b"-Index with name 'nx' not found in database 0\r\n";
    // (settings, library name, what a missing index is answered)
    let cases: [(&[&str], Option<&str>, &[u8]); 5] = [
        (&[], None, redis),
        (&[], Some("redis-py"), redis),
        (&[], Some("GlidePy"), valkey),
        (&["--search-resp3-reply", "redis"], Some("GlidePy"), redis),
        (&["--search-resp3-reply", "valkey"], None, valkey),
    ];
    for (settings, lib, want) in cases {
        let server = Server::start(settings);
        for proto in ["2", "3"] {
            let mut c = server.conn();
            c.cmd(&["HELLO", proto]);
            if let Some(lib) = lib {
                c.ok(&["CLIENT", "SETINFO", "LIB-NAME", lib]);
            }
            for args in &commands {
                let reply = c.cmd(args);
                assert_eq!(
                    reply,
                    want,
                    "{settings:?} {lib:?} RESP{proto} {args:?}: {}",
                    show(&reply)
                );
            }
        }
    }
}

/// Another database than 0 is named in valkey-search's text, as
/// valkey-search names it.
#[test]
fn the_valkey_text_names_the_database() {
    let server = Server::start(&["--search-resp3-reply", "valkey"]);
    let mut c = server.conn();
    c.ok(&["SELECT", "3"]);
    let reply = c.cmd(&["FT.INFO", "nx"]);
    assert_eq!(
        reply,
        b"-Index with name 'nx' not found in database 3\r\n",
        "{}",
        show(&reply)
    );
}
