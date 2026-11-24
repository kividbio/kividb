#![allow(clippy::disallowed_methods)]
// tests/json_packed_arrays.rs
//
// A JSON array of numbers is held packed (`json_tree::Packed`), and an edit it
// cannot take turns it into generic nodes. Nothing a client sees may depend on
// which: this runs the same random documents and random edits against two
// servers, one packing arrays and one holding every array as generic nodes
// (`KIVIDB_TEST_JSON_UNPACKED`), and compares every reply byte for byte, in
// RESP2 and RESP3. Then the stored data: `DUMP` payloads and digests, a
// snapshot loaded by either kind of server, an AOF rewritten and replayed, and
// a full sync each way between them.
//
// `KIVIDB_JSON_DIFF_OTHER=<path to a kividb binary>` runs that binary as the
// second server instead (no packing switch): the same comparison against
// another build, such as the previous release.

mod common;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ── Servers and raw replies ──────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Packed,
    /// Every array as generic nodes, or the other binary.
    Other,
}

struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
    kind: Kind,
    extra: Vec<String>,
}

fn other_binary() -> Option<String> {
    std::env::var("KIVIDB_JSON_DIFF_OTHER")
        .ok()
        .filter(|p| !p.is_empty())
}

fn spawn(dir: &Path, port: u16, kind: Kind, extra: &[String]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let (binary, this_build) = match (kind, other_binary()) {
        (Kind::Other, Some(path)) => (path, false),
        _ => (env!("CARGO_BIN_EXE_kividb").to_string(), true),
    };
    let mut command = Command::new(binary);
    command
        .current_dir(dir)
        .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
        .args(["--io-model", "tokio", "--threads", "2"])
        // Earlier builds have `DEBUG` on, and no such option.
        .args(
            this_build
                .then_some(["--enable-debug-command", "yes"])
                .into_iter()
                .flatten(),
        )
        // Nor `--save`.
        .args(
            extra
                .chunks(2)
                .filter(|pair| this_build || pair[0] != "--save")
                .flatten(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    if kind == Kind::Other && other_binary().is_none() {
        command.env("KIVIDB_TEST_JSON_UNPACKED", "1");
    }
    command.spawn().expect("could not start kividb")
}

impl Server {
    fn start(kind: Kind, extra: &[&str]) -> Server {
        let dir = tempfile::tempdir().unwrap().keep();
        Server::start_in(dir, kind, extra)
    }

    fn start_in(dir: PathBuf, kind: Kind, extra: &[&str]) -> Server {
        let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let (child, port) = common::start_verified(|port| spawn(&dir, port, kind, &extra));
        let server = Server {
            child,
            port,
            dir,
            kind,
            extra,
        };
        server.wait_loaded();
        server
    }

    fn wait_loaded(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut c = self.conn(2);
        loop {
            let reply = c.cmd(&["PING"]);
            if reply == b"+PONG\r\n" {
                return;
            }
            assert!(Instant::now() < deadline, "server still loading");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn conn(&self, resp: u8) -> Conn {
        Conn::connect(self.port, resp)
    }

    /// Stopped with `SHUTDOWN` (an AOF is flushed), and started again in
    /// the same directory, with the same options.
    fn restart(&mut self) {
        let _ = self.conn(2).send(&["SHUTDOWN", "NOSAVE"]);
        let _ = self.child.wait();
        let (dir, kind, extra) = (self.dir.clone(), self.kind, self.extra.clone());
        let (child, port, _) =
            common::restart_verified(|port| spawn(&dir, port, kind, &extra), self.port);
        self.child = child;
        self.port = port;
        self.wait_loaded();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16, resp: u8) -> Conn {
        let deadline = Instant::now() + Duration::from_secs(15);
        let stream = loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                break s;
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            std::thread::sleep(Duration::from_millis(50));
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(60)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        let mut conn = Conn {
            reader: BufReader::new(stream),
            writer,
        };
        if resp == 3 {
            let hello = conn.cmd(&["HELLO", "3"]);
            assert!(hello.starts_with(b"%"), "HELLO 3: {:?}", lossy(&hello));
        }
        conn
    }

    fn send(&mut self, args: &[&str]) -> std::io::Result<()> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf)
    }

    /// One command, and its reply exactly as it came.
    fn cmd(&mut self, args: &[&str]) -> Vec<u8> {
        self.send(args).unwrap();
        let mut out = Vec::new();
        self.read(&mut out);
        out
    }

    fn cmd_bytes(&mut self, args: &[&[u8]]) -> Vec<u8> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        let mut out = Vec::new();
        self.read(&mut out);
        out
    }

    fn read(&mut self, out: &mut Vec<u8>) {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).expect("reply");
        assert!(line.len() >= 3, "short reply line {:?}", lossy(&line));
        out.extend_from_slice(&line);
        let head = std::str::from_utf8(&line[1..line.len() - 2]).unwrap_or("");
        let count = || head.parse::<i64>().unwrap_or(-1);
        match line[0] {
            b'+' | b'-' | b':' | b'_' | b',' | b'#' | b'(' => {}
            b'$' | b'=' | b'!' => {
                let n = count();
                if n >= 0 {
                    let mut data = vec![0u8; n as usize + 2];
                    self.reader.read_exact(&mut data).unwrap();
                    out.extend_from_slice(&data);
                }
            }
            b'*' | b'~' | b'>' => {
                for _ in 0..count().max(0) {
                    self.read(out);
                }
            }
            b'%' | b'|' => {
                for _ in 0..2 * count().max(0) {
                    self.read(out);
                }
            }
            other => panic!("unexpected reply type {:?}", other as char),
        }
    }

    /// A bulk string reply's contents.
    fn text(&mut self, args: &[&str]) -> String {
        let reply = self.cmd(args);
        let reply = lossy(&reply);
        match reply.split_once("\r\n") {
            Some((head, rest)) if head.starts_with('$') || head.starts_with('=') => {
                rest.trim_end_matches("\r\n").to_string()
            }
            _ => reply,
        }
    }

    fn info(&mut self, section: &str, field: &str) -> Option<String> {
        let text = self.text(&["INFO", section]);
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .map(|v| v.trim().to_string())
    }
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

// ── Random documents ─────────────────────────────────────────────────────────

const KEYS: [&str; 4] = ["doc:0", "doc:1", "doc:2", "doc:3"];
const FIELDS: [&str; 7] = ["embedding", "ints", "f64", "mixed", "small", "o", "nested"];

/// A number, in one of the many ways JSON can write one.
fn number(rng: &mut StdRng) -> String {
    const FORMS: [&str; 30] = [
        "0",
        "-0",
        "0.0",
        "-0.0",
        "1",
        "1.0",
        "1.50",
        "1e5",
        "1E+2",
        "2.5E-3",
        "6.4e-05",
        "0.000064",
        "1e21",
        "1e-7",
        "0.1",
        "123456789.123",
        "9007199254740993",
        "9223372036854775807",
        "-9223372036854775808",
        "9223372036854775808",
        "18446744073709551615",
        "18446744073709551616",
        "123456789012345678901234567890",
        "3.4028234663852886e38",
        "1.401298464324817e-45",
        "5e-324",
        "1.7976931348623157e308",
        "-1e300",
        "100.000",
        "7",
    ];
    match rng.gen_range(0..10) {
        0..=3 => float32(rng),
        4 => format!("{:?}", rng.gen_range(-1.0e6..1.0e6f64)),
        5 | 6 => rng.gen_range(-1000i64..1000).to_string(),
        _ => FORMS[rng.gen_range(0..FORMS.len())].to_string(),
    }
}

/// A float32 written as a double, as Python's `json.dumps` writes one.
fn float32(rng: &mut StdRng) -> String {
    let f: f32 = rng.gen_range(-1.0f32..1.0);
    let text = format!("{:?}", f64::from(f));
    if rng.gen_bool(0.1) {
        text.replace('e', "E")
    } else {
        text
    }
}

fn array(rng: &mut StdRng, element: impl Fn(&mut StdRng) -> String) -> String {
    let len = match rng.gen_range(0..6) {
        0 => rng.gen_range(0..8),
        _ => rng.gen_range(8..40),
    };
    let items: Vec<String> = (0..len).map(|_| element(rng)).collect();
    format!("[{}]", items.join(","))
}

fn int(rng: &mut StdRng) -> String {
    match rng.gen_range(0..20) {
        0 => "9223372036854775807".into(),
        1 => "-9223372036854775808".into(),
        2 => "-0".into(),
        _ => rng.gen_range(-100_000i64..100_000).to_string(),
    }
}

fn f64_text(rng: &mut StdRng) -> String {
    match rng.gen_range(0..6) {
        0 => float32(rng),
        1 => "1.0".into(),
        2 => "-0.0".into(),
        _ => format!(
            "{:?}",
            rng.gen_range(-1.0..1.0f64) * 10f64.powi(rng.gen_range(-30..30))
        ),
    }
}

/// A value of any kind, for writes into arrays and members.
fn value(rng: &mut StdRng) -> String {
    match rng.gen_range(0..12) {
        0..=4 => number(rng),
        5 => "\"text\"".into(),
        6 => "null".into(),
        7 => "true".into(),
        8 => "[1,2]".into(),
        9 => "{\"z\":1.5}".into(),
        10 => array(rng, float32),
        _ => array(rng, int),
    }
}

/// An array for a field, each field its own kind.
fn field_array(rng: &mut StdRng, field: &str) -> String {
    match field {
        "embedding" => {
            if rng.gen_bool(0.5) {
                let items: Vec<String> = (0..8).map(|_| float32(rng)).collect();
                format!("[{}]", items.join(","))
            } else {
                array(rng, float32)
            }
        }
        "ints" => array(rng, int),
        "f64" => array(rng, f64_text),
        "mixed" => array(rng, number),
        "small" => {
            let items: Vec<String> = (0..rng.gen_range(0..8)).map(|_| number(rng)).collect();
            format!("[{}]", items.join(","))
        }
        "o" => format!("{{\"a\":{},\"b\":true,\"c\":\"x\"}}", array(rng, float32)),
        _ => format!(
            "[{},{},\"s\",{}]",
            array(rng, int),
            array(rng, float32),
            number(rng)
        ),
    }
}

fn document(rng: &mut StdRng) -> String {
    let mut members = Vec::new();
    for field in FIELDS {
        if rng.gen_bool(0.85) {
            members.push(format!("\"{field}\":{}", field_array(rng, field)));
        }
    }
    members.push(format!("\"n\":{}", number(rng)));
    members.push(format!("\"s\":\"tag{}\"", rng.gen_range(0..3)));
    format!("{{{}}}", members.join(","))
}

/// A path into a field: the field, an element, several, a filter.
fn path(rng: &mut StdRng) -> String {
    let field = FIELDS[rng.gen_range(0..FIELDS.len())];
    let field = if field == "o" { "o.a" } else { field };
    let i = rng.gen_range(-3i64..20);
    match rng.gen_range(0..16) {
        0 => format!("$.{field}"),
        1 | 2 => format!("$.{field}[{i}]"),
        3 => format!("$.{field}[*]"),
        4 => format!(
            "$.{field}[{}:{}]",
            rng.gen_range(0..5),
            rng.gen_range(3..12)
        ),
        5 => format!("$.{field}[?(@ > 0.25)]"),
        6 => format!("$.{field}[?(@ == 1)]"),
        7 => "$..*".into(),
        8 => format!("$..{field}"),
        9 => format!(".{field}"),
        10 => format!("{field}[{}]", rng.gen_range(0..10)),
        11 => format!("$.{field}[0,2,-1]"),
        12 => "$.nested[*][*]".into(),
        13 => "$".into(),
        14 => format!("$.{field}[::-2]"),
        _ => format!("$.{field}[{i}]"),
    }
}

/// One random command, as its arguments.
fn command(rng: &mut StdRng) -> Vec<String> {
    let key = KEYS[rng.gen_range(0..KEYS.len())].to_string();
    let s = |x: &str| x.to_string();
    let field = FIELDS[rng.gen_range(0..FIELDS.len())];
    match rng.gen_range(0..40) {
        0 => vec![s("JSON.SET"), key, s("$"), document(rng)],
        1 | 2 => vec![
            s("JSON.SET"),
            key,
            format!("$.{field}"),
            field_array(rng, field),
        ],
        3..=5 => vec![s("JSON.SET"), key, path(rng), value(rng)],
        6 => vec![s("JSON.SET"), key, path(rng), number(rng), s("XX")],
        7 => {
            let fp = ["FP16", "BF16", "FP32", "FP64"][rng.gen_range(0..4)];
            vec![
                s("JSON.SET"),
                key,
                format!("$.{field}"),
                field_array(rng, field),
                s("FPHA"),
                s(fp),
            ]
        }
        8 | 9 => {
            let mut c = vec![s("JSON.ARRAPPEND"), key, path(rng)];
            for _ in 0..rng.gen_range(1..4) {
                c.push(if rng.gen_bool(0.7) {
                    number(rng)
                } else {
                    value(rng)
                });
            }
            c
        }
        10 | 11 => {
            let mut c = vec![
                s("JSON.ARRINSERT"),
                key,
                path(rng),
                rng.gen_range(-12i64..12).to_string(),
            ];
            for _ in 0..rng.gen_range(1..3) {
                c.push(if rng.gen_bool(0.7) {
                    number(rng)
                } else {
                    value(rng)
                });
            }
            c
        }
        12 => vec![
            s("JSON.ARRPOP"),
            key,
            path(rng),
            rng.gen_range(-10i64..10).to_string(),
        ],
        13 => vec![s("JSON.ARRPOP"), key, path(rng)],
        14 => vec![
            s("JSON.ARRTRIM"),
            key,
            path(rng),
            rng.gen_range(-5i64..10).to_string(),
            rng.gen_range(-5i64..30).to_string(),
        ],
        15 | 16 => vec![s("JSON.NUMINCRBY"), key, path(rng), number(rng)],
        17 => vec![s("JSON.NUMMULTBY"), key, path(rng), number(rng)],
        18 => vec![s("JSON.DEL"), key, path(rng)],
        19 => vec![
            s("JSON.MERGE"),
            key,
            s("$"),
            format!(
                "{{\"{field}\":{}}}",
                if rng.gen_bool(0.2) {
                    s("null")
                } else {
                    field_array(rng, field)
                }
            ),
        ],
        20 => vec![s("JSON.MERGE"), key, path(rng), value(rng)],
        21 => vec![s("JSON.CLEAR"), key, path(rng)],
        22 => vec![s("JSON.TOGGLE"), key, path(rng)],
        23 => vec![s("JSON.STRAPPEND"), key, path(rng), s("\"!\"")],
        24 => vec![s("JSON.TYPE"), key, path(rng)],
        25 => vec![s("JSON.ARRLEN"), key, path(rng)],
        26 => {
            let mut c = vec![s("JSON.ARRINDEX"), key, path(rng), number(rng)];
            if rng.gen_bool(0.5) {
                c.push(rng.gen_range(-5i64..10).to_string());
                c.push(rng.gen_range(-5i64..30).to_string());
            }
            c
        }
        27 => vec![s("JSON.RESP"), key, path(rng)],
        28 => vec![s("JSON.MGET"), key, s(KEYS[0]), s(KEYS[1]), path(rng)],
        29 => vec![s("JSON.OBJLEN"), key, path(rng)],
        30 => vec![s("JSON.OBJKEYS"), key, path(rng)],
        31 => vec![s("JSON.STRLEN"), key, path(rng)],
        32 => {
            let mut c = vec![s("JSON.GET"), key];
            if rng.gen_bool(0.3) {
                c.extend([
                    s("INDENT"),
                    s("  "),
                    s("NEWLINE"),
                    s("\n"),
                    s("SPACE"),
                    s(" "),
                ]);
            }
            for _ in 0..rng.gen_range(0..3) {
                c.push(path(rng));
            }
            c
        }
        33 => vec![s("JSON.GET"), key, path(rng), path(rng)],
        34 => vec![s("JSON.GET"), key, format!("$.{field}.length()")],
        35 => vec![s("JSON.GET"), key, format!("$.{field}[?(@ > $.n)]")],
        36 => vec![
            s("JSON.MSET"),
            key,
            path(rng),
            number(rng),
            s(KEYS[2]),
            s("$"),
            document(rng),
        ],
        37 => vec![s("JSON.SET"), key, format!("$.{field}[*]"), number(rng)],
        38 => vec![
            s("JSON.NUMINCRBY"),
            key,
            format!("$.{field}[*]"),
            number(rng),
        ],
        _ => vec![s("JSON.GET"), key],
    }
}

// ── The comparison ───────────────────────────────────────────────────────────

struct Pair {
    packed: Server,
    other: Server,
}

impl Pair {
    fn start(extra: &[&str]) -> Pair {
        Pair {
            packed: Server::start(Kind::Packed, extra),
            other: Server::start(Kind::Other, extra),
        }
    }
}

/// How many replies were errors, and how many not.
static ERRORS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ANSWERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Every reply of `commands` from both servers, the same.
fn run_same(a: &mut Conn, b: &mut Conn, commands: &[Vec<String>], what: &str) {
    use std::sync::atomic::Ordering::Relaxed;
    for (step, args) in commands.iter().enumerate() {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let (x, y) = (a.cmd(&args), b.cmd(&args));
        assert!(
            x == y,
            "{what}, step {step}: {args:?}\n packed: {}\n other:  {}",
            lossy(&x),
            lossy(&y)
        );
        if x.starts_with(b"-") {
            ERRORS.fetch_add(1, Relaxed);
        } else {
            ANSWERS.fetch_add(1, Relaxed);
        }
    }
}

fn index_commands() -> Vec<Vec<String>> {
    let s = |x: &str| x.to_string();
    vec![vec![
        s("FT.CREATE"),
        s("idx"),
        s("ON"),
        s("JSON"),
        s("PREFIX"),
        s("1"),
        s("doc:"),
        s("SCHEMA"),
        s("$.embedding"),
        s("AS"),
        s("v"),
        s("VECTOR"),
        s("FLAT"),
        s("6"),
        s("TYPE"),
        s("FLOAT32"),
        s("DIM"),
        s("8"),
        s("DISTANCE_METRIC"),
        s("L2"),
        s("$.n"),
        s("AS"),
        s("n"),
        s("NUMERIC"),
        s("$.ints"),
        s("AS"),
        s("i"),
        s("NUMERIC"),
        s("$.s"),
        s("AS"),
        s("s"),
        s("TAG"),
    ]]
}

fn index_queries() -> Vec<Vec<Vec<u8>>> {
    let blob: Vec<u8> = (0..8)
        .flat_map(|i| (i as f32 / 8.0).to_le_bytes())
        .collect();
    let b = |x: &str| x.as_bytes().to_vec();
    vec![
        vec![
            b("FT.SEARCH"),
            b("idx"),
            b("*=>[KNN 3 @v $q AS score]"),
            b("PARAMS"),
            b("2"),
            b("q"),
            blob,
            b("SORTBY"),
            b("score"),
            b("RETURN"),
            b("2"),
            b("score"),
            b("$.embedding"),
            b("DIALECT"),
            b("2"),
        ],
        vec![
            b("FT.SEARCH"),
            b("idx"),
            b("@n:[-inf +inf]"),
            b("SORTBY"),
            b("n"),
            b("RETURN"),
            b("2"),
            b("$.n"),
            b("$.ints"),
        ],
        vec![
            b("FT.SEARCH"),
            b("idx"),
            b("@i:[0 +inf]"),
            b("NOCONTENT"),
            b("SORTBY"),
            b("i"),
        ],
        vec![
            b("FT.SEARCH"),
            b("idx"),
            b("@s:{tag1}"),
            b("SORTBY"),
            b("n"),
            b("NOCONTENT"),
        ],
    ]
}

/// A `DUMP` payload without its trailer (the format version and the
/// checksum): the value as it is serialized.
fn dump_body(reply: &[u8]) -> &[u8] {
    match reply.iter().position(|&c| c == b'\n') {
        Some(at) if reply.starts_with(b"$") && reply.len() >= at + 1 + 10 + 2 => {
            &reply[at + 1..reply.len() - 2 - 10]
        }
        _ => reply,
    }
}

/// The JSON in a bulk reply, parsed.
fn parsed(reply: &[u8]) -> Option<serde_json::Value> {
    let at = reply.iter().position(|&c| c == b'\n')?;
    serde_json::from_slice(reply.get(at + 1..reply.len().checked_sub(2)?)?).ok()
}

/// What each server holds, compared: every document's text, its `DUMP`
/// payload (as serialized: an older build stamps another format version) and
/// digest, and the dataset's digest. Against another build, whose `JSON.GET`
/// may print a number otherwise (`1e+21`), a document's text is compared as
/// the JSON it parses to.
fn same_data(a: &Server, b: &Server, what: &str) {
    let (mut x, mut y) = (a.conn(2), b.conn(2));
    for key in KEYS {
        for args in [
            vec!["JSON.GET", key],
            vec!["DUMP", key],
            vec!["DEBUG", "DIGEST-VALUE", key],
            vec!["TYPE", key],
        ] {
            let (p, q) = (x.cmd(&args), y.cmd(&args));
            let same = match args[0] {
                "DEBUG" if p.starts_with(b"-") || q.starts_with(b"-") => true,
                "DUMP" => dump_body(&p) == dump_body(&q),
                "JSON.GET" if other_binary().is_some() => p == q || parsed(&p) == parsed(&q),
                _ => p == q,
            };
            assert!(
                same,
                "{what}: {args:?}\n first:  {}\n second: {}",
                lossy(&p),
                lossy(&q)
            );
        }
    }
    let (p, q) = (x.cmd(&["DEBUG", "DIGEST"]), y.cmd(&["DEBUG", "DIGEST"]));
    if !p.starts_with(b"-") && !q.starts_with(b"-") {
        assert_eq!(lossy(&p), lossy(&q), "{what}: DEBUG DIGEST");
    }
}

fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn seeds() -> Vec<u64> {
    match std::env::var("JSON_DIFF_SEEDS") {
        Ok(text) => text
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect(),
        Err(_) => vec![1, 2, 3, 1001],
    }
}

fn steps() -> usize {
    std::env::var("JSON_DIFF_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}

#[test]
fn json_replies_do_not_depend_on_how_arrays_are_held() {
    let pair = Pair::start(&["--save", ""]);
    let (mut p2, mut o2) = (pair.packed.conn(2), pair.other.conn(2));
    let (mut p3, mut o3) = (pair.packed.conn(3), pair.other.conn(3));
    // The two really hold arrays differently: a packed one takes less.
    let floats = format!("[{}]", vec!["0.5"; 64].join(","));
    for c in [&mut p2, &mut o2] {
        c.cmd(&["JSON.SET", "probe", "$", &floats]);
    }
    let size = |c: &mut Conn| -> i64 {
        let reply = lossy(&c.cmd(&["JSON.DEBUG", "MEMORY", "probe"]));
        reply.trim_start_matches(':').trim().parse().unwrap()
    };
    let (packed, other) = (size(&mut p2), size(&mut o2));
    assert!(packed * 4 < other, "packed {packed} B, other {other} B");
    run_same(&mut p2, &mut o2, &index_commands(), "index");
    for seed in seeds() {
        let mut rng = StdRng::seed_from_u64(seed);
        let setup: Vec<Vec<String>> = KEYS
            .iter()
            .map(|key| {
                vec![
                    "JSON.SET".into(),
                    key.to_string(),
                    "$".into(),
                    document(&mut rng),
                ]
            })
            .collect();
        run_same(&mut p2, &mut o2, &setup, &format!("seed {seed} documents"));
        for step in 0..steps() {
            let args = command(&mut rng);
            let what = format!("seed {seed}, edit {step}");
            if rng.gen_bool(0.5) {
                run_same(&mut p2, &mut o2, &[args], &format!("{what}, RESP2"));
            } else {
                run_same(&mut p3, &mut o3, &[args], &format!("{what}, RESP3"));
            }
            if step % 50 == 49 {
                for query in index_queries() {
                    let q: Vec<&[u8]> = query.iter().map(Vec::as_slice).collect();
                    let (x, y) = (p2.cmd_bytes(&q), o2.cmd_bytes(&q));
                    assert!(
                        x == y,
                        "{what}: {}\n packed: {}\n other:  {}",
                        lossy(query[2].as_slice()),
                        lossy(&x),
                        lossy(&y)
                    );
                }
            }
        }
        same_data(&pair.packed, &pair.other, &format!("seed {seed}"));
    }
    use std::sync::atomic::Ordering::Relaxed;
    let (errors, answers) = (ERRORS.load(Relaxed), ANSWERS.load(Relaxed));
    eprintln!("{answers} replies the same, and {errors} errors the same");
    // Most edits do something: a generator that only made errors would
    // compare nothing.
    assert!(answers > errors * 2, "{answers} answers, {errors} errors");
}

/// The documents of `seed`, a few hundred edits in, written to `server`.
fn fill(server: &Server, seed: u64) {
    let mut c = server.conn(2);
    let mut rng = StdRng::seed_from_u64(seed);
    for key in KEYS {
        c.cmd(&["JSON.SET", key, "$", &document(&mut rng)]);
    }
    for _ in 0..300 {
        let args = command(&mut rng);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        c.cmd(&args);
    }
}

#[test]
fn a_snapshot_reads_the_same_in_either_kind_of_server() {
    let pair = Pair::start(&["--save", ""]);
    fill(&pair.packed, 7);
    fill(&pair.other, 7);
    if other_binary().is_none() {
        same_data(&pair.packed, &pair.other, "before saving");
    }
    for server in [&pair.packed, &pair.other] {
        let reply = server.conn(2).cmd(&["SAVE"]);
        assert_eq!(lossy(&reply), "+OK\r\n");
    }
    // Each one's snapshot loaded by a server of the other kind.
    let crossed = |from: &Server, kind: Kind| {
        let dir = tempfile::tempdir().unwrap().keep();
        for name in std::fs::read_dir(&from.dir).unwrap() {
            let name = name.unwrap().file_name();
            let name = name.to_string_lossy().into_owned();
            if name.starts_with("dump.") {
                std::fs::copy(from.dir.join(&name), dir.join(&name)).unwrap();
            }
        }
        Server::start_in(dir, kind, &["--save", ""])
    };
    let packed_from_other = crossed(&pair.other, Kind::Packed);
    let other_from_packed = crossed(&pair.packed, Kind::Other);
    same_data(
        &packed_from_other,
        &pair.other,
        "packed server, other's snapshot",
    );
    same_data(
        &other_from_packed,
        &pair.packed,
        "other server, packed one's snapshot",
    );
    if other_binary().is_none() {
        same_data(&packed_from_other, &other_from_packed, "the two loaded");
    }
}

#[test]
fn an_aof_rewritten_and_replayed_reads_the_same() {
    let mut pair = Pair::start(&["--save", "", "--appendonly", "yes"]);
    fill(&pair.packed, 11);
    fill(&pair.other, 11);
    for server in [&pair.packed, &pair.other] {
        let mut c = server.conn(2);
        c.cmd(&["BGREWRITEAOF"]);
        wait_for("the AOF rewrite", || {
            c.info("persistence", "aof_rewrite_in_progress").as_deref() == Some("0")
                && c.info("persistence", "aof_rewrite_scheduled").as_deref() != Some("1")
        });
        // A few more writes, after the rewrite, in the AOF's tail.
        c.cmd(&["JSON.ARRAPPEND", "doc:0", "$.embedding", "0.5", "1.25"]);
        c.cmd(&["JSON.NUMINCRBY", "doc:1", "$.ints[*]", "3"]);
        c.cmd(&["JSON.SET", "doc:2", "$.f64[0]", "\"unpacked\""]);
    }
    if other_binary().is_none() {
        same_data(&pair.packed, &pair.other, "before the restart");
    }
    // Each the same as before its restart.
    for server in [&mut pair.packed, &mut pair.other] {
        let held = |server: &Server| -> Vec<Vec<u8>> {
            let mut c = server.conn(2);
            let mut out: Vec<Vec<u8>> = KEYS.iter().map(|k| c.cmd(&["JSON.GET", k])).collect();
            out.push(c.cmd(&["DEBUG", "DIGEST"]));
            out
        };
        let before = held(server);
        server.restart();
        let after = held(server);
        assert!(
            before == after,
            "{:?} server: the AOF replayed to something else",
            server.kind
        );
    }
    if other_binary().is_none() {
        same_data(&pair.packed, &pair.other, "after replaying the AOF");
    }
}

#[test]
fn a_full_sync_copies_the_same_documents_either_way() {
    let pair = Pair::start(&["--save", ""]);
    fill(&pair.packed, 23);
    fill(&pair.other, 29);
    for (master, kind) in [(&pair.packed, Kind::Other), (&pair.other, Kind::Packed)] {
        let replica = Server::start(kind, &["--save", ""]);
        let mut c = replica.conn(2);
        c.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]);
        wait_for("the replica's sync", || {
            c.info("replication", "master_link_status").as_deref() == Some("up")
                && c.info("replication", "master_sync_in_progress").as_deref() != Some("1")
        });
        // A write after the sync, streamed as the command.
        master
            .conn(2)
            .cmd(&["JSON.ARRINSERT", "doc:3", "$.embedding", "0", "0.75"]);
        let mut m = master.conn(2);
        wait_for("the replica to catch up", || {
            let at = m.info("replication", "master_repl_offset");
            at.is_some() && c.info("replication", "slave_repl_offset") == at
        });
        same_data(
            master,
            &replica,
            &format!("replica of a {:?} master", master.kind),
        );
    }
}
