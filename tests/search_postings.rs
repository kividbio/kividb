// tests/search_postings.rs
//
// TAG and NUMERIC postings (`vector::postings`): a query answered from them
// must answer exactly what trying the filter on every document answers.
//
// Random documents, random filters, and random writes between rounds of
// them (HSET, HDEL, HINCRBY, DEL, RENAME, COPY, MOVE, a key that expires, a
// field that expires, a value of another type over a document); every
// query is checked against a model of the documents kept here, and against
// the server with its postings turned off (`FT.DEBUG POSTINGS … OFF`).
// Filtered KNN is checked the same way: exact where it measures, and with
// the recall its graph search has where it searches.
//
// One server, shared; each test has its own index and prefix.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Map(Vec<(V, V)>),
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
            V::Str(s) => s.parse().unwrap_or_else(|_| panic!("not an integer: {s}")),
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
    fn get(&self, name: &str) -> &V {
        let found = match self {
            V::Map(pairs) => pairs
                .iter()
                .find(|(k, _)| matches!(k, V::Str(k) if k == name))
                .map(|(_, v)| v),
            V::Arr(items) => items
                .chunks(2)
                .find(|pair| matches!(&pair[0], V::Str(k) if k == name))
                .map(|pair| &pair[1]),
            _ => None,
        };
        found.unwrap_or_else(|| panic!("no {name} in {self:?}"))
    }
    /// The keys of an FT.SEARCH … NOCONTENT reply, sorted.
    fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.arr()[1..].iter().map(|v| v.s().to_string()).collect();
        keys.sort();
        keys
    }
    /// The keys of an FT.SEARCH … NOCONTENT reply, in order.
    fn ordered_keys(&self) -> Vec<String> {
        self.arr()[1..].iter().map(|v| v.s().to_string()).collect()
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

    fn send(&mut self, args: &[&[u8]]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    fn raw(&mut self, args: &[&[u8]]) -> V {
        self.send(args);
        self.read()
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
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
            "," => V::Str(rest.to_string()),
            "_" => V::Nil,
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
            "%" | "|" => {
                let n: usize = rest.parse().unwrap();
                let pairs = V::Map((0..n).map(|_| (self.read(), self.read())).collect());
                if kind == "|" { self.read() } else { pairs }
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }
}

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

/// A port outside the ephemeral range (see tests/generics.rs).
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(41);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn port() -> u16 {
    static PORT: OnceLock<u16> = OnceLock::new();
    *PORT.get_or_init(|| {
        let port = free_port();
        thread::spawn(move || {
            kividb::run_for_test(kividb::config::Config {
                port,
                bind: "127.0.0.1".to_string(),
                threads: 2,
                loglevel: "error".to_string(),
                io_model: kividb::config::IoModel::Tokio,
                configfile: "./kividb.conf".to_string(),
                ..Default::default()
            });
        });
        for _ in 0..200 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return port;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {port} did not start");
    })
}

fn conn() -> Conn {
    Conn::connect(port())
}

// ── Random numbers ────────────────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len())]
    }
    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
    fn unit(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32
    }
}

// ── The model ─────────────────────────────────────────────────────────────────
//
// What every document holds, by key, and the filter evaluated on it the way
// the query language defines it.

type Doc = BTreeMap<String, String>;

const PREFIX_FIELDS: [&str; 5] = ["t", "c", "s", "n", "m"];

fn tags(doc: &Doc, field: &str, separator: char, case_sensitive: bool) -> Vec<String> {
    doc.get(field)
        .map(|v| {
            v.split(separator)
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(|t| {
                    if case_sensitive {
                        t.to_string()
                    } else {
                        t.to_lowercase()
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn number(doc: &Doc, field: &str) -> Option<f64> {
    doc.get(field)?.trim().parse::<f64>().ok()
}

#[derive(Debug, Clone)]
enum F {
    All,
    TagC(Vec<String>, Vec<String>),
    TagS(Vec<String>),
    Range(&'static str, f64, bool, f64, bool),
    Ne(&'static str, f64),
    Word(String),
    And(Box<F>, Box<F>),
    Or(Box<F>, Box<F>),
    Not(Box<F>),
    Maybe(Box<F>),
}

fn bound(v: f64, exclusive: bool) -> String {
    let n = if v == f64::INFINITY {
        "+inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        format!("{v}")
    };
    if exclusive { format!("({n}") } else { n }
}

impl F {
    fn query(&self) -> String {
        match self {
            F::All => "*".into(),
            F::TagC(values, prefixes) => {
                let mut parts: Vec<String> = values.clone();
                parts.extend(prefixes.iter().map(|p| format!("{p}*")));
                format!("@c:{{{}}}", parts.join(" | "))
            }
            F::TagS(values) => format!("@s:{{{}}}", values.join(" | ")),
            F::Range(field, lo, lx, hi, hx) => {
                format!("@{field}:[{} {}]", bound(*lo, *lx), bound(*hi, *hx))
            }
            F::Ne(field, v) => format!("@{field}!={v}"),
            F::Word(w) => format!("@t:{w}"),
            F::And(a, b) => format!("({} {})", a.query(), b.query()),
            F::Or(a, b) => format!("({} | {})", a.query(), b.query()),
            F::Not(a) => format!("-{}", a.query()),
            F::Maybe(a) => format!("~{}", a.query()),
        }
    }

    fn eval(&self, doc: &Doc) -> bool {
        match self {
            F::All | F::Maybe(_) => true,
            F::TagC(values, prefixes) => tags(doc, "c", ',', false)
                .iter()
                .any(|t| values.contains(t) || prefixes.iter().any(|p| t.starts_with(p.as_str()))),
            F::TagS(values) => tags(doc, "s", ';', true).iter().any(|t| values.contains(t)),
            F::Range(field, lo, lx, hi, hx) => number(doc, field).is_some_and(|n| {
                (if *lx { n > *lo } else { n >= *lo }) && (if *hx { n < *hi } else { n <= *hi })
            }),
            F::Ne(field, v) => number(doc, field).is_some_and(|n| n != *v),
            F::Word(w) => doc
                .get("t")
                .is_some_and(|t| t.split(' ').any(|word| word == w)),
            F::And(a, b) => a.eval(doc) && b.eval(doc),
            F::Or(a, b) => a.eval(doc) || b.eval(doc),
            F::Not(a) => !a.eval(doc),
        }
    }
}

const WORDS: [&str; 4] = ["alpha", "beta", "gamma", "delta"];
const C_VALUES: [&str; 8] = [
    "red",
    "Blue",
    "green",
    "red, blue",
    " Yellow ,red",
    "",
    "grey",
    "teal,green",
];
const S_VALUES: [&str; 5] = ["A;b", "a", "B;C", "c", "A"];
const N_VALUES: [&str; 12] = [
    "1", "2.5", "-3", "0", "-0", "nan", "x", "inf", "10", "7", " 4 ", "-inf",
];

fn random_filter(rng: &mut Rng, depth: usize) -> F {
    if depth == 0 || rng.chance(35) {
        let numbers = [-3.0, 0.0, 1.0, 2.5, 4.0, 7.0, 10.0, 15.0];
        let num = |rng: &mut Rng| -> f64 {
            match rng.below(10) {
                0 => f64::NEG_INFINITY,
                1 => f64::INFINITY,
                _ => numbers[rng.below(numbers.len())],
            }
        };
        return match rng.below(9) {
            0 => F::TagC(
                vec![rng.pick(&["red", "blue", "green", "yellow"]).to_string()],
                vec![],
            ),
            1 => F::TagC(
                vec!["grey".into(), rng.pick(&["red", "teal"]).to_string()],
                if rng.chance(50) {
                    vec!["gr".into()]
                } else {
                    vec![]
                },
            ),
            2 => F::TagS(vec![rng.pick(&["A", "a", "b", "B", "C"]).to_string()]),
            3 | 4 => {
                let field = if rng.chance(50) { "n" } else { "m" };
                let (a, b) = (num(rng), num(rng));
                F::Range(field, a.min(b), rng.chance(30), a.max(b), rng.chance(30))
            }
            5 => F::Ne(
                if rng.chance(50) { "n" } else { "m" },
                numbers[rng.below(numbers.len())],
            ),
            6 => F::Word(rng.pick(&WORDS).to_string()),
            7 => F::Range("m", f64::NEG_INFINITY, false, f64::INFINITY, false),
            _ => F::TagC(vec![], vec![rng.pick(&["r", "b", "y", "g"]).to_string()]),
        };
    }
    let a = Box::new(random_filter(rng, depth - 1));
    match rng.below(6) {
        0 | 1 => F::And(a, Box::new(random_filter(rng, depth - 1))),
        2 | 3 => F::Or(a, Box::new(random_filter(rng, depth - 1))),
        4 => F::Not(a),
        _ => F::And(
            Box::new(F::Maybe(a)),
            Box::new(random_filter(rng, depth - 1)),
        ),
    }
}

fn random_doc(rng: &mut Rng, with_vector: bool) -> Doc {
    let mut doc = Doc::new();
    for field in PREFIX_FIELDS {
        if !rng.chance(85) {
            continue;
        }
        let value = match field {
            "t" => (0..1 + rng.below(2))
                .map(|_| rng.pick(&WORDS))
                .collect::<Vec<_>>()
                .join(" "),
            "c" => rng.pick(&C_VALUES).to_string(),
            "s" => rng.pick(&S_VALUES).to_string(),
            "n" => rng.pick(&N_VALUES).to_string(),
            _ => rng.below(16).to_string(),
        };
        doc.insert(field.to_string(), value);
    }
    if with_vector {
        let v: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
        doc.insert("v".into(), encode(&v));
    }
    doc
}

const DIM: usize = 8;

/// A vector as the model keeps it: its floats in hex, one string.
fn encode(v: &[f32]) -> String {
    v.iter().map(|f| format!("{:08x}", f.to_bits())).collect()
}

fn decode(s: &str) -> Vec<f32> {
    (0..s.len() / 8)
        .map(|i| f32::from_bits(u32::from_str_radix(&s[i * 8..i * 8 + 8], 16).unwrap()))
        .collect()
}

fn blob(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn hset(c: &mut Conn, key: &str, doc: &Doc) {
    let mut owned: Vec<Vec<u8>> = vec![b"HSET".to_vec(), key.as_bytes().to_vec()];
    for (field, value) in doc {
        owned.push(field.as_bytes().to_vec());
        if field == "v" {
            owned.push(blob(&decode(value)));
        } else {
            owned.push(value.as_bytes().to_vec());
        }
    }
    if owned.len() == 2 {
        // Nothing to write: a document with one field nobody queries.
        owned.extend([b"other".to_vec(), b"1".to_vec()]);
    }
    let args: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
    let reply = c.raw(&args);
    assert!(matches!(reply, V::Int(_)), "{reply:?}");
}

fn create(c: &mut Conn, index: &str, prefix: &str, vector: Option<&str>) {
    let mut args = vec![
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "t",
        "TEXT",
        "c",
        "TAG",
        "s",
        "TAG",
        "SEPARATOR",
        ";",
        "CASESENSITIVE",
        "n",
        "NUMERIC",
        "m",
        "NUMERIC",
        "SORTABLE",
    ];
    let spec = vector.map(|algo| {
        if algo == "FLAT" {
            vec![
                "v",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "8",
                "DISTANCE_METRIC",
                "L2",
            ]
        } else {
            vec![
                "v",
                "VECTOR",
                "HNSW",
                "10",
                "TYPE",
                "FLOAT32",
                "DIM",
                "8",
                "DISTANCE_METRIC",
                "L2",
                "M",
                "16",
                "EF_CONSTRUCTION",
                "200",
            ]
        }
    });
    if let Some(spec) = &spec {
        args.extend(spec.iter().copied());
    }
    let reply = c.cmd(&args);
    assert!(reply.ok(), "{reply:?}");
}

/// The keys a query finds, with the postings on or off.
fn search(c: &mut Conn, index: &str, query: &str) -> Vec<String> {
    let reply = c.cmd(&[
        "FT.SEARCH",
        index,
        query,
        "NOCONTENT",
        "LIMIT",
        "0",
        "100000",
    ]);
    assert!(matches!(reply, V::Arr(_)), "{query}: {reply:?}");
    reply.keys()
}

fn aggregate_count(c: &mut Conn, index: &str, query: &str) -> i64 {
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        index,
        query,
        "GROUPBY",
        "0",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "n",
    ]);
    let rows = reply.arr();
    if rows.len() < 2 {
        return 0;
    }
    rows[1].get("n").i()
}

fn postings(c: &mut Conn, index: &str, on: bool) {
    let reply = c.cmd(&["FT.DEBUG", "POSTINGS", index, if on { "ON" } else { "OFF" }]);
    assert!(reply.ok(), "{reply:?}");
}

/// Every query of a round, three ways: the model, the postings, the scan.
fn check_round(
    c: &mut Conn,
    rng: &mut Rng,
    index: &str,
    model: &BTreeMap<String, Doc>,
    round: usize,
) {
    for _ in 0..60 {
        let filter = random_filter(rng, 3);
        let query = filter.query();
        let expected: Vec<String> = model
            .iter()
            .filter(|(_, doc)| filter.eval(doc))
            .map(|(key, _)| key.clone())
            .collect();
        let found = search(c, index, &query);
        postings(c, index, false);
        let scanned = search(c, index, &query);
        postings(c, index, true);
        assert_eq!(
            scanned, expected,
            "round {round}: the scan differs from the model for {query}"
        );
        assert_eq!(
            found, expected,
            "round {round}: the postings differ for {query}"
        );
        assert_eq!(
            aggregate_count(c, index, &query),
            expected.len() as i64,
            "round {round}: FT.AGGREGATE counts otherwise for {query}"
        );
    }
}

/// The random writes of a round, made to the server and to the model.
fn mutate(
    c: &mut Conn,
    rng: &mut Rng,
    prefix: &str,
    model: &mut BTreeMap<String, Doc>,
    next: &mut usize,
) {
    // From the model as it is now: a key deleted, renamed or moved away
    // earlier in the round is not picked again.
    let some_key = |rng: &mut Rng, model: &BTreeMap<String, Doc>| {
        let n = rng.below(model.len());
        model.keys().nth(n).unwrap().clone()
    };
    for _ in 0..40 {
        if model.is_empty() {
            break;
        }
        match rng.below(12) {
            0 | 1 => {
                // Some fields written again.
                let key = some_key(rng, model);
                let fresh = random_doc(rng, false);
                let written: Doc = fresh.into_iter().filter(|_| rng.chance(50)).collect();
                if written.is_empty() {
                    continue;
                }
                hset(c, &key, &written);
                model.get_mut(&key).unwrap().extend(written);
            }
            2 => {
                let key = some_key(rng, model);
                let field = rng.pick(&PREFIX_FIELDS);
                c.cmd(&["HDEL", &key, field]);
                let doc = model.get_mut(&key).unwrap();
                doc.remove(field);
                if doc.is_empty() {
                    model.remove(&key);
                }
            }
            3 => {
                let key = some_key(rng, model);
                let by = (rng.below(5) as i64 - 2).to_string();
                let reply = c.cmd(&["HINCRBY", &key, "m", &by]);
                if let V::Int(now) = reply {
                    model
                        .get_mut(&key)
                        .unwrap()
                        .insert("m".into(), now.to_string());
                }
            }
            4 => {
                let key = some_key(rng, model);
                c.cmd(&["DEL", &key]);
                model.remove(&key);
            }
            5 => {
                // Renamed, inside the prefix or out of it.
                let key = some_key(rng, model);
                let out = rng.chance(30);
                let to = if out {
                    format!("elsewhere:{next}")
                } else {
                    format!("{prefix}{next}")
                };
                *next += 1;
                assert!(c.cmd(&["RENAME", &key, &to]).ok());
                let doc = model.remove(&key).unwrap();
                if !out {
                    model.insert(to, doc);
                }
            }
            6 => {
                let key = some_key(rng, model);
                let to = format!("{prefix}{next}");
                *next += 1;
                c.cmd(&["COPY", &key, &to]);
                let doc = model[&key].clone();
                model.insert(to, doc);
            }
            7 => {
                // Gone to another database, and back.
                let key = some_key(rng, model);
                assert_eq!(c.cmd(&["MOVE", &key, "3"]), V::Int(1));
                if rng.chance(50) {
                    c.cmd(&["SELECT", "3"]);
                    assert_eq!(c.cmd(&["MOVE", &key, "0"]), V::Int(1));
                    c.cmd(&["SELECT", "0"]);
                } else {
                    model.remove(&key);
                }
            }
            8 => {
                // Expires, and is found expired by a read.
                let key = some_key(rng, model);
                c.cmd(&["PEXPIRE", &key, "1"]);
                thread::sleep(Duration::from_millis(5));
                assert_eq!(c.cmd(&["EXISTS", &key]), V::Int(0));
                model.remove(&key);
            }
            9 => {
                // A field expires, and a read drops it before the sweep can.
                let key = some_key(rng, model);
                let field = rng.pick(&["c", "n", "s"]);
                let reply = c.cmd(&["HPEXPIRE", &key, "1", "FIELDS", "1", field]);
                thread::sleep(Duration::from_millis(5));
                c.cmd(&["HGETALL", &key]);
                if reply.arr()[0] == V::Int(1) {
                    let doc = model.get_mut(&key).unwrap();
                    doc.remove(field);
                    if doc.is_empty() {
                        model.remove(&key);
                    }
                }
            }
            10 => {
                // A string over a document.
                let key = some_key(rng, model);
                assert!(c.cmd(&["SET", &key, "not a hash"]).ok());
                model.remove(&key);
                if rng.chance(50) {
                    c.cmd(&["DEL", &key]);
                }
            }
            _ => {
                let key = format!("{prefix}{next}");
                *next += 1;
                let doc = random_doc(rng, false);
                hset(c, &key, &doc);
                model.insert(
                    key,
                    if doc.is_empty() {
                        Doc::from([("other".into(), "1".into())])
                    } else {
                        doc
                    },
                );
            }
        }
    }
}

#[test]
fn random_filters_answer_from_postings_what_the_scan_answers() {
    let mut c = conn();
    let (index, prefix) = ("rf", "rf:");
    create(&mut c, index, prefix, None);
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut model: BTreeMap<String, Doc> = BTreeMap::new();
    for i in 0..400 {
        let key = format!("{prefix}{i}");
        let doc = random_doc(&mut rng, false);
        hset(&mut c, &key, &doc);
        model.insert(
            key,
            if doc.is_empty() {
                Doc::from([("other".into(), "1".into())])
            } else {
                doc
            },
        );
    }
    // Outside the prefix: in no index.
    hset(
        &mut c,
        "elsewhere:x",
        &Doc::from([("c".into(), "red".into())]),
    );
    let mut next = 400;
    for round in 0..6 {
        check_round(&mut c, &mut rng, index, &model, round);
        mutate(&mut c, &mut rng, prefix, &mut model, &mut next);
    }
    check_round(&mut c, &mut rng, index, &model, 6);

    // FT.INFO says what the postings hold.
    let info = c.cmd(&["FT.INFO", index]);
    assert_eq!(info.get("doc_ids").i(), model.len() as i64, "{info:?}");
    assert_eq!(info.get("num_docs").i(), model.len() as i64, "{info:?}");
}

/// A TAG or NUMERIC field added by FT.ALTER is answered from postings filled
/// from the documents already there.
#[test]
fn fields_added_by_alter_have_postings() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "al",
            "PREFIX",
            "1",
            "al:",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .ok()
    );
    for i in 0..50 {
        let key = format!("al:{i}");
        let colour = if i % 5 == 0 { "red" } else { "blue" };
        c.cmd(&["HSET", &key, "t", "word", "c", colour, "n", &i.to_string()]);
    }
    assert!(
        c.cmd(&[
            "FT.ALTER", "al", "SCHEMA", "ADD", "c", "TAG", "n", "NUMERIC"
        ])
        .ok()
    );
    assert_eq!(search(&mut c, "al", "@c:{red}").len(), 10);
    assert_eq!(search(&mut c, "al", "@n:[10 19]").len(), 10);
    assert_eq!(search(&mut c, "al", "-@c:{red}").len(), 40);
    c.cmd(&["HSET", "al:0", "c", "blue"]);
    assert_eq!(search(&mut c, "al", "@c:{red}").len(), 9);
}

// ── Filtered KNN ──────────────────────────────────────────────────────────────

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// The `k` nearest keys among those the filter matches.
fn exact_knn(model: &BTreeMap<String, Doc>, filter: &F, q: &[f32], k: usize) -> Vec<String> {
    let mut scored: Vec<(f32, &String)> = model
        .iter()
        .filter(|(_, doc)| filter.eval(doc) && doc.contains_key("v"))
        .map(|(key, doc)| (l2(&decode(&doc["v"]), q), key))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(b.1)));
    scored
        .into_iter()
        .take(k)
        .map(|(_, key)| key.clone())
        .collect()
}

/// `found` is as near as `exact`, position by position, to within float
/// rounding: the same documents but for ties, in an order that ties allow.
fn assert_same_nearest(
    model: &BTreeMap<String, Doc>,
    q: &[f32],
    found: &[String],
    exact: &[String],
    what: &str,
) {
    let dist = |key: &String| model.get(key).map(|doc| l2(&decode(&doc["v"]), q));
    assert_eq!(
        found.len(),
        exact.len(),
        "{what}: {found:?} against {exact:?}"
    );
    for (f, e) in found.iter().zip(exact) {
        let (Some(df), Some(de)) = (dist(f), dist(e)) else {
            panic!("{what}: {found:?} against {exact:?}");
        };
        assert!(
            (df - de).abs() <= 1e-5 * de.abs().max(1.0),
            "{what}: {found:?} against {exact:?} ({f} at {df}, {e} at {de})"
        );
    }
}

fn knn(c: &mut Conn, index: &str, filter: &str, q: &[f32], extra: &str) -> V {
    let query = format!("({filter})=>[KNN 10 @v $q{extra}]");
    let b = blob(q);
    c.raw(&[
        b"FT.SEARCH",
        index.as_bytes(),
        query.as_bytes(),
        b"NOCONTENT",
        b"LIMIT",
        b"0",
        b"10",
        b"PARAMS",
        b"2",
        b"q",
        &b,
        b"DIALECT",
        b"2",
    ])
}

fn recall(found: &[String], exact: &[String]) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    found.iter().filter(|k| exact.contains(k)).count() as f64 / exact.len() as f64
}

fn knn_corpus(
    c: &mut Conn,
    index: &str,
    prefix: &str,
    algo: &str,
    n: usize,
    seed: u64,
) -> BTreeMap<String, Doc> {
    create(c, index, prefix, Some(algo));
    let mut rng = Rng(seed);
    let mut model = BTreeMap::new();
    for i in 0..n {
        let key = format!("{prefix}{i}");
        let mut doc = random_doc(&mut rng, true);
        // A tag of ten classes, for a filter that lets 10% through.
        doc.insert("s".into(), format!("k{}", i % 10));
        hset(c, &key, &doc);
        model.insert(key, doc);
    }
    model
}

#[test]
fn filtered_knn_measures_exactly_what_the_filter_matches() {
    let mut c = conn();
    for (index, prefix, algo) in [("kh", "kh:", "HNSW"), ("kf", "kf:", "FLAT")] {
        let mut model = knn_corpus(&mut c, index, prefix, algo, 1500, 0x9e37_79b9_7f4a_7c15);
        let mut rng = Rng(77);
        let mut next = 1500;
        for round in 0..3 {
            for _ in 0..25 {
                let filter = random_filter(&mut rng, 2);
                let q: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
                let exact = exact_knn(&model, &filter, &q, 10);
                let measured = knn(
                    &mut c,
                    index,
                    &filter.query(),
                    &q,
                    " HYBRID_POLICY ADHOC_BF",
                );
                assert_same_nearest(
                    &model,
                    &q,
                    &measured.ordered_keys(),
                    &exact,
                    &format!(
                        "{index} round {round}: ADHOC_BF is not exact for {}",
                        filter.query()
                    ),
                );
                postings(&mut c, index, false);
                let scanned = knn(&mut c, index, &filter.query(), &q, "");
                postings(&mut c, index, true);
                // The scan's distance kernel rounds otherwise than the
                // model's, so two documents at (nearly) the same distance
                // may come in either order: compared by distance.
                assert_same_nearest(
                    &model,
                    &q,
                    &scanned.ordered_keys(),
                    &exact,
                    &format!("{index}: the scan is not exact"),
                );
            }
            mutate(&mut c, &mut rng, prefix, &mut model, &mut next);
            // Writes of the vector too: some documents get a new one.
            for _ in 0..20 {
                let keys: Vec<String> = model.keys().cloned().collect();
                let key = keys[rng.below(keys.len())].clone();
                let v: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
                let doc = Doc::from([("v".to_string(), encode(&v))]);
                hset(&mut c, &key, &doc);
                model.get_mut(&key).unwrap().insert("v".into(), encode(&v));
            }
        }
    }
}

#[test]
fn filtered_knn_searches_the_graph_with_the_recall_of_an_unfiltered_search() {
    let mut c = conn();
    let model = knn_corpus(&mut c, "kr", "kr:", "HNSW", 4000, 0x1234_5678_9abc_def1);
    let mut rng = Rng(5);
    let mut total = [0.0f64; 4];
    const QUERIES: usize = 40;
    for _ in 0..QUERIES {
        let q: Vec<f32> = (0..DIM).map(|_| rng.unit()).collect();
        // Unfiltered, at the same ef.
        let all = F::All;
        let exact = exact_knn(&model, &all, &q, 10);
        let found = knn(&mut c, "kr", "*", &q, " EF_RUNTIME 64").ordered_keys();
        total[0] += recall(&found, &exact);
        // 90% of the documents: the graph is searched (by default).
        let most = F::Not(Box::new(F::TagS(vec!["k3".into()])));
        let exact = exact_knn(&model, &most, &q, 10);
        let found = knn(&mut c, "kr", &most.query(), &q, " EF_RUNTIME 64").ordered_keys();
        total[1] += recall(&found, &exact);
        // 10%, searched because asked to.
        let few = F::TagS(vec!["k7".into()]);
        let exact = exact_knn(&model, &few, &q, 10);
        let found = knn(
            &mut c,
            "kr",
            &few.query(),
            &q,
            " EF_RUNTIME 64 HYBRID_POLICY BATCHES BATCH_SIZE 50",
        )
        .ordered_keys();
        total[2] += recall(&found, &exact);
        // 10%, measured (by default): exact.
        let found = knn(&mut c, "kr", &few.query(), &q, " EF_RUNTIME 64").ordered_keys();
        total[3] += recall(&found, &exact);
    }
    let [unfiltered, most, batches, measured] = total.map(|t| t / QUERIES as f64);
    eprintln!(
        "recall@10: unfiltered {unfiltered:.3}, 90% {most:.3}, 10% BATCHES {batches:.3}, 10% measured {measured:.3}"
    );
    assert!(
        most >= unfiltered - 0.02,
        "90%: {most} against {unfiltered} unfiltered"
    );
    assert!(batches >= 0.9, "BATCHES: {batches}");
    assert_eq!(measured, 1.0);
}

#[test]
fn hybrid_policy_is_checked() {
    let mut c = conn();
    knn_corpus(&mut c, "kp", "kp:", "HNSW", 50, 3);
    let q = [0.5f32; DIM];
    let reply = knn(&mut c, "kp", "@s:{k1}", &q, " HYBRID_POLICY SOMETIMES");
    assert!(
        matches!(&reply, V::Err(e) if e.contains("hybrid policy")),
        "{reply:?}"
    );
    let reply = knn(
        &mut c,
        "kp",
        "@s:{k1}",
        &q,
        " HYBRID_POLICY ADHOC_BF BATCH_SIZE 10",
    );
    assert!(
        matches!(&reply, V::Err(e) if e.contains("batch size")),
        "{reply:?}"
    );
    let reply = knn(
        &mut c,
        "kp",
        "@s:{k1}",
        &q,
        " HYBRID_POLICY batches BATCH_SIZE 10",
    );
    assert_eq!(reply.arr()[0], V::Int(5), "{reply:?}");
    // As attributes too.
    let b = blob(&q);
    let reply = c.raw(&[
        b"FT.SEARCH",
        b"kp",
        b"(@s:{k1})=>[KNN 10 @v $q]=>{$HYBRID_POLICY: ADHOC_BF; $EF_RUNTIME: 20}",
        b"NOCONTENT",
        b"PARAMS",
        b"2",
        b"q",
        &b,
        b"DIALECT",
        b"2",
    ]);
    assert_eq!(reply.arr()[0], V::Int(5), "{reply:?}");
}

// ── The page from postings ────────────────────────────────────────────────────
//
// A query whose matches the postings know exactly reads only the documents
// of the page it returns (`search::page_from_postings`). Its reply must be
// the reply of the query that reads every match (`FT.DEBUG PAGE … OFF`),
// whatever the query asks for, and its count the model's: no document whose
// key has expired, even before anything has deleted it, and none deleted.

/// Held by every test of this file that times a query out (as in
/// tests/search.rs: `FT.CONFIG` is the process's).
fn timeout_test() -> std::sync::MutexGuard<'static, ()> {
    static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
}

fn page(c: &mut Conn, index: &str, on: bool) {
    let reply = c.cmd(&["FT.DEBUG", "PAGE", index, if on { "ON" } else { "OFF" }]);
    assert!(reply.ok(), "{reply:?}");
}

/// FT.SEARCH `index` `args`: the page from postings, every match read, and
/// every document tried (postings off). Asked again if a key was deleted
/// meanwhile (the active expiry cycle): relevance counts the documents.
fn three_ways(c: &mut Conn, index: &str, args: &[String]) -> (V, V, V) {
    let mut cmd: Vec<&str> = vec!["FT.SEARCH", index];
    cmd.extend(args.iter().map(String::as_str));
    loop {
        let before = c.cmd(&["DBSIZE"]);
        let paged = c.cmd(&cmd);
        page(c, index, false);
        let whole = c.cmd(&cmd);
        postings(c, index, false);
        let scanned = c.cmd(&cmd);
        postings(c, index, true);
        page(c, index, true);
        if c.cmd(&["DBSIZE"]) == before {
            return (paged, whole, scanned);
        }
    }
}

/// The count of an FT.SEARCH reply, in either protocol.
fn total_of(reply: &V) -> i64 {
    match reply {
        V::Map(_) => reply.get("total_results").i(),
        V::Arr(items) => items[0].i(),
        other => panic!("not a search reply: {other:?}"),
    }
}

/// Values of the NUMERIC field `o`: ties, minus zero beside zero, the
/// infinities, and one that is no number. No NaN: with one, SORTBY o would
/// read every match.
const O_VALUES: [&str; 9] = ["3", "-0", "0", "x", "-1.5", "3", "inf", "-inf", "12"];

/// The options of a random FT.SEARCH of `query`, and whether its count is
/// the number of documents the filter matches (no INKEYS, no FILTER).
fn random_shape(rng: &mut Rng, query: String, keys: &[String]) -> (Vec<String>, bool) {
    let mut args = vec![query];
    let mut plain = true;
    let push = |args: &mut Vec<String>, words: &[&str]| {
        args.extend(words.iter().map(|w| w.to_string()));
    };
    if rng.chance(35) {
        push(&mut args, &["NOCONTENT"]);
    }
    if rng.chance(30) {
        push(&mut args, &["WITHSCORES"]);
    }
    if rng.chance(20) {
        push(&mut args, &["WITHSORTKEYS"]);
    }
    match rng.below(10) {
        0 | 1 => push(&mut args, &["RETURN", "2", "c", "o"]),
        2 => push(&mut args, &["RETURN", "3", "m", "AS", "mm"]),
        3 => push(&mut args, &["RETURN", "0"]),
        _ => {}
    }
    let sortby = match rng.below(12) {
        0..=2 => Some("o"),
        3 | 4 => Some("m"),
        // NaN in it: every match read.
        5 => Some("n"),
        // A TAG field: every match read.
        6 => Some("c"),
        _ => None,
    };
    if let Some(field) = sortby {
        push(&mut args, &["SORTBY", field]);
        match rng.below(3) {
            0 => push(&mut args, &["ASC"]),
            1 => push(&mut args, &["DESC"]),
            _ => {}
        }
    }
    if rng.chance(10) {
        push(&mut args, &["HIGHLIGHT", "FIELDS", "1", "t"]);
    }
    if rng.chance(10) && !keys.is_empty() {
        let n = 1 + rng.below(30);
        push(&mut args, &["INKEYS", (n + 1).to_string().as_str()]);
        for _ in 0..n {
            let key = keys[rng.below(keys.len())].clone();
            args.push(key);
        }
        args.push("no:such:key".into());
        plain = false;
    }
    if rng.chance(10) {
        let lo = rng.below(8).to_string();
        let hi = (4 + rng.below(12)).to_string();
        push(&mut args, &["FILTER", "m", lo.as_str(), hi.as_str()]);
        plain = false;
    }
    match rng.below(7) {
        0 => {}
        1 => push(&mut args, &["LIMIT", "0", "0"]),
        2 => push(&mut args, &["LIMIT", "100000", "10"]),
        3 => {
            let offset = (200 + rng.below(250)).to_string();
            push(&mut args, &["LIMIT", offset.as_str(), "10"]);
        }
        _ => {
            let offset = rng.below(60).to_string();
            let count = (1 + rng.below(15)).to_string();
            push(&mut args, &["LIMIT", offset.as_str(), count.as_str()]);
        }
    }
    (args, plain)
}

fn create_paged(c: &mut Conn, index: &str, prefix: &str, json: bool) {
    let schema: &[&str] = if json {
        &[
            "ON",
            "JSON",
            "PREFIX",
            "1",
            prefix,
            "SCHEMA",
            "$.t",
            "AS",
            "t",
            "TEXT",
            "$.c",
            "AS",
            "c",
            "TAG",
            "$.s",
            "AS",
            "s",
            "TAG",
            "SEPARATOR",
            ";",
            "CASESENSITIVE",
            "$.n",
            "AS",
            "n",
            "NUMERIC",
            "$.m",
            "AS",
            "m",
            "NUMERIC",
            "SORTABLE",
            "$.o",
            "AS",
            "o",
            "NUMERIC",
        ]
    } else {
        &[
            "ON",
            "HASH",
            "PREFIX",
            "1",
            prefix,
            "SCHEMA",
            "t",
            "TEXT",
            "c",
            "TAG",
            "s",
            "TAG",
            "SEPARATOR",
            ";",
            "CASESENSITIVE",
            "n",
            "NUMERIC",
            "m",
            "NUMERIC",
            "SORTABLE",
            "o",
            "NUMERIC",
        ]
    };
    let mut args = vec!["FT.CREATE", index];
    args.extend_from_slice(schema);
    let reply = c.cmd(&args);
    assert!(reply.ok(), "{reply:?}");
}

/// A document as JSON: numbers as numbers where they are finite ones.
fn json_doc(doc: &Doc) -> String {
    let fields: Vec<String> = doc
        .iter()
        .map(|(field, value)| {
            let number = value.trim().parse::<f64>().ok().filter(|n| n.is_finite());
            match number {
                Some(_) if matches!(field.as_str(), "n" | "m" | "o") => {
                    format!("\"{field}\":{}", value.trim())
                }
                _ => format!("\"{field}\":\"{}\"", value.replace('"', "")),
            }
        })
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// Some keys given a time to live that ends in a few milliseconds and some
/// one that does not, then a wait past the first: queries made at once find
/// those keys expired before the active cycle (every 100 ms) deletes most of
/// them.
fn expire_some(c: &mut Conn, rng: &mut Rng, model: &mut BTreeMap<String, Doc>) {
    for i in 0..7 {
        if model.is_empty() {
            return;
        }
        let key = model.keys().nth(rng.below(model.len())).unwrap().clone();
        if i < 4 {
            assert_eq!(c.cmd(&["PEXPIRE", &key, "15"]), V::Int(1));
            model.remove(&key);
        } else {
            assert_eq!(c.cmd(&["PEXPIRE", &key, "600000"]), V::Int(1));
        }
    }
    thread::sleep(Duration::from_millis(20));
}

/// Random queries of every shape, each three ways, on hashes and on JSON,
/// with documents written, deleted and expired between rounds.
#[test]
fn a_page_from_postings_is_the_page_of_every_match() {
    let mut c = conn();
    let mut rng = Rng(0x6a09_e667_f3bc_c909);
    for json in [false, true] {
        let (index, prefix) = if json { ("pj", "pj:") } else { ("ph", "ph:") };
        create_paged(&mut c, index, prefix, json);
        let mut model: BTreeMap<String, Doc> = BTreeMap::new();
        let write = |c: &mut Conn, rng: &mut Rng, key: &str| -> Doc {
            let mut doc = random_doc(rng, false);
            if rng.chance(85) {
                doc.insert("o".into(), rng.pick(&O_VALUES).to_string());
            }
            if doc.is_empty() {
                doc.insert("other".into(), "1".into());
            }
            if json {
                let reply = c.cmd(&["JSON.SET", key, "$", &json_doc(&doc)]);
                assert!(reply.ok(), "{reply:?}");
            } else {
                hset(c, key, &doc);
            }
            doc
        };
        for i in 0..400 {
            let key = format!("{prefix}{i}");
            let doc = write(&mut c, &mut rng, &key);
            model.insert(key, doc);
        }
        let mut next = 400;
        for round in 0..5 {
            for batch in 0..4 {
                expire_some(&mut c, &mut rng, &mut model);
                let keys: Vec<String> = model.keys().cloned().collect();
                let resp3 = batch == 3;
                if resp3 {
                    c.cmd(&["HELLO", "3"]);
                }
                for _ in 0..15 {
                    let filter = random_filter(&mut rng, 3);
                    let (args, plain) = random_shape(&mut rng, filter.query(), &keys);
                    let (paged, whole, scanned) = three_ways(&mut c, index, &args);
                    assert_eq!(paged, whole, "json {json}, round {round}: {args:?}");
                    assert_eq!(scanned, whole, "json {json}, round {round}: {args:?}");
                    if plain && !json {
                        let expected = model.values().filter(|doc| filter.eval(doc)).count();
                        assert_eq!(
                            total_of(&paged),
                            expected as i64,
                            "round {round}: {args:?}: {paged:?}"
                        );
                    }
                }
                if resp3 {
                    c.cmd(&["HELLO", "2"]);
                }
            }
            if json {
                // Some documents deleted, some written again or added.
                for _ in 0..30 {
                    let key = format!("{prefix}{}", rng.below(next + 20));
                    if rng.chance(40) {
                        c.cmd(&["DEL", &key]);
                        model.remove(&key);
                    } else {
                        let doc = write(&mut c, &mut rng, &key);
                        model.insert(key, doc);
                    }
                }
                next += 20;
            } else {
                mutate(&mut c, &mut rng, prefix, &mut model, &mut next);
            }
        }
    }
}

/// The count read off postings is exact: documents deleted are not in it,
/// nor those whose key has expired, deleted yet or not. With more keys to
/// live a while than matches,
/// the documents are read instead, and the count is the same.
#[test]
fn a_count_from_postings_leaves_out_what_is_gone() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "pc",
            "PREFIX",
            "1",
            "pc:",
            "SCHEMA",
            "c",
            "TAG",
            "o",
            "NUMERIC",
        ])
        .ok()
    );
    for start in (0..2000).step_by(250) {
        for i in start..start + 250 {
            let key = format!("pc:{i}");
            let o = (i % 100).to_string();
            c.send(&[b"HSET", key.as_bytes(), b"c", b"red", b"o", o.as_bytes()]);
        }
        for _ in 0..250 {
            c.read();
        }
    }
    let count = |c: &mut Conn, query: &str| -> i64 {
        total_of(&c.cmd(&["FT.SEARCH", "pc", query, "NOCONTENT", "LIMIT", "0", "0"]))
    };
    assert_eq!(count(&mut c, "@c:{red}"), 2000);
    for i in 0..10 {
        assert_eq!(c.cmd(&["DEL", &format!("pc:{i}")]), V::Int(1));
    }
    // To live on: 200 keys with a time to live, all matches.
    for i in 10..210 {
        assert_eq!(c.cmd(&["EXPIRE", &format!("pc:{i}"), "600"]), V::Int(1));
    }
    let mut checked = 0;
    for attempt in 0..20 {
        // Expired a moment ago; most are still there when the queries run.
        let gone: Vec<String> = (0..15)
            .map(|i| format!("pc:{}", 300 + attempt * 15 + i))
            .collect();
        for key in &gone {
            assert_eq!(c.cmd(&["PEXPIRE", key, "10"]), V::Int(1));
        }
        thread::sleep(Duration::from_millis(15));
        let left = 2000 - 10 - 15 * (attempt as i64 + 1);
        assert_eq!(count(&mut c, "@c:{red}"), left, "attempt {attempt}");
        assert_eq!(count(&mut c, "*"), left, "attempt {attempt}");
        // `o` is 0..99 across the keys: those of 0 to 4, about 100 of them,
        // fewer than the 200 keys with a time to live: read.
        let few = (0..2000)
            .filter(|i| *i >= 10 && (*i < 300 || *i >= 300 + 15 * (attempt + 1)))
            .filter(|i| i % 100 < 5)
            .count() as i64;
        assert_eq!(count(&mut c, "@o:[0 4]"), few, "attempt {attempt}");
        for args in [
            vec!["@c:{red}", "NOCONTENT"],
            vec!["@c:{red}", "SORTBY", "o", "DESC", "LIMIT", "0", "20"],
            vec!["*", "SORTBY", "o", "LIMIT", "90", "30", "WITHSORTKEYS"],
            vec!["-@o:[10 90]", "LIMIT", "5", "5"],
        ] {
            let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            let (paged, whole, scanned) = three_ways(&mut c, "pc", &args);
            assert_eq!(paged, whole, "{args:?}");
            assert_eq!(scanned, whole, "{args:?}");
        }
        checked += 1;
    }
    assert_eq!(checked, 20);
}

/// The postings' thread merges while queries read: thousands of documents
/// written and deleted between queries, so that queries meet waiting ids,
/// runs, and merges under way, of the key order and of a NUMERIC field. Each
/// page is the page of every match.
/// With the TEXT index's words read off by id (#214), a TEXT query's
/// documents are exactly known, and sorted by a NUMERIC field its page comes
/// from the postings (#215): the same page, and the same scores, as reading
/// every match, under every scorer; and without SORTBY where every match
/// scores the same.
#[test]
fn a_text_query_sorted_by_a_number_is_paged_with_its_scores() {
    let mut c = conn();
    let reply = c.cmd(&[
        "FT.CREATE",
        "tq",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "tq:",
        "SCHEMA",
        "t",
        "TEXT",
        "WEIGHT",
        "2",
        "b",
        "TEXT",
        "c",
        "TAG",
        "n",
        "NUMERIC",
        "SORTABLE",
    ]);
    assert!(reply.ok(), "{reply:?}");
    let words = [
        "hello", "world", "running", "runs", "ran", "blue", "green", "the", "quick", "fox",
    ];
    let mut rng = Rng(0x3c6e_f372_fe94_f82b);
    let write = |c: &mut Conn, rng: &mut Rng, i: usize| {
        let mut doc = Doc::new();
        let len = 1 + rng.below(12);
        let t: Vec<&str> = (0..len).map(|_| rng.pick(&words)).collect();
        doc.insert("t".into(), t.join(" "));
        if rng.chance(50) {
            doc.insert("b".into(), rng.pick(&words).to_string());
        }
        doc.insert("c".into(), rng.pick(&["x", "y", "z"]).to_string());
        if rng.chance(90) {
            // Ties, so that the key breaks them.
            doc.insert("n".into(), (rng.below(40) as i64 - 20).to_string());
        }
        hset(c, &format!("tq:{i}"), &doc);
    };
    for i in 0..300 {
        write(&mut c, &mut rng, i);
    }
    let queries = [
        "hello",
        "@t:world",
        "hello world",
        "hello | fox",
        "run*",
        "running",
        "@t:(blue green) @c:{x}",
        "fox -@c:{y}",
        "@b:quick @n:[-5 5]",
    ];
    let scorers = [
        "BM25STD",
        "TFIDF",
        "TFIDF.DOCNORM",
        "BM25",
        "DISMAX",
        "DOCSCORE",
    ];
    for round in 0..3 {
        for query in queries {
            for scorer in scorers {
                for (sort, limits) in [
                    (Some("ASC"), ("0", "10")),
                    (Some("DESC"), ("7", "15")),
                    (None, ("3", "8")),
                ] {
                    let mut args: Vec<String> = vec![query.into(), "SCORER".into(), scorer.into()];
                    if let Some(order) = sort {
                        args.extend(["SORTBY".into(), "n".into(), order.into()]);
                    }
                    args.extend([
                        "LIMIT".into(),
                        limits.0.into(),
                        limits.1.into(),
                        "WITHSCORES".into(),
                    ]);
                    let (paged, whole, scanned) = three_ways(&mut c, "tq", &args);
                    assert_eq!(paged, whole, "round {round}: {args:?}");
                    assert_eq!(scanned, whole, "round {round}: {args:?}");
                    assert!(
                        total_of(&whole) > 0 || query.contains('@'),
                        "{args:?}: {whole:?}"
                    );
                }
            }
        }
        // Documents written again and deleted: their ids given back, and
        // given out again, the words with them.
        for _ in 0..60 {
            let i = rng.below(320);
            if rng.chance(30) {
                c.cmd(&["DEL", &format!("tq:{i}")]);
            } else {
                write(&mut c, &mut rng, i);
            }
        }
    }
    c.cmd(&["FT.DROPINDEX", "tq"]);
}

#[test]
fn a_page_is_right_while_the_postings_merge() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "pm",
            "PREFIX",
            "1",
            "pm:",
            "SCHEMA",
            "c",
            "TAG",
            "o",
            "NUMERIC",
        ])
        .ok()
    );
    let mut rng = Rng(0x452821e638d01377);
    let mut written: Vec<String> = Vec::new();
    for round in 0..24 {
        // A burst of new documents, pipelined, and some deleted.
        for _ in 0..12 {
            for _ in 0..250 {
                let key = format!("pm:{}", rng.below(1 << 30));
                let tag = rng.pick(&["red", "blue", "green"]);
                let o = (rng.below(2000) as i64 - 1000).to_string();
                c.send(&[
                    b"HSET",
                    key.as_bytes(),
                    b"c",
                    tag.as_bytes(),
                    b"o",
                    o.as_bytes(),
                ]);
                written.push(key);
            }
            for _ in 0..250 {
                c.read();
            }
        }
        for _ in 0..200 {
            let key = written.swap_remove(rng.below(written.len()));
            c.cmd(&["DEL", &key]);
        }
        for query in [
            vec!["@c:{red}"],
            vec!["*", "LIMIT", "500", "10"],
            vec!["@c:{blue}", "SORTBY", "o", "DESC", "LIMIT", "0", "15"],
            vec!["@o:[-200 300]", "NOCONTENT", "LIMIT", "0", "40"],
            vec![
                "@c:{green}",
                "SORTBY",
                "o",
                "LIMIT",
                "1000",
                "5",
                "WITHSORTKEYS",
            ],
        ] {
            let args: Vec<String> = query.iter().map(|a| a.to_string()).collect();
            let (paged, whole, scanned) = three_ways(&mut c, "pm", &args);
            assert_eq!(paged, whole, "round {round}: {args:?}");
            assert_eq!(scanned, whole, "round {round}: {args:?}");
        }
    }
}

/// `TIMEOUT` cuts a query answered from postings short, as it does a scan:
/// documents are added until a 1 ms query reports it. Its matches are read
/// (`FT.DEBUG PAGE … OFF`): a count read off postings takes no time to
/// speak of, and is whole, as the end shows.
#[test]
fn a_query_from_postings_keeps_its_timeout() {
    let _one = timeout_test();
    let mut c = conn();
    assert!(c.cmd(&["HELLO", "3"]).get("proto").i() == 3);
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "pt",
            "PREFIX",
            "1",
            "pt:",
            "SCHEMA",
            "c",
            "TAG",
            "t",
            "TEXT"
        ])
        .ok()
    );
    page(&mut c, "pt", false);
    let warned = |reply: &V| {
        match reply {
        V::Map(pairs) => pairs.iter().any(|(k, v)| {
            matches!(k, V::Str(k) if k == "warning")
                && matches!(v, V::Arr(w) if w.iter().any(|w| matches!(w, V::Str(s) if s.contains("Timeout"))))
        }),
        _ => false,
    }
    };
    let mut n = 0;
    let mut timed_out = false;
    while n < 400_000 && !timed_out {
        // Pipelined 250 at a time: a client that writes more before it reads
        // can fill both sockets and wait on the server, which waits on it.
        for _ in 0..40 {
            for i in 0..250 {
                let key = format!("pt:{}", n + i);
                c.send(&[b"HSET", key.as_bytes(), b"c", b"red", b"t", b"hello"]);
            }
            for _ in 0..250 {
                c.read();
            }
            n += 250;
        }
        let reply = c.cmd(&[
            "FT.SEARCH",
            "pt",
            "@c:{red}",
            "NOCONTENT",
            "LIMIT",
            "0",
            "0",
            "TIMEOUT",
            "1",
        ]);
        let again = c.cmd(&[
            "FT.SEARCH",
            "pt",
            "@c:{red}",
            "NOCONTENT",
            "LIMIT",
            "0",
            "0",
            "TIMEOUT",
            "1",
        ]);
        timed_out = warned(&reply) && warned(&again);
    }
    assert!(timed_out, "TIMEOUT 1 still finished after {n} documents");
    let all = c.cmd(&[
        "FT.SEARCH",
        "pt",
        "@c:{red}",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "0",
    ]);
    assert_eq!(all.get("total_results").i(), n as i64, "{all:?}");
    assert!(!warned(&all));
    let cut = c.cmd(&[
        "FT.SEARCH",
        "pt",
        "@c:{red}",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "1",
    ]);
    assert!(warned(&cut), "{cut:?}");
    assert!(cut.get("total_results").i() < n as i64, "{cut:?}");
    // FT.AGGREGATE too.
    let agg = c.cmd(&[
        "FT.AGGREGATE",
        "pt",
        "@c:{red}",
        "TIMEOUT",
        "1",
        "GROUPBY",
        "0",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "c",
    ]);
    assert!(warned(&agg), "{agg:?}");
    // Counted off the postings: whole, in any time.
    page(&mut c, "pt", true);
    let counted = c.cmd(&[
        "FT.SEARCH",
        "pt",
        "@c:{red}",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "1",
    ]);
    assert!(!warned(&counted), "{counted:?}");
    assert_eq!(counted.get("total_results").i(), n as i64, "{counted:?}");
    c.cmd(&["HELLO", "2"]);
}

/// A write never waits for a query to let go of the postings: one that
/// finds them read is queued, and the next query applies the queue before
/// it reads. Four writers, each reading back what it has just written, race
/// two readers of broad pages and the postings' thread; in the end the
/// counts are the model's, and every page is the page of every match.
#[test]
fn writes_racing_queries_are_seen_by_the_next_query() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "pr",
            "PREFIX",
            "1",
            "pr:",
            "SCHEMA",
            "c",
            "TAG",
            "u",
            "TAG",
            "o",
            "NUMERIC",
            "SORTABLE",
        ])
        .ok()
    );
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = [
        vec!["@o:[0 600]", "SORTBY", "o", "LIMIT", "0", "10"],
        vec!["@c:{red|blue}", "NOCONTENT", "LIMIT", "200", "10"],
    ]
    .into_iter()
    .map(|query| {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut c = conn();
            let mut cmd = vec!["FT.SEARCH", "pr"];
            cmd.extend(query);
            let mut queries = 0;
            while !stop.load(Ordering::Relaxed) {
                let reply = c.cmd(&cmd);
                assert!(matches!(reply, V::Arr(_)), "{reply:?}");
                queries += 1;
            }
            queries
        })
    })
    .collect();
    let writers: Vec<_> = (0..4u64)
        .map(|w| {
            thread::spawn(move || {
                let mut c = conn();
                let mut rng = Rng(0x2545_f491_4f6c_dd1d ^ ((w + 1) << 40));
                let mut model: BTreeMap<String, Option<(String, usize)>> = BTreeMap::new();
                for i in 0..2500 {
                    let key = format!("pr:{w}:{}", rng.below(600));
                    if rng.chance(15) {
                        c.cmd(&["DEL", &key]);
                        model.insert(key, None);
                        continue;
                    }
                    let colour = rng.pick(&["red", "blue", "green"]).to_string();
                    let o = rng.below(1000);
                    let u = format!("w{w}x{i}");
                    let reply = c.cmd(&["HSET", &key, "c", &colour, "u", &u, "o", &o.to_string()]);
                    assert!(matches!(reply, V::Int(_)), "{reply:?}");
                    model.insert(key.clone(), Some((colour, o)));
                    if i % 4 == 0 {
                        // Read back at once: its own tag names it alone.
                        let query = format!("@u:{{{u}}}");
                        let found = c.cmd(&["FT.SEARCH", "pr", &query, "NOCONTENT"]);
                        assert_eq!(found.ordered_keys(), [key], "writer {w}, write {i}");
                    }
                }
                model
            })
        })
        .collect();
    let mut model: BTreeMap<String, Option<(String, usize)>> = BTreeMap::new();
    for writer in writers {
        model.extend(writer.join().unwrap());
    }
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        assert!(reader.join().unwrap() > 0);
    }
    let live: Vec<&(String, usize)> = model.values().flatten().collect();
    let count = |c: &mut Conn, query: &str| -> i64 {
        total_of(&c.cmd(&["FT.SEARCH", "pr", query, "NOCONTENT", "LIMIT", "0", "0"]))
    };
    assert_eq!(count(&mut c, "*"), live.len() as i64);
    assert_eq!(
        c.cmd(&["FT.INFO", "pr"]).get("doc_ids").i(),
        live.len() as i64
    );
    for colour in ["red", "blue", "green"] {
        let want = live.iter().filter(|doc| doc.0 == colour).count();
        assert_eq!(count(&mut c, &format!("@c:{{{colour}}}")), want as i64);
    }
    for (lo, hi) in [(0, 99), (250, 600), (990, 999)] {
        let want = live.iter().filter(|doc| (lo..=hi).contains(&doc.1)).count();
        assert_eq!(count(&mut c, &format!("@o:[{lo} {hi}]")), want as i64);
    }
    for query in [
        vec!["@o:[0 600]", "SORTBY", "o", "LIMIT", "0", "10"],
        vec!["@c:{red|blue}", "NOCONTENT", "LIMIT", "200", "10"],
        vec!["@c:{green}", "SORTBY", "o", "DESC", "LIMIT", "5", "20"],
    ] {
        let args: Vec<String> = query.iter().map(|a| a.to_string()).collect();
        let (paged, whole, scanned) = three_ways(&mut c, "pr", &args);
        assert_eq!(paged, whole, "{args:?}");
        assert_eq!(scanned, whole, "{args:?}");
    }
}
