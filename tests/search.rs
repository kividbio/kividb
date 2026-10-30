// tests/search.rs
//
// FT.CREATE's schema, the query language, and FT.SEARCH's arguments and
// replies, over the wire.
//
// The first part is the report that came out of running LiteLLM's semantic
// cache (RedisVL) against v1.0.4: eight items, each with the commands it gave
// and the reply Redis Stack gives.
//
// One server is shared by every test; each uses its own index and key prefix.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

// ── RESP client (RESP2 and RESP3) ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Double(f64),
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
    fn err(&self) -> &str {
        match self {
            V::Err(e) => e,
            other => panic!("not an error: {other:?}"),
        }
    }
    /// The value under `name`: in a map, or in a flat `[name, value, …]` array.
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
    /// The document ids of an FT.SEARCH reply (RESP2), in order.
    fn ids(&self) -> Vec<String> {
        self.arr()[1..]
            .iter()
            .filter_map(|v| match v {
                V::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }
    /// The same, sorted — for queries whose order is not the point.
    fn id_set(&self) -> Vec<String> {
        let mut ids = self.ids();
        ids.sort();
        ids
    }
    /// A flat `[name, value, …]` array as pairs.
    fn pairs(&self) -> Vec<(String, String)> {
        self.arr()
            .chunks(2)
            .map(|pair| (pair[0].s().to_string(), pair[1].s().to_string()))
            .collect()
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
                    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
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

    /// Arguments as bytes: a vector is not text.
    fn raw(&mut self, args: &[&[u8]]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
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
            "," => V::Double(rest.parse().unwrap()),
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
            "%" => {
                let n: usize = rest.parse().unwrap();
                V::Map((0..n).map(|_| (self.read(), self.read())).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see tests/generics.rs).
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

/// A port nothing listens on, outside the range the system hands out to
/// outgoing connections: a client dialling a server that is not listening
/// yet could otherwise be given the very port it dials, and talk to itself.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(37);
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

/// A server of its own, so `FT.CONFIG SET ON_TIMEOUT` and a large insert do
/// not share the process-wide index names of the other tests. `FT.CONFIG`
/// itself is still process-wide; fail tests restore `ON_TIMEOUT` at once.
fn own_conn() -> Conn {
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
            return Conn::connect(port);
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server on port {port} did not start");
}

/// `n` hashes `{prefix}{start}` …, pipelined so a timeout test can fill the
/// index without a round trip per document.
fn hset_many(c: &mut Conn, prefix: &str, start: usize, n: usize) {
    const BATCH: usize = 250;
    let mut i = 0;
    while i < n {
        let end = (i + BATCH).min(n);
        let keys: Vec<String> = (start + i..start + end)
            .map(|j| format!("{prefix}{j}"))
            .collect();
        let mut buf = Vec::new();
        for key in &keys {
            let args = ["HSET", key.as_str(), "t", "hello"];
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(a.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        c.writer.write_all(&buf).unwrap();
        for _ in 0..keys.len() {
            assert!(matches!(c.read(), V::Int(_)), "HSET");
        }
        i = end;
    }
}

/// `n` hashes `{prefix}{start}` … with a TAG `c`, `red` for even numbers and
/// `blue` for odd, and a 4-dimension vector `v`, pipelined like
/// [`hset_many`].
fn hset_tagged_many(c: &mut Conn, prefix: &str, start: usize, n: usize) {
    const BATCH: usize = 250;
    let mut i = 0;
    while i < n {
        let end = (i + BATCH).min(n);
        let mut buf = Vec::new();
        for j in start + i..start + end {
            let key = format!("{prefix}{j}");
            let tag: &[u8] = if j % 2 == 0 { b"red" } else { b"blue" };
            let blob = vector(&[j as f32, 1.0, 2.0, 3.0]);
            let args: [&[u8]; 6] = [b"HSET", key.as_bytes(), b"c", tag, b"v", &blob];
            buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in args {
                buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                buf.extend_from_slice(a);
                buf.extend_from_slice(b"\r\n");
            }
        }
        c.writer.write_all(&buf).unwrap();
        for _ in start + i..start + end {
            assert!(matches!(c.read(), V::Int(_)), "HSET");
        }
        i = end;
    }
}

fn vector(floats: &[f32]) -> Vec<u8> {
    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// `HSET key field value …` where one of the values is a vector.
fn hset_vector(c: &mut Conn, key: &str, fields: &[(&str, &str)], vec_field: &str, v: &[f32]) {
    let blob = vector(v);
    let mut args: Vec<&[u8]> = vec![b"HSET", key.as_bytes()];
    for (name, value) in fields {
        args.push(name.as_bytes());
        args.push(value.as_bytes());
    }
    args.push(vec_field.as_bytes());
    args.push(&blob);
    assert!(matches!(c.raw(&args), V::Int(_)));
}

/// FT.SEARCH with one vector parameter, `$q`, and whatever else is given.
fn search_with_vector(c: &mut Conn, index: &str, query: &str, q: &[f32], rest: &[&str]) -> V {
    let blob = vector(q);
    let mut args: Vec<&[u8]> = vec![b"FT.SEARCH", index.as_bytes(), query.as_bytes()];
    args.extend(rest.iter().map(|a| a.as_bytes()));
    args.extend([&b"PARAMS"[..], b"2", b"q", &blob, b"DIALECT", b"2"]);
    c.raw(&args)
}

// ══ The LiteLLM report ════════════════════════════════════════════════════════

/// Item 1: `FT.CREATE` rejects per-field schema options. Each of these was an
/// error; RedisVL always sends the first two.
#[test]
fn report_1_field_options_are_accepted() {
    let mut c = conn();
    for (n, schema) in [
        &["f", "TEXT", "WEIGHT", "1"][..],
        &["f", "TAG", "SEPARATOR", ","],
        &["f", "TEXT", "SORTABLE"],
        &["f", "TAG", "CASESENSITIVE"],
        &["f", "NUMERIC", "SORTABLE"],
        &[
            "f", "TEXT", "NOSTEM", "g", "TEXT", "PHONETIC", "dm:en", "h", "TAG", "SORTABLE", "UNF",
        ],
    ]
    .iter()
    .enumerate()
    {
        let index = format!("r1:i{n}");
        let prefix = format!("r1:{n}:");
        let mut args = vec![
            "FT.CREATE",
            index.as_str(),
            "ON",
            "HASH",
            "PREFIX",
            "1",
            prefix.as_str(),
            "SCHEMA",
        ];
        args.extend(schema.iter());
        let reply = c.cmd(&args);
        assert!(reply.ok(), "{schema:?}: {reply:?}");
    }

    // As RedisVL writes the semantic cache's index.
    let reply = c.cmd(&[
        "FT.CREATE",
        "r1:cache",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "r1:cache:",
        "SCHEMA",
        "prompt",
        "TEXT",
        "WEIGHT",
        "1",
        "response",
        "TEXT",
        "WEIGHT",
        "1",
        "litellm_cache_key",
        "TAG",
        "SEPARATOR",
        ",",
        "prompt_vector",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "COSINE",
    ]);
    assert!(reply.ok(), "{reply:?}");

    // FT.ALTER reads a schema the same way.
    let reply = c.cmd(&[
        "FT.ALTER",
        "r1:cache",
        "SCHEMA",
        "ADD",
        "model",
        "TAG",
        "SEPARATOR",
        "|",
        "tokens",
        "NUMERIC",
        "SORTABLE",
    ]);
    assert!(reply.ok(), "{reply:?}");
    let info = c.cmd(&["FT.INFO", "r1:cache"]);
    let attributes = info.get("attributes").arr();
    assert_eq!(attributes.len(), 6);
    let model = attributes[4].arr();
    assert_eq!(model[1].s(), "model");
    assert_eq!(model[5].s(), "TAG");
    assert_eq!((model[6].s(), model[7].s()), ("SEPARATOR", "|"));
}

/// Item 8: Say which option was refused, and for which field.
#[test]
fn report_8_schema_errors_name_the_problem() {
    let mut c = conn();
    let err = c.cmd(&["FT.CREATE", "r8:a", "SCHEMA", "k", "TAG", "WEIGHT", "1"]);
    assert_eq!(
        err.err(),
        "Unsupported option `WEIGHT` for TAG field `k`: it is an option of TEXT fields"
    );
    let err = c.cmd(&[
        "FT.CREATE",
        "r8:b",
        "SCHEMA",
        "n",
        "NUMERIC",
        "SEPARATOR",
        ",",
    ]);
    assert!(
        err.err().contains("`SEPARATOR` for NUMERIC field `n`"),
        "{err:?}"
    );
    let err = c.cmd(&["FT.CREATE", "r8:c", "SCHEMA", "f", "BLOB"]);
    assert_eq!(err.err(), "Invalid field type `BLOB` for field `f`");
    let err = c.cmd(&["FT.CREATE", "r8:d", "SCHEMA", "f"]);
    assert_eq!(err.err(), "Field `f` does not have a type");
    let err = c.cmd(&[
        "FT.CREATE",
        "r8:e",
        "ON",
        "HASH",
        "BOGUS",
        "SCHEMA",
        "f",
        "TEXT",
    ]);
    assert!(err.err().contains("Unknown argument `BOGUS`"), "{err:?}");
    let err = c.cmd(&["FT.CREATE", "r8:f", "ON", "HASH", "PREFIX", "1", "x:"]);
    assert_eq!(err.err(), "No schema found");
    // None of them left an index behind.
    for index in ["r8:a", "r8:b", "r8:c", "r8:d", "r8:e", "r8:f"] {
        assert!(c.cmd(&["FT.INFO", index]).err().contains("no such index"));
    }
    // An index that is there cannot be created again.
    assert!(c.cmd(&["FT.CREATE", "r8:ok", "SCHEMA", "f", "TEXT"]).ok());
    assert_eq!(
        c.cmd(&["FT.CREATE", "r8:ok", "SCHEMA", "f", "TEXT"]).err(),
        "Index already exists"
    );
}

fn cache_index(c: &mut Conn, index: &str, prefix: &str) {
    let reply = c.cmd(&[
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "prompt",
        "TEXT",
        "WEIGHT",
        "1",
        "response",
        "TEXT",
        "litellm_cache_key",
        "TAG",
        "SEPARATOR",
        ",",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "COSINE",
    ]);
    assert!(reply.ok(), "{reply:?}");
    for (id, prompt, response, key, v) in [
        ("1", "hello there", "world", "abc", [1.0f32, 0.0, 0.0]),
        ("2", "something other", "thing", "xyz", [0.0, 1.0, 0.0]),
        ("3", "hello again", "again", "abc", [0.9, 0.3, 0.0]),
    ] {
        hset_vector(
            c,
            &format!("{prefix}{id}"),
            &[
                ("prompt", prompt),
                ("response", response),
                ("litellm_cache_key", key),
            ],
            "v",
            &v,
        );
    }
}

/// Item 2: `VECTOR_RANGE` queries returned nothing, with no error.
#[test]
fn report_2_vector_range() {
    let mut c = conn();
    cache_index(&mut c, "r2", "r2:");
    let q = [0.99f32, 0.1, 0.0];

    // The stored vector is at cosine distance ≈ 0.005 from the query.
    let reply = search_with_vector(
        &mut c,
        "r2",
        "@v:[VECTOR_RANGE 0.2 $q]=>{$YIELD_DISTANCE_AS: d}",
        &q,
        &["RETURN", "1", "d"],
    );
    assert_eq!(reply.arr()[0].i(), 2, "{reply:?}");
    // Nearest first.
    assert_eq!(reply.ids(), ["r2:1", "r2:3"]);
    let nearest = reply.arr()[2].pairs();
    assert_eq!(nearest[0].0, "d");
    let distance: f64 = nearest[0].1.parse().unwrap();
    assert!((distance - 0.00506).abs() < 0.0005, "{distance}");

    // The radius as a parameter, a filter beside the range, SORTBY the
    // distance, LIMIT — the query RedisVL sends for a cache lookup.
    let blob = vector(&q);
    let reply = c.raw(&[
        b"FT.SEARCH",
        b"r2",
        b"(@v:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance} @litellm_cache_key:{abc})",
        b"RETURN", b"4", b"response", b"prompt", b"vector_distance", b"litellm_cache_key",
        b"SORTBY", b"vector_distance", b"ASC", b"DIALECT", b"2", b"LIMIT", b"0", b"1",
        b"PARAMS", b"4", b"vector", &blob, b"distance_threshold", b"0.2",
    ]);
    assert_eq!(reply.arr()[0].i(), 2, "{reply:?}");
    assert_eq!(reply.ids(), ["r2:1"]);
    let fields = reply.arr()[2].pairs();
    assert_eq!(fields[0], ("response".to_string(), "world".to_string()));
    assert_eq!(fields[1], ("prompt".to_string(), "hello there".to_string()));
    assert_eq!(fields[2].0, "vector_distance");
    assert_eq!(
        fields[3],
        ("litellm_cache_key".to_string(), "abc".to_string())
    );

    // A filter that leaves nothing in range, and a radius that reaches
    // nothing.
    let reply = search_with_vector(
        &mut c,
        "r2",
        "(@v:[VECTOR_RANGE 0.2 $q] @litellm_cache_key:{xyz})",
        &q,
        &[],
    );
    assert_eq!(reply.arr()[0].i(), 0);
    let reply = search_with_vector(&mut c, "r2", "@v:[VECTOR_RANGE 0.0001 $q]", &q, &[]);
    assert_eq!(reply.arr()[0].i(), 0);
    // And one that reaches everything.
    let reply = search_with_vector(&mut c, "r2", "@v:[VECTOR_RANGE 2 $q]", &q, &["NOCONTENT"]);
    assert_eq!(reply.arr()[0].i(), 3);

    // KNN finds the same document, at the same distance.
    let knn = search_with_vector(
        &mut c,
        "r2",
        "*=>[KNN 1 @v $q AS d]",
        &q,
        &["RETURN", "1", "d"],
    );
    assert_eq!(knn.ids(), ["r2:1"]);
    assert_eq!(knn.arr()[2].pairs()[0].1, nearest[0].1);

    // Vector syntax that is not understood is an error, not an empty result.
    let reply = search_with_vector(&mut c, "r2", "*=>[NEAR 1 @v $q]", &q, &[]);
    assert!(
        reply.err().contains("unsupported vector query"),
        "{reply:?}"
    );
    let reply = c.cmd(&["FT.SEARCH", "r2", "@v:[VECTOR_RANGE 0.2 $missing]"]);
    assert!(reply.err().contains("No such parameter"), "{reply:?}");
    let reply = c.cmd(&[
        "FT.SEARCH",
        "r2",
        "@v:[VECTOR_RANGE 0.2 $q]",
        "PARAMS",
        "2",
        "q",
        "short",
    ]);
    assert!(
        reply.err().contains("does not match index's expected size"),
        "{reply:?}"
    );
}

/// Item 3: A search without a vector clause returned no document fields.
#[test]
fn report_3_fields_of_a_non_vector_search() {
    let mut c = conn();
    cache_index(&mut c, "r3", "r3:");

    let reply = c.cmd(&[
        "FT.SEARCH",
        "r3",
        "@litellm_cache_key:{xyz}",
        "RETURN",
        "1",
        "prompt",
        "DIALECT",
        "2",
    ]);
    assert_eq!(reply.arr()[0].i(), 1);
    assert_eq!(reply.ids(), ["r3:2"]);
    assert_eq!(
        reply.arr()[2].pairs(),
        [("prompt".to_string(), "something other".to_string())]
    );

    // No RETURN: every field of the hash.
    let reply = c.cmd(&["FT.SEARCH", "r3", "@litellm_cache_key:{xyz}"]);
    let names: Vec<String> = reply.arr()[2]
        .arr()
        .chunks(2)
        .map(|p| p[0].s().to_string())
        .collect();
    assert_eq!(names, ["litellm_cache_key", "prompt", "response", "v"]);

    // NOCONTENT, and RETURN 0: ids only.
    for flag in [&["NOCONTENT"][..], &["RETURN", "0"]] {
        let mut args = vec!["FT.SEARCH", "r3", "*"];
        args.extend(flag);
        let reply = c.cmd(&args);
        assert_eq!(reply.arr().len(), 4, "{flag:?}: {reply:?}");
        assert_eq!(reply.id_set(), ["r3:1", "r3:2", "r3:3"]);
    }

    // WITHSCORES: a score between each id and its fields.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "r3",
        "hello",
        "WITHSCORES",
        "RETURN",
        "1",
        "prompt",
    ]);
    assert_eq!(reply.arr()[0].i(), 2);
    let items = reply.arr();
    assert_eq!(items.len(), 7);
    assert!(items[2].s().parse::<f64>().unwrap() > 0.0);
    assert_eq!(items[3].pairs()[0].0, "prompt");

    // RETURN … AS, and a field that is not there is left out.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "r3",
        "@litellm_cache_key:{xyz}",
        "RETURN",
        "4",
        "prompt",
        "AS",
        "p",
        "nosuch",
    ]);
    assert_eq!(
        reply.arr()[2].pairs(),
        [("p".to_string(), "something other".to_string())]
    );
}

fn tenant_index(c: &mut Conn, index: &str, prefix: &str) {
    let reply = c.cmd(&[
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "k",
        "TAG",
        "v",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert!(reply.ok(), "{reply:?}");
    hset_vector(
        c,
        &format!("{prefix}a"),
        &[("k", "tenantA")],
        "v",
        &[0.0, 0.0],
    );
    hset_vector(
        c,
        &format!("{prefix}b"),
        &[("k", "tenantB")],
        "v",
        &[1.0, 1.0],
    );
}

/// Item 4: Negation in a pre-filter was ignored: the excluded tenant's documents
/// came back.
#[test]
fn report_4_negation_in_a_prefilter() {
    let mut c = conn();
    tenant_index(&mut c, "r4", "r4:");
    let q = [0.1f32, 0.1];
    let reply = search_with_vector(
        &mut c,
        "r4",
        "(-@k:{tenantB})=>[KNN 5 @v $q AS d]",
        &q,
        &["NOCONTENT"],
    );
    assert_eq!(reply.id_set(), ["r4:a"]);
    let reply = search_with_vector(
        &mut c,
        "r4",
        "-@k:{tenantA}=>[KNN 5 @v $q AS d]",
        &q,
        &["NOCONTENT"],
    );
    assert_eq!(reply.id_set(), ["r4:b"]);
    // Without a vector clause too.
    assert_eq!(
        c.cmd(&["FT.SEARCH", "r4", "-@k:{tenantB}", "NOCONTENT"])
            .id_set(),
        ["r4:a"]
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "r4", "-@k:{nobody}", "NOCONTENT"])
            .id_set(),
        ["r4:a", "r4:b"]
    );

    // A filter that cannot be parsed is an error. It used to mean "everything".
    let reply = search_with_vector(&mut c, "r4", "(@k:{tenantA}=>[KNN 5 @v $q AS d]", &q, &[]);
    assert!(reply.err().starts_with("Syntax error"), "{reply:?}");
    let reply = search_with_vector(&mut c, "r4", "@nosuch:{x}=>[KNN 5 @v $q AS d]", &q, &[]);
    assert!(reply.err().starts_with("Unknown field"), "{reply:?}");
}

/// Item 5: OR across clauses returned nothing.
#[test]
fn report_5_or_across_clauses() {
    let mut c = conn();
    tenant_index(&mut c, "r5", "r5:");
    let q = [0.1f32, 0.1];
    for query in [
        "(@k:{tenantA}|@k:{nobody})=>[KNN 5 @v $q AS d]",
        "(@k:{tenantA} | @k:{nobody})=>[KNN 5 @v $q AS d]",
        "@k:{tenantA | nobody}=>[KNN 5 @v $q AS d]",
        "((@k:{nobody}) | (@k:{tenantA}))=>[KNN 5 @v $q AS d]",
    ] {
        let reply = search_with_vector(&mut c, "r5", query, &q, &["NOCONTENT"]);
        assert_eq!(reply.id_set(), ["r5:a"], "{query}");
    }
    let reply = search_with_vector(
        &mut c,
        "r5",
        "(@k:{tenantA}|@k:{tenantB})=>[KNN 5 @v $q AS d]",
        &q,
        &["NOCONTENT"],
    );
    // Both, nearest first.
    assert_eq!(reply.ids(), ["r5:a", "r5:b"]);
}

/// Item 6: Tag syntax without braces matched; on Redis it matches nothing.
#[test]
fn report_6_a_tag_field_is_queried_with_braces() {
    let mut c = conn();
    tenant_index(&mut c, "r6", "r6:");
    let q = [0.1f32, 0.1];
    let reply = search_with_vector(
        &mut c,
        "r6",
        "(@k:tenantA)=>[KNN 5 @v $q AS d]",
        &q,
        &["NOCONTENT"],
    );
    assert_eq!(reply.arr()[0].i(), 0, "{reply:?}");
    assert_eq!(c.cmd(&["FT.SEARCH", "r6", "@k:tenantA"]).arr()[0].i(), 0);
    assert_eq!(c.cmd(&["FT.SEARCH", "r6", "@k:{tenantA}"]).arr()[0].i(), 1);
}

/// Item 7: Under RESP3, FT.SEARCH (and FT.AGGREGATE, FT.INFO) answer with a map.
#[test]
fn report_7_resp3_replies_are_maps() {
    let mut c = conn();
    cache_index(&mut c, "r7", "r7:");
    let mut c3 = conn();
    assert!(matches!(c3.cmd(&["HELLO", "3"]), V::Map(_)));

    let reply = c3.cmd(&[
        "FT.SEARCH",
        "r7",
        "@litellm_cache_key:{xyz}",
        "RETURN",
        "1",
        "prompt",
    ]);
    assert_eq!(reply.get("total_results").i(), 1);
    assert_eq!(reply.get("format").s(), "STRING");
    assert_eq!(reply.get("attributes").arr().len(), 0);
    assert_eq!(reply.get("warning").arr().len(), 0);
    let results = reply.get("results").arr();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").s(), "r7:2");
    assert_eq!(
        results[0].get("extra_attributes").get("prompt").s(),
        "something other"
    );
    assert_eq!(results[0].get("values").arr().len(), 0);

    // WITHSCORES adds the score; NOCONTENT takes the fields away.
    let reply = c3.cmd(&["FT.SEARCH", "r7", "hello", "WITHSCORES", "NOCONTENT"]);
    assert_eq!(reply.get("total_results").i(), 2);
    for result in reply.get("results").arr() {
        assert!(matches!(result.get("score"), V::Double(score) if *score > 0.0));
        assert!(matches!(result, V::Map(pairs) if pairs.len() == 3));
    }

    let reply = c3.cmd(&[
        "FT.AGGREGATE",
        "r7",
        "*",
        "LOAD",
        "1",
        "@prompt",
        "SORTBY",
        "1",
        "@prompt",
    ]);
    assert_eq!(reply.get("total_results").i(), 3);
    let rows = reply.get("results").arr();
    assert_eq!(
        rows[0].get("extra_attributes").get("prompt").s(),
        "hello again"
    );

    let info = c3.cmd(&["FT.INFO", "r7"]);
    assert_eq!(info.get("index_name").s(), "r7");
    assert_eq!(info.get("num_docs").i(), 3);
    assert_eq!(info.get("index_definition").get("key_type").s(), "HASH");

    // And the same connection under RESP2 gets arrays.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "r7",
        "@litellm_cache_key:{xyz}",
        "RETURN",
        "1",
        "prompt",
    ]);
    assert!(matches!(reply, V::Arr(_)));
}

// ══ The rest of the query language and of FT.SEARCH ═══════════════════════════

fn catalogue(c: &mut Conn, index: &str, prefix: &str) {
    let reply = c.cmd(&[
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "title",
        "TEXT",
        "WEIGHT",
        "5",
        "body",
        "TEXT",
        "colour",
        "TAG",
        "SEPARATOR",
        ";",
        "sku",
        "TAG",
        "CASESENSITIVE",
        "price",
        "NUMERIC",
        "SORTABLE",
        "name",
        "AS",
        "label",
        "TEXT",
    ]);
    assert!(reply.ok(), "{reply:?}");
    for (id, title, body, colour, sku, price, name) in [
        (
            "1",
            "Red running shoes",
            "light and fast, for the road",
            "red;white",
            "Ab-1",
            "50",
            "Sprint",
        ),
        (
            "2",
            "Blue running shorts",
            "quick-dry fabric",
            "blue",
            "ab-2",
            "20",
            "Dash",
        ),
        (
            "3",
            "Red rain jacket",
            "keeps the rain out; packs small",
            "red;dark green",
            "Cd-3",
            "90",
            "Storm",
        ),
        (
            "4",
            "Garden hose",
            "wi-fi enabled, somehow",
            "dark green",
            "Ef-4",
            "15",
            "Sprout",
        ),
    ] {
        let key = format!("{prefix}{id}");
        let reply = c.cmd(&[
            "HSET", &key, "title", title, "body", body, "colour", colour, "sku", sku, "price",
            price, "name", name,
        ]);
        assert!(matches!(reply, V::Int(_)));
    }
    // One document without a price.
    let key = format!("{prefix}5");
    c.cmd(&[
        "HSET",
        &key,
        "title",
        "Mystery box",
        "body",
        "who knows",
        "colour",
        "red",
    ]);
}

#[test]
fn words_phrases_and_wildcards() {
    let mut c = conn();
    catalogue(&mut c, "q1", "q1:");
    let ids = |c: &mut Conn, query: &str| {
        let reply = c.cmd(&["FT.SEARCH", "q1", query, "NOCONTENT", "LIMIT", "0", "50"]);
        assert!(matches!(reply, V::Arr(_)), "{query}: {reply:?}");
        reply.id_set()
    };
    assert_eq!(ids(&mut c, "running"), ["q1:1", "q1:2"]);
    assert_eq!(ids(&mut c, "red running"), ["q1:1"]);
    assert_eq!(ids(&mut c, "red | blue"), ["q1:1", "q1:2", "q1:3"]);
    assert_eq!(ids(&mut c, "red -running"), ["q1:3"]);
    assert_eq!(ids(&mut c, "(red | blue) running"), ["q1:1", "q1:2"]);
    assert_eq!(ids(&mut c, "\"running shoes\""), ["q1:1"]);
    assert!(ids(&mut c, "\"shoes running\"").is_empty());
    assert_eq!(ids(&mut c, "run*"), ["q1:1", "q1:2"]);
    assert_eq!(ids(&mut c, "*cket"), ["q1:3"]);
    assert_eq!(ids(&mut c, "*unn*"), ["q1:1", "q1:2"]);
    assert_eq!(ids(&mut c, "%jaket%"), ["q1:3"]);
    // In one field, and in either of two.
    assert_eq!(ids(&mut c, "@title:rain"), ["q1:3"]);
    assert_eq!(ids(&mut c, "@body:rain"), ["q1:3"]);
    assert_eq!(ids(&mut c, "@body:fast"), ["q1:1"]);
    assert!(ids(&mut c, "@title:fast").is_empty());
    assert_eq!(ids(&mut c, "@title|body:(fast | hose)"), ["q1:1", "q1:4"]);
    // A field under the name it was given with AS.
    assert_eq!(ids(&mut c, "@label:storm"), ["q1:3"]);
    assert!(
        c.cmd(&["FT.SEARCH", "q1", "@name:storm"])
            .err()
            .starts_with("Unknown field")
    );
    // Punctuation separates words — in the document and in the query.
    assert_eq!(ids(&mut c, "dry"), ["q1:2"]);
    assert_eq!(ids(&mut c, "wi-fi"), ["q1:4"]);
    assert_eq!(ids(&mut c, "fi"), ["q1:4"]);
    // Stopwords restrict nothing, and alone match nothing.
    assert_eq!(ids(&mut c, "the road"), ["q1:1"]);
    assert!(ids(&mut c, "the").is_empty());
    // Everything, and everything but.
    assert_eq!(ids(&mut c, "*").len(), 5);
    assert_eq!(ids(&mut c, "-red").len(), 3);
}

#[test]
fn tags_and_numbers() {
    let mut c = conn();
    catalogue(&mut c, "q2", "q2:");
    let ids = |c: &mut Conn, query: &str| {
        let reply = c.cmd(&["FT.SEARCH", "q2", query, "NOCONTENT", "LIMIT", "0", "50"]);
        assert!(matches!(reply, V::Arr(_)), "{query}: {reply:?}");
        reply.id_set()
    };
    // The field's separator is `;`, so `dark green` is one tag.
    assert_eq!(ids(&mut c, "@colour:{red}"), ["q2:1", "q2:3", "q2:5"]);
    assert_eq!(ids(&mut c, "@colour:{dark green}"), ["q2:3", "q2:4"]);
    assert_eq!(ids(&mut c, "@colour:{dark\\ green}"), ["q2:3", "q2:4"]);
    assert!(ids(&mut c, "@colour:{green}").is_empty());
    assert_eq!(ids(&mut c, "@colour:{white | blue}"), ["q2:1", "q2:2"]);
    assert_eq!(ids(&mut c, "@colour:{RED} @colour:{White}"), ["q2:1"]);
    assert_eq!(ids(&mut c, "@colour:{dark*}"), ["q2:3", "q2:4"]);
    // CASESENSITIVE
    assert_eq!(ids(&mut c, "@sku:{Ab\\-1}"), ["q2:1"]);
    assert!(ids(&mut c, "@sku:{ab\\-1}").is_empty());
    assert_eq!(ids(&mut c, "@sku:{ab\\-2}"), ["q2:2"]);

    assert_eq!(ids(&mut c, "@price:[20 50]"), ["q2:1", "q2:2"]);
    assert_eq!(ids(&mut c, "@price:[(20 50]"), ["q2:1"]);
    assert_eq!(ids(&mut c, "@price:[-inf (20]"), ["q2:4"]);
    assert_eq!(ids(&mut c, "@price:[90 +inf]"), ["q2:3"]);
    assert_eq!(ids(&mut c, "@price:[50]"), ["q2:1"]);
    assert_eq!(ids(&mut c, "@price==50"), ["q2:1"]);
    assert_eq!(ids(&mut c, "@price>=50"), ["q2:1", "q2:3"]);
    assert_eq!(ids(&mut c, "@price>50"), ["q2:3"]);
    assert_eq!(ids(&mut c, "@price<20"), ["q2:4"]);
    assert_eq!(ids(&mut c, "@price!=50"), ["q2:2", "q2:3", "q2:4"]);
    assert_eq!(ids(&mut c, "@price>15 @price<90 @colour:{red}"), ["q2:1"]);
    assert_eq!(
        ids(&mut c, "@colour:{red} -@price:[0 60]"),
        ["q2:3", "q2:5"]
    );

    // Parameters.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q2",
        "@colour:{$c} @price:[$lo $hi]",
        "NOCONTENT",
        "PARAMS",
        "6",
        "c",
        "red",
        "lo",
        "40",
        "hi",
        "60",
        "DIALECT",
        "2",
    ]);
    assert_eq!(reply.id_set(), ["q2:1"]);

    // FT.TAGVALS: the field's separator, the field's case rule.
    let values: Vec<String> = c
        .cmd(&["FT.TAGVALS", "q2", "colour"])
        .arr()
        .iter()
        .map(|v| v.s().to_string())
        .collect();
    assert_eq!(values, ["blue", "dark green", "red", "white"]);
    let values: Vec<String> = c
        .cmd(&["FT.TAGVALS", "q2", "sku"])
        .arr()
        .iter()
        .map(|v| v.s().to_string())
        .collect();
    assert_eq!(values, ["Ab-1", "Cd-3", "Ef-4", "ab-2"]);
}

#[test]
fn sorting_paging_and_what_is_returned() {
    let mut c = conn();
    catalogue(&mut c, "q3", "q3:");

    let reply = c.cmd(&["FT.SEARCH", "q3", "*", "SORTBY", "price", "NOCONTENT"]);
    // The document without a price comes last, whichever way round.
    assert_eq!(reply.ids(), ["q3:4", "q3:2", "q3:1", "q3:3", "q3:5"]);
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q3",
        "*",
        "SORTBY",
        "price",
        "DESC",
        "NOCONTENT",
    ]);
    assert_eq!(reply.ids(), ["q3:3", "q3:1", "q3:2", "q3:4", "q3:5"]);
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q3",
        "*",
        "SORTBY",
        "title",
        "ASC",
        "NOCONTENT",
    ]);
    assert_eq!(reply.ids(), ["q3:2", "q3:4", "q3:5", "q3:3", "q3:1"]);

    // LIMIT pages through the sorted set; the count is of all matches.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q3",
        "*",
        "SORTBY",
        "price",
        "LIMIT",
        "1",
        "2",
        "NOCONTENT",
    ]);
    assert_eq!(reply.arr()[0].i(), 5);
    assert_eq!(reply.ids(), ["q3:2", "q3:1"]);
    let reply = c.cmd(&["FT.SEARCH", "q3", "*", "LIMIT", "0", "0"]);
    assert_eq!(reply.arr().len(), 1);
    assert_eq!(reply.arr()[0].i(), 5);

    // WITHSORTKEYS
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q3",
        "@colour:{blue}",
        "SORTBY",
        "price",
        "WITHSORTKEYS",
        "NOCONTENT",
    ]);
    assert_eq!(reply.arr()[2].s(), "#20");

    // FILTER and INKEYS narrow the result.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q3",
        "*",
        "FILTER",
        "price",
        "20",
        "(90",
        "NOCONTENT",
    ]);
    assert_eq!(reply.id_set(), ["q3:1", "q3:2"]);
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q3",
        "red",
        "INKEYS",
        "2",
        "q3:3",
        "q3:4",
        "NOCONTENT",
    ]);
    assert_eq!(reply.id_set(), ["q3:3"]);

    // A match in a heavier field ranks first: `title` has WEIGHT 5.
    c.cmd(&["HSET", "q3:6", "title", "plain", "body", "hose hose hose"]);
    let reply = c.cmd(&["FT.SEARCH", "q3", "hose", "NOCONTENT"]);
    assert_eq!(reply.ids(), ["q3:4", "q3:6"]);

    // HIGHLIGHT marks the words that matched.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "q3",
        "rain",
        "RETURN",
        "2",
        "title",
        "body",
        "HIGHLIGHT",
        "TAGS",
        "[",
        "]",
    ]);
    let fields = reply.arr()[2].pairs();
    assert_eq!(fields[0].1, "Red [rain] jacket");
    assert_eq!(fields[1].1, "keeps the [rain] out; packs small");

    // Arguments that are not understood are refused.
    assert!(
        c.cmd(&["FT.SEARCH", "q3", "*", "BOGUS"])
            .err()
            .contains("Unknown argument `BOGUS`")
    );
    assert!(
        c.cmd(&["FT.SEARCH", "q3", "*", "LIMIT", "0"])
            .is_err_reply()
    );
    assert!(
        c.cmd(&["FT.SEARCH", "q3", "*", "DIALECT", "7"])
            .is_err_reply()
    );
    assert!(
        c.cmd(&["FT.SEARCH", "nosuch", "*"])
            .err()
            .contains("no such index")
    );
}

impl V {
    fn is_err_reply(&self) -> bool {
        matches!(self, V::Err(_))
    }
}

/// `STOPWORDS 0`: an index with no stopwords at all.
#[test]
fn an_index_can_have_its_own_stopwords() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "q4",
            "PREFIX",
            "1",
            "q4:",
            "STOPWORDS",
            "0",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .ok()
    );
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "q4b",
            "PREFIX",
            "1",
            "q4:",
            "STOPWORDS",
            "2",
            "quick",
            "brown",
            "SCHEMA",
            "t",
            "TEXT",
        ])
        .ok()
    );
    c.cmd(&["HSET", "q4:1", "t", "the quick brown fox"]);
    assert_eq!(
        c.cmd(&["FT.SEARCH", "q4", "the", "NOCONTENT"]).arr()[0].i(),
        1
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "q4b", "the", "NOCONTENT"]).arr()[0].i(),
        1
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "q4b", "quick", "NOCONTENT"]).arr()[0].i(),
        0
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "q4b", "quick fox", "NOCONTENT"]).arr()[0].i(),
        1
    );
    let info = c.cmd(&["FT.INFO", "q4b"]);
    assert_eq!(info.get("stopwords_list").arr().len(), 2);
}

/// A TEXT field added to an index that already has documents is searchable
/// for those documents too.
#[test]
fn a_field_added_with_alter_covers_existing_documents() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "q5",
            "PREFIX",
            "1",
            "q5:",
            "SCHEMA",
            "a",
            "TEXT"
        ])
        .ok()
    );
    c.cmd(&["HSET", "q5:1", "a", "first", "b", "second", "n", "7"]);
    assert!(
        c.cmd(&["FT.SEARCH", "q5", "@b:second"])
            .err()
            .starts_with("Unknown field")
    );
    assert!(
        c.cmd(&[
            "FT.ALTER", "q5", "SCHEMA", "ADD", "b", "TEXT", "n", "NUMERIC", "SORTABLE"
        ])
        .ok()
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "q5", "@b:second", "NOCONTENT"]).ids(),
        ["q5:1"]
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "q5", "second", "NOCONTENT"]).ids(),
        ["q5:1"]
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "q5", "@n:[7 7]", "NOCONTENT"]).ids(),
        ["q5:1"]
    );
    assert!(
        c.cmd(&["FT.ALTER", "q5", "SCHEMA", "ADD", "b", "TEXT"])
            .err()
            .contains("Duplicate field")
    );
}

/// FT.INFO in Redis's shape: what RedisVL reads an index's schema back from.
#[test]
fn info_describes_the_schema() {
    let mut c = conn();
    catalogue(&mut c, "q6", "q6:");
    let info = c.cmd(&["FT.INFO", "q6"]);
    assert_eq!(info.get("index_name").s(), "q6");
    let definition = info.get("index_definition");
    assert_eq!(definition.get("key_type").s(), "HASH");
    assert_eq!(definition.get("prefixes").arr()[0].s(), "q6:");
    assert_eq!(info.get("num_docs").i(), 5);

    let attributes: Vec<Vec<String>> = info
        .get("attributes")
        .arr()
        .iter()
        .map(|a| {
            a.arr()
                .iter()
                .map(|v| match v {
                    V::Int(n) => n.to_string(),
                    other => other.s().to_string(),
                })
                .collect()
        })
        .collect();
    assert_eq!(
        attributes[0],
        [
            "identifier",
            "title",
            "attribute",
            "title",
            "type",
            "TEXT",
            "WEIGHT",
            "5"
        ]
    );
    assert_eq!(
        attributes[2],
        [
            "identifier",
            "colour",
            "attribute",
            "colour",
            "type",
            "TAG",
            "SEPARATOR",
            ";"
        ]
    );
    assert_eq!(
        attributes[3],
        [
            "identifier",
            "sku",
            "attribute",
            "sku",
            "type",
            "TAG",
            "SEPARATOR",
            ",",
            "CASESENSITIVE"
        ]
    );
    assert_eq!(
        attributes[4],
        [
            "identifier",
            "price",
            "attribute",
            "price",
            "type",
            "NUMERIC",
            "SORTABLE"
        ]
    );
    assert_eq!(
        attributes[5],
        [
            "identifier",
            "name",
            "attribute",
            "label",
            "type",
            "TEXT",
            "WEIGHT",
            "1"
        ]
    );

    assert!(
        c.cmd(&[
            "FT.CREATE",
            "q6v",
            "SCHEMA",
            "v",
            "VECTOR",
            "HNSW",
            "10",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "IP",
            "M",
            "8",
            "EF_CONSTRUCTION",
            "100",
        ])
        .ok()
    );
    let info = c.cmd(&["FT.INFO", "q6v"]);
    let v = &info.get("attributes").arr()[0];
    assert_eq!(v.get("type").s(), "VECTOR");
    assert_eq!(v.get("algorithm").s(), "HNSW");
    assert_eq!(v.get("data_type").s(), "FLOAT32");
    assert_eq!(v.get("dim").i(), 4);
    assert_eq!(v.get("distance_metric").s(), "IP");
}

/// FT.AGGREGATE shows what it was asked for.
#[test]
fn aggregate_rows_show_what_was_loaded() {
    let mut c = conn();
    catalogue(&mut c, "q7", "q7:");
    let reply = c.cmd(&["FT.AGGREGATE", "q7", "@colour:{blue}"]);
    assert_eq!(reply.arr()[0].i(), 1);
    assert!(reply.arr()[1].arr().is_empty());

    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "q7",
        "@colour:{blue}",
        "LOAD",
        "2",
        "@title",
        "@price",
    ]);
    assert_eq!(
        reply.arr()[1].pairs(),
        [
            ("title".to_string(), "Blue running shorts".to_string()),
            ("price".to_string(), "20".to_string())
        ]
    );
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "q7",
        "@colour:{blue}",
        "LOAD",
        "3",
        "@price",
        "AS",
        "cost",
        "APPLY",
        "@price * 2",
        "AS",
        "double",
    ]);
    assert_eq!(
        reply.arr()[1].pairs(),
        [
            ("cost".to_string(), "20".to_string()),
            ("double".to_string(), "40".to_string())
        ]
    );
    let reply = c.cmd(&["FT.AGGREGATE", "q7", "@colour:{blue}", "LOAD", "*"]);
    let names: Vec<String> = reply.arr()[1]
        .arr()
        .chunks(2)
        .map(|p| p[0].s().to_string())
        .collect();
    assert_eq!(names, ["body", "colour", "name", "price", "sku", "title"]);

    // After GROUPBY a row is the group.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "q7",
        "-@colour:{blue}",
        "GROUPBY",
        "1",
        "@colour",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "n",
        "SORTBY",
        "2",
        "@n",
        "DESC",
    ]);
    // Grouped by the field's value as stored: four different ones.
    assert_eq!(reply.arr()[0].i(), 4);
    for row in &reply.arr()[1..] {
        let names: Vec<String> = row.pairs().into_iter().map(|(k, _)| k).collect();
        assert_eq!(names, ["colour", "n"]);
    }

    // A query that cannot be parsed is an error here too.
    assert!(
        c.cmd(&["FT.AGGREGATE", "q7", "(@colour:{blue}"])
            .err()
            .starts_with("Syntax error")
    );
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "q7",
        "@colour:{$c}",
        "LOAD",
        "1",
        "@title",
        "PARAMS",
        "2",
        "c",
        "blue",
        "DIALECT",
        "2",
    ]);
    assert_eq!(reply.arr()[0].i(), 1);
}

/// KNN: the default name of the distance, and options.
#[test]
fn knn_options() {
    let mut c = conn();
    tenant_index(&mut c, "q8", "q8:");
    let q = [0.1f32, 0.1];
    let reply = search_with_vector(&mut c, "q8", "*=>[KNN 2 @v $q]", &q, &[]);
    assert_eq!(reply.ids(), ["q8:a", "q8:b"]);
    let fields = reply.arr()[2].pairs();
    assert_eq!(fields[0].0, "__v_score");
    assert!((fields[0].1.parse::<f64>().unwrap() - 0.02).abs() < 1e-6);

    let reply = search_with_vector(
        &mut c,
        "q8",
        "*=>[KNN 2 @v $q AS dist EF_RUNTIME 20]",
        &q,
        &["SORTBY", "dist", "DESC", "RETURN", "1", "dist"],
    );
    assert_eq!(reply.ids(), ["q8:b", "q8:a"]);

    // K as a parameter.
    let blob = vector(&q);
    let reply = c.raw(&[
        b"FT.SEARCH",
        b"q8",
        b"*=>[KNN $k @v $q AS d]",
        b"NOCONTENT",
        b"PARAMS",
        b"4",
        b"k",
        b"1",
        b"q",
        &blob,
        b"DIALECT",
        b"2",
    ]);
    assert_eq!(reply.ids(), ["q8:a"]);
}

// ══ Indexes on JSON documents ═════════════════════════════════════════════════

fn json_catalogue(c: &mut Conn, index: &str, prefix: &str) {
    let reply = c.cmd(&[
        "FT.CREATE",
        index,
        "ON",
        "JSON",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "$.title",
        "AS",
        "title",
        "TEXT",
        "$.colours[*]",
        "AS",
        "colour",
        "TAG",
        "$.price",
        "AS",
        "price",
        "NUMERIC",
        "SORTABLE",
        "$.stock.available",
        "AS",
        "available",
        "TAG",
        "$.embedding",
        "AS",
        "embedding",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        "3",
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert!(reply.ok(), "{reply:?}");
    for (id, doc) in [
        (
            "1",
            r#"{"title":"Red running shoes","colours":["red","white"],"price":80,"stock":{"available":true},"embedding":[1.0,0.0,0.0]}"#,
        ),
        (
            "2",
            r#"{"title":"Blue running shorts","colours":["blue"],"price":20,"stock":{"available":true},"embedding":[0.0,1.0,0.0]}"#,
        ),
        (
            "3",
            r#"{"title":"Green garden hose","colours":["green"],"price":35.5,"stock":{"available":false},"embedding":[0.0,0.0,1.0]}"#,
        ),
    ] {
        let reply = c.cmd(&["JSON.SET", &format!("{prefix}{id}"), "$", doc]);
        assert!(reply.ok(), "{reply:?}");
    }
}

/// `FT.CREATE … ON JSON` used to make an index that indexed nothing: every
/// query of it came back empty.
#[test]
fn a_json_index_finds_documents_by_their_paths() {
    let mut c = conn();
    json_catalogue(&mut c, "j1", "j1:");

    // Words of a TEXT path.
    let reply = c.cmd(&["FT.SEARCH", "j1", "@title:running", "NOCONTENT"]);
    assert_eq!(reply.id_set(), ["j1:1", "j1:2"]);
    assert_eq!(
        c.cmd(&["FT.SEARCH", "j1", "garden", "NOCONTENT"]).ids(),
        ["j1:3"]
    );
    // An array of strings under a TAG path: each is a tag.
    assert_eq!(
        c.cmd(&["FT.SEARCH", "j1", "@colour:{white}", "NOCONTENT"])
            .ids(),
        ["j1:1"]
    );
    assert_eq!(
        c.cmd(&["FT.SEARCH", "j1", "@colour:{red | blue}", "NOCONTENT"])
            .id_set(),
        ["j1:1", "j1:2"]
    );
    // A boolean under a TAG path.
    assert_eq!(
        c.cmd(&["FT.SEARCH", "j1", "@available:{false}", "NOCONTENT"])
            .ids(),
        ["j1:3"]
    );
    // Numbers, and the two together.
    assert_eq!(
        c.cmd(&["FT.SEARCH", "j1", "@price:[30 100]", "NOCONTENT"])
            .id_set(),
        ["j1:1", "j1:3"]
    );
    assert_eq!(
        c.cmd(&[
            "FT.SEARCH",
            "j1",
            "@title:running @price:[-inf 50]",
            "NOCONTENT"
        ])
        .ids(),
        ["j1:2"]
    );
    assert_eq!(
        c.cmd(&[
            "FT.SEARCH",
            "j1",
            "*",
            "SORTBY",
            "price",
            "DESC",
            "NOCONTENT"
        ])
        .ids(),
        ["j1:1", "j1:3", "j1:2"]
    );

    // A hash under the same prefix is not a document of this index.
    assert!(matches!(
        c.cmd(&["HSET", "j1:hash", "title", "running hash"]),
        V::Int(1)
    ));
    assert_eq!(
        c.cmd(&["FT.SEARCH", "j1", "@title:running", "NOCONTENT"])
            .id_set(),
        ["j1:1", "j1:2"]
    );
    let info = c.cmd(&["FT.INFO", "j1"]);
    assert_eq!(info.get("num_docs").i(), 3);
    assert_eq!(info.get("index_definition").get("key_type").s(), "JSON");
}

/// A JSON document is returned whole under `$`; `RETURN` picks paths out of
/// it, by the names the schema gives them or by path.
#[test]
fn what_a_json_index_returns() {
    let mut c = conn();
    json_catalogue(&mut c, "j2", "j2:");

    let reply = c.cmd(&["FT.SEARCH", "j2", "@colour:{blue}"]);
    assert_eq!(reply.arr()[0].i(), 1);
    assert_eq!(reply.arr()[1].s(), "j2:2");
    let fields = reply.arr()[2].pairs();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].0, "$");
    let doc: serde_json::Value = serde_json::from_str(&fields[0].1).unwrap();
    assert_eq!(doc["title"], "Blue running shorts");
    assert_eq!(doc["embedding"], serde_json::json!([0.0, 1.0, 0.0]));

    let reply = c.cmd(&[
        "FT.SEARCH",
        "j2",
        "@colour:{blue}",
        "RETURN",
        "7",
        "title",
        "price",
        "$.stock",
        "AS",
        "stock",
        "$.colours",
        "$.nothing",
    ]);
    assert_eq!(
        reply.arr()[2].pairs(),
        [
            ("title".to_string(), "Blue running shorts".to_string()),
            ("price".to_string(), "20".to_string()),
            ("stock".to_string(), r#"{"available":true}"#.to_string()),
            ("$.colours".to_string(), r#"["blue"]"#.to_string()),
        ]
    );

    // Rows of an aggregation go by the schema's names too.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "j2",
        "@title:running",
        "LOAD",
        "2",
        "@title",
        "@price",
        "SORTBY",
        "2",
        "@price",
        "ASC",
    ]);
    assert_eq!(reply.arr()[0].i(), 2);
    assert_eq!(
        reply.arr()[1].pairs(),
        [
            ("title".to_string(), "Blue running shorts".to_string()),
            ("price".to_string(), "20".to_string())
        ]
    );
    assert_eq!(
        c.cmd(&["FT.TAGVALS", "j2", "colour"])
            .arr()
            .iter()
            .map(|v| v.s().to_string())
            .collect::<Vec<_>>(),
        ["blue", "green", "red", "white"]
    );
}

/// Vectors in a JSON document are arrays of numbers; a KNN query finds them,
/// with a filter or without.
#[test]
fn a_json_index_answers_vector_queries() {
    let mut c = conn();
    json_catalogue(&mut c, "j3", "j3:");

    let reply = search_with_vector(
        &mut c,
        "j3",
        "*=>[KNN 2 @embedding $q AS distance]",
        &[0.9, 0.1, 0.0],
        &["RETURN", "2", "distance", "title"],
    );
    assert_eq!(reply.arr()[0].i(), 2);
    assert_eq!(reply.ids(), ["j3:1", "j3:2"]);
    let nearest = reply.arr()[2].pairs();
    assert_eq!(
        nearest[1],
        ("title".to_string(), "Red running shoes".to_string())
    );
    assert!(nearest[0].1.parse::<f32>().unwrap() < 0.1, "{nearest:?}");

    let reply = search_with_vector(
        &mut c,
        "j3",
        "(@price:[0 50])=>[KNN 2 @embedding $q AS distance]",
        &[0.9, 0.1, 0.0],
        &["NOCONTENT"],
    );
    assert_eq!(reply.ids(), ["j3:2", "j3:3"]);

    let reply = search_with_vector(
        &mut c,
        "j3",
        "@embedding:[VECTOR_RANGE 0.5 $q]",
        &[0.0, 0.0, 1.0],
        &["NOCONTENT"],
    );
    assert_eq!(reply.ids(), ["j3:3"]);
}

/// The index follows the documents: one changed by any JSON command, one
/// deleted, and the ones that were there before the index was.
#[test]
fn a_json_index_follows_its_documents() {
    let mut c = conn();
    // Documents first, the index after.
    for (id, doc) in [
        ("1", r#"{"title":"first draft","price":1,"tags":["a"]}"#),
        ("2", r#"{"title":"second draft","price":2,"tags":["b"]}"#),
    ] {
        assert!(c.cmd(&["JSON.SET", &format!("j4:{id}"), "$", doc]).ok());
    }
    let reply = c.cmd(&[
        "FT.CREATE",
        "j4",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "j4:",
        "SCHEMA",
        "$.title",
        "AS",
        "title",
        "TEXT",
        "$.price",
        "AS",
        "price",
        "NUMERIC",
        "$.tags[*]",
        "AS",
        "tag",
        "TAG",
    ]);
    assert!(reply.ok(), "{reply:?}");
    let ids = |c: &mut Conn, query: &str| c.cmd(&["FT.SEARCH", "j4", query, "NOCONTENT"]).id_set();
    assert_eq!(ids(&mut c, "@title:draft"), ["j4:1", "j4:2"]);

    // One path of a document is set.
    assert!(
        c.cmd(&["JSON.SET", "j4:1", "$.title", r#""final copy""#])
            .ok()
    );
    assert_eq!(ids(&mut c, "@title:draft"), ["j4:2"]);
    assert_eq!(ids(&mut c, "@title:final"), ["j4:1"]);

    // Changed in place by the commands that do that.
    assert_eq!(
        c.cmd(&["JSON.NUMINCRBY", "j4:2", "$.price", "40"]).s(),
        "[42]"
    );
    assert_eq!(ids(&mut c, "@price:[40 50]"), ["j4:2"]);
    c.cmd(&["JSON.ARRAPPEND", "j4:2", "$.tags", r#""urgent""#]);
    assert_eq!(ids(&mut c, "@tag:{urgent}"), ["j4:2"]);
    assert!(
        c.cmd(&["JSON.MERGE", "j4:1", "$", r#"{"tags":["urgent","c"]}"#])
            .ok()
    );
    assert_eq!(ids(&mut c, "@tag:{urgent}"), ["j4:1", "j4:2"]);

    // A path deleted, and a document deleted.
    assert_eq!(c.cmd(&["JSON.DEL", "j4:1", "$.tags"]).i(), 1);
    assert_eq!(ids(&mut c, "@tag:{urgent}"), ["j4:2"]);
    assert_eq!(c.cmd(&["DEL", "j4:2"]).i(), 1);
    assert_eq!(ids(&mut c, "*"), ["j4:1"]);
    assert!(ids(&mut c, "@tag:{urgent}").is_empty());

    // A new one.
    assert!(
        c.cmd(&[
            "JSON.MSET",
            "j4:3",
            "$",
            r#"{"title":"third draft","price":3,"tags":[]}"#
        ])
        .ok()
    );
    assert_eq!(ids(&mut c, "@title:draft"), ["j4:3"]);
}

// ══ Aggregations: reducers, COLLECT, cursors ══════════════════════════════════

fn movies(c: &mut Conn, index: &str, prefix: &str) {
    let reply = c.cmd(&[
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "title",
        "TEXT",
        "genre",
        "TAG",
        "rating",
        "NUMERIC",
        "SORTABLE",
        "year",
        "NUMERIC",
    ]);
    assert!(reply.ok(), "{reply:?}");
    for (id, title, genre, rating, year) in [
        ("1", "Alien", "scifi", "8.5", "1979"),
        ("2", "Arrival", "scifi", "7.9", "2016"),
        ("3", "Solaris", "scifi", "8.0", "1972"),
        ("4", "Heat", "crime", "8.3", "1995"),
        ("5", "Fargo", "crime", "8.1", "1996"),
        ("6", "Airplane", "comedy", "7.7", "1980"),
    ] {
        c.cmd(&[
            "HSET",
            &format!("{prefix}{id}"),
            "title",
            title,
            "genre",
            genre,
            "rating",
            rating,
            "year",
            year,
        ]);
    }
}

/// The rows of an aggregation, each as `(name, value)` pairs — a value being
/// whatever the reply holds: a string or something nested.
fn rows(reply: &V) -> Vec<Vec<(String, V)>> {
    reply.arr()[1..]
        .iter()
        .map(|row| {
            row.arr()
                .chunks(2)
                .map(|pair| (pair[0].s().to_string(), pair[1].clone()))
                .collect()
        })
        .collect()
}

fn strings(words: &[&str]) -> V {
    V::Arr(words.iter().map(|w| V::Str(w.to_string())).collect())
}

/// Redis 8.8: `REDUCE COLLECT` returns the documents of each group — the
/// fields asked for, sorted and cut within the group.
#[test]
fn collect_returns_the_documents_of_each_group() {
    let mut c = conn();
    movies(&mut c, "a1", "a1:");
    // The example of the FT.AGGREGATE page, on six films.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a1",
        "*",
        "LOAD",
        "4",
        "@genre",
        "@title",
        "@rating",
        "@year",
        "GROUPBY",
        "1",
        "@genre",
        "REDUCE",
        "COLLECT",
        "12",
        "FIELDS",
        "3",
        "@title",
        "@rating",
        "@year",
        "SORTBY",
        "2",
        "@rating",
        "DESC",
        "LIMIT",
        "0",
        "2",
        "AS",
        "top",
        "SORTBY",
        "1",
        "@genre",
    ]);
    assert_eq!(reply.arr()[0].i(), 3);
    let entry = |title: &str, rating: &str, year: &str| {
        strings(&["title", title, "rating", rating, "year", year])
    };
    assert_eq!(
        rows(&reply),
        [
            vec![
                ("genre".to_string(), V::Str("comedy".into())),
                (
                    "top".to_string(),
                    V::Arr(vec![entry("Airplane", "7.7", "1980")])
                ),
            ],
            vec![
                ("genre".to_string(), V::Str("crime".into())),
                (
                    "top".to_string(),
                    V::Arr(vec![
                        entry("Heat", "8.3", "1995"),
                        entry("Fargo", "8.1", "1996")
                    ])
                ),
            ],
            vec![
                ("genre".to_string(), V::Str("scifi".into())),
                (
                    "top".to_string(),
                    V::Arr(vec![
                        entry("Alien", "8.5", "1979"),
                        entry("Solaris", "8.0", "1972")
                    ])
                ),
            ],
        ]
    );

    // DISTINCT: one entry for each distinct set of values.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a1",
        "*",
        "LOAD",
        "1",
        "@genre",
        "GROUPBY",
        "0",
        "REDUCE",
        "COLLECT",
        "7",
        "FIELDS",
        "1",
        "@genre",
        "SORTBY",
        "1",
        "@genre",
        "DISTINCT",
        "AS",
        "genres",
    ]);
    assert_eq!(
        rows(&reply)[0][0].1,
        V::Arr(vec![
            strings(&["genre", "comedy"]),
            strings(&["genre", "crime"]),
            strings(&["genre", "scifi"]),
        ])
    );
}

#[test]
fn the_reducers() {
    let mut c = conn();
    movies(&mut c, "a2", "a2:");
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a2",
        "*",
        "LOAD",
        "3",
        "@genre",
        "@rating",
        "@title",
        "GROUPBY",
        "1",
        "@genre",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "n",
        "REDUCE",
        "AVG",
        "1",
        "@rating",
        "AS",
        "avg",
        "REDUCE",
        "MAX",
        "1",
        "@rating",
        "AS",
        "max",
        "REDUCE",
        "STDDEV",
        "1",
        "@rating",
        "AS",
        "sd",
        "REDUCE",
        "QUANTILE",
        "2",
        "@rating",
        "0.5",
        "AS",
        "median",
        "REDUCE",
        "TOLIST",
        "1",
        "@title",
        "AS",
        "titles",
        "REDUCE",
        "FIRST_VALUE",
        "4",
        "@title",
        "BY",
        "@rating",
        "DESC",
        "AS",
        "best",
        "REDUCE",
        "COUNT_DISTINCT",
        "1",
        "@title",
        "AS",
        "distinct",
        "FILTER",
        "@genre == 'scifi'",
    ]);
    let all = rows(&reply);
    assert_eq!(all.len(), 1, "{reply:?}");
    let get = |name: &str| -> V {
        all[0]
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("no {name} in {:?}", all[0]))
            .1
            .clone()
    };
    assert_eq!(get("n").s(), "3");
    assert!((get("avg").s().parse::<f64>().unwrap() - 8.133333).abs() < 1e-5);
    assert_eq!(get("max").s(), "8.500000");
    assert!((get("sd").s().parse::<f64>().unwrap() - 0.321455).abs() < 1e-5);
    assert_eq!(get("median").s(), "8");
    assert_eq!(get("best").s(), "Alien");
    assert_eq!(get("distinct").s(), "3");
    let mut titles: Vec<String> = get("titles")
        .arr()
        .iter()
        .map(|t| t.s().to_string())
        .collect();
    titles.sort();
    assert_eq!(titles, ["Alien", "Arrival", "Solaris"]);

    // SORTBY … MAX keeps the first n; the score of each document on request.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a2",
        "*",
        "SORTBY",
        "2",
        "@rating",
        "DESC",
        "MAX",
        "2",
    ]);
    assert_eq!(
        rows(&reply),
        [
            vec![("rating".to_string(), V::Str("8.5".into()))],
            vec![("rating".to_string(), V::Str("8.3".into()))]
        ]
    );
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a2",
        "alien | heat",
        "ADDSCORES",
        "SORTBY",
        "2",
        "@__score",
        "DESC",
    ]);
    let scored = rows(&reply);
    assert_eq!(scored.len(), 2);
    assert!(
        scored
            .iter()
            .all(|row| { row[0].0 == "__score" && row[0].1.s().parse::<f64>().unwrap() > 0.0 })
    );
}

/// `WITHCURSOR`: the rows a few at a time. `FT.CURSOR READ` used to answer
/// "cursor not found" to everything, there being no cursors.
#[test]
fn a_cursor_reads_an_aggregation_a_few_rows_at_a_time() {
    let mut c = conn();
    movies(&mut c, "a3", "a3:");
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a3",
        "*",
        "LOAD",
        "1",
        "@title",
        "SORTBY",
        "1",
        "@title",
        "WITHCURSOR",
        "COUNT",
        "4",
    ]);
    let titles = |reply: &V| -> Vec<String> {
        rows(&reply.arr()[0])
            .iter()
            .map(|row| row[0].1.s().to_string())
            .collect()
    };
    assert_eq!(titles(&reply), ["Airplane", "Alien", "Arrival", "Fargo"]);
    let cursor = reply.arr()[1].i();
    assert!(cursor > 0);

    // The rest; with nothing left, the cursor is 0 and is no more.
    let reply = c.cmd(&["FT.CURSOR", "READ", "a3", &cursor.to_string()]);
    assert_eq!(titles(&reply), ["Heat", "Solaris"]);
    assert_eq!(reply.arr()[1].i(), 0);
    assert_eq!(
        c.cmd(&["FT.CURSOR", "READ", "a3", &cursor.to_string()])
            .err(),
        "Cursor not found"
    );

    // COUNT on a read overrides the cursor's own; DEL closes one early.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a3",
        "*",
        "LOAD",
        "1",
        "@title",
        "SORTBY",
        "1",
        "@title",
        "WITHCURSOR",
        "COUNT",
        "1",
        "MAXIDLE",
        "10000",
    ]);
    assert_eq!(titles(&reply), ["Airplane"]);
    let cursor = reply.arr()[1].i().to_string();
    let reply = c.cmd(&["FT.CURSOR", "READ", "a3", &cursor, "COUNT", "3"]);
    assert_eq!(titles(&reply), ["Alien", "Arrival", "Fargo"]);
    assert_eq!(reply.arr()[1].i().to_string(), cursor);
    // Another index's name does not open it.
    assert_eq!(
        c.cmd(&["FT.CURSOR", "READ", "q1", &cursor]).err(),
        "Cursor not found"
    );
    assert!(c.cmd(&["FT.CURSOR", "DEL", "a3", &cursor]).ok());
    assert_eq!(
        c.cmd(&["FT.CURSOR", "DEL", "a3", &cursor]).err(),
        "Cursor does not exist"
    );

    // Everything fits in the first read: there is no cursor to come back to.
    let reply = c.cmd(&["FT.AGGREGATE", "a3", "*", "WITHCURSOR"]);
    assert_eq!(reply.arr()[0].arr()[0].i(), 6);
    assert_eq!(reply.arr()[1].i(), 0);

    // A cursor does not outlive its index.
    let reply = c.cmd(&["FT.AGGREGATE", "a3", "*", "WITHCURSOR", "COUNT", "1"]);
    let cursor = reply.arr()[1].i().to_string();
    assert!(c.cmd(&["FT.DROPINDEX", "a3"]).ok());
    assert_eq!(
        c.cmd(&["FT.CURSOR", "READ", "a3", &cursor]).err(),
        "Cursor not found"
    );
}

/// `FT.EXPLAIN` echoed the query back; `FT.PROFILE` returned an empty array.
#[test]
fn explain_and_profile_describe_the_query() {
    let mut c = conn();
    movies(&mut c, "a4", "a4:");
    assert_eq!(
        c.cmd(&[
            "FT.EXPLAIN",
            "a4",
            "(alien | heat) @genre:{scifi} @rating:[8 +inf]"
        ])
        .s(),
        "INTERSECT {\n  UNION {\n    alien\n    heat\n  }\n  TAG:@genre {\n    scifi\n  }\n  \
         NUMERIC {8.000000 <= @rating <= inf}\n}\n"
    );
    assert_eq!(
        c.cmd(&["FT.EXPLAINCLI", "a4", "-@genre:{crime}"]),
        strings(&["NOT {", "  TAG:@genre {", "    crime", "  }", "}"])
    );
    assert_eq!(c.cmd(&["FT.EXPLAIN", "a4", "*"]).s(), "<WILDCARD>\n");
    assert!(c.cmd(&["FT.EXPLAIN", "a4", "@nothere:x"]).is_err_reply());
    assert!(c.cmd(&["FT.EXPLAIN", "nothere", "*"]).is_err_reply());

    // The reply of the query itself, then how it went.
    let reply = c.cmd(&[
        "FT.PROFILE",
        "a4",
        "SEARCH",
        "QUERY",
        "@genre:{scifi}",
        "NOCONTENT",
    ]);
    let plain = c.cmd(&["FT.SEARCH", "a4", "@genre:{scifi}", "NOCONTENT"]);
    assert_eq!(reply.arr().len(), 2);
    assert_eq!(reply.arr()[0], plain);
    let shard = &reply.arr()[1].get("Shards").arr()[0];
    assert!(shard.get("Total profile time").s().parse::<f64>().is_ok());
    assert_eq!(shard.get("Warning").s(), "None");
    let iterators = shard.get("Iterators profile");
    assert_eq!(iterators.get("Type").s(), "TAG");
    assert_eq!(iterators.get("Term").s(), "scifi");
    assert_eq!(iterators.get("Number of reading operations").i(), 3);
    assert_eq!(
        shard.get("Result processors profile").arr()[0]
            .get("Type")
            .s(),
        "Index"
    );

    let reply = c.cmd(&[
        "FT.PROFILE",
        "a4",
        "AGGREGATE",
        "LIMITED",
        "QUERY",
        "*",
        "GROUPBY",
        "1",
        "@genre",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "n",
    ]);
    assert_eq!(reply.arr()[0].arr()[0].i(), 3);
    assert_eq!(
        reply.arr()[1].get("Shards").arr()[0]
            .get("Iterators profile")
            .get("Type")
            .s(),
        "WILDCARD"
    );
    // The query's own error, if it has one.
    assert!(
        c.cmd(&["FT.PROFILE", "a4", "SEARCH", "QUERY", "@nothere:x"])
            .is_err_reply()
    );
    assert!(c.cmd(&["FT.PROFILE", "a4", "SEARCH", "*"]).is_err_reply());
}

/// `FT.CONFIG GET` returned nothing and `SET` accepted anything.
#[test]
fn the_query_engine_settings_can_be_read_and_set() {
    let mut c = conn();
    // No limit unless one is set: see `a_query_is_cut_short_when_it_runs_out_of_time`.
    let reply = c.cmd(&["FT.CONFIG", "GET", "TIMEOUT"]);
    assert_eq!(reply, V::Arr(vec![strings(&["TIMEOUT", "0"])]));
    assert!(c.cmd(&["FT.CONFIG", "GET", "*"]).arr().len() > 10);
    assert!(c.cmd(&["FT.CONFIG", "GET", "NOSUCH"]).arr().is_empty());

    assert!(c.cmd(&["FT.CONFIG", "SET", "MINPREFIX", "3"]).ok());
    assert_eq!(
        c.cmd(&["FT.CONFIG", "GET", "minprefix"]),
        V::Arr(vec![strings(&["MINPREFIX", "3"])])
    );
    assert!(c.cmd(&["FT.CONFIG", "SET", "MINPREFIX", "2"]).ok());
    assert_eq!(
        c.cmd(&["FT.CONFIG", "SET", "NOSUCH", "1"]).err(),
        "Invalid option"
    );
    assert!(
        c.cmd(&["FT.CONFIG", "SET", "TIMEOUT", "soon"])
            .is_err_reply()
    );
    assert!(
        c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "explode"])
            .is_err_reply()
    );
}

/// A hash is no less a document for one of its fields having a TTL. It used
/// to drop out of every query the moment a field was given one.
#[test]
fn a_hash_with_a_field_ttl_is_still_a_document() {
    let mut c = conn();
    catalogue(&mut c, "x1", "x1:");
    let hose = |c: &mut Conn| {
        c.cmd(&["FT.SEARCH", "x1", "@title:hose", "NOCONTENT"])
            .ids()
    };
    assert_eq!(hose(&mut c), ["x1:4"]);

    let reply = c.cmd(&["HEXPIRE", "x1:4", "1000", "FIELDS", "1", "body"]);
    assert_eq!(reply, V::Arr(vec![V::Int(1)]));
    assert_eq!(hose(&mut c), ["x1:4"]);
    let reply = c.cmd(&[
        "FT.SEARCH",
        "x1",
        "@colour:{dark green}",
        "SORTBY",
        "price",
        "RETURN",
        "1",
        "title",
    ]);
    assert_eq!(reply.ids(), ["x1:4", "x1:3"]);
    assert_eq!(
        reply.arr()[2].pairs(),
        [("title".to_string(), "Garden hose".to_string())]
    );
    assert_eq!(c.cmd(&["FT.INFO", "x1"]).get("num_docs").i(), 5);
    assert_eq!(c.cmd(&["FT.AGGREGATE", "x1", "*"]).arr().len(), 6);

    // And one written with a TTL from the start.
    let reply = c.cmd(&[
        "HSETEX",
        "x1:6",
        "EX",
        "1000",
        "FIELDS",
        "2",
        "title",
        "Garden hose reel",
        "price",
        "40",
    ]);
    assert_eq!(reply.i(), 1);
    assert_eq!(hose(&mut c), ["x1:4", "x1:6"]);
}

/// `TIMEOUT` was accepted as a skip-word; a value that is not a number is
/// refused now that the argument is acted on.
#[test]
fn search_timeout_must_be_a_non_negative_integer() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "tb1",
            "PREFIX",
            "1",
            "tb1:",
            "SCHEMA",
            "t",
            "TEXT",
        ])
        .ok()
    );
    assert!(
        c.cmd(&["FT.SEARCH", "tb1", "*", "TIMEOUT", "soon"])
            .err()
            .contains("TIMEOUT")
    );
    assert!(
        c.cmd(&["FT.AGGREGATE", "tb1", "*", "TIMEOUT", "soon"])
            .err()
            .contains("TIMEOUT")
    );
    assert!(c.cmd(&["FT.SEARCH", "tb1", "*", "TIMEOUT"]).is_err_reply());
}

/// Whether a RESP3 reply carries the timeout warning.
fn warned(reply: &V) -> bool {
    reply
        .get("warning")
        .arr()
        .iter()
        .any(|w| w.s().contains("Timeout limit was reached"))
}

/// The value of `name` in the first row of a RESP3 FT.AGGREGATE reply.
fn first_row(reply: &V, name: &str) -> String {
    reply.get("results").arr()[0]
        .get("extra_attributes")
        .get(name)
        .s()
        .to_string()
}

/// `TIMEOUT` / `ON_TIMEOUT` cut a query short. Documents are loaded until a
/// 1 ms RESP3 search reports the timeout warning, and then four times as
/// many again: what took longer than 1 ms then takes five times as long, so
/// that a fast host does not finish the queries below in time after all.
#[test]
fn a_query_is_cut_short_when_it_runs_out_of_time() {
    let mut c = own_conn();
    assert!(matches!(c.cmd(&["HELLO", "3"]), V::Map(_)));
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "to",
            "PREFIX",
            "1",
            "to:",
            "SCHEMA",
            "t",
            "TEXT",
        ])
        .ok()
    );

    let mut n = 0usize;
    let mut timed_out = false;
    while n < 200_000 {
        hset_many(&mut c, "to:", n, 8_000);
        n += 8_000;
        // Twice in a row: one search held up by the host, not by its
        // documents, is not the size to grow from.
        let mut late = || {
            let reply = c.cmd(&["FT.SEARCH", "to", "*", "NOCONTENT", "TIMEOUT", "1"]);
            warned(&reply)
        };
        if late() && late() {
            timed_out = true;
            break;
        }
    }
    assert!(timed_out, "TIMEOUT 1 still finished after {n} documents");
    hset_many(&mut c, "to:", n, 4 * n);
    n *= 5;
    let count = |c: &mut Conn, timeout: &str| -> V {
        c.cmd(&[
            "FT.AGGREGATE",
            "to",
            "*",
            "TIMEOUT",
            timeout,
            "GROUPBY",
            "0",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "c",
        ])
    };

    // `TIMEOUT 0` is no limit, for both commands.
    let all = c.cmd(&[
        "FT.SEARCH",
        "to",
        "*",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "0",
    ]);
    assert_eq!(all.get("total_results").i(), n as i64, "{all:?}");
    assert!(!warned(&all), "{all:?}");
    let all = count(&mut c, "0");
    assert_eq!(first_row(&all, "c"), n.to_string(), "{all:?}");
    assert!(!warned(&all), "{all:?}");

    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "fail"]).ok());
    let search = c.cmd(&["FT.SEARCH", "to", "*", "TIMEOUT", "1"]);
    let aggregate = count(&mut c, "1");
    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "return"]).ok());
    for failed in [search, aggregate] {
        assert!(
            failed.err().contains("Timeout limit was reached"),
            "{failed:?}"
        );
    }

    // `return`, the default: what was found, and the warning.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "to",
        "*",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "1",
    ]);
    assert!(warned(&reply), "{reply:?}");
    assert!(reply.get("total_results").i() < n as i64, "{reply:?}");
    let reply = count(&mut c, "1");
    assert!(warned(&reply), "{reply:?}");
    assert!(
        first_row(&reply, "c").parse::<usize>().unwrap() < n,
        "{reply:?}"
    );

    // A cursor over a partial result says so on every page, not only the
    // first.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "to",
        "*",
        "LOAD",
        "1",
        "@t",
        "TIMEOUT",
        "1",
        "WITHCURSOR",
        "COUNT",
        "10",
    ]);
    assert!(warned(&reply.arr()[0]), "{reply:?}");
    let cursor = reply.arr()[1].i();
    assert!(cursor > 0, "{reply:?}");
    let page = c.cmd(&["FT.CURSOR", "READ", "to", &cursor.to_string()]);
    assert!(warned(&page.arr()[0]), "{page:?}");
    assert_eq!(page.arr()[0].get("results").arr().len(), 10, "{page:?}");
    if page.arr()[1].i() != 0 {
        assert!(c.cmd(&["FT.CURSOR", "DEL", "to", &cursor.to_string()]).ok());
    }

    // Over RESP2 the same timeout is silent: a count short of the whole,
    // not an error.
    let _ = c.cmd(&["HELLO", "2"]);
    let reply = c.cmd(&[
        "FT.SEARCH",
        "to",
        "*",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "1",
    ]);
    assert!(matches!(reply, V::Arr(_)), "{reply:?}");
    assert!(reply.arr()[0].i() < n as i64, "{reply:?}");
}

/// A query that runs to the end is whole, however long it took: only one
/// that was cut short is reported as timed out. A few hundred large
/// documents take well over 1 ms to match and score, and are fewer than
/// the scan reads the clock for, so it does run to the end; it used to be
/// reported as timed out anyway, an error under `ON_TIMEOUT fail`.
#[test]
fn a_query_that_finishes_is_not_reported_as_timed_out() {
    let mut c = own_conn();
    assert!(matches!(c.cmd(&["HELLO", "3"]), V::Map(_)));
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "tf",
            "PREFIX",
            "1",
            "tf:",
            "SCHEMA",
            "t",
            "TEXT",
        ])
        .ok()
    );
    let text = "hello world ".repeat(10_000);
    for i in 0..300 {
        assert!(matches!(
            c.cmd(&["HSET", &format!("tf:{i}"), "t", &text]),
            V::Int(1)
        ));
    }

    let started = Instant::now();
    let reply = c.cmd(&["FT.SEARCH", "tf", "hello", "NOCONTENT", "TIMEOUT", "1"]);
    let took = started.elapsed();
    assert_eq!(reply.get("total_results").i(), 300, "{reply:?}");
    assert!(!warned(&reply), "after {took:?}: {reply:?}");

    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "tf",
        "hello",
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
    assert_eq!(first_row(&reply, "c"), "300", "{reply:?}");
    assert!(!warned(&reply), "{reply:?}");
}

/// A TAG filter has no candidates of its own: FT.SEARCH's KNN with one, and
/// FT.AGGREGATE with one, read every document, and the clock is read
/// between runs of documents rather than before each. They still stop at
/// `TIMEOUT`, and with `TIMEOUT 0` they are whole. Grown as in
/// `a_query_is_cut_short_when_it_runs_out_of_time`.
#[test]
fn a_tag_filtered_scan_is_cut_short_when_it_runs_out_of_time() {
    let mut c = own_conn();
    assert!(matches!(c.cmd(&["HELLO", "3"]), V::Map(_)));
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "tk",
            "PREFIX",
            "1",
            "tk:",
            "SCHEMA",
            "c",
            "TAG",
            "v",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "L2",
        ])
        .ok()
    );
    let q = [0.0f32, 1.0, 2.0, 3.0];
    let knn = |c: &mut Conn, timeout: &str| -> V {
        search_with_vector(
            c,
            "tk",
            "(@c:{red})=>[KNN 10 @v $q]",
            &q,
            &["NOCONTENT", "TIMEOUT", timeout],
        )
    };

    let mut n = 0usize;
    let mut timed_out = false;
    while n < 400_000 {
        hset_tagged_many(&mut c, "tk:", n, 8_000);
        n += 8_000;
        if warned(&knn(&mut c, "1")) && warned(&knn(&mut c, "1")) {
            timed_out = true;
            break;
        }
    }
    assert!(timed_out, "TIMEOUT 1 still finished after {n} documents");
    hset_tagged_many(&mut c, "tk:", n, 4 * n);
    n *= 5;

    // `TIMEOUT 0`: the ten nearest red documents, 0, 2, … 18, in order.
    let all = knn(&mut c, "0");
    assert!(!warned(&all), "{all:?}");
    let keys: Vec<String> = all
        .get("results")
        .arr()
        .iter()
        .map(|r| r.get("id").s().to_string())
        .collect();
    let nearest: Vec<String> = (0..10).map(|i| format!("tk:{}", 2 * i)).collect();
    assert_eq!(keys, nearest, "{all:?}");

    let cut = knn(&mut c, "1");
    assert!(warned(&cut), "{cut:?}");

    let count = |c: &mut Conn, timeout: &str| -> V {
        c.cmd(&[
            "FT.AGGREGATE",
            "tk",
            "@c:{red}",
            "TIMEOUT",
            timeout,
            "GROUPBY",
            "0",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "n",
        ])
    };
    let all = count(&mut c, "0");
    assert!(!warned(&all), "{all:?}");
    assert_eq!(first_row(&all, "n"), (n / 2).to_string(), "{all:?}");
    let cut = count(&mut c, "1");
    assert!(warned(&cut), "{cut:?}");
    assert!(
        first_row(&cut, "n").parse::<usize>().unwrap() < n / 2,
        "{cut:?}"
    );

    // Under `ON_TIMEOUT fail`, an error.
    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "fail"]).ok());
    let search = knn(&mut c, "1");
    let aggregate = count(&mut c, "1");
    assert!(c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "return"]).ok());
    for failed in [search, aggregate] {
        assert!(
            failed.err().contains("Timeout limit was reached"),
            "{failed:?}"
        );
    }
}

/// FT.AGGREGATE's `TIMEOUT` is the option where an option can be, not a word
/// of another clause: an `APPLY … AS TIMEOUT`, or a parameter of that name,
/// was taken for it, and the word after it for the milliseconds.
#[test]
fn an_aggregate_word_named_timeout_is_not_the_option() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "tn",
            "PREFIX",
            "1",
            "tn:",
            "SCHEMA",
            "n",
            "NUMERIC",
            "c",
            "TAG",
        ])
        .ok()
    );
    c.cmd(&["HSET", "tn:1", "n", "1", "c", "red"]);
    c.cmd(&["HSET", "tn:2", "n", "2", "c", "blue"]);

    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "tn",
        "*",
        "LOAD",
        "1",
        "@n",
        "APPLY",
        "@n*10",
        "AS",
        "TIMEOUT",
        "SORTBY",
        "2",
        "@n",
        "ASC",
    ]);
    assert_eq!(reply.arr()[0].i(), 2, "{reply:?}");
    assert_eq!(
        reply.arr()[1].pairs(),
        [
            ("n".to_string(), "1".to_string()),
            ("TIMEOUT".to_string(), "10".to_string())
        ],
        "{reply:?}"
    );

    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "tn",
        "@c:{$TIMEOUT}",
        "PARAMS",
        "2",
        "TIMEOUT",
        "red",
        "LOAD",
        "1",
        "@n",
        "DIALECT",
        "2",
    ]);
    assert_eq!(reply.arr()[0].i(), 1, "{reply:?}");
    assert_eq!(
        reply.arr()[1].pairs(),
        [("n".to_string(), "1".to_string())],
        "{reply:?}"
    );

    // The option itself, after them, is still read, and checked.
    assert!(
        c.cmd(&[
            "FT.AGGREGATE",
            "tn",
            "*",
            "APPLY",
            "@n",
            "AS",
            "TIMEOUT",
            "TIMEOUT",
            "soon",
        ])
        .err()
        .contains("TIMEOUT")
    );
}
