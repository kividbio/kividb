#![allow(clippy::disallowed_methods)]
// GEO fields of an index (#1004): values, queries and refusals, against
// what Redis 8.10.3 answers.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── Minimal RESP client ───────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..40)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.cmd(&args)
    }

    fn cmd(&mut self, args: &[&[u8]]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => RespValue::SimpleStr(line[1..].to_string()),
            Some(b'-') => RespValue::Error(line[1..].to_string()),
            Some(b':') => RespValue::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len == -1 {
                    return RespValue::Nil;
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                RespValue::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP line: {line:?}"),
        }
    }
}

// ── RespValue helpers ─────────────────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap_or(""),
            _ => "",
        }
    }

    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap_or("0").parse().unwrap_or(0),
            _ => 0,
        }
    }

    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => &[],
        }
    }
}

// ── Server harness ────────────────────────────────────────────────────────────

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

fn start_server(port: u16) {
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
}

// ── GEO fields (#1004) ───────────────────────────────────────────────────────
//
// Every expected reply below is the one Redis 8.10.3 gives for the same
// documents and command.

fn keys(r: &RespValue) -> Vec<String> {
    let arr = r.as_array();
    assert!(!arr.is_empty(), "not a search reply: {r:?}");
    let mut keys: Vec<String> = arr[1..].iter().map(|k| k.as_str().to_string()).collect();
    keys.sort();
    assert_eq!(arr[0].as_int(), keys.len() as i64, "{r:?}");
    keys
}

fn search(c: &mut Conn, index: &str, query: &str, extra: &[&str]) -> Vec<String> {
    let mut args = vec!["FT.SEARCH", index, query];
    args.extend_from_slice(extra);
    args.extend(["NOCONTENT", "DIALECT", "2", "LIMIT", "0", "10000"]);
    keys(&c.cmds(&args))
}

fn error(r: RespValue) -> String {
    match r {
        RespValue::Error(e) => e,
        other => panic!("not an error: {other:?}"),
    }
}

/// `num_docs`, `hash_indexing_failures` and the last failure's key.
fn failures(c: &mut Conn, index: &str) -> (i64, i64, String) {
    let info = c.cmds(&["FT.INFO", index]);
    let arr = info.as_array();
    let get = |name: &str| {
        arr.chunks(2)
            .find(|pair| pair[0].as_str() == name)
            .map(|pair| &pair[1])
            .unwrap_or_else(|| panic!("no {name} in FT.INFO"))
    };
    let errors = get("Index Errors").as_array();
    let last = errors
        .chunks(2)
        .find(|pair| pair[0].as_str() == "last indexing error key")
        .map(|pair| pair[1].as_str().to_string())
        .unwrap();
    (
        get("num_docs").as_int(),
        get("hash_indexing_failures").as_int(),
        last,
    )
}

fn cities(c: &mut Conn) {
    c.cmds(&[
        "FT.CREATE",
        "g",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "h:",
        "SCHEMA",
        "loc",
        "GEO",
        "name",
        "TEXT",
        "t",
        "TAG",
    ]);
    for (key, loc) in [
        ("h:paris", "2.3522,48.8566"),
        ("h:lyon", "4.8357,45.7640"),
        ("h:london", "-0.1276,51.5072"),
        ("h:ny", "-74.0060,40.7128"),
        ("h:near", "2.36,48.86"),
        ("h:sp", "2.35, 48.85"),
        ("h:bad", "abc"),
        ("h:out", "2.35,86"),
    ] {
        let tag = if key == "h:ny" { "us" } else { "eu" };
        let name = format!("city {}", &key[2..]);
        c.cmds(&["HSET", key, "loc", loc, "name", &name, "t", tag]);
    }
    c.cmds(&["HSET", "h:noloc", "name", "city noloc", "t", "eu"]);
    c.cmds(&[
        "HSET", "h:edge1", "loc", "179.95,0", "name", "edge", "t", "x",
    ]);
    c.cmds(&[
        "HSET",
        "h:edge2",
        "loc",
        "-179.95,0",
        "name",
        "edge",
        "t",
        "x",
    ]);
}

#[test]
fn geo_queries_answer_as_redis_does() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    cities(&mut c);
    // Two of the ten are not points: left out of the index, and counted.
    assert_eq!(failures(&mut c, "g"), (9, 2, "h:out".to_string()));

    let q = |c: &mut Conn, query: &str| search(c, "g", query, &[]);
    let paris = ["h:near", "h:paris", "h:sp"];
    assert_eq!(q(&mut c, "@loc:[2.35 48.85 10 km]"), paris);
    assert_eq!(q(&mut c, "@loc:[2.35 48.85 3 KM]"), paris);
    assert_eq!(
        q(&mut c, "@loc:[2.35 48.85 500 km]"),
        ["h:london", "h:lyon", "h:near", "h:paris", "h:sp"]
    );
    assert_eq!(q(&mut c, "@loc:[2.35 48.85 30000 ft]"), paris);
    // The point at the centre itself, within a metre of it.
    assert_eq!(q(&mut c, "@loc:[2.35 48.85 1 m]"), ["h:sp"]);
    assert_eq!(q(&mut c, "@loc:[2.35 48.85 5000 mi]").len(), 6);
    // Half the circumference and more: every point.
    for every in [
        "@loc:[2.35 48.85 20000 km]",
        "@loc:[2.35 48.85 1e30 km]",
        "@loc:[2.35 48.85 inf km]",
    ] {
        assert_eq!(q(&mut c, every).len(), 8, "{every}");
    }
    // A centre past the latitudes a geohash covers: nothing, at any radius.
    for polar in [
        "@loc:[0 89 1000 km]",
        "@loc:[0 89 20000 km]",
        "@loc:[0 -90 inf km]",
    ] {
        assert!(q(&mut c, polar).is_empty(), "{polar}");
    }
    // Across the antimeridian, from either side of it.
    for edge in [
        "@loc:[179.9 0 50 km]",
        "@loc:[-179.9 0 50 km]",
        "@loc:[180 0 20 km]",
        "@loc:[-180 0 20 km]",
    ] {
        assert_eq!(q(&mut c, edge), ["h:edge1", "h:edge2"], "{edge}");
    }
    assert_eq!(
        q(&mut c, "-@loc:[2.35 48.85 10 km]"),
        [
            "h:edge1", "h:edge2", "h:london", "h:lyon", "h:noloc", "h:ny"
        ]
    );
    assert_eq!(q(&mut c, "@loc:[2.35 48.85 10 km] @t:{eu}"), paris);
    assert_eq!(
        q(&mut c, "@loc:[2.35 48.85 10 km] | @t:{us}"),
        ["h:near", "h:ny", "h:paris", "h:sp"]
    );
    assert_eq!(q(&mut c, "city @loc:[-74 40.7 10 km]"), ["h:ny"]);
    assert_eq!(
        search(
            &mut c,
            "g",
            "@loc:[$lo 48.85 10 km]",
            &["PARAMS", "2", "lo", "2.35"]
        ),
        paris
    );

    // GEOFILTER: as the clause; several AND together; on a field that is
    // not GEO, or with a NaN centre, nothing.
    let geofilter = |c: &mut Conn, args: &[&str]| {
        let mut all = vec!["GEOFILTER"];
        all.extend_from_slice(args);
        search(c, "g", "*", &all)
    };
    assert_eq!(
        geofilter(&mut c, &["loc", "2.35", "48.85", "10", "km"]),
        paris
    );
    assert_eq!(
        search(
            &mut c,
            "g",
            "*",
            &[
                "GEOFILTER",
                "loc",
                "2.35",
                "48.85",
                "500",
                "km",
                "GEOFILTER",
                "loc",
                "4.83",
                "45.76",
                "10",
                "km"
            ]
        ),
        ["h:lyon"]
    );
    for nothing in [
        ["name", "2.35", "48.85", "10", "km"],
        ["nosuch", "2.35", "48.85", "10", "km"],
        ["loc", "nan", "48.85", "10", "km"],
    ] {
        assert!(geofilter(&mut c, &nothing).is_empty(), "{nothing:?}");
    }
    assert_eq!(
        search(
            &mut c,
            "g",
            "city",
            &["GEOFILTER", "loc", "2.35", "48.85", "10", "km"]
        ),
        paris
    );

    // Refused as Redis refuses them.
    for (query, why) in [
        ("@loc:[2.35 48.85 0 km]", "Invalid GeoFilter radius"),
        ("@loc:[2.35 48.85 -1 km]", "Invalid GeoFilter radius"),
        ("@loc:[200 10 1 km]", "Invalid GeoFilter lat/lon"),
        ("@loc:[inf 10 1 km]", "Invalid GeoFilter lat/lon"),
        ("@loc:[0 91 1 km]", "Invalid GeoFilter lat/lon"),
        ("@loc:[2.35 48.85 10 parsec]", "Invalid GeoFilter unit"),
        ("@name:[2.35 48.85 10 km]", "Expected a GEO field"),
        ("@loc:[nan 10 1 km]", "Syntax error"),
        ("@loc:[2.35 48.85 10]", "Syntax error"),
    ] {
        let e = error(c.cmds(&["FT.SEARCH", "g", query, "DIALECT", "2"]));
        assert!(e.contains(why), "{query}: {e}");
    }
    for (args, why) in [
        (&["loc", "2.35", "48.85", "inf", "km"][..], "<radius>"),
        (
            &["loc", "2.35", "48.85", "0", "km"],
            "Invalid GeoFilter radius",
        ),
        (
            &["loc", "200", "48.85", "10", "km"],
            "Invalid GeoFilter lat/lon",
        ),
        (
            &["loc", "2.35", "48.85", "10", "parsec"],
            "Unknown distance unit parsec",
        ),
        (&["loc", "x", "48.85", "10", "km"], "<lon>"),
        (
            &["loc", "2.35", "48.85", "10"],
            "GEOFILTER requires 5 arguments",
        ),
    ] {
        let mut all = vec!["FT.SEARCH", "g", "*", "GEOFILTER"];
        all.extend_from_slice(args);
        let e = error(c.cmds(&all));
        assert!(e.contains(why), "{args:?}: {e}");
    }
    let e = error(c.cmds(&["FT.SEARCH", "g", "*", "SORTBY", "loc"]));
    assert!(e.contains("SORTBY on GEO field"), "{e}");
    assert_eq!(
        c.cmds(&["FT.EXPLAIN", "g", "@loc:[2.35 48.85 10 km]", "DIALECT", "2"])
            .as_str(),
        "GEO loc:{2.350000,48.850000 --> 10.000000 km}\n"
    );
}

/// At the edge of the radius, a point is measured as `GEOSEARCH` measures a
/// member: by the centre of its geohash's cell, not as it was written. This
/// point is 4.9751 m from the centre as written and 4.9389 m as its cell has
/// it: at 4.957 m, Redis finds it.
#[test]
fn geo_radius_edge_is_measured_as_geosearch_measures() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "g",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "h:",
        "SCHEMA",
        "loc",
        "GEO",
    ]);
    c.cmds(&["HSET", "h:1", "loc", "13.3613891,38.1155561"]);
    let at = |c: &mut Conn, r: &str| search(c, "g", &format!("@loc:[13.3614 38.1156 {r} m]"), &[]);
    assert!(at(&mut c, "4.928882").is_empty());
    assert_eq!(at(&mut c, "4.957005"), ["h:1"]);
    assert_eq!(at(&mut c, "4.985129"), ["h:1"]);
}

/// A document whose GEO field holds no point is left out of the index and
/// counted; written again in another field it stays out and is counted
/// again; given a point, or without the field, it is back; deleted, it is
/// forgotten. As Redis 8.10.3 counts them.
#[test]
fn a_document_whose_geo_field_is_no_point_is_left_out() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "g",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "h:",
        "SCHEMA",
        "loc",
        "GEO",
        "name",
        "TEXT",
    ]);
    c.cmds(&["HSET", "h:1", "loc", "2.35,48.85", "name", "alpha"]);
    c.cmds(&["HSET", "h:2", "loc", "bad", "name", "beta"]);
    assert_eq!(failures(&mut c, "g"), (1, 1, "h:2".into()));
    assert!(search(&mut c, "g", "beta", &[]).is_empty());
    assert_eq!(search(&mut c, "g", "*", &[]), ["h:1"]);

    c.cmds(&["HSET", "h:2", "name", "betty"]);
    assert_eq!(failures(&mut c, "g"), (1, 2, "h:2".into()));
    assert!(search(&mut c, "g", "betty", &[]).is_empty());

    c.cmds(&["HSET", "h:2", "loc", "2.36,48.86"]);
    assert_eq!(failures(&mut c, "g").0, 2);
    assert_eq!(search(&mut c, "g", "betty", &[]), ["h:2"]);

    c.cmds(&["HSET", "h:1", "loc", "nowhere"]);
    assert_eq!(failures(&mut c, "g"), (1, 3, "h:1".into()));
    assert!(search(&mut c, "g", "alpha", &[]).is_empty());
    c.cmds(&["HDEL", "h:1", "loc"]);
    assert_eq!(failures(&mut c, "g").0, 2);
    assert_eq!(search(&mut c, "g", "alpha", &[]), ["h:1"]);

    c.cmds(&["HSET", "h:1", "loc", "x"]);
    c.cmds(&["DEL", "h:1"]);
    c.cmds(&["HSET", "h:1", "name", "alpha"]);
    assert_eq!(search(&mut c, "g", "alpha", &[]), ["h:1"]);

    // Over documents already there: FT.CREATE, and FT.ALTER adding a field.
    c.cmds(&["HSET", "b:1", "p", "1,1"]);
    c.cmds(&["HSET", "b:2", "p", "bad"]);
    c.cmds(&[
        "FT.CREATE",
        "b",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "b:",
        "SCHEMA",
        "p",
        "GEO",
    ]);
    c.cmds(&["HSET", "a:1", "p", "1,1", "q", "x"]);
    c.cmds(&["HSET", "a:2", "p", "bad", "q", "x"]);
    c.cmds(&[
        "FT.CREATE",
        "a",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "a:",
        "SCHEMA",
        "q",
        "TEXT",
    ]);
    assert_eq!(search(&mut c, "a", "x", &[]), ["a:1", "a:2"]);
    c.cmds(&["FT.ALTER", "a", "SCHEMA", "ADD", "p", "GEO"]);
    for index in ["a", "b"] {
        assert_eq!(
            failures(&mut c, index),
            (1, 1, format!("{index}:2")),
            "{index}"
        );
    }
    assert_eq!(search(&mut c, "a", "x", &[]), ["a:1"]);
    assert_eq!(search(&mut c, "a", "@p:[1 1 10 km]", &[]), ["a:1"]);
}

/// JSON, as Redis 8.10.3 indexes it: a string or an array of strings
/// (any one point matching), `null` and `[]` no value; anything else, or an
/// array with one element that is not a point, refuses the document.
#[test]
fn json_geo_fields_take_strings_and_arrays_of_them() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "j",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "j:",
        "SCHEMA",
        "$.loc",
        "AS",
        "loc",
        "GEO",
        "$.t",
        "AS",
        "t",
        "TEXT",
    ]);
    for (key, doc) in [
        ("j:str", r#"{"loc":"2.35,48.85","t":"x"}"#),
        ("j:arr", r#"{"loc":["2.35,48.85","10,10"],"t":"x"}"#),
        ("j:numarr", r#"{"loc":[2.35,48.85],"t":"x"}"#),
        ("j:mix", r#"{"loc":["2.35,48.85","bad"],"t":"x"}"#),
        ("j:mixnum", r#"{"loc":["2.35,48.85",5],"t":"x"}"#),
        ("j:nested", r#"{"loc":[["2.35,48.85"]],"t":"x"}"#),
        ("j:null", r#"{"loc":null,"t":"x"}"#),
        ("j:empty", r#"{"loc":[],"t":"x"}"#),
        ("j:num", r#"{"loc":5,"t":"x"}"#),
        ("j:obj", r#"{"loc":{"a":1},"t":"x"}"#),
    ] {
        c.cmds(&["JSON.SET", key, "$", doc]);
    }
    assert_eq!(failures(&mut c, "j").0, 4);
    assert_eq!(failures(&mut c, "j").1, 6);
    assert_eq!(
        search(&mut c, "j", "*", &[]),
        ["j:arr", "j:empty", "j:null", "j:str"]
    );
    assert_eq!(
        search(&mut c, "j", "@loc:[2.35 48.85 10 km]", &[]),
        ["j:arr", "j:str"]
    );
    assert_eq!(search(&mut c, "j", "@loc:[10 10 10 km]", &[]), ["j:arr"]);
    c.cmds(&[
        "JSON.SET",
        "j:mix",
        "$",
        r#"{"loc":["2.35,48.85","3,3"],"t":"x"}"#,
    ]);
    assert_eq!(search(&mut c, "j", "@loc:[3 3 10 km]", &[]), ["j:mix"]);
}

/// The postings and the filter that reads documents agree, on random points
/// and random circles: the same documents with the postings in use as with
/// them off (`FT.DEBUG POSTINGS g OFF`).
#[test]
fn geo_postings_and_the_document_filter_agree() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "g",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "loc",
        "GEO",
    ]);
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed >> 11) as f64 / (1u64 << 53) as f64
    };
    for i in 0..3000 {
        // Clustered, so that small circles find something.
        let (lon, lat) = (next() * 20.0 - 10.0, next() * 20.0 + 40.0);
        c.cmds(&["HSET", &format!("p:{i}"), "loc", &format!("{lon},{lat}")]);
    }
    let mut queries = Vec::new();
    for _ in 0..60 {
        let (lon, lat) = (next() * 24.0 - 12.0, next() * 24.0 + 38.0);
        let radius = 10f64.powf(next() * 4.0);
        queries.push(format!("@loc:[{lon} {lat} {radius} km]"));
    }
    let with: Vec<Vec<String>> = queries
        .iter()
        .map(|q| search(&mut c, "g", q, &[]))
        .collect();
    assert!(
        with.iter()
            .any(|found| !found.is_empty() && found.len() < 3000)
    );
    c.cmds(&["FT.DEBUG", "POSTINGS", "g", "OFF"]);
    for (q, found) in queries.iter().zip(&with) {
        assert_eq!(&search(&mut c, "g", q, &[]), found, "{q}");
    }
}

fn pack_f32(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// A 2-dimensional L2 FLAT vector field, as `FT.CREATE` arguments.
const VECTOR_2D: [&str; 9] = [
    "VECTOR",
    "FLAT",
    "6",
    "TYPE",
    "FLOAT32",
    "DIM",
    "2",
    "DISTANCE_METRIC",
    "L2",
];

/// `FT.ALTER` adding a field does not put back a document the index left
/// out, whatever the field (NUMERIC, TEXT, VECTOR), with `SKIPINITIALSCAN`
/// or without: `*` and the new field find only the documents it holds, and
/// FT.INFO still counts the one refused.
#[test]
fn ft_alter_leaves_a_refused_document_out() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let v = pack_f32(&[1.0, 1.0]);
    for (i, skip) in [false, true].into_iter().enumerate() {
        let index = format!("g{i}");
        let prefix = format!("h{i}:");
        let mut create = vec!["FT.CREATE", &index, "ON", "HASH", "PREFIX", "1", &prefix];
        create.extend(["SCHEMA", "loc", "GEO", "name", "TEXT"]);
        assert_eq!(c.cmds(&create).as_str(), "OK");
        for (id, loc) in [("1", "1,1"), ("2", "bad")] {
            let key = format!("{prefix}{id}");
            c.cmd(&[
                b"HSET",
                key.as_bytes(),
                b"loc",
                loc.as_bytes(),
                b"name",
                b"beta",
                b"n",
                b"5",
                b"t",
                b"word",
                b"v",
                &v,
            ]);
        }
        let mut added: Vec<&str> = Vec::new();
        for field in [
            &["n", "NUMERIC"][..],
            &["t", "TEXT"],
            &[&["v"][..], &VECTOR_2D].concat(),
        ] {
            let mut alter = vec!["FT.ALTER", index.as_str()];
            if skip {
                alter.push("SKIPINITIALSCAN");
            }
            alter.extend(["SCHEMA", "ADD"]);
            alter.extend_from_slice(field);
            assert_eq!(c.cmds(&alter).as_str(), "OK", "{alter:?}");
            added.push(field[0]);
            let kept = vec![format!("{prefix}1")];
            let ctx = format!("skip={skip} after adding {added:?}");
            assert_eq!(
                failures(&mut c, &index),
                (1, 1, format!("{prefix}2")),
                "{ctx}"
            );
            assert_eq!(search(&mut c, &index, "*", &[]), kept, "{ctx}");
            assert_eq!(search(&mut c, &index, "beta", &[]), kept, "{ctx}");
            // A new field finds nothing with SKIPINITIALSCAN (the documents
            // there were not read for it), and only the one kept without.
            let found = if skip { vec![] } else { kept.clone() };
            if added.contains(&"n") {
                assert_eq!(search(&mut c, &index, "@n:[5 5]", &[]), found, "{ctx}");
            }
            if added.contains(&"t") {
                assert_eq!(search(&mut c, &index, "@t:word", &[]), found, "{ctx}");
            }
            if added.contains(&"v") {
                let r = c.cmd(&[
                    b"FT.SEARCH",
                    index.as_bytes(),
                    b"*=>[KNN 5 @v $b]",
                    b"PARAMS",
                    b"2",
                    b"b",
                    &v,
                    b"NOCONTENT",
                    b"DIALECT",
                    b"2",
                ]);
                assert_eq!(keys(&r), found, "{ctx}");
            }
        }
    }
}

/// A GEO clause filters a KNN query (before the nearest are picked), an
/// `FT.AGGREGATE`, and both sides of an `FT.HYBRID`.
#[test]
fn geo_clauses_filter_knn_aggregate_and_hybrid() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    let mut create = vec![
        "FT.CREATE",
        "g",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "d:",
        "SCHEMA",
    ];
    create.extend(["loc", "GEO", "name", "TEXT", "v"]);
    create.extend_from_slice(&VECTOR_2D);
    assert_eq!(c.cmds(&create).as_str(), "OK");
    // Near: Paris; far: Rome. The far ones' vectors are the nearest.
    for (key, loc, x) in [
        ("d:1", "2.35,48.85", 5.0),
        ("d:2", "2.36,48.86", 6.0),
        ("d:3", "12.49,41.89", 0.0),
        ("d:4", "12.50,41.90", 1.0),
    ] {
        let v = pack_f32(&[x, 0.0]);
        c.cmd(&[
            b"HSET",
            key.as_bytes(),
            b"loc",
            loc.as_bytes(),
            b"name",
            b"cafe",
            b"v",
            &v,
        ]);
    }
    let q = pack_f32(&[0.0, 0.0]);
    let near = "@loc:[2.35 48.85 10 km]";

    let knn = format!("({near})=>[KNN 2 @v $q]");
    let r = c.cmd(&[
        b"FT.SEARCH",
        b"g",
        knn.as_bytes(),
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"NOCONTENT",
        b"DIALECT",
        b"2",
    ]);
    assert_eq!(keys(&r), ["d:1", "d:2"]);
    let r = c.cmd(&[
        b"FT.SEARCH",
        b"g",
        b"*=>[KNN 2 @v $q]",
        b"GEOFILTER",
        b"loc",
        b"2.35",
        b"48.85",
        b"10",
        b"km",
        b"PARAMS",
        b"2",
        b"q",
        &q,
        b"NOCONTENT",
        b"DIALECT",
        b"2",
    ]);
    assert_eq!(keys(&r), ["d:1", "d:2"]);

    let r = c.cmds(&[
        "FT.AGGREGATE",
        "g",
        near,
        "LOAD",
        "1",
        "@__key",
        "SORTBY",
        "2",
        "@__key",
        "ASC",
    ]);
    let rows: Vec<&str> = r.as_array()[1..]
        .iter()
        .map(|row| row.as_array()[1].as_str())
        .collect();
    assert_eq!(rows, ["d:1", "d:2"], "{r:?}");
    let r = c.cmds(&[
        "FT.AGGREGATE",
        "g",
        "@loc:[2.35 48.85 10 km] @name:cafe",
        "GROUPBY",
        "0",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "n",
    ]);
    assert_eq!(r.as_array()[1].as_array()[1].as_int(), 2, "{r:?}");

    // FT.HYBRID: the GEO clause in the text query, and in the vector side's
    // FILTER. Either way only the near documents come back.
    let hybrid_keys = |r: &RespValue| -> Vec<String> {
        let top = r.as_array();
        assert_eq!(top.get(2).map(|v| v.as_str()), Some("results"), "{r:?}");
        let mut keys: Vec<String> = top[3]
            .as_array()
            .iter()
            .map(|row| {
                let row = row.as_array();
                let at = row.iter().position(|v| v.as_str() == "__key").unwrap();
                row[at + 1].as_str().to_string()
            })
            .collect();
        keys.sort();
        keys
    };
    let r = c.cmd(&[
        b"FT.HYBRID",
        b"g",
        b"SEARCH",
        near.as_bytes(),
        b"VSIM",
        b"@v",
        b"$q",
        b"FILTER",
        near.as_bytes(),
        b"PARAMS",
        b"2",
        b"q",
        &q,
    ]);
    assert_eq!(hybrid_keys(&r), ["d:1", "d:2"]);
}

/// Points around the corner of cells at every level, (0, 0), and on a line
/// between cells (11.25, the edge of a cell at 5 bits of longitude): each
/// circle finds what `GEOSEARCH … BYRADIUS` finds among the same points.
#[test]
fn geo_queries_across_cell_edges_find_what_geosearch_finds() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "g",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "loc",
        "GEO",
    ]);
    let mut n = 0;
    for (clon, clat) in [(0.0f64, 0.0f64), (11.25, 30.0)] {
        for i in -8..=8 {
            for j in -8..=8 {
                let (lon, lat) = (clon + f64::from(i) * 0.0013, clat + f64::from(j) * 0.0011);
                let point = format!("{lon},{lat}");
                c.cmds(&["HSET", &format!("p:{n}"), "loc", &point]);
                c.cmds(&[
                    "GEOADD",
                    "z",
                    &lon.to_string(),
                    &lat.to_string(),
                    &format!("p:{n}"),
                ]);
                n += 1;
            }
        }
    }
    for (clon, clat) in [
        (0.0f64, 0.0f64),
        (11.25, 30.0),
        (0.0004, -0.0003),
        (11.2499, 30.0001),
    ] {
        for radius in ["1", "120", "250", "900", "1500", "5000"] {
            let found = search(
                &mut c,
                "g",
                &format!("@loc:[{clon} {clat} {radius} m]"),
                &[],
            );
            let r = c.cmds(&[
                "GEOSEARCH",
                "z",
                "FROMLONLAT",
                &clon.to_string(),
                &clat.to_string(),
                "BYRADIUS",
                radius,
                "m",
            ]);
            let mut expected: Vec<String> = r
                .as_array()
                .iter()
                .map(|m| m.as_str().to_string())
                .collect();
            expected.sort();
            assert_eq!(found, expected, "({clon}, {clat}) {radius} m");
        }
    }
}

/// A JSON document rewritten from several points to fewer, to one, and back
/// to several is found by the points it holds now, and only by them.
#[test]
fn a_json_document_rewritten_to_fewer_points_is_found_by_those_only() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "j",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "j:",
        "SCHEMA",
        "$.loc",
        "AS",
        "loc",
        "GEO",
    ]);
    let at = |c: &mut Conn, lon: u32| search(c, "j", &format!("@loc:[{lon} {lon} 10 km]"), &[]);
    let found = |c: &mut Conn| -> Vec<u32> { (1..=4).filter(|&p| !at(c, p).is_empty()).collect() };
    c.cmds(&["JSON.SET", "j:1", "$", r#"{"loc":["1,1","2,2","3,3"]}"#]);
    assert_eq!(found(&mut c), [1, 2, 3]);
    c.cmds(&["JSON.SET", "j:1", "$.loc", r#"["1,1","3,3"]"#]);
    assert_eq!(found(&mut c), [1, 3]);
    c.cmds(&["JSON.SET", "j:1", "$.loc", r#""2,2""#]);
    assert_eq!(found(&mut c), [2]);
    c.cmds(&["JSON.SET", "j:1", "$.loc", r#"["4,4"]"#]);
    assert_eq!(found(&mut c), [4]);
    c.cmds(&["JSON.SET", "j:1", "$.loc", r#"["1,1","2,2","4,4"]"#]);
    assert_eq!(found(&mut c), [1, 2, 4]);
    c.cmds(&["JSON.SET", "j:1", "$.loc", "[]"]);
    assert!(found(&mut c).is_empty());
    assert_eq!(search(&mut c, "j", "*", &[]), ["j:1"]);
}

/// [`geo_postings_and_the_document_filter_agree`] over JSON documents that
/// hold arrays of one to four points, some rewritten with fewer: the
/// postings (one point a document, and several) and the filter that reads
/// the documents find the same ones.
#[test]
fn geo_postings_and_the_document_filter_agree_on_json_arrays() {
    let port = free_port();
    start_server(port);
    let mut c = Conn::connect(port);
    c.cmds(&[
        "FT.CREATE",
        "j",
        "ON",
        "JSON",
        "PREFIX",
        "1",
        "p:",
        "SCHEMA",
        "$.loc",
        "AS",
        "loc",
        "GEO",
    ]);
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed >> 11) as f64 / (1u64 << 53) as f64
    };
    let doc = |next: &mut dyn FnMut() -> f64| {
        let n = 1 + (next() * 4.0) as usize;
        let points: Vec<String> = (0..n)
            .map(|_| format!("\"{},{}\"", next() * 20.0 - 10.0, next() * 20.0 + 40.0))
            .collect();
        format!("{{\"loc\":[{}]}}", points.join(","))
    };
    for i in 0..1500 {
        c.cmds(&["JSON.SET", &format!("p:{i}"), "$", &doc(&mut next)]);
    }
    for i in (0..1500).step_by(7) {
        c.cmds(&["JSON.SET", &format!("p:{i}"), "$", &doc(&mut next)]);
    }
    let mut queries = Vec::new();
    for _ in 0..60 {
        let (lon, lat) = (next() * 24.0 - 12.0, next() * 24.0 + 38.0);
        let radius = 10f64.powf(next() * 4.0);
        queries.push(format!("@loc:[{lon} {lat} {radius} km]"));
    }
    let with: Vec<Vec<String>> = queries
        .iter()
        .map(|q| search(&mut c, "j", q, &[]))
        .collect();
    assert!(
        with.iter()
            .any(|found| !found.is_empty() && found.len() < 1500)
    );
    c.cmds(&["FT.DEBUG", "POSTINGS", "j", "OFF"]);
    for (q, found) in queries.iter().zip(&with) {
        assert_eq!(&search(&mut c, "j", q, &[]), found, "{q}");
    }
}
