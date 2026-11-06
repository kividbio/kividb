// tests/hash_field_search.rs
//
// A hash field past its time is no part of the document to the search
// indexes, though the sweep has not removed it yet: not to a filter, a
// projection, a sort, an aggregation or the tag values. Real `kividb`
// processes, with active expiry turned off where a test needs a field to
// stay past its time (`DEBUG SET-ACTIVE-EXPIRE 0` is process-wide).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
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
            V::Str(s) => s,
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

    fn cmd_bytes(&mut self, args: &[&[u8]]) -> V {
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
        self.cmd_bytes(&args)
    }

    /// A bulk reply, as the bytes it holds (`DUMP`'s payload is not text).
    fn bulk(&mut self, args: &[&str]) -> Vec<u8> {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in &args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let len: usize = line.trim_end().strip_prefix('$').unwrap().parse().unwrap();
        let mut data = vec![0u8; len + 2];
        self.reader.read_exact(&mut data).unwrap();
        data.truncate(len);
        data
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader
            .read_line(&mut line)
            .expect("no reply in 30 s: the server hangs");
        let line = line.trim_end_matches(['\r', '\n']);
        assert!(!line.is_empty(), "connection closed");
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
}

// ── Server fixture ────────────────────────────────────────────────────────────

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see tests/backup.rs).
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

/// A port nothing is listening on, outside the range the system hands out
/// to outgoing connections.
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

struct Server {
    port: u16,
    dir: PathBuf,
    args: Vec<String>,
    child: Child,
}

impl Server {
    fn start(args: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        let port = free_port();
        let child = spawn(&dir, port, &args);
        let server = Server {
            port,
            dir,
            args,
            child,
        };
        assert_eq!(server.conn().cmd(&["PING"]).s(), "PONG");
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// Killed, and started again on the same directory: the AOF replayed.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = spawn(&self.dir, self.port, &self.args);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", self.port)) {
                drop(s);
                if self.conn().cmd(&["PING"]) == V::Str("PONG".into()) {
                    return;
                }
            }
            assert!(Instant::now() < deadline, "the server did not come back");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn spawn(dir: &std::path::Path, port: u16, args: &[String]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    Command::new(env!("CARGO_BIN_EXE_kividb"))
        .current_dir(dir)
        .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
        .args(args)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Wait, up to ten seconds, for `done`.
fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The keys an `FT.SEARCH ... NOCONTENT` found, in its order.
fn keys(reply: &V) -> Vec<String> {
    reply.arr()[1..].iter().map(|k| k.s().to_string()).collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// `[key, [name, value, ...], ...]` of an `FT.SEARCH` with content: the
/// field names each key came back with.
fn returned(reply: &V) -> Vec<(String, Vec<String>)> {
    reply.arr()[1..]
        .chunks(2)
        .map(|pair| {
            let names = pair[1]
                .arr()
                .chunks(2)
                .map(|nv| nv[0].s().to_string())
                .collect();
            (pair[0].s().to_string(), names)
        })
        .collect()
}

/// The rows of an `FT.AGGREGATE` reply, each as its field names.
fn row_names(reply: &V) -> Vec<Vec<String>> {
    reply.arr()[1..]
        .iter()
        .map(|row| {
            let mut names: Vec<String> = row
                .arr()
                .chunks(2)
                .map(|nv| nv[0].s().to_string())
                .collect();
            names.sort();
            names
        })
        .collect()
}

const VEC: [f32; 2] = [1.0, 0.0];

fn blob(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// In `prefix`: an index of the hashes under it, and three documents.
/// `<prefix>1` loses `n`, `t` and `x` to their time; `<prefix>2` has no
/// TTL; `<prefix>3` has a TTL far off on each of its seven fields.
fn documents(c: &mut Conn, prefix: &str) {
    let index = prefix.trim_end_matches(':');
    let created = c.cmd(&[
        "FT.CREATE",
        index,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "n",
        "NUMERIC",
        "SORTABLE",
        "t",
        "TAG",
        "x",
        "TEXT",
        "o",
        "NUMERIC",
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
    assert!(created.ok(), "{created:?}");
    let v = blob(&VEC);
    for (n, doc) in [
        ("1", ["1", "red", "hello"]),
        ("2", ["5", "blue", "world"]),
        ("3", ["3", "green", "hi"]),
    ] {
        let key = format!("{prefix}{n}");
        let [num, tag, text] = doc;
        let args: [&[u8]; 12] = [
            b"HSET",
            key.as_bytes(),
            b"n",
            num.as_bytes(),
            b"t",
            tag.as_bytes(),
            b"x",
            text.as_bytes(),
            b"o",
            b"2",
            b"v",
            &v,
        ];
        assert_eq!(c.cmd_bytes(&args).i(), 5);
    }
    let p3 = format!("{prefix}3");
    c.cmd(&["HSET", &p3, "e1", "1", "e2", "2"]);
    let ttl = c.cmd(&[
        "HPEXPIRE", &p3, "600000", "FIELDS", "7", "n", "t", "x", "o", "v", "e1", "e2",
    ]);
    assert_eq!(ttl.arr().len(), 7);
    let p1 = format!("{prefix}1");
    let ttl = c.cmd(&["HPEXPIRE", &p1, "50", "FIELDS", "3", "n", "t", "x"]);
    assert_eq!(ttl.arr().len(), 3);
}

/// What every query sees of the documents [`documents`] made, once
/// `<prefix>1`'s fields are past their time.
fn assert_expired_fields_are_not_seen(c: &mut Conn, prefix: &str) {
    let index = prefix.trim_end_matches(':');
    let [p1, p2, p3] = [1, 2, 3].map(|n| format!("{prefix}{n}"));
    let search =
        |c: &mut Conn, query: &str| sorted(keys(&c.cmd(&["FT.SEARCH", index, query, "NOCONTENT"])));
    assert_eq!(search(c, "@n:[0 10]"), [p2.clone(), p3.clone()], "NUMERIC");
    assert_eq!(search(c, "@t:{red}"), Vec::<String>::new(), "TAG");
    assert_eq!(search(c, "@t:{green}"), [p3.as_str()], "TAG, live TTL");
    assert_eq!(search(c, "@x:hello"), Vec::<String>::new(), "TEXT");
    assert_eq!(search(c, "@x:hi"), [p3.as_str()], "TEXT, live TTL");
    assert_eq!(
        search(c, "@n:[0 10] @o:[0 10]"),
        [p2.clone(), p3.clone()],
        "two fields"
    );
    assert_eq!(
        search(c, "@o:[0 10]"),
        [p1.clone(), p2.clone(), p3.clone()],
        "a live field"
    );

    // RETURN: p1 comes back without n.
    let reply = c.cmd(&[
        "FT.SEARCH",
        index,
        "@o:[0 10]",
        "RETURN",
        "1",
        "n",
        "SORTBY",
        "o",
    ]);
    let mut got = returned(&reply);
    got.sort();
    assert_eq!(
        got,
        [
            (p1.clone(), vec![]),
            (p2.clone(), vec!["n".to_string()]),
            (p3.clone(), vec!["n".to_string()])
        ]
    );
    // SORTBY n: p1 has none, and comes last.
    let order = keys(&c.cmd(&[
        "FT.SEARCH",
        index,
        "@o:[0 10]",
        "SORTBY",
        "n",
        "ASC",
        "NOCONTENT",
    ]));
    assert_eq!(order, [p3.clone(), p2.clone(), p1.clone()], "SORTBY");

    // FT.AGGREGATE: p1's row has no n; the expired n matches nothing.
    let rows = row_names(&c.cmd(&[
        "FT.AGGREGATE",
        index,
        "@o:[0 10]",
        "LOAD",
        "2",
        "@__key",
        "@n",
        "SORTBY",
        "2",
        "@__key",
        "ASC",
    ]));
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(!rows[0].contains(&"n".to_string()), "{rows:?}");
    assert!(rows[1].contains(&"n".to_string()) && rows[2].contains(&"n".to_string()));
    let rows = c.cmd(&["FT.AGGREGATE", index, "@n:[0 10]", "LOAD", "1", "@o"]);
    assert_eq!(rows.arr().len() - 1, 2, "{rows:?}");

    // FT.TAGVALS: red is no document's.
    assert_eq!(
        sorted(
            c.cmd(&["FT.TAGVALS", index, "t"])
                .arr()
                .iter()
                .map(|t| t.s().to_string())
                .collect()
        ),
        ["blue", "green"]
    );

    // FT.HYBRID: the vector finds p1, and its LOAD has no n.
    let v = blob(&VEC);
    let args: [&[u8]; 18] = [
        b"FT.HYBRID",
        index.as_bytes(),
        b"SEARCH",
        b"@o:[0 10]",
        b"VSIM",
        b"@v",
        b"$q",
        b"SORTBY",
        b"1",
        b"@n",
        b"ASC",
        b"LOAD",
        b"1",
        b"@n",
        b"PARAMS",
        b"2",
        b"q",
        &v,
    ];
    let reply = c.cmd_bytes(&args);
    let rows: Vec<(String, bool)> = returned(&reply)
        .into_iter()
        .map(|(key, names)| (key, names.contains(&"n".to_string())))
        .collect();
    assert_eq!(rows.len(), 3, "{reply:?}");
    for (key, has_n) in rows {
        assert_eq!(has_n, key != p1, "{reply:?}");
    }
}

#[test]
fn expired_fields_are_not_seen_by_any_query() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).ok());
    // Not read since they expired; and read, by an HGET of a live field
    // that leaves the expired ones where they are.
    documents(&mut c, "unread:");
    documents(&mut c, "read:");
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(c.cmd(&["HGET", "read:1", "o"]).s(), "2");
    for prefix in ["unread:", "read:"] {
        assert_expired_fields_are_not_seen(&mut c, prefix);
        // And the same from a scan of every document, the postings off.
        let index = prefix.trim_end_matches(':');
        assert!(c.cmd(&["FT.DEBUG", "POSTINGS", index, "OFF"]).ok());
        assert_expired_fields_are_not_seen(&mut c, prefix);
        assert!(c.cmd(&["FT.DEBUG", "POSTINGS", index, "ON"]).ok());
    }
    // The fields are still there: nothing was evicted by the queries.
    let usage = c.cmd(&["MEMORY", "USAGE", "unread:1"]).i();
    assert!(c.cmd(&["HGET", "unread:1", "o"]).s() == "2");
    // Once the sweep runs, the answers are the same.
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).ok());
    wait_until("the sweep removes the expired fields", || {
        c.cmd(&["MEMORY", "USAGE", "unread:1"]).i() < usage
    });
    assert_expired_fields_are_not_seen(&mut c, "unread:");
}

/// The walks that index what a database holds leave out a field past its
/// time; a new value written to it is indexed as any write is.
#[test]
fn index_maintenance_leaves_expired_fields_out() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).ok());
    c.cmd(&["HSET", "m:1", "n", "1", "o", "2", "w", "7"]);
    c.cmd(&["HPEXPIRE", "m:1", "50", "FIELDS", "2", "n", "w"]);
    std::thread::sleep(Duration::from_millis(150));
    // FT.CREATE's backfill.
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "m",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "m:",
            "SCHEMA",
            "n",
            "NUMERIC",
            "o",
            "NUMERIC",
        ])
        .ok()
    );
    let count = |c: &mut Conn, q: &str| c.cmd(&["FT.SEARCH", "m", q, "NOCONTENT"]).arr()[0].i();
    assert_eq!(count(&mut c, "@n:[0 10]"), 0);
    assert_eq!(count(&mut c, "@o:[0 10]"), 1);
    // FT.ALTER's backfill of a field added later.
    assert!(
        c.cmd(&["FT.ALTER", "m", "SCHEMA", "ADD", "w", "NUMERIC"])
            .ok()
    );
    assert_eq!(count(&mut c, "@w:[0 10]"), 0);
    // A write over the expired field is indexed.
    c.cmd(&["HSET", "m:1", "n", "3"]);
    assert_eq!(count(&mut c, "@n:[3 3]"), 1);
    // The sweep, once on, leaves the index agreeing with the hash.
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).ok());
    wait_until("the sweep removes w", || {
        c.cmd(&["HPTTL", "m:1", "FIELDS", "1", "w"]).arr()[0].i() == -2
            && count(&mut c, "@w:[0 10]") == 0
    });
    assert_eq!(count(&mut c, "@n:[3 3]"), 1);
    assert_eq!(count(&mut c, "@o:[0 10]"), 1);
}

/// The AOF, replayed after the fields' time has passed, loads the hash and
/// its index as they were written; the queries then leave the expired
/// field out, and the sweep removes it.
#[test]
fn a_replayed_aof_keeps_its_hash_and_queries_leave_the_expired_field_out() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "r",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "r:",
            "SCHEMA",
            "n",
            "NUMERIC",
            "o",
            "NUMERIC",
        ])
        .ok()
    );
    c.cmd(&["HSET", "r:1", "n", "1", "o", "2"]);
    c.cmd(&["HPEXPIRE", "r:1", "300", "FIELDS", "1", "n"]);
    c.cmd(&["HSET", "r:1", "o", "4"]);
    std::thread::sleep(Duration::from_millis(400));
    drop(c);
    server.restart();
    let mut c = server.conn();
    let count = |c: &mut Conn, q: &str| c.cmd(&["FT.SEARCH", "r", q, "NOCONTENT"]).arr()[0].i();
    assert_eq!(
        c.cmd(&["HGET", "r:1", "o"]).s(),
        "4",
        "the write after the TTL"
    );
    assert_eq!(count(&mut c, "@n:[0 10]"), 0);
    assert_eq!(count(&mut c, "@o:[4 4]"), 1);
    assert_eq!(c.cmd(&["HGET", "r:1", "n"]), V::Nil);
}

/// Each way a field's TTL is given, changed or taken away leaves the
/// queries seeing the field exactly while it lives, whatever the earliest
/// deadline the hash remembers.
#[test]
fn every_change_to_a_ttl_keeps_queries_right() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).ok());
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "c",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "c:",
            "SCHEMA",
            "n",
            "NUMERIC",
        ])
        .ok()
    );
    let soon = "50";
    let later = "600000";
    // A field with a later TTL, then n with an earlier one.
    c.cmd(&["HSET", "c:earlier", "n", "1", "o", "1"]);
    c.cmd(&["HPEXPIRE", "c:earlier", later, "FIELDS", "1", "o"]);
    c.cmd(&["HPEXPIRE", "c:earlier", soon, "FIELDS", "1", "n"]);
    // Extended, by GT and by XX: the deadline remembered is early.
    for (key, flag) in [("c:gt", "GT"), ("c:xx", "XX")] {
        c.cmd(&["HSET", key, "n", "1"]);
        c.cmd(&["HPEXPIRE", key, soon, "FIELDS", "1", "n"]);
        let r = c.cmd(&["HPEXPIRE", key, later, flag, "FIELDS", "1", "n"]);
        assert_eq!(r.arr()[0].i(), 1, "{flag}");
    }
    // Shortened by LT.
    c.cmd(&["HSET", "c:lt", "n", "1"]);
    c.cmd(&["HPEXPIRE", "c:lt", later, "FIELDS", "1", "n"]);
    assert_eq!(
        c.cmd(&["HPEXPIRE", "c:lt", soon, "LT", "FIELDS", "1", "n"])
            .arr()[0]
            .i(),
        1
    );
    // The earliest TTL taken away: by HPERSIST, HSET, HGETEX PERSIST, HDEL.
    c.cmd(&["HSET", "c:persist", "n", "1", "o", "1"]);
    c.cmd(&["HPEXPIRE", "c:persist", soon, "FIELDS", "1", "n"]);
    c.cmd(&["HPEXPIRE", "c:persist", later, "FIELDS", "1", "o"]);
    c.cmd(&["HPERSIST", "c:persist", "FIELDS", "1", "n"]);
    c.cmd(&["HSET", "c:hset", "n", "1"]);
    c.cmd(&["HPEXPIRE", "c:hset", soon, "FIELDS", "1", "n"]);
    c.cmd(&["HSET", "c:hset", "n", "2"]);
    c.cmd(&["HSET", "c:getpersist", "n", "1"]);
    c.cmd(&["HPEXPIRE", "c:getpersist", soon, "FIELDS", "1", "n"]);
    c.cmd(&["HGETEX", "c:getpersist", "PERSIST", "FIELDS", "1", "n"]);
    c.cmd(&["HSET", "c:hdel", "n", "1", "m", "1"]);
    c.cmd(&["HPEXPIRE", "c:hdel", soon, "FIELDS", "1", "m"]);
    c.cmd(&["HPEXPIRE", "c:hdel", later, "FIELDS", "1", "n"]);
    c.cmd(&["HDEL", "c:hdel", "m"]);
    // Given by HSETEX and HGETEX.
    c.cmd(&["HSET", "c:hsetex", "o", "1"]);
    c.cmd(&["HSETEX", "c:hsetex", "PX", soon, "FIELDS", "1", "n", "1"]);
    c.cmd(&["HSET", "c:hgetex", "n", "1", "o", "1"]);
    c.cmd(&["HGETEX", "c:hgetex", "PX", soon, "FIELDS", "1", "n"]);
    // Copied and restored while n still lives.
    c.cmd(&["HSET", "c:src", "n", "1", "o", "1"]);
    c.cmd(&["HPEXPIRE", "c:src", soon, "FIELDS", "1", "n"]);
    assert_eq!(c.cmd(&["COPY", "c:src", "c:copy"]).i(), 1);
    let payload = c.bulk(&["DUMP", "c:src"]);
    let restored = c.cmd_bytes(&[b"RESTORE", b"c:restored", b"0", &payload]);
    assert!(restored.ok(), "{restored:?}");

    std::thread::sleep(Duration::from_millis(150));
    let live = [
        "c:getpersist",
        "c:gt",
        "c:hdel",
        "c:hset",
        "c:persist",
        "c:xx",
    ];
    let gone = [
        "c:copy",
        "c:earlier",
        "c:hgetex",
        "c:hsetex",
        "c:lt",
        "c:restored",
        "c:src",
    ];
    // Before anything reads the hashes, then again once HGET has.
    for _ in 0..2 {
        let found = sorted(keys(&c.cmd(&[
            "FT.SEARCH",
            "c",
            "@n:[0 10]",
            "NOCONTENT",
            "LIMIT",
            "0",
            "100",
        ])));
        assert_eq!(found, live);
        for key in live {
            assert!(c.cmd(&["HGET", key, "n"]) != V::Nil, "{key}");
        }
        for key in gone {
            assert_eq!(c.cmd(&["HGET", key, "n"]), V::Nil, "{key}");
        }
    }
}

/// A whole-hash read removes every field past its time however the hash's
/// earliest field time was left: by HPERSIST, HDEL or an extension, or by
/// an earlier time set after one. Active expiry is off, so only the reads
/// remove them.
#[test]
fn whole_hash_reads_find_every_expired_field_however_the_times_changed() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).ok());
    let fields = |c: &mut Conn| {
        let reply = c.cmd(&["HGETALL", "w"]);
        let names: Vec<String> = reply
            .arr()
            .iter()
            .step_by(2)
            .map(|v| v.s().to_string())
            .collect();
        sorted(names)
    };
    c.cmd(&[
        "HSET", "w", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5",
    ]);
    c.cmd(&["HPEXPIRE", "w", "100", "FIELDS", "1", "a"]);
    c.cmd(&["HPERSIST", "w", "FIELDS", "1", "a"]);
    c.cmd(&["HPEXPIRE", "w", "100", "FIELDS", "1", "b"]);
    c.cmd(&["HDEL", "w", "b"]);
    c.cmd(&["HPEXPIRE", "w", "100", "FIELDS", "1", "c"]);
    c.cmd(&["HPEXPIRE", "w", "600000", "FIELDS", "2", "c", "e"]);
    assert_eq!(c.cmd(&["HLEN", "w"]).i(), 4);
    // The earliest time left behind has come; nothing is due.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(c.cmd(&["HLEN", "w"]).i(), 4);
    assert_eq!(fields(&mut c), ["a", "c", "d", "e"]);

    // An earlier time after the extension.
    c.cmd(&["HPEXPIRE", "w", "100", "FIELDS", "1", "d"]);
    assert_eq!(c.cmd(&["HLEN", "w"]).i(), 4);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(c.cmd(&["HLEN", "w"]).i(), 3);
    assert_eq!(fields(&mut c), ["a", "c", "e"]);
    let ttl = c.cmd(&["HPTTL", "w", "FIELDS", "3", "a", "c", "e"]);
    assert_eq!(ttl.arr()[0].i(), -1);
    assert!(ttl.arr()[1].i() > 0 && ttl.arr()[2].i() > 0, "{ttl:?}");

    // Several at once, by HGETALL.
    c.cmd(&["HPEXPIRE", "w", "50", "FIELDS", "2", "c", "e"]);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(fields(&mut c), ["a"]);
    assert_eq!(c.cmd(&["HLEN", "w"]).i(), 1);
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).ok());
}

/// Queries beside writes that take fields' times away, give them again and
/// write the fields anew: what a query finds is what the documents held at
/// some time during it. A field that never has a time is always found;
/// the values never change, so a value returned is always the field's own.
/// Once the writes stop with every `n` past its time, no query finds one,
/// from the postings or from a scan of every document.
#[test]
fn queries_beside_changes_to_field_times() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).ok());
    let schema = [
        "FT.CREATE",
        "cq",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "cq:",
        "SCHEMA",
        "n",
        "NUMERIC",
        "o",
        "NUMERIC",
        "SORTABLE",
    ];
    assert!(c.cmd(&schema).ok());
    let docs = 200;
    for i in 0..docs {
        let (key, v) = (format!("cq:{i}"), i.to_string());
        c.cmd(&["HSET", &key, "n", &v, "o", &v]);
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|r| {
            let stop = std::sync::Arc::clone(&stop);
            let mut c = server.conn();
            std::thread::spawn(move || {
                let mut queries = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) || queries == 0 {
                    queries += 1;
                    let all = c.cmd(&[
                        "FT.SEARCH",
                        "cq",
                        "@o:[0 1000]",
                        "NOCONTENT",
                        "LIMIT",
                        "0",
                        "0",
                    ]);
                    assert_eq!(all.arr()[0].i(), docs, "{all:?}");
                    let reply = if r == 0 {
                        c.cmd(&[
                            "FT.SEARCH",
                            "cq",
                            "@n:[0 1000]",
                            "LIMIT",
                            "0",
                            "300",
                            "RETURN",
                            "1",
                            "n",
                        ])
                    } else {
                        c.cmd(&[
                            "FT.SEARCH",
                            "cq",
                            "@n:[0 1000]",
                            "SORTBY",
                            "o",
                            "LIMIT",
                            "0",
                            "300",
                            "RETURN",
                            "2",
                            "n",
                            "o",
                        ])
                    };
                    assert!(reply.arr()[0].i() <= docs, "{reply:?}");
                    for pair in reply.arr()[1..].chunks(2) {
                        let i = pair[0].s().trim_start_matches("cq:").to_string();
                        for nv in pair[1].arr().chunks(2) {
                            assert_eq!(nv[1].s(), i, "{} of {}", nv[0].s(), pair[0].s());
                        }
                    }
                    let rows = c.cmd(&["FT.AGGREGATE", "cq", "@o:[0 1000]", "LOAD", "1", "@n"]);
                    assert_eq!(rows.arr().len() as i64 - 1, docs, "{rows:?}");
                }
            })
        })
        .collect();
    for round in 0..40 {
        for i in (round % 4..docs).step_by(4) {
            let (key, v) = (format!("cq:{i}"), i.to_string());
            match round % 3 {
                0 => c.cmd(&["HPEXPIRE", &key, "1", "FIELDS", "1", "n"]),
                1 => c.cmd(&["HPERSIST", &key, "FIELDS", "1", "n"]),
                _ => c.cmd(&["HSET", &key, "n", &v]),
            };
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    for i in 0..docs {
        let (key, v) = (format!("cq:{i}"), i.to_string());
        c.cmd(&["HSET", &key, "n", &v]);
        c.cmd(&["HPEXPIRE", &key, "1", "FIELDS", "1", "n"]);
    }
    std::thread::sleep(Duration::from_millis(50));
    for postings in ["ON", "OFF"] {
        assert!(c.cmd(&["FT.DEBUG", "POSTINGS", "cq", postings]).ok());
        let n = c.cmd(&["FT.SEARCH", "cq", "@n:[0 1000]", "NOCONTENT"]);
        assert_eq!(n.arr()[0].i(), 0, "postings {postings}: {n:?}");
        let o = c.cmd(&[
            "FT.SEARCH",
            "cq",
            "@o:[0 1000]",
            "NOCONTENT",
            "LIMIT",
            "0",
            "0",
        ]);
        assert_eq!(o.arr()[0].i(), docs, "postings {postings}");
        let rows = c.cmd(&["FT.AGGREGATE", "cq", "@o:[0 1000]", "LOAD", "1", "@n"]);
        assert!(
            row_names(&rows).iter().all(|names| names.is_empty()),
            "{rows:?}"
        );
    }
    assert!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).ok());
}
