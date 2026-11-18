// Lists kept in chunks of packed elements: every list command gives the same
// answer whatever the form (one chunk, many, irregular after a middle insert,
// plain once an element over 1,024 bytes came in), across every change of
// form; the elements come back the same, in order, after a save, an AOF
// replay, an AOF rewrite, a full sync and the stream after it, DUMP/RESTORE
// and MIGRATE; DEBUG DIGEST-VALUE does not depend on the form. Each test runs
// its own server processes.

mod common;

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn kividb_bin() -> String {
    std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        format!("{manifest}/target/debug/kividb")
    })
}

/// The I/O models the server has here: io_uring's loop runs only on Linux.
fn io_models() -> &'static [&'static str] {
    if cfg!(target_os = "linux") {
        &["uring", "tokio"]
    } else {
        &["tokio"]
    }
}

struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
    args: Vec<String>,
}

impl Server {
    fn start(dir: &Path, extra: &[&str]) -> Self {
        let args: Vec<String> = extra.iter().map(|a| a.to_string()).collect();
        let (child, port) = common::start_verified(|port| spawn(dir, port, &args));
        Server {
            child,
            port,
            dir: dir.to_path_buf(),
            args,
        }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// `SHUTDOWN` with or without a save, and wait for the process to go.
    fn stop(&mut self, how: &str) {
        let mut conn = self.conn();
        let _ = conn
            .stream
            .write_all(format!("*2\r\n$8\r\nSHUTDOWN\r\n${}\r\n{how}\r\n", how.len()).as_bytes());
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "server did not stop");
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Started again on its directory, with its arguments.
    fn restart(&mut self) {
        let (child, port, _) =
            common::restart_verified(|port| spawn(&self.dir, port, &self.args), self.port);
        self.child = child;
        self.port = port;
    }
}

fn spawn(dir: &Path, port: u16, extra: &[String]) -> Child {
    Command::new(kividb_bin())
        .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
        .args(["--dir", dir.to_str().unwrap(), "--loglevel", "warning"])
        .args(extra)
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to start kividb")
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_dir(tag: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("kividb-packed-lists-{tag}-"))
        .tempdir()
        .unwrap()
}

#[derive(Debug, PartialEq, Clone)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<Resp>),
    Nil,
}

impl Resp {
    fn int(&self) -> i64 {
        match self {
            Resp::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }

    fn bulk(&self) -> Vec<u8> {
        match self {
            Resp::Bulk(b) => b.clone(),
            other => panic!("not a bulk string: {other:?}"),
        }
    }

    fn text(&self) -> String {
        String::from_utf8(self.bulk()).unwrap()
    }

    fn items(&self) -> &[Resp] {
        match self {
            Resp::Array(items) => items,
            other => panic!("not an array: {other:?}"),
        }
    }

    fn ok(&self) -> bool {
        *self == Resp::Simple("OK".into())
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Conn { stream, reader }
    }

    fn send(&mut self, args: &[&[u8]]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
    }

    fn raw(&mut self, args: &[&[u8]]) -> Resp {
        self.send(args);
        self.read()
    }

    fn cmd(&mut self, args: &[&str]) -> Resp {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
    }

    fn read(&mut self) -> Resp {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap()),
            "$" if rest == "-1" => Resp::Nil,
            "*" if rest == "-1" => Resp::Nil,
            "$" => {
                let mut data = vec![0u8; rest.parse::<usize>().unwrap() + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(data.len() - 2);
                Resp::Bulk(data)
            }
            "*" | "~" => Resp::Array(
                (0..rest.parse::<usize>().unwrap())
                    .map(|_| self.read())
                    .collect(),
            ),
            _ => panic!("unexpected reply {line:?}"),
        }
    }

    /// `cmd key elements...`, in batches of 100.
    fn push(&mut self, cmd: &str, key: &str, elements: &[Vec<u8>]) -> i64 {
        let mut len = 0;
        for batch in elements.chunks(100) {
            let mut args: Vec<&[u8]> = vec![cmd.as_bytes(), key.as_bytes()];
            args.extend(batch.iter().map(Vec::as_slice));
            len = self.raw(&args).int();
        }
        len
    }

    fn encoding(&mut self, key: &str) -> String {
        self.cmd(&["OBJECT", "ENCODING", key]).text()
    }

    fn lrange(&mut self, key: &str) -> Vec<Vec<u8>> {
        match self.cmd(&["LRANGE", key, "0", "-1"]) {
            Resp::Array(items) => items.iter().map(Resp::bulk).collect(),
            other => panic!("LRANGE {key}: {other:?}"),
        }
    }

    fn info(&mut self, section: &str, field: &str) -> String {
        let text = self.cmd(&["INFO", section]).text();
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .to_string()
    }

    fn digest(&mut self, key: &str) -> Resp {
        self.cmd(&["DEBUG", "DIGEST-VALUE", key])
    }
}

type Model = VecDeque<Vec<u8>>;

/// 32-byte elements, as the memory rows have them.
fn el(i: usize) -> Vec<u8> {
    format!("element:{i:024}").into_bytes()
}

fn els(range: std::ops::Range<usize>) -> Vec<Vec<u8>> {
    range.map(el).collect()
}

const LONG: usize = 2000;

/// The shapes a list takes, each with its elements and what `OBJECT
/// ENCODING` says of it.
struct Shape {
    key: &'static str,
    model: Model,
    encoding: &'static str,
}

fn load_shapes(c: &mut Conn) -> Vec<Shape> {
    let mut shapes = Vec::new();
    // One chunk.
    let one = els(0..10);
    c.push("RPUSH", "one", &one);
    shapes.push(Shape {
        key: "one",
        model: one.into(),
        encoding: "listpack",
    });
    // Many chunks, regular: pushed at both ends.
    let mut many: Model = Model::new();
    c.push("RPUSH", "many", &els(0..300));
    many.extend(els(0..300));
    c.push("LPUSH", "many", &els(1000..1100));
    for e in els(1000..1100) {
        many.push_front(e);
    }
    shapes.push(Shape {
        key: "many",
        model: many,
        encoding: "quicklist",
    });
    // Irregular: inserts and removals in the middle.
    let mut irr: Model = els(0..300).into();
    c.push("RPUSH", "irregular", &els(0..300));
    for j in [150usize, 40, 260, 100] {
        let pivot = el(j);
        c.raw(&[b"LINSERT", b"irregular", b"BEFORE", &pivot, b"ins"]);
        let at = irr.iter().position(|e| *e == pivot).unwrap();
        irr.insert(at, b"ins".to_vec());
    }
    for j in 60..130 {
        c.raw(&[b"LREM", b"irregular", b"1", &el(j)]);
        let at = irr.iter().position(|e| *e == el(j)).unwrap();
        irr.remove(at);
    }
    shapes.push(Shape {
        key: "irregular",
        model: irr,
        encoding: "quicklist",
    });
    // Plain: an element over 1,024 bytes in it.
    let mut plain = els(0..100);
    plain.push(vec![b'L'; LONG]);
    plain.extend(els(100..150));
    c.push("RPUSH", "plain", &plain);
    shapes.push(Shape {
        key: "plain",
        model: plain.into(),
        encoding: "quicklist",
    });
    // Plain still once the long element is gone: a list never goes back.
    let mut after = vec![vec![b'L'; LONG]];
    after.extend(els(0..20));
    c.push("RPUSH", "plain-after", &after);
    c.cmd(&["LPOP", "plain-after"]);
    shapes.push(Shape {
        key: "plain-after",
        model: els(0..20).into(),
        encoding: "quicklist",
    });
    // Odd elements: empty, binary, the longest that packs.
    let odd: Vec<Vec<u8>> = vec![
        vec![],
        b"1".to_vec(),
        vec![0, 255, 13, 10, 0],
        vec![b'l'; 1024],
        vec![],
    ];
    c.push("RPUSH", "odd", &odd);
    shapes.push(Shape {
        key: "odd",
        model: odd.into(),
        encoding: "listpack",
    });
    for shape in &shapes {
        check_shape(c, shape.key, shape, "loaded");
    }
    shapes
}

fn check(c: &mut Conn, key: &str, model: &Model, what: &str) {
    let want: Vec<Vec<u8>> = model.iter().cloned().collect();
    assert_eq!(c.lrange(key), want, "{what}: {key}");
    assert_eq!(
        c.cmd(&["LLEN", key]).int(),
        model.len() as i64,
        "{what}: {key}"
    );
    let n = model.len();
    for i in [0, 1, n / 2, n.saturating_sub(1), 31, 32, 33, 63, 64, 65] {
        let got = c.cmd(&["LINDEX", key, &i.to_string()]);
        match model.get(i) {
            Some(e) => assert_eq!(got, Resp::Bulk(e.clone()), "{what}: {key}[{i}]"),
            None => assert_eq!(got, Resp::Nil, "{what}: {key}[{i}]"),
        }
        let back = -(i as i64) - 1;
        let got = c.cmd(&["LINDEX", key, &back.to_string()]);
        match n.checked_sub(i + 1).and_then(|j| model.get(j)) {
            Some(e) => assert_eq!(got, Resp::Bulk(e.clone()), "{what}: {key}[{back}]"),
            None => assert_eq!(got, Resp::Nil, "{what}: {key}[{back}]"),
        }
    }
}

fn check_shape(c: &mut Conn, key: &str, shape: &Shape, what: &str) {
    check(c, key, &shape.model, what);
    assert_eq!(c.encoding(key), shape.encoding, "{what}: {key}");
}

/// A copy of each shape's elements kept plain (made so by a long element
/// pushed first and popped), for digests that must not depend on the form.
fn reference_copies(c: &mut Conn, shapes: &[Shape]) {
    for shape in shapes {
        let key = format!("{}-ref", shape.key);
        c.push("RPUSH", &key, &[vec![b'L'; LONG]]);
        let elements: Vec<Vec<u8>> = shape.model.iter().cloned().collect();
        c.push("RPUSH", &key, &elements);
        c.cmd(&["LPOP", &key]);
        assert_eq!(c.encoding(&key), "quicklist");
    }
}

fn digests_match(c: &mut Conn, shapes: &[Shape], what: &str) {
    for shape in shapes {
        assert_eq!(
            c.digest(shape.key),
            c.digest(&format!("{}-ref", shape.key)),
            "{what}: {}",
            shape.key
        );
    }
}

/// Every list command against a list of each shape, checked against a
/// model after each, on each I/O model.
#[test]
fn every_list_command_in_each_shape() {
    for model_name in io_models() {
        let dir = temp_dir(&format!("cmds-{model_name}"));
        let server = Server::start(dir.path(), &["--io-model", model_name]);
        let mut c = server.conn();
        let shapes = load_shapes(&mut c);
        reference_copies(&mut c, &shapes);
        digests_match(&mut c, &shapes, model_name);
        for shape in shapes {
            let key = shape.key;
            let mut m = shape.model;
            let what = format!("{model_name} {key}");
            // Enough for the pops and moves below, in the shape's form.
            let pad: Vec<Vec<u8>> = (0..20).map(|i| format!("pad{i}").into_bytes()).collect();
            c.push("RPUSH", key, &pad);
            m.extend(pad);
            let n = m.len();

            // LSET at each end, the middle, inside the first chunk, out of range.
            for (i, value) in [
                (0i64, b"first".to_vec()),
                (-1, b"last".to_vec()),
                (
                    n as i64 / 2,
                    b"a longer element than the one that was there".to_vec(),
                ),
                (32, b"s".to_vec()),
            ] {
                if (i.unsigned_abs() as usize) < n || i == -1 {
                    let reply = c.raw(&[b"LSET", key.as_bytes(), i.to_string().as_bytes(), &value]);
                    assert!(reply.ok(), "{what}: LSET {i}: {reply:?}");
                    let at = if i < 0 {
                        (n as i64 + i) as usize
                    } else {
                        i as usize
                    };
                    m[at] = value;
                }
            }
            assert_eq!(
                c.cmd(&["LSET", key, &n.to_string(), "x"]),
                Resp::Error("ERR index out of range".into())
            );
            check(&mut c, key, &m, &format!("{what} LSET"));

            // LINSERT before and after; a missing pivot.
            assert_eq!(
                c.cmd(&["LINSERT", key, "BEFORE", "last", "before-last"])
                    .int(),
                m.len() as i64 + 1
            );
            let at = m.iter().position(|e| e == b"last").unwrap();
            m.insert(at, b"before-last".to_vec());
            assert_eq!(
                c.cmd(&["LINSERT", key, "AFTER", "first", "after-first"])
                    .int(),
                m.len() as i64 + 1
            );
            m.insert(1, b"after-first".to_vec());
            assert_eq!(c.cmd(&["LINSERT", key, "AFTER", "absent", "x"]).int(), -1);
            check(&mut c, key, &m, &format!("{what} LINSERT"));

            // LREM by count, both ways and all.
            c.cmd(&["RPUSH", key, "x", "y", "x", "x"]);
            c.cmd(&["LPUSH", key, "x", "z"]);
            for e in ["x", "y", "x", "x"] {
                m.push_back(e.as_bytes().to_vec());
            }
            for e in ["x", "z"] {
                m.push_front(e.as_bytes().to_vec());
            }
            assert_eq!(c.cmd(&["LREM", key, "1", "x"]).int(), 1);
            let at = m.iter().position(|e| e == b"x").unwrap();
            m.remove(at);
            assert_eq!(c.cmd(&["LREM", key, "-1", "x"]).int(), 1);
            let at = m.iter().rposition(|e| e == b"x").unwrap();
            m.remove(at);
            assert_eq!(c.cmd(&["LREM", key, "0", "x"]).int(), 2);
            m.retain(|e| e != b"x");
            assert_eq!(c.cmd(&["LREM", key, "0", "absent"]).int(), 0);
            check(&mut c, key, &m, &format!("{what} LREM"));

            // LPOS with RANK, COUNT and MAXLEN.
            let pos = |m: &Model, e: &[u8]| -> Vec<i64> {
                m.iter()
                    .enumerate()
                    .filter(|(_, x)| x.as_slice() == e)
                    .map(|(i, _)| i as i64)
                    .collect()
            };
            let probe = m[m.len() / 2].clone();
            let all = pos(&m, &probe);
            assert_eq!(
                c.raw(&[b"LPOS", key.as_bytes(), &probe]),
                Resp::Int(all[0]),
                "{what} LPOS"
            );
            assert_eq!(
                c.raw(&[b"LPOS", key.as_bytes(), &probe, b"RANK", b"-1"]),
                Resp::Int(*all.last().unwrap()),
                "{what} LPOS RANK -1"
            );
            let got = c.raw(&[b"LPOS", key.as_bytes(), b"y", b"COUNT", b"0"]);
            let want: Vec<Resp> = pos(&m, b"y").into_iter().map(Resp::Int).collect();
            assert_eq!(got, Resp::Array(want), "{what} LPOS COUNT 0");
            let got = c.raw(&[
                b"LPOS",
                key.as_bytes(),
                b"y",
                b"COUNT",
                b"0",
                b"MAXLEN",
                b"5",
            ]);
            let want: Vec<Resp> = pos(&m, b"y")
                .into_iter()
                .filter(|&i| i < 5)
                .map(Resp::Int)
                .collect();
            assert_eq!(got, Resp::Array(want), "{what} LPOS MAXLEN");

            // Pops with and without counts, LMPOP, and blocking pops served at once.
            assert_eq!(c.cmd(&["LPOP", key]), Resp::Bulk(m.pop_front().unwrap()));
            assert_eq!(c.cmd(&["RPOP", key]), Resp::Bulk(m.pop_back().unwrap()));
            let got = c.cmd(&["LPOP", key, "3"]);
            let want: Vec<Resp> = (0..3).map(|_| Resp::Bulk(m.pop_front().unwrap())).collect();
            assert_eq!(got, Resp::Array(want));
            let got = c.cmd(&["RPOP", key, "2"]);
            let want: Vec<Resp> = (0..2).map(|_| Resp::Bulk(m.pop_back().unwrap())).collect();
            assert_eq!(got, Resp::Array(want));
            let got = c.cmd(&["LMPOP", "2", "absent", key, "LEFT", "COUNT", "2"]);
            let want: Vec<Resp> = (0..2).map(|_| Resp::Bulk(m.pop_front().unwrap())).collect();
            assert_eq!(
                got,
                Resp::Array(vec![Resp::Bulk(key.into()), Resp::Array(want)])
            );
            let got = c.cmd(&["BLPOP", key, "0"]);
            assert_eq!(
                got,
                Resp::Array(vec![
                    Resp::Bulk(key.into()),
                    Resp::Bulk(m.pop_front().unwrap())
                ])
            );
            let got = c.cmd(&["BRPOP", "absent", key, "0"]);
            assert_eq!(
                got,
                Resp::Array(vec![
                    Resp::Bulk(key.into()),
                    Resp::Bulk(m.pop_back().unwrap())
                ])
            );
            let got = c.cmd(&["BLMPOP", "0", "1", key, "RIGHT", "COUNT", "2"]);
            let want: Vec<Resp> = (0..2).map(|_| Resp::Bulk(m.pop_back().unwrap())).collect();
            assert_eq!(
                got,
                Resp::Array(vec![Resp::Bulk(key.into()), Resp::Array(want)])
            );
            check(&mut c, key, &m, &format!("{what} pops"));

            // Moves: rotation on the key, and to another key, each way.
            assert_eq!(
                c.cmd(&["RPOPLPUSH", key, key]),
                Resp::Bulk(m.back().unwrap().clone())
            );
            let e = m.pop_back().unwrap();
            m.push_front(e);
            assert_eq!(
                c.cmd(&["LMOVE", key, key, "LEFT", "RIGHT"]),
                Resp::Bulk(m.front().unwrap().clone())
            );
            let e = m.pop_front().unwrap();
            m.push_back(e);
            let dst = format!("{key}-dst");
            let mut d = Model::new();
            let e = m.pop_front().unwrap();
            assert_eq!(
                c.cmd(&["LMOVE", key, &dst, "LEFT", "LEFT"]),
                Resp::Bulk(e.clone())
            );
            d.push_front(e);
            let e = m.pop_back().unwrap();
            assert_eq!(
                c.cmd(&["BLMOVE", key, &dst, "RIGHT", "RIGHT", "0"]),
                Resp::Bulk(e.clone())
            );
            d.push_back(e);
            let e = m.pop_back().unwrap();
            assert_eq!(
                c.cmd(&["BRPOPLPUSH", key, &dst, "0"]),
                Resp::Bulk(e.clone())
            );
            d.push_front(e);
            check(&mut c, key, &m, &format!("{what} moves"));
            check(&mut c, &dst, &d, &format!("{what} moves dst"));

            // LPUSHX/RPUSHX, then LTRIM from both ends.
            c.cmd(&["LPUSHX", key, "a", "b"]);
            m.push_front(b"a".to_vec());
            m.push_front(b"b".to_vec());
            c.cmd(&["RPUSHX", key, "c"]);
            m.push_back(b"c".to_vec());
            assert_eq!(c.cmd(&["LPUSHX", "absent", "a"]).int(), 0);
            check(&mut c, key, &m, &format!("{what} PUSHX"));
            assert!(c.cmd(&["LTRIM", key, "1", "-2"]).ok());
            m.pop_front();
            m.pop_back();
            check(&mut c, key, &m, &format!("{what} LTRIM"));

            // SORT ALPHA, COPY, RENAME, DEL.
            let mut sorted: Vec<Vec<u8>> = m.iter().cloned().collect();
            sorted.sort();
            let got: Vec<Vec<u8>> = c
                .cmd(&["SORT", key, "ALPHA"])
                .items()
                .iter()
                .map(Resp::bulk)
                .collect();
            assert_eq!(got, sorted, "{what} SORT");
            let sorted_key = format!("{key}-sorted");
            assert_eq!(
                c.cmd(&["SORT", key, "ALPHA", "STORE", &sorted_key]).int(),
                m.len() as i64
            );
            check(
                &mut c,
                &sorted_key,
                &sorted.into(),
                &format!("{what} SORT STORE"),
            );
            let copy = format!("{key}-copy");
            assert_eq!(c.cmd(&["COPY", key, &copy]).int(), 1);
            c.cmd(&["RPUSH", &copy, "only in the copy"]);
            check(&mut c, key, &m, &format!("{what} COPY"));
            let renamed = format!("{key}-renamed");
            assert!(c.cmd(&["RENAME", key, &renamed]).ok());
            check(&mut c, &renamed, &m, &format!("{what} RENAME"));
            assert!(c.cmd(&["MEMORY", "USAGE", &renamed]).int() > 0);
            assert_eq!(c.cmd(&["DEL", &renamed]).int(), 1);
            assert_eq!(c.cmd(&["UNLINK", &copy]).int(), 1);
            assert_eq!(c.cmd(&["EXISTS", key, &renamed, &copy]).int(), 0);
        }
    }
}

/// `OBJECT ENCODING` through every change of form: one chunk to many and
/// back, packed to plain by a push, a set and an insert, and an emptied
/// list deleted and made new.
#[test]
fn every_change_of_form() {
    let dir = temp_dir("forms");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    for i in 0..64 {
        c.raw(&[b"RPUSH", b"grow", &el(i)]);
        assert_eq!(c.encoding("grow"), "listpack", "{i}");
    }
    c.raw(&[b"RPUSH", b"grow", &el(64)]);
    assert_eq!(c.encoding("grow"), "quicklist");
    // Back to one chunk when the second empties.
    c.cmd(&["RPOP", "grow"]);
    assert_eq!(c.encoding("grow"), "listpack");
    check(&mut c, "grow", &els(0..64).into(), "grow");
    for how in ["RPUSH", "LPUSH", "LSET", "LINSERT", "RPUSHX", "LPUSHX"] {
        let key = format!("conv-{how}");
        c.push("RPUSH", &key, &els(0..10));
        let mut m: Model = els(0..10).into();
        let long = vec![b'L'; 1025];
        let longest = vec![b'l'; 1024];
        c.raw(&[b"RPUSH", key.as_bytes(), &longest]);
        m.push_back(longest);
        assert_eq!(
            c.encoding(&key),
            "listpack",
            "{how}: 1,024 bytes still pack"
        );
        match how {
            "RPUSH" | "RPUSHX" => {
                c.raw(&[how.as_bytes(), key.as_bytes(), b"a", &long, b"b"]);
                m.extend([b"a".to_vec(), long.clone(), b"b".to_vec()]);
            }
            "LPUSH" | "LPUSHX" => {
                c.raw(&[how.as_bytes(), key.as_bytes(), b"a", &long, b"b"]);
                for e in [b"a".to_vec(), long.clone(), b"b".to_vec()] {
                    m.push_front(e);
                }
            }
            "LSET" => {
                assert!(c.raw(&[b"LSET", key.as_bytes(), b"5", &long]).ok());
                m[5] = long.clone();
            }
            _ => {
                c.raw(&[b"LINSERT", key.as_bytes(), b"AFTER", &el(7), &long]);
                m.insert(8, long.clone());
            }
        }
        assert_eq!(c.encoding(&key), "quicklist", "{how}");
        check(&mut c, &key, &m, how);
        // It stays so with the long element gone, and goes on working.
        c.raw(&[b"LREM", key.as_bytes(), b"0", &long]);
        m.retain(|e| *e != long);
        c.cmd(&["RPUSH", &key, "after"]);
        m.push_back(b"after".to_vec());
        assert_eq!(c.encoding(&key), "quicklist", "{how}");
        check(&mut c, &key, &m, &format!("{how} after"));
        // Emptied, it is deleted; the next list of that name is packed.
        c.cmd(&["LTRIM", &key, "1", "0"]);
        assert_eq!(c.cmd(&["EXISTS", &key]).int(), 0);
        c.cmd(&["RPUSH", &key, "new"]);
        assert_eq!(c.encoding(&key), "listpack", "{how}");
    }
}

/// Snapshot, AOF replay and AOF rewrite: every shape back, in order, and
/// its digest that of a plain copy.
#[test]
fn save_aof_and_rewrite_round_trips() {
    // Snapshot.
    let dir = temp_dir("save");
    let mut server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let shapes = load_shapes(&mut c);
    reference_copies(&mut c, &shapes);
    let big = els(0..100_000);
    c.push("RPUSH", "big", &big);
    assert!(c.cmd(&["SAVE"]).ok());
    server.stop("NOSAVE");
    server.restart();
    let mut c = server.conn();
    for shape in &shapes {
        // Loaded in full chunks: a list that was irregular is regular now,
        // and a plain one is packed again when its elements all fit.
        check(&mut c, shape.key, &shape.model, "snapshot");
    }
    assert_eq!(c.encoding("plain-after"), "listpack");
    assert_eq!(c.encoding("plain"), "quicklist");
    assert_eq!(c.encoding("one"), "listpack");
    assert_eq!(c.lrange("big"), big);
    digests_match(&mut c, &shapes, "snapshot");

    // AOF replay, with conversions, trims and pops in the log.
    let dir = temp_dir("aof");
    let mut server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    let shapes = load_shapes(&mut c);
    reference_copies(&mut c, &shapes);
    for i in 0..600 {
        c.raw(&[b"LPUSH", b"capped", &el(i)]);
        c.cmd(&["LTRIM", "capped", "0", "99"]);
    }
    let capped = c.lrange("capped");
    server.stop("NOSAVE");
    server.restart();
    let mut c = server.conn();
    for shape in &shapes {
        check_shape(&mut c, shape.key, shape, "aof replay");
    }
    digests_match(&mut c, &shapes, "aof replay");
    assert_eq!(c.lrange("capped"), capped);

    // AOF rewrite, then replay of what it wrote.
    assert!(matches!(c.cmd(&["BGREWRITEAOF"]), Resp::Simple(_)));
    let deadline = Instant::now() + Duration::from_secs(60);
    while c.info("persistence", "aof_rewrite_in_progress") != "0"
        || c.info("persistence", "aof_rewrite_scheduled") != "0"
    {
        assert!(Instant::now() < deadline, "rewrite did not end");
        thread::sleep(Duration::from_millis(50));
    }
    server.stop("NOSAVE");
    server.restart();
    let mut c = server.conn();
    for shape in &shapes {
        check(&mut c, shape.key, &shape.model, "aof rewrite");
    }
    digests_match(&mut c, &shapes, "aof rewrite");
    assert_eq!(c.lrange("capped"), capped);
}

/// DUMP/RESTORE within a server and MIGRATE to another.
#[test]
fn dump_restore_and_migrate() {
    let dir = temp_dir("dump");
    let server = Server::start(dir.path(), &[]);
    let other_dir = temp_dir("dump-to");
    let other = Server::start(other_dir.path(), &[]);
    let mut c = server.conn();
    let shapes = load_shapes(&mut c);
    for shape in &shapes {
        let key = shape.key;
        let payload = c.cmd(&["DUMP", key]).bulk();
        let copy = format!("{key}-restored");
        assert!(c.raw(&[b"RESTORE", copy.as_bytes(), b"0", &payload]).ok());
        check(&mut c, &copy, &shape.model, "restored");
        assert_eq!(c.digest(&copy), c.digest(key), "{key}");
        let reply = c.cmd(&[
            "MIGRATE",
            "127.0.0.1",
            &other.port.to_string(),
            key,
            "0",
            "5000",
            "COPY",
        ]);
        assert!(reply.ok(), "{key}: {reply:?}");
    }
    let mut o = other.conn();
    for shape in &shapes {
        check(&mut o, shape.key, &shape.model, "migrated");
        assert_eq!(o.digest(shape.key), c.digest(shape.key), "{}", shape.key);
    }
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

fn synced(m: &mut Conn, r: &mut Conn) {
    let offset = m.info("replication", "master_repl_offset");
    wait_for("the offset", || {
        r.info("replication", "slave_repl_offset") == offset
    });
}

/// A replica's full sync and the stream after it: every shape, and every
/// change of form made on the master after the sync.
#[test]
fn replica_full_sync_and_stream() {
    let dir = temp_dir("master");
    let master = Server::start(dir.path(), &[]);
    let mut m = master.conn();
    let mut shapes = load_shapes(&mut m);
    let rdir = temp_dir("replica");
    let port = master.port.to_string();
    let replica = Server::start(rdir.path(), &["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    wait_for("the link", || {
        r.info("replication", "master_link_status") == "up"
    });
    synced(&mut m, &mut r);
    for shape in &shapes {
        check(&mut r, shape.key, &shape.model, "full sync");
    }

    // Changes of form on the master, streamed.
    m.push("RPUSH", "one", &els(10..100));
    shapes[0].model.extend(els(10..100));
    m.raw(&[b"LSET", b"many", b"7", &[b'L'; 1500]]);
    shapes[1].model[7] = vec![b'L'; 1500];
    m.raw(&[b"LINSERT", b"irregular", b"AFTER", &el(200), b"streamed"]);
    let at = shapes[2].model.iter().position(|e| *e == el(200)).unwrap();
    shapes[2].model.insert(at + 1, b"streamed".to_vec());
    m.cmd(&["LTRIM", "plain", "3", "-3"]);
    let p = &mut shapes[3].model;
    *p = p.range(3..p.len() - 2).cloned().collect();
    m.cmd(&["RPOPLPUSH", "odd", "plain-after"]);
    let e = shapes[5].model.pop_back().unwrap();
    shapes[4].model.push_front(e);
    synced(&mut m, &mut r);
    for shape in &shapes {
        check(&mut r, shape.key, &shape.model, "stream");
        assert_eq!(r.digest(shape.key), m.digest(shape.key), "{}", shape.key);
    }
    assert_eq!(r.cmd(&["DEBUG", "DIGEST"]), m.cmd(&["DEBUG", "DIGEST"]));
}

/// A client blocked on an empty key is served by a push to a packed list,
/// in the order the clients blocked; BLMOVE between a packed list and a
/// plain one, each way.
#[test]
fn blocked_clients_are_served_from_packed_lists() {
    for model_name in io_models() {
        let dir = temp_dir(&format!("block-{model_name}"));
        let server = Server::start(dir.path(), &["--io-model", model_name]);
        let mut c = server.conn();
        let mut waiters: Vec<Conn> = (0..3).map(|_| server.conn()).collect();
        for (i, w) in waiters.iter_mut().enumerate() {
            w.send(&[b"BLPOP", b"q", b"10"]);
            // In order: each blocked before the next.
            wait_for("the client to block", || {
                c.info("clients", "blocked_clients") == (i + 1).to_string()
            });
        }
        assert_eq!(c.cmd(&["RPUSH", "q", "a", "b", "c", "d"]).int(), 4);
        for (w, want) in waiters.iter_mut().zip(["a", "b", "c"]) {
            assert_eq!(
                w.read(),
                Resp::Array(vec![Resp::Bulk(b"q".to_vec()), Resp::Bulk(want.into())]),
                "{model_name}"
            );
        }
        assert_eq!(c.lrange("q"), vec![b"d".to_vec()]);
        assert_eq!(c.encoding("q"), "listpack");

        // BLMOVE: packed source to a plain destination, and back.
        c.push("RPUSH", "plain", &[vec![b'L'; LONG], b"p".to_vec()]);
        let mut w = server.conn();
        w.send(&[b"BLMOVE", b"empty", b"plain", b"LEFT", b"LEFT", b"10"]);
        wait_for("the client to block", || {
            c.info("clients", "blocked_clients") == "1"
        });
        c.cmd(&["RPUSH", "empty", "moved"]);
        assert_eq!(w.read(), Resp::Bulk(b"moved".to_vec()));
        assert_eq!(c.lrange("plain")[0], b"moved".to_vec());
        assert_eq!(c.cmd(&["EXISTS", "empty"]).int(), 0);
        w.send(&[b"BLMOVE", b"plain", b"packed", b"RIGHT", b"LEFT", b"10"]);
        assert_eq!(w.read(), Resp::Bulk(b"p".to_vec()));
        w.send(&[b"BLMOVE", b"plain", b"packed", b"RIGHT", b"LEFT", b"10"]);
        assert_eq!(w.read(), Resp::Bulk(vec![b'L'; LONG]));
        assert_eq!(c.encoding("packed"), "quicklist");
        assert_eq!(
            c.lrange("packed"),
            vec![vec![b'L'; LONG], b"p".to_vec()],
            "{model_name}"
        );
        assert_eq!(c.lrange("plain"), vec![b"moved".to_vec()]);
    }
}

/// The queue and the capped list, over a long run: memory stays level once
/// warmed up.
#[test]
fn queues_and_capped_lists_do_not_grow() {
    let dir = temp_dir("level");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let used = |c: &mut Conn| -> i64 { c.info("memory", "used_memory").parse().unwrap() };
    for (name, push, pop) in [
        ("rq", "RPUSH", Some("LPOP")),
        ("lq", "LPUSH", Some("RPOP")),
        ("capped", "LPUSH", None),
    ] {
        let round = |c: &mut Conn, from: usize| {
            for i in from..from + 20_000 {
                c.raw(&[push.as_bytes(), name.as_bytes(), &el(i)]);
                match pop {
                    Some(pop) if i >= 1000 => {
                        c.cmd(&[pop, name]);
                    }
                    Some(_) => {}
                    None => {
                        c.cmd(&["LTRIM", name, "0", "999"]);
                    }
                }
            }
        };
        round(&mut c, 0);
        let warm = used(&mut c);
        round(&mut c, 20_000);
        round(&mut c, 40_000);
        let after = used(&mut c);
        assert_eq!(c.cmd(&["LLEN", name]).int(), 1000, "{name}");
        // 1,000 elements of 32 bytes are about 37 kB; a list that grew
        // would have grown by far more than 8 kB over 40,000 pushes.
        assert!(
            (after - warm).abs() < 8 * 1024,
            "{name}: used_memory {warm} -> {after}"
        );
        c.cmd(&["DEL", name]);
    }
}

/// Small lists under allkeys-lru: eviction fills the room, neither over
/// nor under it.
#[test]
fn eviction_counts_the_chunks() {
    let dir = temp_dir("evict");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .ok()
    );
    let elements = els(0..10);
    c.push("RPUSH", "probe", &elements);
    assert_eq!(c.encoding("probe"), "listpack");
    let each = c.cmd(&["MEMORY", "USAGE", "probe"]).int();
    c.cmd(&["DEL", "probe"]);

    let room: i64 = 2 << 20;
    let base: i64 = c.info("memory", "used_memory").parse().unwrap();
    let limit = base + room;
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .ok()
    );
    let keys = 3 * room / each;
    for i in 0..keys {
        c.push("RPUSH", &format!("k{i}"), &elements);
    }
    let used: i64 = c.info("memory", "used_memory").parse().unwrap();
    let evicted: i64 = c.info("stats", "evicted_keys").parse().unwrap();
    c.cmd(&["CONFIG", "SET", "maxmemory", "0"]);
    assert!(evicted > 0);
    assert!(
        used <= limit + 64 * 1024,
        "used {used} over the limit {limit}"
    );
    let kept = (0..keys)
        .filter(|i| c.cmd(&["EXISTS", &format!("k{i}")]).int() == 1)
        .count() as i64;
    let fitted = kept * each;
    assert!(
        fitted > room * 6 / 10 && fitted < room * 14 / 10,
        "{kept} lists of {each} bytes in {room}"
    );
    for i in (0..keys).step_by(97) {
        let key = format!("k{i}");
        if c.cmd(&["EXISTS", &key]).int() == 1 {
            assert_eq!(c.lrange(&key), elements);
        }
    }
}

/// `MEMORY USAGE` of a packed list is what its chunks take: within a few
/// percent of what creating it added to `used_memory`.
#[test]
fn memory_usage_is_what_the_chunks_take() {
    let dir = temp_dir("usage");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    for (n, key, keys) in [
        (10usize, "ten", 5000i64),
        (100, "hundred", 1000),
        (10_000, "ten-thousand", 50),
    ] {
        let before: i64 = c.info("memory", "used_memory").parse().unwrap();
        // Many keys of the size (megabytes in all), so a key's share of the
        // table, and what threads have not yet counted, even out.
        for k in 0..keys {
            c.push("RPUSH", &format!("{key}:{k}"), &els(0..n));
        }
        let after: i64 = c.info("memory", "used_memory").parse().unwrap();
        let each = (after - before) / keys;
        let usage = c.cmd(&["MEMORY", "USAGE", &format!("{key}:7")]).int();
        assert!(
            (usage - each).abs() <= each / 10 + 64,
            "{n} elements: MEMORY USAGE {usage}, used_memory grew {each} a key"
        );
    }
}

/// Pushes, a pop and rotations inside `MULTI`/`EXEC`, on a packed list.
#[test]
fn a_transaction_pushes_and_pops_on_a_packed_list() {
    let dir = temp_dir("multi");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    c.push("RPUSH", "q", &els(0..100));
    assert!(c.cmd(&["MULTI"]).ok());
    let queued: [&[&str]; 5] = [
        &["RPUSH", "q", "a", "b"],
        &["LPOP", "q"],
        &["RPOPLPUSH", "q", "q"],
        &["LMOVE", "q", "q", "LEFT", "RIGHT"],
        &["LRANGE", "q", "0", "2"],
    ];
    for cmd in queued {
        assert_eq!(c.cmd(cmd), Resp::Simple("QUEUED".into()));
    }
    let replies = c.cmd(&["EXEC"]);
    let replies = replies.items();
    assert_eq!(replies[0], Resp::Int(102));
    assert_eq!(replies[1], Resp::Bulk(el(0)));
    assert_eq!(replies[2], Resp::Bulk(b"b".to_vec()));
    assert_eq!(replies[3], Resp::Bulk(b"b".to_vec()));
    assert_eq!(
        replies[4],
        Resp::Array(vec![
            Resp::Bulk(el(1)),
            Resp::Bulk(el(2)),
            Resp::Bulk(el(3))
        ])
    );
    let mut m: Model = els(1..100).into();
    m.push_back(b"a".to_vec());
    m.push_back(b"b".to_vec());
    check(&mut c, "q", &m, "after EXEC");
    assert_eq!(c.encoding("q"), "quicklist");
}

/// A script that pushes and pops on a packed list.
#[cfg(feature = "lua")]
#[test]
fn a_script_pushes_and_pops_on_a_packed_list() {
    let dir = temp_dir("script");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    c.push("RPUSH", "s", &els(0..70));
    let reply = c.cmd(&[
        "EVAL",
        "redis.call('RPUSH', KEYS[1], 'x', 'y') redis.call('LPOP', KEYS[1]) \
         return redis.call('RPOPLPUSH', KEYS[1], KEYS[1])",
        "1",
        "s",
    ]);
    assert_eq!(reply, Resp::Bulk(b"y".to_vec()));
    let mut m: Model = Model::new();
    m.push_back(b"y".to_vec());
    m.extend(els(1..70));
    m.push_back(b"x".to_vec());
    check(&mut c, "s", &m, "after the script");
}
