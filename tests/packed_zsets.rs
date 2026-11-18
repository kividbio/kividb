// Small sorted sets kept in one block: every sorted-set command gives the
// same answer whatever the form, across every change of form, and the
// members come back the same, in order, scores bit for bit, after a save, an
// AOF replay, an AOF rewrite, a full sync and the stream after it,
// DUMP/RESTORE and MIGRATE. Each test runs its own server processes.

mod common;

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
        .prefix(&format!("kividb-packed-zsets-{tag}-"))
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

    /// `ZADD key score member ...` of `pairs`, a hundred a command.
    fn zadd(&mut self, key: &str, pairs: &[(Vec<u8>, String)]) {
        for chunk in pairs.chunks(100) {
            let mut args: Vec<&[u8]> = vec![b"ZADD", key.as_bytes()];
            for (m, s) in chunk {
                args.push(s.as_bytes());
                args.push(m);
            }
            assert!(matches!(self.raw(&args), Resp::Int(_)), "{key}");
        }
    }

    fn encoding(&mut self, key: &str) -> String {
        self.cmd(&["OBJECT", "ENCODING", key]).text()
    }

    /// `ZRANGE key 0 -1 WITHSCORES`, scores read as floats.
    fn all(&mut self, key: &str) -> Members {
        flat_pairs(&self.cmd(&["ZRANGE", key, "0", "-1", "WITHSCORES"]))
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

/// Members and their scores, in a set's order.
type Members = Vec<(Vec<u8>, f64)>;

fn score_of(text: &[u8]) -> f64 {
    match text {
        b"inf" => f64::INFINITY,
        b"-inf" => f64::NEG_INFINITY,
        other => std::str::from_utf8(other).unwrap().parse().unwrap(),
    }
}

fn flat_pairs(reply: &Resp) -> Members {
    reply
        .items()
        .chunks(2)
        .map(|p| (p[0].bulk(), score_of(&p[1].bulk())))
        .collect()
}

/// `(member, score)` in a sorted set's order.
fn in_order(pairs: &[(Vec<u8>, String)]) -> Members {
    let mut last = std::collections::BTreeMap::new();
    for (m, s) in pairs {
        last.insert(m.clone(), score_of(s.as_bytes()) + 0.0);
    }
    let mut all: Members = last.into_iter().collect();
    all.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    all
}

fn same_members(got: &Members, want: &Members, what: &str) {
    assert_eq!(got.len(), want.len(), "{what}: count");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        assert_eq!(g.0, w.0, "{what}: member {i}");
        assert_eq!(g.1.to_bits(), w.1.to_bits(), "{what}: score of member {i}");
    }
}

/// `n` members of 14 bytes, scores repeating, as the memory rows have them.
fn members(n: usize, prefix: &str) -> Vec<(Vec<u8>, String)> {
    (0..n)
        .map(|i| {
            (
                format!("{prefix}:{i:08}").into_bytes(),
                format!("{}", (i % 17) as i64 - 8),
            )
        })
        .collect()
}

#[derive(Clone)]
struct Form {
    key: &'static str,
    pairs: Vec<(Vec<u8>, String)>,
    form: &'static str,
}

const EDGES: [&str; 22] = [
    "-inf",
    "+inf",
    "-0",
    "0",
    "1",
    "-1",
    "1.5",
    "-1.5",
    "1e308",
    "-1e308",
    "5e-324",
    "247",
    "248",
    "-129",
    "65536",
    "1700000000",
    "-1700000000123",
    "36028797018963968",
    "9007199254740993",
    "113000000000001000",
    "0.1",
    "3.141592653589793",
];

/// Sorted sets of every form and every boundary between them, at Redis's
/// limits (`zset-max-listpack-entries` 128, `zset-max-listpack-value` 64).
fn forms() -> Vec<Form> {
    let with = |n: usize, extra: (Vec<u8>, String)| {
        let mut p = members(n, "member");
        p.push(extra);
        p
    };
    let form = |key, pairs, form| Form { key, pairs, form };
    vec![
        form("z10", members(10, "member"), "listpack"),
        form("z128", members(128, "member"), "listpack"),
        form("z129", members(129, "member"), "skiplist"),
        form(
            "member64",
            with(5, (vec![b'M'; 64], "1".into())),
            "listpack",
        ),
        form(
            "member65",
            with(5, (vec![b'M'; 65], "1".into())),
            "skiplist",
        ),
        form(
            "edges",
            EDGES
                .iter()
                .enumerate()
                .map(|(i, s)| (format!("e{i:02}").into_bytes(), s.to_string()))
                .collect(),
            "listpack",
        ),
        form(
            "binary",
            vec![
                (b"".to_vec(), "1".into()),
                (b"\xff\x00\x01".to_vec(), "1".into()),
                (b"a\x00".to_vec(), "1".into()),
                (b"\r\n".to_vec(), "1".into()),
                (b"\x00".to_vec(), "2".into()),
            ],
            "listpack",
        ),
        form("big", members(1000, "big"), "skiplist"),
    ]
}

fn load_forms(c: &mut Conn) -> Vec<Form> {
    let forms = forms();
    for form in &forms {
        c.zadd(form.key, &form.pairs);
        assert_eq!(c.encoding(form.key), form.form, "{}", form.key);
    }
    forms
}

fn check_form(c: &mut Conn, key: &str, form: &Form, what: &str) {
    same_members(
        &c.all(key),
        &in_order(&form.pairs),
        &format!("{what}: {key}"),
    );
    assert_eq!(c.encoding(key), form.form, "{what}: {key}");
}

fn check_forms(c: &mut Conn, forms: &[Form], what: &str) {
    for form in forms {
        check_form(c, form.key, form, what);
    }
}

/// Reference copies, kept as a table and a tree, of each form: their
/// digest is what each form's must be.
fn reference_copies(c: &mut Conn, forms: &[Form]) {
    assert!(
        c.cmd(&["CONFIG", "SET", "zset-max-listpack-entries", "0"])
            .ok()
    );
    for form in forms {
        let key = format!("{}-ref", form.key);
        c.zadd(&key, &form.pairs);
        assert_eq!(c.encoding(&key), "skiplist");
    }
    assert!(
        c.cmd(&["CONFIG", "SET", "zset-max-listpack-entries", "128"])
            .ok()
    );
}

fn digests_match(c: &mut Conn, forms: &[Form], what: &str) {
    for form in forms {
        assert_eq!(
            c.digest(form.key),
            c.digest(&format!("{}-ref", form.key)),
            "{what}: {}",
            form.key
        );
    }
}

/// Every reading command against a set of each form, checked against the
/// members it was given, and the writing ones against a copy of it, on
/// each I/O model.
#[test]
fn every_zset_command_in_each_form() {
    for model in io_models() {
        let dir = temp_dir("commands");
        let server = Server::start(dir.path(), &["--io-model", model]);
        let mut c = server.conn();
        let forms = load_forms(&mut c);
        for form in &forms {
            let key = form.key;
            let want = in_order(&form.pairs);
            let n = want.len();
            let what = format!("{model} {key}");
            check_form(&mut c, key, form, &what);
            assert_eq!(c.cmd(&["ZCARD", key]).int(), n as i64, "{what}");
            // By rank, from both ends, and each member's rank and score.
            for (i, (m, s)) in want.iter().enumerate() {
                let rank = c.raw(&[b"ZRANK", key.as_bytes(), m, b"WITHSCORE"]);
                assert_eq!(rank.items()[0].int(), i as i64, "{what}: rank of {m:?}");
                assert_eq!(score_of(&rank.items()[1].bulk()).to_bits(), s.to_bits());
                let rev = c.raw(&[b"ZREVRANK", key.as_bytes(), m]).int();
                assert_eq!(rev, (n - 1 - i) as i64, "{what}");
                let score = score_of(&c.raw(&[b"ZSCORE", key.as_bytes(), m]).bulk());
                assert_eq!(score.to_bits(), s.to_bits(), "{what}");
            }
            assert_eq!(c.cmd(&["ZSCORE", key, "absent"]), Resp::Nil);
            assert_eq!(c.cmd(&["ZRANK", key, "absent"]), Resp::Nil);
            let mscore = c.cmd(&["ZMSCORE", key, "absent", "absent"]);
            assert_eq!(mscore.items().len(), 2);
            for (start, stop) in [
                (0i64, 9i64),
                (3, 3),
                (-5, -1),
                (0, -1),
                (5, 2),
                (-1000, 1000),
            ] {
                let range = flat_pairs(&c.cmd(&[
                    "ZRANGE",
                    key,
                    &start.to_string(),
                    &stop.to_string(),
                    "WITHSCORES",
                ]));
                let norm = |x: i64| if x < 0 { (n as i64 + x).max(0) } else { x };
                let (a, b) = (norm(start), norm(stop).min(n as i64 - 1));
                let expect: Members = if a > b {
                    vec![]
                } else {
                    want[a as usize..=b as usize].to_vec()
                };
                same_members(&range, &expect, &format!("{what} ZRANGE {start} {stop}"));
                let rev = flat_pairs(&c.cmd(&[
                    "ZREVRANGE",
                    key,
                    &start.to_string(),
                    &stop.to_string(),
                    "WITHSCORES",
                ]));
                let mut back = want.clone();
                back.reverse();
                let expect: Members = if a > b {
                    vec![]
                } else {
                    back[a as usize..=b as usize].to_vec()
                };
                same_members(&rev, &expect, &format!("{what} ZREVRANGE {start} {stop}"));
            }
            // By score, each bound open and closed, LIMIT, REV.
            for (lo, hi) in [
                ("-inf", "+inf"),
                ("-1", "3"),
                ("(-1", "(3"),
                ("0", "0"),
                ("(0", "1e308"),
            ] {
                let lo_v = score_of(
                    lo.trim_start_matches('(')
                        .trim_start_matches('+')
                        .as_bytes(),
                );
                let hi_v = score_of(
                    hi.trim_start_matches('(')
                        .trim_start_matches('+')
                        .as_bytes(),
                );
                let inside = |s: f64| {
                    (if lo.starts_with('(') {
                        s > lo_v
                    } else {
                        s >= lo_v
                    }) && (if hi.starts_with('(') {
                        s < hi_v
                    } else {
                        s <= hi_v
                    })
                };
                let expect: Members = want.iter().filter(|(_, s)| inside(*s)).cloned().collect();
                let got = flat_pairs(&c.cmd(&["ZRANGEBYSCORE", key, lo, hi, "WITHSCORES"]));
                same_members(&got, &expect, &format!("{what} BYSCORE {lo} {hi}"));
                assert_eq!(c.cmd(&["ZCOUNT", key, lo, hi]).int(), expect.len() as i64);
                let got = flat_pairs(&c.cmd(&[
                    "ZRANGE",
                    key,
                    lo,
                    hi,
                    "BYSCORE",
                    "LIMIT",
                    "2",
                    "3",
                    "WITHSCORES",
                ]));
                let limited: Members = expect.iter().skip(2).take(3).cloned().collect();
                same_members(&got, &limited, &format!("{what} BYSCORE LIMIT"));
                let got = flat_pairs(&c.cmd(&[
                    "ZRANGE",
                    key,
                    hi,
                    lo,
                    "BYSCORE",
                    "REV",
                    "LIMIT",
                    "1",
                    "2",
                    "WITHSCORES",
                ]));
                let limited: Members = expect.iter().rev().skip(1).take(2).cloned().collect();
                same_members(&got, &limited, &format!("{what} BYSCORE REV"));
            }
            // By lex: every member, and a part.
            let lex: Vec<Vec<u8>> = c
                .cmd(&["ZRANGE", key, "-", "+", "BYLEX"])
                .items()
                .iter()
                .map(Resp::bulk)
                .collect();
            assert_eq!(lex, want.iter().map(|(m, _)| m.clone()).collect::<Vec<_>>());
            assert_eq!(c.cmd(&["ZLEXCOUNT", key, "-", "+"]).int(), n as i64);
            // ZSCAN: a block whole, in order, cursor 0; else all over pages.
            let mut seen = Vec::new();
            let mut cursor = "0".to_string();
            loop {
                let reply = c.cmd(&["ZSCAN", key, &cursor, "COUNT", "5"]);
                cursor = reply.items()[0].text();
                seen.extend(flat_pairs(&reply.items()[1]));
                if form.form == "listpack" {
                    assert_eq!(cursor, "0", "{what}: a block in one call");
                    same_members(&seen, &want, &format!("{what} ZSCAN"));
                }
                if cursor == "0" {
                    break;
                }
            }
            seen.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            seen.dedup();
            same_members(&seen, &want, &format!("{what} ZSCAN all"));
            // ZRANDMEMBER: members of the set, distinct for a count.
            let picked = c.cmd(&["ZRANDMEMBER", key, "5", "WITHSCORES"]);
            let picked = flat_pairs(&picked);
            assert_eq!(picked.len(), 5.min(n));
            for p in &picked {
                assert!(want.contains(p), "{what}: {p:?}");
            }
            let all = flat_pairs(&c.cmd(&["ZRANDMEMBER", key, "100000", "WITHSCORES"]));
            assert_eq!(all.len(), n);

            // Writes, on a copy: pops from each end, removals of each kind.
            let copy = format!("{key}-copy");
            assert_eq!(c.cmd(&["COPY", key, &copy]).int(), 1);
            assert_eq!(c.encoding(&copy), form.form);
            let popped = flat_pairs(&c.cmd(&["ZPOPMIN", &copy, "2"]));
            same_members(
                &popped,
                &want[..2.min(n)].to_vec(),
                &format!("{what} ZPOPMIN"),
            );
            let popped = flat_pairs(&c.cmd(&["ZPOPMAX", &copy, "2"]));
            let mut top: Members = want.iter().rev().take(2).cloned().collect();
            top.truncate(n.saturating_sub(2).min(2));
            same_members(&popped, &top, &format!("{what} ZPOPMAX"));
            let mut rest: Members = want.iter().skip(2).cloned().collect();
            rest.truncate(n.saturating_sub(4));
            same_members(&c.all(&copy), &rest, &format!("{what} after pops"));
            if rest.len() > 4 {
                assert_eq!(c.cmd(&["ZREMRANGEBYRANK", &copy, "1", "2"]).int(), 2);
                rest.drain(1..3);
                let gone = c.raw(&[b"ZREM", copy.as_bytes(), &rest[0].0, b"absent"]);
                assert_eq!(gone.int(), 1);
                rest.remove(0);
                same_members(&c.all(&copy), &rest, &format!("{what} after removals"));
            }
            let removed = c.cmd(&["ZREMRANGEBYSCORE", &copy, "-inf", "+inf"]).int();
            assert_eq!(removed, rest.len() as i64);
            assert_eq!(c.cmd(&["EXISTS", &copy]).int(), 0, "{what}: emptied, gone");
        }
    }
}

/// A set moves to the large form at the 129th member and at a member of
/// 65 bytes (by ZADD, ZINCRBY, GEOADD and with the settings lowered), never
/// back, and every member is kept through each move.
#[test]
fn every_change_of_form_keeps_every_member() {
    let dir = temp_dir("forms");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let mut pairs: Vec<(Vec<u8>, String)> = Vec::new();
    for i in 0..140usize {
        let m = format!("m{i}").into_bytes();
        let s = format!("{}", (i * 7919) % 101);
        c.zadd("grow", &[(m.clone(), s.clone())]);
        pairs.push((m, s));
        let form = if i < 128 { "listpack" } else { "skiplist" };
        assert_eq!(c.encoding("grow"), form, "at {} members", i + 1);
        same_members(&c.all("grow"), &in_order(&pairs), &format!("grow {i}"));
    }
    // Taken back under the limit: stays large, as in Redis.
    for i in 0..130usize {
        c.cmd(&["ZREM", "grow", &format!("m{i}")]);
    }
    assert_eq!(c.encoding("grow"), "skiplist");
    pairs.drain(..130);
    same_members(&c.all("grow"), &in_order(&pairs), "shrunk");

    // A long member, by each writer.
    c.zadd("long", &members(10, "x"));
    c.cmd(&["ZADD", "long", "5", &"L".repeat(65)]);
    assert_eq!(c.encoding("long"), "skiplist");
    assert_eq!(c.cmd(&["ZCARD", "long"]).int(), 11);
    c.zadd("incr", &members(10, "x"));
    c.cmd(&["ZINCRBY", "incr", "2", &"L".repeat(65)]);
    assert_eq!(c.encoding("incr"), "skiplist");
    let mut want = in_order(&members(10, "x"));
    want.push((vec![b'L'; 65], 2.0));
    want.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    same_members(&c.all("incr"), &want, "zincrby long");
    c.cmd(&["GEOADD", "geo", "13.36", "38.11", "palermo"]);
    assert_eq!(c.encoding("geo"), "listpack");
    c.cmd(&["GEOADD", "geo", "15.08", "37.50", &"C".repeat(65)]);
    assert_eq!(c.encoding("geo"), "skiplist");
    assert_eq!(c.cmd(&["ZCARD", "geo"]).int(), 2);

    // Settings lowered: at the next new member, not at an update.
    c.zadd("cfg", &members(20, "c"));
    assert!(
        c.cmd(&["CONFIG", "SET", "zset-max-listpack-entries", "10"])
            .ok()
    );
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "zset-max-ziplist-entries"])
            .items()[1]
            .text(),
        "10"
    );
    c.cmd(&["ZADD", "cfg", "100", "c:00000001"]);
    assert_eq!(c.encoding("cfg"), "listpack", "an update moves nothing");
    c.cmd(&["ZADD", "cfg", "100", "new"]);
    assert_eq!(c.encoding("cfg"), "skiplist");
    assert_eq!(c.cmd(&["ZCARD", "cfg"]).int(), 21);
    assert!(
        c.cmd(&["CONFIG", "SET", "zset-max-listpack-entries", "1000"])
            .ok()
    );
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "zset-max-listpack-entries"])
            .items()[1]
            .text(),
        "128",
        "no more than a block holds"
    );
    assert!(
        c.cmd(&["CONFIG", "SET", "zset-max-listpack-value", "4"])
            .ok()
    );
    c.cmd(&["ZADD", "short", "1", "abcd"]);
    assert_eq!(c.encoding("short"), "listpack");
    c.cmd(&["ZADD", "short", "1", "abcde"]);
    assert_eq!(c.encoding("short"), "skiplist");
    assert!(
        c.cmd(&["CONFIG", "SET", "zset-max-listpack-value", "64"])
            .ok()
    );

    // A ZADD of more pairs than a block holds starts large, as in Redis,
    // even when the pairs repeat one member.
    let mut args: Vec<String> = vec!["ZADD".into(), "many".into()];
    for _ in 0..129 {
        args.extend(["1".to_string(), "same".to_string()]);
    }
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    c.cmd(&refs);
    assert_eq!(c.encoding("many"), "skiplist");
    assert_eq!(c.cmd(&["ZCARD", "many"]).int(), 1);

    // Destinations are built in the form their size calls for.
    c.zadd("src", &members(300, "s"));
    c.cmd(&["ZRANGESTORE", "d128", "src", "0", "127"]);
    assert_eq!(c.encoding("d128"), "listpack");
    c.cmd(&["ZRANGESTORE", "d129", "src", "0", "128"]);
    assert_eq!(c.encoding("d129"), "skiplist");
    c.cmd(&["ZUNIONSTORE", "u", "2", "d128", "z-none"]);
    assert_eq!(c.encoding("u"), "listpack");
    same_members(&c.all("u"), &c.all("d128"), "union of one");
}

/// Snapshot, AOF replay with changes of form in the log, AOF rewrite: the
/// same members, the same form, the same digest as a large copy.
#[test]
fn save_aof_and_rewrite_round_trips() {
    // Snapshot.
    let dir = temp_dir("save");
    let mut server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    reference_copies(&mut c, &forms);
    assert!(c.cmd(&["SAVE"]).ok());
    server.stop("NOSAVE");
    server.restart();
    let mut c = server.conn();
    check_forms(&mut c, &forms, "snapshot");
    digests_match(&mut c, &forms, "snapshot");

    // AOF replay, with removals and conversions in the log.
    let dir = temp_dir("aof");
    let mut server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    reference_copies(&mut c, &forms);
    for i in 0..140 {
        c.cmd(&["ZADD", "churn", &i.to_string(), &format!("c{i}")]);
    }
    for i in 0..130 {
        c.cmd(&["ZREM", "churn", &format!("c{i}")]);
    }
    c.cmd(&["ZINCRBY", "z10", "0.5", "member:00000003"]);
    c.cmd(&["ZPOPMIN", "z128"]);
    let churn = c.all("churn");
    let z10 = c.all("z10");
    let z128 = c.all("z128");
    server.stop("NOSAVE");
    server.restart();
    let mut c = server.conn();
    same_members(&c.all("churn"), &churn, "aof replay churn");
    assert_eq!(c.encoding("churn"), "skiplist");
    same_members(&c.all("z10"), &z10, "aof replay z10");
    same_members(&c.all("z128"), &z128, "aof replay z128");
    for form in &forms[2..] {
        check_form(&mut c, form.key, form, "aof replay");
    }
    digests_match(&mut c, &forms[2..], "aof replay");

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
    for form in &forms[2..] {
        check_form(&mut c, form.key, form, "aof rewrite");
    }
    digests_match(&mut c, &forms[2..], "aof rewrite");
    same_members(&c.all("z10"), &z10, "aof rewrite z10");
    // A rewritten large set of few members is a block again, as Redis
    // loads one.
    same_members(&c.all("churn"), &churn, "aof rewrite churn");
}

/// DUMP/RESTORE within a server and MIGRATE to another: the members and the
/// form they make there.
#[test]
fn dump_restore_and_migrate() {
    let dir = temp_dir("dump");
    let server = Server::start(dir.path(), &[]);
    let other_dir = temp_dir("dump-to");
    let other = Server::start(other_dir.path(), &[]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    for form in &forms {
        let key = form.key;
        let payload = c.cmd(&["DUMP", key]).bulk();
        let copy = format!("{key}-restored");
        assert!(c.raw(&[b"RESTORE", copy.as_bytes(), b"0", &payload]).ok());
        check_form(&mut c, &copy, form, "restored");
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
    check_forms(&mut o, &forms, "migrated");
    for form in &forms {
        assert_eq!(o.digest(form.key), c.digest(form.key), "{}", form.key);
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

/// A replica's full sync and the stream after it: every form, and every
/// change of form made on the master after the sync.
#[test]
fn replica_full_sync_and_stream() {
    let dir = temp_dir("master");
    let master = Server::start(dir.path(), &[]);
    let mut m = master.conn();
    let mut forms = load_forms(&mut m);
    let rdir = temp_dir("replica");
    let port = master.port.to_string();
    let replica = Server::start(rdir.path(), &["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    wait_for("the link", || {
        r.info("replication", "master_link_status") == "up"
    });
    synced(&mut m, &mut r);
    check_forms(&mut r, &forms, "full sync");

    // Changes of form on the master, streamed.
    let more = members(200, "more");
    m.zadd("z10", &more);
    forms[0].pairs.extend(more);
    forms[0].form = "skiplist";
    m.cmd(&["ZREM", "z128", "member:00000005"]);
    forms[1].pairs.remove(5);
    m.cmd(&["ZADD", "member64", "7", &"x".repeat(65)]);
    forms[3].pairs.push((vec![b'x'; 65], "7".into()));
    forms[3].form = "skiplist";
    m.cmd(&["ZINCRBY", "edges", "1", "e05"]);
    forms[5].pairs[5].1 = "0".into();
    m.cmd(&["ZPOPMAX", "binary"]);
    forms[6].pairs.retain(|(member, _)| member != b"\x00");
    synced(&mut m, &mut r);
    for form in &forms {
        check_form(&mut r, form.key, form, "stream");
        assert_eq!(r.digest(form.key), m.digest(form.key), "{}", form.key);
    }
    // And every key, each by its digest.
    let keys = |c: &mut Conn| {
        let mut all: Vec<Vec<u8>> = c
            .cmd(&["KEYS", "*"])
            .items()
            .iter()
            .map(Resp::bulk)
            .collect();
        all.sort();
        all
    };
    let all = keys(&mut m);
    assert_eq!(keys(&mut r), all);
    for key in &all {
        let digest = |c: &mut Conn| c.raw(&[b"DEBUG", b"DIGEST-VALUE", key]);
        let want = digest(&mut m);
        assert!(matches!(&want, Resp::Array(d) if d.len() == 1), "{want:?}");
        assert_eq!(digest(&mut r), want);
    }
}

/// Clients blocked on a packed set are served in the order they blocked,
/// each the lowest member left when its turn comes, by one ZADD of three.
#[test]
fn blocked_pops_are_served_in_order_by_a_zadd() {
    for model in io_models() {
        let dir = temp_dir("blocked");
        let server = Server::start(dir.path(), &["--io-model", model]);
        let mut c = server.conn();
        let mut waiters: Vec<Conn> = Vec::new();
        for (i, cmd) in ["BZPOPMIN", "BZPOPMIN", "BZMPOP"].iter().enumerate() {
            let mut w = server.conn();
            if *cmd == "BZMPOP" {
                w.send(&[b"BZMPOP", b"5", b"1", b"q", b"MIN", b"COUNT", b"2"]);
            } else {
                w.send(&[cmd.as_bytes(), b"q", b"5"]);
            }
            wait_for("the client to block", || {
                c.info("clients", "blocked_clients") == (i + 1).to_string()
            });
            waiters.push(w);
        }
        c.cmd(&[
            "ZADD", "q", "3", "c", "1", "a", "2", "b", "4", "d", "5", "e",
        ]);
        assert_eq!(c.encoding("q"), "listpack");
        let first = waiters[0].read();
        assert_eq!(
            first,
            Resp::Array(vec![
                Resp::Bulk(b"q".to_vec()),
                Resp::Bulk(b"a".to_vec()),
                Resp::Bulk(b"1".to_vec())
            ]),
            "{model}"
        );
        let second = waiters[1].read();
        assert_eq!(second.items()[1], Resp::Bulk(b"b".to_vec()), "{model}");
        let third = waiters[2].read();
        let popped: Vec<Vec<u8>> = third.items()[1]
            .items()
            .iter()
            .map(|p| p.items()[0].bulk())
            .collect();
        assert_eq!(popped, vec![b"c".to_vec(), b"d".to_vec()], "{model}");
        same_members(&c.all("q"), &vec![(b"e".to_vec(), 5.0)], model);
    }
}

/// ZRANDMEMBER on a block picks each member about as often as each other:
/// chi-square over ten members, with bounds far from what chance gives.
#[test]
fn zrandmember_is_uniform_on_packed_sets() {
    let dir = temp_dir("rand");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let pairs = members(10, "member");
    c.zadd("r", &pairs);
    assert_eq!(c.encoding("r"), "listpack");
    let index = |m: &[u8]| pairs.iter().position(|(x, _)| x == m).unwrap();
    let chi = |counts: &[u64]| {
        let total: u64 = counts.iter().sum();
        let expect = total as f64 / counts.len() as f64;
        counts
            .iter()
            .map(|&n| (n as f64 - expect).powi(2) / expect)
            .sum::<f64>()
    };
    // 9 degrees of freedom: 50 is far past p = 1e-6.
    let mut counts = [0u64; 10];
    for m in c.cmd(&["ZRANDMEMBER", "r", "-20000"]).items() {
        counts[index(&m.bulk())] += 1;
    }
    assert!(chi(&counts) < 50.0, "repeats: {counts:?}");
    let mut counts = [0u64; 10];
    for _ in 0..3000 {
        counts[index(&c.cmd(&["ZRANDMEMBER", "r"]).bulk())] += 1;
    }
    assert!(chi(&counts) < 50.0, "one at a time: {counts:?}");
    let mut counts = [0u64; 10];
    for _ in 0..3000 {
        for m in c.cmd(&["ZRANDMEMBER", "r", "3"]).items() {
            counts[index(&m.bulk())] += 1;
        }
    }
    assert!(chi(&counts) < 50.0, "distinct: {counts:?}");
}

/// Under `allkeys-lru`, small sorted sets in blocks are counted at their
/// size: memory use is brought to the limit and kept there, not far under
/// it; `MEMORY USAGE` of a block is what making it added.
#[test]
fn eviction_counts_the_blocks() {
    let dir = temp_dir("evict");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .ok()
    );
    let pairs = members(10, "member");
    // What a set adds, over 2,000 of them, against what MEMORY USAGE says
    // of one.
    let before: i64 = c.info("memory", "used_memory").parse().unwrap();
    for i in 0..2000 {
        c.zadd(&format!("probe{i}"), &pairs);
    }
    let after: i64 = c.info("memory", "used_memory").parse().unwrap();
    let added = (after - before) / 2000;
    assert_eq!(c.encoding("probe7"), "listpack");
    let each = c.cmd(&["MEMORY", "USAGE", "probe7"]).int();
    assert!(
        (each - added).abs() <= added / 5,
        "MEMORY USAGE {each} against {added} added"
    );
    c.cmd(&["FLUSHALL"]);

    let room: i64 = 2 << 20;
    let base: i64 = c.info("memory", "used_memory").parse().unwrap();
    let limit = base + room;
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .ok()
    );
    let keys = 3 * room / each;
    for i in 0..keys {
        c.zadd(&format!("k{i}"), &pairs);
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
    // Not over- or under-shot: what is kept fills the room, give or take.
    let fitted = kept * each;
    assert!(
        fitted > room * 6 / 10 && fitted < room * 14 / 10,
        "{kept} sets of {each} bytes in {room}"
    );
    let want = in_order(&pairs);
    for i in (0..keys).step_by(97) {
        let key = format!("k{i}");
        if c.cmd(&["EXISTS", &key]).int() == 1 {
            same_members(&c.all(&key), &want, &key);
            assert_eq!(c.encoding(&key), "listpack");
        }
    }
}

/// Inside MULTI/EXEC, a set changes form and is read back whole in the same
/// transaction.
#[test]
fn a_transaction_crosses_the_change_of_form() {
    let dir = temp_dir("multi");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    c.zadd("t", &members(127, "t"));
    assert!(c.cmd(&["MULTI"]).ok());
    c.cmd(&["ZADD", "t", "1", "a", "2", "b"]);
    c.cmd(&["OBJECT", "ENCODING", "t"]);
    c.cmd(&["ZCARD", "t"]);
    let reply = c.cmd(&["EXEC"]);
    let items = reply.items();
    assert_eq!(items[0], Resp::Int(2));
    assert_eq!(items[1].text(), "skiplist");
    assert_eq!(items[2], Resp::Int(129));
}

/// A set grown to 128 members of 64 bytes and popped down to one gives its
/// room back: it is not kept at the size it was until it is deleted.
#[test]
fn a_set_popped_down_gives_its_room_back() {
    let dir = temp_dir("shrink");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let pairs: Vec<(Vec<u8>, String)> = (0..128)
        .map(|i| {
            let mut m = vec![b'm'; 64];
            m[..8].copy_from_slice(format!("{i:08}").as_bytes());
            (m, i.to_string())
        })
        .collect();
    c.zadd("z", &pairs);
    assert_eq!(c.encoding("z"), "listpack");
    let full = c.cmd(&["MEMORY", "USAGE", "z"]).int();
    assert!(full > 9_000, "{full}");
    assert_eq!(c.cmd(&["ZPOPMIN", "z", "127"]).items().len(), 254);
    let one = c.cmd(&["MEMORY", "USAGE", "z"]).int();
    assert!(
        one < 400,
        "MEMORY USAGE {one} after popping 127 of 128 (was {full})"
    );
    assert_eq!(c.cmd(&["ZCARD", "z"]).int(), 1);
}

/// Integer arguments are read as Redis reads them: `x`, `+1`, `01` and
/// `-0` are errors, and an error changes nothing.
#[test]
fn integer_arguments_are_read_as_redis_reads_them() {
    let dir = temp_dir("ints");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    c.zadd("z", &members(10, "m"));
    let not_int = Resp::Error("ERR value is not an integer or out of range".into());
    for bad in ["x", "+1", "01", "-0", "1.0", "9223372036854775808"] {
        assert_eq!(c.cmd(&["ZREMRANGEBYRANK", "z", bad, "5"]), not_int, "{bad}");
        assert_eq!(
            c.cmd(&["ZREMRANGEBYRANK", "nokey", "0", bad]),
            not_int,
            "{bad}"
        );
        assert_eq!(c.cmd(&["ZRANGE", "z", bad, "1"]), not_int, "{bad}");
        assert_eq!(
            c.cmd(&["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", bad, "1"]),
            not_int,
            "{bad}"
        );
        assert_eq!(c.cmd(&["ZRANDMEMBER", "z", bad]), not_int, "{bad}");
        assert!(
            matches!(c.cmd(&["ZPOPMIN", "z", bad]), Resp::Error(_)),
            "{bad}"
        );
    }
    assert_eq!(c.cmd(&["ZCARD", "z"]).int(), 10, "nothing was removed");
    assert_eq!(c.cmd(&["ZREMRANGEBYRANK", "z", "-2", "-1"]).int(), 2);
}
