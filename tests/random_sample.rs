// tests/random_sample.rs
//
// `ZRANDMEMBER` and `HRANDFIELD` with a count (#839). They copied the whole
// collection before picking from it, so that a sample of one or ten from a
// million took as long as reading them all. They now read the members they
// pick: a hash by position, a sorted set in one walk to the last position
// picked.
//
// What is checked here is what they answer, as Redis 8.10.2 answers it:
//   - a count of the size or more is the whole collection, in its order (a
//     hash as `HGETALL` has it, a sorted set from the highest score down);
//   - a positive count is distinct members, a negative one may repeat;
//   - every member is as likely as any other, and the order of a sample is
//     random: chi-square bounds far enough out that a fair server fails them
//     about once in a billion runs;
//   - `WITHSCORES` / `WITHVALUES` carry the member's own score / value, flat
//     under RESP2 and a pair each under RESP3;
//   - the cost of a small sample does not grow with the hash's size.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::start_verified;

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(Vec<u8>),
    Err(String),
    Int(i64),
    Dbl(f64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn arr(&self) -> &[V] {
        match self {
            V::Arr(a) => a,
            other => panic!("not an array: {other:?}"),
        }
    }
    fn text(&self) -> String {
        match self {
            V::Str(s) => String::from_utf8_lossy(s).into_owned(),
            other => panic!("not a string: {other:?}"),
        }
    }
    fn num(&self) -> f64 {
        match self {
            V::Dbl(d) => *d,
            V::Str(s) => String::from_utf8_lossy(s).parse().unwrap(),
            V::Int(i) => *i as f64,
            other => panic!("not a number: {other:?}"),
        }
    }
    fn texts(&self) -> Vec<String> {
        self.arr().iter().map(V::text).collect()
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
                s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
                s.set_nodelay(true).unwrap();
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

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        assert!(self.reader.read_line(&mut line).unwrap() > 0, "closed");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.as_bytes().to_vec()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "," => V::Dbl(rest.parse().unwrap()),
            "_" => V::Nil,
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Str(data)
            }
            "*" | "~" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Arr((0..n).map(|_| self.read()).collect())
            }
            "%" => {
                let n: usize = rest.parse().unwrap();
                V::Arr((0..2 * n).map(|_| self.read()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }
}

struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Server {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (child, port) = start_verified(|port| {
            Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(dir.path())
                .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
                .args(["--threads", "2", "--loglevel", "error"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("failed to start kividb")
        });
        Server {
            child,
            port,
            _dir: dir,
        }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A sorted set of `n` members `m1..mn`, `mI` with score `I`.
fn fill_zset(c: &mut Conn, key: &str, n: usize) {
    for chunk in (1..=n).collect::<Vec<_>>().chunks(500) {
        let mut args = vec!["ZADD".to_string(), key.to_string()];
        for i in chunk {
            args.push(i.to_string());
            args.push(format!("m{i}"));
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(matches!(c.cmd(&args), V::Int(_)));
    }
}

/// A hash of `n` fields `f1..fn`, `fI` holding `vI`.
fn fill_hash(c: &mut Conn, key: &str, n: usize) {
    for chunk in (1..=n).collect::<Vec<_>>().chunks(1000) {
        let mut args = vec!["HSET".to_string(), key.to_string()];
        for i in chunk {
            args.push(format!("f{i}"));
            args.push(format!("v{i}"));
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(matches!(c.cmd(&args), V::Int(_)));
    }
}

/// Pearson's statistic of `counts` against equal expectation.
fn chi_square(counts: &[u64]) -> f64 {
    let total: u64 = counts.iter().sum();
    let expected = total as f64 / counts.len() as f64;
    counts
        .iter()
        .map(|&o| (o as f64 - expected).powi(2) / expected)
        .sum()
}

/// A bound `sd` standard deviations over the mean of a chi-square with
/// `df` degrees of freedom (mean `df`, variance `2 df`). At 7 the chance of a
/// fair server failing it is about 1e-9 for the sizes used here.
fn bound(df: usize, sd: f64) -> f64 {
    df as f64 + sd * (2.0 * df as f64).sqrt()
}

/// The position of member `mI` / field `fI` in 0..n.
fn index_of(name: &str) -> usize {
    name[1..].parse::<usize>().unwrap() - 1
}

#[test]
fn a_count_of_the_size_or_more_is_the_whole_sorted_set_from_the_top() {
    let server = Server::start();
    let mut c = server.conn();
    fill_zset(&mut c, "z", 300);
    let want: Vec<String> = (1..=300).rev().map(|i| format!("m{i}")).collect();
    for count in ["300", "301", "5000", "9223372036854775807"] {
        assert_eq!(c.cmd(&["ZRANDMEMBER", "z", count]).texts(), want, "{count}");
    }
    let with = c.cmd(&["ZRANDMEMBER", "z", "400", "WITHSCORES"]);
    let with = with.arr();
    assert_eq!(with.len(), 600);
    for (pair, i) in with.chunks(2).zip((1..=300).rev()) {
        assert_eq!(pair[0].text(), format!("m{i}"));
        assert_eq!(pair[1].num(), i as f64);
    }
    // One short of the size is a sample.
    let sample = c.cmd(&["ZRANDMEMBER", "z", "299"]).texts();
    let mut sorted = sample.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sample.len(), 299);
    assert_eq!(sorted.len(), 299, "distinct");
    // None, and an empty collection.
    assert!(c.cmd(&["ZRANDMEMBER", "z", "0"]).arr().is_empty());
    assert!(
        c.cmd(&["ZRANDMEMBER", "z", "0", "WITHSCORES"])
            .arr()
            .is_empty()
    );
    assert!(c.cmd(&["ZRANDMEMBER", "nokey", "3"]).arr().is_empty());
    assert_eq!(c.cmd(&["ZRANDMEMBER", "nokey"]), V::Nil);
}

#[test]
fn a_count_of_the_size_or_more_is_the_whole_hash_in_its_order() {
    let server = Server::start();
    let mut c = server.conn();
    fill_hash(&mut c, "h", 300);
    let all = c.cmd(&["HGETALL", "h"]);
    let fields: Vec<String> = all.arr().chunks(2).map(|p| p[0].text()).collect();
    for count in ["300", "301", "5000", "9223372036854775807"] {
        assert_eq!(
            c.cmd(&["HRANDFIELD", "h", count]).texts(),
            fields,
            "{count}"
        );
    }
    let with = c.cmd(&["HRANDFIELD", "h", "400", "WITHVALUES"]);
    assert_eq!(with, all);
    let sample = c.cmd(&["HRANDFIELD", "h", "299"]).texts();
    let mut sorted = sample.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!((sample.len(), sorted.len()), (299, 299));
    assert!(c.cmd(&["HRANDFIELD", "h", "0"]).arr().is_empty());
    assert!(
        c.cmd(&["HRANDFIELD", "h", "0", "WITHVALUES"])
            .arr()
            .is_empty()
    );
    assert!(c.cmd(&["HRANDFIELD", "nokey", "-3"]).arr().is_empty());
    assert_eq!(c.cmd(&["HRANDFIELD", "nokey"]), V::Nil);
}

#[test]
fn samples_are_distinct_or_repeating_and_carry_their_own_scores_and_values() {
    let server = Server::start();
    let mut c = server.conn();
    fill_zset(&mut c, "z", 1000);
    fill_hash(&mut c, "h", 1000);
    fill_zset(&mut c, "small", 3);
    for count in [1usize, 2, 10, 500, 999] {
        let z = c.cmd(&["ZRANDMEMBER", "z", &count.to_string(), "WITHSCORES"]);
        let h = c.cmd(&["HRANDFIELD", "h", &count.to_string(), "WITHVALUES"]);
        assert_eq!(z.arr().len(), 2 * count);
        assert_eq!(h.arr().len(), 2 * count);
        let mut seen = std::collections::HashSet::new();
        for pair in z.arr().chunks(2) {
            let name = pair[0].text();
            assert_eq!(
                pair[1].num() as usize,
                index_of(&name) + 1,
                "score of {name}"
            );
            assert!(seen.insert(name), "repeated in a positive count {count}");
        }
        let mut seen = std::collections::HashSet::new();
        for pair in h.arr().chunks(2) {
            let name = pair[0].text();
            assert_eq!(
                pair[1].text(),
                format!("v{}", &name[1..]),
                "value of {name}"
            );
            assert!(seen.insert(name), "repeated in a positive count {count}");
        }
    }
    for count in [-1i64, -2, -10, -5000] {
        let n = (-count) as usize;
        let z = c.cmd(&["ZRANDMEMBER", "z", &count.to_string(), "WITHSCORES"]);
        let h = c.cmd(&["HRANDFIELD", "h", &count.to_string(), "WITHVALUES"]);
        assert_eq!((z.arr().len(), h.arr().len()), (2 * n, 2 * n));
        for pair in z.arr().chunks(2) {
            let name = pair[0].text();
            assert_eq!(pair[1].num() as usize, index_of(&name) + 1);
        }
        for pair in h.arr().chunks(2) {
            let name = pair[0].text();
            assert_eq!(pair[1].text(), format!("v{}", &name[1..]));
        }
    }
    // More than there are, with repeats: all from the three.
    let rep = c.cmd(&["ZRANDMEMBER", "small", "-30"]).texts();
    assert_eq!(rep.len(), 30);
    assert!(rep.iter().all(|m| ["m1", "m2", "m3"].contains(&m.as_str())));
    assert!(rep.iter().collect::<std::collections::HashSet<_>>().len() <= 3);
}

#[test]
fn resp3_pairs_each_member_with_its_score_and_each_field_with_its_value() {
    let server = Server::start();
    let mut c = server.conn();
    fill_zset(&mut c, "z", 200);
    fill_hash(&mut c, "h", 200);
    c.cmd(&["HELLO", "3"]);
    for count in ["3", "-3", "250"] {
        let z = c.cmd(&["ZRANDMEMBER", "z", count, "WITHSCORES"]);
        let want = if count == "250" { 200 } else { 3 };
        assert_eq!(z.arr().len(), want, "{count}");
        for pair in z.arr() {
            let pair = pair.arr();
            assert_eq!(pair.len(), 2);
            assert!(matches!(pair[1], V::Dbl(_)), "{pair:?}");
            assert_eq!(pair[1].num() as usize, index_of(&pair[0].text()) + 1);
        }
        let h = c.cmd(&["HRANDFIELD", "h", count, "WITHVALUES"]);
        assert_eq!(h.arr().len(), want, "{count}");
        for pair in h.arr() {
            let pair = pair.arr();
            assert_eq!(pair.len(), 2);
            assert_eq!(pair[1].text(), format!("v{}", &pair[0].text()[1..]));
        }
    }
    // Without WITHSCORES, members only.
    assert!(matches!(
        c.cmd(&["ZRANDMEMBER", "z", "2"]).arr()[0],
        V::Str(_)
    ));
}

#[test]
fn the_errors_stay_the_same() {
    let server = Server::start();
    let mut c = server.conn();
    fill_zset(&mut c, "z", 5);
    fill_hash(&mut c, "h", 5);
    c.cmd(&["SET", "s", "x"]);
    for args in [
        ["ZRANDMEMBER", "z", "2", "BOGUS"],
        ["HRANDFIELD", "h", "2", "BOGUS"],
    ] {
        assert_eq!(c.cmd(&args), V::Err("ERR syntax error".into()));
    }
    for args in [
        ["ZRANDMEMBER", "z", "-9223372036854775808"],
        ["HRANDFIELD", "h", "-9223372036854775808"],
    ] {
        assert!(matches!(c.cmd(&args), V::Err(e) if e.contains("out of range")));
    }
    assert!(matches!(c.cmd(&["ZRANDMEMBER", "s", "2"]), V::Err(e) if e.starts_with("WRONGTYPE")));
    assert!(matches!(c.cmd(&["HRANDFIELD", "s", "2"]), V::Err(e) if e.starts_with("WRONGTYPE")));
}

/// How often each of `n` members is named by `pick(reply)`, over `calls`
/// replies to `args`.
fn tally(
    c: &mut Conn,
    args: &[&str],
    calls: usize,
    n: usize,
    pick: impl Fn(&[V]) -> Vec<usize>,
) -> Vec<u64> {
    let mut counts = vec![0u64; n];
    for _ in 0..calls {
        let reply = c.cmd(args);
        for at in pick(reply.arr()) {
            counts[at] += 1;
        }
    }
    counts
}

#[test]
fn every_member_is_as_likely_as_any_other() {
    let server = Server::start();
    let mut c = server.conn();
    let n = 50;
    fill_zset(&mut c, "z", n);
    fill_hash(&mut c, "h", n);
    let all = |r: &[V]| r.iter().map(|m| index_of(&m.text())).collect::<Vec<_>>();
    let first = |r: &[V]| vec![index_of(&r[0].text())];
    let limit = bound(n - 1, 7.0);

    for (key, command) in [("z", "ZRANDMEMBER"), ("h", "HRANDFIELD")] {
        // The one member asked for: a bulk string.
        let mut counts = vec![0u64; n];
        for _ in 0..5000 {
            counts[index_of(&c.cmd(&[command, key]).text())] += 1;
        }
        let x = chi_square(&counts);
        assert!(x < limit, "{command} one: chi-square {x}");

        // A sample with repeats: every draw is one trial.
        let r = c.cmd(&[command, key, "-50000"]);
        let mut counts = vec![0u64; n];
        for m in r.arr() {
            counts[index_of(&m.text())] += 1;
        }
        let x = chi_square(&counts);
        assert!(x < limit, "{command} -50000: chi-square {x}");

        // A distinct sample of 5: each member is in about one in ten.
        let calls = 4000;
        let counts = tally(&mut c, &[command, key, "5"], calls, n, all);
        let x = chi_square(&counts);
        assert!(x < limit, "{command} 5: chi-square {x}");
        // And the first of the reply is any member: the order is random.
        let counts = tally(&mut c, &[command, key, "5"], calls, n, first);
        let x = chi_square(&counts);
        assert!(x < limit, "{command} 5, first of reply: chi-square {x}");

        // A distinct sample of 40 of 50 (most of the collection).
        let counts = tally(&mut c, &[command, key, "40"], 2000, n, first);
        let x = chi_square(&counts);
        assert!(x < limit, "{command} 40, first of reply: chi-square {x}");

        // The first of a sample with repeats, one command each.
        let counts = tally(&mut c, &[command, key, "-3"], calls, n, first);
        let x = chi_square(&counts);
        assert!(x < limit, "{command} -3, first of reply: chi-square {x}");
    }
}

#[test]
fn a_sample_reaches_every_part_of_a_big_sorted_set_and_hash() {
    let server = Server::start();
    let mut c = server.conn();
    let n = 20_000;
    fill_zset(&mut c, "z", n);
    fill_hash(&mut c, "h", n);
    // A sample that stopped short of the end of the collection, or began
    // past its start, would leave members never picked, or too few picks.
    for (key, command) in [("z", "ZRANDMEMBER"), ("h", "HRANDFIELD")] {
        let mut counts = vec![0u64; n];
        for _ in 0..50 {
            for m in c.cmd(&[command, key, "-4000"]).arr() {
                counts[index_of(&m.text())] += 1;
            }
        }
        // 200,000 picks over 20,000 members: ten each; one in 22,000
        // members is never picked by a fair draw.
        let seen = counts.iter().filter(|&&k| k > 0).count();
        assert!(
            seen > n - 20,
            "{command}: {} members never picked",
            n - seen
        );
        let x = chi_square(&counts);
        assert!(x < bound(n - 1, 7.0), "{command}: chi-square {x}");
        // The members in twenty groups of names: the table keeps them in an
        // order of its own, so each group is spread over all of it.
        let mut grouped = vec![0u64; 20];
        for (i, k) in counts.iter().enumerate() {
            grouped[i % 20] += k;
        }
        let x = chi_square(&grouped);
        assert!(x < bound(19, 7.0), "{command}: groups {x}");
    }
}

/// Seconds for `calls` of `args`.
fn timed(c: &mut Conn, args: &[&str], calls: usize) -> f64 {
    let start = Instant::now();
    for _ in 0..calls {
        assert!(!matches!(c.cmd(args), V::Err(_)));
    }
    start.elapsed().as_secs_f64()
}

/// The work of `HRANDFIELD h 3` is three fields, however many there are: it
/// takes about as long as an `HGET`. It copied every field before it picked,
/// which on this hash (half a million fields) takes several times as long.
/// The bound is five times, in the same session, on the same server.
#[test]
fn hrandfield_costs_the_count_not_the_size_of_the_hash() {
    let server = Server::start();
    let mut c = server.conn();
    fill_hash(&mut c, "h", 500_000);
    // Warm up.
    timed(&mut c, &["HGET", "h", "f1"], 100);
    timed(&mut c, &["HRANDFIELD", "h", "3"], 100);
    let mut ratios = Vec::new();
    for _ in 0..3 {
        let get = timed(&mut c, &["HGET", "h", "f1"], 300);
        let one = timed(&mut c, &["HRANDFIELD", "h", "3"], 300);
        let neg = timed(&mut c, &["HRANDFIELD", "h", "-3", "WITHVALUES"], 300);
        ratios.push((one / get).max(neg / get));
    }
    // The median of three: one noisy round does not fail it.
    ratios.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert!(
        ratios[1] < 5.0,
        "HRANDFIELD took {:.1} times an HGET ({ratios:?})",
        ratios[1]
    );
}
