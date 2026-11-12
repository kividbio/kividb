// The pending entries each consumer has, as `XINFO CONSUMERS`, `XPENDING`
// and `XREADGROUP` report them, agree with the group's pending list after
// every command that changes it, and come back the same from every path
// that loads a stream: a snapshot, DUMP/RESTORE, the AOF and a replica's
// sync.
//
// The group's pending list, read whole (`XPENDING key g - + n`), is the
// reference: the reads checked against it answer from each consumer's own
// entries.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
enum V {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<V>),
    Nil,
}

impl V {
    fn arr(&self) -> &[V] {
        match self {
            V::Array(items) => items,
            V::Nil => &[],
            other => panic!("not an array: {other:?}"),
        }
    }

    fn str(&self) -> String {
        match self {
            V::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            V::Simple(s) => s.clone(),
            V::Int(n) => n.to_string(),
            other => panic!("not a string: {other:?}"),
        }
    }

    fn bytes(&self) -> Vec<u8> {
        match self {
            V::Bulk(b) => b.clone(),
            other => panic!("not a bulk string: {other:?}"),
        }
    }

    fn int(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            V::Bulk(_) => self.str().parse().unwrap(),
            other => panic!("not an integer: {other:?}"),
        }
    }

    /// A map reply in RESP2: alternating names and values.
    fn field(&self, name: &str) -> &V {
        let items = self.arr();
        let at = items
            .iter()
            .step_by(2)
            .position(|k| k.str() == name)
            .unwrap_or_else(|| panic!("no {name} in {self:?}"));
        &items[at * 2 + 1]
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..200)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to the server");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Conn { stream, reader }
    }

    fn cmd<S: AsRef<[u8]>>(&mut self, args: &[S]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            let arg = arg.as_ref();
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read()
    }

    /// A command that must not fail.
    fn ok<S: AsRef<[u8]> + std::fmt::Debug>(&mut self, args: &[S]) -> V {
        let reply = self.cmd(args);
        assert!(!matches!(reply, V::Error(_)), "{args:?}: {reply:?}");
        reply
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => V::Simple(line[1..].to_string()),
            Some(b'-') => V::Error(line[1..].to_string()),
            Some(b':') => V::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Bulk(data)
            }
            Some(b'*') => {
                let n: i64 = line[1..].parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Array((0..n).map(|_| self.read()).collect())
            }
            _ => panic!("unexpected RESP line: {line:?}"),
        }
    }
}

/// A server process of its own, so it can be stopped and started again on
/// the same directory.
struct Server {
    child: Child,
    port: u16,
}

impl Server {
    /// On a port that was free a moment ago; on another if a server started
    /// alongside takes it first.
    fn start(dir: &Path, extra: &[&str]) -> Server {
        for _ in 0..5 {
            let port = TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(dir)
                .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
                .args(["--dir", dir.to_str().unwrap()])
                .args(["--loglevel", "warning", "--update-check", "no"])
                .args(extra)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("could not start kividb");
            let mut server = Server { child, port };
            if server.is_listening() {
                return server;
            }
        }
        panic!("kividb did not start");
    }

    /// This server, not another, answers on its port.
    fn is_listening(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if !matches!(self.child.try_wait(), Ok(None)) {
                return false;
            }
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                let me = format!("process_id:{}\r\n", self.child.id());
                return match self.conn().cmd(&["INFO", "server"]) {
                    V::Bulk(info) => String::from_utf8_lossy(&info).contains(&me),
                    _ => false,
                };
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// Ended at once, as a crash would end it.
    fn kill(mut self) {
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

const ALL: &str = "1000000000";
const CONSUMERS: [&str; 4] = ["alice", "bob", "carol", "dave"];

/// The group's whole pending list, from the group itself: (ID, consumer,
/// delivery count, whether `XNACK` released it).
fn pending_list(c: &mut Conn, key: &str) -> Vec<(String, String, i64, bool)> {
    c.ok(&["XPENDING", key, "g", "-", "+", ALL])
        .arr()
        .iter()
        .map(|row| {
            let row = row.arr();
            (row[0].str(), row[1].str(), row[3].int(), row[2].int() == -1)
        })
        .collect()
}

fn ids(list: &[(String, String, i64, bool)], consumer: &str) -> Vec<String> {
    list.iter()
        .filter(|(_, c, _, _)| c == consumer)
        .map(|(id, _, _, _)| id.clone())
        .collect()
}

/// Each consumer's name and pending count, as `XINFO CONSUMERS` lists them.
fn xinfo_consumers(c: &mut Conn, key: &str) -> Vec<(String, i64)> {
    c.ok(&["XINFO", "CONSUMERS", key, "g"])
        .arr()
        .iter()
        .map(|m| (m.field("name").str(), m.field("pending").int()))
        .collect()
}

/// Everything about a group that does not depend on the clock.
#[derive(Debug, PartialEq)]
struct View {
    pending: Vec<(String, String, i64, bool)>,
    summary: V,
    consumers: Vec<(String, i64)>,
    owned: Vec<(String, Vec<String>)>,
}

fn view(c: &mut Conn, key: &str) -> View {
    let consumers = xinfo_consumers(c, key);
    let mut names: BTreeSet<String> = consumers.iter().map(|(n, _)| n.clone()).collect();
    names.insert(String::new());
    let owned = names
        .into_iter()
        .map(|name| {
            let own = c.ok(&["XPENDING", key, "g", "-", "+", ALL, &name]);
            let own = own.arr().iter().map(|r| r.arr()[0].str()).collect();
            (name, own)
        })
        .collect();
    View {
        pending: pending_list(c, key),
        summary: c.ok(&["XPENDING", key, "g"]),
        consumers,
        owned,
    }
}

/// Every read that answers from a consumer's own entries agrees with the
/// group's pending list.
fn check(c: &mut Conn, key: &str, at: &str) {
    let list = pending_list(c, key);
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();
    for (_, consumer, _, _) in &list {
        *counts.entry(consumer.clone()).or_default() += 1;
    }

    // XPENDING's summary: the count, the first and last ID, and each
    // consumer's count, by name.
    let summary = c.ok(&["XPENDING", key, "g"]);
    let summary = summary.arr();
    assert_eq!(summary[0].int(), list.len() as i64, "{at}: summary count");
    match (list.first(), list.last()) {
        (Some(first), Some(last)) => {
            assert_eq!(summary[1].str(), first.0, "{at}: summary first ID");
            assert_eq!(summary[2].str(), last.0, "{at}: summary last ID");
        }
        _ => assert_eq!((&summary[1], &summary[2]), (&V::Nil, &V::Nil), "{at}"),
    }
    let listed: Vec<(String, i64)> = summary[3]
        .arr()
        .iter()
        .map(|p| (p.arr()[0].str(), p.arr()[1].int()))
        .collect();
    let want: Vec<(String, i64)> = counts.clone().into_iter().collect();
    assert_eq!(listed, want, "{at}: summary per consumer");

    // XINFO CONSUMERS.
    let consumers = xinfo_consumers(c, key);
    for (name, pending) in &consumers {
        let want = counts.get(name).copied().unwrap_or(0);
        assert_eq!(*pending, want, "{at}: XINFO CONSUMERS pending of {name}");
    }

    // XPENDING for one consumer, whole and in a range.
    let mut names: BTreeSet<String> = consumers.into_iter().map(|(n, _)| n).collect();
    names.extend(counts.keys().cloned());
    names.insert(String::new());
    for name in &names {
        let want = ids(&list, name);
        let got: Vec<String> = c
            .ok(&["XPENDING", key, "g", "-", "+", ALL, name])
            .arr()
            .iter()
            .map(|r| r.arr()[0].str())
            .collect();
        assert_eq!(got, want, "{at}: XPENDING for {name:?}");
        if want.len() > 2 {
            let (from, to) = (&want[1], &want[want.len() - 2]);
            let got: Vec<String> = c
                .ok(&["XPENDING", key, "g", from, to, "2", name])
                .arr()
                .iter()
                .map(|r| r.arr()[0].str())
                .collect();
            assert_eq!(
                got,
                want[1..3.min(want.len() - 1)],
                "{at}: range for {name:?}"
            );
        }
    }

    // XINFO STREAM FULL: each consumer's count and pending entries, and the
    // entries no one has.
    let full = c.ok(&["XINFO", "STREAM", key, "FULL", "COUNT", ALL]);
    let groups = full.field("groups").arr();
    if let Some(group) = groups.first() {
        let nacked = counts.get("").copied().unwrap_or(0);
        assert_eq!(
            group.field("nacked-count").int(),
            nacked,
            "{at}: nacked-count"
        );
        for consumer in group.field("consumers").arr() {
            let name = consumer.field("name").str();
            let own: Vec<String> = consumer
                .field("pending")
                .arr()
                .iter()
                .map(|r| r.arr()[0].str())
                .collect();
            assert_eq!(
                own,
                ids(&list, &name),
                "{at}: STREAM FULL pending of {name}"
            );
            assert_eq!(
                consumer.field("pel-count").int(),
                own.len() as i64,
                "{at}: STREAM FULL pel-count of {name}"
            );
        }
    }
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        (!items.is_empty()).then(|| &items[self.below(items.len())])
    }

    fn consumer(&mut self) -> &'static str {
        CONSUMERS[self.below(CONSUMERS.len())]
    }
}

fn entry_ids(c: &mut Conn, key: &str) -> Vec<String> {
    c.ok(&["XRANGE", key, "-", "+"])
        .arr()
        .iter()
        .map(|e| e.arr()[0].str())
        .collect()
}

/// Up to `n` of `ids`, picked at random.
fn some(rng: &mut Rng, ids: &[String], n: usize) -> Vec<String> {
    (0..1 + rng.below(n))
        .filter_map(|_| rng.pick(ids).cloned())
        .collect()
}

/// `steps` commands that change the pending list, picked at random, each
/// followed by [`check`] when `checked`.
fn churn(c: &mut Conn, key: &str, seed: u64, steps: usize, checked: bool) {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    // An error when it is already there: a second round on the same stream.
    let _ = c.cmd(&["XGROUP", "CREATE", key, "g", "0", "MKSTREAM"]);
    for step in 0..steps {
        let list = pending_list(c, key);
        let pending: Vec<String> = list.iter().map(|p| p.0.clone()).collect();
        let entries = entry_ids(c, key);
        let who = rng.consumer();
        let n = (1 + rng.below(4)).to_string();
        let op = rng.below(16);
        let at = format!("seed {seed} step {step} op {op}");
        match op {
            0 | 1 => {
                for _ in 0..1 + rng.below(3) {
                    c.ok(&["XADD", key, "*", "f", "v"]);
                }
            }
            2 | 3 => {
                let mut args = vec!["XREADGROUP", "GROUP", "g", who, "COUNT", &n];
                if rng.below(5) == 0 {
                    args.push("NOACK");
                }
                args.extend(["STREAMS", key, ">"]);
                c.ok(&args);
            }
            4 => {
                // A history read: this consumer's entries after an ID.
                let mine = ids(&list, who);
                let from = match rng.below(3) {
                    0 => "0".to_string(),
                    _ => rng.pick(&entries).cloned().unwrap_or("0".into()),
                };
                let count = 1 + rng.below(4);
                let after = |id: &String| from == "0" || cmp_id(id, &from).is_gt();
                let want: Vec<String> = mine.into_iter().filter(after).take(count).collect();
                let reply = c.ok(&[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    who,
                    "COUNT",
                    &count.to_string(),
                    "STREAMS",
                    key,
                    &from,
                ]);
                let got: Vec<String> = reply.arr()[0].arr()[1]
                    .arr()
                    .iter()
                    .map(|e| e.arr()[0].str())
                    .collect();
                assert_eq!(got, want, "{at}: history of {who} after {from}");
            }
            5 => {
                let mut acked = some(&mut rng, &pending, 3);
                acked.extend(rng.pick(&entries).cloned());
                if acked.is_empty() {
                    continue;
                }
                let want = acked
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .filter(|id| pending.contains(id))
                    .count();
                let mut args = vec!["XACK".to_string(), key.into(), "g".into()];
                args.extend(acked);
                assert_eq!(c.ok(&args).int(), want as i64, "{at}: XACK");
            }
            6 => {
                let mut args = vec!["XCLAIM".to_string(), key.into(), "g".into(), who.into()];
                args.push("0".into());
                args.extend(some(&mut rng, &pending, 3));
                if rng.below(3) == 0 {
                    args.extend(rng.pick(&entries).cloned());
                    args.push("FORCE".into());
                }
                if args.len() == 5 || args[5] == "FORCE" {
                    continue;
                }
                if rng.below(2) == 0 {
                    args.push("JUSTID".into());
                }
                if rng.below(4) == 0 {
                    args.extend(["RETRYCOUNT".into(), "7".into()]);
                }
                c.ok(&args);
            }
            7 => {
                let start = rng.pick(&pending).cloned().unwrap_or("0-0".into());
                let mut args = vec!["XAUTOCLAIM", key, "g", who, "0", &start, "COUNT", &n];
                if rng.below(2) == 0 {
                    args.push("JUSTID");
                }
                c.ok(&args);
            }
            8 => {
                let mode = ["SILENT", "FAIL", "FATAL"][rng.below(3)];
                let mut nacked = some(&mut rng, &pending, 2);
                let force = rng.below(3) == 0;
                if force {
                    nacked.extend(rng.pick(&entries).cloned());
                }
                if nacked.is_empty() {
                    continue;
                }
                let mut args = vec!["XNACK".to_string(), key.into(), "g".into(), mode.into()];
                args.extend(["IDS".to_string(), nacked.len().to_string()]);
                args.extend(nacked);
                if force {
                    args.push("FORCE".into());
                }
                c.ok(&args);
            }
            9 => {
                let want = ids(&list, who).len() as i64;
                let removed = c.ok(&["XGROUP", "DELCONSUMER", key, "g", who]).int();
                assert_eq!(removed, want, "{at}: DELCONSUMER {who}");
            }
            10 => {
                if let Some(id) = rng.pick(&entries) {
                    c.ok(&["XDEL", key, id]);
                }
            }
            11 => {
                let keep = entries.len().saturating_sub(1 + rng.below(3)).to_string();
                let policy = ["KEEPREF", "DELREF", "ACKED"][rng.below(3)];
                if rng.below(2) == 0 {
                    c.ok(&["XTRIM", key, "MAXLEN", "=", &keep, policy]);
                } else if let Some(min) = rng.pick(&entries) {
                    c.ok(&["XTRIM", key, "MINID", "=", min, policy]);
                }
            }
            12 => {
                let policy = ["KEEPREF", "DELREF", "ACKED"][rng.below(3)];
                let chosen = some(&mut rng, &pending, 2);
                if chosen.is_empty() {
                    continue;
                }
                let mut args = vec!["XACKDEL".to_string(), key.into(), "g".into()];
                args.extend([policy.to_string(), "IDS".into(), chosen.len().to_string()]);
                args.extend(chosen);
                c.ok(&args);
            }
            13 => {
                // Delivered again from the start: entries still pending are
                // handed to whoever reads them now.
                if rng.below(3) == 0 {
                    c.ok(&["XGROUP", "SETID", key, "g", "0"]);
                }
            }
            14 => {
                c.ok(&[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    who,
                    "COUNT",
                    &n,
                    "CLAIM",
                    "0",
                    "STREAMS",
                    key,
                    ">",
                ]);
            }
            _ => {
                c.ok(&["XGROUP", "CREATECONSUMER", key, "g", who]);
            }
        }
        if checked {
            check(c, key, &at);
        }
    }
}

fn cmp_id(a: &str, b: &str) -> std::cmp::Ordering {
    let parse = |s: &str| {
        let (ms, seq) = s.split_once('-').unwrap_or((s, "0"));
        (ms.parse::<u64>().unwrap(), seq.parse::<u64>().unwrap())
    };
    parse(a).cmp(&parse(b))
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(Instant::now() < until, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn each_consumers_entries_agree_with_the_group_through_every_command() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    for seed in 1..=4 {
        let key = format!("s{seed}");
        churn(&mut c, &key, seed, 250, true);
    }
}

#[test]
fn a_consumers_count_and_entries_follow_each_hand_over() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    c.ok(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    let added: Vec<String> = (0..6)
        .map(|_| c.ok(&["XADD", "s", "*", "f", "v"]).str())
        .collect();
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "a",
        "COUNT",
        "4",
        "STREAMS",
        "s",
        ">",
    ]);
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "b",
        "COUNT",
        "2",
        "STREAMS",
        "s",
        ">",
    ]);
    assert_eq!(
        xinfo_consumers(&mut c, "s"),
        [("a".into(), 4), ("b".into(), 2)]
    );

    // XCLAIM hands two of a's to b; XAUTOCLAIM hands b's first back to a.
    c.ok(&["XCLAIM", "s", "g", "b", "0", &added[0], &added[1]]);
    assert_eq!(
        xinfo_consumers(&mut c, "s"),
        [("a".into(), 2), ("b".into(), 4)]
    );
    c.ok(&["XAUTOCLAIM", "s", "g", "a", "0", "0-0", "COUNT", "1"]);
    assert_eq!(
        xinfo_consumers(&mut c, "s"),
        [("a".into(), 3), ("b".into(), 3)]
    );
    // XNACK releases one of b's: no one has it.
    c.ok(&["XNACK", "s", "g", "FAIL", "IDS", "1", &added[4]]);
    assert_eq!(
        xinfo_consumers(&mut c, "s"),
        [("a".into(), 3), ("b".into(), 2)]
    );
    let mut c2 = server.conn();
    let unowned = c2.ok(&["XPENDING", "s", "g", "-", "+", "10", ""]);
    assert_eq!(unowned.arr().len(), 1);
    assert_eq!(unowned.arr()[0].arr()[0].str(), added[4]);
    // XACK and DELCONSUMER take entries off their consumer.
    assert_eq!(c.ok(&["XACK", "s", "g", &added[0], &added[5]]).int(), 2);
    assert_eq!(c.ok(&["XGROUP", "DELCONSUMER", "s", "g", "a"]).int(), 2);
    assert_eq!(xinfo_consumers(&mut c, "s"), [("b".into(), 1)]);
    assert_eq!(c.ok(&["XPENDING", "s", "g"]).arr()[0].int(), 2);
    check(&mut c, "s", "hand-over");
}

#[test]
fn trimming_and_deleting_entries_take_them_off_their_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    c.ok(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    for _ in 0..10 {
        c.ok(&["XADD", "s", "*", "f", "v"]);
    }
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "a",
        "COUNT",
        "5",
        "STREAMS",
        "s",
        ">",
    ]);
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "b",
        "COUNT",
        "5",
        "STREAMS",
        "s",
        ">",
    ]);
    // KEEPREF leaves them pending; DELREF takes them off.
    c.ok(&["XTRIM", "s", "MAXLEN", "=", "8", "KEEPREF"]);
    assert_eq!(
        xinfo_consumers(&mut c, "s"),
        [("a".into(), 5), ("b".into(), 5)]
    );
    c.ok(&["XTRIM", "s", "MAXLEN", "=", "4", "DELREF"]);
    assert_eq!(
        xinfo_consumers(&mut c, "s"),
        [("a".into(), 2), ("b".into(), 4)]
    );
    check(&mut c, "s", "trim");
    // XAUTOCLAIM drops what has gone from the stream.
    c.ok(&["XAUTOCLAIM", "s", "g", "b", "0", "0-0", "COUNT", "10"]);
    assert_eq!(
        xinfo_consumers(&mut c, "s"),
        [("a".into(), 0), ("b".into(), 4)]
    );
    check(&mut c, "s", "autoclaim");
    // The group and the stream going.
    c.ok(&["XGROUP", "DESTROY", "s", "g"]);
    c.ok(&["XGROUP", "CREATE", "s", "g", "0"]);
    assert_eq!(c.ok(&["XPENDING", "s", "g"]).arr()[0].int(), 0);
    c.ok(&["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"]);
    c.ok(&["DEL", "s"]);
    c.ok(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    assert_eq!(xinfo_consumers(&mut c, "s"), []);
    check(&mut c, "s", "recreated");
}

#[test]
fn a_saved_or_restored_group_answers_as_before() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    churn(&mut c, "s", 11, 300, false);
    let before = view(&mut c, "s");
    assert!(!before.pending.is_empty());

    // DUMP and RESTORE.
    let dump = match c.ok(&["DUMP", "s"]) {
        V::Bulk(b) => b,
        other => panic!("DUMP: {other:?}"),
    };
    let mut args = format!(
        "*4\r\n$7\r\nRESTORE\r\n$1\r\nr\r\n$1\r\n0\r\n${}\r\n",
        dump.len()
    )
    .into_bytes();
    args.extend_from_slice(&dump);
    args.extend_from_slice(b"\r\n");
    c.stream.write_all(&args).unwrap();
    assert_eq!(c.read(), V::Simple("OK".into()));
    assert_eq!(view(&mut c, "r"), before, "after RESTORE");
    check(&mut c, "r", "after RESTORE");

    // A snapshot, and a restart.
    c.ok(&["SAVE"]);
    server.kill();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    assert_eq!(view(&mut c, "s"), before, "after a restart");
    check(&mut c, "s", "after a restart");
    churn(&mut c, "s", 12, 100, true);
}

#[test]
fn an_aof_replay_rebuilds_each_consumers_entries() {
    let dir = tempfile::tempdir().unwrap();
    let aof = ["--aof", "--appendfsync", "always"];
    let server = Server::start(dir.path(), &aof);
    let mut c = server.conn();
    churn(&mut c, "s", 21, 300, false);
    let before = view(&mut c, "s");
    server.kill();

    // The commands replayed.
    let server = Server::start(dir.path(), &aof);
    let mut c = server.conn();
    assert_eq!(view(&mut c, "s"), before, "after replaying the AOF");
    check(&mut c, "s", "after replaying the AOF");

    // A rewritten AOF (a snapshot of the dataset, then commands).
    c.ok(&["BGREWRITEAOF"]);
    wait_for("the AOF rewrite", || {
        let info = c.ok(&["INFO", "persistence"]).str();
        info.contains("aof_rewrite_in_progress:0") && info.contains("aof_rewrite_scheduled:0")
    });
    churn(&mut c, "s", 22, 100, false);
    let before = view(&mut c, "s");
    server.kill();
    let server = Server::start(dir.path(), &aof);
    let mut c = server.conn();
    assert_eq!(view(&mut c, "s"), before, "after a rewritten AOF");
    check(&mut c, "s", "after a rewritten AOF");
}

#[test]
fn a_replica_and_then_its_promotion_keep_each_consumers_entries() {
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let master = Server::start(dirs.0.path(), &[]);
    let replica = Server::start(dirs.1.path(), &[]);
    let mut m = master.conn();
    let mut r = replica.conn();

    // What the replica's full sync brings, then the commands that follow.
    churn(&mut m, "s", 31, 200, false);
    r.ok(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]);
    churn(&mut m, "s", 32, 200, false);
    let want = view(&mut m, "s");
    wait_for("the replica to catch up", || {
        r.cmd(&["EXISTS", "s"]) == V::Int(1) && view(&mut r, "s") == want
    });
    check(&mut r, "s", "on the replica");

    // Promoted, it carries on from what it has.
    r.ok(&["REPLICAOF", "NO", "ONE"]);
    churn(&mut r, "s", 33, 100, true);
}

/// Arguments of any mix of text and bytes.
macro_rules! args {
    ($($a:expr),* $(,)?) => { [$(AsRef::<[u8]>::as_ref(&$a)),*] };
}

/// Consumer names as bytes: empty, binary, not UTF-8 (two that read the same
/// as text), and longer than a name kept inline.
const NAMES: [&[u8]; 8] = [
    b"",
    b"\x00",
    b"\xff",
    b"\xfe",
    b"caf\xc3\xa9",
    b"a b",
    b"a",
    b"a consumer name longer than 23 bytes",
];

/// The group's pending list by name as bytes, (ID, consumer), after checking
/// every read that answers from a consumer's own entries against it. (Not
/// the delivery counts: the history reads here add to them.)
fn byte_view(c: &mut Conn, key: &str, at: &str) -> Vec<(String, Vec<u8>)> {
    byte_view_with(c, key, at, true)
}

/// [`byte_view`], with or without the history reads (writes, to a replica).
fn byte_view_with(c: &mut Conn, key: &str, at: &str, history: bool) -> Vec<(String, Vec<u8>)> {
    let list: Vec<(String, Vec<u8>)> = c
        .ok(&args!["XPENDING", key, "g", "-", "+", ALL])
        .arr()
        .iter()
        .map(|r| (r.arr()[0].str(), r.arr()[1].bytes()))
        .collect();
    let mut owned: BTreeMap<Vec<u8>, Vec<String>> = BTreeMap::new();
    for (id, consumer) in &list {
        owned.entry(consumer.clone()).or_default().push(id.clone());
    }
    let own = |name: &[u8]| owned.get(name).cloned().unwrap_or_default();
    let ids_in =
        |rows: &V| -> Vec<String> { rows.arr().iter().map(|r| r.arr()[0].str()).collect() };

    let summary = c.ok(&args!["XPENDING", key, "g"]);
    let counts: Vec<(Vec<u8>, i64)> = summary.arr()[3]
        .arr()
        .iter()
        .map(|p| (p.arr()[0].bytes(), p.arr()[1].int()))
        .collect();
    let want: Vec<(Vec<u8>, i64)> = owned
        .iter()
        .map(|(n, ids)| (n.clone(), ids.len() as i64))
        .collect();
    assert_eq!(counts, want, "{at}: summary per consumer");

    let consumers: Vec<Vec<u8>> = c
        .ok(&args!["XINFO", "CONSUMERS", key, "g"])
        .arr()
        .iter()
        .map(|m| {
            let name = m.field("name").bytes();
            let n = m.field("pending").int();
            assert_eq!(
                n,
                own(&name).len() as i64,
                "{at}: XINFO CONSUMERS of {name:?}"
            );
            name
        })
        .collect();

    let mut names: BTreeSet<Vec<u8>> = NAMES.iter().map(|n| n.to_vec()).collect();
    names.extend(owned.keys().cloned());
    for name in &names {
        let got = ids_in(&c.ok(&args!["XPENDING", key, "g", "-", "+", ALL, name]));
        assert_eq!(got, own(name), "{at}: XPENDING for {name:?}");
    }
    for name in consumers.iter().filter(|_| history) {
        let reply = c.ok(&args![
            "XREADGROUP",
            "GROUP",
            "g",
            name,
            "COUNT",
            ALL,
            "STREAMS",
            key,
            "0"
        ]);
        assert_eq!(
            ids_in(&reply.arr()[0].arr()[1]),
            own(name),
            "{at}: history of {name:?}"
        );
    }

    let full = c.ok(&args!["XINFO", "STREAM", key, "FULL", "COUNT", ALL]);
    let group = &full.field("groups").arr()[0];
    assert_eq!(
        group.field("nacked-count").int(),
        own(b"").len() as i64,
        "{at}: nacked-count"
    );
    for consumer in group.field("consumers").arr() {
        let name = consumer.field("name").bytes();
        assert_eq!(
            ids_in(consumer.field("pending")),
            own(&name),
            "{at}: STREAM FULL of {name:?}"
        );
        assert_eq!(
            consumer.field("pel-count").int(),
            own(&name).len() as i64,
            "{at}"
        );
    }
    list
}

/// Every command that hands an entry over or takes it off, between consumers
/// with the names above.
fn hand_over_by_name(c: &mut Conn, key: &str) {
    c.ok(&args!["XGROUP", "CREATE", key, "g", "$", "MKSTREAM"]);
    let added: Vec<String> = (0..40)
        .map(|_| c.ok(&args!["XADD", key, "*", "f", "v"]).str())
        .collect();
    for name in NAMES {
        c.ok(&args![
            "XREADGROUP",
            "GROUP",
            "g",
            name,
            "COUNT",
            "4",
            "STREAMS",
            key,
            ">"
        ]);
    }
    byte_view(c, key, "delivered");
    let of = |c: &mut Conn, name: &[u8]| -> Vec<String> {
        let rows = c.ok(&args!["XPENDING", key, "g", "-", "+", ALL, name]);
        rows.arr().iter().map(|r| r.arr()[0].str()).collect()
    };

    // Released entries join the real consumer named "" in the same bucket.
    let ff = of(c, b"\xff");
    c.ok(&args!["XNACK", key, "g", "FAIL", "IDS", "2", ff[0], ff[1]]);
    byte_view(c, key, "XNACK");
    let zero = of(c, b"\x00");
    c.ok(&args!["XCLAIM", key, "g", b"\xfe", "0", zero[0], zero[1]]);
    byte_view(c, key, "XCLAIM");
    c.ok(&args![
        "XAUTOCLAIM",
        key,
        "g",
        NAMES[7],
        "0",
        "0-0",
        "COUNT",
        "3"
    ]);
    byte_view(c, key, "XAUTOCLAIM");
    let cafe = of(c, b"caf\xc3\xa9");
    let ab = of(c, b"a b");
    assert_eq!(
        c.ok(&args!["XACK", key, "g", cafe[0], ab[0], added[39]])
            .int(),
        2
    );
    byte_view(c, key, "XACK");
    let a = of(c, b"a");
    c.ok(&args![
        "XACKDEL", key, "g", "DELREF", "IDS", "2", a[0], ab[1]
    ]);
    byte_view(c, key, "XACKDEL");
    for name in [&b"\xfe"[..], b""] {
        let before = byte_view(c, key, "before DELCONSUMER").len();
        let n = c.ok(&args!["XGROUP", "DELCONSUMER", key, "g", name]).int();
        let after = byte_view(c, key, "DELCONSUMER").len();
        assert_eq!(n as usize, before - after, "DELCONSUMER {name:?}");
    }
    // Delivered again to names that went.
    c.ok(&args!["XADD", key, "*", "f", "v"]);
    c.ok(&args!["XREADGROUP", "GROUP", "g", "", "STREAMS", key, ">"]);
    c.ok(&args!["XCLAIM", key, "g", b"\xfe", "0", ff[2]]);
}

#[test]
fn binary_and_empty_consumer_names_keep_their_own_entries() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    hand_over_by_name(&mut c, "s");
    let before = byte_view(&mut c, "s", "all handed over");

    let dump = c.ok(&["DUMP", "s"]).bytes();
    c.ok(&args!["RESTORE", "r", "0", dump]);
    assert_eq!(byte_view(&mut c, "r", "after RESTORE"), before);

    c.ok(&["SAVE"]);
    server.kill();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    assert_eq!(byte_view(&mut c, "s", "after a restart"), before);
}

#[test]
fn binary_and_empty_consumer_names_come_back_from_the_aof_and_a_replica() {
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let aof = ["--aof", "--appendfsync", "always"];
    let master = Server::start(dirs.0.path(), &aof);
    let replica = Server::start(dirs.1.path(), &[]);
    let mut m = master.conn();
    let mut r = replica.conn();
    r.ok(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]);
    hand_over_by_name(&mut m, "s");
    let want = byte_view(&mut m, "s", "on the master");
    assert!(!want.is_empty());
    wait_for("the replica to catch up", || {
        byte_view_quiet(&mut r, "s") == want
    });
    byte_view_with(&mut r, "s", "on the replica", false);

    master.kill();
    let master = Server::start(dirs.0.path(), &aof);
    let mut m = master.conn();
    assert_eq!(byte_view(&mut m, "s", "after replaying the AOF"), want);
}

/// The pending list alone, by name as bytes; nothing when there is no group.
fn byte_view_quiet(c: &mut Conn, key: &str) -> Vec<(String, Vec<u8>)> {
    match c.cmd(&args!["XPENDING", key, "g", "-", "+", ALL]) {
        V::Array(rows) => rows
            .iter()
            .map(|r| (r.arr()[0].str(), r.arr()[1].bytes()))
            .collect(),
        _ => Vec::new(),
    }
}
