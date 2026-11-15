// A consumer group's pending entries are indexed by consumer as well as by
// ID (#637). Every command that adds, moves or removes a pending entry must
// keep the two in step, and every path that copies or loads a group must
// rebuild the index: these tests check each answer the index gives against
// a read of the whole pending list, after random changes and after DUMP and
// RESTORE, a replica's full sync and stream, an AOF replay and a snapshot.

use std::collections::BTreeMap;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
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
            let port = free_port();
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

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

/// A group's pending entries, read from the whole list: ID, owner and
/// delivery count, by ID; and how many each consumer owns.
#[derive(Debug, PartialEq)]
struct GroupView {
    pending: Vec<(String, Vec<u8>, i64)>,
    counts: BTreeMap<Vec<u8>, i64>,
}

/// Reads `group`'s pending entries from the whole list, and checks that
/// every answer the per-consumer index gives agrees with it: the `XPENDING`
/// summary, `XPENDING` for each consumer, `XINFO CONSUMERS`, and the
/// consumers and released count of `XINFO STREAM FULL`.
fn view(c: &mut Conn, key: &str, group: &str) -> GroupView {
    let rows = c.ok(&["XPENDING", key, group, "-", "+", "1000000"]);
    let pending: Vec<(String, Vec<u8>, i64)> = rows
        .arr()
        .iter()
        .map(|r| (r.arr()[0].str(), r.arr()[1].bytes(), r.arr()[3].int()))
        .collect();
    let mut counts: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    for (_, owner, _) in &pending {
        *counts.entry(owner.clone()).or_default() += 1;
    }
    let owned_by = |name: &[u8]| -> Vec<String> {
        pending
            .iter()
            .filter(|(_, owner, _)| owner == name)
            .map(|(id, _, _)| id.clone())
            .collect()
    };

    let summary = c.ok(&["XPENDING", key, group]);
    let summary = summary.arr();
    assert_eq!(summary[0].int(), pending.len() as i64, "summary count");
    if let (Some(first), Some(last)) = (pending.first(), pending.last()) {
        assert_eq!(summary[1].str(), first.0, "summary lowest ID");
        assert_eq!(summary[2].str(), last.0, "summary highest ID");
    }
    let listed: Vec<(Vec<u8>, i64)> = summary[3]
        .arr()
        .iter()
        .map(|p| (p.arr()[0].bytes(), p.arr()[1].int()))
        .collect();
    let want: Vec<(Vec<u8>, i64)> = counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
    assert_eq!(listed, want, "summary per consumer, by name");

    let consumers = c.ok(&["XINFO", "CONSUMERS", key, group]);
    let mut named = Vec::new();
    for consumer in consumers.arr() {
        let name = consumer.field("name").bytes();
        let own = owned_by(&name);
        assert_eq!(
            consumer.field("pending").int(),
            own.len() as i64,
            "XINFO CONSUMERS pending of {}",
            String::from_utf8_lossy(&name)
        );
        let mut args: Vec<Vec<u8>> = ["XPENDING", key, group, "-", "+", "1000000"]
            .iter()
            .map(|a| a.as_bytes().to_vec())
            .collect();
        args.push(name.clone());
        let ids: Vec<String> = c.ok(&args).arr().iter().map(|r| r.arr()[0].str()).collect();
        assert_eq!(ids, own, "XPENDING for {}", String::from_utf8_lossy(&name));
        named.push(name);
    }
    for owner in counts.keys() {
        assert!(
            owner.is_empty() || named.contains(owner),
            "{} owns entries but is not a consumer",
            String::from_utf8_lossy(owner)
        );
    }

    let full = c.ok(&["XINFO", "STREAM", key, "FULL", "COUNT", "0"]);
    let g = full
        .field("groups")
        .arr()
        .iter()
        .find(|g| g.field("name").str() == group)
        .expect("group in XINFO STREAM FULL")
        .clone();
    assert_eq!(g.field("pel-count").int(), pending.len() as i64);
    assert_eq!(
        g.field("nacked-count").int(),
        counts.get(&b""[..]).copied().unwrap_or(0),
        "released entries"
    );
    for consumer in g.field("consumers").arr() {
        let name = consumer.field("name").bytes();
        let own = owned_by(&name);
        assert_eq!(consumer.field("pel-count").int(), own.len() as i64);
        let ids: Vec<String> = consumer
            .field("pending")
            .arr()
            .iter()
            .map(|r| r.arr()[0].str())
            .collect();
        assert_eq!(ids, own, "XINFO STREAM FULL pending of a consumer");
    }

    GroupView { pending, counts }
}

const CONSUMERS: [&str; 5] = ["c0", "c1", "c2", "c3", "c4"];

fn add_entries(c: &mut Conn, key: &str, from: u64, n: u64) {
    for ms in from..from + n {
        c.ok(&["XADD", key, &format!("{ms}-0"), "f", &ms.to_string()]);
    }
}

/// A pending or recently deleted ID, or one that never was.
fn some_id(c: &mut Conn, r: &mut Rng, key: &str, group: &str, top: u64) -> String {
    let rows = c.ok(&["XPENDING", key, group, "-", "+", "1000000"]);
    let rows = rows.arr();
    if !rows.is_empty() && r.below(4) != 0 {
        rows[r.below(rows.len() as u64) as usize].arr()[0].str()
    } else {
        format!("{}-0", 1 + r.below(top))
    }
}

/// One random change of the pending list, through any command that adds,
/// moves or removes a pending entry. Without `history`, no history reads:
/// a history read of an entry still pending but deleted from the stream
/// reaches replicas and the AOF as an `XCLAIM`, which drops the entry there
/// and not on the primary. Redis 8.10.2 does the same, so a copy is
/// compared only after changes that leave no such difference.
fn random_change(c: &mut Conn, r: &mut Rng, top: &mut u64, history: bool) {
    let consumer = CONSUMERS[r.below(CONSUMERS.len() as u64) as usize];
    let group = if r.below(4) == 0 { "h" } else { "g" };
    let policy = ["KEEPREF", "DELREF", "ACKED"][r.below(3) as usize];
    let count = (1 + r.below(20)).to_string();
    match r.below(17) {
        0 | 1 => {
            let mut args = vec!["XREADGROUP", "GROUP", group, consumer, "COUNT", &count];
            if r.below(4) == 0 {
                args.push("NOACK");
            }
            args.extend(["STREAMS", "s", ">"]);
            c.ok(&args);
        }
        2 if history => {
            // A consumer's own history, from the start or from an ID.
            let from = if r.below(2) == 0 {
                "0".to_string()
            } else {
                format!("{}-0", r.below(*top))
            };
            c.ok(&[
                "XREADGROUP",
                "GROUP",
                group,
                consumer,
                "COUNT",
                &count,
                "STREAMS",
                "s",
                &from,
            ]);
        }
        3 | 4 => {
            let a = some_id(c, r, "s", group, *top);
            let b = some_id(c, r, "s", group, *top);
            c.ok(&["XACK", "s", group, &a, &b]);
        }
        5 => {
            let a = some_id(c, r, "s", group, *top);
            let b = some_id(c, r, "s", group, *top);
            c.ok(&["XACKDEL", "s", group, policy, "IDS", "2", &a, &b]);
        }
        6 => {
            let a = some_id(c, r, "s", group, *top);
            c.ok(&["XDELEX", "s", policy, "IDS", "1", &a]);
        }
        7 => {
            let a = some_id(c, r, "s", group, *top);
            let mut args = vec!["XCLAIM", "s", group, consumer, "0", &a];
            if r.below(3) == 0 {
                args.push("FORCE");
            }
            if r.below(3) == 0 {
                args.push("JUSTID");
            }
            c.ok(&args);
        }
        8 => {
            let start = format!("{}-0", r.below(*top));
            let mut args = vec![
                "XAUTOCLAIM",
                "s",
                group,
                consumer,
                "0",
                &start,
                "COUNT",
                &count,
            ];
            if r.below(3) == 0 {
                args.push("JUSTID");
            }
            c.ok(&args);
        }
        9 => {
            if r.below(3) == 0 {
                c.ok(&["XGROUP", "DELCONSUMER", "s", group, consumer]);
            }
        }
        10 => {
            // Back: entries pending for one consumer are delivered again,
            // to whoever reads next, and move to that consumer.
            let to = format!("{}-0", r.below(*top));
            c.ok(&["XGROUP", "SETID", "s", group, &to]);
        }
        11 => {
            let a = some_id(c, r, "s", group, *top);
            c.ok(&["XDEL", "s", &a]);
        }
        12 => {
            if r.below(4) == 0 {
                let min = format!("{}-0", *top / 3 + r.below(5));
                c.ok(&["XTRIM", "s", "MINID", &min, policy]);
            }
        }
        13 => {
            let a = some_id(c, r, "s", group, *top);
            let mode = ["SILENT", "FAIL", "FATAL"][r.below(3) as usize];
            let mut args = vec!["XNACK", "s", group, mode, "IDS", "1", &a];
            if r.below(3) == 0 {
                args.push("FORCE");
            }
            c.ok(&args);
        }
        14 => {
            if r.below(10) == 0 {
                c.ok(&["XGROUP", "DESTROY", "s", "h"]);
                c.ok(&["XGROUP", "CREATE", "s", "h", "0"]);
            }
        }
        15 => {
            // CLAIM: idle entries of any consumer move to this one before
            // new entries are read.
            let idle = r.below(3).to_string();
            c.ok(&[
                "XREADGROUP",
                "GROUP",
                group,
                consumer,
                "COUNT",
                &count,
                "CLAIM",
                &idle,
                "STREAMS",
                "s",
                ">",
            ]);
        }
        _ => {
            let n = 1 + r.below(5);
            add_entries(c, "s", *top + 1, n);
            *top += n;
        }
    }
}

#[test]
fn every_change_of_the_pending_list_keeps_each_consumers_share_in_step() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let mut top = 300;
    add_entries(&mut c, "s", 1, top);
    c.ok(&["XGROUP", "CREATE", "s", "g", "0"]);
    c.ok(&["XGROUP", "CREATE", "s", "h", "0"]);
    let mut r = Rng(0x2545_F491_4F6C_DD1D);
    for _ in 0..1500 {
        random_change(&mut c, &mut r, &mut top, true);
        view(&mut c, "s", "g");
        view(&mut c, "s", "h");
    }
    // The run went through the cases that matter.
    let g = view(&mut c, "s", "g");
    assert!(g.counts.len() >= 2, "{:?}", g.counts);
}

/// A group whose entries are spread over several consumers, some claimed,
/// some acknowledged, some released.
fn busy_group(c: &mut Conn) {
    add_entries(c, "s", 1, 400);
    c.ok(&["XGROUP", "CREATE", "s", "g", "0"]);
    for (i, consumer) in CONSUMERS.iter().enumerate() {
        let count = (40 + 10 * i).to_string();
        c.ok(&[
            "XREADGROUP",
            "GROUP",
            "g",
            consumer,
            "COUNT",
            &count,
            "STREAMS",
            "s",
            ">",
        ]);
    }
    c.ok(&["XACK", "s", "g", "3-0", "45-0", "46-0", "120-0"]);
    c.ok(&["XCLAIM", "s", "g", "c4", "0", "1-0", "2-0", "50-0"]);
    c.ok(&["XAUTOCLAIM", "s", "g", "c3", "0", "100-0", "COUNT", "5"]);
    c.ok(&["XNACK", "s", "g", "FAIL", "IDS", "2", "60-0", "61-0"]);
    c.ok(&["XGROUP", "CREATECONSUMER", "s", "g", "idle"]);
}

#[test]
fn the_index_comes_back_from_every_copy_of_a_group() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    busy_group(&mut c);
    let want = view(&mut c, "s", "g");
    assert_eq!(want.counts.len(), 6, "five consumers and the released");

    // DUMP and RESTORE.
    let payload = c.ok(&["DUMP", "s"]).bytes();
    c.ok(&[
        b"RESTORE".to_vec(),
        b"copy".to_vec(),
        b"0".to_vec(),
        payload,
    ]);
    assert_eq!(view(&mut c, "copy", "g"), want, "RESTORE");

    // A replica's full sync, then changes it applies as they come.
    let rdir = tempfile::tempdir().unwrap();
    let replica = Server::start(
        rdir.path(),
        &["--replicaof", "127.0.0.1", &server.port.to_string()],
    );
    let mut rc = replica.conn();
    let deadline = Instant::now() + Duration::from_secs(30);
    while rc.cmd(&["EXISTS", "copy"]).int() != 1 {
        assert!(Instant::now() < deadline, "the replica did not sync");
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(view(&mut rc, "s", "g"), want, "replica full sync");
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    let mut top = 400;
    c.ok(&["XGROUP", "CREATE", "s", "h", "0"]);
    for _ in 0..300 {
        random_change(&mut c, &mut r, &mut top, false);
    }
    let want = view(&mut c, "s", "g");
    let want_h = view(&mut c, "s", "h");
    c.ok(&["SET", "marker", "1"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while rc.cmd(&["EXISTS", "marker"]).int() != 1 {
        assert!(Instant::now() < deadline, "the replica did not catch up");
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(view(&mut rc, "s", "g"), want, "replica stream");
    assert_eq!(view(&mut rc, "s", "h"), want_h, "replica stream");
    drop(rc);
    replica.kill();

    // The AOF, then a snapshot, after a restart each.
    drop(c);
    server.kill();
    let server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(view(&mut c, "s", "g"), want, "AOF");
    assert_eq!(view(&mut c, "s", "h"), want_h, "AOF");
    c.ok(&["SAVE"]);
    drop(c);
    server.kill();
    std::fs::remove_file(dir.path().join("appendonly.aof")).ok();
    let _ = std::fs::remove_dir_all(dir.path().join("appendonlydir"));
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    assert_eq!(view(&mut c, "s", "g"), want, "snapshot");
    assert_eq!(view(&mut c, "s", "h"), want_h, "snapshot");
}

#[test]
fn a_consumers_history_and_count_skip_everyone_elses_entries() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    add_entries(&mut c, "s", 1, 60);
    c.ok(&["XGROUP", "CREATE", "s", "g", "0"]);
    // Interleaved: a, b, a, b, ...
    for _ in 0..30 {
        for consumer in ["a", "b"] {
            c.ok(&[
                "XREADGROUP",
                "GROUP",
                "g",
                consumer,
                "COUNT",
                "1",
                "STREAMS",
                "s",
                ">",
            ]);
        }
    }
    let history = c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "b",
        "COUNT",
        "3",
        "STREAMS",
        "s",
        "10-0",
    ]);
    let ids: Vec<String> = history.arr()[0].arr()[1]
        .arr()
        .iter()
        .map(|e| e.arr()[0].str())
        .collect();
    assert_eq!(ids, ["12-0", "14-0", "16-0"]);
    let own = c.ok(&["XPENDING", "s", "g", "20-0", "+", "2", "a"]);
    let ids: Vec<String> = own.arr().iter().map(|r| r.arr()[0].str()).collect();
    assert_eq!(ids, ["21-0", "23-0"]);
    // Moved by a claim: counted for its new owner only.
    c.ok(&["XCLAIM", "s", "g", "a", "0", "2-0", "4-0"]);
    let counts = view(&mut c, "s", "g").counts;
    assert_eq!(counts[&b"a"[..]], 32);
    assert_eq!(counts[&b"b"[..]], 28);
    // A consumer deleted takes its entries with it, and only its own.
    assert_eq!(c.ok(&["XGROUP", "DELCONSUMER", "s", "g", "b"]).int(), 28);
    let counts = view(&mut c, "s", "g").counts;
    assert_eq!(counts.len(), 1);
    assert_eq!(counts[&b"a"[..]], 32);
}

#[test]
fn xpending_for_a_consumer_whose_name_is_not_utf8_lists_only_its_entries() {
    // The filter used to be read as UTF-8, and a name that was not dropped
    // it: every consumer's entries came back.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    add_entries(&mut c, "s", 1, 4);
    c.ok(&["XGROUP", "CREATE", "s", "g", "0"]);
    let odd: &[u8] = b"\xffodd";
    for consumer in [odd, b"plain"] {
        c.ok(&[
            &b"XREADGROUP"[..],
            b"GROUP",
            b"g",
            consumer,
            b"COUNT",
            b"2",
            b"STREAMS",
            b"s",
            b">",
        ]);
    }
    let own = c.ok(&[&b"XPENDING"[..], b"s", b"g", b"-", b"+", b"10", odd]);
    let ids: Vec<String> = own.arr().iter().map(|r| r.arr()[0].str()).collect();
    assert_eq!(ids, ["1-0", "2-0"]);
    let counts = view(&mut c, "s", "g").counts;
    assert_eq!(counts[odd], 2);
}

/// Each consumer of `group` with its pending count, from `XINFO CONSUMERS`.
fn consumers(c: &mut Conn, key: &str, group: &str) -> Vec<(String, i64)> {
    c.ok(&["XINFO", "CONSUMERS", key, group])
        .arr()
        .iter()
        .map(|row| (row.field("name").str(), row.field("pending").int()))
        .collect()
}

/// Waits until `replica` has every write `master` took before this.
fn caught_up(master: &mut Conn, replica: &mut Conn, marker: &str) {
    master.ok(&["SET", marker, "1"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while replica.cmd(&["EXISTS", marker]).int() != 1 {
        assert!(Instant::now() < deadline, "the replica did not catch up");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_consumer_made_by_a_read_that_delivered_nothing_is_kept_by_the_aof_and_a_replica() {
    // An `XREADGROUP` creates its consumer whether or not it delivers
    // anything, and Redis 8.10 propagates the creation as `XGROUP
    // CREATECONSUMER` in every such case — when the read waits, at once.
    // One that waited and timed out was propagated as nothing: the
    // consumer was gone after an AOF replay, and never reached a replica
    // (#654).
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &["--aof"]);
    let rdir = tempfile::tempdir().unwrap();
    let replica = Server::start(
        rdir.path(),
        &["--replicaof", "127.0.0.1", &server.port.to_string()],
    );
    let mut c = server.conn();
    let mut rc = replica.conn();
    caught_up(&mut c, &mut rc, "synced");

    // An empty stream: a read that waits and one that does not.
    c.ok(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    fn read(c: &mut Conn, args: &[&str]) -> V {
        let mut cmd = vec!["XREADGROUP", "GROUP", "g"];
        cmd.extend_from_slice(args);
        c.ok(&cmd)
    }
    assert_eq!(
        read(&mut c, &["alice", "BLOCK", "100", "STREAMS", "s", ">"]),
        V::Nil
    );
    assert_eq!(read(&mut c, &["bob", "STREAMS", "s", ">"]), V::Nil);
    // A stream with entries, all delivered already; a read of a consumer's
    // own history, which has none; a read with NOACK.
    c.ok(&["XADD", "t", "1-1", "f", "v"]);
    c.ok(&["XGROUP", "CREATE", "t", "g", "$"]);
    assert_eq!(
        read(&mut c, &["carol", "BLOCK", "100", "STREAMS", "t", ">"]),
        V::Nil
    );
    assert_eq!(read(&mut c, &["dan", "STREAMS", "t", ">"]), V::Nil);
    read(&mut c, &["erin", "STREAMS", "t", "0"]);
    assert_eq!(read(&mut c, &["fay", "NOACK", "STREAMS", "t", ">"]), V::Nil);
    // A read that waits and is then given an entry.
    c.ok(&["XGROUP", "CREATE", "u", "g", "$", "MKSTREAM"]);
    let port = server.port;
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        Conn::connect(port).ok(&["XADD", "u", "2-1", "f", "v"]);
    });
    let woken = read(&mut c, &["gus", "BLOCK", "10000", "STREAMS", "u", ">"]);
    writer.join().unwrap();
    assert_ne!(woken, V::Nil);

    let names = |c: &mut Conn| {
        [
            consumers(c, "s", "g"),
            consumers(c, "t", "g"),
            consumers(c, "u", "g"),
        ]
    };
    let want = names(&mut c);
    let got: Vec<Vec<&str>> = want
        .iter()
        .map(|list| list.iter().map(|(name, _)| name.as_str()).collect())
        .collect();
    assert_eq!(
        got,
        [
            vec!["alice", "bob"],
            vec!["carol", "dan", "erin", "fay"],
            vec!["gus"]
        ]
    );
    assert_eq!(want[2][0].1, 1, "gus has the entry");

    caught_up(&mut c, &mut rc, "marker");
    assert_eq!(names(&mut rc), want, "replica");
    drop(rc);
    replica.kill();

    drop(c);
    server.kill();
    let server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(names(&mut c), want, "AOF");
}
