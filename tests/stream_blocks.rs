// Streams long enough to span many of the blocks their entries are packed
// into, read back every way and through every path that saves or copies a
// stream, against a model kept by the test. Entries of mixed shapes: one
// field and five, names that change, binary and empty names and values,
// values larger than a block, IDs far apart.

use std::collections::BTreeMap;

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

type Fields = Vec<(Vec<u8>, Vec<u8>)>;
/// The entries a stream should hold, by ID.
type Model = BTreeMap<(u64, u64), Fields>;

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

fn id_str((ms, seq): (u64, u64)) -> String {
    format!("{ms}-{seq}")
}

fn parse_id(s: &str) -> (u64, u64) {
    let (ms, seq) = s.split_once('-').unwrap();
    (ms.parse().unwrap(), seq.parse().unwrap())
}

fn fields_for(r: &mut Rng, i: u64) -> Fields {
    let v = |s: &str| s.as_bytes().to_vec();
    match r.below(10) {
        0..=3 => vec![(v("f"), format!("v{i}").into_bytes())],
        4 | 5 => ["type", "user", "ts", "amount", "status"]
            .iter()
            .map(|k| (v(k), format!("{k}-{}", i % 97).into_bytes()))
            .collect(),
        6 => vec![
            (b"\x00\xff".to_vec(), b"\xfe\x00".to_vec()),
            (Vec::new(), Vec::new()),
        ],
        7 => vec![(
            v("big"),
            vec![b'x' + (i % 3) as u8; 2500 + (i % 7) as usize * 100],
        )],
        8 => vec![(v("a"), v("1")), (v("a"), v("2")), (v("f"), v(""))],
        _ => vec![(format!("n{}", i % 5).into_bytes(), v("w"))],
    }
}

fn next_id(r: &mut Rng, (ms, seq): (u64, u64)) -> (u64, u64) {
    match r.below(12) {
        0 if seq < u64::MAX => (ms, seq + 1),
        1 if seq < u64::MAX - (1 << 62) => (ms, seq + (1 << 62)),
        2 if ms < u64::MAX - (1 << 40) => (ms + (1 << 40), r.below(1 << 20)),
        3 if ms < u64::MAX => (ms + 1, u64::MAX - r.below(2)),
        _ => (ms + 1 + r.below(50), r.below(3)),
    }
}

fn xadd(c: &mut Conn, key: &str, id: (u64, u64), fields: &Fields, head: &[&str]) -> V {
    let mut args: Vec<Vec<u8>> = vec![b"XADD".to_vec(), key.as_bytes().to_vec()];
    args.extend(head.iter().map(|h| h.as_bytes().to_vec()));
    args.push(id_str(id).into_bytes());
    for (k, v) in fields {
        args.push(k.clone());
        args.push(v.clone());
    }
    c.cmd(&args)
}

/// Fill `key` with `n` entries; the model of it.
fn fill(c: &mut Conn, key: &str, n: u64, seed: u64) -> Model {
    let mut r = Rng(seed);
    let mut model = Model::new();
    let mut id = (1, 0);
    for i in 0..n {
        let f = fields_for(&mut r, i);
        assert_eq!(xadd(c, key, id, &f, &[]).str(), id_str(id));
        model.insert(id, f);
        id = next_id(&mut r, id);
    }
    model
}

fn entries(reply: &V) -> Vec<((u64, u64), Fields)> {
    reply
        .arr()
        .iter()
        .map(|e| {
            let e = e.arr();
            let kv = e[1].arr();
            let fields = kv.chunks(2).map(|p| (p[0].bytes(), p[1].bytes())).collect();
            (parse_id(&e[0].str()), fields)
        })
        .collect()
}

/// Every way of reading `key` agrees with `model`.
fn check(c: &mut Conn, key: &str, model: &Model, r: &mut Rng, at: &str) {
    let all: Vec<_> = model.iter().map(|(k, v)| (*k, v.clone())).collect();
    assert_eq!(c.ok(&["XLEN", key]).int(), all.len() as i64, "{at}: XLEN");
    assert_eq!(
        entries(&c.ok(&["XRANGE", key, "-", "+"])),
        all,
        "{at}: XRANGE"
    );
    let mut back = all.clone();
    back.reverse();
    assert_eq!(
        entries(&c.ok(&["XREVRANGE", key, "+", "-"])),
        back,
        "{at}: XREVRANGE"
    );
    if all.is_empty() {
        return;
    }
    for _ in 0..30 {
        let a = r.below(all.len() as u64) as usize;
        let b = (a + r.below(150) as usize).min(all.len() - 1);
        let (lo, hi) = (id_str(all[a].0), id_str(all[b].0));
        let count = 1 + r.below(70) as usize;
        let want: Vec<_> = all[a..=b].iter().take(count).cloned().collect();
        let got = entries(&c.ok(&["XRANGE", key, &lo, &hi, "COUNT", &count.to_string()]));
        assert_eq!(got, want, "{at}: XRANGE {lo} {hi} COUNT {count}");
        let want: Vec<_> = all[a..=b].iter().rev().take(count).cloned().collect();
        let got = entries(&c.ok(&["XREVRANGE", key, &hi, &lo, "COUNT", &count.to_string()]));
        assert_eq!(got, want, "{at}: XREVRANGE {hi} {lo} COUNT {count}");
        // Exclusive bounds, and XREAD after an ID.
        let want: Vec<_> = all[a + 1..=b].to_vec();
        let got = entries(&c.ok(&["XRANGE", key, &format!("({lo}"), &hi]));
        assert_eq!(got, want, "{at}: XRANGE ({lo} {hi}");
        let read = c.ok(&["XREAD", "COUNT", &count.to_string(), "STREAMS", key, &lo]);
        let want: Vec<_> = all[a + 1..].iter().take(count).cloned().collect();
        let got = if want.is_empty() {
            Vec::new()
        } else {
            entries(&read.arr()[0].arr()[1])
        };
        assert_eq!(got, want, "{at}: XREAD after {lo}");
    }
    let info = c.ok(&["XINFO", "STREAM", key]);
    let first = entries(&V::Array(vec![info.field("first-entry").clone()]));
    let last = entries(&V::Array(vec![info.field("last-entry").clone()]));
    assert_eq!(first[0], all[0], "{at}: first-entry");
    assert_eq!(last[0], *all.last().unwrap(), "{at}: last-entry");
}

#[test]
fn entries_across_many_blocks_read_back_the_same_every_way() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let model = fill(&mut c, "s", 3000, 11);
    let mut r = Rng(5);
    check(&mut c, "s", &model, &mut r, "filled");
    for (id, fields) in model.iter().step_by(37) {
        let got = entries(&c.ok(&["XRANGE", "s", &id_str(*id), &id_str(*id)]));
        assert_eq!(got, [(*id, fields.clone())]);
    }
    // An ID that is not there, between two that are.
    let (a, _) = model.iter().nth(100).unwrap();
    assert!(
        c.ok(&[
            "XRANGE",
            "s",
            &id_str((a.0, a.1 + 1)),
            &id_str((a.0, a.1 + 1))
        ])
        .arr()
        .is_empty()
            || model.contains_key(&(a.0, a.1 + 1))
    );
}

#[test]
fn deletes_and_trims_across_blocks_agree_with_a_model() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let mut model = fill(&mut c, "s", 4000, 21);
    let mut r = Rng(9);
    // Random XDEL, some IDs already gone.
    for _ in 0..1500 {
        let ids: Vec<_> = model.keys().cloned().collect();
        let x = ids[r.below(ids.len() as u64) as usize];
        let x = if r.below(5) == 0 {
            (x.0, x.1.wrapping_add(1))
        } else {
            x
        };
        let deleted = c.ok(&["XDEL", "s", &id_str(x)]).int();
        assert_eq!(deleted, model.remove(&x).is_some() as i64);
    }
    check(&mut c, "s", &model, &mut r, "after XDEL");
    // A run of neighbours: whole blocks go.
    let run: Vec<_> = model.keys().skip(500).take(300).cloned().collect();
    let mut args = vec!["XDEL".to_string(), "s".to_string()];
    args.extend(run.iter().map(|x| id_str(*x)));
    assert_eq!(c.ok(&args).int(), 300);
    for x in &run {
        model.remove(x);
    }
    check(&mut c, "s", &model, &mut r, "after a run of XDEL");
    // MINID, exact: to the 700th entry.
    let min = *model.keys().nth(700).unwrap();
    assert_eq!(c.ok(&["XTRIM", "s", "MINID", &id_str(min)]).int(), 700);
    model.retain(|k, _| *k >= min);
    check(&mut c, "s", &model, &mut r, "after MINID");
    // MAXLEN, exact.
    assert_eq!(
        c.ok(&["XTRIM", "s", "MAXLEN", "1000"]).int(),
        model.len() as i64 - 1000
    );
    while model.len() > 1000 {
        model.pop_first();
    }
    check(&mut c, "s", &model, &mut r, "after MAXLEN");
    // Approximate: whatever it removes comes off the front.
    let removed = c
        .ok(&[
            "XTRIM",
            "s",
            "MINID",
            "~",
            &id_str(*model.keys().nth(450).unwrap()),
        ])
        .int();
    for _ in 0..removed {
        model.pop_first();
    }
    check(&mut c, "s", &model, &mut r, "after MINID ~");
    let removed = c.ok(&["XTRIM", "s", "MAXLEN", "~", "200"]).int();
    for _ in 0..removed {
        model.pop_first();
    }
    check(&mut c, "s", &model, &mut r, "after MAXLEN ~");
    // XADD with a trim, then everything.
    let mut top = *model.keys().last().unwrap();
    for i in 0..500 {
        top = (top.0 + 1, 0);
        let f = fields_for(&mut r, i);
        xadd(&mut c, "s", top, &f, &["MAXLEN", "=", "150"]);
        model.insert(top, f);
        while model.len() > 150 {
            model.pop_first();
        }
    }
    check(&mut c, "s", &model, &mut r, "after XADD MAXLEN =");
    c.ok(&["XTRIM", "s", "MAXLEN", "0"]);
    model.clear();
    check(&mut c, "s", &model, &mut r, "after MAXLEN 0");
    // Added to again after it emptied.
    xadd(
        &mut c,
        "s",
        (top.0 + 1, 0),
        &vec![(b"f".to_vec(), b"v".to_vec())],
        &[],
    );
    model.insert((top.0 + 1, 0), vec![(b"f".to_vec(), b"v".to_vec())]);
    check(&mut c, "s", &model, &mut r, "added to after emptying");
}

/// A stream's entries, its XINFO STREAM fields, and its groups.
type Snapshot = (Vec<((u64, u64), Fields)>, Vec<V>, V);

/// What a stream holds that does not depend on the clock.
fn snapshot_of(c: &mut Conn, key: &str) -> Snapshot {
    let info = c.ok(&["XINFO", "STREAM", key]);
    let keep: Vec<V> = [
        "length",
        "last-generated-id",
        "max-deleted-entry-id",
        "entries-added",
        "first-entry",
        "last-entry",
    ]
    .iter()
    .map(|f| info.field(f).clone())
    .collect();
    let pending = c.ok(&["XPENDING", key, "g", "-", "+", "1000000"]);
    let pending = V::Array(
        pending
            .arr()
            .iter()
            .map(|row| {
                V::Array(vec![
                    row.arr()[0].clone(),
                    row.arr()[1].clone(),
                    row.arr()[3].clone(),
                ])
            })
            .collect(),
    );
    (entries(&c.ok(&["XRANGE", key, "-", "+"])), keep, pending)
}

fn with_group(c: &mut Conn, key: &str, model: &Model) {
    c.ok(&["XGROUP", "CREATE", key, "g", "0"]);
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "700",
        "STREAMS",
        key,
        ">",
    ]);
    let some: Vec<_> = model
        .keys()
        .skip(100)
        .step_by(3)
        .take(200)
        .cloned()
        .collect();
    for x in some {
        c.ok(&["XDEL", key, &id_str(x)]);
    }
}

#[test]
fn a_stream_of_many_blocks_comes_back_from_every_copy() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    let model = fill(&mut c, "s", 2500, 31);
    with_group(&mut c, "s", &model);
    c.ok(&[
        "XTRIM",
        "s",
        "MINID",
        &id_str(*model.keys().nth(40).unwrap()),
    ]);
    let want = snapshot_of(&mut c, "s");

    // DUMP and RESTORE.
    let payload = c.ok(&["DUMP", "s"]).bytes();
    c.ok(&[
        b"RESTORE".to_vec(),
        b"copy".to_vec(),
        b"0".to_vec(),
        payload,
    ]);
    assert_eq!(snapshot_of(&mut c, "copy"), want, "RESTORE");

    // A replica's full sync.
    let rdir = tempfile::tempdir().unwrap();
    let replica = Server::start(
        rdir.path(),
        &["--replicaof", "127.0.0.1", &server.port.to_string()],
    );
    let mut rc = replica.conn();
    let deadline = Instant::now() + Duration::from_secs(30);
    while rc.cmd(&["XLEN", "copy"]).int() != want.0.len() as i64 {
        assert!(Instant::now() < deadline, "the replica did not sync");
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(snapshot_of(&mut rc, "s"), want, "replica");
    drop(rc);
    replica.kill();

    // The AOF, then a snapshot, after a restart each.
    drop(c);
    server.kill();
    let server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(snapshot_of(&mut c, "s"), want, "AOF");
    c.ok(&["SAVE"]);
    drop(c);
    server.kill();
    std::fs::remove_file(dir.path().join("appendonly.aof")).ok();
    let _ = std::fs::remove_dir_all(dir.path().join("appendonlydir"));
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    assert_eq!(snapshot_of(&mut c, "s"), want, "snapshot");
}

/// A `DUMP` payload of a stream with `entries` in the order given, and
/// `last_id`, in KiviDB's own format.
fn payload(entries: &[((u64, u64), &[u8])], last_id: (u64, u64)) -> Vec<u8> {
    let mut out = vec![0x0c]; // a stream, without idempotency state
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for ((ms, seq), value) in entries {
        out.extend_from_slice(&ms.to_le_bytes());
        out.extend_from_slice(&seq.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes());
        for s in [&b"f"[..], value] {
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s);
        }
    }
    out.extend_from_slice(&last_id.0.to_le_bytes());
    out.extend_from_slice(&last_id.1.to_le_bytes());
    out.extend_from_slice(&[0; 16]); // max deleted ID
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes()); // entries added
    out.extend_from_slice(&[0; 8]); // max entries
    out.extend_from_slice(&0u32.to_le_bytes()); // groups
    let crc = kividb::snapshot::crc64_digest(&out);
    out.extend_from_slice(&[0x0a, 0x00]);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

fn values(c: &mut Conn, key: &str) -> Vec<(String, String)> {
    entries(&c.ok(&["XRANGE", key, "-", "+"]))
        .into_iter()
        .map(|(id, f)| (id_str(id), String::from_utf8(f[0].1.clone()).unwrap()))
        .collect()
}

#[test]
fn out_of_order_and_repeated_ids_load_and_append_as_they_always_have() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    // Out of order, with a repeat: sorted, the later copy kept.
    let p = payload(
        &[
            ((5, 0), b"five"),
            ((1, 0), b"one"),
            ((3, 0), b"first"),
            ((3, 0), b"second"),
            ((2, 7), b"two"),
        ],
        (9, 0),
    );
    c.ok(&[b"RESTORE".to_vec(), b"a".to_vec(), b"0".to_vec(), p]);
    let want = [
        ("1-0", "one"),
        ("2-7", "two"),
        ("3-0", "second"),
        ("5-0", "five"),
    ];
    let want: Vec<_> = want
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
    assert_eq!(values(&mut c, "a"), want);
    assert_eq!(c.ok(&["XLEN", "a"]).int(), 4);

    // `last_id` below the newest entry, past a block of entries: XADD goes
    // in between, and onto an entry that has its ID.
    let many: Vec<((u64, u64), Vec<u8>)> = (1..=200)
        .map(|ms| ((ms * 10, 0), b"loaded".to_vec()))
        .collect();
    let refs: Vec<((u64, u64), &[u8])> = many.iter().map(|(id, v)| (*id, v.as_slice())).collect();
    c.ok(&[
        b"RESTORE".to_vec(),
        b"b".to_vec(),
        b"0".to_vec(),
        payload(&refs, (15, 0)),
    ]);
    for (id, v) in [
        ("25-0", "between"),
        ("30-0", "replaced"),
        ("995-3", "late"),
        ("1000-0", "end"),
    ] {
        assert_eq!(c.ok(&["XADD", "b", id, "f", v]).str(), id);
    }
    assert_eq!(
        c.cmd(&["XADD", "b", "500-0", "f", "x"]),
        V::Error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .into()
        )
    );
    let got = values(&mut c, "b");
    assert_eq!(got.len(), 202);
    assert!(
        got.windows(2)
            .all(|w| parse_id(&w[0].0) < parse_id(&w[1].0)),
        "in ID order"
    );
    let find = |id: &str| got.iter().find(|(i, _)| i == id).map(|(_, v)| v.as_str());
    assert_eq!(find("25-0"), Some("between"));
    assert_eq!(find("30-0"), Some("replaced"));
    assert_eq!(find("995-3"), Some("late"));
    assert_eq!(find("1000-0"), Some("end"));
    assert_eq!(find("990-0"), Some("loaded"));
    let rev: Vec<_> = entries(&c.ok(&["XREVRANGE", "b", "+", "-"]))
        .into_iter()
        .map(|(id, _)| id_str(id))
        .collect();
    let mut fwd: Vec<_> = got.iter().map(|(id, _)| id.clone()).collect();
    fwd.reverse();
    assert_eq!(rev, fwd);
    // And it saves and loads the same.
    let dump = c.ok(&["DUMP", "b"]).bytes();
    c.ok(&[b"RESTORE".to_vec(), b"b2".to_vec(), b"0".to_vec(), dump]);
    assert_eq!(values(&mut c, "b2"), got);
}

#[test]
fn groups_answer_for_entries_deleted_from_blocks_as_before() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    for ms in 1..=300 {
        c.ok(&["XADD", "s", &format!("{ms}-0"), "f", &format!("v{ms}")]);
    }
    c.ok(&["XGROUP", "CREATE", "s", "g", "0"]);
    c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "300",
        "STREAMS",
        "s",
        ">",
    ]);
    // A whole block's worth, and some either side of a block's end.
    for ms in (60..=140).chain([10, 11, 250]) {
        c.ok(&["XDEL", "s", &format!("{ms}-0")]);
    }
    // History: a deleted entry comes back with no fields.
    let reply = c.ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "5",
        "STREAMS",
        "s",
        "58-0",
    ]);
    let rows = reply.arr()[0].arr()[1].arr().to_vec();
    let got: Vec<(String, bool)> = rows
        .iter()
        .map(|e| (e.arr()[0].str(), e.arr()[1] == V::Nil))
        .collect();
    assert_eq!(
        got,
        [
            ("59-0".into(), false),
            ("60-0".into(), true),
            ("61-0".into(), true),
            ("62-0".into(), true),
            ("63-0".into(), true)
        ]
    );
    // XAUTOCLAIM hands over what is left and lists what is gone.
    let reply = c.ok(&["XAUTOCLAIM", "s", "g", "bob", "0", "58-0", "COUNT", "90"]);
    let claimed: Vec<String> = reply.arr()[1]
        .arr()
        .iter()
        .map(|e| e.arr()[0].str())
        .collect();
    let deleted: Vec<String> = reply.arr()[2].arr().iter().map(V::str).collect();
    assert_eq!(claimed.len() + deleted.len(), 90);
    assert_eq!(deleted.len(), 81);
    assert!(
        deleted
            .iter()
            .all(|id| (60..=140).contains(&parse_id(id).0))
    );
    // XCLAIM drops a deleted entry from the pending list.
    let reply = c.ok(&["XCLAIM", "s", "g", "carol", "0", "10-0", "12-0", "250-0"]);
    let claimed: Vec<String> = reply.arr().iter().map(|e| e.arr()[0].str()).collect();
    assert_eq!(claimed, ["12-0"]);
    assert_eq!(c.ok(&["XPENDING", "s", "g"]).arr()[0].int(), 300 - 81 - 2);
}

#[test]
fn memory_usage_and_used_memory_follow_the_entries() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let used = |c: &mut Conn| -> i64 {
        let info = c.ok(&["INFO", "memory"]).str();
        info.lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    };
    let before = used(&mut c);
    for ms in 1..=20_000 {
        c.ok(&["XADD", "s", &format!("{ms}-0"), "f", "v"]);
    }
    let grown = used(&mut c) - before;
    // Packed: a few bytes an entry, not the hundred and fifty of one
    // allocation per entry and field.
    assert!(grown < 20_000 * 20, "{grown} B for 20,000 entries");
    let usage = c.ok(&["MEMORY", "USAGE", "s"]).int();
    assert!(
        (usage - grown).abs() < grown / 5,
        "MEMORY USAGE {usage}, used_memory grew {grown}"
    );
    // Trimmed to a tenth: about a tenth is left.
    c.ok(&["XTRIM", "s", "MAXLEN", "2000"]);
    let left = c.ok(&["MEMORY", "USAGE", "s"]).int();
    assert!(
        left < usage / 5,
        "{left} of {usage} after trimming to a tenth"
    );
    // Trimmed to nothing, it takes what a stream that never held more than
    // one entry takes; deleted, what it took goes too.
    c.ok(&["XTRIM", "s", "MAXLEN", "0"]);
    c.ok(&["XADD", "e", "1-0", "f", "v"]);
    c.ok(&["XDEL", "e", "1-0"]);
    assert_eq!(
        c.ok(&["MEMORY", "USAGE", "s"]),
        c.ok(&["MEMORY", "USAGE", "e"])
    );
    c.ok(&["DEL", "s", "e"]);
    // A few KB are the server's own (buffers, statistics), on master too.
    let held = used(&mut c) - before;
    assert!(held < 8_000, "{held} B held after the stream went");
}

#[test]
fn idempotent_duplicates_are_counted_again_by_an_aof_replay() {
    // Redis 8.10 propagates an `XADD … IDMP` that its producer had already
    // delivered, so that a replay of the AOF counts it in `iids-duplicates`
    // as the first run did. It used to be propagated as nothing, and the
    // count started over after a restart (#655). The entries and the IDs
    // remembered come back as they were.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    let first = c.ok(&["XADD", "s", "IDMP", "p1", "id1", "*", "f", "v"]);
    for _ in 0..2 {
        let retry = c.ok(&["XADD", "s", "IDMP", "p1", "id1", "*", "f", "v"]);
        assert_eq!(retry, first);
    }
    let auto = c.ok(&["XADD", "s", "IDMPAUTO", "p2", "*", "f", "w"]);
    let retry = c.ok(&["XADD", "s", "IDMPAUTO", "p2", "*", "f", "w"]);
    assert_eq!(retry, auto);

    let info = |c: &mut Conn| {
        let info = c.ok(&["XINFO", "STREAM", "s"]);
        let n = |name: &str| info.field(name).int();
        (
            n("length"),
            n("iids-tracked"),
            n("iids-added"),
            n("iids-duplicates"),
            info.field("last-generated-id").str(),
        )
    };
    let want = info(&mut c);
    assert_eq!((want.0, want.1, want.2, want.3), (2, 2, 2, 3));

    drop(c);
    server.kill();
    let server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    assert_eq!(info(&mut c), want, "AOF");
    // A retry after the replay is still recognised, and counted.
    let retry = c.ok(&["XADD", "s", "IDMP", "p1", "id1", "*", "f", "v"]);
    assert_eq!(retry, first);
    assert_eq!(info(&mut c).3, 4);
}
