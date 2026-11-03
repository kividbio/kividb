// Sets of integers kept as a sorted array (#297): what a client sees has to be
// the same as for any other set, on either side of the change of form, and
// across a save, an AOF replay and DUMP/RESTORE. Each test runs its own
// server process.

/// `set-max-intset-entries` when nothing sets it.
const DEFAULT: usize = kividb::member_set::DEFAULT_INTSET_MAX;

/// Limits swept in one server, each over keys of its own.
const LIMITS: [usize; 6] = [0, 64, 128, 256, 512, 1000];
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Whether the server on `port` is process `pid`, by its INFO `process_id`.
fn answers_as(port: u16, pid: u32) -> bool {
    let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
    if s.write_all(b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n")
        .is_err()
    {
        return false;
    }
    let mut reader = BufReader::new(s);
    let mut header = String::new();
    let Some(len) = reader
        .read_line(&mut header)
        .ok()
        .and_then(|_| header.trim_end().strip_prefix('$')?.parse::<usize>().ok())
    else {
        return false;
    };
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).is_ok()
        && String::from_utf8_lossy(&body).contains(&format!("process_id:{pid}\r\n"))
}

struct Server {
    child: Child,
    port: u16,
}

impl Server {
    /// Tries another port when a test running alongside took this one first.
    fn start(dir: &Path, extra: &[&str]) -> Self {
        for _ in 0..5 {
            let port = free_port();
            let mut child = Command::new(kividb_bin())
                .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
                .args(["--dir", dir.to_str().unwrap(), "--loglevel", "warning"])
                .args(extra)
                .stdout(Stdio::null())
                .spawn()
                .expect("failed to start kividb");
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    // Another test's server may hold the port; ours then exits.
                    if answers_as(port, child.id()) {
                        return Server { child, port };
                    }
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                assert!(Instant::now() < deadline, "server did not start");
                thread::sleep(Duration::from_millis(50));
            }
        }
        panic!("server did not start on any port");
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// `SHUTDOWN` with or without a save, and wait for the process to go.
    fn stop(mut self, how: &str) {
        let _ = self.conn().cmd(&["SHUTDOWN", how]);
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "server did not stop");
            thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("kividb-compact-sets-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

    /// An array of bulk strings, sorted: set replies come in no fixed order.
    fn members(&self) -> Vec<Vec<u8>> {
        let Resp::Array(items) = self else {
            panic!("not an array: {self:?}");
        };
        let mut all: Vec<Vec<u8>> = items.iter().map(Resp::bulk).collect();
        all.sort();
        all
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
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Conn { stream, reader }
    }

    fn raw(&mut self, args: &[&[u8]]) -> Resp {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
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

    /// SADD of every member, one command.
    fn sadd(&mut self, key: &str, members: &[String]) -> i64 {
        let mut args = vec!["SADD", key];
        args.extend(members.iter().map(String::as_str));
        self.cmd(&args).int()
    }
}

fn sorted(members: &[String]) -> Vec<Vec<u8>> {
    let mut all: Vec<Vec<u8>> = members.iter().map(|m| m.as_bytes().to_vec()).collect();
    all.sort();
    all.dedup();
    all
}

fn set_limit(c: &mut Conn, limit: usize) {
    let reply = c.cmd(&[
        "CONFIG",
        "SET",
        "set-max-intset-entries",
        &limit.to_string(),
    ]);
    assert_eq!(reply, Resp::Simple("OK".into()));
}

/// Members spread over the widths: small, past i16, past i32.
fn spread(i: usize) -> String {
    match i % 3 {
        0 => (i as i64 - 30).to_string(),
        1 => (i as i64 * 70_000).to_string(),
        _ => (-(i as i64) * 5_000_000_000).to_string(),
    }
}

fn usage(c: &mut Conn, key: &str) -> i64 {
    c.cmd(&["MEMORY", "USAGE", key]).int()
}

/// Text that only looks like an integer is a member of its own, beside the
/// integer it looks like, and comes back exactly as it was added.
#[test]
fn integer_lookalikes_keep_their_bytes() {
    let dir = temp_dir("lookalikes");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();

    assert_eq!(c.cmd(&["SADD", "s", "1", "2", "-3", "0"]).int(), 4);
    for (member, there) in [
        ("1", 1),
        ("01", 0),
        ("+1", 0),
        ("-0", 0),
        ("1.0", 0),
        (" 1", 0),
        ("-3", 1),
    ] {
        assert_eq!(
            c.cmd(&["SISMEMBER", "s", member]).int(),
            there,
            "{member:?}"
        );
    }
    // Lookalikes added later are new members; the integers stay.
    assert_eq!(c.cmd(&["SADD", "s", "01", "+1", "-0", "00", "1"]).int(), 4);
    assert_eq!(c.cmd(&["SCARD", "s"]).int(), 8);
    assert_eq!(
        c.cmd(&["SMEMBERS", "s"]).members(),
        sorted(&["1", "2", "-3", "0", "01", "+1", "-0", "00"].map(String::from))
    );
    assert_eq!(c.cmd(&["SREM", "s", "01", "1"]).int(), 2);
    assert_eq!(c.cmd(&["SISMEMBER", "s", "+1"]).int(), 1);
    assert_eq!(c.cmd(&["SISMEMBER", "s", "1"]).int(), 0);

    // The ends of i64, and one past each: text from there on.
    let edges = [
        "9223372036854775807",
        "-9223372036854775808",
        "9223372036854775808",
        "-9223372036854775809",
    ];
    assert_eq!(c.cmd(&["SADD", "e", edges[0], edges[1]]).int(), 2);
    assert_eq!(c.cmd(&["SISMEMBER", "e", edges[2]]).int(), 0);
    assert_eq!(c.cmd(&["SADD", "e", edges[2], edges[3]]).int(), 2);
    assert_eq!(
        c.cmd(&["SMEMBERS", "e"]).members(),
        sorted(&edges.map(String::from))
    );
    for m in edges {
        assert_eq!(c.cmd(&["SISMEMBER", "e", m]).int(), 1, "{m}");
    }
    assert_eq!(
        c.cmd(&["SMISMEMBER", "e", edges[0], "0", edges[3]]),
        Resp::Array(vec![Resp::Int(1), Resp::Int(0), Resp::Int(1)])
    );
}

/// Up to the limit, at it, one past it, and back down to nothing, at each
/// limit in turn: every member is found throughout, and the empty set is no
/// key.
#[test]
fn crossing_the_limit_keeps_every_member() {
    let dir = temp_dir("limit");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    for limit in LIMITS {
        set_limit(&mut c, limit);
        let key = format!("s{limit}");
        // With no arrays at all, one member still has to cross something.
        let full = limit.max(1);
        let all: Vec<String> = (0..=full).map(spread).collect();
        let (at_limit, one_more) = all.split_at(full);

        assert_eq!(c.sadd(&key, at_limit), full as i64);
        assert_eq!(c.sadd(&key, at_limit), 0, "the same members again");
        assert_eq!(c.cmd(&["SCARD", &key]).int(), full as i64);
        assert_eq!(c.cmd(&["SMEMBERS", &key]).members(), sorted(at_limit));

        assert_eq!(c.sadd(&key, one_more), 1);
        assert_eq!(c.cmd(&["SCARD", &key]).int(), full as i64 + 1);
        assert_eq!(c.cmd(&["SMEMBERS", &key]).members(), sorted(&all));
        for m in &all {
            assert_eq!(c.cmd(&["SISMEMBER", &key, m]).int(), 1, "{limit} {m}");
        }
        for m in &all {
            assert_eq!(c.cmd(&["SREM", &key, m]).int(), 1, "{limit} {m}");
        }
        assert_eq!(c.cmd(&["EXISTS", &key]).int(), 0);
    }
    set_limit(&mut c, DEFAULT);

    // Set algebra over sets in both forms.
    let all: Vec<String> = (0..6).map(spread).collect();
    assert_eq!(c.sadd("w", &all), 6);

    // A non-integer turns a small set over as well.
    assert_eq!(c.sadd("t", &all[..3]), 3);
    assert_eq!(c.cmd(&["SADD", "t", "x"]).int(), 1);
    assert_eq!(
        c.cmd(&["SMEMBERS", "t"]).members(),
        sorted(&[&all[..3], &["x".to_string()]].concat())
    );

    assert_eq!(c.sadd("u", &all[..5]), 5);
    assert_eq!(c.cmd(&["SINTER", "w", "u"]).members(), sorted(&all[..5]));
    assert_eq!(c.cmd(&["SDIFF", "u", "t"]).members(), sorted(&all[3..5]));
    assert_eq!(
        c.cmd(&["SUNION", "t", "u"]).members(),
        sorted(&[&all[..5], &["x".to_string()]].concat())
    );
    assert_eq!(c.cmd(&["SINTERCARD", "2", "w", "t"]).int(), 3);
    assert_eq!(c.cmd(&["SMOVE", "t", "u", "x"]).int(), 1);
    assert_eq!(c.cmd(&["SISMEMBER", "u", "x"]).int(), 1);
    assert_eq!(c.cmd(&["SUNIONSTORE", "v", "u", "u"]).int(), 6);
    assert_eq!(
        c.cmd(&["SMEMBERS", "v"]).members(),
        sorted(&[&all[..5], &["x".to_string()]].concat())
    );

    // SPOP and SRANDMEMBER hand out members as they were added.
    let some = c.cmd(&["SRANDMEMBER", "u", "-20"]);
    let Resp::Array(picked) = some else { panic!() };
    assert_eq!(picked.len(), 20);
    let mut popped = c.cmd(&["SPOP", "u", "3"]).members();
    popped.extend(c.cmd(&["SMEMBERS", "u"]).members());
    popped.sort();
    assert_eq!(popped, sorted(&[&all[..5], &["x".to_string()]].concat()));
}

/// A lower limit leaves a longer array as it is until its next new member,
/// which turns it into the table; a higher one does not turn a table back.
/// `MEMORY USAGE` of an array is what it has allocated, widening included.
#[test]
fn changing_the_limit_while_running() {
    let dir = temp_dir("change");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "set-max-intset-entries"]),
        Resp::Array(vec![
            Resp::Bulk(b"set-max-intset-entries".to_vec()),
            Resp::Bulk(DEFAULT.to_string().into_bytes()),
        ])
    );
    for bad in ["-1", "x", "1.5", ""] {
        let reply = c.cmd(&["CONFIG", "SET", "set-max-intset-entries", bad]);
        assert!(matches!(reply, Resp::Error(_)), "{bad:?}: {reply:?}");
    }

    set_limit(&mut c, 128);
    let members: Vec<String> = (0..100).map(|i| i.to_string()).collect();
    assert_eq!(c.sadd("s", &members), 100);
    let narrow = usage(&mut c, "s");
    assert_eq!(c.cmd(&["SADD", "s", "100000"]).int(), 1);
    let widened = usage(&mut c, "s");
    assert!(widened > narrow, "{narrow} then {widened}");

    set_limit(&mut c, 64);
    assert_eq!(c.cmd(&["SCARD", "s"]).int(), 101);
    assert_eq!(c.cmd(&["SADD", "s", "5"]).int(), 0);
    assert_eq!(c.cmd(&["SREM", "s", "5"]).int(), 1);
    assert_eq!(usage(&mut c, "s"), widened, "still the array");
    assert_eq!(c.cmd(&["SADD", "s", "5"]).int(), 1);
    // Still an array, it would have reused the slot SREM freed: the same size.
    // The table's figure is the estimate sets had before #297.
    assert_ne!(usage(&mut c, "s"), widened);
    let mut want = members.clone();
    want.push("100000".into());
    assert_eq!(c.cmd(&["SMEMBERS", "s"]).members(), sorted(&want));

    set_limit(&mut c, 1000);
    assert_eq!(c.cmd(&["SREM", "s", "5"]).int(), 1);
    assert_eq!(c.cmd(&["SADD", "s", "5"]).int(), 1);
    assert_eq!(c.cmd(&["SMEMBERS", "s"]).members(), sorted(&want));
}

/// The members to persist: in the array, past its limit, mixed, at the ends.
fn persisted(c: &mut Conn) -> Vec<(String, Vec<String>)> {
    let sets = vec![
        (
            "small".to_string(),
            (0..10)
                .map(|i| (10_000_000 + i).to_string())
                .collect::<Vec<_>>(),
        ),
        (
            "wide".to_string(),
            vec![
                i64::MIN.to_string(),
                "-1".into(),
                "0".into(),
                i64::MAX.to_string(),
            ],
        ),
        (
            "full".to_string(),
            (0..DEFAULT as i64)
                .map(|i| (i * 1_000).to_string())
                .collect(),
        ),
        (
            "over".to_string(),
            (0..=DEFAULT as i64).map(|i| i.to_string()).collect(),
        ),
        (
            "mixed".to_string(),
            vec!["1".into(), "01".into(), "-0".into(), "a".into()],
        ),
    ];
    for (key, members) in &sets {
        c.sadd(key, members);
    }
    sets
}

fn check(c: &mut Conn, sets: &[(String, Vec<String>)]) {
    for (key, members) in sets {
        assert_eq!(c.cmd(&["SCARD", key]).int(), members.len() as i64, "{key}");
        assert_eq!(
            c.cmd(&["SMEMBERS", key]).members(),
            sorted(members),
            "{key}"
        );
        for m in members {
            assert_eq!(c.cmd(&["SISMEMBER", key, m]).int(), 1, "{key} {m}");
        }
    }
}

#[test]
fn a_saved_set_loads_back_the_same() {
    let dir = temp_dir("save");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    let sets = persisted(&mut c);
    assert_eq!(c.cmd(&["SAVE"]), Resp::Simple("OK".into()));
    server.stop("NOSAVE");

    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    check(&mut c, &sets);
    // Loaded small, it is still a set like any other.
    assert_eq!(c.cmd(&["SADD", "small", "x"]).int(), 1);
    assert_eq!(c.cmd(&["SCARD", "small"]).int(), 11);
    server.stop("NOSAVE");

    // Loaded with no arrays at all, set in the configuration file.
    let conf = dir.join("kividb.conf");
    std::fs::write(&conf, "set-max-intset-entries 0\n").unwrap();
    let server = Server::start(&dir, &["--configfile", conf.to_str().unwrap()]);
    let mut c = server.conn();
    check(&mut c, &sets);
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "set-max-intset-entries"]),
        Resp::Array(vec![
            Resp::Bulk(b"set-max-intset-entries".to_vec()),
            Resp::Bulk(b"0".to_vec()),
        ])
    );
}

#[test]
fn an_aof_replays_to_the_same_sets() {
    let dir = temp_dir("aof");
    let server = Server::start(&dir, &["--aof"]);
    let mut c = server.conn();
    let mut sets = persisted(&mut c);
    // Removals in both forms are in the log too.
    assert_eq!(c.cmd(&["SREM", "small", "10000003"]).int(), 1);
    assert_eq!(c.cmd(&["SREM", "over", "0"]).int(), 1);
    sets[0].1.retain(|m| m != "10000003");
    sets[3].1.retain(|m| m != "0");
    server.stop("NOSAVE");

    let server = Server::start(&dir, &["--aof"]);
    check(&mut server.conn(), &sets);
}

#[test]
fn dump_and_restore() {
    let dir = temp_dir("dump");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    let sets = persisted(&mut c);
    for (key, _) in &sets {
        let payload = c.cmd(&["DUMP", key]).bulk();
        let copy = format!("{key}-copy");
        assert_eq!(
            c.raw(&[b"RESTORE", copy.as_bytes(), b"0", &payload]),
            Resp::Simple("OK".into())
        );
    }
    let copies: Vec<(String, Vec<String>)> = sets
        .iter()
        .map(|(k, m)| (format!("{k}-copy"), m.clone()))
        .collect();
    check(&mut c, &copies);

    // `SADD s 1 -2 300000` as Redis 8.10.3 DUMPs it: an intset.
    let redis_intset: &[u8] = b"\x0b\x14\x04\x00\x00\x00\x03\x00\x00\x00\xfe\xff\xff\xff\x01\x00\x00\x00\xe0\x93\x04\x00\x0f\x00\xda,\xea\xbaq\xecX\xaf";
    assert_eq!(
        c.raw(&[b"RESTORE", b"from-redis", b"0", redis_intset]),
        Resp::Simple("OK".into())
    );
    assert_eq!(
        c.cmd(&["SMEMBERS", "from-redis"]).members(),
        sorted(&["1", "-2", "300000"].map(String::from))
    );
    assert_eq!(c.cmd(&["SISMEMBER", "from-redis", "-2"]).int(), 1);
}

/// The KDB bytes of a set kept as an array are the layout every release
/// reads: tag 4, a count, then each member's text, length first.
#[test]
fn the_snapshot_layout_does_not_change() {
    use kividb::member_set::MemberSet;
    let mut set = MemberSet::new();
    for m in ["5", "-1", "300"] {
        set.insert_bytes(m.as_bytes());
    }
    let set = std::sync::Arc::new(parking_lot::RwLock::new(set));
    let mut out = Vec::new();
    kividb::snapshot::write_kdb_entry(&mut out, "k", kividb::store::ValueRef::Set(&set)).unwrap();

    let mut want = Vec::new();
    want.extend_from_slice(&1u32.to_le_bytes());
    want.extend_from_slice(b"k");
    want.push(4);
    want.extend_from_slice(&3u32.to_le_bytes());
    let mut written: Vec<&[u8]> = Vec::new();
    let mut at = want.len();
    assert_eq!(&out[..at], &want[..]);
    for _ in 0..3 {
        let len = u32::from_le_bytes(out[at..at + 4].try_into().unwrap()) as usize;
        written.push(&out[at + 4..at + 4 + len]);
        at += 4 + len;
    }
    assert_eq!(at, out.len());
    written.sort();
    assert_eq!(written, [&b"-1"[..], b"300", b"5"]);
}
