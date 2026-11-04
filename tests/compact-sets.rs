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
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn kividb_bin() -> String {
    std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        format!("{manifest}/target/debug/kividb")
    })
}

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (as `tests/commands.rs`).
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

/// A free port outside the range the system hands out to outgoing
/// connections: a client dialling a server that is not listening yet could
/// otherwise be given that port as its own end, and connect to itself.
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
    fn start(dir: impl AsRef<Path>, extra: &[&str]) -> Self {
        let dir = dir.as_ref();
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

/// A directory of the test's own, removed when it is dropped.
fn temp_dir(tag: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("kividb-compact-sets-{tag}-"))
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
    // Read as Redis reads a number: `+5` and `05` are not one.
    for (bad, why) in [
        (
            "-1",
            "argument must be between 0 and 9223372036854775807 inclusive",
        ),
        ("x", "argument couldn't be parsed into an integer"),
        ("1.5", "argument couldn't be parsed into an integer"),
        ("", "argument couldn't be parsed into an integer"),
        ("+5", "argument couldn't be parsed into an integer"),
        ("05", "argument couldn't be parsed into an integer"),
    ] {
        let reply = c.cmd(&["CONFIG", "SET", "set-max-intset-entries", bad]);
        assert_eq!(
            reply,
            Resp::Error(format!(
                "ERR CONFIG SET failed (possibly related to argument \
                 'set-max-intset-entries') - {why}"
            )),
            "{bad:?}"
        );
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

fn encoding(c: &mut Conn, key: &str) -> String {
    String::from_utf8(c.cmd(&["OBJECT", "ENCODING", key]).bulk()).unwrap()
}

fn info(c: &mut Conn, field: &str) -> i64 {
    let text = String::from_utf8(c.cmd(&["INFO", "all"]).bulk()).unwrap();
    let line = text.lines().find(|l| l.starts_with(&format!("{field}:")));
    line.unwrap().split(':').nth(1).unwrap().parse().unwrap()
}

fn ints(range: std::ops::Range<i64>) -> Vec<String> {
    range.map(|i| i.to_string()).collect()
}

/// `intset` exactly while the members are kept as the array; a table is never
/// called one, whatever its members or the limit.
#[test]
fn object_encoding_is_the_form_kept() {
    let dir = temp_dir("encoding");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();

    c.sadd("small", &ints(0..10));
    assert_eq!(encoding(&mut c, "small"), "intset");
    c.sadd("words", &["a".to_string(), "b".to_string()]);
    assert_eq!(encoding(&mut c, "words"), "listpack");

    // Converted by a non-integer, and still a table once it is gone.
    c.cmd(&["SADD", "small", "x"]);
    assert_eq!(encoding(&mut c, "small"), "listpack");
    c.cmd(&["SREM", "small", "x"]);
    assert_eq!(encoding(&mut c, "small"), "listpack");

    // Converted by size: the encoding a table of that size has.
    c.sadd("full", &ints(0..DEFAULT as i64));
    assert_eq!(encoding(&mut c, "full"), "intset");
    c.cmd(&["SADD", "full", &DEFAULT.to_string()]);
    assert_eq!(encoding(&mut c, "full"), "listpack");
    c.sadd("full", &ints(0..200));
    assert_eq!(encoding(&mut c, "full"), "hashtable");

    // A lower limit leaves an array an array until its next new member.
    set_limit(&mut c, 1000);
    c.sadd("big", &ints(0..600));
    assert_eq!(encoding(&mut c, "big"), "intset");
    set_limit(&mut c, 64);
    assert_eq!(encoding(&mut c, "big"), "intset");
    c.cmd(&["SADD", "big", "600"]);
    assert_eq!(encoding(&mut c, "big"), "hashtable");
    set_limit(&mut c, 1000);
    c.cmd(&["SREM", "big", "600"]);
    assert_eq!(encoding(&mut c, "big"), "hashtable");

    // With no arrays, a set of integers is a table from the start.
    set_limit(&mut c, 0);
    c.sadd("none", &ints(0..10));
    assert_eq!(encoding(&mut c, "none"), "listpack");
}

/// Every member there throughout comes back exactly once from a scan that
/// spans the change of form: the cursor is a position by each member's own
/// bytes, which the change leaves alone.
#[test]
fn sscan_across_the_change_of_form() {
    let dir = temp_dir("sscan");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    // Over 512 integers: paged, as Redis pages such a set.
    set_limit(&mut c, 1000);
    let members = ints(0..600);
    c.sadd("s", &members);
    assert_eq!(encoding(&mut c, "s"), "intset");

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor = "0".to_string();
    let mut pages = 0;
    loop {
        let reply = c.cmd(&["SSCAN", "s", &cursor, "COUNT", "50"]);
        let Resp::Array(parts) = reply else { panic!() };
        cursor = String::from_utf8(parts[0].bulk()).unwrap();
        let Resp::Array(page) = &parts[1] else {
            panic!()
        };
        seen.extend(page.iter().map(Resp::bulk));
        pages += 1;
        if pages == 3 {
            assert_ne!(cursor, "0", "the scan has to span the change");
            c.cmd(&["SADD", "s", "x"]);
            assert_eq!(encoding(&mut c, "s"), "hashtable");
        }
        if cursor == "0" {
            break;
        }
    }
    assert!(pages > 3);
    seen.retain(|m| m != b"x");
    seen.sort();
    assert_eq!(seen, sorted(&members), "each member exactly once");

    // A small array comes back whole, as an intset does from Redis.
    c.sadd("t", &ints(0..20));
    let Resp::Array(parts) = c.cmd(&["SSCAN", "t", "0", "COUNT", "2"]) else {
        panic!()
    };
    assert_eq!(parts[0].bulk(), b"0");
    assert_eq!(parts[1].members(), sorted(&ints(0..20)));
}

#[test]
fn spop_and_srandmember_counts() {
    let dir = temp_dir("random");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    let members = ints(-5..15);
    c.sadd("s", &members);
    let all = sorted(&members);

    // SRANDMEMBER: distinct up to the size with a positive count; exactly as
    // many as asked with a negative one, repeats allowed.
    for n in [1, 7, 20, 25] {
        let got = c.cmd(&["SRANDMEMBER", "s", &n.to_string()]).members();
        let mut distinct = got.clone();
        distinct.dedup();
        assert_eq!(got.len(), n.min(20), "{n}");
        assert_eq!(distinct, got, "{n}: distinct");
        assert!(got.iter().all(|m| all.contains(m)));
    }
    let got = c.cmd(&["SRANDMEMBER", "s", "-200"]).members();
    assert_eq!(got.len(), 200);
    assert!(got.iter().all(|m| all.contains(m)));
    let mut distinct = got.clone();
    distinct.dedup();
    assert!(distinct.len() < got.len(), "200 of 20 repeat some");
    assert_eq!(c.cmd(&["SRANDMEMBER", "s", "0"]), Resp::Array(vec![]));
    assert_eq!(c.cmd(&["SMEMBERS", "s"]).members(), all, "left as it was");
    assert_eq!(encoding(&mut c, "s"), "intset");

    // SPOP: distinct members, taken out; a negative count is an error.
    assert!(matches!(c.cmd(&["SPOP", "s", "-1"]), Resp::Error(_)));
    assert_eq!(c.cmd(&["SPOP", "s", "0"]), Resp::Array(vec![]));
    let mut popped = c.cmd(&["SPOP", "s", "6"]).members();
    let mut distinct = popped.clone();
    distinct.dedup();
    assert_eq!((popped.len(), distinct.len()), (6, 6));
    assert_eq!(c.cmd(&["SCARD", "s"]).int(), 14);
    assert_eq!(encoding(&mut c, "s"), "intset");
    for m in &popped {
        assert_eq!(c.raw(&[b"SISMEMBER", b"s", m]).int(), 0);
    }
    popped.extend(c.cmd(&["SPOP", "s", "100"]).members());
    popped.sort();
    assert_eq!(popped, all);
    assert_eq!(c.cmd(&["EXISTS", "s"]).int(), 0);
}

/// SMOVE between the two forms, and into an array it turns into a table.
#[test]
fn smove_between_the_forms() {
    let dir = temp_dir("smove");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    c.sadd("ints", &ints(0..5));
    c.sadd("words", &["a".to_string(), "7".to_string()]);

    // Array to table, and table to array, as integers.
    assert_eq!(c.cmd(&["SMOVE", "ints", "words", "3"]).int(), 1);
    assert_eq!(c.cmd(&["SMOVE", "words", "ints", "7"]).int(), 1);
    assert_eq!(
        c.cmd(&["SMEMBERS", "ints"]).members(),
        sorted(
            &ints(0..8)
                .into_iter()
                .filter(|m| m != "3" && m != "5" && m != "6")
                .collect::<Vec<_>>()
        )
    );
    assert_eq!(encoding(&mut c, "ints"), "intset");
    assert_eq!(
        c.cmd(&["SMEMBERS", "words"]).members(),
        sorted(&["a".to_string(), "3".to_string()])
    );

    // A non-integer turns the destination into a table.
    assert_eq!(c.cmd(&["SMOVE", "words", "ints", "a"]).int(), 1);
    assert_eq!(encoding(&mut c, "ints"), "listpack");
    assert_eq!(c.cmd(&["SISMEMBER", "ints", "a"]).int(), 1);
    assert_eq!(c.cmd(&["SISMEMBER", "words", "a"]).int(), 0);

    // One member past the destination's limit turns it too; one it has
    // already only leaves the source.
    c.sadd("full", &ints(0..DEFAULT as i64));
    c.sadd(
        "from",
        &ints(0..2)
            .into_iter()
            .chain([DEFAULT.to_string()])
            .collect::<Vec<_>>(),
    );
    assert_eq!(c.cmd(&["SMOVE", "from", "full", "1"]).int(), 1);
    assert_eq!(encoding(&mut c, "full"), "intset");
    assert_eq!(c.cmd(&["SISMEMBER", "from", "1"]).int(), 0);
    assert_eq!(
        c.cmd(&["SMOVE", "from", "full", &DEFAULT.to_string()])
            .int(),
        1
    );
    assert_eq!(encoding(&mut c, "full"), "listpack");
    assert_eq!(c.cmd(&["SCARD", "full"]).int(), DEFAULT as i64 + 1);
    assert_eq!(c.cmd(&["SMEMBERS", "from"]).members(), sorted(&ints(0..1)));
    assert_eq!(encoding(&mut c, "from"), "intset");
}

/// A STORE into one of its own sources while that source is an array reads
/// it whole before writing it.
#[test]
fn a_store_into_its_own_compact_source() {
    let dir = temp_dir("store-self");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    assert_eq!(c.sadd("u", &ints(-5..6)), 11);
    assert_eq!(c.sadd("w", &["-5", "0", "3", "100"].map(String::from)), 4);
    assert_eq!(c.sadd("t", &["3", "x"].map(String::from)), 2);
    assert_eq!(encoding(&mut c, "u"), "intset");
    assert_eq!(encoding(&mut c, "t"), "listpack");

    assert_eq!(c.cmd(&["SINTERSTORE", "u", "u", "w"]).int(), 3);
    assert_eq!(
        c.cmd(&["SMEMBERS", "u"]).members(),
        sorted(&["-5", "0", "3"].map(String::from))
    );
    assert_eq!(encoding(&mut c, "u"), "intset");

    // Less a table's members.
    assert_eq!(c.cmd(&["SDIFFSTORE", "u", "u", "t"]).int(), 2);
    assert_eq!(
        c.cmd(&["SMEMBERS", "u"]).members(),
        sorted(&["-5", "0"].map(String::from))
    );
    assert_eq!(encoding(&mut c, "u"), "intset");

    // And a union that brings in a string: the table.
    assert_eq!(c.cmd(&["SUNIONSTORE", "u", "u", "t"]).int(), 4);
    assert_eq!(
        c.cmd(&["SMEMBERS", "u"]).members(),
        sorted(&["-5", "0", "3", "x"].map(String::from))
    );
    assert_eq!(encoding(&mut c, "u"), "listpack");
}

/// `SORT` of an array, by number and `ALPHA`, gives what it gives for the same
/// members in the table.
#[test]
fn sort_of_a_compact_set() {
    let dir = temp_dir("sort");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    let members: Vec<String> = [i64::MIN, -20, -1, 0, 2, 10, i64::MAX]
        .iter()
        .map(i64::to_string)
        .collect();
    c.sadd("a", &members);
    set_limit(&mut c, 0);
    c.sadd("t", &members);
    assert_eq!(encoding(&mut c, "a"), "intset");
    assert_eq!(encoding(&mut c, "t"), "listpack");

    let texts = |reply: Resp| -> Vec<String> {
        let Resp::Array(items) = reply else {
            panic!("not an array: {reply:?}");
        };
        items
            .iter()
            .map(|r| String::from_utf8(r.bulk()).unwrap())
            .collect()
    };
    let numeric = [
        "-9223372036854775808",
        "-20",
        "-1",
        "0",
        "2",
        "10",
        "9223372036854775807",
    ];
    let alpha = [
        "-1",
        "-20",
        "-9223372036854775808",
        "0",
        "10",
        "2",
        "9223372036854775807",
    ];
    let mut descending = numeric;
    descending.reverse();
    for (args, want) in [
        (&[][..], &numeric),
        (&["DESC"][..], &descending),
        (&["ALPHA"][..], &alpha),
    ] {
        for key in ["a", "t"] {
            let mut cmd = vec!["SORT", key];
            cmd.extend_from_slice(args);
            assert_eq!(texts(c.cmd(&cmd)), want, "{cmd:?}");
        }
    }
}

/// A replica can have another `set-max-intset-entries` than its master, and
/// keep a set as the other form: the value's digest is the same in both.
#[test]
fn the_digest_is_the_same_in_both_forms() {
    let dir = temp_dir("digest");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    let members: Vec<String> = [1, 2, 3, -70_000, i64::MIN, i64::MAX]
        .iter()
        .map(i64::to_string)
        .collect();
    c.sadd("a", &members);
    set_limit(&mut c, 0);
    c.sadd("b", &members);
    assert_eq!(encoding(&mut c, "a"), "intset");
    assert_ne!(encoding(&mut c, "b"), "intset");
    let digest = |c: &mut Conn, key: &str| c.cmd(&["DEBUG", "DIGEST-VALUE", key]);
    let a = digest(&mut c, "a");
    assert_eq!(a, digest(&mut c, "b"));
    // And it is a digest of the members: one more, another digest.
    c.cmd(&["SADD", "b", "4"]);
    assert_ne!(a, digest(&mut c, "b"));
}

/// Under `maxmemory`, the arrays are counted at their size: about as many fit
/// as `MEMORY USAGE` says, and a key evicted leaves nothing behind.
#[test]
fn eviction_counts_the_arrays() {
    let dir = temp_dir("evict");
    let server = Server::start(&dir, &[]);
    let mut c = server.conn();
    c.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"]);
    let members = ints(10_000_000..10_000_000 + DEFAULT as i64);
    c.sadd("probe", &members);
    let each = usage(&mut c, "probe");
    c.cmd(&["DEL", "probe"]);

    let room: i64 = 1 << 20;
    let limit = info(&mut c, "used_memory") + room;
    c.cmd(&["CONFIG", "SET", "maxmemory", &limit.to_string()]);
    let keys = 3 * room / each;
    for i in 0..keys {
        c.sadd(&format!("k{i}"), &members);
    }
    c.cmd(&["CONFIG", "SET", "maxmemory", "0"]);

    assert!(info(&mut c, "evicted_keys") > 0);
    let mut kept = Vec::new();
    for i in 0..keys {
        let key = format!("k{i}");
        if c.cmd(&["EXISTS", &key]).int() == 1 {
            kept.push(key);
        }
    }
    // Kept at their real size: counted as tables, a tenth as many would fit.
    let fitted = kept.len() as i64 * each;
    assert!(
        fitted > room / 2 && fitted < room * 3 / 2,
        "{} of {each} bytes in {room}",
        kept.len()
    );
    for key in kept.iter().step_by(25) {
        assert_eq!(c.cmd(&["SCARD", key]).int(), DEFAULT as i64);
        assert_eq!(encoding(&mut c, key), "intset");
    }
    let gone = (0..keys)
        .map(|i| format!("k{i}"))
        .find(|k| !kept.contains(k))
        .unwrap();
    assert_eq!(c.cmd(&["SADD", &gone, "1"]).int(), 1);
    assert_eq!(c.cmd(&["SMEMBERS", &gone]).members(), sorted(&ints(1..2)));
    assert_eq!(encoding(&mut c, &gone), "intset");
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
    let conf = dir.path().join("kividb.conf");
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
