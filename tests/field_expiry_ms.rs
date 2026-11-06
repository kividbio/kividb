// tests/field_expiry_ms.rs
//
// A hash field's deadline, set with `HPEXPIREAT` to a whole millisecond,
// reads back with `HPEXPIRETIME` as that same millisecond after everything
// that writes the hash out and reads it back in: a save and a restart, the
// AOF and its preamble, a replica's full sync and its stream, `DUMP` and
// `RESTORE`, and `MIGRATE`. It was often 1 ms off: each of these cut the
// time now to a whole millisecond before converting it (#448).
//
// Real `kividb` processes, because a restart and a replica are the points of
// interest. Many hashes, so that their loads fall at many points of a
// millisecond.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn text(&self) -> String {
        match self {
            V::Str(s) => String::from_utf8_lossy(s).into_owned(),
            other => panic!("not a string: {other:?}"),
        }
    }
    fn int(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == b"OK")
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

    fn send(&mut self, args: &[&[u8]]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    fn raw(&mut self, args: &[&[u8]]) -> V {
        self.send(args);
        self.read()
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.as_bytes().to_vec()),
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
                V::Str(data)
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

    fn info(&mut self, section: &str, field: &str) -> String {
        self.cmd(&["INFO", section])
            .text()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .trim()
            .to_string()
    }
}

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

/// A port nothing listens on, outside the range handed to outgoing
/// connections.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(43);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn kividb(dir: &Path, port: u16, extra: &[String]) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
    command.current_dir(dir).args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--configfile",
        "kividb.conf",
    ]);
    if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
        command.args(["--io-model", &model]);
    }
    command
        .args(extra)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
    args: Vec<String>,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        let child = kividb(&dir, port, &args);
        let mut server = Server {
            port,
            dir,
            child,
            args,
        };
        server.wait_up();
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn wait_up(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let up = TcpStream::connect(("127.0.0.1", self.port)).and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(10)))?;
                s.write_all(b"*1\r\n$4\r\nPING\r\n")?;
                let mut reply = [0u8; 7];
                s.read_exact(&mut reply)?;
                Ok(&reply == b"+PONG\r\n")
            });
            if up.unwrap_or(false) {
                return;
            }
            assert!(Instant::now() < deadline, "the server did not come up");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Killed, and started again on the same port and directory.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = kividb(&self.dir, self.port, &self.args);
        self.wait_up();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn wait_until(what: &str, secs: u64, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

const HASHES: usize = 300;
const FIELDS: [&str; 3] = ["a", "b", "c"];

/// `HASHES` hashes named `prefix0`, `prefix1`, ..., each of three fields
/// with a deadline of its own, an hour or so out. What each field's deadline
/// is, by hash and field.
fn write_hashes(c: &mut Conn, prefix: &str) -> Vec<(String, Vec<u64>)> {
    let base = unix_ms_now() + 3_600_000;
    let mut want = Vec::new();
    for i in 0..HASHES {
        let key = format!("{prefix}{i}");
        c.send(&[b"HSET", key.as_bytes(), b"a", b"1", b"b", b"2", b"c", b"3"]);
        let mut ats = Vec::new();
        for (j, field) in FIELDS.iter().enumerate() {
            let at = (base + (i * 17 + j * 5) as u64).to_string();
            c.send(&[
                b"HPEXPIREAT",
                key.as_bytes(),
                at.as_bytes(),
                b"FIELDS",
                b"1",
                field.as_bytes(),
            ]);
            ats.push(at.parse().unwrap());
        }
        want.push((key, ats));
    }
    for _ in 0..HASHES * (1 + FIELDS.len()) {
        assert!(!matches!(c.read(), V::Err(_)));
    }
    want
}

/// Every field of `want` has, on `c`, the deadline `want` says, under the
/// name `rename` gives its hash.
fn assert_deadlines(
    c: &mut Conn,
    what: &str,
    want: &[(String, Vec<u64>)],
    rename: impl Fn(&str) -> String,
) {
    let mut wrong = Vec::new();
    for (key, ats) in want {
        let key = rename(key);
        let reply = c.cmd(&["HPEXPIRETIME", &key, "FIELDS", "3", "a", "b", "c"]);
        let V::Arr(got) = &reply else {
            panic!("{what}: HPEXPIRETIME {key}: {reply:?}");
        };
        let got: Vec<u64> = got.iter().map(|v| v.int() as u64).collect();
        if got != *ats {
            wrong.push(format!("{key}: {got:?}, set {ats:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{what}: {} of {} hashes read back other deadlines: {:?}",
        wrong.len(),
        want.len(),
        &wrong[..wrong.len().min(5)]
    );
}

fn same(key: &str) -> String {
    key.to_string()
}

#[test]
fn a_field_deadline_is_kept_to_the_millisecond_by_a_save_and_a_restart() {
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    let want = write_hashes(&mut c, "h");
    assert_deadlines(&mut c, "as set", &want, same);
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    server.restart();
    let mut c = server.conn();
    assert_deadlines(&mut c, "after a restart", &want, same);
    // Saved again from what was loaded, and loaded again.
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    server.restart();
    assert_deadlines(&mut server.conn(), "after a second restart", &want, same);
}

#[test]
fn a_field_deadline_is_kept_to_the_millisecond_by_the_aof_and_its_preamble() {
    let mut server = Server::start(&["--aof"]);
    let mut c = server.conn();
    let want = write_hashes(&mut c, "h");
    // Replayed as written: the commands themselves.
    std::thread::sleep(Duration::from_millis(1_500));
    drop(c);
    server.restart();
    let mut c = server.conn();
    assert_deadlines(&mut c, "after an AOF replay", &want, same);
    // Rewritten: the hashes go into the preamble.
    assert!(matches!(c.cmd(&["BGREWRITEAOF"]), V::Str(_)));
    wait_until("the rewrite to finish", 60, || {
        c.info("persistence", "aof_rewrite_in_progress") == "0"
    });
    std::thread::sleep(Duration::from_millis(1_500));
    drop(c);
    server.restart();
    assert_deadlines(
        &mut server.conn(),
        "after a replay of the preamble",
        &want,
        same,
    );
}

#[test]
fn a_field_deadline_is_kept_to_the_millisecond_by_a_full_sync_and_the_stream() {
    let master = Server::start(&[]);
    let mut m = master.conn();
    let synced = write_hashes(&mut m, "h");
    let port = master.port.to_string();
    let replica = Server::start(&["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    wait_until("the replica's link to come up", 60, || {
        r.info("replication", "master_link_status") == "up"
    });
    let streamed = write_hashes(&mut m, "s");
    wait_until("the replica to catch up", 60, || {
        let offset = m.info("replication", "master_repl_offset");
        r.info("replication", "slave_repl_offset") == offset
    });
    assert_deadlines(&mut r, "after a full sync", &synced, same);
    assert_deadlines(&mut r, "from the stream", &streamed, same);
}

#[test]
fn a_field_deadline_is_kept_to_the_millisecond_by_dump_restore_and_migrate() {
    let source = Server::start(&[]);
    let target = Server::start(&[]);
    let mut c = source.conn();
    let want = write_hashes(&mut c, "h");
    for (key, _) in &want {
        let V::Str(payload) = c.cmd(&["DUMP", key]) else {
            panic!("no DUMP of {key}");
        };
        let copy = format!("r{key}");
        let reply = c.raw(&[b"RESTORE", copy.as_bytes(), b"0", &payload]);
        assert!(reply.ok(), "RESTORE {copy}: {reply:?}");
    }
    assert_deadlines(&mut c, "after DUMP and RESTORE", &want, |key| {
        format!("r{key}")
    });

    let port = target.port.to_string();
    let mut migrate = vec!["MIGRATE", "127.0.0.1", &port, "", "0", "10000", "KEYS"];
    migrate.extend(want.iter().map(|(key, _)| key.as_str()));
    let reply = c.cmd(&migrate);
    assert!(reply.ok(), "MIGRATE: {reply:?}");
    assert_eq!(c.cmd(&["EXISTS", "h0"]).int(), 0);
    assert_deadlines(&mut target.conn(), "after MIGRATE", &want, same);
}

/// The latest deadline Redis lets a hash field have is 2^46 - 1 ms after
/// the epoch. One at it is kept, to the millisecond, by `DUMP` and
/// `RESTORE` and by a save and a restart; one past it, absolute or
/// relative, is refused with Redis's error. `HEXPIRE k 18446744073709552`
/// used to be taken and saved cut to 64 bits, a moment from then, so the
/// field was gone after a restart; `HEXPIRE k 9223372036854775807` stopped
/// the server.
#[test]
fn a_field_deadline_past_what_redis_allows_is_refused_and_the_latest_is_kept() {
    const MAX: u64 = (1 << 46) - 1;
    let mut server = Server::start(&[]);
    let mut c = server.conn();
    assert_eq!(c.cmd(&["HSET", "h", "a", "1", "b", "2"]).int(), 2);
    let max = MAX.to_string();
    let past = (MAX + 1).to_string();
    let past_secs = (MAX / 1000 + 1).to_string();
    for (args, command) in [
        (
            vec!["HPEXPIREAT", "h", &past, "FIELDS", "1", "a"],
            "hpexpireat",
        ),
        (
            vec!["HEXPIREAT", "h", &past_secs, "FIELDS", "1", "a"],
            "hexpireat",
        ),
        (vec!["HPEXPIRE", "h", &max, "FIELDS", "1", "a"], "hpexpire"),
        (
            vec!["HEXPIRE", "h", "18446744073709552", "FIELDS", "1", "a"],
            "hexpire",
        ),
        (
            vec!["HEXPIRE", "h", "9223372036854775807", "FIELDS", "1", "a"],
            "hexpire",
        ),
        (
            vec!["HGETEX", "h", "PXAT", &past, "FIELDS", "1", "a"],
            "hgetex",
        ),
        (
            vec![
                "HGETEX",
                "h",
                "EX",
                "9223372036854775807",
                "FIELDS",
                "1",
                "a",
            ],
            "hgetex",
        ),
        (
            vec!["HSETEX", "h", "PXAT", &past, "FIELDS", "1", "a", "1"],
            "hsetex",
        ),
        (
            vec!["HSETEX", "h", "EXAT", &past_secs, "FIELDS", "1", "a", "1"],
            "hsetex",
        ),
        (
            vec!["HSETEX", "h", "PX", &max, "FIELDS", "1", "a", "1"],
            "hsetex",
        ),
    ] {
        let reply = c.cmd(&args);
        assert_eq!(
            reply,
            V::Err(format!("ERR invalid expire time in '{command}' command")),
            "{args:?}"
        );
    }
    assert_eq!(
        c.cmd(&["HPTTL", "h", "FIELDS", "1", "a"]),
        V::Arr(vec![V::Int(-1)])
    );

    // The latest one Redis allows, and one at the same second.
    let max_secs = (MAX / 1000).to_string();
    assert_eq!(
        c.cmd(&["HPEXPIREAT", "h", &max, "FIELDS", "1", "a"]),
        V::Arr(vec![V::Int(1)])
    );
    assert_eq!(
        c.cmd(&["HEXPIREAT", "h", &max_secs, "FIELDS", "1", "b"]),
        V::Arr(vec![V::Int(1)])
    );
    let want = V::Arr(vec![V::Int(MAX as i64), V::Int((MAX / 1000 * 1000) as i64)]);
    let deadlines =
        |c: &mut Conn, key: &str| c.cmd(&["HPEXPIRETIME", key, "FIELDS", "2", "a", "b"]);
    assert_eq!(deadlines(&mut c, "h"), want);
    let V::Str(payload) = c.cmd(&["DUMP", "h"]) else {
        panic!("no DUMP of h");
    };
    assert!(c.raw(&[b"RESTORE", b"h2", b"0", &payload]).ok());
    assert_eq!(deadlines(&mut c, "h2"), want, "after DUMP and RESTORE");
    assert!(c.cmd(&["SAVE"]).ok());
    drop(c);
    server.restart();
    let mut c = server.conn();
    assert_eq!(deadlines(&mut c, "h"), want, "after a restart");
    assert_eq!(deadlines(&mut c, "h2"), want, "after a restart");
}
