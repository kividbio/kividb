// tests/expire_bounds.rs
//
// A time to live or a Unix time past the latest deadline Redis allows is
// refused with Redis's `ERR invalid expire time in '<command>' command`, by
// every command that sets one, and the connection and the server go on.
// Some of them added it to the clock with `+`, which panics past what the
// clock holds: the connection died. Others took it, and the key read back a
// negative `PEXPIRETIME`.
//
// Real `kividb` processes, on both I/O models: a panic is the point of
// interest, and it is the I/O loop that meets it.

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

    fn cmd(&mut self, args: &[&str]) -> V {
        let raw: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&raw)
            .unwrap_or_else(|| panic!("the connection closed on {args:?}"))
    }

    /// `None` when the connection is closed.
    fn raw(&mut self, args: &[&[u8]]) -> Option<V> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    /// `None` when the connection is closed.
    fn read(&mut self) -> Option<V> {
        let mut line = String::new();
        if self.reader.read_line(&mut line).unwrap_or(0) == 0 {
            return None;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        Some(match kind {
            "+" => V::Str(rest.as_bytes().to_vec()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return Some(V::Nil);
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Str(data)
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Some(V::Nil);
                }
                V::Arr((0..n).map(|_| self.read().unwrap()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        })
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
        let from = (std::process::id() as u16).wrapping_mul(47);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

struct Server {
    port: u16,
    dir: PathBuf,
    child: Child,
}

impl Server {
    /// `model` is the I/O model; `None` is the platform's default.
    fn start(model: Option<&str>) -> Self {
        Self::start_in(tempfile::tempdir().unwrap().keep(), model, &[])
    }

    /// Started in `dir`, which may already hold files for it to load.
    fn start_in(dir: PathBuf, model: Option<&str>, extra: &[&str]) -> Self {
        let port = free_port();
        let child = kividb(&dir, port, model, extra);
        let server = Server { port, dir, child };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let up = TcpStream::connect(("127.0.0.1", port)).and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(10)))?;
                s.write_all(b"*1\r\n$4\r\nPING\r\n")?;
                let mut reply = [0u8; 7];
                s.read_exact(&mut reply)?;
                Ok(&reply == b"+PONG\r\n")
            });
            if up.unwrap_or(false) {
                return server;
            }
            assert!(Instant::now() < deadline, "the server did not come up");
            std::thread::sleep(Duration::from_millis(50));
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
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn kividb(dir: &Path, port: u16, model: Option<&str>, extra: &[&str]) -> Child {
    let log = std::fs::File::create(dir.join("server.log")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
    command.current_dir(dir).args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--threads",
        "2",
    ]);
    let model = model
        .map(str::to_string)
        .or_else(|| std::env::var("KIVIDB_TEST_IO_MODEL").ok());
    if let Some(model) = model {
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

fn invalid(command: &str) -> V {
    V::Err(format!("ERR invalid expire time in '{command}' command"))
}

fn syntax() -> V {
    V::Err("ERR syntax error".into())
}

/// Redis's reply to a number past a `long long`, which it reads times as.
fn not_int() -> V {
    V::Err("ERR value is not an integer or out of range".into())
}

/// Each command is refused as Redis refuses it; the connection answers the
/// next command, and nothing was set.
fn refuses_past_the_latest_deadline(model: Option<&str>) {
    let server = Server::start(model);
    let mut c = server.conn();
    let u64_max = u64::MAX.to_string();
    let i64_max = i64::MAX.to_string();
    // One second past the latest Unix time in seconds, one millisecond past
    // the latest in milliseconds.
    let secs_past = (i64::MAX / 1000 + 1).to_string();
    let ms_past = (i64::MAX as u64 + 1).to_string();
    let latest_secs = (i64::MAX / 1000).to_string();
    let past_u64 = "99999999999999999999";

    let cases: Vec<(Vec<&str>, V)> = vec![
        (vec!["SET", "k", "v", "EXAT", &u64_max], not_int()),
        (vec!["SET", "k", "v", "EXAT", &secs_past], invalid("set")),
        (vec!["SET", "k", "v", "PXAT", &u64_max], not_int()),
        (vec!["SET", "k", "v", "PXAT", &ms_past], not_int()),
        (vec!["SET", "k", "v", "EX", &i64_max], invalid("set")),
        (vec!["SET", "k", "v", "PX", &i64_max], invalid("set")),
        (vec!["GETEX", "g", "EXAT", &u64_max], not_int()),
        (vec!["GETEX", "g", "EXAT", &secs_past], invalid("getex")),
        (vec!["GETEX", "g", "PXAT", &ms_past], not_int()),
        (vec!["GETEX", "g", "EX", &i64_max], invalid("getex")),
        (vec!["SETEX", "k", &u64_max, "v"], not_int()),
        (vec!["SETEX", "k", &i64_max, "v"], invalid("setex")),
        (vec!["PSETEX", "k", &u64_max, "v"], not_int()),
        (vec!["PSETEX", "k", &i64_max, "v"], invalid("psetex")),
        (vec!["MSETEX", "1", "k", "v", "EX", &u64_max], not_int()),
        (
            vec!["MSETEX", "1", "k", "v", "EX", &i64_max],
            invalid("msetex"),
        ),
        (
            vec!["MSETEX", "1", "k", "v", "PX", &i64_max],
            invalid("msetex"),
        ),
        (vec!["MSETEX", "1", "k", "v", "EXAT", &u64_max], not_int()),
        (vec!["MSETEX", "1", "k", "v", "PXAT", &ms_past], not_int()),
        (vec!["EXPIRE", "g", &i64_max], invalid("expire")),
        (vec!["PEXPIRE", "g", &i64_max], invalid("pexpire")),
        (vec!["EXPIREAT", "g", &secs_past], invalid("expireat")),
        (vec!["PEXPIREAT", "g", &ms_past], not_int()),
        // Not an integer, or past one: that, before any bound (#561).
        (vec!["SET", "k", "v", "EX", "abc"], not_int()),
        (vec!["SET", "k", "v", "EXAT", "abc"], not_int()),
        (vec!["SET", "k", "v", "PXAT", past_u64], not_int()),
        (vec!["GETEX", "g", "EXAT", "abc"], not_int()),
        (vec!["SETEX", "k", "abc", "v"], not_int()),
        (vec!["SETEX", "k", past_u64, "v"], not_int()),
        (vec!["PSETEX", "k", "abc", "v"], not_int()),
        (vec!["MSETEX", "1", "k", "v", "EXAT", "abc"], not_int()),
        (vec!["MSETEX", "1", "k", "v", "PXAT", past_u64], not_int()),
        (vec!["EXPIRE", "g", "abc"], not_int()),
        (vec!["EXPIRE", "g", &ms_past], not_int()),
        (vec!["PEXPIRE", "g", &ms_past], not_int()),
        (vec!["EXPIREAT", "g", &ms_past], not_int()),
        (vec!["PEXPIREAT", "g", "abc"], not_int()),
        // A Unix time of 0, or a time to live of 0 or less.
        (vec!["SET", "k", "v", "EXAT", "0"], invalid("set")),
        (vec!["SET", "k", "v", "PXAT", "0"], invalid("set")),
        (vec!["SET", "k", "v", "EXAT", "-1"], invalid("set")),
        (vec!["GETEX", "g", "EXAT", "0"], invalid("getex")),
        (vec!["GETEX", "g", "PXAT", "0"], invalid("getex")),
        (
            vec!["MSETEX", "1", "k", "v", "EXAT", "0"],
            invalid("msetex"),
        ),
        (
            vec!["MSETEX", "1", "k", "v", "PXAT", "0"],
            invalid("msetex"),
        ),
        (vec!["SETEX", "k", "0", "v"], invalid("setex")),
        (vec!["PSETEX", "k", "-1", "v"], invalid("psetex")),
        // Two units, or a unit with KEEPTTL.
        (vec!["SET", "k", "v", "KEEPTTL", "EX", "10"], syntax()),
        (vec!["SET", "k", "v", "EX", "10", "KEEPTTL"], syntax()),
        (vec!["SET", "k", "v", "EX", "10", "PX", "10"], syntax()),
        (vec!["SET", "k", "v", "EX", "abc", "FOO"], syntax()),
        (vec!["GETEX", "g", "EX", "10", "PX", "10"], syntax()),
        (vec!["GETEX", "g", "PERSIST", "EX", "10"], syntax()),
        (
            vec!["MSETEX", "1", "k", "v", "EX", "10", "PX", "10"],
            syntax(),
        ),
        (
            vec!["MSETEX", "1", "k", "v", "KEEPTTL", "EX", "10"],
            syntax(),
        ),
        (vec!["MSETEX", "1", "k", "v", "NX", "XX"], syntax()),
        (vec!["INCREX", "n", "EX", &latest_secs], invalid("increx")),
        (vec!["INCREX", "n", "PX", &i64_max], invalid("increx")),
        (
            vec!["HEXPIRE", "h", &i64_max, "FIELDS", "1", "f"],
            invalid("hexpire"),
        ),
        (
            vec!["HPEXPIRE", "h", &i64_max, "FIELDS", "1", "f"],
            invalid("hpexpire"),
        ),
        (
            vec!["HEXPIREAT", "h", &i64_max, "FIELDS", "1", "f"],
            invalid("hexpireat"),
        ),
        (
            vec!["HPEXPIREAT", "h", &i64_max, "FIELDS", "1", "f"],
            invalid("hpexpireat"),
        ),
        (
            vec!["HGETEX", "h", "EX", &i64_max, "FIELDS", "1", "f"],
            invalid("hgetex"),
        ),
        (
            vec!["HGETEX", "h", "PX", &i64_max, "FIELDS", "1", "f"],
            invalid("hgetex"),
        ),
        (
            vec!["HSETEX", "h", &i64_max, "FIELDS", "1", "f", "v"],
            invalid("hsetex"),
        ),
        (
            vec!["HSETEX", "h", "EX", &i64_max, "FIELDS", "1", "f", "v"],
            invalid("hsetex"),
        ),
        (
            vec!["HSETEX", "h", "PX", &i64_max, "FIELDS", "1", "f", "v"],
            invalid("hsetex"),
        ),
    ];
    assert_eq!(c.cmd(&["HSET", "h", "f", "1"]), V::Int(1));
    for (args, want) in cases {
        // `g` is set again each time: a command that took its time to live
        // shows in its `PTTL` below.
        assert_eq!(c.cmd(&["SET", "g", "1"]), V::Str(b"OK".to_vec()));
        assert_eq!(c.cmd(&args), want, "{args:?}");
        assert_eq!(c.cmd(&["EXISTS", "k"]), V::Int(0), "{args:?} set k");
        assert_eq!(c.cmd(&["PING"]), V::Str(b"PONG".to_vec()), "after {args:?}");
        assert_eq!(c.cmd(&["PTTL", "g"]), V::Int(-1), "after {args:?}");
        assert_eq!(
            c.cmd(&["HPTTL", "h", "FIELDS", "1", "f"]),
            V::Arr(vec![V::Int(-1)]),
            "after {args:?}"
        );
    }
    // Another client is served too.
    assert_eq!(server.conn().cmd(&["PING"]), V::Str(b"PONG".to_vec()));
}

#[test]
fn a_deadline_past_the_latest_is_refused_on_the_default_io_model() {
    refuses_past_the_latest_deadline(None);
}

#[test]
fn a_deadline_past_the_latest_is_refused_on_tokio() {
    refuses_past_the_latest_deadline(Some("tokio"));
}

/// The latest deadline Redis allows is taken, and reads back as itself:
/// never negative, as one past `i64::MAX` milliseconds did. Not on Windows,
/// whose clock ends long before `i64::MAX` milliseconds: there it is refused.
#[cfg(not(windows))]
#[test]
fn the_latest_deadline_is_taken_and_reads_back_as_itself() {
    let server = Server::start(None);
    let mut c = server.conn();
    let i64_max = i64::MAX.to_string();
    let latest_secs = (i64::MAX / 1000).to_string();

    assert_eq!(
        c.cmd(&["SET", "k", "v", "PXAT", &i64_max]),
        V::Str(b"OK".to_vec())
    );
    assert_eq!(c.cmd(&["PEXPIRETIME", "k"]), V::Int(i64::MAX));
    // Rounded to the nearest second, as Redis 8.10 has it.
    assert_eq!(c.cmd(&["EXPIRETIME", "k"]), V::Int(9223372036854776));
    let V::Int(left) = c.cmd(&["PTTL", "k"]) else {
        panic!("no PTTL")
    };
    assert!(left > 0, "PTTL {left}");

    assert_eq!(c.cmd(&["SET", "k2", "v"]), V::Str(b"OK".to_vec()));
    assert_eq!(c.cmd(&["EXPIREAT", "k2", &latest_secs]), V::Int(1));
    assert_eq!(
        c.cmd(&["PEXPIRETIME", "k2"]),
        V::Int(i64::MAX / 1000 * 1000)
    );
    assert_eq!(c.cmd(&["PEXPIREAT", "k2", &i64_max]), V::Int(1));
    assert_eq!(c.cmd(&["PEXPIRETIME", "k2"]), V::Int(i64::MAX));

    // `RESTORE` with a time to live that ends past the latest: kept as the
    // latest, which reads back as itself, where it read back negative.
    let Some(V::Str(payload)) = c.raw(&[b"DUMP", b"k2"]) else {
        panic!("no DUMP of k2")
    };
    let restored = c.raw(&[b"RESTORE", b"r", i64_max.as_bytes(), &payload]);
    assert_eq!(restored, Some(V::Str(b"OK".to_vec())));
    assert_eq!(c.cmd(&["PEXPIRETIME", "r"]), V::Int(i64::MAX));

    // A hash field: 2^46 - 1 ms after the epoch, Redis's latest.
    let latest_field = ((1i64 << 46) - 1).to_string();
    assert_eq!(c.cmd(&["HSET", "h", "f", "1"]), V::Int(1));
    assert_eq!(
        c.cmd(&["HPEXPIREAT", "h", &latest_field, "FIELDS", "1", "f"]),
        V::Arr(vec![V::Int(1)])
    );
    assert_eq!(
        c.cmd(&["HPEXPIRETIME", "h", "FIELDS", "1", "f"]),
        V::Arr(vec![V::Int((1i64 << 46) - 1)])
    );
}

/// The options as Redis 8.10 reads them: the same unit given again replaces
/// the first, and only the last time is read; `GETEX` on a key that is not
/// there is nil whatever the time.
#[test]
fn the_options_are_read_as_redis_reads_them() {
    let server = Server::start(None);
    let mut c = server.conn();
    let ok = V::Str(b"OK".to_vec());
    let ttl = |c: &mut Conn, key: &str| match c.cmd(&["TTL", key]) {
        V::Int(n) => n,
        other => panic!("TTL {key}: {other:?}"),
    };

    assert_eq!(c.cmd(&["SET", "a", "v", "EX", "10", "EX", "20"]), ok);
    assert_eq!(ttl(&mut c, "a"), 20);
    assert_eq!(c.cmd(&["SET", "b", "v", "EX", "abc", "EX", "30"]), ok);
    assert_eq!(ttl(&mut c, "b"), 30);
    assert_eq!(c.cmd(&["SET", "c", "v", "KEEPTTL", "KEEPTTL"]), ok);
    assert_eq!(
        c.cmd(&["GETEX", "a", "EX", "40", "EX", "50"]),
        V::Str(b"v".to_vec())
    );
    assert_eq!(ttl(&mut c, "a"), 50);
    assert_eq!(c.cmd(&["GETEX", "a", "EX", "10", "EX", "abc"]), not_int());
    assert_eq!(
        c.cmd(&["GETEX", "c", "PERSIST", "PERSIST"]),
        V::Str(b"v".to_vec())
    );
    assert_eq!(
        c.cmd(&["MSETEX", "1", "d", "v", "EX", "abc", "EX", "60"]),
        V::Int(1)
    );
    assert_eq!(ttl(&mut c, "d"), 60);
    assert_eq!(
        c.cmd(&["MSETEX", "1", "d", "v", "XX", "XX", "KEEPTTL", "KEEPTTL"]),
        V::Int(1)
    );
    assert_eq!(ttl(&mut c, "d"), 60);

    // A key that is not there: nil, before the time is read.
    for args in [
        &["GETEX", "nosuch", "EX", "abc"][..],
        &["GETEX", "nosuch", "EX", "0"],
        &["GETEX", "nosuch", "EXAT", "0"],
        &["GETEX", "nosuch", "PXAT", "99999999999999999999"],
    ] {
        assert_eq!(c.cmd(args), V::Nil, "{args:?}");
    }
    assert_eq!(c.cmd(&["GETEX", "nosuch", "FOO"]), syntax());
    // One of another type: WRONGTYPE, before the time.
    assert_eq!(c.cmd(&["LPUSH", "l", "x"]), V::Int(1));
    assert!(matches!(c.cmd(&["GETEX", "l", "EX", "abc"]), V::Err(e) if e.starts_with("WRONGTYPE")));
}

/// What 1.2.0 wrote to its AOF, replayed: a deadline past the latest (it
/// wrote `SET … PXAT` up to `u64::MAX` for a large `SETEX`), a `PEXPIREAT`
/// past it, and a Unix time of 0. The keys are kept, with the latest
/// deadline, where refusing the commands lost them; a Unix time of 0 is
/// still a time that has passed, as it was.
#[test]
fn an_aof_written_by_1_2_0_with_far_deadlines_keeps_its_keys() {
    let dir = tempfile::tempdir().unwrap().keep();
    let u64_max = u64::MAX.to_string();
    let ms_past = (i64::MAX as u64 + 1).to_string();
    let secs_past = (i64::MAX / 1000 + 1).to_string();
    let records: Vec<Vec<&str>> = vec![
        vec!["SELECT", "0"],
        vec!["SET", "a", "1", "PXAT", &u64_max],
        vec!["SET", "b", "2", "PXAT", &ms_past],
        vec!["SET", "c", "3"],
        vec!["PEXPIREAT", "c", &u64_max],
        vec!["SET", "d", "4"],
        vec!["EXPIREAT", "d", &secs_past],
        vec!["SET", "e", "5", "EXAT", "0"],
        vec!["SET", "e", "6"],
        vec!["SET", "f", "7", "EXAT", "0"],
        vec!["MSETEX", "1", "g", "8", "PXAT", &u64_max],
    ];
    let mut aof = Vec::new();
    for record in &records {
        aof.extend_from_slice(format!("*{}\r\n", record.len()).as_bytes());
        for part in record {
            aof.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
        }
    }
    std::fs::write(dir.join("appendonly.aof"), aof).unwrap();
    let server = Server::start_in(dir, None, &["--aof"]);
    let mut c = server.conn();
    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("g", "8")] {
        assert_eq!(
            c.cmd(&["GET", key]),
            V::Str(value.as_bytes().to_vec()),
            "{key}"
        );
        assert_eq!(c.cmd(&["PEXPIRETIME", key]), V::Int(i64::MAX), "{key}");
    }
    assert_eq!(c.cmd(&["GET", "e"]), V::Str(b"6".to_vec()));
    assert_eq!(c.cmd(&["PTTL", "e"]), V::Int(-1));
    assert_eq!(c.cmd(&["GET", "f"]), V::Nil);
}
