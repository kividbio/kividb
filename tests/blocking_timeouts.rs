// tests/blocking_timeouts.rs
//
// What a blocking command answers when it ends with nothing, in RESP2 and in
// RESP3, compared byte for byte with Redis (#385): a timeout — or
// `CLIENT UNBLOCK … TIMEOUT` — is a null array (`*-1`, `_` in RESP3) for
// every blocking command, `BRPOPLPUSH` and `BLMOVE` included, whose answer
// is otherwise a bulk string. Where a blocking command cannot wait (inside
// `MULTI`), Redis answers `BRPOPLPUSH` and `BLMOVE` with a null bulk string
// (`$-1`) and the others with a null array, and the non-blocking `RPOPLPUSH`
// and `LMOVE` on an empty list with a null bulk string.
//
// Each test runs a real `kividb` on each I/O model there is: tokio
// everywhere, and io_uring as well on Linux.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(20)))
                    .unwrap();
                let writer = stream.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(stream),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    /// The next reply, exactly as it was sent.
    fn raw(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        assert!(!line.is_empty(), "the connection was closed");
        let head = line.trim_end_matches(['\r', '\n']).to_string();
        let (kind, rest) = head.split_at(1);
        match kind {
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len >= 0 {
                    let mut data = vec![0u8; len as usize + 2];
                    self.reader.read_exact(&mut data).unwrap();
                    line.push_str(&String::from_utf8_lossy(&data));
                }
            }
            "*" | "%" | "~" | ">" => {
                let n: i64 = rest.parse().unwrap();
                let n = if kind == "%" { n * 2 } else { n };
                for _ in 0..n.max(0) {
                    line.push_str(&self.raw());
                }
            }
            _ => {}
        }
        line
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.raw()
    }
}

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see `tests/uring.rs`).
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

/// The I/O models to run each test on: both, where io_uring exists — or the
/// one `KIVIDB_TEST_IO_MODEL` names.
fn io_models() -> &'static [&'static str] {
    match std::env::var("KIVIDB_TEST_IO_MODEL").as_deref() {
        Ok("tokio") => &["tokio"],
        Ok("uring") => &["uring"],
        _ if cfg!(target_os = "linux") => &["tokio", "uring"],
        _ => &["tokio"],
    }
}

struct Server {
    port: u16,
    child: Child,
    dir: PathBuf,
}

impl Server {
    fn start(io: &str) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&dir)
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--io-model",
                io,
                "--threads",
                "4",
                "--configfile",
                "kividb.conf",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, child, dir };
        assert_eq!(server.conn(2).cmd(&["PING"]), "+PONG\r\n");
        if io == "uring" {
            let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
            assert!(log.contains("I/O model      : io_uring"), "{log}");
        }
        server
    }

    /// A connection speaking `resp`.
    fn conn(&self, resp: u8) -> Conn {
        let mut c = Conn::connect(self.port);
        if resp == 3 {
            let hello = c.cmd(&["HELLO", "3"]);
            assert!(hello.starts_with('%'), "{hello:?}");
        }
        c
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Every blocking command, on keys that stay empty, with a timeout of a
/// tenth of a second.
const BLOCKING: &[&[&str]] = &[
    &["BLPOP", "t:list", "0.1"],
    &["BRPOP", "t:list", "0.1"],
    &["BRPOPLPUSH", "t:list", "t:dst", "0.1"],
    &["BLMOVE", "t:list", "t:dst", "LEFT", "RIGHT", "0.1"],
    &["BLMPOP", "0.1", "1", "t:list", "LEFT"],
    &[
        "BLMOVEM", "t:list", "t:dst", "LEFT", "RIGHT", "0.1", "COUNT", "2", "OBO",
    ],
    &["BZPOPMIN", "t:zset", "0.1"],
    &["BZPOPMAX", "t:zset", "0.1"],
    &["BZMPOP", "0.1", "1", "t:zset", "MIN"],
    &["XREAD", "BLOCK", "100", "STREAMS", "t:stream", "$"],
    &[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "BLOCK",
        "100",
        "STREAMS",
        "t:stream",
        ">",
    ],
];

fn null_array(resp: u8) -> &'static str {
    if resp == 3 { "_\r\n" } else { "*-1\r\n" }
}

fn null_bulk(resp: u8) -> &'static str {
    if resp == 3 { "_\r\n" } else { "$-1\r\n" }
}

/// Each blocking command that times out answers with a null array, in
/// RESP2 and in RESP3. `BRPOPLPUSH` and `BLMOVE` used to answer `$-1` in
/// RESP2.
#[test]
fn every_blocking_command_times_out_with_a_null_array() {
    // Every command is tried, and then each that answered otherwise is told.
    let mut wrong = Vec::new();
    for &io in io_models() {
        let server = Server::start(io);
        for resp in [2, 3] {
            let mut c = server.conn(resp);
            assert_eq!(
                c.cmd(&["XGROUP", "CREATE", "t:stream", "g", "$", "MKSTREAM"]),
                "+OK\r\n"
            );
            for block in BLOCKING {
                let reply = c.cmd(block);
                if reply != null_array(resp) {
                    wrong.push(format!("{io} RESP{resp} {}: {reply:?}", block[0]));
                }
            }
            assert_eq!(c.cmd(&["DEL", "t:stream"]), ":1\r\n", "{io}");
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// A wait ended by `CLIENT UNBLOCK … TIMEOUT` is answered as a timeout.
#[test]
fn a_wait_unblocked_as_a_timeout_ends_with_a_null_array() {
    for &io in io_models() {
        let server = Server::start(io);
        for resp in [2, 3] {
            let mut ctl = server.conn(2);
            for block in [
                &["BRPOPLPUSH", "u:list", "u:dst", "0"][..],
                &["BLMOVE", "u:list", "u:dst", "RIGHT", "LEFT", "0"],
                &["BLPOP", "u:list", "0"],
            ] {
                let mut c = server.conn(resp);
                let id = c.cmd(&["CLIENT", "ID"]);
                let id = id.trim_start_matches(':').trim_end().to_string();
                c.send(block);
                let deadline = Instant::now() + Duration::from_secs(10);
                while ctl.cmd(&["CLIENT", "UNBLOCK", &id, "TIMEOUT"]) != ":1\r\n" {
                    assert!(Instant::now() < deadline, "{io} {block:?}: never blocked");
                    thread::sleep(Duration::from_millis(5));
                }
                assert_eq!(c.raw(), null_array(resp), "{io} RESP{resp} {block:?}");
            }
        }
    }
}

/// Inside `MULTI` a blocking command cannot wait, and answers at once:
/// `BRPOPLPUSH` and `BLMOVE` with a null bulk string, the others with a null
/// array, as Redis answers them there.
#[test]
fn inside_multi_brpoplpush_and_blmove_answer_with_a_null_bulk_string() {
    for &io in io_models() {
        let server = Server::start(io);
        for resp in [2, 3] {
            let mut c = server.conn(resp);
            assert_eq!(c.cmd(&["MULTI"]), "+OK\r\n");
            // A timeout so short that it has passed before the command has
            // looked at its key changes nothing: it never waited.
            for block in [
                &["BRPOPLPUSH", "m:list", "m:dst", "0"][..],
                &["BLMOVE", "m:list", "m:dst", "LEFT", "RIGHT", "0"],
                &["BRPOPLPUSH", "m:list", "m:dst", "0.0000001"],
                &["BLMOVE", "m:list", "m:dst", "LEFT", "RIGHT", "0.0000001"],
                &["BLPOP", "m:list", "0"],
                &["BZPOPMIN", "m:zset", "0"],
            ] {
                assert_eq!(c.cmd(block), "+QUEUED\r\n", "{io} {block:?}");
            }
            let want = format!(
                "*6\r\n{}{}{}{}{}{}",
                null_bulk(resp),
                null_bulk(resp),
                null_bulk(resp),
                null_bulk(resp),
                null_array(resp),
                null_array(resp)
            );
            assert_eq!(c.cmd(&["EXEC"]), want, "{io} RESP{resp}");
        }
    }
}

/// In a script, too, a blocking move on an empty list cannot wait, however
/// short its timeout, and the script is given `false` for it, as in Redis.
/// (A null bulk string and a null array both reach Lua as `false`: this
/// pins what the script sees, not which null it was.)
#[cfg(feature = "lua")]
#[test]
fn in_a_script_a_blocking_move_on_an_empty_list_is_false() {
    let script = "local r = redis.call(unpack(ARGV)) \
                  if r == false then return 'false' end return r";
    for &io in io_models() {
        let server = Server::start(io);
        let mut c = server.conn(2);
        for timeout in ["0", "0.0000001", "0.001"] {
            for call in [
                &["BRPOPLPUSH", "s:list", "s:dst", timeout][..],
                &["BLMOVE", "s:list", "s:dst", "LEFT", "RIGHT", timeout],
            ] {
                let mut args = vec!["EVAL", script, "0"];
                args.extend_from_slice(call);
                assert_eq!(c.cmd(&args), "$5\r\nfalse\r\n", "{io} {call:?}");
            }
        }
    }
}

/// The non-blocking moves on an empty list answer with a null bulk string,
/// as in Redis: only the blocking ones that time out answer with an array.
#[test]
fn rpoplpush_and_lmove_on_an_empty_list_answer_with_a_null_bulk_string() {
    for &io in io_models() {
        let server = Server::start(io);
        for resp in [2, 3] {
            let mut c = server.conn(resp);
            assert_eq!(
                c.cmd(&["RPOPLPUSH", "n:list", "n:dst"]),
                null_bulk(resp),
                "{io} RESP{resp}"
            );
            assert_eq!(
                c.cmd(&["LMOVE", "n:list", "n:dst", "LEFT", "RIGHT"]),
                null_bulk(resp),
                "{io} RESP{resp}"
            );
        }
    }
}

const NOT_A_FLOAT: &str = "-ERR timeout is not a float or out of range\r\n";
const OUT_OF_RANGE: &str = "-ERR timeout is out of range\r\n";
const NEGATIVE: &str = "-ERR timeout is negative\r\n";
const NIL: &str = "*-1\r\n";

/// Every timeout text below with what Redis 8.10.2 answers `BLPOP` (all
/// the blocking commands read it alike). Redis reads a `long double`: a
/// number too small for an `f64` is a 1 ms timeout, one below half the
/// smallest subnormal (about 1.8226e-4951) or above the largest value
/// (about 1.1897e4932) is not a float. The one deliberate difference:
/// Redis 8.10.2 waits forever on a negative timeout that is under half a
/// millisecond (`-1e-343`); KiviDB refuses every negative one.
const TIMEOUTS: &[(&str, &str)] = &[
    ("1e-343", NIL),
    ("1e-400", NIL),
    ("+1e-343", NIL),
    (".5e-343", NIL),
    ("5.e-343", NIL),
    ("1e-4950", NIL),
    ("1000e-4953", NIL),
    ("1.83e-4951", NIL),
    ("2e-4951", NIL),
    ("3e-4951", NIL),
    ("4e-4951", NIL),
    ("1e-4951", NOT_A_FLOAT),
    ("1.8e-4951", NOT_A_FLOAT),
    ("1.82e-4951", NOT_A_FLOAT),
    ("1e-4952", NOT_A_FLOAT),
    ("1e-5000", NOT_A_FLOAT),
    ("0.001e-4948", NOT_A_FLOAT),
    ("0.0001e-4947", NOT_A_FLOAT),
    ("-1e-5000", NOT_A_FLOAT),
    ("1e-99999999999999999999", NOT_A_FLOAT),
    ("1e99999999999999999999", NOT_A_FLOAT),
    ("1.2e4932", NOT_A_FLOAT),
    ("1e4933", NOT_A_FLOAT),
    ("1e5000", NOT_A_FLOAT),
    ("xe-5", NOT_A_FLOAT),
    ("1.2.3e-400", NOT_A_FLOAT),
    ("1e2e3", NOT_A_FLOAT),
    ("1e", NOT_A_FLOAT),
    ("1e-", NOT_A_FLOAT),
    (".", NOT_A_FLOAT),
    ("", NOT_A_FLOAT),
    (" 1", NOT_A_FLOAT),
    ("1 ", NOT_A_FLOAT),
    ("1abc", NOT_A_FLOAT),
    ("nan", NOT_A_FLOAT),
    ("inf", OUT_OF_RANGE),
    ("+inf", OUT_OF_RANGE),
    ("2e308", OUT_OF_RANGE),
    ("1e4000", OUT_OF_RANGE),
    ("1e4932", OUT_OF_RANGE),
    ("1.1e4932", OUT_OF_RANGE),
    ("-1", NEGATIVE),
    ("-inf", NEGATIVE),
    ("-2e308", NEGATIVE),
    ("-1e-343", NEGATIVE),
    ("-1e-4950", NEGATIVE),
];

/// Each timeout text is answered as Redis answers it, and a tiny one is
/// answered at once.
#[test]
fn blocking_timeout_extreme_floats() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut c = server.conn(2);
        for &(text, want) in TIMEOUTS {
            let started = Instant::now();
            assert_eq!(c.cmd(&["BLPOP", "n:list", text]), want, "{io} {text:?}");
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "{io} {text:?} took {:?}",
                started.elapsed()
            );
        }
    }
}

/// Every blocking command that reads a timeout shares the reading:
/// `1e-343` and `1e-4950` wait 1 ms, `1e-5000` and `-1e-343` are refused.
/// (`XREAD ... BLOCK` takes whole milliseconds and is another reading.)
#[test]
fn every_blocking_command_reads_a_tiny_timeout_alike() {
    let commands: &[&dyn Fn(&str) -> Vec<String>] = &[
        &|t| ["BLPOP", "k:list", t].map(String::from).to_vec(),
        &|t| ["BRPOP", "k:list", t].map(String::from).to_vec(),
        &|t| ["BRPOPLPUSH", "k:list", "k:dst", t].map(String::from).to_vec(),
        &|t| {
            ["BLMOVE", "k:list", "k:dst", "LEFT", "RIGHT", t]
                .map(String::from)
                .to_vec()
        },
        &|t| ["BLMPOP", t, "1", "k:list", "LEFT"].map(String::from).to_vec(),
        &|t| {
            ["BLMOVEM", "k:list", "k:dst", "LEFT", "RIGHT", t, "COUNT", "2", "OBO"]
                .map(String::from)
                .to_vec()
        },
        &|t| ["BZPOPMIN", "k:zset", t].map(String::from).to_vec(),
        &|t| ["BZPOPMAX", "k:zset", t].map(String::from).to_vec(),
        &|t| ["BZMPOP", t, "1", "k:zset", "MIN"].map(String::from).to_vec(),
    ];
    for &io in io_models() {
        let server = Server::start(io);
        let mut c = server.conn(2);
        for make in commands {
            for (text, want) in [
                ("1e-343", NIL),
                ("1e-4950", NIL),
                ("1e-5000", NOT_A_FLOAT),
                ("-1e-343", NEGATIVE),
                ("2e308", OUT_OF_RANGE),
            ] {
                let args = make(text);
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                let started = Instant::now();
                assert_eq!(c.cmd(&args), want, "{io} {args:?}");
                assert!(
                    started.elapsed() < Duration::from_secs(2),
                    "{io} {args:?} took {:?}",
                    started.elapsed()
                );
            }
        }
    }
}

/// A zero timeout, in any spelling, still waits for ever: nothing answers
/// until a second connection pushes an element.
#[test]
fn a_zero_timeout_still_blocks_until_an_element_arrives() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut pusher = server.conn(2);
        for (n, text) in ["0", "0.0", "0e10", "-0", "-0.0"].iter().enumerate() {
            let key = format!("z:list{n}");
            let mut waiter = server.conn(2);
            waiter
                .reader
                .get_ref()
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            waiter.send(&["BLPOP", &key, text]);
            let mut line = String::new();
            let early = waiter.reader.read_line(&mut line);
            assert!(
                early.is_err(),
                "{io} {text}: BLPOP answered {line:?} before any push"
            );
            waiter
                .reader
                .get_ref()
                .set_read_timeout(Some(Duration::from_secs(20)))
                .unwrap();
            assert_eq!(pusher.cmd(&["LPUSH", &key, "v"]), ":1\r\n", "{io} {text}");
            let want = format!("*2\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len());
            assert_eq!(waiter.raw(), want, "{io} {text}");
        }
    }
}
