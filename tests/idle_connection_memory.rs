#![allow(clippy::disallowed_methods)]
// tests/idle_connection_memory.rs
//
// What an idle connection holds: little for one that has sent nothing or
// one command, and a read buffer that goes back to its small size once a
// connection that sent a large request has sent only small ones for
// `UNUSED_TURNS` turns (and, on the Tokio loop, once it has been quiet);
// while one whose requests arrive in small pieces, or that sends pipelined
// bursts between a few single commands, keeps its buffer from one to the
// next. And that a connection whose buffers went back keeps working in each
// state that outlives a read: in MULTI, blocked in BLPOP, subscribed,
// tracking keys, and with a request only partly sent. Each test runs a real
// `kividb` on both I/O models.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::start_verified;

/// What a request of this size makes the read buffer grow to: well past
/// the read buffer's own size on either loop.
const LARGE: usize = 64 * 1024;
/// Longer than the server waits before it gives a quiet client's buffers
/// back (`query-buffer-idle-resize-after`, 2 s).
const QUIET: Duration = Duration::from_millis(2600);
/// Turns in a row that need no more than a small read's room before the
/// server gives a grown read buffer back (`UNUSED_TURNS` in both loops).
const UNUSED_TURNS: usize = 16;
/// The room a connection's reads start with, and go back to.
const READ_SIZE: i64 = 4 * 1024;

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Bulk(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn text(&self) -> String {
        match self {
            V::Str(s) | V::Err(s) => s.clone(),
            V::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            V::Int(n) => n.to_string(),
            other => panic!("not a string: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
    }
    fn items(&self) -> &[V] {
        match self {
            V::Arr(items) => items,
            other => panic!("not an array: {other:?}"),
        }
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
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                stream.set_nodelay(true).unwrap();
                let writer = stream.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(stream),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn send(&mut self, args: &[&str]) {
        self.writer.write_all(&encode(args)).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).expect("a reply");
        assert!(n > 0, "the server closed the connection");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.to_string()),
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
                V::Bulk(data)
            }
            // RESP3's maps (a key and a value an item), sets and pushes are
            // read as arrays.
            "*" | "%" | "~" | ">" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                let n = if kind == "%" { n * 2 } else { n };
                V::Arr((0..n).map(|_| self.read()).collect())
            }
            "_" => V::Nil,
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }

    /// A large request, so that the read buffer grows to hold it; then
    /// small ones, one read each, so that it goes back to its small size.
    /// `small` small requests: the read buffer goes back at the
    /// `UNUSED_TURNS`th, in its turn on io_uring and before the next read on
    /// the Tokio loop.
    fn grow_then_settle(&mut self, small: usize) {
        assert!(self.cmd(&["SET", "large", &"v".repeat(LARGE)]).ok());
        for _ in 0..small {
            assert_eq!(self.cmd(&["PING"]), V::Str("PONG".into()));
        }
    }
}

/// `used_memory`, read on `ctl` until two readings 50 ms apart agree.
fn used_memory(ctl: &mut Conn) -> i64 {
    let read = |ctl: &mut Conn| -> i64 {
        let info = ctl.cmd(&["INFO", "memory"]).text();
        info.lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .expect("used_memory in INFO memory")
            .trim()
            .parse()
            .unwrap()
    };
    let mut last = read(ctl);
    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(50));
        let now = read(ctl);
        if now == last {
            return now;
        }
        last = now;
    }
    last
}

/// A request, as a client sends it.
fn encode(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

// ── The server ────────────────────────────────────────────────────────────────

/// The I/O models to run a test on: the tokio loop, and on Linux the
/// io_uring one too.
fn io_models() -> &'static [&'static str] {
    if cfg!(target_os = "linux") {
        &["tokio", "uring"]
    } else {
        &["tokio"]
    }
}

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
}

impl Server {
    fn start(io_model: &str) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let (child, port) = start_verified(|port| {
            Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(&dir)
                .args([
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--io-model",
                    io_model,
                    "--threads",
                    "2",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("could not start kividb")
        });
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn used_memory(&self) -> i64 {
        let info = self.conn().cmd(&["INFO", "memory"]).text();
        info.lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .expect("used_memory in INFO memory")
            .trim()
            .parse()
            .unwrap()
    }

    /// `field` of the client named `name` in `CLIENT LIST`.
    fn client_field(&self, name: &str, field: &str) -> u64 {
        let list = self.conn().cmd(&["CLIENT", "LIST"]).text();
        let line = list
            .lines()
            .find(|l| l.contains(&format!(" name={name} ")))
            .unwrap_or_else(|| panic!("no client {name} in {list}"));
        line.split(' ')
            .find_map(|kv| kv.strip_prefix(&format!("{field}=")))
            .unwrap_or_else(|| panic!("no {field} in {line}"))
            .parse()
            .unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ── What an idle connection holds ─────────────────────────────────────────────

/// A connection that has sent nothing, or one `PING`, holds a few
/// kilobytes: its read and reply buffers start small.
#[test]
fn an_idle_connection_holds_little() {
    const CONNS: usize = 200;
    for &io_model in io_models() {
        let server = Server::start(io_model);
        for ping in [false, true] {
            let before = server.used_memory();
            let mut conns: Vec<Conn> = (0..CONNS).map(|_| server.conn()).collect();
            if ping {
                for c in &mut conns {
                    assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
                }
            } else {
                // Each has been taken in by the server: its reply comes after.
                assert_eq!(conns[CONNS - 1].cmd(&["PING"]), V::Str("PONG".into()));
            }
            let per_conn = (server.used_memory() - before) / CONNS as i64;
            assert!(
                per_conn < 32 * 1024,
                "{io_model}, ping {ping}: {per_conn} bytes a connection"
            );
            drop(conns);
            std::thread::sleep(Duration::from_millis(300));
        }
    }
}

/// A read buffer grown for one large request goes back to its small size
/// once the connection's requests have been small for `UNUSED_TURNS` turns:
/// it is not a private one any more (`qbuf-free` 0, as for a connection
/// that never grew one), and what the server holds is what it held before
/// the large request, give or take a read's room.
#[test]
fn a_buffer_grown_for_a_large_request_goes_back() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut ctl = server.conn();
        // The key first, so that the readings differ by the buffers only.
        assert!(ctl.cmd(&["SET", "large", &"v".repeat(LARGE)]).ok());
        let mut c = server.conn();
        assert!(c.cmd(&["CLIENT", "SETNAME", "grown"]).ok());
        assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        let before = used_memory(&mut ctl);
        assert!(c.cmd(&["SET", "large", &"v".repeat(LARGE)]).ok());
        for _ in 0..=UNUSED_TURNS {
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        }
        assert_eq!(server.client_field("grown", "qbuf"), 0, "{io_model}");
        assert_eq!(server.client_field("grown", "qbuf-free"), 0, "{io_model}");
        let after = used_memory(&mut ctl);
        assert!(
            after - before < 2 * READ_SIZE,
            "{io_model}: {before} before the large request, {after} after"
        );
        assert_eq!(c.cmd(&["STRLEN", "large"]), V::Int(LARGE as i64));
    }
}

/// Requests that each arrive in several reads under a small read's room (a
/// slow sender of 8 KiB values, 2 KiB at a time): the buffer they need is
/// kept from one request to the next, rather than given back after each,
/// and given back once the connection's requests are small again.
#[test]
fn requests_arriving_in_small_pieces_keep_their_buffer() {
    const PIECE: usize = 2 * 1024;
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut ctl = server.conn();
        let mut c = server.conn();
        assert!(c.cmd(&["CLIENT", "SETNAME", "slow"]).ok());
        let request = encode(&["SET", "slow", &"s".repeat(8 * 1024)]);
        let mut send_slowly = |c: &mut Conn| {
            for piece in request.chunks(PIECE) {
                c.writer.write_all(piece).unwrap();
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(c.read().ok(), "{io_model}");
        };
        send_slowly(&mut c);
        for _ in 0..=UNUSED_TURNS {
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        }
        let small = used_memory(&mut ctl);
        for round in 0..UNUSED_TURNS + 4 {
            send_slowly(&mut c);
            let held = used_memory(&mut ctl) - small;
            assert!(
                held > READ_SIZE,
                "{io_model}, request {round}: {held} bytes over the small buffers"
            );
        }
        for _ in 0..=UNUSED_TURNS {
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        }
        let back = used_memory(&mut ctl) - small;
        assert!(
            back < READ_SIZE,
            "{io_model}: {back} bytes over the small buffers"
        );
        assert_eq!(c.cmd(&["STRLEN", "slow"]), V::Int(8 * 1024));
    }
}

/// A pipelined burst between a few single commands, again and again: the
/// buffer the burst needs is kept between bursts, rather than given back
/// after the single commands and grown again by the next burst.
#[test]
fn bursts_between_single_commands_keep_their_buffer() {
    const BURST: usize = 400;
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut ctl = server.conn();
        let mut c = server.conn();
        let value = "b".repeat(100);
        let mut burst = Vec::new();
        for i in 0..BURST {
            burst.extend_from_slice(&encode(&["SET", &format!("k{i}"), &value]));
        }
        let mut cycle = |c: &mut Conn| {
            c.writer.write_all(&burst).unwrap();
            for _ in 0..BURST {
                assert!(c.read().ok(), "{io_model}");
            }
            for i in 0..4 {
                assert_eq!(
                    c.cmd(&["GET", &format!("k{i}")]),
                    V::Bulk(value.as_bytes().to_vec()),
                    "{io_model}"
                );
            }
        };
        cycle(&mut c);
        for _ in 0..=UNUSED_TURNS {
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        }
        let small = used_memory(&mut ctl);
        for round in 0..8 {
            cycle(&mut c);
            let held = used_memory(&mut ctl) - small;
            assert!(
                held > 4 * READ_SIZE,
                "{io_model}, burst {round}: {held} bytes over the small buffers"
            );
        }
        for _ in 0..=UNUSED_TURNS {
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        }
        let back = used_memory(&mut ctl) - small;
        assert!(
            back < READ_SIZE,
            "{io_model}: {back} bytes over the small buffers"
        );
    }
}

// ── States that outlive a read ────────────────────────────────────────────────

/// A transaction queued across the buffers going back, and a quiet spell,
/// runs whole.
#[test]
fn a_transaction_survives_the_buffers_going_back() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        assert!(c.cmd(&["MULTI"]).ok());
        assert_eq!(
            c.cmd(&["SET", "large", &"v".repeat(LARGE)]),
            V::Str("QUEUED".into())
        );
        for _ in 0..5 {
            assert_eq!(c.cmd(&["INCR", "n"]), V::Str("QUEUED".into()));
        }
        std::thread::sleep(QUIET);
        let replies = c.cmd(&["EXEC"]);
        assert_eq!(replies.items().len(), 6, "{io_model}: {replies:?}");
        assert_eq!(replies.items()[5], V::Int(5), "{io_model}");
        assert_eq!(c.cmd(&["STRLEN", "large"]), V::Int(LARGE as i64));
    }
}

/// A client blocked in `BLPOP` across the buffers going back and a quiet
/// spell is answered when the list is pushed to, and goes on answering.
#[test]
fn a_blocked_client_survives_the_buffers_going_back() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        // The `UNUSED_TURNS`th small request is the `BLPOP`: the buffers go
        // back in its turn.
        c.grow_then_settle(UNUSED_TURNS - 1);
        c.send(&["BLPOP", "jobs", "0"]);
        std::thread::sleep(QUIET);
        assert_eq!(server.conn().cmd(&["RPUSH", "jobs", "j1"]), V::Int(1));
        let got = c.read();
        assert_eq!(
            got,
            V::Arr(vec![V::Bulk(b"jobs".to_vec()), V::Bulk(b"j1".to_vec())]),
            "{io_model}"
        );
        assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
    }
}

/// A subscriber whose buffers went back, and that has been quiet, is sent
/// what is published.
#[test]
fn a_subscriber_survives_the_buffers_going_back() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        c.grow_then_settle(1);
        let ack = c.cmd(&["SUBSCRIBE", "news"]);
        assert_eq!(ack.items().len(), 3, "{io_model}: {ack:?}");
        for _ in 0..5 {
            let pong = c.cmd(&["PING"]);
            assert_eq!(pong.items()[0], V::Bulk(b"pong".to_vec()), "{io_model}");
        }
        std::thread::sleep(QUIET);
        let big = "m".repeat(LARGE);
        for message in ["first", big.as_str()] {
            assert_eq!(
                server.conn().cmd(&["PUBLISH", "news", message]),
                V::Int(1),
                "{io_model}"
            );
            let got = c.read();
            assert_eq!(
                got.items()[2],
                V::Bulk(message.as_bytes().to_vec()),
                "{io_model}"
            );
        }
    }
}

/// A client tracking keys whose buffers went back, and that has been quiet,
/// is told when a key it read changes.
#[test]
fn a_tracking_client_survives_the_buffers_going_back() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "watched", "1"]).ok());
        let mut c = server.conn();
        let hello = c.cmd(&["HELLO", "3"]);
        assert!(matches!(hello, V::Arr(_)), "{io_model}: {hello:?}");
        assert!(c.cmd(&["CLIENT", "TRACKING", "ON"]).ok());
        c.grow_then_settle(0);
        assert_eq!(c.cmd(&["GET", "watched"]), V::Bulk(b"1".to_vec()));
        for _ in 0..5 {
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        }
        std::thread::sleep(QUIET);
        assert!(server.conn().cmd(&["SET", "watched", "2"]).ok());
        let push = c.read();
        assert_eq!(
            push.items()[0],
            V::Bulk(b"invalidate".to_vec()),
            "{io_model}"
        );
        assert_eq!(
            push.items()[1],
            V::Arr(vec![V::Bulk(b"watched".to_vec())]),
            "{io_model}"
        );
    }
}

/// A request sent in two parts, with a quiet spell between them, after the
/// buffers went back: nothing of the first part is let go of.
#[test]
fn a_request_sent_in_parts_survives_the_buffers_going_back() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        c.grow_then_settle(UNUSED_TURNS + 1);
        for size in [5, 3 * LARGE] {
            let value = "p".repeat(size);
            let request = encode(&["SET", "parts", &value]);
            let (first, rest) = request.split_at(request.len() / 2);
            c.writer.write_all(first).unwrap();
            std::thread::sleep(QUIET);
            c.writer.write_all(rest).unwrap();
            assert!(c.read().ok(), "{io_model}, {size}");
            assert_eq!(
                c.cmd(&["STRLEN", "parts"]),
                V::Int(size as i64),
                "{io_model}"
            );
        }
    }
}

/// A request whose first part arrives in the read that would make the
/// buffers go back, behind a whole one: the buffer keeps it, and the rest
/// completes it, after a quiet spell too. The turn of that read is the
/// `UNUSED_TURNS`th without a need for more than a small read's room
/// whatever reads the large request arrived in, as the turn that completed
/// it held all of it; and the part is held after it (`qbuf`).
#[test]
fn a_request_cut_at_the_read_that_would_shrink_survives() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        assert!(c.cmd(&["CLIENT", "SETNAME", "cut"]).ok());
        for size in [5, 3 * LARGE] {
            // `UNUSED_TURNS - 1` small reads after the large one: the
            // `UNUSED_TURNS`th is this.
            c.grow_then_settle(UNUSED_TURNS - 1);
            let value = "q".repeat(size);
            let request = encode(&["SET", "cut", &value]);
            let cut = 40.min(request.len() / 2);
            let mut first = encode(&["PING"]);
            first.extend_from_slice(&request[..cut]);
            c.writer.write_all(&first).unwrap();
            assert_eq!(c.read(), V::Str("PONG".into()), "{io_model}, {size}");
            assert_eq!(server.client_field("cut", "qbuf"), cut as u64, "{io_model}");
            std::thread::sleep(QUIET);
            c.writer.write_all(&request[cut..]).unwrap();
            assert!(c.read().ok(), "{io_model}, {size}");
            assert_eq!(c.cmd(&["STRLEN", "cut"]), V::Int(size as i64), "{io_model}");
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()), "{io_model}");
        }
    }
}
