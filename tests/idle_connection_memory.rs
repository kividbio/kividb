#![allow(clippy::disallowed_methods)]
// tests/idle_connection_memory.rs
//
// What an idle connection holds: little for one that has sent nothing or
// one command, and a read buffer that goes back to its small size once a
// connection that sent a large request sends small ones again (and, on the
// Tokio loop, once it has been quiet). And that a connection whose buffers
// went back keeps working in each state that outlives a read: in MULTI,
// blocked in BLPOP, subscribed, tracking keys, and with a request only
// partly sent. Each test runs a real `kividb` on both I/O models.

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
    /// `small` small requests: the read buffer goes back at the fourth.
    fn grow_then_settle(&mut self, small: usize) {
        assert!(self.cmd(&["SET", "large", &"v".repeat(LARGE)]).ok());
        for _ in 0..small {
            assert_eq!(self.cmd(&["PING"]), V::Str("PONG".into()));
        }
    }
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
/// once the connection's requests are small again: `qbuf-free`, which shows
/// a buffer the connection keeps for a large request, is back to a read's
/// room.
#[test]
fn a_buffer_grown_for_a_large_request_goes_back() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        assert!(c.cmd(&["CLIENT", "SETNAME", "grown"]).ok());
        assert!(c.cmd(&["SET", "large", &"v".repeat(LARGE)]).ok());
        for _ in 0..5 {
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
        }
        let settled = server.client_field("grown", "qbuf-free");
        assert!(settled <= 8 * 1024, "{io_model}: qbuf-free {settled}");
        assert_eq!(c.cmd(&["STRLEN", "large"]), V::Int(LARGE as i64));
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
        // The fourth small request is the `BLPOP`: the buffers go back in
        // its turn.
        c.grow_then_settle(3);
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
        assert_eq!(push.items()[0], V::Bulk(b"invalidate".to_vec()), "{io_model}");
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
        c.grow_then_settle(5);
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
/// completes it, after a quiet spell too.
#[test]
fn a_request_cut_at_the_read_that_would_shrink_survives() {
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        for size in [5, 3 * LARGE] {
            // Three small reads after the large one: the fourth is this.
            c.grow_then_settle(3);
            let value = "q".repeat(size);
            let request = encode(&["SET", "cut", &value]);
            let cut = 40.min(request.len() / 2);
            let mut first = encode(&["PING"]);
            first.extend_from_slice(&request[..cut]);
            c.writer.write_all(&first).unwrap();
            assert_eq!(c.read(), V::Str("PONG".into()), "{io_model}, {size}");
            std::thread::sleep(QUIET);
            c.writer.write_all(&request[cut..]).unwrap();
            assert!(c.read().ok(), "{io_model}, {size}");
            assert_eq!(c.cmd(&["STRLEN", "cut"]), V::Int(size as i64), "{io_model}");
            assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()), "{io_model}");
        }
    }
}
