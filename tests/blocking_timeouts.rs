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
