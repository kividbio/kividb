// tests/multi_process.rs
//
// What a transaction and the ACL log do where the server's process-wide
// state is involved, so each test runs a real `kividb` of its own: the
// append-only file (`appendonly` is one setting for the whole process) and
// Lua (one script engine). Each test runs on each I/O model there is: tokio
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
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
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
        let from = (std::process::id() as u16).wrapping_mul(53);
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
    /// A server on `io`, with `config` as its configuration file.
    fn start(io: &str, config: &str) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(dir.join("kividb.conf"), config).unwrap();
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
        assert_eq!(Conn::connect(server.port).cmd(&["PING"]), "+PONG\r\n");
        if io == "uring" {
            let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
            assert!(log.contains("I/O model      : io_uring"), "{log}");
        }
        server
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

/// BGREWRITEAOF is not `no-multi` in Redis: queued, and run by EXEC with the
/// commands after it, while EXEC holds the write gate. The rewrite runs on
/// a thread of its own, after the transaction.
#[test]
fn bgrewriteaof_inside_exec_with_appendonly() {
    for io in io_models() {
        let server = Server::start(io, "appendonly yes\n");
        let mut c = server.conn();
        assert_eq!(c.cmd(&["MULTI"]), "+OK\r\n");
        assert_eq!(c.cmd(&["BGREWRITEAOF"]), "+QUEUED\r\n");
        assert_eq!(c.cmd(&["SET", "aof:k", "v"]), "+QUEUED\r\n");
        assert_eq!(
            c.cmd(&["EXEC"]),
            "*2\r\n+Background append only file rewriting started\r\n+OK\r\n",
            "{io}"
        );
        assert_eq!(c.cmd(&["GET", "aof:k"]), "$1\r\nv\r\n");
    }
}

/// What a script is refused is in the ACL log in context `lua`, as Redis
/// logs it.
#[cfg(feature = "lua")]
#[test]
fn a_scripts_refusal_is_logged_in_context_lua() {
    for io in io_models() {
        let server = Server::start(io, "");
        let mut admin = server.conn();
        assert_eq!(
            admin.cmd(&["ACL", "SETUSER", "lu", "on", ">pw", "~*", "+@all", "-get"]),
            "+OK\r\n"
        );
        let mut c = server.conn();
        assert_eq!(c.cmd(&["AUTH", "lu", "pw"]), "+OK\r\n");
        let reply = c.cmd(&["EVAL", "return redis.pcall('GET', 'k')", "0"]);
        assert!(reply.starts_with("-NOPERM"), "{io}: {reply:?}");
        let log = admin.cmd(&["ACL", "LOG"]);
        assert!(
            log.contains("$7\r\ncontext\r\n$3\r\nlua\r\n")
                && log.contains("$6\r\nobject\r\n$3\r\nget\r\n"),
            "{io}: {log}"
        );
    }
}
