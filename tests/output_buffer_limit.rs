// tests/output_buffer_limit.rs
//
// `client-output-buffer-limit` (#774): `CONFIG SET` and `CONFIG GET` of it,
// the configuration file and `CONFIG REWRITE`, and each class's limits
// enforced as Redis 8.10 enforces them: a subscriber dropped at the pub/sub
// hard limit and kept with `pubsub 0 0 0`, the soft limit and its seconds,
// and a normal client's replies held to the `normal` class. Each test runs a
// real `kividb`, on both I/O models where the connection loops differ.
//
// The messages here are large (up to 64 MB), so the tests take turns: one
// runs at a time, and each lets its payloads go before the next.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const MB: usize = 1 << 20;

/// One test at a time: see the top of the file.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
}

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
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn text(&self) -> String {
        match self {
            V::Str(s) | V::Err(s) => s.clone(),
            V::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("not a string: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
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

    fn send(&mut self, args: &[&[u8]]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.send(&args);
        self.read().expect("a reply")
    }

    /// The next value; `None` if the server closed the connection.
    fn read(&mut self) -> Option<V> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => return None,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return None,
            Err(e) => panic!("read: {e}"),
        }
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        Some(match kind {
            "+" => V::Str(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return Some(V::Nil);
                }
                let mut data = vec![0u8; len as usize + 2];
                match self.reader.read_exact(&mut data) {
                    Ok(()) => {}
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                        ) =>
                    {
                        return None;
                    }
                    Err(e) => panic!("read: {e}"),
                }
                data.truncate(len as usize);
                V::Bulk(data)
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Some(V::Nil);
                }
                let mut items = Vec::new();
                for _ in 0..n {
                    items.push(self.read()?);
                }
                V::Arr(items)
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        })
    }

    /// Whether the server closed the connection, read to the end.
    fn closed_within(&mut self, time: Duration) -> bool {
        self.writer.set_read_timeout(Some(time)).unwrap();
        let mut sink = vec![0u8; 1 << 20];
        loop {
            match self.reader.read(&mut sink) {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return true,
                Err(_) => return false,
            }
        }
    }
}

// ── The server ────────────────────────────────────────────────────────────────

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
}

fn port_is_free(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// A port nothing listens on, outside the range handed out to outgoing
/// connections (see `tests/uring.rs`).
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(37);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

/// The I/O models to run a test on: the tokio loop, and on Linux the
/// io_uring one too.
fn io_models() -> &'static [&'static str] {
    if cfg!(target_os = "linux") {
        &["tokio", "uring"]
    } else {
        &["tokio"]
    }
}

impl Server {
    fn start(io_model: &str) -> Self {
        Self::start_with_config(io_model, None)
    }

    /// With `config` as its configuration file, if any.
    fn start_with_config(io_model: &str, config: Option<&str>) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        if let Some(text) = config {
            std::fs::write(dir.join("kividb.conf"), text).unwrap();
        }
        Self::start_in(dir, io_model, config.is_some())
    }

    fn start_in(dir: std::path::PathBuf, io_model: &str, with_config: bool) -> Self {
        let port = free_port();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("server.log"))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
        command.current_dir(&dir).args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--io-model",
            io_model,
            "--threads",
            "2",
        ]);
        if with_config {
            command.args(["--configfile", "kividb.conf"]);
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn config_get(&self) -> String {
        match self
            .conn()
            .cmd(&["CONFIG", "GET", "client-output-buffer-limit"])
        {
            V::Arr(items) if items.len() == 2 => items[1].text(),
            other => panic!("CONFIG GET client-output-buffer-limit: {other:?}"),
        }
    }

    fn config_set(&self, value: &str) {
        let reply = self
            .conn()
            .cmd(&["CONFIG", "SET", "client-output-buffer-limit", value]);
        assert!(reply.ok(), "CONFIG SET {value}: {reply:?}");
    }

    fn info_field(&self, name: &str) -> String {
        let info = self.conn().cmd(&["INFO", "everything"]).text();
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("no {name} in INFO"))
            .trim()
            .to_string()
    }

    /// Stop it, and start another from the same directory.
    fn restart(mut self, io_model: &str) -> Self {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let dir = std::mem::take(&mut self.dir);
        std::mem::forget(self);
        Self::start_in(dir, io_model, true)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

const REDIS_DEFAULT: &str = "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60";

/// A subscriber on `channel`, with the reply to its SUBSCRIBE read.
fn subscriber(server: &Server, channel: &str) -> Conn {
    let mut sub = server.conn();
    let ack = sub.cmd(&["SUBSCRIBE", channel]);
    assert_eq!(
        ack,
        V::Arr(vec![
            V::Bulk(b"subscribe".to_vec()),
            V::Bulk(channel.as_bytes().to_vec()),
            V::Int(1)
        ])
    );
    sub
}

// ── CONFIG ────────────────────────────────────────────────────────────────────

/// `CONFIG GET` gives all three classes, at Redis's defaults, before anything
/// has set them; it gave nothing.
#[test]
fn config_get_gives_redis_defaults() {
    let _turn = serial();
    let server = Server::start("tokio");
    assert_eq!(server.config_get(), REDIS_DEFAULT);
    assert_eq!(
        server.info_field("client_output_buffer_limit_disconnections"),
        "0"
    );
}

/// What `CONFIG SET` takes, and refuses, word for word as Redis 8.10.2.
#[test]
fn config_set_takes_what_redis_takes() {
    let _turn = serial();
    let server = Server::start("tokio");
    let mut c = server.conn();
    let set =
        |c: &mut Conn, value: &str| c.cmd(&["CONFIG", "SET", "client-output-buffer-limit", value]);

    // One class: the others are left as they are.
    assert!(set(&mut c, "pubsub 0 0 0").ok());
    assert_eq!(
        server.config_get(),
        "normal 0 0 0 slave 268435456 67108864 60 pubsub 0 0 0"
    );
    // Several, with units, any case, and `replica` for `slave`.
    assert!(set(&mut c, "NORMAL 1mb 2gb 5 replica 1g 64m 10").ok());
    assert_eq!(
        server.config_get(),
        "normal 1048576 2147483648 5 slave 1000000000 64000000 10 pubsub 0 0 0"
    );
    // A replica's hard limit is `repl-replica-max-backlog-bytes`.
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "repl-replica-max-backlog-bytes"]),
        V::Arr(vec![
            V::Bulk(b"repl-replica-max-backlog-bytes".to_vec()),
            V::Bulk(b"1000000000".to_vec())
        ])
    );
    // Nothing: no change.
    assert!(set(&mut c, "").ok());

    let refused = |why: &str| {
        format!(
            "ERR CONFIG SET failed (possibly related to argument 'client-output-buffer-limit') \
             - {why}"
        )
    };
    for (value, why) in [
        (
            "normal 1 2",
            "Wrong number of arguments in buffer limit configuration.",
        ),
        (
            "normal 1 2 3 pubsub 1 2",
            "Wrong number of arguments in buffer limit configuration.",
        ),
        (
            "master 1 2 3",
            "Invalid client class specified in buffer limit configuration.",
        ),
        (
            "normal x 2 3",
            "Error in hard, soft or soft_seconds setting in buffer limit configuration.",
        ),
        (
            "normal 1 2 -3",
            "Error in hard, soft or soft_seconds setting in buffer limit configuration.",
        ),
        (
            "normal 1.5mb 2 3",
            "Error in hard, soft or soft_seconds setting in buffer limit configuration.",
        ),
    ] {
        assert_eq!(set(&mut c, value), V::Err(refused(why)), "{value}");
    }
    // A refused value changes nothing, not even the classes it got right.
    assert!(matches!(
        set(&mut c, "pubsub 5 6 7 normal x 0 0"),
        V::Err(_)
    ));
    assert_eq!(
        server.config_get(),
        "normal 1048576 2147483648 5 slave 1000000000 64000000 10 pubsub 0 0 0"
    );
}

/// From the configuration file, back out through `CONFIG REWRITE`, and in
/// again after a restart: one line for each class not at its default, as
/// Redis writes them.
#[test]
fn the_config_file_and_rewrite_round_trip() {
    let _turn = serial();
    let server = Server::start_with_config(
        "tokio",
        Some("client-output-buffer-limit pubsub 64mb 16mb 30\n"),
    );
    assert_eq!(
        server.config_get(),
        "normal 0 0 0 slave 268435456 67108864 60 pubsub 67108864 16777216 30"
    );
    server.config_set("normal 100mb 0 0 replica 0 0 0 pubsub 1gb 1000 0");
    assert!(server.conn().cmd(&["CONFIG", "REWRITE"]).ok());
    let written = std::fs::read_to_string(server.dir.join("kividb.conf")).unwrap();
    let lines: Vec<&str> = written
        .lines()
        .filter(|l| l.starts_with("client-output-buffer-limit"))
        .collect();
    assert_eq!(
        lines,
        [
            "client-output-buffer-limit normal 100mb 0 0",
            "client-output-buffer-limit replica 0 0 0",
            "client-output-buffer-limit pubsub 1gb 1000 0",
        ],
        "{written}"
    );
    let server = server.restart("tokio");
    assert_eq!(
        server.config_get(),
        "normal 104857600 0 0 slave 0 0 0 pubsub 1073741824 1000 0"
    );
}

// ── Pub/Sub ───────────────────────────────────────────────────────────────────

/// The issue's table: one subscriber, one message of each size, under the
/// default limit and under `pubsub 0 0 0`. `PUBLISH` counts the subscriber
/// either way, as Redis does; at the default, a message of the hard limit
/// or more disconnects it, and with no limit every one is delivered.
#[test]
fn large_messages_under_the_default_and_with_no_limit() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        for (limit, cut_from_mb) in [(None, 32), (Some("pubsub 0 0 0"), usize::MAX)] {
            if let Some(limit) = limit {
                server.config_set(limit);
            }
            let mut publisher = server.conn();
            for mb in [1, 24, 31, 33, 40, 64] {
                let mut sub = subscriber(&server, "big");
                let payload = vec![b'x'; mb * MB];
                publisher.send(&[b"PUBLISH", b"big", &payload]);
                drop(payload);
                let what = format!("{io_model}, {limit:?}, {mb} MB");
                assert_eq!(publisher.read(), Some(V::Int(1)), "{what}: PUBLISH");
                match sub.read() {
                    Some(V::Arr(frame)) => {
                        assert!(mb < cut_from_mb, "{what}: delivered over the limit");
                        assert_eq!(frame.len(), 3, "{what}");
                        assert!(
                            matches!(&frame[2], V::Bulk(b) if b.len() == mb * MB),
                            "{what}"
                        );
                    }
                    None => assert!(mb >= cut_from_mb, "{what}: disconnected"),
                    other => panic!("{what}: {other:?}"),
                }
            }
        }
        // Three subscribers dropped at the default limit, counted as Redis
        // counts them.
        assert_eq!(
            server.info_field("client_output_buffer_limit_disconnections"),
            "3",
            "{io_model}"
        );
    }
}

/// A subscriber that reads nothing, with more than the soft limit queued
/// for longer than its seconds, is disconnected though nothing more is
/// published to it; with the limit off it stays. `CLIENT LIST` shows what
/// is queued for it as `omem`.
#[test]
fn the_pubsub_soft_limit_and_omem() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        for (limit, cut) in [("pubsub 0 1mb 1", true), ("pubsub 0 0 0", false)] {
            server.config_set(limit);
            let mut sub = subscriber(&server, "slow");
            let mut publisher = server.conn();
            // More than the socket's buffers hold: the rest waits in the
            // server, over the soft limit.
            let payload = "s".repeat(4 * MB);
            for _ in 0..6 {
                assert_eq!(publisher.cmd(&["PUBLISH", "slow", &payload]), V::Int(1));
            }
            let list = publisher.cmd(&["CLIENT", "LIST"]).text();
            let line = list
                .lines()
                .find(|l| l.contains(" sub=1 "))
                .unwrap_or_else(|| panic!("no subscriber in {list}"));
            let field = |name: &str| -> u64 {
                line.split(' ')
                    .find_map(|f| f.strip_prefix(&format!("{name}=")))
                    .unwrap()
                    .parse()
                    .unwrap()
            };
            assert!(field("omem") >= MB as u64, "{io_model}: {line}");
            assert!(field("tot-mem") >= field("omem"), "{io_model}: {line}");
            // More than one second over the soft limit (at most three, as
            // seconds are counted, and the timer checks every second), with
            // room for a slow machine.
            std::thread::sleep(Duration::from_secs(5));
            let numsub = publisher.cmd(&["PUBSUB", "NUMSUB", "slow"]);
            let still = match numsub {
                V::Arr(items) => items[1].i(),
                other => panic!("{other:?}"),
            };
            assert_eq!(still, if cut { 0 } else { 1 }, "{io_model}, {limit}");
            assert_eq!(
                sub.closed_within(Duration::from_secs(5)),
                cut,
                "{io_model}, {limit}"
            );
        }
    }
}

// ── Normal clients ────────────────────────────────────────────────────────────

/// `normal` limits hold a client's replies: one over the hard limit closes
/// the connection unanswered; 0 0 0, the default, lets any reply through.
#[test]
fn a_normal_client_over_the_hard_limit_is_disconnected() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut c = server.conn();
        let mut pipeline = Vec::new();
        for i in 0..20_000 {
            let key = format!("key:{i:08}");
            pipeline.extend_from_slice(
                format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len()).as_bytes(),
            );
        }
        c.writer.write_all(&pipeline).unwrap();
        for _ in 0..20_000 {
            assert!(c.read().unwrap().ok());
        }
        // About 340 KB of keys.
        server.config_set("normal 100kb 0 0");
        let mut keys = server.conn();
        keys.send(&[b"KEYS", b"*"]);
        assert_eq!(keys.read(), None, "{io_model}: a reply over the hard limit");
        assert_eq!(
            server.info_field("client_output_buffer_limit_disconnections"),
            "1",
            "{io_model}"
        );
        // Small replies are not held up.
        let mut small = server.conn();
        assert_eq!(small.cmd(&["GET", "key:00000001"]), V::Bulk(b"v".to_vec()));

        server.config_set("normal 0 0 0");
        let mut keys = server.conn();
        match keys.cmd(&["KEYS", "*"]) {
            V::Arr(all) => assert_eq!(all.len(), 20_000, "{io_model}"),
            other => panic!("{io_model}: {other:?}"),
        }
    }
}

/// The soft limit: a reply over it that the client reads goes through; one
/// it does not read for longer than the seconds closes the connection.
#[test]
fn a_normal_client_over_the_soft_limit_too_long_is_disconnected() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let big = "b".repeat(24 * MB);
        assert!(server.conn().cmd(&["SET", "big", &big]).ok());
        drop(big);
        server.config_set("normal 0 1mb 1");

        // Read at once: delivered.
        let mut reader = server.conn();
        match reader.cmd(&["GET", "big"]) {
            V::Bulk(b) => assert_eq!(b.len(), 24 * MB, "{io_model}"),
            other => panic!("{io_model}: {other:?}"),
        }

        // Not read: more than the socket's buffers hold waits in the server,
        // over the soft limit, until the connection is closed.
        let mut stalled = server.conn();
        stalled.send(&[b"GET", b"big"]);
        std::thread::sleep(Duration::from_secs(4));
        let mut got = 0usize;
        let mut sink = vec![0u8; MB];
        stalled
            .writer
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        loop {
            match stalled.reader.read(&mut sink) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(e) => panic!("{io_model}: {e} after {got} bytes"),
            }
        }
        assert!(
            got < 24 * MB,
            "{io_model}: the whole reply came, {got} bytes"
        );
        server.config_set("normal 0 0 0");
    }
}
