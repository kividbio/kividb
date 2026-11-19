#![allow(clippy::disallowed_methods)]
// tests/output_buffer_limit.rs
//
// `client-output-buffer-limit` (#774): `CONFIG SET` and `CONFIG GET` of it,
// the configuration file and `CONFIG REWRITE`, and each class's limits
// enforced as Redis 8.10 enforces them: a subscriber dropped at the pub/sub
// hard limit and kept with `pubsub 0 0 0`, the soft limit and its seconds,
// and a normal client's replies held to the `normal` class; and a pipeline
// sent before any reply is read, which the server reads while its replies
// wait (#865); and the limits looked at after each reply, so that a pipeline
// is cut at the reply over the hard limit (#893). Each test runs a real
// `kividb`, on both I/O models where the
// connection loops differ.
//
// The messages here are large (up to 64 MB), so the tests take turns: one
// runs at a time, and each lets its payloads go before the next.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

mod common;
use common::start_verified;

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
        self.writer.write_all(&encode(args)).unwrap();
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
            // RESP3's maps (a key and a value an item), sets and pushes are
            // read as arrays.
            "*" | "%" | "~" | ">" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Some(V::Nil);
                }
                let n = if kind == "%" { n * 2 } else { n };
                let mut items = Vec::new();
                for _ in 0..n {
                    items.push(self.read()?);
                }
                V::Arr(items)
            }
            "_" => V::Nil,
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        })
    }

    /// Whether the server closed the connection, read to the end.
    fn closed_within(&mut self, time: Duration) -> bool {
        // macOS refuses to set a timeout on a socket the server has reset.
        if self.writer.set_read_timeout(Some(time)).is_err() {
            return true;
        }
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

/// A request, as a client sends it.
fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

// ── The server ────────────────────────────────────────────────────────────────

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
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
        Self::start_in(dir, io_model, config.is_some(), &[], &[])
    }

    /// A replica of `master`.
    #[cfg(target_os = "linux")]
    fn start_replica(master: &Server) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = master.port.to_string();
        Self::start_in(
            dir,
            "tokio",
            false,
            &["--replicaof", "127.0.0.1", &port],
            &[],
        )
    }

    fn start_in(
        dir: std::path::PathBuf,
        io_model: &str,
        with_config: bool,
        extra: &[&str],
        envs: &[(&str, &str)],
    ) -> Self {
        // Verified to be the process started, also on a restart from the same
        // directory (a port is free between a server's exit and the next
        // start, and another test's server can take it): see tests/common/mod.rs.
        let (child, port) = start_verified(|port| {
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
                "--enable-debug-command",
                "yes",
            ]);
            if with_config {
                command.args(["--configfile", "kividb.conf"]);
            }
            command.args(extra);
            command.envs(envs.iter().copied());
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(log)
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
        Self::start_in(dir, io_model, true, &[], &[])
    }

    /// `CONFIG GET` of one name: its value.
    fn get(&self, name: &str) -> String {
        match self.conn().cmd(&["CONFIG", "GET", name]) {
            V::Arr(items) if items.len() == 2 => items[1].text(),
            other => panic!("CONFIG GET {name}: {other:?}"),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Redis's defaults, but for a replica's soft limit, which is off (Redis:
/// `slave 268435456 67108864 60`).
const DEFAULT: &str = "normal 0 0 0 slave 268435456 0 0 pubsub 33554432 8388608 60";

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
    assert_eq!(server.config_get(), DEFAULT);
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
        "normal 0 0 0 slave 268435456 0 0 pubsub 0 0 0"
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

/// Every value `CONFIG GET` gives, of either name of a replica's hard
/// limit, `CONFIG SET` takes back: 0 (no limit) too, which
/// `client-output-buffer-limit replica 0 0 0` sets. So a `CONFIG SET` of
/// several settings that fails part-way puts the limit back as it was; it
/// used to fail to, and leave the new value.
#[test]
fn every_value_reads_back_and_a_failed_config_set_puts_it_back() {
    let _turn = serial();
    let server = Server::start("tokio");
    let mut c = server.conn();
    for value in [
        "replica 0 0 0",
        "replica 18446744073709551615 0 0",
        "replica 1mb 2mb 3",
    ] {
        server.config_set(value);
        let all = server.config_get();
        assert!(
            c.cmd(&["CONFIG", "SET", "client-output-buffer-limit", &all])
                .ok(),
            "{all}"
        );
        assert_eq!(server.config_get(), all);
        for name in [
            "repl-replica-max-backlog-bytes",
            "repl-replica-sync-max-backlog-bytes",
        ] {
            let current = server.get(name);
            assert!(
                c.cmd(&["CONFIG", "SET", name, &current]).ok(),
                "{name} {current}"
            );
            assert_eq!(server.get(name), current);
        }
    }

    server.config_set("replica 0 0 0");
    for (name, value) in [
        ("repl-replica-max-backlog-bytes", "1mb"),
        ("client-output-buffer-limit", "replica 1mb 0 0"),
    ] {
        let reply = c.cmd(&["CONFIG", "SET", name, value, "maxclients", "-5"]);
        assert!(matches!(reply, V::Err(_)), "{reply:?}");
        assert_eq!(server.get("repl-replica-max-backlog-bytes"), "0", "{name}");
        assert_eq!(
            server.config_get(),
            "normal 0 0 0 slave 0 0 0 pubsub 33554432 8388608 60",
            "{name}"
        );
    }
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
        "normal 0 0 0 slave 268435456 0 0 pubsub 67108864 16777216 30"
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

// ── Pipelines sent before reading (#865) ──────────────────────────────────────

/// `GET big` `gets` times, then `sets` `SET`s of small values: sent whole
/// before any reply is read, as redis-py sends a pipeline.
fn gets_then_sets(gets: usize, sets: usize) -> Vec<u8> {
    let mut pipeline = b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n".repeat(gets);
    for i in 0..sets {
        let key = format!("key:{i:08}");
        pipeline.extend_from_slice(
            format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len()).as_bytes(),
        );
    }
    pipeline
}

/// A client that sends its whole pipeline before it reads a reply is
/// answered in full: while the replies to the first commands wait to be
/// written, the server reads the rest. It stopped reading until they were
/// written, and the client, still sending, never read them.
#[test]
fn a_pipeline_sent_before_reading_is_answered() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let value = "v".repeat(MB);
        assert!(server.conn().cmd(&["SET", "big", &value]).ok());
        let mut pipeline = b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n".repeat(10);
        let set = format!("*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${MB}\r\n{value}\r\n");
        pipeline.extend_from_slice(set.repeat(16).as_bytes());

        let mut c = server.conn();
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        c.writer
            .write_all(&pipeline)
            .unwrap_or_else(|e| panic!("{io_model}: the server stopped reading: {e}"));
        for _ in 0..10 {
            assert_eq!(c.read(), Some(V::Bulk(value.clone().into_bytes())));
        }
        for _ in 0..16 {
            assert!(c.read().unwrap().ok());
        }
    }
}

/// Every way the io_uring loop's first send of a batch of replies can end,
/// in turn: whole, short, nothing (`WouldBlock`), interrupted. Debug builds
/// take them from `KIVIDB_TEST_SEND_NOW`, so each comes up whatever the
/// kernel's socket buffers do. After any but the first, the rest is written
/// while the client is read: every reply arrives whole and in order, one
/// command at a time and in #865's pipeline sent before reading. Fails if a
/// send that took nothing or was interrupted closed the connection, or if a
/// short send's rest were not written.
#[cfg(all(target_os = "linux", debug_assertions))]
#[test]
fn every_way_a_send_without_waiting_ends_writes_the_rest() {
    let _turn = serial();
    let dir = tempfile::tempdir().unwrap().keep();
    let server = Server::start_in(
        dir,
        "uring",
        false,
        &[],
        &[("KIVIDB_TEST_SEND_NOW", "short,none,interrupted,as")],
    );
    let value = "v".repeat(MB);
    assert!(server.conn().cmd(&["SET", "big", &value]).ok());
    let mut c = server.conn();
    c.writer
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    for i in 0..40 {
        assert_eq!(
            c.cmd(&["GET", "big"]),
            V::Bulk(value.clone().into_bytes()),
            "GET {i}"
        );
        assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()), "PING {i}");
    }
    let mut pipeline = b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n".repeat(10);
    let set = format!("*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${MB}\r\n{value}\r\n");
    pipeline.extend_from_slice(set.repeat(16).as_bytes());
    c.writer
        .write_all(&pipeline)
        .unwrap_or_else(|e| panic!("the server stopped reading: {e}"));
    for _ in 0..10 {
        assert_eq!(c.read(), Some(V::Bulk(value.clone().into_bytes())));
    }
    for _ in 0..16 {
        assert!(c.read().unwrap().ok());
    }
    let short: u64 = server.info_field("debug_sends_short").parse().unwrap();
    let none: u64 = server.info_field("debug_sends_none").parse().unwrap();
    assert!(short > 0, "no short send went on to write its rest");
    assert!(none > 0, "no send that took nothing went on to write");
}

/// A subscriber that does not read its messages while it sends a pipeline
/// is answered in full. With its socket full of messages, the task that
/// writes them waits for room while it holds the connection's write lock;
/// the connection, with replies to write, waited for that lock without
/// reading, and the client, still sending, never read: each waited for the
/// other for good, as in #865. The subscriber speaks RESP2, so each PING
/// is answered with a `pong` message (`*2 $4 pong $0`).
#[test]
fn a_pipeline_sent_while_messages_wait_is_answered() {
    let _turn = serial();
    const MESSAGES: usize = 6000;
    const PINGS: usize = 1_000_000;
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut sub = subscriber(&server, "ch");
        // Small buffers both ways, whatever the host's TCP tuning: with a
        // large send buffer the whole pipeline fits in the client's socket
        // and the write returns, so the test would pass without the fix.
        {
            let sock = socket2::SockRef::from(&sub.writer);
            sock.set_recv_buffer_size(64 * 1024).unwrap();
            sock.set_send_buffer_size(64 * 1024).unwrap();
        }
        // 6 MB of messages, under the pubsub class's soft limit: more than
        // the two sockets' buffers hold, so the rest waits in the server.
        let mut publisher = server.conn();
        let message = "m".repeat(1000);
        let publish = format!("*3\r\n$7\r\nPUBLISH\r\n$2\r\nch\r\n$1000\r\n{message}\r\n");
        publisher
            .writer
            .write_all(publish.repeat(MESSAGES).as_bytes())
            .unwrap();
        for _ in 0..MESSAGES {
            assert!(matches!(publisher.read(), Some(V::Int(_))), "{io_model}");
        }
        sub.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        sub.writer
            .write_all(b"*1\r\n$4\r\nPING\r\n".repeat(PINGS).as_slice())
            .unwrap_or_else(|e| panic!("{io_model}: the server stopped reading: {e}"));
        // Every message, then every PING's reply.
        let pong = b"$4\r\npong\r\n";
        let (mut seen, mut pongs, mut tail) = (0usize, 0usize, Vec::new());
        let mut buf = vec![0u8; 1 << 20];
        while pongs < PINGS {
            let n = sub
                .reader
                .read(&mut buf)
                .unwrap_or_else(|e| panic!("{io_model}: {e} after {pongs} replies"));
            assert!(n > 0, "{io_model}: closed after {pongs} replies");
            tail.extend_from_slice(&buf[..n]);
            seen += n;
            let mut at = 0;
            while let Some(i) = tail[at..].windows(pong.len()).position(|w| w == pong) {
                pongs += 1;
                at += i + pong.len();
            }
            tail.drain(..at.max(tail.len().saturating_sub(pong.len() - 1)));
        }
        assert!(seen >= MESSAGES * 1000, "{io_model}: {seen} bytes");
    }
}

/// What the server reads ahead counts against `client-query-buffer-limit`:
/// past it, the client is closed, as Redis closes one whose query buffer
/// reaches the limit.
#[test]
fn reading_ahead_past_the_query_buffer_limit_closes_the_client() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "big", &"v".repeat(MB)]).ok());
        let limit = server
            .conn()
            .cmd(&["CONFIG", "SET", "client-query-buffer-limit", "1mb"]);
        assert!(limit.ok(), "{limit:?}");

        // About 10 MB of replies the client does not read, while it sends about
        // 6 MB of commands.
        let mut c = server.conn();
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let _ = c.writer.write_all(&gets_then_sets(10, 200_000));
        assert!(
            c.closed_within(Duration::from_secs(10)),
            "{io_model}: the client was not closed"
        );
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
    }
}

/// The replies still count against `client-output-buffer-limit` while the
/// server reads ahead: a client over the soft limit for longer than its
/// seconds is closed, whatever it goes on sending.
#[test]
fn reading_ahead_a_client_over_the_soft_limit_too_long_is_disconnected() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let big = "b".repeat(24 * MB);
        assert!(server.conn().cmd(&["SET", "big", &big]).ok());
        drop(big);
        server.config_set("normal 0 1mb 1");

        let mut c = server.conn();
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let _ = c.writer.write_all(&gets_then_sets(1, 200_000));
        // Not read: closed once over the soft limit for longer than its second.
        let deadline = Instant::now() + Duration::from_secs(10);
        while server.info_field("client_output_buffer_limit_disconnections") != "1" {
            assert!(
                Instant::now() < deadline,
                "{io_model}: the client was not closed"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(c.closed_within(Duration::from_secs(10)), "{io_model}");
        server.config_set("normal 0 0 0");
    }
}

/// A client that sends and never reads is closed at its class's hard
/// output limit, by what it has sent while its replies wait: the server
/// stops reading it there, as Redis, running what it reads, has replies
/// over the limit by then. Not at `client-query-buffer-limit`'s gigabyte.
#[test]
fn reading_ahead_a_client_that_never_reads_is_closed_at_the_hard_limit() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        server.config_set("normal 1mb 0 0");

        // About 70 MB of `SET`s, and not a reply read.
        let pipeline = gets_then_sets(0, 2_000_000);
        let mut c = server.conn();
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut sent = 0;
        while sent < pipeline.len() {
            match c.writer.write(&pipeline[sent..]) {
                Ok(n) => sent += n,
                Err(_) => break,
            }
        }
        assert!(
            sent < pipeline.len(),
            "{io_model}: the server took {sent} of {} bytes",
            pipeline.len()
        );
        assert_eq!(
            server.info_field("client_output_buffer_limit_disconnections"),
            "1",
            "{io_model}"
        );
        server.config_set("normal 0 0 0");
    }
}

/// What was read ahead runs a read's worth a turn, as it would have been
/// read, so the replies the output limit looks at after a turn are no more
/// than a read's commands make. A client that sends a long pipeline of
/// reads and then reads every reply is answered in full, however much the
/// replies come to in all.
#[test]
fn what_was_read_ahead_runs_a_read_at_a_time() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let big = "b".repeat(MB / 10);
        let small = "s".repeat(100);
        assert!(server.conn().cmd(&["SET", "b", &big]).ok());
        assert!(server.conn().cmd(&["SET", "s", &small]).ok());
        server.config_set("normal 32mb 0 0");

        // The replies to the first read, about 19 MB to 186 `GET b`, are more
        // than the sockets hold, so the server reads the rest while they wait:
        // 400,000 `GET s`, 9 MB, whose replies come to 44 MB in all and to 20 KB
        // a read.
        let mut pipeline = b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n".repeat(186);
        pipeline.extend_from_slice(&b"*2\r\n$3\r\nGET\r\n$1\r\ns\r\n".repeat(400_000));
        let mut c = server.conn();
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        c.writer
            .write_all(&pipeline)
            .unwrap_or_else(|e| panic!("{io_model}: the server stopped reading: {e}"));
        for i in 0..186 {
            assert_eq!(
                c.read(),
                Some(V::Bulk(big.clone().into_bytes())),
                "{io_model} {i}"
            );
        }
        for i in 0..400_000 {
            assert_eq!(
                c.read(),
                Some(V::Bulk(small.clone().into_bytes())),
                "{io_model} {i}"
            );
        }
        assert_eq!(
            server.info_field("client_output_buffer_limit_disconnections"),
            "0",
            "{io_model}"
        );
        server.config_set("normal 0 0 0");
    }
}

/// Requests of many arguments, each larger than a read, pipelined: each
/// arrives in many reads into a query buffer of its own, which is trimmed
/// between them, and every one runs. A read of 32 KiB into a larger buffer
/// left room the trim then took, which a check of the read did not allow
/// for: a debug build panicked and dropped the client (#893).
#[test]
fn pipelined_requests_larger_than_a_read_all_run() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        // As tests/memory_usage_zset.rs loads sorted sets of 512 members,
        // every score changed: two ZADDs of about 15 and 20 KB a key, 100
        // keys in one write.
        let scattered = |i: u64| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as f64;
        let zadd = |key: &str, scores: &dyn Fn(u64) -> f64| {
            let mut args: Vec<Vec<u8>> = vec![b"ZADD".to_vec(), key.as_bytes().to_vec()];
            for i in 0..512u64 {
                args.push(scores(i).to_string().into_bytes());
                args.push(format!("member-{i:09}").into_bytes());
            }
            let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
            encode(&refs)
        };
        let mut c = server.conn();
        assert!(c.cmd(&["SELECT", "1"]).ok());
        let mut pipeline = Vec::new();
        for k in 0..100 {
            let key = format!("warm:{k}");
            pipeline.extend_from_slice(&zadd(&key, &|i| i as f64));
            pipeline.extend_from_slice(&zadd(&key, &scattered));
        }
        c.writer
            .set_write_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        c.writer
            .write_all(&pipeline)
            .unwrap_or_else(|e| panic!("{io_model}: {e}"));
        for k in 0..100 {
            assert_eq!(c.read(), Some(V::Int(512)), "{io_model}: key {k}");
            assert_eq!(c.read(), Some(V::Int(0)), "{io_model}: key {k} again");
        }
    }
}

/// A blocking command writes the replies to what came before it as it
/// parks, and the server reads the client while that write waits too.
#[test]
fn a_blocking_command_reads_ahead_while_it_writes_what_came_before() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let value = "v".repeat(MB);
        assert!(server.conn().cmd(&["SET", "big", &value]).ok());
        let mut pipeline = b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n".repeat(10);
        pipeline.extend_from_slice(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nnolist\r\n$1\r\n1\r\n");
        let set = format!("*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${MB}\r\n{value}\r\n");
        pipeline.extend_from_slice(set.repeat(16).as_bytes());

        let mut c = server.conn();
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        c.writer
            .write_all(&pipeline)
            .unwrap_or_else(|e| panic!("{io_model}: the server stopped reading: {e}"));
        for _ in 0..10 {
            assert_eq!(c.read(), Some(V::Bulk(value.clone().into_bytes())));
        }
        assert_eq!(c.read(), Some(V::Nil), "{io_model}: BLPOP");
        for _ in 0..16 {
            assert!(c.read().unwrap().ok());
        }
    }
}

/// What is read ahead runs in the order it was sent: 186 `GET`s of a
/// 100 KB value, whose replies wait, then 400,000 `INCR`s of one counter,
/// all sent before a reply is read, answer 1 to 400,000 in turn.
#[test]
fn what_was_read_ahead_runs_in_the_order_it_was_sent() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "b", &"b".repeat(MB / 10)]).ok());
        let mut pipeline = b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n".repeat(186);
        pipeline.extend_from_slice(&b"*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n".repeat(400_000));

        let mut c = server.conn();
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        c.writer
            .write_all(&pipeline)
            .unwrap_or_else(|e| panic!("{io_model}: the server stopped reading: {e}"));
        for i in 0..186 {
            match c.read() {
                Some(V::Bulk(b)) => assert_eq!(b.len(), MB / 10, "{io_model} {i}"),
                other => panic!("{io_model} {i}: {other:?}"),
            }
        }
        for n in 1..=400_000 {
            assert_eq!(c.read(), Some(V::Int(n)), "{io_model}");
        }
    }
}

/// `CLIENT KILL` ends a client whose replies wait while it is read: the
/// write waiting and the reading both end, and so does the connection,
/// with nothing said to it.
#[test]
fn a_client_killed_while_its_replies_wait_is_closed() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "big", &"v".repeat(MB)]).ok());
        let mut c = server.conn();
        let id = c.cmd(&["CLIENT", "ID"]).i().to_string();
        // About 10 MB of replies the client does not read, while it sends
        // about 6 MB of commands.
        c.writer
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        c.writer
            .write_all(&gets_then_sets(10, 200_000))
            .unwrap_or_else(|e| panic!("{io_model}: the server stopped reading: {e}"));
        assert_eq!(
            server.conn().cmd(&["CLIENT", "KILL", "ID", &id]),
            V::Int(1),
            "{io_model}"
        );
        assert!(
            c.closed_within(Duration::from_secs(10)),
            "{io_model}: the client was not closed"
        );
        let list = server.conn().cmd(&["CLIENT", "LIST"]).text();
        assert!(!list.contains(&format!("id={id} ")), "{io_model}: {list}");
    }
}

// ── After each reply (#893) ───────────────────────────────────────────────────

/// A 100 KB value: its reply is 102,411 bytes, so ten of them are under a
/// hard limit of 1 MB and eleven over it.
const BIG: usize = 100 * 1024;

/// `GET big` and `INCR n`, `pairs` times: 43 bytes a pair, so that 80 pairs
/// arrive in one read on either loop.
fn gets_and_incrs(pairs: usize) -> Vec<u8> {
    b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n".repeat(pairs)
}

/// The server's `n` once `c` has been closed, or `None` if it was not.
fn n_once_closed(server: &Server, c: &mut Conn) -> Option<i64> {
    if !c.closed_within(Duration::from_secs(10)) {
        return None;
    }
    match server.conn().cmd(&["GET", "n"]) {
        V::Bulk(b) => Some(String::from_utf8(b).unwrap().parse().unwrap()),
        V::Nil => Some(0),
        other => panic!("GET n: {other:?}"),
    }
}

/// The hard limit is looked at after each reply, not once for all the
/// replies to what one read brought: a pipeline of 80 `GET big` and `INCR`
/// pairs, read at once, is cut after the 11th `GET`, whose reply takes the
/// replies past the limit, and the `INCR`s after it are not run. It ran all
/// 80 and only then closed the client. With the limit off it is answered in
/// full.
#[test]
fn the_hard_limit_cuts_a_pipeline_after_the_reply_over_it() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "big", &"b".repeat(BIG)]).ok());
        server.config_set("normal 1mb 0 0");
        let mut c = server.conn();
        c.writer.write_all(&gets_and_incrs(80)).unwrap();
        assert_eq!(n_once_closed(&server, &mut c), Some(10), "{io_model}");
        assert_eq!(
            server.info_field("client_output_buffer_limit_disconnections"),
            "1",
            "{io_model}"
        );

        // 0 0 0: no limit, and every reply comes.
        server.config_set("normal 0 0 0");
        assert_eq!(server.conn().cmd(&["DEL", "n"]).i(), 1);
        let mut c = server.conn();
        c.writer.write_all(&gets_and_incrs(80)).unwrap();
        for i in 1..=80 {
            match c.read() {
                Some(V::Bulk(b)) => assert_eq!(b.len(), BIG, "{io_model} {i}"),
                other => panic!("{io_model} {i}: {other:?}"),
            }
            assert_eq!(c.read(), Some(V::Int(i)), "{io_model}");
        }
    }
}

/// A `CONFIG SET` of the limit holds the connections already open, the one
/// that set it included, from their next command; and setting it back to
/// 0 0 0 lets them be.
#[test]
fn a_limit_set_while_connected_holds_from_the_next_command() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "big", &"b".repeat(BIG)]).ok());
        let mut before = server.conn();
        let mut setter = server.conn();
        let mut unlimited = server.conn();
        assert_eq!(before.cmd(&["GET", "big"]), V::Bulk(vec![b'b'; BIG]));
        assert!(
            setter
                .cmd(&[
                    "CONFIG",
                    "SET",
                    "client-output-buffer-limit",
                    "normal 1mb 0 0"
                ])
                .ok()
        );
        before.writer.write_all(&gets_and_incrs(80)).unwrap();
        assert_eq!(n_once_closed(&server, &mut before), Some(10), "{io_model}");

        // The one that set it too.
        assert_eq!(server.conn().cmd(&["DEL", "n"]).i(), 1);
        setter.writer.write_all(&gets_and_incrs(80)).unwrap();
        assert_eq!(n_once_closed(&server, &mut setter), Some(10), "{io_model}");

        // Back to none: a connection that was open all along runs all 80.
        server.config_set("normal 0 0 0");
        assert_eq!(server.conn().cmd(&["DEL", "n"]).i(), 1);
        unlimited.writer.write_all(&gets_and_incrs(80)).unwrap();
        for i in 1..=80 {
            assert!(matches!(unlimited.read(), Some(V::Bulk(_))), "{io_model}");
            assert_eq!(unlimited.read(), Some(V::Int(i)), "{io_model}");
        }
    }
}

/// The soft limit does not cut a pipeline short: a client over it is closed
/// only once it has been over it for longer than its seconds, as in Redis.
/// All 80 pairs run, and then, not read, the client is closed.
#[test]
fn the_soft_limit_lets_a_pipeline_run_and_closes_after_its_seconds() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "big", &"b".repeat(BIG)]).ok());
        server.config_set("normal 0 1mb 1");
        let mut c = server.conn();
        c.writer.write_all(&gets_and_incrs(80)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while server.info_field("client_output_buffer_limit_disconnections") != "1" {
            assert!(Instant::now() < deadline, "{io_model}: not closed");
            std::thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(n_once_closed(&server, &mut c), Some(80), "{io_model}");
        server.config_set("normal 0 0 0");
    }
}

/// A RESP3 subscriber's replies are held to the `pubsub` class after each
/// reply too, and not to `normal`'s.
#[test]
fn a_resp3_subscriber_is_held_to_the_pubsub_limit_after_each_reply() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "big", &"b".repeat(BIG)]).ok());
        let resp3_subscriber = || {
            let mut c = server.conn();
            assert!(matches!(c.cmd(&["HELLO", "3"]), V::Arr(_)));
            match c.cmd(&["SUBSCRIBE", "ch"]) {
                V::Arr(items) => assert_eq!(items[0], V::Bulk(b"subscribe".to_vec())),
                other => panic!("{io_model}: {other:?}"),
            }
            c
        };

        server.config_set("normal 1mb 0 0 pubsub 0 0 0");
        let mut c = resp3_subscriber();
        c.writer.write_all(&gets_and_incrs(80)).unwrap();
        for i in 1..=80 {
            assert!(matches!(c.read(), Some(V::Bulk(_))), "{io_model}");
            assert_eq!(c.read(), Some(V::Int(i)), "{io_model}");
        }

        server.config_set("normal 0 0 0 pubsub 1mb 0 0");
        assert_eq!(server.conn().cmd(&["DEL", "n"]).i(), 1);
        let mut c = resp3_subscriber();
        c.writer.write_all(&gets_and_incrs(80)).unwrap();
        assert_eq!(n_once_closed(&server, &mut c), Some(10), "{io_model}");
        server.config_set("pubsub 32mb 8mb 60");
    }
}

/// Closed between two commands, a client leaves nothing half done: a
/// transaction runs whole or not at all, and what ran before the close is
/// applied, in the AOF and on a replica.
#[test]
fn a_client_closed_mid_pipeline_leaves_nothing_half_done() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start_with_config(io_model, Some("appendonly yes\n"));
        assert!(server.conn().cmd(&["SET", "big", &"b".repeat(BIG)]).ok());
        server.config_set("normal 1mb 0 0");

        // MULTI, 20 `GET big` and two INCRs, EXEC; then an INCR after it.
        // EXEC's reply is over the limit: the transaction is applied whole,
        // and what follows it is not run.
        let mut pipeline = b"*1\r\n$5\r\nMULTI\r\n".to_vec();
        pipeline.extend_from_slice(&b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n".repeat(20));
        pipeline.extend_from_slice(b"*2\r\n$4\r\nINCR\r\n$1\r\na\r\n");
        pipeline.extend_from_slice(b"*2\r\n$4\r\nINCR\r\n$1\r\nb\r\n");
        pipeline.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
        pipeline.extend_from_slice(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n");
        let mut c = server.conn();
        c.writer.write_all(&pipeline).unwrap();
        assert!(c.closed_within(Duration::from_secs(10)), "{io_model}");
        let mut check = server.conn();
        assert_eq!(
            check.cmd(&["MGET", "a", "b", "c"]),
            V::Arr(vec![V::Bulk(b"1".to_vec()), V::Bulk(b"1".to_vec()), V::Nil]),
            "{io_model}"
        );

        // Cut in a pipeline of writes: the ten INCRs that ran are in the AOF,
        // and are there after a restart.
        let mut c = server.conn();
        c.writer.write_all(&gets_and_incrs(80)).unwrap();
        assert_eq!(n_once_closed(&server, &mut c), Some(10), "{io_model}");
        let server = server.restart(io_model);
        assert_eq!(
            server.conn().cmd(&["MGET", "a", "b", "c", "n"]),
            V::Arr(vec![
                V::Bulk(b"1".to_vec()),
                V::Bulk(b"1".to_vec()),
                V::Nil,
                V::Bulk(b"10".to_vec())
            ]),
            "{io_model}: after a restart from the AOF"
        );
    }
}

/// What ran before the close reaches a replica.
#[cfg(target_os = "linux")]
#[test]
fn what_ran_before_the_close_reaches_a_replica() {
    let _turn = serial();
    for &io_model in io_models() {
        let master = Server::start(io_model);
        assert!(master.conn().cmd(&["SET", "big", &"b".repeat(BIG)]).ok());
        let replica = Server::start_replica(&master);
        master.config_set("normal 1mb 0 0");
        let mut c = master.conn();
        c.writer.write_all(&gets_and_incrs(80)).unwrap();
        assert_eq!(n_once_closed(&master, &mut c), Some(10), "{io_model}");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if replica.conn().cmd(&["GET", "n"]) == V::Bulk(b"10".to_vec()) {
                break;
            }
            assert!(Instant::now() < deadline, "{io_model}: not on the replica");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// A script is one command: one whose reply is over the limit runs whole,
/// and the client is closed after it.
#[cfg(feature = "lua")]
#[test]
fn a_script_over_the_limit_runs_whole() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        assert!(server.conn().cmd(&["SET", "big", &"b".repeat(BIG)]).ok());
        server.config_set("normal 1mb 0 0");
        let script = "local out = {} for i = 1, 20 do out[i] = redis.call('GET', 'big') \
                      redis.call('INCR', 'n') end return out";
        // Both in one write, so that both arrive in one read: on separate
        // reads the second would run after the first's reply was written,
        // with or without the check after each reply.
        let mut c = server.conn();
        c.writer
            .write_all(&encode(&[b"EVAL", script.as_bytes(), b"0"]).repeat(2))
            .unwrap();
        // The first ran whole; the second, after it, not at all.
        assert_eq!(n_once_closed(&server, &mut c), Some(20), "{io_model}");
        // Nor is the server held up by it.
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
    }
}

/// A reply `CLIENT REPLY OFF` or `SKIP` drops does not count against the
/// limit, hard or soft, as in Redis: `LRANGE` of 1.1 MB, its reply dropped,
/// then `INCR n`, in one read, keeps the client, which runs the `INCR`.
#[test]
fn a_reply_the_client_turned_off_does_not_count() {
    let _turn = serial();
    for &io_model in io_models() {
        let server = Server::start(io_model);
        let mut load = server.conn();
        for chunk in 0..10 {
            let mut args: Vec<Vec<u8>> = vec![b"RPUSH".to_vec(), b"l".to_vec()];
            for i in 0..1000 {
                args.push(format!("{:0>100}", chunk * 1000 + i).into_bytes());
            }
            let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
            load.send(&refs);
            assert!(matches!(load.read(), Some(V::Int(_))));
        }
        let lrange: &[&[u8]] = &[b"LRANGE", b"l", b"0", b"-1"];
        let incr: &[&[u8]] = &[b"INCR", b"n"];
        for limit in ["normal 1mb 0 0", "normal 0 1mb 1"] {
            server.config_set(limit);
            for mode in ["OFF", "SKIP"] {
                let mut c = server.conn();
                let mut pipeline = encode(&[b"CLIENT", b"REPLY", mode.as_bytes()]);
                pipeline.extend_from_slice(&encode(lrange));
                pipeline.extend_from_slice(&encode(incr));
                if mode == "OFF" {
                    pipeline.extend_from_slice(&encode(&[b"CLIENT", b"REPLY", b"ON"]));
                }
                pipeline.extend_from_slice(&encode(&[b"PING"]));
                c.writer.write_all(&pipeline).unwrap();
                let what = format!("{io_model}, {limit}, {mode}");
                if mode == "SKIP" {
                    // SKIP drops the LRANGE's reply only.
                    assert_eq!(c.read(), Some(V::Int(1)), "{what}");
                } else {
                    assert!(c.read().unwrap().ok(), "{what}: CLIENT REPLY ON");
                }
                assert_eq!(c.read(), Some(V::Str("PONG".into())), "{what}");
                assert_eq!(server.conn().cmd(&["DEL", "n"]).i(), 1, "{what}");
            }
        }
        assert_eq!(
            server.info_field("client_output_buffer_limit_disconnections"),
            "0",
            "{io_model}"
        );
        server.config_set("normal 0 0 0");
    }
}

// ── Replicas ──────────────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn info_number(server: &Server, name: &str) -> u64 {
    server.info_field(name).parse().unwrap()
}

/// Writes 4 KB values to `port` until `stop`, about 20 MB a second.
#[cfg(target_os = "linux")]
fn writer(
    port: u16,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut c = Conn::connect(port);
        let value = "w".repeat(4096);
        let mut i = 0u64;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            let mut batch = Vec::new();
            for _ in 0..100 {
                let key = format!("w:{}", i % 20_000);
                i += 1;
                batch.extend_from_slice(
                    format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n{value}\r\n",
                        key.len(),
                        value.len()
                    )
                    .as_bytes(),
                );
            }
            c.writer.write_all(&batch).unwrap();
            for _ in 0..100 {
                c.read().unwrap();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    })
}

/// A replica's soft limit, with its hard limit raised (and with none): a
/// replica that comes out of a full sync with a stream written meanwhile to
/// drain is not dropped for it (that would start the sync again, and
/// again); one that then stays over the soft limit, stopped here, is.
#[cfg(target_os = "linux")]
#[test]
fn a_replica_drains_after_its_sync_and_is_dropped_only_when_it_stays_over() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let _turn = serial();
    for hard in ["2gb", "0"] {
        let master = Server::start("tokio");
        master.config_set(&format!("replica {hard} 1mb 1"));
        assert!(
            master
                .conn()
                .cmd(&["DEBUG", "POPULATE", "50000", "key", "1000"])
                .ok()
        );
        let stop = Arc::new(AtomicBool::new(false));
        let writing = writer(master.port, Arc::clone(&stop));
        let replica = Server::start_replica(&master);
        let deadline = Instant::now() + Duration::from_secs(60);
        while !replica
            .conn()
            .cmd(&["INFO", "replication"])
            .text()
            .contains("master_link_status:up")
        {
            assert!(
                Instant::now() < deadline,
                "{hard}: the replica never synced"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        // Streaming, as the writes go on.
        std::thread::sleep(Duration::from_secs(4));
        assert_eq!(info_number(&master, "connected_slaves"), 1, "{hard}");
        assert_eq!(info_number(&master, "sync_full"), 1, "{hard}: synced again");
        assert_eq!(
            info_number(&master, "client_output_buffer_limit_disconnections"),
            0,
            "{hard}"
        );

        // Stopped, it falls behind and stays over the soft limit.
        let pid = replica.child.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-STOP", &pid])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while info_number(&master, "client_output_buffer_limit_disconnections") == 0 {
            assert!(
                Instant::now() < deadline,
                "{hard}: a stopped replica was kept"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = Command::new("kill").args(["-CONT", &pid]).status();
        stop.store(true, Ordering::Relaxed);
        writing.join().unwrap();
    }
}
