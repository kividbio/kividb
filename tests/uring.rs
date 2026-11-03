// tests/uring.rs
//
// The io_uring connection loop (`src/server_uring.rs`), which is the default
// on Linux and which the tests that run a server inside the test process do
// not reach: they use the tokio loop. A real `kividb --io-model uring`, and
// the things the two loops each implement for themselves — above all
// Pub/Sub, whose messages are written by a task of the connection's own.

#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ── RESP client (RESP2 and RESP3) ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    /// An out-of-band RESP3 frame.
    Push(Vec<V>),
    Map(Vec<(V, V)>),
    Nil,
}

fn s(text: &str) -> V {
    V::Str(text.to_string())
}

impl V {
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn err(&self) -> &str {
        match self {
            V::Err(e) => e,
            other => panic!("not an error: {other:?}"),
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
                    .set_read_timeout(Some(Duration::from_secs(20)))
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

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        let many = |this: &mut Self, n: i64| -> Vec<V> { (0..n).map(|_| this.read()).collect() };
        match kind {
            "+" => V::Str(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "_" => V::Nil,
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Str(String::from_utf8_lossy(&data).into_owned())
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Arr(many(self, n))
            }
            ">" => V::Push(many(self, rest.parse().unwrap())),
            "%" => {
                let n: usize = rest.parse().unwrap();
                V::Map((0..n).map(|_| (self.read(), self.read())).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }
}

// ── The server: one for each test ─────────────────────────────────────────────

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
}

/// Whether nothing has `port`. It is asked with a socket that is bound and
/// never listens. One that listened, as this used to, could be given a
/// connection: a server another test is starting holds, between its fork and
/// its exec, a copy of every socket this process has open, and a client that
/// dialled the port in that moment was connected to the copy and then reset
/// ("Connection reset by peer", at the first command of a test).
fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    // As the server's own socket, so that a copy of this one, while it
    // lasts, does not keep the server from the port.
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

/// A port nothing is listening on — and not one the system hands out to
/// outgoing connections. A client that starts dialling before its server is
/// listening can otherwise be given, as its own end, the very port it is
/// dialling: it is then connected to itself and reads back what it writes.
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

impl Server {
    fn start() -> Self {
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
                "uring",
                "--threads",
                "4",
                "--query-buffer-idle-resize-after-ms",
                "150",
                "--configfile",
                "kividb.conf",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), s("PONG"));
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

fn message(channel: &str, payload: &str) -> V {
    V::Arr(vec![s("message"), s(channel), s(payload)])
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Every test here would pass on the other loop too, and prove nothing: the
/// server says which one it is running.
#[test]
fn the_server_is_running_on_io_uring() {
    let server = Server::start();
    let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
    assert!(log.contains("I/O model      : io_uring"), "{log}");
    assert!(!log.contains("io_uring unavailable"), "{log}");
}

fn client_list_line<'a>(list: &'a V, identity: &str) -> Option<&'a str> {
    let V::Str(list) = list else {
        panic!("CLIENT LIST did not return a string: {list:?}");
    };
    list.lines().find(|line| line.contains(identity))
}

fn client_metric<'a>(line: &'a str, metric: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix(metric))
}

/// The generic integration tests start the library's Tokio server. This test
/// starts the binary with `--io-model uring` and verifies the query-buffer
/// metrics in that separate connection loop.
#[test]
fn query_buffer_metrics_track_partial_and_completed_commands() {
    let server = Server::start();
    let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
    assert!(log.contains("I/O model      : io_uring"), "{log}");
    let mut partial = server.conn();
    let mut observer = server.conn();
    let partial_addr = format!("addr={}", partial.writer.local_addr().unwrap());

    let deadline = Instant::now() + Duration::from_secs(2);
    let idle_line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, &partial_addr).unwrap_or("");
        if client_metric(line, "qbuf-free=").is_some() || Instant::now() >= deadline {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(client_metric(&idle_line, "qbuf="), Some("0"), "{idle_line}");
    assert_eq!(
        client_metric(&idle_line, "qbuf-free="),
        Some("0"),
        "{idle_line}"
    );

    // CLIENT INFO reports the caller's own metrics. Its request has already
    // been parsed before the command formats the response.
    let info = partial.cmd(&["CLIENT", "INFO"]);
    let V::Str(info) = info else {
        panic!("CLIENT INFO did not return a string: {info:?}");
    };
    assert_eq!(client_metric(&info, "qbuf="), Some("0"), "{info}");
    assert_eq!(client_metric(&info, "qbuf-free="), Some("0"), "{info}");
    assert_eq!(partial.cmd(&["CLIENT", "SETNAME", "uring-qbuf"]), s("OK"));

    // The first 14 bytes are a complete PING. The next 10 bytes are an
    // incomplete PING, ending partway through its bulk string. Consuming the
    // first request leaves less than half the buffer, exercising compaction.
    partial
        .writer
        .write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI")
        .unwrap();
    assert_eq!(partial.read(), s("PONG"));

    let deadline = Instant::now() + Duration::from_secs(2);
    let first_partial_line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, "name=uring-qbuf").unwrap_or("");
        if client_metric(line, "qbuf=") == Some("10") || Instant::now() >= deadline {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        client_metric(&first_partial_line, "qbuf="),
        Some("10"),
        "{first_partial_line}"
    );
    // The read buffer starts at `READ_BUFFER` (32 KiB); a freshly opened
    // connection still holds that much spare capacity after a small batch.
    assert_eq!(
        client_metric(&first_partial_line, "qbuf-free="),
        Some("32758"),
        "{first_partial_line}"
    );

    // Complete the bulk string but leave its trailing CRLF outstanding.
    partial.writer.write_all(b"NG").unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let partial_line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, "name=uring-qbuf").unwrap_or("");
        if client_metric(line, "qbuf=") == Some("12") || Instant::now() >= deadline {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        client_metric(&partial_line, "qbuf="),
        Some("12"),
        "{partial_line}"
    );
    assert_eq!(
        client_metric(&partial_line, "qbuf-free="),
        Some("32756"),
        "{partial_line}"
    );

    partial.writer.write_all(b"\r\n").unwrap();
    assert_eq!(partial.read(), s("PONG"));

    let deadline = Instant::now() + Duration::from_secs(2);
    let completed_line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, "name=uring-qbuf").unwrap_or("");
        if client_metric(line, "qbuf=") == Some("0") || Instant::now() >= deadline {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        client_metric(&completed_line, "qbuf="),
        Some("0"),
        "{completed_line}"
    );
    assert_eq!(
        client_metric(&completed_line, "qbuf-free="),
        Some("32768"),
        "{completed_line}"
    );

    // io_uring reclaims an oversized private query buffer *between* reads, not
    // while a read is in flight; but this buffer only reached the 32 KiB floor,
    // which is never reclaimed. Wait past the idle threshold and confirm the
    // floor is held, then send a command and confirm it is still held.
    std::thread::sleep(Duration::from_secs(3));
    let idle_line = {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        client_list_line(&list, "name=uring-qbuf")
            .unwrap_or("")
            .to_string()
    };
    assert_eq!(
        client_metric(&idle_line, "qbuf-free="),
        Some("32768"),
        "an idle io_uring client must retain its private query buffer until the next read: {idle_line}"
    );

    // A private buffer that only ever reached the 32 KiB floor is never
    // reclaimed: like Redis, io_uring keeps buffers at or below
    // `PROTO_RESIZE_THRESHOLD` (32 KiB) untouched, so the floor survives an
    // idle period and the read that follows it. (Reclaiming an *oversized*
    // buffer is the next test's subject.)
    assert_eq!(partial.cmd(&["PING"]), s("PONG"));

    let deadline = Instant::now() + Duration::from_secs(2);
    let floor_line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, "name=uring-qbuf").unwrap_or("");
        if client_metric(line, "qbuf=") == Some("0") || Instant::now() >= deadline {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        client_metric(&floor_line, "qbuf="),
        Some("0"),
        "{floor_line}"
    );
    assert_eq!(
        client_metric(&floor_line, "qbuf-free="),
        Some("32768"),
        "the 32 KiB floor must be retained across reads: {floor_line}"
    );

    let info = partial.cmd(&["CLIENT", "INFO"]);
    let V::Str(info) = info else {
        panic!("CLIENT INFO did not return a string: {info:?}");
    };
    assert_eq!(client_metric(&info, "qbuf="), Some("0"), "{info}");
}

fn client_query_buffer_size(line: &str) -> Option<usize> {
    let qbuf = client_metric(line, "qbuf=")?.parse::<usize>().ok()?;
    let free = client_metric(line, "qbuf-free=")?.parse::<usize>().ok()?;
    Some(qbuf + free)
}

#[test]
fn large_bulk_header_reserves_a_private_query_buffer() {
    let server = Server::start();
    let mut client = server.conn();
    let mut observer = server.conn();
    assert_eq!(
        client.cmd(&["CLIENT", "SETNAME", "uring-fat-qbuf"]),
        s("OK")
    );

    client
        .writer
        .write_all(b"*3\r\n$3\r\nset\r\n$1\r\na\r\n$1000000\r\n")
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, "name=uring-fat-qbuf").unwrap_or("");
        if client_query_buffer_size(line).is_some_and(|size| size > 1_000_000)
            || Instant::now() >= deadline
        {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        client_query_buffer_size(&line).is_some_and(|size| size > 1_000_000),
        "declared fat argument did not reserve private query-buffer capacity: {line}"
    );

    client.writer.write_all(b"a").unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, "name=uring-fat-qbuf").unwrap_or("");
        if client_query_buffer_size(line).is_some_and(|size| size > 1_000_000)
            || Instant::now() >= deadline
        {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        client_query_buffer_size(&line).is_some_and(|size| size > 1_000_000),
        "partial fat argument lost its private query-buffer capacity: {line}"
    );

    // While the request is still incomplete, the buffer is retained: the read
    // that owns it cannot be cancelled, so nothing may rebuild it mid-flight.
    // Confirm it survives past the idle threshold.
    std::thread::sleep(Duration::from_secs(3));
    let line = {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        client_list_line(&list, "name=uring-fat-qbuf")
            .unwrap_or("")
            .to_string()
    };
    assert!(
        client_query_buffer_size(&line).is_some_and(|size| size > 1_000_000),
        "an in-progress client must retain its oversized buffer: {line}"
    );

    // Finish the fat argument so the SET completes and the client is no longer
    // mid-request.
    let mut rest = vec![b'a'; 999_999];
    rest.extend_from_slice(b"\r\n");
    client.writer.write_all(&rest).unwrap();
    assert_eq!(client.read(), s("OK"));

    // The completed SET left the buffer at its full ~1 MB size, but the batch
    // that finished it ends with the buffer empty — and io_uring gives an
    // oversized buffer back right there, at the batch boundary, without waiting
    // for another read. The client stays idle the whole time: no further
    // command is sent.
    let deadline = Instant::now() + Duration::from_secs(4);
    let line = loop {
        let list = observer.cmd(&["CLIENT", "LIST"]);
        let line = client_list_line(&list, "name=uring-fat-qbuf").unwrap_or("");
        if client_query_buffer_size(line).is_some_and(|size| size < 1_000_000)
            || Instant::now() >= deadline
        {
            break line.to_string();
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        client_query_buffer_size(&line).is_some_and(|size| size < 1_000_000),
        "an oversized buffer must be released at the end of the batch that emptied it, \
         even when the client then goes idle: {line}"
    );

    // And the client is still usable: the released buffer reads the next
    // request normally.
    assert_eq!(client.cmd(&["PING"]), s("PONG"));
}

/// The workers share their port with SO_REUSEPORT, so a second server on the
/// same port used to start, report itself ready, and take a share of the
/// first one's clients. It refuses to start, and says why.
#[test]
fn a_second_server_on_the_same_port_refuses_to_start() {
    let first = Server::start();
    let dir = tempfile::tempdir().unwrap();
    let second = Command::new(env!("CARGO_BIN_EXE_kividb"))
        .current_dir(dir.path())
        .args([
            "--port",
            &first.port.to_string(),
            "--bind",
            "127.0.0.1",
            "--io-model",
            "uring",
            "--threads",
            "2",
            "--configfile",
            "kividb.conf",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("could not start kividb");
    let output = second.wait_with_output().unwrap();
    let log = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{log}");
    assert!(log.contains("[FATAL] Can't listen on 127.0.0.1"), "{log}");
    assert!(!log.contains("Ready to accept connections"), "{log}");
    // The first is none the worse for it. And a server started the moment it
    // is killed gets the port: the dead one's sockets, which io_uring keeps
    // for an instant after the process, are not mistaken for a server.
    assert_eq!(first.conn().cmd(&["PING"]), s("PONG"));
    let port = first.port;
    drop(first);
    let log = std::fs::File::create(dir.path().join("server.log")).unwrap();
    let mut third = Command::new(env!("CARGO_BIN_EXE_kividb"))
        .current_dir(dir.path())
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--io-model",
            "uring",
            "--threads",
            "2",
            "--configfile",
            "kividb.conf",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb");
    // A first connection or two may still land on the dead server's sockets
    // and be reset; the new one answers once it is listening.
    let deadline = Instant::now() + Duration::from_secs(10);
    let answered = loop {
        use std::io::{Read, Write};
        let pong = std::net::TcpStream::connect(("127.0.0.1", port))
            .and_then(|mut conn| {
                conn.set_read_timeout(Some(Duration::from_secs(1)))?;
                conn.write_all(b"PING\r\n")?;
                let mut reply = [0u8; 7];
                conn.read_exact(&mut reply)?;
                Ok(reply == *b"+PONG\r\n")
            })
            .unwrap_or(false);
        if pong || Instant::now() > deadline {
            break pong;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let _ = third.kill();
    let _ = third.wait();
    assert!(answered, "the server started after the kill never answered");
}

/// A RESP2 subscriber: its messages, the few commands it may run, and an
/// ordinary connection again once it has unsubscribed.
#[test]
fn a_resp2_subscriber() {
    let server = Server::start();
    let mut sub = server.conn();
    let mut other = server.conn();
    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "u:a", "u:b"]),
        V::Arr(vec![s("subscribe"), s("u:a"), V::Int(1)])
    );
    assert_eq!(
        sub.read(),
        V::Arr(vec![s("subscribe"), s("u:b"), V::Int(2)])
    );
    assert_eq!(
        sub.cmd(&["PSUBSCRIBE", "u:*"]),
        V::Arr(vec![s("psubscribe"), s("u:*"), V::Int(3)])
    );

    // A message through the channel and through the pattern. The subscriber
    // has sent nothing: its messages are not waiting on its next command.
    assert_eq!(other.cmd(&["PUBLISH", "u:a", "one"]).i(), 2);
    assert_eq!(sub.read(), message("u:a", "one"));
    assert_eq!(
        sub.read(),
        V::Arr(vec![s("pmessage"), s("u:*"), s("u:a"), s("one")])
    );

    assert_eq!(sub.cmd(&["PING"]), V::Arr(vec![s("pong"), s("")]));
    assert!(
        sub.cmd(&["GET", "u:key"])
            .err()
            .starts_with("ERR Can't execute 'get': only (P|S)SUBSCRIBE")
    );
    assert_eq!(
        other.cmd(&["PUBSUB", "NUMSUB", "u:a", "u:b"]),
        V::Arr(vec![s("u:a"), V::Int(1), s("u:b"), V::Int(1)])
    );

    assert_eq!(
        sub.cmd(&["UNSUBSCRIBE"]),
        V::Arr(vec![s("unsubscribe"), s("u:a"), V::Int(2)])
    );
    assert_eq!(
        sub.read(),
        V::Arr(vec![s("unsubscribe"), s("u:b"), V::Int(1)])
    );
    assert_eq!(
        sub.cmd(&["PUNSUBSCRIBE"]),
        V::Arr(vec![s("punsubscribe"), s("u:*"), V::Int(0)])
    );
    // The same connection, and nothing is published to it any more.
    assert_eq!(other.cmd(&["PUBLISH", "u:a", "nobody"]).i(), 0);
    assert!(sub.cmd(&["SET", "u:key", "v"]).ok());
    assert_eq!(sub.cmd(&["GET", "u:key"]), s("v"));
    assert_eq!(sub.cmd(&["PING"]), s("PONG"));
}

/// Under RESP3 a subscriber keeps every command, and takes its messages —
/// and the replies to (un)subscribe — as push frames.
#[test]
fn a_resp3_subscriber_keeps_its_commands() {
    let server = Server::start();
    let mut sub = server.conn();
    let mut other = server.conn();
    assert!(matches!(sub.cmd(&["HELLO", "3"]), V::Map(_)));
    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "u3:a"]),
        V::Push(vec![s("subscribe"), s("u3:a"), V::Int(1)])
    );
    assert!(sub.cmd(&["SET", "u3:key", "v"]).ok());
    assert_eq!(sub.cmd(&["GET", "u3:key"]), s("v"));

    assert_eq!(other.cmd(&["PUBLISH", "u3:a", "hello"]).i(), 1);
    assert_eq!(
        sub.read(),
        V::Push(vec![s("message"), s("u3:a"), s("hello")])
    );
    assert_eq!(sub.cmd(&["PING"]), s("PONG"));
    assert_eq!(
        sub.cmd(&["UNSUBSCRIBE", "u3:a"]),
        V::Push(vec![s("unsubscribe"), s("u3:a"), V::Int(0)])
    );
    assert_eq!(other.cmd(&["PUBLISH", "u3:a", "nobody"]).i(), 0);
}

/// A subscriber that is behind gets everything, in order, across channels.
#[test]
fn a_subscriber_that_is_behind_misses_nothing() {
    let server = Server::start();
    let mut sub = server.conn();
    sub.cmd(&["SUBSCRIBE", "ub:a", "ub:b"]);
    sub.read();

    let total = 20_000;
    let mut publisher = server.conn();
    let mut pipeline = Vec::new();
    for i in 0..total {
        let (channel, payload) = (["ub:a", "ub:b"][i % 2], i.to_string());
        pipeline.extend_from_slice(
            format!(
                "*3\r\n$7\r\nPUBLISH\r\n${}\r\n{channel}\r\n${}\r\n{payload}\r\n",
                channel.len(),
                payload.len()
            )
            .as_bytes(),
        );
    }
    publisher.writer.write_all(&pipeline).unwrap();
    for _ in 0..total {
        assert_eq!(publisher.read(), V::Int(1));
    }
    for i in 0..total {
        assert_eq!(sub.read(), message(["ub:a", "ub:b"][i % 2], &i.to_string()));
    }
}

/// One that does not read at all is disconnected at the output buffer limit.
#[test]
fn a_subscriber_that_stops_reading_is_disconnected() {
    let server = Server::start();
    let mut sub = server.conn();
    sub.cmd(&["SUBSCRIBE", "uflood"]);

    let mut publisher = server.conn();
    let megabyte = "m".repeat(1 << 20);
    let mut reached = 0;
    for _ in 0..200 {
        reached = publisher.cmd(&["PUBLISH", "uflood", &megabyte]).i();
        if reached == 0 {
            break;
        }
    }
    assert_eq!(
        reached, 0,
        "200 MB were queued for a client that reads nothing"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let counts = publisher.cmd(&["PUBSUB", "NUMSUB", "uflood"]);
        if counts == V::Arr(vec![s("uflood"), V::Int(0)]) {
            break;
        }
        assert!(Instant::now() < deadline, "still subscribed: {counts:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A subscription made in the same packet as a blocking command: the
/// messages are not held back until the blocking command returns.
#[test]
fn a_blocked_resp3_subscriber_still_gets_its_messages() {
    let server = Server::start();
    let mut sub = server.conn();
    let mut other = server.conn();
    assert!(matches!(sub.cmd(&["HELLO", "3"]), V::Map(_)));
    // Both commands in one write.
    let packet =
        b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nuq:a\r\n*3\r\n$5\r\nBLPOP\r\n$7\r\nuq:list\r\n$1\r\n5\r\n";
    sub.writer.write_all(packet).unwrap();
    assert_eq!(
        sub.read(),
        V::Push(vec![s("subscribe"), s("uq:a"), V::Int(1)])
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while other.cmd(&["PUBLISH", "uq:a", "while blocked"]).i() == 0 {
        assert!(Instant::now() < deadline);
    }
    assert_eq!(
        sub.read(),
        V::Push(vec![s("message"), s("uq:a"), s("while blocked")])
    );
    assert_eq!(other.cmd(&["RPUSH", "uq:list", "x"]).i(), 1);
    // Whatever else was published while it was blocked, then the pop.
    loop {
        match sub.read() {
            V::Push(_) => continue,
            reply => {
                assert_eq!(reply, V::Arr(vec![s("uq:list"), s("x")]));
                break;
            }
        }
    }
}

/// Eviction, transactions and expiry events are the dispatcher's, shared by
/// both loops; one pass through each, here, to see that this loop reaches
/// them.
#[test]
fn the_shared_machinery_is_reached() {
    let server = Server::start();
    let mut c = server.conn();
    // A transaction.
    assert!(c.cmd(&["MULTI"]).ok());
    assert_eq!(c.cmd(&["INCR", "us:n"]), s("QUEUED"));
    assert_eq!(c.cmd(&["INCR", "us:n"]), s("QUEUED"));
    assert_eq!(c.cmd(&["EXEC"]), V::Arr(vec![V::Int(1), V::Int(2)]));

    // An expiry event from the background task, to a subscriber.
    assert!(
        c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "Ex"])
            .ok()
    );
    let mut sub = server.conn();
    sub.cmd(&["SUBSCRIBE", "__keyevent@0__:expired"]);
    assert!(c.cmd(&["SET", "us:gone", "v", "PX", "100"]).ok());
    assert_eq!(sub.read(), message("__keyevent@0__:expired", "us:gone"));

    // Command statistics.
    assert_eq!(c.cmd(&["APPEND", "us:s", "a"]).i(), 1);
    assert_eq!(c.cmd(&["APPEND", "us:s", "b"]).i(), 2);
    let V::Str(stats) = c.cmd(&["INFO", "commandstats"]) else {
        panic!("INFO commandstats");
    };
    assert!(stats.contains("cmdstat_append:calls=2,"), "{stats}");
}

// ── What this loop got wrong, or made easy to get wrong ───────────────────────
//
// On this loop a connection stays on the thread that accepted it, so two
// clients woken by one write really do run at the same moment. On the other
// loop they mostly do not, and the tests below mostly pass there whatever
// the code does.

fn info_field(c: &mut Conn, section: &str, field: &str) -> i64 {
    let V::Str(info) = c.cmd(&["INFO", section]) else {
        panic!("INFO {section}");
    };
    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
        .trim()
        .parse()
        .unwrap()
}

fn wait_for_blocked(c: &mut Conn, count: i64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while info_field(c, "clients", "blocked_clients") != count {
        assert!(
            Instant::now() < deadline,
            "{} clients are blocked, not {count}",
            info_field(c, "clients", "blocked_clients")
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// A client blocked on two keys, woken for the second, does not take what
/// was pushed to the first for a client that has been waiting there longer.
///
/// It did: woken because `l3` had an element, it looked at `l2` first, found
/// the element pushed for the client ahead of it, and took that — leaving
/// that client blocked on an empty list and the element in `l3` with nobody
/// to take it. About one round in eight on this loop.
#[test]
fn clients_blocked_on_several_keys_keep_their_place() {
    let server = Server::start();
    let mut c = server.conn();
    for round in 0..150 {
        let (l1, l2, l3) = (
            format!("l1:{round}"),
            format!("l2:{round}"),
            format!("l3:{round}"),
        );
        let (mut mover, mut first, mut second) = (server.conn(), server.conn(), server.conn());
        mover.send(&["BRPOPLPUSH", &l1, &l3, "0"]);
        wait_for_blocked(&mut c, 1);
        first.send(&["BLPOP", &l2, "0"]);
        wait_for_blocked(&mut c, 2);
        second.send(&["BLPOP", &l2, &l3, "0"]);
        wait_for_blocked(&mut c, 3);

        // One element to the mover, which moves it to where `second` is
        // waiting; one to the list `first` is ahead of `second` on.
        assert!(c.cmd(&["MULTI"]).ok());
        c.cmd(&["LPUSH", &l1, "moved"]);
        c.cmd(&["LPUSH", &l2, "pushed"]);
        assert_eq!(c.cmd(&["EXEC"]), V::Arr(vec![V::Int(1), V::Int(1)]));

        assert_eq!(mover.read(), s("moved"), "round {round}");
        assert_eq!(
            first.read(),
            V::Arr(vec![s(&l2), s("pushed")]),
            "round {round}"
        );
        assert_eq!(
            second.read(),
            V::Arr(vec![s(&l3), s("moved")]),
            "round {round}"
        );
        wait_for_blocked(&mut c, 0);
    }
}

/// `CLIENT KILL … SKIPME no` kills the caller too — after it has been told
/// how many were killed. The connection used to be shut before the reply
/// was written.
#[test]
fn a_client_that_kills_itself_is_told_so() {
    let server = Server::start();
    let mut c = server.conn();
    let mut other = server.conn();
    assert_eq!(other.cmd(&["PING"]), s("PONG"));
    let id = c.cmd(&["CLIENT", "ID"]).i().to_string();

    assert_eq!(c.cmd(&["CLIENT", "KILL", "ID", &id]).i(), 0);
    assert_eq!(c.cmd(&["PING"]), s("PONG"));
    assert_eq!(c.cmd(&["CLIENT", "KILL", "ID", &id, "SKIPME", "no"]).i(), 1);
    // And then it is closed.
    let mut rest = Vec::new();
    assert_eq!(c.reader.read_to_end(&mut rest).unwrap_or(0), 0);
    // The others are not.
    assert_eq!(other.cmd(&["PING"]), s("PONG"));
}

/// A connection that ends while it has tracking on is a tracking client
/// fewer, however it ends; and every change is counted, whichever thread
/// made it. (Both were kept per thread, or at the end of a path that a
/// killed connection did not take.)
#[test]
fn what_is_counted_across_threads_is_counted() {
    let server = Server::start();
    let mut c = server.conn();
    assert_eq!(info_field(&mut c, "clients", "tracking_clients"), 0);

    let mut tracking: Vec<Conn> = (0..6).map(|_| server.conn()).collect();
    for t in &mut tracking {
        t.cmd(&["HELLO", "3"]);
        assert!(t.cmd(&["CLIENT", "TRACKING", "ON"]).ok());
    }
    assert_eq!(info_field(&mut c, "clients", "tracking_clients"), 6);
    // Two are killed, two leave, two turn it off.
    for t in tracking.iter_mut().take(2) {
        let id = t.cmd(&["CLIENT", "ID"]).i().to_string();
        assert_eq!(c.cmd(&["CLIENT", "KILL", "ID", &id]).i(), 1);
    }
    for t in tracking.iter_mut().skip(4) {
        assert!(t.cmd(&["CLIENT", "TRACKING", "OFF"]).ok());
    }
    tracking.truncate(4);
    tracking.drain(2..);
    let deadline = Instant::now() + Duration::from_secs(5);
    while info_field(&mut c, "clients", "tracking_clients") != 0 {
        assert!(
            Instant::now() < deadline,
            "tracking clients are still counted"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // One write from each of several connections — which this loop puts on
    // different threads — is that many changes.
    let before = info_field(&mut c, "persistence", "rdb_changes_since_last_save");
    let mut writers: Vec<Conn> = (0..8).map(|_| server.conn()).collect();
    for (i, w) in writers.iter_mut().enumerate() {
        assert!(w.cmd(&["SET", &format!("counted:{i}"), "v"]).ok());
    }
    assert_eq!(
        info_field(&mut c, "persistence", "rdb_changes_since_last_save"),
        before + 8
    );
}
