// tests/blocked_commandstats_order.rs
//
// A client that `INFO clients` shows as no longer blocked has had its
// blocking command counted in `INFO commandstats` (#896).
//
// A blocking command that is woken is run again, and counted when that run
// is over. The blocked-client count used to go down when the wait ended, so
// a reader in between saw the client unblocked and the call not yet counted:
// Redis's "Blocking command accounted only once in commandstats" failed on
// io_uring under load. Here a client blocks, another wakes it, and a reader
// asks `INFO clients` and `INFO commandstats` back to back until the client
// is no longer blocked; the call must be there, and be there once.
//
// Every test runs a real `kividb` on each I/O model there is: tokio
// everywhere, and io_uring as well on Linux. `KIVIDB_TEST_ROUNDS` sets how
// many blocks each command is put through (default 300).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
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
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream.set_nodelay(true).unwrap();
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
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn send(&mut self, args: &[&str]) {
        self.writer.write_all(&request(args)).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        assert!(!line.is_empty(), "the connection was closed");
        let (kind, rest) = line.split_at(1);
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
                V::Arr((0..n).map(|_| self.read()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }
}

fn request(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

// ── The server: one for each test and I/O model ───────────────────────────────

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
        let from = (std::process::id() as u16).wrapping_mul(41);
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
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
        // A test of the io_uring loop that ran on the other one would prove
        // nothing: the server says which it is running.
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

// ── What the reader looks at ──────────────────────────────────────────────────

/// `field` of an `INFO` reply, as a number.
fn field_of(info: &str, field: &str) -> Option<i64> {
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|line| line.trim().strip_prefix(&prefix)?.parse().ok())
}

/// The number of calls `INFO commandstats` has for `name`: 0 when it has no
/// line for it yet.
fn calls_of(commandstats: &str, name: &str) -> i64 {
    let prefix = format!("cmdstat_{name}:calls=");
    commandstats
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix(&prefix)?
                .split(',')
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or(0)
}

fn bulk(v: V) -> String {
    match v {
        V::Str(text) => text,
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// `INFO clients` and `INFO commandstats`, asked together in one write, so
/// that the second is read as soon after the first as a client can: the
/// tightest reader there is. Returns `blocked_clients`, and the calls of
/// `name` as they stood when the second was answered.
fn look(c: &mut Conn, name: &str) -> (i64, i64) {
    let mut both = request(&["INFO", "clients"]);
    both.extend_from_slice(&request(&["INFO", "commandstats"]));
    c.writer.write_all(&both).unwrap();
    let clients = bulk(c.read());
    let stats = bulk(c.read());
    let blocked = field_of(&clients, "blocked_clients").expect("blocked_clients");
    (blocked, calls_of(&stats, name))
}

fn rounds() -> usize {
    std::env::var("KIVIDB_TEST_ROUNDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(300)
}

/// Put `block` through `rounds` short blocks, each ended by `wake`, and ask
/// after every one whether the reader that sees the client unblocked sees
/// its call. `name` is the command's line in `INFO commandstats`. Returns
/// the rounds in which the call was not there yet.
fn hammer(io: &str, block: &[&str], wake: &[&str], name: &str, rounds: usize) -> usize {
    let server = Server::start(io);
    let mut reader = server.conn();
    let mut waker = server.conn();
    // Streams are read from the end: the key must exist for `$` to mean
    // something, and the writes below add to it.
    waker.cmd(&["XADD", "s", "0-1", "f", "v"]);

    let (done_tx, done_rx) = mpsc::channel::<V>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (id_tx, id_rx) = mpsc::channel::<i64>();
    let port = server.port;
    let block: Vec<String> = block.iter().map(|a| a.to_string()).collect();
    let blocker = thread::spawn(move || {
        let mut c = Conn::connect(port);
        let V::Int(id) = c.cmd(&["CLIENT", "ID"]) else {
            panic!("CLIENT ID is an integer");
        };
        id_tx.send(id).unwrap();
        let args: Vec<&str> = block.iter().map(String::as_str).collect();
        // One block per turn, started when the reader says so, so that two
        // blocks never overlap.
        while go_rx.recv().is_ok() {
            c.send(&args);
            if done_tx.send(c.read()).is_err() {
                break;
            }
        }
    });

    // `{id}` in the waking command is the blocked client's id.
    let id = id_rx.recv().unwrap().to_string();
    let wake: Vec<String> = wake.iter().map(|a| a.replace("{id}", &id)).collect();
    let wake: Vec<&str> = wake.iter().map(String::as_str).collect();
    let wake = wake.as_slice();

    let mut late = 0;
    for round in 1..=rounds as i64 {
        go_tx.send(()).unwrap();
        // The client is blocked.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let (blocked, _) = look(&mut reader, name);
            if blocked == 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{io} {name}: never blocked in round {round}"
            );
            thread::sleep(Duration::from_millis(1));
        }
        waker.cmd(wake);
        // Until it is not: its call is counted by then.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let (blocked, calls) = look(&mut reader, name);
            if blocked == 0 {
                if calls < round {
                    late += 1;
                } else {
                    // Once: not counted again for the run that waited, or
                    // for the one that woke.
                    assert_eq!(calls, round, "{io} {name}: counted twice in round {round}");
                }
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{io} {name}: still blocked after its wake in round {round}"
            );
        }
        // Its reply is a delivery, not an error or a time-out.
        let reply = done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("the blocked client was never answered");
        assert!(
            !matches!(reply, V::Err(_) | V::Nil),
            "{io} {name}: round {round} answered {reply:?}"
        );
    }
    drop(go_tx);
    blocker.join().unwrap();

    // Settled, and counted exactly once each.
    let (blocked, calls) = look(&mut reader, name);
    assert_eq!(blocked, 0, "{io} {name}: still counted as blocked");
    assert_eq!(calls, rounds as i64, "{io} {name}: calls at the end");
    late
}

fn check(block: &[&str], wake: &[&str], name: &str) {
    let rounds = rounds();
    // Every I/O model is run before a failure is reported.
    let mut failed = Vec::new();
    for &io in io_models() {
        let late = hammer(io, block, wake, name, rounds);
        eprintln!("{io} {name}: {late} of {rounds} blocks seen unblocked before counted");
        if late != 0 {
            failed.push(format!(
                "{io} {name}: in {late} of {rounds} blocks the client was seen unblocked \
                 before its call was counted"
            ));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

#[test]
fn blpop_is_counted_before_its_client_is_unblocked() {
    check(&["BLPOP", "l", "0"], &["LPUSH", "l", "x"], "blpop");
}

#[test]
fn blmove_is_counted_before_its_client_is_unblocked() {
    check(
        &["BLMOVE", "src", "dst", "LEFT", "LEFT", "0"],
        &["LPUSH", "src", "x"],
        "blmove",
    );
}

#[test]
fn bzpopmin_is_counted_before_its_client_is_unblocked() {
    check(
        &["BZPOPMIN", "z", "0"],
        &["ZADD", "z", "1", "m"],
        "bzpopmin",
    );
}

#[test]
fn xread_block_is_counted_before_its_client_is_unblocked() {
    check(
        &["XREAD", "BLOCK", "0", "STREAMS", "s", "$"],
        &["XADD", "s", "*", "f", "v"],
        "xread",
    );
}

#[test]
fn wait_is_counted_before_its_client_is_unblocked() {
    // `WAIT` with no replica to reach blocks until it is unblocked.
    check(&["WAIT", "1", "0"], &["CLIENT", "UNBLOCK", "{id}"], "wait");
}

// ── Every way a wait ends leaves no blocked client behind ─────────────────────

fn blocked_clients(c: &mut Conn) -> i64 {
    field_of(&bulk(c.cmd(&["INFO", "clients"])), "blocked_clients").expect("blocked_clients")
}

/// Until `INFO` counts `n` blocked clients.
fn wait_for_blocked(c: &mut Conn, n: i64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while blocked_clients(c) != n {
        assert!(
            Instant::now() < deadline,
            "{what}: never {n} blocked clients (now {})",
            blocked_clients(c)
        );
        thread::sleep(Duration::from_millis(2));
    }
}

/// A client that has sent `block`, and its id: it is blocked once `INFO`
/// says so.
fn blocker(server: &Server, reader: &mut Conn, block: &[&str]) -> (Conn, i64) {
    let mut c = server.conn();
    let V::Int(id) = c.cmd(&["CLIENT", "ID"]) else {
        panic!("CLIENT ID is an integer");
    };
    c.send(block);
    wait_for_blocked(reader, 1, "a blocking command");
    (c, id)
}

#[test]
fn every_end_of_a_wait_leaves_no_blocked_client() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut reader = server.conn();
        let id_of = |id: i64| id.to_string();

        // Its time runs out.
        let (mut c, _) = blocker(&server, &mut reader, &["BLPOP", "l", "0.3"]);
        assert_eq!(c.read(), V::Nil, "{io}: time-out");
        wait_for_blocked(&mut reader, 0, &format!("{io}: time-out"));

        // CLIENT UNBLOCK, as a time-out and as an error.
        let (mut c, id) = blocker(&server, &mut reader, &["BLPOP", "l", "0"]);
        assert_eq!(
            reader.cmd(&["CLIENT", "UNBLOCK", &id_of(id), "TIMEOUT"]),
            V::Int(1)
        );
        assert_eq!(c.read(), V::Nil, "{io}: unblock timeout");
        wait_for_blocked(&mut reader, 0, &format!("{io}: unblock timeout"));
        let (mut c, id) = blocker(&server, &mut reader, &["BLPOP", "l", "0"]);
        assert_eq!(
            reader.cmd(&["CLIENT", "UNBLOCK", &id_of(id), "ERROR"]),
            V::Int(1)
        );
        assert!(matches!(c.read(), V::Err(_)), "{io}: unblock error");
        wait_for_blocked(&mut reader, 0, &format!("{io}: unblock error"));

        // CLIENT KILL.
        let (c, id) = blocker(&server, &mut reader, &["BLPOP", "l", "0"]);
        assert_eq!(reader.cmd(&["CLIENT", "KILL", "ID", &id_of(id)]), V::Int(1));
        wait_for_blocked(&mut reader, 0, &format!("{io}: kill"));
        drop(c);

        // A waiter that is beaten to the element waits again, and is
        // counted once however it goes on.
        let (mut c, _) = blocker(&server, &mut reader, &["BLPOP", "l", "0"]);
        // The push and the pop are one transaction: the list is empty again
        // when the waiter looks.
        let mut all = request(&["MULTI"]);
        all.extend_from_slice(&request(&["LPUSH", "l", "a"]));
        all.extend_from_slice(&request(&["LPOP", "l"]));
        all.extend_from_slice(&request(&["EXEC"]));
        reader.writer.write_all(&all).unwrap();
        for _ in 0..4 {
            reader.read();
        }
        // Parked again, or never woken: one blocked client either way.
        wait_for_blocked(&mut reader, 1, &format!("{io}: beaten to the element"));
        assert_eq!(reader.cmd(&["LPUSH", "l", "b"]), V::Int(1));
        assert!(matches!(c.read(), V::Arr(_)), "{io}: served at last");
        wait_for_blocked(&mut reader, 0, &format!("{io}: re-park"));

        // Its wait ends while a pause holds its resume dispatch: counted
        // once while held, and not at all after.
        let (mut c, id) = blocker(&server, &mut reader, &["BLPOP", "l", "0"]);
        assert_eq!(
            reader.cmd(&["CLIENT", "PAUSE", "20000", "WRITE"]),
            V::Str("OK".into())
        );
        assert_eq!(
            reader.cmd(&["CLIENT", "UNBLOCK", &id_of(id), "TIMEOUT"]),
            V::Int(1)
        );
        // Its wait is over when `CLIENT LIST` no longer shows it blocked;
        // from then on it is held, and one client counts once.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let list = bulk(reader.cmd(&["CLIENT", "LIST"]));
            let mine = list
                .lines()
                .find(|l| l.starts_with(&format!("id={id} ")))
                .expect("listed");
            let flags = mine
                .split(' ')
                .find_map(|word| word.strip_prefix("flags="))
                .expect("flags");
            if !flags.contains('b') {
                break;
            }
            assert!(Instant::now() < deadline, "{io}: still listed as blocked");
        }
        let until = Instant::now() + Duration::from_millis(500);
        while Instant::now() < until {
            assert!(
                blocked_clients(&mut reader) <= 1,
                "{io}: one client held by a pause is counted twice"
            );
        }
        assert_eq!(blocked_clients(&mut reader), 1, "{io}: held by the pause");
        assert_eq!(reader.cmd(&["CLIENT", "UNPAUSE"]), V::Str("OK".into()));
        assert_eq!(c.read(), V::Nil, "{io}: time-out after the pause");
        wait_for_blocked(&mut reader, 0, &format!("{io}: after the pause"));
    }
}
