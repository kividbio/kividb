// tests/blocked_client_gone.rs
//
// A client that goes while the server is waiting on its behalf — in a
// blocking command, held by a pause, in `WAIT` — must be let go at once, as
// Redis lets it go, and must never be given what it was waiting for (#384).
//
// On the io_uring loop a blocked client that had sent more bytes behind its
// blocking command and then closed was not seen to go: it stayed in line,
// kept its connection, and took the next element pushed, which nobody
// received. Bull, which n8n's queue mode uses, does exactly that whenever a
// worker pauses or stops.
//
// Every test runs a real `kividb` on each I/O model there is: tokio
// everywhere, and io_uring as well on Linux.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
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

fn s(text: &str) -> V {
    V::Str(text.to_string())
}

fn strs(items: &[&str]) -> V {
    V::Arr(items.iter().map(|item| s(item)).collect())
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

    /// What the client does when it goes, as ioredis's `disconnect()` does:
    /// its sending side is shut (the server is sent its end), and then the
    /// socket is closed.
    fn close(self) {
        let _ = self.writer.shutdown(Shutdown::Write);
    }

    /// Shut the sending side and wait, for at most `limit`, for the server
    /// to close the connection. How long it took, if it did.
    fn half_close_and_wait(mut self, limit: Duration) -> Option<Duration> {
        let from = Instant::now();
        self.writer.shutdown(Shutdown::Write).unwrap();
        let stream = self.reader.get_mut();
        let mut sink = [0u8; 4096];
        loop {
            let left = limit.checked_sub(from.elapsed())?;
            stream
                .set_read_timeout(Some(left.max(Duration::from_millis(1))))
                .unwrap();
            match stream.read(&mut sink) {
                Ok(0) => return Some(from.elapsed()),
                // Whatever the server still answers on its way out.
                Ok(_) => continue,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return None;
                }
                // Reset: closed too.
                Err(_) => return Some(from.elapsed()),
            }
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
        Self::start_with(io, &[])
    }

    fn start_with(io: &str, extra: &[&str]) -> Self {
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
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), s("PONG"));
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

fn info_field(c: &mut Conn, section: &str, field: &str) -> i64 {
    let V::Str(info) = c.cmd(&["INFO", section]) else {
        panic!("INFO is a bulk string");
    };
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|line| line.trim().strip_prefix(&prefix)?.parse().ok())
        .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
}

/// Until `INFO` counts `n` blocked clients (one a pause holds counts).
fn wait_blocked(c: &mut Conn, n: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while info_field(c, "clients", "blocked_clients") != n {
        assert!(Instant::now() < deadline, "never {n} blocked clients");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Whether `CLIENT LIST` still has the client named `name`.
fn listed(c: &mut Conn, name: &str) -> bool {
    let V::Str(list) = c.cmd(&["CLIENT", "LIST"]) else {
        panic!("CLIENT LIST is a bulk string");
    };
    let field = format!("name={name} ");
    list.lines().any(|line| line.contains(&field))
}

/// Whether the client named `name` leaves `CLIENT LIST` within `limit`.
fn leaves_within(c: &mut Conn, name: &str, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if !listed(c, name) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

/// "At once": Redis lets such a client go when it reads the end of its
/// stream. This leaves room for a loaded machine, and is far below any of
/// the timeouts the tests block with.
const AT_ONCE: Duration = Duration::from_secs(1);

/// A client named `name`, sent `block`, and blocked in it.
fn blocked_client(server: &Server, ctl: &mut Conn, name: &str, block: &[&str]) -> Conn {
    let mut c = server.conn();
    assert_eq!(c.cmd(&["CLIENT", "SETNAME", name]), s("OK"));
    c.send(block);
    wait_blocked(ctl, 1);
    c
}

// ── The blocking commands ─────────────────────────────────────────────────────

/// One blocking command: what it is sent, what gives it something, and what
/// must be true afterwards if nobody took it.
struct Case {
    name: &'static str,
    setup: &'static [&'static [&'static str]],
    block: &'static [&'static str],
    push: &'static [&'static str],
    left: fn(&mut Conn) -> Result<(), String>,
}

fn list_is(c: &mut Conn, key: &str, items: &[&str]) -> Result<(), String> {
    let got = c.cmd(&["LRANGE", key, "0", "-1"]);
    if got == strs(items) {
        Ok(())
    } else {
        Err(format!("{key} holds {got:?}, not {items:?}"))
    }
}

const CASES: &[Case] = &[
    Case {
        name: "brpoplpush",
        setup: &[],
        block: &["BRPOPLPUSH", "brpoplpush:wait", "brpoplpush:active", "30"],
        push: &["LPUSH", "brpoplpush:wait", "job-1"],
        left: |c| {
            list_is(c, "brpoplpush:wait", &["job-1"])?;
            list_is(c, "brpoplpush:active", &[])
        },
    },
    Case {
        name: "blpop",
        setup: &[],
        block: &["BLPOP", "blpop:wait", "30"],
        push: &["LPUSH", "blpop:wait", "job-1"],
        left: |c| list_is(c, "blpop:wait", &["job-1"]),
    },
    Case {
        name: "blmove",
        setup: &[],
        block: &[
            "BLMOVE",
            "blmove:wait",
            "blmove:active",
            "RIGHT",
            "LEFT",
            "30",
        ],
        push: &["LPUSH", "blmove:wait", "job-1"],
        left: |c| {
            list_is(c, "blmove:wait", &["job-1"])?;
            list_is(c, "blmove:active", &[])
        },
    },
    Case {
        name: "bzpopmin",
        setup: &[],
        block: &["BZPOPMIN", "bzpopmin:wait", "30"],
        push: &["ZADD", "bzpopmin:wait", "1", "job-1"],
        left: |c| {
            let got = c.cmd(&["ZRANGE", "bzpopmin:wait", "0", "-1"]);
            if got == strs(&["job-1"]) {
                Ok(())
            } else {
                Err(format!("bzpopmin:wait holds {got:?}"))
            }
        },
    },
    Case {
        name: "xreadgroup",
        setup: &[&["XGROUP", "CREATE", "xreadgroup:s", "g", "$", "MKSTREAM"]],
        block: &[
            "XREADGROUP",
            "GROUP",
            "g",
            "ghost",
            "BLOCK",
            "30000",
            "STREAMS",
            "xreadgroup:s",
            ">",
        ],
        push: &["XADD", "xreadgroup:s", "*", "job", "1"],
        left: |c| {
            // Delivered to nobody: not pending for the consumer that went,
            // and there for the next one to read.
            let V::Arr(summary) = c.cmd(&["XPENDING", "xreadgroup:s", "g"]) else {
                return Err("XPENDING is an array".into());
            };
            if summary.first() != Some(&V::Int(0)) {
                return Err(format!("pending for a consumer that went: {summary:?}"));
            }
            match c.cmd(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "alive",
                "STREAMS",
                "xreadgroup:s",
                ">",
            ]) {
                V::Arr(streams) if streams.len() == 1 => Ok(()),
                other => Err(format!("the next consumer read {other:?}")),
            }
        },
    },
];

/// The issue's own reproduction, for each blocking command: blocked, the
/// client sends more (Bull's `QUIT`), then closes. The server must let it go
/// at once, and what is pushed afterwards must stay where it was pushed.
///
/// Before the fix, on io_uring, the client stayed in `CLIENT LIST` until
/// its timeout and took the element: `BLPOP` lost it, `BRPOPLPUSH` and
/// `BLMOVE` moved it into the list of a worker that no longer existed,
/// `BZPOPMIN` lost the member, and `XREADGROUP` left the entry pending for a
/// consumer that was gone.
#[test]
fn a_client_that_sent_more_behind_its_blocking_command_and_closed_takes_nothing() {
    // Every command is tried, and then what went wrong with each is told.
    let mut failures = Vec::new();
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        for case in CASES {
            for setup in case.setup {
                let reply = ctl.cmd(setup);
                assert!(!matches!(reply, V::Err(_)), "{io} {}: {reply:?}", case.name);
            }
            let name = format!("ghost-{}", case.name);
            let mut c = blocked_client(&server, &mut ctl, &name, case.block);
            // Bytes behind the blocked command, which it does not read
            // while it waits.
            c.send(&["QUIT"]);
            thread::sleep(Duration::from_millis(300));
            c.close();

            let let_go = leaves_within(&mut ctl, &name, AT_ONCE);
            let reply = ctl.cmd(case.push);
            assert!(!matches!(reply, V::Err(_)), "{io} {}: {reply:?}", case.name);
            // Time for a client still in line to take it.
            thread::sleep(Duration::from_millis(300));
            if let Err(e) = (case.left)(&mut ctl) {
                failures.push(format!(
                    "{io} {}: the element went to a client that had gone: {e}",
                    case.name
                ));
            }
            if !let_go {
                failures.push(format!(
                    "{io} {}: the client was still in CLIENT LIST {AT_ONCE:?} after it closed",
                    case.name
                ));
            }
            // Nobody left blocked, before the next command is tried.
            let deadline = Instant::now() + Duration::from_secs(5);
            while info_field(&mut ctl, "clients", "blocked_clients") != 0 {
                assert!(
                    Instant::now() < deadline,
                    "{io} {}: still blocked",
                    case.name
                );
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Ten blocking commands sent together, then `QUIT`, then the close half a
/// second later: the connection is closed as soon as the end arrives —
/// the issue measured 49 seconds on io_uring, one timeout after another —
/// and none of the ten is run after the first.
#[test]
fn a_client_that_pipelined_blocking_commands_and_closed_is_let_go_at_once() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        let mut c = server.conn();
        assert_eq!(c.cmd(&["CLIENT", "SETNAME", "pipeliner"]), s("OK"));
        let mut batch = Vec::new();
        for _ in 0..10 {
            batch.extend(request(&["BRPOPLPUSH", "k1", "k2", "5"]));
        }
        c.writer.write_all(&batch).unwrap();
        wait_blocked(&mut ctl, 1);
        // Sent once the first is blocked, so the server has not read it.
        c.send(&["QUIT"]);
        thread::sleep(Duration::from_millis(500));

        let closed = c.half_close_and_wait(Duration::from_secs(10));
        assert!(
            closed.is_some_and(|took| took < AT_ONCE),
            "{io}: closed after {closed:?}"
        );
        assert!(!listed(&mut ctl, "pipeliner"), "{io}");
        assert_eq!(ctl.cmd(&["LPUSH", "k1", "job"]), V::Int(1), "{io}");
        thread::sleep(Duration::from_millis(200));
        assert_eq!(ctl.cmd(&["LLEN", "k1"]), V::Int(1), "{io}");
        assert_eq!(ctl.cmd(&["LLEN", "k2"]), V::Int(0), "{io}");
    }
}

/// n8n's `bull-idle-claim` integration test, "should stop waiting at once
/// when the worker pauses" (n8n 2.46.6, Bull 4.16.4), as the server sees it.
///
/// A Bull worker waits for jobs on a connection of its own, in
/// `BRPOPLPUSH <wait> <active> <drainDelay>`. To pause, Bull sends `QUIT` on
/// it — queued behind the block — and 500 ms later calls ioredis's
/// `disconnect()`, which shuts the socket's sending side; the pause is over
/// when the server has closed the connection. n8n wants that inside 2 s. On
/// Redis the server closes it as soon as the end arrives; on KiviDB's
/// io_uring loop it stayed until the block timed out, 5 s later, and the
/// pause took 5.5 s (n8n saw 2.5 s: ioredis destroys the socket itself
/// after 2 s). A job that arrives after the pause must stay in the queue
/// for a worker that is still there.
#[test]
fn a_bull_worker_that_pauses_stops_waiting_at_once() {
    const DRAIN_DELAY: &str = "5";
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        let mut worker = blocked_client(
            &server,
            &mut ctl,
            "bull:worker:blocking",
            &[
                "BRPOPLPUSH",
                "bull:jobs:wait",
                "bull:jobs:active",
                DRAIN_DELAY,
            ],
        );

        let paused_at = Instant::now();
        worker.send(&["QUIT"]);
        thread::sleep(Duration::from_millis(500));
        let closed = worker.half_close_and_wait(Duration::from_secs(10));
        let pause_took = paused_at.elapsed();
        assert!(closed.is_some(), "{io}: the server never closed it");
        assert!(
            pause_took < Duration::from_millis(2000),
            "{io}: expected {} ms to be less than 2000",
            pause_took.as_millis()
        );

        assert!(!listed(&mut ctl, "bull:worker:blocking"), "{io}");
        assert_eq!(
            ctl.cmd(&["LPUSH", "bull:jobs:wait", "job-1"]),
            V::Int(1),
            "{io}"
        );
        thread::sleep(Duration::from_millis(300));
        assert_eq!(
            ctl.cmd(&["LRANGE", "bull:jobs:wait", "0", "-1"]),
            strs(&["job-1"]),
            "{io}"
        );
        assert_eq!(ctl.cmd(&["LLEN", "bull:jobs:active"]), V::Int(0), "{io}");
    }
}

/// An element pushed straight after the client has closed, before any
/// periodic look at the socket: it is woken for the element, and must see
/// that it has gone before it takes it — the hand-off is where the element
/// is lost, not only the wait. Before the fix the io_uring loop looked at the
/// socket ten times a second, and an element pushed within that tenth of a
/// second went to the client that had gone.
#[test]
fn an_element_pushed_just_after_the_close_stays() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        let mut taken = Vec::new();
        for round in 0..20 {
            // A list of its own each round: what the last round left stays.
            let wait = format!("race:wait:{round}");
            let c = blocked_client(
                &server,
                &mut ctl,
                "racer",
                &["BRPOPLPUSH", &wait, "race:active", "0"],
            );
            c.close();
            // Enough for the end to reach the server's socket; far less than
            // the tenth of a second the old look waited.
            thread::sleep(Duration::from_millis(20));
            assert_eq!(ctl.cmd(&["LPUSH", &wait, "job"]), V::Int(1), "{io}");
            assert!(leaves_within(&mut ctl, "racer", AT_ONCE), "{io} {round}");
            if list_is(&mut ctl, &wait, &["job"]).is_err() {
                taken.push(round);
            }
        }
        assert!(
            taken.is_empty(),
            "{io}: in rounds {taken:?} the element went to a client that had gone"
        );
        assert_eq!(ctl.cmd(&["LLEN", "race:active"]), V::Int(0), "{io}");
    }
}

/// A blocking pop a `CLIENT PAUSE WRITE` holds, whose client closes before
/// the pause ends. Redis goes on reading a client a pause holds, sees it go,
/// and lets it go; the command never runs. Before the fix KiviDB, on both
/// loops, ran it when the pause ended — and it popped the element there for
/// a client that had gone.
#[test]
fn a_client_that_closes_while_a_pause_holds_it_takes_nothing() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        assert_eq!(ctl.cmd(&["LPUSH", "p:wait", "job-1"]), V::Int(1), "{io}");
        let mut c = server.conn();
        assert_eq!(c.cmd(&["CLIENT", "SETNAME", "held"]), s("OK"));
        assert_eq!(
            ctl.cmd(&["CLIENT", "PAUSE", "30000", "WRITE"]),
            s("OK"),
            "{io}"
        );
        c.send(&["BRPOPLPUSH", "p:wait", "p:active", "0"]);
        c.send(&["QUIT"]);
        wait_blocked(&mut ctl, 1);
        thread::sleep(Duration::from_millis(200));
        c.close();

        let let_go = leaves_within(&mut ctl, "held", AT_ONCE);
        assert_eq!(ctl.cmd(&["CLIENT", "UNPAUSE"]), s("OK"), "{io}");
        thread::sleep(Duration::from_millis(300));
        if let Err(e) = list_is(&mut ctl, "p:wait", &["job-1"]) {
            panic!("{io}: the element went to a client that had gone: {e}");
        }
        assert_eq!(ctl.cmd(&["LLEN", "p:active"]), V::Int(0), "{io}");
        assert!(let_go, "{io}: still listed while the pause held it");
    }
}

/// `WAIT` for more replicas than there are, with no timeout: it waits for
/// ever — and a client that goes meanwhile must be let go, as Redis lets it
/// go, not kept until a replica that will never come.
#[test]
fn a_client_that_closes_in_wait_is_let_go() {
    for &io in io_models() {
        let master = Server::start(io);
        let port = master.port.to_string();
        let _replica = Server::start_with(io, &["--replicaof", "127.0.0.1", &port]);
        let mut ctl = master.conn();
        let deadline = Instant::now() + Duration::from_secs(30);
        while info_field(&mut ctl, "replication", "connected_slaves") != 1 {
            assert!(
                Instant::now() < deadline,
                "{io}: the replica never connected"
            );
            thread::sleep(Duration::from_millis(20));
        }

        let mut c = master.conn();
        assert_eq!(c.cmd(&["CLIENT", "SETNAME", "waiter"]), s("OK"));
        assert_eq!(c.cmd(&["SET", "k", "v"]), s("OK"));
        c.send(&["WAIT", "2", "0"]);
        c.send(&["QUIT"]);
        thread::sleep(Duration::from_millis(300));
        assert!(listed(&mut ctl, "waiter"), "{io}: WAIT did not wait");
        c.close();
        assert!(
            leaves_within(&mut ctl, "waiter", AT_ONCE),
            "{io}: still listed, waiting for a replica, after it closed"
        );
    }
}

/// Inside `MULTI` and in a script a blocking command cannot block: it
/// answers at once, as if its time had run out, as in Redis. So there is no
/// wait there for a client to go during, even with the timeout of 0 that
/// otherwise waits for ever.
#[test]
fn inside_multi_and_scripts_blocking_commands_do_not_wait() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut c = server.conn();
        assert_eq!(c.cmd(&["MULTI"]), s("OK"), "{io}");
        let blocking: [&[&str]; 4] = [
            &["BLPOP", "m:empty", "0"],
            &["BRPOPLPUSH", "m:empty", "m:dst", "0"],
            &["BZPOPMIN", "m:z", "0"],
            &["XREAD", "BLOCK", "0", "STREAMS", "m:s", "0"],
        ];
        for block in blocking {
            assert_eq!(c.cmd(block), s("QUEUED"), "{io} {block:?}");
        }
        let started = Instant::now();
        assert_eq!(c.cmd(&["EXEC"]), V::Arr(vec![V::Nil; 4]), "{io}");
        assert!(started.elapsed() < AT_ONCE, "{io}");
        #[cfg(feature = "lua")]
        {
            let started = Instant::now();
            let script = "return redis.call('BLPOP', KEYS[1], 0)";
            assert_eq!(c.cmd(&["EVAL", script, "1", "m:empty"]), V::Nil, "{io}");
            assert!(started.elapsed() < AT_ONCE, "{io}");
        }
    }
}

/// A client woken for an element whose command is then refused before it
/// looks at the key passes its turn to the client behind it. Here memory is
/// over `maxmemory` when the first client is woken (by a `RENAME`, which is
/// let through then), so its `BRPOPLPUSH`, which may add to memory, is
/// refused with OOM, while the `BLPOP` behind it is not. Before, the first
/// client's place was dropped without passing the turn on: the element sat
/// in the list with a client blocked on it, until something else wrote the
/// key. A pause that holds the resume of a client that then closes, or
/// `READONLY` after a failover, drop the place the same way.
#[test]
fn a_woken_client_refused_before_it_looks_passes_its_turn_on() {
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        assert_eq!(ctl.cmd(&["RPUSH", "o:src", "job"]), V::Int(1), "{io}");
        let mut first = blocked_client(
            &server,
            &mut ctl,
            "first",
            &["BRPOPLPUSH", "o:wait", "o:first", "0"],
        );
        let mut second = server.conn();
        second.send(&["BLPOP", "o:wait", "0"]);
        wait_blocked(&mut ctl, 2);

        for (setting, value) in [("maxmemory-policy", "noeviction"), ("maxmemory", "1")] {
            assert_eq!(ctl.cmd(&["CONFIG", "SET", setting, value]), s("OK"), "{io}");
        }
        assert_eq!(ctl.cmd(&["RENAME", "o:src", "o:wait"]), s("OK"), "{io}");
        let refused = first.read();
        assert!(
            matches!(&refused, V::Err(e) if e.starts_with("OOM")),
            "{io}: {refused:?}"
        );
        second
            .reader
            .get_ref()
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut line = String::new();
        let got = second.reader.read_line(&mut line);
        assert_eq!(
            ctl.cmd(&["CONFIG", "SET", "maxmemory", "0"]),
            s("OK"),
            "{io}"
        );
        assert!(
            got.is_ok_and(|n| n > 0),
            "{io}: the client behind was never woken; o:wait holds {:?}",
            ctl.cmd(&["LRANGE", "o:wait", "0", "-1"])
        );
        assert_eq!(second.read(), s("o:wait"), "{io}");
        assert_eq!(second.read(), s("job"), "{io}");
    }
}

/// A write sent just as a pause ends, by a client that closes at once — a
/// fire-and-forget `LPUSH` from a producer — runs, as in Redis: the pause no
/// longer holds it. While the clients the pause held go on (here a script
/// that runs for a while), the commands that come after wait a moment at
/// the gate, and that wait used to give up on a client that had closed, and
/// drop its command.
#[cfg(feature = "lua")]
#[test]
fn a_write_sent_as_a_pause_ends_runs_though_its_client_closes() {
    // Pure Lua, so its length does not depend on the clock a script reads.
    let script = "local x = 0 for i = 1, 30000000 do x = x + i end return 1";
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        let mut held = server.conn();
        let mut producer = server.conn();
        assert_eq!(
            ctl.cmd(&["CLIENT", "PAUSE", "30000", "WRITE"]),
            s("OK"),
            "{io}"
        );
        held.send(&["EVAL", script, "0"]);
        wait_blocked(&mut ctl, 1);
        assert_eq!(ctl.cmd(&["CLIENT", "UNPAUSE"]), s("OK"), "{io}");
        producer.send(&["LPUSH", "e:q", "job"]);
        producer.close();
        assert_eq!(held.read(), V::Int(1), "{io}");
        let deadline = Instant::now() + Duration::from_secs(5);
        while ctl.cmd(&["LLEN", "e:q"]) != V::Int(1) {
            assert!(
                Instant::now() < deadline,
                "{io}: the LPUSH of a client that closed was dropped"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

/// A client that keeps sending while a pause holds its command is read
/// into memory on the tokio loop, to be run once the pause ends; past
/// `client-query-buffer-limit` it is closed, as Redis closes a client whose
/// query buffer reaches the limit, and its held command is not run. It used
/// to be read without end. (The io_uring loop does not read a client that
/// waits.)
#[test]
fn a_client_that_keeps_sending_while_held_is_closed_past_the_query_buffer_limit() {
    let server = Server::start("tokio");
    let mut ctl = server.conn();
    assert_eq!(
        ctl.cmd(&["CONFIG", "SET", "client-query-buffer-limit", "1mb"]),
        s("OK")
    );
    assert_eq!(ctl.cmd(&["CLIENT", "PAUSE", "30000", "WRITE"]), s("OK"));
    let mut c = server.conn();
    c.send(&["SET", "q:k", "v"]);
    wait_blocked(&mut ctl, 1);

    let mut writer = c.writer.try_clone().unwrap();
    let flood = thread::spawn(move || {
        let ping = request(&["PING"]);
        let chunk: Vec<u8> = ping.iter().copied().cycle().take(64 * 1024).collect();
        for _ in 0..64 {
            if writer.write_all(&chunk).is_err() {
                return;
            }
        }
    });
    let stream = c.reader.get_mut();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut sink = [0u8; 4096];
    let closed = loop {
        match stream.read(&mut sink) {
            Ok(0) => break true,
            Ok(_) => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break false;
            }
            Err(_) => break true,
        }
    };
    assert_eq!(ctl.cmd(&["CLIENT", "UNPAUSE"]), s("OK"));
    drop(c);
    let _ = flood.join();
    assert!(closed, "a client sending 4 MB past a 1 MB limit was kept");
    assert_eq!(ctl.cmd(&["EXISTS", "q:k"]), V::Int(0));
}

/// A blocked client woken by a write that a pause held, as the pause ends,
/// can be kept at the gate a moment after it was woken and looked at its
/// socket: while the other clients the pause let go go on (here a script
/// that runs for a while). If it closes then, the element must stay. The
/// socket is looked at again once the gate lets the resume through; it used
/// to be popped, and answered to nobody.
#[cfg(feature = "lua")]
#[test]
fn a_client_woken_as_a_pause_ends_and_gone_at_the_gate_takes_nothing() {
    let script = "local x = 0 for i = 1, 100000000 do x = x + i end return 1";
    for &io in io_models() {
        let server = Server::start(io);
        let mut ctl = server.conn();
        let woken = blocked_client(&server, &mut ctl, "woken", &["BLPOP", "g:wait", "0"]);
        let mut slow = server.conn();
        let mut pusher = server.conn();
        assert_eq!(
            ctl.cmd(&["CLIENT", "PAUSE", "30000", "WRITE"]),
            s("OK"),
            "{io}"
        );
        slow.send(&["EVAL", script, "0"]);
        pusher.send(&["LPUSH", "g:wait", "job"]);
        wait_blocked(&mut ctl, 3);
        assert_eq!(ctl.cmd(&["CLIENT", "UNPAUSE"]), s("OK"), "{io}");
        // Woken by the push the pause let go, and at the gate while the
        // script runs: well inside the gate's tenth of a second.
        thread::sleep(Duration::from_millis(30));
        woken.close();
        assert_eq!(pusher.read(), V::Int(1), "{io}");
        assert_eq!(slow.read(), V::Int(1), "{io}");
        assert!(leaves_within(&mut ctl, "woken", AT_ONCE), "{io}");
        assert_eq!(
            ctl.cmd(&["LRANGE", "g:wait", "0", "-1"]),
            strs(&["job"]),
            "{io}: the element went to a client that had gone"
        );
    }
}

/// A woken client whose resume is refused (OOM) and that has more pipelined
/// behind it — here a `WAIT` that waits for a replica that is not there —
/// passes its turn on at once, not when its batch of commands is over: the
/// client behind it gets the element while the `WAIT` still waits.
#[test]
fn a_refused_resume_passes_its_turn_on_before_what_follows_it() {
    for &io in io_models() {
        let master = Server::start(io);
        let port = master.port.to_string();
        let _replica = Server::start_with(io, &["--replicaof", "127.0.0.1", &port]);
        let mut ctl = master.conn();
        let deadline = Instant::now() + Duration::from_secs(30);
        while info_field(&mut ctl, "replication", "connected_slaves") != 1 {
            assert!(
                Instant::now() < deadline,
                "{io}: the replica never connected"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(ctl.cmd(&["RPUSH", "w:src", "job"]), V::Int(1), "{io}");
        let mut first = master.conn();
        let mut batch = request(&["BRPOPLPUSH", "w:wait", "w:first", "0"]);
        batch.extend(request(&["WAIT", "2", "0"]));
        first.writer.write_all(&batch).unwrap();
        wait_blocked(&mut ctl, 1);
        let mut second = master.conn();
        second.send(&["BLPOP", "w:wait", "0"]);
        wait_blocked(&mut ctl, 2);

        for (setting, value) in [("maxmemory-policy", "noeviction"), ("maxmemory", "1")] {
            assert_eq!(ctl.cmd(&["CONFIG", "SET", setting, value]), s("OK"), "{io}");
        }
        assert_eq!(ctl.cmd(&["RENAME", "w:src", "w:wait"]), s("OK"), "{io}");
        // The first client's OOM is not read: its replies go out together
        // once its batch is done, and the WAIT in it does not end.
        second
            .reader
            .get_ref()
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut line = String::new();
        let got = second.reader.read_line(&mut line);
        assert_eq!(
            ctl.cmd(&["CONFIG", "SET", "maxmemory", "0"]),
            s("OK"),
            "{io}"
        );
        assert!(
            got.is_ok_and(|n| n > 0),
            "{io}: the client behind was not woken while the WAIT waited; w:wait holds {:?}",
            ctl.cmd(&["LRANGE", "w:wait", "0", "-1"])
        );
        assert_eq!(second.read(), s("w:wait"), "{io}");
        assert_eq!(second.read(), s("job"), "{io}");
    }
}
