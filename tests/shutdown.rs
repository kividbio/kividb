// tests/shutdown.rs
use std::io::Write;
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};
use std::{env, thread};

fn wait_for_server(port: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_exit(server: &mut std::process::Child, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        // try_wait() returns Ok(Some(_)) when the child has exited,
        // Ok(None) when it's still running. This correctly handles
        // zombies on macOS unlike kill(pid, 0).
        match server.try_wait() {
            Ok(Some(_)) => return true, // exited
            Ok(None) => {}              // still running
            Err(_) => return true,      // can't query = already gone
        }
        if start.elapsed() > timeout {
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn kividb_bin() -> String {
    env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| {
        let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        format!("{manifest}/target/debug/kividb")
    })
}

#[test]
fn shutdown_nosave_exits_cleanly() {
    let port = free_port();
    // A directory of its own: one engine to a data directory.
    let mut server = Command::new(kividb_bin())
        .current_dir(tempfile::tempdir().unwrap().keep())
        .args(["--port", &port.to_string()])
        .spawn()
        .expect("failed to start kividb — run `cargo build` first");

    assert!(
        wait_for_server(port, Duration::from_secs(5)),
        "server on port {port} did not come up within 5s"
    );

    let mut conn = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    conn.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .unwrap();
    drop(conn);

    assert!(
        wait_for_exit(&mut server, Duration::from_secs(5)),
        "server did not exit after SHUTDOWN NOSAVE"
    );
}

/// A port nothing is listening on, in 10000-29999 (below the ports the
/// system hands out to outgoing connections), from a place of this process's
/// own: under `cargo nextest` every test is a process.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(31);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)
        else {
            continue;
        };
        let _ = probe.set_reuse_address(true);
        if probe
            .bind(&std::net::SocketAddr::from(([127, 0, 0, 1], port)).into())
            .is_ok()
        {
            return port;
        }
    }
}

/// One command, and its reply: a bulk string's body, `(nil)` for a null
/// one, and any other reply's line as it came.
fn resp_cmd(conn: &mut std::io::BufReader<TcpStream>, args: &[&str]) -> std::io::Result<String> {
    use std::io::BufRead;
    let mut req = format!("*{}\r\n", args.len());
    for a in args {
        req.push_str(&format!("${}\r\n{a}\r\n", a.len()));
    }
    conn.get_mut().write_all(req.as_bytes())?;
    let mut line = String::new();
    if conn.read_line(&mut line)? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    if line.trim_end() == "$-1" {
        return Ok("(nil)".to_string());
    }
    if let Some(len) = line.trim_end().strip_prefix('$') {
        let mut body = vec![0u8; len.parse::<usize>().unwrap() + 2];
        std::io::Read::read_exact(conn, &mut body)?;
        return Ok(String::from_utf8_lossy(&body[..body.len() - 2]).into_owned());
    }
    Ok(line.trim_end().to_string())
}

/// What answered on a port.
enum Answer {
    /// The server this test started, connected.
    Ours(std::io::BufReader<TcpStream>),
    /// Another process: a server another test started on the port while
    /// this one's was down. What it holds says nothing about ours.
    Other(String),
    /// Ours exited without taking connections (its port was taken).
    Exited(std::process::ExitStatus),
}

/// Wait for `server` to take connections on `port`, and make sure it is the
/// process that answers. A port is free between a server's exit and its
/// restart, and another test's server can be started on it then: the
/// restarted one cannot listen, and a client that only connected would read
/// the other's dataset as ours (the restarted server "held 0").
fn connect_to(server: &mut std::process::Child, port: u16, timeout: Duration) -> Answer {
    let start = Instant::now();
    loop {
        if let Ok(Some(status)) = server.try_wait() {
            return Answer::Exited(status);
        }
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
            let mut conn = std::io::BufReader::new(s);
            if let Ok(info) = resp_cmd(&mut conn, &["INFO", "server"])
                && let Some(pid) = info
                    .lines()
                    .find_map(|l| l.strip_prefix("process_id:"))
                    .map(|v| v.trim().to_string())
            {
                if pid == server.id().to_string() {
                    return Answer::Ours(conn);
                }
                return Answer::Other(format!("process {pid}"));
            }
        }
        assert!(
            start.elapsed() < timeout,
            "the server on port {port} did not come up within {timeout:?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// Start a server on `dir`, on `port` if it can be had and on another free
/// port if not, and connect to it: the port does not matter, the data
/// directory does.
fn start_on(
    dir: &std::path::Path,
    port: u16,
) -> (std::process::Child, u16, std::io::BufReader<TcpStream>) {
    start_with(dir, port, &[], &[], &[])
}

/// [`start_on`], with arguments (`base`, then `extra`) and environment.
fn start_with(
    dir: &std::path::Path,
    port: u16,
    base: &[&str],
    extra: &[&str],
    env: &[(&str, &str)],
) -> (std::process::Child, u16, std::io::BufReader<TcpStream>) {
    let mut port = port;
    for _ in 0..10 {
        let mut server = Command::new(kividb_bin())
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .args(base)
            .args(extra)
            .envs(env.iter().copied())
            .spawn()
            .expect("failed to start kividb");
        match connect_to(&mut server, port, Duration::from_secs(60)) {
            Answer::Ours(conn) => return (server, port, conn),
            Answer::Other(who) => {
                eprintln!("port {port} is {who}'s, not this test's server: another port");
            }
            Answer::Exited(status) => {
                eprintln!("the server could not take port {port} ({status}): another port");
            }
        }
        let _ = server.kill();
        let _ = server.wait();
        port = free_port();
    }
    panic!("no port could be had for the server");
}

/// A write acknowledged before a `SHUTDOWN` (or SIGTERM, which takes the
/// same path) is in the final snapshot. The connections already open kept
/// running while the snapshot was written, so a write made after its cut
/// was acknowledged, and then lost with the process.
#[test]
fn shutdown_keeps_every_acknowledged_write() {
    let dir = std::env::temp_dir().join(format!("kividb-shutdown-acks-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let acked = write_until_shutdown(&dir, free_port());
    let kept = counter_after_restart(&dir, free_port(), || {});
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        kept >= acked,
        "{acked} INCRs were acknowledged but the snapshot holds {kept}"
    );
}

/// The restart above is read from the server it started, and from no other:
/// here another process takes the port in the moment between the exit and
/// the restart, and answers every command, as an empty server would. It used
/// to be read as the restarted server, which then "held 0" (seen under
/// `cargo nextest`, where many tests start servers at once).
#[test]
fn a_restart_is_read_from_its_own_server_only() {
    let dir = std::env::temp_dir().join(format!("kividb-shutdown-other-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let acked = write_until_shutdown(&dir, port);
    let answered = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let kept = counter_after_restart(&dir, port, || {
        // An empty server of another process on the port: `(nil)` to a
        // GET, 0 keys, and a process id that is not the restarted one's.
        let listener = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
        let answered = std::sync::Arc::clone(&answered);
        thread::spawn(move || {
            for conn in listener.incoming().flatten() {
                answered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                thread::spawn(move || {
                    use std::io::BufRead;
                    let mut reader = std::io::BufReader::new(conn.try_clone().unwrap());
                    let mut conn = conn;
                    let mut line = String::new();
                    while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
                        // One reply for each command's last argument line.
                        let cmd = line.trim_end().to_ascii_uppercase();
                        line.clear();
                        let reply: &[u8] = match cmd.as_str() {
                            "SERVER" => b"$14\r\nprocess_id:1\r\n\r\n",
                            "COUNTER" => b"$-1\r\n",
                            _ => continue,
                        };
                        if conn.write_all(reply).is_err() {
                            break;
                        }
                    }
                });
            }
        });
    });
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        answered.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "the other process was never asked"
    );
    assert!(
        kept >= acked,
        "{acked} INCRs were acknowledged but the snapshot holds {kept}"
    );
}

/// Start a server on `dir`, fill it, have six connections `INCR` a counter
/// while a seventh sends `SHUTDOWN`, and return how many were acknowledged.
fn write_until_shutdown(dir: &std::path::Path, port: u16) -> u64 {
    let (mut server, port, mut conn) = start_on(dir, port);
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        std::io::BufReader::new(s)
    };
    // Enough data that the final snapshot takes a while: the writers have
    // time to go on while it is written.
    assert_eq!(
        resp_cmd(&mut conn, &["DEBUG", "POPULATE", "300000", "fill", "64"]).unwrap(),
        "+OK"
    );
    let writers: Vec<_> = (0..6)
        .map(|_| {
            let mut conn = connect();
            thread::spawn(move || {
                let mut acked = 0u64;
                while let Ok(reply) = resp_cmd(&mut conn, &["INCR", "counter"]) {
                    if reply.starts_with(':') {
                        acked += 1;
                    } else {
                        break;
                    }
                }
                acked
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(500));
    let _ = resp_cmd(&mut conn, &["SHUTDOWN"]);
    assert!(
        wait_for_exit(&mut server, Duration::from_secs(30)),
        "no exit after SHUTDOWN"
    );
    let status = server.wait().unwrap();
    assert!(status.success(), "the server exited with {status}");
    let acked: u64 = writers.into_iter().map(|w| w.join().unwrap()).sum();
    assert!(acked > 0, "no write was acknowledged");
    acked
}

/// Restart on `dir` (on `port`, once `between` has run), and read the
/// counter from the restarted server.
fn counter_after_restart(dir: &std::path::Path, port: u16, between: impl FnOnce()) -> u64 {
    between();
    let (mut server, _, mut conn) = start_on(dir, port);
    let mut value = None;
    for _ in 0..100 {
        match resp_cmd(&mut conn, &["GET", "counter"]) {
            Ok(v) if !v.starts_with('-') => {
                value = Some(v);
                break;
            }
            // LOADING, or the like: ask again.
            Ok(_) => thread::sleep(Duration::from_millis(100)),
            Err(e) => panic!("the restarted server did not answer: {e}"),
        }
    }
    let _ = server.kill();
    let _ = server.wait();
    match value.as_deref() {
        Some("(nil)") => 0,
        Some(v) => v.parse().unwrap_or_else(|_| panic!("GET counter: {v:?}")),
        None => panic!("the restarted server answered GET with errors only"),
    }
}

/// Once the server has exited, its port takes no connection. On io_uring the
/// kernel kept the workers' sockets listening until it had torn their rings
/// down, a moment after the process was gone: a client that connected then,
/// as one waiting for a restart does, was answered by nobody and reset. The
/// test above then read nothing back from the restarted server, and reported
/// a snapshot that held 0.
#[test]
fn port_takes_no_connection_once_the_server_has_exited() {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("kividb-shutdown-port-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    for mode in ["NOSAVE", "SAVE"] {
        for _ in 0..5 {
            let mut server = Command::new(kividb_bin())
                .args(["--port", &port.to_string(), "--dir", dir.to_str().unwrap()])
                .spawn()
                .expect("failed to start kividb");
            assert!(wait_for_server(port, Duration::from_secs(10)));
            let mut conn = TcpStream::connect(addr).unwrap();
            let req = format!("*2\r\n$8\r\nSHUTDOWN\r\n${}\r\n{mode}\r\n", mode.len());
            conn.write_all(req.as_bytes()).unwrap();
            // The exit is seen as soon as it happens: no sleep between looks.
            let start = Instant::now();
            while server.try_wait().is_ok_and(|exited| exited.is_none()) {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "no exit after SHUTDOWN {mode}"
                );
                std::hint::spin_loop();
            }
            let exited = Instant::now();
            while exited.elapsed() < Duration::from_millis(200) {
                if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
                    let _ = std::fs::remove_dir_all(&dir);
                    panic!(
                        "the port took a connection {:?} after the server exited (SHUTDOWN {mode})",
                        exited.elapsed()
                    );
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── SHUTDOWN as Redis answers it (#429) ──────────────────────────────────
//
// A shutdown that goes ahead is answered with nothing: the connection closes
// when the process exits, after the final save. redis-py's `shutdown()`
// takes any reply for a failure. A shutdown that fails, or is aborted, is
// answered with an error and the server runs on. The replies below are
// Redis 8.10.2's, byte for byte.

/// The io models: `auto` is io_uring where the machine has it.
const MODELS: [&str; 2] = ["auto", "tokio"];

const FAILED: &[u8] = b"-ERR Errors trying to SHUTDOWN. Check logs.\r\n";

/// A server of these tests, in a data directory of its own.
struct Srv {
    child: std::process::Child,
    port: u16,
    dir: std::path::PathBuf,
}

impl Srv {
    fn start(model: &str, extra: &[&str], env: &[(&str, &str)]) -> Srv {
        let dir = tempfile::tempdir().unwrap().keep();
        let (child, port, _) = start_with(&dir, free_port(), &["--io-model", model], extra, env);
        Srv { child, port, dir }
    }

    /// A raw connection: replies are read as bytes.
    fn raw(&self) -> TcpStream {
        let conn = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        conn.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        conn
    }

    fn exited(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        let start = Instant::now();
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            if start.elapsed() > within {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Srv {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn encode(args: &[&str]) -> Vec<u8> {
    let mut req = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        req.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    req
}

/// What the server sends until it closes the connection, or `within` is up:
/// the bytes, and whether it closed.
fn until_closed(conn: &mut TcpStream, within: Duration) -> (Vec<u8>, bool) {
    use std::io::Read;
    let start = Instant::now();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while start.elapsed() < within {
        match conn.read(&mut buf) {
            Ok(0) => return (got, true),
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            // A reset is a close too.
            Err(_) => return (got, true),
        }
    }
    (got, false)
}

/// What the server has sent so far, until `len` bytes or `within`.
fn read_len(conn: &mut TcpStream, len: usize, within: Duration) -> Vec<u8> {
    use std::io::Read;
    let start = Instant::now();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while got.len() < len && start.elapsed() < within {
        match conn.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(_) => {}
        }
    }
    got
}

/// Every form of a SHUTDOWN that goes ahead: no reply at all, then the
/// connection closes, and the process exits with status 0.
#[test]
fn a_shutdown_that_goes_ahead_sends_no_reply() {
    for model in MODELS {
        for form in [
            &["SHUTDOWN"][..],
            &["SHUTDOWN", "NOSAVE"],
            &["SHUTDOWN", "SAVE"],
            &["SHUTDOWN", "NOW"],
            &["SHUTDOWN", "FORCE"],
            &["SHUTDOWN", "NOSAVE", "NOW"],
            &["SHUTDOWN", "NOW", "FORCE"],
            &["shutdown", "nosave"],
        ] {
            let mut server = Srv::start(model, &[], &[]);
            let mut conn = server.raw();
            conn.write_all(&encode(form)).unwrap();
            let (reply, closed) = until_closed(&mut conn, Duration::from_secs(30));
            assert_eq!(
                reply,
                b"",
                "{model}: {form:?} was answered {:?}",
                String::from_utf8_lossy(&reply)
            );
            assert!(closed, "{model}: {form:?}: the connection was not closed");
            let status = server
                .exited(Duration::from_secs(30))
                .unwrap_or_else(|| panic!("{model}: no exit after {form:?}"));
            assert!(status.success(), "{model}: {form:?} exited with {status}");
        }
    }
}

/// Replies that were owed before the SHUTDOWN are sent before the
/// connection closes: a pipelined `SET`, on RESP2 and RESP3; with
/// `CLIENT REPLY OFF` there are none. The commands after a SHUTDOWN that
/// goes ahead are not run.
#[test]
fn replies_before_a_shutdown_are_sent_and_commands_after_it_are_not_run() {
    for model in MODELS {
        for resp3 in [false, true] {
            let dir = tempfile::tempdir().unwrap().keep();
            let (mut server, port, _) =
                start_with(&dir, free_port(), &["--io-model", model], &[], &[]);
            let mut conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
            conn.set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            if resp3 {
                conn.write_all(&encode(&["HELLO", "3"])).unwrap();
                // The reply is a map: the connection is read to its end.
                thread::sleep(Duration::from_millis(300));
                let _ = read_len(&mut conn, usize::MAX, Duration::from_millis(300));
            }
            // One write: the three commands arrive together.
            let mut batch = encode(&["SET", "before", "1"]);
            batch.extend(encode(&["SHUTDOWN", "SAVE"]));
            batch.extend(encode(&["SET", "after", "1"]));
            conn.write_all(&batch).unwrap();
            let (reply, closed) = until_closed(&mut conn, Duration::from_secs(30));
            assert_eq!(
                reply,
                b"+OK\r\n",
                "{model} resp3={resp3}: got {:?}",
                String::from_utf8_lossy(&reply)
            );
            assert!(closed, "{model} resp3={resp3}: not closed");
            assert!(
                wait_for_exit(&mut server, Duration::from_secs(30)),
                "{model}: no exit"
            );
            // What was acknowledged is in the final save; what came after
            // the SHUTDOWN was never run.
            let (mut again, _, mut c) = start_on(&dir, free_port());
            assert_eq!(resp_cmd(&mut c, &["GET", "before"]).unwrap(), "1");
            assert_eq!(resp_cmd(&mut c, &["GET", "after"]).unwrap(), "(nil)");
            let _ = again.kill();
            let _ = again.wait();
            let _ = std::fs::remove_dir_all(&dir);
        }
        // `CLIENT REPLY OFF`: there was no reply to the SET, and there is
        // none to the SHUTDOWN.
        let mut server = Srv::start(model, &[], &[]);
        let mut conn = server.raw();
        let mut batch = encode(&["CLIENT", "REPLY", "OFF"]);
        batch.extend(encode(&["SET", "k", "v"]));
        batch.extend(encode(&["SHUTDOWN", "NOSAVE"]));
        conn.write_all(&batch).unwrap();
        let (reply, closed) = until_closed(&mut conn, Duration::from_secs(30));
        assert_eq!(reply, b"", "{model}: REPLY OFF: got {reply:?}");
        assert!(closed);
        assert!(server.exited(Duration::from_secs(30)).is_some());
    }
}

/// The forms Redis refuses, with its replies; none stops the server.
#[test]
fn shutdown_refusals_match_redis_and_leave_the_server_running() {
    for model in MODELS {
        let server = Srv::start(model, &[], &[]);
        let mut conn = server.raw();
        let cases: [(&[&str], &[u8]); 7] = [
            (&["SHUTDOWN", "SAVE", "NOSAVE"], b"-ERR syntax error\r\n"),
            (&["SHUTDOWN", "FOO"], b"-ERR syntax error\r\n"),
            (&["SHUTDOWN", "ABORT", "NOW"], b"-ERR syntax error\r\n"),
            (&["SHUTDOWN", "ABORT"], b"-ERR No shutdown in progress.\r\n"),
            (&["MULTI"], b"+OK\r\n"),
            (
                &["SHUTDOWN"],
                b"-ERR Command not allowed inside a transaction\r\n",
            ),
            (
                &["EXEC"],
                b"-EXECABORT Transaction discarded because of previous errors.\r\n",
            ),
        ];
        for (args, expected) in cases {
            conn.write_all(&encode(args)).unwrap();
            let got = read_len(&mut conn, expected.len(), Duration::from_secs(10));
            assert_eq!(
                got,
                expected,
                "{model}: {args:?}: got {:?}",
                String::from_utf8_lossy(&got)
            );
        }
        conn.write_all(&encode(&["PING"])).unwrap();
        assert_eq!(
            read_len(&mut conn, 7, Duration::from_secs(10)),
            b"+PONG\r\n"
        );
    }
}

/// A script may not shut the server down.
#[cfg(feature = "lua")]
#[test]
fn a_script_cannot_shut_the_server_down() {
    for model in MODELS {
        let server = Srv::start(model, &[], &[]);
        let mut conn = server.raw();
        conn.write_all(&encode(&["EVAL", "return redis.pcall('shutdown')", "0"]))
            .unwrap();
        let got = read_len(&mut conn, 1, Duration::from_secs(10));
        assert_eq!(got.first(), Some(&b'-'), "{model}: {got:?}");
        conn.write_all(&encode(&["PING"])).unwrap();
        let rest = until_closed(&mut conn, Duration::from_millis(500)).0;
        assert!(rest.ends_with(b"+PONG\r\n"), "{model}: {rest:?}");
    }
}

/// A final save that fails is answered with an error, and the server runs
/// on: writes are accepted again (the gate and the pause are undone), the
/// replies after the SHUTDOWN in the pipeline are sent after the error, and
/// `CLIENT REPLY OFF` keeps the error from being sent. `FORCE` and `NOSAVE`
/// end the process anyway.
#[test]
fn a_failed_final_save_is_an_error_and_the_server_runs_on() {
    for model in MODELS {
        let mut server = Srv::start(model, &[], &[("KIVIDB_TEST_FAILPOINTS", "final-save")]);
        let mut conn = server.raw();
        // The SHUTDOWN, a PING after it in the same write, and a write.
        let mut batch = encode(&["SET", "kept", "1"]);
        batch.extend(encode(&["SHUTDOWN"]));
        batch.extend(encode(&["PING"]));
        conn.write_all(&batch).unwrap();
        let expected = [&b"+OK\r\n"[..], FAILED, b"+PONG\r\n"].concat();
        let got = read_len(&mut conn, expected.len(), Duration::from_secs(30));
        assert_eq!(
            got,
            expected,
            "{model}: got {:?}",
            String::from_utf8_lossy(&got)
        );
        // Not paused, not stopped: another connection writes and reads, and
        // its writes are acknowledged.
        let mut other = server.raw();
        other.write_all(&encode(&["SET", "later", "2"])).unwrap();
        assert_eq!(read_len(&mut other, 5, Duration::from_secs(10)), b"+OK\r\n");
        other.write_all(&encode(&["GET", "kept"])).unwrap();
        assert_eq!(
            read_len(&mut other, 7, Duration::from_secs(10)),
            b"$1\r\n1\r\n"
        );
        // Again: the same, with RESP3 and with replies off.
        conn.write_all(&encode(&["HELLO", "3"])).unwrap();
        thread::sleep(Duration::from_millis(300));
        let _ = read_len(&mut conn, usize::MAX, Duration::from_millis(300));
        conn.write_all(&encode(&["SHUTDOWN", "SAVE"])).unwrap();
        let got = read_len(&mut conn, FAILED.len(), Duration::from_secs(30));
        assert_eq!(
            got,
            FAILED,
            "{model}: resp3: {:?}",
            String::from_utf8_lossy(&got)
        );
        let mut off = encode(&["CLIENT", "REPLY", "OFF"]);
        off.extend(encode(&["SHUTDOWN"]));
        off.extend(encode(&["CLIENT", "REPLY", "ON"]));
        off.extend(encode(&["PING"]));
        conn.write_all(&off).unwrap();
        // The error was not sent; the `+OK` of REPLY ON and the PONG were.
        let got = read_len(&mut conn, 12, Duration::from_secs(30));
        assert_eq!(got, b"+OK\r\n+PONG\r\n", "{model}: reply off: {got:?}");
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "{model}: exited"
        );
        // NOSAVE writes nothing, so nothing fails.
        let mut conn = server.raw();
        conn.write_all(&encode(&["SHUTDOWN", "NOSAVE"])).unwrap();
        let (reply, closed) = until_closed(&mut conn, Duration::from_secs(30));
        assert_eq!(reply, b"", "{model}: NOSAVE");
        assert!(closed);
        assert!(server.exited(Duration::from_secs(30)).unwrap().success());

        // FORCE: the save fails and the server goes anyway, with status 1.
        let mut server = Srv::start(model, &[], &[("KIVIDB_TEST_FAILPOINTS", "final-save")]);
        let mut conn = server.raw();
        conn.write_all(&encode(&["SHUTDOWN", "FORCE"])).unwrap();
        let (reply, closed) = until_closed(&mut conn, Duration::from_secs(30));
        assert_eq!(reply, b"", "{model}: FORCE");
        assert!(closed);
        let status = server.exited(Duration::from_secs(30)).unwrap();
        assert_eq!(
            status.code(),
            Some(1),
            "{model}: FORCE exited with {status}"
        );
    }
}

/// A SIGTERM whose final save fails ends the process with status 1, as it
/// always did; with the save good, it ends with 0.
#[test]
fn sigterm_ends_the_process_after_its_final_save() {
    for model in MODELS {
        for (fails, code) in [(false, 0), (true, 1)] {
            let env: &[(&str, &str)] = if fails {
                &[("KIVIDB_TEST_FAILPOINTS", "final-save")]
            } else {
                &[]
            };
            let mut server = Srv::start(model, &[], env);
            unsafe { libc::kill(server.child.id() as libc::pid_t, libc::SIGTERM) };
            let status = server
                .exited(Duration::from_secs(30))
                .unwrap_or_else(|| panic!("{model}: no exit after SIGTERM"));
            assert_eq!(status.code(), Some(code), "{model} fails={fails}: {status}");
        }
    }
}

/// `SHUTDOWN` waits for a replica that lags, up to `shutdown-timeout`, and
/// `SHUTDOWN ABORT` from another client ends the wait: the one that asked is
/// answered an error, the abort is answered OK, and the server runs on, its
/// clients no longer paused and its replica fed again. (Redis 8.10.2:
/// `-ERR Errors trying to SHUTDOWN. Check logs.` to the first, `+OK` to the
/// second.)
#[test]
fn shutdown_abort_ends_the_wait_for_a_lagging_replica() {
    // NOSAVE waits for the replicas too, as in Redis.
    for (model, form) in MODELS
        .iter()
        .flat_map(|model| [(*model, "SAVE"), (*model, "NOSAVE")])
    {
        let mut master = Srv::start(model, &["--shutdown-timeout", "60"], &[]);
        let mut replica2 = Srv::start(
            model,
            &["--replicaof", "127.0.0.1", &master.port.to_string()],
            &[],
        );
        let mut conn = master.raw();
        let start = Instant::now();
        loop {
            conn.write_all(&encode(&["INFO", "replication"])).unwrap();
            let (info, _) = until_closed(&mut conn, Duration::from_millis(300));
            if String::from_utf8_lossy(&info).contains("connected_slaves:1") {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "the replica did not attach"
            );
        }
        unsafe { libc::kill(replica2.child.id() as libc::pid_t, libc::SIGSTOP) };
        conn.write_all(&encode(&["SET", "lag", "1"])).unwrap();
        assert_eq!(read_len(&mut conn, 5, Duration::from_secs(10)), b"+OK\r\n");
        let mut asker = master.raw();
        asker.write_all(&encode(&["SHUTDOWN", form])).unwrap();
        // Waiting: no reply, no exit.
        let (reply, closed) = until_closed(&mut asker, Duration::from_secs(2));
        assert_eq!(
            (reply.as_slice(), closed),
            (&b""[..], false),
            "{model}: still waiting"
        );
        assert!(master.child.try_wait().unwrap().is_none());
        // Aborted by another client.
        let mut other = master.raw();
        other.write_all(&encode(&["SHUTDOWN", "ABORT"])).unwrap();
        assert_eq!(
            read_len(&mut other, 5, Duration::from_secs(10)),
            b"+OK\r\n",
            "{model}"
        );
        let got = read_len(&mut asker, FAILED.len(), Duration::from_secs(30));
        assert_eq!(
            got,
            FAILED,
            "{model}: the asker got {:?}",
            String::from_utf8_lossy(&got)
        );
        // Nothing is paused now, and nothing is in progress.
        other.write_all(&encode(&["SET", "after", "1"])).unwrap();
        assert_eq!(
            read_len(&mut other, 5, Duration::from_secs(10)),
            b"+OK\r\n",
            "{model}"
        );
        other.write_all(&encode(&["SHUTDOWN", "ABORT"])).unwrap();
        let expected = b"-ERR No shutdown in progress.\r\n";
        assert_eq!(
            read_len(&mut other, expected.len(), Duration::from_secs(10)),
            expected
        );
        unsafe { libc::kill(replica2.child.id() as libc::pid_t, libc::SIGCONT) };
        let _ = replica2.child.kill();
        // And a shutdown can be asked for again, and goes ahead.
        let mut asker = master.raw();
        asker.write_all(&encode(&["SHUTDOWN", "NOW"])).unwrap();
        let (reply, closed) = until_closed(&mut asker, Duration::from_secs(30));
        assert_eq!(
            (reply.as_slice(), closed),
            (&b""[..], true),
            "{model}: again"
        );
        assert!(master.exited(Duration::from_secs(30)).unwrap().success());
    }
}

/// `SHUTDOWN NOSAVE` writes no snapshot, so the AOF is all that carries the
/// writes: what was queued for it is in the file, and synced, before the
/// connection is closed and the process exits, even when replies are sent
/// ahead of their records (`aof-ack-after-write no`).
#[test]
fn shutdown_nosave_leaves_every_queued_write_in_the_aof() {
    for model in MODELS {
        let dir = tempfile::tempdir().unwrap().keep();
        let (mut server, port, _) =
            start_with(&dir, free_port(), &["--io-model", model, "--aof"], &[], &[]);
        let mut conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
        conn.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        conn.write_all(&encode(&["CONFIG", "SET", "aof-ack-after-write", "no"]))
            .unwrap();
        assert_eq!(read_len(&mut conn, 5, Duration::from_secs(10)), b"+OK\r\n");
        const KEYS: usize = 20_000;
        let mut batch = Vec::new();
        for i in 0..KEYS {
            batch.extend(encode(&["SET", &format!("k{i}"), "v"]));
        }
        batch.extend(encode(&["SHUTDOWN", "NOSAVE"]));
        conn.write_all(&batch).unwrap();
        let (replies, closed) = until_closed(&mut conn, Duration::from_secs(60));
        assert!(closed, "{model}: not closed");
        // Every SET was answered before the close.
        assert_eq!(replies.len(), KEYS * 5, "{model}: replies before the close");
        assert!(wait_for_exit(&mut server, Duration::from_secs(30)));
        let (mut again, _, mut c) =
            start_with(&dir, free_port(), &["--io-model", model, "--aof"], &[], &[]);
        assert_eq!(
            resp_cmd(&mut c, &["DBSIZE"]).unwrap(),
            format!(":{KEYS}"),
            "{model}: keys after the restart"
        );
        let _ = again.kill();
        let _ = again.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
