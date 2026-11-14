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
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
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
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
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

#[test]
fn shutdown_abort_keeps_server_alive() {
    let port = free_port();
    let mut server = Command::new(kividb_bin())
        .current_dir(tempfile::tempdir().unwrap().keep())
        .args(["--port", &port.to_string()])
        .spawn()
        .unwrap();

    assert!(
        wait_for_server(port, Duration::from_secs(5)),
        "server on port {port} did not come up within 5s"
    );

    let mut conn = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    use std::io::Read;

    conn.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$5\r\nABORT\r\n")
        .unwrap();
    let mut buf = [0u8; 16];
    let n = conn.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n", "expected +OK from SHUTDOWN ABORT");

    conn.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let n = conn.read(&mut buf).unwrap();
    assert_eq!(
        &buf[..n],
        b"+PONG\r\n",
        "server stopped responding after ABORT"
    );

    server.kill().unwrap();
    let _ = server.wait();
}
