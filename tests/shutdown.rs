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
    let port = 16399u16;
    let mut server = Command::new(kividb_bin())
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

/// A write acknowledged before a `SHUTDOWN` (or SIGTERM, which takes the
/// same path) is in the final snapshot. The connections already open kept
/// running while the snapshot was written, so a write made after its cut
/// was acknowledged, and then lost with the process.
#[test]
fn shutdown_keeps_every_acknowledged_write() {
    use std::io::{BufRead, BufReader};
    let port = 16401u16;
    let dir = std::env::temp_dir().join(format!("kividb-shutdown-acks-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let start = || {
        Command::new(kividb_bin())
            .args(["--port", &port.to_string(), "--dir", dir.to_str().unwrap()])
            .spawn()
            .expect("failed to start kividb")
    };
    let cmd = |conn: &mut BufReader<TcpStream>, args: &[&str]| -> std::io::Result<String> {
        let mut req = format!("*{}\r\n", args.len());
        for a in args {
            req.push_str(&format!("${}\r\n{a}\r\n", a.len()));
        }
        conn.get_mut().write_all(req.as_bytes())?;
        let mut line = String::new();
        if conn.read_line(&mut line)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        if let Some(len) = line.trim_end().strip_prefix('$')
            && len != "-1"
        {
            let mut body = vec![0u8; len.parse::<usize>().unwrap() + 2];
            std::io::Read::read_exact(conn, &mut body)?;
            return Ok(String::from_utf8_lossy(&body[..body.len() - 2]).into_owned());
        }
        Ok(line.trim_end().to_string())
    };
    let connect = || {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        BufReader::new(s)
    };

    let mut server = start();
    assert!(wait_for_server(port, Duration::from_secs(5)));
    // Enough data that the final snapshot takes a while: the writers have
    // time to go on while it is written.
    assert_eq!(
        cmd(
            &mut connect(),
            &["DEBUG", "POPULATE", "300000", "fill", "64"]
        )
        .unwrap(),
        "+OK"
    );

    let writers: Vec<_> = (0..6)
        .map(|_| {
            let mut conn = connect();
            thread::spawn(move || {
                let mut acked = 0u64;
                while let Ok(reply) = cmd(&mut conn, &["INCR", "counter"]) {
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
    let mut killer = connect();
    let _ = cmd(&mut killer, &["SHUTDOWN"]);
    assert!(
        wait_for_exit(&mut server, Duration::from_secs(30)),
        "no exit after SHUTDOWN"
    );
    let acked: u64 = writers.into_iter().map(|w| w.join().unwrap()).sum();
    assert!(acked > 0, "no write was acknowledged");

    let mut server = start();
    assert!(wait_for_server(port, Duration::from_secs(30)));
    let mut conn = connect();
    let mut value = String::new();
    for _ in 0..100 {
        match cmd(&mut conn, &["GET", "counter"]) {
            Ok(v) if !v.starts_with('-') => {
                value = v;
                break;
            }
            _ => thread::sleep(Duration::from_millis(100)),
        }
    }
    let kept: u64 = value.parse().unwrap_or(0);
    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        kept >= acked,
        "{acked} INCRs were acknowledged but the snapshot holds {kept}"
    );
}

#[test]
fn shutdown_abort_keeps_server_alive() {
    let port = 16400u16;
    let mut server = Command::new(kividb_bin())
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
