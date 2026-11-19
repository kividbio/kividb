#![allow(clippy::disallowed_methods)]
// tests/monitor_count.rs
//
// A monitor ended by `RESET` or by `CLIENT KILL` is no longer counted in
// `MONITOR_COUNT`, the one load every command makes: back at 0, a command
// formats and stamps nothing. The count is the process's, so this is a file
// of its own, with one server in the process.
use kividb::monitor::MONITOR_COUNT;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

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

/// A free port, not one the system hands out to outgoing connections.
fn free_port() -> u16 {
    use std::sync::atomic::AtomicU16;
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

fn start() -> u16 {
    let port = free_port();
    let dir = tempfile::tempdir().unwrap().keep();
    let config = kividb::config::Config {
        bind: "127.0.0.1".to_string(),
        port,
        threads: 2,
        aof: false,
        databases: 16,
        loglevel: "error".to_string(),
        io_model: kividb::config::IoModel::Tokio,
        configfile: "./kividb.conf".to_string(),
        dir: dir.to_string_lossy().into_owned(),
        ..Default::default()
    };
    thread::spawn(move || kividb::run_for_test(config));
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = c.write_all(b"PING\r\n");
            let mut b = [0u8; 7];
            if c.read_exact(&mut b).is_ok() && &b == b"+PONG\r\n" {
                return port;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server on {port} did not start");
}

fn conn(port: u16) -> TcpStream {
    let c = TcpStream::connect(("127.0.0.1", port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c
}

fn send_cmd(conn: &mut TcpStream, args: &[&str]) {
    let mut buf = format!("*{}\r\n", args.len());
    for a in args {
        buf.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    conn.write_all(buf.as_bytes()).unwrap();
}

fn read_line(conn: &mut TcpStream) -> String {
    let mut out = String::new();
    let mut b = [0u8; 1];
    loop {
        conn.read_exact(&mut b).unwrap();
        if b[0] == b'\n' {
            break;
        }
        if b[0] != b'\r' {
            out.push(b[0] as char);
        }
    }
    out
}

fn wait_for_count(want: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while MONITOR_COUNT.load(Ordering::Relaxed) != want {
        assert!(
            Instant::now() < deadline,
            "MONITOR_COUNT is {}, not {want}",
            MONITOR_COUNT.load(Ordering::Relaxed)
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn reset_and_client_kill_end_the_count_of_monitors() {
    let port = start();
    let mut admin = conn(port);
    assert_eq!(MONITOR_COUNT.load(Ordering::Relaxed), 0);

    // RESET.
    let mut monitor = conn(port);
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");
    wait_for_count(1);
    // A second MONITOR on the same connection is not a second monitor.
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");
    assert_eq!(MONITOR_COUNT.load(Ordering::Relaxed), 1);
    send_cmd(&mut admin, &["SET", "k", "v"]);
    assert_eq!(read_line(&mut admin), "+OK");
    while !read_line(&mut monitor).ends_with("\"SET\" \"k\" \"v\"") {}
    send_cmd(&mut monitor, &["RESET"]);
    assert_eq!(read_line(&mut monitor), "+RESET");
    wait_for_count(0);

    // CLIENT KILL.
    send_cmd(&mut monitor, &["CLIENT", "ID"]);
    let id = read_line(&mut monitor)[1..].to_string();
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");
    wait_for_count(1);
    send_cmd(&mut admin, &["CLIENT", "KILL", "ID", &id]);
    assert_eq!(read_line(&mut admin), ":1");
    wait_for_count(0);

    // A monitor whose connection just goes.
    let mut monitor = conn(port);
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");
    wait_for_count(1);
    drop(monitor);
    wait_for_count(0);
}
