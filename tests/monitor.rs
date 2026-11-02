// tests/monitor.rs
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn kividb_bin() -> String {
    std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| "target/debug/kividb".into())
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

fn connect_with_retry(port: u16) -> TcpStream {
    (0..40)
        .find_map(|_| {
            thread::sleep(Duration::from_millis(50));
            TcpStream::connect(format!("127.0.0.1:{port}")).ok()
        })
        .unwrap_or_else(|| panic!("could not connect to test server on port {port}"))
}

fn wait_for_server(port: u16) -> bool {
    for _ in 0..50 {
        if let Ok(mut s) = TcpStream::connect(format!("127.0.0.1:{port}")) {
            send_cmd(&mut s, &["PING"]);
            if read_line(&mut s) == "+PONG" {
                return true;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn spawn_server(port: u16) -> std::process::Child {
    std::process::Command::new(kividb_bin())
        .args(["--port", &port.to_string(), "--loglevel", "error"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap()
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

#[test]
fn monitor_receives_commands_from_other_clients() {
    let port = free_port();
    let mut server = spawn_server(port);

    assert!(wait_for_server(port), "server did not start");

    let mut monitor_conn = connect_with_retry(port);
    monitor_conn
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    send_cmd(&mut monitor_conn, &["MONITOR"]);

    let ok = read_line(&mut monitor_conn);
    assert_eq!(ok, "+OK", "MONITOR should return +OK, got: {ok}");

    let mut cmd_conn = connect_with_retry(port);
    thread::sleep(Duration::from_millis(50));
    send_cmd(&mut cmd_conn, &["SET", "monitor_test_key", "hello"]);
    read_line(&mut cmd_conn); // discard SET response

    let monitor_line = read_line(&mut monitor_conn);
    assert!(
        monitor_line.contains("SET"),
        "monitor line should contain SET: {monitor_line:?}"
    );
    assert!(
        monitor_line.contains("monitor_test_key"),
        "monitor line should contain the key: {monitor_line:?}"
    );

    server.kill().unwrap();
    let _ = server.wait();
}

#[test]
fn monitor_line_format_has_timestamp_and_db() {
    let port = free_port();
    let mut server = spawn_server(port);

    assert!(wait_for_server(port), "server did not start");

    let mut monitor_conn = connect_with_retry(port);
    monitor_conn
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    send_cmd(&mut monitor_conn, &["MONITOR"]);

    let ok = read_line(&mut monitor_conn);
    assert_eq!(ok, "+OK", "got: {ok}");

    let mut cmd_conn = connect_with_retry(port);
    thread::sleep(Duration::from_millis(50));
    send_cmd(&mut cmd_conn, &["PING"]);
    read_line(&mut cmd_conn);

    let line = read_line(&mut monitor_conn);
    assert!(line.starts_with('+'), "line should start with +: {line:?}");
    assert!(line.contains('['), "line should contain db/addr: {line:?}");
    assert!(
        line.contains('"'),
        "line should quote command tokens: {line:?}"
    );

    server.kill().unwrap();
    let _ = server.wait();
}
