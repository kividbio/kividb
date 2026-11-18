#![allow(clippy::disallowed_methods)]
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
    spawn_server_with(port, &[])
}

fn spawn_server_with(port: u16, extra: &[&str]) -> std::process::Child {
    // A directory of its own: one engine to a data directory.
    let dir = tempfile::tempdir().unwrap().keep();
    std::process::Command::new(kividb_bin())
        .current_dir(dir)
        .args(["--port", &port.to_string(), "--loglevel", "error"])
        .args(extra)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap()
}

fn resp(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len());
    for a in args {
        buf.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    buf.into_bytes()
}

/// `conns` connections each send `INCRBY c<i> <seq>` for `seq` from 0 to
/// `per - 1`, pipelined, at once. Returns when all have their replies.
fn send_numbered(port: u16, conns: usize, per: usize) {
    let senders: Vec<_> = (0..conns)
        .map(|i| {
            thread::spawn(move || {
                let mut conn = connect_with_retry(port);
                conn.set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                let key = format!("c{i}");
                let mut seq = 0;
                let mut buf = vec![0u8; 1 << 16];
                while seq < per {
                    let n = (per - seq).min(200);
                    let mut batch = Vec::new();
                    for q in seq..seq + n {
                        batch.extend_from_slice(&resp(&["INCRBY", &key, &q.to_string()]));
                    }
                    conn.write_all(&batch).unwrap();
                    let mut lines = 0;
                    while lines < n {
                        let got = conn.read(&mut buf).unwrap();
                        assert!(got > 0, "connection {i} closed");
                        lines += buf[..got].iter().filter(|&&b| b == b'\n').count();
                    }
                    seq += n;
                }
            })
        })
        .collect();
    for s in senders {
        s.join().unwrap();
    }
}

/// What a monitor was shown of [`send_numbered`]'s commands: for each
/// connection the numbers in the order shown, and whether the time stamps
/// ever went back.
fn numbered_lines(shown: &[u8], conns: usize) -> (Vec<Vec<usize>>, bool) {
    let text = String::from_utf8_lossy(shown);
    let mut seqs = vec![Vec::new(); conns];
    let mut last_stamp = (0u64, 0u64);
    let mut back = false;
    for line in text.split("\r\n") {
        let Some(rest) = line.strip_prefix('+') else {
            continue;
        };
        let Some((stamp, rest)) = rest.split_once(' ') else {
            continue;
        };
        let Some((args_at, _)) = rest.match_indices("] ").next() else {
            continue;
        };
        let args: Vec<&str> = rest[args_at + 2..].split(' ').collect();
        if args.len() != 3 || args[0] != "\"INCRBY\"" {
            continue;
        }
        let (s, us) = stamp.split_once('.').unwrap();
        let stamp = (s.parse().unwrap(), us.parse().unwrap());
        back |= stamp < last_stamp;
        last_stamp = last_stamp.max(stamp);
        let conn: usize = args[1].trim_matches('"')[1..].parse().unwrap();
        let seq: usize = args[2].trim_matches('"').parse().unwrap();
        seqs[conn].push(seq);
    }
    (seqs, back)
}

/// Read from `conn` until `lines` lines have come or it is closed; how
/// much was read, and whether it was closed.
fn read_lines(conn: &mut TcpStream, lines: usize, quiet: Duration) -> (Vec<u8>, bool) {
    conn.set_read_timeout(Some(quiet)).unwrap();
    let mut shown = Vec::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut seen = 0;
    while seen < lines {
        match conn.read(&mut buf) {
            Ok(0) => return (shown, true),
            Ok(n) => {
                seen += buf[..n].iter().filter(|&&b| b == b'\n').count();
                shown.extend_from_slice(&buf[..n]);
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return (shown, true),
            Err(_) => break,
        }
    }
    (shown, false)
}

fn info_field(conn: &mut TcpStream, field: &str) -> String {
    send_cmd(conn, &["INFO", "stats"]);
    let header = read_line(conn);
    let len: usize = header[1..].parse().unwrap();
    let mut body = vec![0u8; len + 2];
    conn.read_exact(&mut body).unwrap();
    let body = String::from_utf8(body).unwrap();
    body.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")).map(str::to_string))
        .unwrap_or_default()
}

/// Every command of four connections sending at full speed, over four
/// worker threads, is shown exactly once and in each connection's order,
/// with time stamps that never go back. A monitor used to be fed through a
/// broadcast ring of 4096 lines, and lost what it had not read in time.
fn monitor_shows_every_command_once_in_order(io_model: &str) {
    const CONNS: usize = 4;
    const PER: usize = 25_000;
    let port = free_port();
    let mut server = spawn_server_with(port, &["--io-model", io_model, "--threads", "4"]);
    assert!(wait_for_server(port), "server did not start");

    let mut monitor = connect_with_retry(port);
    monitor
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");

    let reader =
        thread::spawn(move || read_lines(&mut monitor, CONNS * PER, Duration::from_secs(10)));
    send_numbered(port, CONNS, PER);
    let (shown, closed) = reader.join().unwrap();
    assert!(!closed, "the monitor was disconnected");
    let (seqs, back) = numbered_lines(&shown, CONNS);
    for (conn, seq) in seqs.iter().enumerate() {
        assert_eq!(
            seq.len(),
            PER,
            "connection {conn}: {} of {PER} shown",
            seq.len()
        );
        assert!(
            seq.iter().enumerate().all(|(i, &q)| i == q),
            "connection {conn}: shown out of order"
        );
    }
    assert!(!back, "a time stamp went back");

    server.kill().unwrap();
    let _ = server.wait();
}

#[test]
fn monitor_shows_every_command_once_in_order_tokio() {
    monitor_shows_every_command_once_in_order("tokio");
}

#[cfg(target_os = "linux")]
#[test]
fn monitor_shows_every_command_once_in_order_uring() {
    monitor_shows_every_command_once_in_order("uring");
}

/// A monitor that reads nothing while 200,000 commands run is shown all of
/// them when it reads at last (no output limit for its class, `normal`, by
/// default, as in Redis); with `client-output-buffer-limit normal 1mb 0 0`
/// it is disconnected once that much waits for it, and counted.
fn a_monitor_that_falls_behind_waits_or_is_closed_at_its_limit(io_model: &str) {
    const CONNS: usize = 4;
    const PER: usize = 50_000;
    let port = free_port();
    let mut server = spawn_server_with(port, &["--io-model", io_model, "--threads", "4"]);
    assert!(wait_for_server(port), "server did not start");
    let mut admin = connect_with_retry(port);
    admin
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // No limit: everything waits for it.
    let mut monitor = connect_with_retry(port);
    monitor
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");
    send_numbered(port, CONNS, PER);
    let (shown, closed) = read_lines(&mut monitor, CONNS * PER, Duration::from_secs(10));
    assert!(!closed, "the monitor was disconnected without a limit");
    let (seqs, _) = numbered_lines(&shown, CONNS);
    for (conn, seq) in seqs.iter().enumerate() {
        assert_eq!(
            seq.len(),
            PER,
            "connection {conn}: {} of {PER} shown",
            seq.len()
        );
    }
    drop(monitor);

    // A limit: it is disconnected at it, not left behind without a word.
    send_cmd(
        &mut admin,
        &[
            "CONFIG",
            "SET",
            "client-output-buffer-limit",
            "normal 1mb 0 0",
        ],
    );
    assert_eq!(read_line(&mut admin), "+OK");
    let mut monitor = connect_with_retry(port);
    monitor
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");
    send_numbered(port, CONNS, PER);
    let (_, closed) = read_lines(&mut monitor, usize::MAX, Duration::from_secs(5));
    assert!(closed, "a monitor over its limit was not disconnected");
    assert_eq!(
        info_field(&mut admin, "client_output_buffer_limit_disconnections"),
        "1"
    );

    server.kill().unwrap();
    let _ = server.wait();
}

#[test]
fn a_monitor_that_falls_behind_waits_or_is_closed_at_its_limit_tokio() {
    a_monitor_that_falls_behind_waits_or_is_closed_at_its_limit("tokio");
}

#[cfg(target_os = "linux")]
#[test]
fn a_monitor_that_falls_behind_waits_or_is_closed_at_its_limit_uring() {
    a_monitor_that_falls_behind_waits_or_is_closed_at_its_limit("uring");
}

/// The line of a command with arguments to be quoted, as Redis 8.10 shows
/// it (time stamp and address aside): `\n`, `\r`, `\t`, `\a` and `\b` by
/// name, other bytes not printable as `\xHH`, quotes and backslashes
/// escaped.
#[test]
fn monitor_quotes_arguments_as_redis_does() {
    let port = free_port();
    let mut server = spawn_server(port);
    assert!(wait_for_server(port), "server did not start");
    let mut monitor = connect_with_retry(port);
    monitor
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    send_cmd(&mut monitor, &["MONITOR"]);
    assert_eq!(read_line(&mut monitor), "+OK");
    let mut conn = connect_with_retry(port);
    conn.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    send_cmd(&mut conn, &["SET", "k \"q\"", "a\\b\n\r\t\x07\x08\x01~"]);
    assert_eq!(read_line(&mut conn), "+OK");
    let line = read_line(&mut monitor);
    let (_, shown) = line.split_once("] ").unwrap();
    assert_eq!(shown, r#""SET" "k \"q\"" "a\\b\n\r\t\a\b\x01~""#, "{line}");
    server.kill().unwrap();
    let _ = server.wait();
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
