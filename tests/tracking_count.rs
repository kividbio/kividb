// tests/tracking_count.rs
//
// INFO clients' `tracking_clients`. The count is the process's, so the one
// test here has a process, and a server, of its own.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

/// Whether nothing has `port` (see tests/generics.rs).
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

/// A port nothing listens on, outside the ephemeral range (see
/// tests/generics.rs).
fn free_port() -> u16 {
    let from = (std::process::id() as u16).wrapping_mul(31);
    (0..)
        .map(|step: u16| 10_000 + from.wrapping_add(step) % 20_000)
        .find(|&port| port_is_free(port))
        .unwrap()
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        for _ in 0..100 {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                return Conn {
                    reader: BufReader::new(stream.try_clone().unwrap()),
                    writer: stream,
                };
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("could not connect to port {port}");
    }

    /// A command's reply: a simple string, an error or an integer as its
    /// line, a bulk string as its text.
    fn cmd(&mut self, args: &[&str]) -> String {
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        match line.strip_prefix('$') {
            Some("-1") => String::new(),
            Some(len) => {
                let mut text = vec![0u8; len.parse::<usize>().unwrap() + 2];
                std::io::Read::read_exact(&mut self.reader, &mut text).unwrap();
                text.truncate(text.len() - 2);
                String::from_utf8(text).unwrap()
            }
            None => line,
        }
    }

    fn tracking_clients(&mut self) -> u64 {
        let info = self.cmd(&["INFO", "clients"]);
        info.lines()
            .find_map(|line| line.strip_prefix("tracking_clients:"))
            .unwrap_or_else(|| panic!("no tracking_clients in {info}"))
            .parse()
            .unwrap()
    }
}

/// A client that turns tracking on and then disconnects stops being counted,
/// once: the registry takes it off as it lets the client go. (Reported as
/// never happening in #173, from a check that never closed its connection;
/// taking it off a second time, as a fix for that would have, makes the
/// count wrong the other way.)
#[test]
fn a_tracking_client_that_leaves_is_no_longer_counted() {
    let port = free_port();
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
    let mut c = Conn::connect(port);
    assert_eq!(c.tracking_clients(), 0);

    let mut leaving = Conn::connect(port);
    assert_eq!(leaving.cmd(&["CLIENT", "TRACKING", "ON"]), "+OK");
    let mut staying = Conn::connect(port);
    assert_eq!(staying.cmd(&["CLIENT", "TRACKING", "ON", "BCAST"]), "+OK");
    assert_eq!(c.tracking_clients(), 2);

    drop(leaving);
    let mut count = c.tracking_clients();
    for _ in 0..100 {
        if count == 1 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
        count = c.tracking_clients();
    }
    assert_eq!(count, 1, "the client that left is still counted");

    // Turning it off, then leaving, does not take it off twice.
    assert_eq!(staying.cmd(&["CLIENT", "TRACKING", "OFF"]), "+OK");
    assert_eq!(c.tracking_clients(), 0);
    drop(staying);
    thread::sleep(Duration::from_millis(200));
    assert_eq!(c.tracking_clients(), 0);
}
