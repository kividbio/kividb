// tests/stats.rs
//
// INFO stats, counted. Its counters are the process's, so the one test here
// has a process, and a server, of its own: what it reads is its commands'.

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

    fn stat(&mut self, name: &str) -> u64 {
        let info = self.cmd(&["INFO", "stats"]);
        info.lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("no {name} in {info}"))
            .parse()
            .unwrap()
    }
}

/// `total_commands_processed`, `keyspace_hits` and `keyspace_misses` were
/// kept in batches of 128 per thread, and `total_reads_processed` and
/// `total_writes_processed` not at all: a server that had run a few commands
/// reported 0 for each (#136).
#[test]
fn info_stats_counts_the_first_commands() {
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
    assert_eq!(c.cmd(&["SET", "a", "1"]), "+OK");
    assert_eq!(c.cmd(&["GET", "a"]), "1");
    assert_eq!(c.cmd(&["GET", "a"]), "1");
    assert_eq!(c.cmd(&["GET", "nope"]), "");
    assert_eq!(c.cmd(&["INCR", "n"]), ":1");

    // The INFO too: a command is counted as it starts.
    assert_eq!(c.stat("total_commands_processed"), 6);
    assert_eq!(c.stat("keyspace_hits"), 2);
    assert_eq!(c.stat("keyspace_misses"), 1);
    assert!(c.stat("total_reads_processed") >= 8);
    assert!(c.stat("total_writes_processed") >= 8);

    assert_eq!(c.cmd(&["CONFIG", "RESETSTAT"]), "+OK");
    // The INFO that asks.
    assert_eq!(c.stat("total_commands_processed"), 1);
    assert_eq!(c.stat("keyspace_hits"), 0);
    assert!(c.stat("total_reads_processed") <= 3);
}
