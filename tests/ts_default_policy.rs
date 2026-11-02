// tests/ts_default_policy.rs
//
// `ts-duplicate-policy`, the duplicate policy of a series made without one.
// It is the process's, so this file has one test of its own: the other
// time series tests would see it change under them.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

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
    let from = (std::process::id() as u16).wrapping_mul(41);
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

    /// The first line of the reply, and a bulk string's text after it.
    fn cmd(&mut self, args: &[&str]) -> String {
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
        self.line()
    }

    fn line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        match line.strip_prefix('$') {
            Some(len) if len != "-1" => {
                let mut text = vec![0u8; len.parse::<usize>().unwrap() + 2];
                self.reader.read_exact(&mut text).unwrap();
                text.truncate(text.len() - 2);
                String::from_utf8(text).unwrap()
            }
            _ => line,
        }
    }

    /// `CONFIG GET name`'s value.
    fn config(&mut self, name: &str) -> String {
        assert_eq!(self.cmd(&["CONFIG", "GET", name]), "*2");
        self.line();
        self.line()
    }
}

/// The duplicate policy of a series made without one is `ts-duplicate-policy`:
/// `last` unless set, so that an upgraded server keeps doing what it did, and
/// `block` (RedisTimeSeries's default) when asked for. A series keeps the
/// policy it was made with (#127).
#[test]
fn ts_duplicate_policy_is_the_default_of_new_series() {
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
    assert_eq!(c.config("ts-duplicate-policy"), "last");
    assert_eq!(c.cmd(&["TS.ADD", "before", "1000", "1"]), ":1000");
    assert_eq!(c.cmd(&["TS.ADD", "before", "1000", "2"]), ":1000");

    assert_eq!(
        c.cmd(&["CONFIG", "SET", "ts-duplicate-policy", "BLOCK"]),
        "+OK"
    );
    assert_eq!(c.config("ts-duplicate-policy"), "block");
    // Made from now on, by TS.ADD or TS.CREATE: BLOCK.
    assert_eq!(c.cmd(&["TS.ADD", "after", "1000", "1"]), ":1000");
    assert!(
        c.cmd(&["TS.ADD", "after", "1000", "2"])
            .starts_with("-TSDB")
    );
    assert_eq!(c.cmd(&["TS.CREATE", "created"]), "+OK");
    assert_eq!(c.cmd(&["TS.ADD", "created", "5", "1"]), ":5");
    assert!(c.cmd(&["TS.ADD", "created", "5", "2"]).starts_with("-TSDB"));
    // Made before: as it was.
    assert_eq!(c.cmd(&["TS.ADD", "before", "1000", "3"]), ":1000");
    // An explicit policy still wins.
    assert_eq!(
        c.cmd(&["TS.CREATE", "summed", "DUPLICATE_POLICY", "SUM"]),
        "+OK"
    );
    c.cmd(&["TS.ADD", "summed", "7", "1"]);
    assert_eq!(c.cmd(&["TS.ADD", "summed", "7", "2"]), ":7");

    assert!(
        c.cmd(&["CONFIG", "SET", "ts-duplicate-policy", "sometimes"])
            .starts_with("-ERR")
    );
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "ts-duplicate-policy", "last"]),
        "+OK"
    );
}
