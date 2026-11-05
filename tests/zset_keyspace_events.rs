// Keyspace events of ZREMRANGEBYSCORE. The flags are the process's, so this
// file has one test: nothing else changes them.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<V>),
    Nil,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..40)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        self.stream.write_all(&buf).unwrap();
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => V::Simple(line[1..].to_string()),
            Some(b'-') => V::Error(line[1..].to_string()),
            Some(b':') => V::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count < 0 {
                    return V::Nil;
                }
                V::Array((0..count).map(|_| self.read()).collect())
            }
            _ => panic!("unexpected RESP: {line:?}"),
        }
    }

    fn ok(&mut self, args: &[&str]) {
        assert_eq!(self.cmd(args), V::Simple("OK".into()), "{args:?}");
    }
}

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
        let from = (std::process::id() as u16).wrapping_mul(37);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

fn start_server() -> Conn {
    let port = free_port();
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            databases: 16,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    Conn::connect(port)
}

/// The next `pmessage`, as `event key` for keyevents.
fn event(sub: &mut Conn) -> String {
    match sub.read() {
        V::Array(parts) => match &parts[..] {
            [_, _, V::Bulk(channel), V::Bulk(message)] => format!(
                "{} {}",
                String::from_utf8_lossy(channel).rsplit(':').next().unwrap(),
                String::from_utf8_lossy(message)
            ),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn zremrangebyscore_announces_what_it_removed_and_the_emptied_key() {
    let mut c = start_server();
    let port = c.stream.peer_addr().unwrap().port();
    let mut sub = Conn::connect(port);
    let V::Array(reply) = sub.cmd(&["PSUBSCRIBE", "__keyevent@0__:*"]) else {
        panic!("PSUBSCRIBE")
    };
    assert_eq!(reply[0], V::Bulk(b"psubscribe".to_vec()));
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "Ezg"]);
    c.cmd(&["ZADD", "z", "1", "a", "2", "b"]);
    assert_eq!(event(&mut sub), "zadd z");

    assert_eq!(c.cmd(&["ZREMRANGEBYSCORE", "z", "5", "9"]), V::Int(0));
    assert_eq!(c.cmd(&["ZREMRANGEBYSCORE", "z", "1", "1"]), V::Int(1));
    assert_eq!(
        event(&mut sub),
        "zremrangebyscore z",
        "nothing when nothing went"
    );
    assert_eq!(c.cmd(&["ZREMRANGEBYSCORE", "z", "-inf", "+inf"]), V::Int(1));
    assert_eq!(event(&mut sub), "zremrangebyscore z");
    assert_eq!(event(&mut sub), "del z");
}
