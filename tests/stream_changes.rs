// Which XREADGROUPs count as a change of the dataset
// (`rdb_changes_since_last_save`), as in Redis (#27). The count is the
// process's: this file has one test, so nothing else moves it.

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

    /// A number `INFO persistence` reports.
    fn info(&mut self, field: &str) -> u64 {
        let V::Bulk(info) = self.cmd(&["INFO", "persistence"]) else {
            panic!("INFO is a bulk string");
        };
        let prefix = format!("{field}:");
        String::from_utf8(info)
            .unwrap()
            .lines()
            .find_map(|line| {
                line.strip_prefix(&prefix)
                    .map(|v| v.trim().parse().unwrap())
            })
            .unwrap_or_else(|| panic!("{field} in INFO persistence"))
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

/// The Redis suite's "XREADGROUP from PEL does not change dirty": reading a
/// consumer's own pending entries again bumps their delivery count, which
/// replicas are sent, but Redis does not count it as a change. A new
/// consumer, and new entries delivered, are changes.
#[test]
fn xreadgroup_of_pending_entries_is_not_a_change() {
    let mut c = start_server();
    for id in ["1-0", "2-0", "3-0", "4-0"] {
        assert!(matches!(c.cmd(&["XADD", "x", id, "data", "a"]), V::Bulk(_)));
    }
    c.ok(&["XGROUP", "CREATE", "x", "g1", "0"]);
    assert_eq!(
        c.cmd(&["XGROUP", "CREATECONSUMER", "x", "g1", "Alice"]),
        V::Int(1)
    );

    let changes = |c: &mut Conn| c.info("rdb_changes_since_last_save");
    let before = changes(&mut c);
    let V::Array(read) = c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g1",
        "Alice",
        "COUNT",
        "2",
        "STREAMS",
        "x",
        ">",
    ]) else {
        panic!("XREADGROUP")
    };
    assert_eq!(read.len(), 1);
    assert_eq!(
        changes(&mut c),
        before + 1,
        "new entries delivered: a change"
    );

    let before = changes(&mut c);
    for from in ["0", "9000"] {
        let V::Array(_) = c.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "Alice",
            "COUNT",
            "2",
            "STREAMS",
            "x",
            from,
        ]) else {
            panic!("XREADGROUP {from}")
        };
    }
    assert_eq!(changes(&mut c), before, "pending entries read again");
    // The delivery count went up all the same.
    let V::Array(pending) = c.cmd(&["XPENDING", "x", "g1", "-", "+", "10"]) else {
        panic!("XPENDING")
    };
    let V::Array(first) = &pending[0] else {
        panic!("{pending:?}")
    };
    assert_eq!(first[0], V::Bulk(b"1-0".to_vec()));
    assert_eq!(first[3], V::Int(2), "delivered by '>', and again from 0");

    let before = changes(&mut c);
    c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g1",
        "noconsumer",
        "COUNT",
        "2",
        "STREAMS",
        "x",
        "0",
    ]);
    assert_eq!(changes(&mut c), before + 1, "a consumer created");

    // Nothing new to deliver.
    c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g1",
        "Alice",
        "COUNT",
        "10",
        "STREAMS",
        "x",
        ">",
    ]);
    let before = changes(&mut c);
    c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g1",
        "Alice",
        "COUNT",
        "10",
        "STREAMS",
        "x",
        ">",
    ]);
    assert_eq!(changes(&mut c), before, "nothing delivered");
}
