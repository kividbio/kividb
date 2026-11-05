//! What a client sends that is made to hurt the server: counts that do not
//! fit, lengths that lie, nesting without end. Each must be answered with an
//! error, and the server must go on answering.

use kividb::config::{Config, IoModel};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

/// Whether nothing has `port` (see `tests/config.rs`).
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

/// A free port outside the range handed to outgoing connections.
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

fn start_server(requirepass: &str) -> u16 {
    let port = free_port();
    let requirepass = requirepass.to_string();
    thread::spawn(move || {
        kividb::run_for_test(Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            loglevel: "error".to_string(),
            io_model: IoModel::Tokio,
            requirepass,
            slowlog_log_slower_than: u64::MAX,
            ..Default::default()
        });
    });
    port
}

#[derive(Debug, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}

impl Reply {
    fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }
}

struct Conn(TcpStream);

impl Conn {
    fn connect(port: u16) -> Self {
        for _ in 0..50 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                return Self(s);
            }
            thread::sleep(Duration::from_millis(100));
        }
        panic!("could not connect to server on port {port}");
    }

    fn call(&mut self, args: &[&[u8]]) -> Reply {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.0.write_all(&buf).unwrap();
        self.read()
    }

    fn cmd(&mut self, args: &[&str]) -> Reply {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.call(&args)
    }

    fn line(&mut self) -> String {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            self.0.read_exact(&mut byte).unwrap();
            if byte[0] == b'\n' {
                break;
            }
            line.push(byte[0]);
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn read(&mut self) -> Reply {
        let line = self.line();
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => Reply::Simple(rest.to_string()),
            "-" => Reply::Error(rest.to_string()),
            ":" => Reply::Int(rest.parse().unwrap()),
            "$" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Reply::Bulk(None);
                }
                let mut body = vec![0u8; n as usize + 2];
                self.0.read_exact(&mut body).unwrap();
                body.truncate(n as usize);
                Reply::Bulk(Some(body))
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                Reply::Array((0..n.max(0)).map(|_| self.read()).collect())
            }
            _ => panic!("unexpected reply: {line:?}"),
        }
    }

    fn still_answers(&mut self) {
        assert_eq!(self.cmd(&["PING"]), Reply::Simple("PONG".into()));
    }
}

/// A `DUMP` payload with `data` in it, its checksum made to match.
fn payload(data: &[u8]) -> Vec<u8> {
    let mut out = data.to_vec();
    out.extend_from_slice(&[0x0a, 0x00]);
    out.extend_from_slice(&kividb::snapshot::crc64_digest(data).to_le_bytes());
    out
}

#[test]
fn counts_that_do_not_fit_are_errors() {
    let port = start_server("");
    let mut c = Conn::connect(port);
    let huge = "18446744073709551615";
    let half = "9223372036854775808";
    for args in [
        vec!["HSETEX", "h", "FIELDS", half],
        vec!["XACKDEL", "s", "g", "KEEPREF", "IDS", huge, "0-1"],
        vec!["MSETEX", half, "k", "v"],
        vec!["DEBUG", "SLEEP", "-1"],
        vec!["DEBUG", "SLEEP", "inf"],
        vec!["DEBUG", "SLEEP", "nan"],
        vec!["BITFIELD", "b", "SET", "u8", "99999999999999", "1"],
        vec!["BITFIELD", "b", "SET", "u8", "#9999999999999", "1"],
    ] {
        let reply = c.cmd(&args);
        assert!(reply.is_error(), "{args:?}: {reply:?}");
        c.still_answers();
    }
    // Looked into for its keys, which it says nothing about: no keys.
    c.cmd(&["COMMAND", "GETKEYS", "FCALL", "f", huge, "k"]);
    c.still_answers();
}

#[test]
fn a_restore_payload_that_lies_about_its_sizes_is_refused() {
    let port = start_server("");
    let mut c = Conn::connect(port);
    // A list that says it has four thousand million elements, and has none.
    let reply = c.call(&[
        b"RESTORE",
        b"l",
        b"0",
        &payload(&[2, 0xff, 0xff, 0xff, 0xff]),
    ]);
    assert!(reply.is_error(), "{reply:?}");
    c.still_answers();

    // Each four bytes of real payloads, in turn, made the largest number
    // they can hold: every count and length in them lies once.
    c.cmd(&["RPUSH", "list", "a", "bb", "ccc"]);
    c.cmd(&["HSET", "hash", "f", "v", "g", "w"]);
    c.cmd(&["XADD", "stream", "1-1", "f", "v"]);
    c.cmd(&["TS.CREATE", "series"]);
    c.cmd(&["TS.ADD", "series", "1", "2"]);
    for key in ["list", "hash", "stream", "series"] {
        let Reply::Bulk(Some(dump)) = c.cmd(&["DUMP", key]) else {
            panic!("DUMP {key}");
        };
        let data = &dump[..dump.len() - 10];
        for at in 0..data.len().saturating_sub(3) {
            let mut lying = data.to_vec();
            lying[at..at + 4].copy_from_slice(&[0xff; 4]);
            let reply = c.call(&[b"RESTORE", b"copy", b"0", &payload(&lying), b"REPLACE"]);
            assert!(
                reply.is_error() || reply == Reply::Simple("OK".into()),
                "{key} at {at}: {reply:?}"
            );
        }
        c.still_answers();
    }
}

#[test]
fn queries_and_expressions_that_nest_without_end_are_refused() {
    let port = start_server("");
    let mut c = Conn::connect(port);
    c.cmd(&["FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "t", "TEXT"]);
    for query in [
        "(".repeat(200_000),
        "-".repeat(200_000),
        vec!["hello"; 100_000].join(" "),
        vec!["hello"; 100_000].join("|"),
    ] {
        let reply = c.cmd(&["FT.SEARCH", "idx", &query]);
        assert!(reply.is_error(), "{reply:?}");
        c.still_answers();
    }
    let deep = format!("{}1{}", "(".repeat(200_000), ")".repeat(200_000));
    c.cmd(&["FT.AGGREGATE", "idx", "*", "APPLY", &deep, "AS", "x"]);
    c.still_answers();
    c.cmd(&["JSON.SET", "doc", "$", "{\"a\":1}"]);
    let path = format!("$[?{}@.a{}]", "(".repeat(200_000), ")".repeat(200_000));
    let reply = c.cmd(&["JSON.GET", "doc", &path]);
    assert!(reply.is_error(), "{reply:?}");
    c.still_answers();
}

/// Redis's limits for a client that has not authenticated: a request of at
/// most 10 arguments, each at most 16 KB.
#[test]
fn a_client_that_has_not_authenticated_may_not_send_much() {
    let port = start_server("s3cret");
    for request in [
        b"*11\r\n".to_vec(),
        b"*2\r\n$4\r\nAUTH\r\n$100000\r\n".to_vec(),
    ] {
        let mut c = Conn::connect(port);
        c.0.write_all(&request).unwrap();
        let reply = c.read();
        assert!(
            matches!(&reply, Reply::Error(e) if e.contains("unauthenticated")),
            "{reply:?}"
        );
    }
    let mut c = Conn::connect(port);
    assert!(c.cmd(&["AUTH", "s3cret"]) == Reply::Simple("OK".into()));
    let big = "x".repeat(100_000);
    assert_eq!(c.cmd(&["SET", "k", &big]), Reply::Simple("OK".into()));
    let args: Vec<String> = (0..20).map(|i| i.to_string()).collect();
    let mut rpush = vec!["RPUSH", "l"];
    rpush.extend(args.iter().map(String::as_str));
    assert_eq!(c.cmd(&rpush), Reply::Int(20));
}

/// A request that arrives a byte at a time is read whole, once.
#[test]
fn a_request_that_trickles_in_is_read_whole() {
    let port = start_server("");
    let mut c = Conn::connect(port);
    let value = "v".repeat(5000);
    let mut request = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n".to_vec();
    request.extend_from_slice(format!("${}\r\n{value}\r\n", value.len()).as_bytes());
    for chunk in request.chunks(7) {
        c.0.write_all(chunk).unwrap();
        c.0.flush().unwrap();
    }
    assert_eq!(c.read(), Reply::Simple("OK".into()));
    assert_eq!(c.cmd(&["STRLEN", "k"]), Reply::Int(5000));
}

#[cfg(feature = "lua")]
#[test]
fn msgpack_that_nests_without_end_is_an_error() {
    let port = start_server("");
    let mut c = Conn::connect(port);
    let mut packed = vec![0x91u8; 1_000_000];
    packed.push(0x01);
    let reply = c.call(&[b"EVAL", b"return cmsgpack.unpack(ARGV[1])", b"0", &packed]);
    assert!(reply.is_error(), "{reply:?}");
    c.still_answers();
}
