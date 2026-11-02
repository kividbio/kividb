//! Integration tests for all Pub/Sub commands.
//!
//! Pub/sub tests are fundamentally multi-threaded — one thread subscribes,
//! another publishes. Each test gets its own isolated server port.
//!
//! Test coverage:
//!   SUBSCRIBE / UNSUBSCRIBE
//!   PSUBSCRIBE / PUNSUBSCRIBE (pattern matching)
//!   SSUBSCRIBE / SUNSUBSCRIBE (shard channels)
//!   PUBLISH / SPUBLISH
//!   PUBSUB CHANNELS / NUMSUB / NUMPAT
//!   PUBSUB SHARDCHANNELS / SHARDNUMSUB
//!   PING in subscriber mode
//!   UNSUBSCRIBE with no args (unsubscribe all)
//!   Disallowed commands in subscriber mode

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

// ── RESP client ───────────────────────────────────────────────────────────────

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        for _ in 0..60 {
            // 3 seconds total
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let reader = BufReader::new(s.try_clone().unwrap());
                return Conn { stream: s, reader };
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {port} did not start in time");
    }

    fn connect_with_timeout(port: u16, _timeout: Duration) -> Self {
        // The timeout parameter only affects reads after connection,
        // not the connection retry loop. We always connect with a generous
        // read timeout so the subscriber ack read doesn't race.
        for _ in 0..60 {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let reader = BufReader::new(s.try_clone().unwrap());
                return Conn { stream: s, reader };
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {port} did not start in time");
    }

    fn cmd(&mut self, args: &[&[u8]]) -> RespValue {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read_value()
    }

    fn cmds(&mut self, args: &[&str]) -> RespValue {
        let owned: Vec<Vec<u8>> = args.iter().map(|s| s.as_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        self.cmd(&refs)
    }

    /// Send without reading a response (for fire-and-forget subscribe commands)
    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
    }

    fn read_value(&mut self) -> RespValue {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => RespValue::SimpleStr(line[1..].to_string()),
            Some(b'-') => RespValue::Error(line[1..].to_string()),
            Some(b':') => RespValue::Int(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len == -1 {
                    return RespValue::Nil;
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                use std::io::Read;
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                RespValue::Bulk(data)
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().unwrap();
                if count == -1 {
                    return RespValue::Nil;
                }
                let items = (0..count).map(|_| self.read_value()).collect();
                RespValue::Array(items)
            }
            _ => panic!("unexpected RESP byte in line: {:?}", line),
        }
    }

    /// Try to read a value with a short timeout — returns None on timeout/error
    fn try_read(&mut self) -> Option<RespValue> {
        self.stream
            .set_read_timeout(Some(Duration::from_millis(300)))
            .ok()?;
        let result = self.read_value_nonpanic();
        // Restore longer timeout for subsequent reads
        self.stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .ok();
        result
    }

    fn read_value_nonpanic(&mut self) -> Option<RespValue> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) | Err(_) => return None, // timeout or disconnect
            Ok(_) => {}
        }
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => Some(RespValue::SimpleStr(line[1..].to_string())),
            Some(b'-') => Some(RespValue::Error(line[1..].to_string())),
            Some(b':') => Some(RespValue::Int(line[1..].parse().ok()?)),
            Some(b'$') => {
                let len: i64 = line[1..].parse().ok()?;
                if len == -1 {
                    return Some(RespValue::Nil);
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2];
                use std::io::Read;
                self.reader.read_exact(&mut data).ok()?;
                data.truncate(len);
                Some(RespValue::Bulk(data))
            }
            Some(b'*') => {
                let count: i64 = line[1..].parse().ok()?;
                if count == -1 {
                    return Some(RespValue::Nil);
                }
                let items: Option<Vec<_>> =
                    (0..count).map(|_| self.read_value_nonpanic()).collect();
                Some(RespValue::Array(items?))
            }
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum RespValue {
    SimpleStr(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    fn as_str(&self) -> &str {
        match self {
            RespValue::SimpleStr(s) => s,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap(),
            _ => panic!("expected string, got: {:?}", self),
        }
    }
    fn as_int(&self) -> i64 {
        match self {
            RespValue::Int(n) => *n,
            RespValue::Bulk(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            _ => panic!("expected int, got: {:?}", self),
        }
    }
    fn as_array(&self) -> &[RespValue] {
        match self {
            RespValue::Array(v) => v,
            _ => panic!("expected array, got: {:?}", self),
        }
    }
    fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }
}

// ── Server fixture ────────────────────────────────────────────────────────────

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

fn start_server(port: u16) {
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 4,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: "".to_string(),
            aclfile: "".to_string(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            cluster_enabled: false,
            cluster_node_timeout: 15000,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    // Give Windows time to bind the port before any thread tries to connect
    thread::sleep(Duration::from_millis(100));
}

// ── SUBSCRIBE / PUBLISH ───────────────────────────────────────────────────────

/// Basic SUBSCRIBE receives message published by another client
#[test]
fn test_subscribe_receives_publish() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);

        // Subscribe and read the ack
        sub.send(&["SUBSCRIBE", "chan:basic"]);
        let ack = sub.read_value();
        let ack_arr = ack.as_array();
        assert_eq!(ack_arr[0].as_str(), "subscribe");
        assert_eq!(ack_arr[1].as_str(), "chan:basic");
        assert_eq!(ack_arr[2].as_int(), 1);

        // Signal publisher to proceed
        barrier2.wait();

        // Wait for the message
        let msg = sub.read_value();
        let arr = msg.as_array();
        assert_eq!(arr[0].as_str(), "message");
        assert_eq!(arr[1].as_str(), "chan:basic");
        assert_eq!(arr[2].as_str(), "hello world");
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait(); // wait for subscriber to be ready

        // Small sleep to ensure subscriber loop is polling
        thread::sleep(Duration::from_millis(50));

        let r = pub_conn.cmds(&["PUBLISH", "chan:basic", "hello world"]);
        assert_eq!(
            r.as_int(),
            1,
            "expected 1 subscriber to receive the message"
        );
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// PUBLISH to channel with no subscribers returns 0
#[test]
fn test_publish_no_subscribers() {
    let port = free_port();
    start_server(port);

    let mut c = Conn::connect(port);
    let r = c.cmds(&["PUBLISH", "chan:empty", "nothing"]);
    assert_eq!(r.as_int(), 0);
}

/// SUBSCRIBE to multiple channels in one command
#[test]
fn test_subscribe_multiple_channels() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SUBSCRIBE", "ch1", "ch2", "ch3"]);

        // Read 3 subscription acks
        let ack1 = sub.read_value();
        assert_eq!(ack1.as_array()[2].as_int(), 1); // count=1 after first sub

        let ack2 = sub.read_value();
        assert_eq!(ack2.as_array()[2].as_int(), 2); // count=2

        let ack3 = sub.read_value();
        assert_eq!(ack3.as_array()[2].as_int(), 3); // count=3

        b2.wait(); // signal publisher

        // Receive message on ch2
        let msg = sub.read_value();
        assert_eq!(msg.as_array()[1].as_str(), "ch2");
        assert_eq!(msg.as_array()[2].as_str(), "payload2");
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        pub_conn.cmds(&["PUBLISH", "ch2", "payload2"]);
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// UNSUBSCRIBE from a specific channel
#[test]
fn test_unsubscribe_specific() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["SUBSCRIBE", "ch:a", "ch:b"]);
    sub.read_value(); // ack ch:a
    sub.read_value(); // ack ch:b

    // Unsubscribe from ch:a
    sub.send(&["UNSUBSCRIBE", "ch:a"]);
    let unsub = sub.read_value();
    let arr = unsub.as_array();
    assert_eq!(arr[0].as_str(), "unsubscribe");
    assert_eq!(arr[1].as_str(), "ch:a");
    assert_eq!(arr[2].as_int(), 1); // still subscribed to ch:b
}

/// UNSUBSCRIBE with no args unsubscribes from all channels
#[test]
fn test_unsubscribe_all() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["SUBSCRIBE", "ch:x", "ch:y"]);
    sub.read_value(); // ack
    sub.read_value(); // ack

    sub.send(&["UNSUBSCRIBE"]);
    // Should get acks for both channels (order may vary)
    let u1 = sub.read_value();
    let u2 = sub.read_value();
    assert_eq!(u1.as_array()[0].as_str(), "unsubscribe");
    assert_eq!(u2.as_array()[0].as_str(), "unsubscribe");
    // Final count should be 0
    assert_eq!(u2.as_array()[2].as_int(), 0);
}

/// PUBLISH reaches multiple subscribers on the same channel
#[test]
fn test_publish_multiple_subscribers() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(3)); // 2 subs + 1 pub

    let mut handles = vec![];
    for _ in 0..2 {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut sub = Conn::connect(port);
            sub.send(&["SUBSCRIBE", "broadcast"]);
            sub.read_value(); // ack

            b.wait(); // ready

            let msg = sub.read_value();
            assert_eq!(msg.as_array()[2].as_str(), "boom");
        }));
    }

    let b = Arc::clone(&barrier);
    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        b.wait();
        thread::sleep(Duration::from_millis(50));
        let r = pub_conn.cmds(&["PUBLISH", "broadcast", "boom"]);
        assert_eq!(r.as_int(), 2); // both subs received it
    });

    for h in handles {
        h.join().unwrap();
    }
    pub_handle.join().unwrap();
}

// ── PSUBSCRIBE / PUNSUBSCRIBE ─────────────────────────────────────────────────

/// PSUBSCRIBE receives pmessage for matching channels
#[test]
fn test_psubscribe_receives_pmessage() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["PSUBSCRIBE", "news.*"]);

        let ack = sub.read_value();
        assert_eq!(ack.as_array()[0].as_str(), "psubscribe");
        assert_eq!(ack.as_array()[1].as_str(), "news.*");
        assert_eq!(ack.as_array()[2].as_int(), 1);

        b2.wait();

        let msg = sub.read_value();
        let arr = msg.as_array();
        // pmessage has 4 fields: pmessage, pattern, channel, payload
        assert_eq!(arr[0].as_str(), "pmessage");
        assert_eq!(arr[1].as_str(), "news.*");
        assert_eq!(arr[2].as_str(), "news.tech");
        assert_eq!(arr[3].as_str(), "breaking");
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        let r = pub_conn.cmds(&["PUBLISH", "news.tech", "breaking"]);
        assert_eq!(r.as_int(), 1);
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// PSUBSCRIBE pattern does NOT match unrelated channels
#[test]
fn test_psubscribe_no_match() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect_with_timeout(port, Duration::from_millis(500));
        sub.send(&["PSUBSCRIBE", "news.*"]);
        sub.read_value(); // ack

        b2.wait();

        // Should NOT receive anything — weather.today doesn't match news.*
        // try_read with short timeout should return None
        thread::sleep(Duration::from_millis(150));
        let got = sub.try_read();
        assert!(
            got.is_none(),
            "should not receive message for non-matching channel"
        );
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        let r = pub_conn.cmds(&["PUBLISH", "weather.today", "sunny"]);
        assert_eq!(r.as_int(), 0); // no pattern subscribers match
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// PUNSUBSCRIBE removes pattern subscription
#[test]
fn test_punsubscribe() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["PSUBSCRIBE", "h?llo", "w*rld"]);
    sub.read_value(); // ack 1
    sub.read_value(); // ack 2

    sub.send(&["PUNSUBSCRIBE", "h?llo"]);
    let unsub = sub.read_value();
    assert_eq!(unsub.as_array()[0].as_str(), "punsubscribe");
    assert_eq!(unsub.as_array()[1].as_str(), "h?llo");
    assert_eq!(unsub.as_array()[2].as_int(), 1); // w*rld still active
}

/// PUNSUBSCRIBE with no args removes all patterns
#[test]
fn test_punsubscribe_all() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["PSUBSCRIBE", "a*", "b*", "c*"]);
    sub.read_value();
    sub.read_value();
    sub.read_value();

    sub.send(&["PUNSUBSCRIBE"]);
    let u1 = sub.read_value();
    let u2 = sub.read_value();
    let u3 = sub.read_value();
    assert_eq!(u1.as_array()[0].as_str(), "punsubscribe");
    assert_eq!(u2.as_array()[0].as_str(), "punsubscribe");
    assert_eq!(u3.as_array()[0].as_str(), "punsubscribe");
    assert_eq!(u3.as_array()[2].as_int(), 0);
}

/// Both regular subscriber and pattern subscriber receive the same PUBLISH
#[test]
fn test_publish_reaches_pattern_and_exact_subscriber() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(3));

    // Exact subscriber
    let b1 = Arc::clone(&barrier);
    let exact_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SUBSCRIBE", "events.login"]);
        sub.read_value();
        b1.wait();
        let msg = sub.read_value();
        assert_eq!(msg.as_array()[0].as_str(), "message");
        assert_eq!(msg.as_array()[2].as_str(), "user123");
    });

    // Pattern subscriber
    let b2 = Arc::clone(&barrier);
    let pat_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["PSUBSCRIBE", "events.*"]);
        sub.read_value();
        b2.wait();
        let msg = sub.read_value();
        assert_eq!(msg.as_array()[0].as_str(), "pmessage");
        assert_eq!(msg.as_array()[3].as_str(), "user123");
    });

    let b3 = Arc::clone(&barrier);
    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        b3.wait();
        thread::sleep(Duration::from_millis(50));
        let r = pub_conn.cmds(&["PUBLISH", "events.login", "user123"]);
        assert_eq!(r.as_int(), 2); // exact + pattern
    });

    exact_handle.join().unwrap();
    pat_handle.join().unwrap();
    pub_handle.join().unwrap();
}

// ── SSUBSCRIBE / SPUBLISH / SUNSUBSCRIBE ─────────────────────────────────────

/// SSUBSCRIBE receives smessage published via SPUBLISH
#[test]
fn test_ssubscribe_receives_spublish() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SSUBSCRIBE", "shard:orders"]);

        let ack = sub.read_value();
        assert_eq!(ack.as_array()[0].as_str(), "ssubscribe");
        assert_eq!(ack.as_array()[1].as_str(), "shard:orders");
        assert_eq!(ack.as_array()[2].as_int(), 1);

        b2.wait();

        let msg = sub.read_value();
        let arr = msg.as_array();
        assert_eq!(arr[0].as_str(), "smessage");
        assert_eq!(arr[1].as_str(), "shard:orders");
        assert_eq!(arr[2].as_str(), "order-42");
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        let r = pub_conn.cmds(&["SPUBLISH", "shard:orders", "order-42"]);
        assert_eq!(r.as_int(), 1);
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// SPUBLISH does NOT deliver to regular SUBSCRIBE subscribers
#[test]
fn test_spublish_does_not_reach_regular_subscriber() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect_with_timeout(port, Duration::from_millis(500));
        sub.send(&["SUBSCRIBE", "shard:orders"]);
        sub.read_value(); // ack

        b2.wait();

        thread::sleep(Duration::from_millis(150));
        let got = sub.try_read();
        assert!(got.is_none(), "SPUBLISH should not reach regular SUBSCRIBE");
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        // SPUBLISH to shard channel — should NOT reach regular subscriber
        let r = pub_conn.cmds(&["SPUBLISH", "shard:orders", "data"]);
        assert_eq!(r.as_int(), 0);
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// SUNSUBSCRIBE removes shard channel subscription
#[test]
fn test_sunsubscribe() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["SSUBSCRIBE", "sc:a", "sc:b"]);
    sub.read_value();
    sub.read_value();

    sub.send(&["SUNSUBSCRIBE", "sc:a"]);
    let unsub = sub.read_value();
    assert_eq!(unsub.as_array()[0].as_str(), "sunsubscribe");
    assert_eq!(unsub.as_array()[1].as_str(), "sc:a");
    assert_eq!(unsub.as_array()[2].as_int(), 1); // sc:b still active
}

/// SUNSUBSCRIBE with no args removes all shard subscriptions
#[test]
fn test_sunsubscribe_all() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["SSUBSCRIBE", "sc:1", "sc:2"]);
    sub.read_value();
    sub.read_value();

    sub.send(&["SUNSUBSCRIBE"]);
    let u1 = sub.read_value();
    let u2 = sub.read_value();
    assert_eq!(u1.as_array()[0].as_str(), "sunsubscribe");
    assert_eq!(u2.as_array()[0].as_str(), "sunsubscribe");
    assert_eq!(u2.as_array()[2].as_int(), 0);
}

// ── PING in subscriber mode ───────────────────────────────────────────────────

/// PING while subscribed returns pong array, not +PONG
#[test]
fn test_ping_in_subscriber_mode() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["SUBSCRIBE", "ping:test"]);
    sub.read_value(); // ack

    sub.send(&["PING"]);
    let pong = sub.read_value();
    let arr = pong.as_array();
    assert_eq!(arr[0].as_str(), "pong");
    assert_eq!(arr[1].as_str(), ""); // empty payload
}

/// PING with message payload in subscriber mode
#[test]
fn test_ping_with_payload_in_subscriber_mode() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["SUBSCRIBE", "ping:payload"]);
    sub.read_value();

    sub.send(&["PING", "hello"]);
    let pong = sub.read_value();
    let arr = pong.as_array();
    assert_eq!(arr[0].as_str(), "pong");
    assert_eq!(arr[1].as_str(), "hello");
}

// ── Commands disallowed in subscriber mode ────────────────────────────────────

/// SET is not allowed while subscribed — returns error
#[test]
fn test_disallowed_command_in_subscriber_mode() {
    let port = free_port();
    start_server(port);

    let mut sub = Conn::connect(port);
    sub.send(&["SUBSCRIBE", "restrict:test"]);
    sub.read_value();

    sub.send(&["SET", "key", "val"]);
    let r = sub.read_value();
    assert!(r.is_error(), "SET should be rejected in subscribe mode");
}

// ── PUBSUB introspection ──────────────────────────────────────────────────────

/// PUBSUB CHANNELS returns active channels (those with subscribers)
#[test]
fn test_pubsub_channels_basic() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SUBSCRIBE", "pubsub:ch:1", "pubsub:ch:2"]);
        sub.read_value();
        sub.read_value();
        b2.wait(); // signal introspection to proceed

        // keep alive until test is done
        thread::sleep(Duration::from_secs(1));
    });

    let inspect_handle = thread::spawn(move || {
        let mut c = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        let r = c.cmds(&["PUBSUB", "CHANNELS"]);
        let channels: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
        assert!(channels.contains(&"pubsub:ch:1"));
        assert!(channels.contains(&"pubsub:ch:2"));
    });

    inspect_handle.join().unwrap();
    sub_handle.join().unwrap();
}

/// PUBSUB CHANNELS with pattern filter
#[test]
fn test_pubsub_channels_pattern() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SUBSCRIBE", "match:a", "match:b", "other:c"]);
        sub.read_value();
        sub.read_value();
        sub.read_value();
        b2.wait();
        thread::sleep(Duration::from_secs(1));
    });

    let inspect_handle = thread::spawn(move || {
        let mut c = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        let r = c.cmds(&["PUBSUB", "CHANNELS", "match:*"]);
        let channels: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
        assert!(channels.contains(&"match:a"));
        assert!(channels.contains(&"match:b"));
        assert!(
            !channels.contains(&"other:c"),
            "should not include non-matching channel"
        );
    });

    inspect_handle.join().unwrap();
    sub_handle.join().unwrap();
}

/// PUBSUB NUMSUB returns subscriber count per channel
#[test]
fn test_pubsub_numsub() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(3)); // 2 subs + 1 inspector

    let make_sub = |ch: &'static str, b: Arc<Barrier>| {
        thread::spawn(move || {
            let mut sub = Conn::connect(port);
            sub.send(&["SUBSCRIBE", ch]);
            sub.read_value();
            b.wait();
            thread::sleep(Duration::from_secs(1));
        })
    };

    let h1 = make_sub("numsub:ch", Arc::clone(&barrier));
    let h2 = make_sub("numsub:ch", Arc::clone(&barrier));

    let b = Arc::clone(&barrier);
    let inspect_handle = thread::spawn(move || {
        let mut c = Conn::connect(port);
        b.wait();
        thread::sleep(Duration::from_millis(50));

        let r = c.cmds(&["PUBSUB", "NUMSUB", "numsub:ch", "noone"]);
        let arr = r.as_array();
        // Returns: ch1, count1, ch2, count2
        assert_eq!(arr[0].as_str(), "numsub:ch");
        assert_eq!(arr[1].as_int(), 2);
        assert_eq!(arr[2].as_str(), "noone");
        assert_eq!(arr[3].as_int(), 0);
    });

    inspect_handle.join().unwrap();
    h1.join().unwrap();
    h2.join().unwrap();
}

/// PUBSUB NUMPAT returns total pattern subscriptions
#[test]
fn test_pubsub_numpat() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        // 3 patterns
        sub.send(&["PSUBSCRIBE", "pat:*", "h?llo", "w[aeiou]rld"]);
        sub.read_value();
        sub.read_value();
        sub.read_value();
        b2.wait();
        thread::sleep(Duration::from_secs(1));
    });

    let inspect_handle = thread::spawn(move || {
        let mut c = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        let r = c.cmds(&["PUBSUB", "NUMPAT"]);
        // 3 pattern subscriptions from the subscriber above
        assert_eq!(r.as_int(), 3);
    });

    inspect_handle.join().unwrap();
    sub_handle.join().unwrap();
}

/// PUBSUB NUMPAT with no pattern subscribers returns 0
#[test]
fn test_pubsub_numpat_zero() {
    let port = free_port();
    start_server(port);

    let mut c = Conn::connect(port);
    assert_eq!(c.cmds(&["PUBSUB", "NUMPAT"]).as_int(), 0);
}

// ── PUBSUB SHARDCHANNELS / SHARDNUMSUB ───────────────────────────────────────

/// PUBSUB SHARDCHANNELS returns active shard channels
#[test]
fn test_pubsub_shardchannels() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SSUBSCRIBE", "sc:orders", "sc:payments"]);
        sub.read_value();
        sub.read_value();
        b2.wait();
        thread::sleep(Duration::from_secs(1));
    });

    let inspect_handle = thread::spawn(move || {
        let mut c = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        let r = c.cmds(&["PUBSUB", "SHARDCHANNELS"]);
        let channels: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
        assert!(channels.contains(&"sc:orders"));
        assert!(channels.contains(&"sc:payments"));
    });

    inspect_handle.join().unwrap();
    sub_handle.join().unwrap();
}

/// PUBSUB SHARDCHANNELS with pattern filter
#[test]
fn test_pubsub_shardchannels_pattern() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SSUBSCRIBE", "sc:match:1", "sc:match:2", "sc:other"]);
        sub.read_value();
        sub.read_value();
        sub.read_value();
        b2.wait();
        thread::sleep(Duration::from_secs(1));
    });

    let inspect_handle = thread::spawn(move || {
        let mut c = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        let r = c.cmds(&["PUBSUB", "SHARDCHANNELS", "sc:match:*"]);
        let channels: Vec<&str> = r.as_array().iter().map(|v| v.as_str()).collect();
        assert!(channels.contains(&"sc:match:1"));
        assert!(channels.contains(&"sc:match:2"));
        assert!(!channels.contains(&"sc:other"));
    });

    inspect_handle.join().unwrap();
    sub_handle.join().unwrap();
}

/// PUBSUB SHARDNUMSUB returns subscriber count per shard channel
#[test]
fn test_pubsub_shardnumsub() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(3));

    let make_shard_sub = |ch: &'static str, b: Arc<Barrier>| {
        thread::spawn(move || {
            let mut sub = Conn::connect(port);
            sub.send(&["SSUBSCRIBE", ch]);
            sub.read_value();
            b.wait();
            thread::sleep(Duration::from_secs(1));
        })
    };

    let h1 = make_shard_sub("sc:numsub", Arc::clone(&barrier));
    let h2 = make_shard_sub("sc:numsub", Arc::clone(&barrier));

    let b = Arc::clone(&barrier);
    let inspect_handle = thread::spawn(move || {
        let mut c = Conn::connect(port);
        b.wait();
        thread::sleep(Duration::from_millis(50));

        let r = c.cmds(&["PUBSUB", "SHARDNUMSUB", "sc:numsub", "sc:nobody"]);
        let arr = r.as_array();
        assert_eq!(arr[0].as_str(), "sc:numsub");
        assert_eq!(arr[1].as_int(), 2);
        assert_eq!(arr[2].as_str(), "sc:nobody");
        assert_eq!(arr[3].as_int(), 0);
    });

    inspect_handle.join().unwrap();
    h1.join().unwrap();
    h2.join().unwrap();
}

// ── Edge cases ────────────────────────────────────────────────────────────────

/// Subscribing to the same channel twice is idempotent (no double delivery)
#[test]
fn test_subscribe_duplicate_channel_idempotent() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect_with_timeout(port, Duration::from_millis(500));
        // Subscribe to same channel twice
        sub.send(&["SUBSCRIBE", "dup:ch", "dup:ch"]);
        let a1 = sub.read_value();
        // The second subscribe should still ack (count may be 1 or 2 depending on impl)
        // Key point: should not crash or deadlock
        assert_eq!(a1.as_array()[0].as_str(), "subscribe");
        let _a2 = sub.read_value(); // consume second ack

        b2.wait();

        let msg = sub.read_value();
        assert_eq!(msg.as_array()[2].as_str(), "once");

        // Should NOT receive a second copy — short timeout
        thread::sleep(Duration::from_millis(150));
        let second = sub.try_read();
        assert!(
            second.is_none(),
            "message should be received only once despite double subscribe"
        );
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        pub_conn.cmds(&["PUBLISH", "dup:ch", "once"]);
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// PUBLISH returns correct count when subscriber disconnects mid-session
#[test]
fn test_publish_count_after_disconnect() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SUBSCRIBE", "disc:ch"]);
        sub.read_value();
        b2.wait();
        // Drop connection (disconnect)
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        // Give subscriber time to disconnect
        thread::sleep(Duration::from_millis(200));
        sub_handle.join().unwrap();
        thread::sleep(Duration::from_millis(100));
        // Subscriber is gone — count should be 0
        let r = pub_conn.cmds(&["PUBLISH", "disc:ch", "too late"]);
        assert_eq!(r.as_int(), 0);
    });

    pub_handle.join().unwrap();
}

/// Multiple messages on same channel received in order
#[test]
fn test_subscribe_message_ordering() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);
        sub.send(&["SUBSCRIBE", "order:ch"]);
        sub.read_value();
        b2.wait();

        let mut received = Vec::new();
        for _ in 0..5 {
            let msg = sub.read_value();
            received.push(msg.as_array()[2].as_str().to_string());
        }

        assert_eq!(received, vec!["msg0", "msg1", "msg2", "msg3", "msg4"]);
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        for i in 0..5 {
            pub_conn.cmds(&["PUBLISH", "order:ch", &format!("msg{i}")]);
        }
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// Adding more subscriptions while already in subscriber mode works correctly
#[test]
fn test_subscribe_while_in_subscriber_mode() {
    let port = free_port();
    start_server(port);

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);

    let sub_handle = thread::spawn(move || {
        let mut sub = Conn::connect(port);

        // Initial subscribe
        sub.send(&["SUBSCRIBE", "multi:1"]);
        let ack1 = sub.read_value();
        assert_eq!(ack1.as_array()[2].as_int(), 1);

        b2.wait(); // tell publisher we're ready for multi:2

        // Add second channel while already subscribed
        sub.send(&["SUBSCRIBE", "multi:2"]);
        let ack2 = sub.read_value();
        assert_eq!(ack2.as_array()[2].as_int(), 2); // now subscribed to 2

        // Receive message on multi:2
        let msg = sub.read_value();
        assert_eq!(msg.as_array()[1].as_str(), "multi:2");
        assert_eq!(msg.as_array()[2].as_str(), "payload2");
    });

    let pub_handle = thread::spawn(move || {
        let mut pub_conn = Conn::connect(port);
        barrier.wait();
        thread::sleep(Duration::from_millis(100));
        pub_conn.cmds(&["PUBLISH", "multi:2", "payload2"]);
    });

    sub_handle.join().unwrap();
    pub_handle.join().unwrap();
}

/// Channel names are bytes, as in Redis: `PUBLISH` refused one that was not
/// UTF-8, and `SUBSCRIBE` changed it into another (#161).
#[test]
fn test_channel_names_are_binary_safe() {
    let port = free_port();
    start_server(port);
    let mut sub = Conn::connect(port);
    let mut publisher = Conn::connect(port);
    let name: &[u8] = b"bin\xff\xfe:chan";
    let ack = sub.cmd(&[b"SUBSCRIBE", name]);
    assert_eq!(
        ack,
        RespValue::Array(vec![
            RespValue::Bulk(b"subscribe".to_vec()),
            RespValue::Bulk(name.to_vec()),
            RespValue::Int(1),
        ])
    );
    assert_eq!(
        publisher.cmd(&[b"PUBLISH", name, b"hello"]),
        RespValue::Int(1)
    );
    assert_eq!(
        sub.read_value(),
        RespValue::Array(vec![
            RespValue::Bulk(b"message".to_vec()),
            RespValue::Bulk(name.to_vec()),
            RespValue::Bulk(b"hello".to_vec()),
        ])
    );
    // Another name that would read the same, decoded lossily, is another
    // channel.
    assert_eq!(
        publisher.cmd(&[b"PUBLISH", b"bin\xef\xbf\xbd\xef\xbf\xbd:chan", b"x"]),
        RespValue::Int(0)
    );
    let channels = publisher.cmd(&[b"PUBSUB", b"CHANNELS", b"bin*"]);
    assert_eq!(
        channels,
        RespValue::Array(vec![RespValue::Bulk(name.to_vec())])
    );
}
