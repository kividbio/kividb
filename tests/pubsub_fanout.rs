// Fan-out of one message to many subscribers (#851): a message's frame is
// built once for each channel or pattern it goes through and shared by all
// of their subscribers, and a subscriber's buffers are kept from one batch
// to the next. These tests hold what that must not change: each
// subscriber's order under concurrent publishers, one pmessage per matching
// pattern, RESP3 pushes, a PUNSUBSCRIBE or a disconnect in the middle of a
// burst, keyspace events through patterns, and client tracking beside it.
// Each test runs on both I/O models where io_uring exists.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// ── Minimal RESP client ───────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Clone)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<Resp>),
    /// A RESP3 push (`>`).
    Push(Vec<Resp>),
    Map(Vec<(Resp, Resp)>),
    Nil,
}

impl Resp {
    fn items(&self) -> &[Resp] {
        match self {
            Resp::Array(items) | Resp::Push(items) => items,
            _ => panic!("not an array or a push: {self:?}"),
        }
    }
    fn text(&self) -> String {
        match self {
            Resp::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Resp::Simple(s) => s.clone(),
            Resp::Error(e) => panic!("an error where a string was wanted: {e}"),
            _ => panic!("not a string: {self:?}"),
        }
    }
    fn int(&self) -> i64 {
        match self {
            Resp::Int(n) => *n,
            _ => panic!("not an integer: {self:?}"),
        }
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

fn encode(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = (0..100)
            .find_map(|_| {
                TcpStream::connect(("127.0.0.1", port)).ok().or_else(|| {
                    thread::sleep(Duration::from_millis(50));
                    None
                })
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        let reader = BufReader::with_capacity(1 << 16, stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn send(&mut self, args: &[&str]) {
        self.stream.write_all(&encode(args)).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> Resp {
        self.send(args);
        self.read()
    }

    fn resp3(port: u16) -> Self {
        let mut c = Self::connect(port);
        match c.cmd(&["HELLO", "3"]) {
            Resp::Map(fields) => assert!(
                fields
                    .iter()
                    .any(|(k, v)| k.text() == "proto" && *v == Resp::Int(3))
            ),
            other => panic!("HELLO 3: {other:?}"),
        }
        c
    }

    /// The next frame's bytes exactly as they came, for one frame of bulks
    /// (array or push) whose header says how many there are.
    fn raw_frame(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).unwrap();
        out.extend_from_slice(&line);
        let n: usize = std::str::from_utf8(&line[1..line.len() - 2])
            .unwrap()
            .parse()
            .unwrap();
        for _ in 0..n {
            line.clear();
            self.reader.read_until(b'\n', &mut line).unwrap();
            out.extend_from_slice(&line);
            let len: usize = std::str::from_utf8(&line[1..line.len() - 2])
                .unwrap()
                .parse()
                .unwrap();
            let mut data = vec![0u8; len + 2];
            self.reader.read_exact(&mut data).unwrap();
            out.extend_from_slice(&data);
        }
        out
    }

    fn read(&mut self) -> Resp {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).unwrap();
        assert!(line.ends_with(b"\r\n"), "short read: {line:?}");
        let body = String::from_utf8_lossy(&line[1..line.len() - 2]).into_owned();
        match line[0] {
            b'+' => Resp::Simple(body),
            b'-' => Resp::Error(body),
            b':' => Resp::Int(body.parse().unwrap()),
            b'_' => Resp::Nil,
            b'$' => {
                let len: i64 = body.parse().unwrap();
                if len < 0 {
                    return Resp::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                Resp::Bulk(data)
            }
            b'*' | b'>' => {
                let n: i64 = body.parse().unwrap();
                if n < 0 {
                    return Resp::Nil;
                }
                let items = (0..n).map(|_| self.read()).collect();
                if line[0] == b'>' {
                    Resp::Push(items)
                } else {
                    Resp::Array(items)
                }
            }
            b'%' => {
                let n: usize = body.parse().unwrap();
                Resp::Map((0..n).map(|_| (self.read(), self.read())).collect())
            }
            b'#' => Resp::Simple(body),
            other => panic!("unexpected RESP type {:?}: {line:?}", other as char),
        }
    }

    /// Whether anything arrives within `ms`.
    fn quiet_for(&mut self, ms: u64) -> bool {
        if !self.reader.buffer().is_empty() {
            return false;
        }
        self.stream
            .set_read_timeout(Some(Duration::from_millis(ms)))
            .unwrap();
        let mut byte = [0u8; 1];
        let quiet = matches!(
            self.stream.peek(&mut byte),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut
        );
        self.stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        quiet
    }
}

// ── Servers ───────────────────────────────────────────────────────────────────

/// Whether nothing has `port`: asked with a socket that is bound and never
/// listens (see tests/transaction.rs).
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

/// A free port outside the range the system hands out to outgoing
/// connections.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
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

fn start_server(io_model: &kividb::config::IoModel) -> u16 {
    let port = free_port();
    let io_model = io_model.clone();
    thread::spawn(move || {
        let config = kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 4,
            aof: false,
            databases: 16,
            maxmemory: 0,
            loglevel: "error".to_string(),
            io_model,
            replicaof: None,
            replica_announced_port: 0,
            requirepass: String::new(),
            aclfile: "".to_string(),
            cluster_enabled: false,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        };
        kividb::run_for_test(config);
    });
    port
}

/// Both I/O models where io_uring exists, or the one `KIVIDB_TEST_IO_MODEL`
/// names.
fn io_models() -> Vec<kividb::config::IoModel> {
    use kividb::config::IoModel;
    match std::env::var("KIVIDB_TEST_IO_MODEL").as_deref() {
        Ok("tokio") => vec![IoModel::Tokio],
        Ok("uring") => vec![IoModel::Uring],
        _ if cfg!(target_os = "linux") => vec![IoModel::Tokio, IoModel::Uring],
        _ => vec![IoModel::Tokio],
    }
}

/// The publisher number and sequence number of a payload `p<i>:<seq>`.
fn parse_payload(payload: &str) -> (usize, usize) {
    let (p, seq) = payload[1..].split_once(':').unwrap();
    (p.parse().unwrap(), seq.parse().unwrap())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Four publishers at once, each in order on its own channel: every
/// subscriber sees each publisher's messages in order and none missing —
/// through a pattern, under RESP2 and RESP3, and with the channel and a
/// pattern both (the message first, then the pmessage, as in Redis).
#[test]
fn concurrent_publishers_keep_each_subscribers_order() {
    const PUBLISHERS: usize = 4;
    const EACH: usize = 3000;
    for io_model in io_models() {
        let port = start_server(&io_model);
        let mut plain = Conn::connect(port);
        let mut pushed = Conn::resp3(port);
        let mut both = Conn::connect(port);
        assert_eq!(plain.cmd(&["PSUBSCRIBE", "fan:*"]).items()[2].int(), 1);
        assert_eq!(pushed.cmd(&["PSUBSCRIBE", "fan:*"]).items()[2].int(), 1);
        assert_eq!(both.cmd(&["SUBSCRIBE", "fan:0"]).items()[2].int(), 1);
        assert_eq!(both.cmd(&["PSUBSCRIBE", "fan:*"]).items()[2].int(), 2);

        let publishers: Vec<_> = (0..PUBLISHERS)
            .map(|p| {
                thread::spawn(move || {
                    let mut c = Conn::connect(port);
                    let channel = format!("fan:{p}");
                    // Pipelined in runs of 100, so that batches form.
                    for run in 0..EACH / 100 {
                        let mut buf = Vec::new();
                        for seq in run * 100..(run + 1) * 100 {
                            buf.extend(encode(&["PUBLISH", &channel, &format!("p{p}:{seq}")]));
                        }
                        c.stream.write_all(&buf).unwrap();
                        for _ in 0..100 {
                            let receivers = c.read().int();
                            assert_eq!(receivers, if p == 0 { 4 } else { 3 });
                        }
                    }
                })
            })
            .collect();

        for (name, conn) in [("resp2", &mut plain), ("resp3", &mut pushed)] {
            let mut next = [0usize; PUBLISHERS];
            for _ in 0..PUBLISHERS * EACH {
                let msg = conn.read();
                if name == "resp3" {
                    assert!(matches!(msg, Resp::Push(_)), "{io_model:?} {msg:?}");
                }
                let items = msg.items();
                assert_eq!(items[0].text(), "pmessage", "{io_model:?} {name}");
                assert_eq!(items[1].text(), "fan:*");
                let (p, seq) = parse_payload(&items[3].text());
                assert_eq!(items[2].text(), format!("fan:{p}"));
                assert_eq!(
                    seq, next[p],
                    "{io_model:?} {name}: publisher {p} out of order"
                );
                next[p] += 1;
            }
        }
        // `both`: each of publisher 0's messages twice, the message first
        // and then the pmessage (another publisher's can come between them:
        // they publish at once).
        let mut next = [0usize; PUBLISHERS];
        let mut awaiting: Option<usize> = None;
        for _ in 0..(PUBLISHERS + 1) * EACH {
            let msg = both.read();
            let items = msg.items();
            if items[0].text() == "message" {
                let (p, seq) = parse_payload(&items[2].text());
                assert_eq!((p, seq), (0, next[0]), "{io_model:?}");
                assert_eq!(awaiting, None, "{io_model:?}: no pmessage for {awaiting:?}");
                awaiting = Some(seq);
                continue;
            }
            let (p, seq) = parse_payload(&items[3].text());
            if p == 0 {
                assert_eq!(
                    awaiting.take(),
                    Some(seq),
                    "{io_model:?}: pmessage before message"
                );
            }
            assert_eq!(seq, next[p], "{io_model:?}");
            next[p] += 1;
        }
        assert_eq!(next, [EACH; PUBLISHERS]);
        for publisher in publishers {
            publisher.join().unwrap();
        }
        assert!(plain.quiet_for(200) && pushed.quiet_for(200) && both.quiet_for(200));
    }
}

/// A channel that matches several of a client's patterns comes once
/// through each of them, as in Redis; a pattern shared by two clients
/// reaches both. The frames, exactly, under RESP2 and RESP3.
#[test]
fn overlapping_patterns_deliver_one_pmessage_each() {
    for io_model in io_models() {
        let port = start_server(&io_model);
        let mut many = Conn::connect(port);
        let mut one = Conn::resp3(port);
        many.send(&["PSUBSCRIBE", "a*", "*b", "a?", "zz*"]);
        for _ in 0..4 {
            many.read();
        }
        assert_eq!(one.cmd(&["PSUBSCRIBE", "a*"]).items()[2].int(), 1);
        let mut publisher = Conn::connect(port);
        assert_eq!(publisher.cmd(&["PUBLISH", "ab", "x"]).int(), 4);

        let mut frames: Vec<Vec<u8>> = (0..3).map(|_| many.raw_frame()).collect();
        frames.sort();
        let mut expected: Vec<Vec<u8>> = ["*b", "a*", "a?"]
            .iter()
            .map(|p| {
                format!("*4\r\n$8\r\npmessage\r\n$2\r\n{p}\r\n$2\r\nab\r\n$1\r\nx\r\n").into_bytes()
            })
            .collect();
        expected.sort();
        assert_eq!(frames, expected, "{io_model:?}");
        assert_eq!(
            one.raw_frame(),
            b">4\r\n$8\r\npmessage\r\n$2\r\na*\r\n$2\r\nab\r\n$1\r\nx\r\n",
            "{io_model:?}"
        );
        assert!(many.quiet_for(200) && one.quiet_for(200), "{io_model:?}");
        assert_eq!(publisher.cmd(&["PUBSUB", "NUMPAT"]).int(), 4);
    }
}

/// PUNSUBSCRIBE while a burst is being written: what came before its reply
/// is in order with nothing left out, and nothing comes after it.
#[test]
fn punsubscribe_in_the_middle_of_a_burst() {
    for io_model in io_models() {
        let port = start_server(&io_model);
        let mut sub = Conn::connect(port);
        sub.cmd(&["PSUBSCRIBE", "burst:*"]);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let publisher = {
            let stop = std::sync::Arc::clone(&stop);
            thread::spawn(move || {
                let mut c = Conn::connect(port);
                let mut seq = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut buf = Vec::new();
                    for _ in 0..50 {
                        buf.extend(encode(&["PUBLISH", "burst:1", &format!("p0:{seq}")]));
                        seq += 1;
                    }
                    c.stream.write_all(&buf).unwrap();
                    for _ in 0..50 {
                        c.read();
                    }
                }
            })
        };
        let mut next = 0;
        for _ in 0..2000 {
            let msg = sub.read();
            assert_eq!(
                parse_payload(&msg.items()[3].text()).1,
                next,
                "{io_model:?}"
            );
            next += 1;
        }
        sub.send(&["PUNSUBSCRIBE", "burst:*"]);
        loop {
            let msg = sub.read();
            let items = msg.items();
            if items[0].text() == "punsubscribe" {
                assert_eq!(items[2].int(), 0);
                break;
            }
            assert_eq!(parse_payload(&items[3].text()).1, next, "{io_model:?}");
            next += 1;
        }
        assert!(
            sub.quiet_for(300),
            "{io_model:?}: a message after PUNSUBSCRIBE"
        );
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        publisher.join().unwrap();
        assert_eq!(sub.cmd(&["PING"]), Resp::Simple("PONG".into()));
    }
}

/// A subscriber that goes away in the middle of a fan-out: the others get
/// every message, in order, and the server forgets it.
#[test]
fn a_subscriber_leaving_mid_fanout_does_not_stop_the_others() {
    const MESSAGES: usize = 20_000;
    for io_model in io_models() {
        let port = start_server(&io_model);
        let mut stayers: Vec<Conn> = (0..6).map(|_| Conn::connect(port)).collect();
        for c in &mut stayers {
            c.cmd(&["PSUBSCRIBE", "room:*"]);
        }
        let mut leaver = Conn::connect(port);
        leaver.cmd(&["PSUBSCRIBE", "room:*"]);
        let publisher = thread::spawn(move || {
            let mut c = Conn::connect(port);
            for run in 0..MESSAGES / 500 {
                let mut buf = Vec::new();
                for seq in run * 500..(run + 1) * 500 {
                    let payload = format!("p0:{seq}:{}", "x".repeat(200));
                    buf.extend(encode(&[
                        "PUBLISH",
                        &format!("room:{}", seq % 37),
                        &payload,
                    ]));
                }
                c.stream.write_all(&buf).unwrap();
                for _ in 0..500 {
                    c.read();
                }
            }
            c
        });
        for _ in 0..100 {
            leaver.read();
        }
        drop(leaver);
        for (n, c) in stayers.iter_mut().enumerate() {
            for seq in 0..MESSAGES {
                let msg = c.read();
                let payload = msg.items()[3].text();
                assert!(
                    payload.starts_with(&format!("p0:{seq}:")),
                    "{io_model:?} subscriber {n}: wanted {seq}, got {}",
                    &payload[..payload.len().min(20)]
                );
            }
        }
        let mut publisher = publisher.join().unwrap();
        let mut receivers = 0;
        for _ in 0..100 {
            receivers = publisher.cmd(&["PUBLISH", "room:1", "after"]).int();
            if receivers == 6 {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            receivers, 6,
            "{io_model:?}: the one that left is still counted"
        );
        assert_eq!(publisher.cmd(&["PUBSUB", "NUMPAT"]).int(), 1);
    }
}

/// Keyspace events reach pattern subscribers, as pmessages under RESP2 and
/// pushes under RESP3.
#[test]
fn keyspace_events_reach_pattern_subscribers() {
    for io_model in io_models() {
        let port = start_server(&io_model);
        let mut c = Conn::connect(port);
        assert_eq!(
            c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
            Resp::Simple("OK".into())
        );
        let mut plain = Conn::connect(port);
        let mut pushed = Conn::resp3(port);
        plain.cmd(&["PSUBSCRIBE", "__keyspace@0__:fan*"]);
        pushed.cmd(&["PSUBSCRIBE", "__keyevent@0__:*"]);
        assert_eq!(
            c.cmd(&["SET", "fanout-key", "v"]),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            plain.raw_frame(),
            b"*4\r\n$8\r\npmessage\r\n$19\r\n__keyspace@0__:fan*\r\n$25\r\n__keyspace@0__:fanout-key\r\n$3\r\nset\r\n",
            "{io_model:?}"
        );
        assert_eq!(
            pushed.raw_frame(),
            b">4\r\n$8\r\npmessage\r\n$16\r\n__keyevent@0__:*\r\n$18\r\n__keyevent@0__:set\r\n$10\r\nfanout-key\r\n",
            "{io_model:?}"
        );
        c.cmd(&["CONFIG", "SET", "notify-keyspace-events", ""]);
    }
}

/// Client tracking's invalidations still come to a RESP3 connection that is
/// also subscribed to a pattern, beside its pmessages.
#[test]
fn tracking_invalidations_beside_pattern_messages() {
    for io_model in io_models() {
        let port = start_server(&io_model);
        let mut c = Conn::resp3(port);
        assert_eq!(
            c.cmd(&["CLIENT", "TRACKING", "on"]),
            Resp::Simple("OK".into())
        );
        assert_eq!(c.cmd(&["PSUBSCRIBE", "t:*"]).items()[2].int(), 1);
        assert_eq!(c.cmd(&["GET", "tracked"]), Resp::Nil);
        let mut other = Conn::connect(port);
        assert_eq!(
            other.cmd(&["SET", "tracked", "1"]),
            Resp::Simple("OK".into())
        );
        assert_eq!(other.cmd(&["PUBLISH", "t:1", "hi"]).int(), 1);
        let mut got = vec![c.read(), c.read()];
        got.sort_by_key(|m| m.items()[0].text());
        assert_eq!(got[0].items()[0].text(), "invalidate", "{io_model:?}");
        assert_eq!(
            got[0].items()[1],
            Resp::Array(vec![Resp::Bulk(b"tracked".to_vec())])
        );
        assert_eq!(
            got[1],
            Resp::Push(vec![
                Resp::Bulk(b"pmessage".to_vec()),
                Resp::Bulk(b"t:*".to_vec()),
                Resp::Bulk(b"t:1".to_vec()),
                Resp::Bulk(b"hi".to_vec()),
            ]),
            "{io_model:?}"
        );
        assert!(matches!(got[0], Resp::Push(_)));
    }
}
