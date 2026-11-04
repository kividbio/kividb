// tests/resp3.rs — byte-level RESP3 wire-shape tests.
//
// Everything here asserts on RAW BYTES rather than going through a parsing
// client. That is the point: the bugs this file guards are cases where the
// server emitted something a strict RESP3 parser mis-reads or rejects, and a
// lenient in-repo parser would happily accept the broken bytes and hide the
// problem. `tests/e2e/` covers the same commands through redis-py for
// behavioural parity; this file pins the exact encoding.
//
// Each test asserts the RESP2 shape too, so a change that fixes RESP3 by
// breaking RESP2 fails here rather than in production.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

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
            threads: 2,
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
}

/// A deliberately dumb client: it writes commands and hands back raw reply
/// bytes. It does not parse RESP, so it cannot paper over a malformed frame.
struct RawConn {
    stream: TcpStream,
}

impl RawConn {
    fn connect(port: u16) -> Self {
        let stream = (0..40)
            .find_map(|_| {
                thread::sleep(Duration::from_millis(50));
                TcpStream::connect(("127.0.0.1", port)).ok()
            })
            .expect("could not connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        Self { stream }
    }

    /// Send a command and return the raw reply bytes.
    ///
    /// Reads until the reply looks complete rather than assuming it arrives in
    /// one TCP segment — larger aggregate replies are routinely split.
    fn raw(&mut self, args: &[&str]) -> Vec<u8> {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
        }
        self.stream.write_all(&buf).unwrap();

        let mut out = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = self.stream.read(&mut chunk).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&chunk[..n]);
            if reply_is_complete(&out) {
                break;
            }
        }
        out
    }

    /// Negotiate RESP3, discarding the HELLO map.
    fn hello3(&mut self) {
        let reply = self.raw(&["HELLO", "3"]);
        assert!(
            reply.starts_with(b"%") || reply.starts_with(b"*"),
            "HELLO 3 should return a map/array, got {:?}",
            String::from_utf8_lossy(&reply)
        );
    }
}

/// Minimal RESP scanner used only to decide "have we read a whole reply yet".
/// Returns None when more bytes are needed.
fn scan_one(b: &[u8], mut i: usize) -> Option<usize> {
    if i >= b.len() {
        return None;
    }
    let tag = b[i];
    let line_end = find_crlf(b, i)?;
    let header = &b[i + 1..line_end];
    i = line_end + 2;

    match tag {
        b'+' | b'-' | b':' | b',' | b'#' | b'(' | b'_' => Some(i),
        b'$' | b'=' => {
            let n: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if n < 0 {
                return Some(i);
            }
            let end = i + n as usize + 2;
            if end <= b.len() { Some(end) } else { None }
        }
        b'*' | b'~' | b'>' | b'%' => {
            let n: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if n < 0 {
                return Some(i);
            }
            // Maps declare pair count, so they carry twice that many values.
            let elems = if tag == b'%' { n * 2 } else { n };
            for _ in 0..elems {
                i = scan_one(b, i)?;
            }
            Some(i)
        }
        _ => Some(i),
    }
}

fn find_crlf(b: &[u8], from: usize) -> Option<usize> {
    (from..b.len().saturating_sub(1)).find(|&i| b[i] == b'\r' && b[i + 1] == b'\n')
}

fn reply_is_complete(b: &[u8]) -> bool {
    scan_one(b, 0).is_some()
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).replace("\r\n", "\\r\\n")
}

/// Assert raw reply bytes, printing both sides escaped on failure.
macro_rules! assert_wire {
    ($actual:expr, $expected:expr, $what:expr) => {{
        let a: Vec<u8> = $actual;
        let e: &[u8] = $expected;
        assert_eq!(
            a,
            e.to_vec(),
            "{}\n  actual:   {}\n  expected: {}",
            $what,
            s(&a),
            s(e)
        );
    }};
}

// ═══════════════════════════════════════════════════════════════════════════
// Sorted sets — doubles and WITHSCORES nesting
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn zscore_is_a_double_under_resp3_and_a_bulk_string_under_resp2() {
    let port = free_port();
    start_server(port);

    let mut c2 = RawConn::connect(port);
    c2.raw(&["ZADD", "z", "1.5", "a"]);
    assert_wire!(
        c2.raw(&["ZSCORE", "z", "a"]),
        b"$3\r\n1.5\r\n",
        "RESP2 ZSCORE must stay a bulk string"
    );

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["ZSCORE", "z", "a"]),
        b",1.5\r\n",
        "RESP3 ZSCORE must be a double"
    );
}

#[test]
fn zscore_missing_member_is_resp3_null() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["ZADD", "z", "1", "a"]);
    // Whole reply is the null, so the dispatcher's normalize step covers it.
    assert_wire!(
        c3.raw(&["ZSCORE", "z", "nope"]),
        b"_\r\n",
        "RESP3 ZSCORE miss must be the null type, not $-1"
    );
}

#[test]
fn zmscore_nulls_are_nested_and_must_not_be_resp2_encoded() {
    let port = free_port();
    start_server(port);

    let mut c2 = RawConn::connect(port);
    c2.raw(&["ZADD", "z", "1", "a"]);
    assert_wire!(
        c2.raw(&["ZMSCORE", "z", "a", "missing"]),
        b"*2\r\n$1\r\n1\r\n$-1\r\n",
        "RESP2 ZMSCORE unchanged"
    );

    // The regression this guards: a null *inside* an array is invisible to
    // `resp3_normalize_null`, so it has to be encoded correctly at the site.
    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["ZMSCORE", "z", "a", "missing"]),
        b"*2\r\n,1\r\n_\r\n",
        "RESP3 ZMSCORE needs a double and a nested null"
    );
}

#[test]
fn zrange_withscores_nests_pairs_under_resp3_only() {
    let port = free_port();
    start_server(port);

    let mut c2 = RawConn::connect(port);
    c2.raw(&["ZADD", "z", "1", "a", "2", "b"]);
    assert_wire!(
        c2.raw(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]),
        b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n",
        "RESP2 WITHSCORES must stay flat with 2N elements"
    );

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]),
        b"*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2\r\n",
        "RESP3 WITHSCORES must nest each pair, outer length = member count"
    );
}

#[test]
fn zrange_without_withscores_is_flat_under_both_protocols() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["ZADD", "z", "1", "a", "2", "b"]);
    // Nesting applies only to WITHSCORES; a plain range stays a flat array.
    assert_wire!(
        c3.raw(&["ZRANGE", "z", "0", "-1"]),
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        "RESP3 ZRANGE without scores must not nest"
    );
}

#[test]
fn zpopmin_nests_only_when_an_explicit_count_is_given() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);

    // No count → flat [member, score], matching Redis's
    // `use_nested_array = (c->resp > 2 && count != -1)`.
    assert_wire!(
        c3.raw(&["ZPOPMIN", "z"]),
        b"*2\r\n$1\r\na\r\n,1\r\n",
        "RESP3 ZPOPMIN without count must stay flat"
    );

    // Explicit count → nested pairs, even when it is 1.
    assert_wire!(
        c3.raw(&["ZPOPMIN", "z", "1"]),
        b"*1\r\n*2\r\n$1\r\nb\r\n,2\r\n",
        "RESP3 ZPOPMIN with count must nest"
    );
}

#[test]
fn zincrby_and_zadd_incr_return_doubles_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["ZINCRBY", "z", "2.5", "a"]),
        b",2.5\r\n",
        "RESP3 ZINCRBY must be a double"
    );
    assert_wire!(
        c3.raw(&["ZADD", "z", "INCR", "1", "a"]),
        b",3.5\r\n",
        "RESP3 ZADD INCR must be a double"
    );
    // NX on an existing member skips the update and returns null.
    assert_wire!(
        c3.raw(&["ZADD", "z", "NX", "INCR", "1", "a"]),
        b"_\r\n",
        "RESP3 ZADD INCR skipped must be the null type"
    );
}

#[test]
fn zscan_keeps_flat_bulk_strings_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["ZADD", "z", "1", "a"]);
    // Redis's scanCallback renders scores with d2string and replies with
    // addReplyBulk regardless of protocol, so ZSCAN must NOT gain doubles or
    // nesting. This test exists to stop a well-meaning "consistency" change.
    assert_wire!(
        c3.raw(&["ZSCAN", "z", "0"]),
        b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n",
        "ZSCAN must keep flat bulk-string scores under RESP3"
    );
}

#[test]
fn zrank_withscore_returns_a_double_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["ZADD", "z", "1.5", "a"]);
    assert_wire!(
        c3.raw(&["ZRANK", "z", "a", "WITHSCORE"]),
        b"*2\r\n:0\r\n,1.5\r\n",
        "RESP3 ZRANK WITHSCORE must pair an integer rank with a double"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Pub/Sub — push frames
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn subscribe_ack_and_delivery_are_push_frames_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut sub = RawConn::connect(port);
    sub.hello3();
    assert_wire!(
        sub.raw(&["SUBSCRIBE", "ch"]),
        b">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n",
        "RESP3 subscribe ack must be a push frame"
    );

    let mut pubr = RawConn::connect(port);
    pubr.raw(&["PUBLISH", "ch", "hi"]);

    let mut chunk = [0u8; 512];
    let n = sub.stream.read(&mut chunk).unwrap();
    assert_wire!(
        chunk[..n].to_vec(),
        b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n",
        "RESP3 message delivery must be a push frame"
    );
}

#[test]
fn subscribe_stays_an_array_under_resp2() {
    let port = free_port();
    start_server(port);

    let mut sub = RawConn::connect(port);
    assert_wire!(
        sub.raw(&["SUBSCRIBE", "ch"]),
        b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n",
        "RESP2 subscribe ack must stay an array"
    );

    let mut pubr = RawConn::connect(port);
    pubr.raw(&["PUBLISH", "ch", "hi"]);

    let mut chunk = [0u8; 512];
    let n = sub.stream.read(&mut chunk).unwrap();
    assert_wire!(
        chunk[..n].to_vec(),
        b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n",
        "RESP2 message delivery must stay an array"
    );
}

#[test]
fn one_publish_frames_each_subscriber_by_its_own_protocol() {
    let port = free_port();
    start_server(port);

    // The case that forces per-subscriber framing: a single PUBLISH fans out to
    // two subscribers that negotiated different protocols. Deciding the frame
    // once per publish would necessarily be wrong for one of them.
    let mut sub2 = RawConn::connect(port);
    sub2.raw(&["SUBSCRIBE", "shared"]);

    let mut sub3 = RawConn::connect(port);
    sub3.hello3();
    sub3.raw(&["SUBSCRIBE", "shared"]);

    let mut pubr = RawConn::connect(port);
    pubr.raw(&["PUBLISH", "shared", "x"]);

    let mut b2 = [0u8; 512];
    let n2 = sub2.stream.read(&mut b2).unwrap();
    assert_wire!(
        b2[..n2].to_vec(),
        b"*3\r\n$7\r\nmessage\r\n$6\r\nshared\r\n$1\r\nx\r\n",
        "the RESP2 subscriber must get an array"
    );

    let mut b3 = [0u8; 512];
    let n3 = sub3.stream.read(&mut b3).unwrap();
    assert_wire!(
        b3[..n3].to_vec(),
        b">3\r\n$7\r\nmessage\r\n$6\r\nshared\r\n$1\r\nx\r\n",
        "the RESP3 subscriber must get a push frame from the same PUBLISH"
    );
}

// ── Subscribed RESP3 clients are not restricted ──────────────────────────────
//
// Redis gates the "only (P|S)SUBSCRIBE / ... are allowed" restriction on
// `c->flags & CLIENT_PUBSUB && c->resp == 2`. RESP2 clients stay restricted;
// RESP3 clients keep full command access while subscribed. redis-py's RESP3
// pubsub depends on this — it issues ordinary commands on a subscribed
// connection — so a RESP3 client that subscribes was previously unusable.

#[test]
fn resp3_client_can_run_ordinary_commands_while_subscribed() {
    let port = free_port();
    start_server(port);

    let mut c = RawConn::connect(port);
    c.hello3();
    assert_wire!(
        c.raw(&["SUBSCRIBE", "ch"]),
        b">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n",
        "RESP3 subscribe ack"
    );

    // The whole point of the change: these must be served normally, not
    // rejected with "only ... allowed in this context".
    assert_wire!(
        c.raw(&["SET", "k", "v"]),
        b"+OK\r\n",
        "SET must work on a subscribed RESP3 connection"
    );
    assert_wire!(
        c.raw(&["GET", "k"]),
        b"$1\r\nv\r\n",
        "GET must work on a subscribed RESP3 connection"
    );

    // ...and the subscription is still live afterwards.
    let mut pubr = RawConn::connect(port);
    pubr.raw(&["PUBLISH", "ch", "hi"]);
    let mut chunk = [0u8; 512];
    let n = c.stream.read(&mut chunk).unwrap();
    assert_wire!(
        chunk[..n].to_vec(),
        b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n",
        "subscription must survive running other commands"
    );
}

#[test]
fn resp2_client_stays_restricted_while_subscribed() {
    let port = free_port();
    start_server(port);

    let mut c = RawConn::connect(port);
    assert_wire!(
        c.raw(&["SUBSCRIBE", "ch"]),
        b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n",
        "RESP2 subscribe ack"
    );

    // Redis keeps RESP2 subscribers restricted; lifting it for RESP3 must not
    // lift it here too.
    let reply = c.raw(&["SET", "k", "v"]);
    assert!(
        reply.starts_with(b"-"),
        "RESP2 SET while subscribed must be rejected, got {}",
        s(&reply)
    );
}

#[test]
fn resp3_unsubscribe_stops_delivery_and_reports_counts() {
    let port = free_port();
    start_server(port);

    let mut c = RawConn::connect(port);
    c.hello3();
    c.raw(&["SUBSCRIBE", "a"]);
    assert_wire!(
        c.raw(&["SUBSCRIBE", "b"]),
        b">3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n",
        "second subscribe must report a count of 2"
    );
    assert_wire!(
        c.raw(&["UNSUBSCRIBE", "a"]),
        b">3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n",
        "unsubscribe must report the remaining count"
    );

    // Publishing to the unsubscribed channel must deliver nothing. PUBLISH's
    // own return value is the authoritative check: 0 receivers means the
    // forwarder task and its receiver were really dropped, not just ignored.
    let mut pubr = RawConn::connect(port);
    assert_wire!(
        pubr.raw(&["PUBLISH", "a", "x"]),
        b":0\r\n",
        "no subscribers should remain on the unsubscribed channel"
    );
    assert_wire!(
        pubr.raw(&["PUBLISH", "b", "y"]),
        b":1\r\n",
        "the still-subscribed channel must keep its subscriber"
    );
}

#[test]
fn downgrading_to_resp2_while_subscribed_reframes_deliveries() {
    let port = free_port();
    start_server(port);

    let mut c = RawConn::connect(port);
    c.hello3();
    c.raw(&["SUBSCRIBE", "ch"]);

    // Lifting the subscriber-mode restriction makes `HELLO 2` reachable while
    // subscribed, which a RESP2 client could never do. If the delivery framing
    // were captured at subscribe time, this connection would keep receiving `>`
    // push frames its parser no longer understands. Framing is read per
    // delivery instead, so the very next message must arrive as a RESP2 array.
    let hello = c.raw(&["HELLO", "2"]);
    assert!(
        hello.starts_with(b"*") || hello.starts_with(b"%"),
        "HELLO 2 should reply, got {}",
        s(&hello)
    );

    let mut pubr = RawConn::connect(port);
    pubr.raw(&["PUBLISH", "ch", "hi"]);

    let mut chunk = [0u8; 512];
    let n = c.stream.read(&mut chunk).unwrap();
    assert_wire!(
        chunk[..n].to_vec(),
        b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n",
        "after HELLO 2 the delivery must be a RESP2 array, not a push frame"
    );
}

#[test]
fn resp3_subscription_does_not_block_the_connection_for_pipelined_commands() {
    let port = free_port();
    start_server(port);

    let mut c = RawConn::connect(port);
    c.hello3();
    // SUBSCRIBE and a normal command in a single pipelined write — the
    // subscribe must be handled inline rather than swallowing the rest of the
    // buffer into a subscriber-mode handoff.
    c.stream
        .write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nch\r\n*1\r\n$4\r\nPING\r\n")
        .unwrap();

    let mut out = Vec::new();
    let mut chunk = [0u8; 512];
    while !out.ends_with(b"+PONG\r\n") {
        let n = c.stream.read(&mut chunk).unwrap();
        assert!(n > 0, "connection closed before PING was answered");
        out.extend_from_slice(&chunk[..n]);
    }
    assert_wire!(
        out,
        b">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n+PONG\r\n",
        "pipelined SUBSCRIBE + PING must both be answered"
    );
}

#[test]
fn pmessage_is_a_push_frame_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut sub = RawConn::connect(port);
    sub.hello3();
    sub.raw(&["PSUBSCRIBE", "news.*"]);

    let mut pubr = RawConn::connect(port);
    pubr.raw(&["PUBLISH", "news.tech", "v"]);

    let mut chunk = [0u8; 512];
    let n = sub.stream.read(&mut chunk).unwrap();
    assert_wire!(
        chunk[..n].to_vec(),
        b">4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$9\r\nnews.tech\r\n$1\r\nv\r\n",
        "RESP3 pmessage must be a 4-element push frame"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Streams — XREAD becomes a map keyed by stream name
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn xread_is_an_array_of_pairs_under_resp2_and_a_map_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c2 = RawConn::connect(port);
    c2.raw(&["XADD", "st", "1-1", "f", "v"]);

    // RESP2: [[name, [[id, [field, value]]]]]
    assert_wire!(
        c2.raw(&["XREAD", "COUNT", "1", "STREAMS", "st", "0"]),
        b"*1\r\n*2\r\n$2\r\nst\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n",
        "RESP2 XREAD must stay an array of [name, entries] pairs"
    );

    // RESP3: a map keyed by stream name (Redis switches to addReplyMapLen).
    // redis-py hands this back as a dict rather than a list of tuples, so the
    // difference is load-bearing for callers, not cosmetic.
    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["XREAD", "COUNT", "1", "STREAMS", "st", "0"]),
        b"%1\r\n$2\r\nst\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n",
        "RESP3 XREAD must be a map keyed by stream name"
    );
}

#[test]
fn xinfo_stream_is_a_map_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["XADD", "st", "1-1", "f", "v"]);

    let reply = c3.raw(&["XINFO", "STREAM", "st"]);
    assert!(
        reply.starts_with(b"%"),
        "RESP3 XINFO STREAM must be a map, got {}",
        s(&reply)
    );

    let mut c2 = RawConn::connect(port);
    let reply2 = c2.raw(&["XINFO", "STREAM", "st"]);
    assert!(
        reply2.starts_with(b"*"),
        "RESP2 XINFO STREAM must stay a flat array, got {}",
        s(&reply2)
    );
}

#[test]
fn xinfo_stream_nested_entry_nils_use_the_resp3_null() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    // A stream with entries added then deleted reports nil first/last entries,
    // nested inside the map — `resp3_normalize_null` cannot reach those.
    c3.raw(&["XADD", "st", "1-1", "f", "v"]);
    c3.raw(&["XDEL", "st", "1-1"]);

    let reply = c3.raw(&["XINFO", "STREAM", "st"]);
    assert!(
        reply.windows(3).any(|w| w == b"_\r\n"),
        "nested first/last-entry nils must use the RESP3 null type, got {}",
        s(&reply)
    );
    assert!(
        !reply.windows(5).any(|w| w == b"$-1\r\n"),
        "no RESP2 null markers should remain under RESP3, got {}",
        s(&reply)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG GET — map type
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn config_get_is_a_map_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c2 = RawConn::connect(port);
    assert_wire!(
        c2.raw(&["CONFIG", "GET", "maxmemory-policy"]),
        b"*2\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n",
        "RESP2 CONFIG GET must stay a flat array"
    );

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["CONFIG", "GET", "maxmemory-policy"]),
        b"%1\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n",
        "RESP3 CONFIG GET must be a map with pair count, not element count"
    );
}

#[test]
fn config_get_unknown_param_is_an_empty_map_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["CONFIG", "GET", "no-such-parameter"]),
        b"%0\r\n",
        "RESP3 CONFIG GET of an unknown param must be an empty map"
    );
}

#[test]
fn config_get_without_a_parameter_does_not_kill_the_connection() {
    let port = free_port();
    start_server(port);

    let mut c = RawConn::connect(port);
    // `CONFIG GET` with no parameter indexes cmd[2]; this asserts it produces
    // an error reply rather than panicking the connection task. The follow-up
    // PING proves the connection is still usable.
    let reply = c.raw(&["CONFIG", "GET"]);
    assert!(
        reply.starts_with(b"-"),
        "bare CONFIG GET should be an arity error, got {}",
        s(&reply)
    );
    assert_wire!(
        c.raw(&["PING"]),
        b"+PONG\r\n",
        "connection must survive a bare CONFIG GET"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Sets — the RESP3 set type (`~`)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn smembers_uses_the_set_type_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c2 = RawConn::connect(port);
    c2.raw(&["SADD", "s", "only"]);
    assert_wire!(
        c2.raw(&["SMEMBERS", "s"]),
        b"*1\r\n$4\r\nonly\r\n",
        "RESP2 SMEMBERS must stay an array"
    );

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["SMEMBERS", "s"]),
        b"~1\r\n$4\r\nonly\r\n",
        "RESP3 SMEMBERS must use the set type"
    );
}

#[test]
fn set_operations_use_the_set_type_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["SADD", "a", "x"]);
    c3.raw(&["SADD", "b", "x"]);

    for cmd in ["SINTER", "SUNION"] {
        assert_wire!(
            c3.raw(&[cmd, "a", "b"]),
            b"~1\r\n$1\r\nx\r\n",
            "RESP3 set operation must use the set type"
        );
    }
    // SDIFF of identical sets is empty — still a set, not an array.
    assert_wire!(
        c3.raw(&["SDIFF", "a", "b"]),
        b"~0\r\n",
        "RESP3 empty SDIFF must be an empty set"
    );
}

#[test]
fn smembers_missing_key_is_an_empty_set_under_resp3() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["SMEMBERS", "nosuchkey"]),
        b"~0\r\n",
        "RESP3 SMEMBERS on a missing key must be an empty set"
    );
}

#[test]
fn spop_is_a_set_with_count_but_srandmember_stays_an_array() {
    let port = free_port();
    start_server(port);

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    c3.raw(&["SADD", "s", "one"]);

    // SRANDMEMBER can repeat members with a negative count, so it is an array
    // in RESP3 even though it reads from a set.
    assert_wire!(
        c3.raw(&["SRANDMEMBER", "s", "2"]),
        b"*1\r\n$3\r\none\r\n",
        "RESP3 SRANDMEMBER with count must stay an array"
    );
    // SPOP without a count is a plain bulk string, not a set.
    assert_wire!(
        c3.raw(&["SPOP", "s"]),
        b"$3\r\none\r\n",
        "RESP3 SPOP without count must be a bulk string"
    );

    c3.raw(&["SADD", "s2", "two"]);
    assert_wire!(
        c3.raw(&["SPOP", "s2", "1"]),
        b"~1\r\n$3\r\ntwo\r\n",
        "RESP3 SPOP with count must use the set type"
    );
}

#[test]
fn zset_infinity_scores_survive_both_protocols() {
    let port = free_port();
    start_server(port);

    let mut c2 = RawConn::connect(port);
    c2.raw(&["ZADD", "z", "inf", "hi", "-inf", "lo"]);
    assert_wire!(
        c2.raw(&["ZSCORE", "z", "hi"]),
        b"$3\r\ninf\r\n",
        "RESP2 +inf score"
    );

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["ZSCORE", "z", "hi"]),
        b",inf\r\n",
        "RESP3 +inf must use the double type's inf form"
    );
    assert_wire!(
        c3.raw(&["ZSCORE", "z", "lo"]),
        b",-inf\r\n",
        "RESP3 -inf must use the double type's -inf form"
    );
}

/// `MODULE LIST` names the module families KiviDB has built in, with the
/// versions Redis 8.10 reports and an empty path, the way Redis lists its own
/// built-in `vectorset`. Clients check it before they use FT.* or JSON.*:
/// redisvl 0.4 (LlamaIndex's Redis vector store) refuses to connect without
/// `search`.
#[test]
fn module_list_names_the_built_in_families_in_both_protocols() {
    let port = free_port();
    start_server(port);

    let modules = [
        ("search", 81001),
        ("ReJSON", 81000),
        ("timeseries", 81001),
        ("vectorset", 1),
    ];
    let expected = |header: &str| {
        let mut out = format!("*{}\r\n", modules.len());
        for (name, ver) in modules {
            out.push_str(&format!(
                "{header}\r\n$4\r\nname\r\n${}\r\n{name}\r\n$3\r\nver\r\n:{ver}\r\n\
                 $4\r\npath\r\n$0\r\n\r\n$4\r\nargs\r\n*0\r\n",
                name.len()
            ));
        }
        out.into_bytes()
    };

    let mut c2 = RawConn::connect(port);
    assert_wire!(
        c2.raw(&["MODULE", "LIST"]),
        &expected("*8")[..],
        "RESP2 MODULE LIST"
    );

    let mut c3 = RawConn::connect(port);
    c3.hello3();
    assert_wire!(
        c3.raw(&["MODULE", "LIST"]),
        &expected("%4")[..],
        "RESP3 MODULE LIST"
    );
}

/// HELLO's `modules` is the list MODULE LIST gives, as in Redis.
#[test]
fn hello_reports_the_built_in_modules() {
    let port = free_port();
    start_server(port);
    let mut c = RawConn::connect(port);
    let hello = s(&c.raw(&["HELLO", "2"]));
    let list = s(&c.raw(&["MODULE", "LIST"]));
    assert!(
        hello.ends_with(&format!("$7\\r\\nmodules\\r\\n{list}")),
        "HELLO must end with the module list:\n  hello: {hello}\n  list:  {list}"
    );
}

// ── Publishing to oneself (#23) ──────────────────────────────────────────────

/// Read exactly `expected.len()` bytes, or fail if they do not arrive.
fn read_exactly(c: &mut RawConn, expected: &[u8]) -> Vec<u8> {
    c.stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let mut out = vec![0u8; expected.len()];
    c.stream.read_exact(&mut out).unwrap_or_else(|e| {
        panic!("{e}: wanted {:?}", s(expected));
    });
    out
}

/// A RESP3 `message` push for `channel`.
fn message_push(channel: &str, payload: &str) -> Vec<u8> {
    format!(
        ">3\r\n$7\r\nmessage\r\n${}\r\n{channel}\r\n${}\r\n{payload}\r\n",
        channel.len(),
        payload.len()
    )
    .into_bytes()
}

/// RESP for a command.
fn command(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// A message a subscribed connection publishes to itself comes after the
/// reply to the PUBLISH, as in Redis. Its own forwarder used to write it as
/// soon as it was queued, so the two raced; many rounds give the race room.
#[test]
fn a_message_published_to_oneself_comes_after_the_publish_reply() {
    let port = free_port();
    start_server(port);
    let mut c = RawConn::connect(port);
    c.hello3();
    assert_wire!(
        c.raw(&["SUBSCRIBE", "foo"]),
        b">3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n",
        "subscribe"
    );
    for i in 0..200 {
        let payload = format!("m{i}");
        c.stream
            .write_all(&command(&["PUBLISH", "foo", &payload]))
            .unwrap();
        let mut want = b":1\r\n".to_vec();
        want.extend_from_slice(&message_push("foo", &payload));
        let got = read_exactly(&mut c, &want);
        assert_wire!(got, &want[..], "round {i}");
    }
}

/// The Redis suite's "publish to self inside multi".
#[test]
fn messages_published_to_oneself_in_a_transaction_come_after_exec() {
    let port = free_port();
    start_server(port);
    let mut c = RawConn::connect(port);
    c.hello3();
    c.raw(&["SUBSCRIBE", "foo"]);
    assert_wire!(c.raw(&["MULTI"]), b"+OK\r\n", "multi");
    for args in [
        &["PING", "abc"][..],
        &["PUBLISH", "foo", "bar"],
        &["PUBLISH", "foo", "vaz"],
        &["PING", "def"],
    ] {
        assert_wire!(c.raw(args), b"+QUEUED\r\n", "queued");
    }
    c.stream.write_all(&command(&["EXEC"])).unwrap();
    let mut want = b"*4\r\n$3\r\nabc\r\n:1\r\n:1\r\n$3\r\ndef\r\n".to_vec();
    want.extend_from_slice(&message_push("foo", "bar"));
    want.extend_from_slice(&message_push("foo", "vaz"));
    assert_wire!(
        read_exactly(&mut c, &want),
        &want[..],
        "EXEC, then the messages"
    );
}

/// The Redis suite's "unsubscribe inside multi, and publish to self":
/// UNSUBSCRIBE in a transaction is queued, and EXEC's reply carries its
/// replies. It used to run at once, outside the transaction.
#[test]
fn unsubscribe_in_a_transaction_is_queued_and_answered_in_exec() {
    let port = free_port();
    start_server(port);
    let mut c = RawConn::connect(port);
    c.hello3();
    c.stream
        .write_all(&command(&["SUBSCRIBE", "foo", "bar", "baz"]))
        .unwrap();
    let mut want = b">3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n".to_vec();
    want.extend_from_slice(b">3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n");
    want.extend_from_slice(b">3\r\n$9\r\nsubscribe\r\n$3\r\nbaz\r\n:3\r\n");
    assert_wire!(read_exactly(&mut c, &want), &want[..], "subscribe");

    assert_wire!(c.raw(&["MULTI"]), b"+OK\r\n", "multi");
    for args in [
        &["PING", "abc"][..],
        &["UNSUBSCRIBE", "bar"],
        &["UNSUBSCRIBE", "baz"],
        &["PING", "def"],
    ] {
        assert_wire!(c.raw(args), b"+QUEUED\r\n", "queued");
    }
    c.stream.write_all(&command(&["EXEC"])).unwrap();
    let want = b"*4\r\n$3\r\nabc\r\n\
                 >3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:2\r\n\
                 >3\r\n$11\r\nunsubscribe\r\n$3\r\nbaz\r\n:1\r\n\
                 $3\r\ndef\r\n";
    assert_wire!(read_exactly(&mut c, want), &want[..], "exec");

    c.stream
        .write_all(&command(&["PUBLISH", "foo", "vaz"]))
        .unwrap();
    let mut want = b":1\r\n".to_vec();
    want.extend_from_slice(&message_push("foo", "vaz"));
    assert_wire!(
        read_exactly(&mut c, &want),
        &want[..],
        "publish, then the message"
    );
}

/// The Redis suite's "publish to self inside script".
#[cfg(feature = "lua")]
#[test]
fn messages_a_script_publishes_to_its_own_connection_come_after_its_reply() {
    let port = free_port();
    start_server(port);
    let mut c = RawConn::connect(port);
    c.hello3();
    c.raw(&["SUBSCRIBE", "foo"]);
    let script = "redis.call('ping','abc') redis.call('publish','foo','bar') \
                  redis.call('publish','foo','vaz') redis.call('ping','def') return 'bla'";
    c.stream
        .write_all(&command(&["EVAL", script, "0"]))
        .unwrap();
    let mut want = b"$3\r\nbla\r\n".to_vec();
    want.extend_from_slice(&message_push("foo", "bar"));
    want.extend_from_slice(&message_push("foo", "vaz"));
    assert_wire!(
        read_exactly(&mut c, &want),
        &want[..],
        "the script's reply, then the messages"
    );
}
