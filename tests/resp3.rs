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

// ═══════════════════════════════════════════════════════════════════════════
// Lua script replies (#431)
// ═══════════════════════════════════════════════════════════════════════════
//
// As Redis's `luaReplyToRedisReply` writes them: every kind of reply is shaped
// for the client's protocol (a null is `_` to a RESP3 client, `$-1` to a RESP2
// one). The script's own protocol (`redis.setresp`) decides only what a Lua
// boolean is: under 2 (the default) `true` is `:1` and `false` a null; under 3
// they are booleans, `#t`/`#f` to a RESP3 client and `:1`/`:0` to a RESP2 one.
// The expected bytes are Redis 8's.

/// `EVAL script 3 missing h z`, with `h` a hash `{f: v}` and `z` a sorted set
/// `{m: 1.5}`; `missing` does not exist.
#[cfg(feature = "lua")]
fn eval_on(c: &mut RawConn, command: &str, script: &str) -> Vec<u8> {
    c.raw(&[command, script, "3", "missing", "h", "z"])
}

#[cfg(feature = "lua")]
fn lua_conns(port: u16) -> (RawConn, RawConn) {
    let mut c2 = RawConn::connect(port);
    c2.raw(&["DEL", "missing", "h", "z"]);
    c2.raw(&["HSET", "h", "f", "v"]);
    c2.raw(&["ZADD", "z", "1.5", "m"]);
    let mut c3 = RawConn::connect(port);
    c3.hello3();
    (c2, c3)
}

/// Each script's reply to a RESP2 and to a RESP3 client, through `EVAL`.
#[cfg(feature = "lua")]
fn assert_lua_replies(cases: &[(&str, &[u8], &[u8])]) {
    let port = free_port();
    start_server(port);
    let (mut c2, mut c3) = lua_conns(port);
    for (script, resp2, resp3) in cases {
        assert_wire!(
            eval_on(&mut c2, "EVAL", script),
            resp2,
            format!("RESP2: {script}")
        );
        assert_wire!(
            eval_on(&mut c3, "EVAL", script),
            resp3,
            format!("RESP3: {script}")
        );
    }
}

/// The issue's sequence: a script's `false`, and `redis.call`'s nil reply
/// (false in Lua), at the top level and in a table. Celery 5.7's chord
/// script returns `{readycount, totaldiff, size, claimed}` with `totaldiff`
/// a `GET` of a key that is usually missing; redis-py 8 (RESP3) refused
/// the `$-1` with `Protocol Error: b'1'`.
#[cfg(feature = "lua")]
#[test]
fn a_lua_false_is_the_null_of_the_clients_protocol() {
    assert_lua_replies(&[
        (
            "return {1, redis.call('GET', KEYS[1]), '3', 0}",
            b"*4\r\n:1\r\n$-1\r\n$1\r\n3\r\n:0\r\n",
            b"*4\r\n:1\r\n_\r\n$1\r\n3\r\n:0\r\n",
        ),
        ("return false", b"$-1\r\n", b"_\r\n"),
        ("return redis.call('GET', KEYS[1])", b"$-1\r\n", b"_\r\n"),
        ("return nil", b"$-1\r\n", b"_\r\n"),
        ("return true", b":1\r\n", b":1\r\n"),
        (
            "return type(redis.call('GET', KEYS[1]))",
            b"$7\r\nboolean\r\n",
            b"$7\r\nboolean\r\n",
        ),
    ]);
}

/// A false anywhere in a table: deeper arrays, a map's value, a set's member.
/// A nil ends an array, as it does in Redis.
#[cfg(feature = "lua")]
#[test]
fn a_nested_lua_false_is_the_null_of_the_clients_protocol() {
    assert_lua_replies(&[
        (
            "return {1, false, 2}",
            b"*3\r\n:1\r\n$-1\r\n:2\r\n",
            b"*3\r\n:1\r\n_\r\n:2\r\n",
        ),
        (
            "return {true, false}",
            b"*2\r\n:1\r\n$-1\r\n",
            b"*2\r\n:1\r\n_\r\n",
        ),
        ("return {1, nil, 2}", b"*1\r\n:1\r\n", b"*1\r\n:1\r\n"),
        (
            "return {{false, {false}}, 'x'}",
            b"*2\r\n*2\r\n$-1\r\n*1\r\n$-1\r\n$1\r\nx\r\n",
            b"*2\r\n*2\r\n_\r\n*1\r\n_\r\n$1\r\nx\r\n",
        ),
        (
            "return {map={a=false}}",
            b"*2\r\n$1\r\na\r\n$-1\r\n",
            b"%1\r\n$1\r\na\r\n_\r\n",
        ),
        (
            "return {set={[false]=true}}",
            b"*1\r\n$-1\r\n",
            b"~1\r\n_\r\n",
        ),
        (
            "return {1, redis.call}",
            b"*2\r\n:1\r\n$-1\r\n",
            b"*2\r\n:1\r\n_\r\n",
        ),
        (
            "return redis.call('MGET', KEYS[1], KEYS[2])",
            b"*2\r\n$-1\r\n$-1\r\n",
            b"*2\r\n_\r\n_\r\n",
        ),
    ]);
}

/// Under `redis.setresp(3)` the script is a RESP3 client: `redis.call`'s
/// null is a Lua nil (so it ends a table), a Lua boolean is a boolean, and
/// the reply is still shaped for the client's protocol.
#[cfg(feature = "lua")]
#[test]
fn a_script_on_resp3_answers_in_the_clients_protocol() {
    assert_lua_replies(&[
        ("redis.setresp(3) return false", b":0\r\n", b"#f\r\n"),
        ("redis.setresp(3) return true", b":1\r\n", b"#t\r\n"),
        ("redis.setresp(3) return nil", b"$-1\r\n", b"_\r\n"),
        (
            "redis.setresp(3) return {true, false, 1}",
            b"*3\r\n:1\r\n:0\r\n:1\r\n",
            b"*3\r\n#t\r\n#f\r\n:1\r\n",
        ),
        (
            "redis.setresp(3) return redis.call('GET', KEYS[1])",
            b"$-1\r\n",
            b"_\r\n",
        ),
        (
            "redis.setresp(3) return type(redis.call('GET', KEYS[1]))",
            b"$3\r\nnil\r\n",
            b"$3\r\nnil\r\n",
        ),
        (
            "redis.setresp(3) return {1, redis.call('GET', KEYS[1]), '3', 0}",
            b"*1\r\n:1\r\n",
            b"*1\r\n:1\r\n",
        ),
        (
            "redis.setresp(3) return type(redis.call('LPOP', KEYS[1]))",
            b"$3\r\nnil\r\n",
            b"$3\r\nnil\r\n",
        ),
        (
            "redis.setresp(3) return redis.call('HGETALL', KEYS[2])",
            b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n",
            b"%1\r\n$1\r\nf\r\n$1\r\nv\r\n",
        ),
        (
            "redis.setresp(3) return redis.call('ZSCORE', KEYS[3], 'm')",
            b"$3\r\n1.5\r\n",
            b",1.5\r\n",
        ),
        (
            "redis.setresp(3) return {double=3.5}",
            b"$3\r\n3.5\r\n",
            b",3.5\r\n",
        ),
        (
            "redis.setresp(3) return {map={a=1}}",
            b"*2\r\n$1\r\na\r\n:1\r\n",
            b"%1\r\n$1\r\na\r\n:1\r\n",
        ),
        (
            "redis.setresp(3) return {set={x=true}}",
            b"*1\r\n$1\r\nx\r\n",
            b"~1\r\n$1\r\nx\r\n",
        ),
        (
            "redis.setresp(3) redis.setresp(2) return {1, redis.call('GET', KEYS[1]), false}",
            b"*3\r\n:1\r\n$-1\r\n$-1\r\n",
            b"*3\r\n:1\r\n_\r\n_\r\n",
        ),
    ]);
}

/// Error and status tables; a status is one line.
#[cfg(feature = "lua")]
#[test]
fn lua_error_and_status_tables_are_written_as_in_redis() {
    assert_lua_replies(&[
        ("return {err='boom'}", b"-boom\r\n", b"-boom\r\n"),
        ("return {ok='fine'}", b"+fine\r\n", b"+fine\r\n"),
        (
            "return {1, {err='inner'}}",
            b"*2\r\n:1\r\n-inner\r\n",
            b"*2\r\n:1\r\n-inner\r\n",
        ),
        ("return {ok='a\\r\\nb'}", b"+a  b\r\n", b"+a  b\r\n"),
        (
            "redis.setresp(3) return {err='boom'}",
            b"-boom\r\n",
            b"-boom\r\n",
        ),
    ]);
}

/// Typed tables are shaped for the client's protocol, whatever the script's.
#[cfg(feature = "lua")]
#[test]
fn lua_typed_tables_are_shaped_for_the_clients_protocol() {
    assert_lua_replies(&[
        ("return {double=3.5}", b"$3\r\n3.5\r\n", b",3.5\r\n"),
        (
            "return {big_number='12345678901234567890'}",
            b"$20\r\n12345678901234567890\r\n",
            b"(12345678901234567890\r\n",
        ),
        (
            "return {verbatim_string={format='txt', string='hi'}}",
            b"$2\r\nhi\r\n",
            b"=6\r\ntxt:hi\r\n",
        ),
        (
            "return {map={a=1}}",
            b"*2\r\n$1\r\na\r\n:1\r\n",
            b"%1\r\n$1\r\na\r\n:1\r\n",
        ),
        (
            "return {set={x=true}}",
            b"*1\r\n$1\r\nx\r\n",
            b"~1\r\n$1\r\nx\r\n",
        ),
        (
            "redis.setresp(3) return {big_number='12345678901234567890'}",
            b"$20\r\n12345678901234567890\r\n",
            b"(12345678901234567890\r\n",
        ),
        (
            "redis.setresp(3) return {verbatim_string={format='txt', string='hi'}}",
            b"$2\r\nhi\r\n",
            b"=6\r\ntxt:hi\r\n",
        ),
    ]);
}

/// `EVAL_RO`, `EVALSHA`, `FCALL`, `FCALL_RO` and a script in a transaction
/// go through the same conversion.
#[cfg(feature = "lua")]
#[test]
fn every_way_of_running_a_script_sends_its_false_in_the_clients_protocol() {
    let port = free_port();
    start_server(port);
    let (mut c2, mut c3) = lua_conns(port);
    let table = "return {1, redis.call('GET', KEYS[1]), '3', 0}";
    let resp2: &[u8] = b"*4\r\n:1\r\n$-1\r\n$1\r\n3\r\n:0\r\n";
    let resp3: &[u8] = b"*4\r\n:1\r\n_\r\n$1\r\n3\r\n:0\r\n";

    assert_wire!(eval_on(&mut c2, "EVAL_RO", table), resp2, "RESP2 EVAL_RO");
    assert_wire!(eval_on(&mut c3, "EVAL_RO", table), resp3, "RESP3 EVAL_RO");

    let loaded = c2.raw(&["SCRIPT", "LOAD", table]);
    let sha = String::from_utf8_lossy(&loaded)
        .split("\r\n")
        .nth(1)
        .unwrap()
        .to_string();
    assert_wire!(eval_on(&mut c2, "EVALSHA", &sha), resp2, "RESP2 EVALSHA");
    assert_wire!(eval_on(&mut c3, "EVALSHA", &sha), resp3, "RESP3 EVALSHA");
    assert_wire!(
        eval_on(&mut c2, "EVALSHA_RO", &sha),
        resp2,
        "RESP2 EVALSHA_RO"
    );
    assert_wire!(
        eval_on(&mut c3, "EVALSHA_RO", &sha),
        resp3,
        "RESP3 EVALSHA_RO"
    );

    let lib = "#!lua name=resp3_false_431\n\
               redis.register_function('f431_table', function(k, a) return {1, redis.call('GET', k[1]), '3', 0} end)\n\
               redis.register_function('f431_false', function(k, a) return false end)\n\
               redis.register_function{function_name='f431_ro', callback=function(k, a) return redis.call('GET', k[1]) end, flags={'no-writes'}}\n\
               redis.register_function('f431_s3', function(k, a) redis.setresp(3) return {true, false, redis.call('GET', k[1])} end)\n";
    assert_wire!(
        c2.raw(&["FUNCTION", "LOAD", "REPLACE", lib]),
        b"$15\r\nresp3_false_431\r\n",
        "FUNCTION LOAD"
    );
    for (args, want2, want3) in [
        (["FCALL", "f431_table"], resp2, resp3),
        (["FCALL", "f431_false"], &b"$-1\r\n"[..], &b"_\r\n"[..]),
        (["FCALL", "f431_ro"], &b"$-1\r\n"[..], &b"_\r\n"[..]),
        (["FCALL_RO", "f431_ro"], &b"$-1\r\n"[..], &b"_\r\n"[..]),
        (
            ["FCALL", "f431_s3"],
            &b"*2\r\n:1\r\n:0\r\n"[..],
            &b"*2\r\n#t\r\n#f\r\n"[..],
        ),
    ] {
        let cmd = [args[0], args[1], "1", "missing"];
        assert_wire!(c2.raw(&cmd), want2, format!("RESP2 {}", args.join(" ")));
        assert_wire!(c3.raw(&cmd), want3, format!("RESP3 {}", args.join(" ")));
    }
    c2.raw(&["FUNCTION", "DELETE", "resp3_false_431"]);

    for (c, want) in [
        (
            &mut c2,
            &b"*2\r\n$-1\r\n*4\r\n:1\r\n$-1\r\n$1\r\n3\r\n:0\r\n"[..],
        ),
        (
            &mut c3,
            &b"*2\r\n_\r\n*4\r\n:1\r\n_\r\n$1\r\n3\r\n:0\r\n"[..],
        ),
    ] {
        assert_wire!(c.raw(&["MULTI"]), b"+OK\r\n", "MULTI");
        assert_wire!(
            c.raw(&["EVAL", "return false", "0"]),
            b"+QUEUED\r\n",
            "queued"
        );
        assert_wire!(eval_on(c, "EVAL", table), b"+QUEUED\r\n", "queued");
        assert_wire!(c.raw(&["EXEC"]), want, "EXEC of scripts");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FUNCTION LIST and FUNCTION STATS (#476, #439)
// ═══════════════════════════════════════════════════════════════════════════
//
// Under RESP3 the expected bytes are Redis 8.10's: each library, each function
// and the stats are maps, a missing value is `_` and `flags` is a set of simple
// strings. Under RESP2 they are as they were: flat arrays, `$-1` and each flag
// a bulk string (Redis sends a flag as a simple string under both).

#[cfg(feature = "lua")]
#[test]
fn function_list_follows_the_clients_protocol() {
    let port = free_port();
    start_server(port);
    let (mut c2, mut c3) = lua_conns(port);
    let lib = "#!lua name=fnlist_476\n\
               redis.register_function('fnlist_476_a', function() return 1 end)\n\
               redis.register_function{function_name='fnlist_476_b', callback=function() return 2 end, \
               description='has one', flags={'no-writes', 'allow-stale'}}";
    assert_wire!(
        c2.raw(&["FUNCTION", "LOAD", "REPLACE", lib]),
        b"$10\r\nfnlist_476\r\n",
        "FUNCTION LOAD"
    );
    let list = ["FUNCTION", "LIST", "LIBRARYNAME", "fnlist_476"];
    assert_wire!(
        c2.raw(&list),
        b"*1\r\n*6\r\n$12\r\nlibrary_name\r\n$10\r\nfnlist_476\r\n$6\r\nengine\r\n$3\r\nLUA\r\n\
          $9\r\nfunctions\r\n*2\r\n\
          *6\r\n$4\r\nname\r\n$12\r\nfnlist_476_a\r\n$11\r\ndescription\r\n$-1\r\n$5\r\nflags\r\n*0\r\n\
          *6\r\n$4\r\nname\r\n$12\r\nfnlist_476_b\r\n$11\r\ndescription\r\n$7\r\nhas one\r\n\
          $5\r\nflags\r\n*2\r\n$9\r\nno-writes\r\n$11\r\nallow-stale\r\n",
        "RESP2 FUNCTION LIST"
    );
    assert_wire!(
        c3.raw(&list),
        b"*1\r\n%3\r\n$12\r\nlibrary_name\r\n$10\r\nfnlist_476\r\n$6\r\nengine\r\n$3\r\nLUA\r\n\
          $9\r\nfunctions\r\n*2\r\n\
          %3\r\n$4\r\nname\r\n$12\r\nfnlist_476_a\r\n$11\r\ndescription\r\n_\r\n$5\r\nflags\r\n~0\r\n\
          %3\r\n$4\r\nname\r\n$12\r\nfnlist_476_b\r\n$11\r\ndescription\r\n$7\r\nhas one\r\n\
          $5\r\nflags\r\n~2\r\n+no-writes\r\n+allow-stale\r\n",
        "RESP3 FUNCTION LIST"
    );
    // WITHCODE adds a fourth pair: the library's map is `%4`.
    let with_code = c3.raw(&["FUNCTION", "LIST", "WITHCODE", "LIBRARYNAME", "fnlist_476"]);
    assert!(with_code.starts_with(b"*1\r\n%4\r\n"), "{}", s(&with_code));
    assert!(
        with_code
            .windows(b"$12\r\nlibrary_code\r\n".len())
            .any(|w| w == b"$12\r\nlibrary_code\r\n"),
        "{}",
        s(&with_code)
    );
    c2.raw(&["FUNCTION", "DELETE", "fnlist_476"]);
}

/// Neither the counts nor what `running_script` holds are pinned: the
/// function store and the Lua VM are the test process's, and other tests
/// load libraries and run functions in it meanwhile (#431's `f431_s3` was
/// once running here). `running_script` is the protocol's null or its map;
/// the running map's bytes are `function_stats_reply`'s unit test.
#[cfg(feature = "lua")]
#[test]
fn function_stats_follows_the_clients_protocol() {
    let port = free_port();
    start_server(port);
    let (mut c2, mut c3) = lua_conns(port);
    fn check(
        c: &mut RawConn,
        protocol: &str,
        head: &[u8],
        idle: &[u8],
        running: &[u8],
        engines: &[u8],
    ) {
        let stats = c.raw(&["FUNCTION", "STATS"]);
        let rest = stats
            .strip_prefix(head)
            .unwrap_or_else(|| panic!("{protocol}: {}", s(&stats)));
        assert!(
            rest.starts_with(idle) || rest.starts_with(running),
            "{protocol} running_script: {}",
            s(&stats)
        );
        assert!(
            stats.windows(engines.len()).any(|w| w == engines),
            "{protocol} engines: {}",
            s(&stats)
        );
    }
    check(
        &mut c2,
        "RESP2",
        b"*4\r\n$14\r\nrunning_script\r\n",
        b"$-1\r\n",
        b"*6\r\n",
        b"$7\r\nengines\r\n*2\r\n$3\r\nLUA\r\n*4\r\n$15\r\nlibraries_count\r\n:",
    );
    check(
        &mut c3,
        "RESP3",
        b"%2\r\n$14\r\nrunning_script\r\n",
        b"_\r\n",
        b"%3\r\n",
        b"$7\r\nengines\r\n%1\r\n$3\r\nLUA\r\n%2\r\n$15\r\nlibraries_count\r\n:",
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Doubles and JSON replies as Redis 8.10.2 (with RedisJSON 8.10) writes them
// ═══════════════════════════════════════════════════════════════════════════

/// Each command's reply to a RESP2 and a RESP3 client, byte for byte as
/// Redis 8.10.2 sent it (#471, #472, #473).
#[test]
fn doubles_and_json_replies_are_written_as_redis_writes_them() {
    let port = free_port();
    start_server(port);
    let mut c2 = RawConn::connect(port);
    let mut c3 = RawConn::connect(port);
    c3.hello3();

    c2.raw(&[
        "JSON.SET",
        "d",
        "$",
        r#"{"a":1.0,"b":-0.0,"c":100000.0,"d":123456789.12345678,"e":1e300,"f":1e21,"g":1.5e-7,"i":18446744073709551616,"j":1e16,"n":5}"#,
    ]);
    c2.raw(&[
        "ZADD",
        "z",
        "1e16",
        "a",
        "100000000",
        "b",
        "1e21",
        "c",
        "1.5e-7",
        "d",
    ]);

    let both: &[(&[&str], &[u8], &[u8])] = &[
        // JSON text: an exponent has no `+`.
        (
            &["JSON.GET", "d"],
            b"$125\r\n{\"a\":1.0,\"b\":-0.0,\"c\":100000.0,\"d\":123456789.12345678,\"e\":1e300,\"f\":1e21,\"g\":1.5e-7,\"i\":1.8446744073709552e19,\"j\":1e16,\"n\":5}\r\n",
            b"$125\r\n{\"a\":1.0,\"b\":-0.0,\"c\":100000.0,\"d\":123456789.12345678,\"e\":1e300,\"f\":1e21,\"g\":1.5e-7,\"i\":1.8446744073709552e19,\"j\":1e16,\"n\":5}\r\n",
        ),
        (&["JSON.GET", "d", "$.f"], b"$6\r\n[1e21]\r\n", b"$6\r\n[1e21]\r\n"),
        (
            &["JSON.MGET", "d", ".i"],
            b"*1\r\n$21\r\n1.8446744073709552e19\r\n",
            b"*1\r\n$21\r\n1.8446744073709552e19\r\n",
        ),
        // JSON.RESP: a number that is not an integer is a double.
        (&["JSON.RESP", "d", ".a"], b"$1\r\n1\r\n", b",1\r\n"),
        (&["JSON.RESP", "d", ".b"], b"$2\r\n-0\r\n", b",-0\r\n"),
        (&["JSON.RESP", "d", ".c"], b"$6\r\n100000\r\n", b",100000\r\n"),
        (
            &["JSON.RESP", "d", ".d"],
            b"$21\r\n1.2345678912345678e+8\r\n",
            b",1.2345678912345678e+8\r\n",
        ),
        (&["JSON.RESP", "d", ".e"], b"$6\r\n1e+300\r\n", b",1e+300\r\n"),
        (&["JSON.RESP", "d", ".g"], b"$6\r\n1.5e-7\r\n", b",1.5e-7\r\n"),
        (
            &["JSON.RESP", "d", ".i"],
            b"$20\r\n18446744073709552000\r\n",
            b",18446744073709552000\r\n",
        ),
        (
            &["JSON.RESP", "d", ".j"],
            b"$17\r\n10000000000000000\r\n",
            b",10000000000000000\r\n",
        ),
        (&["JSON.RESP", "d", ".n"], b":5\r\n", b":5\r\n"),
        // JSON.TYPE: in RESP3, always in an array of one.
        (&["JSON.TYPE", "d"], b"$6\r\nobject\r\n", b"*1\r\n$6\r\nobject\r\n"),
        (&["JSON.TYPE", "d", ".n"], b"$7\r\ninteger\r\n", b"*1\r\n$7\r\ninteger\r\n"),
        (
            &["JSON.TYPE", "d", "$.a"],
            b"*1\r\n$6\r\nnumber\r\n",
            b"*1\r\n*1\r\n$6\r\nnumber\r\n",
        ),
        (&["JSON.TYPE", "d", "$.nope"], b"*0\r\n", b"*1\r\n*0\r\n"),
        (&["JSON.TYPE", "d", ".nope"], b"$-1\r\n", b"*1\r\n_\r\n"),
        (&["JSON.TYPE", "nokey"], b"$-1\r\n", b"*1\r\n_\r\n"),
        (&["JSON.TYPE", "nokey", "$"], b"$-1\r\n", b"*1\r\n_\r\n"),
        // Scores: whole numbers up to 2^62 as integers.
        (
            &["ZSCORE", "z", "a"],
            b"$17\r\n10000000000000000\r\n",
            b",10000000000000000\r\n",
        ),
        (&["ZSCORE", "z", "b"], b"$9\r\n100000000\r\n", b",100000000\r\n"),
        (&["ZSCORE", "z", "c"], b"$5\r\n1e+21\r\n", b",1e+21\r\n"),
        (&["ZSCORE", "z", "d"], b"$6\r\n1.5e-7\r\n", b",1.5e-7\r\n"),
        // A key that is not there.
        (
            &["JSON.NUMINCRBY", "nokey", ".n", "1"],
            b"-ERR could not perform this operation on a key that doesn't exist\r\n",
            b"-ERR could not perform this operation on a key that doesn't exist\r\n",
        ),
        (
            &["JSON.NUMINCRBY", "nokey", "$.n", "1"],
            b"-ERR could not perform this operation on a key that doesn't exist\r\n",
            b"-ERR could not perform this operation on a key that doesn't exist\r\n",
        ),
    ];
    for (args, resp2, resp3) in both {
        assert_wire!(c2.raw(args), *resp2, format!("RESP2 {args:?}"));
        assert_wire!(c3.raw(args), *resp3, format!("RESP3 {args:?}"));
    }

    // A legacy path adds to every number it matches. RESP2 replies with the
    // last new value, RESP3 with all of them.
    let doc = r#"{"a":{"x":1},"b":{"x":2.5},"c":{"x":"s"}}"#;
    let changed = b"$41\r\n{\"a\":{\"x\":2},\"b\":{\"x\":3.5},\"c\":{\"x\":\"s\"}}\r\n";
    c2.raw(&["JSON.SET", "n2", "$", doc]);
    assert_wire!(
        c2.raw(&["JSON.NUMINCRBY", "n2", "..x", "1"]),
        b"$3\r\n3.5\r\n",
        "RESP2 legacy NUMINCRBY"
    );
    assert_wire!(
        c2.raw(&["JSON.GET", "n2"]),
        changed,
        "after RESP2 NUMINCRBY"
    );
    c3.raw(&["JSON.SET", "n3", "$", doc]);
    assert_wire!(
        c3.raw(&["JSON.NUMINCRBY", "n3", "..x", "1"]),
        b"*3\r\n:2\r\n,3.5\r\n_\r\n",
        "RESP3 legacy NUMINCRBY"
    );
    assert_wire!(
        c3.raw(&["JSON.GET", "n3"]),
        changed,
        "after RESP3 NUMINCRBY"
    );
    assert_wire!(
        c3.raw(&["JSON.NUMMULTBY", "n3", ".b.x", "2"]),
        b"*1\r\n,7\r\n",
        "RESP3 legacy NUMMULTBY"
    );
    assert_wire!(
        c3.raw(&["JSON.NUMINCRBY", "n3", ".c.x", "1"]),
        b"*1\r\n_\r\n",
        "RESP3 legacy NUMINCRBY of a string"
    );
    assert_wire!(
        c3.raw(&["JSON.NUMINCRBY", "n3", ".nope", "1"]),
        b"*0\r\n",
        "RESP3 legacy NUMINCRBY of nothing"
    );
    for path in [".c.x", ".nope"] {
        assert_wire!(
            c2.raw(&["JSON.NUMINCRBY", "n2", path, "1"]),
            b"-ERR Path does not exist or does not contains a number\r\n",
            format!("RESP2 legacy NUMINCRBY {path}")
        );
    }
}
