// tests/parity.rs
//
// Behaviour that was documented as a gap against Redis and no longer is:
// RESET, per-command statistics, expiry notifications and the active expiry
// of hash fields, blocked reads that are woken when they should be,
// scripts that other clients wait for instead of being told BUSY, and
// subscribers that are never left out of the middle of a stream.
//
// One server is shared by the tests, each with its own keys; the one that
// needs a password starts a server of its own. (What happens when a script
// overruns its time limit is tested in `tests/backup.rs`, in a process of its
// own: the script engine is shared by every server in a process.)

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

// ── RESP client (RESP2 and RESP3) ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Double(f64),
    Arr(Vec<V>),
    Map(Vec<(V, V)>),
    Nil,
}

impl V {
    fn s(&self) -> &str {
        match self {
            V::Str(s) | V::Err(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
    fn arr(&self) -> &[V] {
        match self {
            V::Arr(v) => v,
            other => panic!("not an array: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
    }
    fn err(&self) -> &str {
        match self {
            V::Err(e) => e,
            other => panic!("not an error: {other:?}"),
        }
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(s) => {
                    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
                    let writer = s.try_clone().unwrap();
                    return Self {
                        reader: BufReader::new(s),
                        writer,
                    };
                }
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Err(e) => panic!("could not connect to port {port}: {e}"),
            }
        }
    }

    /// Arguments as bytes: a vector is not text.
    fn raw(&mut self, args: &[&[u8]]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "," => V::Double(rest.parse().unwrap()),
            "_" => V::Nil,
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Str(String::from_utf8_lossy(&data).into_owned())
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Arr((0..n).map(|_| self.read()).collect())
            }
            "%" => {
                let n: usize = rest.parse().unwrap();
                V::Map((0..n).map(|_| (self.read(), self.read())).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }
}

// ── Servers ───────────────────────────────────────────────────────────────────

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

fn start(requirepass: &str) -> u16 {
    let port = free_port();
    let requirepass = requirepass.to_string();
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            configfile: "./kividb.conf".to_string(),
            requirepass,
            ..Default::default()
        });
    });
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return port;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server on port {port} did not start");
}

fn shared() -> u16 {
    static PORT: OnceLock<u16> = OnceLock::new();
    *PORT.get_or_init(|| start(""))
}

fn conn() -> Conn {
    Conn::connect(shared())
}

/// One line of an `INFO` section.
fn info(c: &mut Conn, section: &str, field: &str) -> Option<String> {
    let reply = c.cmd(&["INFO", section]);
    reply
        .s()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")))
        .map(|v| v.trim().to_string())
}

/// The `name=value` fields of one `cmdstat_…` line.
fn cmdstat(c: &mut Conn, command: &str) -> Option<std::collections::HashMap<String, f64>> {
    let line = info(c, "commandstats", &format!("cmdstat_{command}"))?;
    Some(
        line.split(',')
            .filter_map(|pair| pair.split_once('='))
            .map(|(k, v)| (k.to_string(), v.parse::<f64>().unwrap()))
            .collect(),
    )
}

// ── RESET ─────────────────────────────────────────────────────────────────────

#[test]
fn reset_puts_a_connection_back_as_it_was_opened() {
    let mut c = conn();
    let mut other = conn();
    assert!(c.cmd(&["SELECT", "3"]).ok());
    assert!(c.cmd(&["CLIENT", "SETNAME", "before-reset"]).ok());
    assert!(matches!(c.cmd(&["HELLO", "3"]), V::Map(_)));
    assert!(c.cmd(&["WATCH", "reset:w"]).ok());
    assert!(c.cmd(&["MULTI"]).ok());
    assert_eq!(c.cmd(&["SET", "reset:queued", "1"]).s(), "QUEUED");

    assert_eq!(c.cmd(&["RESET"]), V::Str("RESET".into()));

    // The transaction is gone, and what was queued never ran.
    assert!(c.cmd(&["EXEC"]).err().contains("EXEC without MULTI"));
    assert_eq!(other.cmd(&["EXISTS", "reset:queued"]).i(), 0);
    // Database 0, no name, RESP2.
    assert!(c.cmd(&["SET", "reset:where", "here"]).ok());
    assert_eq!(other.cmd(&["GET", "reset:where"]).s(), "here");
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), V::Nil);
    assert!(matches!(c.cmd(&["HGETALL", "reset:nohash"]), V::Arr(_)));
    // The watch is gone: a transaction after a write to the key still runs.
    other.cmd(&["SET", "reset:w", "changed"]);
    assert!(c.cmd(&["MULTI"]).ok());
    c.cmd(&["PING"]);
    assert!(matches!(c.cmd(&["EXEC"]), V::Arr(_)));
}

/// On a server whose default user needs a password, RESET leaves the
/// connection unauthenticated.
#[test]
fn reset_forgets_who_the_connection_was() {
    let port = start("secret");
    let mut c = Conn::connect(port);
    assert!(c.cmd(&["GET", "k"]).err().starts_with("NOAUTH"));
    // RESET is allowed before AUTH.
    assert_eq!(c.cmd(&["RESET"]), V::Str("RESET".into()));
    assert!(c.cmd(&["AUTH", "secret"]).ok());
    assert_eq!(c.cmd(&["GET", "k"]), V::Nil);
    assert_eq!(c.cmd(&["RESET"]), V::Str("RESET".into()));
    assert!(c.cmd(&["GET", "k"]).err().starts_with("NOAUTH"));
    assert!(c.cmd(&["AUTH", "secret"]).ok());
    assert_eq!(c.cmd(&["GET", "k"]), V::Nil);
}

// ── INFO ──────────────────────────────────────────────────────────────────────

#[test]
fn info_reports_the_release_it_follows() {
    let mut c = conn();
    assert_eq!(
        info(&mut c, "server", "redis_version").as_deref(),
        Some("8.10.0")
    );
}

/// `INFO commandstats`: calls, time, and the calls that failed — for every
/// command, and per subcommand.
#[test]
fn commandstats_count_every_call() {
    let mut c = conn();
    let before = cmdstat(&mut c, "lolwut").map_or(0.0, |s| s["calls"]);
    for _ in 0..25 {
        c.cmd(&["LOLWUT"]);
    }
    let stats = cmdstat(&mut c, "lolwut").expect("a line for lolwut");
    assert_eq!(stats["calls"] - before, 25.0);
    for field in ["usec", "usec_per_call", "rejected_calls", "failed_calls"] {
        assert!(stats.contains_key(field), "no {field} in {stats:?}");
    }
    assert!(stats["usec"] >= 0.0);

    // A command that fails, and one refused before it runs.
    c.cmd(&["SET", "cs:str", "x"]);
    let before = cmdstat(&mut c, "llen").unwrap_or_default();
    assert!(matches!(c.cmd(&["LLEN", "cs:str"]), V::Err(_)));
    assert!(matches!(c.cmd(&["LLEN"]), V::Err(_)));
    let after = cmdstat(&mut c, "llen").unwrap();
    let grew = |field: &str| after[field] - before.get(field).copied().unwrap_or(0.0);
    assert_eq!(grew("calls"), 2.0);
    assert_eq!(grew("failed_calls"), 1.0);
    assert_eq!(grew("rejected_calls"), 1.0);

    // Subcommands are counted one by one.
    c.cmd(&["OBJECT", "ENCODING", "cs:str"]);
    assert!(cmdstat(&mut c, "object|encoding").unwrap()["calls"] >= 1.0);
    // Not a command: no line.
    c.cmd(&["NOSUCHCOMMAND"]);
    assert!(cmdstat(&mut c, "nosuchcommand").is_none());
}

// ── Expiry: notifications, and hash fields ────────────────────────────────────

/// A subscriber to every keyspace and keyevent channel of database 0.
fn events() -> Conn {
    let mut setup = conn();
    assert!(
        setup
            .cmd(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .ok()
    );
    let mut sub = conn();
    let reply = sub.cmd(&["PSUBSCRIBE", "__key*@0__:*"]);
    assert_eq!(reply.arr()[0].s(), "psubscribe");
    sub
}

/// Read events until `(channel, payload)` arrives.
fn expect_event(sub: &mut Conn, channel: &str, payload: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();
    while Instant::now() < deadline {
        let message = sub.read();
        let parts = message.arr();
        if parts[0].s() != "pmessage" {
            continue;
        }
        if parts[2].s() == channel && parts[3].s() == payload {
            return;
        }
        seen.push(format!("{} {}", parts[2].s(), parts[3].s()));
    }
    panic!("no {channel} {payload}; saw {seen:?}");
}

/// A key whose TTL runs out is announced — whether or not anybody reads it.
#[test]
fn a_key_that_expires_is_announced() {
    let mut sub = events();
    let mut c = conn();
    assert!(c.cmd(&["SET", "ev:untouched", "v", "PX", "150"]).ok());
    // Nobody touches the key: the background task finds it.
    expect_event(&mut sub, "__keyevent@0__:expired", "ev:untouched");

    assert!(c.cmd(&["SET", "ev:read", "v", "PX", "50"]).ok());
    thread::sleep(Duration::from_millis(60));
    assert_eq!(c.cmd(&["GET", "ev:read"]), V::Nil);
    expect_event(&mut sub, "__keyspace@0__:ev:read", "expired");
}

/// Hash fields with a TTL go when their time comes, without being read; a
/// hash with none left goes with them.
#[test]
fn hash_fields_expire_on_their_own() {
    let mut sub = events();
    let mut c = conn();
    let before: u64 = info(&mut c, "stats", "expired_subkeys")
        .unwrap()
        .parse()
        .unwrap();

    c.cmd(&["HSET", "hf:h", "a", "1", "b", "2", "c", "3"]);
    assert_eq!(
        c.cmd(&["HPEXPIRE", "hf:h", "150", "FIELDS", "1", "a"])
            .arr()[0]
            .i(),
        1
    );
    expect_event(&mut sub, "__keyevent@0__:hexpired", "hf:h");
    assert_eq!(c.cmd(&["HLEN", "hf:h"]).i(), 2);
    assert_eq!(c.cmd(&["HEXISTS", "hf:h", "a"]).i(), 0);

    // The rest of them: the hash itself is deleted.
    assert_eq!(
        c.cmd(&["HPEXPIRE", "hf:h", "150", "FIELDS", "2", "b", "c"])
            .arr()
            .len(),
        2
    );
    expect_event(&mut sub, "__keyevent@0__:del", "hf:h");
    assert_eq!(c.cmd(&["EXISTS", "hf:h"]).i(), 0);
    assert_eq!(c.cmd(&["TYPE", "hf:h"]).s(), "none");

    // A hash that is renamed is still found…
    c.cmd(&["HSET", "hf:old", "a", "1", "keep", "2"]);
    c.cmd(&["HPEXPIRE", "hf:old", "300", "FIELDS", "1", "a"]);
    assert!(c.cmd(&["RENAME", "hf:old", "hf:new"]).ok());
    expect_event(&mut sub, "__keyevent@0__:hexpired", "hf:new");
    assert_eq!(c.cmd(&["HKEYS", "hf:new"]).arr().len(), 1);
    // …and so is one made with HSETEX.
    assert_eq!(
        c.cmd(&["HSETEX", "hf:x", "PX", "150", "FIELDS", "1", "only", "v"])
            .i(),
        1
    );
    expect_event(&mut sub, "__keyevent@0__:del", "hf:x");
    assert_eq!(c.cmd(&["EXISTS", "hf:x"]).i(), 0);

    let after: u64 = info(&mut c, "stats", "expired_subkeys")
        .unwrap()
        .parse()
        .unwrap();
    assert!(after - before >= 5, "{before} -> {after}");
}

// ── Blocked reads ─────────────────────────────────────────────────────────────

/// `TS.READ … BLOCK` on a series that is deleted returns then, not at its
/// timeout.
#[test]
fn a_blocked_ts_read_ends_when_its_series_is_deleted() {
    let mut c = conn();
    assert!(c.cmd(&["TS.CREATE", "br:series"]).ok());
    let waiter = thread::spawn(|| {
        let mut w = conn();
        let started = Instant::now();
        let reply = w.cmd(&["TS.READ", "br:series", "$", "BLOCK", "15000", "1"]);
        (reply, started.elapsed())
    });
    thread::sleep(Duration::from_millis(400));
    assert_eq!(c.cmd(&["DEL", "br:series"]).i(), 1);
    let (reply, waited) = waiter.join().unwrap();
    assert_eq!(reply, V::Arr(vec![]));
    assert!(waited < Duration::from_secs(5), "waited {waited:?}");
}

/// `XREADGROUP … CLAIM … BLOCK` returns when a pending entry has been idle
/// for long enough to claim — it does not wait out its whole timeout.
#[test]
fn a_blocked_claim_ends_when_an_entry_becomes_claimable() {
    let mut c = conn();
    c.cmd(&["XADD", "bc:s", "1-0", "f", "v"]);
    assert!(c.cmd(&["XGROUP", "CREATE", "bc:s", "g", "0"]).ok());
    // Delivered to one consumer, who never acknowledges it.
    let first = c.cmd(&["XREADGROUP", "GROUP", "g", "idle", "STREAMS", "bc:s", ">"]);
    assert!(matches!(first, V::Arr(_)));

    let started = Instant::now();
    let reply = c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "worker",
        "BLOCK",
        "15000",
        "CLAIM",
        "600",
        "STREAMS",
        "bc:s",
        ">",
    ]);
    let waited = started.elapsed();
    let entries = reply.arr()[0].arr()[1].arr();
    assert_eq!(entries.len(), 1, "{reply:?}");
    assert_eq!(entries[0].arr()[0].s(), "1-0");
    assert!(
        waited >= Duration::from_millis(400) && waited < Duration::from_secs(6),
        "waited {waited:?}"
    );

    // A new entry still wakes it at once, and nothing claimable or new means
    // the client's own timeout.
    let waiter = thread::spawn(|| {
        let mut w = conn();
        let started = Instant::now();
        let reply = w.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "worker",
            "BLOCK",
            "15000",
            "CLAIM",
            "60000",
            "STREAMS",
            "bc:s",
            ">",
        ]);
        (reply, started.elapsed())
    });
    thread::sleep(Duration::from_millis(300));
    c.cmd(&["XADD", "bc:s", "2-0", "f", "v"]);
    let (reply, waited) = waiter.join().unwrap();
    assert_eq!(reply.arr()[0].arr()[1].arr()[0].arr()[0].s(), "2-0");
    assert!(waited < Duration::from_secs(5), "waited {waited:?}");

    let started = Instant::now();
    let reply = c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "worker",
        "BLOCK",
        "300",
        "CLAIM",
        "60000",
        "STREAMS",
        "bc:s",
        ">",
    ]);
    assert_eq!(reply, V::Nil);
    assert!(started.elapsed() >= Duration::from_millis(250));
}

// ── Scripts ───────────────────────────────────────────────────────────────────

/// While a script runs, other clients' commands wait for it. They used to be
/// answered `-BUSY` at once — as was a second script.
#[cfg(feature = "lua")]
#[test]
fn other_clients_wait_for_a_script_instead_of_being_told_busy() {
    let mut c = conn();
    c.cmd(&["SET", "sc:n", "0"]);
    // A script that takes a while: it counts, with a call into the server
    // on every turn.
    let slow =
        "for i = 1, 3000 do redis.call('INCR', KEYS[1]) end return redis.call('GET', KEYS[1])";

    let scripts: Vec<_> = (0..3)
        .map(|_| {
            thread::spawn(move || {
                let mut s = conn();
                for _ in 0..5 {
                    let reply = s.cmd(&["EVAL", slow, "1", "sc:n"]);
                    assert!(matches!(reply, V::Str(_)), "{reply:?}");
                }
            })
        })
        .collect();
    let plain = thread::spawn(|| {
        let mut p = conn();
        let mut seen = Vec::new();
        for _ in 0..300 {
            let reply = p.cmd(&["GET", "sc:n"]);
            assert!(matches!(reply, V::Str(_)), "{reply:?}");
            seen.push(reply.s().parse::<u64>().unwrap());
            let reply = p.cmd(&["PING"]);
            assert_eq!(reply, V::Str("PONG".into()));
        }
        seen
    });
    for s in scripts {
        s.join().unwrap();
    }
    let seen = plain.join().unwrap();
    assert_eq!(c.cmd(&["GET", "sc:n"]).s(), "45000");
    // A script is one step: nobody sees it part-way through its 3000.
    assert!(
        seen.iter().all(|n| n % 3000 == 0),
        "a reader saw a script half-run: {:?}",
        seen.iter().find(|n| *n % 3000 != 0)
    );
}

// ── Pub/Sub ───────────────────────────────────────────────────────────────────

fn message(channel: &str, payload: &str) -> V {
    V::Arr(vec![
        V::Str("message".into()),
        V::Str(channel.into()),
        V::Str(payload.into()),
    ])
}

/// A subscriber that is behind gets everything it is owed, in order. It used
/// to lose whatever it was more than 1024 messages behind by, without a word.
#[test]
fn a_subscriber_that_is_behind_misses_nothing() {
    let mut sub = conn();
    sub.cmd(&["SUBSCRIBE", "burst:a", "burst:b"]);
    sub.read();

    // All of it published before the subscriber reads any, across two of its
    // channels — the order between them is kept too.
    let total = 20_000;
    let mut publisher = conn();
    let mut pipeline = Vec::new();
    for i in 0..total {
        let (channel, payload) = (["burst:a", "burst:b"][i % 2], i.to_string());
        pipeline.extend_from_slice(
            format!(
                "*3\r\n$7\r\nPUBLISH\r\n${}\r\n{channel}\r\n${}\r\n{payload}\r\n",
                channel.len(),
                payload.len()
            )
            .as_bytes(),
        );
    }
    publisher.writer.write_all(&pipeline).unwrap();
    for _ in 0..total {
        assert_eq!(publisher.read(), V::Int(1));
    }

    for i in 0..total {
        assert_eq!(
            sub.read(),
            message(["burst:a", "burst:b"][i % 2], &i.to_string())
        );
    }
}

/// With subscriptions, a RESP2 connection may only manage them; without, it
/// is an ordinary connection again. It used to be closed instead.
#[test]
fn a_resp2_subscriber_is_an_ordinary_client_again_once_it_has_unsubscribed() {
    let mut c = conn();
    let id = c.cmd(&["CLIENT", "ID"]).i();
    c.cmd(&["SUBSCRIBE", "back:a"]);
    c.cmd(&["PSUBSCRIBE", "back:*"]);

    let refused = c.cmd(&["GET", "back:key"]);
    assert!(
        refused
            .err()
            .starts_with("ERR Can't execute 'get': only (P|S)SUBSCRIBE"),
        "{refused:?}"
    );
    assert_eq!(
        c.cmd(&["PING"]),
        V::Arr(vec![V::Str("pong".into()), V::Str(String::new())])
    );

    // What it is subscribed to is on its line of CLIENT LIST.
    let mut other = conn();
    let list = other.cmd(&["CLIENT", "LIST"]);
    let line = list
        .s()
        .lines()
        .find(|l| l.starts_with(&format!("id={id} ")))
        .expect("the subscriber's line")
        .to_string();
    assert!(line.contains(" sub=1 psub=1 ssub=0 "), "{line}");
    assert_eq!(
        other.cmd(&["PUBSUB", "NUMSUB", "back:a"]),
        V::Arr(vec![V::Str("back:a".into()), V::Int(1)])
    );

    // A message through both the channel and the pattern.
    assert_eq!(other.cmd(&["PUBLISH", "back:a", "hello"]), V::Int(2));
    assert_eq!(c.read(), message("back:a", "hello"));
    assert_eq!(
        c.read(),
        V::Arr(vec![
            V::Str("pmessage".into()),
            V::Str("back:*".into()),
            V::Str("back:a".into()),
            V::Str("hello".into()),
        ])
    );

    assert_eq!(
        c.cmd(&["UNSUBSCRIBE"]),
        V::Arr(vec![
            V::Str("unsubscribe".into()),
            V::Str("back:a".into()),
            V::Int(1)
        ])
    );
    assert_eq!(
        c.cmd(&["PUNSUBSCRIBE", "back:*"]),
        V::Arr(vec![
            V::Str("punsubscribe".into()),
            V::Str("back:*".into()),
            V::Int(0)
        ])
    );

    // The same connection, and everything is allowed again.
    assert!(c.cmd(&["SET", "back:key", "v"]).ok());
    assert_eq!(c.cmd(&["GET", "back:key"]), V::Str("v".into()));
    assert_eq!(c.cmd(&["PING"]), V::Str("PONG".into()));
    assert_eq!(c.cmd(&["CLIENT", "ID"]).i(), id);
    assert_eq!(other.cmd(&["PUBLISH", "back:a", "nobody"]), V::Int(0));
    // Unsubscribing from nothing names nothing.
    assert_eq!(
        c.cmd(&["UNSUBSCRIBE"]),
        V::Arr(vec![V::Str("unsubscribe".into()), V::Nil, V::Int(0)])
    );
}

/// A subscriber that does not read is disconnected when what it is owed
/// passes the output buffer limit — it is not left with a stream that has
/// holes in it, and the server does not keep its backlog for ever.
#[test]
fn a_subscriber_that_stops_reading_is_disconnected() {
    let mut sub = conn();
    sub.cmd(&["SUBSCRIBE", "flood"]);

    let mut publisher = conn();
    let megabyte = "m".repeat(1 << 20);
    let mut reached = 0;
    for _ in 0..200 {
        reached = publisher.cmd(&["PUBLISH", "flood", &megabyte]).i();
        if reached == 0 {
            break;
        }
    }
    assert_eq!(
        reached, 0,
        "200 MB were queued for a client that reads nothing"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let counts = publisher.cmd(&["PUBSUB", "NUMSUB", "flood"]);
        if counts == V::Arr(vec![V::Str("flood".into()), V::Int(0)]) {
            break;
        }
        assert!(Instant::now() < deadline, "still subscribed: {counts:?}");
        thread::sleep(Duration::from_millis(50));
    }
}
