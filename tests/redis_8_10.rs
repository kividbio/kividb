// tests/redis_8_10.rs
//
// Commands and options added between Redis 8.4 and 8.10, over the wire.
//
// Where a command page on redis.io gives an example, the example is the test:
// the same input, and the reply the page shows.
//
// One server is shared by every test in this file (each uses its own key
// names), plus a master/replica pair for the tests about propagation.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

// ── RESP client ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Simple(String),
    Bulk(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn s(&self) -> &str {
        match self {
            V::Simple(s) | V::Bulk(s) | V::Err(s) => s,
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
        matches!(self, V::Simple(s) if s == "OK")
    }
    fn is_err(&self) -> bool {
        matches!(self, V::Err(_))
    }
    /// An array of strings, as a `Vec<String>`.
    fn strings(&self) -> Vec<String> {
        self.arr().iter().map(|v| v.s().to_string()).collect()
    }
    /// The value following `name` in a flat `[name, value, ...]` array.
    fn field(&self, name: &str) -> &V {
        let items = self.arr();
        items
            .chunks(2)
            .find(|pair| matches!(&pair[0], V::Bulk(n) | V::Simple(n) if n == name))
            .map(|pair| &pair[1])
            .unwrap_or_else(|| panic!("no field {name} in {self:?}"))
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

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().unwrap()),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return V::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                V::Bulk(String::from_utf8_lossy(&data).into_owned())
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                V::Arr((0..n).map(|_| self.read()).collect())
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

fn start(port: u16, replicaof: Option<u16>) {
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            replicaof: replicaof.map(|p| vec!["127.0.0.1".to_string(), p.to_string()]),
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server on port {port} did not start");
}

/// A connection to the server every test in this file shares.
fn conn() -> Conn {
    static PORT: OnceLock<u16> = OnceLock::new();
    let port = *PORT.get_or_init(|| {
        let port = free_port();
        start(port, None);
        port
    });
    Conn::connect(port)
}

fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for: {what}");
}

// ── Lists: LMOVEM / BLMOVEM (8.10) ────────────────────────────────────────────

/// The example on the `LMOVEM` page.
#[test]
fn lmovem_moves_several_elements() {
    let mut c = conn();
    c.cmd(&["RPUSH", "lm:mylist", "1", "2", "3", "4"]);
    let moved = c.cmd(&[
        "LMOVEM",
        "lm:mylist",
        "lm:other",
        "LEFT",
        "LEFT",
        "COUNT",
        "2",
        "OBO",
    ]);
    assert_eq!(moved.strings(), ["2", "1"]);
    assert_eq!(
        c.cmd(&["LRANGE", "lm:mylist", "0", "-1"]).strings(),
        ["3", "4"]
    );
    assert_eq!(
        c.cmd(&["LRANGE", "lm:other", "0", "-1"]).strings(),
        ["2", "1"]
    );

    // EXACTLY is all or nothing.
    let none = c.cmd(&[
        "LMOVEM",
        "lm:mylist",
        "lm:other",
        "LEFT",
        "LEFT",
        "EXACTLY",
        "5",
        "BULK",
    ]);
    assert_eq!(none, V::Nil);
    assert_eq!(c.cmd(&["LLEN", "lm:mylist"]).i(), 2);

    // Same key, one by one: the moved elements are reversed in place.
    c.cmd(&["RPUSH", "lm:some", "a", "b", "c"]);
    let moved = c.cmd(&[
        "LMOVEM", "lm:some", "lm:some", "LEFT", "LEFT", "EXACTLY", "2", "OBO",
    ]);
    assert_eq!(moved.strings(), ["b", "a"]);
    assert_eq!(
        c.cmd(&["LRANGE", "lm:some", "0", "-1"]).strings(),
        ["b", "a", "c"]
    );
    // In bulk they keep their order, so the list is unchanged.
    let moved = c.cmd(&[
        "LMOVEM", "lm:some", "lm:some", "LEFT", "LEFT", "EXACTLY", "2", "BULK",
    ]);
    assert_eq!(moved.strings(), ["b", "a"]);
    assert_eq!(
        c.cmd(&["LRANGE", "lm:some", "0", "-1"]).strings(),
        ["b", "a", "c"]
    );

    // Without the how-many block it is LMOVE, replying with a one-element array.
    assert_eq!(
        c.cmd(&["LMOVEM", "lm:some", "lm:one", "RIGHT", "LEFT"])
            .strings(),
        ["c"]
    );
    // A missing source: an empty array, or nil with EXACTLY.
    assert_eq!(
        c.cmd(&["LMOVEM", "lm:nope", "lm:one", "LEFT", "LEFT"]),
        V::Arr(vec![])
    );
    assert_eq!(
        c.cmd(&[
            "LMOVEM", "lm:nope", "lm:one", "LEFT", "LEFT", "EXACTLY", "1", "OBO"
        ]),
        V::Nil
    );
    // The block needs both halves.
    assert!(
        c.cmd(&["LMOVEM", "lm:some", "lm:one", "LEFT", "LEFT", "COUNT", "2"])
            .is_err()
    );

    // A source that is emptied is deleted.
    c.cmd(&[
        "LMOVEM",
        "lm:mylist",
        "lm:other",
        "LEFT",
        "RIGHT",
        "COUNT",
        "10",
        "BULK",
    ]);
    assert_eq!(c.cmd(&["EXISTS", "lm:mylist"]).i(), 0);
    assert_eq!(
        c.cmd(&["LRANGE", "lm:other", "0", "-1"]).strings(),
        ["2", "1", "3", "4"]
    );
}

/// Moving to a key of the wrong type used to drop the element on the floor.
#[test]
fn a_move_to_a_non_list_leaves_the_source_alone() {
    let mut c = conn();
    c.cmd(&["SET", "lw:str", "x"]);
    c.cmd(&["RPUSH", "lw:list", "a", "b"]);
    for cmd in [
        vec!["LMOVE", "lw:list", "lw:str", "LEFT", "LEFT"],
        vec!["RPOPLPUSH", "lw:list", "lw:str"],
        vec![
            "LMOVEM", "lw:list", "lw:str", "LEFT", "LEFT", "COUNT", "2", "BULK",
        ],
        vec!["BLMOVE", "lw:list", "lw:str", "LEFT", "LEFT", "1"],
    ] {
        let reply = c.cmd(&cmd);
        assert!(reply.is_err(), "{cmd:?} -> {reply:?}");
    }
    assert_eq!(
        c.cmd(&["LRANGE", "lw:list", "0", "-1"]).strings(),
        ["a", "b"]
    );
}

#[test]
fn blmovem_waits_for_exactly_the_number_asked_for() {
    let mut pusher = conn();
    let waiter = thread::spawn(|| {
        let mut c = conn();
        c.cmd(&[
            "BLMOVEM", "bl:src", "bl:dst", "LEFT", "RIGHT", "10", "EXACTLY", "2", "BULK",
        ])
    });

    // One element is not enough to wake it for good: it must take nothing.
    thread::sleep(Duration::from_millis(400));
    pusher.cmd(&["RPUSH", "bl:src", "a"]);
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        pusher.cmd(&["LRANGE", "bl:src", "0", "-1"]).strings(),
        ["a"]
    );
    assert_eq!(pusher.cmd(&["EXISTS", "bl:dst"]).i(), 0);

    pusher.cmd(&["RPUSH", "bl:src", "b"]);
    assert_eq!(waiter.join().unwrap().strings(), ["a", "b"]);
    assert_eq!(
        pusher.cmd(&["LRANGE", "bl:dst", "0", "-1"]).strings(),
        ["a", "b"]
    );
    assert_eq!(pusher.cmd(&["EXISTS", "bl:src"]).i(), 0);

    // COUNT only waits for the list to be non-empty; a timeout is nil.
    let mut c = conn();
    let started = Instant::now();
    let reply = c.cmd(&[
        "BLMOVEM", "bl:empty", "bl:dst", "LEFT", "LEFT", "0.3", "COUNT", "5", "OBO",
    ]);
    assert_eq!(reply, V::Nil);
    assert!(started.elapsed() >= Duration::from_millis(250));

    // Inside MULTI it does not block.
    c.cmd(&["MULTI"]);
    c.cmd(&[
        "BLMOVEM", "bl:empty", "bl:dst", "LEFT", "LEFT", "0", "EXACTLY", "1", "OBO",
    ]);
    assert_eq!(c.cmd(&["EXEC"]).arr()[0], V::Nil);
}

// ── Sets: SUNIONCARD / SDIFFCARD (8.10) ───────────────────────────────────────

/// The examples on the `SUNIONCARD` and `SDIFFCARD` pages.
#[test]
fn set_cardinality_commands() {
    let mut c = conn();
    c.cmd(&["SADD", "sc:key1", "a", "b", "c"]);
    c.cmd(&["SADD", "sc:key2", "c", "d", "e"]);

    assert_eq!(c.cmd(&["SUNIONCARD", "2", "sc:key1", "sc:key2"]).i(), 5);
    assert_eq!(
        c.cmd(&["SUNIONCARD", "2", "sc:key1", "sc:key2", "LIMIT", "3"])
            .i(),
        3
    );
    assert_eq!(
        c.cmd(&["SUNIONCARD", "2", "sc:key1", "sc:key2", "APPROX"])
            .i(),
        5
    );
    assert_eq!(c.cmd(&["SUNIONCARD", "2", "sc:key1", "sc:nope"]).i(), 3);

    assert_eq!(c.cmd(&["SDIFFCARD", "2", "sc:key1", "sc:key2"]).i(), 2);
    assert_eq!(
        c.cmd(&["SDIFFCARD", "2", "sc:key1", "sc:key2", "LIMIT", "1"])
            .i(),
        1
    );
    assert_eq!(c.cmd(&["SDIFFCARD", "2", "sc:nope", "sc:key2"]).i(), 0);
    assert_eq!(c.cmd(&["SDIFFCARD", "2", "sc:key1", "sc:key1"]).i(), 0);

    c.cmd(&["SET", "sc:str", "x"]);
    assert!(
        c.cmd(&["SUNIONCARD", "2", "sc:key1", "sc:str"])
            .s()
            .starts_with("WRONGTYPE")
    );
    assert!(c.cmd(&["SDIFFCARD", "1", "sc:key1", "APPROX"]).is_err());
    assert!(c.cmd(&["SUNIONCARD", "0", "sc:key1"]).is_err());
    assert!(c.cmd(&["SUNIONCARD", "3", "sc:key1"]).is_err());
}

// ── Hashes: HIMPORT (8.10) ────────────────────────────────────────────────────

/// The example on the `HIMPORT` pages.
#[test]
fn himport_creates_hashes_from_a_prepared_fieldset() {
    let mut c = conn();
    assert!(
        c.cmd(&["HIMPORT", "PREPARE", "u", "name", "email", "age"])
            .ok()
    );
    assert!(
        c.cmd(&[
            "HIMPORT",
            "SET",
            "hi:user:1",
            "u",
            "alice",
            "a@example.com",
            "30"
        ])
        .ok()
    );
    assert!(
        c.cmd(&[
            "HIMPORT",
            "SET",
            "hi:user:2",
            "u",
            "bob",
            "b@example.com",
            "25"
        ])
        .ok()
    );
    assert_eq!(c.cmd(&["HGET", "hi:user:1", "name"]).s(), "alice");
    assert_eq!(c.cmd(&["HGET", "hi:user:1", "email"]).s(), "a@example.com");
    assert_eq!(c.cmd(&["HGET", "hi:user:2", "age"]).s(), "25");
    assert_eq!(c.cmd(&["HLEN", "hi:user:1"]).i(), 3);

    // It overwrites whatever was there, of any type, TTL included.
    c.cmd(&["SET", "hi:user:3", "a string", "EX", "100"]);
    assert!(
        c.cmd(&[
            "HIMPORT",
            "SET",
            "hi:user:3",
            "u",
            "c",
            "c@example.com",
            "1"
        ])
        .ok()
    );
    assert_eq!(c.cmd(&["TYPE", "hi:user:3"]).s(), "hash");
    assert_eq!(c.cmd(&["TTL", "hi:user:3"]).i(), -1);

    // The value count must match the fieldset, and the fieldset must exist.
    assert!(
        c.cmd(&["HIMPORT", "SET", "hi:x", "u", "only", "two"])
            .is_err()
    );
    assert!(
        c.cmd(&["HIMPORT", "SET", "hi:x", "nope", "a", "b", "c"])
            .is_err()
    );
    assert!(c.cmd(&["HIMPORT", "PREPARE", "dup", "f", "f"]).is_err());
    assert_eq!(c.cmd(&["EXISTS", "hi:x"]).i(), 0);

    // A fieldset belongs to its connection.
    let mut other = conn();
    assert!(
        other
            .cmd(&["HIMPORT", "SET", "hi:x", "u", "a", "b", "c"])
            .is_err()
    );

    c.cmd(&["HIMPORT", "PREPARE", "o", "id", "total"]);
    assert_eq!(c.cmd(&["HIMPORT", "DISCARD", "u"]).i(), 1);
    assert_eq!(c.cmd(&["HIMPORT", "DISCARD", "u"]).i(), 0);
    c.cmd(&["HIMPORT", "PREPARE", "u", "a"]);
    assert_eq!(c.cmd(&["HIMPORT", "DISCARDALL"]).i(), 2);
    // Hashes already created are unaffected.
    assert_eq!(c.cmd(&["HGET", "hi:user:1", "name"]).s(), "alice");
}

/// The Redis form of HSETEX — options first, TTL optional — and the older
/// KiviDB form with the TTL second, which is still accepted.
#[test]
fn hsetex_takes_its_ttl_as_an_option() {
    let mut c = conn();
    let far = "99999999999"; // a Unix time in seconds, comfortably in the future
    assert_eq!(
        c.cmd(&[
            "HSETEX", "hx:k", "EXAT", far, "FIELDS", "2", "f1", "Hello", "f2", "World"
        ])
        .i(),
        1
    );
    let ttls = c.cmd(&["HTTL", "hx:k", "FIELDS", "2", "f1", "f2"]);
    assert!(ttls.arr().iter().all(|t| t.i() > 1_000_000), "{ttls:?}");

    // FNX: none may exist. FXX: all must.
    assert_eq!(
        c.cmd(&[
            "HSETEX", "hx:k", "FNX", "EX", "60", "FIELDS", "2", "f1", "a", "f2", "b"
        ])
        .i(),
        0
    );
    assert_eq!(c.cmd(&["HGET", "hx:k", "f1"]).s(), "Hello");
    let both = c.cmd(&[
        "HSETEX", "hx:k", "FXX", "EX", "60", "KEEPTTL", "FIELDS", "1", "f1", "x",
    ]);
    assert_eq!(
        both.s(),
        "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified"
    );

    // KEEPTTL keeps each field's TTL; no option clears it.
    assert_eq!(
        c.cmd(&[
            "HSETEX", "hx:k", "FXX", "KEEPTTL", "FIELDS", "1", "f1", "hello"
        ])
        .i(),
        1
    );
    assert!(c.cmd(&["HTTL", "hx:k", "FIELDS", "1", "f1"]).arr()[0].i() > 1_000_000);
    assert_eq!(
        c.cmd(&["HSETEX", "hx:k", "FIELDS", "1", "f1", "plain"]).i(),
        1
    );
    assert_eq!(
        c.cmd(&["HTTL", "hx:k", "FIELDS", "1", "f1"]).arr()[0].i(),
        -1
    );
    assert_eq!(c.cmd(&["HGET", "hx:k", "f1"]).s(), "plain");

    assert_eq!(
        c.cmd(&["HSETEX", "hx:k", "PX", "100000", "FIELDS", "1", "f3", "v"])
            .i(),
        1
    );
    let ttl = c.cmd(&["HTTL", "hx:k", "FIELDS", "1", "f3"]).arr()[0].i();
    assert!((1..=100).contains(&ttl), "{ttl}");

    // A time already past: the field is set and gone.
    assert_eq!(
        c.cmd(&["HSETEX", "hx:k", "EXAT", "1", "FIELDS", "1", "gone", "v"])
            .i(),
        1
    );
    assert_eq!(c.cmd(&["HEXISTS", "hx:k", "gone"]).i(), 0);

    // FXX on a missing key sets nothing and creates nothing.
    assert_eq!(
        c.cmd(&[
            "HSETEX",
            "hx:missing",
            "FXX",
            "EX",
            "60",
            "FIELDS",
            "1",
            "f",
            "v"
        ])
        .i(),
        0
    );
    assert_eq!(c.cmd(&["EXISTS", "hx:missing"]).i(), 0);

    // The older form.
    assert_eq!(
        c.cmd(&["HSETEX", "hx:old", "300", "FIELDS", "1", "a", "1"])
            .i(),
        1
    );
    let ttl = c.cmd(&["HTTL", "hx:old", "FIELDS", "1", "a"]).arr()[0].i();
    assert!((1..=300).contains(&ttl), "{ttl}");
    assert!(
        c.cmd(&["HSETEX", "hx:old", "0", "FIELDS", "1", "a", "1"])
            .is_err()
    );
}

/// Subkey notifications (Redis 8.8): which fields a hash command touched.
#[test]
fn hash_subkey_notifications() {
    let mut c = conn();
    let mut sub = conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "ST"])
            .ok()
    );
    sub.cmd(&["PSUBSCRIBE", "__subkey*@0__:sk:*", "__subkeyevent@0__:*"]);
    sub.read(); // the second pattern's confirmation

    // Every message about `sk:` keys, as (channel, payload), until `count`.
    let mut take = |count: usize| -> Vec<(String, String)> {
        let mut seen: Vec<(String, String)> = Vec::new();
        while seen.len() < count {
            let msg = sub.read();
            let msg = msg.arr();
            assert_eq!(msg[0].s(), "pmessage");
            let (channel, payload) = (msg[2].s().to_string(), msg[3].s().to_string());
            // Other tests share this server; keep only our keys, and count a
            // message once though it may match both patterns.
            let ours = channel.contains("sk:") || payload.contains(":sk:");
            if ours && !seen.contains(&(channel.clone(), payload.clone())) {
                seen.push((channel, payload));
            }
        }
        seen
    };

    c.cmd(&["HSET", "sk:myhash", "field1", "val1", "field2", "val2"]);
    assert_eq!(
        take(2),
        [
            (
                "__subkeyspace@0__:sk:myhash".to_string(),
                "hset|6:field1,6:field2".to_string()
            ),
            (
                "__subkeyevent@0__:hset".to_string(),
                "9:sk:myhash|6:field1,6:field2".to_string()
            ),
        ]
    );

    // Only the fields that were actually deleted.
    c.cmd(&["HDEL", "sk:myhash", "field2", "nope"]);
    assert_eq!(take(2)[0].1, "hdel|6:field2");

    c.cmd(&["HINCRBY", "sk:myhash", "n", "5"]);
    assert_eq!(take(2)[0].1, "hincrby|1:n");

    c.cmd(&[
        "HEXPIRE",
        "sk:myhash",
        "100",
        "FIELDS",
        "2",
        "field1",
        "nope",
    ]);
    assert_eq!(take(2)[0].1, "hexpire|6:field1");

    c.cmd(&["HPERSIST", "sk:myhash", "FIELDS", "1", "field1"]);
    assert_eq!(take(2)[0].1, "hpersist|6:field1");

    c.cmd(&["HSETEX", "sk:myhash", "EX", "100", "FIELDS", "1", "t", "v"]);
    let events: Vec<String> = take(4).into_iter().map(|(_, p)| p).collect();
    assert!(events.contains(&"hset|1:t".to_string()), "{events:?}");
    assert!(events.contains(&"hexpire|1:t".to_string()), "{events:?}");

    c.cmd(&["HGETDEL", "sk:myhash", "FIELDS", "1", "t"]);
    assert_eq!(take(2)[0].1, "hdel|1:t");

    // The flags read back, and key-level events were never switched on.
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "notify-keyspace-events"]).arr()[1].s(),
        "ST"
    );
    assert!(c.cmd(&["CONFIG", "SET", "notify-keyspace-events", ""]).ok());
}

// ── Strings: DELEX / DIGEST / SET conditions (8.4, fixes through 8.10) ────────

#[test]
fn digest_is_the_documented_xxh3_and_conditions_use_it() {
    let mut c = conn();
    // The example on the `DIGEST` page.
    c.cmd(&["SET", "dg:key1", "Hello world"]);
    assert_eq!(c.cmd(&["DIGEST", "dg:key1"]).s(), "b6acb9d84a38ff74");

    assert_eq!(
        c.cmd(&["SET", "dg:key1", "x", "IFDEQ", "0000000000000000"]),
        V::Nil
    );
    assert!(
        c.cmd(&["SET", "dg:key1", "changed", "IFDEQ", "b6acb9d84a38ff74"])
            .ok()
    );
    assert_eq!(c.cmd(&["GET", "dg:key1"]).s(), "changed");

    c.cmd(&["SET", "dg:key2", "Hello world"]);
    assert_eq!(
        c.cmd(&["DELEX", "dg:key2", "IFDNE", "b6acb9d84a38ff74"])
            .i(),
        0
    );
    assert_eq!(
        c.cmd(&["DELEX", "dg:key2", "IFDEQ", "B6ACB9D84A38FF74"])
            .i(),
        1
    );
    assert_eq!(c.cmd(&["EXISTS", "dg:key2"]).i(), 0);
}

#[test]
fn delex_without_a_condition_deletes_any_type() {
    let mut c = conn();
    c.cmd(&["RPUSH", "dx:list", "a"]);
    assert_eq!(c.cmd(&["DELEX", "dx:list"]).i(), 1);
    assert_eq!(
        c.cmd(&["DELEX", "dx:list"]).i(),
        0,
        "a missing key is 0, not nil"
    );

    c.cmd(&["SET", "dx:s", "v"]);
    assert_eq!(c.cmd(&["DELEX", "dx:s", "IFEQ", "other"]).i(), 0);
    assert_eq!(c.cmd(&["DELEX", "dx:s", "IFNE", "other"]).i(), 1);
    assert_eq!(c.cmd(&["DELEX", "dx:missing", "IFEQ", "v"]).i(), 0);

    // A condition needs a string, and the errors are Redis 8.10's.
    c.cmd(&["RPUSH", "dx:list", "a"]);
    assert_eq!(
        c.cmd(&["DELEX", "dx:list", "IFEQ", "a"]),
        V::Err("ERR Key should be of string type if conditions are specified".into())
    );
    assert_eq!(
        c.cmd(&["DELEX", "dx:list", "IFEQ"]),
        V::Err("ERR wrong number of arguments for 'delex' command".into())
    );
    assert_eq!(
        c.cmd(&["DELEX", "dx:s2", "IFEQ", "a", "extra"]),
        V::Err("ERR wrong number of arguments for 'delex' command".into())
    );
    c.cmd(&["SET", "dx:s2", "v"]);
    assert_eq!(
        c.cmd(&["DELEX", "dx:s2", "MAYBE", "a"]),
        V::Err("ERR Invalid condition. Use IFEQ, IFNE, IFDEQ, or IFDNE".into())
    );
    for digest in ["", "abc", "0123456789abcdef0"] {
        assert_eq!(
            c.cmd(&["DELEX", "dx:s2", "IFDEQ", digest]),
            V::Err("ERR must be exactly 16 hexadecimal characters".into())
        );
        assert_eq!(
            c.cmd(&["SET", "dx:s2", "w", "IFDNE", digest]),
            V::Err("ERR must be exactly 16 hexadecimal characters".into())
        );
    }
}

/// MSETEX's replies and errors as Redis 8.10 has them, and GETRANGE's empty
/// range when both ends count from the end.
#[test]
fn msetex_and_getrange_follow_redis_8_10() {
    let mut c = conn();
    assert_eq!(
        c.cmd(&[
            "MSETEX", "2", "mx:{t}1", "a", "mx:{t}2", "b", "NX", "EX", "10"
        ])
        .i(),
        1
    );
    assert_eq!(
        c.cmd(&["MSETEX", "1", "mx:{t}1", "c", "NX", "EX", "10"])
            .i(),
        0
    );
    assert_eq!(
        c.cmd(&["MSETEX", "1", "mx:{t}none", "c", "XX", "EX", "10"])
            .i(),
        0
    );
    assert_eq!(c.cmd(&["GET", "mx:{t}1"]).s(), "a");
    for (args, error) in [
        (
            &["MSETEX", "k", "v", "EX", "10"][..],
            "ERR invalid numkeys value",
        ),
        (
            &["MSETEX", "2147483648", "k", "v"],
            "ERR invalid numkeys value",
        ),
        (
            &["MSETEX", "2", "k1", "v1", "k2"],
            "ERR wrong number of key-value pairs",
        ),
        (&["MSETEX", "1", "k", "v", "NX", "XX"], "ERR syntax error"),
        (
            &["MSETEX", "1", "k", "v", "EX", "10", "PX", "10"],
            "ERR syntax error",
        ),
        (
            &["MSETEX", "1", "k", "v", "KEEPTTL", "EX", "10"],
            "ERR syntax error",
        ),
        (&["MSETEX", "1", "k", "v", "BOGUS"], "ERR syntax error"),
    ] {
        assert_eq!(c.cmd(args), V::Err(error.into()), "{args:?}");
    }

    c.cmd(&["SET", "gr", "Hello World"]);
    assert_eq!(c.cmd(&["GETRANGE", "gr", "-100", "-101"]).s(), "");
    assert_eq!(c.cmd(&["GETRANGE", "gr", "-100", "-100"]).s(), "H");
    assert_eq!(c.cmd(&["GETRANGE", "gr", "-4", "-1"]).s(), "orld");
}

#[test]
fn set_conditions_are_mutually_exclusive() {
    let mut c = conn();
    c.cmd(&["SET", "sx:k", "v"]);
    for opts in [
        vec!["NX", "XX"],
        vec!["NX", "IFEQ", "v"],
        vec!["IFEQ", "v", "XX"],
        vec!["IFEQ", "v", "IFNE", "w"],
    ] {
        let mut cmd = vec!["SET", "sx:k", "new"];
        cmd.extend(opts);
        assert!(c.cmd(&cmd).is_err(), "{cmd:?}");
    }
    assert_eq!(c.cmd(&["GET", "sx:k"]).s(), "v");
    assert!(c.cmd(&["SET", "sx:k", "new", "IFEQ", "v"]).ok());
}

// ── Streams ───────────────────────────────────────────────────────────────────

#[test]
fn xadd_idempotent_production() {
    let mut c = conn();
    let first = c.cmd(&["XADD", "id:s", "IDMP", "producer-1", "msg-1", "*", "f", "v"]);
    // The retry gets the original entry's ID and adds nothing.
    let retry = c.cmd(&["XADD", "id:s", "IDMP", "producer-1", "msg-1", "*", "f", "v"]);
    assert_eq!(first, retry);
    assert_eq!(c.cmd(&["XLEN", "id:s"]).i(), 1);

    // A new idempotent ID, or the same one from another producer, is new.
    let second = c.cmd(&["XADD", "id:s", "IDMP", "producer-1", "msg-2", "*", "f", "v"]);
    let other = c.cmd(&["XADD", "id:s", "IDMP", "producer-2", "msg-1", "*", "f", "v"]);
    assert_ne!(first, second);
    assert_ne!(first, other);
    assert_eq!(c.cmd(&["XLEN", "id:s"]).i(), 3);

    // IDMPAUTO derives the ID from the content.
    let a = c.cmd(&["XADD", "id:s", "IDMPAUTO", "producer-3", "*", "temp", "21"]);
    let b = c.cmd(&["XADD", "id:s", "IDMPAUTO", "producer-3", "*", "temp", "21"]);
    let d = c.cmd(&["XADD", "id:s", "IDMPAUTO", "producer-3", "*", "temp", "22"]);
    assert_eq!(a, b);
    assert_ne!(a, d);
    assert_eq!(c.cmd(&["XLEN", "id:s"]).i(), 5);

    // Only with an auto-generated ID.
    assert!(
        c.cmd(&[
            "XADD",
            "id:s",
            "IDMP",
            "p",
            "m",
            "99999999999999-0",
            "f",
            "v"
        ])
        .is_err()
    );

    let info = c.cmd(&["XINFO", "STREAM", "id:s"]);
    assert_eq!(info.field("idmp-duration").i(), 100);
    assert_eq!(info.field("idmp-maxsize").i(), 100);
    assert_eq!(info.field("pids-tracked").i(), 3);
    assert_eq!(info.field("iids-tracked").i(), 5);
    assert_eq!(info.field("iids-added").i(), 5);
    assert_eq!(info.field("iids-duplicates").i(), 2);

    // XCFGSET changes the retention settings and forgets what was remembered.
    assert!(
        c.cmd(&[
            "XCFGSET",
            "id:s",
            "IDMP-DURATION",
            "300",
            "IDMP-MAXSIZE",
            "1000"
        ])
        .ok()
    );
    let info = c.cmd(&["XINFO", "STREAM", "id:s"]);
    assert_eq!(info.field("idmp-duration").i(), 300);
    assert_eq!(info.field("idmp-maxsize").i(), 1000);
    assert_eq!(info.field("pids-tracked").i(), 0);
    assert_eq!(
        c.cmd(&["XCFGSET", "id:s", "IDMP-DURATION", "0"]).s(),
        "ERR invalid duration"
    );
    assert_eq!(
        c.cmd(&["XCFGSET", "id:s", "IDMP-MAXSIZE", "10001"]).s(),
        "ERR invalid maxsize"
    );
    assert_eq!(
        c.cmd(&["XCFGSET", "id:nope", "IDMP-MAXSIZE", "10"]).s(),
        "ERR no such key"
    );
}

/// The examples on the `XREAD` page, with ten entries per stream.
#[test]
fn xread_cumulative_caps() {
    let mut c = conn();
    for stream in ["xr:s1", "xr:s2", "xr:s3"] {
        for n in 1..=10 {
            c.cmd(&["XADD", stream, &format!("{n}-1"), "f", "v"]);
        }
    }
    let total = |reply: &V| -> Vec<(String, usize)> {
        reply
            .arr()
            .iter()
            .map(|s| (s.arr()[0].s().to_string(), s.arr()[1].arr().len()))
            .collect()
    };
    let all = ["STREAMS", "xr:s1", "xr:s2", "xr:s3", "0", "0", "0"];
    let read = |c: &mut Conn, opts: &[&str]| {
        let mut cmd = vec!["XREAD"];
        cmd.extend_from_slice(opts);
        cmd.extend_from_slice(&all);
        c.cmd(&cmd)
    };

    let r = read(&mut c, &["COUNT", "5"]);
    assert_eq!(total(&r).iter().map(|(_, n)| n).sum::<usize>(), 15);

    // Capped across all streams, filling from the first.
    let r = read(&mut c, &["COUNT", "5", "MAXCOUNT", "8"]);
    assert_eq!(
        total(&r),
        [("xr:s1".to_string(), 5), ("xr:s2".to_string(), 3)]
    );

    let r = read(&mut c, &["MAXCOUNT", "7"]);
    assert_eq!(total(&r), [("xr:s1".to_string(), 7)]);

    let r = read(&mut c, &["MAXCOUNT", "5", "MAXSIZE", "100000"]);
    assert_eq!(total(&r), [("xr:s1".to_string(), 5)]);

    // MAXCOUNT cannot be below COUNT; both must be positive.
    assert!(read(&mut c, &["COUNT", "5", "MAXCOUNT", "4"]).is_err());
    assert!(read(&mut c, &["MAXCOUNT", "0"]).is_err());
    assert!(read(&mut c, &["MAXSIZE", "0"]).is_err());

    // A single entry larger than MAXSIZE is still returned.
    let big = "x".repeat(5000);
    c.cmd(&["XADD", "xr:big", "1-1", "f", &big]);
    c.cmd(&["XADD", "xr:big", "2-1", "f", &big]);
    let r = c.cmd(&["XREAD", "MAXSIZE", "50", "STREAMS", "xr:big", "0"]);
    assert_eq!(total(&r), [("xr:big".to_string(), 1)]);
    assert_eq!(r.arr()[0].arr()[1].arr()[0].arr()[0].s(), "1-1");
}

#[test]
fn xreadgroup_claim_and_caps() {
    let mut c = conn();
    for n in 1..=4 {
        c.cmd(&["XADD", "xg:s", &format!("{n}-1"), "f", "v"]);
    }
    assert!(c.cmd(&["XGROUP", "CREATE", "xg:s", "g", "0"]).ok());

    // c1 takes the first two.
    let r = c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c1",
        "COUNT",
        "2",
        "STREAMS",
        "xg:s",
        ">",
    ]);
    let entries = r.arr()[0].arr()[1].arr();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].arr().len(), 2, "plain entries are [id, fields]");

    // c2 claims what c1 left pending, then reads on. Claimed entries come
    // first and say how long they sat and how often they had been delivered.
    thread::sleep(Duration::from_millis(30));
    let r = c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c2",
        "COUNT",
        "3",
        "CLAIM",
        "10",
        "STREAMS",
        "xg:s",
        ">",
    ]);
    let entries = r.arr()[0].arr()[1].arr();
    assert_eq!(entries.len(), 3);
    let ids: Vec<&str> = entries.iter().map(|e| e.arr()[0].s()).collect();
    assert_eq!(ids, ["1-1", "2-1", "3-1"]);
    for claimed in &entries[..2] {
        assert_eq!(claimed.arr().len(), 4);
        assert!(claimed.arr()[2].i() >= 10, "idle ms: {claimed:?}");
        assert_eq!(claimed.arr()[3].i(), 1, "delivered once before");
    }
    assert_eq!(entries[2].arr()[2].i(), 0);
    assert_eq!(entries[2].arr()[3].i(), 0);

    // All three now belong to c2; the claimed two have been delivered twice.
    let pending = c.cmd(&["XPENDING", "xg:s", "g", "-", "+", "10"]);
    let rows: Vec<(String, String, i64)> = pending
        .arr()
        .iter()
        .map(|p| {
            let p = p.arr();
            (p[0].s().to_string(), p[1].s().to_string(), p[3].i())
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("1-1".to_string(), "c2".to_string(), 2),
            ("2-1".to_string(), "c2".to_string(), 2),
            ("3-1".to_string(), "c2".to_string(), 1),
        ]
    );

    // A history read names the stream even when nothing is pending.
    let r = c.cmd(&["XREADGROUP", "GROUP", "g", "c9", "STREAMS", "xg:s", "0"]);
    assert_eq!(r.arr()[0].arr()[0].s(), "xg:s");
    assert!(r.arr()[0].arr()[1].arr().is_empty());

    // MAXCOUNT bounds the delivery, and what is not delivered is not pending.
    let r = c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c3",
        "MAXCOUNT",
        "1",
        "STREAMS",
        "xg:s",
        ">",
    ]);
    assert_eq!(r.arr()[0].arr()[1].arr().len(), 1);
    assert_eq!(c.cmd(&["XPENDING", "xg:s", "g"]).arr()[0].i(), 4);

    // A missing group or key is an error, not "nothing new".
    let r = c.cmd(&["XREADGROUP", "GROUP", "nope", "c", "STREAMS", "xg:s", ">"]);
    assert!(r.s().starts_with("NOGROUP"), "{r:?}");
    let r = c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "STREAMS",
        "xg:missing",
        ">",
    ]);
    assert!(r.s().starts_with("NOGROUP"), "{r:?}");
}

#[test]
fn stream_trimming_reference_policies() {
    let mut c = conn();
    let setup = |c: &mut Conn, key: &str| {
        for n in 1..=4 {
            c.cmd(&["XADD", key, &format!("{n}-1"), "f", "v"]);
        }
        c.cmd(&["XGROUP", "CREATE", key, "g", "0"]);
        // Deliver the first two; acknowledge only the first.
        c.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "2",
            "STREAMS",
            key,
            ">",
        ]);
        c.cmd(&["XACK", key, "g", "1-1"]);
    };

    // KEEPREF (the default): entries go, pending references stay.
    setup(&mut c, "tr:keep");
    assert_eq!(c.cmd(&["XTRIM", "tr:keep", "MAXLEN", "1"]).i(), 3);
    assert_eq!(c.cmd(&["XPENDING", "tr:keep", "g"]).arr()[0].i(), 1);

    // DELREF: the references go too.
    setup(&mut c, "tr:del");
    assert_eq!(c.cmd(&["XTRIM", "tr:del", "MAXLEN", "1", "DELREF"]).i(), 3);
    assert_eq!(c.cmd(&["XPENDING", "tr:del", "g"]).arr()[0].i(), 0);

    // ACKED: only what every group has read and acknowledged — here, 1-1.
    setup(&mut c, "tr:ack");
    assert_eq!(c.cmd(&["XTRIM", "tr:ack", "MAXLEN", "0", "ACKED"]).i(), 1);
    assert_eq!(c.cmd(&["XLEN", "tr:ack"]).i(), 3);

    // The same options on XADD.
    setup(&mut c, "tr:add");
    c.cmd(&["XADD", "tr:add", "DELREF", "MAXLEN", "1", "5-1", "f", "v"]);
    assert_eq!(c.cmd(&["XLEN", "tr:add"]).i(), 1);
    assert_eq!(c.cmd(&["XPENDING", "tr:add", "g"]).arr()[0].i(), 0);

    // A threshold that is not a number is an error — not "trim to zero".
    assert!(c.cmd(&["XTRIM", "tr:keep", "MAXLEN", "1O0"]).is_err());
    assert_eq!(c.cmd(&["XLEN", "tr:keep"]).i(), 1);
}

// ── Sorted sets: AGGREGATE COUNT (8.8) ────────────────────────────────────────

#[test]
fn zset_count_aggregate_and_weights() {
    let mut c = conn();
    c.cmd(&["ZADD", "zc:1", "10", "a", "20", "b"]);
    c.cmd(&["ZADD", "zc:2", "5", "a", "7", "c"]);
    c.cmd(&["ZADD", "zc:3", "1", "a"]);

    // COUNT ignores scores: the score is how many inputs hold the member.
    let r = c.cmd(&[
        "ZUNION",
        "3",
        "zc:1",
        "zc:2",
        "zc:3",
        "AGGREGATE",
        "COUNT",
        "WITHSCORES",
    ]);
    assert_eq!(r.strings(), ["b", "1", "c", "1", "a", "3"]);
    // With WEIGHTS, each input contributes its weight.
    let r = c.cmd(&[
        "ZUNION",
        "3",
        "zc:1",
        "zc:2",
        "zc:3",
        "WEIGHTS",
        "1",
        "10",
        "100",
        "AGGREGATE",
        "COUNT",
        "WITHSCORES",
    ]);
    assert_eq!(r.strings(), ["b", "1", "c", "10", "a", "111"]);

    let r = c.cmd(&[
        "ZINTER",
        "2",
        "zc:1",
        "zc:2",
        "AGGREGATE",
        "COUNT",
        "WITHSCORES",
    ]);
    assert_eq!(r.strings(), ["a", "2"]);
    assert_eq!(
        c.cmd(&[
            "ZUNIONSTORE",
            "zc:out",
            "2",
            "zc:1",
            "zc:2",
            "AGGREGATE",
            "COUNT"
        ])
        .i(),
        3
    );
    assert_eq!(c.cmd(&["ZSCORE", "zc:out", "a"]).s(), "2");
    assert_eq!(
        c.cmd(&[
            "ZINTERSTORE",
            "zc:out",
            "2",
            "zc:1",
            "zc:2",
            "AGGREGATE",
            "COUNT"
        ])
        .i(),
        1
    );

    // Each weight belongs to its own key, whichever set is the smaller.
    let r = c.cmd(&[
        "ZINTER",
        "2",
        "zc:1",
        "zc:3",
        "WEIGHTS",
        "2",
        "3",
        "WITHSCORES",
    ]);
    assert_eq!(r.strings(), ["a", "23"]); // 10*2 + 1*3
    let r = c.cmd(&[
        "ZINTER",
        "2",
        "zc:3",
        "zc:1",
        "WEIGHTS",
        "2",
        "3",
        "WITHSCORES",
    ]);
    assert_eq!(r.strings(), ["a", "32"]); // 1*2 + 10*3

    // An empty result leaves no key behind.
    c.cmd(&["SET", "zc:gone", "x"]);
    assert_eq!(
        c.cmd(&["ZINTERSTORE", "zc:gone", "2", "zc:2", "zc:nope"])
            .i(),
        0
    );
    assert_eq!(c.cmd(&["EXISTS", "zc:gone"]).i(), 0);
}

// ── JSON: FPHA (8.8) ──────────────────────────────────────────────────────────

/// The example on the `JSON.SET` page.
#[test]
fn json_set_fpha() {
    let mut c = conn();
    assert!(
        c.cmd(&[
            "JSON.SET",
            "js:doc",
            "$",
            "[[1,2,3,4e3],[5,6.0,7,8]]",
            "FPHA",
            "FP16"
        ])
        .ok()
    );
    assert_eq!(
        c.cmd(&["JSON.GET", "js:doc", "$"]).s(),
        "[[[1.0,2.0,3.0,4000.0],[5.0,6.0,7.0,8.0]]]"
    );

    // A value too large for the type fails the command and changes nothing.
    let r = c.cmd(&["JSON.SET", "js:doc", "$", "[1e6]", "FPHA", "FP16"]);
    assert_eq!(r.s(), "ERR value out of range");
    assert_eq!(
        c.cmd(&["JSON.GET", "js:doc", "$"]).s(),
        "[[[1.0,2.0,3.0,4000.0],[5.0,6.0,7.0,8.0]]]"
    );
    assert!(
        c.cmd(&["JSON.SET", "js:big", "$", "[1e6]", "FPHA", "BF16"])
            .ok()
    );
    assert!(
        c.cmd(&["JSON.SET", "js:doc", "$", "[1]", "FPHA", "FP8"])
            .is_err()
    );
    assert!(c.cmd(&["JSON.SET", "js:doc", "$", "[1]", "FPHA"]).is_err());
}

// ── Search: aliases and FT.ALIASLIST (8.10) ───────────────────────────────────

#[test]
fn index_aliases() {
    let mut c = conn();
    let create = c.cmd(&[
        "FT.CREATE",
        "al:idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "al:doc:",
        "SCHEMA",
        "title",
        "TEXT",
    ]);
    assert!(create.ok(), "{create:?}");
    c.cmd(&["HSET", "al:doc:1", "title", "hello world"]);

    assert_eq!(c.cmd(&["FT.ALIASLIST", "al:idx"]), V::Arr(vec![]));
    assert!(c.cmd(&["FT.ALIASADD", "al:b", "al:idx"]).ok());
    assert!(c.cmd(&["FT.ALIASADD", "al:a", "al:idx"]).ok());
    assert_eq!(
        c.cmd(&["FT.ALIASLIST", "al:idx"]).strings(),
        ["al:a", "al:b"]
    );
    // Asked through an alias, it lists the index's aliases.
    assert_eq!(c.cmd(&["FT.ALIASLIST", "al:a"]).strings(), ["al:a", "al:b"]);
    assert!(c.cmd(&["FT.ALIASLIST", "al:nope"]).is_err());
    assert!(
        c.cmd(&["FT.ALIASADD", "al:a", "al:idx"]).is_err(),
        "already exists"
    );
    assert!(c.cmd(&["FT.ALIASADD", "al:c", "al:nope"]).is_err());

    // An alias is a name for the index, not a second index.
    let listed = c.cmd(&["FT._LIST"]).strings();
    assert!(listed.contains(&"al:idx".to_string()));
    assert!(
        !listed.iter().any(|n| n == "al:a" || n == "al:b"),
        "{listed:?}"
    );
    let by_name = c.cmd(&["FT.SEARCH", "al:idx", "hello"]);
    let by_alias = c.cmd(&["FT.SEARCH", "al:a", "hello"]);
    assert_eq!(by_name, by_alias);
    assert_eq!(by_alias.arr()[0].i(), 1);

    // A document added later is found through the alias too, and counted once.
    c.cmd(&["HSET", "al:doc:2", "title", "hello again"]);
    assert_eq!(c.cmd(&["FT.SEARCH", "al:b", "hello"]).arr()[0].i(), 2);
    let info = c.cmd(&["FT.INFO", "al:a"]);
    assert_eq!(info.field("index_name").s(), "al:idx");

    // ALIASDEL removes an alias — and only an alias.
    assert!(c.cmd(&["FT.ALIASDEL", "al:idx"]).is_err());
    assert!(c.cmd(&["FT.ALIASDEL", "al:b"]).ok());
    assert!(c.cmd(&["FT.ALIASDEL", "al:b"]).is_err());
    assert_eq!(c.cmd(&["FT.ALIASLIST", "al:idx"]).strings(), ["al:a"]);

    // ALIASUPDATE moves an alias; dropping an index takes its aliases along.
    c.cmd(&[
        "FT.CREATE",
        "al:idx2",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "al:other:",
        "SCHEMA",
        "t",
        "TEXT",
    ]);
    assert!(c.cmd(&["FT.ALIASUPDATE", "al:a", "al:idx2"]).ok());
    assert_eq!(c.cmd(&["FT.ALIASLIST", "al:idx"]), V::Arr(vec![]));
    assert_eq!(c.cmd(&["FT.ALIASLIST", "al:idx2"]).strings(), ["al:a"]);
    assert!(c.cmd(&["FT.DROPINDEX", "al:idx2"]).ok());
    assert!(c.cmd(&["FT.SEARCH", "al:a", "hello"]).is_err());
    assert!(c.cmd(&["FT.DROPINDEX", "al:idx"]).ok());
}

// ── Time series ───────────────────────────────────────────────────────────────

/// `[[ts, value, ...], ...]` as `(ts, [values])`.
fn rows(reply: &V) -> Vec<(i64, Vec<String>)> {
    reply
        .arr()
        .iter()
        .map(|row| {
            let row = row.arr();
            (
                row[0].i(),
                row[1..].iter().map(|v| v.s().to_string()).collect(),
            )
        })
        .collect()
}

fn row(ts: i64, values: &[&str]) -> (i64, Vec<String>) {
    (ts, values.iter().map(|v| v.to_string()).collect())
}

/// The examples on the `TS.RANGE` page.
#[test]
fn ts_range_filters_and_multiple_aggregators() {
    let mut c = conn();
    assert!(
        c.cmd(&["TS.CREATE", "tr:temp", "LABELS", "trtype", "temp"])
            .ok()
    );
    let added = c.cmd(&[
        "TS.MADD", "tr:temp", "1000", "30", "tr:temp", "1010", "35", "tr:temp", "1020", "9999",
        "tr:temp", "1030", "40",
    ]);
    assert_eq!(
        added,
        V::Arr(vec![V::Int(1000), V::Int(1010), V::Int(1020), V::Int(1030)])
    );
    assert_eq!(c.cmd(&["TS.ADD", "tr:temp", "1040", "41"]), V::Int(1040));

    let r = c.cmd(&[
        "TS.RANGE",
        "tr:temp",
        "-",
        "1030",
        "FILTER_BY_VALUE",
        "-100",
        "100",
    ]);
    assert_eq!(
        rows(&r),
        [row(1000, &["30"]), row(1010, &["35"]), row(1030, &["40"])]
    );
    // Timestamps are integers and values simple strings.
    assert_eq!(r.arr()[0].arr()[1], V::Simple("30".to_string()));

    let r = c.cmd(&[
        "TS.RANGE",
        "tr:temp",
        "-",
        "1030",
        "FILTER_BY_VALUE",
        "-100",
        "100",
        "AGGREGATION",
        "min,avg,max",
        "1000",
    ]);
    assert_eq!(rows(&r), [row(1000, &["30", "35", "40"])]);

    let r = c.cmd(&["TS.REVRANGE", "tr:temp", "-", "+", "COUNT", "2"]);
    assert_eq!(rows(&r), [row(1040, &["41"]), row(1030, &["40"])]);

    assert_eq!(c.cmd(&["TS.GET", "tr:temp"]).arr()[0], V::Int(1040));
    assert!(
        c.cmd(&[
            "TS.RANGE",
            "tr:temp",
            "-",
            "+",
            "AGGREGATION",
            "median",
            "10"
        ])
        .is_err()
    );
    assert!(c.cmd(&["TS.RANGE", "tr:nope", "-", "+"]).is_err());
}

#[test]
fn ts_nan_values_and_their_aggregators() {
    let mut c = conn();
    assert!(c.cmd(&["TS.CREATE", "tn:s"]).ok());
    c.cmd(&[
        "TS.MADD", "tn:s", "1000", "1", "tn:s", "1001", "NaN", "tn:s", "1002", "3",
    ]);
    let r = c.cmd(&["TS.RANGE", "tn:s", "-", "+"]);
    assert_eq!(
        rows(&r),
        [row(1000, &["1"]), row(1001, &["NaN"]), row(1002, &["3"])]
    );
    let r = c.cmd(&[
        "TS.RANGE",
        "tn:s",
        "-",
        "+",
        "AGGREGATION",
        "count,countNaN,countAll,avg,max",
        "1000",
    ]);
    assert_eq!(rows(&r), [row(1000, &["2", "1", "3", "2", "3"])]);
}

/// The examples on the `TS.NRANGE` page.
#[test]
fn ts_nrange_joins_series_by_timestamp() {
    let mut c = conn();
    assert!(c.cmd(&["TS.CREATE", "{nr}:1"]).ok());
    assert!(c.cmd(&["TS.CREATE", "{nr}:2"]).ok());
    c.cmd(&["TS.MADD", "{nr}:1", "1000", "10", "{nr}:1", "2000", "12"]);
    c.cmd(&["TS.MADD", "{nr}:2", "1000", "20", "{nr}:2", "3000", "25"]);

    let pivot = |reply: &V| -> Vec<(i64, Vec<String>)> {
        reply
            .arr()
            .iter()
            .map(|e| (e.arr()[0].i(), e.arr()[1].strings()))
            .collect()
    };
    let r = c.cmd(&["TS.NRANGE", "2", "{nr}:1", "{nr}:2", "-", "+"]);
    assert_eq!(
        pivot(&r),
        [
            row(1000, &["10", "20"]),
            row(2000, &["12", "NaN"]),
            row(3000, &["NaN", "25"]),
        ]
    );
    // The reverse form keeps the highest timestamps under COUNT.
    let r = c.cmd(&[
        "TS.NREVRANGE",
        "2",
        "{nr}:1",
        "{nr}:2",
        "-",
        "+",
        "COUNT",
        "2",
    ]);
    assert_eq!(
        pivot(&r),
        [row(3000, &["NaN", "25"]), row(2000, &["12", "NaN"])]
    );

    assert!(c.cmd(&["TS.CREATE", "{nr}:3"]).ok());
    assert!(c.cmd(&["TS.CREATE", "{nr}:4"]).ok());
    c.cmd(&[
        "TS.MADD", "{nr}:3", "1000", "10", "{nr}:3", "1100", "20", "{nr}:3", "2000", "30",
    ]);
    c.cmd(&[
        "TS.MADD", "{nr}:4", "1000", "5", "{nr}:4", "1100", "15", "{nr}:4", "2000", "25",
    ]);
    let r = c.cmd(&[
        "TS.NRANGE",
        "2",
        "{nr}:3",
        "{nr}:4",
        "-",
        "+",
        "AGGREGATION",
        "avg",
        "sum",
        "1000",
    ]);
    assert_eq!(
        pivot(&r),
        [row(1000, &["15", "20"]), row(2000, &["30", "25"])]
    );
    let r = c.cmd(&[
        "TS.NRANGE",
        "2",
        "{nr}:3",
        "{nr}:4",
        "-",
        "+",
        "AGGREGATION",
        "avg,max",
        "sum",
        "1000",
    ]);
    assert_eq!(
        pivot(&r),
        [
            row(1000, &["15", "20", "20"]),
            row(2000, &["30", "30", "25"])
        ]
    );

    // One aggregator argument per key; every key must exist.
    assert!(
        c.cmd(&[
            "TS.NRANGE",
            "2",
            "{nr}:3",
            "{nr}:4",
            "-",
            "+",
            "AGGREGATION",
            "avg",
            "1000"
        ])
        .is_err()
    );
    assert!(
        c.cmd(&["TS.NRANGE", "2", "{nr}:3", "{nr}:nope", "-", "+"])
            .is_err()
    );
    assert!(c.cmd(&["TS.NRANGE", "0", "-", "+"]).is_err());
}

/// The examples on the `TS.READ` page.
#[test]
fn ts_read_pages_and_blocks() {
    let mut c = conn();
    assert!(c.cmd(&["TS.CREATE", "rd:sensor"]).ok());
    for (ts, v) in [("100", "1.0"), ("200", "2.0"), ("300", "3.0")] {
        c.cmd(&["TS.ADD", "rd:sensor", ts, v]);
    }
    let all = [row(100, &["1"]), row(200, &["2"]), row(300, &["3"])];
    assert_eq!(rows(&c.cmd(&["TS.READ", "rd:sensor", "0"])), all);
    assert_eq!(rows(&c.cmd(&["TS.READ", "rd:sensor", "-"])), all);
    assert_eq!(
        rows(&c.cmd(&["TS.READ", "rd:sensor", "-", "MAX_COUNT", "2"])),
        all[..2]
    );
    assert_eq!(
        rows(&c.cmd(&["TS.READ", "rd:sensor", "201", "MAX_COUNT", "2"])),
        all[2..]
    );
    assert_eq!(rows(&c.cmd(&["TS.READ", "rd:sensor", "+"])), all[2..]);
    assert_eq!(c.cmd(&["TS.READ", "rd:sensor", "$"]), V::Arr(vec![]));
    assert_eq!(c.cmd(&["TS.READ", "rd:nope", "0"]), V::Arr(vec![]));

    // min_count cannot be reached: what there is comes back at the timeout.
    let started = Instant::now();
    let r = c.cmd(&["TS.READ", "rd:sensor", "101", "BLOCK", "300", "10"]);
    assert_eq!(rows(&r), all[1..]);
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert_eq!(
        c.cmd(&["TS.READ", "rd:sensor", "301", "BLOCK", "200", "1"]),
        V::Arr(vec![])
    );

    // `$` waits for samples added after the call — two of them, here.
    let waiter = thread::spawn(|| {
        let mut c = conn();
        c.cmd(&["TS.READ", "rd:sensor", "$", "BLOCK", "10000", "2"])
    });
    thread::sleep(Duration::from_millis(400));
    c.cmd(&["TS.ADD", "rd:sensor", "400", "4"]);
    thread::sleep(Duration::from_millis(200));
    c.cmd(&["TS.ADD", "rd:sensor", "500", "5"]);
    assert_eq!(
        rows(&waiter.join().unwrap()),
        [row(400, &["4"]), row(500, &["5"])]
    );

    assert!(
        c.cmd(&[
            "TS.READ",
            "rd:sensor",
            "0",
            "BLOCK",
            "10",
            "3",
            "MAX_COUNT",
            "2"
        ])
        .is_err()
    );
    assert!(c.cmd(&["TS.READ", "rd:sensor", "0", "MAX_COUNT"]).is_err());
    assert!(
        c.cmd(&["TS.READ", "rd:sensor", "0", "MAX_COUNT", "0"])
            .is_err()
    );
    // Blocking is refused where it cannot happen.
    c.cmd(&["MULTI"]);
    c.cmd(&["TS.READ", "rd:sensor", "$", "BLOCK", "0", "1"]);
    assert!(c.cmd(&["EXEC"]).arr()[0].is_err());
}

/// The examples on the `TS.QUERYLABELS` page (with label names of its own, as
/// the server is shared).
#[test]
fn ts_querylabels() {
    let mut c = conn();
    for (key, room, kind) in [
        ("ql:study:temperature", "study", "temperature"),
        ("ql:study:humidity", "study", "humidity"),
        ("ql:kitchen:temperature", "kitchen", "temperature"),
        ("ql:kitchen:humidity", "kitchen", "humidity"),
    ] {
        assert!(
            c.cmd(&["TS.CREATE", key, "LABELS", "qlroom", room, "qltype", kind])
                .ok()
        );
    }
    let r = c.cmd(&["TS.QUERYLABELS", "LABELS", "FILTER", "qlroom=kitchen"]);
    assert_eq!(r.strings(), ["qlroom", "qltype"]);
    let r = c.cmd(&["TS.QUERYLABELS", "VALUES", "qltype"]);
    assert_eq!(r.strings(), ["humidity", "temperature"]);
    let r = c.cmd(&[
        "TS.QUERYLABELS",
        "VALUES",
        "qlroom",
        "FILTER",
        "qltype=humidity",
        "qlroom!=study",
    ]);
    assert_eq!(r.strings(), ["kitchen"]);
    assert_eq!(
        c.cmd(&["TS.QUERYLABELS", "VALUES", "ql-no-such-label"]),
        V::Arr(vec![])
    );
    assert!(c.cmd(&["TS.QUERYLABELS", "VALUES"]).is_err());
    assert!(c.cmd(&["TS.QUERYLABELS", "NAMES"]).is_err());
}

#[test]
fn ts_mrange_excludeempty_and_groupby() {
    let mut c = conn();
    c.cmd(&[
        "TS.CREATE",
        "mr:a",
        "LABELS",
        "mrgroup",
        "x",
        "mrhost",
        "h1",
    ]);
    c.cmd(&[
        "TS.CREATE",
        "mr:b",
        "LABELS",
        "mrgroup",
        "x",
        "mrhost",
        "h2",
    ]);
    c.cmd(&[
        "TS.CREATE",
        "mr:c",
        "LABELS",
        "mrgroup",
        "x",
        "mrhost",
        "h1",
    ]);
    c.cmd(&["TS.MADD", "mr:a", "1000", "1", "mr:a", "2000", "2"]);
    c.cmd(&["TS.MADD", "mr:c", "1000", "10", "mr:c", "3000", "30"]);

    // Every matching series is reported, the empty one with no samples...
    let r = c.cmd(&["TS.MRANGE", "-", "+", "FILTER", "mrgroup=x"]);
    let names: Vec<&str> = r.arr().iter().map(|s| s.arr()[0].s()).collect();
    assert_eq!(names, ["mr:a", "mr:b", "mr:c"]);
    assert!(r.arr()[1].arr()[2].arr().is_empty());
    // ...unless it is excluded.
    let r = c.cmd(&["TS.MRANGE", "-", "+", "FILTER", "mrgroup=x", "EXCLUDEEMPTY"]);
    let names: Vec<&str> = r.arr().iter().map(|s| s.arr()[0].s()).collect();
    assert_eq!(names, ["mr:a", "mr:c"]);
    assert_eq!(
        rows(&r.arr()[0].arr()[2]),
        [row(1000, &["1"]), row(2000, &["2"])]
    );

    // Several aggregators per series.
    let r = c.cmd(&[
        "TS.MRANGE",
        "-",
        "+",
        "AGGREGATION",
        "min,max",
        "5000",
        "FILTER",
        "mrgroup=x",
        "EXCLUDEEMPTY",
    ]);
    assert_eq!(rows(&r.arr()[0].arr()[2]), [row(0, &["1", "2"])]);

    // GROUPBY reduces across the series sharing a label value.
    let r = c.cmd(&[
        "TS.MRANGE",
        "-",
        "+",
        "WITHLABELS",
        "FILTER",
        "mrgroup=x",
        "GROUPBY",
        "mrhost",
        "REDUCE",
        "sum",
    ]);
    let h1 = &r.arr()[0];
    assert_eq!(h1.arr()[0].s(), "mrhost=h1");
    let labels: Vec<Vec<String>> = h1.arr()[1].arr().iter().map(|p| p.strings()).collect();
    assert_eq!(
        labels,
        [
            vec!["mrhost", "h1"],
            vec!["__reducer__", "sum"],
            vec!["__source__", "mr:a,mr:c"],
        ]
    );
    assert_eq!(
        rows(&h1.arr()[2]),
        [row(1000, &["11"]), row(2000, &["2"]), row(3000, &["30"])]
    );

    let both = c.cmd(&[
        "TS.MRANGE",
        "-",
        "+",
        "FILTER",
        "mrgroup=x",
        "GROUPBY",
        "mrhost",
        "REDUCE",
        "sum",
        "EXCLUDEEMPTY",
    ]);
    assert!(both.is_err());
}

/// A compaction is the aggregate of its bucket, however the samples arrive.
#[test]
fn ts_compaction_rules_aggregate_whole_buckets() {
    let mut c = conn();
    for key in ["cp:src", "cp:avg", "cp:count", "cp:nan"] {
        assert!(c.cmd(&["TS.CREATE", key]).ok());
    }
    assert!(
        c.cmd(&[
            "TS.CREATERULE",
            "cp:src",
            "cp:avg",
            "AGGREGATION",
            "avg",
            "1000"
        ])
        .ok()
    );
    assert!(
        c.cmd(&[
            "TS.CREATERULE",
            "cp:src",
            "cp:count",
            "AGGREGATION",
            "count",
            "1000"
        ])
        .ok()
    );
    assert!(
        c.cmd(&[
            "TS.CREATERULE",
            "cp:src",
            "cp:nan",
            "AGGREGATION",
            "countNaN",
            "1000"
        ])
        .ok()
    );

    c.cmd(&["TS.ADD", "cp:src", "1000", "10"]);
    c.cmd(&["TS.ADD", "cp:src", "1100", "20"]);
    c.cmd(&["TS.MADD", "cp:src", "1200", "60", "cp:src", "1300", "NaN"]);
    assert_eq!(
        rows(&c.cmd(&["TS.RANGE", "cp:avg", "-", "+"])),
        [row(1000, &["30"])]
    );
    assert_eq!(
        rows(&c.cmd(&["TS.RANGE", "cp:count", "-", "+"])),
        [row(1000, &["3"])]
    );
    assert_eq!(
        rows(&c.cmd(&["TS.RANGE", "cp:nan", "-", "+"])),
        [row(1000, &["1"])]
    );

    c.cmd(&["TS.ADD", "cp:src", "2500", "7"]);
    assert_eq!(
        rows(&c.cmd(&["TS.RANGE", "cp:avg", "-", "+"])),
        [row(1000, &["30"]), row(2000, &["7"])]
    );

    // A zero bucket would be a division by zero on the next sample.
    assert!(c.cmd(&["TS.CREATE", "cp:zero"]).ok());
    assert!(
        c.cmd(&[
            "TS.CREATERULE",
            "cp:src",
            "cp:zero",
            "AGGREGATION",
            "avg",
            "0"
        ])
        .is_err()
    );
    assert!(
        c.cmd(&[
            "TS.CREATERULE",
            "cp:src",
            "cp:src",
            "AGGREGATION",
            "avg",
            "10"
        ])
        .is_err()
    );
    assert!(
        c.cmd(&[
            "TS.CREATERULE",
            "cp:src",
            "cp:missing",
            "AGGREGATION",
            "avg",
            "10"
        ])
        .is_err()
    );
}

// ── WATCH sees every key a write touches ──────────────────────────────────────

#[test]
fn watch_is_dirtied_by_writes_to_any_of_a_commands_keys() {
    // Its own server: one of the writes below is a FLUSHDB.
    let port = free_port();
    start(port, None);
    let mut watcher = Conn::connect(port);
    let mut writer = Conn::connect(port);
    let mut attempt = |watched: &str, write: &[&str]| -> V {
        watcher.cmd(&["WATCH", watched]);
        writer.cmd(write);
        watcher.cmd(&["MULTI"]);
        watcher.cmd(&["SET", "wt:result", "x"]);
        watcher.cmd(&["EXEC"])
    };
    // The watched key is not the first argument in any of these.
    assert_eq!(attempt("wt:b", &["MSET", "wt:a", "1", "wt:b", "2"]), V::Nil);
    assert_eq!(attempt("wt:dst", &["RENAME", "wt:a", "wt:dst"]), V::Nil);
    Conn::connect(port).cmd(&["RPUSH", "wt:l1", "x"]);
    assert_eq!(
        attempt("wt:l2", &["LMOVE", "wt:l1", "wt:l2", "LEFT", "LEFT"]),
        V::Nil
    );
    assert_eq!(attempt("wt:b", &["FLUSHDB"]), V::Nil);
    // An unrelated write leaves the transaction alone.
    assert!(matches!(
        attempt("wt:b", &["SET", "wt:unrelated", "1"]),
        V::Arr(_)
    ));
}

// ── ACL changes reach connections that are already open ──────────────────────

/// Revoking a permission used to leave every client already logged in as that
/// user with the access it had — and a transaction queued before the
/// revocation ran after it (the Redis 8.10.2 security fix).
#[test]
fn acl_changes_apply_to_open_connections_and_queued_transactions() {
    let port = free_port();
    start(port, None);
    let mut admin = Conn::connect(port);
    assert!(
        admin
            .cmd(&["ACL", "SETUSER", "alice", "on", ">pw", "+@all", "~*"])
            .ok()
    );

    let mut alice = Conn::connect(port);
    assert!(alice.cmd(&["AUTH", "alice", "pw"]).ok());
    assert!(alice.cmd(&["SET", "app:k", "1"]).ok());
    assert!(alice.cmd(&["SET", "other", "1"]).ok());

    // A command is revoked while alice is connected.
    assert!(admin.cmd(&["ACL", "SETUSER", "alice", "-set"]).ok());
    let reply = alice.cmd(&["SET", "app:k", "2"]);
    assert!(
        reply.is_err() && reply.s().starts_with("NOPERM"),
        "{reply:?}"
    );
    assert_eq!(alice.cmd(&["GET", "app:k"]).s(), "1");

    // And given back; then her keys are narrowed.
    assert!(admin.cmd(&["ACL", "SETUSER", "alice", "+set"]).ok());
    assert!(alice.cmd(&["SET", "app:k", "2"]).ok());
    // (After `~*` a narrower pattern is refused, as in Redis: it would add
    // nothing. The patterns are reset first.)
    assert!(admin.cmd(&["ACL", "SETUSER", "alice", "~app:*"]).is_err());
    assert!(
        admin
            .cmd(&["ACL", "SETUSER", "alice", "resetkeys", "~app:*"])
            .ok()
    );
    let reply = alice.cmd(&["GET", "other"]);
    assert!(
        reply.is_err() && reply.s().starts_with("NOPERM"),
        "{reply:?}"
    );
    assert_eq!(alice.cmd(&["GET", "app:k"]).s(), "2");
    assert!(admin.cmd(&["ACL", "SETUSER", "alice", "~*"]).ok());

    // A transaction queued while permitted, executed after the revocation:
    // the queued write is refused, the rest runs.
    assert!(alice.cmd(&["MULTI"]).ok());
    assert_eq!(alice.cmd(&["SET", "app:k", "3"]).s(), "QUEUED");
    assert_eq!(alice.cmd(&["GET", "app:k"]).s(), "QUEUED");
    assert!(admin.cmd(&["ACL", "SETUSER", "alice", "-set"]).ok());
    let results = alice.cmd(&["EXEC"]);
    let results = results.arr();
    assert!(
        results[0].is_err() && results[0].s().starts_with("NOPERM"),
        "{results:?}"
    );
    assert_eq!(results[1].s(), "2");
    assert_eq!(admin.cmd(&["GET", "app:k"]).s(), "2");

    // The same with the single-SET shape, which EXEC applies on a fast path.
    assert!(admin.cmd(&["ACL", "SETUSER", "alice", "+set"]).ok());
    assert!(alice.cmd(&["MULTI"]).ok());
    assert_eq!(alice.cmd(&["SET", "app:k", "4"]).s(), "QUEUED");
    assert!(admin.cmd(&["ACL", "SETUSER", "alice", "-set"]).ok());
    let results = alice.cmd(&["EXEC"]);
    assert!(results.arr()[0].is_err(), "{results:?}");
    assert_eq!(admin.cmd(&["GET", "app:k"]).s(), "2");

    // A deleted user's connections are closed.
    assert_eq!(admin.cmd(&["ACL", "DELUSER", "alice"]).i(), 1);
    alice.send(&["PING"]);
    let mut line = String::new();
    let closed = matches!(alice.reader.read_line(&mut line), Ok(0) | Err(_));
    assert!(closed, "expected the connection to be closed, got {line:?}");
}

// ── Propagation to a replica ──────────────────────────────────────────────────

#[test]
fn new_commands_reach_a_replica_as_their_effects() {
    let master_port = free_port();
    start(master_port, None);
    let replica_port = free_port();
    start(replica_port, Some(master_port));
    let mut m = Conn::connect(master_port);
    let mut r = Conn::connect(replica_port);
    wait_until("the replica to sync", || {
        r.cmd(&["INFO", "replication"])
            .s()
            .contains("master_link_status:up")
    });

    // An idempotent XADD and its retry: one entry, same ID, on both sides.
    let id = m.cmd(&["XADD", "rp:s", "IDMP", "p", "m1", "*", "f", "v"]);
    assert_eq!(
        m.cmd(&["XADD", "rp:s", "IDMP", "p", "m1", "*", "f", "v"]),
        id
    );
    // A hash built from a session-local fieldset.
    m.cmd(&["HIMPORT", "PREPARE", "u", "name", "age"]);
    m.cmd(&["HIMPORT", "SET", "rp:h", "u", "alice", "30"]);
    // A multi-element move, blocking and not.
    m.cmd(&["RPUSH", "rp:l", "1", "2", "3", "4"]);
    m.cmd(&[
        "LMOVEM", "rp:l", "rp:l2", "LEFT", "RIGHT", "COUNT", "2", "BULK",
    ]);
    m.cmd(&[
        "BLMOVEM", "rp:l", "rp:l3", "LEFT", "RIGHT", "1", "COUNT", "5", "OBO",
    ]);
    // A consumer-group delivery, then a claim by another consumer.
    m.cmd(&["XADD", "rp:g", "1-1", "f", "v"]);
    m.cmd(&["XADD", "rp:g", "2-1", "f", "v"]);
    m.cmd(&["XGROUP", "CREATE", "rp:g", "grp", "0"]);
    m.cmd(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c1",
        "COUNT",
        "1",
        "STREAMS",
        "rp:g",
        ">",
    ]);
    m.cmd(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "c2",
        "CLAIM",
        "0",
        "STREAMS",
        "rp:g",
        ">",
    ]);
    // Samples stamped with the master's clock.
    m.cmd(&["TS.ADD", "rp:ts", "*", "1.5"]);
    m.cmd(&["TS.INCRBY", "rp:counter", "5"]);

    assert_eq!(m.cmd(&["WAIT", "1", "5000"]).i(), 1);

    assert_eq!(r.cmd(&["XLEN", "rp:s"]).i(), 1);
    assert_eq!(r.cmd(&["XRANGE", "rp:s", "-", "+"]).arr()[0].arr()[0], id);
    // The replica remembers the pair too: after a failover the retry would
    // still be recognised.
    let info = r.cmd(&["XINFO", "STREAM", "rp:s"]);
    assert_eq!(info.field("iids-tracked").i(), 1);

    assert_eq!(r.cmd(&["HGET", "rp:h", "name"]).s(), "alice");
    assert_eq!(r.cmd(&["HGET", "rp:h", "age"]).s(), "30");

    assert_eq!(r.cmd(&["LRANGE", "rp:l2", "0", "-1"]).strings(), ["1", "2"]);
    assert_eq!(r.cmd(&["LRANGE", "rp:l3", "0", "-1"]).strings(), ["3", "4"]);
    assert_eq!(r.cmd(&["EXISTS", "rp:l"]).i(), 0);

    // The pending list is identical: owner, delivery count and delivery time.
    let pending = |c: &mut Conn| -> Vec<(String, String, i64)> {
        c.cmd(&["XPENDING", "rp:g", "grp", "-", "+", "10"])
            .arr()
            .iter()
            .map(|p| {
                let p = p.arr();
                (p[0].s().to_string(), p[1].s().to_string(), p[3].i())
            })
            .collect()
    };
    let on_master = pending(&mut m);
    assert_eq!(
        on_master,
        [
            ("1-1".to_string(), "c2".to_string(), 2),
            ("2-1".to_string(), "c2".to_string(), 1),
        ]
    );
    assert_eq!(pending(&mut r), on_master);
    let group = |c: &mut Conn| {
        c.cmd(&["XINFO", "GROUPS", "rp:g"]).arr()[0]
            .field("last-delivered-id")
            .s()
            .to_string()
    };
    assert_eq!(group(&mut r), group(&mut m));

    assert_eq!(r.cmd(&["TS.GET", "rp:ts"]), m.cmd(&["TS.GET", "rp:ts"]));
    assert_eq!(
        r.cmd(&["TS.GET", "rp:counter"]),
        m.cmd(&["TS.GET", "rp:counter"])
    );
}

/// Writes that give a TTL as "this long from now", in every spelling, and the
/// keys (and hash fields) they leave with one.
const RELATIVE_TTL_WRITES: &[&[&str]] = &[
    &["SET", "x:set-ex", "v", "EX", "1000"],
    &["SET", "x:set-px", "v", "PX", "1000000"],
    &["SET", "x:set-ifeq", "EX"],
    &["SET", "x:set-ifeq", "v", "IFEQ", "EX", "PX", "1000000"],
    &["SETEX", "x:setex", "1000", "v"],
    &["PSETEX", "x:psetex", "1000000", "v"],
    &["SET", "x:expire", "v"],
    &["EXPIRE", "x:expire", "1000"],
    &["SET", "x:pexpire", "v"],
    &["PEXPIRE", "x:pexpire", "1000000"],
    &["SET", "x:getex", "v"],
    &["GETEX", "x:getex", "EX", "1000"],
    &["MSETEX", "2", "x:{m}1", "v", "x:{m}2", "v", "EX", "1000"],
    &["INCREX", "x:increx", "BYINT", "5", "EX", "1000"],
    &["INCREX", "x:increx", "BYINT", "5", "EX", "5000", "ENX"],
    &["HSET", "x:hash", "a", "1", "b", "2"],
    &["HEXPIRE", "x:hash", "1000", "FIELDS", "1", "a"],
    &["HPEXPIRE", "x:hash", "1000000", "FIELDS", "1", "b"],
    &["HSETEX", "x:hash", "EX", "1000", "FIELDS", "1", "c", "3"],
    &["HSET", "x:hash", "d", "4"],
    &["HGETEX", "x:hash", "PX", "1000000", "FIELDS", "1", "d"],
    // A TTL that is not in the future deletes the key there and then.
    &["SET", "x:gone-1", "v"],
    &["EXPIRE", "x:gone-1", "-1"],
    &["SET", "x:gone-2", "v"],
    &["PEXPIRE", "x:gone-2", "0"],
    // Taking a TTL away.
    &["SET", "x:persist", "v", "EX", "1000"],
    &["GETEX", "x:persist", "PERSIST"],
];

const KEYS_WITH_A_TTL: &[&str] = &[
    "x:set-ex",
    "x:set-px",
    "x:set-ifeq",
    "x:setex",
    "x:psetex",
    "x:expire",
    "x:pexpire",
    "x:getex",
    "x:{m}1",
    "x:{m}2",
    "x:increx",
];

/// When each key, and each field of the hash, expires: Unix milliseconds.
fn expiry_times(c: &mut Conn) -> Vec<i64> {
    let mut times: Vec<i64> = KEYS_WITH_A_TTL
        .iter()
        .map(|key| c.cmd(&["PEXPIRETIME", key]).i())
        .collect();
    let fields = c.cmd(&["HPEXPIRETIME", "x:hash", "FIELDS", "4", "a", "b", "c", "d"]);
    times.extend(fields.arr().iter().map(|t| t.i()));
    times
}

/// The same moments, give or take the time one command takes to run.
fn assert_same_expiries(expected: &[i64], actual: &[i64]) {
    assert_eq!(expected.len(), actual.len());
    for (i, (want, got)) in expected.iter().zip(actual).enumerate() {
        assert!(*want > 0, "entry {i} has no TTL: {expected:?}");
        assert!(
            (want - got).abs() < 250,
            "entry {i} expires at {got}, expected {want}"
        );
    }
}

/// A relative TTL reaches the replica as the moment of expiry, so both sides
/// agree on it however far behind the replica is.
#[test]
fn relative_ttls_reach_a_replica_as_absolute_times() {
    let master_port = free_port();
    start(master_port, None);
    let mut m = Conn::connect(master_port);
    for write in RELATIVE_TTL_WRITES {
        let reply = m.cmd(write);
        assert!(!reply.is_err(), "{write:?}: {reply:?}");
    }
    // The ENX form left the first TTL alone.
    assert_eq!(m.cmd(&["GET", "x:increx"]).s(), "10");
    assert!(m.cmd(&["TTL", "x:increx"]).i() <= 1000);
    let on_master = expiry_times(&mut m);

    // A replica that attaches later gets the same moments from the snapshot,
    // and from the stream for what is written after.
    thread::sleep(Duration::from_millis(1200));
    let replica_port = free_port();
    start(replica_port, Some(master_port));
    let mut r = Conn::connect(replica_port);
    wait_until("the replica to sync", || {
        r.cmd(&["INFO", "replication"])
            .s()
            .contains("master_link_status:up")
    });
    assert_same_expiries(&on_master, &expiry_times(&mut r));

    assert!(m.cmd(&["FLUSHALL"]).ok());
    thread::sleep(Duration::from_millis(1200));
    for write in RELATIVE_TTL_WRITES {
        m.cmd(write);
    }
    let on_master = expiry_times(&mut m);
    assert_eq!(m.cmd(&["WAIT", "1", "5000"]).i(), 1);
    assert_same_expiries(&on_master, &expiry_times(&mut r));
    assert_eq!(r.cmd(&["GET", "x:increx"]).s(), "10");
    assert_eq!(r.cmd(&["EXISTS", "x:gone-1", "x:gone-2"]).i(), 0);
    assert_eq!(r.cmd(&["TTL", "x:persist"]).i(), -1);
    assert_eq!(r.cmd(&["DBSIZE"]).i(), m.cmd(&["DBSIZE"]).i());
}

// ── JSON: JSONPath (8.10 extensions) ──────────────────────────────────────────

const BIKES: &str = r#"{"inventory":{"mountain_bikes":[
    {"id":"bike:1","model":"Phoebe","price":1920,"specs":{"material":"carbon","weight":13.1},"colors":["black","silver"]},
    {"id":"bike:2","model":"Quaoar","price":2072,"specs":{"material":"aluminium","weight":7.9},"colors":["black","white"]},
    {"id":"bike:3","model":"Weywot","price":3264,"specs":{"material":"alloy","weight":13.8}}],
  "commuter_bikes":[
    {"id":"bike:4","model":"Salacia","price":1475,"specs":{"material":"aluminium","weight":16.6},"colors":["black","silver"]},
    {"id":"bike:5","model":"Mimas","price":3941,"specs":{"material":"alloy","weight":11.6}}]}}"#;

/// The examples on the JSON path page that read and update the bike inventory.
#[test]
fn jsonpath_selects_filters_and_updates() {
    let mut c = conn();
    assert!(c.cmd(&["JSON.SET", "jp:bikes", "$", BIKES]).ok());
    let get = |c: &mut Conn, path: &str| c.cmd(&["JSON.GET", "jp:bikes", path]).s().to_string();

    let models = r#"["Phoebe","Quaoar","Weywot"]"#;
    assert_eq!(get(&mut c, "$.inventory.mountain_bikes[*].model"), models);
    assert_eq!(
        get(&mut c, r#"$.inventory["mountain_bikes"][*].model"#),
        models
    );
    assert_eq!(get(&mut c, "$..mountain_bikes[*].model"), models);
    assert_eq!(
        get(&mut c, "$..model"),
        r#"["Phoebe","Quaoar","Weywot","Salacia","Mimas"]"#
    );
    assert_eq!(
        get(&mut c, "$..mountain_bikes[0:2].model"),
        r#"["Phoebe","Quaoar"]"#
    );
    assert_eq!(
        get(
            &mut c,
            "$..mountain_bikes[?(@.price < 3000 && @.specs.weight < 10)].model"
        ),
        r#"["Quaoar"]"#
    );
    assert_eq!(
        get(&mut c, r#"$..[?(@.specs.material == "alloy")].model"#),
        r#"["Weywot","Mimas"]"#
    );
    assert_eq!(
        get(&mut c, r#"$..[?(@.specs.material =~ "(?i)al")].model"#),
        r#"["Quaoar","Weywot","Salacia","Mimas"]"#
    );
    // An object keeps its members in the order they were written.
    assert!(
        get(&mut c, "$.inventory.mountain_bikes[2]")
            .starts_with(r#"[{"id":"bike:3","model":"Weywot","price":3264"#)
    );

    // Updates through a path that matches several values.
    assert_eq!(get(&mut c, "$..price"), "[1920,2072,3264,1475,3941]");
    assert_eq!(
        c.cmd(&["JSON.NUMINCRBY", "jp:bikes", "$..price", "-100"])
            .s(),
        "[1820,1972,3164,1375,3841]"
    );
    assert_eq!(
        c.cmd(&["JSON.NUMINCRBY", "jp:bikes", "$..price", "100"])
            .s(),
        "[1920,2072,3264,1475,3941]"
    );
    assert!(
        c.cmd(&[
            "JSON.SET",
            "jp:bikes",
            "$.inventory.*[?(@.price<2000)].price",
            "1500"
        ])
        .ok()
    );
    assert_eq!(get(&mut c, "$..price"), "[1500,2072,3264,1500,3941]");
    let appended = c.cmd(&[
        "JSON.ARRAPPEND",
        "jp:bikes",
        "$.inventory.*[?(@.price<2000)].colors",
        r#""pink""#,
    ]);
    assert_eq!(
        appended.arr().iter().map(|n| n.i()).collect::<Vec<_>>(),
        [3, 3]
    );
    assert_eq!(
        get(&mut c, "$..[*].colors"),
        r#"[["black","silver","pink"],["black","white"],["black","silver","pink"]]"#
    );
    // One entry per match, nil for a match of another type.
    let lens = c.cmd(&["JSON.ARRLEN", "jp:bikes", "$.inventory.mountain_bikes[0].*"]);
    let lens: Vec<bool> = lens.arr().iter().map(|v| matches!(v, V::Int(_))).collect();
    assert_eq!(lens, [false, false, false, false, true]);
    // Every bike without colours, deleted in one go.
    assert_eq!(
        c.cmd(&["JSON.DEL", "jp:bikes", "$..[?(!@.colors && @.id)]"])
            .i(),
        2
    );
    assert_eq!(get(&mut c, "$..id"), r#"["bike:1","bike:2","bike:4"]"#);
    assert_eq!(
        c.cmd(&["JSON.TYPE", "jp:bikes", "$.inventory.*"]).strings(),
        ["array", "array"]
    );
    assert_eq!(
        c.cmd(&["JSON.TYPE", "jp:bikes", ".inventory"]).s(),
        "object"
    );
}

/// The operator and function examples from the same page, through `JSON.GET`.
#[test]
fn jsonpath_operators_functions_and_projections() {
    let mut c = conn();
    let mut check = |doc: &str, cases: &[(&str, &str)]| {
        assert!(c.cmd(&["JSON.SET", "jp:doc", "$", doc]).ok(), "{doc}");
        for (path, expected) in cases {
            assert_eq!(
                c.cmd(&["JSON.GET", "jp:doc", path]).s(),
                *expected,
                "{path}"
            );
        }
    };
    check(
        r#"[{"a":1,"b":1},{"b":2},{"a":1},{"c":3}]"#,
        &[
            ("$[?!@.a]", r#"[{"b":2},{"c":3}]"#),
            ("$[?!(@.a==1)]", r#"[{"b":2},{"c":3}]"#),
            ("$[?!@.a && @.b]", r#"[{"b":2}]"#),
        ],
    );
    check(
        r#"{"arrs":[[1],[2],[1,2],[1,[2]]],"objs":[{"x":1},{"x":2},{"y":1}]}"#,
        &[
            ("$.arrs[?(@ == [1])]", "[[1]]"),
            ("$.arrs[?(@ == [1,[2]])]", "[[1,[2]]]"),
            (r#"$.objs[?(@ == {"x":1})]"#, r#"[{"x":1}]"#),
        ],
    );
    check(
        r#"[{"a":2,"b":3},{"a":5,"b":2}]"#,
        &[
            ("$[?@.a + 1 == 3]", r#"[{"a":2,"b":3}]"#),
            ("$[?@.a + @.b * 2 == 8]", r#"[{"a":2,"b":3}]"#),
            ("$[?(@.a + @.b) * 2 == 10]", r#"[{"a":2,"b":3}]"#),
        ],
    );
    check(
        r#"{"a":[1,2,3,4],"allow":[2,3]}"#,
        &[
            ("$.a[?@ in [2,4]]", "[2,4]"),
            ("$.a[?@ nin [2,4]]", "[1,3]"),
            ("$.a[?@ in $.allow]", "[2,3]"),
        ],
    );
    check(
        r#"{"a":[[1,2],[1,5],[]]}"#,
        &[("$.a[?@ subsetof [1,2,3]]", "[[1,2],[]]")],
    );
    check(
        r#"{"a":[[1,9],[8,9],[]]}"#,
        &[("$.a[?@ anyof [1,2,3]]", "[[1,9]]")],
    );
    check(
        r#"{"a":[[4,5],[1,9],[]]}"#,
        &[("$.a[?@ noneof [1,2,3]]", "[[4,5],[]]")],
    );
    check(
        r#"{"a":[[4,5],[1],[7,8,9]]}"#,
        &[("$.a[?@ sizeof 2]", "[[4,5]]")],
    );
    check(
        r#"{"a":[[],[1],"",[2,3],{},{"k":1}]}"#,
        &[
            ("$.a[?@ empty true]", r#"[[],"",{}]"#),
            ("$.a[?@ empty false]", r#"[[1],[2,3],{"k":1}]"#),
        ],
    );
    check(
        r#"{"obj":{"x":1,"y":2},"books":[{"t":"a"},{"t":"b"}]}"#,
        &[
            ("$.obj~", r#"["x","y"]"#),
            ("$~", r#"["obj","books"]"#),
            ("$.books~", "[]"),
            ("$.obj.keys()", r#"["x","y"]"#),
            ("$.obj.keys().count()", "[2]"),
        ],
    );
    check(
        r#"{"a":[[1,2,3],[1],"abcd","x"]}"#,
        &[("$.a[?length(@) > 2]", r#"[[1,2,3],"abcd"]"#)],
    );
    check(
        r#"[{"a":1,"b":2,"c":3},{"a":1}]"#,
        &[("$[?count(@.*) == 3]", r#"[{"a":1,"b":2,"c":3}]"#)],
    );
    check(
        r#"[{"a":1},{"a":2}]"#,
        &[("$[?value(@.a) == 1]", r#"[{"a":1}]"#)],
    );
    check(
        r#"{"a":["abc","xabc","a","b"]}"#,
        &[(r#"$.a[?match(@, "a.*")]"#, r#"["abc","a"]"#)],
    );
    check(
        r#"{"a":["abc","xyz","b"]}"#,
        &[(r#"$.a[?search(@, "b")]"#, r#"["abc","b"]"#)],
    );
    check(
        r#"{"a":[{"x":"a","y":"b"},{"x":"a","y":"c"}]}"#,
        &[(
            r#"$.a[?concat(@.x, @.y) == "ab"]"#,
            r#"[{"x":"a","y":"b"}]"#,
        )],
    );
    check(
        r#"{"a":[2.1,3.9,1.0]}"#,
        &[("$.a[?ceiling(@) == 3]", "[2.1]")],
    );
    check(
        r#"{"a":[2.1,2.9,3.5]}"#,
        &[("$.a[?floor(@) == 2]", "[2.1,2.9]")],
    );
    check(
        r#"{"a":[{"n":-5},{"n":5},{"n":-3}]}"#,
        &[("$.a[?abs(@.n) == 5]", r#"[{"n":-5},{"n":5}]"#)],
    );
    check(
        r#"{"a":[{"n":[1,2]},{"n":[9,8]}]}"#,
        &[
            ("$.a[?first(@.n) == 1]", r#"[{"n":[1,2]}]"#),
            ("$.a[?last(@.n) == 8]", r#"[{"n":[9,8]}]"#),
            ("$.a[?index(@.n, -1) == 2]", r#"[{"n":[1,2]}]"#),
        ],
    );
    check(
        r#"{"a":[{"n":[3,1,2]},{"n":[5,6]}]}"#,
        &[
            ("$.a[?sum(@.n) == 6]", r#"[{"n":[3,1,2]}]"#),
            ("$.a[?avg(@.n) == 2]", r#"[{"n":[3,1,2]}]"#),
        ],
    );
    check(r#"{"arr":[1,2,3]}"#, &[("$.arr.append(9)", "[1,2,3,9]")]);
    check(
        r#"{"books":[{"t":"a","price":30},{"t":"b","price":5}]}"#,
        &[(
            r#"$.books[?(@.price >= 10)].append({"t":"X"})"#,
            r#"[{"t":"a","price":30},{"t":"X"}]"#,
        )],
    );
    check(
        r#"{"a":2,"b":4,"arr":[1,2,3]}"#,
        &[
            ("$.a + 1", "[3]"),
            ("$.a * $.b", "[8]"),
            ("($.a + $.b) / 2", "[3.0]"),
            ("$.arr.length()", "[3]"),
            ("length($.arr)", "[3]"),
            ("$.a / 0", "[]"),
        ],
    );

    // Several paths, projections among them: one member each.
    let both = c.cmd(&["JSON.GET", "jp:doc", "$.a + 1", "$.b"]);
    assert_eq!(both.s(), r#"{"$.a + 1":[3],"$.b":[4]}"#);
    // JSON.MGET evaluates the projection for each key; a missing key is nil.
    assert!(c.cmd(&["JSON.SET", "jp:doc2", "$", r#"{"a":10}"#]).ok());
    let each = c.cmd(&["JSON.MGET", "jp:doc", "jp:doc2", "jp:none", "$.a + 1"]);
    assert_eq!(each.arr()[0].s(), "[3]");
    assert_eq!(each.arr()[1].s(), "[11]");
    assert!(matches!(each.arr()[2], V::Nil));
    // A projection computes; it is not somewhere a value can be written.
    assert!(c.cmd(&["JSON.SET", "jp:doc", "$.a + 1", "5"]).is_err());
    assert!(c.cmd(&["JSON.DEL", "jp:doc", "$.arr.length()"]).is_err());
    // A path that cannot be parsed is an error, not an empty result.
    assert!(c.cmd(&["JSON.GET", "jp:doc", "$.obj~.x"]).is_err());
    assert!(c.cmd(&["JSON.GET", "jp:doc", "$[?nosuch(@)]"]).is_err());
}

#[test]
fn json_commands_follow_the_kind_of_path() {
    let mut c = conn();
    let doc =
        r#"{"name":"kivi","n":1,"flag":true,"tags":["a","b"],"nested":{"tags":["x"],"n":2.5}}"#;
    assert!(c.cmd(&["JSON.SET", "jk:doc", "$", doc]).ok());

    // No path, and a legacy path: the value itself.
    assert_eq!(c.cmd(&["JSON.GET", "jk:doc"]).s(), doc);
    assert_eq!(
        c.cmd(&["JSON.GET", "jk:doc", ".nested.tags"]).s(),
        r#"["x"]"#
    );
    assert_eq!(c.cmd(&["JSON.GET", "jk:doc", "nested.n"]).s(), "2.5");
    assert_eq!(
        c.cmd(&["JSON.GET", "jk:doc", r#"["nested"]["tags"][-1]"#])
            .s(),
        r#""x""#
    );
    assert_eq!(
        c.cmd(&["JSON.GET", "jk:doc", ".n", ".name"]).s(),
        r#"{".n":1,".name":"kivi"}"#
    );
    assert!(c.cmd(&["JSON.GET", "jk:doc", ".nope"]).is_err());
    assert_eq!(c.cmd(&["JSON.GET", "jk:doc", "$.nope"]).s(), "[]");
    assert_eq!(c.cmd(&["JSON.TYPE", "jk:doc"]).s(), "object");
    assert_eq!(c.cmd(&["JSON.OBJLEN", "jk:doc"]).i(), 5);
    assert_eq!(c.cmd(&["JSON.ARRLEN", "jk:doc", ".tags"]).i(), 2);
    assert_eq!(
        c.cmd(&["JSON.OBJKEYS", "jk:doc", ".nested"]).strings(),
        ["tags", "n"]
    );

    // Formatting.
    assert_eq!(
        c.cmd(&[
            "JSON.GET", "jk:doc", "INDENT", "  ", "NEWLINE", "\n", "SPACE", " ", ".nested"
        ])
        .s(),
        "{\n  \"tags\": [\n    \"x\"\n  ],\n  \"n\": 2.5\n}"
    );

    // A JSONPath: one entry per match.
    let lens = c.cmd(&["JSON.ARRLEN", "jk:doc", "$..tags"]);
    assert_eq!(lens.arr().iter().map(|n| n.i()).collect::<Vec<_>>(), [2, 1]);
    let appended = c.cmd(&["JSON.ARRAPPEND", "jk:doc", "$..tags", r#""z""#]);
    assert_eq!(
        appended.arr().iter().map(|n| n.i()).collect::<Vec<_>>(),
        [3, 2]
    );
    let popped = c.cmd(&["JSON.ARRPOP", "jk:doc", "$..tags"]);
    assert_eq!(popped.strings(), [r#""z""#, r#""z""#]);
    let toggled = c.cmd(&["JSON.TOGGLE", "jk:doc", "$.*"]);
    let toggled: Vec<Option<i64>> = toggled
        .arr()
        .iter()
        .map(|v| if let V::Int(n) = v { Some(*n) } else { None })
        .collect();
    assert_eq!(toggled, [None, None, Some(0), None, None]);
    assert_eq!(c.cmd(&["JSON.TOGGLE", "jk:doc", ".flag"]).s(), "true");
    assert_eq!(
        c.cmd(&["JSON.NUMINCRBY", "jk:doc", "$..n", "2"]).s(),
        "[3,4.5]"
    );
    assert_eq!(c.cmd(&["JSON.NUMMULTBY", "jk:doc", ".n", "2"]).s(), "6");
    assert!(c.cmd(&["JSON.NUMINCRBY", "jk:doc", ".name", "2"]).is_err());
    assert_eq!(
        c.cmd(&["JSON.NUMINCRBY", "jk:doc", "$.name", "2"]).s(),
        "[null]"
    );
    assert_eq!(
        c.cmd(&["JSON.STRAPPEND", "jk:doc", ".name", r#""db""#]).i(),
        6
    );
    assert_eq!(c.cmd(&["JSON.STRLEN", "jk:doc", "$.name"]).arr()[0].i(), 6);

    // Setting: an existing value, a new member, and paths that lead nowhere.
    assert!(c.cmd(&["JSON.SET", "jk:doc", "$.nested.added", "1"]).ok());
    assert!(matches!(
        c.cmd(&["JSON.SET", "jk:doc", "$.nested.added", "2", "NX"]),
        V::Nil
    ));
    assert!(matches!(
        c.cmd(&["JSON.SET", "jk:doc", "$.nested.other", "2", "XX"]),
        V::Nil
    ));
    assert!(matches!(
        c.cmd(&["JSON.SET", "jk:doc", "$.no.such.parent", "2"]),
        V::Nil
    ));
    assert!(c.cmd(&["JSON.SET", "jk:new", "$.a", "1"]).is_err());
    assert_eq!(c.cmd(&["JSON.GET", "jk:doc", "$.nested.added"]).s(), "[1]");
    // A member that is removed leaves the others in their order.
    assert_eq!(c.cmd(&["JSON.DEL", "jk:doc", "$.n"]).i(), 1);
    assert_eq!(
        c.cmd(&["JSON.OBJKEYS", "jk:doc"]).strings(),
        ["name", "flag", "tags", "nested"]
    );
    assert_eq!(c.cmd(&["JSON.CLEAR", "jk:doc", "$..tags"]).i(), 2);
    assert_eq!(c.cmd(&["JSON.GET", "jk:doc", "$..tags"]).s(), "[[],[]]");

    // JSON.RESP spells the structure out.
    assert!(
        c.cmd(&["JSON.SET", "jk:resp", "$", r#"{"a":[1,"s",null,true,1.5]}"#])
            .ok()
    );
    let reply = c.cmd(&["JSON.RESP", "jk:resp"]);
    let object = reply.arr();
    assert_eq!((object[0].s(), object[1].s()), ("{", "a"));
    let array = object[2].arr();
    assert_eq!(array[0].s(), "[");
    assert_eq!(array[1].i(), 1);
    assert_eq!(array[2].s(), "s");
    assert!(matches!(array[3], V::Nil));
    assert_eq!(array[4].s(), "true");
    assert_eq!(array[5].s(), "1.5");

    // A key of another type is not silently replaced.
    assert!(c.cmd(&["SET", "jk:string", "v"]).ok());
    assert!(c.cmd(&["JSON.SET", "jk:string", "$", "1"]).is_err());
    assert_eq!(c.cmd(&["GET", "jk:string"]).s(), "v");
    // Deleting the root deletes the key, and its TTL with it.
    assert!(c.cmd(&["JSON.SET", "jk:ttl", "$", "1"]).ok());
    assert_eq!(c.cmd(&["EXPIRE", "jk:ttl", "1000"]).i(), 1);
    assert_eq!(c.cmd(&["JSON.DEL", "jk:ttl"]).i(), 1);
    assert!(c.cmd(&["JSON.SET", "jk:ttl", "$", "2"]).ok());
    assert_eq!(c.cmd(&["TTL", "jk:ttl"]).i(), -1);
}

// ── Blocked clients, and keys left empty ──────────────────────────────────────

/// A client that has sent a blocking command and not been answered yet.
fn blocked(port: u16, cmd: &[&str]) -> Conn {
    let mut c = Conn::connect(port);
    c.send(cmd);
    // Long enough for the server to have parked it.
    thread::sleep(Duration::from_millis(150));
    c
}

/// A blocked pop takes the element its own rules pick once the push is
/// complete — not whichever element was pushed first — and the replica ends
/// up with the same list as the master.
#[test]
fn a_blocked_pop_takes_the_element_it_asked_for_and_the_replica_agrees() {
    let master_port = free_port();
    start(master_port, None);
    let replica_port = free_port();
    start(replica_port, Some(master_port));
    let mut m = Conn::connect(master_port);
    let mut r = Conn::connect(replica_port);
    wait_until("the replica to sync", || {
        r.cmd(&["INFO", "replication"])
            .s()
            .contains("master_link_status:up")
    });

    /// A blocked command, the push that wakes it, what it should be
    /// answered, and what should be left in the list.
    struct Case {
        blocked: &'static [&'static str],
        push: &'static [&'static str],
        reply: V,
        left: &'static str,
    }
    let bulk = |s: &str| V::Bulk(s.into());
    let cases = [
        Case {
            blocked: &["BRPOP", "b:1", "0"],
            push: &["RPUSH", "b:1", "a", "b"],
            reply: V::Arr(vec![bulk("b:1"), bulk("b")]),
            left: "a",
        },
        Case {
            blocked: &["BLPOP", "b:2", "0"],
            push: &["LPUSH", "b:2", "a", "b"],
            reply: V::Arr(vec![bulk("b:2"), bulk("b")]),
            left: "a",
        },
        Case {
            blocked: &["BLPOP", "b:3", "0"],
            push: &["RPUSH", "b:3", "a", "b"],
            reply: V::Arr(vec![bulk("b:3"), bulk("a")]),
            left: "b",
        },
        Case {
            blocked: &["BLMPOP", "0", "1", "b:4", "RIGHT", "COUNT", "2"],
            push: &["RPUSH", "b:4", "a", "b", "c"],
            reply: V::Arr(vec![bulk("b:4"), V::Arr(vec![bulk("c"), bulk("b")])]),
            left: "a",
        },
        Case {
            blocked: &["BLMOVE", "b:5", "b:5:dst", "RIGHT", "LEFT", "0"],
            push: &["RPUSH", "b:5", "a", "b"],
            reply: bulk("b"),
            left: "a",
        },
        Case {
            blocked: &["BRPOPLPUSH", "b:6", "b:6:dst", "0"],
            push: &["RPUSH", "b:6", "a", "b"],
            reply: bulk("b"),
            left: "a",
        },
    ];
    for case in cases {
        let key = case.push[1];
        let mut waiter = blocked(master_port, case.blocked);
        // The push reports the length with every element in.
        assert_eq!(
            m.cmd(case.push).i(),
            (case.push.len() - 2) as i64,
            "{:?}",
            case.push
        );
        assert_eq!(waiter.read(), case.reply, "{:?}", case.blocked);
        assert_eq!(m.cmd(&["WAIT", "1", "5000"]).i(), 1);
        for (side, c) in [("master", &mut m), ("replica", &mut r)] {
            assert_eq!(
                c.cmd(&["LRANGE", key, "0", "-1"]).strings(),
                [case.left],
                "{:?} on the {side}",
                case.blocked
            );
        }
    }
    for dst in ["b:5:dst", "b:6:dst"] {
        assert_eq!(r.cmd(&["LRANGE", dst, "0", "-1"]).strings(), ["b"]);
    }

    // Sorted sets: the lowest (or highest) score, not the first member added.
    let mut waiter = blocked(master_port, &["BZPOPMIN", "b:z", "0"]);
    assert_eq!(
        m.cmd(&["ZADD", "b:z", "5", "five", "1", "one", "3", "three"])
            .i(),
        3
    );
    assert_eq!(waiter.read().strings(), ["b:z", "one", "1"]);
    let mut waiter = blocked(
        master_port,
        &["BZMPOP", "0", "1", "b:z2", "MAX", "COUNT", "2"],
    );
    assert_eq!(
        m.cmd(&["ZADD", "b:z2", "5", "five", "1", "one", "3", "three"])
            .i(),
        3
    );
    let reply = waiter.read();
    assert_eq!(reply.arr()[1].arr()[0].strings(), ["five", "5"]);
    assert_eq!(reply.arr()[1].arr()[1].strings(), ["three", "3"]);
    assert_eq!(m.cmd(&["WAIT", "1", "5000"]).i(), 1);
    for c in [&mut m, &mut r] {
        assert_eq!(
            c.cmd(&["ZRANGE", "b:z", "0", "-1"]).strings(),
            ["three", "five"]
        );
        assert_eq!(c.cmd(&["ZRANGE", "b:z2", "0", "-1"]).strings(), ["one"]);
    }

    // Clients blocked on one key are served in the order they blocked,
    // whether the elements arrive in one push or in several.
    for pushes in [
        vec![vec!["RPUSH", "b:two", "x", "y", "z"]],
        vec![
            vec!["RPUSH", "b:two", "x"],
            vec!["RPUSH", "b:two", "y"],
            vec!["RPUSH", "b:two", "z"],
        ],
    ] {
        let mut waiters: Vec<Conn> = (0..3)
            .map(|_| blocked(master_port, &["BLPOP", "b:two", "0"]))
            .collect();
        for push in &pushes {
            assert!(!m.cmd(push).is_err());
        }
        let got: Vec<String> = waiters
            .iter_mut()
            .map(|waiter| waiter.read().strings()[1].clone())
            .collect();
        assert_eq!(got, ["x", "y", "z"], "{pushes:?}");
        assert_eq!(m.cmd(&["EXISTS", "b:two"]).i(), 0);
    }
    // Each takes from the end it asked for: with all three elements there,
    // the BLPOP at the front takes the head, the BRPOP behind it the tail.
    let mut first = blocked(master_port, &["BLPOP", "b:ends", "0"]);
    let mut second = blocked(master_port, &["BRPOP", "b:ends", "0"]);
    let mut third = blocked(master_port, &["BLPOP", "b:ends", "0"]);
    assert_eq!(m.cmd(&["RPUSH", "b:ends", "x", "y", "z"]).i(), 3);
    let got = [first.read(), second.read(), third.read()].map(|v| v.strings()[1].clone());
    assert_eq!(got, ["x", "z", "y"]);

    // A client that needs more than there is does not hold up the one
    // behind it.
    let mut needs_three = blocked(
        master_port,
        &[
            "BLMOVEM",
            "b:line",
            "b:line:dst",
            "LEFT",
            "RIGHT",
            "0",
            "EXACTLY",
            "3",
            "BULK",
        ],
    );
    let mut needs_one = blocked(master_port, &["BLPOP", "b:line", "0"]);
    assert_eq!(m.cmd(&["RPUSH", "b:line", "1"]).i(), 1);
    assert_eq!(needs_one.read().strings(), ["b:line", "1"]);
    assert_eq!(m.cmd(&["RPUSH", "b:line", "2", "3", "4"]).i(), 3);
    assert_eq!(needs_three.read().strings(), ["2", "3", "4"]);

    // A timeout still answers with nil, and a later push is not swallowed.
    let mut waiter = blocked(master_port, &["BLPOP", "b:timeout", "0.3"]);
    assert_eq!(waiter.read(), V::Nil);
    assert_eq!(m.cmd(&["RPUSH", "b:timeout", "kept"]).i(), 1);
    assert_eq!(
        m.cmd(&["LRANGE", "b:timeout", "0", "-1"]).strings(),
        ["kept"]
    );
    // A key of the wrong type is an error straight away.
    assert!(m.cmd(&["SET", "b:string", "v"]).ok());
    assert!(m.cmd(&["BLPOP", "b:string", "0"]).is_err());
    assert!(m.cmd(&["BZPOPMIN", "b:string", "0"]).is_err());
}

/// A list that arrives some other way than a push wakes the clients blocked
/// on its key too. (`SORT … STORE` is the case Redis 8.10 fixed.)
#[test]
fn a_blocked_pop_is_woken_by_whatever_creates_the_list() {
    let port = free_port();
    start(port, None);
    let mut c = Conn::connect(port);

    let routes: Vec<(&str, Vec<Vec<&str>>, &str)> = vec![
        (
            "w:rename",
            vec![
                vec!["RPUSH", "w:src1", "x", "y"],
                vec!["RENAME", "w:src1", "w:rename"],
            ],
            "x",
        ),
        (
            "w:copy",
            vec![
                vec!["RPUSH", "w:src2", "x", "y"],
                vec!["COPY", "w:src2", "w:copy"],
            ],
            "x",
        ),
        (
            "w:sort",
            vec![
                vec!["RPUSH", "w:src3", "2", "1"],
                vec!["SORT", "w:src3", "STORE", "w:sort"],
            ],
            "1",
        ),
        (
            "w:lmove",
            vec![
                vec!["RPUSH", "w:src4", "x", "y"],
                vec!["LMOVE", "w:src4", "w:lmove", "LEFT", "RIGHT"],
            ],
            "x",
        ),
    ];
    for (key, writes, expected) in routes {
        let mut waiter = blocked(port, &["BLPOP", key, "5"]);
        for write in &writes {
            let reply = c.cmd(write);
            assert!(!reply.is_err(), "{write:?}: {reply:?}");
        }
        assert_eq!(waiter.read().strings(), [key, expected], "{key}");
    }

    // The same from inside a transaction: the client is woken, and EXEC's own
    // commands see every element they pushed.
    let mut waiter = blocked(port, &["BLPOP", "w:multi", "0.5"]);
    assert!(c.cmd(&["MULTI"]).ok());
    c.cmd(&["RPUSH", "w:multi", "a"]);
    c.cmd(&["LPOP", "w:multi"]);
    let exec = c.cmd(&["EXEC"]);
    assert_eq!(exec.arr()[0].i(), 1);
    assert_eq!(exec.arr()[1].s(), "a");
    assert_eq!(waiter.read(), V::Nil, "there was nothing left to pop");
}

/// The command that takes the last element out of a list, set, sorted set or
/// hash takes the key with it.
#[test]
fn a_key_left_empty_is_deleted() {
    let port = free_port();
    start(port, None);
    let mut c = Conn::connect(port);
    let cases: Vec<(Vec<&str>, Vec<&str>)> = vec![
        (vec!["RPUSH", "e", "1"], vec!["LPOP", "e"]),
        (vec!["RPUSH", "e", "1"], vec!["RPOP", "e"]),
        (vec!["RPUSH", "e", "1", "2"], vec!["LPOP", "e", "5"]),
        (vec!["RPUSH", "e", "1"], vec!["LMPOP", "1", "e", "LEFT"]),
        (vec!["RPUSH", "e", "1"], vec!["BLPOP", "e", "1"]),
        (vec!["RPUSH", "e", "1"], vec!["LREM", "e", "0", "1"]),
        (vec!["RPUSH", "e", "1"], vec!["LTRIM", "e", "5", "9"]),
        (vec!["SADD", "e", "1"], vec!["SPOP", "e"]),
        (vec!["SADD", "e", "1"], vec!["SREM", "e", "1"]),
        (vec!["ZADD", "e", "1", "m"], vec!["ZREM", "e", "m"]),
        (vec!["ZADD", "e", "1", "m"], vec!["ZPOPMIN", "e"]),
        (vec!["ZADD", "e", "1", "m"], vec!["BZPOPMAX", "e", "1"]),
        (vec!["ZADD", "e", "1", "m"], vec!["ZMPOP", "1", "e", "MIN"]),
        (
            vec!["ZADD", "e", "1", "m"],
            vec!["ZREMRANGEBYSCORE", "e", "0", "5"],
        ),
        (vec!["HSET", "e", "f", "v"], vec!["HDEL", "e", "f"]),
        (
            vec!["HSET", "e", "f", "v"],
            vec!["HGETDEL", "e", "FIELDS", "1", "f"],
        ),
    ];
    for (create, empty) in cases {
        assert!(!c.cmd(&create).is_err(), "{create:?}");
        assert!(!c.cmd(&empty).is_err(), "{empty:?}");
        assert_eq!(c.cmd(&["EXISTS", "e"]).i(), 0, "{empty:?}");
        assert_eq!(c.cmd(&["TYPE", "e"]).s(), "none", "{empty:?}");
        assert_eq!(c.cmd(&["DBSIZE"]).i(), 0, "{empty:?}");
    }

    // Set algebra: a key of another type is an error wherever it appears,
    // and a key that does not exist is an empty set.
    assert!(c.cmd(&["SET", "e:string", "v"]).ok());
    assert_eq!(c.cmd(&["SADD", "e:members", "a", "b"]).i(), 2);
    for command in [
        vec!["SUNION", "e:members", "e:string"],
        vec!["SINTER", "e:members", "e:string"],
        vec!["SDIFF", "e:string", "e:members"],
        vec!["SINTERCARD", "2", "e:members", "e:string"],
        vec!["SUNIONSTORE", "e:out", "e:members", "e:string"],
        vec!["SINTERSTORE", "e:out", "e:string", "e:members"],
        vec!["SDIFFSTORE", "e:out", "e:members", "e:string"],
    ] {
        let reply = c.cmd(&command);
        assert!(
            matches!(&reply, V::Err(e) if e.starts_with("WRONGTYPE")),
            "{command:?}: {reply:?}"
        );
    }
    assert_eq!(c.cmd(&["EXISTS", "e:out"]).i(), 0);
    assert_eq!(c.cmd(&["SUNION", "e:members", "e:missing"]).arr().len(), 2);
    assert_eq!(c.cmd(&["SINTER", "e:members", "e:missing"]).arr().len(), 0);
    // A store replaces the destination, its TTL included.
    assert!(c.cmd(&["SET", "e:replaced", "v", "EX", "100"]).ok());
    assert_eq!(c.cmd(&["SUNIONSTORE", "e:replaced", "e:members"]).i(), 2);
    assert_eq!(c.cmd(&["TTL", "e:replaced"]).i(), -1);
    assert_eq!(
        c.cmd(&["DEL", "e:string", "e:members", "e:replaced"]).i(),
        3
    );

    // A store whose result is empty leaves no destination.
    assert_eq!(c.cmd(&["SADD", "e:set", "1"]).i(), 1);
    assert_eq!(c.cmd(&["SADD", "e:dst", "old"]).i(), 1);
    assert_eq!(c.cmd(&["SDIFFSTORE", "e:dst", "e:set", "e:set"]).i(), 0);
    assert_eq!(c.cmd(&["EXISTS", "e:dst"]).i(), 0);
    // The TTL goes with the key: its successor does not inherit it.
    assert_eq!(c.cmd(&["RPUSH", "e:ttl", "1"]).i(), 1);
    assert_eq!(c.cmd(&["EXPIRE", "e:ttl", "100"]).i(), 1);
    assert_eq!(c.cmd(&["LPOP", "e:ttl"]).s(), "1");
    assert_eq!(c.cmd(&["RPUSH", "e:ttl", "1"]).i(), 1);
    assert_eq!(c.cmd(&["TTL", "e:ttl"]).i(), -1);
    // A stream is the exception: it can be empty and still exist.
    c.cmd(&["XADD", "e:stream", "1-1", "f", "v"]);
    assert_eq!(c.cmd(&["XDEL", "e:stream", "1-1"]).i(), 1);
    assert_eq!(c.cmd(&["EXISTS", "e:stream"]).i(), 1);
    // And a key that still has elements stays.
    assert_eq!(c.cmd(&["RPUSH", "e:keep", "1", "2"]).i(), 2);
    assert_eq!(c.cmd(&["LPOP", "e:keep"]).s(), "1");
    assert_eq!(c.cmd(&["LLEN", "e:keep"]).i(), 1);
}

// ── Several databases written at once ─────────────────────────────────────────

/// Clients writing to different databases at the same time: every write
/// reaches the replica in the database it was made in. The stream says which
/// database a write belongs to with a `SELECT`; when that travelled apart from
/// the write, another client's write could land behind someone else's
/// `SELECT` and be replayed into the wrong database.
#[test]
fn concurrent_writes_to_different_databases_stay_in_their_databases() {
    let master_port = free_port();
    start(master_port, None);
    let replica_port = free_port();
    start(replica_port, Some(master_port));
    let mut m = Conn::connect(master_port);
    let mut r = Conn::connect(replica_port);
    wait_until("the replica to sync", || {
        r.cmd(&["INFO", "replication"])
            .s()
            .contains("master_link_status:up")
    });

    const ROUNDS: usize = 1500;
    // One writer per database, each counting in a key of its own and pushing
    // to a list of its own, plus one that hops between two more databases on
    // every command.
    let writers: Vec<thread::JoinHandle<()>> = (0..4usize)
        .map(|db| {
            thread::spawn(move || {
                let mut c = Conn::connect(master_port);
                assert!(c.cmd(&["SELECT", &db.to_string()]).ok());
                for _ in 0..ROUNDS {
                    c.cmd(&["INCR", &format!("count:{db}")]);
                    c.cmd(&["RPUSH", &format!("log:{db}"), "x"]);
                }
            })
        })
        .chain(std::iter::once(thread::spawn(move || {
            let mut c = Conn::connect(master_port);
            for _ in 0..ROUNDS {
                for db in ["5", "6"] {
                    c.cmd(&["SELECT", db]);
                    c.cmd(&["INCR", "hop"]);
                }
            }
        })))
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    assert_eq!(m.cmd(&["WAIT", "1", "10000"]).i(), 1);

    for (side, c) in [("master", &mut m), ("replica", &mut r)] {
        for db in 0..4usize {
            assert!(c.cmd(&["SELECT", &db.to_string()]).ok());
            assert_eq!(c.cmd(&["DBSIZE"]).i(), 2, "db {db} on the {side}");
            assert_eq!(
                c.cmd(&["GET", &format!("count:{db}")]).s(),
                ROUNDS.to_string(),
                "db {db} on the {side}"
            );
            assert_eq!(
                c.cmd(&["LLEN", &format!("log:{db}")]).i(),
                ROUNDS as i64,
                "db {db} on the {side}"
            );
        }
        for db in ["5", "6"] {
            assert!(c.cmd(&["SELECT", db]).ok());
            assert_eq!(c.cmd(&["DBSIZE"]).i(), 1, "db {db} on the {side}");
            assert_eq!(
                c.cmd(&["GET", "hop"]).s(),
                ROUNDS.to_string(),
                "db {db} on the {side}"
            );
        }
        assert!(c.cmd(&["SELECT", "0"]).ok());
    }
}

/// INCREX's options and errors as Redis 8.10 has them: each option at most
/// once, anything else a syntax error, and Redis's wording for the rest.
#[test]
fn increx_options_and_errors_follow_redis_8_10() {
    let mut c = conn();
    for (args, error) in [
        (&["BYINT", "1", "BYFLOAT", "1"][..], "ERR syntax error"),
        (&["BYINT", "1", "BYINT", "2"], "ERR syntax error"),
        (&["PERSIST", "ENX"], "ERR syntax error"),
        (&["ENX", "PERSIST"], "ERR syntax error"),
        (&["EX", "10", "PX", "10"], "ERR syntax error"),
        (&["SATURATE", "SATURATE"], "ERR syntax error"),
        (
            &["BYINT", "x"],
            "ERR Increment is not an integer or out of range",
        ),
        (&["BYFLOAT", "x"], "ERR Increment is not a valid float"),
        (
            &["BYFLOAT", "inf"],
            "ERR BYFLOAT increment cannot be Infinity",
        ),
        (
            &["LBOUND", "x"],
            "ERR LBOUND is not an integer or out of range",
        ),
        (
            &["BYFLOAT", "1", "UBOUND", "x"],
            "ERR UBOUND is not a valid float",
        ),
        (
            &["LBOUND", "5", "UBOUND", "1"],
            "ERR LBOUND can't be greater than UBOUND",
        ),
        (&["ENX"], "ERR ENX flag requires an expiration"),
        (&["EX", "0"], "ERR invalid expire time in 'increx' command"),
    ] {
        let mut cmd = vec!["INCREX", "ix"];
        cmd.extend_from_slice(args);
        assert_eq!(c.cmd(&cmd), V::Err(error.into()), "{args:?}");
    }

    // Out of bounds: left alone, with the value as it is and 0; SATURATE
    // clamps to the bound.
    assert!(c.cmd(&["SET", "ix", "9223372036854775806"]).ok());
    let reply = c.cmd(&["INCREX", "ix", "BYINT", "5"]);
    assert_eq!(reply.arr()[0].i(), 9223372036854775806);
    assert_eq!(reply.arr()[1].i(), 0);
    assert!(c.cmd(&["SET", "ix", "-9223372036854775808"]).ok());
    assert_eq!(
        c.cmd(&[
            "INCREX",
            "ix",
            "BYINT",
            "1",
            "UBOUND",
            "9223372036854775807",
            "SATURATE",
            "LBOUND",
            "9223372036854775807"
        ]),
        V::Err("ERR applied increment would overflow".into())
    );
    assert!(c.cmd(&["SET", "ix", "inf"]).ok());
    assert_eq!(
        c.cmd(&["INCREX", "ix", "BYFLOAT", "1"]),
        V::Err("ERR value cannot be Infinity".into())
    );

    // A TTL already past: the key goes at once.
    assert!(c.cmd(&["SET", "ix", "10"]).ok());
    let reply = c.cmd(&["INCREX", "ix", "BYINT", "1", "EXAT", "1"]);
    assert_eq!(reply.arr()[0].i(), 11);
    assert_eq!(c.cmd(&["EXISTS", "ix"]).i(), 0);
    assert!(c.cmd(&["SET", "ix", "10", "EX", "500"]).ok());
    c.cmd(&["INCREX", "ix", "BYINT", "1", "PERSIST"]);
    assert_eq!(c.cmd(&["TTL", "ix"]).i(), -1);
}

/// Argument checks Redis 8.10 tightened: LMOVEM's trailer, SDIFFCARD's
/// numbers, a negative LIMIT offset, XREADGROUP's options and IDs.
#[test]
fn argument_checks_follow_redis_8_10() {
    let mut c = conn();
    c.cmd(&["RPUSH", "lm:{t}src", "a", "b", "c"]);
    for (args, error) in [
        (
            &["LMOVEM", "lm:{t}src", "lm:{t}dst", "LEFT"][..],
            "ERR wrong number of arguments for 'lmovem' command",
        ),
        (
            &[
                "LMOVEM",
                "lm:{t}src",
                "lm:{t}dst",
                "LEFT",
                "RIGHT",
                "COUNT",
                "2",
            ],
            "ERR syntax error",
        ),
        (
            &[
                "LMOVEM",
                "lm:{t}src",
                "lm:{t}dst",
                "LEFT",
                "RIGHT",
                "COUNT",
                "2",
                "OBO",
                "x",
            ],
            "ERR syntax error",
        ),
        (
            &[
                "LMOVEM",
                "lm:{t}src",
                "lm:{t}dst",
                "LEFT",
                "RIGHT",
                "COUNT",
                "0",
                "BULK",
            ],
            "ERR count should be greater than 0",
        ),
        (
            &[
                "LMOVEM",
                "lm:{t}src",
                "lm:{t}dst",
                "LEFT",
                "RIGHT",
                "COUNT",
                "a",
                "BULK",
            ],
            "ERR count should be greater than 0",
        ),
        (
            &["SDIFFCARD", "a", "s:{t}1"],
            "ERR numkeys should be greater than 0",
        ),
        (
            &["SDIFFCARD", "1", "s:{t}1", "LIMIT", "a"],
            "ERR LIMIT can't be negative",
        ),
        (
            &["ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "x", "2"],
            "ERR value is not an integer or out of range",
        ),
    ] {
        assert_eq!(c.cmd(args), V::Err(error.into()), "{args:?}");
    }
    assert_eq!(
        c.cmd(&[
            "LMOVEM",
            "lm:{t}src",
            "lm:{t}dst",
            "LEFT",
            "RIGHT",
            "COUNT",
            "2",
            "OBO"
        ])
        .arr()
        .len(),
        2
    );

    c.cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]);
    assert!(
        c.cmd(&["ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "-1", "2"])
            .arr()
            .is_empty()
    );
    assert!(
        c.cmd(&["ZRANGEBYLEX", "z", "-", "+", "LIMIT", "-1", "2"])
            .arr()
            .is_empty()
    );
    assert_eq!(
        c.cmd(&["ZRANGEBYSCORE", "z", "0", "10", "LIMIT", "1", "-1"])
            .arr()
            .len(),
        2
    );

    c.cmd(&["XADD", "xs", "1-0", "f", "v"]);
    assert!(c.cmd(&["XGROUP", "CREATE", "xs", "g", "0"]).ok());
    for (args, error) in [
        (
            &["BLOCK", "abc"][..],
            "ERR timeout is not an integer or out of range",
        ),
        (&["BLOCK", "-1"], "ERR timeout is negative"),
        (
            &["COUNT", "abc"],
            "ERR value is not an integer or out of range",
        ),
        (
            &["MAXCOUNT", "foo"],
            "ERR value is not an integer or out of range",
        ),
        (
            &["MAXCOUNT", "0"],
            "ERR MAXCOUNT must be a positive integer",
        ),
        (&["INVALID", "x"], "ERR syntax error"),
    ] {
        let mut cmd = vec!["XREADGROUP", "GROUP", "g", "c"];
        cmd.extend_from_slice(args);
        cmd.extend_from_slice(&["STREAMS", "xs", ">"]);
        assert_eq!(c.cmd(&cmd), V::Err(error.into()), "{args:?}");
    }
    for id in ["invalid-id", "123-", "-123", "abc-def", "--"] {
        assert_eq!(
            c.cmd(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "xs", id]),
            V::Err("ERR Invalid stream ID specified as stream command argument".into()),
            "{id}"
        );
    }
    assert_eq!(
        c.cmd(&["XREADGROUP", "GROUP", "g", "c", "STREAM", "xs", ">"]),
        V::Err("ERR syntax error".into())
    );
    assert_eq!(
        c.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "1",
            "STREAMS",
            "xs",
            ">"
        ])
        .arr()
        .len(),
        1
    );
}

/// HSETEX and HGETEX take their options after the FIELDS block as well as
/// before it (Redis 8.10), and a field named like an option is still a field.
#[test]
fn hsetex_and_hgetex_take_options_after_fields() {
    let mut c = conn();
    assert_eq!(
        c.cmd(&[
            "HSETEX", "hx", "FIELDS", "2", "f1", "v1", "EX", "v2", "EX", "60"
        ])
        .i(),
        1
    );
    assert_eq!(c.cmd(&["HGET", "hx", "EX"]).s(), "v2", "a field named EX");
    let ttl = c.cmd(&["HTTL", "hx", "FIELDS", "1", "f1"]).arr()[0].i();
    assert!((1..=60).contains(&ttl), "{ttl}");
    assert_eq!(
        c.cmd(&["HSETEX", "hx", "FIELDS", "1", "f1", "w", "FXX", "KEEPTTL"])
            .i(),
        1
    );
    assert!(
        c.cmd(&["HTTL", "hx", "FIELDS", "1", "f1"]).arr()[0].i() > 0,
        "KEEPTTL after FIELDS"
    );
    assert_eq!(
        c.cmd(&["HGETEX", "hx", "FIELDS", "1", "f1", "PERSIST"])
            .arr()[0]
            .s(),
        "w"
    );
    assert_eq!(c.cmd(&["HTTL", "hx", "FIELDS", "1", "f1"]).arr()[0].i(), -1);
}

/// XIDMPRECORD (Redis 8.10): an entry already in the stream recorded as what
/// a producer's idempotent ID produced, so `XADD … IDMP` with that ID returns
/// it instead of adding another.
#[test]
fn xidmprecord_records_an_entry_for_idempotent_production() {
    let mut c = conn();
    let id = c.cmd(&["XADD", "xi", "*", "f", "v"]).s().to_string();
    for (args, error) in [
        (
            &["xi", "p1", "i1"][..],
            "ERR wrong number of arguments for 'xidmprecord' command",
        ),
        (&["nosuch", "p1", "i1", "1-1"], "ERR no such key"),
        (
            &["xi", "p1", "i1", "bad"],
            "ERR Invalid stream ID specified as stream command argument",
        ),
        (
            &["xi", "", "i1", "1-1"],
            "ERR producer ID must be non-empty",
        ),
        (
            &["xi", "p1", "", "1-1"],
            "ERR idempotent ID must be non-empty",
        ),
        (
            &["xi", "p1", "i1", "999-999"],
            "ERR No such message in stream",
        ),
    ] {
        let mut cmd = vec!["XIDMPRECORD"];
        cmd.extend_from_slice(args);
        assert_eq!(c.cmd(&cmd), V::Err(error.into()), "{args:?}");
    }
    assert!(c.cmd(&["XIDMPRECORD", "xi", "p1", "req-1", &id]).ok());
    assert!(
        c.cmd(&["XIDMPRECORD", "xi", "p1", "req-1", &id]).ok(),
        "the same record again"
    );
    // A retry of the request returns the recorded entry; nothing is added.
    assert_eq!(
        c.cmd(&["XADD", "xi", "IDMP", "p1", "req-1", "*", "f", "again"])
            .s(),
        id
    );
    assert_eq!(c.cmd(&["XLEN", "xi"]).i(), 1);
    let other = c.cmd(&["XADD", "xi", "*", "f", "w"]).s().to_string();
    assert_eq!(
        c.cmd(&["XIDMPRECORD", "xi", "p1", "req-1", &other]),
        V::Err("ERR IID already exists for this producer with a different stream ID".into())
    );
}

/// CLIENT INFO's per-client counters (Redis 8.10): bytes in and out,
/// commands, reads, and commands per read.
#[test]
fn client_info_counts_a_connections_traffic() {
    let mut c = conn();
    let field = |info: &str, name: &str| -> i64 {
        info.split_whitespace()
            .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
            .unwrap_or_else(|| panic!("{name} in {info}"))
            .parse()
            .unwrap()
    };
    let before = c.cmd(&["CLIENT", "INFO"]).s().to_string();
    assert!(
        before.contains(" omem-shared=0 omem-unshared=0 "),
        "{before}"
    );
    for _ in 0..3 {
        c.cmd(&["PING"]);
    }
    let after = c.cmd(&["CLIENT", "INFO"]).s().to_string();
    assert_eq!(field(&after, "tot-cmds") - field(&before, "tot-cmds"), 4);
    assert!(field(&after, "read-events") - field(&before, "read-events") >= 4);
    let sum = field(&after, "avg-pipeline-len-sum") - field(&before, "avg-pipeline-len-sum");
    let cnt = field(&after, "avg-pipeline-len-cnt") - field(&before, "avg-pipeline-len-cnt");
    assert!(sum > 0 && sum == cnt, "one command a read: {sum} / {cnt}");
    // Three PINGs (14 bytes each) and this CLIENT INFO (26).
    assert_eq!(
        field(&after, "tot-net-in") - field(&before, "tot-net-in"),
        3 * 14 + 26
    );
    assert!(field(&after, "tot-net-out") - field(&before, "tot-net-out") > 300);
}
