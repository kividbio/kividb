// tests/info_keyspace.rs
//
// INFO keyspace's `avg_ttl`, and INFO keysizes over many keys. `avg_ttl` is
// made of a sum each shard keeps as times to live come, change and go,
// rather than a pass over every key with one (#622): these follow it
// through every command that changes one. A server of its own, one for
// the file's tests, each in databases no other uses.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

/// Whether nothing has `port` (see tests/generics.rs).
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
    let from = (std::process::id() as u16).wrapping_mul(31);
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
                    .set_read_timeout(Some(Duration::from_secs(30)))
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

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
    }

    /// A reply: a simple string, an error or an integer as its line, a
    /// bulk string as its text.
    fn read(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        match line.strip_prefix('$') {
            Some("-1") => String::new(),
            Some(len) => {
                let mut text = vec![0u8; len.parse::<usize>().unwrap() + 2];
                std::io::Read::read_exact(&mut self.reader, &mut text).unwrap();
                text.truncate(text.len() - 2);
                String::from_utf8(text).unwrap()
            }
            None => line,
        }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.read()
    }

    /// `(keys, expires, avg_ttl)` of database `db` in `INFO keyspace`, None
    /// if it has no line.
    fn keyspace(&mut self, db: usize) -> Option<(u64, u64, u64)> {
        let info = self.cmd(&["INFO", "keyspace"]);
        assert!(info.starts_with("# Keyspace\r\n"), "{info}");
        let line = info
            .lines()
            .find_map(|l| l.strip_prefix(&format!("db{db}:")))?
            .to_string();
        let field = |name: &str| -> u64 {
            line.split(',')
                .find_map(|f| f.strip_prefix(&format!("{name}=")))
                .unwrap_or_else(|| panic!("no {name} in {line}"))
                .parse()
                .unwrap()
        };
        Some((field("keys"), field("expires"), field("avg_ttl")))
    }
}

/// The one server of these tests, started by the first to ask: each test
/// keeps to databases of its own.
fn start() -> Conn {
    static PORT: std::sync::LazyLock<u16> = std::sync::LazyLock::new(|| {
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
        port
    });
    Conn::connect(*PORT)
}

/// `avg_ttl` of `db` is `want` milliseconds, less what has passed since
/// `since`, give or take the rounding; and `keys` and `expires` are as
/// said.
#[track_caller]
fn assert_avg(c: &mut Conn, db: usize, keys: u64, expires: u64, want: u64, since: Instant) {
    let (k, e, avg) = c.keyspace(db).expect("no line for the database");
    let passed = since.elapsed().as_millis() as u64;
    assert_eq!((k, e), (keys, expires), "keys, expires");
    assert!(
        avg <= want && avg + passed + 2 >= want,
        "avg_ttl {avg}, want {want} less up to {passed} ms"
    );
}

#[test]
fn avg_ttl_follows_every_change_of_a_time_to_live() {
    let mut c = start();
    let hour = 3_600_000u64;
    // What is left of each time is checked to within the time since
    // here: the steps are half an hour apart and more.
    let t0 = Instant::now();
    // Two keys with a time, one without: the mean of the two.
    assert_eq!(c.cmd(&["SET", "a", "v", "PX", &hour.to_string()]), "+OK");
    assert_eq!(
        c.cmd(&["SET", "b", "v", "PX", &(3 * hour).to_string()]),
        "+OK"
    );
    assert_eq!(c.cmd(&["SET", "plain", "v"]), "+OK");
    assert_avg(&mut c, 0, 3, 2, 2 * hour, t0);

    // Written over with a new time (in place, the same length), and
    // without one but keeping it.
    assert_eq!(
        c.cmd(&["SET", "a", "w", "PX", &(5 * hour).to_string()]),
        "+OK"
    );
    assert_eq!(c.cmd(&["SET", "b", "longer value", "KEEPTTL"]), "+OK");
    assert_avg(&mut c, 0, 3, 2, 4 * hour, t0);

    // A time given, and changed, by EXPIRE and PEXPIREAT.
    assert_eq!(c.cmd(&["PEXPIRE", "plain", &(7 * hour).to_string()]), ":1");
    assert_avg(&mut c, 0, 3, 3, 5 * hour, t0);
    assert_eq!(
        c.cmd(&["EXPIRE", "a", &(2 * hour / 1000).to_string()]),
        ":1"
    );
    assert_avg(&mut c, 0, 3, 3, 4 * hour, t0);

    // Taken away: PERSIST, and SET without KEEPTTL.
    assert_eq!(c.cmd(&["PERSIST", "b"]), ":1");
    assert_avg(&mut c, 0, 3, 2, 9 * hour / 2, t0);
    assert_eq!(c.cmd(&["SET", "a", "x"]), "+OK");
    assert_avg(&mut c, 0, 3, 1, 7 * hour, t0);

    // GETEX gives one; DEL takes a key with one out.
    assert_eq!(
        c.cmd(&["GETEX", "b", "PX", &hour.to_string()]),
        "longer value"
    );
    assert_avg(&mut c, 0, 3, 2, 4 * hour, t0);
    assert_eq!(c.cmd(&["DEL", "plain"]), ":1");
    assert_avg(&mut c, 0, 2, 1, hour, t0);

    // A key that expires: once expiry has taken it out, it is not in the
    // mean.
    assert_eq!(c.cmd(&["SET", "short", "v", "PX", "50"]), "+OK");
    let deadline = Instant::now() + Duration::from_secs(10);
    while c.keyspace(0).unwrap().1 != 1 {
        assert!(Instant::now() < deadline, "the short key never expired");
        thread::sleep(Duration::from_millis(20));
    }
    assert_avg(&mut c, 0, 2, 1, hour, t0);

    // SWAPDB takes the sums along with the keys; MOVE takes a key's time
    // to another database.
    assert_eq!(
        c.cmd(&["SET", "m", "v", "PX", &(3 * hour).to_string()]),
        "+OK"
    );
    assert_eq!(c.cmd(&["MOVE", "m", "2"]), ":1");
    assert_avg(&mut c, 2, 1, 1, 3 * hour, t0);
    assert_eq!(c.cmd(&["SWAPDB", "0", "1"]), "+OK");
    assert_eq!(c.keyspace(0), None);
    assert_avg(&mut c, 1, 2, 1, hour, t0);

    // FLUSHDB takes them all.
    assert_eq!(c.cmd(&["SELECT", "1"]), "+OK");
    assert_eq!(c.cmd(&["FLUSHDB"]), "+OK");
    assert_eq!(c.keyspace(1), None);
    assert_eq!(
        c.cmd(&["SET", "again", "v", "PX", &hour.to_string()]),
        "+OK"
    );
    assert_avg(&mut c, 1, 1, 1, hour, t0);
    assert_eq!(c.cmd(&["FLUSHDB"]), "+OK");
    assert_eq!(c.cmd(&["SELECT", "2"]), "+OK");
    assert_eq!(c.cmd(&["FLUSHDB"]), "+OK");
    assert_eq!((c.keyspace(1), c.keyspace(2)), (None, None));

    // Many keys over every shard, then half of them changed: the mean of
    // what is set, exactly as a pass over them would make it.
    assert_eq!(c.cmd(&["SELECT", "3"]), "+OK");
    let n = 20_000u64;
    for i in 0..n {
        c.send(&[
            "SET",
            &format!("k{i}"),
            "v",
            "PX",
            &(hour + i * 100).to_string(),
        ]);
    }
    for _ in 0..n {
        assert_eq!(c.read(), "+OK");
    }
    for i in (0..n).step_by(2) {
        c.send(&["PERSIST", &format!("k{i}")]);
    }
    for _ in (0..n).step_by(2) {
        assert_eq!(c.read(), ":1");
    }
    // The odd ones: hour + i * 100, i = 1, 3, ..., n - 1.
    let mean = hour + 100 * n / 2;
    assert_avg(&mut c, 3, n, n / 2, mean, t0);
}

/// `INFO keysizes` goes over a shard a few buckets at a time (#622): over
/// many keys, with many buckets a shard, every key is counted once.
#[test]
fn keysizes_counts_every_key_of_a_large_database() {
    let mut c = start();
    assert_eq!(c.cmd(&["SELECT", "5"]), "+OK");
    let n = 200_000;
    for i in 0..n {
        let value = if i % 4 == 0 { "xxxxxxxxxx" } else { "x" };
        c.send(&["SET", &format!("key:{i}"), value]);
    }
    for _ in 0..n {
        assert_eq!(c.read(), "+OK");
    }
    let info = c.cmd(&["INFO", "keysizes"]);
    let line = info
        .lines()
        .find_map(|l| l.strip_prefix("db5_distrib_strings_sizes:"))
        .unwrap_or_else(|| panic!("{info}"));
    assert_eq!(line, format!("1={},8={}", n - n / 4, n / 4));
}
