// A write over a key past its time: counted as expired, and announced as
// `expired` before the write's own event. The stats and event flags are the
// process's, so this file has one test.

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

fn expired_keys(c: &mut Conn) -> i64 {
    match c.cmd(&["INFO", "stats"]) {
        V::Bulk(info) => String::from_utf8(info)
            .unwrap()
            .lines()
            .find_map(|l| {
                l.strip_prefix("expired_keys:")
                    .map(|n| n.trim().parse().unwrap())
            })
            .unwrap(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_write_over_an_expired_key_counts_and_announces_it() {
    let mut c = start_server();
    let port = c.stream.peer_addr().unwrap().port();

    // Counted whether the write or active expiry gets to the key first.
    let before = expired_keys(&mut c);
    for i in 0..10 {
        let key = format!("counted:{i}");
        c.ok(&["SET", &key, "100", "PX", "20"]);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(c.cmd(&["INCR", &key]), V::Int(1));
    }
    thread::sleep(Duration::from_millis(300));
    assert_eq!(expired_keys(&mut c) - before, 10);

    // `expired` comes before the write's event.
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "Ex$l"]);
    let mut sub = Conn::connect(port);
    sub.cmd(&["PSUBSCRIBE", "__keyevent@0__:*"]);
    for i in 0..10 {
        let key = format!("announced:{i}");
        let write = if i % 2 == 0 {
            c.ok(&["SET", &key, "100", "PX", "20"]);
            "incrby"
        } else {
            c.cmd(&["RPUSH", &key, "a"]);
            c.cmd(&["PEXPIRE", &key, "20"]);
            "rpush"
        };
        thread::sleep(Duration::from_millis(30));
        let reply = if write == "incrby" {
            c.cmd(&["INCR", &key])
        } else {
            c.cmd(&["RPUSH", &key, "b"])
        };
        assert_eq!(reply, V::Int(1), "{key}");
        let mut seen = Vec::new();
        loop {
            let e = event(&mut sub);
            if !e.ends_with(&format!(" {key}")) {
                continue;
            }
            let done = e.starts_with(write) && (write == "incrby" || seen.len() > 1);
            seen.push(e);
            if done {
                break;
            }
        }
        let tail: Vec<&str> = seen[seen.len() - 2..].iter().map(|e| e.as_str()).collect();
        assert_eq!(
            tail,
            [format!("expired {key}"), format!("{write} {key}")],
            "{seen:?}"
        );
    }
    drop(sub);

    // MOVE announces nothing of its own. Over a key past its time, `expired`
    // still comes at once, in the key's own database, once, and is counted
    // once, whatever the client does next in another database.
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]);
    let mut sub = Conn::connect(port);
    sub.cmd(&["PSUBSCRIBE", "__keyevent@*__:expired"]);
    // `SORT ... STORE` announces nothing at all here.
    c.ok(&["SELECT", "0"]);
    c.cmd(&["RPUSH", "sort:src", "1"]);
    for i in 0..3 {
        let key = format!("sorted:{i}");
        // Before its time: active expiry may yet take it, and counts it too.
        let before = expired_keys(&mut c);
        c.ok(&["SET", &key, "old", "PX", "20"]);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(c.cmd(&["SORT", "sort:src", "STORE", &key]), V::Int(1));
        assert_eq!(
            next_event(&mut sub, Duration::from_millis(500)),
            Some(("__keyevent@0__:expired".to_string(), key.clone())),
            "{key}"
        );
        assert_eq!(
            next_event(&mut sub, Duration::from_millis(200)),
            None,
            "{key}"
        );
        assert_eq!(expired_keys(&mut c) - before, 1, "{key}");
    }
    for i in 0..6 {
        let key = format!("moved:{i}");
        let before = expired_keys(&mut c);
        c.ok(&["SELECT", "0"]);
        c.ok(&["SET", &key, "old", "PX", "20"]);
        c.ok(&["SELECT", "1"]);
        c.ok(&["SET", &key, "new"]);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(c.cmd(&["MOVE", &key, "0"]), V::Int(1), "{key}");
        let wait_first = i % 2 == 0;
        if !wait_first {
            // An event of another database right after.
            c.ok(&["SET", &format!("other:{i}"), "x"]);
        }
        assert_eq!(
            next_event(&mut sub, Duration::from_millis(500)),
            Some(("__keyevent@0__:expired".to_string(), key.clone())),
            "{key}"
        );
        if wait_first {
            c.ok(&["SET", &format!("other:{i}"), "x"]);
        }
        assert_eq!(
            next_event(&mut sub, Duration::from_millis(200)),
            None,
            "{key}"
        );
        assert_eq!(expired_keys(&mut c) - before, 1, "{key}");
        c.ok(&["SELECT", "0"]);
        assert_eq!(c.cmd(&["GET", &key]), V::Bulk(b"new".to_vec()));
        assert_eq!(c.cmd(&["TTL", &key]), V::Int(-1));
    }

    // Writers on several connections at once, in two databases, some of
    // them on one shared key: each key past its time is announced once, in
    // its own database, and counted once.
    let before = expired_keys(&mut c);
    for db in ["0", "1"] {
        c.ok(&["SELECT", db]);
        for w in 0..4 {
            for i in 0..10 {
                c.ok(&["SET", &format!("conc:{db}:{w}:{i}"), "1", "PX", "20"]);
            }
        }
        c.ok(&["SET", &format!("shared:{db}"), "1", "PX", "20"]);
    }
    thread::sleep(Duration::from_millis(30));
    let writers: Vec<_> = (0..8)
        .map(|w| {
            thread::spawn(move || {
                let mut c = Conn::connect(port);
                let db = (w % 2).to_string();
                c.ok(&["SELECT", &db]);
                c.cmd(&["INCR", &format!("shared:{db}")]);
                for i in 0..10 {
                    let key = format!("conc:{db}:{}:{i}", w / 2);
                    assert_eq!(c.cmd(&["INCR", &key]), V::Int(1), "{key}");
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    let mut seen: Vec<(String, String)> = Vec::new();
    while let Some(e) = next_event(&mut sub, Duration::from_millis(300)) {
        seen.push(e);
    }
    for db in ["0", "1"] {
        let mut keys: Vec<String> = (0..4)
            .flat_map(|w| (0..10).map(move |i| format!("conc:{db}:{w}:{i}")))
            .collect();
        keys.push(format!("shared:{db}"));
        for key in keys {
            let on: Vec<&str> = seen
                .iter()
                .filter(|(_, k)| *k == key)
                .map(|(ch, _)| ch.as_str())
                .collect();
            assert_eq!(on, [format!("__keyevent@{db}__:expired")], "{key}");
        }
    }
    assert_eq!(expired_keys(&mut c) - before, 2 * 41);

    // SWAPDB moves a key with its database: past its time after the swap,
    // it is announced in the database it is now in.
    let before = expired_keys(&mut c);
    c.ok(&["SELECT", "0"]);
    c.ok(&["SET", "swapped", "old", "PX", "60"]);
    c.ok(&["SWAPDB", "0", "1"]);
    thread::sleep(Duration::from_millis(100));
    c.ok(&["SELECT", "1"]);
    assert_eq!(c.cmd(&["SORT", "sort:src", "STORE", "swapped"]), V::Int(1));
    assert_eq!(
        next_event(&mut sub, Duration::from_millis(500)),
        Some(("__keyevent@1__:expired".to_string(), "swapped".to_string()))
    );
    assert_eq!(next_event(&mut sub, Duration::from_millis(200)), None);
    assert_eq!(expired_keys(&mut c) - before, 1);
}

/// The next `pmessage`'s channel and message, if one comes within `wait`.
fn next_event(sub: &mut Conn, wait: Duration) -> Option<(String, String)> {
    sub.stream.set_read_timeout(Some(wait)).unwrap();
    let ready = match sub.reader.fill_buf() {
        Ok(buf) => !buf.is_empty(),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            false
        }
        Err(e) => panic!("{e}"),
    };
    sub.stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    if !ready {
        return None;
    }
    match sub.read() {
        V::Array(parts) => match &parts[..] {
            [_, _, V::Bulk(channel), V::Bulk(message)] => Some((
                String::from_utf8_lossy(channel).into_owned(),
                String::from_utf8_lossy(message).into_owned(),
            )),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}
