// Keyspace events added after Redis 8.0: `o` (overwritten) and `c`
// (type_changed) from 8.2, and `a` (array commands) from 8.10. The flags are
// the process's, so this file has one test: nothing else changes them.

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

#[test]
fn overwritten_type_changed_and_array_events() {
    let mut c = start_server();
    let port = c.stream.peer_addr().unwrap().port();
    let mut sub = Conn::connect(port);
    let subscribe = |sub: &mut Conn| {
        let V::Array(reply) = sub.cmd(&["PSUBSCRIBE", "__keyevent@0__:*"]) else {
            panic!("PSUBSCRIBE")
        };
        assert_eq!(reply[0], V::Bulk(b"psubscribe".to_vec()));
    };
    subscribe(&mut sub);

    // `o` and `c`: a value replaced whole, and by another type.
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "Eoc"]);
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "notify-keyspace-events"]),
        V::Array(vec![
            V::Bulk(b"notify-keyspace-events".to_vec()),
            V::Bulk(b"Eoc".to_vec())
        ])
    );
    c.ok(&["SET", "foo", "bar"]); // new: nothing
    c.ok(&["SET", "foo", "baz"]);
    assert_eq!(event(&mut sub), "overwritten foo");
    c.cmd(&["HSET", "h", "f", "v"]);
    c.cmd(&["HSET", "h", "g", "w"]); // changed in place: nothing
    c.ok(&["SET", "h", "string"]);
    assert_eq!(event(&mut sub), "overwritten h");
    assert_eq!(event(&mut sub), "type_changed h");
    c.cmd(&["SADD", "s1", "a", "b"]);
    c.cmd(&["SADD", "s2", "b"]);
    assert_eq!(c.cmd(&["SDIFFSTORE", "foo", "s1", "s2"]), V::Int(1));
    assert_eq!(event(&mut sub), "overwritten foo");
    assert_eq!(event(&mut sub), "type_changed foo");
    c.cmd(&["LPUSH", "l", "1"]);
    assert_eq!(c.cmd(&["COPY", "s1", "l", "REPLACE"]), V::Int(1));
    assert_eq!(event(&mut sub), "overwritten l");
    assert_eq!(event(&mut sub), "type_changed l");
    c.ok(&["RENAME", "s2", "s1"]);
    assert_eq!(event(&mut sub), "overwritten s1");
    // SET NX on a key that is there writes nothing.
    assert_eq!(c.cmd(&["SET", "h", "x", "NX"]), V::Nil);
    c.ok(&["SET", "marker", "1"]);
    c.ok(&["SET", "marker", "2"]);
    assert_eq!(event(&mut sub), "overwritten marker", "nothing for SET NX");

    // With the command's own events: `overwritten` first for SET (Redis's
    // setKey), after them for RENAME.
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "E$go"]);
    c.ok(&["SET", "order", "1"]);
    assert_eq!(event(&mut sub), "set order");
    c.ok(&["SET", "order", "2"]);
    assert_eq!(event(&mut sub), "overwritten order");
    assert_eq!(event(&mut sub), "set order");
    c.ok(&["SET", "order2", "x"]);
    assert_eq!(event(&mut sub), "set order2");
    c.ok(&["RENAME", "order2", "order"]);
    assert_eq!(event(&mut sub), "rename_from order2");
    assert_eq!(event(&mut sub), "rename_to order");
    assert_eq!(event(&mut sub), "overwritten order");

    // `a`: array commands, and `del` when one empties the array.
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "EA"]);
    assert_eq!(c.cmd(&["ARSET", "arr", "0", "a", "b"]), V::Int(2));
    assert_eq!(event(&mut sub), "arset arr");
    assert_eq!(c.cmd(&["ARDEL", "arr", "5"]), V::Int(0)); // nothing deleted
    assert_eq!(c.cmd(&["ARDEL", "arr", "0"]), V::Int(1));
    assert_eq!(event(&mut sub), "ardel arr");
    assert_eq!(c.cmd(&["ARDELRANGE", "arr", "0", "9"]), V::Int(1));
    assert_eq!(event(&mut sub), "ardelrange arr");
    assert_eq!(event(&mut sub), "del arr");
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "notify-keyspace-events"]),
        V::Array(vec![
            V::Bulk(b"notify-keyspace-events".to_vec()),
            V::Bulk(b"AE".to_vec())
        ])
    );
    // An index that is not one is Redis 8.10's error, whether or not the
    // array is there.
    let V::Error(e) = c.cmd(&["ARGET", "missing", "not-an-index"]) else {
        panic!("ARGET")
    };
    assert_eq!(e, "ERR invalid array index");
    // INCREX's events are INCRBY's and INCRBYFLOAT's, and then expire.
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "E$g"]);
    c.cmd(&["INCREX", "n", "BYINT", "2"]);
    assert_eq!(event(&mut sub), "incrby n");
    c.cmd(&["INCREX", "n", "BYFLOAT", "0.5", "EX", "100"]);
    assert_eq!(event(&mut sub), "incrbyfloat n");
    assert_eq!(event(&mut sub), "expire n");

    // Events as Redis names them, from the commands that announced nothing
    // too, and `keymiss` (#160).
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "EAm"]);
    c.ok(&["SET", "num", "1"]);
    assert_eq!(event(&mut sub), "set num");
    for command in [
        &["INCR", "num"][..],
        &["DECR", "num"],
        &["DECRBY", "num", "2"],
    ] {
        c.cmd(command);
        assert_eq!(event(&mut sub), "incrby num", "{command:?}");
    }
    c.cmd(&["GETSET", "num", "5"]);
    assert_eq!(event(&mut sub), "set num");
    c.cmd(&["SETNX", "fresh", "1"]);
    assert_eq!(event(&mut sub), "set fresh");
    c.cmd(&["GETEX", "fresh", "EX", "100"]);
    assert_eq!(event(&mut sub), "expire fresh");
    c.cmd(&["GETEX", "fresh", "PERSIST"]);
    assert_eq!(event(&mut sub), "persist fresh");
    c.cmd(&["GETDEL", "fresh"]);
    assert_eq!(event(&mut sub), "del fresh");
    c.cmd(&["RPUSH", "src", "a", "b"]);
    assert_eq!(event(&mut sub), "rpush src");
    c.cmd(&["LMOVE", "src", "dst", "LEFT", "RIGHT"]);
    // Redis's order (`lmoveGenericCommand`): the push, then the pop (#173).
    assert_eq!(event(&mut sub), "rpush dst");
    assert_eq!(event(&mut sub), "lpop src");
    c.cmd(&["SETRANGE", "num", "0", "9"]);
    assert_eq!(event(&mut sub), "setrange num");
    c.cmd(&["SETBIT", "bits", "1", "1"]);
    assert_eq!(event(&mut sub), "setbit bits");
    c.cmd(&["PFADD", "hll", "a"]);
    assert_eq!(event(&mut sub), "pfadd hll");
    c.cmd(&["GEOADD", "g", "13.36", "38.11", "p"]);
    assert_eq!(event(&mut sub), "zadd g");
    assert_eq!(c.cmd(&["MOVE", "g", "1"]), V::Int(1));
    assert_eq!(event(&mut sub), "move_from g");
    c.cmd(&["EXISTS", "nothing-here"]); // only asks: no keymiss
    assert_eq!(c.cmd(&["GET", "missing"]), V::Nil);
    assert_eq!(event(&mut sub), "keymiss missing");
    c.ok(&["JSON.SET", "j", "$", "{\"a\":1}"]);
    assert_eq!(event(&mut sub), "json.set j");

    // A flag that is not one is refused, and the flags stay as they were.
    assert!(matches!(
        c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "KEAQ!"]),
        V::Error(_)
    ));
    c.ok(&["SET", "after", "1"]);
    assert_eq!(event(&mut sub), "set after");

    // A move that empties its source, in Redis's order: the push to the
    // destination, the pop from the source, then its delete (#173). The
    // delete came first, and the pop before the push.
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", "Elg"]);
    assert_eq!(c.cmd(&["RPUSH", "from", "x"]), V::Int(1));
    assert_eq!(event(&mut sub), "rpush from");
    assert_eq!(
        c.cmd(&["LMOVE", "from", "to", "LEFT", "RIGHT"]),
        V::Bulk(b"x".to_vec())
    );
    assert_eq!(event(&mut sub), "rpush to");
    assert_eq!(event(&mut sub), "lpop from");
    assert_eq!(event(&mut sub), "del from");
    // One that does not empty it has no delete.
    c.cmd(&["RPUSH", "from", "y", "z"]);
    assert_eq!(event(&mut sub), "rpush from");
    c.cmd(&["RPOPLPUSH", "from", "to"]);
    assert_eq!(event(&mut sub), "lpush to");
    assert_eq!(event(&mut sub), "rpop from");
    c.ok(&["SET", "marker2", "1"]); // not an `l` or `g` event: nothing
    c.cmd(&["DEL", "marker2"]);
    assert_eq!(
        event(&mut sub),
        "del marker2",
        "no delete for a source left with y"
    );
    c.ok(&["CONFIG", "SET", "notify-keyspace-events", ""]);
}
