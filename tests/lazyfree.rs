// Freeing in the background, and INFO's count of it (#19): what UNLINK,
// FLUSHALL / FLUSHDB [ASYNC] and FUNCTION FLUSH ASYNC add to
// `lazyfreed_objects`, as the Redis suite's unit/lazyfree checks.
//
// The counters are the process's, not a server's: the tests here take turns.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

static COUNTERS: Mutex<()> = Mutex::new(());

fn counters() -> std::sync::MutexGuard<'static, ()> {
    COUNTERS.lock().unwrap_or_else(|e| e.into_inner())
}

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

    /// A number `INFO` reports.
    fn info(&mut self, field: &str) -> u64 {
        let V::Bulk(info) = self.cmd(&["INFO", "memory"]) else {
            panic!("INFO is a bulk string");
        };
        let prefix = format!("{field}:");
        String::from_utf8(info)
            .unwrap()
            .lines()
            .find_map(|line| {
                line.strip_prefix(&prefix)
                    .map(|v| v.trim().parse().unwrap())
            })
            .unwrap_or_else(|| panic!("{field} in INFO memory"))
    }

    /// `lazyfreed_objects`, once nothing is pending.
    fn freed_when_done(&mut self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.info("lazyfree_pending_objects") != 0 {
            assert!(Instant::now() < deadline, "lazyfree is not done");
            thread::sleep(Duration::from_millis(20));
        }
        self.info("lazyfreed_objects")
    }

    /// A set of `n` members at `key`.
    fn big_set(&mut self, key: &str, n: usize) {
        let members: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        let mut args = vec!["SADD", key];
        args.extend(members.iter().map(String::as_str));
        assert_eq!(self.cmd(&args), V::Int(n as i64));
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

#[test]
fn unlink_frees_a_large_value_in_the_background_and_counts_it() {
    let _turn = counters();
    let mut c = start_server();
    c.ok(&["CONFIG", "RESETSTAT"]);
    assert_eq!(c.info("lazyfreed_objects"), 0);

    c.big_set("big", 1000);
    assert_eq!(c.cmd(&["UNLINK", "big"]), V::Int(1));
    assert_eq!(c.freed_when_done(), 1, "a set of 1000 members");
    assert_eq!(c.cmd(&["EXISTS", "big"]), V::Int(0));

    // Small values are freed in place, and DEL frees in place whatever the size.
    c.big_set("small", 10);
    c.ok(&["SET", "s", "v"]);
    assert_eq!(c.cmd(&["UNLINK", "small", "s", "missing"]), V::Int(2));
    c.big_set("big", 1000);
    assert_eq!(c.cmd(&["DEL", "big"]), V::Int(1));
    assert_eq!(c.freed_when_done(), 1);

    // A stream: its nodes and its groups' pending entries are the work.
    c.ok(&["CONFIG", "SET", "stream-node-max-entries", "5"]);
    for i in 0..1000 {
        let id = c.cmd(&["XADD", "stream", "*", "foo", &i.to_string()]);
        assert!(matches!(id, V::Bulk(_)), "{id:?}");
    }
    c.ok(&["XGROUP", "CREATE", "stream", "mygroup", "0"]);
    c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "mygroup",
        "Alice",
        "COUNT",
        "2",
        "STREAMS",
        "stream",
        ">",
    ]);
    assert_eq!(c.cmd(&["UNLINK", "stream"]), V::Int(1));
    assert_eq!(c.freed_when_done(), 2, "a stream of 200 nodes");

    // One entry and a group destroyed: freed in place.
    c.cmd(&["XADD", "s", "*", "a", "b"]);
    c.ok(&["XGROUP", "CREATE", "s", "bla", "$"]);
    assert_eq!(c.cmd(&["XGROUP", "DESTROY", "s", "bla"]), V::Int(1));
    assert_eq!(c.cmd(&["UNLINK", "s"]), V::Int(1));
    assert_eq!(c.freed_when_done(), 2);

    c.ok(&["CONFIG", "RESETSTAT"]);
    assert_eq!(c.info("lazyfreed_objects"), 0);
}

#[test]
fn flushes_count_their_keys_except_in_a_transaction() {
    let _turn = counters();
    let mut c = start_server();
    c.ok(&["CONFIG", "RESETSTAT"]);

    // A plain FLUSHALL is, in Redis, an ASYNC flush the client waits for:
    // counted by the time it replies.
    for i in 0..100 {
        c.ok(&["SET", &format!("k{i}"), "v"]);
    }
    c.ok(&["FLUSHALL"]);
    assert_eq!(c.info("lazyfreed_objects"), 100);
    c.ok(&["SET", "x", "1"]);
    c.ok(&["SET", "y", "2"]);
    c.ok(&["FLUSHALL"]);
    c.ok(&["FLUSHALL"]);
    c.ok(&["FLUSHDB"]);
    assert_eq!(c.info("lazyfreed_objects"), 102);

    // Not in a transaction.
    c.ok(&["CONFIG", "RESETSTAT"]);
    c.ok(&["SET", "x", "11"]);
    c.ok(&["SET", "y", "22"]);
    c.ok(&["MULTI"]);
    assert_eq!(c.cmd(&["FLUSHALL"]), V::Simple("QUEUED".into()));
    assert_eq!(c.cmd(&["EXEC"]), V::Array(vec![V::Simple("OK".into())]));
    assert_eq!(c.cmd(&["DBSIZE"]), V::Int(0));
    assert_eq!(c.freed_when_done(), 0);

    // ASYNC: in the background, and only the database flushed.
    c.ok(&["SELECT", "1"]);
    c.big_set("other", 100);
    c.ok(&["SELECT", "0"]);
    for i in 0..1000 {
        c.ok(&["SET", &format!("k{i}"), "v", "EX", "1000"]);
    }
    c.big_set("big", 1000);
    c.ok(&["FLUSHDB", "ASYNC"]);
    assert_eq!(c.cmd(&["DBSIZE"]), V::Int(0));
    assert_eq!(c.cmd(&["TTL", "k1"]), V::Int(-2));
    assert_eq!(c.freed_when_done(), 1001, "the keys, not their TTLs too");
    c.ok(&["SELECT", "1"]);
    assert_eq!(c.cmd(&["SCARD", "other"]), V::Int(100));
    c.ok(&["FLUSHALL", "ASYNC"]);
    assert_eq!(c.cmd(&["DBSIZE"]), V::Int(0));
    assert_eq!(c.freed_when_done(), 1002);
}

#[cfg(feature = "lua")]
#[test]
fn a_flushall_in_a_script_is_not_counted() {
    let _turn = counters();
    let mut c = start_server();
    c.ok(&["CONFIG", "RESETSTAT"]);
    c.ok(&["SET", "x", "1"]);
    assert_eq!(
        c.cmd(&["EVAL", "return redis.call('flushall')", "0"]),
        V::Simple("OK".into())
    );
    assert_eq!(c.cmd(&["DBSIZE"]), V::Int(0));
    assert_eq!(c.freed_when_done(), 0);
}

/// The Redis suite's "async function flush rebuilds Lua VM without causing
/// race condition between main and lazyfree thread".
#[cfg(feature = "lua")]
#[test]
fn function_flush_async_frees_the_functions_vm_in_the_background() {
    let _turn = counters();
    let mut c = start_server();
    for i in 0..200 {
        let code = format!(
            "#!lua name=test{i}\nredis.register_function('test{i}', function() return {i} end)"
        );
        assert_eq!(
            c.cmd(&["FUNCTION", "LOAD", &code]),
            V::Bulk(format!("test{i}").into())
        );
    }
    // Well above an empty VM's; what is collected as they load (the
    // collector is stepped every 50 loads) is not in it.
    let loaded = c.info("used_memory_vm_functions");
    assert!(
        loaded > 60_000,
        "the VM holding 200 functions is {loaded} bytes"
    );

    c.ok(&["CONFIG", "RESETSTAT"]);
    c.ok(&["FUNCTION", "FLUSH", "ASYNC"]);
    assert_eq!(c.info("used_memory_vm_functions"), 0);
    let code = "#!lua name=test\nredis.register_function('test', function() return 1 end)";
    assert_eq!(
        c.cmd(&["FUNCTION", "LOAD", "REPLACE", code]),
        V::Bulk(b"test".to_vec())
    );
    assert_eq!(c.cmd(&["FCALL", "test", "0"]), V::Int(1));
    assert_eq!(c.freed_when_done(), 201, "200 functions and the VM");

    // SYNC frees in place.
    c.ok(&["FUNCTION", "FLUSH", "SYNC"]);
    assert_eq!(c.freed_when_done(), 201);
    assert_eq!(c.cmd(&["FUNCTION", "LIST"]), V::Array(vec![]));
}
