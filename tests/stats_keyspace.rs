#![allow(clippy::disallowed_methods)]
// tests/stats_keyspace.rs
//
// `keyspace_hits` and `keyspace_misses` of INFO stats, counted as Redis counts
// them: a command that reads a key (`lookupKeyRead` there) counts a hit when
// the key is there, whatever its type and whether or not the member, field or
// index asked for is in it, and a miss when it is not; a command that writes
// counts neither. Its counters are the process's, so the one test here has a
// process, and a server, of its own.
//
// The expected numbers are what Redis 8.12.0 (the `redis:8.12` image,
// jemalloc 5.3.0) gave for the same commands in the same order on a server
// that had run `CONFIG RESETSTAT` after the keys were made: one row for each
// command, its hits and its misses.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

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
    let from = (std::process::id() as u16).wrapping_mul(37);
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
                    .set_read_timeout(Some(Duration::from_secs(5)))
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

    /// Reads one whole reply, nested arrays included, and returns its first
    /// line (an array's header, a bulk string's text).
    fn read_reply(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        match line.as_bytes().first() {
            Some(b'$') => {
                let n: i64 = line[1..].parse().unwrap();
                if n < 0 {
                    return String::new();
                }
                let mut text = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut text).unwrap();
                text.truncate(n as usize);
                String::from_utf8_lossy(&text).into_owned()
            }
            Some(b'*') => {
                let n: i64 = line[1..].parse().unwrap();
                for _ in 0..n.max(0) {
                    self.read_reply();
                }
                line
            }
            _ => line,
        }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
        self.read_reply()
    }

    fn stat(&mut self, name: &str) -> u64 {
        let info = self.cmd(&["INFO", "stats"]);
        info.lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("no {name} in {info}"))
            .parse()
            .unwrap()
    }
}

/// (command, keyspace hits it counts, keyspace misses it counts), from Redis
/// 8.12.0. `s` is a string, `n` a string holding 5, `h` a hash of the field
/// `f`, `st` a set of `a` and `b`, `z` a sorted set of `a` and `b`, `l` a
/// list of `a`, `b` and `c`; `nokey` is not there.
#[rustfmt::skip]
const EXPECTED: &[(&[&str], u64, u64)] = &[
    (&["LRANGE", "l", "0", "-1"], 1, 0),
    (&["LRANGE", "l", "50", "60"], 1, 0),
    (&["LRANGE", "nokey", "0", "-1"], 0, 1),
    (&["LRANGE", "s", "0", "-1"], 1, 0),
    (&["LINDEX", "l", "0"], 1, 0),
    (&["LINDEX", "l", "99"], 1, 0),
    (&["LINDEX", "nokey", "0"], 0, 1),
    (&["LINDEX", "s", "0"], 1, 0),
    (&["LLEN", "l"], 1, 0),
    (&["LLEN", "nokey"], 0, 1),
    (&["LLEN", "s"], 1, 0),
    (&["LPOS", "l", "a"], 1, 0),
    (&["LPOS", "l", "zz"], 1, 0),
    (&["LPOS", "nokey", "a"], 0, 1),
    (&["LPOS", "s", "a"], 1, 0),
    (&["GET", "s"], 1, 0),
    (&["GET", "nokey"], 0, 1),
    (&["GET", "h"], 1, 0),
    (&["HGET", "h", "f"], 1, 0),
    (&["HGET", "h", "nof"], 1, 0),
    (&["HGET", "nokey", "f"], 0, 1),
    (&["HGET", "s", "f"], 1, 0),
    (&["HGETALL", "h"], 1, 0),
    (&["HGETALL", "nokey"], 0, 1),
    (&["HGETALL", "s"], 1, 0),
    (&["HMGET", "h", "f", "nof"], 1, 0),
    (&["HEXISTS", "h", "nof"], 1, 0),
    (&["HLEN", "h"], 1, 0),
    (&["HLEN", "nokey"], 0, 1),
    (&["HKEYS", "h"], 1, 0),
    (&["HVALS", "h"], 1, 0),
    (&["HSTRLEN", "h", "f"], 1, 0),
    (&["SMEMBERS", "st"], 1, 0),
    (&["SMEMBERS", "nokey"], 0, 1),
    (&["SMEMBERS", "s"], 1, 0),
    (&["SISMEMBER", "st", "a"], 1, 0),
    (&["SISMEMBER", "st", "zz"], 1, 0),
    (&["SISMEMBER", "nokey", "a"], 0, 1),
    (&["SISMEMBER", "s", "a"], 1, 0),
    (&["SCARD", "st"], 1, 0),
    (&["SCARD", "nokey"], 0, 1),
    (&["ZRANGE", "z", "0", "-1"], 1, 0),
    (&["ZRANGE", "nokey", "0", "-1"], 0, 1),
    (&["ZRANGE", "s", "0", "-1"], 1, 0),
    (&["ZSCORE", "z", "a"], 1, 0),
    (&["ZSCORE", "z", "zz"], 1, 0),
    (&["ZSCORE", "nokey", "a"], 0, 1),
    (&["ZSCORE", "s", "a"], 1, 0),
    (&["ZCARD", "z"], 1, 0),
    (&["ZCARD", "nokey"], 0, 1),
    (&["ZRANK", "z", "zz"], 1, 0),
    (&["ZCOUNT", "z", "0", "5"], 1, 0),
    (&["ZRANGEBYSCORE", "z", "0", "5"], 1, 0),
    (&["ZREVRANGE", "z", "0", "-1"], 1, 0),
    (&["EXISTS", "s"], 1, 0),
    (&["EXISTS", "nokey"], 0, 1),
    (&["EXISTS", "s", "nokey", "n"], 2, 1),
    (&["TYPE", "s"], 1, 0),
    (&["TYPE", "nokey"], 0, 1),
    (&["TTL", "s"], 1, 0),
    (&["TTL", "nokey"], 0, 1),
    (&["PTTL", "s"], 1, 0),
    (&["EXPIRETIME", "s"], 1, 0),
    (&["TOUCH", "s", "nokey"], 1, 1),
    (&["OBJECT", "ENCODING", "s"], 1, 0),
    (&["OBJECT", "ENCODING", "nokey"], 0, 1),
    (&["MGET", "s", "nokey", "h"], 2, 1),
    (&["STRLEN", "s"], 1, 0),
    (&["STRLEN", "nokey"], 0, 1),
    (&["STRLEN", "h"], 1, 0),
    (&["GETRANGE", "s", "0", "-1"], 1, 0),
    (&["GETRANGE", "nokey", "0", "-1"], 0, 1),
    (&["SINTER", "st", "st"], 2, 0),
    (&["SINTER", "st", "nokey"], 1, 1),
    (&["SUNION", "st", "nokey"], 1, 1),
    (&["SDIFF", "nokey", "st"], 1, 1),
    (&["SINTERCARD", "2", "st", "nokey"], 1, 1),
    (&["LPUSH", "l", "x"], 0, 0),
    (&["LPUSHX", "l", "y"], 0, 0),
    (&["LPUSHX", "nokey", "y"], 0, 0),
    (&["LPOP", "l"], 0, 0),
    (&["LPOP", "nokey"], 0, 0),
    (&["LPOP", "s"], 0, 0),
    (&["LMPOP", "1", "l", "LEFT"], 0, 0),
    (&["LMPOP", "1", "nokey", "LEFT"], 0, 0),
    (&["HDEL", "h", "nof"], 0, 0),
    (&["HDEL", "nokey", "f"], 0, 0),
    (&["SREM", "st", "zz"], 0, 0),
    (&["SREM", "nokey", "a"], 0, 0),
    (&["SPOP", "nokey"], 0, 0),
    (&["SMOVE", "nokey", "st", "a"], 0, 0),
    (&["SET", "s", "w"], 0, 0),
    (&["SET", "nn", "1"], 0, 0),
    (&["INCR", "n"], 0, 0),
    (&["DEL", "nn"], 0, 0),
    (&["APPEND", "s", "x"], 0, 0),
    (&["SADD", "st", "c"], 0, 0),
    (&["ZADD", "z", "3", "c"], 0, 0),
    (&["HSET", "h", "g", "2"], 0, 0),
    (&["HGETDEL", "h", "FIELDS", "1", "nof"], 0, 0),
    (&["HGETEX", "nokey", "EX", "100", "FIELDS", "1", "f"], 0, 0),
];

#[test]
fn keyspace_hits_and_misses_are_counted_as_redis_counts_them() {
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
    let mut c = Conn::connect(port);
    for setup in [
        &["SET", "s", "v"][..],
        &["HSET", "h", "f", "1"],
        &["SADD", "st", "a", "b"],
        &["ZADD", "z", "1", "a", "2", "b"],
        &["RPUSH", "l", "a", "b", "c"],
        &["SET", "n", "5"],
    ] {
        c.cmd(setup);
    }
    assert_eq!(c.cmd(&["CONFIG", "RESETSTAT"]), "+OK");
    let (mut hits, mut misses) = (c.stat("keyspace_hits"), c.stat("keyspace_misses"));
    assert_eq!((hits, misses), (0, 0));

    let mut wrong = Vec::new();
    for (args, want_hits, want_misses) in EXPECTED {
        c.cmd(args);
        let (h, m) = (c.stat("keyspace_hits"), c.stat("keyspace_misses"));
        if (h - hits, m - misses) != (*want_hits, *want_misses) {
            wrong.push(format!(
                "{}: {} hits and {} misses, Redis 8.12 {want_hits} and {want_misses}",
                args.join(" "),
                h - hits,
                m - misses
            ));
        }
        (hits, misses) = (h, m);
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
