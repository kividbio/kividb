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

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
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

/// The same for keys made again after `FLUSHALL`: `s`, `g`, `gd` and `sg`
/// strings, `st` a set of `a` and `b`, `st2` one of `b` and `c`, `z` a
/// sorted set of `a` and `b`, `z2` one of `b` and `c`, `h` and `l` as above.
/// The commands that combine keys look every key up, the one that is not
/// there included, and stop at a key of another type. The rows that run a
/// script are left out of a build without Lua.
#[rustfmt::skip]
const COMBINING: &[(&[&str], u64, u64)] = &[
    (&["ZDIFF", "2", "z", "z2"], 2, 0),
    (&["ZDIFF", "2", "z", "nokey"], 1, 1),
    (&["ZDIFF", "2", "nokey", "z"], 1, 1),
    (&["ZDIFF", "2", "s", "z"], 1, 0),
    (&["ZDIFFSTORE", "d", "2", "z", "z2"], 2, 0),
    (&["ZDIFFSTORE", "d", "2", "z", "nokey"], 1, 1),
    (&["ZDIFFSTORE", "d", "2", "nokey", "z"], 1, 1),
    (&["ZINTER", "2", "z", "z2"], 2, 0),
    (&["ZINTER", "2", "nokey", "z"], 1, 1),
    (&["ZINTER", "2", "z", "nokey"], 1, 1),
    (&["ZINTER", "3", "nokey", "nokey", "z"], 1, 2),
    (&["ZINTERSTORE", "d", "2", "z", "z2"], 2, 0),
    (&["ZINTERSTORE", "d", "2", "nokey", "z"], 1, 1),
    (&["ZINTERCARD", "2", "z", "z2"], 2, 0),
    (&["ZINTERCARD", "2", "nokey", "z"], 1, 1),
    (&["ZINTERCARD", "2", "z", "nokey"], 1, 1),
    (&["ZUNION", "2", "z", "z2"], 2, 0),
    (&["ZUNION", "2", "z", "nokey"], 1, 1),
    (&["ZUNIONSTORE", "d", "2", "z", "z2"], 2, 0),
    (&["ZUNIONSTORE", "d", "2", "nokey", "z"], 1, 1),
    (&["ZUNIONSTORE", "d", "2", "s", "z"], 1, 0),
    (&["SINTER", "st", "st2"], 2, 0),
    (&["SINTER", "nokey", "st"], 1, 1),
    (&["SINTER", "st", "nokey"], 1, 1),
    (&["SINTERSTORE", "d", "st", "st2"], 2, 0),
    (&["SINTERSTORE", "d", "nokey", "st"], 1, 1),
    (&["SINTERCARD", "2", "st", "st2"], 2, 0),
    (&["SINTERCARD", "2", "nokey", "st"], 1, 1),
    (&["SUNION", "st", "st2"], 2, 0),
    (&["SUNION", "st", "nokey"], 1, 1),
    (&["SUNIONSTORE", "d", "st", "st2"], 2, 0),
    (&["SUNIONSTORE", "d", "nokey", "st"], 1, 1),
    (&["SDIFF", "st", "st2"], 2, 0),
    (&["SDIFF", "st", "nokey"], 1, 1),
    (&["SDIFFSTORE", "d", "st", "st2"], 2, 0),
    (&["SDIFFSTORE", "d", "nokey", "st"], 1, 1),
    (&["COPY", "s", "s2"], 1, 0),
    (&["COPY", "nokey", "s3"], 0, 1),
    (&["DUMP", "s"], 1, 0),
    (&["DUMP", "nokey"], 0, 1),
    (&["GETEX", "g", "EX", "100"], 1, 0),
    (&["GETEX", "nokey", "EX", "100"], 0, 1),
    (&["GETDEL", "gd"], 1, 0),
    (&["GETDEL", "nokey"], 0, 1),
    (&["SET", "sg", "w", "GET"], 1, 0),
    (&["SET", "nokey2", "w", "GET"], 0, 1),
    (&["SET", "sg", "w", "NX", "GET"], 1, 0),
    (&["EVAL", "return redis.call('GET',KEYS[1])", "1", "s"], 1, 0),
    (&["EVAL", "return redis.call('GET',KEYS[1])", "1", "nokey"], 0, 1),
    (&["EVAL", "return redis.call('HGET',KEYS[1],'f')", "1", "h"], 1, 0),
    (&["EVAL", "return redis.call('SET',KEYS[1],'x')", "1", "s"], 0, 0),
    (&["BLPOP", "l", "0"], 0, 0),
    (&["BLPOP", "nokey", "0.01"], 0, 0),
    (&["BRPOP", "nokey", "0.01"], 0, 0),
];

/// Keys that have passed their time (`PEXPIRE 1`, then a wait until `EXISTS`
/// says 0 for one of them): the other is as much a miss.
#[rustfmt::skip]
const EXPIRED: &[(&[&str], u64, u64)] = &[
    (&["GET", "e1"], 0, 1),
    (&["HGET", "e1", "f"], 0, 1),
    (&["EXISTS", "e1"], 0, 1),
    (&["TTL", "e1"], 0, 1),
];

/// Runs `rows` on `c`, an INFO after each, and says which differ.
fn check(c: &mut Conn, rows: &[(&[&str], u64, u64)], lua: bool) -> Vec<String> {
    let (mut hits, mut misses) = (c.stat("keyspace_hits"), c.stat("keyspace_misses"));
    let mut wrong = Vec::new();
    for (args, want_hits, want_misses) in rows {
        if !lua && args[0] == "EVAL" {
            continue;
        }
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
    wrong
}

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
    let (hits, misses) = (c.stat("keyspace_hits"), c.stat("keyspace_misses"));
    assert_eq!((hits, misses), (0, 0));
    let lua = !c.cmd(&["EVAL", "return 1", "0"]).starts_with('-');
    let mut wrong = check(&mut c, EXPECTED, lua);

    // Keys made again, and the commands that combine them.
    c.cmd(&["FLUSHALL"]);
    for setup in [
        &["SET", "s", "v"][..],
        &["SET", "g", "v"],
        &["SET", "gd", "v"],
        &["SET", "sg", "v"],
        &["HSET", "h", "f", "1"],
        &["SADD", "st", "a", "b"],
        &["SADD", "st2", "b", "c"],
        &["ZADD", "z", "1", "a", "2", "b"],
        &["ZADD", "z2", "1", "b", "2", "c"],
        &["RPUSH", "l", "a", "b", "c"],
    ] {
        c.cmd(setup);
    }
    assert_eq!(c.cmd(&["CONFIG", "RESETSTAT"]), "+OK");
    wrong.extend(check(&mut c, COMBINING, lua));

    // A transaction counts its commands as they run, at EXEC (the INFO that
    // reads the counters goes on another connection: on this one it would
    // be queued).
    let mut other = Conn::connect(port);
    let before = (other.stat("keyspace_hits"), other.stat("keyspace_misses"));
    for args in [&["MULTI"][..], &["GET", "s"], &["GET", "nokey"], &["EXEC"]] {
        c.cmd(args);
    }
    let after = (other.stat("keyspace_hits"), other.stat("keyspace_misses"));
    if (after.0 - before.0, after.1 - before.1) != (1, 1) {
        wrong.push(format!(
            "MULTI GET s, GET nokey EXEC: {after:?} after {before:?}, Redis 8.12 1 hit and 1 miss"
        ));
    }

    // A blocking pop served later, by a push from another connection.
    c.send(&["BLPOP", "bl", "5"]);
    thread::sleep(Duration::from_millis(300));
    let before = (other.stat("keyspace_hits"), other.stat("keyspace_misses"));
    other.cmd(&["LPUSH", "bl", "x"]);
    assert_eq!(c.read_reply(), "*2");
    let after = (other.stat("keyspace_hits"), other.stat("keyspace_misses"));
    if after != before {
        wrong.push(format!(
            "BLPOP served later: {after:?} after {before:?}, Redis 8.12 counts nothing"
        ));
    }

    // Keys that have passed their time.
    for key in ["e1", "e2"] {
        c.cmd(&["SET", key, "v"]);
        c.cmd(&["PEXPIRE", key, "1"]);
    }
    while c.cmd(&["EXISTS", "e2"]) != ":0" {
        thread::sleep(Duration::from_millis(2));
    }
    wrong.extend(check(&mut c, EXPIRED, lua));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
