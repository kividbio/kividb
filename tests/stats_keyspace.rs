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

/// Keys of every type, made again (after `FLUSHALL`) for each row of
/// [`FRESH`]: `s` a string, `b` one with bit 7 set, `p` a HyperLogLog, `h` a
/// hash of `f` and `g` (`g` with a time to live), `st`, `st2`, `z`, `z2`, `l`
/// and `l2` as above (`l2` of numbers), `x` a stream with group `g` that
/// consumer `c1` has read, `geo` two places, `j` a JSON document, `ts` a time
/// series labelled `a=b`, `vs` a vector set of `e1` and `e2`, `bf`, `cf`,
/// `cms`, `tk` and `td` a Bloom filter, a cuckoo filter, a count-min sketch,
/// a top-k and a t-digest.
#[rustfmt::skip]
const FIXTURES: &[&[&str]] = &[
    &["SET", "s", "hello"], &["SET", "n", "5"], &["SETBIT", "b", "7", "1"], &["PFADD", "p", "a", "b"],
    &["HSET", "h", "f", "1", "g", "2"], &["HEXPIRE", "h", "1000", "FIELDS", "1", "g"],
    &["SADD", "st", "a", "b"], &["SADD", "st2", "b", "c"],
    &["ZADD", "z", "1", "a", "2", "b"], &["ZADD", "z2", "1", "b", "2", "c"],
    &["RPUSH", "l", "c", "a", "b"], &["RPUSH", "l2", "3", "1", "2"],
    &["XADD", "x", "1-1", "f", "v"], &["XGROUP", "CREATE", "x", "g", "0"],
    &["XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "x", ">"],
    &["GEOADD", "geo", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"],
    &["JSON.SET", "j", "$", r#"{"a":1,"arr":[1,2],"o":{"k":"v"},"str":"abc"}"#],
    &["TS.CREATE", "ts", "LABELS", "a", "b"], &["TS.ADD", "ts", "1", "1"],
    &["VADD", "vs", "VALUES", "2", "1", "0", "e1"], &["VADD", "vs", "VALUES", "2", "0", "1", "e2"],
    &["BF.ADD", "bf", "a"], &["CF.ADD", "cf", "a"], &["CMS.INITBYDIM", "cms", "10", "2"],
    &["TOPK.RESERVE", "tk", "3"], &["TOPK.ADD", "tk", "a"],
    &["TDIGEST.CREATE", "td"], &["TDIGEST.ADD", "td", "1", "2"],
];

/// (command, hits, misses) from Redis 8.12.0, each row on fresh
/// [`FIXTURES`] after `CONFIG RESETSTAT`: a key that is there (`hit`), one
/// that is not (`nokey`) and one of another type, for the bit commands,
/// `PFCOUNT` and `PFMERGE`, `LCS`, the field-expiry reads, the streams',
/// the geo commands', `OBJECT REFCOUNT`, `SORT` and `SORT_RO` (the keys its
/// patterns name count too), `DIGEST`, and JSON's, time series', vector
/// sets' and the probabilistic types' reads. Writes that read another key
/// first count that key (`BITOP`, `PFMERGE`, `GEOSEARCHSTORE`), and
/// `XREAD` and `XREADGROUP` count a stream twice.
#[rustfmt::skip]
const FRESH: &[(&[&str], u64, u64)] = &[
    (&["GETBIT", "b", "7"], 1, 0),
    (&["GETBIT", "nokey", "7"], 0, 1),
    (&["GETBIT", "h", "7"], 1, 0),
    (&["BITCOUNT", "b"], 1, 0),
    (&["BITCOUNT", "nokey"], 0, 1),
    (&["BITCOUNT", "h"], 1, 0),
    (&["BITCOUNT", "b", "0", "-1"], 1, 0),
    (&["BITCOUNT", "nokey", "0", "-1"], 0, 1),
    (&["BITCOUNT", "h", "0", "-1"], 1, 0),
    (&["BITPOS", "b", "1"], 1, 0),
    (&["BITPOS", "nokey", "1"], 0, 1),
    (&["BITPOS", "h", "1"], 1, 0),
    (&["BITPOS", "b", "0", "0", "-1"], 1, 0),
    (&["BITPOS", "nokey", "0", "0", "-1"], 0, 1),
    (&["BITPOS", "h", "0", "0", "-1"], 1, 0),
    (&["BITFIELD_RO", "b", "GET", "u8", "0"], 1, 0),
    (&["BITFIELD_RO", "nokey", "GET", "u8", "0"], 0, 1),
    (&["BITFIELD_RO", "h", "GET", "u8", "0"], 1, 0),
    (&["BITFIELD", "b", "GET", "u8", "0"], 1, 0),
    (&["BITFIELD", "nokey", "GET", "u8", "0"], 0, 1),
    (&["BITFIELD", "h", "GET", "u8", "0"], 1, 0),
    (&["LCS", "s", "s"], 2, 0),
    (&["LCS", "nokey", "s"], 1, 1),
    (&["LCS", "h", "s"], 2, 0),
    (&["LCS", "nokey", "nokey"], 0, 2),
    (&["LCS", "h", "h"], 2, 0),
    (&["LCS", "s", "s", "LEN"], 2, 0),
    (&["LCS", "nokey", "s", "LEN"], 1, 1),
    (&["LCS", "h", "s", "LEN"], 2, 0),
    (&["PFCOUNT", "p"], 1, 0),
    (&["PFCOUNT", "nokey"], 0, 1),
    (&["PFCOUNT", "h"], 1, 0),
    (&["PFCOUNT", "p", "p"], 2, 0),
    (&["PFCOUNT", "nokey", "p"], 1, 1),
    (&["PFCOUNT", "h", "p"], 1, 0),
    (&["HTTL", "h", "FIELDS", "2", "f", "g"], 1, 0),
    (&["HTTL", "nokey", "FIELDS", "2", "f", "g"], 0, 1),
    (&["HTTL", "s", "FIELDS", "2", "f", "g"], 1, 0),
    (&["HPTTL", "h", "FIELDS", "1", "g"], 1, 0),
    (&["HPTTL", "nokey", "FIELDS", "1", "g"], 0, 1),
    (&["HPTTL", "s", "FIELDS", "1", "g"], 1, 0),
    (&["HEXPIRETIME", "h", "FIELDS", "1", "g"], 1, 0),
    (&["HEXPIRETIME", "nokey", "FIELDS", "1", "g"], 0, 1),
    (&["HEXPIRETIME", "s", "FIELDS", "1", "g"], 1, 0),
    (&["HPEXPIRETIME", "h", "FIELDS", "1", "f"], 1, 0),
    (&["HPEXPIRETIME", "nokey", "FIELDS", "1", "f"], 0, 1),
    (&["HPEXPIRETIME", "s", "FIELDS", "1", "f"], 1, 0),
    (&["XLEN", "x"], 1, 0),
    (&["XLEN", "nokey"], 0, 1),
    (&["XLEN", "s"], 1, 0),
    (&["XRANGE", "x", "-", "+"], 1, 0),
    (&["XRANGE", "nokey", "-", "+"], 0, 1),
    (&["XRANGE", "s", "-", "+"], 1, 0),
    (&["XREVRANGE", "x", "+", "-"], 1, 0),
    (&["XREVRANGE", "nokey", "+", "-"], 0, 1),
    (&["XREVRANGE", "s", "+", "-"], 1, 0),
    (&["XREAD", "STREAMS", "x", "0"], 2, 0),
    (&["XREAD", "STREAMS", "nokey", "0"], 0, 2),
    (&["XREAD", "STREAMS", "s", "0"], 1, 0),
    (&["XREAD", "COUNT", "1", "STREAMS", "x", "0"], 2, 0),
    (&["XREAD", "COUNT", "1", "STREAMS", "nokey", "0"], 0, 2),
    (&["XREAD", "COUNT", "1", "STREAMS", "s", "0"], 1, 0),
    (&["XINFO", "STREAM", "x"], 1, 0),
    (&["XINFO", "STREAM", "nokey"], 0, 1),
    (&["XINFO", "STREAM", "s"], 1, 0),
    (&["XINFO", "GROUPS", "x"], 1, 0),
    (&["XINFO", "GROUPS", "nokey"], 0, 1),
    (&["XINFO", "GROUPS", "s"], 1, 0),
    (&["XINFO", "CONSUMERS", "x", "g"], 1, 0),
    (&["XINFO", "CONSUMERS", "nokey", "g"], 0, 1),
    (&["XINFO", "CONSUMERS", "s", "g"], 1, 0),
    (&["XPENDING", "x", "g"], 1, 0),
    (&["XPENDING", "nokey", "g"], 0, 1),
    (&["XPENDING", "s", "g"], 1, 0),
    (&["XPENDING", "x", "g", "-", "+", "10"], 1, 0),
    (&["XPENDING", "nokey", "g", "-", "+", "10"], 0, 1),
    (&["XPENDING", "s", "g", "-", "+", "10"], 1, 0),
    (&["GEOPOS", "geo", "Palermo"], 1, 0),
    (&["GEOPOS", "nokey", "Palermo"], 0, 1),
    (&["GEOPOS", "s", "Palermo"], 1, 0),
    (&["GEOPOS", "geo", "Palermo", "nomember"], 1, 0),
    (&["GEOPOS", "nokey", "Palermo", "nomember"], 0, 1),
    (&["GEOPOS", "s", "Palermo", "nomember"], 1, 0),
    (&["GEODIST", "geo", "Palermo", "Catania"], 1, 0),
    (&["GEODIST", "nokey", "Palermo", "Catania"], 0, 1),
    (&["GEODIST", "s", "Palermo", "Catania"], 1, 0),
    (&["GEOHASH", "geo", "Palermo"], 1, 0),
    (&["GEOHASH", "nokey", "Palermo"], 0, 1),
    (&["GEOHASH", "s", "Palermo"], 1, 0),
    (&["GEORADIUS_RO", "geo", "15", "37", "200", "km"], 1, 0),
    (&["GEORADIUS_RO", "nokey", "15", "37", "200", "km"], 0, 1),
    (&["GEORADIUS_RO", "s", "15", "37", "200", "km"], 1, 0),
    (&["GEORADIUSBYMEMBER_RO", "geo", "Palermo", "200", "km"], 1, 0),
    (&["GEORADIUSBYMEMBER_RO", "nokey", "Palermo", "200", "km"], 0, 1),
    (&["GEORADIUSBYMEMBER_RO", "s", "Palermo", "200", "km"], 1, 0),
    (&["GEOSEARCH", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"], 1, 0),
    (&["GEOSEARCH", "nokey", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"], 0, 1),
    (&["GEOSEARCH", "s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"], 1, 0),
    (&["GEOSEARCH", "geo", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km"], 1, 0),
    (&["GEOSEARCH", "nokey", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km"], 0, 1),
    (&["GEOSEARCH", "s", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km"], 1, 0),
    (&["OBJECT", "REFCOUNT", "s"], 1, 0),
    (&["OBJECT", "REFCOUNT", "nokey"], 0, 1),
    (&["OBJECT", "REFCOUNT", "h"], 1, 0),
    (&["SORT", "l2"], 1, 0),
    (&["SORT", "nokey"], 0, 1),
    (&["SORT", "s"], 1, 0),
    (&["SORT", "l", "ALPHA"], 1, 0),
    (&["SORT", "nokey", "ALPHA"], 0, 1),
    (&["SORT", "s", "ALPHA"], 1, 0),
    (&["SORT_RO", "l2"], 1, 0),
    (&["SORT_RO", "nokey"], 0, 1),
    (&["SORT_RO", "s"], 1, 0),
    (&["SORT", "l2", "BY", "nosort"], 1, 0),
    (&["SORT", "nokey", "BY", "nosort"], 0, 1),
    (&["SORT", "s", "BY", "nosort"], 1, 0),
    (&["SORT", "l2", "BY", "w_*", "GET", "o_*"], 1, 6),
    (&["SORT", "nokey", "BY", "w_*", "GET", "o_*"], 0, 1),
    (&["SORT", "s", "BY", "w_*", "GET", "o_*"], 1, 0),
    (&["SORT", "l", "ALPHA", "STORE", "sortdst"], 1, 0),
    (&["SORT", "nokey", "ALPHA", "STORE", "sortdst"], 0, 1),
    (&["SORT", "s", "ALPHA", "STORE", "sortdst"], 1, 0),
    (&["JSON.GET", "j"], 1, 0),
    (&["JSON.GET", "nokey"], 0, 1),
    (&["JSON.GET", "s"], 1, 0),
    (&["JSON.GET", "j", "$.a"], 1, 0),
    (&["JSON.GET", "nokey", "$.a"], 0, 1),
    (&["JSON.GET", "s", "$.a"], 1, 0),
    (&["JSON.MGET", "j", "$.a"], 1, 0),
    (&["JSON.MGET", "nokey", "$.a"], 0, 1),
    (&["JSON.MGET", "s", "$.a"], 1, 0),
    (&["JSON.TYPE", "j"], 1, 0),
    (&["JSON.TYPE", "nokey"], 0, 1),
    (&["JSON.TYPE", "s"], 1, 0),
    (&["JSON.STRLEN", "j", "$.str"], 1, 0),
    (&["JSON.STRLEN", "nokey", "$.str"], 0, 1),
    (&["JSON.STRLEN", "s", "$.str"], 1, 0),
    (&["JSON.ARRLEN", "j", "$.arr"], 1, 0),
    (&["JSON.ARRLEN", "nokey", "$.arr"], 0, 1),
    (&["JSON.ARRLEN", "s", "$.arr"], 1, 0),
    (&["JSON.OBJKEYS", "j"], 1, 0),
    (&["JSON.OBJKEYS", "nokey"], 0, 1),
    (&["JSON.OBJKEYS", "s"], 1, 0),
    (&["JSON.OBJLEN", "j"], 1, 0),
    (&["JSON.OBJLEN", "nokey"], 0, 1),
    (&["JSON.OBJLEN", "s"], 1, 0),
    (&["JSON.RESP", "j"], 1, 0),
    (&["JSON.RESP", "nokey"], 0, 1),
    (&["JSON.RESP", "s"], 1, 0),
    (&["JSON.DEBUG", "MEMORY", "j"], 1, 0),
    (&["JSON.DEBUG", "MEMORY", "nokey"], 0, 1),
    (&["JSON.DEBUG", "MEMORY", "s"], 1, 0),
    (&["TS.GET", "ts"], 1, 0),
    (&["TS.GET", "nokey"], 0, 1),
    (&["TS.GET", "s"], 1, 0),
    (&["TS.RANGE", "ts", "-", "+"], 1, 0),
    (&["TS.RANGE", "nokey", "-", "+"], 0, 1),
    (&["TS.RANGE", "s", "-", "+"], 1, 0),
    (&["TS.REVRANGE", "ts", "-", "+"], 1, 0),
    (&["TS.REVRANGE", "nokey", "-", "+"], 0, 1),
    (&["TS.REVRANGE", "s", "-", "+"], 1, 0),
    (&["VCARD", "vs"], 1, 0),
    (&["VCARD", "nokey"], 0, 1),
    (&["VCARD", "s"], 1, 0),
    (&["VDIM", "vs"], 1, 0),
    (&["VDIM", "nokey"], 0, 1),
    (&["VDIM", "s"], 1, 0),
    (&["VEMB", "vs", "e1"], 1, 0),
    (&["VEMB", "nokey", "e1"], 0, 1),
    (&["VEMB", "s", "e1"], 1, 0),
    (&["VSIM", "vs", "ELE", "e1"], 1, 0),
    (&["VSIM", "nokey", "ELE", "e1"], 0, 1),
    (&["VSIM", "s", "ELE", "e1"], 1, 0),
    (&["VINFO", "vs"], 1, 0),
    (&["VINFO", "nokey"], 0, 1),
    (&["VINFO", "s"], 1, 0),
    (&["VISMEMBER", "vs", "e1"], 1, 0),
    (&["VISMEMBER", "nokey", "e1"], 0, 1),
    (&["VISMEMBER", "s", "e1"], 1, 0),
    (&["VRANDMEMBER", "vs"], 1, 0),
    (&["VRANDMEMBER", "nokey"], 0, 1),
    (&["VRANDMEMBER", "s"], 1, 0),
    (&["VLINKS", "vs", "e1"], 1, 0),
    (&["VLINKS", "nokey", "e1"], 0, 1),
    (&["VLINKS", "s", "e1"], 1, 0),
    (&["VGETATTR", "vs", "e1"], 1, 0),
    (&["VGETATTR", "nokey", "e1"], 0, 1),
    (&["VGETATTR", "s", "e1"], 1, 0),
    (&["BF.EXISTS", "bf", "a"], 1, 0),
    (&["BF.EXISTS", "nokey", "a"], 0, 1),
    (&["BF.EXISTS", "s", "a"], 1, 0),
    (&["BF.MEXISTS", "bf", "a", "b"], 1, 0),
    (&["BF.MEXISTS", "nokey", "a", "b"], 0, 1),
    (&["BF.MEXISTS", "s", "a", "b"], 1, 0),
    (&["BF.INFO", "bf"], 1, 0),
    (&["BF.INFO", "nokey"], 0, 1),
    (&["BF.INFO", "s"], 1, 0),
    (&["BF.CARD", "bf"], 1, 0),
    (&["BF.CARD", "nokey"], 0, 1),
    (&["BF.CARD", "s"], 1, 0),
    (&["CF.EXISTS", "cf", "a"], 1, 0),
    (&["CF.EXISTS", "nokey", "a"], 0, 1),
    (&["CF.EXISTS", "s", "a"], 1, 0),
    (&["CF.COUNT", "cf", "a"], 1, 0),
    (&["CF.COUNT", "nokey", "a"], 0, 1),
    (&["CF.COUNT", "s", "a"], 1, 0),
    (&["CF.INFO", "cf"], 1, 0),
    (&["CF.INFO", "nokey"], 0, 1),
    (&["CF.INFO", "s"], 1, 0),
    (&["CMS.QUERY", "cms", "a"], 1, 0),
    (&["CMS.QUERY", "nokey", "a"], 0, 1),
    (&["CMS.QUERY", "s", "a"], 1, 0),
    (&["CMS.INFO", "cms"], 1, 0),
    (&["CMS.INFO", "nokey"], 0, 1),
    (&["CMS.INFO", "s"], 1, 0),
    (&["TOPK.QUERY", "tk", "a"], 1, 0),
    (&["TOPK.QUERY", "nokey", "a"], 0, 1),
    (&["TOPK.QUERY", "s", "a"], 1, 0),
    (&["TOPK.LIST", "tk"], 1, 0),
    (&["TOPK.LIST", "nokey"], 0, 1),
    (&["TOPK.LIST", "s"], 1, 0),
    (&["TOPK.INFO", "tk"], 1, 0),
    (&["TOPK.INFO", "nokey"], 0, 1),
    (&["TOPK.INFO", "s"], 1, 0),
    (&["TDIGEST.QUANTILE", "td", "0.5"], 1, 0),
    (&["TDIGEST.QUANTILE", "nokey", "0.5"], 0, 1),
    (&["TDIGEST.QUANTILE", "s", "0.5"], 1, 0),
    (&["TDIGEST.INFO", "td"], 1, 0),
    (&["TDIGEST.INFO", "nokey"], 0, 1),
    (&["TDIGEST.INFO", "s"], 1, 0),
    (&["TDIGEST.MIN", "td"], 1, 0),
    (&["TDIGEST.MIN", "nokey"], 0, 1),
    (&["TDIGEST.MIN", "s"], 1, 0),
    (&["TDIGEST.MAX", "td"], 1, 0),
    (&["TDIGEST.MAX", "nokey"], 0, 1),
    (&["TDIGEST.MAX", "s"], 1, 0),
    (&["TDIGEST.CDF", "td", "1"], 1, 0),
    (&["TDIGEST.CDF", "nokey", "1"], 0, 1),
    (&["TDIGEST.CDF", "s", "1"], 1, 0),
    (&["TDIGEST.RANK", "td", "1"], 1, 0),
    (&["TDIGEST.RANK", "nokey", "1"], 0, 1),
    (&["TDIGEST.RANK", "s", "1"], 1, 0),
    (&["TDIGEST.REVRANK", "td", "1"], 1, 0),
    (&["TDIGEST.REVRANK", "nokey", "1"], 0, 1),
    (&["TDIGEST.REVRANK", "s", "1"], 1, 0),
    (&["TDIGEST.BYRANK", "td", "0"], 1, 0),
    (&["TDIGEST.BYRANK", "nokey", "0"], 0, 1),
    (&["TDIGEST.BYRANK", "s", "0"], 1, 0),
    (&["TDIGEST.BYREVRANK", "td", "0"], 1, 0),
    (&["TDIGEST.BYREVRANK", "nokey", "0"], 0, 1),
    (&["TDIGEST.BYREVRANK", "s", "0"], 1, 0),
    (&["TDIGEST.TRIMMED_MEAN", "td", "0.1", "0.9"], 1, 0),
    (&["TDIGEST.TRIMMED_MEAN", "nokey", "0.1", "0.9"], 0, 1),
    (&["TDIGEST.TRIMMED_MEAN", "s", "0.1", "0.9"], 1, 0),
    (&["TOPK.COUNT", "tk", "a"], 1, 0),
    (&["TOPK.COUNT", "nokey", "a"], 0, 1),
    (&["TOPK.COUNT", "s", "a"], 1, 0),
    (&["CF.MEXISTS", "cf", "a", "b"], 1, 0),
    (&["CF.MEXISTS", "nokey", "a", "b"], 0, 1),
    (&["CF.MEXISTS", "s", "a", "b"], 1, 0),
    (&["BF.SCANDUMP", "bf", "0"], 1, 0),
    (&["BF.SCANDUMP", "nokey", "0"], 0, 1),
    (&["BF.SCANDUMP", "s", "0"], 1, 0),
    (&["BF.DEBUG", "bf"], 1, 0),
    (&["BF.DEBUG", "nokey"], 0, 1),
    (&["BF.DEBUG", "s"], 1, 0),
    (&["CF.SCANDUMP", "cf", "0"], 1, 0),
    (&["CF.SCANDUMP", "nokey", "0"], 0, 1),
    (&["CF.SCANDUMP", "s", "0"], 1, 0),
    (&["CF.DEBUG", "cf"], 1, 0),
    (&["CF.DEBUG", "nokey"], 0, 1),
    (&["CF.DEBUG", "s"], 1, 0),
    (&["JSON.ARRINDEX", "j", "$.arr", "1"], 1, 0),
    (&["JSON.ARRINDEX", "nokey", "$.arr", "1"], 0, 1),
    (&["JSON.ARRINDEX", "s", "$.arr", "1"], 1, 0),
    (&["VRANGE", "vs", "-", "+", "10"], 1, 0),
    (&["VRANGE", "nokey", "-", "+", "10"], 0, 1),
    (&["VRANGE", "s", "-", "+", "10"], 1, 0),
    (&["BITOP", "AND", "bdst", "b", "b"], 2, 0),
    (&["BITOP", "AND", "bdst", "nokey", "b"], 1, 1),
    (&["BITOP", "AND", "bdst", "h", "b"], 1, 0),
    (&["BITOP", "NOT", "bdst", "b"], 1, 0),
    (&["BITOP", "NOT", "bdst", "nokey"], 0, 1),
    (&["BITOP", "NOT", "bdst", "h"], 1, 0),
    (&["PFMERGE", "pdst", "p", "p"], 2, 1),
    (&["PFMERGE", "pdst", "nokey", "p"], 1, 2),
    (&["PFMERGE", "pdst", "h", "p"], 1, 1),
    (&["PFMERGE", "p", "p"], 2, 0),
    (&["PFMERGE", "nokey", "p"], 1, 1),
    (&["PFMERGE", "h", "p"], 1, 0),
    (&["GEORADIUS", "geo", "15", "37", "200", "km"], 1, 0),
    (&["GEORADIUS", "nokey", "15", "37", "200", "km"], 0, 1),
    (&["GEORADIUS", "s", "15", "37", "200", "km"], 1, 0),
    (&["GEORADIUS", "geo", "15", "37", "200", "km", "STORE", "gdst"], 1, 0),
    (&["GEORADIUS", "nokey", "15", "37", "200", "km", "STORE", "gdst"], 0, 1),
    (&["GEORADIUS", "s", "15", "37", "200", "km", "STORE", "gdst"], 1, 0),
    (&["GEOSEARCHSTORE", "gdst", "geo", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"], 1, 0),
    (&["GEOSEARCHSTORE", "gdst", "nokey", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"], 0, 1),
    (&["GEOSEARCHSTORE", "gdst", "s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km"], 1, 0),
    (&["GEORADIUSBYMEMBER", "geo", "Palermo", "200", "km"], 1, 0),
    (&["GEORADIUSBYMEMBER", "nokey", "Palermo", "200", "km"], 0, 1),
    (&["GEORADIUSBYMEMBER", "s", "Palermo", "200", "km"], 1, 0),
    (&["XREADGROUP", "GROUP", "g", "c2", "STREAMS", "x", ">"], 2, 0),
    (&["XREADGROUP", "GROUP", "g", "c2", "STREAMS", "nokey", ">"], 0, 1),
    (&["XREADGROUP", "GROUP", "g", "c2", "STREAMS", "s", ">"], 1, 0),
    (&["XREADGROUP", "GROUP", "g", "c2", "STREAMS", "x", "0"], 2, 0),
    (&["XREADGROUP", "GROUP", "g", "c2", "STREAMS", "nokey", "0"], 0, 1),
    (&["XREADGROUP", "GROUP", "g", "c2", "STREAMS", "s", "0"], 1, 0),
    (&["SORT", "l2", "BY", "nosort", "GET", "#"], 1, 0),
    (&["SORT", "nokey", "BY", "nosort", "GET", "#"], 0, 1),
    (&["SORT", "s", "BY", "nosort", "GET", "#"], 1, 0),
    (&["SORT", "l2", "BY", "h->f"], 1, 0),
    (&["SORT", "nokey", "BY", "h->f"], 0, 1),
    (&["SORT", "s", "BY", "h->f"], 1, 0),
    (&["SORT", "l2", "GET", "h_*->f"], 1, 3),
    (&["SORT", "nokey", "GET", "h_*->f"], 0, 1),
    (&["SORT", "s", "GET", "h_*->f"], 1, 0),
    (&["SORT_RO", "l2", "BY", "w_*"], 1, 3),
    (&["SORT_RO", "nokey", "BY", "w_*"], 0, 1),
    (&["SORT_RO", "s", "BY", "w_*"], 1, 0),
    (&["DIGEST", "s"], 1, 0),
    (&["DIGEST", "nokey"], 0, 1),
    (&["DIGEST", "h"], 1, 0),
    (&["XAUTOCLAIM", "x", "g", "c1", "0", "0"], 1, 0),
    (&["XAUTOCLAIM", "nokey", "g", "c1", "0", "0"], 0, 1),
    (&["XAUTOCLAIM", "s", "g", "c1", "0", "0"], 1, 0),
    (&["XCLAIM", "x", "g", "c1", "0", "1-1"], 1, 0),
    (&["XCLAIM", "nokey", "g", "c1", "0", "1-1"], 0, 1),
    (&["XCLAIM", "s", "g", "c1", "0", "1-1"], 1, 0),
    (&["XACK", "x", "g", "1-1"], 1, 0),
    (&["XACK", "nokey", "g", "1-1"], 0, 1),
    (&["XACK", "s", "g", "1-1"], 1, 0),
];

/// Runs each of `rows` on fresh [`FIXTURES`], and says which differ.
fn check_fresh(c: &mut Conn, rows: &[(&[&str], u64, u64)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for (args, want_hits, want_misses) in rows {
        c.cmd(&["FLUSHALL"]);
        for setup in FIXTURES {
            c.cmd(setup);
        }
        assert_eq!(c.cmd(&["CONFIG", "RESETSTAT"]), "+OK");
        c.cmd(args);
        let (h, m) = (c.stat("keyspace_hits"), c.stat("keyspace_misses"));
        if (h, m) != (*want_hits, *want_misses) {
            wrong.push(format!(
                "{} (fresh keys): {h} hits and {m} misses, Redis 8.12 {want_hits} and {want_misses}",
                args.join(" ")
            ));
        }
    }
    wrong
}

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

    // The other read commands, each on fresh keys of every type.
    wrong.extend(check_fresh(&mut c, FRESH));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
