// A shard's table grows without holding up other connections (#752).
//
// hashbrown grew a full table in one step, every key hashed again with the
// shard locked: 18 ms for a shard of 230,000 keys in a release build, and
// the shards, filling evenly, grew at about the same time. On the Tokio
// runtime the worker that ran the write that grew a table was as often as
// not the one waiting on the sockets for everybody (#754), and a `PING` on
// another connection waited for the growth. While 8 million keys were
// written, the longest `PING` was 150 ms. The table now grows in steps, a
// few keys for each write (`ShardMap::make_room`).
//
// The test writes 1.2 million keys, pipelined, all of them to one shard, so
// that its table grows past 900,000 keys whatever the number of cores (a
// server has 8 shards a core), while another connection sends `PING` every
// millisecond, and checks the longest `PING`. On Tokio, where the stall
// showed whichever thread the probe was served on.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
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
            .set_read_timeout(Some(Duration::from_secs(120)))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Self { stream, reader }
    }

    fn send(&mut self, buf: &[u8]) {
        self.stream.write_all(buf).unwrap();
    }

    /// One reply's first line, and the bulk string after it if it is one.
    fn read(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        if let Some(len) = line.strip_prefix('$') {
            let len: i64 = len.trim_end().parse().unwrap();
            if len >= 0 {
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                return String::from_utf8(data).unwrap();
            }
        }
        line.trim_end().to_string()
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(&command(args));
        self.read()
    }
}

fn command(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
    }
    buf
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
    use std::sync::atomic::AtomicU16;
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(41);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

/// A server on the Tokio I/O model: on io_uring a stalled thread holds up
/// only its own connections, which the probe's may or may not be.
fn start_server() -> u16 {
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
    let mut c = Conn::connect(port);
    let info = c.cmd(&["INFO", "server"]);
    let api = info
        .lines()
        .find_map(|line| line.strip_prefix("multiplexing_api:"))
        .expect("multiplexing_api in INFO server");
    assert_ne!(
        api.trim(),
        "io_uring",
        "the server under test is on io_uring"
    );
    port
}

#[test]
fn growing_tables_do_not_hold_up_other_connections() {
    // Keys of the first shard: its table's last growth moves 917,504 keys,
    // which it did in one step before.
    const KEYS: usize = 1_200_000;
    const PIPELINE: usize = 1_000;
    let shards = kividb::store::num_shards();
    let names: Vec<String> = (0..)
        .map(|i| format!("key:{i}"))
        .filter(|key| kividb::store::shard_index(shards, key) == 0)
        .take(KEYS)
        .collect();
    let keys = names.len();

    let port = start_server();
    let mut load = Conn::connect(port);
    let stop = Arc::new(AtomicBool::new(false));
    let prober = {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut probe = Conn::connect(port);
            let mut worst = Duration::ZERO;
            let mut pings = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let sent = Instant::now();
                assert_eq!(probe.cmd(&["PING"]), "+PONG");
                worst = worst.max(sent.elapsed());
                pings += 1;
                thread::sleep(Duration::from_millis(1));
            }
            (worst, pings)
        })
    };
    thread::sleep(Duration::from_millis(200));
    let started = Instant::now();
    let mut longest_pipeline = Duration::ZERO;
    let mut pipelines = Vec::with_capacity(keys / PIPELINE + 1);
    let mut buf = Vec::new();
    for from in (0..keys).step_by(PIPELINE) {
        buf.clear();
        for name in &names[from..keys.min(from + PIPELINE)] {
            buf.extend_from_slice(&command(&["SET", name, "0123456789abcdef"]));
        }
        let sent = Instant::now();
        load.send(&buf);
        for _ in from..keys.min(from + PIPELINE) {
            assert_eq!(load.read(), "+OK");
        }
        longest_pipeline = longest_pipeline.max(sent.elapsed());
        pipelines.push(sent.elapsed());
    }
    let took = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let (worst, pings) = prober.join().unwrap();
    assert_eq!(load.cmd(&["DBSIZE"]), format!(":{keys}"));
    for key in [&names[0], &names[keys / 2], &names[keys - 1]] {
        assert_eq!(load.cmd(&["GET", key]), "0123456789abcdef");
    }
    pipelines.sort();
    let typical = pipelines[pipelines.len() / 2];
    // On a machine slower than the one the limit was measured on, the
    // limit grows with it: a pipeline's typical time says how slow it is.
    let limit = Duration::from_millis(LIMIT_MS).max(typical * PIPELINES_OF_SLACK);
    eprintln!(
        "{keys} keys in one of {shards} shards in {took:?}; pipelines took \
         {typical:?} typically, {longest_pipeline:?} at the longest; {pings} \
         PINGs, the longest {worst:?} (limit {limit:?})"
    );
    assert!(pings > 100, "{pings} PINGs");
    assert!(
        worst < limit,
        "a PING on another connection waited {worst:?} while {keys} keys were \
         written, pipelines taking {typical:?} typically"
    );
}

/// The longest a `PING` may wait, on a machine as fast as the one it was
/// measured on. In a debug build on a 4-core VM: 382 ms before (the last
/// growth, of 917,504 keys, in one step), 20 ms now, which is how long a
/// pipeline of 1,000 writes takes.
const LIMIT_MS: u64 = 150;

/// On a slower machine (a shared CI runner, say), the limit is this many
/// typical pipelines instead. The growth that stalled took 19 of them.
const PIPELINES_OF_SLACK: u32 = 7;

/// `SET`s over keys that are there, and `INCR`s, among the writes that grow
/// a table: every write that begins a growth in steps moves the keys to the
/// old table, the written one too, and a write over a key must find it
/// there. Found only in the new table, the write added the key again: two
/// entries for one key, `DBSIZE` one too many, and either value read back
/// (#780 review).
#[test]
fn writes_over_keys_while_tables_grow_keep_one_entry_each() {
    // Keys of the first shard: its table grows in steps five times (from
    // 3,584 keys, 7,168, ... 57,344).
    const KEYS: usize = 60_000;
    const COUNTERS: usize = 64;
    const PIPELINE: usize = 500;
    let shards = kividb::store::num_shards();
    let mut names = (0..)
        .map(|i| format!("w:{i}"))
        .filter(|key| kividb::store::shard_index(shards, key) == 0);
    let keys: Vec<String> = names.by_ref().take(KEYS).collect();
    let counters: Vec<String> = names.take(COUNTERS).collect();

    let port = start_server();
    let mut c = Conn::connect(port);
    // Commands sent together, each with the reply it must get.
    let mut sent: Vec<(Vec<String>, String)> = Vec::new();
    let flush = |c: &mut Conn, sent: &mut Vec<(Vec<String>, String)>| {
        let mut buf = Vec::new();
        for (args, _) in sent.iter() {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            buf.extend_from_slice(&command(&args));
        }
        c.send(&buf);
        for (args, want) in sent.drain(..) {
            assert_eq!(&c.read(), &want, "{args:?}");
        }
    };
    let cmd = |args: &[&str], want: String| (args.iter().map(|a| a.to_string()).collect(), want);

    let mut last: std::collections::HashMap<&str, String> = Default::default();
    let mut counts = vec![0u64; COUNTERS];
    // Three writes over keys that are there for each new key: most of the
    // writes that fill a table are writes over a key.
    let mut x = 88_172_645_463_325_252u64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x as usize
    };
    for (i, key) in keys.iter().enumerate() {
        let value = format!("new {i}");
        sent.push(cmd(&["SET", key, &value], "+OK".into()));
        last.insert(key, value);
        for round in 0..3 {
            if round == 1 {
                let n = next() % COUNTERS;
                counts[n] += 1;
                sent.push(cmd(&["INCR", &counters[n]], format!(":{}", counts[n])));
            } else {
                let over = keys[next() % (i + 1)].as_str();
                let value = format!("over {i} {round}");
                sent.push(cmd(&["SET", over, &value], "+OK".into()));
                last.insert(over, value);
            }
        }
        if (i + 1) % PIPELINE == 0 {
            flush(&mut c, &mut sent);
        }
    }
    flush(&mut c, &mut sent);
    let written = KEYS + counts.iter().filter(|&&n| n > 0).count();
    assert_eq!(c.cmd(&["DBSIZE"]), format!(":{written}"));
    for (n, (key, value)) in last.iter().enumerate() {
        sent.push(cmd(&["GET", key], value.clone()));
        if n % PIPELINE == 0 {
            flush(&mut c, &mut sent);
        }
    }
    for (key, n) in counters.iter().zip(&counts) {
        if *n > 0 {
            sent.push(cmd(&["GET", key], n.to_string()));
        }
    }
    flush(&mut c, &mut sent);
    // Deleted, each key is gone: no second entry comes back.
    for (n, key) in keys.iter().enumerate() {
        sent.push(cmd(&["DEL", key], ":1".into()));
        if n % PIPELINE == 0 {
            flush(&mut c, &mut sent);
        }
    }
    flush(&mut c, &mut sent);
    for (n, key) in keys.iter().enumerate() {
        sent.push(cmd(&["EXISTS", key], ":0".into()));
        if n % PIPELINE == 0 {
            flush(&mut c, &mut sent);
        }
    }
    flush(&mut c, &mut sent);
    assert_eq!(
        c.cmd(&["DBSIZE"]),
        format!(":{}", written - KEYS),
        "keys left after every one was deleted"
    );
}
