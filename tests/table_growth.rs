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
    }
    let took = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let (worst, pings) = prober.join().unwrap();
    assert_eq!(load.cmd(&["DBSIZE"]), format!(":{keys}"));
    for key in [&names[0], &names[keys / 2], &names[keys - 1]] {
        assert_eq!(load.cmd(&["GET", key]), "0123456789abcdef");
    }
    eprintln!(
        "{keys} keys in one of {shards} shards in {took:?}; longest pipeline \
         {longest_pipeline:?}; {pings} PINGs, the longest {worst:?}"
    );
    assert!(pings > 100, "{pings} PINGs");
    assert!(
        worst < Duration::from_millis(LIMIT_MS),
        "a PING on another connection waited {worst:?} while {keys} keys were written"
    );
}

/// The longest a `PING` may wait. In a debug build on a 4-core VM: 382 ms
/// before (the last growth, of 917,504 keys, in one step), 20 ms now, which
/// is how long a pipeline of 1,000 writes takes.
const LIMIT_MS: u64 = 150;
