// tests/concurrent_writes.rs
//
// Writes to one key from many clients at once, with the AOF off and no
// replica: the result must be one that running the same commands one after
// another could have given (#271).
//
// Every write holds the order of its keys (`write_order`) from before it
// reads them until after it has written them. That used to be taken only
// while something observed the write stream (a replica, the AOF), and a
// command that reads a key and writes it back under separate locks of its
// shard lost updates when nothing did: `PFADD`, `HINCRBY` on a new key,
// `BITFIELD INCRBY`, `SADD` against a `SREM` that removed the set it had
// just made, the `*STORE` commands, `COPY` without `REPLACE`.
//
// A real server process, on the default I/O model (io_uring on Linux), so
// that the clients' commands run on several threads at once.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

impl V {
    fn i(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(60)))
                    .unwrap();
                stream.set_nodelay(true).unwrap();
                let writer = stream.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(stream),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn encode(buf: &mut Vec<u8>, args: &[&str]) {
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = Vec::new();
        Self::encode(&mut buf, args);
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    /// Every command of `batch` sent at once, then every reply read.
    fn pipeline(&mut self, batch: &[Vec<String>]) -> Vec<V> {
        let mut buf = Vec::new();
        for args in batch {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            Self::encode(&mut buf, &args);
        }
        self.writer.write_all(&buf).unwrap();
        (0..batch.len()).map(|_| self.read()).collect()
    }

    fn read(&mut self) -> V {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("reply");
        let line = line.trim_end_matches(['\r', '\n']);
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.to_string()),
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
                V::Str(String::from_utf8_lossy(&data).into_owned())
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

struct Server {
    port: u16,
    child: Child,
    dir: std::path::PathBuf,
}

/// Whether nothing has `port`, asked with a socket that is bound and never
/// listens (see `tests/uring.rs`).
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

/// A free port outside the range the system hands out to outgoing
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

impl Server {
    /// The AOF off (the default) and no replica: nothing observes the writes.
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let port = free_port();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kividb"))
            .current_dir(&dir)
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--configfile",
                "kividb.conf",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let server = Server { port, child, dir };
        assert_eq!(server.conn().cmd(&["PING"]), V::Str("PONG".into()));
        server
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Run `clients` clients at once, each given its number and a barrier shared
/// by all of them, and collect what each returns.
fn at_once<T: Send + 'static>(
    server: &Server,
    clients: usize,
    work: impl Fn(usize, &mut Conn, &Barrier) -> T + Send + Sync + 'static,
) -> Vec<T> {
    let work = Arc::new(work);
    let barrier = Arc::new(Barrier::new(clients));
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let mut conn = server.conn();
            let work = Arc::clone(&work);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                work(c, &mut conn, &barrier)
            })
        })
        .collect();
    threads.into_iter().map(|t| t.join().unwrap()).collect()
}

fn args(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| p.to_string()).collect()
}

/// `count` keys made from `prefix`, all in the shard of `anchor`, as the
/// server lays them out on this machine: a command naming several of them
/// runs beside other commands, not alone.
fn keys_in_shard_of(anchor: &str, prefix: &str, count: usize) -> Vec<String> {
    let shards = kividb::store::num_shards();
    let shard = kividb::store::shard_index(shards, anchor);
    (0..)
        .map(|i| format!("{prefix}{i}"))
        .filter(|k| kividb::store::shard_index(shards, k) == shard)
        .take(count)
        .collect()
}

/// The issue's own case: 100 000 distinct elements added by 50 clients give
/// the registers, and so the count, that one client adding them gives.
#[test]
fn pfadd_from_fifty_clients_keeps_every_element() {
    let server = Server::start();
    const ELEMENTS: usize = 100_000;
    const CLIENTS: usize = 50;

    let mut one = server.conn();
    for chunk in (0..ELEMENTS).collect::<Vec<_>>().chunks(1000) {
        let batch: Vec<Vec<String>> = chunk
            .iter()
            .map(|i| args(&["PFADD", "hll:serial", &format!("member:{i:08}")]))
            .collect();
        one.pipeline(&batch);
    }

    at_once(&server, CLIENTS, |c, conn, _| {
        let mine: Vec<usize> = (c..ELEMENTS).step_by(CLIENTS).collect();
        for chunk in mine.chunks(50) {
            let batch: Vec<Vec<String>> = chunk
                .iter()
                .map(|i| args(&["PFADD", "hll:concurrent", &format!("member:{i:08}")]))
                .collect();
            conn.pipeline(&batch);
        }
    });

    let serial = one.cmd(&["PFCOUNT", "hll:serial"]).i();
    let concurrent = one.cmd(&["PFCOUNT", "hll:concurrent"]).i();
    assert_eq!(concurrent, serial, "PFCOUNT differs from the serial run's");
    let serial = one.cmd(&["PFDEBUG", "GETREG", "hll:serial"]);
    let concurrent = one.cmd(&["PFDEBUG", "GETREG", "hll:concurrent"]);
    let (V::Arr(serial), V::Arr(concurrent)) = (serial, concurrent) else {
        panic!("PFDEBUG GETREG did not return registers");
    };
    let differ = serial
        .iter()
        .zip(&concurrent)
        .filter(|(a, b)| a != b)
        .count();
    assert_eq!(differ, 0, "{differ} registers differ from the serial run's");
}

/// Fifty clients increment the same field of the same new hashes: each ends
/// at fifty. Creating the hash was a separate step from finding it absent,
/// and two increments of a new hash both answered 1.
#[test]
fn hincrby_on_new_hashes_from_fifty_clients_counts_every_increment() {
    let server = Server::start();
    const CLIENTS: usize = 50;
    const KEYS: usize = 4000;
    const BATCH: usize = 100;

    at_once(&server, CLIENTS, |_, conn, barrier| {
        for start in (0..KEYS).step_by(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> = (start..start + BATCH)
                .map(|k| args(&["HINCRBY", &format!("counter:{k}"), "f", "1"]))
                .collect();
            conn.pipeline(&batch);
        }
    });

    let mut one = server.conn();
    let short: Vec<(usize, i64)> = (0..KEYS)
        .map(|k| {
            let n = match one.cmd(&["HGET", &format!("counter:{k}"), "f"]) {
                V::Str(s) => s.parse().unwrap(),
                V::Nil => 0,
                other => panic!("HGET: {other:?}"),
            };
            (k, n)
        })
        .filter(|&(_, n)| n != CLIENTS as i64)
        .collect();
    assert!(
        short.is_empty(),
        "{} of {KEYS} hashes lost increments, e.g. {:?}",
        short.len(),
        &short[..short.len().min(5)]
    );
}

/// `SADD` makes a set while `SREM`s of a member it never had run on the same
/// key: whatever order they run in, the member is there afterwards. The set
/// `SADD` had just made, still empty, used to be taken for one a `SREM` had
/// emptied, and removed, with the member then added to a set no longer in
/// the keyspace.
#[test]
fn sadd_beside_srem_of_another_member_keeps_the_member() {
    let server = Server::start();
    const ADDERS: usize = 10;
    const REMOVERS: usize = 40;
    const KEYS: usize = 20_000;
    const BATCH: usize = 200;

    let replies = at_once(&server, ADDERS + REMOVERS, |c, conn, barrier| {
        let mut added = Vec::new();
        for start in (0..KEYS).step_by(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> = (start..start + BATCH)
                .map(|k| {
                    let key = format!("set:{k}");
                    if c < ADDERS {
                        args(&["SADD", &key, "member"])
                    } else {
                        args(&["SREM", &key, "never"])
                    }
                })
                .collect();
            if c < ADDERS {
                added.extend(conn.pipeline(&batch).iter().map(V::i));
            } else {
                conn.pipeline(&batch);
            }
        }
        added
    });

    let mut one = server.conn();
    let missing = (0..KEYS)
        .filter(|k| {
            one.cmd(&["SISMEMBER", &format!("set:{k}"), "member"])
                .i()
                != 1
        })
        .count();
    assert_eq!(missing, 0, "{missing} of {KEYS} sets lost their member");
    // And exactly one `SADD` of each key added it.
    let added: i64 = replies.iter().flatten().sum();
    assert_eq!(added, KEYS as i64, "SADD replied 1 {added} times for {KEYS} keys");
}

/// `BITFIELD ... INCRBY` from fifty clients on one key adds up.
#[test]
fn bitfield_incrby_from_fifty_clients_adds_up() {
    let server = Server::start();
    const CLIENTS: usize = 50;
    const EACH: usize = 2000;

    at_once(&server, CLIENTS, |_, conn, _| {
        let batch: Vec<Vec<String>> = (0..50)
            .map(|_| args(&["BITFIELD", "bits", "INCRBY", "u32", "0", "1"]))
            .collect();
        for _ in 0..EACH / 50 {
            conn.pipeline(&batch);
        }
    });

    let mut one = server.conn();
    let V::Arr(got) = one.cmd(&["BITFIELD", "bits", "GET", "u32", "0"]) else {
        panic!("BITFIELD GET");
    };
    assert_eq!(got[0].i(), (CLIENTS * EACH) as i64);
}

/// `SUNIONSTORE acc acc other`, both keys in one shard, beside `SADD`s to
/// `acc`: every member added is still there. The union read `acc`, a `SADD`
/// landed, and the union wrote the old `acc` back over it.
#[test]
fn sunionstore_of_one_shard_beside_sadd_loses_no_member() {
    let server = Server::start();
    let keys = keys_in_shard_of("acc", "union:", 2);
    let (acc, other) = (keys[0].clone(), keys[1].clone());
    const ADDERS: usize = 25;
    const STORERS: usize = 25;
    const EACH: usize = 2000;

    let mut one = server.conn();
    one.cmd(&["SADD", &other, "from-other"]);

    at_once(&server, ADDERS + STORERS, move |c, conn, _| {
        for start in (0..EACH).step_by(50) {
            let batch: Vec<Vec<String>> = (start..start + 50)
                .map(|i| {
                    if c < ADDERS {
                        args(&["SADD", &acc, &format!("m:{c}:{i}")])
                    } else {
                        args(&["SUNIONSTORE", &acc, &acc, &other])
                    }
                })
                .collect();
            conn.pipeline(&batch);
        }
    });

    let keys = keys_in_shard_of("acc", "union:", 1);
    let card = one.cmd(&["SCARD", &keys[0]]).i();
    assert_eq!(card, (ADDERS * EACH) as i64 + 1, "members were lost");
}

/// Two `COPY`s without `REPLACE` to the same new key, every key in one
/// shard: exactly one of them copies. Both used to find the destination
/// free, and both answered 1.
#[test]
fn copy_to_one_destination_from_two_clients_copies_once() {
    let server = Server::start();
    const KEYS: usize = 3000;
    const BATCH: usize = 100;
    let keys = keys_in_shard_of("copy", "copy:", KEYS + 2);
    let (a, b) = (keys[0].clone(), keys[1].clone());
    let destinations: Vec<String> = keys[2..].to_vec();

    let mut one = server.conn();
    one.cmd(&["SET", &a, "from-a"]);
    one.cmd(&["SET", &b, "from-b"]);

    let replies = at_once(&server, 2, move |c, conn, barrier| {
        let source = if c == 0 { &a } else { &b };
        let mut replies = Vec::new();
        for chunk in destinations.chunks(BATCH) {
            barrier.wait();
            let batch: Vec<Vec<String>> = chunk
                .iter()
                .map(|d| args(&["COPY", source, d]))
                .collect();
            replies.extend(conn.pipeline(&batch).iter().map(V::i));
        }
        replies
    });

    let both = (0..KEYS)
        .filter(|&k| replies[0][k] + replies[1][k] != 1)
        .count();
    assert_eq!(both, 0, "{both} of {KEYS} destinations were copied to twice, or never");
}
