// tests/give_back.rs
//
// Freed memory goes back to the system without a MEMORY PURGE once a lot of
// it was freed (#753): a hash of 3M fields deleted left `used_memory` at
// 1.4 MB and RSS at 188 MiB, the free pages kept in glibc's arenas.
// A binary of its own: RSS is the process's, and nothing else here
// allocates.
// Run with: cargo test --test give_back
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(s) => {
                    let writer = s.try_clone().unwrap();
                    return Self {
                        reader: BufReader::new(s),
                        writer,
                    };
                }
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Err(e) => panic!("connect failed: {e}"),
            }
        }
    }

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len());
        for a in args {
            buf.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
    }

    /// One reply, as text: a simple string, an integer or a bulk string.
    fn read(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') | Some(b':') => line[1..].to_string(),
            Some(b'*') => {
                let n: usize = line[1..].parse().unwrap();
                (0..n).map(|_| self.read()).collect::<Vec<_>>().join("\n")
            }
            Some(b'$') => {
                let len: usize = line[1..].parse().unwrap();
                let mut data = vec![0u8; len + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len);
                String::from_utf8(data).unwrap()
            }
            _ => panic!("unexpected reply: {line:?}"),
        }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.read()
    }

    fn info(&mut self, field: &str) -> Option<u64> {
        self.cmd(&["INFO", "memory"])
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .map(|v| v.trim().parse().unwrap())
    }
}

fn start_server(port: u16) {
    thread::spawn(move || {
        kividb::run_for_test(kividb::config::Config {
            port,
            bind: "127.0.0.1".to_string(),
            threads: 2,
            aof: false,
            loglevel: "error".to_string(),
            io_model: kividb::config::IoModel::Tokio,
            configfile: "./kividb.conf".to_string(),
            ..Default::default()
        });
    });
}

/// This process's resident set, in bytes: the server's, which runs in it.
fn rss() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .unwrap()
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .unwrap();
    kb * 1024
}

/// `HSET key` of `fields` fields, 1,000 to an HSET, pipelined a hundred
/// HSETs at a time.
fn load_hash(c: &mut Conn, key: &str, fields: usize) {
    let per = 1000;
    for batch in 0..fields / per / 100 {
        for n in 0..100 {
            let first = (batch * 100 + n) * per;
            let names: Vec<String> = (first..first + per).map(|i| format!("f{i}")).collect();
            let mut args = vec!["HSET", key];
            for name in &names {
                args.push(name);
                args.push("v");
            }
            c.send(&args);
        }
        for _ in 0..100 {
            c.read();
        }
    }
    assert_eq!(c.cmd(&["HLEN", key]), fields.to_string());
}

/// Wait up to 30 s for RSS to fall to `bound`; what it is then.
fn rss_falls_to(bound: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut now = rss();
    while now > bound && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(250));
        now = rss();
    }
    now
}

/// Far more than the 32 MiB fall a trim waits for, so that a hash field
/// that takes less memory one day does not make the test pass vacuously.
const FIELDS: usize = 1_500_000;

#[test]
fn memory_of_a_deleted_hash_goes_back_to_the_system() {
    let port = 16_960;
    start_server(port);
    let mut c = Conn::connect(port);
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "allocator-give-back"]),
        "allocator-give-back\nyes"
    );

    // Idle after the DEL: given back within a few seconds.
    let before = rss();
    load_hash(&mut c, "bighash", FIELDS);
    let loaded = rss();
    let grew = loaded.saturating_sub(before);
    let used = c.info("used_memory").unwrap();
    let trims = c.info("allocator_trims").unwrap_or(0);
    assert_eq!(c.cmd(&["DEL", "bighash"]), "1");
    let fell = used.saturating_sub(c.info("used_memory").unwrap());
    assert!(fell >= 64 << 20, "used_memory fell by only {fell} bytes");
    assert!(
        grew >= 64 << 20,
        "RSS grew by only {grew} bytes for the hash"
    );
    let now = rss_falls_to(before + grew / 3);
    assert!(
        now <= before + grew / 3,
        "RSS {before} before the hash, {loaded} with it, still {now} 30 s after the DEL"
    );
    let after = c
        .info("allocator_trims")
        .expect("allocator_trims in INFO memory");
    assert!(after > trims, "no trim counted");

    // Busy: no trim while commands keep coming, and one once they stop.
    let before = rss();
    load_hash(&mut c, "bighash", FIELDS);
    let loaded = rss();
    let grew = loaded.saturating_sub(before);
    let stop = Arc::new(AtomicBool::new(false));
    let load = {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut l = Conn::connect(port);
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..50 {
                    let key = format!("k{}", i % 1000);
                    l.send(&["SET", &key, "value"]);
                    l.send(&["GET", &key]);
                    i += 1;
                }
                for _ in 0..100 {
                    l.read();
                }
            }
        })
    };
    // A second of load, so that the server is seen busy.
    thread::sleep(Duration::from_millis(1500));
    let trims = c.info("allocator_trims").unwrap();
    let skipped = c.info("allocator_trims_skipped_busy").unwrap();
    assert_eq!(c.cmd(&["DEL", "bighash"]), "1");
    thread::sleep(Duration::from_secs(3));
    assert_eq!(
        c.info("allocator_trims").unwrap(),
        trims,
        "trimmed while the server was busy"
    );
    assert!(
        c.info("allocator_trims_skipped_busy").unwrap() > skipped,
        "no trim put off"
    );
    stop.store(true, Ordering::Relaxed);
    load.join().unwrap();
    let now = rss_falls_to(before + grew / 3);
    assert!(
        now <= before + grew / 3,
        "RSS {before} before the hash, {loaded} with it, still {now} 30 s after the load stopped"
    );
    assert!(
        c.info("allocator_trims").unwrap() > trims,
        "no trim counted"
    );

    // The switch.
    assert_eq!(c.cmd(&["CONFIG", "SET", "allocator-give-back", "no"]), "OK");
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "allocator-give-back"]),
        "allocator-give-back\nno"
    );
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "allocator-give-back", "yes"]),
        "OK"
    );
}
