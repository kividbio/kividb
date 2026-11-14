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

#[test]
fn memory_of_a_deleted_hash_goes_back_to_the_system() {
    let port = 16_960;
    start_server(port);
    let mut c = Conn::connect(port);
    let before = rss();

    // 600k fields, 1,000 to an HSET, pipelined a hundred HSETs at a time.
    let fields = 600_000;
    let per = 1000;
    for batch in 0..fields / per / 100 {
        for n in 0..100 {
            let first = (batch * 100 + n) * per;
            let names: Vec<String> = (first..first + per).map(|i| format!("f{i}")).collect();
            let mut args = vec!["HSET", "bighash"];
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
    assert_eq!(c.cmd(&["HLEN", "bighash"]), fields.to_string());
    let loaded = rss();
    let grew = loaded.saturating_sub(before);
    assert!(
        grew > 40 << 20,
        "RSS grew by only {grew} bytes for the hash"
    );
    let trims = c.info("allocator_trims").unwrap_or(0);

    assert_eq!(c.cmd(&["DEL", "bighash"]), "1");
    // Most of what the hash took is given back, without a MEMORY PURGE, a
    // few seconds after: the thread looks four times a second.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut now = rss();
    while now > before + grew / 3 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(250));
        now = rss();
    }
    assert!(
        now <= before + grew / 3,
        "RSS {before} before the hash, {loaded} with it, still {now} 30 s after the DEL"
    );
    let after = c
        .info("allocator_trims")
        .expect("allocator_trims in INFO memory");
    assert!(after > trims, "no trim counted");
}
