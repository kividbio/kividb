// tests/defrag.rs
//
// Active defrag (#113, src/compaction.rs): after a delete at random leaves
// the slabs of string values half empty, values are moved until the slabs
// hold little more than what is in them, and every value still reads back.
// A binary of its own: the slabs are the process's, and nothing else here
// makes values.
// Run with: cargo test --test defrag

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

// Read in assertions' messages only, some of them.
#[allow(dead_code)]
#[derive(Debug)]
enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<Reply>),
    Nil,
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

    fn cmd(&mut self, args: &[&str]) -> Reply {
        self.send(args);
        self.read()
    }

    fn read(&mut self) -> Reply {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        match line.as_bytes().first() {
            Some(b'+') => Reply::Simple(line[1..].to_string()),
            Some(b'-') => Reply::Error(line[1..].to_string()),
            Some(b':') => Reply::Integer(line[1..].parse().unwrap()),
            Some(b'$') => {
                let len: i64 = line[1..].parse().unwrap();
                if len < 0 {
                    return Reply::Nil;
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                Reply::Bulk(data)
            }
            Some(b'*') => {
                let n: i64 = line[1..].parse().unwrap();
                Reply::Array((0..n.max(0)).map(|_| self.read()).collect())
            }
            _ => panic!("unexpected reply: {line:?}"),
        }
    }

    /// `args` for each of `keys` (the key goes second), pipelined.
    fn each(&mut self, cmd: &str, keys: impl Iterator<Item = String>, rest: &[&str]) {
        let keys: Vec<String> = keys.collect();
        for chunk in keys.chunks(1000) {
            for key in chunk {
                let mut args = vec![cmd, key.as_str()];
                args.extend_from_slice(rest);
                self.send(&args);
            }
            for _ in chunk {
                let reply = self.read();
                assert!(!matches!(reply, Reply::Error(_)), "{reply:?}");
            }
        }
    }

    fn info(&mut self, field: &str) -> u64 {
        let Reply::Bulk(text) = self.cmd(&["INFO", "memory"]) else {
            panic!("INFO memory");
        };
        String::from_utf8(text)
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")).map(str::to_string))
            .unwrap_or_else(|| panic!("no {field} in INFO memory"))
            .trim()
            .parse()
            .unwrap()
    }

    fn ok(&mut self, args: &[&str]) {
        let reply = self.cmd(args);
        assert!(
            matches!(&reply, Reply::Simple(s) if s == "OK"),
            "{args:?}: {reply:?}"
        );
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

/// The value of key `i`: 100 bytes, its number in them.
fn value(i: usize) -> String {
    format!("{i:0>100}")
}

/// Whether key `i` is one of the half deleted: a fixed pattern that looks
/// random, so that every slab loses about half.
fn deleted(i: usize) -> bool {
    (i.wrapping_mul(2654435761) >> 7).is_multiple_of(2)
}

#[test]
fn values_move_off_half_empty_slabs_and_read_back() {
    let port = 16_950;
    start_server(port);
    let mut c = Conn::connect(port);
    // Off while the slabs are made half empty, and small enough to start.
    c.ok(&["CONFIG", "SET", "activedefrag", "no"]);
    c.ok(&["CONFIG", "SET", "active-defrag-ignore-bytes", "1mb"]);
    c.ok(&["CONFIG", "SET", "active-defrag-cycle-max", "50"]);

    let n = 300_000;
    let keys: Vec<usize> = (0..n).collect();
    for chunk in keys.chunks(1000) {
        for &i in chunk {
            let key = format!("k:{i}");
            c.send(&["SET", &key, &value(i)]);
        }
        for _ in chunk {
            c.read();
        }
    }
    let loaded = c.info("value_slab_bytes");
    c.each(
        "DEL",
        (0..n).filter(|&i| deleted(i)).map(|i| format!("k:{i}")),
        &[],
    );
    let kept = (0..n).filter(|&i| !deleted(i)).count();

    // Off: the slabs keep their size.
    thread::sleep(Duration::from_secs(3));
    let half_empty = c.info("value_slab_bytes");
    let live = c.info("value_slab_live_bytes");
    assert!(
        half_empty * 10 >= loaded * 9,
        "slabs given back with active defrag off: {loaded} -> {half_empty}"
    );
    assert!(half_empty > live * 3 / 2, "{half_empty} for {live} in them");

    c.ok(&["CONFIG", "SET", "activedefrag", "yes"]);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let slabs = c.info("value_slab_bytes");
        let live = c.info("value_slab_live_bytes");
        if slabs * 100 <= live * 125 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "slabs {slabs} for {live} in them (was {half_empty}), hits {}",
            c.info("active_defrag_hits")
        );
        thread::sleep(Duration::from_millis(250));
    }
    assert!(c.info("active_defrag_hits") > 0);

    // Every value that stayed reads back, and none that went does.
    for i in (0..n).step_by(7) {
        let key = format!("k:{i}");
        match c.cmd(&["GET", &key]) {
            Reply::Bulk(got) if !deleted(i) => assert_eq!(got, value(i).as_bytes(), "{key}"),
            Reply::Nil if deleted(i) => {}
            other => panic!("{key}: {other:?}"),
        }
    }
    assert!(matches!(c.cmd(&["DBSIZE"]), Reply::Integer(k) if k as usize == kept));
}
