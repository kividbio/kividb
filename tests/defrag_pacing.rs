// tests/defrag_pacing.rs
//
// Active defrag's pacing (#240, src/compaction.rs `Pacer`): switched off in
// the middle of a pass, at the slowest cap, the thread stops within a
// quantum of work and its rest, and every value still reads back. A binary
// of its own: the slabs and active defrag's settings are the process's.
// Run with: cargo test --test defrag_pacing

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

#[derive(Debug)]
enum Reply {
    Simple(String),
    // Read in assertions' messages only.
    #[allow(dead_code)]
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
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
            _ => panic!("unexpected reply: {line:?}"),
        }
    }

    /// `cmd key rest...` for each of `keys`, pipelined.
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

    /// Wait until INFO's `field` is `want`, for at most `limit`.
    fn wait_for(&mut self, field: &str, want: u64, limit: Duration) -> Duration {
        let started = Instant::now();
        while self.info(field) != want {
            assert!(
                started.elapsed() < limit,
                "{field} not {want} after {limit:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }
        started.elapsed()
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

fn value(i: usize) -> String {
    format!("{i:0>100}")
}

/// Half the keys, in a pattern that looks random: every slab loses about
/// half.
fn deleted(i: usize) -> bool {
    (i.wrapping_mul(2654435761) >> 7).is_multiple_of(2)
}

#[test]
fn switched_off_mid_pass_it_stops_soon_and_every_value_reads_back() {
    let port = 16_952;
    start_server(port);
    let mut c = Conn::connect(port);
    c.ok(&["CONFIG", "SET", "activedefrag", "no"]);
    c.ok(&["CONFIG", "SET", "active-defrag-ignore-bytes", "1mb"]);
    // The slowest pace: 99 ms of rest owed for every millisecond of work,
    // so that the pass is still running when it is switched off.
    c.ok(&["CONFIG", "SET", "active-defrag-cycle-max", "1"]);

    let n = 300_000;
    for chunk in (0..n).collect::<Vec<_>>().chunks(1000) {
        for &i in chunk {
            c.send(&["SET", &format!("k:{i}"), &value(i)]);
        }
        for _ in chunk {
            c.read();
        }
    }
    c.each(
        "DEL",
        (0..n).filter(|&i| deleted(i)).map(|i| format!("k:{i}")),
        &[],
    );

    c.ok(&["CONFIG", "SET", "activedefrag", "yes"]);
    // The defrag thread looks once a second.
    c.wait_for("active_defrag_running", 1, Duration::from_secs(10));
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        c.info("active_defrag_running"),
        1,
        "the pass ended before it could be switched off: hits {}",
        c.info("active_defrag_hits")
    );
    c.ok(&["CONFIG", "SET", "activedefrag", "no"]);
    // At a cap of 1 a quantum of work owes 99 ms of rest; the thread sees
    // the switch when it wakes, and lets go of the slabs it was emptying.
    let stopped = c.wait_for("active_defrag_running", 0, Duration::from_secs(5));
    let hits = c.info("active_defrag_hits");
    thread::sleep(Duration::from_millis(500));
    assert_eq!(
        c.info("active_defrag_hits"),
        hits,
        "moved values after stopping"
    );
    eprintln!("stopped {stopped:?} after the switch, {hits} values moved");

    // Every value that stayed reads back, and none that went does.
    for i in (0..n).step_by(7) {
        let key = format!("k:{i}");
        match c.cmd(&["GET", &key]) {
            Reply::Bulk(got) if !deleted(i) => assert_eq!(got, value(i).as_bytes(), "{key}"),
            Reply::Nil if deleted(i) => {}
            other => panic!("{key}: {other:?}"),
        }
    }
    let kept = (0..n).filter(|&i| !deleted(i)).count();
    assert!(matches!(c.cmd(&["DBSIZE"]), Reply::Integer(k) if k as usize == kept));
}
