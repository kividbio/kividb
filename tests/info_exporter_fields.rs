#![allow(clippy::disallowed_methods)]
// tests/info_exporter_fields.rs
//
// The `INFO` fields a dashboard reads through redis_exporter (see
// benchmarks/redis-exporter-compat): client buffers, ACL denials,
// replication traffic and buffer memory, and the background expiry. Each
// test makes the event happen in real `kividb` processes and reads the
// field. The save and rewrite fields are in tests/checkpoint.rs, which can
// hold a checkpoint at a step.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::start_verified;

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        for _ in 0..200 {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                return Conn {
                    reader: BufReader::new(stream.try_clone().unwrap()),
                    writer: stream,
                };
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("could not connect to port {port}");
    }

    fn send(&mut self, args: &[&str]) {
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.writer.write_all(buf.as_bytes()).unwrap();
    }

    /// A reply: a simple string, an error or an integer as its line, a
    /// bulk string as its text.
    fn read(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        match line.strip_prefix('$') {
            Some("-1") => String::new(),
            Some(len) => {
                let mut text = vec![0u8; len.parse::<usize>().unwrap() + 2];
                self.reader.read_exact(&mut text).unwrap();
                text.truncate(text.len() - 2);
                String::from_utf8(text).unwrap()
            }
            None => line,
        }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.read()
    }

    /// `field` of `INFO section`, as text.
    fn info(&mut self, section: &str, field: &str) -> String {
        let info = self.cmd(&["INFO", section]);
        info.lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .trim()
            .to_string()
    }

    fn info_u64(&mut self, section: &str, field: &str) -> u64 {
        let text = self.info(section, field);
        text.parse()
            .unwrap_or_else(|_| panic!("{field} is not a number: {text}"))
    }
}

struct Server {
    port: u16,
    child: Child,
    dir: PathBuf,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let (child, port) = start_verified(|port| {
            let log = std::fs::File::create(dir.join("server.log")).unwrap();
            Command::new(env!("CARGO_BIN_EXE_kividb"))
                .current_dir(&dir)
                .args([
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--threads",
                    "2",
                    "--configfile",
                    "kividb.conf",
                ])
                .args(extra)
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .expect("could not start kividb")
        });
        Server { port, child, dir }
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

fn wait_until(what: &str, seconds: u64, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Every field of the request is there, with the value of a server that has
/// not been asked for anything yet.
#[test]
fn a_new_server_reports_each_field_at_its_start_value() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    for (section, field, value) in [
        ("clients", "client_recent_max_input_buffer", None),
        ("clients", "client_recent_max_output_buffer", None),
        (
            "stats",
            "client_query_buffer_limit_disconnections",
            Some("0"),
        ),
        ("stats", "evicted_clients", Some("0")),
        ("stats", "acl_access_denied_auth", Some("0")),
        ("stats", "acl_access_denied_cmd", Some("0")),
        ("stats", "acl_access_denied_key", Some("0")),
        ("stats", "acl_access_denied_channel", Some("0")),
        ("stats", "latest_fork_usec", Some("0")),
        ("stats", "total_net_repl_input_bytes", Some("0")),
        ("stats", "total_net_repl_output_bytes", Some("0")),
        ("stats", "expired_time_cap_reached_count", None),
        ("stats", "expire_cycle_cpu_milliseconds", None),
        ("stats", "expired_stale_perc", Some("0.00")),
        ("memory", "mem_total_replication_buffers", Some("0")),
        ("persistence", "rdb_current_bgsave_time_sec", Some("-1")),
        ("persistence", "aof_current_rewrite_time_sec", Some("-1")),
    ] {
        let got = c.info(section, field);
        if let Some(value) = value {
            assert_eq!(got, value, "{field}");
        }
    }
}

/// An `AUTH` that fails, a command, a key and a channel the user's rules
/// refuse each count once, in a field of their own, and nothing else
/// counts them.
#[test]
fn each_kind_of_acl_denial_is_counted_in_its_own_field() {
    let server = Server::start(&[]);
    let mut admin = server.conn();
    assert_eq!(
        admin.cmd(&[
            "ACL",
            "SETUSER",
            "bob",
            "on",
            ">pw",
            "~allowed:*",
            "&only",
            "+get",
            "+subscribe",
        ]),
        "+OK"
    );
    let count = |c: &mut Conn| {
        [
            "acl_access_denied_auth",
            "acl_access_denied_cmd",
            "acl_access_denied_key",
            "acl_access_denied_channel",
        ]
        .map(|field| c.info_u64("stats", field))
    };
    assert_eq!(count(&mut admin), [0, 0, 0, 0]);

    let mut bob = server.conn();
    assert!(bob.cmd(&["AUTH", "bob", "wrong"]).starts_with("-WRONGPASS"));
    assert_eq!(count(&mut admin), [1, 0, 0, 0]);

    assert_eq!(bob.cmd(&["AUTH", "bob", "pw"]), "+OK");
    assert!(bob.cmd(&["SET", "allowed:1", "v"]).starts_with("-NOPERM"));
    assert_eq!(count(&mut admin), [1, 1, 0, 0]);

    assert!(bob.cmd(&["GET", "other"]).starts_with("-NOPERM"));
    assert_eq!(count(&mut admin), [1, 1, 1, 0]);

    assert!(bob.cmd(&["SUBSCRIBE", "nope"]).starts_with("-NOPERM"));
    assert_eq!(count(&mut admin), [1, 1, 1, 1]);

    // What the user may do is not counted.
    assert_eq!(bob.cmd(&["GET", "allowed:1"]), "");
    assert_eq!(count(&mut admin), [1, 1, 1, 1]);

    // And `CONFIG RESETSTAT` clears them, as it clears the other counters.
    assert_eq!(admin.cmd(&["CONFIG", "RESETSTAT"]), "+OK");
    assert_eq!(count(&mut admin), [0, 0, 0, 0]);
}

/// A client that has sent part of a large request holds a query buffer, and
/// one that does not read what it subscribed to holds output; the fields
/// show the largest of each among the clients connected.
#[test]
fn the_largest_client_buffers_are_reported() {
    let server = Server::start(&[]);
    let mut c = server.conn();

    // A request begun and not finished: 1 MiB of a 2 MiB value.
    let mut slow = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    slow.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2097152\r\n")
        .unwrap();
    slow.write_all(&vec![b'x'; 1 << 20]).unwrap();
    wait_until("the query buffer to show", 10, || {
        c.info_u64("clients", "client_recent_max_input_buffer") >= 1 << 20
    });
    // Finished and run, it is gone from the maximum.
    slow.write_all(&vec![b'x'; 1 << 20]).unwrap();
    slow.write_all(b"\r\n").unwrap();
    wait_until("the query buffer to empty", 10, || {
        c.info_u64("clients", "client_recent_max_input_buffer") < 1 << 20
    });
    assert_eq!(c.cmd(&["GET", "k"]).len(), 1 << 21);

    // A subscriber that reads nothing while messages are published to it.
    let mut sub = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
        .unwrap();
    wait_until("the subscription", 10, || {
        c.info_u64("clients", "connected_clients") >= 3
    });
    std::thread::sleep(Duration::from_millis(200));
    let message = "m".repeat(1 << 16);
    let seen = Instant::now();
    let mut max_output = 0;
    while max_output == 0 && seen.elapsed() < Duration::from_secs(20) {
        for _ in 0..64 {
            c.cmd(&["PUBLISH", "c", &message]);
        }
        max_output = c.info_u64("clients", "client_recent_max_output_buffer");
    }
    drop(sub);
    assert!(max_output > 0, "no client output was ever reported");
}

/// A client waiting in a blocking command that goes on sending past
/// `client-query-buffer-limit` is closed, and counted (on the tokio loop,
/// which reads such a client's input as it comes).
#[test]
fn a_client_closed_for_its_query_buffer_is_counted() {
    let server = Server::start(&["--io-model", "tokio"]);
    let mut c = server.conn();
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "client-query-buffer-limit", "1mb"]),
        "+OK"
    );
    let mut waiting = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    waiting
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nnokeys\r\n$1\r\n0\r\n")
        .unwrap();
    waiting
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    wait_until("the client to block", 10, || {
        c.info_u64("clients", "blocked_clients") == 1
    });
    // Ignore the errors of a write to a connection the server has closed.
    for _ in 0..32 {
        if waiting.write_all(&vec![b'p'; 1 << 18]).is_err() {
            break;
        }
    }
    wait_until("the client to be counted", 10, || {
        c.info_u64("stats", "client_query_buffer_limit_disconnections") == 1
    });
    assert_eq!(c.info_u64("stats", "evicted_clients"), 0);
}

/// A replica attached to a master: the master counts what it sent over the
/// link, the replica what it received, and the master's backlog is in
/// `mem_total_replication_buffers`.
#[test]
fn replication_traffic_and_buffers_are_reported() {
    let models: &[&str] = if cfg!(target_os = "linux") {
        &["tokio", "uring"]
    } else {
        &["tokio"]
    };
    for &io in models {
        let master = Server::start(&["--io-model", io]);
        let replica = Server::start(&["--io-model", io]);
        let mut m = master.conn();
        let mut r = replica.conn();
        for i in 0..200 {
            m.cmd(&["SET", &format!("before:{i}"), "value"]);
        }
        assert_eq!(
            r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
            "+OK"
        );
        wait_until("the link", 30, || {
            r.info("replication", "master_link_status") == "up"
        });
        let sent_before = m.info_u64("stats", "total_net_repl_output_bytes");
        let received_before = r.info_u64("stats", "total_net_repl_input_bytes");
        // The full sync's payload is in both.
        assert!(sent_before > 0, "nothing sent at the full sync");
        assert!(received_before > 0, "nothing received at the full sync");

        let value = "v".repeat(1000);
        for i in 0..100 {
            m.cmd(&["SET", &format!("after:{i}"), &value]);
        }
        wait_until("the replica to have the writes", 30, || {
            r.cmd(&["GET", "after:99"]) == value
        });
        let sent = m.info_u64("stats", "total_net_repl_output_bytes") - sent_before;
        let received = r.info_u64("stats", "total_net_repl_input_bytes") - received_before;
        assert!(sent >= 100 * 1000, "the master sent {sent} bytes");
        assert!(
            received >= 100 * 1000,
            "the replica received {received} bytes"
        );
        // What the replica received is what the master sent, give or take what
        // is in flight.
        assert!(received <= sent, "received {received}, sent {sent}");

        // The master's backlog holds the stream (it reports its size as
        // the hub notes it, a moment after).
        wait_until("the backlog to show the stream", 10, || {
            m.info_u64("memory", "mem_total_replication_buffers") >= 100 * 1000
        });
        wait_until("the backlog fields to agree", 10, || {
            m.info_u64("memory", "mem_total_replication_buffers")
                == m.info_u64("memory", "mem_replication_backlog")
        });
        // A master receives nothing over a replication link, a replica sends
        // no stream.
        assert_eq!(m.info_u64("stats", "total_net_repl_input_bytes"), 0);
        assert_eq!(r.info_u64("stats", "total_net_repl_output_bytes"), 0);
    }
}

/// Keys that expire are found past their time by the background cycle, and
/// the cycle's time and its share of stale keys are reported.
#[test]
fn the_background_expiry_is_reported() {
    let server = Server::start(&[]);
    let mut c = server.conn();
    // Keys with a time to live of 1 ms, and others that stay: the cycle
    // finds a share of what it looks at past its time.
    for batch in 0..50 {
        let mut args: Vec<String> = vec!["MSET".into()];
        for i in 0..400 {
            args.push(format!("gone:{batch}:{i}"));
            args.push("v".into());
        }
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert_eq!(c.cmd(&args), "+OK");
        for i in 0..400 {
            c.send(&["PEXPIRE", &format!("gone:{batch}:{i}"), "1"]);
        }
        for _ in 0..400 {
            c.read();
        }
    }
    wait_until("the keys to be expired", 30, || c.cmd(&["DBSIZE"]) == ":0");
    assert!(c.info_u64("stats", "expired_keys_active") > 0);
    let perc: f64 = c.info("stats", "expired_stale_perc").parse().unwrap();
    assert!(perc > 0.0, "expired_stale_perc is {perc}");
    assert!(perc <= 100.0, "expired_stale_perc is {perc}");
    // The cycles that took them took time, which is counted in whole
    // milliseconds; and `CONFIG RESETSTAT` clears all three.
    let _ = c.info_u64("stats", "expire_cycle_cpu_milliseconds");
    let _ = c.info_u64("stats", "expired_time_cap_reached_count");
    assert_eq!(c.cmd(&["CONFIG", "RESETSTAT"]), "+OK");
    assert_eq!(c.info_u64("stats", "expired_time_cap_reached_count"), 0);
}
