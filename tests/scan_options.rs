// tests/scan_options.rs
//
// How SSCAN, HSCAN, ZSCAN and SCAN read their arguments, in Redis's order:
// the cursor, then the key (a missing key is an empty scan, a key of another
// type is WRONGTYPE), and only then the options, one after the other, the
// first fault being the one reported. Every reply below is what Redis 8.10.2
// and 8.10.3 send (checked on both, issue #820): sidekiq-unique-jobs sizes a
// ZSCAN by ZCARD and sends `ZSCAN key 0 MATCH * COUNT 0` for a set that is
// not there, which must be an empty scan, not a syntax error.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::start_verified;

struct Server {
    port: u16,
    child: Child,
}

impl Server {
    fn start() -> Self {
        let (child, port) = start_verified(|port| {
            let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
            command
                .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
                .args(["--threads", "2"])
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
                command.args(["--io-model", &model]);
            }
            command.spawn().expect("start kividb")
        });
        Server { port, child }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .unwrap();
                let writer = stream.try_clone().unwrap();
                return Conn {
                    reader: BufReader::new(stream),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "server did not come up");
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    /// The reply, as text: a bulk string in quotes, an error as `ERR:` and
    /// its text, an array in brackets. A scan's members are in no promised
    /// order, so arrays inside the reply's second place are sorted.
    fn run(&mut self, args: &[&str]) -> String {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn read(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end().to_string();
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => rest.to_string(),
            "-" => format!("ERR:{rest}"),
            ":" => rest.to_string(),
            "$" => {
                let len: i64 = rest.parse().unwrap();
                if len < 0 {
                    return "nil".to_string();
                }
                let mut data = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(len as usize);
                format!("'{}'", String::from_utf8_lossy(&data))
            }
            "*" => {
                let n: usize = rest.parse().unwrap();
                let items: Vec<String> = (0..n).map(|_| self.read()).collect();
                format!("[{}]", items.join(","))
            }
            other => panic!("unexpected RESP type {other:?}"),
        }
    }
}

/// `['0',['b','a']]` as `['0',['a','b']]`: a scan's members come in no
/// promised order.
fn sorted(reply: String) -> String {
    let Some(inner) = reply
        .strip_prefix("['0',[")
        .and_then(|rest| rest.strip_suffix("]]"))
    else {
        return reply;
    };
    let mut items: Vec<&str> = if inner.is_empty() {
        Vec::new()
    } else {
        inner.split(',').collect()
    };
    items.sort_unstable();
    format!("['0',[{}]]", items.join(","))
}

const WRONGTYPE: &str = "ERR:WRONGTYPE Operation against a key holding the wrong kind of value";
const SYNTAX: &str = "ERR:ERR syntax error";
const NOT_INT: &str = "ERR:ERR value is not an integer or out of range";
const CURSOR: &str = "ERR:ERR invalid cursor";
const NO_NOVALUES: &str = "ERR:ERR NOVALUES option can only be used in HSCAN";
const EMPTY: &str = "['0',[]]";

fn fixture(c: &mut Conn) {
    assert_eq!(c.run(&["SADD", "s", "a", "b", "c"]), "3");
    assert_eq!(c.run(&["ZADD", "z", "1", "a", "2", "b"]), "2");
    assert_eq!(c.run(&["HSET", "h", "a", "1", "b", "2"]), "2");
    assert_eq!(c.run(&["SET", "str", "x"]), "OK");
}

/// Rows of `(command, key, arguments after the cursor, reply)`.
type Row = (
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static str,
);

fn r(
    command: &'static str,
    key: &'static str,
    tail: &'static [&'static str],
    reply: &'static str,
) -> Row {
    (command, key, tail, reply)
}

fn check(rows: &[Row]) {
    let server = Server::start();
    let mut c = Conn::connect(server.port);
    fixture(&mut c);
    let mut wrong = Vec::new();
    for (command, key, tail, expected) in rows {
        let mut args = vec![*command, *key, "0"];
        args.extend_from_slice(tail);
        let got = c.run(&args);
        if got != *expected {
            wrong.push(format!("{args:?}: expected {expected}, got {got}"));
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

/// A missing key is an empty scan, whatever the options say or fail to say.
#[test]
fn a_missing_key_is_an_empty_scan_before_the_options_are_read() {
    let mut rows: Vec<Row> = Vec::new();
    let tails: &[&[&str]] = &[
        &[],
        &["COUNT", "0"],
        &["COUNT", "-1"],
        &["COUNT", "abc"],
        &["COUNT", "1.5"],
        &["COUNT", "1", "COUNT", "0"],
        &["COUNT", "0", "COUNT", "abc"],
        &["COUNT"],
        &["MATCH"],
        &["MATCH", "*", "COUNT", "0"],
        &["COUNT", "0", "MATCH", "*"],
        &["COUNT", "9223372036854775808"],
        &["BOGUS"],
        &["BOGUS", "1"],
        &["NOVALUES"],
        &["NOSCORES"],
        &["NOVALUES", "NOSCORES"],
        &["NOVALUES", "COUNT", "0"],
        &["COUNT", "0", "NOSCORES"],
        &["TYPE", "string"],
    ];
    for command in ["SSCAN", "HSCAN", "ZSCAN"] {
        for &tail in tails {
            rows.push(r(command, "missing", tail, EMPTY));
        }
    }
    check(&rows);
}

/// A key of another type is WRONGTYPE before the options are read too.
#[test]
fn a_key_of_another_type_is_wrongtype_before_the_options_are_read() {
    let mut rows: Vec<Row> = Vec::new();
    let tails: &[&[&str]] = &[
        &[],
        &["COUNT", "0"],
        &["COUNT", "abc"],
        &["COUNT", "0", "COUNT", "abc"],
        &["MATCH"],
        &["BOGUS"],
        &["NOVALUES"],
        &["NOSCORES", "COUNT", "0"],
    ];
    for &tail in tails {
        rows.push(r("SSCAN", "str", tail, WRONGTYPE));
        rows.push(r("SSCAN", "z", tail, WRONGTYPE));
        rows.push(r("HSCAN", "str", tail, WRONGTYPE));
        rows.push(r("HSCAN", "s", tail, WRONGTYPE));
        rows.push(r("ZSCAN", "str", tail, WRONGTYPE));
        rows.push(r("ZSCAN", "s", tail, WRONGTYPE));
    }
    check(&rows);
}

/// The cursor is read before anything else, the key not yet looked at.
#[test]
fn the_cursor_is_read_first() {
    let server = Server::start();
    let mut c = Conn::connect(server.port);
    fixture(&mut c);
    for command in ["SSCAN", "HSCAN", "ZSCAN"] {
        for key in ["missing", "str", "s", "h", "z"] {
            for cursor in ["abc", "-1", "18446744073709551616"] {
                assert_eq!(
                    c.run(&[command, key, cursor, "COUNT", "0"]),
                    CURSOR,
                    "{command} {key} {cursor}"
                );
            }
        }
    }
    for cursor in ["abc", "-1", "18446744073709551616"] {
        assert_eq!(c.run(&["SCAN", cursor, "COUNT", "0"]), CURSOR);
    }
}

/// On a key that is there, Redis reads the options in order and reports the
/// first fault: COUNT below 1 is a syntax error, COUNT that is no integer is
/// not one, and a `COUNT 0` is no better in SSCAN and HSCAN than in ZSCAN
/// and SCAN (the table in #820 had it the other way for SSCAN and HSCAN;
/// Redis 8.10.2 and 8.10.3 both refuse it).
#[test]
fn options_on_an_existing_key_are_read_in_order() {
    let mut rows: Vec<Row> = Vec::new();
    // (tail, the reply of every command that takes the options as they are)
    let faults: &[(&[&str], &str)] = &[
        (&["COUNT", "0"], SYNTAX),
        (&["COUNT", "-1"], SYNTAX),
        (&["COUNT", "-9223372036854775808"], SYNTAX),
        (&["COUNT", "abc"], NOT_INT),
        (&["COUNT", "1.5"], NOT_INT),
        (&["COUNT", ""], NOT_INT),
        (&["COUNT", "+3"], NOT_INT),
        (&["COUNT", " 3"], NOT_INT),
        (&["COUNT", "03"], NOT_INT),
        (&["COUNT", "-0"], NOT_INT),
        (&["COUNT", "9223372036854775808"], NOT_INT),
        (&["COUNT", "1", "COUNT", "0"], SYNTAX),
        (&["COUNT", "0", "COUNT", "1"], SYNTAX),
        (&["COUNT", "0", "COUNT", "abc"], SYNTAX),
        (&["COUNT", "abc", "COUNT", "0"], NOT_INT),
        (&["COUNT"], SYNTAX),
        (&["MATCH"], SYNTAX),
        (&["MATCH", "*", "COUNT", "0"], SYNTAX),
        (&["COUNT", "0", "MATCH", "*"], SYNTAX),
        (&["MATCH", "*", "COUNT", "0", "BOGUS"], SYNTAX),
        (&["BOGUS"], SYNTAX),
        (&["BOGUS", "1"], SYNTAX),
        (&["BOGUS", "COUNT", "0"], SYNTAX),
        (&["TYPE", "string"], SYNTAX),
    ];
    for &(tail, reply) in faults {
        for (command, key) in [("SSCAN", "s"), ("HSCAN", "h"), ("ZSCAN", "z")] {
            rows.push(r(command, key, tail, reply));
        }
    }
    // NOSCORES is a ZSCAN option of KiviDB's own (Redis 8.10 has none), and
    // NOVALUES is HSCAN's: anywhere else NOSCORES is a syntax error and
    // NOVALUES has an error of its own, read in its place in the arguments.
    rows.extend_from_slice(&[
        r("SSCAN", "s", &["NOVALUES"], NO_NOVALUES),
        r("SSCAN", "s", &["NOVALUES", "COUNT", "0"], NO_NOVALUES),
        r("SSCAN", "s", &["COUNT", "0", "NOVALUES"], SYNTAX),
        r("SSCAN", "s", &["NOVALUES", "NOSCORES"], NO_NOVALUES),
        r("SSCAN", "s", &["NOVALUES", "NOVALUES"], NO_NOVALUES),
        r(
            "SSCAN",
            "s",
            &["MATCH", "*", "NOVALUES", "COUNT", "5"],
            NO_NOVALUES,
        ),
        r("SSCAN", "s", &["COUNT", "100", "NOVALUES"], NO_NOVALUES),
        r("SSCAN", "s", &["NOSCORES"], SYNTAX),
        r("SSCAN", "s", &["NOSCORES", "NOVALUES"], SYNTAX),
        r("ZSCAN", "z", &["NOVALUES"], NO_NOVALUES),
        r("ZSCAN", "z", &["NOVALUES", "COUNT", "0"], NO_NOVALUES),
        r("ZSCAN", "z", &["COUNT", "0", "NOVALUES"], SYNTAX),
        r("ZSCAN", "z", &["NOVALUES", "NOVALUES"], NO_NOVALUES),
        r("HSCAN", "h", &["NOSCORES"], SYNTAX),
        r("HSCAN", "h", &["NOVALUES", "COUNT", "0"], SYNTAX),
        r("HSCAN", "h", &["COUNT", "0", "NOVALUES"], SYNTAX),
        r("HSCAN", "h", &["NOSCORES", "NOSCORES"], SYNTAX),
    ]);
    check(&rows);
}

/// Options that are fine still work, and the last of a repeated one wins.
#[test]
fn good_options_still_scan() {
    let server = Server::start();
    let mut c = Conn::connect(server.port);
    fixture(&mut c);
    assert_eq!(
        sorted(c.run(&["SSCAN", "s", "0", "COUNT", "9223372036854775807"])),
        "['0',['a','b','c']]"
    );
    assert_eq!(
        sorted(c.run(&["SSCAN", "s", "0", "MATCH", "a*", "MATCH", "b*"])),
        "['0',['b']]"
    );
    assert_eq!(
        sorted(c.run(&["HSCAN", "h", "0", "NOVALUES", "NOVALUES"])),
        "['0',['a','b']]"
    );
    assert_eq!(
        sorted(c.run(&["HSCAN", "h", "0", "MATCH", "*", "NOVALUES", "COUNT", "5"])),
        "['0',['a','b']]"
    );
    assert_eq!(
        sorted(c.run(&["ZSCAN", "z", "0", "MATCH", "a*"])),
        "['0',['1','a']]"
    );
    assert_eq!(
        sorted(c.run(&["ZSCAN", "z", "0", "NOSCORES"])),
        "['0',['a','b']]"
    );
    // SCAN: the same rules, with TYPE.
    assert_eq!(c.run(&["SCAN", "0", "COUNT", "0"]), SYNTAX);
    assert_eq!(c.run(&["SCAN", "0", "COUNT", "-1"]), SYNTAX);
    assert_eq!(c.run(&["SCAN", "0", "COUNT", "abc"]), NOT_INT);
    assert_eq!(c.run(&["SCAN", "0", "COUNT", "+3"]), NOT_INT);
    assert_eq!(c.run(&["SCAN", "0", "MATCH", "*", "COUNT", "0"]), SYNTAX);
    assert_eq!(c.run(&["SCAN", "0", "NOVALUES"]), NO_NOVALUES);
    assert_eq!(c.run(&["SCAN", "0", "NOSCORES"]), SYNTAX);
    assert_eq!(c.run(&["SCAN", "0", "TYPE"]), SYNTAX);
    assert_eq!(c.run(&["SCAN", "0", "MATCH"]), SYNTAX);
    // The arity checks come before everything.
    assert_eq!(
        c.run(&["ZSCAN", "z"]),
        "ERR:ERR wrong number of arguments for 'zscan' command"
    );
    assert_eq!(
        c.run(&["SSCAN", "missing"]),
        "ERR:ERR wrong number of arguments for 'sscan' command"
    );
}

/// `['17',['a','b']]` as the cursor and the names (`NOSCORES`/`NOVALUES`).
fn page(reply: &str) -> (String, Vec<String>) {
    let rest = reply.strip_prefix("['").expect(reply);
    let (cursor, rest) = rest.split_once("',[").expect(reply);
    let inner = rest.strip_suffix("]]").expect(reply);
    let names = if inner.is_empty() {
        Vec::new()
    } else {
        inner
            .split(',')
            .map(|name| name.trim_matches('\'').to_string())
            .collect()
    };
    (cursor.to_string(), names)
}

/// `kind key` with `names` (and a score or value for each), in one call.
fn add(c: &mut Conn, kind: &str, key: &str, names: impl Iterator<Item = String>) {
    let mut args: Vec<String> = vec![kind.into(), key.into()];
    for name in names {
        match kind {
            "SADD" => args.push(name),
            "HSET" => args.extend([name, "v".into()]),
            _ => args.extend(["1".into(), name]),
        }
    }
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let reply = c.run(&args);
    assert!(!reply.starts_with("ERR"), "{reply}");
}

fn remove(c: &mut Conn, kind: &str, key: &str, names: impl Iterator<Item = String>) {
    let cmd = match kind {
        "SADD" => "SREM",
        "HSET" => "HDEL",
        _ => "ZREM",
    };
    let mut args: Vec<String> = vec![cmd.into(), key.into()];
    args.extend(names);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let reply = c.run(&args);
    assert!(!reply.starts_with("ERR"), "{reply}");
}

/// The SCAN guarantee for SSCAN, HSCAN and ZSCAN: a member there from the
/// first call to the last is returned, while another connection grows the
/// collection twentyfold, shrinks it to the members that stay (a table
/// shrinking, or the collection changing to its compact form), and grows it
/// again, between pages; and no page is longer than COUNT asks for, or the
/// floor of a 1,024th of the collection, plus the members at one position
/// (no collision is likely among these). A page is found keeping only about
/// twice COUNT elements (`helpers::scan_page`): growth and shrinking move
/// the cut it makes.
#[test]
fn what_stays_is_returned_while_the_collection_grows_and_shrinks() {
    let server = Server::start();
    let mut c = Conn::connect(server.port);
    let mut other = Conn::connect(server.port);
    for (kind, scan, flag) in [
        ("SADD", "SSCAN", None),
        ("HSET", "HSCAN", Some("NOVALUES")),
        ("ZADD", "ZSCAN", Some("NOSCORES")),
    ] {
        for (count, start) in [(7usize, 1_000usize), (100, 3_000), (1, 150)] {
            let key = format!("{scan}-{count}");
            let keep = || (0..40).map(|i| format!("keep:{i}"));
            add(&mut other, kind, &key, keep());
            add(
                &mut other,
                kind,
                &key,
                (0..start).map(|i| format!("first:{i}")),
            );
            let mut seen = std::collections::HashSet::new();
            let mut cursor = "0".to_string();
            let mut calls = 0;
            loop {
                let count_arg = count.to_string();
                let mut args = vec![scan, key.as_str(), cursor.as_str(), "COUNT", &count_arg];
                args.extend(flag);
                let (next, names) = page(&c.run(&args));
                // At most 23,040 members here: a floor of 22. (A collection
                // in its compact form comes back whole, cursor 0.) A set's
                // table (#764) has no floor: a page ends at the first group
                // boundary once COUNT is reached, and while the table
                // resizes, a group of the smaller array goes with its groups
                // in the larger (up to four in a shrink), so a page can pass
                // COUNT by that unit's members: 64 at most, chains and all,
                // at any fill these sizes reach.
                let most = if scan == "SSCAN" {
                    count + 64
                } else {
                    count.max(22) + 2
                };
                assert!(
                    next == "0" || names.len() <= most,
                    "{scan} COUNT {count}: a page of {}",
                    names.len()
                );
                seen.extend(names);
                calls += 1;
                match calls {
                    2 => add(
                        &mut other,
                        kind,
                        &key,
                        (0..20_000).map(|i| format!("grown:{i}")),
                    ),
                    4 => {
                        remove(
                            &mut other,
                            kind,
                            &key,
                            (0..start).map(|i| format!("first:{i}")),
                        );
                        remove(
                            &mut other,
                            kind,
                            &key,
                            (0..20_000).map(|i| format!("grown:{i}")),
                        );
                    }
                    6 => add(
                        &mut other,
                        kind,
                        &key,
                        (0..3_000).map(|i| format!("again:{i}")),
                    ),
                    _ => {}
                }
                if next == "0" {
                    break;
                }
                assert!(calls < 100_000, "{scan} COUNT {count}: the scan ends");
                cursor = next;
            }
            for name in keep() {
                assert!(
                    seen.contains(&name),
                    "{scan} COUNT {count}: {name} was there throughout and not returned"
                );
            }
        }
    }
}
