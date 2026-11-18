#![allow(clippy::disallowed_methods)]
// Small sets kept in one block (#297, step 1), beside the arrays of integers
// and the tables: every set command gives the same answer whatever the form,
// across every change of form, and the members come back the same after a
// save, an AOF replay, an AOF rewrite, a full sync and the stream after it,
// DUMP/RESTORE and MIGRATE. Each test runs its own server processes.

mod common;

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn kividb_bin() -> String {
    std::env::var("CARGO_BIN_EXE_kividb").unwrap_or_else(|_| {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        format!("{manifest}/target/debug/kividb")
    })
}

/// The I/O models the server has here: io_uring's loop runs only on Linux.
fn io_models() -> &'static [&'static str] {
    if cfg!(target_os = "linux") {
        &["uring", "tokio"]
    } else {
        &["tokio"]
    }
}

struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
    args: Vec<String>,
}

impl Server {
    fn start(dir: &Path, extra: &[&str]) -> Self {
        let args: Vec<String> = extra.iter().map(|a| a.to_string()).collect();
        let (child, port) = common::start_verified(|port| spawn(dir, port, &args));
        Server {
            child,
            port,
            dir: dir.to_path_buf(),
            args,
        }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    /// `SHUTDOWN` with or without a save, and wait for the process to go.
    fn stop(&mut self, how: &str) {
        let mut conn = self.conn();
        let _ = conn
            .stream
            .write_all(format!("*2\r\n$8\r\nSHUTDOWN\r\n${}\r\n{how}\r\n", how.len()).as_bytes());
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "server did not stop");
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Started again on its directory, with its arguments.
    fn restart(&mut self) {
        let (child, port, _) =
            common::restart_verified(|port| spawn(&self.dir, port, &self.args), self.port);
        self.child = child;
        self.port = port;
    }
}

fn spawn(dir: &Path, port: u16, extra: &[String]) -> Child {
    Command::new(kividb_bin())
        .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
        .args(["--dir", dir.to_str().unwrap(), "--loglevel", "warning"])
        .args(extra)
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to start kividb")
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_dir(tag: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("kividb-packed-sets-{tag}-"))
        .tempdir()
        .unwrap()
}

#[derive(Debug, PartialEq, Clone)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<Resp>),
    Nil,
}

impl Resp {
    fn int(&self) -> i64 {
        match self {
            Resp::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }

    fn bulk(&self) -> Vec<u8> {
        match self {
            Resp::Bulk(b) => b.clone(),
            other => panic!("not a bulk string: {other:?}"),
        }
    }

    fn text(&self) -> String {
        String::from_utf8(self.bulk()).unwrap()
    }

    fn items(&self) -> &[Resp] {
        match self {
            Resp::Array(items) => items,
            other => panic!("not an array: {other:?}"),
        }
    }

    /// Bulk strings, sorted: set replies come in no fixed order.
    fn members(&self) -> Vec<Vec<u8>> {
        let mut all: Vec<Vec<u8>> = self.items().iter().map(Resp::bulk).collect();
        all.sort();
        all
    }

    fn ok(&self) -> bool {
        *self == Resp::Simple("OK".into())
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());
        Conn { stream, reader }
    }

    fn raw(&mut self, args: &[&[u8]]) -> Resp {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&buf).unwrap();
        self.read()
    }

    fn cmd(&mut self, args: &[&str]) -> Resp {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.raw(&args)
    }

    fn read(&mut self) -> Resp {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches("\r\n");
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap()),
            "$" if rest == "-1" => Resp::Nil,
            "*" if rest == "-1" => Resp::Nil,
            "$" => {
                let mut data = vec![0u8; rest.parse::<usize>().unwrap() + 2];
                self.reader.read_exact(&mut data).unwrap();
                data.truncate(data.len() - 2);
                Resp::Bulk(data)
            }
            "*" | "~" => Resp::Array(
                (0..rest.parse::<usize>().unwrap())
                    .map(|_| self.read())
                    .collect(),
            ),
            _ => panic!("unexpected reply {line:?}"),
        }
    }

    fn sadd(&mut self, key: &str, members: &[Vec<u8>]) -> i64 {
        let mut args: Vec<&[u8]> = vec![b"SADD", key.as_bytes()];
        args.extend(members.iter().map(Vec::as_slice));
        self.raw(&args).int()
    }

    fn encoding(&mut self, key: &str) -> String {
        self.cmd(&["OBJECT", "ENCODING", key]).text()
    }

    fn smembers(&mut self, key: &str) -> Vec<Vec<u8>> {
        self.cmd(&["SMEMBERS", key]).members()
    }

    fn info(&mut self, section: &str, field: &str) -> String {
        let text = self.cmd(&["INFO", section]).text();
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .to_string()
    }
}

fn texts(range: impl Iterator<Item = String>) -> Vec<Vec<u8>> {
    range.map(String::into_bytes).collect()
}

/// Sets of every form and every boundary between them, at Redis's limits
/// (`set-max-intset-entries` 512, `set-max-listpack-entries` 128,
/// `set-max-listpack-value` 64): key, members, the form they make.
fn forms() -> Vec<(&'static str, Vec<Vec<u8>>, &'static str)> {
    let words = |n: usize| texts((0..n).map(|i| format!("membr:{i:08}")));
    let ints = |n: i64| texts((0..n).map(|i| (i * 7 - 300).to_string()));
    let mut mixed = ints(10);
    mixed.push(b"x".to_vec());
    let mut longest = words(5);
    longest.push(vec![b'L'; 64]);
    let mut too_long = words(5);
    too_long.push(vec![b'L'; 65]);
    let mut lookalikes = ints(3);
    lookalikes.extend([b"01".to_vec(), b"-0".to_vec(), b"+1".to_vec()]);
    let mut wide = texts([i64::MIN, -1, 0, i64::MAX].iter().map(i64::to_string));
    wide.push(b"".to_vec());
    vec![
        ("ints", ints(10), "intset"),
        ("ints-512", ints(512), "intset"),
        ("ints-513", ints(513), "hashtable"),
        ("ints-and-a-word", mixed, "listpack"),
        ("lookalikes", lookalikes, "listpack"),
        ("wide-and-empty", wide, "listpack"),
        ("words", words(10), "listpack"),
        ("words-128", words(128), "listpack"),
        ("words-129", words(129), "hashtable"),
        ("longest-64", longest, "listpack"),
        ("too-long-65", too_long, "hashtable"),
        ("table", words(300), "hashtable"),
    ]
}

fn sorted(members: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let set: BTreeSet<Vec<u8>> = members.iter().cloned().collect();
    set.into_iter().collect()
}

fn load_forms(c: &mut Conn) -> Vec<(&'static str, Vec<Vec<u8>>, &'static str)> {
    let forms = forms();
    for (key, members, want) in &forms {
        // In pieces, as clients add: the form is what adding makes.
        for chunk in members.chunks(50) {
            c.sadd(key, chunk);
        }
        assert_eq!(c.encoding(key), *want, "{key}");
    }
    forms
}

fn check_forms(c: &mut Conn, forms: &[(&str, Vec<Vec<u8>>, &str)], what: &str) {
    for (key, members, want) in forms {
        assert_eq!(c.smembers(key), sorted(members), "{what}: {key}");
        assert_eq!(
            c.cmd(&["SCARD", key]).int(),
            sorted(members).len() as i64,
            "{what}: {key}"
        );
        assert_eq!(c.encoding(key), *want, "{what}: {key}");
    }
}

/// Every set command against a set of each form, checked against the
/// members it was given.
#[test]
fn every_set_command_in_each_form() {
    for model in io_models() {
        let dir = temp_dir("commands");
        let server = Server::start(dir.path(), &["--io-model", model]);
        let mut c = server.conn();
        let forms = load_forms(&mut c);
        for (key, members, form) in &forms {
            let all = sorted(members);
            let what = format!("{model} {key} ({form})");
            // Lookups.
            for m in all.iter().step_by(7) {
                assert_eq!(c.raw(&[b"SISMEMBER", key.as_bytes(), m]).int(), 1, "{what}");
            }
            for absent in [&b"absent"[..], b"membr:99999999", b"99999", b"-0x"] {
                assert_eq!(
                    c.raw(&[b"SISMEMBER", key.as_bytes(), absent]).int(),
                    0,
                    "{what}"
                );
            }
            let reply = c.raw(&[
                b"SMISMEMBER",
                key.as_bytes(),
                &all[0],
                b"absent",
                &all[all.len() - 1],
            ]);
            assert_eq!(
                reply,
                Resp::Array(vec![Resp::Int(1), Resp::Int(0), Resp::Int(1)]),
                "{what}"
            );
            // Random members: members, distinct where asked.
            let one = c.cmd(&["SRANDMEMBER", key]).bulk();
            assert!(all.contains(&one), "{what}");
            for count in [1usize, 5, all.len(), all.len() + 10] {
                let got = c.cmd(&["SRANDMEMBER", key, &count.to_string()]).members();
                let mut distinct = got.clone();
                distinct.dedup();
                assert_eq!(got.len(), count.min(all.len()), "{what} {count}");
                assert_eq!(distinct, got, "{what} {count}");
                assert!(got.iter().all(|m| all.contains(m)), "{what}");
            }
            let got = c.cmd(&["SRANDMEMBER", key, "-50"]).members();
            assert_eq!(got.len(), 50);
            assert!(got.iter().all(|m| all.contains(m)), "{what}");
            // A whole scan sees each member once.
            let mut seen = Vec::new();
            let mut cursor = "0".to_string();
            loop {
                let reply = c.cmd(&["SSCAN", key, &cursor, "COUNT", "10"]);
                cursor = reply.items()[0].text();
                seen.extend(reply.items()[1].items().iter().map(Resp::bulk));
                if cursor == "0" {
                    break;
                }
            }
            seen.sort();
            assert_eq!(seen, all, "{what}: SSCAN");
            // SORT ALPHA gives every member in order.
            let reply = c.cmd(&["SORT", key, "ALPHA"]);
            let got: Vec<Vec<u8>> = reply.items().iter().map(Resp::bulk).collect();
            assert_eq!(got, all, "{what}: SORT");
            // COPY, RENAME: the same members and form.
            assert_eq!(c.cmd(&["COPY", key, "copy"]).int(), 1);
            assert_eq!(c.smembers("copy"), all, "{what}: COPY");
            assert_eq!(c.encoding("copy"), *form, "{what}: COPY");
            assert!(c.cmd(&["RENAME", "copy", "renamed"]).ok());
            assert_eq!(c.smembers("renamed"), all, "{what}: RENAME");
            assert_eq!(c.encoding("renamed"), *form);
            // The digest of the members, whatever the form.
            assert_eq!(
                c.cmd(&["DEBUG", "DIGEST-VALUE", key]),
                c.cmd(&["DEBUG", "DIGEST-VALUE", "renamed"])
            );
            // MEMORY USAGE: something, and the same for the copy.
            let usage = c.cmd(&["MEMORY", "USAGE", key]).int();
            assert!(usage > 0, "{what}");
            assert!(c.cmd(&["MEMORY", "USAGE", "renamed", "SAMPLES", "0"]).int() > 0);
            // SPOP takes out distinct members until there are none.
            let mut popped = c.cmd(&["SPOP", "renamed", "3"]).members();
            while let Resp::Bulk(m) = c.cmd(&["SPOP", "renamed"]) {
                popped.push(m);
            }
            popped.sort();
            assert_eq!(popped, all, "{what}: SPOP");
            assert_eq!(c.cmd(&["EXISTS", "renamed"]).int(), 0, "{what}: emptied");
            // SREM of all deletes the key; DEL and UNLINK of a copy.
            assert_eq!(c.cmd(&["COPY", key, "gone"]).int(), 1);
            let mut args: Vec<&[u8]> = vec![b"SREM", b"gone"];
            args.extend(all.iter().map(Vec::as_slice));
            assert_eq!(c.raw(&args).int(), all.len() as i64);
            assert_eq!(c.cmd(&["EXISTS", "gone"]).int(), 0);
            for del in ["DEL", "UNLINK"] {
                assert_eq!(c.cmd(&["COPY", key, "gone"]).int(), 1);
                assert_eq!(c.cmd(&[del, "gone"]).int(), 1);
                assert_eq!(c.cmd(&["EXISTS", "gone"]).int(), 0);
            }
        }
        // The algebra over every pair of forms.
        for (a, ma, _) in &forms {
            for (b, mb, _) in &forms {
                let sa: BTreeSet<Vec<u8>> = ma.iter().cloned().collect();
                let sb: BTreeSet<Vec<u8>> = mb.iter().cloned().collect();
                let what = format!("{model} {a} {b}");
                let inter: Vec<Vec<u8>> = sa.intersection(&sb).cloned().collect();
                let union: Vec<Vec<u8>> = sa.union(&sb).cloned().collect();
                let diff: Vec<Vec<u8>> = sa.difference(&sb).cloned().collect();
                assert_eq!(c.cmd(&["SINTER", a, b]).members(), inter, "{what}");
                assert_eq!(c.cmd(&["SUNION", a, b]).members(), union, "{what}");
                assert_eq!(c.cmd(&["SDIFF", a, b]).members(), diff, "{what}");
                assert_eq!(c.cmd(&["SINTERCARD", "2", a, b]).int(), inter.len() as i64);
                assert_eq!(
                    c.cmd(&["SINTERCARD", "2", a, b, "LIMIT", "3"]).int(),
                    inter.len().min(3) as i64
                );
                for (store, want) in [
                    ("SINTERSTORE", &inter),
                    ("SUNIONSTORE", &union),
                    ("SDIFFSTORE", &diff),
                ] {
                    assert_eq!(
                        c.cmd(&[store, "dst", a, b]).int(),
                        want.len() as i64,
                        "{what} {store}"
                    );
                    if want.is_empty() {
                        assert_eq!(c.cmd(&["EXISTS", "dst"]).int(), 0);
                        continue;
                    }
                    assert_eq!(c.smembers("dst"), *want, "{what} {store}");
                    // The form adding them one by one would have made.
                    let one_by_one = format!("{store}-model");
                    c.cmd(&["DEL", &one_by_one]);
                    for m in want.iter() {
                        c.raw(&[b"SADD", one_by_one.as_bytes(), m]);
                    }
                    assert_eq!(c.encoding("dst"), c.encoding(&one_by_one), "{what} {store}");
                }
            }
        }
        // SMOVE between every pair, there and back.
        for (a, ma, _) in &forms {
            for (b, _, _) in &forms {
                if a == b {
                    continue;
                }
                let m = &ma[ma.len() / 2];
                let had = c.raw(&[b"SISMEMBER", b.as_bytes(), m]).int();
                assert_eq!(c.raw(&[b"SMOVE", a.as_bytes(), b.as_bytes(), m]).int(), 1);
                assert_eq!(c.raw(&[b"SISMEMBER", a.as_bytes(), m]).int(), 0);
                assert_eq!(c.raw(&[b"SISMEMBER", b.as_bytes(), m]).int(), 1);
                if had == 0 {
                    assert_eq!(c.raw(&[b"SMOVE", b.as_bytes(), a.as_bytes(), m]).int(), 1);
                } else {
                    assert_eq!(c.raw(&[b"SADD", a.as_bytes(), m]).int(), 1);
                }
            }
        }
        // The same members; a set given a member of another kind has moved
        // on from its form, for good.
        for (key, members, _) in &forms {
            assert_eq!(
                c.smembers(key),
                sorted(members),
                "{model} after SMOVE: {key}"
            );
        }
    }
}

/// A set goes from the array to the block to the table as members come, at
/// each limit, never back; every member is there at every step. A limit
/// changed under a live set takes effect at its next new member.
#[test]
fn every_change_of_form_keeps_every_member() {
    let dir = temp_dir("changes");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let check = |c: &mut Conn, key: &str, want: &BTreeSet<Vec<u8>>, form: &str| {
        assert_eq!(
            c.smembers(key),
            want.iter().cloned().collect::<Vec<_>>(),
            "{key}"
        );
        assert_eq!(c.encoding(key), form, "{key}");
    };
    // Array -> block -> table, a member at a time.
    let mut model = BTreeSet::new();
    for i in 0..100 {
        let m = i.to_string().into_bytes();
        c.raw(&[b"SADD", b"s", &m]);
        model.insert(m);
    }
    check(&mut c, "s", &model, "intset");
    c.cmd(&["SADD", "s", "word"]);
    model.insert(b"word".to_vec());
    check(&mut c, "s", &model, "listpack");
    for i in 0..27 {
        let m = format!("w{i}").into_bytes();
        c.raw(&[b"SADD", b"s", &m]);
        model.insert(m);
    }
    assert_eq!(model.len(), 128);
    check(&mut c, "s", &model, "listpack");
    c.cmd(&["SADD", "s", "one-more"]);
    model.insert(b"one-more".to_vec());
    check(&mut c, "s", &model, "hashtable");
    // Never back down, even emptied to one member.
    let all: Vec<Vec<u8>> = model.iter().cloned().collect();
    for m in &all[1..] {
        assert_eq!(c.raw(&[b"SREM", b"s", m]).int(), 1);
        model.remove(m);
    }
    check(&mut c, "s", &model, "hashtable");

    // Array straight to the table: past 512 integers, or a long member.
    let ints: Vec<Vec<u8>> = (0..513).map(|i: i64| i.to_string().into_bytes()).collect();
    c.sadd("i", &ints[..512]);
    assert_eq!(c.encoding("i"), "intset");
    c.raw(&[b"SADD", b"i", &ints[512]]);
    check(&mut c, "i", &ints.iter().cloned().collect(), "hashtable");
    c.cmd(&["SADD", "j", "1", "2"]);
    let long = "x".repeat(65);
    c.cmd(&["SADD", "j", &long]);
    check(
        &mut c,
        "j",
        &[b"1".to_vec(), b"2".to_vec(), long.clone().into_bytes()].into(),
        "hashtable",
    );
    // A block takes a long member by turning into the table.
    c.cmd(&["SADD", "k", "a", "b"]);
    c.cmd(&["SADD", "k", &long]);
    check(
        &mut c,
        "k",
        &[b"a".to_vec(), b"b".to_vec(), long.into_bytes()].into(),
        "hashtable",
    );

    // The limits: clamped to what a block holds, and CONFIG GET says so.
    for (name, asked, kept) in [
        ("set-max-listpack-entries", "1000", "128"),
        ("set-max-listpack-value", "100", "64"),
        ("set-max-listpack-entries", "4", "4"),
        ("set-max-listpack-value", "3", "3"),
    ] {
        assert!(c.cmd(&["CONFIG", "SET", name, asked]).ok());
        assert_eq!(
            c.cmd(&["CONFIG", "GET", name]).items()[1].text(),
            kept,
            "{name}"
        );
    }
    for bad in ["-1", "x", "+5"] {
        assert!(matches!(
            c.cmd(&["CONFIG", "SET", "set-max-listpack-entries", bad]),
            Resp::Error(_)
        ));
    }
    // Now 4 entries of 3 bytes: a block of 4 takes no 5th, nor a long one.
    c.cmd(&["SADD", "p", "a", "b", "c", "d"]);
    assert_eq!(c.encoding("p"), "listpack");
    c.cmd(&["SADD", "p", "e"]);
    check(
        &mut c,
        "p",
        &["a", "b", "c", "d", "e"]
            .map(|m| m.as_bytes().to_vec())
            .into(),
        "hashtable",
    );
    c.cmd(&["SADD", "q", "abcd"]);
    assert_eq!(c.encoding("q"), "hashtable");
    // A set made before the change stays as it is until its next new member.
    c.cmd(&["CONFIG", "SET", "set-max-listpack-entries", "128"]);
    c.cmd(&["CONFIG", "SET", "set-max-listpack-value", "64"]);
    c.cmd(&["SADD", "r", "a", "b", "c", "d", "e", "f"]);
    assert_eq!(c.encoding("r"), "listpack");
    c.cmd(&["CONFIG", "SET", "set-max-listpack-entries", "4"]);
    assert_eq!(c.encoding("r"), "listpack");
    assert_eq!(c.cmd(&["SADD", "r", "a"]).int(), 0);
    assert_eq!(c.encoding("r"), "listpack");
    assert_eq!(c.cmd(&["SREM", "r", "a"]).int(), 1);
    assert_eq!(c.encoding("r"), "listpack");
    assert_eq!(c.cmd(&["SADD", "r", "g"]).int(), 1);
    check(
        &mut c,
        "r",
        &["b", "c", "d", "e", "f", "g"]
            .map(|m| m.as_bytes().to_vec())
            .into(),
        "hashtable",
    );
    // 0: no blocks at all.
    c.cmd(&["CONFIG", "SET", "set-max-listpack-entries", "0"]);
    c.cmd(&["SADD", "t", "a"]);
    assert_eq!(c.encoding("t"), "hashtable");
    c.cmd(&["CONFIG", "SET", "set-max-listpack-entries", "128"]);
}

/// A block or an array comes back whole from one SSCAN, cursor 0, whatever
/// COUNT says, as Redis returns a listpack or an intset; a table is paged.
/// A set that turns into a table between two calls of a scan of it has
/// nothing missed: the scan of the block had ended.
#[test]
fn sscan_of_each_form() {
    let dir = temp_dir("sscan");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    for (key, members, form) in &forms {
        let reply = c.cmd(&["SSCAN", key, "0", "COUNT", "2"]);
        if *form == "hashtable" {
            assert_ne!(reply.items()[0].text(), "0", "{key}");
        } else {
            assert_eq!(reply.items()[0].text(), "0", "{key}");
            assert_eq!(reply.items()[1].members(), sorted(members), "{key}");
            // MATCH filters the whole set.
            let reply = c.cmd(&["SSCAN", key, "0", "MATCH", "*1*", "COUNT", "1"]);
            let want: Vec<Vec<u8>> = sorted(members)
                .into_iter()
                .filter(|m| m.contains(&b'1'))
                .collect();
            assert_eq!(reply.items()[1].members(), want, "{key}");
        }
    }
    // A table scanned across a page boundary while members are added.
    let words: Vec<Vec<u8>> = (0..300).map(|i| format!("w{i}").into_bytes()).collect();
    c.sadd("big", &words);
    let mut seen = Vec::new();
    let mut cursor = "0".to_string();
    let mut pages = 0;
    loop {
        let reply = c.cmd(&["SSCAN", "big", &cursor, "COUNT", "20"]);
        cursor = reply.items()[0].text();
        seen.extend(reply.items()[1].items().iter().map(Resp::bulk));
        pages += 1;
        if pages == 2 {
            c.cmd(&["SADD", "big", "added-meanwhile"]);
        }
        if cursor == "0" {
            break;
        }
    }
    seen.retain(|m| m != b"added-meanwhile");
    seen.sort();
    assert_eq!(seen, sorted(&words), "each member exactly once");
}

fn persisted_forms_match(c: &mut Conn, forms: &[(&str, Vec<Vec<u8>>, &str)], what: &str) {
    check_forms(c, forms, what);
    for (key, _, _) in forms {
        assert_eq!(
            c.cmd(&["DEBUG", "DIGEST-VALUE", key]),
            c.cmd(&["DEBUG", "DIGEST-VALUE", &format!("{key}-ref")]),
            "{what}: {key}"
        );
    }
}

/// Reference copies, kept as a table, of each form's members: their digest
/// is what each form's must be after any round trip.
fn reference_copies(c: &mut Conn, forms: &[(&str, Vec<Vec<u8>>, &str)]) {
    c.cmd(&["CONFIG", "SET", "set-max-intset-entries", "0"]);
    c.cmd(&["CONFIG", "SET", "set-max-listpack-entries", "0"]);
    for (key, members, _) in forms {
        c.sadd(&format!("{key}-ref"), members);
        assert_eq!(c.encoding(&format!("{key}-ref")), "hashtable");
    }
    c.cmd(&["CONFIG", "SET", "set-max-intset-entries", "512"]);
    c.cmd(&["CONFIG", "SET", "set-max-listpack-entries", "128"]);
}

/// A snapshot, an AOF replayed and an AOF rewritten give back every set of
/// every form with the same members, in the same form.
#[test]
fn save_aof_and_rewrite_round_trips() {
    // Snapshot.
    let dir = temp_dir("save");
    let mut server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    reference_copies(&mut c, &forms);
    assert!(c.cmd(&["SAVE"]).ok());
    server.stop("NOSAVE");
    server.restart();
    persisted_forms_match(&mut server.conn(), &forms, "snapshot");

    // AOF replay, with removals and conversions in the log.
    let dir = temp_dir("aof");
    let mut server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    reference_copies(&mut c, &forms);
    server.stop("NOSAVE");
    server.restart();
    persisted_forms_match(&mut server.conn(), &forms, "aof replay");

    // AOF rewrite, then replay of what it wrote.
    let mut c = server.conn();
    assert!(matches!(c.cmd(&["BGREWRITEAOF"]), Resp::Simple(_)));
    let deadline = Instant::now() + Duration::from_secs(60);
    while c.info("persistence", "aof_rewrite_in_progress") != "0"
        || c.info("persistence", "aof_rewrite_scheduled") != "0"
    {
        assert!(Instant::now() < deadline, "rewrite did not end");
        thread::sleep(Duration::from_millis(50));
    }
    server.stop("NOSAVE");
    server.restart();
    persisted_forms_match(&mut server.conn(), &forms, "aof rewrite");
}

/// DUMP/RESTORE within a server and MIGRATE to another: the members and the
/// form they make there.
#[test]
fn dump_restore_and_migrate() {
    let dir = temp_dir("dump");
    let server = Server::start(dir.path(), &[]);
    let other_dir = temp_dir("dump-to");
    let other = Server::start(other_dir.path(), &[]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    for (key, members, form) in &forms {
        let payload = c.cmd(&["DUMP", key]).bulk();
        let copy = format!("{key}-restored");
        assert!(c.raw(&[b"RESTORE", copy.as_bytes(), b"0", &payload]).ok());
        assert_eq!(c.smembers(&copy), sorted(members), "{key}");
        assert_eq!(c.encoding(&copy), *form, "{key}");
        let reply = c.cmd(&[
            "MIGRATE",
            "127.0.0.1",
            &other.port.to_string(),
            key,
            "0",
            "5000",
            "COPY",
        ]);
        assert!(reply.ok(), "{key}: {reply:?}");
    }
    check_forms(&mut other.conn(), &forms, "migrated");
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

/// A replica's full sync and the stream after it: every form, and every
/// change of form made on the master after the sync.
#[test]
fn replica_full_sync_and_stream() {
    let dir = temp_dir("master");
    let master = Server::start(dir.path(), &[]);
    let mut m = master.conn();
    let mut forms = load_forms(&mut m);
    let rdir = temp_dir("replica");
    let port = master.port.to_string();
    let replica = Server::start(rdir.path(), &["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    wait_for("the link", || {
        r.info("replication", "master_link_status") == "up"
    });
    let synced = |m: &mut Conn, r: &mut Conn| {
        let offset = m.info("replication", "master_repl_offset");
        wait_for("the offset", || {
            r.info("replication", "slave_repl_offset") == offset
        });
    };
    synced(&mut m, &mut r);
    check_forms(&mut r, &forms, "full sync");

    // Changes of form on the master, streamed.
    m.cmd(&["SADD", "ints", "word"]);
    forms[0].1.push(b"word".to_vec());
    forms[0].2 = "listpack";
    let more: Vec<Vec<u8>> = (0..200).map(|i| format!("more:{i}").into_bytes()).collect();
    m.sadd("words", &more);
    forms[6].1.extend(more);
    forms[6].2 = "hashtable";
    m.cmd(&["SREM", "words-128", "membr:00000005"]);
    forms[7].1.retain(|w| w != b"membr:00000005");
    m.cmd(&["SPOP", "ints-512", "10"]);
    m.cmd(&["SINTERSTORE", "stored", "words-128", "words-129"]);
    synced(&mut m, &mut r);
    for (key, _, form) in &forms {
        if *key == "ints-512" {
            assert_eq!(r.smembers(key), m.smembers(key));
            continue;
        }
        assert_eq!(r.smembers(key), m.smembers(key), "{key}");
        assert_eq!(r.encoding(key), *form, "{key}");
    }
    assert_eq!(r.smembers("stored"), m.smembers("stored"));
    assert_eq!(r.encoding("stored"), "listpack");
}

/// Under `allkeys-lru`, small sets in blocks are counted at their size:
/// memory use is brought to the limit and kept there, not far under it.
#[test]
fn eviction_counts_the_blocks() {
    let dir = temp_dir("evict");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .ok()
    );
    let members: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("membr:{i:08}").into_bytes())
        .collect();
    c.sadd("probe", &members);
    assert_eq!(c.encoding("probe"), "listpack");
    let each = c.cmd(&["MEMORY", "USAGE", "probe"]).int();
    c.cmd(&["DEL", "probe"]);

    let room: i64 = 2 << 20;
    let base: i64 = c.info("memory", "used_memory").parse().unwrap();
    let limit = base + room;
    assert!(
        c.cmd(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .ok()
    );
    let keys = 3 * room / each;
    for i in 0..keys {
        c.sadd(&format!("k{i}"), &members);
    }
    let used: i64 = c.info("memory", "used_memory").parse().unwrap();
    let evicted: i64 = c.info("stats", "evicted_keys").parse().unwrap();
    c.cmd(&["CONFIG", "SET", "maxmemory", "0"]);
    assert!(evicted > 0);
    assert!(
        used <= limit + 64 * 1024,
        "used {used} over the limit {limit}"
    );
    let kept = (0..keys)
        .filter(|i| c.cmd(&["EXISTS", &format!("k{i}")]).int() == 1)
        .count() as i64;
    // Not over- or under-shot: what is kept fills the room, give or take.
    let fitted = kept * each;
    assert!(
        fitted > room * 6 / 10 && fitted < room * 14 / 10,
        "{kept} sets of {each} bytes in {room}"
    );
    for i in (0..keys).step_by(97) {
        let key = format!("k{i}");
        if c.cmd(&["EXISTS", &key]).int() == 1 {
            assert_eq!(c.smembers(&key), sorted(&members));
            assert_eq!(c.encoding(&key), "listpack");
        }
    }
}
