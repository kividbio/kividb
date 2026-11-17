// Small hashes kept in one block: every hash command gives the same answer
// whatever the form, across every change of form, and the fields come back
// the same, with their deadlines, after a save, an AOF replay, an AOF
// rewrite, a full sync and the stream after it, DUMP/RESTORE and MIGRATE.
// Each test runs its own server processes.

mod common;

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
        .prefix(&format!("kividb-packed-hashes-{tag}-"))
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

    fn hset(&mut self, key: &str, pairs: &[(Vec<u8>, Vec<u8>)]) -> i64 {
        let mut args: Vec<&[u8]> = vec![b"HSET", key.as_bytes()];
        for (f, v) in pairs {
            args.push(f);
            args.push(v);
        }
        self.raw(&args).int()
    }

    fn encoding(&mut self, key: &str) -> String {
        self.cmd(&["OBJECT", "ENCODING", key]).text()
    }

    fn hgetall(&mut self, key: &str) -> Pairs {
        self.cmd(&["HGETALL", key])
            .items()
            .chunks(2)
            .map(|p| (p[0].bulk(), p[1].bulk()))
            .collect()
    }

    fn info(&mut self, section: &str, field: &str) -> String {
        let text = self.cmd(&["INFO", section]).text();
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
            .to_string()
    }

    fn digest(&mut self, key: &str) -> Resp {
        self.cmd(&["DEBUG", "DIGEST-VALUE", key])
    }
}

type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// `n` fields of 14 bytes with values of 32, as the memory rows have them.
fn pairs(n: usize, prefix: &str) -> Pairs {
    (0..n)
        .map(|i| {
            (
                format!("{prefix}:{i:08}").into_bytes(),
                format!("value:{i:026}").into_bytes(),
            )
        })
        .collect()
}

fn sorted(pairs: &[(Vec<u8>, Vec<u8>)]) -> Pairs {
    let mut all = pairs.to_vec();
    all.sort();
    all
}

#[derive(Clone)]
struct Form {
    key: &'static str,
    pairs: Pairs,
    form: &'static str,
    /// Two fields have a deadline (far off).
    ttl: bool,
}

impl Form {
    /// Whether Redis gives the fields in the order they were set: a
    /// listpack's.
    fn in_order(&self) -> bool {
        self.form != "hashtable"
    }
}

/// Hashes of every form and every boundary between them, at Redis's limits
/// (`hash-max-listpack-entries` 512, `hash-max-listpack-value` 64).
fn forms() -> Vec<Form> {
    let with = |n: usize, extra: (Vec<u8>, Vec<u8>)| {
        let mut p = pairs(n, "field");
        p.push(extra);
        p
    };
    let form = |key, pairs, form, ttl| Form {
        key,
        pairs,
        form,
        ttl,
    };
    vec![
        form("h10", pairs(10, "field"), "listpack", false),
        form("h512", pairs(512, "field"), "listpack", false),
        form("h513", pairs(513, "field"), "hashtable", false),
        form(
            "field64",
            with(5, (vec![b'F'; 64], b"v".to_vec())),
            "listpack",
            false,
        ),
        form(
            "field65",
            with(5, (vec![b'F'; 65], b"v".to_vec())),
            "hashtable",
            false,
        ),
        form(
            "value64",
            with(5, (b"f".to_vec(), vec![b'V'; 64])),
            "listpack",
            false,
        ),
        form(
            "value65",
            with(5, (b"f".to_vec(), vec![b'V'; 65])),
            "hashtable",
            false,
        ),
        form(
            "empties",
            vec![(b"".to_vec(), b"".to_vec()), (b"e".to_vec(), b"".to_vec())],
            "listpack",
            false,
        ),
        form("table", pairs(1000, "t"), "hashtable", false),
        form("ttl10", pairs(10, "field"), "listpackex", true),
        form("ttl600", pairs(600, "field"), "hashtable", true),
    ]
}

/// A deadline far enough off never to come during a test.
const FAR_MS: &str = "100000000";
/// The same as a moment (in 2096), so that two hashes given it have one
/// digest.
const FAR_AT: &str = "4000000000000";

fn load(c: &mut Conn, key: &str, form: &Form) {
    // In pieces, as clients write: the form is what writing makes.
    for chunk in form.pairs.chunks(50) {
        c.hset(key, chunk);
    }
    if form.ttl {
        let first = &form.pairs[0].0;
        let last = &form.pairs[form.pairs.len() - 1].0;
        let reply = c.raw(&[
            b"HPEXPIREAT",
            key.as_bytes(),
            FAR_AT.as_bytes(),
            b"FIELDS",
            b"2",
            first,
            last,
        ]);
        assert_eq!(
            reply,
            Resp::Array(vec![Resp::Int(1), Resp::Int(1)]),
            "{key}"
        );
    }
}

fn load_forms(c: &mut Conn) -> Vec<Form> {
    let forms = forms();
    for form in &forms {
        load(c, form.key, form);
        assert_eq!(c.encoding(form.key), form.form, "{}", form.key);
    }
    forms
}

fn check_form(c: &mut Conn, key: &str, form: &Form, what: &str) {
    let got = c.hgetall(key);
    if form.in_order() {
        assert_eq!(got, form.pairs, "{what}: {key}");
    } else {
        assert_eq!(sorted(&got), sorted(&form.pairs), "{what}: {key}");
    }
    assert_eq!(
        c.cmd(&["HLEN", key]).int(),
        form.pairs.len() as i64,
        "{what}: {key}"
    );
    assert_eq!(c.encoding(key), form.form, "{what}: {key}");
    if form.ttl {
        let reply = c.raw(&[
            b"HPEXPIRETIME",
            key.as_bytes(),
            b"FIELDS",
            b"1",
            &form.pairs[0].0,
        ]);
        assert!(reply.items()[0].int() > 0, "{what}: {key} lost its TTL");
    }
}

fn check_forms(c: &mut Conn, forms: &[Form], what: &str) {
    for form in forms {
        check_form(c, form.key, form, what);
    }
}

/// Reference copies, kept as a table, of each form: their digest is what
/// each form's must be.
fn reference_copies(c: &mut Conn, forms: &[Form]) {
    assert!(
        c.cmd(&["CONFIG", "SET", "hash-max-listpack-entries", "0"])
            .ok()
    );
    for form in forms {
        let key = format!("{}-ref", form.key);
        load(c, &key, form);
        assert_eq!(c.encoding(&key), "hashtable");
    }
    assert!(
        c.cmd(&["CONFIG", "SET", "hash-max-listpack-entries", "512"])
            .ok()
    );
}

fn digests_match(c: &mut Conn, forms: &[Form], what: &str) {
    for form in forms {
        assert_eq!(
            c.digest(form.key),
            c.digest(&format!("{}-ref", form.key)),
            "{what}: {}",
            form.key
        );
    }
}

/// Every hash command against a hash of each form, checked against the
/// fields it was given, on each I/O model.
#[test]
fn every_hash_command_in_each_form() {
    for model in io_models() {
        let dir = temp_dir("commands");
        let server = Server::start(dir.path(), &["--io-model", model]);
        let mut c = server.conn();
        let forms = load_forms(&mut c);
        reference_copies(&mut c, &forms);
        for form in &forms {
            let key = form.key;
            let k = key.as_bytes();
            let what = format!("{model} {key} ({})", form.form);
            check_form(&mut c, key, form, &what);
            assert_eq!(c.digest(key), c.digest(&format!("{key}-ref")), "{what}");
            // Lookups.
            for (f, v) in form.pairs.iter().step_by(7) {
                assert_eq!(c.raw(&[b"HGET", k, f]).bulk(), *v, "{what}");
                assert_eq!(c.raw(&[b"HEXISTS", k, f]).int(), 1, "{what}");
                assert_eq!(c.raw(&[b"HSTRLEN", k, f]).int(), v.len() as i64);
            }
            for absent in [&b"absent"[..], b"field:99999999", b"FFFF"] {
                assert_eq!(c.raw(&[b"HGET", k, absent]), Resp::Nil, "{what}");
                assert_eq!(c.raw(&[b"HEXISTS", k, absent]).int(), 0, "{what}");
                assert_eq!(c.raw(&[b"HSTRLEN", k, absent]).int(), 0, "{what}");
            }
            let (first, last) = (&form.pairs[0], &form.pairs[form.pairs.len() - 1]);
            assert_eq!(
                c.raw(&[b"HMGET", k, &first.0, b"absent", &last.0]),
                Resp::Array(vec![
                    Resp::Bulk(first.1.clone()),
                    Resp::Nil,
                    Resp::Bulk(last.1.clone())
                ]),
                "{what}"
            );
            // HKEYS and HVALS: in order where Redis keeps it.
            let keys: Vec<Vec<u8>> = c
                .cmd(&["HKEYS", key])
                .items()
                .iter()
                .map(Resp::bulk)
                .collect();
            let vals: Vec<Vec<u8>> = c
                .cmd(&["HVALS", key])
                .items()
                .iter()
                .map(Resp::bulk)
                .collect();
            let want_keys: Vec<Vec<u8>> = form.pairs.iter().map(|(f, _)| f.clone()).collect();
            let want_vals: Vec<Vec<u8>> = form.pairs.iter().map(|(_, v)| v.clone()).collect();
            if form.in_order() {
                assert_eq!(keys, want_keys, "{what}");
                assert_eq!(vals, want_vals, "{what}");
            }
            let mut sorted_keys = keys.clone();
            sorted_keys.sort();
            let mut want_sorted = want_keys.clone();
            want_sorted.sort();
            assert_eq!(sorted_keys, want_sorted, "{what}");
            // Random fields: fields of the hash, distinct where asked.
            let one = c.cmd(&["HRANDFIELD", key]).bulk();
            assert!(want_keys.contains(&one), "{what}");
            let n = want_keys.len();
            for count in [1usize, 5, n, n + 10] {
                let mut got: Vec<Vec<u8>> = c
                    .cmd(&["HRANDFIELD", key, &count.to_string()])
                    .items()
                    .iter()
                    .map(Resp::bulk)
                    .collect();
                if count >= n && form.in_order() {
                    // All of them, in the order HGETALL has.
                    assert_eq!(got, want_keys, "{what} {count}");
                }
                got.sort();
                let mut distinct = got.clone();
                distinct.dedup();
                assert_eq!(got.len(), count.min(n), "{what} {count}");
                assert_eq!(distinct, got, "{what} {count}");
                assert!(got.iter().all(|f| want_keys.contains(f)), "{what}");
            }
            let got = c.cmd(&["HRANDFIELD", key, "-50", "WITHVALUES"]);
            assert_eq!(got.items().len(), 100, "{what}");
            for pair in got.items().chunks(2) {
                let (f, v) = (pair[0].bulk(), pair[1].bulk());
                assert_eq!(c.raw(&[b"HGET", k, &f]).bulk(), v, "{what}");
            }
            // A whole scan sees each field once.
            let mut seen = Vec::new();
            let mut cursor = "0".to_string();
            loop {
                let reply = c.cmd(&["HSCAN", key, &cursor, "COUNT", "10"]);
                cursor = reply.items()[0].text();
                for pair in reply.items()[1].items().chunks(2) {
                    seen.push((pair[0].bulk(), pair[1].bulk()));
                }
                if cursor == "0" {
                    break;
                }
            }
            assert_eq!(sorted(&seen), sorted(&form.pairs), "{what}");
            // A block with field TTLs is scanned whole too, in order.
            if form.form == "listpackex" {
                let reply = c.cmd(&["HSCAN", key, "0", "COUNT", "1"]);
                assert_eq!(reply.items()[0].text(), "0", "{what}");
                let page: Pairs = reply.items()[1]
                    .items()
                    .chunks(2)
                    .map(|p| (p[0].bulk(), p[1].bulk()))
                    .collect();
                assert_eq!(page, form.pairs, "{what}");
            }
            assert!(c.cmd(&["MEMORY", "USAGE", key]).int() > 0);
            // COPY keeps the form; RENAME, DEL and UNLINK take it whole.
            assert_eq!(c.cmd(&["COPY", key, "copy"]).int(), 1);
            check_form(&mut c, "copy", form, &format!("{what} copy"));
            assert_eq!(c.digest("copy"), c.digest(key), "{what}");
            assert!(c.cmd(&["RENAME", "copy", "renamed"]).ok());
            check_form(&mut c, "renamed", form, &format!("{what} renamed"));
            assert_eq!(c.cmd(&["DEL", "renamed"]).int(), 1);
            assert_eq!(c.cmd(&["COPY", key, "copy"]).int(), 1);
            assert_eq!(c.cmd(&["UNLINK", "copy"]).int(), 1);
            assert_eq!(c.cmd(&["EXISTS", "copy"]).int(), 0);

            // Writes, on a copy, against a model kept here.
            assert_eq!(c.cmd(&["COPY", key, "w"]).int(), 1);
            let mut model = form.pairs.clone();
            // The most fields the copy has at once: past 512 it is a table.
            let peak = model.len() + 2;
            let mid = model.len() / 2;
            let longer = b"a longer value than it was before".to_vec();
            assert_eq!(c.raw(&[b"HSET", b"w", &model[mid].0, &longer]).int(), 0);
            model[mid].1 = longer;
            let shorter = b"s".to_vec();
            assert_eq!(c.raw(&[b"HSET", b"w", &model[0].0, &shorter]).int(), 0);
            model[0].1 = shorter;
            // The first field removed: the others keep their order.
            let gone = model.remove(0);
            assert_eq!(c.raw(&[b"HDEL", b"w", &gone.0, b"absent"]).int(), 1);
            assert_eq!(c.raw(&[b"HSETNX", b"w", &model[0].0, b"no"]).int(), 0);
            assert_eq!(c.cmd(&["HSETNX", "w", "nx", "yes"]).int(), 1);
            model.push((b"nx".to_vec(), b"yes".to_vec()));
            assert_eq!(c.cmd(&["HINCRBY", "w", "n", "41"]).int(), 41);
            assert_eq!(c.cmd(&["HINCRBY", "w", "n", "1"]).int(), 42);
            model.push((b"n".to_vec(), b"42".to_vec()));
            assert_eq!(c.cmd(&["HINCRBYFLOAT", "w", "x", "1.5"]).text(), "1.5");
            model.push((b"x".to_vec(), b"1.5".to_vec()));
            let reply = c.raw(&[b"HGETDEL", b"w", b"FIELDS", b"2", &model[1].0, b"absent"]);
            assert_eq!(reply.items()[0].bulk(), model[1].1, "{what}");
            model.remove(1);
            let reply = c.raw(&[
                b"HGETEX",
                b"w",
                b"EX",
                b"1000",
                b"FIELDS",
                b"1",
                &model[1].0,
            ]);
            assert_eq!(reply.items()[0].bulk(), model[1].1, "{what}");
            let reply = c.cmd(&["HSETEX", "w", "FNX", "EX", "1000", "FIELDS", "1", "ex", "1"]);
            assert_eq!(reply.int(), 1, "{what}");
            model.push((b"ex".to_vec(), b"1".to_vec()));
            let got = c.hgetall("w");
            let block = form.form != "hashtable" && peak <= 512;
            if block {
                assert_eq!(got, model, "{what}: writes");
            } else {
                assert_eq!(sorted(&got), sorted(&model), "{what}: writes");
            }
            assert!(c.cmd(&["HTTL", "w", "FIELDS", "1", "ex"]).items()[0].int() > 0);
            // HPERSIST takes a field's TTL away and leaves the form: other
            // fields still have one (HGETEX gave model[1] one).
            let reply = c.cmd(&["HPERSIST", "w", "FIELDS", "2", "ex", "absent"]);
            assert_eq!(
                reply,
                Resp::Array(vec![Resp::Int(1), Resp::Int(-2)]),
                "{what}"
            );
            assert_eq!(
                c.cmd(&["HTTL", "w", "FIELDS", "1", "ex"]).items()[0].int(),
                -1
            );
            assert_eq!(c.cmd(&["HGET", "w", "ex"]).text(), "1", "{what}");
            // A field TTL makes a block a listpackex; a table stays one.
            let want = if block { "listpackex" } else { "hashtable" };
            assert_eq!(c.encoding("w"), want, "{what}");
            assert_eq!(c.cmd(&["DEL", "w"]).int(), 1);
        }
        // SORT by and getting hash fields, from a block and from a table.
        c.cmd(&["RPUSH", "ids", "1", "2", "3"]);
        for i in 1..=3 {
            let key = format!("obj:{i}");
            c.cmd(&[
                "HSET",
                &key,
                "w",
                &(10 - i).to_string(),
                "name",
                &format!("n{i}"),
            ]);
        }
        c.cmd(&["HSET", "obj:3", "pad", &"p".repeat(100)]);
        assert_eq!(c.encoding("obj:1"), "listpack");
        assert_eq!(c.encoding("obj:3"), "hashtable");
        let sorted_names: Vec<String> = c
            .cmd(&["SORT", "ids", "BY", "obj:*->w", "GET", "obj:*->name"])
            .items()
            .iter()
            .map(Resp::text)
            .collect();
        assert_eq!(sorted_names, ["n3", "n2", "n1"]);
    }
}

/// Each change of form keeps every field, in order where Redis keeps it, and
/// never goes back; the settings take effect from a hash's next write that
/// crosses them, clamped to what a block holds.
#[test]
fn every_change_of_form_keeps_every_field() {
    let dir = temp_dir("convert");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    // By count, a field at a time.
    let all = pairs(520, "field");
    for (i, (f, v)) in all.iter().enumerate() {
        c.raw(&[b"HSET", b"grow", f, v]);
        let want = if i < 512 { "listpack" } else { "hashtable" };
        if i % 64 == 0 || (510..515).contains(&i) {
            assert_eq!(c.encoding("grow"), want, "{i}");
        }
    }
    assert_eq!(sorted(&c.hgetall("grow")), sorted(&all));
    // Shrinking never converts back.
    for (f, _) in &all[..515] {
        c.raw(&[b"HDEL", b"grow", f]);
    }
    assert_eq!(c.encoding("grow"), "hashtable");
    // A table past 512 fields fills a removed field's place with its last.
    assert_eq!(sorted(&c.hgetall("grow")), sorted(&all[515..]));
    // By a long field, a long value, and a value set again too long; with a
    // field TTL kept across.
    for (key, field, value) in [
        ("lf", vec![b'L'; 65], b"v".to_vec()),
        ("lv", b"f".to_vec(), vec![b'V'; 65]),
        ("again", b"a".to_vec(), vec![b'A'; 70]),
    ] {
        c.cmd(&["HSET", key, "a", "1", "b", "2"]);
        c.cmd(&["HPEXPIRE", key, FAR_MS, "FIELDS", "1", "b"]);
        assert_eq!(c.encoding(key), "listpackex", "{key}");
        let at = c.cmd(&["HPEXPIRETIME", key, "FIELDS", "1", "b"]).items()[0].int();
        c.raw(&[b"HSET", key.as_bytes(), &field, &value]);
        assert_eq!(c.encoding(key), "hashtable", "{key}");
        assert_eq!(
            c.cmd(&["HPEXPIRETIME", key, "FIELDS", "1", "b"]).items()[0].int(),
            at,
            "{key}"
        );
        assert_eq!(c.raw(&[b"HGET", key.as_bytes(), &field]).bulk(), value);
        assert_eq!(c.cmd(&["HGET", key, "b"]).text(), "2");
    }
    // By the block's own byte limit: 511 fields of 64 bytes with 64-byte
    // values fit, the 512th does not, whatever the settings say.
    let big: Pairs = (0..512)
        .map(|i| (format!("{i:064}").into_bytes(), vec![b'v'; 64]))
        .collect();
    for chunk in big[..511].chunks(50) {
        c.hset("bytes", chunk);
    }
    assert_eq!(c.encoding("bytes"), "listpack");
    c.hset("bytes", &big[511..]);
    assert_eq!(c.encoding("bytes"), "hashtable");
    assert_eq!(sorted(&c.hgetall("bytes")), sorted(&big));

    // The settings: clamped, under both names, reported as kept.
    for (name, set, want) in [
        ("hash-max-listpack-entries", "1000", "512"),
        ("hash-max-listpack-value", "100", "64"),
        ("hash-max-ziplist-entries", "300", "300"),
        ("hash-max-ziplist-value", "40", "40"),
    ] {
        assert!(c.cmd(&["CONFIG", "SET", name, set]).ok());
        for alias in [
            name.replace("ziplist", "listpack"),
            name.replace("listpack", "ziplist"),
        ] {
            let got = c.cmd(&["CONFIG", "GET", &alias]);
            assert_eq!(got.items()[1].text(), want, "{alias}");
        }
    }
    assert!(matches!(
        c.cmd(&["CONFIG", "SET", "hash-max-listpack-entries", "-1"]),
        Resp::Error(_)
    ));
    // Lower limits: a hash already over them is left as it is until a write
    // crosses them.
    c.cmd(&[
        "HSET", "low", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5",
    ]);
    c.cmd(&["CONFIG", "SET", "hash-max-listpack-entries", "4"]);
    c.cmd(&["CONFIG", "SET", "hash-max-listpack-value", "64"]);
    assert_eq!(c.encoding("low"), "listpack");
    assert_eq!(c.cmd(&["HGET", "low", "e"]).text(), "5");
    // Any write leaves it over the limit, as in Redis.
    c.cmd(&["HSET", "low", "a", "again"]);
    assert_eq!(c.encoding("low"), "hashtable");
    assert_eq!(c.cmd(&["HLEN", "low"]).int(), 5);
    c.cmd(&["HSET", "low4", "a", "1", "b", "2", "c", "3", "d", "4"]);
    assert_eq!(c.encoding("low4"), "listpack");
    c.cmd(&["HSET", "low4", "e", "5"]);
    assert_eq!(c.encoding("low4"), "hashtable");
    c.cmd(&["CONFIG", "SET", "hash-max-listpack-entries", "0"]);
    c.cmd(&["HSET", "never", "a", "1"]);
    assert_eq!(c.encoding("never"), "hashtable");
    c.cmd(&["CONFIG", "SET", "hash-max-listpack-entries", "512"]);
    c.cmd(&["CONFIG", "SET", "hash-max-listpack-value", "8"]);
    c.cmd(&["HSET", "short", "a", "12345678"]);
    assert_eq!(c.encoding("short"), "listpack");
    c.cmd(&["HSET", "short", "a", "123456789"]);
    assert_eq!(c.encoding("short"), "hashtable");
    c.cmd(&["CONFIG", "SET", "hash-max-listpack-value", "64"]);
}

/// A block comes back whole from one HSCAN, cursor 0, whatever COUNT says,
/// as Redis returns a listpack; a table is paged. A hash that turns into a
/// table between two calls of a scan of it has nothing missed: the scan of
/// the block had ended.
#[test]
fn hscan_of_each_form() {
    let dir = temp_dir("hscan");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    for form in &forms {
        let key = form.key;
        let reply = c.cmd(&["HSCAN", key, "0", "COUNT", "2"]);
        if form.form == "hashtable" {
            assert_ne!(reply.items()[0].text(), "0", "{key}");
            continue;
        }
        assert_eq!(reply.items()[0].text(), "0", "{key}");
        let page: Pairs = reply.items()[1]
            .items()
            .chunks(2)
            .map(|p| (p[0].bulk(), p[1].bulk()))
            .collect();
        assert_eq!(page, form.pairs, "{key}: whole, in order");
        let reply = c.cmd(&["HSCAN", key, "0", "COUNT", "1", "NOVALUES"]);
        let names: Vec<Vec<u8>> = reply.items()[1].items().iter().map(Resp::bulk).collect();
        let want: Vec<Vec<u8>> = form.pairs.iter().map(|(f, _)| f.clone()).collect();
        assert_eq!(names, want, "{key}: NOVALUES");
        let reply = c.cmd(&["HSCAN", key, "0", "MATCH", "*1*", "COUNT", "1"]);
        let page: Pairs = reply.items()[1]
            .items()
            .chunks(2)
            .map(|p| (p[0].bulk(), p[1].bulk()))
            .collect();
        let want: Pairs = form
            .pairs
            .iter()
            .filter(|(f, _)| f.contains(&b'1'))
            .cloned()
            .collect();
        assert_eq!(page, want, "{key}: MATCH");
    }
    // A table scanned across a page boundary while fields are added.
    let fields = pairs(600, "w");
    for chunk in fields.chunks(100) {
        c.hset("big", chunk);
    }
    let mut seen = Vec::new();
    let mut cursor = "0".to_string();
    let mut pages = 0;
    loop {
        let reply = c.cmd(&["HSCAN", "big", &cursor, "COUNT", "20"]);
        cursor = reply.items()[0].text();
        for pair in reply.items()[1].items().chunks(2) {
            seen.push((pair[0].bulk(), pair[1].bulk()));
        }
        pages += 1;
        if pages == 2 {
            c.cmd(&["HSET", "big", "added-meanwhile", "v"]);
        }
        if cursor == "0" {
            break;
        }
    }
    seen.retain(|(f, _)| f != b"added-meanwhile");
    seen.sort();
    seen.dedup();
    assert_eq!(seen, sorted(&fields), "each field seen");
    // A block that becomes a table after its scan.
    c.cmd(&["HSET", "turns", "a", "1", "b", "2"]);
    let reply = c.cmd(&["HSCAN", "turns", "0", "COUNT", "1"]);
    assert_eq!(reply.items()[0].text(), "0");
    assert_eq!(reply.items()[1].items().len(), 4);
    c.cmd(&["HSET", "turns", "c", &"x".repeat(100)]);
    assert_eq!(c.encoding("turns"), "hashtable");
}

/// A snapshot, an AOF replayed and an AOF rewritten give back every hash of
/// every form with the same fields, values and deadlines, in the same form.
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
    let mut c = server.conn();
    check_forms(&mut c, &forms, "snapshot");
    digests_match(&mut c, &forms, "snapshot");

    // AOF replay, with removals and conversions in the log.
    let dir = temp_dir("aof");
    let mut server = Server::start(dir.path(), &["--aof"]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    reference_copies(&mut c, &forms);
    for i in 0..600 {
        c.cmd(&["HSET", "churn", &format!("c{i}"), "v"]);
    }
    for i in 0..590 {
        c.cmd(&["HDEL", "churn", &format!("c{i}")]);
    }
    let churn = c.hgetall("churn");
    server.stop("NOSAVE");
    server.restart();
    let mut c = server.conn();
    check_forms(&mut c, &forms, "aof replay");
    digests_match(&mut c, &forms, "aof replay");
    assert_eq!(c.hgetall("churn"), churn);
    assert_eq!(c.encoding("churn"), "hashtable");

    // AOF rewrite, then replay of what it wrote.
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
    let mut c = server.conn();
    check_forms(&mut c, &forms, "aof rewrite");
    digests_match(&mut c, &forms, "aof rewrite");
    // A rewritten table of few fields is a block again, as Redis loads one.
    assert_eq!(sorted(&c.hgetall("churn")), sorted(&churn));
}

/// DUMP/RESTORE within a server and MIGRATE to another: the fields and the
/// form they make there.
#[test]
fn dump_restore_and_migrate() {
    let dir = temp_dir("dump");
    let server = Server::start(dir.path(), &[]);
    let other_dir = temp_dir("dump-to");
    let other = Server::start(other_dir.path(), &[]);
    let mut c = server.conn();
    let forms = load_forms(&mut c);
    for form in &forms {
        let key = form.key;
        let payload = c.cmd(&["DUMP", key]).bulk();
        let copy = format!("{key}-restored");
        assert!(c.raw(&[b"RESTORE", copy.as_bytes(), b"0", &payload]).ok());
        check_form(&mut c, &copy, form, "restored");
        assert_eq!(c.digest(&copy), c.digest(key), "{key}");
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
    let mut o = other.conn();
    check_forms(&mut o, &forms, "migrated");
    for form in &forms {
        assert_eq!(o.digest(form.key), c.digest(form.key), "{}", form.key);
    }
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

fn synced(m: &mut Conn, r: &mut Conn) {
    let offset = m.info("replication", "master_repl_offset");
    wait_for("the offset", || {
        r.info("replication", "slave_repl_offset") == offset
    });
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
    synced(&mut m, &mut r);
    check_forms(&mut r, &forms, "full sync");

    // Changes of form on the master, streamed.
    let more = pairs(600, "more");
    for chunk in more.chunks(100) {
        m.hset("h10", chunk);
    }
    forms[0].pairs.extend(more);
    forms[0].form = "hashtable";
    m.cmd(&["HDEL", "h512", "field:00000005"]);
    forms[1].pairs.remove(5);
    m.cmd(&["HSET", "value64", "f", &"x".repeat(65)]);
    forms[5].pairs[5].1 = vec![b'x'; 65];
    forms[5].form = "hashtable";
    m.cmd(&["HINCRBY", "empties", "n", "3"]);
    forms[7].pairs.push((b"n".to_vec(), b"3".to_vec()));
    m.cmd(&["HPEXPIRE", "empties", FAR_MS, "FIELDS", "1", "n"]);
    forms[7].form = "listpackex";
    synced(&mut m, &mut r);
    for form in &forms {
        check_form(&mut r, form.key, form, "stream");
        assert_eq!(r.digest(form.key), m.digest(form.key), "{}", form.key);
    }
    assert_eq!(r.cmd(&["DEBUG", "DIGEST"]), m.cmd(&["DEBUG", "DIGEST"]));
}

/// Fields of a hash in a block go when their time comes, read or not (lazy
/// and active expiry), and the hash with the last of them; a replica and the
/// AOF end up with what the master has.
#[test]
fn field_expiry_on_packed_hashes() {
    let dir = temp_dir("fexp");
    let mut master = Server::start(dir.path(), &["--aof"]);
    let mut m = master.conn();
    let rdir = temp_dir("fexp-replica");
    let port = master.port.to_string();
    let replica = Server::start(rdir.path(), &["--replicaof", "127.0.0.1", &port]);
    let mut r = replica.conn();
    wait_for("the link", || {
        r.info("replication", "master_link_status") == "up"
    });
    // Read: lazily hidden and removed.
    let fields = pairs(10, "field");
    m.hset("read", &fields);
    m.cmd(&[
        "HPEXPIRE",
        "read",
        "150",
        "FIELDS",
        "3",
        "field:00000000",
        "field:00000004",
        "field:00000009",
    ]);
    assert_eq!(m.encoding("read"), "listpackex");
    // Not read: removed by active expiry, the last field taking the key.
    m.hset("unread", &fields[..3]);
    m.cmd(&[
        "HPEXPIRE",
        "unread",
        "150",
        "FIELDS",
        "3",
        "field:00000000",
        "field:00000001",
        "field:00000002",
    ]);
    thread::sleep(Duration::from_millis(400));
    let want: Pairs = fields
        .iter()
        .enumerate()
        .filter(|(i, _)| ![0, 4, 9].contains(i))
        .map(|(_, p)| p.clone())
        .collect();
    assert_eq!(m.hgetall("read"), want);
    assert_eq!(m.cmd(&["HLEN", "read"]).int(), 7);
    wait_for("the hash all of whose fields went", || {
        m.cmd(&["EXISTS", "unread"]).int() == 0
    });
    synced(&mut m, &mut r);
    assert_eq!(r.hgetall("read"), want);
    assert_eq!(r.cmd(&["EXISTS", "unread"]).int(), 0);
    assert_eq!(r.cmd(&["DEBUG", "DIGEST"]), m.cmd(&["DEBUG", "DIGEST"]));
    let digest = m.cmd(&["DEBUG", "DIGEST"]);
    drop(replica);
    master.stop("NOSAVE");
    master.restart();
    let mut m = master.conn();
    assert_eq!(m.cmd(&["DEBUG", "DIGEST"]), digest, "AOF replay");
    assert_eq!(m.hgetall("read"), want);
}

/// HRANDFIELD on a block picks each field about as often as each other:
/// chi-square over ten fields, with bounds far from what chance gives.
#[test]
fn hrandfield_is_uniform_on_packed_hashes() {
    let dir = temp_dir("rand");
    let server = Server::start(dir.path(), &[]);
    let mut c = server.conn();
    let fields = pairs(10, "field");
    c.hset("r", &fields);
    assert_eq!(c.encoding("r"), "listpack");
    let index = |f: &[u8]| fields.iter().position(|(x, _)| x == f).unwrap();
    let chi = |counts: &[u64]| {
        let total: u64 = counts.iter().sum();
        let expect = total as f64 / counts.len() as f64;
        counts
            .iter()
            .map(|&n| (n as f64 - expect).powi(2) / expect)
            .sum::<f64>()
    };
    // 9 degrees of freedom: 50 is far past p = 1e-6.
    let mut counts = [0u64; 10];
    for f in c.cmd(&["HRANDFIELD", "r", "-20000"]).items() {
        counts[index(&f.bulk())] += 1;
    }
    assert!(chi(&counts) < 50.0, "repeats: {counts:?}");
    let mut counts = [0u64; 10];
    for _ in 0..3000 {
        counts[index(&c.cmd(&["HRANDFIELD", "r"]).bulk())] += 1;
    }
    assert!(chi(&counts) < 50.0, "one at a time: {counts:?}");
    let mut counts = [0u64; 10];
    for _ in 0..3000 {
        for f in c.cmd(&["HRANDFIELD", "r", "3"]).items() {
            counts[index(&f.bulk())] += 1;
        }
    }
    assert!(chi(&counts) < 50.0, "distinct: {counts:?}");
}

/// Under `allkeys-lru`, small hashes in blocks are counted at their size:
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
    let fields = pairs(10, "field");
    c.hset("probe", &fields);
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
        c.hset(&format!("k{i}"), &fields);
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
        "{kept} hashes of {each} bytes in {room}"
    );
    for i in (0..keys).step_by(97) {
        let key = format!("k{i}");
        if c.cmd(&["EXISTS", &key]).int() == 1 {
            assert_eq!(c.hgetall(&key), fields);
            assert_eq!(c.encoding(&key), "listpack");
        }
    }
}
