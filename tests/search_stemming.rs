// tests/search_stemming.rs
//
// Stemming of TEXT fields and the scores of FT.SEARCH, over the wire, held
// against what Redis 8.10.2 (search module 8.10.1) answers to the same
// commands: the expected scores below were read off Redis, with
// `EXPLAINSCORE` to see where each comes from.
//
// `FT.CONFIG` is the process's, and one test here sets `MINSTEMLEN`: every
// test holds `one_at_a_time()` for its whole body.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
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
    fn s(&self) -> &str {
        match self {
            V::Str(s) | V::Err(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }
    fn arr(&self) -> &[V] {
        match self {
            V::Arr(v) => v,
            other => panic!("not an array: {other:?}"),
        }
    }
    fn ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
    }
    /// Whether a flat `[name, value, …]` array has `name` set to `value`.
    fn has(&self, name: &str, value: &str) -> bool {
        self.arr()
            .chunks(2)
            .any(|p| matches!(p, [V::Str(n), V::Str(v)] if n == name && v == value))
    }
    fn err(&self) -> &str {
        match self {
            V::Err(e) => e,
            other => panic!("not an error: {other:?}"),
        }
    }
    /// The ids of an FT.SEARCH … NOCONTENT reply, sorted.
    fn id_set(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.arr()[1..].iter().map(|v| v.s().to_string()).collect();
        ids.sort();
        ids
    }
    /// The (id, score) pairs of an FT.SEARCH … WITHSCORES NOCONTENT reply.
    fn scores(&self) -> Vec<(String, f64)> {
        self.arr()[1..]
            .chunks(2)
            .map(|pair| (pair[0].s().to_string(), pair[1].s().parse().unwrap()))
            .collect()
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(s) => {
                    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
                    let writer = s.try_clone().unwrap();
                    return Self {
                        reader: BufReader::new(s),
                        writer,
                    };
                }
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                Err(e) => panic!("could not connect to port {port}: {e}"),
            }
        }
    }

    fn cmd(&mut self, args: &[&str]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
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

/// A free port outside the ephemeral range (see tests/search.rs).
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
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

fn port() -> u16 {
    static PORT: OnceLock<u16> = OnceLock::new();
    *PORT.get_or_init(|| {
        let port = free_port();
        thread::spawn(move || {
            kividb::run_for_test(kividb::config::Config {
                port,
                bind: "127.0.0.1".to_string(),
                threads: 2,
                loglevel: "error".to_string(),
                io_model: kividb::config::IoModel::Tokio,
                configfile: "./kividb.conf".to_string(),
                ..Default::default()
            });
        });
        for _ in 0..200 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return port;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {port} did not start");
    })
}

fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn conn() -> Conn {
    Conn::connect(port())
}

/// The scores of `query` on `index`, by id, against what Redis gave.
fn assert_scores(c: &mut Conn, index: &str, query: &str, extra: &[&str], want: &[(&str, f64)]) {
    let mut args = vec![
        "FT.SEARCH",
        index,
        query,
        "WITHSCORES",
        "NOCONTENT",
        "DIALECT",
        "2",
    ];
    args.extend_from_slice(extra);
    let reply = c.cmd(&args);
    let mut got = reply.scores();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    let mut want: Vec<(String, f64)> = want.iter().map(|(k, s)| (k.to_string(), *s)).collect();
    want.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got.iter().map(|g| &g.0).collect::<Vec<_>>(),
        want.iter().map(|w| &w.0).collect::<Vec<_>>(),
        "{query} {extra:?}: {reply:?}"
    );
    for ((id, g), (_, w)) in got.iter().zip(&want) {
        assert!(
            (g - w).abs() <= 1e-9 * w.abs().max(1.0),
            "{query} {extra:?}: {id} scored {g}, Redis {w}"
        );
    }
}

fn ids(c: &mut Conn, args: &[&str]) -> Vec<String> {
    let mut all = vec!["FT.SEARCH"];
    all.extend_from_slice(args);
    all.push("NOCONTENT");
    c.cmd(&all).id_set()
}

/// The report: mem0's keyword search found nothing for a word's other form.
#[test]
fn a_word_finds_its_other_forms() {
    let _one = one_at_a_time();
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "s",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "s:",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .ok()
    );
    c.cmd(&["HSET", "s:1", "t", "I prefer email"]);
    c.cmd(&["HSET", "s:2", "t", "running dogs"]);
    assert_eq!(ids(&mut c, &["s", "@t:preferences"]), ["s:1"]);
    assert_eq!(ids(&mut c, &["s", "preferred"]), ["s:1"]);
    assert_eq!(ids(&mut c, &["s", "run"]), ["s:2"]);
    assert_eq!(ids(&mut c, &["s", "dog"]), ["s:2"]);
    assert_eq!(ids(&mut c, &["s", "runs"]), ["s:2"]);
    // VERBATIM: the word only.
    assert!(ids(&mut c, &["s", "@t:preferences", "VERBATIM"]).is_empty());
    assert!(ids(&mut c, &["s", "dog", "VERBATIM"]).is_empty());
    assert_eq!(ids(&mut c, &["s", "prefer", "VERBATIM"]), ["s:1"]);
    // A quoted word is exact.
    assert!(ids(&mut c, &["s", "\"dog\""]).is_empty());
    // Patterns are not stemmed.
    assert!(ids(&mut c, &["s", "dog*"]).len() == 1);
    // A rewrite takes the old words and their stems out.
    c.cmd(&["HSET", "s:2", "t", "cats"]);
    assert!(ids(&mut c, &["s", "dog"]).is_empty());
    assert_eq!(ids(&mut c, &["s", "cat"]), ["s:2"]);
    // Through FT.AGGREGATE too.
    let reply = c.cmd(&["FT.AGGREGATE", "s", "preferences", "LOAD", "1", "@t"]);
    assert_eq!(reply.arr().len(), 2, "{reply:?}");
    c.cmd(&["FT.DROPINDEX", "s", "DD"]);
}

#[test]
fn nostem_fields_and_short_words() {
    let _one = one_at_a_time();
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "ns",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "ns:",
            "SCHEMA",
            "t",
            "TEXT",
            "n",
            "TEXT",
            "NOSTEM"
        ])
        .ok()
    );
    c.cmd(&["HSET", "ns:1", "t", "the dog ran home", "n", "running runs"]);
    c.cmd(&["HSET", "ns:2", "t", "dogs"]);
    // NOSTEM: no stems in the field, and a clause on it alone is not
    // expanded.
    assert_eq!(ids(&mut c, &["ns", "@n:runs"]), ["ns:1"]);
    assert!(ids(&mut c, &["ns", "@n:run"]).is_empty());
    assert!(ids(&mut c, &["ns", "run"]).is_empty());
    let plan = c.cmd(&["FT.EXPLAIN", "ns", "@n:runs"]);
    assert_eq!(plan.s(), "@n:runs\n");
    // `ran` is shorter than MINSTEMLEN (4): no stem, but it is found as
    // itself; `dogs` has `+dog`, which `dog` looks for.
    assert_eq!(ids(&mut c, &["ns", "@t:ran"]), ["ns:1"]);
    assert_eq!(ids(&mut c, &["ns", "@t:dog"]), ["ns:1", "ns:2"]);
    assert_eq!(ids(&mut c, &["ns", "@t:dogs"]), ["ns:1", "ns:2"]);

    // MINSTEMLEN, by either name; words indexed after it changes.
    assert!(
        c.cmd(&["FT.CONFIG", "SET", "MINSTEMLEN", "1"])
            .err()
            .contains("lower than 2")
    );
    assert!(
        c.cmd(&["CONFIG", "SET", "search-min-stem-len", "1"])
            .err()
            .contains("between 2")
    );
    assert!(c.cmd(&["CONFIG", "SET", "search-min-stem-len", "6"]).ok());
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "search-min-stem-len"]).arr()[1].s(),
        "6"
    );
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "ns6",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "ns6:",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .ok()
    );
    c.cmd(&["HSET", "ns6:1", "t", "dogs jumping"]);
    assert!(ids(&mut c, &["ns6", "dog"]).is_empty());
    assert_eq!(ids(&mut c, &["ns6", "jump"]), ["ns6:1"]);
    assert!(c.cmd(&["FT.CONFIG", "SET", "MINSTEMLEN", "4"]).ok());
    assert_eq!(
        c.cmd(&["FT.CONFIG", "GET", "MINSTEMLEN"]).arr()[0].arr()[1].s(),
        "4"
    );
    c.cmd(&["FT.DROPINDEX", "ns", "DD"]);
    c.cmd(&["FT.DROPINDEX", "ns6", "DD"]);
}

#[test]
fn languages() {
    let _one = one_at_a_time();
    let mut c = conn();
    assert_eq!(
        c.cmd(&[
            "FT.CREATE",
            "bad",
            "LANGUAGE",
            "klingon",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .err(),
        "Invalid language"
    );
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "de",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "de:",
            "LANGUAGE",
            "German",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .ok()
    );
    c.cmd(&["HSET", "de:1", "t", "Die Häuser sind schön"]);
    assert_eq!(ids(&mut c, &["de", "haus"]), ["de:1"]);
    assert_eq!(ids(&mut c, &["de", "Häusern"]), ["de:1"]);
    assert!(ids(&mut c, &["de", "häusern", "LANGUAGE", "english"]).is_empty());
    assert_eq!(
        c.cmd(&["FT.SEARCH", "de", "haus", "LANGUAGE", "klingon"])
            .err(),
        "No such language"
    );
    assert_eq!(
        c.cmd(&["FT.EXPLAIN", "de", "häusern"]).s(),
        "UNION {\n  häusern\n  +haus(expanded)\n  haus(expanded)\n}\n"
    );
    let info = c.cmd(&["FT.INFO", "de"]);
    let definition = &info.arr()[5];
    assert!(
        definition.has("default_language", "german"),
        "{definition:?}"
    );

    // A document's own language, from a field; one Redis does not know is
    // the index's.
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "ml",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "ml:",
            "LANGUAGE_FIELD",
            "lang",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .ok()
    );
    c.cmd(&["HSET", "ml:1", "t", "running dogs", "lang", "english"]);
    c.cmd(&["HSET", "ml:2", "t", "laufende Häuser", "lang", "german"]);
    c.cmd(&["HSET", "ml:3", "t", "running dogs", "lang", "klingon"]);
    c.cmd(&["HSET", "ml:4", "t", "running dogs"]);
    assert_eq!(ids(&mut c, &["ml", "run"]), ["ml:1", "ml:3", "ml:4"]);
    assert_eq!(ids(&mut c, &["ml", "haus", "LANGUAGE", "german"]), ["ml:2"]);
    // The language changes alone: the words are stemmed again.
    c.cmd(&["HSET", "ml:2", "lang", "english"]);
    assert!(ids(&mut c, &["ml", "haus", "LANGUAGE", "german"]).is_empty());
    c.cmd(&["HSET", "ml:2", "lang", "german"]);
    assert_eq!(ids(&mut c, &["ml", "haus", "LANGUAGE", "german"]), ["ml:2"]);
    let info = c.cmd(&["FT.INFO", "ml"]);
    assert!(info.arr()[5].has("language_field", "lang"), "{info:?}");
    c.cmd(&["FT.DROPINDEX", "de", "DD"]);
    c.cmd(&["FT.DROPINDEX", "ml", "DD"]);
}

#[test]
fn explain_shows_the_expansion() {
    let _one = one_at_a_time();
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "ex",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "ex:",
            "SCHEMA",
            "t",
            "TEXT",
            "u",
            "TEXT",
            "n",
            "TEXT",
            "NOSTEM"
        ])
        .ok()
    );
    // As Redis 8.10 writes them.
    let cases = [
        ("@t:dog", "@t:UNION {\n  @t:dog\n  @t:+dog(expanded)\n}\n"),
        (
            "@t:prefers",
            "@t:UNION {\n  @t:prefers\n  @t:+prefer(expanded)\n  @t:prefer(expanded)\n}\n",
        ),
        (
            "@t:dogs home",
            "INTERSECT {\n  @t:UNION {\n    @t:dogs\n    @t:+dog(expanded)\n    @t:dog(expanded)\n  }\n  UNION {\n    home\n    +home(expanded)\n  }\n}\n",
        ),
        ("\"dogs home\"", "EXACT {\n  dogs\n  home\n}\n"),
        ("@n:runs", "@n:runs\n"),
        (
            "cats",
            "UNION {\n  cats\n  +cat(expanded)\n  cat(expanded)\n}\n",
        ),
    ];
    for (query, want) in cases {
        assert_eq!(c.cmd(&["FT.EXPLAIN", "ex", query]).s(), want, "{query}");
    }
    assert_eq!(
        c.cmd(&["FT.EXPLAIN", "ex", "cats", "VERBATIM"]).s(),
        "cats\n"
    );
    c.cmd(&["FT.DROPINDEX", "ex"]);
}

/// A query, its options, and the scores Redis gave each document.
type Case<'a> = (&'a str, &'a [&'a str], &'a [(&'a str, f64)]);

/// Gap 4 of the report: scores on Redis's scale. Redis scores a TAG value a
/// query asks for as a term, which the user's `@user_id:{alice}` doubled.
#[test]
fn bm25std_scores_are_redis_scores() {
    let _one = one_at_a_time();
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "p",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "p:",
            "SCHEMA",
            "memory",
            "TEXT",
            "metadata",
            "TEXT",
            "user_id",
            "TAG",
            "agent_id",
            "TAG",
            "created_at",
            "NUMERIC",
        ])
        .ok()
    );
    for (key, memory, user) in [
        (
            "p:1",
            "Prefers email for updates and phone for urgent issues",
            "alice",
        ),
        ("p:2", "Lives in Berlin and likes hiking", "alice"),
        ("p:3", "Uses email at work", "alice"),
        ("p:4", "Allergic to peanuts", "bob"),
    ] {
        c.cmd(&[
            "HSET",
            key,
            "memory",
            memory,
            "metadata",
            "{}",
            "user_id",
            user,
            "created_at",
            "1",
        ]);
    }
    assert_scores(
        &mut c,
        "p",
        "@memory:(email | phone) @user_id:{alice}",
        &["SCORER", "BM25STD"],
        &[("p:1", 1.8096163597679125), ("p:3", 1.1433706324823634)],
    );
    assert_scores(
        &mut c,
        "p",
        "@memory:(email | phone)",
        &[],
        &[("p:1", 1.5232350189387174), ("p:3", 0.7549127721218928)],
    );
    assert_scores(
        &mut c,
        "p",
        "@memory:preferences",
        &[],
        &[("p:1", 0.9666934890833675)],
    );
    assert_scores(
        &mut c,
        "p",
        "@memory:(email | phone) @user_id:{alice}",
        &["SCORER", "TFIDF"],
        &[("p:1", 4.0), ("p:3", 2.0)],
    );
    c.cmd(&["FT.DROPINDEX", "p", "DD"]);

    // Field weights, stems, documents with no text, tags, wildcards.
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "a",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "a:",
            "SCHEMA",
            "t",
            "TEXT",
            "u",
            "TEXT",
            "WEIGHT",
            "2",
            "n",
            "TEXT",
            "NOSTEM",
            "g",
            "TAG",
            "x",
            "NUMERIC",
        ])
        .ok()
    );
    c.cmd(&[
        "HSET",
        "a:1",
        "t",
        "prefers cats and dogs",
        "u",
        "email email",
        "g",
        "red",
    ]);
    c.cmd(&[
        "HSET",
        "a:2",
        "t",
        "the dog ran home",
        "n",
        "running runs",
        "g",
        "red,blue",
    ]);
    c.cmd(&["HSET", "a:3", "g", "blue", "x", "5"]);
    c.cmd(&["HSET", "a:4", "other", "not in schema"]);
    c.cmd(&["HSET", "a:5", "t", "email preferences", "u", "cat"]);
    let cases: &[Case] = &[
        (
            "prefers",
            &[],
            &[("a:1", 1.522257372774615), ("a:5", 0.7942396779178668)],
        ),
        ("@t:prefers", &["VERBATIM"], &[("a:1", 0.9330317633507372)]),
        ("@u:email", &[], &[("a:1", 1.2289730719684653)]),
        (
            "email email",
            &[],
            &[("a:1", 2.4579461439369306), ("a:5", 1.5884793558357335)],
        ),
        (
            "@t:dogs",
            &[],
            &[("a:1", 1.8660635267014745), ("a:2", 1.126964465278121)],
        ),
        (
            "cat",
            &[],
            &[("a:5", 1.7809329110337433), ("a:1", 0.9330317633507372)],
        ),
        (
            "@g:{red | blue}",
            &[],
            &[
                ("a:3", 1.4815624971250623),
                ("a:2", 1.423394893798351),
                ("a:1", 0.5892256094238777),
            ],
        ),
        ("@x:[0 10]", &[], &[("a:3", 0.0)]),
        ("-@t:dog", &[], &[("a:3", 0.0), ("a:4", 0.0), ("a:5", 0.0)]),
        (
            "@t:dog ~@t:home",
            &[],
            &[("a:2", 2.253928930556242), ("a:1", 0.9330317633507372)],
        ),
        (
            "@t:pref*",
            &[],
            &[("a:5", 1.2576691090114025), ("a:1", 0.9330317633507372)],
        ),
        (
            "*",
            &[],
            &[
                ("a:1", 0.6730401489889968),
                ("a:2", 0.812933022657414),
                ("a:3", 1.6923077134691045),
                ("a:4", 1.6923077134691045),
                ("a:5", 0.9072164933249958),
            ],
        ),
        ("@t:(dog home)", &["SCORER", "TFIDF"], &[("a:2", 2.0)]),
        ("@t:(dog ran)", &["SCORER", "TFIDF"], &[("a:2", 4.0)]),
        (
            "@t:dog",
            &["SCORER", "TFIDF.DOCNORM"],
            &[("a:2", 0.4), ("a:1", 0.2857142857142857)],
        ),
        (
            "@t:dog | @u:email",
            &["SCORER", "DISMAX"],
            &[("a:1", 4.0), ("a:2", 1.0)],
        ),
        (
            "@t:dog",
            &["SCORER", "DOCSCORE"],
            &[("a:1", 1.0), ("a:2", 1.0)],
        ),
    ];
    for (query, extra, want) in cases {
        assert_scores(&mut c, "a", query, extra, want);
    }
    assert_eq!(
        c.cmd(&["FT.SEARCH", "a", "dog", "SCORER", "NOPE"]).err(),
        "No such scorer NOPE"
    );
    // ADDSCORES scores as FT.SEARCH does.
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "a",
        "prefers",
        "ADDSCORES",
        "SORTBY",
        "2",
        "@__score",
        "DESC",
    ]);
    assert_eq!(
        reply.arr()[1].arr()[1]
            .s()
            .parse::<f64>()
            .unwrap()
            .to_string()[..6],
        *"1.5222"
    );
    c.cmd(&["FT.DROPINDEX", "a", "DD"]);
}

#[test]
fn highlighting_marks_words_found_by_their_stems() {
    let _one = one_at_a_time();
    let mut c = conn();
    assert!(
        c.cmd(&[
            "FT.CREATE",
            "hl",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "hl:",
            "SCHEMA",
            "t",
            "TEXT"
        ])
        .ok()
    );
    c.cmd(&["HSET", "hl:1", "t", "cats 1234 jumped jumps"]);
    c.cmd(&["HSET", "hl:2", "t", "cat ran, the jumping"]);
    let shown = |c: &mut Conn, query: &str| -> Vec<String> {
        let reply = c.cmd(&[
            "FT.SEARCH",
            "hl",
            query,
            "HIGHLIGHT",
            "RETURN",
            "1",
            "t",
            "SORTBY",
            "t",
        ]);
        reply.arr()[1..]
            .chunks(2)
            .map(|pair| pair[1].arr()[1].s().to_string())
            .collect()
    };
    let mut got = shown(&mut c, "cat");
    got.sort();
    assert_eq!(
        got,
        [
            "<b>cat</b> ran, the jumping",
            "<b>cats</b> 1234 jumped jumps"
        ]
    );
    c.cmd(&["FT.DROPINDEX", "hl", "DD"]);
}
