// tests/keyspec_binary_keys.rs
//
// Generated from the key-spec table (part of #510): `COMMAND LIST` is every
// command the build serves, and for each that `keyspec::key_positions` says
// takes a key the invocation sent has `BINARY` — bytes no UTF-8 string
// holds — at every one of those positions. The reply may be anything but a
// refusal of the key as text: no "invalid UTF-8", no dropped connection,
// no panic. `WRONGTYPE`, "no such key" and errors of the arguments the
// table cannot give are all fine.
//
// A command the table names no key for is sent nothing. A command it names
// keys for that is not sent — one the run showed refuses a binary key
// today — is in `SKIPS` with the reply it gave, so a new command cannot be
// missed silently.
//
// Real `kividb` processes. The I/O model is the platform's own unless
// `KIVIDB_TEST_IO_MODEL` says. The key-spec table differs between the
// default build and `--features lua` (EVAL and FCALL name keys only in
// one), so this runs on both.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kividb::keyspec::{Layout, key_positions};

mod common;
use common::start_verified;

/// A key no UTF-8 string can hold.
const BINARY: &[u8] = b"k\xff\xfe\x00k";

/// Well-formed invocations for the commands a `BINARY`-everywhere form does
/// not form: a count of keys, an option or subcommand that names them, or a
/// non-key argument the command's parser needs. `BINARY` is at every
/// position `key_positions` returns for the form it is in.
const FORMS: &[(&str, &[&[u8]])] = &[
    // `numkeys` first.
    ("MSETEX", &[b"MSETEX", b"1", BINARY, b"v"]),
    ("LMPOP", &[b"LMPOP", b"1", BINARY, b"LEFT"]),
    ("ZMPOP", &[b"ZMPOP", b"1", BINARY, b"LEFT"]),
    ("SINTERCARD", &[b"SINTERCARD", b"1", BINARY]),
    ("SUNIONCARD", &[b"SUNIONCARD", b"1", BINARY]),
    ("SDIFFCARD", &[b"SDIFFCARD", b"1", BINARY]),
    ("ZINTERCARD", &[b"ZINTERCARD", b"1", BINARY]),
    ("ZUNION", &[b"ZUNION", b"1", BINARY]),
    ("ZINTER", &[b"ZINTER", b"1", BINARY]),
    ("ZDIFF", &[b"ZDIFF", b"1", BINARY]),
    ("TS.NRANGE", &[b"TS.NRANGE", b"1", BINARY, b"-", b"+"]),
    ("TS.NREVRANGE", &[b"TS.NREVRANGE", b"1", BINARY, b"-", b"+"]),
    // `numkeys` after an argument. The timeout sits where a number cannot
    // stand, so the form cannot block.
    ("BLMPOP", &[b"BLMPOP", b"x", b"1", BINARY, b"LEFT"]),
    ("BZMPOP", &[b"BZMPOP", b"x", b"1", BINARY, b"LEFT"]),
    ("EVAL", &[b"EVAL", b"return 1", b"1", BINARY]),
    (
        "EVALSHA",
        &[
            b"EVALSHA",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            b"1",
            BINARY,
        ],
    ),
    ("EVAL_RO", &[b"EVAL_RO", b"return 1", b"1", BINARY]),
    (
        "EVALSHA_RO",
        &[
            b"EVALSHA_RO",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            b"1",
            BINARY,
        ],
    ),
    ("FCALL", &[b"FCALL", b"f", b"1", BINARY]),
    ("FCALL_RO", &[b"FCALL_RO", b"f", b"1", BINARY]),
    // Destination, then `numkeys` sources.
    ("ZUNIONSTORE", &[b"ZUNIONSTORE", BINARY, b"1", BINARY]),
    ("ZINTERSTORE", &[b"ZINTERSTORE", BINARY, b"1", BINARY]),
    ("ZDIFFSTORE", &[b"ZDIFFSTORE", BINARY, b"1", BINARY]),
    ("CMS.MERGE", &[b"CMS.MERGE", BINARY, b"1", BINARY]),
    ("TDIGEST.MERGE", &[b"TDIGEST.MERGE", BINARY, b"1", BINARY]),
    // Keys after an operation or an option.
    ("BITOP", &[b"BITOP", b"AND", BINARY, BINARY]),
    ("XREAD", &[b"XREAD", b"STREAMS", BINARY, b"0"]),
    (
        "XREADGROUP",
        &[
            b"XREADGROUP",
            b"GROUP",
            b"g",
            b"c",
            b"STREAMS",
            BINARY,
            b"0",
        ],
    ),
    ("SORT", &[b"SORT", BINARY, b"STORE", BINARY]),
    (
        "GEORADIUS",
        &[
            b"GEORADIUS",
            BINARY,
            b"0",
            b"0",
            b"1",
            b"m",
            b"STORE",
            BINARY,
        ],
    ),
    (
        "GEORADIUSBYMEMBER",
        &[
            b"GEORADIUSBYMEMBER",
            BINARY,
            b"m",
            b"1",
            b"m",
            b"STORE",
            BINARY,
        ],
    ),
    // A refused port answers at once: the destination may not exist.
    (
        "MIGRATE",
        &[b"MIGRATE", b"127.0.0.1", b"1", BINARY, b"0", b"1000"],
    ),
    // Keys after a subcommand.
    ("MEMORY", &[b"MEMORY", b"USAGE", BINARY]),
    ("HIMPORT", &[b"HIMPORT", b"SET", BINARY, b"f", b"v"]),
    ("OBJECT", &[b"OBJECT", b"ENCODING", BINARY]),
    ("XINFO", &[b"XINFO", b"STREAM", BINARY]),
    ("XGROUP", &[b"XGROUP", b"CREATE", BINARY, b"g", b"$"]),
    // The element, score and attributes a form needs where a `BINARY`
    // argument would parse as an option, flag or encoding instead.
    ("VSETATTR", &[b"VSETATTR", BINARY, b"e", b"{}"]),
    ("VSIM", &[b"VSIM", BINARY, b"ELE", b"e"]),
    ("ZADD", &[b"ZADD", BINARY, b"1", b"m"]),
    // A path is not a key: it is filled with one the parser takes, so a
    // UTF-8 refusal can only be about the key. The JSON commands whose
    // path is optional get the form without one.
    ("JSON.GET", &[b"JSON.GET", BINARY]),
    ("JSON.DEL", &[b"JSON.DEL", BINARY]),
    ("JSON.FORGET", &[b"JSON.FORGET", BINARY]),
    ("JSON.TYPE", &[b"JSON.TYPE", BINARY]),
    ("JSON.STRLEN", &[b"JSON.STRLEN", BINARY]),
    ("JSON.ARRLEN", &[b"JSON.ARRLEN", BINARY]),
    ("JSON.ARRPOP", &[b"JSON.ARRPOP", BINARY]),
    ("JSON.OBJKEYS", &[b"JSON.OBJKEYS", BINARY]),
    ("JSON.OBJLEN", &[b"JSON.OBJLEN", BINARY]),
    ("JSON.CLEAR", &[b"JSON.CLEAR", BINARY]),
    ("JSON.TOGGLE", &[b"JSON.TOGGLE", BINARY]),
    ("JSON.RESP", &[b"JSON.RESP", BINARY]),
    ("JSON.MSET", &[b"JSON.MSET", BINARY, b"$", b"{}"]),
    ("JSON.MGET", &[b"JSON.MGET", BINARY, b"$"]),
    ("JSON.SET", &[b"JSON.SET", BINARY, b"$", b"{}"]),
    ("JSON.NUMINCRBY", &[b"JSON.NUMINCRBY", BINARY, b"$", b"1"]),
    ("JSON.NUMMULTBY", &[b"JSON.NUMMULTBY", BINARY, b"$", b"1"]),
    ("JSON.STRAPPEND", &[b"JSON.STRAPPEND", BINARY, b"$", b"x"]),
    ("JSON.ARRAPPEND", &[b"JSON.ARRAPPEND", BINARY, b"$", b"1"]),
    ("JSON.ARRINDEX", &[b"JSON.ARRINDEX", BINARY, b"$", b"1"]),
    (
        "JSON.ARRINSERT",
        &[b"JSON.ARRINSERT", BINARY, b"$", b"0", b"1"],
    ),
    ("JSON.ARRTRIM", &[b"JSON.ARRTRIM", BINARY, b"$", b"0", b"1"]),
    ("JSON.MERGE", &[b"JSON.MERGE", BINARY, b"$", b"{}"]),
];

/// Commands the table names keys for that are sent no binary key, and the
/// reply that put them here. When one is fixed it leaves this list, so the
/// gate keeps covering it.
const SKIPS: &[(&str, &str)] = &[
    // ("ARSET", "ERR ... refused the key as text"),
];

// ── RESP client, of bytes ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(Vec<u8>),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                let writer = s.try_clone().unwrap();
                return Self {
                    reader: BufReader::new(s),
                    writer,
                };
            }
            assert!(Instant::now() < deadline, "could not connect to {port}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.read()
    }

    fn read(&mut self) -> V {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).expect("reply");
        let line = String::from_utf8_lossy(&line).trim_end().to_string();
        let (kind, rest) = line.split_at(1);
        match kind {
            "+" => V::Str(rest.as_bytes().to_vec()),
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
                V::Str(data)
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

// ── The generated test ───────────────────────────────────────────────────────

/// The reply the wrong arity gives, so a longer form can be tried.
fn wrong_arity(reply: &V) -> bool {
    matches!(reply, V::Err(e) if e.to_ascii_lowercase().contains("wrong number of arguments"))
}

/// A refusal of the key as text: the reply, or that none came.
fn utf8_refusal(reply: &V) -> bool {
    match reply {
        V::Err(e) => e.to_ascii_lowercase().contains("utf"),
        _ => false,
    }
}

fn spawn(dir: &Path, port: u16) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kividb"));
    command.current_dir(dir).args([
        "--port",
        &port.to_string(),
        "--bind",
        "127.0.0.1",
        "--threads",
        "2",
    ]);
    if let Ok(model) = std::env::var("KIVIDB_TEST_IO_MODEL") {
        command.args(["--io-model", &model]);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("could not start kividb")
}

#[test]
fn every_command_takes_a_binary_key() {
    let dir = tempfile::tempdir().unwrap().keep();
    let (mut child, port) = start_verified(|port| spawn(&dir, port));
    let mut conn = Conn::connect(port);

    // Every command this build serves, top-level only: a `container|sub`
    // name is covered by its container's form (OBJECT ENCODING, MEMORY
    // USAGE), and the table keys on the container.
    let names = match conn.cmd(&[b"COMMAND", b"LIST"]) {
        V::Arr(names) => names
            .iter()
            .filter_map(|n| match n {
                V::Str(s) => Some(std::str::from_utf8(s).unwrap().to_ascii_uppercase()),
                _ => None,
            })
            .filter(|n| !n.contains('|'))
            .collect::<Vec<_>>(),
        other => panic!("COMMAND LIST: {other:?}"),
    };
    assert!(!names.is_empty(), "COMMAND LIST answered nothing");

    let formed = |name: &str| -> Option<Vec<Vec<u8>>> {
        FORMS
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, form)| form.iter().map(|a| a.to_vec()).collect())
    };

    let positions = |name: &str, form: &[Vec<u8>]| -> Vec<usize> {
        let args: Vec<&[u8]> = form.iter().map(|a| a.as_slice()).collect();
        key_positions(name, &args).iter().collect()
    };

    let mut covered: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    let mut no_keys: Vec<String> = Vec::new();
    let mut refusals: Vec<String> = Vec::new();
    let mut uncovered: Vec<String> = Vec::new();

    for name in &names {
        if let Some((_, why)) = SKIPS.iter().find(|(n, _)| n == name) {
            skipped.push(format!("{name} ({why})"));
            continue;
        }

        let sent: Option<V> = match formed(name) {
            Some(form) => {
                let at = positions(name, &form);
                assert!(
                    !at.is_empty(),
                    "FORMS entry for {name} names no keys: {form:?}"
                );
                for i in &at {
                    assert_eq!(
                        form[*i].as_slice(),
                        BINARY,
                        "FORMS entry for {name} has no binary key at position {i}: {form:?}"
                    );
                }
                Some(conn.cmd(&form.iter().map(|a| a.as_slice()).collect::<Vec<_>>()))
            }
            None => {
                // Every argument `BINARY`: a fixed-shape command names its
                // keys wherever they are. Longer forms are tried first —
                // they name more keys for the commands that take several —
                // and the first length that does not answer the wrong
                // arity is sent.
                let mut reply = None;
                for len in (4..=12usize).chain([3, 2]) {
                    let form: Vec<Vec<u8>> = std::iter::once(name.as_bytes().to_vec())
                        .chain(std::iter::repeat_n(BINARY.to_vec(), len - 1))
                        .collect();
                    if positions(name, &form).is_empty() {
                        continue;
                    }
                    let r = conn.cmd(&form.iter().map(|a| a.as_slice()).collect::<Vec<_>>());
                    if wrong_arity(&r) {
                        continue;
                    }
                    reply = Some(r);
                    break;
                }
                reply
            }
        };

        match sent {
            Some(reply) => {
                if utf8_refusal(&reply) {
                    refusals.push(format!("{name} refused the key as text: {reply:?}"));
                } else {
                    covered.push(name.clone());
                }
            }
            None => match kividb::keyspec::layout(name) {
                Layout::NoKeys => no_keys.push(name.clone()),
                _ => uncovered.push(format!(
                    "{name} names keys by the table but no probed form reached them \
                     (a FORMS or SKIPS entry is needed)"
                )),
            },
        }
    }

    eprintln!(
        "covered {} commands, skipped {}, {} take no keys",
        covered.len(),
        skipped.len(),
        no_keys.len()
    );
    assert!(
        refusals.is_empty(),
        "commands refusing a binary key:\n{}",
        refusals.join("\n")
    );
    assert!(
        uncovered.is_empty(),
        "commands neither covered nor in SKIPS:\n{}",
        uncovered.join("\n")
    );

    // The table knowing no command the run did not classify is what the
    // walk above guarantees; the server being up still is the other half
    // of the assertion.
    match conn.cmd(&[b"PING"]) {
        V::Str(s) if s == b"PONG" => {}
        other => panic!("server did not answer PONG: {other:?}"),
    }
    let _ = child.kill();
    let _ = child.wait();
}
