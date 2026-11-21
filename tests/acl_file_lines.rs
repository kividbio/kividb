#![allow(clippy::disallowed_methods)]
// The ACL file (#510): every file an earlier KiviDB loaded loads to the same
// users and rules, and every user `ACL SETUSER` takes is saved and read back
// the same.
//
// The ignored test needs `KIVIDB_1_2_BIN`: a KiviDB 1.2 release binary (or
// another build to compare with). It loads each file of `CASES` there and
// here, and compares `ACL LIST`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Bulk(Vec<u8>),
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
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        let writer = s.try_clone().unwrap();
        Self {
            reader: BufReader::new(s),
            writer,
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
        let line = String::from_utf8_lossy(&line);
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
                V::Bulk(data)
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                V::Arr((0..n.max(0)).map(|_| self.read()).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }

    /// `ACL LIST`, one line a user.
    fn acl_list(&mut self) -> Vec<Vec<u8>> {
        match self.cmd(&[b"ACL", b"LIST"]) {
            V::Arr(users) => users
                .into_iter()
                .map(|u| match u {
                    V::Bulk(b) => b,
                    other => panic!("{other:?}"),
                })
                .collect(),
            other => panic!("ACL LIST: {other:?}"),
        }
    }
}

fn this_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kividb"))
}

fn old_bin() -> PathBuf {
    PathBuf::from(
        std::env::var("KIVIDB_1_2_BIN")
            .expect("KIVIDB_1_2_BIN must name another KiviDB binary (a 1.2 release)"),
    )
}

/// A server started on `dir`'s `users.acl`, if it starts.
struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
    bin: PathBuf,
}

impl Server {
    fn start(bin: &Path, dir: &Path) -> Option<Server> {
        let port = common::free_port();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let mut child = Command::new(bin)
            .current_dir(dir)
            .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
            .args(["--threads", "1", "--aclfile", "users.acl"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("could not start kividb");
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(Some(_)) = child.try_wait() {
                return None;
            }
            let up = TcpStream::connect(("127.0.0.1", port)).and_then(|mut s| {
                s.set_read_timeout(Some(Duration::from_secs(5)))?;
                s.write_all(b"*1\r\n$4\r\nPING\r\n")?;
                let mut reply = [0u8; 1];
                s.read_exact(&mut reply)?;
                Ok(())
            });
            if up.is_ok() {
                return Some(Server {
                    child,
                    port,
                    dir: dir.to_path_buf(),
                    bin: bin.to_path_buf(),
                });
            }
            assert!(
                Instant::now() < deadline,
                "the server neither started nor exited"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn conn(&self) -> Conn {
        Conn::connect(self.port)
    }

    fn restart(mut self) -> Server {
        // No reply comes: the server goes.
        let _ = self
            .conn()
            .writer
            .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n");
        let _ = self.child.wait();
        let (bin, dir) = (self.bin.clone(), self.dir.clone());
        Server::start(&bin, &dir).expect("restarts on its own file")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `ACL LIST` of a server started on `file` by `bin`; `None` if it does not
/// start.
fn loaded(bin: &Path, file: &[u8]) -> Option<Vec<Vec<u8>>> {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("users.acl"), file).unwrap();
    let server = Server::start(bin, dir.path())?;
    Some(server.conn().acl_list())
}

const DEFAULT: &[u8] = b"user default on nopass ~* &* +@all";

/// ACL files an earlier KiviDB loads, and the users it loads them to
/// (`ACL LIST`, generated with v1.2.2: `acl_files_load_as_1_2_loads_them`
/// checks them against it).
/// What a case is, the file, and the users `ACL LIST` gives.
type Case = (&'static str, &'static [u8], &'static [&'static [u8]]);

const CASES: &[Case] = &[
    (
        "a name that begins with a double quote",
        b"user \"bob on nopass ~* +@all\n",
        &[DEFAULT, b"user \"bob on nopass ~* resetchannels +@all"],
    ),
    (
        "a name that begins with a single quote",
        b"user 'bob on nopass ~* +@all\n",
        &[DEFAULT, b"user 'bob on nopass ~* resetchannels +@all"],
    ),
    (
        "a name in quotes",
        b"user \"bob\" on nopass ~* +@all\n",
        &[DEFAULT, b"user \"bob\" on nopass ~* resetchannels +@all"],
    ),
    (
        "\"default\" beside default",
        b"user \"default\" on nopass ~q:* +@all\nuser default on nopass ~* &* +@all\n",
        &[
            b"user \"default\" on nopass ~q:* resetchannels +@all",
            DEFAULT,
        ],
    ),
    (
        "patterns with a quote inside",
        b"user q on nopass ~a\"b* ~c'd &e\"f +@all\n",
        &[DEFAULT, b"user q on nopass ~a\"b* ~c'd &e\"f +@all"],
    ),
    (
        "patterns with a NUL inside",
        b"user n on nopass ~a\x00* &c\x00d +@all\n",
        &[DEFAULT, b"user n on nopass ~a\x00* &c\x00d +@all"],
    ),
    (
        "NBSP between two words",
        "user s on nopass ~*\u{a0}resetkeys ~bob:* +@all\n".as_bytes(),
        &[DEFAULT, b"user s on nopass ~bob:* resetchannels +@all"],
    ),
    (
        "U+3000, \\x0b and NEL between words",
        "user t\u{3000}on nopass\x0b~x:*\u{85}+@all\n".as_bytes(),
        &[DEFAULT, b"user t on nopass ~x:* resetchannels +@all"],
    ),
    (
        "CRLF line ends and white space at the ends",
        b"  user c on nopass ~* +@all \t\r\nuser d off\t \r\n",
        &[
            b"user c on nopass ~* resetchannels +@all",
            b"user d off resetkeys resetchannels -@all",
            DEFAULT,
        ],
    ),
    (
        "Unicode white space at the ends",
        "\u{3000}user i on nopass ~* +@all\u{a0}\n".as_bytes(),
        &[DEFAULT, b"user i on nopass ~* resetchannels +@all"],
    ),
    (
        "comments and blank lines",
        b"# users\n   # indented\n\n\t\nuser e on nopass ~* +@all\n",
        &[DEFAULT, b"user e on nopass ~* resetchannels +@all"],
    ),
    (
        "a selector",
        b"user f on nopass resetkeys +@all (~s:* +get) (%R~r:* &c +@read)\n",
        &[
            DEFAULT,
            b"user f on nopass resetkeys resetchannels +@all (~s:* resetchannels -@all +get) (%R~r:* &c -@all +@read)",
        ],
    ),
];

/// The users must be the same in either order of the lines `ACL LIST` gives.
fn sorted(mut users: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    users.sort();
    users
}

#[test]
fn acl_files_of_earlier_versions_load_to_the_same_users() {
    for (what, file, want) in CASES {
        let got = loaded(&this_bin(), file).unwrap_or_else(|| panic!("{what}: did not start"));
        assert_eq!(
            sorted(got),
            sorted(want.iter().map(|u| u.to_vec()).collect()),
            "{what}"
        );
    }
}

/// The table above, checked against another binary.
#[test]
#[ignore = "needs a 1.2 binary in KIVIDB_1_2_BIN"]
fn acl_files_load_as_1_2_loads_them() {
    let old = old_bin();
    for (what, file, want) in CASES {
        let there = loaded(&old, file).unwrap_or_else(|| panic!("{what}: 1.2 did not start"));
        let here = loaded(&this_bin(), file).unwrap_or_else(|| panic!("{what}: did not start"));
        assert_eq!(sorted(here), sorted(there.clone()), "{what}");
        assert_eq!(
            sorted(there),
            sorted(want.iter().map(|u| u.to_vec()).collect()),
            "{what}: the table"
        );
    }
}

/// Every user `ACL SETUSER` takes is saved by `ACL SAVE` and read back, at a
/// restart, to the same users.
#[test]
fn every_user_setuser_takes_is_read_back_the_same() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("users.acl"), DEFAULT).unwrap();
    let server = Server::start(&this_bin(), dir.path()).expect("starts");
    let mut c = server.conn();
    let users: &[&[&[u8]]] = &[
        &[b"\"bob", b"on", b">pw", b"~*", b"+@all"],
        &[b"'bob", b"on", b"nopass", b"~a\"b*", b"&c'd", b"+@all"],
        &[b"\"q\"", b"off", b"%R~r:*", b"%W~w:*", b"+get"],
        &[
            b"bin",
            b"on",
            b">\xff\x01",
            b"~app:\xff*",
            b"&ch:\xfe*",
            b"+@all",
        ],
        &[
            b"sel",
            b"on",
            b"nopass",
            b"resetkeys",
            b"+@all",
            b"(~s:\xfe* %R~t:* +get)",
        ],
        &[
            b"nul\x01",
            b"on",
            b"nopass",
            b"~\x01\x7f*",
            b"-@all",
            b"+set",
        ],
    ];
    for rules in users {
        let mut args: Vec<&[u8]> = vec![b"ACL", b"SETUSER"];
        args.extend_from_slice(rules);
        assert_eq!(c.cmd(&args), V::Str("OK".into()), "{rules:?}");
    }
    // What the file could not hold the same is refused.
    let bad: &[[&[u8]; 2]] = &[
        [b"sp ace", b"on"],
        [b"tab\tname", b"on"],
        ["nbsp\u{a0}name".as_bytes(), b"on"],
        [b"p", "~a\u{a0}b".as_bytes()],
        [b"p", "~a\u{3000}b".as_bytes()],
        [b"p", b"~a\x0bb"],
        [b"p", b"~a\x00b"],
        [b"p", b"&a\x00"],
        [b"p", b"&a b"],
    ];
    for bad in bad {
        let mut args: Vec<&[u8]> = vec![b"ACL", b"SETUSER"];
        args.extend_from_slice(bad);
        assert!(matches!(c.cmd(&args), V::Err(_)), "{bad:?}");
    }
    let before = sorted(c.acl_list());
    assert_eq!(c.cmd(&[b"ACL", b"SAVE"]), V::Str("OK".into()));
    drop(c);
    let server = server.restart();
    assert_eq!(sorted(server.conn().acl_list()), before, "after a restart");
}

/// A file with a pattern only a file can hold (a NUL, as an earlier KiviDB
/// saved one) is saved and read back the same too.
#[test]
fn a_pattern_with_a_nul_from_the_file_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("users.acl"),
        b"user default on nopass ~* &* +@all\nuser n on nopass ~a\x00* &c\x00d +@all\n",
    )
    .unwrap();
    let server = Server::start(&this_bin(), dir.path()).expect("starts");
    let mut c = server.conn();
    let before = sorted(c.acl_list());
    assert!(before.contains(&b"user n on nopass ~a\x00* &c\x00d +@all".to_vec()));
    assert_eq!(c.cmd(&[b"ACL", b"SAVE"]), V::Str("OK".into()));
    assert_eq!(c.cmd(&[b"ACL", b"LOAD"]), V::Str("OK".into()));
    assert_eq!(sorted(c.acl_list()), before);
    drop(c);
    let server = server.restart();
    assert_eq!(sorted(server.conn().acl_list()), before);
}
