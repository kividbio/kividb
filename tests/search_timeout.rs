// tests/search_timeout.rs
//
// `search-timeout` and `search-on-timeout`, the server settings for how
// long FT.SEARCH and FT.AGGREGATE may run when a query names no `TIMEOUT`,
// and what running out means. They are `FT.CONFIG`'s `TIMEOUT` and
// `ON_TIMEOUT`, which are the process's, so this file has one test of its
// own: the other search tests would see them change under them.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

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

/// A port nothing listens on, outside the ephemeral range (see
/// tests/generics.rs).
fn free_port() -> u16 {
    let from = (std::process::id() as u16).wrapping_mul(43);
    (0..)
        .map(|step: u16| 10_000 + from.wrapping_add(step) % 20_000)
        .find(|&port| port_is_free(port))
        .unwrap()
}

/// A reply, as far as this test reads one.
#[derive(Debug)]
enum V {
    Str(String),
    Err(String),
    Int(i64),
    Arr(Vec<V>),
    Map(Vec<(V, V)>),
    Nil,
}

impl V {
    fn s(&self) -> &str {
        match self {
            V::Str(s) | V::Err(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }
    fn is_ok(&self) -> bool {
        matches!(self, V::Str(s) if s == "OK")
    }
    fn is_err(&self) -> bool {
        matches!(self, V::Err(_))
    }
    fn get(&self, name: &str) -> &V {
        match self {
            V::Map(pairs) => pairs
                .iter()
                .find(|(k, _)| matches!(k, V::Str(k) if k == name))
                .map(|(_, v)| v)
                .unwrap_or_else(|| panic!("no {name} in {self:?}")),
            other => panic!("not a map: {other:?}"),
        }
    }
    fn warned(&self) -> bool {
        match self.get("warning") {
            V::Arr(items) => items
                .iter()
                .any(|w| w.s().contains("Timeout limit was reached")),
            other => panic!("not an array: {other:?}"),
        }
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn connect(port: u16) -> Self {
        for _ in 0..200 {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(20)))
                    .unwrap();
                return Conn {
                    reader: BufReader::new(stream.try_clone().unwrap()),
                    writer: stream,
                };
            }
            thread::sleep(Duration::from_millis(50));
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

    fn cmd(&mut self, args: &[&str]) -> V {
        self.send(args);
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
            "_" => V::Nil,
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
                V::Arr((0..n.max(0)).map(|_| self.read()).collect())
            }
            "%" => {
                let n: usize = rest.parse().unwrap();
                V::Map((0..n).map(|_| (self.read(), self.read())).collect())
            }
            other => panic!("unexpected RESP type {other:?} in {line:?}"),
        }
    }

    /// `CONFIG GET name`'s value (RESP3: a map).
    fn config(&mut self, name: &str) -> String {
        self.cmd(&["CONFIG", "GET", name]).get(name).s().to_string()
    }

    /// `FT.CONFIG GET name`'s value (RESP3: a map).
    fn ft_config(&mut self, name: &str) -> String {
        self.cmd(&["FT.CONFIG", "GET", name])
            .get(name)
            .s()
            .to_string()
    }

    /// `n` hashes `to:{start}` …, pipelined.
    fn hset_many(&mut self, start: usize, n: usize) {
        for chunk in (start..start + n).collect::<Vec<_>>().chunks(250) {
            for i in chunk {
                self.send(&["HSET", &format!("to:{i}"), "t", "hello"]);
            }
            for _ in chunk {
                assert!(matches!(self.read(), V::Int(_)));
            }
        }
    }
}

/// The settings are given in the config file, and read and set by `CONFIG`
/// as by `FT.CONFIG`: one value, whichever name sets it. With no `TIMEOUT`
/// in a query, `search-timeout` is its limit, and `search-on-timeout` what
/// running out of it means.
#[test]
fn search_timeout_is_the_limit_of_a_query_that_names_none() {
    let port = free_port();
    let mut config = kividb::config::Config {
        port,
        bind: "127.0.0.1".to_string(),
        threads: 2,
        loglevel: "error".to_string(),
        io_model: kividb::config::IoModel::Tokio,
        configfile: "./kividb.conf".to_string(),
        ..Default::default()
    };
    // As lines of the config file are applied.
    let warnings = config
        .apply_config_text("search-timeout 1\nsearch-on-timeout return\n", &|_| false)
        .unwrap();
    assert!(warnings.is_empty(), "{warnings:?}");
    thread::spawn(move || kividb::run_for_test(config));
    let mut c = Conn::connect(port);
    assert!(matches!(c.cmd(&["HELLO", "3"]), V::Map(_)));
    assert_eq!(c.config("search-timeout"), "1");
    assert_eq!(c.ft_config("TIMEOUT"), "1");
    assert_eq!(c.config("search-on-timeout"), "return");

    assert!(
        c.cmd(&[
            "FT.CREATE",
            "to",
            "PREFIX",
            "1",
            "to:",
            "SCHEMA",
            "t",
            "TEXT",
        ])
        .is_ok()
    );
    // Until a query with no TIMEOUT of its own times out, then four times
    // as many again, as in tests/search.rs.
    let search = |c: &mut Conn| c.cmd(&["FT.SEARCH", "to", "*", "NOCONTENT", "LIMIT", "0", "0"]);
    let mut n = 0;
    while !(search(&mut c).warned() && search(&mut c).warned()) {
        assert!(n < 200_000, "no timeout after {n} documents");
        c.hset_many(n, 8_000);
        n += 8_000;
    }
    c.hset_many(n, 4 * n);
    n *= 5;
    let reply = search(&mut c);
    assert!(reply.warned(), "{reply:?}");
    // The query's own TIMEOUT wins.
    let reply = c.cmd(&[
        "FT.SEARCH",
        "to",
        "*",
        "NOCONTENT",
        "LIMIT",
        "0",
        "0",
        "TIMEOUT",
        "0",
    ]);
    assert!(!reply.warned(), "{reply:?}");
    assert!(matches!(reply.get("total_results"), V::Int(t) if *t == n as i64));
    let reply = c.cmd(&[
        "FT.AGGREGATE",
        "to",
        "*",
        "GROUPBY",
        "0",
        "REDUCE",
        "COUNT",
        "0",
    ]);
    assert!(reply.warned(), "{reply:?}");

    // `search-on-timeout fail`: an error.
    assert!(
        c.cmd(&["CONFIG", "SET", "search-on-timeout", "FAIL"])
            .is_ok()
    );
    assert_eq!(c.config("search-on-timeout"), "fail");
    assert_eq!(c.ft_config("ON_TIMEOUT"), "fail");
    assert!(search(&mut c).is_err());
    assert!(
        c.cmd(&["FT.CONFIG", "SET", "ON_TIMEOUT", "return_strict"])
            .is_ok()
    );
    assert_eq!(c.config("search-on-timeout"), "return_strict");
    assert!(search(&mut c).is_err());
    assert!(
        c.cmd(&["CONFIG", "SET", "search-on-timeout", "return"])
            .is_ok()
    );
    assert_eq!(c.ft_config("ON_TIMEOUT"), "return");

    // One value, whichever name sets it.
    assert!(c.cmd(&["CONFIG", "SET", "search-timeout", "50"]).is_ok());
    assert_eq!(c.ft_config("TIMEOUT"), "50");
    assert!(c.cmd(&["FT.CONFIG", "SET", "TIMEOUT", "70"]).is_ok());
    assert_eq!(c.config("search-timeout"), "70");

    // A value either does not take is refused, and changes nothing.
    for (name, bad) in [
        ("search-timeout", "-1"),
        ("search-timeout", "soon"),
        ("search-on-timeout", "sometimes"),
    ] {
        let reply = c.cmd(&["CONFIG", "SET", name, bad]);
        assert!(reply.is_err(), "{name} {bad}: {reply:?}");
    }
    // Past what `search-timeout` takes: refused by `FT.CONFIG` too, so that
    // what `CONFIG REWRITE` writes reads back.
    for (option, bad) in [("TIMEOUT", "9223372036854775808"), ("ON_TIMEOUT", "never")] {
        let reply = c.cmd(&["FT.CONFIG", "SET", option, bad]);
        assert!(reply.is_err(), "{option} {bad}: {reply:?}");
    }
    assert_eq!(c.config("search-timeout"), "70");
    assert_eq!(c.config("search-on-timeout"), "return");
    assert!(
        kividb::config::Config::default()
            .apply_config_text("search-timeout soon\n", &|_| false)
            .is_err()
    );

    // `0`: no limit.
    assert!(c.cmd(&["CONFIG", "SET", "search-timeout", "0"]).is_ok());
    let reply = search(&mut c);
    assert!(!reply.warned(), "{reply:?}");
    assert!(matches!(reply.get("total_results"), V::Int(t) if *t == n as i64));
}
