// tests/cli_directives.rs
//
// `redis-server`'s command-line directives (#390): `kividb --appendonly yes
// --save "900 1" --maxmemory 256mb ...`, as a Compose file, a Helm chart or a
// platform template writes them. Every directive a config file takes is taken
// on the command line, with the same checks; a flag KiviDB has wins over it,
// and it wins over the config file.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

mod common;
use common::{free_port, start_verified};

const BIN: &str = env!("CARGO_BIN_EXE_kividb");

/// A running server with its own directory and log, killed when dropped.
struct Server {
    child: Child,
    port: u16,
    dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    /// What the server has logged to standard error so far.
    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("stderr.log")).unwrap_or_default()
    }

    /// The reply to `CONFIG GET name`, after `AUTH password` if one is given.
    fn config(&self, password: Option<&str>, name: &str) -> String {
        let mut commands: Vec<Vec<&str>> = Vec::new();
        if let Some(p) = password {
            commands.push(vec!["AUTH", p]);
        }
        commands.push(vec!["CONFIG", "GET", name]);
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        for args in &commands {
            let mut out = format!("*{}\r\n", args.len());
            for a in args {
                out.push_str(&format!("${}\r\n{a}\r\n", a.len()));
            }
            stream.write_all(out.as_bytes()).unwrap();
        }
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if password.is_some() {
            reader.read_line(&mut line).unwrap();
            assert_eq!(line.trim_end(), "+OK", "AUTH");
        }
        // *2, $n, name, $m, value; or *0
        let mut first = String::new();
        reader.read_line(&mut first).unwrap();
        if first.trim_end() == "*0" {
            return "(nothing)".to_string();
        }
        let mut lines = vec![first.trim_end().to_string()];
        for _ in 0..4 {
            let mut l = String::new();
            reader.read_line(&mut l).unwrap();
            lines.push(l.trim_end().to_string());
        }
        assert_eq!(lines[0], "*2", "CONFIG GET {name}: {lines:?}");
        assert_eq!(lines[2], name, "CONFIG GET {name}: {lines:?}");
        lines[4].clone()
    }
}

/// Start a server on `args`, as given on a command line (the first being the
/// port's, which is added). `stdin` is what it is fed on standard input.
fn start_with(args: &[&str], stdin: Option<&str>) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let (child, p) = start_verified(|p| {
        let log = std::fs::File::create(path.join("stderr.log")).unwrap();
        let mut command = Command::new(BIN);
        command
            .current_dir(&path)
            .args(args)
            .args(["--port", &p.to_string(), "--bind", "127.0.0.1"])
            .stdout(Stdio::null())
            .stderr(log);
        if stdin.is_some() {
            command.stdin(Stdio::piped());
        } else {
            command.stdin(Stdio::null());
        }
        let mut child = command.spawn().expect("start kividb");
        if let Some(text) = stdin {
            let mut pipe = child.stdin.take().unwrap();
            pipe.write_all(text.as_bytes()).unwrap();
        }
        child
    });
    Server {
        child,
        port: p,
        dir,
    }
}

/// Run the binary on `args` and wait for it to exit: for a command line it
/// refuses.
fn run(args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run kividb")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn a_typical_compose_command_line() {
    let server = start_with(
        &[
            "--appendonly",
            "yes",
            "--requirepass",
            "s3cret",
            "--maxmemory",
            "256mb",
            "--maxmemory-policy",
            "allkeys-lru",
        ],
        None,
    );
    let pw = Some("s3cret");
    assert_eq!(server.config(pw, "appendonly"), "yes");
    assert_eq!(server.config(pw, "maxmemory"), "268435456");
    assert_eq!(server.config(pw, "maxmemory-policy"), "allkeys-lru");
    assert_eq!(server.config(pw, "requirepass"), "**redacted**");
}

#[test]
fn the_directives_of_the_issue() {
    let server = start_with(
        &[
            "--appendonly",
            "no",
            "--save",
            "",
            "--appendfsync",
            "everysec",
            "--dbfilename",
            "dump.rdb",
            "--timeout",
            "0",
            "--tcp-keepalive",
            "300",
            "--io-threads",
            "4",
            "--protected-mode",
            "no",
            "--loglevel",
            "warning",
            "--databases",
            "4",
        ],
        None,
    );
    assert_eq!(server.config(None, "appendonly"), "no");
    assert_eq!(server.config(None, "appendfsync"), "everysec");
    // `dbfilename` is accepted (the snapshot is always dump.kdb) and not reported.
    assert_eq!(server.config(None, "timeout"), "0");
    assert_eq!(server.config(None, "tcp-keepalive"), "300");
    // `io-threads` is accepted and, like `dbfilename`, not reported by CONFIG GET.
    assert_eq!(server.config(None, "protected-mode"), "no");
    assert_eq!(server.config(None, "databases"), "4");
    assert_eq!(server.config(None, "loglevel"), "warning");
    let log = server.log();
    // The ones KiviDB accepts and ignores are said so once each; `save ""`
    // asks for no snapshots and is not one of them.
    for ignored in ["--timeout", "--tcp-keepalive", "--io-threads"] {
        assert_eq!(
            log.matches(&format!("{ignored}: '")).count(),
            1,
            "{ignored}: {log}"
        );
    }
    assert!(!log.contains("--save"), "{log}");
}

#[test]
fn several_values_and_a_quoted_one() {
    let server = start_with(
        &[
            "--save", "900", "1", "--save", "300", "10", "--save", "60 10000",
        ],
        None,
    );
    assert_eq!(
        server
            .log()
            .matches("'save' is accepted but has no effect")
            .count(),
        3,
        "{}",
        server.log()
    );
    assert_eq!(server.config(None, "maxmemory"), "0");
}

#[test]
fn a_value_that_starts_with_a_dash() {
    let server = start_with(&["--requirepass", "-dash-", "--lfu-decay-time", "5"], None);
    assert_eq!(server.config(Some("-dash-"), "lfu-decay-time"), "5");
}

#[test]
fn a_config_file_first_with_overrides() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("redis.conf");
    std::fs::write(
        &file,
        "appendonly no\nauto-aof-rewrite-percentage 120\nmaxmemory-policy noeviction\n\
         maxmemory 1mb\nlfu-decay-time 3\n",
    )
    .unwrap();
    let file = file.to_str().unwrap();
    let server = start_with(
        &[
            file,
            // A directive over the file...
            "--appendonly",
            "yes",
            "--auto-aof-rewrite-percentage",
            "150",
            // ...a flag over the file, and over a directive.
            "--maxmemory",
            "2mb",
            "--maxmemory-policy",
            "allkeys-lfu",
        ],
        None,
    );
    assert_eq!(server.config(None, "appendonly"), "yes");
    assert_eq!(server.config(None, "auto-aof-rewrite-percentage"), "150");
    assert_eq!(server.config(None, "maxmemory"), "2097152");
    assert_eq!(server.config(None, "maxmemory-policy"), "allkeys-lfu");
    // Left as the file has it.
    assert_eq!(server.config(None, "lfu-decay-time"), "3");
}

#[test]
fn a_native_flag_wins_over_a_directive_in_either_order() {
    let server = start_with(
        &["--aof", "--appendonly", "no", "--APPENDFSYNC", "no"],
        None,
    );
    assert_eq!(server.config(None, "appendonly"), "yes");
    let server = start_with(&["--appendonly", "no", "--aof"], None);
    assert_eq!(server.config(None, "appendonly"), "yes");
}

#[test]
fn a_flag_given_twice_is_the_last_one() {
    let server = start_with(
        &["--appendfsync", "always", "--APPENDFSYNC", "everysec"],
        None,
    );
    assert_eq!(server.config(None, "appendfsync"), "everysec");
}

#[test]
fn config_text_on_standard_input() {
    let server = start_with(
        &["-", "--lfu-decay-time", "9"],
        Some("appendonly yes\nlfu-decay-time 4\nlfu-log-factor 7\n"),
    );
    assert_eq!(server.config(None, "appendonly"), "yes");
    // The command line's directives are applied over it.
    assert_eq!(server.config(None, "lfu-decay-time"), "9");
    assert_eq!(server.config(None, "lfu-log-factor"), "7");
}

#[test]
fn an_unknown_directive_names_itself_and_the_nearest() {
    let out = run(&["--maxmemry-policy", "allkeys-lru", "--port", "1"]);
    assert_eq!(out.status.code(), Some(1));
    let err = stderr(&out);
    assert!(err.contains("'--maxmemry-policy'"), "{err}");
    assert!(err.contains("did you mean '--maxmemory-policy'"), "{err}");
    let out = run(&["--frobnicate", "1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("'--frobnicate'"), "{}", stderr(&out));
}

#[test]
fn a_mistyped_flag_suggests_the_flag() {
    let out = run(&["--prot", "6380"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("did you mean '--port'"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn a_bad_value_is_refused_as_a_config_line_is() {
    let out = run(&["--appendonly", "maybe"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("--appendonly") && stderr(&out).contains("'yes' or 'no'"),
        "{}",
        stderr(&out)
    );
    let out = run(&["--dbfilename"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("needs a value"), "{}", stderr(&out));
}

#[test]
fn help_mentions_the_directive_form() {
    let out = run(&["--help"]);
    assert_eq!(out.status.code(), Some(0));
    let mut text = String::new();
    out.stdout.as_slice().read_to_string(&mut text).unwrap();
    assert!(text.contains("--<directive> <value>"), "{text}");
}

#[test]
fn ping_sees_the_port_and_password_of_the_command_line_and_a_config_file() {
    let server = start_with(&["--appendonly", "yes", "--requirepass", "pw"], None);
    let port = server.port.to_string();
    let ping = |args: &[&str]| {
        Command::new(BIN)
            .args(args)
            .env("KIVIDB_PASSWORD", "pw")
            .stdin(Stdio::null())
            .output()
            .unwrap()
    };
    // Directives given to the check are read, and checked, too.
    let out = ping(&["--ping", "--port", &port, "--appendonly", "yes"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let out = ping(&["--ping", "--port", &port, "--no-such-directive", "1"]);
    assert_eq!(out.status.code(), Some(1));
    // The port and password from a config file named first, `redis-server`
    // style, and a directive over it.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("redis.conf");
    std::fs::write(&file, format!("port {}\nrequirepass nope\n", free_port())).unwrap();
    let file = file.to_str().unwrap();
    let out = ping(&[file, "--ping", "--port", &port, "--requirepass", "pw"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
}
