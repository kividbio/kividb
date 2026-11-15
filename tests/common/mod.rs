// tests/common/mod.rs
//
// Shared by the integration tests that start `kividb` processes and start them
// again (`mod common;`). Every test file is its own crate, so a file takes only
// what it uses: the allow below keeps the rest from being reported as dead code.
//
// Why a restart has to be verified. Under `cargo nextest` every test is a
// process of its own and many run at once, each starting servers on ports it
// picked for itself. Between a server's exit and its restart its port is free
// (a restarted server binds only after it has loaded its data), and a server of
// another test process can start there. The restarted one then cannot listen,
// and a client that only connected reads the other's (empty) dataset as what the
// restart had loaded: a false report of data loss. So a start is accepted only
// when the server answering on the port says it is the process that was started,
// and if another one answers, or the child exits because it could not listen,
// the child is started again on another free port (what matters on a restart is
// the data directory, not the port).
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::Child;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

/// Whether nothing has `port`. It is asked with a socket that is bound and
/// never listens. One that listened could be given a connection: a server
/// another test is starting holds, between its fork and its exec, a copy of
/// every socket this process has open, and a client that dialled the port in
/// that moment was connected to the copy and then reset.
pub fn port_is_free(port: u16) -> bool {
    let Ok(probe) = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None) else {
        return false;
    };
    // As the server's own socket, so that a copy of this one, while it
    // lasts, does not keep the server from the port.
    #[cfg(unix)]
    let _ = probe.set_reuse_address(true);
    probe
        .bind(&SocketAddr::from(([127, 0, 0, 1], port)).into())
        .is_ok()
}

/// A port nothing is listening on, and not one the system hands out to
/// outgoing connections (10000-29999 is below the ephemeral range). A client
/// that starts dialling before its server is listening can otherwise be given,
/// as its own end, the very port it is dialling: it is then connected to itself
/// and reads back what it writes. The start of the sequence is the process id,
/// so that test processes running side by side start apart.
pub fn free_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(0);
    loop {
        let step = NEXT.fetch_add(1, Ordering::Relaxed);
        let from = (std::process::id() as u16).wrapping_mul(31);
        let port = 10_000 + from.wrapping_add(step) % 20_000;
        if port_is_free(port) {
            return port;
        }
    }
}

/// What a server answering on a port says about itself.
#[derive(Debug, PartialEq)]
pub enum Answer {
    /// Its process id (`INFO server`).
    Pid(u32),
    /// It answered, but does not say (`INFO` refused, as before `AUTH`).
    Unsaid,
    /// Nothing answered: no connection, or one that was reset or timed out.
    /// An io_uring server's listening socket outlives the process for a
    /// moment, and a client that dialled then is reset.
    Silent,
}

/// Ask the server on `port` who it is.
pub fn answer(port: u16) -> Answer {
    let Ok(mut s) = TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_secs(2),
    ) else {
        return Answer::Silent;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
    if s.write_all(b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n")
        .is_err()
    {
        return Answer::Silent;
    }
    // A bulk string: `$<len>\r\n<info>\r\n`; anything else is an error reply.
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n") {
        if s.read_exact(&mut byte).is_err() {
            return Answer::Silent;
        }
        head.push(byte[0]);
        if head.len() > 64 {
            return Answer::Unsaid;
        }
    }
    let Some(len) = head
        .strip_prefix(b"$")
        .and_then(|n| std::str::from_utf8(n).ok())
        .and_then(|n| n.trim().parse::<usize>().ok())
    else {
        return Answer::Unsaid;
    };
    let mut body = vec![0u8; len];
    if s.read_exact(&mut body).is_err() {
        return Answer::Silent;
    }
    String::from_utf8_lossy(&body)
        .lines()
        .find_map(|l| l.strip_prefix("process_id:"))
        .and_then(|pid| pid.trim().parse().ok())
        .map_or(Answer::Unsaid, Answer::Pid)
}

/// The process id the server answering on `port` says it has, if it says.
pub fn answering_pid(port: u16) -> Option<u32> {
    match answer(port) {
        Answer::Pid(pid) => Some(pid),
        _ => None,
    }
}

/// How one try to start a server ended.
#[derive(Debug, PartialEq)]
pub enum Started {
    /// The child's own server answers on the port (or one that does not say
    /// who it is, as before `AUTH`: it is taken for the one started).
    Ours,
    /// Another process answers on the port.
    Foreign(u32),
    /// The child exited: it could not listen (or failed otherwise).
    Exited,
}

/// Wait until the server answering on `port` is `child`, or is not, or `child`
/// has exited. Panics if nothing answers within `wait`.
pub fn confirm(child: &mut Child, port: u16, wait: Duration) -> Started {
    let deadline = Instant::now() + wait;
    loop {
        if child.try_wait().unwrap().is_some() {
            return Started::Exited;
        }
        match answer(port) {
            Answer::Pid(pid) if pid == child.id() => return Started::Ours,
            Answer::Pid(pid) => return Started::Foreign(pid),
            Answer::Unsaid => return Started::Ours,
            Answer::Silent => {}
        }
        assert!(Instant::now() < deadline, "no server came up on {port}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Start a server with `spawn` (which is given the port to listen on) on
/// `port`, and wait until the server answering on it is the child just
/// started. If another process answers, or the child exits because it could not
/// listen, the child is killed and started again on another free port, up to 10
/// times. Returns the child and the port it ended up on, and `true` if that is
/// not `port`: whatever was pointed at the old one (replicas, a relay, a
/// client's address) has to be pointed at the new one.
pub fn restart_verified(mut spawn: impl FnMut(u16) -> Child, port: u16) -> (Child, u16, bool) {
    let before = port;
    let mut port = port;
    for _ in 0..10 {
        let mut child = spawn(port);
        match confirm(&mut child, port, Duration::from_secs(60)) {
            Started::Ours => return (child, port, port != before),
            Started::Foreign(pid) => {
                eprintln!(
                    "port {port} is answered by process {pid}, not by the server started on it"
                )
            }
            Started::Exited => eprintln!("the server could not take port {port}"),
        }
        let _ = child.kill();
        let _ = child.wait();
        port = free_port();
    }
    panic!("no port could be had for the server");
}

/// `restart_verified` for a first start: on a port of its own picking.
pub fn start_verified(spawn: impl FnMut(u16) -> Child) -> (Child, u16) {
    let (child, port, _) = restart_verified(spawn, free_port());
    (child, port)
}

/// Like `restart_verified`, but the port cannot change (a cluster node's
/// address is in its `nodes.conf` and in its peers'): if another process has
/// taken it, or the child cannot listen on it, panics with that said.
pub fn restart_verified_on(mut spawn: impl FnMut(u16) -> Child, port: u16) -> Child {
    let mut child = spawn(port);
    match confirm(&mut child, port, Duration::from_secs(60)) {
        Started::Ours => child,
        Started::Foreign(pid) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "port {port} taken by another process during restart (process {pid} answers on it); \
                 not a data loss of the restarted server"
            );
        }
        Started::Exited => {
            panic!(
                "port {port} taken by another process during restart: the server could not listen"
            )
        }
    }
}
