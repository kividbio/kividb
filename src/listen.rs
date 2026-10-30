//! The ports clients connect to, and `CONFIG SET port` and `tls-port`
//! moving them (#24).
//!
//! Every accept loop wraps its listener in one of the types here. To move
//! the port, `CONFIG SET` binds a new socket for each loop, before it
//! answers: a port that is in use is refused then, with Redis's `Unable to
//! listen on this port`, and nothing has changed. Each loop then takes one
//! of the new sockets and drops its old one. Connections already open stay
//! open.
//!
//! On Linux the old port is closed before the reply, too: `CONFIG SET`
//! shuts the old sockets down (it keeps a copy of each for that), so a
//! connection that arrives after the reply is refused. That also ends an
//! accept that is waiting on the socket. An io_uring accept has to be ended
//! that way: dropping its future does not take it back from the kernel,
//! which goes on holding the socket, listening, and hands the next
//! connection to nobody (1.1.1 to 1.2.0-rc2).
//!
//! Port 0 is no port: a loop then has no socket and waits, until the port
//! is set again. That is how the TLS port is off, and how it is opened
//! with `CONFIG SET tls-port` on a server that started without it.
//!
//! Servers are told apart by the ports they were started on: a test
//! process runs many, each with its own.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use tokio::sync::watch;

/// Which of a server's ports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    /// `port`: plaintext.
    Plain,
    /// `tls-port`.
    Tls,
}

impl Kind {
    fn what(self) -> &'static str {
        match self {
            Kind::Plain => "connections",
            Kind::Tls => "TLS connections",
        }
    }
}

/// A server, by the `port` and `tls-port` it was started with.
pub type Server = (u16, u16);

/// The server `config` started.
pub fn server_of(config: &crate::config::Config) -> Server {
    (config.port, config.tls_port)
}

struct ServerPort {
    ip: IpAddr,
    /// What clients connect to now; 0 when there is none.
    current: AtomicU16,
    /// Accept loops listening: as many sockets are bound for a new port.
    loops: AtomicUsize,
    /// The io_uring workers share the port, each with a socket of its own.
    reuse_port: bool,
    /// Sockets bound for the new port, one for each loop to take.
    pending: Mutex<Vec<std::net::TcpListener>>,
    /// A copy of each socket listening on `current`, to shut it down with
    /// when the port moves or closes.
    #[cfg(target_os = "linux")]
    open: Mutex<Vec<socket2::Socket>>,
    moved: watch::Sender<u64>,
}

impl ServerPort {
    /// Keep `copy`, of a loop's socket on the current port.
    #[cfg(target_os = "linux")]
    fn hold(&self, copy: std::io::Result<socket2::Socket>) {
        match copy {
            Ok(copy) => self.open.lock().push(copy),
            Err(e) => crate::log_warn!("CONFIG SET port: can't keep the listening socket: {e}"),
        }
    }

    /// Stop the sockets of the port clients connected to until now from
    /// taking connections, and keep `now` instead.
    #[cfg(target_os = "linux")]
    fn shut_old(&self, now: Vec<socket2::Socket>) {
        let old = std::mem::replace(&mut *self.open.lock(), now);
        for socket in old {
            // A listening socket that is shut down listens no more: the
            // kernel refuses connections to it from then on, resets those
            // still waiting to be accepted, and ends the accept waiting on
            // it, with EINVAL.
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    }
}

type Ports = HashMap<(Server, Kind), Arc<ServerPort>>;

static SERVERS: LazyLock<Mutex<Ports>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn server_port(server: Server, kind: Kind) -> Option<Arc<ServerPort>> {
    SERVERS.lock().get(&(server, kind)).cloned()
}

/// The port of `kind` that `server` listens on now; 0 when it has none.
pub fn current_port(server: Server, kind: Kind) -> u16 {
    server_port(server, kind).map_or(
        match kind {
            Kind::Plain => server.0,
            Kind::Tls => server.1,
        },
        |s| s.current.load(Ordering::SeqCst),
    )
}

/// One accept loop's part: told when the port moves.
pub struct PortWatch {
    server: Arc<ServerPort>,
    moved: watch::Receiver<u64>,
}

impl PortWatch {
    fn register(
        server: Server,
        kind: Kind,
        addr: SocketAddr,
        open: bool,
        reuse_port: bool,
    ) -> Self {
        let server = Arc::clone(SERVERS.lock().entry((server, kind)).or_insert_with(|| {
            Arc::new(ServerPort {
                ip: addr.ip(),
                current: AtomicU16::new(if open { addr.port() } else { 0 }),
                loops: AtomicUsize::new(0),
                reuse_port,
                pending: Mutex::new(Vec::new()),
                #[cfg(target_os = "linux")]
                open: Mutex::new(Vec::new()),
                moved: watch::channel(0).0,
            })
        }));
        server.loops.fetch_add(1, Ordering::SeqCst);
        let moved = server.moved.subscribe();
        PortWatch { server, moved }
    }

    /// Whether the port has moved, or closed, since this loop last looked.
    fn has_moved(&self) -> bool {
        self.moved.has_changed().unwrap_or(false)
    }

    /// The socket for the new port once the port has moved, or `None` once
    /// it is closed.
    async fn next(&mut self) -> Option<std::net::TcpListener> {
        loop {
            if self.moved.changed().await.is_err() {
                // The sender lives as long as `server`: never.
                std::future::pending::<()>().await;
            }
            if self.server.current.load(Ordering::SeqCst) == 0 {
                return None;
            }
            if let Some(socket) = self.server.pending.lock().pop() {
                return Some(socket);
            }
        }
    }
}

impl Drop for PortWatch {
    fn drop(&mut self) {
        // The last loop of the port is gone, and with it what accepted:
        // the copies would keep the port listening, for nobody.
        #[cfg(target_os = "linux")]
        if self.server.loops.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.server.shut_old(Vec::new());
        }
        #[cfg(not(target_os = "linux"))]
        self.server.loops.fetch_sub(1, Ordering::SeqCst);
    }
}

/// `CONFIG SET port` or `tls-port`: listen on `port` instead, or on none
/// for 0, or say why not.
pub fn move_to(server: Server, kind: Kind, port: u16) -> Result<(), String> {
    let Some(server) = server_port(server, kind) else {
        return Err("the server is not listening".into());
    };
    let old = server.current.load(Ordering::SeqCst);
    if port == old {
        return Ok(());
    }
    if port == 0 {
        // Each accept loop lets its socket go as it wakes to this, a moment
        // after the reply: it is not waited for, as on io_uring this runs on
        // the very thread of one of those loops. On Linux the sockets are
        // shut down here, before the reply.
        server.pending.lock().clear();
        server.current.store(0, Ordering::SeqCst);
        server.moved.send_modify(|n| *n += 1);
        #[cfg(target_os = "linux")]
        server.shut_old(Vec::new());
        crate::log_info!(
            "No longer accepting {} on {}; the ones open stay open",
            kind.what(),
            SocketAddr::new(server.ip, old)
        );
        return Ok(());
    }
    let addr = SocketAddr::new(server.ip, port);
    let loops = server.loops.load(Ordering::SeqCst).max(1);
    // All of them or none: on an error those bound are dropped, and nothing
    // has changed.
    let sockets = bind_all(addr, loops, server.reuse_port).map_err(|e| e.to_string())?;
    #[cfg(target_os = "linux")]
    let copies = sockets
        .iter()
        .map(|socket| socket2::SockRef::from(socket).try_clone())
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    *server.pending.lock() = sockets;
    server.current.store(port, Ordering::SeqCst);
    // Before the old sockets are shut down: a loop whose accept that ends
    // knows from this why.
    server.moved.send_modify(|n| *n += 1);
    #[cfg(target_os = "linux")]
    server.shut_old(copies);
    crate::log_info!("Ready to accept {} on {addr}", kind.what());
    Ok(())
}

/// A socket for each of `loops` on `addr`, listening.
fn bind_all(
    addr: SocketAddr,
    loops: usize,
    reuse_port: bool,
) -> std::io::Result<Vec<std::net::TcpListener>> {
    #[cfg(target_os = "linux")]
    if reuse_port {
        // With SO_REUSEPORT the kernel lets any socket that set it share
        // the port, another server's too: a probe without it is refused if
        // anything at all listens on the port. Once, before the first of
        // ours: our own would refuse it after that (up to 1.2.0-rc2 it was
        // made for each, and every port was "in use").
        socket(addr)?.bind(&addr.into())?;
    }
    (0..loops).map(|_| bind(addr, reuse_port)).collect()
}

/// A socket for `addr`, set up as the listener the server started with: a
/// port in TIME_WAIT is free.
fn socket(addr: SocketAddr) -> std::io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        None,
    )?;
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    Ok(socket)
}

/// One socket on `addr`, listening, set as the server sets its sockets at
/// startup.
fn bind(addr: SocketAddr, reuse_port: bool) -> std::io::Result<std::net::TcpListener> {
    let socket = socket(addr)?;
    #[cfg(target_os = "linux")]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = reuse_port;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// What an accept loop woke to.
enum Woke<A> {
    Accepted(A),
    Moved(Option<std::net::TcpListener>),
}

/// A Tokio accept loop's listener.
pub struct Listener {
    inner: Option<tokio::net::TcpListener>,
    watch: PortWatch,
}

impl Listener {
    /// The loop of `server`'s port of `kind`, listening with `inner` on
    /// `addr`; with no `inner`, on no port until one is set.
    pub fn new(
        inner: Option<tokio::net::TcpListener>,
        addr: SocketAddr,
        server: Server,
        kind: Kind,
    ) -> Self {
        let watch = PortWatch::register(server, kind, addr, inner.is_some(), false);
        #[cfg(target_os = "linux")]
        if let Some(inner) = &inner {
            watch.server.hold(socket2::SockRef::from(inner).try_clone());
        }
        Listener { inner, watch }
    }

    /// The next connection, on whichever port is current.
    pub async fn accept(&mut self) -> std::io::Result<(tokio::net::TcpStream, SocketAddr)> {
        loop {
            let woke = tokio::select! {
                accepted = async {
                    match &self.inner {
                        Some(inner) => inner.accept().await,
                        None => std::future::pending().await,
                    }
                } => Woke::Accepted(accepted),
                // Dropping a Tokio accept loses nothing: it takes a
                // connection only when it is polled.
                socket = self.watch.next() => Woke::Moved(socket),
            };
            let socket = match woke {
                // Shut down by `move_to`.
                Woke::Accepted(Err(_)) if self.watch.has_moved() => self.watch.next().await,
                Woke::Accepted(accepted) => return accepted,
                Woke::Moved(socket) => socket,
            };
            match socket.map(tokio::net::TcpListener::from_std) {
                None => self.inner = None,
                Some(Ok(moved)) => self.inner = Some(moved),
                Some(Err(e)) => crate::log_warn!("CONFIG SET port: {e}"),
            }
        }
    }
}

/// An io_uring worker's listener.
#[cfg(target_os = "linux")]
pub struct UringListener {
    inner: Option<tokio_uring::net::TcpListener>,
    watch: PortWatch,
}

#[cfg(target_os = "linux")]
impl UringListener {
    /// As [`Listener::new`], for the plaintext port.
    pub fn new(
        inner: Option<tokio_uring::net::TcpListener>,
        server: Server,
        addr: SocketAddr,
    ) -> Self {
        let watch = PortWatch::register(server, Kind::Plain, addr, inner.is_some(), true);
        if let Some(inner) = &inner {
            watch.server.hold(copy_of(inner));
        }
        UringListener { inner, watch }
    }

    /// The next connection, on whichever port is current.
    pub async fn accept(&mut self) -> std::io::Result<(tokio_uring::net::TcpStream, SocketAddr)> {
        loop {
            let Some(inner) = &self.inner else {
                self.inner = self
                    .watch
                    .next()
                    .await
                    .map(tokio_uring::net::TcpListener::from_std);
                continue;
            };
            // The accept is in the kernel from its first poll until it
            // completes, and dropping its future does not take it back:
            // the socket would stay open and listening until a connection
            // came, and that one would be accepted for nobody, and hang. So
            // the accept is always awaited to the end, here once the socket
            // is shut down (by `move_to`, and here again), which ends it at
            // once.
            let (socket, raced) = {
                let accept = inner.accept();
                tokio::pin!(accept);
                let woke = tokio::select! {
                    accepted = &mut accept => Woke::Accepted(accepted),
                    socket = self.watch.next() => Woke::Moved(socket),
                };
                match woke {
                    Woke::Accepted(Err(_)) if self.watch.has_moved() => {
                        (self.watch.next().await, None)
                    }
                    Woke::Accepted(accepted) => return accepted,
                    Woke::Moved(socket) => {
                        shut_down(inner);
                        (socket, accept.await.ok())
                    }
                }
            };
            self.inner = socket.map(tokio_uring::net::TcpListener::from_std);
            // A connection accepted as the port moved is served.
            if let Some(accepted) = raced {
                return Ok(accepted);
            }
        }
    }
}

/// A copy of `listener`'s socket.
#[cfg(target_os = "linux")]
fn copy_of(listener: &tokio_uring::net::TcpListener) -> std::io::Result<socket2::Socket> {
    use std::os::fd::{AsRawFd, BorrowedFd};
    // SAFETY: the descriptor is open as long as `listener` is, and it is
    // borrowed only to be duplicated.
    let fd = unsafe { BorrowedFd::borrow_raw(listener.as_raw_fd()) };
    Ok(socket2::Socket::from(fd.try_clone_to_owned()?))
}

/// Shut `listener`'s socket down: see [`ServerPort::shut_old`].
#[cfg(target_os = "linux")]
fn shut_down(listener: &tokio_uring::net::TcpListener) {
    use std::os::fd::AsRawFd;
    // SAFETY: a plain system call on a descriptor that `listener` keeps
    // open. It fails harmlessly on a socket already shut down.
    unsafe {
        libc::shutdown(listener.as_raw_fd(), libc::SHUT_RDWR);
    }
}
