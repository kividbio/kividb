//! The port clients connect to, and `CONFIG SET port` moving it (#24).
//!
//! Every accept loop wraps its listener in one of the types here. To move
//! the port, `CONFIG SET` binds a new socket for each loop, before it
//! answers: a port that is in use is refused then, with Redis's `Unable to
//! listen on this port`, and nothing has changed. Each loop then takes one
//! of the new sockets and drops its old one, which closes the old port.
//! Connections already open stay open.
//!
//! Servers are told apart by the port they were started on: a test process
//! runs many, each with its own.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use tokio::sync::watch;

struct ServerPort {
    ip: IpAddr,
    /// What clients connect to now.
    current: AtomicU16,
    /// Accept loops listening: as many sockets are bound for a new port.
    loops: AtomicUsize,
    /// The io_uring workers share the port, each with a socket of its own.
    reuse_port: bool,
    /// Sockets bound for the new port, one for each loop to take.
    pending: Mutex<Vec<std::net::TcpListener>>,
    moved: watch::Sender<u64>,
}

static SERVERS: LazyLock<Mutex<HashMap<u16, Arc<ServerPort>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn server(configured: u16) -> Option<Arc<ServerPort>> {
    SERVERS.lock().get(&configured).cloned()
}

/// The port the server started on `configured` listens on now.
pub fn current_port(configured: u16) -> u16 {
    server(configured).map_or(configured, |s| s.current.load(Ordering::SeqCst))
}

/// One accept loop's part: told when the port moves.
pub struct PortWatch {
    server: Arc<ServerPort>,
    moved: watch::Receiver<u64>,
}

impl PortWatch {
    fn register(configured: u16, addr: SocketAddr, reuse_port: bool) -> Self {
        let server = Arc::clone(SERVERS.lock().entry(configured).or_insert_with(|| {
            Arc::new(ServerPort {
                ip: addr.ip(),
                current: AtomicU16::new(addr.port()),
                loops: AtomicUsize::new(0),
                reuse_port,
                pending: Mutex::new(Vec::new()),
                moved: watch::channel(0).0,
            })
        }));
        server.loops.fetch_add(1, Ordering::SeqCst);
        let moved = server.moved.subscribe();
        PortWatch { server, moved }
    }

    /// The socket for the new port, once the port has moved.
    async fn next(&mut self) -> std::net::TcpListener {
        loop {
            if self.moved.changed().await.is_err() {
                // The sender lives as long as `server`: never.
                std::future::pending::<()>().await;
            }
            if let Some(socket) = self.server.pending.lock().pop() {
                return socket;
            }
        }
    }
}

impl Drop for PortWatch {
    fn drop(&mut self) {
        self.server.loops.fetch_sub(1, Ordering::SeqCst);
    }
}

/// `CONFIG SET port`: listen on `port` instead, or say why not.
pub fn move_to(configured: u16, port: u16) -> Result<(), String> {
    let Some(server) = server(configured) else {
        return Err("the server is not listening".into());
    };
    if port == server.current.load(Ordering::SeqCst) {
        return Ok(());
    }
    let addr = SocketAddr::new(server.ip, port);
    let loops = server.loops.load(Ordering::SeqCst).max(1);
    let mut sockets = Vec::with_capacity(loops);
    for _ in 0..loops {
        sockets.push(bind(addr, server.reuse_port).map_err(|e| e.to_string())?);
    }
    *server.pending.lock() = sockets;
    server.current.store(port, Ordering::SeqCst);
    server.moved.send_modify(|n| *n += 1);
    crate::log_info!("Ready to accept connections on {addr}");
    Ok(())
}

fn bind(addr: SocketAddr, reuse_port: bool) -> std::io::Result<std::net::TcpListener> {
    let socket = || {
        let socket = socket2::Socket::new(
            socket2::Domain::for_address(addr),
            socket2::Type::STREAM,
            None,
        )?;
        // As the listener the server started with: a port in TIME_WAIT is
        // free.
        #[cfg(unix)]
        socket.set_reuse_address(true)?;
        Ok::<_, std::io::Error>(socket)
    };
    #[cfg(target_os = "linux")]
    if reuse_port {
        // With SO_REUSEPORT the kernel lets any socket that set it share
        // the port, another server's too: a probe without it is refused if
        // anything at all holds the port.
        socket()?.bind(&addr.into())?;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = reuse_port;
    let socket = socket()?;
    #[cfg(target_os = "linux")]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// A Tokio accept loop's listener.
pub struct Listener {
    inner: tokio::net::TcpListener,
    watch: PortWatch,
}

impl Listener {
    pub fn new(inner: tokio::net::TcpListener, configured: u16) -> Self {
        let addr = inner
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], configured)));
        let watch = PortWatch::register(configured, addr, false);
        Listener { inner, watch }
    }

    /// The next connection, on whichever port is current.
    pub async fn accept(&mut self) -> std::io::Result<(tokio::net::TcpStream, SocketAddr)> {
        loop {
            tokio::select! {
                accepted = self.inner.accept() => return accepted,
                socket = self.watch.next() => match tokio::net::TcpListener::from_std(socket) {
                    Ok(moved) => self.inner = moved,
                    Err(e) => crate::log_warn!("CONFIG SET port: {e}"),
                },
            }
        }
    }
}

/// An io_uring worker's listener.
#[cfg(target_os = "linux")]
pub struct UringListener {
    inner: tokio_uring::net::TcpListener,
    watch: PortWatch,
}

#[cfg(target_os = "linux")]
impl UringListener {
    pub fn new(inner: tokio_uring::net::TcpListener, configured: u16, addr: SocketAddr) -> Self {
        let watch = PortWatch::register(configured, addr, true);
        UringListener { inner, watch }
    }

    /// The next connection, on whichever port is current.
    pub async fn accept(&mut self) -> std::io::Result<(tokio_uring::net::TcpStream, SocketAddr)> {
        loop {
            tokio::select! {
                accepted = self.inner.accept() => return accepted,
                socket = self.watch.next() => {
                    self.inner = tokio_uring::net::TcpListener::from_std(socket);
                }
            }
        }
    }
}
