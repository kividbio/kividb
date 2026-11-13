// src/cluster/migrate_conns.rs
//
// The connections `MIGRATE` keeps to its targets.
//
// `MIGRATE` used to dial its target afresh for every call and close the
// connection once the replies were in. It closed first, so every call left a
// socket in TIME_WAIT on the source, and a resharding tool, which sends one
// `MIGRATE` per batch of keys, could leave tens of thousands of them behind
// (#726, as #723 did for the search fan-out). A connection is now kept after
// a call, for the next one to the same target, the way Redis keeps one.
//
// What makes this different from the search fan-out's pool: `MIGRATE`
// writes, and a `RESTORE` without `REPLACE` sent twice fails the second time
// with `BUSYKEY`. So nothing is ever sent again. A kept connection is first
// checked for being still open, before anything is written on it: one the
// target has closed (its restart, its `timeout`, `CLIENT KILL`) shows as an
// end of stream, or a reset, without a byte sent, and a new connection is
// dialled instead. Once anything has been written, any failure is the
// command's error, as it always was, and the connection is closed.

use std::io::BufReader;
use std::time::{Duration, Instant};

/// How long a connection is kept with nothing sent on it: as in Redis.
pub(crate) const IDLE: Duration = Duration::from_secs(10);

/// How many connections are kept, to all targets together. A resharding
/// moves keys to a few targets at a time; more would only hold sockets open
/// on the targets for nothing.
pub(crate) const MOST: usize = 16;

/// What a kept connection was opened as: to where, logged in how (the
/// `AUTH` or `AUTH2` of the `MIGRATE` that opened it), and over which setup
/// of TLS between nodes. A connection is reused only for the same key, so
/// one made with other credentials, or with TLS files since replaced, is
/// never used.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Key {
    host: String,
    port: u16,
    /// The user of `AUTH2`, `None` with `AUTH`, and the password's SHA-256,
    /// not the password: a password no longer in use is not kept in memory
    /// for the cache's sake.
    auth: Option<(Option<Vec<u8>>, [u8; 32])>,
    /// [`crate::node_tls::generation`], when TLS between nodes is on.
    tls: Option<u64>,
}

impl Key {
    /// `auth` as `MIGRATE` sends it: `AUTH password` or `AUTH user password`.
    pub(crate) fn new(host: &str, port: u16, auth: Option<&[Vec<u8>]>) -> Self {
        use sha2::Digest;
        let auth = auth.map(|parts| match parts {
            [_, user, password] => (Some(user.clone()), sha2::Sha256::digest(password).into()),
            [.., password] => (None, sha2::Sha256::digest(password).into()),
            [] => (None, [0; 32]),
        });
        Self {
            host: host.to_string(),
            port,
            auth,
            tls: crate::node_tls::cluster().then(crate::node_tls::generation),
        }
    }
}

/// A connection to a target.
pub(crate) struct Conn {
    pub(crate) reader: BufReader<Box<dyn crate::node_tls::ReadWrite>>,
    /// The socket under `reader`: for its timeouts, and to look for its end.
    socket: std::net::TcpStream,
    /// The database last selected on it, if one was.
    pub(crate) db: Option<usize>,
    last_used: Instant,
}

impl Conn {
    pub(crate) fn new(
        stream: Box<dyn crate::node_tls::ReadWrite>,
        socket: std::net::TcpStream,
    ) -> Self {
        Self {
            reader: BufReader::new(stream),
            socket,
            db: None,
            last_used: Instant::now(),
        }
    }

    /// The timeouts of this call, whichever connection it goes on.
    pub(crate) fn set_timeout(&self, timeout: Duration) {
        let _ = self.socket.set_read_timeout(Some(timeout));
        let _ = self.socket.set_write_timeout(Some(timeout));
    }

    /// Whether the target still has this connection open, and nothing is
    /// waiting on it. Looked at without blocking, and without reading: an
    /// end of stream, a reset, or bytes nobody asked for (they would be read
    /// as the next call's replies) all mean it is not to be used.
    fn open(&self) -> bool {
        if !self.reader.buffer().is_empty() || self.socket.set_nonblocking(true).is_err() {
            return false;
        }
        let mut byte = [0u8; 1];
        let open = matches!(
            self.socket.peek(&mut byte),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
        );
        self.socket.set_nonblocking(false).is_ok() && open
    }

    fn idle_too_long(&self, now: Instant) -> bool {
        now.duration_since(self.last_used) >= IDLE
    }
}

static KEPT: std::sync::Mutex<Vec<(Key, Conn)>> = std::sync::Mutex::new(Vec::new());

/// Whether a thread is closing the connections that have been idle too long.
static SWEEPING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn kept() -> std::sync::MutexGuard<'static, Vec<(Key, Conn)>> {
    KEPT.lock().unwrap_or_else(|e| e.into_inner())
}

/// Take out of `kept` the connections idle too long.
fn expired(kept: &mut Vec<(Key, Conn)>, now: Instant) -> Vec<(Key, Conn)> {
    let mut gone = Vec::new();
    let mut i = 0;
    while i < kept.len() {
        if kept[i].1.idle_too_long(now) {
            gone.push(kept.swap_remove(i));
        } else {
            i += 1;
        }
    }
    gone
}

/// A kept connection for `key`, still open, if there is one: no longer
/// kept, for the caller to put back with [`put`] if it is still good after.
pub(crate) fn take(key: &Key) -> Option<Conn> {
    let now = Instant::now();
    let (conn, gone) = {
        let mut kept = kept();
        let gone = expired(&mut kept, now);
        let at = kept
            .iter()
            .enumerate()
            .filter(|(_, (k, _))| k == key)
            .max_by_key(|(_, (_, c))| c.last_used)
            .map(|(i, _)| i);
        (at.map(|i| kept.swap_remove(i).1), gone)
    };
    // Closed outside the lock.
    drop(gone);
    conn.filter(Conn::open)
}

/// Keep `conn` for the next call to `key`. It must be ready for one: every
/// reply read whole, and nothing after them. The one idle longest goes if
/// [`MOST`] are kept.
pub(crate) fn put(key: Key, mut conn: Conn) {
    let now = Instant::now();
    conn.last_used = now;
    let gone = {
        let mut kept = kept();
        let mut gone = expired(&mut kept, now);
        if kept.len() >= MOST
            && let Some(oldest) = (0..kept.len()).min_by_key(|&i| kept[i].1.last_used)
        {
            gone.push(kept.swap_remove(oldest));
        }
        kept.push((key, conn));
        gone
    };
    drop(gone);
    sweep_in_the_background();
}

/// Close, every second, the connections idle for [`IDLE`], as long as any
/// are kept: so that a target is not held a connection to for good after a
/// resharding. The thread ends once none are kept, and [`put`] starts
/// another. (`SWEEPING` is cleared with the lock held, and only when none
/// are kept; `put` looks at it after keeping one.)
fn sweep_in_the_background() {
    use std::sync::atomic::Ordering;
    if SWEEPING.swap(true, Ordering::AcqRel) {
        return;
    }
    let started = std::thread::Builder::new()
        .name("migrate-conns".into())
        .spawn(|| {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let gone = {
                    let mut kept = kept();
                    let gone = expired(&mut kept, Instant::now());
                    if kept.is_empty() {
                        SWEEPING.store(false, Ordering::Release);
                        drop(kept);
                        drop(gone);
                        return;
                    }
                    gone
                };
                drop(gone);
            }
        });
    if started.is_err() {
        SWEEPING.store(false, Ordering::Release);
    }
}

/// How many connections are kept to `host:port`.
#[cfg(test)]
pub(crate) fn kept_to(host: &str, port: u16) -> usize {
    kept()
        .iter()
        .filter(|(k, _)| k.host == host && k.port == port)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connect(port: u16) -> Conn {
        let socket = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        let stream: Box<dyn crate::node_tls::ReadWrite> = Box::new(socket.try_clone().unwrap());
        Conn::new(stream, socket)
    }

    /// A kept connection is given out again while the target holds it open,
    /// and not once the target has closed it, or after it sat idle for
    /// [`IDLE`]. Credentials are part of what it is kept for.
    #[test]
    fn a_kept_connection_is_reused_only_while_open_and_for_its_own_login() {
        let target = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = target.local_addr().unwrap().port();
        let auth = vec![b"AUTH".to_vec(), b"pw".to_vec()];
        let key = Key::new("127.0.0.1", port, Some(&auth));

        let conn = connect(port);
        let (accepted, _) = target.accept().unwrap();
        put(key.clone(), conn);
        assert_eq!(kept_to("127.0.0.1", port), 1);
        // Another password, or a user, is another login.
        let other = vec![b"AUTH".to_vec(), b"other".to_vec()];
        assert!(take(&Key::new("127.0.0.1", port, Some(&other))).is_none());
        let user = vec![b"AUTH".to_vec(), b"user".to_vec(), b"pw".to_vec()];
        assert!(take(&Key::new("127.0.0.1", port, Some(&user))).is_none());
        assert!(take(&Key::new("127.0.0.1", port, None)).is_none());
        let conn = take(&key).expect("the kept connection");
        assert_eq!(kept_to("127.0.0.1", port), 0);

        // Closed by the target: not given out.
        put(key.clone(), conn);
        drop(accepted);
        std::thread::sleep(Duration::from_millis(50));
        assert!(take(&key).is_none());

        // Idle too long: closed, not given out.
        let mut conn = connect(port);
        let _accepted = target.accept().unwrap();
        conn.last_used = Instant::now() - IDLE;
        kept().push((key.clone(), conn));
        assert!(take(&key).is_none());
        assert_eq!(kept_to("127.0.0.1", port), 0);
    }

    /// A connection with bytes waiting on it that no call asked for is not
    /// given out: they would be read as the next call's replies.
    #[test]
    fn a_connection_with_unasked_bytes_on_it_is_not_reused() {
        use std::io::Write;
        let target = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = target.local_addr().unwrap().port();
        let key = Key::new("127.0.0.1", port, None);
        let conn = connect(port);
        let (mut accepted, _) = target.accept().unwrap();
        put(key.clone(), conn);
        accepted.write_all(b"+OK\r\n").unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert!(take(&key).is_none());
    }
}
