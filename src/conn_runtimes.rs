//! Where the tokio I/O model runs its plaintext client connections.
//!
//! Each connection runs on one of a fixed set of runtimes of one worker
//! thread each, the one with the fewest connections when it is accepted, and
//! stays there. A worker waits on its own connections' sockets and nothing
//! else, so a request that arrives while it waits wakes that worker alone,
//! once, and the reply goes out from the thread that read the request.
//!
//! A runtime of one worker is still tokio's multi-thread kind, so a command
//! that runs long (`helpers::long_work`, `block_in_place`) hands that
//! worker's other connections to a thread of the runtime's blocking pool,
//! which serves them meanwhile: no other connection waits for it.
//!
//! The server's other tasks (the accept loop, expiry, the replication hub,
//! TLS connections) stay on the main runtime.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One runtime per worker, and how many connections each has.
pub struct ConnRuntimes {
    workers: Vec<Worker>,
}

struct Worker {
    handle: tokio::runtime::Handle,
    connections: Arc<AtomicUsize>,
}

/// Counts a connection on its worker for as long as its task lives.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ConnRuntimes {
    /// `workers` runtimes (at least one), each with one worker thread. They
    /// live as long as the process.
    pub fn start(workers: usize) -> std::io::Result<Self> {
        let mut out = Vec::with_capacity(workers.max(1));
        for _ in 0..workers.max(1) {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_keep_alive(crate::commands::helpers::RUNTIME_THREAD_KEEP_ALIVE)
                .enable_all()
                .build()?;
            let handle = runtime.handle().clone();
            // Never shut down: dropped here, in an async context, it would
            // panic, and the connections on it are the server's.
            std::mem::forget(runtime);
            out.push(Worker {
                handle,
                connections: Arc::new(AtomicUsize::new(0)),
            });
        }
        Ok(Self { workers: out })
    }

    /// The worker with the fewest connections.
    fn pick(&self) -> &Worker {
        self.workers
            .iter()
            .min_by_key(|w| w.connections.load(Ordering::Relaxed))
            .expect("at least one worker")
    }

    /// Run `serve` on `socket` on a worker of its own. `socket` was accepted
    /// on the main runtime: it is moved to the worker's.
    pub fn spawn<F, Fut>(&self, socket: tokio::net::TcpStream, serve: F)
    where
        F: FnOnce(tokio::net::TcpStream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let socket = match socket.into_std() {
            Ok(socket) => socket,
            Err(e) => {
                crate::log_verbose!("Connection error: {e}");
                return;
            }
        };
        let worker = self.pick();
        worker.connections.fetch_add(1, Ordering::Relaxed);
        let counted = Counted(Arc::clone(&worker.connections));
        worker.handle.spawn(async move {
            let _counted = counted;
            // Non-blocking already (`into_std`); registered with this
            // worker's I/O driver here.
            match tokio::net::TcpStream::from_std(socket) {
                Ok(socket) => serve(socket).await,
                Err(e) => crate::log_verbose!("Connection error: {e}"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn connections_go_to_the_least_loaded_worker_and_are_counted_off() {
        let main = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        main.block_on(async {
            let conns = ConnRuntimes::start(2).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
            let mut clients = Vec::new();
            for _ in 0..4 {
                clients.push(tokio::net::TcpStream::connect(addr).await.unwrap());
                let (socket, _) = listener.accept().await.unwrap();
                let done_tx = done_tx.clone();
                conns.spawn(socket, move |mut socket| async move {
                    let mut byte = [0u8; 1];
                    // Served on a worker of its own runtime, not the main one.
                    let thread = std::thread::current().id();
                    if socket.read_exact(&mut byte).await.is_ok() {
                        let _ = socket.write_all(&byte).await;
                    }
                    let _ = done_tx.send(thread);
                });
            }
            let loads: Vec<usize> = conns
                .workers
                .iter()
                .map(|w| w.connections.load(Ordering::Relaxed))
                .collect();
            assert_eq!(loads, vec![2, 2]);
            for client in &mut clients {
                client.write_all(b"x").await.unwrap();
                let mut byte = [0u8; 1];
                client.read_exact(&mut byte).await.unwrap();
                assert_eq!(&byte, b"x");
            }
            let mut threads = std::collections::HashSet::new();
            for _ in 0..4 {
                threads.insert(done_rx.recv().await.unwrap());
            }
            assert_eq!(threads.len(), 2, "two workers served the four");
            // Each task has ended: its count is given back.
            for _ in 0..100 {
                if conns
                    .workers
                    .iter()
                    .all(|w| w.connections.load(Ordering::Relaxed) == 0)
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("connections still counted after their tasks ended");
        });
    }
}
