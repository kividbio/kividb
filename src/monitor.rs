// src/monitor.rs
//
// MONITOR mode — streams every executed command to subscribed clients.
//
// Hot-path contract: MONITOR_COUNT is an AtomicUsize initialized to 0.
// handler.rs does ONE Relaxed load per command. Zero cost when no monitors.
//
// Broadcast design: a single tokio::sync::broadcast channel replaces the
// per-client mpsc map. One send() delivers to all subscribers regardless
// of count — O(1) instead of O(n) in the number of monitors connected.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;

/// Number of currently connected MONITOR clients.
pub static MONITOR_COUNT: AtomicUsize = AtomicUsize::new(0);

// Broadcast channel capacity — how many unread lines can be buffered
// before the slowest subscriber starts dropping messages.
// 4096 is generous; a slow terminal reading MONITOR output is the
// only realistic consumer and it won't fall this far behind.
const BROADCAST_CAP: usize = 4096;

pub struct MonitorRegistry {
    tx: broadcast::Sender<Arc<str>>,
}

impl Default for MonitorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MonitorRegistry {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Self { tx }
    }

    /// Subscribe a new monitor client. Returns its receiver.
    /// Increments MONITOR_COUNT.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<str>> {
        MONITOR_COUNT.fetch_add(1, Ordering::Relaxed);
        self.tx.subscribe()
    }

    /// Decrement MONITOR_COUNT when a monitor client disconnects.
    pub fn unsubscribe(&self) {
        MONITOR_COUNT.fetch_sub(1, Ordering::Relaxed);
    }

    /// Broadcast to all connected monitors — O(1) regardless of count.
    /// Only called from handler.rs when MONITOR_COUNT > 0.
    #[inline]
    pub fn broadcast(&self, line: Arc<str>) {
        // send() only fails when there are no receivers — that's fine,
        // we already checked MONITOR_COUNT > 0 but a race is harmless.
        let _ = self.tx.send(line);
    }
}

pub type SharedMonitorRegistry = Arc<MonitorRegistry>;

pub fn new_monitor_registry() -> SharedMonitorRegistry {
    Arc::new(MonitorRegistry::new())
}

/// Show `cmd` to the clients in `MONITOR` mode, as called by `source`: a
/// client's address, or `lua` for a command a script calls. One atomic load
/// when there are none.
#[inline]
pub fn feed(registry: &SharedMonitorRegistry, cmd: &[&[u8]], db: usize, source: &str) {
    if MONITOR_COUNT.load(Ordering::Relaxed) == 0 {
        return;
    }
    let cmd: Vec<Vec<u8>> = cmd.iter().map(|arg| arg.to_vec()).collect();
    registry.broadcast(format_monitor_line(&cmd, db, source));
}

/// Format a monitor line: `+<secs>.<micros> [<db> <addr>] "CMD" "arg"\r\n`
pub fn format_monitor_line(cmd: &[Vec<u8>], db: usize, peer_addr: &str) -> Arc<str> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let micros = now.subsec_micros();

    let mut line = format!("+{secs}.{micros:06} [{db} {peer_addr}]");
    for token in cmd {
        line.push(' ');
        line.push('"');
        for &b in token {
            match b {
                b'\\' => line.push_str("\\\\"),
                b'"' => line.push_str("\\\""),
                b if b.is_ascii_graphic() || b == b' ' => line.push(b as char),
                _ => line.push_str(&format!("\\x{b:02x}")),
            }
        }
        line.push('"');
    }
    line.push_str("\r\n");
    Arc::from(line.as_str())
}

/// A connection in `MONITOR` mode.
///
/// The connection stays what it was — it can go on sending commands, as it
/// can in Redis — and what the monitors are shown reaches it the way any
/// out-of-band message does: through its push channel, which a task of this
/// fills. Dropping this ends the mode: on `RESET`, or with the connection.
///
/// (A monitor used to be handed to a loop of its own, which read nothing
/// the client sent: `RESET` and `QUIT` were ignored, and on the io_uring
/// server the loop woke a thousand times a second to find that out.)
pub struct Monitoring {
    task: tokio::task::JoinHandle<()>,
    registry: SharedMonitorRegistry,
}

impl Monitoring {
    pub fn start(
        registry: &SharedMonitorRegistry,
        push_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Self {
        let mut rx = registry.subscribe();
        let task = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(line) => {
                        if push_tx.send(line.as_bytes().to_vec()).await.is_err() {
                            break;
                        }
                    }
                    // The client is not reading fast enough to be shown
                    // everything: what it missed is gone.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Monitoring {
            task,
            registry: Arc::clone(registry),
        }
    }
}

impl Drop for Monitoring {
    fn drop(&mut self) {
        self.task.abort();
        self.registry.unsubscribe();
    }
}
