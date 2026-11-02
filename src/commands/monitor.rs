// src/monitor.rs
//
// MONITOR mode — streams every executed command to subscribed clients.
//
// Hot-path contract: MONITOR_COUNT is an AtomicUsize initialized to 0.
// The post-command block in handler.rs does ONE load per command.
// When no monitors are connected (the common case) this load is
// branch-predicted-not-taken and costs ~1ns. No formatting, no allocation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use ahash::AHashMap;
use tokio::sync::mpsc;

/// Number of currently connected MONITOR clients.
/// Incremented when a client enters monitor mode, decremented on disconnect.
/// handler.rs reads this with Relaxed ordering — one load per command.
pub static MONITOR_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Per-monitor sender. Maps client_id → channel sender.
/// Protected by RwLock — only written when a monitor connects/disconnects
/// (rare), read when broadcasting (only when MONITOR_COUNT > 0).
type MonitorSenders = Arc<RwLock<AHashMap<u64, mpsc::UnboundedSender<Arc<str>>>>>;

/// Global registry of connected monitor clients.
pub struct MonitorRegistry {
    senders: MonitorSenders,
}

impl MonitorRegistry {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(RwLock::new(AHashMap::new())),
        }
    }

    /// Register a new monitor client. Returns the receiver end of its channel.
    pub fn register(&self, client_id: u64) -> mpsc::UnboundedReceiver<Arc<str>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.senders.write().insert(client_id, tx);
        MONITOR_COUNT.fetch_add(1, Ordering::Relaxed);
        rx
    }

    /// Deregister a monitor client on disconnect.
    pub fn deregister(&self, client_id: u64) {
        if self.senders.write().remove(&client_id).is_some() {
            // Saturating so a bug can't wrap below zero.
            MONITOR_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Broadcast a formatted monitor line to all connected monitors.
    /// Only called from handler.rs when MONITOR_COUNT > 0.
    pub fn broadcast(&self, line: Arc<str>) {
        // Read lock — multiple commands can broadcast concurrently.
        let senders = self.senders.read();
        for tx in senders.values() {
            // UnboundedSender::send only fails if the receiver is dropped
            // (client disconnected). We ignore the error — the deregister
            // path will clean up the sender on the next command.
            let _ = tx.send(Arc::clone(&line));
        }
    }
}

pub type SharedMonitorRegistry = Arc<MonitorRegistry>;

pub fn new_monitor_registry() -> SharedMonitorRegistry {
    Arc::new(MonitorRegistry::new())
}

/// Format a monitor line in Redis wire format:
/// `+<unix-ts.us> [<db> <addr>:<port>] "CMD" "arg1" "arg2"\r\n`
///
/// This is a simple string (+ prefix), not a bulk string.
/// We format it here so handler.rs doesn't do any allocation on the
/// hot path — this function is only called when MONITOR_COUNT > 0.
pub fn format_monitor_line(
    cmd: &[Vec<u8>],
    db: usize,
    peer_addr: &str,
) -> Arc<str> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    // Redis uses seconds.microseconds format
    let secs = now.as_secs();
    let micros = now.subsec_micros();

    let mut line = format!("+{secs}.{micros:06} [{db} {peer_addr}]");

    for token in cmd {
        line.push(' ');
        line.push('"');
        // Escape backslashes and double-quotes inside tokens
        for &b in token {
            match b {
                b'\\' => line.push_str("\\\\"),
                b'"' => line.push_str("\\\""),
                b if b.is_ascii_graphic() || b == b' ' => line.push(b as char),
                _ => {
                    // Non-printable — hex escape
                    line.push_str(&format!("\\x{b:02x}"));
                }
            }
        }
        line.push('"');
    }
    line.push_str("\r\n");

    Arc::from(line.as_str())
}

/// Handle a connection in MONITOR mode (tokio path).
///
/// Sends +OK, then drains the channel and forwards lines to the client.
/// The only way to exit monitor mode is to disconnect — identical to Redis.
/// Inbound commands from the client while in monitor mode are ignored
/// (read and discarded) to avoid the socket buffer filling up.
pub async fn handle_monitor_mode(
    reader: &mut tokio::io::ReadHalf<tokio::net::TcpStream>,
    resp_tx: &tokio::sync::mpsc::Sender<Vec<u8>>,
    client_id: u64,
    registry: &SharedMonitorRegistry,
) -> Result<(), Box<dyn std::error::Error>> {
    // Send the initial +OK so redis-cli knows monitor mode is active.
    resp_tx.send(b"+OK\r\n".to_vec()).await?;

    let mut rx = registry.register(client_id);

    let mut inbound = [0u8; 256];

    loop {
        tokio::select! {
            // Forward monitor lines to the client as fast as they arrive.
            msg = rx.recv() => {
                match msg {
                    Some(line) => {
                        if resp_tx.send(line.as_bytes().to_vec()).await.is_err() {
                            break; // client writer task died
                        }
                    }
                    None => break, // channel closed
                }
            }
            // Drain inbound bytes so the socket buffer never fills.
            // We don't process them — monitor mode ignores input.
            result = reader.read(&mut inbound) => {
                match result {
                    Ok(0) | Err(_) => break, // client disconnected
                    Ok(_) => {} // discard
                }
            }
        }
    }

    registry.deregister(client_id);
    Ok(())
}