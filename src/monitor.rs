//! `MONITOR`: every command run, shown to the clients that asked.
//!
//! What a monitor is shown, it is shown all of, in order:
//!
//! - **Nothing is dropped.** Each monitor has a queue of its own, without a
//!   bound of its own: like any client's output in Redis, it is held to
//!   `client-output-buffer-limit normal` (the class Redis counts a monitor
//!   in), and a monitor that falls that far behind is disconnected, with a
//!   line in the log, as Redis disconnects it. With the default `normal 0 0
//!   0` it is never disconnected, and what it has not read waits for it, as
//!   in Redis. (A monitor used to be fed through a broadcast ring of 4096
//!   lines; one that fell further behind lost lines without a word.)
//! - **One order for everything.** A line is stamped and queued to every
//!   monitor under one lock, so all monitors see the same order, its time
//!   stamps never go back, and a connection's commands are in the order it
//!   sent them. A command is shown when it is called, before it runs (see
//!   `run_command`): two connections' commands on one key, called at
//!   nearly the same moment on two threads, can run in the other order
//!   than they are shown. Redis, with one thread, has no such moment.
//!
//! Cost: with no monitor, one relaxed load per command (`MONITOR_COUNT`),
//! as before. With monitors, a command's line is formatted once, then the
//! lock is taken for the stamp and one queue push a monitor.

use crate::client_registry::ClientRegistry;
use crate::output_limits::{Class, OutputLimits};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::mpsc;

/// Number of currently connected MONITOR clients.
pub static MONITOR_COUNT: AtomicUsize = AtomicUsize::new(0);

/// How much the forwarder takes from the queue for one write.
const BATCH_BYTES: usize = 256 << 10;
/// What the forwarder keeps of its buffer once the queue is empty.
const KEEP_BUFFER: usize = 16 << 10;
/// The room in front of a line for its time stamp: `+`, ten digits of
/// seconds, `.` and six of microseconds. (Ten digits from 2001 to 2286; a
/// clock outside that is given a line made again, `restamp`.)
const STAMP: usize = 18;

/// A monitor, as the commands' side sees it.
struct Sink {
    client_id: u64,
    registry: ClientRegistry,
    limits: Arc<OutputLimits>,
    queue: mpsc::UnboundedSender<Arc<[u8]>>,
    /// Bytes queued and not yet taken by the forwarder.
    pending: AtomicUsize,
    /// When `pending` went over the soft limit; 0 while it is under.
    over_soft_since: AtomicU64,
    /// Set once it has been disconnected for falling behind.
    closed: AtomicBool,
}

impl Sink {
    /// Queue `line`. `false`: it took the monitor over its limit, and the
    /// monitor is to be disconnected.
    fn offer(&self, line: &Arc<[u8]>) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return true;
        }
        let pending = self.pending.fetch_add(line.len(), Ordering::Relaxed) + line.len();
        let limit = self.limits.get(Class::Normal);
        let over_soft = limit.soft != 0 && pending as u64 >= limit.soft;
        if (limit.hard != 0 && pending as u64 >= limit.hard)
            || (over_soft && self.over_soft_limit_too_long(limit.soft_seconds))
        {
            self.closed.store(true, Ordering::Relaxed);
            return false;
        }
        if !over_soft && self.over_soft_since.load(Ordering::Relaxed) != 0 {
            self.over_soft_since.store(0, Ordering::Relaxed);
        }
        // Fails only once the forwarder is gone, with the connection.
        let _ = self.queue.send(Arc::clone(line));
        true
    }

    fn over_soft_limit_too_long(&self, seconds: u64) -> bool {
        let now = crate::output_limits::now_secs();
        match self
            .over_soft_since
            .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => false,
            Err(since) => now.saturating_sub(since) > seconds,
        }
    }

    /// Close the connection of a monitor that fell too far behind.
    #[cold]
    fn disconnect(&self) {
        crate::stats::OUTPUT_BUFFER_LIMIT_DISCONNECTIONS.fetch_add(1, Ordering::Relaxed);
        log_warn!(
            "Client id={} closed: it is in MONITOR and {} bytes behind, over \
             client-output-buffer-limit normal",
            self.client_id,
            self.pending.load(Ordering::Relaxed)
        );
        self.registry.kill(self.client_id);
    }
}

/// The monitors of a server.
#[derive(Default)]
pub struct MonitorRegistry {
    sinks: Mutex<Vec<Arc<Sink>>>,
}

impl MonitorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn add(&self, sink: Arc<Sink>) {
        let mut sinks = self.sinks.lock();
        sinks.push(sink);
        MONITOR_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn remove(&self, sink: &Arc<Sink>) {
        let mut sinks = self.sinks.lock();
        if let Some(at) = sinks.iter().position(|s| Arc::ptr_eq(s, sink)) {
            sinks.swap_remove(at);
            MONITOR_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Stamp `line` (which has [`STAMP`] bytes of room in front) and queue
    /// it to every monitor, under the lock: see the module's documentation.
    fn broadcast(&self, line: Vec<u8>) {
        let mut line: Arc<[u8]> = Arc::from(line);
        let mut behind: Vec<Arc<Sink>> = Vec::new();
        {
            let sinks = self.sinks.lock();
            if sinks.is_empty() {
                return;
            }
            stamp(&mut line);
            for sink in sinks.iter() {
                if !sink.offer(&line) {
                    behind.push(Arc::clone(sink));
                }
            }
        }
        for sink in behind {
            sink.disconnect();
        }
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
    feed_monitors(registry, cmd, db, source);
}

#[cold]
#[inline(never)]
fn feed_monitors(registry: &SharedMonitorRegistry, cmd: &[&[u8]], db: usize, source: &str) {
    // Passwords and secret settings are not shown (`slowlog::redact`).
    let line = match crate::slowlog::redact(cmd) {
        Some(redacted) => unstamped_line(&redacted, db, source),
        None => unstamped_line(cmd, db, source),
    };
    registry.broadcast(line);
}

/// A monitor line but for its time stamp, which [`STAMP`] bytes in front
/// are left for: ` [<db> <addr>] "CMD" "arg"\r\n`, each argument quoted as
/// Redis quotes it (`sdscatrepr`).
fn unstamped_line<A: AsRef<[u8]>>(cmd: &[A], db: usize, source: &str) -> Vec<u8> {
    let args: usize = cmd.iter().map(|a| a.as_ref().len() + 3).sum();
    let mut line = Vec::with_capacity(STAMP + 24 + source.len() + args + 2);
    line.resize(STAMP, b'0');
    line.extend_from_slice(b" [");
    crate::resp::write::push_decimal(&mut line, db as u64);
    line.push(b' ');
    line.extend_from_slice(source.as_bytes());
    line.push(b']');
    for token in cmd {
        line.extend_from_slice(b" \"");
        for &b in token.as_ref() {
            match b {
                b'\\' => line.extend_from_slice(b"\\\\"),
                b'"' => line.extend_from_slice(b"\\\""),
                b'\n' => line.extend_from_slice(b"\\n"),
                b'\r' => line.extend_from_slice(b"\\r"),
                b'\t' => line.extend_from_slice(b"\\t"),
                0x07 => line.extend_from_slice(b"\\a"),
                0x08 => line.extend_from_slice(b"\\b"),
                b if b.is_ascii_graphic() || b == b' ' => line.push(b),
                _ => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    line.extend_from_slice(&[
                        b'\\',
                        b'x',
                        HEX[(b >> 4) as usize],
                        HEX[(b & 15) as usize],
                    ]);
                }
            }
        }
        line.push(b'"');
    }
    line.extend_from_slice(b"\r\n");
    line
}

/// Write the time now into the first [`STAMP`] bytes of `line`, which is
/// held by nobody else yet: `+<secs>.<micros>`.
fn stamp(line: &mut Arc<[u8]>) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let (secs, micros) = (now.as_secs(), now.subsec_micros());
    match Arc::get_mut(line) {
        Some(bytes) if (1_000_000_000..10_000_000_000).contains(&secs) => {
            let head = &mut bytes[..STAMP];
            head[0] = b'+';
            let mut s = secs;
            for at in (1..11).rev() {
                head[at] = b'0' + (s % 10) as u8;
                s /= 10;
            }
            head[11] = b'.';
            let mut m = micros;
            for at in (12..18).rev() {
                head[at] = b'0' + (m % 10) as u8;
                m /= 10;
            }
        }
        _ => restamp(line, secs, micros),
    }
}

#[cold]
fn restamp(line: &mut Arc<[u8]>, secs: u64, micros: u32) {
    let mut fresh = format!("+{secs}.{micros:06}").into_bytes();
    fresh.extend_from_slice(&line[STAMP..]);
    *line = Arc::from(fresh);
}

/// A whole monitor line as it is sent, for a time `secs.micros`: the format
/// Redis's `replicationFeedMonitors` writes.
pub fn format_monitor_line<A: AsRef<[u8]>>(
    cmd: &[A],
    db: usize,
    peer_addr: &str,
    secs: u64,
    micros: u32,
) -> Vec<u8> {
    let mut line = format!("+{secs}.{micros:06}").into_bytes();
    line.extend_from_slice(&unstamped_line(cmd, db, peer_addr)[STAMP..]);
    line
}

/// A connection in `MONITOR` mode.
///
/// The connection stays what it was — it can go on sending commands, as it
/// can in Redis — and what the monitors are shown is written to it by a
/// task of the connection's loop, from the [`Outbox`]. Dropping this ends
/// the mode: on `RESET`, or with the connection.
pub struct Monitoring {
    sink: Arc<Sink>,
    registry: SharedMonitorRegistry,
    forwarder: Option<tokio::task::JoinHandle<()>>,
}

/// A connection made a monitor, and the queue of what it is to be shown.
/// The caller starts a task that writes what [`Outbox::next`] yields, and
/// gives it to [`Monitoring::forwarded_by`].
pub fn connect(
    registry: &SharedMonitorRegistry,
    client_registry: ClientRegistry,
    client_id: u64,
    limits: Arc<OutputLimits>,
) -> (Monitoring, Outbox) {
    let (queue, rx) = mpsc::unbounded_channel();
    let sink = Arc::new(Sink {
        client_id,
        registry: client_registry,
        limits,
        queue,
        pending: AtomicUsize::new(0),
        over_soft_since: AtomicU64::new(0),
        closed: AtomicBool::new(false),
    });
    registry.add(Arc::clone(&sink));
    (
        Monitoring {
            sink: Arc::clone(&sink),
            registry: Arc::clone(registry),
            forwarder: None,
        },
        Outbox {
            rx,
            me: sink,
            buf: Vec::new(),
        },
    )
}

impl Monitoring {
    /// The task writing what this monitor is shown; stopped with it.
    pub fn forwarded_by(&mut self, task: tokio::task::JoinHandle<()>) {
        self.forwarder = Some(task);
    }
}

impl Drop for Monitoring {
    fn drop(&mut self) {
        if let Some(task) = self.forwarder.take() {
            task.abort();
        }
        self.registry.remove(&self.sink);
    }
}

/// The lines queued for a monitor, for its forwarder task.
pub struct Outbox {
    rx: mpsc::UnboundedReceiver<Arc<[u8]>>,
    me: Arc<Sink>,
    buf: Vec<u8>,
}

impl Outbox {
    /// Waits for lines and returns the next of them, together, to be
    /// written. Give the buffer back with [`Outbox::written`].
    pub async fn next(&mut self) -> Option<Vec<u8>> {
        let first = match self.rx.try_recv() {
            Ok(line) => line,
            Err(_) => {
                if self.buf.capacity() > KEEP_BUFFER {
                    self.buf = Vec::new();
                }
                self.rx.recv().await?
            }
        };
        // Let this thread's other ready connections run first: what they
        // are shown meanwhile goes out in the same write.
        tokio::task::yield_now().await;
        let mut out = std::mem::take(&mut self.buf);
        out.clear();
        out.extend_from_slice(&first);
        while out.len() < BATCH_BYTES {
            match self.rx.try_recv() {
                Ok(line) => out.extend_from_slice(&line),
                Err(_) => break,
            }
        }
        let pending = self.me.pending.fetch_sub(out.len(), Ordering::Relaxed) - out.len();
        let soft = self.me.limits.get(Class::Normal).soft;
        if soft == 0 || (pending as u64) < soft {
            self.me.over_soft_since.store(0, Ordering::Relaxed);
        }
        Some(out)
    }

    /// The buffer [`Outbox::next`] gave, written: kept for the next batch.
    pub fn written(&mut self, buf: Vec<u8>) {
        self.buf = buf;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_is_quoted_as_redis_quotes_it() {
        let line = format_monitor_line(
            &[&b"SET"[..], b"k\"\\", b"a\nb\r\t\x07\x08\x01 \xff"],
            3,
            "127.0.0.1:5000",
            1_700_000_000,
            42,
        );
        assert_eq!(
            String::from_utf8(line).unwrap(),
            "+1700000000.000042 [3 127.0.0.1:5000] \"SET\" \"k\\\"\\\\\" \
             \"a\\nb\\r\\t\\a\\b\\x01 \\xff\"\r\n"
        );
    }

    #[test]
    fn the_stamp_is_written_in_place() {
        let mut line: Arc<[u8]> = Arc::from(unstamped_line(&[b"PING"], 0, "lua"));
        stamp(&mut line);
        let text = std::str::from_utf8(&line).unwrap();
        assert!(text.starts_with('+'), "{text}");
        assert_eq!(&text[11..12], ".", "{text}");
        assert!(text[1..11].bytes().all(|b| b.is_ascii_digit()), "{text}");
        assert!(text[12..18].bytes().all(|b| b.is_ascii_digit()), "{text}");
        assert!(text.ends_with(" [0 lua] \"PING\"\r\n"), "{text}");
    }
}
