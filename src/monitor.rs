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
//!   in Redis. What is queued shows in its `omem` in `CLIENT LIST`. (A
//!   monitor used to be fed through a broadcast ring of 4096 lines; one that
//!   fell further behind lost lines without a word.)
//! - **One order for everything.** A line is stamped and queued to every
//!   monitor under one lock, so all monitors see the same order, its time
//!   stamps are in the order of the lines (read from the system clock, they
//!   go back only when the clock is set back, as in Redis), and a
//!   connection's commands are in the order it sent them. A command is shown
//!   when it is called, before it runs (see `run_command`); Redis shows it
//!   once it has run. Two connections' commands on one key, called at
//!   nearly the same moment on two threads, can run in the other order than
//!   they are shown. Redis, with one thread, has no such moment.
//!
//! Cost: with no monitor, one relaxed load per command (`MONITOR_COUNT`),
//! as before. With monitors, a command's line is formatted once, then the
//! lock is taken for the stamp and a push onto each monitor's queue, and
//! nothing else: the monitors' writers are woken after it is let go, and
//! only those that had nothing queued.

use crate::client_registry::{Activity, ClientRegistry};
use crate::output_limits::{Class, OutputLimits};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::Notify;

/// Number of currently connected MONITOR clients.
pub static MONITOR_COUNT: AtomicUsize = AtomicUsize::new(0);

/// How much the forwarder takes from the queue for one write.
const BATCH_BYTES: usize = 256 << 10;
/// What the forwarder keeps of its buffer once the queue is empty.
const KEEP_BUFFER: usize = 16 << 10;
/// The lines' room the forwarder keeps once the queue is empty.
const KEEP_LINES: usize = 1024;
/// The room in front of a line for its time stamp: `+`, ten digits of
/// seconds, `.` and six of microseconds. (Ten digits from 2001 to 2286; a
/// clock outside that is given a line made again, `restamp`.)
const STAMP: usize = 18;

/// A monitor, as the commands' side sees it.
struct Sink {
    client_id: u64,
    registry: ClientRegistry,
    limits: Arc<OutputLimits>,
    /// The connection's, where `CLIENT LIST` reads `omem` from.
    activity: Option<Arc<Activity>>,
    /// The lines not yet taken by the forwarder: pushed under the
    /// registry's lock, so in one order for every monitor; taken by the
    /// forwarder all at once, under this lock alone.
    queue: Mutex<VecDeque<Arc<[u8]>>>,
    /// Set when a line went onto an empty queue: its forwarder is to be
    /// woken, once the registry's lock is let go.
    wake: AtomicBool,
    /// Wakes the forwarder.
    ready: Notify,
    /// Bytes queued and not yet taken by the forwarder.
    pending: AtomicUsize,
    /// When `pending` went over the soft limit; 0 while it is under.
    over_soft_since: AtomicU64,
    /// Set once it is over its limit: nothing more is queued.
    closed: AtomicBool,
    /// Set once it has been disconnected for it, exactly once.
    disconnected: AtomicBool,
}

impl Sink {
    /// Queue `line`; under the registry's lock. A line that takes the
    /// monitor over its limit closes it instead (see `after_offer`).
    fn offer(&self, line: &Arc<[u8]>) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        let pending = self.pending.fetch_add(line.len(), Ordering::Relaxed) + line.len();
        self.note_pending(pending);
        let limit = self.limits.get(Class::Normal);
        let over_soft = limit.soft != 0 && pending as u64 >= limit.soft;
        if (limit.hard != 0 && pending as u64 >= limit.hard)
            || (over_soft && self.over_soft_limit_too_long(limit.soft_seconds))
        {
            self.closed.store(true, Ordering::Relaxed);
            return;
        }
        if !over_soft && self.over_soft_since.load(Ordering::Relaxed) != 0 {
            self.over_soft_since.store(0, Ordering::Relaxed);
        }
        let mut queue = self.queue.lock();
        if queue.is_empty() {
            self.wake.store(true, Ordering::Relaxed);
        }
        queue.push_back(Arc::clone(line));
    }

    /// After the registry's lock: wake the forwarder if a line went onto
    /// its empty queue, and disconnect the monitor if a line took it over
    /// its limit.
    #[inline]
    fn after_offer(&self) {
        if self.wake.load(Ordering::Relaxed) && self.wake.swap(false, Ordering::Relaxed) {
            self.ready.notify_one();
        }
        if self.closed.load(Ordering::Relaxed) && !self.disconnected.swap(true, Ordering::Relaxed) {
            self.disconnect();
        }
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

    /// What `CLIENT LIST` shows as the connection's `omem`.
    #[inline]
    fn note_pending(&self, pending: usize) {
        if let Some(activity) = &self.activity {
            activity.set_monitor_output(pending);
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

/// The monitors of a server: the list is replaced, not changed, when one
/// comes or goes, so that a line's sender holds it past the lock to wake
/// the monitors' writers.
#[derive(Default)]
pub struct MonitorRegistry {
    sinks: Mutex<Arc<Vec<Arc<Sink>>>>,
}

impl MonitorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn add(&self, sink: Arc<Sink>) {
        let mut sinks = self.sinks.lock();
        let mut next = Vec::clone(&sinks);
        next.push(sink);
        *sinks = Arc::new(next);
        MONITOR_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn remove(&self, sink: &Arc<Sink>) {
        let mut sinks = self.sinks.lock();
        if let Some(at) = sinks.iter().position(|s| Arc::ptr_eq(s, sink)) {
            let mut next = Vec::clone(&sinks);
            next.swap_remove(at);
            *sinks = Arc::new(next);
            MONITOR_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Stamp `line` (which has [`STAMP`] bytes of room in front) and queue
    /// it to every monitor, under the lock: see the module's documentation.
    /// The writers are woken, and a monitor over its limit disconnected,
    /// once the lock is let go.
    fn broadcast(&self, line: Vec<u8>) {
        let mut line: Arc<[u8]> = Arc::from(line);
        let sinks = {
            let sinks = self.sinks.lock();
            if sinks.is_empty() {
                return;
            }
            stamp(&mut line);
            for sink in sinks.iter() {
                sink.offer(&line);
            }
            Arc::clone(&sinks)
        };
        for sink in sinks.iter() {
            sink.after_offer();
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
/// The connection stays what it was â€” it can go on sending commands, as it
/// can in Redis â€” and what the monitors are shown is written to it by a
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
    let activity = client_registry.activity(client_id);
    let sink = Arc::new(Sink {
        client_id,
        registry: client_registry,
        limits,
        activity,
        queue: Mutex::new(VecDeque::new()),
        wake: AtomicBool::new(false),
        ready: Notify::new(),
        pending: AtomicUsize::new(0),
        over_soft_since: AtomicU64::new(0),
        closed: AtomicBool::new(false),
        disconnected: AtomicBool::new(false),
    });
    registry.add(Arc::clone(&sink));
    (
        Monitoring {
            sink: Arc::clone(&sink),
            registry: Arc::clone(registry),
            forwarder: None,
        },
        Outbox {
            me: sink,
            taken: VecDeque::new(),
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
        self.sink.note_pending(0);
    }
}

/// The lines queued for a monitor, for its forwarder task.
pub struct Outbox {
    me: Arc<Sink>,
    /// Lines taken from the queue and not yet written.
    taken: VecDeque<Arc<[u8]>>,
    buf: Vec<u8>,
}

impl Outbox {
    /// What is in the queue, after what was taken before.
    fn take(&mut self) {
        let mut queue = self.me.queue.lock();
        if self.taken.is_empty() {
            std::mem::swap(&mut *queue, &mut self.taken);
        } else {
            self.taken.extend(queue.drain(..));
        }
    }

    /// Waits for lines and returns the next of them, together, to be
    /// written. Give the buffer back with [`Outbox::written`].
    pub async fn next(&mut self) -> Option<Vec<u8>> {
        if self.taken.is_empty() {
            self.take();
        }
        while self.taken.is_empty() {
            if self.buf.capacity() > KEEP_BUFFER {
                self.buf = Vec::new();
            }
            if self.taken.capacity() > KEEP_LINES {
                self.taken = VecDeque::new();
                let mut queue = self.me.queue.lock();
                if queue.is_empty() && queue.capacity() > KEEP_LINES {
                    *queue = VecDeque::new();
                }
            }
            // A line queued after the queue was seen empty has set `wake`,
            // and is notified once its sender lets the registry go: kept
            // as a permit if this has not begun to wait yet.
            self.me.ready.notified().await;
            self.take();
        }
        // Let this thread's other ready connections run first: what they
        // are shown meanwhile goes out in the same write.
        tokio::task::yield_now().await;
        self.take();
        let mut out = std::mem::take(&mut self.buf);
        out.clear();
        while let Some(line) = self.taken.front() {
            if !out.is_empty() && out.len() + line.len() > BATCH_BYTES {
                break;
            }
            out.extend_from_slice(line);
            self.taken.pop_front();
        }
        let pending = self.me.pending.fetch_sub(out.len(), Ordering::Relaxed) - out.len();
        self.me.note_pending(pending);
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
