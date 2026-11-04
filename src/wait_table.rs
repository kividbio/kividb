use ahash::AHashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::oneshot;

// What a woken waiter receives. Nothing is delivered through it — a waiter
// looks at the key for itself once it is woken — so it is always empty; the
// type is what the connection loop's wait is written against.
pub(crate) type Value = Arc<[u8]>;

/// A blocked client's place in line, taken when its command first has to
/// wait and kept until it is answered. The client holds it; the table holds
/// only a weak reference, so a client that goes away — answered, timed out or
/// disconnected — leaves the line without anyone having to remember to take
/// it out.
pub struct Ticket {
    /// Lower is earlier.
    id: u64,
}

/// One client in the line for a key.
struct Queued {
    id: u64,
    alive: Weak<Ticket>,
    /// How to wake it. `None` once it has been woken and until it has looked
    /// at the key and either gone or waited again.
    tx: Option<oneshot::Sender<Value>>,
}

pub struct WaitTable {
    // Key → stream readers (`XREAD BLOCK`, `XREADGROUP BLOCK`) waiting on it,
    // by registration: a [`StreamWait`] takes its own out when it is dropped,
    // without looking through the others. They are all woken together, so
    // their order does not matter. See [`WaitTable::notify_stream_waiters`].
    inner: Mutex<AHashMap<String, AHashMap<u64, oneshot::Sender<Value>>>>,
    /// Key → clients waiting to be told the key was written: the blocking
    /// list and sorted-set commands and `TS.READ`. See
    /// [`WaitTable::register_signal`].
    signals: Mutex<AHashMap<String, Vec<oneshot::Sender<Value>>>>,
    /// Number of entries in `signals`, so a push can skip the lock entirely
    /// when nobody is waiting — which is nearly always.
    signal_count: AtomicUsize,
    /// Key → the clients blocked on it in a list or sorted-set command, in
    /// the order they blocked. See [`WaitTable::enqueue`].
    queues: Mutex<AHashMap<String, Vec<Queued>>>,
    /// Key → the clients blocked on it in `XREADGROUP`, in the order they
    /// blocked. A line of its own: these clients are waiting on a stream and
    /// those in `queues` on a list or a sorted set, and what one kind finds
    /// at the key says nothing about what the other will. Each line is woken
    /// from its head when the key is written.
    reader_queues: Mutex<AHashMap<String, Vec<Queued>>>,
    /// Number of entries in `queues` and `reader_queues`, for the same
    /// reason as `signal_count`.
    queued_count: AtomicUsize,
    next_ticket: AtomicU64,
}

impl WaitTable {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(AHashMap::new()),
            signals: Mutex::new(AHashMap::new()),
            signal_count: AtomicUsize::new(0),
            queues: Mutex::new(AHashMap::new()),
            reader_queues: Mutex::new(AHashMap::new()),
            queued_count: AtomicUsize::new(0),
            next_ticket: AtomicU64::new(1),
        }
    }

    /// Drop every blocked-client registration (used when replacing replica data from master).
    pub fn clear(&self) {
        self.inner.lock().clear();
        let mut signals = self.signals.lock();
        signals.clear();
        self.signal_count.store(0, Ordering::Relaxed);
        self.queues.lock().clear();
        self.reader_queues.lock().clear();
        self.queued_count.store(0, Ordering::Relaxed);
    }

    // ── The line for a key ────────────────────────────────────────────────
    //
    // Clients blocked on the same key are served in the order they blocked:
    // the one that has waited longest gets the first element. Nothing is
    // handed to them — each pops for itself (see
    // `commands::blocking::wait_for_keys`) — so the order has to be kept by
    // who is *woken*: one client at a time, from the front, each passing the
    // turn to the one behind it once it has looked. Waking them all would
    // leave the order to the scheduler.

    /// A place at the back of the line, for a command that is about to wait.
    pub fn new_ticket(&self) -> Arc<Ticket> {
        Arc::new(Ticket {
            id: self.next_ticket.fetch_add(1, Ordering::Relaxed),
        })
    }

    /// Stand in the line for `key` — or, already in it, stay where `ticket`
    /// says — and return the receiver this client is woken through.
    pub fn enqueue(&self, key: &str, ticket: &Arc<Ticket>) -> oneshot::Receiver<Value> {
        self.enqueue_in(&self.queues, key, ticket)
    }

    /// [`enqueue`](Self::enqueue), in the line of the key's `XREADGROUP`
    /// clients.
    pub fn enqueue_reader(&self, key: &str, ticket: &Arc<Ticket>) -> oneshot::Receiver<Value> {
        self.enqueue_in(&self.reader_queues, key, ticket)
    }

    fn enqueue_in(
        &self,
        lines: &Mutex<AHashMap<String, Vec<Queued>>>,
        key: &str,
        ticket: &Arc<Ticket>,
    ) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        let mut queues = lines.lock();
        let queue = queues.entry(key.to_string()).or_default();
        // The moment to forget clients that have gone: it keeps a key that is
        // waited on again and again from collecting them.
        let before = queue.len();
        queue.retain(|queued| queued.alive.strong_count() > 0);
        self.queued_count
            .fetch_sub(before - queue.len(), Ordering::Relaxed);
        match queue.binary_search_by_key(&ticket.id, |queued| queued.id) {
            Ok(at) => queue[at].tx = Some(tx),
            Err(at) => {
                queue.insert(
                    at,
                    Queued {
                        id: ticket.id,
                        alive: Arc::downgrade(ticket),
                        tx: Some(tx),
                    },
                );
                self.queued_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        rx
    }

    /// Leave the line for `key`.
    pub fn dequeue(&self, key: &str, ticket: &Ticket) {
        self.dequeue_in(&self.queues, key, ticket);
    }

    /// Leave the line of `key`'s `XREADGROUP` clients, and give the turn to
    /// the client behind: it may be reading for another group, for which
    /// there is something.
    pub fn reader_done(&self, key: &str, ticket: &Ticket) {
        self.dequeue_in(&self.reader_queues, key, ticket);
        self.wake_next_in(&self.reader_queues, key, ticket.id);
    }

    /// Stay in that line, and give the turn to the client behind.
    pub fn reader_pass_on(&self, key: &str, ticket: &Ticket) {
        self.wake_next_in(&self.reader_queues, key, ticket.id);
    }

    fn dequeue_in(&self, lines: &Mutex<AHashMap<String, Vec<Queued>>>, key: &str, ticket: &Ticket) {
        let mut queues = lines.lock();
        let Some(queue) = queues.get_mut(key) else {
            return;
        };
        if let Ok(at) = queue.binary_search_by_key(&ticket.id, |queued| queued.id) {
            queue.remove(at);
            self.queued_count.fetch_sub(1, Ordering::Relaxed);
        }
        if queue.is_empty() {
            queues.remove(key);
        }
    }

    /// Whether a client that blocked before `ticket` is still in the line
    /// for `key`.
    pub fn someone_ahead(&self, key: &str, ticket: &Ticket) -> bool {
        if self.queued_count.load(Ordering::Relaxed) == 0 {
            return false;
        }
        self.queues.lock().get(key).is_some_and(|queue| {
            queue
                .iter()
                .any(|queued| queued.id < ticket.id && queued.alive.strong_count() > 0)
        })
    }

    /// Make sure the client at the head of the line for `key` looks at it.
    pub fn wake_head(&self, key: &str) {
        self.wake_next(key, 0);
    }

    /// Give the turn to the client behind `ticket` in the line for `key`.
    /// Every client calls this once it has looked at the key, whether or not
    /// it found something: there may be more than it took, or something it
    /// could not use that the next one can.
    pub fn pass_on(&self, key: &str, ticket: &Ticket) {
        self.wake_next(key, ticket.id);
    }

    /// Wake the first client in the line for `key` whose ticket is after
    /// `after`. If that client has already been woken and has not looked yet,
    /// there is nothing to do: it will, and it will pass the turn on.
    fn wake_next(&self, key: &str, after: u64) {
        self.wake_next_in(&self.queues, key, after);
    }

    fn wake_next_in(&self, lines: &Mutex<AHashMap<String, Vec<Queued>>>, key: &str, after: u64) {
        if self.queued_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut queues = lines.lock();
        let Some(queue) = queues.get_mut(key) else {
            return;
        };
        let before = queue.len();
        queue.retain(|queued| queued.alive.strong_count() > 0);
        self.queued_count
            .fetch_sub(before - queue.len(), Ordering::Relaxed);
        if let Some(next) = queue.iter_mut().find(|queued| queued.id > after)
            && let Some(tx) = next.tx.take()
        {
            let _ = tx.send(Arc::from([].as_slice()));
        }
        if queue.is_empty() {
            queues.remove(key);
        }
    }

    /// Register to be woken when `key` is written.
    ///
    /// The waiter is told nothing more than that: it looks at the key itself,
    /// takes what its own rules say it should, and goes back to waiting if
    /// there was nothing for it — another client was quicker, or the list is
    /// still too short for `BLMOVEM … EXACTLY`.
    pub fn register_signal(&self, key: &str) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        let mut signals = self.signals.lock();
        signals.entry(key.to_string()).or_default().push(tx);
        self.signal_count.fetch_add(1, Ordering::Relaxed);
        rx
    }

    /// Whether anyone at all is waiting for a signal. One atomic load, so the
    /// write path can skip working out which keys it touched.
    #[inline]
    pub fn has_signal_waiters(&self) -> bool {
        self.signal_count.load(Ordering::Relaxed) != 0
            || self.queued_count.load(Ordering::Relaxed) != 0
    }

    /// Wake everyone registered with [`register_signal`](Self::register_signal)
    /// on `key`. Called after the key has been written.
    #[inline]
    pub fn notify_signal(&self, key: &str) {
        // The lines for the key: the client at the head of each.
        self.wake_next(key, 0);
        self.wake_next_in(&self.reader_queues, key, 0);
        // And everyone who asked only to be told.
        if self.signal_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let Some(waiters) = self.signals.lock().remove(key) else {
            return;
        };
        self.signal_count
            .fetch_sub(waiters.len(), Ordering::Relaxed);
        let sentinel: Value = Arc::from([].as_slice());
        for tx in waiters {
            let _ = tx.send(sentinel.clone());
        }
    }

    /// Wake everyone waiting to be told that a key was written: every key has
    /// been, in the only way a flush writes one.
    pub fn notify_all_signals(&self) {
        let keys: Vec<String> = {
            let signals = self.signals.lock();
            let queues = self.queues.lock();
            let readers = self.reader_queues.lock();
            signals
                .keys()
                .chain(queues.keys())
                .chain(readers.keys())
                .cloned()
                .collect()
        };
        for key in keys {
            self.notify_signal(&key);
        }
    }

    /// How many keys have a client blocked on them, and how many of those
    /// have one that is also waiting to hear of the key's deletion (a reader
    /// of a consumer group): `INFO`'s `total_blocking_keys` and
    /// `total_blocking_keys_on_nokey`.
    pub fn blocking_keys(&self) -> (usize, usize) {
        let mut keys: ahash::AHashSet<String> = ahash::AHashSet::new();
        for (key, waiters) in self.inner.lock().iter() {
            if waiters.values().any(|tx| !tx.is_closed()) {
                keys.insert(key.clone());
            }
        }
        for (key, waiters) in self.signals.lock().iter() {
            if waiters.iter().any(|tx| !tx.is_closed()) {
                keys.insert(key.clone());
            }
        }
        let alive =
            |queue: &Vec<Queued>| queue.iter().any(|queued| queued.alive.strong_count() > 0);
        for (key, queue) in self.queues.lock().iter() {
            if alive(queue) {
                keys.insert(key.clone());
            }
        }
        let mut on_nokey = 0;
        for (key, queue) in self.reader_queues.lock().iter() {
            if alive(queue) {
                keys.insert(key.clone());
                on_nokey += 1;
            }
        }
        (keys.len(), on_nokey)
    }

    /// Forget signal registrations on `key` whose client has stopped waiting
    /// (timed out or disconnected).
    pub fn prune_signals(&self, key: &str) {
        if self.signal_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut signals = self.signals.lock();
        if let Some(waiters) = signals.get_mut(key) {
            let before = waiters.len();
            waiters.retain(|tx| !tx.is_closed());
            self.signal_count
                .fetch_sub(before - waiters.len(), Ordering::Relaxed);
            if waiters.is_empty() {
                signals.remove(key);
            }
        }
    }

    /// A stream reader's registrations, all taken out when it is dropped.
    pub fn stream_wait(self: &Arc<Self>) -> StreamWait {
        StreamWait {
            table: self.clone(),
            registered: Vec::new(),
        }
    }

    /// Wake all waiters registered on `key` without delivering a value.
    /// Used by XADD to wake XREAD BLOCK clients — the client re-reads the
    /// stream itself after waking, so we just need the signal.
    /// Returns the number of waiters notified.
    pub fn notify_stream_waiters(&self, key: &str) -> usize {
        // We reuse the existing Value channel but send a zero-length sentinel.
        // The receiver treats any arrival as "data may be available, go check".
        let sentinel: Value = Arc::from([].as_slice());
        // Every waiter is woken (a broadcast: every XREAD BLOCK client on the
        // key deserves to look), and the key leaves the table. A sender whose
        // receiver is gone fails to send, and is not counted.
        let Some(waiters) = self.inner.lock().remove(key) else {
            return 0;
        };
        waiters
            .into_values()
            .map(|tx| tx.send(sentinel.clone()).is_ok())
            .filter(|sent| *sent)
            .count()
    }
}

/// A stream reader's registrations, removed from every key when dropped.
pub struct StreamWait {
    table: Arc<WaitTable>,
    /// Each registration's key and its own id: a reader may name a stream
    /// twice (`XREAD STREAMS s s 0 0`), and each is a waiter of its own.
    registered: Vec<(String, u64)>,
}

impl StreamWait {
    /// Register on `key` and return the receiver the reader is woken
    /// through. A tokio oneshot, so the caller does not block an OS thread.
    pub fn register(&mut self, key: &str) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        let id = self.table.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.table
            .inner
            .lock()
            .entry(key.to_string())
            .or_default()
            .insert(id, tx);
        self.registered.push((key.to_string(), id));
        rx
    }
}

impl Drop for StreamWait {
    fn drop(&mut self) {
        if self.registered.is_empty() {
            return;
        }
        let mut map = self.table.inner.lock();
        for (key, id) in &self.registered {
            // Gone already if an XADD woke the key's waiters.
            let Some(waiters) = map.get_mut(key) else {
                continue;
            };
            waiters.remove(id);
            if waiters.is_empty() {
                map.remove(key);
            }
        }
    }
}

impl Default for WaitTable {
    fn default() -> Self {
        Self::new()
    }
}
// Share safely across threads
unsafe impl Send for WaitTable {}
unsafe impl Sync for WaitTable {}

#[cfg(test)]
mod signal_tests {
    use super::*;

    #[test]
    fn a_signal_wakes_every_waiter_on_the_key_and_no_one_else() {
        let table = WaitTable::new();
        let mut a1 = table.register_signal("a");
        let mut a2 = table.register_signal("a");
        let mut b = table.register_signal("b");

        table.notify_signal("a");
        assert!(a1.try_recv().is_ok());
        assert!(a2.try_recv().is_ok());
        assert!(b.try_recv().is_err());
        assert_eq!(table.signal_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn pruning_forgets_waiters_that_gave_up() {
        let table = WaitTable::new();
        let gone = table.register_signal("a");
        let _still_here = table.register_signal("a");
        drop(gone);
        table.prune_signals("a");
        assert_eq!(table.signal_count.load(Ordering::Relaxed), 1);
        table.clear();
        assert_eq!(table.signal_count.load(Ordering::Relaxed), 0);
    }

    /// One client at a time, from the front; a client that has been woken
    /// keeps its place until it has looked; a client that has gone is skipped.
    #[test]
    fn the_line_is_served_from_the_front_one_at_a_time() {
        let table = WaitTable::new();
        let (first, second, third) = (table.new_ticket(), table.new_ticket(), table.new_ticket());
        // Arriving out of order does not change the order.
        let mut rx2 = table.enqueue("k", &second);
        let mut rx1 = table.enqueue("k", &first);
        let mut rx3 = table.enqueue("k", &third);
        assert!(table.has_signal_waiters());

        table.notify_signal("k");
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_err());
        // A second write while the first client has not looked yet wakes no
        // one else: the turn is still the first client's.
        table.notify_signal("k");
        assert!(rx2.try_recv().is_err());

        // The first client took what it wanted and passes the turn on.
        table.dequeue("k", &first);
        table.pass_on("k", &first);
        assert!(rx2.try_recv().is_ok());
        assert!(rx3.try_recv().is_err());

        // The second found nothing and waits again, in the same place…
        let mut rx2 = table.enqueue("k", &second);
        table.pass_on("k", &second);
        assert!(rx3.try_recv().is_ok());
        // …so the next write is its turn again, not the third client's.
        let mut rx3 = table.enqueue("k", &third);
        table.notify_signal("k");
        assert!(rx2.try_recv().is_ok());
        assert!(rx3.try_recv().is_err());

        // A client that disconnects is not waited for.
        drop(second);
        table.notify_signal("k");
        assert!(rx3.try_recv().is_ok());
        drop(third);
        table.notify_signal("k");
        assert!(!table.has_signal_waiters());
    }

    /// Dropping a reader's wait unregisters it from every key, even with its
    /// receivers still alive (racers aborted but not yet dropped).
    #[test]
    fn a_stream_reader_unregisters_from_every_key() {
        let table = Arc::new(WaitTable::new());
        let mut wait = table.stream_wait();
        let rxs: Vec<_> = (0..1000).map(|i| wait.register(&format!("s{i}"))).collect();
        drop(wait);
        assert!(table.inner.lock().is_empty());
        drop(rxs);
    }

    /// A reader may name a stream twice (`XREAD STREAMS s s 0 0`): each is
    /// woken, and dropping the reader takes both out.
    #[test]
    fn a_stream_named_twice_is_two_waiters() {
        let table = Arc::new(WaitTable::new());
        let mut wait = table.stream_wait();
        let mut first = wait.register("s");
        let mut second = wait.register("s");
        assert_eq!(table.notify_stream_waiters("s"), 2);
        assert!(first.try_recv().is_ok());
        assert!(second.try_recv().is_ok());
        let _again = wait.register("s");
        let _again_too = wait.register("s");
        drop(wait);
        assert!(table.inner.lock().is_empty());
    }

    #[test]
    fn unregistering_one_reader_leaves_the_others_waiting() {
        let table = Arc::new(WaitTable::new());
        let (mut first, mut second) = (table.stream_wait(), table.stream_wait());
        let mut first_a = first.register("a");
        let mut first_b = first.register("b");
        let mut second_a = second.register("a");

        drop(first);
        assert_eq!(table.notify_stream_waiters("a"), 1);
        assert!(second_a.try_recv().is_ok());
        assert!(first_a.try_recv().is_err());
        assert!(first_b.try_recv().is_err());
        drop(second);
        assert!(table.inner.lock().is_empty());
    }

    #[test]
    fn has_signal_waiters_follows_registrations() {
        let table = WaitTable::new();
        assert!(!table.has_signal_waiters());
        let _rx = table.register_signal("a");
        assert!(table.has_signal_waiters());
        table.notify_signal("a");
        assert!(!table.has_signal_waiters());
    }
}
