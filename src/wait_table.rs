use crate::store::Key;
use ahash::AHashMap;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::oneshot;

// What a woken waiter receives. A client blocked in a line (see
// [`WaitTable::enqueue_all`]) is sent the key that woke it; nothing else is
// delivered through it — a waiter looks at the key for itself once it is
// woken — and the other waiters are sent nothing.
pub(crate) type Value = Arc<[u8]>;

/// A blocked client's place in line, taken when its command first has to
/// wait and kept until it is answered. The client holds it; the table holds
/// only its [`Bell`], which says whether the ticket is still held, so a
/// client that goes away — answered, timed out or disconnected — leaves the
/// line without anyone having to remember to take it out.
///
/// And without losing the turn: a client that was woken and is dropped
/// before it has looked at the key — its connection closed, or its command
/// refused before its handler ran (a pause it went in, `READONLY` after a
/// failover, `OOM`) — passes the turn to the client behind it when the ticket
/// goes (see the `Drop`). Otherwise the element it was woken for would wait
/// there for the next write, with clients blocked on the key (#384).
pub struct Ticket {
    /// Lower is earlier.
    id: u64,
    /// The lines it waits in, to leave when it is dropped: recorded when its
    /// command parks (see [`Ticket::parks_in`]), so that one answered at
    /// once costs nothing for this, and forgotten when it has left them
    /// ([`WaitTable::leave_all`]), so that one answered after a wait does
    /// not look for itself in every line again.
    lines: Mutex<Vec<(Line, Key)>>,
    table: Weak<WaitTable>,
    /// How it is woken, from whichever line it is woken in.
    bell: Arc<Bell>,
}

/// How a client standing in lines for several keys is woken: one for all of
/// them (#758). A client blocked on 270 keys used to be given a receiver for
/// each, raced by a task of its own with a timer of its own — 270 tasks and
/// timers for each wait, and as many again every time it was woken and
/// waited again. It now waits on one receiver, with one timer, whichever key
/// wakes it; the key that did is what it is sent.
struct Bell {
    /// The sender of the wait under way, taken by the first key to wake it.
    /// A fresh one for each wait ([`Ticket::arm`]).
    tx: Mutex<Option<oneshot::Sender<Value>>>,
    /// Cleared when the ticket is dropped: its client is no longer waiting,
    /// and the lines skip it.
    alive: AtomicBool,
}

impl Bell {
    #[inline]
    fn alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Wake the client for `key`, unless another key has woken it already
    /// in this wait (it will look at all its keys anyway).
    fn ring(&self, key: &[u8]) {
        if let Some(tx) = self.tx.lock().take() {
            let _ = tx.send(Arc::from(key));
        }
    }
}

/// Which of a key's two lines (see [`WaitTable`]).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Line {
    Keys,
    Readers,
}

impl Ticket {
    /// Its command parks, waiting on `keys` (`readers`: in their
    /// `XREADGROUP` lines): what the `Drop` is to leave. Once: a command
    /// that parks again waits on the same keys.
    pub fn parks_in(&self, keys: &[impl AsRef<[u8]>], readers: bool) {
        let line = if readers { Line::Readers } else { Line::Keys };
        let mut lines = self.lines.lock();
        if lines.is_empty() {
            lines.extend(keys.iter().map(|key| (line, Key::new(key.as_ref()))));
        }
    }

    /// A fresh receiver for the wait about to start: the one a key that is
    /// written wakes. The receiver of the last wait, if there was one, is
    /// woken no more.
    fn arm(&self) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        *self.bell.tx.lock() = Some(tx);
        rx
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.bell.alive.store(false, Ordering::Release);
        // Taken without a lock: nothing else can reach a ticket being dropped.
        let lines = std::mem::take(self.lines.get_mut());
        if lines.is_empty() {
            return;
        }
        let Some(table) = self.table.upgrade() else {
            return;
        };
        table.leave_dropped(&lines, self.id);
    }
}

/// One client in the line for a key.
struct Queued {
    bell: Arc<Bell>,
    /// Woken, and it has not yet looked at the key and either gone or
    /// waited again: the turn is with it.
    woken: bool,
}

/// Key → the clients in its line, by ticket: the order they blocked in.
/// Standing in line, leaving it and finding its head each take a few steps
/// whatever the length of the line, where a sorted `Vec` took a pass over
/// it (#758: a thousand clients blocked on the same 270 keys made every
/// wait, wake-up and timeout walk 270 lines of a thousand).
type Lines = AHashMap<Key, BTreeMap<u64, Queued>>;

/// One kind of line, for every key, and how many clients stand in it.
struct LineTable {
    lines: Mutex<Lines>,
    /// Number of entries in `lines`, so a write can skip the lock entirely
    /// when nobody is waiting — which is nearly always.
    count: AtomicUsize,
}

impl LineTable {
    fn new() -> Self {
        Self {
            lines: Mutex::new(AHashMap::new()),
            count: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.count.load(Ordering::Relaxed) == 0
    }

    fn clear(&self) {
        self.lines.lock().clear();
        self.count.store(0, Ordering::Relaxed);
    }
}

pub struct WaitTable {
    // Key → stream readers (`XREAD BLOCK`, `XREADGROUP BLOCK`) waiting on it,
    // by registration: a [`StreamWait`] takes its own out when it is dropped,
    // without looking through the others. They are all woken together, so
    // their order does not matter. See [`WaitTable::notify_stream_waiters`].
    inner: Mutex<AHashMap<Key, AHashMap<u64, oneshot::Sender<Value>>>>,
    /// Key → clients waiting to be told the key was written: `TS.READ`. See
    /// [`WaitTable::register_signal`].
    signals: Mutex<AHashMap<Key, Vec<oneshot::Sender<Value>>>>,
    /// Number of entries in `signals`, so a push can skip the lock entirely
    /// when nobody is waiting — which is nearly always.
    signal_count: AtomicUsize,
    /// Key → the clients blocked on it in a list or sorted-set command, in
    /// the order they blocked. See [`WaitTable::enqueue_all`].
    queues: LineTable,
    /// Key → the clients blocked on it in `XREADGROUP`, in the order they
    /// blocked. A line of its own: these clients are waiting on a stream and
    /// those in `queues` on a list or a sorted set, and what one kind finds
    /// at the key says nothing about what the other will. Each line is woken
    /// from its head when the key is written.
    reader_queues: LineTable,
    next_ticket: AtomicU64,
}

impl WaitTable {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(AHashMap::new()),
            signals: Mutex::new(AHashMap::new()),
            signal_count: AtomicUsize::new(0),
            queues: LineTable::new(),
            reader_queues: LineTable::new(),
            next_ticket: AtomicU64::new(1),
        }
    }

    /// Drop every blocked-client registration (used when replacing replica data from master).
    pub fn clear(&self) {
        self.inner.lock().clear();
        let mut signals = self.signals.lock();
        signals.clear();
        self.signal_count.store(0, Ordering::Relaxed);
        self.queues.clear();
        self.reader_queues.clear();
    }

    fn table(&self, line: Line) -> &LineTable {
        match line {
            Line::Keys => &self.queues,
            Line::Readers => &self.reader_queues,
        }
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
    pub fn new_ticket(self: &Arc<Self>) -> Arc<Ticket> {
        Arc::new(Ticket {
            id: self.next_ticket.fetch_add(1, Ordering::Relaxed),
            lines: Mutex::new(Vec::new()),
            table: Arc::downgrade(self),
            bell: Arc::new(Bell {
                tx: Mutex::new(None),
                alive: AtomicBool::new(true),
            }),
        })
    }

    /// A ticket dropped while it stood in lines: out of them, and, where it
    /// had been woken and had not looked, the turn to the client behind it.
    /// Nothing to do for one that left the lines before it went, as a
    /// command that is answered does.
    fn leave_dropped(&self, lines: &[(Line, Key)], id: u64) {
        for line in [Line::Keys, Line::Readers] {
            let table = self.table(line);
            if table.is_empty() {
                continue;
            }
            let keys = lines
                .iter()
                .filter(|(of, _)| *of == line)
                .map(|(_, key)| key.as_bytes());
            let woken = self.leave_in(table, keys, id);
            for (key, woken) in lines.iter().filter(|(of, _)| *of == line).zip(woken) {
                if woken {
                    self.wake_next_in(table, key.1.as_bytes(), id);
                }
            }
        }
    }

    /// Stand in the line for each of `keys` — or, already in it, stay where
    /// `ticket` says — under one lock, and return the receiver this client
    /// is woken through, by whichever of them is written first.
    pub fn enqueue_all(
        &self,
        keys: &[impl AsRef<[u8]>],
        ticket: &Arc<Ticket>,
    ) -> oneshot::Receiver<Value> {
        self.enqueue_in(&self.queues, keys, ticket)
    }

    /// [`enqueue_all`](Self::enqueue_all) for one key.
    #[cfg(test)]
    pub fn enqueue(
        &self,
        key: &(impl AsRef<[u8]> + ?Sized),
        ticket: &Arc<Ticket>,
    ) -> oneshot::Receiver<Value> {
        self.enqueue_in(&self.queues, &[key.as_ref()], ticket)
    }

    /// [`enqueue_all`](Self::enqueue_all), in the lines of the keys'
    /// `XREADGROUP` clients.
    pub fn enqueue_readers(
        &self,
        keys: &[impl AsRef<[u8]>],
        ticket: &Arc<Ticket>,
    ) -> oneshot::Receiver<Value> {
        self.enqueue_in(&self.reader_queues, keys, ticket)
    }

    /// [`enqueue_readers`](Self::enqueue_readers) for one key.
    #[cfg(test)]
    pub fn enqueue_reader(
        &self,
        key: &(impl AsRef<[u8]> + ?Sized),
        ticket: &Arc<Ticket>,
    ) -> oneshot::Receiver<Value> {
        self.enqueue_in(&self.reader_queues, &[key.as_ref()], ticket)
    }

    fn enqueue_in(
        &self,
        table: &LineTable,
        keys: &[impl AsRef<[u8]>],
        ticket: &Arc<Ticket>,
    ) -> oneshot::Receiver<Value> {
        // Armed before it stands in line: a write the moment it does wakes it.
        let rx = ticket.arm();
        let mut lines = table.lines.lock();
        let mut added = 0;
        for key in keys {
            let key = key.as_ref();
            if !lines.contains_key(key) {
                lines.insert(Key::new(key), BTreeMap::new());
            }
            let Some(line) = lines.get_mut(key) else {
                continue;
            };
            match line.entry(ticket.id) {
                // Waiting again, in the same place: not woken now.
                std::collections::btree_map::Entry::Occupied(mut at) => at.get_mut().woken = false,
                std::collections::btree_map::Entry::Vacant(at) => {
                    at.insert(Queued {
                        bell: Arc::clone(&ticket.bell),
                        woken: false,
                    });
                    added += 1;
                }
            }
        }
        table.count.fetch_add(added, Ordering::Relaxed);
        rx
    }

    /// Leave the line for `key`.
    #[cfg(test)]
    pub fn dequeue(&self, key: &(impl AsRef<[u8]> + ?Sized), ticket: &Ticket) {
        self.leave_in(&self.queues, std::iter::once(key.as_ref()), ticket.id);
    }

    /// Leave the line of each of `keys`, under one lock, and say for each
    /// whether the turn there was this client's: it had been woken there
    /// and had not looked since it last waited. A key named twice is left
    /// once; the second time says `false`.
    pub fn dequeue_all(&self, keys: &[impl AsRef<[u8]>], ticket: &Ticket) -> Vec<bool> {
        self.leave_in(&self.queues, keys.iter().map(|key| key.as_ref()), ticket.id)
    }

    /// The command is answered: it has left every line its ticket stands
    /// in, and its ticket's `Drop` has nothing left to do.
    pub fn left_all(&self, ticket: &Ticket) {
        ticket.lines.lock().clear();
    }

    fn leave_in<'k>(
        &self,
        table: &LineTable,
        keys: impl Iterator<Item = &'k [u8]>,
        id: u64,
    ) -> Vec<bool> {
        let mut lines = table.lines.lock();
        let mut removed = 0;
        let woken = keys
            .map(|key| {
                let Some(line) = lines.get_mut(key) else {
                    return false;
                };
                let woken = line.remove(&id).map(|queued| {
                    removed += 1;
                    queued.woken
                });
                if line.is_empty() {
                    lines.remove(key);
                }
                woken.unwrap_or(false)
            })
            .collect();
        table.count.fetch_sub(removed, Ordering::Relaxed);
        woken
    }

    /// Leave the line of `key`'s `XREADGROUP` clients, and give the turn to
    /// the client behind: it may be reading for another group, for which
    /// there is something.
    pub fn reader_done(&self, key: &(impl AsRef<[u8]> + ?Sized), ticket: &Ticket) {
        let key = key.as_ref();
        self.leave_in(&self.reader_queues, std::iter::once(key), ticket.id);
        self.wake_next_in(&self.reader_queues, key, ticket.id);
    }

    /// Stay in that line, and give the turn to the client behind.
    pub fn reader_pass_on(&self, key: &(impl AsRef<[u8]> + ?Sized), ticket: &Ticket) {
        let key = key.as_ref();
        self.wake_next_in(&self.reader_queues, key, ticket.id);
    }

    /// Whether a client that blocked before `ticket` is still in the line
    /// for `key`.
    pub fn someone_ahead(&self, key: &(impl AsRef<[u8]> + ?Sized), ticket: &Ticket) -> bool {
        let key = key.as_ref();
        if self.queues.is_empty() {
            return false;
        }
        // From the front: the first that is still waiting answers it, and
        // that is nearly always the first.
        self.queues.lines.lock().get(key).is_some_and(|line| {
            line.range(..ticket.id)
                .any(|(_, queued)| queued.bell.alive())
        })
    }

    /// Make sure the client at the head of the line for `key` looks at it.
    pub fn wake_head(&self, key: &(impl AsRef<[u8]> + ?Sized)) {
        let key = key.as_ref();
        self.wake_next(key, 0);
    }

    /// Give the turn to the client behind `ticket` in the line for `key`.
    /// Every client calls this once it has looked at the key, whether or not
    /// it found something: there may be more than it took, or something it
    /// could not use that the next one can.
    pub fn pass_on(&self, key: &(impl AsRef<[u8]> + ?Sized), ticket: &Ticket) {
        let key = key.as_ref();
        self.wake_next(key, ticket.id);
    }

    /// Wake the first client in the line for `key` whose ticket is after
    /// `after`. If that client has already been woken and has not looked yet,
    /// there is nothing to do: it will, and it will pass the turn on.
    fn wake_next(&self, key: &[u8], after: u64) {
        self.wake_next_in(&self.queues, key, after);
    }

    fn wake_next_in(&self, table: &LineTable, key: &[u8], after: u64) {
        if table.is_empty() {
            return;
        }
        let mut lines = table.lines.lock();
        let Some(line) = lines.get_mut(key) else {
            return;
        };
        // Clients that have gone are passed over, and forgotten: a ticket
        // dropped without leaving (its command unwound) leaves its entries
        // here, and this is where they go. One that was woken and went
        // without looking hands its turn on by being passed over.
        let mut gone = Vec::new();
        for (&id, queued) in line.range_mut(after.saturating_add(1)..) {
            if !queued.bell.alive() {
                gone.push(id);
                continue;
            }
            if !queued.woken {
                queued.woken = true;
                queued.bell.ring(key);
            }
            break;
        }
        for id in &gone {
            line.remove(id);
        }
        table.count.fetch_sub(gone.len(), Ordering::Relaxed);
        if line.is_empty() {
            lines.remove(key);
        }
    }

    /// Register to be woken when `key` is written.
    ///
    /// The waiter is told nothing more than that: it looks at the key itself,
    /// takes what its own rules say it should, and goes back to waiting if
    /// there was nothing for it.
    pub fn register_signal(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> oneshot::Receiver<Value> {
        let key = key.as_ref();
        let (tx, rx) = oneshot::channel();
        let mut signals = self.signals.lock();
        signals.entry(Key::new(key)).or_default().push(tx);
        self.signal_count.fetch_add(1, Ordering::Relaxed);
        rx
    }

    /// Whether anyone at all is waiting for a signal. One atomic load per
    /// kind of waiter, so the write path can skip working out which keys it
    /// touched.
    #[inline]
    pub fn has_signal_waiters(&self) -> bool {
        self.signal_count.load(Ordering::Relaxed) != 0
            || !self.queues.is_empty()
            || !self.reader_queues.is_empty()
    }

    /// Wake everyone registered with [`register_signal`](Self::register_signal)
    /// on `key`. Called after the key has been written.
    #[inline]
    pub fn notify_signal(&self, key: &(impl AsRef<[u8]> + ?Sized)) {
        let key = key.as_ref();
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
        let keys: Vec<Key> = {
            let signals = self.signals.lock();
            let queues = self.queues.lines.lock();
            let readers = self.reader_queues.lines.lock();
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
        let mut keys: ahash::AHashSet<Key> = ahash::AHashSet::new();
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
        let alive = |line: &BTreeMap<u64, Queued>| line.values().any(|queued| queued.bell.alive());
        for (key, line) in self.queues.lines.lock().iter() {
            if alive(line) {
                keys.insert(key.clone());
            }
        }
        let mut on_nokey = 0;
        for (key, line) in self.reader_queues.lines.lock().iter() {
            if alive(line) {
                keys.insert(key.clone());
                on_nokey += 1;
            }
        }
        (keys.len(), on_nokey)
    }

    /// Forget signal registrations on `key` whose client has stopped waiting
    /// (timed out or disconnected).
    pub fn prune_signals(&self, key: &(impl AsRef<[u8]> + ?Sized)) {
        let key = key.as_ref();
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
    pub fn notify_stream_waiters(&self, key: &(impl AsRef<[u8]> + ?Sized)) -> usize {
        let key = key.as_ref();
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
    registered: Vec<(Key, u64)>,
}

impl StreamWait {
    /// Register on `key` and return the receiver the reader is woken
    /// through. A tokio oneshot, so the caller does not block an OS thread.
    pub fn register(&mut self, key: &(impl AsRef<[u8]> + ?Sized)) -> oneshot::Receiver<Value> {
        let key = key.as_ref();
        let (tx, rx) = oneshot::channel();
        let id = self.table.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.table
            .inner
            .lock()
            .entry(Key::new(key))
            .or_default()
            .insert(id, tx);
        self.registered.push((Key::new(key), id));
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
        let table = Arc::new(WaitTable::new());
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

    /// A client woken and then dropped before it has looked passes its turn
    /// on: the client behind it is woken, without another write to the key.
    /// One dropped before it was woken, or after it looked and left, wakes
    /// nobody.
    #[test]
    fn a_woken_client_dropped_before_it_looked_passes_its_turn_on() {
        let table = Arc::new(WaitTable::new());
        for line in [Line::Keys, Line::Readers] {
            // Each parks, as a command that waits does.
            let enqueue = |ticket: &Arc<Ticket>| {
                let rx = match line {
                    Line::Keys => table.enqueue("k", ticket),
                    Line::Readers => table.enqueue_reader("k", ticket),
                };
                ticket.parks_in(&["k".to_string()], line == Line::Readers);
                rx
            };
            let (first, second, third) =
                (table.new_ticket(), table.new_ticket(), table.new_ticket());
            let mut rx1 = enqueue(&first);
            let mut rx2 = enqueue(&second);
            let mut rx3 = enqueue(&third);

            // Not woken: dropping it wakes nobody.
            drop(third);
            assert!(rx3.try_recv().is_err());

            table.notify_signal("k");
            assert!(rx1.try_recv().is_ok());
            assert!(rx2.try_recv().is_err());
            // Woken, and gone without looking: the next one's turn.
            drop(first);
            assert!(rx2.try_recv().is_ok());

            // Looked and left: no second turn from its drop.
            match line {
                Line::Keys => {
                    table.dequeue("k", &second);
                    table.pass_on("k", &second);
                }
                Line::Readers => table.reader_done("k", &second),
            }
            let fourth = table.new_ticket();
            let mut rx4 = enqueue(&fourth);
            drop(second);
            assert!(rx4.try_recv().is_err());
            drop(fourth);
            assert!(!table.has_signal_waiters());
        }
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

    /// A client in the lines of several keys waits on one receiver, which
    /// the first of them to be written wakes, with that key's name; leaving
    /// says in which lines the turn was its own (#758).
    #[test]
    fn one_receiver_for_all_of_a_clients_keys() {
        let table = Arc::new(WaitTable::new());
        let ticket = table.new_ticket();
        let mut rx = table.enqueue_all(&["a", "b", "c"], &ticket);
        ticket.parks_in(&["a", "b", "c"], false);
        assert_eq!(table.queues.count.load(Ordering::Relaxed), 3);
        table.notify_signal("b");
        assert_eq!(rx.try_recv().unwrap().as_ref(), b"b");
        // Written again, and another key: the turn is with it already, and
        // it has been woken in this wait.
        table.notify_signal("b");
        table.notify_signal("c");
        assert_eq!(
            table.dequeue_all(&["a", "b", "c", "b"], &ticket),
            [false, true, true, false]
        );
        table.left_all(&ticket);
        assert_eq!(table.queues.count.load(Ordering::Relaxed), 0);
        assert!(table.queues.lines.lock().is_empty());
        drop(ticket);
        assert!(!table.has_signal_waiters());
    }

    /// Waiting again re-arms: the receiver of the last wait is not woken,
    /// the new one is, and the client keeps its place in every line.
    #[test]
    fn waiting_again_keeps_the_place_in_every_line() {
        let table = Arc::new(WaitTable::new());
        let first = table.new_ticket();
        let second = table.new_ticket();
        let _rx1 = table.enqueue_all(&["a", "b"], &first);
        let mut rx2 = table.enqueue_all(&["a", "b"], &second);
        table.notify_signal("a");
        // The first looked, found nothing it could take, and waits again.
        let mut rx1 = table.enqueue_all(&["a", "b"], &first);
        table.notify_signal("b");
        assert_eq!(rx1.try_recv().unwrap().as_ref(), b"b");
        assert!(rx2.try_recv().is_err());
        assert!(table.someone_ahead("a", &second));
        assert!(!table.someone_ahead("a", &first));
    }

    /// A ticket dropped while it stands in many lines leaves all of them,
    /// and passes its turn on only where it had been woken.
    #[test]
    fn a_dropped_ticket_leaves_every_line() {
        let table = Arc::new(WaitTable::new());
        let keys: Vec<String> = (0..270).map(|i| format!("queue:{i}")).collect();
        let tickets: Vec<_> = (0..100).map(|_| table.new_ticket()).collect();
        let mut rxs: Vec<_> = tickets
            .iter()
            .map(|ticket| {
                let rx = table.enqueue_all(&keys, ticket);
                ticket.parks_in(&keys, false);
                rx
            })
            .collect();
        assert_eq!(table.queues.count.load(Ordering::Relaxed), 270 * 100);
        table.notify_signal("queue:7");
        assert_eq!(rxs[0].try_recv().unwrap().as_ref(), b"queue:7");
        let mut tickets = tickets.into_iter();
        // The first was woken for queue:7 and goes without looking: the
        // second is woken for it, and for nothing else.
        drop(tickets.next());
        assert_eq!(rxs[1].try_recv().unwrap().as_ref(), b"queue:7");
        assert!(rxs[2].try_recv().is_err());
        assert_eq!(table.queues.count.load(Ordering::Relaxed), 270 * 99);
        drop(tickets);
        assert_eq!(table.queues.count.load(Ordering::Relaxed), 0);
        assert!(table.queues.lines.lock().is_empty());
    }

    /// Entries left behind by a ticket that went without leaving (its
    /// command unwound before it parked) are passed over and forgotten.
    #[test]
    fn entries_of_a_ticket_that_never_parked_are_passed_over() {
        let table = Arc::new(WaitTable::new());
        let gone = table.new_ticket();
        let behind = table.new_ticket();
        let _gone_rx = table.enqueue_all(&["k"], &gone);
        let mut rx = table.enqueue_all(&["k"], &behind);
        drop(gone);
        assert!(!table.someone_ahead("k", &behind));
        table.notify_signal("k");
        assert_eq!(rx.try_recv().unwrap().as_ref(), b"k");
        assert_eq!(table.queues.count.load(Ordering::Relaxed), 1);
    }
}
