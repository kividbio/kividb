//! Pub/Sub: the hub that routes a published message to its subscribers, and
//! what a connection keeps while it is one.
//!
//! Every subscribed connection has one queue. A publish looks up who is
//! subscribed and appends to each of their queues; the connection's forwarder
//! task takes from its queue and writes to the socket. So a subscriber sees
//! messages in the order they were published, whichever of its channels and
//! patterns they came through, and none is ever left out of the middle: a
//! subscriber that cannot keep up is disconnected once its queue passes the
//! limits below — Redis's `client-output-buffer-limit pubsub 32mb 8mb 60`.
//!
//! A subscribed connection stays in its ordinary command loop. Under RESP3
//! it keeps every command; under RESP2 the loop allows it only
//! (P|S)SUBSCRIBE, (P|S)UNSUBSCRIBE, PING, QUIT and RESET until it has no
//! subscriptions left, when it is an ordinary connection again.

use crate::client_registry::ClientRegistry;
use ahash::{AHashMap, AHashSet};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};

/// A subscriber with more than this queued is disconnected.
const HARD_LIMIT: usize = 32 << 20;
/// So is one that has had more than this queued for `SOFT_SECONDS`.
const SOFT_LIMIT: usize = 8 << 20;
const SOFT_SECONDS: u64 = 60;
/// How much the forwarder takes from the queue for one write.
const BATCH_BYTES: usize = 256 << 10;

const CHANNEL: usize = 0;
const PATTERN: usize = 1;
const SHARD: usize = 2;

/// One message on its way to one subscriber.
struct Delivery {
    kind: usize,
    /// What the subscriber subscribed to: the channel, or the pattern.
    subscription: Arc<str>,
    /// The channel the message was published on.
    channel: Arc<str>,
    payload: Arc<[u8]>,
}

impl Delivery {
    /// Roughly what it costs to keep queued, and to send.
    fn weight(&self) -> usize {
        self.payload.len() + self.channel.len() + 64
    }
}

/// Seconds since the first call, from 1: 0 is kept to mean "not set".
fn uptime_secs() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs()
        + 1
}

// ── Subscriber ────────────────────────────────────────────────────────────────

/// The hub's side of a subscribed connection.
struct Subscriber {
    client_id: u64,
    registry: ClientRegistry,
    queue: mpsc::UnboundedSender<Delivery>,
    /// Weight queued and not yet taken by the forwarder.
    pending: AtomicUsize,
    /// When `pending` went over the soft limit; 0 while it is under.
    over_soft_since: AtomicU64,
    /// Set once it has been disconnected for falling behind.
    closed: AtomicBool,
    /// What it is subscribed to right now, by kind. The forwarder checks a
    /// message against this as it writes, so that nothing for a channel
    /// follows the reply to its UNSUBSCRIBE.
    live: Mutex<[AHashSet<Arc<str>>; 3]>,
    /// Set from a SUBSCRIBE until its reply has been written: the forwarder
    /// waits, so that no message comes before the reply either. Set too by a
    /// message this connection publishes to itself, for the same reason: it
    /// comes after the reply to the command that published it (#23).
    hold: AtomicBool,
    released: Notify,
}

enum Offer {
    Queued,
    /// Already disconnected, or gone.
    Refused,
    /// This message took it over a limit.
    Overflow,
}

impl Subscriber {
    /// `from`: the connection that published it, if it was one.
    fn offer(&self, delivery: Delivery, from: Option<u64>) -> Offer {
        if self.closed.load(Ordering::Relaxed) {
            return Offer::Refused;
        }
        let weight = delivery.weight();
        let pending = self.pending.fetch_add(weight, Ordering::Relaxed) + weight;
        if pending > SOFT_LIMIT {
            if pending > HARD_LIMIT || self.over_soft_limit_too_long() {
                self.closed.store(true, Ordering::Relaxed);
                return Offer::Overflow;
            }
        } else if self.over_soft_since.load(Ordering::Relaxed) != 0 {
            self.over_soft_since.store(0, Ordering::Relaxed);
        }
        // To itself: held until this connection's reply is written, as Redis
        // holds such a push until after the command (`pending_push_messages`).
        // Its forwarder would otherwise write it first, inside a script or a
        // transaction as for a plain PUBLISH. Set before it is queued, so the
        // forwarder sees the hold whenever it sees the message.
        if from == Some(self.client_id) {
            self.hold.store(true, Ordering::Release);
        }
        match self.queue.send(delivery) {
            Ok(()) => Offer::Queued,
            Err(_) => Offer::Refused,
        }
    }

    fn over_soft_limit_too_long(&self) -> bool {
        let now = uptime_secs();
        match self
            .over_soft_since
            .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => false,
            Err(since) => now.saturating_sub(since) >= SOFT_SECONDS,
        }
    }

    /// Close the connection of a subscriber that fell too far behind.
    fn disconnect(&self) {
        log_warn!(
            "Client id={} closed: it is subscribed and {} bytes behind, over the \
             pub/sub output buffer limit",
            self.client_id,
            self.pending.load(Ordering::Relaxed)
        );
        self.registry.kill(self.client_id);
    }
}

// ── Hub ───────────────────────────────────────────────────────────────────────

type Table = AHashMap<Arc<str>, Vec<Arc<Subscriber>>>;

struct Shared {
    /// By kind: channels, patterns, shard channels. A name is in a table
    /// only while somebody is subscribed to it.
    tables: RwLock<[Table; 3]>,
    /// Subscriptions of every kind, so that a publish with nobody listening
    /// — every keyspace event on most servers — takes no lock.
    subscriptions: AtomicUsize,
}

#[derive(Clone)]
pub struct PubSubHub {
    inner: Arc<Shared>,
}

impl Default for PubSubHub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubHub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Shared {
                tables: RwLock::new([Table::new(), Table::new(), Table::new()]),
                subscriptions: AtomicUsize::new(0),
            }),
        }
    }

    fn add(&self, kind: usize, name: &Arc<str>, subscriber: &Arc<Subscriber>) {
        let mut tables = self.inner.tables.write();
        tables[kind]
            .entry(Arc::clone(name))
            .or_default()
            .push(Arc::clone(subscriber));
        self.inner.subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    fn remove(&self, kind: usize, name: &str, subscriber: &Arc<Subscriber>) {
        let mut tables = self.inner.tables.write();
        let Some(subscribers) = tables[kind].get_mut(name) else {
            return;
        };
        let Some(at) = subscribers.iter().position(|s| Arc::ptr_eq(s, subscriber)) else {
            return;
        };
        subscribers.swap_remove(at);
        if subscribers.is_empty() {
            tables[kind].remove(name);
        }
        self.inner.subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    // ── Publish ───────────────────────────────────────────────────────────

    /// To the channel's subscribers and to those of every pattern it matches.
    /// Returns how many subscriptions it was delivered to — a client that has
    /// both the channel and a matching pattern counts twice, as in Redis.
    pub fn publish(&self, channel: &str, payload: Arc<[u8]>) -> usize {
        self.publish_from(channel, payload, None)
    }

    /// [`publish`](Self::publish), by the connection `from`: what it sends
    /// itself waits for its reply (see `Subscriber::hold`).
    pub fn publish_from(&self, channel: &str, payload: Arc<[u8]>, from: Option<u64>) -> usize {
        if self.inner.subscriptions.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        let mut delivered = 0;
        let mut overflowed: Vec<Arc<Subscriber>> = Vec::new();
        {
            let tables = self.inner.tables.read();
            let mut name: Option<Arc<str>> = None;
            if let Some((key, subscribers)) = tables[CHANNEL].get_key_value(channel) {
                name = Some(Arc::clone(key));
                for subscriber in subscribers {
                    let delivery = Delivery {
                        kind: CHANNEL,
                        subscription: Arc::clone(key),
                        channel: Arc::clone(key),
                        payload: Arc::clone(&payload),
                    };
                    delivered += hand_over(subscriber, delivery, from, &mut overflowed);
                }
            }
            for (pattern, subscribers) in &tables[PATTERN] {
                if !glob_matches(pattern, channel) {
                    continue;
                }
                let channel = Arc::clone(name.get_or_insert_with(|| Arc::from(channel)));
                for subscriber in subscribers {
                    let delivery = Delivery {
                        kind: PATTERN,
                        subscription: Arc::clone(pattern),
                        channel: Arc::clone(&channel),
                        payload: Arc::clone(&payload),
                    };
                    delivered += hand_over(subscriber, delivery, from, &mut overflowed);
                }
            }
        }
        // With the tables unlocked: closing a connection ends its
        // subscriptions, which locks them.
        for subscriber in overflowed {
            subscriber.disconnect();
        }
        delivered
    }

    pub fn spublish(&self, shard: &str, payload: Arc<[u8]>) -> usize {
        self.spublish_from(shard, payload, None)
    }

    /// [`spublish`](Self::spublish), by the connection `from`.
    pub fn spublish_from(&self, shard: &str, payload: Arc<[u8]>, from: Option<u64>) -> usize {
        if self.inner.subscriptions.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        let mut delivered = 0;
        let mut overflowed: Vec<Arc<Subscriber>> = Vec::new();
        {
            let tables = self.inner.tables.read();
            if let Some((key, subscribers)) = tables[SHARD].get_key_value(shard) {
                for subscriber in subscribers {
                    let delivery = Delivery {
                        kind: SHARD,
                        subscription: Arc::clone(key),
                        channel: Arc::clone(key),
                        payload: Arc::clone(&payload),
                    };
                    delivered += hand_over(subscriber, delivery, from, &mut overflowed);
                }
            }
        }
        for subscriber in overflowed {
            subscriber.disconnect();
        }
        delivered
    }

    // ── PUBSUB introspection ──────────────────────────────────────────────

    fn names(&self, kind: usize, pattern: Option<&str>) -> Vec<String> {
        let tables = self.inner.tables.read();
        let mut names: Vec<String> = tables[kind]
            .keys()
            .filter(|name| pattern.map(|p| glob_matches(p, name)).unwrap_or(true))
            .map(|name| name.to_string())
            .collect();
        names.sort();
        names
    }

    fn counts(&self, kind: usize, names: &[&str]) -> Vec<(String, usize)> {
        let tables = self.inner.tables.read();
        names
            .iter()
            .map(|name| {
                (
                    name.to_string(),
                    tables[kind].get(*name).map(Vec::len).unwrap_or(0),
                )
            })
            .collect()
    }

    pub fn channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.names(CHANNEL, pattern)
    }

    pub fn numsub(&self, names: &[&str]) -> Vec<(String, usize)> {
        self.counts(CHANNEL, names)
    }

    /// The number of distinct patterns subscribed to, by however many clients.
    pub fn numpat(&self) -> usize {
        self.inner.tables.read()[PATTERN].len()
    }

    pub fn shard_channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.names(SHARD, pattern)
    }

    pub fn shardnumsub(&self, names: &[&str]) -> Vec<(String, usize)> {
        self.counts(SHARD, names)
    }
}

/// 1 if the subscriber took the message.
fn hand_over(
    subscriber: &Arc<Subscriber>,
    delivery: Delivery,
    from: Option<u64>,
    overflowed: &mut Vec<Arc<Subscriber>>,
) -> usize {
    match subscriber.offer(delivery, from) {
        Offer::Queued => 1,
        Offer::Refused => 0,
        Offer::Overflow => {
            overflowed.push(Arc::clone(subscriber));
            0
        }
    }
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    crate::commands::helpers::glob_match(pattern, text)
}

// ── Framing ───────────────────────────────────────────────────────────────────
//
// Messages and the replies to (un)subscribe commands are ordinary arrays
// under RESP2 and *push* frames (`>`) under RESP3 — Redis uses
// `addReplyPushLen` for both. A RESP3 client tells an out-of-band delivery
// from the reply to a command it issued by that tag. The framing is the
// connection's protocol at the moment of writing, so that a `HELLO` while
// subscribed changes it from the next frame on.

fn write_header(out: &mut Vec<u8>, resp: u8, items: usize) {
    out.push(if resp >= 3 { b'>' } else { b'*' });
    out.extend_from_slice(items.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn write_bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(data.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

fn write_delivery(out: &mut Vec<u8>, resp: u8, delivery: &Delivery) {
    match delivery.kind {
        PATTERN => {
            write_header(out, resp, 4);
            write_bulk(out, b"pmessage");
            write_bulk(out, delivery.subscription.as_bytes());
        }
        kind => {
            write_header(out, resp, 3);
            let word: &[u8] = if kind == SHARD {
                b"smessage"
            } else {
                b"message"
            };
            write_bulk(out, word);
        }
    }
    write_bulk(out, delivery.channel.as_bytes());
    write_bulk(out, &delivery.payload);
}

/// The reply to one name of a (un)subscribe command. `name` is `None` for an
/// unsubscribe-from-everything that found nothing to unsubscribe from.
fn write_ack(out: &mut Vec<u8>, resp: u8, word: &str, name: Option<&str>, count: usize) {
    write_header(out, resp, 3);
    write_bulk(out, word.as_bytes());
    match name {
        Some(name) => write_bulk(out, name.as_bytes()),
        None if resp >= 3 => out.extend_from_slice(b"_\r\n"),
        None => out.extend_from_slice(b"$-1\r\n"),
    }
    out.push(b':');
    out.extend_from_slice(count.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

// ── A connection's subscriptions ──────────────────────────────────────────────

/// Commands handled by [`Subscriptions::handle`].
pub fn is_subscription_command(name: &str) -> bool {
    matches!(
        name,
        "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" | "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE"
    )
}

/// What a RESP2 connection that has subscriptions is told instead of having
/// `name` run, if anything: Redis allows it only the subscription commands,
/// PING, QUIT and RESET, and answers PING with an array so that the reply
/// can be told from a message.
pub fn resp2_subscriber_reply(name: &str, cmd: &[&[u8]]) -> Option<Vec<u8>> {
    match name {
        "QUIT" | "RESET" => None,
        _ if is_subscription_command(name) => None,
        "PING" => {
            let mut out = Vec::new();
            write_header(&mut out, 2, 2);
            write_bulk(&mut out, b"pong");
            write_bulk(&mut out, cmd.get(1).copied().unwrap_or(b""));
            Some(out)
        }
        _ => Some(
            format!(
                "-ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / \
                 QUIT / RESET are allowed in this context\r\n",
                name.to_lowercase()
            )
            .into_bytes(),
        ),
    }
}

/// The connection's side of its subscriptions. Dropping it ends them all.
pub struct Subscriptions {
    hub: PubSubHub,
    me: Arc<Subscriber>,
    /// In the order they were subscribed to, by kind.
    names: [Vec<Arc<str>>; 3],
    forwarder: Option<tokio::task::JoinHandle<()>>,
}

/// A connection's subscriptions, none yet, and the queue its messages will
/// arrive on. The caller starts a task that writes what [`Outbox::next`]
/// yields to the socket, and gives it to [`Subscriptions::forwarded_by`].
pub fn connect(
    hub: &PubSubHub,
    registry: ClientRegistry,
    client_id: u64,
) -> (Subscriptions, Outbox) {
    let (queue, rx) = mpsc::unbounded_channel();
    let me = Arc::new(Subscriber {
        client_id,
        registry,
        queue,
        pending: AtomicUsize::new(0),
        over_soft_since: AtomicU64::new(0),
        closed: AtomicBool::new(false),
        live: Mutex::new([AHashSet::new(), AHashSet::new(), AHashSet::new()]),
        hold: AtomicBool::new(false),
        released: Notify::new(),
    });
    let subscriptions = Subscriptions {
        hub: hub.clone(),
        me: Arc::clone(&me),
        names: [Vec::new(), Vec::new(), Vec::new()],
        forwarder: None,
    };
    (subscriptions, Outbox { rx, me })
}

impl Subscriptions {
    /// The task writing this connection's messages; stopped when the
    /// subscriptions end.
    pub fn forwarded_by(&mut self, task: tokio::task::JoinHandle<()>) {
        self.forwarder = Some(task);
    }

    /// Channels, patterns and shard channels together: the count every
    /// reply to a (un)subscribe carries.
    pub fn total(&self) -> usize {
        self.names.iter().map(Vec::len).sum()
    }

    /// The replies written so far have reached the socket: messages may
    /// follow them.
    pub fn flushed(&self) {
        if self.me.hold.swap(false, Ordering::AcqRel) {
            self.me.released.notify_one();
        }
    }

    /// Run one subscription command; the replies go out with the
    /// connection's other replies, in order.
    pub fn handle(&mut self, cmd: &[&[u8]], resp: u8) -> Vec<u8> {
        let mut out = Vec::new();
        let Some(first) = cmd.first() else {
            return out;
        };
        let name = String::from_utf8_lossy(first).to_ascii_uppercase();
        let (kind, subscribing, word) = match name.as_str() {
            "SUBSCRIBE" => (CHANNEL, true, "subscribe"),
            "PSUBSCRIBE" => (PATTERN, true, "psubscribe"),
            "SSUBSCRIBE" => (SHARD, true, "ssubscribe"),
            "UNSUBSCRIBE" => (CHANNEL, false, "unsubscribe"),
            "PUNSUBSCRIBE" => (PATTERN, false, "punsubscribe"),
            "SUNSUBSCRIBE" => (SHARD, false, "sunsubscribe"),
            _ => return out,
        };

        if subscribing {
            if cmd.len() < 2 {
                return format!("-ERR wrong number of arguments for '{word}' command\r\n")
                    .into_bytes();
            }
            // Before the hub knows: whatever is published from here on waits
            // for these replies to be written.
            self.me.hold.store(true, Ordering::Release);
            for raw in &cmd[1..] {
                let target: Arc<str> = Arc::from(String::from_utf8_lossy(raw).as_ref());
                let new = self.me.live.lock()[kind].insert(Arc::clone(&target));
                if new {
                    self.names[kind].push(Arc::clone(&target));
                    self.hub.add(kind, &target, &self.me);
                }
                write_ack(&mut out, resp, word, Some(target.as_ref()), self.total());
            }
        } else if cmd.len() == 1 {
            let all = std::mem::take(&mut self.names[kind]);
            if all.is_empty() {
                write_ack(&mut out, resp, word, None, self.total());
            }
            let others = self.total();
            for (done, target) in all.iter().enumerate() {
                self.forget(kind, target);
                write_ack(
                    &mut out,
                    resp,
                    word,
                    Some(target.as_ref()),
                    others + all.len() - done - 1,
                );
            }
        } else {
            for raw in &cmd[1..] {
                let target = String::from_utf8_lossy(raw);
                if let Some(at) = self.names[kind].iter().position(|n| **n == *target) {
                    let target = self.names[kind].remove(at);
                    self.forget(kind, &target);
                }
                write_ack(&mut out, resp, word, Some(target.as_ref()), self.total());
            }
        }

        let counts = [
            self.names[CHANNEL].len(),
            self.names[PATTERN].len(),
            self.names[SHARD].len(),
        ];
        self.me
            .registry
            .update(self.me.client_id, |info| info.subscriptions = counts);
        out
    }

    /// End one subscription, already taken out of `names`.
    fn forget(&self, kind: usize, target: &str) {
        // First, so that what is still queued for it is not written.
        self.me.live.lock()[kind].remove(target);
        self.hub.remove(kind, target, &self.me);
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        for kind in [CHANNEL, PATTERN, SHARD] {
            for target in std::mem::take(&mut self.names[kind]) {
                self.forget(kind, &target);
            }
        }
        if let Some(task) = self.forwarder.take() {
            task.abort();
        }
        self.me
            .registry
            .update(self.me.client_id, |info| info.subscriptions = [0; 3]);
    }
}

/// The messages queued for a connection, for its forwarder task.
pub struct Outbox {
    rx: mpsc::UnboundedReceiver<Delivery>,
    me: Arc<Subscriber>,
}

/// Some messages, in order, taken from the queue.
pub struct Batch {
    deliveries: Vec<Delivery>,
    me: Arc<Subscriber>,
}

impl Outbox {
    /// Waits for messages and returns the next of them. The caller locks
    /// the socket for writing, and only then calls [`Batch::encode`].
    pub async fn next(&mut self) -> Option<Batch> {
        let first = self.rx.recv().await?;
        let mut weight = first.weight();
        let mut deliveries = vec![first];
        while weight < BATCH_BYTES {
            match self.rx.try_recv() {
                Ok(delivery) => {
                    weight += delivery.weight();
                    deliveries.push(delivery);
                }
                Err(_) => break,
            }
        }
        let pending = self.me.pending.fetch_sub(weight, Ordering::Relaxed) - weight;
        if pending <= SOFT_LIMIT {
            self.me.over_soft_since.store(0, Ordering::Relaxed);
        }
        // The reply to a SUBSCRIBE goes first: see `Subscriber::hold`. What
        // was taken above was published before whatever a later SUBSCRIBE
        // brings, so one wait covers the batch.
        while self.me.hold.load(Ordering::Acquire) {
            self.me.released.notified().await;
        }
        Some(Batch {
            deliveries,
            me: Arc::clone(&self.me),
        })
    }
}

impl Batch {
    /// The frames to write, for a connection speaking `resp` — without the
    /// messages of subscriptions that have ended since they were queued.
    /// Empty if that is all of them.
    ///
    /// Called with the socket locked for writing: the connection's loop ends
    /// a subscription before it writes the reply saying so, so a message is
    /// either written before that reply or not at all.
    pub fn encode(&self, resp: u8) -> Vec<u8> {
        let live = self.me.live.lock();
        let mut out = Vec::new();
        for delivery in &self.deliveries {
            if live[delivery.kind].contains(&delivery.subscription) {
                write_delivery(&mut out, resp, delivery);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriber(hub: &PubSubHub, id: u64) -> (Subscriptions, Outbox) {
        connect(hub, crate::client_registry::new_client_registry(), id)
    }

    async fn frames(outbox: &mut Outbox, resp: u8) -> String {
        let batch = outbox.next().await.unwrap();
        String::from_utf8(batch.encode(resp)).unwrap()
    }

    #[tokio::test]
    async fn a_message_reaches_channel_and_pattern_subscribers_in_order() {
        let hub = PubSubHub::new();
        assert_eq!(hub.publish("news", Arc::from(&b"nobody"[..])), 0);

        let (mut subs, mut outbox) = subscriber(&hub, 1);
        let ack = subs.handle(&[b"SUBSCRIBE", b"news", b"sport"], 2);
        assert_eq!(
            ack,
            b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$5\r\nsport\r\n:2\r\n"
        );
        subs.handle(&[b"PSUBSCRIBE", b"n*"], 2);
        subs.flushed();

        // Channel and pattern both: two deliveries, as Redis counts them.
        assert_eq!(hub.publish("news", Arc::from(&b"1"[..])), 2);
        assert_eq!(hub.publish("sport", Arc::from(&b"2"[..])), 1);
        assert_eq!(hub.publish("nothing", Arc::from(&b"3"[..])), 1);
        assert_eq!(hub.publish("other", Arc::from(&b"4"[..])), 0);
        assert_eq!(
            frames(&mut outbox, 2).await,
            "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$1\r\n1\r\n\
             *4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$1\r\n1\r\n\
             *3\r\n$7\r\nmessage\r\n$5\r\nsport\r\n$1\r\n2\r\n\
             *4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$7\r\nnothing\r\n$1\r\n3\r\n"
        );

        assert_eq!(hub.channels(None), ["news", "sport"]);
        assert_eq!(hub.numpat(), 1);
        drop(subs);
        assert!(hub.channels(None).is_empty());
        assert_eq!(hub.numpat(), 0);
        assert_eq!(hub.publish("news", Arc::from(&b"5"[..])), 0);
    }

    #[tokio::test]
    async fn push_frames_under_resp3_and_shard_channels_apart() {
        let hub = PubSubHub::new();
        let (mut subs, mut outbox) = subscriber(&hub, 1);
        assert_eq!(
            subs.handle(&[b"SSUBSCRIBE", b"s"], 3),
            b">3\r\n$10\r\nssubscribe\r\n$1\r\ns\r\n:1\r\n"
        );
        subs.flushed();
        assert_eq!(hub.publish("s", Arc::from(&b"x"[..])), 0);
        assert_eq!(hub.spublish("s", Arc::from(&b"y"[..])), 1);
        assert_eq!(
            frames(&mut outbox, 3).await,
            ">3\r\n$8\r\nsmessage\r\n$1\r\ns\r\n$1\r\ny\r\n"
        );
        assert_eq!(hub.shardnumsub(&["s"]), [("s".to_string(), 1)]);
        assert!(hub.channels(None).is_empty());
    }

    #[tokio::test]
    async fn nothing_for_a_channel_follows_its_unsubscribe() {
        let hub = PubSubHub::new();
        let (mut subs, mut outbox) = subscriber(&hub, 1);
        subs.handle(&[b"SUBSCRIBE", b"a", b"b"], 2);
        subs.flushed();
        hub.publish("a", Arc::from(&b"1"[..]));
        hub.publish("b", Arc::from(&b"2"[..]));
        // Queued, not yet written, when `a` is unsubscribed from.
        assert_eq!(
            subs.handle(&[b"UNSUBSCRIBE", b"a"], 2),
            b"*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n"
        );
        assert_eq!(
            frames(&mut outbox, 2).await,
            "*3\r\n$7\r\nmessage\r\n$1\r\nb\r\n$1\r\n2\r\n"
        );
    }

    #[tokio::test]
    async fn messages_wait_for_the_reply_to_subscribe() {
        let hub = PubSubHub::new();
        let (mut subs, mut outbox) = subscriber(&hub, 1);
        subs.handle(&[b"SUBSCRIBE", b"a"], 2);
        hub.publish("a", Arc::from(&b"1"[..]));
        let waiting =
            tokio::time::timeout(std::time::Duration::from_millis(50), outbox.next()).await;
        assert!(waiting.is_err(), "a message was let out before the reply");
    }

    #[test]
    fn unsubscribing_counts_down_and_from_nothing_is_a_null() {
        let hub = PubSubHub::new();
        let (mut subs, _outbox) = subscriber(&hub, 1);
        assert_eq!(
            subs.handle(&[b"UNSUBSCRIBE"], 2),
            b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
        );
        assert_eq!(
            subs.handle(&[b"PUNSUBSCRIBE"], 3),
            b">3\r\n$12\r\npunsubscribe\r\n_\r\n:0\r\n"
        );
        subs.handle(&[b"SUBSCRIBE", b"a", b"b", b"a"], 2);
        subs.handle(&[b"PSUBSCRIBE", b"p"], 2);
        assert_eq!(subs.total(), 3);
        assert_eq!(
            subs.handle(&[b"UNSUBSCRIBE"], 2),
            b"*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:2\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:1\r\n"
        );
        assert!(
            String::from_utf8(subs.handle(&[b"SUBSCRIBE"], 2))
                .unwrap()
                .starts_with("-ERR wrong number of arguments")
        );
    }

    #[test]
    fn a_subscriber_too_far_behind_is_cut_off_not_skipped() {
        let hub = PubSubHub::new();
        let (mut subs, _outbox) = subscriber(&hub, 1);
        subs.handle(&[b"SUBSCRIBE", b"a"], 2);
        let big: Arc<[u8]> = Arc::from(vec![0u8; 1 << 20]);
        // Nobody is reading its queue: every message up to the limit is
        // queued, and after that it takes none.
        let taken: usize = (0..40).map(|_| hub.publish("a", Arc::clone(&big))).sum();
        assert_eq!(taken, HARD_LIMIT / ((1 << 20) + 65));
        assert_eq!(hub.publish("a", Arc::from(&b"small"[..])), 0);
    }

    #[test]
    fn a_resp2_subscriber_is_allowed_little() {
        assert_eq!(
            resp2_subscriber_reply("PING", &[b"PING"]).unwrap(),
            b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"
        );
        assert_eq!(
            resp2_subscriber_reply("PING", &[b"PING", b"hi"]).unwrap(),
            b"*2\r\n$4\r\npong\r\n$2\r\nhi\r\n"
        );
        assert!(resp2_subscriber_reply("RESET", &[b"RESET"]).is_none());
        assert!(resp2_subscriber_reply("PUNSUBSCRIBE", &[b"PUNSUBSCRIBE"]).is_none());
        let refused = resp2_subscriber_reply("GET", &[b"GET", b"k"]).unwrap();
        assert!(
            String::from_utf8(refused)
                .unwrap()
                .starts_with("-ERR Can't execute 'get': only (P|S)SUBSCRIBE")
        );
    }
}
