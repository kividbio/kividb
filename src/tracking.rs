// src/tracking.rs
//
// Client-side caching invalidation table.
//
// Maps key → set of client IDs that have read that key and have
// CLIENT TRACKING ON. When a key is written, all registered clients
// receive a RESP3 push invalidation message.
//
// Design: DashMap for lock-free concurrent reads from the write path.
// Each shard is independently locked so SET on "foo" doesn't block
// GET on "bar" from registering "bar" → client_id.
use std::sync::atomic::{AtomicUsize, Ordering};

/// Number of clients with CLIENT TRACKING ON.
/// handler.rs does ONE Relaxed load per command on both the read and write
/// paths. Zero cost when no tracking clients are connected.
pub static TRACKING_CLIENT_COUNT: AtomicUsize = AtomicUsize::new(0);

use ahash::AHashSet;
use std::sync::Arc;

pub type TrackingTable = Arc<TrackingTableInner>;

use ahash::AHashMap;
use parking_lot::RwLock;

/// A client in `BCAST` mode: the prefixes it asked for — it is told of
/// every write to a key that begins with one; the empty prefix is every key
/// — and whether it asked not to be told of its own writes.
struct Broadcast {
    prefixes: Vec<String>,
    noloop: bool,
}

/// Keys a `BCAST` client is to be told have changed: one message for each
/// of its prefixes, sent when the command that changed them is over.
pub struct Pending {
    pub client: u64,
    prefix: String,
    pub keys: Vec<String>,
}

pub struct TrackingTableInner {
    /// Key → the clients that have read it and are to be told when it
    /// changes. (Not the `BCAST` clients: they are told by prefix.)
    table: RwLock<AHashMap<String, AHashSet<u64>>>,
    broadcast: RwLock<AHashMap<u64, Broadcast>>,
    /// Number of `BCAST` clients: a write looks no further when it is zero.
    broadcasting: AtomicUsize,
    pending: parking_lot::Mutex<Vec<Pending>>,
    has_pending: std::sync::atomic::AtomicBool,
    /// `tracking-table-max-keys`: zero for no limit.
    max_keys: AtomicUsize,
    /// The server's users, for what a `BCAST` client may be told of (see
    /// [`readable`]). Unset, every key is told.
    acl: std::sync::OnceLock<crate::acl::AclStore>,
}

impl Default for TrackingTableInner {
    fn default() -> Self {
        Self::new()
    }
}

/// A key the table has let go of, and the clients that were tracking it.
pub type Dropped = (String, AHashSet<u64>);

/// `tracking-table-max-keys` of a table made from now on: the configuration
/// file's, else Redis's default (#142). `CONFIG SET` changes the table's own.
pub static DEFAULT_MAX_KEYS: AtomicUsize = AtomicUsize::new(1_000_000);

impl TrackingTableInner {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(AHashMap::new()),
            broadcast: RwLock::new(AHashMap::new()),
            broadcasting: AtomicUsize::new(0),
            pending: parking_lot::Mutex::new(Vec::new()),
            has_pending: std::sync::atomic::AtomicBool::new(false),
            max_keys: AtomicUsize::new(DEFAULT_MAX_KEYS.load(Ordering::Relaxed)),
            acl: std::sync::OnceLock::new(),
        }
    }

    /// Register that client `client_id` has read `key`. Returns the keys
    /// that had to be dropped to keep the table within its limit: their
    /// clients are told those keys are invalid, which is always safe.
    #[must_use]
    pub fn register(&self, key: &str, client_id: u64) -> Vec<Dropped> {
        let mut table = self.table.write();
        table.entry(key.to_string()).or_default().insert(client_id);
        Self::trim(&mut table, self.max_keys.load(Ordering::Relaxed), Some(key))
    }

    /// `tracking-table-max-keys`. Returns what no longer fits.
    #[must_use]
    pub fn set_max_keys(&self, max_keys: usize) -> Vec<Dropped> {
        self.max_keys.store(max_keys, Ordering::Relaxed);
        Self::trim(&mut self.table.write(), max_keys, None)
    }

    pub fn max_keys(&self) -> usize {
        self.max_keys.load(Ordering::Relaxed)
    }

    fn trim(
        table: &mut AHashMap<String, AHashSet<u64>>,
        max_keys: usize,
        keep: Option<&str>,
    ) -> Vec<Dropped> {
        if max_keys == 0 || table.len() <= max_keys {
            return Vec::new();
        }
        let excess = table.len() - max_keys;
        let doomed: Vec<String> = table
            .keys()
            .filter(|key| keep != Some(key.as_str()))
            .take(excess)
            .cloned()
            .collect();
        doomed
            .into_iter()
            .filter_map(|key| table.remove(&key).map(|clients| (key, clients)))
            .collect()
    }

    /// Return the set of client IDs tracking `key`, and remove the entry.
    /// Removal is correct — after invalidation is sent, clients will
    /// re-register on their next read if they still care about the key.
    pub fn take_clients(&self, key: &str) -> Option<AHashSet<u64>> {
        self.table.write().remove(key)
    }

    /// Forget every key: the keyspace was flushed.
    pub fn clear(&self) {
        self.table.write().clear();
    }

    /// `(keys, clients across all keys, prefixes)`, for `INFO`.
    pub fn totals(&self) -> (usize, usize, usize) {
        let table = self.table.read();
        (
            table.len(),
            table.values().map(|clients| clients.len()).sum(),
            self.broadcast
                .read()
                .values()
                .map(|client| client.prefixes.len())
                .sum(),
        )
    }

    /// Put `client_id` in `BCAST` mode with these prefixes, in place of any
    /// it had.
    pub fn set_broadcast(&self, client_id: u64, prefixes: Vec<String>, noloop: bool) {
        let mut broadcast = self.broadcast.write();
        broadcast.insert(client_id, Broadcast { prefixes, noloop });
        self.broadcasting.store(broadcast.len(), Ordering::Relaxed);
    }

    /// `key` has been written, by `writer` if by a client: note it for the
    /// `BCAST` clients it concerns. They are told when the command is over
    /// ([`take_pending`](Self::take_pending)), all of a command's keys at once.
    pub fn changed(&self, key: &str, writer: Option<u64>) {
        if self.broadcasting.load(Ordering::Relaxed) == 0 {
            return;
        }
        let broadcast = self.broadcast.read();
        let mut pending = self.pending.lock();
        for (client, mode) in broadcast.iter() {
            if mode.noloop && writer == Some(*client) {
                continue;
            }
            // Prefixes of one client do not overlap: at most one matches.
            let Some(prefix) = mode
                .prefixes
                .iter()
                .find(|prefix| key.starts_with(prefix.as_str()))
            else {
                continue;
            };
            match pending
                .iter_mut()
                .find(|p| p.client == *client && p.prefix == *prefix)
            {
                Some(entry) => {
                    if !entry.keys.iter().any(|known| known == key) {
                        entry.keys.push(key.to_string());
                    }
                }
                None => pending.push(Pending {
                    client: *client,
                    prefix: prefix.clone(),
                    keys: vec![key.to_string()],
                }),
            }
        }
        if !pending.is_empty() {
            self.has_pending.store(true, Ordering::Relaxed);
        }
    }

    /// What `BCAST` clients are waiting to be told.
    pub fn take_pending(&self) -> Vec<Pending> {
        if !self.has_pending.swap(false, Ordering::Relaxed) {
            return Vec::new();
        }
        std::mem::take(&mut *self.pending.lock())
    }

    /// Remove all tracking entries for a client (on disconnect, or when it
    /// turns tracking off).
    pub fn unregister_client(&self, client_id: u64) {
        let mut table = self.table.write();
        table.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
        drop(table);
        let mut broadcast = self.broadcast.write();
        if broadcast.remove(&client_id).is_some() {
            self.broadcasting.store(broadcast.len(), Ordering::Relaxed);
            self.pending.lock().retain(|p| p.client != client_id);
        }
    }

    pub fn enable_tracking(&self) {
        TRACKING_CLIENT_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    pub fn disable_tracking(&self) {
        TRACKING_CLIENT_COUNT.fetch_sub(1, Ordering::Relaxed);
    }
}

// ── Telling clients ───────────────────────────────────────────────────────────

type Registry = crate::client_registry::ClientRegistry;

/// Tell the client `tracker` that `keys` are no longer what it has cached —
/// every key, if `None`: the keyspace was flushed.
///
/// `writer` is the client whose command changed them, if a client's did: a
/// tracker that asked for `NOLOOP` is not told of its own writes. `current`
/// is the connection this is running on: a message for that connection is
/// held until its reply has been written, so that the two cannot arrive the
/// wrong way round; for any other it goes out at once.
///
/// The message goes to the client the tracker redirected them to, if it did.
/// To a RESP3 connection it is a push. To a RESP2 one it can only be sent as
/// a message on the invalidation channel, which takes a redirection, to a
/// connection that has subscribed; otherwise there is no way to send it.
pub fn tell(
    registry: &Registry,
    tracker: u64,
    keys: Option<&[&str]>,
    writer: Option<u64>,
    current: Option<u64>,
) {
    let Some(info) = registry.get(tracker) else {
        return;
    };
    if !info.tracking || (info.tracking_noloop && writer == Some(tracker)) {
        return;
    }
    let send = |target: &crate::client_registry::ClientInfo, frame: Vec<u8>| {
        if current == Some(target.id) {
            // What made room for the command comes before the command's
            // reply; what the command did comes after it.
            let before = BEFORE_REPLY.with_borrow_mut(|held| match held {
                Some(held) => {
                    held.extend_from_slice(&frame);
                    true
                }
                None => false,
            });
            if !before {
                target.activity.push_after_reply(&frame);
            }
        } else if let Some(tx) = &target.push_tx
            && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(frame)
        {
            // Dropped, the message would leave the client serving a key that
            // has changed, with nothing to tell it so (#161). Closing the
            // connection makes it drop what it cached, as a client does when
            // it loses its connection; Redis closes one whose output buffer
            // passed its limit the same way.
            // Throttled: until the connection is gone, every write it is told
            // of finds the queue full again.
            log_throttled!(
                10,
                crate::loglevel::WARN,
                "Client id={} closed: it uses client-side caching and did not read its \
                 invalidation messages, over {} waiting",
                target.id,
                tx.max_capacity()
            );
            registry.kill(target.id);
        }
    };
    let (target, redirected) = match info.tracking_redirect {
        None => (info, false),
        Some(redirect) => match registry.get(redirect) {
            Some(target) => (target, true),
            None => {
                // The client they were redirected to has gone. The tracker
                // is told so, if it can be, and remembers.
                registry.update(tracker, |info| info.tracking_redirect_broken = true);
                if info.resp >= 3 {
                    let frame = format!(">2\r\n$21\r\ntracking-redir-broken\r\n:{redirect}\r\n");
                    send(&info, frame.into_bytes());
                }
                return;
            }
        },
    };
    let frame = if target.resp >= 3 {
        invalidation(keys, 3)
    } else if redirected && target.subscriptions.iter().any(|n| *n > 0) {
        invalidation(keys, 2)
    } else {
        return;
    };
    send(&target, frame);
}

thread_local! {
    /// While a command is evicting keys to make room for itself: the
    /// messages that tells its own connection of, which are to be written
    /// before its reply. See [`before_the_reply`].
    static BEFORE_REPLY: std::cell::RefCell<Option<Vec<u8>>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `tell` — something that tells clients of invalid keys — with what it
/// has to say to the connection running the command put before the
/// command's reply rather than after it: the keys went before the command
/// did anything. [`with_what_came_before`] puts it there.
pub fn before_the_reply(tell: impl FnOnce()) {
    let held = BEFORE_REPLY
        .with_borrow_mut(|held| held.take())
        .unwrap_or_default();
    BEFORE_REPLY.with_borrow_mut(|slot| *slot = Some(held));
    tell();
    BEFORE_REPLY.with_borrow_mut(|slot| {
        // Kept, as not-collecting, until the reply is made.
        if slot.as_ref().is_some_and(|held| held.is_empty()) {
            *slot = None;
        } else if let Some(held) = slot.take() {
            PENDING_BEFORE.with_borrow_mut(|pending| pending.extend_from_slice(&held));
        }
    });
}

thread_local! {
    static PENDING_BEFORE: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// `reply`, preceded by whatever [`before_the_reply`] collected while the
/// command ran.
#[inline(always)]
pub fn with_what_came_before(reply: Vec<u8>) -> Vec<u8> {
    if TRACKING_CLIENT_COUNT.load(Ordering::Relaxed) == 0 {
        return reply;
    }
    preceded(reply)
}

#[cold]
fn preceded(reply: Vec<u8>) -> Vec<u8> {
    let mut before = PENDING_BEFORE.with_borrow_mut(std::mem::take);
    if before.is_empty() {
        return reply;
    }
    before.extend_from_slice(&reply);
    before
}

fn invalidation(keys: Option<&[&str]>, resp: u8) -> Vec<u8> {
    match keys {
        Some(keys) => build_invalidation_message(keys, resp),
        None if resp >= 3 => b">2\r\n$10\r\ninvalidate\r\n_\r\n".to_vec(),
        None => format!(
            "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n$-1\r\n",
            TRACKING_CHANNEL.len(),
            TRACKING_CHANNEL
        )
        .into_bytes(),
    }
}

/// `key` is not what it was: the clients that read it are told, each in its
/// own message, and the `BCAST` clients have it noted for theirs.
pub fn invalidate(
    table: &TrackingTableInner,
    registry: &Registry,
    key: &str,
    writer: Option<u64>,
    current: Option<u64>,
) {
    table.changed(key, writer);
    if let Some(clients) = table.take_clients(key) {
        for client in clients {
            tell(registry, client, Some(&[key]), writer, current);
        }
    }
}

/// The keys the table dropped to stay within its limit: their clients are
/// told, as if the keys had changed.
pub fn tell_dropped(registry: &Registry, dropped: Vec<Dropped>, current: Option<u64>) {
    for (key, clients) in dropped {
        for client in clients {
            tell(registry, client, Some(&[key.as_str()]), None, current);
        }
    }
}

/// Send the `BCAST` clients what has been noted for them.
pub fn flush_pending(table: &TrackingTableInner, registry: &Registry, current: Option<u64>) {
    for pending in table.take_pending() {
        let keys = readable(table, registry, pending.client, &pending.keys);
        if keys.is_empty() {
            continue;
        }
        // `NOLOOP` was applied when the keys were noted.
        tell(registry, pending.client, Some(&keys), None, current);
    }
}

/// The keys of `keys` that `client`'s user may read. A `BCAST` client is
/// told only of those, as in Redis 8.10: of the keys its prefixes match, the
/// ones its user is let read, by the user it is now. (They are told when the
/// command that wrote them is over, so a client that authenticates again is
/// told of what was written before under the user it was.)
fn readable<'k>(
    table: &TrackingTableInner,
    registry: &Registry,
    client: u64,
    keys: &'k [String],
) -> Vec<&'k str> {
    let all = || keys.iter().map(String::as_str).collect();
    let Some(acl) = table.acl.get() else {
        return all();
    };
    let Some(name) = registry.get(client).map(|info| info.user) else {
        return all();
    };
    let users = acl.read();
    let Some(user) = users.get(&name) else {
        return all();
    };
    if user.all_keys() {
        return all();
    }
    keys.iter()
        .filter(|key| crate::acl::may_read_key(user, key))
        .map(String::as_str)
        .collect()
}

/// The keyspace was flushed: every tracking client is told that everything
/// it has cached is invalid, and nothing is tracked any more.
pub fn invalidate_everything(
    table: &TrackingTableInner,
    registry: &Registry,
    current: Option<u64>,
) {
    table.clear();
    table.take_pending();
    for tracker in registry.tracking_ids() {
        tell(registry, tracker, None, None, current);
    }
}

/// Called by a connection when a command of its own is over: the `BCAST`
/// messages that command gave rise to are sent, and what was held for this
/// connection until its reply was written is written after it.
#[inline(always)]
pub fn after_command(
    table: &TrackingTableInner,
    registry: &Registry,
    client_id: u64,
    activity: &crate::client_registry::Activity,
    out: &mut Vec<u8>,
) {
    if TRACKING_CLIENT_COUNT.load(Ordering::Relaxed) == 0 {
        return;
    }
    tell_after_command(table, registry, client_id, activity, out);
}

#[cold]
fn tell_after_command(
    table: &TrackingTableInner,
    registry: &Registry,
    client_id: u64,
    activity: &crate::client_registry::Activity,
    out: &mut Vec<u8>,
) {
    flush_pending(table, registry, Some(client_id));
    activity.drain_after_reply(out);
}

/// The tracking table and the clients of each server in this process, by
/// its store — for a key that expires, which happens where there is no
/// connection to ask. (A process holds one server, except under test.)
static SERVERS: RwLock<Vec<(usize, std::sync::Weak<TrackingTableInner>, Registry)>> =
    RwLock::new(Vec::new());

fn store_id(store: &crate::store::Store) -> usize {
    Arc::as_ptr(store) as *const () as usize
}

/// Make `table` and `registry` the ones that hear of `store`'s keys expiring.
pub fn register_for(
    store: &crate::store::Store,
    table: &TrackingTable,
    registry: &Registry,
    acl: &crate::acl::AclStore,
) {
    let _ = table.acl.set(std::sync::Arc::clone(acl));
    let id = store_id(store);
    let mut servers = SERVERS.write();
    servers.retain(|(known, table, _)| *known != id && table.strong_count() > 0);
    servers.push((id, Arc::downgrade(table), Arc::clone(registry)));
}

/// `key` has expired and been removed: the clients caching it are told.
#[inline]
pub fn expired(store: &crate::store::Store, key: &str) {
    if TRACKING_CLIENT_COUNT.load(Ordering::Relaxed) == 0 {
        return;
    }
    let id = store_id(store);
    let server = SERVERS
        .read()
        .iter()
        .find(|(known, _, _)| *known == id)
        .and_then(|(_, table, registry)| {
            table.upgrade().map(|table| (table, Arc::clone(registry)))
        });
    if let Some((table, registry)) = server {
        invalidate(&table, &registry, key, None, None);
        flush_pending(&table, &registry, None);
    }
}

pub fn new_tracking_table() -> TrackingTable {
    Arc::new(TrackingTableInner::new())
}

#[inline]
pub fn tracking_client_count() -> usize {
    TRACKING_CLIENT_COUNT.load(Ordering::Relaxed)
}

/// Channel name Redis uses to deliver invalidations to RESP2 clients that opted
/// in via `CLIENT TRACKING ... REDIRECT <id>` (`TrackingChannelName` in tracking.c).
const TRACKING_CHANNEL: &str = "__redis__:invalidate";

/// Build an invalidation message for the given keys, shaped for `resp` — the
/// **target** client's negotiated protocol, which is not necessarily the
/// writer's. One write fans out to every client caching that key, and those
/// clients can be on different protocols, so the frame has to be built per
/// target rather than once per write.
///
/// RESP3 (`>= 3`) — a 2-element push frame, matching Redis `sendTrackingMessage`:
///
/// ```text
/// >2\r\n$10\r\ninvalidate\r\n*N\r\n$len\r\nkey\r\n...
/// ```
///
/// RESP2 — the same payload delivered as an ordinary pub/sub message on
/// `__redis__:invalidate`, which is how Redis reaches a redirected RESP2
/// client (`addReplyPubsubMessage`):
///
/// ```text
/// *3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*N\r\n$len\r\nkey\r\n...
/// ```
///
/// Both shapes are fixes for real bugs, not cosmetic parity:
///
///  * The RESP3 frame previously declared **3** elements and prepended a literal
///    `"push"` string. Redis sends `addReplyPushLen(c, 2)` with no such element,
///    so a strict RESP3 client either mis-framed the reply or blocked waiting for
///    a third element that never arrived.
///  * RESP2 targets previously received the `>` push frame too. `>` does not
///    exist in RESP2, so a redirected RESP2 client got bytes its parser cannot
///    represent — the exact configuration `REDIRECT` exists to serve.
///
/// One known deviation, called out rather than papered over: Redis only sends the
/// RESP2 form to a redirect target that is actually subscribed to the tracking
/// channel (`CLIENT_PUBSUB`). This registry delivers to any target holding a
/// `push_tx`, because subscription state isn't tracked per push channel here.
/// The effect is that an unsubscribed RESP2 redirect target may receive a
/// well-formed pub/sub message it didn't subscribe to — strictly better than the
/// previous behaviour of sending it unparseable RESP3 bytes.
pub fn build_invalidation_message(keys: &[&str], resp: u8) -> Vec<u8> {
    let mut msg = if resp >= 3 {
        b">2\r\n$10\r\ninvalidate\r\n".to_vec()
    } else {
        format!(
            "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n",
            TRACKING_CHANNEL.len(),
            TRACKING_CHANNEL
        )
        .into_bytes()
    };
    msg.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
    for key in keys {
        msg.extend_from_slice(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());
    }
    msg
}

#[cfg(test)]
mod invalidation_frame_tests {
    use super::*;

    #[test]
    fn resp3_frame_is_a_two_element_push_with_no_push_literal() {
        let msg = build_invalidation_message(&["foo"], 3);
        assert_eq!(
            msg,
            b">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n".to_vec(),
            "RESP3 invalidation must be a 2-element push frame (Redis addReplyPushLen(c,2))"
        );
        // Guards the specific regression: a literal "push" element used to be
        // emitted, with the length declared as 3.
        assert!(!msg.starts_with(b">3\r\n"));
        assert!(!msg.windows(4).any(|w| w == b"push"));
    }

    #[test]
    fn resp2_frame_is_a_pubsub_message_not_a_push_type() {
        let msg = build_invalidation_message(&["foo"], 2);
        assert_eq!(
            msg,
            b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$3\r\nfoo\r\n".to_vec(),
        );
        // `>` is not a RESP2 type — a RESP2 client cannot parse it at all.
        assert!(!msg.starts_with(b">"));
    }

    #[test]
    fn multiple_keys_are_framed_as_one_array_under_both_protocols() {
        let keys = ["a", "bb"];
        let expected_tail = b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n";

        let r3 = build_invalidation_message(&keys, 3);
        assert!(r3.ends_with(expected_tail), "RESP3 key array malformed");
        assert!(
            r3.starts_with(b">2\r\n"),
            "arity must stay 2 regardless of key count"
        );

        let r2 = build_invalidation_message(&keys, 2);
        assert!(r2.ends_with(expected_tail), "RESP2 key array malformed");
    }

    #[test]
    fn declared_channel_length_matches_the_channel_string() {
        // The RESP2 header hardcodes nothing — but the test above asserts $20,
        // so pin the constant that produces it.
        assert_eq!(TRACKING_CHANNEL.len(), 20);
    }
}
