// src/watch.rs

use ahash::AHashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::store::Key;

pub trait WatchNotifier: Send + Sync {
    /// Watch `key` in `db` for the client whose dirty flag is `flag`.
    /// `expired`: whether the key had already run out of time when it was
    /// watched (Redis's `watchedKey.expired`), a mark the registry then
    /// clears or sets as Redis does.
    fn watch(&self, db: usize, key: &[u8], flag: Arc<AtomicBool>, expired: bool);
    /// Whether that watch is marked expired now: what `EXEC` asks of a
    /// watched key it finds past its time, which a `SWAPDB` may have put
    /// there marked (`transaction::watched_key_expired`).
    fn marked_expired(&self, db: usize, key: &[u8], flag: &Arc<AtomicBool>) -> bool;
    /// `key` was modified: the watches on it are broken. Except a watch on
    /// a key that had run out of time when it was watched and is still
    /// there, as Redis's `touchWatchedKey` takes it. If the key is now gone,
    /// "Already expired key is deleted, so logically no change": the watch
    /// holds, and is no longer marked expired. If it is still there, the
    /// watch holds as it is. Only a replica keeps such a key: until its
    /// master's `DEL` comes, which must not fail the `EXEC` (#666).
    fn notify(&self, db: usize, key: &[u8]);
    /// [`notify`](Self::notify), by a caller that has just written `key` and
    /// still holds its shard: the key is there, so every watch on it is
    /// broken, and the store is not looked at.
    fn notify_written(&self, db: usize, key: &[u8]);
    fn unwatch_all(&self, flags: &[Arc<AtomicBool>]);
    /// Whether any key is being watched at all. Lets a caller skip working
    /// out which keys a write touched when nobody could care.
    fn has_watchers(&self) -> bool;
    /// A flush or `SWAPDB` of `db` (`None` = every database) replaces what
    /// each watched key is: `state` says, for a watched key, what it is
    /// before and what it is after. The watches go as Redis's
    /// `touchAllWatchedKeysInDb` takes them ([`Presence`]).
    fn notify_replaced(&self, db: Option<usize>, state: &Replaced<'_>);
}

/// What a flush or a `SWAPDB` makes of a watched key of a database: what
/// the key is before it, and what it is after
/// ([`WatchNotifier::notify_replaced`]).
pub type Replaced<'a> = dyn Fn(usize, &[u8]) -> (Presence, Presence) + 'a;

/// What a watched key is, for a flush or a `SWAPDB`. A key whose time has
/// run out is still there (Redis's `dbFind` finds it), but expired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presence {
    Absent,
    Expired,
    Live,
}

/// What `key` of database `db` of `store` is now: a look, not a use. Past
/// its time by the command's time, as Redis's `keyIsExpired` says, even
/// where expiry is held (a replica applying its master's stream, which
/// keeps such keys for its master's `DEL`).
pub fn presence(store: &crate::store::Store, db: usize, key: &[u8]) -> Presence {
    let Some(shards) = store.get(db) else {
        return Presence::Absent;
    };
    let map = crate::read_lock!(crate::store::get_shard(shards, key));
    match map.peek_expiring(key) {
        None => Presence::Absent,
        Some((_, Some(at))) if crate::commands::helpers::now_ms() > at => Presence::Expired,
        Some(_) => Presence::Live,
    }
}

/// The watch registry of each server in this process, by its store — for a
/// key that expires, which happens where there is no connection to ask (the
/// background task, a read that finds the key's time is up). A process holds
/// one server, except under test.
static REGISTRIES: RwLock<Vec<(usize, std::sync::Weak<WatchRegistryInner>)>> =
    RwLock::new(Vec::new());
/// Watches registered in any of them: with none, an expiry looks no further.
static WATCHES: AtomicUsize = AtomicUsize::new(0);

fn store_id(store: &crate::store::Store) -> usize {
    Arc::as_ptr(store) as *const () as usize
}

/// Make `registry` the one that hears about `store`'s keys expiring.
pub fn register_for(store: &crate::store::Store, registry: &WatchRegistry) {
    *registry.store.lock() = Some(Arc::downgrade(store));
    let id = store_id(store);
    let mut registries = REGISTRIES.write();
    registries.retain(|(known, registry)| *known != id && registry.strong_count() > 0);
    registries.push((id, Arc::downgrade(registry)));
}

/// `key` has expired and been removed: for a client watching it, it has
/// been modified.
#[inline]
pub fn expired(store: &crate::store::Store, db: usize, key: &(impl AsRef<[u8]> + ?Sized)) {
    expired_in(store_id(store), db, key.as_ref());
}

/// [`expired`], for the store at address `id`.
#[inline]
pub fn expired_in(id: usize, db: usize, key: &[u8]) {
    if WATCHES.load(Ordering::Relaxed) == 0 {
        return;
    }
    let registry = REGISTRIES
        .read()
        .iter()
        .find(|(known, _)| *known == id)
        .and_then(|(_, registry)| registry.upgrade());
    if let Some(registry) = registry {
        registry.notify_gone(db, key);
    }
}

/// One client's watch on one key.
struct Watch {
    /// The client's: set when a key it watches is modified (Redis's
    /// `CLIENT_DIRTY_CAS`).
    dirty: Arc<AtomicBool>,
    /// The key had run out of time when it was watched (Redis's
    /// `watchedKey.expired`).
    expired: AtomicBool,
}

impl Watch {
    fn was_expired(&self) -> bool {
        self.expired.load(Ordering::Acquire)
    }

    fn break_it(&self) {
        self.dirty.store(true, Ordering::Release);
    }
}

type WatchMap = RwLock<AHashMap<(usize, Key), Vec<Watch>>>;

pub struct WatchRegistryInner {
    inner: WatchMap,
    /// Count of currently registered watches across all connections.
    /// Zero means notify() is a no-op — checked with a single atomic load,
    /// no lock needed on the common (no-WATCH) path.
    watch_count: AtomicUsize,
    /// The store whose keys are watched ([`register_for`]): looked at only
    /// for a watch on a key that had expired when it was watched.
    store: parking_lot::Mutex<Option<std::sync::Weak<Vec<crate::store::Db>>>>,
}

impl WatchRegistryInner {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(AHashMap::new()),
            watch_count: AtomicUsize::new(0),
            store: parking_lot::Mutex::new(None),
        }
    }

    /// `key` has expired and gone: removed, or written over by a write that
    /// found it past its time, which expires it first, as in Redis
    /// (`expiry::written_over`). A watch on it is broken, unless it had
    /// expired when it was watched: then its going is no change, and the
    /// watch is no longer marked expired (Redis's `touchWatchedKey`, called
    /// by `deleteExpiredKeyAndPropagate`). The write's own change comes
    /// after, and breaks it.
    pub fn notify_gone(&self, db: usize, key: &[u8]) {
        if self.watch_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let map = self.inner.read();
        for watch in map.get(&(db, Key::new(key))).into_iter().flatten() {
            if watch.was_expired() {
                watch.expired.store(false, Ordering::Release);
            } else {
                watch.break_it();
            }
        }
    }

    /// Whether `key` is gone from the store. Without a store, it is taken
    /// to be there.
    fn gone(&self, db: usize, key: &[u8]) -> bool {
        let store = self
            .store
            .lock()
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        store.is_some_and(|store| presence(&store, db, key) == Presence::Absent)
    }
}

impl Default for WatchRegistryInner {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchNotifier for WatchRegistryInner {
    fn watch(&self, db: usize, key: &[u8], flag: Arc<AtomicBool>, expired: bool) {
        self.watch_count.fetch_add(1, Ordering::Relaxed);
        WATCHES.fetch_add(1, Ordering::Relaxed);
        self.inner
            .write()
            .entry((db, Key::new(key)))
            .or_default()
            .push(Watch {
                dirty: flag,
                expired: AtomicBool::new(expired),
            });
    }

    fn marked_expired(&self, db: usize, key: &[u8], flag: &Arc<AtomicBool>) -> bool {
        self.inner
            .read()
            .get(&(db, Key::new(key)))
            .is_some_and(|watches| {
                watches
                    .iter()
                    .any(|watch| Arc::ptr_eq(&watch.dirty, flag) && watch.was_expired())
            })
    }

    #[inline]
    fn notify(&self, db: usize, key: &[u8]) {
        if self.watch_count.load(Ordering::Relaxed) == 0 {
            return; // zero-cost on the common path
        }
        let id = (db, Key::new(key));
        {
            let map = self.inner.read();
            let Some(watches) = map.get(&id) else {
                return;
            };
            if !watches.iter().any(Watch::was_expired) {
                watches.iter().for_each(Watch::break_it);
                return;
            }
        }
        // A watch on a key that had expired when it was watched: whether the
        // key is gone decides. The store is looked at with the registry let
        // go of: a writer holding a shard may be waiting for the registry
        // (`notify_written`).
        let gone = self.gone(db, key);
        let map = self.inner.read();
        for watch in map.get(&id).into_iter().flatten() {
            if watch.was_expired() {
                if gone {
                    // "Deleted keys are not flagged as expired."
                    watch.expired.store(false, Ordering::Release);
                }
                // Still there: the replica's master wrote the key without a
                // `DEL` first (inside a transaction or a script whose time
                // still had it alive). Redis leaves the watch as it is, and
                // the `EXEC` runs. Redis also stops looking (`break`), which
                // leaves every later watch on the key unbroken, even one set
                // on the key while it was alive; those are still broken here.
                continue;
            }
            watch.break_it();
        }
    }

    #[inline]
    fn notify_written(&self, db: usize, key: &[u8]) {
        if self.watch_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let map = self.inner.read();
        if let Some(watches) = map.get(&(db, Key::new(key))) {
            watches.iter().for_each(Watch::break_it);
        }
    }

    #[inline]
    fn has_watchers(&self) -> bool {
        self.watch_count.load(Ordering::Relaxed) != 0
    }

    fn notify_replaced(&self, db: Option<usize>, state: &Replaced<'_>) {
        if !self.has_watchers() {
            return;
        }
        for ((watched_db, key), watches) in self.inner.read().iter() {
            if db.is_some_and(|db| db != *watched_db) {
                continue;
            }
            let (before, after) = state(*watched_db, key);
            if before == Presence::Absent && after == Presence::Absent {
                continue;
            }
            for watch in watches {
                if watch.was_expired() {
                    match after {
                        // "Expired key now deleted. No logical change."
                        Presence::Absent => {
                            watch.expired.store(false, Ordering::Release);
                            continue;
                        }
                        // "Expired key remains expired."
                        Presence::Expired => continue,
                        Presence::Live => {}
                    }
                } else if before == Presence::Absent && after == Presence::Expired {
                    // "Non-existing key is replaced with an expired key."
                    watch.expired.store(true, Ordering::Release);
                    continue;
                }
                watch.break_it();
            }
        }
    }

    fn unwatch_all(&self, flags: &[Arc<AtomicBool>]) {
        if flags.is_empty() {
            return;
        }
        let removed = {
            let mut map = self.inner.write();
            let mut count = 0usize;
            map.retain(|_, watchers| {
                let before = watchers.len();
                watchers.retain(|w| !flags.iter().any(|f| Arc::ptr_eq(&w.dirty, f)));
                count += before - watchers.len();
                !watchers.is_empty()
            });
            count
        };
        let mut all = WATCHES.load(Ordering::Relaxed);
        while let Err(seen) = WATCHES.compare_exchange_weak(
            all,
            all.saturating_sub(removed),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            all = seen;
        }
        // Saturating so a bug can't wrap the counter negative.
        let mut current = self.watch_count.load(Ordering::Relaxed);
        while let Err(seen) = self.watch_count.compare_exchange_weak(
            current,
            current.saturating_sub(removed),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            current = seen;
        }
    }
}

pub type WatchRegistry = Arc<WatchRegistryInner>;

pub fn new_watch_registry() -> WatchRegistry {
    Arc::new(WatchRegistryInner::new())
}

/// The watches of the server whose store is `.0`, for what writes to it
/// without a connection of its own: a replica applying its master's stream.
/// A client watching a key on the replica sees the master's writes to it
/// (#159); it saw none.
pub struct ForStore<'a>(pub &'a crate::store::Store);

impl ForStore<'_> {
    fn registry(&self) -> Option<WatchRegistry> {
        if WATCHES.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let id = store_id(self.0);
        REGISTRIES
            .read()
            .iter()
            .find(|(known, _)| *known == id)
            .and_then(|(_, registry)| registry.upgrade())
    }
}

impl WatchNotifier for ForStore<'_> {
    fn watch(&self, _db: usize, _key: &[u8], _flag: Arc<AtomicBool>, _expired: bool) {}
    fn marked_expired(&self, _db: usize, _key: &[u8], _flag: &Arc<AtomicBool>) -> bool {
        false
    }
    fn notify(&self, db: usize, key: &[u8]) {
        if let Some(registry) = self.registry() {
            registry.notify(db, key);
        }
    }
    fn notify_written(&self, db: usize, key: &[u8]) {
        if let Some(registry) = self.registry() {
            registry.notify_written(db, key);
        }
    }
    fn unwatch_all(&self, _flags: &[Arc<AtomicBool>]) {}
    fn has_watchers(&self) -> bool {
        WATCHES.load(Ordering::Relaxed) > 0
    }
    fn notify_replaced(&self, db: Option<usize>, state: &Replaced<'_>) {
        if let Some(registry) = self.registry() {
            registry.notify_replaced(db, state);
        }
    }
}

// ── No-op implementation — used by replication ────────────────────────────

pub struct NoopWatchRegistry;

impl WatchNotifier for NoopWatchRegistry {
    fn watch(&self, _db: usize, _key: &[u8], _flag: Arc<AtomicBool>, _expired: bool) {}
    fn marked_expired(&self, _db: usize, _key: &[u8], _flag: &Arc<AtomicBool>) -> bool {
        false
    }
    fn notify(&self, _db: usize, _key: &[u8]) {}
    fn notify_written(&self, _db: usize, _key: &[u8]) {}
    fn unwatch_all(&self, _flags: &[Arc<AtomicBool>]) {}
    fn has_watchers(&self) -> bool {
        false
    }
    fn notify_replaced(&self, _db: Option<usize>, _state: &Replaced<'_>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    use Presence::{Absent, Expired, Live};

    fn flag(set: bool) -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(set))
    }

    /// A flush: the key is what `before` says, and is then gone.
    fn flushed(before: Presence) -> impl Fn(usize, &[u8]) -> (Presence, Presence) {
        move |_, _| (before, Absent)
    }

    #[test]
    fn a_flush_invalidates_the_watches_on_present_keys_of_that_database() {
        let reg = WatchRegistryInner::new();
        assert!(!reg.has_watchers());
        let (a, b) = (flag(false), flag(false));
        reg.watch(0, b"k", Arc::clone(&a), false);
        reg.watch(1, b"k", Arc::clone(&b), false);
        assert!(reg.has_watchers());

        reg.notify_replaced(Some(1), &flushed(Live));
        assert!(!a.load(Ordering::Acquire));
        assert!(b.load(Ordering::Acquire));
        reg.notify_replaced(None, &|db, _| (if db == 1 { Live } else { Absent }, Absent));
        assert!(!a.load(Ordering::Acquire));
        reg.notify_replaced(None, &flushed(Live));
        assert!(a.load(Ordering::Acquire));
    }

    /// Redis's `touchAllWatchedKeysInDb`, watch by watch: a key whose time
    /// ran out after it was watched is modified by its flush; one that had
    /// run out when it was watched is not, and is no longer marked so.
    #[test]
    fn a_flush_or_swap_takes_each_watch_by_whether_its_key_had_expired_when_watched() {
        let reg = WatchRegistryInner::new();
        let (live, was) = (flag(false), flag(false));
        reg.watch(0, b"k", Arc::clone(&live), false);
        reg.watch(0, b"k", Arc::clone(&was), true);

        // Swapped for another expired key: no change for the expired watch.
        reg.notify_replaced(None, &|_, _| (Expired, Expired));
        assert!(live.load(Ordering::Acquire));
        assert!(!was.load(Ordering::Acquire));
        assert!(reg.marked_expired(0, b"k", &was));

        // Flushed: the expired key is gone, which is no change either.
        reg.notify_replaced(None, &flushed(Expired));
        assert!(!was.load(Ordering::Acquire));
        assert!(!reg.marked_expired(0, b"k", &was));

        // A key that was not there, swapped for an expired one, is marked
        // expired, and the watch holds.
        let absent = flag(false);
        reg.watch(0, b"a", Arc::clone(&absent), false);
        reg.notify_replaced(None, &|_, key| {
            if key == b"a" {
                (Absent, Expired)
            } else {
                (Absent, Absent)
            }
        });
        assert!(!absent.load(Ordering::Acquire));
        assert!(reg.marked_expired(0, b"a", &absent));
        // Then swapped for a live key: modified.
        reg.notify_replaced(None, &|_, key| {
            if key == b"a" {
                (Expired, Live)
            } else {
                (Absent, Absent)
            }
        });
        assert!(absent.load(Ordering::Acquire));
    }

    /// Redis's `touchWatchedKey`: a write to a key that had expired when it
    /// was watched, and is still there (without a store to look at, a key is
    /// taken to be there), leaves that watch as it is; the key's going
    /// clears its mark; a write after that breaks it. A watch on a key that
    /// was alive is broken by either.
    #[test]
    fn a_watch_on_an_expired_key_holds_until_the_key_is_gone_and_written_again() {
        let reg = WatchRegistryInner::new();
        let (live, was) = (flag(false), flag(false));
        reg.watch(0, b"k", Arc::clone(&live), false);
        reg.watch(0, b"k", Arc::clone(&was), true);
        reg.notify(0, b"k");
        assert!(live.load(Ordering::Acquire));
        assert!(!was.load(Ordering::Acquire));
        assert!(reg.marked_expired(0, b"k", &was));
        reg.notify_gone(0, b"k");
        assert!(!was.load(Ordering::Acquire));
        assert!(!reg.marked_expired(0, b"k", &was));
        reg.notify(0, b"k");
        assert!(was.load(Ordering::Acquire));
    }
}

thread_local! {
    /// Keys the command now running names and has left exactly as they were.
    static UNCHANGED: std::cell::RefCell<Vec<(usize, Key)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// A command says that `key`, which it names, is as it was: the dispatcher,
/// which marks the watches on every key a successful write names, leaves
/// this one's alone. (`SMOVE` of a member the destination already has.)
pub fn unchanged(db: usize, key: &(impl AsRef<[u8]> + ?Sized)) {
    UNCHANGED.with(|keys| keys.borrow_mut().push((db, Key::new(key.as_ref()))));
}

/// Whether the running command said so of `key`.
pub fn is_unchanged(db: usize, key: &[u8]) -> bool {
    UNCHANGED.with(|keys| {
        let keys = keys.borrow();
        !keys.is_empty() && keys.iter().any(|(d, k)| *d == db && *k == key)
    })
}

/// The command is over.
pub fn clear_unchanged() {
    UNCHANGED.with(|keys| {
        let mut keys = keys.borrow_mut();
        if !keys.is_empty() {
            keys.clear();
        }
    });
}
