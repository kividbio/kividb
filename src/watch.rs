// src/watch.rs

use ahash::AHashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub trait WatchNotifier: Send + Sync {
    fn watch(&self, db: usize, key: &str, flag: Arc<AtomicBool>);
    fn notify(&self, db: usize, key: &str);
    fn unwatch_all(&self, flags: &[Arc<AtomicBool>]);
    /// Whether any key is being watched at all. Lets a caller skip working
    /// out which keys a write touched when nobody could care.
    fn has_watchers(&self) -> bool;
    /// Invalidate every watch in `db` (`None` = every database): the whole
    /// keyspace changed under them (`SWAPDB`).
    fn notify_db(&self, db: Option<usize>);
    /// Invalidate the watches in `db` (`None` = every database) on keys for
    /// which `exists` says yes: those are the keys a flush removes.
    fn notify_existing(&self, db: Option<usize>, exists: &dyn Fn(usize, &str) -> bool);
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
    let id = store_id(store);
    let mut registries = REGISTRIES.write();
    registries.retain(|(known, registry)| *known != id && registry.strong_count() > 0);
    registries.push((id, Arc::downgrade(registry)));
}

/// `key` has expired and been removed: for a client watching it, it has
/// been modified.
#[inline]
pub fn expired(store: &crate::store::Store, db: usize, key: &str) {
    if WATCHES.load(Ordering::Relaxed) == 0 {
        return;
    }
    let id = store_id(store);
    let registry = REGISTRIES
        .read()
        .iter()
        .find(|(known, _)| *known == id)
        .and_then(|(_, registry)| registry.upgrade());
    if let Some(registry) = registry {
        registry.notify(db, key);
    }
}

type WatchMap = RwLock<AHashMap<(usize, String), Vec<Arc<AtomicBool>>>>;

pub struct WatchRegistryInner {
    inner: WatchMap,
    /// Count of currently registered watches across all connections.
    /// Zero means notify() is a no-op — checked with a single atomic load,
    /// no lock needed on the common (no-WATCH) path.
    watch_count: AtomicUsize,
}

impl WatchRegistryInner {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(AHashMap::new()),
            watch_count: AtomicUsize::new(0),
        }
    }
}

impl Default for WatchRegistryInner {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchNotifier for WatchRegistryInner {
    fn watch(&self, db: usize, key: &str, flag: Arc<AtomicBool>) {
        self.watch_count.fetch_add(1, Ordering::Relaxed);
        WATCHES.fetch_add(1, Ordering::Relaxed);
        self.inner
            .write()
            .entry((db, key.to_string()))
            .or_default()
            .push(flag);
    }

    #[inline]
    fn notify(&self, db: usize, key: &str) {
        if self.watch_count.load(Ordering::Relaxed) == 0 {
            return; // zero-cost on the common path
        }
        let map = self.inner.read();
        if let Some(flags) = map.get(&(db, key.to_string())) {
            for f in flags {
                f.store(true, Ordering::Release);
            }
        }
    }

    #[inline]
    fn has_watchers(&self) -> bool {
        self.watch_count.load(Ordering::Relaxed) != 0
    }

    fn notify_db(&self, db: Option<usize>) {
        if !self.has_watchers() {
            return;
        }
        for ((watched_db, _), flags) in self.inner.read().iter() {
            if db.is_none_or(|db| db == *watched_db) {
                for f in flags {
                    f.store(true, Ordering::Release);
                }
            }
        }
    }

    fn notify_existing(&self, db: Option<usize>, exists: &dyn Fn(usize, &str) -> bool) {
        if !self.has_watchers() {
            return;
        }
        for ((watched_db, key), flags) in self.inner.read().iter() {
            if db.is_none_or(|db| db == *watched_db) && exists(*watched_db, key) {
                for f in flags {
                    f.store(true, Ordering::Release);
                }
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
                watchers.retain(|w| !flags.iter().any(|f| Arc::ptr_eq(w, f)));
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
    fn watch(&self, _db: usize, _key: &str, _flag: Arc<AtomicBool>) {}
    fn notify(&self, db: usize, key: &str) {
        if let Some(registry) = self.registry() {
            registry.notify(db, key);
        }
    }
    fn unwatch_all(&self, _flags: &[Arc<AtomicBool>]) {}
    fn has_watchers(&self) -> bool {
        WATCHES.load(Ordering::Relaxed) > 0
    }
    fn notify_db(&self, db: Option<usize>) {
        if let Some(registry) = self.registry() {
            registry.notify_db(db);
        }
    }
    fn notify_existing(&self, db: Option<usize>, exists: &dyn Fn(usize, &str) -> bool) {
        if let Some(registry) = self.registry() {
            registry.notify_existing(db, exists);
        }
    }
}

// ── No-op implementation — used by replication ────────────────────────────

pub struct NoopWatchRegistry;

impl WatchNotifier for NoopWatchRegistry {
    fn watch(&self, _db: usize, _key: &str, _flag: Arc<AtomicBool>) {}
    fn notify(&self, _db: usize, _key: &str) {}
    fn unwatch_all(&self, _flags: &[Arc<AtomicBool>]) {}
    fn has_watchers(&self) -> bool {
        false
    }
    fn notify_db(&self, _db: Option<usize>) {}
    fn notify_existing(&self, _db: Option<usize>, _exists: &dyn Fn(usize, &str) -> bool) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_flush_invalidates_every_watch_in_that_database() {
        let reg = WatchRegistryInner::new();
        assert!(!reg.has_watchers());
        let (a, b) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        reg.watch(0, "k", Arc::clone(&a));
        reg.watch(1, "k", Arc::clone(&b));
        assert!(reg.has_watchers());

        reg.notify_db(Some(1));
        assert!(!a.load(Ordering::Acquire));
        assert!(b.load(Ordering::Acquire));
        reg.notify_db(None);
        assert!(a.load(Ordering::Acquire));
    }
}

thread_local! {
    /// Keys the command now running names and has left exactly as they were.
    static UNCHANGED: std::cell::RefCell<Vec<(usize, String)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// A command says that `key`, which it names, is as it was: the dispatcher,
/// which marks the watches on every key a successful write names, leaves
/// this one's alone. (`SMOVE` of a member the destination already has.)
pub fn unchanged(db: usize, key: &str) {
    UNCHANGED.with(|keys| keys.borrow_mut().push((db, key.to_string())));
}

/// Whether the running command said so of `key`.
pub fn is_unchanged(db: usize, key: &str) -> bool {
    UNCHANGED.with(|keys| {
        let keys = keys.borrow();
        !keys.is_empty() && keys.iter().any(|(d, k)| *d == db && k == key)
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
