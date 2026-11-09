// src/field_expiry.rs
//
// Hash fields with a TTL expire when their time comes — not when somebody
// next looks at them — and only a master removes them.
//
// This keeps the list of hashes that have field TTLs, with the earliest time
// one of each is due, and the background expiry task (`helpers::
// spawn_active_expire_task`) calls [`sweep`] ten times a second.
//
// Every removal of a field for its deadline goes through [`expire_due`]:
// the sweep's, a read's, and a write's that finds one before it writes. As a
// key's expiry (`expiry`), it holds what a write holds, removes the fields
// due, the hash if that empties it, and propagates an `HDEL` (or the hash's
// `DEL`/`UNLINK`) before anything else is written to the key. A command that
// finds a field due does so before its own record, so `HINCRBY h f 1` on an
// expired `f` reaches replicas and the AOF as `HDEL h f`, `HINCRBY h f 1`.
//
// Nothing else removes a field for its time ([`policy`]): not a dataset
// being loaded, not the AOF replayed, not a replica of a master that sends
// these deletions, which hides a field past its deadline from its readers
// and waits for the `HDEL`. A replica of a master that sends none (KiviDB
// 1.2.0 and before) removes its fields itself, as every replica did then.

use crate::store::ValueRef;
use ahash::AHashMap;
use parking_lot::Mutex;
use std::time::Instant;

use crate::store::Store;
use crate::vector::IndexStore;

/// A hash with field TTLs: which server's store it is in, which database,
/// and its key.
type Entry = (usize, usize, crate::store::Key);

/// The hashes to watch, each with the earliest time one of its fields is due.
static WATCHED: Mutex<Option<AHashMap<Entry, Instant>>> = Mutex::new(None);

/// How many hashes [`WATCHED`] holds, of every store: stored under its lock
/// after each change to it (see [`any_watched`]).
static WATCHED_LEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Whether any hash is watched. While none is, no sweep is part way through
/// one: a sweep's hash stays on the list until the sweep's last write to it,
/// done under the hash's write order, has been made ([`settle_watch`]). A
/// write applied from the master's stream takes its keys' order while one
/// is, and when it may watch one itself, which it does before its own last
/// write to the hash (see `handler::run_command`, #371).
#[inline]
pub fn any_watched() -> bool {
    #[cfg(test)]
    if let Some(id) = WATCH_SCOPE.with(|scope| scope.get()) {
        return WATCHED
            .lock()
            .as_ref()
            .is_some_and(|watched| watched.keys().any(|(store, _, _)| *store == id));
    }
    WATCHED_LEN.load(std::sync::atomic::Ordering::Acquire) > 0
}

#[cfg(test)]
thread_local! {
    /// The store [`any_watched`] answers for on this thread, if a test said
    /// so ([`WatchScope`]): a server has one store, and a test's should not
    /// count the watches other tests left in theirs.
    static WATCH_SCOPE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// For a test: on this thread, [`any_watched`] answers for `store` alone
/// until this is dropped.
#[cfg(test)]
pub(crate) struct WatchScope;

#[cfg(test)]
impl WatchScope {
    pub(crate) fn of(store: &Store) -> Self {
        let id = store_id(store);
        WATCH_SCOPE.with(|scope| scope.set(Some(id)));
        Self
    }
}

#[cfg(test)]
impl Drop for WatchScope {
    fn drop(&mut self) {
        WATCH_SCOPE.with(|scope| scope.set(None));
    }
}

/// A test's [`AfterRemoval`] hook.
#[cfg(test)]
type RemovalHook = Box<dyn FnMut(&[u8])>;

#[cfg(test)]
thread_local! {
    /// For a test: run on this thread between the removal of a hash's due
    /// fields and the propagation of their `HDEL`, with the hash's key
    /// ([`AfterRemoval`]). The moment another write to the hash must not
    /// fall into.
    static AFTER_REMOVAL: std::cell::RefCell<Option<RemovalHook>> =
        const { std::cell::RefCell::new(None) };
}

/// For a test: `hook` runs on this thread after each removal of due fields
/// and before their `HDEL` is sent, until this is dropped.
#[cfg(test)]
pub(crate) struct AfterRemoval;

#[cfg(test)]
impl AfterRemoval {
    pub(crate) fn run(hook: impl FnMut(&[u8]) + 'static) -> Self {
        AFTER_REMOVAL.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
        Self
    }
}

#[cfg(test)]
impl Drop for AfterRemoval {
    fn drop(&mut self) {
        AFTER_REMOVAL.with(|slot| *slot.borrow_mut() = None);
    }
}

fn store_id(store: &Store) -> usize {
    std::sync::Arc::as_ptr(store) as *const () as usize
}

/// What a hash's place on the watch list takes, for a key of `key_len`
/// bytes (`MEMORY USAGE`): its bucket and control byte in the list's
/// table, and its own copy of the key (`to_string`, exactly as long as the
/// key). Not the table's empty room: that is no one hash's, and deleting a
/// hash does not give it back (the table does not shrink).
pub fn watch_entry_size(key_len: usize) -> usize {
    std::mem::size_of::<(Entry, Instant)>() + 1 + key_len
}

/// Note that the hash at `key` has a field due at `at`.
pub fn track(store: &Store, db: usize, key: &(impl AsRef<[u8]> + ?Sized), at: Instant) {
    let mut watched = WATCHED.lock();
    let watched = watched.get_or_insert_with(AHashMap::new);
    let earliest = watched
        .entry((store_id(store), db, crate::store::Key::new(key.as_ref())))
        .or_insert(at);
    if at < *earliest {
        *earliest = at;
    }
    WATCHED_LEN.store(watched.len(), std::sync::atomic::Ordering::Release);
}

/// The keys of database `db` that hold a hash with field TTLs. Reading such
/// a hash can drop a field that is past its time before the sweep gets to
/// it, and the indexes hear of it only from the sweep: a query that answers
/// from postings tries these documents themselves (see
/// `commands::search::plan`). Every query that answers from postings asks:
/// while no hash is watched, without the lock every one of them took.
pub fn watched_keys(store: &Store, db: usize) -> Vec<crate::store::Key> {
    if !any_watched() {
        return Vec::new();
    }
    let id = store_id(store);
    let watched = WATCHED.lock();
    let Some(watched) = watched.as_ref() else {
        return Vec::new();
    };
    watched
        .keys()
        .filter(|(store, d, _)| *store == id && *d == db)
        .map(|(_, _, key)| key.clone())
        .collect()
}

/// Look at `key` and, if it is a hash with field TTLs, [`track`] it. For the
/// commands that put such a hash under a key: the field-TTL commands
/// themselves, and `RENAME`, `COPY`, `RESTORE`, `MOVE`.
pub fn track_key(store: &Store, db: usize, key: &(impl AsRef<[u8]> + ?Sized)) {
    let key = key.as_ref();
    let Some(db_ref) = store.get(db) else {
        return;
    };
    let earliest = match crate::store::get_shard(db_ref, key).read().get(key) {
        Some(ValueRef::HashEx(hash)) => hash.read().expiries.values().min().copied(),
        _ => None,
    };
    if let Some(at) = earliest {
        track(store, db, key, at);
    }
}

/// [`track_key`] for every key of the store — after `SWAPDB`, where what is
/// tracked is under the wrong database.
pub fn rescan(store: &Store) {
    for (db, db_ref) in store.iter().enumerate() {
        for shard in db_ref.iter() {
            let due: Vec<(crate::store::Key, Instant)> = shard
                .read()
                .iter()
                .filter_map(|(key, value)| match value {
                    ValueRef::HashEx(hash) => hash
                        .read()
                        .expiries
                        .values()
                        .min()
                        .map(|at| (crate::store::Key::new(key), *at)),
                    _ => None,
                })
                .collect();
            for (key, at) in due {
                track(store, db, &key, at);
            }
        }
    }
}

/// What may be done here about a hash field past its deadline.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    /// Removed, and its removal propagated ([`expire_due`]).
    Expire,
    /// Left in place and hidden from readers: a replica waiting for its
    /// master's `HDEL`, or a master whose expiry is paused or whose stream a
    /// shutdown has ended. A write finds the field as it is.
    Hide,
    /// Left in place and seen: a dataset being loaded, the AOF replayed, the
    /// stream of a master that sends these deletions applied.
    Keep,
}

/// [`Policy`] of this thread, for `store`. Only asked once a field is due.
#[cold]
pub fn policy(store: &Store) -> Policy {
    if crate::expiry::fields_held() || crate::expiry::replaying() {
        return Policy::Keep;
    }
    match crate::expiry::of(store) {
        Some(target) if target.on_replica_fields() || target.stream_closed() => Policy::Hide,
        _ if crate::pause::expiry_deferred() || crate::expiry::loading() => Policy::Hide,
        _ => Policy::Expire,
    }
}

/// What [`expire_due`] did.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expired {
    /// Fields removed.
    pub fields: usize,
    /// The hash was left empty and taken out of the keyspace.
    pub key_removed: bool,
}

/// Remove the fields of the hash at `key` whose deadline is at or before
/// `now`, as [`policy`] `Expire` says to; the hash too if that empties it.
///
/// For a command: called with no lock of the key's shard or of the hash
/// held. Inside a command the write gate and the key's order are held
/// already and are not taken again; outside one (a read), they are taken
/// here, so that the `HDEL` falls where the removal did among the writes to
/// the key, and a cut does not fall between the two. A command that holds
/// the key's order whole holds it until its own record is queued, after
/// this one's.
#[cold]
#[inline(never)]
pub fn expire_due(
    store: &Store,
    index_store: &IndexStore,
    db: usize,
    key: &[u8],
    now: Instant,
) -> Expired {
    let target = crate::expiry::of(store);
    let told = target.as_ref().filter(|target| target.observed());
    let ordered = told.map(|_| crate::expiry::order(db, std::iter::once(key)));
    let unobserved =
        crate::commands::helpers::unobserved_order(told.is_none(), db, std::iter::once(key));
    if ordered.is_some() && crate::pause::expiry_deferred() {
        return Expired::default();
    }
    let (expired, _) = expire_ordered(store, index_store, db, key, now, told);
    drop(unobserved);
    drop(ordered);
    after_expiry(store, db, key, &expired);
    expired
}

/// The removal itself, holding what [`expire_due`] or the sweep takes: the
/// write gate and the key's order. Returns what was removed, and the
/// earliest deadline the hash still has.
fn expire_ordered(
    store: &Store,
    index_store: &IndexStore,
    db: usize,
    key: &[u8],
    now: Instant,
    told: Option<&crate::expiry::Target>,
) -> (Expired, Option<Instant>) {
    let Some(db_ref) = store.get(db) else {
        return (Expired::default(), None);
    };
    let id = crate::vector::store_id(store);
    let marks = crate::vector::has_index_of(index_store, db);
    let (fields, next, taken) = {
        let mut map = crate::store::get_shard(db_ref, key).write();
        let hash = match map.get(key) {
            Some(ValueRef::HashEx(hash)) => std::sync::Arc::clone(hash),
            _ => return (Expired::default(), None),
        };
        let mut inner = hash.write();
        let fields: Vec<String> = inner
            .expiries
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(field, _)| field.clone())
            .collect();
        for field in &fields {
            inner.evict_field(field);
        }
        let next = inner.expiries.values().min().copied();
        let emptied = !fields.is_empty() && inner.fields.is_empty();
        drop(inner);
        let taken = if emptied { map.remove(key) } else { None };
        if taken.is_some() && marks {
            crate::vector::mark_leaving(id, db, key);
        }
        (fields, next, taken)
    };
    if fields.is_empty() {
        return (Expired::default(), next);
    }
    #[cfg(test)]
    AFTER_REMOVAL.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(key);
        }
    });
    let key_removed = taken.is_some();
    if let Some(told) = told {
        if key_removed {
            told.propagate(db, [key]);
        } else {
            told.propagate_fields(db, key, fields.iter().map(String::as_str));
        }
    }
    if let Some(hub) = crate::keyspace_events::hub_for(store) {
        use crate::keyspace_events::{KS_GENERIC, KS_HASH, notify, notify_subkeys};
        notify(&hub, db, KS_HASH, "hexpired", key);
        notify_subkeys(
            &hub,
            db,
            "hexpired",
            key,
            fields.iter().map(|f| f.as_bytes()),
        );
        if key_removed {
            notify(&hub, db, KS_GENERIC, "del", key);
        }
    }
    // The indexes under the key's order, as a command's write: out of it a
    // write that came between could be written over with what this read.
    if key_removed {
        crate::vector::maybe_unindex(index_store, db, key);
        if marks {
            crate::vector::unmark_leaving(id, db, key);
        }
    } else {
        crate::vector::reindex_key(index_store, store, db, key, crate::vector::Write::Refresh);
    }
    // Freed outside the shard's lock.
    drop(taken);
    let expired = Expired {
        fields: fields.len(),
        key_removed,
    };
    (expired, if key_removed { None } else { next })
}

/// What follows a removal once the key's order is let go of: clients
/// watching or caching the key told, and the fields counted.
fn after_expiry(store: &Store, db: usize, key: &[u8], expired: &Expired) {
    if expired.fields == 0 {
        return;
    }
    let id = store_id(store);
    crate::watch::expired_in(id, db, key);
    crate::tracking::expired_in(id, key);
    for _ in 0..expired.fields {
        crate::stats::record_expired_subkey();
    }
}

/// Expire the fields that are due, in every hash of `store` that is being
/// watched.
pub fn sweep(store: &Store, index_store: &IndexStore) {
    let id = store_id(store);
    let now = Instant::now();
    let due: Vec<(usize, crate::store::Key)> = {
        let watched = WATCHED.lock();
        let Some(watched) = watched.as_ref() else {
            return;
        };
        watched
            .iter()
            .filter(|((store, _, _), at)| *store == id && **at <= now)
            .map(|((_, db, key), _)| (*db, key.clone()))
            .collect()
    };
    if due.is_empty() {
        return;
    }
    let target = crate::expiry::of(store);
    if target
        .as_ref()
        .is_some_and(crate::expiry::Target::on_replica_fields)
    {
        settle_on_replica(store, id, due, now);
        return;
    }
    let told = target.as_ref().filter(|target| target.observed());

    for (db, key) in due {
        // In the order a command takes them: the command gate, the write
        // gate, the key's tag, the key's order (see `helpers::expire_batch`).
        let command = crate::command_gate::enter();
        let gate = told.map(|_| crate::write_gate::enter());
        // A key whose tag a script holds is left for the next sweep: waiting
        // for it, holding the write gate, could wait behind a cut that waits
        // for this.
        let tags = if crate::tag_locks::enabled() {
            crate::tag_locks::try_lock_keys(std::iter::once(key.as_bytes()))
        } else {
            None
        };
        if tags.as_ref().is_some_and(|tags| tags.left_some()) {
            continue;
        }
        let ordered = told.map(|_| crate::expiry::order(db, std::iter::once(key.as_bytes())));
        let unobserved = crate::commands::helpers::unobserved_order(
            told.is_none(),
            db,
            std::iter::once(key.as_bytes()),
        );
        if crate::expiry::loading() || (ordered.is_some() && crate::pause::expiry_deferred()) {
            continue;
        }
        let (expired, next) = expire_ordered(store, index_store, db, &key, now, told);
        // Nothing found to expire: a reindex all the same, under the order,
        // as the watch is settled below and queries stop trying the hash
        // themselves (see `watched_keys`).
        if expired.fields == 0 {
            crate::vector::reindex_key(index_store, store, db, &key, crate::vector::Write::Refresh);
        }
        #[cfg(test)]
        crate::test_hooks::at("sweep: watch");
        // Under the order too: an `HPEXPIRE` beside this tracks its field
        // onto the watch, which must not then be dropped.
        settle_watch(id, db, &key, next, now);
        drop(unobserved);
        drop(ordered);
        drop(tags);
        drop(gate);
        drop(command);
        after_expiry(store, db, &key, &expired);
    }
}

/// A replica that waits for its master's `HDEL`s: the watch of each hash in
/// `due` kept while it has a deadline, looked at again a second from now
/// when that is past; dropped once it has none. Nothing is removed.
fn settle_on_replica(store: &Store, id: usize, due: Vec<(usize, crate::store::Key)>, now: Instant) {
    let again = now + std::time::Duration::from_secs(1);
    for (db, key) in due {
        let next = store.get(db).and_then(|db_ref| {
            match crate::store::get_shard(db_ref, &key).read().get(&key) {
                Some(ValueRef::HashEx(hash)) => hash.read().expiries.values().min().copied(),
                _ => None,
            }
        });
        settle_watch(id, db, &key, next.map(|at| at.max(again)), now);
    }
}

/// Remove, telling no one, the hash fields of `store` whose deadline was
/// before `before` (milliseconds since the Unix epoch); returns how many.
/// What a replica does at a partial resync with a master that sends the
/// deletion of every field it expires, for the fields none will send one
/// of (`ReplState::field_expiry_sweep_owed`).
pub fn expire_fields_due_before(store: &Store, index_store: &IndexStore, before: u64) -> usize {
    let ago = crate::commands::helpers::now_ms().saturating_sub(before);
    let Some(cutoff) = Instant::now().checked_sub(std::time::Duration::from_millis(ago)) else {
        return 0;
    };
    let mut removed = 0;
    for (db, db_ref) in store.iter().enumerate() {
        for shard in db_ref.iter() {
            let keys: Vec<crate::store::Key> = shard
                .read()
                .iter()
                .filter_map(|(key, value)| match value {
                    ValueRef::HashEx(hash) if hash.read().has_due(cutoff) => {
                        Some(crate::store::Key::new(key))
                    }
                    _ => None,
                })
                .collect();
            for key in keys {
                let _command = crate::command_gate::enter();
                let _order = crate::write_order::lock_keys(db, std::iter::once(key.as_bytes()));
                let (expired, _) = expire_ordered(store, index_store, db, &key, cutoff, None);
                removed += expired.fields;
                drop(_order);
                after_expiry(store, db, &key, &expired);
            }
        }
    }
    removed
}

/// What the sweep of `key`, which began at `now`, leaves of its watch: kept
/// for `next`, the time its next field is due, or dropped. Under the key's
/// write order.
fn settle_watch(id: usize, db: usize, key: &[u8], next: Option<Instant>, now: Instant) {
    let mut watched = WATCHED.lock();
    if let Some(watched) = watched.as_mut() {
        let entry = (id, db, crate::store::Key::new(key));
        match next {
            // Due sooner than the field this found, if a time to live was
            // given before this took the order: keep the earlier.
            Some(at) => {
                let current = watched.entry(entry).or_insert(at);
                if *current <= now || at < *current {
                    *current = at;
                }
            }
            None => {
                watched.remove(&entry);
            }
        }
        WATCHED_LEN.store(watched.len(), std::sync::atomic::Ordering::Release);
    }
}

/// For a test: the watches of its store, and only those. The list is keyed
/// by a store's address, and a store freed by another test may have had the
/// same one: what it left watched is forgotten when this is made, and what
/// this store leaves when this is dropped. Its thread is
/// [`crate::expiry::Undisturbed`] meanwhile: no other test's load or pause
/// keeps its fields past their deadlines.
#[cfg(test)]
pub(crate) struct OwnWatches(usize, crate::expiry::Undisturbed);

#[cfg(test)]
impl OwnWatches {
    pub(crate) fn of(store: &Store) -> Self {
        let id = store_id(store);
        forget_watches(id);
        Self(id, crate::expiry::Undisturbed::here())
    }
}

#[cfg(test)]
impl Drop for OwnWatches {
    fn drop(&mut self) {
        forget_watches(self.0);
    }
}

#[cfg(test)]
fn forget_watches(id: usize) {
    if let Some(watched) = WATCHED.lock().as_mut() {
        watched.retain(|(store, _, _), _| *store != id);
        WATCHED_LEN.store(watched.len(), std::sync::atomic::Ordering::Release);
    }
}

/// How many hashes are being watched. For tests.
#[cfg(test)]
fn watched_in(store: &Store) -> usize {
    let id = store_id(store);
    WATCHED
        .lock()
        .as_ref()
        .map_or(0, |w| w.keys().filter(|(s, _, _)| *s == id).count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{HashInner, KiviDBValue};
    use std::sync::Arc;
    use std::time::Duration;

    fn hash_with(fields: &[(&str, Option<Duration>)]) -> KiviDBValue {
        let mut inner = HashInner::new();
        for (name, ttl) in fields {
            inner.fields.insert(name.to_string(), Arc::from(&b"v"[..]));
            if let Some(ttl) = ttl {
                inner
                    .expiries
                    .insert(name.to_string(), Instant::now() + *ttl);
            }
        }
        KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(inner)))
    }

    fn fields_of(store: &Store, key: &str) -> Option<Vec<String>> {
        match crate::store::get_shard(&store[0], key).read().get(key) {
            Some(ValueRef::HashEx(hash)) => {
                let mut names: Vec<String> = hash.read().fields.keys().cloned().collect();
                names.sort();
                Some(names)
            }
            _ => None,
        }
    }

    /// An index named `prefix` of the hashes under it, with a NUMERIC `n`,
    /// in `index_store`; and at `key` a hash with `n = 1`, whose time to
    /// live has just ended, and `o = x`: in the postings, and watched.
    fn hash_losing_n(
        store: &Store,
        index_store: &IndexStore,
        prefix: &str,
        key: &str,
    ) -> crate::vector::IndexDef {
        use crate::vector::{FieldOptions, FieldType, IndexOn, IndexSchema, SchemaField};
        let def = IndexSchema {
            name: prefix.into(),
            db_index: 0,
            on: IndexOn::Hash,
            prefixes: vec![prefix.into()],
            fields: vec![SchemaField {
                name: "n".into(),
                field_type: FieldType::Numeric,
                options: FieldOptions::default(),
            }],
            aliases: Vec::new(),
            stopwords: None,
            synonyms: Default::default(),
            language: Default::default(),
            default_score: 1.0,
            score_field: None,
        }
        .into_def();
        index_store.write().insert(prefix.into(), def.clone());
        let fields: Vec<(String, Arc<[u8]>)> = vec![
            ("n".into(), Arc::from(&b"1"[..])),
            ("o".into(), Arc::from(&b"x"[..])),
        ];
        let mut inner = HashInner::new();
        for (name, value) in &fields {
            inner.fields.insert(name.clone(), Arc::clone(value));
        }
        inner
            .expiries
            .insert("n".into(), Instant::now() + Duration::from_millis(1));
        crate::store::get_shard(&store[0], key).write().insert(
            key,
            KiviDBValue::HashEx(Arc::new(parking_lot::RwLock::new(inner))),
        );
        let mut p = def.postings.write();
        p.write(key, &fields, true);
        def.postings.done(p);
        track_key(store, 0, key);
        std::thread::sleep(Duration::from_millis(5));
        def
    }

    /// `write` on a thread of its own, holding the write order of `key` as
    /// a command does, given 300 ms to finish: it finishes if nothing holds
    /// the order, and waits for it otherwise. The thread, to join.
    fn write_beside(
        key: &'static str,
        write: impl FnOnce() + Send + 'static,
    ) -> std::thread::JoinHandle<()> {
        let (done, finished) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let _order = crate::write_order::lock_keys(0, std::iter::once(key.as_bytes()));
            write();
            let _ = done.send(());
        });
        let _ = finished.recv_timeout(Duration::from_millis(300));
        thread
    }

    /// The field-TTL hash at `key` of `store`, to write to.
    fn hash_at(store: &Store, key: &str) -> Arc<parking_lot::RwLock<HashInner>> {
        match crate::store::get_shard(&store[0], key).read().get(key) {
            Some(ValueRef::HashEx(hash)) => Arc::clone(hash),
            _ => panic!("no hash with field TTLs at {key}"),
        }
    }

    /// An `HSET` that comes while the sweep brings a hash's postings up to
    /// date, after the sweep has read the hash's fields, waits for the
    /// sweep, and is not written over with what the sweep read (#364).
    #[test]
    fn a_write_beside_the_sweep_is_not_written_over_in_the_postings() {
        use std::cell::RefCell;
        use std::rc::Rc;
        const KEY: &str = "f364w:k";
        // Again if a replay elsewhere in the process deferred indexing, and
        // the sweep did not get to the postings.
        for _ in 0..50 {
            let store = crate::store::new_store(1);
            let _own = OwnWatches::of(&store);
            let index_store = crate::vector::new_index_store();
            let def = hash_losing_n(&store, &index_store, "f364w:", KEY);
            let beside = Rc::new(RefCell::new(None));
            {
                let (store, def, beside) = (Arc::clone(&store), def.clone(), Rc::clone(&beside));
                crate::test_hooks::set("index_fields: fields read", move || {
                    // HSET k n 3.
                    *beside.borrow_mut() = Some(write_beside(KEY, move || {
                        let three: Arc<[u8]> = Arc::from(&b"3"[..]);
                        hash_at(&store, KEY)
                            .write()
                            .fields
                            .insert("n".into(), Arc::clone(&three));
                        def.postings
                            .write_fields(KEY, &[("n".into(), three)], false, None);
                    }));
                });
            }
            sweep(&store, &index_store);
            crate::test_hooks::clear("index_fields: fields read");
            let Some(thread) = beside.borrow_mut().take() else {
                continue;
            };
            thread.join().unwrap();
            let p = def.postings.read();
            let id = p.id(KEY).expect("still in the index");
            let found = p.range("n", 0.0, false, 10.0, false).unwrap();
            assert!(found.get(id), "the postings lost the n the HSET wrote");
            return;
        }
        panic!("the sweep never got to the postings");
    }

    /// An `HPEXPIRE` that comes while the sweep settles a hash's watch, the
    /// sweep having found no field left with a time to live, waits for the
    /// sweep, and its field is watched (#364).
    #[test]
    fn a_ttl_given_beside_the_sweep_is_watched() {
        use std::cell::RefCell;
        use std::rc::Rc;
        const KEY: &str = "f364t:k";
        let store = crate::store::new_store(1);
        let _own = OwnWatches::of(&store);
        let index_store = crate::vector::new_index_store();
        hash_losing_n(&store, &index_store, "f364t:", KEY);
        let beside = Rc::new(RefCell::new(None));
        {
            let (store, beside) = (Arc::clone(&store), Rc::clone(&beside));
            crate::test_hooks::set("sweep: watch", move || {
                // HPEXPIRE k 60000 FIELDS 1 o.
                *beside.borrow_mut() = Some(write_beside(KEY, move || {
                    hash_at(&store, KEY)
                        .write()
                        .expiries
                        .insert("o".into(), Instant::now() + Duration::from_secs(60));
                    track_key(&store, 0, KEY);
                }));
            });
        }
        sweep(&store, &index_store);
        crate::test_hooks::clear("sweep: watch");
        beside
            .borrow_mut()
            .take()
            .expect("the sweep settled the watch")
            .join()
            .unwrap();
        assert_eq!(fields_of(&store, KEY).unwrap(), ["o"], "n was swept");
        assert_eq!(
            watched_keys(&store, 0),
            [KEY],
            "o has a time to live, and the hash is watched"
        );
    }

    /// `cmd` applied as a replica applies its master's stream, on a thread
    /// of its own, given 500 ms to finish: it finishes if nothing holds the
    /// order of its keys, and waits for it otherwise. The thread, to join,
    /// which says whether indexing was deferred around the write (by a
    /// replay elsewhere in the process), when the attempt shows nothing.
    fn applied_beside(
        store: &Store,
        index_store: &IndexStore,
        cmd: Vec<String>,
    ) -> std::thread::JoinHandle<bool> {
        let (store, index_store) = (Arc::clone(store), Arc::clone(index_store));
        let (done, finished) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let deferred = applied(&store, &index_store, cmd);
            let _ = done.send(());
            deferred
        });
        let _ = finished.recv_timeout(Duration::from_millis(500));
        thread
    }

    /// `cmd` applied on this thread as a replica applies its master's
    /// stream: whether indexing was deferred around it (see
    /// [`applied_beside`]).
    fn applied(store: &Store, index_store: &IndexStore, cmd: Vec<String>) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        let config = Arc::new(crate::config::Config::default_replica());
        let repl_state = Arc::new(crate::replication::ReplState::new_master());
        let dict = crate::vector::new_dict_store();
        let wait = Arc::new(crate::wait_table::WaitTable::new());
        let ts = crate::store::new_ts_index();
        let (compaction_tx, _compaction_rx) = crate::vector::new_compaction_channel();
        let env = crate::replication::replica_client::ReplicationApplyEnv::new_inert();
        let mut db = 0usize;
        let deferred = crate::vector::DEFER_INDEXING.load(SeqCst);
        env.apply_command(
            cmd.into_iter().map(String::into_bytes).collect(),
            store,
            &repl_state,
            index_store,
            &dict,
            &wait,
            &ts,
            &config,
            &compaction_tx,
            &mut db,
            &crate::watch::NoopWatchRegistry,
            "replica:test",
        )
        .expect("applied");
        deferred || crate::vector::DEFER_INDEXING.load(SeqCst)
    }

    /// On a replica: an `HSET` from the master, applied while the sweep
    /// brings a hash's postings up to date, after the sweep has read the
    /// hash's fields, waits for the sweep, and is not written over (#371).
    #[test]
    fn a_write_from_the_master_beside_the_sweep_is_not_written_over() {
        use std::cell::RefCell;
        use std::rc::Rc;
        const KEY: &str = "f371w:k";
        // Again if a replay elsewhere in the process deferred indexing.
        for _ in 0..50 {
            let store = crate::store::new_store(1);
            let _own = OwnWatches::of(&store);
            let index_store = crate::vector::new_index_store();
            let def = hash_losing_n(&store, &index_store, "f371w:", KEY);
            let beside = Rc::new(RefCell::new(None));
            {
                let (store, index_store, beside) = (
                    Arc::clone(&store),
                    Arc::clone(&index_store),
                    Rc::clone(&beside),
                );
                crate::test_hooks::set("index_fields: fields read", move || {
                    let cmd = ["HSET", KEY, "n", "3"].map(String::from).to_vec();
                    *beside.borrow_mut() = Some(applied_beside(&store, &index_store, cmd));
                });
            }
            sweep(&store, &index_store);
            crate::test_hooks::clear("index_fields: fields read");
            let Some(thread) = beside.borrow_mut().take() else {
                continue;
            };
            if thread.join().unwrap() {
                continue;
            }
            let p = def.postings.read();
            let id = p.id(KEY).expect("still in the index");
            let found = p.range("n", 0.0, false, 10.0, false).unwrap();
            assert!(
                found.get(id),
                "the postings lost the n the master's HSET wrote"
            );
            return;
        }
        panic!("the sweep never got to the postings");
    }

    /// On a replica: an `HPEXPIREAT` from the master, applied while the
    /// sweep settles a hash's watch, the sweep having found no field left
    /// with a time to live, waits for the sweep, and its field is watched
    /// (#371).
    #[test]
    fn a_ttl_from_the_master_beside_the_sweep_is_watched() {
        use std::cell::RefCell;
        use std::rc::Rc;
        const KEY: &str = "f371t:k";
        let store = crate::store::new_store(1);
        let _own = OwnWatches::of(&store);
        let index_store = crate::vector::new_index_store();
        hash_losing_n(&store, &index_store, "f371t:", KEY);
        let beside = Rc::new(RefCell::new(None));
        {
            let (store, index_store, beside) = (
                Arc::clone(&store),
                Arc::clone(&index_store),
                Rc::clone(&beside),
            );
            crate::test_hooks::set("sweep: watch", move || {
                let at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    + 60_000;
                let cmd = ["HPEXPIREAT", KEY, &at.to_string(), "FIELDS", "1", "o"]
                    .map(String::from)
                    .to_vec();
                *beside.borrow_mut() = Some(applied_beside(&store, &index_store, cmd));
            });
        }
        sweep(&store, &index_store);
        crate::test_hooks::clear("sweep: watch");
        beside
            .borrow_mut()
            .take()
            .expect("the sweep settled the watch")
            .join()
            .unwrap();
        assert_eq!(fields_of(&store, KEY).unwrap(), ["o"], "n was swept");
        assert!(
            watched_keys(&store, 0).iter().any(|key| key == KEY),
            "o has a time to live from the master, and the hash is watched"
        );
    }

    /// On a replica with nothing watched: an `HPEXPIREAT` from the master
    /// watches the hash, then brings its postings up to date. A sweep that
    /// reaches the hash in between, its field due, waits for the write, and
    /// the postings and the watch agree when both are done (#376). Without
    /// the write's order the sweep evicted the field and dropped the watch,
    /// and the write then put the field back in the postings, for good.
    /// What is watched is what this store has: the watches other tests left
    /// in theirs would make the write take its order anyway (#378).
    #[test]
    fn a_ttl_from_the_master_that_starts_the_watching_is_ordered() {
        use std::cell::RefCell;
        use std::rc::Rc;
        const KEY: &str = "f376:k";
        // Again if a replay elsewhere in the process deferred indexing.
        for _ in 0..50 {
            let store = crate::store::new_store(1);
            let _own = OwnWatches::of(&store);
            let _scope = WatchScope::of(&store);
            let index_store = crate::vector::new_index_store();
            let def = hash_losing_n(&store, &index_store, "f376:", KEY);
            // Not watched, its n with no time to live: as the master's HSET
            // left it.
            hash_at(&store, KEY).write().expiries.clear();
            forget_watches(store_id(&store));
            let sweeper = Rc::new(RefCell::new(None));
            {
                let (store, index_store, sweeper) = (
                    Arc::clone(&store),
                    Arc::clone(&index_store),
                    Rc::clone(&sweeper),
                );
                crate::test_hooks::set("index_fields: fields read", move || {
                    // n is due; the sweep, from a thread of its own.
                    std::thread::sleep(Duration::from_millis(60));
                    let (done, finished) = std::sync::mpsc::channel();
                    *sweeper.borrow_mut() = Some(std::thread::spawn(move || {
                        sweep(&store, &index_store);
                        let _ = done.send(());
                    }));
                    let _ = finished.recv_timeout(Duration::from_millis(300));
                });
            }
            let at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                + 50;
            let cmd = ["HPEXPIREAT", KEY, &at.to_string(), "FIELDS", "1", "n"]
                .map(String::from)
                .to_vec();
            // Applied on this thread, where the hook is.
            let deferred = applied(&store, &index_store, cmd);
            crate::test_hooks::clear("index_fields: fields read");
            let Some(sweeper) = sweeper.borrow_mut().take() else {
                continue;
            };
            sweeper.join().unwrap();
            if deferred {
                continue;
            }
            // What the sweep did last stands.
            sweep(&store, &index_store);
            let p = def.postings.read();
            let id = p.id(KEY).expect("still in the index");
            let in_postings = p.range("n", 0.0, false, 10.0, false).unwrap().get(id);
            drop(p);
            let has_n = fields_of(&store, KEY).unwrap().contains(&"n".to_string());
            let watched = watched_keys(&store, 0).iter().any(|key| key == KEY);
            assert!(
                !in_postings || has_n || watched,
                "the postings have n, the hash has not, and nothing watches it"
            );
            return;
        }
        panic!("the write never got to the postings");
    }

    #[test]
    fn due_fields_go_and_an_empty_hash_goes_with_them() {
        let store = crate::store::new_store(1);
        let _own = OwnWatches::of(&store);
        let index_store = crate::vector::new_index_store();
        let put = |key: &str, value: KiviDBValue| {
            crate::store::get_shard(&store[0], key)
                .write()
                .insert(key, value);
            track_key(&store, 0, key);
        };
        let soon = Some(Duration::from_millis(30));
        let later = Some(Duration::from_secs(3600));
        put("some", hash_with(&[("a", soon), ("b", later), ("c", None)]));
        put("all", hash_with(&[("a", soon), ("b", soon)]));
        put("none", hash_with(&[("a", None)]));
        assert_eq!(watched_in(&store), 2, "a hash without TTLs is not watched");

        // Nothing is due yet.
        sweep(&store, &index_store);
        assert_eq!(fields_of(&store, "some").unwrap().len(), 3);

        std::thread::sleep(Duration::from_millis(60));
        sweep(&store, &index_store);
        assert_eq!(fields_of(&store, "some").unwrap(), ["b", "c"]);
        assert_eq!(fields_of(&store, "all"), None, "an empty hash is deleted");
        assert_eq!(fields_of(&store, "none").unwrap(), ["a"]);
        // `some` is still watched, for `b`; `all` is gone from the list.
        assert_eq!(watched_in(&store), 1);

        // A key that stopped being such a hash is dropped from the list when
        // it comes due.
        put("replaced", hash_with(&[("a", soon)]));
        crate::store::get_shard(&store[0], "replaced")
            .write()
            .insert_str("replaced", &b"x"[..]);
        std::thread::sleep(Duration::from_millis(60));
        sweep(&store, &index_store);
        assert_eq!(watched_in(&store), 1);

        // After a rescan, what is there is what is watched.
        forget_watches(store_id(&store));
        rescan(&store);
        assert_eq!(watched_in(&store), 1);
    }
}

/// What a master tells for a hash field that goes for its deadline, and
/// where; what a replica, or a thread with fields held, does with one.
/// Commands run as a client runs them, their stream read off the hub.
#[cfg(test)]
mod propagation_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use tokio::sync::mpsc;

    use crate::replication::replica_client::{ReplicationApplyEnv, run_as_client};
    use crate::replication::{HubMsg, HubTx, ReplState};
    use crate::store::{self, Store, ValueRef};
    use crate::vector::IndexStore;

    struct Master {
        store: Store,
        repl: Arc<ReplState>,
        index: IndexStore,
        hub_tx: HubTx,
        rx: mpsc::UnboundedReceiver<HubMsg>,
        env: ReplicationApplyEnv,
        _own: super::OwnWatches,
    }

    /// A master with a replica's backlog to feed. Its store is never freed:
    /// a store is found by its address (`expiry::of`), which a freed one
    /// could hand to another test's.
    fn master() -> Master {
        let store = store::new_store(1);
        std::mem::forget(Arc::clone(&store));
        let repl = Arc::new(ReplState::new_master());
        repl.backlog_active.store(true, Ordering::Relaxed);
        let (hub_tx, rx) = mpsc::unbounded_channel();
        crate::expiry::register(&store, &repl, &hub_tx);
        let index = crate::vector::new_index_store();
        let own = super::OwnWatches::of(&store);
        Master {
            store,
            repl,
            index,
            hub_tx,
            rx,
            env: ReplicationApplyEnv::new_inert(),
            _own: own,
        }
    }

    impl Master {
        fn run(&self, cmd: &[&str]) -> String {
            self.run_watched(cmd, &crate::watch::NoopWatchRegistry)
        }

        fn run_watched(&self, cmd: &[&str], watch: &dyn crate::watch::WatchNotifier) -> String {
            let cmd: Vec<&[u8]> = cmd.iter().map(|part| part.as_bytes()).collect();
            self.run_bytes_watched(&cmd, watch)
        }

        /// [`Master::run`], with arguments that need not be text.
        fn run_bytes(&self, cmd: &[&[u8]]) -> String {
            self.run_bytes_watched(cmd, &crate::watch::NoopWatchRegistry)
        }

        fn run_bytes_watched(
            &self,
            cmd: &[&[u8]],
            watch: &dyn crate::watch::WatchNotifier,
        ) -> String {
            let out = run_as_client(
                &self.env,
                cmd,
                &self.store,
                &self.repl,
                &self.index,
                &self.hub_tx,
                watch,
            );
            String::from_utf8_lossy(&out).into_owned()
        }

        /// The commands sent so far, `SELECT`s left out.
        fn sent(&mut self) -> Vec<Vec<String>> {
            let mut bytes = Vec::new();
            while let Ok(msg) = self.rx.try_recv() {
                if let HubMsg::Propagate(payload) = msg {
                    bytes.extend_from_slice(&payload);
                }
            }
            commands_in(&bytes)
                .into_iter()
                .filter(|c| !c[0].eq_ignore_ascii_case("SELECT"))
                .collect()
        }

        /// `field` of `key` made due, its deadline gone by a moment ago.
        fn due(&self, key: &str, field: &str) {
            self.deadline(key, field, Instant::now() - Duration::from_millis(5));
        }

        fn deadline(&self, key: &str, field: &str, at: Instant) {
            let hash = match store::get_shard(&self.store[0], key).read().get(key) {
                Some(ValueRef::HashEx(hash)) => Arc::clone(hash),
                _ => panic!("{key} has no field TTLs"),
            };
            hash.write().expiries.insert(field.to_string(), at);
            super::track_key(&self.store, 0, key);
        }

        /// What `key` physically holds, past deadlines included.
        fn held(&self, key: &str) -> Option<Vec<(String, String)>> {
            let shard = store::get_shard(&self.store[0], key);
            let map = shard.read();
            let fields: Vec<(String, String)> = match map.get(key)? {
                ValueRef::HashEx(hash) => hash
                    .read()
                    .fields
                    .iter()
                    .map(|(f, v)| (f.clone(), String::from_utf8_lossy(v).into_owned()))
                    .collect(),
                ValueRef::Hash(hash) => hash
                    .read()
                    .iter()
                    .map(|(f, v)| (f.clone(), String::from_utf8_lossy(v).into_owned()))
                    .collect(),
                _ => return Some(Vec::new()),
            };
            let mut fields = fields;
            fields.sort();
            Some(fields)
        }

        /// `key` = { f: 5, keep: 1 } with `f` due.
        fn with_due_field(&mut self, key: &str) {
            self.run(&["HSET", key, "f", "5", "keep", "1"]);
            self.run(&["HPEXPIRE", key, "60000", "FIELDS", "1", "f"]);
            self.due(key, "f");
            self.sent();
        }

        /// `key` = { f: 5 } with `f`, its only field, due.
        fn with_only_field_due(&mut self, key: &str) {
            self.run(&["HSET", key, "f", "5"]);
            self.run(&["HPEXPIRE", key, "60000", "FIELDS", "1", "f"]);
            self.due(key, "f");
            self.sent();
        }
    }

    /// The commands of a RESP stream of arrays of bulk strings.
    fn commands_in(mut bytes: &[u8]) -> Vec<Vec<String>> {
        fn line(bytes: &mut &[u8]) -> String {
            let end = bytes.windows(2).position(|w| w == b"\r\n").unwrap();
            let text = String::from_utf8_lossy(&bytes[..end]).into_owned();
            *bytes = &bytes[end + 2..];
            text
        }
        let mut out = Vec::new();
        while !bytes.is_empty() {
            let head = line(&mut bytes);
            let n: usize = head[1..].parse().unwrap();
            let mut parts = Vec::with_capacity(n);
            for _ in 0..n {
                let len: usize = line(&mut bytes)[1..].parse().unwrap();
                parts.push(String::from_utf8_lossy(&bytes[..len]).into_owned());
                bytes = &bytes[len + 2..];
            }
            out.push(parts);
        }
        out
    }

    fn cmd(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|p| p.to_string()).collect()
    }

    fn deletion(key: &str) -> [Vec<String>; 2] {
        [cmd(&["UNLINK", key]), cmd(&["DEL", key])]
    }

    #[test]
    fn an_increment_of_a_due_field_follows_its_deletion() {
        let mut m = master();
        m.with_due_field("h");
        assert_eq!(m.run(&["HINCRBY", "h", "f", "1"]), ":1\r\n");
        assert_eq!(
            m.sent(),
            [cmd(&["HDEL", "h", "f"]), cmd(&["HINCRBY", "h", "f", "1"])]
        );
        m.with_due_field("x");
        assert_eq!(m.run(&["HINCRBYFLOAT", "x", "f", "1.5"]), "$3\r\n1.5\r\n");
        let sent = m.sent();
        assert_eq!(sent[0], cmd(&["HDEL", "x", "f"]));
        assert_eq!(sent.len(), 2, "{sent:?}");
    }

    /// A field past its deadline is no field to `HSETNX`, as in Redis.
    #[test]
    fn hsetnx_sets_a_due_field_after_its_deletion() {
        let mut m = master();
        m.with_due_field("h");
        assert_eq!(m.run(&["HSETNX", "h", "f", "new"]), ":1\r\n");
        assert_eq!(
            m.sent(),
            [cmd(&["HDEL", "h", "f"]), cmd(&["HSETNX", "h", "f", "new"])]
        );
        // Live now: refused, with nothing expired before it.
        assert_eq!(m.run(&["HSETNX", "h", "f", "again"]), ":0\r\n");
        assert!(!m.sent().iter().any(|c| c[0] == "HDEL"));
    }

    /// A dataset another test is loading is not this master's: its due
    /// field goes before the `XX` is weighed. The load, which is the
    /// process's, was taken as this master's own, and the field was kept and
    /// given the new deadline: `:1` (#535).
    #[test]
    fn another_tests_load_leaves_this_ones_expiry_alone() {
        let mut m = master();
        m.with_due_field("h");
        std::thread::scope(|s| {
            // Dropped before the scope waits for the loader, if this fails.
            let (loaded, load_started) = std::sync::mpsc::channel();
            let (stop, stopped) = std::sync::mpsc::channel::<()>();
            s.spawn(move || {
                let _load = crate::expiry::Loading::start();
                loaded.send(()).unwrap();
                let _ = stopped.recv();
            });
            load_started.recv().unwrap();
            assert!(std::thread::spawn(crate::expiry::loading).join().unwrap());
            assert_eq!(
                m.run(&["HPEXPIRE", "h", "100000", "XX", "FIELDS", "1", "f"]),
                "*1\r\n:-2\r\n"
            );
            drop(stop);
        });
        assert_eq!(m.sent(), [cmd(&["HDEL", "h", "f"])]);
    }

    /// `XX` and `GT` are weighed here: the replica gets what was decided.
    #[test]
    fn a_conditional_field_expire_is_sent_as_its_effect() {
        let mut m = master();
        m.with_due_field("h");
        assert_eq!(
            m.run(&["HPEXPIRE", "h", "100000", "XX", "FIELDS", "1", "f"]),
            "*1\r\n:-2\r\n"
        );
        assert_eq!(m.sent(), [cmd(&["HDEL", "h", "f"])]);

        m.run(&["HPEXPIRE", "h", "100000", "FIELDS", "1", "keep"]);
        let sent = m.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0][0], "HPEXPIREAT");
        assert_eq!(&sent[0][3..], ["FIELDS", "1", "keep"]);
        // A condition that does not hold: nothing.
        assert_eq!(
            m.run(&["HPEXPIRE", "h", "1000", "GT", "FIELDS", "1", "keep"]),
            "*1\r\n:0\r\n"
        );
        assert!(m.sent().is_empty());
        // One that holds: the deadline alone, no `GT`.
        assert_eq!(
            m.run(&[
                "HPEXPIRE", "h", "900000", "GT", "FIELDS", "2", "keep", "nope"
            ]),
            "*2\r\n:1\r\n:-2\r\n"
        );
        let sent = m.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0][0], "HPEXPIREAT");
        assert_eq!(&sent[0][3..], ["FIELDS", "1", "keep"]);
        // A moment past deletes: an `HDEL`.
        assert_eq!(
            m.run(&["HPEXPIREAT", "h", "1", "FIELDS", "1", "keep"]),
            "*1\r\n:2\r\n"
        );
        assert_eq!(m.sent(), [cmd(&["HDEL", "h", "keep"])]);
        // On no field at all: nothing.
        assert_eq!(
            m.run(&["HPEXPIRE", "nokey", "1000", "FIELDS", "1", "f"]),
            "*1\r\n:-2\r\n"
        );
        assert!(m.sent().is_empty());
    }

    #[test]
    fn hpersist_is_sent_as_the_ttls_it_took_away() {
        let mut m = master();
        m.run(&["HSET", "h", "plain", "1", "ttl", "2"]);
        m.run(&["HPEXPIRE", "h", "100000", "FIELDS", "1", "ttl"]);
        m.with_due_field("h");
        assert_eq!(
            m.run(&["HPERSIST", "h", "FIELDS", "4", "f", "plain", "ttl", "nope"]),
            "*4\r\n:-2\r\n:-1\r\n:1\r\n:-2\r\n"
        );
        assert_eq!(
            m.sent(),
            [
                cmd(&["HDEL", "h", "f"]),
                cmd(&["HPERSIST", "h", "FIELDS", "1", "ttl"])
            ]
        );
        // Nothing to take away: nothing sent.
        m.run(&["HPERSIST", "h", "FIELDS", "1", "plain"]);
        assert!(m.sent().is_empty());
    }

    #[test]
    fn hgetex_and_hsetex_are_sent_as_their_effects() {
        let mut m = master();
        m.run(&["HSET", "h", "a", "1", "b", "2"]);
        m.sent();
        // A read: nothing.
        m.run(&["HGETEX", "h", "FIELDS", "1", "a"]);
        assert!(m.sent().is_empty());
        m.run(&["HGETEX", "h", "PX", "100000", "FIELDS", "2", "a", "nope"]);
        let sent = m.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0][0], "HPEXPIREAT");
        assert_eq!(&sent[0][3..], ["FIELDS", "1", "a"]);
        m.run(&["HGETEX", "h", "PERSIST", "FIELDS", "2", "a", "b"]);
        assert_eq!(m.sent(), [cmd(&["HPERSIST", "h", "FIELDS", "1", "a"])]);
        m.run(&["HGETEX", "h", "PXAT", "1", "FIELDS", "1", "b"]);
        assert_eq!(m.sent(), [cmd(&["HDEL", "h", "b"])]);

        // A refused `HSETEX`: nothing; a done one without its `FNX`.
        assert_eq!(
            m.run(&["HSETEX", "h", "FNX", "PX", "5000", "FIELDS", "1", "a", "x"]),
            ":0\r\n"
        );
        assert!(m.sent().is_empty());
        m.run(&["HSETEX", "h", "FXX", "PX", "5000", "FIELDS", "1", "a", "x"]);
        let sent = m.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0][..3], ["HSETEX", "h", "PXAT"]);
        assert_eq!(&sent[0][4..], ["FIELDS", "1", "a", "x"]);
        m.run(&["HSETEX", "h", "PXAT", "1", "FIELDS", "1", "a", "y"]);
        assert_eq!(m.sent(), [cmd(&["HDEL", "h", "a"])]);
    }

    /// `HSETEX` finds its due fields in the lookup it writes under (#543):
    /// gone first, their `HDEL` before its record, so a due field is no
    /// field to `FNX`. The record is the client's own fields and values,
    /// bytes that are not UTF-8 included.
    #[test]
    fn hsetex_writes_after_the_deletion_of_its_due_fields() {
        let mut m = master();
        m.with_due_field("h");
        assert_eq!(
            m.run(&["HSETEX", "h", "FNX", "PX", "100000", "FIELDS", "1", "f", "new"]),
            ":1\r\n"
        );
        let sent = m.sent();
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert_eq!(sent[0], cmd(&["HDEL", "h", "f"]));
        assert_eq!(sent[1][..3], ["HSETEX", "h", "PXAT"]);
        assert_eq!(&sent[1][4..], ["FIELDS", "1", "f", "new"]);
        assert_eq!(
            m.held("h"),
            Some(vec![
                ("f".to_string(), "new".to_string()),
                ("keep".to_string(), "1".to_string())
            ])
        );

        m.with_only_field_due("e");
        assert_eq!(
            m.run(&["HSETEX", "e", "KEEPTTL", "FIELDS", "1", "g", "1"]),
            ":1\r\n"
        );
        let sent = m.sent();
        assert!(deletion("e").contains(&sent[0]), "{sent:?}");
        assert_eq!(
            sent[1],
            cmd(&["HSETEX", "e", "KEEPTTL", "FIELDS", "1", "g", "1"])
        );
        assert_eq!(m.held("e"), Some(vec![("g".to_string(), "1".to_string())]));

        let name: &[u8] = b"n\xff\x00";
        let value: &[u8] = b"\xfe\r\n";
        assert_eq!(
            m.run_bytes(&[b"HSETEX", b"b", b"FIELDS", b"1", name, value]),
            ":1\r\n"
        );
        let mut bytes = Vec::new();
        while let Ok(msg) = m.rx.try_recv() {
            if let crate::replication::HubMsg::Propagate(payload) = msg {
                bytes.extend_from_slice(&payload);
            }
        }
        let mut wanted = b"*6\r\n$6\r\nHSETEX\r\n$1\r\nb\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n".to_vec();
        wanted.extend_from_slice(b"$3\r\nn\xff\x00\r\n$3\r\n\xfe\r\n\r\n");
        assert!(bytes.ends_with(&wanted), "{:?}", String::from_utf8_lossy(&bytes));
    }

    /// With nothing to read the stream, `HSETEX` builds no record of what it
    /// did (#543), and still removes a due field before it writes.
    #[test]
    fn an_hsetex_nobody_observes_still_writes_after_its_due_fields() {
        let mut m = master();
        m.with_due_field("h");
        m.repl.backlog_active.store(false, Ordering::Relaxed);
        assert!(!m.repl.feeds_hub());
        // A record left from before: begun anew, and not kept.
        crate::replication::script_feed::effects_begin();
        assert_eq!(
            m.run(&["HSETEX", "h", "FNX", "PX", "100000", "FIELDS", "1", "f", "new"]),
            ":1\r\n"
        );
        assert!(!crate::replication::script_feed::recording());
        assert!(m.sent().is_empty());
        assert_eq!(
            m.held("h"),
            Some(vec![
                ("f".to_string(), "new".to_string()),
                ("keep".to_string(), "1".to_string())
            ])
        );
        assert_eq!(
            m.run(&["HSETEX", "h", "FNX", "PX", "100000", "FIELDS", "1", "f", "again"]),
            ":0\r\n"
        );
    }

    /// The last field due: the hash goes, its deletion sent, and the write
    /// after it makes a new one.
    #[test]
    fn a_hash_emptied_by_its_deadlines_is_deleted_before_the_write_that_remakes_it() {
        let mut m = master();
        m.with_only_field_due("h");
        assert_eq!(m.run(&["HINCRBY", "h", "f", "1"]), ":1\r\n");
        let sent = m.sent();
        assert!(deletion("h").contains(&sent[0]), "{sent:?}");
        assert_eq!(sent[1], cmd(&["HINCRBY", "h", "f", "1"]));
        assert!(matches!(
            store::get_shard(&m.store[0], "h").read().get("h"),
            Some(ValueRef::Hash(_))
        ));

        // Another type after it: once a read has found the hash empty.
        m.with_only_field_due("t");
        assert_eq!(m.run(&["HGET", "t", "f"]), "$-1\r\n");
        assert_eq!(m.run(&["LPUSH", "t", "x"]), ":1\r\n");
        let sent = m.sent();
        assert!(deletion("t").contains(&sent[0]), "{sent:?}");
        assert_eq!(sent[1], cmd(&["LPUSH", "t", "x"]));
    }

    #[test]
    fn a_read_deletes_a_due_field_once_and_the_sweep_finds_nothing_more() {
        let mut m = master();
        m.with_due_field("h");
        assert_eq!(m.run(&["HGET", "h", "f"]), "$-1\r\n");
        assert_eq!(m.run(&["HLEN", "h"]), ":1\r\n");
        super::sweep(&m.store, &m.index);
        assert_eq!(m.sent(), [cmd(&["HDEL", "h", "f"])]);
        assert_eq!(m.held("h").unwrap(), [("keep".into(), "1".into())]);
    }

    #[test]
    fn the_sweep_sends_an_hdel_or_the_deletion_of_an_emptied_hash() {
        let mut m = master();
        m.with_due_field("some");
        m.with_only_field_due("all");
        super::sweep(&m.store, &m.index);
        let sent = m.sent();
        assert!(sent.contains(&cmd(&["HDEL", "some", "f"])), "{sent:?}");
        assert!(deletion("all").iter().any(|d| sent.contains(d)), "{sent:?}");
        assert_eq!(sent.len(), 2);
        assert_eq!(m.held("all"), None);
        super::sweep(&m.store, &m.index);
        assert!(m.sent().is_empty());
    }

    /// Loading, replaying or applying a master's stream: a due field is
    /// there, and is written to as the master wrote to it.
    #[test]
    fn with_fields_held_a_due_field_is_neither_removed_nor_sent() {
        let mut m = master();
        m.with_due_field("h");
        {
            let _hold = crate::expiry::hold_of(false, true);
            assert_eq!(m.run(&["HGET", "h", "f"]), "$1\r\n5\r\n");
            assert_eq!(m.run(&["HINCRBY", "h", "f", "1"]), ":6\r\n");
            assert_eq!(
                m.run(&["HPERSIST", "h", "FIELDS", "1", "f"]),
                "*1\r\n:1\r\n"
            );
        }
        let sent = m.sent();
        assert!(!sent.iter().any(|c| c[0] == "HDEL"), "{sent:?}");
        assert_eq!(
            m.held("h").unwrap(),
            [("f".into(), "6".into()), ("keep".into(), "1".into())]
        );
        // Its time to live was taken away under the hold: live now.
        assert_eq!(m.run(&["HGET", "h", "f"]), "$1\r\n6\r\n");
    }

    /// A Valkey 9 `DUMP` payload (`RDB_TYPE_HASH_2`, RDB version 80, its
    /// checksum left out as Valkey's `RESTORE` allows): `h` = { f: 5, keep: 1 },
    /// `f` due at `f_at` (Unix milliseconds).
    fn valkey_hash_dump(f_at: u64) -> Vec<u8> {
        let mut out = vec![22u8, 2];
        for (field, value, at) in [(&b"f"[..], &b"5"[..], f_at as i64), (b"keep", b"1", -1)] {
            out.push(field.len() as u8);
            out.extend_from_slice(field);
            out.push(value.len() as u8);
            out.extend_from_slice(value);
            out.extend_from_slice(&at.to_le_bytes());
        }
        out.extend_from_slice(&80u16.to_le_bytes());
        out.extend_from_slice(&[0; 8]);
        out
    }

    /// A `RESTORE` of a Valkey 9 payload whose field was live on the master
    /// and is past its deadline by the time a lagging replica applies it,
    /// followed by the master's `HPERSIST` and `HINCRBY` of that field: the
    /// replica keeps the field as the master did, and ends with what the
    /// master has. The payload's field used to be dropped by the replica's
    /// clock on the way in, and the field the master kept for good was gone
    /// for good on the replica (#415).
    #[test]
    fn a_restored_valkey_field_past_its_deadline_is_kept_for_the_writes_after_it() {
        let m = master();
        let at = crate::commands::helpers::now_ms() - 50;
        let payload = valkey_hash_dump(at);
        {
            let _hold = crate::expiry::hold_of(true, true);
            assert_eq!(m.run_bytes(&[b"RESTORE", b"h", b"0", &payload]), "+OK\r\n");
            assert_eq!(m.held("h").unwrap().len(), 2, "f is kept");
            assert_eq!(m.run(&["HINCRBY", "h", "f", "1"]), ":6\r\n");
            assert_eq!(
                m.run(&["HPERSIST", "h", "FIELDS", "1", "f"]),
                "*1\r\n:1\r\n"
            );
        }
        assert_eq!(m.run(&["HGET", "h", "f"]), "$1\r\n6\r\n");
    }

    /// The same payload restored on a master, past its deadline: the field
    /// is kept and goes by field expiry, its `HDEL` sent, as a Redis
    /// payload's does; not dropped unsaid by the restore itself.
    #[test]
    fn a_restored_valkey_field_past_its_deadline_goes_with_its_hdel_on_a_master() {
        let mut m = master();
        let at = crate::commands::helpers::now_ms() - 50;
        assert_eq!(
            m.run_bytes(&[b"RESTORE", b"h", b"0", &valkey_hash_dump(at)]),
            "+OK\r\n"
        );
        assert_eq!(m.run(&["HGET", "h", "f"]), "$-1\r\n");
        assert_eq!(m.held("h").unwrap(), [("keep".into(), "1".into())]);
        let sent = m.sent();
        assert!(sent.contains(&cmd(&["HDEL", "h", "f"])), "{sent:?}");
    }

    /// A deadline that passed while the master's write was on its way is
    /// set, not taken as one that deletes the field.
    #[test]
    fn a_deadline_that_passed_in_transit_is_set_where_fields_are_held() {
        let m = master();
        m.run(&["HSET", "h", "f", "5"]);
        let past = (crate::commands::helpers::now_ms() - 50).to_string();
        {
            let _hold = crate::expiry::hold_of(true, true);
            assert_eq!(
                m.run(&["HPEXPIREAT", "h", &past, "FIELDS", "1", "f"]),
                "*1\r\n:1\r\n"
            );
            assert_eq!(
                m.run(&["HSETEX", "h", "PXAT", &past, "FIELDS", "1", "g", "1"]),
                ":1\r\n"
            );
            assert_eq!(
                m.run(&["HGETEX", "h", "PXAT", &past, "FIELDS", "1", "f"]),
                "*1\r\n$1\r\n5\r\n"
            );
        }
        assert_eq!(
            m.held("h").unwrap(),
            [("f".into(), "5".into()), ("g".into(), "1".into())]
        );
    }

    /// A replica of a master that sends `HDEL`s hides a due field and
    /// leaves it; one of a master that sends none removes it itself.
    #[test]
    fn a_replica_hides_or_removes_as_its_master_sends_deletions_or_not() {
        let mut m = master();
        m.with_due_field("h");
        m.repl.become_slave();
        m.repl
            .master_propagates_field_expiry
            .store(true, Ordering::Relaxed);
        assert_eq!(m.run(&["HGET", "h", "f"]), "$-1\r\n");
        assert_eq!(m.run(&["HLEN", "h"]), ":1\r\n");
        assert_eq!(m.run(&["HEXISTS", "h", "f"]), ":0\r\n");
        assert_eq!(m.run(&["HTTL", "h", "FIELDS", "1", "f"]), "*1\r\n:-2\r\n");
        assert_eq!(m.run(&["HGETALL", "h"]), "*2\r\n$4\r\nkeep\r\n$1\r\n1\r\n");
        super::sweep(&m.store, &m.index);
        assert_eq!(m.held("h").unwrap().len(), 2, "still there");
        assert!(
            super::watched_keys(&m.store, 0).contains(&crate::store::Key::from("h")),
            "still watched"
        );

        m.repl
            .master_propagates_field_expiry
            .store(false, Ordering::Relaxed);
        assert_eq!(m.run(&["HGET", "h", "f"]), "$-1\r\n");
        assert_eq!(m.held("h").unwrap(), [("keep".into(), "1".into())]);
        assert!(m.sent().is_empty(), "a replica sends nothing");
    }

    /// A large hash on a replica, one field of it due: the point reads
    /// (`HGET`, `HMGET`, `HEXISTS`, `HSTRLEN`) look at the fields they ask
    /// for, with no copy of the rest; the due one is hidden, the others
    /// shown, and nothing is removed. With fields held, the due one is
    /// shown, as the writes of the stream find it.
    #[test]
    fn a_point_read_of_a_large_hash_on_a_replica_hides_only_the_due_field() {
        let m = master();
        let mut hset = vec!["HSET".to_string(), "big".to_string()];
        for i in 0..2_000 {
            hset.push(format!("f{i}"));
            hset.push(format!("v{i}"));
        }
        let hset: Vec<&str> = hset.iter().map(String::as_str).collect();
        assert_eq!(m.run(&hset), ":2000\r\n");
        assert_eq!(
            m.run(&["HPEXPIRE", "big", "60000", "FIELDS", "2", "f7", "f1500"]),
            "*2\r\n:1\r\n:1\r\n"
        );
        m.due("big", "f7");
        m.repl.become_slave();
        m.repl
            .master_propagates_field_expiry
            .store(true, Ordering::Relaxed);

        assert_eq!(m.run(&["HGET", "big", "f1500"]), "$5\r\nv1500\r\n");
        assert_eq!(m.run(&["HGET", "big", "f1999"]), "$5\r\nv1999\r\n");
        assert_eq!(m.run(&["HGET", "big", "f7"]), "$-1\r\n");
        assert_eq!(
            m.run(&["HMGET", "big", "f7", "f8", "nope"]),
            "*3\r\n$-1\r\n$2\r\nv8\r\n$-1\r\n"
        );
        assert_eq!(m.run(&["HEXISTS", "big", "f7"]), ":0\r\n");
        assert_eq!(m.run(&["HEXISTS", "big", "f8"]), ":1\r\n");
        assert_eq!(m.run(&["HSTRLEN", "big", "f7"]), ":0\r\n");
        assert_eq!(m.run(&["HSTRLEN", "big", "f1500"]), ":5\r\n");
        // The whole-hash reads agree.
        assert_eq!(m.run(&["HLEN", "big"]), ":1999\r\n");
        assert_eq!(m.held("big").unwrap().len(), 2_000, "nothing removed");
        {
            let _hold = crate::expiry::hold_of(true, true);
            assert_eq!(m.run(&["HGET", "big", "f7"]), "$2\r\nv7\r\n");
            assert_eq!(m.run(&["HEXISTS", "big", "f7"]), ":1\r\n");
        }
        assert_eq!(m.run(&["HGET", "big", "f7"]), "$-1\r\n");
    }

    #[test]
    fn a_watch_of_the_hash_is_broken_by_the_removal() {
        let mut m = master();
        let registry: crate::watch::WatchRegistry =
            Arc::new(crate::watch::WatchRegistryInner::new());
        crate::watch::register_for(&m.store, &registry);
        m.with_due_field("h");
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        crate::watch::WatchNotifier::watch(&*registry, 0, b"h", Arc::clone(&flag));
        m.run(&["HGET", "h", "keep"]);
        assert!(flag.load(Ordering::SeqCst), "the watch was not broken");
        crate::watch::WatchNotifier::unwatch_all(&*registry, &[flag]);
    }

    /// What `stream`, the records a master sent, makes of an empty store
    /// when applied in order: as a replica applies its master's stream, or,
    /// with `aof`, as the AOF is replayed at startup.
    fn applied(m: &Master, stream: Vec<Vec<String>>, aof: bool) -> Master {
        let _replay = aof.then(crate::expiry::replay);
        let replica = store::new_store(1);
        std::mem::forget(Arc::clone(&replica));
        let repl = Arc::new(ReplState::new_master());
        let applier = ReplicationApplyEnv::new_inert();
        let config = Arc::new(crate::config::Config::default_replica());
        let dict = crate::vector::new_dict_store();
        let wait = Arc::new(crate::wait_table::WaitTable::new());
        let ts = crate::store::new_ts_index();
        let (compaction_tx, _rx) = crate::vector::new_compaction_channel();
        let replica_index = crate::vector::new_index_store();
        let mut db = 0usize;
        for command in stream {
            applier
                .apply_command(
                    command.into_iter().map(String::into_bytes).collect(),
                    &replica,
                    &repl,
                    &replica_index,
                    &dict,
                    &wait,
                    &ts,
                    &config,
                    &compaction_tx,
                    &mut db,
                    &crate::watch::NoopWatchRegistry,
                    "replica:test",
                )
                .unwrap();
        }
        Master {
            store: replica,
            repl,
            index: replica_index,
            hub_tx: m.hub_tx.clone(),
            rx: mpsc::unbounded_channel().1,
            env: ReplicationApplyEnv::new_inert(),
            _own: super::OwnWatches::of(&store::new_store(1)),
        }
    }

    /// A `SORT` whose `BY` and `GET` read a field of a hash it does not
    /// name, found past its time, and another client's `HINCRBY` of that
    /// field, run at the one moment it must not fall into: after the `SORT`
    /// has removed the field and before the field's `HDEL` is sent. The
    /// `SORT` holds the order of the keys it names, and the removal, made
    /// inside it, took none of the hash's: the `HINCRBY` went in there, and
    /// the stream carried `HINCRBY h f 1` and then `HDEL h f`. The master
    /// kept `f` = 1; a replica and a replay of the AOF, holding the field
    /// until its `HDEL`, made it 41 and then removed it (#415 review). Such
    /// a `SORT` now takes the command gate alone: the `HINCRBY` waits for
    /// it, and follows the `HDEL`. With `STORE` and without.
    #[test]
    fn a_write_to_a_hash_a_sort_reads_falls_after_the_hdel_of_its_due_field() {
        for store in [true, false] {
            let mut m = master();
            assert_eq!(m.run(&["RPUSH", "ids", "1", "2", "3"]), ":3\r\n");
            for (id, w) in [("1", "30"), ("2", "40"), ("3", "20")] {
                m.run(&["HSET", &format!("obj:{id}"), "w", w, "keep", "1"]);
            }
            m.run(&["HPEXPIRE", "obj:2", "60000", "FIELDS", "1", "w"]);
            m.due("obj:2", "w");
            let setup = m.sent();

            // The writer, held until the SORT has removed the field.
            let (go, wait_for_go) = std::sync::mpsc::channel::<()>();
            let (wrote, written) = std::sync::mpsc::channel::<String>();
            let writer = {
                let (store, repl, index, hub_tx) = (
                    Arc::clone(&m.store),
                    Arc::clone(&m.repl),
                    Arc::clone(&m.index),
                    m.hub_tx.clone(),
                );
                std::thread::spawn(move || {
                    wait_for_go.recv().unwrap();
                    let env = ReplicationApplyEnv::new_inert();
                    let cmd: [&[u8]; 4] = [b"HINCRBY", b"obj:2", b"w", b"1"];
                    let reply = run_as_client(
                        &env,
                        &cmd,
                        &store,
                        &repl,
                        &index,
                        &hub_tx,
                        &crate::watch::NoopWatchRegistry,
                    );
                    // Unheard if it came after the moment: the hook is gone by then.
                    let _ = wrote.send(String::from_utf8(reply).unwrap());
                })
            };
            // At the moment between the removal and its HDEL: the writer is
            // let go, and given time to finish there if anything lets it.
            let in_between = Arc::new(std::sync::Mutex::new(None::<String>));
            let hook = {
                let in_between = Arc::clone(&in_between);
                let mut go = Some(go);
                super::AfterRemoval::run(move |key| {
                    if key == b"obj:2"
                        && let Some(go) = go.take()
                    {
                        go.send(()).unwrap();
                        *in_between.lock().unwrap() =
                            written.recv_timeout(Duration::from_millis(500)).ok();
                    }
                })
            };
            let mut sort = vec![
                "SORT", "ids", "BY", "obj:*->w", "GET", "#", "GET", "obj:*->w",
            ];
            if store {
                sort.extend(["STORE", "dst"]);
            }
            let reply = m.run(&sort);
            drop(hook);
            writer.join().unwrap();
            assert_eq!(
                *in_between.lock().unwrap(),
                None,
                "the HINCRBY ran between the removal and its HDEL"
            );
            let sorted = "*6\r\n$1\r\n2\r\n$-1\r\n$1\r\n3\r\n$2\r\n20\r\n$1\r\n1\r\n$2\r\n30\r\n";
            if store {
                assert_eq!(reply, ":6\r\n");
            } else {
                assert_eq!(reply, sorted);
            }

            // The master: the field went, and HINCRBY made it anew, for good.
            assert_eq!(
                m.held("obj:2").unwrap(),
                [("keep".into(), "1".into()), ("w".into(), "1".into())]
            );
            assert_eq!(
                m.run(&["HTTL", "obj:2", "FIELDS", "1", "w"]),
                "*1\r\n:-1\r\n"
            );
            // The stream: the HDEL first.
            let stream = m.sent();
            let whole: Vec<Vec<String>> = setup
                .iter()
                .cloned()
                .chain(stream.iter().cloned())
                .collect();
            let at = |what: &[&str]| {
                stream
                    .iter()
                    .position(|c| c == &cmd(what))
                    .unwrap_or_else(|| panic!("no {what:?} in {stream:?}"))
            };
            assert!(
                at(&["HDEL", "obj:2", "w"]) < at(&["HINCRBY", "obj:2", "w", "1"]),
                "{stream:?}"
            );
            // A replica, and a replay of the AOF, end where the master is.
            for aof in [false, true] {
                let replayed = applied(&m, whole.clone(), aof);
                for key in ["obj:1", "obj:2", "obj:3"] {
                    assert_eq!(m.held(key), replayed.held(key), "{key}, aof {aof}");
                }
                if store {
                    let list =
                        "*6\r\n$1\r\n2\r\n$0\r\n\r\n$1\r\n3\r\n$2\r\n20\r\n$1\r\n1\r\n$2\r\n30\r\n";
                    assert_eq!(m.run(&["LRANGE", "dst", "0", "-1"]), list);
                    assert_eq!(
                        replayed.run(&["LRANGE", "dst", "0", "-1"]),
                        list,
                        "aof {aof}"
                    );
                }
            }
        }
    }

    /// Writers, readers and the sweep at once, near the deadlines: the
    /// stream, applied in order with fields held, gives what the master
    /// holds.
    #[test]
    fn the_stream_rebuilds_the_master_under_concurrent_writes_and_the_sweep() {
        let mut m = master();
        let keys: Vec<String> = (0..16).map(|i| format!("c:{i}")).collect();
        for key in &keys {
            m.run(&["HSET", key, "f", "0", "g", "0", "keep", "1"]);
        }
        // The writers put off or lift every deadline of `keys`, often before
        // it comes, so that none may expire at all. `c:due` is none of
        // theirs: its field expires under them.
        m.run(&["HSET", "c:due", "f", "0", "keep", "1"]);
        m.run(&["HPEXPIRE", "c:due", "1", "FIELDS", "1", "f"]);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sweeper = {
            let (stop, store, index) = (
                Arc::clone(&stop),
                Arc::clone(&m.store),
                Arc::clone(&m.index),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    super::sweep(&store, &index);
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        };
        let writers: Vec<_> = (0..4)
            .map(|w| {
                let (store, repl, index, hub_tx) = (
                    Arc::clone(&m.store),
                    Arc::clone(&m.repl),
                    Arc::clone(&m.index),
                    m.hub_tx.clone(),
                );
                let (keys, stop) = (keys.clone(), Arc::clone(&stop));
                std::thread::spawn(move || {
                    let env = ReplicationApplyEnv::new_inert();
                    let noop = crate::watch::NoopWatchRegistry;
                    let run = |cmd: &[&str]| {
                        let cmd: Vec<&[u8]> = cmd.iter().map(|part| part.as_bytes()).collect();
                        run_as_client(&env, &cmd, &store, &repl, &index, &hub_tx, &noop);
                    };
                    let mut i = 0usize;
                    while !stop.load(Ordering::Relaxed) {
                        let key = &keys[(i * 7 + w) % keys.len()];
                        let ms = ((i % 5) + 1).to_string();
                        match i % 6 {
                            0 => run(&["HPEXPIRE", key, &ms, "FIELDS", "2", "f", "g"]),
                            1 => run(&["HINCRBY", key, "f", "1"]),
                            2 => run(&["HSETNX", key, "g", "n"]),
                            3 => run(&["HPERSIST", key, "FIELDS", "1", "f"]),
                            4 => run(&["HPEXPIRE", key, "50", "XX", "FIELDS", "1", "g"]),
                            _ => run(&["HGETALL", key]),
                        }
                        i += 1;
                    }
                })
            })
            .collect();
        let started = std::time::Instant::now();
        let has_f = |m: &Master| {
            m.held("c:due")
                .is_some_and(|h| h.iter().any(|(f, _)| f == "f"))
        };
        while has_f(&m) {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the sweep never expired c:due's field"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(600).saturating_sub(started.elapsed()));
        stop.store(true, Ordering::Relaxed);
        for writer in writers {
            writer.join().unwrap();
        }
        sweeper.join().unwrap();
        let stream = m.sent();
        assert!(stream.iter().any(|c| c[0] == "HDEL"), "nothing expired");

        let replayed = applied(&m, stream, false);
        for key in keys.iter().map(String::as_str).chain(["c:due"]) {
            assert_eq!(m.held(key), replayed.held(key), "{key}");
        }
    }
}
