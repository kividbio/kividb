// src/field_expiry.rs
//
// Hash fields with a TTL expire when their time comes — not when somebody
// next looks at them.
//
// A field's TTL (`HEXPIRE`, `HSETEX`, `HGETEX`) used to be acted on only by a
// command that touched the field. A hash nobody read kept its expired fields
// for ever; one whose fields had all expired stayed in the keyspace, counted
// by `DBSIZE` and found by `SCAN`; and nothing was published when a field went.
//
// This keeps the list of hashes that have field TTLs, with the earliest time
// one of each is due, and the background expiry task (`helpers::
// spawn_active_expire_task`) calls [`sweep`] ten times a second. A hash that
// is due has its expired fields removed, `hexpired` published for them, and
// — if that left it empty — is deleted, with `del` published.
//
// Replicas are not told. A field's TTL travels to them as the moment it
// expires (`HPEXPIREAT`), and each runs this same sweep.

use crate::store::ValueRef;
use ahash::AHashMap;
use parking_lot::Mutex;
use std::time::Instant;

use crate::store::Store;
use crate::vector::IndexStore;

/// A hash with field TTLs: which server's store it is in, which database,
/// and its key.
type Entry = (usize, usize, String);

/// The hashes to watch, each with the earliest time one of its fields is due.
static WATCHED: Mutex<Option<AHashMap<Entry, Instant>>> = Mutex::new(None);

fn store_id(store: &Store) -> usize {
    std::sync::Arc::as_ptr(store) as *const () as usize
}

/// Note that the hash at `key` has a field due at `at`.
pub fn track(store: &Store, db: usize, key: &str, at: Instant) {
    let mut watched = WATCHED.lock();
    let earliest = watched
        .get_or_insert_with(AHashMap::new)
        .entry((store_id(store), db, key.to_string()))
        .or_insert(at);
    if at < *earliest {
        *earliest = at;
    }
}

/// The keys of database `db` that hold a hash with field TTLs. Reading such
/// a hash can drop a field that is past its time before the sweep gets to
/// it, and the indexes hear of it only from the sweep: a query that answers
/// from postings tries these documents themselves (see
/// `commands::search::plan`).
pub fn watched_keys(store: &Store, db: usize) -> Vec<String> {
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
pub fn track_key(store: &Store, db: usize, key: &str) {
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
            let due: Vec<(String, Instant)> = shard
                .read()
                .iter()
                .filter_map(|(key, value)| match value {
                    ValueRef::HashEx(hash) => hash
                        .read()
                        .expiries
                        .values()
                        .min()
                        .map(|at| (key.to_string(), *at)),
                    _ => None,
                })
                .collect();
            for (key, at) in due {
                track(store, db, &key, at);
            }
        }
    }
}

/// Expire the fields that are due, in every hash of `store` that is being
/// watched.
pub fn sweep(store: &Store, index_store: &IndexStore) {
    let id = store_id(store);
    let now = Instant::now();
    let due: Vec<(usize, String)> = {
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
    let hub = crate::keyspace_events::hub_for(store);

    for (db, key) in due {
        // What is to become of the watch: `Some(at)` to keep it, for the
        // next field that is due; `None` to drop it.
        let mut next: Option<Instant> = None;
        let mut expired: Vec<String> = Vec::new();
        let mut emptied = false;
        // Ordered against the writes to the key, as a command is (#271): an
        // `HSET` holds the hash between finding it and writing to it, and a
        // hash this emptied and removed in between took that field with it.
        let command = crate::command_gate::enter();
        let order = crate::write_order::lock_keys(db, std::iter::once(key.as_bytes()));
        if let Some(db_ref) = store.get(db) {
            let hash = match crate::store::get_shard(db_ref, &key)
                .read()
                .get(key.as_str())
            {
                Some(ValueRef::HashEx(hash)) => Some(std::sync::Arc::clone(hash)),
                _ => None,
            };
            if let Some(hash) = hash {
                let mut inner = hash.write();
                expired = inner
                    .expiries
                    .iter()
                    .filter(|(_, at)| **at <= now)
                    .map(|(field, _)| field.clone())
                    .collect();
                for field in &expired {
                    inner.evict_field(field);
                }
                emptied = inner.fields.is_empty();
                next = inner.expiries.values().min().copied();
            }
        }

        if emptied {
            // Nothing is left of it. Checked again under the shard's lock: a
            // field may have been added since.
            let removed = {
                let db_ref = &store[db];
                let mut map = crate::store::get_shard(db_ref, &key).write();
                let still_empty = matches!(
                    map.get(key.as_str()),
                    Some(ValueRef::HashEx(hash)) if hash.read().fields.is_empty()
                );
                still_empty && map.remove(key.as_str()).is_some()
            };
            if removed {
                crate::vector::maybe_unindex(index_store, db, &key);
                next = None;
            }
            emptied = removed;
        }
        // Fields went: the indexes that cover the hash are brought up to
        // date with what it holds now. Also when none was found here to
        // expire: a read that came first dropped them (and left the indexes
        // as they were), and once the watch is dropped below, queries stop
        // trying this document themselves (see `watched_keys`). Under the
        // key's write order, as a command brings the indexes up to date with
        // its write. Out of it, an `HSET` that ran between this reading the
        // hash and writing its postings was written over with what this had
        // read, and the hash, no longer watched, was not tried again (#364).
        if !emptied {
            crate::vector::reindex_key(index_store, store, db, &key, crate::vector::Write::Refresh);
        }
        // The watch too, as a command that gives a field a time to live
        // tracks the hash under the order. Out of it, an `HPEXPIRE` that ran
        // meanwhile tracked its field onto this watch, which was then
        // dropped: the field was never swept, and queries trusted the
        // postings for the hash from then on (#364).
        #[cfg(test)]
        crate::test_hooks::at("sweep: watch");
        settle_watch(id, db, &key, next, now);
        drop(order);
        drop(command);

        if let Some(hub) = &hub {
            if !expired.is_empty() {
                crate::keyspace_events::notify(
                    hub,
                    db,
                    crate::keyspace_events::KS_HASH,
                    "hexpired",
                    &key,
                );
                crate::keyspace_events::notify_subkeys(
                    hub,
                    db,
                    "hexpired",
                    &key,
                    expired.iter().map(|f| f.as_bytes()),
                );
            }
            if emptied {
                crate::keyspace_events::notify(
                    hub,
                    db,
                    crate::keyspace_events::KS_GENERIC,
                    "del",
                    &key,
                );
            }
        }
        for _ in &expired {
            crate::stats::record_expired_subkey();
        }
    }
}

/// What the sweep of `key`, which began at `now`, leaves of its watch: kept
/// for `next`, the time its next field is due, or dropped. Under the key's
/// write order.
fn settle_watch(id: usize, db: usize, key: &str, next: Option<Instant>, now: Instant) {
    let mut watched = WATCHED.lock();
    if let Some(watched) = watched.as_mut() {
        let entry = (id, db, key.to_string());
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

    #[test]
    fn due_fields_go_and_an_empty_hash_goes_with_them() {
        let store = crate::store::new_store(1);
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
        WATCHED
            .lock()
            .as_mut()
            .unwrap()
            .retain(|(s, _, _), _| *s != store_id(&store));
        rescan(&store);
        assert_eq!(watched_in(&store), 1);
    }
}
