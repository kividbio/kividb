use std::sync::Arc;

use ahash::AHashMap;
use ahash::AHashSet;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::available_parallelism;

mod access;
mod entry;
mod shard_map;
pub use access::AccessScope;
#[allow(unused_imports)] // Not every item is named outside the store yet.
pub use shard_map::{
    EntryRef, EntryView, Leftover, Relocated, ShardMap, StrRef, Taken, ValueRef, Written,
};

/// Which shard `key` is in, of `shards` (a power of two).
///
/// 64-bit FNV-1a on every target (a 32-bit `usize` can't hold its
/// constants); the shard is taken from its low bits.
#[inline]
pub fn shard_index(shards: usize, key: &str) -> usize {
    shard_index_of_bytes(shards, key.as_bytes())
}

/// [`shard_index`] of a key as the command gave it, not yet checked to be
/// UTF-8 (the same shard: the hash is of the bytes).
#[inline]
pub fn shard_index_of_bytes(shards: usize, key: &[u8]) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in key {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) & (shards - 1)
}

#[inline(always)]
pub fn get_shard<'a>(db: &'a Db, key: &str) -> &'a Shard {
    &db[shard_index(db.len(), key)]
}

/// [`get_shard`] of a key as the command gave it, in bytes.
#[inline(always)]
pub fn get_shard_of_bytes<'a>(db: &'a Db, key: &[u8]) -> &'a Shard {
    &db[shard_index_of_bytes(db.len(), key)]
}

pub fn num_shards() -> usize {
    let cores = available_parallelism().map(|n| n.get()).unwrap_or(8);

    // 8 shards per core, rounded up to nearest power of 2
    // 8 cores  → 64 shards
    // 16 cores → 128 shards
    // 64 cores → 512 shards
    let target = cores * 8;
    target.next_power_of_two()
}

/// One shard of expiry times.
///
/// Its table behind a lock, and beside the lock how many of its keys have a
/// time to live.
///
/// Nearly every command asks whether its key has expired, and in most
/// datasets most shards hold no key with a time to live. A read that finds
/// its key finds the key's time in the same entry; a command that asks
/// before it locks the shard for what it does would take the lock twice. So
/// the shard keeps the count beside the lock, written by whoever held the
/// lock for writing as they let go, and read by anybody without taking
/// anything.
pub struct ShardLock {
    map: RwLock<ShardMap>,
    /// Keys in `map` with a time to live, as of the last writer to let go.
    ttls: AtomicUsize,
    /// Whether `map` holds any key, as of the last writer to let go.
    keys: AtomicBool,
}

/// The write lock of a [`ShardLock`]. It is the table, and on the way out
/// it leaves the count of keys with a time to live where a reader can see
/// it without the lock.
pub struct ShardWrite<'a> {
    map: parking_lot::RwLockWriteGuard<'a, ShardMap>,
    ttls: &'a AtomicUsize,
    keys: &'a AtomicBool,
}

impl std::ops::Deref for ShardWrite<'_> {
    type Target = ShardMap;
    #[inline(always)]
    fn deref(&self) -> &ShardMap {
        &self.map
    }
}

impl std::ops::DerefMut for ShardWrite<'_> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut ShardMap {
        &mut self.map
    }
}

impl Drop for ShardWrite<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        // Before the lock is released (the guard is dropped after this).
        // Its line is this thread's already: the lock's word is beside it.
        let ttls = self.map.ttl_len();
        if self.ttls.load(Ordering::Relaxed) != ttls {
            self.ttls.store(ttls, Ordering::Release);
        }
        let keys = !self.map.is_empty();
        if self.keys.load(Ordering::Relaxed) != keys {
            self.keys.store(keys, Ordering::Release);
        }
    }
}

impl ShardLock {
    pub fn new(map: ShardMap) -> Self {
        ShardLock {
            ttls: AtomicUsize::new(map.ttl_len()),
            keys: AtomicBool::new(!map.is_empty()),
            map: RwLock::new(map),
        }
    }

    #[inline(always)]
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, ShardMap> {
        self.map.read()
    }

    #[inline(always)]
    pub fn write(&self) -> ShardWrite<'_> {
        ShardWrite {
            map: self.map.write(),
            ttls: &self.ttls,
            keys: &self.keys,
        }
    }

    /// No key in this shard has a time to live, as of the last write to
    /// it. Costs one load and takes no lock. A writer giving the first one
    /// a time at this moment is not seen, exactly as if this had been asked
    /// a moment earlier.
    #[inline(always)]
    pub fn holds_no_ttl(&self) -> bool {
        self.ttls.load(Ordering::Acquire) == 0
    }

    /// The shard holds no key, as of the last write to it. One load, no
    /// lock: what eviction asks of every shard of every database before it
    /// samples, on every write while memory is full.
    #[inline(always)]
    pub fn holds_no_keys(&self) -> bool {
        !self.keys.load(Ordering::Acquire)
    }

    /// How many of this shard's keys have a time to live, as of the last
    /// write to it, read without the lock.
    #[inline]
    pub fn ttl_count(&self) -> usize {
        self.ttls.load(Ordering::Acquire)
    }

    /// Give the table back the room of keys that have gone (#113).
    ///
    /// A hash table grows as keys come and does not shrink as they go:
    /// after a reshard moved half the keys away, or a big delete, each
    /// shard's table kept the size it had at its largest, and the memory of
    /// every empty slot. Called on each shard about once a second (see
    /// `commands::helpers::shrink_sparse_tables`), as Redis resizes its
    /// tables from its cron.
    ///
    /// A table holding no more than 4/9 of what it has room for is shrunk
    /// to its keys and an eighth more, and so is the list of the keys with
    /// a time to live. Tables grow by doubling, so the smallest one with
    /// that room has room for less than twice it: a table just shrunk, and
    /// one just grown, are never sparse enough to shrink, and a shard does
    /// not go back and forth unless its keys do, by more than an eighth.
    /// The rehash holds the shard's lock no longer than the growth it
    /// undoes did. Returns whether it shrank.
    pub fn shrink_if_sparse(&self) -> bool {
        if !self.read().is_sparse() {
            return false;
        }
        let mut m = self.write();
        if !m.is_sparse() {
            return false;
        }
        let keep = m.len() + m.len() / 8;
        m.shrink_to(keep);
        true
    }
}

impl Default for ShardLock {
    fn default() -> Self {
        Self::new(ShardMap::new())
    }
}

/// A key, as the keyspace holds it: the bytes in the entry itself when there
/// are twenty-four or fewer, which is nearly every key anybody uses, and on
/// the heap only past that.
///
/// As a `String` every key was a block of its own beside the table: sixteen
/// bytes and an allocator's header for a key like `user:1234567`, and a
/// second place in memory to go to for every lookup, to compare it. It is
/// the same size as a `String` was, so the table is no larger.
pub type Key = compact_str::CompactString;

pub type Shard = Arc<ShardLock>;
pub type Db = Arc<Vec<Shard>>;
pub type Store = Arc<Vec<Db>>;

// Global label index for TimeSeries: "label=value" -> set of ts keys
pub type TsLabelIndex = AHashMap<String, AHashSet<String>>;
pub type SharedTsIndex = Arc<RwLock<TsLabelIndex>>;

pub fn new_ts_index() -> SharedTsIndex {
    Arc::new(RwLock::new(AHashMap::new()))
}

pub fn new_store(dbs: usize) -> Store {
    let num_shards = num_shards();
    // let total_shards = dbs * num_shards;
    // println!("Using {total_shards} shards");

    let dbs = (0..dbs)
        .map(|_| {
            let shards = (0..num_shards)
                .map(|_| Arc::new(ShardLock::default()))
                .collect::<Vec<_>>();
            Arc::new(shards)
        })
        .collect::<Vec<_>>();

    Arc::new(dbs)
}

macro_rules! mem_swap_rwlock_maps {
    ($a:expr, $b:expr) => {
        std::mem::swap(&mut *$a.write(), &mut *$b.write());
    };
}

/// What a flush handed back, so the caller can decide whether it is worth
/// asking the allocator to return pages to the OS.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Flushed {
    /// Entries removed: keys.
    pub entries: usize,
    /// Keys removed.
    pub keys: usize,
    /// Bucket slots whose backing allocation was freed. Zero when every shard
    /// took the keep-capacity path.
    pub slots_released: usize,
}

impl std::ops::AddAssign for Flushed {
    fn add_assign(&mut self, rhs: Self) {
        self.entries += rhs.entries;
        self.keys += rhs.keys;
        self.slots_released += rhs.slots_released;
    }
}

/// Bucket-slot capacity at or above which a flush drops a shard's table
/// instead of just emptying it.
///
/// `HashMap::clear()` keeps the bucket array at its high-water capacity. For a
/// large keyspace that is the single biggest thing a flush fails to give back —
/// 1.7M keys leave ~100 MB of empty tables resident, which no `MEMORY PURGE`
/// can reclaim because the process still owns them.
///
/// Dropping unconditionally would be worse for the common case though: the test
/// suite issues `FLUSHALL` before every test and benchmarks flush between runs,
/// where the tables are a few dozen slots, freeing them returns nothing to the
/// OS (the allocation is far below the mmap threshold), and the next fill pays
/// to grow them back from zero. So small tables keep the old behaviour and only
/// tables big enough to matter are released.
const SHARD_RELEASE_MIN_CAPACITY: usize = 128;

/// Entry count at or above which a flush is worth a `malloc_trim`.
///
/// The key strings and values freed by a flush are small allocations that glibc
/// parks in its arena free lists, so they stay resident until something asks for
/// them back. Trimming a nearly-empty heap costs little but is pure overhead in
/// the flush-between-tests loop, hence the floor.
const TRIM_MIN_ENTRIES: usize = 4096;

impl Flushed {
    /// Whether this flush freed enough for a `malloc_trim` to pay for itself.
    pub fn should_trim(&self) -> bool {
        self.slots_released > 0 || self.entries >= TRIM_MIN_ENTRIES
    }
}

/// Empty one shard, releasing its table when the table is large enough to be
/// worth releasing. See [`SHARD_RELEASE_MIN_CAPACITY`].
fn clear_shard_map(map: &mut ShardMap) -> Flushed {
    let entries = map.len();
    let capacity = map.capacity();
    if capacity >= SHARD_RELEASE_MIN_CAPACITY {
        *map = ShardMap::default();
        Flushed {
            entries,
            slots_released: capacity,
            ..Flushed::default()
        }
    } else {
        map.clear();
        Flushed {
            entries,
            ..Flushed::default()
        }
    }
}

/// Empty every shard of one database. Used by `FLUSHDB`.
pub fn clear_db(db: &Db) -> Flushed {
    let mut total = Flushed::default();
    for shard in db.iter() {
        total += clear_shard_map(&mut shard.write());
    }
    total.keys = total.entries;
    total
}

/// The values of one database, taken out of it to be freed elsewhere: what
/// `FLUSHDB ASYNC` and `FLUSHALL ASYNC` hand to the lazyfree thread. A shard
/// keeps a small table, as [`clear_shard_map`] does.
pub fn take_db(db: &Db) -> (Vec<ShardMap>, Flushed) {
    let mut taken = Vec::with_capacity(db.len());
    let mut total = Flushed::default();
    for shard in db.iter() {
        let mut map = shard.write();
        let capacity = map.capacity();
        total.entries += map.len();
        taken.push(if capacity >= SHARD_RELEASE_MIN_CAPACITY {
            total.slots_released += capacity;
            std::mem::take(&mut *map)
        } else {
            std::mem::replace(&mut *map, ShardMap::with_capacity(capacity))
        });
    }
    total.keys = total.entries;
    (taken, total)
}

/// Empty every database. Used by `FLUSHALL` and `CLUSTER RESET HARD`.
pub fn clear_store(store: &Store) -> Flushed {
    let mut total = Flushed::default();
    for db in store.iter() {
        total += clear_db(db);
    }
    total
}

#[inline]
pub fn clear_ts_label_index(ts: &SharedTsIndex) {
    ts.write().clear();
}

/// Rebuild the time-series label index from the series actually stored.
///
/// The index is derived state: it is maintained by `TS.CREATE` / `TS.ALTER`
/// as they run and is not part of a snapshot. Nothing rebuilt it after a
/// snapshot load or a replica full sync, so `TS.MGET` / `TS.MRANGE` /
/// `TS.QUERYINDEX` — everything that selects series by label — matched nothing
/// on a restarted server or a freshly synced replica, while the series
/// themselves were all present.
pub fn rebuild_ts_label_index(store: &Store, ts: &SharedTsIndex) {
    let mut index = TsLabelIndex::new();
    for db in store.iter() {
        for shard in db.iter() {
            for (key, value) in shard.read().iter() {
                if let ValueRef::TimeSeries(series) = value {
                    for (label, label_value) in series.read().labels.iter() {
                        index
                            .entry(format!("{label}={label_value}"))
                            .or_default()
                            .insert(key.to_string());
                    }
                }
            }
        }
    }
    *ts.write() = index;
}

/// Swap every shard HashMap between `live` and `staging`; layouts must match (same databases / shards).
pub fn swap_kv_with_staging(live: &Store, staging: &Store) {
    assert_eq!(live.len(), staging.len(), "staging store db count mismatch");
    for (db_live, db_staging) in live.iter().zip(staging.iter()) {
        assert_eq!(
            db_live.len(),
            db_staging.len(),
            "staging shard count mismatch"
        );
        for (s_live, s_staging) in db_live.iter().zip(db_staging.iter()) {
            mem_swap_rwlock_maps!(s_live, s_staging);
        }
    }
}

#[cfg(test)]
mod flush_tests {
    use super::*;
    use crate::types::KiviDBValue;

    fn val() -> KiviDBValue {
        KiviDBValue::String(crate::types::SharedBytes::from(&b"v"[..]))
    }

    fn fill(store: &Store, db_index: usize, count: usize) {
        for i in 0..count {
            let key = format!("flush-key-{i:012}");
            get_shard(&store[db_index], &key)
                .write()
                .insert(key.as_str(), val());
        }
    }

    fn total(db: &Db) -> (usize, usize) {
        db.iter().fold((0, 0), |(len, cap), s| {
            let m = s.read();
            (len + m.len(), cap + m.capacity())
        })
    }

    /// The bug this guards: `clear()` empties a shard but keeps its bucket array
    /// at high-water capacity, so flushing a large keyspace leaves the tables
    /// resident and no `MEMORY PURGE` can get them back.
    #[test]
    fn clear_db_releases_large_shard_tables() {
        let store = new_store(1);
        // Enough keys that every shard lands well above the release floor.
        fill(&store, 0, SHARD_RELEASE_MIN_CAPACITY * num_shards() * 4);

        let (len_before, cap_before) = total(&store[0]);
        assert!(len_before > 0);
        assert!(cap_before >= SHARD_RELEASE_MIN_CAPACITY);

        let flushed = clear_db(&store[0]);

        let (len_after, cap_after) = total(&store[0]);
        assert_eq!(len_after, 0, "flush must remove every key");
        assert_eq!(cap_after, 0, "flush must hand the bucket arrays back");
        assert_eq!(flushed.entries, len_before);
        assert_eq!(flushed.slots_released, cap_before);
        assert!(flushed.should_trim());
    }

    /// The flip side: the suite runs `FLUSHALL` before every test and benchmarks
    /// flush between runs. Those tables are tiny, freeing them returns nothing to
    /// the OS, and dropping them would make the next fill pay to grow from zero.
    #[test]
    fn clear_db_keeps_small_shard_tables() {
        let store = new_store(1);
        fill(&store, 0, 8);

        let (_, cap_before) = total(&store[0]);
        let flushed = clear_db(&store[0]);
        let (len_after, cap_after) = total(&store[0]);

        assert_eq!(len_after, 0, "flush must remove every key");
        assert_eq!(cap_after, cap_before, "small tables keep their capacity");
        assert_eq!(flushed.slots_released, 0);
        assert!(
            !flushed.should_trim(),
            "a handful of keys must not trigger a malloc_trim"
        );
    }

    #[test]
    fn clear_store_empties_every_db_and_its_times_to_live() {
        let store = new_store(4);
        for db in 0..4 {
            fill(&store, db, 64);
            let key = format!("flush-key-{:012}", 7);
            assert!(get_shard(&store[db], &key).write().set_expires(&key, 1));
        }

        let flushed = clear_store(&store);

        assert_eq!(flushed.entries, 4 * 64);
        for db in 0..4 {
            assert_eq!(total(&store[db]).0, 0, "db{db} still has keys");
            assert!(
                store[db]
                    .iter()
                    .all(|s| s.holds_no_ttl() && s.read().ttl_len() == 0),
                "db{db} still has times to live"
            );
        }
    }

    /// `FLUSHDB` is scoped to the selected database.
    #[test]
    fn clear_db_leaves_other_dbs_alone() {
        let store = new_store(2);
        fill(&store, 0, 32);
        fill(&store, 1, 32);

        clear_db(&store[0]);

        assert_eq!(total(&store[0]).0, 0);
        assert_eq!(total(&store[1]).0, 32, "FLUSHDB must not touch other dbs");
    }

    /// A flush big enough to matter is worth a trim even when every individual
    /// table stayed under the release floor.
    #[test]
    fn should_trim_tracks_entry_volume() {
        assert!(!Flushed::default().should_trim());
        assert!(
            !Flushed {
                entries: TRIM_MIN_ENTRIES - 1,
                slots_released: 0,
                ..Flushed::default()
            }
            .should_trim()
        );
        assert!(
            Flushed {
                entries: TRIM_MIN_ENTRIES,
                slots_released: 0,
                ..Flushed::default()
            }
            .should_trim()
        );
        assert!(
            Flushed {
                entries: 1,
                slots_released: 1,
                ..Flushed::default()
            }
            .should_trim()
        );
    }

    /// Flushing must leave the shards usable, not just empty.
    #[test]
    fn store_is_writable_after_flush() {
        let store = new_store(1);
        fill(&store, 0, SHARD_RELEASE_MIN_CAPACITY * num_shards() * 4);
        clear_db(&store[0]);

        fill(&store, 0, 100);
        assert_eq!(total(&store[0]).0, 100);
        let key = "flush-key-000000000042".to_string();
        assert!(get_shard(&store[0], &key).read().contains_key(key.as_str()));
    }
}

#[cfg(test)]
mod staging_swap_tests {
    use super::*;
    use crate::types::KiviDBValue;

    /// Full-resync uses swap so live never serves an empty cleared map while RDB is on the wire.
    #[test]
    fn swap_kv_with_staging_moves_staging_maps_into_live() {
        let live = new_store(1);
        let staging = new_store(1);
        let k = "swap-key";
        let val = KiviDBValue::String(crate::types::SharedBytes::from(&b"from-staging"[..]));
        get_shard(&staging[0], k).write().insert(k, val);
        assert!(get_shard(&live[0], k).read().get(k).is_none());
        swap_kv_with_staging(&live, &staging);
        match get_shard(&live[0], k)
            .read()
            .get(k)
            .expect("key on live after swap")
        {
            ValueRef::String(b) => assert_eq!(&b[..], b"from-staging"),
            _ => panic!("expected string value"),
        }
    }

    #[test]
    fn a_swap_moves_times_to_live_with_their_keys() {
        let live = new_store(1);
        let staging = new_store(1);
        let k = "exp-key";
        let val = KiviDBValue::String(crate::types::SharedBytes::from(&b"v"[..]));
        get_shard(&staging[0], k)
            .write()
            .insert_expiring(k, val, Some(1_234));
        assert!(!get_shard(&staging[0], k).holds_no_ttl());
        swap_kv_with_staging(&live, &staging);
        assert_eq!(get_shard(&live[0], k).read().expires(k), Some(1_234));
        assert!(!get_shard(&live[0], k).holds_no_ttl());
        assert!(get_shard(&staging[0], k).holds_no_ttl());
    }
}

#[cfg(test)]
mod shrink_tests {
    use super::*;
    use crate::types::KiviDBValue;

    fn table(keys: usize) -> ShardLock {
        let mut map = ShardMap::new();
        for i in 0..keys {
            map.insert(
                format!("key-{i}"),
                KiviDBValue::String(crate::types::SharedBytes::from(i.to_string().as_str())),
            );
        }
        ShardLock::new(map)
    }

    fn value(map: &ShardLock, key: &str) -> Option<Vec<u8>> {
        match map.read().get(key) {
            Some(ValueRef::String(b)) => Some(b.to_vec()),
            _ => None,
        }
    }

    #[test]
    fn a_table_keys_have_mostly_left_shrinks_once() {
        let map = table(100_000);
        let grown = map.read().capacity();
        for i in (0..100_000).filter(|i| i % 10 != 0) {
            map.write().remove(&format!("key-{i}"));
        }
        assert!(map.shrink_if_sparse());
        let shrunk = map.read().capacity();
        assert!(shrunk * 4 < grown, "{grown} -> {shrunk}");
        assert!(shrunk >= 10_000 + 10_000 / 8, "no room left: {shrunk}");
        assert_eq!(map.read().len(), 10_000);
        assert_eq!(value(&map, "key-990"), Some(b"990".to_vec()));
        // Not again until more keys go.
        assert!(!map.shrink_if_sparse());
    }

    #[test]
    fn a_list_of_times_to_live_most_have_left_shrinks() {
        let map = table(10_000);
        for i in 0..10_000 {
            map.write().set_expires(&format!("key-{i}"), u64::MAX / 2);
        }
        let grown = {
            let m = map.read();
            assert_eq!(m.ttl_len(), 10_000);
            m.ttl_len()
        };
        for i in (0..10_000).filter(|i| i % 10 != 0) {
            assert!(map.write().persist(&format!("key-{i}")));
        }
        // The table is full; the list is not.
        assert!(map.shrink_if_sparse());
        assert_eq!(map.read().ttl_len(), 1_000);
        assert_eq!(map.read().len(), 10_000);
        assert_eq!(map.read().expires("key-990"), Some(u64::MAX / 2));
        assert_eq!(map.read().expires("key-991"), None);
        assert!(grown > 1_000);
        assert!(!map.shrink_if_sparse());
    }

    #[test]
    fn full_small_and_just_grown_tables_are_left_alone() {
        let map = table(100_000);
        assert!(!map.shrink_if_sparse());
        let small = table(100);
        small.write().clear();
        assert!(!small.shrink_if_sparse());
        // Grown by one key past its room: half empty, but not sparse.
        let map = table(0);
        let mut i = 0u64;
        loop {
            let before = map.read().capacity();
            map.write().insert_str(format!("key-{i}"), b"v");
            i += 1;
            if before >= 4096 && map.read().capacity() > before {
                break;
            }
        }
        assert!(!map.shrink_if_sparse());
    }
}

#[cfg(test)]
mod shard_lock_tests {
    use super::*;
    use crate::types::KiviDBValue;

    fn s() -> KiviDBValue {
        KiviDBValue::String(crate::types::SharedBytes::from(&b"v"[..]))
    }

    /// The count readers see without the lock follows every way a writer
    /// can give a key a time to live or take it away, because it is taken
    /// as the writer lets go.
    #[test]
    fn the_count_follows_what_writers_do() {
        let cell = ShardLock::default();
        assert!(cell.holds_no_ttl());
        assert!(cell.holds_no_keys());
        cell.write().insert_str("k", b"v");
        assert!(!cell.holds_no_keys());
        cell.write().remove("k");
        assert!(cell.holds_no_keys());

        cell.write().insert_expiring("a", s(), Some(5));
        assert!(!cell.holds_no_ttl());

        cell.write().insert_str("b", b"x");
        cell.write().set_expires("b", 9);
        assert_eq!(cell.ttl_count(), 2);
        cell.write().remove("a");
        assert!(!cell.holds_no_ttl());
        // An overwrite keeps it; one that says so takes it away.
        cell.write().insert("b", s());
        assert_eq!(cell.read().expires("b"), Some(9));
        cell.write().insert_expiring("b", s(), None);
        assert!(cell.holds_no_ttl());

        cell.write().insert_expiring("c", s(), Some(1));
        assert!(cell.write().persist("c"));
        assert!(cell.holds_no_ttl());
        cell.write().set_expires("c", 3);
        cell.write().clear();
        assert!(cell.holds_no_ttl());

        // Swapped whole, as SWAPDB and a flush do.
        let other = ShardLock::default();
        other.write().insert_expiring("c", s(), Some(2));
        std::mem::swap(&mut *cell.write(), &mut *other.write());
        assert!(!cell.holds_no_ttl());
        assert!(other.holds_no_ttl());
        assert!(!cell.holds_no_keys());
        assert!(other.holds_no_keys());
        assert_eq!(cell.read().ttl_len(), 1);
    }
}
