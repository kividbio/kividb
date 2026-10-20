use std::sync::Arc;
use std::time::SystemTime;

use ahash::AHashMap;
use ahash::AHashSet;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;

use crate::types::KiviDBValue;

/// Which shard `key` is in, of `shards` (a power of two). The keyspace and
/// the expiry store have the same number, so one index serves both: a
/// command that looks in both hashes its key once.
#[inline]
pub fn shard_index(shards: usize, key: &str) -> usize {
    let mut hash: usize = 0xcbf29ce484222325;
    for byte in key.bytes() {
        hash ^= byte as usize;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash & (shards - 1)
}

pub fn get_shard<'a>(db: &'a Db, key: &str) -> &'a Shard {
    &db[shard_index(db.len(), key)]
}

pub fn get_expiry_shard<'a>(db: &'a ExpiryDb, key: &str) -> &'a ExpiryShard {
    &db[shard_index(db.len(), key)]
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
/// Nearly every command asks whether its key has expired, and in most
/// datasets most shards hold no expiry time at all. Taking the lock to find
/// that out writes the lock's word, which every thread asking shares: on many
/// cores the cache line spends its time moving between them. So the shard
/// keeps a count beside the lock, written by whoever held the lock for
/// writing as they let go, and read by anybody without taking anything.
pub struct ExpiryCell {
    /// Entries in `map`, as of the last writer to let go of it.
    len: AtomicUsize,
    map: RwLock<AHashMap<Key, SystemTime>>,
}

/// The write lock of an [`ExpiryCell`]. It is the map, and on the way out it
/// leaves the map's size where a reader can see it without the lock.
pub struct ExpiryWrite<'a> {
    map: parking_lot::RwLockWriteGuard<'a, AHashMap<Key, SystemTime>>,
    len: &'a AtomicUsize,
}

impl std::ops::Deref for ExpiryWrite<'_> {
    type Target = AHashMap<Key, SystemTime>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl std::ops::DerefMut for ExpiryWrite<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl Drop for ExpiryWrite<'_> {
    #[inline]
    fn drop(&mut self) {
        // Before the lock is released (the guard is dropped after this).
        self.len.store(self.map.len(), Ordering::Release);
    }
}

impl ExpiryCell {
    pub fn new() -> Self {
        ExpiryCell {
            len: AtomicUsize::new(0),
            map: RwLock::new(AHashMap::new()),
        }
    }

    #[inline]
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, AHashMap<Key, SystemTime>> {
        self.map.read()
    }

    #[inline]
    pub fn write(&self) -> ExpiryWrite<'_> {
        ExpiryWrite {
            map: self.map.write(),
            len: &self.len,
        }
    }

    /// No key in this shard has an expiry time, as of the last write to it.
    /// Costs one load and takes no lock. A writer adding the first one at
    /// this moment is not seen, exactly as if this had been asked a moment
    /// earlier.
    #[inline]
    pub fn holds_nothing(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }
}

impl Default for ExpiryCell {
    fn default() -> Self {
        Self::new()
    }
}

pub type ExpiryShard = Arc<ExpiryCell>;
pub type ExpiryDb = Arc<Vec<ExpiryShard>>;
pub type ExpiryStore = Arc<Vec<ExpiryDb>>;

/// A key, as the keyspace holds it: the bytes in the entry itself when there
/// are twenty-four or fewer, which is nearly every key anybody uses, and on
/// the heap only past that.
///
/// As a `String` every key was a block of its own beside the table: sixteen
/// bytes and an allocator's header for a key like `user:1234567`, and a
/// second place in memory to go to for every lookup, to compare it. It is
/// the same size as a `String` was, so the table is no larger.
pub type Key = compact_str::CompactString;

pub type Shard = Arc<RwLock<AHashMap<Key, KiviDBValue>>>;
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
                .map(|_| Arc::new(RwLock::new(AHashMap::new())))
                .collect::<Vec<_>>();
            Arc::new(shards)
        })
        .collect::<Vec<_>>();

    Arc::new(dbs)
}

pub fn new_expiry_store(dbs: usize) -> ExpiryStore {
    let num_shards = num_shards();
    let dbs = (0..dbs)
        .map(|_| {
            let shards = (0..num_shards)
                .map(|_| Arc::new(ExpiryCell::new()))
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
    /// Entries removed: keys, and the TTLs of those that had one.
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
fn clear_shard_map<K, V>(map: &mut AHashMap<K, V>) -> Flushed {
    let entries = map.len();
    let capacity = map.capacity();
    if capacity >= SHARD_RELEASE_MIN_CAPACITY {
        *map = AHashMap::new();
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
        total += clear_shard_map(&mut *shard.write());
    }
    total.keys = total.entries;
    total
}

/// The values of one database, taken out of it to be freed elsewhere: what
/// `FLUSHDB ASYNC` and `FLUSHALL ASYNC` hand to the lazyfree thread. A shard
/// keeps a small table, as [`clear_shard_map`] does.
pub fn take_db(db: &Db) -> (Vec<AHashMap<Key, KiviDBValue>>, Flushed) {
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
            std::mem::replace(&mut *map, AHashMap::with_capacity(capacity))
        });
    }
    total.keys = total.entries;
    (taken, total)
}

/// Empty every expiry shard of one database.
pub fn clear_expiry_db(db: &ExpiryDb) -> Flushed {
    let mut total = Flushed::default();
    for shard in db.iter() {
        total += clear_shard_map(&mut *shard.write());
    }
    total
}

/// Empty every database. Used by `FLUSHALL` and `CLUSTER RESET HARD`.
pub fn clear_store(store: &Store, expiry: &ExpiryStore) -> Flushed {
    let mut total = Flushed::default();
    for db in store.iter() {
        total += clear_db(db);
    }
    for db in expiry.iter() {
        total += clear_expiry_db(db);
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
                if let KiviDBValue::TimeSeries(series) = value {
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

/// Same as [`swap_kv_with_staging`] for expiry shards.
pub fn swap_expiry_with_staging(live: &ExpiryStore, staging: &ExpiryStore) {
    assert_eq!(
        live.len(),
        staging.len(),
        "staging expiry db count mismatch"
    );
    for (db_live, db_staging) in live.iter().zip(staging.iter()) {
        assert_eq!(
            db_live.len(),
            db_staging.len(),
            "staging expiry shard count mismatch"
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
    use std::time::SystemTime;

    fn val() -> KiviDBValue {
        KiviDBValue::String(crate::types::SharedBytes::from(&b"v"[..]))
    }

    fn fill(store: &Store, db_index: usize, count: usize) {
        for i in 0..count {
            let key = format!("flush-key-{i:012}");
            get_shard(&store[db_index], &key)
                .write()
                .insert(key.into(), val());
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
    fn clear_store_empties_every_db_and_expiry() {
        let store = new_store(4);
        let expiry = new_expiry_store(4);
        for db in 0..4 {
            fill(&store, db, 64);
            let key = format!("exp-{db}");
            get_expiry_shard(&expiry[db], &key)
                .write()
                .insert(Key::from(key), SystemTime::now());
        }

        let flushed = clear_store(&store, &expiry);

        assert_eq!(flushed.entries, 4 * 64 + 4);
        for db in 0..4 {
            assert_eq!(total(&store[db]).0, 0, "db{db} still has keys");
            assert_eq!(
                expiry[db].iter().map(|s| s.read().len()).sum::<usize>(),
                0,
                "db{db} still has expiries"
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
    use std::time::SystemTime;

    /// Full-resync uses swap so live never serves an empty cleared map while RDB is on the wire.
    #[test]
    fn swap_kv_with_staging_moves_staging_maps_into_live() {
        let live = new_store(1);
        let staging = new_store(1);
        let k = "swap-key";
        let val = KiviDBValue::String(crate::types::SharedBytes::from(&b"from-staging"[..]));
        get_shard(&staging[0], k).write().insert(k.into(), val);
        assert!(get_shard(&live[0], k).read().get(k).is_none());
        swap_kv_with_staging(&live, &staging);
        match get_shard(&live[0], k)
            .read()
            .get(k)
            .expect("key on live after swap")
        {
            KiviDBValue::String(b) => assert_eq!(&b[..], b"from-staging"),
            _ => panic!("expected string value"),
        }
    }

    #[test]
    fn swap_expiry_with_staging_moves_expiry_shards() {
        let live = new_expiry_store(1);
        let staging = new_expiry_store(1);
        let k = "exp-key";
        let t = SystemTime::now();
        get_expiry_shard(&staging[0], k)
            .write()
            .insert(Key::from(k), t);
        assert!(get_expiry_shard(&live[0], k).read().get(k).is_none());
        swap_expiry_with_staging(&live, &staging);
        assert_eq!(get_expiry_shard(&live[0], k).read().get(k), Some(&t));
    }
}

#[cfg(test)]
mod expiry_cell_tests {
    use super::ExpiryCell;
    use std::time::SystemTime;

    /// The count readers see without the lock follows every way a writer
    /// can change the map, because it is taken as the writer lets go.
    #[test]
    fn the_count_follows_what_writers_do() {
        let cell = ExpiryCell::new();
        assert!(cell.holds_nothing());

        cell.write()
            .insert(crate::store::Key::from("a"), SystemTime::now());
        assert!(!cell.holds_nothing());

        cell.write()
            .entry(crate::store::Key::from("b"))
            .or_insert_with(SystemTime::now);
        cell.write().remove("a");
        assert!(!cell.holds_nothing());

        cell.write().retain(|_, _| false);
        assert!(cell.holds_nothing());

        // Swapped whole, as SWAPDB and a flush do.
        let other = ExpiryCell::new();
        other
            .write()
            .insert(crate::store::Key::from("c"), SystemTime::now());
        std::mem::swap(&mut *cell.write(), &mut *other.write());
        assert!(!cell.holds_nothing());
        assert!(other.holds_nothing());
        assert_eq!(cell.read().len(), 1);
    }
}
