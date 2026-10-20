//! What happens when `maxmemory` is reached: Redis's `maxmemory-policy`.
//!
//! With `noeviction` — the default — a write that would use more memory is
//! refused. With any other policy the server deletes keys to make room for
//! it, choosing as Redis does: it looks at a few keys picked at random
//! (`maxmemory-samples`) and deletes the one the policy likes least, until
//! it is under the limit or there is nothing left that the policy lets it
//! delete.
//!
//! | policy            | among                | deletes the one               |
//! |-------------------|----------------------|-------------------------------|
//! | `allkeys-lru`     | every key            | least recently used           |
//! | `allkeys-lfu`     | every key            | least frequently used         |
//! | `allkeys-lrm`     | every key            | least recently modified (8.6) |
//! | `allkeys-random`  | every key            | whichever comes up            |
//! | `volatile-lru`    | keys with a TTL      | least recently used           |
//! | `volatile-lfu`    | keys with a TTL      | least frequently used         |
//! | `volatile-lrm`    | keys with a TTL      | least recently modified       |
//! | `volatile-random` | keys with a TTL      | whichever comes up            |
//! | `volatile-ttl`    | keys with a TTL      | nearest to expiring           |
//!
//! When a key was last used, or modified, and how often, is recorded here
//! and not in the keyspace, and only while a policy that asks for it is
//! selected: with `noeviction` a command pays one atomic load for all of
//! this. The records are kept by a hash of the key, so two keys can share
//! one; the choice is approximate anyway.
//!
//! An eviction is a deletion like any other: replicas and the AOF are sent a
//! `DEL`, a `WATCH` on the key is tripped, clients caching it are told, and
//! `evicted` is published to keyspace subscribers.

use crate::commands::ctx::Ctx;
use crate::store::Store;
use ahash::AHashMap;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime};

// ── The policy ────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Policy {
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    AllKeysLrm,
    AllKeysRandom,
    VolatileLru,
    VolatileLfu,
    VolatileLrm,
    VolatileRandom,
    VolatileTtl,
}

const POLICIES: [(Policy, &str); 10] = [
    (Policy::NoEviction, "noeviction"),
    (Policy::AllKeysLru, "allkeys-lru"),
    (Policy::AllKeysLfu, "allkeys-lfu"),
    (Policy::AllKeysLrm, "allkeys-lrm"),
    (Policy::AllKeysRandom, "allkeys-random"),
    (Policy::VolatileLru, "volatile-lru"),
    (Policy::VolatileLfu, "volatile-lfu"),
    (Policy::VolatileLrm, "volatile-lrm"),
    (Policy::VolatileRandom, "volatile-random"),
    (Policy::VolatileTtl, "volatile-ttl"),
];

/// The names, for an error message.
pub const POLICY_NAMES: &str = "volatile-lru, allkeys-lru, volatile-lfu, allkeys-lfu, \
     volatile-lrm, allkeys-lrm, volatile-random, allkeys-random, volatile-ttl, noeviction";

impl Policy {
    pub fn parse(name: &str) -> Option<Self> {
        POLICIES
            .iter()
            .find(|(_, n)| n.eq_ignore_ascii_case(name))
            .map(|(policy, _)| *policy)
    }

    pub fn name(self) -> &'static str {
        POLICIES[self as usize].1
    }

    /// Only keys with a TTL may go.
    fn volatile(self) -> bool {
        matches!(
            self,
            Policy::VolatileLru
                | Policy::VolatileLfu
                | Policy::VolatileLrm
                | Policy::VolatileRandom
                | Policy::VolatileTtl
        )
    }

    pub fn is_lfu(self) -> bool {
        matches!(self, Policy::AllKeysLfu | Policy::VolatileLfu)
    }

    /// It chooses by how keys have been used, so their use is recorded.
    fn records_use(self) -> bool {
        !matches!(
            self,
            Policy::NoEviction
                | Policy::AllKeysRandom
                | Policy::VolatileRandom
                | Policy::VolatileTtl
        )
    }
}

static POLICY: AtomicU8 = AtomicU8::new(Policy::NoEviction as u8);
static RECORDING: AtomicBool = AtomicBool::new(false);
/// `key-access-tracking`: record use whatever the policy.
static TRACK_ALWAYS: AtomicBool = AtomicBool::new(false);
static SAMPLES: AtomicUsize = AtomicUsize::new(5);
static LFU_LOG_FACTOR: AtomicU32 = AtomicU32::new(10);
static LFU_DECAY_MINUTES: AtomicU32 = AtomicU32::new(1);

pub fn policy() -> Policy {
    POLICIES[POLICY.load(Ordering::Relaxed) as usize].0
}

pub fn set_policy(policy: Policy) {
    let before = self::policy();
    POLICY.store(policy as u8, Ordering::Relaxed);
    let recording = policy.records_use() || key_access_tracking();
    RECORDING.store(recording, Ordering::Relaxed);
    if !recording || policy.is_lfu() != before.is_lfu() {
        // What was recorded is of no use to the new policy.
        for stripe in RECORDS.iter() {
            *stripe.lock() = AHashMap::new();
        }
    }
}

/// `key-access-tracking`: whether a key's use is recorded under every
/// policy, so that `OBJECT IDLETIME` and `FREQ` report it under
/// `noeviction` too, as Redis does (#21). Off by default: recording costs
/// every command about 500 instructions (11-13% of a GET or SET) and a key
/// about 53 bytes (195 -> 248 at 100 B), measured on 2026-10-20. Under an
/// LRU or LFU policy it is recorded either way.
pub fn key_access_tracking() -> bool {
    TRACK_ALWAYS.load(Ordering::Relaxed)
}

pub fn set_key_access_tracking(on: bool) {
    TRACK_ALWAYS.store(on, Ordering::Relaxed);
    set_policy(policy());
}

/// How many keys are looked at to choose one (`maxmemory-samples`).
pub fn samples() -> usize {
    SAMPLES.load(Ordering::Relaxed)
}

pub fn set_samples(n: usize) {
    SAMPLES.store(n.clamp(1, 64), Ordering::Relaxed);
}

pub fn lfu_log_factor() -> u32 {
    LFU_LOG_FACTOR.load(Ordering::Relaxed)
}

pub fn set_lfu_log_factor(n: u32) {
    LFU_LOG_FACTOR.store(n, Ordering::Relaxed);
}

pub fn lfu_decay_time() -> u32 {
    LFU_DECAY_MINUTES.load(Ordering::Relaxed)
}

pub fn set_lfu_decay_time(minutes: u32) {
    LFU_DECAY_MINUTES.store(minutes, Ordering::Relaxed);
}

/// The settings a server starts with.
pub fn configure(config: &crate::config::Config) {
    TRACK_ALWAYS.store(config.key_access_tracking, Ordering::Relaxed);
    set_policy(Policy::parse(&config.maxmemory_policy).unwrap_or(Policy::NoEviction));
    set_samples(config.maxmemory_samples);
    set_lfu_log_factor(config.lfu_log_factor);
    set_lfu_decay_time(config.lfu_decay_time);
}

// ── How keys have been used ───────────────────────────────────────────────────

/// A new key's frequency count, as in Redis (`LFU_INIT_VAL`): above zero, so
/// that it is not the first to go before it has had a chance to be used.
const LFU_INITIAL: u8 = 5;

#[derive(Clone, Copy)]
struct Use {
    /// Seconds on [`clock`] when it was last read or written.
    used: u32,
    /// And when it was last written.
    modified: u32,
    /// Logarithmic count of uses, decaying.
    frequency: u8,
    /// Minutes on [`clock`] when `frequency` was last decayed.
    decayed: u32,
}

const RECORD_STRIPES: usize = 64;
/// A stripe that has grown past this is looked at: see [`trim`].
const STRIPE_FLOOR: usize = 4096;

static RECORDS: LazyLock<Vec<Mutex<AHashMap<u64, Use>>>> = LazyLock::new(|| {
    (0..RECORD_STRIPES)
        .map(|_| Mutex::new(AHashMap::new()))
        .collect()
});
/// How large a stripe may be before the older half of it is dropped.
static STRIPE_LIMIT: AtomicUsize = AtomicUsize::new(STRIPE_FLOOR);

/// Where [`clock`] starts: far enough above 0 that a key can be older than
/// the server, as `RESTORE ... IDLETIME` makes one (Redis's LRU clock,
/// 24 bits of seconds, holds 194 days).
const CLOCK_BASE: u32 = 1 << 24;

/// Seconds since the first call, plus [`CLOCK_BASE`].
fn clock() -> u32 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    CLOCK_BASE + START.elapsed().as_secs() as u32
}

fn record_id(db: usize, key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(key, db as u64)
}

fn decayed(mut record: Use, now: u32) -> Use {
    let period = lfu_decay_time();
    let minutes = now / 60;
    // A period of 0 is no decay at all.
    let periods = minutes
        .saturating_sub(record.decayed)
        .checked_div(period)
        .unwrap_or(0);
    if periods > 0 {
        record.frequency = record.frequency.saturating_sub(periods.min(255) as u8);
        record.decayed = minutes;
    }
    record
}

/// Whether this use is counted: less and less likely as the count grows, so
/// that 255 is reached after about a million uses with the default factor.
fn counts(frequency: u8) -> bool {
    if frequency == u8::MAX {
        return false;
    }
    let above = frequency.saturating_sub(LFU_INITIAL) as f64;
    let chance = 1.0 / (above * lfu_log_factor() as f64 + 1.0);
    rand::random::<f64>() < chance
}

/// Whether commands have to report the keys they use. One atomic load.
#[inline]
pub fn recording() -> bool {
    RECORDING.load(Ordering::Relaxed)
}

/// `key` was read, or written.
pub fn used(store: &Store, db: usize, key: &[u8], wrote: bool) {
    let now = clock();
    let id = record_id(db, key);
    let mut stripe = RECORDS[id as usize % RECORD_STRIPES].lock();
    let record = stripe.entry(id).or_insert(Use {
        used: now,
        modified: now,
        frequency: LFU_INITIAL,
        decayed: now / 60,
    });
    *record = decayed(*record, now);
    if counts(record.frequency) {
        record.frequency += 1;
    }
    record.used = now;
    if wrote {
        record.modified = now;
    }
    if stripe.len() > STRIPE_LIMIT.load(Ordering::Relaxed) {
        trim(store, &mut stripe);
    }
}

/// Records outlive their keys — a key that is deleted, or expires, leaves
/// its record behind, and a read of a key that is not there makes one. So a
/// stripe that has outgrown what the keyspace accounts for drops the older
/// half of its records. A key without a record counts as older than any key
/// with one, so nothing a policy would have kept is lost by it.
fn trim(store: &Store, stripe: &mut AHashMap<u64, Use>) {
    let keys: usize = store
        .iter()
        .flat_map(|db| db.iter())
        .map(|shard| shard.read().len())
        .sum();
    let limit = STRIPE_FLOOR.max(keys / RECORD_STRIPES * 2);
    STRIPE_LIMIT.store(limit, Ordering::Relaxed);
    if stripe.len() <= limit {
        return;
    }
    let mut ages: Vec<u32> = stripe.values().map(|record| record.used).collect();
    let middle = ages.len() / 2;
    let (_, median, _) = ages.select_nth_unstable(middle);
    let median = *median;
    stripe.retain(|_, record| record.used > median);
}

fn record_of(db: usize, key: &[u8]) -> Option<Use> {
    let id = record_id(db, key);
    RECORDS[id as usize % RECORD_STRIPES]
        .lock()
        .get(&id)
        .map(|record| decayed(*record, clock()))
}

/// Seconds since `key` was last used (`OBJECT IDLETIME`), as far as is known.
pub fn idle_seconds(db: usize, key: &[u8]) -> u64 {
    match record_of(db, key) {
        Some(record) => clock().saturating_sub(record.used) as u64,
        None => (clock() - CLOCK_BASE) as u64,
    }
}

/// `RESTORE ... IDLETIME` / `FREQ`: the restored key keeps the standing it
/// had where it was dumped. As in Redis, the frequency is set under an LFU
/// policy and the idle time under any other.
pub fn restored(store: &Store, db: usize, key: &[u8], idle: Option<u64>, freq: Option<u8>) {
    used(store, db, key, true);
    let id = record_id(db, key);
    let mut stripe = RECORDS[id as usize % RECORD_STRIPES].lock();
    let Some(record) = stripe.get_mut(&id) else {
        return;
    };
    if policy().is_lfu() {
        if let Some(freq) = freq {
            record.frequency = freq;
        }
    } else if let Some(idle) = idle {
        let idle = idle.min(u64::from(CLOCK_BASE)) as u32;
        record.used = clock().saturating_sub(idle);
    }
}

/// The frequency count of `key` (`OBJECT FREQ`).
pub fn frequency(db: usize, key: &[u8]) -> u8 {
    record_of(db, key).map_or(LFU_INITIAL, |record| record.frequency)
}

// ── Making room ───────────────────────────────────────────────────────────────

/// Commands that Redis runs however much memory is in use, because they do
/// not add to it (they lack its `denyoom` flag): deletions, expiry, moves.
/// Refusing `DEL` to a server that is out of memory would leave no way out.
pub fn allowed_when_out_of_memory(name: &str) -> bool {
    matches!(
        name,
        "DEL"
            | "UNLINK"
            | "DELEX"
            | "FLUSHDB"
            | "FLUSHALL"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "GETDEL"
            | "RENAME"
            | "RENAMENX"
            | "MOVE"
            | "SWAPDB"
            | "LPOP"
            | "RPOP"
            | "LMPOP"
            | "BLPOP"
            | "BRPOP"
            | "BLMPOP"
            | "LREM"
            | "LTRIM"
            | "SREM"
            | "SPOP"
            | "HDEL"
            | "HGETDEL"
            | "HEXPIRE"
            | "HPEXPIRE"
            | "HEXPIREAT"
            | "HPEXPIREAT"
            | "HPERSIST"
            | "ZREM"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "ZMPOP"
            | "BZMPOP"
            | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYRANK"
            | "ZREMRANGEBYLEX"
            | "XDEL"
            | "XDELEX"
            | "XTRIM"
            | "XACK"
            | "XACKDEL"
            | "XGROUP"
            | "JSON.DEL"
            | "JSON.FORGET"
            | "JSON.CLEAR"
            | "TS.DEL"
            | "VREM"
            | "FT.DROPINDEX"
            | "FUNCTION"
            | "SCRIPT"
            | "MULTI"
            | "EXEC"
            | "DISCARD"
            | "RESTORE-ASKING"
    )
}

/// How much a policy wants a key gone: the highest goes.
type Unwanted = u64;

/// A shard of either store, to read: the keyspace's and the expiry store's
/// are different types that both lock to a map.
trait Shard<V> {
    type Key: AsRef<str>;
    fn read_map(&self) -> impl std::ops::Deref<Target = AHashMap<Self::Key, V>>;
}

impl<V> Shard<V> for Arc<RwLock<AHashMap<crate::store::Key, V>>> {
    type Key = crate::store::Key;
    fn read_map(&self) -> impl std::ops::Deref<Target = AHashMap<Self::Key, V>> {
        self.read()
    }
}

impl Shard<SystemTime> for crate::store::ExpiryShard {
    type Key = crate::store::Key;
    fn read_map(&self) -> impl std::ops::Deref<Target = AHashMap<Self::Key, SystemTime>> {
        self.read()
    }
}

/// Up to `count` keys of one database, from a random place in it, each with
/// what `take` makes of its value. Nothing of the caller's runs with a shard
/// locked but `take`.
fn sample<V, T>(
    shards: &[impl Shard<V>],
    count: usize,
    rng: &mut impl Rng,
    take: impl Fn(&V) -> T,
) -> Vec<(String, T)> {
    let mut found = Vec::with_capacity(count);
    if shards.is_empty() {
        return found;
    }
    let first = rng.gen_range(0..shards.len());
    for step in 0..shards.len() {
        let map = shards[(first + step) % shards.len()].read_map();
        if map.is_empty() {
            continue;
        }
        let from = rng.gen_range(0..map.len());
        for (key, value) in map.iter().skip(from).take(count - found.len()) {
            found.push((key.as_ref().to_string(), take(value)));
        }
        if found.len() >= count {
            break;
        }
    }
    found
}

/// The key to delete next: the least wanted of a sample from each database
/// in `dbs`.
fn choose(ctx: &Ctx, policy: Policy, dbs: &[usize], rng: &mut impl Rng) -> Option<(usize, String)> {
    let now = clock();
    let count = samples();
    let mut worst: Option<(Unwanted, usize, String)> = None;
    for &db in dbs {
        let candidates: Vec<(String, Option<SystemTime>)> = if policy.volatile() {
            sample(&ctx.expiry_store[db], count, rng, |at| Some(*at))
        } else {
            sample(&ctx.store[db], count, rng, |_| None)
        };
        for (key, expires) in candidates {
            let unwanted: Unwanted = match policy {
                // Every sampled key is as unwanted as the next, and the
                // sample was random already.
                Policy::AllKeysRandom | Policy::VolatileRandom => 0,
                Policy::VolatileTtl => {
                    let at = expires
                        .and_then(|at| at.duration_since(SystemTime::UNIX_EPOCH).ok())
                        .map_or(u64::MAX, |at| at.as_millis() as u64);
                    u64::MAX - at
                }
                Policy::AllKeysLru | Policy::VolatileLru => record_of(db, key.as_bytes())
                    .map_or(u64::MAX, |record| now.saturating_sub(record.used) as u64),
                Policy::AllKeysLrm | Policy::VolatileLrm => record_of(db, key.as_bytes())
                    .map_or(u64::MAX, |record| {
                        now.saturating_sub(record.modified) as u64
                    }),
                Policy::AllKeysLfu | Policy::VolatileLfu => {
                    (u8::MAX - frequency(db, key.as_bytes())) as u64
                }
                Policy::NoEviction => return None,
            };
            if worst.as_ref().is_none_or(|(most, _, _)| unwanted > *most) {
                worst = Some((unwanted, db, key));
            }
        }
    }
    worst.map(|(_, db, key)| (db, key))
}

/// Delete `key` to make room, with everything a deletion entails.
fn evict(ctx: &mut Ctx, db: usize, key: &str, ordered: bool) -> bool {
    // Ordered against other writes to the key, like any write, while
    // anything reads the write stream. See `write_order`.
    let _order = (ordered && !crate::command_gate::alone_on_this_thread())
        .then(|| crate::write_order::lock_keys(db, std::iter::once(key.as_bytes())));

    if !crate::commands::helpers::delete_key(ctx.store, ctx.expiry_store, db, key) {
        // Gone already.
        return false;
    }
    crate::vector::maybe_unindex(ctx.index_store, key);
    crate::vector::maybe_request_compaction(ctx.index_store, ctx.compaction_tx, key);
    crate::stats::record_evicted();
    crate::stats::record_rdb_change();
    crate::keyspace_events::notify(
        ctx.pubsub,
        db,
        crate::keyspace_events::KS_EVICTED,
        "evicted",
        key,
    );
    ctx.watch_registry.notify(db, key);
    crate::commands::handler::invalidate_tracked_key(ctx, key, false);

    ctx.repl_state.note_local_write();
    if crate::replication::write_is_observed(ctx) {
        // In the key's database, whichever the client has selected.
        let selected = *ctx.selected_db;
        *ctx.selected_db = db;
        crate::replication::replicate_one_write(ctx, &[b"DEL".to_vec(), key.as_bytes().to_vec()]);
        *ctx.selected_db = selected;
    }
    true
}

/// Delete keys until memory use is at most `limit`. False if the policy has
/// nothing more to delete and it is still over: the write is refused.
///
/// Called with the write gate held and before the command has locked its
/// own keys' order.
pub fn make_room(ctx: &mut Ctx, limit: usize, ordered: bool) -> bool {
    let policy = policy();
    if crate::memory_use::allocated() <= limit {
        return true;
    }
    if policy == Policy::NoEviction {
        return false;
    }
    // The databases worth sampling. Found once: a database is not emptied
    // and refilled in the time this takes, and if it is, it is found empty.
    let dbs: Vec<usize> = (0..ctx.store.len())
        .filter(|&db| {
            if policy.volatile() {
                ctx.expiry_store[db]
                    .iter()
                    .any(|shard| !shard.read().is_empty())
            } else {
                ctx.store[db].iter().any(|shard| !shard.read().is_empty())
            }
        })
        .collect();
    let mut rng = rand::thread_rng();
    // A key that could not be deleted — it went between being chosen and
    // being deleted — is no reason to stop, but nothing but those is.
    let mut misses = 0;
    while crate::memory_use::allocated() > limit {
        let Some((db, key)) = choose(ctx, policy, &dbs, &mut rng) else {
            return false;
        };
        if evict(ctx, db, &key, ordered) {
            misses = 0;
        } else {
            // A TTL without a key would be chosen for ever by a volatile
            // policy.
            crate::commands::helpers::clear_expiry(ctx.expiry_store, db, &key);
            misses += 1;
            if misses > 64 {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_by_name() {
        for (policy, name) in POLICIES {
            assert_eq!(Policy::parse(name), Some(policy));
            assert_eq!(policy.name(), name);
        }
        assert_eq!(Policy::parse("ALLKEYS-LRU"), Some(Policy::AllKeysLru));
        assert_eq!(Policy::parse("lru"), None);
        assert!(Policy::VolatileTtl.volatile() && !Policy::VolatileTtl.records_use());
        assert!(Policy::AllKeysLrm.records_use() && !Policy::AllKeysLrm.volatile());
    }

    #[test]
    fn frequency_decays_by_the_minute() {
        let record = Use {
            used: 0,
            modified: 0,
            frequency: 12,
            decayed: 0,
        };
        // Ten minutes on, with the default of one count a minute.
        assert_eq!(decayed(record, 600).frequency, 2);
        assert_eq!(decayed(record, 6000).frequency, 0);
        assert_eq!(decayed(record, 59).frequency, 12);
    }

    #[test]
    fn a_sample_is_no_more_than_was_asked_for_and_finds_what_there_is() {
        let shards: Vec<Arc<RwLock<AHashMap<crate::store::Key, u8>>>> = (0..8)
            .map(|_| Arc::new(RwLock::new(AHashMap::new())))
            .collect();
        let mut rng = rand::thread_rng();
        let keys = |found: Vec<(String, u8)>| -> Vec<String> {
            found.into_iter().map(|(key, _)| key).collect()
        };
        assert!(sample(&shards, 5, &mut rng, |v| *v).is_empty());

        // One key, in one shard of eight: it is found wherever the walk starts.
        shards[3].write().insert("only".into(), 1);
        for _ in 0..50 {
            assert_eq!(keys(sample(&shards, 5, &mut rng, |v| *v)), ["only"]);
        }
        for i in 0..100 {
            shards[i % 8].write().insert(format!("k{i}").into(), 1);
        }
        let found = sample(&shards, 5, &mut rng, |v| *v);
        assert!((1..=5).contains(&found.len()), "{found:?}");
    }
}
