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
//! When a key was last used (or modified), or how often, is kept in the
//! key's own entry, in one 32-bit word of its header, as Redis keeps it in
//! the object's `lru` field: see [`Access`]. It is written only while a
//! policy that asks for it is selected (or `key-access-tracking` is on):
//! with `noeviction` a command pays one atomic load for all of this. The
//! word is written only when it changes, so a key read again and again
//! within the same second dirties its cache line once.
//!
//! An eviction is a deletion like any other: replicas and the AOF are sent a
//! `DEL`, a `WATCH` on the key is tripped, clients caching it are told, and
//! `evicted` is published to keyspace subscribers.

use crate::commands::ctx::Ctx;
use crate::store::Store;
use rand::Rng;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;

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

    fn is_lrm(self) -> bool {
        matches!(self, Policy::AllKeysLrm | Policy::VolatileLrm)
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

/// What a use of a key writes to its word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Track {
    /// Nothing.
    Off,
    /// The clock, at every use: the LRU policies, and `key-access-tracking`
    /// under a policy that keeps nothing itself.
    Used,
    /// The clock, at every write only: the LRM policies.
    Modified,
    /// The LFU count.
    Frequency,
}

static POLICY: AtomicU8 = AtomicU8::new(Policy::NoEviction as u8);
static RECORDING: AtomicBool = AtomicBool::new(false);
static TRACK: AtomicU8 = AtomicU8::new(Track::Off as u8);
/// `key-access-tracking`: record use whatever the policy.
static TRACK_ALWAYS: AtomicBool = AtomicBool::new(false);
static SAMPLES: AtomicUsize = AtomicUsize::new(5);
static LFU_LOG_FACTOR: AtomicU32 = AtomicU32::new(10);
static LFU_DECAY_MINUTES: AtomicU32 = AtomicU32::new(1);

pub fn policy() -> Policy {
    POLICIES[POLICY.load(Ordering::Relaxed) as usize].0
}

fn track() -> Track {
    match TRACK.load(Ordering::Relaxed) {
        1 => Track::Used,
        2 => Track::Modified,
        3 => Track::Frequency,
        _ => Track::Off,
    }
}

/// Select `policy`. Nothing is cleared: a word written for the other kind
/// of policy (a clock read as a count, or the other way round) carries the
/// other kind's tag, and counts as nothing known until the key is used
/// again (see [`Access`]). That is what clearing the records did, without
/// a pass over every key; Redis reads the other kind's word as its own,
/// and says that "LRU and LFU data will take some time to adjust".
pub fn set_policy(policy: Policy) {
    POLICY.store(policy as u8, Ordering::Relaxed);
    let recording = policy.records_use() || key_access_tracking();
    let track = if !recording {
        Track::Off
    } else if policy.is_lfu() {
        Track::Frequency
    } else if policy.is_lrm() {
        Track::Modified
    } else {
        Track::Used
    };
    TRACK.store(track as u8, Ordering::Relaxed);
    RECORDING.store(recording, Ordering::Relaxed);
}

/// `key-access-tracking`: whether a key's use is recorded under every
/// policy, so that `OBJECT IDLETIME` and `FREQ` report it under
/// `noeviction` too, as Redis does (#21). Off by default: recording costs
/// every command a second lookup of each of its keys, under its shard's read
/// lock (it costs a key nothing: the word is in every entry anyway). Under
/// an LRU or LFU policy it is recorded either way.
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

/// Where [`clock`] starts: far enough above 0 that a key can be older than
/// the server, as `RESTORE ... IDLETIME` makes one (Redis's LRU clock,
/// 24 bits of seconds, holds 194 days).
const CLOCK_BASE: u32 = 1 << 24;

/// A key's access word: the 32 bits its entry's header keeps for this
/// (`store::entry`), as Redis's `robj.lru`. The top two bits say what the
/// rest is:
///
/// ```text
/// 00  nothing recorded (a key made while nothing was, or never used)
/// 01  [`clock`] seconds when last used (LRU), or written (LRM): 30 bits
/// 10  LFU: minutes when last used, 16 bits (wrapping) << 8 | count, 8 bits
/// ```
///
/// The LFU half is Redis's own: a logarithmic count that grows less likely
/// to rise the higher it is (`lfu-log-factor`), and falls by one for every
/// `lfu-decay-time` minutes the key goes unused. A word of the other kind,
/// or none, is read as nothing known: the oldest idle time (the server's
/// age), and a count of [`LFU_INITIAL`].
pub struct Access;

impl Access {
    const TAG: u32 = 0b11 << 30;
    const CLOCK: u32 = 0b01 << 30;
    const LFU: u32 = 0b10 << 30;
    const PAYLOAD: u32 = !Self::TAG;

    /// The clock in the word, if it holds one.
    #[inline]
    fn clock_of(word: u32) -> Option<u32> {
        (word & Self::TAG == Self::CLOCK).then_some(word & Self::PAYLOAD)
    }

    #[inline]
    fn clock_word(seconds: u32) -> u32 {
        Self::CLOCK | (seconds & Self::PAYLOAD)
    }

    #[inline]
    fn lfu_word(minutes: u32, count: u8) -> u32 {
        Self::LFU | (minutes & 0xffff) << 8 | count as u32
    }

    /// The count in an LFU word, decayed to `minutes` (not written back:
    /// as Redis's `LFUDecrAndReturn`). [`LFU_INITIAL`] for any other word.
    fn frequency(word: u32, minutes: u32, decay: u32) -> u8 {
        if word & Self::TAG != Self::LFU {
            return LFU_INITIAL;
        }
        let count = (word & 0xff) as u8;
        let then = (word >> 8) & 0xffff;
        let now = minutes & 0xffff;
        // The minutes wrap at 2^16 (45 days), as Redis's do.
        let elapsed = if now >= then {
            now - then
        } else {
            0xffff - then + now
        };
        // A period of 0 is no decay at all.
        let periods = elapsed.checked_div(decay).unwrap_or(0);
        count.saturating_sub(periods.min(255) as u8)
    }

    /// The word after a use, at `seconds` on [`clock`]; `roll` is a random
    /// number in [0, 1) for the LFU count's chance to rise.
    ///
    /// A word of 0 is a new entry, and the write that made the key is not a
    /// use of it: the key starts at [`LFU_INITIAL`], as Redis's new objects
    /// do. Counting it would put every new key one above that, level with a
    /// key read once, and a stream of new keys would push out the keys that
    /// are read (hot keys kept 34% against Redis's 96% in a test of that).
    fn after_use(word: u32, track: Track, seconds: u32, roll: impl FnOnce() -> f64) -> u32 {
        match track {
            Track::Off => word,
            Track::Used | Track::Modified => Self::clock_word(seconds),
            Track::Frequency if word == 0 => Self::lfu_word(seconds / 60, LFU_INITIAL),
            Track::Frequency => {
                let minutes = seconds / 60;
                let mut count = Self::frequency(word, minutes, lfu_decay_time());
                if counts(count, roll) {
                    count += 1;
                }
                Self::lfu_word(minutes, count)
            }
        }
    }
}

/// Seconds since the first call, plus [`CLOCK_BASE`].
fn clock() -> u32 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    CLOCK_BASE + START.elapsed().as_secs() as u32
}

/// Whether this use is counted: less and less likely as the count grows, so
/// that 255 is reached after about a million uses with the default factor
/// (Redis's `LFULogIncr`). `roll` is drawn only below 255.
fn counts(frequency: u8, roll: impl FnOnce() -> f64) -> bool {
    if frequency == u8::MAX {
        return false;
    }
    let above = frequency.saturating_sub(LFU_INITIAL) as f64;
    let chance = 1.0 / (above * lfu_log_factor() as f64 + 1.0);
    roll() < chance
}

/// Whether commands have to report the keys they use. One atomic load.
#[inline]
pub fn recording() -> bool {
    RECORDING.load(Ordering::Relaxed)
}

/// `key` was read, or written: its word is brought up to date, in place,
/// with its shard locked for reading (see `ShardMap::touch`). A key that is
/// not there is left alone. The key is found by its bytes, without asking
/// whether they are UTF-8 (that check was a quarter of this).
pub fn used(store: &Store, db: usize, key: &[u8], wrote: bool) {
    let track = track();
    if track == Track::Off || (track == Track::Modified && !wrote) {
        return;
    }
    let seconds = clock();
    crate::store::get_shard_of_bytes(&store[db], key)
        .read()
        .touch(key, |word| {
            Access::after_use(word, track, seconds, rand::random::<f64>)
        });
}

/// Seconds since a key with the access word `word` was last used (`OBJECT
/// IDLETIME`), as far as is known: since the server started if nothing is.
pub fn idle_seconds(word: u32) -> u64 {
    let now = clock();
    match Access::clock_of(word) {
        Some(then) => now.saturating_sub(then) as u64,
        None => (now - CLOCK_BASE) as u64,
    }
}

/// The frequency count of a key with the access word `word` (`OBJECT FREQ`).
pub fn frequency(word: u32) -> u8 {
    Access::frequency(word, clock() / 60, lfu_decay_time())
}

/// `RESTORE ... IDLETIME` / `FREQ`: the restored key keeps the standing it
/// had where it was dumped. As in Redis, the frequency is set under an LFU
/// policy and the idle time under any other; without either, the restore
/// is a use like any write.
pub fn restored(store: &Store, db: usize, key: &[u8], idle: Option<u64>, freq: Option<u8>) {
    let track = track();
    if track == Track::Off {
        return;
    }
    let seconds = clock();
    crate::store::get_shard_of_bytes(&store[db], key)
        .read()
        .touch(key, |word| match (track, idle, freq) {
            (Track::Frequency, _, Some(freq)) => Access::lfu_word(seconds / 60, freq),
            (Track::Used | Track::Modified, Some(idle), _) => {
                let idle = idle.min(u64::from(CLOCK_BASE)) as u32;
                Access::clock_word(seconds.saturating_sub(idle))
            }
            _ => Access::after_use(word, track, seconds, rand::random::<f64>),
        });
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

/// A key sampled for eviction, with its expiry time when it was sampled
/// from the keys with one (milliseconds since the Unix epoch), and its
/// access word ([`Access`]) as it was then.
type Sampled = (String, Option<u64>, u32);

/// Up to `count` keys from about `at` in `shard`, into `found`: of all its
/// keys, or (`volatile`) of those with a time to live.
fn sample_into(
    shard: &crate::store::Shard,
    volatile: bool,
    at: usize,
    count: usize,
    found: &mut Vec<Sampled>,
) {
    if !volatile {
        // From a random bucket: as many buckets as keys wanted, over how
        // full the table is, where walking to a random key walked past half
        // the shard on average.
        let map = shard.read();
        found.extend(
            map.sample_from(at, count)
                .map(|e| (e.key().to_string(), None, e.access())),
        );
        return;
    }
    if shard.holds_no_ttl() {
        return;
    }
    let map = shard.read();
    let keys = map.ttl_len();
    if keys == 0 {
        return;
    }
    // Places in the shard's list of keys with a time to live, each found in
    // one step: all of them if there are no more than wanted, else `count`
    // spread from `at` by a stride with no factor in common with a power of
    // two. Walking to a random one walked past half the shard's times on
    // average.
    const STRIDE: usize = 0x9e37_79b9_7f4a_7c15_u64 as usize;
    let take = count.min(keys);
    found.extend((0..take).filter_map(|i| {
        let place = if take == keys {
            i
        } else {
            at.wrapping_add(i.wrapping_mul(STRIDE)) % keys
        };
        map.ttl_entry(place)
            .map(|e| (e.key().to_string(), e.expires(), e.access()))
    }));
}

/// Up to `count` keys of one database, from a random place in it: of all
/// its keys, or (`volatile`) of those with a time to live.
fn sample(
    shards: &[crate::store::Shard],
    volatile: bool,
    count: usize,
    rng: &mut impl Rng,
) -> Vec<Sampled> {
    let mut found = Vec::with_capacity(count);
    if shards.is_empty() {
        return found;
    }
    let first = rng.gen_range(0..shards.len());
    for step in 0..shards.len() {
        let at = rng.gen_range(0..usize::MAX);
        sample_into(
            &shards[(first + step) % shards.len()],
            volatile,
            at,
            count - found.len(),
            &mut found,
        );
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
    let decay = lfu_decay_time();
    let count = samples();
    let mut worst: Option<(Unwanted, usize, String)> = None;
    for &db in dbs {
        let candidates: Vec<Sampled> = sample(&ctx.store[db], policy.volatile(), count, rng);
        for (key, expires, word) in candidates {
            let unwanted: Unwanted = match policy {
                // Every sampled key is as unwanted as the next, and the
                // sample was random already.
                Policy::AllKeysRandom | Policy::VolatileRandom => 0,
                Policy::VolatileTtl => u64::MAX - expires.unwrap_or(u64::MAX),
                // A key whose use is not known goes first, as one older
                // than any whose use is.
                Policy::AllKeysLru
                | Policy::VolatileLru
                | Policy::AllKeysLrm
                | Policy::VolatileLrm => {
                    Access::clock_of(word).map_or(u64::MAX, |then| now.saturating_sub(then) as u64)
                }
                Policy::AllKeysLfu | Policy::VolatileLfu => {
                    (u8::MAX - Access::frequency(word, now / 60, decay)) as u64
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

    if !crate::commands::helpers::delete_key(ctx.store, db, key) {
        // Gone already.
        return false;
    }
    crate::vector::maybe_unindex(ctx.index_store, db, key);
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
    // Without a lock: this runs on every write while memory is full, and
    // locking every shard of every empty database (16 x 256 on 32 cores)
    // cost more than the write and the eviction together.
    let dbs: Vec<usize> = (0..ctx.store.len())
        .filter(|&db| {
            if policy.volatile() {
                ctx.store[db].iter().any(|shard| !shard.holds_no_ttl())
            } else {
                ctx.store[db].iter().any(|shard| !shard.holds_no_keys())
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
    use std::sync::Arc;

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
        let word = Access::lfu_word(0, 12);
        // Ten minutes on, with one count a minute (the default).
        assert_eq!(Access::frequency(word, 10, 1), 2);
        assert_eq!(Access::frequency(word, 100, 1), 0);
        assert_eq!(Access::frequency(word, 0, 1), 12);
        // Every two minutes; and never.
        assert_eq!(Access::frequency(word, 10, 2), 7);
        assert_eq!(Access::frequency(word, 10_000, 0), 12);
        // The minutes wrap at 2^16, as Redis's do.
        let late = Access::lfu_word(0xfffe, 12);
        assert_eq!(Access::frequency(late, 0x1_0003, 1), 8);
    }

    /// A clock word and a count are told apart, and neither is read as the
    /// other: after a switch from LRU to LFU, or back, a key's word means
    /// nothing until the key is used again.
    #[test]
    fn a_word_says_what_it_holds() {
        let clock = Access::clock_word(CLOCK_BASE + 100);
        assert_eq!(Access::clock_of(clock), Some(CLOCK_BASE + 100));
        assert_eq!(Access::frequency(clock, 1, 1), LFU_INITIAL);
        let lfu = Access::lfu_word(1, 200);
        assert_eq!(Access::clock_of(lfu), None);
        assert_eq!(Access::frequency(lfu, 1, 1), 200);
        assert_eq!(Access::clock_of(0), None);
        assert_eq!(Access::frequency(0, 1, 1), LFU_INITIAL);
    }

    #[test]
    fn a_use_writes_what_the_policy_keeps() {
        let now = CLOCK_BASE + 7_200;
        let never = || -> f64 { panic!("no roll wanted") };
        assert_eq!(Access::after_use(42, Track::Off, now, never), 42);
        for track in [Track::Used, Track::Modified] {
            let word = Access::after_use(0, track, now, never);
            assert_eq!(Access::clock_of(word), Some(now));
            // Used again in the same second: the very same word, which is
            // what lets a read leave the line alone.
            assert_eq!(Access::after_use(word, track, now, never), word);
        }
        // The write that makes a key is no use of it: a new entry (a word
        // of 0) starts at the initial count, as Redis's new keys do.
        let new = Access::after_use(0, Track::Frequency, now, never);
        assert_eq!(Access::frequency(new, now / 60, 1), LFU_INITIAL);
        // Its first use always counts (the chance is 1 at the initial
        // count); so does a key's first use after a switch from LRU.
        let first = Access::after_use(new, Track::Frequency, now, || 0.99);
        assert_eq!(Access::frequency(first, now / 60, 1), LFU_INITIAL + 1);
        let clock = Access::clock_word(now);
        let first = Access::after_use(clock, Track::Frequency, now, || 0.99);
        assert_eq!(Access::frequency(first, now / 60, 1), LFU_INITIAL + 1);
        // Higher up, a use counts only on a lucky roll.
        let high = Access::lfu_word(now / 60, 100);
        assert_eq!(Access::after_use(high, Track::Frequency, now, || 0.5), high);
        assert_eq!(
            Access::frequency(
                Access::after_use(high, Track::Frequency, now, || 0.0),
                now / 60,
                1
            ),
            101
        );
        // 255 is the ceiling, and no roll is drawn for it.
        let top = Access::lfu_word(now / 60, 255);
        assert_eq!(Access::after_use(top, Track::Frequency, now, never), top);
    }

    #[test]
    fn a_sample_is_no_more_than_was_asked_for_and_finds_what_there_is() {
        let shards: Vec<crate::store::Shard> = (0..8)
            .map(|_| Arc::new(crate::store::ShardLock::default()))
            .collect();
        let one = || crate::types::KiviDBValue::String(crate::types::SharedBytes::from("1"));
        let mut rng = rand::thread_rng();
        let keys = |found: Vec<Sampled>| -> Vec<String> {
            found.into_iter().map(|(key, _, _)| key).collect()
        };
        assert!(sample(&shards, false, 5, &mut rng).is_empty());
        assert!(sample(&shards, true, 5, &mut rng).is_empty());

        // One key, in one shard of eight: it is found wherever the walk starts.
        shards[3].write().insert("only", one());
        for _ in 0..50 {
            assert_eq!(keys(sample(&shards, false, 5, &mut rng)), ["only"]);
        }
        // Without a time to live it is no volatile policy's.
        assert!(sample(&shards, true, 5, &mut rng).is_empty());
        shards[3].write().set_expires("only", 1_234);
        for _ in 0..50 {
            assert_eq!(
                sample(&shards, true, 5, &mut rng),
                [("only".to_string(), Some(1_234), 0)]
            );
        }
        for i in 0..100 {
            shards[i % 8].write().insert(format!("k{i}"), one());
        }
        let found = sample(&shards, false, 5, &mut rng);
        assert_eq!(found.len(), 5, "{found:?}");
    }

    /// The volatile sample is from the keys with a time to live alone, as
    /// many as asked for, and spread over them rather than a run of them.
    #[test]
    fn a_volatile_sample_is_of_keys_with_a_time_to_live() {
        let shards: Vec<crate::store::Shard> = (0..2)
            .map(|_| Arc::new(crate::store::ShardLock::default()))
            .collect();
        for i in 0..1_000u64 {
            let mut map = shards[(i % 2) as usize].write();
            map.insert_str(format!("k{i}"), b"v");
            if i % 4 == 0 {
                map.set_expires(&format!("k{i}"), 10_000 + i);
            }
        }
        let mut rng = rand::thread_rng();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let found = sample(&shards, true, 5, &mut rng);
            assert_eq!(found.len(), 5);
            for (key, at, _) in found {
                let i: u64 = key[1..].parse().unwrap();
                assert_eq!(i % 4, 0, "{key} has no time to live");
                assert_eq!(at, Some(10_000 + i));
                seen.insert(i);
            }
        }
        // 1,000 picks of 250 keys: nearly all of them seen.
        assert!(seen.len() > 200, "{} of 250", seen.len());
    }
}
