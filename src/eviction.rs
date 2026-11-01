//! What happens when `maxmemory` is reached: Redis's `maxmemory-policy`.
//!
//! With `noeviction` — the default — a write that would use more memory is
//! refused. With any other policy the server deletes keys to make room for
//! it, choosing much as Redis does: it looks at a few keys picked at random
//! (`maxmemory-samples`), and again at the few it liked least the last
//! time, and deletes the one the policy likes least, until it is under the
//! limit or there is nothing left that the policy lets it delete. Every key
//! is as likely to be picked as any other (see `sample_shard`), and the keys
//! looked at again are read again, as they are now (see `KEPT`).
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
//! with `noeviction` a command pays one atomic load for all of this, and
//! each of its lookups one thread-local load. The word is written only when
//! it changes, so a key read again and again within the same second dirties
//! its cache line once.
//!
//! **Where a use is written.** By the lookups the command makes anyway: the
//! dispatcher opens a `store::AccessScope` around the command with the
//! [`stamp`] a use writes, and the lookups that read or write a key write
//! it to the entry they find (see `store::access`). A command that only
//! asks about keys (`OBJECT`, `TYPE`, `TTL`, `EXISTS`, ...) opens one that
//! writes nothing. Two kinds of use are still written after the command, by
//! a second lookup of each key it names ([`used`]): a modification under
//! the LRM policies, which only the command's outcome tells; and the keys a
//! script declares, which its calls need not read.
//!
//! An eviction is a deletion like any other: replicas and the AOF are sent a
//! `DEL`, a `WATCH` on the key is tripped, clients caching it are told, and
//! `evicted` is published to keyspace subscribers.

use crate::commands::ctx::Ctx;
use crate::store::Store;
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
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
/// every command a clock reading, and each lookup a store to the key's
/// entry when its word changes (it costs a key nothing: the word is in every
/// entry anyway). Under an LRU or LFU policy it is recorded either way.
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

static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Seconds since the first call, plus [`CLOCK_BASE`].
fn clock() -> u32 {
    clock_at(Instant::now())
}

/// [`clock`] at `now`.
#[inline]
fn clock_at(now: Instant) -> u32 {
    CLOCK_BASE + now.saturating_duration_since(*START).as_secs() as u32
}

/// What a command that uses keys has its lookups write to them, starting
/// at `now` (`store::AccessScope`): the clock (LRU), or the minute with the
/// stamp's top bit set (LFU, see [`counted`]). 0, nothing, when nothing is
/// recorded, or only modifications are (LRM: see the module).
#[inline]
pub fn stamp(now: Instant) -> u32 {
    match track() {
        Track::Used => Access::clock_word(clock_at(now)),
        Track::Frequency => Access::LFU | ((clock_at(now) / 60) & 0xffff) << 8,
        Track::Off | Track::Modified => 0,
    }
}

/// [`stamp`] for a command that only writes, and whose writes all go
/// through: a modification is a use under the LRM policies too.
#[inline]
pub fn write_stamp(now: Instant) -> u32 {
    match track() {
        Track::Modified => Access::clock_word(clock_at(now)),
        _ => stamp(now),
    }
}

/// The access word `word` after one more use, under the LFU stamp `stamp`
/// (see [`stamp`]).
pub(crate) fn counted(word: u32, stamp: u32) -> u32 {
    let minutes = (stamp >> 8) & 0xffff;
    Access::after_use(word, Track::Frequency, minutes * 60, rand::random::<f64>)
}

/// Whether a command's keys have their use written after it, by [`used`],
/// rather than by its lookups: under the LRM policies, and for a script
/// (`script`).
#[inline]
pub fn used_after(script: bool) -> bool {
    match track() {
        Track::Off => false,
        Track::Modified => true,
        Track::Used | Track::Frequency => script,
    }
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

/// `key` was read, or written, by a command whose use is written after it
/// ([`used_after`]): its word is brought up to date, in place, with its
/// shard locked for reading (see `ShardMap::touch`). A key that is not
/// there is left alone. The key is found by its bytes, without asking
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

/// How much `policy` wants a key gone whose access word is `word` and whose
/// time to live ends at `expires`; `now` is [`clock`].
#[inline]
fn unwanted(policy: Policy, word: u32, expires: Option<u64>, now: u32, decay: u32) -> Unwanted {
    match policy {
        // Every sampled key is as unwanted as the next, and the sample was
        // random already.
        Policy::AllKeysRandom | Policy::VolatileRandom | Policy::NoEviction => 0,
        Policy::VolatileTtl => u64::MAX - expires.unwrap_or(u64::MAX),
        // A key whose use is not known goes first, as one older than any
        // whose use is.
        Policy::AllKeysLru | Policy::VolatileLru | Policy::AllKeysLrm | Policy::VolatileLrm => {
            Access::clock_of(word).map_or(u64::MAX, |then| now.saturating_sub(then) as u64)
        }
        Policy::AllKeysLfu | Policy::VolatileLfu => {
            (u8::MAX - Access::frequency(word, now / 60, decay)) as u64
        }
    }
}

/// How many random buckets are tried for each key wanted before the next
/// full bucket from a random one is settled for. A table is kept at least
/// 4/9 full (`store::shrink_if_sparse`), but may be sparser until it is
/// shrunk.
const TRIES: usize = 32;

/// Up to `count` keys of `map`, each handed to `found` with its place (a
/// bucket of the table, or of the list of keys with a time to live): of all
/// its keys, or (`volatile`) of those with a time to live. How many it found.
///
/// Each is at a place picked at random, a bucket picked again until it is
/// full, so that every key is as likely to be looked at as any other. The
/// keys of a run of buckets from a random one, as this used to take them,
/// are not: the first key after a run of empty buckets comes first far more
/// often than one after a full bucket, and the same few places are looked
/// at again and again. The policy takes what it does not want from them, a
/// key it wants comes in from the keys after, and in time all a sample of
/// them holds is keys the policy wants, of which one goes. Under
/// `allkeys-lfu`, with a set of keys read again and again and a stream of
/// new keys written once (a test in which Redis 8.10 keeps 96% of the read
/// keys), runs of buckets kept 76% of them, and of the read keys that went,
/// four in five had been read more often than every new key still there.
fn sample_shard<'m>(
    map: &'m crate::store::ShardMap,
    volatile: bool,
    count: usize,
    rng: &mut impl Rng,
    mut found: impl FnMut(usize, crate::store::EntryView<'m>),
) -> usize {
    if volatile {
        // A list without gaps: any place holds a key.
        let keys = map.ttl_len();
        let (count, every) = if keys <= count {
            (keys, true)
        } else {
            (count, false)
        };
        for i in 0..count {
            let place = if every { i } else { rng.gen_range(0..keys) };
            if let Some(e) = map.ttl_entry(place) {
                found(place, e);
            }
        }
        return count;
    }
    let keys = map.len();
    let buckets = map.buckets();
    if keys <= count {
        // All of them.
        let mut n = 0;
        for place in 0..buckets {
            if n == keys {
                break;
            }
            if let Some(e) = map.entry_at(place) {
                found(place, e);
                n += 1;
            }
        }
        return n;
    }
    // A table has a power of two of buckets.
    let mask = buckets.next_power_of_two() - 1;
    'keys: for _ in 0..count {
        for _ in 0..TRIES {
            let place = rng.r#gen::<usize>() & mask;
            if place >= buckets {
                continue;
            }
            if let Some(e) = map.entry_at(place) {
                found(place, e);
                continue 'keys;
            }
        }
        // Very sparse: the next full bucket from a random one.
        let start = rng.gen_range(0..buckets);
        for step in 0..buckets {
            let place = (start + step) % buckets;
            if let Some(e) = map.entry_at(place) {
                found(place, e);
                continue 'keys;
            }
        }
    }
    count
}

/// Up to `count` keys of one database, from a random shard and as many after
/// it as it takes, each handed to `found` with its shard and place: of all
/// its keys, or (`volatile`) of those with a time to live. Shards with none,
/// as far as is known without a lock, are passed over unlocked.
fn sample_db(
    shards: &[crate::store::Shard],
    volatile: bool,
    count: usize,
    rng: &mut impl Rng,
    mut found: impl FnMut(usize, usize, crate::store::EntryView<'_>),
) {
    if shards.is_empty() {
        return;
    }
    let first = rng.gen_range(0..shards.len());
    let mut got = 0;
    for step in 0..shards.len() {
        let index = (first + step) % shards.len();
        let shard = &shards[index];
        if (volatile && shard.holds_no_ttl()) || (!volatile && shard.holds_no_keys()) {
            continue;
        }
        let map = shard.read();
        got += sample_shard(&map, volatile, count - got, rng, |place, e| {
            found(index, place, e)
        });
        if got >= count {
            break;
        }
    }
}

/// How many of the keys looked at for one eviction, after the one that goes,
/// a thread keeps for its next: those the policy wanted least.
///
/// A sample is a handful of keys, and the one of them the policy wants
/// least is often a key it wants: when every key sampled is. Looking at more
/// keys makes that rarer, and the keys looked at last time are the cheapest
/// to look at again: their lines were read a moment ago. So each choice is
/// among the keys of a new sample and those kept from the last, each kept
/// key read again where it was found — how long since it was used, or how
/// often, as it is now, not as it was: a kept key that has been used since
/// is only as unwanted as it is now. (Redis keeps a pool of sixteen keys
/// with what they were worth when sampled, and a key used since it was put
/// there still goes first.) With four, the test above keeps as many of
/// its read keys as Redis does.
const KEPT: usize = 4;

/// Where a key looked at for an eviction was found: its database, shard and
/// place (see [`sample_shard`]), which entry it was ([`EntryView::id`]),
/// and how unwanted it was.
///
/// [`EntryView::id`]: crate::store::EntryView::id
#[derive(Clone, Copy, Default)]
struct Spot {
    db: usize,
    shard: usize,
    place: usize,
    id: usize,
    unwanted: Unwanted,
}

/// The keys a thread keeps from one eviction to its next ([`KEPT`]).
#[derive(Clone, Copy, Default)]
struct Kept {
    len: usize,
    spots: [Spot; KEPT],
}

thread_local! {
    static KEPT_SPOTS: std::cell::Cell<Kept> = std::cell::Cell::new(Kept::default());
}

/// The most unwanted keys of a choice, most unwanted first, each entry once:
/// the one to go, and the [`KEPT`] after it. The key of the first is a
/// copy, made when it came first (the shard is not held after).
struct Choice {
    top: [Spot; KEPT + 1],
    len: usize,
    key: Option<crate::store::Key>,
}

impl Choice {
    fn new() -> Self {
        Choice {
            top: [Spot::default(); KEPT + 1],
            len: 0,
            key: None,
        }
    }

    /// `spot`, whose key is `key`. Among keys as unwanted, the one offered
    /// first stays first.
    #[inline]
    fn offer(&mut self, spot: Spot, key: &str) {
        let top = &self.top[..self.len];
        if top.iter().any(|s| s.id == spot.id) {
            // Sampled twice, or kept and sampled again.
            return;
        }
        let at = top
            .iter()
            .position(|s| spot.unwanted > s.unwanted)
            .unwrap_or(self.len);
        if at > KEPT {
            return;
        }
        if at == 0 {
            self.key = Some(crate::store::Key::from(key));
        }
        // Those after it move down one, the last falling off a full list.
        let len = (self.len + 1).min(KEPT + 1);
        self.top.copy_within(at..len - 1, at + 1);
        self.top[at] = spot;
        self.len = len;
    }

    /// The key to delete, and the database it is in; the keys after it are
    /// kept for the thread's next choice.
    fn decide(self) -> Option<(usize, crate::store::Key)> {
        let mut kept = Kept::default();
        if self.len > 1 {
            kept.len = self.len - 1;
            kept.spots[..kept.len].copy_from_slice(&self.top[1..self.len]);
        }
        KEPT_SPOTS.with(|k| k.set(kept));
        let db = self.top[0].db;
        self.key.map(|key| (db, key))
    }
}

/// The key to delete next: the least wanted of a sample from each database
/// in `dbs`, and of the keys kept from this thread's last choice.
fn choose(
    ctx: &Ctx,
    policy: Policy,
    dbs: &[usize],
    rng: &mut impl Rng,
) -> Option<(usize, crate::store::Key)> {
    let now = clock();
    let decay = lfu_decay_time();
    let volatile = policy.volatile();
    let random = matches!(policy, Policy::AllKeysRandom | Policy::VolatileRandom);
    // Under a random policy any key will do: one is enough, and nothing is
    // worth keeping.
    let count = if random { 1 } else { samples() };
    let mut choice = Choice::new();
    if !random {
        let kept = KEPT_SPOTS.with(|k| k.take());
        for spot in &kept.spots[..kept.len] {
            // Where it was, if it is still there (the same entry: a key
            // deleted and written again is a new entry, and a new key). A
            // place in a database that has gone from the sample is passed
            // over, as are those of another store's.
            if !dbs.contains(&spot.db) {
                continue;
            }
            let Some(shard) = ctx.store.get(spot.db).and_then(|db| db.get(spot.shard)) else {
                continue;
            };
            let map = shard.read();
            let entry = if volatile {
                map.ttl_entry(spot.place)
            } else {
                map.entry_at(spot.place)
            };
            if let Some(e) = entry.filter(|e| e.id() == spot.id) {
                let unwanted = unwanted(policy, e.access(), e.expires(), now, decay);
                choice.offer(Spot { unwanted, ..*spot }, e.key());
            }
        }
    }
    for &db in dbs {
        sample_db(&ctx.store[db], volatile, count, rng, |shard, place, e| {
            let unwanted = unwanted(policy, e.access(), e.expires(), now, decay);
            let spot = Spot {
                db,
                shard,
                place,
                id: e.id(),
                unwanted,
            };
            choice.offer(spot, e.key());
        });
    }
    choice.decide()
}

/// The databases worth sampling, as a thread last found them: those with
/// keys (or, under a volatile policy, keys with a time to live).
///
/// Found without a lock, by asking each shard; but asked of every shard of
/// every database on every evicting write, that was a load from each of
/// 16 x 128 lines of memory (the shards are apart), nearly all of them the
/// empty databases'. A thread asks again once its answer is
/// [`Dbs::TRUSTED`] old, or when none of the databases it knows of has a
/// key to give.
#[derive(Default)]
struct Dbs {
    store: usize,
    volatile: bool,
    found: Option<Instant>,
    list: Vec<usize>,
}

impl Dbs {
    const TRUSTED: std::time::Duration = std::time::Duration::from_millis(10);

    /// The list, found again if it is not to be trusted (`fresh`: it is not).
    fn get(&mut self, ctx: &Ctx, volatile: bool, fresh: bool) -> &[usize] {
        let store = Arc::as_ptr(ctx.store) as *const () as usize;
        let now = Instant::now();
        if fresh
            || self.store != store
            || self.volatile != volatile
            || self
                .found
                .is_none_or(|found| now.saturating_duration_since(found) >= Self::TRUSTED)
        {
            self.store = store;
            self.volatile = volatile;
            self.found = Some(now);
            self.list.clear();
            self.list.extend((0..ctx.store.len()).filter(|&db| {
                ctx.store[db].iter().any(|shard| {
                    if volatile {
                        !shard.holds_no_ttl()
                    } else {
                        !shard.holds_no_keys()
                    }
                })
            }));
        }
        &self.list
    }
}

thread_local! {
    static DBS: std::cell::RefCell<Dbs> = std::cell::RefCell::new(Dbs::default());
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
    // Taken out for the while, and put back: nothing here makes room again.
    let mut dbs = DBS.with(|d| std::mem::take(&mut *d.borrow_mut()));
    let done = make_room_in(ctx, policy, limit, ordered, &mut dbs);
    DBS.with(|d| *d.borrow_mut() = dbs);
    done
}

fn make_room_in(ctx: &mut Ctx, policy: Policy, limit: usize, ordered: bool, dbs: &mut Dbs) -> bool {
    let volatile = policy.volatile();
    let mut fresh = false;
    let mut rng = rand::thread_rng();
    // A key that could not be deleted — it went between being chosen and
    // being deleted — is no reason to stop, but nothing but those is.
    let mut misses = 0;
    while crate::memory_use::allocated() > limit {
        let Some((db, key)) = choose(ctx, policy, dbs.get(ctx, volatile, fresh), &mut rng) else {
            if fresh {
                return false;
            }
            // Nothing where there were keys: ask again where they are.
            fresh = true;
            continue;
        };
        fresh = false;
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

    /// A key sampled, with its expiry time when it was sampled from the keys
    /// with one, and its access word.
    type Sampled = (String, Option<u64>, u32);

    fn sample(
        shards: &[crate::store::Shard],
        volatile: bool,
        count: usize,
        rng: &mut impl Rng,
    ) -> Vec<Sampled> {
        let mut found = Vec::new();
        sample_db(shards, volatile, count, rng, |_, _, e| {
            let expires = if volatile { e.expires() } else { None };
            found.push((e.key().to_string(), expires, e.access()))
        });
        found
    }

    /// Every key is as likely to be sampled as any other, wherever it is in
    /// its table: a key after a run of empty buckets no more than one after
    /// a full bucket. (Taken as a run of buckets from a random one, the key
    /// after the longest run of empty ones came up many times as often as
    /// the average key, and the keys in a run of full ones hardly at all.)
    #[test]
    fn every_key_is_as_likely_to_be_sampled() {
        let shard = crate::store::ShardLock::default();
        {
            let mut map = shard.write();
            for i in 0..3_000 {
                map.insert_str(format!("k{i}"), b"v");
            }
            // Gaps here and there: a third of the keys stay.
            for i in (0..3_000).filter(|i| i % 3 != 0) {
                map.remove(&format!("k{i}"));
            }
        }
        let map = shard.read();
        assert_eq!(map.len(), 1_000);
        let mut times = std::collections::HashMap::<String, u32>::new();
        let mut rng = rand::thread_rng();
        // 200 times each, on average.
        for _ in 0..40_000 {
            let n = sample_shard(&map, false, 5, &mut rng, |_, e| {
                *times.entry(e.key().to_string()).or_default() += 1;
            });
            assert_eq!(n, 5);
        }
        assert_eq!(times.len(), 1_000);
        let least = *times.values().min().unwrap();
        let most = *times.values().max().unwrap();
        // Seven standard deviations either side (about 14 each).
        assert!(
            least >= 100 && most <= 300,
            "keys sampled from {least} to {most} times"
        );
    }

    /// Of the keys looked at for an eviction, the most unwanted goes, and
    /// the next most unwanted are kept for the next choice, each entry once;
    /// among keys as unwanted, the one looked at first.
    #[test]
    fn the_most_unwanted_goes_and_the_next_are_kept() {
        let spot = |id: usize, unwanted: Unwanted| Spot {
            db: 3,
            shard: 1,
            place: id * 10,
            id,
            unwanted,
        };
        let mut c = Choice::new();
        for (id, unwanted) in [
            (1, 10),
            (2, 30),
            (3, 20),
            (2, 30),
            (4, 5),
            (5, 25),
            (6, 1),
            (7, 40),
            (8, 20),
        ] {
            c.offer(spot(id, unwanted), &format!("k{id}"));
        }
        let ids = |s: &[Spot]| s.iter().map(|s| s.id).collect::<Vec<_>>();
        // 2 is there once; 8 is as unwanted as 3, and came after it.
        assert_eq!(ids(&c.top[..c.len]), [7, 2, 5, 3, 8]);
        assert_eq!(
            c.decide().map(|(db, key)| (db, key.to_string())),
            Some((3, "k7".to_string()))
        );
        let kept = KEPT_SPOTS.with(|k| k.get());
        assert_eq!(ids(&kept.spots[..kept.len]), [2, 5, 3, 8]);
        assert_eq!(kept.spots[0].place, 20);
        // Nothing looked at: nothing goes, and nothing is kept.
        assert!(Choice::new().decide().is_none());
        assert_eq!(KEPT_SPOTS.with(|k| k.get()).len, 0);
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
