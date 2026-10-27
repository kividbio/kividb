use crate::store::Key;
use crate::{
    store::{self, ExpiryShard, ExpiryStore, Store},
    types::KiviDBValue,
    vector::IndexStore,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Redis `when` / `SETEX` style upper bound: absolute deadline in milliseconds as `long long`.
pub const REDIS_WHEN_MAX_MS: i128 = 9223372036854775807; // i64::MAX

pub const ERR_VALUE_NOT_INTEGER: &str = "ERR value is not an integer or out of range";
pub const ERR_INVALID_EXPIRE_EXPIRE_CMD: &str = "ERR invalid expire time in 'expire' command";
pub const ERR_INVALID_EXPIRE_SET_CMD: &str = "ERR invalid expire time in 'set' command";
pub const ERR_INVALID_EXPIRE_GETEX_CMD: &str = "ERR invalid expire time in 'getex' command";
pub const ERR_INVALID_EXPIRE_PEXPIRE_CMD: &str = "ERR invalid expire time in 'pexpire' command";
pub const ERR_INVALID_EXPIRE_PEXPIREAT_CMD: &str = "ERR invalid expire time in PEXPIREAT";

/// Optional `[NX | XX | GT | LT]` modifiers on `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT`
/// (same semantics as Redis `expireGenericCommand`).
#[derive(Default, Clone, Copy)]
pub struct ExpireModifierFlags {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
}

/// Parse trailing tokens from `cmd[start..]` as expire modifiers; errors match Redis strings.
pub fn parse_expire_modifier_flags(
    cmd: &[&[u8]],
    start: usize,
) -> Result<ExpireModifierFlags, String> {
    let mut f = ExpireModifierFlags::default();
    let mut i = start;
    while i < cmd.len() {
        let raw = match parse_utf8(cmd[i]) {
            Some(s) => s,
            None => return Err("ERR syntax error".to_string()),
        };
        let u = raw.to_ascii_uppercase();
        match u.as_str() {
            "NX" => f.nx = true,
            "XX" => f.xx = true,
            "GT" => f.gt = true,
            "LT" => f.lt = true,
            _ => {
                return Err(format!("ERR Unsupported option {}", raw));
            }
        }
        i += 1;
    }
    if (f.lt || f.gt || f.xx) && f.nx {
        return Err(
            "ERR NX and XX, GT or LT options at the same time are not compatible".to_string(),
        );
    }
    if f.gt && f.lt {
        return Err("ERR GT and LT options at the same time are not compatible".to_string());
    }
    Ok(f)
}

/// Redis `expireGenericCommand` conditional checks (absolute `new_abs` vs existing deadline).
pub fn expire_modifier_allows_set(
    flags: &ExpireModifierFlags,
    current: Option<SystemTime>,
    new_abs: SystemTime,
) -> bool {
    if flags.nx && current.is_some() {
        return false;
    }
    if flags.xx && current.is_none() {
        return false;
    }
    // GT: set only if the new absolute deadline is after the current one. A
    // key with no time to live has, for this, an infinite one: nothing is
    // after it.
    if flags.gt && current.is_none_or(|c| new_abs <= c) {
        return false;
    }
    if flags.lt
        && let Some(c) = current
        && new_abs >= c
    {
        return false;
    }
    true
}

/// `EXPIRE`: relative seconds → absolute deadline in ms (`basetime` + `secs * 1000`), Redis bounds.
pub fn absolute_when_ms_from_expire_secs(secs: i128) -> Result<i128, &'static str> {
    let lo = (i64::MIN / 1000) as i128;
    let hi = (i64::MAX / 1000) as i128;
    if secs < lo || secs > hi {
        return Err(ERR_INVALID_EXPIRE_EXPIRE_CMD);
    }
    let ttl_ms = secs * 1000;
    let now_ms = wall_now_ms_i128();
    let when_ms = ttl_ms
        .checked_add(now_ms)
        .ok_or(ERR_INVALID_EXPIRE_EXPIRE_CMD)?;
    if when_ms < i64::MIN as i128 || when_ms > i64::MAX as i128 {
        return Err(ERR_INVALID_EXPIRE_EXPIRE_CMD);
    }
    Ok(when_ms)
}

/// `PEXPIRE`: relative millis → absolute deadline (`basetime` + `ms`).
pub fn absolute_when_ms_from_pexpire_ms(ms: i128) -> Result<i128, &'static str> {
    let now_ms = wall_now_ms_i128();
    let when_ms = now_ms
        .checked_add(ms)
        .ok_or(ERR_INVALID_EXPIRE_PEXPIRE_CMD)?;
    if when_ms < i64::MIN as i128 || when_ms > i64::MAX as i128 {
        return Err(ERR_INVALID_EXPIRE_PEXPIRE_CMD);
    }
    Ok(when_ms)
}

/// Wall clock in ms since UNIX epoch, matching [`crate::script_clock::ttl_wall_ms_now`].
#[inline]
pub fn wall_now_ms_i128() -> i128 {
    crate::script_clock::ttl_wall_ms_now() as i128
}

/// Parse a command token as a base-10 `i128` (no leading-only whitespace edge cases in Redis).
#[inline]
pub fn parse_bytes_as_i128(arg: &[u8]) -> Option<i128> {
    let s = std::str::from_utf8(arg).ok()?;
    if s.is_empty() {
        return None;
    }
    s.parse().ok()
}

/// Absolute [`SystemTime`] from signed Unix milliseconds (past → before UNIX_EPOCH).
pub fn system_time_from_unix_millis_i128(ms: i128) -> Option<SystemTime> {
    if ms >= 0 {
        let u = u64::try_from(ms).ok()?;
        UNIX_EPOCH.checked_add(Duration::from_millis(u))
    } else {
        let neg = ms.checked_neg()?;
        let u = u64::try_from(neg).ok()?;
        UNIX_EPOCH.checked_sub(Duration::from_millis(u))
    }
}

/// `TTL`: remaining whole seconds, rounded **up** (Redis semantics).
#[inline]
pub fn ttl_remaining_secs_ceil(remaining: Duration) -> i64 {
    let ms = remaining.as_millis();
    if ms == 0 {
        return 0;
    }
    let ceil_secs = ms.div_ceil(1000);
    if ceil_secs > i64::MAX as u128 {
        i64::MAX
    } else {
        ceil_secs as i64
    }
}

/// Relative TTL from seconds (`EXPIRE`): validates overflow against Redis `when` in ms.
pub fn deadline_after_relative_secs(
    secs: i128,
    err_invalid: &'static str,
) -> Result<SystemTime, &'static str> {
    if secs <= 0 {
        return Err(err_invalid);
    }
    let now_ms = wall_now_ms_i128();
    let ttl_ms = secs.checked_mul(1000).ok_or(err_invalid)?;
    let when_ms = now_ms.checked_add(ttl_ms).ok_or(err_invalid)?;
    if when_ms > REDIS_WHEN_MAX_MS {
        return Err(err_invalid);
    }
    system_time_from_unix_millis_i128(when_ms).ok_or(err_invalid)
}

/// Relative deadline from milliseconds (`PEXPIRE`, `SET PX`, …).
pub fn deadline_after_relative_millis(
    ms: i128,
    err_invalid: &'static str,
) -> Result<SystemTime, &'static str> {
    if ms <= 0 {
        return Err(err_invalid);
    }
    let now_ms = wall_now_ms_i128();
    let when_ms = now_ms.checked_add(ms).ok_or(err_invalid)?;
    if when_ms > REDIS_WHEN_MAX_MS {
        return Err(err_invalid);
    }
    system_time_from_unix_millis_i128(when_ms).ok_or(err_invalid)
}

#[macro_export]
macro_rules! read_lock {
    ($shard:expr) => {
        $shard.read()
    };
}

#[macro_export]
macro_rules! write_lock {
    ($shard:expr) => {
        $shard.write()
    };
}

/// Most keys one batch deletes from a shard between releases of its locks.
const ACTIVE_EXPIRE_BATCH: usize = 1024;

/// How long one cycle may run: a quarter of the time between cycles, as in
/// Redis. Reads expire keys lazily whatever this does.
const ACTIVE_EXPIRE_BUDGET: Duration = Duration::from_millis(25);

/// A cycle whose time runs out while more than this percentage of the keys
/// it looked at were past their time is behind, and the next starts at once:
/// Redis's `ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE`.
const ACTIVE_EXPIRE_STALE_PERCENT: usize = 10;

/// Where the active expiry cycle has got to. A cycle that runs out of time
/// is taken up from here by the next.
#[derive(Default)]
pub struct ExpireCursor {
    db: usize,
    shard: usize,
    /// This cycle's share of the shards: those whose number is `part` modulo
    /// `parts` (one thread of several, each with its own). `parts` 0 is 1.
    part: usize,
    parts: usize,
}

impl ExpireCursor {
    /// The cursor of expiry thread `part` of `parts`.
    pub fn part(part: usize, parts: usize) -> Self {
        Self {
            part,
            parts,
            ..Self::default()
        }
    }

    fn parts(&self) -> usize {
        self.parts.max(1)
    }

    fn owns(&self, shard: usize) -> bool {
        shard % self.parts() == self.part
    }
}

/// Threads that expire keys, each going through its own share of the
/// shards: a quarter of the cores, at least one. One thread deleted at most
/// about 700,000 keys a second, and a pipelined load of `SET ... EX` on 8
/// vCPUs wrote about 650,000 a second: it fell behind, and memory grew until
/// the server was killed. Each rests 100 ms between cycles when it is not
/// behind, so the threads cost next to nothing when there is little to do.
fn expiry_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() / 4)
        .unwrap_or(1)
        .max(1)
}

/// Active expiration (Redis `activeExpireCycle`): remove the keys whose time
/// has passed, in every database, without waiting for them to be read.
///
/// It goes round every shard of every database once, and stops when it has,
/// or when its time is up. It returns whether it is behind: its time ran out
/// while more than a tenth of the keys it looked at were past their time
/// ([`ACTIVE_EXPIRE_STALE_PERCENT`]), so the caller should come back at once
/// rather than in 100 ms. Running out of time with few expired keys found is
/// not being behind: the next cycle takes up where this one stopped. In a
/// shard it finds the keys past their time [`ACTIVE_EXPIRE_BATCH`] at a time,
/// each pass over the shard's expiry times stopping at its batch's last key,
/// and deletes each batch under one lock of the expiry shard and one of the
/// value shard.
///
/// It used to take twenty keys at a time, each time searching the shard from
/// its start: every twenty passed over the same live keys again, so the more
/// keys a shard held the slower it went. Under a steady stream of `SET ...
/// EX`, keys came due faster than that removed them (about 80,000 a second at
/// 17 million keys), and memory grew without end (#87).
///
/// Coming back at once whenever its time ran out, as #87's fix first did,
/// kept it scanning without a pause under steady TTL traffic: a pass over
/// 1.8 million live keys takes more than its 25 ms, and each found only the
/// few that had come due since. That was a whole core of eight, taken from
/// the workers, with its shard locks in their way.
pub fn active_expire_cycle(
    store: &Store,
    expiry_store: &ExpiryStore,
    index_store: &IndexStore,
    cursor: &mut ExpireCursor,
) -> bool {
    // The shards this cycle's part owns.
    let shards: usize = expiry_store
        .iter()
        .map(|db| (0..db.len()).filter(|&s| cursor.owns(s)).count())
        .sum();
    if shards == 0 {
        return false;
    }
    let started = std::time::Instant::now();
    let mut visited = 0usize;
    // Keys looked at, and of them those past their time.
    let (mut seen, mut due_total) = (0usize, 0usize);
    let behind = stale;
    while visited < shards {
        if cursor.db >= expiry_store.len() {
            cursor.db = 0;
        }
        let db_exp = &expiry_store[cursor.db];
        if cursor.shard >= db_exp.len() {
            cursor.db += 1;
            cursor.shard = 0;
            continue;
        }
        if !cursor.owns(cursor.shard) {
            cursor.shard += 1;
            continue;
        }
        let (db, shard) = (cursor.db, cursor.shard);
        let cell = &db_exp[shard];
        if !cell.holds_nothing() {
            let now = crate::script_clock::wall_system_time_now();
            // A batch at a time: a pass stops at the batch's last key, so it
            // copies no more keys than it deletes. Collecting every due key
            // of the shard first copied all of them, again each cycle, when
            // the budget ran out after the first batch: with 49 million keys
            // the thread spent its time copying and deleted 5,000 a second.
            loop {
                let (looked, due) = expire_pass(store, expiry_store, index_store, db, shard, now);
                seen += looked;
                due_total += due;
                if started.elapsed() >= ACTIVE_EXPIRE_BUDGET {
                    // This shard is gone through again next time: what is
                    // left of it, and what has come due since.
                    return behind(seen, due_total);
                }
                if due < ACTIVE_EXPIRE_BATCH {
                    break;
                }
            }
        }
        cursor.shard += 1;
        visited += 1;
        if visited.is_multiple_of(64) && started.elapsed() >= ACTIVE_EXPIRE_BUDGET {
            return visited < shards && behind(seen, due_total);
        }
    }
    false
}

/// One pass over shard `shard` of database `db`: up to
/// [`ACTIVE_EXPIRE_BATCH`] keys past their time at `now`, the pass stopping
/// at the last of them, deleted. Returns how many keys it looked at and how
/// many were due.
fn expire_pass(
    store: &Store,
    expiry_store: &ExpiryStore,
    index_store: &IndexStore,
    db: usize,
    shard: usize,
    now: SystemTime,
) -> (usize, usize) {
    let mut looked = 0usize;
    let due: Vec<Key> = read_lock!(expiry_store[db][shard])
        .iter()
        .inspect(|_| looked += 1)
        .filter(|(_, exp)| **exp < now)
        .take(ACTIVE_EXPIRE_BATCH)
        .map(|(k, _)| k.clone())
        .collect();
    if !due.is_empty() {
        expire_batch(store, expiry_store, index_store, db, shard, &due, now);
    }
    (looked, due.len())
}

/// Whether more than [`ACTIVE_EXPIRE_STALE_PERCENT`] of the `seen` keys
/// were `due`: a cycle whose time ran out with this true is behind.
fn stale(seen: usize, due: usize) -> bool {
    due > 0 && due * 100 > seen * ACTIVE_EXPIRE_STALE_PERCENT
}

/// Delete the keys of `batch`, all in shard `shard` of database `db`, that
/// are still past their time: one lock of the expiry shard to take them out
/// of it, then one of the value shard, the two never held together, as every
/// writer takes them. A key given a new time meanwhile is left alone.
fn expire_batch(
    store: &Store,
    expiry_store: &ExpiryStore,
    index_store: &IndexStore,
    db: usize,
    shard: usize,
    batch: &[Key],
    now: SystemTime,
) {
    let mut due: Vec<&Key> = Vec::with_capacity(batch.len());
    {
        let mut times = write_lock!(expiry_store[db][shard]);
        for key in batch {
            if times.get(key.as_str()).is_some_and(|exp| *exp < now) {
                times.remove(key.as_str());
                due.push(key);
            }
        }
    }
    if due.is_empty() {
        return;
    }
    // Both stores have as many shards, picked by the same hash; if that ever
    // changes, find each key's value shard.
    let values_db = &store[db];
    let same_shards = values_db.len() == expiry_store[db].len();
    let mut gone: Vec<&Key> = Vec::with_capacity(due.len());
    let mut freed: Vec<KiviDBValue> = Vec::with_capacity(due.len());
    if same_shards {
        let mut values = write_lock!(values_db[shard]);
        for key in due {
            if let Some(value) = values.remove(key.as_str()) {
                freed.push(value);
                gone.push(key);
            }
        }
    } else {
        for key in due {
            if let Some(value) = write_lock!(store::get_shard(values_db, key)).remove(key.as_str())
            {
                freed.push(value);
                gone.push(key);
            }
        }
    }
    // Freed outside the lock.
    drop(freed);
    for key in gone {
        crate::stats::record_expired();
        crate::stats::EXPIRED_KEYS_ACTIVE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::keyspace_events::notify_expired(store, db, key);
        crate::watch::expired(store, db, key);
        crate::tracking::expired(store, key);
        crate::vector::maybe_unindex(index_store, key);
    }
}

/// How often each expiry thread looks for tables to shrink.
const SHRINK_PERIOD: Duration = Duration::from_secs(1);

/// Shrink the tables, of values and of expiry times, of `cursor`'s shards
/// that keys have mostly left ([`store::shrink_if_sparse`], #113).
fn shrink_sparse_tables(store: &Store, expiry_store: &ExpiryStore, cursor: &ExpireCursor) {
    for db in store.iter() {
        for (s, shard) in db.iter().enumerate() {
            if cursor.owns(s) {
                store::shrink_if_sparse(shard);
            }
        }
    }
    for db in expiry_store.iter() {
        for (s, cell) in db.iter().enumerate() {
            if cursor.owns(s) {
                cell.shrink_if_sparse();
            }
        }
    }
}

/// Lazy TTL: delete an expired key before a read and return true if it was removed.
#[inline]
pub fn lazy_expire_key(store: &Store, expiry_store: &ExpiryStore, db: usize, key: &str) -> bool {
    if is_expired(expiry_store, db, key) {
        delete_key_if_expired(store, expiry_store, db, key)
    } else {
        false
    }
}

/// What a read does with a key it has found expired: deletes it, as
/// [`delete_key_if_expired`] does, unless writes are paused
/// ([`crate::pause::expiry_deferred`]). Then the key is left where it is, to
/// be deleted, counted and announced once the pause is over, and the read
/// takes it as gone all the same. Returns whether it is to be taken as gone.
///
/// For reads only. A write that goes on to work on the key needs it really
/// gone, and writes are held while writes are paused, so they never meet a
/// key left this way.
#[inline]
pub fn expire_on_read(store: &Store, expiry_store: &ExpiryStore, db: usize, key: &str) -> bool {
    if crate::pause::expiry_deferred() {
        return true;
    }
    delete_key_if_expired(store, expiry_store, db, key)
}

/// [`lazy_expire_key`] for a read: see [`expire_on_read`].
#[inline]
pub fn lazy_expire_on_read(
    store: &Store,
    expiry_store: &ExpiryStore,
    db: usize,
    key: &str,
) -> bool {
    is_expired(expiry_store, db, key) && expire_on_read(store, expiry_store, db, key)
}

/// Background sampler so keys — and hash fields — expire without a read
/// (Redis `active_expire_cycle`). `pubsub` is where their expiry is announced.
pub fn spawn_active_expire_task(
    store: Store,
    expiry_store: ExpiryStore,
    index_store: IndexStore,
    pubsub: crate::pubsub::PubSubHub,
    latency: crate::latency::SharedLatencyTracker,
) {
    crate::keyspace_events::register_hub(&store, &pubsub);
    // Whatever was loaded at startup that has field TTLs.
    crate::field_expiry::rescan(&store);
    let parts = expiry_threads();
    for part in 0..parts {
        let (store, expiry_store, index_store) =
            (store.clone(), expiry_store.clone(), index_store.clone());
        // The first thread also reports the latency event and sweeps the
        // hash fields' times.
        let latency = (part == 0).then(|| latency.clone());
        std::thread::spawn(move || {
            let mut cursor = ExpireCursor::part(part, parts);
            // Every 100 ms, as Redis's `hz 10`; straight after a cycle that
            // is behind (its time ran out while more than a tenth of what it
            // looked at was expired), as Redis goes on while it keeps finding
            // them.
            let mut wait = Duration::from_millis(100);
            let mut shrunk_at = std::time::Instant::now();
            loop {
                std::thread::sleep(wait);
                // Nothing expires while writes are paused, as in Redis: the
                // keys and fields that come due meanwhile go once the pause
                // is over.
                if crate::pause::expiry_deferred() {
                    continue;
                }
                let started = std::time::Instant::now();
                let behind = active_expire_cycle(&store, &expiry_store, &index_store, &mut cursor);
                wait = Duration::from_millis(if behind { 1 } else { 100 });
                if shrunk_at.elapsed() >= SHRINK_PERIOD {
                    shrink_sparse_tables(&store, &expiry_store, &cursor);
                    shrunk_at = std::time::Instant::now();
                }
                let Some(latency) = &latency else {
                    continue;
                };
                // Redis's `expire-cycle` latency event: a cycle that took at
                // least `latency-monitor-threshold` ms (0, the default, is
                // off).
                let threshold = crate::latency::threshold_ms();
                let took = started.elapsed().as_millis() as u64;
                if threshold > 0 && took >= threshold {
                    latency.record("expire-cycle", took.min(u32::MAX as u64) as u32);
                }
                crate::field_expiry::sweep(&store, &expiry_store, &index_store);
            }
        });
    }
}

/// Glob-style match (Redis KEYS / PSUBSCRIBE). Fast path avoids `glob::Pattern` stack use on
/// huge `*?…` patterns; bracket / escape syntax defers to `glob`.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    super::glob_redis::matches(pattern.as_bytes(), text.as_bytes())
}

/// [`glob_match`] of bytes: a channel's name need not be UTF-8.
pub fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    super::glob_redis::matches(pattern, text)
}

pub fn get_expiry_shard<'a>(
    expiry_store: &'a ExpiryStore,
    db: usize,
    key: &str,
) -> &'a ExpiryShard {
    let db = &expiry_store[db];
    store::get_expiry_shard(db, key)
}

/// Absolute wall time when a key expires (`now + ttl`). Safe across threads (unlike `Instant`).
#[inline]
pub fn expiry_at_after(ttl: Duration) -> SystemTime {
    crate::script_clock::wall_system_time_now()
        .checked_add(ttl)
        .unwrap_or_else(|| UNIX_EPOCH + ttl)
}

#[inline]
pub fn expiry_unix_secs(ts_secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(ts_secs)
}

#[inline]
pub fn expiry_unix_millis(ts_ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ts_ms)
}

pub fn is_expired(expiry_store: &ExpiryStore, db: usize, key: &str) -> bool {
    let shard = get_expiry_shard(expiry_store, db, key);
    // Usually: nothing in this shard expires, and no lock is needed to know.
    if shard.holds_nothing() {
        return false;
    }
    let map = read_lock!(shard);
    match map.get(key) {
        // Past it, not at it: a key lives through the millisecond it ends
        // in, as in Redis, so never for less than it was given.
        Some(exp) => crate::script_clock::wall_system_time_now() > *exp,
        None => false,
    }
}

// parse a command argument as utf8 - used everywhere
pub fn parse_utf8(bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(bytes).ok()
}

// write a value and clear its expiry atomically-ish
pub fn store_value(
    store: &Store,
    expiry_store: &ExpiryStore,
    db: usize,
    key: &str,
    value: KiviDBValue,
) {
    let db_ref = &store[db];
    let value_shard = store::get_shard(db_ref, key);
    {
        let mut map = write_lock!(value_shard);
        map.insert(Key::from(key), value);
    }

    // only clear expiry if one actually exists — avoid the write lock entirely
    let expiry_shard = get_expiry_shard(expiry_store, db, key);
    if expiry_shard.holds_nothing() {
        return;
    }
    {
        // cheap read lock first
        let map = read_lock!(expiry_shard);
        if !map.contains_key(key) {
            return; // no expiry set, skip the write lock
        }
    }
    // expiry exists, take write lock and remove it
    {
        let mut map = write_lock!(expiry_shard);
        map.remove(key);
    }
}

pub fn set_expiry(expiry_store: &ExpiryStore, db: usize, key: &str, expires_at: SystemTime) {
    let shard = get_expiry_shard(expiry_store, db, key);
    let mut map = write_lock!(shard);
    map.insert(Key::from(key), expires_at);
}

pub fn clear_expiry(expiry_store: &ExpiryStore, db: usize, key: &str) {
    let shard = get_expiry_shard(expiry_store, db, key);
    let mut map = write_lock!(shard);
    map.remove(key);
}

pub fn delete_key(store: &Store, expiry_store: &ExpiryStore, db: usize, key: &str) -> bool {
    let db_ref = &store[db];
    let value_shard = store::get_shard(db_ref, key);
    let removed = {
        let mut map = write_lock!(value_shard);
        map.remove(key).is_some()
    };
    clear_expiry(expiry_store, db, key);
    removed
}

/// [`delete_key`], handing back the value, for `UNLINK` to free: after the
/// shard lock is released, and in the background if it is large.
pub fn take_key(
    store: &Store,
    expiry_store: &ExpiryStore,
    db: usize,
    key: &str,
) -> Option<KiviDBValue> {
    let value_shard = store::get_shard(&store[db], key);
    let removed = write_lock!(value_shard).remove(key);
    clear_expiry(expiry_store, db, key);
    removed
}

/// [`delete_key`] plus `expired_keys` stat — use only after an `is_expired` check (lazy TTL).
#[inline]
pub fn delete_key_if_expired(
    store: &Store,
    expiry_store: &ExpiryStore,
    db: usize,
    key: &str,
) -> bool {
    let removed = delete_key(store, expiry_store, db, key);
    if removed {
        crate::stats::record_expired();
        crate::keyspace_events::notify_expired(store, db, key);
        crate::watch::expired(store, db, key);
        crate::tracking::expired(store, key);
    }
    removed
}

/// Format a score for RESP output.
/// Redis returns "1" not "1.0", but "1.5" not "1.50000000001".
/// A score that is a whole number of fewer than nine digits, as that
/// number: `3` for `3.0`. Most scores are (a rank, a count, a timestamp in
/// seconds is not, quite), and they are written as their digits and nothing
/// else. Finding that out the long way cost a formatted float and five
/// strings for every score in every reply.
///
/// Not zero: minus zero is a score too, and is written `-0`.
#[inline]
pub fn whole_score(score: f64) -> Option<i64> {
    if score != 0.0 && score.abs() < 1e8 && score.fract() == 0.0 {
        Some(score as i64)
    } else {
        None
    }
}

pub fn format_score(score: f64) -> String {
    if let Some(whole) = whole_score(score) {
        return whole.to_string();
    }
    if score == f64::INFINITY {
        return "inf".to_string();
    }
    if score == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if score == 0.0 {
        return if score.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    // The shortest digits that read back as the same double, laid out as
    // Redis lays them out (its `fpconv_dtoa`): plain decimal for ordinary
    // magnitudes, `d.ddde+XX` beyond them — 1.7976931348623157e+308, not
    // three hundred digits.
    let scientific = format!("{:e}", score.abs());
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let count = digits.len() as i32;
    // The number is `digits` × 10^k.
    let k = exponent - (count - 1);
    let body = if k >= 0 && exponent.abs() < count + 7 {
        format!("{digits}{}", "0".repeat(k as usize))
    } else if k < 0 && (k > -7 || exponent.abs() < 4) {
        let point = count + k;
        if point <= 0 {
            format!("0.{}{digits}", "0".repeat((-point) as usize))
        } else {
            format!(
                "{}.{}",
                &digits[..point as usize],
                &digits[point as usize..]
            )
        }
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        if count > 1 {
            format!("{}.{}e{sign}{}", &digits[..1], &digits[1..], exponent.abs())
        } else {
            format!("{digits}e{sign}{}", exponent.abs())
        }
    };
    if score < 0.0 {
        format!("-{body}")
    } else {
        body
    }
}

#[cfg(test)]
mod score_format_tests {
    use super::format_score;

    #[test]
    fn scores_are_written_as_redis_writes_them() {
        for (score, text) in [
            (0.0, "0"),
            (1.0, "1"),
            (-1.0, "-1"),
            (1.5, "1.5"),
            (-2.25, "-2.25"),
            (10.0, "10"),
            (100000.0, "100000"),
            (0.1, "0.1"),
            (0.001, "0.001"),
            (3.0e-5, "0.00003"),
            (1.0e-7, "1e-7"),
            (1.0e21, "1e+21"),
            (123456789012345680.0, "123456789012345680"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (-1.7976931348623157e308, "-1.7976931348623157e+308"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ] {
            assert_eq!(format_score(score), text, "{score:e}");
        }
    }
}

/// The arguments the multi-key pops share (`LMPOP`, `ZMPOP` and their
/// blocking forms): `numkeys key [key ...] <first | second> [COUNT count]`,
/// with `numkeys` at `cmd[at]`. Returns how many keys there are, whether the
/// second of `words` was given, and the count.
pub fn parse_mpop(
    cmd: &[&[u8]],
    at: usize,
    words: [&str; 2],
) -> Result<(usize, bool, usize), Vec<u8>> {
    use crate::resp::write::resp_error;
    let num_keys =
        cmd.get(at)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| resp_error("ERR numkeys should be greater than 0"))? as usize;
    let which = at + 1 + num_keys;
    let Some(word) = cmd.get(which) else {
        return Err(resp_error("ERR syntax error"));
    };
    let second = if word.eq_ignore_ascii_case(words[0].as_bytes()) {
        false
    } else if word.eq_ignore_ascii_case(words[1].as_bytes()) {
        true
    } else {
        return Err(resp_error("ERR syntax error"));
    };
    let count = match &cmd[which + 1..] {
        [] => 1,
        [option, n] if option.eq_ignore_ascii_case(b"COUNT") => std::str::from_utf8(n)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| resp_error("ERR count should be greater than 0"))?
            as usize,
        _ => return Err(resp_error("ERR syntax error")),
    };
    Ok((num_keys, second, count))
}

// ── Scanning ──────────────────────────────────────────────────────────────────
//
// SCAN, SSCAN, HSCAN and ZSCAN promise one thing: an element that is in the
// collection from the first call of a scan to the last is returned by one of
// them, whatever is added or removed in between.
//
// The cursor used to be a count of elements already returned, into an order
// — the hash map's own, or a sort done afresh on each call — that every
// insertion and removal shifts: delete a few elements behind the cursor and
// the scan steps over as many in front of it. (And the sort made each call
// of SSCAN and HSCAN cost a sort of the whole collection.)
//
// Here every element has a *position*: a hash of it, the same every time. A
// scan goes through the positions in order, and its cursor is the position
// to go on from. Adding and removing elements moves nothing: an element is
// where its hash puts it. One pass over the collection finds the next page.

/// Positions are 48 bits, so that a cursor has room above them for which
/// part of the keyspace it is in.
const SCAN_POSITION_BITS: u32 = 48;

/// Where `element` comes in the order a scan goes through its collection in.
pub fn scan_position(element: &[u8]) -> u64 {
    static HASHER: std::sync::LazyLock<ahash::RandomState> = std::sync::LazyLock::new(|| {
        ahash::RandomState::with_seeds(0x6b69_7669, 0x7363_616e, 0x636f_6e73, 0x6973_7465)
    });
    HASHER.hash_one(element) >> (64 - SCAN_POSITION_BITS)
}

/// The next page of a scan of a collection of `len` elements: the elements
/// from `cursor` on, as many as `count` asks for, and the cursor to go on
/// from — zero when that was the last page.
///
/// `count` is a hint, as in Redis. A page holds at least `len / pages`
/// elements whatever it says, so that a scan is over in `pages` calls: each
/// call is a pass over the collection, and `COUNT 10` on a million elements
/// would otherwise be a hundred thousand of them.
pub fn scan_page<T>(
    elements: impl Iterator<Item = (u64, T)>,
    cursor: u64,
    count: usize,
    len: usize,
    pages: usize,
) -> (Vec<T>, u64) {
    let want = count.max(len / pages.max(1)).max(1);
    let mut ahead: Vec<(u64, T)> = elements
        .filter(|(position, _)| *position >= cursor)
        .collect();
    if ahead.len() <= want {
        return (ahead.into_iter().map(|(_, element)| element).collect(), 0);
    }
    // The page ends before the position of the first element left out: so
    // that elements at one position are never split between two pages.
    ahead.select_nth_unstable_by_key(want, |(position, _)| *position);
    let mut boundary = ahead[want].0;
    if ahead[..want]
        .iter()
        .all(|(position, _)| *position == boundary)
    {
        // (Every one of them at a single position: take that position whole.)
        boundary += 1;
    }
    let more = ahead.iter().any(|(position, _)| *position >= boundary);
    (
        ahead
            .into_iter()
            .filter(|(position, _)| *position < boundary)
            .map(|(_, element)| element)
            .collect(),
        if more { boundary } else { 0 },
    )
}

/// `[MATCH pattern] [COUNT count]` and whatever else a scan command takes.
#[derive(Default)]
pub struct ScanOptions {
    pub pattern: Option<String>,
    pub count: usize,
    /// `SCAN … TYPE type`, in lower case.
    pub of_type: Option<String>,
    /// `HSCAN … NOVALUES`
    pub no_values: bool,
    /// `ZSCAN … NOSCORES`
    pub no_scores: bool,
}

/// The cursor and the options of a scan command, from `cmd[at..]`.
/// `extra` names the options this command takes besides `MATCH` and `COUNT`.
pub fn scan_arguments(
    cmd: &[&[u8]],
    at: usize,
    extra: &[&str],
) -> Result<(u64, ScanOptions), Vec<u8>> {
    use crate::resp::write::resp_error;
    let syntax = || resp_error("ERR syntax error");
    let cursor = cmd
        .get(at)
        .and_then(|raw| parse_utf8(raw))
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| resp_error("ERR invalid cursor"))?;
    let mut options = ScanOptions {
        count: 10,
        ..Default::default()
    };
    let mut i = at + 1;
    while i < cmd.len() {
        let word = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        let value = cmd.get(i + 1);
        match (word.as_str(), value) {
            ("MATCH", Some(pattern)) => {
                options.pattern = Some(String::from_utf8_lossy(pattern).into_owned());
                i += 2;
            }
            ("COUNT", Some(count)) => {
                options.count = match parse_utf8(count).and_then(|s| s.parse::<i64>().ok()) {
                    Some(n) if n >= 1 => n as usize,
                    Some(_) => return Err(syntax()),
                    None => return Err(resp_error("ERR value is not an integer or out of range")),
                };
                i += 2;
            }
            ("TYPE", Some(of_type)) if extra.contains(&"TYPE") => {
                options.of_type = Some(String::from_utf8_lossy(of_type).to_ascii_lowercase());
                i += 2;
            }
            ("NOVALUES", _) if extra.contains(&"NOVALUES") => {
                options.no_values = true;
                i += 1;
            }
            ("NOSCORES", _) if extra.contains(&"NOSCORES") => {
                options.no_scores = true;
                i += 1;
            }
            _ => return Err(syntax()),
        }
    }
    Ok((cursor, options))
}

#[cfg(test)]
mod scan_tests {
    use super::{scan_page, scan_position};

    /// A whole scan of `set`, with `change` run on it after each page.
    fn scan(
        set: &mut std::collections::BTreeSet<u32>,
        count: usize,
        mut change: impl FnMut(&mut std::collections::BTreeSet<u32>, usize),
    ) -> Vec<u32> {
        let mut seen = Vec::new();
        let mut cursor = 0;
        let mut calls = 0;
        loop {
            let (page, next) = scan_page(
                set.iter().map(|n| (scan_position(&n.to_le_bytes()), *n)),
                cursor,
                count,
                set.len(),
                1024,
            );
            seen.extend(page);
            calls += 1;
            change(set, calls);
            if next == 0 {
                return seen;
            }
            assert!(next > cursor, "a scan goes forward");
            cursor = next;
        }
    }

    #[test]
    fn everything_is_returned_once_when_nothing_changes() {
        let mut set: std::collections::BTreeSet<u32> = (0..5000).collect();
        let mut seen = scan(&mut set, 10, |_, _| {});
        assert_eq!(seen.len(), 5000);
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 5000);
    }

    #[test]
    fn what_stays_is_returned_whatever_else_comes_and_goes() {
        // 0..100 stay; the rest are removed part of the way through, and
        // others added: the collection shrinks and grows under the scan.
        let mut set: std::collections::BTreeSet<u32> = (0..1100).collect();
        let seen = scan(&mut set, 10, |set, calls| {
            if calls == 3 {
                set.retain(|n| *n < 100);
            }
            if calls == 5 {
                set.extend(5000..7000);
            }
            if calls == 9 {
                set.retain(|n| *n < 100 || *n % 2 == 0);
            }
        });
        for n in 0..100 {
            assert!(
                seen.contains(&n),
                "{n} was there throughout and was not returned"
            );
        }
    }

    #[test]
    fn a_page_is_not_smaller_than_a_scan_in_a_thousand_calls_needs() {
        let set: Vec<u32> = (0..100_000).collect();
        let (page, next) = scan_page(
            set.iter().map(|n| (scan_position(&n.to_le_bytes()), *n)),
            0,
            10,
            set.len(),
            1024,
        );
        assert!(page.len() >= 97 && next != 0);
        // A small collection comes in one page.
        let (page, next) = scan_page([(5u64, 'a'), (9, 'b')].into_iter(), 0, 10, 2, 1024);
        assert_eq!((page, next), (vec!['a', 'b'], 0));
    }
}

// ── Names that are not text ───────────────────────────────────────────────────
//
// A hash keeps its fields by name, as `String`s. Redis's field names are
// bytes: any bytes. So that a hash can hold them all, a name that is not
// valid UTF-8 is kept in a form that is — each byte that is not part of
// valid text becomes one character from a block set aside for it (U+F780 –
// U+F7FF, private use: byte 0x80 is U+F780, 0xFF is U+F7FF) — and is turned
// back on the way out. Ordinary text is itself, both ways, and costs one
// scan. Text that happens to use those characters has their bytes put in
// the same form, so that nothing can be mistaken for something else.

const BYTE_BLOCK: u32 = 0xF700;

fn in_byte_block(c: char) -> bool {
    (0xF780..=0xF7FF).contains(&(c as u32))
}

fn push_as_block(out: &mut String, bytes: &[u8]) {
    for byte in bytes {
        // 0x80..=0xFF only: bytes below that are text wherever they are.
        match char::from_u32(BYTE_BLOCK + *byte as u32) {
            Some(c) if *byte >= 0x80 => out.push(c),
            _ => out.push(*byte as char),
        }
    }
}

/// The `String` a name of any bytes is kept as.
pub fn name_from_bytes(raw: &[u8]) -> std::borrow::Cow<'_, str> {
    if let Ok(text) = std::str::from_utf8(raw)
        && !text.contains(in_byte_block)
    {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(raw.len() + 8);
    let mut rest = raw;
    while !rest.is_empty() {
        let (text, bad) = match std::str::from_utf8(rest) {
            Ok(text) => (text, 0),
            Err(e) => {
                let good = e.valid_up_to();
                // The bytes of one broken sequence, or everything left.
                let bad = e.error_len().unwrap_or(rest.len() - good);
                // Valid up to `good`, by the error's own account.
                (std::str::from_utf8(&rest[..good]).unwrap_or(""), bad)
            }
        };
        for c in text.chars() {
            if in_byte_block(c) {
                let mut utf8 = [0u8; 4];
                push_as_block(&mut out, c.encode_utf8(&mut utf8).as_bytes());
            } else {
                out.push(c);
            }
        }
        let consumed = text.len();
        push_as_block(&mut out, &rest[consumed..consumed + bad]);
        rest = &rest[consumed + bad..];
    }
    std::borrow::Cow::Owned(out)
}

/// The bytes a kept name stands for.
pub fn name_to_bytes(name: &str) -> std::borrow::Cow<'_, [u8]> {
    if !name.contains(in_byte_block) {
        return std::borrow::Cow::Borrowed(name.as_bytes());
    }
    let mut out = Vec::with_capacity(name.len());
    for c in name.chars() {
        if in_byte_block(c) {
            out.push((c as u32 - BYTE_BLOCK) as u8);
        } else {
            let mut utf8 = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut utf8).as_bytes());
        }
    }
    std::borrow::Cow::Owned(out)
}

#[cfg(test)]
mod name_tests {
    use super::{name_from_bytes, name_to_bytes};

    #[test]
    fn any_bytes_survive_the_round_trip() {
        let mut cases: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"plain".to_vec(),
            "caf\u{e9} \u{1F600}".as_bytes().to_vec(),
            vec![0xff],
            vec![0x80, 0x00, 0x7f, 0xfe],
            b"abc\xc3".to_vec(),                    // a sequence cut short
            b"\xe2\x82 tail".to_vec(),              // and one cut in the middle
            "\u{F780}\u{F7FF}".as_bytes().to_vec(), // the block's own characters
            (0u8..=255).collect(),
        ];
        // And a few thousand pseudo-random ones.
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for len in 0..2000usize {
            let bytes: Vec<u8> = (0..len % 37)
                .map(|_| {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (state >> 56) as u8
                })
                .collect();
            cases.push(bytes);
        }
        let mut seen = std::collections::HashMap::new();
        for raw in cases {
            let name = name_from_bytes(&raw).into_owned();
            assert_eq!(name_to_bytes(&name).as_ref(), raw.as_slice(), "{raw:?}");
            // Two different inputs never share a name.
            if let Some(other) = seen.insert(name.clone(), raw.clone()) {
                assert_eq!(other, raw, "{name:?}");
            }
        }
        // Text is itself.
        assert!(matches!(
            name_from_bytes(b"field"),
            std::borrow::Cow::Borrowed("field")
        ));
    }
}

#[cfg(test)]
mod whole_score_tests {
    use super::{format_score, whole_score};

    #[test]
    fn a_whole_score_is_written_as_its_digits_and_the_rest_as_before() {
        for (score, text) in [
            (3.0, "3"),
            (-12.0, "-12"),
            (1.0, "1"),
            (99_999_999.0, "99999999"),
            (-99_999_999.0, "-99999999"),
            // Past eight digits it is the long way round, as it always was.
            (100_000_000.0, "1e+8"),
            (10_000_000.0, "10000000"),
            (123_456_789.0, "123456789"),
            (0.0, "0"),
            (-0.0, "-0"),
            (1.5, "1.5"),
            (-0.25, "-0.25"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ] {
            assert_eq!(format_score(score), text, "{score:?}");
        }
        assert_eq!(whole_score(42.0), Some(42));
        assert_eq!(whole_score(0.0), None);
        assert_eq!(whole_score(-0.0), None);
        assert_eq!(whole_score(0.5), None);
        assert_eq!(whole_score(1e8), None);
        assert_eq!(whole_score(f64::NAN), None);
        assert_eq!(whole_score(f64::INFINITY), None);
    }
}

#[cfg(test)]
mod active_expiry_tests {
    use super::*;
    use crate::shared_bytes::SharedBytes;

    fn put(store: &Store, expiry: &ExpiryStore, key: &str, at: SystemTime) {
        store_value(
            store,
            expiry,
            0,
            key,
            KiviDBValue::String(SharedBytes::from(&b"v"[..])),
        );
        set_expiry(expiry, 0, key, at);
    }

    fn keys(store: &Store) -> usize {
        store[0].iter().map(|s| s.read().len()).sum()
    }

    fn times(expiry: &ExpiryStore) -> usize {
        expiry[0].iter().map(|s| s.read().len()).sum()
    }

    /// Every key past its time goes, in as few cycles as the budget allows;
    /// a key with time left, or none, stays.
    #[test]
    fn a_cycle_removes_every_expired_key_and_no_other() {
        let store = store::new_store(1);
        let expiry = store::new_expiry_store(1);
        let index: IndexStore = Default::default();
        let past = SystemTime::now() - Duration::from_secs(5);
        let future = SystemTime::now() + Duration::from_secs(3600);
        for i in 0..20_000 {
            put(&store, &expiry, &format!("gone:{i}"), past);
            put(&store, &expiry, &format!("kept:{i}"), future);
        }
        store_value(
            &store,
            &expiry,
            0,
            "forever",
            KiviDBValue::String(SharedBytes::from(&b"v"[..])),
        );
        let before = crate::stats::EXPIRED_KEYS_ACTIVE.load(std::sync::atomic::Ordering::Relaxed);

        let mut cursor = ExpireCursor::default();
        let mut cycles = 0;
        while keys(&store) > 20_001 {
            active_expire_cycle(&store, &expiry, &index, &mut cursor);
            cycles += 1;
            assert!(cycles < 1000, "the cycle never caught up");
        }
        assert_eq!(keys(&store), 20_001);
        assert_eq!(times(&expiry), 20_000);
        assert!(
            store[0]
                .iter()
                .all(|s| s.read().keys().all(|k| !k.starts_with("gone:")))
        );
        let after = crate::stats::EXPIRED_KEYS_ACTIVE.load(std::sync::atomic::Ordering::Relaxed);
        assert!(after - before >= 20_000);
    }

    /// A key whose time was put back after the cycle found it due is kept.
    #[test]
    fn a_key_given_a_new_time_is_kept() {
        let store = store::new_store(1);
        let expiry = store::new_expiry_store(1);
        let index: IndexStore = Default::default();
        let past = SystemTime::now() - Duration::from_secs(5);
        put(&store, &expiry, "k", past);
        let shard = store::shard_index(expiry[0].len(), "k");
        set_expiry(&expiry, 0, "k", SystemTime::now() + Duration::from_secs(60));
        expire_batch(
            &store,
            &expiry,
            &index,
            0,
            shard,
            &[Key::from("k")],
            SystemTime::now(),
        );
        assert_eq!(keys(&store), 1);
        assert_eq!(times(&expiry), 1);
    }

    /// Behind only when more than a tenth of what was seen was due: the few
    /// keys that come due between passes under steady TTL traffic do not
    /// keep the thread from resting.
    #[test]
    fn behind_takes_more_than_a_tenth_due() {
        assert!(!stale(1_800_000, 9_000));
        assert!(!stale(1_000, 100));
        assert!(stale(1_000, 101));
        assert!(stale(40_000, 20_000));
        assert!(!stale(0, 0));
    }

    /// A cycle that goes round every shard within its time is not behind,
    /// whatever it removed, and keeps what is live.
    #[test]
    fn a_full_round_is_not_behind() {
        let store = store::new_store(1);
        let expiry = store::new_expiry_store(1);
        let index: IndexStore = Default::default();
        let past = SystemTime::now() - Duration::from_secs(5);
        let future = SystemTime::now() + Duration::from_secs(3600);
        for i in 0..2_000 {
            put(&store, &expiry, &format!("kept:{i}"), future);
        }
        put(&store, &expiry, "gone", past);
        let mut cursor = ExpireCursor::default();
        assert!(!active_expire_cycle(&store, &expiry, &index, &mut cursor));
        assert_eq!(keys(&store), 2_000);
    }

    /// A shard with three batches' worth due: a pass takes one batch and
    /// stops there, so it copies no more keys than it deletes (it used to copy
    /// every due key of the shard, then run out of time after one batch).
    #[test]
    fn a_pass_takes_one_batch_and_stops() {
        let store = store::new_store(1);
        let expiry = store::new_expiry_store(1);
        let index: IndexStore = Default::default();
        let past = SystemTime::now() - Duration::from_secs(5);
        let shards = expiry[0].len();
        // Long keys, as UUIDs are: past CompactString's inline length.
        let mut n = 0;
        let mut i = 0;
        while n < 3 * ACTIVE_EXPIRE_BATCH {
            let key = format!("gone:{i:0>40}");
            i += 1;
            if store::shard_index(shards, &key) == 0 {
                put(&store, &expiry, &key, past);
                n += 1;
            }
        }
        let (looked, due) = expire_pass(&store, &expiry, &index, 0, 0, SystemTime::now());
        assert_eq!(due, ACTIVE_EXPIRE_BATCH);
        assert_eq!(
            looked, ACTIVE_EXPIRE_BATCH,
            "it went past the batch's last key"
        );
        assert_eq!(keys(&store), n - ACTIVE_EXPIRE_BATCH);

        let mut cursor = ExpireCursor::default();
        let mut cycles = 0;
        while keys(&store) > 0 {
            active_expire_cycle(&store, &expiry, &index, &mut cursor);
            cycles += 1;
            assert!(cycles < 1000, "never emptied");
        }
    }

    /// Two parts between them go through every shard, each only its own.
    #[test]
    fn parts_share_the_shards() {
        let store = store::new_store(1);
        let expiry = store::new_expiry_store(1);
        let index: IndexStore = Default::default();
        let past = SystemTime::now() - Duration::from_secs(5);
        for i in 0..5_000 {
            put(&store, &expiry, &format!("gone:{i}"), past);
        }
        let mut even = ExpireCursor::part(0, 2);
        while active_expire_cycle(&store, &expiry, &index, &mut even) {}
        let shards = expiry[0].len();
        for (s, cell) in expiry[0].iter().enumerate() {
            let left = cell.read().len();
            if s % 2 == 0 {
                assert_eq!(left, 0, "shard {s} is part 0's");
            }
        }
        let odd_left: usize = (1..shards)
            .step_by(2)
            .map(|s| expiry[0][s].read().len())
            .sum();
        assert!(odd_left > 0, "part 0 went through part 1's shards");
        let mut odd = ExpireCursor::part(1, 2);
        while active_expire_cycle(&store, &expiry, &index, &mut odd) {}
        assert_eq!(times(&expiry), 0);
        assert_eq!(keys(&store), 0);
    }
}
