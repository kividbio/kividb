use crate::commands::ctx::Ctx;
use crate::commands::helpers::*;
use crate::keyspace_events::{KS_GENERIC, KS_STRING, notify};
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::types::SharedBytes;
use crate::{read_lock, write_lock};
use std::time::{SystemTime, UNIX_EPOCH};

/// The longest a string value may be: Redis's default `proto-max-bulk-len`.
const MAX_STRING_LEN: usize = 512 * 1024 * 1024;

/// The key and value pairs of an `MSET`-shaped command, or `None` if a key
/// is not UTF-8.
fn key_value_pairs<'a>(cmd: &[&'a [u8]]) -> Vec<(&'a [u8], &'a [u8])> {
    cmd[1..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (pair[0], pair[1]))
        .collect()
}

/// The shards a command's keys are in, each once, in the order they stand
/// in the database.
///
/// That is the order to lock them in. Whatever else holds more than one
/// shard at a time (a snapshot, a rewrite of the AOF) takes them in that
/// order too, so no two of them can each hold what the other waits for.
fn shards_of<'k>(db: &store::Db, keys: impl Iterator<Item = &'k [u8]>) -> Vec<usize> {
    let mut indexes: Vec<usize> = keys.map(|key| store::shard_index(db.len(), key)).collect();
    indexes.sort_unstable();
    indexes.dedup();
    indexes
}

/// Every shard a command's keys are in, locked for reading at once: what
/// makes a read of several keys a read of one moment.
struct ReadShards<'a> {
    shards: usize,
    held: Vec<(usize, parking_lot::RwLockReadGuard<'a, store::ShardMap>)>,
}

impl<'a> ReadShards<'a> {
    fn lock<'k>(db: &'a store::Db, keys: impl Iterator<Item = &'k [u8]>) -> Self {
        let held = shards_of(db, keys)
            .into_iter()
            .map(|index| (index, db[index].read()))
            .collect();
        Self {
            shards: db.len(),
            held,
        }
    }

    /// The table `key` is in, if it is a key this was locked for.
    fn map(&self, key: &[u8]) -> Option<&store::ShardMap> {
        let index = store::shard_index(self.shards, key);
        let at = self
            .held
            .binary_search_by_key(&index, |(have, _)| *have)
            .ok()?;
        Some(&self.held[at].1)
    }
}

/// The same for writing: what makes a write of several keys one step.
pub(super) struct WriteShards<'a> {
    shards: usize,
    held: Vec<(usize, store::ShardWrite<'a>)>,
}

impl Drop for WriteShards<'_> {
    /// Every shard is let go of before anything a write left is announced
    /// ([`store::unlock_together`]).
    fn drop(&mut self) {
        store::unlock_together(self.held.drain(..).map(|(_, shard)| shard));
    }
}

impl<'a> WriteShards<'a> {
    /// Every shard of `keys`, held at once, and never waited for while
    /// another is held.
    ///
    /// Each shard is tried, in the database's order. If one is busy,
    /// everything taken so far is let go, that one is waited for alone
    /// (spinning a little first, see [`store::ShardLock::write_after_spinning`]),
    /// and the others are tried again with it held. Taking them in order
    /// and waiting for each in turn, as this did, held the shards already
    /// taken while it waited: with many `MSET`s of keys in many shards, one
    /// that waited held up every other that wanted one of its shards, and
    /// those the next, and the server sat idle in a queue of them (166K
    /// `MSET`s of 10 keys a second at 30% CPU on 32 vCPUs).
    ///
    /// No deadlock: nothing waits while it holds a shard, so no two can
    /// each hold what the other waits for, whatever order anybody else
    /// takes them in. Nobody sees part of the write either: it starts only
    /// once every shard is held.
    ///
    /// Out of line: `SET` is in the same `handle`, and must not pay for it.
    #[inline(never)]
    pub(super) fn lock<'k>(db: &'a store::Db, keys: impl Iterator<Item = &'k [u8]>) -> Self {
        let indexes = shards_of(db, keys);
        let mut held = Vec::with_capacity(indexes.len());
        // The shard last found busy, held from when it was waited for.
        let mut waited: Option<(usize, store::ShardWrite<'a>)> = None;
        'all: loop {
            for &index in &indexes {
                let guard = match waited.take() {
                    Some((at, guard)) if at == index => guard,
                    other => {
                        waited = other;
                        match db[index].try_write() {
                            Some(guard) => guard,
                            None => {
                                held.clear();
                                drop(waited.take());
                                waited = Some((index, db[index].write_after_spinning()));
                                continue 'all;
                            }
                        }
                    }
                };
                held.push((index, guard));
            }
            break;
        }
        Self {
            shards: db.len(),
            held,
        }
    }

    /// The table `key` is in. The key must be one this was locked for.
    pub(super) fn map(&mut self, key: &[u8]) -> &mut store::ShardMap {
        let index = store::shard_index(self.shards, key);
        let at = self
            .held
            .binary_search_by_key(&index, |(have, _)| *have)
            .expect("a key of the command the shards were locked for");
        &mut self.held[at].1
    }
}

/// `n` in decimal, written into `buf`: what the counters write, without a
/// `String` to make and free.
#[inline]
fn decimal(n: i64, buf: &mut [u8; 20]) -> &[u8] {
    let mut at = buf.len();
    let mut rest = n.unsigned_abs();
    loop {
        at -= 1;
        buf[at] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    if n < 0 {
        at -= 1;
        buf[at] = b'-';
    }
    &buf[at..]
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    match name {
        // ── SET ───────────────────────────────────────────────────────────────────────
        //   SET key value
        //       [NX | XX | IFEQ cmp-val | IFNE cmp-val | IFDEQ hex-digest | IFDNE hex-digest]
        //       [GET]
        //       [EX seconds | PX millis | EXAT unix-secs | PXAT unix-millis | KEEPTTL]
        //
        // Design: we parse options first with zero allocations, then take the write
        // lock exactly once. The bare "SET key value" fast path is identical to before
        // — same lock, same Arc::from, same resp_simple("OK"). No regression.
        "SET" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'set' command",
                ));
            }

            let key: &[u8] = cmd[1];

            // ── parse options (everything after the value) ────────────────────────
            // We use indices into cmd to avoid allocating strings for option names.
            #[derive(PartialEq)]
            enum Condition {
                None,
                NX,
                XX,
                IfeqVal,
                IfneVal,
                IfdeqDigest,
                IfdneDigest,
            }

            let mut condition = Condition::None;
            let mut cond_arg: Option<&[u8]> = None; // points into cmd — zero-copy
            let mut want_get = false;
            let mut keepttl = false;
            // The time option and its argument: read once every option has
            // been, as Redis reads them.
            let mut expire: Option<(TimeArg, &[u8])> = None;

            let mut i = 3usize;
            while i < cmd.len() {
                let tok = std::str::from_utf8(cmd[i])
                    .unwrap_or("")
                    .to_ascii_uppercase();
                match tok.as_str() {
                    // The condition options are mutually exclusive. `SET k v NX
                    // IFEQ x` used to be accepted, the last one silently winning.
                    "NX" => {
                        if !matches!(condition, Condition::None | Condition::NX) {
                            return Some(resp_error("ERR syntax error"));
                        }
                        condition = Condition::NX;
                    }
                    "XX" => {
                        if !matches!(condition, Condition::None | Condition::XX) {
                            return Some(resp_error("ERR syntax error"));
                        }
                        condition = Condition::XX;
                    }
                    "GET" => {
                        want_get = true;
                    }
                    "KEEPTTL" => {
                        if expire.is_some() {
                            return Some(resp_error("ERR syntax error"));
                        }
                        keepttl = true;
                    }

                    "IFEQ" | "IFNE" | "IFDEQ" | "IFDNE" => {
                        if condition != Condition::None {
                            return Some(resp_error("ERR syntax error"));
                        }
                        i += 1;
                        cond_arg = match cmd.get(i) {
                            Some(b) => Some(b),
                            None => return Some(resp_error("ERR syntax error")),
                        };
                        condition = match tok.as_str() {
                            "IFEQ" => Condition::IfeqVal,
                            "IFNE" => Condition::IfneVal,
                            "IFDEQ" => Condition::IfdeqDigest,
                            _ => Condition::IfdneDigest,
                        };
                    }

                    // One unit, and not with KEEPTTL, as Redis parses them;
                    // the same one again replaces it. `EX 10 PX 10` was the
                    // last one, and `KEEPTTL EX 10` an invalid time. From a
                    // stream, mixed units are still the last one: 1.2.0 took
                    // them, and logged `SET k v EX 10 EX 20` as `SET k v PXAT
                    // <t> EX 20`.
                    "EX" | "PX" | "EXAT" | "PXAT" => {
                        let arg = TimeArg::of_option(cmd[i]);
                        if keepttl
                            || i + 1 >= cmd.len()
                            || (expire.is_some_and(|(unit, _)| Some(unit) != arg)
                                && !applying_stream(ctx))
                        {
                            return Some(resp_error("ERR syntax error"));
                        }
                        expire = arg.map(|arg| (arg, cmd[i + 1]));
                        i += 1;
                    }

                    _ => {
                        // Unknown token — Redis returns a syntax error
                        return Some(resp_error("ERR syntax error"));
                    }
                }
                i += 1;
            }

            // ── the time, as Redis reads it: an integer, then in bounds ──────────
            let expiry = match expire {
                Some((arg, raw)) => {
                    match key_deadline(raw, arg, ERR_INVALID_EXPIRE_SET_CMD, applying_stream(ctx)) {
                        Ok(t) => Some(t),
                        Err(e) => return Some(resp_error(e)),
                    }
                }
                None => None,
            };

            // ── check & handle expiry on the existing key ─────────────────────────
            // Do this before taking the write lock so we don't hold it longer than needed.
            let db = &ctx.store[*ctx.selected_db];
            let index = store::shard_index(db.len(), key);
            let shard = &db[index];
            if !shard.holds_no_ttl() && is_expired(ctx.store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, *ctx.selected_db, key);
            }

            // ── single write-lock scope ───────────────────────────────────────────
            let mut map = write_lock!(shard);

            // Looked up once; what follows asks this, not the map again. A
            // plain SET asks nothing of what is there: it is not looked up
            // before it is written over.
            let existing = if want_get || condition != Condition::None {
                map.get(key)
            } else {
                None
            };
            let key_exists = existing.is_some();

            // GET: capture old value before any mutation (nil if absent/wrong-type)
            let old_val: Option<SharedBytes> = if want_get {
                match existing {
                    Some(ValueRef::String(v)) => {
                        crate::stats::record_hit();
                        Some(v.to_shared())
                    }
                    Some(_) => {
                        crate::stats::record_hit();
                        // Redis returns WRONGTYPE if GET is used on a non-string key
                        return Some(resp_error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ));
                    }
                    None => {
                        crate::stats::record_miss();
                        None
                    }
                }
            } else {
                None
            };

            // ── condition checks ──────────────────────────────────────────────────
            // Redis SET overwrites values of any type for unconditional SET, and for XX
            // "exists" means the key is present (not only a string). IFEQ/IFNE/... need a
            // string to compare — wrong type is WRONGTYPE.
            let current_bytes: Option<SharedBytes> = match existing {
                // Only a condition that compares values reads this.
                Some(ValueRef::String(_))
                    if matches!(condition, Condition::None | Condition::NX | Condition::XX) =>
                {
                    None
                }
                Some(ValueRef::String(v)) => Some(v.to_shared()),
                Some(_) => {
                    if condition == Condition::NX {
                        return Some(resp_null_bulk());
                    }
                    match &condition {
                        Condition::None | Condition::XX => None,
                        Condition::IfeqVal
                        | Condition::IfneVal
                        | Condition::IfdeqDigest
                        | Condition::IfdneDigest => {
                            return Some(resp_error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            ));
                        }
                        Condition::NX => None,
                    }
                }
                None => None,
            };

            if matches!(condition, Condition::IfdeqDigest | Condition::IfdneDigest)
                && current_bytes.is_some()
                && cond_arg.is_some_and(|digest| digest.len() != 16)
            {
                return Some(resp_error("ERR must be exactly 16 hexadecimal characters"));
            }

            let should_write = match &condition {
                Condition::None => true,

                // NX: only write if key does NOT exist
                Condition::NX => !key_exists,

                // XX: only write if key DOES exist
                Condition::XX => key_exists,

                // IFEQ: only write if existing value == cond_arg
                Condition::IfeqVal => match &current_bytes {
                    Some(v) => v.as_ref() == cond_arg.unwrap_or(&[]),
                    None => false,
                },

                // IFNE: only write if existing value != cond_arg
                Condition::IfneVal => match &current_bytes {
                    Some(v) => v.as_ref() != cond_arg.unwrap_or(&[]),
                    None => true, // absent key — treat as not-equal, write proceeds
                },

                // IFDEQ: only write if digest(existing value) == cond_arg (hex)
                Condition::IfdeqDigest => match &current_bytes {
                    Some(v) => digest_matches(v, cond_arg.unwrap_or(&[])),
                    None => false,
                },

                // IFDNE: only write if digest(existing value) != cond_arg
                Condition::IfdneDigest => match &current_bytes {
                    Some(v) => !digest_matches(v, cond_arg.unwrap_or(&[])),
                    None => true,
                },
            };

            if !should_write {
                // Condition not met — return nil (GET response) or nil bulk
                drop(map);
                return Some(if want_get {
                    match old_val {
                        Some(v) => resp_bulk(&v),
                        None => resp_null_bulk(),
                    }
                } else {
                    resp_null_bulk()
                });
            }

            // ── write ─────────────────────────────────────────────────────────────
            // The value and its time to live, in one step: the key's own
            // with KEEPTTL, else the one given, or none. Over the old value
            // in its own entry when it can be (`ShardMap::set_str`).
            let replaced = if keepttl {
                map.set_str(key, cmd[2])
            } else {
                map.set_str_expiring(key, cmd[2], expiry.map(unix_ms))
            };
            // A document written over has left the indexes that held it by
            // the time the shard is let go of here (`ShardMap::put`).
            drop(map);
            // The value that was there is let go of with the shard unlocked:
            // its count is in memory nothing has touched lately, and freeing
            // it is the allocator's time, neither of them the shard's to wait
            // for.
            drop(replaced);

            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            if !keepttl && expiry.is_some() {
                notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            }
            // Return: GET → old value (or nil); no GET → +OK
            Some(if want_get {
                match old_val {
                    Some(v) => resp_bulk(&v),
                    None => resp_null_bulk(),
                }
            } else {
                resp_simple("OK")
            })
        }

        "GET" => {
            if cmd.len() != 2 {
                return Some(resp_error("ERR wrong number of args for 'get' command"));
            }
            let key: &[u8] = cmd[1];

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            // The key's time to live is in the entry the value is read from,
            // and looked at there, once.
            Some(match map.lookup(key) {
                store::Lookup::Past => {
                    drop(map);
                    expire_on_read(ctx.store, *ctx.selected_db, key);
                    crate::stats::record_miss();
                    resp_null_bulk()
                }
                store::Lookup::Live(ValueRef::String(v)) => {
                    crate::stats::record_hit();
                    resp_bulk(&v)
                }
                store::Lookup::Live(_) => {
                    crate::stats::record_hit();
                    resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")
                }
                store::Lookup::Absent => {
                    crate::stats::record_miss();
                    resp_null_bulk()
                }
            })
        }

        "EXPIRE" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'expire' command",
                ));
            }
            let key: &[u8] = cmd[1];
            // The options are read before the time, as in Redis.
            let flags = match parse_expire_modifier_flags(cmd.as_slice(), 3) {
                Ok(f) => f,
                Err(msg) => return Some(resp_error(&msg)),
            };
            // The clock is read once, here.
            let (secs, new_abs) = match expire_deadline(
                cmd[2],
                TimeArg::Secs,
                ERR_INVALID_EXPIRE_EXPIRE_CMD,
                applying_stream(ctx),
            ) {
                Ok(t) => t,
                Err(e) => return Some(resp_error(e)),
            };
            // Only a time to live of zero or less deletes the key here, as in
            // Redis, and as `expire_feed` propagates it. A positive one ends
            // after the time it was added to, however late this is: a key
            // whose time has already come by now goes the way every expired
            // key goes, with its `DEL`. Deleted here, it went with nothing
            // propagated, and stayed on every replica and in the AOF for
            // good.
            Some(crate::commands::generics::apply_expire(
                ctx,
                key,
                new_abs,
                secs <= 0,
                &flags,
            ))
        }

        "SETEX" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'setex' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let expires_at = match key_deadline(
                cmd[2],
                TimeArg::Secs,
                ERR_INVALID_EXPIRE_SETEX_CMD,
                applying_stream(ctx),
            ) {
                Ok(t) => t,
                Err(e) => return Some(resp_error(e)),
            };
            let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
            let replaced =
                write_lock!(shard).set_str_expiring(key, cmd[3], Some(unix_ms(expires_at)));
            drop(replaced);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            Some(resp_simple("OK"))
        }

        "EXISTS" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'exists' command",
                ));
            }

            let mut count = 0i64;
            let db = &ctx.store[*ctx.selected_db];

            for k in cmd.iter().skip(1) {
                // The key as it stands in the command, whatever its bytes.
                let key: &[u8] = k;
                if is_expired(ctx.store, *ctx.selected_db, key) {
                    expire_on_read(ctx.store, *ctx.selected_db, key);
                    crate::stats::record_miss();
                    continue;
                }
                let shard = store::get_shard(db, key);
                let map = read_lock!(shard);

                if map.contains_key(key) {
                    crate::stats::record_hit();
                    count += 1;
                } else {
                    crate::stats::record_miss();
                }
            }

            Some(resp_integer(count))
        }

        "INCR" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'incr' command",
                ));
            }

            let db = &ctx.store[*ctx.selected_db];
            let key: &[u8] = cmd[1];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);

            let next = match map.get(key) {
                None => 1i64,
                Some(ValueRef::String(v)) => {
                    let s = match std::str::from_utf8(v.as_ref()) {
                        Ok(k) => k,
                        Err(_) => {
                            return Some(resp_error("ERR value is not an integer or out of range"));
                        }
                    };

                    match s.parse::<i64>() {
                        // At i64::MAX it wrapped to i64::MIN, or panicked in a
                        // debug build (#124).
                        Ok(n) => match n.checked_add(1) {
                            Some(n) => n,
                            None => {
                                return Some(resp_error(
                                    "ERR increment or decrement would overflow",
                                ));
                            }
                        },
                        Err(_) => {
                            return Some(resp_error("ERR value is not an integer or out of range"));
                        }
                    }
                }
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };

            // INCR must read then write so the lock scope is unavoidable.
            drop(map.set_str(key, decimal(next, &mut [0; 20])));
            drop(map); // explicitly drop lock before AOF
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", key);
            Some(resp_integer(next))
        }

        "STRLEN" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'strlen' command",
                ));
            }
            let key: &[u8] = cmd[1];
            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                crate::stats::record_miss();
                return Some(resp_integer(0));
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            match map.get(key) {
                Some(ValueRef::String(v)) => {
                    crate::stats::record_hit();
                    Some(resp_integer(v.len() as i64))
                }
                Some(_) => {
                    crate::stats::record_hit();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
            }
        }

        // ── APPEND ────────────────────────────────────────────────────────────────────
        // Appends bytes to the existing string, creating it if absent.
        // Returns the new length. Written where the string is kept when
        // nothing but its key holds it, a long one in a block with room to
        // spare for the next appends; else copied once (`append_str`).
        "APPEND" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'append' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let suffix = &cmd[2];
            if is_expired(ctx.store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, *ctx.selected_db, key);
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            // As Redis: a string does not grow past 512 MB.
            match map.append_str(key, suffix, MAX_STRING_LEN) {
                Ok((len, written)) => {
                    drop(map);
                    // The entry it replaced, if any, freed unlocked.
                    drop(written);
                    notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "append", key);
                    Some(resp_integer(len as i64))
                }
                Err(store::AppendRefused::TooLong) => Some(resp_error(
                    "ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                )),
                Err(store::AppendRefused::WrongType) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        // ── GETDEL ────────────────────────────────────────────────────────────────────
        // Atomically returns the value and then deletes the key.
        // Both happen under the same write lock — no TOCTOU window.
        "GETDEL" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'getdel' command",
                ));
            }
            let key: &[u8] = cmd[1];
            if is_expired(ctx.store, *ctx.selected_db, key) {
                // The key's time is up: nil as for one that was never
                // there. Its going still tells the watches — a key that
                // was alive when it was watched and runs out of time
                // afterwards does fail the `EXEC`, as in Redis
                // (`deleteExpiredKeyAndPropagate`) — while one already
                // marked expired holds. `unchanged` keeps the dispatcher,
                // which marks every key a successful write names, from
                // marking this one on top of that (#674).
                delete_key_if_expired(ctx.store, *ctx.selected_db, key);
                crate::watch::unchanged(*ctx.selected_db, key);
                return Some(resp_null_bulk());
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            // A key of another type is left as it is, with its time to live
            // and its LFU count (#221): looked at, not taken out and put back.
            match map.remove_if(key, |value| matches!(value, ValueRef::String(_))) {
                store::Removal::Taken(taken) => {
                    drop(map);
                    let ValueRef::String(v) = taken.value() else {
                        unreachable!()
                    };
                    crate::stats::record_hit();
                    notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "del", key);
                    Some(resp_bulk(&v))
                }
                store::Removal::Kept => {
                    drop(map);
                    crate::stats::record_hit();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
                store::Removal::Absent => {
                    crate::stats::record_miss();
                    // Nothing there to take: nothing was deleted, so a
                    // watch on the key holds (#674).
                    crate::watch::unchanged(*ctx.selected_db, key);
                    Some(resp_null_bulk())
                }
            }
        }

        // ── GETSET ────────────────────────────────────────────────────────────────────
        // Deprecated in Redis 6.2 but still widely used. Returns the old value, sets new.
        "GETSET" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'getset' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            // The reply is the old value, made before it is written over. A
            // key of another type is left as it is, as in Redis (it was
            // replaced, and the reply was the error).
            let reply = match map.get(key) {
                Some(ValueRef::String(v)) => {
                    crate::stats::record_hit();
                    resp_bulk(&v)
                }
                Some(_) => {
                    crate::stats::record_hit();
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
                None => {
                    crate::stats::record_miss();
                    resp_null_bulk()
                }
            };
            // GETSET always persists the key.
            let replaced = map.set_str_expiring(key, cmd[2], None);
            drop(map);
            drop(replaced);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            Some(reply)
        }

        // ── SETNX ─────────────────────────────────────────────────────────────────────
        // SET if Not eXists. Returns 1 if set, 0 if key already existed.
        "SETNX" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'setnx' command",
                ));
            }
            let key: &[u8] = cmd[1];
            // Check expiry first — an expired key counts as non-existent
            if is_expired(ctx.store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, *ctx.selected_db, key);
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            if map.contains_key(key) {
                return Some(resp_integer(0));
            }
            drop(map.set_str(key, cmd[2]));
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            Some(resp_integer(1))
        }

        // ── PSETEX ────────────────────────────────────────────────────────────────────
        // Like SETEX but the TTL is in milliseconds.
        "PSETEX" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'psetex' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let expires_at = match key_deadline(
                cmd[2],
                TimeArg::Millis,
                ERR_INVALID_EXPIRE_PSETEX_CMD,
                applying_stream(ctx),
            ) {
                Ok(t) => t,
                Err(e) => return Some(resp_error(e)),
            };
            let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
            let replaced =
                write_lock!(shard).set_str_expiring(key, cmd[3], Some(unix_ms(expires_at)));
            drop(replaced);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            Some(resp_simple("OK"))
        }

        // ── MSET ──────────────────────────────────────────────────────────────────────
        // Atomically set N key-value pairs. Always succeeds. Returns OK.
        // One step: every shard of its keys is held at once while they are
        // written (`WriteShards`), so no reader finds some of them set and
        // the others not yet.
        "MSET" => {
            // needs key value key value ... — odd total args, even pairs after cmd name
            if cmd.len() < 3 || cmd.len().is_multiple_of(2) {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'mset' command",
                ));
            }
            // All of it or none: a key that cannot be one is found before
            // anything is written. (It used to be passed over, and the rest
            // written, with `OK` for an answer.)
            let pairs = key_value_pairs(cmd);
            let db = *ctx.selected_db;
            {
                // Every shard the keys are in, locked together: nobody sees
                // some of the keys set and the others not yet.
                let mut shards = WriteShards::lock(&ctx.store[db], pairs.iter().map(|p| p.0));
                for (key, value) in &pairs {
                    drop(shards.map(key).set_str_expiring(*key, *value, None));
                }
            }
            for (key, _) in &pairs {
                notify(ctx.pubsub, db, KS_STRING, "set", key);
            }
            Some(resp_simple("OK"))
        }

        // ── MGET ──────────────────────────────────────────────────────────────────────
        // Returns values for N keys. Missing or wrong-type keys return null bulk.
        // Each key hits its own shard independently — no cross-shard lock held.
        "MGET" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'mget' command",
                ));
            }
            // MGET's nils sit inside the array, so `resp3_normalize_null` — which
            // only rewrites a reply that is *entirely* a top-level null — never
            // sees them. Under RESP3 they must be the null type, or a strict
            // client reads `$-1` as a literal value instead of a miss.
            let resp = crate::commands::handler::effective_resp_for_reply(ctx);
            let nil = resp_null_for(resp);
            let db = &ctx.store[*ctx.selected_db];
            let mut results: Vec<Vec<u8>> = Vec::with_capacity(cmd.len() - 1);
            // Every shard the keys are in, locked together: the values are
            // those of one moment, not of as many moments as there are keys.
            let shards = ReadShards::lock(db, cmd[1..].iter().copied());
            for k in cmd[1..].iter() {
                let key: &[u8] = k;
                // A key past its time is not there. It is left for the next
                // command on it, or the sweep, to remove: that takes its
                // shard for writing, which this holds for reading.
                let val = match shards.map(key).map(|map| map.lookup(key)) {
                    Some(store::Lookup::Live(ValueRef::String(v))) => {
                        crate::stats::record_hit();
                        resp_bulk(v.as_bytes())
                    }
                    Some(store::Lookup::Live(_)) => {
                        crate::stats::record_hit();
                        nil.clone()
                    }
                    _ => {
                        crate::stats::record_miss();
                        nil.clone()
                    }
                };
                results.push(val);
            }
            Some(resp_array(results))
        }

        // ── MSETNX ────────────────────────────────────────────────────────────────────
        // Set all pairs only if NONE of the keys currently exist.
        // Returns 1 if all were set, 0 if any existed (and nothing is written).
        "MSETNX" => {
            if cmd.len() < 3 || cmd.len().is_multiple_of(2) {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'msetnx' command",
                ));
            }
            let pairs = key_value_pairs(cmd);
            let db = *ctx.selected_db;
            {
                // Looked at and written under the same locks: no key can
                // come to be between the looking and the writing. (They
                // were two passes, each key under a lock of its own, and
                // two clients could both be told that they had set a key.)
                let mut shards = WriteShards::lock(&ctx.store[db], pairs.iter().map(|p| p.0));
                for (key, _) in &pairs {
                    // A key past its time is not there, and is written over.
                    let map = shards.map(key);
                    if map.contains_key(key) && !map.expires(key).is_some_and(is_past) {
                        return Some(resp_integer(0));
                    }
                }
                for (key, value) in &pairs {
                    drop(shards.map(key).set_str_expiring(*key, *value, None));
                }
            }
            for (key, _) in &pairs {
                notify(ctx.pubsub, db, KS_STRING, "set", key);
            }
            Some(resp_integer(1))
        }

        // ── INCRBY ────────────────────────────────────────────────────────────────────
        // Increment by an arbitrary i64. Same lock pattern as INCR.
        "INCRBY" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'incrby' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let delta: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            // Check overflow properly
            let next = match map.get(key) {
                None => delta,
                Some(ValueRef::String(v)) => match std::str::from_utf8(&v)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) => match n.checked_add(delta) {
                        Some(result) => result,
                        None => {
                            return Some(resp_error("ERR increment or decrement would overflow"));
                        }
                    },
                    None => return Some(resp_error("ERR value is not an integer or out of range")),
                },
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            drop(map.set_str(key, decimal(next, &mut [0; 20])));
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", key);
            Some(resp_integer(next))
        }

        // ── DECR / DECRBY ─────────────────────────────────────────────────────────────
        // Mirror of INCR/INCRBY but subtracts. We reuse the same pattern.
        "DECR" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'decr' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            let next = match map.get(key) {
                None => -1i64,
                Some(ValueRef::String(v)) => match std::str::from_utf8(&v)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) => match n.checked_sub(1) {
                        Some(r) => r,
                        None => {
                            return Some(resp_error("ERR increment or decrement would overflow"));
                        }
                    },
                    None => return Some(resp_error("ERR value is not an integer or out of range")),
                },
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            drop(map.set_str(key, decimal(next, &mut [0; 20])));
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", key);
            Some(resp_integer(next))
        }

        "DECRBY" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'decrby' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let delta: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            let next = match map.get(key) {
                None => -delta,
                Some(ValueRef::String(v)) => match std::str::from_utf8(&v)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) => match n.checked_sub(delta) {
                        Some(r) => r,
                        None => {
                            return Some(resp_error("ERR increment or decrement would overflow"));
                        }
                    },
                    None => return Some(resp_error("ERR value is not an integer or out of range")),
                },
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            drop(map.set_str(key, decimal(next, &mut [0; 20])));
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", key);
            Some(resp_integer(next))
        }

        // ── INCRBYFLOAT ───────────────────────────────────────────────────────────────
        // Redis stores the result as a string. It trims trailing zeros: "3.10" → "3.1".
        // f64 arithmetic is used — same as Redis.
        "INCRBYFLOAT" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'incrbyfloat' command",
                ));
            }
            let key: &[u8] = cmd[1];
            use crate::commands::long_double::{self, Increment, LongDouble};
            const NOT_A_FLOAT: &str = "ERR value is not a valid float";
            // Read before the shard is locked, and refused only after the
            // stored value is: the key's type first, then its value, then
            // the increment, as in Redis.
            let delta = LongDouble::parse(cmd[2]);
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            // In `long double`, as Redis on x86-64 (#387).
            let current = match map.get(key) {
                None => long_double::ZERO,
                Some(ValueRef::String(v)) => match LongDouble::parse(&v) {
                    Some(n) => n,
                    None => return Some(resp_error(NOT_A_FLOAT)),
                },
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            let Some(delta) = delta else {
                return Some(resp_error(NOT_A_FLOAT));
            };
            let s = match long_double::increment(current, delta) {
                Increment::Sum(s) => s,
                Increment::NotFinite => {
                    return Some(resp_error("ERR increment would produce NaN or Infinity"));
                }
            };
            drop(map.set_str(key, s.as_bytes()));
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrbyfloat", key);
            Some(resp_bulk_str(&s))
        }

        // ── INCREX (Redis 8.8) ────────────────────────────────────────────────────────
        //   INCREX key [BYFLOAT increment | BYINT increment]
        //             [LBOUND lower] [UBOUND upper] [SATURATE]
        //             [EX s | PX ms | EXAT unix-s | PXAT unix-ms | PERSIST] [ENX]
        //
        // Increment plus bounds plus expiry, atomically. Replies with a
        // two-element array: [new_value, actual_increment]. `actual_increment`
        // is 0 when a bound violation caused the operation to be skipped, which
        // is what makes the rate-limiter pattern work without a second read.
        "INCREX" => Some(cmd_increx(ctx)),

        // ── GETRANGE / SUBSTR ─────────────────────────────────────────────────────────
        // Returns a byte substring using Redis's inclusive negative-index semantics.
        // SUBSTR is an old alias for GETRANGE.
        "GETRANGE" | "SUBSTR" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'getrange' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let start: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let end: i64 = match parse_utf8(cmd[3]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                crate::stats::record_miss();
                return Some(resp_bulk(b""));
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_bulk(b""))
                }
                Some(ValueRef::String(_)) if start < 0 && end < 0 && start > end => {
                    crate::stats::record_hit();
                    Some(resp_bulk(b""))
                }
                Some(ValueRef::String(v)) => {
                    let len = v.len() as i64;
                    // Convert negative indices: -1 = last byte
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    } as usize;
                    let e = if end < 0 {
                        (len + end).max(0)
                    } else {
                        end.min(len - 1)
                    } as usize;
                    crate::stats::record_hit();
                    if s > e || len == 0 {
                        Some(resp_bulk(b""))
                    } else {
                        Some(resp_bulk(&v[s..=e]))
                    }
                }
                Some(_) => {
                    crate::stats::record_hit();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }

        // ── SETRANGE ──────────────────────────────────────────────────────────────────
        // Overwrites bytes at a given offset, zero-padding if needed.
        // Creates the key if it doesn't exist (treats it as empty string).
        // Written where the string is kept when nothing else holds it and its
        // block has room; otherwise copied once (`edit_str_in_place`).
        "SETRANGE" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'setrange' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let offset: usize = match parse_utf8(cmd[2]).and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if n >= 0 => n as usize,
                Some(_) => return Some(resp_error("ERR offset is out of range")),
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let patch = &cmd[3];
            // A string is at most 512 MB (Redis's `proto-max-bulk-len`). An
            // offset beyond that used to be tried: the allocation failed and
            // took the connection with it.
            if !patch.is_empty()
                && offset
                    .checked_add(patch.len())
                    .is_none_or(|end| end > MAX_STRING_LEN)
            {
                return Some(resp_error(
                    "ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                ));
            }
            if is_expired(ctx.store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, *ctx.selected_db, key);
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            let len = match map.get(key) {
                None => 0,
                Some(ValueRef::String(v)) => v.len(),
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            // Nothing to write: the string is as it was, and a key that was
            // not there is not created. Checked here because the helper
            // grows the string to `offset` even for an empty patch.
            if patch.is_empty() {
                return Some(resp_integer(len as i64));
            }
            let Some(written) = map.edit_str_in_place(key, offset, patch) else {
                // Not reached: the key was a string or absent under this
                // lock, and the length was checked above.
                return Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            };
            drop(map);
            // A copied string's old entry is freed with the shard unlocked.
            drop(written);
            Some(resp_integer(len.max(offset + patch.len()) as i64))
        }

        // ── GETEX ─────────────────────────────────────────────────────────────────────
        // GET + optionally modify expiry. Options: EX secs, PX millis, EXAT unix-secs,
        // PXAT unix-millis, PERSIST (remove TTL). Returns the value or null.
        "GETEX" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'getex' command",
                ));
            }
            let key: &[u8] = cmd[1];
            // The options first, as Redis's `getexCommand` reads them: one
            // of EX, PX, EXAT, PXAT and PERSIST (the same one again
            // replaces it), anything else a syntax error. Only the first was
            // read: `GETEX k EX 10 PX 5` was taken. The time itself is read
            // once the key is found, as there.
            let mut expire: Option<(TimeArg, &[u8])> = None;
            let mut persist = false;
            let mut j = 2;
            while j < cmd.len() {
                match TimeArg::of_option(cmd[j]) {
                    Some(arg)
                        if !persist
                            && expire.is_none_or(|(unit, _)| unit == arg)
                            && j + 1 < cmd.len() =>
                    {
                        expire = Some((arg, cmd[j + 1]));
                        j += 1;
                    }
                    None if cmd[j].eq_ignore_ascii_case(b"PERSIST") && expire.is_none() => {
                        persist = true;
                    }
                    _ => return Some(resp_error("ERR syntax error")),
                }
                j += 1;
            }
            if is_expired(ctx.store, *ctx.selected_db, key) {
                // The key's time is up: nil as for one that was never
                // there. Its going still tells the watches — a key that
                // was alive when it was watched and runs out of time
                // afterwards does fail the `EXEC`, as in Redis
                // (`deleteExpiredKeyAndPropagate`) — while one already
                // marked expired holds. `unchanged` keeps the dispatcher,
                // which marks every key a successful write names, from
                // marking this one on top of that (#674).
                delete_key_if_expired(ctx.store, *ctx.selected_db, key);
                crate::watch::unchanged(*ctx.selected_db, key);
                return Some(resp_null_bulk());
            }

            // Read the value first — short read lock, then drop before touching expiry
            let val = {
                let db = &ctx.store[*ctx.selected_db];
                let shard = store::get_shard(db, key);
                let map = read_lock!(shard);
                match map.get(key) {
                    Some(ValueRef::String(v)) => {
                        crate::stats::record_hit();
                        Some(v.to_shared())
                    }
                    Some(_) => {
                        crate::stats::record_hit();
                        return Some(resp_error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ));
                    }
                    None => {
                        crate::stats::record_miss();
                        None
                    }
                }
            };

            // The time, for a key that is there: a missing one is nil
            // whatever the time, as in Redis.
            let deadline = match expire {
                Some((arg, raw)) if val.is_some() => {
                    match key_deadline(raw, arg, ERR_INVALID_EXPIRE_GETEX_CMD, applying_stream(ctx))
                    {
                        Ok(at) => Some(at),
                        Err(e) => return Some(resp_error(e)),
                    }
                }
                _ => None,
            };
            // Apply it, and say so as Redis does: a time that has already
            // come deletes the key now (`del`), where it was kept to expire
            // later, and `persist` only when there was a time to take away.
            // From a stream the time is set as it is (`expiry::held`).
            if val.is_some() {
                let db = *ctx.selected_db;
                match deadline {
                    Some(at)
                        if at <= crate::script_clock::wall_system_time_now()
                            && !applying_stream(ctx) =>
                    {
                        delete_key(ctx.store, db, key);
                        notify(ctx.pubsub, db, KS_GENERIC, "del", key);
                    }
                    Some(at) => {
                        set_expiry(ctx.store, db, key, at);
                        notify(ctx.pubsub, db, KS_GENERIC, "expire", key);
                    }
                    None if persist && clear_expiry(ctx.store, db, key) => {
                        notify(ctx.pubsub, db, KS_GENERIC, "persist", key);
                    }
                    None => {}
                }
            }
            match val {
                Some(v) => Some(resp_bulk(&v)),
                None => {
                    // Not there: nothing was written, so a watch on the
                    // key holds (#674).
                    crate::watch::unchanged(*ctx.selected_db, key);
                    Some(resp_null_bulk())
                }
            }
        }

        // ── LCS ───────────────────────────────────────────────────────────────────────
        // Finds the Longest Common Substring (Redis calls it "subsequence" in docs but
        // the actual Redis behaviour is subsequence, not substring — we match that).
        // Syntax: LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]
        // Complexity: O(N*M) — only runs on explicit call, never on the hot path.
        "LCS" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'lcs' command",
                ));
            }
            let key1: &[u8] = cmd[1];
            let key2: &[u8] = cmd[2];

            // Parse options
            let mut want_len = false;
            let mut want_idx = false;
            let mut min_match_len: usize = 0;
            let mut with_match_len = false;

            let mut i = 3;
            while i < cmd.len() {
                match std::str::from_utf8(cmd[i])
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "LEN" => {
                        want_len = true;
                    }
                    "IDX" => {
                        want_idx = true;
                    }
                    "WITHMATCHLEN" => {
                        with_match_len = true;
                    }
                    "MINMATCHLEN" => {
                        i += 1;
                        min_match_len = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return Some(resp_error("ERR MINMATCHLEN requires an integer")),
                        };
                    }
                    _ => {}
                }
                i += 1;
            }

            // Fetch both values under their respective shard read locks.
            // We read key1 first, drop lock, then read key2. This avoids holding
            // two shard locks simultaneously (deadlock risk if same shard).
            // Both keys are looked up (and counted) before either's type
            // is refused, as Redis does.
            let mut wrong_type = false;
            let s1: SharedBytes = {
                if is_expired(ctx.store, *ctx.selected_db, key1) {
                    expire_on_read(ctx.store, *ctx.selected_db, key1);
                    crate::stats::record_miss();
                    SharedBytes::from(&[][..])
                } else {
                    let db = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db, key1);
                    let map = read_lock!(shard);
                    let found = map.get(key1);
                    crate::stats::record_lookup(found.is_some());
                    match found {
                        Some(ValueRef::String(v)) => v.to_shared(),
                        Some(_) => {
                            wrong_type = true;
                            SharedBytes::from(&[][..])
                        }
                        None => SharedBytes::from(&[][..]),
                    }
                }
            };

            let s2: SharedBytes = {
                if is_expired(ctx.store, *ctx.selected_db, key2) {
                    expire_on_read(ctx.store, *ctx.selected_db, key2);
                    crate::stats::record_miss();
                    SharedBytes::from(&[][..])
                } else {
                    let db = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db, key2);
                    let map = read_lock!(shard);
                    let found = map.get(key2);
                    crate::stats::record_lookup(found.is_some());
                    match found {
                        Some(ValueRef::String(v)) => v.to_shared(),
                        Some(_) => {
                            wrong_type = true;
                            SharedBytes::from(&[][..])
                        }
                        None => SharedBytes::from(&[][..]),
                    }
                }
            };
            if wrong_type {
                return Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }

            // ── LCS DP core ──────────────────────────────────────────────────────────
            // Standard O(N*M) DP. We use a flat Vec<u32> as the DP table to avoid
            // per-row heap allocations. Two rows suffice for LCS length, but for IDX
            // mode we need the full table to backtrack.
            let n = s1.len();
            let m = s2.len();

            if want_idx {
                // Full table needed for backtracking — allocates O(N*M) u32s.
                // Cap to prevent OOM on absurdly large inputs: 512×512 = 256KB.
                // Redis itself has no guard; we add a pragmatic one.
                if n > 0 && m > 0 && n.saturating_mul(m) > 512 * 512 {
                    return Some(resp_error("ERR LCS IDX inputs too large (>512 bytes each)"));
                }
                let mut dp = vec![0u32; (n + 1) * (m + 1)];
                for i in 1..=n {
                    for j in 1..=m {
                        dp[i * (m + 1) + j] = if s1[i - 1] == s2[j - 1] {
                            dp[(i - 1) * (m + 1) + (j - 1)] + 1
                        } else {
                            dp[(i - 1) * (m + 1) + j].max(dp[i * (m + 1) + (j - 1)])
                        };
                    }
                }

                // Backtrack to collect matches
                struct Match {
                    s1_start: usize,
                    s1_end: usize,
                    s2_start: usize,
                    s2_end: usize,
                    len: usize,
                }
                let mut matches: Vec<Match> = Vec::new();
                let mut i = n;
                let mut j = m;
                while i > 0 && j > 0 {
                    if s1[i - 1] == s2[j - 1] {
                        // Walk back to find the extent of this run
                        let mut run = 0usize;
                        while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] {
                            run += 1;
                            i -= 1;
                            j -= 1;
                        }
                        if run >= min_match_len.max(1) {
                            matches.push(Match {
                                s1_start: i,
                                s1_end: i + run - 1,
                                s2_start: j,
                                s2_end: j + run - 1,
                                len: run,
                            });
                        }
                    } else if dp[(i - 1) * (m + 1) + j] >= dp[i * (m + 1) + (j - 1)] {
                        i -= 1;
                    } else {
                        j -= 1;
                    }
                }
                // Redis returns matches in descending order (end of string first)
                // They are already in reverse since we backtracked; no sort needed.

                let lcs_len = dp[n * (m + 1) + m] as i64;

                // Build RESP array: ["matches", [[s1range, s2range, (optlen)], ...], "len", N]
                let mut match_items: Vec<Vec<u8>> = Vec::with_capacity(matches.len());
                for m_entry in &matches {
                    let s1_range = resp_array(vec![
                        resp_integer(m_entry.s1_start as i64),
                        resp_integer(m_entry.s1_end as i64),
                    ]);
                    let s2_range = resp_array(vec![
                        resp_integer(m_entry.s2_start as i64),
                        resp_integer(m_entry.s2_end as i64),
                    ]);
                    let mut triple = vec![s1_range, s2_range];
                    if with_match_len {
                        triple.push(resp_integer(m_entry.len as i64));
                    }
                    match_items.push(resp_array(triple));
                }

                return Some(resp_array(vec![
                    resp_bulk_str("matches"),
                    resp_array(match_items),
                    resp_bulk_str("len"),
                    resp_integer(lcs_len),
                ]));
            }

            // ── LEN-only or plain LCS (two-row DP — O(M) space) ──────────────────────
            let mut prev = vec![0u32; m + 1];
            let mut curr = vec![0u32; m + 1];
            for i in 1..=n {
                for j in 1..=m {
                    curr[j] = if s1[i - 1] == s2[j - 1] {
                        prev[j - 1] + 1
                    } else {
                        curr[j - 1].max(prev[j])
                    };
                }
                std::mem::swap(&mut prev, &mut curr);
                // reset curr for next row
                curr.iter_mut().for_each(|x| *x = 0);
            }
            let lcs_len = prev[m] as i64;

            if want_len {
                return Some(resp_integer(lcs_len));
            }

            // Plain mode: return the actual LCS string.
            // We need a full backtrack here too, so re-run with full table
            // only when the string is actually requested and not IDX.
            // For simplicity, reconstruct by re-running full DP (strings are
            // already fetched; the re-run cost is the same O(N*M) anyway).
            if n == 0 || m == 0 {
                return Some(resp_bulk(b""));
            }
            let mut dp2 = vec![0u32; (n + 1) * (m + 1)];
            for i in 1..=n {
                for j in 1..=m {
                    dp2[i * (m + 1) + j] = if s1[i - 1] == s2[j - 1] {
                        dp2[(i - 1) * (m + 1) + (j - 1)] + 1
                    } else {
                        dp2[(i - 1) * (m + 1) + j].max(dp2[i * (m + 1) + (j - 1)])
                    };
                }
            }
            // Reconstruct LCS string by backtracking
            let mut result: Vec<u8> = Vec::with_capacity(lcs_len as usize);
            let mut i = n;
            let mut j = m;
            while i > 0 && j > 0 {
                if s1[i - 1] == s2[j - 1] {
                    result.push(s1[i - 1]);
                    i -= 1;
                    j -= 1;
                } else if dp2[(i - 1) * (m + 1) + j] >= dp2[i * (m + 1) + (j - 1)] {
                    i -= 1;
                } else {
                    j -= 1;
                }
            }
            result.reverse();
            Some(resp_bulk(&result))
        }

        // ── DIGEST ────────────────────────────────────────────────────────────────────
        // Returns the SHA-256 hex digest of the string value stored at key.
        // Redis 8.4+. Returns nil for missing keys, error for wrong type.
        // We use a pure-Rust SHA-256 so no external crate is required — just
        // implement the standard algorithm inline for the 64-byte block case.
        // If you already have sha2 in Cargo.toml, use that instead.
        "DIGEST" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'digest' command",
                ));
            }
            let key: &[u8] = cmd[1];
            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                crate::stats::record_miss();
                return Some(resp_null_bulk());
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            let found = map.get(key);
            crate::stats::record_lookup(found.is_some());
            match found {
                None => Some(resp_null_bulk()),
                Some(ValueRef::String(v)) => {
                    let hex = value_digest(&v);
                    Some(resp_bulk_str(&hex))
                }
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        // ── DELEX ────────────────────────────────────────────────────────────────────
        // Conditionally delete based on value comparison or digest comparison.
        // Syntax: DELEX key [IFEQ val | IFNE val | IFDEQ digest | IFDNE digest]
        // Returns: 1 if deleted, 0 if condition not met, nil if key missing.
        // Redis 8.4+.
        "DELEX" => {
            // `DELEX key` alone is valid: with no condition it deletes the key
            // whatever its type. It used to be an arity error.
            if cmd.len() != 2 && cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'delex' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let db_idx = *ctx.selected_db;

            // An expired key does not exist; the reply is 0, not nil.
            if is_expired(ctx.store, db_idx, key) {
                delete_key_if_expired(ctx.store, db_idx, key);
                return Some(resp_integer(0));
            }

            // The comparison and the delete happen under one lock. They used
            // to be a read, an unlock and a separate delete, so a concurrent
            // SET between the two was deleted even though the condition had
            // been checked against the value it replaced — the exact race a
            // compare-and-delete exists to prevent.
            let shard = store::get_shard(&ctx.store[db_idx], key);
            let mut map = write_lock!(shard);
            let should_delete = match map.get(key) {
                None => return Some(resp_integer(0)),
                Some(_) if cmd.len() == 2 => true,
                Some(ValueRef::String(stored)) => {
                    let arg = cmd[3];
                    let digest = cmd[2].eq_ignore_ascii_case(b"IFDEQ")
                        || cmd[2].eq_ignore_ascii_case(b"IFDNE");
                    if digest && arg.len() != 16 {
                        return Some(resp_error("ERR must be exactly 16 hexadecimal characters"));
                    }
                    if cmd[2].eq_ignore_ascii_case(b"IFEQ") {
                        stored.as_ref() == arg
                    } else if cmd[2].eq_ignore_ascii_case(b"IFNE") {
                        stored.as_ref() != arg
                    } else if cmd[2].eq_ignore_ascii_case(b"IFDEQ") {
                        digest_matches(&stored, arg)
                    } else if cmd[2].eq_ignore_ascii_case(b"IFDNE") {
                        !digest_matches(&stored, arg)
                    } else {
                        return Some(resp_error(
                            "ERR Invalid condition. Use IFEQ, IFNE, IFDEQ, or IFDNE",
                        ));
                    }
                }
                Some(_) => {
                    return Some(resp_error(
                        "ERR Key should be of string type if conditions are specified",
                    ));
                }
            };
            if !should_delete {
                return Some(resp_integer(0));
            }
            let removed = map.remove(key);
            drop(map);
            drop(removed);
            Some(resp_integer(1))
        }

        // ── MSETEX ───────────────────────────────────────────────────────────────────
        // Atomically sets N key-value pairs with a shared expiry.
        // Syntax: MSETEX numkeys k v [k v ...] [NX|XX] [EX s|PX ms|EXAT ts|PXAT ts|KEEPTTL]
        // Redis 8.4+. The numkeys argument tells us how many k/v pairs follow.
        "MSETEX" => {
            if cmd.len() < 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'msetex' command",
                ));
            }
            let numkeys: usize = match parse_utf8(cmd[1]).and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if (1..=i32::MAX as i64).contains(&n) => n as usize,
                _ => return Some(resp_error("ERR invalid numkeys value")),
            };
            // pairs occupy cmd[2..2+numkeys*2]
            let pairs_end = numkeys.saturating_mul(2).saturating_add(2);
            if cmd.len() < pairs_end {
                return Some(resp_error("ERR wrong number of key-value pairs"));
            }
            // NX or XX, and KEEPTTL or one unit of EX, PX, EXAT and PXAT,
            // anything else a syntax error, as Redis parses them. The same
            // option again is taken, and a time given again replaces the
            // first; only the last is read.
            let mut nx = false;
            let mut xx = false;
            let mut keepttl = false;
            let mut expire: Option<(TimeArg, &[u8])> = None;
            let mut i = pairs_end;
            while i < cmd.len() {
                let opt = cmd[i];
                let arg = TimeArg::of_option(opt);
                if opt.eq_ignore_ascii_case(b"NX") && !xx {
                    nx = true;
                } else if opt.eq_ignore_ascii_case(b"XX") && !nx {
                    xx = true;
                } else if opt.eq_ignore_ascii_case(b"KEEPTTL") && expire.is_none() {
                    keepttl = true;
                } else if let Some(arg) = arg
                    && !keepttl
                    && i + 1 < cmd.len()
                    && (expire.is_none_or(|(unit, _)| unit == arg) || applying_stream(ctx))
                {
                    expire = Some((arg, cmd[i + 1]));
                    i += 1;
                } else {
                    return Some(resp_error("ERR syntax error"));
                }
                i += 1;
            }
            let expiry: Option<SystemTime> = match expire {
                Some((arg, raw)) => match key_deadline(
                    raw,
                    arg,
                    ERR_INVALID_EXPIRE_MSETEX_CMD,
                    applying_stream(ctx),
                ) {
                    Ok(t) => Some(t),
                    Err(e) => return Some(resp_error(e)),
                },
                None => None,
            };

            if nx && xx {
                return Some(resp_error("ERR NX and XX options are mutually exclusive"));
            }

            // NX: all keys must be absent; XX: all keys must be present
            // Check first without writing (read pass), then write.
            let pairs = &cmd[2..pairs_end];

            if nx || xx {
                for pair in pairs.as_chunks::<2>().0 {
                    let key: &[u8] = pair[0];
                    let exists = if is_expired(ctx.store, *ctx.selected_db, key) {
                        delete_key_if_expired(ctx.store, *ctx.selected_db, key);
                        false
                    } else {
                        let db = &ctx.store[*ctx.selected_db];
                        let shard = store::get_shard(db, key);
                        read_lock!(shard).contains_key(key)
                    };
                    if nx && exists {
                        return Some(resp_integer(0));
                    }
                    if xx && !exists {
                        return Some(resp_integer(0));
                    }
                }
            }

            // Write pass
            for pair in pairs.as_chunks::<2>().0 {
                let key: &[u8] = pair[0];
                let value = KiviDBValue::String(SharedBytes::from(pair[1]));

                if keepttl {
                    // Just overwrite value, leave existing expiry intact
                    let db = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db, key);
                    let mut map = write_lock!(shard);
                    map.insert(key, value);
                } else {
                    store_value_expiring(ctx.store, *ctx.selected_db, key, value, expiry);
                }
            }
            Some(resp_integer(1))
        }

        _ => None, // ← tells the router this module doesn't handle it
    }
}

/// The hash digest of a string value, as `DIGEST` reports it and as
/// `SET … IFDEQ/IFDNE` and `DELEX … IFDEQ/IFDNE` compare against: the 64-bit
/// XXH3 hash, as 16 lowercase hex digits.
///
/// This used to be SHA-256. The digest is not an opaque token: its point is
/// that a client can compute it locally from a value it already holds and
/// send it instead of the value, so it has to be the hash Redis documents.
/// A client doing that against a SHA-256 server never matched.
fn value_digest(data: &[u8]) -> String {
    format!("{:016x}", xxhash_rust::xxh3::xxh3_64(data))
}

/// Whether `given` (hex, any case) is the digest of `value`.
fn digest_matches(value: &[u8], given: &[u8]) -> bool {
    std::str::from_utf8(given).is_ok_and(|given| value_digest(value).eq_ignore_ascii_case(given))
}

#[cfg(test)]
mod digest_tests {
    use super::*;

    /// The example on the Redis `DIGEST` command page.
    #[test]
    fn digest_is_xxh3_as_documented() {
        assert_eq!(value_digest(b"Hello world"), "b6acb9d84a38ff74");
        assert!(digest_matches(b"Hello world", b"B6ACB9D84A38FF74"));
        assert!(!digest_matches(b"Hello world!", b"b6acb9d84a38ff74"));
    }
}

// ── INCREX ───────────────────────────────────────────────────────────────────

/// Which numeric domain `INCREX` is operating in.
///
/// `BYINT` (and the no-option default) requires the stored value to parse as an
/// integer — a stored `"1.5"` is an error, matching INCR/INCRBY. `BYFLOAT`
/// accepts either, since integers promote to float losslessly.
#[derive(PartialEq, Clone, Copy)]
enum IncrexMode {
    Int,
    Float,
}

/// How the expiry options should be applied once the increment lands.
enum IncrexExpiry {
    Keep,
    Set(SystemTime),
    Persist,
}

/// `INCREX key [BYFLOAT n | BYINT n] [LBOUND l] [UBOUND u] [SATURATE]
///            [EX s | PX ms | EXAT s | PXAT ms | PERSIST] [ENX]`
fn cmd_increx(ctx: &mut Ctx) -> Vec<u8> {
    use crate::replication::script_feed::{effect, effects_begin};
    let cmd = &ctx.cmd;
    if cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'increx' command");
    }
    let key: Vec<u8> = cmd[1].to_vec();

    // The options as Redis 8.10's `parseIncrExArgumentsOrReply` takes them:
    // each at most once, BYINT or BYFLOAT, one expiration or PERSIST, ENX
    // not with PERSIST; anything else is a syntax error. The bounds are read
    // after, in the type BYINT or BYFLOAT chose.
    let mut mode = IncrexMode::Int;
    let mut mode_set = false;
    let mut incr_int: i64 = 1;
    let mut incr_float: f64 = 0.0;
    let mut lbound_raw: Option<&[u8]> = None;
    let mut ubound_raw: Option<&[u8]> = None;
    let mut saturate = false;
    let mut expire: Option<(&str, &[u8])> = None;
    let mut persist = false;
    let mut enx = false;
    let syntax = || resp_error("ERR syntax error");

    let mut i = 2usize;
    while i < cmd.len() {
        let tok = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        let next = cmd.get(i + 1).copied();
        match (tok.as_str(), next) {
            ("BYINT", Some(raw)) if !mode_set => {
                incr_int = match parse_utf8(raw).and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return resp_error("ERR Increment is not an integer or out of range"),
                };
                mode_set = true;
                i += 2;
            }
            ("BYFLOAT", Some(raw)) if !mode_set => {
                incr_float = match parse_utf8(raw).and_then(|s| s.parse::<f64>().ok()) {
                    Some(v) if !v.is_nan() => v,
                    _ => return resp_error("ERR Increment is not a valid float"),
                };
                if incr_float.is_infinite() {
                    return resp_error("ERR BYFLOAT increment cannot be Infinity");
                }
                mode = IncrexMode::Float;
                mode_set = true;
                i += 2;
            }
            ("LBOUND", Some(raw)) if lbound_raw.is_none() => {
                lbound_raw = Some(raw);
                i += 2;
            }
            ("UBOUND", Some(raw)) if ubound_raw.is_none() => {
                ubound_raw = Some(raw);
                i += 2;
            }
            ("SATURATE", _) if !saturate => {
                saturate = true;
                i += 1;
            }
            ("ENX", _) if !enx && !persist => {
                enx = true;
                i += 1;
            }
            ("PERSIST", _) if expire.is_none() && !persist && !enx => {
                persist = true;
                i += 1;
            }
            ("EX" | "PX" | "EXAT" | "PXAT", Some(raw)) if expire.is_none() && !persist => {
                expire = Some((
                    match tok.as_str() {
                        "EX" => "EX",
                        "PX" => "PX",
                        "EXAT" => "EXAT",
                        _ => "PXAT",
                    },
                    raw,
                ));
                i += 2;
            }
            _ => return syntax(),
        }
    }

    let (lbound_i, ubound_i, lbound_f, ubound_f) = match mode {
        IncrexMode::Int => {
            let bound = |raw: Option<&[u8]>, default: i64, what: &str| match raw {
                None => Ok(default),
                Some(raw) => parse_utf8(raw)
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or_else(|| {
                        resp_error(&format!("ERR {what} is not an integer or out of range"))
                    }),
            };
            let l = match bound(lbound_raw, i64::MIN, "LBOUND") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let u = match bound(ubound_raw, i64::MAX, "UBOUND") {
                Ok(v) => v,
                Err(e) => return e,
            };
            (l, u, 0.0, 0.0)
        }
        IncrexMode::Float => {
            let bound = |raw: Option<&[u8]>, default: f64, what: &str| match raw {
                None => Ok(default),
                Some(raw) => parse_utf8(raw)
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|v| !v.is_nan())
                    .ok_or_else(|| resp_error(&format!("ERR {what} is not a valid float"))),
            };
            let l = match bound(lbound_raw, f64::MIN, "LBOUND") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let u = match bound(ubound_raw, f64::MAX, "UBOUND") {
                Ok(v) => v,
                Err(e) => return e,
            };
            (0, 0, l, u)
        }
    };
    let bounds_inverted = match mode {
        IncrexMode::Int => lbound_i > ubound_i,
        IncrexMode::Float => lbound_f > ubound_f,
    };
    if bounds_inverted {
        return resp_error("ERR LBOUND can't be greater than UBOUND");
    }
    if enx && expire.is_none() {
        return resp_error("ERR ENX flag requires an expiration");
    }
    let expiry = match expire {
        None if persist => IncrexExpiry::Persist,
        None => IncrexExpiry::Keep,
        Some((unit, raw)) => {
            let n: i64 = match parse_utf8(raw).and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => return resp_error(ERR_VALUE_NOT_INTEGER),
            };
            if n <= 0 || (matches!(unit, "EX" | "EXAT") && n > i64::MAX / 1000) {
                return resp_error(ERR_INVALID_EXPIRE_INCREX_CMD);
            }
            let arg = match unit {
                "EX" => TimeArg::Secs,
                "PX" => TimeArg::Millis,
                "EXAT" => TimeArg::UnixSecs,
                _ => TimeArg::UnixMillis,
            };
            let at = deadline_ms_of(n as i128, arg, ERR_INVALID_EXPIRE_INCREX_CMD, false, true)
                .ok()
                .and_then(system_time_from_unix_millis_i128);
            match at {
                Some(at) => IncrexExpiry::Set(at),
                None => return resp_error(ERR_INVALID_EXPIRE_INCREX_CMD),
            }
        }
    };

    // What is propagated is what this did, not the command: see below.
    effects_begin();

    // A key that has already expired behaves exactly like an absent one.
    lazy_expire_key(ctx.store, *ctx.selected_db, &key);

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &key);
    let mut map = write_lock!(shard);

    let existing: Option<&[u8]> = match map.get(key.as_slice()) {
        None => None,
        Some(ValueRef::String(v)) => Some(v.as_bytes()),
        Some(_) => {
            return resp_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    };
    let existed = existing.is_some();

    // Compute the new value and the increment actually applied. Out of
    // [LBOUND, UBOUND], or past what the type holds: left alone, with the
    // current value and 0, or with SATURATE clamped to the bound.
    let (new_repr, applied_repr) = match mode {
        IncrexMode::Int => {
            let current: i64 = match existing {
                None => 0,
                Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => return resp_error(ERR_VALUE_NOT_INTEGER),
                },
            };
            let (sum, overflow) = current.overflowing_add(incr_int);
            let value = if overflow || sum > ubound_i || sum < lbound_i {
                if !saturate {
                    return resp_array(vec![resp_integer(current), resp_integer(0)]);
                }
                if overflow {
                    if incr_int >= 0 { ubound_i } else { lbound_i }
                } else if sum > ubound_i {
                    ubound_i
                } else {
                    lbound_i
                }
            } else {
                sum
            };
            let Some(delta) = value.checked_sub(current) else {
                return resp_error("ERR applied increment would overflow");
            };
            (value.to_string(), delta.to_string())
        }
        IncrexMode::Float => {
            let current: f64 = match existing {
                None => 0.0,
                Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                    Some(n) if !f64::is_nan(n) => n,
                    _ => return resp_error("ERR value is not a valid float"),
                },
            };
            if current.is_infinite() {
                return resp_error("ERR value cannot be Infinity");
            }
            let sum = current + incr_float;
            let overflow = sum.is_infinite();
            let value = if overflow || sum > ubound_f || sum < lbound_f {
                if !saturate {
                    return resp_array(vec![
                        resp_bulk_str(&format_increx_float(current)),
                        resp_bulk_str("0"),
                    ]);
                }
                if overflow {
                    if incr_float >= 0.0 {
                        ubound_f
                    } else {
                        lbound_f
                    }
                } else if sum > ubound_f {
                    ubound_f
                } else {
                    lbound_f
                }
            } else {
                sum
            };
            let delta = value - current;
            if delta.is_infinite() {
                return resp_error("ERR applied increment would be Infinity");
            }
            (format_increx_float(value), format_increx_float(delta))
        }
    };
    let reply = match mode {
        // Integer mode replies with RESP integers in both slots.
        IncrexMode::Int => resp_array(vec![
            resp_integer(new_repr.parse().unwrap_or(0)),
            resp_integer(applied_repr.parse().unwrap_or(0)),
        ]),
        // Float mode replies with bulk strings, the same encoding INCRBYFLOAT
        // already uses on this server.
        IncrexMode::Float => {
            resp_array(vec![resp_bulk_str(&new_repr), resp_bulk_str(&applied_repr)])
        }
    };

    // ENX: only set a TTL when the key has none, so a window rate limiter
    // creates a new window without extending a live one.
    let has_ttl = existed && map.expires(key.as_slice()).is_some();
    let new_expiry = match expiry {
        IncrexExpiry::Set(at) if !(enx && has_ttl) => Some(at),
        _ => None,
    };
    let as_bytes = |s: &[u8]| s.to_vec();

    // A TTL already past: the key goes, and is propagated as DEL, or UNLINK
    // with `lazyfree-lazy-expire` (Redis 8.10). Past by the command's time,
    // as Redis's `checkAlreadyExpired`: a script's or an `EXEC`'s (#659).
    if let Some(at) = new_expiry
        && at <= crate::script_clock::wall_system_time_now()
    {
        if existed {
            let removed = map.remove(key.as_slice());
            drop(map);
            drop(removed);
            let lazy = crate::commands::server::remembered_config("lazyfree-lazy-expire")
                .is_some_and(|v| v.eq_ignore_ascii_case("yes"));
            effect(vec![
                as_bytes(if lazy { b"UNLINK" } else { b"DEL" }),
                as_bytes(&key),
            ]);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "del", &key);
        }
        crate::stats::record_expired();
        return reply;
    }

    // The value and its time to live in one step.
    let replaced = if persist {
        map.insert_str_expiring(&*key, new_repr.as_bytes(), None)
    } else if let Some(at) = new_expiry {
        map.insert_str_expiring(&*key, new_repr.as_bytes(), Some(unix_ms(at)))
    } else {
        map.insert_str(&*key, new_repr.as_bytes())
    };
    drop(map);
    drop(replaced);

    // Propagated as a SET of the result, so float formatting cannot drift on
    // replicas or an AOF replay, with the TTL as it now is (Redis 8.10).
    let mut set = vec![
        as_bytes(b"SET"),
        as_bytes(&key),
        as_bytes(new_repr.as_bytes()),
    ];
    let mut persisted = false;
    if persist {
        persisted = has_ttl;
    } else if let Some(at) = new_expiry {
        let ms = at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis());
        set.extend([as_bytes(b"PXAT"), as_bytes(ms.to_string().as_bytes())]);
    } else {
        set.push(as_bytes(b"KEEPTTL"));
    }
    effect(set);

    let event = match mode {
        IncrexMode::Int => "incrby",
        IncrexMode::Float => "incrbyfloat",
    };
    notify(ctx.pubsub, *ctx.selected_db, KS_STRING, event, &key);
    if persisted {
        notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "persist", &key);
    }
    if new_expiry.is_some() {
        notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", &key);
    }
    reply
}

/// Format a float the way Redis formats `INCRBYFLOAT` results: shortest
/// representation, trailing zeros trimmed.
fn format_increx_float(v: f64) -> String {
    let mut s = format!("{v}");
    if s.contains('.') {
        s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    }
    if s == "-0" { "0".to_string() } else { s }
}

#[cfg(test)]
mod write_shards_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    /// Keys in `n` different shards of `db`, in the database's order.
    fn keys_in_different_shards(db: &store::Db, n: usize) -> Vec<String> {
        let mut by_shard = std::collections::BTreeMap::new();
        for i in 0.. {
            let key = format!("k{i}");
            by_shard
                .entry(store::shard_index(db.len(), &key))
                .or_insert(key);
            if by_shard.len() == n {
                break;
            }
        }
        by_shard.into_values().collect()
    }

    /// While `MSET` waits for a busy shard, it holds none of the others:
    /// another writer can take one of them meanwhile. Holding them while
    /// it waited queued every `MSET` behind every other.
    #[test]
    fn waiting_for_a_busy_shard_holds_no_other() {
        let store = store::new_store(1);
        let db = Arc::clone(&store[0]);
        let keys = keys_in_different_shards(&db, 3);
        let (first, busy) = (keys[0].clone(), keys[2].clone());
        let first_shard = store::shard_index(db.len(), &first);
        let busy_shard = store::shard_index(db.len(), &busy);

        let holder = db[busy_shard].write();
        let locked = Arc::new(AtomicBool::new(false));
        let mset = {
            let (db, keys, locked) = (Arc::clone(&db), keys.clone(), Arc::clone(&locked));
            crate::cpus::spawn(move || {
                let mut shards = WriteShards::lock(&db, keys.iter().map(String::as_bytes));
                locked.store(true, Ordering::SeqCst);
                for key in &keys {
                    drop(
                        shards
                            .map(key.as_bytes())
                            .set_str_expiring(key.as_str(), b"v", None),
                    );
                }
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !locked.load(Ordering::SeqCst),
            "it went on without the busy shard"
        );
        // The first shard is free while it waits.
        let mut taken = None;
        for _ in 0..200 {
            if let Some(guard) = db[first_shard].try_write() {
                taken = Some(guard);
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(taken.is_some(), "the first shard was held while waiting");
        drop(taken);
        drop(holder);
        mset.join().unwrap();
        assert!(locked.load(Ordering::SeqCst));
        for key in &keys {
            let shard = store::get_shard(&db, key);
            assert!(shard.read().contains_key(key), "{key}");
        }
    }

    /// Many writers of overlapping shards, each taking them as `MSET`
    /// does, never deadlock and never hold a shard together.
    #[test]
    fn overlapping_writers_finish_and_exclude_each_other() {
        let store = store::new_store(1);
        let db = Arc::clone(&store[0]);
        let keys = keys_in_different_shards(&db, 12);
        let inside: Arc<Vec<std::sync::atomic::AtomicUsize>> = Arc::new(
            (0..db.len())
                .map(|_| std::sync::atomic::AtomicUsize::new(0))
                .collect(),
        );
        let writers: Vec<_> = (0..8)
            .map(|w| {
                let (db, keys, inside) = (Arc::clone(&db), keys.clone(), Arc::clone(&inside));
                crate::cpus::spawn(move || {
                    for round in 0..2000usize {
                        // A different window of the keys each time, so the
                        // writers overlap in some shards and not others.
                        let from = (w * 3 + round) % keys.len();
                        let mine: Vec<&[u8]> = (0..5)
                            .map(|i| keys[(from + i * 2) % keys.len()].as_bytes())
                            .collect();
                        let _shards = WriteShards::lock(&db, mine.iter().copied());
                        let held: Vec<usize> = shards_of(&db, mine.iter().copied());
                        for &s in &held {
                            assert_eq!(
                                inside[s].fetch_add(1, Ordering::SeqCst),
                                0,
                                "shard {s} held twice"
                            );
                        }
                        std::hint::spin_loop();
                        for &s in &held {
                            inside[s].fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
    }
}

#[cfg(test)]
mod setrange_tests {
    use super::*;
    use crate::commands::bitmap::tests::{
        Holder, SIZES, SMALL, alone, counting, dispatched, in_entry, put, stored, with_ctx,
    };
    use std::sync::Arc;

    /// `line`, split at spaces (a trailing space is an empty patch), run by
    /// the strings handler on `store`'s database 0: the reply, whether the
    /// write is left unpropagated, and the most the handler allocated at
    /// once, if this thread counts it.
    fn run(store: &store::Store, line: &str) -> (Vec<u8>, bool, Option<isize>) {
        with_ctx(store, line, &crate::watch::NoopWatchRegistry, |mut ctx| {
            let name = line.split(' ').next().unwrap();
            let (reply, peak) = crate::memory_use::peak_during(|| handle(&mut ctx, name));
            (reply.unwrap(), ctx.skip_write_side_effects, peak)
        })
    }

    /// [`run`] in a test that counts allocations (see `counting`).
    fn counted(store: &store::Store, line: &str) -> (Vec<u8>, bool, isize) {
        let (reply, skipped, peak) = run(store, line);
        (
            reply,
            skipped,
            peak.expect("a thread that counts its allocations"),
        )
    }

    /// `len` bytes that differ from their neighbours.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    const WRONGTYPE: &[u8] = b"-WRONGTYPE";

    /// A patch inside a string nothing else holds is written where the
    /// string is, at every size up to 64 MiB: no copy, and the reply is the
    /// length.
    #[test]
    fn a_setrange_inside_a_string_nothing_else_holds_copies_nothing() {
        counting(
            "commands::strings::setrange_tests::a_setrange_inside_a_string_nothing_else_holds_copies_nothing",
            || {
                for len in SIZES.into_iter().chain([64 << 20]) {
                    let store = store::new_store(1);
                    let mut value = pattern(len);
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
                    for offset in [0, len / 2, len - 3] {
                        let line = format!("SETRANGE k {offset} abc");
                        let (reply, skipped, peak) = counted(&store, &line);
                        assert_eq!((reply, skipped), (resp_integer(len as i64), false));
                        assert!(peak < SMALL, "{len} {offset}: {peak} bytes");
                        value[offset..offset + 3].copy_from_slice(b"abc");
                        assert_eq!(stored(&store, b"k"), Some((value.clone(), at)), "{len}");
                    }
                }
            },
        );
    }

    /// Held by a snapshot (the entry) or a reply (the bytes), the string is
    /// copied once and the holder keeps the bytes it held; bytes in the
    /// entry that a reply copied out are not held, and stay in place.
    #[test]
    fn a_setrange_of_a_held_string_copies_it_once_and_the_holder_keeps_its_bytes() {
        counting(
            "commands::strings::setrange_tests::a_setrange_of_a_held_string_copies_it_once_and_the_holder_keeps_its_bytes",
            || {
                for len in SIZES.into_iter().chain([64 << 20]) {
                    let store = store::new_store(1);
                    let mut value = pattern(len);
                    put(&store, b"k", &value);
                    let inline = in_entry(&store, b"k");
                    let holders = [Holder::Nothing, Holder::Entry, Holder::Bytes];
                    for (holder, offset) in holders.into_iter().zip([0, 2, 4]) {
                        let (old, at) = stored(&store, b"k").unwrap();
                        let shard = store::get_shard(&store[0], b"k");
                        let entry = (holder == Holder::Entry).then(|| shard.read().get_ref(b"k"));
                        let bytes =
                            (holder == Holder::Bytes).then(|| match shard.read().get(b"k") {
                                Some(ValueRef::String(b)) => b.to_shared(),
                                _ => panic!("no string"),
                            });
                        let line = format!("SETRANGE k {offset} zz");
                        let (reply, skipped, peak) = counted(&store, &line);
                        let case = format!("{len} {holder:?}");
                        assert_eq!(
                            (reply, skipped),
                            (resp_integer(len as i64), false),
                            "{case}"
                        );
                        value[offset..offset + 2].copy_from_slice(b"zz");
                        let (now, moved_to) = stored(&store, b"k").unwrap();
                        assert_eq!(now, value, "{case}");
                        let copied = holder == Holder::Entry || holder == Holder::Bytes && !inline;
                        assert_eq!(moved_to != at, copied, "{case}");
                        if copied {
                            // One copy, not a copy to change and then another to store.
                            let most = (len + len / 2) as isize + SMALL;
                            assert!(peak >= len as isize && peak < most, "{case}: {peak}");
                        } else {
                            assert!(peak < SMALL, "{case}: {peak} bytes");
                        }
                        if let Some(Some(entry)) = &entry {
                            assert!(
                                matches!(entry.value(), ValueRef::String(b) if b[..] == old[..]),
                                "{case}"
                            );
                        }
                        if let Some(bytes) = &bytes {
                            assert_eq!(bytes[..], old[..], "{case}");
                        }
                    }
                }
            },
        );
    }

    /// Growth stays where the string is while its block keeps its class,
    /// the gap zero-filled; past the class, or for a block of the system
    /// allocator's, the string is copied once into a new block, zero-filled
    /// the same way.
    #[test]
    fn a_setrange_grows_in_place_only_within_its_block() {
        counting(
            "commands::strings::setrange_tests::a_setrange_grows_in_place_only_within_its_block",
            || {
                let slab = |len: usize| crate::value_slab::class_of(len + 8);
                let top = crate::value_slab::block_size(slab(16 << 10).unwrap()) - 8;
                assert!(top > (16 << 10) + 2, "no room in the class");
                // "k" and 110 bytes make a 123-byte entry, in the class of 113
                // to 128, as 100 bytes do; 120 bytes pass it.
                for (len, end, stays) in [
                    (100, 110, true),
                    (100, 120, false),
                    (16 << 10, top, true),
                    (top, top + 1, false),
                    (64 << 10, (64 << 10) + 1, false),
                    (1 << 20, (1 << 20) + 10, false),
                ] {
                    let store = store::new_store(1);
                    let mut value = pattern(len);
                    put(&store, b"k", &value);
                    let (_, at) = stored(&store, b"k").unwrap();
                    let line = format!("SETRANGE k {} ab", end - 2);
                    let (reply, skipped, peak) = counted(&store, &line);
                    assert_eq!((reply, skipped), (resp_integer(end as i64), false), "{len}");
                    value.resize(end - 2, 0);
                    value.extend_from_slice(b"ab");
                    let (now, moved_to) = stored(&store, b"k").unwrap();
                    assert_eq!(now, value, "{len}");
                    assert_eq!(moved_to == at, stays, "{len}");
                    if stays {
                        assert!(peak < SMALL, "{len}: {peak} bytes");
                    } else {
                        let most = (end + end / 2) as isize + SMALL;
                        assert!(peak >= len as isize && peak < most, "{len}: {peak}");
                    }
                }
            },
        );
    }

    /// An empty patch writes nothing and copies nothing: the reply is the
    /// length, a missing key is not made, a string does not grow to the
    /// offset (however far), and another type is refused. The dispatcher
    /// still sees a write, as before.
    #[test]
    fn an_empty_setrange_writes_nothing_and_copies_nothing() {
        counting(
            "commands::strings::setrange_tests::an_empty_setrange_writes_nothing_and_copies_nothing",
            || {
                let store = store::new_store(1);
                for line in [
                    "SETRANGE m 10 ",
                    "SETRANGE m 0 ",
                    "SETRANGE m 1099511627776 ",
                ] {
                    let (reply, skipped, peak) = counted(&store, line);
                    assert_eq!((reply, skipped), (resp_integer(0), false), "{line}");
                    assert!(peak < SMALL, "{line}: {peak} bytes");
                    assert_eq!(stored(&store, b"m"), None, "{line}");
                }
                for len in SIZES.into_iter().chain([64 << 20]) {
                    let value = pattern(len);
                    put(&store, b"k", &value);
                    let was = stored(&store, b"k");
                    for offset in [0, len / 2, len + 10, 1 << 40] {
                        let line = format!("SETRANGE k {offset} ");
                        let (reply, skipped, peak) = counted(&store, &line);
                        assert_eq!((reply, skipped), (resp_integer(len as i64), false));
                        assert!(peak < SMALL, "{len} {offset}: {peak} bytes");
                        assert_eq!(stored(&store, b"k"), was, "{len} {offset}");
                    }
                }
                let set = crate::member_set::MemberSet::new();
                let set = KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(set)));
                drop(store::get_shard(&store[0], b"s").write().insert(b"s", set));
                let (reply, _, _) = counted(&store, "SETRANGE s 0 ");
                assert!(reply.starts_with(WRONGTYPE), "{reply:?}");
            },
        );
    }

    /// A key's time to live stays, in place or copied; a key past it is
    /// written as a new one, zero-filled to the offset, without one.
    /// Another type, a length past 512 MB and a bad offset are refused, and
    /// leave everything as it was.
    #[test]
    fn setrange_keeps_a_time_to_live_and_refuses_as_before() {
        let store = store::new_store(1);
        let later = crate::commands::helpers::now_ms() + 3_600_000;
        for (name, len) in [("inline", 100), ("slab", 16 << 10), ("own", 64 << 10)] {
            let key = name.as_bytes();
            let shard = store::get_shard(&store[0], key);
            drop(
                shard
                    .write()
                    .insert_str_expiring(key, pattern(len), Some(later)),
            );
            let at = stored(&store, key).unwrap().1;
            assert_eq!(
                run(&store, &format!("SETRANGE {name} 0 a")).0,
                resp_integer(len as i64)
            );
            assert_eq!(stored(&store, key).unwrap().1, at, "{name}: in place");
            let held = shard.read().get_ref(key);
            let line = format!("SETRANGE {name} {} b", len + 1);
            assert_eq!(run(&store, &line).0, resp_integer(len as i64 + 2));
            drop(held);
            assert_ne!(stored(&store, key).unwrap().1, at, "{name}: copied");
            assert_eq!(shard.read().expires(key), Some(later), "{name}");
            let mut want = pattern(len);
            want[0] = b'a';
            want.extend_from_slice(b"\0b");
            assert_eq!(stored(&store, key).unwrap().0, want, "{name}");
        }
        let shard = store::get_shard(&store[0], b"past");
        drop(shard.write().insert_str_expiring(b"past", b"abc", Some(1)));
        assert_eq!(run(&store, "SETRANGE past 2 x").0, resp_integer(3));
        assert_eq!(stored(&store, b"past").unwrap().0, b"\0\0x");
        assert_eq!(shard.read().expires(b"past"), None);

        let set = crate::member_set::MemberSet::new();
        let set = KiviDBValue::Set(Arc::new(parking_lot::RwLock::new(set)));
        drop(
            store::get_shard(&store[0], b"set")
                .write()
                .insert(b"set", set),
        );
        assert!(run(&store, "SETRANGE set 0 x").0.starts_with(WRONGTYPE));
        let map = store::get_shard(&store[0], b"set").read();
        assert!(matches!(map.get(b"set"), Some(ValueRef::Set(_))));
        drop(map);

        put(&store, b"k", b"abc");
        for (line, error) in [
            (
                "SETRANGE k 536870912 x",
                "-ERR string exceeds maximum allowed size",
            ),
            (
                "SETRANGE k 536870911 xy",
                "-ERR string exceeds maximum allowed size",
            ),
            ("SETRANGE k -1 x", "-ERR offset is out of range"),
            (
                "SETRANGE k x x",
                "-ERR value is not an integer or out of range",
            ),
        ] {
            let reply = run(&store, line).0;
            assert!(reply.starts_with(error.as_bytes()), "{line}: {reply:?}");
        }
        assert_eq!(stored(&store, b"k").unwrap().0, b"abc");
    }

    /// Through the dispatcher, a `SETRANGE` written in place breaks a
    /// `WATCH` on its key and announces `setrange`, as one that copies a
    /// held string does. An empty one announces nothing but still breaks
    /// the watch, as before. Alone: the event flags are the process's.
    #[test]
    fn a_setrange_breaks_a_watch_and_announces_itself_as_before() {
        alone(
            "commands::strings::setrange_tests::a_setrange_breaks_a_watch_and_announces_itself_as_before",
            || {
                use crate::keyspace_events as events;
                use crate::watch::WatchNotifier;
                use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
                events::KS_FLAGS.store(events::parse_ks_flags("KE$"), Relaxed);
                let store = store::new_store(1);
                let watches = crate::watch::WatchRegistryInner::new();
                for (name, len) in [("inline", 100), ("slab", 16 << 10), ("own", 64 << 10)] {
                    let key = name.as_bytes();
                    put(&store, key, &pattern(len));
                    let mut want = pattern(len);
                    for (offset, patch, held) in [(0, "ab", false), (2, "cd", true), (4, "", false)]
                    {
                        let shard = store::get_shard(&store[0], key);
                        let held = held.then(|| shard.read().get_ref(key));
                        let at = stored(&store, key).unwrap().1;
                        let broken = Arc::new(AtomicBool::new(false));
                        watches.watch(0, key, Arc::clone(&broken), false);
                        events::hold();
                        let line = format!("SETRANGE {name} {offset} {patch}");
                        let reply = dispatched(&store, &line, &watches);
                        let announced = events::release();
                        let case = format!("{line:?}, held: {}", held.is_some());
                        assert_eq!(reply, resp_integer(len as i64), "{case}");
                        want[offset..offset + patch.len()].copy_from_slice(patch.as_bytes());
                        let (now, moved_to) = stored(&store, key).unwrap();
                        assert_eq!(now, want, "{case}");
                        assert_eq!(moved_to == at, held.is_none(), "{case}");
                        assert!(broken.load(Relaxed), "{case}: the watch");
                        let want_events: Vec<(Vec<u8>, Arc<[u8]>)> = if patch.is_empty() {
                            vec![]
                        } else {
                            let mut space = b"__keyspace@0__:".to_vec();
                            space.extend_from_slice(key);
                            let event = b"__keyevent@0__:setrange".to_vec();
                            vec![
                                (space, Arc::from(&b"setrange"[..])),
                                (event, Arc::from(key)),
                            ]
                        };
                        assert_eq!(announced, want_events, "{case}: the events");
                        drop(held);
                    }
                }
            },
        );
    }

    /// A capture of the dataset (what `BGSAVE`, a full sync, an AOF rewrite
    /// and `BACKUP` write out) holds each string's entry: `SETRANGE`s after
    /// it change the live value only, and the image has the bytes from
    /// before.
    #[test]
    fn a_capture_keeps_the_bytes_from_before_a_setrange() {
        let store = store::new_store(1);
        let keys = ["inline", "slab", "own"];
        let lens = [100, 16 << 10, 1 << 20];
        for (key, len) in keys.into_iter().zip(lens) {
            put(&store, key.as_bytes(), &pattern(len));
        }
        let frozen = crate::snapshot::freeze_dataset(&store, None).unwrap();
        for (key, len) in keys.into_iter().zip(lens) {
            let at = stored(&store, key.as_bytes()).unwrap().1;
            // The first copies the string; the second finds it unheld.
            for line in [
                format!("SETRANGE {key} 0 zz"),
                format!("SETRANGE {key} 4 yy"),
            ] {
                assert_eq!(run(&store, &line).0, resp_integer(len as i64), "{line}");
            }
            let (now, moved_to) = stored(&store, key.as_bytes()).unwrap();
            assert_ne!(moved_to, at, "{key}");
            assert_eq!(&now[..6], &[b'z', b'z', 2, 3, b'y', b'y'], "{key}");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.kdb");
        let path = path.to_str().unwrap();
        frozen.write_file(path).unwrap();
        let loaded = crate::snapshot::load_snapshot(path).unwrap();
        for (key, len) in keys.into_iter().zip(lens) {
            match loaded.dbs[0].get(&store::Key::from(key)) {
                Some(KiviDBValue::String(b)) => assert!(b[..] == pattern(len)[..], "{key}"),
                _ => panic!("{key} not in the image"),
            }
        }
    }
}
