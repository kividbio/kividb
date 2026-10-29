use crate::commands::ctx::Ctx;
use crate::commands::helpers::*;
use crate::keyspace_events::{KS_GENERIC, KS_STRING, notify};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::types::SharedBytes;
use crate::{read_lock, write_lock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The longest a string value may be: Redis's default `proto-max-bulk-len`.
const MAX_STRING_LEN: usize = 512 * 1024 * 1024;

/// The key and value pairs of an `MSET`-shaped command, or `None` if a key
/// is not UTF-8.
fn utf8_pairs<'a>(cmd: &[&'a [u8]]) -> Option<Vec<(&'a str, &'a [u8])>> {
    cmd[1..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Some((parse_utf8(pair[0])?, pair[1])))
        .collect()
}

/// A key's time to live is dropped, if it has one: what a plain write does.
/// Its value shard may be held, as it is by the commands above: that is the
/// order the two are always taken in.
fn forget_expiry(expiry_store: &store::ExpiryStore, db: usize, key: &str) {
    let shard = get_expiry_shard(expiry_store, db, key);
    if !shard.holds_nothing() && read_lock!(shard).contains_key(key) {
        write_lock!(shard).remove(key);
    }
}

/// The shards a command's keys are in, each once, in the order they stand
/// in the database.
///
/// That is the order to lock them in. Whatever else holds more than one
/// shard at a time (a snapshot, a rewrite of the AOF) takes them in that
/// order too, so no two of them can each hold what the other waits for.
fn shards_of<'k>(db: &store::Db, keys: impl Iterator<Item = &'k str>) -> Vec<usize> {
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
    fn lock<'k>(db: &'a store::Db, keys: impl Iterator<Item = &'k str>) -> Self {
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
    fn map(&self, key: &str) -> Option<&store::ShardMap> {
        let index = store::shard_index(self.shards, key);
        let at = self
            .held
            .binary_search_by_key(&index, |(have, _)| *have)
            .ok()?;
        Some(&self.held[at].1)
    }
}

/// The same for writing: what makes a write of several keys one step.
struct WriteShards<'a> {
    shards: usize,
    held: Vec<(usize, parking_lot::RwLockWriteGuard<'a, store::ShardMap>)>,
}

impl<'a> WriteShards<'a> {
    fn lock<'k>(db: &'a store::Db, keys: impl Iterator<Item = &'k str>) -> Self {
        let held = shards_of(db, keys)
            .into_iter()
            .map(|index| (index, db[index].write()))
            .collect();
        Self {
            shards: db.len(),
            held,
        }
    }

    /// The table `key` is in. The key must be one this was locked for.
    fn map(&mut self, key: &str) -> &mut store::ShardMap {
        let index = store::shard_index(self.shards, key);
        let at = self
            .held
            .binary_search_by_key(&index, |(have, _)| *have)
            .expect("a key of the command the shards were locked for");
        &mut self.held[at].1
    }
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

            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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
            let mut expiry: Option<SystemTime> = None;

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

                    "EX" => {
                        i += 1;
                        let secs = match cmd.get(i) {
                            Some(b) => match parse_bytes_as_i128(b) {
                                Some(s) => s,
                                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
                            },
                            None => return Some(resp_error("ERR syntax error")),
                        };
                        match deadline_after_relative_secs(secs, ERR_INVALID_EXPIRE_SET_CMD) {
                            Ok(t) => expiry = Some(t),
                            Err(e) => return Some(resp_error(e)),
                        }
                    }

                    "PX" => {
                        i += 1;
                        let ms = match cmd.get(i) {
                            Some(b) => match parse_bytes_as_i128(b) {
                                Some(m) => m,
                                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
                            },
                            None => return Some(resp_error("ERR syntax error")),
                        };
                        match deadline_after_relative_millis(ms, ERR_INVALID_EXPIRE_SET_CMD) {
                            Ok(t) => expiry = Some(t),
                            Err(e) => return Some(resp_error(e)),
                        }
                    }

                    "EXAT" => {
                        i += 1;
                        let ts: u64 = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return Some(resp_error("ERR invalid expire time in SET")),
                        };
                        expiry = Some(expiry_unix_secs(ts));
                    }

                    "PXAT" => {
                        i += 1;
                        let ts_ms: u64 = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return Some(resp_error("ERR invalid expire time in SET")),
                        };
                        expiry = Some(expiry_unix_millis(ts_ms));
                    }

                    _ => {
                        // Unknown token — Redis returns a syntax error
                        return Some(resp_error("ERR syntax error"));
                    }
                }
                i += 1;
            }

            // ── expiry sanity ─────────────────────────────────────────────────────
            if keepttl && expiry.is_some() {
                return Some(resp_error("ERR invalid expire time in SET"));
            }

            // ── check & handle expiry on the existing key ─────────────────────────
            // Do this before taking the write lock so we don't hold it longer than needed.
            // The keyspace and the expiry store have the same shards: the key
            // is hashed once for both.
            let db = &ctx.store[*ctx.selected_db];
            let index = store::shard_index(db.len(), key);
            let exp_shard = &ctx.expiry_store[*ctx.selected_db][index];
            if !exp_shard.holds_nothing() && is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
            }

            // ── single write-lock scope ───────────────────────────────────────────
            let shard = &db[index];
            // Made before the shard is locked, for the same reason the old
            // value is dropped after: allocating and copying are not the
            // shard's to wait for. A condition that fails wastes it.
            let value = KiviDBValue::String(SharedBytes::from(cmd[2]));
            let mut map = write_lock!(shard);

            // Looked up once; what follows asks this, not the map again.
            let existing = map.get(key);
            let key_exists = existing.is_some();

            // GET: capture old value before any mutation (nil if absent/wrong-type)
            let old_val: Option<SharedBytes> = if want_get {
                match existing {
                    Some(ValueRef::String(v)) => Some(v.to_shared()),
                    Some(_) => {
                        // Redis returns WRONGTYPE if GET is used on a non-string key
                        return Some(resp_error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ));
                    }
                    None => None,
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
            // KEEPTTL: preserve existing expiry — don't touch expiry_store at all.
            // For everything else: the store_value call (below) clears stale expiry,
            // then we optionally set a new one.
            //
            // We insert directly here (already holding the write lock) rather than
            // calling store_value so we don't re-acquire the shard lock.
            // An overwrite keeps the key it has: no new string for it.
            let replaced = map.insert(key, value);
            drop(map); // ← release write lock BEFORE touching expiry_store
            // The value that was there is let go of with the shard unlocked:
            // its count is in memory nothing has touched lately, and freeing
            // it is the allocator's time, neither of them the shard's to wait
            // for.
            drop(replaced);

            if !keepttl {
                // Only take the expiry locks if there's actually an expiry to clear.
                // With no TTLs in the shard, this costs one load.
                if !exp_shard.holds_nothing() && read_lock!(exp_shard).contains_key(key) {
                    write_lock!(exp_shard).remove(key);
                }
                // Then set new one if provided
                if let Some(exp) = expiry {
                    set_expiry(ctx.expiry_store, *ctx.selected_db, key, exp);
                }
            }

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
            let key = match std::str::from_utf8(cmd[1]) {
                Ok(k) => k,
                Err(_) => return Some(resp_error("ERR must be valid UTF-8")),
            };

            // One hash for both stores: they have the same shards.
            let db = &ctx.store[*ctx.selected_db];
            let index = store::shard_index(db.len(), key);
            if !ctx.expiry_store[*ctx.selected_db][index].holds_nothing()
                && is_expired(ctx.expiry_store, *ctx.selected_db, key)
            {
                expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                crate::stats::record_miss();
                return Some(resp_null_bulk());
            }

            let shard = &db[index];
            let map = read_lock!(shard);
            Some(match map.get(key) {
                Some(ValueRef::String(v)) => {
                    crate::stats::record_hit();
                    resp_bulk(&v)
                }
                Some(_) => {
                    crate::stats::record_miss();
                    resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")
                }
                None => {
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let secs = match parse_bytes_as_i128(cmd[2]) {
                Some(s) => s,
                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
            };
            let flags = match parse_expire_modifier_flags(cmd.as_slice(), 3) {
                Ok(f) => f,
                Err(msg) => return Some(resp_error(&msg)),
            };

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);

            // Redis: EXPIRE key 0 deletes the key immediately (same as DEL).
            if secs == 0 {
                if !read_lock!(shard).contains_key(key) {
                    return Some(resp_integer(0));
                }
                let removed = delete_key(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                if removed {
                    crate::vector::maybe_unindex(ctx.index_store, key);
                    crate::vector::maybe_request_compaction(
                        ctx.index_store,
                        ctx.compaction_tx,
                        key,
                    );
                }
                return Some(resp_integer(if removed { 1 } else { 0 }));
            }

            if !read_lock!(shard).contains_key(key) {
                return Some(resp_integer(0));
            }

            let when_ms = match absolute_when_ms_from_expire_secs(secs) {
                Ok(w) => w,
                Err(e) => return Some(resp_error(e)),
            };
            let new_abs = match system_time_from_unix_millis_i128(when_ms) {
                Some(t) => t,
                None => return Some(resp_error(ERR_INVALID_EXPIRE_EXPIRE_CMD)),
            };

            let exp_shard = get_expiry_shard(ctx.expiry_store, *ctx.selected_db, key);
            let current = read_lock!(exp_shard).get(key).copied();

            if !expire_modifier_allows_set(&flags, current, new_abs) {
                return Some(resp_integer(0));
            }

            let now = crate::script_clock::wall_system_time_now();
            if new_abs <= now {
                let removed = delete_key(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                if removed {
                    crate::vector::maybe_unindex(ctx.index_store, key);
                    crate::vector::maybe_request_compaction(
                        ctx.index_store,
                        ctx.compaction_tx,
                        key,
                    );
                }
                return Some(resp_integer(if removed { 1 } else { 0 }));
            }

            set_expiry(ctx.expiry_store, *ctx.selected_db, key, new_abs);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            Some(resp_integer(1))
        }

        "SETEX" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'setex' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let secs: u64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) if n > 0 => n,
                _ => return Some(resp_error("ERR invalid expire time in SETEX")),
            };
            let value: KiviDBValue = KiviDBValue::String(SharedBytes::from(cmd[3]));
            let expires_at = expiry_at_after(Duration::from_secs(secs));

            store_value(ctx.store, ctx.expiry_store, *ctx.selected_db, key, value);
            set_expiry(ctx.expiry_store, *ctx.selected_db, key, expires_at);
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
                // The key as it stands in the command: each was copied to a
                // string of its own to be looked up.
                let Some(key) = parse_utf8(k) else {
                    continue;
                };
                if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                    expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                    continue;
                }
                let shard = store::get_shard(db, key);
                let map = read_lock!(shard);

                if map.contains_key(key) {
                    count += 1;
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
            let key = match String::from_utf8(cmd[1].to_vec()) {
                Ok(k) => k,
                Err(_) => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let shard = store::get_shard(db, &key);
            let mut map = write_lock!(shard);

            let next = match map.get(key.as_str()) {
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

            // INCR must read then write so the lock scope is unavoidable
            // but we can at least avoid the intermediate to_string().as_bytes() chain
            let new_val = KiviDBValue::String(SharedBytes::from(next.to_string().as_bytes()));
            map.insert(Key::from(&*key), new_val);
            drop(map); // explicitly drop lock before AOF
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", &key);
            Some(resp_integer(next))
        }

        "STRLEN" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'strlen' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                return Some(resp_integer(0));
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            match map.get(key) {
                Some(ValueRef::String(v)) => Some(resp_integer(v.len() as i64)),
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
                None => Some(resp_integer(0)),
            }
        }

        // ── APPEND ────────────────────────────────────────────────────────────────────
        // Appends bytes to the existing string, creating it if absent.
        // Returns the new length. Because Arc<[u8]> is immutable, we must allocate
        // a new buffer with the concatenated bytes — this is the only way.
        "APPEND" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'append' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let suffix = &cmd[2];
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            let new_val: SharedBytes = match map.get(key) {
                None => SharedBytes::from(*suffix),
                Some(ValueRef::String(existing)) => {
                    // As Redis: a string does not grow past 512 MB.
                    if existing.len() + suffix.len() > MAX_STRING_LEN {
                        return Some(resp_error(
                            "ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                        ));
                    }
                    SharedBytes::concat(&[&existing, suffix])
                }
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            let len = new_val.len() as i64;
            map.insert(Key::from(key), KiviDBValue::String(new_val));
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "append", key);
            Some(resp_integer(len))
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                return Some(resp_null_bulk());
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            match map.remove(key) {
                Some(KiviDBValue::String(v)) => {
                    drop(map);
                    // also clear expiry
                    let expiry_shard = get_expiry_shard(ctx.expiry_store, *ctx.selected_db, key);
                    write_lock!(expiry_shard).remove(key);
                    crate::stats::record_hit();
                    notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "del", key);
                    Some(resp_bulk(&v))
                }
                Some(other) => {
                    // put it back — we peeked the wrong type
                    map.insert(Key::from(key), other);
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
                None => {
                    crate::stats::record_miss();
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let new_val = KiviDBValue::String(SharedBytes::from(cmd[2]));
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            let old = map.insert(Key::from(key), new_val);
            drop(map);
            // clear any existing TTL — GETSET always persists the key
            let expiry_shard = get_expiry_shard(ctx.expiry_store, *ctx.selected_db, key);
            write_lock!(expiry_shard).remove(key);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            match old {
                Some(KiviDBValue::String(v)) => {
                    crate::stats::record_hit();
                    Some(resp_bulk(&v))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
                None => {
                    crate::stats::record_miss();
                    Some(resp_null_bulk())
                }
            }
        }

        // ── SETNX ─────────────────────────────────────────────────────────────────────
        // SET if Not eXists. Returns 1 if set, 0 if key already existed.
        "SETNX" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'setnx' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            // Check expiry first — an expired key counts as non-existent
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            if map.contains_key(key) {
                return Some(resp_integer(0));
            }
            map.insert(
                Key::from(key),
                KiviDBValue::String(SharedBytes::from(cmd[2])),
            );
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            Some(resp_integer(1))
        }

        // ── PSETEX ────────────────────────────────────────────────────────────────────
        // Like SETEX but the TTL is in milliseconds. Uses Duration::from_millis.
        "PSETEX" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'psetex' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let millis: u64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) if n > 0 => n,
                _ => return Some(resp_error("ERR invalid expire time in PSETEX")),
            };
            let value = KiviDBValue::String(SharedBytes::from(cmd[3]));
            let expires_at = expiry_at_after(Duration::from_millis(millis));
            store_value(ctx.store, ctx.expiry_store, *ctx.selected_db, key, value);
            set_expiry(ctx.expiry_store, *ctx.selected_db, key, expires_at);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "set", key);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            Some(resp_simple("OK"))
        }

        // ── MSET ──────────────────────────────────────────────────────────────────────
        // Atomically set N key-value pairs. Always succeeds. Returns OK.
        // Note: "atomic" here means no partial failure — all pairs are written.
        // We iterate shards independently, which is fine since each shard lock is
        // independent and MSET doesn't promise cross-shard atomicity in Redis either.
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
            let Some(pairs) = utf8_pairs(cmd) else {
                return Some(resp_error("ERR must be valid UTF-8"));
            };
            let db = *ctx.selected_db;
            {
                // Every shard the keys are in, locked together: nobody sees
                // some of the keys set and the others not yet.
                let mut shards = WriteShards::lock(&ctx.store[db], pairs.iter().map(|p| p.0));
                for (key, value) in &pairs {
                    shards.map(key).insert(
                        Key::from(*key),
                        KiviDBValue::String(SharedBytes::from(*value)),
                    );
                    forget_expiry(ctx.expiry_store, db, key);
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
            let shards = ReadShards::lock(db, cmd[1..].iter().filter_map(|key| parse_utf8(key)));
            for k in cmd[1..].iter() {
                let key = match parse_utf8(k) {
                    Some(k) => k,
                    None => {
                        results.push(nil.clone());
                        continue;
                    }
                };
                // A key past its time is not there. It is left for the next
                // command on it, or the sweep, to remove: that takes its
                // shard for writing, which this holds for reading.
                if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                    crate::stats::record_miss();
                    results.push(nil.clone());
                    continue;
                }
                let val = match shards.map(key).and_then(|map| map.get(key)) {
                    Some(ValueRef::String(v)) => {
                        crate::stats::record_hit();
                        resp_bulk(v.as_bytes())
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
            let Some(pairs) = utf8_pairs(cmd) else {
                return Some(resp_error("ERR must be valid UTF-8"));
            };
            let db = *ctx.selected_db;
            {
                // Looked at and written under the same locks: no key can
                // come to be between the looking and the writing. (They
                // were two passes, each key under a lock of its own, and
                // two clients could both be told that they had set a key.)
                let mut shards = WriteShards::lock(&ctx.store[db], pairs.iter().map(|p| p.0));
                for (key, _) in &pairs {
                    // A key past its time is not there, and is written over.
                    if shards.map(key).contains_key(key) && !is_expired(ctx.expiry_store, db, key) {
                        return Some(resp_integer(0));
                    }
                }
                for (key, value) in &pairs {
                    shards.map(key).insert(
                        Key::from(*key),
                        KiviDBValue::String(SharedBytes::from(*value)),
                    );
                    forget_expiry(ctx.expiry_store, db, key);
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
            let key = match String::from_utf8(cmd[1].to_vec()) {
                Ok(k) => k,
                Err(_) => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let delta: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, &key);
            let mut map = write_lock!(shard);
            // Check overflow properly
            let next = match map.get(key.as_str()) {
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
            map.insert(
                Key::from(&*key),
                KiviDBValue::String(SharedBytes::from(next.to_string().as_bytes())),
            );
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", &key);
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
            let key = match String::from_utf8(cmd[1].to_vec()) {
                Ok(k) => k,
                Err(_) => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, &key);
            let mut map = write_lock!(shard);
            let next = match map.get(key.as_str()) {
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
            map.insert(
                Key::from(&*key),
                KiviDBValue::String(SharedBytes::from(next.to_string().as_bytes())),
            );
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", &key);
            Some(resp_integer(next))
        }

        "DECRBY" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'decrby' command",
                ));
            }
            let key = match String::from_utf8(cmd[1].to_vec()) {
                Ok(k) => k,
                Err(_) => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let delta: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, &key);
            let mut map = write_lock!(shard);
            let next = match map.get(key.as_str()) {
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
            map.insert(
                Key::from(&*key),
                KiviDBValue::String(SharedBytes::from(next.to_string().as_bytes())),
            );
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrby", &key);
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
            let key = match String::from_utf8(cmd[1].to_vec()) {
                Ok(k) => k,
                Err(_) => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let delta: f64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not a valid float")),
            };
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, &key);
            let mut map = write_lock!(shard);
            let current: f64 = match map.get(key.as_str()) {
                None => 0.0,
                Some(ValueRef::String(v)) => {
                    match std::str::from_utf8(&v).ok().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return Some(resp_error("ERR value is not a valid float")),
                    }
                }
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            let next = current + delta;
            if next.is_infinite() || next.is_nan() {
                return Some(resp_error("ERR increment would produce NaN or Infinity"));
            }
            // Redis trims trailing zeros. format!("{}", f) already does this in Rust for many cases.
            let s = format!("{}", next);
            map.insert(
                Key::from(&*key),
                KiviDBValue::String(SharedBytes::from(s.as_bytes())),
            );
            drop(map);
            notify(ctx.pubsub, *ctx.selected_db, KS_STRING, "incrbyfloat", &key);
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let start: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let end: i64 = match parse_utf8(cmd[3]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                return Some(resp_bulk(b""));
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            match map.get(key) {
                None => Some(resp_bulk(b"")),
                Some(ValueRef::String(_)) if start < 0 && end < 0 && start > end => {
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
                    if s > e || len == 0 {
                        crate::stats::record_miss();
                        Some(resp_bulk(b""))
                    } else {
                        crate::stats::record_hit();
                        Some(resp_bulk(&v[s..=e]))
                    }
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }

        // ── SETRANGE ──────────────────────────────────────────────────────────────────
        // Overwrites bytes at a given offset, zero-padding if needed.
        // Creates the key if it doesn't exist (treats it as empty string).
        "SETRANGE" => {
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'setrange' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
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
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let mut map = write_lock!(shard);
            let existing: Vec<u8> = match map.get(key) {
                None => vec![],
                Some(ValueRef::String(v)) => v.to_vec(), // copy into owned Vec to mutate
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            // Nothing to write: the string is as it was, and a key that
            // was not there is not created.
            if patch.is_empty() {
                return Some(resp_integer(existing.len() as i64));
            }
            let required_len = offset + patch.len();
            let mut buf = existing;
            if buf.len() < required_len {
                buf.resize(required_len, 0u8); // zero-pad
            }
            buf[offset..offset + patch.len()].copy_from_slice(patch);
            let new_len = buf.len() as i64;
            map.insert(
                Key::from(key),
                KiviDBValue::String(SharedBytes::from(buf.as_slice())),
            );
            drop(map);
            Some(resp_integer(new_len))
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
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
                        crate::stats::record_miss();
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

            // Parse and apply the optional expiry modifier
            if cmd.len() >= 3 {
                let opt = match parse_utf8(cmd[2]) {
                    Some(s) => s.to_ascii_uppercase(),
                    None => return Some(resp_error("ERR syntax error")),
                };

                match opt.as_str() {
                    "EX" => {
                        let secs = match cmd.get(3) {
                            Some(b) => match parse_bytes_as_i128(b) {
                                Some(s) => s,
                                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
                            },
                            None => return Some(resp_error("ERR syntax error")),
                        };
                        let exp = match deadline_after_relative_secs(
                            secs,
                            ERR_INVALID_EXPIRE_GETEX_CMD,
                        ) {
                            Ok(t) => t,
                            Err(e) => return Some(resp_error(e)),
                        };
                        set_expiry(ctx.expiry_store, *ctx.selected_db, key, exp);
                    }

                    "PX" => {
                        let millis = match cmd.get(3) {
                            Some(b) => match parse_bytes_as_i128(b) {
                                Some(m) => m,
                                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
                            },
                            None => return Some(resp_error("ERR syntax error")),
                        };
                        let exp = match deadline_after_relative_millis(
                            millis,
                            ERR_INVALID_EXPIRE_GETEX_CMD,
                        ) {
                            Ok(t) => t,
                            Err(e) => return Some(resp_error(e)),
                        };
                        set_expiry(ctx.expiry_store, *ctx.selected_db, key, exp);
                    }

                    // EXAT / PXAT: store absolute wall times (scripts see frozen wall clock).
                    "EXAT" => {
                        let unix_secs: u64 = match cmd
                            .get(3)
                            .and_then(|b| parse_utf8(b))
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return Some(resp_error("ERR invalid expire time in GETEX")),
                        };
                        set_expiry(
                            ctx.expiry_store,
                            *ctx.selected_db,
                            key,
                            expiry_unix_secs(unix_secs),
                        );
                    }

                    "PXAT" => {
                        let unix_millis: u64 = match cmd
                            .get(3)
                            .and_then(|b| parse_utf8(b))
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return Some(resp_error("ERR invalid expire time in GETEX")),
                        };
                        set_expiry(
                            ctx.expiry_store,
                            *ctx.selected_db,
                            key,
                            expiry_unix_millis(unix_millis),
                        );
                    }

                    "PERSIST" => {
                        clear_expiry(ctx.expiry_store, *ctx.selected_db, key);
                    }

                    _ => return Some(resp_error("ERR syntax error")),
                }
            }

            // What it did to the expiry, as Redis says it.
            if val.is_some() && cmd.len() >= 3 {
                let persisted = cmd[2..]
                    .iter()
                    .any(|arg| arg.eq_ignore_ascii_case(b"PERSIST"));
                let event = if persisted { "persist" } else { "expire" };
                notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, event, key);
            }
            match val {
                Some(v) => Some(resp_bulk(&v)),
                None => Some(resp_null_bulk()),
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
            let key1 = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key1 must be valid UTF-8")),
            };
            let key2 = match parse_utf8(cmd[2]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key2 must be valid UTF-8")),
            };

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
            let s1: SharedBytes = {
                if is_expired(ctx.expiry_store, *ctx.selected_db, key1) {
                    expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key1);
                    SharedBytes::from(&[][..])
                } else {
                    let db = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db, key1);
                    let map = read_lock!(shard);
                    match map.get(key1) {
                        Some(ValueRef::String(v)) => v.to_shared(),
                        Some(_) => {
                            return Some(resp_error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            ));
                        }
                        None => SharedBytes::from(&[][..]),
                    }
                }
            };

            let s2: SharedBytes = {
                if is_expired(ctx.expiry_store, *ctx.selected_db, key2) {
                    expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key2);
                    SharedBytes::from(&[][..])
                } else {
                    let db = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db, key2);
                    let map = read_lock!(shard);
                    match map.get(key2) {
                        Some(ValueRef::String(v)) => v.to_shared(),
                        Some(_) => {
                            return Some(resp_error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            ));
                        }
                        None => SharedBytes::from(&[][..]),
                    }
                }
            };

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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
                return Some(resp_null_bulk());
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            match map.get(key) {
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let db_idx = *ctx.selected_db;

            // An expired key does not exist; the reply is 0, not nil.
            if is_expired(ctx.expiry_store, db_idx, key) {
                delete_key_if_expired(ctx.store, ctx.expiry_store, db_idx, key);
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
            map.remove(key);
            drop(map);
            clear_expiry(ctx.expiry_store, db_idx, key);
            crate::vector::maybe_unindex(ctx.index_store, key);
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
            // NX or XX, and one of EX, PX, EXAT, PXAT and KEEPTTL: each once,
            // anything else a syntax error, as Redis parses them.
            {
                let (mut condition, mut ttl) = (false, false);
                let mut j = pairs_end;
                while j < cmd.len() {
                    let opt = String::from_utf8_lossy(cmd[j]).to_ascii_uppercase();
                    match opt.as_str() {
                        "NX" | "XX" if !condition => condition = true,
                        "KEEPTTL" if !ttl => ttl = true,
                        "EX" | "PX" | "EXAT" | "PXAT" if !ttl && j + 1 < cmd.len() => {
                            ttl = true;
                            j += 1;
                        }
                        _ => return Some(resp_error("ERR syntax error")),
                    }
                    j += 1;
                }
            }

            // Parse options after the pairs
            let mut nx = false;
            let mut xx = false;
            let mut expiry: Option<SystemTime> = None;
            let mut keepttl = false;

            let mut i = pairs_end;
            while i < cmd.len() {
                match std::str::from_utf8(cmd[i])
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "NX" => {
                        nx = true;
                    }
                    "XX" => {
                        xx = true;
                    }
                    "KEEPTTL" => {
                        keepttl = true;
                    }
                    "EX" => {
                        i += 1;
                        let secs: u64 = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) if n > 0 => n,
                            _ => return Some(resp_error("ERR invalid expire time in MSETEX")),
                        };
                        expiry = Some(expiry_at_after(Duration::from_secs(secs)));
                    }
                    "PX" => {
                        i += 1;
                        let ms: u64 = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) if n > 0 => n,
                            _ => return Some(resp_error("ERR invalid expire time in MSETEX")),
                        };
                        expiry = Some(expiry_at_after(Duration::from_millis(ms)));
                    }
                    "EXAT" => {
                        i += 1;
                        let ts: u64 = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return Some(resp_error("ERR invalid EXAT timestamp")),
                        };
                        expiry = Some(expiry_unix_secs(ts));
                    }
                    "PXAT" => {
                        i += 1;
                        let ts_ms: u64 = match cmd
                            .get(i)
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return Some(resp_error("ERR invalid PXAT timestamp")),
                        };
                        expiry = Some(expiry_unix_millis(ts_ms));
                    }
                    _ => {}
                }
                i += 1;
            }

            if nx && xx {
                return Some(resp_error("ERR NX and XX options are mutually exclusive"));
            }

            // NX: all keys must be absent; XX: all keys must be present
            // Check first without writing (read pass), then write.
            let pairs = &cmd[2..pairs_end];

            if nx || xx {
                for pair in pairs.as_chunks::<2>().0 {
                    let key = match parse_utf8(pair[0]) {
                        Some(k) => k,
                        None => continue,
                    };
                    let exists = if is_expired(ctx.expiry_store, *ctx.selected_db, key) {
                        delete_key_if_expired(ctx.store, ctx.expiry_store, *ctx.selected_db, key);
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
                let key = match parse_utf8(pair[0]) {
                    Some(k) => k,
                    None => continue,
                };
                let value = KiviDBValue::String(SharedBytes::from(pair[1]));

                if keepttl {
                    // Just overwrite value, leave existing expiry intact
                    let db = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db, key);
                    let mut map = write_lock!(shard);
                    map.insert(Key::from(key), value);
                } else {
                    store_value(ctx.store, ctx.expiry_store, *ctx.selected_db, key, value);
                    if let Some(exp) = expiry {
                        set_expiry(ctx.expiry_store, *ctx.selected_db, key, exp);
                    }
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return resp_error("ERR key must be valid UTF-8"),
    };

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
                return resp_error("ERR invalid expire time in 'increx' command");
            }
            IncrexExpiry::Set(match unit {
                "EX" => expiry_at_after(Duration::from_secs(n as u64)),
                "PX" => expiry_at_after(Duration::from_millis(n as u64)),
                "EXAT" => UNIX_EPOCH + Duration::from_secs(n as u64),
                _ => UNIX_EPOCH + Duration::from_millis(n as u64),
            })
        }
    };

    // What is propagated is what this did, not the command: see below.
    effects_begin();

    // A key that has already expired behaves exactly like an absent one.
    lazy_expire_key(ctx.store, ctx.expiry_store, *ctx.selected_db, &key);

    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, &key);
    let mut map = write_lock!(shard);

    let existing: Option<&[u8]> = match map.get(key.as_str()) {
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
    let has_ttl = existed && {
        let shard = get_expiry_shard(ctx.expiry_store, *ctx.selected_db, &key);
        read_lock!(shard).contains_key(key.as_str())
    };
    let new_expiry = match expiry {
        IncrexExpiry::Set(at) if !(enx && has_ttl) => Some(at),
        _ => None,
    };
    let as_bytes = |s: &str| s.as_bytes().to_vec();

    // A TTL already past: the key goes, and is propagated as DEL, or UNLINK
    // with `lazyfree-lazy-expire` (Redis 8.10).
    if let Some(at) = new_expiry
        && at <= SystemTime::now()
    {
        if existed {
            map.remove(key.as_str());
            drop(map);
            clear_expiry(ctx.expiry_store, *ctx.selected_db, &key);
            let lazy = crate::commands::server::remembered_config("lazyfree-lazy-expire")
                .is_some_and(|v| v.eq_ignore_ascii_case("yes"));
            effect(vec![
                as_bytes(if lazy { "UNLINK" } else { "DEL" }),
                as_bytes(&key),
            ]);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "del", &key);
        }
        crate::stats::record_expired();
        return reply;
    }

    map.insert(
        Key::from(&*key),
        KiviDBValue::String(SharedBytes::from(new_repr.as_bytes())),
    );
    drop(map); // release the value lock before touching expiry_store

    // Propagated as a SET of the result, so float formatting cannot drift on
    // replicas or an AOF replay, with the TTL as it now is (Redis 8.10).
    let mut set = vec![as_bytes("SET"), as_bytes(&key), as_bytes(&new_repr)];
    let mut persisted = false;
    if persist {
        persisted = has_ttl;
        clear_expiry(ctx.expiry_store, *ctx.selected_db, &key);
    } else if let Some(at) = new_expiry {
        set_expiry(ctx.expiry_store, *ctx.selected_db, &key, at);
        let ms = at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis());
        set.extend([as_bytes("PXAT"), as_bytes(&ms.to_string())]);
    } else {
        set.push(as_bytes("KEEPTTL"));
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
