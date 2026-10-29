use crate::commands::ctx::Ctx;
use crate::commands::helpers::{
    ERR_INVALID_EXPIRE_PEXPIRE_CMD, ERR_INVALID_EXPIRE_PEXPIREAT_CMD, ERR_VALUE_NOT_INTEGER,
    absolute_when_ms_from_pexpire_ms, delete_key, delete_key_if_expired,
    expire_modifier_allows_set, expire_on_read, expiry_at_after, expiry_unix_millis,
    expiry_unix_secs, from_unix_ms, get_expiry, glob_match, is_expired, lazy_expire_on_read,
    parse_bytes_as_i128, parse_expire_modifier_flags, parse_utf8, set_expiry,
    system_time_from_unix_millis_i128, take_key, ttl_remaining_secs_ceil,
};
use crate::keyspace_events::{KS_GENERIC, notify};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What `OBJECT ENCODING` says of a value.
///
/// Redis keeps a small value in a compact form and a large one in a general
/// one, and the name of the form is something clients look at — to tell an
/// integer from a string, or to check that a structure is still in its
/// small form. Values here are not kept in those forms; the answer is the
/// form Redis would be keeping this value in, by Redis's default limits.
/// Whether Redis would keep `value` in a compact encoding (`intset`,
/// `listpack`). Redis scans those whole: `SSCAN`, `HSCAN` and `ZSCAN` of one
/// reply with every element and cursor 0, whatever the cursor and `COUNT`
/// say. Clients rely on it — BullMQ's dependency pagination expects a small
/// set in one page.
pub(crate) fn scans_whole(value: ValueRef<'_>) -> bool {
    matches!(encoding_of(value), "intset" | "listpack" | "listpackex")
}

fn encoding_of(value: ValueRef<'_>) -> &'static str {
    // An entry of a compact structure is at most this long.
    const SMALL: usize = 64;
    match value {
        ValueRef::String(bytes) => {
            let integer = bytes.len() <= 20
                && std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| text.parse::<i64>().ok())
                    .is_some_and(|n| n.to_string().as_bytes() == &bytes[..]);
            if integer {
                "int"
            } else if bytes.len() <= 44 {
                "embstr"
            } else {
                "raw"
            }
        }
        ValueRef::List(list) => {
            let list = list.read();
            // One node of a quicklist holds 8 kB.
            if list.iter().map(|element| element.len() + 11).sum::<usize>() <= 8192 {
                "listpack"
            } else {
                "quicklist"
            }
        }
        ValueRef::Set(set) => {
            let set = set.read();
            let integers = set.iter().all(|member| {
                std::str::from_utf8(member)
                    .ok()
                    .and_then(|text| text.parse::<i64>().ok())
                    .is_some_and(|n| n.to_string().as_bytes() == &member[..])
            });
            if integers && set.len() <= 512 {
                "intset"
            } else if set.len() <= 128 && set.iter().all(|member| member.len() <= SMALL) {
                "listpack"
            } else {
                "hashtable"
            }
        }
        ValueRef::SortedSet(zset) => {
            let zset = zset.read();
            if zset.members.len() <= 128 && zset.members.keys().all(|member| member.len() <= SMALL)
            {
                "listpack"
            } else {
                "skiplist"
            }
        }
        ValueRef::Hash(hash) => {
            let hash = hash.read();
            let small = hash.len() <= 512
                && hash
                    .iter()
                    .all(|(field, value)| field.len() <= SMALL && value.len() <= SMALL);
            if small { "listpack" } else { "hashtable" }
        }
        ValueRef::HashEx(hash) => {
            let hash = hash.read();
            let small = hash.fields.len() <= 512
                && hash
                    .fields
                    .iter()
                    .all(|(field, value)| field.len() <= SMALL && value.len() <= SMALL);
            if small { "listpackex" } else { "hashtable" }
        }
        ValueRef::Stream(_) => "stream",
        ValueRef::Array(_) => "sliced-array",
        _ => "raw",
    }
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    match name {
        "SCAN" => {
            // SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'scan' command",
                ));
            }
            let (cursor, options) =
                match crate::commands::helpers::scan_arguments(cmd, 1, &["TYPE"]) {
                    Ok(parsed) => parsed,
                    Err(e) => return Some(e),
                };

            // The keyspace is gone through a shard at a time, and each
            // shard in the order of its keys' positions (see
            // `helpers::scan_position`): the cursor is the shard, and the
            // position to go on from in it. Keys written or deleted while
            // a scan is under way do not move the others.
            const SHARD_SHIFT: u32 = 48;
            let db = &ctx.store[*ctx.selected_db];
            let num_shards = db.len() as u64;
            let mut shard_idx = cursor >> SHARD_SHIFT;
            let mut position = cursor & ((1u64 << SHARD_SHIFT) - 1);

            let mut keys: Vec<String> = Vec::new();
            let mut looked_at = 0usize;
            let mut next_cursor: u64 = 0;
            while shard_idx < num_shards {
                let map = read_lock!(db[shard_idx as usize]);
                let (page, next) = crate::commands::helpers::scan_page(
                    map.iter().map(|(key, value)| {
                        (
                            crate::commands::helpers::scan_position(key.as_bytes()),
                            (key, value),
                        )
                    }),
                    position,
                    options.count.saturating_sub(looked_at).max(1),
                    map.len(),
                    // A shard is a small part of the keyspace: it is gone
                    // through in a few pages.
                    16,
                );
                // `COUNT` is how many keys to look at, not how many to
                // return: the ones of another type count too.
                looked_at += page.len();
                keys.extend(
                    page.into_iter()
                        .filter(|(_, value)| {
                            options
                                .of_type
                                .as_deref()
                                .is_none_or(|t| value.type_name().eq_ignore_ascii_case(t))
                        })
                        .map(|(key, _)| key.to_string()),
                );
                drop(map);
                if next != 0 {
                    next_cursor = (shard_idx << SHARD_SHIFT) | next;
                    break;
                }
                shard_idx += 1;
                position = 0;
                if looked_at >= options.count {
                    if shard_idx < num_shards {
                        next_cursor = shard_idx << SHARD_SHIFT;
                    }
                    break;
                }
            }
            // Falling out of the loop with shard_idx == num_shards means
            // every shard was gone through: next_cursor stays 0 (done).

            keys.retain(|k| !lazy_expire_on_read(ctx.store, *ctx.selected_db, k));
            if let Some(ref pat) = options.pattern {
                keys.retain(|k| glob_match(pat, k));
            }

            let key_items: Vec<Vec<u8>> = keys.iter().map(|k| resp_bulk_str(k)).collect();

            Some(resp_array(vec![
                resp_bulk_str(&next_cursor.to_string()),
                resp_array(key_items),
            ]))
        }

        "KEYS" => {
            if cmd.len() != 2 {
                return Some(resp_error("ERR wrong number of arguments for cmd 'keys'"));
            }

            let pattern = match std::str::from_utf8(cmd[1]) {
                Ok(s) => s,
                Err(_) => return Some(resp_error("ERR invalid pattern")),
            };

            let db = &ctx.store[*ctx.selected_db];
            // Collect into a temporary — we need the count before we can
            // write the RESP array header, and we can't hold all shard locks
            // simultaneously. Collecting key slices (not copies) keeps this cheap.
            // Note: we can't return borrowed keys across the lock boundary,
            // so we collect owned Strings.
            let mut candidates: Vec<String> = Vec::new();
            for shard in db.iter() {
                let map = read_lock!(shard);
                candidates.extend(map.keys().map(|k| k.to_string()));
            }
            let mut matching: Vec<String> = Vec::new();
            for key in candidates {
                if lazy_expire_on_read(ctx.store, *ctx.selected_db, &key) {
                    continue;
                }
                if glob_match(pattern, &key) {
                    matching.push(key);
                }
            }

            let mut buf = Vec::with_capacity(16 + matching.len() * 24);
            buf.push(b'*');
            buf.extend_from_slice(matching.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for k in &matching {
                write_bulk(&mut buf, k.as_bytes());
            }
            Some(buf)
        }

        "OBJECT" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'object' command",
                ));
            }

            let subcommand = String::from_utf8_lossy(cmd[1]).to_ascii_uppercase();

            match subcommand.as_str() {
                "ENCODING" => {
                    if cmd.len() != 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'object|encoding' command",
                        ));
                    }
                    let key = match parse_utf8(cmd[2]) {
                        Some(k) => k,
                        None => return Some(resp_error("ERR key must be valid UTF-8")),
                    };
                    let db = *ctx.selected_db;
                    let gone = lazy_expire_on_read(ctx.store, db, key);
                    let map = read_lock!(store::get_shard(&ctx.store[db], key));
                    Some(match map.get(key).filter(|_| !gone) {
                        Some(value) => resp_bulk_str(encoding_of(value)),
                        None => resp_null_bulk(),
                    })
                }
                // What is known of a key's use is known while an eviction
                // policy that goes by it is selected. Redis, which always
                // keeps one or the other, refuses the one it is not keeping.
                "IDLETIME" | "FREQ" => {
                    if cmd.len() != 3 {
                        return Some(resp_error(&format!(
                            "ERR wrong number of arguments for 'object|{}' command",
                            subcommand.to_lowercase()
                        )));
                    }
                    let key = match parse_utf8(cmd[2]) {
                        Some(k) => k,
                        None => return Some(resp_error("ERR key must be valid UTF-8")),
                    };
                    let db = *ctx.selected_db;
                    let gone = lazy_expire_on_read(ctx.store, db, key);
                    if gone || !read_lock!(store::get_shard(&ctx.store[db], key)).contains_key(key) {
                        return Some(resp_null_bulk());
                    }
                    let lfu = crate::eviction::policy().is_lfu();
                    let recording = crate::eviction::recording();
                    Some(match (subcommand.as_str(), lfu) {
                        ("FREQ", false) => resp_error(
                            "ERR An LFU maxmemory policy is not selected, access frequency \
                             not tracked. Please note that when switching between policies at \
                             runtime LRU and LFU data will take some time to adjust.",
                        ),
                        ("IDLETIME", true) => resp_error(
                            "ERR An LFU maxmemory policy is selected, idle time not tracked. \
                             Please note that when switching between policies at runtime LRU \
                             and LFU data will take some time to adjust.",
                        ),
                        ("FREQ", true) => {
                            resp_integer(crate::eviction::frequency(db, key.as_bytes()) as i64)
                        }
                        _ if recording => {
                            resp_integer(crate::eviction::idle_seconds(db, key.as_bytes()) as i64)
                        }
                        // Nothing is recorded: with `noeviction` and the
                        // policies that do not go by use, keys are not
                        // watched.
                        _ => resp_integer(0),
                    })
                }
                "HELP" => Some(resp_array(
                    [
                        "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "ENCODING <key>",
                        "    Return the kind of internal representation used in order to store the value",
                        "    associated with a <key>.",
                        "FREQ <key>",
                        "    Return the access frequency index of the <key>. The returned integer is",
                        "    proportional to the logarithm of the recent access frequency of the key.",
                        "IDLETIME <key>",
                        "    Return the idle time of the <key>, that is the approximated number of",
                        "    seconds elapsed since the last access to the key.",
                        "REFCOUNT <key>",
                        "    Return the number of references of the value associated with the specified",
                        "    <key>.",
                        "HELP",
                        "    Print this help.",
                    ]
                    .iter()
                    .map(|line| resp_simple(line))
                    .collect(),
                )),
                "REFCOUNT" => {
                    // Always 1 — every KiviDBValue is behind an Arc but we expose 1
                    // because from Redis's perspective the "object" is referenced once.
                    // Returning the actual Arc::strong_count would expose internal details
                    // and would confuse clients that use REFCOUNT to detect shared objects.
                    if cmd.len() != 3 {
                        return Some(resp_error(
                            "ERR wrong number of arguments for 'object|refcount' command",
                        ));
                    }
                    let key = match parse_utf8(cmd[2]) {
                        Some(k) => k,
                        None => return Some(resp_error("ERR key must be valid UTF-8")),
                    };
                    if is_expired(ctx.store, *ctx.selected_db, key) {
                        expire_on_read(ctx.store, *ctx.selected_db, key);
                        return Some(resp_null_bulk());
                    }
                    let db = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db, key);
                    let map = read_lock!(shard);
                    if map.contains_key(key) {
                        Some(resp_integer(1))
                    } else {
                        Some(resp_null_bulk())
                    }
                }
                _ => Some(resp_error("ERR unknown subcommand")),
            }
        }

        "TTL" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'ttl' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                return Some(resp_integer(-2));
            }

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let expires = match read_lock!(shard).get_expiring(key) {
                None => return Some(resp_integer(-2)), // key does not exist
                Some((_, expires)) => expires,
            };
            match expires {
                None => Some(resp_integer(-1)), // key exists but no TTL
                Some(at) => match from_unix_ms(at)
                    .duration_since(crate::script_clock::wall_system_time_now())
                {
                    Ok(d) => Some(resp_integer(ttl_remaining_secs_ceil(d))),
                    Err(_) => Some(resp_integer(-2)),
                },
            }
        }

        "PERSIST" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'persist' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            if crate::commands::helpers::clear_expiry(ctx.store, *ctx.selected_db, key) {
                notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "persist", key);
                Some(resp_integer(1))
            } else {
                Some(resp_integer(0))
            }
        }

        "TYPE" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'type' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            // Expired keys are invisible — report as "none"
            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                return Some(resp_simple("none"));
            }

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            match map.get(key) {
                Some(v) => Some(resp_simple(v.type_name())),
                None => Some(resp_simple("none")),
            }
        }

        // ── PEXPIRE ───────────────────────────────────────────────────────────────────
        // Like EXPIRE but the TTL is given in milliseconds.
        // Returns 1 if the key exists and the TTL was set, 0 if the key doesn't exist.
        "PEXPIRE" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'pexpire' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let ms = match parse_bytes_as_i128(cmd[2]) {
                Some(m) => m,
                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
            };
            let flags = match parse_expire_modifier_flags(cmd.as_slice(), 3) {
                Ok(f) => f,
                Err(msg) => return Some(resp_error(&msg)),
            };

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            if !read_lock!(shard).contains_key(key) {
                return Some(resp_integer(0));
            }

            if ms == 0 {
                let removed = delete_key(ctx.store, *ctx.selected_db, key);
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

            let when_ms = match absolute_when_ms_from_pexpire_ms(ms) {
                Ok(w) => w,
                Err(e) => return Some(resp_error(e)),
            };
            let new_exp = match system_time_from_unix_millis_i128(when_ms) {
                Some(t) => t,
                None => return Some(resp_error(ERR_INVALID_EXPIRE_PEXPIRE_CMD)),
            };

            let current = get_expiry(ctx.store, *ctx.selected_db, key);

            if !expire_modifier_allows_set(&flags, current, new_exp) {
                return Some(resp_integer(0));
            }

            let now = crate::script_clock::wall_system_time_now();
            if new_exp <= now {
                let removed = delete_key(ctx.store, *ctx.selected_db, key);
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

            set_expiry(ctx.store, *ctx.selected_db, key, new_exp);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            Some(resp_integer(1))
        }

        // ── EXPIREAT ──────────────────────────────────────────────────────────────────
        // Like EXPIRE but the argument is an absolute Unix timestamp in seconds.
        "EXPIREAT" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'expireat' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let unix_secs = match parse_bytes_as_i128(cmd[2]) {
                Some(n) => n,
                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
            };
            if unix_secs < 0 {
                return Some(resp_error("ERR invalid expire time in EXPIREAT"));
            }
            let unix_secs_u64 = match u64::try_from(unix_secs) {
                Ok(n) => n,
                Err(_) => return Some(resp_error("ERR invalid expire time in EXPIREAT")),
            };
            let flag = cmd
                .get(3)
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.to_ascii_uppercase());

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            if !read_lock!(shard).contains_key(key) {
                return Some(resp_integer(0));
            }

            let new_exp = expiry_unix_secs(unix_secs_u64);
            let now = crate::script_clock::wall_system_time_now();
            if new_exp <= now {
                let removed = delete_key(ctx.store, *ctx.selected_db, key);
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

            if let Some(ref f) = flag {
                let current = get_expiry(ctx.store, *ctx.selected_db, key);
                let should_set = match f.as_str() {
                    "NX" => current.is_none(),
                    "XX" => current.is_some(),
                    // No time to live is an infinite one: nothing is
                    // greater, and anything is less.
                    "GT" => current.is_some_and(|c| new_exp > c),
                    "LT" => current.is_none_or(|c| new_exp < c),
                    _ => return Some(resp_error("ERR Unsupported option")),
                };
                if !should_set {
                    return Some(resp_integer(0));
                }
            }

            set_expiry(ctx.store, *ctx.selected_db, key, new_exp);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            Some(resp_integer(1))
        }

        // ── PEXPIREAT ─────────────────────────────────────────────────────────────────
        // Like EXPIREAT but the timestamp is in milliseconds.
        "PEXPIREAT" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'pexpireat' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let unix_ms = match parse_bytes_as_i128(cmd[2]) {
                Some(n) => n,
                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
            };
            let new_exp = match system_time_from_unix_millis_i128(unix_ms) {
                Some(t) => t,
                None => return Some(resp_error(ERR_INVALID_EXPIRE_PEXPIREAT_CMD)),
            };
            let flag = cmd
                .get(3)
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.to_ascii_uppercase());

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            if !read_lock!(shard).contains_key(key) {
                return Some(resp_integer(0));
            }

            let now = crate::script_clock::wall_system_time_now();
            if new_exp <= now {
                let removed = delete_key(ctx.store, *ctx.selected_db, key);
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

            if let Some(ref f) = flag {
                let current = get_expiry(ctx.store, *ctx.selected_db, key);
                let should_set = match f.as_str() {
                    "NX" => current.is_none(),
                    "XX" => current.is_some(),
                    // No time to live is an infinite one: nothing is
                    // greater, and anything is less.
                    "GT" => current.is_some_and(|c| new_exp > c),
                    "LT" => current.is_none_or(|c| new_exp < c),
                    _ => return Some(resp_error("ERR Unsupported option")),
                };
                if !should_set {
                    return Some(resp_integer(0));
                }
            }

            set_expiry(ctx.store, *ctx.selected_db, key, new_exp);
            notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
            Some(resp_integer(1))
        }

        // ── PTTL ──────────────────────────────────────────────────────────────────────
        // Like TTL but returns the remaining time in milliseconds.
        // Returns -1 if no TTL, -2 if key doesn't exist.
        "PTTL" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'pttl' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                return Some(resp_integer(-2));
            }

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let expires = match read_lock!(shard).get_expiring(key) {
                None => return Some(resp_integer(-2)),
                Some((_, expires)) => expires,
            };
            match expires {
                None => Some(resp_integer(-1)),
                Some(at) => match from_unix_ms(at)
                    .duration_since(crate::script_clock::wall_system_time_now())
                {
                    Ok(d) => Some(resp_integer(d.as_millis() as i64)),
                    Err(_) => Some(resp_integer(-2)),
                },
            }
        }

        "RENAME" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'rename' command",
                ));
            }
            let src = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR invalid key")),
            };
            let dst = match parse_utf8(cmd[2]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR invalid key")),
            };

            let db = *ctx.selected_db;
            let db_ref = &ctx.store[db];

            // Read + remove from src, with its time to live
            let (value, expiry) = {
                let shard = store::get_shard(db_ref, &src);
                let mut map = write_lock!(shard);
                match map.remove(src.as_str()) {
                    Some(v) => {
                        let expiry = v.expires();
                        (v.into_value(), expiry)
                    }
                    None => return Some(resp_error("ERR no such key")),
                }
            };

            // Write to dst, the time to live going with the value: the one
            // dst had, if any, goes.
            {
                let shard = store::get_shard(db_ref, &dst);
                let mut map = write_lock!(shard);
                map.insert_expiring(Key::from(&*dst), value, expiry);
            }
            notify(ctx.pubsub, db, KS_GENERIC, "rename_from", &src);
            notify(ctx.pubsub, db, KS_GENERIC, "rename_to", &dst);
            Some(resp_simple("OK"))
        }

        "RENAMENX" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'renamenx' command",
                ));
            }
            let src = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR invalid key")),
            };
            let dst = match parse_utf8(cmd[2]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR invalid key")),
            };

            let db = *ctx.selected_db;
            let db_ref = &ctx.store[db];

            // Check src exists
            let (src_value, expiry) = {
                let shard = store::get_shard(db_ref, &src);
                let map = read_lock!(shard);
                match map.get_expiring(src.as_str()) {
                    Some((v, expiry)) => (v.to_value(), expiry),
                    None => return Some(resp_error("ERR no such key")),
                }
            };

            // Check dst does NOT exist
            {
                let shard = store::get_shard(db_ref, &dst);
                let map = read_lock!(shard);
                if map.contains_key(dst.as_str()) {
                    return Some(resp_integer(0));
                }
            }

            // Remove src, write dst
            {
                let shard = store::get_shard(db_ref, &src);
                write_lock!(shard).remove(src.as_str());
            }
            {
                let shard = store::get_shard(db_ref, &dst);
                write_lock!(shard).insert_expiring(Key::from(&*dst), src_value, expiry);
            }
            notify(ctx.pubsub, db, KS_GENERIC, "rename_from", &src);
            notify(ctx.pubsub, db, KS_GENERIC, "rename_to", &dst);
            Some(resp_integer(1))
        }

        "COPY" => {
            // COPY source destination [DB destination-db] [REPLACE]
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'copy' command",
                ));
            }
            let src_key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR invalid key")),
            };
            let dst_key = match parse_utf8(cmd[2]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR invalid key")),
            };

            let src_db = *ctx.selected_db;
            let mut dst_db = src_db;
            let mut replace = false;

            let mut i = 3;
            while i < cmd.len() {
                let opt = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
                match opt.as_str() {
                    "DB" => {
                        i += 1;
                        dst_db = match parse_utf8(cmd.get(i).copied().unwrap_or(b""))
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => {
                                return Some(resp_error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        };
                    }
                    "REPLACE" => replace = true,
                    _ => return Some(resp_error("ERR syntax error")),
                }
                i += 1;
            }

            if dst_db >= ctx.store.len() {
                return Some(resp_error("ERR invalid DB index"));
            }

            // Read source value, and its time to live
            let (value, expiry) = {
                let db_ref = &ctx.store[src_db];
                let shard = store::get_shard(db_ref, &src_key);
                let map = read_lock!(shard);
                match map.get_expiring(src_key.as_str()) {
                    // A copy of its own, not another name for the same
                    // value. See `deep_clone`.
                    Some((v, expiry)) => (v.deep_clone(), expiry),
                    None => return Some(resp_integer(0)),
                }
            };

            // Check destination
            let dst_db_ref = &ctx.store[dst_db];
            {
                let shard = store::get_shard(dst_db_ref, &dst_key);
                let map = read_lock!(shard);
                if map.contains_key(dst_key.as_str()) && !replace {
                    return Some(resp_integer(0));
                }
            }

            // Write to destination, with the source's time to live (the
            // key replaced may have had one: the copy has the source's, or
            // none).
            {
                let shard = store::get_shard(dst_db_ref, &dst_key);
                write_lock!(shard).insert_expiring(Key::from(&*dst_key), value, expiry);
            }
            notify(ctx.pubsub, dst_db, KS_GENERIC, "copy_to", &dst_key);
            Some(resp_integer(1))
        }

        // `UNLINK` is `DEL` that frees the value somewhere else. Values here
        // are freed where they are dropped, so the two are one command.
        "DEL" | "UNLINK" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'del' command",
                ));
            }

            let mut removed = 0i64;
            // `UNLINK` frees a large value in the background (`lazyfree`);
            // `DEL` frees it here, as Redis does with `lazyfree-lazy-user-del no`.
            let unlink = cmd[0].eq_ignore_ascii_case(b"UNLINK");

            for k in cmd.iter().skip(1) {
                let key = match parse_utf8(k) {
                    Some(k) => k,
                    None => continue,
                };
                // Redis: expireIfNeeded deletes volatile keys first; that removal does not add
                // to the DEL count (see redis `delGenericCommand`).
                if is_expired(ctx.store, *ctx.selected_db, key) {
                    if delete_key_if_expired(ctx.store, *ctx.selected_db, key) {
                        crate::vector::maybe_unindex(ctx.index_store, key);
                        crate::vector::maybe_request_compaction(
                            ctx.index_store,
                            ctx.compaction_tx,
                            key,
                        );
                    }
                    continue;
                }
                let deleted = if unlink {
                    take_key(ctx.store, *ctx.selected_db, key)
                        .map(crate::lazyfree::release)
                        .is_some()
                } else {
                    delete_key(ctx.store, *ctx.selected_db, key)
                };
                if deleted {
                    crate::vector::maybe_unindex(ctx.index_store, key);
                    crate::vector::maybe_request_compaction(
                        ctx.index_store,
                        ctx.compaction_tx,
                        key,
                    );
                    notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "del", key);
                    removed += 1;
                } else if ctx.watch_registry.has_watchers() {
                    // There was nothing to delete: a client watching the key
                    // has seen no change.
                    crate::watch::unchanged(*ctx.selected_db, key);
                }
            }

            Some(resp_integer(removed))
        }

        "MOVE" => {
            // MOVE key db — move key to another database
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'move' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR invalid key")),
            };
            let target_db: usize = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR invalid DB index")),
            };

            let src_db = *ctx.selected_db;

            if target_db >= ctx.store.len() || target_db == src_db {
                return Some(resp_integer(0));
            }

            // Read + remove from source, with its time to live
            let (value, expiry) = {
                let db_ref = &ctx.store[src_db];
                let shard = store::get_shard(db_ref, &key);
                let mut map = write_lock!(shard);
                match map.remove(key.as_str()) {
                    Some(v) => {
                        let expiry = v.expires();
                        (v.into_value(), expiry)
                    }
                    None => return Some(resp_integer(0)),
                }
            };

            // If key exists in target, put it back in source and return 0
            let dst_db_ref = &ctx.store[target_db];
            {
                let shard = store::get_shard(dst_db_ref, &key);
                let map = read_lock!(shard);
                if map.contains_key(key.as_str()) {
                    // Restore to source
                    drop(map);
                    let src_shard = store::get_shard(&ctx.store[src_db], &key);
                    write_lock!(src_shard).insert_expiring(Key::from(key), value, expiry);
                    return Some(resp_integer(0));
                }
            }

            // Write to target
            {
                let shard = store::get_shard(dst_db_ref, &key);
                write_lock!(shard).insert_expiring(Key::from(&*key), value, expiry);
            }

            Some(resp_integer(1))
        }

        "SWAPDB" => {
            // SWAPDB db1 db2 — atomically swap two databases
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'swapdb' command",
                ));
            }
            let db1: usize = match parse_utf8(cmd[1]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR invalid DB index")),
            };
            let db2: usize = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR invalid DB index")),
            };

            if db1 >= ctx.store.len() || db2 >= ctx.store.len() {
                return Some(resp_error("ERR invalid DB index"));
            }

            if db1 == db2 {
                return Some(resp_simple("OK"));
            }

            // Swap all shards between db1 and db2
            // We lock each shard pair in ascending index order to avoid deadlock
            let db1_ref = &ctx.store[db1];
            let db2_ref = &ctx.store[db2];

            for (shard1, shard2) in db1_ref.iter().zip(db2_ref.iter()) {
                let mut map1 = write_lock!(shard1);
                let mut map2 = write_lock!(shard2);
                std::mem::swap(&mut *map1, &mut *map2);
            }

            Some(resp_simple("OK"))
        }

        "RANDOMKEY" => {
            // A key picked at random: from a shard picked at random, and,
            // if that one is empty, from the ones after it. A key whose
            // time has run out is not a key: it is removed, and another
            // picked.
            let db = *ctx.selected_db;
            let shards = ctx.store[db].len();
            let mut expired = None;
            for _ in 0..64 {
                let start = rand::random::<usize>() % shards.max(1);
                let picked = (0..shards).find_map(|step| {
                    let map = read_lock!(ctx.store[db][(start + step) % shards]);
                    map.random_entry(rand::random::<usize>)
                        .map(|(key, _)| key.to_string())
                });
                match picked {
                    None => break,
                    Some(key) if lazy_expire_on_read(ctx.store, db, &key) => {
                        expired = Some(key);
                    }
                    Some(key) => return Some(resp_bulk_str(&key)),
                }
            }
            // While writes are paused an expired key is not removed, so
            // every pick could land on one again: as in Redis, the answer is
            // then one of them, not nil.
            match expired {
                Some(key) if crate::pause::expiry_deferred() => Some(resp_bulk_str(&key)),
                _ => Some(resp_null_bulk()),
            }
        }

        "TOUCH" => {
            // TOUCH key [key ...]
            // Returns the count of keys that exist (expired keys count as absent).
            // What it is for — marking the keys as used — the dispatcher
            // does for every command's keys: see `eviction::used`.
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'touch' command",
                ));
            }
            let db = &ctx.store[*ctx.selected_db];
            let mut count = 0i64;
            for raw_key in cmd.iter().skip(1) {
                let key = match parse_utf8(raw_key) {
                    Some(k) => k,
                    None => continue,
                };
                if is_expired(ctx.store, *ctx.selected_db, key) {
                    expire_on_read(ctx.store, *ctx.selected_db, key);
                    continue;
                }
                let shard = store::get_shard(db, key);
                if read_lock!(shard).contains_key(key) {
                    count += 1;
                }
            }
            Some(resp_integer(count))
        }

        "DUMP" => {
            // DUMP key
            // Serializes the value at key to a binary string in KiviDB's snapshot
            // format, wrapped in a 10-byte envelope (2-byte version + 8-byte CRC64).
            // Returns nil if the key does not exist.
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'dump' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                return Some(resp_null_bulk());
            }
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            let value = match map.get(key) {
                Some(v) => v.to_value(),
                None => return Some(resp_null_bulk()),
            };
            drop(map);

            // The value in our snapshot encoding, followed by a 10-byte
            // trailer: 2-byte version + CRC64 of the encoding.
            match crate::snapshot::dump_payload(&value) {
                Ok(out) => Some(resp_bulk(&out)),
                Err(e) => Some(resp_error(&format!("ERR serialization failed: {e}"))),
            }
        }

        "RESTORE" => {
            // RESTORE key ttl serialized-value [REPLACE] [ABSTTL]
            // [IDLETIME seconds] [FREQ frequency]
            if cmd.len() < 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'restore' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let ttl_ms: u64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR invalid TTL value")),
            };
            let payload = &cmd[3];

            // Parse options
            let mut replace = false;
            let mut absttl = false;
            let mut idle: Option<u64> = None;
            let mut freq: Option<u8> = None;
            let mut i = 4usize;
            while i < cmd.len() {
                let option = parse_utf8(cmd[i]).map(|s| s.to_ascii_uppercase());
                let value = cmd.get(i + 1).and_then(|v| parse_utf8(v));
                match option.as_deref() {
                    Some("REPLACE") => replace = true,
                    Some("ABSTTL") => absttl = true,
                    Some("IDLETIME") if value.is_some() && freq.is_none() => {
                        match value.and_then(|v| v.parse::<i64>().ok()) {
                            Some(seconds) if seconds >= 0 => idle = Some(seconds as u64),
                            _ => {
                                return Some(resp_error(
                                    "ERR Invalid IDLETIME value, must be >= 0",
                                ));
                            }
                        }
                        i += 1;
                    }
                    Some("FREQ") if value.is_some() && idle.is_none() => {
                        match value.and_then(|v| v.parse::<i64>().ok()) {
                            Some(count @ 0..=255) => freq = Some(count as u8),
                            _ => {
                                return Some(resp_error(
                                    "ERR Invalid FREQ value, must be >= 0 and <= 255",
                                ));
                            }
                        }
                        i += 1;
                    }
                    Some("IDLETIME") | Some("FREQ") if value.is_some() => {
                        return Some(resp_error(
                            "ERR IDLETIME and FREQ cannot be set at the same time.",
                        ));
                    }
                    _ => return Some(resp_error("ERR syntax error")),
                }
                i += 1;
            }

            // Redis checks BUSYKEY before validating the serialized payload (invalid DUMP must
            // still report an existing key).
            let db_idx = *ctx.selected_db;
            if !replace {
                let db = &ctx.store[db_idx];
                let shard = store::get_shard(db, &key);
                let map = read_lock!(shard);
                if map.contains_key(key.as_str()) {
                    return Some(resp_error("BUSYKEY Target key name already exists."));
                }
            }

            // Verify the trailer (version + checksum) and decode.
            let value = match crate::snapshot::restore_payload_checked(payload) {
                Ok(v) => v,
                Err(e) => return Some(resp_error(e.message())),
            };

            // A restored series has to be findable by its labels, the same as
            // one made with TS.CREATE.
            let ts_labels: Vec<(String, String)> = match &value {
                KiviDBValue::TimeSeries(series) => series
                    .read()
                    .labels
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                _ => Vec::new(),
            };

            // Store it, with its time to live
            let expires_at = (ttl_ms > 0).then(|| {
                if absttl {
                    expiry_unix_millis(ttl_ms)
                } else {
                    expiry_at_after(Duration::from_millis(ttl_ms))
                }
            });
            crate::commands::helpers::store_value_expiring(
                ctx.store, db_idx, &key, value, expires_at,
            );
            if !ts_labels.is_empty() {
                let mut index = write_lock!(ctx.ts_index);
                for (label, label_value) in ts_labels {
                    index
                        .entry(format!("{label}={label_value}"))
                        .or_default()
                        .insert(key.clone());
                }
            }

            if crate::eviction::recording() {
                crate::eviction::restored(ctx.store, db_idx, key.as_bytes(), idle, freq);
            }

            crate::keyspace_events::notify(
                ctx.pubsub,
                db_idx,
                crate::keyspace_events::KS_GENERIC,
                "restore",
                &key,
            );
            Some(resp_simple("OK"))
        }

        "SORT" | "SORT_RO" => {
            // SORT key [BY pattern] [LIMIT offset count] [GET pattern [...]]
            //          [ASC|DESC] [ALPHA] [STORE destination]
            // SORT_RO is identical but rejects STORE.
            let is_ro = name == "SORT_RO";
            if cmd.len() < 2 {
                return Some(resp_error(&format!(
                    "ERR wrong number of arguments for '{}' command",
                    name.to_ascii_lowercase()
                )));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                // SORT on missing key returns empty array, or 0 if STORE
                return Some(resp_array(vec![]));
            }

            // ── Parse options ──────────────────────────────────────────────────
            let mut alpha = false;
            let mut descending = false;
            let mut limit_offset: usize = 0;
            let mut limit_count: Option<usize> = None;
            let mut store_dst: Option<String> = None;
            // BY and GET patterns are parsed but we only support the simple
            // nosort/GET # case; others return ERR as documented.
            let mut by_pattern: Option<String> = None;
            let mut get_patterns: Vec<String> = Vec::new();

            let mut i = 2usize;
            while i < cmd.len() {
                let tok = match parse_utf8(cmd[i]) {
                    Some(s) => s.to_ascii_uppercase(),
                    None => return Some(resp_error("ERR syntax error")),
                };
                match tok.as_str() {
                    "ALPHA" => alpha = true,
                    "ASC" => descending = false,
                    "DESC" => descending = true,
                    "LIMIT" => {
                        if i + 2 >= cmd.len() {
                            return Some(resp_error("ERR syntax error"));
                        }
                        // Before the first element is the first element.
                        limit_offset =
                            match parse_utf8(cmd[i + 1]).and_then(|s| s.parse::<i64>().ok()) {
                                Some(n) => n.max(0) as usize,
                                None => {
                                    return Some(resp_error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            };
                        limit_count =
                            match parse_utf8(cmd[i + 2]).and_then(|s| s.parse::<i64>().ok()) {
                                Some(n) if n >= 0 => Some(n as usize),
                                Some(_) => None, // -1 means all
                                None => {
                                    return Some(resp_error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            };
                        i += 2;
                    }
                    "STORE" => {
                        if is_ro {
                            return Some(resp_error("ERR syntax error"));
                        }
                        i += 1;
                        store_dst = match cmd.get(i).and_then(|b| parse_utf8(b)) {
                            Some(s) => Some(s.to_string()),
                            None => return Some(resp_error("ERR syntax error")),
                        };
                    }
                    "BY" => {
                        i += 1;
                        by_pattern = match cmd.get(i).and_then(|b| parse_utf8(b)) {
                            Some(s) => Some(s.to_string()),
                            None => return Some(resp_error("ERR syntax error")),
                        };
                    }
                    "GET" => {
                        i += 1;
                        match cmd.get(i).and_then(|b| parse_utf8(b)) {
                            Some(s) => get_patterns.push(s.to_string()),
                            None => return Some(resp_error("ERR syntax error")),
                        };
                    }
                    _ => return Some(resp_error("ERR syntax error")),
                }
                i += 1;
            }

            // ── Collect elements ───────────────────────────────────────────────
            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let map = read_lock!(shard);
            let missing = !map.contains_key(key);

            let mut elements: Vec<Arc<[u8]>> = match map.get(key) {
                None => vec![], // ← don't return, just give empty vec
                Some(ValueRef::List(l)) => l.read().iter().cloned().collect(),
                Some(ValueRef::Set(s)) => s.read().iter().cloned().collect(),
                Some(ValueRef::SortedSet(z)) => {
                    z.read().by_score.keys().map(|(_, m)| m.clone()).collect()
                }
                Some(_) => {
                    return Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            };
            // A sorted set has an order of its own, which `BY nosort` keeps
            // — backwards, with `DESC`.
            let in_score_order = matches!(map.get(key), Some(ValueRef::SortedSet(_)));
            drop(map);

            let constant_by_lex = by_pattern
                .as_deref()
                .map(|p| !p.eq_ignore_ascii_case("nosort") && !p.contains('*'))
                .unwrap_or(false);

            // ── BY <pattern>: resolve sort keys via external key lookups ──────────────
            //
            // Pattern syntax: "weight_*" → replace * with the element value.
            // For Hash fields: "obj_*->field" → look up key obj_{elem}, read field.
            // Missing keys sort as 0.0 (numeric) or "" (alpha), matching Redis.
            //
            // "nosort" and "NOSORT" skip this entirely — elements keep insertion order.

            let sort_keys: Vec<Option<f64>> = if constant_by_lex {
                vec![]
            } else {
                match by_pattern.as_deref() {
                    None => {
                        // No BY
                        vec![None; elements.len()]
                    }

                    Some(p) if p.eq_ignore_ascii_case("nosort") => {
                        // BY nosort
                        vec![None; elements.len()]
                    }

                    Some(pat) => {
                        let db = &ctx.store[*ctx.selected_db];

                        elements
                            .iter()
                            .map(|elem| {
                                let elem_str = std::str::from_utf8(elem).unwrap_or("");

                                if let Some(arrow) = pat.find("->") {
                                    // Hash field pattern
                                    let key_pat = &pat[..arrow];
                                    let field = &pat[arrow + 2..];
                                    let lookup_key = key_pat.replace('*', elem_str);

                                    let shard = store::get_shard(db, &lookup_key);
                                    let map = read_lock!(shard);

                                    match map.get(lookup_key.as_str()) {
                                        Some(ValueRef::Hash(h)) => {
                                            let inner = h.read();
                                            inner
                                                .get(field)
                                                .and_then(|v| std::str::from_utf8(v).ok())
                                                .and_then(|s| s.parse::<f64>().ok())
                                        }
                                        _ => None,
                                    }
                                } else {
                                    // Plain string pattern
                                    let lookup_key = pat.replace('*', elem_str);
                                    let shard = store::get_shard(db, &lookup_key);
                                    let map = read_lock!(shard);

                                    match map.get(lookup_key.as_str()) {
                                        Some(ValueRef::String(s)) => std::str::from_utf8(&s)
                                            .ok()
                                            .and_then(|v| v.parse::<f64>().ok()),
                                        _ => None,
                                    }
                                }
                            })
                            .collect()
                    }
                }
            };

            if missing {
                if let Some(ref dst) = store_dst {
                    let dst_shard = store::get_shard(&ctx.store[*ctx.selected_db], dst);
                    write_lock!(dst_shard).remove(dst.as_str());
                    return Some(resp_integer(0));
                }
                return Some(resp_array(vec![]));
            }

            // ── Sort ───────────────────────────────────────────────────────────
            let is_nosort = by_pattern
                .as_deref()
                .map(|p| p.eq_ignore_ascii_case("nosort"))
                .unwrap_or(false);

            if is_nosort && in_score_order && descending {
                elements.reverse();
            } else if constant_by_lex {
                if descending {
                    elements.sort_unstable_by(|a, b| b.cmp(a));
                } else {
                    elements.sort_unstable();
                }
            } else if !is_nosort {
                // Zip elements with their resolved sort keys, sort the pairs, unzip.
                // This is the idiomatic Rust way to sort with external keys while
                // keeping indices aligned — no unsafe, no index juggling.
                let has_external_keys = sort_keys.iter().any(|k| k.is_some());

                if has_external_keys {
                    // External BY pattern — sort by resolved numeric keys.
                    // Missing keys (None) sort as 0.0 matching Redis behaviour.
                    let mut paired: Vec<(f64, Arc<[u8]>)> = sort_keys
                        .into_iter()
                        .zip(elements)
                        .map(|(k, e)| (k.unwrap_or(0.0), e))
                        .collect();

                    if descending {
                        paired.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
                    } else {
                        paired.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                    }
                    elements = paired.into_iter().map(|(_, e)| e).collect();
                } else if alpha {
                    // ALPHA sort by element value
                    if descending {
                        elements.sort_unstable_by(|a, b| b.cmp(a));
                    } else {
                        elements.sort_unstable();
                    }
                } else {
                    // Numeric sort by element value (default)
                    // Use a Cell to signal parse failure from inside the closure —
                    // sort_unstable_by requires Ord so we can't return a Result.
                    let sort_err = std::cell::Cell::new(false);
                    elements.sort_unstable_by(|a, b| {
                        let na = std::str::from_utf8(a)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok());
                        let nb = std::str::from_utf8(b)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok());
                        match (na, nb) {
                            (Some(x), Some(y)) => {
                                if descending {
                                    y.total_cmp(&x)
                                } else {
                                    x.total_cmp(&y)
                                }
                            }
                            _ => {
                                sort_err.set(true);
                                std::cmp::Ordering::Equal
                            }
                        }
                    });
                    if sort_err.get() {
                        return Some(resp_error(
                            "ERR One or more scores can't be converted into double",
                        ));
                    }
                }
            }

            // ── Apply LIMIT ────────────────────────────────────────────────────
            let total = elements.len();
            let start = limit_offset.min(total);
            let elements: Vec<Arc<[u8]>> = match limit_count {
                Some(count) => elements.into_iter().skip(start).take(count).collect(),
                None => elements.into_iter().skip(start).collect(),
            };

            // Build raw output values (before RESP encoding) — used for both
            // the client response AND STORE.
            let raw_output: Vec<Vec<u8>> = if get_patterns.is_empty() {
                elements.iter().map(|e| e.to_vec()).collect()
            } else {
                let mut out = Vec::with_capacity(elements.len() * get_patterns.len());
                for elem in &elements {
                    let elem_str = std::str::from_utf8(elem).unwrap_or("");
                    for pat in &get_patterns {
                        if pat == "#" {
                            out.push(elem.to_vec());
                        } else if let Some(arrow) = pat.find("->") {
                            let key_pat = &pat[..arrow];
                            let field = &pat[arrow + 2..];
                            let lookup_key = key_pat.replace('*', elem_str);

                            let shard = store::get_shard(db, &lookup_key);
                            let map = read_lock!(shard);
                            // Hash field branch — push raw bytes, not resp_bulk
                            let val = match map.get(lookup_key.as_str()) {
                                Some(ValueRef::Hash(h)) => {
                                    let inner = h.read();
                                    inner.get(field).map(|v| v.to_vec()) // raw bytes
                                }
                                _ => None,
                            };
                            // None = missing → push empty Vec as sentinel for nil
                            out.push(val.unwrap_or_default());
                        } else {
                            let lookup_key = pat.replace('*', elem_str);
                            let shard = store::get_shard(db, &lookup_key);
                            let map = read_lock!(shard);
                            let val = match map.get(lookup_key.as_str()) {
                                Some(ValueRef::String(s)) => Some(s.to_vec()), // raw bytes
                                _ => None,
                            };
                            out.push(val.unwrap_or_default());
                        }
                    }
                }
                out
            };

            // ── STORE → write result as List, return count ─────────────────────
            if let Some(ref dst) = store_dst {
                let new_list: std::collections::VecDeque<Arc<[u8]>> =
                    raw_output.iter().map(|v| Arc::from(v.as_slice())).collect();
                let count = new_list.len() as i64;
                let db_idx = *ctx.selected_db;
                let dst_shard = store::get_shard(&ctx.store[db_idx], dst);
                let mut dst_map = write_lock!(dst_shard);
                if new_list.is_empty() {
                    dst_map.remove(dst.as_str());
                } else {
                    dst_map.insert(
                        Key::from(dst),
                        KiviDBValue::List(std::sync::Arc::new(parking_lot::RwLock::new(new_list))),
                    );
                }
                return Some(resp_integer(count));
            }
            // SORT ... GET pattern yields a nil per unresolved lookup, nested
            // inside the result array.
            let nil = resp_null_for(crate::commands::handler::effective_resp_for_reply(ctx));
            let resp_output: Vec<Vec<u8>> = raw_output
                .iter()
                .map(|v| {
                    if v.is_empty() {
                        nil.clone()
                    } else {
                        resp_bulk(v)
                    }
                })
                .collect();
            Some(resp_array(resp_output))
        }

        // ── EXPIRETIME ────────────────────────────────────────────────────────────────
        // Returns the absolute Unix timestamp (seconds) at which the key will expire.
        //   -1 if the key exists but has no TTL
        //   -2 if the key does not exist or is already expired
        //
        // Stored expiry is absolute wall time (`SystemTime`).
        "EXPIRETIME" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'expiretime' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            // Lazy-evict expired keys, same as TTL/PTTL do.
            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                return Some(resp_integer(-2));
            }

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let exp = match read_lock!(shard).get_expiring(key) {
                None => return Some(resp_integer(-2)),
                Some((_, None)) => return Some(resp_integer(-1)),
                Some((_, Some(at))) => from_unix_ms(at),
            };

            Some(resp_integer(system_time_to_unix(exp, TimeUnit::Secs)))
        }

        // ── PEXPIRETIME ───────────────────────────────────────────────────────────────
        // Same as EXPIRETIME but returns the Unix timestamp in milliseconds.
        "PEXPIRETIME" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'pexpiretime' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
                return Some(resp_integer(-2));
            }

            let db = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db, key);
            let exp = match read_lock!(shard).get_expiring(key) {
                None => return Some(resp_integer(-2)),
                Some((_, None)) => return Some(resp_integer(-1)),
                Some((_, Some(at))) => from_unix_ms(at),
            };

            Some(resp_integer(system_time_to_unix(exp, TimeUnit::Millis)))
        }

        _ => None, // ← tells the router this module doesn't handle it
    }
}

// ── Time conversion helper ────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum TimeUnit {
    Secs,
    Millis,
}

fn system_time_to_unix(exp: SystemTime, unit: TimeUnit) -> i64 {
    let d = exp.duration_since(UNIX_EPOCH).unwrap_or_default();
    match unit {
        TimeUnit::Secs => d.as_secs() as i64,
        TimeUnit::Millis => d.as_millis() as i64,
    }
}
