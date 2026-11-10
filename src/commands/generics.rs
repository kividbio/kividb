use crate::commands::ctx::Ctx;
use crate::commands::helpers::{
    ERR_INVALID_EXPIRE_PEXPIRE_CMD, ERR_INVALID_EXPIRE_PEXPIREAT_CMD, ERR_VALUE_NOT_INTEGER,
    ExpireModifierFlags, absolute_when_ms_from_pexpire_ms, delete_key, delete_key_if_expired,
    expire_modifier_allows_set, expire_on_read, expiry_at_after, expiry_unix_millis, from_unix_ms,
    get_expiry, is_expired, lazy_expire_on_read, ms_to_nearest_secs, parse_bytes_as_i128,
    parse_expire_modifier_flags, parse_utf8, set_expiry, system_time_from_unix_millis_i128,
    take_key, ttl_remaining_ms,
};
use crate::keyspace_events::{KS_GENERIC, notify};
use crate::resp::write::*;
use crate::store;
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

pub(crate) fn encoding_of(value: ValueRef<'_>) -> &'static str {
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

/// The end of `EXPIRE`, `PEXPIRE`, `EXPIREAT` and `PEXPIREAT`, once each has
/// read its arguments: `new_abs` is the deadline asked for, and `deletes`
/// whether it is one that deletes the key now (a time to live of zero or
/// less, a moment that has come).
///
/// The `NX` / `XX` / `GT` / `LT` condition is weighed first, as in Redis.
/// The deleting deadline was handled ahead of it, so `EXPIRE k 0 NX` on a
/// key with a time to live deleted it and answered 1, where Redis answers 0
/// and leaves the key as it was (#270). An answer of 0 propagates nothing
/// (`script_feed::expire_feed`); a deletion goes as a `DEL`.
///
/// While the AOF is replayed (`expiry::replaying`) a moment that has come
/// deletes nothing: it is set, as Redis sets it while it loads. The log's
/// `PEXPIREAT k t` was written while `t` was ahead, and the writes after it
/// were made to the key that had it: deleted here, the replayed `HSET k …`
/// that followed made a new key of what came after, with no time to live,
/// for good. Set, the key keeps its deadline, the writes go into it, and
/// expiry takes it out once the load is done, with its `DEL` logged.
pub(crate) fn apply_expire(
    ctx: &Ctx,
    key: &[u8],
    new_abs: SystemTime,
    deletes: bool,
    flags: &ExpireModifierFlags,
) -> Vec<u8> {
    let deletes = deletes && !crate::expiry::replaying();
    let db = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db, key);
    if !read_lock!(shard).contains_key(key) {
        return resp_integer(0);
    }
    let current = get_expiry(ctx.store, *ctx.selected_db, key);
    if !expire_modifier_allows_set(flags, current, new_abs) {
        return resp_integer(0);
    }
    if deletes {
        let removed = delete_key(ctx.store, *ctx.selected_db, key);
        if removed {
            crate::vector::maybe_unindex(ctx.index_store, *ctx.selected_db, key);
            crate::vector::maybe_request_compaction(ctx.index_store, ctx.compaction_tx, key);
        }
        return resp_integer(if removed { 1 } else { 0 });
    }
    set_expiry(ctx.store, *ctx.selected_db, key, new_abs);
    notify(ctx.pubsub, *ctx.selected_db, KS_GENERIC, "expire", key);
    resp_integer(1)
}

/// A `SORT` `BY` or `GET` pattern taken apart (`sort_pattern_parts`).
struct SortPattern<'a> {
    /// Where its first `*` is.
    star: usize,
    /// What follows the `*` in the key's name.
    suffix: &'a [u8],
    /// The hash field it names after `->`, if any.
    field: Option<&'a [u8]>,
}

/// A `SORT` `BY` or `GET` pattern taken apart. `None` when it has no `*`:
/// it names no key.
fn sort_pattern_parts(pat: &[u8]) -> Option<SortPattern<'_>> {
    let star = memchr::memchr(b'*', pat)?;
    let rest = &pat[star + 1..];
    let (suffix, field) = match memchr::memmem::find(rest, b"->") {
        Some(at) if at + 2 < rest.len() => (&rest[..at], Some(&rest[at + 2..])),
        _ => (rest, None),
    };
    Some(SortPattern {
        star,
        suffix,
        field,
    })
}

/// Whether `SORT` `cmd` reads a field of a hash it does not name, by a `BY`
/// or `GET` pattern. Such a read can find the field past its time, and on a
/// master removes it and sends its `HDEL` (`sort_pattern_value`). That is a
/// write to a key the command holds no order of: its own order, of the keys
/// it names, is held, and a write made inside it takes none more
/// (`write_order`). Another client's write to the hash could then fall
/// between the removal and its `HDEL`, and reach the replicas and the AOF
/// ahead of it, as `HINCRBY h f 1` then `HDEL h f` (#415 review). So such a
/// `SORT` takes the command gate alone, as a script does, which reads keys
/// it does not name too (`handler::takes_the_gate_alone`). Options are read
/// as `keyspec`'s `SORT` reads them.
pub(crate) fn sort_reads_hash_fields(cmd: &[&[u8]]) -> bool {
    sort_patterns(cmd).any(|pat| sort_pattern_parts(pat).is_some_and(|p| p.field.is_some()))
}

/// The `BY` and `GET` patterns of `SORT` or `SORT_RO` `cmd`, read as
/// `keyspec`'s `SORT` reads its options: a `LIMIT`'s two numbers and a
/// `STORE`'s destination are none. Case does not matter.
fn sort_patterns<'a>(cmd: &'a [&'a [u8]]) -> impl Iterator<Item = &'a [u8]> + 'a {
    let mut i = 2;
    std::iter::from_fn(move || {
        while i < cmd.len() {
            let option = cmd[i];
            i += 1;
            if option.eq_ignore_ascii_case(b"LIMIT") {
                i += 2;
            } else if option.eq_ignore_ascii_case(b"BY") || option.eq_ignore_ascii_case(b"GET") {
                let pat = cmd.get(i).copied();
                i += 1;
                if pat.is_some() {
                    return pat;
                }
            } else if option.eq_ignore_ascii_case(b"STORE") {
                i += 1;
            }
        }
        None
    })
}

/// For `SORT` or `SORT_RO` `cmd` called by a script held to a hash tag
/// (`lua-parallel-scripts`): the first `BY` or `GET` pattern that can name
/// a key outside the tag, which `may_touch` tells (`tag_locks`). Such a key
/// is one the script does not name and holds no lock of: a client's write
/// to it does not wait for the script, and a field of it the `SORT` finds
/// past its time would be removed and its `HDEL` sent with another write to
/// it in between (#415 review). The command gate taken alone for a `SORT`
/// (`sort_reads_hash_fields`) is no help here, as the script holds the gate
/// already.
///
/// A pattern names keys when it has a `*`; `#` and one without a `*` name
/// none. One that names keys is kept to the tag only when what comes before
/// its `*` holds the whole tag: then every key it names carries that tag,
/// whatever the element (`{q}:w_*`, `{q}:obj:*->w`). Otherwise the element
/// decides the key, and so the tag. String keys count as hashes do: a read
/// of one outside the tag is as much outside it as a `GET` of it would be.
pub(crate) fn sort_pattern_outside<'a>(
    cmd: &'a [&'a [u8]],
    may_touch: impl Fn(&[u8]) -> bool,
) -> Option<&'a [u8]> {
    sort_patterns(cmd).find(|pat| {
        sort_pattern_parts(pat).is_some_and(|p| {
            let prefix = &pat[..p.star];
            crate::tag_locks::tag_of(prefix).is_none() || !may_touch(prefix)
        })
    })
}

/// What a `SORT` `BY` or `GET` pattern names for `elem`, read as Redis reads
/// it (`lookupKeyByPattern`): the first `*` stands for the element's bytes,
/// and a `->field` after it, with a field name, names a field of the hash
/// at that key. `None` when the pattern has no `*`, or names nothing there.
fn sort_pattern_value(ctx: &Ctx, pat: &[u8], elem: &[u8]) -> Option<Vec<u8>> {
    let SortPattern {
        star,
        suffix,
        field,
    } = sort_pattern_parts(pat)?;
    let mut key = Vec::with_capacity(star + elem.len() + suffix.len());
    key.extend_from_slice(&pat[..star]);
    key.extend_from_slice(elem);
    key.extend_from_slice(suffix);
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], &key);
    let now = std::time::Instant::now();
    let mut tried = false;
    loop {
        let map = read_lock!(shard);
        return match (map.peek(&key), field) {
            (Some(ValueRef::String(s)), None) => Some(s.to_vec()),
            (Some(ValueRef::Hash(h)), Some(field)) => {
                let name = crate::key_bytes::text_of(field);
                h.read().get(&*name).map(|v| v.to_vec())
            }
            // A field past its deadline is what a hash command makes of it
            // (#415): removed first where this node removes fields, its
            // `HDEL` sent ahead of this `SORT`; kept where the stream or the
            // AOF being applied says when it goes, so that a `STORE` comes
            // out there as it did on the master; hidden from a reader
            // otherwise.
            (Some(ValueRef::HashEx(h)), Some(field)) => {
                let name = crate::key_bytes::text_of(field);
                let inner = h.read();
                let value = inner.fields.get(&*name)?;
                if inner.expiries.get(&*name).is_none_or(|at| *at > now) {
                    return Some(value.to_vec());
                }
                match crate::field_expiry::policy(ctx.store) {
                    crate::field_expiry::Policy::Keep => Some(value.to_vec()),
                    crate::field_expiry::Policy::Expire if !tried => {
                        drop(inner);
                        drop(map);
                        tried = true;
                        crate::field_expiry::expire_due(
                            ctx.store,
                            ctx.index_store,
                            *ctx.selected_db,
                            &key,
                            now,
                        );
                        continue;
                    }
                    _ => None,
                }
            }
            _ => None,
        };
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

            let mut keys: Vec<crate::store::Key> = Vec::new();
            let mut looked_at = 0usize;
            let mut next_cursor: u64 = 0;
            while shard_idx < num_shards {
                let map = read_lock!(db[shard_idx as usize]);
                let (page, next) = crate::commands::helpers::scan_page(
                    map.iter().map(|(key, value)| {
                        (crate::commands::helpers::scan_position(key), (key, value))
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
                        .map(|(key, _)| crate::store::Key::new(key)),
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
                keys.retain(|k| crate::commands::helpers::glob_match_bytes(pat, k));
            }

            let key_items: Vec<Vec<u8>> = keys.iter().map(|k| resp_bulk(k)).collect();

            Some(resp_array(vec![
                resp_bulk_str(&next_cursor.to_string()),
                resp_array(key_items),
            ]))
        }

        "KEYS" => {
            if cmd.len() != 2 {
                return Some(resp_error("ERR wrong number of arguments for cmd 'keys'"));
            }

            // Any bytes, as Redis's `stringmatchlen` matches them.
            let pattern = cmd[1];

            let db = &ctx.store[*ctx.selected_db];
            // Collect into a temporary — we need the count before we can
            // write the RESP array header, and we can't hold all shard locks
            // simultaneously. Collecting key slices (not copies) keeps this cheap.
            // Note: we can't return borrowed keys across the lock boundary,
            // so we collect owned keys.
            let mut candidates: Vec<crate::store::Key> = Vec::new();
            for shard in db.iter() {
                let map = read_lock!(shard);
                candidates.extend(map.keys().map(crate::store::Key::new));
            }
            let mut matching: Vec<crate::store::Key> = Vec::new();
            for key in candidates {
                if lazy_expire_on_read(ctx.store, *ctx.selected_db, &key) {
                    continue;
                }
                if crate::commands::helpers::glob_match_bytes(pattern, &key) {
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
                    let key: &[u8] = cmd[2];
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
                    let key: &[u8] = cmd[2];
                    let db = *ctx.selected_db;
                    let gone = lazy_expire_on_read(ctx.store, db, key);
                    // The key's access word, read without counting as a use.
                    let word = if gone {
                        None
                    } else {
                        read_lock!(store::get_shard(&ctx.store[db], key)).access(key)
                    };
                    let Some(word) = word else {
                        return Some(resp_null_bulk());
                    };
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
                        ("FREQ", true) => resp_integer(crate::eviction::frequency(word) as i64),
                        _ if recording => resp_integer(crate::eviction::idle_seconds(word) as i64),
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
                    let key: &[u8] = cmd[2];
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
            let key: &[u8] = cmd[1];

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
                Some(at) => Some(resp_integer(ms_to_nearest_secs(ttl_remaining_ms(at)))),
            }
        }

        "PERSIST" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'persist' command",
                ));
            }
            let key: &[u8] = cmd[1];

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
            let key: &[u8] = cmd[1];

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
            let key: &[u8] = cmd[1];
            // The options are read before the time, as in Redis.
            let flags = match parse_expire_modifier_flags(cmd.as_slice(), 3) {
                Ok(f) => f,
                Err(msg) => return Some(resp_error(&msg)),
            };
            let ms = match parse_bytes_as_i128(cmd[2]) {
                Some(m) => m,
                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
            };
            // The clock is read once, here.
            let when_ms = match absolute_when_ms_from_pexpire_ms(ms) {
                Ok(w) => w,
                Err(e) => return Some(resp_error(e)),
            };
            let new_exp = match system_time_from_unix_millis_i128(when_ms) {
                Some(t) => t,
                None => return Some(resp_error(ERR_INVALID_EXPIRE_PEXPIRE_CMD)),
            };
            // Only a time to live of zero or less deletes the key here, as
            // `expire_feed` propagates it. `PEXPIRE k 1` found the clock a
            // millisecond on, as it does about once in 650 calls, and deleted
            // the key with nothing propagated: the replica kept it for good.
            // A positive one goes the way every expired key goes, with its
            // `DEL`.
            Some(apply_expire(ctx, key, new_exp, ms <= 0, &flags))
        }

        // ── EXPIREAT ──────────────────────────────────────────────────────────────────
        // Like EXPIRE but the argument is an absolute Unix timestamp in seconds.
        "EXPIREAT" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'expireat' command",
                ));
            }
            let key: &[u8] = cmd[1];
            // Every option, and checked against the others, before anything
            // is touched: only `cmd[3]` was read, and an unknown one was
            // refused after a moment that had come had deleted the key.
            let flags = match parse_expire_modifier_flags(cmd.as_slice(), 3) {
                Ok(f) => f,
                Err(msg) => return Some(resp_error(&msg)),
            };
            let unix_secs = match parse_bytes_as_i128(cmd[2]) {
                Some(n) => n,
                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
            };
            let unix_ms = match unix_secs.checked_mul(1000) {
                Some(ms) => ms,
                None => return Some(resp_error("ERR invalid expire time in EXPIREAT")),
            };
            let new_exp = match system_time_from_unix_millis_i128(unix_ms) {
                Some(t) => t,
                None => return Some(resp_error("ERR invalid expire time in EXPIREAT")),
            };
            let now = crate::script_clock::wall_system_time_now();
            Some(apply_expire(ctx, key, new_exp, new_exp <= now, &flags))
        }

        // ── PEXPIREAT ─────────────────────────────────────────────────────────────────
        // Like EXPIREAT but the timestamp is in milliseconds.
        "PEXPIREAT" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'pexpireat' command",
                ));
            }
            let key: &[u8] = cmd[1];
            // As `EXPIREAT`.
            let flags = match parse_expire_modifier_flags(cmd.as_slice(), 3) {
                Ok(f) => f,
                Err(msg) => return Some(resp_error(&msg)),
            };
            let unix_ms = match parse_bytes_as_i128(cmd[2]) {
                Some(n) => n,
                None => return Some(resp_error(ERR_VALUE_NOT_INTEGER)),
            };
            let new_exp = match system_time_from_unix_millis_i128(unix_ms) {
                Some(t) => t,
                None => return Some(resp_error(ERR_INVALID_EXPIRE_PEXPIREAT_CMD)),
            };
            let now = crate::script_clock::wall_system_time_now();
            Some(apply_expire(ctx, key, new_exp, new_exp <= now, &flags))
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
            let key: &[u8] = cmd[1];

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
                Some(at) => Some(resp_integer(ttl_remaining_ms(at))),
            }
        }

        "RENAME" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'rename' command",
                ));
            }
            let src: Vec<u8> = cmd[1].to_vec();
            let dst: Vec<u8> = cmd[2].to_vec();

            let db = *ctx.selected_db;
            let db_ref = &ctx.store[db];
            // `remove` takes a key past its time too.
            if is_expired(ctx.store, db, &src) {
                delete_key_if_expired(ctx.store, db, &src);
            }
            // A key renamed to itself stays as it is, as in Redis: it was
            // taken out and put back, a new entry with a new LFU count.
            if src == dst {
                let there = read_lock!(store::get_shard(db_ref, &src)).contains_key(&src);
                return Some(if there {
                    resp_simple("OK")
                } else {
                    resp_error("ERR no such key")
                });
            }

            // Read + remove from src, with its time to live
            let (value, expiry) = {
                let shard = store::get_shard(db_ref, &src);
                let mut map = write_lock!(shard);
                match map.remove(src.as_slice()) {
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
                map.insert_expiring(&*dst, value, expiry);
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
            let src: Vec<u8> = cmd[1].to_vec();
            let dst: Vec<u8> = cmd[2].to_vec();

            let db = *ctx.selected_db;
            let db_ref = &ctx.store[db];

            // Check src exists
            let (src_value, expiry) = {
                let shard = store::get_shard(db_ref, &src);
                let map = read_lock!(shard);
                match map.get_expiring(src.as_slice()) {
                    Some((v, expiry)) => (v.to_value(), expiry),
                    None => return Some(resp_error("ERR no such key")),
                }
            };

            // Check dst does NOT exist
            {
                let shard = store::get_shard(db_ref, &dst);
                let map = read_lock!(shard);
                if map.contains_key(dst.as_slice()) {
                    return Some(resp_integer(0));
                }
            }

            // Remove src, write dst
            {
                let shard = store::get_shard(db_ref, &src);
                write_lock!(shard).remove(src.as_slice());
            }
            {
                let shard = store::get_shard(db_ref, &dst);
                write_lock!(shard).insert_expiring(&*dst, src_value, expiry);
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
            let src_key: Vec<u8> = cmd[1].to_vec();
            let dst_key: Vec<u8> = cmd[2].to_vec();

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
                match map.get_expiring(src_key.as_slice()) {
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
                if map.contains_key(dst_key.as_slice()) && !replace {
                    return Some(resp_integer(0));
                }
            }

            // Write to destination, with the source's time to live (the
            // key replaced may have had one: the copy has the source's, or
            // none).
            {
                let shard = store::get_shard(dst_db_ref, &dst_key);
                write_lock!(shard).insert_expiring(&*dst_key, value, expiry);
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

            // Any bytes: what a master expires, evicts or migrates away is
            // deleted on its replicas, and in its AOF, by the DEL or UNLINK
            // it sends, whatever its key (#437). A key that is not text was
            // skipped.
            for &key in cmd.iter().skip(1) {
                // Redis: expireIfNeeded deletes volatile keys first; that removal does not add
                // to the DEL count (see redis `delGenericCommand`).
                if is_expired(ctx.store, *ctx.selected_db, key) {
                    if delete_key_if_expired(ctx.store, *ctx.selected_db, key) {
                        crate::vector::maybe_unindex(ctx.index_store, *ctx.selected_db, key);
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
                    crate::vector::maybe_unindex(ctx.index_store, *ctx.selected_db, key);
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
            let key: Vec<u8> = cmd[1].to_vec();
            let target_db: usize = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR invalid DB index")),
            };

            let src_db = *ctx.selected_db;

            if target_db >= ctx.store.len() || target_db == src_db {
                return Some(resp_integer(0));
            }

            // `remove` takes a key past its time too.
            if is_expired(ctx.store, src_db, &key) {
                delete_key_if_expired(ctx.store, src_db, &key);
            }

            // As in Redis, the key is looked up, a use of it, and the target
            // looked at before anything is taken: a key the target already
            // has is left where it was. It was taken out and put back, a new
            // entry with a new LFU count.
            let src_shard = store::get_shard(&ctx.store[src_db], &key);
            let dst_shard = store::get_shard(&ctx.store[target_db], &key);
            if !read_lock!(src_shard).contains_key(&key) || read_lock!(dst_shard).contains_key(&key)
            {
                return Some(resp_integer(0));
            }
            let Some(taken) = write_lock!(src_shard).remove(&key) else {
                return Some(resp_integer(0));
            };
            // The entry itself moves: the key's value, time to live and
            // access word go with it, as its object does in Redis. If the
            // target has been given the key meanwhile, it goes back.
            let moved = write_lock!(dst_shard).put_back(taken);
            match moved {
                Ok(written_over) => {
                    drop(written_over);
                    Some(resp_integer(1))
                }
                Err(taken) => {
                    let back = write_lock!(src_shard).put_back(taken);
                    drop(back);
                    Some(resp_integer(0))
                }
            }
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
            // picked, for as long as it takes, as in Redis (#566): each
            // pick that removes one leaves one fewer to pick.
            //
            // An expired key that stays (on a replica, while writes are
            // paused, or while loading) could be picked again and again:
            // after 100 of those, as in Redis, the answer is one of them.
            //
            // Whether expired keys stay is asked again at every pick, as
            // Redis does: a pause that begins while this looks must not
            // leave it looking for ever.
            let db = *ctx.selected_db;
            let shards = ctx.store[db].len();
            let expired_keys_stay = || {
                crate::pause::expiry_deferred()
                    || crate::expiry::held()
                    || crate::expiry::of(ctx.store)
                        .as_ref()
                        .is_some_and(crate::expiry::Target::on_replica)
            };
            let mut kept_expired = 0;
            loop {
                let start = rand::random::<usize>() % shards.max(1);
                let picked = (0..shards).find_map(|step| {
                    let map = read_lock!(ctx.store[db][(start + step) % shards]);
                    map.random_entry(rand::random::<usize>)
                        .map(|(key, _)| crate::store::Key::new(key))
                });
                match picked {
                    None => break Some(resp_null_bulk()),
                    Some(key) if lazy_expire_on_read(ctx.store, db, &key) => {
                        if expired_keys_stay() {
                            kept_expired += 1;
                            if kept_expired == 100 {
                                break Some(resp_bulk(&key));
                            }
                        }
                    }
                    Some(key) => break Some(resp_bulk(&key)),
                }
            }
        }

        "TOUCH" => {
            // TOUCH key [key ...]
            // Returns the count of keys that exist (expired keys count as absent).
            // What it is for — marking the keys as used — the lookup below
            // does, as every command's lookups of its keys do: see
            // `store::AccessScope`.
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'touch' command",
                ));
            }
            let db = &ctx.store[*ctx.selected_db];
            let mut count = 0i64;
            for raw_key in cmd.iter().skip(1) {
                let key: &[u8] = raw_key;
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
            let key: &[u8] = cmd[1];
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
            let key: Vec<u8> = cmd[1].to_vec();
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
                if map.contains_key(key.as_slice()) {
                    return Some(resp_error("BUSYKEY Target key name already exists."));
                }
            }

            // Verify the trailer (version + checksum) and decode.
            let value = match crate::snapshot::restore_payload_checked(payload) {
                Ok(v) => v,
                Err(e) => return Some(resp_error(e.message())),
            };

            // A series restored has no links to other series, as in
            // RedisTimeSeries (`RestoreKey`): a payload of 1.2.0, or
            // `MIGRATE` from a node of 1.2.0, carries its compaction rules
            // and source, by key name (#593). An atomic slot migration sends
            // the rules among the keys it moves as `TS.CREATERULE`s.
            if let KiviDBValue::TimeSeries(series) = &value {
                let mut series = series.write();
                series.rules.clear();
                series.source = None;
            }

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
                    // The index holds names (`key_bytes::text_of`).
                    index
                        .entry(format!("{label}={label_value}"))
                        .or_default()
                        .insert(crate::key_bytes::text_of(&key).into_owned());
                }
            }

            if crate::eviction::recording() {
                crate::eviction::restored(ctx.store, db_idx, key.as_slice(), idle, freq);
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
            let key: &[u8] = cmd[1];

            // A source past its time is a missing one: an empty array, or with
            // STORE, the destination deleted and 0, as for any missing key.
            // It returned the empty array here, STORE or not, and left the
            // destination as it was.
            if is_expired(ctx.store, *ctx.selected_db, key) {
                expire_on_read(ctx.store, *ctx.selected_db, key);
            }

            // ── Parse options ──────────────────────────────────────────────────
            let mut alpha = false;
            let mut descending = false;
            let mut limit_offset: usize = 0;
            let mut limit_count: Option<usize> = None;
            // The destination and the patterns are bytes, as keys are.
            let mut store_dst: Option<Vec<u8>> = None;
            let mut by_pattern: Option<Vec<u8>> = None;
            let mut get_patterns: Vec<Vec<u8>> = Vec::new();

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
                        store_dst = match cmd.get(i) {
                            Some(b) => Some(b.to_vec()),
                            None => return Some(resp_error("ERR syntax error")),
                        };
                    }
                    "BY" => {
                        i += 1;
                        by_pattern = match cmd.get(i) {
                            Some(b) => Some(b.to_vec()),
                            None => return Some(resp_error("ERR syntax error")),
                        };
                    }
                    "GET" => {
                        i += 1;
                        match cmd.get(i) {
                            Some(b) => get_patterns.push(b.to_vec()),
                            None => return Some(resp_error("ERR syntax error")),
                        };
                    }
                    _ => return Some(resp_error("ERR syntax error")),
                }
                i += 1;
            }

            // A `BY` pattern with a `*`, and a `GET` pattern other than `#`,
            // read keys named by the elements. As in Redis, they need read
            // access to every key. A `BY` with no `*` reads no key: it only
            // skips the sort.
            let by_reads_keys = by_pattern.as_deref().is_some_and(|p| p.contains(&b'*'));
            let get_reads_keys = get_patterns.iter().any(|p| p.as_slice() != b"#");
            if by_reads_keys || get_reads_keys {
                let acl = ctx.acl_store.read();
                let restricted = acl
                    .get(ctx.authenticated_user)
                    .is_some_and(|user| !user.reads_all_keys());
                if restricted {
                    let option = if by_reads_keys { "BY" } else { "GET" };
                    return Some(resp_error(&format!(
                        "ERR {option} option of SORT denied due to insufficient ACL permissions."
                    )));
                }
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
                .map(|p| !p.eq_ignore_ascii_case(b"nosort") && !p.contains(&b'*'))
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

                    Some(p) if p.eq_ignore_ascii_case(b"nosort") => {
                        // BY nosort
                        vec![None; elements.len()]
                    }

                    Some(pat) => elements
                        .iter()
                        .map(|elem| {
                            sort_pattern_value(ctx, pat, elem)
                                .and_then(|v| std::str::from_utf8(&v).ok()?.parse::<f64>().ok())
                        })
                        .collect(),
                }
            };

            if missing {
                if let Some(ref dst) = store_dst {
                    let dst_shard = store::get_shard(&ctx.store[*ctx.selected_db], dst);
                    write_lock!(dst_shard).remove(dst.as_slice());
                    return Some(resp_integer(0));
                }
                return Some(resp_array(vec![]));
            }

            // ── Sort ───────────────────────────────────────────────────────────
            let is_nosort = by_pattern
                .as_deref()
                .map(|p| p.eq_ignore_ascii_case(b"nosort"))
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
            // `None` is a lookup that found nothing: nil in the reply, an
            // empty string in a `STORE`d list (as in Redis). An empty string
            // that was found is an empty string, not a nil.
            let raw_output: Vec<Option<Vec<u8>>> = if get_patterns.is_empty() {
                elements.iter().map(|e| Some(e.to_vec())).collect()
            } else {
                let mut out = Vec::with_capacity(elements.len() * get_patterns.len());
                for elem in &elements {
                    for pat in &get_patterns {
                        if pat.as_slice() == b"#" {
                            out.push(Some(elem.to_vec()));
                        } else {
                            out.push(sort_pattern_value(ctx, pat, elem));
                        }
                    }
                }
                out
            };

            // ── STORE → write result as List, return count ─────────────────────
            if let Some(ref dst) = store_dst {
                let new_list: std::collections::VecDeque<Arc<[u8]>> = raw_output
                    .iter()
                    .map(|v| Arc::from(v.as_deref().unwrap_or_default()))
                    .collect();
                let count = new_list.len() as i64;
                let db_idx = *ctx.selected_db;
                let dst_shard = store::get_shard(&ctx.store[db_idx], dst);
                let mut dst_map = write_lock!(dst_shard);
                if new_list.is_empty() {
                    dst_map.remove(dst.as_slice());
                } else {
                    dst_map.insert_expiring(
                        dst,
                        KiviDBValue::List(std::sync::Arc::new(parking_lot::RwLock::new(new_list))),
                        None,
                    );
                }
                return Some(resp_integer(count));
            }
            // SORT ... GET pattern yields a nil per unresolved lookup, nested
            // inside the result array.
            let nil = resp_null_for(crate::commands::handler::effective_resp_for_reply(ctx));
            let resp_output: Vec<Vec<u8>> = raw_output
                .iter()
                .map(|v| match v {
                    Some(v) => resp_bulk(v),
                    None => nil.clone(),
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
            let key: &[u8] = cmd[1];

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
            let key: &[u8] = cmd[1];

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
        // To the nearest second, as `TTL` (Redis's `(ms + 500) / 1000`).
        TimeUnit::Secs => ms_to_nearest_secs(d.as_millis() as i64),
        TimeUnit::Millis => d.as_millis() as i64,
    }
}

#[cfg(test)]
mod sort_gate_tests {
    use super::sort_reads_hash_fields;

    fn reads(args: &[&str]) -> bool {
        let cmd: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        sort_reads_hash_fields(&cmd)
    }

    /// Only a `BY` or `GET` pattern that names a hash field of another key
    /// takes the command gate alone; nothing that merely looks like one.
    #[test]
    fn only_a_pattern_naming_a_hash_field_counts() {
        assert!(reads(&["SORT", "ids", "BY", "obj:*->w"]));
        assert!(reads(&[
            "SORT", "ids", "GET", "#", "GET", "obj:*->w", "STORE", "dst"
        ]));
        assert!(reads(&["sort", "ids", "by", "w_*", "get", "o_*->n"]));
        assert!(!reads(&["SORT", "ids"]));
        assert!(!reads(&["SORT", "ids", "BY", "nosort", "STORE", "dst"]));
        assert!(!reads(&[
            "SORT", "ids", "BY", "weight_*", "GET", "#", "GET", "name_*"
        ]));
        // No `*`: names no key. `->` with nothing after: no field.
        assert!(!reads(&["SORT", "ids", "BY", "obj->w"]));
        assert!(!reads(&["SORT", "ids", "GET", "obj:*->"]));
        // A destination or a LIMIT argument is no pattern.
        assert!(!reads(&["SORT", "ids", "STORE", "x:*->y"]));
        assert!(!reads(&["SORT", "ids", "LIMIT", "0", "x:*->y"]));
        assert!(!reads(&["SORT", "ids", "BY"]));
    }

    /// The first pattern of `args` that can name a key outside the tag `q`.
    fn outside(args: &[&str]) -> Option<String> {
        let cmd: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        super::sort_pattern_outside(&cmd, |key| crate::tag_locks::tag_of(key) == Some(&b"q"[..]))
            .map(|pat| String::from_utf8_lossy(pat).into_owned())
    }

    /// A pattern is kept to a tag only when what comes before its `*`
    /// holds the whole tag; string keys count as hashes do; `#`, no `*`, a
    /// destination and a `LIMIT` name nothing.
    #[test]
    fn only_a_pattern_whose_prefix_holds_the_tag_keeps_to_it() {
        let out = |args: &[&str]| outside(args).unwrap_or_default();
        assert_eq!(out(&["SORT", "{q}:ids", "BY", "obj:*->w"]), "obj:*->w");
        assert_eq!(
            out(&["SORT_RO", "{q}:ids", "GET", "#", "GET", "o:*->n"]),
            "o:*->n"
        );
        assert_eq!(out(&["sort", "{q}:ids", "bY", "w_*"]), "w_*");
        assert_eq!(out(&["sort", "{q}:ids", "gEt", "{r}:*"]), "{r}:*");
        // The element would close the tag, or there is none before the `*`.
        assert_eq!(out(&["SORT", "{q}:ids", "BY", "{q*}->w"]), "{q*}->w");
        assert_eq!(out(&["SORT", "{q}:ids", "BY", "*{q}->w"]), "*{q}->w");
        assert_eq!(out(&["SORT", "{q}:ids", "BY", "{}*{q}"]), "{}*{q}");
        // The first `*` is the element's, whatever follows.
        assert_eq!(out(&["SORT", "{q}:ids", "BY", "a*{q}*"]), "a*{q}*");
        assert_eq!(outside(&["SORT", "{q}:ids", "BY", "{q}:obj:*->w"]), None);
        assert_eq!(outside(&["SORT", "{q}:ids", "GET", "x{q}y*"]), None);
        assert_eq!(
            outside(&["SORT", "{q}:ids", "BY", "{q}*", "GET", "#"]),
            None
        );
        assert_eq!(outside(&["SORT", "{q}:ids"]), None);
        assert_eq!(outside(&["SORT", "{q}:ids", "BY", "nosort"]), None);
        assert_eq!(outside(&["SORT", "{q}:ids", "BY", "obj->w"]), None);
        assert_eq!(outside(&["SORT", "{q}:ids", "STORE", "x:*->y"]), None);
        assert_eq!(outside(&["SORT", "{q}:ids", "LIMIT", "0", "x:*->y"]), None);
        assert_eq!(outside(&["SORT", "{q}:ids", "GET"]), None);
        // Each pattern is asked, not only the first.
        assert_eq!(
            out(&[
                "SORT", "{q}:ids", "BY", "{q}:*", "LIMIT", "0", "1", "GET", "{q}:*", "GET", "o*"
            ]),
            "o*"
        );
    }
}
