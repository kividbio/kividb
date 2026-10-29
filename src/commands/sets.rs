use crate::commands::ctx::Ctx;
use crate::commands::helpers::{lazy_expire_on_read, parse_utf8};
use crate::keyspace_events::{KS_SET, notify};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};
use ahash::AHashSet;
use parking_lot::RwLock;
use std::sync::Arc;

type Set = Arc<RwLock<crate::member_set::MemberSet>>;

/// Effective protocol for the reply being built (honours `redis.setresp`).
///
/// RESP3 gives set-valued replies a dedicated type (`~`), which clients use to
/// hand back a native unordered set instead of a list. SMEMBERS / SINTER /
/// SUNION / SDIFF and `SPOP key <count>` all use it; SRANDMEMBER and SSCAN do
/// not — see their call sites for why.
#[inline]
fn reply_resp(ctx: &Ctx) -> u8 {
    crate::commands::handler::effective_resp_for_reply(ctx)
}

fn get_or_create_set(ctx: &Ctx, key: &str) -> Result<Set, Vec<u8>> {
    // Why does this helper exist? Every mutating set command (SADD, SREM, SPOP, SMOVE)
    // does the exact same "get shard → write-lock map → get-or-create set → clone Arc → drop map lock → lock inner set" dance.
    // Without the helper you'd copy that pattern 6+ times and risk inconsistency.
    // The helper encodes the lock-ordering contract in one place: shard map lock is taken and released before the inner set is touched.

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    // Nearly always the set is there: looked for with the shard held for
    // reading, which other readers of the shard do not wait for. It was
    // always taken for writing, in case the set had to be made.
    match read_lock!(shard).get(key) {
        Some(ValueRef::Set(s)) => return Ok(Arc::clone(s)),
        Some(_) => {
            return Err(resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ));
        }
        None => {}
    }
    let mut map = write_lock!(shard);

    // Looked for again: it may have been made since the look above.
    match map.get(key) {
        None => {
            let s = Arc::new(RwLock::new(crate::member_set::MemberSet::new()));
            map.insert(Key::from(key), KiviDBValue::Set(Arc::clone(&s)));
            Ok(s)
        }
        Some(ValueRef::Set(s)) => Ok(Arc::clone(s)),
        Some(_) => Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

// ── load_set ──────────────────────────────────────────────────────────────────
// Reads the set at `key` and returns a cloned, owned AHashSet.
// Returns Some(empty set) when the key doesn't exist.
// Returns None on WRONGTYPE — callers treat None as an error signal.
//
// We clone the inner set rather than holding the RwLock across set-algebra
// operations. This keeps lock scopes short and avoids holding the shard lock
// while doing O(n) work (diff/inter/union). The clone is O(n) in the set size,
// same as the operation itself, so it doesn't change the complexity class.
fn load_set(ctx: &Ctx, key: &str) -> Option<crate::member_set::MemberSet> {
    if lazy_expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key) {
        return Some(crate::member_set::MemberSet::new());
    }
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);

    match map.get(key) {
        None => Some(crate::member_set::MemberSet::new()),
        Some(ValueRef::Set(s)) => Some(s.read().clone()),
        Some(_) => None,
    }
}

/// The members of a set, copied out of the store.
type Members = crate::member_set::MemberSet;

/// The sets named by `key_args`, in order. A key that does not exist is an
/// empty set; a key that holds something else is a WRONGTYPE error, whichever
/// position it is in.
fn load_sets(ctx: &Ctx, key_args: &[&[u8]]) -> Result<Vec<Members>, Vec<u8>> {
    key_args
        .iter()
        .map(|key_bytes| {
            let key = std::str::from_utf8(key_bytes)
                .map_err(|_| resp_error("ERR must be valid UTF-8"))?;
            load_set(ctx, key).ok_or_else(|| {
                resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")
            })
        })
        .collect()
}

// ── set_diff ──────────────────────────────────────────────────────────────────
// Returns elements in the first set that are absent from all subsequent sets.
// `key_args` is a slice of raw command bytes — each entry is a key name.
// Uses retain() for a single O(n) pass per additional set rather than nested
// loops, keeping overall complexity at O(n * m) where m is number of sets.
fn set_diff(ctx: &Ctx, key_args: &[&[u8]]) -> Result<Vec<Arc<[u8]>>, Vec<u8>> {
    let mut sets = load_sets(ctx, key_args)?.into_iter();
    let Some(mut result) = sets.next() else {
        return Ok(vec![]);
    };
    for other in sets {
        result.retain(|v| !other.contains(v));
    }
    Ok(result.into_iter().collect())
}

// ── set_inter ─────────────────────────────────────────────────────────────────
// Returns only elements present in every set.
// Starts with the first set, then retains only elements found in each
// subsequent set.
fn set_inter(ctx: &Ctx, key_args: &[&[u8]]) -> Result<Vec<Arc<[u8]>>, Vec<u8>> {
    let mut sets = load_sets(ctx, key_args)?.into_iter();
    let Some(mut result) = sets.next() else {
        return Ok(vec![]);
    };
    for other in sets {
        result.retain(|v| other.contains(v));
    }
    Ok(result.into_iter().collect())
}

// ── set_union ─────────────────────────────────────────────────────────────────
// Returns all unique elements across every set.
// Extends a single accumulator AHashSet — duplicates are deduplicated
// automatically by the set semantics.
fn set_union(ctx: &Ctx, key_args: &[&[u8]]) -> Result<Vec<Arc<[u8]>>, Vec<u8>> {
    let mut result = AHashSet::<Arc<[u8]>>::new();
    for set in load_sets(ctx, key_args)? {
        result.extend(set);
    }
    Ok(result.into_iter().collect())
}

// ── store_set ─────────────────────────────────────────────────────────────────
// Writes a Vec<Arc<[u8]>> back into the store as a KiviDBValue::Set.
// Overwrites whatever was at dst_key — this is the correct behaviour for
// SDIFFSTORE / SINTERSTORE / SUNIONSTORE which always replace the destination:
// the value, and with it any TTL the old one had. (An empty result leaves no
// key at all; the dispatcher removes it — see `settle_written_keys`.)
fn store_set(ctx: &mut Ctx, dst_key: &str, members: Vec<Arc<[u8]>>) {
    let set: crate::member_set::MemberSet = members.into_iter().collect();
    let db = *ctx.selected_db;
    let db_ref = &ctx.store[db];
    let shard = store::get_shard(db_ref, dst_key);
    {
        let mut map = write_lock!(shard);
        map.insert(
            Key::from(dst_key),
            KiviDBValue::Set(Arc::new(RwLock::new(set))),
        );
    }
    crate::commands::helpers::clear_expiry(ctx.expiry_store, db, dst_key);
}

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;

    match name {
        "SADD" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sadd' command",
                ));
            }

            // The key and the members as they stand in the command. The key
            // was copied to a string of its own, and every member to a shared
            // copy in a vector, whether or not the set had it already.
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let set = match get_or_create_set(ctx, key) {
                Ok(s) => s,
                Err(e) => return Some(e),
            };

            let mut inner = set.write();
            let mut added = 0i64;
            for member in &cmd[2..] {
                if inner.insert_bytes(member) {
                    added += 1;
                }
            }
            drop(inner);
            if added > 0 {
                notify(ctx.pubsub, *ctx.selected_db, KS_SET, "sadd", key);
            }
            Some(resp_integer(added))
        }

        "SCARD" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'scard' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            if lazy_expire_on_read(ctx.store, ctx.expiry_store, *ctx.selected_db, key) {
                return Some(resp_integer(0));
            }

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => Some(resp_integer(0)),
                Some(ValueRef::Set(s)) => Some(resp_integer(s.read().len() as i64)),
                Some(_) => Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }

        "SISMEMBER" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sismember' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let member = &cmd[2];

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
                Some(ValueRef::Set(s)) => {
                    let inner = s.read();
                    let found = inner.contains(member);
                    if found {
                        crate::stats::record_hit();
                    } else {
                        crate::stats::record_miss();
                    }
                    Some(resp_integer(found as i64))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }

        "SMISMEMBER" => {
            // SMISMEMBER key member [member ...]  — returns array of 1/0
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'smismember' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    // key missing: all zeroes
                    let zeros = vec![resp_integer(0); cmd.len() - 2];
                    Some(resp_array(zeros))
                }
                Some(ValueRef::Set(s)) => {
                    let inner = s.read();
                    let results: Vec<Vec<u8>> = cmd[2..]
                        .iter()
                        .map(|m| resp_integer(inner.contains(m) as i64))
                        .collect();
                    crate::stats::record_hit();
                    Some(resp_array(results))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }
        "SMEMBERS" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'smembers' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    // Missing key is an empty *set* under RESP3
                    // (`shared.emptyset[c->resp]`), not an empty array.
                    Some(resp_set_for(reply_resp(ctx), vec![]))
                }
                Some(ValueRef::Set(s)) => {
                    let inner = s.read();
                    let mut buf = Vec::with_capacity(16 + inner.len() * 24);
                    // RESP3 has a real set type (`~`); RESP2 only has arrays.
                    // Identical payload, one byte of header difference — this
                    // keeps the single-pass build instead of collecting into a
                    // Vec just to re-encode it.
                    buf.push(if reply_resp(ctx) >= 3 { b'~' } else { b'*' });
                    buf.extend_from_slice(inner.len().to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for v in inner.iter() {
                        write_bulk(&mut buf, v);
                    }
                    crate::stats::record_hit();
                    Some(buf)
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }
        "SREM" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'srem' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
                Some(ValueRef::Set(s)) => {
                    let mut inner = s.write();
                    let mut removed = 0i64;
                    for m in cmd.iter().skip(2) {
                        // remove() on crate::member_set::MemberSet accepts &[u8] via Borrow
                        if inner.remove(m) {
                            removed += 1;
                        }
                    }
                    drop(inner);
                    crate::stats::record_hit();
                    if removed > 0 {
                        notify(ctx.pubsub, *ctx.selected_db, KS_SET, "srem", key);
                    }
                    Some(resp_integer(removed))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }
        "SPOP" => {
            if cmd.len() < 2 || cmd.len() > 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'spop' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let count: Option<usize> = match cmd.get(2) {
                None => None,
                Some(raw) => match parse_utf8(raw).and_then(|s| s.parse::<i64>().ok()) {
                    Some(n) if n >= 0 => Some(n as usize),
                    Some(_) => {
                        return Some(resp_error("ERR value is out of range, must be positive"));
                    }
                    None => return Some(resp_error("ERR value is not an integer or out of range")),
                },
            };

            let resp = reply_resp(ctx);
            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => match count {
                    None => {
                        crate::stats::record_miss();
                        Some(resp_null_bulk())
                    }
                    // With a count, a missing key is an empty set under RESP3.
                    Some(_) => Some(resp_set_for(resp, vec![])),
                },
                Some(ValueRef::Set(s)) => {
                    let mut inner = s.write();
                    // Which members go is random, as the command's name
                    // says, and each is found without looking at the rest:
                    // see `MemberSet`.
                    let mut rng = rand::thread_rng();
                    let wanted = count.unwrap_or(1).min(inner.len());
                    let mut popped: Vec<Arc<[u8]>> = Vec::with_capacity(wanted);
                    while popped.len() < wanted {
                        match inner.pop_random(&mut rng) {
                            Some(member) => popped.push(member),
                            None => break,
                        }
                    }
                    drop(inner);
                    if popped.is_empty() {
                        crate::stats::record_miss();
                    } else {
                        crate::stats::record_hit();
                        notify(ctx.pubsub, *ctx.selected_db, KS_SET, "spop", key);
                    }
                    match count {
                        None => Some(match popped.first() {
                            Some(member) => resp_bulk(member),
                            None => resp_null_bulk(),
                        }),
                        // SPOP with a count replies with the RESP3 set type
                        // (`addReplySetLen` in spopWithCountCommand). Note
                        // SRANDMEMBER below deliberately stays an array: a
                        // negative count there may repeat members, which a
                        // set cannot represent.
                        Some(_) => Some(resp_set_for(
                            resp,
                            popped.iter().map(|v| resp_bulk(v)).collect(),
                        )),
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

        "SRANDMEMBER" => {
            if cmd.len() < 2 || cmd.len() > 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'srandmember' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let count: Option<i64> = match cmd.get(2) {
                None => None,
                Some(raw) => match parse_utf8(raw).and_then(|s| s.parse::<i64>().ok()) {
                    // The magnitude has to be a count.
                    Some(i64::MIN) => return Some(resp_error("ERR value is out of range")),
                    Some(n) => Some(n),
                    None => return Some(resp_error("ERR value is not an integer or out of range")),
                },
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => match count {
                    None => {
                        crate::stats::record_miss();
                        Some(resp_null_bulk())
                    }
                    Some(_) => Some(resp_array(vec![])),
                },
                Some(ValueRef::Set(s)) => {
                    let inner = s.read(); // read-only — no removal
                    let mut rng = rand::thread_rng();
                    crate::stats::record_hit();
                    match count {
                        None => Some(match inner.random(&mut rng) {
                            Some(member) => resp_bulk(member),
                            None => resp_null_bulk(),
                        }),
                        // A count: that many distinct members, or all of them.
                        Some(n) if n >= 0 => Some(resp_array(
                            inner
                                .random_distinct(n as usize, &mut rng)
                                .iter()
                                .map(|member| resp_bulk(member))
                                .collect(),
                        )),
                        // A negative count: that many picks, each from the
                        // whole set, so a member may come up more than once.
                        Some(n) => {
                            let mut items: Vec<Vec<u8>> = Vec::new();
                            for _ in 0..n.unsigned_abs() {
                                match inner.random(&mut rng) {
                                    Some(member) => items.push(resp_bulk(member)),
                                    None => break,
                                }
                            }
                            Some(resp_array(items))
                        }
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

        "SMOVE" => {
            // SMOVE source destination member
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'smove' command",
                ));
            }
            let src_key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let dst_key = match parse_utf8(cmd[2]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let member: Arc<[u8]> = Arc::from(cmd[3]);
            let db = *ctx.selected_db;
            let db_ref = &ctx.store[db];
            const WRONGTYPE: &str =
                "WRONGTYPE Operation against a key holding the wrong kind of value";

            // Both keys are looked at before either is changed: a
            // destination of another type is an error that moves nothing.
            let source = match read_lock!(store::get_shard(db_ref, &src_key)).get(src_key.as_str())
            {
                None => None,
                Some(ValueRef::Set(s)) => Some(Arc::clone(s)),
                Some(_) => return Some(resp_error(WRONGTYPE)),
            };
            match read_lock!(store::get_shard(db_ref, &dst_key)).get(dst_key.as_str()) {
                None | Some(ValueRef::Set(_)) => {}
                Some(_) => return Some(resp_error(WRONGTYPE)),
            }
            let Some(source) = source else {
                crate::stats::record_miss();
                return Some(resp_integer(0));
            };
            if src_key == dst_key {
                // Moving it to where it is: it is there or it is not.
                return Some(resp_integer(source.read().contains(member.as_ref()) as i64));
            }
            if !source.write().remove(member.as_ref()) {
                crate::stats::record_miss();
                return Some(resp_integer(0));
            }

            let added = {
                let shard = store::get_shard(db_ref, &dst_key);
                let mut map = write_lock!(shard);
                match map.get(dst_key.as_str()) {
                    Some(ValueRef::Set(s)) => s.write().insert(Arc::clone(&member)),
                    _ => {
                        let mut new_set = crate::member_set::MemberSet::new();
                        new_set.insert(Arc::clone(&member));
                        map.insert(
                            Key::from(&*dst_key),
                            KiviDBValue::Set(Arc::new(RwLock::new(new_set))),
                        );
                        true
                    }
                }
            };

            crate::stats::record_hit();
            notify(ctx.pubsub, db, KS_SET, "srem", &src_key);
            if added {
                notify(ctx.pubsub, db, KS_SET, "sadd", &dst_key);
            } else {
                // It was in the destination already, which is as it was: a
                // transaction watching it has nothing to be aborted for.
                crate::watch::unchanged(db, &dst_key);
            }
            Some(resp_integer(1))
        }
        "SDIFF" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sdiff' command",
                ));
            }
            crate::stats::record_hit();
            let resp = reply_resp(ctx);
            let members = match set_diff(ctx, &cmd[1..]) {
                Ok(members) => members,
                Err(e) => return Some(e),
            };
            Some(resp_set_for(
                resp,
                members.into_iter().map(|v| resp_bulk(&v)).collect(),
            ))
        }

        "SDIFFSTORE" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sdiffstore' command",
                ));
            }
            let dst_key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let result = match set_diff(ctx, &cmd[2..]) {
                Ok(result) => result,
                Err(e) => return Some(e),
            };
            let len = result.len() as i64;
            store_set(ctx, &dst_key, result);
            crate::stats::record_hit();
            notify(ctx.pubsub, *ctx.selected_db, KS_SET, "sdiffstore", &dst_key);
            Some(resp_integer(len))
        }

        "SINTER" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sinter' command",
                ));
            }
            crate::stats::record_hit();
            let resp = reply_resp(ctx);
            let members = match set_inter(ctx, &cmd[1..]) {
                Ok(members) => members,
                Err(e) => return Some(e),
            };
            Some(resp_set_for(
                resp,
                members.into_iter().map(|v| resp_bulk(&v)).collect(),
            ))
        }

        "SINTERSTORE" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sinterstore' command",
                ));
            }
            let dst_key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let result = match set_inter(ctx, &cmd[2..]) {
                Ok(result) => result,
                Err(e) => return Some(e),
            };
            let len = result.len() as i64;
            store_set(ctx, &dst_key, result);
            crate::stats::record_hit();
            notify(
                ctx.pubsub,
                *ctx.selected_db,
                KS_SET,
                "sinterstore",
                &dst_key,
            );
            Some(resp_integer(len))
        }

        "SINTERCARD" => {
            // SINTERCARD numkeys key [key ...] [LIMIT limit]
            let numkeys = match parse_utf8(cmd[1]).and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if n > 0 => n as usize,
                _ => return Some(resp_error("ERR numkeys should be greater than 0")),
            };
            let keys_end = 2 + numkeys;
            if cmd.len() < keys_end {
                return Some(resp_error(
                    "ERR Number of keys can't be greater than number of args",
                ));
            }
            // 0: no limit.
            let limit: usize = match &cmd[keys_end..] {
                [] => 0,
                [word, n] if word.eq_ignore_ascii_case(b"LIMIT") => {
                    match parse_utf8(n).and_then(|s| s.parse::<i64>().ok()) {
                        Some(n) if n >= 0 => n as usize,
                        _ => return Some(resp_error("ERR LIMIT can't be negative")),
                    }
                }
                _ => return Some(resp_error("ERR syntax error")),
            };

            let result = match set_inter(ctx, &cmd[2..keys_end]) {
                Ok(result) => result,
                Err(e) => return Some(e),
            };
            let count = if limit > 0 {
                result.len().min(limit)
            } else {
                result.len()
            };
            crate::stats::record_hit();
            Some(resp_integer(count as i64))
        }

        "SUNION" => {
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sunion' command",
                ));
            }
            crate::stats::record_hit();
            let resp = reply_resp(ctx);
            let members = match set_union(ctx, &cmd[1..]) {
                Ok(members) => members,
                Err(e) => return Some(e),
            };
            Some(resp_set_for(
                resp,
                members.into_iter().map(|v| resp_bulk(&v)).collect(),
            ))
        }

        "SUNIONSTORE" => {
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sunionstore' command",
                ));
            }
            let dst_key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let result = match set_union(ctx, &cmd[2..]) {
                Ok(result) => result,
                Err(e) => return Some(e),
            };
            let len = result.len() as i64;
            store_set(ctx, &dst_key, result);
            crate::stats::record_hit();
            notify(
                ctx.pubsub,
                *ctx.selected_db,
                KS_SET,
                "sunionstore",
                &dst_key,
            );
            Some(resp_integer(len))
        }

        "SSCAN" => {
            // SSCAN key cursor [MATCH pattern] [COUNT count]
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'sscan' command",
                ));
            }
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };
            let (cursor, options) = match crate::commands::helpers::scan_arguments(cmd, 2, &[]) {
                Ok(parsed) => parsed,
                Err(e) => return Some(e),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_array(vec![resp_bulk_str("0"), resp_array(vec![])]))
                }
                Some(value @ ValueRef::Set(s)) => {
                    // A set Redis would encode compactly comes back whole.
                    let (cursor, count) = if crate::commands::generics::scans_whole(value) {
                        (0, usize::MAX)
                    } else {
                        (cursor, options.count)
                    };
                    let inner = s.read();
                    // In the order of the members' positions: see
                    // `helpers::scan_position`.
                    let (page, next_cursor) = crate::commands::helpers::scan_page(
                        inner
                            .iter()
                            .map(|m| (crate::commands::helpers::scan_position(m), m)),
                        cursor,
                        count,
                        inner.len(),
                        1024,
                    );
                    let items: Vec<Vec<u8>> = page
                        .into_iter()
                        .filter(|m| {
                            options.pattern.as_deref().is_none_or(|pat| {
                                crate::commands::helpers::glob_match(
                                    pat,
                                    &String::from_utf8_lossy(m),
                                )
                            })
                        })
                        .map(|m| resp_bulk(m))
                        .collect();

                    crate::stats::record_hit();
                    Some(resp_array(vec![
                        resp_bulk_str(&next_cursor.to_string()),
                        resp_array(items),
                    ]))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }

        // SUNIONCARD / SDIFFCARD
        _ => crate::commands::set_card::handle(ctx, name),
    }
}
