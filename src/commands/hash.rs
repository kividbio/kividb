use crate::commands::ctx::Ctx;
use crate::commands::handler::effective_resp_for_reply;
use crate::commands::helpers::*;
use crate::keyspace_events::{KS_HASH, notify, notify_subkeys};
use crate::resp::write::*;
use crate::store;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    if matches!(name, "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT") {
        match canonical_field_args(&ctx.cmd, FieldArgs::Expire) {
            Ok(Some(ordered)) => ctx.cmd = ordered,
            Ok(None) => {}
            Err(e) => return Some(e),
        }
    }
    let cmd = &ctx.cmd;

    match name {
        "HSET" => {
            if cmd.len() < 4 || !cmd.len().is_multiple_of(2) {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hset' command",
                ));
            }

            let key: &[u8] = cmd[1];

            // Collect (field_name, bytes) pairs from the command args
            // cmd[2], cmd[3] = first pair; cmd[4], cmd[5] = second; ...
            let pairs: Vec<(String, Arc<[u8]>)> = cmd[2..]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|chunk| {
                    let field = crate::commands::helpers::name_from_bytes(chunk[0]).into_owned();
                    let val: Arc<[u8]> = Arc::from(chunk[1]);
                    (field, val)
                })
                .collect();

            let added = match write_hash(ctx, key, |fields, mut expiries| {
                let mut new_count = 0i64;
                for (field, val) in &pairs {
                    // HSET removes any field-TTL; matches Redis 7.4 semantics
                    // where setting a field clears its TTL.
                    if let Some(expiries) = expiries.as_deref_mut() {
                        expiries.remove(field);
                    }
                    if fields.insert(field.clone(), Arc::clone(val)).is_none() {
                        new_count += 1;
                    }
                }
                new_count
            }) {
                Ok(added) => added,
                Err(e) => return Some(e),
            };

            // Now update any matching vector indexes — outside the shard lock
            crate::vector::maybe_index(
                ctx.index_store,
                *ctx.selected_db,
                key,
                &pairs,
                crate::vector::Write::Fields,
            );
            notify(ctx.pubsub, *ctx.selected_db, KS_HASH, "hset", key);
            let fields = pairs.iter().map(|(f, _)| f.as_bytes());
            notify_subkeys(ctx.pubsub, *ctx.selected_db, "hset", key, fields);
            Some(resp_integer(added))
        }

        "HGET" => {
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hget' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let field = crate::commands::helpers::name_from_bytes(cmd[2]);
            let field: &str = &field;

            match with_hash_fields(ctx, key, |inner| inner.get(field).map(|v| resp_bulk(v))) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_null_bulk())
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(Some(v))) => {
                    crate::stats::record_hit();
                    Some(v)
                }
                Some(Ok(None)) => {
                    crate::stats::record_miss();
                    Some(resp_null_bulk())
                }
            }
        }

        "HGETALL" => {
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hgetall' command",
                ));
            }
            let key: &[u8] = cmd[1];

            match with_hash_read(ctx, key, |inner| {
                let eff = effective_resp_for_reply(ctx);
                if eff == 3 {
                    let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(inner.len() * 2);
                    for (k, v) in inner.iter() {
                        chunks.push(resp_bulk(&crate::commands::helpers::name_to_bytes(k)));
                        chunks.push(resp_bulk(v.as_ref()));
                    }
                    Some(resp_map(chunks))
                } else {
                    let count = inner.len() * 2;
                    let mut buf = Vec::with_capacity(16 + count * 32);
                    buf.push(b'*');
                    buf.extend_from_slice(count.to_string().as_bytes());
                    buf.extend_from_slice(b"\r\n");
                    for (k, v) in inner.iter() {
                        write_bulk(&mut buf, &crate::commands::helpers::name_to_bytes(k));
                        write_bulk(&mut buf, v);
                    }
                    Some(buf)
                }
            }) {
                None => {
                    crate::stats::record_miss();
                    // A missing key is an empty *map* under RESP3, matching the
                    // populated case above. Replying with an empty array here
                    // made clients hand back `[]` instead of `{}` for a missing
                    // key while returning a dict for a present one — the same
                    // command yielding two different types.
                    Some(resp_map_for(effective_resp_for_reply(ctx), vec![]))
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(arr)) => {
                    crate::stats::record_hit();
                    arr
                }
            }
        }

        "HMSET" => {
            // HMSET key field value [field value ...]
            // Identical to HSET but always returns OK instead of count.
            // Deprecated in Redis 4.0 but still widely used by clients.
            if cmd.len() < 4 || !cmd.len().is_multiple_of(2) {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hmset' command",
                ));
            }
            let key: &[u8] = cmd[1];

            let pairs: Vec<(String, Arc<[u8]>)> = cmd[2..]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|chunk| {
                    let field = crate::commands::helpers::name_from_bytes(chunk[0]).into_owned();
                    let val: Arc<[u8]> = Arc::from(chunk[1]);
                    (field, val)
                })
                .collect();

            if let Err(e) = write_hash(ctx, key, |fields, mut expiries| {
                for (field, val) in &pairs {
                    if let Some(expiries) = expiries.as_deref_mut() {
                        expiries.remove(field);
                    }
                    fields.insert(field.clone(), Arc::clone(val));
                }
            }) {
                return Some(e);
            }

            crate::vector::maybe_index(
                ctx.index_store,
                *ctx.selected_db,
                key,
                &pairs,
                crate::vector::Write::Fields,
            );
            notify(ctx.pubsub, *ctx.selected_db, KS_HASH, "hset", key);
            let fields = pairs.iter().map(|(f, _)| f.as_bytes());
            notify_subkeys(ctx.pubsub, *ctx.selected_db, "hset", key, fields);
            Some(resp_simple("OK"))
        }

        "HMGET" => {
            // HMGET key field [field ...]
            // Returns an array of values, one per field.
            // Missing fields return nil (null bulk string).
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hmget' command",
                ));
            }
            let key: &[u8] = cmd[1];

            // Per-field nils are nested inside the array, so they need the
            // protocol-correct encoding here — `resp3_normalize_null` only
            // rewrites a reply that is nothing but a top-level null.
            let resp = effective_resp_for_reply(ctx);
            match with_hash_fields(ctx, key, |inner| {
                cmd[2..]
                    .iter()
                    .map(|f| {
                        let field = crate::commands::helpers::name_from_bytes(f);
                        match inner.get(field.as_ref()) {
                            Some(v) => {
                                crate::stats::record_hit();
                                resp_bulk(v)
                            }
                            None => {
                                crate::stats::record_miss();
                                resp_null_for(resp)
                            }
                        }
                    })
                    .collect::<Vec<Vec<u8>>>()
            }) {
                None => {
                    crate::stats::record_miss();
                    let nils = vec![resp_null_for(resp); cmd.len() - 2];
                    Some(resp_array(nils))
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(items)) => {
                    crate::stats::record_hit();
                    Some(resp_array(items))
                }
            }
        }

        "HDEL" => {
            // HDEL key field [field ...]
            // Removes fields from a hash. Returns count of fields actually removed.
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hdel' command",
                ));
            }
            let key: &[u8] = cmd[1];

            match with_hash_write(ctx, key, |fields, mut expiries| {
                let mut removed: Vec<&[u8]> = Vec::new();
                for f in cmd[2..].iter() {
                    let field = crate::commands::helpers::name_from_bytes(f);
                    if crate::types::hash_remove(fields, field.as_ref()).is_some() {
                        if let Some(exps) = expiries.as_mut() {
                            exps.remove(field.as_ref());
                        }
                        removed.push(*f);
                    }
                }
                removed
            }) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(removed)) => {
                    crate::stats::record_hit();
                    if !removed.is_empty() {
                        notify(ctx.pubsub, *ctx.selected_db, KS_HASH, "hdel", key);
                    }
                    let n = removed.len() as i64;
                    notify_subkeys(ctx.pubsub, *ctx.selected_db, "hdel", key, removed);
                    Some(resp_integer(n))
                }
            }
        }

        "HEXISTS" => {
            // HEXISTS key field
            // Returns 1 if the field exists, 0 otherwise.
            if cmd.len() != 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hexists' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let field = crate::commands::helpers::name_from_bytes(cmd[2]);
            let field: &str = &field;

            match with_hash_fields(ctx, key, |inner| {
                if inner.contains_key(field) {
                    1i64
                } else {
                    0i64
                }
            }) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(n)) => {
                    crate::stats::record_hit();
                    Some(resp_integer(n))
                }
            }
        }

        "HLEN" => {
            // HLEN key
            // Returns the number of fields in the hash.
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hlen' command",
                ));
            }
            let key: &[u8] = cmd[1];

            match with_hash_read(ctx, key, |inner| inner.len() as i64) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_integer(0))
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(n)) => {
                    crate::stats::record_hit();
                    Some(resp_integer(n))
                }
            }
        }

        "HKEYS" => {
            // HKEYS key
            // Returns all field names in the hash.
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hkeys' command",
                ));
            }
            let key: &[u8] = cmd[1];

            match with_hash_read(ctx, key, |inner| {
                let mut buf = Vec::with_capacity(16 + inner.len() * 24);
                buf.push(b'*');
                buf.extend_from_slice(inner.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for k in inner.keys() {
                    write_bulk(&mut buf, &crate::commands::helpers::name_to_bytes(k));
                }
                Some(buf)
            }) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_array(vec![]))
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(buf)) => {
                    crate::stats::record_hit();
                    buf
                }
            }
        }

        "HVALS" => {
            // HVALS key
            // Returns all values in the hash.
            if cmd.len() != 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hvals' command",
                ));
            }
            let key: &[u8] = cmd[1];

            match with_hash_read(ctx, key, |inner| {
                let mut buf = Vec::with_capacity(16 + inner.len() * 24);
                buf.push(b'*');
                buf.extend_from_slice(inner.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for v in inner.values() {
                    write_bulk(&mut buf, v);
                }
                Some(buf)
            }) {
                None => {
                    crate::stats::record_miss();
                    Some(resp_array(vec![]))
                }
                Some(Err(e)) => {
                    crate::stats::record_miss();
                    Some(e)
                }
                Some(Ok(buf)) => {
                    crate::stats::record_hit();
                    buf
                }
            }
        }

        "HINCRBY" => {
            // HINCRBY key field increment
            // Increments the integer value of a hash field by increment.
            // Creates the field with value 0 if it doesn't exist.
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hincrby' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let field = crate::commands::helpers::name_from_bytes(cmd[2]).into_owned();
            let incr: i64 = match parse_utf8(cmd[3]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };

            // A hash past its time goes first; then the field is found and
            // written, or the hash made with it, with the shard held all
            // the while (`write_hash`). Made after another hold of the
            // shard had found no hash, it was made twice by two increments
            // at once, and both answered `incr` (#271).
            lazy_expire_key(ctx.store, *ctx.selected_db, key);
            let result = match write_hash(ctx, key, |fields, _expiries| {
                let current: i64 = match fields.get(&field) {
                    None => 0,
                    Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return Err(resp_error("ERR hash value is not an integer")),
                    },
                };
                // Past the end of a 64-bit integer is an error: it used to
                // wrap, or in a debug build take the connection down.
                let Some(next) = current.checked_add(incr) else {
                    return Err(resp_error("ERR increment or decrement would overflow"));
                };
                fields.insert(field.clone(), Arc::from(next.to_string().as_bytes()));
                Ok(resp_integer(next))
            }) {
                Err(e) | Ok(Err(e)) => Some(e),
                Ok(Ok(out)) => Some(out),
            };
            if result
                .as_ref()
                .map(|r| !r.starts_with(b"-"))
                .unwrap_or(false)
            {
                notify(ctx.pubsub, *ctx.selected_db, KS_HASH, "hincrby", key);
                notify_subkeys(ctx.pubsub, *ctx.selected_db, "hincrby", key, [cmd[2]]);
            }
            result
        }

        "HINCRBYFLOAT" => {
            // HINCRBYFLOAT key field increment
            // Same as HINCRBY but for floats.
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hincrbyfloat' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let field = crate::commands::helpers::name_from_bytes(cmd[2]).into_owned();
            let incr: f64 = match parse_utf8(cmd[3]).and_then(|s| s.parse::<f64>().ok()) {
                Some(n) if n.is_finite() => n,
                Some(_) => return Some(resp_error("ERR value is NaN or Infinity")),
                None => return Some(resp_error("ERR value is not a valid float")),
            };

            // As `HINCRBY`.
            lazy_expire_key(ctx.store, *ctx.selected_db, key);
            let result = match write_hash(ctx, key, |fields, _expiries| {
                let current: f64 = match fields.get(&field) {
                    None => 0.0,
                    Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return Err(resp_error("ERR hash value is not a float")),
                    },
                };
                let next = current + incr;
                if !next.is_finite() {
                    return Err(resp_error("ERR increment would produce NaN or Infinity"));
                }
                let s = format!("{}", next);
                fields.insert(field.clone(), Arc::from(s.as_bytes()));
                Ok(resp_bulk_str(&s))
            }) {
                Err(e) | Ok(Err(e)) => Some(e),
                Ok(Ok(out)) => Some(out),
            };
            if result
                .as_ref()
                .map(|r| !r.starts_with(b"-"))
                .unwrap_or(false)
            {
                notify(ctx.pubsub, *ctx.selected_db, KS_HASH, "hincrbyfloat", key);
                notify_subkeys(ctx.pubsub, *ctx.selected_db, "hincrbyfloat", key, [cmd[2]]);
            }
            result
        }

        "HSETNX" => {
            // HSETNX key field value
            // Set field only if it does not already exist.
            // Returns 1 if set, 0 if field already existed.
            if cmd.len() != 4 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hsetnx' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let field = crate::commands::helpers::name_from_bytes(cmd[2]).into_owned();
            let val: Arc<[u8]> = Arc::from(cmd[3]);

            let was_set = match write_hash(ctx, key, |fields, _expiries| {
                if fields.contains_key(&field) {
                    false
                } else {
                    fields.insert(field.clone(), Arc::clone(&val));
                    true
                }
            }) {
                Ok(was_set) => was_set,
                Err(e) => return Some(e),
            };
            if was_set {
                notify(ctx.pubsub, *ctx.selected_db, KS_HASH, "hset", key);
                notify_subkeys(ctx.pubsub, *ctx.selected_db, "hset", key, [cmd[2]]);
            }
            Some(resp_integer(was_set as i64))
        }

        "HRANDFIELD" => {
            // HRANDFIELD key [count [WITHVALUES]]
            // Returns random field(s) from the hash.
            if cmd.len() < 2 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hrandfield' command",
                ));
            }
            let key: &[u8] = cmd[1];

            // `[count [WITHVALUES]]`, read before the key is looked at.
            let count: Option<i64> = match cmd.get(2) {
                None => None,
                Some(raw) => match parse_utf8(raw).and_then(|s| s.parse::<i64>().ok()) {
                    // The magnitude has to be a count.
                    Some(i64::MIN) => return Some(resp_error("ERR value is out of range")),
                    Some(n) => Some(n),
                    None => return Some(resp_error("ERR value is not an integer or out of range")),
                },
            };
            let with_values = match &cmd[cmd.len().min(3)..] {
                [] => false,
                [word] if word.eq_ignore_ascii_case(b"WITHVALUES") => true,
                _ => return Some(resp_error("ERR syntax error")),
            };
            // Each field is two elements of the reply then, and the reply's
            // length has to fit.
            if with_values && count.is_some_and(|n| n.unsigned_abs() > (i64::MAX / 2) as u64) {
                return Some(resp_error("ERR value is out of range"));
            }
            // Under RESP3 a field with its value is a pair of its own.
            let pairs = with_values && crate::commands::handler::effective_resp_for_reply(ctx) >= 3;
            // Nothing there: nil for the one field asked for, no fields for
            // a count of them.
            let none = if count.is_some() {
                resp_array(vec![])
            } else {
                resp_null_bulk()
            };

            let now = expire_before(ctx, key);
            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                Some(v) if matches!(v, ValueRef::Hash(_) | ValueRef::HashEx(_)) => {
                    // Get a borrow into the field map regardless of variant.
                    let plain_guard;
                    let ex_guard;
                    let ex_view;
                    let inner: &crate::types::HashFields = match v {
                        ValueRef::Hash(h) => {
                            plain_guard = h.read();
                            &plain_guard
                        }
                        ValueRef::HashEx(hx) => {
                            ex_guard = hx.read();
                            ex_view = visible(ctx, &ex_guard, now);
                            &ex_view
                        }
                        _ => unreachable!(),
                    };
                    if inner.is_empty() {
                        return Some(none);
                    }

                    // No count arg — return single random field name
                    let Some(count) = count else {
                        let idx = rand::random::<usize>() % inner.len();
                        let field = inner.keys().nth(idx).unwrap();
                        return Some(resp_bulk(&crate::commands::helpers::name_to_bytes(field)));
                    };
                    let push = |result: &mut Vec<Vec<u8>>, f: &String, v: &Arc<[u8]>| {
                        if pairs {
                            result.push(resp_array(vec![
                                resp_bulk(&crate::commands::helpers::name_to_bytes(f)),
                                resp_bulk(v),
                            ]));
                        } else {
                            result.push(resp_bulk(&crate::commands::helpers::name_to_bytes(f)));
                            if with_values {
                                result.push(resp_bulk(v));
                            }
                        }
                    };

                    let fields: Vec<(&String, &Arc<[u8]>)> = inner.iter().collect();
                    let len = fields.len();

                    // Positive count: unique fields up to count
                    // Negative count: allow repeats, abs(count) total
                    let mut result: Vec<Vec<u8>> = Vec::new();

                    if count >= 0 {
                        let take = (count as usize).min(len);
                        // Shuffle via partial Fisher-Yates in index space
                        let mut indices: Vec<usize> = (0..len).collect();
                        for i in 0..take {
                            let j = i + rand::random::<usize>() % (len - i);
                            indices.swap(i, j);
                        }
                        for &idx in &indices[..take] {
                            let (f, v) = fields[idx];
                            push(&mut result, f, v);
                        }
                    } else {
                        // Negative count — sample with replacement
                        let take = count.unsigned_abs() as usize;
                        for _ in 0..take {
                            let idx = rand::random::<usize>() % len;
                            let (f, v) = fields[idx];
                            push(&mut result, f, v);
                        }
                    }

                    crate::stats::record_hit();
                    Some(resp_array(result))
                }
                Some(_) => {
                    crate::stats::record_miss();
                    Some(resp_error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
                None => {
                    crate::stats::record_miss();
                    Some(none)
                }
            }
        }

        "HSCAN" => {
            // HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
            if cmd.len() < 3 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hscan' command",
                ));
            }
            let key: &[u8] = cmd[1];
            let (cursor, options) =
                match crate::commands::helpers::scan_arguments(cmd, 2, &["NOVALUES"]) {
                    Ok(parsed) => parsed,
                    Err(e) => return Some(e),
                };

            let now = expire_before(ctx, key);
            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                Some(v) if matches!(v, ValueRef::Hash(_) | ValueRef::HashEx(_)) => {
                    // A hash Redis would encode compactly comes back whole.
                    let (cursor, count) = if crate::commands::generics::scans_whole(v) {
                        (0, usize::MAX)
                    } else {
                        (cursor, options.count)
                    };
                    // Get a borrow into the field map regardless of variant.
                    let plain_guard;
                    let ex_guard;
                    let ex_view;
                    let inner: &crate::types::HashFields = match v {
                        ValueRef::Hash(h) => {
                            plain_guard = h.read();
                            &plain_guard
                        }
                        ValueRef::HashEx(hx) => {
                            ex_guard = hx.read();
                            ex_view = visible(ctx, &ex_guard, now);
                            &ex_view
                        }
                        _ => unreachable!(),
                    };

                    // In the order of the fields' positions: see
                    // `helpers::scan_position`.
                    let (page, next_cursor) = crate::commands::helpers::scan_page(
                        inner.iter().map(|(f, v)| {
                            (
                                crate::commands::helpers::scan_position(f.as_bytes()),
                                (f, v),
                            )
                        }),
                        cursor,
                        count,
                        inner.len(),
                        1024,
                    );
                    let mut items: Vec<Vec<u8>> = Vec::with_capacity(page.len() * 2);
                    for (f, v) in page {
                        if options.pattern.as_deref().is_some_and(|pat| {
                            !crate::commands::helpers::glob_match_bytes(
                                pat,
                                &crate::commands::helpers::name_to_bytes(f),
                            )
                        }) {
                            continue;
                        }
                        items.push(resp_bulk(&crate::commands::helpers::name_to_bytes(f)));
                        if !options.no_values {
                            items.push(resp_bulk(v));
                        }
                    }

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
                None => {
                    crate::stats::record_miss();
                    Some(resp_array(vec![resp_bulk_str("0"), resp_array(vec![])]))
                }
            }
        }

        // ── HEXPIRE ──────────────────────────────────────────────────────────
        // HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [...]
        //
        // Per-field returns:
        //   -2 = no such field
        //    0 = condition (NX/XX/GT/LT) not met, expiry left as-is
        //    1 = expiry set
        //    2 = the supplied time was non-positive (≤0) so the field was
        //        deleted immediately. Only HEXPIREAT/HPEXPIREAT trigger this
        //        in practice; for relative TTL we reject ≤0 with an error.
        "HEXPIRE" => {
            if cmd.len() < 6 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hexpire' command",
                ));
            }
            let key: Vec<u8> = cmd[1].to_vec();
            let secs: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            // 0 is now, and the fields go, as in Redis 8.10; only a
            // negative time is refused.
            if secs < 0 {
                return Some(resp_error("ERR invalid expire time, must be >= 0"));
            }
            let Some((new_exp, at_ms)) = field_deadline_after(secs, true) else {
                return Some(invalid_field_expire_time("HEXPIRE"));
            };
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };
            let results = apply_field_expire(ctx, &key, &fields, new_exp, at_ms, flag, secs == 0);
            // results is already a Vec<Vec<u8>> of resp_integer / resp_error.
            // If a single WRONGTYPE error was returned, surface that directly.
            if results.len() == 1 && results[0].starts_with(b"-") {
                return Some(results.into_iter().next().unwrap());
            }
            Some(resp_array(results))
        }

        // ── HPEXPIRE ─────────────────────────────────────────────────────────
        // Same as HEXPIRE but the TTL is in milliseconds.
        "HPEXPIRE" => {
            if cmd.len() < 6 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hpexpire' command",
                ));
            }
            let key: Vec<u8> = cmd[1].to_vec();
            let ms: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            // 0 is now, and the fields go, as in Redis 8.10; only a
            // negative time is refused.
            if ms < 0 {
                return Some(resp_error("ERR invalid expire time, must be >= 0"));
            }
            let Some((new_exp, at_ms)) = field_deadline_after(ms, false) else {
                return Some(invalid_field_expire_time("HPEXPIRE"));
            };
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };
            let results = apply_field_expire(ctx, &key, &fields, new_exp, at_ms, flag, ms == 0);
            if results.len() == 1 && results[0].starts_with(b"-") {
                return Some(results.into_iter().next().unwrap());
            }
            Some(resp_array(results))
        }

        // ── HEXPIREAT ────────────────────────────────────────────────────────
        // HEXPIREAT key unix-time-seconds [NX|XX|GT|LT] FIELDS numfields field [...]
        //
        // Per-field returns: -2 missing, 0 cond not met, 1 set, 2 already-past
        // (field deleted immediately).
        "HEXPIREAT" => {
            if cmd.len() < 6 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hexpireat' command",
                ));
            }
            let key: Vec<u8> = cmd[1].to_vec();
            let unix_secs: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            if let Some(refused) = negative_unix_time(ctx, unix_secs) {
                return Some(refused);
            }
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };

            // A moment already past deletes the field (2), or, where fields
            // are held, is set as it is (see `apply_to_hashex`). One past the
            // latest deadline Redis allows is refused, as by Redis.
            let at_ms = if unix_secs <= 0 {
                0
            } else {
                match field_deadline_ms(unix_secs, true, 0) {
                    Some(at_ms) => at_ms as u64,
                    None => return Some(invalid_field_expire_time("HEXPIREAT")),
                }
            };
            let (new_exp, is_past) = absolute_deadline(at_ms, unix_secs <= 0);
            let results = apply_field_expire(ctx, &key, &fields, new_exp, at_ms, flag, is_past);
            if results.len() == 1 && results[0].starts_with(b"-") {
                return Some(results.into_iter().next().unwrap());
            }
            Some(resp_array(results))
        }

        // ── HPEXPIREAT ───────────────────────────────────────────────────────
        // Same as HEXPIREAT but the timestamp is in milliseconds.
        "HPEXPIREAT" => {
            if cmd.len() < 6 {
                return Some(resp_error(
                    "ERR wrong number of arguments for 'hpexpireat' command",
                ));
            }
            let key: Vec<u8> = cmd[1].to_vec();
            let unix_ms: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            if let Some(refused) = negative_unix_time(ctx, unix_ms) {
                return Some(refused);
            }
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };

            let at_ms = if unix_ms <= 0 {
                0
            } else {
                match field_deadline_ms(unix_ms, false, 0) {
                    Some(at_ms) => at_ms as u64,
                    None => return Some(invalid_field_expire_time("HPEXPIREAT")),
                }
            };
            let (new_exp, is_past) = absolute_deadline(at_ms, unix_ms <= 0);
            let results = apply_field_expire(ctx, &key, &fields, new_exp, at_ms, flag, is_past);
            if results.len() == 1 && results[0].starts_with(b"-") {
                return Some(results.into_iter().next().unwrap());
            }
            Some(resp_array(results))
        }

        "HTTL" => run_hfe_read(ctx, "HTTL", HfeReadMode::RemainingSecs),
        "HPTTL" => run_hfe_read(ctx, "HPTTL", HfeReadMode::RemainingMillis),
        "HEXPIRETIME" => run_hfe_read(ctx, "HEXPIRETIME", HfeReadMode::AbsoluteSecs),
        "HPEXPIRETIME" => run_hfe_read(ctx, "HPEXPIRETIME", HfeReadMode::AbsoluteMillis),
        "HPERSIST" => run_hpersist(ctx),
        "HGETDEL" => run_hgetdel(ctx),
        "HGETEX" => run_hgetex(ctx),
        "HSETEX" => run_hsetex(ctx),
        "HSTRLEN" => run_hstrlen(ctx),

        // HIMPORT; `None` tells the router this module doesn't handle it.
        _ => crate::commands::hash_import::handle(ctx, name),
    }
}

// ── HashEx interop helper ─────────────────────────────────────────────────────
//
// Every hash command reaches the fields through these, for a plain `Hash` and
// a `HashEx` alike. A `HashEx` with a field past its deadline is the only one
// that costs more: the fields due are removed first ([`expire_first`]), or,
// where this node does not remove them, hidden from a reader ([`visible`]).
// `now` is read once per command, and the removal and the command agree on it.
//
// Returns:
//   None         — key is absent
//   Some(Err(e)) — key is the wrong type; e is the RESP error to return
//   Some(Ok(v))  — closure result

/// Whether the fields of `hash` due at `now` are to be removed before the
/// command goes on: once, and only where this node removes them
/// (`field_expiry::policy`).
#[inline]
fn expire_first(ctx: &Ctx, hash: &crate::types::HashInner, now: Instant, tried: &mut bool) -> bool {
    if *tried || !hash.has_due(now) {
        return false;
    }
    *tried = true;
    crate::field_expiry::policy(ctx.store) == crate::field_expiry::Policy::Expire
}

/// The fields of `key` due at `now` removed, with no lock of its shard or of
/// the hash held, before the command looks at the hash again.
#[cold]
#[inline(never)]
fn expire_fields(ctx: &Ctx, key: &[u8], now: Instant) {
    crate::field_expiry::expire_due(ctx.store, ctx.index_store, *ctx.selected_db, key, now);
}

/// What a reader is shown of `hash`: the fields due at `now` left out, when
/// they are still there and this node is not to see them.
#[inline]
fn visible<'a>(
    ctx: &Ctx,
    hash: &'a crate::types::HashInner,
    now: Instant,
) -> std::borrow::Cow<'a, crate::types::HashFields> {
    if hash.has_due(now)
        && crate::field_expiry::policy(ctx.store) != crate::field_expiry::Policy::Keep
    {
        std::borrow::Cow::Owned(hash.live_fields(now))
    } else {
        std::borrow::Cow::Borrowed(&hash.fields)
    }
}

/// For a command that looks at the hash at `key` under locks of its own:
/// its fields due now removed first, where this node removes them. Returns
/// the `now` they were due at, for the command to hide by.
fn expire_before(ctx: &Ctx, key: &[u8]) -> Instant {
    let now = Instant::now();
    let due = match read_lock!(store::get_shard(&ctx.store[*ctx.selected_db], key)).get(key) {
        Some(ValueRef::HashEx(hx)) => expire_first(ctx, &hx.read(), now, &mut false),
        _ => false,
    };
    if due {
        expire_fields(ctx, key, now);
    }
    now
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// The fields of the hash at `key` as a reader is shown them: the whole of
/// them, those due and not to be seen left out ([`visible`]).
fn with_hash_read<F, R>(ctx: &Ctx, key: &[u8], f: F) -> Option<Result<R, Vec<u8>>>
where
    F: FnOnce(&crate::types::HashFields) -> R,
{
    read_hash(ctx, key, |fields, hide| match hide {
        None => f(fields),
        Some((hash, now)) => f(&hash.live_fields(now)),
    })
}

/// [`with_hash_read`] for a command that looks fields up by name (`HGET`,
/// `HMGET`, `HEXISTS`, `HSTRLEN`): the same rule, applied to the fields it
/// asks for, with no copy of the rest of the hash however many are due.
fn with_hash_fields<F, R>(ctx: &Ctx, key: &[u8], f: F) -> Option<Result<R, Vec<u8>>>
where
    F: FnOnce(&FieldsShown<'_>) -> R,
{
    read_hash(ctx, key, |fields, hide| {
        f(&FieldsShown {
            fields,
            hide: hide.map(|(hash, now)| (&hash.expiries, now)),
        })
    })
}

/// A hash's fields, looked up one at a time, as a reader is shown them.
struct FieldsShown<'a> {
    fields: &'a crate::types::HashFields,
    /// The deadlines and the moment by which fields due are hidden, when
    /// some are due and this node is not to see them.
    hide: Option<(&'a crate::types::Expiries, Instant)>,
}

impl<'a> FieldsShown<'a> {
    fn get(&self, field: &str) -> Option<&'a Arc<[u8]>> {
        let value = self.fields.get(field)?;
        match self.hide {
            Some((times, now)) if times.get(field).is_some_and(|at| *at <= now) => None,
            _ => Some(value),
        }
    }

    fn contains_key(&self, field: &str) -> bool {
        self.get(field).is_some()
    }
}

/// The read of the hash at `key` that [`with_hash_read`] and
/// [`with_hash_fields`] share: `f` is given its fields and, when fields due
/// at `now` are to be hidden from the reader, the hash and `now`. The fields
/// due go first where this node removes them ([`expire_first`]); whether
/// any is due is looked at once.
fn read_hash<R>(
    ctx: &Ctx,
    key: &[u8],
    f: impl FnOnce(&crate::types::HashFields, Option<(&crate::types::HashInner, Instant)>) -> R,
) -> Option<Result<R, Vec<u8>>> {
    if lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return None;
    }
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut now: Option<Instant> = None;
    let mut tried = false;
    loop {
        let map = read_lock!(shard);
        return match map.get(key) {
            None => None,
            Some(ValueRef::Hash(h)) => {
                let inner = h.read();
                Some(Ok(f(&inner, None)))
            }
            Some(ValueRef::HashEx(hx)) => {
                let now = *now.get_or_insert_with(Instant::now);
                let inner = hx.read();
                if !inner.has_due(now) {
                    return Some(Ok(f(&inner.fields, None)));
                }
                if !tried {
                    tried = true;
                    if crate::field_expiry::policy(ctx.store) == crate::field_expiry::Policy::Expire
                    {
                        drop(inner);
                        drop(map);
                        expire_fields(ctx, key, now);
                        continue;
                    }
                }
                let hide =
                    crate::field_expiry::policy(ctx.store) != crate::field_expiry::Policy::Keep;
                Some(Ok(f(&inner.fields, hide.then_some((&*inner, now)))))
            }
            Some(_) => Some(Err(resp_error(WRONGTYPE))),
        };
    }
}

/// Mutable variant: closure gets `&mut crate::types::HashFields` for the
/// fields map, regardless of the underlying variant. The fields past their
/// deadline go first (see [`expire_first`]).
fn with_hash_write<F, R>(ctx: &Ctx, key: &[u8], f: F) -> Option<Result<R, Vec<u8>>>
where
    F: FnOnce(&mut crate::types::HashFields, Option<&mut crate::types::Expiries>) -> R,
{
    if lazy_expire_key(ctx.store, *ctx.selected_db, key) {
        return None;
    }
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut now: Option<Instant> = None;
    let mut tried = false;
    loop {
        let map = read_lock!(shard);
        return match map.get(key) {
            None => None,
            Some(ValueRef::Hash(h)) => {
                let mut inner = h.write();
                Some(Ok(f(&mut inner, None)))
            }
            Some(ValueRef::HashEx(hx)) => {
                let now = *now.get_or_insert_with(Instant::now);
                let mut inner = hx.write();
                if expire_first(ctx, &inner, now, &mut tried) {
                    drop(inner);
                    drop(map);
                    expire_fields(ctx, key, now);
                    continue;
                }
                // Split borrow so the closure can touch both maps.
                let crate::types::HashInner { fields, expiries } = &mut *inner;
                Some(Ok(f(fields, Some(expiries))))
            }
            Some(_) => Some(Err(resp_error(WRONGTYPE))),
        };
    }
}

/// Write fields of the hash at `key` with `write`, which is given its fields
/// and, for a hash with field TTLs, their times; the hash is made if there
/// is none, with what `write` puts in it. `Err` is the WRONGTYPE reply.
///
/// All of it with the key's shard held: for reading when the hash is there,
/// which keeps a command that empties it (`HDEL`, the field-expiry sweep)
/// from taking it out of the keyspace meanwhile; for writing when it is
/// made, and then made whole, never empty. Found under one hold of the
/// shard and written after it, the fields went into a hash no longer in the
/// keyspace, and a new hash made twice at once kept one maker's fields.
///
/// A hash's fields past their deadline go first ([`expire_first`]); if that
/// empties it, the hash is made anew.
fn write_hash<R>(
    ctx: &Ctx,
    key: &[u8],
    mut write: impl FnMut(&mut crate::types::HashFields, Option<&mut crate::types::Expiries>) -> R,
) -> Result<R, Vec<u8>> {
    let shard = store::get_shard(&ctx.store[*ctx.selected_db], key);
    let mut now: Option<Instant> = None;
    let mut tried = false;
    loop {
        let absent = match read_lock!(shard).get(key) {
            Some(ValueRef::Hash(h)) => return Ok(write(&mut h.write(), None)),
            Some(ValueRef::HashEx(hx)) => {
                let now = *now.get_or_insert_with(Instant::now);
                if let Some(out) = write_hash_ex(ctx, hx, now, &mut tried, &mut write) {
                    return Ok(out);
                }
                false
            }
            Some(_) => return Err(resp_error(WRONGTYPE)),
            None => true,
        };
        if absent {
            let mut map = write_lock!(shard);
            // Looked for again: it may have been made since.
            match map.get(key) {
                Some(ValueRef::Hash(h)) => return Ok(write(&mut h.write(), None)),
                Some(ValueRef::HashEx(hx)) => {
                    let now = *now.get_or_insert_with(Instant::now);
                    if let Some(out) = write_hash_ex(ctx, hx, now, &mut tried, &mut write) {
                        return Ok(out);
                    }
                }
                Some(_) => return Err(resp_error(WRONGTYPE)),
                None => {
                    let mut fields = crate::types::HashFields::default();
                    let out = write(&mut fields, None);
                    if !fields.is_empty() {
                        let hash = Arc::new(parking_lot::RwLock::new(fields));
                        map.insert(key, KiviDBValue::Hash(hash));
                    }
                    return Ok(out);
                }
            }
        }
        // Fields were due: removed with the shard let go of, then again.
        expire_fields(ctx, key, *now.get_or_insert_with(Instant::now));
    }
}

/// [`write_hash`]'s write of a hash with field TTLs. `None`: fields are due
/// and to be removed first.
fn write_hash_ex<R>(
    ctx: &Ctx,
    hash: &parking_lot::RwLock<crate::types::HashInner>,
    now: Instant,
    tried: &mut bool,
    write: &mut impl FnMut(&mut crate::types::HashFields, Option<&mut crate::types::Expiries>) -> R,
) -> Option<R> {
    let mut inner = hash.write();
    if expire_first(ctx, &inner, now, tried) {
        return None;
    }
    let crate::types::HashInner { fields, expiries } = &mut *inner;
    Some(write(fields, Some(expiries)))
}

// ── Hash field-TTL helpers ───────────────────────────────────────────────────
//
// Shared between HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT (the setters) and
// HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME (the readers). Lives here rather than
// in helpers.rs because it's hash-internal and references HashInner directly.

/// Per-field result code returned by HEXPIRE-family setters.
/// Matches Redis's documented return values exactly.
const HFE_NO_SUCH_FIELD: i64 = -2;
const HFE_NO_TTL_SET: i64 = -1; // used by readers (HTTL/HPTTL); reserved here for symmetry
const HFE_COND_NOT_MET: i64 = 0;
const HFE_OK: i64 = 1;
const HFE_DELETED_DUE_TO_PAST: i64 = 2;

#[allow(dead_code)] // HFE_NO_TTL_SET will be used by HTTL/HPTTL in change #5
const _: i64 = HFE_NO_TTL_SET;

/// Parsed flag for the conditional set arg in HEXPIRE-family commands.
#[derive(Clone, Copy)]
enum HfeFlag {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

impl HfeFlag {
    fn from_token(s: &str) -> Option<Self> {
        match s {
            "NX" => Some(HfeFlag::Nx),
            "XX" => Some(HfeFlag::Xx),
            "GT" => Some(HfeFlag::Gt),
            "LT" => Some(HfeFlag::Lt),
            _ => None,
        }
    }
}

/// Parse the tail of a hash field-TTL command:
///   ... [NX|XX|GT|LT] FIELDS numfields field [field ...]
/// `start_idx` points at the first byte AFTER the seconds/ms/timestamp arg
/// (e.g. cmd[3] for HEXPIRE because cmd[0]=HEXPIRE, cmd[1]=key, cmd[2]=seconds).
///
/// Returns (flag, fields) on success. On error returns a RESP error to send
/// straight back to the client.
fn parse_hfe_tail(cmd: &[Vec<u8>], start_idx: usize) -> Result<(HfeFlag, Vec<String>), Vec<u8>> {
    let mut idx = start_idx;
    let mut flag = HfeFlag::None;

    if idx >= cmd.len() {
        return Err(resp_error("ERR syntax error"));
    }

    // Optional NX/XX/GT/LT before FIELDS keyword
    let first_tok = String::from_utf8_lossy(&cmd[idx]).to_ascii_uppercase();
    if first_tok != "FIELDS" {
        match HfeFlag::from_token(&first_tok) {
            Some(f) => {
                flag = f;
                idx += 1;
            }
            None => return Err(resp_error("ERR syntax error")),
        }
    }

    if idx >= cmd.len() {
        return Err(resp_error("ERR syntax error"));
    }
    let fields_tok = String::from_utf8_lossy(&cmd[idx]).to_ascii_uppercase();
    if fields_tok != "FIELDS" {
        return Err(resp_error(
            "ERR Mandatory argument FIELDS is missing or not at the right position",
        ));
    }
    idx += 1;

    if idx >= cmd.len() {
        return Err(resp_error("ERR syntax error"));
    }
    let numfields: usize = match parse_utf8(&cmd[idx]).and_then(|s| s.parse().ok()) {
        Some(n) if n > 0 => n,
        _ => {
            return Err(resp_error(
                "ERR Parameter `numFields` should be greater than 0",
            ));
        }
    };
    idx += 1;

    if cmd.len() - idx != numfields {
        return Err(resp_error(
            "ERR The `numFields` parameter must match the number of arguments",
        ));
    }

    let mut fields = Vec::with_capacity(numfields);
    for f in &cmd[idx..] {
        fields.push(crate::commands::helpers::name_from_bytes(f).into_owned());
    }

    Ok((flag, fields))
}

/// Decide whether to set a new expiry given the existing one and the flag.
///
/// `current` is the existing per-field expiry (None = field has no TTL).
/// `new_exp` is the proposed new expiry.
fn should_set_field_expiry(current: Option<Instant>, new_exp: Instant, flag: HfeFlag) -> bool {
    match flag {
        HfeFlag::None => true,
        HfeFlag::Nx => current.is_none(),
        HfeFlag::Xx => current.is_some(),
        HfeFlag::Gt => match current {
            Some(c) => new_exp > c,
            None => false, // GT on no-TTL means "no current ceiling to beat" → don't set
        },
        HfeFlag::Lt => match current {
            Some(c) => new_exp < c,
            None => true, // LT on no-TTL: any finite TTL is less than ∞ → set
        },
    }
}

/// Apply HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT semantics for a single hash key.
///
/// Steps:
///   1. WRONGTYPE if the key is not a hash variant.
///   2. If the key is missing, every field returns -2 (no such field).
///   3. If the key is a plain `Hash`, upgrade to `HashEx` in place — but only
///      if at least one of the requested fields actually exists, to avoid
///      pointlessly upgrading a hash that nobody is going to use TTLs on.
///   4. For each requested field, check existence + condition + apply.
///
/// `new_exp` is the proposed expiry as an Instant.
/// `is_past` indicates whether the user-supplied absolute time was already in
/// the past (only relevant for HEXPIREAT / HPEXPIREAT). When true, an OK set
/// translates to immediate deletion + return code 2.
fn apply_field_expire(
    ctx: &mut Ctx,
    key: &[u8],
    fields: &[String],
    new_exp: Instant,
    at_ms: u64,
    flag: HfeFlag,
    is_past: bool,
) -> Vec<Vec<u8>> {
    crate::replication::script_feed::effects_begin_if_read(ctx);
    let results = apply_field_expire_inner(ctx, key, fields, new_exp, flag, is_past);
    // One result per field when the command ran; a lone error otherwise.
    if results.len() == fields.len() {
        let db = *ctx.selected_db;
        let with_code = |code: i64| -> Vec<&[u8]> {
            let wanted = resp_integer(code);
            fields
                .iter()
                .zip(&results)
                .filter(|(_, r)| **r == wanted)
                .map(|(f, _)| f.as_bytes())
                .collect()
        };
        // 1: the TTL was set. 2: the time had already passed, so the field
        // was deleted on the spot.
        let (expiring, deleted) = (with_code(HFE_OK), with_code(2));
        // Sent as what it did, its condition decided here: a replica that
        // weighed `XX` or `GT` again against its own fields could decide
        // otherwise. A field it found missing, or left alone, is not sent.
        record_field_effect(b"HPEXPIREAT", key, Some(at_ms), &expiring);
        record_field_effect(b"HDEL", key, None, &deleted);
        if !expiring.is_empty() {
            notify(ctx.pubsub, db, KS_HASH, "hexpire", key);
            notify_subkeys(ctx.pubsub, db, "hexpire", key, expiring);
        }
        if !deleted.is_empty() {
            notify(ctx.pubsub, db, KS_HASH, "hdel", key);
            notify_subkeys(ctx.pubsub, db, "hdel", key, deleted);
        }
    }
    results
}

fn apply_field_expire_inner(
    ctx: &mut Ctx,
    key: &[u8],
    fields: &[String],
    new_exp: Instant,
    flag: HfeFlag,
    is_past: bool,
) -> Vec<Vec<u8>> {
    use parking_lot::RwLock;

    expire_before(ctx, key);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let mut map = write_lock!(shard);

    // ── Type check + upgrade decision ──────────────────────────────────────
    enum Variant {
        Missing,
        WrongType,
        Hash(Arc<RwLock<crate::types::HashFields>>),
        HashEx(Arc<RwLock<crate::types::HashInner>>),
    }
    let variant = match map.get(key) {
        None => Variant::Missing,
        Some(ValueRef::Hash(h)) => Variant::Hash(Arc::clone(h)),
        Some(ValueRef::HashEx(h)) => Variant::HashEx(Arc::clone(h)),
        Some(_) => Variant::WrongType,
    };

    match variant {
        Variant::WrongType => {
            vec![resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )]
        }
        Variant::Missing => {
            // Per Redis: every field returns -2.
            fields
                .iter()
                .map(|_| resp_integer(HFE_NO_SUCH_FIELD))
                .collect()
        }
        Variant::Hash(plain) => {
            // Upgrade plain Hash → HashEx in place. We only do this when at
            // least one field actually exists (otherwise we'd churn the
            // variant for nothing). Read the plain hash first to check.
            let plain_read = plain.read();
            let any_exists = fields.iter().any(|f| plain_read.contains_key(f));
            if !any_exists {
                // All fields are missing — return -2 for each, leave variant alone.
                return fields
                    .iter()
                    .map(|f| {
                        if plain_read.contains_key(f) {
                            // Unreachable per the any_exists check above, but
                            // kept for symmetry with the non-trivial path.
                            resp_integer(HFE_OK)
                        } else {
                            resp_integer(HFE_NO_SUCH_FIELD)
                        }
                    })
                    .collect();
            }
            drop(plain_read);

            // Move the inner map out by cloning fields into a new HashInner.
            // We can't move out of an Arc, so we drain via a write lock.
            let mut plain_w = plain.write();
            let mut new_inner = crate::types::HashInner::new();
            new_inner.fields = std::mem::take(&mut *plain_w);
            drop(plain_w);

            let new_arc = Arc::new(RwLock::new(new_inner));
            map.insert(key, KiviDBValue::HashEx(Arc::clone(&new_arc)));

            apply_to_hashex(&new_arc, fields, new_exp, flag, is_past)
        }
        Variant::HashEx(hx) => apply_to_hashex(&hx, fields, new_exp, flag, is_past),
    }
}

/// Whether this thread sets a field's deadline that has passed rather than
/// deleting the field: it replays the AOF, or applies a stream with fields
/// held (`expiry::fields_held`).
#[inline]
fn fields_are_held() -> bool {
    crate::expiry::replaying() || crate::expiry::fields_held()
}

/// The deadline `at_ms` (milliseconds since the Unix epoch) as an `Instant`,
/// and whether it has passed (`past`: a time of zero or less, which has).
/// Converted to the nanosecond (`FieldClock`, #448); one that has passed is
/// kept as the moment it was, for where fields are held.
fn absolute_deadline(at_ms: u64, past: bool) -> (Instant, bool) {
    let clock = FieldClock::now();
    match clock.instant_of(at_ms) {
        Some(at) if !past => (at, false),
        _ => (clock.instant_or_past_of(at_ms), true),
    }
}

/// Record, as the running command's effect for replicas and the AOF,
/// `command key [at] FIELDS n field…`, or `HDEL key field…`. `fields` are
/// names as a hash keeps them. Nothing for no fields.
fn record_field_effect(command: &[u8], key: &[u8], at_ms: Option<u64>, fields: &[&[u8]]) {
    if fields.is_empty() || !crate::replication::script_feed::recording() {
        return;
    }
    let name = |f: &&[u8]| {
        crate::commands::helpers::name_to_bytes(std::str::from_utf8(f).unwrap_or_default())
            .into_owned()
    };
    let mut parts = vec![command.to_vec(), key.to_vec()];
    if command != b"HDEL" {
        if let Some(at) = at_ms {
            parts.push(at.to_string().into_bytes());
        }
        parts.push(b"FIELDS".to_vec());
        parts.push(fields.len().to_string().into_bytes());
    }
    parts.extend(fields.iter().map(name));
    crate::replication::script_feed::effect(parts);
}

/// The per-field loop. Splits out so both the upgrade and already-HashEx paths
/// share it.
fn apply_to_hashex(
    hx: &Arc<parking_lot::RwLock<crate::types::HashInner>>,
    fields: &[String],
    new_exp: Instant,
    flag: HfeFlag,
    is_past: bool,
) -> Vec<Vec<u8>> {
    let mut inner = hx.write();
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(fields.len());

    for f in fields {
        if !inner.fields.contains_key(f) {
            out.push(resp_integer(HFE_NO_SUCH_FIELD));
            continue;
        }

        let current = inner.expiries.get(f).copied();
        if !should_set_field_expiry(current, new_exp, flag) {
            out.push(resp_integer(HFE_COND_NOT_MET));
            continue;
        }

        // Replaying the AOF, or applying a master's stream with fields held,
        // the field's deadline is set all the same: the writes after it were
        // made while it lived, and the `HDEL` that follows says when it went.
        // A deadline that passed while the master's write was on its way is
        // one of these.
        if is_past && !fields_are_held() {
            // The user gave an absolute time already in the past. Redis
            // semantics: drop the field immediately and return 2.
            inner.evict_field(f);
            out.push(resp_integer(HFE_DELETED_DUE_TO_PAST));
            continue;
        }

        inner.expiries.insert(f.clone(), new_exp);
        out.push(resp_integer(HFE_OK));
    }

    out
}

// ── Hash field-TTL readers (HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME) ───────

#[derive(Clone, Copy)]
enum HfeReadMode {
    /// HTTL — remaining seconds until expiry.
    RemainingSecs,
    /// HPTTL — remaining milliseconds until expiry.
    RemainingMillis,
    /// HEXPIRETIME — absolute Unix expiry in seconds.
    AbsoluteSecs,
    /// HPEXPIRETIME — absolute Unix expiry in milliseconds.
    AbsoluteMillis,
}

/// Parse the `... FIELDS numfields field [field ...]` tail used by all four
/// readers. No NX/XX/GT/LT for these commands.
fn parse_hfe_read_tail(cmd: &[&[u8]], start_idx: usize) -> Result<Vec<String>, Vec<u8>> {
    let mut idx = start_idx;
    if idx >= cmd.len() {
        return Err(resp_error("ERR syntax error"));
    }
    let tok = String::from_utf8_lossy(cmd[idx]).to_ascii_uppercase();
    if tok != "FIELDS" {
        return Err(resp_error(
            "ERR Mandatory argument FIELDS is missing or not at the right position",
        ));
    }
    idx += 1;
    if idx >= cmd.len() {
        return Err(resp_error("ERR syntax error"));
    }
    let numfields = match parse_utf8(cmd[idx]).and_then(|s| s.parse::<i64>().ok()) {
        Some(n) if n > 0 => n as usize,
        _ => {
            return Err(resp_error(
                "ERR Number of fields must be a positive integer",
            ));
        }
    };
    idx += 1;
    if cmd.len() - idx != numfields {
        return Err(resp_error(
            "ERR The `numfields` parameter must match the number of arguments",
        ));
    }
    let mut fields = Vec::with_capacity(numfields);
    for f in &cmd[idx..] {
        fields.push(crate::commands::helpers::name_from_bytes(f).into_owned());
    }
    Ok(fields)
}

/// Redis's reply to a negative Unix time given to `HEXPIREAT`, `HPEXPIREAT`
/// or `EXAT`/`PXAT` of `HGETEX` and `HSETEX` (#486); `None` when `n` is
/// taken. 0 is taken, and the fields go, as in Redis. A negative one used
/// to be taken as a time that had passed, and the fields were deleted
/// where Redis keeps them and refuses the command.
///
/// From the write stream (a replica applying its master's, an AOF
/// replayed) a negative one is still taken as passed: a 1.2.0 server did
/// that and sent the command on as it came, and a field it deleted would
/// otherwise come back. No Redis or Valkey master sends one.
fn negative_unix_time(ctx: &Ctx, n: i64) -> Option<Vec<u8>> {
    (n < 0 && !(ctx.apply_replication || crate::expiry::replaying()))
        .then(|| resp_error("ERR invalid expire time, must be >= 0"))
}

/// Convert a stored Instant expiry to the requested unit.
/// Past Instants saturate to 0; the lazy-eviction sweep already removed the
/// field by the time we reach this helper, but we still handle it defensively.
///
/// What is left is rounded up, as a key's is: a field given ten seconds is
/// said, asked at once, to have ten, where cutting the part of a unit that
/// has gone by since said nine (and 249 of 250 ms). A time of day is rounded
/// to the nearest millisecond for the same reason: it is worked out from two
/// clocks read one after the other, and `HPEXPIREAT t` must read back as `t`.
fn instant_to_hfe_value(exp: Instant, mode: HfeReadMode) -> i64 {
    let now_mono = Instant::now();
    let now_sys = std::time::SystemTime::now();
    let delta = exp.saturating_duration_since(now_mono);
    let at_ms = || {
        let unix_now = now_sys
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        // Saturated, not cut to 64 bits, which made a far deadline negative.
        i64::try_from((unix_now + delta + Duration::from_micros(500)).as_millis())
            .unwrap_or(i64::MAX)
    };
    let saturated = |n: u128| i64::try_from(n).unwrap_or(i64::MAX);

    match mode {
        HfeReadMode::RemainingSecs => saturated(delta.as_micros().div_ceil(1_000_000)),
        HfeReadMode::RemainingMillis => saturated(delta.as_micros().div_ceil(1_000)),
        HfeReadMode::AbsoluteSecs => at_ms() / 1000,
        HfeReadMode::AbsoluteMillis => at_ms(),
    }
}

/// Run a HTTL-family command end-to-end.
fn run_hfe_read(ctx: &mut Ctx, cmd_name: &str, mode: HfeReadMode) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return Some(resp_error(&format!(
            "ERR wrong number of arguments for '{}' command",
            cmd_name.to_ascii_lowercase()
        )));
    }
    let key: Vec<u8> = cmd[1].to_vec();
    let fields = match parse_hfe_read_tail(cmd, 2) {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    let now = expire_before(ctx, &key);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_slice()) {
        None => Some(resp_array(
            fields
                .iter()
                .map(|_| resp_integer(HFE_NO_SUCH_FIELD))
                .collect(),
        )),
        Some(ValueRef::Hash(h)) => {
            // Plain Hash → no field has a TTL. Either field exists (-1) or it
            // doesn't (-2).
            let inner = h.read();
            let out: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| {
                    if inner.contains_key(f) {
                        resp_integer(-1)
                    } else {
                        resp_integer(HFE_NO_SUCH_FIELD)
                    }
                })
                .collect();
            Some(resp_array(out))
        }
        Some(ValueRef::HashEx(hx)) => {
            let inner = hx.read();
            let shown = visible(ctx, &inner, now);
            let out: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| {
                    if !shown.contains_key(f) {
                        return resp_integer(HFE_NO_SUCH_FIELD);
                    }
                    match inner.expiries.get(f) {
                        None => resp_integer(-1),
                        Some(exp) => resp_integer(instant_to_hfe_value(*exp, mode)),
                    }
                })
                .collect();
            Some(resp_array(out))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

fn run_hpersist(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hpersist' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();
    let fields = match parse_hfe_read_tail(cmd, 2) {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    crate::replication::script_feed::effects_begin_if_read(ctx);
    expire_before(ctx, &key);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_slice()) {
        None => Some(resp_array(
            fields
                .iter()
                .map(|_| resp_integer(HFE_NO_SUCH_FIELD))
                .collect(),
        )),
        Some(ValueRef::Hash(h)) => {
            // Plain Hash has no TTLs by definition: existing field → -1, missing → -2.
            let inner = h.read();
            let out: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| {
                    if inner.contains_key(f) {
                        resp_integer(-1)
                    } else {
                        resp_integer(HFE_NO_SUCH_FIELD)
                    }
                })
                .collect();
            Some(resp_array(out))
        }
        Some(ValueRef::HashEx(hx)) => {
            // A field due here is not removed by this when this node leaves
            // it for its master's `HDEL` (or loads, or applies a stream):
            // its time to live is taken away, as on the master.
            let mut inner = hx.write();
            let mut persisted: Vec<&[u8]> = Vec::new();
            let out: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| {
                    if !inner.fields.contains_key(f) {
                        return resp_integer(HFE_NO_SUCH_FIELD);
                    }
                    if inner.expiries.remove(f).is_some() {
                        persisted.push(f.as_bytes());
                        resp_integer(1) // TTL removed
                    } else {
                        resp_integer(-1) // existed but had no TTL
                    }
                })
                .collect();
            drop(inner);
            if !persisted.is_empty() {
                // Sent as what it did: the fields whose time to live it took
                // away, and only if there were any.
                record_field_effect(b"HPERSIST", &key, None, &persisted);
                let db = *ctx.selected_db;
                notify(ctx.pubsub, db, KS_HASH, "hpersist", &key);
                notify_subkeys(ctx.pubsub, db, "hpersist", &key, persisted);
            }
            Some(resp_array(out))
        }
        Some(_) => Some(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    }
}

// ── HGETDEL ──────────────────────────────────────────────────────────────────
fn run_hgetdel(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hgetdel' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();
    let fields = match parse_hfe_read_tail(cmd, 2) {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    // Per-field nils are nested inside the reply array; encode them for the
    // negotiated protocol rather than hardcoding RESP2's $-1.
    let resp = effective_resp_for_reply(ctx);
    expire_before(ctx, &key);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_slice()) {
        None => Some(resp_array(
            fields.iter().map(|_| resp_null_for(resp)).collect(),
        )),
        Some(ValueRef::Hash(h)) => {
            let h = Arc::clone(h);
            drop(map);
            let mut inner = h.write();
            let mut deleted: Vec<&[u8]> = Vec::new();
            let out: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| match crate::types::hash_remove(&mut inner, f) {
                    Some(v) => {
                        deleted.push(f.as_bytes());
                        resp_bulk(&v)
                    }
                    None => resp_null_for(resp),
                })
                .collect();
            drop(inner);
            if !deleted.is_empty() {
                let db = *ctx.selected_db;
                notify(ctx.pubsub, db, KS_HASH, "hdel", &key);
                notify_subkeys(ctx.pubsub, db, "hdel", &key, deleted);
            }
            Some(resp_array(out))
        }
        Some(ValueRef::HashEx(hx)) => {
            let hx = Arc::clone(hx);
            drop(map);
            let mut inner = hx.write();
            let mut deleted: Vec<&[u8]> = Vec::new();
            let out: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| match inner.evict_field(f) {
                    Some(v) => {
                        deleted.push(f.as_bytes());
                        resp_bulk(&v)
                    }
                    None => resp_null_for(resp),
                })
                .collect();
            drop(inner);
            crate::stats::record_hit();
            if !deleted.is_empty() {
                let db = *ctx.selected_db;
                notify(ctx.pubsub, db, KS_HASH, "hdel", &key);
                notify_subkeys(ctx.pubsub, db, "hdel", &key, deleted);
            }
            Some(resp_array(out))
        }
        Some(_) => {
            crate::stats::record_miss();
            Some(resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ))
        }
    }
}

// ── HGETEX ───────────────────────────────────────────────────────────────────
//
// HGETEX key [EX seconds | PX ms | EXAT unix-secs | PXAT unix-ms | PERSIST]
//        FIELDS numfields field [field ...]
//
// Reads field values, optionally manipulating per-field TTLs in the same call.
// Returns array of values (nil for missing fields), independent of the TTL
// action's result.

#[derive(Clone, Copy)]
enum HgetexAction {
    None,
    Persist,
    /// The deadline, and the same as milliseconds since the Unix epoch.
    SetExpiry(Instant, u64),
    /// An absolute time already in the past, in milliseconds since the Unix
    /// epoch.
    SetExpiryPast(u64),
}

fn run_hgetex(ctx: &mut Ctx) -> Option<Vec<u8>> {
    match canonical_field_args(&ctx.cmd, FieldArgs::GetEx) {
        Ok(Some(ordered)) => ctx.cmd = ordered,
        Ok(None) => {}
        Err(e) => return Some(e),
    }
    // Per-field nils are nested inside the reply array — see HMGET.
    let resp = effective_resp_for_reply(ctx);
    let cmd = &ctx.cmd;
    if cmd.len() < 5 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hgetex' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();

    // Parse the optional TTL action that sits between the key and the FIELDS
    // keyword. Possibilities:
    //   <none>  → just a read
    //   PERSIST
    //   EX  <seconds>
    //   PX  <milliseconds>
    //   EXAT <unix-seconds>
    //   PXAT <unix-millis>
    let mut idx = 2usize;
    let action = {
        if idx >= cmd.len() {
            return Some(resp_error("ERR syntax error"));
        }
        let tok = String::from_utf8_lossy(cmd[idx]).to_ascii_uppercase();
        match tok.as_str() {
            "FIELDS" => HgetexAction::None,
            "PERSIST" => {
                idx += 1;
                HgetexAction::Persist
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if idx + 1 >= cmd.len() {
                    return Some(resp_error("ERR syntax error"));
                }
                let n: i64 = match parse_utf8(cmd[idx + 1]).and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => {
                        return Some(resp_error("ERR value is not an integer or out of range"));
                    }
                };
                // Past the latest deadline Redis allows: refused, as by Redis.
                let relative = matches!(tok.as_str(), "EX" | "PX");
                let secs = matches!(tok.as_str(), "EX" | "EXAT");
                if !relative && n > 0 && field_deadline_ms(n, secs, 0).is_none() {
                    return Some(invalid_field_expire_time("HGETEX"));
                }
                if !relative && let Some(refused) = negative_unix_time(ctx, n) {
                    return Some(refused);
                }
                let act = match tok.as_str() {
                    "EX" | "PX" if n < 0 => {
                        return Some(resp_error("ERR invalid expire time, must be >= 0"));
                    }
                    // 0 is now: the fields go, as in Redis 8.10.
                    "EX" | "PX" if n == 0 => HgetexAction::SetExpiryPast(now_ms()),
                    "EX" | "PX" => match field_deadline_after(n, secs) {
                        Some((at, at_ms)) => HgetexAction::SetExpiry(at, at_ms),
                        None => return Some(invalid_field_expire_time("HGETEX")),
                    },
                    _ => {
                        let at_ms = if tok == "EXAT" {
                            (n.max(0) as u64).saturating_mul(1000)
                        } else {
                            n.max(0) as u64
                        };
                        match absolute_deadline(at_ms, n <= 0) {
                            (at, false) => HgetexAction::SetExpiry(at, at_ms),
                            (_, true) => HgetexAction::SetExpiryPast(at_ms),
                        }
                    }
                };
                idx += 2;
                act
            }
            _ => return Some(resp_error("ERR syntax error")),
        }
    };

    let fields = match parse_hfe_read_tail(cmd, idx) {
        Ok(f) => f,
        Err(e) => return Some(e),
    };
    // Where fields are held, a moment that has come is set (see
    // `apply_to_hashex`).
    let action = match action {
        HgetexAction::SetExpiryPast(at_ms) if fields_are_held() => {
            HgetexAction::SetExpiry(absolute_deadline(at_ms, false).0, at_ms)
        }
        other => other,
    };
    crate::replication::script_feed::effects_begin_if_read(ctx);
    let now = expire_before(ctx, &key);

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);

    // Special case: if action is None or Persist, no upgrade to HashEx is
    // needed. Otherwise we may need to upgrade.
    let needs_upgrade = matches!(
        action,
        HgetexAction::SetExpiry(..) | HgetexAction::SetExpiryPast(_)
    );

    if needs_upgrade {
        // Take write lock on the shard so we can upgrade Hash → HashEx if needed.
        let mut wmap = write_lock!(shard);
        match wmap.get(key.as_slice()) {
            None => Some(resp_array(
                fields.iter().map(|_| resp_null_for(resp)).collect(),
            )),
            Some(ValueRef::Hash(plain)) => {
                // Upgrade in place: only if at least one requested field
                // exists. Otherwise this call is purely a (failing) read.
                let plain = Arc::clone(plain);
                let plain_r = plain.read();
                let any = fields.iter().any(|f| plain_r.contains_key(f));
                drop(plain_r);
                if !any {
                    return Some(resp_array(
                        fields.iter().map(|_| resp_null_for(resp)).collect(),
                    ));
                }
                let mut plain_w = plain.write();
                let mut new_inner = crate::types::HashInner::new();
                new_inner.fields = std::mem::take(&mut *plain_w);
                drop(plain_w);
                let new_arc = Arc::new(parking_lot::RwLock::new(new_inner));
                wmap.insert(&*key, KiviDBValue::HashEx(Arc::clone(&new_arc)));
                drop(wmap);
                Some(resp_array(hgetex_apply(
                    ctx, &key, &new_arc, &fields, action, resp, now,
                )))
            }
            Some(ValueRef::HashEx(hx)) => {
                let hx = Arc::clone(hx);
                drop(wmap);
                Some(resp_array(hgetex_apply(
                    ctx, &key, &hx, &fields, action, resp, now,
                )))
            }
            Some(_) => Some(resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        }
    } else {
        // No-upgrade fast path: read lock + maybe Persist.
        let map = read_lock!(shard);
        match map.get(key.as_slice()) {
            None => Some(resp_array(
                fields.iter().map(|_| resp_null_for(resp)).collect(),
            )),
            Some(ValueRef::Hash(h)) => {
                let h = Arc::clone(h);
                drop(map);
                // PERSIST on a plain Hash is a no-op; just read the values.
                let inner = h.read();
                let out: Vec<Vec<u8>> = fields
                    .iter()
                    .map(|f| match inner.get(f) {
                        Some(v) => resp_bulk(v),
                        None => resp_null_for(resp),
                    })
                    .collect();
                crate::stats::record_hit();
                Some(resp_array(out))
            }
            Some(ValueRef::HashEx(hx)) => {
                let hx = Arc::clone(hx);
                drop(map);
                crate::stats::record_hit();
                Some(resp_array(hgetex_apply(
                    ctx, &key, &hx, &fields, action, resp, now,
                )))
            }
            Some(_) => {
                crate::stats::record_miss();
                Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ))
            }
        }
    }
}

/// Per-field execution for HGETEX. Returns the value array (one entry per
/// field — bulk for present, nil for missing). The TTL action is applied
/// inline.
fn hgetex_apply(
    ctx: &Ctx,
    key: &[u8],
    hx: &Arc<parking_lot::RwLock<crate::types::HashInner>>,
    fields: &[String],
    action: HgetexAction,
    resp: u8,
    now: Instant,
) -> Vec<Vec<u8>> {
    let mut inner = hx.write();
    // A field due and not removed here (see `field_expiry::policy`) is not
    // there to a reader.
    let hide = inner.has_due(now)
        && crate::field_expiry::policy(ctx.store) != crate::field_expiry::Policy::Keep;
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(fields.len());
    // The fields the TTL action actually changed.
    let mut affected: Vec<&[u8]> = Vec::new();

    for f in fields {
        let due = hide && inner.expiries.get(f).is_some_and(|at| *at <= now);
        let value = match inner.fields.get(f).cloned() {
            Some(v) if !due => v,
            _ => {
                out.push(resp_null_for(resp));
                continue;
            }
        };

        match action {
            HgetexAction::None => {}
            HgetexAction::Persist => {
                if inner.expiries.remove(f).is_some() {
                    affected.push(f.as_bytes());
                }
            }
            HgetexAction::SetExpiry(exp, _) => {
                inner.expiries.insert(f.clone(), exp);
                affected.push(f.as_bytes());
            }
            HgetexAction::SetExpiryPast(_) => {
                // Past time → remove the field immediately. We've already
                // captured `value` for the response.
                inner.evict_field(f);
                affected.push(f.as_bytes());
            }
        }
        out.push(resp_bulk(&value));
    }
    drop(inner);

    // Sent as what it did to the fields it found: a read alone, nothing.
    match action {
        HgetexAction::None => {}
        HgetexAction::Persist => record_field_effect(b"HPERSIST", key, None, &affected),
        HgetexAction::SetExpiry(_, at_ms) => {
            record_field_effect(b"HPEXPIREAT", key, Some(at_ms), &affected)
        }
        HgetexAction::SetExpiryPast(_) => record_field_effect(b"HDEL", key, None, &affected),
    }
    let event = match action {
        HgetexAction::None => None,
        HgetexAction::Persist => Some("hpersist"),
        HgetexAction::SetExpiry(..) => Some("hexpire"),
        HgetexAction::SetExpiryPast(_) => Some("hdel"),
    };
    if let Some(event) = event
        && !affected.is_empty()
    {
        let db = *ctx.selected_db;
        notify(ctx.pubsub, db, KS_HASH, event, key);
        notify_subkeys(ctx.pubsub, db, event, key, affected);
    }
    out
}

// ── HSETEX ───────────────────────────────────────────────────────────────────
//
// HSETEX key [FNX | FXX] [EX seconds | PX milliseconds | EXAT unix-seconds |
//            PXAT unix-milliseconds | KEEPTTL] FIELDS numfields field value [...]
//
// Atomically set one or more fields, with a shared TTL if one is given.
//   FNX → only set if NONE of the fields already exist
//   FXX → only set if ALL of the fields already exist
//   KEEPTTL → leave each field's existing TTL as it is
//   no TTL option → the fields are set without a TTL (any they had is cleared)
// Returns 1 if all fields were set, 0 if FNX/FXX blocked it.
//
// An older KiviDB-only form puts the TTL second — `HSETEX key seconds …` — and
// treats KEEPTTL as "keep a field's TTL if it has one, otherwise use
// `seconds`". It is still accepted: a second argument that is a number can
// only be that form.

/// What `HSETEX` does to the TTL of each field it sets.
#[derive(Clone, Copy)]
enum HsetexTtl {
    /// No option: the field ends up with no TTL.
    Clear,
    /// `KEEPTTL`: whatever TTL the field has stays.
    Keep,
    /// The deadline, and the same in milliseconds since the Unix epoch.
    Set(Instant, u64),
    /// An absolute time already in the past: the field expires on the spot.
    Past(u64),
    /// The legacy form with `KEEPTTL`: keep an existing TTL, else set this one.
    KeepOrSet(Instant, u64),
}

/// Which of the hash field commands whose arguments Redis 8.10 takes in any
/// order around their `FIELDS numfields ...` block.
#[derive(Clone, Copy, PartialEq)]
enum FieldArgs {
    /// HSETEX: `[FNX|FXX] [EX|PX|EXAT|PXAT n|KEEPTTL]`, field-value pairs.
    SetEx,
    /// HGETEX: `[EX|PX|EXAT|PXAT n|PERSIST]`, fields.
    GetEx,
    /// HEXPIRE and its kind: the time at 2, then `[NX|XX|GT|LT]`, fields.
    Expire,
}

/// The arguments of a [`FieldArgs`] command checked as Redis 8.10 checks
/// them (`parseHashFieldExpireArgs`, `parseHashCommandArgs`), with its
/// errors, and put in the order the handlers here read them: the options,
/// then `FIELDS`. The block is skipped by its count, as Redis skips it, so a
/// field or value that reads like an option (`EX`, `FIELDS`) is not taken
/// for one. `Ok(None)`: nothing to reorder.
fn canonical_field_args<'a>(
    cmd: &[&'a [u8]],
    kind: FieldArgs,
) -> Result<Option<Vec<&'a [u8]>>, Vec<u8>> {
    let name = String::from_utf8_lossy(cmd[0]).to_ascii_lowercase();
    let arity = || {
        resp_error(&format!(
            "ERR wrong number of arguments for '{name}' command"
        ))
    };
    let per_field = if kind == FieldArgs::SetEx { 2 } else { 1 };
    let first = if kind == FieldArgs::Expire {
        // The time comes first, and is read before anything else.
        if cmd.len() < 3 {
            return Err(arity());
        }
        if parse_utf8(cmd[2])
            .and_then(|s| s.parse::<i64>().ok())
            .is_none()
        {
            return Err(resp_error("ERR value is not an integer or out of range"));
        }
        3
    } else {
        2
    };
    let one_ttl = match kind {
        FieldArgs::SetEx => {
            "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified"
        }
        _ => "ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified",
    };
    let mut fields: Option<(usize, usize)> = None;
    let mut options: Vec<&'a [u8]> = Vec::new();
    let (mut ttl, mut condition, mut flags) = (false, false, 0u32);
    let mut i = first;
    while i < cmd.len() {
        let word = String::from_utf8_lossy(cmd[i]).to_ascii_uppercase();
        match (kind, word.as_str()) {
            (_, "FIELDS") => {
                if fields.is_some() {
                    return Err(resp_error("ERR FIELDS keyword specified multiple times"));
                }
                let count = if kind == FieldArgs::Expire {
                    if i + 2 >= cmd.len() {
                        return Err(resp_error(
                            "ERR FIELDS requires at least numfields and one field argument",
                        ));
                    }
                    parse_utf8(cmd[i + 1])
                        .and_then(|s| s.parse::<i64>().ok())
                        .filter(|n| (1..=i32::MAX as i64).contains(n))
                        .ok_or_else(|| {
                            resp_error("ERR Parameter `numFields` should be greater than 0")
                        })?
                } else {
                    let Some(raw) = cmd.get(i + 1) else {
                        return Err(arity());
                    };
                    parse_utf8(raw)
                        .and_then(|s| s.parse::<i64>().ok())
                        .filter(|n| (1..=i32::MAX as i64).contains(n))
                        .ok_or_else(|| resp_error("ERR invalid number of fields"))?
                } as usize;
                let end = i + 2 + count * per_field;
                if end > cmd.len() {
                    return Err(resp_error("ERR wrong number of arguments"));
                }
                fields = Some((i, end));
                i = end;
                continue;
            }
            (FieldArgs::SetEx | FieldArgs::GetEx, "EX" | "PX" | "EXAT" | "PXAT") => {
                if ttl {
                    return Err(resp_error(one_ttl));
                }
                if i + 1 >= cmd.len() {
                    return Err(resp_error("ERR missing expire time"));
                }
                ttl = true;
                options.extend_from_slice(&cmd[i..i + 2]);
                i += 2;
                continue;
            }
            (FieldArgs::SetEx, "KEEPTTL") | (FieldArgs::GetEx, "PERSIST") => {
                if ttl {
                    return Err(resp_error(one_ttl));
                }
                ttl = true;
            }
            (FieldArgs::SetEx, "FNX" | "FXX") => {
                if condition {
                    return Err(resp_error(
                        "ERR Only one of FXX or FNX arguments can be specified",
                    ));
                }
                condition = true;
            }
            (FieldArgs::Expire, "NX" | "XX" | "GT" | "LT") => {
                flags |= match word.as_str() {
                    "NX" => 1,
                    "XX" => 2,
                    "GT" => 4,
                    _ => 8,
                };
                // One is passed on; more than one is an error below.
                if options.is_empty() {
                    options.push(cmd[i]);
                }
            }
            _ => {
                return Err(resp_error(&format!(
                    "ERR unknown argument: {}",
                    String::from_utf8_lossy(cmd[i])
                )));
            }
        }
        if !matches!(word.as_str(), "NX" | "XX" | "GT" | "LT") {
            options.push(cmd[i]);
        }
        i += 1;
    }
    let Some((at, end)) = fields else {
        return Err(resp_error("ERR missing FIELDS argument"));
    };
    if flags.count_ones() > 1 {
        return Err(resp_error("ERR Multiple condition flags specified"));
    }
    let mut ordered = Vec::with_capacity(cmd.len());
    ordered.extend_from_slice(&cmd[..first]);
    ordered.extend_from_slice(&options);
    ordered.extend_from_slice(&cmd[at..end]);
    Ok(
        (ordered.len() != cmd.len() || ordered.iter().zip(cmd).any(|(a, b)| !std::ptr::eq(*a, *b)))
            .then_some(ordered),
    )
}

fn run_hsetex(ctx: &mut Ctx) -> Option<Vec<u8>> {
    // The older form, the TTL in seconds straight after the key, is still
    // taken here, and read by the parser below as it was.
    let legacy = ctx
        .cmd
        .get(2)
        .and_then(|a| parse_utf8(a))
        .is_some_and(|s| s.parse::<i64>().is_ok());
    if !legacy {
        match canonical_field_args(&ctx.cmd, FieldArgs::SetEx) {
            Ok(Some(ordered)) => ctx.cmd = ordered,
            Ok(None) => {}
            Err(e) => return Some(e),
        }
    }
    let cmd = &ctx.cmd;
    if cmd.len() < 6 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hsetex' command",
        ));
    }
    let key: Vec<u8> = cmd[1].to_vec();
    let integer = |arg: &[u8]| parse_utf8(arg).and_then(|s| s.parse::<i64>().ok());

    let mut idx = 2usize;
    // Legacy form: the TTL in seconds comes straight after the key.
    let legacy_expiry = match integer(cmd[2]) {
        Some(secs) if secs > 0 => {
            let Some(deadline) = field_deadline_after(secs, true) else {
                return Some(invalid_field_expire_time("HSETEX"));
            };
            idx = 3;
            Some(deadline)
        }
        Some(_) => return Some(resp_error("ERR invalid expire time, must be > 0")),
        None => None,
    };

    let mut fnx = false;
    let mut fxx = false;
    let mut keepttl = false;
    let mut ttl: Option<HsetexTtl> = legacy_expiry.map(|(at, at_ms)| HsetexTtl::Set(at, at_ms));
    const ONE_TTL_OPTION: &str =
        "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified";

    loop {
        if idx >= cmd.len() {
            return Some(resp_error("ERR syntax error"));
        }
        let tok = String::from_utf8_lossy(cmd[idx]).to_ascii_uppercase();
        match tok.as_str() {
            "FNX" | "FXX" => {
                if fnx || fxx {
                    return Some(resp_error(
                        "ERR Only one of FXX or FNX arguments can be specified",
                    ));
                }
                fnx = tok == "FNX";
                fxx = tok == "FXX";
                idx += 1;
            }
            "KEEPTTL" => {
                if keepttl || (ttl.is_some() && legacy_expiry.is_none()) {
                    return Some(resp_error(ONE_TTL_OPTION));
                }
                keepttl = true;
                idx += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if ttl.is_some() || keepttl {
                    return Some(resp_error(ONE_TTL_OPTION));
                }
                let Some(n) = cmd.get(idx + 1).and_then(|a| integer(a)) else {
                    return Some(resp_error("ERR value is not an integer or out of range"));
                };
                // Past the latest deadline Redis allows: refused, as by Redis.
                let relative = matches!(tok.as_str(), "EX" | "PX");
                let secs = matches!(tok.as_str(), "EX" | "EXAT");
                if !relative && n > 0 && field_deadline_ms(n, secs, 0).is_none() {
                    return Some(invalid_field_expire_time("HSETEX"));
                }
                if !relative && let Some(refused) = negative_unix_time(ctx, n) {
                    return Some(refused);
                }
                ttl = Some(match tok.as_str() {
                    "EX" | "PX" if n < 0 => {
                        return Some(resp_error("ERR invalid expire time, must be >= 0"));
                    }
                    // 0 is now: the fields are set and go, as in Redis 8.10.
                    "EX" | "PX" if n == 0 => HsetexTtl::Past(now_ms()),
                    "EX" | "PX" => match field_deadline_after(n, secs) {
                        Some((at, at_ms)) => HsetexTtl::Set(at, at_ms),
                        None => return Some(invalid_field_expire_time("HSETEX")),
                    },
                    // A Unix time, to the nanosecond (#448): cutting the
                    // time now to a millisecond made the deadline later.
                    _ => {
                        let at_ms = if tok == "EXAT" {
                            (n.max(0) as u64).saturating_mul(1000)
                        } else {
                            n.max(0) as u64
                        };
                        match absolute_deadline(at_ms, n <= 0) {
                            (at, false) => HsetexTtl::Set(at, at_ms),
                            (_, true) => HsetexTtl::Past(at_ms),
                        }
                    }
                });
                idx += 2;
            }
            "FIELDS" => break,
            _ => return Some(resp_error("ERR syntax error")),
        }
    }
    let ttl = match (ttl, keepttl, legacy_expiry) {
        (_, true, Some((at, at_ms))) => HsetexTtl::KeepOrSet(at, at_ms),
        (_, true, None) => HsetexTtl::Keep,
        // Where fields are held, a moment that has come is set (see
        // `apply_to_hashex`).
        (Some(HsetexTtl::Past(at_ms)), false, _) if fields_are_held() => {
            HsetexTtl::Set(absolute_deadline(at_ms, false).0, at_ms)
        }
        (Some(ttl), false, _) => ttl,
        (None, false, _) => HsetexTtl::Clear,
    };

    // Now at FIELDS. Consume numfields then exactly numfields*2 args (field/value pairs).
    idx += 1;
    if idx >= cmd.len() {
        return Some(resp_error("ERR syntax error"));
    }
    let numfields: usize = match parse_utf8(cmd[idx]).and_then(|s| s.parse().ok()) {
        Some(n) if n > 0 => n,
        _ => {
            return Some(resp_error(
                "ERR Parameter `numFields` should be greater than 0",
            ));
        }
    };
    idx += 1;
    if numfields.checked_mul(2) != Some(cmd.len() - idx) {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hsetex' command",
        ));
    }

    let mut pairs: Vec<(String, Arc<[u8]>)> = Vec::with_capacity(numfields);
    let mut i = idx;
    while i < cmd.len() {
        let field = crate::commands::helpers::name_from_bytes(cmd[i]).into_owned();
        let val: Arc<[u8]> = Arc::from(cmd[i + 1]);
        pairs.push((field, val));
        i += 2;
    }

    let fields_at = idx;
    let record = crate::replication::script_feed::effects_begin_if_read(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    // The fields of the hash due now go first, where this node removes them
    // ([`expire_first`]): found under the same lookup as the write, not by
    // one of their own before it (#543). The write holds its key's order
    // throughout, so nothing else writes the hash between the two.
    let mut now: Option<Instant> = None;
    let mut tried = false;
    let expiring: Vec<&[u8]> = loop {
        let mut map = write_lock!(shard);

        // Get or upgrade-or-create a HashEx for this key.
        let (hx, had_ttls) = match map.get(key.as_slice()) {
            Some(ValueRef::HashEx(h)) => (Arc::clone(h), true),
            Some(ValueRef::Hash(plain)) => {
                // Upgrade in place
                let plain = Arc::clone(plain);
                let mut plain_w = plain.write();
                let mut new_inner = crate::types::HashInner::new();
                new_inner.fields = std::mem::take(&mut *plain_w);
                drop(plain_w);
                let new_arc = Arc::new(parking_lot::RwLock::new(new_inner));
                map.insert(&*key, KiviDBValue::HashEx(Arc::clone(&new_arc)));
                (new_arc, false)
            }
            Some(_) => {
                return Some(resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            None => {
                // FXX can never be satisfied on a key that does not exist; do not
                // leave an empty hash behind for having asked.
                if fxx {
                    return Some(resp_integer(0));
                }
                let new_arc = Arc::new(parking_lot::RwLock::new(crate::types::HashInner::new()));
                map.insert(&*key, KiviDBValue::HashEx(Arc::clone(&new_arc)));
                (new_arc, false)
            }
        };
        drop(map);

        let mut inner = hx.write();
        if had_ttls {
            let now = *now.get_or_insert_with(Instant::now);
            if expire_first(ctx, &inner, now, &mut tried) {
                drop(inner);
                expire_fields(ctx, &key, now);
                continue;
            }
        }

        // FNX / FXX precheck (atomic: all-or-nothing).
        if fnx && pairs.iter().any(|(f, _)| inner.fields.contains_key(f)) {
            return Some(resp_integer(0));
        }
        if fxx && !pairs.iter().all(|(f, _)| inner.fields.contains_key(f)) {
            return Some(resp_integer(0));
        }

        // Apply all writes.
        let mut expiring: Vec<&[u8]> = Vec::new();
        for (f, v) in &pairs {
            inner.fields.insert(f.clone(), Arc::clone(v));
            match ttl {
                HsetexTtl::Clear => {
                    inner.expiries.remove(f);
                }
                HsetexTtl::Keep => {}
                HsetexTtl::Set(at, _) => {
                    inner.expiries.insert(f.clone(), at);
                    expiring.push(f.as_bytes());
                }
                HsetexTtl::KeepOrSet(at, _) => {
                    if !inner.expiries.contains_key(f) {
                        inner.expiries.insert(f.clone(), at);
                        expiring.push(f.as_bytes());
                    }
                }
                HsetexTtl::Past(_) => {
                    inner.evict_field(f);
                }
            }
        }
        break expiring;
    };

    let db = *ctx.selected_db;
    let all_fields = || pairs.iter().map(|(f, _)| f.as_bytes());
    if record {
        record_hsetex_effect(&key, &cmd[fields_at..], &pairs, ttl, &expiring);
    }
    if matches!(ttl, HsetexTtl::Past(_)) {
        // Set and expired in one step: the set, then the deletion, as Redis
        // 8.10 announces them.
        notify(ctx.pubsub, db, KS_HASH, "hset", &key);
        notify_subkeys(ctx.pubsub, db, "hset", &key, all_fields());
        notify(ctx.pubsub, db, KS_HASH, "hdel", &key);
        notify_subkeys(ctx.pubsub, db, "hdel", &key, all_fields());
    } else {
        crate::vector::maybe_index(
            ctx.index_store,
            db,
            &key,
            &pairs,
            crate::vector::Write::Fields,
        );
        notify(ctx.pubsub, db, KS_HASH, "hset", &key);
        notify_subkeys(ctx.pubsub, db, "hset", &key, all_fields());
        if !expiring.is_empty() {
            notify(ctx.pubsub, db, KS_HASH, "hexpire", &key);
            notify_subkeys(ctx.pubsub, db, "hexpire", &key, expiring);
        }
    }

    Some(resp_integer(1))
}

/// `HSETEX`'s effect, its `FNX`/`FXX` decided here: the fields set, with
/// the deadline they were given as a moment; or, for a moment already past,
/// their deletion.
///
/// Only when the record is read (`script_feed::effects_begin_if_read`).
/// `args` are the command's own field-value pairs, as the client sent them:
/// copied once, straight into the record, not from the names the hash
/// keeps them under.
fn record_hsetex_effect(
    key: &[u8],
    args: &[&[u8]],
    pairs: &[(String, Arc<[u8]>)],
    ttl: HsetexTtl,
    expiring: &[&[u8]],
) {
    if let HsetexTtl::Past(_) = ttl {
        let names: Vec<&[u8]> = pairs.iter().map(|(f, _)| f.as_bytes()).collect();
        record_field_effect(b"HDEL", key, None, &names);
        return;
    }
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(6 + args.len());
    parts.push(b"HSETEX".to_vec());
    parts.push(key.to_vec());
    match ttl {
        HsetexTtl::Set(_, at_ms) => {
            parts.push(b"PXAT".to_vec());
            parts.push(at_ms.to_string().into_bytes());
        }
        HsetexTtl::Keep | HsetexTtl::KeepOrSet(..) => parts.push(b"KEEPTTL".to_vec()),
        HsetexTtl::Clear | HsetexTtl::Past(_) => {}
    }
    parts.push(b"FIELDS".to_vec());
    parts.push(pairs.len().to_string().into_bytes());
    parts.extend(args.iter().map(|arg| arg.to_vec()));
    crate::replication::script_feed::effect(parts);
    if let HsetexTtl::KeepOrSet(_, at_ms) = ttl {
        record_field_effect(b"HPEXPIREAT", key, Some(at_ms), expiring);
    }
}

// ── HSTRLEN ──────────────────────────────────────────────────────────────────
fn run_hstrlen(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hstrlen' command",
        ));
    }
    let key: &[u8] = cmd[1];
    let field = crate::commands::helpers::name_from_bytes(cmd[2]);
    let field: &str = &field;

    match with_hash_fields(ctx, key, |fields| {
        fields.get(field).map(|v| v.len() as i64).unwrap_or(0)
    }) {
        None => {
            crate::stats::record_miss();
            Some(resp_integer(0))
        }
        Some(Err(e)) => {
            crate::stats::record_miss();
            Some(e)
        }
        Some(Ok(n)) => {
            crate::stats::record_hit();
            Some(resp_integer(n))
        }
    }
}
