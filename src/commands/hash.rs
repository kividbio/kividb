use crate::commands::ctx::Ctx;
use crate::commands::handler::effective_resp_for_reply;
use crate::commands::helpers::*;
use crate::keyspace_events::{KS_HASH, notify, notify_subkeys};
use crate::resp::write::*;
use crate::store;
use crate::store::Key;
use crate::store::ValueRef;
use crate::types::KiviDBValue;
use crate::{read_lock, write_lock};
use ahash::AHashMap;
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

            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR must be valid UTF-8")),
            };

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);

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

            let added = {
                let mut map = write_lock!(shard);
                match map.get(key) {
                    Some(ValueRef::Hash(h)) => {
                        let h = Arc::clone(h);
                        drop(map);
                        let mut inner = h.write();
                        let mut new_count = 0i64;
                        for (field, val) in &pairs {
                            if inner.insert(field.clone(), Arc::clone(val)).is_none() {
                                new_count += 1;
                            }
                        }
                        new_count
                    }
                    Some(ValueRef::HashEx(hx)) => {
                        let hx = Arc::clone(hx);
                        drop(map);
                        let mut inner = hx.write();
                        let mut new_count = 0i64;
                        for (field, val) in &pairs {
                            // HSET removes any field-TTL; matches Redis 7.4 semantics
                            // where setting a field clears its TTL.
                            inner.expiries.remove(field);
                            if inner
                                .fields
                                .insert(field.clone(), Arc::clone(val))
                                .is_none()
                            {
                                new_count += 1;
                            }
                        }
                        new_count
                    }
                    Some(_) => {
                        return Some(resp_error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ));
                    }
                    None => {
                        let h =
                            Arc::new(parking_lot::RwLock::new(crate::types::HashFields::default()));
                        map.insert(Key::from(key), KiviDBValue::Hash(Arc::clone(&h)));
                        drop(map);
                        let mut inner = h.write();
                        let mut new_count = 0i64;
                        for (field, val) in &pairs {
                            if inner.insert(field.clone(), Arc::clone(val)).is_none() {
                                new_count += 1;
                            }
                        }
                        new_count
                    }
                }
            }; // shard write lock drops here

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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let field = crate::commands::helpers::name_from_bytes(cmd[2]);
            let field: &str = &field;

            match with_hash_read(ctx, key, |inner| inner.get(field).map(|v| resp_bulk(v))) {
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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

            {
                let db_ref = &ctx.store[*ctx.selected_db];
                let shard = store::get_shard(db_ref, key);
                let mut map = write_lock!(shard);
                match map.get(key) {
                    Some(ValueRef::Hash(h)) => {
                        let h = Arc::clone(h);
                        drop(map);
                        let mut inner = h.write();
                        for (field, val) in &pairs {
                            inner.insert(field.clone(), Arc::clone(val));
                        }
                    }
                    Some(ValueRef::HashEx(hx)) => {
                        let hx = Arc::clone(hx);
                        drop(map);
                        let mut inner = hx.write();
                        for (field, val) in &pairs {
                            inner.expiries.remove(field);
                            inner.fields.insert(field.clone(), Arc::clone(val));
                        }
                    }
                    Some(_) => {
                        return Some(resp_error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ));
                    }
                    None => {
                        let h =
                            Arc::new(parking_lot::RwLock::new(crate::types::HashFields::default()));
                        map.insert(Key::from(key), KiviDBValue::Hash(Arc::clone(&h)));
                        drop(map);
                        let mut inner = h.write();
                        for (field, val) in &pairs {
                            inner.insert(field.clone(), Arc::clone(val));
                        }
                    }
                }
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

            // Per-field nils are nested inside the array, so they need the
            // protocol-correct encoding here — `resp3_normalize_null` only
            // rewrites a reply that is nothing but a top-level null.
            let resp = effective_resp_for_reply(ctx);
            match with_hash_read(ctx, key, |inner| {
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let field = crate::commands::helpers::name_from_bytes(cmd[2]);
            let field: &str = &field;

            match with_hash_read(ctx, key, |inner| {
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let field = crate::commands::helpers::name_from_bytes(cmd[2]).into_owned();
            let incr: i64 = match parse_utf8(cmd[3]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };

            let result = match with_hash_write(ctx, key, |fields, _expiries| {
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
                None => {
                    // Key doesn't exist — create a fresh plain Hash with this one field
                    let db_ref = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db_ref, key);
                    let mut map = write_lock!(shard);
                    let h = Arc::new(parking_lot::RwLock::new(crate::types::HashFields::default()));
                    h.write()
                        .insert(field, Arc::from(incr.to_string().as_bytes()));
                    map.insert(Key::from(key), KiviDBValue::Hash(Arc::clone(&h)));
                    Some(resp_integer(incr))
                }
                Some(Err(e)) => Some(e),
                Some(Ok(Err(parse_err))) => Some(parse_err),
                Some(Ok(Ok(out))) => Some(out),
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let field = crate::commands::helpers::name_from_bytes(cmd[2]).into_owned();
            let incr: f64 = match parse_utf8(cmd[3]).and_then(|s| s.parse::<f64>().ok()) {
                Some(n) if n.is_finite() => n,
                Some(_) => return Some(resp_error("ERR value is NaN or Infinity")),
                None => return Some(resp_error("ERR value is not a valid float")),
            };

            let result = match with_hash_write(ctx, key, |fields, _expiries| {
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
                None => {
                    let db_ref = &ctx.store[*ctx.selected_db];
                    let shard = store::get_shard(db_ref, key);
                    let mut map = write_lock!(shard);
                    let s = format!("{}", incr);
                    let h = Arc::new(parking_lot::RwLock::new(crate::types::HashFields::default()));
                    h.write().insert(field, Arc::from(s.as_bytes()));
                    map.insert(Key::from(key), KiviDBValue::Hash(Arc::clone(&h)));
                    Some(resp_bulk_str(&s))
                }
                Some(Err(e)) => Some(e),
                Some(Ok(Err(parse_err))) => Some(parse_err),
                Some(Ok(Ok(out))) => Some(out),
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let field = crate::commands::helpers::name_from_bytes(cmd[2]).into_owned();
            let val: Arc<[u8]> = Arc::from(cmd[3]);

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let was_set = {
                let mut map = write_lock!(shard);
                match map.get(key) {
                    Some(ValueRef::Hash(h)) => {
                        let h = Arc::clone(h);
                        drop(map);
                        let mut inner = h.write();
                        if inner.contains_key(&field) {
                            false
                        } else {
                            inner.insert(field, val);
                            true
                        }
                    }
                    Some(ValueRef::HashEx(hx)) => {
                        let hx = Arc::clone(hx);
                        drop(map);
                        let mut inner = hx.write();
                        if inner.fields.contains_key(&field) {
                            false
                        } else {
                            inner.fields.insert(field, val);
                            true
                        }
                    }
                    Some(_) => {
                        return Some(resp_error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ));
                    }
                    None => {
                        let h =
                            Arc::new(parking_lot::RwLock::new(crate::types::HashFields::default()));
                        h.write().insert(field, val);
                        map.insert(Key::from(key), KiviDBValue::Hash(Arc::clone(&h)));
                        true
                    }
                }
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };

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

            let db_ref = &ctx.store[*ctx.selected_db];
            let shard = store::get_shard(db_ref, key);
            let map = read_lock!(shard);

            match map.get(key) {
                Some(v) if matches!(v, ValueRef::Hash(_) | ValueRef::HashEx(_)) => {
                    // Get a borrow into the field map regardless of variant.
                    let plain_guard;
                    let ex_guard;
                    let inner: &crate::types::HashFields = match v {
                        ValueRef::Hash(h) => {
                            plain_guard = h.read();
                            &plain_guard
                        }
                        ValueRef::HashEx(hx) => {
                            // Sweep expired fields under write lock first.
                            hx.write().sweep_expired();
                            ex_guard = hx.read();
                            &ex_guard.fields
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k,
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let (cursor, options) =
                match crate::commands::helpers::scan_arguments(cmd, 2, &["NOVALUES"]) {
                    Ok(parsed) => parsed,
                    Err(e) => return Some(e),
                };

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
                    let inner: &crate::types::HashFields = match v {
                        ValueRef::Hash(h) => {
                            plain_guard = h.read();
                            &plain_guard
                        }
                        ValueRef::HashEx(hx) => {
                            // Sweep expired fields under write lock first.
                            hx.write().sweep_expired();
                            ex_guard = hx.read();
                            &ex_guard.fields
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
                        if options
                            .pattern
                            .as_deref()
                            .is_some_and(|pat| !crate::commands::helpers::glob_match(pat, f))
                        {
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let secs: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            // 0 is now, and the fields go, as in Redis 8.10; only a
            // negative time is refused.
            if secs < 0 {
                return Some(resp_error("ERR invalid expire time, must be >= 0"));
            }
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };
            let new_exp = Instant::now() + Duration::from_secs(secs as u64);
            let results = apply_field_expire(ctx, &key, &fields, new_exp, flag, secs == 0);
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let ms: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            // 0 is now, and the fields go, as in Redis 8.10; only a
            // negative time is refused.
            if ms < 0 {
                return Some(resp_error("ERR invalid expire time, must be >= 0"));
            }
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };
            let new_exp = Instant::now() + Duration::from_millis(ms as u64);
            let results = apply_field_expire(ctx, &key, &fields, new_exp, flag, ms == 0);
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let unix_secs: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };

            // Convert absolute Unix seconds to Instant. If the timestamp is
            // already in the past, signal is_past so apply_to_hashex
            // immediately deletes the field and returns 2.
            let (new_exp, is_past) = if unix_secs <= 0 {
                (Instant::now(), true)
            } else {
                let target = std::time::UNIX_EPOCH + Duration::from_secs(unix_secs as u64);
                match target.duration_since(std::time::SystemTime::now()) {
                    Ok(d) => (Instant::now() + d, false),
                    Err(_) => (Instant::now(), true),
                }
            };

            let results = apply_field_expire(ctx, &key, &fields, new_exp, flag, is_past);
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
            let key = match parse_utf8(cmd[1]) {
                Some(k) => k.to_string(),
                None => return Some(resp_error("ERR key must be valid UTF-8")),
            };
            let unix_ms: i64 = match parse_utf8(cmd[2]).and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Some(resp_error("ERR value is not an integer or out of range")),
            };
            let (flag, fields) = match parse_hfe_tail(
                &cmd.iter().map(|v| v.to_vec()).collect::<Vec<Vec<u8>>>(),
                3,
            ) {
                Ok(t) => t,
                Err(e) => return Some(e),
            };

            let (new_exp, is_past) = if unix_ms <= 0 {
                (Instant::now(), true)
            } else {
                let target = std::time::UNIX_EPOCH + Duration::from_millis(unix_ms as u64);
                match target.duration_since(std::time::SystemTime::now()) {
                    Ok(d) => (Instant::now() + d, false),
                    Err(_) => (Instant::now(), true),
                }
            };

            let results = apply_field_expire(ctx, &key, &fields, new_exp, flag, is_past);
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
// Existing HSET/HGET/HMGET/HGETALL/HDEL/etc. were written before the HashEx
// variant existed. To avoid duplicating each command's body for two variants,
// we route every read through this helper which yields a consistent view:
// for a plain Hash, the closure sees the inner AHashMap directly; for a
// HashEx, it sees the inner.fields map (sweeping expired entries first).
//
// Returns:
//   None         — key is absent
//   Some(Err(e)) — key is the wrong type; e is the RESP error to return
//   Some(Ok(v))  — closure result
fn with_hash_read<F, R>(ctx: &Ctx, key: &str, f: F) -> Option<Result<R, Vec<u8>>>
where
    F: FnOnce(&crate::types::HashFields) -> R,
{
    if lazy_expire_on_read(ctx.store, *ctx.selected_db, key) {
        return None;
    }
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => None,
        Some(ValueRef::Hash(h)) => {
            let inner = h.read();
            Some(Ok(f(&inner)))
        }
        Some(ValueRef::HashEx(hx)) => {
            // Sweep expired fields under a write lock before exposing them.
            // This is the lazy-eviction tick — readers also clean up.
            let mut inner_w = hx.write();
            inner_w.sweep_expired();
            drop(inner_w);
            let inner_r = hx.read();
            Some(Ok(f(&inner_r.fields)))
        }
        Some(_) => Some(Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ))),
    }
}

/// Mutable variant: closure gets `&mut crate::types::HashFields` for the
/// fields map, regardless of the underlying variant. Lazy-evicts expired
/// fields first if the variant is HashEx.
fn with_hash_write<F, R>(ctx: &Ctx, key: &str, f: F) -> Option<Result<R, Vec<u8>>>
where
    F: FnOnce(
        &mut crate::types::HashFields,
        Option<&mut AHashMap<String, std::time::Instant>>,
    ) -> R,
{
    if lazy_expire_key(ctx.store, *ctx.selected_db, key) {
        return None;
    }
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, key);
    let map = read_lock!(shard);
    match map.get(key) {
        None => None,
        Some(ValueRef::Hash(h)) => {
            let mut inner = h.write();
            Some(Ok(f(&mut inner, None)))
        }
        Some(ValueRef::HashEx(hx)) => {
            let mut inner = hx.write();
            inner.sweep_expired();
            // Split borrow so the closure can touch both maps.
            let crate::types::HashInner { fields, expiries } = &mut *inner;
            Some(Ok(f(fields, Some(expiries))))
        }
        Some(_) => Some(Err(resp_error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ))),
    }
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
    key: &str,
    fields: &[String],
    new_exp: Instant,
    flag: HfeFlag,
    is_past: bool,
) -> Vec<Vec<u8>> {
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
    key: &str,
    fields: &[String],
    new_exp: Instant,
    flag: HfeFlag,
    is_past: bool,
) -> Vec<Vec<u8>> {
    use parking_lot::RwLock;

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
            map.insert(Key::from(key), KiviDBValue::HashEx(Arc::clone(&new_arc)));

            apply_to_hashex(&new_arc, fields, new_exp, flag, is_past)
        }
        Variant::HashEx(hx) => apply_to_hashex(&hx, fields, new_exp, flag, is_past),
    }
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

        if is_past {
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
        (unix_now + delta + Duration::from_micros(500)).as_millis() as i64
    };

    match mode {
        HfeReadMode::RemainingSecs => delta.as_micros().div_ceil(1_000_000) as i64,
        HfeReadMode::RemainingMillis => delta.as_micros().div_ceil(1_000) as i64,
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let fields = match parse_hfe_read_tail(cmd, 2) {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_str()) {
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
            // Sweep expired fields under write lock first so we don't report
            // stale data.
            hx.write().sweep_expired();
            let inner = hx.read();
            let out: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| {
                    if !inner.fields.contains_key(f) {
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let fields = match parse_hfe_read_tail(cmd, 2) {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_str()) {
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
            // Sweep then operate under write lock. We need write because we
            // remove from the expiries map.
            let mut inner = hx.write();
            inner.sweep_expired();
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let fields = match parse_hfe_read_tail(cmd, 2) {
        Ok(f) => f,
        Err(e) => return Some(e),
    };

    // Per-field nils are nested inside the reply array; encode them for the
    // negotiated protocol rather than hardcoding RESP2's $-1.
    let resp = effective_resp_for_reply(ctx);
    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let map = read_lock!(shard);

    match map.get(key.as_str()) {
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
            inner.sweep_expired();
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
    SetExpiry(Instant),
    SetExpiryPast, // absolute time was already in the past
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };

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
                let act = match tok.as_str() {
                    "EX" | "PX" if n < 0 => {
                        return Some(resp_error("ERR invalid expire time, must be >= 0"));
                    }
                    // 0 is now: the fields go, as in Redis 8.10.
                    "EX" | "PX" if n == 0 => HgetexAction::SetExpiryPast,
                    "EX" => HgetexAction::SetExpiry(Instant::now() + Duration::from_secs(n as u64)),
                    "PX" => {
                        HgetexAction::SetExpiry(Instant::now() + Duration::from_millis(n as u64))
                    }
                    "EXAT" => {
                        if n <= 0 {
                            HgetexAction::SetExpiryPast
                        } else {
                            let target = std::time::UNIX_EPOCH + Duration::from_secs(n as u64);
                            match target.duration_since(std::time::SystemTime::now()) {
                                Ok(d) => HgetexAction::SetExpiry(Instant::now() + d),
                                Err(_) => HgetexAction::SetExpiryPast,
                            }
                        }
                    }
                    "PXAT" => {
                        if n <= 0 {
                            HgetexAction::SetExpiryPast
                        } else {
                            let target = std::time::UNIX_EPOCH + Duration::from_millis(n as u64);
                            match target.duration_since(std::time::SystemTime::now()) {
                                Ok(d) => HgetexAction::SetExpiry(Instant::now() + d),
                                Err(_) => HgetexAction::SetExpiryPast,
                            }
                        }
                    }
                    _ => unreachable!(),
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

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);

    // Special case: if action is None or Persist, no upgrade to HashEx is
    // needed. Otherwise we may need to upgrade.
    let needs_upgrade = matches!(
        action,
        HgetexAction::SetExpiry(_) | HgetexAction::SetExpiryPast
    );

    if needs_upgrade {
        // Take write lock on the shard so we can upgrade Hash → HashEx if needed.
        let mut wmap = write_lock!(shard);
        match wmap.get(key.as_str()) {
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
                wmap.insert(Key::from(&*key), KiviDBValue::HashEx(Arc::clone(&new_arc)));
                drop(wmap);
                Some(resp_array(hgetex_apply(
                    ctx, &key, &new_arc, &fields, action, resp,
                )))
            }
            Some(ValueRef::HashEx(hx)) => {
                let hx = Arc::clone(hx);
                drop(wmap);
                Some(resp_array(hgetex_apply(
                    ctx, &key, &hx, &fields, action, resp,
                )))
            }
            Some(_) => Some(resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        }
    } else {
        // No-upgrade fast path: read lock + maybe Persist.
        let map = read_lock!(shard);
        match map.get(key.as_str()) {
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
                    ctx, &key, &hx, &fields, action, resp,
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
    key: &str,
    hx: &Arc<parking_lot::RwLock<crate::types::HashInner>>,
    fields: &[String],
    action: HgetexAction,
    resp: u8,
) -> Vec<Vec<u8>> {
    let mut inner = hx.write();
    inner.sweep_expired();
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(fields.len());
    // The fields the TTL action actually changed.
    let mut affected: Vec<&[u8]> = Vec::new();

    for f in fields {
        let value = match inner.fields.get(f).cloned() {
            Some(v) => v,
            None => {
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
            HgetexAction::SetExpiry(exp) => {
                inner.expiries.insert(f.clone(), exp);
                affected.push(f.as_bytes());
            }
            HgetexAction::SetExpiryPast => {
                // Past time → remove the field immediately. We've already
                // captured `value` for the response.
                inner.evict_field(f);
                affected.push(f.as_bytes());
            }
        }
        out.push(resp_bulk(&value));
    }
    drop(inner);

    let event = match action {
        HgetexAction::None => None,
        HgetexAction::Persist => Some("hpersist"),
        HgetexAction::SetExpiry(_) => Some("hexpire"),
        HgetexAction::SetExpiryPast => Some("hdel"),
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
    Set(Instant),
    /// An absolute time already in the past: the field expires on the spot.
    Past,
    /// The legacy form with `KEEPTTL`: keep an existing TTL, else set this one.
    KeepOrSet(Instant),
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
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k.to_string(),
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let integer = |arg: &[u8]| parse_utf8(arg).and_then(|s| s.parse::<i64>().ok());

    let mut idx = 2usize;
    // Legacy form: the TTL in seconds comes straight after the key.
    let legacy_expiry = match integer(cmd[2]) {
        Some(secs) if secs > 0 => {
            idx = 3;
            Some(Instant::now() + Duration::from_secs(secs as u64))
        }
        Some(_) => return Some(resp_error("ERR invalid expire time, must be > 0")),
        None => None,
    };

    let mut fnx = false;
    let mut fxx = false;
    let mut keepttl = false;
    let mut ttl: Option<HsetexTtl> = legacy_expiry.map(HsetexTtl::Set);
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
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                ttl = Some(match tok.as_str() {
                    "EX" | "PX" if n < 0 => {
                        return Some(resp_error("ERR invalid expire time, must be >= 0"));
                    }
                    // 0 is now: the fields are set and go, as in Redis 8.10.
                    "EX" | "PX" if n == 0 => HsetexTtl::Past,
                    "EX" | "PX" => {
                        let ms = if tok == "EX" {
                            n.saturating_mul(1000)
                        } else {
                            n
                        };
                        HsetexTtl::Set(Instant::now() + Duration::from_millis(ms as u64))
                    }
                    _ => {
                        let at_ms = if tok == "EXAT" {
                            n.saturating_mul(1000)
                        } else {
                            n
                        };
                        if at_ms <= now_ms {
                            HsetexTtl::Past
                        } else {
                            HsetexTtl::Set(
                                Instant::now() + Duration::from_millis((at_ms - now_ms) as u64),
                            )
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
        (_, true, Some(at)) => HsetexTtl::KeepOrSet(at),
        (_, true, None) => HsetexTtl::Keep,
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

    let db_ref = &ctx.store[*ctx.selected_db];
    let shard = store::get_shard(db_ref, &key);
    let mut map = write_lock!(shard);

    // Get or upgrade-or-create a HashEx for this key.
    let hx = match map.get(key.as_str()) {
        Some(ValueRef::HashEx(h)) => Arc::clone(h),
        Some(ValueRef::Hash(plain)) => {
            // Upgrade in place
            let plain = Arc::clone(plain);
            let mut plain_w = plain.write();
            let mut new_inner = crate::types::HashInner::new();
            new_inner.fields = std::mem::take(&mut *plain_w);
            drop(plain_w);
            let new_arc = Arc::new(parking_lot::RwLock::new(new_inner));
            map.insert(Key::from(&*key), KiviDBValue::HashEx(Arc::clone(&new_arc)));
            new_arc
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
            map.insert(Key::from(&*key), KiviDBValue::HashEx(Arc::clone(&new_arc)));
            new_arc
        }
    };
    drop(map);

    let mut inner = hx.write();
    inner.sweep_expired();

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
            HsetexTtl::Set(at) => {
                inner.expiries.insert(f.clone(), at);
                expiring.push(f.as_bytes());
            }
            HsetexTtl::KeepOrSet(at) => {
                if !inner.expiries.contains_key(f) {
                    inner.expiries.insert(f.clone(), at);
                    expiring.push(f.as_bytes());
                }
            }
            HsetexTtl::Past => {
                inner.evict_field(f);
            }
        }
    }
    drop(inner);

    let db = *ctx.selected_db;
    let all_fields = || pairs.iter().map(|(f, _)| f.as_bytes());
    if matches!(ttl, HsetexTtl::Past) {
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

// ── HSTRLEN ──────────────────────────────────────────────────────────────────
fn run_hstrlen(ctx: &mut Ctx) -> Option<Vec<u8>> {
    let cmd = &ctx.cmd;
    if cmd.len() != 3 {
        return Some(resp_error(
            "ERR wrong number of arguments for 'hstrlen' command",
        ));
    }
    let key = match parse_utf8(cmd[1]) {
        Some(k) => k,
        None => return Some(resp_error("ERR key must be valid UTF-8")),
    };
    let field = crate::commands::helpers::name_from_bytes(cmd[2]);
    let field: &str = &field;

    match with_hash_read(ctx, key, |fields| {
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
